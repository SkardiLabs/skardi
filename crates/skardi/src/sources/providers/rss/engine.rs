//! The freshness state machine: the one place that decides whether a scan
//! serves a cached window, revalidates it, refetches it, or degrades to stale
//! rows — and the first component that composes the config, OPML, egress,
//! fetch, parse, schema, and cache layers into something a table provider can
//! call.
//!
//! ## Per-feed degradation is data, not scan failure
//!
//! [`RssEngine::serve_feed`] returns `Option<RecordBatch>` and has no `Err`
//! at all. Every failure — refused egress, timeout, `500`, unparseable
//! document — resolves to either the last good window stamped `stale-error`
//! or zero rows, with the reason recorded in the feed's observation and
//! surfaced through `feeds.last_error`. That is what makes one dead feed
//! among fifty leave the other forty-nine queryable: a scan cannot fail
//! because of one subscription.
//!
//! ## `feeds` is a pure state read
//!
//! [`RssEngine::feeds_row`] is synchronous and never touches the fetcher, so
//! `SELECT * FROM feeds` issues zero requests at any moment — *including*
//! immediately after a failure. A failure re-arms the TTL to
//! [`failure_fuse`] (negative caching), so a dead feed is not re-poked on
//! every scan either.
//!
//! ## Where the launch gate is checked
//!
//! `serve_feed` takes a `launch_gate` closure the exec layer uses to stop
//! launching fetches once a `LIMIT` is satisfied. A within-TTL cache hit
//! serves regardless of the gate — there is no side effect to gate. An
//! expired feed re-checks the gate *after* acquiring the politeness permit
//! and immediately before fetching. That placement is load-bearing rather
//! than incidental: DataFusion polls all partitions concurrently, so every
//! partition passes any pre-acquire check while nothing has been emitted yet
//! and then queues on the semaphore, which makes a pre-acquire check a no-op
//! under concurrency. Only a gate evaluated after the permit can actually
//! stop a launch. A closed gate returns `None` without fetching and without
//! writing health state, so the feed behaves exactly like one the plan
//! pruned.
//!
//! ## Feed keys never come from a query
//!
//! `MemoryFeedCache` keys entries by `&str` and bounds observation-only
//! entries with a last-resort backstop, on the assumption that keys come from
//! the fixed, config-derived subscription list. This module is where that
//! assumption is enforced: [`RssEngine::subscription`] is the only way any
//! path here obtains a feed key, it resolves the caller's `&str` against
//! `self.subscriptions` (answering `None` for anything else), and every key
//! passed to the cache is the resolved `ResolvedSubscription::name` — a
//! string owned by this engine — never the argument the caller supplied. A
//! predicate value, a projection, or any other query-influenced string can
//! therefore only ever produce zero rows.
//!
//! ## What may reach `feeds.last_error`
//!
//! This module is the only writer of that column. [`MAX_ERROR_CHARS`] bounds
//! its length, but truncation is not redaction, so the property that matters is
//! separate:
//!
//! > No `feeds.last_error` value contains text taken from the feed body, with
//! > one deliberate exception: a JSON Feed's declared `version` string, when
//! > that version is unsupported.
//!
//! **The tests enforce that, not this comment.**
//! `parse_failure_last_error_never_echoes_character_data` embeds a sentinel in
//! body content across several document shapes — each chosen because it reaches
//! a different error path, with a counter that fails if a shape stops erroring
//! — and asserts the sentinel never appears in the recorded error.
//! `json_unsupported_version_is_the_one_body_text_kept_in_last_error` pins the
//! exception from the other side, so it cannot quietly widen. Three successive
//! attempts at explaining this property in prose were each wrong in a different
//! way; the tests were right every time. If a dependency upgrade changes any of
//! this, they fail, and whoever sees that should re-derive the situation rather
//! than trust the paragraph below.
//!
//! What was *measured*, against `feed-rs` 2.4.0 and the `quick-xml` 0.41.0 it
//! resolves to (`Cargo.lock`) — evidence for the property today, not a taxonomy
//! of everything a malformed document can produce:
//!
//! - Two error families were observed reaching this column, and both carry
//!   structural text only. A truncated document reports
//!   `SyntaxError::UnclosedTag` — "tag not closed: `>` not found before end of
//!   input" (`quick-xml-0.41.0/src/errors.rs:71`), prefixed "syntax error: " by
//!   `Error`'s `Display` (`src/errors.rs:287`). An out-of-range character
//!   reference such as `&#x110000;` reports `EscapeError::InvalidCharRef`,
//!   which renders the parsed *number* alone — "`1114112` is not a valid
//!   codepoint" (`src/escape.rs:30`) — and nothing adjacent to it.
//! - `EscapeError::UnrecognizedEntity` is the variant that would interpolate a
//!   token lifted from the document (`src/escape.rs:66-68`). Its only producer
//!   is `quick_xml::escape::unescape`, and feed-rs's only call to that is on
//!   attribute values, where the error is discarded:
//!   `unescape(&decoded_value).unwrap_or_else(|_| decoded_value.clone())`
//!   (`feed-rs-2.4.0/src/xml/mod.rs:597-598`). Element text does not go through
//!   `unescape` at all — feed-rs resolves references itself and writes an
//!   unresolvable entity back into the text verbatim
//!   (`feed-rs-2.4.0/src/xml/mod.rs:333-345`), which is why an undefined entity
//!   in a title yields no error at all.
//! - The exception is `ParseFeedError::JsonUnsupportedVersion(String)`, whose
//!   `Display` is "unsupported version: {version}"
//!   (`feed-rs-2.4.0/src/parser/mod.rs:66`), reached from
//!   `src/parser/json/mod.rs:29`. That string is a member value out of the
//!   document. It is kept because an unsupported version is undiagnosable
//!   without it, and it is bounded by [`MAX_ERROR_CHARS`] like everything else
//!   here.
//! - [`FetchError`]'s own strings — this crate's, so not a dependency
//!   question — are statuses, byte and second counts, and URLs: the configured
//!   feed URL, or a redirect `Location`. A `Location` is attacker-influenced
//!   but is not body content, and the cap bounds it.
//!
//! ## No in-flight coalescing
//!
//! Two concurrent scans that both find the same feed expired can both fetch
//! it. That is a documented future extension, and `open_connector`'s cache
//! has the same property.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::record_batch::RecordBatch;
use tokio::sync::Semaphore;

use super::ResolvedSubscription;
use super::cache::{
    CachedWindow, FeedCache, FeedObservation, FeedStatus, MemoryFeedCache, failure_fuse,
};
use super::config::RssConfig;
use super::egress::EgressPolicy;
use super::error::RssError;
use super::fetch::{FeedFetcher, FetchError, FetchOutcome, Validators};
use super::parse::{ParsedDocument, parse_feed_document};
use super::schema::{FeedsRow, build_feeds_batch, build_items_batch, with_window_status};

/// Byte budget for the window cache, shared across every feed of one source.
pub const CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Headroom over the subscription count for the cache's window-entry bound,
/// so a source whose feeds all hold a window at once is not evicting on the
/// steady state.
const WINDOW_ENTRY_HEADROOM: usize = 8;

/// Length cap on a stored `feeds.last_error`, in characters. A bound on length
/// only — see the module doc for the separate argument about what content can
/// reach the column at all.
const MAX_ERROR_CHARS: usize = 512;

/// Ceiling on the configured TTL. `RssConfig` puts no upper bound on
/// `ttl_seconds`, and the TTL becomes the `Duration` added to an `Instant`
/// on every arm; `std`'s own `Instant` docs warn that a large enough add
/// "panics on macOS" and that `Add<Duration> for Instant` "may panic if the
/// resulting point in time cannot be represented". A year is longer than any
/// meaningful feed TTL, so clamping here costs nothing and keeps the arming
/// arithmetic in a range the platform can represent.
const MAX_TTL: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// The freshness state machine for one `rss` data source.
pub struct RssEngine {
    source_name: String,
    subscriptions: Vec<ResolvedSubscription>,
    /// Subscription name → index into `subscriptions`. Names are unique
    /// (`RssConfig::validate` and `resolve_subscriptions` both reject
    /// duplicates), so this is a total, injective index of the fixed
    /// subscription list — see the module doc on feed keys.
    by_name: HashMap<String, usize>,
    fetcher: FeedFetcher,
    cache: Arc<dyn FeedCache>,
    /// Politeness bound: at most `max_concurrent` feeds in flight per
    /// process, and the queue a closed launch gate cancels a feed out of.
    semaphore: Arc<Semaphore>,
    ttl: Duration,
    scan_timeout: Duration,
}

impl RssEngine {
    /// Build an engine over `subscriptions`, with its own fetcher and
    /// in-memory window cache sized from the subscription count.
    pub fn new(
        source_name: String,
        subscriptions: Vec<ResolvedSubscription>,
        config: &RssConfig,
        policy: Arc<EgressPolicy>,
    ) -> Result<Self, RssError> {
        let fetcher = FeedFetcher::new(
            policy,
            Duration::from_secs(config.request_timeout_seconds),
            config.max_response_bytes,
            config.user_agent.clone(),
        )?;
        let cache = Arc::new(MemoryFeedCache::new(
            CACHE_MAX_BYTES,
            subscriptions.len().saturating_add(WINDOW_ENTRY_HEADROOM),
        ));
        Ok(Self::with_parts(
            source_name,
            subscriptions,
            config,
            fetcher,
            cache,
        ))
    }

    /// Assemble an engine around a caller-supplied fetcher and cache — the
    /// seam this module's own tests use to point the fetcher at a mock
    /// server's egress policy.
    pub(crate) fn with_parts(
        source_name: String,
        subscriptions: Vec<ResolvedSubscription>,
        config: &RssConfig,
        fetcher: FeedFetcher,
        cache: Arc<dyn FeedCache>,
    ) -> Self {
        let by_name = subscriptions
            .iter()
            .enumerate()
            .map(|(index, sub)| (sub.name.clone(), index))
            .collect();
        let configured_ttl = Duration::from_secs(config.ttl_seconds);
        let ttl = configured_ttl.min(MAX_TTL);
        if ttl != configured_ttl {
            tracing::warn!(
                source = %source_name,
                configured_ttl_seconds = config.ttl_seconds,
                effective_ttl_seconds = ttl.as_secs(),
                "rss ttl_seconds clamped to the engine's ceiling"
            );
        }
        Self {
            source_name,
            subscriptions,
            by_name,
            fetcher,
            cache,
            // `RssConfig::validate` rejects `max_concurrent: 0`; the floor
            // keeps a directly constructed config from producing a
            // semaphore that parks every fetch forever.
            semaphore: Arc::new(Semaphore::new(config.max_concurrent.max(1))),
            ttl,
            scan_timeout: Duration::from_secs(config.scan_timeout_seconds),
        }
    }

    /// The fixed subscription list this engine serves, in config order.
    pub fn subscriptions(&self) -> &[ResolvedSubscription] {
        &self.subscriptions
    }

    /// Deadline for one whole scan, applied by the exec layer.
    pub fn scan_timeout(&self) -> Duration {
        self.scan_timeout
    }

    /// Serve one feed's `items` window: the full 17-column batch with the
    /// right `window_status`, or `None` for zero rows.
    ///
    /// Never returns an error — see the module doc on degradation — and never
    /// fetches when the cached window is still within its TTL. See the module
    /// doc for `launch_gate`'s contract.
    pub async fn serve_feed(
        &self,
        feed: &str,
        launch_gate: impl Fn() -> bool + Send,
    ) -> Option<RecordBatch> {
        // Resolving first is what keeps every cache key config-derived: from
        // here on the feed is `sub.name`, owned by `self.subscriptions`, and
        // the caller's `feed` argument is never used again.
        let Some(sub) = self.subscription(feed) else {
            // Still one record per serve: a name that is not a subscription is
            // a projection or a stale plan referring to a feed this source no
            // longer has, which is worth seeing rather than silently zero
            // rows. `feed` is echoed because the whole point is which name was
            // asked for; it reaches no cache and no request.
            tracing::debug!(
                source = %self.source_name,
                feed,
                outcome = "unknown-feed",
                "rss feed served"
            );
            return None;
        };
        let started = Instant::now();
        let (batch, log) = self.serve_subscription(sub, launch_gate).await;
        // One record per serve. Feed URLs are safe to log; response bodies
        // are never logged, and `bytes`/`rows` describe the body without
        // quoting it.
        tracing::debug!(
            source = %self.source_name,
            feed = %sub.name,
            url = %sub.url,
            outcome = log.outcome,
            http_status = ?log.http_status,
            bytes = log.bytes,
            rows = log.rows,
            notes = log.notes,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "rss feed served"
        );
        batch
    }

    /// The state machine proper, over an already-resolved subscription.
    async fn serve_subscription(
        &self,
        sub: &ResolvedSubscription,
        launch_gate: impl Fn() -> bool + Send,
    ) -> (Option<RecordBatch>, ServeLog) {
        let snapshot = self.cache.snapshot(&sub.name, Instant::now());

        if snapshot.within_ttl {
            // Zero network, zero permit, and the gate is not consulted:
            // serving what is already cached has no side effect to gate.
            // `Never`/`Error` have no window label, so those serve zero rows.
            let batch = snapshot.window.as_ref().and_then(|window| {
                with_window_status(&window.batch, snapshot.observation.last_status)
            });
            let rows = batch.as_ref().map_or(0, RecordBatch::num_rows);
            return (
                batch,
                ServeLog {
                    outcome: "cache-hit",
                    http_status: snapshot.observation.http_status,
                    bytes: 0,
                    rows,
                    notes: 0,
                },
            );
        }

        // Politeness: at most `max_concurrent` feeds in flight per process.
        // The permit is released when this guard drops, whether this future
        // completes or is cancelled mid-fetch — `SemaphorePermit`'s `Drop`
        // calls `Semaphore::add_permits` (tokio 1.52.3,
        // `src/sync/semaphore.rs:1196`).
        let _permit = match self.semaphore.acquire().await {
            Ok(permit) => permit,
            // Documented to fail only once the semaphore is closed ("If the
            // semaphore has been closed, this returns an `AcquireError`"),
            // and nothing here ever closes it. Degrade to zero rows rather
            // than panic if that ever changes.
            Err(_) => return (None, ServeLog::bare("error")),
        };

        // The gate is re-read *here*: after the permit, immediately before
        // the fetch. A check before the wait would be read by every
        // partition while nothing has been emitted yet and would therefore
        // be stale by the time this feed's turn came — see the module doc.
        // Returning here writes no health state at all, so the feed is
        // neither fetched nor health-refreshed, exactly like a partition the
        // plan never launched.
        if !launch_gate() {
            return (None, ServeLog::bare("gate-closed"));
        }

        let validators = snapshot.window.as_ref().map(|window| Validators {
            etag: window.etag.clone(),
            last_modified: window.last_modified.clone(),
        });

        match self.fetcher.fetch(&sub.url, validators.as_ref()).await {
            Ok(FetchOutcome::NotModified { http_status }) => {
                self.cache.record_not_modified(
                    &sub.name,
                    http_status,
                    now_ms(),
                    arm(Instant::now(), self.ttl),
                );
                // The `304` confirms exactly the window whose validators this
                // attempt sent, which is the one in `snapshot` — read before
                // the permit, so with no in-flight coalescing a concurrent
                // scan may have replaced the cached window in between. This
                // serve then emits the older rows while `feeds.item_count`,
                // read from the observation, describes the newer ones. That is
                // the one observable consequence of allowing double fetches:
                // serving the window this `304` actually vouches for is more
                // defensible than stamping `revalidated` on rows no server
                // confirmed, and both windows are legitimate reads of the feed.
                let batch = snapshot
                    .window
                    .as_ref()
                    .and_then(|window| with_window_status(&window.batch, FeedStatus::Revalidated));
                let rows = batch.as_ref().map_or(0, RecordBatch::num_rows);
                (
                    batch,
                    ServeLog {
                        outcome: "revalidated",
                        http_status: Some(http_status),
                        bytes: 0,
                        rows,
                        notes: 0,
                    },
                )
            }
            Ok(FetchOutcome::Fetched {
                body,
                http_status,
                etag,
                last_modified,
                content_type,
            }) => {
                let bytes = body.len();
                match parse_feed_document(&body, content_type.as_deref()) {
                    Ok(document) => {
                        let notes = document.conformance_notes.len();
                        let batch = self.record_fresh_window(
                            sub,
                            document,
                            http_status,
                            etag,
                            last_modified,
                        );
                        let rows = batch.num_rows();
                        (
                            Some(batch),
                            ServeLog {
                                outcome: "fetched",
                                http_status: Some(http_status),
                                bytes,
                                rows,
                                notes,
                            },
                        )
                    }
                    // The document was fetched but is not a feed. The stage
                    // and the parser's own reason are what make it
                    // diagnosable; the sniffed dialect survives because it is
                    // read off the raw bytes before parsing.
                    Err(failure) => self.degrade(
                        sub,
                        Some(http_status),
                        parse_error_message(failure.stage, &failure.reason),
                        failure.dialect_declared,
                        bytes,
                    ),
                }
            }
            Err(error) => {
                let http_status = match &error {
                    FetchError::Status { status } => Some(*status),
                    _ => None,
                };
                self.degrade(
                    sub,
                    http_status,
                    truncate(&error.to_string(), MAX_ERROR_CHARS),
                    None,
                    0,
                )
            }
        }
    }

    /// Store a freshly parsed window and its observation, and hand back the
    /// batch to serve. `build_items_batch` already stamps `fresh`, so the
    /// served batch needs no relabelling.
    fn record_fresh_window(
        &self,
        sub: &ResolvedSubscription,
        document: ParsedDocument,
        http_status: u16,
        etag: Option<String>,
        last_modified: Option<String>,
    ) -> RecordBatch {
        let batch = build_items_batch(&sub.name, &sub.url, &document.items);
        let observation = FeedObservation {
            last_fetch_ms: Some(now_ms()),
            last_status: FeedStatus::Fresh,
            http_status: Some(http_status),
            last_error: None,
            dialect: Some(document.dialect.to_string()),
            dialect_declared: document.dialect_declared,
            // A clean document records an empty list (`"[]"`), not NULL:
            // "parsed with nothing to note" and "never parsed" are different
            // states and `feeds.conformance_notes` distinguishes them.
            conformance_notes: serde_json::to_string(&document.conformance_notes).ok(),
            title: document.meta.title,
            site_url: document.meta.site_url,
            description: document.meta.description,
            item_count: Some(batch.num_rows() as u64),
        };
        self.cache.record_success(
            &sub.name,
            CachedWindow {
                batch: batch.clone(),
                etag,
                last_modified,
            },
            observation,
            arm(Instant::now(), self.ttl),
        );
        batch
    }

    /// Record a failed attempt and serve whatever is still serveable: the
    /// last good window stamped `stale-error`, or zero rows.
    ///
    /// The TTL re-arms to [`failure_fuse`], not to the success TTL, so a dead
    /// feed is retried at most once per fuse window instead of on every scan
    /// — the negative caching that keeps `feeds_row` request-free right after
    /// a failure.
    fn degrade(
        &self,
        sub: &ResolvedSubscription,
        http_status: Option<u16>,
        error: String,
        dialect_declared: Option<String>,
        bytes: usize,
    ) -> (Option<RecordBatch>, ServeLog) {
        self.cache.record_failure(
            &sub.name,
            http_status,
            error.clone(),
            dialect_declared,
            now_ms(),
            arm(Instant::now(), failure_fuse(self.ttl)),
        );
        tracing::warn!(
            source = %self.source_name,
            feed = %sub.name,
            url = %sub.url,
            %error,
            "rss feed degraded"
        );

        // Read back after recording: `record_failure` is what decides
        // between `StaleError` (a window survived to serve) and `Error` (none
        // did), so the status stamped on the rows comes from the cache rather
        // than from a second guess here.
        let snapshot = self.cache.snapshot(&sub.name, Instant::now());
        let batch = snapshot
            .window
            .as_ref()
            .and_then(|window| with_window_status(&window.batch, snapshot.observation.last_status));
        let rows = batch.as_ref().map_or(0, RecordBatch::num_rows);
        (
            batch,
            ServeLog {
                outcome: snapshot.observation.last_status.as_str(),
                http_status,
                bytes,
                rows,
                notes: 0,
            },
        )
    }

    /// One feed's health as a single-row `feeds` batch, or zero rows when
    /// `feed` is not one of this engine's subscriptions.
    ///
    /// Synchronous and side-effect-free with respect to the network: a
    /// `feeds` scan issues no requests, whatever state the feed is in. It
    /// cannot even reach the fetcher — there is no `await` here to hang one
    /// off of.
    pub fn feeds_row(&self, feed: &str) -> RecordBatch {
        let Some(sub) = self.subscription(feed) else {
            // Not a configured subscription, so there is no row to report.
            return build_feeds_batch(&[]);
        };
        let snapshot = self.cache.snapshot(&sub.name, Instant::now());
        let observation = snapshot.observation;
        // The validators live on the window, so they read NULL once a window
        // has been evicted under memory pressure while the health it was
        // observed with survives.
        let (etag, last_modified) = snapshot
            .window
            .map_or((None, None), |window| (window.etag, window.last_modified));
        build_feeds_batch(&[FeedsRow {
            name: sub.name.clone(),
            url: sub.url.clone(),
            title: observation.title,
            site_url: observation.site_url,
            description: observation.description,
            last_fetch_ms: observation.last_fetch_ms,
            last_status: observation.last_status.as_str(),
            http_status: observation.http_status,
            last_error: observation.last_error,
            etag,
            last_modified,
            dialect: observation.dialect,
            dialect_declared: observation.dialect_declared,
            conformance_notes: observation.conformance_notes,
            item_count: observation.item_count,
        }])
    }

    /// Resolve a caller-supplied name against this engine's own subscription
    /// list — the single gate the module doc's feed-key invariant rests on.
    fn subscription(&self, feed: &str) -> Option<&ResolvedSubscription> {
        let index = *self.by_name.get(feed)?;
        self.subscriptions.get(index)
    }
}

/// What one serve did, for the single `debug!` line [`RssEngine::serve_feed`]
/// emits. Threading this out of the state machine rather than logging inside
/// each branch is what keeps it one record per serve however the serve ended.
struct ServeLog {
    /// The full domain of the `outcome` log field: `cache-hit` |
    /// `revalidated` | `fetched` | `stale-error` | `error` | `gate-closed`.
    /// [`RssEngine::serve_feed`] emits one more value without building a
    /// `ServeLog` for it — `unknown-feed`, for a name that is not a
    /// subscription, which returns before the state machine runs.
    outcome: &'static str,
    http_status: Option<u16>,
    /// Decoded response body size; `0` when no body was read.
    bytes: usize,
    rows: usize,
    /// Conformance notes recorded by this serve's parse, if it parsed one.
    notes: usize,
}

impl ServeLog {
    /// An outcome with nothing to report but its name — no response, no rows.
    fn bare(outcome: &'static str) -> Self {
        Self {
            outcome,
            http_status: None,
            bytes: 0,
            rows: 0,
            notes: 0,
        }
    }
}

/// The `feeds.last_error` text for a failed parse: the stage that gave up plus
/// the parser's own reason.
///
/// The cap applies to the *composed* string, not just to `reason`. Bounding the
/// reason alone would let the `"parse failed at …: "` prefix push the stored
/// value past [`MAX_ERROR_CHARS`], which is a cap on what lands in the column.
fn parse_error_message(stage: &str, reason: &str) -> String {
    truncate(
        &format!("parse failed at {stage}: {reason}"),
        MAX_ERROR_CHARS,
    )
}

/// Bound a stored error string to `max_chars` *characters*, cutting on a char
/// boundary so a multi-byte sequence is never split. A length bound only:
/// nothing here removes content, so what may appear in `feeds.last_error` is
/// decided by which strings are passed in, not by this.
fn truncate(text: &str, max_chars: usize) -> String {
    match text.char_indices().nth(max_chars) {
        Some((byte_index, _)) => text[..byte_index].to_string(),
        None => text.to_string(),
    }
}

/// Wall-clock milliseconds since the Unix epoch, for `feeds.last_fetch`.
/// A clock before the epoch, or a value past `i64`, reads as `0` rather than
/// wrapping into a nonsense timestamp.
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|since| i64::try_from(since.as_millis()).ok())
        .unwrap_or(0)
}

/// `now + after`, without the panic `Instant`'s `+` operator documents ("This
/// function may panic if the resulting point in time cannot be represented by
/// the underlying data structure. See `Instant::checked_add` for a version
/// without panic" — and `std`'s `Instant` docs give a large-add example that
/// "panics on macOS" specifically). Both durations that reach here are
/// bounded — the TTL by [`MAX_TTL`], a failure fuse by [`failure_fuse`]'s own
/// 300s clamp — so the fallback is a safety net rather than a live path;
/// falling back to `now` leaves the feed due immediately, which costs a fetch
/// rather than a panic mid-scan.
fn arm(now: Instant, after: Duration) -> Instant {
    now.checked_add(after).unwrap_or(now)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use arrow::array::{Array, UInt64Array};

    use super::*;
    use crate::sources::providers::rss::cache::FeedSnapshot;
    use crate::sources::providers::rss::config::{FeedSubscription, inline_config};
    use crate::sources::providers::rss::testutil::{
        MockFeedServer, MockResponse, RSS2_MINIMAL, str_col, str_opt_col,
    };

    /// An engine over `feeds` (`(name, path)` pairs) pointed at `server`,
    /// with `ttl_seconds` and a default politeness bound.
    fn test_engine(server: &MockFeedServer, feeds: &[(&str, &str)], ttl_seconds: u64) -> RssEngine {
        let urls: Vec<(String, String)> = feeds
            .iter()
            .map(|(name, path)| ((*name).to_string(), format!("{}{path}", server.url())))
            .collect();
        engine_over(&urls, ttl_seconds, 4)
    }

    /// The same, over absolute URLs — for the feeds that must *not* point at
    /// the mock server (an egress-blocked target, say).
    fn engine_over(
        feeds: &[(String, String)],
        ttl_seconds: u64,
        max_concurrent: usize,
    ) -> RssEngine {
        let cache = Arc::new(MemoryFeedCache::new(CACHE_MAX_BYTES, feeds.len() + 8));
        engine_with_cache(feeds, ttl_seconds, max_concurrent, cache)
    }

    /// A `FeedCache` that answers "expired" for every feed while delegating
    /// every write, and every other part of the read, to a real
    /// [`MemoryFeedCache`].
    ///
    /// Needed because a failure arms to `failure_fuse(ttl)`, at least 30
    /// seconds even at `ttl_seconds: 0`, so no configuration can get a test to
    /// a *third* attempt after a failed one. Overriding only the verdict keeps
    /// every state transition under test real — the window, the validators,
    /// and the statuses all come from the wrapped cache — without a sleep.
    struct AlwaysExpired(MemoryFeedCache);

    impl FeedCache for AlwaysExpired {
        fn snapshot(&self, feed: &str, now: Instant) -> FeedSnapshot {
            FeedSnapshot {
                within_ttl: false,
                ..self.0.snapshot(feed, now)
            }
        }

        fn record_success(
            &self,
            feed: &str,
            window: CachedWindow,
            observation: FeedObservation,
            armed_until: Instant,
        ) {
            self.0
                .record_success(feed, window, observation, armed_until);
        }

        fn record_not_modified(
            &self,
            feed: &str,
            http_status: u16,
            last_fetch_ms: i64,
            armed_until: Instant,
        ) {
            self.0
                .record_not_modified(feed, http_status, last_fetch_ms, armed_until);
        }

        fn record_failure(
            &self,
            feed: &str,
            http_status: Option<u16>,
            error: String,
            dialect_declared: Option<String>,
            last_fetch_ms: i64,
            armed_until: Instant,
        ) {
            self.0.record_failure(
                feed,
                http_status,
                error,
                dialect_declared,
                last_fetch_ms,
                armed_until,
            );
        }
    }

    fn engine_with_cache(
        feeds: &[(String, String)],
        ttl_seconds: u64,
        max_concurrent: usize,
        cache: Arc<dyn FeedCache>,
    ) -> RssEngine {
        let subscriptions: Vec<ResolvedSubscription> = feeds
            .iter()
            .map(|(name, url)| ResolvedSubscription {
                name: name.clone(),
                url: url.clone(),
            })
            .collect();
        let mut config = inline_config(
            subscriptions
                .iter()
                .map(|sub| FeedSubscription {
                    url: sub.url.clone(),
                    name: Some(sub.name.clone()),
                })
                .collect(),
        );
        config.ttl_seconds = ttl_seconds;
        config.max_concurrent = max_concurrent;
        config.request_timeout_seconds = 5;
        let fetcher = FeedFetcher::new(
            Arc::new(EgressPolicy::allowing_loopback_for_tests()),
            Duration::from_secs(config.request_timeout_seconds),
            config.max_response_bytes,
            config.user_agent.clone(),
        )
        .expect("build the test fetcher");
        RssEngine::with_parts(
            "rss_test".to_string(),
            subscriptions,
            &config,
            fetcher,
            cache,
        )
    }

    fn u64_col(batch: &RecordBatch, name: &str) -> Vec<Option<u64>> {
        let index = batch.schema().index_of(name).expect("column exists");
        let column = batch
            .column(index)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("column is UInt64");
        (0..column.len())
            .map(|row| column.is_valid(row).then(|| column.value(row)))
            .collect()
    }

    #[tokio::test]
    async fn fresh_fetch_parses_and_stamps_fresh() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        let engine = test_engine(&server, &[("a", "/f.xml")], 900);

        let batch = engine.serve_feed("a", || true).await.expect("rows served");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(str_col(&batch, "window_status"), vec!["fresh"]);
        assert_eq!(str_col(&batch, "feed"), vec!["a"]);
        assert_eq!(server.requests().len(), 1);

        // Second serve within TTL: zero additional network.
        let again = engine.serve_feed("a", || true).await.expect("rows served");
        assert_eq!(str_col(&again, "window_status"), vec!["fresh"]);
        assert_eq!(server.requests().len(), 1);
    }

    #[tokio::test]
    async fn expired_with_etag_takes_304_and_stamps_revalidated() {
        let server = MockFeedServer::start(|req| {
            if req.header("if-none-match").is_some() {
                MockResponse::status(304)
            } else {
                MockResponse::xml(RSS2_MINIMAL).with_header("etag", "\"v1\"")
            }
        })
        .await;
        let engine = test_engine(&server, &[("a", "/f.xml")], 0); // always-live

        engine.serve_feed("a", || true).await.expect("first serve");
        let batch = engine.serve_feed("a", || true).await.expect("second serve");
        assert_eq!(str_col(&batch, "window_status"), vec!["revalidated"]);
        assert_eq!(batch.num_rows(), 1, "the cached window is still served");
        assert_eq!(server.requests().len(), 2);

        let row = engine.feeds_row("a");
        assert_eq!(str_col(&row, "last_status"), vec!["revalidated"]);
        assert_eq!(str_opt_col(&row, "etag"), vec![Some("\"v1\"".to_string())]);
    }

    #[tokio::test]
    async fn failed_refetch_serves_stale_rows_and_records_error() {
        let hits = Arc::new(AtomicUsize::new(0));
        let h = Arc::clone(&hits);
        let server = MockFeedServer::start(move |_| {
            if h.fetch_add(1, Ordering::SeqCst) == 0 {
                MockResponse::xml(RSS2_MINIMAL)
            } else {
                MockResponse::status(500)
            }
        })
        .await;
        let engine = test_engine(&server, &[("a", "/f.xml")], 0);

        engine.serve_feed("a", || true).await.expect("first serve");
        // 500 on every retry → the cached window is served stale.
        let batch = engine.serve_feed("a", || true).await.expect("stale rows");
        assert_eq!(str_col(&batch, "window_status"), vec!["stale-error"]);
        assert_eq!(batch.num_rows(), 1);

        let row = engine.feeds_row("a");
        assert_eq!(str_col(&row, "last_status"), vec!["stale-error"]);
        let error = str_opt_col(&row, "last_error")[0]
            .clone()
            .expect("last_error recorded");
        assert!(
            error.contains("500"),
            "last_error names the status: {error}"
        );

        // Negative cache: an immediate third serve does not re-poke the dead
        // feed, and reading its health issues no request either. That third
        // serve goes through the cache-hit path, which must stamp the *stored*
        // status rather than the `fresh` the window was built with.
        let n = server.requests().len();
        let cached = engine
            .serve_feed("a", || true)
            .await
            .expect("stale rows again, from the cache");
        assert_eq!(
            str_col(&cached, "window_status"),
            vec!["stale-error"],
            "the cache-hit path stamps the stored status, not the batch's build-time label"
        );
        engine.feeds_row("a");
        assert_eq!(server.requests().len(), n);
    }

    /// A `304` after a failure clears the error and restores `revalidated`
    /// while keeping the validators that earned the `304`. Needs three
    /// attempts against one feed, which `failure_fuse`'s 30s floor rules out
    /// by configuration — hence [`AlwaysExpired`].
    #[tokio::test]
    async fn stale_error_then_304_returns_to_revalidated_and_clears_the_error() {
        let hits = Arc::new(AtomicUsize::new(0));
        let h = Arc::clone(&hits);
        let server = MockFeedServer::start(move |req| {
            match h.fetch_add(1, Ordering::SeqCst) {
                // The window and its etag.
                0 => MockResponse::xml(RSS2_MINIMAL).with_header("etag", "\"v1\""),
                // Every attempt of the second serve (MAX_ATTEMPTS is 3).
                1..=3 => MockResponse::status(500),
                _ if req.header("if-none-match").is_some() => MockResponse::status(304),
                // A 304 here would be unconditional; fail loudly instead.
                _ => MockResponse::status(400),
            }
        })
        .await;
        let urls = vec![("a".to_string(), format!("{}/f.xml", server.url()))];
        let cache = Arc::new(AlwaysExpired(MemoryFeedCache::new(CACHE_MAX_BYTES, 8)));
        let engine = engine_with_cache(&urls, 900, 4, cache);

        engine.serve_feed("a", || true).await.expect("first serve");
        let stale = engine.serve_feed("a", || true).await.expect("stale rows");
        assert_eq!(str_col(&stale, "window_status"), vec!["stale-error"]);
        assert!(str_opt_col(&engine.feeds_row("a"), "last_error")[0].is_some());

        let revalidated = engine.serve_feed("a", || true).await.expect("304 serve");
        assert_eq!(str_col(&revalidated, "window_status"), vec!["revalidated"]);
        assert_eq!(revalidated.num_rows(), 1);

        let row = engine.feeds_row("a");
        assert_eq!(str_col(&row, "last_status"), vec!["revalidated"]);
        assert_eq!(
            str_opt_col(&row, "last_error"),
            vec![None],
            "a successful revalidation clears the previous failure's error"
        );
        assert_eq!(
            str_opt_col(&row, "etag"),
            vec![Some("\"v1\"".to_string())],
            "the validators that earned the 304 survive it"
        );
    }

    /// The politeness permit is released when a serve is cancelled mid-fetch,
    /// not just when it completes — otherwise one cancelled scan would park
    /// every later fetch of that source forever.
    #[tokio::test]
    async fn a_cancelled_serve_releases_the_politeness_permit() {
        let server = MockFeedServer::start(|_| {
            MockResponse::xml(RSS2_MINIMAL).with_delay(Duration::from_millis(300))
        })
        .await;
        let urls = vec![
            ("a".to_string(), format!("{}/a.xml", server.url())),
            ("b".to_string(), format!("{}/b.xml", server.url())),
        ];
        // One permit, so `b` can only proceed if `a` gave its permit back.
        let engine = engine_over(&urls, 900, 1);

        let cancelled =
            tokio::time::timeout(Duration::from_millis(30), engine.serve_feed("a", || true)).await;
        assert!(
            cancelled.is_err(),
            "the serve must still have been in flight when the timeout dropped it"
        );
        // A leaked permit makes this hang rather than fail, so bound it.
        let served = tokio::time::timeout(Duration::from_secs(10), engine.serve_feed("b", || true))
            .await
            .expect("b acquired the permit a released on cancellation");
        assert!(served.is_some());
        assert_eq!(
            str_col(&engine.feeds_row("b"), "last_status"),
            vec!["fresh"]
        );
        // The cancelled serve wrote no health state: it was dropped before it
        // could record anything.
        assert_eq!(
            str_col(&engine.feeds_row("a"), "last_status"),
            vec!["never"]
        );
    }

    #[tokio::test]
    async fn never_fetched_failure_yields_zero_rows_and_error_status() {
        let server = MockFeedServer::start(|_| MockResponse::status(500)).await;
        let engine = test_engine(&server, &[("a", "/f.xml")], 900);

        assert!(engine.serve_feed("a", || true).await.is_none());
        let row = engine.feeds_row("a");
        assert_eq!(str_col(&row, "last_status"), vec!["error"]);
        assert_eq!(u64_col(&row, "item_count"), vec![None]);

        // The failure armed the TTL, so reading health does not re-poke it.
        let n = server.requests().len();
        engine.feeds_row("a");
        assert_eq!(server.requests().len(), n);
    }

    #[tokio::test]
    async fn feeds_row_before_any_scan_is_never_and_issues_no_requests() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        let engine = test_engine(&server, &[("a", "/f.xml")], 900);

        let row = engine.feeds_row("a");
        assert_eq!(row.num_rows(), 1);
        assert_eq!(str_col(&row, "last_status"), vec!["never"]);
        assert_eq!(str_col(&row, "name"), vec!["a"]);
        assert_eq!(str_opt_col(&row, "last_error"), vec![None]);
        assert_eq!(server.requests().len(), 0);
    }

    #[tokio::test]
    async fn parse_failure_records_stage_and_declared_dialect() {
        // Declares rss-2.0 and then stops mid-element: the sniff succeeds,
        // the parse cannot.
        let server = MockFeedServer::start(|_| {
            MockResponse::xml("<rss version=\"2.0\"><channel><title>truncat")
        })
        .await;
        let engine = test_engine(&server, &[("a", "/f.xml")], 900);

        assert!(engine.serve_feed("a", || true).await.is_none());
        let row = engine.feeds_row("a");
        assert_eq!(str_col(&row, "last_status"), vec!["error"]);
        let error = str_opt_col(&row, "last_error")[0]
            .clone()
            .expect("last_error recorded");
        assert!(
            error.contains("parse failed at strict-parse"),
            "last_error names the stage: {error}"
        );
        assert_eq!(
            str_opt_col(&row, "dialect_declared"),
            vec![Some("rss-2.0".to_string())],
            "the declared-dialect sniff survives a failed parse"
        );
    }

    /// `last_error` must carry no response-body content. The 512-char cap
    /// bounds length but does not redact, so the guarantee rests on the parse
    /// error never echoing character data — pinned here end to end for both
    /// shapes that could carry it.
    ///
    /// Each body carries the sentinel in body content, in a shape chosen to
    /// reach a different error path. Whether a given body fails to parse is
    /// feed-rs's business and may change between versions; the invariant is that
    /// if a reason lands in `last_error`, the sentinel is not in it and the
    /// stored string respects the cap.
    #[tokio::test]
    async fn parse_failure_last_error_never_echoes_character_data() {
        const SENTINEL: &str = "SHOULD-NOT-LEAK";
        let bodies = [
            // Truncated mid-element, sentinel as ordinary character data.
            // Reaches `SyntaxError::UnclosedTag`.
            "<rss version=\"2.0\"><channel><title>SHOULD-NOT-LEAK secret prose",
            // Truncated, sentinel shaped as an undefined entity reference —
            // the input `EscapeError::UnrecognizedEntity` would quote verbatim.
            // Reaches `UnclosedTag` too: the structural failure comes first.
            "<rss version=\"2.0\"><channel><title>&SHOULD-NOT-LEAK; truncat",
            // Sentinel adjacent to an out-of-range character reference in
            // character data. This is the shape that reaches
            // `EscapeError::InvalidCharRef`, the only escape error observed
            // reaching this column — the position matters, since the same
            // reference inside `<title>` is swallowed per-element and yields no
            // error at all.
            concat!(
                r#"<rss version="2.0"><channel>SHOULD-NOT-LEAK &#x110000;"#,
                r#"<title>t</title><link>https://e.example/</link>"#,
                r#"<description>d</description></channel></rss>"#,
            ),
            // Well-formed, so nothing fails today: feed-rs 2.4.0 reads an
            // undefined entity back as literal text and reports no error at
            // all. Kept as a live guard — the day a future feed-rs starts
            // reporting escape errors for element text, this body produces one.
            concat!(
                r#"<rss version="2.0"><channel><title>&SHOULD-NOT-LEAK;</title>"#,
                r#"<link>https://e.example/</link><description>d</description>"#,
                r#"</channel></rss>"#,
            ),
        ];

        let mut errors_seen = 0;
        for body in bodies {
            let server = MockFeedServer::start(move |_| MockResponse::xml(body)).await;
            let engine = test_engine(&server, &[("a", "/f.xml")], 900);

            engine.serve_feed("a", || true).await;
            if let Some(error) = str_opt_col(&engine.feeds_row("a"), "last_error")[0].clone() {
                errors_seen += 1;
                assert!(
                    !error.contains(SENTINEL),
                    "response body content reached last_error for {body:?}: {error}"
                );
                assert!(
                    error.chars().count() <= MAX_ERROR_CHARS,
                    "stored error exceeds the stated cap: {}",
                    error.chars().count()
                );
            }
        }
        // Guards the loop against going quietly vacuous: three of the four
        // bodies must still reach the failure path for the assertions above to
        // mean anything. If this count moves, a shape changed error family and
        // the new reason needs re-checking by hand.
        assert_eq!(
            errors_seen, 3,
            "expected the two truncated bodies and the bad character reference to record \
             an error, and the well-formed one not to"
        );
    }

    /// The one documented exception to "no body text in `last_error`": feed-rs
    /// interpolates a JSON Feed's declared `version` when it does not recognise
    /// it, and that string is a member value out of the document.
    ///
    /// Pinned from the exception's side deliberately. The audit claims exactly
    /// one carve-out, and a test that only asserted the absence of leaks would
    /// pass just as happily if this one silently widened — or if someone
    /// "fixed" it without updating the audit.
    #[tokio::test]
    async fn json_unsupported_version_is_the_one_body_text_kept_in_last_error() {
        let body = r#"{"version":"SHOULD-NOT-LEAK-1.9","title":"t","items":[]}"#;
        let server = MockFeedServer::start(move |_| {
            MockResponse::new(200, body.as_bytes().to_vec())
                .with_header("content-type", "application/json")
        })
        .await;
        let engine = test_engine(&server, &[("a", "/f.json")], 900);

        assert!(engine.serve_feed("a", || true).await.is_none());
        let error = str_opt_col(&engine.feeds_row("a"), "last_error")[0]
            .clone()
            .expect("an unsupported version is a parse failure");
        assert!(
            error.contains("unsupported version: SHOULD-NOT-LEAK-1.9"),
            "the declared version is kept, because the error is undiagnosable \
             without it: {error}"
        );
        assert!(error.chars().count() <= MAX_ERROR_CHARS);
    }

    /// The exception is bounded by the same cap as everything else, so an
    /// absurdly long declared version cannot turn one field into an unbounded
    /// write.
    #[tokio::test]
    async fn a_huge_json_version_is_still_capped() {
        let body = format!(
            r#"{{"version":"{}","title":"t","items":[]}}"#,
            "v".repeat(20_000)
        );
        let server = MockFeedServer::start(move |_| {
            MockResponse::new(200, body.clone().into_bytes())
                .with_header("content-type", "application/json")
        })
        .await;
        let engine = test_engine(&server, &[("a", "/f.json")], 900);

        assert!(engine.serve_feed("a", || true).await.is_none());
        let error = str_opt_col(&engine.feeds_row("a"), "last_error")[0]
            .clone()
            .expect("last_error recorded");
        assert_eq!(error.chars().count(), MAX_ERROR_CHARS);
    }

    #[tokio::test]
    async fn egress_blocked_feed_degrades_like_unreachable() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        let engine = engine_over(
            &[("a".to_string(), "http://10.1.2.3/f".to_string())],
            900,
            4,
        );

        assert!(engine.serve_feed("a", || true).await.is_none());
        let row = engine.feeds_row("a");
        assert_eq!(str_col(&row, "last_status"), vec!["error"]);
        let error = str_opt_col(&row, "last_error")[0]
            .clone()
            .expect("last_error recorded");
        assert!(
            error.contains("egress blocked"),
            "last_error names the refusal: {error}"
        );
        assert_eq!(
            server.requests().len(),
            0,
            "nothing was connected to at all"
        );
    }

    #[tokio::test]
    async fn conformance_notes_land_in_feeds_row() {
        let naked_amp = concat!(
            r#"<rss version="2.0"><channel><title>Fish & Chips</title>"#,
            r#"<link>https://e.example/</link><description>d</description>"#,
            r#"<item><guid>g1</guid><title>t</title></item></channel></rss>"#,
        );
        let server = MockFeedServer::start(|_| MockResponse::xml(naked_amp)).await;
        let engine = test_engine(&server, &[("a", "/f.xml")], 900);

        engine.serve_feed("a", || true).await.expect("rows served");
        let notes = str_opt_col(&engine.feeds_row("a"), "conformance_notes")[0]
            .clone()
            .expect("notes recorded");
        assert!(
            notes.contains("sanitation: escaped-naked-ampersands"),
            "notes carry the repair: {notes}"
        );
    }

    #[tokio::test]
    async fn item_count_and_meta_populate() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        let engine = test_engine(&server, &[("a", "/f.xml")], 900);

        engine.serve_feed("a", || true).await.expect("rows served");
        let row = engine.feeds_row("a");
        assert_eq!(u64_col(&row, "item_count"), vec![Some(1)]);
        assert_eq!(
            str_opt_col(&row, "title"),
            vec![Some("Minimal Feed".to_string())]
        );
        assert_eq!(
            str_opt_col(&row, "site_url"),
            vec![Some("https://feed.example/".to_string())]
        );
        assert_eq!(
            str_opt_col(&row, "description"),
            vec![Some("A minimal feed.".to_string())]
        );
        assert_eq!(
            str_opt_col(&row, "dialect"),
            vec![Some("rss-2.0".to_string())]
        );
        assert_eq!(
            str_opt_col(&row, "conformance_notes"),
            vec![Some("[]".to_string())],
            "a clean feed records an empty note list, not NULL"
        );
    }

    #[tokio::test]
    async fn false_launch_gate_skips_fetch_and_health_write() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        // ttl 0: the feed is expired again the moment the first serve ends.
        let engine = test_engine(&server, &[("a", "/f.xml")], 0);

        engine.serve_feed("a", || true).await.expect("first serve");
        let before = engine.feeds_row("a");
        let requests = server.requests().len();

        assert!(
            engine.serve_feed("a", || false).await.is_none(),
            "a closed gate serves nothing"
        );
        assert_eq!(server.requests().len(), requests, "and fetches nothing");
        let after = engine.feeds_row("a");
        assert_eq!(
            str_col(&after, "last_status"),
            str_col(&before, "last_status"),
            "health is untouched: neither fetched nor health-refreshed"
        );
        assert_eq!(
            str_opt_col(&after, "last_error"),
            str_opt_col(&before, "last_error")
        );
        assert_eq!(
            u64_col(&after, "item_count"),
            u64_col(&before, "item_count")
        );
    }

    #[tokio::test]
    async fn false_gate_still_serves_within_ttl_cache() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        let engine = test_engine(&server, &[("a", "/f.xml")], 900);

        engine.serve_feed("a", || true).await.expect("first serve");
        let batch = engine
            .serve_feed("a", || false)
            .await
            .expect("a cache hit has no side effect to gate");
        assert_eq!(str_col(&batch, "window_status"), vec!["fresh"]);
        assert_eq!(server.requests().len(), 1);
    }

    /// The gate is re-read *after* the politeness permit, not before the
    /// wait — a pre-acquire-only check cannot stop a launch once the LIMIT
    /// is filled, because every partition passes it while nothing has been
    /// emitted yet and then queues.
    ///
    /// Pinned with one permit and two feeds. `#[tokio::test]` builds a
    /// current-thread runtime (tokio-macros 2.7.0, `src/entry.rs:91`:
    /// `default_flavor` is `CurrentThread` when `is_test`), so no other task
    /// — the mock server's accept loop included — can run while this task is
    /// being polled, and `biased;` makes `join!` poll top to bottom
    /// (documented in tokio's `join!`: it "will cause `join` to poll the
    /// futures in the order they appear from top to bottom"). So the order
    /// is fixed: `a` takes the only permit and parks in its fetch, `b` is
    /// polled next and parks on the semaphore with the gate still open, and
    /// only then does the server handler run and close the gate. A
    /// pre-acquire-only implementation would have let `b` through and
    /// fetched `/b.xml`.
    #[tokio::test]
    async fn launch_gate_is_rechecked_after_acquiring_the_permit() {
        let gate_open = Arc::new(AtomicBool::new(true));
        let closer = Arc::clone(&gate_open);
        let server = MockFeedServer::start(move |_| {
            // Serving one feed is enough to fill the LIMIT: close the gate
            // while `a` still holds the only permit.
            closer.store(false, Ordering::SeqCst);
            MockResponse::xml(RSS2_MINIMAL).with_delay(Duration::from_millis(50))
        })
        .await;
        let urls = vec![
            ("a".to_string(), format!("{}/a.xml", server.url())),
            ("b".to_string(), format!("{}/b.xml", server.url())),
        ];
        let engine = engine_over(&urls, 900, 1);

        let gate = Arc::clone(&gate_open);
        let (first, second) = tokio::join!(
            biased;
            engine.serve_feed("a", || true),
            engine.serve_feed("b", move || gate.load(Ordering::SeqCst)),
        );

        assert!(first.is_some(), "a held the permit and fetched");
        assert!(
            second.is_none(),
            "b's gate closed while it queued for the permit"
        );
        let paths: Vec<String> = server
            .requests()
            .iter()
            .map(|req| req.path.clone())
            .collect();
        assert_eq!(
            paths,
            vec!["/a.xml".to_string()],
            "b must never have been fetched"
        );
        // And b's health is untouched, exactly like a pruned partition.
        assert_eq!(
            str_col(&engine.feeds_row("b"), "last_status"),
            vec!["never"]
        );
    }

    /// Feed-key discipline: a name that is not a configured subscription
    /// resolves to nothing, so no query-influenced string can ever become a
    /// cache key.
    #[tokio::test]
    async fn unknown_feed_name_serves_nothing_and_reaches_no_state() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        let engine = test_engine(&server, &[("a", "/f.xml")], 900);

        assert!(engine.serve_feed("' OR 1=1 --", || true).await.is_none());
        assert_eq!(engine.feeds_row("' OR 1=1 --").num_rows(), 0);
        assert_eq!(server.requests().len(), 0);
    }

    /// The stored value, prefix included, respects the cap — a reason bounded
    /// on its own would leave the column at `512 + prefix`.
    #[test]
    fn parse_error_message_caps_the_composed_string_not_just_the_reason() {
        let message = parse_error_message("strict-parse", &"x".repeat(4_000));
        assert_eq!(message.chars().count(), MAX_ERROR_CHARS);
        assert!(message.starts_with("parse failed at strict-parse: "));
        // A short reason is untouched.
        assert_eq!(
            parse_error_message("refused-internal-dtd", "internal DTD subset refused"),
            "parse failed at refused-internal-dtd: internal DTD subset refused"
        );
    }

    #[test]
    fn truncate_bounds_length_on_char_boundaries() {
        let long = "é".repeat(1_000);
        let cut = truncate(&long, MAX_ERROR_CHARS);
        assert_eq!(cut.chars().count(), MAX_ERROR_CHARS);
        assert_eq!(truncate("short", MAX_ERROR_CHARS), "short");
    }
}
