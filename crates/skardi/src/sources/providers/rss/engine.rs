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
//! ## A within-TTL entry is only a cache hit if it can still serve
//!
//! The TTL lives on the feed's observation and the rows live on its window, and
//! the cache can drop the second while keeping the first — window eviction is
//! specified to preserve the observation (`cache.rs:36-42`). So "within TTL"
//! and "has rows" are independent, and a feed can be both fresh and empty. That
//! combination is not served: `window_lost` classifies it as a miss so the
//! scan refetches, because the alternative is zero rows beside a `feeds` row
//! reporting `fresh` with a non-zero `item_count` and no error — the exact
//! state `cache.rs:216-223` argues is unacceptable. Pinned by
//! `within_ttl_serve_refetches_after_its_window_was_evicted`.
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
//! ## Parsing runs off the worker threads
//!
//! `parse_feed_document` is synchronous CPU over attacker-authored bytes.
//! [`parse_off_worker`] moves it to the blocking pool and fuses it with a
//! budget scaled from `max_response_bytes` ([`parse_fuse`]) — their docs
//! have the full account. The politeness permit, held across that await, is
//! what bounds how many parses run at once.
//!
//! ## Feed keys never come from a query
//!
//! `MemoryFeedCache` keys entries by `&str` and bounds observation-only
//! entries with a last-resort backstop, on the assumption that keys come from
//! the fixed, config-derived subscription list. This module is where that
//! assumption is enforced: `RssEngine::subscription` is the only way any
//! path here obtains a feed key, it resolves the caller's `&str` against
//! `self.subscriptions` (answering `None` for anything else), and every key
//! passed to the cache is the resolved `ResolvedSubscription::name` — a
//! string owned by this engine — never the argument the caller supplied. A
//! predicate value, a projection, or any other query-influenced string can
//! therefore only ever produce zero rows.
//!
//! ## What may reach `feeds.last_error`
//!
//! This module is the only writer of that column, and the column is read
//! straight into an agent's context by `sync`'s closing health report
//! (`docs/rss.md`), so what a feed can put in it is a prompt-content question,
//! not a tidiness one. [`MAX_ERROR_CHARS`] bounds its length; truncation is not
//! redaction, so the content question is separate.
//!
//! The property, stated as measured rather than as a wish:
//!
//! > A `feeds.last_error` value may quote a **feed-supplied token that sat in a
//! > structural position** — an element or attribute *name*, an attribute value
//! > the parser had to interpret (a `type`/MIME string), a declared version
//! > string, or the JSON member *value* that failed a type check. It never
//! > quotes a value the provider actually reads as content: the character data
//! > of an element, a `title`, a `description`, an entry body. The cap is the
//! > only bound on how long a quoted token may be.
//!
//! That is deliberate rather than accidental: those fragments are what make a
//! malformed feed diagnosable, and the plan's error-redaction decision keeps
//! them. But note what the second sentence does *not* say. A feed author who
//! wants arbitrary text of their choosing in this column can get it, by putting
//! that text in a structural position — an attribute value, or a JSON member
//! whose declared type it violates — and the cap is then the whole defence.
//! Measured: a ~1 KB string in a JSON Feed's `tags`, `authors`, `attachments`,
//! or `size_in_bytes` is quoted verbatim into the column up to
//! [`MAX_ERROR_CHARS`]. What is *not* reachable is the other thing: prose
//! sitting where prose belongs stays out even when the document fails to parse
//! for an unrelated reason.
//!
//! **The tests enforce this, not this comment.**
//! `parse_failure_last_error_quotes_structure_not_prose` runs a table of
//! document shapes, each declaring whether its sentinel is expected to reach the
//! column, and asserts that shape by shape in both directions — so a leak into a
//! prose slot fails, and so does a `Kept` fragment quietly disappearing or
//! widening. `json_unsupported_version_is_body_text_kept_in_last_error` and
//! `a_huge_json_version_is_still_capped` pin the version-string case and its
//! bound. Every prose explanation of this property so far has needed
//! correction — each time for a different reason, twice inside text written to
//! fix the previous one, and once more when a reviewer found four reachable
//! shapes the property had denied outright — while the tests were right every
//! time. If a dependency upgrade changes any of this they fail, and whoever sees
//! that should re-derive the situation rather than trust the paragraphs below.
//!
//! What was *measured*, at `feed-rs` 2.4.0 and the `quick-xml` 0.41.0 it
//! resolves to (`Cargo.lock`). This is the evidence behind the property today,
//! **not** a closed list of what a malformed document can produce — the shapes
//! below are the ones swept, and a shape nobody swept is not a shape nobody can
//! write:
//!
//! - **Element names.** A mismatched end tag reports quick-xml's ill-formed
//!   family and names both tags: `expected </entry>, but </X> was found`, where
//!   `X` is whatever the document wrote. Measured through a mismatch inside an
//!   `xhtml` `<content>` body too, so the shape is not confined to the feed's
//!   own elements.
//! - **Attribute values the parser interprets.** An Atom
//!   `<content type="X">` whose `X` is not a type feed-rs handles reports
//!   `unsupported content type X`. `X` is a raw attribute value.
//! - **JSON type-mismatch values.** `serde_json` renders the offending value in
//!   `invalid type: …, expected …`, and for a string it renders that string
//!   verbatim and unabbreviated — `invalid type: string "X", expected u64`.
//!   Reached from every JSON Feed member whose declared type a string violates.
//!   This is the widest of the channels and the reason the cap is load-bearing.
//! - **Declared version strings.** `ParseFeedError::JsonUnsupportedVersion`'s
//!   `Display` is `unsupported version: {version}`
//!   (`feed-rs-2.4.0/src/parser/mod.rs:66`, reached from
//!   `src/parser/json/mod.rs:29`) — a member value out of the document, kept
//!   because an unsupported version is undiagnosable without it.
//! - **Structural syntax errors carry no document text at all.** A truncated
//!   document reports `SyntaxError::UnclosedTag` — "tag not closed: `>` not
//!   found before end of input" (`quick-xml-0.41.0/src/errors.rs:71`), prefixed
//!   "syntax error: " by `Error`'s `Display` (`src/errors.rs:287`). An
//!   out-of-range character reference such as `&#x110000;` reports
//!   `EscapeError::InvalidCharRef`, which renders the parsed *number* alone —
//!   "`1114112` is not a valid codepoint" (`src/escape.rs:30`) — and nothing
//!   adjacent to it. These are the shapes that make character data in a failing
//!   document stay out of the column.
//! - **Why character data stays out.**
//!   `EscapeError::UnrecognizedEntity` is a variant that would interpolate a
//!   token lifted from the document (`src/escape.rs:66-68`). It was not observed
//!   reaching this column, and the check behind that is a grep over the one
//!   crate we pin rather than a claim about quick-xml's internals. Grepping
//!   `feed-rs-2.4.0/src` for every quick-xml unescaping and entity-resolution
//!   routine — `unescape`, `normalize_attr`, `decode_and_normalize`,
//!   `resolve_predefined_entity`, `resolve_char_ref` — returns three hits
//!   outside feed-rs's own tests, all in `src/xml/mod.rs`, and each is one of:
//!   `resolve_char_ref()?` at `:333`, which is the `InvalidCharRef` path above;
//!   `resolve_predefined_entity` at `:337`, which returns an `Option` and so
//!   raises nothing; and `unescape` at `:597`, whose error is discarded by
//!   `unwrap_or_else(|_| decoded_value.clone())`. Element text goes through the
//!   first two, not `unescape`: feed-rs resolves references itself and writes an
//!   unresolvable entity back into the text verbatim (`:333-345`), which is why
//!   an undefined entity in a title was measured to produce no error at all.
//!   quick-xml raises this variant from other routines too (`escape.rs:295`,
//!   `:807`, `de/mod.rs:2470`); the grep above is the statement that nothing
//!   here calls them.
//! - `FetchError`'s own strings — this crate's, so not a dependency
//!   question — are statuses, byte and second counts, and URLs: the configured
//!   feed URL, or a redirect `Location`. A `Location` is attacker-influenced
//!   but is not body content, and the cap bounds it.
//!
//! ## No in-flight coalescing, but generation-checked commits
//!
//! Two concurrent scans that both find the same feed expired can both fetch
//! it: fetches are query-driven, queries are not coordinated, and
//! `open_connector`'s cache set the same precedent. What a double fetch is
//! *not* allowed to do is corrupt the cache. Each commit carries the
//! generation of the snapshot its fetch was decided from, and a commit that
//! lost the race is dropped instead of applied in completion order —
//! `cache.rs`'s trait doc has the rules. Without that gate a slower, older
//! `200` could overwrite a newer window wholesale, a `304` could stamp
//! `revalidated` on a window its validators never came from, and a slow
//! failure could arm the fuse against a window a faster fetch just
//! refreshed.
//!
//! The alternative — per-feed *singleflight*, where the second scan awaits
//! the first's result instead of fetching — remains a deliberate
//! non-feature. Recorded here so a future decision starts from the
//! trade-offs rather than rediscovering them:
//!
//! - **What it buys**: no duplicate `GET` (politeness toward feed servers —
//!   the one cost the generation gate does not remove), and concurrent
//!   scans serving one agreed window instead of two legitimate reads.
//! - **What it costs**: an in-flight registry keyed by feed, and leader
//!   hand-off on cancellation — a leader dropped by its scan deadline or
//!   LIMIT gate must not strand its waiters, and waiter-becomes-leader
//!   under drop-at-any-await is easy to get subtly wrong. It also couples
//!   scans: one query's partition inherits the tail latency of another
//!   query's fetch.
//! - **When to revisit**: duplicate fetches showing up in feed-server logs
//!   or politeness complaints — not cache correctness, which the
//!   generation gate already owns.
//!
//! ## No per-host bound
//!
//! The semaphore this module holds is a **total** fetch-parallelism bound
//! for one source, and nothing here accounts per host: feeds that share a
//! host can receive up to `max_concurrent` concurrent requests. That is the
//! framing PR #180's review settled on — the spec's Fetcher section and its
//! YAML example were corrected to match — so the naming here says
//! "fetch-parallelism", not "politeness", wherever it defines the bound. A
//! `politeness permit` elsewhere in this module is the same object under its
//! operational name; it does not re-assert a per-host promise.
//!
//! Baseline host-level politeness rests instead on honoring `Retry-After`
//! (`fetch.rs`, capped by `MAX_RETRY_WAIT`) and on TTL pacing — a feed is
//! not refetched until its window expires.
//!
//! A proactive per-host cap is an **open decision, not a promise**, and this
//! semaphore is where it would go. What has to be settled before writing
//! any of it:
//!
//! - **Count by hostname?** Misses shared infrastructure: Substack,
//!   Feedburner and Cloudflare front thousands of distinct feed hostnames
//!   behind a few addresses, so a per-hostname cap would let all of them
//!   through and bound nothing that the far end actually experiences.
//! - **Count by resolved IP?** Tangles with the egress/DNS-resolver layer
//!   (`egress::PolicyDns` is where addresses become known, and a hostname's
//!   answer can change between fetches), and it throttles unrelated feeds
//!   that merely share a CDN address.
//! - **Count by inferred CDN/operator?** Needs a classification this
//!   provider has no source of truth for.
//!
//! Until one of those is chosen, no per-host guarantee should be advertised
//! in code, docs, or config comments. Revisit when a feed operator actually
//! complains, or when `429`s can be attributed to this source's own
//! concurrency rather than to its request rate.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::record_batch::RecordBatch;
use tokio::sync::Semaphore;

use super::ResolvedSubscription;
use super::cache::{
    CachedWindow, FeedCache, FeedObservation, FeedSnapshot, FeedStatus, MemoryFeedCache,
    failure_fuse,
};
use super::config::{DEFAULT_MAX_RESPONSE_BYTES, RssConfig};
use super::egress::EgressPolicy;
use super::error::{MAX_ERROR_CHARS, RssError, truncate};
use super::fetch::{FeedFetcher, FetchError, FetchOutcome, Validators};
use super::parse::{ParseFailure, ParsedDocument, parse_feed_document};
use super::schema::{FeedsRow, build_feeds_batch, build_items_batch, with_window_status};

/// Byte budget for the window cache, shared across every feed of one source.
pub const CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Headroom over the subscription count for the cache's window-entry bound,
/// so a source whose feeds all hold a window at once is not evicting on the
/// steady state.
const WINDOW_ENTRY_HEADROOM: usize = 8;

/// Ceiling on the configured TTL. `RssConfig` puts no upper bound on
/// `ttl_seconds`, and the TTL becomes the `Duration` added to an `Instant`
/// on every arm; `std`'s own `Instant` docs warn that a large enough add
/// "panics on macOS" and that `Add<Duration> for Instant` "may panic if the
/// resulting point in time cannot be represented". A year is longer than any
/// meaningful feed TTL, so clamping here costs nothing and keeps the arming
/// arithmetic in a range the platform can represent.
const MAX_TTL: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// Base of one feed's parse fuse: ten seconds of parse budget per
/// [`DEFAULT_MAX_RESPONSE_BYTES`] of licensed body. [`parse_fuse`] scales it
/// by the configured `max_response_bytes`, so the time budget tracks the
/// input budget instead of silently diverging from it.
///
/// [`parse_off_worker`] runs the parse on the blocking pool, so a slow parse
/// no longer pins a runtime worker — but the partition still awaits the
/// result, and without a fuse a pathological document would hold its
/// partition (and its politeness permit) for the rest of the scan deadline.
/// Parsing is linear in a capped body (internal DTDs are refused, HTML
/// deeper than `convert::MAX_HTML_DEPTH` degrades to tag-stripping), and a
/// legitimate document at the *default* cap parses in well under a second —
/// so ten seconds per unit is not a tuning knob but headroom: anything that
/// reaches the fuse is hostile, broken, or a super-linear corner in the
/// parsing stack this crate does not own. Negative caching
/// ([`failure_fuse`]) then keeps the offender from being re-parsed on every
/// scan.
const PARSE_TIMEOUT: Duration = Duration::from_secs(10);

/// Ceiling on the scaled parse fuse. Past an hour — the exec layer's own
/// ceiling on the scan timeout — a fuse protects nothing, and an absurd
/// `max_response_bytes` must not manufacture an absurd duration out of
/// [`parse_fuse`]'s multiplication.
const MAX_PARSE_FUSE: Duration = Duration::from_secs(60 * 60);

/// One feed's parse fuse: [`PARSE_TIMEOUT`] per
/// [`DEFAULT_MAX_RESPONSE_BYTES`] unit of the configured
/// `max_response_bytes`, partial units rounded up, capped at
/// [`MAX_PARSE_FUSE`].
///
/// The ten-second base was calibrated against the default body cap. The
/// cap itself is operator-raisable with no upper bound, and an operator who
/// raises it has licensed proportionally more parse *work* — parse is
/// linear in the body — so a constant fuse would misfire on exactly the
/// larger documents the raised cap now permits. Scaling the time budget
/// with the input budget keeps the fuse meaning what it meant at the
/// default: generous headroom over any legitimate document.
fn parse_fuse(max_response_bytes: u64) -> Duration {
    let units = max_response_bytes
        .div_ceil(DEFAULT_MAX_RESPONSE_BYTES)
        .max(1);
    Duration::from_secs(PARSE_TIMEOUT.as_secs().saturating_mul(units)).min(MAX_PARSE_FUSE)
}

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
    /// Total fetch-parallelism bound (not per-host): at most
    /// `max_concurrent` of this source's feeds in flight, and the queue a
    /// closed launch gate cancels a feed out of. One engine is built per
    /// registered source, so this bounds a source — not a process, and not a
    /// host. See the module doc's "No per-host bound" for why the accurate
    /// label matters here and what a real per-host cap would have to settle
    /// first.
    semaphore: Arc<Semaphore>,
    ttl: Duration,
    scan_timeout: Duration,
    /// One parse's wait budget, scaled from `max_response_bytes` — see
    /// [`parse_fuse`].
    parse_fuse: Duration,
}

impl RssEngine {
    /// Build an engine over `subscriptions`, with its own fetcher and
    /// in-memory window cache sized from the subscription count.
    pub fn new(
        source_name: String,
        subscriptions: Vec<ResolvedSubscription>,
        config: &RssConfig,
        policy: Arc<dyn EgressPolicy>,
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
        // `RssConfig::validate` rejects `max_concurrent: 0`; the floor keeps
        // a directly constructed config from producing a semaphore that
        // parks every fetch forever. The ceiling is tokio's own:
        // `Semaphore::new` panics above `Semaphore::MAX_PERMITS`
        // (tokio-1.52.3 `src/sync/batch_semaphore.rs:130`, the assert at
        // `:142-144`), and validation deliberately puts no upper bound on
        // the field — so without this clamp a large-but-valid YAML integer
        // would abort registration with a panic instead of degrading to a
        // value beyond any real fleet's needs. Same clamp-and-warn
        // treatment as `ttl_seconds` above; the ceiling is deliberately
        // *not* the subscription count, because permits are shared across
        // concurrent scans, so feed count does not bound useful concurrency.
        let max_concurrent = config.max_concurrent.clamp(1, Semaphore::MAX_PERMITS);
        if config.max_concurrent > Semaphore::MAX_PERMITS {
            tracing::warn!(
                source = %source_name,
                configured_max_concurrent = config.max_concurrent,
                effective_max_concurrent = max_concurrent,
                "rss max_concurrent clamped to the semaphore's ceiling"
            );
        }
        let scan_timeout = Duration::from_secs(config.scan_timeout_seconds);
        let parse_fuse = parse_fuse(config.max_response_bytes);
        if parse_fuse >= scan_timeout {
            // The parse fuse exists to fail *diagnosably* (degrade +
            // `last_error`) before the scan deadline fails tracelessly (a
            // deadline drop writes no health state). A `max_response_bytes`
            // large enough to push the fuse past `scan_timeout_seconds`
            // inverts that order: parse timeouts will surface as deadline
            // drops instead. Warn rather than clamp — the operator's remedy
            // is raising `scan_timeout_seconds` to match the body budget
            // they configured, not a shorter fuse misfiring on the
            // legitimate documents that budget now permits.
            tracing::warn!(
                source = %source_name,
                parse_fuse_seconds = parse_fuse.as_secs(),
                scan_timeout_seconds = config.scan_timeout_seconds,
                "rss parse fuse meets or exceeds the scan timeout; raise scan_timeout_seconds"
            );
        }
        Self {
            source_name,
            subscriptions,
            by_name,
            fetcher,
            cache,
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            ttl,
            scan_timeout,
            parse_fuse,
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
        // One record per serve. This `debug` line is the only place a feed
        // URL is logged — a subscription URL can carry a private query
        // token, so it stays out of `info`-and-above events (the degraded
        // `warn` names the feed, not the URL). Response bodies are never
        // logged, and `bytes`/`rows` describe the body without quoting it.
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

        if snapshot.within_ttl && !window_lost(&snapshot) {
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

        // Total fetch parallelism for *this source*: at most
        // `max_concurrent` feeds in flight. Not per host and not per process
        // — see the field's doc.
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

        // The validators live on the window, so a feed that reached here
        // through [`window_lost`] has none to send and this is an
        // unconditional `GET`. That is the right request to make: there is no
        // cached copy left for a `304` to confirm, so a conditional one could
        // only answer "still current" about rows this process no longer holds
        // — the state `record_not_modified` has to record as an error
        // (`cache.rs:561-577`). A full body is what actually refills the
        // window.
        let validators = snapshot.window.as_ref().map(|window| Validators {
            etag: window.etag.clone(),
            last_modified: window.last_modified.clone(),
        });

        match self.fetcher.fetch(&sub.url, validators.as_ref()).await {
            Ok(FetchOutcome::NotModified { http_status }) => {
                self.cache.record_not_modified(
                    &sub.name,
                    snapshot.generation,
                    http_status,
                    now_ms(),
                    arm(Instant::now(), self.ttl),
                );
                // The `304` confirms exactly the window whose validators this
                // attempt sent, which is the one in `snapshot` — read before
                // the permit, so a concurrent scan may have committed a
                // different window in between. The generation carried above
                // makes this commit a no-op in that case: `revalidated` must
                // not be stamped on rows this `304` never saw (the cache
                // trait's commit-generation doc has the rules). This serve
                // still emits its own snapshot's rows either way — they are a
                // real window the `304` really did vouch for, and both
                // windows are legitimate reads of the feed.
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
                match parse_off_worker(body, content_type, self.parse_fuse).await {
                    Ok(document) => {
                        let notes = document.conformance_notes.len();
                        let batch = self.record_fresh_window(
                            sub,
                            snapshot.generation,
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
                        snapshot.generation,
                        Some(http_status),
                        parse_error_message(failure.stage, &failure.reason),
                        failure.dialect_declared,
                        bytes,
                    ),
                }
            }
            // An egress refusal is a policy verdict, not a transient fault:
            // it purges the cached window instead of serving it stale, so
            // the refused subscription contributes zero item rows from this
            // serve on — see `deny` and `record_egress_denial`.
            Err(FetchError::Egress(denied)) => self.deny(
                sub,
                snapshot.generation,
                truncate(&denied.to_string(), MAX_ERROR_CHARS),
            ),
            Err(error) => {
                let http_status = match &error {
                    FetchError::Status { status } => Some(*status),
                    _ => None,
                };
                self.degrade(
                    sub,
                    snapshot.generation,
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
    ///
    /// The batch is built — and returned — before the cache commit, on
    /// purpose: `record_success` may drop the commit as stale (the cache
    /// trait's commit-generation doc), but this serve's fetch was a
    /// legitimate read of the feed either way, so its own query is still
    /// answered from what it fetched.
    fn record_fresh_window(
        &self,
        sub: &ResolvedSubscription,
        expected_generation: u64,
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
            expected_generation,
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
        expected_generation: u64,
        http_status: Option<u16>,
        error: String,
        dialect_declared: Option<String>,
        bytes: usize,
    ) -> (Option<RecordBatch>, ServeLog) {
        self.cache.record_failure(
            &sub.name,
            expected_generation,
            http_status,
            error.clone(),
            dialect_declared,
            now_ms(),
            arm(Instant::now(), failure_fuse(self.ttl)),
        );
        self.degraded_serve(sub, http_status, error, bytes)
    }

    /// Record an egress refusal and serve the aftermath. Unlike [`degrade`],
    /// the refusal purges the cached window ([`FeedCache::record_egress_denial`]
    /// has the full argument), so the read-back in [`degraded_serve`] finds
    /// `Error` and no window — zero rows, whatever the cache held before,
    /// which is what the design requires of a policy refusal (its
    /// failure-mode table and acceptance criterion 15). The fuse still arms:
    /// a refusal is negative-cached exactly like any other failure.
    fn deny(
        &self,
        sub: &ResolvedSubscription,
        expected_generation: u64,
        error: String,
    ) -> (Option<RecordBatch>, ServeLog) {
        self.cache.record_egress_denial(
            &sub.name,
            expected_generation,
            error.clone(),
            now_ms(),
            arm(Instant::now(), failure_fuse(self.ttl)),
        );
        self.degraded_serve(sub, None, error, 0)
    }

    /// The shared tail of [`degrade`] and [`deny`]: the warning, then the
    /// post-commit read-back that decides what — if anything — is still
    /// serveable.
    fn degraded_serve(
        &self,
        sub: &ResolvedSubscription,
        http_status: Option<u16>,
        error: String,
        bytes: usize,
    ) -> (Option<RecordBatch>, ServeLog) {
        // No `url` field: a subscription URL can carry a private query
        // token, and this event fires at `warn` — a level ordinary
        // deployments export. `source` + `feed` locate the subscription;
        // the URL is one `feeds.url` lookup away.
        tracing::warn!(
            source = %self.source_name,
            feed = %sub.name,
            %error,
            "rss feed degraded"
        );

        // Read back after recording: the cache commit is what decides
        // between `StaleError` (a window survived to serve) and `Error`
        // (none did — either none existed, or an egress denial purged it),
        // so the status stamped on the rows comes from the cache rather than
        // from a second guess here. If the commit was dropped as stale
        // (the cache trait's commit-generation doc), the read-back returns
        // whatever the intervening commit left — e.g. a concurrent fetch's
        // fresh window — and serving that freshest known state is exactly
        // right for a serve whose own attempt failed.
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
            last_status: observation.last_status,
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

/// Whether `snapshot`'s observation claims a servable window that is no longer
/// there — the one shape in which a within-TTL entry must *not* short-circuit
/// the network.
///
/// [`FeedStatus::window_status_str`] is the discriminator, and it is exactly the
/// right one: it answers `Some` for `Fresh`/`Revalidated`/`StaleError` and
/// `None` for `Never`/`Error` (`cache.rs:136-143`). So `Some` with no window
/// means the two halves of the entry disagree — the health record says rows are
/// serveable, and there are none.
///
/// That state is reachable with no caller mistake. `MemoryFeedCache` evicts a
/// window without its observation (`cache.rs:372-383`, deliberately — see
/// `cache.rs:36-42`), driven by *other* feeds' `record_success` calls against
/// the byte or entry budget, and nothing about that eviction touches the
/// evicted feed's TTL. `record_success` also declines to store a window that
/// alone exceeds `max_bytes` while still recording the observation
/// (`cache.rs:521-530`), which lands in the same place.
///
/// Short-circuiting there would serve zero rows silently while `feeds` reported
/// the feed `fresh` with a non-zero `item_count` and a NULL `last_error`, for
/// the rest of the TTL — precisely the combination
/// [`FeedCache::record_not_modified`]'s doc rejects on the `304` path
/// (`cache.rs:216-223`): "`feeds` reporting a healthy feed with a non-zero
/// `item_count` while `items` returns nothing, and no column anywhere
/// explaining why". Treating it as a miss costs one unconditional request and
/// gets the data back on this scan, rather than leaving a capture gap until the
/// TTL expires.
///
/// `Error` is deliberately *not* in this set: no window and no claim to one is a
/// coherent state, and it is the negative cache — refetching it here would undo
/// [`failure_fuse`].
fn window_lost(snapshot: &FeedSnapshot) -> bool {
    snapshot.window.is_none()
        && snapshot
            .observation
            .last_status
            .window_status_str()
            .is_some()
}

/// The `feeds.last_error` text for a failed parse: the stage that gave up plus
/// the parser's own reason.
///
/// Run `parse_feed_document` on the blocking pool, fused by `fuse`
/// (production passes the engine's [`parse_fuse`]-scaled budget).
///
/// The parse is synchronous CPU over an attacker-authored body of up to
/// `max_response_bytes` — sanitation, XML/JSON parsing, and per-item
/// HTML→Markdown. Run inline it pins a runtime worker for its whole
/// duration: the runtime schedules cooperatively, so between two `.await`s
/// neither the exec layer's `timeout_at` nor any other task sharing the
/// runtime (the server's query handling included) gets a look-in, and
/// `max_concurrent` simultaneous parses can sit on that many workers at
/// once. On the blocking pool the workers stay free, and this `.await` is a
/// real yield point the scan deadline can fire at. Concurrency needs no new
/// bound: the caller holds its politeness permit across this await, so at
/// most `max_concurrent` parses run per source.
///
/// Both synthesized failures degrade the one feed, preserving the engine's
/// no-`Err` contract:
/// - `"timeout"`: the fuse elapsed. The partition moves on; the detached
///   parse runs to completion on its blocking thread and its result is
///   dropped — a running thread cannot be preempted, so the fuse bounds the
///   *stall*, while the burn it abandons is itself bounded by parse being
///   linear in a capped body.
/// - `"panic"`: `parse_feed_document` is designed not to panic on any
///   input; if it ever does, the feed degrades instead of the scan
///   unwinding. The `JoinError` is *not* interpolated into the reason:
///   its `Display` quotes the panic payload (tokio-1.52.3
///   `src/runtime/task/error.rs`, the `Repr::Panic` arm), and a payload
///   can carry feed-authored bytes — a library panicking through an
///   `unwrap` or an `assert` on parsed input prints them — which would
///   put attacker-authored text in `feeds.last_error` and from there into
///   an agent's context. Nothing is lost by dropping it: the panic hook
///   has already written the payload *and* a backtrace to the process
///   log, which is where a bug report against the parsing stack starts
///   anyway. The same arm also catches the runtime-shutdown
///   cancellation, which is why the reason distinguishes the two.
async fn parse_off_worker(
    body: Vec<u8>,
    content_type: Option<String>,
    fuse: Duration,
) -> Result<ParsedDocument, ParseFailure> {
    let parse =
        tokio::task::spawn_blocking(move || parse_feed_document(&body, content_type.as_deref()));
    match tokio::time::timeout(fuse, parse).await {
        Ok(Ok(result)) => result,
        Ok(Err(join_error)) => Err(join_failure(&join_error)),
        Err(_elapsed) => Err(ParseFailure {
            stage: "timeout",
            reason: format!(
                "feed parse did not finish within {}s; abandoned",
                fuse.as_secs()
            ),
            dialect_declared: None,
        }),
    }
}

/// The `"panic"` [`ParseFailure`] for a parse task that did not return a
/// value — see [`parse_off_worker`]'s doc for why the `JoinError` itself is
/// never quoted.
fn join_failure(join_error: &tokio::task::JoinError) -> ParseFailure {
    ParseFailure {
        stage: "panic",
        reason: if join_error.is_panic() {
            "feed parse panicked; the payload and backtrace are in the server log".to_string()
        } else {
            "feed parse did not complete: its task was cancelled".to_string()
        },
        dialect_declared: None,
    }
}

/// The cap applies to the *composed* string, not just to `reason`. Bounding the
/// reason alone would let the `"parse failed at …: "` prefix push the stored
/// value past [`MAX_ERROR_CHARS`], which is a cap on what lands in the column.
fn parse_error_message(stage: &str, reason: &str) -> String {
    truncate(
        &format!("parse failed at {stage}: {reason}"),
        MAX_ERROR_CHARS,
    )
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
    use std::net::IpAddr;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use arrow::array::{Array, UInt64Array};

    use super::*;
    use crate::sources::providers::open_connector::testutil::{CapturedEvent, capture_events};
    use crate::sources::providers::rss::config::{FeedSubscription, inline_config};
    use crate::sources::providers::rss::egress::{AllowAll, EgressReason};
    use crate::sources::providers::rss::testutil::{
        MockFeedServer, MockResponse, RSS2_MINIMAL, str_col, str_opt_col,
    };

    /// Test-only denying policy: refuses exactly the listed addresses, allows
    /// everything else. Duplicated from `fetch.rs`'s test module of the same
    /// name — a test module cannot see another module's private test type.
    #[derive(Debug)]
    struct DenyList(Vec<IpAddr>);
    impl EgressPolicy for DenyList {
        fn check_ip(&self, ip: IpAddr) -> Result<(), EgressReason> {
            if self.0.contains(&ip) {
                Err("test-denied".into())
            } else {
                Ok(())
            }
        }
    }

    /// Flips from allow-everything to deny-everything on demand — the
    /// "verdict changed after the cache was warmed" scenario, whether by a
    /// dynamic policy or a new DNS answer.
    #[derive(Debug)]
    struct TogglePolicy(AtomicBool);
    impl EgressPolicy for TogglePolicy {
        fn check_ip(&self, _ip: IpAddr) -> Result<(), EgressReason> {
            if self.0.load(Ordering::SeqCst) {
                Err("test-denied".into())
            } else {
                Ok(())
            }
        }
    }

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
            expected_generation: u64,
            window: CachedWindow,
            observation: FeedObservation,
            armed_until: Instant,
        ) {
            self.0
                .record_success(feed, expected_generation, window, observation, armed_until);
        }

        fn record_not_modified(
            &self,
            feed: &str,
            expected_generation: u64,
            http_status: u16,
            last_fetch_ms: i64,
            armed_until: Instant,
        ) {
            self.0.record_not_modified(
                feed,
                expected_generation,
                http_status,
                last_fetch_ms,
                armed_until,
            );
        }

        fn record_failure(
            &self,
            feed: &str,
            expected_generation: u64,
            http_status: Option<u16>,
            error: String,
            dialect_declared: Option<String>,
            last_fetch_ms: i64,
            armed_until: Instant,
        ) {
            self.0.record_failure(
                feed,
                expected_generation,
                http_status,
                error,
                dialect_declared,
                last_fetch_ms,
                armed_until,
            );
        }

        fn record_egress_denial(
            &self,
            feed: &str,
            expected_generation: u64,
            error: String,
            last_fetch_ms: i64,
            armed_until: Instant,
        ) {
            self.0.record_egress_denial(
                feed,
                expected_generation,
                error,
                last_fetch_ms,
                armed_until,
            );
        }
    }

    /// A cache that remembers every `armed_until` the engine hands
    /// `record_failure`, delegating every call to a real [`MemoryFeedCache`].
    ///
    /// The failure fuse has no other observable: `FeedSnapshot` exposes
    /// `within_ttl`, not the instant behind it, and no configuration can make a
    /// test wait one out. See
    /// [`a_failure_arms_the_ttls_quarter_not_the_floor`].
    struct RecordsFailureArm {
        inner: MemoryFeedCache,
        armed: std::sync::Mutex<Vec<Instant>>,
    }

    impl RecordsFailureArm {
        fn new(inner: MemoryFeedCache) -> Self {
            Self {
                inner,
                armed: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn armed_instants(&self) -> Vec<Instant> {
            self.armed.lock().unwrap_or_else(|p| p.into_inner()).clone()
        }
    }

    impl FeedCache for RecordsFailureArm {
        fn snapshot(&self, feed: &str, now: Instant) -> FeedSnapshot {
            self.inner.snapshot(feed, now)
        }

        fn record_success(
            &self,
            feed: &str,
            expected_generation: u64,
            window: CachedWindow,
            observation: FeedObservation,
            armed_until: Instant,
        ) {
            self.inner
                .record_success(feed, expected_generation, window, observation, armed_until);
        }

        fn record_not_modified(
            &self,
            feed: &str,
            expected_generation: u64,
            http_status: u16,
            last_fetch_ms: i64,
            armed_until: Instant,
        ) {
            self.inner.record_not_modified(
                feed,
                expected_generation,
                http_status,
                last_fetch_ms,
                armed_until,
            );
        }

        fn record_failure(
            &self,
            feed: &str,
            expected_generation: u64,
            http_status: Option<u16>,
            error: String,
            dialect_declared: Option<String>,
            last_fetch_ms: i64,
            armed_until: Instant,
        ) {
            self.armed
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(armed_until);
            self.inner.record_failure(
                feed,
                expected_generation,
                http_status,
                error,
                dialect_declared,
                last_fetch_ms,
                armed_until,
            );
        }

        fn record_egress_denial(
            &self,
            feed: &str,
            expected_generation: u64,
            error: String,
            last_fetch_ms: i64,
            armed_until: Instant,
        ) {
            self.inner.record_egress_denial(
                feed,
                expected_generation,
                error,
                last_fetch_ms,
                armed_until,
            );
        }
    }

    /// The OSS default: no destination filtering, sufficient for every test
    /// except the one that proves a denial reaches `feeds.last_error` — see
    /// [`engine_with_cache_and_policy`].
    fn engine_with_cache(
        feeds: &[(String, String)],
        ttl_seconds: u64,
        max_concurrent: usize,
        cache: Arc<dyn FeedCache>,
    ) -> RssEngine {
        engine_with_cache_and_policy(
            feeds,
            ttl_seconds,
            max_concurrent,
            cache,
            Arc::new(AllowAll),
        )
    }

    /// [`engine_with_cache`] with a caller-supplied egress policy — the seam
    /// [`egress_blocked_feed_degrades_like_unreachable`] uses to inject a
    /// [`DenyList`] instead of the OSS `AllowAll` default.
    fn engine_with_cache_and_policy(
        feeds: &[(String, String)],
        ttl_seconds: u64,
        max_concurrent: usize,
        cache: Arc<dyn FeedCache>,
        policy: Arc<dyn EgressPolicy>,
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
            policy,
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

    /// Captured events with `message`, in emission order.
    ///
    /// Mirrors `exec.rs`'s helper of the same name; the two cannot be shared
    /// because each lives inside its own module's `#[cfg(test)] mod tests`.
    fn events_with_message(
        events: &Arc<std::sync::Mutex<Vec<CapturedEvent>>>,
        message: &str,
    ) -> Vec<CapturedEvent> {
        events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|event| event.message == message)
            .cloned()
            .collect()
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
        // Guard, not an assertion target: this test reaches the `rss feed
        // degraded` warn callsite, and `tracing` caches a callsite's `Interest`
        // globally on first use — a guardless test reaching it first would cache
        // `Interest::never` and silently empty
        // `a_degraded_feed_emits_a_warning_naming_the_feed_and_reason`.
        let (_interest_guard, _) = capture_events();
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
        // Guard, not an assertion target: this test reaches the `rss feed
        // degraded` warn callsite, and `tracing` caches a callsite's `Interest`
        // globally on first use — a guardless test reaching it first would cache
        // `Interest::never` and silently empty
        // `a_degraded_feed_emits_a_warning_naming_the_feed_and_reason`.
        let (_interest_guard, _) = capture_events();
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
        // Guard, not an assertion target: this test reaches the `rss feed
        // degraded` warn callsite, and `tracing` caches a callsite's `Interest`
        // globally on first use — a guardless test reaching it first would cache
        // `Interest::never` and silently empty
        // `a_degraded_feed_emits_a_warning_naming_the_feed_and_reason`.
        let (_interest_guard, _) = capture_events();
        let server = MockFeedServer::start(|_| MockResponse::status(500)).await;
        let engine = test_engine(&server, &[("a", "/f.xml")], 900);

        assert!(engine.serve_feed("a", || true).await.is_none());
        let row = engine.feeds_row("a");
        assert_eq!(str_col(&row, "last_status"), vec!["error"]);
        assert_eq!(u64_col(&row, "item_count"), vec![None]);

        // Negative cache: the failure armed a fuse, so an immediate second
        // serve does not re-poke the dead feed. Asserted through `serve_feed`
        // and not `feeds_row` — the latter is synchronous with no `await`, so it
        // structurally cannot fetch whatever the fuse says, and asserting a
        // stable request count across it proved nothing.
        let n = server.requests().len();
        assert!(
            engine.serve_feed("a", || true).await.is_none(),
            "still no window to serve"
        );
        assert_eq!(
            server.requests().len(),
            n,
            "the second serve went to the negative cache, not the network"
        );
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
        // Guard, not an assertion target: this test reaches the `rss feed
        // degraded` warn callsite, and `tracing` caches a callsite's `Interest`
        // globally on first use — a guardless test reaching it first would cache
        // `Interest::never` and silently empty
        // `a_degraded_feed_emits_a_warning_naming_the_feed_and_reason`.
        let (_interest_guard, _) = capture_events();
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

    /// What may reach `feeds.last_error`, pinned in both directions.
    ///
    /// The module doc states the property; this table is what enforces it. Each
    /// shape declares the [`Fate`] of a sentinel placed at one specific position
    /// in the document, and the assertions below check that fate exactly — so a
    /// sentinel in a *prose* position reaching the column fails, and so does a
    /// `Kept` fragment quietly vanishing (which would mean the error stopped
    /// being diagnosable) or a `NoError` shape starting to error (which means a
    /// dependency changed and the row needs re-deriving by hand).
    ///
    /// The earlier version of this test placed its sentinel only in character
    /// data and stated the property as "no body text at all". A reviewer then
    /// found four reachable shapes that put feed-supplied text in the column,
    /// all four of which are `Kept` rows below.
    #[tokio::test]
    async fn parse_failure_last_error_quotes_structure_not_prose() {
        // Guard, not an assertion target: this test reaches the `rss feed
        // degraded` warn callsite, and `tracing` caches a callsite's `Interest`
        // globally on first use — a guardless test reaching it first would cache
        // `Interest::never` and silently empty
        // `a_degraded_feed_emits_a_warning_naming_the_feed_and_reason`.
        let (_interest_guard, _) = capture_events();
        const SENTINEL: &str = "SHOULD-NOT-LEAK";

        /// Where a shape's sentinel is expected to end up.
        #[derive(Debug, PartialEq, Eq)]
        enum Fate {
            /// The shape fails to parse and the sentinel must not be in the
            /// recorded error: it sat somewhere the provider reads as content.
            Absent,
            /// The shape fails to parse and the sentinel *is* in the error, by
            /// the plan's error-redaction decision: it sat in a structural
            /// position, and the fragment is what makes the failure
            /// diagnosable.
            Kept,
            /// The shape does not fail to parse at all today, so there is no
            /// error to inspect. A live guard rather than a filler row.
            NoError,
        }

        // (label, body, content-type is JSON, expected fate)
        let shapes: &[(&str, &str, bool, Fate)] = &[
            // ---- Absent: the sentinel sits where the provider reads content.
            (
                "character data, document truncated",
                "<rss version=\"2.0\"><channel><title>SHOULD-NOT-LEAK secret prose",
                false,
                // Reaches `SyntaxError::UnclosedTag`, which names no document
                // text at all.
                Fate::Absent,
            ),
            (
                "undefined entity in character data, document truncated",
                // The input `EscapeError::UnrecognizedEntity` would quote
                // verbatim. Reaches `UnclosedTag` instead: the structural
                // failure comes first.
                "<rss version=\"2.0\"><channel><title>&SHOULD-NOT-LEAK; truncat",
                false,
                Fate::Absent,
            ),
            (
                "character data beside an out-of-range character reference",
                // The one shape that reaches `EscapeError::InvalidCharRef`,
                // which renders the parsed number alone. Position matters: the
                // same reference inside `<title>` is swallowed per-element and
                // yields no error.
                concat!(
                    r#"<rss version="2.0"><channel>SHOULD-NOT-LEAK &#x110000;"#,
                    r#"<title>t</title><link>https://e.example/</link>"#,
                    r#"<description>d</description></channel></rss>"#,
                ),
                false,
                Fate::Absent,
            ),
            (
                "entry prose, document failing structurally elsewhere",
                // The shape that matters most: an article body in the slot an
                // article body belongs in, in a document that *does* fail. The
                // error names the mismatched tag, not the summary.
                concat!(
                    r#"<feed xmlns="http://www.w3.org/2005/Atom"><title>t</title>"#,
                    r#"<id>i</id><entry><id>e</id>"#,
                    r#"<summary>SHOULD-NOT-LEAK the whole article body</summary>"#,
                    r#"</mismatched></feed>"#,
                ),
                false,
                Fate::Absent,
            ),
            (
                "feed title, JSON failing on a type elsewhere",
                // Same idea on the JSON side: the sentinel is a correctly typed
                // `title`, and the failure is a different member's type.
                r#"{"version":"https://jsonfeed.org/version/1.1","title":"SHOULD-NOT-LEAK prose","items":"x"}"#,
                true,
                Fate::Absent,
            ),
            // ---- NoError: nothing fails, so nothing is asserted about content.
            (
                "undefined entity in a well-formed title",
                // feed-rs 2.4.0 writes an unresolvable entity back into the text
                // verbatim and reports nothing. Kept as a live guard: the day a
                // future feed-rs starts reporting escape errors for element
                // text, this row's fate flips and this test says so.
                concat!(
                    r#"<rss version="2.0"><channel><title>&SHOULD-NOT-LEAK;</title>"#,
                    r#"<link>https://e.example/</link><description>d</description>"#,
                    r#"</channel></rss>"#,
                ),
                false,
                Fate::NoError,
            ),
            // ---- Kept: the sentinel sits in a structural position.
            (
                "Atom content type attribute",
                // An attribute value the parser has to interpret; the error is
                // `unsupported content type SHOULD-NOT-LEAK`.
                concat!(
                    r#"<feed xmlns="http://www.w3.org/2005/Atom"><title>t</title>"#,
                    r#"<id>i</id><entry><id>e</id>"#,
                    r#"<content type="SHOULD-NOT-LEAK">x</content></entry></feed>"#,
                ),
                false,
                Fate::Kept,
            ),
            (
                "mismatched end tag names the element",
                // quick-xml's ill-formed family quotes both tag names.
                concat!(
                    r#"<feed xmlns="http://www.w3.org/2005/Atom"><title>t</title>"#,
                    r#"<id>i</id><entry><id>e</id></SHOULD-NOT-LEAK></feed>"#,
                ),
                false,
                Fate::Kept,
            ),
            (
                "JSON string where a u64 was declared",
                // serde_json renders the offending value verbatim in
                // `invalid type: string "…", expected u64`.
                concat!(
                    r#"{"version":"https://jsonfeed.org/version/1.1","title":"t","#,
                    r#""items":[{"id":"1","attachments":[{"url":"u","#,
                    r#""size_in_bytes":"SHOULD-NOT-LEAK"}]}]}"#,
                ),
                true,
                Fate::Kept,
            ),
            (
                "JSON string where a sequence was declared",
                r#"{"version":"https://jsonfeed.org/version/1.1","title":"t","items":"SHOULD-NOT-LEAK"}"#,
                true,
                Fate::Kept,
            ),
        ];

        let mut errors_seen = 0;
        for (label, body, is_json, fate) in shapes {
            let body = (*body).to_string();
            let is_json = *is_json;
            let server = MockFeedServer::start(move |_| {
                if is_json {
                    MockResponse::new(200, body.clone().into_bytes())
                        .with_header("content-type", "application/json")
                } else {
                    MockResponse::xml(&body)
                }
            })
            .await;
            let engine = test_engine(&server, &[("a", "/f")], 900);

            engine.serve_feed("a", || true).await;
            let recorded = str_opt_col(&engine.feeds_row("a"), "last_error")[0].clone();

            match (fate, &recorded) {
                (Fate::NoError, Some(error)) => panic!(
                    "shape {label:?} was expected to parse cleanly but recorded an error — \
                     a dependency changed and this row must be re-derived by hand: {error}"
                ),
                (Fate::NoError, None) => {}
                (_, None) => panic!(
                    "shape {label:?} was expected to fail to parse and did not; the assertion \
                     it exists for never ran"
                ),
                (Fate::Absent, Some(error)) => {
                    errors_seen += 1;
                    assert!(
                        !error.contains(SENTINEL),
                        "feed content the provider reads as prose reached last_error for \
                         {label:?}: {error}"
                    );
                }
                (Fate::Kept, Some(error)) => {
                    errors_seen += 1;
                    assert!(
                        error.contains(SENTINEL),
                        "shape {label:?} is documented as quoting its structural fragment and \
                         no longer does; the module doc's measured list is now wrong: {error}"
                    );
                }
            }
            // Whatever the fate, the cap holds — it is the only bound on a
            // `Kept` fragment's length.
            if let Some(error) = recorded {
                assert!(
                    error.chars().count() <= MAX_ERROR_CHARS,
                    "stored error for {label:?} exceeds the cap: {}",
                    error.chars().count()
                );
            }
        }

        // Guards the loop against going quietly vacuous. The per-shape match
        // above already fails a row whose fate changed, so this is the
        // belt-and-braces total: nine of the ten shapes must reach the failure
        // path for the assertions to have run.
        assert_eq!(
            errors_seen,
            shapes
                .iter()
                .filter(|(_, _, _, f)| *f != Fate::NoError)
                .count(),
            "every shape but the well-formed one must record an error"
        );
        assert_eq!(errors_seen, 9);
    }

    /// A `Kept` fragment is bounded only by the cap, and the cap is therefore
    /// the whole defence against a feed choosing what lands in this column.
    ///
    /// Measured with a JSON type mismatch because that is the widest of the
    /// channels: `serde_json` renders the offending string verbatim and
    /// unabbreviated, so a feed can put an arbitrary ~1 KB of its own choosing
    /// here and see [`MAX_ERROR_CHARS`] of it stored.
    #[tokio::test]
    async fn a_json_type_mismatch_quotes_arbitrary_text_up_to_the_cap() {
        // Guard, not an assertion target: this test reaches the `rss feed
        // degraded` warn callsite, and `tracing` caches a callsite's `Interest`
        // globally on first use — a guardless test reaching it first would cache
        // `Interest::never` and silently empty
        // `a_degraded_feed_emits_a_warning_naming_the_feed_and_reason`.
        let (_interest_guard, _) = capture_events();
        let filler = "arbitrary feed-chosen text ".repeat(40);
        let body = format!(
            concat!(
                r#"{{"version":"https://jsonfeed.org/version/1.1","title":"t","#,
                r#""items":[{{"id":"1","tags":"{}"}}]}}"#,
            ),
            filler
        );
        assert!(
            filler.len() > MAX_ERROR_CHARS,
            "the filler must overrun the cap"
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
            .expect("a type mismatch is a parse failure");
        assert!(
            error.contains("arbitrary feed-chosen text"),
            "the offending value is quoted verbatim: {error}"
        );
        assert_eq!(
            error.chars().count(),
            MAX_ERROR_CHARS,
            "and the cap is what stops it"
        );
    }

    /// A JSON Feed's declared `version`, kept when unrecognised: feed-rs
    /// interpolates it, and it is a member value out of the document.
    ///
    /// Pinned from the kept side deliberately. A test that only asserted the
    /// absence of leaks would pass just as happily if this fragment silently
    /// disappeared — leaving an unsupported version undiagnosable — or if
    /// someone "fixed" it without updating the module doc.
    #[tokio::test]
    async fn json_unsupported_version_is_body_text_kept_in_last_error() {
        // Guard, not an assertion target: this test reaches the `rss feed
        // degraded` warn callsite, and `tracing` caches a callsite's `Interest`
        // globally on first use — a guardless test reaching it first would cache
        // `Interest::never` and silently empty
        // `a_degraded_feed_emits_a_warning_naming_the_feed_and_reason`.
        let (_interest_guard, _) = capture_events();
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
    ///
    /// The stored length is asserted as a literal `512` rather than against
    /// [`MAX_ERROR_CHARS`], because 512 is what `docs/rss.md:974` and
    /// `docs/rss/semantics.yaml:85` publish to consumers of the column — this
    /// is the end-to-end half of `error.rs`'s
    /// `max_error_chars_is_the_number_the_docs_publish`, and it is what pins
    /// the *column* rather than the constant.
    #[tokio::test]
    async fn a_huge_json_version_is_still_capped() {
        // Guard, not an assertion target: this test reaches the `rss feed
        // degraded` warn callsite, and `tracing` caches a callsite's `Interest`
        // globally on first use — a guardless test reaching it first would cache
        // `Interest::never` and silently empty
        // `a_degraded_feed_emits_a_warning_naming_the_feed_and_reason`.
        let (_interest_guard, _) = capture_events();
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
        assert_eq!(
            error.chars().count(),
            512,
            "the documented cap on feeds.last_error, spelled out"
        );
    }

    /// AC4's "a tracing warning is emitted — nothing silent" clause, pinned.
    ///
    /// Every degraded feed emits exactly one `warn` naming the source, the
    /// subscription, its URL, and the error — the log-side half of a degradation
    /// that `feeds.last_error` carries in band. Nothing asserted this before, so
    /// deleting the `warn!` would have been invisible.
    ///
    /// Note the [`capture_events`] contract this test depends on, and why every
    /// other test above that reaches `degrade` now holds a guard too: `tracing`
    /// caches a callsite's `Interest` globally on first use, so a guardless test
    /// that reached this `warn!` first could cache `Interest::never` for the
    /// whole binary and empty the assertions below without failing anything.
    #[tokio::test]
    async fn a_degraded_feed_emits_a_warning_naming_the_feed_and_reason() {
        let (_guard, events) = capture_events();
        let server = MockFeedServer::start(|_| MockResponse::status(503)).await;
        let engine = test_engine(&server, &[("a", "/f.xml")], 900);

        assert!(engine.serve_feed("a", || true).await.is_none());

        let warnings = events_with_message(&events, "rss feed degraded");
        assert_eq!(
            warnings.len(),
            1,
            "exactly one warning per degraded serve, not zero and not one per attempt"
        );
        let warning = &warnings[0];
        assert_eq!(warning.level, tracing::Level::WARN);
        assert_eq!(warning.fields.get("feed").map(String::as_str), Some("a"));
        assert_eq!(
            warning.fields.get("source").map(String::as_str),
            Some("rss_test")
        );
        assert!(
            warning.fields.get("url").is_none(),
            "no URL at warn — a subscription URL can carry a private query token"
        );
        let error = warning
            .fields
            .get("error")
            .expect("the warning carries the reason, not just the feed name");
        assert!(
            error.contains("503"),
            "and the reason is the one recorded in last_error: {error}"
        );
        assert_eq!(
            warning.fields.get("error").cloned(),
            str_opt_col(&engine.feeds_row("a"), "last_error")[0].clone(),
            "the log and the column carry the same string, so neither can drift"
        );

        // A healthy serve emits none, so the warning means something.
        let ok = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        let healthy = test_engine(&ok, &[("b", "/f.xml")], 900);
        healthy.serve_feed("b", || true).await.expect("b served");
        assert_eq!(
            events_with_message(&events, "rss feed degraded").len(),
            1,
            "a successful serve adds no degradation warning"
        );
    }

    #[tokio::test]
    async fn egress_blocked_feed_degrades_like_unreachable() {
        // Guard, not an assertion target: this test reaches the `rss feed
        // degraded` warn callsite, and `tracing` caches a callsite's `Interest`
        // globally on first use — a guardless test reaching it first would cache
        // `Interest::never` and silently empty
        // `a_degraded_feed_emits_a_warning_naming_the_feed_and_reason`.
        let (_interest_guard, _) = capture_events();
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        let feeds = vec![("a".to_string(), "http://10.1.2.3/f".to_string())];
        let cache = Arc::new(MemoryFeedCache::new(CACHE_MAX_BYTES, feeds.len() + 8));
        // The feed URL's host is an IP literal, so `check_hop_target` checks
        // it directly without ever reaching `PolicyDns` — deny exactly the
        // address the fetch actually targets.
        let policy: Arc<dyn EgressPolicy> = Arc::new(DenyList(vec!["10.1.2.3".parse().unwrap()]));
        let engine = engine_with_cache_and_policy(&feeds, 900, 4, cache, policy);

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

    /// The review-requested scenario: a feed cached under an allowing
    /// verdict, then refused — a changed policy or a new DNS answer. The
    /// refusal must produce zero item rows *despite* the warm cache (a
    /// policy verdict is not a transient fault to serve stale through), the
    /// refusal must land in `feeds`, and the fuse must keep serving zero
    /// rows without re-poking the destination.
    #[tokio::test]
    async fn a_denial_after_a_warm_cache_serves_zero_rows_not_stale() {
        let (_interest_guard, _) = capture_events();
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        // The mock's URL has an IP-literal host (127.0.0.1), so
        // `check_hop_target` consults the policy directly on every fetch.
        let feeds = vec![("a".to_string(), format!("{}/f.xml", server.url()))];
        let cache = Arc::new(MemoryFeedCache::new(CACHE_MAX_BYTES, 64));
        let policy = Arc::new(TogglePolicy(AtomicBool::new(false)));
        // `ttl_seconds: 0`: every serve refetches, so the second serve below
        // actually consults the flipped policy instead of the cache.
        let engine = engine_with_cache_and_policy(
            &feeds,
            0,
            4,
            cache,
            Arc::clone(&policy) as Arc<dyn EgressPolicy>,
        );

        // Warm the cache under the allowing verdict.
        assert_eq!(
            engine
                .serve_feed("a", || true)
                .await
                .expect("the allowed fetch warms the cache")
                .num_rows(),
            1
        );

        // The verdict changes.
        policy.0.store(true, Ordering::SeqCst);

        // The denial serve: zero rows despite the warm cache.
        assert!(
            engine.serve_feed("a", || true).await.is_none(),
            "no stale rows from a refused destination"
        );
        let row = engine.feeds_row("a");
        assert_eq!(str_col(&row, "last_status"), vec!["error"]);
        let error = str_opt_col(&row, "last_error")[0]
            .clone()
            .expect("last_error records the refusal");
        assert!(error.contains("egress blocked"), "{error}");

        // Within the fuse: still zero rows, and no new connection attempts —
        // the denial negative-caches instead of re-poking every scan.
        let requests_after_denial = server.requests().len();
        assert!(engine.serve_feed("a", || true).await.is_none());
        assert_eq!(
            server.requests().len(),
            requests_after_denial,
            "the fuse holds: a denied feed is not re-poked within it"
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

    /// A within-TTL feed whose window was evicted refetches rather than
    /// silently serving nothing.
    ///
    /// The seam two task-scoped tests each missed: `cache.rs`'s
    /// `eviction_drops_window_and_validators_but_keeps_observation` asserts the
    /// cache's half and never runs the engine, while this module's TTL tests run
    /// the engine and never evict. Reverting [`window_lost`] makes the third
    /// serve below return `None` with no request, while `feeds` still reports
    /// `fresh` / `item_count = 1` / NULL `last_error` — a silent capture gap for
    /// the rest of the TTL.
    #[tokio::test]
    async fn within_ttl_serve_refetches_after_its_window_was_evicted() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        let urls = vec![
            ("a".to_string(), format!("{}/a.xml", server.url())),
            ("b".to_string(), format!("{}/b.xml", server.url())),
        ];

        // Size the byte budget for exactly one window, measured off a real one
        // rather than guessed: a throwaway engine with the shipped budget
        // serves `a` once and reports what its window costs. Both feeds serve
        // the same document under equal-length names and paths, so `b`'s window
        // costs the same and the two cannot both fit.
        let probe = engine_over(&urls, 900, 4);
        let window_bytes = probe
            .serve_feed("a", || true)
            .await
            .expect("probe serve")
            .get_array_memory_size();

        let cache = Arc::new(MemoryFeedCache::new(window_bytes + 8, 64));
        let engine = engine_with_cache(&urls, 900, 4, cache);

        let first = engine.serve_feed("a", || true).await.expect("a served");
        assert_eq!(first.num_rows(), 1);

        // `b`'s window evicts `a`'s under the byte budget while `a` is still
        // deep inside its 900s TTL. `a`'s observation survives by design.
        engine.serve_feed("b", || true).await.expect("b served");
        let health = engine.feeds_row("a");
        assert_eq!(
            str_col(&health, "last_status"),
            vec!["fresh"],
            "eviction keeps the observation, so health still reads fresh"
        );
        assert_eq!(u64_col(&health, "item_count"), vec![Some(1)]);
        assert_eq!(str_opt_col(&health, "last_error"), vec![None]);
        assert_eq!(
            str_opt_col(&health, "etag"),
            vec![None],
            "the validators went with the window, so the refetch below has none"
        );

        let before = server.requests().len();
        let again = engine
            .serve_feed("a", || true)
            .await
            .expect("an evicted window inside its TTL refetches instead of serving nothing");
        assert_eq!(again.num_rows(), 1, "the rows are actually back");
        assert_eq!(str_col(&again, "window_status"), vec!["fresh"]);

        let refetches = &server.requests()[before..];
        assert_eq!(
            refetches.len(),
            1,
            "exactly one request, not zero and not a retry storm"
        );
        assert_eq!(refetches[0].path, "/a.xml");
        assert_eq!(
            refetches[0].header("if-none-match"),
            None,
            "no window means no validators: an unconditional GET, which is the \
             only request that can refill the window"
        );
        assert_eq!(refetches[0].header("if-modified-since"), None);
    }

    /// The negative cache is *not* collateral of the fix above: `error` within
    /// its failure fuse has no window and claims none, so it stays a cache hit
    /// and a dead feed is still not re-poked on every scan.
    #[tokio::test]
    async fn within_ttl_error_state_still_short_circuits_the_network() {
        // Guard, not an assertion target: this test reaches the `rss feed
        // degraded` warn callsite, and `tracing` caches a callsite's `Interest`
        // globally on first use — a guardless test reaching it first would cache
        // `Interest::never` and silently empty
        // `a_degraded_feed_emits_a_warning_naming_the_feed_and_reason`.
        let (_interest_guard, _) = capture_events();
        let server = MockFeedServer::start(|_| MockResponse::status(500)).await;
        let engine = test_engine(&server, &[("a", "/f.xml")], 900);

        assert!(engine.serve_feed("a", || true).await.is_none());
        assert_eq!(
            str_col(&engine.feeds_row("a"), "last_status"),
            vec!["error"]
        );
        let after_failure = server.requests().len();

        assert!(engine.serve_feed("a", || true).await.is_none());
        assert_eq!(
            server.requests().len(),
            after_failure,
            "a window-less `error` inside its fuse must not refetch"
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

    /// A parse that outlives its fuse degrades to a `"timeout"` failure
    /// instead of holding the partition. `Duration::ZERO` stands in for "any
    /// parse outlives it"; the body is non-trivial so the blocking thread
    /// cannot have finished before the fuse is first polled.
    #[tokio::test]
    async fn a_parse_that_outlives_the_fuse_degrades_to_a_timeout_failure() {
        let mut body = String::from(r#"<rss version="2.0"><channel><title>t</title>"#);
        for i in 0..2_000 {
            body.push_str(&format!("<item><guid>g{i}</guid><title>x</title></item>"));
        }
        body.push_str("</channel></rss>");

        let failure = parse_off_worker(body.into_bytes(), None, Duration::ZERO)
            .await
            .unwrap_err();
        assert_eq!(failure.stage, "timeout");
        assert!(
            failure.reason.contains("did not finish"),
            "{}",
            failure.reason
        );
    }

    /// The same body parses fine under the production fuse — the fuse
    /// degrades slow parses, it does not tax normal ones.
    #[tokio::test]
    async fn a_parse_within_the_fuse_returns_the_document() {
        let document = parse_off_worker(RSS2_MINIMAL.as_bytes().to_vec(), None, PARSE_TIMEOUT)
            .await
            .expect("a well-formed document parses within the fuse");
        assert_eq!(document.items.len(), 1);
    }

    /// A panicking parse must not carry its payload into `feeds.last_error`:
    /// `JoinError`'s own `Display` quotes it, and a payload can hold
    /// feed-authored bytes (a library `unwrap`/`assert` on parsed input
    /// prints them), which the column feeds into an agent's context. The
    /// panic hook has already written payload and backtrace to the process
    /// log, so the column loses no diagnostic reach by naming it instead.
    ///
    /// The panicking task prints to stderr while this test runs; that is the
    /// hook doing exactly what the assertion below relies on.
    #[tokio::test]
    async fn a_panicked_parse_does_not_quote_the_payload() {
        let join_error = tokio::task::spawn_blocking(|| panic!("feed-authored payload"))
            .await
            .expect_err("the task panicked");
        assert!(join_error.is_panic());
        assert!(
            join_error.to_string().contains("feed-authored payload"),
            "guard: if JoinError stops quoting payloads, this test's premise is gone"
        );

        let failure = join_failure(&join_error);
        assert_eq!(failure.stage, "panic");
        assert!(
            !failure.reason.contains("feed-authored payload"),
            "the payload must not reach last_error: {}",
            failure.reason
        );
        assert!(
            failure.reason.contains("server log"),
            "and the reason must say where the payload actually is: {}",
            failure.reason
        );
    }

    /// The fuse scales with the licensed input — ten seconds per 5 MiB unit,
    /// partial units rounded up, floored at one unit, capped at an hour —
    /// so raising `max_response_bytes` cannot make the fuse misfire on the
    /// legitimate large documents it now permits.
    #[test]
    fn the_parse_fuse_tracks_max_response_bytes() {
        assert_eq!(
            parse_fuse(DEFAULT_MAX_RESPONSE_BYTES),
            Duration::from_secs(10),
            "the default cap keeps the original ten-second fuse"
        );
        assert_eq!(
            parse_fuse(1),
            Duration::from_secs(10),
            "floored at one unit"
        );
        assert_eq!(
            parse_fuse(DEFAULT_MAX_RESPONSE_BYTES + 1),
            Duration::from_secs(20),
            "partial units round up"
        );
        assert_eq!(
            parse_fuse(DEFAULT_MAX_RESPONSE_BYTES * 10),
            Duration::from_secs(100)
        );
        assert_eq!(
            parse_fuse(u64::MAX),
            MAX_PARSE_FUSE,
            "capped, not overflowing"
        );
    }

    /// A body budget big enough to push the fuse past the scan timeout
    /// inverts the diagnosable-before-traceless ordering (parse timeouts
    /// would surface as deadline drops with no `last_error`); the engine
    /// says so at construction instead of leaving it to be discovered from
    /// a `never` row.
    #[tokio::test]
    async fn a_parse_fuse_past_the_scan_timeout_warns_at_construction() {
        let (_guard, events) = capture_events();
        let subscriptions = vec![ResolvedSubscription {
            name: "a".to_string(),
            url: "https://feed.example/f.xml".to_string(),
        }];
        let mut config = inline_config(vec![FeedSubscription {
            url: "https://feed.example/f.xml".to_string(),
            name: Some("a".to_string()),
        }]);
        // 7 units × 10s = 70s ≥ the 60s default scan timeout.
        config.max_response_bytes = DEFAULT_MAX_RESPONSE_BYTES * 7;
        let fetcher = FeedFetcher::new(
            Arc::new(AllowAll),
            Duration::from_secs(5),
            config.max_response_bytes,
            config.user_agent.clone(),
        )
        .expect("build the test fetcher");
        let _engine = RssEngine::with_parts(
            "rss_test".to_string(),
            subscriptions,
            &config,
            fetcher,
            Arc::new(MemoryFeedCache::new(CACHE_MAX_BYTES, 8)),
        );

        let warned = events_with_message(
            &events,
            "rss parse fuse meets or exceeds the scan timeout; raise scan_timeout_seconds",
        );
        assert_eq!(warned.len(), 1, "one warning per engine built");
        assert_eq!(warned[0].level, tracing::Level::WARN);
        assert_eq!(warned[0].field("parse_fuse_seconds"), Some("70"));
        assert_eq!(warned[0].field("scan_timeout_seconds"), Some("60"));
    }

    /// A well-formed two-item document, distinguishable from [`RSS2_MINIMAL`]
    /// by row count — the "fresher content" side of the commit-race tests.
    const RSS2_TWO_ITEMS: &str = concat!(
        r#"<rss version="2.0"><channel>"#,
        r#"<title>Minimal Feed</title>"#,
        r#"<link>https://feed.example/</link>"#,
        r#"<description>d</description>"#,
        r#"<item><guid>g1</guid><title>one</title></item>"#,
        r#"<item><guid>g2</guid><title>two</title></item>"#,
        r#"</channel></rss>"#
    );

    /// The 200/200 commit race, end to end: two concurrent serves of the same
    /// always-live feed, where the first request's response is delayed until
    /// after the second has committed. The generation gate drops the slower
    /// commit — the cache keeps the fresher window instead of applying
    /// commits in completion order.
    #[tokio::test]
    async fn a_slower_concurrent_fetch_cannot_regress_the_window() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls2 = Arc::clone(&calls);
        let server = MockFeedServer::start(move |_req| {
            if calls2.fetch_add(1, Ordering::SeqCst) == 0 {
                // The first request in flight: a delayed, older single-item
                // body.
                MockResponse::xml(RSS2_MINIMAL).with_delay(Duration::from_millis(400))
            } else {
                MockResponse::xml(RSS2_TWO_ITEMS)
            }
        })
        .await;
        let cache = Arc::new(MemoryFeedCache::new(CACHE_MAX_BYTES, 64));
        let feeds = vec![("a".to_string(), format!("{}/f.xml", server.url()))];
        let engine = engine_with_cache(&feeds, 0, 6, Arc::clone(&cache) as Arc<dyn FeedCache>);

        let slow = engine.serve_feed("a", || true);
        let fast = async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            engine.serve_feed("a", || true).await
        };
        let (slow_batch, fast_batch) = tokio::join!(slow, fast);

        // Each serve answers its own query from its own legitimate read...
        assert_eq!(slow_batch.expect("slow serve emits its read").num_rows(), 1);
        assert_eq!(fast_batch.expect("fast serve emits its read").num_rows(), 2);
        // ...but the cache keeps the first-committed, fresher window.
        let snap = cache.snapshot("a", Instant::now());
        assert_eq!(
            snap.window.as_ref().unwrap().batch.num_rows(),
            2,
            "the slower fetch's commit must be dropped, not applied last"
        );
        assert_eq!(snap.observation.item_count, Some(2));
    }

    /// The 200/304 commit race, end to end: a delayed `304` answering
    /// validators minted from the primed window arrives after a concurrent
    /// full `200` replaced that window. The stale `304` must neither stamp
    /// `revalidated` on the new window nor re-arm it — while its own serve
    /// still emits the rows its validators really did vouch for.
    #[tokio::test]
    async fn a_stale_304_end_to_end_does_not_relabel_the_new_window() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls2 = Arc::clone(&calls);
        let server = MockFeedServer::start(move |_req| {
            match calls2.fetch_add(1, Ordering::SeqCst) {
                // Prime: a window with validators, so the racing scans below
                // send conditional requests.
                0 => MockResponse::xml(RSS2_MINIMAL).with_header("etag", "\"v1\""),
                // The slow racer's conditional request: a delayed 304.
                1 => MockResponse::status(304).with_delay(Duration::from_millis(400)),
                // The fast racer: fresh content.
                _ => MockResponse::xml(RSS2_TWO_ITEMS),
            }
        })
        .await;
        let cache = Arc::new(MemoryFeedCache::new(CACHE_MAX_BYTES, 64));
        let feeds = vec![("a".to_string(), format!("{}/f.xml", server.url()))];
        let engine = engine_with_cache(&feeds, 0, 6, Arc::clone(&cache) as Arc<dyn FeedCache>);

        engine.serve_feed("a", || true).await.expect("primed");

        let slow_304 = engine.serve_feed("a", || true);
        let fast_200 = async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            engine.serve_feed("a", || true).await
        };
        let (revalidated_read, fresh_read) = tokio::join!(slow_304, fast_200);

        // The 304 serve emits the window its validators vouched for...
        assert_eq!(
            revalidated_read
                .expect("the 304 serves its snapshot window")
                .num_rows(),
            1
        );
        assert_eq!(fresh_read.expect("the 200 serves its fetch").num_rows(), 2);
        // ...while the cache keeps the fresh window unrelabelled: the stale
        // 304's commit was dropped.
        let snap = cache.snapshot("a", Instant::now());
        assert!(
            matches!(snap.observation.last_status, FeedStatus::Fresh),
            "not `revalidated` — the 304 never saw this window: {:?}",
            snap.observation.last_status
        );
        assert_eq!(snap.window.as_ref().unwrap().batch.num_rows(), 2);
    }

    /// `ttl_seconds` is stored raw by `RssConfig` — validation puts no ceiling
    /// on it at all — and [`RssEngine::with_parts`] is the first code that turns
    /// it into the `Duration` [`arm`] adds to an `Instant`.
    ///
    /// The counterpart of `exec.rs`'s
    /// `an_absurd_scan_timeout_is_clamped_rather_than_overflowing` for the
    /// sibling constant, written to the same shape: the clamp *and* the warning
    /// that makes it visible, at the same absurd input.
    #[tokio::test]
    async fn an_absurd_ttl_is_clamped_rather_than_overflowing() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        // Set before the engine is built: `with_parts` is where the clamp and
        // its warning happen.
        let (_guard, events) = capture_events();
        let engine = test_engine(&server, &[("a", "/f.xml")], u64::MAX);

        // Serving is what actually performs the add — an unclamped
        // `Duration::from_secs(u64::MAX)` is the value `Instant`'s `+` operator
        // documents as possibly panicking, and which `arm` catches.
        let batch = engine.serve_feed("a", || true).await.expect("rows served");
        assert_eq!(str_col(&batch, "window_status"), vec!["fresh"]);

        // The clamp is silent otherwise: an operator who configured a longer TTL
        // than they get has to be able to see that from the log.
        let clamped =
            events_with_message(&events, "rss ttl_seconds clamped to the engine's ceiling");
        assert_eq!(clamped.len(), 1, "one warning per engine built");
        assert_eq!(clamped[0].level, tracing::Level::WARN);
        assert_eq!(
            clamped[0].field("configured_ttl_seconds"),
            Some(u64::MAX.to_string().as_str())
        );
        assert_eq!(
            clamped[0].field("effective_ttl_seconds"),
            Some(MAX_TTL.as_secs().to_string().as_str())
        );
    }

    /// `max_concurrent` is stored raw by `RssConfig` — validation rejects
    /// only zero — and `Semaphore::new` panics above
    /// `Semaphore::MAX_PERMITS`, so an absurd-but-valid YAML integer would
    /// otherwise abort registration with a panic instead of a clamp. The
    /// sibling of the ttl and scan-timeout clamp tests, to the same shape:
    /// the clamp *and* the warning.
    #[tokio::test]
    async fn an_absurd_max_concurrent_is_clamped_rather_than_panicking() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        // Set before the engine is built: `with_parts` is where the clamp and
        // its warning happen — and where the unclamped value would panic.
        let (_guard, events) = capture_events();
        let feeds = vec![("a".to_string(), format!("{}/f.xml", server.url()))];
        let cache = Arc::new(MemoryFeedCache::new(CACHE_MAX_BYTES, 64));
        let engine = engine_with_cache(&feeds, 900, usize::MAX, cache);

        // Construction survived, and the semaphore actually hands out
        // permits: a serve completes.
        let batch = engine.serve_feed("a", || true).await.expect("rows served");
        assert_eq!(str_col(&batch, "window_status"), vec!["fresh"]);

        let clamped = events_with_message(
            &events,
            "rss max_concurrent clamped to the semaphore's ceiling",
        );
        assert_eq!(clamped.len(), 1, "one warning per engine built");
        assert_eq!(clamped[0].level, tracing::Level::WARN);
        assert_eq!(
            clamped[0].field("configured_max_concurrent"),
            Some(usize::MAX.to_string().as_str())
        );
        assert_eq!(
            clamped[0].field("effective_max_concurrent"),
            Some(Semaphore::MAX_PERMITS.to_string().as_str())
        );
    }

    /// [`arm`]'s fallback, exercised directly.
    ///
    /// [`MAX_TTL`] and [`failure_fuse`]'s own clamp keep every duration that
    /// reaches `arm` in production far inside the representable range, so this
    /// is the only place the `checked_add` is reachable. Without it the `+`
    /// operator would panic here instead of returning `now`, and a panic mid-scan
    /// is what the fallback exists to avoid.
    #[test]
    fn arm_saturates_to_now_rather_than_panicking_on_an_unrepresentable_add() {
        let now = Instant::now();
        assert_eq!(
            arm(now, Duration::MAX),
            now,
            "an add no platform Instant can represent leaves the feed due immediately"
        );
        // The ordinary case still moves the deadline forward, so the fallback is
        // not simply swallowing every add.
        assert!(arm(now, Duration::from_secs(30)) > now);
    }

    /// The engine arms a failed feed with `failure_fuse(self.ttl)`, not a
    /// hardcoded floor.
    ///
    /// `cache.rs`'s `failure_fuse_is_clamped` pins the function
    /// (`clamp(ttl / 4, 30s, 300s)`, `cache.rs:286-288`); nothing pinned which
    /// duration the engine passes it. Every other engine test configures
    /// `ttl_seconds` 0 or 900 and then observes only "the dead feed is not
    /// re-poked on the next serve" — which a hardcoded 30s would satisfy just as
    /// well, since no test can wait a fuse out.
    ///
    /// At `ttl_seconds: 900` the fuse is `900 / 4 = 225s`, strictly between the
    /// 30s floor and the 300s ceiling, so the armed instant discriminates the
    /// real expression from either clamp arm. 225 is spelled out rather than
    /// computed from `failure_fuse`, so the assertion cannot agree with a
    /// changed one.
    #[tokio::test]
    async fn a_failure_arms_the_ttls_quarter_not_the_floor() {
        // Guard, not an assertion target: this test reaches the `rss feed
        // degraded` warn callsite, and `tracing` caches a callsite's `Interest`
        // globally on first use — a guardless test reaching it first would cache
        // `Interest::never` and silently empty
        // `a_degraded_feed_emits_a_warning_naming_the_feed_and_reason`.
        let (_interest_guard, _) = capture_events();
        let server = MockFeedServer::start(|_| MockResponse::status(500)).await;
        let urls = vec![("a".to_string(), format!("{}/f.xml", server.url()))];
        let cache = Arc::new(RecordsFailureArm::new(MemoryFeedCache::new(
            CACHE_MAX_BYTES,
            8,
        )));
        let engine = engine_with_cache(&urls, 900, 4, Arc::clone(&cache) as Arc<dyn FeedCache>);

        let before = Instant::now();
        assert!(engine.serve_feed("a", || true).await.is_none());
        let after = Instant::now();

        let armed = cache.armed_instants();
        assert_eq!(armed.len(), 1, "one failed serve records one arm");
        // `arm` ran somewhere in `before..=after`, so the duration it added is
        // bracketed by these two and 225s has to fall inside the bracket.
        let lower = armed[0].duration_since(after);
        let upper = armed[0].duration_since(before);
        let expected = Duration::from_secs(225);
        assert!(
            lower <= expected && expected <= upper,
            "the engine armed a fuse in {lower:?}..={upper:?}; ttl_seconds 900 must arm \
             failure_fuse(900s) = 225s — neither the 30s floor nor the 300s ceiling"
        );
    }
}
