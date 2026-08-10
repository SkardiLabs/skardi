//! Per-feed TTL cache: the health record and parsed window a scan reads
//! between fetches.
//!
//! Each feed key holds two things that age independently:
//!
//! - an [`FeedObservation`] — the feed's health (status, last error, dialect,
//!   item count, ...) — which persists across every call that doesn't remove
//!   its whole entry: [`FeedCache::record_success`] replaces it wholesale,
//!   [`FeedCache::record_failure`] and [`FeedCache::record_not_modified`]
//!   mutate select fields of it in place, and only [`MemoryFeedCache`]'s
//!   last-resort `max_observations` backstop (see below) ever discards one
//!   outright; and
//! - an optional [`CachedWindow`] — the parsed `items` batch plus the
//!   conditional-GET validators that produced it — which the byte/entry
//!   budget can evict independently of the observation.
//!
//! ## TTL re-arms on every attempt
//!
//! [`FeedCache::record_success`], [`FeedCache::record_not_modified`], and
//! [`FeedCache::record_failure`] all take an `armed_until: Instant` the
//! caller computes and push the feed's next-attempt time to exactly that
//! instant, regardless of which of the three ran. This is what makes a dead
//! feed a bounded cost rather than a per-scan one: a failure is retried at
//! most once per [`failure_fuse`] window instead of on every scan, and a
//! server's `Retry-After` (folded into the caller's `armed_until`) survives
//! across scans instead of resetting the moment the attempt that read it
//! finishes.
//!
//! ## Only a complete window is ever stored
//!
//! The only method that takes a window is [`FeedCache::record_success`] —
//! there is no partial-window setter, so a half-parsed feed has no path into
//! the cache.
//!
//! ## Eviction keeps the observation
//!
//! When the byte or entry budget forces a window out, [`MemoryFeedCache`]
//! drops the [`CachedWindow`] (batch and validators together — they live in
//! the same `Option`) but leaves the feed's [`FeedObservation`] in place, so
//! a `feeds` scan — specified to be a pure state read that never fetches —
//! never loses a feed's health just because its window happened to be
//! evicted.
//!
//! A separate, much larger `max_observations` bound exists purely as a
//! last-resort backstop: `feed` is a fully generic `&str`, nothing in this
//! module restricts it to a known subscription list, so the map of
//! observations alone (unlike the windowed subset above) has no bound
//! without one. See [`MemoryFeedCache`]'s doc for why removing a whole entry
//! there does not conflict with the guarantee just described.
//!
//! ## A `304` can outlive the window it would have confirmed
//!
//! Eviction is driven by *other* feeds' `record_success` calls, so it can land
//! between the moment a caller reads a feed's validators out of
//! [`FeedCache::snapshot`] and the moment the resulting `304` comes back. The
//! conditional request was well-formed and the answer is truthful — the
//! caller's copy *is* still current upstream — but the copy itself is gone, so
//! there is nothing left to serve. This is a reachable state under memory
//! pressure, not a caller bug: [`FeedCache::record_not_modified`] handles it
//! by re-arming the timer like any other attempt and recording
//! [`FeedStatus::Error`] with [`WINDOW_EVICTED_ON_REVALIDATION`] as the
//! reason, so the zero rows that follow have an explanation an operator can
//! read. See that method's doc for why `Error` rather than `Revalidated`.
//!
//! ## A single window larger than the whole byte budget
//!
//! [`MemoryFeedCache::record_success`] measures the incoming window with
//! `RecordBatch::get_array_memory_size()` before inserting anything. If that
//! alone exceeds `max_bytes`, the window can never fit no matter what else is
//! evicted, so it is never stored — not even transiently — while the
//! observation is still recorded. Any window the feed previously held is
//! dropped too: `record_success` always replaces wholesale, so an
//! oversized new window does not leave a stale one behind under the new
//! observation's identity.
//!
//! ## No in-flight coalescing, generation-checked commits
//!
//! Two scans that both find the same feed's TTL expired can both fetch it —
//! there is no request coalescing here, matching `open_connector`'s cache;
//! singleflight stays a documented future extension (the engine's module
//! doc records its trade-offs). What this module does own is keeping those
//! racing commits from corrupting the store: the [`FeedCache`] trait's
//! commit-generation contract drops a commit whose snapshot the world has
//! moved past, so completion order cannot regress a window or mislabel one.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;

use super::error::{MAX_ERROR_CHARS, MAX_FEED_TEXT_CHARS, truncate};

/// A feed's freshness state, and the single source of the exact strings the
/// `feeds.last_status` column serves.
///
/// Callers must go through [`FeedStatus::as_str`] rather than writing the
/// literal — `"never"`, `"fresh"`, `"revalidated"`, `"stale-error"`,
/// `"error"` — themselves. Within this module, `as_str` is the only place
/// that spells those strings for `feeds.last_status`, so a typo like
/// `stale_error` fails to compile here instead of silently breaking that
/// column's contract.
///
/// The same holds for `items.window_status`: `schema.rs`'s two sites both go
/// through [`FeedStatus::window_status_str`] — `build_items_batch` stamps a
/// freshly built window from [`FeedStatus::Fresh`], and `with_window_status`
/// takes a [`FeedStatus`] rather than a `&str` — so neither column's domain
/// can be broken by a typo at a call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FeedStatus {
    /// No fetch attempt has ever completed for this feed.
    #[default]
    Never,
    /// The last attempt fetched a fresh body and parsed it successfully.
    Fresh,
    /// The last attempt got a conditional `304`; the cached window is still
    /// current.
    Revalidated,
    /// The last attempt failed, but a window from an earlier success is
    /// still cached and can be served stale.
    StaleError,
    /// The last attempt failed and no window is cached to fall back on.
    Error,
}

impl FeedStatus {
    /// The exact string this status serializes to as `feeds.last_status`.
    pub fn as_str(&self) -> &'static str {
        match self {
            FeedStatus::Never => "never",
            FeedStatus::Fresh => "fresh",
            FeedStatus::Revalidated => "revalidated",
            FeedStatus::StaleError => "stale-error",
            FeedStatus::Error => "error",
        }
    }

    /// The `items.window_status` label a snapshot with this status should
    /// serve, or `None` for the two statuses that serve zero rows (nothing
    /// has ever been fetched, or the last attempt failed with nothing
    /// cached to fall back on).
    pub fn window_status_str(&self) -> Option<&'static str> {
        match self {
            FeedStatus::Fresh => Some("fresh"),
            FeedStatus::Revalidated => Some("revalidated"),
            FeedStatus::StaleError => Some("stale-error"),
            FeedStatus::Never | FeedStatus::Error => None,
        }
    }
}

/// One feed's health, independent of whether its window is currently cached.
///
/// `Default` is the state of a feed that has never been attempted: every
/// field `None` and [`FeedStatus::Never`].
///
/// Every feed- or server-controlled string retained here is length-capped.
/// Six of the fields below are written from the wire: `title` and
/// `description` carry the document's own text, `site_url` its alternate
/// link, `dialect_declared` and `conformance_notes` sniffs that quote it, and
/// `last_error` a diagnostic that may. Nothing downstream bounds any of them —
/// [`MemoryFeedCache`]'s budget meters `RecordBatch` bytes only (see
/// [`MemoryFeedCache::record_success`]), so an observation is never charged
/// against `max_bytes` at all — and an observation deliberately outlives the
/// window it describes (the module doc's "eviction keeps the observation"), so
/// an uncapped string here is retained past the point where the bytes it came
/// from are gone. A single `<title>` may be as large as `max_response_bytes`.
///
/// [`FeedObservation::capped`] is where that holds: it is applied where an
/// observation is stored, so the bound is a property of this boundary rather
/// than something each construction site has to remember. Adding a
/// feed-controlled string field means adding it there too.
#[derive(Debug, Clone, Default)]
pub struct FeedObservation {
    pub last_fetch_ms: Option<i64>,
    pub last_status: FeedStatus,
    pub http_status: Option<u16>,
    pub last_error: Option<String>,
    pub dialect: Option<String>,
    pub dialect_declared: Option<String>,
    pub conformance_notes: Option<String>,
    pub title: Option<String>,
    pub site_url: Option<String>,
    pub description: Option<String>,
    pub item_count: Option<u64>,
}

impl FeedObservation {
    /// This observation with every feed- or server-controlled string bounded
    /// to its column's cap — see the struct doc for why one is needed at all.
    ///
    /// Two caps, because the fields divide into two kinds of text.
    /// `title` and `description` are the feed's own prose and get the looser
    /// [`MAX_FEED_TEXT_CHARS`]; `site_url`, `dialect_declared`,
    /// `conformance_notes`, and `last_error` are identifiers and diagnostics
    /// and get [`MAX_ERROR_CHARS`], the bound every other feed-influenced
    /// diagnostic in this provider is held to.
    ///
    /// `dialect` is absent because it is never feed-controlled: its only
    /// writer is [`super::conformance::parsed_dialect`], which returns a
    /// `&'static str` from a closed set. `last_error` is included even though
    /// its writers cap it already, so the guarantee at this boundary is total
    /// rather than inherited from every upstream remembering to. Non-string
    /// fields are untouched — this is a length bound and nothing else.
    pub fn capped(mut self) -> Self {
        fn cap(text: Option<String>, max_chars: usize) -> Option<String> {
            text.map(|t| truncate(&t, max_chars))
        }
        self.last_error = cap(self.last_error, MAX_ERROR_CHARS);
        self.dialect_declared = cap(self.dialect_declared, MAX_ERROR_CHARS);
        self.conformance_notes = cap(self.conformance_notes, MAX_ERROR_CHARS);
        self.site_url = cap(self.site_url, MAX_ERROR_CHARS);
        self.title = cap(self.title, MAX_FEED_TEXT_CHARS);
        self.description = cap(self.description, MAX_FEED_TEXT_CHARS);
        self
    }
}

/// A feed's cached window: the parsed `items` batch plus the conditional-GET
/// validators that produced it. The two travel together — a batch is never
/// kept without the validators that would let a future fetch revalidate it,
/// and eviction drops both at once by dropping this whole struct.
#[derive(Debug, Clone)]
pub struct CachedWindow {
    pub batch: RecordBatch,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

/// A point-in-time read of one feed's cached state.
#[derive(Debug, Clone)]
pub struct FeedSnapshot {
    pub observation: FeedObservation,
    pub window: Option<CachedWindow>,
    pub within_ttl: bool,
    /// Commit generation this snapshot was read at — the ticket the three
    /// `record_*` methods check before committing (see the trait doc). `0`
    /// for a feed with no entry; the counter hands out values from `1`, so
    /// `0` never collides with a live entry's generation.
    pub generation: u64,
}

/// Per-feed TTL cache. A sync trait with a `Mutex` behind each implementation
/// (see [`MemoryFeedCache`]) rather than an async one, so a later persistent
/// implementation can swap in without touching callers — every observation of
/// this cache's state flows through these four methods.
///
/// ## Commit generations
///
/// With no in-flight coalescing (see the module doc), two scans can fetch
/// the same feed concurrently and their commits arrive in completion order.
/// Each `record_*` therefore carries `expected_generation` — the
/// [`FeedSnapshot::generation`] its fetch was decided from — and a commit
/// whose generation no longer matches the entry's is *stale*: something
/// else committed while this response was in flight. A stale commit is
/// dropped, with one exception: a stale *success* still supersedes a
/// failure-labelled state (each impl documents its rule inline). The gate
/// is what keeps completion-order commits from regressing the window to an
/// older response, stamping `revalidated` on a window the `304`'s
/// validators never came from, or arming the failure fuse against a window
/// a faster fetch just refreshed.
pub trait FeedCache: Send + Sync {
    /// Read `feed`'s current state as of `now`. Unknown feeds report
    /// [`FeedStatus::Never`], no window, and `within_ttl: false`.
    fn snapshot(&self, feed: &str, now: Instant) -> FeedSnapshot;

    /// Record a successful fetch-and-parse: replace both the window and the
    /// observation wholesale, and arm the feed's next-attempt time to
    /// `armed_until`.
    fn record_success(
        &self,
        feed: &str,
        expected_generation: u64,
        window: CachedWindow,
        observation: FeedObservation,
        armed_until: Instant,
    );

    /// Record a conditional `304`: the existing window is still current, so
    /// it is left untouched, but the observation's fetch metadata and status
    /// move to [`FeedStatus::Revalidated`], and the next-attempt time re-arms
    /// to `armed_until`.
    ///
    /// If the window was evicted while this attempt was in flight — see the
    /// module doc — the fetch metadata and the re-arm still apply (an attempt
    /// happened, and skipping the re-arm would leave the feed expired and
    /// refetching on every scan), but the status becomes
    /// [`FeedStatus::Error`] with [`WINDOW_EVICTED_ON_REVALIDATION`] as
    /// `last_error` rather than [`FeedStatus::Revalidated`].
    ///
    /// `Error` is the honest answer there even though the feed itself is
    /// healthy. It is the one status whose contract is "no window is cached to
    /// fall back on", which is exactly the situation; it is also the status
    /// whose [`FeedStatus::window_status_str`] is `None`, so *from the next scan
    /// on* it agrees with the zero rows that scan will serve. `Revalidated`
    /// would instead claim a current window exists indefinitely — leaving
    /// `feeds` reporting a healthy feed with a non-zero `item_count` while
    /// `items` returns nothing, and no column anywhere explaining why.
    ///
    /// The serve that *records* this state is the one exception: the engine
    /// already read a window out of its pre-permit snapshot and stamps those
    /// rows `revalidated` from a literal, so for that one racing scan
    /// `items.window_status = 'revalidated'` sits beside
    /// `feeds.last_status = 'error'`. That is the same no-coalescing
    /// consequence the engine's `304` branch documents — the rows it serves are
    /// a real window the `304` really did vouch for — and it lasts exactly one
    /// scan.
    ///
    /// Recovery does not depend on this choice: with no window there are no
    /// validators to send, so the next attempt after the timer expires is an
    /// unconditional `GET` that rebuilds the window whatever status was
    /// recorded here.
    fn record_not_modified(
        &self,
        feed: &str,
        expected_generation: u64,
        http_status: u16,
        last_fetch_ms: i64,
        armed_until: Instant,
    );

    /// Record a failed attempt: the observation's status becomes
    /// [`FeedStatus::StaleError`] if a window is currently cached (serve
    /// stale) or [`FeedStatus::Error`] if not, `last_error` is recorded, and
    /// the next-attempt time re-arms to `armed_until` regardless — this is
    /// the negative-caching half of the TTL contract.
    ///
    /// `dialect_declared` carries a sniff that succeeded even though the
    /// attempt as a whole did not: the declared dialect is read off the raw
    /// bytes before parsing, so a document that fails to parse still has one
    /// to report. `Some` overwrites the recorded value, `None` leaves
    /// whatever was already known in place — a transport failure has no
    /// document to sniff and must not erase an earlier success's answer.
    fn record_failure(
        &self,
        feed: &str,
        expected_generation: u64,
        http_status: Option<u16>,
        error: String,
        dialect_declared: Option<String>,
        last_fetch_ms: i64,
        armed_until: Instant,
    );

    /// Record an egress-policy refusal: like [`FeedCache::record_failure`],
    /// except the cached window — validators included — is dropped rather
    /// than kept for stale serving, so the status is always
    /// [`FeedStatus::Error`].
    ///
    /// A refusal is a policy verdict, not a transient fault, and
    /// `StaleError`'s contract — "temporarily unreachable, serve the last
    /// good read" — is wrong for a destination the *active* policy forbids:
    /// the policy may have changed since the window was fetched, or the host
    /// may now resolve somewhere denied (DNS rebinding), and the design
    /// requires a refused subscription to contribute zero item rows while
    /// `feeds` records the refusal (the design doc's failure-mode table and
    /// acceptance criterion 15). Dropping the window is what makes that
    /// stick: the denial's own serve has nothing to emit, and every
    /// within-fuse scan after it takes the cache-hit path into the same zero
    /// rows — `Error` claims no window, so `window_lost` does not force a
    /// refetch — instead of resurrecting the stale window. The observation
    /// survives, as everywhere else; once the fuse expires, a re-allowed
    /// fetch rebuilds the window with an unconditional `GET` (its validators
    /// went with it).
    ///
    /// No `http_status` and no `dialect_declared`: a refusal happens before
    /// any connection, so there is no response to describe.
    fn record_egress_denial(
        &self,
        feed: &str,
        expected_generation: u64,
        error: String,
        last_fetch_ms: i64,
        armed_until: Instant,
    );
}

/// `feeds.last_error` for a `304` whose window was evicted while the request
/// was in flight — see [`FeedCache::record_not_modified`]. Names no particular
/// bound, since any of the three can produce the state: `max_bytes` and
/// `max_entries` through `Inner::evict`, and the `max_observations` backstop
/// through `Inner::evict_observations`. Carries no response content of any
/// kind: the condition is entirely local to this cache, and the string is a
/// fixed literal.
pub const WINDOW_EVICTED_ON_REVALIDATION: &str = "revalidated (304) but the cached window had already been evicted from the feed \
     cache; the next attempt refetches it unconditionally";

/// The negative-cache TTL for a feed that just failed: `clamp(ttl / 4, 30s,
/// 300s)`. Shorter than the success TTL so a dead feed is retried sooner
/// than a healthy one's normal refresh, but still bounded away from zero
/// when `ttl_seconds` is 0 (always-live) so a failing always-live feed is not
/// retried on literally every scan.
pub fn failure_fuse(ttl: Duration) -> Duration {
    (ttl / 4).clamp(Duration::from_secs(30), Duration::from_secs(300))
}

/// How many times `max_entries` the last-resort whole-entry bound
/// (`max_observations`) is set to, before the [`MIN_OBSERVATIONS`] floor is
/// applied. `max_entries` bounds feeds holding a window, sized by the
/// operator for a realistic number of concurrently fetched feeds; feeds that
/// are failing or being revalidated hold no window at all and so don't count
/// against it, yet still occupy a map slot. 8x gives that traffic
/// comfortable headroom over the windowed bound before the backstop below
/// ever engages, while still capping unbounded growth in the number of
/// distinct feed keys ever seen (`feed` is a plain `&str` — nothing here
/// restricts it to a known subscription list).
const MAX_OBSERVATIONS_MULTIPLIER: usize = 8;

/// Floor under `max_observations`, so a caller-supplied `max_entries` of `0`
/// (window caching fully disabled — a coherent configuration: `feeds`
/// health is still worth tracking with no window cache at all) cannot turn
/// the backstop into "discard the observation the instant it's inserted."
/// 8 mirrors `1 * MAX_OBSERVATIONS_MULTIPLIER` — the headroom the smallest
/// *window-caching* configuration (`max_entries == 1`) already gets — so
/// disabling window caching entirely doesn't collapse observation tracking
/// below what the smallest windowed configuration would have.
const MIN_OBSERVATIONS: usize = 8;

/// A cached window plus the byte count it was charged against the budget
/// with, so eviction can subtract exactly what was added.
struct WindowEntry {
    window: CachedWindow,
    bytes: usize,
}

/// One feed's full cache entry: an always-present observation, and an
/// optional window that the byte/entry budget can evict independently.
///
/// Recency lives here, as a tick per entry, rather than in a list of keys
/// held beside the map. A key list has to be *searched* to move a feed to the
/// front, so every access costs a scan of every other feed — and `snapshot`
/// runs once per feed per scan, against a list `max_observations` (8x
/// `max_entries`) long, with `max_entries` itself sized to the subscription
/// count. That is quadratic in the number of subscriptions for bookkeeping a
/// cache hit does not need. The order is only ever *read* when a bound is
/// exceeded, so it is recorded here in O(1) and derived by the scans in
/// `evict`/`evict_observations`, which are the rare path.
struct Entry {
    observation: FeedObservation,
    window: Option<WindowEntry>,
    armed_until: Instant,
    /// Value of [`Inner::commit_counter`] at this entry's last accepted
    /// commit — what `expected_generation` is checked against. `0` only
    /// before the first commit, matching the `0` a snapshot reports for an
    /// absent entry (the counter hands out values from `1`).
    generation: u64,
    /// Tick of the last access of any kind — the order `max_observations`
    /// evicts by. Set by every one of the four [`FeedCache`] methods, so an
    /// observation-only entry that keeps getting queried or refreshed is
    /// exactly as protected from the backstop as a windowed one.
    last_used: u64,
    /// Tick of the last access that served or stored `window` — the order
    /// `max_bytes`/`max_entries` evict by. Meaningless while `window` is
    /// `None`, which is why the scan that reads it filters on that first.
    window_last_used: u64,
}

impl Entry {
    fn new(armed_until: Instant) -> Self {
        Self {
            observation: FeedObservation::default(),
            window: None,
            armed_until,
            generation: 0,
            last_used: 0,
            window_last_used: 0,
        }
    }
}

struct Inner {
    map: HashMap<String, Entry>,
    window_bytes: usize,
    /// How many entries in `map` hold a window — what `max_entries` bounds,
    /// kept as a count so the check stays O(1) instead of a scan on every
    /// write. See [`MemoryFeedCache`]'s doc for why `max_entries` bounds this
    /// rather than the whole map. It changes in exactly two places,
    /// [`Inner::store_window`] and [`Inner::drop_window`], so it cannot drift
    /// from the map it describes.
    windowed: usize,
    /// Hands out the strictly increasing ticks the entries above record. At
    /// one tick per nanosecond a `u64` lasts ~584 years, so wrap-around is not
    /// a case this reasons about.
    clock: u64,
    /// Hands out commit generations, cache-wide rather than per-entry: an
    /// entry evicted by the observation backstop and later recreated starts
    /// over at `generation: 0`, and a per-entry counter restarting with it
    /// would let a snapshot taken of the *old* entry's `0` falsely match the
    /// new entry's `0`. A cache-wide monotonic counter never re-issues a
    /// value, so a stale ticket can never match by accident. Same
    /// wrap-around argument as `clock`.
    commit_counter: u64,
}

impl Inner {
    /// The next tick. Strictly increasing, so no two accesses ever compare
    /// equal and the eviction scans always have a single minimum — the
    /// eviction order is therefore total, not merely a partial one broken by
    /// `HashMap` iteration order.
    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    /// The generation an accepted commit stamps on its entry. First value is
    /// `1`, so an absent entry's `0` (see [`FeedSnapshot::generation`]) is
    /// never re-issued.
    fn next_generation(&mut self) -> u64 {
        self.commit_counter += 1;
        self.commit_counter
    }

    /// Store `feed`'s window, charging its bytes and marking it used now.
    fn store_window(&mut self, feed: &str, window: CachedWindow, bytes: usize) {
        let tick = self.tick();
        let Some(entry) = self.map.get_mut(feed) else {
            return;
        };
        entry.window_last_used = tick;
        let previous = entry.window.replace(WindowEntry { window, bytes });

        match previous {
            // Replacing in place would otherwise leak the old window's bytes.
            // `record_success` drops first, so this arm is defensive.
            Some(old) => self.window_bytes = self.window_bytes.saturating_sub(old.bytes),
            None => self.windowed += 1,
        }
        self.window_bytes += bytes;
    }

    /// Drop `feed`'s window (if any), crediting its bytes and the windowed
    /// count back. A no-op if `feed` has no entry or no window.
    fn drop_window(&mut self, feed: &str) {
        let Some(entry) = self.map.get_mut(feed) else {
            return;
        };
        let Some(w) = entry.window.take() else {
            return;
        };
        self.window_bytes = self.window_bytes.saturating_sub(w.bytes);
        self.windowed -= 1;
    }

    /// Mark `feed`'s window used now.
    fn touch_window(&mut self, feed: &str) {
        let tick = self.tick();
        if let Some(entry) = self.map.get_mut(feed) {
            entry.window_last_used = tick;
        }
    }

    /// Mark `feed` used now, whether or not it currently holds a window.
    fn touch_entry(&mut self, feed: &str) {
        let tick = self.tick();
        if let Some(entry) = self.map.get_mut(feed) {
            entry.last_used = tick;
        }
    }

    /// The windowed feed whose window was least recently used. `None` when no
    /// feed holds one. O(map), and reached only from [`Inner::evict`].
    fn oldest_windowed(&self) -> Option<String> {
        self.map
            .iter()
            .filter(|(_, entry)| entry.window.is_some())
            .min_by_key(|(_, entry)| entry.window_last_used)
            .map(|(feed, _)| feed.clone())
    }

    /// The least recently used entry of any kind. O(map), and reached only
    /// from [`Inner::evict_observations`].
    fn oldest_entry(&self) -> Option<String> {
        self.map
            .iter()
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(feed, _)| feed.clone())
    }

    /// Evict least-recently-used windows (dropping only the window, never
    /// the observation) until both the byte and entry budgets hold.
    fn evict(&mut self, max_bytes: usize, max_entries: usize) {
        while self.windowed > max_entries || self.window_bytes > max_bytes {
            let Some(oldest) = self.oldest_windowed() else {
                break;
            };
            self.drop_window(&oldest);
        }
    }

    /// Last-resort backstop: evict least-recently-used *whole* entries,
    /// observation included, until the map holds at most `max_observations`
    /// distinct feed keys. See [`MemoryFeedCache`]'s doc for why this
    /// coexists with (rather than contradicts) `evict`'s observation-
    /// preserving eviction above.
    fn evict_observations(&mut self, max_observations: usize) {
        while self.map.len() > max_observations {
            let Some(oldest) = self.oldest_entry() else {
                break;
            };
            // Through `drop_window` so the byte and windowed counts are
            // credited by the one place that maintains them, rather than by a
            // second copy of that arithmetic here.
            self.drop_window(&oldest);
            self.map.remove(&oldest);
        }
    }
}

/// In-memory [`FeedCache`], bounded by window bytes, window count, and
/// (as a last resort) total observation count, behind a `Mutex`
/// (hand-rolled LRU, following `open_connector/cache.rs`'s precedent rather
/// than adding the `lru` crate). Recency is a tick recorded on each entry
/// rather than a list of keys kept beside the map — see `Entry` (private,
/// so named in code font, not linked) for why that shape and not the other.
///
/// `max_entries` bounds how many feeds may hold a cached window at once —
/// it does not bound the map itself, since a feed that is only failing or
/// being revalidated holds no window and so never counts against it. That
/// gap is what `max_observations` (`max(max_entries *
/// MAX_OBSERVATIONS_MULTIPLIER, MIN_OBSERVATIONS)`) closes: it
/// caps the map's total size — windowed and observation-only entries
/// together — and, once exceeded, evicts the least-recently-used *whole*
/// entry, observation included, via `Inner::evict_observations`. The floor
/// matters at `max_entries == 0` (window caching disabled entirely, a
/// coherent configuration on its own): without it, `max_observations` would
/// also be `0`, and the backstop would discard every observation the moment
/// it was inserted instead of leaving observation tracking intact.
///
/// This does not contradict the "eviction keeps the observation" guarantee
/// the module doc describes for `max_bytes`/`max_entries`: that guarantee
/// exists so a `feeds` scan can still report health for a feed whose
/// *window* was evicted under memory pressure while the feed is still being
/// actively scanned. A whole entry only reaches `max_observations` once the
/// map holds far more distinct keys than the configured subscription list
/// could produce for `max_entries`-many concurrently windowed feeds — i.e.
/// keys that are no longer being scanned at all — and for a key this cache
/// has never seen, [`FeedCache::snapshot`] already answers
/// [`FeedStatus::Never`], which is the honest answer for a subscription this
/// cache has no record of. The two bounds protect different things: one
/// keeps a live feed's health visible after its window is reclaimed, the
/// other keeps the map itself from growing without limit.
pub struct MemoryFeedCache {
    inner: Mutex<Inner>,
    max_bytes: usize,
    max_entries: usize,
    max_observations: usize,
}

impl MemoryFeedCache {
    pub fn new(max_bytes: usize, max_entries: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                window_bytes: 0,
                windowed: 0,
                clock: 0,
                commit_counter: 0,
            }),
            max_bytes,
            max_entries,
            max_observations: max_entries
                .saturating_mul(MAX_OBSERVATIONS_MULTIPLIER)
                .max(MIN_OBSERVATIONS),
        }
    }

    /// Lock the inner state. Poisoning degrades to the inner state rather
    /// than panicking, matching `open_connector/cache.rs`'s `ScanCache`.
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }
}

impl FeedCache for MemoryFeedCache {
    fn snapshot(&self, feed: &str, now: Instant) -> FeedSnapshot {
        let mut inner = self.lock();
        let Some(entry) = inner.map.get(feed) else {
            return FeedSnapshot {
                observation: FeedObservation::default(),
                window: None,
                within_ttl: false,
                generation: 0,
            };
        };
        let within_ttl = now < entry.armed_until;
        let observation = entry.observation.clone();
        let window = entry.window.as_ref().map(|w| w.window.clone());
        let generation = entry.generation;
        let has_window = window.is_some();

        if has_window {
            inner.touch_window(feed);
        }
        // A read is still an access for the whole-entry LRU, whether or not
        // this feed currently holds a window — see `Inner::touch_entry`.
        inner.touch_entry(feed);
        FeedSnapshot {
            observation,
            window,
            within_ttl,
            generation,
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
        // Measured before taking the lock: a batch's memory size is a pure
        // function of its own buffers, so there's no need to hold the lock
        // while computing it.
        let bytes = window.batch.get_array_memory_size();
        let mut inner = self.lock();

        // Generation gate — see the trait doc. The one stale commit that is
        // *accepted* is a success superseding a failure-labelled (or absent)
        // state: dropping it would leave the feed negatively cached for up
        // to the failure fuse despite a successful response in hand, which
        // is worse than either racing response winning.
        let (current, status) = inner.map.get(feed).map_or((0, None), |e| {
            (e.generation, Some(e.observation.last_status))
        });
        if current != expected_generation
            && !matches!(
                status,
                None | Some(FeedStatus::Error | FeedStatus::StaleError)
            )
        {
            tracing::debug!(
                feed,
                commit = "success",
                expected_generation,
                current_generation = current,
                "rss stale cache commit dropped"
            );
            return;
        }
        let generation = inner.next_generation();

        // `record_success` always replaces wholesale: whatever window this
        // feed held before is gone now, whether or not the new one below
        // ends up fitting.
        inner.drop_window(feed);

        let entry = inner
            .map
            .entry(feed.to_string())
            .or_insert_with(|| Entry::new(armed_until));
        entry.generation = generation;
        // The one place a whole observation enters the store, and so where its
        // strings are bounded — see [`FeedObservation::capped`]. Applied here
        // rather than at the caller so no future construction site can omit it.
        entry.observation = observation.capped();
        entry.armed_until = armed_until;

        // A window that alone exceeds the whole byte budget can never fit no
        // matter what else is evicted — see the module doc. Store the
        // observation (already done above) but skip the window entirely
        // rather than insert-then-immediately-evict it.
        if bytes <= self.max_bytes {
            inner.store_window(feed, window, bytes);
            inner.evict(self.max_bytes, self.max_entries);
        }

        inner.touch_entry(feed);
        inner.evict_observations(self.max_observations);
    }

    fn record_not_modified(
        &self,
        feed: &str,
        expected_generation: u64,
        http_status: u16,
        last_fetch_ms: i64,
        armed_until: Instant,
    ) {
        let mut inner = self.lock();
        // Generation gate — see the trait doc. A stale `304` has nothing to
        // salvage: its status stamp would label a window its validators
        // never came from, and the re-arm it would perform was already done
        // by whatever committed in between.
        let current = inner.map.get(feed).map_or(0, |e| e.generation);
        if current != expected_generation {
            tracing::debug!(
                feed,
                commit = "not-modified",
                expected_generation,
                current_generation = current,
                "rss stale cache commit dropped"
            );
            return;
        }
        let generation = inner.next_generation();
        let entry = inner
            .map
            .entry(feed.to_string())
            .or_insert_with(|| Entry::new(armed_until));
        entry.generation = generation;

        // An attempt happened and it is the caller's `armed_until` that
        // decides when the next one may, so the fetch metadata and the re-arm
        // are unconditional. In particular they must not be skipped when the
        // window turns out to be gone: the module doc's TTL contract is that
        // all three recording methods push the next-attempt time to exactly
        // `armed_until`, and a `304` that quietly declined to would leave the
        // feed expired and refetching on every scan.
        entry.observation.http_status = Some(http_status);
        entry.observation.last_fetch_ms = Some(last_fetch_ms);
        entry.armed_until = armed_until;

        if entry.window.is_some() {
            entry.observation.last_status = FeedStatus::Revalidated;
            entry.observation.last_error = None;
        } else {
            // Reachable without any caller mistake — another feed's
            // `record_success` can evict this one's window between the
            // snapshot that produced the validators and the `304` answering
            // them — so this is reported, not asserted. See the trait
            // method's doc for why `Error` is the honest status.
            entry.observation.last_status = FeedStatus::Error;
            // Routed through the cap like every other write to this column
            // rather than relying on the literal above staying short. It is 131
            // characters today, so this is a no-op; the point is that the
            // column has one bound and no writer sits outside it.
            entry.observation.last_error =
                Some(truncate(WINDOW_EVICTED_ON_REVALIDATION, MAX_ERROR_CHARS));
            tracing::warn!(
                feed,
                http_status,
                "rss feed revalidated but its cached window had already been evicted"
            );
        }

        inner.touch_entry(feed);
        // Normally a no-op — the entry existed already, since a conditional
        // request needs validators this cache handed out — but `entry()` above
        // can insert, so the bound is re-checked rather than assumed.
        inner.evict_observations(self.max_observations);
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
        let mut inner = self.lock();
        // Generation gate — see the trait doc. A stale failure must not
        // stamp `stale-error` over — nor arm the failure fuse against —
        // state committed by a fetch that outran this one; if the
        // intervening commit was itself a failure, the only loss is the
        // newer error string.
        let current = inner.map.get(feed).map_or(0, |e| e.generation);
        if current != expected_generation {
            tracing::debug!(
                feed,
                commit = "failure",
                expected_generation,
                current_generation = current,
                "rss stale cache commit dropped"
            );
            return;
        }
        let generation = inner.next_generation();
        let entry = inner
            .map
            .entry(feed.to_string())
            .or_insert_with(|| Entry::new(armed_until));
        entry.generation = generation;
        // A failed refresh does not invalidate a previously cached window —
        // it is left exactly as it was, available to serve stale, and
        // determines which of the two failure statuses applies.
        entry.observation.last_status = if entry.window.is_some() {
            FeedStatus::StaleError
        } else {
            FeedStatus::Error
        };
        entry.observation.http_status = http_status;
        entry.observation.last_error = Some(error);
        entry.observation.last_fetch_ms = Some(last_fetch_ms);
        // Only overwrite when the caller actually has a sniff to report — see
        // the trait method's doc.
        if dialect_declared.is_some() {
            entry.observation.dialect_declared = dialect_declared;
        }
        entry.armed_until = armed_until;
        // The other path that writes feed- and server-controlled strings into a
        // retained observation, so it is held to the same bound `record_success`
        // is, through the same entry point. Both fields it just wrote are capped
        // by their own writers too; going through `capped()` here is what keeps
        // that a redundancy rather than the only thing holding the invariant up.
        let observation = std::mem::take(&mut entry.observation);
        entry.observation = observation.capped();

        inner.touch_entry(feed);
        inner.evict_observations(self.max_observations);
    }

    fn record_egress_denial(
        &self,
        feed: &str,
        expected_generation: u64,
        error: String,
        last_fetch_ms: i64,
        armed_until: Instant,
    ) {
        let mut inner = self.lock();
        // Generation gate — see the trait doc. Same drop-always rule as
        // `record_failure`: a stale denial must not purge a window a fetch
        // that outran this one just committed under an allowing verdict.
        let current = inner.map.get(feed).map_or(0, |e| e.generation);
        if current != expected_generation {
            tracing::debug!(
                feed,
                commit = "egress-denial",
                expected_generation,
                current_generation = current,
                "rss stale cache commit dropped"
            );
            return;
        }
        let generation = inner.next_generation();
        // The one difference from `record_failure`: the window goes — see
        // the trait method's doc for why a policy refusal must not leave
        // content behind to serve stale.
        inner.drop_window(feed);
        let entry = inner
            .map
            .entry(feed.to_string())
            .or_insert_with(|| Entry::new(armed_until));
        entry.generation = generation;
        entry.observation.last_status = FeedStatus::Error;
        entry.observation.http_status = None;
        entry.observation.last_error = Some(error);
        entry.observation.last_fetch_ms = Some(last_fetch_ms);
        entry.armed_until = armed_until;
        // Same cap discipline as the other write paths: the denial string is
        // built from policy-side facts, but the bound belongs to the store,
        // not to trust in any particular writer.
        let observation = std::mem::take(&mut entry.observation);
        entry.observation = observation.capped();

        inner.touch_entry(feed);
        inner.evict_observations(self.max_observations);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::UInt64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// A minimal window with `rows` rows of a single `UInt64` column — real
    /// enough to size and count, without pulling in `schema::build_items_batch`
    /// (this module doesn't depend on the `items` schema shape at all).
    fn window_with_rows(rows: usize) -> CachedWindow {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
        let ids: UInt64Array = (0..rows as u64).collect();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ids)]).unwrap();
        CachedWindow {
            batch,
            etag: Some("\"etag\"".into()),
            last_modified: Some("Mon, 20 Jul 2026 10:00:00 GMT".into()),
        }
    }

    fn obs_fresh(item_count: u64) -> FeedObservation {
        FeedObservation {
            last_fetch_ms: Some(1_700_000_000_000),
            last_status: FeedStatus::Fresh,
            http_status: Some(200),
            last_error: None,
            dialect: Some("rss2".into()),
            dialect_declared: Some("rss2".into()),
            conformance_notes: None,
            title: Some("Feed".into()),
            site_url: Some("https://example.com".into()),
            description: Some("desc".into()),
            item_count: Some(item_count),
        }
    }

    /// Every field an over-long value, so a cap that is missing or applied to
    /// the wrong field shows up as a wrong length rather than passing by luck.
    fn obs_all_strings_long(chars: usize) -> FeedObservation {
        let long = "x".repeat(chars);
        FeedObservation {
            last_fetch_ms: Some(1_700_000_000_000),
            last_status: FeedStatus::Fresh,
            http_status: Some(200),
            last_error: Some(long.clone()),
            dialect: Some("rss-2.0".into()),
            dialect_declared: Some(long.clone()),
            conformance_notes: Some(long.clone()),
            title: Some(long.clone()),
            site_url: Some(long.clone()),
            description: Some(long),
            item_count: Some(7),
        }
    }

    /// Every feed- or server-controlled string is bounded, each at its own
    /// column's cap: prose (`title`, `description`) at `MAX_FEED_TEXT_CHARS`,
    /// identifiers and diagnostics at `MAX_ERROR_CHARS`. Nothing downstream
    /// bounds these — the cache's budget meters `RecordBatch` bytes only — so
    /// this is the only place the length is decided.
    #[test]
    fn capped_bounds_every_feed_controlled_string_at_its_own_cap() {
        let capped = obs_all_strings_long(10_000).capped();

        assert_eq!(
            capped.title.as_ref().unwrap().chars().count(),
            MAX_FEED_TEXT_CHARS
        );
        assert_eq!(
            capped.description.as_ref().unwrap().chars().count(),
            MAX_FEED_TEXT_CHARS
        );
        assert_eq!(
            capped.site_url.as_ref().unwrap().chars().count(),
            MAX_ERROR_CHARS
        );
        assert_eq!(
            capped.dialect_declared.as_ref().unwrap().chars().count(),
            MAX_ERROR_CHARS
        );
        assert_eq!(
            capped.conformance_notes.as_ref().unwrap().chars().count(),
            MAX_ERROR_CHARS
        );
        assert_eq!(
            capped.last_error.as_ref().unwrap().chars().count(),
            MAX_ERROR_CHARS,
            "capped again here even though its writers cap it, so the bound at \
             this boundary does not depend on them"
        );
    }

    /// The conservativeness direction: a realistic observation passes through
    /// unchanged. A cap applied to the wrong field, or one off by a character,
    /// is visible here as a value that no longer matches what went in.
    #[test]
    fn capped_leaves_values_within_bounds_byte_identical() {
        let before = FeedObservation {
            last_fetch_ms: Some(1_700_000_000_000),
            last_status: FeedStatus::Fresh,
            http_status: Some(200),
            last_error: None,
            dialect: Some("rss-2.0".into()),
            dialect_declared: Some("rss-2.0".into()),
            conformance_notes: Some("duplicate-identity: 1".into()),
            title: Some("Daily Notes on Distributed Systems".into()),
            site_url: Some("https://example.com/blog/index.html".into()),
            description: Some("Occasional writing about storage engines.".into()),
            item_count: Some(42),
        };
        let after = before.clone().capped();

        assert_eq!(after.title, before.title);
        assert_eq!(after.description, before.description);
        assert_eq!(after.site_url, before.site_url);
        assert_eq!(after.dialect_declared, before.dialect_declared);
        assert_eq!(after.conformance_notes, before.conformance_notes);
        assert_eq!(after.last_error, before.last_error);
        assert_eq!(
            after.dialect, before.dialect,
            "never feed-controlled, never cut"
        );
    }

    /// The cap counts characters, not bytes, so a feed writing in a script
    /// whose scalars are 3 or 4 bytes wide is not cut to a third of the
    /// allowance — and never mid-scalar. `truncate` decides this; the point
    /// here is that the field-level use inherits it.
    #[test]
    fn capped_counts_characters_on_multi_byte_text() {
        let title = "标题".repeat(10_000);
        // Spelled as an escape: a single 4-byte scalar, with no chance of a
        // variation selector riding along and making the byte count ambiguous.
        let description = "\u{1F680}".repeat(10_000);
        let capped = FeedObservation {
            title: Some(title),
            description: Some(description),
            ..FeedObservation::default()
        }
        .capped();

        let title = capped.title.expect("title survives");
        assert_eq!(title.chars().count(), MAX_FEED_TEXT_CHARS);
        // 3-byte scalars: a byte-counting cap would have kept a third of these.
        assert_eq!(title.len(), MAX_FEED_TEXT_CHARS * 3);
        let description = capped.description.expect("description survives");
        assert_eq!(description.chars().count(), MAX_FEED_TEXT_CHARS);
        assert_eq!(description.len(), MAX_FEED_TEXT_CHARS * 4);
    }

    /// A pure length bound: the fields that carry no feed text are the feed's
    /// health, and `capped()` must not be a place where health can change.
    #[test]
    fn capped_leaves_non_string_fields_alone() {
        let before = obs_all_strings_long(10_000);
        let after = before.clone().capped();

        assert_eq!(after.item_count, before.item_count);
        assert_eq!(after.http_status, before.http_status);
        assert_eq!(after.last_fetch_ms, before.last_fetch_ms);
        assert!(matches!(after.last_status, FeedStatus::Fresh));
    }

    /// The bound is reached through the cache's own API, not only by calling
    /// `capped()` directly: `record_success` is where an observation enters the
    /// store, so a caller that never heard of the cap still cannot get an
    /// unbounded string retained. The window it stored alongside is untouched —
    /// `items` content is bounded by the byte budget instead, and must stay
    /// verbatim.
    #[test]
    fn record_success_caps_the_observation_it_stores_and_leaves_the_window_whole() {
        let cache = MemoryFeedCache::new(1 << 20, 64);
        let t0 = Instant::now();
        cache.record_success(
            "a",
            0,
            window_with_rows(3),
            obs_all_strings_long(10_000),
            t0 + Duration::from_secs(900),
        );

        let snap = cache.snapshot("a", t0 + Duration::from_secs(1));
        assert_eq!(
            snap.observation.title.as_ref().unwrap().chars().count(),
            MAX_FEED_TEXT_CHARS
        );
        assert_eq!(
            snap.observation.site_url.as_ref().unwrap().chars().count(),
            MAX_ERROR_CHARS
        );
        assert_eq!(snap.window.as_ref().unwrap().batch.num_rows(), 3);
    }

    /// `record_failure` writes feed- and server-controlled strings straight
    /// into a retained observation, so it is held to the same bound rather
    /// than trusting its caller to have applied one.
    #[test]
    fn record_failure_caps_the_strings_it_writes() {
        let cache = MemoryFeedCache::new(1 << 20, 64);
        let t0 = Instant::now();
        cache.record_failure(
            "a",
            0,
            Some(500),
            "x".repeat(10_000),
            Some(format!("unknown:{}", "y".repeat(10_000))),
            1,
            t0 + Duration::from_secs(30),
        );

        let snap = cache.snapshot("a", t0 + Duration::from_secs(1));
        assert_eq!(
            snap.observation
                .last_error
                .as_ref()
                .unwrap()
                .chars()
                .count(),
            MAX_ERROR_CHARS
        );
        assert_eq!(
            snap.observation
                .dialect_declared
                .as_ref()
                .unwrap()
                .chars()
                .count(),
            MAX_ERROR_CHARS
        );
    }

    #[test]
    fn success_arms_ttl_and_snapshot_reports_within_ttl() {
        let cache = MemoryFeedCache::new(1 << 20, 64);
        let t0 = Instant::now();
        cache.record_success(
            "a",
            0,
            window_with_rows(2),
            obs_fresh(2),
            t0 + Duration::from_secs(900),
        );
        let snap = cache.snapshot("a", t0 + Duration::from_secs(1));
        assert!(snap.within_ttl);
        assert!(matches!(snap.observation.last_status, FeedStatus::Fresh));
        assert_eq!(snap.window.as_ref().unwrap().batch.num_rows(), 2);
        assert!(
            !cache
                .snapshot("a", t0 + Duration::from_secs(901))
                .within_ttl
        );
    }

    #[test]
    fn failure_is_negative_cached_with_window_kept() {
        let cache = MemoryFeedCache::new(1 << 20, 64);
        let t0 = Instant::now();
        cache.record_success("a", 0, window_with_rows(2), obs_fresh(2), t0); // expired immediately
        cache.record_failure(
            "a",
            1,
            Some(503),
            "http status 503".into(),
            None,
            1,
            t0 + Duration::from_secs(30),
        );
        let snap = cache.snapshot("a", t0 + Duration::from_secs(1));
        assert!(
            snap.within_ttl,
            "failure re-armed the timer (negative cache)"
        );
        assert!(matches!(
            snap.observation.last_status,
            FeedStatus::StaleError
        ));
        assert!(
            snap.window.is_some(),
            "stale window retained for serve-stale"
        );
        assert_eq!(
            snap.observation.last_error.as_deref(),
            Some("http status 503")
        );
    }

    #[test]
    fn failure_without_window_is_error_status() {
        let cache = MemoryFeedCache::new(1 << 20, 64);
        let t0 = Instant::now();
        cache.record_failure(
            "a",
            0,
            Some(500),
            "http status 500".into(),
            None,
            1,
            t0 + Duration::from_secs(30),
        );
        let snap = cache.snapshot("a", t0 + Duration::from_secs(1));
        assert!(matches!(snap.observation.last_status, FeedStatus::Error));
        assert!(snap.window.is_none());
        assert_eq!(
            snap.observation.last_error.as_deref(),
            Some("http status 500")
        );
        assert!(
            snap.within_ttl,
            "failure still arms the negative-cache timer"
        );
    }

    #[test]
    fn eviction_drops_window_and_validators_but_keeps_observation() {
        let one = window_with_rows(2);
        let bytes = one.batch.get_array_memory_size();
        // Room for exactly one window: inserting a second must evict the first.
        let cache = MemoryFeedCache::new(bytes + 8, 64);
        let t0 = Instant::now();
        let armed = t0 + Duration::from_secs(900);
        cache.record_success("a", 0, window_with_rows(2), obs_fresh(2), armed);
        cache.record_success("b", 0, window_with_rows(2), obs_fresh(3), armed);

        let snap_a = cache.snapshot("a", t0 + Duration::from_secs(1));
        assert!(
            snap_a.window.is_none(),
            "a's window (and its validators) must be evicted to make room for b"
        );
        assert!(
            matches!(snap_a.observation.last_status, FeedStatus::Fresh),
            "the observation survives eviction of its window"
        );
        assert_eq!(snap_a.observation.item_count, Some(2));

        let snap_b = cache.snapshot("b", t0 + Duration::from_secs(1));
        assert_eq!(snap_b.window.as_ref().unwrap().batch.num_rows(), 2);
    }

    #[test]
    fn unknown_feed_snapshot_is_never() {
        let cache = MemoryFeedCache::new(1 << 20, 64);
        let snap = cache.snapshot("ghost", Instant::now());
        assert!(matches!(snap.observation.last_status, FeedStatus::Never));
        assert!(snap.window.is_none());
        assert!(!snap.within_ttl);
    }

    #[test]
    fn failure_fuse_is_clamped() {
        assert_eq!(
            failure_fuse(Duration::from_secs(0)),
            Duration::from_secs(30)
        );
        assert_eq!(
            failure_fuse(Duration::from_secs(900)),
            Duration::from_secs(225)
        );
        assert_eq!(
            failure_fuse(Duration::from_secs(10_000)),
            Duration::from_secs(300)
        );
    }

    #[test]
    fn not_modified_rearms_and_flips_to_revalidated() {
        let cache = MemoryFeedCache::new(1 << 20, 64);
        let t0 = Instant::now();
        cache.record_success("a", 0, window_with_rows(2), obs_fresh(2), t0); // expired immediately
        cache.record_not_modified("a", 1, 304, 1, t0 + Duration::from_secs(900));
        let snap = cache.snapshot("a", t0 + Duration::from_secs(1));
        assert!(
            snap.within_ttl,
            "a 304 re-arms the timer exactly like success/failure do"
        );
        assert!(matches!(
            snap.observation.last_status,
            FeedStatus::Revalidated
        ));
        assert_eq!(snap.observation.http_status, Some(304));
        assert_eq!(snap.observation.last_fetch_ms, Some(1));
        assert!(
            snap.window.is_some(),
            "the existing window is kept across revalidation"
        );
        assert_eq!(snap.window.as_ref().unwrap().batch.num_rows(), 2);
    }

    /// A `304` whose window was evicted while the request was in flight. This
    /// state is reachable without any caller mistake — the eviction below is
    /// driven entirely by another feed's `record_success` against the byte
    /// budget, exactly as it would be in production — so the contract is that
    /// it re-arms and reports, in *both* build profiles. It previously fired a
    /// `debug_assert!` (a panic inside a partition's poll for every debug
    /// build, integration suites included) and returned early without arming,
    /// which left the feed expired and refetching on every scan.
    #[test]
    fn record_not_modified_after_window_eviction_rearms_and_records_it() {
        let one = window_with_rows(1);
        let bytes = one.batch.get_array_memory_size();
        // Room for exactly one window, so "b" arriving evicts "a"'s.
        let cache = MemoryFeedCache::new(bytes + 8, 64);
        let t0 = Instant::now();
        // "a" is armed to `t0`, i.e. already expired — which is the only state
        // that makes a caller send validators in the first place, and what
        // makes the re-arm below the *only* thing that can put the feed back
        // within its TTL.
        cache.record_success("a", 0, window_with_rows(1), obs_fresh(1), t0);
        // "a" holds a window and its validators; a scan would send them now.
        assert!(cache.snapshot("a", t0).window.is_some());
        assert!(!cache.snapshot("a", t0 + Duration::from_secs(1)).within_ttl);

        // Natural byte pressure from another feed drops "a"'s window while
        // that conditional request is in flight.
        cache.record_success(
            "b",
            0,
            window_with_rows(1),
            obs_fresh(1),
            t0 + Duration::from_secs(900),
        );
        assert!(
            cache.snapshot("a", t0).window.is_none(),
            "b's window evicted a's under the byte budget"
        );

        // No panic in either profile: run it directly rather than through
        // `catch_unwind`, so a reintroduced assertion fails this test.
        cache.record_not_modified("a", 1, 304, 42, t0 + Duration::from_secs(600));

        let snap = cache.snapshot("a", t0 + Duration::from_secs(1));
        assert!(
            snap.within_ttl,
            "the 304 re-armed the timer even though the window was gone — \
             otherwise the feed refetches on every scan"
        );
        assert!(
            matches!(snap.observation.last_status, FeedStatus::Error),
            "no window means zero rows, and Error is the status that says so: {:?}",
            snap.observation.last_status
        );
        assert_eq!(
            snap.observation.last_error.as_deref(),
            Some(WINDOW_EVICTED_ON_REVALIDATION),
            "the zero rows an operator will see have a stated reason"
        );
        assert_eq!(snap.observation.http_status, Some(304));
        assert_eq!(snap.observation.last_fetch_ms, Some(42));
        assert!(snap.window.is_none());
        // The earlier success's identity survives, as eviction always leaves
        // the observation in place.
        assert_eq!(snap.observation.item_count, Some(1));
    }

    #[test]
    fn lru_touch_order_respected() {
        let one = window_with_rows(1);
        let bytes = one.batch.get_array_memory_size();
        // Room for exactly two windows.
        let cache = MemoryFeedCache::new(bytes * 2 + 8, 64);
        let t0 = Instant::now();
        let armed = t0 + Duration::from_secs(900);
        cache.record_success("a", 0, window_with_rows(1), obs_fresh(1), armed);
        cache.record_success("b", 0, window_with_rows(1), obs_fresh(1), armed);
        // Touch "a" so "b" becomes the least-recently-used entry.
        assert!(
            cache
                .snapshot("a", t0 + Duration::from_secs(1))
                .window
                .is_some()
        );
        cache.record_success("c", 0, window_with_rows(1), obs_fresh(1), armed);

        assert!(
            cache
                .snapshot("a", t0 + Duration::from_secs(1))
                .window
                .is_some(),
            "recently touched entry stays"
        );
        assert!(
            cache
                .snapshot("b", t0 + Duration::from_secs(1))
                .window
                .is_none(),
            "least-recently-used entry is evicted, not the touched one"
        );
        assert!(
            cache
                .snapshot("c", t0 + Duration::from_secs(1))
                .window
                .is_some()
        );
    }

    #[test]
    fn max_entries_bound_evicts_lru_window() {
        // Bytes budget is generous; only the entry-count bound is at play.
        let cache = MemoryFeedCache::new(1 << 20, 2);
        let t0 = Instant::now();
        let armed = t0 + Duration::from_secs(900);
        cache.record_success("a", 0, window_with_rows(1), obs_fresh(1), armed);
        cache.record_success("b", 0, window_with_rows(1), obs_fresh(1), armed);
        cache.record_success("c", 0, window_with_rows(1), obs_fresh(1), armed);

        let snap_a = cache.snapshot("a", t0 + Duration::from_secs(1));
        assert!(
            snap_a.window.is_none(),
            "the entry-count bound must evict the LRU window, not just the byte budget"
        );
        assert!(matches!(snap_a.observation.last_status, FeedStatus::Fresh));
        assert!(
            cache
                .snapshot("b", t0 + Duration::from_secs(1))
                .window
                .is_some()
        );
        assert!(
            cache
                .snapshot("c", t0 + Duration::from_secs(1))
                .window
                .is_some()
        );
    }

    #[test]
    fn max_observations_backstop_evicts_lru_whole_entry() {
        // `max_entries` of 1 sets `max_observations` to `1 * 8 = 8` (see
        // `MAX_OBSERVATIONS_MULTIPLIER`). Every feed here is recorded via
        // `record_failure` with no window at all, so this drives the
        // whole-entry backstop in isolation from the windowed bound above.
        let cache = MemoryFeedCache::new(1 << 20, 1);
        let t0 = Instant::now();
        let armed = t0 + Duration::from_secs(30);
        for i in 0..8 {
            cache.record_failure(
                &format!("feed-{i}"),
                0,
                Some(500),
                "http status 500".into(),
                None,
                1,
                armed,
            );
        }
        // 8 distinct keys now fill the map exactly to `max_observations`;
        // touch "feed-0" so it is not the least-recently-used entry once a
        // 9th key arrives.
        let touched = cache.snapshot("feed-0", t0 + Duration::from_secs(1));
        assert!(matches!(touched.observation.last_status, FeedStatus::Error));

        // A 9th distinct feed key pushes the map to 9 entries, over the
        // 8-entry `max_observations` bound; "feed-1" — never touched again
        // after its own insertion, and now the least-recently-used key —
        // must be the one dropped, whole entry and all.
        cache.record_failure(
            "feed-8",
            0,
            Some(500),
            "http status 500".into(),
            None,
            1,
            armed,
        );

        let evicted = cache.snapshot("feed-1", t0 + Duration::from_secs(1));
        assert!(
            matches!(evicted.observation.last_status, FeedStatus::Never),
            "the least-recently-used whole entry, observation included, must be gone"
        );
        let survivor = cache.snapshot("feed-0", t0 + Duration::from_secs(1));
        assert!(
            matches!(survivor.observation.last_status, FeedStatus::Error),
            "the recently touched entry survives with its observation intact"
        );
    }

    /// `windowed` and `window_bytes` are counters kept beside the map rather
    /// than derived from it, so what the eviction bounds test is only as true
    /// as the two places that maintain them. This drives every path that
    /// stores or drops a window — a first success, a replacing success, a
    /// failure that must leave the window alone, a byte-budget eviction, and
    /// the whole-entry backstop — and then compares both counters against the
    /// map itself.
    #[test]
    fn window_accounting_agrees_with_the_map_after_every_path() {
        let bytes = window_with_rows(1).batch.get_array_memory_size();
        // Room for two windows, so the third success evicts.
        let cache = MemoryFeedCache::new(bytes * 2 + 8, 8);
        let t0 = Instant::now();
        let armed = t0 + Duration::from_secs(900);

        cache.record_success("a", 0, window_with_rows(1), obs_fresh(1), armed);
        cache.record_success("b", 0, window_with_rows(1), obs_fresh(1), armed);
        // Replacing an existing window must credit the old bytes back.
        // ("a"'s entry holds generation 1 from its first commit above.)
        cache.record_success("a", 1, window_with_rows(1), obs_fresh(1), armed);
        // A failure leaves "a"'s window in place to serve stale. ("a" now
        // holds generation 3: commits are numbered cache-wide in call order.)
        cache.record_failure("a", 3, Some(500), "boom".into(), None, 1, armed);
        // Over the byte budget: one window is evicted, its observation kept.
        cache.record_success("c", 0, window_with_rows(1), obs_fresh(1), armed);
        // Windowless keys, driving the whole-entry backstop at 8 * 8 = 64.
        for i in 0..70 {
            cache.record_failure(
                &format!("f{i}"),
                0,
                Some(500),
                "boom".into(),
                None,
                1,
                armed,
            );
        }

        let inner = cache.lock();
        assert_eq!(
            inner.windowed,
            inner.map.values().filter(|e| e.window.is_some()).count(),
            "the windowed count must equal the windows the map actually holds"
        );
        assert_eq!(
            inner.window_bytes,
            inner
                .map
                .values()
                .filter_map(|e| e.window.as_ref())
                .map(|w| w.bytes)
                .sum::<usize>(),
            "the byte total must equal the bytes the map actually holds"
        );
    }

    #[test]
    fn max_entries_zero_disables_window_cache_but_keeps_observations() {
        // `max_entries = 0` is a coherent way to disable window caching
        // entirely while still wanting `feeds` health tracked. Without the
        // `MIN_OBSERVATIONS` floor, `max_observations` would also be 0 and
        // the backstop would discard each observation the instant it was
        // inserted; this pins the floor keeping it alive instead.
        let cache = MemoryFeedCache::new(1 << 20, 0);
        let t0 = Instant::now();
        let armed = t0 + Duration::from_secs(900);

        cache.record_success("a", 0, window_with_rows(2), obs_fresh(2), armed);
        let snap_a = cache.snapshot("a", t0 + Duration::from_secs(1));
        assert!(
            snap_a.window.is_none(),
            "max_entries = 0 must disable window caching, not just shrink it"
        );
        assert!(matches!(snap_a.observation.last_status, FeedStatus::Fresh));

        // Insert a second, different feed — with no floor, `max_observations`
        // would be 0 and this insert's `evict_observations` call would have
        // already discarded "a"'s observation before this snapshot ever runs.
        cache.record_success("b", 0, window_with_rows(1), obs_fresh(1), armed);
        let snap_a_after = cache.snapshot("a", t0 + Duration::from_secs(1));
        assert!(
            matches!(snap_a_after.observation.last_status, FeedStatus::Fresh),
            "the floor must keep a's observation alive across a later, unrelated insert"
        );
        let snap_b = cache.snapshot("b", t0 + Duration::from_secs(1));
        assert!(matches!(snap_b.observation.last_status, FeedStatus::Fresh));
    }

    /// The 200/200 commit race: two scans snapshot the same generation, both
    /// fetch, and the slower response commits second. Without the generation
    /// gate the second commit would replace the first wholesale — the window
    /// regressing to the older response while labelled `fresh`.
    #[test]
    fn a_second_success_from_the_same_snapshot_is_dropped() {
        let cache = MemoryFeedCache::new(1 << 20, 64);
        let t0 = Instant::now();
        let armed = t0 + Duration::from_secs(900);

        // Both scans read generation 0 (no entry yet). The fast fetch
        // commits first; the slow one still carries the shared ticket.
        cache.record_success("a", 0, window_with_rows(2), obs_fresh(2), armed);
        cache.record_success("a", 0, window_with_rows(1), obs_fresh(1), armed);

        let snap = cache.snapshot("a", t0 + Duration::from_secs(1));
        assert_eq!(
            snap.window.as_ref().unwrap().batch.num_rows(),
            2,
            "the first-committed window stays; the stale commit is dropped"
        );
        assert_eq!(snap.observation.item_count, Some(2));
    }

    /// The 200/304 commit race: a `304` whose validators came from the
    /// pre-race window must not stamp `revalidated` on — nor re-arm — the
    /// window a concurrent `200` committed in between.
    #[test]
    fn a_stale_304_does_not_label_a_window_it_never_validated() {
        let cache = MemoryFeedCache::new(1 << 20, 64);
        let t0 = Instant::now();

        // The primed window both racing scans snapshot: generation 1,
        // already expired so both fetch.
        cache.record_success("a", 0, window_with_rows(1), obs_fresh(1), t0);
        // The concurrent full `200` wins the race.
        cache.record_success(
            "a",
            1,
            window_with_rows(2),
            obs_fresh(2),
            t0 + Duration::from_secs(900),
        );
        // The `304` arrives late, its validators minted from generation 1.
        cache.record_not_modified("a", 1, 304, 99, t0 + Duration::from_secs(600));

        let snap = cache.snapshot("a", t0 + Duration::from_secs(1));
        assert!(
            matches!(snap.observation.last_status, FeedStatus::Fresh),
            "not `revalidated`: the 304 never saw the two-row window"
        );
        assert_eq!(snap.window.as_ref().unwrap().batch.num_rows(), 2);
        assert_ne!(
            snap.observation.last_fetch_ms,
            Some(99),
            "the dropped 304 must not update fetch metadata either"
        );
    }

    /// A slow failure must not stamp `stale-error` over — nor arm the
    /// failure fuse against — a window a faster concurrent fetch just
    /// refreshed.
    #[test]
    fn a_stale_failure_does_not_degrade_a_fresher_success() {
        let cache = MemoryFeedCache::new(1 << 20, 64);
        let t0 = Instant::now();

        cache.record_success("a", 0, window_with_rows(1), obs_fresh(1), t0);
        // Both racing scans snapshot generation 1; the success commits first,
        // armed 900s out.
        cache.record_success(
            "a",
            1,
            window_with_rows(2),
            obs_fresh(2),
            t0 + Duration::from_secs(900),
        );
        // The slow failure would have armed the 30s fuse.
        cache.record_failure(
            "a",
            1,
            Some(500),
            "http status 500".into(),
            None,
            7,
            t0 + Duration::from_secs(30),
        );

        let snap = cache.snapshot("a", t0 + Duration::from_secs(1));
        assert!(matches!(snap.observation.last_status, FeedStatus::Fresh));
        assert_eq!(snap.observation.last_error, None);
        assert!(
            cache
                .snapshot("a", t0 + Duration::from_secs(600))
                .within_ttl,
            "the success's 900s arm stands; the dropped failure's 30s fuse does not"
        );
    }

    /// The one stale commit that is accepted: a success superseding a
    /// failure-labelled state. Dropping it would leave the feed negatively
    /// cached for up to the failure fuse despite a successful response in
    /// hand.
    #[test]
    fn a_stale_success_supersedes_a_failure_commit() {
        let cache = MemoryFeedCache::new(1 << 20, 64);
        let t0 = Instant::now();

        // Concurrent first fetches: both snapshot generation 0. The failure
        // commits first...
        cache.record_failure(
            "a",
            0,
            None,
            "transport error: connection refused".into(),
            None,
            1,
            t0 + Duration::from_secs(30),
        );
        // ...and the slower success still lands.
        cache.record_success(
            "a",
            0,
            window_with_rows(2),
            obs_fresh(2),
            t0 + Duration::from_secs(900),
        );

        let snap = cache.snapshot("a", t0 + Duration::from_secs(1));
        assert!(matches!(snap.observation.last_status, FeedStatus::Fresh));
        assert_eq!(snap.window.as_ref().unwrap().batch.num_rows(), 2);
        assert_eq!(snap.observation.last_error, None);
    }

    /// An egress refusal purges the window instead of keeping it for stale
    /// serving — a policy verdict must not leave forbidden content behind —
    /// while the observation records the refusal and the fuse still arms.
    #[test]
    fn an_egress_denial_drops_the_window_and_negative_caches() {
        let cache = MemoryFeedCache::new(1 << 20, 64);
        let t0 = Instant::now();
        cache.record_success("a", 0, window_with_rows(2), obs_fresh(2), t0); // expired at once
        cache.record_egress_denial(
            "a",
            1,
            "egress blocked: host 'feed.example' resolves to private address 10.0.0.1".into(),
            7,
            t0 + Duration::from_secs(30),
        );

        let snap = cache.snapshot("a", t0 + Duration::from_secs(1));
        assert!(
            snap.window.is_none(),
            "the refused feed's window (and validators) must be purged"
        );
        assert!(
            matches!(snap.observation.last_status, FeedStatus::Error),
            "`error`, not `stale-error`: there is deliberately nothing to serve stale"
        );
        assert!(
            snap.observation
                .last_error
                .as_deref()
                .unwrap()
                .contains("egress blocked")
        );
        assert!(
            snap.within_ttl,
            "a refusal negative-caches exactly like any other failure"
        );
    }

    /// A stale denial obeys the same generation rule as a stale failure: it
    /// must not purge a window a fetch that outran it just committed under
    /// an allowing verdict.
    #[test]
    fn a_stale_egress_denial_does_not_purge_a_fresher_success() {
        let cache = MemoryFeedCache::new(1 << 20, 64);
        let t0 = Instant::now();
        cache.record_success("a", 0, window_with_rows(1), obs_fresh(1), t0);
        // Both racing scans snapshot generation 1; the success commits first.
        cache.record_success(
            "a",
            1,
            window_with_rows(2),
            obs_fresh(2),
            t0 + Duration::from_secs(900),
        );
        cache.record_egress_denial(
            "a",
            1,
            "egress blocked: host 'feed.example' resolves to private address 10.0.0.1".into(),
            7,
            t0 + Duration::from_secs(30),
        );

        let snap = cache.snapshot("a", t0 + Duration::from_secs(1));
        assert!(matches!(snap.observation.last_status, FeedStatus::Fresh));
        assert_eq!(snap.window.as_ref().unwrap().batch.num_rows(), 2);
    }
}
