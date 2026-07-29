//! Per-feed TTL cache: the health record and parsed window a scan reads
//! between fetches.
//!
//! Each feed key holds two things that age independently:
//!
//! - an [`FeedObservation`] — the feed's health (status, last error, dialect,
//!   item count, ...) — which, once set, is only ever replaced, never
//!   dropped; and
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
//! ## No in-flight coalescing
//!
//! Two scans that both find the same feed's TTL expired can both fetch it —
//! there is no request coalescing here, matching `open_connector`'s cache.
//! That is a documented future extension, not this module's job.

// `FeedCache`/`MemoryFeedCache` have no production caller yet — Task 11 (the
// engine) is the first one. Until then, everything here outside of this
// module's own tests is unreferenced from a build that excludes test code,
// and `cargo check`/`cargo build` would otherwise flag it. Mirrors
// `fetch.rs`'s identical situation and identical fix; remove once Task 11
// wires `FeedCache` into the engine.
#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;

/// A feed's freshness state, and the single source of the exact strings the
/// `feeds.last_status` column serves.
///
/// Callers must go through [`FeedStatus::as_str`] rather than writing the
/// literal — `"never"`, `"fresh"`, `"revalidated"`, `"stale-error"`,
/// `"error"` — themselves. That method is the only place in non-test code
/// allowed to spell those strings, so a typo like `stale_error` fails to
/// compile instead of silently breaking this public SQL surface.
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
}

/// Per-feed TTL cache. A sync trait with a `Mutex` behind each implementation
/// (see [`MemoryFeedCache`]) rather than an async one, so a later persistent
/// implementation can swap in without touching callers — every observation of
/// this cache's state flows through these four methods.
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
        window: CachedWindow,
        observation: FeedObservation,
        armed_until: Instant,
    );

    /// Record a conditional `304`: the existing window is still current, so
    /// it is left untouched, but the observation's fetch metadata and status
    /// move to [`FeedStatus::Revalidated`], and the next-attempt time re-arms
    /// to `armed_until`.
    fn record_not_modified(
        &self,
        feed: &str,
        http_status: u16,
        last_fetch_ms: i64,
        armed_until: Instant,
    );

    /// Record a failed attempt: the observation's status becomes
    /// [`FeedStatus::StaleError`] if a window is currently cached (serve
    /// stale) or [`FeedStatus::Error`] if not, `last_error` is recorded, and
    /// the next-attempt time re-arms to `armed_until` regardless — this is
    /// the negative-caching half of the TTL contract.
    fn record_failure(
        &self,
        feed: &str,
        http_status: Option<u16>,
        error: String,
        last_fetch_ms: i64,
        armed_until: Instant,
    );
}

/// The negative-cache TTL for a feed that just failed: `clamp(ttl / 4, 30s,
/// 300s)`. Shorter than the success TTL so a dead feed is retried sooner
/// than a healthy one's normal refresh, but still bounded away from zero
/// when `ttl_seconds` is 0 (always-live) so a failing always-live feed is not
/// retried on literally every scan.
pub fn failure_fuse(ttl: Duration) -> Duration {
    (ttl / 4).clamp(Duration::from_secs(30), Duration::from_secs(300))
}

/// A cached window plus the byte count it was charged against the budget
/// with, so eviction can subtract exactly what was added.
struct WindowEntry {
    window: CachedWindow,
    bytes: usize,
}

/// One feed's full cache entry: an always-present observation, and an
/// optional window that the byte/entry budget can evict independently.
struct Entry {
    observation: FeedObservation,
    window: Option<WindowEntry>,
    armed_until: Instant,
}

struct Inner {
    map: HashMap<String, Entry>,
    /// Feeds that currently hold a window, most-recently-used first. Feeds
    /// with no window (never fetched, or evicted) are not tracked here —
    /// see [`MemoryFeedCache`]'s doc for why `max_entries` bounds this list
    /// rather than the whole map.
    window_order: VecDeque<String>,
    window_bytes: usize,
}

impl Inner {
    /// Drop `feed`'s window (if any) and remove it from the LRU list,
    /// crediting its bytes back to the budget. A no-op if `feed` has no
    /// entry or no window.
    fn drop_window(&mut self, feed: &str) {
        if let Some(entry) = self.map.get_mut(feed)
            && let Some(w) = entry.window.take()
        {
            self.window_bytes = self.window_bytes.saturating_sub(w.bytes);
        }
        self.window_order.retain(|k| k != feed);
    }

    /// Move `feed` to the front of the window LRU list.
    fn touch_window(&mut self, feed: &str) {
        self.window_order.retain(|k| k != feed);
        self.window_order.push_front(feed.to_string());
    }

    /// Evict least-recently-used windows (dropping only the window, never
    /// the observation) until both the byte and entry budgets hold.
    fn evict(&mut self, max_bytes: usize, max_entries: usize) {
        while self.window_order.len() > max_entries || self.window_bytes > max_bytes {
            let Some(oldest) = self.window_order.pop_back() else {
                break;
            };
            if let Some(entry) = self.map.get_mut(&oldest)
                && let Some(w) = entry.window.take()
            {
                self.window_bytes = self.window_bytes.saturating_sub(w.bytes);
            }
        }
    }
}

/// In-memory [`FeedCache`], bounded by window bytes and window count,
/// behind a `Mutex` (hand-rolled LRU, following `open_connector/cache.rs`'s
/// precedent rather than adding the `lru` crate).
///
/// `max_entries` bounds how many feeds may hold a cached window at once, not
/// the total number of feeds the map has ever seen: an observation is never
/// evicted, only replaced, and the set of feed keys is bounded already by
/// the (locally configured, not attacker-supplied) subscription list, so a
/// second cap on raw map size would add nothing. `max_entries` earns its
/// keep as a hard cap on window count independent of `max_bytes`, covering
/// the case where many small windows would otherwise slip under the byte
/// budget while still being too numerous.
pub struct MemoryFeedCache {
    inner: Mutex<Inner>,
    max_bytes: usize,
    max_entries: usize,
}

impl MemoryFeedCache {
    pub fn new(max_bytes: usize, max_entries: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                window_order: VecDeque::new(),
                window_bytes: 0,
            }),
            max_bytes,
            max_entries,
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
            };
        };
        let within_ttl = now < entry.armed_until;
        let observation = entry.observation.clone();
        let window = entry.window.as_ref().map(|w| w.window.clone());
        let has_window = window.is_some();

        if has_window {
            inner.touch_window(feed);
        }
        FeedSnapshot {
            observation,
            window,
            within_ttl,
        }
    }

    fn record_success(
        &self,
        feed: &str,
        window: CachedWindow,
        observation: FeedObservation,
        armed_until: Instant,
    ) {
        // Measured before taking the lock: a batch's memory size is a pure
        // function of its own buffers, so there's no need to hold the lock
        // while computing it.
        let bytes = window.batch.get_array_memory_size();
        let mut inner = self.lock();

        // `record_success` always replaces wholesale: whatever window this
        // feed held before is gone now, whether or not the new one below
        // ends up fitting.
        inner.drop_window(feed);

        let entry = inner.map.entry(feed.to_string()).or_insert_with(|| Entry {
            observation: FeedObservation::default(),
            window: None,
            armed_until,
        });
        entry.observation = observation;
        entry.armed_until = armed_until;

        // A window that alone exceeds the whole byte budget can never fit no
        // matter what else is evicted — see the module doc. Store the
        // observation (already done above) but skip the window entirely
        // rather than insert-then-immediately-evict it.
        if bytes <= self.max_bytes {
            entry.window = Some(WindowEntry { window, bytes });
            inner.window_bytes += bytes;
            inner.window_order.push_front(feed.to_string());
            inner.evict(self.max_bytes, self.max_entries);
        }
    }

    fn record_not_modified(
        &self,
        feed: &str,
        http_status: u16,
        last_fetch_ms: i64,
        armed_until: Instant,
    ) {
        let mut inner = self.lock();
        let has_window = inner.map.get(feed).is_some_and(|e| e.window.is_some());
        debug_assert!(
            has_window,
            "record_not_modified called for feed {feed:?} with no cached window: the \
             engine only sends conditional-GET validators when a window (and the \
             validators that came with it) are cached, so an unconditional 304 should be \
             unreachable in practice"
        );
        if !has_window {
            // No-op status-wise: without a window there is nothing this
            // 304 could be confirming is still current, so leave whatever
            // state (or absence of one) already exists untouched.
            return;
        }
        let entry = inner
            .map
            .get_mut(feed)
            .expect("has_window was true, so the entry exists");
        entry.observation.last_status = FeedStatus::Revalidated;
        entry.observation.http_status = Some(http_status);
        entry.observation.last_fetch_ms = Some(last_fetch_ms);
        entry.observation.last_error = None;
        entry.armed_until = armed_until;
    }

    fn record_failure(
        &self,
        feed: &str,
        http_status: Option<u16>,
        error: String,
        last_fetch_ms: i64,
        armed_until: Instant,
    ) {
        let mut inner = self.lock();
        let entry = inner.map.entry(feed.to_string()).or_insert_with(|| Entry {
            observation: FeedObservation::default(),
            window: None,
            armed_until,
        });
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
        entry.armed_until = armed_until;
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

    #[test]
    fn success_arms_ttl_and_snapshot_reports_within_ttl() {
        let cache = MemoryFeedCache::new(1 << 20, 64);
        let t0 = Instant::now();
        cache.record_success(
            "a",
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
        cache.record_success("a", window_with_rows(2), obs_fresh(2), t0); // expired immediately
        cache.record_failure(
            "a",
            Some(503),
            "http status 503".into(),
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
            Some(500),
            "http status 500".into(),
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
        cache.record_success("a", window_with_rows(2), obs_fresh(2), armed);
        cache.record_success("b", window_with_rows(2), obs_fresh(3), armed);

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
        cache.record_success("a", window_with_rows(2), obs_fresh(2), t0); // expired immediately
        cache.record_not_modified("a", 304, 1, t0 + Duration::from_secs(900));
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

    #[test]
    fn record_not_modified_without_window_is_unreachable_but_asserted() {
        let cache = MemoryFeedCache::new(1 << 20, 64);
        // No prior `record_success`, so there is no window — this is exactly
        // the "unconditional 304" case the module doc says the engine should
        // never produce; the cache still must not panic in a release build,
        // so assert the debug-only signal rather than a panic.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            cache.record_not_modified("ghost", 304, 1, Instant::now());
        }));
        if cfg!(debug_assertions) {
            assert!(
                result.is_err(),
                "expected the debug assertion to fire for an unconditional 304"
            );
        } else {
            assert!(result.is_ok());
        }
        let snap = cache.snapshot("ghost", Instant::now());
        assert!(
            matches!(snap.observation.last_status, FeedStatus::Never),
            "a no-op call must not fabricate a Revalidated status"
        );
    }

    #[test]
    fn lru_touch_order_respected() {
        let one = window_with_rows(1);
        let bytes = one.batch.get_array_memory_size();
        // Room for exactly two windows.
        let cache = MemoryFeedCache::new(bytes * 2 + 8, 64);
        let t0 = Instant::now();
        let armed = t0 + Duration::from_secs(900);
        cache.record_success("a", window_with_rows(1), obs_fresh(1), armed);
        cache.record_success("b", window_with_rows(1), obs_fresh(1), armed);
        // Touch "a" so "b" becomes the least-recently-used entry.
        assert!(
            cache
                .snapshot("a", t0 + Duration::from_secs(1))
                .window
                .is_some()
        );
        cache.record_success("c", window_with_rows(1), obs_fresh(1), armed);

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
        cache.record_success("a", window_with_rows(1), obs_fresh(1), armed);
        cache.record_success("b", window_with_rows(1), obs_fresh(1), armed);
        cache.record_success("c", window_with_rows(1), obs_fresh(1), armed);

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
}
