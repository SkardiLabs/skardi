//! Bounded TTL cache for completed scans.
//!
//! Live reads are the default; `ttl == 0` disables the cache entirely. The
//! cache is bounded by bytes and entries and evicts least-recently-used.
//! Keys are canonical (see [`scan_cache_key`]) so semantically identical
//! scans hit regardless of map/projection ordering.
//!
//! Scope: entries are written when a scan *completes*, so the cache dedups
//! repeated queries over time — it does **not** deduplicate scans that
//! overlap in time (e.g. the two sides of a self-join, which typically run
//! concurrently and start before either completes). In-flight request
//! coalescing is a future extension.
//!
//! Caching claims no transactional consistency: a live multi-page scan can
//! observe upstream changes between pages, subject to the provider's own
//! pagination guarantees.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use serde_json::{Value, json};

use crate::util::hash::blake3_hex;
use crate::util::json::canonical_json;

/// Default entry cap when the byte budget alone would allow unbounded growth.
const DEFAULT_MAX_ENTRIES: usize = 256;

/// Whole-scan cache entry.
struct CacheEntry {
    batches: Vec<RecordBatch>,
    bytes: usize,
    inserted_at: Instant,
}

/// Key components that uniquely identify one scan. Everything that could
/// change the result set is part of the key.
#[derive(Debug)]
pub struct ScanKeyParts<'a> {
    /// Gateway (data source) name.
    pub gateway: &'a str,
    /// Connection alias, when the binding uses one.
    pub connection_alias: Option<&'a str>,
    /// Open Connector action ID backing the table.
    pub action_id: &'a str,
    /// Source-pack version of the table definition.
    pub source_pack_version: u32,
    /// Resource inputs bound to the table (owner/repo, workspace, …).
    pub resource: &'a Value,
    /// Translated (pushed-down) filter inputs.
    pub filter_inputs: &'a [(String, Value)],
    /// Projected column names, in scan order.
    pub projection: &'a [String],
    /// SQL limit after pushdown. Part of the key because a truncated
    /// (LIMIT-incomplete) scan must never serve a fuller query.
    pub limit: Option<usize>,
    /// Fingerprint of the Arrow schema the scan emits.
    pub schema_fingerprint: &'a str,
}

/// Build the canonical cache key for one scan.
///
/// Resource maps are canonicalized (keys sorted recursively) and filter
/// inputs are sorted by field name, so `{"a":1,"b":2}` and `{"b":2,"a":1}`
/// hit the same entry. Projection order is significant — a different column
/// order is a different result shape.
pub fn scan_cache_key(parts: &ScanKeyParts) -> String {
    let mut filters: Vec<Value> = parts
        .filter_inputs
        .iter()
        .map(|(field, value)| json!([field, value]))
        .collect();
    filters.sort_by_key(|a| a.to_string());

    canonical_json(&json!({
        "gateway": parts.gateway,
        "connection_alias": parts.connection_alias,
        "action_id": parts.action_id,
        "source_pack_version": parts.source_pack_version,
        "resource": parts.resource,
        "filters": filters,
        "projection": parts.projection,
        "limit": parts.limit,
        "schema_fingerprint": parts.schema_fingerprint,
    }))
}

/// BLAKE3 fingerprint of an Arrow schema: field order is significant (it is
/// the emitted batch shape), fields are rendered `name:type:nullable`.
pub fn schema_fingerprint(schema: &SchemaRef) -> String {
    let mut canonical = String::new();
    for field in schema.fields() {
        canonical.push_str(field.name());
        canonical.push(':');
        canonical.push_str(&field.data_type().to_string());
        canonical.push(':');
        canonical.push_str(if field.is_nullable() { "1" } else { "0" });
        canonical.push(';');
    }
    blake3_hex(canonical.as_bytes())
}

struct CacheInner {
    entries: HashMap<String, CacheEntry>,
    /// Most-recently-used first.
    lru: VecDeque<String>,
    total_bytes: usize,
}

/// Bounded in-memory TTL cache behind a mutex. Lock poisoning degrades to
/// the inner state rather than panicking (repo convention).
pub struct ScanCache {
    inner: Mutex<CacheInner>,
    ttl: Duration,
    max_bytes: usize,
    max_entries: usize,
}

impl std::fmt::Debug for ScanCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (entries, bytes) = match self.inner.lock() {
            Ok(inner) => (inner.entries.len(), inner.total_bytes),
            Err(p) => {
                let inner = p.into_inner();
                (inner.entries.len(), inner.total_bytes)
            }
        };
        f.debug_struct("ScanCache")
            .field("ttl", &self.ttl)
            .field("max_bytes", &self.max_bytes)
            .field("max_entries", &self.max_entries)
            .field("entries", &entries)
            .field("bytes", &bytes)
            .finish()
    }
}

impl ScanCache {
    /// Create a cache. `ttl == Duration::ZERO` disables it (live reads).
    pub fn new(ttl: Duration, max_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(CacheInner {
                entries: HashMap::new(),
                lru: VecDeque::new(),
                total_bytes: 0,
            }),
            ttl,
            max_bytes,
            max_entries: DEFAULT_MAX_ENTRIES,
        }
    }

    /// Whether caching is enabled (`ttl > 0`).
    pub fn is_enabled(&self) -> bool {
        self.ttl > Duration::ZERO
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CacheInner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Look up a fresh entry. A hit refreshes recency; an expired entry is
    /// dropped and reported as a miss. Disabled caches always miss.
    pub fn get(&self, key: &str) -> Option<Vec<RecordBatch>> {
        if !self.is_enabled() {
            return None;
        }
        let mut inner = self.lock();
        let expired = inner
            .entries
            .get(key)
            .map(|entry| entry.inserted_at.elapsed() >= self.ttl);
        match expired {
            None => None,
            Some(true) => {
                if let Some(entry) = inner.entries.remove(key) {
                    inner.total_bytes = inner.total_bytes.saturating_sub(entry.bytes);
                }
                inner.lru.retain(|k| k != key);
                None
            }
            Some(false) => {
                inner.lru.retain(|k| k != key);
                inner.lru.push_front(key.to_string());
                inner.entries.get(key).map(|entry| entry.batches.clone())
            }
        }
    }

    /// Store a completed scan's batches, evicting least-recently-used entries
    /// until the byte and entry budgets hold. A batch set larger than the
    /// whole byte budget is not cached. Disabled caches drop silently.
    pub fn put(&self, key: String, batches: Vec<RecordBatch>) {
        if !self.is_enabled() {
            return;
        }
        let bytes: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
        if bytes > self.max_bytes {
            return;
        }

        let mut inner = self.lock();
        if let Some(old) = inner.entries.remove(&key) {
            inner.total_bytes = inner.total_bytes.saturating_sub(old.bytes);
            inner.lru.retain(|k| k != &key);
        }

        inner.entries.insert(
            key.clone(),
            CacheEntry {
                batches,
                bytes,
                inserted_at: Instant::now(),
            },
        );
        inner.total_bytes += bytes;
        inner.lru.push_front(key);

        while inner.entries.len() > self.max_entries || inner.total_bytes > self.max_bytes {
            let Some(oldest) = inner.lru.pop_back() else {
                break;
            };
            if let Some(entry) = inner.entries.remove(&oldest) {
                inner.total_bytes = inner.total_bytes.saturating_sub(entry.bytes);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{RecordBatch, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(rows: u64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
        let ids: UInt64Array = (0..rows).collect();
        RecordBatch::try_new(schema, vec![Arc::new(ids)]).unwrap()
    }

    fn parts<'a>(gateway: &'a str, projection: &'a [String]) -> ScanKeyParts<'a> {
        // `resource` points at a leaked test value so the returned parts
        // never borrow a temporary (tests are tiny; leaking is fine).
        ScanKeyParts {
            gateway,
            connection_alias: None,
            action_id: "mock.list_items",
            source_pack_version: 1,
            resource: Box::leak(Box::new(json!({"workspace": "demo"}))),
            filter_inputs: &[],
            projection,
            limit: None,
            schema_fingerprint: "fp",
        }
    }

    #[test]
    fn disabled_cache_never_hits() {
        let cache = ScanCache::new(Duration::ZERO, 1 << 20);
        let key = scan_cache_key(&parts("saas", &[]));
        cache.put(key.clone(), vec![batch(1)]);
        assert!(cache.get(&key).is_none());
        assert!(!cache.is_enabled());
    }

    #[test]
    fn fresh_entry_hits_and_expired_entry_misses() {
        let cache = ScanCache::new(Duration::from_millis(50), 1 << 20);
        let key = scan_cache_key(&parts("saas", &[]));
        cache.put(key.clone(), vec![batch(2)]);
        assert_eq!(cache.get(&key).map(|b| b[0].num_rows()), Some(2));

        std::thread::sleep(Duration::from_millis(80));
        assert!(cache.get(&key).is_none(), "entry past its TTL must miss");
    }

    #[test]
    fn empty_completed_scan_is_a_cache_hit() {
        let cache = ScanCache::new(Duration::from_secs(60), 1 << 20);
        let key = scan_cache_key(&parts("saas", &[]));
        cache.put(key.clone(), vec![]);
        assert_eq!(cache.get(&key), Some(vec![]));
    }

    #[test]
    fn byte_budget_evicts_least_recently_used() {
        let one = batch(10);
        let bytes = one.get_array_memory_size();
        let cache = ScanCache::new(Duration::from_secs(60), bytes * 2 + 8);

        let key_a = scan_cache_key(&parts("a", &[]));
        let key_b = scan_cache_key(&parts("b", &[]));
        let key_c = scan_cache_key(&parts("c", &[]));
        cache.put(key_a.clone(), vec![batch(10)]);
        cache.put(key_b.clone(), vec![batch(10)]);
        // Refresh A so B is the LRU victim.
        assert!(cache.get(&key_a).is_some());
        cache.put(key_c.clone(), vec![batch(10)]);

        assert!(
            cache.get(&key_a).is_some(),
            "recently refreshed entry stays"
        );
        assert!(cache.get(&key_b).is_none(), "LRU entry is evicted");
        assert!(cache.get(&key_c).is_some());
    }

    #[test]
    fn oversized_scan_is_not_cached() {
        let cache = ScanCache::new(Duration::from_secs(60), 8);
        let key = scan_cache_key(&parts("saas", &[]));
        cache.put(key.clone(), vec![batch(10)]);
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn key_ignores_resource_and_filter_ordering() {
        let projection = vec!["id".to_string()];
        let resource_a = json!({"a": 1, "b": 2});
        let filters_a = [("x".to_string(), json!(1)), ("y".to_string(), json!(2))];
        let a = ScanKeyParts {
            resource: &resource_a,
            filter_inputs: &filters_a,
            ..parts("saas", &projection)
        };
        let resource_b = json!({"b": 2, "a": 1});
        let filters_b = [("y".to_string(), json!(2)), ("x".to_string(), json!(1))];
        let b = ScanKeyParts {
            resource: &resource_b,
            filter_inputs: &filters_b,
            ..parts("saas", &projection)
        };
        assert_eq!(scan_cache_key(&a), scan_cache_key(&b));
    }

    #[test]
    fn key_distinguishes_alias_projection_and_gateway() {
        let projection = vec!["id".to_string()];
        let base = scan_cache_key(&parts("saas", &projection));
        let other_gateway = scan_cache_key(&parts("other", &projection));
        assert_ne!(base, other_gateway);

        let aliased = ScanKeyParts {
            connection_alias: Some("work"),
            ..parts("saas", &projection)
        };
        assert_ne!(base, scan_cache_key(&aliased));

        let limited = ScanKeyParts {
            limit: Some(1),
            ..parts("saas", &projection)
        };
        assert_ne!(base, scan_cache_key(&limited));

        let pack_v2 = ScanKeyParts {
            source_pack_version: 2,
            ..parts("saas", &projection)
        };
        assert_ne!(base, scan_cache_key(&pack_v2));

        let other_projection = scan_cache_key(&parts("saas", &["name".to_string()]));
        assert_ne!(base, other_projection);
    }

    #[test]
    fn schema_fingerprint_tracks_shape() {
        let a = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
        let b = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, true)]));
        assert_ne!(schema_fingerprint(&a), schema_fingerprint(&b));
        assert_eq!(schema_fingerprint(&a), schema_fingerprint(&a));
    }
}
