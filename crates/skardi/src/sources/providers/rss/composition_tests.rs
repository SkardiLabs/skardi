//! End-to-end composition: the provider is a protocol adapter, and history,
//! chunking and citability are user-space SQL over it.
//!
//! Task 18's [`super::integration_tests`] proves the adapter's own contract —
//! what a scan fetches, what it degrades to, what it refuses. This suite is one
//! layer up: it proves that the *downstream* story the design tells can be
//! written with the primitives that already ship, and nothing else. Every
//! archive here is an ordinary writable sqlite source, every ingest is an
//! ordinary `INSERT … SELECT`, and the only RSS-specific thing about any of it
//! is which catalog the rows are read from.
//!
//! ## Why this suite is in-crate rather than in `crates/skardi/tests/`
//!
//! Same reason as [`super::integration_tests`]: [`MockFeedServer`] lives in
//! the test-only `pub(crate)` `testutil`, unreachable from an external test
//! crate. [`super::register_rss_tables_with_policy`] itself is `pub` — its
//! external-crate proof is `crates/skardi/tests/rss_egress_injection.rs` —
//! though every test here registers with the same `AllowAll` production
//! ships.
//!
//! ## Why this module has its own mock harness
//!
//! [`super::integration_tests`]'s `TestNews` owns its server *and* its
//! `SessionContext` together, and its scripts answer request-by-request. Two
//! tests here need neither: [`subscription_add_is_config_only`] must point a
//! *second* `SessionContext` at the same still-running server, and every other
//! test only ever needs "this path serves this document until I say otherwise".
//! [`MockFeeds`] is that smaller thing, and it is deliberately not a
//! generalisation of `TestNews` — a shared harness would have to grow a
//! server/context split that Task 18's suite has no use for.
//!
//! ## Timing
//!
//! Real time throughout, for the reason recorded in [`super::integration_tests`]:
//! `#[tokio::test(start_paused = true)]` does not work against
//! [`MockFeedServer`], because tokio's auto-advance races ahead of the real
//! socket round-trip and reqwest's own timeout — also on the paused clock —
//! fires first. Every query goes through [`Composition::sql`], which carries an
//! explicit [`QUERY_CEILING`], so a regression that parks a scan forever fails
//! with a named assertion rather than hanging the test binary.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Array, Int64Array, RecordBatch, TimestampMillisecondArray, UInt64Array};
use datafusion::prelude::SessionContext;
use tokio_rusqlite::Connection;

use super::config::{FeedSubscription, RssConfig, inline_config};
use super::egress::AllowAll;
use super::register_rss_tables_with_policy;
use super::testutil::{MockFeedServer, MockResponse, str_col, str_opt_col, total_rows};
use crate::model::chunking::ChunkingRegistry;
use crate::sources::hierarchy::HierarchyLevel;
use crate::sources::providers::sqlite::register_sqlite_tables;

/// The rss catalog: `news.main.items` / `news.main.feeds` throughout.
const NEWS: &str = "news";
/// The writable sqlite catalog holding `news_items` / `news_chunks`.
const ARCHIVE: &str = "archive";
/// The read-only sqlite catalog the federated-join test joins against.
const META: &str = "meta";

/// Backstop on any single statement in this suite, for the reason recorded in
/// [`super::integration_tests`]: a regression that parks a scan forever must
/// fail with a named assertion rather than read as a CI timeout.
const QUERY_CEILING: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// The archive schema and the two ingest statements
// ---------------------------------------------------------------------------

/// The two-table archive, executed through `tokio_rusqlite` by the test rather
/// than by any production code path: this DDL belongs to the *user* of the
/// provider, which is the whole point of the suite.
///
/// `news_items` is one row per entry with `content` exactly as `items` served
/// it. `news_chunks` is derived and disposable — [`parameter_change_rebuild_from_retained_content`]
/// deletes and rebuilds it, which is only possible because `news_items` kept
/// the text.
const ARCHIVE_DDL: &str = "
    CREATE TABLE IF NOT EXISTS news_items (
      feed TEXT NOT NULL, guid TEXT NOT NULL, title TEXT, link TEXT, author TEXT,
      published TIMESTAMP, content TEXT, PRIMARY KEY (feed, guid));
    CREATE TABLE IF NOT EXISTS news_chunks (
      feed TEXT NOT NULL, guid TEXT NOT NULL, chunk_idx INTEGER NOT NULL,
      chunk_text TEXT NOT NULL, embedding BLOB, ingested_at TIMESTAMP,
      PRIMARY KEY (feed, guid, chunk_idx));
";

/// Statement A: append entries the archive has not seen, by anti-join on the
/// provider's own identity columns `(feed, guid)`.
///
/// This is the statement that makes `skardi sync` re-runnable: it is the
/// *absence* of a matching archive row, not a timestamp or a high-water mark,
/// that decides what is new — so a window that rolled backwards, a feed that
/// re-served an old entry, and a second run five seconds later all add nothing.
const INSERT_ITEMS: &str = "\
INSERT INTO archive.main.news_items (feed, guid, title, link, author, published, content)
SELECT i.feed, i.guid, i.title, i.link, i.author, i.published, COALESCE(i.content, i.summary)
FROM news.main.items i
LEFT JOIN archive.main.news_items a ON a.feed = i.feed AND a.guid = i.guid
WHERE a.guid IS NULL";

/// Statement B: chunk the archived text of entries that have no chunks yet.
///
/// Reads `archive.main.news_items`, never `news.main.items`: chunking is a
/// pure function of retained content, so it costs no network and survives the
/// live window rolling out from under it.
///
/// DataFusion has no `WITH ORDINALITY`, and the shipped idiom (`docs/chunk.md`,
/// "Inline ingestion") is a plain `UNNEST(chunk(...))` subquery, so the index
/// comes from a window function. `ROW_NUMBER` with no `ORDER BY` inside the
/// window leaves chunk→index assignment unspecified; the tests below assert
/// that the indices are dense `0..n-1` per `(feed, guid)` and that the stored
/// texts are exactly the ones `chunk()` produces — never which text got which
/// index.
///
/// `embedding` is the argument rather than a fixed `NULL` so the `#[ignore]`d
/// live variant can substitute `vec_to_binary(candle(…))` without restating
/// the statement.
fn insert_chunks(size: u32, overlap: u32, embedding: &str) -> String {
    format!(
        "\
INSERT INTO archive.main.news_chunks (feed, guid, chunk_idx, chunk_text, embedding, ingested_at)
SELECT s.feed, s.guid,
       ROW_NUMBER() OVER (PARTITION BY s.feed, s.guid) - 1 AS chunk_idx,
       s.chunk_text, {embedding} AS embedding, now() AS ingested_at
FROM (
  SELECT n.feed, n.guid, UNNEST(chunk('markdown', n.content, {size}, {overlap})) AS chunk_text
  FROM archive.main.news_items n
  LEFT JOIN archive.main.news_chunks e ON e.feed = n.feed AND e.guid = n.guid
  WHERE e.guid IS NULL AND n.content IS NOT NULL
) s"
    )
}

/// The chunk parameters the first ingest uses.
const FIRST_SIZE: u32 = 1200;
const FIRST_OVERLAP: u32 = 120;
/// The parameters [`parameter_change_rebuild_from_retained_content`] rebuilds with.
const REBUILD_SIZE: u32 = 600;
const REBUILD_OVERLAP: u32 = 60;

/// The closing `SELECT` of a `sync` run: the degraded subscriptions, with the
/// reason and the as-of time.
const HEALTH_REPORT: &str = "\
SELECT name, last_status, last_error, last_fetch FROM news.main.feeds
WHERE last_status IN ('error', 'never', 'stale-error')
ORDER BY name";

// ---------------------------------------------------------------------------
// Feed documents
// ---------------------------------------------------------------------------

/// One paragraph of the archive fixtures' bodies, on the wire.
///
/// Long enough that repeating it a few times pushes a body past
/// [`FIRST_SIZE`], which is what makes "more than one chunk" and "more chunks
/// at a smaller size" observable at all.
const PARA_HTML: &str = "<p>Filler paragraph with <strong>bold</strong> emphasis and a \
<a href=\"https://feed.example/more\">link</a> in it, written long enough that repeating it a \
handful of times pushes one entry's body well past the chunk target the first ingest uses.</p>";

/// The same paragraph as stored — the Markdown [`PARA_HTML`] converts to.
///
/// Hand-written rather than derived, so the pair pins the HTML→Markdown step:
/// the two constants are independent, and the archived text is compared
/// against a body assembled from *this* one.
const PARA_MD: &str = "Filler paragraph with **bold** emphasis and a \
[link](https://feed.example/more) in it, written long enough that repeating it a handful of \
times pushes one entry's body well past the chunk target the first ingest uses.";

/// Paragraphs per archive-fixture body.
const PARAS: usize = 6;

/// `(guid, title, pubDate)` for the three entries the archive tests ingest, in
/// the order the feed serves them. Distinct `pubDate`s so a citability
/// assertion can name a specific entry's `published`.
const ENTRIES: [(&str, &str, &str); 3] = [
    (
        "news-1",
        "Alpha announcement",
        "Mon, 20 Jul 2026 10:00:00 GMT",
    ),
    (
        "news-2",
        "Beta announcement",
        "Tue, 21 Jul 2026 11:00:00 GMT",
    ),
    (
        "news-3",
        "Gamma announcement",
        "Wed, 22 Jul 2026 12:00:00 GMT",
    ),
];

/// One archive entry's `<content:encoded>` body, on the wire.
fn entry_html(title: &str) -> String {
    let mut html = format!("<h2>{title}</h2>");
    for _ in 0..PARAS {
        html.push_str(PARA_HTML);
    }
    html
}

/// The same body as `items.content` serves it, and therefore as the archive
/// must store it.
fn entry_markdown(title: &str) -> String {
    let mut markdown = format!("## {title}");
    for _ in 0..PARAS {
        markdown.push_str("\n\n");
        markdown.push_str(PARA_MD);
    }
    markdown
}

/// An RSS 2.0 document serving the *last* `count` of [`ENTRIES`].
///
/// From the end, because [`ENTRIES`] is oldest-first and shrinking `count` is
/// how these tests roll the live window: a feed that keeps its N most recent
/// entries and drops what falls off the bottom is the ordinary case the archive
/// exists for, and the entry it drops is the oldest one.
fn archive_feed(count: usize) -> String {
    let mut doc = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/"
     xmlns:dc="http://purl.org/dc/elements/1.1/">
<channel><title>World News</title><link>https://feed.example/</link>
<description>The archive fixtures' channel.</description>"#,
    );
    for (guid, title, pub_date) in &ENTRIES[ENTRIES.len() - count..] {
        doc.push_str(&format!(
            "<item><guid isPermaLink=\"false\">{guid}</guid><title>{title}</title>\
             <link>https://feed.example/{guid}</link>\
             <dc:creator>Ada Lovelace</dc:creator><pubDate>{pub_date}</pubDate>\
             <content:encoded><![CDATA[{}]]></content:encoded></item>",
            entry_html(title)
        ));
    }
    doc.push_str("</channel></rss>");
    doc
}

/// A well-formed RSS 2.0 document with one short `<item>` per guid — the shape
/// the tests that care about *which feed a row came from* use, rather than
/// about its body.
fn simple_feed(guids: &[&str]) -> String {
    let mut doc = String::from(
        r#"<rss version="2.0"><channel><title>Mock Feed</title>
<link>https://feed.example/</link><description>A mock feed.</description>"#,
    );
    for guid in guids {
        doc.push_str(&format!(
            "<item><guid>{guid}</guid><title>{guid} title</title>\
             <link>https://feed.example/{guid}</link>\
             <description>Short summary for {guid}.</description></item>"
        ));
    }
    doc.push_str("</channel></rss>");
    doc
}

// ---------------------------------------------------------------------------
// The mock server
// ---------------------------------------------------------------------------

/// What one path currently answers. Replaced wholesale by
/// [`MockFeeds::serve`]; there is no per-request scripting here because no test
/// in this module needs a path to answer differently to two requests in a row.
#[derive(Clone)]
struct Canned {
    status: u16,
    body: Vec<u8>,
}

impl Canned {
    fn xml(body: &str) -> Self {
        Self {
            status: 200,
            body: body.as_bytes().to_vec(),
        }
    }

    fn status(status: u16) -> Self {
        Self {
            status,
            body: Vec::new(),
        }
    }
}

/// A running mock feed host whose paths can be re-pointed mid-test, and which
/// outlives any one [`SessionContext`] registered against it.
struct MockFeeds {
    server: MockFeedServer,
    paths: Arc<Mutex<HashMap<String, Canned>>>,
}

impl MockFeeds {
    async fn start() -> Self {
        let paths: Arc<Mutex<HashMap<String, Canned>>> = Arc::new(Mutex::new(HashMap::new()));
        let handler_paths = Arc::clone(&paths);
        let server = MockFeedServer::start(move |request| {
            let paths = handler_paths
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match paths.get(&request.path) {
                Some(canned) => MockResponse::new(canned.status, canned.body.clone())
                    .with_header("content-type", "application/xml"),
                // An unscripted path is a mistake in the test. `404` is outside
                // `fetch.rs`'s `RETRYABLE_STATUSES`, so it surfaces after one
                // request rather than after a whole attempt budget.
                None => MockResponse::status(404),
            }
        })
        .await;
        Self { server, paths }
    }

    /// Point `path` at `canned` from the next request on.
    fn serve(&self, path: &str, canned: Canned) {
        self.paths
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(path.to_string(), canned);
    }

    fn url(&self) -> String {
        self.server.url()
    }

    fn request_count(&self) -> usize {
        self.server.requests().len()
    }

    /// The paths of every request observed, sorted — concurrent partitions make
    /// arrival order undefined, and no assertion here depends on it.
    fn sorted_paths(&self) -> Vec<String> {
        let mut paths: Vec<String> = self
            .server
            .requests()
            .into_iter()
            .map(|request| request.path)
            .collect();
        paths.sort();
        paths
    }
}

// ---------------------------------------------------------------------------
// The archive database
// ---------------------------------------------------------------------------

/// A sqlite file holding the archive schema, plus a direct connection for the
/// steps a test performs *outside* SQL — creating the tables, and the one
/// `DELETE` that stands in for an operator dropping a derived table.
struct ArchiveDb {
    // Held for its `Drop`: the file and its WAL sidecars go away with it.
    _dir: tempfile::TempDir,
    path: String,
    conn: Connection,
}

impl ArchiveDb {
    async fn create() -> Self {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir
            .path()
            .join("archive.db")
            .to_str()
            .expect("temp path is utf-8")
            .to_string();
        let conn = Connection::open(&path).await.expect("open archive db");
        conn.call(
            |conn| -> std::result::Result<(), tokio_rusqlite::rusqlite::Error> {
                conn.execute_batch(ARCHIVE_DDL)
            },
        )
        .await
        .expect("create the archive schema");
        Self {
            _dir: dir,
            path,
            conn,
        }
    }

    /// Run a statement outside DataFusion, returning the rows it changed.
    async fn execute(&self, sql: &'static str) -> usize {
        self.conn
            .call(move |conn| conn.execute(sql, []))
            .await
            .unwrap_or_else(|e| panic!("archive statement {sql:?}: {e}"))
    }

    /// `SELECT count(*)` read straight from sqlite, bypassing DataFusion — so a
    /// count here cannot be produced by the same layer whose writes it checks.
    async fn count(&self, table: &'static str) -> i64 {
        self.conn
            .call(move |conn| {
                conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
            })
            .await
            .unwrap_or_else(|e| panic!("counting {table}: {e}"))
    }
}

// ---------------------------------------------------------------------------
// The composed context
// ---------------------------------------------------------------------------

/// A `SessionContext` with an rss catalog, a writable archive catalog, and the
/// `chunk` UDF — the three things the design says a user needs, and nothing
/// else.
struct Composition {
    ctx: SessionContext,
}

impl Composition {
    /// Register `feeds`, given as `(subscription name, path on the mock)`.
    ///
    /// `tune` runs on the spec-default config after this harness's own two
    /// adjustments, so a test can override either. Those two: the request
    /// timeout drops from 10s to 5s and the scan deadline from 60s to 20s, so a
    /// test that starts hanging says so in seconds rather than sitting on the
    /// production deadline.
    async fn register(
        mock: &MockFeeds,
        feeds: &[(&str, &str)],
        archive: &ArchiveDb,
        tune: impl FnOnce(&mut RssConfig),
    ) -> Self {
        let subscriptions = feeds
            .iter()
            .map(|(name, path)| FeedSubscription {
                url: format!("{}{path}", mock.url()),
                name: Some((*name).to_string()),
            })
            .collect();
        let mut config = inline_config(subscriptions);
        config.request_timeout_seconds = 5;
        config.scan_timeout_seconds = 20;
        tune(&mut config);

        let mut ctx = SessionContext::new();
        register_rss_tables_with_policy(
            &mut ctx,
            NEWS,
            Some(&config),
            false,
            HierarchyLevel::Catalog,
            // The production default: no destination filtering.
            Arc::new(AllowAll),
        )
        .await
        .expect("registering the rss source succeeds");

        register_sqlite_tables(
            &mut ctx,
            ARCHIVE,
            &archive.path,
            None,
            true,
            None,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("registering the writable archive succeeds");

        Arc::new(ChunkingRegistry::new()).register_chunk_udf(&mut ctx);

        Self { ctx }
    }

    /// Attach a read-only sqlite catalog under [`META`].
    async fn with_meta(mut self, db_path: &str) -> Self {
        register_sqlite_tables(
            &mut self.ctx,
            META,
            db_path,
            None,
            false,
            None,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("registering the read-only meta source succeeds");
        self
    }

    /// Run one statement to completion, under [`QUERY_CEILING`].
    async fn sql(&self, sql: &str) -> Vec<RecordBatch> {
        tokio::time::timeout(QUERY_CEILING, async {
            self.ctx
                .sql(sql)
                .await
                .unwrap_or_else(|e| panic!("plan {sql:?}: {e}"))
                .collect()
                .await
                .unwrap_or_else(|e| panic!("execute {sql:?}: {e}"))
        })
        .await
        .unwrap_or_else(|_| panic!("{sql:?} did not finish within {QUERY_CEILING:?}"))
    }

    /// Run an `INSERT` and return the row count DataFusion reports for it.
    ///
    /// This is the direct measurement idempotence needs: a second run reporting
    /// `0` says the statement *wrote* nothing, which a table-level count alone
    /// could not distinguish from a write that happened to land on the same
    /// rows.
    async fn insert(&self, sql: &str) -> u64 {
        let batches = self.sql(sql).await;
        assert_eq!(
            total_rows(&batches),
            1,
            "an INSERT reports exactly one count row"
        );
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("an INSERT's count column is UInt64")
            .value(0)
    }
}

// ---------------------------------------------------------------------------
// Result helpers
// ---------------------------------------------------------------------------

/// One non-nullable `Utf8` column, flattened across a result's batches.
fn col(batches: &[RecordBatch], name: &str) -> Vec<String> {
    batches.iter().flat_map(|b| str_col(b, name)).collect()
}

/// One nullable `Utf8` column, NULLs preserved.
fn opt_col(batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
    batches.iter().flat_map(|b| str_opt_col(b, name)).collect()
}

/// One non-nullable `Int64` column (`news_chunks.chunk_idx`).
fn i64_col(batches: &[RecordBatch], name: &str) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            let index = batch
                .schema()
                .index_of(name)
                .unwrap_or_else(|e| panic!("batch has no column {name:?}: {e}"));
            let column = batch
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap_or_else(|| panic!("column {name:?} is not Int64"));
            assert_eq!(column.null_count(), 0, "column {name:?} has NULLs");
            column.values().to_vec()
        })
        .collect()
}

/// Which rows of a `Timestamp(Millisecond, UTC)` column are non-NULL.
///
/// `feeds.last_fetch` is a timestamp, not a string, so [`opt_col`] cannot read
/// it. The health report owes an as-of time whenever an attempt has been made
/// and none before that, which is a question about presence rather than about
/// the instant — and the instant is a wall-clock reading no test can pin.
fn ts_present(batches: &[RecordBatch], name: &str) -> Vec<bool> {
    batches
        .iter()
        .flat_map(|batch| {
            let index = batch
                .schema()
                .index_of(name)
                .unwrap_or_else(|e| panic!("batch has no column {name:?}: {e}"));
            let column = batch
                .column(index)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap_or_else(|| panic!("column {name:?} is not Timestamp(Millisecond)"));
            (0..column.len())
                .map(|row| column.is_valid(row))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Assert `actual` contains `needle`, naming both on failure. A bare `contains`
/// assertion would report only "false".
#[track_caller]
fn assert_contains(actual: &str, needle: &str) {
    assert!(actual.contains(needle), "expected {needle:?} in {actual:?}");
}

/// Group `(feed, guid, chunk_idx, chunk_text)` rows by entry, asserting on the
/// way that every group's indices are dense `0..n-1`.
///
/// Density, not assignment: `ROW_NUMBER` with no `ORDER BY` inside the window
/// leaves which chunk gets which index unspecified, so the properties that are
/// actually owed are that the indices form a gapless range per entry — which is
/// what makes `(feed, guid, chunk_idx)` a usable primary key and a stable
/// citation target — and that no chunk was lost. The caller checks the second
/// against a fresh `chunk()` expansion.
#[track_caller]
fn chunks_by_entry(batches: &[RecordBatch]) -> BTreeMap<(String, String), Vec<String>> {
    let feeds = col(batches, "feed");
    let guids = col(batches, "guid");
    let indices = i64_col(batches, "chunk_idx");
    let texts = col(batches, "chunk_text");
    assert_eq!(feeds.len(), guids.len());
    assert_eq!(feeds.len(), indices.len());
    assert_eq!(feeds.len(), texts.len());

    let mut grouped: BTreeMap<(String, String), Vec<(i64, String)>> = BTreeMap::new();
    for row in 0..feeds.len() {
        grouped
            .entry((feeds[row].clone(), guids[row].clone()))
            .or_default()
            .push((indices[row], texts[row].clone()));
    }

    grouped
        .into_iter()
        .map(|(entry, mut rows)| {
            rows.sort_by_key(|(index, _)| *index);
            let seen: Vec<i64> = rows.iter().map(|(index, _)| *index).collect();
            let dense: Vec<i64> = (0..rows.len() as i64).collect();
            assert_eq!(
                seen, dense,
                "chunk_idx for {entry:?} is not a dense 0..n-1 range"
            );
            let texts = rows.into_iter().map(|(_, text)| text).collect();
            (entry, texts)
        })
        .collect()
}

/// A sorted copy, for comparing two collections as multisets.
fn sorted(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values
}

// ---------------------------------------------------------------------------
// A federated join between the provider and another source
// ---------------------------------------------------------------------------

/// `items` joins against an unrelated sqlite table on `feed`, in one query, in
/// one context.
///
/// The join is the point: the provider contributes rows keyed by its own
/// subscription names, and a second source supplies per-subscription metadata
/// the provider knows nothing about. `tier` cannot come from anywhere but the
/// sqlite side, and `guid` cannot come from anywhere but the feed, so a result
/// row proves both halves were read and matched.
#[tokio::test]
async fn federated_join_items_with_sqlite() {
    let mock = MockFeeds::start().await;
    for feed in ["a", "b", "c"] {
        mock.serve(
            &format!("/{feed}.xml"),
            Canned::xml(&simple_feed(&[&format!("{feed}-1")])),
        );
    }

    let meta_dir = tempfile::tempdir().expect("create temp dir");
    let meta_path = meta_dir
        .path()
        .join("meta.db")
        .to_str()
        .expect("temp path is utf-8")
        .to_string();
    let meta_conn = Connection::open(&meta_path).await.expect("open meta db");
    meta_conn
        .call(
            |conn| -> std::result::Result<(), tokio_rusqlite::rusqlite::Error> {
                conn.execute_batch(
                    "CREATE TABLE feed_meta (feed TEXT, tier TEXT);
                 INSERT INTO feed_meta (feed, tier) VALUES ('a', 'primary');
                 INSERT INTO feed_meta (feed, tier) VALUES ('b', 'secondary');",
                )
            },
        )
        .await
        .expect("seed feed_meta");

    let archive = ArchiveDb::create().await;
    let news = Composition::register(
        &mock,
        &[("a", "/a.xml"), ("b", "/b.xml"), ("c", "/c.xml")],
        &archive,
        |_| {},
    )
    .await
    .with_meta(&meta_path)
    .await;

    let joined = news
        .sql(
            "SELECT i.guid, m.tier \
             FROM news.main.items i JOIN meta.main.feed_meta m ON m.feed = i.feed \
             ORDER BY i.guid",
        )
        .await;
    assert_eq!(
        col(&joined, "guid"),
        vec!["a-1", "b-1"],
        "the subscription with no metadata row is dropped by the inner join"
    );
    assert_eq!(col(&joined, "tier"), vec!["primary", "secondary"]);
    assert_eq!(
        mock.sorted_paths(),
        vec!["/a.xml", "/b.xml", "/c.xml"],
        "the join predicate is not on `feed`'s equality to a literal, so nothing prunes"
    );
}

// ---------------------------------------------------------------------------
// The archive: two INSERTs, idempotent, and citable after the window rolls
// ---------------------------------------------------------------------------

/// The heart of the downstream contract.
///
/// Three properties, in one test because they are the same run: ingest is two
/// `INSERT`s; re-running both writes nothing; and after the live window drops
/// an entry, that entry is still fully queryable from the archive.
///
/// `ttl_seconds = 0` throughout, so every `items` scan is a live fetch. That is
/// deliberate and load-bearing for the idempotence half: with a TTL the second
/// run would be answered from the first run's cache, and "the second `INSERT`
/// wrote nothing" would be a statement about the cache rather than about the
/// anti-join. Here the feed really is re-fetched and re-parsed between runs —
/// asserted, by the request count — and the anti-join is the only thing
/// stopping the rows from being written twice.
#[tokio::test]
async fn archive_ingest_is_idempotent_and_survives_window_roll() {
    let mock = MockFeeds::start().await;
    mock.serve("/world.xml", Canned::xml(&archive_feed(3)));
    let archive = ArchiveDb::create().await;
    let news = Composition::register(&mock, &[("world", "/world.xml")], &archive, |config| {
        config.ttl_seconds = 0
    })
    .await;

    // --- first sync
    assert_eq!(news.insert(INSERT_ITEMS).await, 3, "three entries are new");
    let first_chunks = news
        .insert(&insert_chunks(FIRST_SIZE, FIRST_OVERLAP, "NULL"))
        .await;
    assert!(
        first_chunks >= 3,
        "each entry's body is longer than {FIRST_SIZE} characters, so three entries owe at \
         least three chunks; got {first_chunks}"
    );
    assert_eq!(archive.count("news_items").await, 3);
    assert_eq!(archive.count("news_chunks").await, first_chunks as i64);

    // The content is Markdown, and it is the *feed's* Markdown: compared both
    // against a body assembled from `PARA_MD` (which pins the HTML→Markdown
    // conversion, since `PARA_HTML` is a separate constant) and against what
    // `items` served in this very run (which pins that the archive copied it
    // rather than transformed it).
    let archived = news
        .sql("SELECT guid, content FROM archive.main.news_items ORDER BY guid")
        .await;
    assert_eq!(col(&archived, "guid"), vec!["news-1", "news-2", "news-3"]);
    assert_eq!(
        opt_col(&archived, "content")[0].as_deref(),
        Some(entry_markdown(ENTRIES[0].1).as_str()),
        "the archived body is the Markdown the conversion owes, not the HTML on the wire"
    );
    let served = news
        .sql("SELECT guid, content FROM news.main.items ORDER BY guid")
        .await;
    assert_eq!(
        opt_col(&archived, "content"),
        opt_col(&served, "content"),
        "the archive stores content exactly as `items` served it"
    );

    // Every chunk `chunk()` produces is stored, with dense indices per entry.
    let stored = news
        .sql("SELECT feed, guid, chunk_idx, chunk_text FROM archive.main.news_chunks")
        .await;
    let grouped = chunks_by_entry(&stored);
    assert_eq!(grouped.len(), 3, "every entry contributed chunks");
    for ((feed, guid), texts) in &grouped {
        assert_eq!(feed, "world");
        // Without this, the dense-`0..n-1` property above would be satisfied by
        // a single `[0]` per entry and would assert nothing about indexing.
        // Measured: each body is 1,335 Markdown characters and the
        // `FIRST_SIZE`/`FIRST_OVERLAP` split gives it two chunks.
        assert!(
            texts.len() > 1,
            "{guid} produced {} chunk(s); the body is no longer long enough for the \
             chunk-index assertions to mean anything",
            texts.len()
        );
        let expected = news
            .sql(&format!(
                "SELECT UNNEST(chunk('markdown', content, {FIRST_SIZE}, {FIRST_OVERLAP})) \
                 AS chunk_text FROM archive.main.news_items WHERE guid = '{guid}'"
            ))
            .await;
        assert_eq!(
            sorted(texts.clone()),
            sorted(col(&expected, "chunk_text")),
            "the stored chunks for {guid} are not the ones chunk() produces"
        );
    }

    // --- second sync, same feed, live re-fetch: nothing is written twice
    let before_rerun = mock.request_count();
    assert_eq!(
        news.insert(INSERT_ITEMS).await,
        0,
        "re-running the entry INSERT wrote rows a second time"
    );
    assert_eq!(
        news.insert(&insert_chunks(FIRST_SIZE, FIRST_OVERLAP, "NULL"))
            .await,
        0,
        "re-running the chunk INSERT wrote rows a second time"
    );
    assert!(
        mock.request_count() > before_rerun,
        "the second run was served from cache, so it did not exercise the anti-join"
    );
    assert_eq!(archive.count("news_items").await, 3);
    assert_eq!(archive.count("news_chunks").await, first_chunks as i64);

    // --- the live window rolls: the feed drops its oldest entry
    mock.serve("/world.xml", Canned::xml(&archive_feed(2)));
    let live = news
        .sql("SELECT guid FROM news.main.items ORDER BY guid")
        .await;
    assert_eq!(
        col(&live, "guid"),
        vec!["news-2", "news-3"],
        "the live window did not actually shrink, so nothing below is a citability test"
    );

    assert_eq!(news.insert(INSERT_ITEMS).await, 0, "no entry is new");
    assert_eq!(
        news.insert(&insert_chunks(FIRST_SIZE, FIRST_OVERLAP, "NULL"))
            .await,
        0
    );
    assert_eq!(
        archive.count("news_items").await,
        3,
        "an entry leaving the live window must not remove it from the archive"
    );
    assert_eq!(archive.count("news_chunks").await, first_chunks as i64);

    // The dropped entry is still citable: title, link and published, from the
    // archive alone, with no request able to supply them. This is the whole
    // reason `news_items` stores content and metadata verbatim rather than only
    // chunks — a citation has to survive the source dropping the page.
    let dropped = news
        .sql(
            "SELECT guid, title, link, published FROM archive.main.news_items \
             WHERE guid = 'news-1'",
        )
        .await;
    assert_eq!(col(&dropped, "guid"), vec!["news-1"]);
    assert_eq!(
        opt_col(&dropped, "title"),
        vec![Some(ENTRIES[0].1.to_string())]
    );
    assert_eq!(
        opt_col(&dropped, "link"),
        vec![Some("https://feed.example/news-1".to_string())]
    );
    // Spelled out rather than reformatted from `ENTRIES[0].2`: the RFC-822
    // `pubDate` on the wire becomes a `Timestamp(Millisecond, UTC)` in `items`
    // and then a sqlite `TIMESTAMP` — which `sqlite_type_to_arrow` maps to
    // `Utf8` (`sources/providers/sqlite/mod.rs:1497-1523`, where only
    // INT/REAL/BLOB/BOOL are special-cased), so DataFusion casts the timestamp
    // to a string on the way in. This is the rendering that cast produces, and
    // it is what a citation would show.
    assert_eq!(
        opt_col(&dropped, "published"),
        vec![Some("2026-07-20T10:00:00Z".to_string())],
        "the dropped entry's published time survived the window roll"
    );
}

// ---------------------------------------------------------------------------
// The `sync` health report
// ---------------------------------------------------------------------------

/// A `sync` run's closing `SELECT`: the degraded subscriptions and why, empty
/// when everything is healthy, and never a reason to fail the run.
///
/// Three states in one context, because the report's shape only means anything
/// as a contrast: never-attempted, all-healthy, and one-degraded. The last
/// clause is what an in-repo test can say about "never changes the run's exit
/// status" — the report is a plain `SELECT` over `feeds` that returns `Ok` in
/// all three states, including the one where a subscription is failing. (That a
/// `skardi sync` *process* then exits zero is the CLI's packaging, which is M3.)
#[tokio::test]
async fn sync_closing_health_report_shape() {
    let mock = MockFeeds::start().await;
    for feed in ["alive", "dying"] {
        mock.serve(
            &format!("/{feed}.xml"),
            Canned::xml(&simple_feed(&[&format!("{feed}-1")])),
        );
    }
    let archive = ArchiveDb::create().await;
    let news = Composition::register(
        &mock,
        &[("alive", "/alive.xml"), ("dying", "/dying.xml")],
        &archive,
        |config| config.ttl_seconds = 0,
    )
    .await;

    // --- before any scan: both subscriptions are degraded, as `never`
    let untouched = news.sql(HEALTH_REPORT).await;
    assert_eq!(col(&untouched, "name"), vec!["alive", "dying"]);
    assert_eq!(col(&untouched, "last_status"), vec!["never", "never"]);
    assert_eq!(
        opt_col(&untouched, "last_error"),
        vec![None, None],
        "a never-attempted subscription has no error to report, only a status"
    );
    assert_eq!(
        ts_present(&untouched, "last_fetch"),
        vec![false, false],
        "and no as-of time"
    );
    assert_eq!(
        mock.request_count(),
        0,
        "the health report reached the fetcher"
    );

    // --- everything healthy: the report is empty
    news.sql("SELECT guid FROM news.main.items").await;
    let after_scan = mock.request_count();
    let healthy = news.sql(HEALTH_REPORT).await;
    assert_eq!(
        total_rows(&healthy),
        0,
        "a healthy run's report lists nothing: {:?}",
        col(&healthy, "name")
    );
    assert_eq!(
        mock.request_count(),
        after_scan,
        "the health report reached the fetcher"
    );

    // --- one feed dies: the report names it, and it alone
    mock.serve("/dying.xml", Canned::status(500));
    news.sql("SELECT guid FROM news.main.items").await;
    let degraded = news.sql(HEALTH_REPORT).await;
    assert_eq!(
        col(&degraded, "name"),
        vec!["dying"],
        "the healthy neighbour must not appear in the report"
    );
    assert_eq!(
        col(&degraded, "last_status"),
        vec!["stale-error"],
        "the feed has a cached window, so it degrades rather than going dark"
    );
    assert_contains(
        opt_col(&degraded, "last_error")[0]
            .as_deref()
            .expect("a degraded subscription reports why"),
        "http status 500",
    );
    assert_eq!(
        ts_present(&degraded, "last_fetch"),
        vec![true],
        "the report carries the as-of time of the failed attempt"
    );

    // The degraded subscription is still serving rows — the report is an
    // observation about the run, not a gate on it.
    let items = news
        .sql("SELECT feed, window_status FROM news.main.items ORDER BY feed")
        .await;
    assert_eq!(col(&items, "feed"), vec!["alive", "dying"]);
    assert_eq!(col(&items, "window_status"), vec!["fresh", "stale-error"]);
}

// ---------------------------------------------------------------------------
// Adding a subscription is a config edit
// ---------------------------------------------------------------------------

/// Adding a feed changes configuration and nothing else: the archive is
/// untouched, no existing subscription is re-fetched on registration, and the
/// new feed's rows arrive on its first `items` scan.
///
/// Two `SessionContext`s over one still-running mock and one archive file,
/// which is what "config edit plus restart" looks like from the engine's side.
/// The second context's `INSERT` adding exactly one row is the assertion that
/// carries the criterion: it can only be one if the two feeds already archived
/// were matched by the anti-join *and* the third was fetched for the first
/// time.
#[tokio::test]
async fn subscription_add_is_config_only() {
    let mock = MockFeeds::start().await;
    for feed in ["a", "b", "c"] {
        mock.serve(
            &format!("/{feed}.xml"),
            Canned::xml(&simple_feed(&[&format!("{feed}-1")])),
        );
    }
    let archive = ArchiveDb::create().await;

    // --- config v1: two subscriptions
    let v1 = Composition::register(
        &mock,
        &[("a", "/a.xml"), ("b", "/b.xml")],
        &archive,
        |config| config.ttl_seconds = 0,
    )
    .await;
    assert_eq!(v1.insert(INSERT_ITEMS).await, 2);
    assert_eq!(archive.count("news_items").await, 2);
    assert_eq!(mock.sorted_paths(), vec!["/a.xml", "/b.xml"]);
    drop(v1);

    // --- config v2: the same two, plus one
    let after_v1 = mock.request_count();
    let v2 = Composition::register(
        &mock,
        &[("a", "/a.xml"), ("b", "/b.xml"), ("c", "/c.xml")],
        &archive,
        |config| config.ttl_seconds = 0,
    )
    .await;
    assert_eq!(
        mock.request_count(),
        after_v1,
        "registering the new subscription performed network I/O"
    );

    assert_eq!(
        v2.insert(INSERT_ITEMS).await,
        1,
        "only the added subscription's entry is new to the archive"
    );
    assert_eq!(archive.count("news_items").await, 3);
    let archived = news_guids(&v2).await;
    assert_eq!(archived, vec!["a-1", "b-1", "c-1"]);
    assert!(
        mock.sorted_paths().contains(&"/c.xml".to_string()),
        "the new subscription's first items scan is what fetched it: {:?}",
        mock.sorted_paths()
    );

    // And the new subscription reports its own health, like any other.
    let feeds = v2
        .sql("SELECT name, last_status, last_error FROM news.main.feeds ORDER BY name")
        .await;
    assert_eq!(col(&feeds, "name"), vec!["a", "b", "c"]);
    assert_eq!(
        col(&feeds, "last_status"),
        vec!["fresh", "fresh", "fresh"],
        "the added feed was fetched successfully alongside the existing two"
    );
    assert_eq!(opt_col(&feeds, "last_error"), vec![None, None, None]);
}

/// The guids in the archive, in order.
async fn news_guids(news: &Composition) -> Vec<String> {
    let batches = news
        .sql("SELECT guid FROM archive.main.news_items ORDER BY guid")
        .await;
    col(&batches, "guid")
}

// ---------------------------------------------------------------------------
// Re-chunking from retained content
// ---------------------------------------------------------------------------

/// Changing the chunk parameters rebuilds `news_chunks` from
/// `news_items.content` alone — no request, and no dependence on the live
/// window still holding the entries.
///
/// This is the property that makes storing content verbatim worth its bytes: a
/// pipeline whose archive held only chunks would have to re-fetch (and could
/// not, for anything the window has dropped) to change a chunk size.
#[tokio::test]
async fn parameter_change_rebuild_from_retained_content() {
    let mock = MockFeeds::start().await;
    mock.serve("/world.xml", Canned::xml(&archive_feed(3)));
    let archive = ArchiveDb::create().await;
    let news = Composition::register(&mock, &[("world", "/world.xml")], &archive, |config| {
        // `ttl_seconds = 0` is what gives the request-count assertion below its
        // teeth: under a long TTL a rebuild that *did* read `news.main.items`
        // would be answered from the ingest's cache and cost no request, so the
        // count would be unchanged either way and would prove nothing. At zero,
        // any scan of `items` issues a request and the assertion fails.
        config.ttl_seconds = 0
    })
    .await;

    assert_eq!(news.insert(INSERT_ITEMS).await, 3);
    let coarse = news
        .insert(&insert_chunks(FIRST_SIZE, FIRST_OVERLAP, "NULL"))
        .await;
    let after_ingest = mock.request_count();
    assert_eq!(after_ingest, 1, "the ingest fetched the feed exactly once");

    // The live window is now irrelevant: drop it entirely. Anything the rebuild
    // reads has to come from `news_items`.
    mock.serve("/world.xml", Canned::status(500));

    let deleted = archive.execute("DELETE FROM news_chunks").await;
    assert_eq!(deleted, coarse as usize, "the derived table was emptied");
    assert_eq!(archive.count("news_chunks").await, 0);

    let fine = news
        .insert(&insert_chunks(REBUILD_SIZE, REBUILD_OVERLAP, "NULL"))
        .await;
    assert!(
        fine > coarse,
        "a {REBUILD_SIZE}-character target must split the same bodies into more chunks than \
         a {FIRST_SIZE}-character one did ({fine} vs {coarse})"
    );
    assert_eq!(archive.count("news_chunks").await, fine as i64);
    assert_eq!(
        mock.request_count(),
        after_ingest,
        "the rebuild touched the live window"
    );

    // The rebuilt rows are a complete, densely-indexed re-chunking of the
    // retained content — not a partial one that happened to be larger.
    let stored = news
        .sql("SELECT feed, guid, chunk_idx, chunk_text FROM archive.main.news_chunks")
        .await;
    let grouped = chunks_by_entry(&stored);
    assert_eq!(grouped.len(), 3);
    for ((_, guid), texts) in &grouped {
        let expected = news
            .sql(&format!(
                "SELECT UNNEST(chunk('markdown', content, {REBUILD_SIZE}, {REBUILD_OVERLAP})) \
                 AS chunk_text FROM archive.main.news_items WHERE guid = '{guid}'"
            ))
            .await;
        assert_eq!(
            sorted(texts.clone()),
            sorted(col(&expected, "chunk_text")),
            "the rebuilt chunks for {guid} are not the ones the new parameters produce"
        );
    }

    // And `news_items` is exactly as it was — the rebuild read it, nothing more.
    assert_eq!(archive.count("news_items").await, 3);
    let archived = news
        .sql("SELECT guid, content FROM archive.main.news_items ORDER BY guid")
        .await;
    assert_eq!(
        opt_col(&archived, "content")[0].as_deref(),
        Some(entry_markdown(ENTRIES[0].1).as_str())
    );
}

// ---------------------------------------------------------------------------
// The live embedding variant
// ---------------------------------------------------------------------------

/// The same chunk `INSERT` with a real embedding in place of the `NULL`.
///
/// `#[ignore]`d for the reason the rest of the repo's `candle` tests are: no
/// model exists on disk in a default CI run, so the default-run archive tests
/// above stop at a `NULL` embedding column. `vec_to_binary` is what makes the
/// `List<Float32>` the UDF returns storable in a sqlite `BLOB`, and is the
/// idiom `sources/providers/sqlite/vec_to_binary.rs`'s own module doc gives for
/// exactly this call shape.
#[cfg(feature = "candle")]
#[tokio::test]
#[ignore = "live: requires SKARDI_TEST_EMBED_MODEL pointing at a local embedding model dir"]
async fn archive_ingest_with_candle_embeddings() {
    use crate::model::candle::CandleModelRegistry;
    use crate::sources::providers::sqlite::register_vec_to_binary_udf;
    use arrow::array::BinaryArray;

    // CI runs every `--ignored` test with `--all-features` and no model on
    // disk; live-resource tests skip rather than panic when the resource is
    // absent (the convention documents' LibreOffice tests and sqlite-vec's
    // `SQLITE_VEC_PATH` tests already follow).
    let Ok(model) = std::env::var("SKARDI_TEST_EMBED_MODEL") else {
        eprintln!("skipping: SKARDI_TEST_EMBED_MODEL not set (needs a local embedding model dir)");
        return;
    };
    assert!(
        !model.contains('\''),
        "the model path is interpolated into SQL as a literal: {model:?}"
    );

    let mock = MockFeeds::start().await;
    mock.serve("/world.xml", Canned::xml(&archive_feed(3)));
    let archive = ArchiveDb::create().await;
    let mut news = Composition::register(&mock, &[("world", "/world.xml")], &archive, |_| {}).await;
    Arc::new(CandleModelRegistry::new()).register_candle_udf(&mut news.ctx);
    register_vec_to_binary_udf(&mut news.ctx);

    assert_eq!(news.insert(INSERT_ITEMS).await, 3);
    let written = news
        .insert(&insert_chunks(
            FIRST_SIZE,
            FIRST_OVERLAP,
            &format!("vec_to_binary(candle('{model}', s.chunk_text))"),
        ))
        .await;
    assert!(written >= 3, "three entries owe at least three chunks");

    let stored = news
        .sql("SELECT embedding FROM archive.main.news_chunks")
        .await;
    assert_eq!(total_rows(&stored) as u64, written);
    for batch in &stored {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("a sqlite BLOB column reads back as Binary");
        for row in 0..column.len() {
            assert!(column.is_valid(row), "row {row} stored a NULL embedding");
            let bytes = column.value(row);
            assert!(!bytes.is_empty(), "row {row} stored an empty embedding");
            assert_eq!(
                bytes.len() % 4,
                0,
                "a packed f32 blob is a whole number of 4-byte lanes, got {} bytes",
                bytes.len()
            );
        }
    }
}
