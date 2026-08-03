//! The acceptance-criteria crosswalk: every test here drives real SQL against
//! a registered `rss` catalog whose feeds live on a [`MockFeedServer`].
//!
//! ## Why this suite is in-crate rather than in `crates/skardi/tests/`
//!
//! Registration's test seam, [`register_rss_tables_with_policy`], is
//! `#[cfg(test)] pub(crate)`: it lets this suite inject an `EgressPolicy`,
//! though every test here uses the same `AllowAll` production ships. An
//! external test crate cannot reach a `pub(crate)` item, and widening the
//! seam to a public entry point would put policy selection on this
//! provider's public surface for no OSS need. `open_connector`'s own
//! mock-HTTP suite is in-crate for the same reason.
//!
//! ## What each test owns
//!
//! Every test builds its own [`TestNews`]: its own server, its own subscription
//! list, its own `SessionContext`, and therefore its own `RssEngine` and
//! window cache. Nothing is shared between tests, so a TTL or health-state
//! assertion describes only what that test did.
//!
//! ## Timing
//!
//! Real time throughout. `#[tokio::test(start_paused = true)]` does not work
//! against [`MockFeedServer`]: tokio's auto-advance races ahead of the real
//! socket round-trip and reqwest's own timeout — also on the paused clock —
//! fires first. Every wait is bounded by an explicit `tokio::time::timeout`
//! ([`TestNews::sql`] carries one for the whole suite) so a regression fails by
//! assertion rather than by hanging the binary.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{Array, RecordBatch, UInt16Array};
use datafusion::prelude::SessionContext;

use super::config::{FeedSubscription, RssConfig, inline_config};
use super::egress::AllowAll;
use super::fetch::MAX_ATTEMPTS;
use super::register_rss_tables_with_policy;
use super::testutil::{
    MockFeedServer, MockResponse, RecordedRequest, str_col, str_opt_col, total_rows,
};
use crate::sources::hierarchy::HierarchyLevel;

/// The catalog name every test registers under, so the SQL below reads
/// `news.main.items` / `news.main.feeds` throughout.
const SOURCE: &str = "news";

/// Backstop on any single query in this suite. Far longer than any bound a
/// test means to exercise — the point is only that a regression that parks a
/// scan forever (a leaked politeness permit, a fetch that never resolves)
/// fails with a named assertion instead of hanging the test binary, where it
/// would read as a CI timeout rather than as this suite's failure.
const QUERY_CEILING: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// A well-formed RSS 2.0 document with one `<item>` per entry of `guids`.
///
/// Every channel-level field the dialect requires is present, so a batch built
/// from it records no conformance notes. Callers give each feed distinct guids,
/// which is what lets an assertion say *which* feed a row came from rather than
/// only how many rows arrived.
fn rss_with(guids: &[&str]) -> String {
    let mut doc = String::from(r#"<rss version="2.0"><channel><title>Mock Feed</title>"#);
    doc.push_str(r#"<link>https://feed.example/</link>"#);
    doc.push_str(r#"<description>A mock feed.</description>"#);
    for guid in guids {
        doc.push_str(&format!(
            "<item><guid>{guid}</guid><title>{guid} title</title>\
             <link>https://feed.example/{guid}</link></item>"
        ));
    }
    doc.push_str("</channel></rss>");
    doc
}

/// The default body served for a mock-hosted path: one item whose guid names
/// the path it came from.
fn default_body(path: &str) -> String {
    rss_with(&[&format!("{path}#1")])
}

/// The guid [`default_body`] gives `path`'s single item.
fn default_guid(path: &str) -> String {
    format!("{path}#1")
}

// ---------------------------------------------------------------------------
// The scripted-response harness
// ---------------------------------------------------------------------------

/// One canned HTTP response, in a form a script can hold and hand out
/// repeatedly.
///
/// [`MockResponse`] is deliberately not `Clone` — it is consumed by the server
/// as it writes it — so a script that answers more than one request stores this
/// instead and builds a fresh [`MockResponse`] per request.
#[derive(Clone)]
struct Canned {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    delay: Option<Duration>,
}

impl Canned {
    /// `200 OK` with an XML content type — a feed document.
    fn xml(body: &str) -> Self {
        Self {
            status: 200,
            headers: vec![("content-type".to_string(), "application/xml".to_string())],
            body: body.as_bytes().to_vec(),
            delay: None,
        }
    }

    /// A bare status with no body and no headers.
    fn status(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
            delay: None,
        }
    }

    /// A raw byte body under a caller-chosen status — the gzip fixture's shape.
    fn bytes(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body,
            delay: None,
        }
    }

    fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    /// Hold the response back by `delay` before writing it, for the tests that
    /// need a slow upstream.
    fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    fn into_response(self) -> MockResponse {
        let mut response = MockResponse::new(self.status, self.body);
        for (name, value) in self.headers {
            response = response.with_header(&name, &value);
        }
        match self.delay {
            Some(delay) => response.with_delay(delay),
            None => response,
        }
    }
}

/// What one path answers, request by request.
struct FeedScript {
    /// Responses in order; once exhausted the last one repeats, so
    /// `always(x)` is just a one-step script.
    steps: Vec<Canned>,
    /// How many requests this script has answered.
    served: usize,
    /// When set, this path behaves like a feed host with a validator: a `200`
    /// carries the `etag`, and a request presenting it as `If-None-Match` gets
    /// a bare `304` instead of the next step.
    etag: Option<String>,
}

impl FeedScript {
    /// One response, repeated for every request.
    fn always(canned: Canned) -> Self {
        Self::steps(vec![canned])
    }

    /// A response per request, the last repeating. Panics on an empty list:
    /// a script with nothing to serve is a mistake in the test, not a case to
    /// invent a response for.
    fn steps(steps: Vec<Canned>) -> Self {
        assert!(!steps.is_empty(), "a feed script needs at least one step");
        Self {
            steps,
            served: 0,
            etag: None,
        }
    }

    /// Give this path a validator — see [`FeedScript::etag`].
    fn with_etag(mut self, etag: &str) -> Self {
        self.etag = Some(etag.to_string());
        self
    }

    /// Answer one request, advancing the script.
    fn answer(&mut self, request: &RecordedRequest) -> MockResponse {
        self.served += 1;
        if let Some(etag) = &self.etag
            && request.header("if-none-match").as_deref() == Some(etag.as_str())
        {
            return MockResponse::status(304);
        }
        let step = self.steps[(self.served - 1).min(self.steps.len() - 1)].clone();
        match (&self.etag, step.status) {
            (Some(etag), 200) => step.with_header("etag", etag).into_response(),
            _ => step.into_response(),
        }
    }
}

/// Path → script, shared between the test body and the server's handler.
type Scripts = Arc<Mutex<HashMap<String, FeedScript>>>;

/// A registered `rss` source called `news`, its mock server, and the scripts
/// that decide what each path answers.
struct TestNews {
    server: MockFeedServer,
    ctx: SessionContext,
    scripts: Scripts,
}

impl TestNews {
    /// Register a source over `feeds`, given as `(name, target)` pairs.
    ///
    /// A `target` beginning with `http://` or `https://` is used verbatim —
    /// for a subscription that must point somewhere other than this harness's
    /// own mock server. Anything else is a path on the mock server, and gets
    /// a default script serving [`default_body`].
    ///
    /// `tune` runs on the spec-default config after this harness's own two
    /// adjustments, so a test can override either. Those two: the request
    /// timeout drops from 10s to 5s and the scan deadline from 60s to 20s, so a
    /// test that starts hanging says so in seconds rather than sitting on the
    /// production deadline.
    async fn start(feeds: &[(&str, &str)], tune: impl FnOnce(&mut RssConfig)) -> Self {
        let scripts: Scripts = Arc::new(Mutex::new(HashMap::new()));
        let handler_scripts = Arc::clone(&scripts);
        let server = MockFeedServer::start(move |request| {
            let mut scripts = handler_scripts
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match scripts.get_mut(&request.path) {
                Some(script) => script.answer(request),
                // An unscripted path is a mistake in the test. `404` is outside
                // `fetch.rs`'s `RETRYABLE_STATUSES`, so it surfaces as that
                // feed's `last_error` after one request rather than after a
                // whole attempt budget.
                None => MockResponse::status(404),
            }
        })
        .await;

        let mut subscriptions = Vec::with_capacity(feeds.len());
        for (name, target) in feeds {
            let url = if target.starts_with("http://") || target.starts_with("https://") {
                (*target).to_string()
            } else {
                scripts
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(
                        (*target).to_string(),
                        FeedScript::always(Canned::xml(&default_body(target))),
                    );
                format!("{}{target}", server.url())
            };
            subscriptions.push(FeedSubscription {
                url,
                name: Some((*name).to_string()),
            });
        }

        let mut config = inline_config(subscriptions);
        config.request_timeout_seconds = 5;
        config.scan_timeout_seconds = 20;
        tune(&mut config);

        let mut ctx = SessionContext::new();
        register_rss_tables_with_policy(
            &mut ctx,
            SOURCE,
            Some(&config),
            false,
            HierarchyLevel::Catalog,
            // The production default: no destination filtering.
            Arc::new(AllowAll),
        )
        .await
        .expect("registration succeeds");

        Self {
            server,
            ctx,
            scripts,
        }
    }

    /// Replace what `path` answers from the next request on.
    fn script(&self, path: &str, script: FeedScript) {
        self.scripts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(path.to_string(), script);
    }

    /// Run one query to completion, under [`QUERY_CEILING`].
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

    /// Every request the mock has observed, in arrival order.
    fn requests(&self) -> Vec<RecordedRequest> {
        self.server.requests()
    }

    /// The paths of every request observed, in arrival order.
    fn paths(&self) -> Vec<String> {
        self.requests()
            .into_iter()
            .map(|request| request.path)
            .collect()
    }

    fn request_count(&self) -> usize {
        self.server.requests().len()
    }

    /// A handle on the registered context, for the one test that has to run a
    /// query on a task it then aborts.
    fn context(&self) -> SessionContext {
        self.ctx.clone()
    }

    /// Wait until the mock has observed at least `count` requests, or fail.
    ///
    /// Polled rather than signalled because the mock records requests under its
    /// own lock with nothing to subscribe to. The bound is what turns "the
    /// request never arrived" into a named failure instead of a hung binary.
    async fn await_requests(&self, count: usize, within: Duration) {
        let deadline = Instant::now() + within;
        while self.request_count() < count {
            assert!(
                Instant::now() < deadline,
                "only {} of {count} requests arrived within {within:?}",
                self.request_count()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
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

/// One nullable `UInt16` column (`feeds.http_status`), NULLs preserved.
fn u16_col(batches: &[RecordBatch], name: &str) -> Vec<Option<u16>> {
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
                .downcast_ref::<UInt16Array>()
                .unwrap_or_else(|| panic!("column {name:?} is not UInt16"));
            (0..column.len())
                .map(|row| column.is_valid(row).then(|| column.value(row)))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// A sorted copy of `paths` — for asserting *which* paths were fetched when
/// concurrent partitions make the arrival order undefined.
fn sorted(mut paths: Vec<String>) -> Vec<String> {
    paths.sort();
    paths
}

/// Assert that `actual` contains `needle`, naming both on failure. A bare
/// `contains` assertion would report only "false".
#[track_caller]
fn assert_contains(actual: &str, needle: &str) {
    assert!(actual.contains(needle), "expected {needle:?} in {actual:?}");
}

// ---------------------------------------------------------------------------
// AC1 — registration is zero-network
// ---------------------------------------------------------------------------

/// Registering N subscriptions touches no host, and neither does a `feeds`
/// scan: startup cost is proportional to the size of the subscription list,
/// not to the availability of the hosts on it.
#[tokio::test]
async fn ac1_registration_is_zero_network() {
    let news = TestNews::start(&[("a", "/a.xml"), ("b", "/b.xml"), ("c", "/c.xml")], |_| {}).await;
    assert_eq!(
        news.request_count(),
        0,
        "registration performed network I/O"
    );

    let feeds = news
        .sql("SELECT name, last_status, last_error FROM news.main.feeds ORDER BY name")
        .await;
    assert_eq!(col(&feeds, "name"), vec!["a", "b", "c"]);
    assert_eq!(
        col(&feeds, "last_status"),
        vec!["never", "never", "never"],
        "no subscription has been attempted"
    );
    assert_eq!(opt_col(&feeds, "last_error"), vec![None, None, None]);
    assert_eq!(
        news.request_count(),
        0,
        "a feeds scan performed network I/O"
    );
}

// ---------------------------------------------------------------------------
// AC2 — a full scan visits every feed; a feed predicate visits exactly one
// ---------------------------------------------------------------------------

/// `ttl_seconds: 0` throughout, so the second query is a live fetch rather than
/// a cache hit: what is under test here is which feeds a predicate visits, not
/// the TTL (AC3 owns that).
#[tokio::test]
async fn ac2_full_scan_fetches_all_where_prunes_to_one() {
    let news = TestNews::start(
        &[("a", "/a.xml"), ("b", "/b.xml"), ("c", "/c.xml")],
        |config| config.ttl_seconds = 0,
    )
    .await;

    let all = news
        .sql("SELECT feed, guid FROM news.main.items ORDER BY feed")
        .await;
    assert_eq!(col(&all, "feed"), vec!["a", "b", "c"]);
    assert_eq!(
        col(&all, "guid"),
        vec![
            default_guid("/a.xml"),
            default_guid("/b.xml"),
            default_guid("/c.xml")
        ],
        "each row carries the guid of the feed it was fetched from"
    );
    assert_eq!(
        sorted(news.paths()),
        vec!["/a.xml", "/b.xml", "/c.xml"],
        "a full scan visits every subscription"
    );

    let one = news
        .sql("SELECT feed, guid FROM news.main.items WHERE feed = 'b'")
        .await;
    assert_eq!(col(&one, "feed"), vec!["b"]);
    assert_eq!(col(&one, "guid"), vec![default_guid("/b.xml")]);
    assert_eq!(
        news.paths()[3..],
        ["/b.xml".to_string()],
        "the pruned scan fetched exactly the one feed the predicate names"
    );
}

// ---------------------------------------------------------------------------
// AC3 — the TTL, and the conditional-GET path once it expires
// ---------------------------------------------------------------------------

/// Two halves, each needing its own engine because the TTL is fixed at
/// registration: within the TTL a second scan issues no request at all, and
/// with the TTL expired an unchanged feed takes the `304` path and serves its
/// cached window stamped `revalidated`.
#[tokio::test]
async fn ac3_ttl_and_304_paths() {
    // --- within the TTL: the second scan is served from cache
    let cached = TestNews::start(
        &[("a", "/a.xml"), ("b", "/b.xml"), ("c", "/c.xml")],
        |config| config.ttl_seconds = 900,
    )
    .await;
    let first = cached
        .sql("SELECT feed, window_status FROM news.main.items")
        .await;
    assert_eq!(total_rows(&first), 3);
    assert_eq!(cached.request_count(), 3, "one fetch per feed");

    let second = cached
        .sql("SELECT feed, window_status FROM news.main.items")
        .await;
    assert_eq!(total_rows(&second), 3, "the cached windows still serve");
    assert_eq!(
        cached.request_count(),
        3,
        "a second scan within the TTL issues no request"
    );

    // --- TTL expired, feed unchanged: conditional GET, 304, revalidated
    let live = TestNews::start(&[("a", "/a.xml")], |config| config.ttl_seconds = 0).await;
    live.script(
        "/a.xml",
        FeedScript::always(Canned::xml(&default_body("/a.xml"))).with_etag("\"v1\""),
    );

    let fresh = live
        .sql("SELECT guid, window_status FROM news.main.items")
        .await;
    assert_eq!(col(&fresh, "window_status"), vec!["fresh"]);
    assert_eq!(live.request_count(), 1);
    assert_eq!(
        live.requests()[0].header("if-none-match"),
        None,
        "the first request has no validator to send"
    );

    let revalidated = live
        .sql("SELECT guid, window_status FROM news.main.items")
        .await;
    assert_eq!(live.request_count(), 2, "the expired feed was revalidated");
    assert_eq!(
        live.requests()[1].header("if-none-match").as_deref(),
        Some("\"v1\""),
        "the second request carries the etag the first response set"
    );
    assert_eq!(
        col(&revalidated, "window_status"),
        vec!["revalidated"],
        "a 304 serves the cached window, relabelled"
    );
    assert_eq!(
        col(&revalidated, "guid"),
        vec![default_guid("/a.xml")],
        "and the rows are the ones the 304 vouches for"
    );

    let feeds = live
        .sql("SELECT last_status, http_status, etag FROM news.main.feeds")
        .await;
    assert_eq!(col(&feeds, "last_status"), vec!["revalidated"]);
    assert_eq!(u16_col(&feeds, "http_status"), vec![Some(304)]);
    assert_eq!(
        opt_col(&feeds, "etag"),
        vec![Some("\"v1\"".to_string())],
        "the validator survives the revalidation"
    );
}

// ---------------------------------------------------------------------------
// AC4 — one dead feed among N degrades alone, visibly, in the result stream
// ---------------------------------------------------------------------------

/// The degradation has to be visible *as data*: the dead feed's last good
/// window is still served, stamped `stale-error`, while its neighbours are
/// untouched value for value.
#[tokio::test]
async fn ac4_dead_feed_isolation_with_stale_stamp() {
    let news = TestNews::start(
        &[("a", "/a.xml"), ("b", "/b.xml"), ("c", "/c.xml")],
        |config| config.ttl_seconds = 0,
    )
    .await;

    let healthy = news
        .sql("SELECT feed, guid, title, window_status FROM news.main.items ORDER BY feed")
        .await;
    assert_eq!(col(&healthy, "feed"), vec!["a", "b", "c"]);
    assert_eq!(
        col(&healthy, "window_status"),
        vec!["fresh", "fresh", "fresh"]
    );

    news.script("/b.xml", FeedScript::always(Canned::status(500)));

    let degraded = news
        .sql("SELECT feed, guid, title, window_status FROM news.main.items ORDER BY feed")
        .await;
    assert_eq!(
        col(&degraded, "feed"),
        vec!["a", "b", "c"],
        "the dead feed still contributes its cached window"
    );
    assert_eq!(
        col(&degraded, "window_status"),
        vec!["fresh", "stale-error", "fresh"],
        "and only that feed's rows are relabelled"
    );
    assert_eq!(
        col(&degraded, "guid"),
        col(&healthy, "guid"),
        "the served rows are the same rows, dead feed included"
    );
    assert_eq!(
        opt_col(&degraded, "title"),
        opt_col(&healthy, "title"),
        "a neighbour's values are unaffected by another feed's failure"
    );

    let feeds = news
        .sql(
            "SELECT name, last_status, http_status, last_error, item_count \
             FROM news.main.feeds ORDER BY name",
        )
        .await;
    assert_eq!(
        col(&feeds, "last_status"),
        vec!["fresh", "stale-error", "fresh"]
    );
    assert_eq!(
        u16_col(&feeds, "http_status"),
        vec![Some(200), Some(500), Some(200)]
    );
    let errors = opt_col(&feeds, "last_error");
    assert_eq!(errors[0], None, "a healthy feed carries no error");
    assert_eq!(errors[2], None, "a healthy feed carries no error");
    assert_contains(
        errors[1]
            .as_deref()
            .expect("the dead feed records an error"),
        "http status 500",
    );

    // `500` is retryable, so the dead feed costs a whole attempt budget rather
    // than one request. Asserted against the constant so a change to the retry
    // policy shows up here as a deliberate edit rather than a surprise.
    assert_eq!(
        news.paths().iter().filter(|path| *path == "/b.xml").count(),
        1 + MAX_ATTEMPTS as usize,
        "the first scan's one success, then one exhausted attempt budget"
    );
}

// ---------------------------------------------------------------------------
// AC13 — a `feeds` scan issues zero requests at any moment
// ---------------------------------------------------------------------------

/// Including immediately after a failure: the failure re-arms the TTL to the
/// negative-caching fuse, so a dead feed is not re-poked, and `feeds` cannot
/// reach the fetcher at all.
#[tokio::test]
async fn ac13_feeds_scan_zero_requests_even_after_failure() {
    let news = TestNews::start(&[("a", "/a.xml"), ("b", "/b.xml")], |config| {
        config.ttl_seconds = 0
    })
    .await;

    news.sql("SELECT guid FROM news.main.items").await;
    news.script("/b.xml", FeedScript::always(Canned::status(500)));
    news.sql("SELECT guid FROM news.main.items").await;

    let after_failure = news.request_count();
    let feeds = news
        .sql("SELECT name, last_status FROM news.main.feeds ORDER BY name")
        .await;
    assert_eq!(
        col(&feeds, "last_status"),
        vec!["fresh", "stale-error"],
        "the failure is visible in the health table"
    );
    assert_eq!(
        news.request_count(),
        after_failure,
        "a feeds scan right after a failure issued a request"
    );

    news.sql("SELECT * FROM news.main.feeds").await;
    assert_eq!(
        news.request_count(),
        after_failure,
        "a second feeds scan issued a request"
    );

    // The other half of "not re-poked": the failure re-armed the dead feed's
    // timer to the negative-caching fuse rather than to the (zero) success TTL,
    // so even an `items` scan leaves it alone until the fuse expires. The fuse
    // floors at 30s (`cache.rs`'s `failure_fuse`), which is far longer than this
    // test takes, so a re-poke would show up as a new request here.
    let items = news
        .sql("SELECT feed, window_status FROM news.main.items ORDER BY feed")
        .await;
    assert_eq!(
        col(&items, "window_status"),
        vec!["fresh", "stale-error"],
        "the dead feed still serves its stale window from cache"
    );
    assert_eq!(
        news.paths().iter().filter(|path| *path == "/b.xml").count(),
        1 + MAX_ATTEMPTS as usize,
        "the dead feed was re-poked inside its failure fuse"
    );
}

// ---------------------------------------------------------------------------
// The bounds: response size, per-request timeout, Retry-After
// ---------------------------------------------------------------------------

/// The decompressed-size cap, exercised against a real gzip bomb.
///
/// The mechanics matter: `fixtures/bomb.xml.gz` is 8,396 bytes on the wire and
/// inflates to 8,388,854 bytes of *valid* RSS 2.0 — 1.60x the 5 MiB
/// `max_response_bytes` default this test leaves untouched. The wire form is
/// three orders of magnitude under the cap, so a refusal here can only have
/// come from counting decoded bytes; a cap applied to the compressed stream
/// would let this through and the feed would parse. `corpus.rs`'s
/// `the_gzip_bomb_fixture_is_still_a_bomb` pins the fixture's own properties
/// (gzip magic, footer `ISIZE` over the cap, wire form small enough to commit)
/// so this test can assume them.
#[tokio::test]
async fn decompressed_cap_rejects_gzip_bomb() {
    const BOMB: &[u8] = include_bytes!("fixtures/bomb.xml.gz");

    let news = TestNews::start(
        &[("bomb", "/bomb.xml"), ("healthy", "/healthy.xml")],
        |_| {},
    )
    .await;
    assert!(
        (BOMB.len() as u64) < 64 * 1024,
        "the body served is {} bytes, which is not a decompression test",
        BOMB.len()
    );
    news.script(
        "/bomb.xml",
        FeedScript::always(
            Canned::bytes(200, BOMB.to_vec())
                .with_header("content-type", "application/xml")
                .with_header("content-encoding", "gzip"),
        ),
    );

    let items = news
        .sql("SELECT feed, guid FROM news.main.items ORDER BY feed")
        .await;
    assert_eq!(
        col(&items, "feed"),
        vec!["healthy"],
        "the bomb contributes no rows; its healthy sibling is unaffected"
    );
    assert_eq!(col(&items, "guid"), vec![default_guid("/healthy.xml")]);

    let feeds = news
        .sql("SELECT name, last_status, last_error FROM news.main.feeds ORDER BY name")
        .await;
    assert_eq!(col(&feeds, "last_status"), vec!["error", "fresh"]);
    assert_eq!(
        opt_col(&feeds, "last_error")[0].as_deref(),
        Some("response exceeded 5242880 bytes"),
        "the refusal names the decoded-byte cap it broke"
    );

    assert_eq!(
        news.paths()
            .iter()
            .filter(|path| *path == "/bomb.xml")
            .count(),
        1,
        "an over-cap body is terminal, not retried"
    );
}

/// A per-request timeout isolates a slow feed: it degrades, its neighbour
/// serves, and the whole query still finishes well inside the scan deadline.
///
/// The outer `tokio::time::timeout` is what makes a regression assert rather
/// than hang. Real time, with the request timeout (1s) far below the harness's
/// 20s scan deadline so the bound under test is the one that fires — the scan
/// deadline degrading the partition instead would produce the same zero rows
/// with a *different* `feeds` row, which is why `last_error` is asserted
/// exactly.
#[tokio::test]
async fn request_timeout_isolates_slow_feed() {
    let news = TestNews::start(&[("fast", "/fast.xml"), ("slow", "/slow.xml")], |config| {
        config.request_timeout_seconds = 1;
    })
    .await;
    news.script(
        "/slow.xml",
        FeedScript::always(
            Canned::xml(&default_body("/slow.xml")).with_delay(Duration::from_secs(3)),
        ),
    );

    let started = Instant::now();
    let items = tokio::time::timeout(
        Duration::from_secs(30),
        news.sql("SELECT feed, guid FROM news.main.items ORDER BY feed"),
    )
    .await
    .expect("the slow feed must time out rather than hang the scan");
    let elapsed = started.elapsed();

    assert_eq!(
        col(&items, "feed"),
        vec!["fast"],
        "the fast feed serves while the slow one is still being retried"
    );
    assert!(
        elapsed < Duration::from_secs(20),
        "the query took {elapsed:?}, so it was the scan deadline that cut it, not the \
         per-request timeout"
    );

    let feeds = news
        .sql("SELECT name, last_status, last_error FROM news.main.feeds ORDER BY name")
        .await;
    assert_eq!(col(&feeds, "last_status"), vec!["fresh", "error"]);
    assert_eq!(
        opt_col(&feeds, "last_error")[1].as_deref(),
        Some("request timed out after 1s"),
        "the recorded reason is the per-request timeout, not the scan deadline"
    );
    assert_eq!(
        news.paths()
            .iter()
            .filter(|path| *path == "/slow.xml")
            .count(),
        MAX_ATTEMPTS as usize,
        "a timeout is retryable, so the slow feed spends its whole attempt budget"
    );
}

/// A `429` with `Retry-After: 1` is waited out *inside* the scan and then
/// succeeds — the wait is honoured rather than the feed being given up on.
#[tokio::test]
async fn retry_after_is_honored_within_scan() {
    let news = TestNews::start(&[("a", "/a.xml")], |_| {}).await;
    news.script(
        "/a.xml",
        FeedScript::steps(vec![
            Canned::status(429).with_header("retry-after", "1"),
            Canned::xml(&default_body("/a.xml")),
        ]),
    );

    let started = Instant::now();
    let items = news.sql("SELECT feed, guid FROM news.main.items").await;
    let elapsed = started.elapsed();

    assert_eq!(col(&items, "guid"), vec![default_guid("/a.xml")]);
    assert_eq!(
        news.paths(),
        vec!["/a.xml", "/a.xml"],
        "the retry happened, and only one of them"
    );
    assert!(
        elapsed >= Duration::from_secs(1),
        "the retry waited {elapsed:?}, which is less than the Retry-After it was given"
    );

    let feeds = news
        .sql("SELECT last_status, http_status FROM news.main.feeds")
        .await;
    assert_eq!(col(&feeds, "last_status"), vec!["fresh"]);
    assert_eq!(u16_col(&feeds, "http_status"), vec![Some(200)]);
}

// ---------------------------------------------------------------------------
// The LIMIT launch gate, and cancellation
// ---------------------------------------------------------------------------

/// A `LIMIT` stops *requests*, and the spec's documented distinction between
/// the two shapes that reach the scan differently.
///
/// `LIMIT 1` alone: DataFusion copies the fetch into the `TableScan`, so the
/// scan gets `limit: Some(1)`, and only one of the three feeds is requested.
///
/// What stops the other two here is *not* the launch gate — this test cannot
/// tell the gate from ordinary stream cancellation, and an earlier version of
/// this docstring claimed the post-permit gate re-read. Measured by mutation:
/// with the exec pre-check and the closure gate both defeated (`exec.rs:357`
/// and `:363`), this test still passes while four `exec.rs` tests fail. With one
/// politeness permit the two later partitions are still queued on the semaphore
/// when the limit fills, so DataFusion drops their un-polled streams — the
/// mechanism [`cancellation_stops_further_fetches`] owns directly. The gate
/// itself is pinned in `exec.rs`, where a closed gate *is* distinguishable from a
/// dropped stream: `limit_satisfied_stops_launching_fetches` (`exec.rs:531`) for
/// the pre-check, and `a_queued_partition_is_stopped_by_the_gate_after_the_permit`
/// (`exec.rs:572`) for the re-read after the permit.
///
/// `ORDER BY guid LIMIT 1`: a Top-K has to see every row before it knows which
/// one wins, so the limit stops at the sort and never reaches the scan. Every
/// feed is fetched. `ttl_seconds: 0` makes that measurable — otherwise the
/// second query would be answered from the first one's cache and prove nothing.
#[tokio::test]
async fn limit_stops_launching_fetches() {
    let news = TestNews::start(
        &[("a", "/a.xml"), ("b", "/b.xml"), ("c", "/c.xml")],
        |config| {
            config.ttl_seconds = 0;
            config.max_concurrent = 1;
        },
    )
    .await;

    let limited = news.sql("SELECT guid FROM news.main.items LIMIT 1").await;
    assert_eq!(total_rows(&limited), 1);
    assert_eq!(
        news.request_count(),
        1,
        "one row of LIMIT is one fetch: the partitions past it must not launch one"
    );

    let top_k = news
        .sql("SELECT guid FROM news.main.items ORDER BY guid LIMIT 1")
        .await;
    assert_eq!(total_rows(&top_k), 1);
    assert_eq!(
        col(&top_k, "guid"),
        vec![default_guid("/a.xml")],
        "the Top-K's winner is the smallest guid across every feed"
    );
    assert_eq!(
        sorted(news.paths()[1..].to_vec()),
        vec!["/a.xml", "/b.xml", "/c.xml"],
        "a Top-K consumes every partition, so the limit cannot gate any fetch"
    );
}

/// Cancelling a query stops the fetches it had not launched yet.
///
/// Three slow feeds and one permit: the first is in flight when the query is
/// aborted and the other two are still queued on the semaphore. Dropping the
/// stream drops their futures before they are ever handed the permit, so the
/// request count stays at one — checked after a wait long enough for both
/// delayed responses to have completed had the fetches been launched.
#[tokio::test]
async fn cancellation_stops_further_fetches() {
    let news = TestNews::start(
        &[("a", "/a.xml"), ("b", "/b.xml"), ("c", "/c.xml")],
        |config| config.max_concurrent = 1,
    )
    .await;
    for path in ["/a.xml", "/b.xml", "/c.xml"] {
        news.script(
            path,
            FeedScript::always(
                Canned::xml(&default_body(path)).with_delay(Duration::from_millis(500)),
            ),
        );
    }

    let ctx = news.context();
    let query = tokio::spawn(async move {
        ctx.sql("SELECT guid FROM news.main.items")
            .await
            .expect("plan")
            .collect()
            .await
    });

    news.await_requests(1, Duration::from_secs(10)).await;
    query.abort();
    assert!(
        query
            .await
            .expect_err("the query was aborted")
            .is_cancelled(),
        "the task ended for some reason other than the abort"
    );

    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        news.paths().len(),
        1,
        "the cancelled scan launched further fetches: {:?}",
        news.paths()
    );
}

// ---------------------------------------------------------------------------
// Pruning at the SQL surface
// ---------------------------------------------------------------------------

/// Five subscriptions, so an `IN` list can name four of them — the shortest
/// list that reaches the provider as an `Expr::InList` at all. See
/// [`a_short_in_list_is_rewritten_to_a_disjunction_and_still_prunes`] for why
/// the length matters, and why both lengths now prune anyway.
const FIVE_FEEDS: [(&str, &str); 5] = [
    ("a", "/a.xml"),
    ("b", "/b.xml"),
    ("c", "/c.xml"),
    ("d", "/d.xml"),
    ("e", "/e.xml"),
];

/// A non-negated `IN` list prunes to exactly its members — for a list long
/// enough to survive expression simplification.
#[tokio::test]
async fn a_long_in_list_prunes_to_its_members() {
    let news = TestNews::start(&FIVE_FEEDS, |_| {}).await;

    let items = news
        .sql("SELECT feed FROM news.main.items WHERE feed IN ('a', 'b', 'c', 'd') ORDER BY feed")
        .await;
    assert_eq!(col(&items, "feed"), vec!["a", "b", "c", "d"]);
    assert_eq!(
        sorted(news.paths()),
        vec!["/a.xml", "/b.xml", "/c.xml", "/d.xml"],
        "the feed the IN list omits is never fetched"
    );
}

/// An `IN` list of three or fewer values never reaches this provider as an
/// `Expr::InList` — and prunes anyway, because the classifier recognises the
/// disjunction it was rewritten into.
///
/// Measured, not inferred: `ShortenInListSimplifier` rewrites `col IN (…)` into
/// a chain of `OR`s when the list holds a single value, or at most
/// `THRESHOLD_INLINE_INLIST` (3) values with a plain column on the left
/// (datafusion-optimizer 52.5.0,
/// `src/simplify_expressions/inlist_simplifier.rs:38-56`, the non-negated fold
/// at `:82-90`, and the constant at
/// `src/simplify_expressions/expr_simplifier.rs:111`). `split_conjunction`
/// recurses only through `AND`, so the resulting `OR` arrives at the provider as
/// one `BinaryExpr` — which `table.rs`'s classifier now reads as a disjunction
/// of feed equalities and prunes to the union of its leaves. `EXPLAIN` for this
/// query is a bare `TableScan: items projection=[feed],
/// full_filters=[items.feed = Utf8("a") OR items.feed = Utf8("c")]` over
/// `RssScanExec: kind=items feeds=2`: no `Filter` survives above the scan, which
/// is the `Exact` claim being honoured.
///
/// This is the SQL shape that made the classifier's `InList` arm unreachable
/// below four values. Keeping the test at two values is the point: it fails if
/// the disjunction path regresses, and the `EXPLAIN` above is how to tell which
/// arm a given length lands on.
#[tokio::test]
async fn a_short_in_list_is_rewritten_to_a_disjunction_and_still_prunes() {
    let news = TestNews::start(&[("a", "/a.xml"), ("b", "/b.xml"), ("c", "/c.xml")], |_| {}).await;

    let items = news
        .sql("SELECT feed FROM news.main.items WHERE feed IN ('a', 'c') ORDER BY feed")
        .await;
    assert_eq!(col(&items, "feed"), vec!["a", "c"]);
    assert_eq!(
        sorted(news.paths()),
        vec!["/a.xml", "/c.xml"],
        "the rewritten short IN list prunes to its members, so `b` is never fetched"
    );
}

/// The same disjunction written by hand, which no `IN` rewrite produces and
/// which the `InList` arm could therefore never have covered.
///
/// The row assertion is the half that must never regress — pruning is an
/// optimisation, returning exactly `a` and `c` is not — and the request count is
/// the only thing that catches a silent loss of pruning, since an unpruned scan
/// returns the same rows.
#[tokio::test]
async fn a_hand_written_disjunction_of_feed_equalities_prunes() {
    let news = TestNews::start(&FIVE_FEEDS, |_| {}).await;

    let items = news
        .sql(
            "SELECT feed FROM news.main.items \
             WHERE feed = 'a' OR feed = 'c' ORDER BY feed",
        )
        .await;
    assert_eq!(col(&items, "feed"), vec!["a", "c"]);
    assert_eq!(
        sorted(news.paths()),
        vec!["/a.xml", "/c.xml"],
        "a hand-written OR over one feed column prunes to the union of its leaves"
    );
}

/// A disjunction mixing `feed` and `feed_url` is *not* prunable: the classifier
/// takes one feed column per disjunction, so this shape stays `Unsupported` and
/// DataFusion filters it above the scan.
///
/// The rows are the obligation, and they are right either way. The request count
/// is what states the deliberate limitation — if mixed-column disjunctions are
/// ever supported, this is the test that says so.
#[tokio::test]
async fn a_disjunction_mixing_feed_and_feed_url_does_not_prune() {
    let news = TestNews::start(&[("a", "/a.xml"), ("b", "/b.xml"), ("c", "/c.xml")], |_| {}).await;
    let c_url = format!("{}/c.xml", news.server.url());

    let items = news
        .sql(&format!(
            "SELECT feed FROM news.main.items \
             WHERE feed = 'a' OR feed_url = '{c_url}' ORDER BY feed"
        ))
        .await;
    assert_eq!(
        col(&items, "feed"),
        vec!["a", "c"],
        "the predicate is still applied — above the scan, by DataFusion"
    );
    assert_eq!(
        sorted(news.paths()),
        vec!["/a.xml", "/b.xml", "/c.xml"],
        "a disjunction over two feed columns prunes nothing, so every subscription is fetched"
    );
}

/// Two prunable predicates on one column intersect at the SQL surface.
///
/// This pins the end-to-end assumption the provider's `Exact` claim rests on:
/// DataFusion splits a conjunction into separate `Expr`s before consulting
/// `supports_filters_pushdown`, and the provider intersects them by narrowing
/// the surviving subscription list once per predicate. Nothing else in the tree
/// pins that: if the split stopped happening the conjunction would arrive as a
/// single `BinaryExpr` the classifier rejects, pruning would silently degrade to
/// visiting all five feeds, and the rows would still be correct — so only a
/// request count catches it.
///
/// Both operand orders, because the intersection is what is under test: a
/// provider that kept only the *first* prunable predicate would fetch four feeds
/// on the first query, and one that kept only the *last* would fetch four on the
/// second.
#[tokio::test]
async fn conjunction_of_feed_predicates_prunes_to_the_intersection() {
    // ttl 0: the second query must be a live fetch, not the first one's cache.
    let news = TestNews::start(&FIVE_FEEDS, |config| config.ttl_seconds = 0).await;

    let in_then_eq = news
        .sql("SELECT feed FROM news.main.items WHERE feed IN ('a','b','c','d') AND feed = 'b'")
        .await;
    assert_eq!(col(&in_then_eq, "feed"), vec!["b"]);
    assert_eq!(
        news.paths(),
        vec!["/b.xml"],
        "the intersection of the two predicates is one feed, so one fetch"
    );

    let eq_then_in = news
        .sql("SELECT feed FROM news.main.items WHERE feed = 'b' AND feed IN ('a','b','c','d')")
        .await;
    assert_eq!(col(&eq_then_in, "feed"), vec!["b"]);
    assert_eq!(
        news.paths()[1..],
        ["/b.xml".to_string()],
        "the intersection does not depend on which predicate came first"
    );
}

/// Two differently-named subscriptions may share a `feed_url`:
/// `RssConfig::validate` enforces unique *names* only. Pruning on that shared
/// URL must visit both.
///
/// The failure this guards against returns wrong results while still claiming
/// `Exact`: a URL→subscription lookup that stopped at the first match would
/// drop one feed's rows entirely, and every other assertion in this suite would
/// still pass.
#[tokio::test]
async fn pruning_by_a_shared_feed_url_visits_every_subscription_using_it() {
    let news = TestNews::start(
        &[
            ("primary", "/shared.xml"),
            ("mirror", "/shared.xml"),
            ("other", "/other.xml"),
        ],
        |_| {},
    )
    .await;
    let shared = format!("{}/shared.xml", news.server.url());

    let items = news
        .sql(&format!(
            "SELECT feed, feed_url FROM news.main.items WHERE feed_url = '{shared}' ORDER BY feed"
        ))
        .await;
    assert_eq!(
        col(&items, "feed"),
        vec!["mirror", "primary"],
        "both subscriptions on the shared URL must contribute rows"
    );
    assert_eq!(col(&items, "feed_url"), vec![shared.clone(), shared]);
    assert_eq!(
        news.paths(),
        vec!["/shared.xml", "/shared.xml"],
        "one fetch per surviving subscription, and the unrelated feed is pruned away"
    );
}

// ---------------------------------------------------------------------------
// The remaining surface criteria
// ---------------------------------------------------------------------------

/// Every request carries the configured `User-Agent`.
///
/// The expected value is rebuilt here from the crate version rather than read
/// off the config, because the header is wire-visible: an upstream feed host
/// sees this string, so a change to its shape has to fail a test rather than
/// pass one that derives its expectation from the same place.
#[tokio::test]
async fn user_agent_is_sent() {
    let news = TestNews::start(&[("a", "/a.xml"), ("b", "/b.xml")], |_| {}).await;
    news.sql("SELECT guid FROM news.main.items").await;

    let expected = format!(
        "skardi-rss/{} (+https://github.com/SkardiLabs/skardi)",
        env!("CARGO_PKG_VERSION")
    );
    let requests = news.requests();
    assert_eq!(requests.len(), 2, "both feeds were fetched");
    for request in &requests {
        assert_eq!(
            request.header("user-agent").as_deref(),
            Some(expected.as_str()),
            "request to {} sent the wrong User-Agent",
            request.path
        );
    }
}

/// `SELECT count(*)` reaches the scan as the empty projection, and a
/// zero-column batch that lost its row count would make the count wrong.
///
/// Checked against the same rows read the ordinary way, *and* against a spelled
/// out number — a bug that dropped a whole feed's window would keep the two
/// paths agreeing with each other.
#[tokio::test]
async fn count_star_over_items_is_row_accurate() {
    let news = TestNews::start(&[("a", "/a.xml"), ("b", "/b.xml")], |_| {}).await;
    news.script(
        "/a.xml",
        FeedScript::always(Canned::xml(&rss_with(&["a1", "a2", "a3"]))),
    );

    let counted = news.sql("SELECT count(*) AS n FROM news.main.items").await;
    let n = counted[0]
        .column_by_name("n")
        .expect("count(*) column")
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count(*) is Int64")
        .value(0);
    assert_eq!(n, 4, "three items from feed a plus one from feed b");

    let listed = news.sql("SELECT guid FROM news.main.items").await;
    assert_eq!(
        total_rows(&listed) as i64,
        n,
        "the empty projection and the ordinary one disagree about the row count"
    );
    assert_eq!(
        sorted(col(&listed, "guid")),
        vec!["/b.xml#1", "a1", "a2", "a3"]
    );
}

/// The absence check the design prescribes: a subscription that served nothing
/// is found by an anti-join against `items`, which is only meaningful because
/// `feeds` stays total over the subscription list.
///
/// The `items` scan is warmed first so the join's own `feeds` scan reads settled
/// health state: within one query DataFusion is free to drive the two scans in
/// either order, and this test is about the anti-join finding the right
/// subscription, not about that ordering. The failure fuse keeps the warmed
/// state in place — the second scan is a cache hit for both feeds.
#[tokio::test]
async fn absence_check_pattern_works() {
    let news = TestNews::start(&[("alive", "/alive.xml"), ("dead", "/dead.xml")], |_| {}).await;
    news.script("/dead.xml", FeedScript::always(Canned::status(404)));

    let warm = news.sql("SELECT feed FROM news.main.items").await;
    assert_eq!(col(&warm, "feed"), vec!["alive"]);
    let warmed_requests = news.request_count();

    let missing = news
        .sql(
            "SELECT f.name, f.last_status FROM news.main.feeds f \
             LEFT JOIN news.main.items i ON i.feed = f.name \
             WHERE i.feed IS NULL ORDER BY f.name",
        )
        .await;
    assert_eq!(
        col(&missing, "name"),
        vec!["dead"],
        "the subscription with no items is the one the anti-join returns"
    );
    assert_eq!(
        col(&missing, "last_status"),
        vec!["error"],
        "and the health table says why it has none"
    );
    assert_eq!(
        news.request_count(),
        warmed_requests,
        "the anti-join's scans were served from cache, so the state it read is the warmed one"
    );
}
