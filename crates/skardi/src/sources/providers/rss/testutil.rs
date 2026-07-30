//! A minimal mock feed server for the fetcher's tests, hand-rolled over
//! `tokio::net::TcpListener` the same way
//! `open_connector/testutil.rs`'s `MockGateway` is — that module speaks just
//! enough HTTP/1.1 for `reqwest`, and so does this one.
//!
//! It differs from the Open Connector mock in exactly the ways feed
//! responses differ from gateway envelopes: bodies are raw bytes (so a
//! future gzip fixture can be served without a text detour), every response
//! header is caller-controlled rather than a hardcoded `content-type`, and a
//! response can carry an artificial delay for exercising the fetcher's
//! timeout path.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Array, RecordBatch, StringArray};
use datafusion::error::Result as DFResult;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use super::ResolvedSubscription;
use super::cache::MemoryFeedCache;
use super::config::{FeedSubscription, RssConfig, inline_config};
use super::egress::EgressPolicy;
use super::engine::{CACHE_MAX_BYTES, RssEngine};
use super::fetch::FeedFetcher;

/// A well-formed RSS 2.0 document carrying every `channel`-level field the
/// dialect requires plus exactly one item, so a batch built from it has one
/// row and `conformance_notes` comes out empty. The baseline body for the
/// engine's tests; a test that needs a *defect* spells the defect out inline
/// rather than editing this.
pub(crate) const RSS2_MINIMAL: &str = concat!(
    r#"<rss version="2.0"><channel>"#,
    r#"<title>Minimal Feed</title>"#,
    r#"<link>https://feed.example/</link>"#,
    r#"<description>A minimal feed.</description>"#,
    r#"<item><guid>https://feed.example/1</guid><title>First post</title>"#,
    r#"<link>https://feed.example/1</link></item>"#,
    r#"</channel></rss>"#,
);

/// The `Utf8` column named `name`, or a panic naming the column — every
/// caller is a test asserting against a fixed schema, so a missing or
/// retyped column is a bug in the test's expectations either way.
fn utf8_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
    let index = batch
        .schema()
        .index_of(name)
        .unwrap_or_else(|e| panic!("batch has no column {name:?}: {e}"));
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("column {name:?} is not Utf8"))
}

/// Read a `Utf8` column as owned strings, panicking on a NULL — for the
/// columns the schema declares non-nullable (`window_status`,
/// `last_status`, `feed`, …).
pub(crate) fn str_col(batch: &RecordBatch, name: &str) -> Vec<String> {
    let column = utf8_column(batch, name);
    (0..column.len())
        .map(|row| {
            assert!(
                column.is_valid(row),
                "column {name:?} row {row} is NULL but the schema declares it non-nullable"
            );
            column.value(row).to_string()
        })
        .collect()
}

/// Read a nullable `Utf8` column, preserving NULLs — for `last_error`,
/// `dialect_declared`, `conformance_notes`, and friends, where the
/// difference between NULL and `""` is the assertion.
pub(crate) fn str_opt_col(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
    let column = utf8_column(batch, name);
    (0..column.len())
        .map(|row| {
            if column.is_valid(row) {
                Some(column.value(row).to_string())
            } else {
                None
            }
        })
        .collect()
}

/// `(name, url)` pairs pointing at `server`, from `(name, path)` pairs — the
/// subscription list shape [`test_engine`] takes.
pub(crate) fn feed_urls(server: &MockFeedServer, feeds: &[(&str, &str)]) -> Vec<(String, String)> {
    feeds
        .iter()
        .map(|(name, path)| ((*name).to_string(), format!("{}{path}", server.url())))
        .collect()
}

/// An [`RssEngine`] over `feeds` (`(name, url)` pairs, e.g. from
/// [`feed_urls`]), with `tune` applied to the spec-default config before the
/// engine is assembled — the seam for `ttl_seconds`, `max_concurrent`, and
/// `scan_timeout_seconds`.
///
/// The fetcher is pointed at the loopback-allowing egress policy so the
/// subscriptions can name [`MockFeedServer`]. `request_timeout_seconds` is
/// pulled down from the spec default (30) to 5 so a test that means to hit a
/// *different* bound does not first have to wait out a request timeout.
///
/// `engine.rs`'s own tests have a near-identical private helper. It cannot be
/// shared in either direction: it lives inside that module's `#[cfg(test)] mod
/// tests`, which nothing outside the module can name. This copy is the one the
/// exec layer's tests build on.
pub(crate) fn test_engine(
    feeds: &[(String, String)],
    tune: impl FnOnce(&mut RssConfig),
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
    config.request_timeout_seconds = 5;
    tune(&mut config);
    let fetcher = FeedFetcher::new(
        Arc::new(EgressPolicy::allowing_loopback_for_tests()),
        Duration::from_secs(config.request_timeout_seconds),
        config.max_response_bytes,
        config.user_agent.clone(),
    )
    .expect("build the test fetcher");
    let cache = Arc::new(MemoryFeedCache::new(
        CACHE_MAX_BYTES,
        subscriptions.len() + 8,
    ));
    RssEngine::with_parts(
        "rss_test".to_string(),
        subscriptions,
        &config,
        fetcher,
        cache,
    )
}

/// Drain one partition's stream to its batches.
///
/// Panics on an `Err` item rather than returning it: no `rss` partition may
/// ever surface an error — a dead or slow feed degrades to zero rows — so an
/// error here is a failed assertion, and the panic message says which.
pub(crate) async fn collect_stream(
    stream: DFResult<SendableRecordBatchStream>,
) -> Vec<RecordBatch> {
    let mut stream = stream.expect("execute returned a stream");
    let mut batches = Vec::new();
    while let Some(item) = stream.next().await {
        batches.push(item.expect("an rss partition must never yield an error"));
    }
    batches
}

/// Total rows across a partition's batches.
pub(crate) fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// One request observed by the mock server.
#[derive(Debug, Clone)]
pub(crate) struct RecordedRequest {
    /// HTTP method (`GET`, …).
    pub(crate) method: String,
    /// Request path including any query string.
    pub(crate) path: String,
    headers: HashMap<String, String>,
}

impl RecordedRequest {
    /// Look up a header by name (case-insensitive).
    pub(crate) fn header(&self, name: &str) -> Option<String> {
        self.headers.get(&name.to_ascii_lowercase()).cloned()
    }
}

/// A canned response the handler returns for a request.
///
/// Unlike `open_connector/testutil.rs`'s `MockResponse`, headers are never
/// injected automatically (no default `content-type`): the fetcher's tests
/// need to control `etag`, `last-modified`, `location`, `retry-after`, and
/// `content-encoding` precisely, including their absence.
pub(crate) struct MockResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    delay: Option<Duration>,
}

impl MockResponse {
    /// Any status with a raw byte body and no headers.
    pub(crate) fn new(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: body.into(),
            delay: None,
        }
    }

    /// `200 OK`, `content-type: application/xml`, UTF-8 body — the common
    /// case for a well-formed feed response.
    pub(crate) fn xml(body: &str) -> Self {
        Self::new(200, body.as_bytes().to_vec()).with_header("content-type", "application/xml")
    }

    /// A bare status with an empty body and no headers — the starting point
    /// for redirects (chain `.with_header("location", …)`), `304`, and
    /// 4xx/5xx responses.
    pub(crate) fn status(status: u16) -> Self {
        Self::new(status, Vec::new())
    }

    /// Attach an extra response header.
    pub(crate) fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    /// Delay writing the response by `delay`, simulating a slow upstream
    /// for the fetcher's timeout tests.
    pub(crate) fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }
}

type Handler = Arc<dyn Fn(&RecordedRequest) -> MockResponse + Send + Sync>;

/// A running mock feed server. Dropping it aborts the accept loop.
pub(crate) struct MockFeedServer {
    url: String,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    accept_loop: JoinHandle<()>,
}

impl MockFeedServer {
    /// Start a server on an ephemeral localhost port. `handler` is invoked
    /// for every request and may hold state (e.g. an `AtomicUsize` counting
    /// calls to script a retry sequence).
    pub(crate) async fn start(
        handler: impl Fn(&RecordedRequest) -> MockResponse + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock feed server");
        let port = listener.local_addr().expect("local addr").port();
        let requests: Arc<Mutex<Vec<RecordedRequest>>> = Arc::new(Mutex::new(Vec::new()));

        let accept_loop = {
            let requests = Arc::clone(&requests);
            let handler: Handler = Arc::new(handler);
            tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    let requests = Arc::clone(&requests);
                    let handler = Arc::clone(&handler);
                    tokio::spawn(async move {
                        if let Err(e) = serve_connection(stream, handler, requests).await {
                            tracing::debug!("mock feed server connection ended: {e}");
                        }
                    });
                }
            })
        };

        Self {
            url: format!("http://127.0.0.1:{port}"),
            requests,
            accept_loop,
        }
    }

    /// Base URL for this server (`http://127.0.0.1:<port>`, no trailing
    /// slash).
    pub(crate) fn url(&self) -> String {
        self.url.clone()
    }

    /// All requests observed so far, in arrival order.
    pub(crate) fn requests(&self) -> Vec<RecordedRequest> {
        self.requests
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}

impl Drop for MockFeedServer {
    fn drop(&mut self) {
        self.accept_loop.abort();
    }
}

/// Serve one connection: one request in, one (optionally delayed) response
/// out, then close — same one-request-per-connection shape as
/// `open_connector/testutil.rs`, so a fresh TCP connection (and thus a fresh
/// egress check on `PolicyDns`'s side, in the fetcher's case) is exactly
/// what a second logical request means.
async fn serve_connection(
    mut stream: tokio::net::TcpStream,
    handler: Handler,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
) -> std::io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let mut tmp = [0u8; 4096];

    // Read until the end of the request head.
    let head_len = loop {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos + 4;
        }
        if buf.len() > 64 * 1024 {
            return Ok(()); // absurd head; give up on this connection
        }
    };

    let head = String::from_utf8_lossy(&buf[..head_len]).to_string();
    let mut lines = head.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();

    let mut headers = HashMap::new();
    let mut content_length = 0usize;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim().to_ascii_lowercase();
            let value = value.trim().to_string();
            if name == "content-length" {
                content_length = value.parse().unwrap_or(0);
            }
            headers.insert(name, value);
        }
    }

    // Consume the declared body, reading more if it hasn't arrived yet.
    // Every fetch this module serves is a GET with no body, but draining
    // whatever a client did send keeps the connection well-behaved instead
    // of leaving unread bytes ahead of the response.
    while buf.len() < head_len + content_length {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }

    let request = RecordedRequest {
        method,
        path,
        headers,
    };
    requests
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .push(request.clone());

    let response = handler(&request);
    if let Some(delay) = response.delay {
        tokio::time::sleep(delay).await;
    }

    let reason = reason_phrase(response.status);
    let mut head_out = format!(
        "HTTP/1.1 {} {}\r\ncontent-length: {}\r\nconnection: close\r\n",
        response.status,
        reason,
        response.body.len()
    );
    for (name, value) in &response.headers {
        head_out.push_str(&format!("{name}: {value}\r\n"));
    }
    head_out.push_str("\r\n");

    stream.write_all(head_out.as_bytes()).await?;
    stream.write_all(&response.body).await?;
    stream.shutdown().await
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        206 => "Partial Content",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        304 => "Not Modified",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        404 => "Not Found",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Status",
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Smoke test for the harness itself: a real `reqwest` client against a
    /// running server, checking the response body/headers and what the
    /// server recorded, before anything in `fetch.rs` relies on it.
    #[tokio::test]
    async fn smoke_test_serves_recorded_responses_and_records_requests() {
        let server =
            MockFeedServer::start(|_req| MockResponse::xml("<x/>").with_header("etag", "\"v1\""))
                .await;

        let client = reqwest::Client::builder()
            .user_agent("skardi-test")
            .build()
            .expect("build reqwest client");
        let resp = client
            .get(format!("{}/feed.xml", server.url()))
            .send()
            .await
            .expect("send request");

        assert_eq!(resp.status(), 200);
        assert_eq!(resp.headers().get("etag").unwrap(), "\"v1\"");
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "application/xml"
        );
        let body = resp.bytes().await.expect("read body");
        assert_eq!(&body[..], b"<x/>");

        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "GET");
        assert_eq!(requests[0].path, "/feed.xml");
        assert_eq!(
            requests[0].header("user-agent").as_deref(),
            Some("skardi-test")
        );
    }
}
