//! A minimal mock HTTP/1.1 server for the in-crate reqwest clients' test
//! suites, hand-rolled over `tokio::net::TcpListener` so those suites need
//! no mock HTTP crate.
//!
//! It speaks just enough HTTP/1.1 for `reqwest`: read the request head,
//! consume the declared body, answer from a user-supplied handler, and close
//! the connection (which tells reqwest to open a fresh one next time). One
//! request per connection is a feature, not a shortcut: a second logical
//! request means a fresh TCP connection, and therefore — in the rss
//! fetcher's case — a fresh egress check on `PolicyDns`'s side.
//!
//! Two suites drive it, through re-exports that keep their provider-local
//! names: `open_connector/testutil.rs` (`MockGateway`) and `rss/testutil.rs`
//! (`MockFeedServer`). Each adds its own response sugar next to those
//! re-exports (`ok` for gateway envelopes, `xml` for feed documents); the
//! transport below is flavor-free. No response header is ever injected
//! automatically: the rss fetcher's tests need to control `etag`,
//! `last-modified`, `location`, `retry-after`, and `content-encoding`
//! precisely — including their absence — and nothing in the Open Connector
//! client reads a response `content-type` (bodies are parsed as JSON
//! regardless), so a default would serve no one.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// One request observed by the mock server.
#[derive(Debug, Clone)]
pub(crate) struct RecordedRequest {
    /// HTTP method (`GET`, `POST`, …).
    pub(crate) method: String,
    /// Request path including any query string.
    pub(crate) path: String,
    /// Request body as UTF-8 lossy text (empty for the GETs the rss
    /// fetcher sends; the gateway suites assert on their POST payloads).
    pub(crate) body: String,
    headers: HashMap<String, String>,
}

impl RecordedRequest {
    /// Look up a header by name (case-insensitive).
    pub(crate) fn header(&self, name: &str) -> Option<String> {
        self.headers.get(&name.to_ascii_lowercase()).cloned()
    }
}

/// A canned response the handler returns for a request.
pub(crate) struct MockResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    delay: Option<Duration>,
    truncate_body_at: Option<usize>,
}

impl MockResponse {
    /// Any status with a raw byte body and no headers. Bodies are bytes
    /// rather than text so a gzip fixture can be served without a detour.
    pub(crate) fn new(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: body.into(),
            delay: None,
            truncate_body_at: None,
        }
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
    /// for a client's timeout tests.
    pub(crate) fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    /// Advertise the full body's length in `content-length` but send only
    /// its first `sent_bytes` bytes before closing the connection —
    /// simulating an upstream that dies mid-transfer. The declared/actual
    /// length mismatch is what makes the client see a body-read error
    /// rather than mistaking the prefix for a short-but-complete response.
    pub(crate) fn with_truncated_body(mut self, sent_bytes: usize) -> Self {
        self.truncate_body_at = Some(sent_bytes);
        self
    }
}

type Handler = Arc<dyn Fn(&RecordedRequest) -> MockResponse + Send + Sync>;

/// A running mock server. Dropping it aborts the accept loop.
///
/// The base URL is reachable both as the `url` field and through
/// [`MockHttpServer::url`]: the two consolidated suites grew the two styles
/// independently, and rewriting either's call sites would be churn for no
/// behavior.
pub(crate) struct MockHttpServer {
    /// Base URL (`http://127.0.0.1:<port>`, no trailing slash).
    pub(crate) url: String,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    accept_loop: JoinHandle<()>,
}

impl MockHttpServer {
    /// Start a server on an ephemeral localhost port. `handler` is invoked
    /// for every request and may hold state (e.g. an `AtomicUsize` counting
    /// calls to script a retry sequence).
    pub(crate) async fn start(
        handler: impl Fn(&RecordedRequest) -> MockResponse + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock http server");
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
                            tracing::debug!("mock http server connection ended: {e}");
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

impl Drop for MockHttpServer {
    fn drop(&mut self) {
        self.accept_loop.abort();
    }
}

/// Serve one connection: one request in, one (optionally delayed) response
/// out, then close.
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
    // Draining whatever the client sent keeps the connection well-behaved
    // instead of leaving unread bytes ahead of the response.
    while buf.len() < head_len + content_length {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    let body = String::from_utf8_lossy(&buf[head_len..buf.len().min(head_len + content_length)])
        .to_string();

    let request = RecordedRequest {
        method,
        path,
        body,
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
    // `content-length` above always declares the full body; sending less
    // (see `MockResponse::with_truncated_body`) is deliberate, so the
    // client observes a mid-body connection loss instead of a complete
    // response.
    let sent = match response.truncate_body_at {
        Some(n) => &response.body[..n.min(response.body.len())],
        None => &response.body[..],
    };
    stream.write_all(sent).await?;
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
    /// server recorded, before any client suite relies on it.
    #[tokio::test]
    async fn smoke_test_serves_recorded_responses_and_records_requests() {
        let server = MockHttpServer::start(|_req| {
            MockResponse::new(200, "<x/>")
                .with_header("content-type", "application/xml")
                .with_header("etag", "\"v1\"")
        })
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

    /// A bare status is served as such, and a scripted delay holds the
    /// response back for at least its duration. Also what keeps [`MockResponse::status`]
    /// and [`MockResponse::with_delay`] exercised in a test build without the
    /// `rss` feature, whose suites are otherwise their only callers.
    #[tokio::test]
    async fn bare_status_and_delay_are_served_as_scripted() {
        let server = MockHttpServer::start(|_req| {
            MockResponse::status(304).with_delay(Duration::from_millis(50))
        })
        .await;

        let started = std::time::Instant::now();
        let resp = reqwest::Client::new()
            .get(format!("{}/probe", server.url()))
            .send()
            .await
            .expect("send request");
        assert!(
            started.elapsed() >= Duration::from_millis(50),
            "the scripted delay held the response back"
        );
        assert_eq!(resp.status(), 304);
        assert_eq!(resp.content_length(), Some(0));
    }

    /// A truncated body declares its full length but dies mid-transfer, so
    /// the client sees a body-read error, not a short complete response.
    /// Also the non-`rss` test build's only caller of
    /// [`MockResponse::with_truncated_body`].
    #[tokio::test]
    async fn truncated_body_declares_full_length_but_dies_mid_transfer() {
        let server = MockHttpServer::start(|_req| {
            MockResponse::new(200, "0123456789").with_truncated_body(4)
        })
        .await;

        let resp = reqwest::Client::new()
            .get(format!("{}/cut", server.url()))
            .send()
            .await
            .expect("the head and status arrive intact");
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.content_length(), Some(10));
        resp.bytes()
            .await
            .expect_err("the body dies before the declared length");
    }
}
