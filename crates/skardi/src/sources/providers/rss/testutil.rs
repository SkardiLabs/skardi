//! A minimal mock feed server for the fetcher's tests, hand-rolled over
//! `tokio::net::TcpListener` the same way
//! `open_connector/testutil.rs`'s `MockGateway` is — that module speaks just
//! enough HTTP/1.1 for `reqwest`, and so does this one.
//!
//! It differs from the Open Connector mock in exactly the ways feed
//! responses differ from gateway envelopes: bodies are raw bytes (so a
//! future gzip fixture can be served without a text detour), every response
//! header is caller-controlled rather than a hardcoded `content-type`, a
//! response can carry an artificial delay for exercising the fetcher's
//! timeout path, and a response body can be cut off mid-transfer for
//! exercising the fetcher's body-phase retry path.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

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
    truncate_body_at: Option<usize>,
}

impl MockResponse {
    /// Any status with a raw byte body and no headers.
    pub(crate) fn new(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: body.into(),
            delay: None,
            truncate_body_at: None,
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
