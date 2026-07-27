//! A minimal mock Open Connector gateway for tests, plus a tracing capture
//! for asserting on emitted events.
//!
//! The gateway is hand-rolled over `tokio::net::TcpListener` so the test
//! suite needs no mock HTTP crate. It speaks just enough HTTP/1.1 for
//! `reqwest`: read the request head, consume the declared body, answer from
//! a user-supplied handler, and close the connection (which tells reqwest to
//! open a fresh one next time).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::field::{Field, Visit};

/// One request observed by the mock gateway.
#[derive(Debug, Clone)]
pub(crate) struct RecordedRequest {
    /// HTTP method (`GET`, `POST`, …).
    pub(crate) method: String,
    /// Request path including any query string.
    pub(crate) path: String,
    /// Request body as UTF-8 lossy text.
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
    body: String,
}

impl MockResponse {
    /// `200 OK` with a JSON body.
    pub(crate) fn ok(body: &str) -> Self {
        Self::new(200, body)
    }

    /// Any status with a body.
    pub(crate) fn new(status: u16, body: &str) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: body.to_string(),
        }
    }

    /// Attach an extra response header.
    pub(crate) fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }
}

/// Wrap executor output (or any `data` payload) in the gateway's uniform
/// success envelope, exactly as `POST /v1/actions/{id}` returns it.
pub(crate) fn envelope_ok(data: &str) -> String {
    format!(r#"{{"success":true,"message":"OK","data":{data},"meta":{{}}}}"#)
}

/// A failed gateway envelope with an `errorCode`, as the gateway returns
/// alongside a 4xx/5xx status.
pub(crate) fn envelope_err(error_code: &str, message: &str) -> String {
    format!(
        r#"{{"success":false,"message":"{message}","data":null,"errorCode":"{error_code}","meta":{{}}}}"#
    )
}

/// A discovery envelope (`GET /v1/actions/{{id}}`) whose `data` carries the
/// given schemas and execution block. `read_only` renders the
/// forward-compatible `execution.readOnly` field when present — today's
/// gateway omits it.
pub(crate) fn discovery_ok(
    input_schema: &str,
    output_schema: &str,
    locally_executable: bool,
    read_only: Option<bool>,
) -> String {
    let read_only = match read_only {
        Some(value) => format!(r#","readOnly":{value}"#),
        None => String::new(),
    };
    envelope_ok(&format!(
        r#"{{"inputSchema":{input_schema},"outputSchema":{output_schema},"execution":{{"locallyExecutable":{locally_executable}{read_only}}}}}"#
    ))
}

type Handler = Arc<dyn Fn(&RecordedRequest) -> MockResponse + Send + Sync>;

/// A running mock gateway. Dropping it aborts the accept loop.
pub(crate) struct MockGateway {
    /// Base URL suitable for `OpenConnectorClient` (`http://127.0.0.1:<port>`).
    pub(crate) url: String,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    accept_loop: JoinHandle<()>,
}

impl MockGateway {
    /// Start a gateway on an ephemeral localhost port. `handler` is invoked
    /// for every request and may hold state (e.g. an `AtomicUsize` counting
    /// calls to script retry sequences).
    pub(crate) async fn start(
        handler: impl Fn(&RecordedRequest) -> MockResponse + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock gateway");
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
                            tracing::debug!("mock gateway connection ended: {e}");
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

    /// All requests observed so far, in arrival order.
    pub(crate) fn requests(&self) -> Vec<RecordedRequest> {
        self.requests
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}

impl Drop for MockGateway {
    fn drop(&mut self) {
        self.accept_loop.abort();
    }
}

/// Serve one connection: one request in, one response out, then close.
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
    let reason = match response.status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Status",
    };
    let mut text = format!(
        "HTTP/1.1 {} {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n",
        response.status,
        reason,
        response.body.len()
    );
    for (name, value) in &response.headers {
        text.push_str(&format!("{name}: {value}\r\n"));
    }
    text.push_str("\r\n");
    text.push_str(&response.body);

    stream.write_all(text.as_bytes()).await?;
    stream.shutdown().await
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// One tracing event captured by [`capture_events`].
#[derive(Debug, Clone)]
pub(crate) struct CapturedEvent {
    pub(crate) level: tracing::Level,
    /// The event's format-string message (e.g. "Open Connector scan completed").
    pub(crate) message: String,
    /// Structured fields, rendered to strings.
    pub(crate) fields: HashMap<String, String>,
}

impl CapturedEvent {
    /// Look up one structured field by name.
    pub(crate) fn field(&self, name: &str) -> Option<&str> {
        self.fields.get(name).map(String::as_str)
    }
}

/// Renders an event's fields into owned strings. Typed values keep their
/// plain rendering (`true`, `3`); everything else (including `%`-Display
/// arguments and the message) comes through `record_debug`.
#[derive(Default)]
struct FieldRecorder {
    message: String,
    fields: HashMap<String, String>,
}

impl Visit for FieldRecorder {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let rendered = format!("{value:?}");
        if field.name() == "message" {
            self.message = rendered;
        } else {
            self.fields.insert(field.name().to_string(), rendered);
        }
    }
}

/// Minimal subscriber that records every event into a shared vector.
struct CaptureSubscriber {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
    next_span_id: AtomicU64,
}

impl tracing::Subscriber for CaptureSubscriber {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _attrs: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        // Span IDs must be non-zero; the counter starts at 1.
        tracing::span::Id::from_u64(self.next_span_id.fetch_add(1, Ordering::Relaxed))
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut recorder = FieldRecorder::default();
        event.record(&mut recorder);
        self.events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(CapturedEvent {
                level: *event.metadata().level(),
                message: recorder.message,
                fields: recorder.fields,
            });
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

/// Set an environment variable for the guard's lifetime and restore the
/// previous state — prior value or absence — on drop, including on panic,
/// so a failing test cannot leak its variable into tests that run later.
///
/// `set_var`/`remove_var` are unsafe because mutating the process
/// environment is not thread-safe; what keeps the tests sound is what
/// always has — per-test-unique variable names — and the guard adds
/// restore-on-drop on top, not thread safety.
pub(crate) struct EnvVarGuard {
    name: String,
    previous: Option<std::ffi::OsString>,
}

impl EnvVarGuard {
    pub(crate) fn set(name: &str, value: &str) -> Self {
        let previous = std::env::var_os(name);
        unsafe { std::env::set_var(name, value) };
        Self {
            name: name.to_string(),
            previous,
        }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => unsafe { std::env::set_var(&self.name, value) },
            None => unsafe { std::env::remove_var(&self.name) },
        }
    }
}

/// Capture every tracing event emitted on the current thread while the
/// returned guard lives. `#[tokio::test]` bodies run on a single-threaded
/// runtime, so scan events land on the test thread and parallel tests stay
/// isolated (the subscriber is a thread-local default, never global).
pub(crate) fn capture_events() -> (
    tracing::subscriber::DefaultGuard,
    Arc<Mutex<Vec<CapturedEvent>>>,
) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let subscriber = CaptureSubscriber {
        events: Arc::clone(&events),
        next_span_id: AtomicU64::new(1),
    };
    (tracing::subscriber::set_default(subscriber), events)
}
