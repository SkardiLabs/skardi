//! Test support for the Open Connector suites: the mock gateway (a thin
//! flavor over the crate-shared mock HTTP server), envelope builders, and a
//! tracing capture for asserting on emitted events.
//!
//! The server itself lives in [`crate::util::mock_http`] — hand-rolled over
//! `tokio::net::TcpListener` so the test suite needs no mock HTTP crate,
//! speaking just enough HTTP/1.1 for `reqwest`. This module re-exports it
//! under this suite's historical names and adds the gateway-flavored
//! response constructor.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};

pub(crate) use crate::util::mock_http::{
    MockHttpServer as MockGateway, MockResponse, RecordedRequest,
};

impl MockResponse {
    /// `200 OK` with a JSON body.
    ///
    /// No `content-type` header travels with it (the shared server injects
    /// none, and this constructor adds none): nothing in
    /// `OpenConnectorClient` reads a response content type — bodies are
    /// parsed as JSON regardless — so declaring one would pin a header no
    /// test observes.
    pub(crate) fn ok(body: &str) -> Self {
        Self::new(200, body)
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

/// Mapped columns whose dotted path is NOT declared in a captured
/// contract's row-item schema — the subset the fingerprint gate cannot
/// protect, because upstream leaves those fields to `additionalProperties`
/// passthrough. Packs pin this set explicitly so the coverage gap is a
/// conscious, reviewed fact rather than an implicit one.
pub(crate) fn fingerprint_uncovered_columns(
    contract: &str,
    row_path: &str,
    fields: &[crate::sources::providers::open_connector::json_to_arrow::FieldMapping],
) -> Vec<&'static str> {
    fn descend<'a>(mut node: &'a serde_json::Value, path: &str) -> &'a serde_json::Value {
        for segment in path.split('.') {
            node = &node["properties"][segment];
        }
        node
    }
    let contract: serde_json::Value = serde_json::from_str(contract).expect("contract parses");
    let items = &descend(&contract, row_path.strip_prefix("$.").expect("row path"))["items"];
    fields
        .iter()
        .filter(|field| descend(items, field.path).is_null())
        .map(|field| field.name)
        .collect()
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
