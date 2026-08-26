//! Test support for the Open Connector suites: the mock gateway (a thin
//! flavor over the crate-shared mock HTTP server), envelope builders, the
//! Arrow/SQL accessors every pack suite leans on, and a tracing capture
//! for asserting on emitted events.
//!
//! The server itself lives in [`crate::util::mock_http`] — hand-rolled over
//! `tokio::net::TcpListener` so the test suite needs no mock HTTP crate,
//! speaking just enough HTTP/1.1 for `reqwest`. This module re-exports it
//! under this suite's historical names and adds the gateway-flavored
//! response constructor.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{BooleanArray, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use serde_json::Value;
use tracing::field::{Field, Visit};

use crate::sources::providers::open_connector::json_to_arrow::RowConverter;
use crate::sources::providers::open_connector::row_path::RowPath;
use crate::sources::providers::open_connector::source_pack::SourcePackTable;

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

/// Run one SQL statement to completion and return every result batch.
pub(crate) async fn collect(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect")
}

/// Convert one gateway page into a batch through the table's declared row
/// path and field mappings — the same path the scan takes, so a fixture
/// asserted through this helper vouches for the real conversion.
pub(crate) fn convert_page(table: &SourcePackTable, page: &Value) -> RecordBatch {
    let rows = RowPath::parse(table.row_path)
        .expect("row path")
        .rows(page, 1)
        .expect("row array");
    RowConverter::new(table.fields)
        .expect("converter")
        .convert(rows, 1)
        .expect("page converts")
}

/// A named column downcast to `StringArray`; panics name the missing
/// column so a schema drift reads as itself, not as a bare `unwrap`.
pub(crate) fn utf8<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column {name}"))
        .as_any()
        .downcast_ref()
        .expect("Utf8 column")
}

/// A named column downcast to `BooleanArray`.
pub(crate) fn boolean<'a>(batch: &'a RecordBatch, name: &str) -> &'a BooleanArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column {name}"))
        .as_any()
        .downcast_ref()
        .expect("Boolean column")
}

/// Every value of a Utf8 column across all result batches, in row order.
pub(crate) fn column_values(batches: &[RecordBatch], name: &str) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("column {name}"))
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8 column")
                .clone();
            (0..values.len()).map(move |i| values.value(i).to_string())
        })
        .collect()
}

/// The sorted top-level keys of one recorded executor input, for asserting
/// exactly which inputs traveled on the wire.
pub(crate) fn input_keys(input: &Value) -> Vec<&str> {
    let mut keys: Vec<&str> = input
        .as_object()
        .expect("input object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    keys
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

/// Mapped columns this walker cannot resolve under `properties` in a
/// captured contract's row-item schema. Packs pin the set explicitly so it
/// stays a conscious, reviewed fact.
///
/// **Read the result carefully — it conflates two different situations,
/// and only one of them is a gap in the fingerprint gate.**
///
/// 1. *Genuinely undeclared*: upstream leaves the field to
///    `additionalProperties` passthrough, so nothing about it is in the
///    schema and the fingerprint cannot notice it changing. A real gap;
///    only phase-4 real rows vouch for those columns. (github, slack, and
///    every feishu table, whose item schemas are declared wholly loose.)
/// 2. *Declared but unreachable by THIS walker*: the path is declared,
///    just not under a plain `properties` chain — typically inside an
///    `anyOf` branch, which this function does not descend. The
///    fingerprint hashes the whole schema including those branches, so
///    declared drift there still fails registration. Nothing is
///    unprotected; the walker is simply blind. (Every gmail `messages`
///    column; most of notion's search-backed ones.)
///
/// Teaching the walker to descend branch schemas would empty case 2 and
/// leave case 1 — but registration cannot know which branch a runtime
/// input selects, so the honest reduction is the intersection of the
/// branches' declarations. Each pack's pin comment says which case it is
/// in; do not read a non-empty list here as "outside the gate".
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
