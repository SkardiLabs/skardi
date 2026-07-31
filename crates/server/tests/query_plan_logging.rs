//! Regression guard for the confidentiality claim behind `POST /query`:
//! literal values from a statement must not reach the tracing/OTLP stream,
//! even with DEBUG logging turned on.
//!
//! Suppressing the handler's own `sql` field is not sufficient — DataFusion
//! re-prints the same literals inside the plans its analyzer/optimizer log at
//! DEBUG (`Projection: Utf8("…")`) and inside the datafusion-tracing span
//! fields (`ProjectionExec: expr=[…]`). These tests run a real query through
//! the server's own instrumented `SessionState`, capture everything the
//! subscriber emits, and assert a sentinel literal never appears.

use std::io;
use std::sync::{Arc, Mutex};

use datafusion::prelude::SessionContext;
use skardi_server::logging::build_env_filter;
use skardi_server::server::instrument_session_state;
use tracing_subscriber::layer::SubscriberExt;

/// Value inlined into the statement. Must never show up in captured output.
const SENTINEL: &str = "TOP_SECRET_PR173";

/// Shared buffer behind a `MakeWriter`, so the test can read back everything
/// the fmt layer formatted.
#[derive(Clone, Default)]
struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

impl CaptureWriter {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

impl io::Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
    type Writer = CaptureWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Run `SELECT '<SENTINEL>' AS secret` through the server's instrumented
/// session state under `RUST_LOG=rust_log`, returning everything the
/// subscriber emitted.
///
/// Deliberately synchronous with a current-thread runtime: `with_default`
/// installs the dispatcher per-thread, and a current-thread runtime keeps
/// every poll (including DataFusion's spawned execution tasks) on it.
fn capture_query_logs(rust_log: &str, allow_plan_value_logging: bool) -> String {
    let capture = CaptureWriter::default();
    let subscriber = tracing_subscriber::registry()
        .with(build_env_filter(Some(rust_log), allow_plan_value_logging))
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(capture.clone())
                .with_ansi(false)
                // Span fields (where the plan node displays live) are only
                // formatted when span lifecycle events are emitted.
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL),
        );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    tracing::subscriber::with_default(subscriber, || {
        runtime.block_on(async {
            let state = instrument_session_state(
                datafusion_federation::default_session_state(),
                Vec::new(),
            );
            let ctx = SessionContext::new_with_state(state);
            let batches = ctx
                .sql(&format!("SELECT '{SENTINEL}' AS secret"))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        });
    });

    capture.contents()
}

#[test]
fn debug_logging_does_not_emit_query_literals() {
    let logs = capture_query_logs("debug", false);
    assert!(
        !logs.contains(SENTINEL),
        "RUST_LOG=debug leaked a query literal into the trace stream:\n{logs}"
    );
}

#[test]
fn trace_logging_does_not_emit_query_literals() {
    let logs = capture_query_logs("trace", false);
    assert!(
        !logs.contains(SENTINEL),
        "RUST_LOG=trace leaked a query literal into the trace stream:\n{logs}"
    );
}

#[test]
fn targeted_datafusion_debug_does_not_emit_query_literals() {
    // The floor must survive an operator explicitly naming the plan-printing
    // targets, not just a blanket `debug`.
    let logs = capture_query_logs(
        "info,datafusion=trace,datafusion_optimizer=debug,skardi_query_plan=trace",
        false,
    );
    assert!(
        !logs.contains(SENTINEL),
        "targeted datafusion debug directives leaked a query literal:\n{logs}"
    );
}

#[test]
fn opt_in_env_var_restores_plan_logging() {
    // Positive control. Without this the tests above would pass just as well
    // if the capture harness recorded nothing at all — this proves the
    // sentinel *is* reachable, and that the operator escape hatch works.
    let logs = capture_query_logs("debug", true);
    assert!(
        logs.contains(SENTINEL),
        "expected plan values with the opt-in enabled; capture harness may be \
         broken:\n{logs}"
    );
}
