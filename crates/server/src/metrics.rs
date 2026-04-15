use opentelemetry::{KeyValue, global, metrics::Counter, metrics::Histogram};

/// Per-pipeline OTel metrics instruments.
///
/// Instruments are created once at startup from the global meter provider
/// (which must already be set by `telemetry::init`) and then shared via
/// `AppState`.  Recording is cheap — just an atomic increment / histogram
/// update with two key-value attributes.
///
/// Exported metrics
/// ----------------
/// | Name                        | Type      | Labels                         |
/// |-----------------------------|-----------|--------------------------------|
/// | `pipeline_requests_total`   | Counter   | `pipeline`, `status`           |
/// | `pipeline_latency_ms`       | Histogram | `pipeline`                     |
///
/// `status` is `"success"` or `"error"`.  Error requests are therefore
/// queryable as:
///
/// ```text
/// rate(pipeline_requests_total{status="error"}[5m])
/// ```
///
/// and error rate as:
///
/// ```text
/// rate(pipeline_requests_total{status="error"}[5m])
///     / rate(pipeline_requests_total[5m])
/// ```
#[derive(Clone)]
pub struct PipelineMetrics {
    /// Total requests per pipeline + outcome
    pub requests_total: Counter<u64>,
    /// End-to-end handler latency in milliseconds
    pub latency_ms: Histogram<f64>,
}

impl PipelineMetrics {
    /// Build the instruments from the global meter.
    ///
    /// Call after `telemetry::init()` so the global meter provider is set.
    /// Falls back to a no-op provider if it was never initialised (e.g. in
    /// tests).
    pub fn new() -> Self {
        let meter = global::meter("skardi-server");

        let requests_total = meter
            .u64_counter("pipeline_requests_total")
            .with_description("Total pipeline execution requests by outcome")
            .with_unit("{request}")
            .build();

        let latency_ms = meter
            .f64_histogram("pipeline_latency_ms")
            .with_description("Pipeline execution latency in milliseconds")
            .with_unit("ms")
            .build();

        Self {
            requests_total,
            latency_ms,
        }
    }

    /// Record a successful pipeline execution.
    pub fn record_success(&self, pipeline: &str, latency_ms: f64) {
        let attrs = [
            KeyValue::new("pipeline", pipeline.to_string()),
            KeyValue::new("status", "success"),
        ];
        self.requests_total.add(1, &attrs);
        self.latency_ms.record(
            latency_ms,
            &[KeyValue::new("pipeline", pipeline.to_string())],
        );
    }

    /// Record a failed pipeline execution.
    pub fn record_error(&self, pipeline: &str, latency_ms: f64, error_type: &str) {
        let attrs = [
            KeyValue::new("pipeline", pipeline.to_string()),
            KeyValue::new("status", "error"),
            KeyValue::new("error_type", error_type.to_string()),
        ];
        self.requests_total.add(1, &attrs);
        self.latency_ms.record(
            latency_ms,
            &[KeyValue::new("pipeline", pipeline.to_string())],
        );
    }
}
