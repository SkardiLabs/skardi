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

/// OTel instruments for `/webhooks/:name`.
///
/// | Name                      | Type      | Labels            |
/// |---------------------------|-----------|-------------------|
/// | `webhook_requests_total`  | Counter   | `name`, `outcome` |
/// | `webhook_rows_total`      | Counter   | `name`, `field`   |
/// | `webhook_quarantined_rows_total` | Counter | `name`, `field` |
/// | `webhook_latency_ms`      | Histogram | `name`            |
///
/// `outcome` is `ok` for a committed delivery, `handshake_ok` /
/// `handshake_rejected` for the GET handshake, and otherwise the failure
/// class (`signature_rejected`, `payload_too_large`, `invalid_payload`,
/// `conversion_error`, `write_error`). Requests for a webhook name that is
/// not configured are not recorded: the name is caller-controlled and would
/// be an unbounded label.
#[derive(Clone)]
pub struct WebhookMetrics {
    /// Deliveries and handshakes per webhook + outcome
    pub requests_total: Counter<u64>,
    /// Rows appended per webhook + route field
    pub rows_total: Counter<u64>,
    /// Rows the converter rejected and the dead-letter table received
    pub quarantined_total: Counter<u64>,
    /// End-to-end handler latency in milliseconds
    pub latency_ms: Histogram<f64>,
}

impl Default for WebhookMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl WebhookMetrics {
    /// Build the instruments from the global meter (a no-op provider when
    /// telemetry was never initialised, as in tests).
    pub fn new() -> Self {
        let meter = global::meter("skardi-server");
        Self {
            requests_total: meter
                .u64_counter("webhook_requests_total")
                .with_description("Webhook deliveries and handshakes by outcome")
                .with_unit("{request}")
                .build(),
            rows_total: meter
                .u64_counter("webhook_rows_total")
                .with_description("Rows appended by webhook deliveries, per route field")
                .with_unit("{row}")
                .build(),
            quarantined_total: meter
                .u64_counter("webhook_quarantined_rows_total")
                .with_description(
                    "Rows a webhook delivery carried that the converter rejected and the dead-letter table received",
                )
                .with_unit("{row}")
                .build(),
            latency_ms: meter
                .f64_histogram("webhook_latency_ms")
                .with_description("Webhook handler latency in milliseconds")
                .with_unit("ms")
                .build(),
        }
    }

    /// Record one request's outcome and latency.
    pub fn record_request(&self, name: &str, outcome: &str, latency_ms: f64) {
        self.requests_total.add(
            1,
            &[
                KeyValue::new("name", name.to_string()),
                KeyValue::new("outcome", outcome.to_string()),
            ],
        );
        self.latency_ms
            .record(latency_ms, &[KeyValue::new("name", name.to_string())]);
    }

    /// Record rows quarantined for one route of one delivery.
    pub fn record_quarantined(&self, name: &str, field: &str, rows: u64) {
        self.quarantined_total.add(
            rows,
            &[
                KeyValue::new("name", name.to_string()),
                KeyValue::new("field", field.to_string()),
            ],
        );
    }

    /// Record rows appended for one route of one delivery.
    pub fn record_rows(&self, name: &str, field: &str, rows: u64) {
        self.rows_total.add(
            rows,
            &[
                KeyValue::new("name", name.to_string()),
                KeyValue::new("field", field.to_string()),
            ],
        );
    }
}
