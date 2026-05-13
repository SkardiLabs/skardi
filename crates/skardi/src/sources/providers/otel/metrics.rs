//! OTEL provider metrics — observability for the OTEL consumer itself.
//!
//! Two instruments per `OtelMetrics`, sharing the global OpenTelemetry
//! meter set up by `skardi-server::telemetry::init`:
//!
//! | Name                                 | Type      | Labels                          |
//! | ------------------------------------ | --------- | ------------------------------- |
//! | `skardi_otel_queries_total`          | Counter   | `source`, `backend`, `outcome`  |
//! | `skardi_otel_query_duration_seconds` | Histogram | `source`, `backend`             |
//!
//! `outcome` is one of `success`, `upstream_error`, `parse_error`,
//! `transport_error`, `timeout`, `result_too_large`, `window_too_large`,
//! `missing_credential`, `unsupported_pushdown`, `other`. This is the
//! enum mapping from [`OtelError`] variants.
//!
//! When the server has not set a meter provider yet (typical in tests),
//! `global::meter` returns a no-op meter so calls are cheap and silent
//! — no special tests-only branch is needed.

use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{KeyValue, global};

use super::error::OtelError;

/// Counter + histogram pair scoped to a single registered OTEL source.
/// Cheap to clone (instruments are `Arc`-shared internally).
#[derive(Clone)]
pub struct OtelMetrics {
    queries_total: Counter<u64>,
    query_duration_seconds: Histogram<f64>,
    source: String,
    backend: String,
}

impl OtelMetrics {
    /// Build the instruments from the global meter. `source` and
    /// `backend` are recorded as labels on every emission.
    pub fn new(source: &str, backend: &str) -> Self {
        let meter = global::meter("skardi-otel");
        let queries_total = meter
            .u64_counter("skardi_otel_queries_total")
            .with_description("Total OTEL HTTP queries by source, backend, and outcome")
            .with_unit("{query}")
            .build();
        let query_duration_seconds = meter
            .f64_histogram("skardi_otel_query_duration_seconds")
            .with_description("OTEL upstream query latency in seconds")
            .with_unit("s")
            .build();
        Self {
            queries_total,
            query_duration_seconds,
            source: source.to_string(),
            backend: backend.to_string(),
        }
    }

    /// Record one query. `outcome` describes whether the call succeeded
    /// and, if not, the typed reason. `duration_secs` is the wall-clock
    /// time from request issue to result available (or error). Both
    /// metrics get the `source` + `backend` attributes; the counter
    /// additionally carries `outcome`.
    pub fn record(&self, outcome: Outcome, duration_secs: f64) {
        let outcome_str = outcome.as_str();
        let counter_attrs = [
            KeyValue::new("source", self.source.clone()),
            KeyValue::new("backend", self.backend.clone()),
            KeyValue::new("outcome", outcome_str),
        ];
        let histogram_attrs = [
            KeyValue::new("source", self.source.clone()),
            KeyValue::new("backend", self.backend.clone()),
        ];
        self.queries_total.add(1, &counter_attrs);
        self.query_duration_seconds
            .record(duration_secs, &histogram_attrs);
    }
}

impl std::fmt::Debug for OtelMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Skip the OpenTelemetry instruments (no useful Debug) — show
        // only the labels we tag emissions with.
        f.debug_struct("OtelMetrics")
            .field("source", &self.source)
            .field("backend", &self.backend)
            .finish_non_exhaustive()
    }
}

/// Outcome label values for `skardi_otel_queries_total`. One variant
/// per [`OtelError`] variant so dashboards can break down failures by
/// root cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Success,
    UpstreamError,
    ParseError,
    TransportError,
    Timeout,
    ResultTooLarge,
    WindowTooLarge,
    MissingCredential,
    UnsupportedPushdown,
    Other,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::UpstreamError => "upstream_error",
            Self::ParseError => "parse_error",
            Self::TransportError => "transport_error",
            Self::Timeout => "timeout",
            Self::ResultTooLarge => "result_too_large",
            Self::WindowTooLarge => "window_too_large",
            Self::MissingCredential => "missing_credential",
            Self::UnsupportedPushdown => "unsupported_pushdown",
            Self::Other => "other",
        }
    }

    /// Map an `OtelError` reference to its dashboard-facing outcome
    /// label.
    pub fn from_error(err: &OtelError) -> Self {
        match err {
            OtelError::Http { .. } => Self::TransportError,
            OtelError::UpstreamStatus { .. } => Self::UpstreamError,
            OtelError::Parse { .. } => Self::ParseError,
            OtelError::Timeout { .. } => Self::Timeout,
            OtelError::ResultTooLarge { .. } => Self::ResultTooLarge,
            OtelError::WindowTooLarge { .. } => Self::WindowTooLarge,
            OtelError::MissingCredential { .. } => Self::MissingCredential,
            OtelError::UnsupportedPushdown { .. } => Self::UnsupportedPushdown,
            OtelError::InlineCredentialRejected { .. } | OtelError::Backend { .. } => Self::Other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcome_strings_are_stable() {
        // Dashboards / Grafana saved queries depend on these label
        // strings. Pin them so a refactor of `Outcome` can't silently
        // break alerting.
        assert_eq!(Outcome::Success.as_str(), "success");
        assert_eq!(Outcome::UpstreamError.as_str(), "upstream_error");
        assert_eq!(Outcome::Timeout.as_str(), "timeout");
        assert_eq!(Outcome::ResultTooLarge.as_str(), "result_too_large");
        assert_eq!(
            Outcome::UnsupportedPushdown.as_str(),
            "unsupported_pushdown"
        );
    }

    #[test]
    fn outcome_maps_each_otel_error_variant() {
        assert_eq!(
            Outcome::from_error(&OtelError::Timeout {
                source_name: "x".to_string(),
                timeout: std::time::Duration::from_secs(1),
            }),
            Outcome::Timeout
        );
        assert_eq!(
            Outcome::from_error(&OtelError::ResultTooLarge {
                source_name: "x".to_string(),
                cap: 1,
            }),
            Outcome::ResultTooLarge
        );
        assert_eq!(
            Outcome::from_error(&OtelError::UpstreamStatus {
                source_name: "x".to_string(),
                status: 500,
                body: "boom".to_string(),
            }),
            Outcome::UpstreamError
        );
        assert_eq!(
            Outcome::from_error(&OtelError::UnsupportedPushdown {
                source_name: "x".to_string(),
                predicate: "p".to_string(),
                hint: "h".to_string(),
            }),
            Outcome::UnsupportedPushdown
        );
        assert_eq!(
            Outcome::from_error(&OtelError::Backend {
                source_name: "x".to_string(),
                message: "anything".to_string(),
            }),
            Outcome::Other
        );
    }

    #[test]
    fn record_with_no_meter_provider_does_not_panic() {
        // In tests we don't initialise a meter provider; `global::meter`
        // falls back to a no-op meter and recording is a silent
        // success.
        let m = OtelMetrics::new("prom", "prometheus");
        m.record(Outcome::Success, 0.012);
        m.record(Outcome::Timeout, 5.0);
    }
}
