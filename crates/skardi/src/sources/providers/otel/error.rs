//! Error type for the OTEL provider.
//!
//! `OtelError` is the single error type returned from every OTEL backend
//! call (HTTP, parsing, guardrail checks, translator failures). It
//! converts losslessly into `DataFusionError::External` so the engine
//! sees a typed error chain in `cause()` while the message preserves the
//! source name for operators reading server logs.
//!
//! See `openspec/changes/add-otel-data-source/design.md` Decision 4 for
//! the `UnsupportedPushdown` contract.

use std::time::Duration;

use datafusion::error::DataFusionError;
use thiserror::Error;

/// All errors surfaced by the OTEL provider.
#[derive(Debug, Error)]
pub enum OtelError {
    /// Transport-level HTTP failure (DNS, TLS, connection reset).
    #[error("otel source `{source_name}`: http transport error: {source}")]
    Http {
        source_name: String,
        #[source]
        source: reqwest::Error,
    },

    /// Upstream returned a non-2xx HTTP status.
    #[error(
        "otel source `{source_name}`: upstream returned HTTP {status}: {}",
        truncate(body, 200)
    )]
    UpstreamStatus {
        source_name: String,
        status: u16,
        body: String,
    },

    /// Response parsing failed (malformed JSON, unexpected shape, etc.).
    #[error("otel source `{source_name}`: failed to parse upstream response: {message}")]
    Parse {
        source_name: String,
        message: String,
    },

    /// Per-query result-size cap exceeded.
    #[error(
        "otel source `{source_name}`: query result exceeded `max_result_rows` ({cap}); \
         tighten the query or raise the cap in ctx.yaml"
    )]
    ResultTooLarge { source_name: String, cap: usize },

    /// Range-query window exceeded the configured `max_window`.
    #[error(
        "otel source `{source_name}`: requested window {requested:?} exceeds `max_window` ({max:?})"
    )]
    WindowTooLarge {
        source_name: String,
        requested: Duration,
        max: Duration,
    },

    /// A required credential env var was unset or empty at registration.
    #[error("otel source `{source_name}`: required env var `{var}` is unset or empty")]
    MissingCredential { source_name: String, var: String },

    /// An inline credential appeared in ctx.yaml — rejected at config load.
    #[error(
        "otel source `{source_name}`: inline secret in field `{field}` is not permitted; \
         use `env:` to reference an environment variable instead"
    )]
    InlineCredentialRejected { source_name: String, field: String },

    /// Request exceeded `request_timeout`.
    #[error("otel source `{source_name}`: request exceeded timeout of {timeout:?}")]
    Timeout {
        source_name: String,
        timeout: Duration,
    },

    /// The translator does not recognize this predicate shape; caller
    /// should use the `prom_query` / `loki_query` escape hatch.
    #[error(
        "otel source `{source_name}`: cannot push down `{predicate}` into the backend query — \
         {hint}"
    )]
    UnsupportedPushdown {
        source_name: String,
        predicate: String,
        hint: String,
    },

    /// Catch-all for backend-specific errors that don't fit the shapes above.
    #[error("otel source `{source_name}`: {message}")]
    Backend {
        source_name: String,
        message: String,
    },
}

impl From<OtelError> for DataFusionError {
    fn from(err: OtelError) -> Self {
        DataFusionError::External(Box::new(err))
    }
}

/// Truncate a string for inclusion in error messages, appending a marker
/// when truncation happened. Operates on chars (not bytes) so we never
/// split a UTF-8 sequence mid-byte.
fn truncate(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let head: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{head}... [truncated]")
    } else {
        head
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_pushdown_message_quotes_predicate_and_includes_hint() {
        let err = OtelError::UnsupportedPushdown {
            source_name: "prom".to_string(),
            predicate: "value > 0.5".to_string(),
            hint: "use prom_query('http_requests_total > 0.5') instead".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("`prom`"));
        assert!(msg.contains("`value > 0.5`"));
        assert!(msg.contains("prom_query("));
    }

    #[test]
    fn upstream_status_truncates_long_bodies() {
        let body = "x".repeat(500);
        let err = OtelError::UpstreamStatus {
            source_name: "prom".to_string(),
            status: 500,
            body,
        };
        let msg = err.to_string();
        assert!(msg.contains("HTTP 500"));
        assert!(msg.contains("truncated"));
    }

    #[test]
    fn upstream_status_leaves_short_bodies_intact() {
        let err = OtelError::UpstreamStatus {
            source_name: "prom".to_string(),
            status: 400,
            body: "parse error".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("HTTP 400"));
        assert!(msg.contains("parse error"));
        assert!(!msg.contains("truncated"));
    }

    #[test]
    fn truncate_handles_multi_byte_chars_safely() {
        let s = "日本語の文字列".repeat(50);
        let _ = truncate(&s, 10);
    }

    #[test]
    fn converts_to_datafusion_external_error() {
        let err = OtelError::MissingCredential {
            source_name: "prom".to_string(),
            var: "PROMETHEUS_BEARER".to_string(),
        };
        let df_err: DataFusionError = err.into();
        let msg = df_err.to_string();
        assert!(msg.contains("PROMETHEUS_BEARER"));
        assert!(msg.contains("prom"));
    }
}
