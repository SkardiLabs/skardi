//! Verifies that the SQL validator (`crates/skardi/src/sources/sql_validator.rs`)
//! rejects write operations against OTEL-backed tables when they are
//! registered as `AccessMode::ReadOnly`.
//!
//! The server's `validate_pipeline_sql` (in `crates/server/src/config.rs`)
//! is responsible for putting the backend-specific table names
//! (`metrics` for Prometheus, `logs` for Loki) into the validator
//! config; this test exercises the validator directly to keep it
//! independent of the server crate. The server-side wiring is exercised
//! end-to-end by the pipeline integration tests once a `INSERT INTO
//! metrics` pipeline file is part of the configured set.

#![cfg(feature = "otel")]

use skardi::sources::sql_validator::{
    AccessMode, SqlValidationError, SqlValidatorConfig, validate_sql,
};

fn otel_config() -> SqlValidatorConfig {
    // Mirrors what `validate_pipeline_sql` builds for a `type: otel`
    // entry: bothbackend-specific table name AND the source's `name`
    // field are registered as ReadOnly.
    SqlValidatorConfig::new()
        .with_table("metrics", AccessMode::ReadOnly)
        .with_table("logs", AccessMode::ReadOnly)
        .with_table("prom", AccessMode::ReadOnly)
        .with_table("loki", AccessMode::ReadOnly)
}

#[test]
fn select_against_otel_metrics_is_permitted() {
    let cfg = otel_config();
    assert!(
        validate_sql(
            "SELECT * FROM metrics WHERE name = 'http_requests_total'",
            &cfg,
        )
        .is_ok()
    );
    assert!(validate_sql("SELECT * FROM logs WHERE labels['app'] = 'checkout'", &cfg,).is_ok());
}

#[test]
fn insert_into_metrics_is_rejected_with_named_error() {
    let cfg = otel_config();
    let err = validate_sql("INSERT INTO metrics (name, value) VALUES ('up', 1.0)", &cfg)
        .expect_err("OTEL sources are read-only; INSERT must be rejected");

    match err {
        SqlValidationError::WriteNotAllowed { operation, table } => {
            assert_eq!(operation, "INSERT");
            assert_eq!(
                table, "metrics",
                "error must name the OTEL table that was targeted"
            );
        }
        other => panic!("expected WriteNotAllowed, got {other:?}"),
    }
}

#[test]
fn update_against_otel_logs_is_rejected() {
    let cfg = otel_config();
    let err = validate_sql("UPDATE logs SET line = 'tampered' WHERE ts > 0", &cfg)
        .expect_err("OTEL logs are read-only");
    match err {
        SqlValidationError::WriteNotAllowed { operation, table } => {
            assert_eq!(operation, "UPDATE");
            assert_eq!(table, "logs");
        }
        other => panic!("expected WriteNotAllowed, got {other:?}"),
    }
}

#[test]
fn delete_from_otel_metrics_is_rejected() {
    let cfg = otel_config();
    let err = validate_sql("DELETE FROM metrics WHERE name = 'up'", &cfg)
        .expect_err("OTEL metrics are read-only");
    match err {
        SqlValidationError::WriteNotAllowed { operation, table } => {
            assert_eq!(operation, "DELETE");
            assert_eq!(table, "metrics");
        }
        other => panic!("expected WriteNotAllowed, got {other:?}"),
    }
}

#[test]
fn error_message_includes_table_name() {
    let cfg = otel_config();
    let err = validate_sql(
        "INSERT INTO metrics VALUES ('http_requests_total', NULL, 0, 1.0)",
        &cfg,
    )
    .expect_err("INSERT must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("metrics"),
        "error message should mention the rejected table: {msg}"
    );
    assert!(
        msg.contains("INSERT") || msg.contains("Write") || msg.contains("write"),
        "error message should name the rejected operation: {msg}"
    );
}
