//! Integration test for the `type: otel` ctx.yaml parsing path.
//!
//! Validates that a full `DataSource` entry with a nested `otel:` block
//! deserializes cleanly into the typed config, and that the documented
//! failure modes (missing backend, unsupported backend, inline secrets,
//! missing env var) surface clear errors per
//! `openspec/changes/add-otel-data-source/specs/otel-source/spec.md`.
//!
//! Companion to the per-module unit tests under
//! `crates/skardi/src/sources/providers/otel/`.

#![cfg(feature = "otel")]

use skardi::sources::providers::otel::OtelBackend;
use skardi_server::config::DataSource;

/// Helper: parse a single `data_sources[]` entry from a YAML string.
fn parse_source(yaml: &str) -> Result<DataSource, String> {
    serde_yaml::from_str(yaml).map_err(|e| e.to_string())
}

#[test]
fn valid_prometheus_source_parses() {
    let ds = parse_source(
        r#"
name: prom
type: otel
otel:
  backend: prometheus
  url: http://localhost:9090
"#,
    )
    .expect("valid prometheus config should parse");

    assert_eq!(ds.name, "prom");
    let otel = ds.otel.expect("otel field should be present");
    assert_eq!(otel.backend, OtelBackend::Prometheus);
    assert_eq!(otel.url.as_str(), "http://localhost:9090/");
    // Defaults should not be materialized into the struct yet (they
    // come from `*_or_default()` accessors).
    assert!(otel.max_result_rows.is_none());
}

#[test]
fn valid_loki_source_parses_with_full_config() {
    let ds = parse_source(
        r#"
name: logs
type: otel
description: "Loki logs from prod cluster"
otel:
  backend: loki
  url: http://loki:3100
  auth:
    kind: bearer
    env: LOKI_BEARER
  extra_headers:
    X-Scope-OrgID: tenant-a
  max_result_rows: 100000
  default_window: 1h
  request_timeout: 30s
"#,
    )
    .expect("valid loki config should parse");

    assert_eq!(ds.name, "logs");
    assert_eq!(
        ds.description.as_deref(),
        Some("Loki logs from prod cluster")
    );
    let otel = ds.otel.expect("otel field should be present");
    assert_eq!(otel.backend, OtelBackend::Loki);
    assert_eq!(
        otel.extra_headers.get("X-Scope-OrgID").map(String::as_str),
        Some("tenant-a")
    );
    assert_eq!(otel.max_result_rows_or_default(), 100_000);
    assert_eq!(
        otel.request_timeout_or_default(),
        std::time::Duration::from_secs(30)
    );
}

#[test]
fn missing_backend_field_is_rejected() {
    let err = parse_source(
        r#"
name: prom
type: otel
otel:
  url: http://localhost:9090
"#,
    )
    .expect_err("missing backend should fail parse");

    assert!(
        err.contains("backend") || err.contains("missing field"),
        "error message should mention backend, got: {err}"
    );
}

#[test]
fn unsupported_backend_is_rejected_with_pointer_to_supported_values() {
    let err = parse_source(
        r#"
name: traces
type: otel
otel:
  backend: tempo
  url: http://tempo:3200
"#,
    )
    .expect_err("unsupported backend should fail parse");

    // serde produces "unknown variant `tempo`, expected `prometheus` or `loki`".
    assert!(
        err.contains("tempo"),
        "should name the bad value, got: {err}"
    );
    assert!(
        err.contains("prometheus") && err.contains("loki"),
        "should list the supported backends, got: {err}"
    );
}

#[test]
fn inline_bearer_token_in_ctx_yaml_is_rejected() {
    let err = parse_source(
        r#"
name: prom
type: otel
otel:
  backend: prometheus
  url: http://localhost:9090
  auth:
    kind: bearer
    token: "leaked-into-yaml"
"#,
    )
    .expect_err("inline token should be rejected at deserialize");

    assert!(
        err.contains("inline secrets are not permitted"),
        "error must explicitly mention inline secrets, got: {err}"
    );
    assert!(
        err.contains("env"),
        "error must point users at the `env:` form, got: {err}"
    );
}

#[test]
fn inline_password_in_basic_auth_is_rejected() {
    let err = parse_source(
        r#"
name: prom
type: otel
otel:
  backend: prometheus
  url: http://localhost:9090
  auth:
    kind: basic
    username_env: PROM_USER
    password: "secret"
"#,
    )
    .expect_err("inline password should be rejected at deserialize");

    assert!(
        err.contains("inline secrets are not permitted"),
        "got: {err}"
    );
    assert!(err.contains("password_env"), "got: {err}");
}

#[test]
fn missing_env_var_surfaces_at_load_credentials_time() {
    let var = "SKARDI_OTEL_INTEGRATION_MISSING_VAR";
    // SAFETY: single-threaded test scope; we own this uniquely-named var.
    unsafe { std::env::remove_var(var) };

    let ds = parse_source(&format!(
        r#"
name: prom
type: otel
otel:
  backend: prometheus
  url: http://localhost:9090
  auth:
    kind: bearer
    env: {var}
"#,
    ))
    .expect("config itself parses fine — env-var resolution happens later");

    let otel = ds.otel.expect("otel field present");
    let err = otel
        .load_credentials("prom")
        .expect_err("missing env var should error");

    // The error message must name both the source and the variable so
    // an operator can locate and fix the misconfiguration without
    // chasing through logs.
    let msg = err.to_string();
    assert!(msg.contains("prom"), "should name the source: {msg}");
    assert!(msg.contains(var), "should name the missing var: {msg}");
}

#[test]
fn empty_env_var_is_treated_as_missing() {
    let var = "SKARDI_OTEL_INTEGRATION_EMPTY_VAR";
    // SAFETY: single-threaded test scope.
    unsafe { std::env::set_var(var, "") };

    let ds = parse_source(&format!(
        r#"
name: prom
type: otel
otel:
  backend: prometheus
  url: http://localhost:9090
  auth:
    kind: bearer
    env: {var}
"#,
    ))
    .unwrap();

    let err = ds
        .otel
        .unwrap()
        .load_credentials("prom")
        .expect_err("empty env var should be treated as missing");
    assert!(err.to_string().contains(var));

    // SAFETY: clean up after ourselves so re-runs are deterministic.
    unsafe { std::env::remove_var(var) };
}
