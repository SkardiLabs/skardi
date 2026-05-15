//! Integration tests for the Prometheus OTEL provider.
//!
//! Stubs the upstream Prometheus HTTP API via [`wiremock`] and runs
//! end-to-end queries through a real DataFusion `SessionContext`:
//! `register_prometheus_source` → SQL → `PromMetricsTable` /
//! `prom_query` / `prom_range` → HTTP call → Arrow batches.
//!
//! Tier-1 coverage exercises the predicate-pushdown path. Tier-3
//! coverage exercises the parametric escape-hatch table functions.
//! Both pin the upstream URL with assertions so test failures point
//! at the wire-level mismatch (wrong endpoint, wrong query params)
//! rather than just "no rows".

#![cfg(feature = "otel")]

use std::collections::BTreeMap;
use std::time::Duration;

use datafusion::prelude::SessionContext;
use serde_json::json;
use url::Url;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use skardi::sources::providers::otel::OtelSourceConfig;
use skardi::sources::providers::otel::http::{OtelHttpClient, OtelHttpClientConfig, ResolvedAuth};
use skardi::sources::providers::otel::prometheus::register_prometheus_source;

/// Build a `(MockServer, SessionContext, OtelSourceConfig)` triple
/// with the Prometheus provider already registered against the mock
/// server. Tests then call `ctx.sql(...)` and assert on the resulting
/// batches.
async fn fixture() -> (MockServer, SessionContext, OtelSourceConfig) {
    let server = MockServer::start().await;
    let url = Url::parse(&server.uri()).unwrap();

    let config = OtelSourceConfig {
        backend: skardi::sources::providers::otel::OtelBackend::Prometheus,
        url: url.clone(),
        auth: Default::default(),
        extra_headers: BTreeMap::new(),
        max_result_rows: Some(50_000),
        default_window: None,
        default_step: None,
        max_window: None,
        request_timeout: Some(Duration::from_secs(5)),
    };

    let client = OtelHttpClient::new(OtelHttpClientConfig {
        source_name: "prom".to_string(),
        base_url: url,
        auth: ResolvedAuth::None,
        extra_headers: BTreeMap::new(),
        request_timeout: Duration::from_secs(5),
    })
    .unwrap();

    let ctx = SessionContext::new();
    register_prometheus_source(&ctx, "prom", &config, client).unwrap();

    (server, ctx, config)
}

/// JSON body of a successful `/api/v1/query` response carrying a
/// single-sample vector.
fn vector_response_body() -> serde_json::Value {
    json!({
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {
                    "metric": {
                        "__name__": "http_requests_total",
                        "service": "api",
                        "method": "GET"
                    },
                    "value": [1_700_000_000.5, "42"]
                },
                {
                    "metric": {
                        "__name__": "http_requests_total",
                        "service": "checkout",
                        "method": "POST"
                    },
                    "value": [1_700_000_000.5, "7"]
                }
            ]
        }
    })
}

fn matrix_response_body() -> serde_json::Value {
    json!({
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [
                {
                    "metric": {
                        "__name__": "http_requests_total",
                        "service": "api"
                    },
                    "values": [
                        [1_700_000_000.0, "10"],
                        [1_700_000_060.0, "11"],
                        [1_700_000_120.0, "13"]
                    ]
                }
            ]
        }
    })
}

#[tokio::test]
async fn tier1_metrics_table_pushes_required_name_predicate_to_query_range() {
    let (server, ctx, _cfg) = fixture().await;

    Mock::given(method("GET"))
        .and(path("/api/v1/query_range"))
        .and(query_param("query", "http_requests_total{}"))
        .respond_with(ResponseTemplate::new(200).set_body_json(matrix_response_body()))
        .expect(1)
        .mount(&server)
        .await;

    let batches = ctx
        .sql("SELECT name, value FROM metrics WHERE name = 'http_requests_total'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "matrix response should produce 3 rows");
}

#[tokio::test]
async fn tier1_metrics_table_uses_instant_query_for_ts_equality() {
    let (server, ctx, _cfg) = fixture().await;

    Mock::given(method("GET"))
        .and(path("/api/v1/query"))
        .and(query_param("query", "up{}"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vector_response_body()))
        .expect(1)
        .mount(&server)
        .await;

    let batches = ctx
        .sql(
            "SELECT name, value FROM metrics \
             WHERE name = 'up' AND ts = TIMESTAMP '2023-11-14T22:13:20Z'",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "vector response should produce 2 rows");
}

#[tokio::test]
async fn tier1_metrics_table_rejects_select_star_without_name_predicate() {
    let (_server, ctx, _cfg) = fixture().await;

    // No name predicate → translator's UnsupportedPushdown bubbles up
    // through DataFusion as an error rather than fanning out across
    // every metric.
    let result = ctx
        .sql("SELECT * FROM metrics")
        .await
        .unwrap()
        .collect()
        .await;

    let err = result.expect_err("expected UnsupportedPushdown error");
    let msg = err.to_string();
    assert!(
        msg.contains("prom_query("),
        "error should point at prom_query() escape hatch: {msg}"
    );
}

#[tokio::test]
async fn tier3_prom_query_returns_vector_results() {
    let (server, ctx, _cfg) = fixture().await;

    Mock::given(method("GET"))
        .and(path("/api/v1/query"))
        .and(query_param("query", "rate(http_requests_total[5m])"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vector_response_body()))
        .expect(1)
        .mount(&server)
        .await;

    let batches = ctx
        .sql("SELECT * FROM prom_query('rate(http_requests_total[5m])')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn tier3_prom_query_surfaces_upstream_parse_error() {
    let (server, ctx, _cfg) = fixture().await;

    let parse_error = json!({
        "status": "error",
        "errorType": "bad_data",
        "error": "invalid parameter \"query\": parse error"
    });

    Mock::given(method("GET"))
        .and(path("/api/v1/query"))
        .respond_with(ResponseTemplate::new(400).set_body_json(parse_error))
        .expect(1)
        .mount(&server)
        .await;

    let result = ctx
        .sql("SELECT * FROM prom_query('not valid {{')")
        .await
        .unwrap()
        .collect()
        .await;

    let err = result.expect_err("expected upstream parse error");
    let msg = err.to_string();
    assert!(msg.contains("prom"), "error should name the source: {msg}");
    assert!(
        msg.contains("parse error") || msg.contains("bad_data"),
        "error should surface the upstream parser message: {msg}"
    );
}

#[tokio::test]
async fn tier3_prom_query_returns_empty_batch_on_empty_result() {
    let (server, ctx, _cfg) = fixture().await;

    Mock::given(method("GET"))
        .and(path("/api/v1/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": []
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let batches = ctx
        .sql("SELECT * FROM prom_query('up{job=\"nonexistent\"}')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0, "empty result is not an error");
}

#[tokio::test]
async fn row_cap_is_enforced_during_batch_construction() {
    let server = MockServer::start().await;
    let url = Url::parse(&server.uri()).unwrap();

    let config = OtelSourceConfig {
        backend: skardi::sources::providers::otel::OtelBackend::Prometheus,
        url: url.clone(),
        auth: Default::default(),
        extra_headers: BTreeMap::new(),
        max_result_rows: Some(2),
        default_window: None,
        default_step: None,
        max_window: None,
        request_timeout: Some(Duration::from_secs(5)),
    };

    let client = OtelHttpClient::new(OtelHttpClientConfig {
        source_name: "prom".to_string(),
        base_url: url,
        auth: ResolvedAuth::None,
        extra_headers: BTreeMap::new(),
        request_timeout: Duration::from_secs(5),
    })
    .unwrap();

    let ctx = SessionContext::new();
    register_prometheus_source(&ctx, "prom", &config, client).unwrap();

    // 3-row matrix response → cap of 2 must trip.
    Mock::given(method("GET"))
        .and(path("/api/v1/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(matrix_response_body()))
        .expect(1)
        .mount(&server)
        .await;

    let result = ctx
        .sql("SELECT * FROM prom_query('http_requests_total')")
        .await
        .unwrap()
        .collect()
        .await;

    let err = result.expect_err("cap=2 with 3-row response should error");
    let msg = err.to_string();
    assert!(
        msg.contains("max_result_rows") || msg.contains("exceeded"),
        "error should mention the cap: {msg}"
    );
    assert!(msg.contains("prom"), "error should name the source: {msg}");
}

#[tokio::test]
async fn extra_headers_are_forwarded_to_upstream() {
    let server = MockServer::start().await;
    let url = Url::parse(&server.uri()).unwrap();

    let mut extra = BTreeMap::new();
    extra.insert("X-Scope-OrgID".to_string(), "tenant-a".to_string());

    let config = OtelSourceConfig {
        backend: skardi::sources::providers::otel::OtelBackend::Prometheus,
        url: url.clone(),
        auth: Default::default(),
        extra_headers: extra.clone(),
        max_result_rows: Some(50_000),
        default_window: None,
        default_step: None,
        max_window: None,
        request_timeout: Some(Duration::from_secs(5)),
    };

    let client = OtelHttpClient::new(OtelHttpClientConfig {
        source_name: "prom".to_string(),
        base_url: url,
        auth: ResolvedAuth::None,
        extra_headers: extra,
        request_timeout: Duration::from_secs(5),
    })
    .unwrap();

    let ctx = SessionContext::new();
    register_prometheus_source(&ctx, "prom", &config, client).unwrap();

    use wiremock::matchers::header;
    Mock::given(method("GET"))
        .and(path("/api/v1/query"))
        .and(header("X-Scope-OrgID", "tenant-a"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vector_response_body()))
        .expect(1)
        .mount(&server)
        .await;

    let batches = ctx
        .sql("SELECT * FROM prom_query('up')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

// ---------------------------------------------------------------------------
// Regression tests for the projection-pushdown bug — see PR #140 review.
//
// Pre-fix, `PromMetricsTable::scan` and `PromEscapeProvider::scan` ignored
// the `projection` argument while still reporting the full 4-column schema
// from `properties().schema()`. The planner then built downstream column
// references against the *projected* schema and tripped the assertion
// `Input field name <X> does not match with the projection expression <Y>`
// in `datafusion-physical-expr/src/projection.rs`.
//
// These tests pin three failure shapes the reviewer reproduced:
//   A. `SELECT labels['k'] AS …, value FROM prom_query(...)`  — column subset
//   B. tier-1 + `labels['k']` projection over `metrics`
//   C. `UNION ALL` of two `prom_query` results aliasing a literal as `name`
// ---------------------------------------------------------------------------

#[tokio::test]
async fn projection_pushdown_select_labels_extract_and_value() {
    // Repro A: tier-3 + `labels['k']` projection. The outer SELECT drops
    // `name` and `ts`, so DataFusion pushes a projection of [labels, value]
    // into prom_query's scan.
    let (server, ctx, _cfg) = fixture().await;

    Mock::given(method("GET"))
        .and(path("/api/v1/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vector_response_body()))
        .expect(1)
        .mount(&server)
        .await;

    let batches = ctx
        .sql(
            "SELECT labels['service'] AS service, value \
             FROM prom_query('rate(http_requests_total[5m])')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn projection_pushdown_subset_columns_from_metrics_table() {
    // Repro B: tier-1 — selecting a subset of columns from `metrics` must
    // not trip the projection assertion either.
    let (server, ctx, _cfg) = fixture().await;

    Mock::given(method("GET"))
        .and(path("/api/v1/query_range"))
        .and(query_param("query", "http_requests_total{}"))
        .respond_with(ResponseTemplate::new(200).set_body_json(matrix_response_body()))
        .expect(1)
        .mount(&server)
        .await;

    let batches = ctx
        .sql(
            "SELECT labels, value \
             FROM metrics \
             WHERE name = 'http_requests_total'",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
    // Projected schema must contain exactly the requested columns in order.
    let schema = batches[0].schema();
    let names: Vec<_> = schema.fields().iter().map(|f| f.name().clone()).collect();
    assert_eq!(names, vec!["labels", "value"]);
}

#[tokio::test]
async fn projection_pushdown_union_all_of_two_prom_query_results() {
    // Repro C: UNION ALL across two prom_query calls with a literal aliased
    // as `name`. Pre-fix this tripped the assertion even with no
    // `labels['k']` access at all.
    let (server, ctx, _cfg) = fixture().await;

    Mock::given(method("GET"))
        .and(path("/api/v1/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vector_response_body()))
        .expect(2)
        .mount(&server)
        .await;

    let batches = ctx
        .sql(
            "SELECT 'a' AS metric, labels, value \
                 FROM prom_query('rate(http_requests_total[5m])') \
             UNION ALL \
             SELECT 'b' AS metric, labels, value \
                 FROM prom_query('rate(http_requests_total[1m])')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4, "two 2-row vector responses unioned");
}
