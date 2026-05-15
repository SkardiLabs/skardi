//! Integration tests for the Loki OTEL provider.
//!
//! Stubs Loki's HTTP API via [`wiremock`] and exercises the full path
//! through DataFusion: `register_loki_source` → SQL → `LokiLogsTable`
//! / `loki_query` / `loki_range` → HTTP → Arrow batches.
//!
//! Companion to `otel_prometheus.rs`. Same architecture, different
//! endpoints and response shapes.

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
use skardi::sources::providers::otel::loki::register_loki_source;

async fn fixture() -> (MockServer, SessionContext, OtelSourceConfig) {
    let server = MockServer::start().await;
    let url = Url::parse(&server.uri()).unwrap();

    let config = OtelSourceConfig {
        backend: skardi::sources::providers::otel::OtelBackend::Loki,
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
        source_name: "loki".to_string(),
        base_url: url,
        auth: ResolvedAuth::None,
        extra_headers: BTreeMap::new(),
        request_timeout: Duration::from_secs(5),
    })
    .unwrap();

    let ctx = SessionContext::new();
    register_loki_source(&ctx, "loki", &config, client).unwrap();

    (server, ctx, config)
}

fn streams_response_body() -> serde_json::Value {
    json!({
        "status": "success",
        "data": {
            "resultType": "streams",
            "result": [
                {
                    "stream": {"app": "checkout", "level": "error"},
                    "values": [
                        ["1700000000000000000", "checkout failed: timeout"],
                        ["1700000000123456789", "retry exhausted"]
                    ]
                },
                {
                    "stream": {"app": "checkout", "level": "warn"},
                    "values": [
                        ["1700000000200000000", "slow downstream"]
                    ]
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
                    "metric": {"app": "checkout"},
                    "values": [
                        [1_700_000_000.0, "0.5"],
                        [1_700_000_060.0, "0.6"]
                    ]
                }
            ]
        }
    })
}

#[tokio::test]
async fn tier1_logs_table_rejects_select_star_without_stream_label_predicate() {
    let (_server, ctx, _cfg) = fixture().await;

    // No `labels['k']` predicate → empty stream selector → table-level
    // rejection. LogQL requires at least one stream-label matcher.
    let result = ctx.sql("SELECT * FROM logs").await.unwrap().collect().await;
    let err = result.expect_err("expected UnsupportedPushdown");
    let msg = err.to_string();
    assert!(
        msg.contains("loki_range(") || msg.contains("loki_query("),
        "error should point at escape hatch: {msg}"
    );
}

#[tokio::test]
async fn tier1_logs_table_rejects_pure_line_filter_with_no_stream_label() {
    let (_server, ctx, _cfg) = fixture().await;

    // Even with a recognized predicate (line LIKE), the translator
    // emits an empty stream selector, which the table rejects before
    // calling Loki.
    let result = ctx
        .sql(
            "SELECT * FROM logs \
             WHERE line LIKE '%error%' \
             AND ts > NOW() - INTERVAL '5 minutes'",
        )
        .await
        .unwrap()
        .collect()
        .await;
    let err = result.expect_err("expected UnsupportedPushdown");
    let msg = err.to_string();
    assert!(
        msg.contains("loki_range("),
        "error should point at loki_range escape hatch: {msg}"
    );
}

#[tokio::test]
async fn tier3_loki_range_returns_log_lines_from_streams_response() {
    let (server, ctx, _cfg) = fixture().await;

    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query_range"))
        .and(query_param("query", r#"{app="checkout"} |= "error""#))
        .respond_with(ResponseTemplate::new(200).set_body_json(streams_response_body()))
        .expect(1)
        .mount(&server)
        .await;

    let batches = ctx
        .sql(
            "SELECT ts, line, stream \
             FROM loki_range( \
                 '{app=\"checkout\"} |= \"error\"', \
                 TIMESTAMP '2023-11-14T22:00:00Z', \
                 TIMESTAMP '2023-11-14T22:13:20Z' \
             )",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "two streams should produce 3 total rows");
}

#[tokio::test]
async fn tier3_loki_range_with_explicit_limit_forwards_it_upstream() {
    let (server, ctx, _cfg) = fixture().await;

    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query_range"))
        .and(query_param("limit", "100"))
        .respond_with(ResponseTemplate::new(200).set_body_json(streams_response_body()))
        .expect(1)
        .mount(&server)
        .await;

    let _ = ctx
        .sql(
            "SELECT * FROM loki_range( \
                '{app=\"x\"}', \
                TIMESTAMP '2023-11-14T22:00:00Z', \
                TIMESTAMP '2023-11-14T22:13:20Z', \
                100 \
             )",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
}

#[tokio::test]
async fn tier3_loki_query_returns_instant_results() {
    let (server, ctx, _cfg) = fixture().await;

    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query"))
        .and(query_param("query", r#"{app="checkout"}"#))
        .respond_with(ResponseTemplate::new(200).set_body_json(streams_response_body()))
        .expect(1)
        .mount(&server)
        .await;

    let batches = ctx
        .sql(r#"SELECT * FROM loki_query('{app="checkout"}')"#)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn tier3_loki_query_handles_matrix_response_by_packing_value_into_line() {
    let (server, ctx, _cfg) = fixture().await;

    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(matrix_response_body()))
        .expect(1)
        .mount(&server)
        .await;

    let batches = ctx
        .sql(r#"SELECT line, stream FROM loki_query('rate({app="checkout"}[5m])')"#)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "matrix with 2 samples should produce 2 rows");
    // Per-row `stream = "matrix"` and `line = <value_string>` packing
    // is verified in the unit-level
    // `build_record_batch_from_matrix_packs_value_into_line` test;
    // here we only need to confirm the response shape survives the
    // full DataFusion pipeline.
}

#[tokio::test]
async fn tier3_loki_query_returns_empty_batch_on_empty_result() {
    let (server, ctx, _cfg) = fixture().await;

    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": []
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let batches = ctx
        .sql(r#"SELECT * FROM loki_query('{app="nonexistent"}')"#)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0, "empty result is not an error");
}

#[tokio::test]
async fn tier3_loki_query_surfaces_upstream_parse_error() {
    let (server, ctx, _cfg) = fixture().await;

    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "status": "error",
            "errorType": "bad_data",
            "error": "invalid LogQL"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let result = ctx
        .sql(r#"SELECT * FROM loki_query('not valid')"#)
        .await
        .unwrap()
        .collect()
        .await;
    let err = result.expect_err("expected upstream parse error");
    let msg = err.to_string();
    assert!(msg.contains("loki"), "error should name source: {msg}");
    assert!(
        msg.contains("invalid LogQL") || msg.contains("bad_data"),
        "error should surface upstream message: {msg}"
    );
}

#[tokio::test]
async fn row_cap_is_enforced_for_loki() {
    let server = MockServer::start().await;
    let url = Url::parse(&server.uri()).unwrap();

    let config = OtelSourceConfig {
        backend: skardi::sources::providers::otel::OtelBackend::Loki,
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
        source_name: "loki".to_string(),
        base_url: url,
        auth: ResolvedAuth::None,
        extra_headers: BTreeMap::new(),
        request_timeout: Duration::from_secs(5),
    })
    .unwrap();

    let ctx = SessionContext::new();
    register_loki_source(&ctx, "loki", &config, client).unwrap();

    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(streams_response_body()))
        .expect(1)
        .mount(&server)
        .await;

    let result = ctx
        .sql(r#"SELECT * FROM loki_query('{app="x"}')"#)
        .await
        .unwrap()
        .collect()
        .await;
    let err = result.expect_err("cap=2 with 3-row response should error");
    let msg = err.to_string();
    assert!(
        msg.contains("max_result_rows") || msg.contains("exceeded"),
        "error should mention cap: {msg}"
    );
    assert!(msg.contains("loki"), "error should name source: {msg}");
}

// ---------------------------------------------------------------------------
// `labels['k']` pushdown — end-to-end via wiremock so we pin the exact
// LogQL stream selector that leaves the translator and reaches Loki.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tier1_logs_table_pushes_labels_eq_into_stream_selector() {
    let (server, ctx, _cfg) = fixture().await;

    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query_range"))
        .and(query_param("query", r#"{app="checkout"}"#))
        .respond_with(ResponseTemplate::new(200).set_body_json(streams_response_body()))
        .expect(1)
        .mount(&server)
        .await;

    let batches = ctx
        .sql(
            "SELECT ts, line FROM logs \
             WHERE labels['app'] = 'checkout' \
               AND ts BETWEEN TIMESTAMP '2023-11-14T22:00:00Z' \
                          AND TIMESTAMP '2023-11-14T23:00:00Z'",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn tier1_logs_table_pushes_labels_with_line_filter_into_logql() {
    let (server, ctx, _cfg) = fixture().await;

    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query_range"))
        .and(query_param("query", r#"{app="checkout"} |= "timeout""#))
        .respond_with(ResponseTemplate::new(200).set_body_json(streams_response_body()))
        .expect(1)
        .mount(&server)
        .await;

    let batches = ctx
        .sql(
            "SELECT ts, line FROM logs \
             WHERE labels['app'] = 'checkout' \
               AND line LIKE '%timeout%' \
               AND ts BETWEEN TIMESTAMP '2023-11-14T22:00:00Z' \
                          AND TIMESTAMP '2023-11-14T23:00:00Z'",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

// ---------------------------------------------------------------------------
// Regression test for the projection-pushdown bug — see PR #140 review.
//
// Before the fix, `LokiEscapeProvider::scan` ignored the `projection`
// argument while still advertising the full 4-column schema, causing
// DataFusion to trip `Input field name <X> does not match with the
// projection expression <Y>` whenever a query picked a column subset
// or aliased a literal over a real column.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn projection_pushdown_select_labels_extract_from_loki_range() {
    let (server, ctx, _cfg) = fixture().await;

    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query_range"))
        .respond_with(ResponseTemplate::new(200).set_body_json(streams_response_body()))
        .expect(1)
        .mount(&server)
        .await;

    let batches = ctx
        .sql(
            "SELECT labels['app'] AS app, line \
             FROM loki_range( \
                 '{app=\"checkout\"}', \
                 TIMESTAMP '2023-11-14T22:00:00Z', \
                 TIMESTAMP '2023-11-14T23:00:00Z')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "streams response should produce 3 rows");
    let schema = batches[0].schema();
    let names: Vec<_> = schema.fields().iter().map(|f| f.name().clone()).collect();
    assert_eq!(names, vec!["app", "line"]);
}
