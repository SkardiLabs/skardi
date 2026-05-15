//! Smoke test for the `demo/otel_service_health/` pipelines.
//!
//! Boots the demo's pipelines against wiremock-stubbed Prometheus + Loki
//! upstreams (so the test doesn't need the `observability/docker-compose.yml`
//! stack running in CI) and verifies that each pipeline endpoint returns
//! a non-empty response. Per `add-otel-data-source/tasks.md` 8.3.

#![cfg(feature = "otel")]

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use datafusion::prelude::SessionContext;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use skardi::pipeline::pipeline::{Pipeline, StandardPipeline};
use tempfile::TempDir;
use tower::ServiceExt;
use url::Url;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use skardi::sources::providers::otel::OtelSourceConfig;
use skardi::sources::providers::otel::http::{OtelHttpClient, OtelHttpClientConfig, ResolvedAuth};
use skardi::sources::providers::otel::loki::register_loki_source;
use skardi::sources::providers::otel::prometheus::register_prometheus_source;

use skardi_server::auth::layer::AuthLayer;
use skardi_server::config::{CliArgs, ServerConfig};
use skardi_server::metrics::PipelineMetrics;
use skardi_server::semantics::SemanticsRegistry;
use skardi_server::server::{AppState, configure_routes};

fn prom_response() -> serde_json::Value {
    json!({
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [
                {
                    "metric": {"__name__": "http_requests_total", "service": "api"},
                    "values": [
                        [1_700_000_000.0, "42"],
                        [1_700_000_060.0, "43"]
                    ]
                }
            ]
        }
    })
}

fn prom_histogram_quantile_response() -> serde_json::Value {
    json!({
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {
                    "metric": {"route": "/checkout"},
                    "value": [1_700_000_000.0, "0.42"]
                },
                {
                    "metric": {"route": "/cart"},
                    "value": [1_700_000_000.0, "0.18"]
                }
            ]
        }
    })
}

fn loki_response() -> serde_json::Value {
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
                }
            ]
        }
    })
}

/// Set up a `SessionContext` with OTEL sources registered against wiremock
/// servers, load the three demo pipelines, and assemble an `AppState` so
/// the test can drive HTTP requests through the same router the production
/// binary uses.
async fn fixture() -> (MockServer, MockServer, AppState, TempDir) {
    let prom_server = MockServer::start().await;
    let loki_server = MockServer::start().await;

    let prom_url = Url::parse(&prom_server.uri()).unwrap();
    let loki_url = Url::parse(&loki_server.uri()).unwrap();

    let ctx = Arc::new(SessionContext::new());

    // --- Prometheus source ---
    let prom_cfg = OtelSourceConfig {
        backend: skardi::sources::providers::otel::OtelBackend::Prometheus,
        url: prom_url.clone(),
        auth: Default::default(),
        extra_headers: BTreeMap::new(),
        max_result_rows: Some(50_000),
        default_window: None,
        default_step: None,
        max_window: None,
        request_timeout: Some(Duration::from_secs(5)),
    };
    let prom_client = OtelHttpClient::new(OtelHttpClientConfig {
        source_name: "prom".to_string(),
        base_url: prom_url,
        auth: ResolvedAuth::None,
        extra_headers: BTreeMap::new(),
        request_timeout: Duration::from_secs(5),
    })
    .unwrap();
    register_prometheus_source(&ctx, "prom", &prom_cfg, prom_client).unwrap();

    // --- Loki source ---
    let loki_cfg = OtelSourceConfig {
        backend: skardi::sources::providers::otel::OtelBackend::Loki,
        url: loki_url.clone(),
        auth: Default::default(),
        extra_headers: BTreeMap::new(),
        max_result_rows: Some(5_000),
        default_window: None,
        default_step: None,
        max_window: None,
        request_timeout: Some(Duration::from_secs(5)),
    };
    let loki_client = OtelHttpClient::new(OtelHttpClientConfig {
        source_name: "loki".to_string(),
        base_url: loki_url,
        auth: ResolvedAuth::None,
        extra_headers: BTreeMap::new(),
        request_timeout: Duration::from_secs(5),
    })
    .unwrap();
    register_loki_source(&ctx, "loki", &loki_cfg, loki_client).unwrap();

    // Verify the demo pipeline files load cleanly. We re-write them
    // into a tempdir so this test stays independent of the
    // `demo/otel_service_health/` directory layout if it moves.
    let tmp = TempDir::new().unwrap();
    let demo_root = std::env::current_dir()
        .unwrap()
        .ancestors()
        .find(|p| {
            p.join("demo/otel_service_health/pipelines/service_health.yaml")
                .exists()
        })
        .expect("test must run inside the repo so the demo files are reachable")
        .to_path_buf();

    let mut pipelines: HashMap<String, StandardPipeline> = HashMap::new();
    for name in ["service_health", "top_error_logs", "latency_p99_by_route"] {
        let src = demo_root
            .join("demo/otel_service_health/pipelines")
            .join(format!("{name}.yaml"));
        let dst = tmp.path().join(format!("{name}.yaml"));
        std::fs::copy(&src, &dst)
            .unwrap_or_else(|e| panic!("failed to copy demo pipeline {}: {e}", src.display()));
        let pipeline = StandardPipeline::load_from_file(&dst, Arc::clone(&ctx))
            .await
            .unwrap_or_else(|e| panic!("demo pipeline {name} failed to load: {e}"));
        pipelines.insert(pipeline.name().to_string(), pipeline);
    }

    let engine = Arc::new(skardi::engine::datafusion::DataFusionEngine::new_with_arc(
        Arc::clone(&ctx),
    ));
    let config = ServerConfig {
        pipelines,
        jobs: HashMap::new(),
        data_sources: vec![],
        semantics: SemanticsRegistry::default(),
        args: CliArgs {
            pipeline_path: None,
            jobs_path: None,
            jobs_db_path: None,
            ctx_file: None,
            semantics_path: None,
            port: 0,
        },
    };
    let state = AppState {
        config: Arc::new(RwLock::new(config)),
        engine,
        session_ctx: Arc::clone(&ctx),
        metrics: PipelineMetrics::new(),
        auth_layer: AuthLayer::None,
        jobs: None,
    };
    (prom_server, loki_server, state, tmp)
}

async fn body_to_json(resp: axum::response::Response) -> Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn demo_service_health_pipeline_returns_rows_via_metrics_table() {
    let (prom_server, _loki_server, state, _tmp) = fixture().await;

    // `service_health` selects `* FROM metrics WHERE name='http_requests_total'`
    // which the v1 translator pushes down to /api/v1/query_range.
    Mock::given(method("GET"))
        .and(path("/api/v1/query_range"))
        .respond_with(ResponseTemplate::new(200).set_body_json(prom_response()))
        .expect(1..)
        .mount(&prom_server)
        .await;

    let app = configure_routes(state);
    let req = json!({});
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/service-health/execute")
                .header("content-type", "application/json")
                .body(Body::from(req.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    assert_eq!(body["success"], true, "body: {body}");
    assert!(
        body["rows"].as_u64().unwrap_or(0) > 0,
        "expected at least one row: {body}"
    );
}

#[tokio::test]
async fn demo_latency_p99_pipeline_returns_rows_via_prom_query() {
    let (prom_server, _loki_server, state, _tmp) = fixture().await;

    // `latency_p99_by_route` uses prom_query → /api/v1/query (instant).
    Mock::given(method("GET"))
        .and(path("/api/v1/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(prom_histogram_quantile_response()))
        .expect(1..)
        .mount(&prom_server)
        .await;

    let app = configure_routes(state);
    let req = json!({});
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/latency-p99-by-route/execute")
                .header("content-type", "application/json")
                .body(Body::from(req.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let body = body_to_json(resp).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["success"], true, "body: {body}");
    assert!(
        body["rows"].as_u64().unwrap_or(0) > 0,
        "expected rows: {body}"
    );
}

#[tokio::test]
async fn demo_top_error_logs_pipeline_returns_rows_via_loki_range() {
    let (_prom_server, loki_server, state, _tmp) = fixture().await;

    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query_range"))
        .respond_with(ResponseTemplate::new(200).set_body_json(loki_response()))
        .expect(1..)
        .mount(&loki_server)
        .await;

    let app = configure_routes(state);
    let req = json!({});
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/top-error-logs/execute")
                .header("content-type", "application/json")
                .body(Body::from(req.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    assert_eq!(body["success"], true, "body: {body}");
    assert!(
        body["rows"].as_u64().unwrap_or(0) > 0,
        "expected rows: {body}"
    );
}
