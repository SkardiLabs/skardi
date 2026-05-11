//! HTTP integration tests for pipeline endpoints — boots the same
//! `configure_routes` axum router the binary does, loads a pipeline from
//! an envelope-format YAML, and exercises the request/response surface.
//!
//! Mirrors `jobs_http.rs`.

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use datafusion::prelude::SessionContext;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use skardi::pipeline::pipeline::{Pipeline, StandardPipeline};
use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, RwLock};
use tempfile::TempDir;
use tower::ServiceExt;

use skardi_server::auth::layer::AuthLayer;
use skardi_server::config::{CliArgs, ServerConfig};
use skardi_server::metrics::PipelineMetrics;
use skardi_server::semantics::SemanticsRegistry;
use skardi_server::server::{AppState, configure_routes};

fn write_yaml(path: &std::path::Path, content: &str) {
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(content.as_bytes()).unwrap();
}

/// Five-row `products` MemTable: id, brand, price, category. Used as the
/// fact table for the pipeline's SELECT.
fn products_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("brand", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("category", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Apple", "Sony", "Apple", "Samsung", "Apple",
            ])),
            Arc::new(Float64Array::from(vec![
                1299.0, 199.0, 999.0, 599.0, 2499.0,
            ])),
            Arc::new(StringArray::from(vec![
                "Electronics",
                "Audio",
                "Electronics",
                "Electronics",
                "Electronics",
            ])),
        ],
    )
    .unwrap()
}

/// Build an `AppState` with a `products` MemTable and — when requested —
/// a `product-search` pipeline loaded from disk in envelope format.
async fn make_app_state(with_pipeline: bool) -> (AppState, TempDir) {
    let tmp = TempDir::new().unwrap();
    let ctx = Arc::new(SessionContext::new());
    ctx.register_batch("products", products_batch()).unwrap();

    let mut pipelines: HashMap<String, StandardPipeline> = HashMap::new();
    if with_pipeline {
        let yaml_path = tmp.path().join("product-search.yaml");
        write_yaml(
            &yaml_path,
            r#"
kind: pipeline
metadata:
  name: "product-search"
  version: "1.0.0"
  description: "Filter products by brand + max price"
spec:
  query: |
    SELECT id, brand, price, category
    FROM products
    WHERE brand = {brand} AND price <= {max_price}
    ORDER BY price DESC
"#,
        );
        let pipeline = StandardPipeline::load_from_file(&yaml_path, Arc::clone(&ctx))
            .await
            .unwrap();
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
        api_keys: None,
        jobs: None,
    };
    (state, tmp)
}

async fn body_to_json(resp: axum::response::Response) -> Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

// ---------------------------------------------------------------------------
// POST /:name/execute — happy path.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn http_execute_pipeline_returns_matching_rows() {
    let (state, _tmp) = make_app_state(true).await;
    let app = configure_routes(state);

    // `ExecuteRequest` uses `#[serde(flatten)]`, so params live at the
    // root of the JSON body, not inside a `parameters` wrapper.
    let req_body = json!({
        "brand": "Apple",
        "max_price": 1500.0,
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/product-search/execute")
                .header("content-type", "application/json")
                .body(Body::from(req_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    assert_eq!(body["success"], true, "body: {body}");
    // Two Apple rows are <= 1500 (ids 1 and 3); the third Apple (id=5) is
    // 2499 and must be filtered out.
    assert_eq!(body["rows"].as_u64(), Some(2), "body: {body}");
    let data = body["data"].as_array().unwrap();
    let ids: Vec<i64> = data.iter().map(|row| row["id"].as_i64().unwrap()).collect();
    assert_eq!(
        ids,
        vec![1, 3],
        "expected price-desc ordering, got: {data:?}"
    );
}

// ---------------------------------------------------------------------------
// Missing required parameter → 400 with a structured error body.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn http_execute_missing_param_returns_400() {
    let (state, _tmp) = make_app_state(true).await;
    let app = configure_routes(state);

    let req_body = json!({ "brand": "Apple" }); // missing max_price
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/product-search/execute")
                .header("content-type", "application/json")
                .body(Body::from(req_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    let msg = body["error"].as_str().unwrap_or_default();
    assert!(
        msg.contains("max_price"),
        "error should name the missing param, got: {body}"
    );
}

// ---------------------------------------------------------------------------
// Parameter value of an unsupported JSON type (e.g. a nested object) → 400.
// Extra keys not referenced by the SQL are silently ignored, which is a
// separate contract; this test exercises the type-validation path.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn http_execute_unsupported_param_type_returns_400() {
    let (state, _tmp) = make_app_state(true).await;
    let app = configure_routes(state);

    let req_body = json!({
        "brand": { "nested": "object" },   // object is not a scalar → rejected
        "max_price": 1500.0,
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/product-search/execute")
                .header("content-type", "application/json")
                .body(Body::from(req_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    let msg = body["error"].as_str().unwrap_or_default();
    assert!(
        msg.to_lowercase().contains("brand") || msg.to_lowercase().contains("unsupported"),
        "error should flag the brand parameter, got: {body}"
    );
}

// ---------------------------------------------------------------------------
// Unknown pipeline → 404 with `available_pipelines` listed in the details.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn http_execute_unknown_pipeline_returns_404() {
    let (state, _tmp) = make_app_state(true).await;
    let app = configure_routes(state);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/does-not-exist/execute")
                .header("content-type", "application/json")
                .body(Body::from("{\"parameters\":{}}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = body_to_json(resp).await;
    let available = body["details"]["available_pipelines"]
        .as_array()
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
        .unwrap_or_default();
    assert!(
        available.contains(&"product-search"),
        "404 body should enumerate registered pipelines, got: {body}"
    );
}

// ---------------------------------------------------------------------------
// GET /pipelines — lists registered pipelines with version + endpoint.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn http_list_pipelines_returns_registered() {
    let (state, _tmp) = make_app_state(true).await;
    let app = configure_routes(state);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/pipelines")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    assert_eq!(body["count"].as_u64(), Some(1));
    let entry = &body["pipelines"].as_array().unwrap()[0];
    assert_eq!(entry["name"].as_str(), Some("product-search"));
    assert_eq!(entry["version"].as_str(), Some("1.0.0"));
    assert_eq!(entry["endpoint"].as_str(), Some("/product-search/execute"));
}

// ---------------------------------------------------------------------------
// GET /pipeline/:name — metadata + inferred parameters for one pipeline.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn http_get_pipeline_info_returns_metadata_and_params() {
    let (state, _tmp) = make_app_state(true).await;
    let app = configure_routes(state);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/pipeline/product-search")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    assert_eq!(body["success"], true);
    assert_eq!(body["pipeline"]["name"].as_str(), Some("product-search"));
    assert_eq!(body["pipeline"]["version"].as_str(), Some("1.0.0"));
    // Inferred params come from `{brand}` and `{max_price}` in the SQL.
    // The handler returns them as an array of `{ name, type }` objects;
    // iteration order is unstable so assert by name-set rather than index.
    let names: Vec<&str> = body["pipeline"]["parameters"]
        .as_array()
        .expect("parameters array")
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"brand"), "params: {names:?}");
    assert!(names.contains(&"max_price"), "params: {names:?}");
}

// ---------------------------------------------------------------------------
// GET /pipeline/:name — unknown name → 404.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn http_get_pipeline_info_unknown_returns_404() {
    let (state, _tmp) = make_app_state(true).await;
    let app = configure_routes(state);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/pipeline/missing")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// GET /health/:name — healthy when the pipeline exists and its sources
// are reachable. We register the `products` table directly on the session
// context, so the data-sources check is "0 data_sources total" — the pipe
// itself should still report accessible.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn http_pipeline_health_check_returns_ok() {
    let (state, _tmp) = make_app_state(true).await;
    let app = configure_routes(state);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health/product-search")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    assert_eq!(body["pipeline"]["name"].as_str(), Some("product-search"));
    // There's a `status` field at the root — it can be "healthy" or a
    // downgraded variant depending on data source count. Either way, the
    // endpoint must not 5xx, and the pipeline block must echo the name.
    assert!(body.get("status").is_some(), "health body: {body}");
}
