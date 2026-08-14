//! HTTP integration tests for pipeline-execution auditing
//! (`POST /:name/execute`) — mirrors the `/query` audit coverage in
//! `query_http.rs`, but exercises the pipeline path: record-before-execute,
//! the `x-skardi-session-id` header, and value-free recording (only the
//! pipeline name is ever written, never parameter values).

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use datafusion::prelude::SessionContext;
use serde_json::{Value, json};
use skardi::pipeline::pipeline::{Pipeline, StandardPipeline};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

use skardi_server::auth::layer::AuthLayer;
use skardi_server::config::{CliArgs, ServerConfig};
use skardi_server::query_audit::QueryAuditStore;
use skardi_server::semantics::SemanticsRegistry;
use skardi_server::server::{AppState, configure_routes};

/// Name of the pipeline registered by `make_app_state`. Bound as a `const`
/// so assertions and the YAML fixture below stay in sync.
const TEST_PIPELINE_NAME: &str = "product-search";

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

/// Build an `AppState` with a `products` MemTable, the `product-search`
/// pipeline loaded from disk in envelope format, and — when requested — an
/// operator audit ledger wired in.
async fn make_app_state(query_audit: Option<Arc<QueryAuditStore>>) -> (AppState, TempDir) {
    let tmp = TempDir::new().unwrap();
    let ctx = Arc::new(SessionContext::new());
    ctx.register_batch("products", products_batch()).unwrap();

    let mut pipelines: HashMap<String, StandardPipeline> = HashMap::new();
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
            query_audit_db: None,
            query_audit_retention_days: None,
        },
    };
    let state = AppState::new(
        config,
        engine,
        Arc::clone(&ctx),
        AuthLayer::None,
        None,
        query_audit,
    );
    (state, tmp)
}

/// POST `{"brand": "Apple", "max_price": ...}`-shaped params to
/// `/product-search/execute`, attaching the given extra headers.
async fn execute_with_headers(
    state: &AppState,
    headers: &[(&str, &str)],
    params: Value,
) -> axum::response::Response {
    let app = configure_routes(state.clone());
    let mut builder = Request::builder()
        .method("POST")
        .uri(format!("/{TEST_PIPELINE_NAME}/execute"))
        .header("content-type", "application/json");
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    app.oneshot(builder.body(Body::from(params.to_string())).unwrap())
        .await
        .unwrap()
}

/// Full parameter set the fixture pipeline needs to execute successfully.
fn valid_params() -> Value {
    json!({"brand": "Apple", "max_price": 1500.0})
}

#[tokio::test]
async fn successful_execution_is_audited_with_session() {
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    let resp =
        execute_with_headers(&state, &[("x-skardi-session-id", "sess-9")], valid_params()).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let rows = store.list_by_session("sess-9").await.unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row["statement_kind"], "pipeline");
    assert_eq!(row["sql"], TEST_PIPELINE_NAME);
    assert_eq!(row["status"], "succeeded");
    assert!(row["row_count"].as_u64().is_some());
    assert!(row["ai_context"].is_null());
}

#[tokio::test]
async fn execution_without_header_audits_null_session() {
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    let resp = execute_with_headers(&state, &[], valid_params()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(store.count().await.unwrap(), 1);
    // Not reachable via list_by_session: session_id is NULL.
    assert!(store.list_by_session("").await.unwrap().is_empty());
}

#[tokio::test]
async fn no_store_configured_executes_without_recording() {
    let (state, _tmp) = make_app_state(None).await;
    let resp = execute_with_headers(&state, &[], valid_params()).await;
    assert_eq!(resp.status(), StatusCode::OK);
}
