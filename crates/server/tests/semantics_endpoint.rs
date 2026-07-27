//! Integration test for the `kind: semantics` overlay:
//! boot the same `configure_routes` axum router the binary does, register
//! a MemTable, attach a semantics overlay, and verify the descriptions
//! flow out through `GET /data_source`.
//!
//! Companion to `pipelines_http.rs` and `jobs_http.rs`.

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use datafusion::prelude::SessionContext;
use http_body_util::BodyExt;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

use skardi::sources::DataSourceType;
use skardi_server::auth::layer::AuthLayer;
use skardi_server::config::{CliArgs, DataSource, ServerConfig};
use skardi_server::semantics::SemanticsRegistry;
use skardi_server::server::{AppState, configure_routes};

fn write_yaml(path: &std::path::Path, content: &str) {
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(content.as_bytes()).unwrap();
}

fn products_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("brand", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3])),
            Arc::new(StringArray::from(vec!["Apple", "Sony", "Apple"])),
            Arc::new(Float64Array::from(vec![1299.0, 199.0, 999.0])),
        ],
    )
    .unwrap()
}

fn ctx_descriptions(sources: &[DataSource]) -> Vec<(String, Option<String>)> {
    sources
        .iter()
        .map(|ds| (ds.name.clone(), ds.description.clone()))
        .collect()
}

fn products_data_source(description: Option<&str>) -> DataSource {
    DataSource {
        name: "products".to_string(),
        source_type: DataSourceType::Csv,
        path: PathBuf::from("unused.csv"),
        connection_string: None,
        schema: None,
        options: None,
        access_mode: Default::default(),
        enable_cache: false,
        hierarchy_level: Default::default(),
        description: description.map(str::to_string),
        open_connector: None,
    }
}

async fn make_state(data_sources: Vec<DataSource>, semantics: SemanticsRegistry) -> AppState {
    let ctx = Arc::new(SessionContext::new());
    ctx.register_batch("products", products_batch()).unwrap();

    let engine = Arc::new(skardi::engine::datafusion::DataFusionEngine::new_with_arc(
        Arc::clone(&ctx),
    ));
    let config = ServerConfig {
        pipelines: HashMap::new(),
        jobs: HashMap::new(),
        data_sources,
        semantics,
        args: CliArgs {
            pipeline_path: None,
            jobs_path: None,
            jobs_db_path: None,
            ctx_file: None,
            semantics_path: None,
            port: 0,
        },
    };
    AppState::new(config, engine, Arc::clone(&ctx), AuthLayer::None, None)
}

async fn body_to_json(resp: axum::response::Response) -> Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

async fn fetch_data_source(state: AppState) -> Value {
    let app = configure_routes(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/data_source")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    body_to_json(resp).await
}

// ---------------------------------------------------------------------------
// Semantics file overlays surface on the catalog endpoint.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn semantics_file_descriptions_appear_in_data_source_response() {
    let tmp = TempDir::new().unwrap();
    let sem_path = tmp.path().join("sem.yaml");
    write_yaml(
        &sem_path,
        r#"
kind: semantics
metadata:
  name: products-semantics
  version: 1.0.0
spec:
  sources:
    - name: products
      description: "Five-row product fixture (semantics-overridden)"
      columns:
        - name: id
          description: "Stable internal SKU"
        - name: price
          description: "Retail price in USD"
"#,
    );

    let data_sources = vec![products_data_source(None)];
    let semantics =
        SemanticsRegistry::build(Some(&sem_path), &ctx_descriptions(&data_sources)).unwrap();
    let state = make_state(data_sources, semantics).await;

    let body = fetch_data_source(state).await;
    let table = &body["data"][0]["tables"][0];

    assert_eq!(
        table["description"].as_str(),
        Some("Five-row product fixture (semantics-overridden)"),
        "table description missing/wrong: {body}"
    );

    let cols = table["schema"].as_array().unwrap();
    let col_desc = |name: &str| -> Option<&str> {
        cols.iter()
            .find(|c| c["name"].as_str() == Some(name))
            .and_then(|c| c["description"].as_str())
    };
    assert_eq!(col_desc("id"), Some("Stable internal SKU"));
    assert_eq!(col_desc("price"), Some("Retail price in USD"));
    // `brand` has no overlay — the field is omitted from the JSON entirely.
    let brand = cols
        .iter()
        .find(|c| c["name"].as_str() == Some("brand"))
        .unwrap();
    assert!(
        brand.get("description").is_none(),
        "brand should not have a description key, got: {brand}"
    );
}

// ---------------------------------------------------------------------------
// ctx-inline `description` is the fallback when no semantics file is loaded.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ctx_inline_description_used_when_no_semantics_file() {
    let data_sources = vec![products_data_source(Some("From ctx.yaml"))];
    let semantics = SemanticsRegistry::build(None, &ctx_descriptions(&data_sources)).unwrap();
    let state = make_state(data_sources, semantics).await;

    let body = fetch_data_source(state).await;
    let table = &body["data"][0]["tables"][0];
    assert_eq!(table["description"].as_str(), Some("From ctx.yaml"));
    // No column overlays exist, so no schema entry should carry one.
    for col in table["schema"].as_array().unwrap() {
        assert!(
            col.get("description").is_none(),
            "no column should have a description, got: {col}"
        );
    }
}

// ---------------------------------------------------------------------------
// Semantics file wins over the ctx-inline description on the same source.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn semantics_file_overrides_ctx_inline_description() {
    let tmp = TempDir::new().unwrap();
    let sem_path = tmp.path().join("sem.yaml");
    write_yaml(
        &sem_path,
        r#"
kind: semantics
metadata: { name: t }
spec:
  sources:
    - name: products
      description: "Override"
"#,
    );

    let data_sources = vec![products_data_source(Some("Original from ctx"))];
    let semantics =
        SemanticsRegistry::build(Some(&sem_path), &ctx_descriptions(&data_sources)).unwrap();
    let state = make_state(data_sources, semantics).await;

    let body = fetch_data_source(state).await;
    let table = &body["data"][0]["tables"][0];
    assert_eq!(table["description"].as_str(), Some("Override"));
}
