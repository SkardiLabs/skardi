//! HTTP integration tests for the ad-hoc SQL endpoint (`POST /query`) —
//! boots the same `configure_routes` axum router the binary does and
//! exercises the request/response surface. Mirrors `pipelines_http.rs`.

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use datafusion::prelude::SessionContext;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use skardi::engine::datafusion::DataFusionEngine;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tower::ServiceExt;

use skardi_server::auth::layer::AuthLayer;
use skardi_server::auth::mode::AuthMode;
use skardi_server::config::{
    AccessMode, CliArgs, DataSource, DataSourceType, ServerConfig, validator_config_from_sources,
};
use skardi_server::metrics::PipelineMetrics;
use skardi_server::semantics::SemanticsRegistry;
use skardi_server::server::{AppState, configure_routes};

/// Five-row `products` MemTable: id, brand.
fn products_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("brand", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Apple", "Sony", "Apple", "Samsung", "Apple",
            ])),
        ],
    )
    .unwrap()
}

/// One-row `scratch` MemTable used as the read-write INSERT target.
fn scratch_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64]))]).unwrap()
}

fn data_source(name: &str, access_mode: AccessMode) -> DataSource {
    DataSource {
        name: name.to_string(),
        source_type: DataSourceType::Csv,
        path: Default::default(),
        connection_string: None,
        schema: None,
        options: None,
        hierarchy_level: Default::default(),
        access_mode,
        enable_cache: false,
        description: None,
    }
}

/// AppState with `products` (read_only) and `scratch` (read_write) MemTables.
fn make_state() -> AppState {
    let ctx = Arc::new(SessionContext::new());
    ctx.register_batch("products", products_batch()).unwrap();
    ctx.register_batch("scratch", scratch_batch()).unwrap();
    let engine = Arc::new(DataFusionEngine::new_with_arc(Arc::clone(&ctx)));
    let config = ServerConfig {
        pipelines: HashMap::new(),
        jobs: HashMap::new(),
        data_sources: vec![
            data_source("products", AccessMode::ReadOnly),
            data_source("scratch", AccessMode::ReadWrite),
        ],
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
    let validator_config = Arc::new(validator_config_from_sources(&config.data_sources));
    AppState {
        config: Arc::new(RwLock::new(config)),
        engine,
        session_ctx: ctx,
        metrics: PipelineMetrics::new(),
        auth_layer: AuthLayer::None,
        jobs: None,
        validator_config,
    }
}

async fn post_query(state: AppState, body: Value) -> axum::response::Response {
    let app = configure_routes(state);
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/query")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
    )
    .await
    .unwrap()
}

async fn body_to_json(resp: axum::response::Response) -> Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

// ---------------------------------------------------------------------------
// Happy path

#[tokio::test]
async fn select_returns_rows_with_envelope() {
    let resp = post_query(
        make_state(),
        json!({"sql": "SELECT id, brand FROM products ORDER BY id"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    assert_eq!(body["success"], json!(true));
    assert_eq!(body["rows"], json!(5));
    assert_eq!(body["truncated"], json!(false));
    assert_eq!(body["data"].as_array().unwrap().len(), 5);
    assert_eq!(body["data"][0]["brand"], json!("Apple"));
    assert!(body["execution_time_ms"].is_u64());
}

#[tokio::test]
async fn max_rows_truncates_and_flags() {
    let resp = post_query(
        make_state(),
        json!({"sql": "SELECT id FROM products ORDER BY id", "max_rows": 2}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    assert_eq!(body["rows"], json!(2));
    assert_eq!(body["truncated"], json!(true));
    assert_eq!(body["data"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn result_exactly_at_cap_is_not_truncated() {
    let resp = post_query(
        make_state(),
        json!({"sql": "SELECT id FROM products", "max_rows": 5}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    assert_eq!(body["rows"], json!(5));
    assert_eq!(body["truncated"], json!(false));
}

#[tokio::test]
async fn max_rows_usize_max_does_not_overflow() {
    let resp = post_query(
        make_state(),
        json!({"sql": "SELECT id FROM products", "max_rows": 18446744073709551615u64}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    assert_eq!(body["rows"], json!(5));
    assert_eq!(body["truncated"], json!(false));
}

// ---------------------------------------------------------------------------
// Validation errors → 400

#[tokio::test]
async fn max_rows_zero_rejected() {
    let resp = post_query(make_state(), json!({"sql": "SELECT 1", "max_rows": 0})).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("parameter_validation_error"));
}

#[tokio::test]
async fn ddl_rejected() {
    let resp = post_query(make_state(), json!({"sql": "DROP TABLE products"})).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("sql_validation_error"));
    assert_eq!(body["details"]["operation"], json!("DROP"));
}

#[tokio::test]
async fn copy_rejected() {
    let resp = post_query(make_state(), json!({"sql": "COPY products TO 'out.csv'"})).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("sql_validation_error"));
}

#[tokio::test]
async fn multi_statement_rejected() {
    let resp = post_query(make_state(), json!({"sql": "SELECT 1; SELECT 2"})).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("sql_validation_error"));
    assert_eq!(body["details"]["statement_count"], json!(2));
}

#[tokio::test]
async fn unparseable_sql_rejected() {
    let resp = post_query(make_state(), json!({"sql": "SELEKT * FROM products"})).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("sql_validation_error"));
}

// ---------------------------------------------------------------------------
// Access modes

#[tokio::test]
async fn insert_into_read_only_source_rejected() {
    let resp = post_query(
        make_state(),
        json!({"sql": "INSERT INTO products (id, brand) VALUES (6, 'LG')"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("sql_validation_error"));
    assert_eq!(body["details"]["operation"], json!("INSERT"));
    assert_eq!(body["details"]["table"], json!("products"));
}

#[tokio::test]
async fn default_qualified_insert_into_read_only_source_rejected() {
    // DataFusion resolves `public.products` / `datafusion.public.products`
    // to the same read-only `products` table — qualifying must not bypass
    // the access-mode gate.
    for table in ["public.products", "datafusion.public.products"] {
        let resp = post_query(
            make_state(),
            json!({"sql": format!("INSERT INTO {table} (id, brand) VALUES (6, 'LG')")}),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "table: {table}");
        let body = body_to_json(resp).await;
        assert_eq!(body["error_type"], json!("sql_validation_error"));
    }
}

#[tokio::test]
async fn insert_into_read_write_source_allowed() {
    let state = make_state();
    let resp = post_query(
        state.clone(),
        json!({"sql": "INSERT INTO scratch (id) VALUES (99)"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    assert_eq!(body["success"], json!(true));
    assert_eq!(body["truncated"], json!(false));

    // The write is visible to a follow-up query on the same state.
    let resp = post_query(
        state,
        json!({"sql": "SELECT count(*) AS c FROM scratch WHERE id = 99"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    assert_eq!(body["data"][0]["c"], json!(1));
}

// ---------------------------------------------------------------------------
// Execution errors → 500

#[tokio::test]
async fn unknown_table_is_execution_error() {
    let resp = post_query(make_state(), json!({"sql": "SELECT * FROM no_such_table"})).await;
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("query_execution_error"));
}

// ---------------------------------------------------------------------------
// Auth

#[tokio::test]
async fn missing_session_returns_401_when_auth_enabled() {
    unsafe {
        std::env::set_var("AUTH_SECRET", "test-secret-that-is-at-least-32-characters!");
        std::env::set_var("AUTH_DB_PATH", ":memory:");
        std::env::remove_var("AUTH_BASE_URL");
    }
    let layer = AuthLayer::build(&AuthMode::BetterAuthDieselSqlite)
        .await
        .unwrap();
    let mut state = make_state();
    state.auth_layer = layer;
    let resp = post_query(state, json!({"sql": "SELECT 1"})).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn explain_analyze_insert_into_read_only_rejected() {
    let resp = post_query(
        make_state(),
        json!({"sql": "EXPLAIN ANALYZE INSERT INTO products (id, brand) VALUES (6, 'LG')"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("sql_validation_error"));
}

#[tokio::test]
async fn select_from_auth_schema_rejected() {
    // `auth.sessions` holds live bearer tokens; ad-hoc SQL must never be
    // able to read the auth schema, even for authenticated callers.
    let resp = post_query(
        make_state(),
        json!({"sql": "SELECT token FROM auth.sessions"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("sql_validation_error"));
    assert_eq!(body["details"]["schema"], json!("auth"));
}

#[tokio::test]
async fn auth_schema_in_subquery_rejected() {
    let resp = post_query(
        make_state(),
        json!({"sql": "SELECT * FROM (SELECT token FROM auth.sessions) t"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("sql_validation_error"));
}

#[tokio::test]
async fn merge_rejected_by_allowlist() {
    let resp = post_query(
        make_state(),
        json!({"sql": "MERGE INTO scratch USING products ON scratch.id = products.id \
                       WHEN MATCHED THEN UPDATE SET id = 0"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("sql_validation_error"));
}

#[tokio::test]
async fn brace_containing_literal_is_not_mangled() {
    // Ad-hoc SQL is not a pipeline template: `{...}` inside string literals
    // must survive validation and execution untouched.
    let resp = post_query(
        make_state(),
        json!({"sql": r#"SELECT 'a{b' AS "c}d" FROM products LIMIT 1"#}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    assert_eq!(body["data"][0]["c}d"], json!("a{b"));
}

#[tokio::test]
async fn prepare_insert_into_read_only_rejected() {
    let resp = post_query(
        make_state(),
        json!({"sql": "PREPARE p AS INSERT INTO products (id, brand) VALUES (6, 'LG')"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("sql_validation_error"));
}
