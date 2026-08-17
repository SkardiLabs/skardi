//! HTTP integration tests for pipeline-execution auditing
//! (`POST /:name/execute`) — mirrors the `/query` audit coverage in
//! `query_http.rs`, but exercises the pipeline path: record-before-execute,
//! the `x-skardi-session-id` header, and value-free recording (only the
//! pipeline name is ever written, never parameter values).

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use axum::body::Body;
use axum::http::{HeaderValue, Request, StatusCode};
use datafusion::prelude::SessionContext;
use http_body_util::BodyExt;
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

/// Name of the second, intentionally-broken pipeline registered by
/// `make_app_state` — its SQL references a table that existed only long
/// enough for load-time schema inference and was then deregistered, so
/// execution fails inside the engine (not at parameter validation),
/// exercising the `query_execution_error` audit path.
const BROKEN_PIPELINE_NAME: &str = "broken-pipeline";

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

    // A second pipeline whose SQL targets a table that is present at load
    // time (schema inference needs it to exist) but removed immediately
    // after, so `engine.execute` fails at runtime rather than at parameter
    // validation — the fixture for `engine_failure_is_audited_as_failed`.
    ctx.register_batch("vanishing_table", products_batch())
        .unwrap();
    let broken_yaml_path = tmp.path().join("broken-pipeline.yaml");
    write_yaml(
        &broken_yaml_path,
        r#"
kind: pipeline
metadata:
  name: "broken-pipeline"
  version: "1.0.0"
  description: "References a table removed after load, to force an engine-execution failure"
spec:
  query: |
    SELECT id FROM vanishing_table
"#,
    );
    let broken_pipeline = StandardPipeline::load_from_file(&broken_yaml_path, Arc::clone(&ctx))
        .await
        .unwrap();
    pipelines.insert(broken_pipeline.name().to_string(), broken_pipeline);
    ctx.deregister_table("vanishing_table").unwrap();

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

/// POST an (empty) body to `/{BROKEN_PIPELINE_NAME}/execute`, attaching the
/// given extra headers. The broken pipeline takes no parameters, so an
/// empty JSON object is a complete, valid request body — any failure comes
/// from the engine, not parameter validation.
async fn execute_broken_pipeline(
    state: &AppState,
    headers: &[(&str, &str)],
) -> axum::response::Response {
    let app = configure_routes(state.clone());
    let mut builder = Request::builder()
        .method("POST")
        .uri(format!("/{BROKEN_PIPELINE_NAME}/execute"))
        .header("content-type", "application/json");
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    app.oneshot(builder.body(Body::from(json!({}).to_string())).unwrap())
        .await
        .unwrap()
}

/// Parse a response body as JSON. Only the failure-path tests need to
/// inspect `error_type`; the happy-path tests above read the audit ledger
/// instead.
async fn body_to_json(resp: axum::response::Response) -> Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
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
    // Deterministic fixture: Apple rows at 1299.0 and 999.0 pass the 1500.0
    // cap; the 2499.0 one doesn't. An exact count catches a handler that
    // records the wrong number, which "is a number" would wave past.
    assert_eq!(row["row_count"], json!(2));
    assert!(row["ai_context"].is_null());
}

/// The branch's central invariant, pinned end-to-end: no parameter value may
/// reach any column of the ledger row — not `sql`, not `error`, not a future
/// `ai_context` change. Greps the whole serialized row for a canary value so
/// the test survives refactors of which column would leak.
#[tokio::test]
async fn parameter_values_never_reach_the_ledger() {
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    let resp = execute_with_headers(
        &state,
        &[("x-skardi-session-id", "sess-pii")],
        json!({"brand": "PII-CANARY-42", "max_price": 1.0}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);

    let rows = store.list_by_session("sess-pii").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        !rows[0].to_string().contains("PII-CANARY-42"),
        "a parameter value reached the ledger: {}",
        rows[0]
    );
}

/// The `to_str` reject (a header value outside visible ASCII) can't be built
/// through `execute_with_headers` — `Request::builder().header()` refuses the
/// bytes client-side — so this constructs the `HeaderValue` directly.
#[tokio::test]
async fn non_visible_ascii_session_header_is_400_and_records_nothing() {
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    let app = configure_routes(state.clone());
    let request = Request::builder()
        .method("POST")
        .uri(format!("/{TEST_PIPELINE_NAME}/execute"))
        .header("content-type", "application/json")
        .header(
            "x-skardi-session-id",
            HeaderValue::from_bytes(&[0xff]).unwrap(),
        )
        .body(Body::from(valid_params().to_string()))
        .unwrap();
    let resp = app.oneshot(request).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("parameter_validation_error"));
    assert!(
        body["error"].as_str().unwrap().contains("visible ASCII"),
        "expected the ASCII reject, got {body}"
    );
    assert_eq!(store.count().await.unwrap(), 0);
}

/// The docs promise the header is validated whether or not auditing is
/// enabled; this pins the audit-off half of that promise.
#[tokio::test]
async fn session_header_is_validated_even_with_auditing_off() {
    let (state, _tmp) = make_app_state(None).await;
    let resp = execute_with_headers(
        &state,
        &[("x-skardi-session-id", "x".repeat(201).as_str())],
        valid_params(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("x-skardi-session-id"),
        "expected the session-header reject, got {body}"
    );
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

// ---------------------------------------------------------------------------
// Failure paths: an engine failure must still be audited as `failed`, a
// validation failure must record nothing, a malformed session header must
// be rejected before any pipeline work happens, and an unwritable audit
// store must fail closed (no execution) rather than silently skip auditing.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn engine_failure_is_audited_as_failed() {
    // The broken pipeline's SQL references a table that was deregistered
    // from the DataFusion context right after load, so the engine call
    // itself fails (past parameter validation, past the audit-started
    // write).
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    let resp = execute_broken_pipeline(&state, &[("x-skardi-session-id", "sess-f")]).await;
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let rows = store.list_by_session("sess-f").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["status"], "failed");
    // The ledger stores a fixed, value-free error kind — never the raw
    // engine error text, which could echo a caller-supplied parameter value
    // substituted directly into the SQL as a literal.
    assert_eq!(rows[0]["error"], "query_execution_error");
}

#[tokio::test]
async fn param_validation_failure_records_nothing() {
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    let resp = execute_with_headers(&state, &[], json!({})).await; // missing required param
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(store.count().await.unwrap(), 0);
}

#[tokio::test]
async fn malformed_session_header_is_400_and_records_nothing() {
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    for bad in [String::new(), "x".repeat(201)] {
        // The body must be one that would otherwise succeed, so the header is
        // the only possible reject cause — an invalid body 400s with the same
        // error_type at parameter validation, which would make this test pass
        // even with the header check deleted.
        let resp = execute_with_headers(
            &state,
            &[("x-skardi-session-id", bad.as_str())],
            valid_params(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = body_to_json(resp).await;
        assert_eq!(body["error_type"], json!("parameter_validation_error"));
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("x-skardi-session-id"),
            "expected the session-header reject, got {body}"
        );
    }
    assert_eq!(store.count().await.unwrap(), 0);
}

#[tokio::test]
async fn duplicate_session_header_is_400_and_records_nothing() {
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    // Valid body for the same reason as the malformed-header test above: the
    // header must be the only possible reject cause.
    let resp = execute_with_headers(
        &state,
        &[
            ("x-skardi-session-id", "sess-1"),
            ("x-skardi-session-id", "sess-2"),
        ],
        valid_params(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("parameter_validation_error"));
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("must not be sent more than once"),
        "expected the duplicate-header reject, got {body}"
    );
    assert_eq!(store.count().await.unwrap(), 0);
}

#[tokio::test]
async fn audit_write_failure_is_503_and_pipeline_does_not_run() {
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    store.close_for_test().await;
    // Must be a fully valid parameter set: parameter validation runs before
    // the audit-started write, so an invalid body (like the brief's
    // `{"limit": 5}`, which this fixture's pipeline doesn't accept) would
    // 400 out at validation and never reach the audit write this test
    // targets.
    let resp = execute_with_headers(&state, &[], valid_params()).await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("query_audit_error"));
}
