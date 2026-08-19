//! HTTP integration tests for authentication on the `/jobs/*` endpoints.
//!
//! Until this change the jobs handlers performed no auth check at all, while
//! `/query` and `POST /:pipeline/execute` both called `require_session` as
//! their first statement. That gap became load-bearing once job submissions
//! started writing into the query-audit ledger (#219): it made
//! `POST /jobs/:name/run` the ledger's only unauthenticated write path, so an
//! unauthenticated caller could mint `session_id` values into rows the Learn
//! stage stitches on.
//!
//! These tests pin both halves: every endpoint 401s without a session when
//! auth is configured, and every endpoint is unaffected when it is not.

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use skardi::jobs::{JobDefinition, JobExecutor, JobStore, SqliteJobStore};
use skardi::sources::DataSourceType;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

use skardi_server::auth::layer::AuthLayer;
use skardi_server::auth::mode::AuthMode;
use skardi_server::config::{CliArgs, ServerConfig};
use skardi_server::semantics::SemanticsRegistry;
use skardi_server::server::{AppState, configure_routes};

const TEST_JOB_NAME: &str = "ingest-all";

/// Every route under `/jobs`, as (method, uri). Kept as one list so a route
/// added to `configure_routes` without an auth gate shows up as a missing
/// entry here rather than as silently uncovered surface.
fn all_job_routes() -> Vec<(&'static str, String)> {
    vec![
        ("GET", "/jobs".to_string()),
        ("POST", format!("/jobs/{TEST_JOB_NAME}/run")),
        ("GET", "/jobs/runs".to_string()),
        ("GET", "/jobs/runs/some-run-id".to_string()),
        ("POST", "/jobs/runs/some-run-id/cancel".to_string()),
    ]
}

fn write_yaml(path: &std::path::Path, content: &str) {
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(content.as_bytes()).unwrap();
}

fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))]).unwrap()
}

/// `AppState` with a working jobs executor and the given auth layer.
async fn make_app_state(auth_layer: AuthLayer) -> (AppState, TempDir) {
    let tmp = TempDir::new().unwrap();
    let ctx = Arc::new(SessionContext::new());
    ctx.register_batch("src", sample_batch()).unwrap();

    let dest_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let dest = MemTable::try_new(
        dest_schema.clone(),
        vec![vec![RecordBatch::new_empty(dest_schema)]],
    )
    .unwrap();
    ctx.register_table("dest", Arc::new(dest)).unwrap();

    let yaml_path = tmp.path().join("j.yaml");
    write_yaml(
        &yaml_path,
        r#"
kind: job
metadata:
  name: "ingest-all"
  version: "1.0.0"
spec:
  query: |
    SELECT id FROM src
  destination:
    table: "dest"
    mode: append
"#,
    );
    let job = JobDefinition::load_from_file(&yaml_path, Arc::clone(&ctx))
        .await
        .unwrap()
        .unwrap();
    let mut jobs_map = HashMap::new();
    jobs_map.insert(job.name().to_string(), job);

    let store = Arc::new(SqliteJobStore::open_in_memory().await.unwrap());
    let executor = Some(Arc::new(JobExecutor::new(
        jobs_map.clone(),
        store as Arc<dyn JobStore>,
        Arc::clone(&ctx),
        HashMap::<String, DataSourceType>::new(),
        HashMap::new(),
    )));

    let engine = Arc::new(skardi::engine::datafusion::DataFusionEngine::new_with_arc(
        Arc::clone(&ctx),
    ));
    let config = ServerConfig {
        pipelines: HashMap::new(),
        jobs: jobs_map,
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
        auth_layer,
        executor,
        None,
        Default::default(),
    );
    (state, tmp)
}

async fn better_auth_layer() -> AuthLayer {
    unsafe {
        std::env::set_var("AUTH_SECRET", "test-secret-that-is-at-least-32-characters!");
        std::env::set_var("AUTH_DB_PATH", ":memory:");
        std::env::remove_var("AUTH_BASE_URL");
    }
    AuthLayer::build(&AuthMode::BetterAuthDieselSqlite)
        .await
        .unwrap()
}

async fn body_to_json(resp: axum::response::Response) -> Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

async fn call(
    state: &AppState,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
) -> axum::response::Response {
    let app = configure_routes(state.clone());
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    app.oneshot(builder.body(Body::from(json!({}).to_string())).unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn every_jobs_route_401s_without_a_session() {
    let (state, _tmp) = make_app_state(better_auth_layer().await).await;
    for (method, uri) in all_job_routes() {
        let resp = call(&state, method, &uri, &[]).await;
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {uri} was not gated"
        );
        let body = body_to_json(resp).await;
        // The jobs endpoint family answers in one envelope, so the auth
        // rejection must not arrive in `/query`'s shape.
        assert_eq!(body["error_type"], json!("unauthorized"), "{method} {uri}");
        assert_eq!(body["success"], json!(false), "{method} {uri}");
        assert!(body["timestamp"].is_string(), "{method} {uri}");
    }
}

#[tokio::test]
async fn every_jobs_route_401s_with_an_invalid_bearer_token() {
    let (state, _tmp) = make_app_state(better_auth_layer().await).await;
    for (method, uri) in all_job_routes() {
        let resp = call(&state, method, &uri, &[("authorization", "Bearer bogus")]).await;
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {uri} accepted a bogus token"
        );
    }
}

#[tokio::test]
async fn auth_precedes_job_existence_and_subsystem_checks() {
    // An unauthenticated caller must not be able to distinguish "this job
    // exists" (404 vs 200) or "jobs are enabled here" (503) by reading the
    // status code — so the gate sits ahead of both checks rather than inside
    // the precedence ladder they form.
    let (state, _tmp) = make_app_state(better_auth_layer().await).await;

    let resp = call(&state, "POST", "/jobs/no-such-job/run", &[]).await;
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "unknown-job 404 leaked ahead of the auth gate"
    );

    let (mut jobless, _tmp2) = make_app_state(better_auth_layer().await).await;
    jobless.jobs = None;
    let resp = call(&jobless, "POST", &format!("/jobs/{TEST_JOB_NAME}/run"), &[]).await;
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "jobs-disabled 503 leaked ahead of the auth gate"
    );
}

#[tokio::test]
async fn no_auth_server_is_unaffected() {
    // `verify_session` short-circuits when no auth layer is configured, so
    // adding the gate must not change behaviour for deployments that never
    // enabled auth — the large majority today.
    let (state, _tmp) = make_app_state(AuthLayer::None).await;

    let resp = call(&state, "GET", "/jobs", &[]).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = call(&state, "POST", &format!("/jobs/{TEST_JOB_NAME}/run"), &[]).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    assert!(body["run_id"].is_string(), "no run was created: {body}");

    // A read path too, so the gate is not merely absent from the write one.
    let resp = call(&state, "GET", "/jobs/runs", &[]).await;
    assert_eq!(resp.status(), StatusCode::OK);
}
