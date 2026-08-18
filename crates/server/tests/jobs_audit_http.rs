//! HTTP integration tests for job-submission auditing (`POST
//! /jobs/:name/run`) — mirrors `pipeline_audit_http.rs`'s coverage of the
//! pipeline path, but exercises the jobs path: record-before-submit, the
//! shared `x-skardi-session-id` header, the run_id stamp on success, and
//! value-free recording (only `name@version` is ever written, never
//! parameter values).

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use axum::body::Body;
use axum::http::{HeaderValue, Request, StatusCode};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use skardi::jobs::{JobDefinition, JobExecutor, JobStore, SqliteJobStore};
use skardi::sources::DataSourceType;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tower::ServiceExt;

use skardi_server::auth::layer::AuthLayer;
use skardi_server::config::{CliArgs, ServerConfig};
use skardi_server::query_audit::QueryAuditStore;
use skardi_server::semantics::SemanticsRegistry;
use skardi_server::server::{AppState, configure_routes};

/// Name of the job registered by `make_app_state`. Bound as a `const` so
/// assertions and the YAML fixture below stay in sync.
const TEST_JOB_NAME: &str = "ingest-all";

/// Version set in the fixture job's `metadata.version`; ledger rows store
/// `sql = "<name>@<version>"`.
const TEST_JOB_VERSION: &str = "1.0.0";

/// Name of the second fixture job registered by `make_app_state`: its SQL
/// requires a `min_id` parameter that the submit-time param-validation step
/// checks for *inside* `executor.submit` — i.e. strictly after the
/// existence pre-check, header validation, and audit write the handler does
/// itself. Submitting with no parameters is therefore a deterministic,
/// post-audit-write rejection (`JobSubmitError::MissingParameters`),
/// exercising the `Err` arm of the outcome-stamp contract.
const REJECTING_JOB_NAME: &str = "ingest-filtered";

/// Version set in the rejecting fixture job's `metadata.version`.
const REJECTING_JOB_VERSION: &str = "1.0.0";

fn write_yaml(path: &std::path::Path, content: &str) {
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(content.as_bytes()).unwrap();
}

fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))]).unwrap()
}

/// Build an `AppState` with a working jobs executor + destination fixture
/// (cribbed from `jobs_http.rs`), and — when requested — an operator audit
/// ledger wired in (cribbed from `pipeline_audit_http.rs`'s `make_app_state`
/// param shape).
async fn make_app_state(query_audit: Option<Arc<QueryAuditStore>>) -> (AppState, TempDir) {
    let tmp = TempDir::new().unwrap();
    let ctx = Arc::new(SessionContext::new());
    ctx.register_batch("src", sample_batch()).unwrap();

    // Destination MemTable — DataFusion handles INSERT INTO for this.
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

    // A second job whose SQL requires a parameter (`min_id`) that the
    // caller never supplies in the rejection tests below — `executor.submit`
    // rejects it with `JobSubmitError::MissingParameters` *after* the
    // handler's own existence/header/audit-write steps have already run,
    // giving a deterministic post-audit-write failure to assert on.
    let rejecting_yaml_path = tmp.path().join("j2.yaml");
    write_yaml(
        &rejecting_yaml_path,
        r#"
kind: job
metadata:
  name: "ingest-filtered"
  version: "1.0.0"
spec:
  query: |
    SELECT id FROM src WHERE id >= {min_id}
  destination:
    table: "dest"
    mode: append
"#,
    );
    let rejecting_job = JobDefinition::load_from_file(&rejecting_yaml_path, Arc::clone(&ctx))
        .await
        .unwrap()
        .unwrap();
    jobs_map.insert(rejecting_job.name().to_string(), rejecting_job);

    let store = Arc::new(SqliteJobStore::open_in_memory().await.unwrap());
    let data_source_types: HashMap<String, DataSourceType> = HashMap::new();
    let executor = Some(Arc::new(JobExecutor::new(
        jobs_map.clone(),
        store as Arc<dyn JobStore>,
        Arc::clone(&ctx),
        data_source_types,
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
        AuthLayer::None,
        executor,
        query_audit,
    );
    (state, tmp)
}

async fn body_to_json(resp: axum::response::Response) -> Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

/// POST `body` to `/jobs/{job_name}/run`, attaching the given extra headers.
async fn submit_job_with_headers(
    state: &AppState,
    job_name: &str,
    headers: &[(&str, &str)],
    body: Value,
) -> axum::response::Response {
    let app = configure_routes(state.clone());
    let mut builder = Request::builder()
        .method("POST")
        .uri(format!("/jobs/{job_name}/run"))
        .header("content-type", "application/json");
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    app.oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap()
}

/// POST `body` to `/jobs/{TEST_JOB_NAME}/run`, attaching the given extra
/// headers.
async fn submit_with_headers(
    state: &AppState,
    headers: &[(&str, &str)],
    body: Value,
) -> axum::response::Response {
    submit_job_with_headers(state, TEST_JOB_NAME, headers, body).await
}

/// Poll `/jobs/runs/:run_id` until it reaches a terminal state, so
/// `record_job_outcome`'s async write has actually landed before assertions
/// read the ledger.
async fn wait_for_terminal(state: &AppState, run_id: &str) -> Value {
    let app = configure_routes(state.clone());
    for _ in 0..200 {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/jobs/runs/{run_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = body_to_json(resp).await;
        let status = body.get("status").unwrap().as_str().unwrap();
        if matches!(status, "succeeded" | "failed" | "cancelled") {
            return body;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("run never reached terminal state");
}

#[tokio::test]
async fn submission_is_audited_with_session_and_run_id() {
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    let resp = submit_with_headers(&state, &[("x-skardi-session-id", "sess-j")], json!({})).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    let run_id = body.get("run_id").unwrap().as_str().unwrap().to_string();

    wait_for_terminal(&state, &run_id).await;

    let rows = store.list_by_session("sess-j").await.unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row["statement_kind"], json!("job"));
    assert_eq!(row["sql"], format!("{TEST_JOB_NAME}@{TEST_JOB_VERSION}"));
    assert_eq!(row["status"], json!("succeeded"));
    assert_eq!(row["run_id"], json!(run_id));
    assert!(row["ai_context"].is_null());
    assert!(row["row_count"].is_null());
}

#[tokio::test]
async fn submission_without_header_audits_null_session() {
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    let resp = submit_with_headers(&state, &[], json!({})).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    let run_id = body.get("run_id").unwrap().as_str().unwrap().to_string();
    wait_for_terminal(&state, &run_id).await;

    assert_eq!(store.count().await.unwrap(), 1);
    assert!(store.list_by_session("").await.unwrap().is_empty());
}

#[tokio::test]
async fn no_store_configured_still_submits() {
    let (state, _tmp) = make_app_state(None).await;
    let resp = submit_with_headers(&state, &[], json!({})).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn unknown_job_with_malformed_header_is_404_and_records_nothing() {
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    let app = configure_routes(state.clone());
    let request = Request::builder()
        .method("POST")
        .uri("/jobs/no-such-job/run")
        .header("content-type", "application/json")
        .header("x-skardi-session-id", "")
        .body(Body::from(json!({}).to_string()))
        .unwrap();
    let resp = app.oneshot(request).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert_eq!(store.count().await.unwrap(), 0);
}

#[tokio::test]
async fn malformed_header_on_real_job_is_400_and_records_nothing() {
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    let resp = submit_with_headers(
        &state,
        &[("x-skardi-session-id", "x".repeat(201).as_str())],
        json!({}),
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
    assert_eq!(store.count().await.unwrap(), 0);
}

#[tokio::test]
async fn non_visible_ascii_session_header_on_real_job_is_400() {
    // Cannot be built through `submit_with_headers` — `Request::builder`
    // refuses the bytes client-side — so this constructs the `HeaderValue`
    // directly, mirroring `pipeline_audit_http.rs`'s equivalent test.
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    let app = configure_routes(state.clone());
    let request = Request::builder()
        .method("POST")
        .uri(format!("/jobs/{TEST_JOB_NAME}/run"))
        .header("content-type", "application/json")
        .header(
            "x-skardi-session-id",
            HeaderValue::from_bytes(&[0xff]).unwrap(),
        )
        .body(Body::from(json!({}).to_string()))
        .unwrap();
    let resp = app.oneshot(request).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(store.count().await.unwrap(), 0);
}

#[tokio::test]
async fn audit_write_failure_is_503_and_job_is_not_submitted() {
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    store.close_for_test().await;

    let resp = submit_with_headers(&state, &[], json!({})).await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("query_audit_error"));

    // The "not submitted" half is directly observable: the jobs ledger has
    // no new run.
    let executor = state.jobs.clone().unwrap();
    let runs = executor.store().list_runs(None, 10).await.unwrap();
    assert!(
        runs.is_empty(),
        "job was submitted despite the audit-write failure: {runs:?}"
    );
}

#[tokio::test]
async fn parameter_values_never_reach_the_audit_row() {
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    let resp = submit_with_headers(
        &state,
        &[("x-skardi-session-id", "sess-pii")],
        json!({"canary": "PII-CANARY-77"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    let run_id = body.get("run_id").unwrap().as_str().unwrap().to_string();
    wait_for_terminal(&state, &run_id).await;

    let rows = store.list_by_session("sess-pii").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        !rows[0].to_string().contains("PII-CANARY-77"),
        "a parameter value reached the audit ledger: {}",
        rows[0]
    );
}

/// The `Err` half of "outcome stamped on both arms": a submission that
/// clears the handler's own existence pre-check, header validation, and
/// audit write, then gets rejected by `executor.submit` itself
/// (`JobSubmitError::MissingParameters`, since `ingest-filtered` requires
/// `min_id` and this submits none). Pins that the ledger row still gets a
/// terminal stamp — `failed`, the fixed `category()` string (not the
/// human-readable message), and a null `run_id` because no run was ever
/// created — and that the HTTP response keeps the endpoint's existing
/// rejection contract (400, `missing_parameters`) unchanged.
#[tokio::test]
async fn submit_rejection_after_audit_write_is_recorded_failed() {
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    let resp = submit_job_with_headers(
        &state,
        REJECTING_JOB_NAME,
        &[("x-skardi-session-id", "sess-reject")],
        json!({}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("missing_parameters"));

    let rows = store.list_by_session("sess-reject").await.unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(
        row["sql"],
        format!("{REJECTING_JOB_NAME}@{REJECTING_JOB_VERSION}")
    );
    assert_eq!(row["status"], json!("failed"));
    assert_eq!(row["error"], json!("missing_parameters"));
    assert!(row["run_id"].is_null());

    // Confirms the executor never created a run for the rejected submission
    // — the same "not submitted" observability the audit-write-failure test
    // above uses.
    let executor = state.jobs.clone().unwrap();
    let runs = executor.store().list_runs(None, 10).await.unwrap();
    assert!(
        runs.is_empty(),
        "a run was created despite the rejection: {runs:?}"
    );
}
