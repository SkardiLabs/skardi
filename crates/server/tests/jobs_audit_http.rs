//! HTTP integration tests for job-submission auditing (`POST
//! /jobs/:name/run`) — mirrors `pipeline_audit_http.rs`'s coverage of the
//! pipeline path, but exercises the jobs path: record-before-submit, the
//! shared `x-skardi-session-id` header, the job_run_id stamp on success, and
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
use skardi_server::server::{AppState, configure_routes, repair_lost_job_correlations};

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
            mcp_allowed_hosts: vec![],
        },
    };
    let state = AppState::new(
        config,
        engine,
        Arc::clone(&ctx),
        AuthLayer::None,
        executor,
        query_audit,
        Default::default(),
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
async fn submission_is_audited_with_session_and_job_run_id() {
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
    assert_eq!(row["job_run_id"], json!(run_id));
    // #206's identity envelope owns `run_id` — the caller's own run, not the
    // job run this submission produced. The bridge stamp must not touch it.
    assert!(row["run_id"].is_null());
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
    // Same discriminator the pipeline path supplies. `error_type` is shared
    // with genuine job-parameter rejections on this endpoint, so
    // `details.header` is the only machine-readable way a client can tell a
    // bad session header from a bad job parameter.
    assert_eq!(body["error_type"], json!("parameter_validation_error"));
    assert_eq!(body["details"]["header"], json!("x-skardi-session-id"));
    assert_eq!(store.count().await.unwrap(), 0);
}

/// The docs promise the header is validated whether or not auditing is
/// enabled; this pins the audit-off half of that promise (mirrors
/// `pipeline_audit_http.rs`'s `session_header_is_validated_even_with_auditing_off`).
#[tokio::test]
async fn session_header_is_validated_even_with_auditing_off() {
    let (state, _tmp) = make_app_state(None).await;
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
/// human-readable message), and a null `job_run_id` because no run was ever
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
    assert!(row["job_run_id"].is_null());

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

// ---------------------------------------------------------------------------
// The submission bridge: durable in `job_runs`, best-effort in `query_audit`

#[tokio::test]
async fn submission_bridge_points_both_ways() {
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;

    let resp =
        submit_with_headers(&state, &[("x-skardi-session-id", "sess-bridge")], json!({})).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let run_id = body_to_json(resp).await["run_id"]
        .as_str()
        .unwrap()
        .to_string();
    wait_for_terminal(&state, &run_id).await;

    let rows = store.list_by_session("sess-bridge").await.unwrap();
    assert_eq!(rows.len(), 1);
    let audit_id = rows[0]["id"].as_str().unwrap();

    // Forward: audit row -> run. Stamped after `executor.submit` returned,
    // so this half is best-effort.
    assert_eq!(rows[0]["job_run_id"], json!(run_id));

    // Reverse: run -> audit row. Written in the INSERT that created the run,
    // so this half is durable the moment the run exists.
    let executor = state.jobs.clone().unwrap();
    let run = executor.store().get_run(&run_id).await.unwrap().unwrap();
    assert_eq!(
        run.submission_id.as_deref(),
        Some(audit_id),
        "the run does not point back at its audit row"
    );
}

#[tokio::test]
async fn correlation_survives_a_lost_forward_stamp() {
    // The failure this whole change exists for: `record_job_outcome` fails,
    // times out, or the process dies before it lands, so the audit row keeps
    // `job_run_id = NULL` and reconciles to `unknown`. Before the reverse pointer
    // that lost the correlation permanently — `job_runs` carried neither a
    // session id nor an audit-row id to rebuild from.
    //
    // Reproduced at the executor seam rather than over HTTP, because the
    // point is precisely that no forward stamp is ever written: the handler
    // would race to write one.
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    let executor = state.jobs.clone().unwrap();

    let audit_id = store
        .record_job_submitted(TEST_JOB_NAME, TEST_JOB_VERSION, Some("sess-lost"))
        .await
        .unwrap();
    let run_id = executor
        .submit(TEST_JOB_NAME, HashMap::new(), Some(&audit_id))
        .await
        .unwrap();

    // No `record_job_outcome` call — then a restart reconciles the orphan.
    store.reconcile_orphaned("simulated crash").await.unwrap();

    let rows = store.list_by_session("sess-lost").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["status"], json!("unknown"));
    assert!(
        rows[0]["job_run_id"].is_null(),
        "precondition: the forward stamp must be missing"
    );

    // Recoverable anyway, from the half that was durable.
    let recovered = executor
        .store()
        .get_run_by_submission_id(rows[0]["id"].as_str().unwrap())
        .await
        .unwrap()
        .expect("the correlation was lost");
    assert_eq!(recovered.id, run_id);
    assert_eq!(recovered.job_name, TEST_JOB_NAME);
}

#[tokio::test]
async fn the_startup_repair_pass_relinks_the_row_an_auditor_reads() {
    // `correlation_survives_a_lost_forward_stamp` proves the linkage is still
    // *on disk*. This proves something stronger and is the point of the whole
    // change: the row an auditor actually reads gets fixed, without an operator
    // holding both SQLite files open.
    //
    // `record_job_outcome` cannot do it — its `WHERE status = 'started'` guard
    // means that once `reconcile_orphaned` has rewritten the row to `unknown`,
    // no later well-behaved write can ever stamp it.
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    let executor = state.jobs.clone().unwrap();

    let audit_id = store
        .record_job_submitted(TEST_JOB_NAME, TEST_JOB_VERSION, Some("sess-repair"))
        .await
        .unwrap();
    let run_id = executor
        .submit(TEST_JOB_NAME, HashMap::new(), Some(&audit_id))
        .await
        .unwrap();

    // The crash: no outcome recorded, then a restart reconciles the orphan.
    store.reconcile_orphaned("simulated crash").await.unwrap();
    let before = store.get(&audit_id).await.unwrap().unwrap();
    assert_eq!(before["status"], json!("unknown"));
    assert!(
        before["job_run_id"].is_null(),
        "precondition: the forward stamp must be missing"
    );

    // The next boot.
    let repaired = repair_lost_job_correlations(&store, executor.store().as_ref())
        .await
        .unwrap();
    assert_eq!(repaired, 1);

    let after = store.get(&audit_id).await.unwrap().unwrap();
    assert_eq!(
        after["job_run_id"],
        json!(run_id),
        "the ledger row was not re-linked: {after}"
    );
    // The outcome genuinely was never observed, so `unknown` stays the truth;
    // only the linkage is recovered.
    assert_eq!(after["status"], json!("unknown"));
    assert_eq!(after["session_id"], json!("sess-repair"));

    // Idempotent: a second boot finds nothing left to do.
    assert_eq!(
        repair_lost_job_correlations(&store, executor.store().as_ref())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn the_repair_pass_leaves_a_submission_that_never_created_a_run_alone() {
    // A candidate row with no matching run means `submit` failed before
    // `create_run`. `job_runs` is never pruned, so the miss is positive
    // evidence of "never ran" — a different fact from "ran, linkage lost", and
    // the pass must not blur them.
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    let executor = state.jobs.clone().unwrap();

    let audit_id = store
        .record_job_submitted(TEST_JOB_NAME, TEST_JOB_VERSION, None)
        .await
        .unwrap();
    store.reconcile_orphaned("simulated crash").await.unwrap();

    assert_eq!(
        repair_lost_job_correlations(&store, executor.store().as_ref())
            .await
            .unwrap(),
        0
    );
    let row = store.get(&audit_id).await.unwrap().unwrap();
    assert!(row["job_run_id"].is_null());
    assert_eq!(row["status"], json!("unknown"));
}

#[tokio::test]
async fn the_repair_pass_cannot_touch_a_submission_still_in_flight() {
    // The pass runs at startup, but nothing stops an operator-triggered or
    // future periodic call, so it must not race a live submission: a `started`
    // row is `record_job_outcome`'s to settle, not the repair's.
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    let executor = state.jobs.clone().unwrap();

    let audit_id = store
        .record_job_submitted(TEST_JOB_NAME, TEST_JOB_VERSION, None)
        .await
        .unwrap();
    executor
        .submit(TEST_JOB_NAME, HashMap::new(), Some(&audit_id))
        .await
        .unwrap();

    // No `reconcile_orphaned`: the row is still `started`.
    assert_eq!(
        repair_lost_job_correlations(&store, executor.store().as_ref())
            .await
            .unwrap(),
        0,
        "a live submission was treated as a lost one"
    );
    assert_eq!(
        store.get(&audit_id).await.unwrap().unwrap()["status"],
        json!("started")
    );
}

#[tokio::test]
async fn unaudited_server_leaves_submission_id_null() {
    // No ledger means no token to carry; the column must stay NULL rather
    // than pick up some stand-in that would collide in the reverse lookup.
    let (state, _tmp) = make_app_state(None).await;
    let resp = submit_with_headers(&state, &[], json!({})).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    let run_id = body["run_id"].as_str().unwrap().to_string();

    let executor = state.jobs.clone().unwrap();
    let run = executor.store().get_run(&run_id).await.unwrap().unwrap();
    assert!(run.submission_id.is_none());
}

/// GET `uri` and return the parsed body.
async fn get_json(state: &AppState, uri: &str) -> Value {
    let app = configure_routes(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "GET {uri}");
    body_to_json(resp).await
}

#[tokio::test]
async fn the_token_resolves_a_run_on_the_way_in_and_is_never_returned() {
    // The reverse pointer has to be reachable over HTTP, or the operator
    // procedure is "open the SQLite file". It is reachable as a filter, not as
    // a field: `submission_id` is a `query_audit` primary key, and the ledger
    // is a deliberately access-restricted artifact (0600 on its file), while
    // `GET /jobs/runs` returns every run to any authenticated session — jobs
    // auth is authentication, not authorization. Emitting the token would
    // publish one caller's audit-row id to every other caller.
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;

    let resp = submit_with_headers(&state, &[("x-skardi-session-id", "sess-api")], json!({})).await;
    let run_id = body_to_json(resp).await["run_id"]
        .as_str()
        .unwrap()
        .to_string();
    let detail = wait_for_terminal(&state, &run_id).await;
    let audit_id = store.list_by_session("sess-api").await.unwrap()[0]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Not on the way out — neither from the detail route nor the list route.
    assert!(
        detail.get("submission_id").is_none(),
        "run detail leaked the audit row id: {detail}"
    );
    let listed = get_json(&state, "/jobs/runs").await;
    assert!(
        listed["runs"][0].get("submission_id").is_none(),
        "the runs list leaked the audit row id: {listed}"
    );

    // On the way in: an exact-token lookup resolving the one matching run.
    let found = get_json(&state, &format!("/jobs/runs?submission_id={audit_id}")).await;
    assert_eq!(found["count"], json!(1), "{found}");
    assert_eq!(found["runs"][0]["run_id"], json!(run_id));
}

#[tokio::test]
async fn an_unmatched_token_is_an_empty_result_not_an_error() {
    // With no pruning on `job_runs`, a miss is positive evidence that `submit`
    // never created a run — a fact worth returning cleanly rather than as a
    // 404 an operator has to disambiguate from a bad route.
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;

    let found = get_json(&state, "/jobs/runs?submission_id=audit-never-existed").await;
    assert_eq!(found["count"], json!(0));
    assert_eq!(found["runs"], json!([]));
    assert_eq!(found["success"], json!(true));
}

#[tokio::test]
async fn the_token_lookup_finds_a_run_that_has_fallen_off_the_recent_window() {
    // The case the filter exists for. `list_job_runs` clamps to 500 with no
    // offset, so on a server busy enough for the concurrent-submission
    // ambiguity `job_run_id` was introduced to remove, the run an operator
    // needs during an incident is exactly the one no longer in the window.
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    let executor = state.jobs.clone().unwrap();
    let jobs = executor.store();

    let audit_id = store
        .record_job_submitted(TEST_JOB_NAME, TEST_JOB_VERSION, None)
        .await
        .unwrap();
    let wanted = executor
        .submit(TEST_JOB_NAME, HashMap::new(), Some(&audit_id))
        .await
        .unwrap();

    // Bury it under more runs than the list route will ever return.
    for _ in 0..3 {
        executor
            .submit(TEST_JOB_NAME, HashMap::new(), None)
            .await
            .unwrap();
    }
    let listed = get_json(&state, "/jobs/runs?limit=2").await;
    assert_eq!(listed["count"], json!(2), "precondition: a bounded window");

    let found = get_json(&state, &format!("/jobs/runs?submission_id={audit_id}")).await;
    assert_eq!(found["count"], json!(1));
    assert_eq!(found["runs"][0]["run_id"], json!(wanted));
    // `job` and `limit` cannot narrow a token lookup away — one token names at
    // most one run.
    let found = get_json(
        &state,
        &format!("/jobs/runs?submission_id={audit_id}&limit=1&job=nope"),
    )
    .await;
    assert_eq!(found["runs"][0]["run_id"], json!(wanted));
    drop(jobs);
}
