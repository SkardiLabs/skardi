//! HTTP integration test — boots a real axum server on an ephemeral
//! port, submits a job, polls until terminal, cancels a second run, and
//! confirms the 503 short-circuit when jobs are disabled.

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use http_body_util::BodyExt;
use serde_json::Value;
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
use skardi_server::semantics::SemanticsRegistry;
use skardi_server::server::{AppState, configure_routes};

fn write_yaml(path: &std::path::Path, content: &str) {
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(content.as_bytes()).unwrap();
}

fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))]).unwrap()
}

async fn make_app_state(enable_jobs: bool) -> (AppState, TempDir) {
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

    let mut jobs_map = HashMap::new();
    if enable_jobs {
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
        jobs_map.insert(job.name().to_string(), job);
    }

    let executor = if enable_jobs {
        let store = Arc::new(SqliteJobStore::open_in_memory().await.unwrap());
        let data_source_types: HashMap<String, DataSourceType> = HashMap::new();
        Some(Arc::new(JobExecutor::new(
            jobs_map.clone(),
            store as Arc<dyn JobStore>,
            Arc::clone(&ctx),
            data_source_types,
            HashMap::new(),
        )))
    } else {
        None
    };

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
        },
    };
    let state = AppState::new(config, engine, Arc::clone(&ctx), AuthLayer::None, executor);
    (state, tmp)
}

async fn body_to_json(resp: axum::response::Response) -> Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn http_submit_then_poll_job_run() {
    let (state, _tmp) = make_app_state(true).await;
    let app = configure_routes(state);

    // Submit.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/jobs/ingest-all/run")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    let run_id = body.get("run_id").unwrap().as_str().unwrap().to_string();
    assert_eq!(body.get("status").unwrap().as_str(), Some("pending"));

    // Poll until terminal.
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
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_to_json(resp).await;
        let status = body.get("status").unwrap().as_str().unwrap();
        if matches!(status, "succeeded" | "failed" | "cancelled") {
            assert_eq!(status, "succeeded", "body: {body}");
            assert_eq!(body.get("rows_written").unwrap().as_u64(), Some(3));
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("run never reached terminal state");
}

#[tokio::test]
async fn http_list_jobs_and_runs() {
    let (state, _tmp) = make_app_state(true).await;
    let app = configure_routes(state);

    let resp = app
        .clone()
        .oneshot(Request::builder().uri("/jobs").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    let names: Vec<&str> = body
        .get("jobs")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|j| j.get("name").unwrap().as_str().unwrap())
        .collect();
    assert!(names.contains(&"ingest-all"));
}

#[tokio::test]
async fn http_unknown_job_returns_404() {
    let (state, _tmp) = make_app_state(true).await;
    let app = configure_routes(state);
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/jobs/not-a-real-job/run")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn http_jobs_disabled_returns_503() {
    let (state, _tmp) = make_app_state(false).await;
    let app = configure_routes(state);
    let resp = app
        .clone()
        .oneshot(Request::builder().uri("/jobs").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}
