//! The startup half of the audit bridge: that `setup_app_state` actually runs
//! the correlation repair, on the real on-disk ledgers, in the right order.
//!
//! `jobs_audit_http.rs` drives `repair_lost_job_correlations` directly, which
//! pins what the pass *does*. It cannot catch the call site going missing —
//! and a repair pass nothing calls leaves the ledger exactly as broken as no
//! pass at all. This boots twice over the same two files instead.

use skardi::jobs::{JobRun, JobRunStatus};
use skardi_server::config::{CliArgs, load_server_config};
use skardi_server::query_audit::{QueryAuditStatus, QueryAuditStore};
use skardi_server::server::{AppState, setup_app_state};
use std::path::{Path, PathBuf};

const JOB_YAML: &str = r#"
kind: job
metadata:
  name: "nightly"
  version: "1.0.0"
spec:
  query: |
    SELECT 1 AS id
  destination:
    table: "dest"
    mode: append
"#;

fn args(jobs_path: PathBuf, jobs_db: PathBuf, audit_db: PathBuf) -> CliArgs {
    CliArgs {
        pipeline_path: None,
        jobs_path: Some(jobs_path),
        jobs_db_path: Some(jobs_db),
        ctx_file: None,
        semantics_path: None,
        port: 8080,
        query_audit_db: Some(audit_db),
        query_audit_retention_days: None,
        mcp_allowed_hosts: vec![],
    }
}

async fn boot(jobs_path: &Path, jobs_db: &Path, audit_db: &Path) -> AppState {
    let config = load_server_config(args(
        jobs_path.to_path_buf(),
        jobs_db.to_path_buf(),
        audit_db.to_path_buf(),
    ))
    .await
    .expect("config loads");
    setup_app_state(config).await.expect("startup")
}

#[tokio::test]
async fn a_restart_repairs_a_correlation_lost_before_the_previous_shutdown() {
    let dir = tempfile::TempDir::new().unwrap();
    let jobs_yaml = dir.path().join("nightly.yaml");
    std::fs::write(&jobs_yaml, JOB_YAML).unwrap();
    let jobs_db = dir.path().join("jobs.db");
    let audit_db = dir.path().join("audit.db");

    // ---- first boot: plant exactly the state a crash in the stamp window
    // leaves behind — a real run carrying the token, and an audit row that
    // never got its forward pointer.
    let audit_id = {
        let state = boot(&jobs_yaml, &jobs_db, &audit_db).await;
        let audit = state.query_audit.clone().expect("audit ledger configured");
        let jobs = state.jobs.clone().expect("jobs enabled").store();

        let audit_id = audit
            .record_job_submitted("nightly", "1.0.0", Some("sess-crash"))
            .await
            .unwrap();
        jobs.create_run(&JobRun {
            id: "run-crashed".to_string(),
            job_name: "nightly".to_string(),
            parameters: "{}".to_string(),
            status: JobRunStatus::Succeeded,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            started_at: None,
            finished_at: None,
            rows_written: Some(1),
            snapshot_id: None,
            error: None,
            submission_id: Some(audit_id.clone()),
        })
        .await
        .unwrap();

        // No `record_job_outcome`: the process "dies" here.
        audit_id
    };

    // The row is still `started` on disk — the reconcile that turns it into a
    // repair candidate is itself part of the next startup.
    {
        let audit = QueryAuditStore::open(&audit_db).await.unwrap();
        let row = audit.get(&audit_id).await.unwrap().unwrap();
        assert_eq!(row["status"], serde_json::json!("started"));
        assert!(row["job_run_id"].is_null());
    }

    // ---- second boot: reconcile, then repair, both wired into startup.
    let state = boot(&jobs_yaml, &jobs_db, &audit_db).await;
    let audit = state.query_audit.clone().unwrap();
    let row = audit.get(&audit_id).await.unwrap().unwrap();

    assert_eq!(
        row["job_run_id"],
        serde_json::json!("run-crashed"),
        "startup did not repair the correlation: {row}"
    );
    // The outcome was never observed, so `unknown` stays the truth; the repair
    // recovers the linkage only.
    assert_eq!(
        row["status"],
        serde_json::json!(QueryAuditStatus::Unknown.as_str())
    );
    assert_eq!(row["session_id"], serde_json::json!("sess-crash"));
}

#[tokio::test]
async fn a_clean_restart_repairs_nothing_and_leaves_stamped_rows_alone() {
    let dir = tempfile::TempDir::new().unwrap();
    let jobs_yaml = dir.path().join("nightly.yaml");
    std::fs::write(&jobs_yaml, JOB_YAML).unwrap();
    let jobs_db = dir.path().join("jobs.db");
    let audit_db = dir.path().join("audit.db");

    let audit_id = {
        let state = boot(&jobs_yaml, &jobs_db, &audit_db).await;
        let audit = state.query_audit.clone().unwrap();
        let jobs = state.jobs.clone().unwrap().store();

        let audit_id = audit
            .record_job_submitted("nightly", "1.0.0", None)
            .await
            .unwrap();
        jobs.create_run(&JobRun {
            id: "run-ok".to_string(),
            job_name: "nightly".to_string(),
            parameters: "{}".to_string(),
            status: JobRunStatus::Succeeded,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            started_at: None,
            finished_at: None,
            rows_written: Some(1),
            snapshot_id: None,
            error: None,
            submission_id: Some(audit_id.clone()),
        })
        .await
        .unwrap();
        // This time the outcome *is* recorded, as it is on the happy path.
        audit
            .record_job_outcome(&audit_id, Some("run-ok"), QueryAuditStatus::Succeeded, None)
            .await
            .unwrap();
        audit_id
    };

    let state = boot(&jobs_yaml, &jobs_db, &audit_db).await;
    let row = state
        .query_audit
        .clone()
        .unwrap()
        .get(&audit_id)
        .await
        .unwrap()
        .unwrap();
    // A correctly stamped, terminal row must survive a restart untouched — the
    // repair's guards are what keep it out of the candidate set.
    assert_eq!(row["job_run_id"], serde_json::json!("run-ok"));
    assert_eq!(row["status"], serde_json::json!("succeeded"));
}
