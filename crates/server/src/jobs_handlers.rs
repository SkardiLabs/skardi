//! Axum handlers for the jobs primitive.
//!
//! Four endpoints:
//!
//! * `POST /jobs/:name/run`         — submit a new run
//! * `GET  /jobs/runs/:run_id`      — poll a single run
//! * `GET  /jobs/runs`              — list recent runs (optional `?job=name`, `?limit=N`)
//! * `POST /jobs/runs/:run_id/cancel` — best-effort cancel
//!
//! The submit handler validates parameters + destination synchronously and
//! returns `{ run_id, status: "pending" }` immediately; the actual query
//! runs in a background Tokio task managed by [`skardi::jobs::JobExecutor`].

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use skardi::jobs::{JobRun, JobSubmitError};
use std::collections::HashMap;

use crate::query_audit::{QueryAuditStatus, QueryAuditStore};
use crate::server::AppState;
use crate::session_header::session_id_from_headers;

#[derive(Debug, Deserialize)]
pub struct SubmitRunRequest {
    /// Dynamic JSON parameters that match the job's request schema.
    /// Same shape as the pipeline-execute endpoint's request body.
    #[serde(default, flatten)]
    pub parameters: HashMap<String, Value>,
}

#[derive(Debug, Serialize)]
pub struct SubmitRunResponse {
    pub run_id: String,
    pub status: String,
}

/// Error body for every job endpoint. Mirrors the pipeline handler's
/// `{ success, error, error_type, details, timestamp }` shape so client
/// code can share parsing.
#[derive(Debug, Serialize)]
pub struct JobErrorResponse {
    pub success: bool,
    pub error: String,
    pub error_type: String,
    pub details: Option<Value>,
    pub timestamp: String,
}

fn error_json(msg: &str, kind: &str, details: Option<Value>) -> Json<JobErrorResponse> {
    Json(JobErrorResponse {
        success: false,
        error: msg.to_string(),
        error_type: kind.to_string(),
        details,
        timestamp: chrono::Utc::now().to_rfc3339(),
    })
}

fn submit_error_status(err: &JobSubmitError) -> StatusCode {
    match err {
        JobSubmitError::UnknownJob(_) => StatusCode::NOT_FOUND,
        JobSubmitError::MissingParameters(_)
        | JobSubmitError::UnsupportedParameter(_)
        | JobSubmitError::DbDestinationMissing { .. }
        | JobSubmitError::LakeDestinationMissing { .. }
        | JobSubmitError::SchemaMismatch { .. }
        | JobSubmitError::SqlPlanFailure { .. }
        | JobSubmitError::DestinationResolutionFailed { .. }
        | JobSubmitError::NonTransactionalDestination { .. } => StatusCode::BAD_REQUEST,
        JobSubmitError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Stamp the terminal outcome onto a job-submission audit record.
///
/// Mirrors `query_audit::finish_audit`'s policy: a failure (or timeout) here
/// cannot un-submit a job that already reached the executor, so it is logged
/// rather than surfaced — the row simply stays `started` and the next
/// startup reconciles it to `unknown`. No-op when auditing is off or when
/// the pre-submit write itself failed (no `audit_id`, which fails the
/// request closed before this point is ever reached).
async fn finish_job_audit(
    store: Option<&QueryAuditStore>,
    audit_id: Option<&str>,
    job_run_id: Option<&str>,
    status: QueryAuditStatus,
    error: Option<&str>,
) {
    let (Some(store), Some(id)) = (store, audit_id) else {
        return;
    };
    if let Err(e) = store
        .record_job_outcome(id, job_run_id, status, error)
        .await
    {
        tracing::error!("Failed to record job-audit outcome for {id}: {e}");
    }
}

fn job_run_to_json(run: &JobRun) -> Value {
    serde_json::json!({
        "run_id": run.id,
        "job": run.job_name,
        "status": run.status.as_str(),
        "parameters": serde_json::from_str::<Value>(&run.parameters).unwrap_or(Value::Null),
        "created_at": run.created_at,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "rows_written": run.rows_written,
        "snapshot_id": run.snapshot_id,
        "error": run.error,
    })
}

/// `POST /jobs/:name/run`
pub async fn submit_job_run(
    State(app_state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(req): Json<SubmitRunRequest>,
) -> Result<Json<SubmitRunResponse>, (StatusCode, Json<JobErrorResponse>)> {
    let Some(executor) = app_state.jobs.clone() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            error_json(
                "Jobs subsystem is not enabled on this server",
                "jobs_disabled",
                None,
            ),
        ));
    };

    // Existence + version pre-check, ahead of header validation: the reject
    // below would otherwise label a metric/log line with an arbitrary
    // caller-supplied URL segment (the metric-cardinality / status-precedence
    // lesson from #213 round 3, mirrored from `execute_pipeline_by_name`).
    // Post-lookup, `name` is bounded to configured jobs, and an unknown job
    // correctly 404s regardless of header shape. This check and
    // `executor.submit`'s own name resolution a few lines down read two
    // *different* maps: this one is `app_state.config` (a
    // `std::sync::RwLock<ServerConfig>`), the executor's is its own
    // `Arc<tokio::sync::RwLock<HashMap<String, JobDefinition>>>` built from
    // `config.jobs.clone()` at construction (see
    // `crates/skardi/src/jobs/executor.rs`). Both are startup snapshots of
    // the same job set, and today nothing writes to either map after
    // startup, so the two lookups can never actually disagree. If a future
    // hot-reload updates one map without the other, this pre-check and
    // `executor.submit` could resolve different versions of the same job —
    // the ledger might record a version the run didn't use, or this could
    // 404 a job the executor would still accept (or vice versa). That
    // divergence is that future feature's problem to solve, not something
    // this handler guards against today.
    let version = {
        let config = app_state.config.read().unwrap_or_else(|p| p.into_inner());
        match config.jobs.get(&name) {
            Some(def) => def.version().to_string(),
            None => {
                return Err((
                    StatusCode::NOT_FOUND,
                    error_json(
                        &format!("Job '{name}' not found"),
                        "unknown_job",
                        Some(serde_json::json!({ "job": name })),
                    ),
                ));
            }
        }
    };

    let session_id = session_id_from_headers(&headers).map_err(|msg| {
        (
            StatusCode::BAD_REQUEST,
            error_json(&msg, "parameter_validation_error", None),
        )
    })?;

    tracing::info!(
        session_id = session_id.as_deref().unwrap_or_default(),
        "Received submit request for job '{}'",
        name
    );

    // Record-before-submit, fail-closed: a write failure here means the job
    // is never handed to the executor. An audited server must not run
    // anything it cannot account for.
    let audit_id = match &app_state.query_audit {
        Some(store) => match store
            .record_job_submitted(&name, &version, session_id.as_deref())
            .await
        {
            Ok(id) => Some(id),
            Err(e) => {
                tracing::error!("Job audit write failed; refusing to submit: {e}");
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    error_json(
                        "Query auditing is enabled but the audit record could not be \
                         written; the job was not submitted",
                        "query_audit_error",
                        None,
                    ),
                ));
            }
        },
        None => None,
    };

    match executor.submit(&name, req.parameters).await {
        Ok(run_id) => {
            finish_job_audit(
                app_state.query_audit.as_deref(),
                audit_id.as_deref(),
                Some(&run_id),
                QueryAuditStatus::Succeeded,
                None,
            )
            .await;
            Ok(Json(SubmitRunResponse {
                run_id,
                status: "pending".to_string(),
            }))
        }
        Err(err) => {
            let status = submit_error_status(&err);
            let kind = err.category().to_string();
            let msg = err.to_string();
            let details = match &err {
                JobSubmitError::SchemaMismatch { table, details } => Some(serde_json::json!({
                    "table": table,
                    "diff": details,
                })),
                JobSubmitError::UnknownJob(job) => Some(serde_json::json!({ "job": job })),
                _ => None,
            };
            finish_job_audit(
                app_state.query_audit.as_deref(),
                audit_id.as_deref(),
                None,
                QueryAuditStatus::Failed,
                Some(&kind),
            )
            .await;
            Err((status, error_json(&msg, &kind, details)))
        }
    }
}

/// `GET /jobs/runs/:run_id`
pub async fn get_job_run(
    State(app_state): State<AppState>,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<JobErrorResponse>)> {
    let Some(executor) = app_state.jobs.clone() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            error_json("Jobs subsystem is not enabled", "jobs_disabled", None),
        ));
    };
    match executor.store().get_run(&run_id).await {
        Ok(Some(run)) => Ok(Json(job_run_to_json(&run))),
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            error_json(&format!("Run '{run_id}' not found"), "unknown_run", None),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            error_json(&e.to_string(), "internal_error", None),
        )),
    }
}

#[derive(Debug, Deserialize)]
pub struct ListRunsQuery {
    pub job: Option<String>,
    pub limit: Option<usize>,
}

/// `GET /jobs/runs?job=...&limit=...`
pub async fn list_job_runs(
    State(app_state): State<AppState>,
    Query(q): Query<ListRunsQuery>,
) -> Result<Json<Value>, (StatusCode, Json<JobErrorResponse>)> {
    let Some(executor) = app_state.jobs.clone() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            error_json("Jobs subsystem is not enabled", "jobs_disabled", None),
        ));
    };
    let limit = q.limit.unwrap_or(50).clamp(1, 500);
    match executor.store().list_runs(q.job.as_deref(), limit).await {
        Ok(runs) => {
            let body: Vec<Value> = runs.iter().map(job_run_to_json).collect();
            Ok(Json(serde_json::json!({
                "success": true,
                "runs": body,
                "count": runs.len(),
            })))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            error_json(&e.to_string(), "internal_error", None),
        )),
    }
}

/// `POST /jobs/runs/:run_id/cancel` — returns `cancelled: true` when the
/// flag was set, `false` when the run is already terminal or unknown to
/// the executor.
pub async fn cancel_job_run(
    State(app_state): State<AppState>,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<JobErrorResponse>)> {
    let Some(executor) = app_state.jobs.clone() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            error_json("Jobs subsystem is not enabled", "jobs_disabled", None),
        ));
    };
    let cancelled = executor.cancel(&run_id).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            error_json(&e.to_string(), "internal_error", None),
        )
    })?;
    Ok(Json(serde_json::json!({
        "success": true,
        "run_id": run_id,
        "cancelled": cancelled,
    })))
}

/// `GET /jobs` — list registered job names with their destinations. Useful
/// for CLI discovery.
pub async fn list_jobs(
    State(app_state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, Json<JobErrorResponse>)> {
    let Some(executor) = app_state.jobs.clone() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            error_json("Jobs subsystem is not enabled", "jobs_disabled", None),
        ));
    };
    let names = executor.list_jobs().await;
    let mut items = Vec::with_capacity(names.len());
    for name in &names {
        if let Some(def) = executor.get_job(name).await {
            let params: Vec<String> = def.pipeline.request_schema.fields.keys().cloned().collect();
            items.push(serde_json::json!({
                "name": def.name(),
                "version": def.version(),
                "destination": {
                    "table": def.destination.table,
                    "mode": format!("{:?}", def.destination.mode).to_lowercase(),
                    "create_if_missing": def.destination.create_if_missing,
                },
                "parameters": params,
            }));
        }
    }
    Ok(Json(serde_json::json!({
        "success": true,
        "jobs": items,
        "count": items.len(),
    })))
}
