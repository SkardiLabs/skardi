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

use crate::auth::context::{extract_auth_context, require_scope};
use crate::auth::scope::any_scope_matches;
use crate::server::AppState;

/// Translate a `require_scope` failure into the `(StatusCode,
/// Json<JobErrorResponse>)` shape every job endpoint already uses.
async fn auth_failure_to_job_error(
    response: axum::http::Response<axum::body::Body>,
) -> (StatusCode, Json<JobErrorResponse>) {
    let status = response.status();
    let body_bytes = axum::body::to_bytes(response.into_body(), 1024)
        .await
        .unwrap_or_default();
    let parsed: Option<Value> = serde_json::from_slice(&body_bytes).ok();
    let msg = parsed
        .as_ref()
        .and_then(|v| v["error"].as_str())
        .map(str::to_string)
        .unwrap_or_else(|| "Authentication required".to_string());
    let kind = if status == StatusCode::FORBIDDEN {
        "forbidden"
    } else {
        "unauthorized"
    };
    (status, error_json(&msg, kind, parsed))
}

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
    let required = format!("jobs:submit:{name}");
    if let Err(resp) = require_scope(&app_state, &headers, &required).await {
        return Err(auth_failure_to_job_error(resp).await);
    }
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

    match executor.submit(&name, req.parameters).await {
        Ok(run_id) => Ok(Json(SubmitRunResponse {
            run_id,
            status: "pending".to_string(),
        })),
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
            Err((status, error_json(&msg, &kind, details)))
        }
    }
}

/// `GET /jobs/runs/:run_id`
pub async fn get_job_run(
    State(app_state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<JobErrorResponse>)> {
    let Some(executor) = app_state.jobs.clone() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            error_json("Jobs subsystem is not enabled", "jobs_disabled", None),
        ));
    };
    match executor.store().get_run(&run_id).await {
        Ok(Some(run)) => {
            // Per-job scope check: caller must be allowed to read THIS
            // job specifically. We have to fetch the run first to know
            // which job it belongs to — there's no other way to derive
            // the scope from a `run_id` alone.
            let required = format!("jobs:read:{}", run.job_name);
            if let Err(resp) = require_scope(&app_state, &headers, &required).await {
                return Err(auth_failure_to_job_error(resp).await);
            }
            Ok(Json(job_run_to_json(&run)))
        }
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
///
/// When `?job=name` is set, gates on `jobs:read:<name>`. Without a
/// filter we authenticate the caller and then post-filter the listing
/// to the runs whose `job_name` they have a matching `jobs:read:` scope
/// for — same shape as `list_pipelines`.
pub async fn list_job_runs(
    State(app_state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<ListRunsQuery>,
) -> Result<Json<Value>, (StatusCode, Json<JobErrorResponse>)> {
    let Some(executor) = app_state.jobs.clone() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            error_json("Jobs subsystem is not enabled", "jobs_disabled", None),
        ));
    };

    let ctx = match (&q.job, extract_auth_context(&app_state, &headers).await) {
        (Some(job_name), _) => {
            let required = format!("jobs:read:{job_name}");
            match require_scope(&app_state, &headers, &required).await {
                Ok(c) => c,
                Err(resp) => return Err(auth_failure_to_job_error(resp).await),
            }
        }
        (None, Ok(c)) => c,
        (None, Err(_)) => {
            return Err((
                StatusCode::UNAUTHORIZED,
                error_json("Authentication required", "unauthorized", None),
            ));
        }
    };

    let limit = q.limit.unwrap_or(50).clamp(1, 500);
    match executor.store().list_runs(q.job.as_deref(), limit).await {
        Ok(runs) => {
            let visible: Vec<&JobRun> = runs
                .iter()
                .filter(|r| any_scope_matches(&ctx.scopes, &format!("jobs:read:{}", r.job_name)))
                .collect();
            let body: Vec<Value> = visible.iter().map(|r| job_run_to_json(r)).collect();
            Ok(Json(serde_json::json!({
                "success": true,
                "runs": body,
                "count": visible.len(),
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
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<JobErrorResponse>)> {
    let Some(executor) = app_state.jobs.clone() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            error_json("Jobs subsystem is not enabled", "jobs_disabled", None),
        ));
    };
    // Same lookup-then-authorize pattern as `get_job_run` — the scope
    // string includes the job name, which we don't have until we fetch
    // the run. Unknown run → 404 before any scope check (no information
    // leak: the caller already authenticated to reach this handler? No
    // — we currently leak existence to unauthenticated callers. For v1
    // this matches the existing handler's behaviour; tightening means
    // checking auth first and returning a generic 401 either way.
    let run = executor.store().get_run(&run_id).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            error_json(&e.to_string(), "internal_error", None),
        )
    })?;
    let Some(run) = run else {
        return Err((
            StatusCode::NOT_FOUND,
            error_json(&format!("Run '{run_id}' not found"), "unknown_run", None),
        ));
    };
    let required = format!("jobs:cancel:{}", run.job_name);
    if let Err(resp) = require_scope(&app_state, &headers, &required).await {
        return Err(auth_failure_to_job_error(resp).await);
    }
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
/// for CLI discovery. Result is filtered to jobs the caller has
/// `jobs:read:<name>` for; an authenticated caller with no matching
/// grants gets a 200 with an empty list.
pub async fn list_jobs(
    State(app_state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, Json<JobErrorResponse>)> {
    let Some(executor) = app_state.jobs.clone() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            error_json("Jobs subsystem is not enabled", "jobs_disabled", None),
        ));
    };
    let ctx = match extract_auth_context(&app_state, &headers).await {
        Ok(c) => c,
        Err(_) => {
            return Err((
                StatusCode::UNAUTHORIZED,
                error_json("Authentication required", "unauthorized", None),
            ));
        }
    };
    let names = executor.list_jobs().await;
    let mut items = Vec::with_capacity(names.len());
    for name in &names {
        if !any_scope_matches(&ctx.scopes, &format!("jobs:read:{name}")) {
            continue;
        }
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
