//! The job executor — submit, spawn, cancel.
//!
//! Given a loaded `JobDefinition`, a shared `SessionContext`, and a map
//! of user-bound parameters, the executor:
//!
//! 1. validates parameter names and values against the inferred request
//!    schema;
//! 2. resolves the destination (Lance or DB) and runs the submit-time
//!    pre-flight (existence + schema diff);
//! 3. substitutes parameters into the SQL template (reusing the same
//!    scalar-literal substitution pipelines use);
//! 4. creates a `JobRun` row in `pending` state;
//! 5. spawns a background Tokio task that runs the query, streams batches
//!    to the destination, and updates the ledger row to its terminal
//!    state.
//!
//! Submit errors are surfaced synchronously (so the server can 400 / 404
//! before the row ever reaches `pending`). Runtime errors show up later
//! via the `failed` status + `error` field on the row.

use anyhow::{Context, Result};
use arrow::datatypes::Schema;
use chrono::Utc;
use datafusion::prelude::SessionContext;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

use super::definition::{Destination, DestinationMode, JobDefinition};
use super::destination::{JobDestination, JobDestinationKind, LanceDestination, SqlDmlDestination};
use super::store::{JobRun, JobRunStatus, JobStore};
use crate::pipeline::pipeline::Pipeline;
use crate::sources::DataSourceType;

/// Submit-time errors — everything the executor rejects before creating a
/// `JobRun` row. The server maps these to HTTP status codes.
#[derive(Debug, Error)]
pub enum JobSubmitError {
    #[error("Job '{0}' not found")]
    UnknownJob(String),

    #[error("Missing required parameter(s): {0}")]
    MissingParameters(String),

    #[error(
        "Unsupported parameter type for '{0}': only strings, numbers, booleans, and null are allowed"
    )]
    UnsupportedParameter(String),

    #[error(
        "Destination table '{table}' does not exist; create it with your DB's DDL before running the job"
    )]
    DbDestinationMissing { table: String },

    #[error("Destination dataset '{table}' does not exist and `create_if_missing` is false")]
    LakeDestinationMissing { table: String },

    #[error("Destination schema mismatch for '{table}': {details}")]
    SchemaMismatch { table: String, details: String },

    #[error("Failed to plan SQL for job '{job}': {source}")]
    SqlPlanFailure {
        job: String,
        #[source]
        source: anyhow::Error,
    },

    #[error("Unknown destination table '{table}' — could not resolve against the session context")]
    DestinationResolutionFailed { table: String },

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl JobSubmitError {
    /// Short, machine-readable category for the HTTP layer.
    pub fn category(&self) -> &'static str {
        match self {
            Self::UnknownJob(_) => "unknown_job",
            Self::MissingParameters(_) => "missing_parameters",
            Self::UnsupportedParameter(_) => "unsupported_parameter",
            Self::DbDestinationMissing { .. } => "destination_missing",
            Self::LakeDestinationMissing { .. } => "destination_missing",
            Self::SchemaMismatch { .. } => "schema_mismatch",
            Self::SqlPlanFailure { .. } => "sql_plan_failure",
            Self::DestinationResolutionFailed { .. } => "destination_resolution_failed",
            Self::Internal(_) => "internal_error",
        }
    }
}

/// Shared between submit-time and runtime halves — lets an explicit
/// `cancel` call reach the spawned task before it commits to the
/// destination.
#[derive(Debug, Default)]
struct CancelFlag {
    cancelled: AtomicBool,
}

impl CancelFlag {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

/// The core executor. Holds a shared DataFusion SessionContext (the same
/// one serving HTTP pipelines), the loaded job registry, the run ledger,
/// and a per-run cancel flag.
pub struct JobExecutor {
    jobs: Arc<tokio::sync::RwLock<HashMap<String, JobDefinition>>>,
    store: Arc<dyn JobStore>,
    session_ctx: Arc<SessionContext>,
    /// Map of source name → its configured DataSourceType, so the executor
    /// can decide whether a destination is a lake (Lance) or a DB target.
    data_source_types: Arc<HashMap<String, DataSourceType>>,
    cancel_flags: Arc<Mutex<HashMap<String, Arc<CancelFlag>>>>,
}

impl JobExecutor {
    pub fn new(
        jobs: HashMap<String, JobDefinition>,
        store: Arc<dyn JobStore>,
        session_ctx: Arc<SessionContext>,
        data_source_types: HashMap<String, DataSourceType>,
    ) -> Self {
        Self {
            jobs: Arc::new(tokio::sync::RwLock::new(jobs)),
            store,
            session_ctx,
            data_source_types: Arc::new(data_source_types),
            cancel_flags: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn store(&self) -> Arc<dyn JobStore> {
        Arc::clone(&self.store)
    }

    /// Listed job names, alphabetical.
    pub async fn list_jobs(&self) -> Vec<String> {
        let jobs = self.jobs.read().await;
        let mut v: Vec<String> = jobs.keys().cloned().collect();
        v.sort();
        v
    }

    /// Look up a loaded job definition by name — pipeline metadata and
    /// destination block both come back.
    pub async fn get_job(&self, name: &str) -> Option<JobDefinition> {
        let jobs = self.jobs.read().await;
        jobs.get(name).cloned()
    }

    /// Submit a run. Validates params + destination up front, persists a
    /// `pending` row, and spawns the background task. Returns the run id.
    pub async fn submit(
        &self,
        job_name: &str,
        params: HashMap<String, Value>,
    ) -> Result<String, JobSubmitError> {
        let job = {
            let jobs = self.jobs.read().await;
            jobs.get(job_name)
                .cloned()
                .ok_or_else(|| JobSubmitError::UnknownJob(job_name.to_string()))?
        };

        // Param validation.
        let expected: Vec<String> = job
            .pipeline
            .request_schema()
            .fields
            .keys()
            .cloned()
            .collect();
        let missing: Vec<&str> = expected
            .iter()
            .filter(|n| !params.contains_key(n.as_str()))
            .map(|s| s.as_str())
            .collect();
        if !missing.is_empty() {
            return Err(JobSubmitError::MissingParameters(missing.join(", ")));
        }

        // Substitute parameters into the SQL template. Using the same
        // sort-longest-first ordering as the pipeline handler so a shorter
        // name cannot corrupt a longer shared-prefix name during
        // `str::replace`.
        let mut expected_sorted = expected.clone();
        expected_sorted.sort_by_key(|s: &String| std::cmp::Reverse(s.len()));
        let (rendered_sql, bad_types) = substitute_sql_params(job.sql(), &expected_sorted, &params);
        if let Some(name) = bad_types {
            return Err(JobSubmitError::UnsupportedParameter(name));
        }

        // Destination pre-flight.
        let destination = self.resolve_destination(&job.destination).await?;

        let query_schema = self
            .session_ctx
            .sql(&rendered_sql)
            .await
            .map(|df| df.schema().as_arrow().clone())
            .map_err(|e| JobSubmitError::SqlPlanFailure {
                job: job_name.to_string(),
                source: anyhow::anyhow!("{e}"),
            })?;

        self.preflight(destination.as_ref(), &job.destination, &query_schema)
            .await?;

        // Persist the pending row.
        let run_id = uuid::Uuid::new_v4().simple().to_string();
        let params_json = sorted_json(&params);
        let now = chrono::Utc::now().to_rfc3339();
        let run = JobRun {
            id: run_id.clone(),
            job_name: job_name.to_string(),
            parameters: params_json,
            status: JobRunStatus::Pending,
            created_at: now,
            started_at: None,
            finished_at: None,
            rows_written: None,
            snapshot_id: None,
            error: None,
        };
        self.store
            .create_run(&run)
            .await
            .map_err(JobSubmitError::Internal)?;

        // Register cancel flag + spawn the background task.
        let cancel_flag = Arc::new(CancelFlag::default());
        {
            let mut flags = self.cancel_flags.lock().unwrap_or_else(|p| p.into_inner());
            flags.insert(run_id.clone(), Arc::clone(&cancel_flag));
        }

        let store = Arc::clone(&self.store);
        let session_ctx = Arc::clone(&self.session_ctx);
        let flags_map = Arc::clone(&self.cancel_flags);
        let run_id_for_task = run_id.clone();
        let mode = job.destination.mode;
        let timeout_ms = job.execution.timeout_ms;
        tokio::spawn(async move {
            run_job_task(
                run_id_for_task,
                store,
                session_ctx,
                destination,
                rendered_sql,
                mode,
                timeout_ms,
                cancel_flag,
                flags_map,
            )
            .await;
        });

        Ok(run_id)
    }

    /// Flip the cancel flag for a run — the spawned task will notice
    /// before it commits and transition the row to `cancelled`.
    pub async fn cancel(&self, run_id: &str) -> Result<bool> {
        let flag = {
            let flags = self.cancel_flags.lock().unwrap_or_else(|p| p.into_inner());
            flags.get(run_id).cloned()
        };
        match flag {
            Some(f) => {
                f.cancel();
                Ok(true)
            }
            None => Ok(false),
        }
    }

    async fn resolve_destination(
        &self,
        dest: &Destination,
    ) -> Result<Arc<dyn JobDestination>, JobSubmitError> {
        // First segment of a dotted identifier is the data-source name.
        let root = dest.table.split('.').next().unwrap_or(&dest.table);
        match self.data_source_types.get(root) {
            Some(DataSourceType::Lance) => {
                // Lake destinations expect the resolver to know the path.
                let path = self.lance_path_for(root).ok_or(
                    JobSubmitError::DestinationResolutionFailed {
                        table: dest.table.clone(),
                    },
                )?;
                Ok(Arc::new(LanceDestination::new(path)))
            }
            Some(DataSourceType::Postgres)
            | Some(DataSourceType::Mysql)
            | Some(DataSourceType::Sqlite)
            | Some(DataSourceType::Mongo)
            | Some(DataSourceType::Redis)
            | Some(DataSourceType::Seekdb) => Ok(Arc::new(SqlDmlDestination::new(
                Arc::clone(&self.session_ctx),
                dest.table.clone(),
            ))),
            Some(DataSourceType::Iceberg) => Err(JobSubmitError::Internal(anyhow::anyhow!(
                "Iceberg destinations are not supported in MVP — deferred to v1.1"
            ))),
            Some(DataSourceType::Csv) | Some(DataSourceType::Parquet) => {
                Err(JobSubmitError::Internal(anyhow::anyhow!(
                    "CSV/Parquet destinations are not supported — bare Parquet has no atomic commit; use Lance"
                )))
            }
            None => {
                // The table may be a bare name registered directly (e.g. a
                // MemTable in tests). Treat as DB so the pre-flight runs.
                Ok(Arc::new(SqlDmlDestination::new(
                    Arc::clone(&self.session_ctx),
                    dest.table.clone(),
                )))
            }
        }
    }

    fn lance_path_for(&self, _name: &str) -> Option<String> {
        // The DataSourceType map does not carry paths, so the executor
        // relies on the path-resolver callback installed by the caller.
        // In the MVP this is only set via the server, which knows the
        // full DataSource list. Tests register LanceDestination directly.
        None
    }

    async fn preflight(
        &self,
        destination: &dyn JobDestination,
        config: &Destination,
        query_schema: &Schema,
    ) -> Result<(), JobSubmitError> {
        let existing = destination
            .schema()
            .await
            .map_err(JobSubmitError::Internal)?;

        match existing {
            Some(schema) => diff_schemas(&config.table, query_schema, &schema).map_err(|details| {
                JobSubmitError::SchemaMismatch {
                    table: config.table.clone(),
                    details,
                }
            }),
            None => match destination.kind() {
                JobDestinationKind::Db => Err(JobSubmitError::DbDestinationMissing {
                    table: config.table.clone(),
                }),
                JobDestinationKind::Lake if !config.create_if_missing => {
                    Err(JobSubmitError::LakeDestinationMissing {
                        table: config.table.clone(),
                    })
                }
                JobDestinationKind::Lake => Ok(()),
            },
        }
    }

    /// Flip every non-terminal row in the ledger to `failed`. Called at
    /// server startup so a crash doesn't leave phantom `Running` rows.
    pub async fn reconcile_on_startup(&self) -> Result<usize> {
        self.store
            .reconcile_orphaned("server restarted before run completed")
            .await
    }
}

/// Substitute `{name}` placeholders in `sql` with JSON parameter values,
/// mirroring the pipeline handler's scalar-literal substitution. Returns
/// `(rendered_sql, Some(name))` on the first unsupported value; otherwise
/// `(rendered_sql, None)`.
fn substitute_sql_params(
    sql: &str,
    expected_params_longest_first: &[String],
    parameters: &HashMap<String, Value>,
) -> (String, Option<String>) {
    let mut out = sql.to_string();

    for param_name in expected_params_longest_first {
        let placeholder = format!("{{{}}}", param_name);
        let Some(value) = parameters.get(param_name) else {
            continue;
        };
        let literal = match value {
            Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Null => "NULL".to_string(),
            _ => return (out, Some(param_name.clone())),
        };
        out = out.replace(&placeholder, &literal);
    }

    (out, None)
}

fn sorted_json(params: &HashMap<String, Value>) -> String {
    let sorted: BTreeMap<&str, &Value> = params.iter().map(|(k, v)| (k.as_str(), v)).collect();
    serde_json::to_string(&sorted).unwrap_or_else(|_| "{}".to_string())
}

/// Order-insensitive schema diff: checks that every column in `produced`
/// has a same-named field in `expected` with the same Arrow DataType.
/// Nullability is allowed to widen (`produced.nullable = true` into a
/// `nullable = false` destination still passes, consistent with how Lance
/// and INSERT INTO handle nulls).
fn diff_schemas(table: &str, produced: &Schema, expected: &Schema) -> Result<(), String> {
    let expected_by_name: HashMap<&str, &arrow::datatypes::Field> = expected
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.as_ref()))
        .collect();
    let mut errs: Vec<String> = Vec::new();
    for produced_field in produced.fields() {
        let name = produced_field.name();
        let Some(expected_field) = expected_by_name.get(name.as_str()) else {
            errs.push(format!(
                "column '{}' is in the query output but not in destination '{}'",
                name, table
            ));
            continue;
        };
        if produced_field.data_type() != expected_field.data_type() {
            errs.push(format!(
                "column '{}' type mismatch: query produces {:?}, destination expects {:?}",
                name,
                produced_field.data_type(),
                expected_field.data_type()
            ));
        }
    }
    // Also flag non-nullable destination columns that the query does not
    // produce — INSERT INTO will reject those with an opaque error.
    for expected_field in expected.fields() {
        let produced_has = produced
            .fields()
            .iter()
            .any(|f| f.name() == expected_field.name());
        if !produced_has && !expected_field.is_nullable() {
            errs.push(format!(
                "required column '{}' is missing from query output",
                expected_field.name()
            ));
        }
    }
    if errs.is_empty() {
        Ok(())
    } else {
        Err(errs.join("; "))
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_job_task(
    run_id: String,
    store: Arc<dyn JobStore>,
    session_ctx: Arc<SessionContext>,
    destination: Arc<dyn JobDestination>,
    rendered_sql: String,
    mode: DestinationMode,
    timeout_ms: Option<u64>,
    cancel: Arc<CancelFlag>,
    flags_map: Arc<Mutex<HashMap<String, Arc<CancelFlag>>>>,
) {
    let started = Utc::now().to_rfc3339();
    if let Err(e) = store
        .update_status(
            &run_id,
            JobRunStatus::Running,
            Some(started.clone()),
            None,
            None,
            None,
            None,
        )
        .await
    {
        tracing::error!("failed to mark run {} running: {}", run_id, e);
    }

    let result = async {
        if cancel.is_cancelled() {
            return Err(anyhow::anyhow!("cancelled before execution"));
        }
        let df = session_ctx
            .sql(&rendered_sql)
            .await
            .context("Failed to plan rendered SQL")?;
        let batches = df.collect().await.context("Failed to collect job output")?;
        if cancel.is_cancelled() {
            return Err(anyhow::anyhow!("cancelled before commit"));
        }
        destination
            .write(batches, mode)
            .await
            .context("Destination write failed")
    };

    let outcome = match timeout_ms {
        Some(ms) if ms > 0 => match tokio::time::timeout(Duration::from_millis(ms), result).await {
            Ok(r) => r,
            Err(_) => Err(anyhow::anyhow!(
                "job timed out after {}ms before commit",
                ms
            )),
        },
        _ => result.await,
    };

    let finished = Utc::now().to_rfc3339();
    match outcome {
        Ok(w) if cancel.is_cancelled() => {
            // Race: task finished successfully but cancel flag flipped
            // between the write call and here. Report as cancelled — the
            // write did land, so include row count / snapshot for
            // transparency.
            if let Err(e) = store
                .update_status(
                    &run_id,
                    JobRunStatus::Cancelled,
                    None,
                    Some(finished.clone()),
                    Some(w.rows_written),
                    w.snapshot_id,
                    Some("cancelled after commit".to_string()),
                )
                .await
            {
                tracing::error!("failed to mark run {} cancelled: {}", run_id, e);
            }
        }
        Ok(w) => {
            if let Err(e) = store
                .update_status(
                    &run_id,
                    JobRunStatus::Succeeded,
                    None,
                    Some(finished),
                    Some(w.rows_written),
                    w.snapshot_id,
                    None,
                )
                .await
            {
                tracing::error!("failed to mark run {} succeeded: {}", run_id, e);
            }
        }
        Err(err) => {
            let status = if cancel.is_cancelled() {
                JobRunStatus::Cancelled
            } else {
                JobRunStatus::Failed
            };
            if let Err(e) = store
                .update_status(
                    &run_id,
                    status,
                    None,
                    Some(finished),
                    None,
                    None,
                    Some(format!("{err:?}")),
                )
                .await
            {
                tracing::error!("failed to mark run {} failed: {}", run_id, e);
            }
        }
    }

    // Drop the cancel flag once the row is terminal.
    let mut flags = flags_map.lock().unwrap_or_else(|p| p.into_inner());
    flags.remove(&run_id);
}

// ---------------------------------------------------------------------------
// Server-facing helpers to wire `DataSource` → the executor's destination
// resolver. These live here (rather than in the server crate) so the
// runner binary can reuse them unchanged.
// ---------------------------------------------------------------------------

/// A tiny struct describing one data source, just what the executor needs
/// for its destination lookup. The server populates it from its
/// `Vec<DataSource>`.
#[derive(Debug, Clone)]
pub struct DataSourceInfo {
    pub name: String,
    pub source_type: DataSourceType,
    pub path: Option<String>,
}

/// Build the path-resolver map the executor uses for Lance destinations.
/// For a table named `foo` backed by a Lance source, the first dotted
/// segment of `destination.table` is `foo` and the path comes from the
/// source's `path`.
pub fn lance_paths_from_sources(sources: &[DataSourceInfo]) -> HashMap<String, String> {
    sources
        .iter()
        .filter(|s| matches!(s.source_type, DataSourceType::Lance))
        .filter_map(|s| s.path.as_ref().map(|p| (s.name.clone(), p.clone())))
        .collect()
}

/// Convenience wrapper that combines a full `JobExecutor` with a
/// name→path map for Lance destinations. The server constructs this once
/// at startup and hands it to handlers via `AppState`.
pub struct JobExecutorBundle {
    pub executor: Arc<JobExecutor>,
    pub lance_paths: Arc<HashMap<String, String>>,
}

impl JobExecutorBundle {
    pub fn new(executor: Arc<JobExecutor>, lance_paths: HashMap<String, String>) -> Self {
        Self {
            executor,
            lance_paths: Arc::new(lance_paths),
        }
    }
}

// ---------------------------------------------------------------------------
// A more complete executor impl that honours lance paths provided externally.
// ---------------------------------------------------------------------------

/// Extension trait so callers that have a `lance_paths: HashMap<String,
/// String>` can submit jobs whose Lance destinations resolve against
/// those paths. Used by the server.
#[async_trait::async_trait]
pub trait JobExecutorExt {
    async fn submit_with_lance_paths(
        &self,
        job_name: &str,
        params: HashMap<String, Value>,
        lance_paths: &HashMap<String, String>,
    ) -> Result<String, JobSubmitError>;
}

#[async_trait::async_trait]
impl JobExecutorExt for JobExecutor {
    async fn submit_with_lance_paths(
        &self,
        job_name: &str,
        params: HashMap<String, Value>,
        lance_paths: &HashMap<String, String>,
    ) -> Result<String, JobSubmitError> {
        // Peek the destination so we can build the right resolver without
        // loading the job twice. If the job is unknown, surface that up
        // front.
        let job = {
            let jobs = self.jobs.read().await;
            jobs.get(job_name)
                .cloned()
                .ok_or_else(|| JobSubmitError::UnknownJob(job_name.to_string()))?
        };

        // For Lance destinations, plug the path into a one-shot executor
        // wrapper via a captured closure.
        let root = job
            .destination
            .table
            .split('.')
            .next()
            .unwrap_or(&job.destination.table);
        if let Some(path) = lance_paths.get(root) {
            return submit_with_lance_path_override(self, &job, job_name, params, path.clone())
                .await;
        }
        // Non-Lance: defer to the default resolver.
        self.submit(job_name, params).await
    }
}

async fn submit_with_lance_path_override(
    exec: &JobExecutor,
    job: &JobDefinition,
    job_name: &str,
    params: HashMap<String, Value>,
    lance_path: String,
) -> Result<String, JobSubmitError> {
    // Param validation — same as `submit`.
    let expected: Vec<String> = job
        .pipeline
        .request_schema()
        .fields
        .keys()
        .cloned()
        .collect();
    let missing: Vec<&str> = expected
        .iter()
        .filter(|n: &&String| !params.contains_key(n.as_str()))
        .map(|s: &String| s.as_str())
        .collect();
    if !missing.is_empty() {
        return Err(JobSubmitError::MissingParameters(missing.join(", ")));
    }

    let mut expected_sorted: Vec<String> = expected.clone();
    expected_sorted.sort_by_key(|s: &String| std::cmp::Reverse(s.len()));
    let (rendered_sql, bad_types) = substitute_sql_params(job.sql(), &expected_sorted, &params);
    if let Some(name) = bad_types {
        return Err(JobSubmitError::UnsupportedParameter(name));
    }

    let destination: Arc<dyn JobDestination> = Arc::new(LanceDestination::new(lance_path));

    let query_schema = exec
        .session_ctx
        .sql(&rendered_sql)
        .await
        .map(|df| df.schema().as_arrow().clone())
        .map_err(|e| JobSubmitError::SqlPlanFailure {
            job: job_name.to_string(),
            source: anyhow::anyhow!("{e}"),
        })?;

    exec.preflight(destination.as_ref(), &job.destination, &query_schema)
        .await?;

    let run_id = Uuid::new_v4().simple().to_string();
    let params_json = sorted_json(&params);
    let now = Utc::now().to_rfc3339();
    let run = JobRun {
        id: run_id.clone(),
        job_name: job_name.to_string(),
        parameters: params_json,
        status: JobRunStatus::Pending,
        created_at: now,
        started_at: None,
        finished_at: None,
        rows_written: None,
        snapshot_id: None,
        error: None,
    };
    exec.store
        .create_run(&run)
        .await
        .map_err(JobSubmitError::Internal)?;

    let cancel_flag = Arc::new(CancelFlag::default());
    {
        let mut flags = exec.cancel_flags.lock().unwrap_or_else(|p| p.into_inner());
        flags.insert(run_id.clone(), Arc::clone(&cancel_flag));
    }

    let store = Arc::clone(&exec.store);
    let session_ctx = Arc::clone(&exec.session_ctx);
    let flags_map = Arc::clone(&exec.cancel_flags);
    let run_id_for_task = run_id.clone();
    let mode = job.destination.mode;
    let timeout_ms = job.execution.timeout_ms;
    tokio::spawn(async move {
        run_job_task(
            run_id_for_task,
            store,
            session_ctx,
            destination,
            rendered_sql,
            mode,
            timeout_ms,
            cancel_flag,
            flags_map,
        )
        .await;
    });

    Ok(run_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;

    #[test]
    fn substitute_sql_params_replaces_string_and_number() {
        let sql = "SELECT * WHERE a = {a} AND b = {b}";
        let mut params = HashMap::new();
        params.insert("a".to_string(), Value::String("hi".into()));
        params.insert("b".to_string(), Value::Number(serde_json::Number::from(7)));
        let (out, bad) = substitute_sql_params(sql, &["a".to_string(), "b".to_string()], &params);
        assert!(bad.is_none());
        assert_eq!(out, "SELECT * WHERE a = 'hi' AND b = 7");
    }

    #[test]
    fn substitute_sql_params_escapes_single_quote() {
        let sql = "SELECT {x}";
        let mut params = HashMap::new();
        params.insert("x".to_string(), Value::String("it's".into()));
        let (out, _) = substitute_sql_params(sql, &["x".to_string()], &params);
        assert_eq!(out, "SELECT 'it''s'");
    }

    #[test]
    fn substitute_sql_params_rejects_array() {
        let sql = "{x}";
        let mut params = HashMap::new();
        params.insert("x".to_string(), serde_json::json!([1, 2, 3]));
        let (_, bad) = substitute_sql_params(sql, &["x".to_string()], &params);
        assert_eq!(bad.as_deref(), Some("x"));
    }

    #[test]
    fn diff_schemas_accepts_matching_columns() {
        let a = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let b = ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("id", DataType::Int64, false),
        ]);
        diff_schemas("t", &a, &b).expect("order-insensitive match should pass");
    }

    #[test]
    fn diff_schemas_rejects_missing_required_column() {
        let query = ArrowSchema::new(vec![Field::new("id", DataType::Int64, false)]);
        let dest = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let err = diff_schemas("t", &query, &dest).unwrap_err();
        assert!(err.contains("'name'"), "unexpected: {err}");
    }

    #[test]
    fn diff_schemas_rejects_type_mismatch() {
        let query = ArrowSchema::new(vec![Field::new("amount", DataType::Int64, false)]);
        let dest = ArrowSchema::new(vec![Field::new("amount", DataType::Float64, false)]);
        let err = diff_schemas("t", &query, &dest).unwrap_err();
        assert!(err.contains("type mismatch"), "unexpected: {err}");
    }

    fn mk_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap()
    }

    async fn setup_executor_with_memtable_dest() -> (Arc<JobExecutor>, tempfile::TempDir) {
        use std::io::Write;

        let ctx = Arc::new(SessionContext::new());
        // Register a destination MemTable the job can insert into.
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let tbl = datafusion::datasource::MemTable::try_new(
            schema.clone(),
            vec![vec![RecordBatch::new_empty(schema)]],
        )
        .unwrap();
        ctx.register_table("dest", Arc::new(tbl)).unwrap();

        // Also register a source table for the job SELECT to read from.
        ctx.register_batch("src", mk_batch()).unwrap();

        // Write a job YAML to disk and load it.
        let tmp = tempfile::TempDir::new().unwrap();
        let yaml_path = tmp.path().join("ingest.yaml");
        let yaml = r#"
kind: job
metadata:
  name: "ingest"
  version: "1.0.0"
query: |
  SELECT id FROM src WHERE id >= {min_id}
destination:
  table: "dest"
  mode: append
"#;
        let mut f = std::fs::File::create(&yaml_path).unwrap();
        f.write_all(yaml.as_bytes()).unwrap();
        let job = JobDefinition::load_from_file(&yaml_path, Arc::clone(&ctx))
            .await
            .unwrap()
            .unwrap();

        let mut map = HashMap::new();
        map.insert("ingest".to_string(), job);
        let store = Arc::new(
            super::super::store::SqliteJobStore::open_in_memory()
                .await
                .unwrap(),
        );
        let exec = Arc::new(JobExecutor::new(map, store, ctx, HashMap::new()));
        (exec, tmp)
    }

    #[tokio::test]
    async fn submit_unknown_job_rejected() {
        let (exec, _tmp) = setup_executor_with_memtable_dest().await;
        let err = exec
            .submit("does-not-exist", HashMap::new())
            .await
            .unwrap_err();
        assert!(matches!(err, JobSubmitError::UnknownJob(_)));
    }

    #[tokio::test]
    async fn submit_missing_param_rejected() {
        let (exec, _tmp) = setup_executor_with_memtable_dest().await;
        let err = exec.submit("ingest", HashMap::new()).await.unwrap_err();
        assert!(
            matches!(err, JobSubmitError::MissingParameters(_)),
            "got {err}"
        );
    }

    #[tokio::test]
    async fn submit_runs_end_to_end_for_memtable() {
        let (exec, _tmp) = setup_executor_with_memtable_dest().await;
        let mut params = HashMap::new();
        params.insert(
            "min_id".to_string(),
            Value::Number(serde_json::Number::from(1)),
        );
        let run_id = exec.submit("ingest", params).await.unwrap();

        // Spin until terminal. `submit` returns synchronously after
        // spawning, so the row exists but the task may still be running.
        for _ in 0..100 {
            if let Some(r) = exec.store.get_run(&run_id).await.unwrap() {
                if r.status.is_terminal() {
                    if r.status != JobRunStatus::Succeeded {
                        panic!("expected success, got {:?}: {:?}", r.status, r.error);
                    }
                    assert_eq!(r.rows_written, Some(3));
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("run never reached terminal state");
    }

    #[tokio::test]
    async fn submit_rejects_db_destination_missing() {
        use std::io::Write;

        let ctx = Arc::new(SessionContext::new());
        ctx.register_batch("src", mk_batch()).unwrap();
        let tmp = tempfile::TempDir::new().unwrap();
        let yaml_path = tmp.path().join("ingest.yaml");
        let yaml = r#"
kind: job
metadata:
  name: "ingest"
  version: "1.0.0"
query: |
  SELECT id FROM src
destination:
  table: "missing_table"
  mode: append
  create_if_missing: false
"#;
        std::fs::File::create(&yaml_path)
            .unwrap()
            .write_all(yaml.as_bytes())
            .unwrap();
        let job = JobDefinition::load_from_file(&yaml_path, Arc::clone(&ctx))
            .await
            .unwrap()
            .unwrap();

        let mut map = HashMap::new();
        map.insert("ingest".to_string(), job);
        let store = Arc::new(
            super::super::store::SqliteJobStore::open_in_memory()
                .await
                .unwrap(),
        );
        let exec = JobExecutor::new(map, store, ctx, HashMap::new());

        // A non-Lance destination that doesn't exist should be rejected.
        let err = exec.submit("ingest", HashMap::new()).await.unwrap_err();
        assert!(
            matches!(err, JobSubmitError::DbDestinationMissing { .. }),
            "got {err}"
        );
    }
}
