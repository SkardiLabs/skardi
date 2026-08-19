//! Run ledger for the jobs primitive.
//!
//! The `JobStore` trait holds CRUD over `JobRun` records. The MVP ships a
//! single SQLite-backed implementation at a fixed path (typically
//! `~/.skardi/jobs.db`). The trait exists so a Postgres-backed impl can
//! land later without restructuring the executor — relevant when the
//! server + runner split lands and both pods need to share the ledger.
//!
//! One process writes the SQLite file; reads and writes are serialized
//! through a single `tokio_rusqlite::Connection`.

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio_rusqlite::{Connection, Row, rusqlite};

/// DDL for the run ledger. Idempotent — run on every `open` via
/// `ensure_schema`. When a new column or index is needed, add it here and
/// rely on `CREATE ... IF NOT EXISTS`; rusqlite will no-op on existing
/// objects. A real migration system is a v1.1 concern.
const INIT_SCHEMA_SQL: &str = "CREATE TABLE IF NOT EXISTS job_runs (
    id            TEXT PRIMARY KEY,
    job_name      TEXT NOT NULL,
    parameters    TEXT NOT NULL,
    status        TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    started_at    TEXT,
    finished_at   TEXT,
    rows_written  INTEGER,
    snapshot_id   TEXT,
    error         TEXT,
    submission_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_job_runs_name_created
    ON job_runs (job_name, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_job_runs_status
    ON job_runs (status);";

/// Index over [`JobRun::submission_id`], applied after the migration below has
/// guaranteed the column exists — so it cannot live in [`INIT_SCHEMA_SQL`],
/// which also runs against pre-column ledgers.
///
/// Partial, because the column is NULL for every run not submitted through an
/// audited server, and because the only query against it is an equality
/// lookup for one submission.
const SUBMISSION_ID_INDEX_SQL: &str = "CREATE INDEX IF NOT EXISTS idx_job_runs_submission_id
    ON job_runs (submission_id) WHERE submission_id IS NOT NULL;";

/// True for the `duplicate column name` error SQLite raises when an
/// `ALTER TABLE ... ADD COLUMN` loses a race with another connection that
/// already added it. Matched on message text because `rusqlite` reports it as
/// a generic `SqliteFailure` with no distinguishing code.
fn is_duplicate_column(e: &rusqlite::Error) -> bool {
    e.to_string().contains("duplicate column name")
}

/// Lifecycle status of a single job run.
///
/// `Pending` — row has been created, background task has not started yet.
/// `Running` — background task is actively executing the query/write.
/// `Succeeded` — the destination commit finalized and the row is frozen.
/// `Failed` — the task errored out; `error` holds the message. Destination
/// is at whatever pre-job version it had.
/// `Cancelled` — an explicit `cancel` call reached the task before it
/// committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobRunStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl JobRunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "pending" => Self::Pending,
            "running" => Self::Running,
            "succeeded" => Self::Succeeded,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            other => anyhow::bail!("unknown job status: {other}"),
        })
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

/// One row of the run ledger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRun {
    pub id: String,
    pub job_name: String,
    /// JSON-encoded map of bound parameter values at submit time.
    pub parameters: String,
    pub status: JobRunStatus,
    /// ISO-8601 timestamp; set when the row is created.
    pub created_at: String,
    /// ISO-8601 timestamp; set when the task transitions into `Running`.
    pub started_at: Option<String>,
    /// ISO-8601 timestamp; set when the row reaches a terminal state.
    pub finished_at: Option<String>,
    pub rows_written: Option<u64>,
    /// Destination snapshot or version identifier (Lance: version number as
    /// string). For DB destinations this is typically null.
    pub snapshot_id: Option<String>,
    pub error: Option<String>,
    /// Opaque correlation token supplied by whoever submitted the run, stored
    /// verbatim and never interpreted here.
    ///
    /// The jobs subsystem has no notion of the query-audit ledger; this is the
    /// seam that lets a caller which *does* — the server — make the
    /// correlation reconstructable from either side. It writes its audit row
    /// id here in the same INSERT that creates the run, so the pointer is
    /// durable at run-creation time rather than stamped afterwards. See the
    /// ledger section of `docs/server.md`.
    pub submission_id: Option<String>,
}

/// Trait over the run ledger. All methods are async because the default
/// backend talks to SQLite through `tokio_rusqlite`.
#[async_trait]
pub trait JobStore: Send + Sync {
    async fn create_run(&self, run: &JobRun) -> Result<()>;
    async fn get_run(&self, run_id: &str) -> Result<Option<JobRun>>;
    /// Look a run up by the opaque token its submitter stored on it.
    ///
    /// This is the direction that makes the correlation *reconstructable*:
    /// the submitter already knows its own token, so it can recover the run
    /// even when its own forward pointer was never written.
    ///
    /// Returns the most recently created match. Duplicate tokens are a
    /// caller error rather than something this ledger enforces — nothing
    /// here can know whether a given token is meant to be unique.
    async fn get_run_by_submission_id(&self, submission_id: &str) -> Result<Option<JobRun>>;
    async fn list_runs(&self, job_name: Option<&str>, limit: usize) -> Result<Vec<JobRun>>;
    async fn update_status(
        &self,
        run_id: &str,
        status: JobRunStatus,
        started_at: Option<String>,
        finished_at: Option<String>,
        rows_written: Option<u64>,
        snapshot_id: Option<String>,
        error: Option<String>,
    ) -> Result<()>;
    /// Rewrite any non-terminal row to `Failed` with `reason` as the error
    /// message. Called on server startup so crash-killed runs don't stay
    /// `Running` forever.
    async fn reconcile_orphaned(&self, reason: &str) -> Result<usize>;
}

/// SQLite-backed implementation of [`JobStore`].
pub struct SqliteJobStore {
    conn: Arc<Connection>,
    path: PathBuf,
}

impl std::fmt::Debug for SqliteJobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteJobStore")
            .field("path", &self.path)
            .finish()
    }
}

impl SqliteJobStore {
    /// Open (creating if missing) the SQLite ledger at `path`. Parent
    /// directories are created as needed — this is how the MVP default of
    /// `~/.skardi/jobs.db` works out of the box.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create jobs.db parent dir: {parent:?}"))?;
        }
        let conn = Connection::open(&path)
            .await
            .with_context(|| format!("Failed to open jobs.db: {path:?}"))?;
        let store = Self {
            conn: Arc::new(conn),
            path,
        };
        store.ensure_schema().await?;
        Ok(store)
    }

    /// Open an in-memory ledger. Useful for tests.
    pub async fn open_in_memory() -> Result<Self> {
        let conn = Connection::open(":memory:")
            .await
            .context("Failed to open in-memory jobs.db")?;
        let store = Self {
            conn: Arc::new(conn),
            path: PathBuf::from(":memory:"),
        };
        store.ensure_schema().await?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    async fn ensure_schema(&self) -> Result<()> {
        self.conn
            .call(|conn| -> std::result::Result<(), rusqlite::Error> {
                conn.execute_batch(INIT_SCHEMA_SQL)?;
                Ok(())
            })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to initialize jobs.db schema: {e}"))?;

        // `CREATE TABLE IF NOT EXISTS` above no-ops on an existing ledger and
        // will not add columns, so ledgers written before `submission_id`
        // need the column bolted on. Idempotent: re-checked against
        // pragma table_info on every open.
        self.conn
            .call(|conn| -> std::result::Result<(), rusqlite::Error> {
                let has_column = conn
                    .prepare(
                        "SELECT 1 FROM pragma_table_info('job_runs') \
                         WHERE name = 'submission_id'",
                    )?
                    .exists([])?;
                if !has_column {
                    // The check is not atomic with the ALTER, so a second
                    // process opening the same file can win the race. Its
                    // `duplicate column name` is exactly the post-condition
                    // wanted here — treat it as success rather than failing
                    // startup over a benign race.
                    match conn.execute("ALTER TABLE job_runs ADD COLUMN submission_id TEXT", []) {
                        Ok(_) => {}
                        Err(e) if is_duplicate_column(&e) => {}
                        Err(e) => return Err(e),
                    }
                }
                conn.execute_batch(SUBMISSION_ID_INDEX_SQL)?;
                Ok(())
            })
            .await
            .map_err(|e| {
                anyhow::anyhow!("Failed to migrate jobs.db schema (submission_id): {e}")
            })?;
        Ok(())
    }
}

fn row_to_job_run(row: &Row<'_>) -> rusqlite::Result<JobRun> {
    let status_str: String = row.get("status")?;
    let status = JobRunStatus::from_str(&status_str).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(
            3,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::other(e.to_string())),
        )
    })?;
    Ok(JobRun {
        id: row.get("id")?,
        job_name: row.get("job_name")?,
        parameters: row.get("parameters")?,
        status,
        created_at: row.get("created_at")?,
        started_at: row.get("started_at")?,
        finished_at: row.get("finished_at")?,
        rows_written: row.get::<_, Option<i64>>("rows_written")?.map(|v| v as u64),
        snapshot_id: row.get("snapshot_id")?,
        error: row.get("error")?,
        submission_id: row.get("submission_id")?,
    })
}

#[async_trait]
impl JobStore for SqliteJobStore {
    async fn create_run(&self, run: &JobRun) -> Result<()> {
        let run = run.clone();
        self.conn
            .call(move |conn| -> std::result::Result<(), rusqlite::Error> {
                conn.execute(
                    "INSERT INTO job_runs
                         (id, job_name, parameters, status, created_at, started_at,
                          finished_at, rows_written, snapshot_id, error, submission_id)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                    rusqlite::params![
                        run.id,
                        run.job_name,
                        run.parameters,
                        run.status.as_str(),
                        run.created_at,
                        run.started_at,
                        run.finished_at,
                        run.rows_written.map(|v| v as i64),
                        run.snapshot_id,
                        run.error,
                        run.submission_id,
                    ],
                )?;
                Ok(())
            })
            .await
            .map_err(|e| anyhow::anyhow!("create_run failed: {e}"))?;
        Ok(())
    }

    async fn get_run(&self, run_id: &str) -> Result<Option<JobRun>> {
        let run_id = run_id.to_string();
        let row = self
            .conn
            .call(
                move |conn| -> std::result::Result<Option<JobRun>, rusqlite::Error> {
                    let mut stmt = conn.prepare(
                        "SELECT id, job_name, parameters, status, created_at, started_at,
                            finished_at, rows_written, snapshot_id, error, submission_id
                     FROM job_runs
                     WHERE id = ?1",
                    )?;
                    let mut rows = stmt.query(rusqlite::params![run_id])?;
                    match rows.next()? {
                        Some(row) => Ok(Some(row_to_job_run(row)?)),
                        None => Ok(None),
                    }
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("get_run failed: {e}"))?;
        Ok(row)
    }

    async fn get_run_by_submission_id(&self, submission_id: &str) -> Result<Option<JobRun>> {
        let submission_id = submission_id.to_string();
        let row = self
            .conn
            .call(
                move |conn| -> std::result::Result<Option<JobRun>, rusqlite::Error> {
                    let mut stmt = conn.prepare(
                        "SELECT id, job_name, parameters, status, created_at, started_at,
                            finished_at, rows_written, snapshot_id, error, submission_id
                     FROM job_runs
                     WHERE submission_id = ?1
                     ORDER BY created_at DESC
                     LIMIT 1",
                    )?;
                    let mut rows = stmt.query(rusqlite::params![submission_id])?;
                    match rows.next()? {
                        Some(row) => Ok(Some(row_to_job_run(row)?)),
                        None => Ok(None),
                    }
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("get_run_by_submission_id failed: {e}"))?;
        Ok(row)
    }

    async fn list_runs(&self, job_name: Option<&str>, limit: usize) -> Result<Vec<JobRun>> {
        let job_name = job_name.map(|s| s.to_string());
        let limit = limit as i64;
        let rows = self
            .conn
            .call(
                move |conn| -> std::result::Result<Vec<JobRun>, rusqlite::Error> {
                    let (sql, params): (&str, Vec<rusqlite::types::Value>) = match &job_name {
                        Some(name) => (
                            "SELECT id, job_name, parameters, status, created_at, started_at,
                                    finished_at, rows_written, snapshot_id, error, submission_id
                             FROM job_runs
                             WHERE job_name = ?1
                             ORDER BY created_at DESC
                             LIMIT ?2",
                            vec![
                                rusqlite::types::Value::Text(name.clone()),
                                rusqlite::types::Value::Integer(limit),
                            ],
                        ),
                        None => (
                            "SELECT id, job_name, parameters, status, created_at, started_at,
                                    finished_at, rows_written, snapshot_id, error, submission_id
                             FROM job_runs
                             ORDER BY created_at DESC
                             LIMIT ?1",
                            vec![rusqlite::types::Value::Integer(limit)],
                        ),
                    };
                    let mut stmt = conn.prepare(sql)?;
                    let rows = stmt
                        .query_map(rusqlite::params_from_iter(params), |row| {
                            row_to_job_run(row)
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    Ok(rows)
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("list_runs failed: {e}"))?;
        Ok(rows)
    }

    async fn update_status(
        &self,
        run_id: &str,
        status: JobRunStatus,
        started_at: Option<String>,
        finished_at: Option<String>,
        rows_written: Option<u64>,
        snapshot_id: Option<String>,
        error: Option<String>,
    ) -> Result<()> {
        let run_id = run_id.to_string();
        let status_str = status.as_str().to_string();
        self.conn
            .call(move |conn| -> std::result::Result<(), rusqlite::Error> {
                conn.execute(
                    "UPDATE job_runs
                     SET status       = ?2,
                         started_at   = COALESCE(?3, started_at),
                         finished_at  = COALESCE(?4, finished_at),
                         rows_written = COALESCE(?5, rows_written),
                         snapshot_id  = COALESCE(?6, snapshot_id),
                         error        = COALESCE(?7, error)
                     WHERE id = ?1",
                    rusqlite::params![
                        run_id,
                        status_str,
                        started_at,
                        finished_at,
                        rows_written.map(|v| v as i64),
                        snapshot_id,
                        error,
                    ],
                )?;
                Ok(())
            })
            .await
            .map_err(|e| anyhow::anyhow!("update_status failed: {e}"))?;
        Ok(())
    }

    async fn reconcile_orphaned(&self, reason: &str) -> Result<usize> {
        let reason = reason.to_string();
        let updated = self
            .conn
            .call(move |conn| -> std::result::Result<usize, rusqlite::Error> {
                let now = chrono::Utc::now().to_rfc3339();
                let n = conn.execute(
                    "UPDATE job_runs
                     SET status = 'failed',
                         finished_at = ?1,
                         error = ?2
                     WHERE status IN ('pending', 'running')",
                    rusqlite::params![now, reason],
                )?;
                Ok(n)
            })
            .await
            .map_err(|e| anyhow::anyhow!("reconcile_orphaned failed: {e}"))?;
        Ok(updated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_run(id: &str, job_name: &str) -> JobRun {
        sample_run_with_submission(id, job_name, None)
    }

    fn sample_run_with_submission(id: &str, job_name: &str, submission_id: Option<&str>) -> JobRun {
        JobRun {
            id: id.to_string(),
            job_name: job_name.to_string(),
            parameters: r#"{"from_date":"2026-01-01"}"#.to_string(),
            status: JobRunStatus::Pending,
            created_at: "2026-04-21T00:00:00Z".to_string(),
            started_at: None,
            finished_at: None,
            rows_written: None,
            snapshot_id: None,
            error: None,
            submission_id: submission_id.map(str::to_string),
        }
    }

    #[tokio::test]
    async fn status_round_trip_strings() {
        for status in [
            JobRunStatus::Pending,
            JobRunStatus::Running,
            JobRunStatus::Succeeded,
            JobRunStatus::Failed,
            JobRunStatus::Cancelled,
        ] {
            assert_eq!(JobRunStatus::from_str(status.as_str()).unwrap(), status);
        }
        assert!(JobRunStatus::from_str("not-a-status").is_err());
    }

    #[tokio::test]
    async fn create_get_list_round_trip() {
        let store = SqliteJobStore::open_in_memory().await.unwrap();
        store.create_run(&sample_run("r1", "ingest")).await.unwrap();
        store.create_run(&sample_run("r2", "ingest")).await.unwrap();
        store.create_run(&sample_run("r3", "other")).await.unwrap();

        let got = store.get_run("r1").await.unwrap().unwrap();
        assert_eq!(got.job_name, "ingest");
        assert_eq!(got.status, JobRunStatus::Pending);

        let by_name = store.list_runs(Some("ingest"), 10).await.unwrap();
        assert_eq!(by_name.len(), 2);
        assert!(by_name.iter().all(|r| r.job_name == "ingest"));

        let all = store.list_runs(None, 10).await.unwrap();
        assert_eq!(all.len(), 3);
    }

    #[tokio::test]
    async fn update_status_progresses_row() {
        let store = SqliteJobStore::open_in_memory().await.unwrap();
        store.create_run(&sample_run("r1", "ingest")).await.unwrap();

        store
            .update_status(
                "r1",
                JobRunStatus::Running,
                Some("2026-04-21T00:01:00Z".to_string()),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let got = store.get_run("r1").await.unwrap().unwrap();
        assert_eq!(got.status, JobRunStatus::Running);
        assert_eq!(got.started_at.as_deref(), Some("2026-04-21T00:01:00Z"));

        store
            .update_status(
                "r1",
                JobRunStatus::Succeeded,
                None,
                Some("2026-04-21T00:02:00Z".to_string()),
                Some(123),
                Some("7".to_string()),
                None,
            )
            .await
            .unwrap();
        let got = store.get_run("r1").await.unwrap().unwrap();
        assert_eq!(got.status, JobRunStatus::Succeeded);
        assert_eq!(got.rows_written, Some(123));
        assert_eq!(got.snapshot_id.as_deref(), Some("7"));
    }

    #[tokio::test]
    async fn reconcile_orphaned_marks_non_terminal_rows_failed() {
        let store = SqliteJobStore::open_in_memory().await.unwrap();

        let mut r_pending = sample_run("r-pending", "ingest");
        r_pending.status = JobRunStatus::Pending;
        store.create_run(&r_pending).await.unwrap();

        let mut r_running = sample_run("r-running", "ingest");
        r_running.status = JobRunStatus::Running;
        store.create_run(&r_running).await.unwrap();

        let mut r_done = sample_run("r-done", "ingest");
        r_done.status = JobRunStatus::Succeeded;
        store.create_run(&r_done).await.unwrap();

        let updated = store
            .reconcile_orphaned("server restart mid-run")
            .await
            .unwrap();
        assert_eq!(updated, 2, "pending + running rows should be reconciled");

        assert_eq!(
            store.get_run("r-pending").await.unwrap().unwrap().status,
            JobRunStatus::Failed
        );
        assert_eq!(
            store.get_run("r-running").await.unwrap().unwrap().status,
            JobRunStatus::Failed
        );
        assert_eq!(
            store.get_run("r-done").await.unwrap().unwrap().status,
            JobRunStatus::Succeeded
        );
    }

    #[tokio::test]
    async fn persists_across_opens() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let p = tmp.path().to_path_buf();
        drop(tmp); // let SqliteJobStore create the file

        {
            let s = SqliteJobStore::open(&p).await.unwrap();
            s.create_run(&sample_run("rX", "ingest")).await.unwrap();
        }
        {
            let s = SqliteJobStore::open(&p).await.unwrap();
            let got = s.get_run("rX").await.unwrap().unwrap();
            assert_eq!(got.job_name, "ingest");
        }
    }

    #[tokio::test]
    async fn submission_id_round_trips_and_looks_up_in_reverse() {
        let s = SqliteJobStore::open_in_memory().await.unwrap();
        s.create_run(&sample_run_with_submission(
            "r1",
            "ingest",
            Some("audit-abc"),
        ))
        .await
        .unwrap();

        assert_eq!(
            s.get_run("r1").await.unwrap().unwrap().submission_id,
            Some("audit-abc".to_string())
        );
        let found = s
            .get_run_by_submission_id("audit-abc")
            .await
            .unwrap()
            .expect("submission token did not resolve to its run");
        assert_eq!(found.id, "r1");
        assert!(
            s.get_run_by_submission_id("audit-nope")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn unattributed_runs_keep_a_null_submission_id() {
        // Runs submitted through an unaudited server carry no token, and must
        // not collide with each other in the reverse lookup.
        let s = SqliteJobStore::open_in_memory().await.unwrap();
        s.create_run(&sample_run("r1", "ingest")).await.unwrap();
        s.create_run(&sample_run("r2", "ingest")).await.unwrap();
        assert!(
            s.get_run("r1")
                .await
                .unwrap()
                .unwrap()
                .submission_id
                .is_none()
        );
        assert!(s.list_runs(None, 10).await.unwrap().len() == 2);
    }

    #[tokio::test]
    async fn open_migrates_a_ledger_written_before_submission_id() {
        // `CREATE TABLE IF NOT EXISTS` no-ops on an existing ledger and will
        // not add columns, so a jobs.db from before this change has to be
        // migrated on open — and must stay readable and writable after.
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        drop(tmp);
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE job_runs (
                    id TEXT PRIMARY KEY, job_name TEXT NOT NULL,
                    parameters TEXT NOT NULL, status TEXT NOT NULL,
                    created_at TEXT NOT NULL, started_at TEXT, finished_at TEXT,
                    rows_written INTEGER, snapshot_id TEXT, error TEXT);
                 INSERT INTO job_runs (id, job_name, parameters, status, created_at)
                 VALUES ('old-run', 'ingest', '{}', 'succeeded', '2026-01-01T00:00:00Z');",
            )
            .unwrap();
        }

        let s = SqliteJobStore::open(&path).await.unwrap();

        // The pre-migration row survives, with the new column NULL.
        let old = s.get_run("old-run").await.unwrap().unwrap();
        assert_eq!(old.job_name, "ingest");
        assert!(old.submission_id.is_none());

        // And the migrated ledger accepts attributed writes.
        s.create_run(&sample_run_with_submission(
            "new-run",
            "ingest",
            Some("audit-1"),
        ))
        .await
        .unwrap();
        assert_eq!(
            s.get_run_by_submission_id("audit-1")
                .await
                .unwrap()
                .unwrap()
                .id,
            "new-run"
        );

        // The partial index is applied in the same guarded step as the ALTER,
        // so a migrated ledger must not be left without it.
        let has_index = s
            .conn
            .call(|conn| -> std::result::Result<bool, rusqlite::Error> {
                conn.prepare(
                    "SELECT 1 FROM sqlite_master
                      WHERE type = 'index' AND name = 'idx_job_runs_submission_id'",
                )?
                .exists([])
            })
            .await
            .unwrap();
        assert!(
            has_index,
            "migrated ledger is missing the submission_id index"
        );
    }

    #[test]
    fn duplicate_column_error_is_recognised() {
        // The migration's check-then-ALTER is not atomic, so a second process
        // can win the race. Treating its error as success is only safe if it
        // can be told apart from a real failure.
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE job_runs (id TEXT PRIMARY KEY);")
            .unwrap();
        conn.execute("ALTER TABLE job_runs ADD COLUMN submission_id TEXT", [])
            .unwrap();
        let err = conn
            .execute("ALTER TABLE job_runs ADD COLUMN submission_id TEXT", [])
            .expect_err("adding an existing column must fail");
        assert!(is_duplicate_column(&err), "unrecognised: {err}");

        let other = conn
            .execute("ALTER TABLE nonexistent ADD COLUMN submission_id TEXT", [])
            .expect_err("altering a missing table must fail");
        assert!(!is_duplicate_column(&other), "over-matched: {other}");
    }
}
