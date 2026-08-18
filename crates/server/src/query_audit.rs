//! Durable audit store for ad-hoc `/query` statements and pipeline
//! executions.
//!
//! Off unless the operator sets `--query-audit-db <path>`. When enabled, every
//! statement the `/query` endpoint accepts — and every `POST /:name/execute`
//! pipeline run — is written to a SQLite database *before* execution and
//! updated with its outcome afterwards, so the store answers "what ran, on
//! whose behalf, and did it succeed" rather than merely "what was attempted".
//!
//! The `sql` column is overloaded by row kind: raw SQL for ad-hoc rows,
//! `name@version` for `statement_kind = 'pipeline'` rows (the versioned
//! template lives on disk; parameter values are never recorded — the version
//! is what keeps "what ran" answerable after the promotion loop edits a
//! template, since rows are kept forever by default). Ad-hoc rows carry
//! `statement_kind` values from `StatementKind`'s `Debug` form — `Query` /
//! `Other` — not SQL verbs like `select`.
//!
//! Design notes:
//!
//! * **Durable before execution.** [`QueryAuditStore::record_started`] commits
//!   with `synchronous = FULL` and returns the row id. If that write fails, the
//!   handler rejects the request — an unrecorded query never runs.
//! * **Async.** All access goes through `tokio_rusqlite`, which owns a
//!   dedicated blocking thread. No filesystem I/O happens on a Tokio worker,
//!   and requests are not serialized behind a mutex held across a `write`.
//! * **Private by default.** The database (and its WAL sidecars) are created
//!   `0600` on Unix. It holds raw SQL, which may embed secrets or PII.
//! * **Queryable.** Indexed by `session_id` and `created_at`, so an operator
//!   can reconstruct an agent session with plain SQL.
//! * **Never silently disabled.** Opening or migrating the store is fatal at
//!   startup; the server refuses to run with auditing configured but broken.
//!
//! Retention is explicit: rows older than `--query-audit-retention-days` are
//! pruned at startup and hourly thereafter. Without that flag rows are kept
//! forever, and pruning/rotation is the operator's call.

use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio_rusqlite::{Connection, params, rusqlite};

/// Upper bound on any single ledger write. Fail-closed only means "503, and
/// the statement does not run" if a write that *hangs* (a stalled fsync on
/// EBS/NFS, a full dm-thin pool) is also treated as failed — otherwise the
/// request hangs with it, and because the store is one serialized writer
/// thread, every subsequent audited request queues behind it. A timed-out
/// write may still land later on the writer thread; for `record_outcome`
/// (and the retention prune) that leaves an orphaned `started` row, which
/// the next startup reconciles to `unknown` exactly like a crash mid-query.
/// The two pre-execution writers (`record_started`,
/// `record_pipeline_started`) know more than that — a timeout there means
/// the caller has already told the requester "503, did not run" — so they
/// follow up with a corrective UPDATE instead of leaving that ambiguity for
/// startup to paper over; see `QueryAuditStore::spawn_timeout_correction`.
///
/// Individual stores may override this via `write_timeout` (tests only, to
/// exercise the timeout branch without waiting out the real bound); the
/// const remains the production default.
pub(crate) const AUDIT_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum length of a session id, in characters — shared by `/query`'s
/// `ai_context.session_id` and the pipeline execute endpoint's
/// `x-skardi-session-id` header (see `query_handlers::validate_ai_context`
/// and `pipeline_handlers::session_id_from_headers`), so the two paths can't
/// drift apart. It is an opaque grouping key, not a payload.
///
/// `skardi-cli` restates this cap under the same name (`run.rs`) because the
/// CLI crate does not depend on this one; keep them in sync.
pub(crate) const MAX_SESSION_ID_CHARS: usize = 200;

/// Error from [`bounded`] that keeps "the write timed out" distinguishable
/// from "the write ran and failed" (e.g. `ConnectionClosed`). Only the
/// pre-execution writers act on that distinction — see
/// [`QueryAuditStore::spawn_timeout_correction`] — everywhere else this
/// converts straight to `anyhow::Error` via `?`, same as before.
#[derive(Debug)]
enum BoundedError {
    /// Elapsed `timeout` without the write completing. It may still land
    /// later on the writer thread.
    TimedOut(anyhow::Error),
    /// The write itself returned an error.
    Other(anyhow::Error),
}

impl BoundedError {
    fn is_timeout(&self) -> bool {
        matches!(self, BoundedError::TimedOut(_))
    }
}

impl std::fmt::Display for BoundedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BoundedError::TimedOut(e) | BoundedError::Other(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for BoundedError {}

// `anyhow`'s blanket `impl<E: StdError + Send + Sync + 'static> From<E> for
// anyhow::Error` covers the `BoundedError -> anyhow::Error` conversion `?`
// needs at every call site below — an explicit `impl From<BoundedError> for
// anyhow::Error` here would conflict with it.

/// Await a ledger write, bounding it with `timeout` (production callers pass
/// [`AUDIT_WRITE_TIMEOUT`]; tests may pass a shorter bound via
/// `QueryAuditStore::write_timeout`). Elapsed is an error like any other
/// write failure — see the timeout const for why — but callers that need to
/// react specifically to a timeout can check [`BoundedError::is_timeout`].
async fn bounded<T>(
    write: impl Future<Output = tokio_rusqlite::Result<T>>,
    what: &'static str,
    timeout: Duration,
) -> std::result::Result<T, BoundedError> {
    match tokio::time::timeout(timeout, write).await {
        Ok(result) => result.context(what).map_err(BoundedError::Other),
        Err(_) => Err(BoundedError::TimedOut(anyhow!(
            "{what}: timed out after {timeout:?}"
        ))),
    }
}

/// Stamp the terminal outcome onto an audit record.
///
/// Unlike the pre-execution write, a failure (or timeout) here cannot un-run
/// the statement, so it is logged rather than surfaced: the row simply stays
/// `started` and the next startup reconciles it to `unknown`. No-op when
/// auditing is off. Callers pass `app_state.query_audit.as_deref()`.
pub(crate) async fn finish_audit(
    store: Option<&QueryAuditStore>,
    audit_id: Option<&str>,
    status: QueryAuditStatus,
    row_count: Option<usize>,
    error: Option<&str>,
) {
    let (Some(store), Some(id)) = (store, audit_id) else {
        return;
    };
    if let Err(e) = store.record_outcome(id, status, row_count, error).await {
        tracing::error!("Failed to record query-audit outcome for {id}: {e}");
    }
}

/// Shorthand for the closure return type `tokio_rusqlite::Connection::call`
/// expects; the crate cannot infer it from a bare `Ok(())`.
type SqlResult<T> = std::result::Result<T, rusqlite::Error>;

/// Ledger DDL. Idempotent — applied on every `open`.
const INIT_SCHEMA_SQL: &str = "CREATE TABLE IF NOT EXISTS query_audit (
    id             TEXT PRIMARY KEY,
    created_at     TEXT NOT NULL,
    finished_at    TEXT,
    sql            TEXT NOT NULL,
    ai_context     TEXT,
    session_id     TEXT,
    max_rows       INTEGER NOT NULL,
    statement_kind TEXT NOT NULL,
    status         TEXT NOT NULL,
    row_count      INTEGER,
    error          TEXT
);
CREATE INDEX IF NOT EXISTS idx_query_audit_session_created
    ON query_audit (session_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_query_audit_created
    ON query_audit (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_query_audit_status
    ON query_audit (status);
CREATE INDEX IF NOT EXISTS idx_query_audit_statement_kind
    ON query_audit (statement_kind);";

/// Outcome of an audited statement.
///
/// `Started` — the row was committed but the engine has not answered yet. A row
/// left in this state after a restart was killed mid-flight; startup rewrites
/// those to `Unknown`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryAuditStatus {
    Started,
    Succeeded,
    Failed,
    Unknown,
}

impl QueryAuditStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Unknown => "unknown",
        }
    }
}

/// SQLite-backed audit ledger for `/query`.
pub struct QueryAuditStore {
    conn: Connection,
    path: PathBuf,
    /// Bound passed to [`bounded`] for every write. [`AUDIT_WRITE_TIMEOUT`]
    /// in production; overridable in tests via [`Self::with_write_timeout`]
    /// so the timeout branch can be exercised without waiting 5s.
    write_timeout: Duration,
}

impl std::fmt::Debug for QueryAuditStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryAuditStore")
            .field("path", &self.path)
            .finish()
    }
}

impl QueryAuditStore {
    /// Open (creating if missing) the audit database at `path`, applying the
    /// schema and locking the files down to owner-only.
    ///
    /// Errors here are fatal to startup by design: an operator who asked for an
    /// audit trail must not get a server that quietly runs without one.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "Failed to create query-audit parent dir: {}",
                    parent.display()
                )
            })?;
        }
        // Create the file ourselves so it is never briefly world-readable:
        // `OpenOptions::create` honours the process umask (typically 0644),
        // and SQLite would inherit that.
        create_private_file(&path)?;

        let conn = Connection::open(&path)
            .await
            .with_context(|| format!("Failed to open query-audit db: {}", path.display()))?;

        conn.call(|conn| -> SqlResult<()> {
            // WAL keeps the pre-execution insert from blocking on readers;
            // FULL makes that insert durable before the query is handed to the
            // engine.
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "synchronous", "FULL")?;
            conn.execute_batch(INIT_SCHEMA_SQL)?;
            Ok(())
        })
        .await
        .with_context(|| format!("Failed to initialise query-audit db: {}", path.display()))?;

        // WAL creates `-wal`/`-shm` sidecars on first write; they carry the same
        // content and need the same protection.
        restrict_permissions(&path)?;
        for suffix in ["-wal", "-shm"] {
            let mut sidecar = path.clone().into_os_string();
            sidecar.push(suffix);
            let sidecar = PathBuf::from(sidecar);
            if sidecar.exists() {
                restrict_permissions(&sidecar)?;
            }
        }

        Ok(Self {
            conn,
            path,
            write_timeout: AUDIT_WRITE_TIMEOUT,
        })
    }

    /// Open an in-memory ledger. Tests only — nothing is durable.
    pub async fn open_in_memory() -> Result<Self> {
        let conn = Connection::open(":memory:")
            .await
            .context("Failed to open in-memory query-audit db")?;
        conn.call(|conn| -> SqlResult<()> {
            conn.execute_batch(INIT_SCHEMA_SQL)?;
            Ok(())
        })
        .await
        .context("Failed to initialise in-memory query-audit db")?;
        Ok(Self {
            conn,
            path: PathBuf::from(":memory:"),
            write_timeout: AUDIT_WRITE_TIMEOUT,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Override the write bound. Tests only — lets a test exercise the
    /// timeout branch (finding: "the timeout branch that fail-closed rests
    /// on is untested") in milliseconds instead of waiting out the real
    /// [`AUDIT_WRITE_TIMEOUT`].
    #[cfg(test)]
    pub(crate) fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }

    /// Close the backing connection, leaving the store permanently unwritable.
    /// Exists so tests can exercise the handler's fail-closed path (a store
    /// that cannot record must stop queries from running).
    pub async fn close_for_test(&self) {
        let _ = self.conn.clone().close().await;
    }

    /// Commit the pre-execution record and return its id.
    ///
    /// The caller must treat an `Err` as fatal to the request: the point of the
    /// store is that nothing executes unrecorded.
    pub async fn record_started(
        &self,
        sql: &str,
        ai_context: Option<&Value>,
        max_rows: usize,
        statement_kind: &str,
    ) -> Result<String> {
        let id = new_id();
        let created_at = chrono::Utc::now().to_rfc3339();
        // `session_id` is denormalised out of the context object so the index
        // can serve session lookups without JSON parsing.
        let session_id = ai_context
            .and_then(|c| c.get("session_id"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let ai_context = ai_context.map(ToString::to_string);
        let sql = sql.to_string();
        let statement_kind = statement_kind.to_string();
        let row_id = id.clone();

        match bounded(
            self.conn.call(move |conn| -> SqlResult<()> {
                conn.execute(
                    "INSERT INTO query_audit
                        (id, created_at, sql, ai_context, session_id, max_rows,
                         statement_kind, status)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        row_id,
                        created_at,
                        sql,
                        ai_context,
                        session_id,
                        max_rows as i64,
                        statement_kind,
                        QueryAuditStatus::Started.as_str(),
                    ],
                )?;
                Ok(())
            }),
            "Failed to write pre-execution query-audit record",
            self.write_timeout,
        )
        .await
        {
            Ok(()) => Ok(id),
            Err(e) => {
                if e.is_timeout() {
                    self.spawn_timeout_correction(id);
                }
                Err(e.into())
            }
        }
    }

    /// Insert a `started` row for a pipeline execution.
    ///
    /// Stores `name@version` in the `sql` column: the template lives on disk
    /// with no secrets, and pipelines are precisely the artifacts the
    /// promotion loop edits — rows outlive template revisions (retention is
    /// off by default), so the name alone stops answering *what SQL ran*.
    /// `version` comes from the pipeline's `metadata.version`. Parameter
    /// values are deliberately never recorded — they are where PII lives.
    /// `ai_context` is left NULL rather than synthesized so the column always
    /// means "caller-sent object".
    pub async fn record_pipeline_started(
        &self,
        pipeline_name: &str,
        version: &str,
        session_id: Option<&str>,
    ) -> Result<String> {
        let id = new_id();
        let created_at = chrono::Utc::now().to_rfc3339();
        let name_at_version = format!("{pipeline_name}@{version}");
        let session_id = session_id.map(str::to_string);
        let row_id = id.clone();

        match bounded(
            self.conn.call(move |conn| -> SqlResult<()> {
                conn.execute(
                    "INSERT INTO query_audit
                        (id, created_at, sql, ai_context, session_id, max_rows,
                         statement_kind, status)
                     VALUES (?1, ?2, ?3, NULL, ?4, ?5, ?6, ?7)",
                    params![
                        row_id,
                        created_at,
                        name_at_version,
                        session_id,
                        PIPELINE_MAX_ROWS_SENTINEL,
                        PIPELINE_STATEMENT_KIND,
                        QueryAuditStatus::Started.as_str(),
                    ],
                )?;
                Ok(())
            }),
            "Failed to write pre-execution pipeline-audit record",
            self.write_timeout,
        )
        .await
        {
            Ok(()) => Ok(id),
            Err(e) => {
                if e.is_timeout() {
                    self.spawn_timeout_correction(id);
                }
                Err(e.into())
            }
        }
    }

    /// Update a record with its terminal outcome.
    pub async fn record_outcome(
        &self,
        id: &str,
        status: QueryAuditStatus,
        row_count: Option<usize>,
        error: Option<&str>,
    ) -> Result<()> {
        let id = id.to_string();
        let finished_at = chrono::Utc::now().to_rfc3339();
        let status = status.as_str();
        let row_count = row_count.map(|n| n as i64);
        let error = error.map(str::to_string);

        bounded(
            self.conn.call(move |conn| -> SqlResult<()> {
                conn.execute(
                    "UPDATE query_audit
                        SET status = ?2, finished_at = ?3, row_count = ?4, error = ?5
                      WHERE id = ?1",
                    params![id, status, finished_at, row_count, error],
                )?;
                Ok(())
            }),
            "Failed to update query-audit record",
            self.write_timeout,
        )
        .await?;
        Ok(())
    }

    /// Best-effort correction for a pre-execution write that timed out from
    /// the caller's point of view, submitted as a follow-up UPDATE on a
    /// cloned connection.
    ///
    /// Why this is safe: `tokio_rusqlite::Connection` funnels every `call`
    /// onto one dedicated writer thread via a FIFO channel. The abandoned
    /// INSERT's `call` was already sent into that channel before `bounded`
    /// gave up on it (the timeout races the *await*, not the *send* — the
    /// future is polled at least once, which is when the message goes onto
    /// the channel). This UPDATE's `call` is only sent afterwards, from the
    /// spawned task below, so it is queued strictly after the INSERT and is
    /// guaranteed to be applied to the real row if the INSERT ever lands —
    /// turning a fact we already know ("the request was told 503, did not
    /// run") into the ledger's `failed`/`audit_write_timeout`, instead of
    /// letting startup reconciliation later guess `unknown` ("may have run
    /// after a crash") for a row we know did not run. If the INSERT itself
    /// never lands (e.g. the connection died rather than merely stalled),
    /// this UPDATE matches no row and is a harmless no-op.
    ///
    /// Fire-and-forget by design: the request has already returned 503 by
    /// the time this could resolve, so the caller does not await it. Any
    /// failure here can only be observed via logging.
    fn spawn_timeout_correction(&self, id: String) {
        let conn = self.conn.clone();
        tokio::spawn(async move {
            let finished_at = chrono::Utc::now().to_rfc3339();
            let outcome = conn
                .call(move |conn| -> SqlResult<usize> {
                    conn.execute(
                        "UPDATE query_audit
                            SET status = ?2, finished_at = ?3, error = ?4
                          WHERE id = ?1 AND status = ?5",
                        params![
                            id,
                            QueryAuditStatus::Failed.as_str(),
                            finished_at,
                            "audit_write_timeout",
                            QueryAuditStatus::Started.as_str(),
                        ],
                    )
                })
                .await;
            match outcome {
                // 0 rows means either the INSERT never landed, or it landed
                // and something else already moved it off `started` —
                // either way there is nothing to correct.
                Ok(_) => {}
                Err(e) => tracing::warn!(
                    "Failed to apply audit-write-timeout correction to query-audit record: {e}"
                ),
            }
        });
    }

    /// Rewrite rows still marked `started` to `unknown`. Called at startup so a
    /// crash-killed query does not masquerade as still running.
    pub async fn reconcile_orphaned(&self, reason: &str) -> Result<usize> {
        let reason = reason.to_string();
        let finished_at = chrono::Utc::now().to_rfc3339();
        let updated = self
            .conn
            .call(move |conn| -> SqlResult<usize> {
                let n = conn.execute(
                    "UPDATE query_audit
                        SET status = ?1, finished_at = ?2, error = ?3
                      WHERE status = ?4",
                    params![
                        QueryAuditStatus::Unknown.as_str(),
                        finished_at,
                        reason,
                        QueryAuditStatus::Started.as_str(),
                    ],
                )?;
                Ok(n)
            })
            .await
            .context("Failed to reconcile orphaned query-audit records")?;
        Ok(updated)
    }

    /// Delete records created before `cutoff` (RFC 3339). Returns the total
    /// row count deleted.
    ///
    /// Runs as repeated bounded, `PRUNE_BATCH_SIZE`-row deletes rather than
    /// one unbounded `DELETE`. This store's connection is a single
    /// serialized writer thread shared with every audited request's
    /// pre-execution INSERT (see the module docs); an unbounded delete over
    /// a multi-million-row backlog under `synchronous = FULL` can exceed
    /// [`AUDIT_WRITE_TIMEOUT`] on its own — which, on that shared thread,
    /// would make every *other* concurrent `/query` and pipeline-execute
    /// request time out and 503 even though nothing is wrong with them, and
    /// the prune itself still would not have finished. Chunking keeps each
    /// individual write comfortably inside the bound (wrapped in `bounded`
    /// like every other write) and yields between chunks so writes queued
    /// behind the prune get a chance to interleave rather than all landing
    /// after it.
    pub async fn prune_before(&self, cutoff: chrono::DateTime<chrono::Utc>) -> Result<usize> {
        let cutoff = cutoff.to_rfc3339();
        let mut total = 0usize;
        loop {
            let batch_cutoff = cutoff.clone();
            let deleted = bounded(
                self.conn.call(move |conn| -> SqlResult<usize> {
                    let n = conn.execute(
                        "DELETE FROM query_audit WHERE id IN (
                            SELECT id FROM query_audit WHERE created_at < ?1 LIMIT ?2
                         )",
                        params![batch_cutoff, PRUNE_BATCH_SIZE as i64],
                    )?;
                    Ok(n)
                }),
                "Failed to prune query-audit records",
                self.write_timeout,
            )
            .await?;
            if deleted == 0 {
                break;
            }
            total += deleted;
            tokio::task::yield_now().await;
        }
        Ok(total)
    }

    /// Fetch one record as a JSON object. Test/diagnostic helper.
    pub async fn get(&self, id: &str) -> Result<Option<Value>> {
        let id = id.to_string();
        let row = self
            .conn
            .call(move |conn| -> SqlResult<Option<Value>> {
                let mut stmt = conn.prepare(
                    "SELECT id, created_at, finished_at, sql, ai_context, session_id,
                            max_rows, statement_kind, status, row_count, error
                       FROM query_audit WHERE id = ?1",
                )?;
                let row = stmt
                    .query_row(params![id], |row| {
                        Ok(serde_json::json!({
                            "id": row.get::<_, String>(0)?,
                            "created_at": row.get::<_, String>(1)?,
                            "finished_at": row.get::<_, Option<String>>(2)?,
                            "sql": row.get::<_, String>(3)?,
                            "ai_context": row.get::<_, Option<String>>(4)?
                                .and_then(|s| serde_json::from_str::<Value>(&s).ok()),
                            "session_id": row.get::<_, Option<String>>(5)?,
                            "max_rows": row.get::<_, i64>(6)?,
                            "statement_kind": row.get::<_, String>(7)?,
                            "status": row.get::<_, String>(8)?,
                            "row_count": row.get::<_, Option<i64>>(9)?,
                            "error": row.get::<_, Option<String>>(10)?,
                        }))
                    })
                    .ok();
                Ok(row)
            })
            .await
            .context("Failed to read query-audit record")?;
        Ok(row)
    }

    /// All records for one `session_id`, oldest first. Test/diagnostic helper
    /// that also demonstrates the session index.
    pub async fn list_by_session(&self, session_id: &str) -> Result<Vec<Value>> {
        let session_id = session_id.to_string();
        let ids = self
            .conn
            .call(move |conn| -> SqlResult<Vec<String>> {
                let mut stmt = conn.prepare(
                    // `id` breaks created_at ties: `to_rfc3339` emits
                    // variable-precision subseconds, so two same-instant rows
                    // compare equal as strings, and `new_id()` is monotonic
                    // within a process — making the order total.
                    "SELECT id FROM query_audit WHERE session_id = ?1 \
                     ORDER BY created_at ASC, id ASC",
                )?;
                let ids = stmt
                    .query_map(params![session_id], |row| row.get::<_, String>(0))?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                Ok(ids)
            })
            .await
            .context("Failed to list query-audit records by session")?;

        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(record) = self.get(&id).await? {
                out.push(record);
            }
        }
        Ok(out)
    }

    /// Total record count. Test/diagnostic helper.
    pub async fn count(&self) -> Result<usize> {
        let n = self
            .conn
            .call(|conn| -> SqlResult<i64> {
                let n: i64 =
                    conn.query_row("SELECT count(*) FROM query_audit", [], |r| r.get(0))?;
                Ok(n)
            })
            .await
            .context("Failed to count query-audit records")?;
        Ok(n as usize)
    }
}

/// Create `path` owner-only if it does not exist yet. Existing files are left
/// alone here; [`restrict_permissions`] tightens them after open.
fn create_private_file(path: &Path) -> Result<()> {
    if path.exists() {
        return Ok(());
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(path) {
        Ok(_) => Ok(()),
        // Lost a race with another process; permissions get fixed below.
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e)
            .with_context(|| format!("Failed to create query-audit db file: {}", path.display())),
    }
}

/// Force owner-only permissions on an audit file. No-op off Unix, where the
/// operator owns access control (documented in `docs/server.md`).
fn restrict_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).with_context(
            || {
                format!(
                    "Failed to restrict query-audit file permissions: {}",
                    path.display()
                )
            },
        )?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

/// Statement-kind marker distinguishing pipeline rows from ad-hoc SQL rows.
const PIPELINE_STATEMENT_KIND: &str = "pipeline";

/// `max_rows` does not apply to pipeline executions, but the column is NOT
/// NULL; pipeline rows store this sentinel.
///
/// The sentinel is unambiguous only because `/query` rejects
/// `max_rows: Some(0)` up front (the `Some(0)` arm in
/// `query_handlers::execute_query`), so `0` can never appear on an ad-hoc
/// row. That invariant lives in a
/// different module — if `/query` ever starts allowing `max_rows: 0`, this
/// sentinel needs a new value (or a dedicated column) first.
const PIPELINE_MAX_ROWS_SENTINEL: i64 = 0;

/// Rows deleted per [`QueryAuditStore::prune_before`] batch. Small enough
/// that a batch delete is comfortably inside [`AUDIT_WRITE_TIMEOUT`] even on
/// a slow disk; see that method's doc comment for why batching exists at
/// all.
const PRUNE_BATCH_SIZE: usize = 1000;

/// Random-ish unique row id. Avoids a `uuid` dependency: the timestamp keeps
/// ids ordered and the zero-padded hex counter disambiguates within the same
/// nanosecond without breaking lexicographic order — `list_by_session`'s
/// `id ASC` tie-break (and any other string sort) depends on same-width
/// hex; an unpadded counter would sort `…-f` after `…-10`.
fn new_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{nanos:x}-{seq:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn records_sql_ai_context_and_outcome() {
        let store = QueryAuditStore::open_in_memory().await.unwrap();
        let ctx = json!({"purpose": "kyc", "session_id": "sess-1"});
        let id = store
            .record_started(
                "SELECT * FROM t WHERE ssn = '123-45-6789'",
                Some(&ctx),
                100,
                "Query",
            )
            .await
            .unwrap();

        let record = store.get(&id).await.unwrap().unwrap();
        assert_eq!(
            record["sql"],
            json!("SELECT * FROM t WHERE ssn = '123-45-6789'")
        );
        assert_eq!(record["ai_context"]["purpose"], json!("kyc"));
        assert_eq!(record["session_id"], json!("sess-1"));
        assert_eq!(record["max_rows"], json!(100));
        assert_eq!(record["statement_kind"], json!("Query"));
        assert_eq!(record["status"], json!("started"));
        assert!(record["finished_at"].is_null());

        store
            .record_outcome(&id, QueryAuditStatus::Succeeded, Some(3), None)
            .await
            .unwrap();
        let record = store.get(&id).await.unwrap().unwrap();
        assert_eq!(record["status"], json!("succeeded"));
        assert_eq!(record["row_count"], json!(3));
        assert!(!record["finished_at"].is_null());
    }

    #[tokio::test]
    async fn failed_outcome_carries_the_error() {
        let store = QueryAuditStore::open_in_memory().await.unwrap();
        let id = store
            .record_started("SELECT 1", None, 1, "Query")
            .await
            .unwrap();
        store
            .record_outcome(&id, QueryAuditStatus::Failed, None, Some("boom"))
            .await
            .unwrap();
        let record = store.get(&id).await.unwrap().unwrap();
        assert_eq!(record["status"], json!("failed"));
        assert_eq!(record["error"], json!("boom"));
    }

    #[tokio::test]
    async fn session_lookup_returns_records_in_order() {
        let store = QueryAuditStore::open_in_memory().await.unwrap();
        let ctx = json!({"purpose": "audit", "session_id": "sess-42"});
        let other = json!({"purpose": "audit", "session_id": "sess-other"});
        store
            .record_started("SELECT 1", Some(&ctx), 1, "Query")
            .await
            .unwrap();
        store
            .record_started("SELECT 2", Some(&other), 1, "Query")
            .await
            .unwrap();
        store
            .record_started("SELECT 3", Some(&ctx), 1, "Query")
            .await
            .unwrap();

        let session = store.list_by_session("sess-42").await.unwrap();
        assert_eq!(session.len(), 2);
        assert_eq!(session[0]["sql"], json!("SELECT 1"));
        assert_eq!(session[1]["sql"], json!("SELECT 3"));
    }

    #[tokio::test]
    async fn reconcile_marks_orphans_unknown() {
        let store = QueryAuditStore::open_in_memory().await.unwrap();
        let orphan = store
            .record_started("SELECT 1", None, 1, "Query")
            .await
            .unwrap();
        let done = store
            .record_started("SELECT 2", None, 1, "Query")
            .await
            .unwrap();
        store
            .record_outcome(&done, QueryAuditStatus::Succeeded, Some(1), None)
            .await
            .unwrap();

        assert_eq!(
            store.reconcile_orphaned("server restarted").await.unwrap(),
            1
        );
        assert_eq!(
            store.get(&orphan).await.unwrap().unwrap()["status"],
            json!("unknown")
        );
        assert_eq!(
            store.get(&done).await.unwrap().unwrap()["status"],
            json!("succeeded")
        );
    }

    #[tokio::test]
    async fn prune_deletes_only_older_records() {
        let store = QueryAuditStore::open_in_memory().await.unwrap();
        store
            .record_started("SELECT 1", None, 1, "Query")
            .await
            .unwrap();
        assert_eq!(store.count().await.unwrap(), 1);

        // Nothing is older than an hour ago yet.
        assert_eq!(
            store
                .prune_before(chrono::Utc::now() - chrono::Duration::hours(1))
                .await
                .unwrap(),
            0
        );
        assert_eq!(store.count().await.unwrap(), 1);

        assert_eq!(store.prune_before(chrono::Utc::now()).await.unwrap(), 1);
        assert_eq!(store.count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn prune_deletes_across_multiple_batches() {
        // PRUNE_BATCH_SIZE is 1000; insert more than two batches' worth so a
        // single-DELETE implementation and a chunked one would both "work"
        // in the sense of deleting everything, but only the chunked one
        // does it as more than one bounded write.
        let store = QueryAuditStore::open_in_memory().await.unwrap();
        let total_rows = (PRUNE_BATCH_SIZE * 2) + 500;
        for _ in 0..total_rows {
            store
                .record_started("SELECT 1", None, 1, "Query")
                .await
                .unwrap();
        }
        assert_eq!(store.count().await.unwrap(), total_rows);

        let deleted = store.prune_before(chrono::Utc::now()).await.unwrap();
        assert_eq!(deleted, total_rows);
        assert_eq!(store.count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn record_pipeline_started_times_out_when_writer_thread_is_blocked() {
        // Finding: the timeout branch that fail-closed rests on had no test
        // exercising it directly — `close_for_test` only covers the
        // immediate-error path (`ConnectionClosed`). This makes the bound
        // injectable so a test can pin it far below AUDIT_WRITE_TIMEOUT.
        let store = QueryAuditStore::open_in_memory()
            .await
            .unwrap()
            .with_write_timeout(Duration::from_millis(100));

        // Occupy the dedicated writer thread well past the bound so the
        // next write has to wait behind it instead of running immediately.
        let blocker = store.conn.clone();
        let blocker_task = tokio::spawn(async move {
            let _ = blocker
                .call(|_conn| -> SqlResult<()> {
                    std::thread::sleep(Duration::from_millis(400));
                    Ok(())
                })
                .await;
        });
        // Give the blocking call a moment to actually start running on the
        // writer thread before racing it.
        tokio::time::sleep(Duration::from_millis(20)).await;

        let started = std::time::Instant::now();
        let result = store
            .record_pipeline_started("weekly-churn", "1.0.0", None)
            .await;
        let elapsed = started.elapsed();

        assert!(result.is_err(), "expected a timeout error, got {result:?}");
        assert!(
            elapsed < Duration::from_millis(400),
            "should have returned near the 100ms bound rather than waiting \
             out the full 400ms blocking call: {elapsed:?}"
        );

        blocker_task.await.unwrap();
    }

    #[tokio::test]
    async fn timed_out_pipeline_start_is_corrected_to_failed_once_it_lands() {
        // Covers the finding-2 fix: a pre-execution write that times out
        // from the caller's perspective, but whose INSERT still lands later
        // on the writer thread, must not sit there as an ambiguous
        // `started` -> `unknown` row. It should end up `failed` with
        // `audit_write_timeout` instead. The FIFO ordering the fix relies
        // on (INSERT queued before the corrective UPDATE) is guaranteed by
        // the crate, not by timing, so this is asserted with a bounded poll
        // rather than a fixed sleep — not because the outcome is racy, only
        // because *when* the writer thread gets to it is not.
        let store = QueryAuditStore::open_in_memory()
            .await
            .unwrap()
            .with_write_timeout(Duration::from_millis(100));

        let blocker = store.conn.clone();
        let blocker_task = tokio::spawn(async move {
            let _ = blocker
                .call(|_conn| -> SqlResult<()> {
                    std::thread::sleep(Duration::from_millis(300));
                    Ok(())
                })
                .await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let result = store
            .record_pipeline_started("weekly-churn", "1.0.0", Some("sess-timeout"))
            .await;
        assert!(result.is_err(), "expected a timeout error, got {result:?}");

        blocker_task.await.unwrap();

        let mut rows = Vec::new();
        for _ in 0..50 {
            rows = store.list_by_session("sess-timeout").await.unwrap();
            if rows
                .first()
                .is_some_and(|r| r["status"] != json!("started"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0]["status"], json!("failed"));
        assert_eq!(rows[0]["error"], json!("audit_write_timeout"));
        assert!(!rows[0]["finished_at"].is_null());
    }

    #[tokio::test]
    async fn on_disk_db_is_owner_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("audit.db");
        let store = QueryAuditStore::open(&path).await.unwrap();
        store
            .record_started("SELECT 1", None, 1, "Query")
            .await
            .unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for candidate in [
                path.clone(),
                PathBuf::from(format!("{}-wal", path.display())),
            ] {
                if candidate.exists() {
                    let mode = std::fs::metadata(&candidate).unwrap().permissions().mode();
                    assert_eq!(
                        mode & 0o077,
                        0,
                        "{} should not be group/other readable (mode {mode:o})",
                        candidate.display()
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn records_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.db");
        let id = {
            let store = QueryAuditStore::open(&path).await.unwrap();
            store
                .record_started("SELECT 'durable'", None, 1, "Query")
                .await
                .unwrap()
        };

        let reopened = QueryAuditStore::open(&path).await.unwrap();
        let record = reopened.get(&id).await.unwrap().unwrap();
        assert_eq!(record["sql"], json!("SELECT 'durable'"));
    }

    #[tokio::test]
    async fn pipeline_row_round_trips() {
        let store = QueryAuditStore::open_in_memory().await.unwrap();
        let id = store
            .record_pipeline_started("weekly-churn", "1.0.0", Some("sess-1"))
            .await
            .unwrap();
        store
            .record_outcome(&id, QueryAuditStatus::Succeeded, Some(42), None)
            .await
            .unwrap();
        let row = store.get(&id).await.unwrap().unwrap();
        assert_eq!(row["sql"], json!("weekly-churn@1.0.0"));
        assert_eq!(row["statement_kind"], json!("pipeline"));
        assert_eq!(row["session_id"], json!("sess-1"));
        assert_eq!(row["max_rows"], json!(0));
        assert_eq!(row["row_count"], json!(42));
        assert!(row["ai_context"].is_null());
    }

    #[tokio::test]
    async fn pipeline_row_without_session_has_null_session_id() {
        let store = QueryAuditStore::open_in_memory().await.unwrap();
        let id = store
            .record_pipeline_started("weekly-churn", "1.0.0", None)
            .await
            .unwrap();
        let row = store.get(&id).await.unwrap().unwrap();
        assert!(row["session_id"].is_null());
    }

    #[tokio::test]
    async fn list_by_session_interleaves_queries_and_pipelines() {
        let store = QueryAuditStore::open_in_memory().await.unwrap();
        let ctx = serde_json::json!({"purpose": "p", "session_id": "sess-mix"});
        store
            .record_started("SELECT 1", Some(&ctx), 10, "Query")
            .await
            .unwrap();
        store
            .record_pipeline_started("weekly-churn", "1.0.0", Some("sess-mix"))
            .await
            .unwrap();
        let rows = store.list_by_session("sess-mix").await.unwrap();
        assert_eq!(rows.len(), 2);
        // The promised property is the *ordering* (insertion order via
        // created_at with id as tie-break), plus the sql-column overload —
        // a bare count passes with ORDER BY dropped or reversed.
        assert_eq!(rows[0]["statement_kind"], json!("Query"));
        assert_eq!(rows[0]["sql"], json!("SELECT 1"));
        assert_eq!(rows[1]["statement_kind"], json!("pipeline"));
        assert_eq!(rows[1]["sql"], json!("weekly-churn@1.0.0"));
    }

    #[tokio::test]
    async fn orphaned_pipeline_rows_reconcile_to_unknown() {
        let store = QueryAuditStore::open_in_memory().await.unwrap();
        let id = store
            .record_pipeline_started("weekly-churn", "1.0.0", None)
            .await
            .unwrap();
        let n = store.reconcile_orphaned("test restart").await.unwrap();
        assert_eq!(n, 1);
        let row = store.get(&id).await.unwrap().unwrap();
        assert_eq!(row["status"], json!("unknown"));
    }
}
