//! Durable audit store for ad-hoc `/query` statements and pipeline
//! executions.
//!
//! Off unless the operator selects a backend: `--query-audit-db <path>` (the
//! SQLite file) or the [`PG_DSN_ENV`] environment variable (Postgres — an
//! env var because the DSN carries a credential; see [`postgres`]'s module doc for
//! the storage-swap rules). Either way, every statement the `/query`
//! endpoint accepts — and every `POST /:name/execute` pipeline run — is
//! written *before* execution and updated with its outcome afterwards, so
//! the store answers "what ran, on whose behalf, and did it succeed" rather
//! than merely "what was attempted".
//!
//! The `sql` column is overloaded by row kind: raw SQL for ad-hoc rows,
//! `name@version` for `statement_kind = 'pipeline'` rows (the versioned
//! template lives on disk; parameter values are never recorded — the version
//! is what keeps "what ran" answerable after the promotion loop edits a
//! template, since rows are kept forever by default). Ad-hoc rows carry
//! `statement_kind` values naming the server's statement *classification* —
//! `query` / `other` (see [`QUERY_STATEMENT_KIND`]) — not SQL verbs like
//! `select`.
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

mod postgres;
pub use postgres::PG_DSN_ENV;
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
/// `ai_context.session_id` and the `x-skardi-session-id` header consumed by
/// both the pipeline execute endpoint and the jobs submit endpoint (see
/// `query_handlers::validate_ai_context` and
/// `session_header::session_id_from_headers` in the server crate),
/// so all three paths can't drift apart. It is an opaque grouping key, not a
/// payload.
///
/// `skardi-cli` restates this cap under the same name
/// (`crates/cli/src/session.rs`) because the CLI crate does not depend on this
/// one; keep them in sync.
/// `pub` because the handlers that enforce this bound now live in a
/// different crate: `pub(crate)` was the right scope while this module was
/// inside the server, and the extraction is what widened it.
pub const MAX_SESSION_ID_CHARS: usize = 200;

/// Error from [`bounded`] that keeps "the write timed out" distinguishable
/// from "the write ran and failed" (e.g. `ConnectionClosed`). Only the
/// pre-execution writers act on that distinction — see
/// [`QueryAuditStore::spawn_timeout_correction`] — everywhere else this
/// converts straight to `anyhow::Error` via `?`, same as before.
#[derive(Debug)]
pub(crate) enum BoundedError {
    /// Elapsed `timeout` without the write completing. It may still land
    /// later on the writer thread.
    TimedOut(anyhow::Error),
    /// The write itself returned an error.
    Other(anyhow::Error),
}

impl BoundedError {
    pub(crate) fn is_timeout(&self) -> bool {
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
pub(crate) async fn bounded<T, E>(
    write: impl Future<Output = std::result::Result<T, E>>,
    what: &'static str,
    timeout: Duration,
) -> std::result::Result<T, BoundedError>
where
    E: std::error::Error + Send + Sync + 'static,
{
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
/// `pub` for the same reason as [`MAX_SESSION_ID_CHARS`] — the ad-hoc and
/// pipeline handlers both stamp their terminal outcome through this, and they
/// are no longer in this crate.
pub async fn finish_audit(
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
    error          TEXT,
    request_id     TEXT,
    org_id         TEXT,
    workspace_id   TEXT,
    user_id        TEXT,
    run_id         TEXT,
    job_run_id     TEXT
);
CREATE INDEX IF NOT EXISTS idx_query_audit_session_created
    ON query_audit (session_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_query_audit_created
    ON query_audit (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_query_audit_status
    ON query_audit (status);
CREATE INDEX IF NOT EXISTS idx_query_audit_statement_kind
    ON query_audit (statement_kind);";

/// The five identity columns, in order. `CREATE TABLE IF NOT EXISTS` cannot
/// retrofit an existing table, so [`ensure_added_columns`] reconciles the
/// live schema on every open — additive, idempotent, and a no-op on fresh
/// databases.
const IDENTITY_COLUMNS: [&str; 5] = ["request_id", "org_id", "workspace_id", "user_id", "run_id"];

/// Columns bridging a ledger row to another ledger's record of the same work.
/// Reconciled by the same mechanism as [`IDENTITY_COLUMNS`], but kept a
/// separate list because they answer a different question: identity is *who
/// asked*, a bridge is *what the work became*.
///
/// `job_run_id` holds `job_runs.id` for `statement_kind = 'job'` rows.
/// Deliberately not the identity envelope's `run_id`, which names the caller's
/// own run — one column, one meaning.
const BRIDGE_COLUMNS: [&str; 1] = ["job_run_id"];

/// Index over the `job_run_id` bridge, applied after [`ensure_added_columns`]
/// has guaranteed the column exists — so it cannot live in
/// [`INIT_SCHEMA_SQL`], which also runs against pre-column databases, where
/// indexing a missing column would fail.
///
/// The forward direction — session to its rows — is served by
/// `idx_query_audit_session_created`. This covers the reverse: given a run id
/// from `GET /jobs/runs`, which session submitted it. Partial, because
/// `job_run_id` is NULL on every non-job row, so the index stays proportional
/// to job submissions rather than to a ledger that is append-only and has
/// retention off by default.
const JOB_RUN_ID_INDEX_SQL: &str = "CREATE INDEX IF NOT EXISTS idx_query_audit_job_run_id
    ON query_audit (job_run_id) WHERE job_run_id IS NOT NULL;";

/// True for the `duplicate column name` error SQLite raises when an
/// `ALTER TABLE ... ADD COLUMN` loses a race with another connection that
/// already added it.
///
/// Matched on message text because `rusqlite` surfaces it as a generic
/// `SqliteFailure` with `SQLITE_ERROR`, carrying no distinguishing code.
fn is_duplicate_column(e: &rusqlite::Error) -> bool {
    e.to_string().contains("duplicate column name")
}

/// Add any of `columns` the live table is missing, as nullable `TEXT`.
fn ensure_added_columns(conn: &rusqlite::Connection, columns: &[&str]) -> SqlResult<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(query_audit)")?;
    let existing: std::collections::HashSet<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<std::result::Result<_, _>>()?;
    for col in columns {
        if !existing.contains(*col) {
            // The read above is not atomic with this ALTER: two processes
            // opening the same file concurrently (a rolling restart, an
            // operator tool, a second server pointed at the same path) can
            // both observe the column missing and both issue it. The loser
            // gets `duplicate column name` — which is exactly the
            // post-condition this step wanted, so it counts as success rather
            // than aborting startup over a benign race.
            match conn.execute(
                &format!("ALTER TABLE query_audit ADD COLUMN {col} TEXT"),
                [],
            ) {
                Ok(_) => {}
                Err(e) if is_duplicate_column(&e) => {}
                Err(e) => return Err(e),
            }
        }
    }
    Ok(())
}

/// Normalise the pre-#219 `statement_kind` casing on rows already on disk.
///
/// Ad-hoc rows used to record the `Debug` form of `StatementKind` — `Query`
/// and `Other`. They now record [`QUERY_STATEMENT_KIND`] /
/// [`OTHER_STATEMENT_KIND`], so without this a ledger written before the
/// change holds both casings for one concept: `WHERE statement_kind =
/// 'query'` (the filter `docs/server.md` documents) silently returns only
/// post-upgrade rows, and `= 'Query'` silently loses everything after.
/// `idx_query_audit_statement_kind` makes both look fast, so the only symptom
/// is wrong counts. The ledger is append-only with retention off by default,
/// so those rows never age out on their own.
///
/// Idempotent and a no-op on fresh databases, like the column reconcile it
/// sits beside. `statement_kind` carries no `COLLATE NOCASE`, so the match is
/// case-sensitive and normalised rows cannot be rewritten again. `pipeline`
/// and `job` were lowercase from the start and are untouched.
fn normalise_statement_kind_casing(conn: &rusqlite::Connection) -> SqlResult<()> {
    for (legacy, current) in [
        ("Query", QUERY_STATEMENT_KIND),
        ("Other", OTHER_STATEMENT_KIND),
    ] {
        conn.execute(
            "UPDATE query_audit SET statement_kind = ?1 WHERE statement_kind = ?2",
            params![current, legacy],
        )?;
    }
    Ok(())
}

/// Every column added to `query_audit` after its original DDL, the indexes
/// that can only be built once those columns exist, and the one-shot data
/// normalisations that keep an existing file consistent with what the current
/// code writes.
fn ensure_schema_additions(conn: &rusqlite::Connection) -> SqlResult<()> {
    ensure_added_columns(conn, &IDENTITY_COLUMNS)?;
    ensure_added_columns(conn, &BRIDGE_COLUMNS)?;
    conn.execute_batch(JOB_RUN_ID_INDEX_SQL)?;
    normalise_statement_kind_casing(conn)?;
    Ok(())
}

/// Who ran the statement, when the distribution knows. Every field optional:
/// the OSS single-user server records none of them; an authenticated
/// distribution (e.g. an engine fronted by an identity-minting gateway)
/// fills what its envelope carries. Nullable columns, never a fork — the
/// same table remains readable by the same tooling either way.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryIdentity {
    pub request_id: Option<String>,
    pub org_id: Option<String>,
    pub workspace_id: Option<String>,
    pub user_id: Option<String>,
    pub run_id: Option<String>,
}

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

/// Durable audit ledger for `/query` — the SQLite file by default, or the
/// Postgres backend when [`PG_DSN_ENV`] selects it. Same fail-closed
/// contract either way; see [`postgres`]'s module doc for the storage-swap rules
/// and the one honest divergence.
pub struct QueryAuditStore {
    backend: Backend,
    /// Bound passed to [`bounded`] for every write. [`AUDIT_WRITE_TIMEOUT`]
    /// in production; overridable in tests via [`Self::with_write_timeout`]
    /// so the timeout branch can be exercised without waiting 5s.
    write_timeout: Duration,
}

/// The two storage arms. Private: callers select via [`QueryAuditStore::open`]
/// vs [`QueryAuditStore::open_postgres`] and are backend-blind afterwards.
enum Backend {
    Sqlite { conn: Connection, path: PathBuf },
    Postgres(postgres::PgAudit),
}

impl std::fmt::Debug for QueryAuditStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryAuditStore")
            .field("path", &self.path())
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
            // SQLite installs no busy handler by default: a write that finds
            // the lock held returns `SQLITE_BUSY` immediately. Two servers
            // pointed at one `--query-audit-db` — the case
            // `ensure_added_columns` and the `started`-only guard are written
            // for — would then fail on the lock before ever reaching the
            // races being guarded: B's `ALTER TABLE` during A's INSERT aborts
            // B's startup with `database is locked`, and an audited request
            // that loses the lock 503s under a fail-closed policy that was
            // meant for real write failures. Bounded by the same
            // `AUDIT_WRITE_TIMEOUT` the callers already wait out, so waiting
            // for the lock cannot outlast the request bound. Matches the
            // sqlite source provider, which defaults `busy_timeout_ms` to
            // 5000 rather than taking SQLite's default.
            conn.busy_timeout(AUDIT_WRITE_TIMEOUT)?;
            conn.execute_batch(INIT_SCHEMA_SQL)?;
            ensure_schema_additions(conn)?;
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
            backend: Backend::Sqlite { conn, path },
            write_timeout: AUDIT_WRITE_TIMEOUT,
        })
    }

    /// Open the POSTGRES-backed ledger at `dsn` (see [`PG_DSN_ENV`]).
    ///
    /// Connects eagerly and applies the schema: like [`Self::open`], errors
    /// are fatal to startup by design — an operator who asked for an audit
    /// trail must not get a server that quietly runs without one. The DSN is
    /// never logged; `path()`/`Debug` render the redacted authority only.
    pub async fn open_postgres(dsn: &str) -> Result<Self> {
        Ok(Self {
            backend: Backend::Postgres(postgres::PgAudit::open(dsn).await?),
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
            ensure_schema_additions(conn)?;
            Ok(())
        })
        .await
        .context("Failed to initialise in-memory query-audit db")?;
        Ok(Self {
            backend: Backend::Sqlite {
                conn,
                path: PathBuf::from(":memory:"),
            },
            write_timeout: AUDIT_WRITE_TIMEOUT,
        })
    }

    /// The ledger's location: the file path on SQLite, the redacted
    /// `postgres://host/db` authority on Postgres (never the DSN — it
    /// carries a credential).
    pub fn path(&self) -> &Path {
        match &self.backend {
            Backend::Sqlite { path, .. } => path,
            Backend::Postgres(pg) => pg.redacted(),
        }
    }

    /// The sqlite connection, tests only — a handful of tests hold the
    /// writer thread hostage or issue raw SQL to stage legacy data.
    #[cfg(test)]
    fn sqlite_conn(&self) -> Connection {
        match &self.backend {
            Backend::Sqlite { conn, .. } => conn.clone(),
            Backend::Postgres(_) => panic!("test helper: sqlite backend expected"),
        }
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
        match &self.backend {
            Backend::Sqlite { conn, .. } => {
                let _ = conn.clone().close().await;
            }
            Backend::Postgres(pg) => pg.close_for_test().await,
        }
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
        self.record_started_for(sql, ai_context, max_rows, statement_kind, None)
            .await
    }

    /// [`record_started`], carrying the caller's identity when the
    /// distribution has one. `None` fields land as NULL — the OSS server
    /// always passes `None` for the whole thing.
    pub async fn record_started_for(
        &self,
        sql: &str,
        ai_context: Option<&Value>,
        max_rows: usize,
        statement_kind: &str,
        identity: Option<&QueryIdentity>,
    ) -> Result<String> {
        let conn = match &self.backend {
            // The pg arm owns its timeout correction (it holds the row id).
            Backend::Postgres(pg) => {
                return pg
                    .record_started_for(
                        sql,
                        ai_context,
                        max_rows,
                        statement_kind,
                        identity,
                        self.write_timeout,
                    )
                    .await
                    .map_err(Into::into);
            }
            Backend::Sqlite { conn, .. } => conn,
        };
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
        let identity = identity.cloned().unwrap_or_default();

        match bounded(
            conn.call(move |conn| -> SqlResult<()> {
                conn.execute(
                    "INSERT INTO query_audit
                        (id, created_at, sql, ai_context, session_id, max_rows,
                         statement_kind, status,
                         request_id, org_id, workspace_id, user_id, run_id)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                    params![
                        row_id,
                        created_at,
                        sql,
                        ai_context,
                        session_id,
                        max_rows as i64,
                        statement_kind,
                        QueryAuditStatus::Started.as_str(),
                        identity.request_id,
                        identity.org_id,
                        identity.workspace_id,
                        identity.user_id,
                        identity.run_id,
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
        let conn = match &self.backend {
            Backend::Postgres(pg) => {
                return pg
                    .record_name_at_version_started(
                        &format!("{pipeline_name}@{version}"),
                        session_id,
                        PIPELINE_STATEMENT_KIND,
                        self.write_timeout,
                    )
                    .await
                    .map_err(Into::into);
            }
            Backend::Sqlite { conn, .. } => conn,
        };
        let id = new_id();
        let created_at = chrono::Utc::now().to_rfc3339();
        let name_at_version = format!("{pipeline_name}@{version}");
        let session_id = session_id.map(str::to_string);
        let row_id = id.clone();

        match bounded(
            conn.call(move |conn| -> SqlResult<()> {
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

    /// Insert a `started` row for a job *submission*.
    ///
    /// The row's lifecycle is the submission's, not the run's: `succeeded`
    /// means "accepted and enqueued" (stamped with the `job_run_id` that
    /// bridges to the jobs ledger, the authority on the run itself), `failed` means
    /// the executor rejected it. Stores `name@version`; parameter values are
    /// never recorded here — `job_runs.parameters` is a separate concern.
    pub async fn record_job_submitted(
        &self,
        job_name: &str,
        version: &str,
        session_id: Option<&str>,
    ) -> Result<String> {
        let conn = match &self.backend {
            Backend::Postgres(pg) => {
                return pg
                    .record_name_at_version_started(
                        &format!("{job_name}@{version}"),
                        session_id,
                        JOB_STATEMENT_KIND,
                        self.write_timeout,
                    )
                    .await
                    .map_err(Into::into);
            }
            Backend::Sqlite { conn, .. } => conn,
        };
        let id = new_id();
        let created_at = chrono::Utc::now().to_rfc3339();
        let name_at_version = format!("{job_name}@{version}");
        let session_id = session_id.map(str::to_string);
        let row_id = id.clone();

        match bounded(
            conn.call(move |conn| -> SqlResult<()> {
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
                        JOB_STATEMENT_KIND,
                        QueryAuditStatus::Started.as_str(),
                    ],
                )?;
                Ok(())
            }),
            "Failed to write pre-execution job-audit record",
            self.write_timeout,
        )
        .await
        {
            Ok(()) => Ok(id),
            Err(e) => {
                // Same reasoning as `record_pipeline_started`: this is a
                // pre-execution write, so a timeout means the caller has
                // already been told "503, the job was not submitted" — a
                // fact worth recording precisely instead of letting startup
                // reconcile the abandoned row to the ambiguous `unknown`.
                if e.is_timeout() {
                    self.spawn_timeout_correction(id);
                }
                Err(e.into())
            }
        }
    }

    /// Update a job-submission record with its terminal outcome, stamping the
    /// `job_run_id` that bridges this row to the jobs ledger's authoritative
    /// record of the run itself. `job_run_id` is `None` when the submission was
    /// rejected before a run was ever created.
    ///
    /// Distinct from the identity envelope's `run_id` ([`QueryIdentity`]),
    /// which names the *caller's* run rather than the job run this submission
    /// produced. One column, one meaning.
    ///
    /// Only applies to a row still in `started`, the same guard (and the same
    /// reasoning) as [`Self::spawn_timeout_correction`]: the write is
    /// monotonic, so a row that already reached a terminal state stays there.
    /// Within one process the transition is unreachable — the 503 path returns
    /// before any outcome stamp — but the ledger is a *file*, and nothing stops
    /// two servers being pointed at one `--query-audit-db`. In that
    /// configuration server B's startup [`Self::reconcile_orphaned`] can stamp
    /// A's in-flight submission `unknown` before A's outcome write lands;
    /// without this guard that late write would resurrect a terminal row and
    /// defeat reconciliation. 0 rows updated is therefore not an error: it
    /// means something else already settled the row.
    pub async fn record_job_outcome(
        &self,
        id: &str,
        job_run_id: Option<&str>,
        status: QueryAuditStatus,
        error: Option<&str>,
    ) -> Result<()> {
        let conn = match &self.backend {
            Backend::Postgres(pg) => {
                return pg
                    .record_job_outcome(id, job_run_id, status, error, self.write_timeout)
                    .await;
            }
            Backend::Sqlite { conn, .. } => conn,
        };
        let id = id.to_string();
        let finished_at = chrono::Utc::now().to_rfc3339();
        let status = status.as_str();
        let job_run_id = job_run_id.map(str::to_string);
        let error = error.map(str::to_string);
        bounded(
            conn.call(move |conn| -> SqlResult<()> {
                conn.execute(
                    "UPDATE query_audit
                        SET status = ?2, finished_at = ?3, job_run_id = ?4, error = ?5
                      WHERE id = ?1 AND status = ?6",
                    params![
                        id,
                        status,
                        finished_at,
                        job_run_id,
                        error,
                        QueryAuditStatus::Started.as_str(),
                    ],
                )?;
                Ok(())
            }),
            "Failed to update job-audit record",
            self.write_timeout,
        )
        .await?;
        Ok(())
    }

    /// Update a record with its terminal outcome.
    ///
    /// Shares [`Self::record_job_outcome`]'s `started`-only guard, for the
    /// same reason.
    pub async fn record_outcome(
        &self,
        id: &str,
        status: QueryAuditStatus,
        row_count: Option<usize>,
        error: Option<&str>,
    ) -> Result<()> {
        let conn = match &self.backend {
            Backend::Postgres(pg) => {
                return pg
                    .record_outcome(id, status, row_count, error, self.write_timeout)
                    .await;
            }
            Backend::Sqlite { conn, .. } => conn,
        };
        let id = id.to_string();
        let finished_at = chrono::Utc::now().to_rfc3339();
        let status = status.as_str();
        let row_count = row_count.map(|n| n as i64);
        let error = error.map(str::to_string);

        bounded(
            conn.call(move |conn| -> SqlResult<()> {
                conn.execute(
                    "UPDATE query_audit
                        SET status = ?2, finished_at = ?3, row_count = ?4, error = ?5
                      WHERE id = ?1 AND status = ?6",
                    params![
                        id,
                        status,
                        finished_at,
                        row_count,
                        error,
                        QueryAuditStatus::Started.as_str(),
                    ],
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
        let conn = match &self.backend {
            Backend::Postgres(pg) => {
                return pg.spawn_timeout_correction(id, self.write_timeout);
            }
            Backend::Sqlite { conn, .. } => conn.clone(),
        };
        // The correction is triggered by a stalled writer, which is exactly
        // the condition under which awaiting it could never resolve — so the
        // task gives up on the *await*, not on the work. The UPDATE closure
        // is already in the channel by then and runs whenever the writer
        // drains, whether or not anyone is still listening; bounding the
        // await only stops one parked task per timed-out write from
        // accumulating for the duration of the stall.
        let correction_bound = self.write_timeout;
        tokio::spawn(async move {
            let finished_at = chrono::Utc::now().to_rfc3339();
            let update = conn.call(move |conn| -> SqlResult<usize> {
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
            });
            match tokio::time::timeout(correction_bound, update).await {
                // 0 rows means either the INSERT never landed, or it landed
                // and something else already moved it off `started` —
                // either way there is nothing to correct.
                Ok(Ok(_)) => {}
                Ok(Err(e)) => tracing::warn!(
                    "Failed to apply audit-write-timeout correction to query-audit record: {e}"
                ),
                Err(_) => tracing::warn!(
                    "Audit-write-timeout correction still queued after {correction_bound:?}; \
                     it will apply when the writer drains"
                ),
            }
        });
    }

    /// Rewrite rows still marked `started` to `unknown`. Called at startup so a
    /// crash-killed query does not masquerade as still running.
    pub async fn reconcile_orphaned(&self, reason: &str) -> Result<usize> {
        let conn = match &self.backend {
            Backend::Postgres(pg) => return pg.reconcile_orphaned(reason).await,
            Backend::Sqlite { conn, .. } => conn,
        };
        let reason = reason.to_string();
        let finished_at = chrono::Utc::now().to_rfc3339();
        let updated = conn
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

    /// Job rows whose forward pointer was lost: `status = unknown` with no
    /// `job_run_id`. Returns their ids, oldest first.
    ///
    /// Exactly the rows [`Self::record_job_outcome`] can no longer repair — its
    /// `WHERE status = 'started'` guard means once `reconcile_orphaned` has
    /// rewritten a row to `unknown`, no later well-behaved write can ever stamp
    /// it. Without a pass like this, "the correlation is recoverable" is only
    /// true for an operator with `sqlite3` and both ledger files open; the row
    /// an auditor actually reads still says `unknown, NULL`, which for a job
    /// row means "definitely submitted, linkage lost" while reading identically
    /// to a query row's "may have run after a crash".
    ///
    /// The repair itself lives in the server, which is the only layer holding
    /// both ledgers: `job_runs.submission_id` carries this id, so each of these
    /// resolves through `JobStore::get_run_by_submission_id`.
    pub async fn job_rows_missing_run_id(&self) -> Result<Vec<String>> {
        let conn = match &self.backend {
            Backend::Postgres(pg) => return pg.job_rows_missing_run_id().await,
            Backend::Sqlite { conn, .. } => conn,
        };
        let ids = conn
            .call(move |conn| -> SqlResult<Vec<String>> {
                let mut stmt = conn.prepare(
                    "SELECT id FROM query_audit
                      WHERE statement_kind = ?1
                        AND status = ?2
                        AND job_run_id IS NULL
                      ORDER BY created_at ASC",
                )?;
                let rows = stmt.query_map(
                    params![JOB_STATEMENT_KIND, QueryAuditStatus::Unknown.as_str()],
                    |row| row.get::<_, String>(0),
                )?;
                rows.collect()
            })
            .await
            .context("Failed to list job rows missing job_run_id")?;
        Ok(ids)
    }

    /// Stamp `job_run_id` onto a row whose forward pointer was lost. Returns
    /// whether a row was updated.
    ///
    /// Deliberately not reusing [`Self::record_job_outcome`]: that guards on
    /// `status = 'started'`, which is precisely the state these rows are no
    /// longer in. The guard here is the mirror image — `unknown` with the
    /// column still NULL — so a repair can neither overwrite a pointer that was
    /// written correctly nor touch a row that is still live. `status` is left
    /// as `unknown`, which stays the truth: the outcome was never observed, only
    /// the linkage is recovered.
    pub async fn backfill_job_run_id(&self, id: &str, job_run_id: &str) -> Result<bool> {
        let conn = match &self.backend {
            Backend::Postgres(pg) => return pg.backfill_job_run_id(id, job_run_id).await,
            Backend::Sqlite { conn, .. } => conn,
        };
        let id = id.to_string();
        let job_run_id = job_run_id.to_string();
        let updated = conn
            .call(move |conn| -> SqlResult<usize> {
                conn.execute(
                    "UPDATE query_audit
                        SET job_run_id = ?2
                      WHERE id = ?1
                        AND job_run_id IS NULL
                        AND status = ?3",
                    params![id, job_run_id, QueryAuditStatus::Unknown.as_str()],
                )
            })
            .await
            .context("Failed to backfill job_run_id")?;
        Ok(updated > 0)
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
        let conn = match &self.backend {
            Backend::Postgres(pg) => return pg.prune_before(cutoff, self.write_timeout).await,
            Backend::Sqlite { conn, .. } => conn,
        };
        let cutoff = cutoff.to_rfc3339();
        let mut total = 0usize;
        loop {
            let batch_cutoff = cutoff.clone();
            let deleted = bounded(
                conn.call(move |conn| -> SqlResult<usize> {
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
        let conn = match &self.backend {
            Backend::Postgres(pg) => return pg.get(id).await,
            Backend::Sqlite { conn, .. } => conn,
        };
        let id = id.to_string();
        let row = conn
            .call(move |conn| -> SqlResult<Option<Value>> {
                let mut stmt = conn.prepare(
                    "SELECT id, created_at, finished_at, sql, ai_context, session_id,
                            max_rows, statement_kind, status, row_count, error,
                            request_id, org_id, workspace_id, user_id, run_id,
                            job_run_id
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
                            "request_id": row.get::<_, Option<String>>(11)?,
                            "org_id": row.get::<_, Option<String>>(12)?,
                            "workspace_id": row.get::<_, Option<String>>(13)?,
                            "user_id": row.get::<_, Option<String>>(14)?,
                            "run_id": row.get::<_, Option<String>>(15)?,
                            "job_run_id": row.get::<_, Option<String>>(16)?,
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
        let conn = match &self.backend {
            Backend::Postgres(pg) => {
                let ids = pg.list_session_ids(session_id).await?;
                let mut out = Vec::with_capacity(ids.len());
                for id in ids {
                    if let Some(record) = self.get(&id).await? {
                        out.push(record);
                    }
                }
                return Ok(out);
            }
            Backend::Sqlite { conn, .. } => conn,
        };
        let session_id = session_id.to_string();
        let ids = conn
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
        let conn = match &self.backend {
            Backend::Postgres(pg) => return pg.count().await,
            Backend::Sqlite { conn, .. } => conn,
        };
        let n = conn
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
pub(crate) const PIPELINE_STATEMENT_KIND: &str = "pipeline";

/// Statement-kind markers for the two ad-hoc row kinds.
///
/// `pub` and named rather than derived from the server's statement-classifier
/// `Debug` form. The ledger's `statement_kind` vocabulary is a consumer-facing
/// contract ("consumers filtering the ledger must match these exact strings"),
/// and leaking `Debug` both bound that contract to a type in another crate
/// whose variant names could be renamed freely, and mixed `PascalCase` ad-hoc
/// values with the `lowercase` `pipeline` / `job` markers. All four values now
/// live here, in one casing.
///
/// The classifier itself stays in the server — this crate deliberately does
/// not depend on `skardi` (#206) — so the mapping from its variants onto these
/// constants lives in `server::query_handlers::adhoc_statement_kind`. The
/// vocabulary is owned here; only the translation is over there.
pub const QUERY_STATEMENT_KIND: &str = "query";
/// See [`QUERY_STATEMENT_KIND`].
pub const OTHER_STATEMENT_KIND: &str = "other";

/// Statement-kind marker for job-submission rows. Nothing outside this
/// module reads it directly (Task 2 goes through `record_job_submitted`),
/// so it stays private, mirroring `PIPELINE_STATEMENT_KIND`'s narrowing.
pub(crate) const JOB_STATEMENT_KIND: &str = "job";

/// `max_rows` does not apply to pipeline executions, but the column is NOT
/// NULL; pipeline rows store this sentinel. Job-submission rows share it for
/// the same reason — `max_rows` has no meaning for a job run either.
///
/// The sentinel is unambiguous only because `/query` rejects
/// `max_rows: Some(0)` up front (the `Some(0)` arm in
/// `query_handlers::execute_query`), so `0` can never appear on an ad-hoc
/// row. That invariant lives in a
/// different module — if `/query` ever starts allowing `max_rows: 0`, this
/// sentinel needs a new value (or a dedicated column) first.
pub(crate) const PIPELINE_MAX_ROWS_SENTINEL: i64 = 0;

/// Rows deleted per [`QueryAuditStore::prune_before`] batch. Small enough
/// that a batch delete is comfortably inside [`AUDIT_WRITE_TIMEOUT`] even on
/// a slow disk; see that method's doc comment for why batching exists at
/// all.
pub(crate) const PRUNE_BATCH_SIZE: usize = 1000;

/// Random-ish unique row id. Avoids a `uuid` dependency: the timestamp keeps
/// ids ordered and the zero-padded hex counter disambiguates within the same
/// nanosecond without breaking lexicographic order — `list_by_session`'s
/// `id ASC` tie-break (and any other string sort) depends on same-width
/// hex; an unpadded counter would sort `…-f` after `…-10`.
pub(crate) fn new_id() -> String {
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

    /// The identity columns exist for downstream distributions whose caller
    /// is authenticated (the cloud engine's envelope): five nullable columns,
    /// written when the caller supplies them, NULL otherwise. OSS rows leave
    /// them empty — same table, same file, same tooling.
    #[tokio::test]
    async fn identity_travels_when_supplied_and_is_null_otherwise() {
        let store = QueryAuditStore::open_in_memory().await.unwrap();
        let identity = QueryIdentity {
            request_id: Some("req-9c41".into()),
            org_id: Some("acme".into()),
            workspace_id: Some("ws-core".into()),
            user_id: Some("user:acme/alice".into()),
            run_id: None,
        };
        let with_id = store
            .record_started_for("SELECT 1", None, 10, "Query", Some(&identity))
            .await
            .unwrap();
        let record = store.get(&with_id).await.unwrap().unwrap();
        assert_eq!(record["request_id"], json!("req-9c41"));
        assert_eq!(record["org_id"], json!("acme"));
        assert_eq!(record["workspace_id"], json!("ws-core"));
        assert_eq!(record["user_id"], json!("user:acme/alice"));
        assert!(record["run_id"].is_null());

        let anon = store
            .record_started("SELECT 2", None, 10, "Query")
            .await
            .unwrap();
        let record = store.get(&anon).await.unwrap().unwrap();
        for col in ["request_id", "org_id", "workspace_id", "user_id", "run_id"] {
            assert!(record[col].is_null(), "OSS rows leave {col} NULL");
        }
    }

    /// A database created by a pre-identity binary gains the columns on open,
    /// idempotently — `CREATE TABLE IF NOT EXISTS` alone cannot retrofit an
    /// existing table, so open() must reconcile the live schema.
    #[tokio::test]
    async fn an_old_database_gains_the_identity_columns_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.db");
        {
            // Simulate the pre-identity schema exactly.
            let conn = Connection::open(&path).await.unwrap();
            conn.call(|conn| -> SqlResult<()> {
                conn.execute_batch(
                    "CREATE TABLE query_audit (
                        id TEXT PRIMARY KEY, created_at TEXT NOT NULL,
                        finished_at TEXT, sql TEXT NOT NULL, ai_context TEXT,
                        session_id TEXT, max_rows INTEGER NOT NULL,
                        statement_kind TEXT NOT NULL, status TEXT NOT NULL,
                        row_count INTEGER, error TEXT);",
                )?;
                conn.execute(
                    "INSERT INTO query_audit (id, created_at, sql, max_rows, statement_kind, status)
                     VALUES ('old-row', '2026-01-01T00:00:00Z', 'SELECT 0', 1, 'Query', 'succeeded')",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
            conn.close().await.unwrap();
        }

        // Open twice: the migration must be idempotent.
        for _ in 0..2 {
            let store = QueryAuditStore::open(&path).await.unwrap();
            let record = store.get("old-row").await.unwrap().unwrap();
            assert_eq!(record["sql"], json!("SELECT 0"), "old rows survive");
            assert!(record["user_id"].is_null(), "old rows read NULL identity");
        }
    }

    #[tokio::test]
    async fn records_sql_ai_context_and_outcome() {
        let store = QueryAuditStore::open_in_memory().await.unwrap();
        let ctx = json!({"purpose": "kyc", "session_id": "sess-1"});
        let id = store
            .record_started(
                "SELECT * FROM t WHERE ssn = '123-45-6789'",
                Some(&ctx),
                100,
                "query",
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
        assert_eq!(record["statement_kind"], json!("query"));
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
            .record_started("SELECT 1", None, 1, "query")
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
            .record_started("SELECT 1", Some(&ctx), 1, "query")
            .await
            .unwrap();
        store
            .record_started("SELECT 2", Some(&other), 1, "query")
            .await
            .unwrap();
        store
            .record_started("SELECT 3", Some(&ctx), 1, "query")
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
            .record_started("SELECT 1", None, 1, "query")
            .await
            .unwrap();
        let done = store
            .record_started("SELECT 2", None, 1, "query")
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
            .record_started("SELECT 1", None, 1, "query")
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
                .record_started("SELECT 1", None, 1, "query")
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
        let blocker = store.sqlite_conn();
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

        let blocker = store.sqlite_conn();
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
            .record_started("SELECT 1", None, 1, "query")
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
                .record_started("SELECT 'durable'", None, 1, "query")
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
            .record_started("SELECT 1", Some(&ctx), 10, "query")
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
        assert_eq!(rows[0]["statement_kind"], json!("query"));
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

    #[tokio::test]
    async fn only_unknown_job_rows_with_a_null_pointer_are_repair_candidates() {
        let store = QueryAuditStore::open_in_memory().await.unwrap();

        // A row whose stamp was lost: reconciled to `unknown`, pointer NULL.
        let lost = store
            .record_job_submitted("nightly", "1.0.0", None)
            .await
            .unwrap();
        // A row that recorded its outcome normally.
        let stamped = store
            .record_job_submitted("nightly", "1.0.0", None)
            .await
            .unwrap();
        store
            .record_job_outcome(&stamped, Some("run-ok"), QueryAuditStatus::Succeeded, None)
            .await
            .unwrap();
        // A non-job row, also reconciled — must never be a candidate.
        let query_row = store
            .record_started("SELECT 1", None, 10, "Query")
            .await
            .unwrap();

        store.reconcile_orphaned("crash").await.unwrap();

        let candidates = store.job_rows_missing_run_id().await.unwrap();
        assert_eq!(candidates, vec![lost.clone()]);
        assert!(!candidates.contains(&stamped), "a stamped row is not lost");
        assert!(!candidates.contains(&query_row), "query rows are not jobs");
    }

    #[tokio::test]
    async fn backfill_restores_a_lost_pointer_and_refuses_every_other_row() {
        let store = QueryAuditStore::open_in_memory().await.unwrap();
        let lost = store
            .record_job_submitted("nightly", "1.0.0", Some("sess-x"))
            .await
            .unwrap();
        store.reconcile_orphaned("crash").await.unwrap();

        assert!(
            store.backfill_job_run_id(&lost, "run-found").await.unwrap(),
            "a lost row must be repairable"
        );
        let row = store.get(&lost).await.unwrap().unwrap();
        assert_eq!(row["job_run_id"], json!("run-found"));
        // The outcome was never observed; only the linkage is recovered.
        assert_eq!(row["status"], json!("unknown"));
        assert_eq!(row["session_id"], json!("sess-x"));

        // Idempotent: a second pass finds nothing to do and cannot overwrite.
        assert!(
            !store.backfill_job_run_id(&lost, "run-other").await.unwrap(),
            "a repaired row must not be rewritten"
        );
        assert_eq!(
            store.get(&lost).await.unwrap().unwrap()["job_run_id"],
            json!("run-found")
        );
        assert!(store.job_rows_missing_run_id().await.unwrap().is_empty());

        // A row still `started` is live, not lost — the guard must decline it,
        // so a repair pass can never race a submission in flight.
        let live = store
            .record_job_submitted("nightly", "1.0.0", None)
            .await
            .unwrap();
        assert!(
            !store.backfill_job_run_id(&live, "run-live").await.unwrap(),
            "a started row must be left to record_job_outcome"
        );
        assert!(store.get(&live).await.unwrap().unwrap()["job_run_id"].is_null());
    }

    #[tokio::test]
    async fn record_job_outcome_cannot_repair_what_reconcile_already_touched() {
        // The premise the repair pass exists for: once a row is `unknown`, the
        // `status = 'started'` guard means no later well-behaved write can ever
        // stamp it. If this ever stops being true, the pass is redundant.
        let store = QueryAuditStore::open_in_memory().await.unwrap();
        let id = store
            .record_job_submitted("nightly", "1.0.0", None)
            .await
            .unwrap();
        store.reconcile_orphaned("crash").await.unwrap();

        store
            .record_job_outcome(&id, Some("run-late"), QueryAuditStatus::Succeeded, None)
            .await
            .unwrap();
        let row = store.get(&id).await.unwrap().unwrap();
        assert!(
            row["job_run_id"].is_null(),
            "record_job_outcome stamped a reconciled row; the repair pass is now dead code"
        );
        assert_eq!(row["status"], json!("unknown"));
    }

    #[tokio::test]
    async fn job_row_round_trips_with_job_run_id() {
        let store = QueryAuditStore::open_in_memory().await.unwrap();
        let id = store
            .record_job_submitted("nightly-backfill", "2.1.0", Some("sess-j"))
            .await
            .unwrap();
        store
            .record_job_outcome(&id, Some("run-abc123"), QueryAuditStatus::Succeeded, None)
            .await
            .unwrap();
        let row = store.get(&id).await.unwrap().unwrap();
        assert_eq!(row["sql"], json!("nightly-backfill@2.1.0"));
        assert_eq!(row["statement_kind"], json!("job"));
        assert_eq!(row["session_id"], json!("sess-j"));
        assert_eq!(row["status"], json!("succeeded"));
        assert_eq!(row["job_run_id"], json!("run-abc123"));
        // The identity envelope's `run_id` is a different column with a
        // different meaning and must stay untouched by the bridge stamp.
        assert!(row["run_id"].is_null());
        assert_eq!(row["max_rows"], json!(0));
        assert!(row["ai_context"].is_null());
        assert!(row["row_count"].is_null());
    }

    #[tokio::test]
    async fn rejected_job_submission_records_fixed_kind_and_null_job_run_id() {
        let store = QueryAuditStore::open_in_memory().await.unwrap();
        let id = store
            .record_job_submitted("nightly-backfill", "2.1.0", None)
            .await
            .unwrap();
        store
            .record_job_outcome(&id, None, QueryAuditStatus::Failed, Some("schema_mismatch"))
            .await
            .unwrap();
        let row = store.get(&id).await.unwrap().unwrap();
        assert_eq!(row["status"], json!("failed"));
        assert_eq!(row["error"], json!("schema_mismatch"));
        assert!(row["job_run_id"].is_null());
    }

    #[tokio::test]
    async fn list_by_session_interleaves_all_three_kinds_in_order() {
        let store = QueryAuditStore::open_in_memory().await.unwrap();
        let ctx = serde_json::json!({"purpose": "p", "session_id": "sess-all"});
        store
            .record_started("SELECT 1", Some(&ctx), 10, "query")
            .await
            .unwrap();
        store
            .record_pipeline_started("weekly-churn", "1.0.0", Some("sess-all"))
            .await
            .unwrap();
        store
            .record_job_submitted("nightly-backfill", "2.1.0", Some("sess-all"))
            .await
            .unwrap();
        let rows = store.list_by_session("sess-all").await.unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["statement_kind"], json!("query"));
        assert_eq!(rows[1]["statement_kind"], json!("pipeline"));
        assert_eq!(rows[2]["statement_kind"], json!("job"));
        assert_eq!(rows[2]["sql"], json!("nightly-backfill@2.1.0"));
    }

    #[tokio::test]
    async fn orphaned_job_rows_reconcile_to_unknown() {
        let store = QueryAuditStore::open_in_memory().await.unwrap();
        let id = store
            .record_job_submitted("nightly-backfill", "2.1.0", None)
            .await
            .unwrap();
        let n = store.reconcile_orphaned("test restart").await.unwrap();
        assert_eq!(n, 1);
        let row = store.get(&id).await.unwrap().unwrap();
        assert_eq!(row["status"], json!("unknown"));
    }

    #[tokio::test]
    async fn open_migrates_old_schema_without_job_run_id_column() {
        // A database created before job_run_id existed must open and serve job
        // rows after migration. Covers the bridge column specifically: #206's
        // identity columns have their own old-schema fixture, and both are
        // reconciled by the same `ensure_schema_additions` step.
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("old.db");
        {
            // Original DDL: INIT_SCHEMA_SQL before either the identity
            // columns or the bridge column existed.
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE query_audit (
                    id TEXT PRIMARY KEY, created_at TEXT NOT NULL,
                    finished_at TEXT, sql TEXT NOT NULL, ai_context TEXT,
                    session_id TEXT, max_rows INTEGER NOT NULL,
                    statement_kind TEXT NOT NULL, status TEXT NOT NULL,
                    row_count INTEGER, error TEXT);",
            )
            .unwrap();
        }
        let store = QueryAuditStore::open(&path).await.unwrap();
        let id = store
            .record_job_submitted("nightly-backfill", "2.1.0", None)
            .await
            .unwrap();
        let row = store.get(&id).await.unwrap().unwrap();
        assert_eq!(row["statement_kind"], json!("job"));
        assert!(row["job_run_id"].is_null());
    }

    #[tokio::test]
    async fn open_normalises_legacy_statement_kind_casing() {
        // A ledger written before the casing change holds `Query` / `Other`
        // (the leaked `Debug` form). After upgrading, the documented filter
        // `WHERE statement_kind = 'query'` must see those rows too — otherwise
        // every pre-upgrade ad-hoc statement is silently invisible to the
        // intention-mining consumer the ledger exists for.
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("legacy-casing.db");
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE query_audit (
                    id TEXT PRIMARY KEY, created_at TEXT NOT NULL,
                    finished_at TEXT, sql TEXT NOT NULL, ai_context TEXT,
                    session_id TEXT, max_rows INTEGER NOT NULL,
                    statement_kind TEXT NOT NULL, status TEXT NOT NULL,
                    row_count INTEGER, error TEXT);",
            )
            .unwrap();
            for (id, kind) in [
                ("old-q", "Query"),
                ("old-o", "Other"),
                ("old-p", "pipeline"),
            ] {
                conn.execute(
                    "INSERT INTO query_audit
                        (id, created_at, sql, max_rows, statement_kind, status, session_id)
                     VALUES (?1, '2026-08-01T00:00:00Z', 'SELECT 1', 10, ?2, 'succeeded', 'sess-legacy')",
                    params![id, kind],
                )
                .unwrap();
            }
        }

        let store = QueryAuditStore::open(&path).await.unwrap();
        store
            .record_started(
                "SELECT 2",
                Some(&json!({ "session_id": "sess-legacy" })),
                10,
                QUERY_STATEMENT_KIND,
            )
            .await
            .unwrap();

        let rows = store.list_by_session("sess-legacy").await.unwrap();
        let kinds: Vec<&str> = rows
            .iter()
            .map(|r| r["statement_kind"].as_str().unwrap())
            .collect();
        // Both pre-upgrade ad-hoc rows now answer to the documented filter,
        // alongside the post-upgrade one. `pipeline` was already lowercase.
        assert_eq!(kinds.iter().filter(|k| **k == "query").count(), 2);
        assert_eq!(kinds.iter().filter(|k| **k == "other").count(), 1);
        assert_eq!(kinds.iter().filter(|k| **k == "pipeline").count(), 1);
        assert!(
            !kinds.iter().any(|k| *k == "Query" || *k == "Other"),
            "legacy casing survived the migration: {kinds:?}"
        );

        // Idempotent: a second open must not disturb the normalised rows.
        drop(store);
        let store = QueryAuditStore::open(&path).await.unwrap();
        let rows = store.list_by_session("sess-legacy").await.unwrap();
        assert_eq!(rows.len(), 4);
        assert!(
            rows.iter()
                .all(|r| r["statement_kind"] != json!("Query")
                    && r["statement_kind"] != json!("Other")),
            "second open reintroduced legacy casing"
        );
    }

    #[tokio::test]
    async fn open_creates_job_run_id_index_on_migrated_and_fresh_databases() {
        // The index cannot live in INIT_SCHEMA_SQL — that batch also runs
        // against pre-column databases, where indexing job_run_id would fail
        // — so it is applied in `ensure_schema_additions`, after the column is
        // guaranteed. Pin that it lands on both a fresh database and a
        // migrated one.
        async fn index_exists(store: &QueryAuditStore) -> bool {
            store
                .sqlite_conn()
                .call(|conn| -> SqlResult<bool> {
                    conn.prepare(
                        "SELECT 1 FROM sqlite_master
                          WHERE type = 'index' AND name = 'idx_query_audit_job_run_id'",
                    )?
                    .exists([])
                })
                .await
                .unwrap()
        }

        let tmp = tempfile::TempDir::new().unwrap();

        let fresh = QueryAuditStore::open(tmp.path().join("fresh.db"))
            .await
            .unwrap();
        assert!(
            index_exists(&fresh).await,
            "fresh database is missing the job_run_id index"
        );

        let old_path = tmp.path().join("old.db");
        {
            let conn = rusqlite::Connection::open(&old_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE query_audit (
                    id TEXT PRIMARY KEY, created_at TEXT NOT NULL,
                    finished_at TEXT, sql TEXT NOT NULL, ai_context TEXT,
                    session_id TEXT, max_rows INTEGER NOT NULL,
                    statement_kind TEXT NOT NULL, status TEXT NOT NULL,
                    row_count INTEGER, error TEXT);",
            )
            .unwrap();
        }
        let migrated = QueryAuditStore::open(&old_path).await.unwrap();
        assert!(
            index_exists(&migrated).await,
            "migrated database is missing the job_run_id index"
        );
    }

    #[test]
    fn duplicate_column_error_is_recognised() {
        // The migration's check-then-ALTER is not atomic, so a second process
        // can win the race and leave this connection's ALTER failing with
        // `duplicate column name`. That is the post-condition the step wanted,
        // so `open()` treats it as success — but only if it can tell that
        // error apart from a real one. rusqlite reports it as a generic
        // SQLITE_ERROR with no distinguishing code, hence the text match;
        // this test is what keeps the match honest against a rusqlite bump.
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE query_audit (id TEXT PRIMARY KEY);")
            .unwrap();
        conn.execute("ALTER TABLE query_audit ADD COLUMN job_run_id TEXT", [])
            .unwrap();
        let err = conn
            .execute("ALTER TABLE query_audit ADD COLUMN job_run_id TEXT", [])
            .expect_err("adding an existing column must fail");
        assert!(is_duplicate_column(&err), "unrecognised: {err}");

        // A genuinely different failure must not be swallowed.
        let other = conn
            .execute("ALTER TABLE nonexistent ADD COLUMN run_id TEXT", [])
            .expect_err("altering a missing table must fail");
        assert!(!is_duplicate_column(&other), "over-matched: {other}");
    }

    #[tokio::test]
    async fn job_outcome_does_not_resurrect_a_reconciled_row() {
        // Two servers sharing one --query-audit-db file: B's startup
        // reconciliation settles A's in-flight submission to `unknown`, then
        // A's outcome write lands. Without the started-only guard that late
        // write would flip a terminal row back to `succeeded` and defeat
        // reconciliation.
        let store = QueryAuditStore::open_in_memory().await.unwrap();
        let id = store
            .record_job_submitted("nightly-backfill", "2.1.0", Some("sess-j"))
            .await
            .unwrap();
        store.reconcile_orphaned("server B restart").await.unwrap();

        // Not an error: 0 rows updated means something else already settled it.
        store
            .record_job_outcome(&id, Some("run-late"), QueryAuditStatus::Succeeded, None)
            .await
            .unwrap();

        let row = store.get(&id).await.unwrap().unwrap();
        assert_eq!(row["status"], json!("unknown"));
        // `job_run_id`, not `run_id`: the bridge stamp is the only column
        // `record_job_outcome` writes, so it is the only one whose absence
        // proves the guard blocked the late write. `run_id` is #206's
        // identity envelope — always NULL here regardless of the guard.
        assert!(row["job_run_id"].is_null(), "terminal row was resurrected");
    }

    #[tokio::test]
    async fn query_outcome_does_not_resurrect_a_reconciled_row() {
        let store = QueryAuditStore::open_in_memory().await.unwrap();
        let id = store
            .record_started("SELECT 1", None, 10, "query")
            .await
            .unwrap();
        store.reconcile_orphaned("server B restart").await.unwrap();

        store
            .record_outcome(&id, QueryAuditStatus::Succeeded, Some(7), None)
            .await
            .unwrap();

        let row = store.get(&id).await.unwrap().unwrap();
        assert_eq!(row["status"], json!("unknown"));
        assert!(row["row_count"].is_null(), "terminal row was resurrected");
    }
}
