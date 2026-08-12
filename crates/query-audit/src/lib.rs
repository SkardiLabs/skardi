//! Durable audit store for ad-hoc `/query` statements.
//!
//! Off unless the operator sets `--query-audit-db <path>`. When enabled, every
//! statement the `/query` endpoint accepts is written to a SQLite database
//! *before* execution and updated with its outcome afterwards, so the store
//! answers "what ran, on whose behalf, and did it succeed" rather than merely
//! "what was attempted".
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

use anyhow::{Context, Result};
use serde_json::Value;
use std::path::{Path, PathBuf};
use tokio_rusqlite::{Connection, params, rusqlite};

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
    run_id         TEXT
);
CREATE INDEX IF NOT EXISTS idx_query_audit_session_created
    ON query_audit (session_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_query_audit_created
    ON query_audit (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_query_audit_status
    ON query_audit (status);";

/// The five identity columns, in order. `CREATE TABLE IF NOT EXISTS` cannot
/// retrofit an existing table, so [`ensure_identity_columns`] reconciles the
/// live schema on every open — additive, idempotent, and a no-op on fresh
/// databases.
const IDENTITY_COLUMNS: [&str; 5] = ["request_id", "org_id", "workspace_id", "user_id", "run_id"];

fn ensure_identity_columns(conn: &rusqlite::Connection) -> SqlResult<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(query_audit)")?;
    let existing: std::collections::HashSet<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<std::result::Result<_, _>>()?;
    for col in IDENTITY_COLUMNS {
        if !existing.contains(col) {
            conn.execute(
                &format!("ALTER TABLE query_audit ADD COLUMN {col} TEXT"),
                [],
            )?;
        }
    }
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

/// SQLite-backed audit ledger for `/query`.
pub struct QueryAuditStore {
    conn: Connection,
    path: PathBuf,
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
            ensure_identity_columns(conn)?;
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

        Ok(Self { conn, path })
    }

    /// Open an in-memory ledger. Tests only — nothing is durable.
    pub async fn open_in_memory() -> Result<Self> {
        let conn = Connection::open(":memory:")
            .await
            .context("Failed to open in-memory query-audit db")?;
        conn.call(|conn| -> SqlResult<()> {
            conn.execute_batch(INIT_SCHEMA_SQL)?;
            ensure_identity_columns(conn)?;
            Ok(())
        })
        .await
        .context("Failed to initialise in-memory query-audit db")?;
        Ok(Self {
            conn,
            path: PathBuf::from(":memory:"),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
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

        self.conn
            .call(move |conn| -> SqlResult<()> {
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
            })
            .await
            .context("Failed to write pre-execution query-audit record")?;

        Ok(id)
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

        self.conn
            .call(move |conn| -> SqlResult<()> {
                conn.execute(
                    "UPDATE query_audit
                        SET status = ?2, finished_at = ?3, row_count = ?4, error = ?5
                      WHERE id = ?1",
                    params![id, status, finished_at, row_count, error],
                )?;
                Ok(())
            })
            .await
            .context("Failed to update query-audit record")?;
        Ok(())
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

    /// Delete records created before `cutoff` (RFC 3339). Returns the row count.
    pub async fn prune_before(&self, cutoff: chrono::DateTime<chrono::Utc>) -> Result<usize> {
        let cutoff = cutoff.to_rfc3339();
        let deleted = self
            .conn
            .call(move |conn| -> SqlResult<usize> {
                let n = conn.execute(
                    "DELETE FROM query_audit WHERE created_at < ?1",
                    params![cutoff],
                )?;
                Ok(n)
            })
            .await
            .context("Failed to prune query-audit records")?;
        Ok(deleted)
    }

    /// Fetch one record as a JSON object. Test/diagnostic helper.
    pub async fn get(&self, id: &str) -> Result<Option<Value>> {
        let id = id.to_string();
        let row = self
            .conn
            .call(move |conn| -> SqlResult<Option<Value>> {
                let mut stmt = conn.prepare(
                    "SELECT id, created_at, finished_at, sql, ai_context, session_id,
                            max_rows, statement_kind, status, row_count, error,
                            request_id, org_id, workspace_id, user_id, run_id
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
                    "SELECT id FROM query_audit WHERE session_id = ?1 ORDER BY created_at ASC",
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

/// Random-ish unique row id. Avoids a `uuid` dependency: the timestamp keeps
/// ids ordered and the counter disambiguates within the same nanosecond.
fn new_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{nanos:x}-{seq:x}")
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
}
