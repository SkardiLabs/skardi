//! The Postgres storage backend for [`QueryAuditStore`](crate::QueryAuditStore)
//! — the SAME fail-closed contract as the SQLite file, storage swapped.
//!
//! What "same contract" means, hazard by hazard:
//!
//! * **Durable before execution.** `record_started*` commits and returns the
//!   row id; the caller treats an `Err` as fatal to the request. Postgres is
//!   `synchronous_commit = on` by default, the durability `synchronous =
//!   FULL` bought on the file.
//! * **Boot-fatal.** [`PgAudit::open`] connects EAGERLY and applies the DDL;
//!   a server configured for auditing that cannot reach its ledger refuses
//!   to start. (The cloud engine's best-effort ledger is lazy on purpose —
//!   that is a different product with the opposite loss contract.)
//! * **Two-phase with the `started`-only guard.** Outcome stamps are
//!   monotonic (`WHERE status = 'started'`), so reconciliation can never be
//!   overwritten by a late stamp.
//! * **Startup reconcile.** Rows left `started` by a crash rewrite to
//!   `unknown` on the next open, exactly like the file.
//! * **Bounded writes.** Every write runs under the store's
//!   [`AUDIT_WRITE_TIMEOUT`](crate::AUDIT_WRITE_TIMEOUT); a timed-out
//!   pre-execution write follows up with the corrective UPDATE. One honest
//!   divergence: the SQLite writer is a single FIFO thread, which GUARANTEES
//!   the correction lands after its INSERT; a connection pool does not, so
//!   on Postgres the correction is best-effort and a row whose INSERT lands
//!   after the correction stays `started` until the next startup reconciles
//!   it — the same class as a crash, and the same repair.
//!
//! The schema mirrors the SQLite table column-for-column — TEXT timestamps
//! (RFC 3339 sorts lexicographically), TEXT `ai_context` — so every query,
//! its ordering semantics, and the consumer-facing `statement_kind` /
//! `status` vocabularies are SHARED with the file backend rather than ported
//! and drifting. Consumers who want Postgres types can cast in SQL
//! (`ai_context::jsonb`, `created_at::timestamptz`).
//!
//! Privacy: the file backend's `0600` becomes role hygiene here — the ledger
//! holds raw SQL, so point the DSN at a role and database that only the
//! operator reads. The DSN itself arrives via environment variable (never a
//! flag: it carries a credential, and argv leaks into process listings), and
//! this module never logs it — the store's `Debug`/`path()` render only the
//! redacted authority.

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::path::PathBuf;

use crate::{BoundedError, QueryAuditStatus, QueryIdentity, bounded, new_id};

/// The environment variable naming the Postgres audit DSN. An env var, not a
/// flag: the DSN carries a credential, and `--query-audit-db` in argv is
/// visible to every process listing and shell history. Mutually exclusive
/// with `--query-audit-db` — the server refuses to start with both set.
pub const PG_DSN_ENV: &str = "SKARDI_QUERY_AUDIT_PG_DSN";

/// Ledger DDL, the SQLite schema mirrored column-for-column (see the module
/// doc for why TEXT stays TEXT). Idempotent — applied on every open. Fresh
/// databases get every column up front, so the file backend's ALTER-based
/// retrofits have no Postgres counterpart; the one data normalisation
/// (legacy `statement_kind` casing) is applied alongside, mirroring
/// `ensure_schema_additions`.
const INIT_SCHEMA_PG: &str = "CREATE TABLE IF NOT EXISTS query_audit (
    id             TEXT PRIMARY KEY,
    created_at     TEXT NOT NULL,
    finished_at    TEXT,
    sql            TEXT NOT NULL,
    ai_context     TEXT,
    session_id     TEXT,
    max_rows       BIGINT NOT NULL,
    statement_kind TEXT NOT NULL,
    status         TEXT NOT NULL,
    row_count      BIGINT,
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
    ON query_audit (statement_kind);
CREATE INDEX IF NOT EXISTS idx_query_audit_job_run_id
    ON query_audit (job_run_id) WHERE job_run_id IS NOT NULL;";

/// The Postgres arm's connection and identity. Constructed only by
/// [`PgAudit::open`]; all writes are driven through the store facade, which
/// owns the timeout.
pub(crate) struct PgAudit {
    pool: PgPool,
    /// Redacted `host[:port]/database` for `Debug` and `path()` — never the
    /// DSN, which carries a credential.
    redacted: PathBuf,
}

/// `host[:port]/database` from a DSN, with userinfo (and its password)
/// dropped. Falls back to a constant marker rather than echoing an
/// unparsable DSN, which could itself be the credential.
fn redact_dsn(dsn: &str) -> PathBuf {
    let after_scheme = dsn.split_once("://").map(|(_, r)| r).unwrap_or("");
    let after_user = after_scheme
        .rsplit_once('@')
        .map(|(_, r)| r)
        .unwrap_or(after_scheme);
    let hostdb = after_user.split('?').next().unwrap_or(after_user);
    if hostdb.is_empty() {
        PathBuf::from("postgres://<unparsed>")
    } else {
        PathBuf::from(format!("postgres://{hostdb}"))
    }
}

impl PgAudit {
    /// Connect EAGERLY and apply the schema. Errors are fatal to startup by
    /// design — the same "an operator who asked for an audit trail must not
    /// get a server that quietly runs without one" as the file backend.
    pub(crate) async fn open(dsn: &str) -> Result<Self> {
        let redacted = redact_dsn(dsn);
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(dsn)
            .await
            .with_context(|| {
                format!(
                    "Failed to connect the Postgres query-audit ledger at {}",
                    redacted.display()
                )
            })?;
        sqlx::raw_sql(INIT_SCHEMA_PG)
            .execute(&pool)
            .await
            .with_context(|| {
                format!(
                    "Failed to initialise the Postgres query-audit schema at {}",
                    redacted.display()
                )
            })?;
        // The same one-shot normalisation `ensure_schema_additions` runs on
        // the file: harmless on fresh databases, load-bearing on a table
        // populated by a pre-#219 writer.
        for (current, legacy) in [
            (crate::QUERY_STATEMENT_KIND, "Query"),
            (crate::OTHER_STATEMENT_KIND, "Other"),
        ] {
            sqlx::query("UPDATE query_audit SET statement_kind = $1 WHERE statement_kind = $2")
                .bind(current)
                .bind(legacy)
                .execute(&pool)
                .await
                .context("Failed to normalise legacy statement_kind casing")?;
        }
        Ok(Self { pool, redacted })
    }

    pub(crate) fn redacted(&self) -> &std::path::Path {
        &self.redacted
    }

    pub(crate) async fn close_for_test(&self) {
        self.pool.close().await;
    }

    pub(crate) async fn record_started_for(
        &self,
        sql: &str,
        ai_context: Option<&Value>,
        max_rows: usize,
        statement_kind: &str,
        identity: Option<&QueryIdentity>,
        timeout: std::time::Duration,
    ) -> std::result::Result<String, BoundedError> {
        let id = new_id();
        let created_at = chrono::Utc::now().to_rfc3339();
        let session_id = ai_context
            .and_then(|c| c.get("session_id"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let ai_context = ai_context.map(ToString::to_string);
        let identity = identity.cloned().unwrap_or_default();
        match bounded(
            sqlx::query(
                "INSERT INTO query_audit
                    (id, created_at, sql, ai_context, session_id, max_rows,
                     statement_kind, status,
                     request_id, org_id, workspace_id, user_id, run_id)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
            )
            .bind(&id)
            .bind(&created_at)
            .bind(sql)
            .bind(&ai_context)
            .bind(&session_id)
            .bind(max_rows as i64)
            .bind(statement_kind)
            .bind(QueryAuditStatus::Started.as_str())
            .bind(&identity.request_id)
            .bind(&identity.org_id)
            .bind(&identity.workspace_id)
            .bind(&identity.user_id)
            .bind(&identity.run_id)
            .execute(&self.pool),
            "Failed to write pre-execution query-audit record",
            timeout,
        )
        .await
        {
            Ok(_) => Ok(id),
            Err(e) => {
                // Same corrective UPDATE as the file backend: the caller has
                // already been told 503, so if the INSERT ever lands the row
                // should read failed/audit_write_timeout, not `started`.
                if e.is_timeout() {
                    self.spawn_timeout_correction(id, timeout);
                }
                Err(e)
            }
        }
    }

    /// One INSERT shape serves both pipeline and job `started` rows — they
    /// differ only in `statement_kind` (the file backend spells them as two
    /// methods for historical reasons; the SQL is the same).
    pub(crate) async fn record_name_at_version_started(
        &self,
        name_at_version: &str,
        session_id: Option<&str>,
        statement_kind: &str,
        timeout: std::time::Duration,
    ) -> std::result::Result<String, BoundedError> {
        let id = new_id();
        let created_at = chrono::Utc::now().to_rfc3339();
        match bounded(
            sqlx::query(
                "INSERT INTO query_audit
                    (id, created_at, sql, ai_context, session_id, max_rows,
                     statement_kind, status)
                 VALUES ($1, $2, $3, NULL, $4, $5, $6, $7)",
            )
            .bind(&id)
            .bind(&created_at)
            .bind(name_at_version)
            .bind(session_id)
            .bind(crate::PIPELINE_MAX_ROWS_SENTINEL)
            .bind(statement_kind)
            .bind(QueryAuditStatus::Started.as_str())
            .execute(&self.pool),
            "Failed to write pre-execution audit record",
            timeout,
        )
        .await
        {
            Ok(_) => Ok(id),
            Err(e) => {
                if e.is_timeout() {
                    self.spawn_timeout_correction(id, timeout);
                }
                Err(e)
            }
        }
    }

    pub(crate) async fn record_outcome(
        &self,
        id: &str,
        status: QueryAuditStatus,
        row_count: Option<usize>,
        error: Option<&str>,
        timeout: std::time::Duration,
    ) -> Result<()> {
        let finished_at = chrono::Utc::now().to_rfc3339();
        bounded(
            sqlx::query(
                "UPDATE query_audit
                    SET status = $2, finished_at = $3, row_count = $4, error = $5
                  WHERE id = $1 AND status = $6",
            )
            .bind(id)
            .bind(status.as_str())
            .bind(&finished_at)
            .bind(row_count.map(|n| n as i64))
            .bind(error)
            .bind(QueryAuditStatus::Started.as_str())
            .execute(&self.pool),
            "Failed to update query-audit record",
            timeout,
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn record_job_outcome(
        &self,
        id: &str,
        job_run_id: Option<&str>,
        status: QueryAuditStatus,
        error: Option<&str>,
        timeout: std::time::Duration,
    ) -> Result<()> {
        let finished_at = chrono::Utc::now().to_rfc3339();
        bounded(
            sqlx::query(
                "UPDATE query_audit
                    SET status = $2, finished_at = $3, job_run_id = $4, error = $5
                  WHERE id = $1 AND status = $6",
            )
            .bind(id)
            .bind(status.as_str())
            .bind(&finished_at)
            .bind(job_run_id)
            .bind(error)
            .bind(QueryAuditStatus::Started.as_str())
            .execute(&self.pool),
            "Failed to update job-audit record",
            timeout,
        )
        .await?;
        Ok(())
    }

    /// See `QueryAuditStore::spawn_timeout_correction` for the contract, and
    /// this module's doc for the one honest divergence (no FIFO guarantee —
    /// startup reconcile is the backstop).
    pub(crate) fn spawn_timeout_correction(&self, id: String, bound: std::time::Duration) {
        let pool = self.pool.clone();
        tokio::spawn(async move {
            let finished_at = chrono::Utc::now().to_rfc3339();
            let update = sqlx::query(
                "UPDATE query_audit
                        SET status = $2, finished_at = $3, error = $4
                      WHERE id = $1 AND status = $5",
            )
            .bind(&id)
            .bind(QueryAuditStatus::Failed.as_str())
            .bind(&finished_at)
            .bind("audit_write_timeout")
            .bind(QueryAuditStatus::Started.as_str())
            .execute(&pool);
            match tokio::time::timeout(bound, update).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => tracing::warn!(
                    "Failed to apply audit-write-timeout correction to query-audit record: {e}"
                ),
                Err(_) => tracing::warn!(
                    "Audit-write-timeout correction did not land within {bound:?}; \
                     the row reconciles at the next startup if its INSERT ever landed"
                ),
            }
        });
    }

    pub(crate) async fn reconcile_orphaned(&self, reason: &str) -> Result<usize> {
        let finished_at = chrono::Utc::now().to_rfc3339();
        let done = sqlx::query(
            "UPDATE query_audit
                SET status = $1, finished_at = $2, error = $3
              WHERE status = $4",
        )
        .bind(QueryAuditStatus::Unknown.as_str())
        .bind(&finished_at)
        .bind(reason)
        .bind(QueryAuditStatus::Started.as_str())
        .execute(&self.pool)
        .await
        .context("Failed to reconcile orphaned query-audit records")?;
        Ok(done.rows_affected() as usize)
    }

    pub(crate) async fn job_rows_missing_run_id(&self) -> Result<Vec<String>> {
        let rows = sqlx::query(
            "SELECT id FROM query_audit
              WHERE statement_kind = $1
                AND status = $2
                AND job_run_id IS NULL
              ORDER BY created_at ASC",
        )
        .bind(crate::JOB_STATEMENT_KIND)
        .bind(QueryAuditStatus::Unknown.as_str())
        .fetch_all(&self.pool)
        .await
        .context("Failed to list job rows missing job_run_id")?;
        Ok(rows.iter().map(|r| r.get::<String, _>("id")).collect())
    }

    pub(crate) async fn backfill_job_run_id(&self, id: &str, job_run_id: &str) -> Result<bool> {
        let done = sqlx::query(
            "UPDATE query_audit
                SET job_run_id = $2
              WHERE id = $1
                AND job_run_id IS NULL
                AND status = $3",
        )
        .bind(id)
        .bind(job_run_id)
        .bind(QueryAuditStatus::Unknown.as_str())
        .execute(&self.pool)
        .await
        .context("Failed to backfill job_run_id")?;
        Ok(done.rows_affected() > 0)
    }

    /// Chunked like the file backend's, and for the same reason bounded:
    /// each batch stays inside the write timeout so a multi-million-row
    /// backlog cannot occupy the ledger past the bound in one statement.
    pub(crate) async fn prune_before(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
        timeout: std::time::Duration,
    ) -> Result<usize> {
        let cutoff = cutoff.to_rfc3339();
        let mut total = 0usize;
        loop {
            let deleted = bounded(
                sqlx::query(
                    "DELETE FROM query_audit WHERE id IN (
                        SELECT id FROM query_audit WHERE created_at < $1 LIMIT $2
                     )",
                )
                .bind(&cutoff)
                .bind(crate::PRUNE_BATCH_SIZE as i64)
                .execute(&self.pool),
                "Failed to prune query-audit records",
                timeout,
            )
            .await?;
            let deleted = deleted.rows_affected() as usize;
            if deleted == 0 {
                break;
            }
            total += deleted;
            tokio::task::yield_now().await;
        }
        Ok(total)
    }

    pub(crate) async fn get(&self, id: &str) -> Result<Option<Value>> {
        let row = sqlx::query(
            "SELECT id, created_at, finished_at, sql, ai_context, session_id,
                    max_rows, statement_kind, status, row_count, error,
                    request_id, org_id, workspace_id, user_id, run_id,
                    job_run_id
               FROM query_audit WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to read query-audit record")?;
        Ok(row.map(|row| {
            serde_json::json!({
                "id": row.get::<String, _>("id"),
                "created_at": row.get::<String, _>("created_at"),
                "finished_at": row.get::<Option<String>, _>("finished_at"),
                "sql": row.get::<String, _>("sql"),
                "ai_context": row.get::<Option<String>, _>("ai_context")
                    .and_then(|s| serde_json::from_str::<Value>(&s).ok()),
                "session_id": row.get::<Option<String>, _>("session_id"),
                "max_rows": row.get::<i64, _>("max_rows"),
                "statement_kind": row.get::<String, _>("statement_kind"),
                "status": row.get::<String, _>("status"),
                "row_count": row.get::<Option<i64>, _>("row_count"),
                "error": row.get::<Option<String>, _>("error"),
                "request_id": row.get::<Option<String>, _>("request_id"),
                "org_id": row.get::<Option<String>, _>("org_id"),
                "workspace_id": row.get::<Option<String>, _>("workspace_id"),
                "user_id": row.get::<Option<String>, _>("user_id"),
                "run_id": row.get::<Option<String>, _>("run_id"),
                "job_run_id": row.get::<Option<String>, _>("job_run_id"),
            })
        }))
    }

    pub(crate) async fn list_session_ids(&self, session_id: &str) -> Result<Vec<String>> {
        let rows = sqlx::query(
            "SELECT id FROM query_audit WHERE session_id = $1 \
             ORDER BY created_at ASC, id ASC",
        )
        .bind(session_id)
        .fetch_all(&self.pool)
        .await
        .context("Failed to list query-audit records by session")?;
        Ok(rows.iter().map(|r| r.get::<String, _>("id")).collect())
    }

    pub(crate) async fn count(&self) -> Result<usize> {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM query_audit")
            .fetch_one(&self.pool)
            .await
            .context("Failed to count query-audit records")?;
        Ok(n as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::redact_dsn;

    /// `Debug`/`path()` render this and ONLY this — the DSN carries a
    /// credential, so the userinfo (and anything unparsable, which could be
    /// the credential itself) must never survive redaction.
    #[test]
    fn redaction_drops_userinfo_and_query_string() {
        assert_eq!(
            redact_dsn("postgres://user:hunter2@db.example:5432/audit?sslmode=require")
                .to_str()
                .unwrap(),
            "postgres://db.example:5432/audit"
        );
        assert_eq!(
            redact_dsn("postgres://db.example/audit").to_str().unwrap(),
            "postgres://db.example/audit"
        );
        for garbage in ["hunter2", "postgres://", ""] {
            let out = redact_dsn(garbage);
            let out = out.to_str().unwrap();
            assert!(!out.contains("hunter2"), "{out}");
        }
    }
}
