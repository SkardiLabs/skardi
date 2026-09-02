//! The best-effort query ledger — the SECOND contract this crate hosts, and
//! deliberately not a storage backend of [`QueryAuditStore`].
//!
//! [`QueryAuditStore`](crate::QueryAuditStore) is a **compliance record**:
//! durable before execution, fail-closed (a statement that cannot be
//! recorded does not run), two-phase (`started` → outcome, with startup
//! reconciliation). This module is an **analytics ledger**: one row per
//! DECIDED statement (`succeeded` / `failed` / `refused`), written
//! best-effort AFTER the outcome is known, on a bounded queue the caller
//! never waits for. A Postgres outage degrades the ledger and never the
//! query path; loss is counted ([`METRICS`]), never silent.
//!
//! The OSS server does not wire this module; its consumer is the governed
//! cloud engine (skardi-cloud's `2026-08-30-query-ledger-postgres-design.md`),
//! whose N per-workspace pods share one Postgres and write as RLS-pinned
//! per-workspace roles. It lives here so the audit domain has one home and
//! the shared vocabulary (identity columns, `MAX_SESSION_ID_CHARS`) cannot
//! drift — NOT because the two contracts are interchangeable. If you want
//! "audit that refuses to run unrecorded statements", you want
//! [`QueryAuditStore`]; if you want "learn-loop analytics that never costs a
//! query", you want this.
//!
//! Schema: the `query_ledger` table, versioned migration constants in
//! [`queries`] (`QUERY_LEDGER_MIGRATION_0001`, …), applied by the DOWNSTREAM
//! deployment's migration path — this module runs no DDL
//! (its writers connect as roles that deliberately cannot).

pub mod queries;
pub mod read;
pub mod writer;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Notify, mpsc, watch};

/// `sql` is truncated to this at ROW ASSEMBLY: the learn loop wants
/// patterns, not megabyte literals, and bounding at ingestion is what keeps
/// the queue's worst case ~40 MiB instead of ~8 GiB (1024 rows × an 8 MiB
/// statement ceiling).
pub const SQL_MAX_BYTES: usize = 32 * 1024;

/// `error` shares `ai_context`'s 4 KiB bound: failures and refusals are the
/// most instructive rows, but they carry engine text, not documents.
pub const ERROR_MAX_BYTES: usize = 4 * 1024;

/// Queue capacity in ROWS; with every field bounded at assembly the byte
/// worst case is ~40 MiB per process.
pub const QUEUE_CAPACITY: usize = 1024;

/// Bound on each identity field (`org_id`, `workspace_id`, `user_id`,
/// `request_id`) at assembly. Production values are gate-validated slugs a
/// few dozen bytes long; the cap exists because the ~40 MiB queue worst
/// case is only true if EVERY field is bounded — an unbounded identity
/// string would let 1,024 queued rows hold arbitrarily more.
pub const IDENTITY_MAX_BYTES: usize = 4 * 1024;

/// Rows per multi-row INSERT flush.
pub const FLUSH_BATCH_ROWS: usize = 64;

/// Wall-clock bound on one flush. A slow PG drops the batch (counted),
/// never stalls the writer behind an unbounded await.
pub const FLUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Loss accounting: dropped is never silent. The consumer renders this into
/// its own metrics surface.
#[derive(Debug, Default)]
pub struct Metrics {
    /// A flush failed or timed out; the whole batch was dropped (logged with
    /// its request ids).
    pub insert_failures_pg: AtomicU64,
    /// `try_send` refused — the channel was full; the row was dropped.
    pub insert_failures_channel_full: AtomicU64,
}

pub static METRICS: Metrics = Metrics {
    insert_failures_pg: AtomicU64::new(0),
    insert_failures_channel_full: AtomicU64::new(0),
};

impl Metrics {
    pub fn render(&self) -> String {
        format!(
            "# TYPE ledger_insert_failures_total counter\n\
             ledger_insert_failures_total{{reason=\"pg\"}} {}\n\
             ledger_insert_failures_total{{reason=\"channel_full\"}} {}\n",
            self.insert_failures_pg.load(Ordering::Relaxed),
            self.insert_failures_channel_full.load(Ordering::Relaxed),
        )
    }
}

/// Row status vocabulary. `refused` is this ledger's extension to the
/// [`QueryAuditStatus`](crate::QueryAuditStatus) set; `started`/`unknown`
/// have no counterpart here because there is no two-phase write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowStatus {
    Succeeded,
    Failed,
    Refused,
}

impl RowStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            RowStatus::Succeeded => "succeeded",
            RowStatus::Failed => "failed",
            RowStatus::Refused => "refused",
        }
    }
}

/// One fully-bounded, insert-ready row. **Assembly is the single place every
/// field is bounded and coerced**: `sql` truncated to [`SQL_MAX_BYTES`],
/// `error` to [`ERROR_MAX_BYTES`], `max_rows` clamped into `i64` — so a batch
/// can never be poisoned by one row.
/// Fields are deliberately NOT public: the bound is a TYPE invariant.
/// The only way to obtain a `LedgerRow` is [`RowDraft::capture`] →
/// [`RowDraft::finish`], so a row handed to [`PgLedger::record`] is bounded
/// by construction — public fields would let any caller bypass assembly and
/// break both the ~40 MiB queue worst case and the no-poisoned-batch
/// guarantee.
#[derive(Debug, Clone)]
pub struct LedgerRow {
    pub(crate) org_id: String,
    pub(crate) workspace_id: String,
    pub(crate) user_id: String,
    pub(crate) request_id: String,
    pub(crate) session_id: Option<String>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) finished_at: DateTime<Utc>,
    pub(crate) sql: String,
    pub(crate) sql_truncated: bool,
    pub(crate) ai_context: Option<Value>,
    pub(crate) statement_kind: &'static str,
    pub(crate) max_rows: i64,
    pub(crate) status: RowStatus,
    pub(crate) row_count: Option<i64>,
    pub(crate) error: Option<String>,
}

/// The identity + request facts captured once the caller's identity gate has
/// passed. The `sql` snapshot is truncated at capture, so a pending context
/// never pins a multi-MiB statement.
/// Same visibility rule as [`LedgerRow`]: constructed only by
/// [`RowDraft::capture`], so the bounds cannot be bypassed.
#[derive(Debug, Clone)]
pub struct RowDraft {
    pub(crate) org_id: String,
    pub(crate) workspace_id: String,
    pub(crate) user_id: String,
    pub(crate) request_id: String,
    pub(crate) session_id: Option<String>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) sql: String,
    pub(crate) sql_truncated: bool,
    pub(crate) ai_context: Option<Value>,
    pub(crate) max_rows: i64,
}

impl RowDraft {
    /// Capture the bounded draft. `session_id` is denormalized out of
    /// `ai_context` exactly as [`QueryAuditStore`](crate::QueryAuditStore)
    /// does, so session lookups keep working on the same key — and the bound
    /// is [`MAX_SESSION_ID_CHARS`](crate::MAX_SESSION_ID_CHARS) in
    /// CHARACTERS, the same unit the wire validation counts in.
    #[allow(clippy::too_many_arguments)] // mirrors the row's identity columns
    pub fn capture(
        org_id: &str,
        workspace_id: &str,
        user_id: &str,
        request_id: &str,
        sql: &str,
        ai_context: Option<&Value>,
        requested_max_rows: Option<usize>,
        default_max_rows: usize,
    ) -> Self {
        let (sql, sql_truncated) = bound_text(sql, SQL_MAX_BYTES);
        let session_id = ai_context
            .and_then(|c| c.get("session_id"))
            .and_then(Value::as_str)
            // A longer value is a malformed caller assertion, dropped rather
            // than truncated into a different-looking session — and one
            // carrying U+0000 (unstorable in TEXT) is dropped for the same
            // reason: scrubbing would make it a different-looking session.
            .filter(|s| s.chars().count() <= crate::MAX_SESSION_ID_CHARS && !s.contains('\0'))
            .map(str::to_string);
        // The column bound (≤ 4 KiB) is enforced HERE, not only by a route's
        // own refusal: a refused row for an oversized ai_context must not
        // smuggle the very payload the refusal exists to bound. Dropped, not
        // truncated — a truncated JSON document is not a JSON document.
        let ai_context = ai_context
            .filter(|c| {
                serde_json::to_vec(c)
                    .map(|v| v.len() <= ERROR_MAX_BYTES)
                    .unwrap_or(false)
                    // JSONB cannot represent U+0000 anywhere in the document
                    // (keys included); like the oversize case this drops the
                    // WHOLE document rather than mutating it — an edited JSON
                    // document is a different document.
                    && !json_contains_nul(c)
            })
            .cloned();
        Self {
            // Identity values arrive gate-validated in production, but
            // assembly is the single bounding point and must not trust that:
            // one NUL here would still poison the batch, and one unbounded
            // string would void the queue's ~40 MiB worst case.
            org_id: bound_text(org_id, IDENTITY_MAX_BYTES).0,
            workspace_id: bound_text(workspace_id, IDENTITY_MAX_BYTES).0,
            user_id: bound_text(user_id, IDENTITY_MAX_BYTES).0,
            request_id: bound_text(request_id, IDENTITY_MAX_BYTES).0,
            session_id,
            created_at: Utc::now(),
            sql,
            sql_truncated,
            ai_context,
            // Clamped into i64 at assembly: a refused row's requested value
            // may exceed any ceiling — a 3,000,000,000 must land clamped,
            // not sink its batch.
            max_rows: requested_max_rows
                .unwrap_or(default_max_rows)
                .try_into()
                .unwrap_or(i64::MAX),
        }
    }

    /// Finish the draft into an insert-ready row.
    pub fn finish(
        self,
        status: RowStatus,
        row_count: Option<usize>,
        error: Option<String>,
    ) -> LedgerRow {
        LedgerRow {
            org_id: self.org_id,
            workspace_id: self.workspace_id,
            user_id: self.user_id,
            request_id: self.request_id,
            session_id: self.session_id,
            created_at: self.created_at,
            finished_at: Utc::now(),
            sql: self.sql,
            sql_truncated: self.sql_truncated,
            ai_context: self.ai_context,
            statement_kind: "query",
            max_rows: self.max_rows,
            status,
            row_count: row_count.map(|n| i64::try_from(n).unwrap_or(i64::MAX)),
            error: error.map(|e| bound_text(&e, ERROR_MAX_BYTES).0),
        }
    }
}

/// U+0000 cannot be stored in Postgres TEXT or JSONB — one NUL in one
/// field would reject the INSERT and poison the row's whole flush batch
/// (up to [`FLUSH_BATCH_ROWS`] of OTHER callers' rows), which is exactly
/// what assembly-time bounding exists to prevent. Scrubbed to U+FFFD, the
/// standard replacement character, so the row still records that a value
/// was there and was altered.
fn scrub_nul(s: &str) -> String {
    if s.contains('\0') {
        s.replace('\0', "\u{FFFD}")
    } else {
        s.to_string()
    }
}

/// True when any string in the document — values or object KEYS — contains
/// U+0000, which JSONB cannot represent.
fn json_contains_nul(v: &serde_json::Value) -> bool {
    match v {
        Value::String(s) => s.contains('\0'),
        Value::Array(a) => a.iter().any(json_contains_nul),
        Value::Object(o) => o
            .iter()
            .any(|(k, val)| k.contains('\0') || json_contains_nul(val)),
        _ => false,
    }
}

/// Scrub then truncate to at most `max` bytes on a char boundary. Returns
/// the (possibly shortened) string and whether TRUNCATION happened (the NUL
/// scrub is not flagged — a NUL was never valid content to preserve).
/// Scrub first: the replacement char is 3 bytes where NUL was 1, so the
/// byte bound must be applied to the final text.
fn bound_text(s: &str, max: usize) -> (String, bool) {
    let scrubbed = scrub_nul(s);
    let (t, truncated) = truncate_utf8(&scrubbed, max);
    (t, truncated)
}

/// Truncate to at most `max` bytes on a char boundary. Returns the (possibly
/// shortened) string and whether truncation happened.
fn truncate_utf8(s: &str, max: usize) -> (String, bool) {
    if s.len() <= max {
        return (s.to_string(), false);
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    (s[..end].to_string(), true)
}

/// The queued recorder: a bounded queue into the writer task and the lazy
/// pool the read path shares. Cloning shares both, and the writer's handle
/// (see [`Self::shutdown`]).
#[derive(Clone)]
pub struct PgLedger {
    tx: mpsc::Sender<LedgerRow>,
    pool: sqlx::PgPool,
    writer: Arc<WriterControl>,
}

/// The writer task's shutdown plumbing. Held behind an `Arc` so every clone
/// of the handle can trigger and await the same drain.
pub(crate) struct WriterControl {
    /// Signals the writer to close its receiver and drain what is buffered.
    pub(crate) drain: Notify,
    /// Becomes `true` when the writer task has exited — set by a drop guard
    /// inside the task, so it fires on the graceful path, on a panic, and on
    /// an abort alike. SHARED completion state, not a taken JoinHandle:
    /// every concurrent shutdown caller awaits the same signal, and a
    /// cancelled waiter cancels only itself.
    done: watch::Receiver<bool>,
}

impl PgLedger {
    /// Construct the lazy pool (max 2 connections — the consumer's many
    /// processes share one PG) and spawn the writer. Makes NO connection:
    /// nothing about the ledger is boot-fatal.
    pub fn spawn(dsn: &str) -> anyhow::Result<Self> {
        Self::spawn_with_capacity(dsn, QUEUE_CAPACITY)
    }

    /// [`spawn`](Self::spawn) with an explicit queue bound. Production always
    /// goes through `spawn` ([`QUEUE_CAPACITY`]); this seam exists so the
    /// queue-full contract — drop + count, never block — is testable without
    /// enqueueing a thousand rows against a hung writer.
    pub fn spawn_with_capacity(dsn: &str, capacity: usize) -> anyhow::Result<Self> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect_lazy(dsn)?;
        if capacity == 0 {
            // tokio's bounded channel PANICS on zero; this constructor
            // advertises Result, so invalid configuration must not be able
            // to take the process down.
            anyhow::bail!("ledger queue capacity must be >= 1 (got 0)");
        }
        let (tx, rx) = mpsc::channel(capacity);
        let (done_tx, done_rx) = watch::channel(false);
        let writer = Arc::new(WriterControl {
            drain: Notify::new(),
            done: done_rx,
        });
        tokio::spawn(writer::run(pool.clone(), rx, writer.clone(), done_tx));
        Ok(Self { tx, pool, writer })
    }

    /// Graceful shutdown: signal the writer to stop accepting rows, drain and
    /// flush everything already queued, and wait for it to finish. Call this
    /// before letting the Tokio runtime wind down — a detached writer is
    /// aborted WITH the runtime, mid-flush, and rows lost that way are
    /// counted by nobody, which would break the loss-is-never-silent
    /// contract. After shutdown, [`Self::record`] counts every further row
    /// as a channel loss (the same accounting as a full queue).
    ///
    /// Unbounded by design — the drain is at most `capacity / FLUSH_BATCH`
    /// flushes of [`FLUSH_TIMEOUT`] each; a caller with a deadline wraps
    /// this in `tokio::time::timeout`, and rows still queued at a HARD
    /// abort remain the accepted pod-crash loss class. Concurrent callers
    /// all await the SAME completion state (a watch the writer's drop guard
    /// sets on every exit path, panic included), so whichever caller
    /// controls teardown holds it open until the drain is really done, and
    /// one cancelled waiter cancels nobody else.
    pub async fn shutdown(&self) {
        self.writer.drain.notify_one();
        let mut done = self.writer.done.clone();
        // Err = the sender dropped, which only happens when the task is
        // gone; either way, the writer is no longer running.
        let _ = done.wait_for(|finished| *finished).await;
    }

    /// Enqueue one decided row. Never waits, never errors to the caller: a
    /// full channel drops the row and counts it.
    pub fn record(&self, row: LedgerRow) {
        if let Err(e) = self.tx.try_send(row) {
            METRICS
                .insert_failures_channel_full
                .fetch_add(1, Ordering::Relaxed);
            let request_id = match &e {
                TrySendError::Full(r) | TrySendError::Closed(r) => r.request_id.clone(),
            };
            tracing::warn!(%request_id, "ledger row dropped: queue full or writer gone");
        }
    }

    pub fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The metrics text the consumer splices into its /metrics endpoint:
    /// both loss reasons render, with live counter values.
    #[test]
    fn metrics_render_names_both_loss_reasons() {
        let m = Metrics::default();
        m.insert_failures_pg.store(3, Ordering::Relaxed);
        m.insert_failures_channel_full.store(7, Ordering::Relaxed);
        let text = m.render();
        assert!(text.contains("ledger_insert_failures_total{reason=\"pg\"} 3"));
        assert!(text.contains("ledger_insert_failures_total{reason=\"channel_full\"} 7"));
        assert!(text.starts_with("# TYPE ledger_insert_failures_total counter"));
    }

    #[test]
    fn assembly_bounds_every_field() {
        // An 8 MiB statement lands truncated at 32 KiB with the flag set.
        let big_sql = "S".repeat(8 * 1024 * 1024);
        let draft = RowDraft::capture(
            "acme",
            "acme-prod",
            "user:acme/u1",
            "req-1",
            &big_sql,
            Some(&json!({"session_id": "s-1", "purpose": "test"})),
            // The poison case: a refused row whose requested max_rows
            // exceeds i64 must clamp, not sink its batch.
            Some(usize::MAX),
            1000,
        );
        assert_eq!(draft.sql.len(), SQL_MAX_BYTES);
        assert!(draft.sql_truncated);
        assert_eq!(draft.max_rows, i64::MAX);
        assert_eq!(draft.session_id.as_deref(), Some("s-1"));

        let row = draft.finish(
            RowStatus::Refused,
            None,
            Some("plan-error: ".to_string() + &"x".repeat(64 * 1024)),
        );
        assert_eq!(row.status.as_str(), "refused");
        assert!(row.error.as_ref().unwrap().len() <= ERROR_MAX_BYTES);
        assert!(row.row_count.is_none());
        assert!(row.finished_at >= row.created_at);
    }

    #[test]
    fn truncation_respects_char_boundaries() {
        // A multi-byte char straddling the cut must not split.
        let s = format!("{}文", "a".repeat(SQL_MAX_BYTES - 1));
        let (t, truncated) = truncate_utf8(&s, SQL_MAX_BYTES);
        assert!(truncated);
        assert!(t.len() <= SQL_MAX_BYTES);
        assert!(std::str::from_utf8(t.as_bytes()).is_ok());
    }

    #[test]
    fn oversized_ai_context_never_reaches_a_row() {
        // The refused row for an oversized ai_context must not carry it.
        let big = json!({"session_id": "s-1", "blob": "x".repeat(64 * 1024)});
        let draft = RowDraft::capture("o", "w", "u", "r", "SELECT 1", Some(&big), None, 1000);
        assert!(draft.ai_context.is_none());
        // session_id extraction still happened before the drop.
        assert_eq!(draft.session_id.as_deref(), Some("s-1"));
    }

    #[test]
    fn overlong_session_ids_are_dropped_not_truncated() {
        let ai = json!({"session_id": "s".repeat(201)});
        let draft = RowDraft::capture("o", "w", "u", "r", "SELECT 1", Some(&ai), None, 1000);
        assert!(draft.session_id.is_none());
    }

    /// tokio's bounded channel panics on zero; the Result-returning
    /// constructor must refuse instead (AGENTS.md: no panics in production).
    #[tokio::test]
    async fn zero_capacity_is_an_error_not_a_panic() {
        let err = match PgLedger::spawn_with_capacity("postgres://u:p@192.0.2.1:5432/x", 0) {
            Ok(_) => panic!("zero capacity must refuse"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("capacity"), "{err}");
    }

    /// The queue's ~40 MiB worst case requires EVERY field bounded — an
    /// oversized identity string must cap at assembly like everything else.
    #[test]
    fn identity_fields_are_bounded_at_assembly() {
        let huge = "x".repeat(1024 * 1024);
        let draft = RowDraft::capture(&huge, &huge, &huge, &huge, "SELECT 1", None, None, 1000);
        for v in [
            &draft.org_id,
            &draft.workspace_id,
            &draft.user_id,
            &draft.request_id,
        ] {
            assert_eq!(v.len(), IDENTITY_MAX_BYTES);
        }
    }

    /// The SELECT bounds fields server-side so `fetch_all` cannot
    /// materialize unbounded rows; its hardcoded literals must track the
    /// Rust consts, or the two layers drift.
    #[test]
    fn select_page_literals_track_the_rust_bounds() {
        let q = queries::SELECT_PAGE;
        assert!(q.contains(&format!("left(sql, {SQL_MAX_BYTES})")));
        assert!(q.contains(&format!("octet_length(sql) > {SQL_MAX_BYTES}")));
        assert!(q.contains(&format!("left(error, {ERROR_MAX_BYTES})")));
        assert!(q.contains(&format!(
            "octet_length(ai_context::text) <= {ERROR_MAX_BYTES}"
        )));
        for col in [
            "org_id",
            "workspace_id",
            "user_id",
            "request_id",
            "session_id",
        ] {
            assert!(
                q.contains(&format!("left({col}, {IDENTITY_MAX_BYTES})")),
                "{col} must be bounded in the SELECT"
            );
        }
    }

    /// U+0000 is legal in a Rust String but unstorable in Postgres TEXT and
    /// JSONB — one NUL would reject the INSERT and poison the row's whole
    /// flush batch. Assembly scrubs text fields (U+FFFD), drops a NUL-bearing
    /// session_id (scrubbing would rename the session), and drops a
    /// NUL-bearing ai_context whole, keys included.
    #[test]
    fn nul_never_survives_assembly() {
        let ai = json!({"session_id": "s\u{0}1", "purpose": "p"});
        let draft = RowDraft::capture(
            "o\u{0}rg",
            "w",
            "u",
            "r",
            "SELECT '\u{0}'",
            Some(&ai),
            None,
            1000,
        );
        assert_eq!(draft.sql, "SELECT '\u{FFFD}'");
        assert!(!draft.sql_truncated, "a scrub is not a truncation");
        assert_eq!(draft.org_id, "o\u{FFFD}rg");
        assert!(draft.session_id.is_none(), "a NUL session id is dropped");
        assert!(
            draft.ai_context.is_none(),
            "a NUL-bearing document is dropped"
        );

        // NUL hiding in an object KEY is caught too.
        let keyed = json!({"purpose": "p", "me\u{0}ta": {"x": 1}});
        let draft = RowDraft::capture("o", "w", "u", "r", "SELECT 1", Some(&keyed), None, 1000);
        assert!(draft.ai_context.is_none());

        // A nested NUL in an array value is caught.
        let nested = json!({"purpose": "p", "tags": ["ok", "ba\u{0}d"]});
        let draft = RowDraft::capture("o", "w", "u", "r", "SELECT 1", Some(&nested), None, 1000);
        assert!(draft.ai_context.is_none());

        // A clean document still travels.
        let clean = json!({"session_id": "s-1", "purpose": "p"});
        let draft = RowDraft::capture("o", "w", "u", "r", "SELECT 1", Some(&clean), None, 1000);
        assert!(draft.ai_context.is_some());

        // The error text is scrubbed at finish.
        let row = draft.finish(RowStatus::Failed, None, Some("boom\u{0}!".into()));
        assert_eq!(row.error.as_deref(), Some("boom\u{FFFD}!"));
    }

    /// The 200 bound is CHARACTERS, the unit the wire validation counts in —
    /// a 150-char CJK id is 450 UTF-8 bytes and contract-valid, and a
    /// byte-counted filter would silently drop it, breaking its own session
    /// filter. 201 chars stays dropped in any alphabet.
    #[test]
    fn session_id_bound_counts_characters_not_bytes() {
        let cjk = "审".repeat(150);
        assert_eq!(cjk.len(), 450, "the fixture must be multi-byte");
        let ai = json!({ "session_id": cjk.clone() });
        let draft = RowDraft::capture("o", "w", "u", "r", "SELECT 1", Some(&ai), None, 1000);
        assert_eq!(draft.session_id.as_deref(), Some(cjk.as_str()));

        let too_long = json!({ "session_id": "审".repeat(201) });
        let draft = RowDraft::capture("o", "w", "u", "r", "SELECT 1", Some(&too_long), None, 1000);
        assert!(draft.session_id.is_none());
    }
}
