//! Every SQL statement the best-effort ledger runs, as documented constants
//! (SQL lives in `pub const` strings, never inline in call sites) — plus the
//! table's DDL, of which this file is the single source of truth.

/// One batch INSERT row's worth of placeholders is appended per queued row
/// by the writer ([`super::writer`]); this is the fixed head. Column order
/// is the binding order in `writer::flush` — the two are one file apart on
/// purpose, and the store integration test round-trips every column.
pub const INSERT_HEAD: &str = "INSERT INTO query_ledger \
     (org_id, workspace_id, user_id, request_id, session_id, \
      created_at, finished_at, sql, sql_truncated, ai_context, \
      statement_kind, max_rows, status, row_count, error) ";

/// The read page (§5): the deployment's own workspace only — `$1` comes from
/// the envelope, never the query string — newest-first over the stable
/// `(created_at, id)` keyset. The cursor keys land in `$2`/`$3`: rows
/// strictly below `(cursor_created_at, cursor_id)` in that order. For the
/// first page the caller passes `(infinity, i64::MAX)` so the predicate is
/// vacuously true — one statement, no dynamic SQL.
///
/// `$4`/`$5` are `since`/`until` (NULL = unbounded), `$6` the optional
/// `session_id`, `$7` the optional `status`, `$8` the LIMIT (already capped
/// at 500 by the route).
/// Text fields are BOUNDED IN THE SELECT (`left(...)`), not only at Rust
/// serialization: a row written past the assembly can be arbitrarily large,
/// and `fetch_all` materializes up to 500 whole rows before Rust sees one —
/// unbounded fields there are an OOM, not a formatting problem. `left`
/// counts characters (so a multi-byte page may still exceed the BYTE bound
/// by ~4x); the Rust side re-applies the byte-precise bounds, and the
/// `sql_truncated` flag is computed here from `octet_length` so a SQL-side
/// cut is still declared to the reader. The literals must equal
/// `SQL_MAX_BYTES` / `ERROR_MAX_BYTES` / the read field bound — pinned by a
/// unit test.
pub const SELECT_PAGE: &str = "SELECT id, \
            left(org_id, 4096) AS org_id, \
            left(workspace_id, 4096) AS workspace_id, \
            left(user_id, 4096) AS user_id, \
            left(request_id, 4096) AS request_id, \
            left(session_id, 4096) AS session_id, \
            created_at, finished_at, \
            left(sql, 32768) AS sql, \
            (sql_truncated OR octet_length(sql) > 32768) AS sql_truncated, \
            CASE WHEN octet_length(ai_context::text) <= 4096 THEN ai_context \
                 ELSE NULL END AS ai_context, \
            left(statement_kind, 4096) AS statement_kind, \
            max_rows, status, row_count, \
            left(error, 4096) AS error \
     FROM query_ledger \
     WHERE workspace_id = $1 \
       AND (created_at, id) < ($2, $3) \
       AND ($4::timestamptz IS NULL OR created_at >= $4) \
       AND ($5::timestamptz IS NULL OR created_at <= $5) \
       AND ($6::text IS NULL OR session_id = $6) \
       AND ($7::text IS NULL OR status = $7) \
     ORDER BY created_at DESC, id DESC \
     LIMIT $8";

/// The one database the ledger lives in (2026-08-30 design §3). Kept
/// adjacent to [`CREATE_DATABASE`], which spells the same name as a literal:
/// sqlx 0.9's `SqlSafeStr` audit gate accepts only literals, so the pair is
/// co-located here where a rename cannot drift them apart silently.
pub const LEDGER_DB_NAME: &str = "skardi_ledger";

/// The migrate job's existence probe (`skardi-ledger-migrate` step 1):
/// `CREATE DATABASE` cannot run inside a transaction and has no
/// `IF NOT EXISTS`, so the binary probes first and treats a raced duplicate
/// create (42P04) as success.
pub const DATABASE_EXISTS: &str = "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)";

/// See [`LEDGER_DB_NAME`] for why the name is spelled twice.
pub const CREATE_DATABASE: &str = r#"CREATE DATABASE "skardi_ledger""#;

/// Migration 0001 — the `query_ledger` schema's first (and so far only)
/// migration, and the single source of truth for it. This module runs no
/// DDL itself (its writers connect as roles that deliberately cannot); the
/// consumer's migration path applies it, and each consumer migration file
/// must stay BYTE-IDENTICAL to its versioned constant here (skardi-cloud
/// pins that with a test against its sqlx migration, whose checksum is
/// itself locked and append-only).
///
/// **This constant is IMMUTABLE once a consumer has shipped it** — exactly
/// like the migration files it feeds. A schema change is a NEW versioned
/// constant (`QUERY_LEDGER_MIGRATION_0002`, …) that consumers append as a
/// new migration and pin the same way; the full schema is the ordered
/// application of every constant. Editing an existing constant would break
/// every consumer's locked checksum and is never the answer.
pub const QUERY_LEDGER_MIGRATION_0001: &str = r#"-- The cloud query ledger (design: 2026-08-30-query-ledger-postgres-design.md §3).
--
-- One row per DECIDED statement — status ∈ (succeeded, failed, refused);
-- there is no `started` phase and no orphan reconciliation, because the
-- cloud ledger records completed attempts best-effort (§2), which is what
-- permits N engine pods to share this table.
--
-- Applied ONLY by the `skardi-ledger-migrate` job binary (§6) with a
-- superuser-capable DSN; engine pods never run DDL here. Row-level security
-- is FORCED so even a future table owner is policy-bound; per-workspace
-- roles and their policies are minted by the operator, not this migration.
--
-- `org_id`/`workspace_id` are TEXT slugs — the envelope deliberately carries
-- slugs, never row uuids (§3). `org_id` is write-time attribution and is
-- never rewritten; `workspace_id` is ownership (RLS, reads, retention,
-- teardown all pivot on it).

CREATE TABLE query_ledger (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id         TEXT NOT NULL,
    workspace_id   TEXT NOT NULL,
    user_id        TEXT NOT NULL,
    request_id     TEXT NOT NULL,
    session_id     TEXT,
    created_at     TIMESTAMPTZ NOT NULL,
    finished_at    TIMESTAMPTZ NOT NULL,
    sql            TEXT NOT NULL,
    sql_truncated  BOOLEAN NOT NULL DEFAULT FALSE,
    ai_context     JSONB,
    statement_kind TEXT NOT NULL,
    max_rows       BIGINT NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('succeeded', 'failed', 'refused')),
    row_count      BIGINT,
    error          TEXT
);

ALTER TABLE query_ledger ENABLE ROW LEVEL SECURITY;
ALTER TABLE query_ledger FORCE ROW LEVEL SECURITY;

CREATE INDEX ledger_ws_created_idx  ON query_ledger (workspace_id, created_at DESC, id DESC);
CREATE INDEX ledger_ws_session_idx  ON query_ledger (workspace_id, session_id, created_at DESC, id DESC);
CREATE INDEX ledger_org_created_idx ON query_ledger (org_id, created_at DESC);
"#;
