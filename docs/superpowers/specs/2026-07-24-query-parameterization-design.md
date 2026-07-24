# Parameterized `POST /query` + Caller `purpose`

**Date:** 2026-07-24
**Status:** Proposed
**Branch:** `feat/query-parameterization-design`

## Goal

Let callers of the ad-hoc `POST /query` endpoint send a **parameterized** SQL
template plus a separate `params` object, and an optional **`purpose`** string.
This closes an information-leak gap in the endpoint's audit logging and lets us
accumulate caller intent over time.

This revisits a deliberate scoping decision: the original design
(`docs/superpowers/specs/2026-07-17-query-endpoint-design.md:172`) put "query
parameterization" **out of scope** ("callers send final SQL; pipelines already
cover templated queries").

## Motivation

The endpoint records an audit trail before executing every statement
(`crates/server/src/query_handlers.rs:108-113`):

```rust
tracing::info!(
    sql = %request.sql,
    max_rows,
    kind = ?statement_kind,
    "Executing ad-hoc query"
);
```

Because `sql` is a `tracing` field and the endpoint takes **final SQL with
literal values inlined** (`WHERE email = 'alice@example.com'`), this INFO line
is simultaneously a **values log**. When OTLP export is enabled
(`crates/server/src/telemetry.rs`) the field propagates to whatever external
collector is configured, plus any local log sink. Any secret or PII passed as a
literal (`WHERE token = '...'`) is exposed at INFO to everyone who can read
those traces/logs.

Scope of the exposure: it is confined to **observability sinks**. The query text
is *not* sent to any embedding/LLM/external AI service — `remote_embed` and
`llm_extract` only send result column values, never the SQL text.

Two improvements address this:

1. **Parameterization** — callers send a template (`{name}` placeholders) plus a
   `params` object. The endpoint logs the *template* (placeholders, no values)
   and substitutes values only for execution, so literals never reach logs.
2. **`purpose`** — a caller-supplied reason (agents document why they ran a
   query), logged as structured context so intent accumulates on the existing
   audit path.

## API Contract

### Request

`POST /query`

```json
{
  "sql": "SELECT * FROM users WHERE email = {email}",
  "params": { "email": "alice@example.com" },
  "max_rows": 500,
  "purpose": "Resolve the account for a support ticket lookup"
}
```

- `sql` (string, required) — one SQL statement, optionally containing `{name}`
  placeholders.
- `params` (object, optional) — maps placeholder name → JSON value. Same value
  shapes and SQL-safe rendering as pipeline parameters (string quote-escaped;
  number/bool/null verbatim; scalar array → `[a, b, c]` vector literal;
  array-of-arrays → `VALUES` tuple list). When omitted, `sql` is treated as
  final SQL — **backward compatible** with existing callers.
- `max_rows` (positive integer, optional, default 1000) — unchanged.
- `purpose` (string, optional) — caller intent, capped at ~2000 chars. Logged,
  never executed.

### Placeholder convention: `{name}`, not `$1`

We reuse the pipeline endpoint's existing `{name}` named-placeholder machinery
(`substitute_sql_params`, `scalar_to_sql`, `row_cell_to_sql` in
`crates/server/src/pipeline_handlers.rs:455-604`) rather than introducing a
Postgres-style `$1` positional scheme. Rationale:

- **One mental model** across pipelines and ad-hoc queries.
- **Reuse of injection-safe rendering** already covered by tests.
- We inline-substitute rather than use DataFusion's native `$name` binding for
  the same documented reason as the rest of the codebase: some UDTFs
  (`sqlite_fts`) require string **literals** at plan time, before native binding
  would substitute placeholders (`crates/cli/src/main.rs:1519-1522`).

### Success response (200)

Unchanged — same envelope as today (`data`, `rows`, `truncated`,
`execution_time_ms`, `timestamp`).

### Error responses

Existing `ErrorResponse` envelope. New/affected rows:

| Status | `error_type` | Cause |
|--------|--------------|-------|
| 400 | `parameter_validation_error` | A `{name}` placeholder in `sql` has no matching key in `params` (missing), a `params` value is an unsupported shape (empty array, mixed-shape array, inconsistent tuple widths), or `purpose` exceeds the length cap. |
| 400 | `sql_validation_error` | Unchanged — evaluated against the **substituted** SQL. |

## Components

### 1. Shared parameter helpers

Promote `scalar_to_sql`, `row_cell_to_sql`, and `substitute_sql_params`
(`crates/server/src/pipeline_handlers.rs:455-604`) from private to `pub(crate)`
(or lift into a small `crate::sql_params` module). No behavior change to
pipelines.

### 2. `QueryRequest` (`crates/server/src/query_handlers.rs:30-37`)

```rust
pub struct QueryRequest {
    pub sql: String,
    pub max_rows: Option<usize>,
    pub params: Option<HashMap<String, Value>>,   // new
    pub purpose: Option<String>,                  // new
}
```

### 3. Handler flow (`execute_query`)

Between the `max_rows` check and validation:

1. If `params` is present and non-empty: `expected` = its keys **sorted
   longest-first** (the ordering `substitute_sql_params` requires so a shorter
   name can't corrupt a longer one that shares its prefix); clone `sql` into
   `final_sql`; run `substitute_sql_params`. Any `missing`/`unsupported` →
   400 `parameter_validation_error` (mirror the pipeline handler's shaping).
   When no params: `final_sql = sql`.
2. `validate_single_sql(&final_sql, &app_state.adhoc_policy)` — the existing
   security checks (DDL/write/denied-schema/single-statement) run against the
   **executed** SQL, so the invariant holds after substitution. Injection is
   prevented by `scalar_to_sql`'s quote escaping, exactly as in pipelines. The
   validator needs no change (final SQL has no braces).
3. Execute `final_sql` (not `request.sql`).

### 4. Logging changes

- INFO audit line (`:108-113`): log `sql = %request.sql` (the **template** with
  `{name}` placeholders) plus a new `purpose` field and `max_rows`. Value-free
  whenever `params` is used.
- DEBUG lines (`:75`, `:144`): log the **template**, never `final_sql`, so
  literal values never reach logs even at DEBUG.
- `purpose` rides the same OTLP/audit path already in place — no new storage is
  introduced; "rich context over time" is served by the existing trace/log sink.

## Testing

- **Unit:** the query path's use of `substitute_sql_params` — string param
  quote-escaped; scalar array → vector literal; missing `{name}` → error;
  injection attempt (`x' OR '1'='1`) escaped to a literal, matches nothing.
- **HTTP integration** (extend the `/query` tests):
  - Parameterized SELECT returns the row; captured INFO log contains `{email}`
    and **not** the value (assert via a `tracing` capture layer).
  - `purpose` present → appears as a span/log field.
  - No `params`/`purpose` → behaves exactly as today (backward compat).
  - Missing param → 400; unsupported param shape → 400; over-long `purpose` → 400.

## Out of Scope

- DataFusion native `$name`/`ParamValues` binding (blocked by UDTF literal-at-
  plan-time requirement; inline substitution is the codebase-wide pattern).
- `$1` positional placeholders (we standardize on `{name}` for reuse).
- Persisting `purpose` to a dedicated store/table — structured logging on the
  existing audit path is the mechanism for now.
- Redacting values from *un-parameterized* requests: callers that still send
  final SQL with inlined literals accept that those literals are logged (same as
  today). Parameterization is the opt-in path to keep values out of logs.
