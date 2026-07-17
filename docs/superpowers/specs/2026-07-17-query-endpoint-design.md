# Ad-hoc SQL Query Endpoint for skardi-server

**Date:** 2026-07-17
**Status:** Approved
**Branch:** `BtXin/feat/add_query_endpoint_to_server`

## Goal

Add a `POST /query` endpoint to skardi-server that executes ad-hoc SQL against
the data sources registered from `ctx.yaml`, bringing the CLI's
`skardi query --sql "..."` capability to the HTTP server.

## Requirements

- **Statement policy:** DDL (CREATE, DROP, ALTER, TRUNCATE, ...) is always
  rejected. DML (INSERT, UPDATE, DELETE) is allowed only against sources whose
  `ctx.yaml` entry sets `access_mode: read_write`; sources default to
  `read_only`. SELECT/EXPLAIN/SHOW/DESCRIBE are always allowed.
- **Result cap:** a default cap of 1000 rows applies unless the request
  overrides it with `max_rows`. Responses indicate truncation.
- **Response format:** the same JSON envelope the pipeline-execute endpoint
  returns.
- **Auth:** same session gate as pipeline execution (`verify_session`);
  enforced only when the auth layer is enabled.

## API Contract

### Request

`POST /query`

```json
{ "sql": "SELECT * FROM products WHERE price > 10", "max_rows": 500 }
```

- `sql` (string, required) — exactly one SQL statement. Multi-statement input
  is rejected at validation with a 400 (DataFusion's `ctx.sql()` would reject
  it anyway; validating first gives a clearer error).
- `max_rows` (positive integer, optional, default **1000**) — result row cap.
  There is no server-side maximum; callers may set it as high as they accept
  responsibility for.

The route is mounted in `configure_routes` (`crates/server/src/server.rs`)
alongside existing routes. No conflict with `POST /:name/execute`, which is a
two-segment route.

### Success response (200)

Same envelope as pipeline execution, plus a `truncated` field:

```json
{
  "success": true,
  "data": [ { "col": "value" } ],
  "rows": 500,
  "truncated": true,
  "execution_time_ms": 42,
  "timestamp": "2026-07-17T00:00:00Z"
}
```

`truncated` is `true` when the query produced more rows than the cap; `data`
then contains exactly `max_rows` rows.

### Error responses

All errors use the existing `ErrorResponse` envelope
(`success: false, error, error_type, details, timestamp`):

| Status | `error_type` | Cause |
|--------|--------------|-------|
| 400 | `sql_validation_error` | Parse error, DDL, COPY, multi-statement input, or a write against a `read_only` source. `details` includes the operation and table where applicable. |
| 401 | `unauthorized` | Auth layer enabled and session invalid/missing. |
| 500 | `query_execution_error` | Engine failure during execution. |
| 500 | `result_conversion_error` | RecordBatch → JSON conversion failure. |

The raw SQL is logged at debug level only and never echoed into error
responses (same policy as pipeline execution).

## Components

### 1. `crates/server/src/query_handlers.rs` (new)

`execute_query` handler and its `QueryRequest` deserialization struct. Flow:

1. `verify_session` auth check (identical to `execute_pipeline_by_name`).
2. Build a `SqlValidatorConfig` from `state.config.read().data_sources` —
   per request, so runtime config updates via the `RwLock` are respected.
   This mirrors `validate_pipeline_sql` (`crates/server/src/config.rs`).
3. `validate_sql` — rejects DDL/COPY/multi-statement, enforces per-table
   access modes.
4. Execute via `DataFusionEngine::execute_with_limit(sql, max_rows + 1)`.
5. Slice to `max_rows` rows, set `truncated = fetched > max_rows`.
6. Convert with the shared `record_batch_to_json`, return the success
   envelope with the `truncated` field.

Metrics are recorded through the existing `PipelineMetrics` under the label
`"query"` (success and error paths, same as pipelines).

### 2. Shared response helpers: `crates/server/src/response.rs` (new)

`create_success_response`, `create_error_response`, `record_batch_to_json`,
and the `ErrorResponse` type are currently private to the 1,625-line
`pipeline_handlers.rs`. Extract them into `response.rs` and use them from both
handler modules. No behavior change; targeted cleanup only. The success helper
gains an optional way to attach the `truncated` field (pipeline responses are
unchanged).

### 3. `DataFusionEngine::execute_with_limit` (new method)

In `crates/skardi/src/engine/datafusion.rs`:

```rust
async fn execute_with_limit(&self, sql: &str, limit: usize) -> Result<RecordBatch>
```

Applies `DataFrame::limit(0, Some(limit))` before `collect()`, so the cap
pushes down into the query plan rather than buffering the full result set.
Batch concatenation and empty-result handling follow the existing `execute`
implementation.

The handler chooses the execution path deterministically: validation already
parses the SQL, so it knows whether the statement is a query
(`Statement::Query` → `execute_with_limit`) or a write (INSERT/UPDATE/DELETE →
plain `execute`, since writes return a count/empty batch, not a large result,
and a `Limit` node is not meaningful on a DML plan). Write responses report
`truncated: false`.

### 4. Validator hardening: `crates/skardi/src/sources/sql_validator.rs`

The current catch-all `_ => Ok(())` in `validate_statement` allows
`COPY ... TO '/path'`, which can write files on the server's filesystem. Add
explicit rejections for `Statement::Copy` and `Statement::CopyIntoSnowflake`.
`CREATE EXTERNAL TABLE` is already covered by the `CreateTable` arm.

This also tightens the existing pipeline config-load validation — a strict
improvement. No known `ctx.yaml` pipelines use COPY.

`validate_sql` additionally gains a single-statement check used by the query
endpoint (a new error variant, e.g. `MultipleStatements`).

## Error Handling

- Validation failures never reach the engine.
- Lock poisoning on `state.config` recovers via
  `.unwrap_or_else(|p| p.into_inner())` per project error-handling rules; no
  raw `.unwrap()` outside tests.
- Engine and conversion errors reuse the existing error-detail shapes from
  pipeline execution.

## Testing

- **Unit — validator:** COPY rejected; multi-statement rejected; existing
  DDL/access-mode tests still pass.
- **Unit — engine:** `execute_with_limit` truncates at the limit; returns
  fewer rows untouched; empty result yields an empty batch with the query
  schema.
- **HTTP integration** (`crates/server/tests/query_http.rs`, modeled on
  `pipelines_http.rs`):
  - Plain SELECT against a registered source succeeds with the envelope shape.
  - DDL (`DROP TABLE ...`) → 400 `sql_validation_error`.
  - INSERT into a `read_only` source → 400 with operation/table details.
  - INSERT into a `read_write` source → success.
  - Result larger than `max_rows` → `truncated: true`, exactly `max_rows` rows.
  - Unparseable SQL → 400.
  - Auth enabled + no session → 401.

## Out of Scope

- Arrow IPC / streaming responses (JSON only, matching the rest of the API).
- Query parameterization (callers send final SQL; pipelines already cover
  templated queries).
- Async/job-based execution for long-running queries.
- Per-user or role-based SQL permissions beyond the existing session gate.
