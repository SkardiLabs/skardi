# Replace `/query`'s flat `purpose` with a structured `ai_context` object

**Date:** 2026-07-28
**Status:** Proposed — partly superseded by
[2026-07-31](2026-07-31-query-audit-store-design.md), which replaces the file
sink with a durable SQLite ledger and closes the `ai_context: null` gap.
**Branch:** `feat/query-parameterization-design`

## Goal

Evolve the single `POST /query` endpoint (see the 2026-07-24 spec) so that caller
intent is carried in a structured, extensible **`ai_context`** JSON object instead
of a flat `purpose` string.

`ai_context` is **optional** — application/console queries don't need it — but when
an AI agent supplies it, it must carry a minimum structure: a `purpose` and a
`session_id`. Beyond those, the object is free-form, so users can design their own
context structure over time. This is the first step toward Skardi as a
"data-query-driven context" platform: the query carries the context that explains
and groups it.

There is a **single** query endpoint; no `/parameterized_query` split.

## Motivation

The 2026-07-24 change added a flat `purpose: Option<String>` so callers could
document why a query runs. That was too narrow:

- Intent is not the only thing worth attaching to a query. Agents also need to
  **group** queries that belong to one reasoning session, so a query log can be
  read as a session, not a pile of unrelated statements.
- A flat field can't grow. A JSON object lets callers attach their own keys
  (agent id, trace id, task id, …) without further endpoint changes.

Naming it `ai_context` (not `context`) signals that this is agent-supplied
metadata; ordinary application queries are expected to omit it entirely.

As before: this concerns observability only. The query text and `ai_context` are
never sent to any embedding/LLM/external service.

## API Contract

`POST /query` — the `purpose` field is **replaced** by `ai_context`; response
unchanged.

```json
{
  "sql": "SELECT * FROM products WHERE price > 10",
  "max_rows": 500,
  "ai_context": {
    "purpose": "Populate the weekly pricing dashboard",
    "session_id": "sess-2026-07-28-abc123",
    "agent_id": "pricing-bot"
  }
}
```

- `sql` (string, required) — unchanged; final SQL, one statement.
- `max_rows` (positive integer, optional, default 1000) — unchanged.
- `ai_context` (object, optional) — agent-supplied context. When **present**:
  - must be a JSON **object** (not an array, string, number, bool, or null);
  - must contain **`purpose`**: a non-empty string, ≤ `MAX_PURPOSE_CHARS` (2000);
  - must contain **`session_id`**: a non-empty string, ≤ `MAX_SESSION_ID_CHARS`
    (200), used to group queries from one agent session;
  - may contain any other keys, free-form and unvalidated;
  - total serialized size ≤ `MAX_AI_CONTEXT_BYTES` (4096).

Any violation → `400` with `parameter_validation_error`. Omitting `ai_context`
entirely is valid and behaves exactly as a request with no context (backward
compatible with pre-`ai_context` callers and with application queries).

## Validation rules

Checked before SQL validation/execution, in this order (first failure wins):

1. `ai_context` absent → skip all context checks.
2. `ai_context` present but not a JSON object → 400
   (`"ai_context must be a JSON object"`).
3. Serialized `ai_context` byte length > `MAX_AI_CONTEXT_BYTES` → 400.
4. `purpose` missing / not a string / empty → 400.
5. `purpose` length > `MAX_PURPOSE_CHARS` → 400.
6. `session_id` missing / not a string / empty → 400.
7. `session_id` length > `MAX_SESSION_ID_CHARS` → 400.

Each failure records a `parameter_validation_error` metric, mirroring the existing
`max_rows` / `purpose` validation paths.

## Logging

`ai_context` is caller **metadata**, not data values — logging it is the point.
The SQL-secrecy posture from the 2026-07-24 spec is unchanged: raw SQL is still
never logged in the general stream and goes only to the opt-in file sink.

- **INFO audit marker** (`query_handlers.rs`): replace the `purpose` field with the
  full serialized `ai_context` (an empty/omitted context logs as `{}` or is
  omitted). Still no `sql` field on this line.
- **Operator sink**: the record replaces the `purpose` column with the
  `ai_context` object, alongside the raw `sql`, `max_rows`, and timestamp.
  (The sink itself became the SQLite ledger in the 2026-07-31 spec.)

## Components

- `crates/server/src/query_handlers.rs`
  - `QueryRequest.purpose: Option<String>` → `ai_context: Option<serde_json::Value>`.
  - Add the validation rules above; introduce `MAX_SESSION_ID_CHARS` and
    `MAX_AI_CONTEXT_BYTES` constants; keep/reuse `MAX_PURPOSE_CHARS`.
  - Thread `ai_context` into the INFO marker and the file-log call.
- `crates/server/src/query_log.rs`
  - `record(sql, purpose: Option<&str>, max_rows)` →
    `record(sql, ai_context: Option<&serde_json::Value>, max_rows)`; the JSON line
    gains an `ai_context` field and drops `purpose`.
- `crates/server/tests/query_http.rs` — update existing `purpose` tests to the new
  shape; add the new validation cases (see Testing).
- `docs/server.md` — document `ai_context`, its required inner fields, the size
  caps, and that it is optional and agent-oriented.

## Testing

- No `ai_context` → 200; behaves as today (backward compatible).
- Valid `ai_context` (purpose + session_id) → 200; the INFO marker and (when the
  file sink is enabled) the query-log line contain the `ai_context` object.
- `ai_context` not an object (e.g. a string or array) → 400
  `parameter_validation_error`.
- `ai_context` present but missing `purpose` → 400.
- `ai_context` present but missing `session_id` → 400.
- `purpose` empty or over `MAX_PURPOSE_CHARS` → 400.
- `session_id` empty or over `MAX_SESSION_ID_CHARS` → 400.
- `ai_context` over `MAX_AI_CONTEXT_BYTES` → 400.
- Extra free-form keys (e.g. `agent_id`) are accepted and preserved into the log.

## Rejected alternatives

- **Keep the flat `purpose` string** — can't group queries by session and can't
  grow without further endpoint changes.
- **Make `ai_context` required** — would break application/console callers and
  existing tests; the platform value comes from agents *choosing* to supply rich
  context, not from forcing it on every query.
- **Validate the full free-form structure** — defeats the extensibility goal;
  only the two anchor fields (`purpose`, `session_id`) plus an overall size cap
  are enforced.

## Out of Scope

- Query parameterization / a `params` channel (still out, as in the 2026-07-24
  spec).
- ~~Persisting `ai_context` to a database/store (the local file sink only).~~
  Superseded: the 2026-07-31 spec persists it to a SQLite ledger, indexed by
  `session_id`.
- Aggregating or querying by `session_id` server-side (operators query the
  ledger directly).
