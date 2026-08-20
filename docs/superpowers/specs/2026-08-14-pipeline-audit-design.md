# Pipeline-execution auditing: close the loop's blind spot

**Date:** 2026-08-14
**Scope:** `crates/server` (pipeline handler + audit store), `crates/cli` (`skardi run --session-id`), `docs/server.md`, `docs/pipelines.md`, `docs/cli.md`

## Problem

The query audit ledger (`--query-audit-db`, #173) records every ad-hoc
`POST /query` — but pipeline executions are not recorded anywhere
(`pipeline_handlers.rs` never touches the audit store). This makes the
self-improving loop degrade its own input as it succeeds: the moment the
query-log skill promotes a recurring query into a pipeline
(skardi-skills#25), that query's recurrence signal leaves the ledger. The
Learn stage goes blind to exactly the tools the Act stage created.

## Design

**One ledger, both kinds of executions.** Every data access the server
performs gets a row in the same `query_audit` store, whether it arrived as
ad-hoc SQL or a named pipeline call.

### What a pipeline row records

| column | value for a pipeline row |
| --- | --- |
| `statement_kind` | `"pipeline"` (ad-hoc rows keep `Query` / `Other` — the `Debug` form of the server's `StatementKind` classifier, not SQL verbs; correction from review, the original draft wrongly said `select` / `dml`). **Superseded** by `2026-08-18-jobs-audit-design.md`: ad-hoc rows are now `query` / `other`, mapped explicitly, so all four values share one casing. |
| `sql` | **`name@version`** (from `metadata.version`; revised in review — originally the bare name, but rows outlive template revisions since retention is off by default, so the name alone stops answering *what SQL ran*) — the versioned template lives on disk with no secrets and `metadata.description` carries the purpose (written at promotion time) |
| `session_id` | from the optional `X-Skardi-Session-Id` request header |
| `ai_context` | NULL — purpose lives in the pipeline description; a synthetic object would masquerade as caller-sent data |
| `max_rows` | 0, documented as "not applicable to pipeline rows" (column is NOT NULL; a sentinel beats a schema change) |
| `created_at` / `finished_at` / `status` / `row_count` / `error` | same lifecycle as ad-hoc rows |

Parameter values are **never recorded** — params are exactly where PII
lives, consistent with the existing confidentiality stance (raw ad-hoc SQL
is itself only stored because the operator opted in by configuring the DB).

No schema change and no migration: `statement_kind` and `session_id`
columns already exist. `--query-audit-db` has never shipped in a tagged
release, so there are no deployed DBs to migrate anyway — but we don't need
the freedom.

### Transport: why a header, not a body field

The pipeline request body IS the parameter map (`ExecuteRequest` flattens
it), so any reserved body key could collide with a legitimate SQL parameter
of the same name. `X-Skardi-Session-Id` avoids that, requires no change to
the inferred request schema, and is ignorable by existing callers.

Validation mirrors `ai_context.session_id` on `/query`: non-empty, ≤ 200
chars. A malformed header (empty, oversized, outside visible ASCII, or
containing a space, tab or comma — the last three added in review) → 400
`parameter_validation_error` with `details.header` — silently dropping it
would corrupt session stitching, the one job the field has. Space and tab
are rejected because HTTP parsers trim surrounding whitespace, so an
untrimmed value would be recorded under a different key than the caller
sent; comma because intermediaries may merge repeated header lines
comma-separated (RFC 9110 §5.3, §5.5). The CLI enforces the identical
predicate before transport, so no value can pass client-side and be
rewritten or rejected server-side.

### Semantics (mirroring `/query` exactly)

- **Record-before-execute.** The `started` row is committed after parameter
  validation succeeds, before the engine runs. Param-validation failures are
  not audited (nothing executed), same as `/query`'s rejects.
- **Fail-closed.** If the pre-execution write fails, respond 503 and do not
  run the pipeline. An operator who configured an audit trail must never get
  unaudited executions.
- **Free rides.** Orphan reconciliation, retention pruning, and
  `list_by_session` all key on columns pipeline rows share — they cover the
  new rows with zero changes.
- **No store configured → no-op.** `query_audit: None` keeps today's
  behavior exactly.

### Store API

A dedicated `record_pipeline_started(name, session_id)` insert on
`QueryAuditStore` rather than bending `record_started` (whose signature
derives `session_id` from an `ai_context` object pipelines don't have).
`record_outcome` is reused as-is.

### CLI companion

`skardi run` gains `--session-id <ID>`, sent as the header. Optional; no
default. (`skardi query` ai_context passthrough remains a separate,
already-noted gap — out of scope here.)

## Non-goals

- Recording rendered SQL or parameter values for pipeline rows.
- Per-call `purpose` on pipelines — `metadata.description` carries it.
- `ai_context` object transport for pipelines. If a future need appears,
  the header can grow a sibling; today `session_id` is the only per-call
  fact with no static substitute.
- Auditing the pipeline dashboard's internal reads, health checks, or
  `GET` endpoints — only `POST /:name/execute`.
- Auditing job runs (`POST /jobs/:name/run`) in this ledger. Jobs already
  have their own durable run ledger (parameters, status, run id — the
  SQLite jobs store), so unlike pipelines they were never unobserved; what
  they lack is `ai_context`/session attribution in the *query* ledger.
  Unifying the two ledgers is real future work — best coordinated with the
  identity-column work in #206 rather than bolted on here. Superseded by
  `2026-08-18-jobs-audit-design.md`, which adds submission-event
  attribution while leaving run records in the jobs ledger.

## Testing

- **Store unit tests** (`query_audit.rs`): pipeline row round-trip;
  `list_by_session` interleaves ad-hoc and pipeline rows for one session
  ordered by `created_at`; orphaned pipeline rows reconcile.
- **HTTP integration tests** (`tests/pipelines_http.rs` or a new
  `tests/pipeline_audit_http.rs`): success writes `started`→`succeeded`
  with name + row_count; engine failure writes `failed` with error; header
  present → `session_id` recorded; absent → NULL; malformed header → 400
  and nothing recorded; param-validation failure → nothing recorded; store
  write failure → 503 and pipeline does not run (reuse the
  `close_for_test` trick from `query_http.rs`); no store configured →
  everything works, nothing recorded.
- **CLI test** (`crates/cli`): `--session-id` sets the header
  (wiremock header assertion, matching existing CLI test style).

## Docs

- `docs/server.md`: extend the audit-ledger section — pipeline rows, the
  header, the max_rows sentinel, fail-closed parity.
- `docs/pipelines.md`: document `X-Skardi-Session-Id` on execute.
- `docs/cli.md`: `skardi run --session-id`.
