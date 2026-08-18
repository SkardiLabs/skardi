# Job-submission attribution: the loop's last unattributed surface

**Date:** 2026-08-18
**Scope:** `crates/server` (jobs handler + audit store), `crates/cli` (`skardi job run --session-id`), docs. Stacked on #213 (`worktree-pipeline-audit`); supersedes the jobs non-goal in `2026-08-14-pipeline-audit-design.md`.

## Problem

Job *runs* are fully observed — the jobs ledger (`job_runs`) records
parameters, status, timestamps, and run id. What they lack is
*attribution*: no `session_id`, no link to the agent session that submitted
them. So the one thing the Learn stage cannot do is stitch "this session ran
four ad-hoc queries, called two pipelines, then submitted a backfill job"
into a single intention — and an agent that manually submits the same job
on a cadence (a promotion signal: it wants to be a routine) is invisible to
mining. Meanwhile scheduler-style callers carry no intention signal at all.
Conveniently, skardi has no built-in cron: every submission arrives through
`POST /jobs/:name/run`, so attribution has exactly one seam, and "no
session header = not agent behavior" falls out naturally.

## Design

**Record the submission event, not the run.** One new row kind in
`query_audit`; run detail stays in `job_runs`, bridged by `run_id`. No
double-logging: the audit row's lifecycle is the *submission's*, and the
jobs ledger remains the authority on the run.

### The job row

| column | value |
| --- | --- |
| `statement_kind` | `"job"` |
| `sql` | `name@version` (from `JobDefinition::version()`), same convention as pipeline rows |
| `session_id` | from the optional `X-Skardi-Session-Id` header — same validation, same shared cap |
| `ai_context` | NULL (same rationale as pipeline rows) |
| `max_rows` | 0 sentinel (same invariant note) |
| `run_id` | **new nullable column** — `job_runs.id` once the submission is accepted; NULL on non-job rows and on rejected submissions |
| `status` | submission-scoped: `started` before submit → `succeeded` + `run_id` once enqueued → `failed` + fixed error kind if the executor rejects it. For job rows, `succeeded` means "submission accepted", documented on the method; run outcome lives in `job_runs`. (Alternative considered: a new `submitted` terminal status — rejected to keep the status enum and every existing consumer's filter unchanged.) |
| `error` on rejection | `JobSubmitError::category()` — static per-variant strings (`unknown_job`, `schema_mismatch`, …), value-free by construction; the HTTP response keeps the full message |

### Schema change: `run_id`

First actual schema change to the ledger. `--query-audit-db` remains
unreleased, but dev databases exist, so `open()` gains an idempotent guarded
migration: `ALTER TABLE query_audit ADD COLUMN run_id TEXT` when
`pragma table_info` lacks it. `get()`/`list_by_session` include the column.
Exact-correlation is the point: name + timestamp correlation is ambiguous
under concurrent submissions of the same job.

### Handler semantics (mirroring #213's lessons)

1. **Jobs subsystem disabled** → existing `503 jobs_disabled`, before
   anything else (unchanged).
2. **Job existence checked before header validation** (via `config.jobs`,
   the same read-lock pattern as the pipeline lookup): unknown job → 404
   regardless of header shape. This is round 3's metric-cardinality /
   status-precedence lesson applied from the start — even though jobs
   handlers currently record no metrics, the 404-wins precedence is the
   contract. The existence pre-check races with `executor.submit`'s own
   resolution (config can change between them); a TOCTOU loser gets a
   `failed` audit row with `unknown_job`, which is accurate.
3. **Header validated** with the same `session_id_from_headers`, which
   **moves from `pipeline_handlers` to `query_audit`** alongside
   `SESSION_ID_HEADER` — third consumer, same boundary argument that moved
   `finish_audit` (this is ledger-attribution plumbing, not
   pipeline-endpoint logic).
4. **Record-before-submit, fail-closed**: `record_job_submitted` commits the
   `started` row before `executor.submit`; a failed/timed-out write → `503
   query_audit_error` and the job is **not submitted** (nothing may run
   unaccounted — the run is what must not start). All writes inherit
   `AUDIT_WRITE_TIMEOUT` via `bounded`.
5. **Outcome**: submit `Ok(run_id)` → `record_job_outcome(id,
   Some(run_id), Succeeded, None)`; submit `Err` → `record_job_outcome(id,
   None, Failed, Some(err.category()))`, response unchanged.
6. **No store configured** → today's behavior exactly.

### CLI

`skardi job run <name> --session-id <ID>`, sent as the header. The
client-side validation (non-empty, ≤ 200 chars, visible ASCII, no comma/tab)
moves from `run.rs` into a shared `crates/cli/src/session.rs`
(`MAX_SESSION_ID_CHARS` + `validate_session_id`) consumed by both commands —
the same rule must not be maintained twice inside one crate.

### Store API

- `record_job_submitted(&self, job_name: &str, version: &str, session_id: Option<&str>) -> Result<String>`
- `record_job_outcome(&self, id: &str, run_id: Option<&str>, status: QueryAuditStatus, error: Option<&str>) -> Result<()>`
  (dedicated method rather than widening `record_outcome`: existing callers
  keep their signature, and the `run_id` stamp is job-specific)

## Non-goals

- Auditing run *outcomes* in `query_audit` (the jobs ledger owns them;
  `run_id` bridges).
- `GET /jobs/*` reads, run polling, cancellation — reads and control-plane
  operations, not data-touching submissions. (Cancel arguably mutates; it
  acts on a run already attributed at submission.)
- Backfilling `require_session` onto jobs handlers: they perform **no auth
  check at all** today, unlike `/query` and pipelines. Pre-existing,
  surfaced to maintainers as an observation — a scope of its own.
- Reconciling the jobs ledger's raw-parameter storage with the query
  ledger's never-store-values stance. Flagged: `job_runs.parameters` holds
  exactly what `query_audit` refuses to hold, and deserves the same
  threat-model review — but changing what the jobs ledger records is a
  behavior change for existing consumers of `GET /jobs/runs`.

## Testing

- **Store unit tests**: job row round-trip incl. `run_id` stamp;
  `list_by_session` interleaves all three kinds in insertion order;
  orphaned job rows reconcile; migration adds `run_id` to a pre-existing
  old-schema database file (open twice: once with the old DDL, once
  through `open()`).
- **HTTP integration** (`tests/jobs_audit_http.rs`, harness cribbed from
  `jobs_http.rs` + `pipeline_audit_http.rs`): submission audited with
  session and `run_id` equal to the response's; absent header → NULL
  session; no store → no-op; unknown job + malformed header → 404,
  nothing recorded; malformed header on a real job → 400, nothing
  recorded; fail-closed 503 → **no run row in the jobs ledger** (the
  "not submitted" half is observable, unlike #213's engine); rejected
  submission → `failed` row, fixed kind, `run_id` NULL; params canary —
  a planted parameter value appears nowhere in the serialized audit row
  (it *will* legitimately be in `job_runs.parameters`; the canary greps
  only the audit row).
- **CLI**: `--session-id` header present/absent; invalid value fails fast
  with no request (shared helper, both commands).

## Docs

- `docs/server.md`: extend the ledger section — job rows, `run_id`
  bridge, submission-vs-run semantics, the `statement_kind` value set
  gains `job`.
- `docs/jobs.md`: session attribution on submit; the retryable
  `503 query_audit_error` (same contract as pipelines).
- `docs/cli.md`: `skardi job run --session-id`.
- `2026-08-14-pipeline-audit-design.md`: non-goals note updated to point
  here (superseded).
