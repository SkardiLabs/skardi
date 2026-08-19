# Job-submission attribution: the loop's last unattributed surface

**Date:** 2026-08-18
**Scope:** `crates/server` (jobs handler + audit store), `crates/cli` (`skardi job run --session-id`), docs. Stacked on #213 (`worktree-pipeline-audit`); rebased onto #206's ledger-crate extraction (see *Rebase decisions* below);
supersedes the jobs non-goal in `2026-08-14-pipeline-audit-design.md`, and
its `statement_kind` row: ad-hoc rows now record `query` / `other`, mapped
explicitly rather than leaked from `StatementKind`'s `Debug` form, so the
ledger's four-value vocabulary shares one casing while `--query-audit-db`
is still unreleased and the change is free.

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
`query_audit`; run detail stays in `job_runs`, bridged by `job_run_id`. No
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
| `job_run_id` | **new nullable column** — `job_runs.id` once the submission is accepted; NULL on non-job rows and on rejected submissions |
| `status` | submission-scoped: `started` before submit → `succeeded` + `job_run_id` once enqueued → `failed` + fixed error kind if the executor rejects it. For job rows, `succeeded` means "submission accepted", documented on the method; run outcome lives in `job_runs`. (Alternative considered: a new `submitted` terminal status — rejected to keep the status enum and every existing consumer's filter unchanged.) |
| `error` on rejection | `JobSubmitError::category()` — static per-variant strings (`unknown_job`, `schema_mismatch`, …), value-free by construction; the HTTP response keeps the full message |

### Schema change: `job_run_id`

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
  keep their signature, and the `job_run_id` stamp is job-specific)

## Rebase decisions (#206 landed first)

#206 extracted the ledger into `crates/query-audit` (`skardi-query-audit`,
re-exported at the old path) and added five nullable identity columns. Three
consequences for this change, resolved as follows.

**The bridge column is `job_run_id`, not `run_id`.** #206's identity envelope
already claims `run_id`, meaning *which of the caller's runs issued this
statement* — filled at insert time by a distribution that authenticates its
callers. This change's column means *which job run this submission produced*,
stamped at outcome time. Same name, two meanings, distinguishable only by
`statement_kind`: exactly the overloading review pushed back on for
`statement_kind`'s own casing. Renaming ours was the only option that did not
redefine a just-merged field the cloud engine adopts wholesale. A test asserts
the bridge stamp leaves the identity `run_id` NULL.

**One additive-column mechanism, not two.** #206's `ensure_identity_columns`
already reconciles the live schema against a column list on every open, which
is what this change's standalone guarded `ALTER` was doing for one column.
Generalised to `ensure_added_columns(conn, columns)` and called with two named
lists — `IDENTITY_COLUMNS` (who asked) and `BRIDGE_COLUMNS` (what the work
became) — kept separate because they answer different questions. The
duplicate-column tolerance review asked for now covers all six columns rather
than only ours: #206 introduced the same check-then-ACT race five times over.

**`session_id_from_headers` moves to the server, not the ledger.** This change
originally relocated it into `query_audit` as its third consumer. That is no
longer possible: the helper needs `axum::http::HeaderMap`, and
`skardi-query-audit` depends on neither `axum` nor `skardi` — which is the
entire point of the extraction. It now lives in
`crates/server/src/session_header.rs`, shared by the pipeline and jobs
handlers, still validating against the ledger's `MAX_SESSION_ID_CHARS` so the
three attribution paths cannot drift. For the same reason the
`StatementKind` → marker mapping sits in `query_handlers` while the marker
strings stay `pub` in the ledger: vocabulary owned by the ledger, translation
owned by the server.

## Non-goals

- Auditing run *outcomes* in `query_audit` (the jobs ledger owns them;
  `job_run_id` bridges).
- `GET /jobs/*` reads, run polling, cancellation — reads and control-plane
  operations, not data-touching submissions. (Cancel arguably mutates; it
  acts on a run already attributed at submission.)
- Backfilling `require_session` onto jobs handlers: they perform **no auth
  check at all** today, unlike `/query` and pipelines. Pre-existing as a
  *submission* concern, but this change gives it a second edge worth stating
  plainly: `POST /jobs/:name/run` becomes the audit ledger's only
  unauthenticated write path. An unauthenticated caller can therefore mint
  arbitrary `session_id` values into `query_audit` — including ones belonging
  to real agent sessions, which the Learn stage stitches on and cannot
  distinguish — and can queue unrate-limited work onto the store's single
  serialized writer thread, whose stalls 503 every concurrent `/query` and
  pipeline execute. Called out in `docs/server.md`'s ledger section so an
  operator enabling `--query-audit-db` behind auth knows one seam is not
  gated; the fix is a scope of its own.
- **Guaranteeing** the `job_run_id` correlation. The column exists because
  name + timestamp is ambiguous under concurrent submissions of the same
  job, and it removes that ambiguity on the happy path — but the stamp is
  written after `executor.submit` has already created the run, best-effort.
  If it fails, times out, or the process dies first, the run exists in
  `job_runs` while the audit row keeps `run_id = NULL` and reconciles to
  `unknown`. Because `job_runs` carries neither `session_id` nor an
  audit-row id, there is no reverse pointer: the correlation is
  unrecoverable, not merely delayed, and `unknown` on a job row means
  "definitely submitted, linkage lost" rather than the "may have run after a
  crash" it means on a query row. Closing this needs a durable half in the
  jobs ledger — an `audit_id` (or `session_id`) column on `job_runs`, plumbed
  through `executor.submit` — which puts a server-layer audit concern into
  the core `skardi` crate's jobs subsystem, a layering decision this change
  does not make. Stated in `docs/server.md` so consumers read `job_run_id` as
  usually-exact rather than guaranteed.
- Reconciling the jobs ledger's raw-parameter storage with the query
  ledger's never-store-values stance. Flagged: `job_runs.parameters` holds
  exactly what `query_audit` refuses to hold, and deserves the same
  threat-model review — but changing what the jobs ledger records is a
  behavior change for existing consumers of `GET /jobs/runs`.

## Testing

- **Store unit tests**: job row round-trip incl. `job_run_id` stamp;
  `list_by_session` interleaves all three kinds in insertion order;
  orphaned job rows reconcile; migration adds `job_run_id` to a pre-existing
  old-schema database file (open twice: once with the old DDL, once
  through `open()`).
- **HTTP integration** (`tests/jobs_audit_http.rs`, harness cribbed from
  `jobs_http.rs` + `pipeline_audit_http.rs`): submission audited with
  session and `job_run_id` equal to the response's; absent header → NULL
  session; no store → no-op; unknown job + malformed header → 404,
  nothing recorded; malformed header on a real job → 400, nothing
  recorded; fail-closed 503 → **no run row in the jobs ledger** (the
  "not submitted" half is observable, unlike #213's engine); rejected
  submission → `failed` row, fixed kind, `job_run_id` NULL; params canary —
  a planted parameter value appears nowhere in the serialized audit row
  (it *will* legitimately be in `job_runs.parameters`; the canary greps
  only the audit row).
- **CLI**: `--session-id` header present/absent; invalid value fails fast
  with no request (shared helper, both commands).

## Docs

- `docs/server.md`: extend the ledger section — job rows, `job_run_id`
  bridge, submission-vs-run semantics, the `statement_kind` value set
  gains `job`.
- `docs/jobs.md`: session attribution on submit; the retryable
  `503 query_audit_error` (same contract as pipelines).
- `docs/cli.md`: `skardi job run --session-id`.
- `2026-08-14-pipeline-audit-design.md`: non-goals note updated to point
  here (superseded).
