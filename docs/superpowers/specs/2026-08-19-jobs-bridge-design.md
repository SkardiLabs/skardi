# Making the job-submission correlation reconstructable

**Date:** 2026-08-19
**Scope:** `crates/skardi` (jobs store + executor), `crates/server` (jobs
handler), docs. Stacked on the jobs-auth change, which is stacked on the
jobs-audit change (#219). Closes the second non-goal #219 recorded.

## Why now

#219 added `query_audit.job_run_id` because "name + timestamp correlation is
ambiguous under concurrent submissions of the same job." The column removes
that ambiguity on the happy path, but not in general: it is stamped *after*
`executor.submit` has already returned and created the run.

- If `record_job_outcome` fails or times out, `finish_job_audit` logs and
  moves on — correct, since you cannot un-submit. The row keeps
  `status = started`, `job_run_id = NULL`.
- If the process dies in that window, `reconcile_orphaned` rewrites the row
  to `status = unknown`, `job_run_id = NULL`.

In both cases the run exists in `job_runs` and really did execute, but the
correlation was **permanently unrecoverable**: `job_runs` carried neither a
session id nor an audit-row id, so nothing could rebuild it. The exact
ambiguity `job_run_id` was introduced to eliminate came back, silently — and the
surviving row was the least informative one, since `unknown` means "may have
run after a crash" for a query row but "definitely submitted, linkage lost"
for a job row.

This is structurally weaker than the pipeline case, where a lost outcome
stamp costs only the result, not the identity of the thing that ran.

## What

`job_runs` gains a nullable `submission_id`: an **opaque correlation token
supplied by the submitter, stored verbatim and never interpreted by the jobs
subsystem**. The server writes its audit-row id there; a caller with no
ledger passes `None`.

The framing matters for layering. The core `skardi` crate has no notion of
the server's query-audit ledger and does not acquire one here — it stores a
token whose meaning lives entirely with whoever supplied it. What it does
provide is the lookup that makes the token useful:
`JobStore::get_run_by_submission_id`.

**Written in the same INSERT that creates the run**, not stamped afterwards.
That is the whole point: the reverse pointer is durable the moment the run
exists, so it cannot be lost to the same window that loses the forward one.
The two halves are asymmetric on purpose —

| direction | column | when written | reliability |
| --- | --- | --- | --- |
| audit row → run | `query_audit.job_run_id` | after `submit` returns | best-effort |
| run → audit row | `job_runs.submission_id` | with the run's INSERT | durable |

Read as one bridge with a fast half and a reliable half: `job_run_id` for the
common lookup, `submission_id` when it is NULL. Both directions are indexed
(`job_run_id`'s partial index came with #219; `submission_id` gets its own here).

**`submit` takes the token as a required argument** rather than an optional
one behind a defaulted overload. It is an attribution seam, so a new call
site should have to decide rather than inherit a silent `None`. Seven
existing test call sites pass `None` explicitly.

**Migration.** `CREATE TABLE IF NOT EXISTS` no-ops on an existing `jobs.db`
and will not add columns, so the column is bolted on with a guarded
idempotent `ALTER`, reusing #219's duplicate-column tolerance: the
check-then-ALTER is not atomic, and a process that loses the race gets
exactly the post-condition it wanted rather than a startup failure.

**Exposure.** `submission_id` is surfaced on `GET /jobs/runs` and
`GET /jobs/runs/:run_id` — a reverse pointer readable only by opening the
SQLite file is not much of a recovery path for an operator.

## Non-goals

- Making `job_run_id` itself reliable. It cannot be: the run id does not exist
  until `submit` returns. The fix is a second pointer, not a better first
  one.
- Uniqueness of `submission_id`. Nothing in `job_runs` can know whether a
  given token is meant to be unique, so duplicates are a caller error;
  `get_run_by_submission_id` returns the most recent match.
- Putting `session_id` on `job_runs` as well. The audit row already holds it
  and is reachable from the token, so a second copy would be denormalized
  state that can disagree with the ledger.
- `job_runs.parameters` storing raw parameter values — still the open
  threat-model question from #219 and #217.

## Testing

**Store unit tests** (`crates/skardi/src/jobs/store.rs`): token round-trips
and resolves in reverse; a miss returns `None`; unattributed runs keep NULL
without colliding; a pre-column `jobs.db` migrates on open, keeps its old
rows readable, accepts attributed writes, and ends up with the index;
duplicate-column detection is exercised against a real rusqlite error and a
non-duplicate one so it cannot over-match.

**HTTP integration** (`crates/server/tests/jobs_audit_http.rs`): the bridge
points both ways after a real submission; `submission_id` is NULL on an
unaudited server; the runs API exposes it.

The load-bearing one is `correlation_survives_a_lost_forward_stamp`: it
submits through the executor with a token, **never** records an outcome, runs
`reconcile_orphaned` to reproduce the crash, asserts the audit row really is
`unknown` with `job_run_id = NULL`, and then recovers the run from the token
alone. Driven at the executor seam rather than over HTTP precisely because
the handler would race to write the forward stamp this test needs absent.

Mutation-checked: passing `None` instead of the audit id in the handler fails
the bridge tests.
