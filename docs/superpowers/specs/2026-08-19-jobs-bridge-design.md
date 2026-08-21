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

**The pointer is repaired into the ledger at startup, not left to an
operator.** A durable reverse pointer only makes the correlation *recoverable*;
on its own, nothing recovers it, and the row an auditor reads still says
`unknown, NULL` forever — `record_job_outcome` guards on `status = 'started'`,
so once `reconcile_orphaned` has run no later well-behaved write can stamp it.
So `setup_app_state`, which is the one place holding both ledgers, passes over
job rows left `unknown` with `job_run_id IS NULL` after both stores have
reconciled their own orphans, and re-links each through
`get_run_by_submission_id`. `status` stays `unknown`, which remains true: the
outcome was never observed, only the linkage is recovered. The pass is
idempotent, cannot overwrite a correctly-written pointer (`backfill_job_run_id`
guards on `unknown` + NULL), cannot touch a row still `started`, and logs
rather than fails startup — a ledger that cannot be repaired should not stop a
server serving.

A candidate with no matching run is left alone. `job_runs` is never pruned, so
that miss is positive evidence `submit` never created a run — the fact that
separates "definitely submitted, linkage lost" from "never ran", which is the
ambiguity this whole change is about.

**`submit` takes the token as a required argument** rather than an optional
one behind a defaulted overload. It is an attribution seam, so a new call
site should have to decide rather than inherit a silent `None`. Seven
existing test call sites pass `None` explicitly.

**Migration.** `CREATE TABLE IF NOT EXISTS` no-ops on an existing `jobs.db`
and will not add columns, so the column is bolted on with a guarded
idempotent `ALTER`, reusing #219's duplicate-column tolerance: the
check-then-ALTER is not atomic, and a process that loses the race gets exactly
the post-condition it wanted rather than a startup failure. Factored as
`ensure_added_columns(conn, &ADDED_COLUMNS)`, mirroring the shape `query-audit`
already arrived at, so the next column on `job_runs` is a list entry rather
than a fourth copy of the block.

That tolerance is only half a story without the pragmas underneath it, which
this change also adds: `jobs.db` set neither `busy_timeout` nor WAL, so
SQLite's default "fail immediately" busy handler meant the *other*
interleaving — the winner still mid-write when the loser's `ALTER` fires —
surfaced as `database is locked`, which is not a `duplicate column name` and so
took startup down over the same benign race. `query-audit` sets both before its
own column reconciliation for exactly this reason. Every other `job_runs` write
was exposed to the same default.

**Uniqueness is enforced**, by a partial unique index
(`WHERE submission_id IS NOT NULL`). Since `get_run_by_submission_id` resolves
`ORDER BY created_at DESC LIMIT 1`, an unconstrained duplicate makes it return
a *confidently wrong* run — no error, same shape as a correct answer, for the
one column whose purpose is saying which run a submission produced. The
constraint moves that from a wrong answer during an incident to a `create_run`
failure where the mistake is made. It constrains nothing that exists today: the
single production writer mints a fresh audit id per submission. NULLs stay
unconstrained, so unattributed runs — every run on an unaudited server — are
unaffected. The index is created under a new name after a `DROP IF EXISTS` of
the old one, because `CREATE UNIQUE INDEX IF NOT EXISTS` matches on *name* and
would otherwise silently no-op against the non-unique index earlier builds of
this branch wrote, leaving a ledger that looks constrained and is not.

**Exposure: a filter on the way in, not a field on the way out.**
`GET /jobs/runs?submission_id=<token>` resolves the single run carrying that
token; run payloads deliberately do not include it. A reverse pointer readable
only by opening the SQLite file is not much of a recovery path — but neither is
a broadcast one. `submission_id` is a `query_audit` primary key and that ledger
is chmod 0600, while `GET /jobs/runs` returns every run to any authenticated
session (`/jobs/*` auth is authentication, not authorization), so emitting it
would publish one caller's audit-row id to every other caller. The filter also
serves the operator strictly better: `list_runs` clamps to 500 with no offset,
so on a server busy enough for the concurrent-submission ambiguity that
motivated `job_run_id`, the run being looked for is precisely the one that has
fallen off the window.

**`jobs.db` is protected on the same terms as the audit ledger** — created
owner-only and re-chmodded on every open, WAL sidecars included. It has to be:
`job_runs.parameters` holds the raw parameter *values* `query_audit` refuses to
store, and since `submission_id` the file also links each run to a protected
audit row. Two halves of one audit record with one permission decision;
otherwise the weaker half sets the real posture.

## Non-goals

- Making `job_run_id` itself reliable. It cannot be: the run id does not exist
  until `submit` returns. The fix is a second pointer, not a better first
  one.
- A length cap on the token. `submit` takes an unbounded `&str`, matching the
  column's opaque-TEXT contract. The only production caller supplies a
  server-minted audit id, and the failure a cap would prevent — a large string
  in a TEXT column — is not in the class the unique index addresses, which is a
  silently *wrong* lookup answer. Contrast `X-Skardi-Session-Id`, capped at 200
  characters because it arrives from outside.
- Periodic repair. The pass runs at startup only. Rows lost mid-run stay
  `unknown, NULL` until the next boot; the pass is safe to call at any time
  (its guards decline live rows), so a scheduled variant is additive.
- Putting `session_id` on `job_runs` as well. The audit row already holds it
  and is reachable from the token, so a second copy would be denormalized
  state that can disagree with the ledger.
- `job_runs.parameters` storing raw parameter values — still the open
  threat-model question from #219 and #217.

## Testing

**Store unit tests** (`crates/skardi/src/jobs/store.rs`): token round-trips
and resolves in reverse; a miss returns `None`; unattributed runs keep NULL
without colliding; a pre-column `jobs.db` migrates on open, keeps its old
rows readable, accepts attributed writes, and ends up with the *unique* index
and without the old non-unique one; a duplicate token fails `create_run`
instead of shadowing a run; many unattributed runs still insert freely under
the partial index; re-opening a ledger that carries the old index name
converges on the constrained schema and stays re-runnable; `open` leaves the
file and its WAL sidecars 0600 with WAL and a non-zero busy timeout set;
duplicate-column detection is exercised against a real rusqlite error and a
non-duplicate one so it cannot over-match.

**Ledger unit tests** (`crates/query-audit/src/lib.rs`): only `unknown` job
rows with a NULL pointer are repair candidates (a stamped job row and a
reconciled *query* row are both excluded); a backfill restores the pointer,
leaves `status` and `session_id` alone, is idempotent, and declines a row still
`started`. One test pins the premise the whole pass rests on — that
`record_job_outcome` cannot stamp a row `reconcile_orphaned` has already
touched — so if that guard ever changes, the pass is flagged as dead code
rather than left in place.

**HTTP integration** (`crates/server/tests/jobs_audit_http.rs`): the bridge
points both ways after a real submission; `submission_id` is NULL on an
unaudited server; the token resolves through `?submission_id=` and is absent
from both the list and detail payloads; an unmatched token is an empty result
rather than an error; the lookup finds a run that has fallen off the list
window, and `job`/`limit` cannot narrow it away.

The startup repair is driven through the same `repair_lost_job_correlations`
the server calls, not a re-implementation: it re-links the row an auditor
reads, is idempotent across a second boot, leaves a submission that never
created a run alone, and cannot touch a submission still in flight.

**Startup wiring** (`crates/server/tests/jobs_bridge_startup.rs`): those tests
pin what the pass does but not that anything calls it, and a repair pass with
no call site leaves the ledger exactly as broken as no pass at all. So this
boots `setup_app_state` twice over the same two on-disk ledgers — planting a
run carrying a token and an audit row that never got its stamp, then asserting
the second boot re-linked it — and pins that a clean restart leaves a correctly
stamped row untouched. Mutation-checked: disabling the call site fails it.

The load-bearing one is `correlation_survives_a_lost_forward_stamp`: it
submits through the executor with a token, **never** records an outcome, runs
`reconcile_orphaned` to reproduce the crash, asserts the audit row really is
`unknown` with `job_run_id = NULL`, and then recovers the run from the token
alone. Driven at the executor seam rather than over HTTP precisely because
the handler would race to write the forward stamp this test needs absent.

Mutation-checked: passing `None` instead of the audit id in the handler fails
the bridge tests.
