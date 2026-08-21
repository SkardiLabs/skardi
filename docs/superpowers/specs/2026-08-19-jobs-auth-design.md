# Jobs endpoints: require an authenticated session

**Date:** 2026-08-19
**Scope:** `crates/server` (jobs handlers), docs. Stacked on the jobs-audit
change (#219); closes the non-goal that change recorded rather than fixed.

## Why now

The jobs handlers have never performed an auth check, while `/query` and
`POST /:pipeline/execute` both call `require_session` as their first
statement. On its own that is a submission-authorization gap and arguably
pre-existing. #219 changed what it costs:

- **Attribution forgery.** Job submissions now write `session_id` into
  `query_audit` from the `X-Skardi-Session-Id` header. That made
  `POST /jobs/:name/run` the ledger's *only* unauthenticated write path, so
  anyone who could reach it could mint rows carrying a real agent session's
  id. The Learn stage stitches intentions on exactly that key, and a forged
  row lands inside a legitimate session's `list_by_session` read
  indistinguishable from a real one.
- **Cross-endpoint availability coupling.** Every submission — including ones
  the executor will reject — issues writes on `QueryAuditStore`'s single
  serialized writer thread, shared with every audited `/query` and pipeline
  execution. Work exceeding `AUDIT_WRITE_TIMEOUT` there 503s those other
  requests. The endpoint handed an unauthenticated actor an unrate-limited
  way to queue onto it.

Authentication does not make the header trustworthy — it is self-reported on
every endpoint, by design — but it bounds *who* can self-report to callers
the operator has admitted.

## What

`require_session` on all five `/jobs/*` handlers, not only the write path.
The load-bearing reason is the run history: `GET /jobs/runs` and
`GET /jobs/runs/:run_id` return `job_runs.parameters` — raw parameter
*values*, which is exactly what `query_audit` refuses to store, and which
nothing else on the server exposes. Gating submissions while leaving those
readable would close the smaller hole.

`GET /jobs` is gated for consistency across the family rather than for
confidentiality: job names, versions, destinations, parameter names and
example request bodies are all rendered by the ungated dashboard at `GET /`
(`gui.rs::render_job_card`), so gating this route alone does not make them
non-public. See the non-goals.

**Placement: first statement, ahead of the jobs-disabled 503 and the
job-existence 404.** #219 established a precedence ladder among those
checks; the auth gate deliberately sits outside it rather than inside, so
neither "jobs are enabled here" nor "this job name exists" is readable off
the status code — both otherwise are.

"First statement" is about the handler body. Axum runs extractors ahead of
it, so a malformed `?limit=` or a non-object JSON body still answers
400/422 before the gate is reached. The residue is schema-shaped rather
than data-shaped, is identical on `/query` and `POST /:pipeline/execute`,
and is bounded by axum's 2 MB default body cap; the audit writes this gate
exists to protect all sit below it. Making the invariant hold literally
would mean a middleware gate on the `/jobs` subtree — out of scope here.

**Response shape.** `require_session` reports through the shared
`ErrorResponse`; the jobs endpoints answer in `JobErrorResponse`. A thin
`require_job_session` adapter keeps the status and re-renders the body, so
one endpoint family does not answer in two envelopes. Both `error` and
`error_type` are propagated rather than the type being pinned to
`unauthorized` — the only classification `require_session` produces today,
but `verify_session` already distinguishes "Authentication required" from
"Invalid or expired session" internally, and a constant would let the
message diverge from the type if that ever surfaced.

**No behaviour change without auth.** `verify_session` returns `Ok` when
`auth_layer.as_better_auth()` is `None`, so servers that never configured
auth are unaffected — pinned by a test rather than left to inspection.

## Non-goals

- Authorization beyond "has a valid session" — no per-job ACLs, no
  distinction between submitting and cancelling. Every authenticated caller
  can do everything, as on `/query` and pipelines today.
- Making the session header attestable. It remains self-reported; see the
  ledger section of `docs/server.md`.
- `job_runs.parameters` storing raw parameter values. Gating the read
  narrows the exposure but does not resolve the threat-model question #219
  flagged alongside #217.
- Gating the dashboard at `GET /`. It renders every job's name, version,
  destination, parameter list and an example request body, so job names are
  effectively public whatever this change does. Gating it is a UX decision
  about the dashboard, not a ledger-integrity one, and deserves its own
  change; the write-attribution and audit-thread fixes here do not depend on
  it.

## Testing

`crates/server/tests/jobs_auth_http.rs`:

- Every route in one table-driven list 401s without a session, and 401s with
  a bogus bearer token. Driving the list rather than five separate tests
  means a route added to `configure_routes` without a gate shows up as a
  missing entry rather than as silently uncovered surface.
- The rejection arrives in `JobErrorResponse`'s envelope
  (`success`/`error_type`/`timestamp`), not `/query`'s.
- Auth precedes both the unknown-job 404 and the jobs-disabled 503.
- A no-auth server still serves `GET /jobs`, `POST /jobs/:name/run` (with a
  real run created) and `GET /jobs/runs`.

Mutation-checked: removing the gate from any single handler fails the
table-driven tests.
