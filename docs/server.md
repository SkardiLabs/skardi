# Skardi Server

`skardi-server` is the HTTP process that hosts two peer surfaces on one
engine:

- **Online serving — [pipelines](pipelines.md).** Parameterized SQL
  served synchronously as REST endpoints
- **Offline [jobs](jobs.md).** The same SQL shape run asynchronously into
  a durable destination , with a run ledger and
  atomic commit.

Both surfaces share the same context file (data sources + access mode +
caching), the same YAML envelope, and the same HTTP listener. This page covers the
shared server concerns; the per-surface reference lives in
[pipelines.md](pipelines.md) and [jobs.md](jobs.md).

---

## Running the server

```bash
cargo run --bin skardi-server -- \
  --ctx <path-to-ctx.yaml> \
  --pipeline <pipeline-file-or-directory> \
  --jobs <job-file-or-directory> \
  --jobs-db <path-to-jobs.db> \
  --semantics <semantics-file-or-directory> \
  --port 8080
```

| Flag | Description |
|------|-------------|
| `--ctx` | Context YAML defining data sources (required). |
| `--pipeline` | Pipeline YAML file or directory of pipeline files. When omitted, `POST /:name/execute` and `/pipelines` return empty. |
| `--jobs` | Job YAML file or directory. When omitted, every `/jobs/*` endpoint returns `503` with `error_type: jobs_disabled`. |
| `--jobs-db` | SQLite run ledger for jobs. Default: `~/.skardi/jobs.db` (parent dirs created on first use). |
| `--semantics` | `kind: semantics` YAML file or directory. Attaches NL descriptions to tables / columns on `GET /data_source`. **Auto-discovered** from `<ctx_dir>/semantics/` or `<ctx_dir>/semantics.yaml` when omitted. See [semantics.md](semantics.md). |
| `--port` | Port to listen on. Default: `8080`. |

On startup the server:

1. Loads the context file and registers every data source.
2. Loads pipeline and job files; rejects any YAML missing the correct
   `kind:` at the root.
3. Opens (creating if needed) the SQLite jobs ledger and reconciles
   orphan runs — any row left in `pending` or `running` by a previous
   crash is rewritten to `failed` with the message `"server restarted
   before run completed"`.
4. Binds the HTTP listener.

---

## Dashboard

Once the server is running, open `http://localhost:8080` in a browser to
access the built-in dashboard. The dashboard has three tabs covering the
three primitives the server exposes — **Pipelines**, **Jobs**, and
**Semantics** — and a shared filter input scoped to the active tab.

**Pipelines tab.** One card per registered pipeline, with:

- **Endpoint URL** — the `POST` path to call, with a one-click copy button.
- **Parameters** — names and inferred types extracted from the pipeline SQL.
- **Example request** — a ready-to-run `curl` command.
- **Try It** — an interactive panel to edit the JSON body and execute the
  pipeline from the browser.

**Jobs tab.** One card per registered job:

- **Endpoint URL** — `POST /jobs/<name>/run`.
- **Destination** — table, write mode, `create_if_missing`, and the
  optional `timeout_ms` from the job YAML.
- **Parameters** — names and inferred types from the job SQL.
- **Example request** — `curl` for a fresh submit.
- **Submit Run** — interactive panel that submits to the run endpoint and
  shows the response (`{ run_id, status }` on success, the structured
  error body on failure).
- **Recent Runs** — the five most recent runs for this job (id, status,
  relative time), refreshed automatically after a submit and via a
  manual *Refresh* button.

When the server was started without `--jobs`, this tab shows an empty
state pointing back to the flag. When `--jobs` was given but no job YAML
loaded, it shows "No jobs registered."

**Semantics tab.** One card per registered data source, with:

- **Source name and type** (e.g. `csv`, `postgres`, `lance`).
- **Table description** — merged from the `kind: semantics` overlay first,
  ctx-inline `description` second, "No description provided." otherwise.
- **Columns** — every column on the registered table with its Arrow type
  and the column-level description from the semantics overlay (or
  "No description.").

No configuration required — the dashboard is built into `skardi-server`
and updates automatically when pipelines, jobs, or semantics reload.

---

## API endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Pipeline dashboard UI. |
| `/health` | GET | Service health check. |
| `/data_source` | GET | List all registered data sources. |
| `/pipelines` | GET | List all registered pipelines. |
| `/pipeline/:name` | GET | Metadata for one pipeline. |
| `/health/:name` | GET | Per-pipeline health check (includes upstream data-source status). |
| `/:name/execute` | POST | Execute a pipeline by name. Body is the JSON param map. See [pipelines.md](pipelines.md). |
| `/query` | POST | Execute one ad-hoc SQL statement. Body: `{ "sql": "...", "max_rows": 1000, "ai_context": { "purpose": "...", "session_id": "..." } }`. See [§ Ad-hoc queries](#ad-hoc-queries). |
| `/jobs` | GET | List all registered jobs with destinations. |
| `/jobs/:name/run` | POST | Submit a new job run. Body is the JSON param map. See [jobs.md](jobs.md). |
| `/jobs/runs` | GET | List recent runs; supports `?job=<name>&limit=N`. |
| `/jobs/runs/:run_id` | GET | Current state of one run. |
| `/jobs/runs/:run_id/cancel` | POST | Flag a run for cancellation. |

Request / response bodies for pipeline execution are documented in
[pipelines.md § Response format](pipelines.md#response-format); job run
submission and the run lifecycle are documented in
[jobs.md § HTTP endpoints](jobs.md#http-endpoints).

### Ad-hoc queries

`POST /query` runs a single SQL statement against the registered data sources.
DDL/COPY are always rejected; DML is allowed only against `access_mode:
read_write` sources. Results are capped at `max_rows` (default 1000) and the
response carries a `truncated` flag.

Request fields:

- `sql` (string, required) — one SQL statement.
- `max_rows` (positive integer, optional, default 1000) — result row cap.
- `ai_context` (object, optional) — agent-supplied context describing and
  grouping the query. Application/console queries omit it. When present it must
  be a JSON object carrying two required non-empty strings — `purpose`
  (≤ 2000 chars, why the query runs) and `session_id` (≤ 200 chars, groups
  queries from one agent session) — plus any free-form keys of the caller's
  choosing. The whole object must serialize to ≤ 4096 bytes. Recorded for
  observability; never executed. Any violation → `400
  parameter_validation_error`. Omitting the field is valid; sending
  `"ai_context": null` is *not* — an explicit null is a present-but-malformed
  value and is rejected like any other non-object.

#### Query confidentiality

Callers may inline literal secrets or PII into `sql`, so by default **no query
text or literal value reaches the log/trace/OTLP stream.** Two things enforce
that:

- The handler's INFO audit marker is value-free: it carries `ai_context`,
  `max_rows`, statement kind and timing, never the statement.
- The tracing filter pins every plan-printing target (`datafusion*`,
  `sqlparser`, and the server's own `skardi_query_plan` instrumentation spans)
  to INFO, and drops any `RUST_LOG` directive that would lower them.
  Suppressing the handler's `sql` field alone is not enough — DataFusion's
  analyzer/optimizer reprint the same literals inside the plans they log at
  DEBUG (`Projection: Utf8("…")`), and those records fan out to every
  configured collector.

  An operator debugging a planning problem on non-sensitive data can lift the
  floor with `SKARDI_ALLOW_PLAN_VALUE_LOGGING=1`. It is a separate, explicit
  opt-in because it re-enables value export.

#### Query audit ledger

`--query-audit-db <path>` turns on a durable, queryable record of what ran.
Off by default; when off, raw SQL is never persisted anywhere.

Each accepted statement is committed to a SQLite `query_audit` table **before**
execution and updated with its outcome afterwards:

| column | notes |
| --- | --- |
| `id` | stable record id |
| `created_at` / `finished_at` | RFC 3339 |
| `sql` | raw statement text |
| `ai_context` | the caller's object, verbatim JSON |
| `session_id` | denormalised from `ai_context` for indexed session lookup |
| `max_rows` | requested row cap (`0` on pipeline and job rows — not applicable) |
| `job_run_id` | `job_runs.id` bridge, job rows only; NULL if the outcome stamp was lost — see `job_runs.submission_id` for the durable reverse pointer. Distinct from the identity envelope's `run_id`, which names the *caller's* run — one column, one meaning. |
| `request_id`, `org_id`, `workspace_id`, `user_id`, `run_id` | caller-identity envelope; all NULL on this server, filled by distributions that authenticate their callers |
| `statement_kind` | `query` or `other` for ad-hoc rows (the server's statement *classification* — not SQL verbs like `select`/`dml`), `pipeline` for pipeline rows, `job` for job rows. Consumers filtering the ledger must match these exact strings. |
| `status` | `started` → `succeeded` / `failed`, or `unknown` after a crash |
| `row_count`, `error` | outcome detail |

Indexed on `(session_id, created_at)`, `created_at`, `status` and
`statement_kind`, so an agent session can be reconstructed — and a single row
kind selected — with plain SQL. `job_run_id` carries its own partial index
(`WHERE job_run_id IS NOT NULL`) so the reverse lookup — given a run id from
`GET /jobs/runs`, which session submitted it — does not scan a table that is
append-only and has retention off by default.

What is **not** here: job runs. `POST /jobs/:name/run` executes its own SQL
through the same engine but is recorded in the separate jobs run ledger (see
[jobs.md](jobs.md)), not in `query_audit`. An operator reasoning about
coverage during an incident should read this ledger as "ad-hoc queries and
pipeline executions", not "everything the server ran".

Failure semantics are explicit:

- **Startup.** Opening or migrating the ledger is fatal — a server configured
  to audit never runs silently unaudited.
- **Before execution.** If the pre-execution write fails, the request is
  rejected with `503 query_audit_error` and the statement is **not executed**.
  Each write is bounded (5s), so a *hung* fsync fails the request rather than
  hanging it. Because an abandoned write may still land on the writer thread
  afterwards, a timed-out pre-execution write is followed by a corrective
  update marking that row `failed` with `error = audit_write_timeout` — the
  server knows the statement did not run, and the ledger says so rather than
  degrading to the ambiguous `unknown`.
- **After execution.** A failed outcome update is logged only; the query
  already ran. The row stays `started` and the next startup reconciles it to
  `unknown`, as it does for statements killed mid-flight.

Retention is opt-in: `--query-audit-retention-days <n>` deletes records older
than `n` days at startup and hourly thereafter. Without it, records are kept
forever and pruning is the operator's call. The prune deletes in batches,
yielding between them: it shares the ledger's single writer thread with the
fail-closed write path, so an unchunked delete over a large backlog would
starve concurrent requests into `503 query_audit_error`. Enabling retention
for the first time on a big ledger is therefore slow, not disruptive. Each
batch is bounded like any other write, so on pathologically slow storage the
*startup* prune can fail and abort startup — the same fail-closed stance
that makes a broken ledger fatal rather than silently skipped. Later hourly
prunes only warn.

The ledger holds raw SQL, so it is created owner-only (`0600` on Unix,
including the WAL sidecars). It is a local database, never the OTLP/tracing
pipeline, so enabling it does not push query text to external collectors.

#### Pipeline executions in the ledger

When `--query-audit-db` is configured, `POST /:pipeline/execute` is audited
with the same record-before-execute and fail-closed semantics as `/query`
(a failed pre-execution write returns 503 and the pipeline does not run).
A pipeline row differs from an ad-hoc row in four ways:

- `statement_kind` is `pipeline`, and the `sql` column holds
  `name@version` (from `metadata.version`), not SQL — the versioned
  template lives on disk, and the pipeline's `description` carries its
  purpose. The version matters because pipelines are exactly the artifacts
  the promotion loop edits, and rows are kept forever by default: without
  it, "what SQL ran" stops being answerable once a template is revised.

  **Parse rule:** split on the *last* `@` (`rsplit_once('@')`). Pipeline
  names are not charset-restricted, so a name may itself contain `@`
  (`billing@eu@1.0.0` is the pipeline `billing@eu` at version `1.0.0`). A
  trailing `@` with nothing after it means the pipeline declared an empty
  `metadata.version` — a config smell in the pipeline, not a truncated row.

  The version pins the **template**, not the exact statement: parameter
  substitution is textual, so two executions of `weekly-churn@1.0.0` run
  the same SQL skeleton with different literals. Values are deliberately
  not recorded (see the confidentiality note above), so the ledger
  identifies the artifact and its revision, not the byte-exact statement.
- Parameter values are never recorded: params are where PII lives.
  `ai_context` is always NULL on pipeline rows.
- `max_rows` is stored as `0` (not applicable to pipelines).
- On failure, `error` holds a fixed kind (`query_execution_error` or
  `result_conversion_error`), never engine error text — engine errors can
  echo substituted parameter values back, and those must not reach the
  ledger. The full error still goes to the HTTP caller.

Scope of the guarantee: "parameter values never reach the ledger" covers the
ledger only. Two of the four surfaces that used to carry parameter values are
now closed — the `ERROR`-level unsupported-parameter log (on by default,
fanning out to any configured OTLP collector) and the HTTP `400` body's
`unsupported_parameters` list both name the parameter and its JSON *kind*
now, never its contents. Two remain, tracked in
[#217](https://github.com/SkardiLabs/skardi/issues/217): a `DEBUG`-level log
of the substituted SQL (needs an operator to raise `RUST_LOG`, then egresses
to the trace sink), and HTTP `500` bodies, where engine error text can quote
a value back to the caller who sent it — `/query` redacts both to "see
server logs"; the pipeline endpoint does not yet. Do not read the ledger's
redaction as implying the logs are fully covered.

Sizing note: with auditing on, every pipeline execution adds two
`synchronous = FULL` ledger writes on the request's critical path — and the
write is fail-closed, so a failing audit disk turns into `503
query_audit_error` rather than dropped records (each write is bounded by a
5-second timeout, so a *hung* fsync is treated as a failed one instead of
hanging the request). All ledger writes — `/query`'s included — funnel
through one serialized writer thread, so this is a ceiling on total audited
throughput, not just a per-request latency adder. Pipelines are typically
the higher-QPS path (they are the promoted recurring queries), so put the
ledger on storage you'd trust under your serving load.

`session_id` comes from the optional `X-Skardi-Session-Id` request header
(non-empty, ≤ 200 characters, visible ASCII with no spaces, tabs or commas).
A malformed header is rejected with `400 parameter_validation_error` — carrying
`details.header` so an agent can tell a header reject from a parameter reject
— rather than silently dropped. With the header present, one agent session's
ad-hoc queries and pipeline calls interleave under a single `session_id` in
the ledger, ordered by `created_at`.

**`session_id` is caller-asserted, not authenticated.** It groups executions;
it does not attest to their origin. Any caller may stamp its executions with
another agent's session id, or omit the header and land unattributed. Treat it
as a correlation key when reading the ledger, never as evidence of who ran
what. (Spaces are rejected precisely because HTTP intermediaries may trim
surrounding whitespace, which would silently re-key an execution.)

Reachability caveat: producing that interleaving from the shipped CLI
requires both halves, and today only `skardi run --session-id` exists —
`skardi query` cannot send `ai_context` yet
([#218](https://github.com/SkardiLabs/skardi/issues/218)), so the ad-hoc
half of a CLI session lands unattributed until then. Direct HTTP callers
get the full interleaving today.

#### Job submissions in the ledger

`POST /jobs/:name/run` is audited as a *submission event*: the row's
lifecycle is the submission's, not the run's. `statement_kind` is `job`,
`sql` holds `name@version`, and on acceptance the row is stamped
`succeeded` with the `job_run_id` that bridges to the jobs ledger — which
remains the authority on the run itself (parameters, progress, outcome).
A rejected submission is stamped `failed` with the executor's fixed error
category, never its message text. Unlike pipelines, a job's parameter
validation happens inside the executor — after the audit write — so a
parameter rejection leaves a `failed` row rather than recording nothing.
Record-before-submit and the fail-closed `503 query_audit_error` behave
exactly as for pipelines: a job the ledger cannot account for is not
submitted. The same `X-Skardi-Session-Id` header (same validation)
attributes the submission, so `list_by_session` returns an agent session's
ad-hoc queries, pipeline calls, and job submissions in one ordered read.

Two properties of this seam an operator should know before relying on it:

- **Attribution is authenticated but still self-reported.** Every `/jobs/*`
  endpoint calls `require_session` first, so the ledger has no
  unauthenticated write path — an anonymous caller can no longer mint
  `session_id` values into `query_audit`, nor queue writes onto its single
  serialized writer thread. The header itself is unchanged: it names a
  session, it does not prove one, exactly as on `/query` and
  `POST /:pipeline/execute`. An authenticated caller can still stamp its
  submissions with another session's id. Read `session_id` as a correlation
  key, never as evidence of who ran what.

- **The `job_run_id` bridge is best-effort, but the correlation is not.**
  `query_audit.job_run_id` is stamped *after* `executor.submit` returns, so if
  that write fails, times out, or the process dies in the window, the audit
  row keeps `job_run_id = NULL` and reconciles to `unknown`. The correlation
  survives anyway: `job_runs.submission_id` holds the audit row's id, written
  in the same INSERT that creates the run, so it is durable the moment the
  run exists. Read the pair as one bridge with a fast half and a reliable
  half — `job_run_id` for the common lookup, `submission_id` when it is NULL.
  Both directions are indexed.

  `submission_id` is NULL for runs submitted to a server with no
  `--query-audit-db`. The jobs subsystem stores it verbatim and never
  interprets it — it has no notion of the audit ledger.

- **The repair happens on the next boot; you do not need the token by hand.**
  At startup, once both ledgers are open and each has reconciled its own
  orphans, the server passes over job rows left `unknown` with
  `job_run_id IS NULL` and re-links each from `job_runs.submission_id`. The row
  an auditor reads ends up carrying the run id, so "linkage lost" is a
  condition that survives a crash but not a restart. `status` stays `unknown` —
  the outcome genuinely was never observed; only the linkage is recovered. The
  pass is idempotent, never overwrites a pointer that was written correctly,
  and never touches a row still `started`. A failure is logged and does not
  block startup.

- **The token resolves through the runs API as a filter, not as a field.**
  `GET /jobs/runs?submission_id=<audit row id>` returns the single matching run
  (an empty list on a miss). It is deliberately *not* included in run payloads:
  it is a `query_audit` primary key, that ledger is chmod 0600, and
  `GET /jobs/runs` returns every run to any authenticated session — the
  `/jobs/*` gate is authentication, not authorization — so emitting it would
  publish one caller's audit-row id to every other caller. A filter on the way
  in also serves the operator better: it resolves a run that has already
  fallen off the 500-row list window, which during an incident is exactly the
  run being looked for.

- **The two halves expire on different clocks.** `query_audit` has retention
  (`--query-audit-retention-days`, pruned at startup and hourly); `job_runs`
  has no pruning at all. So under retention an old run keeps a `submission_id`
  pointing at a row that has been deleted, indistinguishable from a token that
  was never valid. The pointer stays durable; its target does not.

  The asymmetry cuts the useful way too. Because `job_runs` is never pruned,
  **no match for a token is positive evidence that `submit` never created a
  run** — which is what separates "definitely submitted, linkage lost" from
  "never ran" for an `unknown` job row. That inference is the operator
  procedure for the ambiguity this bridge exists to remove, and it stops being
  sound the moment anything starts pruning `job_runs`.

- **`jobs.db` is protected on the same terms as the audit ledger.** It is
  created owner-only (0600) and re-chmodded on every open, along with its
  WAL sidecars, matching `--query-audit-db`. It has to be: `job_runs.parameters`
  holds the raw submit-time parameter *values* the audit ledger deliberately
  refuses to store, and since `submission_id` the file also links each run to a
  protected audit row. Two halves of one audit record with one permission
  decision — otherwise the weaker half sets the real posture. Off Unix the
  chmod is a no-op, as with the audit ledger.

---

## Context files

A context file (`ctx.yaml`) defines the data sources available to both
pipelines and jobs. Each data source is registered as a table (or
catalog) in the query engine, and the same registration serves both
surfaces — a pipeline's `SELECT` and a job's `INSERT` target the same
logical names.

```yaml
kind: context

metadata:
  name: products-ctx

spec:
  data_sources:
    - name: "products"          # Table name used in SQL queries
      type: "csv"               # Data source type
      path: "data/products.csv" # File path or connection string
      options:                  # Type-specific options
        has_header: true
        delimiter: ","
        schema_infer_max_records: 1000
      description: "Product catalog"
```

A single context can mix source types:

```yaml
kind: context

metadata:
  name: mixed-ctx

spec:
  data_sources:
    - name: "users"
      type: "postgres"
      connection_string: "postgresql://localhost:5432/mydb?sslmode=disable"
      options:
        table: "users"
        schema: "public"
        user_env: "PG_USER"
        pass_env: "PG_PASSWORD"

    - name: "orders"
      type: "csv"
      path: "docs/sample_data/orders.csv"
      options:
        has_header: true
        delimiter: ","
```

### Access mode

By default, every data source is **read-only** — only `SELECT` queries
are allowed. To enable write operations (`INSERT`, `UPDATE`, `DELETE` —
used by job destinations with `kind: sql` and by write-through
pipelines), set `access_mode: read_write` on the data source.

Only `postgres`, `mysql`, `sqlite`, `mongo`, and `redis` sources support
`read_write`; setting it on other types fails at startup.

```yaml
spec:
  data_sources:
    - name: "users"
      type: "postgres"
      connection_string: "postgresql://localhost:5432/mydb?sslmode=disable"
      access_mode: read_write    # Enable INSERT / UPDATE / DELETE
      options:
        table: "users"
        user_env: "PG_USER"
        pass_env: "PG_PASSWORD"

    - name: "products"
      type: "csv"
      path: "data/products.csv"
      # access_mode defaults to read_only (CSV has no write path)
```

A pipeline or job that attempts a write on a `read_only` source is
rejected before execution:

```
Write operation not allowed on data source 'products'. The data source is
configured with 'read_only' access mode.
```

### In-memory caching

For file-based sources (`csv`, `parquet`, `iceberg`), set
`enable_cache: true` to load the entire dataset into memory at startup —
significantly faster repeated queries at the cost of RSS.

```yaml
spec:
  data_sources:
    - name: "products"
      type: "csv"
      path: "data/products.csv"
      enable_cache: true          # Load into memory at startup
      options:
        has_header: true
```

The cache is built once at startup and reused for every subsequent query
on that source, from pipelines and jobs alike.

---

## Next

- **[Pipelines](pipelines.md)** — YAML shape, parameters, invocation, and response format for the online-serving side.
- **[Jobs](jobs.md)** — YAML shape, destinations, run ledger, and cancellation for the offline-batch side.
- **[Semantics](semantics.md)** — natural-language descriptions on tables and columns; the agent-facing catalog overlay.
- **[CLI](cli.md)** — `skardi run`, aliases, federated SQL from the shell.
