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
[pipelines.md](pipelines.md) and [jobs.md](jobs.md). For the broader
story, see [agent_data_plane.md](agent_data_plane.md).

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
| `max_rows`, `statement_kind` | request shape |
| `status` | `started` → `succeeded` / `failed`, or `unknown` after a crash |
| `row_count`, `error` | outcome detail |

Indexed on `(session_id, created_at)`, `created_at` and `status`, so an agent
session can be reconstructed with plain SQL.

Failure semantics are explicit:

- **Startup.** Opening or migrating the ledger is fatal — a server configured
  to audit never runs silently unaudited.
- **Before execution.** If the pre-execution write fails, the request is
  rejected with `503 query_audit_error` and the statement is **not executed**.
- **After execution.** A failed outcome update is logged only; the query
  already ran. The row stays `started` and the next startup reconciles it to
  `unknown`, as it does for statements killed mid-flight.

Retention is opt-in: `--query-audit-retention-days <n>` deletes records older
than `n` days at startup and hourly thereafter. Without it, records are kept
forever and pruning is the operator's call.

The ledger holds raw SQL, so it is created owner-only (`0600` on Unix,
including the WAL sidecars). It is a local database, never the OTLP/tracing
pipeline, so enabling it does not push query text to external collectors.

#### Pipeline executions in the ledger

When `--query-audit-db` is configured, `POST /:pipeline/execute` is audited
with the same record-before-execute and fail-closed semantics as `/query`
(a failed pre-execution write returns 503 and the pipeline does not run).
A pipeline row differs from an ad-hoc row in four ways:

- `statement_kind` is `pipeline`, and the `sql` column holds the pipeline
  *name*, not SQL — the template lives on disk, and the pipeline's
  `description` carries its purpose.
- Parameter values are never recorded: params are where PII lives.
  `ai_context` is always NULL on pipeline rows.
- `max_rows` is stored as `0` (not applicable to pipelines).
- On failure, `error` holds a fixed kind (`query_execution_error` or
  `result_conversion_error`), never engine error text — engine errors can
  echo substituted parameter values back, and those must not reach the
  ledger. The full error still goes to the HTTP caller.

`session_id` comes from the optional `X-Skardi-Session-Id` request header
(non-empty, ≤ 200 characters). A malformed header is rejected with `400
parameter_validation_error` rather than silently dropped. With the header
present, one agent session's ad-hoc queries and pipeline calls interleave
under a single `session_id` in the ledger, ordered by `created_at`.

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
- **[Why an agent data plane](agent_data_plane.md)** — why the data plane is shaped this way.
