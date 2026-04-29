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
access the built-in dashboard. Today it covers pipelines — each
registered pipeline is shown as a card with:

- **Endpoint URL** — the `POST` path to call, with a one-click copy button.
- **Parameters** — names and inferred types extracted from the pipeline SQL.
- **Example request** — a ready-to-run `curl` command.
- **Try It** — an interactive panel to edit the JSON body and execute the
  pipeline from the browser.

No configuration required — the dashboard is built into `skardi-server`
and updates automatically when pipelines reload. A job-side dashboard
view (recent runs, submit / poll / cancel) is on the roadmap.

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
| `/jobs` | GET | List all registered jobs with destinations. |
| `/jobs/:name/run` | POST | Submit a new job run. Body is the JSON param map. See [jobs.md](jobs.md). |
| `/jobs/runs` | GET | List recent runs; supports `?job=<name>&limit=N`. |
| `/jobs/runs/:run_id` | GET | Current state of one run. |
| `/jobs/runs/:run_id/cancel` | POST | Flag a run for cancellation. |

Request / response bodies for pipeline execution are documented in
[pipelines.md § Response format](pipelines.md#response-format); job run
submission and the run lifecycle are documented in
[jobs.md § HTTP endpoints](jobs.md#http-endpoints).

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
