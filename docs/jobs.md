# Offline Jobs (`kind: job`)

Offline jobs in Skardi are parameterized SQL queries whose rows are written
to a durable destination — a Lance dataset on disk, or a read-write database
table your pipelines already know how to read from. They're the offline,
async peer of online-serving pipelines: where a pipeline is a synchronous
HTTP request / response (the read path agents hit at tool-call time), a job
runs in the background, persists a run row to a SQLite ledger, and is
polled by run id (the write path agents use to commit durable results).

**In one sentence:** a pipeline answers a query; a job commits the answer
somewhere you can query again later.

### Why this is a first-class primitive, not just "a long-running pipeline"

Jobs are the write path for agent autonomy. An agent that can only answer
queries is a search engine; an agent that can commit durable, queryable
results to the lake is a data engineer. The part that separates the two —
atomic commits, run ledger, submit-time schema diff, crash recovery,
cancellation — is exactly what makes agent-initiated writes *safe* without
requiring a human in the approval loop. See
[docs/spark_for_agents.md § Trust the agent, but make writes safe](spark_for_agents.md#4-trust-the-agent-but-make-writes-safe)
for the design rationale.

---

## When to use a job

- **Backfilling a lake from a federated source.** Your wiki lives in
  Postgres; you want a queryable snapshot in Lance for BI.
- **Nightly ingest.** Pull the last 24h of rows into a destination table.
- **Long-running transforms.** Anything whose wall-clock time makes an
  HTTP request/response awkward.

Use a pipeline (not a job) when the caller needs the rows back in the same
HTTP response.

---

## Writing a job YAML

A job YAML has the same `metadata:` + `query:` blocks as a pipeline, plus
two new sections and a `kind: job` discriminator at the root:

```yaml
kind: job

metadata:
  name: "backfill-wiki-range"
  version: "1.0.0"
  description: "Backfill wiki_pages into the lake for a given date range."

# {placeholder} tokens are inferred as typed scalar params, exactly the
# way pipelines work. No separate `parameters:` block.

spec:
  query: |
    SELECT slug, title, page_type, content, updated_at
    FROM wiki.main.wiki_pages
    WHERE updated_at >= {from_date}
      AND updated_at <  {to_date}
  destination:
    table: "wiki_lake"          # DataFusion table identifier — bare or dotted
    mode: append                # append is the only supported mode in MVP
    create_if_missing: true     # lake destinations only (see below)
  execution:
    timeout_ms: 3600000         # optional wall-clock cap; default = no timeout
```

### `kind:`

Every resource YAML carries a `kind:` discriminator at the root. `kind: job`
tells the loader to treat the file as a job; pipeline YAMLs set
`kind: pipeline`, contexts set `kind: context`, and alias files set
`kind: aliases`. A server started with `--jobs <dir>` scans every
`.yaml` / `.yml` file in the directory and silently skips files that are
not `kind: job`, so it's safe to intermix pipelines and jobs on disk.

### `destination.table`

Any DataFusion table identifier. Bare names (`wiki_lake`) resolve the
same way as a `FROM` clause does; dotted names (`wikidb.public.wiki_log`)
reach into catalog-registered sources. The **first dotted segment** is
taken as the data source name — the executor uses it to decide whether
the destination is a lake (Lance) or a transactional SQL DB
(**Postgres / MySQL / SQLite**).

Non-transactional backends (Redis, MongoDB, SeekDB) are **rejected at
submit time** with `error_type: non_transactional_destination`. Those
providers' write paths do not wrap an INSERT in a transaction, so a
mid-run failure could leave partial rows visible — which violates the
atomicity contract every other destination honors. They remain fine as
data *sources* in pipelines and jobs; they just cannot be the
destination of a job.

### `destination.mode`

- `append` (default, and the only mode in MVP) — add rows to the destination.
  - *Lake:* the dataset is created if `create_if_missing: true` and it
    does not yet exist, else appended to.
  - *DB:* rows are added with `INSERT INTO <table> SELECT ...`, wrapped
    in one transaction per run.

**Overwrite is deliberately not supported.** Overwriting a DB destination
would need `DELETE FROM` + `INSERT` to share one transaction, and the
DataFusion SQL surface we drive DB writes through does not expose a
multi-statement transaction handle — so a mid-run INSERT failure after a
successful DELETE would silently leave the table empty. Rather than ship
a version of overwrite that is atomic for Lance but not for DB
destinations, MVP rejects overwrite at YAML load time across the board.
Upserts / merge are out of scope for the same reason.

Workarounds while overwrite is out:

- *Lake:* delete the dataset directory and re-run `append` with
  `create_if_missing: true`.
- *DB:* write to a staging table with `append`, then swap with your
  database's native `RENAME TABLE` (or equivalent) in one DDL statement.

### `destination.create_if_missing`

- **Lake destinations** (Lance today, Iceberg / Delta later) — first run
  creates the dataset with the query's output schema. Defaults to `true`.
  Set `false` when you want a submit to fail if the dataset is missing —
  useful for guarded production jobs.
- **DB destinations** — always ignored. DDL for federated DB engines is
  its own subsystem and permanently out of scope for jobs; create the
  table out-of-band with your DB's own DDL tooling.

### `execution.timeout_ms`

Wall-clock cap. If the query + write together exceed this, the task is
aborted and the row is marked `failed` with a timeout message. On lake
destinations the Lance manifest is never committed; on DB destinations
the wrapping transaction is rolled back. Either way, the destination is
left at its pre-job state.

### Parameter placeholders

Same rules as pipelines: `{name}` binds to a typed scalar value at submit
time. Values substitute as SQL literals (strings are single-quoted and
escaped, numbers/booleans/null emit raw). `{name}` never substitutes into
identifier positions — the query shape is fixed at load time, only values
move.

`${name}` (dollar-brace) is reserved for v1.1 executor-resolved variables
like `${watermark}` and `${last_successful_run.finished_at}`.

---

## Running the server with jobs enabled

```bash
cargo run --bin skardi-server -- \
  --ctx demo/llm_wiki/cli/ctx.yaml \
  --pipeline demo/llm_wiki/cli/pipelines \
  --jobs demo/llm_wiki/cli/jobs \
  --port 8080
```

| Flag | Description |
|------|-------------|
| `--jobs <path>` | File or directory of job YAMLs. When omitted, `/jobs/*` endpoints return `503`. |
| `--jobs-db <path>` | SQLite run ledger. Default: `~/.skardi/jobs.db` (parent dirs created on first use). |

On startup the server:

1. Opens (creating if needed) the SQLite ledger.
2. **Reconciles orphans** — any row left in `pending` or `running` by a
   previous crash is rewritten to `failed` with the message `"server
   restarted before run completed"`. This is the only time the server
   touches a run row it didn't create this process.
3. Loads `--jobs` and registers the destination types.

### HTTP endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/jobs` | GET | List every registered job with its destination |
| `/jobs/:name/run` | POST | Submit a new run; body is the param map |
| `/jobs/runs` | GET | List recent runs; supports `?job=<name>&limit=N` |
| `/jobs/runs/:run_id` | GET | Current state of one run |
| `/jobs/runs/:run_id/cancel` | POST | Flag a run for cancellation |

When the server was started without `--jobs`, every endpoint above
returns `503 Service Unavailable` with `error_type: jobs_disabled`.

---

## Submitting a run from the CLI

The CLI ships a matching `skardi job` subcommand that POSTs to the
server. The server URL defaults to `http://127.0.0.1:8080`; override with
`$SKARDI_SERVER_URL` or the `--server` flag.

```bash
# Submit — returns the run_id
skardi job run wiki-backfill-to-lake \
    --param slug_prefix='entity/%' \
    --param limit:int=1000

# Poll the run
skardi job status <run_id>

# List recent runs
skardi job list --job wiki-backfill-to-lake --limit 20

# Cancel an in-flight run — the executor's cancel flag is read by a
# stream adapter wrapping the query output, so the next batch poll
# errors out and the destination's in-flight transaction / Lance
# manifest commit unwinds cleanly with no rows visible. Runs that
# already committed before cancel was observed report cancelled: false.
skardi job cancel <run_id>

# Discover all registered jobs and their destinations
skardi job show
```

`--param` uses the exact same typing rules as `skardi run`:

- `name=42` → int
- `name=3.14` → float
- `name=true` → bool
- `name=null` → SQL NULL
- `name=hello` → string (no quotes needed)
- `name:str=42` → force string (useful for numeric-looking strings)

Jobs run **only inside the server** — there is no in-process fallback
and no `--ctx` flag on the CLI-side job commands. If the server isn't
running you'll get a connection-refused error.

---

## Submit-time validation (the "pre-flight")

Every submit runs three checks *before* creating a run row, so a
malformed submit never pollutes the ledger:

1. **Parameter presence** — every `{placeholder}` in the SQL must have a
   bound value. Unmapped params → 400 with the list of missing names.
2. **Parameter type** — only strings, numbers, booleans, and null are
   accepted (same as the pipeline handler). Arrays and objects → 400.
3. **Destination diff** — the executor plans the SELECT, takes its
   output schema, and compares it against the destination:
   - *Destination exists* → order-insensitive column diff. Extra columns
     in the query, mismatched Arrow types, or non-nullable destination
     columns not produced by the query all reject the submit.
   - *Destination missing, DB* → always reject with
     `"destination table '<name>' does not exist; create it with your
     DB's DDL before running the job"`.
   - *Destination missing, lake, `create_if_missing: true`* → accept;
     schema is realized on first write.
   - *Destination missing, lake, `create_if_missing: false`* → reject.

On rejection the HTTP response is `400` with an `error_type` suitable
for agent handling:

| `error_type` | Meaning |
|--------------|---------|
| `unknown_job` | `/jobs/:name/run` targets a name the server doesn't know |
| `missing_parameters` | One or more `{placeholders}` not bound |
| `unsupported_parameter` | Bound value is an array / object |
| `destination_missing` | DB table doesn't exist; or lake + `create_if_missing: false` |
| `non_transactional_destination` | Destination source type is Redis / MongoDB / SeekDB — rejected because its write path cannot guarantee atomicity |
| `schema_mismatch` | Column diff — `details.diff` carries a human-readable string |
| `sql_plan_failure` | DataFusion rejected the rendered SQL |

---

## Execution and sizing

Jobs stream the query output through the destination rather than
buffering it in the server process. The DataFusion result is consumed
batch-by-batch; peak memory is proportional to the in-flight batches
(~2 at a time on the skardi side), not the total row count. This means
the server itself is not a bottleneck for job size.

What *is* a bottleneck depends on the destination:

- **Lance destinations** have no practical size limit. Lance is the
  scale-out storage format; multi-TB datasets are routine. Commit
  semantics are the same regardless of size: data files are written
  as the stream flows, the manifest is committed once at the end, and
  a mid-run failure leaves no visible trace.

- **SQL DML destinations** write inside one wrapping transaction. This
  gives you atomicity, but the destination database pays for it: a
  huge INSERT holds write locks for the whole run, grows the WAL /
  undo log in proportion to the insert size, and can lag replication.
  As a soft guideline, keep SQL DML jobs to roughly **10M rows or
  10GB**. The sink logs a `tracing::warn!` the first time a run
  crosses that threshold — not an error, just a signpost.

  For larger one-shot ingests, use your database's native bulk
  loader (`COPY FROM` on Postgres, `LOAD DATA LOCAL INFILE` on MySQL)
  from outside skardi. For continuous replication, use a CDC tool
  (Debezium, Fivetran, Airbyte). Both are future integration points
  that live outside the jobs primitive — jobs are for the sweet spot
  of "too big for a synchronous pipeline, small enough that one
  atomic transaction is still sane."

---

## The run ledger

SQLite file (default `~/.skardi/jobs.db`) with a single `job_runs`
table. Every submit appends a row; every lifecycle transition updates it.
One process writes (the server); reads and writes are serialized through
a single connection, so lookups from the CLI are always consistent with
what the background task last persisted.

Row fields, matching the CLI `status` response:

| Field | Notes |
|-------|-------|
| `run_id` | UUID v4, hex-only (no dashes) — the id in the HTTP response and the CLI |
| `job` | Job name from `metadata.name` |
| `status` | `pending` → `running` → terminal (`succeeded` \| `failed` \| `cancelled`) |
| `parameters` | JSON of the bound values |
| `created_at` / `started_at` / `finished_at` | ISO-8601 timestamps |
| `rows_written` | Set on `succeeded`; also set on post-commit cancels |
| `snapshot_id` | For Lance: the version the commit landed on, as a string |
| `error` | Non-null on failures / cancels; free-form message |

---

## Atomicity and failure modes

Every supported destination — Lance, Postgres, MySQL, SQLite — provides
end-to-end atomicity: either the whole run lands or nothing does. The
streaming implementation preserves this because the atomic unit
(Lance manifest commit, SQL wrapping transaction) is the last step,
applied only after the stream drains successfully.

| Scenario | What the user sees |
|----------|--------------------|
| Query errors mid-stream | Row → `failed`, `error` carries the SQL / planner message. Lake: no manifest commit, destination unchanged. DB: the wrapping transaction aborts, no rows visible. |
| Timeout (`execution.timeout_ms`) | Row → `failed` with `"job timed out after <N>ms before commit"`. Same destination guarantees as the error case. |
| `skardi job cancel` before commit | The shared cancel flag flips; the stream adapter errors out of its next `poll_next`; the destination's in-flight transaction / manifest unwinds. Row → `cancelled`, destination unchanged. |
| `skardi job cancel` after commit | Race: the commit landed before the cancel flag was observed. Row → `cancelled` with `rows_written` + `snapshot_id` populated and `error: "cancelled after commit"`. Cancel is reported truthfully but cannot roll the commit back. |
| Server crash or SIGKILL mid-run | Lake: no manifest commit, dataset at the previous version. DB: the transaction aborts on connection drop, no rows visible. On restart, the orphaned run row is rewritten to `failed` with `"server restarted before run completed"`. |

Bare Parquet destinations are deliberately **not** supported — a crashed
writer would leave partial `.parquet` files visible to readers with no
rollback. Lance (today) and Iceberg (v1.1) both solve this by layering a
versioned manifest on top of columnar files.

Non-transactional SQL-ish backends (Redis, MongoDB, SeekDB) are rejected
at submit time for the same reason: without a wrapping transaction there
is no way to roll back a mid-run failure.

---

## A complete worked example

The [llm_wiki demo](../demo/llm_wiki/cli/) ships a
[backfill job](../demo/llm_wiki/cli/jobs/backfill_to_lake.yaml) that
copies a slug-prefix slice of `wiki_pages` from the demo's SQLite into a
Lance dataset:

```yaml
# demo/llm_wiki/cli/jobs/backfill_to_lake.yaml (excerpt)

kind: job

metadata:
  name: "wiki-backfill-to-lake"
  version: "1.0.0"

spec:
  query: |
    SELECT slug, title, page_type, content, updated_at
    FROM wiki.main.wiki_pages
    WHERE slug LIKE {slug_prefix}
    ORDER BY updated_at DESC
    LIMIT {limit}
  destination:
    table: "wiki_lake"
    mode: append
    create_if_missing: true
```

To run it end-to-end, boot the server with `--jobs` pointing at the
demo's job directory and with a Lance data source named `wiki_lake` in
your ctx:

```yaml
# ctx.yaml — add this entry under `spec.data_sources:`
    - name: wiki_lake
      type: lance
      path: demo/llm_wiki/wiki_lake.lance
```

Then:

```bash
cargo run --bin skardi-server -- \
  --ctx demo/llm_wiki/cli/ctx.yaml \
  --pipeline demo/llm_wiki/cli/pipelines \
  --jobs demo/llm_wiki/cli/jobs \
  --port 8080
```

In another terminal:

```bash
skardi job run wiki-backfill-to-lake \
    --param slug_prefix:str='entity/%' \
    --param limit:int=500
```

The CLI prints `submitted: <run_id> (pending)`. Follow it with
`skardi job status <run_id>` until you see `"status": "succeeded"` and a
non-null `snapshot_id` (the Lance dataset version).

Re-running the same command appends more rows to the same dataset.

---

## What's not in MVP

- **Iceberg destination** — read-only today. Deferred to v1.1.
- **Delta Lake destination** — same shape as Lance; lands when a demand
  case appears.
- **`mode: overwrite`** — deferred pending provider-native transactional
  DML (see the `destination.mode` section above for the atomicity
  reasoning).
- **Upsert / merge.**
- **Watermark / incremental templating** (`${last_successful_run.finished_at}`).
- **Cron / event triggers.**
- **Streaming / CDC.**
- **Partitioned / parallel batch execution** — one spawned task per run.
- **Hot-reload of job YAMLs from disk** — the server currently requires
  a restart to pick up new jobs.
- **HTTP-submitted job definitions** — agents *invoke* jobs, not *define*
  them. Permanently out of scope.
- **DDL for DB destinations** — permanently out of scope. Create DB
  tables with your DB's own DDL.

The design doc at `.claude/plans/pipelines_as_jobs.md` has the long-form
rationale for each of these.
