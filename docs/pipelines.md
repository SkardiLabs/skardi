# Pipelines

Pipelines are Skardi's **online-serving** primitive: a parameterized SQL
query declared in YAML, exposed synchronously as a REST endpoint and
runnable from the CLI. They're the read path every agent tool call hits —
the low-latency peer of [offline jobs](jobs.md), which use the same SQL
shape for durable async writes.

**In one sentence:** a pipeline answers a query; a job commits the answer
somewhere you can query again later.

One pipeline YAML drives every agent-facing surface:

- **REST** — `POST /<name>/execute` against `skardi-server` today.
- **Shell** — `skardi run <name> --param=…` from the CLI today.
- **Claude skills** — auto-generated Markdown under `.claude/skills/` (v1.1
  roadmap).
- **MCP tools** — same YAML projected to MCP tools for non-Claude hosts via
  `skardi mcp` — see [mcp.md](mcp.md).

This page covers the pipeline YAML shape, parameter inference, invocation,
and response format. For the HTTP binding and shared concerns (context
files, access mode, caching), see [server.md](server.md). For the async
peer, see [jobs.md](jobs.md).

---

## Pipeline YAML shape

Every pipeline file is a `{ kind, metadata, spec }` envelope. `spec.query`
holds the SQL, with `{placeholder}` tokens for parameters.

```yaml
kind: pipeline

metadata:
  name: product-search-demo
  version: 1.0.0
  description: "Product search and filtering"

spec:
  query: |
    SELECT
      "Name"  AS product_name,
      "Brand" AS brand,
      "Price" AS price
    FROM products
    WHERE ({brand}     IS NULL OR "Brand" = {brand})
      AND ({max_price} IS NULL OR "Price" < {max_price})
    ORDER BY "Price" ASC
    LIMIT {limit}
```

The loader is strict — a file without `kind: pipeline` at the root is
rejected at startup, and a pipeline file under a `--jobs` directory is
silently skipped.

### Parameter placeholders

Parameters are `{name}` tokens in the SQL. The loader extracts the set of
placeholders and infers a type per parameter from how it's used in the
query. No separate `parameters:` block is needed — the SQL is the
declaration.

Each `POST /<name>/execute` body is a JSON object keyed by placeholder
name; the CLI takes `--param name=value` flags with optional typed
suffixes (`--param limit:int=10`).

### Parameter descriptions

An optional `spec.parameters` map attaches a one-line, human-written
description to any parameter:

```yaml
spec:
  parameters:
    brand: "Exact brand name; null matches every brand"
    max_price: "Upper price bound in USD, inclusive"
  query: |
    SELECT ...
    WHERE ({brand} IS NULL OR "Brand" = {brand})
      AND ({max_price} IS NULL OR "Price" < {max_price})
```

Descriptions are documentation, not declaration — the parameter set still
comes from the SQL, and every key under `spec.parameters` must name a
`{placeholder}` the query declares; a misspelled or stale key fails the
load. Each description is published as the standard `description` keyword
inside that parameter's `json_schema` in the enriched `GET /pipelines`
inventory, which `skardi mcp` projects verbatim into MCP tool input
schemas — the sentence written here is exactly what a model reads before
filling the parameter.

### Parameter shapes

The `{name}` token is replaced with a SQL-safe literal at execution time.
The supported JSON value → SQL literal mapping is:

| JSON shape | Renders as | Example use |
|---|---|---|
| `"abc"` | `'abc'` (escaped, single-quoted) | `WHERE name = {name}` |
| `123` | `123` | `LIMIT {top_k}` |
| `true` / `false` | `true` / `false` | `WHERE active = {active}` |
| `null` | `NULL` | `WHERE {brand} IS NULL OR brand = {brand}` |
| `[1, 2, 3]` (array of scalars) | `[1, 2, 3]` | pgvector / SeekDB VECTOR literal |
| `[[…], […]]` (array of arrays) | `(c1, c2, …), (c1, c2, …)` | `INSERT … VALUES {rows}` |

The array-of-arrays shape lets one parameter carry a multi-row VALUES
clause whose batch size is set by the caller, not baked into the YAML:

```yaml
spec:
  query: |
    INSERT INTO docs (id, title, embedding) VALUES {rows}
```

```bash
curl -X POST http://localhost:8080/batch_insert/execute \
  -H "Content-Type: application/json" \
  -d '{
    "rows": [
      ["d1", "doc-a", [1.0, 0.0, 0.0, 0.0]],
      ["d2", "doc-b", [0.0, 1.0, 0.0, 0.0]],
      ["d3", "doc-c", [0.0, 0.0, 1.0, 0.0]]
    ]
  }'
```

renders as:

```sql
INSERT INTO docs (id, title, embedding) VALUES
  ('d1', 'doc-a', [1.0, 0.0, 0.0, 0.0]),
  ('d2', 'doc-b', [0.0, 1.0, 0.0, 0.0]),
  ('d3', 'doc-c', [0.0, 0.0, 1.0, 0.0])
```

A nested array inside a row tuple (e.g. an `embedding` cell) renders as
the bracketed scalar form `[v1, v2, v3]` — the same text shape pgvector
and SeekDB's `VECTOR` columns accept for a single-row insert.

**Reject case.** Mixed-shape arrays — where some elements of `{rows}` are
themselves arrays and others are scalars — return
`parameter_validation_error: Unsupported parameter type` rather than
silently emitting malformed SQL. Callers must pass *every* element of a
row-list parameter as an array, even for batch size 1.

**Empty batch.** A zero-length array (`{"rows": []}` or `{"embedding": []}`)
is rejected with `parameter_validation_error: empty array — provide at
least one row/element`. There is no SQL expansion that makes `VALUES`
with zero rows valid, and a zero-element vector literal is also not a
useful pgvector / SeekDB input. CDC consumers that may produce empty
batches should filter client-side before calling the pipeline.

**Supported sources.** The renderer emits standard SQL, but only the
writable sources accept it through DataFusion's `INSERT INTO` path:

| Source | Multi-row `VALUES {rows}` | Nested-array cell (vector) | How |
|---|---|---|---|
| PostgreSQL | ✅ | ✅ pgvector text input | `SqlxPostgresInsertExec` re-renders as one multi-row VALUES inside a transaction |
| SeekDB | ✅ | ✅ `VECTOR(N)` text input | Delegated to `datafusion-table-providers`; one multi-row VALUES per call |
| MySQL | ✅ | ⚠️ scalar cells only | Delegated to `datafusion-table-providers`; one multi-row VALUES per call. No native vector type |
| SQLite | ✅ | ⚠️ scalar cells only | DataFusion materializes VALUES into one batch; provider replays row-by-row in a single transaction. No native vector type |
| MongoDB | ✅ | ⚠️ scalar cells only | DataFusion materializes VALUES into one batch; each row → one BSON document. Nested-array cells are not exercised — schema is Utf8 and a `[v1, v2, v3]` cell would land as a coerced string, not a BSON array |
| Redis | ✅ | ⚠️ scalar cells only | DataFusion materializes VALUES into one batch; each row → one keyed hash. Nested-array cells are not exercised — Redis hash fields are flat strings, so a `[v1, v2, v3]` cell would be coerced to a single string field |
| Lance | ❌ | n/a | `Dataset` is scan-only — use the [`kind: job`](jobs.md) primitive for atomic writes |
| Iceberg | ❌ | n/a | Read-only in MVP (writes deferred to v1.1) |
| CSV | ❌ | n/a | File source has no commit primitive |
| Parquet | ❌ | n/a | File source has no commit primitive |

For Lance, point the pipeline at a writable mirror (Postgres / SQLite)
or use a job to land batches atomically; the renderer itself is identical.

Runnable examples (one per writable source):
- `docs/postgres/pipelines/batch_insert_users.yaml`
- `docs/postgres/pipelines/batch_insert_docs_with_embeddings.yaml`
- `docs/mysql/pipelines/batch_insert_users.yaml`
- `docs/sqlite/pipelines/batch_insert_users.yaml`
- `docs/seekdb/pipelines/batch_insert_users.yaml`
- `docs/seekdb/pipelines/batch_insert_docs_with_embeddings.yaml`
- `docs/mongo/pipelines/batch_insert_products.yaml`
- `docs/redis/pipelines/batch_insert_products.yaml`

### Optional filter pattern

Use `{param} IS NULL OR …` to make a filter optional — callers pass `null`
(or omit the CLI flag) to skip it.

```sql
WHERE ({brand} IS NULL OR "Brand" = {brand})
  AND ({max_price} IS NULL OR "Price" < {max_price})
```

This keeps one pipeline YAML flexible instead of fan-out into a dozen
near-duplicate files.

---

## Invoking a pipeline

### Over HTTP

```bash
curl -X POST http://localhost:8080/product-search-demo/execute \
  -H "Content-Type: application/json" \
  -d '{"brand": "Apple", "max_price": 500.0, "limit": 10}'
```

### From the shell

```bash
skardi run product-search-demo \
  --param brand=Apple \
  --param max_price:float=500 \
  --param limit:int=10
```

The CLI resolves the pipeline YAML directly from disk — no running server
required — so any agent with a Bash tool can invoke any pipeline as a
verb. Define a [user alias](cli.md) and
`skardi search "…"` becomes a one-word tool call.

---

## Response format

**Success:**

```json
{
  "success": true,
  "data": [{"product_name": "Laptop", "price": 999.99}],
  "rows": 1,
  "execution_time_ms": 15,
  "timestamp": "2026-01-15T12:00:00.000Z"
}
```

**Error:**

```json
{
  "success": false,
  "error": "Missing required parameters: limit",
  "error_type": "parameter_validation_error",
  "details": {"missing_parameters": ["limit"]},
  "timestamp": "2026-01-15T12:00:00.000Z"
}
```

`error_type` is a stable tag (`parameter_validation_error`,
`data_source_error`, `query_execution_error`, …) suitable for agents to
branch on without parsing human-readable text.

One tag deserves special handling: **`query_audit_error` (HTTP 503)**. It
appears only when the operator enabled the audit ledger
(`--query-audit-db`) and the pre-execution audit write failed or timed out.
It means **the pipeline did not run** — unlike a `500`, retrying later is
exactly right, and unlike a `400` there is nothing to fix in the request.
An agent that treats it as a generic failure either abandons work that
never executed or hammers a stalled ledger; back off and retry instead.

### Session attribution

Send `X-Skardi-Session-Id: <id>` (non-empty, ≤ 200 chars) with an execute
request to group this run with the rest of an agent session in the query
audit ledger (`--query-audit-db`). The header is optional. It is always
validated — a malformed value (empty, over 200 characters, outside visible
ASCII, containing a space, tab or comma, or sent more than once) is rejected
with `400 parameter_validation_error` whether or not auditing is enabled. The
reject carries `details.header` so an agent can branch on it without parsing
the message. Two of those rules earn their keep: a **comma** is rejected
because proxies may merge repeated header lines into one comma-separated
value (RFC 9110 §5.3), which would otherwise smuggle a duplicate past the
duplicate check; a **space** is rejected because HTTP parsers trim leading and
trailing whitespace (RFC 9110 §5.5), so ` sess-1 ` would be silently recorded
under the different key `sess-1` — breaking the session stitching the field
exists for. A well-formed value is simply unused when `--query-audit-db` is
not configured. It is a header rather than a body field because the request
body is the parameter map itself — a reserved key could collide with a SQL
parameter of the same name.

`session_id` is caller-asserted, not authenticated: it groups executions, it
does not attest to their origin.

---

## Pipelines vs. jobs — picking the right shape

Use a **pipeline** when the caller needs the rows back in the same HTTP
response. Use a **job** when the result should land somewhere durable (a
Lance dataset, a table in a read-write DB) and the caller doesn't need to
block on completion. Both are the same YAML envelope and the same SQL
dialect; the difference is the destination and the synchronous /
asynchronous contract.

See [jobs.md](jobs.md) for the job YAML shape, destinations, run ledger,
and cancellation semantics.
