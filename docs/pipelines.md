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
- **MCP tools** — same YAML projected to MCP for non-Claude hosts (v1.1
  roadmap).

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
