# Catalog Semantics

A **semantics overlay** attaches natural-language descriptions to the
tables and columns already registered through a context file. The server
loads them at startup alongside pipelines, jobs, and the context, and
the catalog endpoint (`GET /data_source`) emits the descriptions on its
response so an agent can read them when picking a tool.

This page documents the YAML shape, how the server loads it, the
override / fallback rules, and the resulting JSON shape on the catalog
endpoint.

---

## Why

Raw `column: Utf8` schemas are not enough for an agent to pick the right
tool. The model needs to know *what* the column holds — `price_usd` is
"retail price in USD", `slug` is the "URL-stable identifier", and so
on. Stuffing those descriptions inline in `ctx.yaml` works for a single
table but does not scale: catalog-mode sources have many tables, and
auto-generated descriptions (e.g. from the
[`auto_knowledge_base`](https://github.com/SkardiLabs/skardi-skills/tree/main/auto_knowledge_base)
skill) want their own file so they don't pollute hand-curated config.

A separate `kind: semantics` resource is the answer: same envelope as
context / pipelines / jobs, hot-pluggable at startup, freely composable
across multiple files.

---

## File shape

```yaml
kind: semantics

metadata:
  name: basic-semantics
  version: 1.0.0

spec:
  sources:
    - name: products              # must match a data_sources[].name in the ctx
      description: "Product catalog with pricing/inventory. One row per SKU."
      columns:
        - name: id
          description: "Stable internal SKU; primary key."
        - name: price
          description: "Retail price in USD."
```

`spec.sources[]` is a flat list of overlays. The `name` field
cross-references `data_sources[].name` from the ctx; semantics for an
unknown source are warned about (not failed) at load time so a stale
overlay does not brick a partially-rebooted server.

`description` and `columns` are both optional — supply only what you
have. Unknown columns are not reported (the merge runs at request time
against the live Arrow schema).

A complete worked example lives at
[`docs/basic/semantics.yaml`](basic/semantics.yaml), paired with the
existing [`docs/basic/ctx.yaml`](basic/ctx.yaml).

---

## Loading

```bash
skardi-server \
  --ctx ctx.yaml \
  --pipeline pipelines/ \
  --semantics semantics/ \    # file or directory
  --port 8080
```

`--semantics` accepts either a single yaml file or a directory:

- **Single file** — must be `kind: semantics`. A wrong or missing kind
  is treated the same as for `--jobs`: the file is silently skipped.
- **Directory** — every `*.yaml` / `*.yml` at one level is scanned, in
  alphabetical order. Files whose root `kind:` is not `semantics` are
  silently skipped, so a single shared config directory can mix pipeline
  / job / context / semantics yamls.

When no `--semantics` flag is passed, the server starts normally and
the catalog endpoint falls back to `data_sources[].description` only
(see *Fallback* below).

---

## Composition rules

Multiple semantics files may be merged into one registry. The rules:

| Situation | Behavior |
|-----------|----------|
| Two files describe the same `(source)` table | **Hard error** at startup. Both file paths are reported. |
| Two files describe the same `(source, column)` | **Hard error** at startup. Both file paths are reported. |
| A file references an unknown source | **Warning**. The entry is kept in the registry but never matches. |
| A file is named explicitly (`--semantics file.yaml`) and is missing `kind: semantics` | Soft skip — same as a non-semantics file in a directory scan. |

The duplicate-is-error rule keeps auto-generated overlays composable:
each file owns its own slice of the catalog and never silently overwrites
a sibling.

---

## Fallback to ctx-inline `description`

`data_sources[]` in `ctx.yaml` already accepts a free-text `description`
field:

```yaml
spec:
  data_sources:
    - name: products
      type: csv
      path: data/products.csv
      description: "Product catalog dataset"
```

That value is **the table-level fallback** — used when no semantics
overlay supplies one. A semantics file's `description` always wins over
the ctx-inline value when both are present. Column-level descriptions
have no ctx fallback; they live only in semantics files.

The merge precedence:

1. `kind: semantics` overlay (table or column)
2. `data_sources[].description` (table-level only)
3. None — the field is omitted from the JSON response.

---

## Where it shows up

The catalog endpoint `GET /data_source` returns the merged view:

```bash
curl http://localhost:8080/data_source
```

```json
{
  "success": true,
  "count": 1,
  "data": [
    {
      "name": "products",
      "type": "csv",
      "path": "data/products.csv",
      "tables": [
        {
          "name": "products",
          "description": "Product catalog with pricing/inventory. One row per SKU.",
          "schema": [
            { "name": "id",     "type": "Int64",   "nullable": false, "description": "Stable internal SKU; primary key." },
            { "name": "brand",  "type": "Utf8",    "nullable": false },
            { "name": "price",  "type": "Float64", "nullable": false, "description": "Retail price in USD." }
          ]
        }
      ]
    }
  ],
  "timestamp": "..."
}
```

`description` is omitted from the JSON when no overlay or fallback is
present, so the wire shape stays clean for sources that opt out.

---

## Limitations

- The current `GET /data_source` response emits **one table per data
  source** (the source name *is* the table name). Catalog-mode sources
  expose many tables under a single registration, but only the
  source-level description bubbles through today. Per-table semantics
  for catalog-mode sources is on the roadmap.
- There is no agent-callable `describe` verb yet. Agents reach the
  semantics through the HTTP endpoint above; a CLI / pipeline form is
  a separate task on the roadmap.

---

## Next

- **[Server](server.md)** — full flag reference and lifecycle.
- **[Catalog mode](catalog.md)** — registering an entire database as a DataFusion catalog.
- **[Spark for Agents](spark_for_agents.md)** — why this primitive exists.
