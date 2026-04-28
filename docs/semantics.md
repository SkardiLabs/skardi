# Catalog Semantics

A **semantics overlay** attaches natural-language descriptions to the
tables and columns already registered through a context file. Both
binaries consume it:

- `skardi-server` loads it at startup and emits the descriptions on
  `GET /data_source` so an agent can read them when picking a tool.
- `skardi query --schema` renders the descriptions inline next to each
  table and column, for human inspection.

This page documents the YAML shape, how the loader finds it, the
override / fallback rules, and where the descriptions surface.

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
# server
skardi-server \
  --ctx ctx.yaml \
  --pipeline pipelines/ \
  --semantics semantics/ \    # optional; auto-discovered next to ctx if omitted
  --port 8080

# CLI
skardi query --ctx ctx.yaml --schema --all
skardi query --ctx ctx.yaml --schema --all --semantics ./custom/semantics.yaml
```

Both binaries follow the same resolution order:

1. **Explicit `--semantics <path>`** — used directly. Accepts either a
   single yaml file or a directory.
2. **Auto-discovered `<ctx_dir>/semantics/`** (directory) — every
   `*.yaml` / `*.yml` at one level is scanned, in alphabetical order.
3. **Auto-discovered `<ctx_dir>/semantics.yaml`** (single file).
4. None — the catalog falls back to `data_sources[].description` only
   (see *Fallback* below).

When `--semantics` points at a directory, files whose root `kind:` is
not `semantics` are silently skipped, so a single shared config
directory can mix pipeline / job / context / semantics yamls. A single
file passed explicitly with the wrong or missing kind is also a soft
skip — same behavior as `--jobs`.

> **Auto-discovery collision**: defining both
> `<ctx_dir>/semantics/` and `<ctx_dir>/semantics.yaml` is a hard error
> at startup. Pick one. Silent shadowing of overlays that drive an
> agent's catalog view is exactly the bug worth being loud about.

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

### `skardi query --schema`

The CLI renders the merged view inline next to each table and column.
A `--` separator carries the description; lines without an overlay or
fallback render bare, so existing scripts that parse the output keep
working.

```bash
$ skardi query --ctx ./ctx.yaml --schema --all
table: products  -- Product catalog with pricing/inventory. One row per SKU.
  id: Int64  -- Stable internal SKU; primary key.
  brand: Utf8
  price: Float64  -- Retail price in USD.
```

No flag is needed to opt in: if a `kind: semantics` overlay is
discovered (or `data_sources[].description` is set), the descriptions
appear automatically.

### `GET /data_source` (server)

The catalog endpoint returns the merged view:

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
- The same limitation applies to `skardi query --schema`: catalog-mode
  sources (e.g. SQLite registered as a catalog) get the source-level
  description attached to *every* inner table, since there is no
  per-inner-table semantics yet.
- There is no agent-callable `describe` verb yet. Agents reach the
  semantics through the HTTP endpoint above; a pipeline form is a
  separate task on the roadmap.

---

## Next

- **[Server](server.md)** — full flag reference and lifecycle.
- **[Catalog mode](catalog.md)** — registering an entire database as a DataFusion catalog.
- **[Spark for Agents](spark_for_agents.md)** — why this primitive exists.
