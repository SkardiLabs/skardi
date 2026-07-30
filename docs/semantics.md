# Catalog Semantics

A **semantics overlay** attaches natural-language descriptions to the
tables and columns already registered through a context file. It is
loaded entirely server-side:

- `skardi-server` loads it at startup and emits the descriptions on
  `GET /data_source` so an agent can read them when picking a tool.
- `skardi schema` — the thin CLI's client for that same endpoint —
  prints the merged view for human inspection, against whatever
  server the CLI is pointed at.

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

`spec.sources[]` is a flat list of overlays. The `name` field is either:

- **A bare source name** — cross-references `data_sources[].name` from
  the ctx. For table-mode sources (CSV, Parquet, Lance, Iceberg…) where
  the source name *is* the physical table name, this is the only form
  you need. For [catalog-mode sources](catalog.md) (Postgres / MySQL /
  SQLite registered with `hierarchy_level: catalog`), the bare entry
  applies as a *broad fallback* to every inner table.
- **A fully-qualified DataFusion path** `catalog.schema.table` —
  targets one specific physical table. Useful for catalog-mode sources
  where a single ctx registration spawns many inner tables and you
  want per-inner-table descriptions.

```yaml
spec:
  sources:
    # 1-part: broad fallback for the whole `mydb` catalog.
    - name: mydb
      description: "Internal application DB"

    # 3-part: targets a specific inner table. Wins over the bare entry
    # above for `mydb.public.users` only.
    - name: mydb.public.users
      description: "Auth + profile data, one row per registered account"
      columns:
        - name: id
          description: "User ID (auth.users.id)"

    - name: mydb.public.orders
      description: "Submitted orders"
      columns:
        - name: id
          description: "Order number, monotonic"
```

Names with anything other than 1 or 3 dot-separated segments are a
hard error (e.g. `schema.table`, `a.b.c.d`, or empty segments like
`mydb..users`). Semantics for an unknown source / catalog are warned
about (not failed) at load time so a stale overlay does not brick a
partially-rebooted server.

`description` and `columns` are both optional — supply only what you
have. Unknown columns are not reported (the merge runs at request time
against the live Arrow schema).

A complete worked example lives at
[`docs/basic/semantics.yaml`](basic/semantics.yaml), paired with the
existing [`docs/basic/ctx.yaml`](basic/ctx.yaml).

---

## Loading

```bash
# Load semantics as part of starting the server
skardi-server \
  --ctx ctx.yaml \
  --pipeline pipelines/ \
  --semantics semantics/ \    # optional; auto-discovered next to ctx if omitted
  --port 8080

# or point --semantics at a single file instead of a directory
skardi-server --ctx ctx.yaml --semantics ./custom/semantics.yaml
```

The CLI never loads a semantics file itself — it has no local catalog
to merge one into. Once the server above is running, ask it for the
merged view over HTTP:

```bash
skardi schema
```

`skardi-server` resolves the path with this order:

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

Multiple semantics files may be merged into one registry. Bare and
qualified entries live in different addressing spaces — they're not
duplicates of each other even when they describe the same physical
table. The rules:

| Situation | Behavior |
|-----------|----------|
| Two files share the same key (same bare `name:`, **or** same `catalog.schema.table`) at table or column level | **Hard error** at startup. Both file paths are reported. |
| One file has `name: mydb`, another has `name: mydb.public.users` | **Both kept**. The qualified entry wins for `mydb.public.users`; the bare entry covers every other inner table. |
| `name:` has 0, 2, or 4+ dot-separated segments (or any empty segment) | **Hard error** at startup. |
| A file references an unknown source / catalog | **Warning**. The entry is kept in the registry but never matches. |
| A file is named explicitly (`--semantics file.yaml`) and is missing `kind: semantics` | Soft skip — same as a non-semantics file in a directory scan. |

The duplicate-is-error rule keeps auto-generated overlays composable:
each file owns its own slice of the catalog and never silently overwrites
a sibling.

---

## Fallback / precedence

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

That value is the table-level **ctx-inline fallback** — used when no
semantics overlay supplies one. Column-level descriptions have no ctx
fallback; they live only in semantics files.

The merge precedence (most-specific wins):

1. **Qualified semantics overlay** — `name: catalog.schema.table` (table
   or column).
2. **Bare semantics overlay** — `name: <source>` (table or column).
3. **`data_sources[].description`** (table-level only).
4. None — the field is omitted from the JSON response.

Steps 1 and 2 cooperate: a qualified entry only covers what it
addresses; the bare entry continues to apply to anything the qualified
entry didn't touch. So writing both forms is the normal path for
catalog-mode sources where most inner tables share a description but a
few want their own.

---

## Where it shows up

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

### `skardi schema`

The CLI has no local catalog of its own — `skardi schema` is a thin
client that calls `GET /data_source` on the server it's pointed at and
pretty-prints the same JSON response shown just above. The merged
descriptions show up automatically whenever the server has a
`kind: semantics` overlay (or `data_sources[].description`) loaded —
no flag is needed to opt in:

```bash
skardi schema
```

The output is always pretty-printed JSON — `skardi schema` takes no flags.

---

## Limitations

- `GET /data_source` still emits **one table per data source** (the
  source name *is* the table name in the JSON response). Catalog-mode
  sources expose many inner tables, but the HTTP endpoint doesn't
  enumerate them yet — so qualified `catalog.schema.table` overlays
  defined for inner tables won't surface on the endpoint (or on
  `skardi schema`, which only renders that endpoint's response) until
  the endpoint is extended.
- There is no agent-callable `describe` verb yet. Agents reach the
  semantics through the HTTP endpoint above; a pipeline form is a
  separate task on the roadmap.

---

## Next

- **[Server](server.md)** — full flag reference and lifecycle.
- **[Catalog mode](catalog.md)** — registering an entire database as a DataFusion catalog.
- **[Spark for Agents](spark_for_agents.md)** — why this primitive exists.
