# Skardi CLI

CLI for running SQL queries against local files, remote object stores, datalake formats, and databases. No server required.

## Install

From the repo root:

```bash
cargo install --path crates/cli
```

Then run `skardi` from anywhere.

## Run without installing

From the repo root:

```bash
cargo run -p skardi-cli -- <command> [options]
```

## Commands

### `query` — Run SQL or show schema

Execute a SQL query or show table schema(s). Data sources can come from:

- **Local files** — CSV, Parquet, JSON/NDJSON (directly by path in SQL or via context file)
- **Remote files** — S3, GCS, Azure Blob, HTTP/HTTPS, OSS, COS (directly by URL in SQL or via context file)
- **Datalake formats** — Lance (directly by path in SQL or via context file), Iceberg (via context file)
- **Databases** — PostgreSQL, MySQL, SQLite, MongoDB (via context file or direct path for SQLite)

#### Query files directly (no context file needed)

You can query local or remote files directly by referencing their paths in SQL — no context file or pre-registration required:

```bash
# Local files
skardi query --sql "SELECT * FROM './data/products.csv' LIMIT 10"
skardi query --sql "SELECT * FROM '/absolute/path/events.parquet'"
skardi query --sql "SELECT * FROM './data/logs.json'"

# Lance datasets
skardi query --sql "SELECT * FROM './embeddings.lance' LIMIT 5"

# SQLite tables (pattern: path/to/file.db.table_name)
skardi query --sql "SELECT * FROM './data/my_database.db.users'"
skardi query --sql "SELECT * FROM './data/app.sqlite.customers'"

# Remote files (S3, GCS, Azure)
skardi query --sql "SELECT * FROM 's3://mybucket/data/events.parquet'"
skardi query --sql "SELECT * FROM 'gs://mybucket/data.csv'"
skardi query --sql "SELECT * FROM 'az://mycontainer/data.parquet'"
skardi query --sql "SELECT * FROM 'https://example.com/data.csv'"

# Join across sources
skardi query --sql "
  SELECT a.*, b.score
  FROM './users.csv' a
  JOIN 's3://mybucket/scores.parquet' b ON a.id = b.user_id
"
```

Remote storage credentials are read from standard environment variables (e.g., `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` for S3; `GOOGLE_SERVICE_ACCOUNT` for GCS; `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCESS_KEY` for Azure).

**Supported remote schemes:** `s3://`, `gs://`, `gcs://`, `az://`, `azure://`, `abfs://`, `abfss://`, `http://`, `https://`, `oss://` (Alibaba), `cos://` (Tencent)

#### Query with a context file

For database sources or when you want named tables, use a context file:

```bash
# Inline SQL
skardi query --ctx <path-to-ctx.yaml> --sql "SELECT * FROM my_table LIMIT 10"

# SQL from file
skardi query --ctx <path-to-ctx.yaml> --file query.sql
skardi query -f ./queries/report.sql

# With default ctx (SKARDICONFIG env or ~/.skardi/config/ctx.yaml)
export SKARDICONFIG=/path/to/ctx.yaml
skardi query --sql "SELECT * FROM my_table"
skardi query --file report.sql
```

**Context file resolution** (when `--ctx` is omitted): `SKARDICONFIG` env, then `~/.skardi/config/ctx.yaml`. If no context file is found, the query runs without pre-registered tables (you can still query files directly by path).

#### Schema inspection

Use `--schema` with either `--all` (all tables) or `-t TABLE` (one table):

```bash
skardi query --ctx ./demo/ctx.yaml --schema --all
skardi query --ctx ./demo/ctx.yaml --schema -t products

```

#### Context file format

```yaml
data_sources:
  # Local CSV
  - name: products
    type: csv
    path: data/products.csv
    options:
      has_header: true
      delimiter: ","
      schema_infer_max_records: 1000

  # Local Parquet
  - name: events
    type: parquet
    path: data/events.parquet

  # Remote Parquet (S3)
  - name: remote_events
    type: parquet
    path: s3://mybucket/data/events.parquet

  # JSON / NDJSON
  - name: logs
    type: json
    path: data/logs.json

  # Lance dataset
  - name: embeddings
    type: lance
    path: data/embeddings.lance

  # Iceberg table
  - name: transactions
    type: iceberg
    path: s3://warehouse/path
    options:
      namespace: my_db
      table: transactions
      aws_region: us-east-1

  # PostgreSQL
  - name: users
    type: postgres
    connection_string: postgresql://localhost:5432/mydb
    options:
      table: users
      schema: public
      user_env: PG_USER
      pass_env: PG_PASS

  # MySQL
  - name: orders
    type: mysql
    connection_string: mysql://localhost:3306/mydb
    options:
      table: orders
      user_env: MYSQL_USER
      pass_env: MYSQL_PASS

  # SQLite
  - name: users
    type: sqlite
    path: data/my_database.db
    options:
      table: users
      busy_timeout_ms: "5000"   # Optional

  # MongoDB
  - name: profiles
    type: mongo
    connection_string: mongodb://localhost:27017
    options:
      database: mydb
      collection: profiles
      primary_key: _id
```

**Supported types:**

| Type | Source | Path / Connection |
|------|--------|-------------------|
| `csv` | Local or remote CSV files | File path or remote URL |
| `parquet` | Local or remote Parquet files | File path or remote URL |
| `json` / `ndjson` | Local or remote JSON files | File path or remote URL |
| `lance` | Lance vector datasets | Local path |
| `iceberg` | Apache Iceberg tables | Warehouse path (local or S3) |
| `postgres` | PostgreSQL tables | `postgresql://host:port/db` |
| `mysql` | MySQL tables | `mysql://host:port/db` |
| `sqlite` | SQLite tables | Local file path (e.g. `data/my.db`) |
| `mongo` | MongoDB collections | `mongodb://host:port` |

**Path resolution:** Relative paths in the context file are resolved relative to your **current working directory**.

**Database credentials:** For security, database credentials are supplied via environment variables (specified in `options` as `user_env` / `pass_env`), not in the connection string.

#### Vector search with `lance_knn`

The `lance_knn` table function is built-in and lets you run K-nearest-neighbor searches against Lance datasets.

The Lance dataset must be registered first — either via a context file or by querying it by path (which auto-registers it under the file stem as the table name). For example, querying `'./embeddings.lance'` registers it as `embeddings`.

```sql
-- Syntax: lance_knn(table_name, vector_column, query_vector, k [, filter])
```

Arguments:
1. `table_name` (string) — Name of the registered Lance table
2. `vector_column` (string) — Column containing the vectors
3. `query_vector` (array or subquery) — The query vector to search for
4. `k` (integer) — Number of nearest neighbors to return
5. `filter` (string, optional) — SQL filter predicate applied before KNN search

The result includes all columns from the table (except the vector column) plus a `_distance` column.

**Using with a context file:**

```yaml
# ctx.yaml
data_sources:
  - name: embeddings
    type: lance
    path: data/embeddings.lance
```

```bash
skardi query --ctx ./ctx.yaml --sql "
  SELECT id, label, _distance
  FROM lance_knn('embeddings', 'vector', [0.1, 0.2, 0.3], 5)
"
```

**Using with direct path (no context file):**

First reference the Lance dataset in a query so it gets auto-registered, then use `lance_knn` with the derived table name (file stem):

```bash
# The path './embeddings.lance' auto-registers as table name 'embeddings'
skardi query --sql "
  SELECT * FROM lance_knn('embeddings', 'vector',
    (SELECT vector FROM './embeddings.lance' WHERE id = 42), 10)
"
```

**More examples:**

```sql
-- KNN with a literal vector
SELECT * FROM lance_knn('embeddings', 'vector', [0.1, 0.2, 0.3, ...], 10)

-- KNN with a subquery vector
SELECT * FROM lance_knn('embeddings', 'vector',
    (SELECT vector FROM embeddings WHERE id = 42), 10)

-- KNN with a pre-filter
SELECT * FROM lance_knn('embeddings', 'vector', [0.1, 0.2, ...], 10,
    'category = ''electronics''')
```

#### Full-text search with `lance_fts`

The `lance_fts` table function is built-in and lets you run full-text search (BM25) against Lance datasets with a full-text index.

Like `lance_knn`, the Lance dataset must be registered first — either via a context file or by querying it by path (which auto-registers it under the file stem as the table name).

```sql
-- Syntax: lance_fts(table_name, text_column, search_query, limit)
```

Arguments:
1. `table_name` (string) — Name of the registered Lance table
2. `text_column` (string) — Column containing the text to search
3. `search_query` (string) — The search query (see query syntax below)
4. `limit` (integer) — Maximum number of results to return

The result includes all columns from the table plus a `_score` column with BM25 relevance scores.

**Query syntax:**

| Syntax | Example | Description |
|--------|---------|-------------|
| Term search | `'umbrella train'` | OR logic across terms, ranked by BM25 |
| Phrase search | `'"train to boston"'` | Exact phrase match |
| Fuzzy search | `'rammen~1'` | Typo-tolerant (edit distance 1–2) |
| Boolean search | `'+umbrella -train'` | `+` = must include, `-` = must exclude |

**Using with a context file:**

```yaml
# ctx.yaml
data_sources:
  - name: products
    type: lance
    path: data/products.lance
```

```bash
skardi query --ctx ./ctx.yaml --sql "
  SELECT id, description, _score
  FROM lance_fts('products', 'description', 'wireless headphones', 10)
"
```

**Using with direct path (no context file):**

```bash
# The path './products.lance' auto-registers as table name 'products'
skardi query --sql "
  SELECT * FROM lance_fts('products', 'description', 'wireless headphones', 10)
"
```

**More examples:**

```sql
-- Term search
SELECT * FROM lance_fts('products', 'description', 'umbrella', 10)

-- Phrase search
SELECT * FROM lance_fts('products', 'description', '"noise cancelling"', 10)

-- Fuzzy search
SELECT * FROM lance_fts('products', 'description', 'headphnes~1', 10)

-- Boolean search
SELECT * FROM lance_fts('products', 'description', '+wireless -bluetooth', 10)

-- With WHERE filter
SELECT * FROM lance_fts('products', 'description', 'premium', 50)
WHERE category = 'electronics' AND price < 20
```

### `run` — Execute a pipeline YAML by name

A **pipeline** is a named SQL template stored in a YAML file:

```yaml
# demo/llm_wiki/pipelines-cli/list.yaml
metadata:
  name: "wiki-list"
query: |
  SELECT slug, title, page_type, updated_at
  FROM wiki.main.wiki_pages
  WHERE page_type LIKE {page_type_pattern}
    AND slug      LIKE {slug_prefix}
  ORDER BY updated_at DESC
  LIMIT {limit}
```

`{name}` placeholders are substituted at call time. Each parameter must be
bound via `--param NAME=VALUE`; values are rendered as SQL-safe literals
before DataFusion sees the query (strings are single-quoted with `'` → `''`
escaping, so quotes inside values can't break out).

```bash
skardi run wiki-list --ctx ./demo/llm_wiki/cli-ctx.yaml \
  --param 'page_type_pattern=%' \
  --param 'slug_prefix=concept/%' \
  --param 'limit=10'
```

**Pipeline discovery** — the CLI scans a single directory for
`*.yaml` / `*.yml` files, resolved in this order:

1. `--pipeline-dir <DIR>` flag.
2. `pipelines_dir:` key in the ctx YAML (relative paths resolve against the
   ctx file's directory).
3. Otherwise, no pipelines are registered — only `skardi run` invocations
   that pass `--pipeline-dir` will find one.

Example ctx addition:

```yaml
data_sources:
  - name: wiki
    type: sqlite
    path: demo/llm_wiki/wiki.db
    access_mode: read_write
    hierarchy_level: catalog

pipelines_dir: pipelines-cli
```

**Parameter typing** — values pass through a heuristic: bare integers become
`Int64`, bare floats become `Float64`, `true`/`false` become `Boolean`,
everything else is `Utf8`. Force a type explicitly with `NAME:TYPE=VALUE`:

```bash
# Force "42" to be a string even though it parses as an int
skardi run my-pipeline --param 'query:str=42' --param 'limit:int=10'
```

Supported types: `str` / `string`, `int` / `i64`, `float` / `f64`, `bool`.

### `alias` — Bind a short verb to a pipeline

Aliases let you replace `skardi run wiki-search-hybrid --query="..." --text_query="..." ...`
with a one-word verb like `skardi grep "..."`. They are a **CLI-only**
concept: the server does not read alias files. Any unknown subcommand is
looked up in the alias store, resolved to a pipeline + params, and
dispatched to the same code path as `skardi run`.

#### Add an alias

```bash
skardi alias add grep \
  --ctx ./demo/llm_wiki/cli-ctx.yaml \
  --pipeline wiki-search-hybrid \
  --positional query \
  --default 'text_query={query}' \
  --default 'vector_weight=0.5' \
  --default 'text_weight=0.5' \
  --default 'limit=10' \
  --description "Hybrid search over the wiki"
```

Flags:

- `--pipeline <NAME>` (required) — `metadata.name` of the pipeline to call.
- `--positional <NAMES>` — comma-separated pipeline-param names to bind to
  positional CLI args in order (e.g. `--positional query,text_query`).
- `--default <NAME=VALUE>` (repeatable) — default value for a param. May
  contain `{other}` tokens that are substituted from an already-bound param
  (one level), so a single positional can fan out to multiple params.
- `--description <TEXT>` — optional short help string shown in `alias list`.
- `--force` — overwrite an existing alias with the same name.

Now `grep` is a first-class verb:

```bash
skardi grep "turing machine" --ctx ./demo/llm_wiki/cli-ctx.yaml
# → skardi run wiki-search-hybrid \
#     --param 'query=turing machine' \
#     --param 'text_query=turing machine' \
#     --param 'vector_weight=0.5' \
#     --param 'text_weight=0.5' \
#     --param 'limit=10'

# Flag overrides beat positional/default bindings
skardi grep "turing machine" --ctx ./demo/llm_wiki/cli-ctx.yaml \
  --text_query='bletchley OR enigma' --limit=3
```

Positional args bind in order to `alias.positional`. Extra positional args
error. `--name=value` / `--name value` flags always win over positional
binds and defaults.

#### List / show / remove

```bash
skardi alias list  --ctx ./demo/llm_wiki/cli-ctx.yaml
skardi alias show  grep --ctx ./demo/llm_wiki/cli-ctx.yaml
skardi alias remove grep --ctx ./demo/llm_wiki/cli-ctx.yaml
```

#### Alias file resolution

The aliases YAML is resolved in this order:

1. `--aliases <PATH>` flag.
2. `SKARDI_ALIASES` env var.
3. `aliases.yaml` next to the active ctx file (only if it already exists).
4. `~/.skardi/config/aliases.yaml`.

The file is a simple top-level map keyed by alias name:

```yaml
# demo/llm_wiki/aliases.yaml
grep:
  pipeline: wiki-search-hybrid
  positional: [query]
  defaults:
    text_query: "{query}"
    vector_weight: "0.5"
    limit: "10"
  description: Hybrid search over the wiki
ls:
  pipeline: wiki-list
  defaults:
    page_type_pattern: "%"
    slug_prefix: "%"
    limit: "100"
```

Hand-editing the file is fine — `skardi alias add` is just a convenience
that round-trips through serde.

## Examples

```bash
# Simple query (no context file needed)
skardi query --sql "SELECT 1"

# Query a local file directly
skardi query --sql "SELECT count(*) FROM './data/products.csv'"

# Query a remote parquet file
skardi query --sql "SELECT * FROM 's3://mybucket/events.parquet' LIMIT 10"

# Query a Lance dataset
skardi query --sql "SELECT * FROM './embeddings.lance' LIMIT 5"

# Query a SQLite table directly
skardi query --sql "SELECT * FROM './data/app.db.users' LIMIT 10"

# With context file
cargo run -p skardi-cli -- query --ctx ./demo/ctx.yaml --sql "SELECT * FROM products LIMIT 5"

# Show schema
skardi query --ctx ./demo/ctx.yaml --schema --all
skardi query --ctx ./demo/ctx.yaml --schema -t products

# SQL from file
skardi query --ctx ./demo/ctx.yaml -f ./queries/report.sql

# Run a pipeline YAML by name, passing named parameters
skardi run wiki-list --ctx ./demo/llm_wiki/cli-ctx.yaml \
  --param 'page_type_pattern=entity' --param 'slug_prefix=%' --param 'limit=20'

# Invoke a user-defined alias (dispatches to `skardi run <pipeline>`)
skardi grep "turing machine" --ctx ./demo/llm_wiki/cli-ctx.yaml

# Manage aliases
skardi alias list --ctx ./demo/llm_wiki/cli-ctx.yaml
skardi alias show grep --ctx ./demo/llm_wiki/cli-ctx.yaml
```
