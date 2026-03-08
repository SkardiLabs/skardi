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
- **Databases** — PostgreSQL, MySQL, MongoDB (via context file)

#### Query files directly (no context file needed)

You can query local or remote files directly by referencing their paths in SQL — no context file or pre-registration required:

```bash
# Local files
skardi query --sql "SELECT * FROM './data/products.csv' LIMIT 10"
skardi query --sql "SELECT * FROM '/absolute/path/events.parquet'"
skardi query --sql "SELECT * FROM './data/logs.json'"

# Lance datasets
skardi query --sql "SELECT * FROM './embeddings.lance' LIMIT 5"

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

# With context file
cargo run -p skardi-cli -- query --ctx ./demo/ctx.yaml --sql "SELECT * FROM products LIMIT 5"

# Show schema
skardi query --ctx ./demo/ctx.yaml --schema --all
skardi query --ctx ./demo/ctx.yaml --schema -t products

# SQL from file
skardi query --ctx ./demo/ctx.yaml -f ./queries/report.sql
```
