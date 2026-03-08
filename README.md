<div align="center">
<p align="center">

<img src="asset/logo.png" alt="Skardi Logo" width="700">

**The Declarative Data Contract Execution Layer for AI and Agents**<br/>
**Turn SQL queries into live API endpoints — connect CSV, Parquet, PostgreSQL, MySQL, MongoDB, Iceberg, Lance, and S3 in a single query.**

[CI]: https://github.com/SkardiLabs/skardi/actions/workflows/ci.yml
[CI Badge]: https://github.com/SkardiLabs/skardi/actions/workflows/ci.yml/badge.svg

[![CI Badge]][CI]

</p>
</div>

<hr />

Skardi lets you define SQL queries in YAML files and instantly serve them as parameterized HTTP APIs. Connect to multiple data sources, run federated queries across them, and expose the results as REST endpoints — all without writing application code.

> **Warning:** This software is in BETA. It may still contain bugs and unexpected behavior. Use caution with production data and ensure you have backups. Feel free to contact us if you want to have a POC for the product.

## Key Features

- **Declarative pipelines** — Define SQL queries in YAML, get REST APIs automatically
- **Automatic parameter inference** — Request parameters, types, and response schemas are inferred from your SQL
- **Multi-source federation** — JOIN across CSV, Parquet, PostgreSQL, MySQL, MongoDB, Iceberg, and Lance in a single query
- **Full CRUD** — SELECT, INSERT, UPDATE, and DELETE operations on supported databases
- **Vector search** — Native KNN similarity search via Lance integration
- **S3 support** — Read CSV, Parquet, and Lance files directly from S3
- **Docker ready** — Ship as a container with your config files mounted at runtime
- **ONNX inference** — Run ONNX model predictions inline in SQL via the `onnx_predict` UDF
- **CLI for local & remote queries** — Run SQL against local files, remote object stores (S3, GCS, Azure), databases, and datalake formats without starting a server

## Table of Contents

- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Skardi Server](#skardi-server)
  - [Running the Server](#running-the-server)
  - [API Endpoints](#api-endpoints)
  - [Context Files](#context-files)
  - [Access Mode](#access-mode)
  - [In-Memory Caching](#in-memory-caching)
  - [Pipeline Files](#pipeline-files)
  - [Runtime Pipeline Registration](#runtime-pipeline-registration)
- [Skardi CLI](#skardi-cli)
- [Supported Data Sources](#supported-data-sources)
  - [CSV](#csv)
  - [Parquet](#parquet)
  - [PostgreSQL](#postgresql)
  - [MySQL](#mysql)
  - [MongoDB](#mongodb)
  - [Apache Iceberg](#apache-iceberg)
  - [Lance (Vector Search)](#lance-vector-search)
  - [S3 Remote Files](#s3-remote-files)
- [ONNX Model Inference](#onnx-model-inference)
- [Federated Queries](#federated-queries)
- [Docker](#docker)
- [Building from Source](#building-from-source)

## Quick Start

```bash
# Build
cargo build --release

# Start the server with a context and pipeline
cargo run --bin skardi-server -- \
  --ctx demo/ctx.yaml \
  --pipeline demo/pipeline.yaml \
  --port 8080

# Execute the pipeline
curl -X POST http://localhost:8080/product-search-demo/execute \
  -H "Content-Type: application/json" \
  -d '{"brand": null, "max_price": 100.0, "color": null, "limit": 5}'
```

## Architecture

Skardi has two main components:

- **`skardi-server`** — An HTTP server that loads data sources from a **context file**, registers SQL pipelines, and serves them as REST endpoints.
- **`skardi-cli`** (`skardi`) — A command-line tool for running SQL queries against local files, remote object stores, databases, and datalake formats without starting a server.

Both components use [Apache DataFusion](https://datafusion.apache.org/) as the query engine, which enables federated queries across heterogeneous data sources.

## Skardi Server

### Running the Server

```bash
cargo run --bin skardi-server -- \
  --ctx <path-to-ctx.yaml> \
  --pipeline <path-to-pipeline.yaml-or-directory> \
  --port 8080
```

| Flag | Description |
|------|-------------|
| `--ctx` | Path to the context YAML file that defines data sources |
| `--pipeline` | Path to a pipeline YAML file or a directory of pipeline files |
| `--port` | Port to listen on (default: 8080) |

The server can also start without any files and accept pipelines registered at runtime via `POST /register_pipeline`.

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Service health check |
| `/health/:name` | GET | Per-pipeline health check (includes data source status) |
| `/pipelines` | GET | List all registered pipelines |
| `/pipeline/:name` | GET | Get specific pipeline info |
| `/register_pipeline` | POST | Register a new pipeline at runtime |
| `/data_source` | GET | List all data sources |
| `/:name/execute` | POST | Execute a pipeline by name |

### Context Files

A context file (`ctx.yaml`) defines the data sources available to your pipelines. Each data source is registered as a table in the query engine.

```yaml
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

You can define multiple data sources of different types in a single context file:

```yaml
data_sources:
  - name: "campaigns"
    type: "postgres"
    connection_string: "postgresql://localhost:5432/mydb?sslmode=disable"
    options:
      table: "campaigns"
      schema: "public"
      user_env: "PG_USER"
      pass_env: "PG_PASSWORD"

  - name: "content_ads"
    type: "csv"
    path: "data/content_ads.csv"
    options:
      has_header: true
      delimiter: ","
```

### Access Mode

By default, all data sources are **read-only** — only `SELECT` queries are allowed. To enable write operations (`INSERT`, `UPDATE`, `DELETE`), set `access_mode: read_write` on the data source. Only `postgres` and `mysql` sources support `read_write` mode; setting it on other types will produce an error at startup.

```yaml
data_sources:
  - name: "users"
    type: "postgres"
    connection_string: "postgresql://localhost:5432/mydb?sslmode=disable"
    access_mode: read_write    # Enable INSERT/UPDATE/DELETE
    options:
      table: "users"
      user_env: "PG_USER"
      pass_env: "PG_PASSWORD"

  - name: "products"
    type: "csv"
    path: "data/products.csv"
    # access_mode defaults to read_only (CSV doesn't support writes)
```

If a pipeline attempts a write operation on a `read_only` source, the server returns an error:
```
Write operation not allowed on data source 'products'. The data source is configured with 'read_only' access mode.
```

### In-Memory Caching

For file-based sources (`csv`, `parquet`, `iceberg`), you can set `enable_cache: true` to load the entire dataset into memory at startup. This gives significantly faster query performance at the cost of memory usage.

```yaml
data_sources:
  - name: "products"
    type: "csv"
    path: "data/products.csv"
    enable_cache: true          # Load into memory at startup
    options:
      has_header: true
```

This is useful for datasets that are queried frequently and fit in memory. The cache is created once at startup and used for all subsequent queries.

### Pipeline Files

A pipeline file defines a SQL query with parameter placeholders. Parameters are enclosed in `{braces}` and automatically extracted. Types and response schemas are inferred from the SQL and table schemas.

```yaml
metadata:
  name: product-search-demo
  version: 1.0.0
  description: "Product search and filtering"

query: |
  SELECT
    "Name" as product_name,
    "Brand" as brand,
    "Price" as price
  FROM products
  WHERE ({brand} IS NULL OR "Brand" = {brand})
    AND ({max_price} IS NULL OR "Price" < {max_price})
  ORDER BY "Price" ASC
  LIMIT {limit}
```

Execute with:

```bash
curl -X POST http://localhost:8080/product-search-demo/execute \
  -H "Content-Type: application/json" \
  -d '{"brand": "Apple", "max_price": 500.0, "limit": 10}'
```

Use the `{param} IS NULL OR ...` pattern for optional filters — pass `null` to skip a filter.

### Runtime Pipeline Registration

Pipelines can be registered at runtime without restarting the server:

```bash
curl -X POST http://localhost:8080/register_pipeline \
  -H "Content-Type: application/json" \
  -d '{"path": "path/to/pipeline.yaml"}'
```

### Response Format

**Success:**
```json
{
  "success": true,
  "data": [{"product_name": "Laptop", "price": 999.99}],
  "rows": 1,
  "execution_time_ms": 15,
  "timestamp": "2025-01-15T12:00:00.000Z"
}
```

**Error:**
```json
{
  "success": false,
  "error": "Missing required parameters: limit",
  "error_type": "parameter_validation_error",
  "details": {"missing_parameters": ["limit"]},
  "timestamp": "2025-01-15T12:00:00.000Z"
}
```

## Skardi CLI

The CLI lets you run SQL queries against local files, remote object stores, databases, and datalake formats — no server required.

### Install

```bash
cargo install --path crates/cli
```

### Usage

```bash
# Query files directly by path (no context file needed)
skardi query --sql "SELECT * FROM './data/products.csv' LIMIT 10"
skardi query --sql "SELECT * FROM 's3://mybucket/events.parquet' LIMIT 10"
skardi query --sql "SELECT * FROM './embeddings.lance' LIMIT 5"

# Query with a context file (for databases, named tables, etc.)
skardi query --ctx ./ctx.yaml --sql "SELECT * FROM products LIMIT 10"

# SQL from file
skardi query --ctx ./ctx.yaml --file query.sql

# Show table schemas
skardi query --ctx ./ctx.yaml --schema --all
skardi query --ctx ./ctx.yaml --schema -t products
```

**Supported sources:**

| Category | Types |
|----------|-------|
| Local files | CSV, Parquet, JSON/NDJSON, Lance |
| Remote stores | S3, GCS, Azure Blob, HTTP/HTTPS, OSS, COS |
| Datalake formats | Lance, Iceberg |
| Databases | PostgreSQL, MySQL, MongoDB |

**Context file resolution** (when `--ctx` is omitted): checks `SKARDICONFIG` env var, then `~/.skardi/config/ctx.yaml`. If no context file is found, the query runs without pre-registered tables (you can still query files directly by path).

For full details, see [crates/cli/README.md](crates/cli/README.md).

## Supported Data Sources

### CSV

```yaml
- name: "products"
  type: "csv"
  path: "data/products.csv"
  options:
    has_header: true
    delimiter: ","
    schema_infer_max_records: 1000
```

### Parquet

```yaml
- name: "events"
  type: "parquet"
  path: "data/events.parquet"
```

### PostgreSQL

Full CRUD support (SELECT, INSERT, UPDATE, DELETE) with federated query capability.

```yaml
- name: "users"
  type: "postgres"
  connection_string: "postgresql://localhost:5432/mydb?sslmode=disable"
  options:
    table: "users"
    schema: "public"          # Optional, default: "public"
    user_env: "PG_USER"       # Env var for username
    pass_env: "PG_PASSWORD"   # Env var for password
```

```bash
export PG_USER="myuser"
export PG_PASSWORD="mypassword"
```

For detailed setup, CRUD examples, and federated queries, see [demo/postgres/POSTGRES_DEMO.md](demo/postgres/POSTGRES_DEMO.md).

### MySQL

Full CRUD support (SELECT, INSERT, UPDATE, DELETE) with federated query capability.

```yaml
- name: "users"
  type: "mysql"
  connection_string: "mysql://localhost:3306/mydb"
  options:
    table: "users"
    user_env: "MYSQL_USER"
    pass_env: "MYSQL_PASSWORD"
```

```bash
export MYSQL_USER="myuser"
export MYSQL_PASSWORD="mypassword"
```

For detailed setup, CRUD examples, and federated queries, see [demo/mysql/MYSQL_DEMO.md](demo/mysql/MYSQL_DEMO.md).

### MongoDB

Full CRUD support with point lookups, full scans, and federated queries.

```yaml
- name: "products"
  type: "mongo"
  connection_string: "mongodb://localhost:27017"
  options:
    database: "mydb"
    collection: "products"
    primary_key: "product_id"
    user_env: "MONGO_USER"
    pass_env: "MONGO_PASS"
```

```bash
export MONGO_USER="myuser"
export MONGO_PASS="mypassword"
```

For detailed setup, CRUD examples, and federated queries, see [demo/mongo/MONGO_DEMO.md](demo/mongo/MONGO_DEMO.md).

### Apache Iceberg

Query Iceberg tables with support for schema evolution, partition pruning, and time travel.

```yaml
- name: "nyc_taxi"
  type: "iceberg"
  path: "/path/to/iceberg-warehouse"
  options:
    namespace: "nyc"
    table: "trips"
```

For S3-backed Iceberg tables:

```yaml
- name: "s3_iceberg"
  type: "iceberg"
  path: "s3://my-bucket/iceberg-warehouse"
  options:
    namespace: "production"
    table: "events"
    aws_region: "us-east-1"
    aws_access_key_id_env: "AWS_ACCESS_KEY_ID"
    aws_secret_access_key_env: "AWS_SECRET_ACCESS_KEY"
```

For detailed setup and examples, see [demo/iceberg/ICEBERG_DEMO.md](demo/iceberg/ICEBERG_DEMO.md).

### Lance (Vector Search)

Native KNN (K-Nearest Neighbors) similarity search using the `lance_knn` table function.

```yaml
- name: "sift_items"
  type: "lance"
  path: "data/vec_data.lance/"
  description: "Vector embeddings"
```

Query with the `lance_knn` table function:

```sql
SELECT knn.id, knn.item_id, knn._distance
FROM lance_knn(
  'sift_items',          -- table name
  'vector',              -- vector column
  (SELECT vector FROM sift_items WHERE id = {ref_id}),  -- query vector
  {k}                    -- number of neighbors
) knn
WHERE knn.id != {ref_id}
```

| Dataset Size | Without Optimization | With Lance KNN | Speedup |
|--------------|---------------------|----------------|---------|
| 10K vectors  | ~50ms              | ~5ms           | 10x     |
| 100K vectors | ~500ms             | ~8ms           | 62x     |
| 1M vectors   | ~5000ms            | ~15ms          | 333x    |

For full details on vector search, see [demo/lance/LANCE_DEMO.md](demo/lance/LANCE_DEMO.md).

### S3 Remote Files

Read CSV, Parquet, and Lance files from S3. Credentials are loaded from environment variables — never from config files.

```yaml
- name: "sales_data"
  type: "parquet"
  location: "remote_s3"
  path: "s3://my-bucket/sales/data.parquet"
  description: "Sales data in S3"
```

Authentication methods: environment variables, AWS CLI profiles, IAM roles, or AWS SSO.

```bash
export AWS_ACCESS_KEY_ID="your_key"
export AWS_SECRET_ACCESS_KEY="your_secret"
# Or use: export AWS_PROFILE="your_profile"
```

For full S3 configuration, IAM permissions, and troubleshooting, see [demo/S3_USAGE.md](demo/S3_USAGE.md).

## ONNX Model Inference

Run ONNX model predictions directly in SQL using the built-in `onnx_predict` scalar UDF. Models are loaded lazily on first use and cached in memory.

```sql
onnx_predict('path/to/model.onnx', input1, input2, ...) -> FLOAT
```

- First argument: path to an `.onnx` file (relative to the server's working directory)
- Remaining arguments: model inputs (types are auto-detected from the ONNX model)
- Returns: `FLOAT` per row, or `LIST(FLOAT)` when inputs are aggregated lists

Example — score candidates with a Neural Collaborative Filtering model:

```sql
SELECT
  item_id,
  onnx_predict('models/ncf.onnx',
    CAST({user_id} AS BIGINT),
    CAST(item_id AS BIGINT)
  ) AS score
FROM candidates
ORDER BY score DESC
LIMIT 10
```

Pre-built models are available in the `models/` directory (`ncf.onnx`, `TinyTimeMixer.onnx`).

For the full guide including the movie recommendation demo, see [demo/onnx_predict/ONNX_PREDICT_DEMO.md](demo/onnx_predict/ONNX_PREDICT_DEMO.md).

## Federated Queries

One of Skardi's most powerful features is the ability to JOIN data across different source types in a single SQL query. DataFusion handles the federation transparently.

Example: Join a CSV file with a PostgreSQL table and write results back to PostgreSQL:

```yaml
metadata:
  name: "federated_join_and_insert"
  version: "1.0"

query: |
  INSERT INTO user_order_stats (user_id, user_name, total_orders, total_spent)
  SELECT
    u.id as user_id,
    u.name as user_name,
    COUNT(o.order_id) as total_orders,
    SUM(o.amount) as total_spent
  FROM users u                    -- PostgreSQL table
  INNER JOIN csv_orders o         -- CSV file
    ON u.id = o.user_id
  WHERE u.name = {name}
  GROUP BY u.id, u.name
```

## Docker

### Build the image

```bash
docker build -t skardi .
```

### Run with config files mounted

```bash
docker run --rm \
  -v /path/to/your/ctx.yaml:/config/ctx.yaml \
  -v /path/to/your/pipeline.yaml:/config/pipeline.yaml \
  -p 8080:8080 \
  skardi \
  --ctx /config/ctx.yaml \
  --pipeline /config/pipeline.yaml \
  --port 8080
```

Mount an entire directory of pipeline files:

```bash
docker run --rm \
  -v /path/to/your/ctx.yaml:/config/ctx.yaml \
  -v /path/to/your/pipelines:/config/pipelines \
  -p 8080:8080 \
  skardi \
  --ctx /config/ctx.yaml \
  --pipeline /config/pipelines \
  --port 8080
```

Start without config and register pipelines at runtime:

```bash
docker run --rm -p 8080:8080 skardi
```

## Building from Source

```bash
# Clone the repository
git clone https://github.com/BtXin/skardi.git
cd skardi

# Build server
cargo build --release -p skardi-server

# Build CLI
cargo build --release -p skardi-cli

# Or install CLI globally
cargo install --path crates/cli
```

## Demo & Examples

The [demo/](demo/) directory contains complete working examples:

| Directory | Description |
|-----------|-------------|
| [demo/README.md](demo/README.md) | Product search demo (CSV/Parquet) |
| [demo/postgres/](demo/postgres/) | PostgreSQL CRUD and federated query examples |
| [demo/mysql/](demo/mysql/) | MySQL CRUD and federated query examples |
| [demo/mongo/](demo/mongo/) | MongoDB CRUD and federated query examples |
| [demo/iceberg/](demo/iceberg/) | Apache Iceberg integration examples |
| [demo/lance/](demo/lance/) | Lance vector search examples |
| [demo/onnx_predict/](demo/onnx_predict/) | ONNX model inference in SQL |
| [demo/S3_USAGE.md](demo/S3_USAGE.md) | S3 data source configuration guide |

## License

See [LICENSE](LICENSE) for details.
