<div align="center">
<p align="center">

<img src="asset/logo.png" alt="Skardi Logo" width="700">

**SQL across anything: query, join, and aggregate over local files, databases, S3, and vector stores — or turn any SQL into a parameterized HTTP API, zero application code required, written in Rust, powered by Apache Datafusion.**

[CI]: https://github.com/SkardiLabs/skardi/actions/workflows/ci.yml
[CI Badge]: https://github.com/SkardiLabs/skardi/actions/workflows/ci.yml/badge.svg
[crates.io]: https://crates.io/crates/skardi
[crates.io Badge]: https://img.shields.io/crates/v/skardi.svg
[Docs]: https://docs.rs/skardi
[Docs Badge]: https://docs.rs/skardi/badge.svg
[Discord]: https://discord.gg/S5YQQPEV2m
[Discord Badge]: https://img.shields.io/discord/1234567890?label=Discord&logo=discord&logoColor=white

[![CI Badge]][CI]
[![crates.io Badge]][crates.io]
[![Docs Badge]][Docs]
[![Discord Badge]][Discord]

</p>
</div>

<hr />

Skardi runs federated SQL across files, databases, object stores, and vector stores — and turns any query into a parameterized REST API with no application code.

- **`skardi-cli`** — Run SQL queries locally against files, object stores, databases, and datalake formats. Ideal for local agents like [OpenClaw](https://github.com/openclaw/openclaw) that need structured data access without a running server.
- **`skardi-server`** — Define SQL queries in YAML and serve them as parameterized HTTP APIs. Connect to multiple data sources, run federated queries, and expose results as REST endpoints.

> **⚠️Warning:** This software is in BETA. It may still contain bugs and unexpected behavior. Use caution with production data and ensure you have backups. Feel free to contact us if you want to have a POC for the product.

## Key Features

- **CLI for local agents & queries** — Run SQL against local files, remote object stores (S3, GCS, Azure), databases, and datalake formats — ideal for local AI agents like [OpenClaw](https://github.com/openclaw/openclaw)
- **Declarative pipelines** — Define SQL queries in YAML, get REST APIs automatically
- **Automatic parameter inference** — Request parameters, types, and response schemas are inferred from your SQL
- **Multi-source federation** — JOIN across CSV, Parquet, PostgreSQL, MySQL, SQLite, MongoDB, Redis, Iceberg, and Lance in a single query
- **Full CRUD** — SELECT, INSERT, UPDATE, and DELETE operations on supported databases
- **Vector search** — Native KNN similarity search via Lance integration, or via PG vector.
- **Full-text search** — BM25-scored full-text search via Lance inverted indexes
- **S3 support** — Read CSV, Parquet, and Lance files directly from S3
- **Docker ready** — Ship as a container with your config files mounted at runtime
- **ONNX inference** — Run ONNX model predictions inline in SQL via the `onnx_predict` UDF (requires `--features onnx`)

## Table of Contents

- [Cloud (Sealos)](#cloud-sealos)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Skardi CLI](#skardi-cli)
- [Skardi Server](#skardi-server)
  - [Running the Server](#running-the-server)
  - [Dashboard](#dashboard)
  - [API Endpoints](#api-endpoints)
  - [Context Files](#context-files)
  - [Access Mode](#access-mode)
  - [In-Memory Caching](#in-memory-caching)
  - [Pipeline Files](#pipeline-files)
- [Supported Data Sources](#supported-data-sources)
  - [CSV](#csv)
  - [Parquet](#parquet)
  - [PostgreSQL](#postgresql)
  - [MySQL](#mysql)
  - [SQLite](#sqlite)
  - [MongoDB](#mongodb)
  - [Redis](#redis)
  - [Apache Iceberg](#apache-iceberg)
  - [Lance (Vector Search & Full-Text Search)](#lance-vector-search--full-text-search)
  - [S3 Remote Files](#s3-remote-files)
- [ONNX Model Inference](#onnx-model-inference)
- [Federated Queries](#federated-queries)
- [Observability](#observability)
- [Docker](#docker)
- [Building from Source](#building-from-source)
- [Community](#community)

## Cloud (Sealos)

The fastest way to get started is with **[skardi-skills](https://github.com/SkardiLabs/skardi-skills)** — a collection of ready-to-deploy Skardi templates for [Sealos](https://sealos.io). Launch a fully configured Skardi server in the cloud with one click, no local setup required.

## Installation

### Docker (GHCR)

Pre-built Docker images are published to GitHub Container Registry on every release.

```bash
# Default image
docker pull ghcr.io/skardilabs/skardi/skardi-server:latest

# With ONNX inference support
docker pull ghcr.io/skardilabs/skardi/skardi-server-onnx:latest

# Pull a specific version
docker pull ghcr.io/skardilabs/skardi/skardi-server:0.1.0
docker pull ghcr.io/skardilabs/skardi/skardi-server-onnx:0.1.0
```

### CLI Binary

Download the latest CLI binary for your platform:

```bash
curl -fSL "https://github.com/SkardiLabs/skardi/releases/latest/download/skardi-$(uname -m | sed 's/arm64/aarch64/')-$(uname -s | sed 's/Linux/unknown-linux-gnu/' | sed 's/Darwin/apple-darwin/').tar.gz" | tar xz
sudo mv skardi /usr/local/bin/
```

Or download manually from the [Releases](https://github.com/SkardiLabs/skardi/releases) page. Available targets:

| Platform | Target |
|----------|--------|
| Linux x86_64 | `skardi-x86_64-unknown-linux-gnu.tar.gz` |
| Linux ARM64 | `skardi-aarch64-unknown-linux-gnu.tar.gz` |
| macOS ARM64 (Apple Silicon) | `skardi-aarch64-apple-darwin.tar.gz` |

> **Note:** macOS Intel (x86_64) binaries are not provided. Apple no longer produces Intel-based Macs. You can [build from source](#building-from-source) if needed.

## Quick Start

```bash
# Build
cargo build --release

# --- Skardi CLI ---
# Query local files directly
skardi query --sql "SELECT * FROM './data/products.csv' LIMIT 10"

# Query remote files
skardi query --sql "SELECT * FROM 's3://mybucket/events.parquet' LIMIT 10"

# --- Skardi Server ---
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

- **`skardi-cli`** (`skardi`) — A command-line tool for running SQL queries against local files, remote object stores, databases, and datalake formats without starting a server. Perfect for powering local AI agents like [OpenClaw](https://github.com/openclaw/openclaw) with structured data access.
- **`skardi-server`** — An HTTP server that loads data sources from a **context file**, registers SQL pipelines, and serves them as REST endpoints.

Both components use [Apache DataFusion](https://datafusion.apache.org/) as the query engine, which enables federated queries across heterogeneous data sources.

## Skardi CLI

The CLI lets you run SQL queries against local files, remote object stores, databases, and datalake formats — no server required. It's a great fit for local AI agents like [OpenClaw](https://github.com/openclaw/openclaw) that need to query structured data on the fly.

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
| Databases | PostgreSQL, MySQL, SQLite, MongoDB, Redis |

**Context file resolution** (when `--ctx` is omitted): checks `SKARDICONFIG` env var, then `~/.skardi/config/ctx.yaml`. If no context file is found, the query runs without pre-registered tables (you can still query files directly by path).

For full details, see [crates/cli/README.md](crates/cli/README.md).

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

### Dashboard

Once the server is running, open `http://localhost:8080` in your browser to access the pipeline dashboard.

The dashboard lists every registered pipeline as a card showing:
- **Endpoint URL** — the `POST` path to call, with a one-click copy button
- **Parameters** — inferred parameter names and types from the pipeline SQL
- **Example request** — a ready-to-run `curl` command for the pipeline
- **Try It** — an interactive panel where you can edit the JSON body and execute the pipeline directly from the browser

No configuration required — the dashboard is built into `skardi-server` and updates automatically when pipelines are loaded.

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Pipeline dashboard UI |
| `/health` | GET | Service health check |
| `/health/:name` | GET | Per-pipeline health check (includes data source status) |
| `/pipelines` | GET | List all registered pipelines |
| `/pipeline/:name` | GET | Get specific pipeline info |
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
    path: "demo/sample_data/orders.csv"
    options:
      has_header: true
      delimiter: ","
```

### Access Mode

By default, all data sources are **read-only** — only `SELECT` queries are allowed. To enable write operations (`INSERT`, `UPDATE`, `DELETE`), set `access_mode: read_write` on the data source. Only `postgres`, `mysql`, `sqlite`, `mongo`, and `redis` sources support `read_write` mode; setting it on other types will produce an error at startup.

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

For detailed setup, CRUD examples, and federated queries, see [demo/postgres/README.md](demo/postgres/README.md).

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

For detailed setup, CRUD examples, and federated queries, see [demo/mysql/README.md](demo/mysql/README.md).

### SQLite

Full CRUD support (SELECT, INSERT, UPDATE, DELETE) with no external server required — just a local `.db` file.

```yaml
- name: "users"
  type: "sqlite"
  path: "data/my_database.db"
  options:
    table: "users"
    busy_timeout_ms: "5000"     # Optional, default: 5000
```

SQLite requires no credentials — just the path to the database file.

**CLI direct path query** (no context file needed):
```bash
skardi query --sql "SELECT * FROM './data/my_database.db.users'"
```

For detailed setup, CRUD examples, and federated queries, see [demo/sqlite/README.md](demo/sqlite/README.md).

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

For detailed setup, CRUD examples, and federated queries, see [demo/mongo/README.md](demo/mongo/README.md).

### Redis

Full CRUD support with point lookups (O(1) via direct key construction), full scans, and federated queries. Redis hashes map directly to SQL rows.

```yaml
- name: "products"
  type: "redis"
  connection_string: "redis://localhost:6379"
  options:
    key_space: "mydb"
    table: "products"
    key_column: "product_id"
```

Redis keys follow the pattern `{key_space}:{table}:{key_column_value}`, where `key_column` is extracted from the key suffix and exposed as a SQL column. For initially empty tables, use the `columns` option to declare the schema upfront so INSERT operations work immediately.

For detailed setup, CRUD examples, and federated queries, see [demo/redis/README.md](demo/redis/README.md).

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

For detailed setup and examples, see [demo/iceberg/README.md](demo/iceberg/README.md).

### Lance (Vector Search & Full-Text Search)

Native KNN (K-Nearest Neighbors) similarity search using the `lance_knn` table function, and BM25-scored full-text search using the `lance_fts` table function.

```yaml
- name: "sift_items"
  type: "lance"
  path: "data/vec_data.lance/"
  description: "Vector embeddings"
```

#### Vector Search (lance_knn)

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

#### Full-Text Search (lance_fts)

```sql
-- Basic term search (BM25 scored)
SELECT id, description, _score
FROM lance_fts('my_table', 'description', 'search terms', 10)

-- Phrase search
SELECT * FROM lance_fts('my_table', 'description', '"exact phrase"', 10)

-- With WHERE clause filter pushdown
SELECT * FROM lance_fts('my_table', 'description', 'search terms', 10)
WHERE category = 'food' AND price < 20
```

Requires a Lance INVERTED index on the text column. See [demo/lance/README.md](demo/lance/README.md) for full details on vector search and full-text search.

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

> **Note:** ONNX support is behind a feature flag. Build with `--features onnx` to enable it:
> ```bash
> cargo build --release -p skardi-server --features onnx
> ```

Run ONNX model predictions directly in SQL using the `onnx_predict` scalar UDF. Models are loaded lazily on first use and cached in memory.

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

For the full guide including the movie recommendation demo, see [demo/onnx_predict/README.md](demo/onnx_predict/README.md).

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

## Observability

`skardi-server` exports traces and metrics via [OpenTelemetry](https://opentelemetry.io/) (OTLP gRPC), giving you full visibility into query execution inside Grafana.

### What is instrumented

- **Traces** — Every DataFusion execution plan node is wrapped with a span. You can see `elapsed_compute`, `output_rows`, spill counts, and optimizer rule timings per query.
- **Pipeline metrics** — Per-pipeline request count, latency, and error rate exported via OTLP on every `/execute` call.
- **Logs** — Structured logs forwarded to Loki via the OTel Collector.

### Pipeline metrics

Every call to `/:name/execute` records two OTel metrics:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `pipeline_requests_total` | Counter | `pipeline`, `status` (`success`/`error`), `error_type` | Total requests by outcome |
| `pipeline_latency_ms` | Histogram | `pipeline` | End-to-end handler latency in milliseconds |

**Verify metrics are flowing** — the OTel Collector exposes a Prometheus scrape endpoint at `:8889`. After firing at least one request, run:

```bash
curl -s http://localhost:8889/metrics | grep pipeline
```

Expected output:
```
pipeline_latency_ms_bucket{pipeline="my-pipeline",le="..."} 2
pipeline_latency_ms_count{pipeline="my-pipeline"} 2
pipeline_latency_ms_sum{pipeline="my-pipeline"} 45.3
pipeline_requests_total{pipeline="my-pipeline",status="success"} 1
pipeline_requests_total{pipeline="my-pipeline",status="error",error_type="parameter_validation_error"} 1
```

> **Note:** The periodic exporter flushes every 60 seconds. If the output is empty, wait a moment and retry.

Example PromQL queries for Grafana dashboards:

```promql
# Requests per second per pipeline
rate(pipeline_requests_total[5m])

# Error rate (fraction of failing requests)
rate(pipeline_requests_total{status="error"}[5m])
  / rate(pipeline_requests_total[5m])

# p99 latency per pipeline
histogram_quantile(0.99, rate(pipeline_latency_ms_bucket[5m]))

# p50 latency per pipeline
histogram_quantile(0.50, rate(pipeline_latency_ms_bucket[5m]))
```

### Local observability stack

A `docker-compose.yml` is included with a pre-configured Grafana stack:

| Service | Role | Port |
|---------|------|------|
| OTel Collector | Receives OTLP from the server, fans out to backends | 4317 (gRPC), 4318 (HTTP) |
| Grafana Tempo | Trace storage | — |
| Prometheus | Metric storage (scrapes collector at :8889) | 9090 |
| Grafana Loki | Log storage | — |
| Grafana | Visualization (Tempo + Prometheus + Loki pre-provisioned) | 3000 |

```bash
# Start the stack
docker-compose -f observability/docker-compose.yml up -d

# Run the server pointing at the collector
OTLP_ENDPOINT=http://localhost:4317 RUST_LOG=debug cargo run -p skardi-server -- --port 8080
```

Then open Grafana at **http://localhost:3000** — all three datasources (Tempo, Prometheus, Loki) are pre-provisioned.

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OTLP_ENDPOINT` | `http://localhost:4317` | OTLP gRPC endpoint to export to |
| `RUST_LOG` | `info` | Log level. Use `debug` to see per-query DataFusion span detail |

### Log levels

- `RUST_LOG=info` — normal production operation; high-level server events only
- `RUST_LOG=debug` — shows per-query DataFusion execution spans (plan nodes, row counts, optimizer timing)

## Docker

### Build the image

```bash
docker build -t skardi .

# With ONNX support
docker build -t skardi --build-arg FEATURES=onnx .
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

## Building from Source

```bash
# Clone the repository
git clone https://github.com/SkardiLabs/skardi.git
cd skardi

# Build CLI
cargo build --release -p skardi-cli

# Or install CLI globally
cargo install --path crates/cli

# Build server
cargo build --release -p skardi-server

# Build server with ONNX model inference support
cargo build --release -p skardi-server --features onnx
```

## Demo & Examples

The [demo/](demo/) directory contains complete working examples:

| Directory | Description |
|-----------|-------------|
| [demo/README.md](demo/README.md) | Product search demo (CSV/Parquet) |
| [demo/postgres/](demo/postgres/) | PostgreSQL CRUD and federated query examples |
| [demo/mysql/](demo/mysql/) | MySQL CRUD and federated query examples |
| [demo/sqlite/](demo/sqlite/) | SQLite CRUD and federated query examples |
| [demo/mongo/](demo/mongo/) | MongoDB CRUD and federated query examples |
| [demo/redis/](demo/redis/) | Redis CRUD and federated query examples |
| [demo/iceberg/](demo/iceberg/) | Apache Iceberg integration examples |
| [demo/lance/](demo/lance/) | Lance vector search and full-text search examples |
| [demo/onnx_predict/](demo/onnx_predict/) | ONNX model inference in SQL |
| [demo/S3_USAGE.md](demo/S3_USAGE.md) | S3 data source configuration guide |

## Community

Have questions, ideas, or want to share what you're building with Skardi? Join us on [Discord](https://discord.gg/S5YQQPEV2m)!

## License

See [LICENSE](LICENSE) for details.
