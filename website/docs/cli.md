---
sidebar_position: 4
---

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

For full details, see [crates/cli/README.md](https://github.com/SkardiLabs/skardi/blob/main/crates/cli/README.md).
