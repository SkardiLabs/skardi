---
sidebar_position: 1
---

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

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
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
