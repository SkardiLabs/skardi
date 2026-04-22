# PostgreSQL Integration

This guide covers how to integrate PostgreSQL tables with Skardi, including INSERT, UPDATE, DELETE operations and federated queries with CSV data.

## Quick Start (Docker)

For the fastest setup, use Docker:

```bash
# 1. Start PostgreSQL in Docker
docker run --name postgres-skardi \
  -e POSTGRES_DB=mydb \
  -e POSTGRES_USER=skardi_user \
  -e POSTGRES_PASSWORD=skardi_pass \
  -p 5432:5432 \
  -d postgres:16

# 2. Create test data
docker exec -i postgres-skardi psql -U skardi_user -d mydb << 'EOF'
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL
);
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    product VARCHAR(100) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL
);
CREATE TABLE user_order_stats (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(100),
    user_email VARCHAR(100),
    total_orders INT,
    total_spent DECIMAL(10, 2),
    last_order_date VARCHAR(50)
);
INSERT INTO users (name, email) VALUES
    ('Alice Smith', 'alice@example.com'),
    ('Bob Johnson', 'bob@example.com'),
    ('Carol Williams', 'carol@example.com');
INSERT INTO orders (user_id, product, amount) VALUES
    (1, 'Laptop', 999.99),
    (2, 'Keyboard', 79.99),
    (3, 'Monitor', 299.99);
EOF

# 3. Set environment variables
export PG_USER="skardi_user"
export PG_PASSWORD="skardi_pass"

# 4. Create a pipeline for querying
cat > /tmp/pg_query_pipeline.yaml << 'EOF'
name: "pg_user_query"
version: "1.0"
query:
  sql: "SELECT * FROM users WHERE id = {user_id}"
EOF

# 5. Start Skardi
cargo run --bin skardi-server -- \
  --ctx docs/postgres/ctx_postgres_demo.yaml \
  --pipeline /tmp/pg_query_pipeline.yaml \
  --port 8080

# 6. Execute with parameters
curl -X POST http://localhost:8080/pg_user_query/execute \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1}' | jq .
```

## Prerequisites

1. **PostgreSQL Server** running locally or remotely
2. **PostgreSQL Database** with test tables

## Running the Example

1. **Set environment variables**:
   ```bash
   export PG_USER="skardi_user"
   export PG_PASSWORD="skardi_pass"
   ```

2. **Start Skardi server with pipelines**:

   Example pipeline files are provided in `docs/postgres/pipelines/`:
   - `query_user_by_id.yaml` - Query user by ID
   - `insert_user.yaml` - Insert new user
   - `update_user_email.yaml` - Update a user's email by name
   - `delete_user.yaml` - Delete a user by name
   - `federated_join_and_insert.yaml` - Join CSV + PostgreSQL and write results

   Pass them all at server start using the `--pipeline` flag (accepts a directory or individual files):
   ```bash
   cargo run --bin skardi-server -- \
     --ctx docs/postgres/ctx_postgres_demo.yaml \
     --pipeline docs/postgres/pipelines/ \
     --port 8080
   ```

3. **Execute pipelines**:

   ```bash
   # Execute with parameters
   curl -X POST http://localhost:8080/query_user_by_id/execute \
     -H "Content-Type: application/json" \
     -d '{"user_id": 1}' | jq .
   ```

## Single INSERT Example

Insert a new user into the PostgreSQL table:

```bash
# Execute INSERT with parameters (pipeline was loaded at server start)
curl -X POST http://localhost:8080/insert_user/execute \
  -H "Content-Type: application/json" \
  -d '{"name": "David Brown", "email": "david@example.com"}' | jq .
```

**Verify the insert:**
```bash
docker exec postgres-skardi psql -U skardi_user -d mydb \
  -c "SELECT * FROM users"
```

## UPDATE Example

Update an existing user's email address:

```bash
# Execute UPDATE with parameters
curl -X POST http://localhost:8080/update_user_email/execute \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice Smith", "new_email": "alice.smith@newdomain.com"}' | jq .
```

**Response:**
```json
{
  "data": [{"count": 1}],
  "execution_time_ms": 12,
  "rows": 1,
  "success": true
}
```

The `count` field reports the number of rows affected. A value of `0` means no row matched the `WHERE` clause.

**Verify the update:**
```bash
docker exec postgres-skardi psql -U skardi_user -d mydb \
  -c "SELECT * FROM users WHERE name = 'Alice Smith'"
```

**Update multiple columns at once** by extending the pipeline SQL:
```sql
UPDATE users SET email = {new_email}, name = {new_name} WHERE name = {name}
```

## DELETE Example

Delete a user by name:

```bash
# Execute DELETE with parameters
curl -X POST http://localhost:8080/delete_user/execute \
  -H "Content-Type: application/json" \
  -d '{"name": "David Brown"}' | jq .
```

**Response:**
```json
{
  "data": [{"count": 1}],
  "execution_time_ms": 8,
  "rows": 1,
  "success": true
}
```

The `count` field reports the number of rows deleted. A value of `0` means no row matched the `WHERE` clause.

**Verify the delete:**
```bash
docker exec postgres-skardi psql -U skardi_user -d mydb \
  -c "SELECT * FROM users"
```

> **Note:** Omitting the `WHERE` clause deletes all rows in the table. Always double-check your filter parameters before executing a DELETE pipeline against production data.

## Federated Query Example: Join CSV + PostgreSQL

This example demonstrates **joining data from multiple sources** (CSV file + PostgreSQL table) with a **parameterized filter** and writing the aggregated results back to PostgreSQL.

### What This Does

```
CSV File (orders.csv)         PostgreSQL (users table)
8 rows of order data    +     3 rows of user data
         │                             │
         └─────────┬───────────────────┘
                   │
              DataFusion
        WHERE u.name = {name}
           JOIN + Aggregate
                   │
                   ▼
      PostgreSQL (user_order_stats)
     Aggregated statistics for filtered user
```

### Shared CSV Data Source

This guide uses the same CSV file as the MySQL guide (`docs/sample_data/orders.csv`):

```csv
order_id,user_id,product,amount,order_date
1001,1,Laptop,999.99,2024-01-15
1002,1,Mouse,29.99,2024-01-16
1003,2,Keyboard,79.99,2024-01-17
1004,3,Monitor,299.99,2024-01-18
1005,1,USB Cable,9.99,2024-01-19
1006,2,Headphones,149.99,2024-01-20
1007,3,Webcam,89.99,2024-01-21
1008,2,Mousepad,19.99,2024-01-22
```

### Execute the Federated Query

```bash
# Execute for Alice Smith
curl -X POST http://localhost:8080/federated_join_and_insert/execute \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice Smith"}' | jq .
```

**Response:**
```json
{
  "data": [{"count": 1}],
  "execution_time_ms": 45,
  "rows": 1,
  "success": true
}
```

### Query Multiple Users

```bash
# Execute for Bob Johnson
curl -X POST http://localhost:8080/federated_join_and_insert/execute \
  -H "Content-Type: application/json" \
  -d '{"name": "Bob Johnson"}' | jq .

# Execute for Carol Williams
curl -X POST http://localhost:8080/federated_join_and_insert/execute \
  -H "Content-Type: application/json" \
  -d '{"name": "Carol Williams"}' | jq .
```

### Verify Results

```bash
docker exec postgres-skardi psql -U skardi_user -d mydb \
  -c "SELECT * FROM user_order_stats"
```

**Output (after running all three users):**
```
 user_id |   user_name    |    user_email     | total_orders | total_spent | last_order_date
---------+----------------+-------------------+--------------+-------------+-----------------
       1 | Alice Smith    | alice@example.com |            3 |     1039.97 | 2024-01-19
       2 | Bob Johnson    | bob@example.com   |            3 |      249.97 | 2024-01-22
       3 | Carol Williams | carol@example.com |            2 |      389.98 | 2024-01-21
```

**What Happened:**
1. 📊 Read orders from CSV file
2. 🔍 Filtered users by the `name` parameter
3. 👥 Joined with matching user from PostgreSQL
4. 📈 Aggregated: COUNT orders, SUM amounts, MAX date for that user
5. 💾 Wrote aggregated row to PostgreSQL

## pgvector: KNN Similarity Search

Skardi supports [pgvector](https://github.com/pgvector/pgvector) tables via the `pg_knn()` table function. Any Postgres data source that has a `vector` column is automatically available for KNN search — no extra configuration needed.

### Setup

```bash
# 1. Start PostgreSQL with pgvector
docker run --name postgres-pgvector \
  -e POSTGRES_DB=mydb \
  -e POSTGRES_USER=skardi_user \
  -e POSTGRES_PASSWORD=skardi_pass \
  -p 5432:5432 \
  -d pgvector/pgvector:pg16

# 2. Create a table with a vector column and HNSW index
docker exec -i postgres-pgvector psql -U skardi_user -d mydb << 'EOF'
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE documents (
    id BIGSERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    metadata TEXT,
    embedding vector(4)   -- use your actual dimension (e.g. 1536 for OpenAI)
);

-- One HNSW index per operator class (each metric requires its own index)
CREATE INDEX ON documents USING hnsw (embedding vector_ip_ops)     WITH (m = 16, ef_construction = 64);
CREATE INDEX ON documents USING hnsw (embedding vector_l2_ops)     WITH (m = 16, ef_construction = 64);
CREATE INDEX ON documents USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);

-- Vectors are designed so each metric returns a different ranking (see "Start and query" below).
-- doc-2 shares direction with doc-1 but has 5× larger magnitude:
--   <#>  ranks it first  (high dot product)
--   <->  ranks it last   (far in Euclidean space)
--   <=>  ties it with doc-1 (direction is identical)
INSERT INTO documents (content, metadata, embedding) VALUES
    ('Rust systems programming',                    'doc-1', '[0.6, 0.8, 0.0, 0.0]'),
    ('Systems programming at scale with Rust',      'doc-2', '[3.0, 4.0, 0.0, 0.0]'),
    ('Introduction to programming languages',       'doc-3', '[0.5, 0.5, 0.5, 0.5]'),
    ('Database query optimization',                 'doc-4', '[0.0, 0.0, 0.6, 0.8]');
EOF

# 3. Set credentials
export PG_USER="skardi_user"
export PG_PASSWORD="skardi_pass"
```

### Context file

Add `documents` as a regular Postgres data source — `pg_knn` discovers the vector column automatically:

```yaml
# ctx_pgvector_demo.yaml

kind: context

metadata:
  name: example-context
  version: 1.0.0

spec:
  data_sources:
    - name: "documents"
      type: "postgres"
      connection_string: "postgresql://localhost:5432/mydb?sslmode=disable"
      options:
        table: "documents"
        schema: "public"
        user_env: "PG_USER"
        pass_env: "PG_PASSWORD"
```

### Pipelines

Three pipeline files are provided in `docs/postgres/pipelines/vector_demo/`, one per distance metric:

| File | Operator | Score meaning |
|---|---|---|
| `vector_search_inner_product.yaml` | `<#>` | Negative inner product — lower (more negative) is more similar |
| `vector_search_l2.yaml` | `<->` | Euclidean distance — lower is more similar |
| `vector_search_cosine.yaml` | `<=>` | Cosine distance in [0, 2] — lower is more similar |

Each pipeline finds documents similar to a seed document using its stored embedding as the query vector:

```yaml
# vector_search_cosine.yaml
query: |
  SELECT id, content, metadata, _score
  FROM pg_knn('documents', 'embedding',
      (SELECT embedding FROM documents WHERE id = {seed_id}),
      '<=>', {k})
  ORDER BY _score
  LIMIT {limit}
```

Each pipeline accepts three parameters: `seed_id` (the document to use as the query vector), `k` (how many candidates `pg_knn` pulls from the ANN index), and `limit` (how many rows to return after the outer `ORDER BY`). Typically `k >= limit`.

### Start and query

Load all three pipelines at once and query each endpoint:

```bash
cargo run --bin skardi-server -- \
  --ctx docs/postgres/ctx_pgvector_demo.yaml \
  --pipeline docs/postgres/pipelines/vector_demo/ \
  --port 8080

# Inner product
curl -X POST http://localhost:8080/vector-search-inner-product/execute \
  -H "Content-Type: application/json" \
  -d '{"seed_id": 1, "k": 10, "limit": 3}' | jq .

# L2 (Euclidean)
curl -X POST http://localhost:8080/vector-search-l2/execute \
  -H "Content-Type: application/json" \
  -d '{"seed_id": 1, "k": 10, "limit": 3}' | jq .

# Cosine
curl -X POST http://localhost:8080/vector-search-cosine/execute \
  -H "Content-Type: application/json" \
  -d '{"seed_id": 1, "k": 10, "limit": 3}' | jq .
```

**`<#>` inner product** — doc-2 ranks first: same direction as doc-1 but 5× larger magnitude boosts the dot product.
```json
{
  "data": [
    {"id": 2, "content": "Systems programming at scale with Rust", "metadata": "doc-2", "_score": -5.0},
    {"id": 1, "content": "Rust systems programming",               "metadata": "doc-1", "_score": -1.0},
    {"id": 3, "content": "Introduction to programming languages",  "metadata": "doc-3", "_score": -0.7}
  ],
  "rows": 3,
  "success": true
}
```

**`<->` L2** — doc-3 ranks second: it is geometrically closer than doc-4; doc-2 ranks last despite being on-topic because of its large magnitude.
```json
{
  "data": [
    {"id": 1, "content": "Rust systems programming",               "metadata": "doc-1", "_score": 0.0},
    {"id": 3, "content": "Introduction to programming languages",  "metadata": "doc-3", "_score": 0.77},
    {"id": 4, "content": "Database query optimization",            "metadata": "doc-4", "_score": 1.41}
  ],
  "rows": 3,
  "success": true
}
```

**`<=>` cosine** — doc-2 ties with doc-1: direction is identical regardless of magnitude; doc-4 is completely orthogonal.
```json
{
  "data": [
    {"id": 1, "content": "Rust systems programming",               "metadata": "doc-1", "_score": 0.0},
    {"id": 2, "content": "Systems programming at scale with Rust", "metadata": "doc-2", "_score": 0.0},
    {"id": 3, "content": "Introduction to programming languages",  "metadata": "doc-3", "_score": 0.3}
  ],
  "rows": 3,
  "success": true
}
```

### `pg_knn` parameters

```sql
pg_knn(table_name, vector_col, query_vec, metric, k [, filter])
```

| Argument | Type | Description |
|---|---|---|
| `table_name` | string | DataFusion table name (as declared in the context file) |
| `vector_col` | string | Name of the `vector` column to search |
| `query_vec` | float array or subquery | Query embedding, e.g. `[0.1, 0.2, ...]` |
| `metric` | string | pgvector operator: `<#>` (inner product), `<->` (L2), or `<=>` (cosine) |
| `k` | integer | Number of nearest neighbours to return |
| `filter` | string (optional) | SQL WHERE predicate pushed directly into the Postgres query |

`pg_knn` runs the search entirely in Postgres, so the HNSW index is always used.

`_score` is the raw pgvector distance value — lower is always more similar regardless of metric (for `inner_product` the score is negative).

Additional `WHERE` clauses written in the pipeline SQL are pushed down the same way:

```sql
SELECT id, content, _score
FROM pg_knn('documents', 'embedding', {query_vector}, 'metadata = ''doc-1''')
WHERE _score < -0.5
```

### Cross-source join (pgvector + CSV)

`pg_knn` results are a normal DataFusion table and can be joined with any other registered source. Using the CSV orders data already present in this guide:

```yaml
# Add to ctx_pgvector_demo.yaml

kind: context

metadata:
  name: example-context
  version: 1.0.0

spec:
  data_sources:
    - name: "documents"
      type: "postgres"
      connection_string: "postgresql://localhost:5432/mydb?sslmode=disable"
      options:
        table: "documents"
        schema: "public"
        user_env: "PG_USER"
        pass_env: "PG_PASSWORD"
    - name: "csv_orders"
      type: "csv"
      path: "docs/sample_data/orders.csv"
```

```sql
-- Pipeline SQL: find semantically similar documents, then attach order data
SELECT v.id, v.content, v._score, o.product, o.amount
FROM pg_knn('documents', 'embedding', {query_vector}) v
JOIN csv_orders o ON o.user_id = v.id
ORDER BY v._score
```

## Full-Text Search with `pg_fts`

Skardi supports PostgreSQL's built-in full-text search via the `pg_fts()` table function. Any Postgres data source with a text column can be searched — no extra configuration needed.

Under the hood, `pg_fts` uses `websearch_to_tsquery` for query parsing and `ts_rank` for relevance scoring.

### Setup

```bash
# 1. Start PostgreSQL (reuse the existing container or create one)
docker run --name postgres-skardi \
  -e POSTGRES_DB=mydb \
  -e POSTGRES_USER=skardi_user \
  -e POSTGRES_PASSWORD=skardi_pass \
  -p 5432:5432 \
  -d postgres:16

# 2. Create a table with text content
docker exec -i postgres-skardi psql -U skardi_user -d mydb << 'EOF'
CREATE TABLE articles (
    id SERIAL PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    body TEXT NOT NULL,
    category VARCHAR(50) NOT NULL
);

INSERT INTO articles (title, body, category) VALUES
    ('Intro to Machine Learning', 'machine learning model training deep neural network supervised algorithms', 'ai'),
    ('Natural Language Processing', 'natural language processing text classification sentiment analysis tokenization', 'ai'),
    ('Database Query Optimization', 'database query optimization indexing performance tuning relational algebra', 'database'),
    ('Deep Learning Advances', 'machine learning classification supervised training model convolutional neural network', 'research'),
    ('Neural Network Architectures', 'deep learning neural network convolutional image recognition transformer attention', 'ai');
EOF

# 3. Set credentials
export PG_USER="skardi_user"
export PG_PASSWORD="skardi_pass"
```

### Context file

Add `articles` as a regular Postgres data source — `pg_fts` discovers text columns automatically:

```yaml
# ctx_pgfts_demo.yaml

kind: context

metadata:
  name: example-context
  version: 1.0.0

spec:
  data_sources:
    - name: "articles"
      type: "postgres"
      connection_string: "postgresql://localhost:5432/mydb?sslmode=disable"
      options:
        table: "articles"
        schema: "public"
        user_env: "PG_USER"
        pass_env: "PG_PASSWORD"
```

### Pipelines

Two pipeline files are provided in `docs/postgres/pipelines/fts_demo/`:

| File | Description |
|---|---|
| `fts_search.yaml` | Basic full-text search |
| `fts_search_with_filter.yaml` | FTS with category filter pushdown |

### Start and query

```bash
cargo run --bin skardi-server -- \
  --ctx docs/postgres/ctx_pgfts_demo.yaml \
  --pipeline docs/postgres/pipelines/fts_demo/ \
  --port 8080
```

# Basic search
```bash
curl -X POST http://localhost:8080/fts-search/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "machine learning", "limit": 5}' | jq .
```

```json
{
  "success": true,
  "data": [
    {
      "id": 1,
      "title": "Intro to Machine Learning",
      "category": "ai",
      "_score": 0.09910321980714798
    },
    {
      "id": 5,
      "title": "Neural Network Architectures",
      "category": "ai",
      "_score": 0.09910321980714798
    }
  ],
  "rows": 2,
  "execution_time_ms": 295,
  "timestamp": "2026-04-10T18:39:47.725679+00:00"
}
```


# With category filter
```bash
curl -X POST http://localhost:8080/fts-search-with-filter/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "neural network", "category": "ai", "limit": 5}' | jq .
```

```json
{
  "success": true,
  "data": [
    {
      "id": 1,
      "title": "Intro to Machine Learning",
      "category": "ai",
      "_score": 0.09910321980714798
    },
    {
      "id": 4,
      "title": "Deep Learning Advances",
      "category": "research",
      "_score": 0.09910321980714798
    }
  ],
  "rows": 2,
  "execution_time_ms": 252,
  "timestamp": "2026-04-10T18:39:38.994683+00:00"
}
```

**Web-search-style queries** — `websearch_to_tsquery` supports:

| Syntax | Meaning | Example |
|---|---|---|
| `foo bar` | AND (both terms required) | `machine learning` |
| `"foo bar"` | Exact phrase | `"neural network"` |
| `foo or bar` | OR (either term) | `machine or database` |
| `-foo` | NOT (exclude term) | `learning -database` |

### `pg_fts` parameters

```sql
pg_fts(table_name, text_col, query, limit [, language])
```

| Argument | Type | Description |
|---|---|---|
| `table_name` | string | DataFusion table name (as declared in the context file) |
| `text_col` | string | Name of the text column to search |
| `query` | string | Search query (parsed by `websearch_to_tsquery`) |
| `limit` | integer | Maximum number of results to return (1–500) |
| `language` | string (optional) | PostgreSQL text search configuration (default: `'english'`). Common values: `'simple'`, `'spanish'`, `'german'`, `'chinese'`, etc. |

`_score` is the `ts_rank` value — higher means more relevant.

Additional `WHERE` clauses written in the pipeline SQL are pushed down into the PostgreSQL query:

```sql
SELECT id, title, _score
FROM pg_fts('articles', 'body', {query}, 10)
WHERE category = 'ai'
ORDER BY _score DESC
```

## Troubleshooting

### Connection Refused
```
Error: Failed to create PostgreSQL connection pool
```
**Solution**: Verify PostgreSQL server is running:
```bash
docker ps | grep postgres-skardi
docker logs postgres-skardi
```

### Authentication Failed
```
Error: password authentication failed
```
**Solution**: Check environment variables:
```bash
echo $PG_USER
echo $PG_PASSWORD
```

### Table Not Found
```
Error: relation "users" does not exist
```
**Solution**: Verify table exists:
```bash
docker exec postgres-skardi psql -U skardi_user -d mydb -c "\dt"
```

### INSERT Fails with "null but schema specifies non-nullable"
```
Error: Invalid batch column at '0' has null but schema specifies non-nullable
```
**Solution**: This occurs when a table has `SERIAL` or `NOT NULL` columns that DataFusion cannot populate. For INSERT target tables, avoid using `SERIAL PRIMARY KEY` - use a regular column as the primary key instead:
```sql
-- Instead of:
CREATE TABLE my_table (id SERIAL PRIMARY KEY, ...);

-- Use:
CREATE TABLE my_table (user_id INT PRIMARY KEY, ...);
```

### Port Already in Use
```
Error: role "skardi_user" does not exist
```
**Solution**: Another PostgreSQL instance may be running on port 5432. Check with:
```bash
lsof -i :5432
```
Either stop the conflicting service or use a different port for the Docker container.

## Catalog Mode: Load an Entire Schema at Once

Instead of adding one entry per table, omit the `table` option to let Skardi
discover and register **every table and view** in the target schema
automatically. This is called *catalog mode*.

### Context file (`ctx_postgres_catalog_demo.yaml`)

```yaml
kind: context

metadata:
  name: example-context
  version: 1.0.0

spec:
  data_sources:
    # One entry loads both schemas from the same DB.
    # Tables are accessible as mydb.public.users, mydb.analytics.monthly_revenue, …
    - name: "mydb"
      type: "postgres"
      hierarchy_level: "catalog"        # required to enable catalog mode
      access_mode: "read_write"
      connection_string: "postgresql://localhost:5432/mydb?sslmode=disable"
      options:
        allowed_schemas: "public,analytics"  # comma-separated; omit to load all schemas
        user_env: "PG_USER"
        pass_env: "PG_PASSWORD"
```

> **Notes:**
> - `table` and `schema` options are rejected when `hierarchy_level: catalog` is set.
> - `allowed_schemas` must be either absent (loads all non-system schemas) or a non-empty comma-separated string. An empty string causes a startup error.

### Quick Start with Catalog Mode

```bash
# 1. Start PostgreSQL with pgvector (required for vector search in catalog mode)
docker run --name postgres-skardi \
  -e POSTGRES_DB=mydb \
  -e POSTGRES_USER=skardi_user \
  -e POSTGRES_PASSWORD=skardi_pass \
  -p 5432:5432 \
  -d pgvector/pgvector:pg16

docker exec -i postgres-skardi psql -U skardi_user -d mydb << 'EOF'
-- Enable pgvector (safe to run even if the extension is already present)
CREATE EXTENSION IF NOT EXISTS vector;

-- public schema tables (same as the regular example)
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL
);
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    product VARCHAR(100) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL
);
CREATE TABLE user_order_stats (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(100),
    user_email VARCHAR(100),
    total_orders INT,
    total_spent DECIMAL(10, 2),
    last_order_date VARCHAR(50)
);
INSERT INTO users (name, email) VALUES
    ('Alice Smith', 'alice@example.com'),
    ('Bob Johnson', 'bob@example.com'),
    ('Carol Williams', 'carol@example.com');
INSERT INTO orders (user_id, product, amount) VALUES
    (1, 'Laptop', 999.99),
    (2, 'Keyboard', 79.99),
    (3, 'Monitor', 299.99);

-- Vector search table: loaded automatically by catalog mode because it is in public schema
CREATE TABLE documents (
    id BIGSERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    metadata TEXT,
    embedding vector(4)   -- use your actual dimension (e.g. 1536 for OpenAI)
);
CREATE INDEX ON documents USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);
INSERT INTO documents (content, metadata, embedding) VALUES
    ('Rust systems programming',                    'doc-1', '[0.6, 0.8, 0.0, 0.0]'),
    ('Systems programming at scale with Rust',      'doc-2', '[3.0, 4.0, 0.0, 0.0]'),
    ('Introduction to programming languages',       'doc-3', '[0.5, 0.5, 0.5, 0.5]'),
    ('Database query optimization',                 'doc-4', '[0.0, 0.0, 0.6, 0.8]');

-- analytics schema tables (second schema, same DB)
CREATE SCHEMA analytics;
CREATE TABLE analytics.monthly_revenue (
    user_id INT NOT NULL,
    month VARCHAR(7) NOT NULL,   -- e.g. "2024-01"
    revenue DECIMAL(10, 2) NOT NULL
);
INSERT INTO analytics.monthly_revenue VALUES
    (1, '2024-01', 1029.98),
    (2, '2024-01',  229.98),
    (3, '2024-01',  389.98);
EOF

# 2. Set environment variables
export PG_USER="skardi_user"
export PG_PASSWORD="skardi_pass"

# 3. Start Skardi using the catalog context and catalog-specific pipelines
cargo run --bin skardi-server -- \
  --ctx docs/postgres/ctx_postgres_catalog_demo.yaml \
  --pipeline docs/postgres/pipelines_for_catalog_example/ \
  --port 8080
```

At startup Skardi logs something like:

```
Registered PostgreSQL catalog 'mydb' with 5 table(s) (read-write)
Registered 'mydb.public.documents' in pg_knn registry for vector search
```

### Querying Across Tables and Schemas

In catalog mode, tables are registered under a three-part name:
`<catalog_name>.<schema_name>.<table_name>`.

For the context above:
- `mydb.public.users`
- `mydb.public.orders`
- `mydb.public.user_order_stats`
- `mydb.analytics.monthly_revenue`

```bash
# Join users ↔ orders (both from the public schema)
curl -X POST http://localhost:8080/list_all_users_and_orders/execute \
  -H "Content-Type: application/json" \
  -d '{}' | jq .

# Join mydb.public.users ↔ mydb.analytics.monthly_revenue (cross-schema)
curl -X POST http://localhost:8080/cross_schema_summary/execute \
  -H "Content-Type: application/json" \
  -d '{}' | jq .
```

The pipeline SQLs use fully qualified paths so there is no ambiguity when
tables from multiple schemas are in scope at the same time.

### Vector Search in Catalog Mode

When a table has a `vector` column it is automatically registered in the `pg_knn` registry as part of catalog loading — no extra configuration needed. The registry key uses the three-part catalog name, so `pg_knn` calls must use `'catalog.schema.table'` as the first argument:

```sql
-- mydb.public.documents was loaded as part of the catalog entry named "mydb"
SELECT id, content, metadata, _score
FROM pg_knn('mydb.public.documents', 'embedding',
    [0.6, 0.8, 0.0, 0.0],
    '<=>', 10)
ORDER BY _score
LIMIT {limit}
```

> **Note on the query vector:** In catalog mode the subquery seed-vector form (e.g. `SELECT embedding FROM mydb.public.documents WHERE id = {seed_id}`) cannot be used because DataFusion would unparse the three-part table reference to PostgreSQL SQL which only understands `schema.table`. Pass the query vector as a literal float array instead.

A ready-made pipeline is provided at `pipelines_for_catalog_example/vector_search_catalog.yaml`. Start the server and query it:

```bash
cargo run --bin skardi-server -- \
  --ctx docs/postgres/ctx_postgres_catalog_demo.yaml \
  --pipeline docs/postgres/pipelines_for_catalog_example/ \
  --port 8080

# Search with a literal query vector
curl -X POST http://localhost:8080/vector-search-catalog/execute \
  -H "Content-Type: application/json" \
  -d '{"query_vector": [0.6, 0.8, 0.0, 0.0], "k": 10, "limit": 3}' | jq .
```

**Example response:**
```json
{
  "data": [
    {"id": 1, "content": "Rust systems programming",               "metadata": "doc-1", "_score": 0.0},
    {"id": 2, "content": "Systems programming at scale with Rust", "metadata": "doc-2", "_score": 0.0},
    {"id": 3, "content": "Introduction to programming languages",  "metadata": "doc-3", "_score": 0.3}
  ],
  "rows": 3,
  "success": true
}
```

At startup Skardi logs a line for each vector-enabled table registered into the KNN registry:

```
Registered 'mydb.public.documents' in pg_knn registry for vector search
```

You can mix KNN results with regular catalog tables in the same query. For example, join the nearest-neighbor documents with the users table:

```sql
SELECT d.id, d.content, d._score, u.name AS author
FROM pg_knn('mydb.public.documents', 'embedding',
    [0.6, 0.8, 0.0, 0.0], '<=>', 10) d
JOIN mydb.public.users u ON u.id = d.id
ORDER BY d._score
```

### How It Works

| Config | Behavior |
|--------|----------|
| `hierarchy_level: table` (default) | Registers the single table named by `options.table` under the DataFusion name given by `name` |
| `hierarchy_level: catalog` | Introspects `information_schema.tables` and registers every `BASE TABLE` as `<name>.<schema>.<table>` |

Use `allowed_schemas` to restrict which schemas are loaded. Point multiple
catalog entries at the same connection string with different `allowed_schemas`
values to expose several schemas from one database.

---

## Configuration Reference

### ctx_postgres_demo.yaml

```yaml
kind: context

metadata:
  name: example-context
  version: 1.0.0

spec:
  data_sources:
    - name: "users"
      type: "postgres"
      connection_string: "postgresql://localhost:5432/mydb?sslmode=disable"
      options:
        table: "users"
        schema: "public"
        user_env: "PG_USER"
        pass_env: "PG_PASSWORD"
```

### Options — table mode (`hierarchy_level: table`, default)

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `table` | Yes | - | Table name to register |
| `schema` | No | `public` | Schema containing the table |
| `user_env` | No | - | Environment variable for username |
| `pass_env` | No | - | Environment variable for password |

### Options — catalog mode (`hierarchy_level: catalog`)

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `allowed_schemas` | No | _(all non-system schemas)_ | Comma-separated list of schemas to load, e.g. `"public,analytics"` |
| `user_env` | No | - | Environment variable for username |
| `pass_env` | No | - | Environment variable for password |

`table` and `schema` are **not allowed** in catalog mode and will cause a startup error.

### Connection String Parameters

| Parameter | Description |
|-----------|-------------|
| `sslmode` | `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full` |
| `connect_timeout` | Connection timeout in seconds |
| `application_name` | Application name for PostgreSQL logs |

## Cleanup

```bash
# Stop and remove the PostgreSQL container
docker stop postgres-skardi
docker rm postgres-skardi
```
