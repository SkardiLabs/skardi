# SQLite Integration

This guide covers how to integrate SQLite databases with Skardi, including INSERT, UPDATE, DELETE operations and federated queries with CSV data.

## Quick Start

SQLite requires no external server — just a local `.db` file:

```bash
# 1. Create the SQLite database and test data
sqlite3 docs/sqlite/demo.db << 'EOF'
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL
);
CREATE TABLE orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    product TEXT NOT NULL,
    amount REAL NOT NULL
);
CREATE TABLE user_order_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL UNIQUE,
    user_name TEXT NOT NULL,
    user_email TEXT NOT NULL,
    total_orders INTEGER NOT NULL,
    total_spent REAL NOT NULL,
    last_order_date TEXT
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

# 1b. Create sample CSV file (for federated query example)
mkdir -p docs/sample_data
cat > docs/sample_data/orders.csv << 'EOF'
order_id,user_id,product,amount,order_date
1001,1,Laptop,999.99,2024-01-15
1002,1,Mouse,29.99,2024-01-16
1003,2,Keyboard,79.99,2024-01-17
1004,3,Monitor,299.99,2024-01-18
1005,1,USB Cable,9.99,2024-01-19
1006,2,Headphones,149.99,2024-01-20
EOF

# 2. Start Skardi server
cargo run --bin skardi-server -- \
  --ctx docs/sqlite/ctx_sqlite_demo.yaml \
  --pipeline docs/sqlite/pipelines/ \
  --port 8080

# 3. Execute with parameters
curl -X POST http://localhost:8080/query_user_by_id/execute \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1}' | jq .
```

## Using the CLI (Direct Path Query)

SQLite tables can be queried directly by path — no context file needed. Use the pattern `path/to/file.db.table_name`:

```bash
# Query a table directly
skardi query --sql "SELECT * FROM './docs/sqlite/demo.db.users'"

# Join two tables from the same database
skardi query --sql "
  SELECT u.name, o.product, o.amount
  FROM './docs/sqlite/demo.db.users' u
  JOIN './docs/sqlite/demo.db.orders' o ON u.id = o.user_id
"

# Works with .sqlite and .sqlite3 extensions too
skardi query --sql "SELECT * FROM './data/app.sqlite.customers'"
skardi query --sql "SELECT * FROM './data/app.sqlite3.customers'"
```

## Running the Example

1. **Create the database** (see Quick Start step 1 above)

2. **Start Skardi server with pipelines**:

   Example pipeline files are provided in `docs/sqlite/pipelines/`:
   - `query_user_by_id.yaml` - Query user by ID
   - `insert_user.yaml` - Insert new user
   - `update_user_email.yaml` - Update a user's email by name
   - `delete_user.yaml` - Delete a user by name
   - `federated_join_and_insert.yaml` - Join CSV + SQLite and write results back

   Pass them all at server start using the `--pipeline` flag (accepts a directory or individual files):
   ```bash
   cargo run --bin skardi-server -- \
     --ctx docs/sqlite/ctx_sqlite_demo.yaml \
     --pipeline docs/sqlite/pipelines/ \
     --port 8080
   ```

3. **Execute pipelines**:

   ```bash
   # Query a user by ID
   curl -X POST http://localhost:8080/query_user_by_id/execute \
     -H "Content-Type: application/json" \
     -d '{"user_id": 1}' | jq .
   ```

## Single INSERT Example

Insert a new user into the SQLite table:

```bash
# Execute INSERT with parameters
curl -X POST http://localhost:8080/insert_user/execute \
  -H "Content-Type: application/json" \
  -d '{"name": "David Brown", "email": "david@example.com"}' | jq .
```

**Verify the insert:**
```bash
sqlite3 docs/sqlite/demo.db "SELECT * FROM users"
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
sqlite3 docs/sqlite/demo.db "SELECT * FROM users WHERE name = 'Alice Smith'"
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

**Verify the delete:**
```bash
sqlite3 docs/sqlite/demo.db "SELECT * FROM users"
```

> **Note:** Omitting the `WHERE` clause deletes all rows in the table. Always double-check your filter parameters before executing a DELETE pipeline against production data.

## Federated Query Example: Join CSV + SQLite

This example demonstrates **joining data from multiple sources** (CSV file + SQLite table) and writing the aggregated results back to SQLite.

### What This Does

```
CSV File (orders.csv)         SQLite (users table)
6 rows of order data    +     3 rows of user data
         |                             |
         +-------------+--------------+
                        |
                   DataFusion
                JOIN + Aggregate
                        |
                        v
             SQLite (user_order_stats)
          Aggregated statistics per user
```

### Execute

```bash
# Execute for a specific user by name
curl -X POST http://localhost:8080/federated_join_and_insert/execute \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice Smith"}' | jq .
```

**Response:**
```json
{
  "data": [{"count": 1}],
  "execution_time_ms": 42,
  "rows": 1,
  "success": true
}
```

### Verify Results

```bash
sqlite3 docs/sqlite/demo.db "SELECT * FROM user_order_stats"
```

**Output (after executing for "Alice Smith"):**
```
1|1|Alice Smith|alice@example.com|3|1039.97|2024-01-19
```

You can execute for other users as well:
```bash
# Execute for Bob
curl -X POST http://localhost:8080/federated_join_and_insert/execute \
  -H "Content-Type: application/json" \
  -d '{"name": "Bob Johnson"}' | jq .

# Execute for Carol
curl -X POST http://localhost:8080/federated_join_and_insert/execute \
  -H "Content-Type: application/json" \
  -d '{"name": "Carol Williams"}' | jq .
```

## Catalog Mode

Instead of registering tables one by one, you can expose an entire SQLite database as a
DataFusion **catalog**. Every non-system table and view is registered automatically, and
you query them with the three-part `catalog.main.table` syntax (SQLite's primary schema
is always `main`).

### Context file

```yaml
# docs/sqlite/ctx_sqlite_catalog_demo.yaml
data_sources:
  - name: "demo_catalog"
    type: "sqlite"
    hierarchy_level: "catalog"
    path: "docs/sqlite/demo.db"
    description: "Entire demo.db SQLite database registered as a DataFusion catalog"
```

### Start the server

```bash
cargo run --bin skardi-server -- \
  --ctx docs/sqlite/ctx_sqlite_catalog_demo.yaml \
  --pipeline docs/sqlite/pipelines/catalog_demo/ \
  --port 8080
```

### Example queries

Tables are referenced as `<catalog>.main.<table>`:

```bash
# List users (limit 10)
curl -X POST http://localhost:8080/sqlite-catalog-list-users/execute \
  -H "Content-Type: application/json" \
  -d '{"limit": 10}' | jq .

# Join users and orders through the catalog
curl -X POST http://localhost:8080/sqlite-catalog-cross-table-join/execute \
  -H "Content-Type: application/json" \
  -d '{"limit": 20}' | jq .

# Aggregate order totals per user
curl -X POST http://localhost:8080/sqlite-catalog-user-order-summary/execute \
  -H "Content-Type: application/json" \
  -d '{"min_orders": 1}' | jq .
```

### Table mode vs catalog mode at a glance

| | Table mode (default) | Catalog mode |
|---|---|---|
| `hierarchy_level` | `table` or omit | `catalog` |
| `table` option | required | not used |
| SQL reference | `table_name` | `catalog.main.table` |
| Tables loaded | one | all non-system |
| Good for | single-table APIs | cross-table joins, schema discovery |

## Troubleshooting

### Database File Not Found
```
Error: Failed to create SQLite connection pool
```
**Solution**: Verify the database file exists and the path is correct:
```bash
ls -la docs/sqlite/demo.db
```

### Table Not Found
```
Error: Failed to create table provider
```
**Solution**: Verify the table exists in the database:
```bash
sqlite3 docs/sqlite/demo.db ".tables"
sqlite3 docs/sqlite/demo.db ".schema users"
```

### Database Locked
```
Error: database is locked
```
**Solution**: SQLite only allows one writer at a time. Check if another process has the database open:
```bash
# Check for processes using the database file
lsof docs/sqlite/demo.db
```

You can also increase the busy timeout via the `busy_timeout_ms` option in the context file:
```yaml
options:
  table: "users"
  busy_timeout_ms: "10000"  # Wait up to 10 seconds for locks
```

## Full-Text Search (FTS5)

SQLite FTS5 provides indexed full-text search with BM25 relevance scoring — no external extension needed.

### Setup

```bash
# Create an FTS5 virtual table
sqlite3 docs/sqlite/fts_demo.db << 'EOF'
CREATE VIRTUAL TABLE articles_fts USING fts5(title, body, category);
INSERT INTO articles_fts (title, body, category) VALUES
    ('Machine Learning Basics', 'Introduction to machine learning algorithms and neural networks', 'ai'),
    ('Database Systems', 'Overview of relational database management systems and SQL', 'database'),
    ('Deep Learning', 'Advanced neural network architectures for machine learning', 'ai'),
    ('Web Development', 'Modern web frameworks and frontend technologies', 'web'),
    ('NLP Guide', 'NLP techniques for text analysis and machine learning applications', 'ml');
EOF
```

### Query

```bash
# Start with FTS context
cargo run --bin skardi-server -- \
  --ctx docs/sqlite/ctx_sqlite_fts_demo.yaml \
  --pipeline docs/sqlite/pipelines/fts_demo/ \
  --port 8080

# Basic FTS search
curl -X POST http://localhost:8080/sqlite-fts-search/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "machine learning", "limit": 10}' | jq .

# Response
{
  "success": true,
  "data": [
    {
      "title": "Deep Learning",
      "body": "Advanced neural network architectures for machine learning",
      "category": "ai",
      "_score": 0.0000020624999999999997
    },
    {
      "title": "Machine Learning Basics",
      "body": "Introduction to machine learning algorithms and neural networks",
      "category": "ai",
      "_score": 0.0000019130434782608697
    },
    {
      "title": "NLP Guide",
      "body": "NLP techniques for text analysis and machine learning applications",
      "category": "ml",
      "_score": 0.0000019130434782608697
    }
  ],
  "rows": 3,
  "execution_time_ms": 16,
  "timestamp": "2026-04-11T07:24:38.753556+00:00"
}

# FTS with category filter
curl -X POST http://localhost:8080/sqlite-fts-search-with-filter/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "machine learning", "category": "ai", "limit": 10}' | jq .

 # Response
 {
  "success": true,
  "data": [
    {
      "title": "Deep Learning",
      "body": "Advanced neural network architectures for machine learning",
      "category": "ai",
      "_score": 0.0000020624999999999997
    },
    {
      "title": "Machine Learning Basics",
      "body": "Introduction to machine learning algorithms and neural networks",
      "category": "ai",
      "_score": 0.0000019130434782608697
    }
  ],
  "rows": 2,
  "execution_time_ms": 6,
  "timestamp": "2026-04-11T07:26:59.099570+00:00"
}

```

### SQL Syntax

```sql
-- sqlite_fts(table, text_col, query, limit)
SELECT title, body, category, _score
FROM sqlite_fts('articles_fts', 'body', 'machine learning', 10)
ORDER BY _score DESC

-- With WHERE clause filter pushdown
SELECT title, body, category, _score
FROM sqlite_fts('articles_fts', 'body', 'neural network', 10)
WHERE category = 'ai'
ORDER BY _score DESC
```

FTS5 query syntax supports:
- Plain terms (AND'd by default): `machine learning`
- Quoted phrases: `"neural network"`
- NOT operator: `learning NOT database`
- OR operator: `machine OR database`
- Prefix queries: `mach*`

### Write

FTS5 indexes are updated automatically on INSERT and DELETE — no rebuild needed.

```bash
# Insert a new article (immediately searchable)
curl -X POST http://localhost:8080/sqlite-fts-insert-article/execute \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Reinforcement Learning",
    "body": "Reinforcement Learning works by training agents through reward signals and policy optimization",
    "category": "ai"
  }' | jq .

# Delete an article by title
curl -X POST http://localhost:8080/sqlite-fts-delete-article/execute \
  -H "Content-Type: application/json" \
  -d '{"title": "Web Development"}' | jq .

# Verify: search should now find the new article
curl -X POST http://localhost:8080/sqlite-fts-search/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "reinforcement", "limit": 5}' | jq .

Response :

{
  "success": true,
  "data": [
    {
      "title": "Reinforcement Learning",
      "body": "Reinforcement Learning works by training agents through reward signals and policy optimization",
      "category": "ai",
      "_score": 1.1554256414233957
    }
  ],
  "rows": 1,
  "execution_time_ms": 9,
  "timestamp": "2026-04-12T06:50:20.190923+00:00"
}

```

## Vector Similarity Search (sqlite-vec)

KNN vector search using [sqlite-vec](https://github.com/asg017/sqlite-vec) `vec0` virtual tables with indexed vector search.

### Prerequisites

```bash
pip install sqlite-vec fastembed
```

The sqlite-vec pip package provides the Python bindings for setup.

### Setup

```bash
# 1. Create the database with vec0 table and seed embeddings
#    (embeds item names using sentence-transformers all-MiniLM-L6-v2, 384 dims)
python docs/sqlite/setup_knn_demo.py

# 2. Download the candle embedding model for query-time embedding (one-time)
python docs/embeddings/candle/setup.py

# 3. Set the extension path for the server
#    If using the pip package:
export VEC0_PATH=$(python -c "import sqlite_vec; print(sqlite_vec.loadable_path())")
```

### Query (Semantic Search)

The `candle()` UDF embeds the query text on the fly using a local BERT model. No external API needed.

```bash
cargo run --bin skardi-server --features candle -- \
  --ctx docs/sqlite/ctx_sqlite_knn_demo.yaml \
  --pipeline docs/sqlite/pipelines/knn_demo/ \
  --port 8080

# Semantic KNN search — query is embedded on the fly
curl -X POST http://localhost:8080/sqlite-knn-search/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "portable computing device", "k": 3}' | jq .

# Response (scores depend on the embedding model — exact values will vary)
{
  "success": true,
  "data": [
    {
      "item_id": 1,
      "_score": 0.6727945804595947
    },
    {
      "item_id": 4,
      "_score": 0.7535505890846252
    },
    {
      "item_id": 5,
      "_score": 0.8204019665718079
    }
  ],
  "rows": 3,
  "execution_time_ms": 295,
  "timestamp": "2026-04-12T06:58:58.032915+00:00"
}

# KNN with JOIN to get full item details
curl -X POST http://localhost:8080/sqlite-knn-search-with-join/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "portable computing device", "k": 3}' | jq .

# Response
{
  "success": true,
  "data": [
    {
      "id": 1,
      "name": "Laptop",
      "category": "electronics",
      "_score": 0.6727945804595947
    },
    {
      "id": 4,
      "name": "Tablet",
      "category": "electronics",
      "_score": 0.7535505890846252
    },
    {
      "id": 5,
      "name": "Notebook",
      "category": "education",
      "_score": 0.8204019665718079
    }
  ],
  "rows": 3,
  "execution_time_ms": 201,
  "timestamp": "2026-04-12T06:59:14.064319+00:00"
}

# Find items similar to an existing item (scalar subquery)
curl -X POST http://localhost:8080/sqlite-knn-search-by-seed/execute \
  -H "Content-Type: application/json" \
  -d '{"seed_id": 1, "k": 3}' | jq .

# Response
{
  "success": true,
  "data": [
    {
      "id": 1,
      "name": "Laptop",
      "category": "electronics",
      "_score": 0.0
    },
    {
      "id": 5,
      "name": "Notebook",
      "category": "education",
      "_score": 0.5731309652328491
    },
    {
      "id": 4,
      "name": "Tablet",
      "category": "electronics",
      "_score": 0.6620298624038696
    }
  ],
  "rows": 3,
  "execution_time_ms": 31,
  "timestamp": "2026-04-12T06:59:38.149485+00:00"
}
```

### SQL Syntax

```sql
-- Semantic search: embed query on the fly with candle()
SELECT item_id, _score
FROM sqlite_knn(
  'vec_items', 'embedding',
  candle('models/generated/bge-small-en-v1.5', 'portable computing device'),
  10
)

-- Find similar items using an existing item's embedding (scalar subquery)
SELECT i.name, i.category, knn._score
FROM sqlite_knn(
  'vec_items', 'embedding',
  (SELECT embedding FROM vec_items WHERE item_id = 1),
  10
) knn
JOIN items i ON i.id = knn.item_id
ORDER BY knn._score

-- Join with data table for full results
SELECT i.name, i.category, knn._score
FROM sqlite_knn(
  'vec_items', 'embedding',
  candle('models/generated/bge-small-en-v1.5', 'search query'),
  10
) knn
JOIN items i ON i.id = knn.item_id
ORDER BY knn._score

-- Insert: embed text and write vector to the vec0 index
-- vec_to_binary() packs List<Float32> into the BLOB format expected by vec0.
-- Works with all embedding UDFs: candle(), gguf(), remote_embed().
INSERT INTO vec_items (item_id, embedding)
SELECT 6, vec_to_binary(candle('models/generated/bge-small-en-v1.5', 'Smartwatch wearable device'))
```

### Write

The `candle()` UDF can be used in INSERT to embed text and write the vector directly. Wrap the embedding call with `vec_to_binary()` to convert the `List<Float32>` output into the packed f32 BLOB format expected by sqlite-vec. This works with all embedding UDFs (`candle()`, `gguf()`, `remote_embed()`).

```bash
# 1. Insert item metadata into the data table
curl -X POST http://localhost:8080/sqlite-knn-insert-item/execute \
  -H "Content-Type: application/json" \
  -d '{"id": 6, "name": "Smartwatch", "category": "electronics"}' | jq .

# 2. Embed the item text and insert the vector into the vec0 index
curl -X POST http://localhost:8080/sqlite-knn-insert-vector/execute \
  -H "Content-Type: application/json" \
  -d '{"id": 6, "text": "Smartwatch wearable device for fitness tracking"}' | jq .

# 3. Verify: the new item should be searchable immediately
curl -X POST http://localhost:8080/sqlite-knn-search-with-join/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "wearable fitness", "k": 3}' | jq .

# Delete an item and its vector
curl -X POST http://localhost:8080/sqlite-knn-delete-vector/execute \
  -H "Content-Type: application/json" \
  -d '{"id": 3}' | jq .

curl -X POST http://localhost:8080/sqlite-knn-delete-item/execute \
  -H "Content-Type: application/json" \
  -d '{"id": 3}' | jq .
```

The `extensions` option in the context file tells Skardi to load the sqlite-vec extension on each connection:
```yaml
options:
  table: "vec_items"
  extensions: "/path/to/vec0"
```

## Cleanup

The examples in this guide create local SQLite database files under `docs/sqlite/`. These are
covered by `.gitignore`, but you can remove them when you're done:

```bash
# Stop the server (Ctrl-C in its terminal) before removing files.
rm -f docs/sqlite/demo.db docs/sqlite/demo.db-wal docs/sqlite/demo.db-shm
rm -f docs/sqlite/fts_demo.db docs/sqlite/fts_demo.db-wal docs/sqlite/fts_demo.db-shm
rm -f docs/sqlite/knn_demo.db docs/sqlite/knn_demo.db-wal docs/sqlite/knn_demo.db-shm
```

## Context File Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `table` | Yes | — | SQLite table name to register |
| `busy_timeout_ms` | No | `5000` | Time in milliseconds to wait for database locks |
| `read_pool_size` | No | `4` | Number of read connections in the pool |
| `extensions` | No | — | Comma-separated paths to SQLite extensions to load (e.g. sqlite-vec) |
