# MongoDB Integration Demo

This guide demonstrates how to integrate MongoDB collections with Skardi.

## Quick Start

```bash
# 1. Start MongoDB in Docker
docker run --name mongo-skardi \
  -e MONGO_INITDB_ROOT_USERNAME=root \
  -e MONGO_INITDB_ROOT_PASSWORD=rootpass \
  -p 27017:27017 \
  -d mongo:7.0

# 2. Create test database and collection with sample data
docker exec -i mongo-skardi mongosh -u root -p rootpass --authenticationDatabase admin << 'EOF'
use mydb

db.createCollection("products", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["product_id", "name", "price"],
      properties: {
        product_id: { bsonType: "string" },
        name: { bsonType: "string" },
        category: { bsonType: "string" },
        price: { bsonType: "double" },
        in_stock: { bsonType: "bool" }
      }
    }
  }
})

db.products.insertMany([
  { _id: "PROD001", product_id: "PROD001", name: "Laptop", category: "Electronics", price: 999.99, in_stock: true },
  { _id: "PROD002", product_id: "PROD002", name: "Keyboard", category: "Electronics", price: 79.99, in_stock: true },
  { _id: "PROD003", product_id: "PROD003", name: "Monitor", category: "Electronics", price: 299.99, in_stock: false },
  { _id: "PROD004", product_id: "PROD004", name: "Mouse", category: "Electronics", price: 29.99, in_stock: true },
  { _id: "PROD005", product_id: "PROD005", name: "Desk Chair", category: "Furniture", price: 199.99, in_stock: true }
])

db.createCollection("product_stats", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["stat_id"],
      properties: {
        stat_id: { bsonType: "string" },
        category: { bsonType: "string" },
        total_products: { bsonType: "long" },
        total_value: { bsonType: "double" },
        avg_price: { bsonType: "double" }
      }
    }
  }
})
EOF

# 3. Set MongoDB credentials and start Skardi server
export MONGO_USER=root
export MONGO_PASS=rootpass

cargo run --bin skardi-server -- \
  --ctx docs/mongo/ctx_mongo_demo.yaml \
  --pipeline docs/mongo/pipelines/ \
  --port 8080
```

## Available Pipelines

| Pipeline | Description |
|----------|-------------|
| `query_product_by_id` | Point lookup by product ID |
| `list_all_products` | Full scan of all products |
| `insert_product` | Insert a single product |
| `insert_products_from_select` | Insert multiple products |
| `update_product_price` | Update a product's price by ID |
| `delete_product` | Delete a product by ID |
| `federated_join_and_insert` | Join CSV inventory with MongoDB, insert aggregated stats |

---

## 1. Point Lookup

Query a specific product by ID using the primary key for efficient single-document retrieval.

```bash
# Execute the query
curl -X POST http://localhost:8080/query_product_by_id/execute \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD001"}' | jq .
```

**Response:**
```json
{
  "data": [{"product_id": "PROD001", "name": "Laptop", "category": "Electronics", "price": 999.99, "in_stock": true}],
  "execution_time_ms": 5,
  "rows": 1,
  "success": true
}
```

---

## 2. Full Scan

List all products in the catalog.

```bash
curl -X POST http://localhost:8080/list_all_products/execute \
  -H "Content-Type: application/json" \
  -d '{}' | jq .
```

**Response:**
```json
{
  "data": [
    {"product_id": "PROD005", "name": "Desk Chair", "category": "Furniture", "price": 199.99, "in_stock": true},
    {"product_id": "PROD002", "name": "Keyboard", "category": "Electronics", "price": 79.99, "in_stock": true},
    {"product_id": "PROD001", "name": "Laptop", "category": "Electronics", "price": 999.99, "in_stock": true},
    {"product_id": "PROD003", "name": "Monitor", "category": "Electronics", "price": 299.99, "in_stock": false},
    {"product_id": "PROD004", "name": "Mouse", "category": "Electronics", "price": 29.99, "in_stock": true}
  ],
  "execution_time_ms": 12,
  "rows": 5,
  "success": true
}
```

---

## 3. Insert Single Document

Insert a new product (uses upsert semantics based on primary key).

```bash
curl -X POST http://localhost:8080/insert_product/execute \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD006", "name": "Webcam", "category": "Electronics", "price": 89.99, "in_stock": true}' | jq .
```

**Response:**
```json
{"data": [{"count": 1}], "execution_time_ms": 8, "rows": 1, "success": true}
```

**Verify in MongoDB:**
```bash
docker exec mongo-skardi mongosh -u root -p rootpass --authenticationDatabase admin --eval \
  'use mydb; db.products.find({product_id: "PROD006"}).pretty()'
```

---

## 4. Insert Multiple Documents

Insert multiple products using a VALUES clause.

```bash
curl -X POST http://localhost:8080/insert_products_from_select/execute \
  -H "Content-Type: application/json" \
  -d '{
    "product_id_1": "PROD007", "name_1": "Headphones", "category_1": "Electronics", "price_1": 149.99, "in_stock_1": true,
    "product_id_2": "PROD008", "name_2": "USB Hub", "category_2": "Electronics", "price_2": 39.99, "in_stock_2": true
  }' | jq .
```

**Response:**
```json
{"data": [{"count": 2}], "execution_time_ms": 15, "rows": 1, "success": true}
```

---

## 5. Update a Product

Update a product's price by its product ID.

```bash
curl -X POST http://localhost:8080/update_product_price/execute \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD001", "price": 899.99}' | jq .
```

**Response:**
```json
{"data": [{"count": 1}], "execution_time_ms": 6, "rows": 1, "success": true}
```

**Verify in MongoDB:**
```bash
docker exec mongo-skardi mongosh -u root -p rootpass --authenticationDatabase admin --eval \
  'use mydb; db.products.find({product_id: "PROD001"}).pretty()'
```

---

## 6. Delete a Product

Delete a product by its product ID.

```bash
curl -X POST http://localhost:8080/delete_product/execute \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD006"}' | jq .
```

**Response:**
```json
{"data": [{"count": 1}], "execution_time_ms": 5, "rows": 1, "success": true}
```

**Verify in MongoDB:**
```bash
docker exec mongo-skardi mongosh -u root -p rootpass --authenticationDatabase admin --eval \
  'use mydb; db.products.find().pretty()'
```

---

## 7. Federated Query: Join CSV + MongoDB

Join data from multiple sources (CSV file + MongoDB collection) and write aggregated results back to MongoDB.

```
CSV (product_inventory.csv)     MongoDB (products)
         │                            │
         └──────────┬─────────────────┘
                    │
               DataFusion
            JOIN + Aggregate
                    │
                    ▼
         MongoDB (product_stats)
```

```bash
# Aggregate Electronics category
curl -X POST http://localhost:8080/federated_join_and_insert/execute \
  -H "Content-Type: application/json" \
  -d '{"category": "Electronics"}' | jq .

# Aggregate Furniture category
curl -X POST http://localhost:8080/federated_join_and_insert/execute \
  -H "Content-Type: application/json" \
  -d '{"category": "Furniture"}' | jq .
```

**Verify Results:**
```bash
docker exec mongo-skardi mongosh -u root -p rootpass --authenticationDatabase admin --eval \
  'use mydb; db.product_stats.find().pretty()'
```

---

## MongoDB Full-Text Search (`mongo_fts`)

Skardi supports MongoDB's `$text` full-text search via the `mongo_fts()` table function. Any MongoDB collection with a [text index](https://www.mongodb.com/docs/manual/core/index-text/) can be searched directly from SQL.

This is particularly useful for hybrid search with [FastGPT](https://github.com/labring/FastGPT), which stores pre-tokenized (Jieba) text in MongoDB alongside pgvector embeddings in PostgreSQL.

### Setup

```bash
# 1. Start MongoDB in Docker (or reuse the existing container)
docker run --name mongo-skardi \
  -e MONGO_INITDB_ROOT_USERNAME=root \
  -e MONGO_INITDB_ROOT_PASSWORD=rootpass \
  -p 27017:27017 \
  -d mongo:7.0

# 2. Create a collection with a text index and sample data
docker exec -i mongo-skardi mongosh -u root -p rootpass --authenticationDatabase admin << 'EOF'
use mydb

db.createCollection("dataset_data_texts")

// Create a text index on fullTextToken.
// A simple index works with or without additional filters.
// For production (e.g. FastGPT with millions of docs), use a compound index
// { teamId: 1, fullTextToken: "text" } for better performance — but note
// that compound text indexes require equality on prefix fields in every query.
db.dataset_data_texts.createIndex(
  { fullTextToken: "text" },
  { default_language: "none" }
)

// Insert sample data (pre-tokenized with spaces, simulating Jieba output)
db.dataset_data_texts.insertMany([
  {
    _id: "data001", dataId: "data001", teamId: "team1", datasetId: "ds1", collectionId: "col1",
    fullTextToken: "machine learning model training deep neural network"
  },
  {
    _id: "data002", dataId: "data002", teamId: "team1", datasetId: "ds1", collectionId: "col1",
    fullTextToken: "natural language processing text classification sentiment"
  },
  {
    _id: "data003", dataId: "data003", teamId: "team1", datasetId: "ds1", collectionId: "col2",
    fullTextToken: "database query optimization indexing performance tuning"
  },
  {
    _id: "data004", dataId: "data004", teamId: "team2", datasetId: "ds2", collectionId: "col3",
    fullTextToken: "machine learning classification supervised training model"
  },
  {
    _id: "data005", dataId: "data005", teamId: "team1", datasetId: "ds1", collectionId: "col1",
    fullTextToken: "deep learning neural network convolutional image recognition"
  }
])
EOF

# 3. Set credentials
export MONGO_USER=root
export MONGO_PASS=rootpass
```

### Context file

```yaml
# ctx_mongo_fts_demo.yaml
data_sources:
  - name: "dataset_data_texts"
    type: "mongo"
    access_mode: "read_write"
    connection_string: "mongodb://localhost:27017"
    options:
      database: "mydb"
      collection: "dataset_data_texts"
      primary_key: "dataId"
      user_env: "MONGO_USER"
      pass_env: "MONGO_PASS"
```

### Pipelines

Two pipeline files are provided in `docs/mongo/pipelines/fts_demo/`:

| File | Description |
|---|---|
| `fts_search.yaml` | Basic full-text search |
| `fts_search_with_filter.yaml` | Full-text search with teamId filter pushdown |

```yaml
# fts_search.yaml
query: |
  SELECT "dataId", "teamId", "datasetId", "collectionId", "fullTextToken", _score
  FROM mongo_fts('dataset_data_texts', {query}, {limit})
  ORDER BY _score DESC
```

> **Note:** MongoDB field names are case-sensitive. Use double-quoted identifiers (`"dataId"`) in SQL to preserve camelCase, since DataFusion lowercases unquoted identifiers.

### Start and query

```bash
cargo run --bin skardi-server -- \
  --ctx docs/mongo/ctx_mongo_fts_demo.yaml \
  --pipeline docs/mongo/pipelines/fts_demo/ \
  --port 8080

# Basic search: find documents about "machine learning"
curl -X POST http://localhost:8080/fts-search/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "machine learning", "limit": 10}' | jq .

# Filtered search: only team1 documents about "neural network"
curl -X POST http://localhost:8080/fts-search-with-filter/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "neural network", "team_id": "team1", "limit": 10}' | jq .
```

**Basic search** — returns documents containing "machine" OR "learning", ranked by textScore:
```json
{
  "data": [
    {"dataId": "data004", "teamId": "team2", "_score": 1.25, "fullTextToken": "machine learning classification supervised training model"},
    {"dataId": "data001", "teamId": "team1", "_score": 1.15, "fullTextToken": "machine learning model training deep neural network"},
    {"dataId": "data005", "teamId": "team1", "_score": 0.75, "fullTextToken": "deep learning neural network convolutional image recognition"}
  ],
  "rows": 3,
  "success": true
}
```

**Filtered search** — `WHERE "teamId" = 'team1'` is pushed down to MongoDB alongside `$text`:
```json
{
  "data": [
    {"dataId": "data001", "teamId": "team1", "_score": 1.1, "fullTextToken": "machine learning model training deep neural network"},
    {"dataId": "data005", "teamId": "team1", "_score": 1.0, "fullTextToken": "deep learning neural network convolutional image recognition"}
  ],
  "rows": 2,
  "success": true
}
```

### `mongo_fts` parameters

```sql
mongo_fts(collection, query, limit)
```

| Argument | Type | Description |
|---|---|---|
| `collection` | string | DataFusion table name (as declared in the context file) |
| `query` | string | Search terms. Space-separated for OR; `"quoted"` for phrase match; `-term` for negation |
| `limit` | integer | Maximum number of results (1-500) |

`_score` is MongoDB's `textScore` — higher means more relevant.

`WHERE` clauses are pushed down to MongoDB when possible (equality, comparison operators on indexed fields). This also satisfies compound text index prefix requirements.

### Query syntax

MongoDB's `$text` search supports several query forms:

```sql
-- OR search (default): matches documents containing "machine" OR "learning"
SELECT * FROM mongo_fts('dataset_data_texts', 'machine learning', 10)

-- Phrase search: matches the exact phrase "machine learning"
SELECT * FROM mongo_fts('dataset_data_texts', '"machine learning"', 10)

-- Negation: matches "machine" but NOT "learning"
SELECT * FROM mongo_fts('dataset_data_texts', 'machine -learning', 10)

-- With filter pushdown
SELECT * FROM mongo_fts('dataset_data_texts', 'neural network', 10)
WHERE "teamId" = 'team1'
```

### Compound text index (production)

For large collections (e.g. FastGPT), use a compound text index to narrow by a prefix field first:

```js
db.dataset_data_texts.createIndex(
  { teamId: 1, fullTextToken: "text" },
  { default_language: "none" }
)
```

With a compound index, MongoDB **requires** equality on all prefix fields in every `$text` query.
This means `WHERE "teamId" = 'team1'` becomes mandatory — queries without it will fail.

---

## Cleanup

```bash
docker stop mongo-skardi && docker rm mongo-skardi
pkill -f skardi-server
```

---

## Advanced Configuration

### Credential Management

MongoDB credentials are read from environment variables for security. Do not embed credentials in the connection string.

```yaml
data_sources:
  - name: "products"
    type: "mongo"
    connection_string: "mongodb://localhost:27017"
    options:
      database: "mydb"
      collection: "products"
      primary_key: "product_id"
      user_env: "MONGO_USER"      # Environment variable for username
      pass_env: "MONGO_PASS"      # Environment variable for password
```

```bash
# Set credentials before starting the server
export MONGO_USER=myuser
export MONGO_PASS=mypassword
```

### Multiple Databases

```yaml
data_sources:
  - name: "prod_products"
    type: "mongo"
    connection_string: "mongodb://prod-server:27017"
    options:
      database: "production"
      collection: "products"
      primary_key: "product_id"
      user_env: "PROD_MONGO_USER"
      pass_env: "PROD_MONGO_PASS"

  - name: "staging_products"
    type: "mongo"
    connection_string: "mongodb://staging-server:27017"
    options:
      database: "staging"
      collection: "products"
      primary_key: "product_id"
      user_env: "STAGING_MONGO_USER"
      pass_env: "STAGING_MONGO_PASS"
```

### Schema Inference

Skardi infers the MongoDB collection schema at startup by:

1. Reading the collection's `$jsonSchema` validator (if defined)
2. Falling back to sampling existing documents

If your collection is **empty and has no validator**, only the primary key field will be available in SQL. To ensure all fields are discoverable before inserting any data, define a `$jsonSchema` validator when creating the collection:

```js
db.createCollection("my_collection", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["doc_id", "fullTextToken"],
      properties: {
        doc_id: { bsonType: "string" },
        fullTextToken: { bsonType: "string" }
      }
    }
  }
})
```

This is especially important for write-first workflows (e.g. RAG ingestion) where the server starts before any documents exist.

### MongoDB Atlas

```yaml
data_sources:
  - name: "products"
    type: "mongo"
    connection_string: "mongodb+srv://cluster0.xxxxx.mongodb.net"
    options:
      database: "mydb"
      collection: "products"
      primary_key: "product_id"
      user_env: "ATLAS_USER"
      pass_env: "ATLAS_PASS"
```
