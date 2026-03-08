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
  --ctx demo/ctx_mongo_demo.yaml \
  --port 8080 &

sleep 3

for pipeline in demo/mongo_pipelines/*.yaml; do
  curl -s -X POST http://localhost:8080/register_pipeline \
    -H "Content-Type: application/json" \
    -d "{\"path\": \"$pipeline\"}"
  echo ""
done

echo "Server ready!"
```

## Available Pipelines

| Pipeline | Description |
|----------|-------------|
| `query_product_by_id` | Point lookup by product ID |
| `list_all_products` | Full scan of all products |
| `insert_product` | Insert a single product |
| `insert_products_from_select` | Insert multiple products |
| `federated_join_and_insert` | Join CSV inventory with MongoDB, insert aggregated stats |

---

## 1. Point Lookup

Query a specific product by ID using the primary key for efficient single-document retrieval.

```bash
# First, switch to the point lookup pipeline
curl -X POST http://localhost:8080/register_pipeline \
  -H "Content-Type: application/json" \
  -d '{"path": "crates/server/demo/mongo_pipelines/query_product_by_id.yaml"}'

# Execute the query
curl -X POST http://localhost:8080/query_product_by_id/execute \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD001"}'
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
curl -X POST http://localhost:8080/register_pipeline \
  -H "Content-Type: application/json" \
  -d '{"path": "demo/mongo_pipelines/list_all_products.yaml"}'

curl -X POST http://localhost:8080/list_all_products/execute \
  -H "Content-Type: application/json" \
  -d '{}'
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
curl -X POST http://localhost:8080/register_pipeline \
  -H "Content-Type: application/json" \
  -d '{"path": "demo/mongo_pipelines/insert_product.yaml"}'

curl -X POST http://localhost:8080/insert_product/execute \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD006", "name": "Webcam", "category": "Electronics", "price": 89.99, "in_stock": true}'
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
curl -X POST http://localhost:8080/register_pipeline \
  -H "Content-Type: application/json" \
  -d '{"path": "crates/server/demo/mongo_pipelines/insert_products_from_select.yaml"}'

curl -X POST http://localhost:8080/insert_products_from_select/execute \
  -H "Content-Type: application/json" \
  -d '{
    "product_id_1": "PROD007", "name_1": "Headphones", "category_1": "Electronics", "price_1": 149.99, "in_stock_1": true,
    "product_id_2": "PROD008", "name_2": "USB Hub", "category_2": "Electronics", "price_2": 39.99, "in_stock_2": true
  }'
```

**Response:**
```json
{"data": [{"count": 2}], "execution_time_ms": 15, "rows": 1, "success": true}
```

---

## 5. Federated Query: Join CSV + MongoDB

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
curl -X POST http://localhost:8080/register_pipeline \
  -H "Content-Type: application/json" \
  -d '{"path": "demo/mongo_pipelines/federated_join_and_insert.yaml"}'

# Aggregate Electronics category
curl -X POST http://localhost:8080/federated_join_and_insert/execute \
  -H "Content-Type: application/json" \
  -d '{"category": "Electronics"}'

# Aggregate Furniture category
curl -X POST http://localhost:8080/federated_join_and_insert/execute \
  -H "Content-Type: application/json" \
  -d '{"category": "Furniture"}'
```

**Verify Results:**
```bash
docker exec mongo-skardi mongosh -u root -p rootpass --authenticationDatabase admin --eval \
  'use mydb; db.product_stats.find().pretty()'
```

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
