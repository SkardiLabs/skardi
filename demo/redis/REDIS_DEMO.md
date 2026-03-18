# Redis Integration Demo

This guide demonstrates how to integrate Redis hash tables with Skardi.

## Quick Start

```bash
# 1. Start Redis in Docker
docker run --name redis-skardi \
  -p 6379:6379 \
  -d redis:7.4

# 2. Populate sample data (each product is a Redis hash)
docker exec -i redis-skardi redis-cli << 'EOF'
HSET mydb:products:PROD001 name "Laptop" category "Electronics" price "999.99" in_stock "true"
HSET mydb:products:PROD002 name "Keyboard" category "Electronics" price "79.99" in_stock "true"
HSET mydb:products:PROD003 name "Monitor" category "Electronics" price "299.99" in_stock "false"
HSET mydb:products:PROD004 name "Mouse" category "Electronics" price "29.99" in_stock "true"
HSET mydb:products:PROD005 name "Desk Chair" category "Furniture" price "199.99" in_stock "true"
EOF

# 3. Start Skardi server
cargo run --bin skardi-server -- \
  --ctx demo/redis/ctx_redis_demo.yaml \
  --pipeline demo/redis/pipelines/ \
  --port 8080
```

## Data Model

Redis hashes map directly to SQL rows:

```
Redis Key: {key_space}:{table}:{key_column_value}
Hash Fields: column1=value1, column2=value2, ...
```

For example, `mydb:products:PROD001` contains:
```
name      → "Laptop"
category  → "Electronics"
price     → "999.99"
in_stock  → "true"
```

The `key_column` option (`product_id`) is extracted from the Redis key suffix and exposed as a SQL column.

## Available Pipelines

| Pipeline | Description |
|----------|-------------|
| `query_product_by_id` | Point lookup by product ID |
| `list_all_products` | Full scan of all products |
| `insert_product` | Insert a single product |
| `update_product_price` | Update a product's price by ID |
| `delete_product` | Delete a product by ID |
| `federated_join_and_insert` | Join CSV inventory with Redis, insert aggregated stats |

---

## 1. Point Lookup

Query a specific product by ID. Uses the fast path (direct key construction, O(1)) when filtering on the key column.

```bash
curl -X POST http://localhost:8080/query_product_by_id/execute \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD001"}'
```

**Response:**
```json
{
  "data": [{"product_id": "PROD001", "name": "Laptop", "category": "Electronics", "price": "999.99", "in_stock": "true"}],
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
  -d '{}'
```

**Response:**
```json
{
  "data": [
    {"product_id": "PROD005", "name": "Desk Chair", "category": "Furniture", "price": "199.99", "in_stock": "true"},
    {"product_id": "PROD002", "name": "Keyboard", "category": "Electronics", "price": "79.99", "in_stock": "true"},
    {"product_id": "PROD001", "name": "Laptop", "category": "Electronics", "price": "999.99", "in_stock": "true"},
    {"product_id": "PROD003", "name": "Monitor", "category": "Electronics", "price": "299.99", "in_stock": "false"},
    {"product_id": "PROD004", "name": "Mouse", "category": "Electronics", "price": "29.99", "in_stock": "true"}
  ],
  "execution_time_ms": 12,
  "rows": 5,
  "success": true
}
```

---

## 3. Insert a Product

Insert a new product. Creates a Redis hash at `mydb:products:PROD006`.

```bash
curl -X POST http://localhost:8080/insert_product/execute \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD006", "name": "Webcam", "category": "Electronics", "price": "89.99", "in_stock": "true"}'
```

**Response:**
```json
{"data": [{"count": 1}], "execution_time_ms": 8, "rows": 1, "success": true}
```

**Verify in Redis:**
```bash
docker exec redis-skardi redis-cli HGETALL mydb:products:PROD006
```

---

## 4. Update a Product

Update a product's price by its product ID.

```bash
curl -X POST http://localhost:8080/update_product_price/execute \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD001", "price": "899.99"}'
```

**Response:**
```json
{"data": [{"count": 1}], "execution_time_ms": 6, "rows": 1, "success": true}
```

**Verify in Redis:**
```bash
docker exec redis-skardi redis-cli HGETALL mydb:products:PROD001
```

---

## 5. Delete a Product

Delete a product by its product ID.

```bash
curl -X POST http://localhost:8080/delete_product/execute \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD006"}'
```

**Response:**
```json
{"data": [{"count": 1}], "execution_time_ms": 5, "rows": 1, "success": true}
```

**Verify in Redis:**
```bash
docker exec redis-skardi redis-cli KEYS "mydb:products:*"
```

---

## 6. Federated Query: Join CSV + Redis

Join data from multiple sources (CSV file + Redis) and write aggregated results back to Redis.

```
CSV (product_inventory.csv)     Redis (products)
         │                            │
         └──────────┬─────────────────┘
                    │
               DataFusion
            JOIN + Aggregate
                    │
                    ▼
           Redis (product_stats)
```

```bash
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
docker exec redis-skardi redis-cli KEYS "mydb:product_stats:*"
docker exec redis-skardi redis-cli HGETALL mydb:product_stats:Electronics
```

---

## Cleanup

```bash
docker stop redis-skardi && docker rm redis-skardi
pkill -f skardi-server
```

---

## Configuration Reference

### Basic Configuration

```yaml
data_sources:
  - name: "products"
    type: "redis"
    access_mode: "read_write"
    connection_string: "redis://localhost:6379"
    description: "Redis hash table for products"
    options:
      key_space: "mydb"           # Namespace prefix for Redis keys
      table: "products"           # Table name (keys become {key_space}:{table}:{id})
      key_column: "product_id"    # Column derived from the key suffix (optional)
```

### Redis with Authentication

```yaml
data_sources:
  - name: "products"
    type: "redis"
    connection_string: "redis://:mypassword@localhost:6379"
    options:
      key_space: "mydb"
      table: "products"
      key_column: "product_id"
```

### Multiple Redis Databases

```yaml
data_sources:
  - name: "cache_products"
    type: "redis"
    connection_string: "redis://localhost:6379/0"
    options:
      key_space: "app"
      table: "products"
      key_column: "product_id"

  - name: "analytics"
    type: "redis"
    connection_string: "redis://localhost:6379/1"
    options:
      key_space: "analytics"
      table: "events"
```
