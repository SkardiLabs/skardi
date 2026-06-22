# Amazon DynamoDB Integration

This guide demonstrates how to integrate Amazon DynamoDB tables with Skardi. A
DynamoDB table maps to one Skardi table: each item becomes a row and each
top-level attribute becomes a column. Reads (scan + filter pushdown) and writes
(INSERT / UPDATE / DELETE) are both supported.

The examples below use [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html)
so they run without an AWS account. The same context file works against real
Amazon DynamoDB — just change `connection_string` to the regional endpoint.

## Quick Start

```bash
# 1. Start DynamoDB Local in Docker
docker run --name dynamodb-skardi -p 8000:8000 -d amazon/dynamodb-local:2.5.2

# 2. DynamoDB Local ignores credential values but the AWS SDK still requires
#    them to be set. Any non-empty value works.
export AWS_ACCESS_KEY_ID=dummy
export AWS_SECRET_ACCESS_KEY=dummy
export AWS_DEFAULT_REGION=us-east-1
EP="http://localhost:8000"

# 3. Create the products table (partition key: product_id) and seed sample data
aws dynamodb create-table --endpoint-url "$EP" \
  --table-name products \
  --attribute-definitions AttributeName=product_id,AttributeType=S \
  --key-schema AttributeName=product_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST
aws dynamodb wait table-exists --endpoint-url "$EP" --table-name products

put() { aws dynamodb put-item --endpoint-url "$EP" --table-name products --item "$1"; }
put '{"product_id":{"S":"PROD001"},"name":{"S":"Laptop"},"category":{"S":"Electronics"},"price":{"N":"999.99"},"in_stock":{"BOOL":true}}'
put '{"product_id":{"S":"PROD002"},"name":{"S":"Keyboard"},"category":{"S":"Electronics"},"price":{"N":"79.99"},"in_stock":{"BOOL":true}}'
put '{"product_id":{"S":"PROD003"},"name":{"S":"Monitor"},"category":{"S":"Electronics"},"price":{"N":"299.99"},"in_stock":{"BOOL":false}}'
put '{"product_id":{"S":"PROD004"},"name":{"S":"Mouse"},"category":{"S":"Electronics"},"price":{"N":"29.99"},"in_stock":{"BOOL":true}}'
put '{"product_id":{"S":"PROD005"},"name":{"S":"Desk Chair"},"category":{"S":"Furniture"},"price":{"N":"199.99"},"in_stock":{"BOOL":true}}'

# 4. Start the Skardi server against the demo context + pipelines
cargo run --bin skardi-server -- \
  --ctx docs/dynamodb/ctx_dynamodb_demo.yaml \
  --pipeline docs/dynamodb/pipelines/ \
  --port 8080
```

## Data Model

| DynamoDB concept | Maps to |
|---|---|
| Table | One Skardi SQL table |
| Item | Row |
| Top-level attribute | Column |
| Partition key (hash) | First, non-nullable column (`partition_key` option) |
| Sort key (range), if any | Second, non-nullable column (`sort_key` option) |

Attribute types are mapped to Arrow types as follows:

| DynamoDB type | Arrow type |
|---|---|
| `S` (string) | `Utf8` |
| `N` (number, integral) | `Int64` |
| `N` (number, fractional) | `Float64` |
| `BOOL` | `Boolean` |
| everything else (`M`, `L`, `SS`, `B`, …) | `Utf8` (debug-rendered) |

DynamoDB is schemaless, so the column set is inferred by sampling one item at
startup. **An attribute that is absent from an item reads as SQL `NULL`.** If
your table can be empty at startup, only the declared key attributes will be
discoverable — see [Schema notes](#schema-notes).

## Available Pipelines

| Pipeline | Description |
|----------|-------------|
| `query_product_by_id` | Point lookup by partition key |
| `list_all_products` | Full scan of all products |
| `filter_by_category` | Filter pushed down as a DynamoDB `FilterExpression` |
| `insert_product` | Insert (put) a single product |
| `update_product_price` | Update a product's price by ID |
| `delete_product` | Delete a product by ID |
| `federated_join` | Join the CSV inventory with the DynamoDB table |

---

## 1. Point Lookup

```bash
curl -X POST http://localhost:8080/query_product_by_id/execute \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD001"}' | jq .
```

**Response:**
```json
{
  "success": true,
  "data": [{"product_id": "PROD001", "category": "Electronics", "in_stock": true, "name": "Laptop", "price": 999.99}],
  "rows": 1
}
```

---

## 2. Full Scan

```bash
curl -X POST http://localhost:8080/list_all_products/execute \
  -H "Content-Type: application/json" \
  -d '{}' | jq .
```

**Response** (DynamoDB scans return items in unspecified order):
```json
{
  "success": true,
  "data": [
    {"product_id": "PROD001", "category": "Electronics", "in_stock": true,  "name": "Laptop",     "price": 999.99},
    {"product_id": "PROD005", "category": "Furniture",   "in_stock": true,  "name": "Desk Chair", "price": 199.99},
    {"product_id": "PROD004", "category": "Electronics", "in_stock": true,  "name": "Mouse",      "price": 29.99},
    {"product_id": "PROD003", "category": "Electronics", "in_stock": false, "name": "Monitor",    "price": 299.99},
    {"product_id": "PROD002", "category": "Electronics", "in_stock": true,  "name": "Keyboard",   "price": 79.99}
  ],
  "rows": 5
}
```

---

## 3. Filter Pushdown

`WHERE category = {category}` is translated into a DynamoDB `FilterExpression`
(`#n0 = :v0`) and evaluated server-side, so only matching items are returned
over the wire.

```bash
curl -X POST http://localhost:8080/filter_by_category/execute \
  -H "Content-Type: application/json" \
  -d '{"category": "Furniture"}' | jq .
```

**Response:**
```json
{
  "success": true,
  "data": [{"product_id": "PROD005", "category": "Furniture", "in_stock": true, "name": "Desk Chair", "price": 199.99}],
  "rows": 1
}
```

Equality, inequality (`<>`), and range comparisons (`<`, `<=`, `>`, `>=`)
between a column and a literal push down. Other predicates are still applied by
DataFusion after the scan.

---

## 4. Insert

Inserts use DynamoDB `PutItem`, which upserts on the partition key.

```bash
curl -X POST http://localhost:8080/insert_product/execute \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD006", "name": "Webcam", "category": "Electronics", "price": 89.99, "in_stock": true}' | jq .
```

**Response:**
```json
{"success": true, "data": [{"count": 1}], "rows": 1}
```

**Verify:**
```bash
curl -X POST http://localhost:8080/query_product_by_id/execute \
  -H "Content-Type: application/json" -d '{"product_id": "PROD006"}' | jq .
# => {"product_id":"PROD006","category":"Electronics","in_stock":true,"name":"Webcam","price":89.99}
```

---

## 5. Update

DynamoDB cannot update by arbitrary predicate, so Skardi scans for the matching
keys (using filter pushdown) and issues an `UpdateItem` per key. Key columns
(`partition_key` / `sort_key`) cannot be updated.

```bash
curl -X POST http://localhost:8080/update_product_price/execute \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD001", "price": 899.99}' | jq .
```

**Response:**
```json
{"success": true, "data": [{"count": 1}], "rows": 1}
```

---

## 6. Delete

Like UPDATE, DELETE resolves matching keys via a filtered scan, then issues a
`DeleteItem` per key.

```bash
curl -X POST http://localhost:8080/delete_product/execute \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD006"}' | jq .
```

**Response:**
```json
{"success": true, "data": [{"count": 1}], "rows": 1}
```

---

## 7. Federated Query: Join CSV + DynamoDB

Join the CSV inventory file with the DynamoDB `products` table and aggregate.

```bash
curl -X POST http://localhost:8080/federated_join/execute \
  -H "Content-Type: application/json" \
  -d '{"category": "Electronics"}' | jq .
```

**Response** (order unspecified):
```json
{
  "success": true,
  "data": [
    {"product_id": "PROD001", "name": "Laptop",   "category": "Electronics", "price": 899.99, "total_quantity": 80},
    {"product_id": "PROD002", "name": "Keyboard", "category": "Electronics", "price": 79.99,  "total_quantity": 100},
    {"product_id": "PROD003", "name": "Monitor",  "category": "Electronics", "price": 299.99, "total_quantity": 0},
    {"product_id": "PROD004", "name": "Mouse",    "category": "Electronics", "price": 29.99,  "total_quantity": 350}
  ],
  "rows": 4
}
```

---

## Connection Options

| Option | Type | Required | Description |
|---|---|---|---|
| `table` | string | yes | DynamoDB table name |
| `partition_key` | string | no | Partition (hash) key attribute name. Auto-detected from the table's key schema via `DescribeTable`; supply it only as a fallback for when `DescribeTable` is unavailable (e.g. restricted IAM permissions) |
| `sort_key` | string | no | Sort (range) key attribute name for composite-key tables. Also auto-detected from `DescribeTable`; a fallback otherwise |
| `region` | string | no | AWS region (default `us-east-1`) |
| `access_key_env` | string | no | Env var holding the AWS access key id |
| `secret_key_env` | string | no | Env var holding the AWS secret access key |

### Read planning (key-aware access)

Skardi inspects each query's `WHERE` clause and picks the cheapest DynamoDB
access pattern the predicates allow:

| Predicate on the key | DynamoDB API used |
|---|---|
| Full primary key by equality (`pk = …`, or `pk = … AND sk = …`) | `GetItem` — single-item read |
| Partition key by equality, with an optional sort-key condition (`pk = … [AND sk >= …]`) | `Query` — reads only that partition |
| Anything else (non-key filter, or partition key not pinned by equality) | `Scan` — full table read + `FilterExpression` |

A `Scan` reads (and bills for) the entire table regardless of how selective the
filter is, so pinning the partition key turns an O(table) read into an O(1) /
O(partition) one. Equality on a key is required: `pk > …` cannot use
`Query`/`GetItem` and falls back to `Scan`. Non-key predicates are always
re-applied by the query engine after the fetch, so results are identical
whichever path is chosen.

`connection_string` is the **endpoint URL**:

- DynamoDB Local: `http://localhost:8000`
- Amazon DynamoDB: the regional endpoint, e.g. `https://dynamodb.us-east-1.amazonaws.com`

When `access_key_env` / `secret_key_env` are omitted, the default AWS credential
provider chain is used (environment, shared config, IAM role, etc.), so on EC2 /
ECS / Lambda no credential options are needed.

## Schema notes

The schema is inferred by sampling a single item at startup. Because DynamoDB
items are schemaless:

- An attribute missing from the sampled item won't appear as a column. If
  different items have different attributes, declare the union you care about by
  ensuring the sampled item is representative, or keep attributes consistent.
- An attribute absent on a given row reads as SQL `NULL`.
- If the table is **empty** at startup, only the partition key (and sort key, if
  configured) are available until data is written.

## Cleanup

```bash
docker stop dynamodb-skardi && docker rm dynamodb-skardi
pkill -f skardi-server
```
