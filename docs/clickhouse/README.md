# ClickHouse Integration

This guide covers querying **ClickHouse** from Skardi.

ClickHouse is a columnar OLAP database with an HTTP query interface. Skardi
registers each ClickHouse table (or a whole server, in catalog mode) as a
DataFusion table, so you can `SELECT`, aggregate, and **federate ClickHouse
data with any other Skardi source** (CSV, Postgres, Lance, …) in a single
query. Filters, projections, and `LIMIT`s are unparsed back to ClickHouse SQL
and executed server-side, so scans stream only the rows a query actually
needs.

> **Access is read-only.** ClickHouse has no transactional UPDATE/DELETE —
> mutations (`ALTER TABLE ... UPDATE/DELETE`) are asynchronous background
> rewrites, and a mid-stream INSERT failure leaves partial parts visible.
> `access_mode: read_write` is rejected for ClickHouse sources.

## Quick Start

```bash
# 1. Start ClickHouse in Docker (same image + credentials as CI)
docker run -d --name clickhouse-skardi \
  -p 8123:8123 \
  -e CLICKHOUSE_DB=mydb \
  -e CLICKHOUSE_USER=skardi_user \
  -e CLICKHOUSE_PASSWORD=skardi_pass \
  clickhouse/clickhouse-server:24.8

# 2. Seed sample data
docker exec -i clickhouse-skardi clickhouse-client \
  --user skardi_user --password skardi_pass --multiquery <<'EOF'
CREATE TABLE mydb.users (
    id UInt32,
    name String,
    email String
) ENGINE = MergeTree ORDER BY id;

INSERT INTO mydb.users VALUES
    (1, 'Alice Smith', 'alice@example.com'),
    (2, 'Bob Johnson', 'bob@example.com'),
    (3, 'Carol Williams', 'carol@example.com');

CREATE TABLE mydb.orders (
    id UInt32,
    user_id UInt32,
    product String,
    amount Float64
) ENGINE = MergeTree ORDER BY id;

INSERT INTO mydb.orders VALUES
    (1, 1, 'Laptop', 999.99),
    (2, 2, 'Keyboard', 79.99),
    (3, 3, 'Monitor', 299.99);

CREATE TABLE mydb.products (
    product_id String,
    name String,
    category Nullable(String),
    price Float64,
    in_stock Bool
) ENGINE = MergeTree ORDER BY product_id;

INSERT INTO mydb.products VALUES
    ('PROD001', 'Laptop', 'Electronics', 999.99, true),
    ('PROD002', 'Keyboard', 'Electronics', 79.99, true),
    ('PROD003', 'Monitor', 'Electronics', 299.99, false),
    ('PROD004', 'Mouse', 'Electronics', 29.99, true),
    ('PROD005', 'Desk Chair', NULL, 199.99, true);
EOF

# 3. Export the credentials the demo context reads via user_env / pass_env
export CLICKHOUSE_USER=skardi_user
export CLICKHOUSE_PASSWORD=skardi_pass

# 4. Start the Skardi server against the demo context + pipelines
cargo run --bin skardi-server -- \
  --ctx docs/clickhouse/ctx_clickhouse_demo.yaml \
  --pipeline docs/clickhouse/pipelines/ \
  --port 8080
```

## Data Model

A ClickHouse table maps 1:1 to a SQL table; column types map to Arrow:

| ClickHouse type | Arrow / SQL projection |
|-----------------|------------------------|
| `UInt32` / `Int64` / … | corresponding integer column |
| `String` | `Utf8` column |
| `Float64` | `Float64` column |
| `Nullable(T)` | nullable column of `T` |
| `Bool` | `Boolean` column |
| `DateTime64` | `Timestamp` column |
| `DateTime` / `Date` | `UInt32` / `UInt16` (raw epoch days/seconds — ClickHouse's ArrowStream format does not tag them as timestamps; use `DateTime64` for a real `Timestamp` column) |

Each table-mode data source binds to **one** ClickHouse table (via the `table`
option). To expose several tables, declare one data source per table — see
[`ctx_clickhouse_demo.yaml`](ctx_clickhouse_demo.yaml) — or register the whole
server at once with [catalog mode](#catalog-mode).

The table's Arrow schema is inferred at server startup with a
`SELECT * … LIMIT 0` probe (via ClickHouse's ArrowStream output format, plus
an engine lookup in `system.tables`), so the ClickHouse endpoint must be
reachable when Skardi loads its context (the same eager-connect behaviour as
the Postgres/MySQL/Mongo providers). An empty table registers cleanly — the
schema never depends on sampled rows. Note this means a table whose engine
refuses direct SELECT (e.g. `Kafka`) cannot be registered in table mode.

## Query Pushdown

Pipeline SQL is planned by DataFusion, and the table scan underneath is
unparsed back into ClickHouse SQL: predicates on the scanned table,
projections, and bare `LIMIT`s run **inside ClickHouse**, so a
`WHERE id = {user_id}` pipeline transfers one row, not the table. Joins and
aggregations execute in Skardi after the (already filtered) scans return —
including joins between two tables on the *same* ClickHouse server. `ORDER BY`
is not pushed down either, so a `LIMIT` that sits above an `ORDER BY` also
runs in Skardi (the scan still only fetches the projected columns).

**Aggregates are not pushed down.** A bare `SELECT count(*) FROM t` streams
one column of `t` (the narrowest fixed-width column) over HTTP and counts
client-side, instead of letting ClickHouse answer from part metadata. On
OLAP-sized tables, put a selective `WHERE` on such queries — or query a
pre-aggregated table — until aggregate pushdown lands (upstream's
`clickhouse-federation` feature).

## Available Pipelines

Every pipeline is **parameterised** — each `execute` call supplies its
parameters in the JSON request body (the `{name}` placeholders in the pipeline
SQL).

| Pipeline | Parameters | Description |
|----------|------------|-------------|
| `query_user_by_id` | `user_id` | Point lookup by primary key |
| `list_all_users` | `limit` | Full scan; the `ORDER BY` + `LIMIT` run in Skardi |
| `products_by_category` | `category` | Filter on a `Nullable(String)` column |
| `user_order_summary` | `min_total` | Join users ⨝ orders, aggregate spend per user |
| `federated_stock_value` | `warehouse` | Join ClickHouse products with a CSV warehouse map |

> The response bodies below are captured from a live run against
> `clickhouse/clickhouse-server:24.8` with the seed data above, using the
> parameter values shown in each request. `execution_time_ms` and the
> `timestamp` field (elided from the examples) vary per run.

---

## 1. Point Lookup

```bash
curl -X POST http://localhost:8080/query_user_by_id/execute \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1}' | jq .
```

**Response:**
```json
{
  "success": true,
  "data": [
    {"id": 1, "name": "Alice Smith", "email": "alice@example.com"}
  ],
  "rows": 1,
  "execution_time_ms": 80
}
```

---

## 2. Full Scan with Limit

```bash
curl -X POST http://localhost:8080/list_all_users/execute \
  -H "Content-Type: application/json" \
  -d '{"limit": 10}' | jq .
```

**Response:**
```json
{
  "success": true,
  "data": [
    {"id": 1, "name": "Alice Smith", "email": "alice@example.com"},
    {"id": 2, "name": "Bob Johnson", "email": "bob@example.com"},
    {"id": 3, "name": "Carol Williams", "email": "carol@example.com"}
  ],
  "rows": 3,
  "execution_time_ms": 12
}
```

---

## 3. Filter on a Nullable Column

`PROD005` has a `NULL` category and is excluded by the equality filter, as SQL
semantics demand.

```bash
curl -X POST http://localhost:8080/products_by_category/execute \
  -H "Content-Type: application/json" \
  -d '{"category": "Electronics"}' | jq .
```

**Response:**
```json
{
  "success": true,
  "data": [
    {"product_id": "PROD001", "name": "Laptop", "price": 999.99, "in_stock": true},
    {"product_id": "PROD003", "name": "Monitor", "price": 299.99, "in_stock": false},
    {"product_id": "PROD002", "name": "Keyboard", "price": 79.99, "in_stock": true},
    {"product_id": "PROD004", "name": "Mouse", "price": 29.99, "in_stock": true}
  ],
  "rows": 4,
  "execution_time_ms": 14
}
```

---

## 4. Join + Aggregation

```bash
curl -X POST http://localhost:8080/user_order_summary/execute \
  -H "Content-Type: application/json" \
  -d '{"min_total": 100}' | jq .
```

**Response:** (Bob's 79.99 keyboard order is below the threshold)
```json
{
  "success": true,
  "data": [
    {"name": "Alice Smith", "order_count": 1, "total_spent": 999.99},
    {"name": "Carol Williams", "order_count": 1, "total_spent": 299.99}
  ],
  "rows": 2,
  "execution_time_ms": 29
}
```

---

## 5. Federated Query: ClickHouse ⨝ CSV

Join the ClickHouse `products` table with a CSV product → warehouse map and
roll up the stock value per warehouse — a single SQL query spanning two
backends.

```
ClickHouse (products)   CSV (product_inventory.csv)
      │                          │
      └────────────┬─────────────┘
                   │
              DataFusion
            JOIN + Aggregate
                   │
                   ▼
        per-warehouse stock value
```

```bash
curl -X POST http://localhost:8080/federated_stock_value/execute \
  -H "Content-Type: application/json" \
  -d '{"warehouse": "us-west"}' | jq .
```

**Response:** (`PROD001`, `PROD002`, `PROD005` are stocked in `us-west`)
```json
{
  "success": true,
  "data": [
    {"warehouse": "us-west", "skus": 3, "stock_value": 16999.39}
  ],
  "rows": 1,
  "execution_time_ms": 16
}
```

---

## Catalog Mode

Instead of declaring one data source per table, register the whole server as a
DataFusion catalog. Every table in the allowed databases becomes addressable
as `ch_catalog.<database>.<table>`:

```bash
cargo run --bin skardi-server -- \
  --ctx docs/clickhouse/ctx_clickhouse_catalog_demo.yaml \
  --pipeline docs/clickhouse/pipelines/catalog_demo/ \
  --port 8080
```

```bash
curl -X POST http://localhost:8080/clickhouse-catalog-list-users/execute \
  -H "Content-Type: application/json" \
  -d '{"limit": 2}' | jq .
```

**Response:**
```json
{
  "success": true,
  "data": [
    {"id": 1, "name": "Alice Smith", "email": "alice@example.com"},
    {"id": 2, "name": "Bob Johnson", "email": "bob@example.com"}
  ],
  "rows": 2,
  "execution_time_ms": 236
}
```

```bash
curl -X POST http://localhost:8080/clickhouse-catalog-cross-table-join/execute \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1}' | jq .
```

**Response:**
```json
{
  "success": true,
  "data": [
    {"name": "Alice Smith", "product": "Laptop", "amount": 999.99}
  ],
  "rows": 1,
  "execution_time_ms": 84
}
```

Catalog mode must not mix with per-table options — `table` / `schema` /
`database` are rejected when `hierarchy_level: catalog` is set. Use
`allowed_schemas` to restrict which databases are exposed; omit it to include
every non-system database.

Catalog assembly is **best-effort**: a table whose schema can't be fetched
(e.g. a view over a dropped table, or a permissions gap) is skipped with a
warning instead of failing startup. Stream-like engine tables (`Kafka`,
`RabbitMQ`, `NATS`, `FileLog` — engines that refuse direct SELECT) and
materialized-view inner tables (`.inner…` names) are excluded up front.

---

## Cleanup

```bash
docker stop clickhouse-skardi && docker rm clickhouse-skardi
pkill -f skardi-server
```

---

## Connection Options

ClickHouse sources use `connection_string` for the **HTTP interface** URL
(e.g. `http://localhost:8123`, or `https://…` for TLS/ClickHouse Cloud) and
the following `options` keys:

| Option | Required | Description |
|--------|----------|-------------|
| `table` | table mode | ClickHouse table to register. |
| `database` | no | Database holding the table (defaults to the server's default database, usually `default`). |
| `allowed_schemas` | no (catalog mode) | Comma-separated database allow-list; omit to expose all non-system databases. |
| `user_env` | for auth | **Name of an environment variable** holding the username. The out-of-the-box `default` user needs no credentials, so this is optional. |
| `pass_env` | for auth | **Name of an environment variable** holding the password. |

Option validation is strict and runs at registration, when the server loads
its `--ctx` file: an unrecognised key — e.g. a misspelled `pass_env`, which
would otherwise silently connect as the `default` user — is a hard error, as
is an option that belongs to the other hierarchy mode (`table`/`database` in
catalog mode, `allowed_schemas` in table mode) or an `allowed_schemas` with no
non-empty entry.

The native TCP protocol (port 9000) is not supported — registration rejects
non-`http(s)` schemes. Credentials embedded in the URL
(`http://user:pass@host`) are rejected outright, and so is any URL query
string (ClickHouse accepts `?user=…&password=…` as HTTP auth) — the
connection pool would ignore them, and connection strings are logged and
surfaced by the data-sources API. Use `user_env` / `pass_env` so secrets stay
out of the YAML, the logs, and the API.

## Access Mode

This source is **read-only**. Declaring `access_mode: read_write` on a
ClickHouse source is rejected at the provider boundary when the server loads
its `--ctx` file — and ClickHouse sources are rejected as job destinations
too. Ingest into ClickHouse should go through ClickHouse's own
INSERT pipelines (Kafka engine, `clickhouse-client`, HTTP inserts) — Skardi is
the query side.
