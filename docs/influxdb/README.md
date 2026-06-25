# InfluxDB 3 Integration

This guide covers querying **InfluxDB 3** (Core / Enterprise) from Skardi.

InfluxDB 3 is itself built on Apache Arrow + DataFusion and exposes an Arrow
**Flight SQL** endpoint. Skardi connects to that endpoint and registers each
measurement (or an arbitrary SQL query) as a table, so you can `SELECT`,
aggregate, and **federate InfluxDB time-series data with any other Skardi
source** (CSV, Postgres, Iceberg, …) in a single query.

> **Access is read-only.** Flight SQL serves `SELECT`s only. Writes to InfluxDB
> go through the line-protocol ingest API and are out of scope for the query
> engine — `access_mode: read_write` is rejected for InfluxDB sources.

## Quick Start

```bash
# 1. Start InfluxDB 3 Core in Docker (auth disabled for the demo)
docker run -d --name influxdb3-skardi \
  -p 8181:8181 \
  -v influxdb3-skardi-data:/var/lib/influxdb3 \
  influxdb:3-core influxdb3 serve \
    --node-id node0 \
    --object-store file \
    --data-dir /var/lib/influxdb3 \
    --without-auth

# 2. Create the database
curl -s -XPOST "http://localhost:8181/api/v3/configure/database" \
  -H "Content-Type: application/json" \
  -d '{"db": "metrics"}'

# 3. Write sample data (line protocol: cpu + mem measurements)
curl -s "http://localhost:8181/api/v3/write_lp?db=metrics&precision=second" \
  --data-binary @- << 'EOF'
cpu,host=host1,region=us-west usage_user=12.5,usage_system=3.2 1700000000
cpu,host=host1,region=us-west usage_user=64.1,usage_system=9.8 1700000060
cpu,host=host2,region=us-west usage_user=41.0,usage_system=6.0 1700000000
cpu,host=host2,region=us-west usage_user=88.7,usage_system=12.3 1700000060
cpu,host=host3,region=us-east usage_user=22.4,usage_system=4.1 1700000000
mem,host=host1,region=us-west used_percent=48.2 1700000000
mem,host=host2,region=us-west used_percent=73.9 1700000000
mem,host=host3,region=us-east used_percent=31.5 1700000000
EOF

# 4. Start the Skardi server against the demo context + pipelines
cargo run --bin skardi-server -- \
  --ctx docs/influxdb/ctx_influxdb_demo.yaml \
  --pipeline docs/influxdb/pipelines/ \
  --port 8080
```

> **Image tag note:** `influxdb:3-core` tracks the latest InfluxDB 3 Core
> release. If your environment pins a specific version, substitute it here.

## Data Model

An InfluxDB measurement maps to a SQL table:

| InfluxDB concept | SQL projection |
|------------------|----------------|
| measurement (`cpu`) | table (`cpu`) |
| tag (`host`, `region`) | `Utf8` column |
| field (`usage_user`) | `Float64` / typed column |
| `time` | `Timestamp(ns)` column |

Each Skardi data source binds to **one** measurement (via the `measurement`
option) or **one** SQL query (via the `query` option). To expose several
measurements, declare one data source per measurement — see
[`ctx_influxdb_demo.yaml`](ctx_influxdb_demo.yaml), which registers both `cpu`
and `mem`.

The table's Arrow schema is inferred at server startup from InfluxDB's
`GetFlightInfo` response, so the InfluxDB endpoint must be reachable when Skardi
loads its context (the same eager-connect behaviour as the Postgres/MySQL/Mongo
providers).

## Available Pipelines

| Pipeline | Description |
|----------|-------------|
| `list_all_cpu` | Full scan of CPU samples, newest first |
| `cpu_by_host` | CPU samples for one host (tag filter) |
| `high_cpu` | Samples above a user-CPU threshold (parameterised) |
| `avg_usage_by_host` | Average user CPU per host (aggregation) |
| `federated_cpu_by_datacenter` | Join InfluxDB `cpu` with a CSV host map, aggregate per datacenter |

> The response bodies below are captured from a live run against InfluxDB 3
> Core with the sample data above. `execution_time_ms` and `timestamp` vary
> per run.

---

## 1. Full Scan

```bash
curl -X POST http://localhost:8080/list_all_cpu/execute \
  -H "Content-Type: application/json" \
  -d '{}' | jq .
```

**Response:**
```json
{
  "success": true,
  "data": [
    {"host": "host1", "region": "us-west", "time": "2023-11-14T22:14:20", "usage_user": 64.1, "usage_system": 9.8},
    {"host": "host2", "region": "us-west", "time": "2023-11-14T22:14:20", "usage_user": 88.7, "usage_system": 12.3},
    {"host": "host1", "region": "us-west", "time": "2023-11-14T22:13:20", "usage_user": 12.5, "usage_system": 3.2},
    {"host": "host2", "region": "us-west", "time": "2023-11-14T22:13:20", "usage_user": 41.0, "usage_system": 6.0},
    {"host": "host3", "region": "us-east", "time": "2023-11-14T22:13:20", "usage_user": 22.4, "usage_system": 4.1}
  ],
  "rows": 5,
  "execution_time_ms": 668
}
```

---

## 2. Filter by Host

```bash
curl -X POST http://localhost:8080/cpu_by_host/execute \
  -H "Content-Type: application/json" \
  -d '{"host": "host1"}' | jq .
```

**Response:**
```json
{
  "success": true,
  "data": [
    {"host": "host1", "time": "2023-11-14T22:14:20", "usage_user": 64.1, "usage_system": 9.8},
    {"host": "host1", "time": "2023-11-14T22:13:20", "usage_user": 12.5, "usage_system": 3.2}
  ],
  "rows": 2,
  "execution_time_ms": 170
}
```

---

## 3. Threshold Filter

```bash
curl -X POST http://localhost:8080/high_cpu/execute \
  -H "Content-Type: application/json" \
  -d '{"threshold": 50}' | jq .
```

**Response:**
```json
{
  "success": true,
  "data": [
    {"host": "host2", "time": "2023-11-14T22:14:20", "usage_user": 88.7},
    {"host": "host1", "time": "2023-11-14T22:14:20", "usage_user": 64.1}
  ],
  "rows": 2,
  "execution_time_ms": 164
}
```

---

## 4. Aggregation

```bash
curl -X POST http://localhost:8080/avg_usage_by_host/execute \
  -H "Content-Type: application/json" \
  -d '{}' | jq .
```

**Response:**
```json
{
  "success": true,
  "data": [
    {"host": "host1", "avg_user": 38.3, "samples": 2},
    {"host": "host2", "avg_user": 64.85, "samples": 2},
    {"host": "host3", "avg_user": 22.4, "samples": 1}
  ],
  "rows": 3,
  "execution_time_ms": 169
}
```

---

## 5. Federated Query: InfluxDB ⨝ CSV

Join the InfluxDB `cpu` measurement with a CSV host → datacenter map and
aggregate per datacenter — a single SQL query spanning two backends.

```
InfluxDB (cpu)          CSV (host_metadata.csv)
      │                          │
      └────────────┬─────────────┘
                   │
              DataFusion
            JOIN + Aggregate
                   │
                   ▼
        per-datacenter rollup
```

```bash
curl -X POST http://localhost:8080/federated_cpu_by_datacenter/execute \
  -H "Content-Type: application/json" \
  -d '{}' | jq .
```

**Response:**
```json
{
  "success": true,
  "data": [
    {"datacenter": "us-west-1b", "owner": "platform", "avg_user": 64.85, "max_user": 88.7, "samples": 2},
    {"datacenter": "us-west-1a", "owner": "platform", "avg_user": 38.3, "max_user": 64.1, "samples": 2},
    {"datacenter": "us-east-2a", "owner": "analytics", "avg_user": 22.4, "max_user": 22.4, "samples": 1}
  ],
  "rows": 3,
  "execution_time_ms": 167
}
```

---

## Cleanup

```bash
docker stop influxdb3-skardi && docker rm influxdb3-skardi
docker volume rm influxdb3-skardi-data
pkill -f skardi-server
```

---

## Connection Options

InfluxDB sources use `connection_string` for the Flight gRPC endpoint URL
(e.g. `http://localhost:8181`, or `https://…` for TLS) and the following
`options` keys:

| Option | Required | Description |
|--------|----------|-------------|
| `measurement` (alias `table`) | one of these | Measurement name; expands to `SELECT * FROM "<measurement>"`. |
| `query` | or this | Full SQL backing the table (overrides `measurement`). |
| `database` | recommended | InfluxDB 3 database / bucket; sent as the `database` gRPC header so the server picks the right database. |
| `token` | for auth | API token; sent as `authorization: Bearer <token>`. |
| `flight.sql.*` | optional | Any raw Flight SQL driver option, forwarded verbatim (takes precedence). E.g. `flight.sql.username`, `flight.sql.password`, `flight.sql.header.<name>`. |

### With Authentication (production)

Real InfluxDB deployments require a token. Drop `--without-auth`, mint an admin
token (`docker exec influxdb3-skardi influxdb3 create token --admin`), and add
it to each source:

```yaml
spec:
  data_sources:
    - name: "cpu"
      type: "influxdb"
      connection_string: "http://localhost:8181"
      options:
        database: "metrics"
        measurement: "cpu"
        token: "apiv3_your_token_here"
```

### Custom Query / Pre-aggregation

Push work into InfluxDB by binding a table to a custom query instead of a whole
measurement:

```yaml
    - name: "cpu_hourly"
      type: "influxdb"
      connection_string: "http://localhost:8181"
      options:
        database: "metrics"
        query: "SELECT host, date_bin(INTERVAL '1 hour', time) AS hour, avg(usage_user) AS avg_user FROM cpu GROUP BY host, hour"
```
