# Apache Iceberg Integration Demo

This guide demonstrates how to integrate Apache Iceberg tables with Skardi.

## Overview

Apache Iceberg is an open table format for huge analytic datasets. Skardi supports querying Iceberg tables using DataFusion's native Iceberg integration, enabling:

- **Time travel queries** - Query historical snapshots
- **Schema evolution** - Handle schema changes gracefully
- **Partition pruning** - Efficient queries on partitioned data
- **Federated queries** - Join Iceberg data with other sources (CSV, Parquet, PostgreSQL, etc.)

## Quick Start

### 1. Create Sample Iceberg Data

First, create a sample Iceberg warehouse with test data. You can use PySpark or any Iceberg-compatible tool:


```python
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType, TimestampType, IntegerType, DoubleType, NestedField
)
import pyarrow as pa

# Create local filesystem catalog
catalog = load_catalog(
    "local",
    **{
        "type": "sql",
        "uri": "sqlite:////tmp/iceberg-warehouse/catalog.db",
        "warehouse": "/tmp/iceberg-warehouse"
    }
)

# Create namespace
catalog.create_namespace("nyc")

# Define schema
schema = Schema(
    NestedField(1, "trip_id", StringType(), required=True),
    NestedField(2, "pickup_datetime", TimestampType(), required=True),
    NestedField(3, "dropoff_datetime", TimestampType(), required=True),
    NestedField(4, "pickup_location_id", IntegerType(), required=True),
    NestedField(5, "dropoff_location_id", IntegerType(), required=True),
    NestedField(6, "passenger_count", IntegerType(), required=False),
    NestedField(7, "trip_distance", DoubleType(), required=False),
    NestedField(8, "fare_amount", DoubleType(), required=False),
    NestedField(9, "tip_amount", DoubleType(), required=False),
    NestedField(10, "total_amount", DoubleType(), required=False),
)

# Create table
table = catalog.create_table("nyc.trips", schema=schema)

# Append data using PyArrow
data = pa.table({
    "trip_id": ["trip_001", "trip_002", "trip_003"],
    "pickup_datetime": pa.array([...]),
    # ... more columns
})
table.append(data)
```

**Or use the provided init script with a virtual environment:**

```bash
# Navigate to the iceberg pipelines directory
cd demo/iceberg_pipelines

# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install PyIceberg with required extras
pip install "pyiceberg[pyarrow,sqlite]"

# Run the initialization script
python init_data.py

# Deactivate when done
deactivate
```

### 2. Configure Skardi Context

Create a context file (`ctx_iceberg_demo.yaml`):

```yaml
data_sources:
  - name: "nyc_taxi"
    type: "iceberg"
    path: "/tmp/iceberg-warehouse"
    description: "NYC taxi trips Iceberg catalog"
    options:
      namespace: "nyc"
      table: "trips"
  - name: "zones"
    type: "csv"
    path: "crates/server/demo/sample_data/taxi_zones.csv"
    description: "NYC taxi zone lookup"
```

### 3. Start Skardi Server

```bash
cargo run --bin skardi-server -- \
  --ctx demo/ctx_iceberg_demo.yaml \
  --pipeline demo/iceberg_pipelines/query_trips_by_date.yaml \
  --port 8080
```

### 4. Query the Data

```bash
# Query trips by date range
curl -X POST http://localhost:8080/query-trips-by-date/execute \
  -H "Content-Type: application/json" \
  -d '{
    "start_date": "2024-01-15",
    "end_date": "2024-01-17",
    "limit": 10
  }'
```

**Expected Response:**
```json
{
  "success": true,
  "data": [
    {
      "trip_id": "trip_001",
      "pickup_datetime": "2024-01-15T08:30:00",
      "dropoff_datetime": "2024-01-15T08:45:00",
      "passenger_count": 2,
      "trip_distance": 3.5,
      "fare_amount": 15.00,
      "tip_amount": 3.00,
      "total_amount": 18.00
    }
  ],
  "rows": 1,
  "execution_time_ms": 45
}
```

## Query Syntax

Iceberg tables are registered directly with DataFusion, allowing simple table names:

```sql
SELECT * FROM table_name
```

Where `table_name` is the `name` field in your data source config.

**Example:**
```yaml
# Config
- name: "nyc_taxi"          # table name in queries
  type: "iceberg"
  options:
    namespace: "nyc"        # Iceberg namespace (used to locate the table)
    table: "trips"          # Iceberg table name (used to locate the table)
```

```sql
-- Query (simple table name!)
SELECT * FROM nyc_taxi
```

This makes it easy to join Iceberg tables with other data sources:
```sql
SELECT * FROM nyc_taxi t JOIN zones z ON t.pickup_location_id = z.location_id
```

## Example Pipelines

### Query Trips by Date

```yaml
metadata:
  name: "query-trips-by-date"
  version: "1.0.0"

query: |
  SELECT
    trip_id,
    pickup_datetime,
    passenger_count,
    trip_distance,
    total_amount
  FROM nyc_taxi
  WHERE pickup_datetime >= {start_date}
    AND pickup_datetime < {end_date}
  ORDER BY pickup_datetime
  LIMIT {limit}
```

### Trip Statistics by Location

```yaml
metadata:
  name: "trip-statistics"
  version: "1.0.0"

query: |
  SELECT
    pickup_location_id,
    COUNT(*) as total_trips,
    AVG(trip_distance) as avg_distance,
    AVG(fare_amount) as avg_fare,
    SUM(total_amount) as total_revenue
  FROM nyc_taxi
  WHERE pickup_location_id = {location_id}
  GROUP BY pickup_location_id
```

### Federated Query: Join with CSV

Join Iceberg trip data with a CSV zone lookup file:

```yaml
metadata:
  name: "federated-join-zones"
  version: "1.0.0"

query: |
  SELECT
    t.trip_id,
    t.pickup_datetime,
    z.zone_name as pickup_zone,
    z.borough as pickup_borough,
    t.total_amount
  FROM nyc_taxi t
  INNER JOIN zones z ON t.pickup_location_id = z.location_id
  WHERE z.borough = {borough}
  LIMIT {limit}
```

```bash
curl -X POST http://localhost:8080/federated-join-zones/execute \
  -H "Content-Type: application/json" \
  -d '{"borough": "Manhattan", "limit": 10}'
```

## S3 Iceberg Tables

For Iceberg tables stored on S3:

```yaml
data_sources:
  - name: "s3_iceberg"
    type: "iceberg"
    path: "s3://my-bucket/iceberg-warehouse"
    options:
      namespace: "production"
      table: "events"
      aws_region: "us-east-1"
      aws_access_key_id_env: "AWS_ACCESS_KEY_ID"
      aws_secret_access_key_env: "AWS_SECRET_ACCESS_KEY"
```

```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"

cargo run --bin skardi-server -- \
  --ctx ctx_s3_iceberg.yaml \
  --pipeline your_pipeline.yaml \
  --port 8080
```

## Troubleshooting

### Table Not Found

```
Error: Table not found: nyc.trips
```

**Solutions:**
1. Verify the warehouse path is correct
2. Check that namespace and table exist in the Iceberg catalog
3. Ensure metadata files are present: `ls /tmp/iceberg-warehouse/nyc/trips/metadata/`

### Metadata Version Mismatch

```
Error: Failed to load table metadata
```

**Solutions:**
1. Ensure you're using a compatible Iceberg format version
2. Check that metadata files are not corrupted
3. Verify file permissions on the warehouse directory

### Schema Evolution Issues

If your table schema has evolved:
1. Skardi reads the current schema from metadata
2. Historical data is automatically projected to the current schema
3. New columns added will show as NULL for older data

### Performance Tips

1. **Partition pruning**: Add WHERE clauses on partition columns
   ```sql
   WHERE pickup_datetime >= '2024-01-01'  -- Uses partition pruning
   ```

2. **Column projection**: Select only needed columns
   ```sql
   SELECT trip_id, total_amount  -- Better than SELECT *
   FROM nyc_taxi
   ```

3. **Predicate pushdown**: Filters are pushed to Parquet readers
   ```sql
   WHERE fare_amount > 10.0  -- Pushed down to file readers
   ```

## Comparison with Other Sources

| Feature | Iceberg | Parquet | CSV |
|---------|---------|---------|-----|
| Schema evolution | ✅ | ❌ | ❌ |
| Time travel | ✅ | ❌ | ❌ |
| Partition pruning | ✅ | Manual | ❌ |
| ACID transactions | ✅ | ❌ | ❌ |
| File format | Parquet | Parquet | CSV |
| Metadata overhead | Medium | None | None |
