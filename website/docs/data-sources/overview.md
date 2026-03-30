---
sidebar_position: 1
---

## Supported Data Sources

### CSV

```yaml
- name: "products"
  type: "csv"
  path: "data/products.csv"
  options:
    has_header: true
    delimiter: ","
    schema_infer_max_records: 1000
```

### Parquet

```yaml
- name: "events"
  type: "parquet"
  path: "data/events.parquet"
```

### PostgreSQL

Full CRUD support (SELECT, INSERT, UPDATE, DELETE) with federated query capability.

```yaml
- name: "users"
  type: "postgres"
  connection_string: "postgresql://localhost:5432/mydb?sslmode=disable"
  options:
    table: "users"
    schema: "public"          # Optional, default: "public"
    user_env: "PG_USER"       # Env var for username
    pass_env: "PG_PASSWORD"   # Env var for password
```

```bash
export PG_USER="myuser"
export PG_PASSWORD="mypassword"
```

For detailed setup, CRUD examples, and federated queries, see [demo/postgres/README.md](https://github.com/SkardiLabs/skardi/blob/main/demo/postgres/README.md).

### MySQL

Full CRUD support (SELECT, INSERT, UPDATE, DELETE) with federated query capability.

```yaml
- name: "users"
  type: "mysql"
  connection_string: "mysql://localhost:3306/mydb"
  options:
    table: "users"
    user_env: "MYSQL_USER"
    pass_env: "MYSQL_PASSWORD"
```

```bash
export MYSQL_USER="myuser"
export MYSQL_PASSWORD="mypassword"
```

For detailed setup, CRUD examples, and federated queries, see [demo/mysql/README.md](https://github.com/SkardiLabs/skardi/blob/main/demo/mysql/README.md).

### SQLite

Full CRUD support (SELECT, INSERT, UPDATE, DELETE) with no external server required — just a local `.db` file.

```yaml
- name: "users"
  type: "sqlite"
  path: "data/my_database.db"
  options:
    table: "users"
    busy_timeout_ms: "5000"     # Optional, default: 5000
```

SQLite requires no credentials — just the path to the database file.

**CLI direct path query** (no context file needed):
```bash
skardi query --sql "SELECT * FROM './data/my_database.db.users'"
```

For detailed setup, CRUD examples, and federated queries, see [demo/sqlite/README.md](https://github.com/SkardiLabs/skardi/blob/main/demo/sqlite/README.md).

### MongoDB

Full CRUD support with point lookups, full scans, and federated queries.

```yaml
- name: "products"
  type: "mongo"
  connection_string: "mongodb://localhost:27017"
  options:
    database: "mydb"
    collection: "products"
    primary_key: "product_id"
    user_env: "MONGO_USER"
    pass_env: "MONGO_PASS"
```

```bash
export MONGO_USER="myuser"
export MONGO_PASS="mypassword"
```

For detailed setup, CRUD examples, and federated queries, see [demo/mongo/README.md](https://github.com/SkardiLabs/skardi/blob/main/demo/mongo/README.md).

### Redis

Full CRUD support with point lookups (O(1) via direct key construction), full scans, and federated queries. Redis hashes map directly to SQL rows.

```yaml
- name: "products"
  type: "redis"
  connection_string: "redis://localhost:6379"
  options:
    key_space: "mydb"
    table: "products"
    key_column: "product_id"
```

Redis keys follow the pattern `{key_space}:{table}:{key_column_value}`, where `key_column` is extracted from the key suffix and exposed as a SQL column. For initially empty tables, use the `columns` option to declare the schema upfront so INSERT operations work immediately.

For detailed setup, CRUD examples, and federated queries, see [demo/redis/README.md](https://github.com/SkardiLabs/skardi/blob/main/demo/redis/README.md).

### Apache Iceberg

Query Iceberg tables with support for schema evolution, partition pruning, and time travel.

```yaml
- name: "nyc_taxi"
  type: "iceberg"
  path: "/path/to/iceberg-warehouse"
  options:
    namespace: "nyc"
    table: "trips"
```

For S3-backed Iceberg tables:

```yaml
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

For detailed setup and examples, see [demo/iceberg/README.md](https://github.com/SkardiLabs/skardi/blob/main/demo/iceberg/README.md).

### Lance (Vector Search & Full-Text Search)

Native KNN (K-Nearest Neighbors) similarity search using the `lance_knn` table function, and BM25-scored full-text search using the `lance_fts` table function.

```yaml
- name: "sift_items"
  type: "lance"
  path: "data/vec_data.lance/"
  description: "Vector embeddings"
```

#### Vector Search (lance_knn)

```sql
SELECT knn.id, knn.item_id, knn._distance
FROM lance_knn(
  'sift_items',          -- table name
  'vector',              -- vector column
  (SELECT vector FROM sift_items WHERE id = {ref_id}),  -- query vector
  {k}                    -- number of neighbors
) knn
WHERE knn.id != {ref_id}
```

| Dataset Size | Without Optimization | With Lance KNN | Speedup |
|--------------|---------------------|----------------|---------|
| 10K vectors  | ~50ms              | ~5ms           | 10x     |
| 100K vectors | ~500ms             | ~8ms           | 62x     |
| 1M vectors   | ~5000ms            | ~15ms          | 333x    |

#### Full-Text Search (lance_fts)

```sql
-- Basic term search (BM25 scored)
SELECT id, description, _score
FROM lance_fts('my_table', 'description', 'search terms', 10)

-- Phrase search
SELECT * FROM lance_fts('my_table', 'description', '"exact phrase"', 10)

-- With WHERE clause filter pushdown
SELECT * FROM lance_fts('my_table', 'description', 'search terms', 10)
WHERE category = 'food' AND price < 20
```

Requires a Lance INVERTED index on the text column. See [demo/lance/README.md](https://github.com/SkardiLabs/skardi/blob/main/demo/lance/README.md) for full details on vector search and full-text search.

### S3 Remote Files

Read CSV, Parquet, and Lance files from S3. Credentials are loaded from environment variables — never from config files.

```yaml
- name: "sales_data"
  type: "parquet"
  location: "remote_s3"
  path: "s3://my-bucket/sales/data.parquet"
  description: "Sales data in S3"
```

Authentication methods: environment variables, AWS CLI profiles, IAM roles, or AWS SSO.

```bash
export AWS_ACCESS_KEY_ID="your_key"
export AWS_SECRET_ACCESS_KEY="your_secret"
# Or use: export AWS_PROFILE="your_profile"
```

For full S3 configuration, IAM permissions, and troubleshooting, see [demo/S3_USAGE.md](https://github.com/SkardiLabs/skardi/blob/main/demo/S3_USAGE.md).
