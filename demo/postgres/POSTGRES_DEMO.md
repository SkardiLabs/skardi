# PostgreSQL Integration Demo

This guide demonstrates how to integrate PostgreSQL tables with Skardi, including INSERT, UPDATE, DELETE operations and federated queries with CSV data.

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
  --ctx demo/ctx_postgres_demo.yaml \
  --pipeline /tmp/pg_query_pipeline.yaml \
  --port 8080

# 6. Execute with parameters
curl -X POST http://localhost:8080/pg_user_query/execute \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1}'
```

## Prerequisites

1. **PostgreSQL Server** running locally or remotely
2. **PostgreSQL Database** with test tables

## Running the Demo

1. **Set environment variables**:
   ```bash
   export PG_USER="skardi_user"
   export PG_PASSWORD="skardi_pass"
   ```

2. **Start Skardi server**:
   ```bash
   cargo run --bin skardi-server -- --ctx demo/ctx_postgres_demo.yaml --port 8080
   ```

3. **Use pre-built query pipelines**:

   Example pipeline files are provided in `demo/postgres_pipelines/`:
   - `query_user_by_id.yaml` - Query user by ID
   - `insert_user.yaml` - Insert new user
   - `update_user_email.yaml` - Update a user's email by name
   - `delete_user.yaml` - Delete a user by name
   - `federated_join_and_insert.yaml` - Join CSV + PostgreSQL and write results

4. **Register and execute pipelines**:

   ```bash
   # Register the query user by ID pipeline
   curl -X POST http://localhost:8080/register_pipeline \
     -H "Content-Type: application/json" \
     -d '{"path": "crates/server/demo/postgres_pipelines/query_user_by_id.yaml"}'

   # Execute with parameters
   curl -X POST http://localhost:8080/query_user_by_id/execute \
     -H "Content-Type: application/json" \
     -d '{"user_id": 1}'
   ```

## Single INSERT Example

Insert a new user into the PostgreSQL table:

```bash
# Register the insert pipeline
curl -X POST http://localhost:8080/register_pipeline \
  -H "Content-Type: application/json" \
  -d '{"path": "crates/server/demo/postgres_pipelines/insert_user.yaml"}'

# Execute INSERT with parameters
curl -X POST http://localhost:8080/insert_user/execute \
  -H "Content-Type: application/json" \
  -d '{"name": "David Brown", "email": "david@example.com"}'
```

**Verify the insert:**
```bash
docker exec postgres-skardi psql -U skardi_user -d mydb \
  -c "SELECT * FROM users"
```

## UPDATE Example

Update an existing user's email address:

```bash
# Register the update pipeline
curl -X POST http://localhost:8080/register_pipeline \
  -H "Content-Type: application/json" \
  -d '{"path": "crates/server/demo/postgres_pipelines/update_user_email.yaml"}'

# Execute UPDATE with parameters
curl -X POST http://localhost:8080/update_user_email/execute \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice Smith", "new_email": "alice.smith@newdomain.com"}'
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
# Register the delete pipeline
curl -X POST http://localhost:8080/register_pipeline \
  -H "Content-Type: application/json" \
  -d '{"path": "demo/postgres_pipelines/delete_user.yaml"}'

# Execute DELETE with parameters
curl -X POST http://localhost:8080/delete_user/execute \
  -H "Content-Type: application/json" \
  -d '{"name": "David Brown"}'
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

The demo uses the same CSV file as the MySQL demo (`crates/server/demo/sample_data/orders.csv`):

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
# Register the federated join pipeline
curl -X POST http://localhost:8080/register_pipeline \
  -H "Content-Type: application/json" \
  -d '{"path": "demo/postgres_pipelines/federated_join_and_insert.yaml"}'

# Execute for Alice Smith
curl -X POST http://localhost:8080/federated_join_and_insert/execute \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice Smith"}'
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
  -d '{"name": "Bob Johnson"}'

# Execute for Carol Williams
curl -X POST http://localhost:8080/federated_join_and_insert/execute \
  -H "Content-Type: application/json" \
  -d '{"name": "Carol Williams"}'
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

## Configuration Reference

### ctx_postgres_demo.yaml

```yaml
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

### Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `table` | Yes | - | Table name to register |
| `schema` | No | `public` | Schema name |
| `user_env` | No | - | Environment variable for username |
| `pass_env` | No | - | Environment variable for password |

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
