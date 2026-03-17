# SQLite Integration Demo

This guide demonstrates how to integrate SQLite databases with Skardi, including INSERT, UPDATE, DELETE operations and federated queries with CSV data.

## Quick Start

SQLite requires no external server — just a local `.db` file:

```bash
# 1. Create the SQLite database and test data
sqlite3 demo/sqlite/demo.db << 'EOF'
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

# 1b. Create sample CSV file (for federated query demo)
mkdir -p demo/sample_data
cat > demo/sample_data/orders.csv << 'EOF'
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
  --ctx demo/sqlite/ctx_sqlite_demo.yaml \
  --pipeline demo/sqlite/pipelines/ \
  --port 8080

# 3. Execute with parameters
curl -X POST http://localhost:8080/query_user_by_id/execute \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1}'
```

## Using the CLI (Direct Path Query)

SQLite tables can be queried directly by path — no context file needed. Use the pattern `path/to/file.db.table_name`:

```bash
# Query a table directly
skardi query --sql "SELECT * FROM './demo/sqlite/demo.db.users'"

# Join two tables from the same database
skardi query --sql "
  SELECT u.name, o.product, o.amount
  FROM './demo/sqlite/demo.db.users' u
  JOIN './demo/sqlite/demo.db.orders' o ON u.id = o.user_id
"

# Works with .sqlite and .sqlite3 extensions too
skardi query --sql "SELECT * FROM './data/app.sqlite.customers'"
skardi query --sql "SELECT * FROM './data/app.sqlite3.customers'"
```

## Running the Demo

1. **Create the database** (see Quick Start step 1 above)

2. **Start Skardi server with pipelines**:

   Example pipeline files are provided in `demo/sqlite/pipelines/`:
   - `query_user_by_id.yaml` - Query user by ID
   - `insert_user.yaml` - Insert new user
   - `update_user_email.yaml` - Update a user's email by name
   - `delete_user.yaml` - Delete a user by name
   - `federated_join_and_insert.yaml` - Join CSV + SQLite and write results back

   Pass them all at server start using the `--pipeline` flag (accepts a directory or individual files):
   ```bash
   cargo run --bin skardi-server -- \
     --ctx demo/sqlite/ctx_sqlite_demo.yaml \
     --pipeline demo/sqlite/pipelines/ \
     --port 8080
   ```

3. **Execute pipelines**:

   ```bash
   # Query a user by ID
   curl -X POST http://localhost:8080/query_user_by_id/execute \
     -H "Content-Type: application/json" \
     -d '{"user_id": 1}'
   ```

## Single INSERT Example

Insert a new user into the SQLite table:

```bash
# Execute INSERT with parameters
curl -X POST http://localhost:8080/insert_user/execute \
  -H "Content-Type: application/json" \
  -d '{"name": "David Brown", "email": "david@example.com"}'
```

**Verify the insert:**
```bash
sqlite3 demo/sqlite/demo.db "SELECT * FROM users"
```

## UPDATE Example

Update an existing user's email address:

```bash
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
sqlite3 demo/sqlite/demo.db "SELECT * FROM users WHERE name = 'Alice Smith'"
```

## DELETE Example

Delete a user by name:

```bash
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

**Verify the delete:**
```bash
sqlite3 demo/sqlite/demo.db "SELECT * FROM users"
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
  -d '{"name": "Alice Smith"}'
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
sqlite3 demo/sqlite/demo.db "SELECT * FROM user_order_stats"
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
  -d '{"name": "Bob Johnson"}'

# Execute for Carol
curl -X POST http://localhost:8080/federated_join_and_insert/execute \
  -H "Content-Type: application/json" \
  -d '{"name": "Carol Williams"}'
```

## Troubleshooting

### Database File Not Found
```
Error: Failed to create SQLite connection pool
```
**Solution**: Verify the database file exists and the path is correct:
```bash
ls -la demo/sqlite/demo.db
```

### Table Not Found
```
Error: Failed to create table provider
```
**Solution**: Verify the table exists in the database:
```bash
sqlite3 demo/sqlite/demo.db ".tables"
sqlite3 demo/sqlite/demo.db ".schema users"
```

### Database Locked
```
Error: database is locked
```
**Solution**: SQLite only allows one writer at a time. Check if another process has the database open:
```bash
# Check for processes using the database file
lsof demo/sqlite/demo.db
```

You can also increase the busy timeout via the `busy_timeout_ms` option in the context file:
```yaml
options:
  table: "users"
  busy_timeout_ms: "10000"  # Wait up to 10 seconds for locks
```

## Context File Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `table` | Yes | — | SQLite table name to register |
| `busy_timeout_ms` | No | `5000` | Time in milliseconds to wait for database locks |
