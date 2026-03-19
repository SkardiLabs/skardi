# MySQL Integration Demo

This guide demonstrates how to integrate MySQL tables with Skardi, including INSERT, UPDATE, DELETE operations and federated queries with CSV data.

## Quick Start (Docker)

For the fastest setup, use Docker:

```bash
# 1. Start MySQL in Docker
docker run --name mysql-skardi \
  -e MYSQL_ROOT_PASSWORD=rootpass \
  -e MYSQL_DATABASE=mydb \
  -e MYSQL_USER=skardi_user \
  -e MYSQL_PASSWORD=skardi_pass \
  -p 3306:3306 \
  -d mysql:8.0

# 2. Create test data
docker exec -i mysql-skardi mysql -u skardi_user -pskardi_pass mydb << 'EOF'
CREATE TABLE users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL
);
CREATE TABLE orders (
    id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    product VARCHAR(100) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL
);
CREATE TABLE user_order_stats (
    id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    user_name VARCHAR(100) NOT NULL,
    user_email VARCHAR(100) NOT NULL,
    total_orders INT NOT NULL,
    total_spent DECIMAL(10, 2) NOT NULL,
    last_order_date VARCHAR(50),
    UNIQUE KEY unique_user (user_id)
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

# 2b. Create sample CSV file (for federated query demo)
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

# 3. Set environment variables
export MYSQL_USER="skardi_user"
export MYSQL_PASSWORD="skardi_pass"

# 4. Create a pipeline for querying
cat > /tmp/mysql_query_pipeline.yaml << 'EOF'
name: "mysql_user_query"
version: "1.0"
query:
  sql: "SELECT * FROM users WHERE id = {user_id}"
EOF

# 5. Start Skardi
cargo run --bin skardi-server -- \
  --ctx demo/mysql/ctx_mysql_demo.yaml \
  --pipeline path/to/mysql_query_pipeline.yaml \
  --port 8080

# 6. Execute with parameters
curl -X POST http://localhost:8080/mysql_user_query/execute \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1}'
```

## Prerequisites

1. **MySQL Server** running locally or remotely
2. **MySQL Database** with test tables

## Running the Demo

1. **Set environment variables**:
   ```bash
   export MYSQL_USER="skardi_user"
   export MYSQL_PASSWORD="skardi_pass"
   ```

2. **Start Skardi server with pipelines**:

   Example pipeline files are provided in `demo/mysql/pipelines/`:
   - `query_user_by_id.yaml` - Query user by ID
   - `search_users_by_email.yaml` - Search by email pattern
   - `user_orders_summary.yaml` - Get user's order summary
   - `insert_user.yaml` - Insert new user
   - `update_user_email.yaml` - Update a user's email by name
   - `delete_user.yaml` - Delete a user by name

   Pass them all at server start using the `--pipeline` flag (accepts a directory or individual files):
   ```bash
   cargo run --bin skardi-server -- \
     --ctx demo/mysql/ctx_mysql_demo.yaml \
     --pipeline demo/mysql/pipelines/ \
     --port 8080
   ```

   You can also create your own:
   ```bash
   # Create a custom pipeline
   cat > /tmp/my_query.yaml << 'EOF'
   name: "my_custom_query"
   version: "1.0"
   query:
     sql: "SELECT * FROM users WHERE id = {user_id}"
   EOF
   ```

3. **Execute pipelines**:

   ```bash
   # Execute with parameters
   curl -X POST http://localhost:8080/query_user_by_id/execute \
     -H "Content-Type: application/json" \
     -d '{"user_id": 1}'

   # Search for users by email pattern
   curl -X POST http://localhost:8080/search_users_by_email/execute \
     -H "Content-Type: application/json" \
     -d '{"email_pattern": "%@example.com"}'

   # Get user's order summary
   curl -X POST http://localhost:8080/user_orders_summary/execute \
     -H "Content-Type: application/json" \
     -d '{"user_id": 1}'
   ```

## Single INSERT Example

Insert a new user into the MySQL table:

```bash
# Execute INSERT with parameters
curl -X POST http://localhost:8080/insert_user/execute \
  -H "Content-Type: application/json" \
  -d '{"name": "David Brown", "email": "david@example.com"}'
```

**Verify the insert:**
```bash
docker exec mysql-skardi mysql -u skardi_user -pskardi_pass mydb \
  -e "SELECT * FROM users"
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
docker exec mysql-skardi mysql -u skardi_user -pskardi_pass mydb \
  -e "SELECT * FROM users WHERE name = 'Alice Smith'"
```

**Update multiple columns at once** by extending the pipeline SQL:
```sql
UPDATE users SET email = {new_email}, name = {new_name} WHERE name = {name}
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

The `count` field reports the number of rows deleted. A value of `0` means no row matched the `WHERE` clause.

**Verify the delete:**
```bash
docker exec mysql-skardi mysql -u skardi_user -pskardi_pass mydb \
  -e "SELECT * FROM users"
```

> **Note:** Omitting the `WHERE` clause deletes all rows in the table. Always double-check your filter parameters before executing a DELETE pipeline against production data.

## Federated Query Example: Join CSV + MySQL

This example demonstrates **joining data from multiple sources** (CSV file + MySQL table) and writing the aggregated results back to MySQL.

### What This Does

```
CSV File (orders.csv)         MySQL (users table)
6 rows of order data    +     3 rows of user data
         │                             │
         └─────────┬───────────────────┘
                   │
              DataFusion
           JOIN + Aggregate
                   │
                   ▼
        MySQL (user_order_stats)
     Aggregated statistics per user
```

### Pipeline

Create `federated_join.yaml`:

```yaml
metadata:
  name: "federated_join_and_insert"
  version: "1.0"
  description: "Join CSV orders with MySQL users (filtered by name) and write aggregated results to MySQL"

query: |
  INSERT INTO user_order_stats (user_id, user_name, user_email, total_orders, total_spent, last_order_date)
  SELECT
    u.id as user_id,
    u.name as user_name,
    u.email as user_email,
    COUNT(o.order_id) as total_orders,
    SUM(o.amount) as total_spent,
    MAX(o.order_date) as last_order_date
  FROM users u                    -- MySQL table
  INNER JOIN csv_orders o         -- CSV file
    ON u.id = o.user_id
  WHERE u.name = {name}           -- Filter by user name (HTTP parameter)
  GROUP BY u.id, u.name, u.email
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
docker exec mysql-skardi mysql -u skardi_user -pskardi_pass mydb \
  -e "SELECT * FROM user_order_stats"
```

**Output (after executing for "Alice Smith"):**
```
+----+---------+-------------+-------------------+--------------+-------------+-----------------+
| id | user_id | user_name   | user_email        | total_orders | total_spent | last_order_date |
+----+---------+-------------+-------------------+--------------+-------------+-----------------+
|  1 |       1 | Alice Smith | alice@example.com |            3 |     1039.97 | 2024-01-19      |
+----+---------+-------------+-------------------+--------------+-------------+-----------------+
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

**What Happened:**
1. 📊 Read orders from CSV file
2. 👥 Joined with users from MySQL, filtered by name parameter
3. 📈 Aggregated: COUNT orders, SUM amounts, MAX date for the specified user
4. 💾 Wrote 1 aggregated row to MySQL (transactional)

## Troubleshooting

### Connection Refused
```
Error: Failed to create MySQL connection pool
```
**Solution**: Verify MySQL server is running and accessible:

```bash
# Check if MySQL is running (Docker)
docker ps | grep mysql-skardi

# Check if MySQL is running (Linux)
sudo systemctl status mysql

# Check if MySQL is running (macOS)
brew services list | grep mysql

# Test connection
mysql -h localhost -u $MYSQL_USER -p$MYSQL_PASSWORD -e "SELECT 1"

# If using Docker, ensure port 3306 is exposed
docker port mysql-skardi
```

### Table Not Found
```
Error: Failed to create table provider
```
**Solution**: Verify table exists and user has permissions:
```sql
SHOW TABLES;
DESCRIBE users;
```

### Authentication Failed
```
Error: Access denied for user
```
**Solution**: Check environment variables are set correctly:
```bash
echo $MYSQL_USER
echo $MYSQL_PASSWORD

# Verify user exists and has permissions
mysql -u root -p -e "SELECT User, Host FROM mysql.user WHERE User='$MYSQL_USER';"
mysql -u root -p -e "SHOW GRANTS FOR '$MYSQL_USER'@'localhost';"
```

### Docker Container Exits Immediately
```
docker ps -a shows mysql-skardi exited
```
**Solution**: Check logs for errors:
```bash
docker logs mysql-skardi

# Common fix: Remove existing container and data
docker rm mysql-skardi
docker volume prune

# Restart with clean state
docker run --name mysql-skardi \
  -e MYSQL_ROOT_PASSWORD=rootpass \
  -e MYSQL_DATABASE=mydb \
  -e MYSQL_USER=skardi_user \
  -e MYSQL_PASSWORD=skardi_pass \
  -p 3306:3306 \
  -d mysql:8.0
```

## Advanced Usage

### Multiple Databases

```yaml
data_sources:
  - name: "prod_users"
    type: "mysql"
    connection_string: "mysql://prod-server:3306/production"
    options:
      table: "users"
      user_env: "PROD_MYSQL_USER"
      pass_env: "PROD_MYSQL_PASSWORD"

  - name: "staging_users"
    type: "mysql"
    connection_string: "mysql://staging-server:3306/staging"
    options:
      table: "users"
      user_env: "STAGING_MYSQL_USER"
      pass_env: "STAGING_MYSQL_PASSWORD"
```
