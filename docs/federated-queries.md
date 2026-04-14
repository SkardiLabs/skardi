# Federated Queries

One of Skardi's most powerful features is the ability to JOIN data across different source types in a single SQL query. DataFusion handles the federation transparently.

## Example: CSV + PostgreSQL

Join a CSV file with a PostgreSQL table and write results back to PostgreSQL:

```yaml
metadata:
  name: "federated_join_and_insert"
  version: "1.0"

query: |
  INSERT INTO user_order_stats (user_id, user_name, total_orders, total_spent)
  SELECT
    u.id as user_id,
    u.name as user_name,
    COUNT(o.order_id) as total_orders,
    SUM(o.amount) as total_spent
  FROM users u                    -- PostgreSQL table
  INNER JOIN csv_orders o         -- CSV file
    ON u.id = o.user_id
  WHERE u.name = {name}
  GROUP BY u.id, u.name
```

## More Examples

Federated query examples are included in each data source's docs:

- [PostgreSQL](postgres/) — `pipelines/federated_join_and_insert.yaml`
- [MySQL](mysql/) — `pipelines/federated_join_and_insert.yaml`
- [SQLite](sqlite/) — `pipelines/federated_join_and_insert.yaml`
- [Redis](redis/) — `pipelines/federated_join_and_insert.yaml`
