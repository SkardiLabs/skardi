# Catalog Mode

Instead of registering tables one by one, set `hierarchy_level: "catalog"` on a data source to load the entire database automatically. Every table and view is registered under a named DataFusion catalog and queried with a three-part reference.

Supported for **PostgreSQL**, **MySQL**, and **SQLite**.

## Configuration

```yaml
data_sources:
  - name: "mydb_catalog"
    type: "postgres"          # or mysql / sqlite
    hierarchy_level: "catalog"
    connection_string: "postgres://localhost:5432/mydb"
    options:
      user_env: "PG_USER"
      pass_env: "PG_PASSWORD"
      # Optionally restrict to specific schemas:
      # allowed_schemas: "public,analytics"
```

## Querying

Tables are addressable as `catalog.schema.table`:

```sql
-- PostgreSQL / MySQL
SELECT * FROM mydb_catalog.public.users LIMIT 10;

-- SQLite (schema is always "main")
SELECT * FROM mydb_catalog.main.users LIMIT 10;
```

## Working Examples

See the docs directories for full working examples:
- [docs/postgres/](postgres/) — `ctx_postgres_catalog_demo.yaml`
- [docs/mysql/](mysql/) — `ctx_mysql_catalog_demo.yaml`
- [docs/sqlite/](sqlite/) — `ctx_sqlite_catalog_demo.yaml`
