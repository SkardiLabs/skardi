use crate::sources::DataSourceType;
use crate::sources::hierarchy::{
    HierarchyLevel, SourceLabel, build_catalog, parse_allowed_schemas, retry_with_timeout,
};
use crate::sources::providers::mysql_wire::parse_mysql_wire_connection_params;
use anyhow::{Context, Result};
use arrow::array::{RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Constraints;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, dml::InsertOp};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::MySqlDialect;
use datafusion_table_providers::mysql::MySQLTableFactory;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::mysqlconn::MySQLConnection;
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use futures::stream;
use mysql_async::prelude::Queryable;
use mysql_async::{Params, Row, Value};
use secrecy::SecretString;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// Register MySQL tables or a whole database (catalog) into a DataFusion [`SessionContext`].
///
/// Single-table mode (default) registers one table under `name`.  Catalog mode registers one provider per table across all non-system schemas.
///
/// # Arguments
/// * `session_ctx` - DataFusion session context to register tables into
/// * `name` - Name to register the table (table mode) or catalog (catalog mode) as
/// * `connection_string` - MySQL connection string (e.g., "mysql://host:port/db")
///   Note: Username and password should NOT be included in the connection string.
///   Use `user_env` and `pass_env` options instead.
/// * `options` - Optional configuration (see below)
/// * `read_write` - If true, register as read-write (allows INSERT/UPDATE/DELETE)
/// * `hierarchy_level` - [`HierarchyLevel::Table`] (default) or [`HierarchyLevel::Catalog`]
///
/// # Options
/// * `table` - Table name (required in table mode)
/// * `schema` - Database/schema name (optional in table mode)
/// * `allowed_schemas` - Comma-separated schema allow-list (catalog mode only)
/// * `user_env` - Environment variable name for username
/// * `pass_env` - Environment variable name for password
/// * `ssl_mode` - "disabled" | "preferred" | "required" (default: "disabled")
pub async fn register_mysql_tables(
    session_ctx: &mut SessionContext,
    name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
    read_write: bool,
    hierarchy_level: HierarchyLevel,
) -> Result<()> {
    let mode_str = if read_write {
        "read-write"
    } else {
        "read-only"
    };
    match hierarchy_level {
        HierarchyLevel::Catalog => {
            register_mysql_catalog(
                session_ctx,
                name,
                connection_string,
                options,
                read_write,
                mode_str,
            )
            .await
        }
        HierarchyLevel::Table => {
            register_single_mysql_table(
                session_ctx,
                name,
                connection_string,
                options,
                read_write,
                mode_str,
            )
            .await
        }
    }
}

/// Register one MySQL table under `name` in the default catalog.
async fn register_single_mysql_table(
    session_ctx: &mut SessionContext,
    name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
    read_write: bool,
    mode_str: &str,
) -> Result<()> {
    tracing::info!(
        "Registering MySQL table: {} with connection: {} ({})",
        name,
        connection_string,
        mode_str
    );
    tracing::debug!("Options: {:?}", options);

    let table_name = options
        .and_then(|opts| opts.get("table"))
        .ok_or_else(|| anyhow::anyhow!("MySQL data source '{}' requires 'table' option", name))?;

    let schema_name = options.and_then(|opts| opts.get("schema"));

    tracing::debug!(
        "Connecting to MySQL table: {} as '{}'",
        if let Some(schema) = schema_name {
            format!("{}.{}", schema, table_name)
        } else {
            table_name.to_string()
        },
        name
    );

    let params = parse_connection_params(connection_string, options)?;

    let label = SourceLabel::new(DataSourceType::Mysql, HierarchyLevel::Table, name);
    let pool = Arc::new(
        retry_with_timeout(label, "pool creation", || async {
            MySQLConnectionPool::new(params.clone())
                .await
                .map_err(|e| anyhow::anyhow!(e))
        })
        .await
        .with_context(|| format!("Failed to create MySQL connection pool for '{}'", name))?,
    );

    let table_reference = if let Some(schema) = schema_name {
        TableReference::partial(schema.as_str(), table_name.as_str())
    } else {
        TableReference::bare(table_name.as_str())
    };

    tracing::debug!(
        "Creating MySQL table provider ({}) for: {:?}",
        mode_str,
        table_reference
    );

    let table_provider =
        build_mysql_table_provider(Arc::clone(&pool), table_reference.clone(), read_write).await?;

    session_ctx
        .register_table(name, table_provider)
        .map_err(|e| {
            tracing::error!("Failed to register table with DataFusion: {:?}", e);
            e
        })
        .with_context(|| format!("Failed to register MySQL table '{}' with DataFusion", name))?;

    tracing::info!(
        "Successfully registered MySQL table '{}' as '{}' ({})",
        table_reference,
        name,
        mode_str
    );

    Ok(())
}

/// Register an entire MySQL database as a named DataFusion catalog.
async fn register_mysql_catalog(
    session_ctx: &mut SessionContext,
    catalog_name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
    read_write: bool,
    mode_str: &str,
) -> Result<()> {
    tracing::info!("Registering MySQL catalog: {} ({})", catalog_name, mode_str,);

    let params = parse_connection_params(connection_string, options)?;
    let label = SourceLabel::new(DataSourceType::Mysql, HierarchyLevel::Catalog, catalog_name);

    let pool = Arc::new(
        retry_with_timeout(label, "pool creation", || async {
            MySQLConnectionPool::new(params.clone())
                .await
                .map_err(|e| anyhow::anyhow!(e))
        })
        .await
        .with_context(|| {
            format!(
                "Failed to create MySQL connection pool for catalog '{}'",
                catalog_name
            )
        })?,
    );

    let allowed_schemas = parse_allowed_schemas(options);
    let schema_tables = retry_with_timeout(label, "information_schema introspection", || async {
        list_mysql_tables_in_catalog(&pool, allowed_schemas.as_deref()).await
    })
    .await
    .with_context(|| {
        format!(
            "Failed to list MySQL tables for catalog-wide registration in source '{}'",
            catalog_name
        )
    })?;

    if schema_tables.is_empty() {
        tracing::warn!(
            "No tables found in MySQL catalog for source '{}'",
            catalog_name
        );
    }

    let table_count = schema_tables.len();

    build_catalog(
        session_ctx,
        catalog_name,
        schema_tables,
        |_schema, table_name| {
            let pool = Arc::clone(&pool);
            async move {
                // Use a bare reference: the pool is already connected to the right database,
                // and MySQLTableFactory's table_exists check only matches unqualified names.
                let table_reference = TableReference::bare(table_name.as_str());
                build_mysql_table_provider(pool, table_reference, read_write).await
            }
        },
    )
    .await
    .with_context(|| format!("Failed to build MySQL catalog '{}'", catalog_name))?;

    tracing::info!(
        "Registered MySQL catalog '{}' with {} table(s) ({})",
        catalog_name,
        table_count,
        mode_str
    );

    Ok(())
}

/// List all user tables and views across a MySQL instance via `information_schema`.
///
/// Reuses the already-established `pool` connection (avoiding a second handshake).
/// Excludes built-in system schemas (`mysql`, `information_schema`, `performance_schema`, `sys`).
/// When `allowed_schemas` is `Some`, the filter is pushed into the SQL `WHERE` clause as a
/// parameterized `IN (...)` list rather than filtered client-side.
// TODO: downcasts to `MySQLConnection`, an internal type of datafusion-table-providers. A
// minor version bump could reshape that type and break this at runtime. Upstream a typed
// accessor (or expose `information_schema` listing) so we can drop the downcast.
async fn list_mysql_tables_in_catalog(
    pool: &MySQLConnectionPool,
    allowed_schemas: Option<&[String]>,
) -> Result<Vec<(String, String)>> {
    let db_conn = pool.connect().await.map_err(|e| {
        anyhow::anyhow!("Failed to connect to MySQL for catalog introspection: {e}")
    })?;

    let mysql_conn = db_conn
        .as_any()
        .downcast_ref::<MySQLConnection>()
        .ok_or_else(|| {
            anyhow::anyhow!("Unexpected MySQL connection type during catalog introspection")
        })?;

    let mut conn = mysql_conn.conn.lock().await;

    const BASE_QUERY: &str = "SELECT table_schema, table_name
         FROM information_schema.tables
         WHERE table_type IN ('BASE TABLE', 'VIEW')
           AND table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')";

    let rows: Vec<(String, String)> = match allowed_schemas {
        Some(allowed) if !allowed.is_empty() => {
            let placeholders = vec!["?"; allowed.len()].join(",");
            let query = format!(
                "{BASE_QUERY} AND table_schema IN ({placeholders}) \
                 ORDER BY table_schema, table_name"
            );
            let params: Vec<Value> = allowed.iter().map(|s| Value::from(s.clone())).collect();
            conn.exec_map(
                query,
                Params::Positional(params),
                |(schema, table): (String, String)| (schema, table),
            )
            .await
            .with_context(|| "Failed to query information_schema for catalog listing")?
        }
        _ => {
            let query = format!("{BASE_QUERY} ORDER BY table_schema, table_name");
            conn.query_map(query, |(schema, table): (String, String)| (schema, table))
                .await
                .with_context(|| "Failed to query information_schema for catalog listing")?
        }
    };

    Ok(rows)
}

/// Build a [`TableProvider`] for a single MySQL table, wrapping read-write providers in
/// [`MySQLDmlProvider`] to expose `DELETE` and `UPDATE` support.
async fn build_mysql_table_provider(
    pool: Arc<MySQLConnectionPool>,
    table_reference: TableReference,
    read_write: bool,
) -> Result<Arc<dyn TableProvider>> {
    let factory = MySQLTableFactory::new(Arc::clone(&pool));

    let inner = if read_write {
        factory
            .read_write_table_provider(table_reference.clone())
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to create read-write table provider for '{:?}': {}",
                    table_reference,
                    e
                )
            })?
    } else {
        factory
            .table_provider(table_reference.clone())
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to create read-only table provider for '{:?}': {}",
                    table_reference,
                    e
                )
            })?
    };

    let result: Arc<dyn TableProvider> = if read_write {
        Arc::new(MySQLDmlProvider {
            inner,
            pool,
            table_reference,
        })
    } else {
        inner
    };

    Ok(result)
}

// ─── DML support wrapper ────────────────────────────────────────────────────

/// Wraps a read-write [`TableProvider`] to add `DELETE` and `UPDATE` support
/// that DataFusion 52 exposes via [`TableProvider::delete_from`] and
/// [`TableProvider::update`].
#[derive(Debug)]
struct MySQLDmlProvider {
    inner: Arc<dyn TableProvider>,
    pool: Arc<MySQLConnectionPool>,
    table_reference: TableReference,
}

#[async_trait]
impl TableProvider for MySQLDmlProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, op).await
    }

    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let table = quote_mysql_table(&self.table_reference);
        let where_clause = build_mysql_where_clause(&filters)?;
        let sql = format!("DELETE FROM {table}{where_clause}");
        Ok(Arc::new(MySQLDmlExec::new(Arc::clone(&self.pool), sql)))
    }

    async fn update(
        &self,
        _state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if assignments.is_empty() {
            return Err(DataFusionError::Plan(
                "UPDATE requires at least one assignment".to_string(),
            ));
        }

        let unparser = Unparser::new(&MySqlDialect {});
        let set_clause = assignments
            .iter()
            .map(|(col, expr)| {
                let val = unparser
                    .expr_to_sql(expr)
                    .map_err(|e| {
                        DataFusionError::Plan(format!(
                            "Failed to unparse assignment expression for column '{col}': {e}"
                        ))
                    })?
                    .to_string();
                Ok(format!("{} = {val}", quote_mysql_ident(col)))
            })
            .collect::<DataFusionResult<Vec<_>>>()?
            .join(", ");

        let table = quote_mysql_table(&self.table_reference);
        let where_clause = build_mysql_where_clause(&filters)?;
        let sql = format!("UPDATE {table} SET {set_clause}{where_clause}");
        Ok(Arc::new(MySQLDmlExec::new(Arc::clone(&self.pool), sql)))
    }
}

/// Builds a ` WHERE expr1 AND expr2 ...` clause from a list of DataFusion
/// filter expressions. Returns an empty string when `filters` is empty.
fn build_mysql_where_clause(filters: &[Expr]) -> DataFusionResult<String> {
    if filters.is_empty() {
        return Ok(String::new());
    }
    let unparser = Unparser::new(&MySqlDialect {});
    let parts = filters
        .iter()
        .map(|e| {
            unparser.expr_to_sql(e).map(|s| s.to_string()).map_err(|e| {
                DataFusionError::Plan(format!("Failed to unparse filter expression: {e}"))
            })
        })
        .collect::<DataFusionResult<Vec<_>>>()?;
    Ok(format!(" WHERE {}", parts.join(" AND ")))
}

/// Quotes a MySQL identifier with backticks, escaping any embedded backticks.
fn quote_mysql_ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// Produces a properly backtick-quoted `[[catalog.]schema.]table` string.
fn quote_mysql_table(tbl: &TableReference) -> String {
    [tbl.catalog(), tbl.schema(), Some(tbl.table())]
        .into_iter()
        .flatten()
        .map(quote_mysql_ident)
        .collect::<Vec<_>>()
        .join(".")
}

// ─── Execution plan for DELETE / UPDATE results ─────────────────────────────

/// A leaf [`ExecutionPlan`] that executes a pre-built MySQL DML statement
/// and returns a single row `{ count: u64 }` with the number of affected rows.
struct MySQLDmlExec {
    pool: Arc<MySQLConnectionPool>,
    sql: String,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl MySQLDmlExec {
    fn new(pool: Arc<MySQLConnectionPool>, sql: String) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            pool,
            sql,
            schema,
            properties,
        }
    }
}

impl fmt::Debug for MySQLDmlExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MySQLDmlExec(sql={})", self.sql)
    }
}

impl DisplayAs for MySQLDmlExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MySQLDmlExec")
    }
}

impl ExecutionPlan for MySQLDmlExec {
    fn name(&self) -> &str {
        "MySQLDmlExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let pool = Arc::clone(&self.pool);
        let sql = self.sql.clone();
        let schema = Arc::clone(&self.schema);

        let future =
            async move {
                let conn_obj = pool.connect_direct().await.map_err(|e| {
                    DataFusionError::Execution(format!("MySQL DML connect error: {e}"))
                })?;
                let mut conn = conn_obj.conn.lock().await;
                let conn = &mut *conn;
                let _: Vec<Row> = conn.exec(&sql, Params::Empty).await.map_err(|e| {
                    DataFusionError::Execution(format!("MySQL DML execute error: {e}"))
                })?;
                let rows_affected = conn.affected_rows();
                let count_array = Arc::new(UInt64Array::from(vec![rows_affected]));
                RecordBatch::try_new(Arc::clone(&schema), vec![count_array])
                    .map_err(DataFusionError::from)
            };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream::once(future),
        )))
    }
}

// ─── Connection-string parser ────────────────────────────────────────────────

/// Parse MySQL connection string into parameters HashMap for MySQLConnectionPool
///
/// MySQLConnectionPool expects a HashMap with keys like:
/// - host, tcp_port, db_name, user, pass
/// - And potentially ssl-related options
fn parse_connection_params(
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
) -> Result<HashMap<String, SecretString>> {
    parse_mysql_wire_connection_params(connection_string, options, 3306, "MySQL")
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    #[test]
    fn test_parse_connection_params_full() {
        let conn_str = "mysql://localhost:3306/mydb";
        let params = parse_connection_params(conn_str, None).unwrap();

        assert_eq!(params.get("host").unwrap().expose_secret(), "localhost");
        assert_eq!(params.get("tcp_port").unwrap().expose_secret(), "3306");
        assert_eq!(params.get("db").unwrap().expose_secret(), "mydb");
        assert_eq!(params.get("sslmode").unwrap().expose_secret(), "disabled");
    }

    #[test]
    fn test_parse_connection_params_default_port() {
        let conn_str = "mysql://localhost/mydb";
        let params = parse_connection_params(conn_str, None).unwrap();

        assert_eq!(params.get("tcp_port").unwrap().expose_secret(), "3306");
    }

    #[test]
    fn test_parse_connection_params_custom_port() {
        let conn_str = "mysql://localhost:3307/mydb";
        let params = parse_connection_params(conn_str, None).unwrap();

        assert_eq!(params.get("tcp_port").unwrap().expose_secret(), "3307");
    }

    #[test]
    fn test_parse_connection_params_no_database() {
        let conn_str = "mysql://localhost:3306";
        let params = parse_connection_params(conn_str, None).unwrap();

        assert!(!params.contains_key("db"));
    }

    #[test]
    fn test_parse_connection_params_invalid_url() {
        let conn_str = "not-a-valid-url";
        let result = parse_connection_params(conn_str, None);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid MySQL connection string")
        );
    }

    #[test]
    fn test_parse_connection_params_with_env_credentials() {
        unsafe {
            std::env::set_var("TEST_MYSQL_USER", "testuser");
            std::env::set_var("TEST_MYSQL_PASS", "testpass");
        }

        let conn_str = "mysql://localhost:3306/mydb";
        let mut options: HashMap<String, String> = HashMap::new();
        options.insert("user_env".to_string(), "TEST_MYSQL_USER".to_string());
        options.insert("pass_env".to_string(), "TEST_MYSQL_PASS".to_string());

        let params = parse_connection_params(conn_str, Some(&options)).unwrap();

        assert_eq!(params.get("user").unwrap().expose_secret(), "testuser");
        assert_eq!(params.get("pass").unwrap().expose_secret(), "testpass");

        unsafe {
            std::env::remove_var("TEST_MYSQL_USER");
            std::env::remove_var("TEST_MYSQL_PASS");
        }
    }

    #[test]
    fn test_parse_connection_params_missing_user_env() {
        let conn_str = "mysql://localhost:3306/mydb";
        let mut options: HashMap<String, String> = HashMap::new();
        options.insert("user_env".to_string(), "NONEXISTENT_MYSQL_USER".to_string());

        let result = parse_connection_params(conn_str, Some(&options));

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Environment variable 'NONEXISTENT_MYSQL_USER' not found")
        );
    }

    #[test]
    fn test_parse_connection_params_missing_pass_env() {
        unsafe {
            std::env::set_var("TEST_MYSQL_USER_ONLY", "testuser");
        }

        let conn_str = "mysql://localhost:3306/mydb";
        let mut options: HashMap<String, String> = HashMap::new();
        options.insert("user_env".to_string(), "TEST_MYSQL_USER_ONLY".to_string());
        options.insert("pass_env".to_string(), "NONEXISTENT_MYSQL_PASS".to_string());

        let result = parse_connection_params(conn_str, Some(&options));

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Environment variable 'NONEXISTENT_MYSQL_PASS' not found")
        );

        unsafe {
            std::env::remove_var("TEST_MYSQL_USER_ONLY");
        }
    }

    #[test]
    fn test_parse_connection_params_ssl_mode_disabled() {
        let conn_str = "mysql://localhost:3306/mydb";
        let mut options: HashMap<String, String> = HashMap::new();
        options.insert("ssl_mode".to_string(), "disabled".to_string());

        let params = parse_connection_params(conn_str, Some(&options)).unwrap();

        assert_eq!(params.get("sslmode").unwrap().expose_secret(), "disabled");
    }

    #[test]
    fn test_parse_connection_params_ssl_mode_required() {
        let conn_str = "mysql://localhost:3306/mydb";
        let mut options: HashMap<String, String> = HashMap::new();
        options.insert("ssl_mode".to_string(), "required".to_string());

        let params = parse_connection_params(conn_str, Some(&options)).unwrap();

        assert_eq!(params.get("sslmode").unwrap().expose_secret(), "required");
    }

    #[test]
    fn test_parse_connection_params_ssl_mode_case_insensitive() {
        let conn_str = "mysql://localhost:3306/mydb";
        let mut options: HashMap<String, String> = HashMap::new();
        options.insert("ssl_mode".to_string(), "PREFERRED".to_string());

        let params = parse_connection_params(conn_str, Some(&options)).unwrap();

        assert_eq!(params.get("sslmode").unwrap().expose_secret(), "preferred");
    }

    #[test]
    fn test_missing_table_option() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut session_ctx = SessionContext::new();
            let result = register_mysql_tables(
                &mut session_ctx,
                "test_table",
                "mysql://localhost:3306/db",
                None,
                false,
                HierarchyLevel::default(),
            )
            .await;

            assert!(result.is_err());
            let error_msg = result.unwrap_err().to_string();
            assert!(error_msg.contains("requires 'table' option"));
        });
    }

    // ─── HierarchyLevel dispatch tests ──────────────────────────────────

    #[test]
    fn test_hierarchy_level_default_is_table() {
        assert_eq!(HierarchyLevel::default(), HierarchyLevel::Table);
    }

    #[test]
    fn test_hierarchy_level_default_routes_to_table_mode() {
        // Default → table mode → fails with "requires 'table' option", not a catalog error.
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut ctx = SessionContext::new();
            let err = register_mysql_tables(
                &mut ctx,
                "t",
                "mysql://localhost/db",
                None,
                false,
                HierarchyLevel::default(),
            )
            .await
            .unwrap_err();
            assert!(err.to_string().contains("requires 'table' option"));
        });
    }

    #[test]
    fn test_hierarchy_level_table_explicit_routes_to_table_mode() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut ctx = SessionContext::new();
            let err = register_mysql_tables(
                &mut ctx,
                "t",
                "mysql://localhost/db",
                None,
                false,
                HierarchyLevel::Table,
            )
            .await
            .unwrap_err();
            assert!(err.to_string().contains("requires 'table' option"));
        });
    }

    #[test]
    fn test_table_option_extraction() {
        let mut options: HashMap<String, String> = HashMap::new();
        options.insert("table".to_string(), "users".to_string());

        let table_name = options.get("table").unwrap();
        assert_eq!(table_name, "users");
    }

    #[test]
    fn test_schema_option_present() {
        let mut options: HashMap<String, String> = HashMap::new();
        options.insert("schema".to_string(), "mydb".to_string());

        let schema_name = options.get("schema");
        assert_eq!(schema_name, Some(&"mydb".to_string()));
    }

    #[test]
    fn test_schema_option_absent() {
        let options: HashMap<String, String> = HashMap::new();

        let schema_name = options.get("schema");
        assert!(schema_name.is_none());
    }

    #[test]
    fn test_table_reference_bare() {
        let table = "users";
        let reference = TableReference::bare(table);

        assert_eq!(reference.to_string(), "users");
    }

    #[test]
    fn test_table_reference_with_schema() {
        let schema = "mydb";
        let table = "users";
        let reference = TableReference::partial(schema, table);

        assert_eq!(reference.to_string(), "mydb.users");
    }

    #[test]
    fn test_parse_connection_params_with_ip_host() {
        let conn_str = "mysql://192.168.1.100:3306/mydb";
        let params = parse_connection_params(conn_str, None).unwrap();

        assert_eq!(params.get("host").unwrap().expose_secret(), "192.168.1.100");
    }

    #[test]
    fn test_parse_connection_params_with_subdirectory_path() {
        let conn_str = "mysql://localhost:3306/mydb/extra";
        let params = parse_connection_params(conn_str, None).unwrap();

        assert_eq!(params.get("db").unwrap().expose_secret(), "mydb");
    }

    // ─── Integration test helpers ────────────────────────────────────────

    /// Register a MySQL table from the CI docker service.
    /// Expects MYSQL_USER and MYSQL_PASSWORD env vars to be set.
    async fn register_ci_table(ctx: &mut SessionContext, table: &str) {
        let mut options = HashMap::new();
        options.insert("table".to_string(), table.to_string());
        options.insert("user_env".to_string(), "MYSQL_USER".to_string());
        options.insert("pass_env".to_string(), "MYSQL_PASSWORD".to_string());
        options.insert("ssl_mode".to_string(), "disabled".to_string());
        register_mysql_tables(
            ctx,
            table,
            "mysql://127.0.0.1:3306/mydb",
            Some(&options),
            true,
            HierarchyLevel::Table,
        )
        .await
        .unwrap_or_else(|e| panic!("register {} failed: {}", table, e));
    }

    /// Register the entire CI MySQL database as a catalog under `catalog_name`.
    /// Expects MYSQL_USER and MYSQL_PASSWORD env vars to be set.
    async fn register_ci_catalog(ctx: &mut SessionContext, catalog_name: &str) {
        let mut options = HashMap::new();
        options.insert("user_env".to_string(), "MYSQL_USER".to_string());
        options.insert("pass_env".to_string(), "MYSQL_PASSWORD".to_string());
        options.insert("ssl_mode".to_string(), "disabled".to_string());
        register_mysql_tables(
            ctx,
            catalog_name,
            "mysql://127.0.0.1:3306/mydb",
            Some(&options),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap_or_else(|e| panic!("register catalog {} failed: {}", catalog_name, e));
    }

    async fn query_all(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        let df = ctx.sql(sql).await.expect("parse sql");
        df.collect().await.expect("collect results")
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    // ─── Scan tests (integration) ───────────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_scan_all_rows() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "users").await;

        let batches = query_all(&ctx, "SELECT id, name, email FROM users ORDER BY id").await;
        assert!(total_rows(&batches) >= 3);
    }

    #[tokio::test]
    #[ignore]
    async fn test_scan_with_projection() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "users").await;

        let batches = query_all(&ctx, "SELECT name FROM users ORDER BY id").await;
        assert!(total_rows(&batches) >= 3);
        assert_eq!(batches[0].num_columns(), 1);

        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Alice Smith");
    }

    #[tokio::test]
    #[ignore]
    async fn test_scan_with_filter() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "users").await;

        let batches = query_all(&ctx, "SELECT id, name FROM users WHERE id = 2").await;
        assert_eq!(total_rows(&batches), 1);

        let names = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Bob Johnson");
    }

    #[tokio::test]
    #[ignore]
    async fn test_scan_with_limit() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "users").await;

        let batches = query_all(&ctx, "SELECT id FROM users LIMIT 2").await;
        assert_eq!(total_rows(&batches), 2);
    }

    // ─── Insert test (integration) ──────────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_insert_into() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "users").await;

        ctx.sql("INSERT INTO users (name, email) VALUES ('Dave Brown', 'dave@example.com')")
            .await
            .expect("parse insert")
            .collect()
            .await
            .expect("execute insert");

        let batches = query_all(&ctx, "SELECT id, name, email FROM users ORDER BY id").await;
        assert!(total_rows(&batches) >= 4);
    }

    /// Multi-row `INSERT INTO ... VALUES (...), (...), (...)` — the shape the
    /// server-side renderer emits when a pipeline parameter is the
    /// array-of-arrays form `{"rows": [[..], [..]]}`. The MySQL provider
    /// delegates to `datafusion-table-providers`, which re-renders the batch
    /// as a single multi-row VALUES so the insert reaches MySQL as one
    /// statement.
    #[tokio::test]
    #[ignore]
    async fn test_insert_multi_row_values() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "users").await;

        // Pre-clean so a re-run starts from a known state (the seed table has
        // a UNIQUE constraint on `email`).
        ctx.sql("DELETE FROM users WHERE name LIKE 'MyBatch%'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        ctx.sql(
            "INSERT INTO users (name, email) VALUES \
             ('MyBatch1', 'mybatch1@example.com'), \
             ('MyBatch2', 'mybatch2@example.com'), \
             ('MyBatch3', 'mybatch3@example.com')",
        )
        .await
        .expect("parse multi-row insert")
        .collect()
        .await
        .expect("execute multi-row insert");

        let batches = query_all(
            &ctx,
            "SELECT name FROM users WHERE name LIKE 'MyBatch%' ORDER BY name",
        )
        .await;
        assert_eq!(total_rows(&batches), 3);
    }

    // ─── Delete tests (integration) ─────────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_delete_with_filter() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "users").await;

        // Insert a row we can safely delete
        ctx.sql("INSERT INTO users (name, email) VALUES ('DeleteMe', 'deleteme@example.com')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let before = query_all(&ctx, "SELECT id FROM users WHERE name = 'DeleteMe'").await;
        assert_eq!(total_rows(&before), 1);

        ctx.sql("DELETE FROM users WHERE name = 'DeleteMe'")
            .await
            .expect("parse delete")
            .collect()
            .await
            .expect("execute delete");

        let after = query_all(&ctx, "SELECT id FROM users WHERE name = 'DeleteMe'").await;
        assert_eq!(total_rows(&after), 0);
    }

    #[tokio::test]
    #[ignore]
    async fn test_delete_no_matching_rows() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "users").await;

        let before = query_all(&ctx, "SELECT id FROM users WHERE id = 1").await;
        assert_eq!(total_rows(&before), 1);

        ctx.sql("DELETE FROM users WHERE id = 99999")
            .await
            .expect("parse delete")
            .collect()
            .await
            .expect("execute delete");

        let after = query_all(&ctx, "SELECT id FROM users WHERE id = 1").await;
        assert_eq!(total_rows(&after), 1);
    }

    // ─── Update tests (integration) ─────────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_update_single_column_with_filter() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "users").await;

        ctx.sql("UPDATE users SET email = 'alice_updated@example.com' WHERE name = 'Alice Smith'")
            .await
            .expect("parse update")
            .collect()
            .await
            .expect("execute update");

        let batches = query_all(&ctx, "SELECT email FROM users WHERE name = 'Alice Smith'").await;
        assert_eq!(total_rows(&batches), 1);

        let emails = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(emails.value(0), "alice_updated@example.com");
    }

    #[tokio::test]
    #[ignore]
    async fn test_update_no_matching_rows() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "users").await;

        let before = query_all(&ctx, "SELECT email FROM users WHERE id = 3").await;
        assert_eq!(total_rows(&before), 1);
        let before_email = before[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(0)
            .to_string();

        ctx.sql("UPDATE users SET email = 'nobody@example.com' WHERE id = 99999")
            .await
            .expect("parse update")
            .collect()
            .await
            .expect("execute update");

        let after = query_all(&ctx, "SELECT email FROM users WHERE id = 3").await;
        assert_eq!(total_rows(&after), 1);
        let after_email = after[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(after_email, before_email);
    }

    #[tokio::test]
    #[ignore]
    async fn test_update_multiple_columns() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "users").await;

        ctx.sql(
            "INSERT INTO users (name, email) VALUES ('MySqlMultiUpdate', 'mysql_multi_update@example.com')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

        ctx.sql(
            "UPDATE users
             SET name = 'MySqlMultiUpdateRenamed',
                 email = 'mysql_multi_update_renamed@example.com'
             WHERE name = 'MySqlMultiUpdate'",
        )
        .await
        .expect("parse update")
        .collect()
        .await
        .expect("execute update");

        let batches = query_all(
            &ctx,
            "SELECT name, email
             FROM users
             WHERE email = 'mysql_multi_update_renamed@example.com'",
        )
        .await;
        assert_eq!(total_rows(&batches), 1);

        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let emails = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "MySqlMultiUpdateRenamed");
        assert_eq!(emails.value(0), "mysql_multi_update_renamed@example.com");

        ctx.sql("DELETE FROM users WHERE email = 'mysql_multi_update_renamed@example.com'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
    }

    // ─── Combined DML test (integration) ────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_insert_update_delete_round_trip() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "users").await;

        // 1. Insert
        ctx.sql("INSERT INTO users (name, email) VALUES ('RoundTrip', 'roundtrip@example.com')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let after_insert = query_all(&ctx, "SELECT id FROM users WHERE name = 'RoundTrip'").await;
        assert_eq!(total_rows(&after_insert), 1);

        // 2. Update
        ctx.sql(
            "UPDATE users SET email = 'roundtrip_updated@example.com' WHERE name = 'RoundTrip'",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        let batches = query_all(&ctx, "SELECT email FROM users WHERE name = 'RoundTrip'").await;
        let emails = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(emails.value(0), "roundtrip_updated@example.com");

        // 3. Delete
        ctx.sql("DELETE FROM users WHERE name = 'RoundTrip'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let after_delete = query_all(&ctx, "SELECT id FROM users WHERE name = 'RoundTrip'").await;
        assert_eq!(total_rows(&after_delete), 0);
    }

    // ─── Multi-table tests (integration) ────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_scan_orders_table() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "orders").await;

        let batches = query_all(
            &ctx,
            "SELECT id, user_id, product, amount FROM orders ORDER BY id",
        )
        .await;
        assert!(total_rows(&batches) >= 3);
    }

    #[tokio::test]
    #[ignore]
    async fn test_cross_table_join() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "users").await;
        register_ci_table(&mut ctx, "orders").await;

        let batches = query_all(
            &ctx,
            "SELECT u.name, o.product, o.amount
             FROM users u
             INNER JOIN orders o ON u.id = o.user_id
             ORDER BY o.id",
        )
        .await;
        assert!(total_rows(&batches) >= 3);
    }

    #[tokio::test]
    #[ignore]
    async fn test_insert_select_aggregation() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "users").await;
        register_ci_table(&mut ctx, "orders").await;
        register_ci_table(&mut ctx, "user_order_stats").await;

        ctx.sql("DELETE FROM user_order_stats WHERE user_id = 1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        ctx.sql(
            "INSERT INTO user_order_stats (user_id, user_name, user_email, total_orders, total_spent, last_order_date)
             SELECT
               CAST(u.id AS INT),
               u.name,
               u.email,
               CAST(COUNT(o.id) AS INT),
               CAST(SUM(o.amount) AS DECIMAL(10,2)),
               CAST('N/A' AS VARCHAR(50))
             FROM users u
             INNER JOIN orders o ON u.id = o.user_id
             WHERE u.name = 'Alice Smith'
             GROUP BY u.id, u.name, u.email",
        )
        .await
        .expect("parse insert-select")
        .collect()
        .await
        .expect("execute insert-select");

        let batches = query_all(
            &ctx,
            "SELECT user_id, user_name, total_orders FROM user_order_stats WHERE user_id = 1",
        )
        .await;
        assert_eq!(total_rows(&batches), 1);
    }

    #[tokio::test]
    #[ignore]
    async fn test_insert_select_multiple_users() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "users").await;
        register_ci_table(&mut ctx, "orders").await;
        register_ci_table(&mut ctx, "user_order_stats").await;

        ctx.sql(
            "DELETE FROM user_order_stats
             WHERE user_id IN (1, 2, 3)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

        ctx.sql(
            "INSERT INTO user_order_stats (user_id, user_name, user_email, total_orders, total_spent, last_order_date)
             SELECT
               CAST(u.id AS INT),
               u.name,
               u.email,
               CAST(COUNT(o.id) AS INT),
               CAST(SUM(o.amount) AS DECIMAL(10,2)),
               CAST('N/A' AS VARCHAR(50))
             FROM users u
             INNER JOIN orders o ON u.id = o.user_id
             GROUP BY u.id, u.name, u.email",
        )
        .await
        .expect("parse insert-select all")
        .collect()
        .await
        .expect("execute insert-select all");

        let batches = query_all(
            &ctx,
            "SELECT user_name, total_orders
             FROM user_order_stats
             WHERE user_name IN ('Alice Smith', 'Bob Johnson', 'Carol Williams')
             ORDER BY user_name",
        )
        .await;
        assert!(total_rows(&batches) >= 3);
    }

    // ─── Catalog mode tests (integration) ───────────────────────────────

    /// Catalog is registered and contains the expected schema + tables.
    #[tokio::test]
    #[ignore]
    async fn test_catalog_registers_expected_tables() {
        let mut ctx = SessionContext::new();
        register_ci_catalog(&mut ctx, "mydb_catalog").await;

        let catalog = ctx.catalog("mydb_catalog").expect("catalog not found");
        let schema = catalog.schema("mydb").expect("schema 'mydb' not found");
        assert!(
            schema.table("users").await.unwrap().is_some(),
            "table 'users' missing from catalog"
        );
        assert!(
            schema.table("orders").await.unwrap().is_some(),
            "table 'orders' missing from catalog"
        );
    }

    /// Tables can be queried using the three-part `catalog.schema.table` reference.
    #[tokio::test]
    #[ignore]
    async fn test_catalog_scan_via_qualified_name() {
        let mut ctx = SessionContext::new();
        register_ci_catalog(&mut ctx, "mydb_catalog").await;

        let batches = query_all(
            &ctx,
            "SELECT id, name FROM mydb_catalog.mydb.users ORDER BY id",
        )
        .await;
        assert!(total_rows(&batches) >= 3);
    }

    /// Two tables from the same catalog can be joined with qualified references.
    #[tokio::test]
    #[ignore]
    async fn test_catalog_cross_table_join() {
        let mut ctx = SessionContext::new();
        register_ci_catalog(&mut ctx, "mydb_catalog").await;

        let batches = query_all(
            &ctx,
            "SELECT u.name, o.product
             FROM mydb_catalog.mydb.users u
             INNER JOIN mydb_catalog.mydb.orders o ON u.id = o.user_id
             ORDER BY o.id",
        )
        .await;
        assert!(total_rows(&batches) >= 3);
    }

    /// `allowed_schemas` restricts catalog registration to the listed schemas.
    #[tokio::test]
    #[ignore]
    async fn test_catalog_allowed_schemas_includes_only_listed() {
        let mut ctx = SessionContext::new();
        let mut options = HashMap::new();
        options.insert("user_env".to_string(), "MYSQL_USER".to_string());
        options.insert("pass_env".to_string(), "MYSQL_PASSWORD".to_string());
        options.insert("ssl_mode".to_string(), "disabled".to_string());
        options.insert("allowed_schemas".to_string(), "mydb".to_string());

        register_mysql_tables(
            &mut ctx,
            "filtered_catalog",
            "mysql://127.0.0.1:3306/mydb",
            Some(&options),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("register filtered catalog");

        let catalog = ctx.catalog("filtered_catalog").expect("catalog not found");
        assert!(
            catalog.schema("mydb").is_some(),
            "allowed schema 'mydb' should be present"
        );
        assert!(
            catalog.schema("information_schema").is_none(),
            "system schema should be excluded"
        );
    }

    /// Read-write catalog mode allows INSERT and subsequent scan via qualified name.
    #[tokio::test]
    #[ignore]
    async fn test_catalog_read_write_insert_and_scan() {
        let mut ctx = SessionContext::new();
        let mut options = HashMap::new();
        options.insert("user_env".to_string(), "MYSQL_USER".to_string());
        options.insert("pass_env".to_string(), "MYSQL_PASSWORD".to_string());
        options.insert("ssl_mode".to_string(), "disabled".to_string());

        register_mysql_tables(
            &mut ctx,
            "mydb_rw",
            "mysql://127.0.0.1:3306/mydb",
            Some(&options),
            true,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("register rw catalog");

        ctx.sql(
            "INSERT INTO mydb_rw.mydb.users (name, email)
             VALUES ('CatalogInsert', 'catalog_insert@example.com')",
        )
        .await
        .expect("parse insert")
        .collect()
        .await
        .expect("execute insert");

        let batches = query_all(
            &ctx,
            "SELECT id FROM mydb_rw.mydb.users WHERE name = 'CatalogInsert'",
        )
        .await;
        assert_eq!(total_rows(&batches), 1);

        // Cleanup
        ctx.sql("DELETE FROM mydb_rw.mydb.users WHERE name = 'CatalogInsert'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_user_order_stats_schema_visibility() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "user_order_stats").await;

        let catalog = ctx.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();
        let table = schema.table("user_order_stats").await.unwrap().unwrap();
        let table_schema = table.schema();
        let field_names: Vec<&str> = table_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            field_names,
            vec![
                "id",
                "user_id",
                "user_name",
                "user_email",
                "total_orders",
                "total_spent",
                "last_order_date",
            ]
        );
    }
}
