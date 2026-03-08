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
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use futures::stream;
use mysql_async::prelude::Queryable;
use mysql_async::{Params, Row};
use secrecy::SecretString;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// Register MySQL tables into DataFusion SessionContext
///
/// # Arguments
/// * `session_ctx` - DataFusion session context to register tables into
/// * `name` - Name to register the table as
/// * `connection_string` - MySQL connection string (e.g., "mysql://host:port/db")
///   Note: Username and password should NOT be included in the connection string.
///   Use `user_env` and `pass_env` options instead.
/// * `options` - Optional configuration (e.g., table name, schema)
/// * `read_write` - If true, register as read-write table provider (allows INSERT/UPDATE/DELETE)
///
/// # Options
/// * `table` - Specific table name to register (required)
/// * `schema` - Schema/database name (optional, can be in connection string)
/// * `user_env` - Environment variable name for username (optional, if not set, no username is used)
/// * `pass_env` - Environment variable name for password (optional, if not set, no password is used)
/// * `ssl_mode` - SSL mode: "disabled", "preferred", "required" (optional, defaults to "disabled" for local dev)
pub async fn register_mysql_tables(
    session_ctx: &mut SessionContext,
    name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
    read_write: bool,
) -> Result<()> {
    tracing::info!(
        "Registering MySQL table: {} with connection: {} (read_write: {})",
        name,
        connection_string,
        read_write
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

    let pool = Arc::new(
        MySQLConnectionPool::new(params)
            .await
            .with_context(|| format!("Failed to create MySQL connection pool for '{}'", name))?,
    );

    let factory = MySQLTableFactory::new(Arc::clone(&pool));

    let table_reference = if let Some(schema) = schema_name {
        TableReference::partial(schema.as_str(), table_name.as_str())
    } else {
        TableReference::bare(table_name.as_str())
    };

    let mode_str = if read_write {
        "read-write"
    } else {
        "read-only"
    };
    tracing::debug!(
        "Creating MySQL table provider ({}) for: {:?}",
        mode_str,
        table_reference
    );

    let table_provider = if read_write {
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

    // Wrap read-write providers to expose DELETE/UPDATE support
    let table_provider: Arc<dyn TableProvider> = if read_write {
        Arc::new(MySQLDmlProvider {
            inner: table_provider,
            pool: Arc::clone(&pool),
            table_reference: table_reference.clone(),
        })
    } else {
        table_provider
    };

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
    let url = url::Url::parse(connection_string)
        .with_context(|| format!("Invalid MySQL connection string: {}", connection_string))?;

    let mut params: HashMap<String, SecretString> = HashMap::new();

    if let Some(host) = url.host_str() {
        params.insert(
            "host".to_string(),
            SecretString::new(host.to_string().into_boxed_str()),
        );
    }

    if let Some(port) = url.port() {
        params.insert(
            "tcp_port".to_string(),
            SecretString::new(port.to_string().into_boxed_str()),
        );
    } else {
        params.insert(
            "tcp_port".to_string(),
            SecretString::new("3306".to_string().into_boxed_str()),
        );
    }

    let db_name = url
        .path()
        .trim_start_matches('/')
        .split('/')
        .next()
        .unwrap_or("");

    if !db_name.is_empty() {
        params.insert(
            "db".to_string(),
            SecretString::new(db_name.to_string().into_boxed_str()),
        );
    }

    if let Some(opts) = options {
        if let Some(user_env) = opts.get("user_env") {
            let username = std::env::var(user_env).with_context(|| {
                format!(
                    "Environment variable '{}' not found for MySQL user",
                    user_env
                )
            })?;
            params.insert(
                "user".to_string(),
                SecretString::new(username.into_boxed_str()),
            );
        }

        if let Some(pass_env) = opts.get("pass_env") {
            let password = std::env::var(pass_env).with_context(|| {
                format!(
                    "Environment variable '{}' not found for MySQL password",
                    pass_env
                )
            })?;
            params.insert(
                "pass".to_string(),
                SecretString::new(password.into_boxed_str()),
            );
        }
    }

    let ssl_mode = options
        .and_then(|opts| opts.get("ssl_mode"))
        .map(|s| s.to_lowercase())
        .unwrap_or_else(|| {
            tracing::debug!(
                "SSL mode not specified, defaulting to 'disabled' for local development"
            );
            "disabled".to_string()
        });

    tracing::debug!("Setting SSL mode to: {}", ssl_mode);

    params.insert(
        "sslmode".to_string(),
        SecretString::new(ssl_mode.into_boxed_str()),
    );

    Ok(params)
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
            )
            .await;

            assert!(result.is_err());
            let error_msg = result.unwrap_err().to_string();
            assert!(error_msg.contains("requires 'table' option"));
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
}
