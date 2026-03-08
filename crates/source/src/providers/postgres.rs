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
use datafusion::sql::unparser::dialect::PostgreSqlDialect;
use datafusion_table_providers::postgres::PostgresTableFactory;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use futures::stream;
use secrecy::SecretString;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// Register PostgreSQL tables into DataFusion SessionContext
///
/// # Arguments
/// * `session_ctx` - DataFusion session context to register tables into
/// * `name` - Name to register the table as
/// * `connection_string` - PostgreSQL connection string (e.g., "postgresql://host:port/db")
///   Note: Username and password should NOT be included in the connection string.
///   Use `user_env` and `pass_env` options instead.
/// * `options` - Optional configuration (e.g., table name, schema)
/// * `read_write` - If true, register as read-write table provider (allows INSERT/UPDATE/DELETE)
///
/// # Options
/// * `table` - Specific table name to register (required)
/// * `schema` - Schema name (default: "public")
/// * `user_env` - Environment variable name for username (optional, if not set, no username is used)
/// * `pass_env` - Environment variable name for password (optional, if not set, no password is used)
pub async fn register_postgres_tables(
    session_ctx: &mut SessionContext,
    name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
    read_write: bool,
) -> Result<()> {
    tracing::info!(
        "Registering PostgreSQL table: {} with connection: {} (read_write: {})",
        name,
        connection_string,
        read_write
    );
    tracing::debug!("Options: {:?}", options);

    // Extract table name from options (required)
    let table_name = options.and_then(|opts| opts.get("table")).ok_or_else(|| {
        anyhow::anyhow!("PostgreSQL data source '{}' requires 'table' option", name)
    })?;

    // Extract schema name from options (default: "public")
    let schema_name = options
        .and_then(|opts| opts.get("schema"))
        .unwrap_or(&"public".to_string())
        .clone();

    tracing::debug!(
        "Connecting to PostgreSQL table: {}.{} as '{}'",
        schema_name,
        table_name,
        name
    );

    // Parse connection string into parameters for PostgresConnectionPool
    let params = parse_connection_string(connection_string, options)?;

    // Create PostgreSQL connection pool
    let pool =
        Arc::new(PostgresConnectionPool::new(params).await.with_context(|| {
            format!("Failed to create PostgreSQL connection pool for '{}'", name)
        })?);

    // Create table factory
    let factory = PostgresTableFactory::new(Arc::clone(&pool));

    // Create the table reference: schema.table
    let table_reference = TableReference::partial(schema_name.as_str(), table_name.as_str());

    let mode_str = if read_write {
        "read-write"
    } else {
        "read-only"
    };
    tracing::debug!(
        "Creating PostgreSQL table provider ({}) for: {:?}",
        mode_str,
        table_reference
    );

    // Create the table provider based on access mode
    let table_provider = if read_write {
        factory
            .read_write_table_provider(table_reference.clone())
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to create read-write table provider for '{}.{}': {}",
                    schema_name,
                    table_name,
                    e
                )
            })?
    } else {
        factory
            .table_provider(table_reference.clone())
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to create read-only table provider for '{}.{}': {}",
                    schema_name,
                    table_name,
                    e
                )
            })?
    };

    // Wrap read-write providers to expose DELETE/UPDATE support
    let table_provider: Arc<dyn TableProvider> = if read_write {
        Arc::new(PostgresDmlProvider {
            inner: table_provider,
            pool: Arc::clone(&pool),
            table_reference: TableReference::partial(schema_name.as_str(), table_name.as_str()),
        })
    } else {
        table_provider
    };

    // Register the table with DataFusion
    session_ctx
        .register_table(name, table_provider)
        .map_err(|e| {
            tracing::error!("Failed to register table with DataFusion: {:?}", e);
            e
        })
        .with_context(|| format!("Failed to register table '{}' with DataFusion", name))?;

    tracing::info!(
        "Successfully registered PostgreSQL table '{}.{}' as '{}' ({})",
        schema_name,
        table_name,
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
struct PostgresDmlProvider {
    inner: Arc<dyn TableProvider>,
    pool: Arc<PostgresConnectionPool>,
    table_reference: TableReference,
}

#[async_trait]
impl TableProvider for PostgresDmlProvider {
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
        let table = self.table_reference.to_quoted_string();
        let where_clause = build_where_clause(&filters)?;
        let sql = format!("DELETE FROM {table}{where_clause}");
        Ok(Arc::new(PostgresDmlExec::new(Arc::clone(&self.pool), sql)))
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

        let unparser = Unparser::new(&PostgreSqlDialect {});
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
                Ok(format!("{} = {val}", quote_pg_ident(col)))
            })
            .collect::<DataFusionResult<Vec<_>>>()?
            .join(", ");

        let table = self.table_reference.to_quoted_string();
        let where_clause = build_where_clause(&filters)?;
        let sql = format!("UPDATE {table} SET {set_clause}{where_clause}");
        Ok(Arc::new(PostgresDmlExec::new(Arc::clone(&self.pool), sql)))
    }
}

/// Builds a ` WHERE expr1 AND expr2 ...` clause from a list of DataFusion
/// filter expressions.  Returns an empty string when `filters` is empty.
fn build_where_clause(filters: &[Expr]) -> DataFusionResult<String> {
    if filters.is_empty() {
        return Ok(String::new());
    }
    let unparser = Unparser::new(&PostgreSqlDialect {});
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

/// Quotes a PostgreSQL identifier with double quotes, escaping any embedded
/// double-quote characters.
fn quote_pg_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

// ─── Execution plan for DELETE / UPDATE results ─────────────────────────────

/// A leaf [`ExecutionPlan`] that executes a pre-built Postgres DML statement
/// and returns a single row `{ count: u64 }` with the number of affected rows.
struct PostgresDmlExec {
    pool: Arc<PostgresConnectionPool>,
    sql: String,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl PostgresDmlExec {
    fn new(pool: Arc<PostgresConnectionPool>, sql: String) -> Self {
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

impl fmt::Debug for PostgresDmlExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PostgresDmlExec(sql={})", self.sql)
    }
}

impl DisplayAs for PostgresDmlExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PostgresDmlExec")
    }
}

impl ExecutionPlan for PostgresDmlExec {
    fn name(&self) -> &str {
        "PostgresDmlExec"
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

        let future = async move {
            let conn = pool.connect_direct().await.map_err(|e| {
                DataFusionError::Execution(format!("Postgres DML connect error: {e}"))
            })?;
            let rows_affected = conn.conn.execute(sql.as_str(), &[]).await.map_err(|e| {
                DataFusionError::Execution(format!("Postgres DML execute error: {e}"))
            })?;
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

/// Parse PostgreSQL connection string into parameters HashMap
///
/// Converts a standard PostgreSQL connection string like:
/// "postgresql://host:port/db"
/// into a HashMap with secret-protected values
///
/// Username and password are ONLY read from environment variables if `user_env`
/// or `pass_env` are provided in options. We do not allow credentials in the
/// connection string for security reasons.
fn parse_connection_string(
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
) -> Result<HashMap<String, SecretString>> {
    // Parse the connection string URL
    let url = url::Url::parse(connection_string).with_context(|| {
        format!(
            "Invalid PostgreSQL connection string: {}",
            connection_string
        )
    })?;

    let mut params: HashMap<String, SecretString> = HashMap::new();

    // Extract host
    if let Some(host) = url.host_str() {
        params.insert(
            "host".to_string(),
            SecretString::new(host.to_string().into_boxed_str()),
        );
    }

    // Extract port
    if let Some(port) = url.port() {
        params.insert(
            "port".to_string(),
            SecretString::new(port.to_string().into_boxed_str()),
        );
    }

    // Extract user - only from environment variable if user_env is specified
    // We do not allow username/password in connection string for security
    if let Some(opts) = options {
        if let Some(user_env) = opts.get("user_env") {
            // Read username from environment variable (required)
            let username = std::env::var(user_env).with_context(|| {
                format!(
                    "Environment variable '{}' not found for PostgreSQL user",
                    user_env
                )
            })?;
            params.insert(
                "user".to_string(),
                SecretString::new(username.into_boxed_str()),
            );
        }
        // If user_env is not specified, we don't set user (no fallback to connection string)
    }

    // Extract password - only from environment variable if pass_env is specified
    // We do not allow username/password in connection string for security
    if let Some(opts) = options {
        if let Some(pass_env) = opts.get("pass_env") {
            // Read password from environment variable (required)
            let password = std::env::var(pass_env).with_context(|| {
                format!(
                    "Environment variable '{}' not found for PostgreSQL password",
                    pass_env
                )
            })?;
            params.insert(
                "pass".to_string(),
                SecretString::new(password.into_boxed_str()),
            );
        }
        // If pass_env is not specified, we don't set password (no fallback to connection string)
    }

    // Extract database name from path (remove leading /)
    let db_name = url.path().trim_start_matches('/');
    if !db_name.is_empty() {
        params.insert(
            "db".to_string(),
            SecretString::new(db_name.to_string().into_boxed_str()),
        );
    }

    // Extract query parameters
    for (key, value) in url.query_pairs() {
        params.insert(
            key.to_string(),
            SecretString::new(value.to_string().into_boxed_str()),
        );
    }

    Ok(params)
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    #[test]
    fn test_parse_connection_string_full() {
        let conn_str = "postgresql://@localhost:5432/mydb";
        let params = parse_connection_string(conn_str, None).unwrap();

        assert!(params.contains_key("host"));
        assert!(params.contains_key("port"));
        assert!(params.contains_key("db"));
    }

    #[test]
    fn test_parse_connection_string_no_password() {
        let conn_str = "postgresql://localhost:5432/mydb";
        let params = parse_connection_string(conn_str, None).unwrap();

        assert!(params.contains_key("host"));
    }

    #[test]
    fn test_parse_connection_string_with_query_params() {
        let conn_str = "postgresql://localhost:5432/db?sslmode=require&connect_timeout=10";
        let params = parse_connection_string(conn_str, None).unwrap();

        assert!(params.contains_key("sslmode"));
        assert!(params.contains_key("connect_timeout"));
    }

    #[test]
    fn test_parse_connection_string_invalid() {
        let conn_str = "not-a-valid-url";
        let result = parse_connection_string(conn_str, None);

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_connection_string_with_both_env() {
        unsafe {
            std::env::set_var("TEST_PG_USER", "envuser");
            std::env::set_var("TEST_PG_PASS", "envpass");
        }
        let conn_str = "postgresql://localhost:5432/mydb";
        let mut options: HashMap<String, String> = HashMap::new();
        options.insert("user_env".to_string(), "TEST_PG_USER".to_string());
        options.insert("pass_env".to_string(), "TEST_PG_PASS".to_string());
        let params = parse_connection_string(conn_str, Some(&options)).unwrap();

        assert_eq!(params.get("user").unwrap().expose_secret(), "envuser");
        assert_eq!(params.get("pass").unwrap().expose_secret(), "envpass");
        unsafe {
            std::env::remove_var("TEST_PG_USER");
            std::env::remove_var("TEST_PG_PASS");
        }
    }

    #[test]
    fn test_parse_connection_string_missing_env_var() {
        let conn_str = "postgresql://localhost:5432/mydb";
        let mut options: HashMap<String, String> = HashMap::new();
        options.insert("user_env".to_string(), "NONEXISTENT_VAR".to_string());
        let result = parse_connection_string(conn_str, Some(&options));

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Environment variable 'NONEXISTENT_VAR' not found")
        );
    }

    #[test]
    fn test_missing_table_option() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut session_ctx = SessionContext::new();
            let result = register_postgres_tables(
                &mut session_ctx,
                "test_table",
                "postgresql://localhost:5432/db",
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
    fn test_schema_option_default() {
        let options: HashMap<String, String> = HashMap::new();
        let schema_name = options
            .get("schema")
            .unwrap_or(&"public".to_string())
            .clone();

        assert_eq!(schema_name, "public");
    }

    #[test]
    fn test_schema_option_custom() {
        let mut options: HashMap<String, String> = HashMap::new();
        options.insert("schema".to_string(), "custom_schema".to_string());

        let schema_name = options
            .get("schema")
            .unwrap_or(&"public".to_string())
            .clone();

        assert_eq!(schema_name, "custom_schema");
    }

    #[test]
    fn test_table_reference_creation() {
        let schema = "public";
        let table = "users";
        let reference = TableReference::partial(schema, table);

        // Test that the reference is created successfully
        assert_eq!(reference.to_string(), "public.users");
    }

    // Note: Integration tests with actual PostgreSQL connection would require
    // a running PostgreSQL instance and should be placed in integration test files
    // or behind a feature flag for CI/CD environments
}
