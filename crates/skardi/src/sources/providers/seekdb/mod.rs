//! SeekDB provider.
//!
//! [SeekDB](https://github.com/oceanbase/seekdb) is an AI-native search database
//! built on OceanBase. It speaks the MySQL wire protocol, so we reuse the
//! `mysql_async`-based connection machinery from the MySQL provider for CRUD,
//! but add two things that the vanilla MySQL provider does not have:
//!
//! * `seekdb_fts` — a UDTF wrapping SeekDB's native `FULLTEXT` index with the
//!   IK analyzer, returning a BM25-style `_score` column.
//! * `seekdb_knn` — a UDTF wrapping SeekDB's native `VECTOR` column + HNSW
//!   index, returning a `_score` column equal to the raw distance (lower is
//!   more similar).
//!
//! Everything else — scan / insert / delete / update, catalog mode, env-var
//! credentials, SSL toggle — is handled exactly the same way the MySQL
//! provider handles it. Docker one-liner:
//!
//! ```bash
//! docker run -d --name seekdb -p 2881:2881 -p 2886:2886 \
//!   -v ./data:/var/lib/oceanbase oceanbase/seekdb:latest
//! ```

pub mod fts_exec;
pub mod fts_table_function;
pub mod knn_exec;
pub mod knn_table_function;

pub use fts_table_function::register_seekdb_fts_udtf;
pub use knn_table_function::{SeekDbKnnEntry, register_seekdb_knn_udtf};

use crate::sources::DataSourceType;
use crate::sources::hierarchy::{
    HierarchyLevel, SourceLabel, build_catalog, parse_allowed_schemas, retry_with_timeout,
};
use crate::sources::providers::{DatasetEntry, DatasetRegistry};
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

/// Default SeekDB port (OceanBase MySQL-compatible endpoint).
const DEFAULT_SEEKDB_PORT: &str = "2881";

/// Register SeekDB tables or a whole database (catalog) into a DataFusion [`SessionContext`].
///
/// Single-table mode (default) registers one table under `name`. Catalog mode
/// registers one provider per table across all non-system schemas.
///
/// Because SeekDB is MySQL wire-compatible, this piggybacks on the MySQL
/// connection pool from `datafusion-table-providers`. The dispatch code path
/// is identical to MySQL's except for:
///
/// * default port (2881 instead of 3306),
/// * a `DataSourceType::Seekdb` source label on retry/timeout metrics,
/// * optional population of the shared dataset registry so that the
///   `seekdb_fts` / `seekdb_knn` UDTFs can look up the table by name.
///
/// # Arguments
/// * `session_ctx` - DataFusion session context to register tables into
/// * `name` - Name to register the table (table mode) or catalog (catalog mode) as
/// * `connection_string` - SeekDB connection string (e.g., "mysql://host:2881/db")
///   Note: Username and password should NOT be included in the connection string.
///   Use `user_env` and `pass_env` options instead.
/// * `options` - Optional configuration (see below)
/// * `read_write` - If true, register as read-write (allows INSERT/UPDATE/DELETE)
/// * `registry` - Optional dataset registry for `seekdb_fts` / `seekdb_knn`
/// * `hierarchy_level` - [`HierarchyLevel::Table`] (default) or [`HierarchyLevel::Catalog`]
///
/// # Options
/// * `table` - Table name (required in table mode)
/// * `schema` - Database/schema name (optional in table mode)
/// * `allowed_schemas` - Comma-separated schema allow-list (catalog mode only)
/// * `user_env` - Environment variable name for username
/// * `pass_env` - Environment variable name for password
/// * `ssl_mode` - "disabled" | "preferred" | "required" (default: "disabled")
pub async fn register_seekdb_tables(
    session_ctx: &mut SessionContext,
    name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
    read_write: bool,
    registry: Option<&DatasetRegistry>,
    hierarchy_level: HierarchyLevel,
) -> Result<()> {
    let mode_str = if read_write {
        "read-write"
    } else {
        "read-only"
    };
    match hierarchy_level {
        HierarchyLevel::Catalog => {
            register_seekdb_catalog(
                session_ctx,
                name,
                connection_string,
                options,
                read_write,
                mode_str,
                registry,
            )
            .await
        }
        HierarchyLevel::Table => {
            register_single_seekdb_table(
                session_ctx,
                name,
                connection_string,
                options,
                read_write,
                mode_str,
                registry,
            )
            .await
        }
    }
}

/// Register one SeekDB table under `name` in the default catalog.
async fn register_single_seekdb_table(
    session_ctx: &mut SessionContext,
    name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
    read_write: bool,
    mode_str: &str,
    registry: Option<&DatasetRegistry>,
) -> Result<()> {
    tracing::info!(
        "Registering SeekDB table: {} with connection: {} ({})",
        name,
        connection_string,
        mode_str
    );
    tracing::debug!("Options: {:?}", options);

    let table_name = options
        .and_then(|opts| opts.get("table"))
        .ok_or_else(|| anyhow::anyhow!("SeekDB data source '{}' requires 'table' option", name))?;

    let schema_name = options.and_then(|opts| opts.get("schema"));

    let params = parse_connection_params(connection_string, options)?;

    let label = SourceLabel::new(DataSourceType::Seekdb, HierarchyLevel::Table, name);
    let pool = Arc::new(
        retry_with_timeout(label, "pool creation", || async {
            MySQLConnectionPool::new(params.clone())
                .await
                .map_err(|e| anyhow::anyhow!(e))
        })
        .await
        .with_context(|| format!("Failed to create SeekDB connection pool for '{}'", name))?,
    );

    let table_reference = if let Some(schema) = schema_name {
        TableReference::partial(schema.as_str(), table_name.as_str())
    } else {
        TableReference::bare(table_name.as_str())
    };

    tracing::debug!(
        "Creating SeekDB table provider ({}) for: {:?}",
        mode_str,
        table_reference
    );

    let table_provider =
        build_seekdb_table_provider(Arc::clone(&pool), table_reference.clone(), read_write).await?;

    // Populate the registry for seekdb_fts / seekdb_knn UDTFs.
    if let Some(registry) = registry {
        let columns: Vec<(String, DataType)> = table_provider
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect();
        let entry = SeekDbKnnEntry {
            pool: Arc::clone(&pool),
            qualified_table: quote_seekdb_table(&table_reference),
            columns,
        };
        let mut reg = registry
            .write()
            .map_err(|e| anyhow::anyhow!("seekdb registry lock error: {}", e))?;
        reg.insert(name.to_string(), DatasetEntry::Seekdb(entry));
        tracing::debug!("Registered SeekDB table '{}' in dataset registry", name);
    }

    session_ctx
        .register_table(name, table_provider)
        .map_err(|e| {
            tracing::error!("Failed to register table with DataFusion: {:?}", e);
            e
        })
        .with_context(|| format!("Failed to register SeekDB table '{}' with DataFusion", name))?;

    tracing::info!(
        "Successfully registered SeekDB table '{}' as '{}' ({})",
        table_reference,
        name,
        mode_str
    );

    Ok(())
}

/// Register an entire SeekDB database as a named DataFusion catalog.
async fn register_seekdb_catalog(
    session_ctx: &mut SessionContext,
    catalog_name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
    read_write: bool,
    mode_str: &str,
    registry: Option<&DatasetRegistry>,
) -> Result<()> {
    tracing::info!(
        "Registering SeekDB catalog: {} ({})",
        catalog_name,
        mode_str
    );

    let params = parse_connection_params(connection_string, options)?;
    let label = SourceLabel::new(
        DataSourceType::Seekdb,
        HierarchyLevel::Catalog,
        catalog_name,
    );

    let pool = Arc::new(
        retry_with_timeout(label, "pool creation", || async {
            MySQLConnectionPool::new(params.clone())
                .await
                .map_err(|e| anyhow::anyhow!(e))
        })
        .await
        .with_context(|| {
            format!(
                "Failed to create SeekDB connection pool for catalog '{}'",
                catalog_name
            )
        })?,
    );

    let allowed_schemas = parse_allowed_schemas(options);
    let schema_tables = retry_with_timeout(label, "information_schema introspection", || async {
        list_seekdb_tables_in_catalog(&pool, allowed_schemas.as_deref()).await
    })
    .await
    .with_context(|| {
        format!(
            "Failed to list SeekDB tables for catalog-wide registration in source '{}'",
            catalog_name
        )
    })?;

    if schema_tables.is_empty() {
        tracing::warn!(
            "No tables found in SeekDB catalog for source '{}'",
            catalog_name
        );
    }

    let table_count = schema_tables.len();
    let registry_shared = registry.map(Arc::clone);
    let catalog_name_owned = catalog_name.to_string();

    build_catalog(
        session_ctx,
        catalog_name,
        schema_tables,
        |schema, table_name| {
            let pool = Arc::clone(&pool);
            let registry_c = registry_shared.clone();
            let catalog_c = catalog_name_owned.clone();
            let schema_c = schema.clone();
            let table_c = table_name.clone();
            async move {
                let table_reference = TableReference::bare(table_c.as_str());
                let provider = build_seekdb_table_provider(
                    Arc::clone(&pool),
                    table_reference.clone(),
                    read_write,
                )
                .await?;

                if let Some(registry) = registry_c {
                    let columns: Vec<(String, DataType)> = provider
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| (f.name().clone(), f.data_type().clone()))
                        .collect();
                    let entry = SeekDbKnnEntry {
                        pool: Arc::clone(&pool),
                        qualified_table: quote_seekdb_ident(&schema_c)
                            + "."
                            + &quote_seekdb_ident(&table_c),
                        columns,
                    };
                    // Key matches the three-part SQL reference so that UDTF
                    // callers can use e.g. `seekdb_fts('demo.main.articles', ...)`.
                    let key = format!("{}.{}.{}", catalog_c, schema_c, table_c);
                    let mut reg = registry
                        .write()
                        .map_err(|e| anyhow::anyhow!("seekdb registry lock error: {}", e))?;
                    reg.insert(key, DatasetEntry::Seekdb(entry));
                }

                Ok(provider)
            }
        },
    )
    .await
    .with_context(|| format!("Failed to build SeekDB catalog '{}'", catalog_name))?;

    tracing::info!(
        "Registered SeekDB catalog '{}' with {} table(s) ({})",
        catalog_name,
        table_count,
        mode_str
    );

    Ok(())
}

/// List all user tables and views across a SeekDB instance via `information_schema`.
///
/// Excludes built-in OceanBase/MySQL system schemas.
async fn list_seekdb_tables_in_catalog(
    pool: &MySQLConnectionPool,
    allowed_schemas: Option<&[String]>,
) -> Result<Vec<(String, String)>> {
    let db_conn = pool.connect().await.map_err(|e| {
        anyhow::anyhow!("Failed to connect to SeekDB for catalog introspection: {e}")
    })?;

    let mysql_conn = db_conn
        .as_any()
        .downcast_ref::<MySQLConnection>()
        .ok_or_else(|| {
            anyhow::anyhow!("Unexpected MySQL connection type during SeekDB catalog introspection")
        })?;

    let mut conn = mysql_conn.conn.lock().await;

    // OceanBase/SeekDB inherits MySQL's system schemas plus `oceanbase`.
    const BASE_QUERY: &str = "SELECT table_schema, table_name
         FROM information_schema.tables
         WHERE table_type IN ('BASE TABLE', 'VIEW')
           AND table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys', 'oceanbase', 'LBACSYS', 'ORAAUDITOR')";

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
            .with_context(|| "Failed to query information_schema for SeekDB catalog listing")?
        }
        _ => {
            let query = format!("{BASE_QUERY} ORDER BY table_schema, table_name");
            conn.query_map(query, |(schema, table): (String, String)| (schema, table))
                .await
                .with_context(|| "Failed to query information_schema for SeekDB catalog listing")?
        }
    };

    Ok(rows)
}

/// Build a [`TableProvider`] for a single SeekDB table, wrapping read-write providers in
/// [`SeekDbDmlProvider`] to expose `DELETE` and `UPDATE` support.
async fn build_seekdb_table_provider(
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
                    "Failed to create read-write SeekDB table provider for '{:?}': {}",
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
                    "Failed to create read-only SeekDB table provider for '{:?}': {}",
                    table_reference,
                    e
                )
            })?
    };

    let result: Arc<dyn TableProvider> = if read_write {
        Arc::new(SeekDbDmlProvider {
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
struct SeekDbDmlProvider {
    inner: Arc<dyn TableProvider>,
    pool: Arc<MySQLConnectionPool>,
    table_reference: TableReference,
}

#[async_trait]
impl TableProvider for SeekDbDmlProvider {
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
        let table = quote_seekdb_table(&self.table_reference);
        let where_clause = build_seekdb_where_clause(&filters)?;
        let sql = format!("DELETE FROM {table}{where_clause}");
        Ok(Arc::new(SeekDbDmlExec::new(Arc::clone(&self.pool), sql)))
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
                Ok(format!("{} = {val}", quote_seekdb_ident(col)))
            })
            .collect::<DataFusionResult<Vec<_>>>()?
            .join(", ");

        let table = quote_seekdb_table(&self.table_reference);
        let where_clause = build_seekdb_where_clause(&filters)?;
        let sql = format!("UPDATE {table} SET {set_clause}{where_clause}");
        Ok(Arc::new(SeekDbDmlExec::new(Arc::clone(&self.pool), sql)))
    }
}

/// Builds a ` WHERE expr1 AND expr2 ...` clause from a list of DataFusion
/// filter expressions. Returns an empty string when `filters` is empty.
fn build_seekdb_where_clause(filters: &[Expr]) -> DataFusionResult<String> {
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

/// Quote a SeekDB identifier with backticks, escaping any embedded backticks.
pub(crate) fn quote_seekdb_ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// Produce a backtick-quoted `[[catalog.]schema.]table` string.
pub(crate) fn quote_seekdb_table(tbl: &TableReference) -> String {
    [tbl.catalog(), tbl.schema(), Some(tbl.table())]
        .into_iter()
        .flatten()
        .map(quote_seekdb_ident)
        .collect::<Vec<_>>()
        .join(".")
}

/// Convert a DataFusion filter `Expr` to a MySQL-dialect SQL string usable
/// inside a SeekDB WHERE clause. Returns `None` for expressions the unparser
/// cannot represent — callers skip pushdown in that case.
pub(crate) fn expr_to_seekdb_sql(expr: &Expr) -> Option<String> {
    let unparser = Unparser::new(&MySqlDialect {});
    unparser.expr_to_sql(expr).ok().map(|ast| ast.to_string())
}

// ─── Execution plan for DELETE / UPDATE results ─────────────────────────────

/// A leaf [`ExecutionPlan`] that executes a pre-built SeekDB DML statement
/// and returns a single row `{ count: u64 }` with the number of affected rows.
struct SeekDbDmlExec {
    pool: Arc<MySQLConnectionPool>,
    sql: String,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl SeekDbDmlExec {
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

impl fmt::Debug for SeekDbDmlExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SeekDbDmlExec(sql={})", self.sql)
    }
}

impl DisplayAs for SeekDbDmlExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SeekDbDmlExec")
    }
}

impl ExecutionPlan for SeekDbDmlExec {
    fn name(&self) -> &str {
        "SeekDbDmlExec"
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
            let conn_obj = pool.connect_direct().await.map_err(|e| {
                DataFusionError::Execution(format!("SeekDB DML connect error: {e}"))
            })?;
            let mut conn = conn_obj.conn.lock().await;
            let conn = &mut *conn;
            let _: Vec<Row> = conn.exec(&sql, Params::Empty).await.map_err(|e| {
                DataFusionError::Execution(format!("SeekDB DML execute error: {e}"))
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

/// Parse a SeekDB connection string into the parameter map expected by
/// [`MySQLConnectionPool`]. Identical to MySQL's parser except that the
/// default port is 2881 (OceanBase / SeekDB) instead of 3306.
pub(crate) fn parse_connection_params(
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
) -> Result<HashMap<String, SecretString>> {
    let url = url::Url::parse(connection_string)
        .with_context(|| format!("Invalid SeekDB connection string: {}", connection_string))?;

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
            SecretString::new(DEFAULT_SEEKDB_PORT.to_string().into_boxed_str()),
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
                    "Environment variable '{}' not found for SeekDB user",
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
                    "Environment variable '{}' not found for SeekDB password",
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

    params.insert(
        "sslmode".to_string(),
        SecretString::new(ssl_mode.into_boxed_str()),
    );

    Ok(params)
}

// ─── Unit tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    #[test]
    fn test_parse_connection_params_full() {
        let conn_str = "mysql://localhost:2881/mydb";
        let params = parse_connection_params(conn_str, None).unwrap();

        assert_eq!(params.get("host").unwrap().expose_secret(), "localhost");
        assert_eq!(params.get("tcp_port").unwrap().expose_secret(), "2881");
        assert_eq!(params.get("db").unwrap().expose_secret(), "mydb");
        assert_eq!(params.get("sslmode").unwrap().expose_secret(), "disabled");
    }

    #[test]
    fn test_parse_connection_params_default_port_is_2881() {
        let conn_str = "mysql://localhost/mydb";
        let params = parse_connection_params(conn_str, None).unwrap();

        assert_eq!(
            params.get("tcp_port").unwrap().expose_secret(),
            DEFAULT_SEEKDB_PORT,
            "SeekDB should default to 2881, not MySQL's 3306"
        );
    }

    #[test]
    fn test_parse_connection_params_custom_port() {
        let conn_str = "mysql://localhost:2899/mydb";
        let params = parse_connection_params(conn_str, None).unwrap();

        assert_eq!(params.get("tcp_port").unwrap().expose_secret(), "2899");
    }

    #[test]
    fn test_parse_connection_params_no_database() {
        let conn_str = "mysql://localhost:2881";
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
                .contains("Invalid SeekDB connection string")
        );
    }

    #[test]
    fn test_parse_connection_params_with_env_credentials() {
        unsafe {
            std::env::set_var("TEST_SEEKDB_USER", "seekuser");
            std::env::set_var("TEST_SEEKDB_PASS", "seekpass");
        }

        let conn_str = "mysql://localhost:2881/mydb";
        let mut options: HashMap<String, String> = HashMap::new();
        options.insert("user_env".to_string(), "TEST_SEEKDB_USER".to_string());
        options.insert("pass_env".to_string(), "TEST_SEEKDB_PASS".to_string());

        let params = parse_connection_params(conn_str, Some(&options)).unwrap();

        assert_eq!(params.get("user").unwrap().expose_secret(), "seekuser");
        assert_eq!(params.get("pass").unwrap().expose_secret(), "seekpass");

        unsafe {
            std::env::remove_var("TEST_SEEKDB_USER");
            std::env::remove_var("TEST_SEEKDB_PASS");
        }
    }

    #[test]
    fn test_parse_connection_params_missing_user_env() {
        let conn_str = "mysql://localhost:2881/mydb";
        let mut options: HashMap<String, String> = HashMap::new();
        options.insert(
            "user_env".to_string(),
            "NONEXISTENT_SEEKDB_USER".to_string(),
        );

        let result = parse_connection_params(conn_str, Some(&options));

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Environment variable 'NONEXISTENT_SEEKDB_USER' not found")
        );
    }

    #[test]
    fn test_parse_connection_params_ssl_mode_required() {
        let conn_str = "mysql://localhost:2881/mydb";
        let mut options: HashMap<String, String> = HashMap::new();
        options.insert("ssl_mode".to_string(), "required".to_string());

        let params = parse_connection_params(conn_str, Some(&options)).unwrap();

        assert_eq!(params.get("sslmode").unwrap().expose_secret(), "required");
    }

    #[test]
    fn test_parse_connection_params_ssl_mode_case_insensitive() {
        let conn_str = "mysql://localhost:2881/mydb";
        let mut options: HashMap<String, String> = HashMap::new();
        options.insert("ssl_mode".to_string(), "PREFERRED".to_string());

        let params = parse_connection_params(conn_str, Some(&options)).unwrap();

        assert_eq!(params.get("sslmode").unwrap().expose_secret(), "preferred");
    }

    #[test]
    fn test_quote_seekdb_ident_backticks_and_escaping() {
        assert_eq!(quote_seekdb_ident("articles"), "`articles`");
        assert_eq!(quote_seekdb_ident("weird`name"), "`weird``name`");
    }

    #[test]
    fn test_quote_seekdb_table_bare() {
        let tr = TableReference::bare("articles");
        assert_eq!(quote_seekdb_table(&tr), "`articles`");
    }

    #[test]
    fn test_quote_seekdb_table_partial() {
        let tr = TableReference::partial("mydb", "articles");
        assert_eq!(quote_seekdb_table(&tr), "`mydb`.`articles`");
    }

    #[test]
    fn test_missing_table_option() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut session_ctx = SessionContext::new();
            let result = register_seekdb_tables(
                &mut session_ctx,
                "test_table",
                "mysql://localhost:2881/db",
                None,
                false,
                None,
                HierarchyLevel::default(),
            )
            .await;

            assert!(result.is_err());
            let error_msg = result.unwrap_err().to_string();
            assert!(error_msg.contains("requires 'table' option"));
        });
    }

    #[test]
    fn test_hierarchy_level_default_routes_to_table_mode() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut ctx = SessionContext::new();
            let err = register_seekdb_tables(
                &mut ctx,
                "t",
                "mysql://localhost/db",
                None,
                false,
                None,
                HierarchyLevel::default(),
            )
            .await
            .unwrap_err();
            assert!(err.to_string().contains("requires 'table' option"));
        });
    }

    // ─── Integration test helpers ────────────────────────────────────────
    // These require a live SeekDB instance. Set SEEKDB_USER, SEEKDB_PASSWORD.

    async fn register_ci_table(ctx: &mut SessionContext, table: &str) {
        let mut options = HashMap::new();
        options.insert("table".to_string(), table.to_string());
        options.insert("user_env".to_string(), "SEEKDB_USER".to_string());
        options.insert("pass_env".to_string(), "SEEKDB_PASSWORD".to_string());
        options.insert("ssl_mode".to_string(), "disabled".to_string());
        register_seekdb_tables(
            ctx,
            table,
            "mysql://127.0.0.1:2881/mydb",
            Some(&options),
            true,
            None,
            HierarchyLevel::Table,
        )
        .await
        .unwrap_or_else(|e| panic!("register {} failed: {}", table, e));
    }

    async fn query_all(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        let df = ctx.sql(sql).await.expect("parse sql");
        df.collect().await.expect("collect results")
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

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
    async fn test_scan_with_filter() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "users").await;

        let batches = query_all(&ctx, "SELECT id, name FROM users WHERE id = 2").await;
        assert_eq!(total_rows(&batches), 1);
    }

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

        let batches = query_all(&ctx, "SELECT id, name FROM users ORDER BY id").await;
        assert!(total_rows(&batches) >= 4);

        // Clean up
        ctx.sql("DELETE FROM users WHERE name = 'Dave Brown'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_delete_with_filter() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "users").await;

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

        // Reset
        ctx.sql("UPDATE users SET email = 'alice@example.com' WHERE name = 'Alice Smith'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
    }
}
