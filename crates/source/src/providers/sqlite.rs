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
use datafusion::sql::unparser::dialect::SqliteDialect;
use datafusion_table_providers::sql::db_connection_pool::Mode;
use datafusion_table_providers::sql::db_connection_pool::sqlitepool::SqliteConnectionPoolFactory;
use datafusion_table_providers::sqlite::write::SqliteTableWriter;
use datafusion_table_providers::sqlite::{Sqlite, SqliteTableFactory};
use futures::stream;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio_rusqlite::Connection;

/// Create a read-only SQLite table provider for a single table.
pub async fn create_sqlite_table_provider(
    db_path: &str,
    table_name: &str,
) -> Result<Arc<dyn TableProvider>> {
    let pool = Arc::new(
        SqliteConnectionPoolFactory::new(db_path, Mode::File, Duration::from_millis(5000))
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create SQLite connection pool: {}", e))?,
    );

    let factory = SqliteTableFactory::new(Arc::clone(&pool));
    let table_ref = TableReference::bare(table_name);

    let provider = factory
        .table_provider(table_ref)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create SQLite table provider: {}", e))?;

    Ok(provider)
}

/// Register SQLite tables into DataFusion SessionContext
///
/// # Arguments
/// * `session_ctx` - DataFusion session context to register tables into
/// * `name` - Name to register the table as
/// * `db_path` - Path to the SQLite database file (e.g., "/data/my.db")
/// * `options` - Optional configuration (e.g., table name)
/// * `read_write` - If true, register as read-write table provider (allows INSERT/UPDATE/DELETE)
///
/// # Options
/// * `table` - Specific table name to register (required)
/// * `busy_timeout_ms` - Busy timeout in milliseconds (optional, defaults to 5000)
pub async fn register_sqlite_tables(
    session_ctx: &mut SessionContext,
    name: &str,
    db_path: &str,
    options: Option<&HashMap<String, String>>,
    read_write: bool,
) -> Result<()> {
    tracing::info!(
        "Registering SQLite table: {} with path: {} (read_write: {})",
        name,
        db_path,
        read_write
    );
    tracing::debug!("Options: {:?}", options);

    let table_name = options
        .and_then(|opts| opts.get("table"))
        .ok_or_else(|| anyhow::anyhow!("SQLite data source '{}' requires 'table' option", name))?;

    let busy_timeout_ms: u64 = options
        .and_then(|opts| opts.get("busy_timeout_ms"))
        .and_then(|v| v.parse().ok())
        .unwrap_or(5000);

    tracing::debug!(
        "Connecting to SQLite table: {} in database '{}' as '{}'",
        table_name,
        db_path,
        name
    );

    let mode = if db_path == ":memory:" {
        Mode::Memory
    } else {
        Mode::File
    };

    let pool = Arc::new(
        SqliteConnectionPoolFactory::new(db_path, mode, Duration::from_millis(busy_timeout_ms))
            .build()
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to create SQLite connection pool for '{}': {}",
                    name,
                    e
                )
            })?,
    );

    let factory = SqliteTableFactory::new(Arc::clone(&pool));

    let table_reference = TableReference::bare(table_name.as_str());

    let mode_str = if read_write {
        "read-write"
    } else {
        "read-only"
    };
    tracing::debug!(
        "Creating SQLite table provider ({}) for: {:?}",
        mode_str,
        table_reference
    );

    let table_provider = factory
        .table_provider(table_reference.clone())
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to create table provider for '{:?}': {}",
                table_reference,
                e
            )
        })?;

    // Wrap read-write providers to expose INSERT/DELETE/UPDATE support
    let table_provider: Arc<dyn TableProvider> = if read_write {
        // Open a separate connection for DELETE/UPDATE operations
        let dml_conn = if db_path == ":memory:" {
            anyhow::bail!(
                "SQLite in-memory databases do not support read_write mode \
                 because DML operations require a separate connection"
            );
        } else {
            Connection::open(&db_path)
                .await
                .with_context(|| format!("Failed to open SQLite DML connection for '{}'", name))?
        };

        // Wrap with SqliteTableWriter for INSERT support (uses the library's DataSink)
        let schema = table_provider.schema();
        let sqlite = Sqlite::new(
            table_reference.clone(),
            schema,
            Arc::clone(&pool),
            Constraints::new_unverified(vec![]),
        );
        let write_provider = SqliteTableWriter::create(table_provider, sqlite, None);

        // Wrap with SQLiteDmlProvider for DELETE/UPDATE support
        Arc::new(SQLiteDmlProvider {
            inner: write_provider,
            conn: Arc::new(dml_conn),
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
        .with_context(|| format!("Failed to register SQLite table '{}' with DataFusion", name))?;

    tracing::info!(
        "Successfully registered SQLite table '{}' as '{}' ({})",
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
struct SQLiteDmlProvider {
    inner: Arc<dyn TableProvider>,
    conn: Arc<Connection>,
    table_reference: TableReference,
}

#[async_trait]
impl TableProvider for SQLiteDmlProvider {
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
        let table = quote_sqlite_table(&self.table_reference);
        let where_clause = build_sqlite_where_clause(&filters)?;
        let sql = format!("DELETE FROM {table}{where_clause}");
        Ok(Arc::new(SQLiteDmlExec::new(Arc::clone(&self.conn), sql)))
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

        let unparser = Unparser::new(&SqliteDialect {});
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
                Ok(format!("{} = {val}", quote_sqlite_ident(col)))
            })
            .collect::<DataFusionResult<Vec<_>>>()?
            .join(", ");

        let table = quote_sqlite_table(&self.table_reference);
        let where_clause = build_sqlite_where_clause(&filters)?;
        let sql = format!("UPDATE {table} SET {set_clause}{where_clause}");
        Ok(Arc::new(SQLiteDmlExec::new(Arc::clone(&self.conn), sql)))
    }
}

/// Builds a ` WHERE expr1 AND expr2 ...` clause from a list of DataFusion
/// filter expressions. Returns an empty string when `filters` is empty.
fn build_sqlite_where_clause(filters: &[Expr]) -> DataFusionResult<String> {
    if filters.is_empty() {
        return Ok(String::new());
    }
    let unparser = Unparser::new(&SqliteDialect {});
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

/// Quotes a SQLite identifier with double quotes, escaping any embedded double quotes.
fn quote_sqlite_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Produces a properly quoted table reference string for SQLite.
fn quote_sqlite_table(tbl: &TableReference) -> String {
    // SQLite doesn't use catalog/schema in the same way; just use the table name.
    quote_sqlite_ident(tbl.table())
}

// ─── Execution plan for DELETE / UPDATE results ─────────────────────────────

/// A leaf [`ExecutionPlan`] that executes a pre-built SQLite DML statement
/// and returns a single row `{ count: u64 }` with the number of affected rows.
struct SQLiteDmlExec {
    conn: Arc<Connection>,
    sql: String,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl SQLiteDmlExec {
    fn new(conn: Arc<Connection>, sql: String) -> Self {
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
            conn,
            sql,
            schema,
            properties,
        }
    }
}

impl fmt::Debug for SQLiteDmlExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SQLiteDmlExec(sql={})", self.sql)
    }
}

impl DisplayAs for SQLiteDmlExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SQLiteDmlExec")
    }
}

impl ExecutionPlan for SQLiteDmlExec {
    fn name(&self) -> &str {
        "SQLiteDmlExec"
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
        let conn = Arc::clone(&self.conn);
        let sql = self.sql.clone();
        let schema = Arc::clone(&self.schema);

        let future = async move {
            let rows_affected: u64 = conn
                .call(
                    move |conn| -> Result<u64, tokio_rusqlite::rusqlite::Error> {
                        let affected = conn.execute(&sql, [])?;
                        Ok(affected as u64)
                    },
                )
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!("SQLite DML execute error: {e}"))
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;

    #[test]
    fn test_quote_sqlite_ident() {
        assert_eq!(quote_sqlite_ident("users"), "\"users\"");
        assert_eq!(quote_sqlite_ident("my\"table"), "\"my\"\"table\"");
    }

    #[test]
    fn test_quote_sqlite_table() {
        let reference = TableReference::bare("users");
        assert_eq!(quote_sqlite_table(&reference), "\"users\"");
    }

    #[test]
    fn test_missing_table_option() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut session_ctx = SessionContext::new();
            let result =
                register_sqlite_tables(&mut session_ctx, "test_table", "/tmp/test.db", None, false)
                    .await;

            assert!(result.is_err());
            let error_msg = result.unwrap_err().to_string();
            assert!(error_msg.contains("requires 'table' option"));
        });
    }

    // ─── Helper ─────────────────────────────────────────────────────────

    /// Create a temp SQLite file with a `test_items` table seeded with sample rows.
    /// Returns the path to the temp file (caller must clean up).
    async fn create_test_db() -> tempfile::TempPath {
        let tmp = tempfile::NamedTempFile::new().expect("create temp file");
        let path = tmp.into_temp_path();
        let db_path = path.to_str().unwrap().to_string();

        let conn = Connection::open(&db_path).await.expect("open temp sqlite");
        conn.call(|conn| -> Result<(), tokio_rusqlite::rusqlite::Error> {
            conn.execute_batch(
                "CREATE TABLE test_items (
                     id    INTEGER PRIMARY KEY,
                     name  TEXT    NOT NULL,
                     value INTEGER NOT NULL
                 );
                 INSERT INTO test_items (id, name, value) VALUES (1, 'alice', 10);
                 INSERT INTO test_items (id, name, value) VALUES (2, 'bob',   20);
                 INSERT INTO test_items (id, name, value) VALUES (3, 'carol', 30);",
            )?;
            Ok(())
        })
        .await
        .expect("seed table");
        conn.close().await.expect("close seed connection");

        path
    }

    /// Register `test_items` from the given db path with `read_write` mode.
    async fn register_test_table(ctx: &mut SessionContext, db_path: &str) {
        let mut options = HashMap::new();
        options.insert("table".to_string(), "test_items".to_string());
        register_sqlite_tables(ctx, "test_items", db_path, Some(&options), true)
            .await
            .expect("register sqlite table");
    }

    /// Collect a SELECT query and return all batches.
    async fn query_all(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        let df = ctx.sql(sql).await.expect("parse sql");
        df.collect().await.expect("collect results")
    }

    /// Total row count across batches.
    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    // ─── Insert test ────────────────────────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_insert_into() {
        let db_path = create_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_test_table(&mut ctx, db).await;

        // Insert a new row via DataFusion SQL
        ctx.sql("INSERT INTO test_items (id, name, value) VALUES (4, 'dave', 40)")
            .await
            .expect("parse insert")
            .collect()
            .await
            .expect("execute insert");

        // Verify the new row exists
        let batches = query_all(&ctx, "SELECT id, name, value FROM test_items ORDER BY id").await;
        assert_eq!(total_rows(&batches), 4);

        // Verify the last row's values
        let last_batch = &batches[batches.len() - 1];
        let ids = last_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(ids.values().iter().any(|&v| v == 4), "id 4 should exist");
    }

    // ─── Delete tests ───────────────────────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_delete_with_filter() {
        let db_path = create_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_test_table(&mut ctx, db).await;

        // Delete rows where id > 1
        ctx.sql("DELETE FROM test_items WHERE id > 1")
            .await
            .expect("parse delete")
            .collect()
            .await
            .expect("execute delete");

        // Only alice (id=1) should remain
        let batches = query_all(&ctx, "SELECT id, name FROM test_items").await;
        assert_eq!(total_rows(&batches), 1);

        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
    }

    #[tokio::test]
    #[ignore]
    async fn test_delete_all_rows() {
        let db_path = create_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_test_table(&mut ctx, db).await;

        // Delete without a WHERE clause removes everything
        ctx.sql("DELETE FROM test_items")
            .await
            .expect("parse delete all")
            .collect()
            .await
            .expect("execute delete all");

        let batches = query_all(&ctx, "SELECT id FROM test_items").await;
        assert_eq!(total_rows(&batches), 0);
    }

    #[tokio::test]
    #[ignore]
    async fn test_delete_no_matching_rows() {
        let db_path = create_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_test_table(&mut ctx, db).await;

        // Delete with a filter that matches nothing
        ctx.sql("DELETE FROM test_items WHERE id = 999")
            .await
            .expect("parse delete")
            .collect()
            .await
            .expect("execute delete");

        // All 3 original rows should still be there
        let batches = query_all(&ctx, "SELECT id FROM test_items").await;
        assert_eq!(total_rows(&batches), 3);
    }

    // ─── Update tests ───────────────────────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_update_single_column_with_filter() {
        let db_path = create_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_test_table(&mut ctx, db).await;

        // Double the value for bob (id=2)
        ctx.sql("UPDATE test_items SET value = 200 WHERE id = 2")
            .await
            .expect("parse update")
            .collect()
            .await
            .expect("execute update");

        let batches = query_all(&ctx, "SELECT value FROM test_items WHERE id = 2").await;
        assert_eq!(total_rows(&batches), 1);

        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 200);
    }

    #[tokio::test]
    #[ignore]
    async fn test_update_multiple_columns() {
        let db_path = create_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_test_table(&mut ctx, db).await;

        // Update both name and value for carol
        ctx.sql("UPDATE test_items SET name = 'charlie', value = 300 WHERE id = 3")
            .await
            .expect("parse update")
            .collect()
            .await
            .expect("execute update");

        let batches = query_all(&ctx, "SELECT name, value FROM test_items WHERE id = 3").await;
        assert_eq!(total_rows(&batches), 1);

        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let values = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(names.value(0), "charlie");
        assert_eq!(values.value(0), 300);
    }

    #[tokio::test]
    #[ignore]
    async fn test_update_all_rows() {
        let db_path = create_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_test_table(&mut ctx, db).await;

        // Set every row's value to 0
        ctx.sql("UPDATE test_items SET value = 0")
            .await
            .expect("parse update all")
            .collect()
            .await
            .expect("execute update all");

        let batches = query_all(&ctx, "SELECT value FROM test_items").await;
        assert_eq!(total_rows(&batches), 3);

        for batch in &batches {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..values.len() {
                assert_eq!(values.value(i), 0);
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_update_no_matching_rows() {
        let db_path = create_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_test_table(&mut ctx, db).await;

        // Update with a filter that matches nothing
        ctx.sql("UPDATE test_items SET value = 999 WHERE id = 999")
            .await
            .expect("parse update")
            .collect()
            .await
            .expect("execute update");

        // All original values should be unchanged
        let batches = query_all(&ctx, "SELECT value FROM test_items ORDER BY id").await;
        assert_eq!(total_rows(&batches), 3);

        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 10);
        assert_eq!(values.value(1), 20);
        assert_eq!(values.value(2), 30);
    }

    // ─── Combined DML test ──────────────────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_insert_update_delete_round_trip() {
        let db_path = create_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_test_table(&mut ctx, db).await;

        // Start: 3 rows (alice=10, bob=20, carol=30)

        // 1. Insert a new row
        ctx.sql("INSERT INTO test_items (id, name, value) VALUES (4, 'dave', 40)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batches = query_all(&ctx, "SELECT id FROM test_items").await;
        assert_eq!(total_rows(&batches), 4);

        // 2. Update dave's value
        ctx.sql("UPDATE test_items SET value = 44 WHERE id = 4")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batches = query_all(&ctx, "SELECT value FROM test_items WHERE id = 4").await;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 44);

        // 3. Delete alice and bob
        ctx.sql("DELETE FROM test_items WHERE id <= 2")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batches = query_all(&ctx, "SELECT id FROM test_items ORDER BY id").await;
        assert_eq!(total_rows(&batches), 2);

        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 3); // carol
        assert_eq!(ids.value(1), 4); // dave
    }
}
