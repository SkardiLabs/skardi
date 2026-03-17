use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Constraints;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, dml::InsertOp};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::SqliteDialect;
use futures::{StreamExt, stream};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_rusqlite::Connection;

/// Default number of read connections in the pool.
const DEFAULT_READ_POOL_SIZE: usize = 4;

/// Create a read-only SQLite table provider for a single table.
pub async fn create_sqlite_table_provider(
    db_path: &str,
    table_name: &str,
) -> Result<Arc<dyn TableProvider>> {
    let table_reference = TableReference::bare(table_name);
    let provider = SqliteTableProvider::new(
        db_path,
        table_reference,
        5000,
        DEFAULT_READ_POOL_SIZE,
        false,
    )
    .await?;
    Ok(Arc::new(provider))
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
/// * `read_pool_size` - Number of read connections (optional, defaults to 4)
pub async fn register_sqlite_tables(
    session_ctx: &mut SessionContext,
    name: &str,
    db_path: &str,
    options: Option<&HashMap<String, String>>,
    read_write: bool,
) -> Result<()> {
    let mode_str = if read_write {
        "read-write"
    } else {
        "read-only"
    };
    tracing::info!(
        "Registering SQLite table: {} with path: {} ({})",
        name,
        db_path,
        mode_str
    );
    tracing::debug!("Options: {:?}", options);

    let table_name = options
        .and_then(|opts| opts.get("table"))
        .ok_or_else(|| anyhow::anyhow!("SQLite data source '{}' requires 'table' option", name))?;

    let busy_timeout_ms: u64 = options
        .and_then(|opts| opts.get("busy_timeout_ms"))
        .and_then(|v| v.parse().ok())
        .unwrap_or(5000);

    let read_pool_size: usize = options
        .and_then(|opts| opts.get("read_pool_size"))
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_READ_POOL_SIZE);

    tracing::debug!(
        "Connecting to SQLite table: {} in database '{}' as '{}'",
        table_name,
        db_path,
        name
    );

    let table_reference = TableReference::bare(table_name.as_str());

    let provider = SqliteTableProvider::new(
        db_path,
        table_reference.clone(),
        busy_timeout_ms,
        read_pool_size,
        read_write,
    )
    .await
    .with_context(|| {
        format!(
            "Failed to create SQLite table provider for '{}'",
            table_name
        )
    })?;

    session_ctx
        .register_table(name, Arc::new(provider))
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

// ─── SqliteTableProvider ─────────────────────────────────────────────────────

/// A custom SQLite table provider using tokio-rusqlite directly.
///
/// Uses a pool of read connections (round-robin) for concurrent scans and
/// a single write connection for INSERT/UPDATE/DELETE. All connections
/// enable WAL mode for concurrent reader support.
struct SqliteTableProvider {
    /// Pool of read connections for concurrent scans.
    read_pool: Vec<Arc<Connection>>,
    /// Round-robin counter for read connection selection.
    read_pool_idx: AtomicUsize,
    /// Single write connection (None if read-only).
    write_conn: Option<Arc<Connection>>,
    /// Table reference for SQL generation.
    table_reference: TableReference,
    /// Schema derived from PRAGMA table_info().
    schema: SchemaRef,
    /// Whether this provider supports writes.
    read_write: bool,
}

impl fmt::Debug for SqliteTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteTableProvider")
            .field("table_reference", &self.table_reference)
            .field("read_pool_size", &self.read_pool.len())
            .field("read_write", &self.read_write)
            .finish()
    }
}

impl SqliteTableProvider {
    /// Open connections, enable WAL mode, read schema from PRAGMA table_info().
    async fn new(
        db_path: &str,
        table_reference: TableReference,
        busy_timeout_ms: u64,
        read_pool_size: usize,
        read_write: bool,
    ) -> Result<Self> {
        let pool_size = read_pool_size.max(1);

        // Open read connections
        let mut read_pool = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let conn = Connection::open(db_path)
                .await
                .with_context(|| format!("Failed to open SQLite read connection: {}", db_path))?;
            init_connection(&conn, busy_timeout_ms).await?;
            read_pool.push(Arc::new(conn));
        }

        // Open write connection if read-write
        let write_conn = if read_write {
            let conn = Connection::open(db_path)
                .await
                .with_context(|| format!("Failed to open SQLite write connection: {}", db_path))?;
            init_connection(&conn, busy_timeout_ms).await?;
            Some(Arc::new(conn))
        } else {
            None
        };

        // Read schema from any read connection
        let schema = read_schema_from_pragma(&read_pool[0], table_reference.table()).await?;

        if schema.fields().is_empty() {
            tracing::warn!(
                "PRAGMA table_info returned empty schema for '{}' — table may not exist",
                table_reference.table()
            );
        }

        Ok(Self {
            read_pool,
            read_pool_idx: AtomicUsize::new(0),
            write_conn,
            table_reference,
            schema,
            read_write,
        })
    }

    /// Pick the next read connection via round-robin.
    fn next_read_conn(&self) -> Arc<Connection> {
        let idx = self.read_pool_idx.fetch_add(1, Ordering::Relaxed) % self.read_pool.len();
        Arc::clone(&self.read_pool[idx])
    }

    /// Get the write connection, or return an error if read-only.
    fn write_conn(&self) -> DataFusionResult<Arc<Connection>> {
        self.write_conn.as_ref().cloned().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Table '{}' is registered as read-only",
                self.table_reference
            ))
        })
    }
}

/// Enable WAL mode and set busy timeout on a connection.
async fn init_connection(conn: &Connection, busy_timeout_ms: u64) -> Result<()> {
    let timeout = busy_timeout_ms;
    conn.call(
        move |conn| -> std::result::Result<(), tokio_rusqlite::rusqlite::Error> {
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "busy_timeout", timeout)?;
            Ok(())
        },
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to initialize SQLite connection: {}", e))
}

/// Read schema via PRAGMA table_info().
async fn read_schema_from_pragma(conn: &Connection, table_name: &str) -> Result<SchemaRef> {
    let tbl = table_name.to_string();
    let fields: Vec<Field> = conn
        .call(
            move |conn| -> std::result::Result<Vec<Field>, tokio_rusqlite::rusqlite::Error> {
                let mut stmt = conn.prepare(&format!("PRAGMA table_info(\"{}\")", tbl))?;
                let rows = stmt.query_map([], |row| {
                    let col_name: String = row.get(1)?;
                    let col_type: String = row.get(2)?;
                    let not_null: bool = row.get(3)?;
                    Ok((col_name, col_type, not_null))
                })?;
                let mut fields = Vec::new();
                for row in rows {
                    let (col_name, col_type, not_null) = row?;
                    let data_type = sqlite_type_to_arrow(&col_type);
                    fields.push(Field::new(col_name, data_type, !not_null));
                }
                Ok(fields)
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("PRAGMA table_info failed: {}", e))?;

    Ok(Arc::new(Schema::new(fields)))
}

#[async_trait]
impl TableProvider for SqliteTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        None
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let conn = self.next_read_conn();
        Ok(Arc::new(SQLiteScanExec::new(
            conn,
            self.table_reference.clone(),
            Arc::clone(&self.schema),
            projection.cloned(),
            filters.to_vec(),
            limit,
        )))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let conn = self.write_conn()?;
        Ok(Arc::new(SQLiteInsertExec::new(
            conn,
            self.table_reference.clone(),
            Arc::clone(&self.schema),
            input,
            op,
        )))
    }

    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let conn = self.write_conn()?;
        let table = quote_sqlite_table(&self.table_reference);
        let where_clause = build_sqlite_where_clause(&filters)?;
        let sql = format!("DELETE FROM {table}{where_clause}");
        Ok(Arc::new(SQLiteDmlExec::new(conn, sql)))
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

        let conn = self.write_conn()?;
        let table = quote_sqlite_table(&self.table_reference);
        let where_clause = build_sqlite_where_clause(&filters)?;
        let sql = format!("UPDATE {table} SET {set_clause}{where_clause}");
        Ok(Arc::new(SQLiteDmlExec::new(conn, sql)))
    }
}

// ─── SQLiteScanExec ──────────────────────────────────────────────────────────

/// Execution plan that reads from SQLite via `SELECT` with projection,
/// filter pushdown, and limit support.
struct SQLiteScanExec {
    conn: Arc<Connection>,
    table_reference: TableReference,
    /// Full table schema from PRAGMA.
    table_schema: SchemaRef,
    /// Output schema after projection.
    output_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl SQLiteScanExec {
    fn new(
        conn: Arc<Connection>,
        table_reference: TableReference,
        table_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
    ) -> Self {
        let output_schema = if let Some(ref proj) = projection {
            Arc::new(table_schema.project(proj).expect("valid projection"))
        } else {
            Arc::clone(&table_schema)
        };
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            conn,
            table_reference,
            table_schema,
            output_schema,
            projection,
            filters,
            limit,
            properties,
        }
    }

    /// Build the SELECT SQL string.
    fn build_sql(&self) -> DataFusionResult<String> {
        // Column list
        let columns: Vec<String> = if let Some(ref proj) = self.projection {
            proj.iter()
                .map(|&i| quote_sqlite_ident(self.table_schema.field(i).name()))
                .collect()
        } else {
            self.table_schema
                .fields()
                .iter()
                .map(|f| quote_sqlite_ident(f.name()))
                .collect()
        };

        let table = quote_sqlite_table(&self.table_reference);
        let where_clause = build_sqlite_where_clause(&self.filters)?;
        let limit_clause = self
            .limit
            .map(|n| format!(" LIMIT {n}"))
            .unwrap_or_default();

        Ok(format!(
            "SELECT {} FROM {table}{where_clause}{limit_clause}",
            columns.join(", ")
        ))
    }
}

impl fmt::Debug for SQLiteScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SQLiteScanExec(table={})", self.table_reference)
    }
}

impl DisplayAs for SQLiteScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SQLiteScanExec: table={}", self.table_reference)
    }
}

impl ExecutionPlan for SQLiteScanExec {
    fn name(&self) -> &str {
        "SQLiteScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
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
        let sql = self.build_sql()?;
        let output_schema = Arc::clone(&self.output_schema);
        let num_cols = output_schema.fields().len();
        let field_types: Vec<DataType> = output_schema
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect();

        let future = async move {
            let batch: Vec<Vec<tokio_rusqlite::rusqlite::types::Value>> = conn
                .call(
                    move |conn| -> std::result::Result<_, tokio_rusqlite::rusqlite::Error> {
                        let mut stmt = conn.prepare(&sql)?;
                        let mut col_values: Vec<Vec<tokio_rusqlite::rusqlite::types::Value>> =
                            (0..num_cols).map(|_| Vec::new()).collect();

                        let mut rows = stmt.query([])?;
                        while let Some(row) = rows.next()? {
                            for col_idx in 0..num_cols {
                                let val: tokio_rusqlite::rusqlite::types::Value =
                                    row.get_unwrap(col_idx);
                                col_values[col_idx].push(val);
                            }
                        }

                        Ok(col_values)
                    },
                )
                .await
                .map_err(|e| DataFusionError::Execution(format!("SQLite scan error: {e}")))?;

            // Convert columnar rusqlite::Value vectors into Arrow arrays
            let arrays: Vec<ArrayRef> = batch
                .into_iter()
                .zip(field_types.iter())
                .map(|(values, data_type)| sqlite_values_to_arrow(&values, data_type))
                .collect();

            RecordBatch::try_new(output_schema, arrays).map_err(DataFusionError::from)
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.output_schema),
            stream::once(future),
        )))
    }
}

/// Convert a column of `rusqlite::types::Value` into an Arrow `ArrayRef`.
fn sqlite_values_to_arrow(
    values: &[tokio_rusqlite::rusqlite::types::Value],
    data_type: &DataType,
) -> ArrayRef {
    use tokio_rusqlite::rusqlite::types::Value;

    match data_type {
        DataType::Int64 => {
            let arr: Int64Array = values
                .iter()
                .map(|v| match v {
                    Value::Integer(i) => Some(*i),
                    Value::Null => None,
                    _ => None,
                })
                .collect();
            Arc::new(arr)
        }
        DataType::Float64 => {
            let arr: Float64Array = values
                .iter()
                .map(|v| match v {
                    Value::Real(f) => Some(*f),
                    Value::Integer(i) => Some(*i as f64),
                    Value::Null => None,
                    _ => None,
                })
                .collect();
            Arc::new(arr)
        }
        DataType::Boolean => {
            let arr: BooleanArray = values
                .iter()
                .map(|v| match v {
                    Value::Integer(i) => Some(*i != 0),
                    Value::Null => None,
                    _ => None,
                })
                .collect();
            Arc::new(arr)
        }
        DataType::Binary => {
            let arr: BinaryArray = values
                .iter()
                .map(|v| match v {
                    Value::Blob(b) => Some(b.as_slice()),
                    Value::Null => None,
                    _ => None,
                })
                .collect();
            Arc::new(arr)
        }
        // Default: Utf8 (TEXT, VARCHAR, etc.)
        _ => {
            let strings: Vec<Option<String>> = values
                .iter()
                .map(|v| match v {
                    Value::Text(s) => Some(s.clone()),
                    Value::Integer(i) => Some(i.to_string()),
                    Value::Real(f) => Some(f.to_string()),
                    Value::Null => None,
                    _ => None,
                })
                .collect();
            let arr: StringArray = strings.iter().map(|v| v.as_deref()).collect();
            Arc::new(arr)
        }
    }
}

// ─── SQLiteInsertExec ────────────────────────────────────────────────────────

/// Execution plan that consumes input batches and inserts them into SQLite.
struct SQLiteInsertExec {
    conn: Arc<Connection>,
    table_reference: TableReference,
    table_schema: SchemaRef,
    input: Arc<dyn ExecutionPlan>,
    op: InsertOp,
    output_schema: SchemaRef,
    properties: PlanProperties,
}

impl SQLiteInsertExec {
    fn new(
        conn: Arc<Connection>,
        table_reference: TableReference,
        table_schema: SchemaRef,
        input: Arc<dyn ExecutionPlan>,
        op: InsertOp,
    ) -> Self {
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            conn,
            table_reference,
            table_schema,
            input,
            op,
            output_schema,
            properties,
        }
    }
}

impl fmt::Debug for SQLiteInsertExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SQLiteInsertExec(table={})", self.table_reference)
    }
}

impl DisplayAs for SQLiteInsertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SQLiteInsertExec")
    }
}

impl ExecutionPlan for SQLiteInsertExec {
    fn name(&self) -> &str {
        "SQLiteInsertExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // Require all input data to be coalesced into a single partition
        // so we can insert it in one transaction.
        vec![Distribution::SinglePartition]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "SQLiteInsertExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&self.conn),
            self.table_reference.clone(),
            Arc::clone(&self.table_schema),
            children.into_iter().next().unwrap(),
            self.op,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let conn = Arc::clone(&self.conn);
        let table_ref = self.table_reference.clone();
        let op = self.op;
        let output_schema = Arc::clone(&self.output_schema);

        let mut input_stream = self.input.execute(partition, context)?;
        // Use the input schema to determine which columns are being inserted.
        // DataFusion may provide a subset of table columns (e.g., INSERT INTO t (col1, col2) SELECT ...).
        let input_schema = self.input.schema();

        let future = async move {
            // Collect all batches first so we can move them into conn.call()
            let mut batches: Vec<RecordBatch> = Vec::new();
            while let Some(batch_result) = input_stream.next().await {
                let batch = batch_result?;
                if batch.num_rows() > 0 {
                    batches.push(batch);
                }
            }

            let total_rows: u64 = conn
                .call(
                    move |conn| -> std::result::Result<u64, tokio_rusqlite::rusqlite::Error> {
                        let tx = conn.transaction()?;

                        // Handle overwrite mode: delete all rows first
                        if matches!(op, InsertOp::Overwrite) {
                            let table = quote_sqlite_table(&table_ref);
                            tx.execute(&format!("DELETE FROM {table}"), [])?;
                        }

                        // Build the INSERT statement template using input schema columns
                        let col_names: Vec<String> = input_schema
                            .fields()
                            .iter()
                            .map(|f| quote_sqlite_ident(f.name()))
                            .collect();
                        let table = quote_sqlite_table(&table_ref);
                        let placeholders: Vec<&str> = vec!["?"; col_names.len()];
                        let insert_sql = format!(
                            "INSERT INTO {} ({}) VALUES ({})",
                            table,
                            col_names.join(", "),
                            placeholders.join(", ")
                        );

                        let mut total: u64 = 0;

                        for batch in &batches {
                            let num_rows = batch.num_rows();
                            let num_cols = batch.num_columns();

                            for row_idx in 0..num_rows {
                                let params: Vec<tokio_rusqlite::rusqlite::types::Value> = (0
                                    ..num_cols)
                                    .map(|col_idx| arrow_value_to_sqlite(batch, row_idx, col_idx))
                                    .collect();

                                let param_refs: Vec<&dyn tokio_rusqlite::rusqlite::types::ToSql> =
                                    params
                                        .iter()
                                        .map(|v| v as &dyn tokio_rusqlite::rusqlite::types::ToSql)
                                        .collect();

                                tx.execute(&insert_sql, param_refs.as_slice())?;
                                total += 1;
                            }
                        }

                        tx.commit()?;
                        Ok(total)
                    },
                )
                .await
                .map_err(|e| DataFusionError::Execution(format!("SQLite INSERT error: {e}")))?;

            let count_array = Arc::new(UInt64Array::from(vec![total_rows]));
            RecordBatch::try_new(output_schema, vec![count_array]).map_err(DataFusionError::from)
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.output_schema),
            stream::once(future),
        )))
    }
}

/// Extract a single cell from a RecordBatch as a `rusqlite::types::Value`.
fn arrow_value_to_sqlite(
    batch: &RecordBatch,
    row: usize,
    col: usize,
) -> tokio_rusqlite::rusqlite::types::Value {
    use arrow::array::{Array, AsArray};
    use tokio_rusqlite::rusqlite::types::Value;

    let array = batch.column(col);
    if array.is_null(row) {
        return Value::Null;
    }

    match array.data_type() {
        DataType::Int8 => Value::Integer(
            array
                .as_primitive::<arrow::datatypes::Int8Type>()
                .value(row) as i64,
        ),
        DataType::Int16 => Value::Integer(
            array
                .as_primitive::<arrow::datatypes::Int16Type>()
                .value(row) as i64,
        ),
        DataType::Int32 => Value::Integer(
            array
                .as_primitive::<arrow::datatypes::Int32Type>()
                .value(row) as i64,
        ),
        DataType::Int64 => Value::Integer(
            array
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(row),
        ),
        DataType::UInt8 => Value::Integer(
            array
                .as_primitive::<arrow::datatypes::UInt8Type>()
                .value(row) as i64,
        ),
        DataType::UInt16 => Value::Integer(
            array
                .as_primitive::<arrow::datatypes::UInt16Type>()
                .value(row) as i64,
        ),
        DataType::UInt32 => Value::Integer(
            array
                .as_primitive::<arrow::datatypes::UInt32Type>()
                .value(row) as i64,
        ),
        DataType::UInt64 => Value::Integer(
            array
                .as_primitive::<arrow::datatypes::UInt64Type>()
                .value(row) as i64,
        ),
        DataType::Float16 => Value::Real(
            array
                .as_primitive::<arrow::datatypes::Float16Type>()
                .value(row)
                .to_f64(),
        ),
        DataType::Float32 => Value::Real(
            array
                .as_primitive::<arrow::datatypes::Float32Type>()
                .value(row) as f64,
        ),
        DataType::Float64 => Value::Real(
            array
                .as_primitive::<arrow::datatypes::Float64Type>()
                .value(row),
        ),
        DataType::Boolean => Value::Integer(if array.as_boolean().value(row) { 1 } else { 0 }),
        DataType::Utf8 => Value::Text(array.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => Value::Text(array.as_string::<i64>().value(row).to_string()),
        DataType::Binary => Value::Blob(array.as_binary::<i32>().value(row).to_vec()),
        DataType::LargeBinary => Value::Blob(array.as_binary::<i64>().value(row).to_vec()),
        _ => Value::Text(format!("{:?}", array.as_ref())),
    }
}

// ─── SQLiteDmlExec (DELETE / UPDATE) ─────────────────────────────────────────

/// A leaf `ExecutionPlan` that executes a pre-built SQLite DML statement
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

// ─── Helper functions ────────────────────────────────────────────────────────

/// Map SQLite type affinity strings to Arrow DataTypes.
fn sqlite_type_to_arrow(sqlite_type: &str) -> DataType {
    let upper = sqlite_type.to_uppercase();
    if upper.contains("INT") {
        DataType::Int64
    } else if upper.contains("REAL") || upper.contains("FLOAT") || upper.contains("DOUBLE") {
        DataType::Float64
    } else if upper.contains("BLOB") {
        DataType::Binary
    } else if upper.contains("BOOL") {
        DataType::Boolean
    } else {
        // TEXT, VARCHAR, CHAR, CLOB, and anything else → Utf8
        DataType::Utf8
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
    quote_sqlite_ident(tbl.table())
}

// ─── Tests ───────────────────────────────────────────────────────────────────

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

    // ─── Scan tests ─────────────────────────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_scan_all_rows() {
        let db_path = create_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_test_table(&mut ctx, db).await;

        let batches = query_all(&ctx, "SELECT id, name, value FROM test_items ORDER BY id").await;
        assert_eq!(total_rows(&batches), 3);

        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        assert_eq!(ids.value(2), 3);
    }

    #[tokio::test]
    #[ignore]
    async fn test_scan_with_projection() {
        let db_path = create_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_test_table(&mut ctx, db).await;

        let batches = query_all(&ctx, "SELECT name FROM test_items ORDER BY id").await;
        assert_eq!(total_rows(&batches), 3);
        assert_eq!(batches[0].num_columns(), 1);

        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");
    }

    #[tokio::test]
    #[ignore]
    async fn test_scan_with_filter() {
        let db_path = create_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_test_table(&mut ctx, db).await;

        let batches = query_all(&ctx, "SELECT id, name FROM test_items WHERE id = 2").await;
        assert_eq!(total_rows(&batches), 1);

        let names = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "bob");
    }

    #[tokio::test]
    #[ignore]
    async fn test_scan_with_limit() {
        let db_path = create_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_test_table(&mut ctx, db).await;

        let batches = query_all(&ctx, "SELECT id FROM test_items LIMIT 2").await;
        assert_eq!(total_rows(&batches), 2);
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

        ctx.sql("DELETE FROM test_items WHERE id > 1")
            .await
            .expect("parse delete")
            .collect()
            .await
            .expect("execute delete");

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

        ctx.sql("DELETE FROM test_items WHERE id = 999")
            .await
            .expect("parse delete")
            .collect()
            .await
            .expect("execute delete");

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
            .downcast_ref::<StringArray>()
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

        ctx.sql("UPDATE test_items SET value = 999 WHERE id = 999")
            .await
            .expect("parse update")
            .collect()
            .await
            .expect("execute update");

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

    // ─── Multi-table helper ────────────────────────────────────────────

    /// Create a temp SQLite file with `users`, `orders`, and `user_order_stats`
    /// tables, matching the demo schema for federated query testing.
    async fn create_multi_table_db() -> tempfile::TempPath {
        let tmp = tempfile::NamedTempFile::new().expect("create temp file");
        let path = tmp.into_temp_path();
        let db_path = path.to_str().unwrap().to_string();

        let conn = Connection::open(&db_path).await.expect("open temp sqlite");
        conn.call(|conn| -> Result<(), tokio_rusqlite::rusqlite::Error> {
            conn.execute_batch(
                "CREATE TABLE users (
                     id    INTEGER PRIMARY KEY AUTOINCREMENT,
                     name  TEXT NOT NULL,
                     email TEXT UNIQUE NOT NULL
                 );
                 CREATE TABLE orders (
                     id       INTEGER PRIMARY KEY AUTOINCREMENT,
                     user_id  INTEGER NOT NULL,
                     product  TEXT NOT NULL,
                     amount   REAL NOT NULL
                 );
                 CREATE TABLE user_order_stats (
                     id              INTEGER PRIMARY KEY AUTOINCREMENT,
                     user_id         INTEGER NOT NULL UNIQUE,
                     user_name       TEXT NOT NULL,
                     user_email      TEXT NOT NULL,
                     total_orders    INTEGER NOT NULL,
                     total_spent     REAL NOT NULL,
                     last_order_date TEXT
                 );
                 INSERT INTO users (name, email) VALUES
                     ('Alice Smith', 'alice@example.com'),
                     ('Bob Johnson', 'bob@example.com');
                 INSERT INTO orders (user_id, product, amount) VALUES
                     (1, 'Laptop', 999.99),
                     (1, 'Mouse', 29.99),
                     (2, 'Keyboard', 79.99);
",
            )?;
            Ok(())
        })
        .await
        .expect("seed multi-table db");
        conn.close().await.expect("close seed connection");

        path
    }

    /// Register multiple tables from the same DB path with the given session context.
    async fn register_multi_tables(ctx: &mut SessionContext, db_path: &str) {
        for (reg_name, table_name) in [
            ("users", "users"),
            ("orders", "orders"),
            ("user_order_stats", "user_order_stats"),
        ] {
            let mut options = HashMap::new();
            options.insert("table".to_string(), table_name.to_string());
            register_sqlite_tables(ctx, reg_name, db_path, Some(&options), true)
                .await
                .unwrap_or_else(|e| panic!("register {} failed: {}", reg_name, e));
        }
    }

    // ─── INSERT INTO ... SELECT tests ──────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_empty_table_schema_from_pragma() {
        let db_path = create_multi_table_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_multi_tables(&mut ctx, db).await;

        let catalog = ctx.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();

        // user_order_stats is empty but should still have schema from PRAGMA
        let table = schema.table("user_order_stats").await.unwrap().unwrap();
        let table_schema = table.schema();
        let fields: Vec<&str> = table_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            fields,
            vec![
                "id",
                "user_id",
                "user_name",
                "user_email",
                "total_orders",
                "total_spent",
                "last_order_date"
            ]
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_insert_select_from_same_db() {
        let db_path = create_multi_table_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_multi_tables(&mut ctx, db).await;

        // INSERT INTO ... SELECT (aggregate orders into user_order_stats)
        ctx.sql(
            "INSERT INTO user_order_stats (user_id, user_name, user_email, total_orders, total_spent, last_order_date)
             SELECT
               u.id,
               u.name,
               u.email,
               CAST(COUNT(o.id) AS BIGINT),
               CAST(SUM(o.amount) AS DOUBLE),
               CAST('N/A' AS TEXT)
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

        // Verify the aggregated row exists
        let batches = query_all(
            &ctx,
            "SELECT user_id, user_name, total_orders, total_spent FROM user_order_stats",
        )
        .await;
        assert_eq!(total_rows(&batches), 1);

        let user_ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(user_ids.value(0), 1); // Alice's id
    }

    #[tokio::test]
    #[ignore]
    async fn test_insert_select_multiple_users() {
        let db_path = create_multi_table_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_multi_tables(&mut ctx, db).await;

        ctx.sql(
            "INSERT INTO user_order_stats (user_id, user_name, user_email, total_orders, total_spent, last_order_date)
             SELECT
               u.id,
               u.name,
               u.email,
               CAST(COUNT(o.id) AS BIGINT),
               CAST(SUM(o.amount) AS DOUBLE),
               CAST('N/A' AS TEXT)
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
            "SELECT user_id, user_name, total_orders FROM user_order_stats ORDER BY user_id",
        )
        .await;
        assert_eq!(total_rows(&batches), 2);
    }

    #[tokio::test]
    #[ignore]
    async fn test_schema_visibility_across_tables() {
        let db_path = create_multi_table_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_multi_tables(&mut ctx, db).await;

        let batches = query_all(&ctx, "SELECT id, name, email FROM users ORDER BY id").await;
        assert_eq!(total_rows(&batches), 2);

        let batches = query_all(
            &ctx,
            "SELECT id, user_id, product, amount FROM orders ORDER BY id",
        )
        .await;
        assert_eq!(total_rows(&batches), 3);

        // Verify JOIN works across tables
        let batches = query_all(
            &ctx,
            "SELECT u.name, o.product, o.amount
             FROM users u
             INNER JOIN orders o ON u.id = o.user_id
             ORDER BY o.id",
        )
        .await;
        assert_eq!(total_rows(&batches), 3);
    }
}
