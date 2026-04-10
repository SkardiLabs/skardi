use crate::sources::providers::DatasetRegistry;
use anyhow::{Context, Result};
use arrow::array::{RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, Session};
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
use datafusion_table_providers::sql::arrow_sql_gen::statement::InsertBuilder;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use futures::{StreamExt, stream};
use secrecy::SecretString;
use sqlx::PgPool;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// Register PostgreSQL tables or a whole database (catalog) into a DataFusion [`SessionContext`].
///
/// Single-table mode reuses datafusion-table-providers' `SqlTable` for reads and sqlx for writes
/// (INSERT/UPDATE/DELETE), fixing issues with auto-generated columns. Catalog mode registers a
/// [`MemoryCatalogProvider`] with one provider per table; read-write uses one shared sqlx pool
/// cloned per table wrapper.
///
/// # Arguments
/// * `session_ctx` - DataFusion session context to register tables into
/// * `name` - Name to register the table as
/// * `connection_string` - PostgreSQL connection string (e.g., "postgresql://host:port/db")
///   Note: Username and password should NOT be included in the connection string.
///   Use `user_env` and `pass_env` options instead.
/// * `options` - Optional configuration (e.g., table name, schema)
/// * `read_write` - If true, register as read-write table provider (allows INSERT/UPDATE/DELETE)
/// * `hierarchy_level` - `None` → table mode (default); `Some(HierarchyLevel::Catalog)` loads the whole DB as a catalog
///
/// # Options
/// * `table` - Table name (required in table mode, the default when `hierarchy_level` is `None`)
/// * `schema` - Schema name (default: "public")
/// * `allowed_schemas` - Comma-separated schema allow-list (catalog mode only)
/// * `user_env` - Environment variable name for username (optional)
/// * `pass_env` - Environment variable name for password (optional)
pub async fn register_postgres_tables(
    session_ctx: &mut SessionContext,
    name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
    read_write: bool,
    pg_knn_registry: Option<&DatasetRegistry>,
    hierarchy_level: Option<crate::sources::HierarchyLevel>,
) -> Result<()> {
    let hierarchy_level = hierarchy_level.unwrap_or_default();
    match hierarchy_level {
        crate::sources::HierarchyLevel::Catalog => {
            register_postgres_catalog(session_ctx, name, connection_string, options, read_write)
                .await
        }
        crate::sources::HierarchyLevel::Table => {
            register_single_postgres_table(
                session_ctx,
                name,
                connection_string,
                options,
                read_write,
            )
            .await
        }
    }
}

/// Register one logical table (`options.table`) under `name` in the default catalog.
async fn register_single_postgres_table(
    session_ctx: &mut SessionContext,
    name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
    read_write: bool,
) -> Result<()> {
    let mode_str = if read_write {
        "read-write"
    } else {
        "read-only"
    };
    tracing::info!(
        "Registering PostgreSQL table (sqlx): {} with connection: {} ({})",
        name,
        connection_string,
        mode_str,
    );

    let schema_name = options
        .and_then(|opts| opts.get("schema"))
        .unwrap_or(&"public".to_string())
        .clone();
    let table_name = options.and_then(|opts| opts.get("table")).ok_or_else(|| {
        anyhow::anyhow!("PostgreSQL single-table registration requires 'table' option")
    })?;

    let pool_params = parse_connection_params(connection_string, options)?;
    let read_pool = Arc::new(
        PostgresConnectionPool::new(pool_params)
            .await
            .with_context(|| {
                format!("Failed to create PostgreSQL connection pool for '{}'", name)
            })?,
    );
    let factory = PostgresTableFactory::new(Arc::clone(&read_pool));
    let table_reference = TableReference::partial(schema_name.as_str(), table_name.as_str());
    let table_provider = build_postgres_table_provider(
        &factory,
        connection_string,
        options,
        table_reference,
        read_write,
        None,
    )
    .await?;

    session_ctx
        .register_table(name, table_provider)
        .with_context(|| format!("Failed to register table '{}' with DataFusion", name))?;

    tracing::info!(
        "Successfully registered PostgreSQL table '{}.{}' as '{}' ({})",
        schema_name,
        table_name,
        name,
        mode_str,
    );

    // Register in the dataset registry so pg_knn() and pg_fts() UDTFs can find this table.
    // Reuse the sqlx_pool and columns already fetched above.
    if let Some(registry) = pg_knn_registry {
        let qualified_table = format!(
            "\"{}\".\"{}\"",
            schema_name.replace('"', "\"\""),
            table_name.replace('"', "\"\"")
        );

        let entry = PgKnnEntry {
            pool: Arc::new(sqlx_pool_for_knn),
            qualified_table,
            columns,
        };

        registry
            .write()
            .map_err(|e| anyhow::anyhow!("pg_knn registry lock poisoned: {}", e))?
            .insert(name.to_string(), DatasetEntry::Postgres(entry));

        tracing::info!(
            "Registered '{}' in dataset registry for pg_knn/pg_fts",
            name
        );
    }

    Ok(())
}

/// Register an entire Postgres database (catalog) as a named DataFusion catalog.
async fn register_postgres_catalog(
    session_ctx: &mut SessionContext,
    catalog_name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
    read_write: bool,
) -> Result<()> {
    let mode_str = if read_write {
        "read-write"
    } else {
        "read-only"
    };
    tracing::info!(
        "Registering PostgreSQL catalog (sqlx): {} with connection: {} ({})",
        catalog_name,
        connection_string,
        mode_str,
    );

    let pool_params = parse_connection_params(connection_string, options)?;
    let read_pool = Arc::new(
        PostgresConnectionPool::new(pool_params)
            .await
            .with_context(|| {
                format!(
                    "Failed to create PostgreSQL connection pool for catalog '{}'",
                    catalog_name
                )
            })?,
    );

    // Build the sqlx pool unconditionally: it is needed for table discovery in
    // both read-only and read-write modes, and reused for writes when read_write=true.
    let sqlx_url = build_sqlx_connection_url(connection_string, options)?;
    let sqlx_pool = PgPool::connect(&sqlx_url).await.with_context(|| {
        format!(
            "Failed to create sqlx PgPool for catalog '{}'",
            catalog_name
        )
    })?;

    let allowed_schemas = parse_allowed_schemas(options);
    let schema_tables = list_postgres_tables_in_catalog(&sqlx_pool, allowed_schemas.as_ref())
        .await
        .with_context(|| {
            format!(
                "Failed to list PostgreSQL tables for catalog-wide registration in source '{}'",
                catalog_name
            )
        })?;

    if schema_tables.is_empty() {
        tracing::warn!(
            "No tables found in PostgreSQL catalog for source '{}'",
            catalog_name
        );
    }

    let write_pool = if read_write { Some(&sqlx_pool) } else { None };

    let factory = PostgresTableFactory::new(Arc::clone(&read_pool));
    let catalog_provider = Arc::new(MemoryCatalogProvider::new());

    for (schema, table_name) in &schema_tables {
        let table_reference = TableReference::partial(schema.as_str(), table_name.as_str());
        let table_provider = build_postgres_table_provider(
            &factory,
            connection_string,
            options,
            table_reference,
            read_write,
            write_pool,
        )
        .await
        .with_context(|| {
            format!(
                "Failed to build table provider for '{}.{}' in catalog '{}'",
                schema, table_name, catalog_name
            )
        })?;

        if catalog_provider.schema(schema).is_none() {
            catalog_provider
                .register_schema(schema, Arc::new(MemorySchemaProvider::new()))
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to register schema '{}' for catalog '{}': {}",
                        schema,
                        catalog_name,
                        e
                    )
                })?;
        }

        let schema_provider = catalog_provider.schema(schema).ok_or_else(|| {
            anyhow::anyhow!(
                "Schema '{}' was not found after registration in catalog '{}'",
                schema,
                catalog_name
            )
        })?;

        schema_provider
            .register_table(table_name.to_string(), table_provider)
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to register table '{}.{}' in catalog '{}': {}",
                    schema,
                    table_name,
                    catalog_name,
                    e
                )
            })?;

        tracing::debug!(
            "Prepared '{}.{}' in catalog '{}'",
            schema,
            table_name,
            catalog_name
        );
    }

    session_ctx.register_catalog(catalog_name, catalog_provider);
    tracing::info!(
        "Registered PostgreSQL catalog '{}' with {} table(s) ({})",
        catalog_name,
        schema_tables.len(),
        mode_str
    );

    Ok(())
}

async fn build_postgres_table_provider(
    factory: &PostgresTableFactory,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
    table_reference: TableReference,
    read_write: bool,
    reuse_sqlx_pool: Option<&PgPool>,
) -> Result<Arc<dyn TableProvider>> {
    let read_provider: Arc<dyn TableProvider> = factory
        .table_provider(table_reference.clone())
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to create read table provider for '{}': {}",
                table_reference,
                e
            )
        })?;

    if !read_write {
        return Ok(read_provider);
    }

    let schema_name = table_reference
        .schema()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Table reference '{}' must include a schema for read-write Postgres registration",
                table_reference
            )
        })?
        .to_string();
    let table_name = table_reference.table().to_string();

    let sqlx_pool = if let Some(pool) = reuse_sqlx_pool {
        pool.clone()
    } else {
        let sqlx_url = build_sqlx_connection_url(connection_string, options)?;
        PgPool::connect(&sqlx_url).await.with_context(|| {
            format!(
                "Failed to create sqlx PgPool for '{}.{}'",
                schema_name, table_name
            )
        })?
    };

    let auto_generated_columns =
        detect_auto_generated_columns(&sqlx_pool, &schema_name, &table_name).await?;

    if !auto_generated_columns.is_empty() {
        tracing::debug!(
            "Detected auto-generated columns for '{}.{}': {:?}",
            schema_name,
            table_name,
            auto_generated_columns
        );
    }

    Ok(Arc::new(SqlxPostgresTableProvider {
        read_provider,
        sqlx_pool,
        table_reference,
        auto_generated_columns,
    }))
}

async fn list_postgres_tables_in_catalog(
    pool: &PgPool,
    allowed_schemas: Option<&Vec<String>>,
) -> Result<Vec<(String, String)>> {
    let mut rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT table_schema, table_name
         FROM information_schema.tables
         WHERE table_type = 'BASE TABLE'
           AND table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
           AND table_schema NOT LIKE 'pg_temp_%'
         ORDER BY table_schema, table_name",
    )
    .fetch_all(pool)
    .await?;
    if let Some(allowed) = allowed_schemas {
        rows.retain(|(schema, _)| allowed.iter().any(|s| s == schema));
    }
    Ok(rows)
}

fn parse_allowed_schemas(options: Option<&HashMap<String, String>>) -> Option<Vec<String>> {
    let value = options.and_then(|opts| opts.get("allowed_schemas"))?;
    let values = value
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

// ─── SqlxPostgresTableProvider ──────────────────────────────────────────────

/// A read-write PostgreSQL table provider that delegates reads to
/// datafusion-table-providers' `SqlTable` and uses sqlx for writes.
///
/// This fixes the auto-generated column bug (datafusion-table-providers#538)
/// by excluding auto-generated columns from INSERT statements.
#[derive(Debug)]
struct SqlxPostgresTableProvider {
    /// Read provider from datafusion-table-providers (SqlTable or federated)
    read_provider: Arc<dyn TableProvider>,
    /// sqlx connection pool for write operations
    sqlx_pool: PgPool,
    /// Fully qualified table reference (schema.table)
    table_reference: TableReference,
    /// Column names that are auto-generated (SERIAL, IDENTITY, sequences)
    auto_generated_columns: Vec<String>,
}

#[async_trait]
impl TableProvider for SqlxPostgresTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.read_provider.schema()
    }

    fn table_type(&self) -> TableType {
        self.read_provider.table_type()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.read_provider.constraints()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.read_provider
            .scan(state, projection, filters, limit)
            .await
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SqlxPostgresInsertExec::new(
            self.sqlx_pool.clone(),
            self.table_reference.clone(),
            self.auto_generated_columns.clone(),
            input,
            op,
        )))
    }

    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let table = self.table_reference.to_quoted_string();
        let where_clause = build_where_clause(&filters)?;
        let sql = format!("DELETE FROM {table}{where_clause}");
        Ok(Arc::new(SqlxPostgresDmlExec::new(
            self.sqlx_pool.clone(),
            sql,
        )))
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
        Ok(Arc::new(SqlxPostgresDmlExec::new(
            self.sqlx_pool.clone(),
            sql,
        )))
    }
}

// ─── INSERT execution plan ──────────────────────────────────────────────────

/// An `ExecutionPlan` that collects input record batches and executes INSERT
/// statements via sqlx, excluding auto-generated columns.
struct SqlxPostgresInsertExec {
    sqlx_pool: PgPool,
    table_reference: TableReference,
    auto_generated_columns: Vec<String>,
    input: Arc<dyn ExecutionPlan>,
    op: InsertOp,
    /// Output schema: single `count` column
    output_schema: SchemaRef,
    properties: PlanProperties,
}

impl SqlxPostgresInsertExec {
    fn new(
        sqlx_pool: PgPool,
        table_reference: TableReference,
        auto_generated_columns: Vec<String>,
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
            sqlx_pool,
            table_reference,
            auto_generated_columns,
            input,
            op,
            output_schema,
            properties,
        }
    }
}

impl fmt::Debug for SqlxPostgresInsertExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SqlxPostgresInsertExec(table={})", self.table_reference)
    }
}

impl DisplayAs for SqlxPostgresInsertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SqlxPostgresInsertExec")
    }
}

impl ExecutionPlan for SqlxPostgresInsertExec {
    fn name(&self) -> &str {
        "SqlxPostgresInsertExec"
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

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "SqlxPostgresInsertExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            self.sqlx_pool.clone(),
            self.table_reference.clone(),
            self.auto_generated_columns.clone(),
            children.into_iter().next().unwrap(),
            self.op,
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let pool = self.sqlx_pool.clone();
        let table_ref = self.table_reference.clone();
        let auto_gen_cols = self.auto_generated_columns.clone();
        let op = self.op;
        let output_schema = Arc::clone(&self.output_schema);

        // Collect from all input partitions to handle multi-partition plans
        let input_partitions = self.input.properties().partitioning.partition_count();
        let mut input_streams: Vec<SendableRecordBatchStream> = Vec::new();
        for p in 0..input_partitions {
            input_streams.push(self.input.execute(p, Arc::clone(&context))?);
        }

        let future = async move {
            let mut total_rows: u64 = 0;

            // Begin transaction
            let mut tx = pool.begin().await.map_err(|e| {
                DataFusionError::Execution(format!("Failed to begin transaction: {e}"))
            })?;

            // Handle overwrite mode: truncate table first
            if matches!(op, InsertOp::Overwrite) {
                let delete_sql = format!("DELETE FROM {}", table_ref.to_quoted_string());
                sqlx::query(&delete_sql)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to delete existing data for overwrite: {e}"
                        ))
                    })?;
            }

            // Collect and insert batches from all input partitions
            for mut input_stream in input_streams {
                while let Some(batch_result) = input_stream.next().await {
                    let batch = batch_result?;
                    if batch.num_rows() == 0 {
                        continue;
                    }

                    // Project out auto-generated columns
                    let filtered_batch = filter_batch_columns(&batch, &auto_gen_cols)?;
                    let num_rows = filtered_batch.num_rows() as u64;

                    // Build INSERT SQL using InsertBuilder from datafusion-table-providers
                    let batches = vec![filtered_batch];
                    let insert_sql = InsertBuilder::new(&table_ref, &batches)
                        .build_postgres(None)
                        .map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to build INSERT statement: {e}"
                            ))
                        })?;

                    // Execute via sqlx within the transaction
                    sqlx::query(&insert_sql)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| {
                            DataFusionError::Execution(format!("Failed to execute INSERT: {e}"))
                        })?;

                    total_rows += num_rows;
                }
            }

            // Commit transaction
            tx.commit().await.map_err(|e| {
                DataFusionError::Execution(format!("Failed to commit transaction: {e}"))
            })?;

            let count_array = Arc::new(UInt64Array::from(vec![total_rows]));
            RecordBatch::try_new(output_schema, vec![count_array]).map_err(DataFusionError::from)
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.output_schema),
            stream::once(future),
        )))
    }
}

/// Project out auto-generated columns from a RecordBatch.
fn filter_batch_columns(
    batch: &RecordBatch,
    auto_generated_columns: &[String],
) -> DataFusionResult<RecordBatch> {
    if auto_generated_columns.is_empty() {
        return Ok(batch.clone());
    }

    let schema = batch.schema();
    let indices: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| !auto_generated_columns.contains(f.name()))
        .map(|(i, _)| i)
        .collect();

    batch.project(&indices).map_err(DataFusionError::from)
}

// ─── DML execution plan (DELETE / UPDATE) ───────────────────────────────────

/// A leaf `ExecutionPlan` that executes a pre-built Postgres DML statement
/// via sqlx and returns a single row `{ count: u64 }` with affected rows.
struct SqlxPostgresDmlExec {
    sqlx_pool: PgPool,
    sql: String,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl SqlxPostgresDmlExec {
    fn new(sqlx_pool: PgPool, sql: String) -> Self {
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
            sqlx_pool,
            sql,
            schema,
            properties,
        }
    }
}

impl fmt::Debug for SqlxPostgresDmlExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SqlxPostgresDmlExec(sql={})", self.sql)
    }
}

impl DisplayAs for SqlxPostgresDmlExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SqlxPostgresDmlExec")
    }
}

impl ExecutionPlan for SqlxPostgresDmlExec {
    fn name(&self) -> &str {
        "SqlxPostgresDmlExec"
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
        let pool = self.sqlx_pool.clone();
        let sql = self.sql.clone();
        let schema = Arc::clone(&self.schema);

        let future = async move {
            let result = sqlx::query(&sql).execute(&pool).await.map_err(|e| {
                DataFusionError::Execution(format!("Postgres DML execute error: {e}"))
            })?;
            let rows_affected = result.rows_affected();
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

// ─── SQL helpers ────────────────────────────────────────────────────────────

/// Builds a ` WHERE expr1 AND expr2 ...` clause from DataFusion filter expressions.
/// Returns an empty string when `filters` is empty.
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

/// Quotes a PostgreSQL identifier with double quotes, escaping embedded double-quotes.
fn quote_pg_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

// ─── Connection helpers ─────────────────────────────────────────────────────

/// Build a sqlx-compatible connection URL from the same parameters used by the
/// existing provider. Credentials come from environment variables only.
fn build_sqlx_connection_url(
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
) -> Result<String> {
    let url = url::Url::parse(connection_string)
        .with_context(|| format!("Invalid PostgreSQL connection string: {connection_string}"))?;

    let host = url.host_str().unwrap_or("localhost");
    let port = url.port().unwrap_or(5432);
    let db = url.path().trim_start_matches('/');

    let user = options
        .and_then(|o| o.get("user_env"))
        .map(|env_key| {
            std::env::var(env_key).with_context(|| {
                format!("Environment variable '{env_key}' not found for PostgreSQL user")
            })
        })
        .transpose()?;

    let pass = options
        .and_then(|o| o.get("pass_env"))
        .map(|env_key| {
            std::env::var(env_key).with_context(|| {
                format!("Environment variable '{env_key}' not found for PostgreSQL password")
            })
        })
        .transpose()?;

    let mut result = String::from("postgres://");
    if let Some(u) = &user {
        result.push_str(
            &percent_encoding::utf8_percent_encode(u, percent_encoding::NON_ALPHANUMERIC)
                .to_string(),
        );
        if let Some(p) = &pass {
            result.push(':');
            result.push_str(
                &percent_encoding::utf8_percent_encode(p, percent_encoding::NON_ALPHANUMERIC)
                    .to_string(),
            );
        }
        result.push('@');
    }
    result.push_str(&format!("{host}:{port}/{db}"));

    // Forward query parameters (sslmode, etc.)
    if let Some(query) = url.query() {
        result.push('?');
        result.push_str(query);
    }

    Ok(result)
}

/// Parse PostgreSQL connection string into parameters for PostgresConnectionPool.
/// Same logic as the existing postgres provider.
fn parse_connection_params(
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
) -> Result<HashMap<String, SecretString>> {
    let url = url::Url::parse(connection_string).with_context(|| {
        format!(
            "Invalid PostgreSQL connection string: {}",
            connection_string
        )
    })?;

    let mut params: HashMap<String, SecretString> = HashMap::new();

    if let Some(host) = url.host_str() {
        params.insert(
            "host".to_string(),
            SecretString::new(host.to_string().into_boxed_str()),
        );
    }

    if let Some(port) = url.port() {
        params.insert(
            "port".to_string(),
            SecretString::new(port.to_string().into_boxed_str()),
        );
    }

    if let Some(opts) = options {
        if let Some(user_env) = opts.get("user_env") {
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

        if let Some(pass_env) = opts.get("pass_env") {
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
    }

    let db_name = url.path().trim_start_matches('/');
    if !db_name.is_empty() {
        params.insert(
            "db".to_string(),
            SecretString::new(db_name.to_string().into_boxed_str()),
        );
    }

    for (key, value) in url.query_pairs() {
        params.insert(
            key.to_string(),
            SecretString::new(value.to_string().into_boxed_str()),
        );
    }

    Ok(params)
}

/// Query information_schema to detect auto-generated columns (SERIAL, IDENTITY, sequences).
async fn detect_auto_generated_columns(
    pool: &PgPool,
    schema_name: &str,
    table_name: &str,
) -> Result<Vec<String>> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = $2 \
         AND (is_identity = 'YES' OR column_default LIKE 'nextval%')",
    )
    .bind(schema_name)
    .bind(table_name)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("Failed to detect auto-generated columns for '{schema_name}.{table_name}'")
    })?;

    Ok(rows.into_iter().map(|r| r.0).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BooleanArray, Float64Array, Int16Array, Int32Array, Int64Array, StringArray,
    };
    use datafusion::common::Column;
    use datafusion::logical_expr::Operator;
    use secrecy::ExposeSecret;

    // ─── build_sqlx_connection_url tests ────────────────────────────────

    #[test]
    fn test_build_sqlx_connection_url_basic() {
        let url = build_sqlx_connection_url("postgresql://localhost:5432/mydb", None).unwrap();
        assert_eq!(url, "postgres://localhost:5432/mydb");
    }

    #[test]
    fn test_build_sqlx_connection_url_with_query_params() {
        let url =
            build_sqlx_connection_url("postgresql://localhost:5432/mydb?sslmode=require", None)
                .unwrap();
        assert_eq!(url, "postgres://localhost:5432/mydb?sslmode=require");
    }

    #[test]
    fn test_build_sqlx_connection_url_with_env_credentials() {
        unsafe {
            std::env::set_var("TEST_SQLX_PG_USER", "testuser");
            std::env::set_var("TEST_SQLX_PG_PASS", "testpass");
        }
        let mut options = HashMap::new();
        options.insert("user_env".to_string(), "TEST_SQLX_PG_USER".to_string());
        options.insert("pass_env".to_string(), "TEST_SQLX_PG_PASS".to_string());

        let url =
            build_sqlx_connection_url("postgresql://localhost:5432/mydb", Some(&options)).unwrap();
        assert_eq!(url, "postgres://testuser:testpass@localhost:5432/mydb");

        unsafe {
            std::env::remove_var("TEST_SQLX_PG_USER");
            std::env::remove_var("TEST_SQLX_PG_PASS");
        }
    }

    #[test]
    fn test_build_sqlx_connection_url_special_chars_in_password() {
        unsafe {
            std::env::set_var("TEST_SQLX_PG_USER2", "user@domain");
            std::env::set_var("TEST_SQLX_PG_PASS2", "p@ss:w0rd/special");
        }
        let mut options = HashMap::new();
        options.insert("user_env".to_string(), "TEST_SQLX_PG_USER2".to_string());
        options.insert("pass_env".to_string(), "TEST_SQLX_PG_PASS2".to_string());

        let url =
            build_sqlx_connection_url("postgresql://localhost:5432/mydb", Some(&options)).unwrap();
        // Special chars should be percent-encoded
        assert!(url.contains("user%40domain"));
        assert!(url.contains("p%40ss%3Aw0rd%2Fspecial"));

        unsafe {
            std::env::remove_var("TEST_SQLX_PG_USER2");
            std::env::remove_var("TEST_SQLX_PG_PASS2");
        }
    }

    #[test]
    fn test_build_sqlx_connection_url_missing_env() {
        let mut options = HashMap::new();
        options.insert("user_env".to_string(), "NONEXISTENT_SQLX_VAR".to_string());

        let result = build_sqlx_connection_url("postgresql://localhost:5432/mydb", Some(&options));
        assert!(result.is_err());
    }

    #[test]
    fn test_build_sqlx_connection_url_default_port() {
        let url = build_sqlx_connection_url("postgresql://myhost/mydb", None).unwrap();
        assert_eq!(url, "postgres://myhost:5432/mydb");
    }

    #[test]
    fn test_build_sqlx_connection_url_invalid() {
        let result = build_sqlx_connection_url("not-a-valid-url", None);
        assert!(result.is_err());
    }

    // ─── parse_connection_params tests ──────────────────────────────────

    #[test]
    fn test_parse_connection_params_basic() {
        let params = parse_connection_params("postgresql://localhost:5432/mydb", None).unwrap();
        assert!(params.contains_key("host"));
        assert!(params.contains_key("port"));
        assert!(params.contains_key("db"));
    }

    #[test]
    fn test_parse_connection_params_no_password() {
        let params = parse_connection_params("postgresql://localhost:5432/mydb", None).unwrap();
        assert!(params.contains_key("host"));
        assert!(!params.contains_key("user"));
        assert!(!params.contains_key("pass"));
    }

    #[test]
    fn test_parse_connection_params_with_query_params() {
        let params = parse_connection_params(
            "postgresql://localhost:5432/db?sslmode=require&connect_timeout=10",
            None,
        )
        .unwrap();
        assert!(params.contains_key("sslmode"));
        assert!(params.contains_key("connect_timeout"));
    }

    #[test]
    fn test_parse_connection_params_invalid() {
        let result = parse_connection_params("not-a-valid-url", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_connection_params_with_env_credentials() {
        unsafe {
            std::env::set_var("TEST_SQLX_PARSE_USER", "envuser");
            std::env::set_var("TEST_SQLX_PARSE_PASS", "envpass");
        }
        let mut options = HashMap::new();
        options.insert("user_env".to_string(), "TEST_SQLX_PARSE_USER".to_string());
        options.insert("pass_env".to_string(), "TEST_SQLX_PARSE_PASS".to_string());

        let params =
            parse_connection_params("postgresql://localhost:5432/mydb", Some(&options)).unwrap();

        assert_eq!(params.get("user").unwrap().expose_secret(), "envuser");
        assert_eq!(params.get("pass").unwrap().expose_secret(), "envpass");

        unsafe {
            std::env::remove_var("TEST_SQLX_PARSE_USER");
            std::env::remove_var("TEST_SQLX_PARSE_PASS");
        }
    }

    #[test]
    fn test_parse_connection_params_missing_env_var() {
        let mut options = HashMap::new();
        options.insert(
            "user_env".to_string(),
            "NONEXISTENT_SQLX_PARSE_VAR".to_string(),
        );

        let result = parse_connection_params("postgresql://localhost:5432/mydb", Some(&options));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Environment variable 'NONEXISTENT_SQLX_PARSE_VAR' not found")
        );
    }

    // ─── table/schema option tests ──────────────────────────────────────

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
        assert_eq!(reference.to_string(), "public.users");
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
                None,
                None,
            )
            .await;

            assert!(result.is_err());
            let error_msg = result.unwrap_err().to_string();
            assert!(error_msg.contains("requires 'table' option"));
        });
    }

    // ─── SQL helper tests ───────────────────────────────────────────────

    #[test]
    fn test_build_where_clause_empty() {
        let clause = build_where_clause(&[]).unwrap();
        assert_eq!(clause, "");
    }

    #[test]
    fn test_build_where_clause_single_filter() {
        let filter = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("id"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Int64(Some(42)),
                None,
            )),
        });
        let clause = build_where_clause(&[filter]).unwrap();
        assert!(clause.starts_with(" WHERE "));
        assert!(clause.contains("id"));
        assert!(clause.contains("42"));
    }

    #[test]
    fn test_build_where_clause_multiple_filters() {
        let filter1 = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("age"))),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Int64(Some(18)),
                None,
            )),
        });
        let filter2 = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("active"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Boolean(Some(true)),
                None,
            )),
        });
        let clause = build_where_clause(&[filter1, filter2]).unwrap();
        assert!(clause.starts_with(" WHERE "));
        assert!(clause.contains(" AND "));
    }

    #[test]
    fn test_quote_pg_ident() {
        assert_eq!(quote_pg_ident("name"), "\"name\"");
        assert_eq!(quote_pg_ident("my\"col"), "\"my\"\"col\"");
    }

    #[test]
    fn test_quote_pg_ident_reserved_word() {
        assert_eq!(quote_pg_ident("select"), "\"select\"");
        assert_eq!(quote_pg_ident("user"), "\"user\"");
    }

    // ─── filter_batch_columns tests ─────────────────────────────────────

    #[test]
    fn test_filter_batch_no_auto_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::new_empty(schema);
        let result = filter_batch_columns(&batch, &[]).unwrap();
        assert_eq!(result.num_columns(), 2);
    }

    #[test]
    fn test_filter_batch_with_auto_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["Alice"])),
                Arc::new(StringArray::from(vec![Some("alice@example.com")])),
            ],
        )
        .unwrap();

        let result = filter_batch_columns(&batch, &["id".to_string()]).unwrap();
        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.schema().field(0).name(), "name");
        assert_eq!(result.schema().field(1).name(), "email");
    }

    #[test]
    fn test_filter_batch_multiple_auto_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("created_at", DataType::Utf8, true),
            Field::new("updated_at", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["Alice"])),
                Arc::new(StringArray::from(vec![Some("2024-01-01")])),
                Arc::new(StringArray::from(vec![Some("2024-01-02")])),
            ],
        )
        .unwrap();

        let result =
            filter_batch_columns(&batch, &["id".to_string(), "created_at".to_string()]).unwrap();
        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.schema().field(0).name(), "name");
        assert_eq!(result.schema().field(1).name(), "updated_at");
    }

    #[test]
    fn test_filter_batch_preserves_data() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
                Arc::new(Int32Array::from(vec![30, 25, 35])),
            ],
        )
        .unwrap();

        let result = filter_batch_columns(&batch, &["id".to_string()]).unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 2);

        // Verify actual data values are preserved
        let name_col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(name_col.value(1), "Bob");
        assert_eq!(name_col.value(2), "Charlie");

        let age_col = result
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(age_col.value(0), 30);
        assert_eq!(age_col.value(1), 25);
        assert_eq!(age_col.value(2), 35);
    }

    #[test]
    fn test_filter_batch_all_columns_auto() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("seq", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![100])),
            ],
        )
        .unwrap();

        let result = filter_batch_columns(&batch, &["id".to_string(), "seq".to_string()]).unwrap();
        assert_eq!(result.num_columns(), 0);
        assert_eq!(result.num_rows(), 1);
    }

    // ─── InsertBuilder integration (batch projection) tests ─────────────

    #[test]
    fn test_insert_builder_with_filtered_batch() {
        // Verify that InsertBuilder generates correct SQL when given a projected batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
                Arc::new(Int32Array::from(vec![30, 25])),
            ],
        )
        .unwrap();

        let table_ref = TableReference::partial("public", "users");
        let batches = vec![batch];
        let sql = InsertBuilder::new(&table_ref, &batches)
            .build_postgres(None)
            .unwrap();

        // Should include only "name" and "age", not any auto-generated "id"
        assert!(sql.contains("\"name\""));
        assert!(sql.contains("\"age\""));
        assert!(sql.contains("Alice"));
        assert!(sql.contains("Bob"));
        assert!(!sql.contains("\"id\""));
    }

    #[test]
    fn test_insert_builder_with_various_types() {
        // Test InsertBuilder handles multiple Arrow data types correctly
        let schema = Arc::new(Schema::new(vec![
            Field::new("int_col", DataType::Int64, false),
            Field::new("float_col", DataType::Float64, true),
            Field::new("str_col", DataType::Utf8, true),
            Field::new("bool_col", DataType::Boolean, true),
            Field::new("small_int_col", DataType::Int16, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(Float64Array::from(vec![3.14])),
                Arc::new(StringArray::from(vec![Some("hello")])),
                Arc::new(BooleanArray::from(vec![Some(true)])),
                Arc::new(Int16Array::from(vec![7])),
            ],
        )
        .unwrap();

        let table_ref = TableReference::partial("public", "test_types");
        let batches = vec![batch];
        let sql = InsertBuilder::new(&table_ref, &batches)
            .build_postgres(None)
            .unwrap();

        assert!(sql.starts_with("INSERT INTO"));
        assert!(sql.contains("\"public\".\"test_types\""));
        assert!(sql.contains("42"));
        assert!(sql.contains("hello"));
    }

    #[test]
    fn test_insert_builder_with_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("Alice"), None])),
                Arc::new(Int32Array::from(vec![None, Some(25)])),
            ],
        )
        .unwrap();

        let table_ref = TableReference::bare("test_nulls");
        let batches = vec![batch];
        let sql = InsertBuilder::new(&table_ref, &batches)
            .build_postgres(None)
            .unwrap();

        assert!(sql.contains("NULL"));
        assert!(sql.contains("Alice"));
        assert!(sql.contains("25"));
    }

    // ─── parse_allowed_schemas tests ────────────────────────────────────

    #[test]
    fn test_parse_allowed_schemas_none_options() {
        let result = parse_allowed_schemas(None);
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_allowed_schemas_missing_key() {
        let options: HashMap<String, String> = HashMap::new();
        let result = parse_allowed_schemas(Some(&options));
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_allowed_schemas_empty_value() {
        let mut options = HashMap::new();
        options.insert("allowed_schemas".to_string(), "".to_string());
        let result = parse_allowed_schemas(Some(&options));
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_allowed_schemas_whitespace_only() {
        let mut options = HashMap::new();
        options.insert("allowed_schemas".to_string(), "  ,  ,  ".to_string());
        let result = parse_allowed_schemas(Some(&options));
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_allowed_schemas_single() {
        let mut options = HashMap::new();
        options.insert("allowed_schemas".to_string(), "public".to_string());
        let result = parse_allowed_schemas(Some(&options)).unwrap();
        assert_eq!(result, vec!["public"]);
    }

    #[test]
    fn test_parse_allowed_schemas_multiple() {
        let mut options = HashMap::new();
        options.insert(
            "allowed_schemas".to_string(),
            "public,private,analytics".to_string(),
        );
        let result = parse_allowed_schemas(Some(&options)).unwrap();
        assert_eq!(result, vec!["public", "private", "analytics"]);
    }

    #[test]
    fn test_parse_allowed_schemas_whitespace_trimming() {
        let mut options = HashMap::new();
        options.insert(
            "allowed_schemas".to_string(),
            " public , private , analytics ".to_string(),
        );
        let result = parse_allowed_schemas(Some(&options)).unwrap();
        assert_eq!(result, vec!["public", "private", "analytics"]);
    }

    #[test]
    fn test_parse_allowed_schemas_empty_segments_filtered() {
        let mut options = HashMap::new();
        options.insert(
            "allowed_schemas".to_string(),
            "public,,analytics".to_string(),
        );
        let result = parse_allowed_schemas(Some(&options)).unwrap();
        assert_eq!(result, vec!["public", "analytics"]);
    }

    // ─── HierarchyLevel tests ────────────────────────────────────────────

    #[test]
    fn test_hierarchy_level_default_is_table() {
        assert_eq!(
            crate::sources::HierarchyLevel::default(),
            crate::sources::HierarchyLevel::Table
        );
    }

    #[test]
    fn test_hierarchy_level_as_str_table() {
        assert_eq!(crate::sources::HierarchyLevel::Table.as_str(), "table");
    }

    #[test]
    fn test_hierarchy_level_as_str_catalog() {
        assert_eq!(crate::sources::HierarchyLevel::Catalog.as_str(), "catalog");
    }

    #[test]
    fn test_hierarchy_level_serde_roundtrip() {
        let table = crate::sources::HierarchyLevel::Table;
        let serialized = serde_json::to_string(&table).unwrap();
        let deserialized: crate::sources::HierarchyLevel =
            serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, crate::sources::HierarchyLevel::Table);

        let catalog = crate::sources::HierarchyLevel::Catalog;
        let serialized = serde_json::to_string(&catalog).unwrap();
        let deserialized: crate::sources::HierarchyLevel =
            serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, crate::sources::HierarchyLevel::Catalog);
    }

    #[test]
    fn test_hierarchy_level_serde_lowercase_strings() {
        let table: crate::sources::HierarchyLevel = serde_json::from_str("\"table\"").unwrap();
        assert_eq!(table, crate::sources::HierarchyLevel::Table);

        let catalog: crate::sources::HierarchyLevel = serde_json::from_str("\"catalog\"").unwrap();
        assert_eq!(catalog, crate::sources::HierarchyLevel::Catalog);
    }

    // ─── Catalog dispatch tests (unit) ──────────────────────────────────

    /// Catalog mode should never fail with "requires 'table' option" — that error
    /// is exclusive to single-table mode when no `table` key is provided.
    #[tokio::test]
    async fn test_catalog_mode_error_is_not_missing_table_option() {
        let mut session_ctx = SessionContext::new();
        let result = register_postgres_tables(
            &mut session_ctx,
            "mydb",
            // Use an unreachable address to guarantee a connection failure without hanging.
            "postgresql://127.0.0.1:19999/nonexistent",
            None,
            false,
            None,
            Some(crate::sources::HierarchyLevel::Catalog),
        )
        .await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        // Catalog path must never surface the single-table "requires 'table' option" message.
        assert!(!error_msg.contains("requires 'table' option"));
    }

    /// Table mode without a `table` option always fails with a descriptive error.
    #[tokio::test]
    async fn test_table_mode_requires_table_option() {
        let mut session_ctx = SessionContext::new();
        let result = register_postgres_tables(
            &mut session_ctx,
            "test_source",
            "postgresql://localhost:5432/db",
            None,
            false,
            None,
            Some(crate::sources::HierarchyLevel::Table),
        )
        .await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("requires 'table' option")
        );
    }

    /// `None` for `hierarchy_level` defaults to table mode.
    #[tokio::test]
    async fn test_hierarchy_level_none_defaults_to_table_mode() {
        let mut session_ctx = SessionContext::new();
        let result = register_postgres_tables(
            &mut session_ctx,
            "test_source",
            "postgresql://localhost:5432/db",
            None,
            false,
            None,
            None, // None → Table
        )
        .await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("requires 'table' option")
        );
    }

    // ─── allowed_schemas filtering logic tests ───────────────────────────

    /// Unit-test the schema filtering logic exercised inside `list_postgres_tables_in_catalog`.
    #[test]
    fn test_allowed_schemas_filter_retains_matching_rows() {
        let rows = vec![
            ("public".to_string(), "users".to_string()),
            ("private".to_string(), "secrets".to_string()),
            ("analytics".to_string(), "events".to_string()),
        ];

        let allowed = vec!["public".to_string(), "analytics".to_string()];
        let mut filtered = rows.clone();
        filtered.retain(|(schema, _)| allowed.iter().any(|s| s == schema));

        assert_eq!(filtered.len(), 2);
        assert!(filtered.contains(&("public".to_string(), "users".to_string())));
        assert!(filtered.contains(&("analytics".to_string(), "events".to_string())));
        assert!(!filtered.iter().any(|(s, _)| s == "private"));
    }

    #[test]
    fn test_allowed_schemas_filter_with_no_match() {
        let rows = vec![
            ("public".to_string(), "users".to_string()),
            ("private".to_string(), "secrets".to_string()),
        ];

        let allowed = vec!["analytics".to_string()];
        let mut filtered = rows.clone();
        filtered.retain(|(schema, _)| allowed.iter().any(|s| s == schema));

        assert!(filtered.is_empty());
    }

    #[test]
    fn test_allowed_schemas_filter_none_retains_all() {
        // When `allowed_schemas` is None the retain is skipped and all rows are kept.
        let rows = vec![
            ("public".to_string(), "users".to_string()),
            ("private".to_string(), "secrets".to_string()),
        ];
        let allowed: Option<Vec<String>> = None;
        let mut filtered = rows.clone();
        if let Some(allowed) = &allowed {
            filtered.retain(|(schema, _)| allowed.iter().any(|s| s == schema));
        }
        assert_eq!(filtered.len(), 2);
        assert!(filtered.contains(&("public".to_string(), "users".to_string())));
        assert!(filtered.contains(&("private".to_string(), "secrets".to_string())));
    }

    // ─── Integration test helpers ────────────────────────────────────────

    /// Register a PostgreSQL table from the CI docker service.
    /// Expects PG_USER and PG_PASSWORD env vars to be set.
    async fn register_ci_table(ctx: &mut SessionContext, table: &str) {
        let mut options = HashMap::new();
        options.insert("table".to_string(), table.to_string());
        options.insert("schema".to_string(), "public".to_string());
        options.insert("user_env".to_string(), "PG_USER".to_string());
        options.insert("pass_env".to_string(), "PG_PASSWORD".to_string());
        register_postgres_tables(
            ctx,
            table,
            "postgresql://127.0.0.1:5432/mydb?sslmode=disable",
            Some(&options),
            true,
            None,
            None,
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
            .downcast_ref::<StringArray>()
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
            .downcast_ref::<StringArray>()
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

        ctx.sql("INSERT INTO users (name, email) VALUES ('Dave Brown', 'dave_pg@example.com')")
            .await
            .expect("parse insert")
            .collect()
            .await
            .expect("execute insert");

        let batches = query_all(&ctx, "SELECT id, name, email FROM users ORDER BY id").await;
        assert!(total_rows(&batches) >= 4);
    }

    // ─── Delete tests (integration) ─────────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_delete_with_filter() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "users").await;

        ctx.sql("INSERT INTO users (name, email) VALUES ('PgDeleteMe', 'pgdeleteme@example.com')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let before = query_all(&ctx, "SELECT id FROM users WHERE name = 'PgDeleteMe'").await;
        assert_eq!(total_rows(&before), 1);

        ctx.sql("DELETE FROM users WHERE name = 'PgDeleteMe'")
            .await
            .expect("parse delete")
            .collect()
            .await
            .expect("execute delete");

        let after = query_all(&ctx, "SELECT id FROM users WHERE name = 'PgDeleteMe'").await;
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

        ctx.sql(
            "UPDATE users SET email = 'alice_pg_updated@example.com' WHERE name = 'Alice Smith'",
        )
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
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(emails.value(0), "alice_pg_updated@example.com");
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
            .downcast_ref::<StringArray>()
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
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string();
        assert_eq!(before_email, after_email);
    }

    #[tokio::test]
    #[ignore]
    async fn test_update_multiple_columns() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "users").await;

        ctx.sql(
            "INSERT INTO users (name, email) VALUES ('PgMultiUpdate', 'pg_multi_update@example.com')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

        ctx.sql(
            "UPDATE users
             SET name = 'PgMultiUpdateRenamed',
                 email = 'pg_multi_update_renamed@example.com'
             WHERE name = 'PgMultiUpdate'",
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
             WHERE email = 'pg_multi_update_renamed@example.com'",
        )
        .await;
        assert_eq!(total_rows(&batches), 1);

        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let emails = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "PgMultiUpdateRenamed");
        assert_eq!(emails.value(0), "pg_multi_update_renamed@example.com");

        ctx.sql("DELETE FROM users WHERE email = 'pg_multi_update_renamed@example.com'")
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
        ctx.sql(
            "INSERT INTO users (name, email) VALUES ('PgRoundTrip', 'pgroundtrip@example.com')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        let after_insert = query_all(&ctx, "SELECT id FROM users WHERE name = 'PgRoundTrip'").await;
        assert_eq!(total_rows(&after_insert), 1);

        // 2. Update
        ctx.sql(
            "UPDATE users SET email = 'pgroundtrip_updated@example.com' WHERE name = 'PgRoundTrip'",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        let batches = query_all(&ctx, "SELECT email FROM users WHERE name = 'PgRoundTrip'").await;
        let emails = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(emails.value(0), "pgroundtrip_updated@example.com");

        // 3. Delete
        ctx.sql("DELETE FROM users WHERE name = 'PgRoundTrip'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let after_delete = query_all(&ctx, "SELECT id FROM users WHERE name = 'PgRoundTrip'").await;
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
        assert_eq!(total_rows(&batches), 3);
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
               u.id,
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
               u.id,
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

    #[tokio::test]
    #[ignore]
    async fn test_empty_table_schema_detection() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "user_order_stats").await;

        // user_order_stats starts empty but schema should be detected
        let catalog = ctx.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();
        let table = schema.table("user_order_stats").await.unwrap().unwrap();
        let table_schema = table.schema();
        let field_names: Vec<&str> = table_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert!(field_names.contains(&"user_id"));
        assert!(field_names.contains(&"user_name"));
        assert!(field_names.contains(&"total_orders"));
    }
}
