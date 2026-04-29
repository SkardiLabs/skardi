pub mod fts_exec;
pub mod fts_table_function;
pub mod knn_exec;
pub mod knn_table_function;
pub mod vec_to_binary;

pub use fts_table_function::register_sqlite_fts_udtf;
pub use knn_table_function::{SqliteEntry, register_sqlite_knn_udtf};
pub use vec_to_binary::register_vec_to_binary_udf;

use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, FixedSizeListArray, Float32Array, Float64Array,
    Int64Array, ListArray, RecordBatch, RecordBatchOptions, StringArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{Constraints, ScalarValue};
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

use crate::sources::DataSourceType;
use crate::sources::hierarchy::{
    HierarchyLevel, SourceLabel, build_catalog, parse_allowed_schemas, retry_with_timeout,
};
use crate::sources::providers::{DatasetEntry, DatasetRegistry};

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
        &[],
    )
    .await?;
    Ok(Arc::new(provider))
}

/// Register SQLite tables or a whole database (catalog) into a DataFusion [`SessionContext`].
///
/// Single-table mode (default) registers one table under `name`. Catalog mode registers a
/// `MemoryCatalogProvider` with one provider per table across all non-system tables in the
/// database (SQLite schema `main`).
///
/// # Arguments
/// * `session_ctx` - DataFusion session context to register tables into
/// * `name` - Name to register the table (table mode) or catalog (catalog mode) as
/// * `db_path` - Path to the SQLite database file (e.g., "/data/my.db")
/// * `options` - Optional configuration (see below)
/// * `read_write` - If true, register as read-write (allows INSERT/UPDATE/DELETE)
/// * `registry` - Optional dataset registry for sqlite_knn / sqlite_fts table functions
/// * `hierarchy_level` - `HierarchyLevel::Table` (default) or `HierarchyLevel::Catalog` loads the whole DB
///
/// # Options
/// * `table` - Table name (required in table mode, not used in catalog mode)
/// * `busy_timeout_ms` - Busy timeout in milliseconds (optional, defaults to 5000)
/// * `read_pool_size` - Number of read connections per table (optional, defaults to 4)
/// * `extensions` - Comma-separated extension paths to load (e.g. sqlite-vec)
/// * `extensions_env` - Comma-separated env var names whose values are extension paths
/// * `allowed_schemas` - Comma-separated schema allow-list (catalog mode only; SQLite schema is `main`)
pub async fn register_sqlite_tables(
    session_ctx: &mut SessionContext,
    name: &str,
    db_path: &str,
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
            register_sqlite_catalog(
                session_ctx,
                name,
                db_path,
                options,
                read_write,
                mode_str,
                registry,
            )
            .await
        }
        HierarchyLevel::Table => {
            register_single_sqlite_table(
                session_ctx,
                name,
                db_path,
                options,
                read_write,
                mode_str,
                registry,
            )
            .await
        }
    }
}

/// Register one SQLite table under `name` in the default catalog.
async fn register_single_sqlite_table(
    session_ctx: &mut SessionContext,
    name: &str,
    db_path: &str,
    options: Option<&HashMap<String, String>>,
    read_write: bool,
    mode_str: &str,
    registry: Option<&DatasetRegistry>,
) -> Result<()> {
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

    // Comma-separated list of extension paths to load (e.g. sqlite-vec).
    // Supports both literal paths (`extensions`) and env var names (`extensions_env`).
    let mut extensions: Vec<String> = options
        .and_then(|opts| opts.get("extensions"))
        .map(|v| v.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();
    if let Some(env_key) = options.and_then(|opts| opts.get("extensions_env")) {
        for key in env_key.split(',') {
            let key = key.trim();
            if let Ok(val) = std::env::var(key) {
                extensions.push(val);
            } else {
                tracing::warn!("SQLite extension env var '{}' not set, skipping", key);
            }
        }
    }

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
        &extensions,
    )
    .await
    .with_context(|| {
        format!(
            "Failed to create SQLite table provider for '{}'",
            table_name
        )
    })?;

    // Populate the registry for sqlite_knn / sqlite_fts table functions.
    if let Some(registry) = registry {
        let columns: Vec<(String, DataType)> = provider
            .schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect();
        let entry = SqliteEntry {
            conn: Arc::clone(&provider.read_pool[0]),
            table_name: table_name.clone(),
            columns,
        };
        let mut reg = registry
            .write()
            .map_err(|e| anyhow::anyhow!("sqlite registry lock error: {}", e))?;
        reg.insert(name.to_string(), DatasetEntry::Sqlite(entry));
        tracing::debug!("Registered SQLite table '{}' in dataset registry", name);
    }

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

/// Register an entire SQLite database as a named DataFusion catalog.
///
/// All non-system tables and views in the `main` schema are registered automatically.
/// Tables are addressable with the three-part reference `catalog.main.table`.
///
/// A single [`Connection`] pool (configured via `read_pool_size`) is opened once and
/// shared across every table provider in the catalog, rather than allocating a fresh pool
/// per table. This keeps file-descriptor usage bounded regardless of table count.
///
/// If a `registry` is supplied, each table is inserted under the key
/// `"<catalog>.<schema>.<table>"` so that `sqlite_knn` / `sqlite_fts` can look it up with the
/// same three-part identifier that appears in SQL (e.g. `sqlite_knn('demo.main.vec_items', …)`).
async fn register_sqlite_catalog(
    session_ctx: &mut SessionContext,
    catalog_name: &str,
    db_path: &str,
    options: Option<&HashMap<String, String>>,
    read_write: bool,
    mode_str: &str,
    registry: Option<&DatasetRegistry>,
) -> Result<()> {
    tracing::info!(
        "Registering SQLite catalog: {} ({})",
        catalog_name,
        mode_str
    );

    let busy_timeout_ms: u64 = options
        .and_then(|opts| opts.get("busy_timeout_ms"))
        .and_then(|v| v.parse().ok())
        .unwrap_or(5000);

    let read_pool_size: usize = options
        .and_then(|opts| opts.get("read_pool_size"))
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_READ_POOL_SIZE);

    let mut extensions: Vec<String> = options
        .and_then(|opts| opts.get("extensions"))
        .map(|v| v.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();
    if let Some(env_key) = options.and_then(|opts| opts.get("extensions_env")) {
        for key in env_key.split(',') {
            let key = key.trim();
            if let Ok(val) = std::env::var(key) {
                extensions.push(val);
            } else {
                tracing::warn!("SQLite extension env var '{}' not set, skipping", key);
            }
        }
    }

    let label = SourceLabel::new(
        DataSourceType::Sqlite,
        HierarchyLevel::Catalog,
        catalog_name,
    );

    // Open the shared read/write pool exactly once and reuse it across every table in
    // the catalog. Without this, each table would allocate its own `read_pool_size`
    // connections and blow up FD usage on databases with many tables.
    let (read_pool, write_conn) = open_sqlite_pool(
        db_path,
        busy_timeout_ms,
        read_pool_size,
        read_write,
        &extensions,
    )
    .await
    .with_context(|| {
        format!(
            "Failed to open shared SQLite pool for catalog '{}'",
            catalog_name
        )
    })?;

    // Use one of the shared connections for introspection rather than opening another.
    let intro_conn = Arc::clone(&read_pool[0]);
    let mut schema_tables = retry_with_timeout(label, "sqlite_master introspection", || async {
        list_sqlite_tables_in_catalog(&intro_conn).await
    })
    .await?;

    let allowed_schemas = parse_allowed_schemas(options);
    if let Some(ref allowed) = allowed_schemas {
        if !allowed.iter().any(|s| s == "main") {
            tracing::warn!(
                "SQLite catalog '{}' has allowed_schemas={:?} which excludes 'main'; \
                 all SQLite tables live in schema 'main' so no tables will be registered",
                catalog_name,
                allowed
            );
        }
        schema_tables.retain(|(schema, _)| allowed.iter().any(|s| s == schema));
    }

    if schema_tables.is_empty() {
        tracing::warn!(
            "No tables found in SQLite catalog for source '{}'",
            catalog_name
        );
    }

    let table_count = schema_tables.len();

    let shared_read_pool = Arc::new(read_pool);
    let shared_write_conn = write_conn;
    let catalog_name_owned = catalog_name.to_string();

    build_catalog(
        session_ctx,
        catalog_name,
        schema_tables,
        |schema, table_name| {
            let read_pool_c = Arc::clone(&shared_read_pool);
            let write_conn_c = shared_write_conn.clone();
            let registry_c = registry.map(Arc::clone);
            let catalog_c = catalog_name_owned.clone();
            async move {
                let provider = SqliteTableProvider::from_shared_pool(
                    (*read_pool_c).clone(),
                    write_conn_c,
                    TableReference::bare(table_name.as_str()),
                    read_write,
                )
                .await
                .with_context(|| {
                    format!(
                        "Failed to create SQLite table provider for '{}.{}'",
                        schema, table_name
                    )
                })?;

                if let Some(registry) = registry_c {
                    let columns: Vec<(String, DataType)> = provider
                        .schema
                        .fields()
                        .iter()
                        .map(|f| (f.name().clone(), f.data_type().clone()))
                        .collect();
                    let entry = SqliteEntry {
                        conn: Arc::clone(&provider.read_pool[0]),
                        table_name: table_name.clone(),
                        columns,
                    };
                    // Key matches the three-part SQL reference so that UDTF callers can use
                    // e.g. `sqlite_knn('demo.main.vec_items', ...)` — the UDTF looks up by
                    // the exact string the caller passes.
                    let key = format!("{}.{}.{}", catalog_c, schema, table_name);
                    let mut reg = registry
                        .write()
                        .map_err(|e| anyhow::anyhow!("sqlite registry lock error: {}", e))?;
                    reg.insert(key, DatasetEntry::Sqlite(entry));
                }

                Ok(Arc::new(provider) as Arc<dyn TableProvider>)
            }
        },
    )
    .await
    .with_context(|| format!("Failed to build SQLite catalog '{}'", catalog_name))?;

    tracing::info!(
        "Registered SQLite catalog '{}' with {} table(s) ({})",
        catalog_name,
        table_count,
        mode_str
    );

    Ok(())
}

/// List user tables and views in the SQLite database's `main` schema.
///
/// Filters out:
/// * SQLite's internal bookkeeping tables (`sqlite_%` — e.g. `sqlite_sequence`, `sqlite_stat*`).
/// * Virtual-table shadow tables, which have `sql IS NULL` in `sqlite_master` (e.g. the
///   `<name>_data` / `_idx` / `_config` tables auto-created by FTS5 and `vec0`).
///   DataFusion cannot meaningfully register these on their own, and attempting to do so
///   would fail schema introspection at best or corrupt the parent virtual table at worst.
async fn list_sqlite_tables_in_catalog(conn: &Connection) -> Result<Vec<(String, String)>> {
    let names: Vec<String> = conn
        .call(
            move |conn| -> std::result::Result<Vec<String>, tokio_rusqlite::rusqlite::Error> {
                let mut stmt = conn.prepare(
                    "SELECT name FROM sqlite_master \
                     WHERE type IN ('table', 'view') \
                       AND name NOT LIKE 'sqlite_%' \
                       AND sql IS NOT NULL \
                     ORDER BY name",
                )?;
                stmt.query_map([], |row| row.get::<_, String>(0))?
                    .collect::<std::result::Result<Vec<_>, _>>()
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to list tables in SQLite catalog: {}", e))?;

    // SQLite's primary schema is always "main"
    Ok(names.into_iter().map(|t| ("main".to_string(), t)).collect())
}

/// Open and initialize a SQLite connection pool (read + optional write) that can be shared
/// across multiple [`SqliteTableProvider`] instances, as is done by catalog mode.
///
/// Each connection has WAL mode enabled, the configured busy timeout applied, and all
/// requested dynamic extensions loaded by [`init_connection`].
async fn open_sqlite_pool(
    db_path: &str,
    busy_timeout_ms: u64,
    read_pool_size: usize,
    read_write: bool,
    extensions: &[String],
) -> Result<(Vec<Arc<Connection>>, Option<Arc<Connection>>)> {
    let pool_size = read_pool_size.max(1);

    let mut read_pool = Vec::with_capacity(pool_size);
    for _ in 0..pool_size {
        let conn = Connection::open(db_path)
            .await
            .with_context(|| format!("Failed to open SQLite read connection: {}", db_path))?;
        init_connection(&conn, busy_timeout_ms, extensions).await?;
        read_pool.push(Arc::new(conn));
    }

    let write_conn = if read_write {
        let conn = Connection::open(db_path)
            .await
            .with_context(|| format!("Failed to open SQLite write connection: {}", db_path))?;
        init_connection(&conn, busy_timeout_ms, extensions).await?;
        Some(Arc::new(conn))
    } else {
        None
    };

    Ok((read_pool, write_conn))
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
    /// Open connections, enable WAL mode, load extensions, read schema from PRAGMA table_info().
    async fn new(
        db_path: &str,
        table_reference: TableReference,
        busy_timeout_ms: u64,
        read_pool_size: usize,
        read_write: bool,
        extensions: &[String],
    ) -> Result<Self> {
        let (read_pool, write_conn) = open_sqlite_pool(
            db_path,
            busy_timeout_ms,
            read_pool_size,
            read_write,
            extensions,
        )
        .await?;
        Self::from_shared_pool(read_pool, write_conn, table_reference, read_write).await
    }

    /// Build a provider on top of a pre-initialized connection pool.
    ///
    /// Catalog mode uses this to share a single pool across every table in the database
    /// instead of allocating a fresh pool per table. Each call still runs `PRAGMA
    /// table_info` against the first read connection to derive the table schema.
    async fn from_shared_pool(
        read_pool: Vec<Arc<Connection>>,
        write_conn: Option<Arc<Connection>>,
        table_reference: TableReference,
        read_write: bool,
    ) -> Result<Self> {
        if read_pool.is_empty() {
            anyhow::bail!("SqliteTableProvider::from_shared_pool requires a non-empty read pool");
        }

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

/// Validate an extension path before loading: must be absolute and free of
/// `..` traversal components. Catches typos or misconfigured options that
/// could cause SQLite to load an unintended shared library.
fn validate_extension_path(path: &str) -> Result<()> {
    use std::path::{Component, Path};

    let p = Path::new(path);
    if !p.is_absolute() {
        anyhow::bail!(
            "SQLite extension path must be absolute, got '{}'. \
             Use an absolute path to the extension shared library.",
            path
        );
    }
    if p.components().any(|c| matches!(c, Component::ParentDir)) {
        anyhow::bail!(
            "SQLite extension path must not contain '..' components, got '{}'",
            path
        );
    }
    Ok(())
}

/// Enable WAL mode, set busy timeout, and optionally load extensions on a connection.
async fn init_connection(
    conn: &Connection,
    busy_timeout_ms: u64,
    extensions: &[String],
) -> Result<()> {
    let timeout = busy_timeout_ms;
    for ext_path in extensions {
        validate_extension_path(ext_path)?;
    }
    let exts = extensions.to_vec();
    conn.call(
        move |conn| -> std::result::Result<(), tokio_rusqlite::rusqlite::Error> {
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "busy_timeout", timeout)?;

            // Load dynamic extensions (e.g. sqlite-vec for vec0 virtual tables).
            if !exts.is_empty() {
                // SAFETY: extension loading is intentionally enabled here based on
                // user-configured `extensions` option. We disable it again after loading.
                unsafe { conn.load_extension_enable()? };
                for ext_path in &exts {
                    unsafe { conn.load_extension(ext_path, None::<&str>)? };
                }
                conn.load_extension_disable()?;
            }

            Ok(())
        },
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to initialize SQLite connection: {}", e))
}

/// Read schema via PRAGMA table_info().
pub(crate) async fn read_schema_from_pragma(
    conn: &Connection,
    table_name: &str,
) -> Result<SchemaRef> {
    let tbl = table_name.to_string();
    let fields: Vec<Field> = conn
        .call(
            move |conn| -> std::result::Result<Vec<Field>, tokio_rusqlite::rusqlite::Error> {
                // Detect FTS5 virtual tables — their columns are always text,
                // but PRAGMA table_info reports empty type strings (same as vec0).
                let is_fts = is_fts_table(conn, &tbl);

                let mut stmt = conn.prepare(&format!("PRAGMA table_info(\"{}\")", tbl))?;
                let rows = stmt.query_map([], |row| {
                    let col_name: String = row.get(1)?;
                    let col_type: String = row.get(2)?;
                    let not_null: bool = row.get(3)?;
                    let is_pk: bool = row.get::<_, i32>(5)? != 0;
                    Ok((col_name, col_type, not_null, is_pk))
                })?;
                let mut fields = Vec::new();
                for row in rows {
                    let (col_name, col_type, not_null, is_pk) = row?;
                    let data_type = if is_fts && col_type.is_empty() && !is_pk {
                        // FTS5 columns are always text.
                        DataType::Utf8
                    } else {
                        sqlite_type_to_arrow(&col_type, is_pk)
                    };
                    fields.push(Field::new(col_name, data_type, !not_null));
                }
                Ok(fields)
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("PRAGMA table_info failed: {}", e))?;

    Ok(Arc::new(Schema::new(fields)))
}

/// Check if a table is an FTS5 virtual table by inspecting sqlite_master.
fn is_fts_table(conn: &tokio_rusqlite::rusqlite::Connection, table_name: &str) -> bool {
    conn.query_row(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
        [table_name],
        |row| row.get::<_, Option<String>>(0),
    )
    .ok()
    .flatten()
    .map(|sql| {
        let upper = sql.to_uppercase();
        upper.contains("FTS5") || upper.contains("FTS4") || upper.contains("FTS3")
    })
    .unwrap_or(false)
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
        Ok(Arc::new(SqliteScanExec::new(
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
        Ok(Arc::new(SqliteInsertExec::new(
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
        Ok(Arc::new(SqliteDmlExec::new(conn, sql)))
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
        Ok(Arc::new(SqliteDmlExec::new(conn, sql)))
    }
}

// ─── SqliteScanExec ──────────────────────────────────────────────────────────

/// Execution plan that reads from SQLite via `SELECT` with projection,
/// filter pushdown, and limit support.
struct SqliteScanExec {
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

impl SqliteScanExec {
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

        // DataFusion pushes down an empty projection for queries like `count(*)`
        // where only the row count is needed. SQLite rejects `SELECT  FROM t`, so
        // emit a constant projection that preserves row count (`SELECT 1 FROM t`)
        // and let `execute` return a zero-column batch with the correct row count.
        let projection_clause = if columns.is_empty() {
            "1".to_string()
        } else {
            columns.join(", ")
        };

        Ok(format!(
            "SELECT {projection_clause} FROM {table}{where_clause}{limit_clause}"
        ))
    }
}

impl fmt::Debug for SqliteScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SqliteScanExec(table={})", self.table_reference)
    }
}

impl DisplayAs for SqliteScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SqliteScanExec: table={}", self.table_reference)
    }
}

impl ExecutionPlan for SqliteScanExec {
    fn name(&self) -> &str {
        "SqliteScanExec"
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
            let (batch, row_count): (Vec<Vec<tokio_rusqlite::rusqlite::types::Value>>, usize) =
                conn.call(
                    move |conn| -> std::result::Result<_, tokio_rusqlite::rusqlite::Error> {
                        let mut stmt = conn.prepare(&sql)?;
                        let mut col_values: Vec<Vec<tokio_rusqlite::rusqlite::types::Value>> =
                            (0..num_cols).map(|_| Vec::new()).collect();
                        let mut row_count: usize = 0;

                        let mut rows = stmt.query([])?;
                        while let Some(row) = rows.next()? {
                            for col_idx in 0..num_cols {
                                let val: tokio_rusqlite::rusqlite::types::Value =
                                    row.get(col_idx)?;
                                col_values[col_idx].push(val);
                            }
                            row_count += 1;
                        }

                        Ok((col_values, row_count))
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

            if num_cols == 0 {
                // Zero-column batch (e.g. from `count(*)` projection pushdown).
                // RecordBatch::try_new would return a 0-row batch; pass the row
                // count explicitly so aggregates see the real input cardinality.
                let options = RecordBatchOptions::new().with_row_count(Some(row_count));
                RecordBatch::try_new_with_options(output_schema, arrays, &options)
                    .map_err(DataFusionError::from)
            } else {
                RecordBatch::try_new(output_schema, arrays).map_err(DataFusionError::from)
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.output_schema),
            stream::once(future),
        )))
    }
}

/// Convert a column of `rusqlite::types::Value` into an Arrow `ArrayRef`.
pub(crate) fn sqlite_values_to_arrow(
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

// ─── SqliteInsertExec ────────────────────────────────────────────────────────

/// Execution plan that consumes input batches and inserts them into SQLite.
struct SqliteInsertExec {
    conn: Arc<Connection>,
    table_reference: TableReference,
    table_schema: SchemaRef,
    input: Arc<dyn ExecutionPlan>,
    op: InsertOp,
    output_schema: SchemaRef,
    properties: PlanProperties,
}

impl SqliteInsertExec {
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

impl fmt::Debug for SqliteInsertExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SqliteInsertExec(table={})", self.table_reference)
    }
}

impl DisplayAs for SqliteInsertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SqliteInsertExec")
    }
}

impl ExecutionPlan for SqliteInsertExec {
    fn name(&self) -> &str {
        "SqliteInsertExec"
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
                "SqliteInsertExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&self.conn),
            self.table_reference.clone(),
            Arc::clone(&self.table_schema),
            children.into_iter().next().expect("len == 1 checked above"),
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
        // List<Float32> → packed little-endian f32 BLOB (for sqlite-vec vec0 tables).
        DataType::List(field) if *field.data_type() == DataType::Float32 => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("DataType::List guarantees ListArray");
            let values = list.value(row);
            let f32_arr = values
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("List<Float32> guarantees Float32Array values");
            let blob: Vec<u8> = f32_arr
                .values()
                .iter()
                .flat_map(|f| f.to_le_bytes())
                .collect();
            Value::Blob(blob)
        }
        // FixedSizeList<Float32> → packed little-endian f32 BLOB.
        DataType::FixedSizeList(field, _) if *field.data_type() == DataType::Float32 => {
            let list = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .expect("DataType::FixedSizeList guarantees FixedSizeListArray");
            let values = list.value(row);
            let f32_arr = values
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("FixedSizeList<Float32> guarantees Float32Array values");
            let blob: Vec<u8> = f32_arr
                .values()
                .iter()
                .flat_map(|f| f.to_le_bytes())
                .collect();
            Value::Blob(blob)
        }
        _ => Value::Text(format!("{:?}", array.as_ref())),
    }
}

// ─── SqliteDmlExec (DELETE / UPDATE) ─────────────────────────────────────────

/// A leaf `ExecutionPlan` that executes a pre-built SQLite DML statement
/// and returns a single row `{ count: u64 }` with the number of affected rows.
struct SqliteDmlExec {
    conn: Arc<Connection>,
    sql: String,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl SqliteDmlExec {
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

impl fmt::Debug for SqliteDmlExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SqliteDmlExec(sql={})", self.sql)
    }
}

impl DisplayAs for SqliteDmlExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SqliteDmlExec")
    }
}

impl ExecutionPlan for SqliteDmlExec {
    fn name(&self) -> &str {
        "SqliteDmlExec"
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
///
/// `is_pk` indicates whether the column is a primary key. Virtual tables
/// (e.g. sqlite-vec vec0) report empty type strings for all columns via
/// PRAGMA table_info. Primary key columns with empty type are INTEGER rowids;
/// non-pk columns with empty type are typically BLOBs (e.g. vector embeddings).
pub(crate) fn sqlite_type_to_arrow(sqlite_type: &str, is_pk: bool) -> DataType {
    let upper = sqlite_type.to_uppercase();
    if upper.is_empty() {
        if is_pk {
            // Virtual table primary keys (rowid) are always INTEGER.
            DataType::Int64
        } else {
            // Non-pk virtual table columns (e.g. vec0 embeddings) — map to
            // Binary so BLOB data is preserved correctly.
            DataType::Binary
        }
    } else if upper.contains("INT") {
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
pub(crate) fn quote_sqlite_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Try to convert a DataFusion `Expr` to a SQLite SQL string suitable for
/// use in a WHERE clause pushed down to SQLite.
pub(crate) fn expr_to_sqlite_sql(expr: &Expr) -> Option<String> {
    let unparser = Unparser::new(&SqliteDialect {});
    unparser.expr_to_sql(expr).ok().map(|ast| ast.to_string())
}

/// Extract a string literal from a DataFusion `Expr`.
/// Returns an empty string for NULL placeholders (schema inference).
pub(crate) fn extract_string(expr: &Expr, name: &str) -> DataFusionResult<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => Ok(s.clone()),
        Expr::Literal(ScalarValue::Null, _) => Ok(String::new()),
        _ => Err(DataFusionError::Plan(format!(
            "sqlite: '{}' must be a string literal",
            name
        ))),
    }
}

/// Produces a properly quoted table reference string for SQLite.
fn quote_sqlite_table(tbl: &TableReference) -> String {
    quote_sqlite_ident(tbl.table())
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array as _, Int64Array};

    #[test]
    fn test_empty_type_maps_to_binary_for_non_pk() {
        // vec0 virtual tables report empty type strings for vector columns.
        assert_eq!(sqlite_type_to_arrow("", false), DataType::Binary);
    }

    #[test]
    fn test_empty_type_maps_to_int64_for_pk() {
        // vec0 primary key columns report empty type but are INTEGER rowids.
        assert_eq!(sqlite_type_to_arrow("", true), DataType::Int64);
    }

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
            let result = register_sqlite_tables(
                &mut session_ctx,
                "test_table",
                "/tmp/test.db",
                None,
                false,
                None,
                HierarchyLevel::Table,
            )
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
        register_sqlite_tables(
            ctx,
            "test_items",
            db_path,
            Some(&options),
            true,
            None,
            HierarchyLevel::Table,
        )
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

    /// Regression test for #97: `count(*)` was rewritten to an empty projection
    /// (`SELECT  FROM "t"`), which SQLite rejects with a syntax error.
    #[tokio::test]
    #[ignore]
    async fn test_count_star_pushdown() {
        let db_path = create_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_test_table(&mut ctx, db).await;

        let batches = query_all(&ctx, "SELECT count(*) FROM test_items").await;
        assert_eq!(total_rows(&batches), 1);

        let counts = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.value(0), 3);
    }

    /// `count(*)` combined with a WHERE clause still needs the projection pushdown
    /// to produce the correct row count after filtering.
    #[tokio::test]
    #[ignore]
    async fn test_count_star_with_filter() {
        let db_path = create_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_test_table(&mut ctx, db).await;

        let batches = query_all(&ctx, "SELECT count(*) FROM test_items WHERE id > 1").await;
        assert_eq!(total_rows(&batches), 1);

        let counts = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.value(0), 2);
    }

    /// `count(*)` over an empty table must return 0, not a SQLite syntax error.
    #[tokio::test]
    #[ignore]
    async fn test_count_star_empty_table() {
        let db_path = create_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_test_table(&mut ctx, db).await;

        ctx.sql("DELETE FROM test_items")
            .await
            .expect("parse delete")
            .collect()
            .await
            .expect("execute delete");

        let batches = query_all(&ctx, "SELECT count(*) FROM test_items").await;
        assert_eq!(total_rows(&batches), 1);

        let counts = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.value(0), 0);
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

    /// Multi-row `INSERT INTO ... VALUES (...), (...), (...)` — the shape the
    /// server-side renderer emits when a pipeline parameter is the
    /// array-of-arrays form `{"rows": [[..], [..]]}`. DataFusion parses the
    /// VALUES list into a single batch with N rows, then SqliteInsertExec
    /// loops the batch row-by-row inside a transaction. Verifies the path
    /// commits all rows atomically.
    #[tokio::test]
    #[ignore]
    async fn test_insert_multi_row_values() {
        let db_path = create_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_test_table(&mut ctx, db).await;

        ctx.sql(
            "INSERT INTO test_items (id, name, value) VALUES \
             (10, 'eve', 100), (11, 'frank', 110), (12, 'gina', 120)",
        )
        .await
        .expect("parse multi-row insert")
        .collect()
        .await
        .expect("execute multi-row insert");

        let batches = query_all(
            &ctx,
            "SELECT id, name FROM test_items WHERE id >= 10 ORDER BY id",
        )
        .await;
        assert_eq!(total_rows(&batches), 3);
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
            register_sqlite_tables(
                ctx,
                reg_name,
                db_path,
                Some(&options),
                true,
                None,
                HierarchyLevel::Table,
            )
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

    // ─── Read-your-own-write tests ─────────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_read_own_write_insert_then_select() {
        let db_path = create_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_test_table(&mut ctx, db).await;

        // Insert via DataFusion
        ctx.sql("INSERT INTO test_items (id, name, value) VALUES (4, 'dave', 40)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Read back immediately — must see the new row
        let batches = query_all(&ctx, "SELECT id, name FROM test_items WHERE id = 4").await;
        assert_eq!(
            total_rows(&batches),
            1,
            "inserted row must be visible immediately"
        );

        let names = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "dave");
    }

    #[tokio::test]
    #[ignore]
    async fn test_read_own_write_delete_then_select() {
        let db_path = create_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_test_table(&mut ctx, db).await;

        // Delete via DataFusion
        ctx.sql("DELETE FROM test_items WHERE id = 1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Read back — deleted row must be gone
        let batches = query_all(&ctx, "SELECT id FROM test_items WHERE id = 1").await;
        assert_eq!(total_rows(&batches), 0, "deleted row must not be visible");

        // Other rows still present
        let batches = query_all(&ctx, "SELECT id FROM test_items").await;
        assert_eq!(total_rows(&batches), 2);
    }

    #[tokio::test]
    #[ignore]
    async fn test_read_own_write_update_then_select() {
        let db_path = create_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();
        register_test_table(&mut ctx, db).await;

        // Update via DataFusion
        ctx.sql("UPDATE test_items SET value = 999 WHERE id = 2")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Read back — must see updated value
        let batches = query_all(&ctx, "SELECT value FROM test_items WHERE id = 2").await;
        assert_eq!(total_rows(&batches), 1);

        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            values.value(0),
            999,
            "updated value must be visible immediately"
        );
    }

    // ─── Virtual-table empty-type column tests ─────────────────────────

    /// Create a temp SQLite database with a table that has an empty-type column
    /// (simulates vec0 virtual table behaviour where PRAGMA reports '' for
    /// vector columns).
    async fn create_empty_type_db() -> tempfile::TempPath {
        let tmp = tempfile::NamedTempFile::new().expect("create temp file");
        let path = tmp.into_temp_path();
        let db_path = path.to_str().unwrap().to_string();

        let conn = Connection::open(&db_path).await.expect("open temp sqlite");
        conn.call(|conn| -> Result<(), tokio_rusqlite::rusqlite::Error> {
            // Use "" as the column type to simulate vec0's PRAGMA output.
            conn.execute_batch(
                "CREATE TABLE blob_items (
                     id   INTEGER PRIMARY KEY,
                     data \"\"  -- empty type, like vec0 virtual tables
                 );",
            )?;

            // Insert packed f32 BLOBs.
            let vecs: Vec<(i64, Vec<f32>)> =
                vec![(1, vec![1.0, 0.0, 0.0]), (2, vec![0.0, 1.0, 0.0])];
            let mut stmt = conn.prepare("INSERT INTO blob_items (id, data) VALUES (?1, ?2)")?;
            for (id, vec) in &vecs {
                let blob: Vec<u8> = vec.iter().flat_map(|f| f.to_le_bytes()).collect();
                stmt.execute(tokio_rusqlite::rusqlite::params![id, blob])?;
            }
            Ok(())
        })
        .await
        .expect("seed blob_items table");
        conn.close().await.expect("close seed connection");

        path
    }

    #[tokio::test]
    #[ignore]
    async fn test_empty_type_column_read_as_binary() {
        let db_path = create_empty_type_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();

        let mut options = HashMap::new();
        options.insert("table".to_string(), "blob_items".to_string());
        register_sqlite_tables(
            &mut ctx,
            "blob_items",
            db,
            Some(&options),
            false,
            None,
            HierarchyLevel::Table,
        )
        .await
        .expect("register blob_items");

        // Read back — data column should be Binary, not Utf8
        let batches = query_all(&ctx, "SELECT id, data FROM blob_items ORDER BY id").await;
        assert_eq!(total_rows(&batches), 2);

        let schema = batches[0].schema();
        let data_field = schema.field_with_name("data").unwrap();
        assert_eq!(
            *data_field.data_type(),
            DataType::Binary,
            "empty-type column should map to Binary"
        );

        // Verify BLOB content is preserved (first row: [1.0, 0.0, 0.0])
        let data_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("data column should be BinaryArray");
        let blob = data_col.value(0);
        assert_eq!(blob.len(), 12, "3 × f32 = 12 bytes");

        let floats: Vec<f32> = blob
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        assert_eq!(floats, vec![1.0f32, 0.0, 0.0]);
    }

    #[tokio::test]
    #[ignore]
    async fn test_empty_type_column_subquery_extracts_blob() {
        let db_path = create_empty_type_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();

        let mut options = HashMap::new();
        options.insert("table".to_string(), "blob_items".to_string());
        register_sqlite_tables(
            &mut ctx,
            "blob_items",
            db,
            Some(&options),
            false,
            None,
            HierarchyLevel::Table,
        )
        .await
        .expect("register blob_items");

        // Subquery fetching a BLOB column should work (simulates the vec0
        // subquery path in sqlite_knn).
        let batches = query_all(&ctx, "SELECT data FROM blob_items WHERE id = 1").await;
        assert_eq!(total_rows(&batches), 1);

        let data_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("subquery should return BinaryArray");
        assert!(!data_col.is_null(0), "BLOB should not be null");
        assert_eq!(data_col.value(0).len(), 12);
    }

    // ─── Catalog mode tests ─────────────────────────────────────────────

    /// Create a temp SQLite database with a couple of related tables and a view so that
    /// catalog-mode tests can exercise three-part references, cross-table joins, and
    /// view registration without depending on any dynamic extensions.
    async fn create_catalog_test_db() -> tempfile::TempPath {
        let tmp = tempfile::NamedTempFile::new().expect("create temp file");
        let path = tmp.into_temp_path();
        let db_path = path.to_str().unwrap().to_string();

        let conn = Connection::open(&db_path).await.expect("open temp sqlite");
        conn.call(|conn| -> Result<(), tokio_rusqlite::rusqlite::Error> {
            conn.execute_batch(
                "CREATE TABLE users (
                     id    INTEGER PRIMARY KEY AUTOINCREMENT,
                     name  TEXT    NOT NULL
                 );
                 INSERT INTO users (name) VALUES ('alice'), ('bob'), ('carol');

                 CREATE TABLE orders (
                     id      INTEGER PRIMARY KEY AUTOINCREMENT,
                     user_id INTEGER NOT NULL,
                     amount  INTEGER NOT NULL
                 );
                 INSERT INTO orders (user_id, amount) VALUES (1, 100), (1, 50), (2, 200);

                 CREATE VIEW user_totals AS
                     SELECT u.name AS name, SUM(o.amount) AS total
                     FROM users u
                     JOIN orders o ON u.id = o.user_id
                     GROUP BY u.id, u.name;",
            )?;
            Ok(())
        })
        .await
        .expect("seed catalog db");
        conn.close().await.expect("close seed connection");

        path
    }

    #[tokio::test]
    #[ignore]
    async fn test_list_sqlite_tables_filters_internal_and_shadow_tables() {
        // AUTOINCREMENT on `users`/`orders` causes SQLite to auto-create `sqlite_sequence`,
        // which must be filtered by the `name NOT LIKE 'sqlite_%'` clause.
        let db_path = create_catalog_test_db().await;
        let db = db_path.to_str().unwrap();

        let conn = Connection::open(db).await.expect("open");
        init_connection(&conn, 5000, &[]).await.expect("init");
        let tables = list_sqlite_tables_in_catalog(&conn)
            .await
            .expect("list tables");

        let names: Vec<&str> = tables.iter().map(|(_, t)| t.as_str()).collect();
        assert!(names.contains(&"users"), "expected users in {:?}", names);
        assert!(names.contains(&"orders"), "expected orders in {:?}", names);
        assert!(
            names.contains(&"user_totals"),
            "expected user_totals view in {:?}",
            names
        );
        assert!(
            !names.iter().any(|n| n.starts_with("sqlite_")),
            "sqlite_* internal tables must be filtered: {:?}",
            names
        );

        // Every schema pair should be ("main", _).
        for (schema, _) in &tables {
            assert_eq!(schema, "main");
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_catalog_registers_all_user_tables_and_views() {
        let db_path = create_catalog_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();

        register_sqlite_tables(
            &mut ctx,
            "demo",
            db,
            None,
            false,
            None,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("register catalog");

        let users = query_all(&ctx, "SELECT id FROM demo.main.users ORDER BY id").await;
        assert_eq!(total_rows(&users), 3);

        let orders = query_all(&ctx, "SELECT id FROM demo.main.orders ORDER BY id").await;
        assert_eq!(total_rows(&orders), 3);

        // Views are registered the same way as base tables.
        let totals = query_all(
            &ctx,
            "SELECT name, total FROM demo.main.user_totals ORDER BY name",
        )
        .await;
        assert_eq!(total_rows(&totals), 2);
    }

    #[tokio::test]
    #[ignore]
    async fn test_catalog_cross_table_join() {
        let db_path = create_catalog_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();

        register_sqlite_tables(
            &mut ctx,
            "demo",
            db,
            None,
            false,
            None,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("register catalog");

        let batches = query_all(
            &ctx,
            "SELECT u.name, SUM(o.amount) AS total \
             FROM demo.main.users u \
             JOIN demo.main.orders o ON u.id = o.user_id \
             GROUP BY u.name \
             ORDER BY u.name",
        )
        .await;
        assert_eq!(total_rows(&batches), 2);
    }

    #[tokio::test]
    #[ignore]
    async fn test_catalog_excludes_sqlite_sequence() {
        let db_path = create_catalog_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();

        register_sqlite_tables(
            &mut ctx,
            "demo",
            db,
            None,
            false,
            None,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("register catalog");

        // sqlite_sequence is auto-created by AUTOINCREMENT but must never surface in the catalog.
        let result = ctx
            .sql("SELECT * FROM demo.main.sqlite_sequence")
            .await
            .and_then(|df| futures::executor::block_on(df.collect()));
        assert!(
            result.is_err(),
            "sqlite_sequence should not be registered in the catalog"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_catalog_allowed_schemas_main_includes_all() {
        let db_path = create_catalog_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();

        let mut options = HashMap::new();
        options.insert("allowed_schemas".to_string(), "main".to_string());
        register_sqlite_tables(
            &mut ctx,
            "demo",
            db,
            Some(&options),
            false,
            None,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("register catalog");

        let users = query_all(&ctx, "SELECT id FROM demo.main.users ORDER BY id").await;
        assert_eq!(total_rows(&users), 3);
    }

    #[tokio::test]
    #[ignore]
    async fn test_catalog_allowed_schemas_non_main_is_empty() {
        let db_path = create_catalog_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();

        let mut options = HashMap::new();
        options.insert("allowed_schemas".to_string(), "public".to_string());
        register_sqlite_tables(
            &mut ctx,
            "demo",
            db,
            Some(&options),
            false,
            None,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("register catalog");

        // The catalog is registered but no tables are attached — any reference should
        // fail at planning time.
        let result = ctx.sql("SELECT * FROM demo.main.users").await;
        assert!(
            result.is_err(),
            "allowed_schemas=public should register no tables under SQLite's 'main' schema"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_catalog_read_write_mode_allows_inserts() {
        let db_path = create_catalog_test_db().await;
        let db = db_path.to_str().unwrap();
        let mut ctx = SessionContext::new();

        register_sqlite_tables(
            &mut ctx,
            "demo",
            db,
            None,
            true,
            None,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("register catalog rw");

        ctx.sql("INSERT INTO demo.main.users (name) VALUES ('dave')")
            .await
            .expect("parse insert")
            .collect()
            .await
            .expect("execute insert");

        let batches = query_all(&ctx, "SELECT name FROM demo.main.users WHERE name = 'dave'").await;
        assert_eq!(total_rows(&batches), 1);
    }

    #[tokio::test]
    #[ignore]
    async fn test_catalog_empty_database_is_ok() {
        let tmp = tempfile::NamedTempFile::new().expect("create temp file");
        let path = tmp.into_temp_path();
        let db = path.to_str().unwrap();

        // Materialize an empty database file.
        let conn = Connection::open(db).await.expect("open empty db");
        conn.close().await.expect("close empty db");

        let mut ctx = SessionContext::new();
        // An empty database should register successfully (with a warning) rather than erroring.
        register_sqlite_tables(
            &mut ctx,
            "demo",
            db,
            None,
            false,
            None,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("register empty catalog");
    }

    #[tokio::test]
    #[ignore]
    async fn test_catalog_shares_single_read_pool() {
        // Catalog mode should allocate exactly `read_pool_size` read connections total,
        // not `read_pool_size * table_count`. We verify this indirectly by observing
        // that each registered provider holds Arc-clones of the *same* underlying
        // connections — i.e. the Arc pointers overlap across providers.
        let db_path = create_catalog_test_db().await;
        let db = db_path.to_str().unwrap();

        let (read_pool, write_conn) = open_sqlite_pool(db, 5000, 2, false, &[])
            .await
            .expect("open shared pool");
        assert_eq!(read_pool.len(), 2);
        assert!(write_conn.is_none());

        let users_provider = SqliteTableProvider::from_shared_pool(
            read_pool.clone(),
            None,
            TableReference::bare("users"),
            false,
        )
        .await
        .expect("users provider");

        let orders_provider = SqliteTableProvider::from_shared_pool(
            read_pool.clone(),
            None,
            TableReference::bare("orders"),
            false,
        )
        .await
        .expect("orders provider");

        // Both providers should reference the *same* underlying connections, not fresh ones.
        for (u, o) in users_provider
            .read_pool
            .iter()
            .zip(orders_provider.read_pool.iter())
        {
            assert!(
                Arc::ptr_eq(u, o),
                "catalog providers must share the same connections"
            );
        }
    }
}
