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
use crate::sources::providers::mysql_wire::parse_mysql_wire_connection_params;
use crate::sources::providers::{DatasetEntry, DatasetRegistry};
use anyhow::{Context, Result};
use arrow::array::{RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{Constraints, ScalarValue, plan_err};
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
use datafusion_table_providers::mysql::write::MySQLTableWriter;
use datafusion_table_providers::mysql::{DynMySQLConnectionPool, MySQL};
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::mysqlconn::MySQLConnection;
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use datafusion_table_providers::sql::sql_provider_datafusion::SqlTable;
use futures::stream;
use mysql_async::prelude::Queryable;
use mysql_async::{Params, Row, Value};
use secrecy::SecretString;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// Default SeekDB port (OceanBase MySQL-compatible endpoint).
const DEFAULT_SEEKDB_PORT: u16 = 2881;

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
                // The pool is bound to a single default database, but catalog
                // enumeration spans every non-system schema — so the qualified
                // reference must carry the schema explicitly, otherwise SHOW
                // COLUMNS and SqlTable's generated SQL fall back to the
                // connection's default DB and either fail or hit the wrong
                // table.
                let table_reference = TableReference::partial(schema_c.as_str(), table_c.as_str());
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
                        qualified_table: quote_seekdb_table(&table_reference),
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
///
/// Unlike the vanilla MySQL provider, we resolve the table's Arrow schema
/// ourselves via [`resolve_seekdb_schema`] and inject it into
/// [`SqlTable::new_with_schema`]. That skips the upstream factory's auto
/// schema resolution, which chokes on SeekDB's native `VECTOR(N)` columns
/// because datafusion-table-providers has no MySQL-type mapping for them.
/// The VECTOR column stays queryable through `seekdb_knn`.
async fn build_seekdb_table_provider(
    pool: Arc<MySQLConnectionPool>,
    table_reference: TableReference,
    read_write: bool,
) -> Result<Arc<dyn TableProvider>> {
    let filtered_schema = resolve_seekdb_schema(&pool, &table_reference)
        .await
        .with_context(|| format!("Failed to resolve SeekDB schema for '{}'", table_reference))?;

    let dyn_pool: Arc<DynMySQLConnectionPool> = Arc::clone(&pool) as Arc<DynMySQLConnectionPool>;

    let read_provider: Arc<dyn TableProvider> = Arc::new(
        SqlTable::new_with_schema(
            "seekdb",
            &dyn_pool,
            Arc::clone(&filtered_schema),
            table_reference.clone(),
        )
        .with_dialect(Arc::new(MySqlDialect {})),
    );

    let inner: Arc<dyn TableProvider> = if read_write {
        let mysql_write = MySQL::new(
            table_reference.to_string(),
            Arc::clone(&pool),
            Arc::clone(&filtered_schema),
            Constraints::default(),
        );
        MySQLTableWriter::create(read_provider, mysql_write, None)
    } else {
        read_provider
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

/// Introspect a SeekDB table's columns via `SHOW COLUMNS` and return an Arrow
/// [`SchemaRef`] that excludes columns we cannot map: the native `VECTOR(N)`
/// type (surfaced only via `seekdb_knn`) and any novel MySQL types outside
/// [`mysql_type_to_arrow`]'s coverage.
async fn resolve_seekdb_schema(
    pool: &MySQLConnectionPool,
    table_reference: &TableReference,
) -> Result<SchemaRef> {
    let db_conn = pool
        .connect()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to SeekDB for schema resolution: {e}"))?;

    let mysql_conn = db_conn
        .as_any()
        .downcast_ref::<MySQLConnection>()
        .ok_or_else(|| {
            anyhow::anyhow!("Unexpected MySQL connection type during SeekDB schema resolution")
        })?;

    let mut conn = mysql_conn.conn.lock().await;

    let qualified = quote_seekdb_table(table_reference);
    let query = format!("SHOW COLUMNS FROM {qualified}");
    let rows: Vec<(Option<String>, Option<String>)> = conn
        .query_map(query.as_str(), |row: Row| {
            let field: Option<String> = row.get("Field");
            let ty: Option<String> = row.get("Type");
            (field, ty)
        })
        .await
        .with_context(|| {
            format!(
                "Failed to introspect SeekDB columns for '{}'",
                table_reference
            )
        })?;

    build_schema_from_columns(table_reference, rows)
}

/// Build a filtered Arrow [`SchemaRef`] from a `SHOW COLUMNS` result.
///
/// Pure helper (no I/O) so the VECTOR-to-Utf8 coercion and unsupported-type
/// skipping can be unit-tested without a live SeekDB. Returns an error if any
/// row is missing a `Field` or `Type` — those columns are `NOT NULL` on MySQL-
/// compatible servers, so empty values indicate a protocol-level mismatch we
/// want to surface loudly rather than swallow into a bogus schema.
fn build_schema_from_columns(
    table_reference: &TableReference,
    rows: Vec<(Option<String>, Option<String>)>,
) -> Result<SchemaRef> {
    let mut fields: Vec<Field> = Vec::with_capacity(rows.len());
    for (name, ty) in rows {
        let name = name.filter(|s| !s.is_empty()).ok_or_else(|| {
            anyhow::anyhow!(
                "SeekDB SHOW COLUMNS returned a row with empty 'Field' for table '{}'",
                table_reference
            )
        })?;
        let ty = ty
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow::anyhow!(
                "SeekDB SHOW COLUMNS returned a row with empty 'Type' for column '{}' in table '{}'",
                name,
                table_reference
            ))?;
        let ty_lc = ty.to_lowercase();
        // SeekDB's native `VECTOR(N)` has no Arrow mapping in
        // datafusion-table-providers, but dropping the column entirely breaks
        // scalar-subquery references like
        // `(SELECT embedding FROM docs WHERE id = {seed_id})` used by the KNN
        // by-seed pipeline: the SQL planner cannot resolve `embedding` against
        // a schema where it doesn't exist, `sql_expr_to_logical_expr` errors,
        // and DataFusion silently drops the offending table-function argument
        // (see datafusion-sql/src/relation/mod.rs `flat_map` over args).
        // Match pg_knn's pgvector handling: surface VECTOR as Utf8. SeekDB
        // serialises vectors as pgvector-style string literals
        // (`'[1.0, 0.0, 0.0, 0.0]'`), so a plain `SELECT embedding` decodes
        // cleanly; real distance queries go through `seekdb_knn`.
        let arrow_ty = if ty_lc.starts_with("vector") {
            DataType::Utf8
        } else if let Some(ty) = mysql_type_to_arrow(&ty_lc) {
            ty
        } else {
            tracing::warn!(
                "SeekDB table '{}': skipping column '{}' with unmapped MySQL type '{}'",
                table_reference,
                name,
                ty
            );
            continue;
        };
        // Declare every column nullable, matching datafusion-table-providers'
        // MySQL backend. An AUTO_INCREMENT / DEFAULT column is omitted from
        // INSERT column lists, so DataFusion materialises NULL into the
        // corresponding Arrow column before handing the batch to the MySQL
        // writer. If we narrowed nullability to the DB's NOT NULL hint, that
        // INSERT batch would fail schema validation even though MySQL itself
        // would supply the default on the server side.
        fields.push(Field::new(&name, arrow_ty, true));
    }

    if fields.is_empty() {
        anyhow::bail!(
            "SeekDB table '{}' has no Arrow-mappable columns (VECTOR and unsupported types excluded)",
            table_reference
        );
    }

    Ok(Arc::new(Schema::new(fields)))
}

/// Map a MySQL-compatible column-type string (as returned by `SHOW COLUMNS`:
/// e.g. `int(11)`, `varchar(100)`, `bigint unsigned`, `decimal(10,2)`) to an
/// Arrow [`DataType`]. Returns `None` for types outside our coverage so the
/// caller can log + skip rather than panic.
fn mysql_type_to_arrow(type_lc: &str) -> Option<DataType> {
    let unsigned = type_lc.contains("unsigned");
    let head = type_lc
        .split_once('(')
        .map(|(h, _)| h)
        .unwrap_or(type_lc)
        .split_whitespace()
        .next()?
        .trim();
    match head {
        "bool" | "boolean" => Some(DataType::Boolean),
        "bit" => {
            if type_lc.starts_with("bit(1)") || type_lc == "bit" {
                Some(DataType::Boolean)
            } else {
                Some(DataType::Binary)
            }
        }
        // MySQL convention: tinyint(1) is a boolean.
        "tinyint" if type_lc.starts_with("tinyint(1)") => Some(DataType::Boolean),
        "tinyint" => Some(if unsigned {
            DataType::UInt8
        } else {
            DataType::Int8
        }),
        "smallint" => Some(if unsigned {
            DataType::UInt16
        } else {
            DataType::Int16
        }),
        "mediumint" | "int" | "integer" => Some(if unsigned {
            DataType::UInt32
        } else {
            DataType::Int32
        }),
        "bigint" => Some(if unsigned {
            DataType::UInt64
        } else {
            DataType::Int64
        }),
        "float" => Some(DataType::Float32),
        "double" | "real" => Some(DataType::Float64),
        "decimal" | "numeric" | "newdecimal" => {
            let (p, s) = parse_decimal_args(type_lc).unwrap_or((10, 0));
            Some(DataType::Decimal128(p, s))
        }
        "varchar" | "char" | "text" | "tinytext" | "mediumtext" | "longtext" | "enum" | "set"
        | "json" => Some(DataType::Utf8),
        "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => {
            Some(DataType::Binary)
        }
        "date" | "newdate" => Some(DataType::Date32),
        "datetime" | "timestamp" => Some(DataType::Timestamp(TimeUnit::Microsecond, None)),
        "time" => Some(DataType::Time64(TimeUnit::Microsecond)),
        "year" => Some(DataType::Int16),
        _ => None,
    }
}

/// Parse `(P,S)` out of a MySQL decimal-like declaration. Clamps precision
/// into Arrow's `Decimal128` range (1..=38) and keeps scale within `[0, p]`.
fn parse_decimal_args(type_lc: &str) -> Option<(u8, i8)> {
    let args = type_lc.split_once('(')?.1.split_once(')')?.0;
    let mut it = args.split(',').map(|s| s.trim().parse::<u32>().ok());
    let p = it.next()??;
    let s = it.next().unwrap_or(Some(0))?;
    let p = p.clamp(1, 38) as u8;
    let s = (s as i8).clamp(0, p as i8);
    Some((p, s))
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

/// Extract a string-literal argument from a table-function `Expr`. Shared by
/// `seekdb_fts` and `seekdb_knn` so they can emit consistent error messages
/// prefixed with the caller's UDTF name (`fn_name`). NULL is accepted as a
/// placeholder during pipeline schema inference and returns the empty string.
pub(crate) fn extract_string_arg(
    expr: &Expr,
    fn_name: &str,
    arg: &str,
) -> DataFusionResult<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => Ok(s.clone()),
        Expr::Literal(ScalarValue::Null, _) => Ok(String::new()),
        _ => plan_err!("{fn_name}: '{arg}' must be a string literal"),
    }
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
            // Go through the pool (not `connect_direct`) so DELETE / UPDATE
            // statements return their connection for reuse. `connect_direct`
            // opens a fresh MySQL connection per call and never releases it,
            // which leaks FDs under any write-heavy workload. FTS and KNN use
            // the same pooled path.
            let conn_obj = pool.connect().await.map_err(|e| {
                DataFusionError::Execution(format!("SeekDB DML connect error: {e}"))
            })?;
            let mysql_conn = conn_obj
                .as_any()
                .downcast_ref::<MySQLConnection>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "SeekDB DML: unexpected connection type from pool".to_string(),
                    )
                })?;
            let mut conn = mysql_conn.conn.lock().await;
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
/// [`MySQLConnectionPool`]. Delegates to the shared MySQL wire-protocol
/// parser with SeekDB's default port (2881).
pub(crate) fn parse_connection_params(
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
) -> Result<HashMap<String, SecretString>> {
    parse_mysql_wire_connection_params(connection_string, options, DEFAULT_SEEKDB_PORT, "SeekDB")
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
            DEFAULT_SEEKDB_PORT.to_string(),
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

    // ─── mysql_type_to_arrow / resolve_seekdb_schema tests ───────────────────

    #[test]
    fn mysql_type_to_arrow_integers() {
        assert_eq!(mysql_type_to_arrow("int(11)"), Some(DataType::Int32));
        assert_eq!(
            mysql_type_to_arrow("int(11) unsigned"),
            Some(DataType::UInt32)
        );
        assert_eq!(mysql_type_to_arrow("bigint"), Some(DataType::Int64));
        assert_eq!(
            mysql_type_to_arrow("bigint unsigned"),
            Some(DataType::UInt64)
        );
        assert_eq!(mysql_type_to_arrow("smallint"), Some(DataType::Int16));
        assert_eq!(mysql_type_to_arrow("tinyint(4)"), Some(DataType::Int8));
    }

    #[test]
    fn mysql_type_to_arrow_boolean_flavors() {
        // MySQL convention: tinyint(1) is a boolean even though SHOW COLUMNS
        // reports it as tinyint.
        assert_eq!(mysql_type_to_arrow("tinyint(1)"), Some(DataType::Boolean));
        assert_eq!(mysql_type_to_arrow("boolean"), Some(DataType::Boolean));
        assert_eq!(mysql_type_to_arrow("bit(1)"), Some(DataType::Boolean));
        assert_eq!(mysql_type_to_arrow("bit(8)"), Some(DataType::Binary));
    }

    #[test]
    fn mysql_type_to_arrow_text_and_binary() {
        assert_eq!(mysql_type_to_arrow("varchar(100)"), Some(DataType::Utf8));
        assert_eq!(mysql_type_to_arrow("text"), Some(DataType::Utf8));
        assert_eq!(mysql_type_to_arrow("longtext"), Some(DataType::Utf8));
        assert_eq!(mysql_type_to_arrow("json"), Some(DataType::Utf8));
        assert_eq!(mysql_type_to_arrow("varbinary(16)"), Some(DataType::Binary));
        assert_eq!(mysql_type_to_arrow("blob"), Some(DataType::Binary));
    }

    #[test]
    fn mysql_type_to_arrow_decimal_parses_precision_scale() {
        assert_eq!(
            mysql_type_to_arrow("decimal(10,2)"),
            Some(DataType::Decimal128(10, 2))
        );
        // No args → MySQL default (10, 0).
        assert_eq!(
            mysql_type_to_arrow("decimal"),
            Some(DataType::Decimal128(10, 0))
        );
        // Only precision.
        assert_eq!(
            mysql_type_to_arrow("decimal(7)"),
            Some(DataType::Decimal128(7, 0))
        );
        // Clamp precision to Arrow's 1..=38 window.
        assert_eq!(
            mysql_type_to_arrow("decimal(65,10)"),
            Some(DataType::Decimal128(38, 10))
        );
    }

    #[test]
    fn mysql_type_to_arrow_dates_and_times() {
        assert_eq!(mysql_type_to_arrow("date"), Some(DataType::Date32));
        assert_eq!(
            mysql_type_to_arrow("datetime"),
            Some(DataType::Timestamp(TimeUnit::Microsecond, None))
        );
        assert_eq!(
            mysql_type_to_arrow("timestamp"),
            Some(DataType::Timestamp(TimeUnit::Microsecond, None))
        );
        assert_eq!(
            mysql_type_to_arrow("time"),
            Some(DataType::Time64(TimeUnit::Microsecond))
        );
    }

    #[test]
    fn mysql_type_to_arrow_rejects_vector_and_unknown() {
        // VECTOR is filtered out at the caller (resolve_seekdb_schema), but
        // mysql_type_to_arrow itself returns None for it — callers log + skip.
        assert_eq!(mysql_type_to_arrow("vector(4)"), None);
        assert_eq!(mysql_type_to_arrow("vector(1536)"), None);
        assert_eq!(mysql_type_to_arrow("geometry"), None);
        assert_eq!(mysql_type_to_arrow("some_oddball_type"), None);
    }

    // ─── build_schema_from_columns ───────────────────────────────────────────

    fn col(name: &str, ty: &str) -> (Option<String>, Option<String>) {
        (Some(name.to_string()), Some(ty.to_string()))
    }

    #[test]
    fn build_schema_maps_vector_to_utf8() {
        let tr = TableReference::bare("docs");
        let rows = vec![
            col("id", "int(11)"),
            col("title", "varchar(200)"),
            col("embedding", "VECTOR(1536)"),
        ];
        let schema = build_schema_from_columns(&tr, rows).unwrap();
        let embedding = schema.field_with_name("embedding").unwrap();
        assert_eq!(
            embedding.data_type(),
            &DataType::Utf8,
            "VECTOR(N) must surface as Utf8 so subquery references resolve"
        );
        assert!(embedding.is_nullable());
    }

    #[test]
    fn build_schema_skips_unmapped_types() {
        let tr = TableReference::bare("t");
        let rows = vec![col("id", "int(11)"), col("geom", "geometry")];
        let schema = build_schema_from_columns(&tr, rows).unwrap();
        assert!(schema.field_with_name("id").is_ok());
        assert!(
            schema.field_with_name("geom").is_err(),
            "unmapped MySQL type should be dropped with a warning"
        );
    }

    #[test]
    fn build_schema_all_columns_nullable() {
        let tr = TableReference::bare("t");
        let rows = vec![col("id", "int(11)"), col("name", "varchar(100)")];
        let schema = build_schema_from_columns(&tr, rows).unwrap();
        for f in schema.fields() {
            assert!(f.is_nullable(), "field {} should be nullable", f.name());
        }
    }

    #[test]
    fn build_schema_bails_on_empty_field() {
        let tr = TableReference::bare("t");
        let rows = vec![(Some(String::new()), Some("int(11)".into()))];
        let err = build_schema_from_columns(&tr, rows)
            .unwrap_err()
            .to_string();
        assert!(err.contains("empty 'Field'"), "got: {err}");
    }

    #[test]
    fn build_schema_bails_on_missing_type() {
        let tr = TableReference::bare("t");
        let rows = vec![(Some("id".into()), None)];
        let err = build_schema_from_columns(&tr, rows)
            .unwrap_err()
            .to_string();
        assert!(err.contains("empty 'Type'"), "got: {err}");
    }

    #[test]
    fn build_schema_bails_when_every_column_is_unmapped() {
        let tr = TableReference::bare("t");
        let rows = vec![col("g", "geometry"), col("p", "polygon")];
        let err = build_schema_from_columns(&tr, rows)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no Arrow-mappable columns"), "got: {err}");
    }
}
