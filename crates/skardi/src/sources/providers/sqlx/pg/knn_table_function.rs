//! Table function for pgvector KNN search.
//!
//! Usage:
//! ```sql
//! -- Literal vector (inner product, default)
//! SELECT * FROM pg_knn('table_name', 'embedding', [0.1, 0.2, ...])
//!
//! -- Subquery vector (find items similar to a stored embedding)
//! SELECT * FROM pg_knn('table_name', 'embedding',
//!     (SELECT embedding FROM other_table WHERE id = {id}))
//!
//! -- Distance metric (4th) and k (5th) are required
//! SELECT * FROM pg_knn('table_name', 'embedding', [0.1, 0.2, ...], '<=>', 10)
//!
//! -- With optional inline WHERE filter (6th argument)
//! SELECT * FROM pg_knn('table_name', 'embedding', [0.1, 0.2, ...], '<->', 5, 'dataset_id = ''abc''')
//!
//! -- Additional WHERE clause (pushed down into the Postgres query)
//! SELECT * FROM pg_knn('table_name', 'embedding', [0.1, 0.2, ...])
//! WHERE collection_id = 'xyz'
//! ```
//!
//! Returns all non-vector columns from the table plus `_score Float64`.
//! The score is the raw pgvector distance value for the chosen metric — lower is more similar
//! (for `inner_product` the score is negative).

use arrow::array::{Array, Float32Array, Float64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{Result as DFResult, ScalarValue, plan_err};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::PostgreSqlDialect;
use sqlx::PgPool;
use sqlx::Row;
use std::any::Any;
use std::sync::Arc;

use super::knn_exec::{DistanceMetric, PgKnnExec, PgVectorFetchExec};
use super::utils::expr_to_pg_sql;
use crate::sources::providers::knn_utils::MAX_KNN_K;
use crate::sources::providers::{DatasetEntry, DatasetRegistry};

/// Entry stored in the registry for each registered Postgres table.
#[derive(Clone, Debug)]
pub struct PgKnnEntry {
    /// Connection pool for this table.
    pub pool: Arc<PgPool>,
    /// Fully-qualified table identifier for SQL (e.g. `"public"."modeldata"`).
    pub qualified_table: String,
    /// All non-vector columns and their Arrow types.
    /// Populated at registration time from `information_schema.columns`.
    pub columns: Vec<(String, DataType)>,
}

// ─── TableFunctionImpl ───────────────────────────────────────────────────────

/// Table function that performs pgvector KNN search.
#[derive(Debug)]
pub struct PgKnnTableFunction {
    registry: DatasetRegistry,
}

impl PgKnnTableFunction {
    pub fn new(registry: DatasetRegistry) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for PgKnnTableFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if exprs.len() < 5 || exprs.len() > 6 {
            return plan_err!(
                "pg_knn(table, vector_col, query_vec, metric, k [, filter]) expects 5-6 arguments, got {}. \
                 metric must be one of: <#> (inner product), <-> (L2), <=> (cosine)",
                exprs.len()
            );
        }

        let table_name = extract_string(&exprs[0], "table")?;
        let vector_col = extract_string(&exprs[1], "vector_col")?;

        let metric = {
            let s = extract_string(&exprs[3], "metric")?;
            s.parse::<DistanceMetric>()
                .map_err(datafusion::error::DataFusionError::Plan)?
        };

        let k = extract_k(&exprs[4])?;

        let inline_filter = if exprs.len() == 6 {
            extract_string(&exprs[5], "filter").ok()
        } else {
            None
        };

        // Look up pool + columns from registry.
        let entry = {
            let reg = self.registry.read().map_err(|e| {
                datafusion::error::DataFusionError::Internal(format!(
                    "pg_knn registry lock error: {}",
                    e
                ))
            })?;
            let raw = reg.get(&table_name).cloned().ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!(
                    "pg_knn: table '{}' not found in registry. \
                     Make sure the data source is declared with type 'postgres'.",
                    table_name
                ))
            })?;
            match raw {
                DatasetEntry::Postgres(e) => e,
                _ => return plan_err!("pg_knn: table '{}' is not a Postgres dataset", table_name),
            }
        };

        // Try to extract a literal vector. If arg[2] is a scalar subquery, unparse it
        // to SQL so the vector can be fetched directly via sqlx at execution time
        // (bypassing datafusion-table-providers, which can't decode the `vector` type).
        let literal_vector = extract_vector(&exprs[2]).ok();
        let query_vector_sql = if literal_vector.is_none() {
            build_vector_fetch_sql(&exprs[2]).ok()
        } else {
            None
        };

        // Build output schema: all non-vector columns + _score.
        let mut fields: Vec<Field> = entry
            .columns
            .iter()
            .filter(|(name, _)| name != &vector_col)
            .map(|(name, dtype)| Field::new(name.clone(), dtype.clone(), true))
            .collect();
        fields.push(Field::new("_score", DataType::Float64, true));
        let schema: SchemaRef = Arc::new(Schema::new(fields));

        Ok(Arc::new(PgKnnProvider {
            pool: entry.pool,
            qualified_table: entry.qualified_table,
            vector_col,
            literal_vector,
            query_vector_sql,
            inline_filter,
            schema,
            metric,
            k,
        }))
    }
}

// ─── TableProvider ───────────────────────────────────────────────────────────

struct PgKnnProvider {
    pool: Arc<PgPool>,
    qualified_table: String,
    vector_col: String,
    /// Pre-computed query vector for the literal path. `None` for the subquery path.
    literal_vector: Option<Vec<f32>>,
    /// SQL that fetches the query vector as pgvector text (subquery path).
    /// Executed directly via sqlx; `None` when using the literal path.
    query_vector_sql: Option<String>,
    inline_filter: Option<String>,
    schema: SchemaRef,
    metric: DistanceMetric,
    k: usize,
}

impl std::fmt::Debug for PgKnnProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgKnnProvider")
            .field("qualified_table", &self.qualified_table)
            .field("vector_col", &self.vector_col)
            .field(
                "vector_source",
                if self.literal_vector.is_some() {
                    &"literal"
                } else {
                    &"subquery"
                },
            )
            .finish()
    }
}

#[async_trait]
impl TableProvider for PgKnnProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Return Exact only for filters we can successfully convert to Postgres SQL.
        // Unsupported means DataFusion will re-apply the filter on the result set.
        Ok(filters
            .iter()
            .map(|expr| {
                if expr_to_pg_sql(expr).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Combine inline filter (5th argument) with WHERE-clause filters.
        // Only filters that expr_to_pg_sql can convert are pushed down (matches supports_filters_pushdown).
        let mut parts: Vec<String> = Vec::new();
        if let Some(ref f) = self.inline_filter {
            parts.push(f.clone());
        }
        for expr in filters {
            if let Some(sql) = expr_to_pg_sql(expr) {
                parts.push(sql);
            }
        }
        let filter = if parts.is_empty() {
            None
        } else {
            Some(parts.join(" AND "))
        };

        // Build schema respecting column projection.
        let schema = if let Some(proj) = projection {
            let fields: Vec<Field> = proj.iter().map(|&i| self.schema.field(i).clone()).collect();
            Arc::new(Schema::new(fields))
        } else {
            self.schema.clone()
        };

        let mut exec: PgKnnExec = if let Some(ref vec) = self.literal_vector {
            // Literal path — vector known at planning time.
            PgKnnExec::new(
                Arc::clone(&self.pool),
                self.qualified_table.clone(),
                self.vector_col.clone(),
                vec.clone(),
                filter,
                schema,
                self.metric,
                self.k,
            )
        } else if let Some(ref sql) = self.query_vector_sql {
            // Subquery path — SQL was already unparsed at planning time.
            // Use PgVectorFetchExec (fetches `embedding::text` via sqlx) as the
            // child, so knn_utils::extract_query_vector can parse the pgvector
            // text format without hitting datafusion-table-providers' inability
            // to decode the `vector` Postgres OID.
            let fetch_exec = Arc::new(PgVectorFetchExec::new(Arc::clone(&self.pool), sql.clone()));
            PgKnnExec::new_with_subquery(
                Arc::clone(&self.pool),
                self.qualified_table.clone(),
                self.vector_col.clone(),
                fetch_exec,
                filter,
                schema,
                self.metric,
                self.k,
            )
        } else {
            return plan_err!(
                "pg_knn: query_vec must be a literal array or a scalar subquery, \
                 e.g. (SELECT embedding FROM t WHERE id = {{id}})"
            );
        };

        // Apply scan limit if provided (e.g., from SQL LIMIT clause).
        // This slices the result after the KNN search, matching lance_knn behaviour.
        // The Postgres LIMIT (k) is preserved so HNSW/IVFFlat indexes are used.
        if let Some(n) = limit {
            exec = exec.with_scan_limit(n);
        }

        Ok(Arc::new(exec))
    }
}

// ─── Subquery SQL builder ─────────────────────────────────────────────────────

/// Unparse a scalar subquery expression to a SQL string that fetches the vector
/// column as pgvector text, so it can be executed directly via sqlx.
///
/// The returned SQL wraps the subquery and casts its first column to `text`:
/// ```sql
/// SELECT "<col>"::text FROM (<inner>) AS _knn_subq LIMIT 1
/// ```
fn build_vector_fetch_sql(expr: &Expr) -> DFResult<String> {
    let Expr::ScalarSubquery(subquery) = expr else {
        return Err(datafusion::error::DataFusionError::Plan(
            "pg_knn: expected a scalar subquery for the vector argument".to_string(),
        ));
    };

    let col_name = subquery
        .subquery
        .schema()
        .field(0)
        .name()
        .replace('"', "\"\"");

    let unparser = Unparser::new(&PostgreSqlDialect {});
    let inner_sql = unparser
        .plan_to_sql(subquery.subquery.as_ref())
        .map_err(|e| {
            datafusion::error::DataFusionError::Plan(format!(
                "pg_knn: failed to unparse vector subquery: {e}"
            ))
        })?
        .to_string();

    Ok(format!(
        "SELECT \"{col_name}\"::text FROM ({inner_sql}) AS _knn_subq LIMIT 1"
    ))
}

// ─── Registration ────────────────────────────────────────────────────────────

/// Register the `pg_knn` table function with the DataFusion session.
pub fn register_pg_knn_udtf(ctx: &datafusion::prelude::SessionContext, registry: DatasetRegistry) {
    ctx.register_udtf("pg_knn", Arc::new(PgKnnTableFunction::new(registry)));
}

// ─── Schema inference ────────────────────────────────────────────────────────

/// Query `information_schema.columns` and return the column list
/// (name, Arrow DataType) for a given table.
///
/// Columns of type `USER-DEFINED` (pgvector's `vector`) are included with
/// `DataType::Utf8` as a placeholder — callers filter them out by name.
pub async fn fetch_table_columns(
    pool: &PgPool,
    schema_name: &str,
    table_name: &str,
) -> anyhow::Result<Vec<(String, DataType)>> {
    let rows = sqlx::query(
        "SELECT column_name, data_type, udt_name, numeric_precision, numeric_scale \
         FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = $2 \
         ORDER BY ordinal_position",
    )
    .bind(schema_name)
    .bind(table_name)
    .fetch_all(pool)
    .await?;

    let mut columns = Vec::new();
    for row in &rows {
        let col_name: String = row.try_get("column_name")?;
        let data_type: String = row.try_get("data_type")?;
        let udt_name: String = row.try_get("udt_name")?;
        let precision: Option<i32> = row.try_get("numeric_precision")?;
        let scale: Option<i32> = row.try_get("numeric_scale")?;

        let arrow_type = pg_type_to_arrow(&data_type, &udt_name, precision, scale);
        columns.push((col_name, arrow_type));
    }

    Ok(columns)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{DFSchema, Spans};
    use datafusion::logical_expr::{EmptyRelation, LogicalPlan, logical_plan::Subquery};

    fn subquery_expr_with_col(col_name: &str) -> Expr {
        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            col_name,
            DataType::Utf8,
            true,
        )]));
        let df_schema = Arc::new(DFSchema::try_from(arrow_schema).unwrap());
        let plan = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: df_schema,
        }));
        Expr::ScalarSubquery(Subquery {
            subquery: plan,
            outer_ref_columns: vec![],
            spans: Spans::default(),
        })
    }

    #[test]
    fn test_build_vector_fetch_sql_normal_col_name() {
        let expr = subquery_expr_with_col("embedding");
        let sql = build_vector_fetch_sql(&expr).unwrap();
        assert!(sql.contains("\"embedding\"::text"), "sql={sql}");
        assert!(sql.contains("AS _knn_subq LIMIT 1"), "sql={sql}");
    }

    #[test]
    fn test_build_vector_fetch_sql_col_name_with_embedded_quote() {
        // A column name containing " must be doubled inside the SQL identifier.
        let expr = subquery_expr_with_col("weird\"col");
        let sql = build_vector_fetch_sql(&expr).unwrap();
        // weird"col → "weird""col"
        assert!(
            sql.contains("\"weird\"\"col\"::text"),
            "embedded quote must be doubled; sql={sql}"
        );
    }

    #[test]
    fn test_build_vector_fetch_sql_non_subquery_returns_error() {
        let expr = Expr::Column(datafusion::common::Column::new_unqualified(
            "not_a_subquery",
        ));
        assert!(build_vector_fetch_sql(&expr).is_err());
    }
}

fn pg_type_to_arrow(
    data_type: &str,
    udt_name: &str,
    numeric_precision: Option<i32>,
    numeric_scale: Option<i32>,
) -> DataType {
    // pgvector type — keep as placeholder; caller excludes by column name.
    if udt_name == "vector" || udt_name == "halfvec" {
        return DataType::Utf8;
    }

    match data_type {
        "smallint" | "integer" => DataType::Int32,
        "bigint" => DataType::Int64,
        "real" => DataType::Float32,
        "double precision" => DataType::Float64,
        // Use Decimal128 to match what datafusion-table-providers returns when reading.
        // Fall back to (38, 10) when precision/scale are not specified (unbound NUMERIC).
        "numeric" | "decimal" => {
            let p = numeric_precision.unwrap_or(38) as u8;
            let s = numeric_scale.unwrap_or(10) as i8;
            DataType::Decimal128(p, s)
        }
        "boolean" => DataType::Boolean,
        // Everything else (text, varchar, uuid, json, jsonb, timestamp, date, …)
        _ => DataType::Utf8,
    }
}

// ─── Argument extraction helpers ─────────────────────────────────────────────

fn extract_k(expr: &Expr) -> DFResult<usize> {
    let k = match expr {
        Expr::Literal(ScalarValue::Int64(Some(n)), _) => Ok(*n as usize),
        Expr::Literal(ScalarValue::Int32(Some(n)), _) => Ok(*n as usize),
        Expr::Literal(ScalarValue::UInt64(Some(n)), _) => Ok(*n as usize),
        _ => plan_err!("pg_knn: k must be a positive integer literal"),
    }?;
    if k == 0 || k > MAX_KNN_K {
        return plan_err!("pg_knn: k must be between 1 and {MAX_KNN_K}, got {k}");
    }
    Ok(k)
}

fn extract_string(expr: &Expr, name: &str) -> DFResult<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Ok(s.clone()),
        Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => Ok(s.clone()),
        _ => plan_err!("pg_knn: '{}' must be a string literal", name),
    }
}

fn extract_vector(expr: &Expr) -> DFResult<Vec<f32>> {
    let values: Arc<dyn arrow::array::Array> = match expr {
        Expr::Literal(ScalarValue::List(arr), _) => {
            if arr.is_empty() {
                return plan_err!("pg_knn: query_vec must not be empty");
            }
            arr.value(0)
        }
        Expr::Literal(ScalarValue::FixedSizeList(arr), _) => {
            if arr.is_empty() {
                return plan_err!("pg_knn: query_vec must not be empty");
            }
            arr.value(0)
        }
        _ => return plan_err!("pg_knn: query_vec must be a literal array (e.g. [0.1, 0.2, ...])"),
    };

    if let Some(f32_arr) = values.as_any().downcast_ref::<Float32Array>() {
        return Ok(f32_arr.values().to_vec());
    }
    if let Some(f64_arr) = values.as_any().downcast_ref::<Float64Array>() {
        return Ok(f64_arr.values().iter().map(|&v| v as f32).collect());
    }

    plan_err!("pg_knn: query_vec elements must be Float32 or Float64")
}
