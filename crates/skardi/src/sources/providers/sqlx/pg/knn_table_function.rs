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

use super::dialect::GovernedPostgresDialect;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{Result as DFResult, plan_err};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::unparser::Unparser;
use sqlx::PgPool;
use sqlx::Row;
use std::any::Any;
use std::sync::Arc;

use super::knn_exec::{DistanceMetric, PgKnnExec, PgVectorFetchExec};
use super::utils::expr_to_pg_sql;
use crate::sources::providers::knn_utils::{extract_k, extract_literal_vector};
use crate::sources::providers::udtf_args::{optional_string_arg, strict_string_arg};
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
    /// The same measured overlap budget as the registered table scan.
    pub governed_array_key_budget: Option<usize>,
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

        let table_name = strict_string_arg(&exprs[0], "pg_knn", "table")?;
        let vector_col = strict_string_arg(&exprs[1], "pg_knn", "vector_col")?;

        let metric = {
            let s = strict_string_arg(&exprs[3], "pg_knn", "metric")?;
            s.parse::<DistanceMetric>()
                .map_err(datafusion::error::DataFusionError::Plan)?
        };

        let k = extract_k(&exprs[4], "pg_knn")?;

        // NULL means "no filter" (the pipeline placeholder); anything else
        // that isn't a string literal is an error — the previous `.ok()`
        // silently dropped a malformed filter, returning unfiltered rows.
        let inline_filter = if exprs.len() == 6 {
            optional_string_arg(&exprs[5], "pg_knn", "filter")?
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
        let literal_vector = extract_literal_vector(&exprs[2], "pg_knn").ok();
        let query_vector_sql = if literal_vector.is_none() {
            build_vector_fetch_sql(&exprs[2], entry.governed_array_key_budget).ok()
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
            governed_array_key_budget: entry.governed_array_key_budget,
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
    governed_array_key_budget: Option<usize>,
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
                if expr_to_pg_sql(expr, self.governed_array_key_budget).is_some() {
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
            if let Some(sql) = expr_to_pg_sql(expr, self.governed_array_key_budget) {
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
fn build_vector_fetch_sql(
    expr: &Expr,
    governed_array_key_budget: Option<usize>,
) -> DFResult<String> {
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

    let dialect = GovernedPostgresDialect::new(governed_array_key_budget);
    let unparser = Unparser::new(&dialect);
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
        let precision = try_get_opt_int(row, "numeric_precision")?;
        let scale = try_get_opt_int(row, "numeric_scale")?;

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
        let sql = build_vector_fetch_sql(&expr, None).unwrap();
        assert!(sql.contains("\"embedding\"::text"), "sql={sql}");
        assert!(sql.contains("AS _knn_subq LIMIT 1"), "sql={sql}");
    }

    #[test]
    fn test_build_vector_fetch_sql_col_name_with_embedded_quote() {
        // A column name containing " must be doubled inside the SQL identifier.
        let expr = subquery_expr_with_col("weird\"col");
        let sql = build_vector_fetch_sql(&expr, None).unwrap();
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
        assert!(build_vector_fetch_sql(&expr, None).is_err());
    }

    fn knn_args(filter: Expr) -> Vec<Expr> {
        use datafusion::logical_expr::lit;
        vec![
            lit("items"),
            lit("embedding"),
            lit("placeholder"), // vector arg is resolved after the filter
            lit("<->"),
            lit(5i64),
            filter,
        ]
    }

    #[test]
    fn malformed_inline_filter_is_an_error_not_silently_dropped() {
        // A filter the planner can't read must fail the query — the old
        // `.ok()` swallowed it and returned unfiltered rows.
        use datafusion::logical_expr::lit;
        let function = PgKnnTableFunction::new(Arc::new(std::sync::RwLock::new(
            std::collections::HashMap::new(),
        )));
        let err = function.call(&knn_args(lit(42i64))).unwrap_err();
        assert!(
            err.to_string()
                .contains("pg_knn: 'filter' must be a string literal"),
            "got {err}"
        );
    }

    #[test]
    fn null_inline_filter_means_no_filter() {
        // NULL is the pipeline placeholder for "not provided": argument
        // extraction passes and planning proceeds to the registry lookup
        // (which fails here only because the test registry is empty).
        let function = PgKnnTableFunction::new(Arc::new(std::sync::RwLock::new(
            std::collections::HashMap::new(),
        )));
        let err = function
            .call(&knn_args(Expr::Literal(
                datafusion::common::ScalarValue::Null,
                None,
            )))
            .unwrap_err();
        assert!(
            err.to_string().contains("not found in registry"),
            "NULL filter must not fail extraction; got {err}"
        );
    }
}

/// Read an optional integer column without assuming its wire width.
///
/// On Postgres, `information_schema.columns.numeric_precision` and
/// `numeric_scale` are `cardinal_number`, which decodes as INT4. A
/// Postgres-compatible server is free to expose them wider or narrower:
/// TiDB Cloud's Postgres mode returns INT8 (seen 2026-10-16 — every table on
/// such a source failed registration with "Rust type `Option<i32>` (as SQL
/// type `INT4`) is not compatible with SQL type `INT8`", taking the whole
/// engine down with it, since one failed registration fails boot). The
/// datafusion-table-providers crate beside this one already reads the same
/// columns with a width-tolerant decoder (`row_opt_int` in its
/// `postgresconn.rs`); this is the same posture for the one query that did
/// not.
///
/// A value wider than `i32` is treated as absent rather than an error: a
/// precision past `i32::MAX` is not a precision Postgres can have, and the
/// caller already treats `None` as "use the type's default".
pub fn try_get_opt_int(row: &sqlx::postgres::PgRow, col: &str) -> anyhow::Result<Option<i32>> {
    let narrow = match row.try_get::<Option<i32>, _>(col) {
        Ok(v) => return Ok(v),
        Err(e) => e,
    };
    if let Ok(v) = row.try_get::<Option<i64>, _>(col) {
        return Ok(v.and_then(|v| i32::try_from(v).ok()));
    }
    if let Ok(v) = row.try_get::<Option<i16>, _>(col) {
        return Ok(v.map(i32::from));
    }
    Err(anyhow::Error::from(narrow).context(format!("decoding '{col}' as any of INT2/INT4/INT8")))
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
        "ARRAY" if udt_name == "_int8" => {
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
        }
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
