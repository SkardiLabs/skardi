//! Table function for SeekDB native KNN vector search.
//!
//! Usage:
//! ```sql
//! -- Literal vector, L2 distance (default)
//! SELECT * FROM seekdb_knn('docs', 'embedding', [0.1, 0.2, 0.3], 'l2', 10)
//!
//! -- Cosine distance, with WHERE filter
//! SELECT * FROM seekdb_knn('docs', 'embedding', [0.1, 0.2, 0.3], 'cosine', 5)
//! WHERE category = 'news'
//!
//! -- Scalar-subquery query vector (find similar to a stored item)
//! SELECT * FROM seekdb_knn('docs', 'embedding',
//!   (SELECT embedding FROM docs WHERE id = 1), 'l2', 10)
//! ```
//!
//! Returns all non-vector columns plus `_score Float64`. Lower `_score` means
//! more similar — matching `pg_knn` and `sqlite_knn`.

use arrow::array::{Array, Float32Array, Float64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{Result as DFResult, ScalarValue, plan_err};
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use std::any::Any;
use std::sync::Arc;

use super::expr_to_seekdb_sql;
use super::fts_table_function::extract_string;
use super::knn_exec::{DistanceMetric, SeekDbKnnExec};
use crate::sources::providers::knn_utils::extract_k;
use crate::sources::providers::{DatasetEntry, DatasetRegistry};

/// Entry stored in the registry for each registered SeekDB table.
#[derive(Clone, Debug)]
pub struct SeekDbKnnEntry {
    /// Connection pool for this table.
    pub pool: Arc<MySQLConnectionPool>,
    /// Fully-qualified, backtick-quoted table identifier.
    pub qualified_table: String,
    /// All columns and their Arrow types (from PRAGMA / information_schema).
    pub columns: Vec<(String, DataType)>,
}

// ─── TableFunctionImpl ───────────────────────────────────────────────────────

/// Table function that performs SeekDB KNN search.
#[derive(Debug)]
pub struct SeekDbKnnTableFunction {
    registry: DatasetRegistry,
}

impl SeekDbKnnTableFunction {
    pub fn new(registry: DatasetRegistry) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for SeekDbKnnTableFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if exprs.len() != 5 {
            return plan_err!(
                "seekdb_knn(table, vector_col, query_vec, metric, k) expects 5 arguments, got {}. \
                 metric must be one of: l2, cosine, inner_product",
                exprs.len()
            );
        }

        let table_name = extract_string(&exprs[0], "table")?;
        let vector_col = extract_string(&exprs[1], "vector_col")?;

        let metric_str = extract_string(&exprs[3], "metric")?;
        let metric = if metric_str.is_empty() {
            // NULL placeholder during schema inference — default to L2.
            DistanceMetric::default()
        } else {
            metric_str
                .parse::<DistanceMetric>()
                .map_err(DataFusionError::Plan)?
        };

        let k = extract_k(&exprs[4], "seekdb_knn")?;

        let literal_vector = extract_vector(&exprs[2]).ok();

        let entry = {
            let reg = self.registry.read().map_err(|e| {
                DataFusionError::Internal(format!("seekdb_knn registry lock error: {}", e))
            })?;
            let raw = reg.get(&table_name).cloned().ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "seekdb_knn: table '{}' not found in registry. \
                     Make sure the data source is declared with type 'seekdb'.",
                    table_name
                ))
            })?;
            match raw {
                DatasetEntry::Seekdb(e) => e,
                _ => {
                    return plan_err!("seekdb_knn: table '{}' is not a SeekDB dataset", table_name);
                }
            }
        };

        // Output schema: all non-vector columns + _score.
        let mut fields: Vec<Field> = entry
            .columns
            .iter()
            .filter(|(name, _)| name != &vector_col)
            .map(|(name, dtype)| Field::new(name.clone(), dtype.clone(), true))
            .collect();
        fields.push(Field::new("_score", DataType::Float64, true));
        let schema: SchemaRef = Arc::new(Schema::new(fields));

        Ok(Arc::new(SeekDbKnnProvider {
            pool: entry.pool,
            qualified_table: entry.qualified_table,
            vector_col,
            literal_vector,
            query_vector_expr: exprs[2].clone(),
            schema,
            metric,
            k,
        }))
    }
}

// ─── TableProvider ───────────────────────────────────────────────────────────

struct SeekDbKnnProvider {
    pool: Arc<MySQLConnectionPool>,
    qualified_table: String,
    vector_col: String,
    literal_vector: Option<Vec<f32>>,
    query_vector_expr: Expr,
    schema: SchemaRef,
    metric: DistanceMetric,
    k: usize,
}

impl std::fmt::Debug for SeekDbKnnProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeekDbKnnProvider")
            .field("qualified_table", &self.qualified_table)
            .field("vector_col", &self.vector_col)
            .field("metric", &self.metric)
            .field("k", &self.k)
            .finish()
    }
}

#[async_trait]
impl TableProvider for SeekDbKnnProvider {
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
        Ok(filters
            .iter()
            .map(|expr| {
                if expr_to_seekdb_sql(expr).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let mut parts: Vec<String> = Vec::new();
        for expr in filters {
            if let Some(sql) = expr_to_seekdb_sql(expr) {
                parts.push(sql);
            }
        }
        let filter = if parts.is_empty() {
            None
        } else {
            Some(parts.join(" AND "))
        };

        let schema = if let Some(proj) = projection {
            let fields: Vec<Field> = proj.iter().map(|&i| self.schema.field(i).clone()).collect();
            Arc::new(Schema::new(fields))
        } else {
            self.schema.clone()
        };

        let mut exec: SeekDbKnnExec = if let Some(ref vec) = self.literal_vector {
            SeekDbKnnExec::new(
                Arc::clone(&self.pool),
                self.qualified_table.clone(),
                self.vector_col.clone(),
                vec.clone(),
                filter,
                schema,
                self.metric,
                self.k,
            )
        } else if let Expr::ScalarSubquery(subquery) = &self.query_vector_expr {
            let physical_plan = state
                .create_physical_plan(subquery.subquery.as_ref())
                .await?;
            SeekDbKnnExec::new_with_subquery(
                Arc::clone(&self.pool),
                self.qualified_table.clone(),
                self.vector_col.clone(),
                physical_plan,
                filter,
                schema,
                self.metric,
                self.k,
            )
        } else {
            return plan_err!(
                "seekdb_knn: query_vec must be a literal array or a scalar subquery, \
                 e.g. [0.1, 0.2, ...] or (SELECT embedding FROM t WHERE id = 1)"
            );
        };

        if let Some(n) = limit {
            exec = exec.with_scan_limit(n);
        }

        Ok(Arc::new(exec))
    }
}

// ─── Registration ────────────────────────────────────────────────────────────

/// Register the `seekdb_knn` table function with the DataFusion session.
pub fn register_seekdb_knn_udtf(ctx: &SessionContext, registry: DatasetRegistry) {
    ctx.register_udtf(
        "seekdb_knn",
        Arc::new(SeekDbKnnTableFunction::new(registry)),
    );
}

// ─── Argument extraction helpers ─────────────────────────────────────────────

fn extract_vector(expr: &Expr) -> DFResult<Vec<f32>> {
    let values: Arc<dyn arrow::array::Array> = match expr {
        Expr::Literal(ScalarValue::List(arr), _) => {
            if arr.is_empty() {
                return plan_err!("seekdb_knn: query_vec must not be empty");
            }
            arr.value(0)
        }
        Expr::Literal(ScalarValue::FixedSizeList(arr), _) => {
            if arr.is_empty() {
                return plan_err!("seekdb_knn: query_vec must not be empty");
            }
            arr.value(0)
        }
        _ => {
            return plan_err!(
                "seekdb_knn: query_vec must be a literal array (e.g. [0.1, 0.2, ...])"
            );
        }
    };

    if values.is_empty() {
        return plan_err!("seekdb_knn: query_vec must not be empty");
    }
    if let Some(f32_arr) = values.as_any().downcast_ref::<Float32Array>() {
        return Ok(f32_arr.values().to_vec());
    }
    if let Some(f64_arr) = values.as_any().downcast_ref::<Float64Array>() {
        return Ok(f64_arr.values().iter().map(|&v| v as f32).collect());
    }

    plan_err!("seekdb_knn: query_vec elements must be Float32 or Float64")
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::RwLock;

    fn make_knn_function() -> SeekDbKnnTableFunction {
        let registry: DatasetRegistry = Arc::new(RwLock::new(HashMap::new()));
        SeekDbKnnTableFunction::new(registry)
    }

    fn lit_str(s: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(s.to_string())), None)
    }

    fn lit_int(n: i64) -> Expr {
        Expr::Literal(ScalarValue::Int64(Some(n)), None)
    }

    fn lit_null() -> Expr {
        Expr::Literal(ScalarValue::Null, None)
    }

    fn lit_vec(values: &[f64]) -> Expr {
        use arrow::array::{ArrayRef, Float64Array, ListArray};
        use arrow::buffer::OffsetBuffer;
        let arr = Float64Array::from(values.to_vec());
        let list = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Float64, true)),
            OffsetBuffer::from_lengths([values.len()]),
            Arc::new(arr) as ArrayRef,
            None,
        );
        Expr::Literal(ScalarValue::List(Arc::new(list)), None)
    }

    #[test]
    fn test_wrong_arg_count_rejected() {
        let func = make_knn_function();
        let result = func.call(&[lit_str("t"), lit_str("col"), lit_vec(&[0.1, 0.2])]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("expects 5 arguments"),
            "expected arg count error, got: {err}"
        );
    }

    #[test]
    fn test_k_over_max_rejected() {
        let func = make_knn_function();
        let result = func.call(&[
            lit_str("t"),
            lit_str("col"),
            lit_vec(&[0.1, 0.2]),
            lit_str("l2"),
            lit_int(501),
        ]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("must be between 1 and 500"),
            "expected k limit error, got: {err}"
        );
    }

    #[test]
    fn test_k_zero_rejected() {
        let func = make_knn_function();
        let result = func.call(&[
            lit_str("t"),
            lit_str("col"),
            lit_vec(&[0.1, 0.2]),
            lit_str("l2"),
            lit_int(0),
        ]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("positive integer"),
            "expected positive-integer error, got: {err}"
        );
    }

    #[test]
    fn test_k_negative_rejected() {
        let func = make_knn_function();
        let result = func.call(&[
            lit_str("t"),
            lit_str("col"),
            lit_vec(&[0.1, 0.2]),
            lit_str("l2"),
            lit_int(-1),
        ]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("positive integer") && err.contains("-1"),
            "expected positive-integer error with original value, got: {err}"
        );
    }

    #[test]
    fn test_null_placeholders_accepted() {
        let func = make_knn_function();
        let result = func.call(&[
            lit_str("t"),
            lit_str("col"),
            lit_null(),
            lit_null(),
            lit_null(),
        ]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not found in registry"),
            "expected registry error, got: {err}"
        );
    }

    #[test]
    fn test_unknown_metric_rejected() {
        let func = make_knn_function();
        let result = func.call(&[
            lit_str("t"),
            lit_str("col"),
            lit_vec(&[0.1, 0.2]),
            lit_str("manhattan"),
            lit_int(10),
        ]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unknown metric"),
            "expected unknown-metric error, got: {err}"
        );
    }

    #[test]
    fn test_empty_vector_rejected() {
        // `extract_vector` errors but `call` only propagates the error when the
        // registry lookup has already failed; here we confirm the helper rejects empty.
        let empty = lit_vec(&[]);
        assert!(extract_vector(&empty).is_err());
    }

    #[test]
    fn test_extract_vector_f32_literal() {
        let v = lit_vec(&[0.1, 0.2, 0.3]);
        let out = extract_vector(&v).unwrap();
        assert_eq!(out.len(), 3);
        assert!((out[0] - 0.1).abs() < 1e-6);
    }
}
