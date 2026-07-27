//! Table function for Lance KNN search
//!
//! Usage:
//! ```sql
//! -- With literal vector
//! SELECT * FROM lance_knn('table_name', 'embedding', [0.1, 0.2, ...], 10)
//!
//! -- With subquery vector
//! SELECT * FROM lance_knn('table_name', 'embedding',
//!     (SELECT embedding FROM users WHERE id = $1), 10)
//!
//! -- With optional filter
//! SELECT * FROM lance_knn('table_name', 'embedding',
//!     (SELECT embedding FROM users WHERE id = $1), 10, 'category = ''electronics''')
//! ```

use arrow::array::{Array, ArrayRef, Float32Array, Float64Array};
use arrow::datatypes::{DataType, Field, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{Result as DFResult, ScalarValue, plan_err};
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use lance::dataset::Dataset;
use std::any::Any;
use std::sync::Arc;

use super::knn_exec::LanceKnnExec;
use super::utils::expr_to_lance_sql;
use crate::sources::providers::knn_utils::extract_k;
use crate::sources::providers::udtf_args::strict_string_arg;
use crate::sources::providers::{DatasetEntry, DatasetRegistry};

/// Table function that creates KNN search on Lance tables
#[derive(Debug)]
pub struct LanceKnnTableFunction {
    dataset_registry: DatasetRegistry,
}

impl LanceKnnTableFunction {
    pub fn new(dataset_registry: DatasetRegistry) -> Self {
        Self { dataset_registry }
    }
}

impl TableFunctionImpl for LanceKnnTableFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if exprs.len() < 4 || exprs.len() > 5 {
            return plan_err!(
                "lance_knn(table_name, vector_column, query_vector, k, [filter]) expects 4-5 arguments, got {}",
                exprs.len()
            );
        }

        // Extract string arguments
        let table_name = strict_string_arg(&exprs[0], "lance_knn", "table_name")?;
        let vector_column = strict_string_arg(&exprs[1], "lance_knn", "vector_column")?;
        let k = extract_k(&exprs[3], "lance_knn")?;
        let filter = if exprs.len() == 5 {
            Some(strict_string_arg(&exprs[4], "lance_knn", "filter")?)
        } else {
            None
        };

        // Get dataset from registry
        let dataset = {
            let registry = self.dataset_registry.read().map_err(|e| {
                datafusion::error::DataFusionError::Internal(format!("Registry lock error: {}", e))
            })?;
            let entry = registry.get(&table_name).cloned().ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!(
                    "lance_knn: table '{}' not found in registry",
                    table_name
                ))
            })?;
            match entry {
                DatasetEntry::Lance(ds) => ds,
                _ => return plan_err!("lance_knn: table '{}' is not a Lance dataset", table_name),
            }
        };

        // Try to extract literal vector, otherwise store expr for subquery
        let query_vector_expr = exprs[2].clone();
        let literal_vector = try_extract_vector(&query_vector_expr)?;

        Ok(Arc::new(LanceKnnProvider {
            dataset,
            vector_column,
            literal_vector,
            query_vector_expr,
            k,
            filter,
        }))
    }
}

/// Thin wrapper that returns LanceKnnExec from scan()
struct LanceKnnProvider {
    dataset: Arc<Dataset>,
    vector_column: String,
    literal_vector: Option<ArrayRef>,
    query_vector_expr: Expr,
    k: usize,
    filter: Option<String>,
}

impl std::fmt::Debug for LanceKnnProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LanceKnnProvider")
            .field("vector_column", &self.vector_column)
            .field("k", &self.k)
            .finish()
    }
}

#[async_trait]
impl TableProvider for LanceKnnProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // Build schema: dataset fields (excluding vector) + _distance
        let lance_schema = self.dataset.schema();
        let mut fields: Vec<Field> = lance_schema
            .fields
            .iter()
            .filter(|f| f.name.as_str() != self.vector_column)
            .map(|f| f.into())
            .collect();
        fields.push(Field::new("_distance", DataType::Float32, true));
        Arc::new(arrow::datatypes::Schema::new(fields))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Accept all filters as exact pushdown — Lance's SQL filter parser handles
        // standard comparison operators (=, <, >, <=, >=, AND, OR, IN, etc.)
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Exact)
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let mut exec = if let Some(ref vector) = self.literal_vector {
            // Literal vector path
            LanceKnnExec::try_new(
                self.dataset.clone(),
                self.vector_column.clone(),
                vector.clone(),
                self.k,
            )?
        } else {
            // Subquery path - create physical plan for deferred evaluation
            let Expr::ScalarSubquery(subquery) = &self.query_vector_expr else {
                return plan_err!(
                    "lance_knn: query_vector must be literal array or scalar subquery"
                );
            };

            let physical_plan = state
                .create_physical_plan(subquery.subquery.as_ref())
                .await?;

            let column_name = subquery
                .subquery
                .schema()
                .fields()
                .first()
                .map(|f| f.name().clone())
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(
                        "lance_knn: subquery must return at least one column".to_string(),
                    )
                })?;

            LanceKnnExec::try_new_with_deferred_extraction(
                self.dataset.clone(),
                self.vector_column.clone(),
                physical_plan,
                column_name,
                self.k,
            )?
        };

        // Apply column projection if DataFusion requests a subset of columns
        if let Some(proj) = projection {
            exec = exec.with_projection(proj.clone())?;
        }

        // Combine inline filter (from function argument) with WHERE clause filters
        let mut filter_parts: Vec<String> = Vec::new();
        if let Some(ref f) = self.filter {
            filter_parts.push(f.clone());
        }
        if !filters.is_empty() {
            let where_filter = filters
                .iter()
                .map(|f| expr_to_lance_sql(f))
                .collect::<Vec<_>>()
                .join(" AND ");
            filter_parts.push(where_filter);
        }
        if !filter_parts.is_empty() {
            exec = exec.with_filter(filter_parts.join(" AND "));
        }

        // Apply scan limit if provided (e.g., from SQL LIMIT clause)
        if let Some(n) = limit {
            exec = exec.with_limit(n);
        }

        Ok(Arc::new(exec))
    }
}

// Helper functions for argument extraction

fn try_extract_vector(expr: &Expr) -> DFResult<Option<ArrayRef>> {
    match expr {
        Expr::Literal(ScalarValue::List(arr), _) => {
            let list_arr = arr.as_ref();
            if list_arr.is_empty() {
                return plan_err!("lance_knn: query_vector cannot be empty");
            }
            let values = list_arr.value(0);
            extract_float32_array(&values)
        }
        Expr::Literal(ScalarValue::FixedSizeList(arr), _) => {
            let list_arr = arr.as_ref();
            if list_arr.is_empty() {
                return plan_err!("lance_knn: query_vector cannot be empty");
            }
            let values = list_arr.value(0);
            extract_float32_array(&values)
        }
        _ => Ok(None), // Not a literal, could be subquery
    }
}

/// Extract a Float32Array from an array, casting from Float64 if needed.
/// DataFusion parses untyped float literals (e.g. `[0.1, 0.2]`) as Float64,
/// but Lance expects Float32 vectors.
fn extract_float32_array(values: &dyn Array) -> DFResult<Option<ArrayRef>> {
    if let Some(f32_arr) = values.as_any().downcast_ref::<Float32Array>() {
        return Ok(Some(Arc::new(f32_arr.clone()) as ArrayRef));
    }
    if let Some(f64_arr) = values.as_any().downcast_ref::<Float64Array>() {
        let f32_arr: Float32Array = f64_arr.iter().map(|v| v.map(|x| x as f32)).collect();
        return Ok(Some(Arc::new(f32_arr) as ArrayRef));
    }
    plan_err!("lance_knn: query_vector must contain Float32 or Float64 values")
}

/// Register lance_knn table function with SessionContext
pub fn register_lance_knn_udtf(
    ctx: &datafusion::prelude::SessionContext,
    dataset_registry: DatasetRegistry,
) {
    ctx.register_udtf(
        "lance_knn",
        Arc::new(LanceKnnTableFunction::new(dataset_registry)),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::providers::DatasetRegistry;
    use arrow::array::{Float32Array, Int64Array, StringArray};
    use std::path::Path;

    // Uses test_data.lance which is a superset of vec_data.lance
    // with additional columns (description, category) for filter testing.
    // Schema: id (int64), vector (128-dim float), item_id (int64),
    //         revenue (float64), description (string), category (string)
    const DATASET_PATH: &str = "data/test_data.lance";

    async fn setup_knn_context() -> datafusion::prelude::SessionContext {
        use super::super::registration::register_lance_table;

        let dataset_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(DATASET_PATH);
        let dataset_path_str = dataset_path.to_str().unwrap();

        assert!(
            dataset_path.exists(),
            "Test dataset not found at {dataset_path_str}. \
             Run: python scripts/prepare_fts_test_data.py"
        );

        let mut ctx = datafusion::prelude::SessionContext::new();
        let registry: DatasetRegistry =
            Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));

        register_lance_table(&mut ctx, "knn_data", dataset_path_str, Some(&registry))
            .await
            .expect("Failed to register lance table");

        register_lance_knn_udtf(&ctx, registry);
        ctx
    }

    /// Read a real vector from the dataset to use as a query vector.
    /// Returns a SQL literal string like `[0.1, 0.2, ...]`.
    async fn read_query_vector(ctx: &datafusion::prelude::SessionContext) -> String {
        let df = ctx
            .sql("SELECT vector FROM knn_data LIMIT 1")
            .await
            .expect("Failed to read vector");
        let batches = df.collect().await.expect("Failed to collect vector");
        let batch = &batches[0];
        let list_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeListArray>()
            .expect("vector column should be FixedSizeList");
        let values = list_col.value(0);
        let floats = values
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("vector values should be Float32");
        let elements: Vec<String> = (0..floats.len())
            .map(|i| {
                let v = floats.value(i);
                // Ensure values always have a decimal point so DataFusion parses as Float64
                if v.fract() == 0.0 {
                    format!("{:.1}", v)
                } else {
                    format!("{}", v)
                }
            })
            .collect();
        format!("[{}]", elements.join(", "))
    }

    // ── Integration tests ──
    // Require data/test_data.lance (run: python scripts/prepare_fts_test_data.py)
    // Run with: cargo test -p sources -- --ignored lance_knn

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_knn_basic_search() {
        let ctx = setup_knn_context().await;
        let query_vec = read_query_vector(&ctx).await;

        let sql = format!(
            "SELECT id, item_id, _distance FROM lance_knn('knn_data', 'vector', {}, 5)",
            query_vec
        );
        let df = ctx.sql(&sql).await.expect("SQL parse failed");
        let batches = df.collect().await.expect("Query execution failed");

        assert!(!batches.is_empty(), "Expected non-empty results");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows > 0, "Expected at least 1 result");
        assert!(total_rows <= 5, "Expected at most 5 results (k=5)");

        // _distance should be non-negative
        let distances = batches[0]
            .column_by_name("_distance")
            .expect("Missing _distance column")
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("_distance should be Float32");
        for i in 0..distances.len() {
            assert!(
                !distances.is_null(i) && distances.value(i) >= 0.0,
                "_distance should be non-negative"
            );
        }
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_knn_distance_ordering() {
        let ctx = setup_knn_context().await;
        let query_vec = read_query_vector(&ctx).await;

        let sql = format!(
            "SELECT _distance FROM lance_knn('knn_data', 'vector', {}, 10)",
            query_vec
        );
        let df = ctx.sql(&sql).await.expect("SQL parse failed");
        let batches = df.collect().await.expect("Query execution failed");

        let mut all_distances = Vec::new();
        for batch in &batches {
            let distances = batch
                .column_by_name("_distance")
                .unwrap()
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            for i in 0..distances.len() {
                all_distances.push(distances.value(i));
            }
        }

        // Distances should be in ascending order (nearest first)
        if all_distances.len() > 1 {
            for i in 1..all_distances.len() {
                assert!(
                    all_distances[i] >= all_distances[i - 1],
                    "Distances should be ascending: {} < {} at index {}",
                    all_distances[i],
                    all_distances[i - 1],
                    i
                );
            }
        }
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_knn_schema_excludes_vector_column() {
        let ctx = setup_knn_context().await;
        let query_vec = read_query_vector(&ctx).await;

        let sql = format!(
            "SELECT * FROM lance_knn('knn_data', 'vector', {}, 1)",
            query_vec
        );
        let df = ctx.sql(&sql).await.expect("SQL parse failed");
        let field_names: Vec<&str> = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();

        assert!(field_names.contains(&"id"), "Should contain 'id'");
        assert!(field_names.contains(&"item_id"), "Should contain 'item_id'");
        assert!(
            field_names.contains(&"_distance"),
            "Should contain '_distance'"
        );
        assert!(
            !field_names.contains(&"vector"),
            "Should NOT contain 'vector' (excluded from output)"
        );
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_knn_k_limits_results() {
        let ctx = setup_knn_context().await;
        let query_vec = read_query_vector(&ctx).await;

        for k in [1, 3, 7] {
            let sql = format!(
                "SELECT id FROM lance_knn('knn_data', 'vector', {}, {})",
                query_vec, k
            );
            let df = ctx.sql(&sql).await.expect("SQL parse failed");
            let batches = df.collect().await.expect("Query execution failed");
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert!(
                total_rows <= k,
                "k={k}: expected at most {k} results, got {total_rows}"
            );
        }
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_knn_with_sql_limit() {
        let ctx = setup_knn_context().await;
        let query_vec = read_query_vector(&ctx).await;

        // Request k=10 but SQL LIMIT 3 — should return at most 3
        let sql = format!(
            "SELECT id, _distance FROM lance_knn('knn_data', 'vector', {}, 10) LIMIT 3",
            query_vec
        );
        let df = ctx.sql(&sql).await.expect("SQL parse failed");
        let batches = df.collect().await.expect("Query execution failed");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(
            total_rows <= 3,
            "LIMIT 3 should restrict to at most 3 rows, got {total_rows}"
        );
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_knn_where_category_filter() {
        let ctx = setup_knn_context().await;
        let query_vec = read_query_vector(&ctx).await;

        let sql = format!(
            "SELECT id, category, _distance \
             FROM lance_knn('knn_data', 'vector', {}, 50) \
             WHERE category = 'electronics'",
            query_vec
        );
        let df = ctx.sql(&sql).await.expect("SQL parse failed");
        let batches = df.collect().await.expect("Query execution failed");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows > 0, "Expected results with category filter");

        // Every returned row must have category = 'electronics'
        for batch in &batches {
            let cats = batch
                .column_by_name("category")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..cats.len() {
                assert_eq!(
                    cats.value(i),
                    "electronics",
                    "WHERE filter should restrict category"
                );
            }
        }
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_knn_where_numeric_filter() {
        let ctx = setup_knn_context().await;
        let query_vec = read_query_vector(&ctx).await;

        let sql = format!(
            "SELECT id, item_id, _distance \
             FROM lance_knn('knn_data', 'vector', {}, 50) \
             WHERE item_id > 500",
            query_vec
        );
        let df = ctx.sql(&sql).await.expect("SQL parse failed");
        let batches = df.collect().await.expect("Query execution failed");

        for batch in &batches {
            let item_ids = batch
                .column_by_name("item_id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..item_ids.len() {
                assert!(
                    item_ids.value(i) > 500,
                    "WHERE filter should restrict item_id > 500, got {}",
                    item_ids.value(i)
                );
            }
        }
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_knn_where_compound_filter() {
        let ctx = setup_knn_context().await;
        let query_vec = read_query_vector(&ctx).await;

        let sql = format!(
            "SELECT id, category, item_id, _distance \
             FROM lance_knn('knn_data', 'vector', {}, 50) \
             WHERE category = 'electronics' AND item_id > 100",
            query_vec
        );
        let df = ctx.sql(&sql).await.expect("SQL parse failed");
        let batches = df.collect().await.expect("Query execution failed");

        for batch in &batches {
            let cats = batch
                .column_by_name("category")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let item_ids = batch
                .column_by_name("item_id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..cats.len() {
                assert_eq!(cats.value(i), "electronics");
                assert!(item_ids.value(i) > 100);
            }
        }
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_knn_where_filters_reduce_results() {
        let ctx = setup_knn_context().await;
        let query_vec = read_query_vector(&ctx).await;

        // Run without filter
        let sql_all = format!(
            "SELECT id FROM lance_knn('knn_data', 'vector', {}, 50)",
            query_vec
        );
        let df_all = ctx.sql(&sql_all).await.expect("SQL parse failed");
        let batches_all = df_all.collect().await.expect("Query execution failed");
        let rows_all: usize = batches_all.iter().map(|b| b.num_rows()).sum();

        // Run with category filter — should return fewer (or equal) rows
        let sql_filtered = format!(
            "SELECT id FROM lance_knn('knn_data', 'vector', {}, 50) \
             WHERE category = 'outdoor'",
            query_vec
        );
        let df_filtered = ctx.sql(&sql_filtered).await.expect("SQL parse failed");
        let batches_filtered = df_filtered.collect().await.expect("Query execution failed");
        let rows_filtered: usize = batches_filtered.iter().map(|b| b.num_rows()).sum();

        assert!(
            rows_filtered > 0,
            "Filtered query should still return results"
        );
        assert!(
            rows_filtered <= rows_all,
            "Filtered results ({rows_filtered}) should be <= unfiltered ({rows_all})"
        );
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_knn_inline_filter_argument() {
        let ctx = setup_knn_context().await;
        let query_vec = read_query_vector(&ctx).await;

        // Use the 5th argument (inline filter) instead of WHERE clause
        let sql = format!(
            "SELECT id, category, _distance \
             FROM lance_knn('knn_data', 'vector', {}, 50, 'category = ''electronics''')",
            query_vec
        );
        let df = ctx.sql(&sql).await.expect("SQL parse failed");
        let batches = df.collect().await.expect("Query execution failed");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows > 0, "Expected results with inline filter");

        for batch in &batches {
            let cats = batch
                .column_by_name("category")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..cats.len() {
                assert_eq!(
                    cats.value(i),
                    "electronics",
                    "Inline filter should restrict category"
                );
            }
        }
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_knn_subquery_vector() {
        let ctx = setup_knn_context().await;

        // Use a subquery to fetch the vector for id = 1
        let sql = "SELECT id, _distance FROM lance_knn('knn_data', 'vector', \
                   (SELECT vector FROM knn_data WHERE id = 1), 5)";
        let df = ctx.sql(sql).await.expect("SQL parse failed");
        let batches = df.collect().await.expect("Query execution failed");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(
            total_rows > 0,
            "Subquery vector search should return results"
        );
        assert!(total_rows <= 5, "Expected at most 5 results (k=5)");

        // The first result should be the vector itself (distance ~0)
        let distances = batches[0]
            .column_by_name("_distance")
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert!(
            distances.value(0) < 0.001,
            "First result should be the query vector itself (distance ~0), got {}",
            distances.value(0)
        );
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_knn_invalid_table_name() {
        let ctx = setup_knn_context().await;

        let result = ctx
            .sql("SELECT * FROM lance_knn('nonexistent_table', 'vector', [0.1, 0.2], 10)")
            .await;

        assert!(result.is_err(), "Expected error for nonexistent table");
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_knn_where_with_limit_combined() {
        let ctx = setup_knn_context().await;
        let query_vec = read_query_vector(&ctx).await;

        // Combine WHERE + LIMIT: k=50, filtered, then limited to 2
        let sql = format!(
            "SELECT id, category, _distance \
             FROM lance_knn('knn_data', 'vector', {}, 50) \
             WHERE category = 'electronics' \
             LIMIT 2",
            query_vec
        );
        let df = ctx.sql(&sql).await.expect("SQL parse failed");
        let batches = df.collect().await.expect("Query execution failed");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(
            total_rows <= 2,
            "LIMIT 2 should restrict to at most 2 rows, got {total_rows}"
        );

        for batch in &batches {
            let cats = batch
                .column_by_name("category")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..cats.len() {
                assert_eq!(cats.value(i), "electronics");
            }
        }
    }
}
