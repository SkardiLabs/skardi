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
//!
//! -- Embedding UDF: any scalar function returning List<Float32> works directly.
//! SELECT * FROM seekdb_knn('docs', 'embedding',
//!   candle('models/bge-small-en-v1.5', 'how do I tune HNSW?'), 'cosine', 10)
//!
//! -- Catalog-mode data sources: the first arg is the three-part key
//! -- `<catalog>.<schema>.<table>` exactly as it was registered.
//! SELECT * FROM seekdb_knn('seekdb_cat.mydb.docs', 'embedding',
//!   [0.1, 0.2, 0.3], 'l2', 10)
//! ```
//!
//! Returns all non-vector columns plus `_score Float64`. Lower `_score` means
//! more similar — matching `pg_knn` and `sqlite_knn`.
//!
//! **Table-name arg.** In table mode the first argument is the bare name the
//! data source was registered under. In catalog mode it is
//! `<catalog>.<schema>.<table>` (unquoted, case-sensitive) — the same three-
//! part reference DataFusion uses to route SQL against the registered catalog.
//!
//! **HNSW (approximate) search.** The generated SQL uses SeekDB's
//! `APPROXIMATE` keyword on the `ORDER BY`, which is what steers the planner
//! to the `VECTOR INDEX SCAN` (HNSW) path. Without it, SeekDB silently
//! degrades to a `TABLE FULL SCAN + TOP-N SORT` — correct but O(n). HNSW
//! only kicks in when the target table has a `VECTOR INDEX ... WITH (TYPE
//! = HNSW, DISTANCE = <m>)` whose declared `<m>` matches the metric arg to
//! `seekdb_knn`; mismatched-metric queries fall back to a full scan.

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{Result as DFResult, plan_err};
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use std::any::Any;
use std::sync::Arc;

use super::knn_exec::{DistanceMetric, SeekDbKnnExec};
use super::{expr_to_seekdb_sql, extract_string_arg};
use crate::sources::providers::knn_utils::{extract_k, extract_literal_vector};
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

        let table_name = extract_string_arg(&exprs[0], "seekdb_knn", "table")?;
        let vector_col = extract_string_arg(&exprs[1], "seekdb_knn", "vector_col")?;

        let metric_str = extract_string_arg(&exprs[3], "seekdb_knn", "metric")?;
        let metric = if metric_str.is_empty() {
            // NULL placeholder during schema inference — default to L2.
            DistanceMetric::default()
        } else {
            metric_str
                .parse::<DistanceMetric>()
                .map_err(DataFusionError::Plan)?
        };

        let k = extract_k(&exprs[4], "seekdb_knn")?;

        let literal_vector = extract_literal_vector(&exprs[2], "seekdb_knn").ok();

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
        } else if let Some(physical_plan) =
            resolve_query_vector_plan(&self.query_vector_expr, state).await?
        {
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
                "seekdb_knn: query_vec must be a literal array, a scalar subquery, \
                 or a scalar function call — e.g. [0.1, 0.2, ...], \
                 (SELECT embedding FROM t WHERE id = 1), or candle('model', 'text')"
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

/// Resolve a non-literal `query_vec` argument to a one-row `ExecutionPlan`
/// whose first column is the query vector.
///
/// Returns `Ok(None)` for expressions that are neither a scalar subquery nor a
/// scalar function — the caller emits the appropriate user-facing error.
///
/// For `Expr::ScalarFunction` (e.g. `candle('model', 'text')` or any UDF that
/// returns `List<Float32>`), the expression is wrapped in a trivial
/// `Projection` over a single-row empty relation so DataFusion can plan and
/// evaluate it at execution time. This is pool-free and async-friendly —
/// factored out so it can be tested independently of the MySQL connection.
pub(super) async fn resolve_query_vector_plan(
    expr: &Expr,
    state: &dyn Session,
) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
    match expr {
        Expr::ScalarSubquery(subquery) => {
            let plan = state
                .create_physical_plan(subquery.subquery.as_ref())
                .await?;
            Ok(Some(plan))
        }
        Expr::ScalarFunction(_) => {
            let logical_plan = LogicalPlanBuilder::empty(true)
                .project(vec![expr.clone()])?
                .build()?;
            let physical_plan = state.create_physical_plan(&logical_plan).await?;
            Ok(Some(physical_plan))
        }
        _ => Ok(None),
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::ScalarValue;
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
        // Delegation smoke test: the shared `extract_literal_vector` rejects an
        // empty list and tags the error with the caller's fn_name.
        let empty = lit_vec(&[]);
        let err = extract_literal_vector(&empty, "seekdb_knn")
            .unwrap_err()
            .to_string();
        assert!(err.contains("seekdb_knn") && err.contains("must not be empty"));
    }

    #[test]
    fn test_extract_vector_f32_literal() {
        // Delegation smoke test: the shared helper handles the common Float32 case.
        let v = lit_vec(&[0.1, 0.2, 0.3]);
        let out = extract_literal_vector(&v, "seekdb_knn").unwrap();
        assert_eq!(out.len(), 3);
        assert!((out[0] - 0.1).abs() < 1e-6);
    }

    // ─── ScalarFunction (embedding UDF) support ─────────────────────────────
    //
    // The following tests exercise `query_vec = embed_udf('text')`. `call()` must
    // accept the expression (deferring resolution), and `resolve_query_vector_plan`
    // must turn it into an `ExecutionPlan` whose first column is `List<Float32>`.

    use arrow::array::{Float32Builder, ListBuilder};
    use datafusion::logical_expr::expr::ScalarFunction;
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    /// Minimal UDF that mimics an embedding model: `fake_embed(text) -> List<Float32>`
    /// returning `[0.1, 0.2, 0.3]` for every input row. Used to verify the
    /// ScalarFunction arg path without pulling in an actual embedding backend.
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct FakeEmbedUDF {
        signature: Signature,
    }

    impl FakeEmbedUDF {
        fn new() -> Self {
            Self {
                signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for FakeEmbedUDF {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn name(&self) -> &str {
            "fake_embed"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
            Ok(DataType::List(Arc::new(Field::new_list_field(
                DataType::Float32,
                true,
            ))))
        }
        fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
            let mut builder = ListBuilder::new(Float32Builder::new());
            for _ in 0..args.number_rows {
                builder.values().append_slice(&[0.1f32, 0.2, 0.3]);
                builder.append(true);
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }
    }

    fn embed_call(text: &str) -> (ScalarUDF, Expr) {
        let udf = ScalarUDF::new_from_impl(FakeEmbedUDF::new());
        let expr = Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf.clone()),
            vec![lit_str(text)],
        ));
        (udf, expr)
    }

    #[test]
    fn test_scalar_function_arg_deferred_to_scan() {
        // `call()` must accept a ScalarFunction query_vec and defer resolution.
        // With an empty registry, we expect the registry-not-found error rather
        // than an "must be literal" arg-type error.
        let func = make_knn_function();
        let (_udf, embed_expr) = embed_call("hello");
        let result = func.call(&[
            lit_str("t"),
            lit_str("col"),
            embed_expr,
            lit_str("cosine"),
            lit_int(5),
        ]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not found in registry"),
            "ScalarFunction query_vec should reach registry lookup, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_resolve_query_vector_plan_scalar_function() {
        use crate::sources::providers::knn_utils::extract_query_vector;

        let ctx = SessionContext::new();
        let (udf, embed_expr) = embed_call("hello world");
        ctx.register_udf(udf);

        let state = ctx.state();
        let plan = resolve_query_vector_plan(&embed_expr, &state)
            .await
            .expect("resolver should succeed")
            .expect("ScalarFunction should produce a plan");

        let vec = extract_query_vector(plan, ctx.task_ctx())
            .await
            .expect("plan should execute");
        assert_eq!(vec, Some(vec![0.1f32, 0.2, 0.3]));
    }

    #[tokio::test]
    async fn test_resolve_query_vector_plan_rejects_column_ref() {
        // Bare column references and other non-literal/non-function exprs should
        // return None so the caller emits the user-facing error.
        let ctx = SessionContext::new();
        let state = ctx.state();
        let col_expr = datafusion::logical_expr::col("embedding");
        let plan = resolve_query_vector_plan(&col_expr, &state)
            .await
            .expect("resolver should not error for unsupported exprs");
        assert!(plan.is_none());
    }

    // ─── Integration tests ──────────────────────────────────────────────────
    //
    // Require a live SeekDB instance on localhost:2881 with the README
    // quick-start schema seeded (docs/seekdb/README.md):
    //
    //     docker run -d --name seekdb -p 2881:2881 -p 2886:2886 oceanbase/seekdb:latest
    //     # then apply the quick-start CREATE TABLE + INSERT block
    //
    // Expected `docs` rows (the seed's 5 vectors):
    //     doc-a electronics [1,0,0,0]
    //     doc-b electronics [0,1,0,0]
    //     doc-c books       [0,0,1,0]
    //     doc-d electronics [1,1,0,0]
    //     doc-e books       [0.5,0.5,0.5,0.5]
    //
    // Run with: `cargo test -p skardi -- --ignored seekdb`

    use arrow::array::{Float64Array, Int32Array, RecordBatch, StringArray};

    use super::super::register_seekdb_tables;
    use crate::sources::hierarchy::HierarchyLevel;

    async fn register_ci_knn(ctx: &mut SessionContext, table: &str) -> DatasetRegistry {
        let registry: DatasetRegistry = Arc::new(RwLock::new(HashMap::new()));

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
            Some(&registry),
            HierarchyLevel::Table,
        )
        .await
        .unwrap_or_else(|e| panic!("register {} failed: {}", table, e));

        register_seekdb_knn_udtf(ctx, Arc::clone(&registry));
        registry
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
    async fn test_knn_basic_search() {
        let mut ctx = SessionContext::new();
        let _reg = register_ci_knn(&mut ctx, "docs").await;

        let batches = query_all(
            &ctx,
            "SELECT title, _score \
             FROM seekdb_knn('docs', 'embedding', [1.0, 0.0, 0.0, 0.0], 'l2', 3) \
             ORDER BY _score ASC",
        )
        .await;

        assert_eq!(total_rows(&batches), 3, "k=3 must return exactly 3 rows");

        let titles = batches[0]
            .column_by_name("title")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("title is Utf8");
        assert_eq!(
            titles.value(0),
            "doc-a",
            "[1,0,0,0] is an exact match for doc-a"
        );

        let scores = batches[0]
            .column_by_name("_score")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("_score is Float64");
        assert!(
            scores.value(0).abs() < 1e-6,
            "exact match should score ~0, got {}",
            scores.value(0)
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_knn_distance_ordering() {
        let mut ctx = SessionContext::new();
        let _reg = register_ci_knn(&mut ctx, "docs").await;

        let batches = query_all(
            &ctx,
            "SELECT _score \
             FROM seekdb_knn('docs', 'embedding', [1.0, 0.0, 0.0, 0.0], 'l2', 5) \
             ORDER BY _score ASC",
        )
        .await;

        let scores = batches[0]
            .column_by_name("_score")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        for i in 1..scores.len() {
            assert!(
                scores.value(i - 1) <= scores.value(i),
                "_score must be ascending: scores[{}]={} > scores[{}]={}",
                i - 1,
                scores.value(i - 1),
                i,
                scores.value(i),
            );
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_knn_excludes_vector_column() {
        let mut ctx = SessionContext::new();
        let _reg = register_ci_knn(&mut ctx, "docs").await;

        // SELECT *: the output schema surfaces every non-vector column and
        // `_score`, but must not expose the raw VECTOR column.
        let batches = query_all(
            &ctx,
            "SELECT * FROM seekdb_knn('docs', 'embedding', [1.0, 0.0, 0.0, 0.0], 'l2', 1)",
        )
        .await;

        assert_eq!(total_rows(&batches), 1);
        let schema = batches[0].schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            names.contains(&"id") && names.contains(&"title") && names.contains(&"_score"),
            "expected id/title/_score in output schema, got {names:?}"
        );
        assert!(
            !names.contains(&"embedding"),
            "VECTOR column must be hidden from seekdb_knn output, got {names:?}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_knn_respects_k() {
        let mut ctx = SessionContext::new();
        let _reg = register_ci_knn(&mut ctx, "docs").await;

        let batches = query_all(
            &ctx,
            "SELECT id FROM seekdb_knn('docs', 'embedding', [0.5, 0.5, 0.5, 0.5], 'l2', 2)",
        )
        .await;

        assert_eq!(total_rows(&batches), 2, "k=2 must cap the result");
    }

    #[tokio::test]
    #[ignore]
    async fn test_knn_inner_product_metric() {
        // Guards against silent regressions in the InnerProduct → SeekDB
        // function-name mapping. CI seeds `docs_ip` with a matching
        // `DISTANCE = INNER_PRODUCT` HNSW index; the query here only succeeds
        // if `NEGATIVE_INNER_PRODUCT()` is a real SeekDB function AND the
        // planner accepts it with APPROXIMATE. If either assumption breaks we
        // get a clear runtime error instead of silent performance loss.
        let mut ctx = SessionContext::new();
        let _reg = register_ci_knn(&mut ctx, "docs_ip").await;

        let batches = query_all(
            &ctx,
            "SELECT title, _score \
             FROM seekdb_knn('docs_ip', 'embedding', [1.0, 0.0, 0.0, 0.0], 'inner_product', 4) \
             ORDER BY _score ASC",
        )
        .await;

        assert!(total_rows(&batches) >= 2, "expected rows from IP search");

        // NEGATIVE_INNER_PRODUCT means lower = more similar; `ip-a` has dot
        // product 1.0 with the query, so its `_score` should be the most
        // negative (or smallest) of the set.
        let titles = batches[0]
            .column_by_name("title")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("title is Utf8");
        assert_eq!(
            titles.value(0),
            "ip-a",
            "inner-product: most-similar vector must rank first"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_knn_subquery_find_similar() {
        let mut ctx = SessionContext::new();
        let _reg = register_ci_knn(&mut ctx, "docs").await;

        // Scalar-subquery query vector: the seed row's own embedding must be
        // returned first with _score ~0 (exact self-match).
        let batches = query_all(
            &ctx,
            "SELECT id, title, _score \
             FROM seekdb_knn('docs', 'embedding', \
                  (SELECT embedding FROM docs WHERE title = 'doc-a'), \
                  'cosine', 3) \
             ORDER BY _score ASC",
        )
        .await;

        assert_eq!(total_rows(&batches), 3);

        let titles = batches[0]
            .column_by_name("title")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            titles.value(0),
            "doc-a",
            "self-match should rank first, got {:?}",
            titles.value(0)
        );

        let scores = batches[0]
            .column_by_name("_score")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            scores.value(0).abs() < 1e-6,
            "self-match should score ~0, got {}",
            scores.value(0)
        );

        // id column is Int32 on this schema — smoke-check typing too so a
        // future schema drift surfaces loudly.
        let ids = batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id is Int32");
        assert!(ids.value(0) >= 1);
    }
}
