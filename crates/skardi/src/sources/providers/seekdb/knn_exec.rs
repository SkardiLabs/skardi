//! Physical execution plan for SeekDB native KNN vector search.
//!
//! SeekDB (OceanBase) stores vectors in a native `VECTOR` column indexed with
//! HNSW. The query shape we generate uses SeekDB's distance functions:
//!
//! ```sql
//! SELECT <cols>, <dist_fn>(`<vector_col>`, ?) AS `_score`
//! FROM <qualified_table>
//! [WHERE <filter>]
//! ORDER BY <dist_fn>(`<vector_col>`, ?)
//! LIMIT <k>
//! ```
//!
//! where `<dist_fn>` is one of `L2_DISTANCE`, `COSINE_DISTANCE`, or
//! `INNER_PRODUCT`, and the query vector is bound as a JSON-encoded array
//! (SeekDB accepts the `[x,y,z]` textual form for `VECTOR` values).
//!
//! The raw distance expression is duplicated in SELECT and ORDER BY so that
//! SeekDB's HNSW index can be used — a subquery alias or `_score` reference
//! in ORDER BY would prevent the optimiser from matching the index expression.
//!
//! Lower `_score` means more similar, matching `pg_knn` and `sqlite_knn`.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::mysqlconn::MySQLConnection;
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use futures::stream;
use mysql_async::prelude::*;
use mysql_async::{Params, Row, Value};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

use super::fts_exec::rows_to_batch;
use super::quote_seekdb_ident;
use crate::sources::providers::knn_utils::extract_query_vector;

// ─── DistanceMetric ──────────────────────────────────────────────────────────

/// SeekDB KNN distance metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DistanceMetric {
    /// Euclidean (L2) distance. Lower is more similar.
    #[default]
    L2,
    /// Cosine distance. Lower is more similar.
    Cosine,
    /// Negative inner product. Lower is more similar.
    InnerProduct,
}

impl DistanceMetric {
    /// The SeekDB distance function name for this metric.
    pub fn function(self) -> &'static str {
        match self {
            DistanceMetric::L2 => "L2_DISTANCE",
            DistanceMetric::Cosine => "COSINE_DISTANCE",
            DistanceMetric::InnerProduct => "NEGATIVE_INNER_PRODUCT",
        }
    }
}

impl std::str::FromStr for DistanceMetric {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Accept both symbolic forms (shared with pg_knn) and SeekDB names for
        // convenience — the caller shouldn't have to memorise which syntax a
        // given backend uses.
        match s.to_ascii_lowercase().as_str() {
            "l2" | "l2_distance" | "<->" => Ok(DistanceMetric::L2),
            "cosine" | "cosine_distance" | "<=>" => Ok(DistanceMetric::Cosine),
            "inner_product" | "ip" | "<#>" => Ok(DistanceMetric::InnerProduct),
            other => Err(format!(
                "seekdb_knn: unknown metric '{other}'. Expected one of: l2, cosine, inner_product"
            )),
        }
    }
}

/// Format a `Vec<f32>` in SeekDB VECTOR literal form: `[0.1,0.2,0.3]`.
pub(crate) fn format_vector_literal(v: &[f32]) -> String {
    let parts: Vec<String> = v.iter().map(|x| format!("{}", x)).collect();
    format!("[{}]", parts.join(","))
}

// ─── SeekDbKnnExec ───────────────────────────────────────────────────────────

/// Physical execution plan for SeekDB KNN search.
#[derive(Debug, Clone)]
pub struct SeekDbKnnExec {
    pool: Arc<MySQLConnectionPool>,
    qualified_table: String,
    vector_col: String,
    metric: DistanceMetric,
    k: usize,
    /// Pre-computed query vector (literal path). Empty when using subquery path.
    query_vector: Vec<f32>,
    /// Child plan that yields the query vector at execution time (subquery path).
    query_vector_plan: Option<Arc<dyn ExecutionPlan>>,
    filter: Option<String>,
    scan_limit: Option<usize>,
    schema: SchemaRef,
    plan_properties: PlanProperties,
}

impl SeekDbKnnExec {
    pub fn new(
        pool: Arc<MySQLConnectionPool>,
        qualified_table: String,
        vector_col: String,
        query_vector: Vec<f32>,
        filter: Option<String>,
        schema: SchemaRef,
        metric: DistanceMetric,
        k: usize,
    ) -> Self {
        let plan_properties = Self::make_properties(&schema);
        Self {
            pool,
            qualified_table,
            vector_col,
            metric,
            k,
            query_vector,
            query_vector_plan: None,
            filter,
            scan_limit: None,
            schema,
            plan_properties,
        }
    }

    pub fn new_with_subquery(
        pool: Arc<MySQLConnectionPool>,
        qualified_table: String,
        vector_col: String,
        child: Arc<dyn ExecutionPlan>,
        filter: Option<String>,
        schema: SchemaRef,
        metric: DistanceMetric,
        k: usize,
    ) -> Self {
        let plan_properties = Self::make_properties(&schema);
        Self {
            pool,
            qualified_table,
            vector_col,
            metric,
            k,
            query_vector: Vec::new(),
            query_vector_plan: Some(child),
            filter,
            scan_limit: None,
            schema,
            plan_properties,
        }
    }

    pub fn with_scan_limit(mut self, limit: usize) -> Self {
        self.scan_limit = Some(limit);
        self
    }

    fn make_properties(schema: &SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }

    /// Build the SELECT column list from the output schema (excludes `_score`).
    fn select_columns(&self) -> String {
        self.schema
            .fields()
            .iter()
            .filter(|f| f.name() != "_score")
            .map(|f| quote_seekdb_ident(f.name()))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Build the parameterised KNN SELECT query. Two `?` placeholders bind the
    /// same query vector — once in the SELECT list (score), once in the
    /// ORDER BY (so the HNSW index can match the exact expression).
    pub(crate) fn build_query(&self) -> String {
        let cols = self.select_columns();
        let vec_col = quote_seekdb_ident(&self.vector_col);
        let where_clause = self
            .filter
            .as_deref()
            .map(|f| format!(" WHERE {}", f))
            .unwrap_or_default();

        let dist_fn = self.metric.function();
        let dist_expr = format!("{dist_fn}({vec_col}, ?)");
        let score_expr = format!("{dist_expr} AS `_score`");
        let select_list = if cols.is_empty() {
            score_expr
        } else {
            format!("{cols}, {score_expr}")
        };
        format!(
            "SELECT {select_list} \
             FROM {table}{where_clause} \
             ORDER BY {dist_expr} \
             LIMIT {k}",
            table = self.qualified_table,
            k = self.k,
        )
    }

    async fn run(&self, context: Arc<TaskContext>) -> DFResult<RecordBatch> {
        let query_vector: Vec<f32> = if !self.query_vector.is_empty() {
            self.query_vector.clone()
        } else if let Some(ref plan) = self.query_vector_plan {
            match extract_query_vector(plan.clone(), context).await? {
                Some(vec) => vec,
                None => {
                    tracing::debug!(
                        "seekdb_knn: subquery returned no rows, returning empty result"
                    );
                    return Ok(RecordBatch::new_empty(self.schema.clone()));
                }
            }
        } else {
            return Err(DataFusionError::Internal(
                "SeekDbKnnExec: both query_vector and query_vector_plan are absent".to_string(),
            ));
        };

        let sql = self.build_query();
        tracing::debug!("seekdb_knn SQL: {}", sql);

        let conn_obj =
            self.pool.connect().await.map_err(|e| {
                DataFusionError::Execution(format!("seekdb_knn connect error: {e}"))
            })?;

        let mysql_conn = conn_obj
            .as_any()
            .downcast_ref::<MySQLConnection>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "seekdb_knn: unexpected connection type from pool".to_string(),
                )
            })?;

        let mut conn = mysql_conn.conn.lock().await;

        let lit = format_vector_literal(&query_vector);
        let params = Params::Positional(vec![Value::from(lit.clone()), Value::from(lit)]);
        let rows: Vec<Row> = conn
            .exec(sql.as_str(), params)
            .await
            .map_err(|e| DataFusionError::Execution(format!("seekdb_knn execute error: {e}")))?;

        let mut batch = rows_to_batch(&rows, &self.schema)?;
        if let Some(n) = self.scan_limit {
            if batch.num_rows() > n {
                batch = batch.slice(0, n);
            }
        }
        Ok(batch)
    }
}

impl DisplayAs for SeekDbKnnExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SeekDbKnnExec: table={}, vector_col={}, metric={:?}, k={}{}",
            self.qualified_table,
            self.vector_col,
            self.metric,
            self.k,
            if self.query_vector_plan.is_some() {
                " (subquery)"
            } else {
                ""
            }
        )
    }
}

impl ExecutionPlan for SeekDbKnnExec {
    fn name(&self) -> &str {
        "SeekDbKnnExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        if let Some(ref plan) = self.query_vector_plan {
            vec![plan]
        } else {
            vec![]
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        match (self.query_vector_plan.is_some(), children.len()) {
            (true, 1) => Ok(Arc::new(SeekDbKnnExec {
                query_vector_plan: Some(children[0].clone()),
                ..(*self).clone()
            })),
            (false, 0) => Ok(self),
            _ => Err(DataFusionError::Internal(format!(
                "SeekDbKnnExec expected {} children, got {}",
                if self.query_vector_plan.is_some() {
                    1
                } else {
                    0
                },
                children.len()
            ))),
        }
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let exec = self.clone();
        let schema = self.schema.clone();
        let fut = async move { exec.run(context).await };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(fut),
        )))
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────
// Query-building tests drive a `QueryBuilder` shim so they don't require a
// real MySQLConnectionPool (and therefore no running SeekDB instance).

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct QueryBuilder {
    pub qualified_table: String,
    pub vector_col: String,
    pub metric: DistanceMetric,
    pub k: usize,
    pub filter: Option<String>,
    pub schema: SchemaRef,
}

#[cfg(test)]
impl QueryBuilder {
    pub(crate) fn select_columns(&self) -> String {
        self.schema
            .fields()
            .iter()
            .filter(|f| f.name() != "_score")
            .map(|f| quote_seekdb_ident(f.name()))
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub(crate) fn build_query(&self) -> String {
        let cols = self.select_columns();
        let vec_col = quote_seekdb_ident(&self.vector_col);
        let where_clause = self
            .filter
            .as_deref()
            .map(|f| format!(" WHERE {}", f))
            .unwrap_or_default();

        let dist_fn = self.metric.function();
        let dist_expr = format!("{dist_fn}({vec_col}, ?)");
        let score_expr = format!("{dist_expr} AS `_score`");
        let select_list = if cols.is_empty() {
            score_expr
        } else {
            format!("{cols}, {score_expr}")
        };
        format!(
            "SELECT {select_list} \
             FROM {table}{where_clause} \
             ORDER BY {dist_expr} \
             LIMIT {k}",
            table = self.qualified_table,
            k = self.k,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::str::FromStr;

    fn make_builder(
        cols: Vec<(&str, DataType)>,
        metric: DistanceMetric,
        filter: Option<&str>,
        k: usize,
    ) -> QueryBuilder {
        let mut fields: Vec<Field> = cols
            .into_iter()
            .map(|(name, dt)| Field::new(name, dt, true))
            .collect();
        fields.push(Field::new("_score", DataType::Float64, true));
        let schema = Arc::new(Schema::new(fields));
        QueryBuilder {
            qualified_table: "`mydb`.`docs`".to_string(),
            vector_col: "embedding".to_string(),
            metric,
            k,
            filter: filter.map(str::to_string),
            schema,
        }
    }

    #[test]
    fn test_metric_from_str_valid_l2() {
        assert_eq!(DistanceMetric::from_str("l2").unwrap(), DistanceMetric::L2);
        assert_eq!(
            DistanceMetric::from_str("L2_DISTANCE").unwrap(),
            DistanceMetric::L2
        );
        assert_eq!(DistanceMetric::from_str("<->").unwrap(), DistanceMetric::L2);
    }

    #[test]
    fn test_metric_from_str_valid_cosine() {
        assert_eq!(
            DistanceMetric::from_str("cosine").unwrap(),
            DistanceMetric::Cosine
        );
        assert_eq!(
            DistanceMetric::from_str("cosine_distance").unwrap(),
            DistanceMetric::Cosine
        );
        assert_eq!(
            DistanceMetric::from_str("<=>").unwrap(),
            DistanceMetric::Cosine
        );
    }

    #[test]
    fn test_metric_from_str_valid_inner_product() {
        assert_eq!(
            DistanceMetric::from_str("inner_product").unwrap(),
            DistanceMetric::InnerProduct
        );
        assert_eq!(
            DistanceMetric::from_str("<#>").unwrap(),
            DistanceMetric::InnerProduct
        );
    }

    #[test]
    fn test_metric_from_str_invalid() {
        assert!(DistanceMetric::from_str("nope").is_err());
        assert!(DistanceMetric::from_str("").is_err());
    }

    #[test]
    fn test_metric_default_is_l2() {
        assert_eq!(DistanceMetric::default(), DistanceMetric::L2);
    }

    #[test]
    fn test_format_vector_literal() {
        assert_eq!(format_vector_literal(&[1.0, 2.0, 3.0]), "[1,2,3]");
        assert_eq!(format_vector_literal(&[0.1_f32, 0.2_f32]), "[0.1,0.2]");
        assert_eq!(format_vector_literal(&[]), "[]");
    }

    #[test]
    fn test_build_query_uses_positional_params() {
        let b = make_builder(vec![("id", DataType::Int64)], DistanceMetric::L2, None, 10);
        let sql = b.build_query();
        // The vector placeholder (`?`) appears twice — once in SELECT, once in ORDER BY.
        assert_eq!(
            sql.matches('?').count(),
            2,
            "expected exactly 2 '?' placeholders; sql={sql}"
        );
    }

    #[test]
    fn test_build_query_uses_correct_function() {
        for (metric, fn_name) in [
            (DistanceMetric::L2, "L2_DISTANCE"),
            (DistanceMetric::Cosine, "COSINE_DISTANCE"),
            (DistanceMetric::InnerProduct, "NEGATIVE_INNER_PRODUCT"),
        ] {
            let b = make_builder(vec![("id", DataType::Int64)], metric, None, 10);
            let sql = b.build_query();
            assert!(
                sql.contains(fn_name),
                "metric {metric:?} should use {fn_name}; sql={sql}"
            );
        }
    }

    #[test]
    fn test_build_query_order_by_uses_raw_distance_expr() {
        let b = make_builder(vec![("id", DataType::Int64)], DistanceMetric::L2, None, 5);
        let sql = b.build_query();
        assert!(
            sql.contains("ORDER BY L2_DISTANCE(`embedding`, ?)"),
            "ORDER BY must use the raw distance expression for index use; sql={sql}"
        );
    }

    #[test]
    fn test_build_query_limit() {
        for k in [1usize, 5, 100] {
            let b = make_builder(vec![("id", DataType::Int64)], DistanceMetric::L2, None, k);
            let sql = b.build_query();
            assert!(sql.contains(&format!("LIMIT {k}")));
        }
    }

    #[test]
    fn test_build_query_with_filter() {
        let b = make_builder(
            vec![("id", DataType::Int64)],
            DistanceMetric::L2,
            Some("category = 'news'"),
            10,
        );
        let sql = b.build_query();
        assert!(sql.contains("WHERE category = 'news'"));
    }

    #[test]
    fn test_build_query_without_filter_has_no_where() {
        let b = make_builder(
            vec![("id", DataType::Int64)],
            DistanceMetric::Cosine,
            None,
            10,
        );
        let sql = b.build_query();
        assert!(!sql.contains("WHERE"));
    }

    #[test]
    fn test_build_query_quotes_vector_column() {
        let b = make_builder(
            vec![("id", DataType::Int64)],
            DistanceMetric::Cosine,
            None,
            10,
        );
        let sql = b.build_query();
        assert!(sql.contains("`embedding`"));
    }

    #[test]
    fn test_select_columns_excludes_score() {
        let b = make_builder(
            vec![("id", DataType::Int64), ("title", DataType::Utf8)],
            DistanceMetric::L2,
            None,
            10,
        );
        let cols = b.select_columns();
        assert!(cols.contains("`id`"));
        assert!(cols.contains("`title`"));
        assert!(!cols.contains("_score"));
    }
}
