//! Physical execution plan for SQLite KNN vector search via sqlite-vec.

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use futures::stream;
use std::any::Any;
use std::fmt;
use std::sync::Arc;
use tokio_rusqlite::Connection;

use super::{quote_sqlite_ident, sqlite_values_to_arrow};
use crate::sources::providers::knn_utils::extract_query_vector;

/// Physical execution plan that runs a sqlite-vec KNN query on a vec0 virtual table.
///
/// Generates SQL of the form:
/// ```sql
/// SELECT <cols>, distance AS _score
/// FROM "<table>"
/// WHERE "<vector_col>" MATCH ?1
///   [AND <filter>]
/// ORDER BY distance
/// LIMIT <k>
/// ```
///
/// The query vector is passed as packed little-endian f32 bytes via a bind parameter.
/// The distance metric is determined by the vec0 table's `distance_metric` option
/// (set at creation time, e.g. `distance_metric=L2` or `distance_metric=cosine`).
#[derive(Debug, Clone)]
pub struct SqliteKnnExec {
    conn: Arc<Connection>,
    /// Table name (the vec0 virtual table).
    table_name: String,
    /// Name of the vector column in the vec0 table.
    vector_col: String,
    /// Pre-computed query vector (literal path). `None` when using subquery path.
    query_vector: Option<Vec<f32>>,
    /// Child plan that yields the query vector at execution time (subquery path).
    query_vector_plan: Option<Arc<dyn ExecutionPlan>>,
    /// Number of nearest neighbours to return.
    k: usize,
    /// Optional SQL WHERE predicate (no "WHERE" keyword).
    filter: Option<String>,
    /// Optional scan limit from an outer SQL LIMIT clause.
    scan_limit: Option<usize>,
    /// Output schema: non-vector columns + `_score Float64`.
    schema: SchemaRef,
    /// Cached DataFusion plan metadata.
    plan_properties: PlanProperties,
}

impl SqliteKnnExec {
    /// Literal vector path — query vector known at planning time.
    pub fn new(
        conn: Arc<Connection>,
        table_name: String,
        vector_col: String,
        query_vector: Vec<f32>,
        filter: Option<String>,
        schema: SchemaRef,
        k: usize,
    ) -> Self {
        let plan_properties = Self::make_properties(&schema);
        Self {
            conn,
            table_name,
            vector_col,
            query_vector: Some(query_vector),
            query_vector_plan: None,
            k,
            filter,
            scan_limit: None,
            schema,
            plan_properties,
        }
    }

    /// Subquery path — query vector extracted from `child` at execution time.
    pub fn new_with_subquery(
        conn: Arc<Connection>,
        table_name: String,
        vector_col: String,
        child: Arc<dyn ExecutionPlan>,
        filter: Option<String>,
        schema: SchemaRef,
        k: usize,
    ) -> Self {
        let plan_properties = Self::make_properties(&schema);
        Self {
            conn,
            table_name,
            vector_col,
            query_vector: None,
            query_vector_plan: Some(child),
            k,
            filter,
            scan_limit: None,
            schema,
            plan_properties,
        }
    }

    fn make_properties(schema: &SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }

    /// Set the scan limit (from an outer SQL LIMIT clause).
    pub fn with_scan_limit(mut self, limit: usize) -> Self {
        self.scan_limit = Some(limit);
        self
    }

    /// Build the SELECT column list from the output schema (excludes `_score`).
    fn select_columns(&self) -> String {
        self.schema
            .fields()
            .iter()
            .filter(|f| f.name() != "_score")
            .map(|f| quote_sqlite_ident(f.name()))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Build the vec0 KNN SELECT query.
    ///
    /// Uses `?1` as the bind parameter for the query vector (packed f32 bytes).
    pub(crate) fn build_query(&self) -> String {
        let cols = self.select_columns();
        let table = quote_sqlite_ident(&self.table_name);
        let vec_col = quote_sqlite_ident(&self.vector_col);
        let match_expr = format!("{vec_col} MATCH ?1");

        let effective_limit = self.scan_limit.map(|sl| sl.min(self.k)).unwrap_or(self.k);

        let mut where_parts = vec![match_expr];
        // vec0 uses k as a WHERE constraint, not SQL LIMIT.
        where_parts.push(format!("k = {effective_limit}"));
        if let Some(ref f) = self.filter {
            where_parts.push(f.clone());
        }
        let where_clause = format!(" WHERE {}", where_parts.join(" AND "));

        let score_expr = "distance AS _score";
        let select_list = if cols.is_empty() {
            score_expr.to_string()
        } else {
            format!("{cols}, {score_expr}")
        };

        format!(
            "SELECT {select_list} \
             FROM {table}{where_clause} \
             ORDER BY distance",
        )
    }

    /// Execute the query and return all rows as a single `RecordBatch`.
    async fn run(&self, context: Arc<TaskContext>) -> DFResult<RecordBatch> {
        // Resolve query vector: literal or from child plan.
        let query_vector: Vec<f32> = if let Some(ref vec) = self.query_vector {
            vec.clone()
        } else if let Some(ref plan) = self.query_vector_plan {
            match extract_query_vector(plan.clone(), context).await? {
                Some(vec) => vec,
                None => {
                    tracing::debug!(
                        "sqlite_knn: subquery returned no rows, returning empty result"
                    );
                    return Ok(RecordBatch::new_empty(self.schema.clone()));
                }
            }
        } else {
            return Err(DataFusionError::Internal(
                "SqliteKnnExec: both query_vector and query_vector_plan are absent".to_string(),
            ));
        };

        let sql = self.build_query();
        tracing::debug!("sqlite_knn SQL: {}", sql);

        let conn = Arc::clone(&self.conn);
        let schema = self.schema.clone();
        let num_cols = schema.fields().len();
        let field_types: Vec<DataType> = schema
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect();

        let col_values: Vec<Vec<tokio_rusqlite::rusqlite::types::Value>> = conn
            .call(
                move |conn| -> std::result::Result<_, tokio_rusqlite::rusqlite::Error> {
                    let mut stmt = conn.prepare(&sql)?;

                    // Convert query vector to packed f32 bytes for sqlite-vec MATCH.
                    let vec_bytes: Vec<u8> =
                        query_vector.iter().flat_map(|f| f.to_le_bytes()).collect();

                    let mut col_values: Vec<Vec<tokio_rusqlite::rusqlite::types::Value>> =
                        (0..num_cols).map(|_| Vec::new()).collect();

                    let mut rows = stmt.query([vec_bytes])?;
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
            .map_err(|e| DataFusionError::Execution(format!("sqlite_knn error: {e}")))?;

        let arrays: Vec<ArrayRef> = col_values
            .into_iter()
            .zip(field_types.iter())
            .map(|(values, data_type)| sqlite_values_to_arrow(&values, data_type))
            .collect();

        RecordBatch::try_new(schema, arrays)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl DisplayAs for SqliteKnnExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SqliteKnnExec: table={}, vector_col={}, k={}",
            self.table_name, self.vector_col, self.k
        )
    }
}

impl ExecutionPlan for SqliteKnnExec {
    fn name(&self) -> &str {
        "SqliteKnnExec"
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
            (true, 1) => Ok(Arc::new(SqliteKnnExec {
                query_vector_plan: Some(children[0].clone()),
                ..(*self).clone()
            })),
            (false, 0) => Ok(self),
            _ => Err(DataFusionError::Internal(format!(
                "SqliteKnnExec expected {} children, got {}",
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

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_exec(
        cols: Vec<(&str, DataType)>,
        vector_col: &str,
        filter: Option<&str>,
        k: usize,
    ) -> SqliteKnnExec {
        let conn = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { Connection::open_in_memory().await.unwrap() });
        let mut fields: Vec<Field> = cols
            .into_iter()
            .map(|(name, dt)| Field::new(name, dt, true))
            .collect();
        fields.push(Field::new("_score", DataType::Float64, true));
        let schema = Arc::new(Schema::new(fields));
        SqliteKnnExec::new(
            Arc::new(conn),
            "vec_items".to_string(),
            vector_col.to_string(),
            vec![0.1, 0.2, 0.3],
            filter.map(str::to_string),
            schema,
            k,
        )
    }

    #[test]
    fn test_build_query_uses_match() {
        let exec = make_exec(
            vec![("id", DataType::Int64), ("name", DataType::Utf8)],
            "embedding",
            None,
            10,
        );
        let sql = exec.build_query();
        assert!(
            sql.contains("MATCH ?1"),
            "query should use MATCH with parameter; sql={sql}"
        );
    }

    #[test]
    fn test_build_query_uses_k_in_where() {
        let exec = make_exec(vec![("id", DataType::Int64)], "embedding", None, 10);
        let sql = exec.build_query();
        assert!(
            sql.contains("k = 10"),
            "k should appear as WHERE constraint; sql={sql}"
        );
    }

    #[test]
    fn test_build_query_uses_distance_as_score() {
        let exec = make_exec(vec![("id", DataType::Int64)], "embedding", None, 10);
        let sql = exec.build_query();
        assert!(
            sql.contains("distance AS _score"),
            "should alias distance as _score; sql={sql}"
        );
    }

    #[test]
    fn test_build_query_orders_by_distance() {
        let exec = make_exec(vec![("id", DataType::Int64)], "embedding", None, 10);
        let sql = exec.build_query();
        assert!(
            sql.contains("ORDER BY distance"),
            "should order by distance; sql={sql}"
        );
    }

    #[test]
    fn test_build_query_with_filter() {
        let exec = make_exec(
            vec![("id", DataType::Int64)],
            "embedding",
            Some("category = 'news'"),
            10,
        );
        let sql = exec.build_query();
        assert!(
            sql.contains("category = 'news'"),
            "filter should appear in WHERE clause; sql={sql}"
        );
        assert!(
            sql.contains("AND"),
            "filter should be ANDed with MATCH and k; sql={sql}"
        );
    }

    #[test]
    fn test_build_query_k_values() {
        for k in [1, 5, 100] {
            let exec = make_exec(vec![("id", DataType::Int64)], "embedding", None, k);
            let sql = exec.build_query();
            assert!(
                sql.contains(&format!("k = {k}")),
                "expected k = {k}; sql={sql}"
            );
        }
    }

    #[test]
    fn test_build_query_quotes_vector_column() {
        let exec = make_exec(vec![("id", DataType::Int64)], "my embedding", None, 10);
        let sql = exec.build_query();
        assert!(
            sql.contains("\"my embedding\""),
            "vector column should be quoted; sql={sql}"
        );
    }

    #[test]
    fn test_select_columns_excludes_score() {
        let exec = make_exec(
            vec![("id", DataType::Int64), ("name", DataType::Utf8)],
            "embedding",
            None,
            10,
        );
        let cols = exec.select_columns();
        assert!(cols.contains("\"id\""));
        assert!(cols.contains("\"name\""));
        assert!(!cols.contains("_score"));
    }

    #[test]
    fn test_scan_limit_takes_minimum() {
        let exec =
            make_exec(vec![("id", DataType::Int64)], "embedding", None, 100).with_scan_limit(5);
        let sql = exec.build_query();
        assert!(
            sql.contains("k = 5"),
            "scan_limit < k should win; sql={sql}"
        );
    }

    #[test]
    fn test_scan_limit_does_not_exceed_function_k() {
        let exec =
            make_exec(vec![("id", DataType::Int64)], "embedding", None, 10).with_scan_limit(50);
        let sql = exec.build_query();
        assert!(
            sql.contains("k = 10"),
            "k < scan_limit should win; sql={sql}"
        );
    }
}
