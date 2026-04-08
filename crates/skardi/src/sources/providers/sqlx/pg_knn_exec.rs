//! Physical execution plan for pgvector KNN search.

use arrow::array::{
    ArrayRef, BooleanBuilder, Decimal128Builder, Float32Builder, Float64Builder, Int32Builder,
    Int64Builder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use futures::stream;
use sqlx::PgPool;
use sqlx::Row;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

use crate::sources::providers::knn_utils::extract_query_vector;

// ─── DistanceMetric ───────────────────────────────────────────────────────────

/// pgvector distance metric to use for KNN search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DistanceMetric {
    /// Negative inner product (`<#>`). Score is negative — lower is more similar.
    #[default]
    InnerProduct,
    /// Euclidean (L2) distance (`<->`). Lower is more similar.
    L2,
    /// Cosine distance (`<=>`). Lower is more similar.
    Cosine,
}

impl DistanceMetric {
    /// The pgvector SQL operator for this metric.
    pub fn operator(self) -> &'static str {
        match self {
            DistanceMetric::InnerProduct => "<#>",
            DistanceMetric::L2 => "<->",
            DistanceMetric::Cosine => "<=>",
        }
    }
}

impl std::str::FromStr for DistanceMetric {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "<#>" => Ok(DistanceMetric::InnerProduct),
            "<->" => Ok(DistanceMetric::L2),
            "<=>" => Ok(DistanceMetric::Cosine),
            other => Err(format!(
                "pg_knn: unknown metric '{}'. Expected one of: <#> (inner product), <-> (L2), <=> (cosine)",
                other
            )),
        }
    }
}

// ─── PgVectorFetchExec ───────────────────────────────────────────────────────

/// Leaf execution plan that fetches the query vector from Postgres via sqlx,
/// returning it as a single-row `StringArray` containing the pgvector text
/// representation (`[0.1,0.2,...]`).
///
/// This bypasses datafusion-table-providers' inability to decode the Postgres
/// `vector` type, while still producing an `ExecutionPlan` that the shared
/// `knn_utils::extract_query_vector` utility can consume.
#[derive(Clone)]
pub(super) struct PgVectorFetchExec {
    pool: Arc<PgPool>,
    /// SQL that returns `embedding::text` for one row.
    sql: String,
    schema: SchemaRef,
    plan_properties: PlanProperties,
}

impl PgVectorFetchExec {
    pub(super) fn new(pool: Arc<PgPool>, sql: String) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new("vec", DataType::Utf8, true)]));
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            pool,
            sql,
            schema,
            plan_properties,
        }
    }
}

impl fmt::Debug for PgVectorFetchExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PgVectorFetchExec(sql={})", self.sql)
    }
}

impl DisplayAs for PgVectorFetchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PgVectorFetchExec")
    }
}

impl ExecutionPlan for PgVectorFetchExec {
    fn name(&self) -> &str {
        "PgVectorFetchExec"
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
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "PgVectorFetchExec expects 0 children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let pool = Arc::clone(&self.pool);
        let sql = self.sql.clone();
        let schema = self.schema.clone();

        let fut = async move {
            let row = sqlx::query(&sql)
                .fetch_optional(pool.as_ref())
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!("pg_knn vector fetch error: {e}"))
                })?;

            let mut b = StringBuilder::new();
            match row {
                Some(r) => {
                    let text: String = r.try_get(0).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "pg_knn: failed to read vector as text: {e}"
                        ))
                    })?;
                    b.append_value(&text);
                }
                None => b.append_null(),
            }

            RecordBatch::try_new(schema, vec![Arc::new(b.finish()) as ArrayRef])
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream::once(fut),
        )))
    }
}

// ─── PgKnnExec ───────────────────────────────────────────────────────────────

/// Physical execution plan for pgvector KNN search.
///
/// Runs:
/// ```sql
/// SELECT <cols>, ("<vector_col>" <#> '<query>'::vector)::float8 AS _score
/// FROM "<schema>"."<table>"
/// [WHERE <filter>]
/// ORDER BY _score
/// LIMIT 10;
/// ```
///
/// The query vector may be supplied as a pre-computed `Vec<f32>` (literal path)
/// or deferred to a child `ExecutionPlan` (subquery path). For the subquery
/// path, a `PgVectorFetchExec` is used as the child — it fetches the vector
/// column as pgvector text via sqlx, which `knn_utils::extract_query_vector`
/// then parses into `Vec<f32>`.
#[derive(Debug, Clone)]
pub struct PgKnnExec {
    pool: Arc<PgPool>,
    /// Fully-qualified table (e.g. `"public"."modeldata"`)
    qualified_table: String,
    /// Name of the vector column (used in the distance expression)
    vector_col: String,
    /// Distance metric controlling which pgvector operator is used.
    metric: DistanceMetric,
    /// Number of nearest neighbours to return.
    k: usize,
    /// Pre-computed query vector (literal path). Empty when using subquery path.
    query_vector: Vec<f32>,
    /// Child plan that yields the query vector at execution time (subquery path).
    query_vector_plan: Option<Arc<dyn ExecutionPlan>>,
    /// Optional SQL WHERE predicate (no "WHERE" keyword)
    filter: Option<String>,
    /// Output schema: non-vector columns + `_score Float64`
    schema: SchemaRef,
    /// Cached DataFusion plan metadata (partitioning, emission type, boundedness)
    plan_properties: PlanProperties,
}

impl PgKnnExec {
    /// Literal vector path — query vector known at planning time.
    pub fn new(
        pool: Arc<PgPool>,
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
            schema,
            plan_properties,
        }
    }

    /// Subquery path — query vector extracted from `child` at execution time.
    ///
    /// Pass a `PgVectorFetchExec` as the child to handle the pgvector `vector`
    /// Postgres type correctly.
    pub fn new_with_subquery(
        pool: Arc<PgPool>,
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

    /// Build the SELECT column list from the output schema (excludes `_score`).
    ///
    /// Decimal128 columns are cast to `float8` so that sqlx can decode them
    /// without requiring the `rust-decimal`/`bigdecimal` feature.
    fn select_columns(&self) -> String {
        self.schema
            .fields()
            .iter()
            .filter(|f| f.name() != "_score")
            .map(|f| {
                let name = format!("\"{}\"", f.name().replace('"', "\"\""));
                match f.data_type() {
                    DataType::Decimal128(_, _) => format!("{name}::float8 AS {name}"),
                    _ => name,
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Format a query vector as a pgvector literal `'[0.1,0.2,...]'`.
    fn format_vector_literal(vec: &[f32]) -> String {
        let inner = vec
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(",");
        format!("[{}]", inner)
    }

    /// Build the full KNN SELECT query.
    fn build_query(&self, query_vector: &[f32]) -> String {
        let cols = self.select_columns();
        let vec_lit = Self::format_vector_literal(query_vector);
        let vec_col = self.vector_col.replace('"', "\"\"");
        let where_clause = self
            .filter
            .as_deref()
            .map(|f| format!(" WHERE {}", f))
            .unwrap_or_default();

        let op = self.metric.operator();
        let score_expr = format!("(\"{vec_col}\" {op} '{vec_lit}'::vector)::float8 AS _score");
        let select_list = if cols.is_empty() {
            score_expr
        } else {
            format!("{cols}, {score_expr}")
        };
        format!(
            "SELECT {select_list} \
             FROM {table}{where_clause} \
             ORDER BY _score \
             LIMIT {k}",
            table = self.qualified_table,
            k = self.k,
        )
    }

    /// Execute the query and return all rows as a single `RecordBatch`.
    async fn run(&self, context: Arc<TaskContext>) -> DFResult<RecordBatch> {
        let query_vector: Vec<f32> = if !self.query_vector.is_empty() {
            self.query_vector.clone()
        } else if let Some(ref plan) = self.query_vector_plan {
            match extract_query_vector(plan.clone(), context).await? {
                Some(vec) => vec,
                None => {
                    tracing::debug!("pg_knn: subquery returned no rows, returning empty result");
                    return Ok(RecordBatch::new_empty(self.schema.clone()));
                }
            }
        } else {
            unreachable!("PgKnnExec: both query_vector and query_vector_plan are absent");
        };

        let sql = self.build_query(&query_vector);
        tracing::debug!("pg_knn SQL: {}", sql);

        let rows = sqlx::query(&sql)
            .fetch_all(self.pool.as_ref())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        rows_to_batch(&rows, &self.schema)
    }
}

impl DisplayAs for PgKnnExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PgKnnExec: table={}, vector_col={}, metric={:?}, k={}{}",
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

impl ExecutionPlan for PgKnnExec {
    fn name(&self) -> &str {
        "PgKnnExec"
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
            (true, 1) => Ok(Arc::new(PgKnnExec {
                query_vector_plan: Some(children[0].clone()),
                ..(*self).clone()
            })),
            (false, 0) => Ok(self),
            _ => Err(DataFusionError::Internal(format!(
                "PgKnnExec expected {} children, got {}",
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

// ─── Row → RecordBatch conversion ───────────────────────────────────────────

fn rows_to_batch(rows: &[sqlx::postgres::PgRow], schema: &SchemaRef) -> DFResult<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        columns.push(build_column(rows, field.name(), field.data_type())?);
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn build_column(rows: &[sqlx::postgres::PgRow], col: &str, dtype: &DataType) -> DFResult<ArrayRef> {
    /// Convert a sqlx error into a DataFusion execution error for a named column.
    #[inline]
    fn decode_err(col: &str, expected: &str, e: sqlx::Error) -> DataFusionError {
        DataFusionError::Execution(format!(
            "pg_knn: type mismatch decoding column '{}' as {}: {}",
            col, expected, e
        ))
    }

    Ok(match dtype {
        DataType::Int32 => {
            let mut b = Int32Builder::new();
            for row in rows {
                b.append_option(
                    row.try_get::<Option<i32>, _>(col)
                        .map_err(|e| decode_err(col, "Int32", e))?,
                );
            }
            Arc::new(b.finish())
        }
        DataType::Int64 => {
            let mut b = Int64Builder::new();
            for row in rows {
                b.append_option(
                    row.try_get::<Option<i64>, _>(col)
                        .map_err(|e| decode_err(col, "Int64", e))?,
                );
            }
            Arc::new(b.finish())
        }
        DataType::Float32 => {
            let mut b = Float32Builder::new();
            for row in rows {
                b.append_option(
                    row.try_get::<Option<f32>, _>(col)
                        .map_err(|e| decode_err(col, "Float32", e))?,
                );
            }
            Arc::new(b.finish())
        }
        DataType::Float64 => {
            let mut b = Float64Builder::new();
            for row in rows {
                b.append_option(
                    row.try_get::<Option<f64>, _>(col)
                        .map_err(|e| decode_err(col, "Float64", e))?,
                );
            }
            Arc::new(b.finish())
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::new();
            for row in rows {
                b.append_option(
                    row.try_get::<Option<bool>, _>(col)
                        .map_err(|e| decode_err(col, "Boolean", e))?,
                );
            }
            Arc::new(b.finish())
        }
        // NUMERIC/DECIMAL: read as f64 (the SELECT casts to float8) then scale to i128.
        DataType::Decimal128(_, scale) => {
            let scale_factor = 10i128.pow(*scale as u32);
            let mut b = Decimal128Builder::new().with_data_type(dtype.clone());
            for row in rows {
                match row
                    .try_get::<Option<f64>, _>(col)
                    .map_err(|e| decode_err(col, "Decimal128 (via float8)", e))?
                {
                    Some(v) => b.append_value((v * scale_factor as f64).round() as i128),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        // Utf8 catch-all: text, varchar, uuid, json, timestamp, date, etc.
        // Decode failures are non-fatal here because schema inference maps many
        // Postgres types to Utf8 as a best effort; warn and emit null instead.
        _ => {
            let mut b = StringBuilder::new();
            for row in rows {
                match row.try_get::<Option<String>, _>(col) {
                    Ok(Some(s)) => b.append_value(&s),
                    Ok(None) => b.append_null(),
                    Err(e) => {
                        tracing::warn!(
                            column = col,
                            error = %e,
                            "pg_knn: failed to decode column as Utf8, emitting null"
                        );
                        b.append_null();
                    }
                }
            }
            Arc::new(b.finish())
        }
    })
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::str::FromStr;

    /// Build a `PgKnnExec` with a lazy (never-connecting) pool for query-building tests.
    fn make_exec(
        cols: Vec<(&str, DataType)>,
        metric: DistanceMetric,
        filter: Option<&str>,
        k: usize,
    ) -> PgKnnExec {
        let pool =
            Arc::new(sqlx::PgPool::connect_lazy("postgresql://localhost/test").expect("lazy pool"));
        let mut fields: Vec<Field> = cols
            .into_iter()
            .map(|(name, dt)| Field::new(name, dt, true))
            .collect();
        fields.push(Field::new("_score", DataType::Float64, true));
        let schema = Arc::new(Schema::new(fields));
        PgKnnExec::new(
            pool,
            "\"public\".\"docs\"".to_string(),
            "embedding".to_string(),
            vec![0.1, 0.2, 0.3],
            filter.map(str::to_string),
            schema,
            metric,
            k,
        )
    }

    // ── DistanceMetric::from_str ──────────────────────────────────────────

    #[test]
    fn test_metric_from_str_valid() {
        assert_eq!(
            DistanceMetric::from_str("<#>").unwrap(),
            DistanceMetric::InnerProduct
        );
        assert_eq!(DistanceMetric::from_str("<->").unwrap(), DistanceMetric::L2);
        assert_eq!(
            DistanceMetric::from_str("<=>").unwrap(),
            DistanceMetric::Cosine
        );
    }

    #[test]
    fn test_metric_from_str_invalid() {
        assert!(DistanceMetric::from_str("inner_product").is_err());
        assert!(DistanceMetric::from_str("l2").is_err());
        assert!(DistanceMetric::from_str("cosine").is_err());
        assert!(DistanceMetric::from_str("").is_err());
    }

    #[test]
    fn test_metric_from_str_error_lists_valid_operators() {
        let err = DistanceMetric::from_str("bad").unwrap_err();
        assert!(err.contains("<#>") && err.contains("<->") && err.contains("<=>"));
    }

    // ── DistanceMetric::operator ──────────────────────────────────────────

    #[test]
    fn test_metric_operator_round_trips() {
        for metric in [
            DistanceMetric::InnerProduct,
            DistanceMetric::L2,
            DistanceMetric::Cosine,
        ] {
            let op = metric.operator();
            assert_eq!(DistanceMetric::from_str(op).unwrap(), metric);
        }
    }

    // ── DistanceMetric::default ───────────────────────────────────────────

    #[test]
    fn test_metric_default_is_inner_product() {
        assert_eq!(DistanceMetric::default(), DistanceMetric::InnerProduct);
    }

    // ── format_vector_literal ─────────────────────────────────────────────

    #[test]
    fn test_format_vector_literal() {
        assert_eq!(
            PgKnnExec::format_vector_literal(&[0.1, 0.2, 0.3]),
            "[0.1,0.2,0.3]"
        );
    }

    #[test]
    fn test_format_vector_literal_empty() {
        assert_eq!(PgKnnExec::format_vector_literal(&[]), "[]");
    }

    #[test]
    fn test_format_vector_literal_single() {
        // f32 1.0 serialises as "1", not "1.0"
        assert_eq!(PgKnnExec::format_vector_literal(&[1.0]), "[1]");
    }

    // ── select_columns ────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_select_columns_excludes_score() {
        let exec = make_exec(
            vec![("id", DataType::Int64), ("content", DataType::Utf8)],
            DistanceMetric::InnerProduct,
            None,
            10,
        );
        let cols = exec.select_columns();
        assert!(cols.contains("\"id\""));
        assert!(cols.contains("\"content\""));
        assert!(!cols.contains("_score"));
    }

    #[tokio::test]
    async fn test_select_columns_casts_decimal_to_float8() {
        let exec = make_exec(
            vec![("price", DataType::Decimal128(10, 2))],
            DistanceMetric::InnerProduct,
            None,
            10,
        );
        let cols = exec.select_columns();
        assert!(
            cols.contains("::float8"),
            "Decimal128 columns must be cast to float8"
        );
    }

    // ── build_query ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_build_query_uses_correct_operator() {
        for (metric, op) in [
            (DistanceMetric::InnerProduct, "<#>"),
            (DistanceMetric::L2, "<->"),
            (DistanceMetric::Cosine, "<=>"),
        ] {
            let exec = make_exec(vec![("id", DataType::Int64)], metric, None, 10);
            let sql = exec.build_query(&[0.1, 0.2]);
            assert!(
                sql.contains(op),
                "metric {metric:?} should use operator {op}"
            );
        }
    }

    #[tokio::test]
    async fn test_build_query_limit() {
        for k in [1, 5, 100] {
            let exec = make_exec(vec![("id", DataType::Int64)], DistanceMetric::L2, None, k);
            let sql = exec.build_query(&[0.1]);
            assert!(sql.contains(&format!("LIMIT {k}")));
        }
    }

    #[tokio::test]
    async fn test_build_query_vector_literal_embedded() {
        let exec = make_exec(
            vec![("id", DataType::Int64)],
            DistanceMetric::InnerProduct,
            None,
            10,
        );
        let sql = exec.build_query(&[0.5, 0.25]);
        assert!(sql.contains("'[0.5,0.25]'::vector"));
    }

    #[tokio::test]
    async fn test_build_query_with_filter() {
        let exec = make_exec(
            vec![("id", DataType::Int64)],
            DistanceMetric::L2,
            Some("category = 'news'"),
            10,
        );
        let sql = exec.build_query(&[0.1, 0.2]);
        assert!(sql.contains("WHERE category = 'news'"));
    }

    #[tokio::test]
    async fn test_build_query_without_filter_has_no_where() {
        let exec = make_exec(
            vec![("id", DataType::Int64)],
            DistanceMetric::Cosine,
            None,
            10,
        );
        let sql = exec.build_query(&[0.1]);
        assert!(!sql.contains("WHERE"));
    }

    #[tokio::test]
    async fn test_build_query_quotes_vector_column() {
        let exec = make_exec(
            vec![("id", DataType::Int64)],
            DistanceMetric::Cosine,
            None,
            10,
        );
        let sql = exec.build_query(&[0.1]);
        assert!(sql.contains("\"embedding\""));
    }
}
