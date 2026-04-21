//! Physical execution plan for SeekDB (OceanBase) native full-text search.
//!
//! SeekDB exposes MySQL-compatible `FULLTEXT` indexes with the IK parser.
//! The physical SQL shape we generate is:
//!
//! ```sql
//! SELECT <cols>,
//!        MATCH(`<text_col>`) AGAINST (? IN NATURAL LANGUAGE MODE) AS `_score`
//! FROM <qualified_table>
//! WHERE MATCH(`<text_col>`) AGAINST (? IN NATURAL LANGUAGE MODE)
//!   [AND <filter>]
//! ORDER BY `_score` DESC
//! LIMIT <limit>
//! ```
//!
//! The same search term is bound twice — once for scoring, once for filtering —
//! via `mysql_async`'s positional parameters. Higher `_score` means more
//! relevant, matching the convention used by `pg_fts` and `sqlite_fts`.

use arrow::array::{
    ArrayRef, BooleanBuilder, Decimal128Builder, Float32Builder, Float64Builder, Int32Builder,
    Int64Builder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, SchemaRef};
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

use super::quote_seekdb_ident;

/// Physical execution plan that runs a SeekDB full-text search using
/// `MATCH() AGAINST()` and returns matching rows with a `_score` column.
#[derive(Debug, Clone)]
pub struct SeekDbFtsExec {
    pool: Arc<MySQLConnectionPool>,
    /// Fully-qualified table identifier (e.g. ``` `mydb`.`articles` ```).
    qualified_table: String,
    /// Name of the text column that has the FULLTEXT index.
    text_col: String,
    /// The user's search query string.
    query: String,
    /// Maximum number of results to return.
    limit: usize,
    /// Optional SQL WHERE predicate (no "WHERE" keyword).
    filter: Option<String>,
    /// Optional scan limit from an outer SQL LIMIT clause.
    scan_limit: Option<usize>,
    /// Output schema: table columns + `_score Float64`.
    schema: SchemaRef,
    plan_properties: PlanProperties,
}

impl SeekDbFtsExec {
    pub fn new(
        pool: Arc<MySQLConnectionPool>,
        qualified_table: String,
        text_col: String,
        query: String,
        limit: usize,
        filter: Option<String>,
        schema: SchemaRef,
    ) -> Self {
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            pool,
            qualified_table,
            text_col,
            query,
            limit,
            filter,
            scan_limit: None,
            schema,
            plan_properties,
        }
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
            .map(|f| quote_seekdb_ident(f.name()))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Build the FTS search SELECT query. Uses positional parameters for the
    /// query string so the user input is never interpolated into SQL.
    pub(crate) fn build_query(&self) -> String {
        let cols = self.select_columns();
        let text_col = quote_seekdb_ident(&self.text_col);
        let match_expr = format!("MATCH({text_col}) AGAINST (? IN NATURAL LANGUAGE MODE)");
        let score_expr = format!("{match_expr} AS `_score`");

        let mut where_parts = vec![match_expr.clone()];
        if let Some(ref f) = self.filter {
            where_parts.push(f.clone());
        }
        let where_clause = format!(" WHERE {}", where_parts.join(" AND "));

        let effective_limit = self
            .scan_limit
            .map(|sl| sl.min(self.limit))
            .unwrap_or(self.limit);

        let select_list = if cols.is_empty() {
            score_expr
        } else {
            format!("{cols}, {score_expr}")
        };

        format!(
            "SELECT {select_list} \
             FROM {table}{where_clause} \
             ORDER BY `_score` DESC \
             LIMIT {limit}",
            table = self.qualified_table,
            limit = effective_limit,
        )
    }

    /// Execute the query and return all rows as a single `RecordBatch`.
    async fn run(&self) -> DFResult<RecordBatch> {
        let sql = self.build_query();
        tracing::debug!("seekdb_fts SQL: {}", sql);

        let conn_obj =
            self.pool.connect().await.map_err(|e| {
                DataFusionError::Execution(format!("seekdb_fts connect error: {e}"))
            })?;

        let mysql_conn = conn_obj
            .as_any()
            .downcast_ref::<MySQLConnection>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "seekdb_fts: unexpected connection type from pool".to_string(),
                )
            })?;

        let mut conn = mysql_conn.conn.lock().await;

        // The query string appears twice in the SQL — once for scoring, once
        // for filtering — so we bind it twice.
        let q = self.query.clone();
        let params = Params::Positional(vec![Value::from(q.clone()), Value::from(q)]);
        let rows: Vec<Row> = conn
            .exec(sql.as_str(), params)
            .await
            .map_err(|e| DataFusionError::Execution(format!("seekdb_fts execute error: {e}")))?;

        rows_to_batch(&rows, &self.schema)
    }
}

impl DisplayAs for SeekDbFtsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SeekDbFtsExec: table={}, text_col={}, limit={}",
            self.qualified_table, self.text_col, self.limit
        )
    }
}

impl ExecutionPlan for SeekDbFtsExec {
    fn name(&self) -> &str {
        "SeekDbFtsExec"
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
                "SeekDbFtsExec expects 0 children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let exec = self.clone();
        let schema = self.schema.clone();
        let fut = async move { exec.run().await };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(fut),
        )))
    }
}

// ─── Row → RecordBatch ──────────────────────────────────────────────────────

/// Convert a slice of `mysql_async::Row`s into an Arrow `RecordBatch` matching
/// the given schema. Shared by `SeekDbFtsExec` and `SeekDbKnnExec`.
pub(crate) fn rows_to_batch(rows: &[Row], schema: &SchemaRef) -> DFResult<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        columns.push(build_column(rows, field.name(), field.data_type())?);
    }
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn build_column(rows: &[Row], col: &str, dtype: &DataType) -> DFResult<ArrayRef> {
    /// Convert a mysql_async row-decode error into a DataFusion execution error.
    #[inline]
    fn decode_err(col: &str, expected: &str, detail: impl std::fmt::Display) -> DataFusionError {
        DataFusionError::Execution(format!(
            "seekdb: type mismatch decoding column '{}' as {}: {}",
            col, expected, detail
        ))
    }

    Ok(match dtype {
        DataType::Int32 => {
            let mut b = Int32Builder::new();
            for row in rows {
                let v: Option<i32> = row.get(col).ok_or_else(|| {
                    decode_err(col, "Int32", "column not present or null encoding invalid")
                })?;
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        DataType::Int64 => {
            let mut b = Int64Builder::new();
            for row in rows {
                let v: Option<i64> = row.get(col).ok_or_else(|| {
                    decode_err(col, "Int64", "column not present or null encoding invalid")
                })?;
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        DataType::Float32 => {
            let mut b = Float32Builder::new();
            for row in rows {
                let v: Option<f32> = row.get(col).ok_or_else(|| {
                    decode_err(
                        col,
                        "Float32",
                        "column not present or null encoding invalid",
                    )
                })?;
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        DataType::Float64 => {
            let mut b = Float64Builder::new();
            for row in rows {
                let v: Option<f64> = row.get(col).ok_or_else(|| {
                    decode_err(
                        col,
                        "Float64",
                        "column not present or null encoding invalid",
                    )
                })?;
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::new();
            for row in rows {
                let v: Option<bool> = row.get(col).ok_or_else(|| {
                    decode_err(
                        col,
                        "Boolean",
                        "column not present or null encoding invalid",
                    )
                })?;
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        DataType::Decimal128(_, scale) => {
            // SeekDB returns DECIMAL as a string via the text protocol. We
            // parse through f64 for simplicity, which loses precision beyond
            // ~15-17 significant digits — acceptable for typical money /
            // ratio columns but wrong for decimal(38, N) values carrying more
            // than ~17 digits of payload. Switch to a string-based decimal
            // parser (e.g. rust_decimal or manual digit shifting) if a
            // high-precision use case surfaces.
            let scale_factor = 10i128.pow(*scale as u32);
            let mut b = Decimal128Builder::new().with_data_type(dtype.clone());
            for row in rows {
                let v: Option<String> = row.get(col).ok_or_else(|| {
                    decode_err(col, "Decimal128 (via string)", "column not present")
                })?;
                match v {
                    Some(s) => {
                        let parsed: f64 = s
                            .parse()
                            .map_err(|e| decode_err(col, "Decimal128 (via string)", e))?;
                        b.append_value((parsed * scale_factor as f64).round() as i128);
                    }
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        // Utf8 catch-all: varchar, text, timestamp, date, json, etc.
        _ => {
            let mut b = StringBuilder::new();
            for row in rows {
                let v: Option<String> = row.get(col).ok_or_else(|| {
                    decode_err(col, "Utf8", "column not present or null encoding invalid")
                })?;
                match v {
                    Some(s) => b.append_value(&s),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
    })
}

// ─── Tests ──────────────────────────────────────────────────────────────────
// Query-building tests avoid instantiating a real SeekDbFtsExec (which would
// require a live MySQLConnectionPool) by driving `QueryBuilder` directly — a
// tiny shim holding the same pieces as the exec.

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct QueryBuilder {
    pub qualified_table: String,
    pub text_col: String,
    pub limit: usize,
    pub filter: Option<String>,
    pub scan_limit: Option<usize>,
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
        let text_col = quote_seekdb_ident(&self.text_col);
        let match_expr = format!("MATCH({text_col}) AGAINST (? IN NATURAL LANGUAGE MODE)");
        let score_expr = format!("{match_expr} AS `_score`");

        let mut where_parts = vec![match_expr];
        if let Some(ref f) = self.filter {
            where_parts.push(f.clone());
        }
        let where_clause = format!(" WHERE {}", where_parts.join(" AND "));

        let effective_limit = self
            .scan_limit
            .map(|sl| sl.min(self.limit))
            .unwrap_or(self.limit);

        let select_list = if cols.is_empty() {
            score_expr
        } else {
            format!("{cols}, {score_expr}")
        };

        format!(
            "SELECT {select_list} \
             FROM {table}{where_clause} \
             ORDER BY `_score` DESC \
             LIMIT {limit}",
            table = self.qualified_table,
            limit = effective_limit,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_builder(
        cols: Vec<(&str, DataType)>,
        text_col: &str,
        filter: Option<&str>,
        limit: usize,
    ) -> QueryBuilder {
        let mut fields: Vec<Field> = cols
            .into_iter()
            .map(|(name, dt)| Field::new(name, dt, true))
            .collect();
        fields.push(Field::new("_score", DataType::Float64, true));
        let schema = Arc::new(Schema::new(fields));
        QueryBuilder {
            qualified_table: "`mydb`.`articles`".to_string(),
            text_col: text_col.to_string(),
            limit,
            filter: filter.map(str::to_string),
            scan_limit: None,
            schema,
        }
    }

    #[test]
    fn test_build_query_uses_match_against() {
        let b = make_builder(
            vec![("id", DataType::Int64), ("content", DataType::Utf8)],
            "content",
            None,
            10,
        );
        let sql = b.build_query();
        assert!(
            sql.contains("MATCH(`content`) AGAINST (? IN NATURAL LANGUAGE MODE)"),
            "should use MATCH/AGAINST with positional param; sql={sql}"
        );
    }

    #[test]
    fn test_build_query_emits_score_column() {
        let b = make_builder(vec![("id", DataType::Int64)], "body", None, 10);
        let sql = b.build_query();
        assert!(
            sql.contains("AS `_score`"),
            "score column should be aliased; sql={sql}"
        );
    }

    #[test]
    fn test_build_query_orders_by_score_desc() {
        let b = make_builder(vec![("id", DataType::Int64)], "body", None, 10);
        let sql = b.build_query();
        assert!(
            sql.contains("ORDER BY `_score` DESC"),
            "should order by _score desc; sql={sql}"
        );
    }

    #[test]
    fn test_build_query_with_filter() {
        let b = make_builder(
            vec![("id", DataType::Int64)],
            "body",
            Some("category = 'news'"),
            10,
        );
        let sql = b.build_query();
        assert!(
            sql.contains("category = 'news'"),
            "filter should appear in WHERE clause; sql={sql}"
        );
        assert!(
            sql.contains("MATCH") && sql.contains("AND"),
            "filter should be ANDed with MATCH; sql={sql}"
        );
    }

    #[test]
    fn test_build_query_limit_respected() {
        for limit in [1usize, 5, 100] {
            let b = make_builder(vec![("id", DataType::Int64)], "body", None, limit);
            let sql = b.build_query();
            assert!(sql.contains(&format!("LIMIT {limit}")));
        }
    }

    #[test]
    fn test_build_query_quotes_text_column() {
        let b = make_builder(vec![("id", DataType::Int64)], "full_text", None, 10);
        let sql = b.build_query();
        assert!(
            sql.contains("`full_text`"),
            "text column must be backtick-quoted; sql={sql}"
        );
    }

    #[test]
    fn test_select_columns_excludes_score() {
        let b = make_builder(
            vec![("id", DataType::Int64), ("title", DataType::Utf8)],
            "body",
            None,
            10,
        );
        let cols = b.select_columns();
        assert!(cols.contains("`id`"));
        assert!(cols.contains("`title`"));
        assert!(!cols.contains("_score"));
    }

    #[test]
    fn test_scan_limit_takes_minimum() {
        let mut b = make_builder(vec![("id", DataType::Int64)], "body", None, 100);
        b.scan_limit = Some(5);
        let sql = b.build_query();
        assert!(
            sql.contains("LIMIT 5"),
            "scan_limit < limit should win; sql={sql}"
        );
    }

    #[test]
    fn test_scan_limit_does_not_exceed_function_limit() {
        let mut b = make_builder(vec![("id", DataType::Int64)], "body", None, 10);
        b.scan_limit = Some(50);
        let sql = b.build_query();
        assert!(
            sql.contains("LIMIT 10"),
            "function limit < scan_limit should win; sql={sql}"
        );
    }

    #[test]
    fn test_build_query_no_data_columns_still_emits_score() {
        let b = make_builder(vec![], "body", None, 10);
        let sql = b.build_query();
        assert!(sql.contains("AS `_score`"));
        assert!(sql.starts_with("SELECT "));
    }
}
