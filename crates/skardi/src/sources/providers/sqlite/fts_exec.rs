//! Physical execution plan for SQLite FTS5 full-text search.

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions, new_empty_array};
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

use super::fts_query::websearch_to_fts5;
use super::{quote_sqlite_ident, sqlite_values_to_arrow};

/// Physical execution plan that runs a SQLite FTS5 full-text search query using
/// `MATCH` and returns matching rows with a `_score` column derived from `bm25()`.
///
/// Generates SQL of the form:
/// ```sql
/// SELECT <cols>,
///        -bm25("<table>") AS _score
/// FROM "<table>"
/// WHERE "<text_col>" MATCH '<query>'
///   [AND <filter>]
/// ORDER BY rank
/// LIMIT <limit>
/// ```
///
/// Note: FTS5's `rank` is equivalent to `bm25(table)` and is negative (more
/// negative = better match). We negate it so `_score` is positive and higher
/// means more relevant, consistent with `pg_fts`.
///
/// `query` is what the user typed, not an FTS5 expression: it is translated by
/// [`websearch_to_fts5`] on the way in, so the same parameter means the same
/// thing here as it does in `pg_fts` behind `websearch_to_tsquery`. Text with
/// nothing to search for returns no rows without reaching SQLite, because FTS5
/// rejects an empty match string as a syntax error.
#[derive(Debug, Clone)]
pub struct SqliteFtsExec {
    conn: Arc<Connection>,
    /// Table name (the FTS5 virtual table).
    table_name: String,
    /// Name of the text column to search.
    text_col: String,
    /// The user's search text, verbatim — translated to an FTS5 expression at
    /// execution time rather than stored translated, so `EXPLAIN` output and
    /// error messages keep showing what was actually asked.
    query: String,
    /// Maximum number of results to return.
    limit: usize,
    /// Optional SQL WHERE predicate (no "WHERE" keyword).
    filter: Option<String>,
    /// Optional scan limit from an outer SQL LIMIT clause.
    scan_limit: Option<usize>,
    /// Output schema: table columns + `_score Float64`.
    schema: SchemaRef,
    /// Cached DataFusion plan metadata.
    plan_properties: PlanProperties,
}

impl SqliteFtsExec {
    pub fn new(
        conn: Arc<Connection>,
        table_name: String,
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
            conn,
            table_name,
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
            .map(|f| quote_sqlite_ident(f.name()))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Build the FTS5 search SELECT query.
    ///
    /// Uses a parameterised placeholder (`?1`) for the search query string.
    pub(crate) fn build_query(&self) -> String {
        let cols = self.select_columns();
        let table = quote_sqlite_ident(&self.table_name);
        let text_col = quote_sqlite_ident(&self.text_col);
        // bm25() returns negative values; negate so higher = more relevant.
        let score_expr = format!("-bm25({table}) AS _score");
        let match_expr = format!("{text_col} MATCH ?1");

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
             ORDER BY rank \
             LIMIT {limit}",
            limit = effective_limit,
        )
    }

    /// Execute the query and return all rows as a single `RecordBatch`.
    async fn run(&self) -> DFResult<RecordBatch> {
        let schema = self.schema.clone();

        // No searchable term — an empty or punctuation-only question. FTS5
        // treats an empty match string as a syntax error, so answer it here
        // instead of letting a blank question come back as an execution
        // failure (or, worse, as arbitrary rows).
        let Some(match_expr) = websearch_to_fts5(&self.query) else {
            tracing::debug!("sqlite_fts: query has no searchable term; returning no rows");
            return RecordBatch::try_new_with_options(
                schema.clone(),
                schema
                    .fields()
                    .iter()
                    .map(|f| new_empty_array(f.data_type()))
                    .collect(),
                &RecordBatchOptions::new().with_row_count(Some(0)),
            )
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
        };

        let sql = self.build_query();
        tracing::debug!("sqlite_fts SQL: {}", sql);

        let conn = Arc::clone(&self.conn);
        let query = match_expr;
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
                    let mut col_values: Vec<Vec<tokio_rusqlite::rusqlite::types::Value>> =
                        (0..num_cols).map(|_| Vec::new()).collect();

                    let mut rows = stmt.query([&query])?;
                    while let Some(row) = rows.next()? {
                        for col_idx in 0..num_cols {
                            let val: tokio_rusqlite::rusqlite::types::Value = row.get(col_idx)?;
                            col_values[col_idx].push(val);
                        }
                    }

                    Ok(col_values)
                },
            )
            .await
            .map_err(|e| DataFusionError::Execution(format!("sqlite_fts error: {e}")))?;

        let arrays: Vec<ArrayRef> = col_values
            .into_iter()
            .zip(field_types.iter())
            .map(|(values, data_type)| sqlite_values_to_arrow(&values, data_type))
            .collect();

        RecordBatch::try_new(schema, arrays)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl DisplayAs for SqliteFtsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SqliteFtsExec: table={}, text_col={}, limit={}",
            self.table_name, self.text_col, self.limit
        )
    }
}

impl ExecutionPlan for SqliteFtsExec {
    fn name(&self) -> &str {
        "SqliteFtsExec"
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
                "SqliteFtsExec expects 0 children".to_string(),
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

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_exec(
        cols: Vec<(&str, DataType)>,
        text_col: &str,
        query: &str,
        filter: Option<&str>,
        limit: usize,
    ) -> SqliteFtsExec {
        let conn = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { Connection::open_in_memory().await.unwrap() });
        let mut fields: Vec<Field> = cols
            .into_iter()
            .map(|(name, dt)| Field::new(name, dt, true))
            .collect();
        fields.push(Field::new("_score", DataType::Float64, true));
        let schema = Arc::new(Schema::new(fields));
        SqliteFtsExec::new(
            Arc::new(conn),
            "articles_fts".to_string(),
            text_col.to_string(),
            query.to_string(),
            limit,
            filter.map(str::to_string),
            schema,
        )
    }

    #[test]
    fn test_build_query_uses_match() {
        let exec = make_exec(
            vec![("id", DataType::Int64), ("content", DataType::Utf8)],
            "content",
            "test query",
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
    fn test_build_query_uses_bm25_score() {
        let exec = make_exec(
            vec![("id", DataType::Int64)],
            "body",
            "search terms",
            None,
            10,
        );
        let sql = exec.build_query();
        assert!(
            sql.contains("-bm25("),
            "should use negated bm25 for scoring; sql={sql}"
        );
        assert!(
            sql.contains("AS _score"),
            "score column should be aliased; sql={sql}"
        );
    }

    #[test]
    fn test_build_query_orders_by_rank() {
        let exec = make_exec(vec![("id", DataType::Int64)], "body", "test", None, 10);
        let sql = exec.build_query();
        assert!(
            sql.contains("ORDER BY rank"),
            "should order by rank; sql={sql}"
        );
    }

    #[test]
    fn test_build_query_with_filter() {
        let exec = make_exec(
            vec![("id", DataType::Int64)],
            "body",
            "test",
            Some("category = 'news'"),
            10,
        );
        let sql = exec.build_query();
        assert!(
            sql.contains("category = 'news'"),
            "filter should appear in WHERE clause; sql={sql}"
        );
        assert!(
            sql.contains("MATCH") && sql.contains("AND"),
            "filter should be ANDed with FTS match; sql={sql}"
        );
    }

    #[test]
    fn test_build_query_limit() {
        for limit in [1, 5, 100] {
            let exec = make_exec(vec![("id", DataType::Int64)], "body", "q", None, limit);
            let sql = exec.build_query();
            assert!(sql.contains(&format!("LIMIT {limit}")));
        }
    }

    #[test]
    fn test_build_query_quotes_text_column() {
        let exec = make_exec(vec![("id", DataType::Int64)], "full text", "test", None, 10);
        let sql = exec.build_query();
        assert!(
            sql.contains("\"full text\""),
            "text column should be quoted; sql={sql}"
        );
    }

    #[test]
    fn test_select_columns_excludes_score() {
        let exec = make_exec(
            vec![("id", DataType::Int64), ("title", DataType::Utf8)],
            "body",
            "q",
            None,
            10,
        );
        let cols = exec.select_columns();
        assert!(cols.contains("\"id\""));
        assert!(cols.contains("\"title\""));
        assert!(!cols.contains("_score"));
    }

    #[test]
    fn test_scan_limit_takes_minimum() {
        let exec =
            make_exec(vec![("id", DataType::Int64)], "body", "q", None, 100).with_scan_limit(5);
        let sql = exec.build_query();
        assert!(
            sql.contains("LIMIT 5"),
            "scan_limit < limit should win; sql={sql}"
        );
    }

    #[test]
    fn test_scan_limit_does_not_exceed_function_limit() {
        let exec =
            make_exec(vec![("id", DataType::Int64)], "body", "q", None, 10).with_scan_limit(50);
        let sql = exec.build_query();
        assert!(
            sql.contains("LIMIT 10"),
            "function limit < scan_limit should win; sql={sql}"
        );
    }
}
