//! Table function for SQLite FTS5 full-text search.
//!
//! Usage:
//! ```sql
//! -- Basic full-text search
//! SELECT * FROM sqlite_fts('articles_fts', 'body', 'search query', 10)
//!
//! -- With WHERE clause filter pushdown
//! SELECT * FROM sqlite_fts('articles_fts', 'body', 'search terms', 10)
//! WHERE category = 'news'
//! ```
//!
//! The first argument must be a registered SQLite FTS5 virtual table.
//! Uses FTS5's `MATCH` operator for full-text search and `bm25()` for relevance scoring.
//!
//! Returns all table columns plus `_score Float64` (negated bm25 score).
//! Higher score means more relevant.

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{Result as DFResult, ScalarValue, plan_err};
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use std::any::Any;
use std::sync::Arc;
use tokio_rusqlite::Connection;

use super::expr_to_sqlite_sql;
use super::fts_exec::SqliteFtsExec;
use crate::sources::providers::udtf_args::string_arg;
use crate::sources::providers::{DatasetEntry, DatasetRegistry};

/// Maximum allowed FTS result limit.
const MAX_FTS_LIMIT: usize = 500;

// ─── TableFunctionImpl ───────────────────────────────────────────────────────

/// Table function that performs SQLite FTS5 full-text search.
#[derive(Debug)]
pub struct SqliteFtsTableFunction {
    registry: DatasetRegistry,
}

impl SqliteFtsTableFunction {
    pub fn new(registry: DatasetRegistry) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for SqliteFtsTableFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if exprs.len() != 4 {
            return plan_err!(
                "sqlite_fts(table, text_col, query, limit) expects 4 arguments, got {}",
                exprs.len()
            );
        }

        let table_name = string_arg(&exprs[0], "sqlite_fts", "table")?;
        let text_col = string_arg(&exprs[1], "sqlite_fts", "text_col")?;
        let query = string_arg(&exprs[2], "sqlite_fts", "query")?;
        // `extract_int` returns `None` for NULL literals (placeholder mode used
        // by pipeline validation) and `Some(v)` for a real value, which is
        // already guaranteed positive. In placeholder mode, substitute a stub
        // limit of 1 so downstream planning succeeds.
        let limit = match extract_int(&exprs[3], "limit")? {
            None => 1,
            Some(v) if v > MAX_FTS_LIMIT => {
                return plan_err!(
                    "sqlite_fts: limit must be between 1 and {}, got {}",
                    MAX_FTS_LIMIT,
                    v
                );
            }
            Some(v) => v,
        };

        // Look up connection + columns from registry.
        let entry = {
            let reg = self.registry.read().map_err(|e| {
                DataFusionError::Internal(format!("sqlite_fts registry lock error: {}", e))
            })?;
            let raw = reg.get(&table_name).cloned().ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "sqlite_fts: table '{}' not found in registry. \
                     Make sure the data source is declared with type 'sqlite'.",
                    table_name
                ))
            })?;
            match raw {
                DatasetEntry::Sqlite(e) => e,
                _ => {
                    return plan_err!("sqlite_fts: table '{}' is not a SQLite dataset", table_name);
                }
            }
        };

        // Build output schema: all columns + _score.
        let mut fields: Vec<Field> = entry
            .columns
            .iter()
            .map(|(name, dtype)| Field::new(name.clone(), dtype.clone(), true))
            .collect();
        fields.push(Field::new("_score", DataType::Float64, true));
        let schema: SchemaRef = Arc::new(Schema::new(fields));

        Ok(Arc::new(SqliteFtsProvider {
            conn: entry.conn,
            table_name: entry.table_name,
            text_col,
            query,
            limit,
            schema,
        }))
    }
}

// ─── TableProvider ───────────────────────────────────────────────────────────

struct SqliteFtsProvider {
    conn: Arc<Connection>,
    table_name: String,
    text_col: String,
    query: String,
    limit: usize,
    schema: SchemaRef,
}

impl std::fmt::Debug for SqliteFtsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteFtsProvider")
            .field("table_name", &self.table_name)
            .field("text_col", &self.text_col)
            .field("query", &self.query)
            .field("limit", &self.limit)
            .finish()
    }
}

#[async_trait]
impl TableProvider for SqliteFtsProvider {
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
                if expr_to_sqlite_sql(expr).is_some() {
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
        // Collect pushable WHERE-clause filters.
        let mut parts: Vec<String> = Vec::new();
        for expr in filters {
            if let Some(sql) = expr_to_sqlite_sql(expr) {
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

        let mut exec = SqliteFtsExec::new(
            Arc::clone(&self.conn),
            self.table_name.clone(),
            self.text_col.clone(),
            self.query.clone(),
            self.limit,
            filter,
            schema,
        );

        if let Some(n) = limit {
            exec = exec.with_scan_limit(n);
        }

        Ok(Arc::new(exec))
    }
}

// ─── Argument extraction helpers ─────────────────────────────────────────────

/// Extract an integer literal argument.
///
/// Returns `Ok(None)` for NULL (placeholder mode used during pipeline validation).
/// Returns `Ok(Some(v))` for a strictly positive integer literal. Any zero or
/// negative literal is rejected with a planning error.
fn extract_int(expr: &Expr, name: &str) -> DFResult<Option<usize>> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(v @ 1..)), _) => Ok(Some(*v as usize)),
        Expr::Literal(ScalarValue::Int64(Some(v)), _) => {
            plan_err!(
                "sqlite_fts: '{}' must be a positive integer, got {}",
                name,
                v
            )
        }
        Expr::Literal(ScalarValue::Int32(Some(v @ 1..)), _) => Ok(Some(*v as usize)),
        Expr::Literal(ScalarValue::Int32(Some(v)), _) => {
            plan_err!(
                "sqlite_fts: '{}' must be a positive integer, got {}",
                name,
                v
            )
        }
        Expr::Literal(ScalarValue::UInt64(Some(0)), _)
        | Expr::Literal(ScalarValue::UInt32(Some(0)), _) => {
            plan_err!("sqlite_fts: '{}' must be a positive integer, got 0", name)
        }
        Expr::Literal(ScalarValue::UInt64(Some(v)), _) => Ok(Some(*v as usize)),
        Expr::Literal(ScalarValue::UInt32(Some(v)), _) => Ok(Some(*v as usize)),
        Expr::Literal(ScalarValue::Null, _) => Ok(None),
        _ => plan_err!("sqlite_fts: '{}' must be an integer literal", name),
    }
}

// ─── Registration ────────────────────────────────────────────────────────────

/// Register the `sqlite_fts` table function with the DataFusion session.
pub fn register_sqlite_fts_udtf(ctx: &SessionContext, registry: DatasetRegistry) {
    ctx.register_udtf(
        "sqlite_fts",
        Arc::new(SqliteFtsTableFunction::new(registry)),
    );
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::RwLock;

    fn make_fts_function() -> SqliteFtsTableFunction {
        let registry: DatasetRegistry = Arc::new(RwLock::new(HashMap::new()));
        SqliteFtsTableFunction::new(registry)
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

    #[test]
    fn test_null_query_and_null_limit_accepted() {
        let func = make_fts_function();
        let result = func.call(&[
            lit_str("some_table"),
            lit_str("col"),
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
    fn test_null_query_with_literal_limit_accepted() {
        let func = make_fts_function();
        let result = func.call(&[
            lit_str("some_table"),
            lit_str("col"),
            lit_null(),
            lit_int(60),
        ]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not found in registry"),
            "expected registry error, got: {err}"
        );
    }

    #[test]
    fn test_literal_query_with_null_limit_accepted() {
        let func = make_fts_function();
        let result = func.call(&[
            lit_str("some_table"),
            lit_str("col"),
            lit_str("test query"),
            lit_null(),
        ]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not found in registry"),
            "expected registry error, got: {err}"
        );
    }

    #[test]
    fn test_limit_over_max_rejected() {
        let func = make_fts_function();
        let result = func.call(&[
            lit_str("some_table"),
            lit_str("col"),
            lit_str("test"),
            lit_int(501),
        ]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("limit must be between 1 and 500"),
            "expected limit error, got: {err}"
        );
    }

    #[test]
    fn test_uint_zero_limit_rejected() {
        // Unsigned 0 must not slip past the positive-integer guard.
        let func = make_fts_function();
        let result = func.call(&[
            lit_str("some_table"),
            lit_str("col"),
            lit_str("test"),
            Expr::Literal(ScalarValue::UInt64(Some(0)), None),
        ]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("positive integer"),
            "expected positive-integer error, got: {err}"
        );
    }

    #[test]
    fn test_wrong_arg_count_rejected() {
        let func = make_fts_function();
        let result = func.call(&[lit_str("table"), lit_str("col"), lit_str("query")]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("expects 4 arguments"),
            "expected arg count error, got: {err}"
        );
    }

    #[test]
    fn test_negative_limit_rejected() {
        let func = make_fts_function();
        let result = func.call(&[
            lit_str("some_table"),
            lit_str("col"),
            lit_str("test"),
            lit_int(-1),
        ]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("positive integer"),
            "expected positive-integer error, got: {err}"
        );
    }

    #[test]
    fn test_zero_limit_rejected() {
        let func = make_fts_function();
        let result = func.call(&[
            lit_str("some_table"),
            lit_str("col"),
            lit_str("test"),
            lit_int(0),
        ]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("positive integer"),
            "expected positive-integer error, got: {err}"
        );
    }

    // ─── sqlite_fts integration tests ────────────────────────────────────
    // Require a SQLite database with an FTS5 table seeded.

    use arrow::array::{Array, Float64Array, RecordBatch, StringArray};

    use super::super::register_sqlite_tables;
    use crate::sources::hierarchy::HierarchyLevel;

    async fn create_fts_test_db() -> tempfile::TempPath {
        let tmp = tempfile::NamedTempFile::new().expect("create temp file");
        let path = tmp.into_temp_path();
        let db_path = path.to_str().unwrap().to_string();

        let conn = Connection::open(&db_path).await.expect("open temp sqlite");
        conn.call(|conn| -> Result<(), tokio_rusqlite::rusqlite::Error> {
            conn.execute_batch(
                "CREATE VIRTUAL TABLE articles_fts USING fts5(title, body, category);
                 INSERT INTO articles_fts (title, body, category) VALUES
                   ('Machine Learning Basics', 'Introduction to machine learning algorithms and neural networks', 'ai'),
                   ('Database Systems', 'Overview of relational database management systems and SQL', 'database'),
                   ('Deep Learning', 'Advanced neural network architectures for machine learning', 'ai'),
                   ('Web Development', 'Modern web frameworks and frontend technologies', 'web'),
                   ('Natural Language Processing', 'NLP techniques for text analysis and machine learning applications', 'ai'),
                   ('Gateway Modes', 'The gateway runs in read-only mode and it doesn''t write anything back', 'ops'),
                   ('Retry Policy', 'note: the retry policy is described in the ops runbook', 'ops');",
            )?;
            Ok(())
        })
        .await
        .expect("seed fts table");
        conn.close().await.expect("close seed connection");

        path
    }

    async fn register_ci_fts(ctx: &mut SessionContext) -> (DatasetRegistry, tempfile::TempPath) {
        let registry: DatasetRegistry = Arc::new(RwLock::new(HashMap::new()));
        let db_path = create_fts_test_db().await;
        let db = db_path.to_str().unwrap();

        let mut options = HashMap::new();
        options.insert("table".to_string(), "articles_fts".to_string());

        register_sqlite_tables(
            ctx,
            "articles_fts",
            db,
            Some(&options),
            false,
            Some(&registry),
            HierarchyLevel::Table,
        )
        .await
        .expect("register fts table failed");

        register_sqlite_fts_udtf(ctx, Arc::clone(&registry));
        (registry, db_path)
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
    async fn test_fts_basic_search() {
        let mut ctx = SessionContext::new();
        let (_reg, _db) = register_ci_fts(&mut ctx).await;

        let batches = query_all(
            &ctx,
            "SELECT title, _score FROM sqlite_fts('articles_fts', 'body', 'machine learning', 10)",
        )
        .await;

        let rows = total_rows(&batches);
        assert!(
            rows >= 2,
            "expected at least 2 results for 'machine learning', got {rows}"
        );

        let scores = batches[0]
            .column_by_name("_score")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            scores.value(0) > 0.0,
            "score should be positive (negated bm25)"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_fts_respects_limit() {
        let mut ctx = SessionContext::new();
        let (_reg, _db) = register_ci_fts(&mut ctx).await;

        let batches = query_all(
            &ctx,
            "SELECT title FROM sqlite_fts('articles_fts', 'body', 'learning', 1)",
        )
        .await;
        assert_eq!(total_rows(&batches), 1);
    }

    #[tokio::test]
    #[ignore]
    async fn test_fts_no_results() {
        let mut ctx = SessionContext::new();
        let (_reg, _db) = register_ci_fts(&mut ctx).await;

        let batches = query_all(
            &ctx,
            "SELECT title FROM sqlite_fts('articles_fts', 'body', 'xyznonexistent', 10)",
        )
        .await;
        assert_eq!(total_rows(&batches), 0);
    }

    #[tokio::test]
    #[ignore]
    async fn test_fts_with_where_filter() {
        let mut ctx = SessionContext::new();
        let (_reg, _db) = register_ci_fts(&mut ctx).await;

        let all_batches = query_all(
            &ctx,
            "SELECT title, category FROM sqlite_fts('articles_fts', 'body', 'machine learning', 10)",
        )
        .await;
        let all_rows = total_rows(&all_batches);

        let filtered_batches = query_all(
            &ctx,
            "SELECT title, category FROM sqlite_fts('articles_fts', 'body', 'machine learning', 10) WHERE category = 'ai'",
        )
        .await;
        let filtered_rows = total_rows(&filtered_batches);

        assert!(
            filtered_rows <= all_rows,
            "filtered rows ({filtered_rows}) should be <= unfiltered ({all_rows})"
        );

        for batch in &filtered_batches {
            let categories = batch
                .column_by_name("category")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..categories.len() {
                assert_eq!(categories.value(i), "ai");
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_fts_score_ordering() {
        let mut ctx = SessionContext::new();
        let (_reg, _db) = register_ci_fts(&mut ctx).await;

        let batches = query_all(
            &ctx,
            "SELECT title, _score FROM sqlite_fts('articles_fts', 'body', 'machine learning', 10) ORDER BY _score DESC",
        )
        .await;

        let rows = total_rows(&batches);
        if rows >= 2 {
            let scores = batches[0]
                .column_by_name("_score")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert!(
                scores.value(0) >= scores.value(1),
                "scores should be descending: {} >= {}",
                scores.value(0),
                scores.value(1)
            );
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_fts_phrase_search() {
        let mut ctx = SessionContext::new();
        let (_reg, _db) = register_ci_fts(&mut ctx).await;

        let batches = query_all(
            &ctx,
            r#"SELECT title FROM sqlite_fts('articles_fts', 'body', '"neural network"', 10)"#,
        )
        .await;

        let rows = total_rows(&batches);
        assert!(
            rows >= 1,
            "expected at least 1 result for phrase 'neural network', got {rows}"
        );
    }

    // ─── Read-your-own-write ─────────────────────────────────────────

    /// Register FTS table as read_write so INSERT and sqlite_fts share the
    /// same connection (write conn), ensuring immediate visibility.
    async fn register_ci_fts_rw(ctx: &mut SessionContext) -> (DatasetRegistry, tempfile::TempPath) {
        let registry: DatasetRegistry = Arc::new(RwLock::new(HashMap::new()));
        let db_path = create_fts_test_db().await;
        let db = db_path.to_str().unwrap();

        let mut options = HashMap::new();
        options.insert("table".to_string(), "articles_fts".to_string());

        register_sqlite_tables(
            ctx,
            "articles_fts",
            db,
            Some(&options),
            true,
            Some(&registry),
            HierarchyLevel::Table,
        )
        .await
        .expect("register fts table (rw) failed");

        register_sqlite_fts_udtf(ctx, Arc::clone(&registry));
        (registry, db_path)
    }

    #[tokio::test]
    #[ignore]
    async fn test_fts_read_own_write_insert_then_search() {
        let mut ctx = SessionContext::new();
        let (_reg, _db) = register_ci_fts_rw(&mut ctx).await;

        // Verify 'reinforcement' not found initially
        let batches = query_all(
            &ctx,
            "SELECT title FROM sqlite_fts('articles_fts', 'body', 'reinforcement', 10)",
        )
        .await;
        assert_eq!(total_rows(&batches), 0);

        // INSERT a new article
        ctx.sql(
            "INSERT INTO articles_fts (title, body, category) \
             VALUES ('Reinforcement Learning', 'Training agents through reinforcement and reward signals', 'ai')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

        // Search must find the just-inserted row
        let batches = query_all(
            &ctx,
            "SELECT title, _score FROM sqlite_fts('articles_fts', 'body', 'reinforcement', 10)",
        )
        .await;
        assert_eq!(
            total_rows(&batches),
            1,
            "inserted article must be immediately searchable via FTS"
        );

        let titles = batches[0]
            .column_by_name("title")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(titles.value(0), "Reinforcement Learning");
    }

    #[tokio::test]
    #[ignore]
    async fn test_fts_read_own_write_delete_then_search() {
        let mut ctx = SessionContext::new();
        let (_reg, _db) = register_ci_fts_rw(&mut ctx).await;

        // Verify 'web frameworks' is found initially
        let batches = query_all(
            &ctx,
            "SELECT title FROM sqlite_fts('articles_fts', 'body', 'web frameworks', 10)",
        )
        .await;
        assert_eq!(total_rows(&batches), 1);

        // DELETE the article
        ctx.sql("DELETE FROM articles_fts WHERE title = 'Web Development'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Search must NOT find the deleted row
        let batches = query_all(
            &ctx,
            "SELECT title FROM sqlite_fts('articles_fts', 'body', 'web frameworks', 10)",
        )
        .await;
        assert_eq!(
            total_rows(&batches),
            0,
            "deleted article must not appear in FTS results"
        );
    }

    // ─── The parameter is user text, not an FTS5 expression ──────────────
    // Every query in this block returned an execution error before
    // `websearch_to_fts5` sat in front of FTS5 (SkardiLabs/skardi-skills#39):
    // FTS5 read the apostrophe as a string delimiter, the colon as a column
    // filter, and the hyphen as a term boundary. These are the questions a
    // person types into a search box, so they have to answer.

    /// A hyphenated technical term is the vocabulary a technical corpus is
    /// made of. It has to find the row, not merely avoid erroring.
    #[tokio::test]
    #[ignore]
    async fn test_fts_hyphenated_term_finds_the_row() {
        let mut ctx = SessionContext::new();
        let (_reg, _db) = register_ci_fts(&mut ctx).await;

        let batches = query_all(
            &ctx,
            "SELECT title FROM sqlite_fts('articles_fts', 'body', 'read-only mode', 10)",
        )
        .await;

        let titles = batches[0]
            .column_by_name("title")
            .expect("title column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("title is Utf8");
        assert_eq!(total_rows(&batches), 1, "expected the read-only row");
        assert_eq!(titles.value(0), "Gateway Modes");
    }

    /// The apostrophe matters most: a natural English question hits it before
    /// a hyphenated term does.
    #[tokio::test]
    #[ignore]
    async fn test_fts_apostrophe_finds_the_row() {
        let mut ctx = SessionContext::new();
        let (_reg, _db) = register_ci_fts(&mut ctx).await;

        // Doubled for the SQL string literal; FTS5 sees `doesn't write`.
        let batches = query_all(
            &ctx,
            "SELECT title FROM sqlite_fts('articles_fts', 'body', 'doesn''t write', 10)",
        )
        .await;
        assert_eq!(total_rows(&batches), 1, "expected the read-only row");
    }

    /// A colon used to be read as a column filter and reported as
    /// `no such column: note`.
    #[tokio::test]
    #[ignore]
    async fn test_fts_colon_is_literal_text_not_a_column_filter() {
        let mut ctx = SessionContext::new();
        let (_reg, _db) = register_ci_fts(&mut ctx).await;

        let batches = query_all(
            &ctx,
            "SELECT title FROM sqlite_fts('articles_fts', 'body', 'note: retry policy', 10)",
        )
        .await;
        assert_eq!(total_rows(&batches), 1, "expected the retry-policy row");
    }

    /// A blank question is answerable without asking FTS5, which rejects an
    /// empty match string outright. Zero rows is the honest answer; the
    /// alternative that used to be possible — rows for a question nobody
    /// asked — is worse than an error.
    #[tokio::test]
    #[ignore]
    async fn test_fts_query_with_nothing_to_search_for_returns_no_rows() {
        let mut ctx = SessionContext::new();
        let (_reg, _db) = register_ci_fts(&mut ctx).await;

        for query in ["", "   ", "---"] {
            let batches = query_all(
                &ctx,
                &format!("SELECT title FROM sqlite_fts('articles_fts', 'body', '{query}', 10)"),
            )
            .await;
            assert_eq!(total_rows(&batches), 0, "query {query:?}");
        }
    }

    /// `websearch_to_tsquery`'s two operators, honoured on this backend too.
    #[tokio::test]
    #[ignore]
    async fn test_fts_or_and_exclusion_operators() {
        let mut ctx = SessionContext::new();
        let (_reg, _db) = register_ci_fts(&mut ctx).await;

        let both = query_all(
            &ctx,
            "SELECT title FROM sqlite_fts('articles_fts', 'body', 'gateway or runbook', 10)",
        )
        .await;
        assert_eq!(total_rows(&both), 2, "or should reach both ops articles");

        let excluded = query_all(
            &ctx,
            "SELECT title FROM sqlite_fts('articles_fts', 'body', 'gateway or runbook -policy', 10)",
        )
        .await;
        assert_eq!(
            total_rows(&excluded),
            1,
            "-policy should drop the runbook row"
        );
    }
}
