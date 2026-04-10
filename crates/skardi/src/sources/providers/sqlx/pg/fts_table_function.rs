//! Table function for PostgreSQL full-text search.
//!
//! Usage:
//! ```sql
//! -- Basic full-text search
//! SELECT * FROM pg_fts('table_name', 'text_column', 'search query', 10)
//!
//! -- With web-search-style syntax (quotes, OR, negation)
//! SELECT * FROM pg_fts('table_name', 'text_column', '"exact phrase" or alternative -excluded', 20)
//!
//! -- With WHERE clause filter pushdown
//! SELECT * FROM pg_fts('table_name', 'text_column', 'search terms', 10)
//! WHERE category = 'news'
//! ```
//!
//! Uses `websearch_to_tsquery` for query parsing, which supports:
//! - Plain terms (AND'd by default): `foo bar`
//! - Quoted phrases: `"foo bar"`
//! - OR operator: `foo or bar`
//! - Negation: `-excluded`
//!
//! Returns all table columns plus `_score Float64` (ts_rank score).
//! Higher score means more relevant.

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{Result as DFResult, ScalarValue, plan_err};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use std::any::Any;
use std::sync::Arc;

use super::fts_exec::PgFtsExec;
use super::utils::expr_to_pg_sql;
use crate::sources::providers::{DatasetEntry, DatasetRegistry};

/// Maximum allowed FTS result limit.
const MAX_FTS_LIMIT: usize = 500;

// ─── TableFunctionImpl ───────────────────────────────────────────────────────

/// Table function that performs PostgreSQL full-text search.
#[derive(Debug)]
pub struct PgFtsTableFunction {
    registry: DatasetRegistry,
}

impl PgFtsTableFunction {
    pub fn new(registry: DatasetRegistry) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for PgFtsTableFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if exprs.len() != 4 {
            return plan_err!(
                "pg_fts(table, text_col, query, limit) expects 4 arguments, got {}",
                exprs.len()
            );
        }

        let table_name = extract_string(&exprs[0], "table")?;
        let text_col = extract_string(&exprs[1], "text_col")?;
        let query = extract_string(&exprs[2], "query")?;
        let limit = extract_int(&exprs[3], "limit")?;

        // The inferencer replaces {param} with NULL, yielding empty string for
        // strings and 0 for integers. Accept these as placeholders — validate only
        // when real values are provided.
        if !query.is_empty() && limit > MAX_FTS_LIMIT {
            return plan_err!(
                "pg_fts: limit must be between 1 and {}, got {}",
                MAX_FTS_LIMIT,
                limit
            );
        }
        // Use a safe default when limit is a NULL placeholder (0).
        let limit = if limit == 0 { 1 } else { limit };

        // Look up pool + columns from registry.
        let entry = {
            let reg = self.registry.read().map_err(|e| {
                datafusion::error::DataFusionError::Internal(format!(
                    "pg_fts registry lock error: {}",
                    e
                ))
            })?;
            let raw = reg.get(&table_name).cloned().ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!(
                    "pg_fts: table '{}' not found in registry. \
                     Make sure the data source is declared with type 'postgres'.",
                    table_name
                ))
            })?;
            match raw {
                DatasetEntry::Postgres(e) => e,
                _ => return plan_err!("pg_fts: table '{}' is not a Postgres dataset", table_name),
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

        Ok(Arc::new(PgFtsProvider {
            pool: entry.pool,
            qualified_table: entry.qualified_table,
            text_col,
            query,
            limit,
            schema,
        }))
    }
}

// ─── TableProvider ───────────────────────────────────────────────────────────

struct PgFtsProvider {
    pool: Arc<sqlx::PgPool>,
    qualified_table: String,
    text_col: String,
    query: String,
    limit: usize,
    schema: SchemaRef,
}

impl std::fmt::Debug for PgFtsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgFtsProvider")
            .field("qualified_table", &self.qualified_table)
            .field("text_col", &self.text_col)
            .field("query", &self.query)
            .field("limit", &self.limit)
            .finish()
    }
}

#[async_trait]
impl TableProvider for PgFtsProvider {
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
                if expr_to_pg_sql(expr).is_some() {
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
            if let Some(sql) = expr_to_pg_sql(expr) {
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

        let mut exec = PgFtsExec::new(
            Arc::clone(&self.pool),
            self.qualified_table.clone(),
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

fn extract_string(expr: &Expr, name: &str) -> DFResult<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => Ok(s.clone()),
        // Accept NULL as placeholder during pipeline validation/schema inference.
        Expr::Literal(ScalarValue::Null, _) => Ok(String::new()),
        _ => plan_err!("pg_fts: '{}' must be a string literal", name),
    }
}

fn extract_int(expr: &Expr, name: &str) -> DFResult<usize> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(v)), _) => Ok(*v as usize),
        Expr::Literal(ScalarValue::Int32(Some(v)), _) => Ok(*v as usize),
        Expr::Literal(ScalarValue::UInt64(Some(v)), _) => Ok(*v as usize),
        // Accept NULL as placeholder during pipeline validation/schema inference.
        Expr::Literal(ScalarValue::Null, _) => Ok(0),
        _ => plan_err!("pg_fts: '{}' must be an integer literal", name),
    }
}

// ─── Registration ────────────────────────────────────────────────────────────

/// Register the `pg_fts` table function with the DataFusion session.
pub fn register_pg_fts_udtf(ctx: &SessionContext, registry: DatasetRegistry) {
    ctx.register_udtf("pg_fts", Arc::new(PgFtsTableFunction::new(registry)));
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, RecordBatch, StringArray};
    use std::collections::HashMap;
    use std::sync::RwLock;

    // ─── NULL placeholder tests (unit, no Postgres needed) ──────────────
    // The pipeline schema inferencer replaces {param} with NULL before
    // planning. These tests verify that pg_fts accepts NULL placeholders
    // in any combination without erroring during call().

    fn make_fts_function() -> PgFtsTableFunction {
        let registry: DatasetRegistry = Arc::new(RwLock::new(HashMap::new()));
        PgFtsTableFunction::new(registry)
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
    fn test_wrong_arg_count_rejected() {
        let func = make_fts_function();
        let result = func.call(&[lit_str("table"), lit_str("col"), lit_str("query")]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("expects 4 arguments"),
            "expected arg count error, got: {err}"
        );
    }

    // ─── pg_fts integration tests ───────────────────────────────────────
    // Require CI PostgreSQL with seeded articles table containing a text column.

    use crate::sources::providers::sqlx::pg::postgres::register_postgres_tables;

    async fn register_ci_fts(ctx: &mut SessionContext) -> DatasetRegistry {
        let registry: DatasetRegistry = Arc::new(RwLock::new(HashMap::new()));

        let mut options = HashMap::new();
        options.insert("table".to_string(), "articles".to_string());
        options.insert("schema".to_string(), "public".to_string());
        options.insert("user_env".to_string(), "PG_USER".to_string());
        options.insert("pass_env".to_string(), "PG_PASS".to_string());

        register_postgres_tables(
            ctx,
            "articles",
            "postgresql://127.0.0.1:5432/mydb?sslmode=disable",
            Some(&options),
            false,
            Some(&registry),
        )
        .await
        .expect("register articles table failed");

        register_pg_fts_udtf(ctx, Arc::clone(&registry));
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
    async fn test_fts_basic_search() {
        let mut ctx = SessionContext::new();
        let _reg = register_ci_fts(&mut ctx).await;

        let batches = query_all(
            &ctx,
            "SELECT id, title, _score FROM pg_fts('articles', 'body', 'machine learning', 10)",
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
        assert!(scores.value(0) > 0.0, "ts_rank score should be positive");
    }

    #[tokio::test]
    #[ignore]
    async fn test_fts_respects_limit() {
        let mut ctx = SessionContext::new();
        let _reg = register_ci_fts(&mut ctx).await;

        let batches = query_all(
            &ctx,
            "SELECT id FROM pg_fts('articles', 'body', 'learning', 1)",
        )
        .await;
        assert_eq!(total_rows(&batches), 1);
    }

    #[tokio::test]
    #[ignore]
    async fn test_fts_no_results() {
        let mut ctx = SessionContext::new();
        let _reg = register_ci_fts(&mut ctx).await;

        let batches = query_all(
            &ctx,
            "SELECT id FROM pg_fts('articles', 'body', 'xyznonexistent', 10)",
        )
        .await;
        assert_eq!(total_rows(&batches), 0);
    }

    #[tokio::test]
    #[ignore]
    async fn test_fts_with_where_filter() {
        let mut ctx = SessionContext::new();
        let _reg = register_ci_fts(&mut ctx).await;

        // "machine learning" matches docs in multiple categories;
        // filtering by 'ai' should narrow results.
        let all_batches = query_all(
            &ctx,
            "SELECT id, category FROM pg_fts('articles', 'body', 'machine learning', 10)",
        )
        .await;
        let all_rows = total_rows(&all_batches);

        let filtered_batches = query_all(
            &ctx,
            "SELECT id, category FROM pg_fts('articles', 'body', 'machine learning', 10) WHERE category = 'ai'",
        )
        .await;
        let filtered_rows = total_rows(&filtered_batches);

        assert!(
            filtered_rows < all_rows,
            "filtered rows ({filtered_rows}) should be fewer than unfiltered ({all_rows})"
        );

        // Verify all returned rows have category = 'ai'
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
        let _reg = register_ci_fts(&mut ctx).await;

        let batches = query_all(
            &ctx,
            "SELECT id, _score FROM pg_fts('articles', 'body', 'machine learning', 10) ORDER BY _score DESC",
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
        let _reg = register_ci_fts(&mut ctx).await;

        // websearch_to_tsquery supports quoted phrases
        let batches = query_all(
            &ctx,
            r#"SELECT id FROM pg_fts('articles', 'body', '"neural network"', 10)"#,
        )
        .await;

        let rows = total_rows(&batches);
        assert!(
            rows >= 1,
            "expected at least 1 result for phrase 'neural network', got {rows}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_fts_negation() {
        let mut ctx = SessionContext::new();
        let _reg = register_ci_fts(&mut ctx).await;

        // websearch_to_tsquery supports negation with -
        let batches = query_all(
            &ctx,
            "SELECT id FROM pg_fts('articles', 'body', 'learning -database', 10)",
        )
        .await;

        let rows = total_rows(&batches);
        assert!(
            rows >= 1,
            "expected at least 1 result for 'learning -database', got {rows}"
        );
    }
}
