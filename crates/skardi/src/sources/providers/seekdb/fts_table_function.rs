//! Table function for SeekDB native full-text search.
//!
//! Usage:
//! ```sql
//! -- Basic full-text search (NATURAL LANGUAGE mode with the FULLTEXT/IK index).
//! SELECT * FROM seekdb_fts('articles', 'body', 'machine learning', 10)
//!
//! -- With WHERE clause filter pushdown.
//! SELECT * FROM seekdb_fts('articles', 'body', 'search terms', 10)
//! WHERE category = 'news'
//!
//! -- Catalog-mode data sources: the first arg is the three-part key
//! -- `<catalog>.<schema>.<table>` exactly as it was registered.
//! SELECT * FROM seekdb_fts('seekdb_cat.mydb.articles', 'body', 'ml', 10)
//! ```
//!
//! The first argument must be a registered SeekDB table whose `text_col`
//! column has a `FULLTEXT` index (optionally declared with `WITH PARSER IK`
//! for Chinese tokenisation). In catalog mode the key is
//! `<catalog>.<schema>.<table>` (unquoted, case-sensitive); in table mode it
//! is the bare name the data source was registered under.
//!
//! Returns all table columns plus `_score Float64` (MySQL's MATCH-AGAINST
//! natural language score). Higher is more relevant.

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
use super::fts_exec::SeekDbFtsExec;
use crate::sources::providers::udtf_args::string_arg;
use crate::sources::providers::{DatasetEntry, DatasetRegistry};

/// Maximum allowed FTS result limit.
pub const MAX_FTS_LIMIT: usize = 500;

// ─── TableFunctionImpl ───────────────────────────────────────────────────────

/// Table function that performs SeekDB full-text search.
#[derive(Debug)]
pub struct SeekDbFtsTableFunction {
    registry: DatasetRegistry,
}

impl SeekDbFtsTableFunction {
    pub fn new(registry: DatasetRegistry) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for SeekDbFtsTableFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if exprs.len() != 4 {
            return plan_err!(
                "seekdb_fts(table, text_col, query, limit) expects 4 arguments, got {}",
                exprs.len()
            );
        }

        let table_name = string_arg(&exprs[0], "seekdb_fts", "table")?;
        let text_col = string_arg(&exprs[1], "seekdb_fts", "text_col")?;
        let query = string_arg(&exprs[2], "seekdb_fts", "query")?;
        let limit = match extract_int(&exprs[3], "limit")? {
            None => 1,
            Some(v) if v > MAX_FTS_LIMIT => {
                return plan_err!(
                    "seekdb_fts: limit must be between 1 and {}, got {}",
                    MAX_FTS_LIMIT,
                    v
                );
            }
            Some(v) => v,
        };

        let entry = {
            let reg = self.registry.read().map_err(|e| {
                DataFusionError::Internal(format!("seekdb_fts registry lock error: {}", e))
            })?;
            let raw = reg.get(&table_name).cloned().ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "seekdb_fts: table '{}' not found in registry. \
                     Make sure the data source is declared with type 'seekdb'.",
                    table_name
                ))
            })?;
            match raw {
                DatasetEntry::Seekdb(e) => e,
                _ => {
                    return plan_err!("seekdb_fts: table '{}' is not a SeekDB dataset", table_name);
                }
            }
        };

        let mut fields: Vec<Field> = entry
            .columns
            .iter()
            .map(|(name, dtype)| Field::new(name.clone(), dtype.clone(), true))
            .collect();
        fields.push(Field::new("_score", DataType::Float64, true));
        let schema: SchemaRef = Arc::new(Schema::new(fields));

        Ok(Arc::new(SeekDbFtsProvider {
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

struct SeekDbFtsProvider {
    pool: Arc<MySQLConnectionPool>,
    qualified_table: String,
    text_col: String,
    query: String,
    limit: usize,
    schema: SchemaRef,
}

impl std::fmt::Debug for SeekDbFtsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeekDbFtsProvider")
            .field("qualified_table", &self.qualified_table)
            .field("text_col", &self.text_col)
            .field("query", &self.query)
            .field("limit", &self.limit)
            .finish()
    }
}

#[async_trait]
impl TableProvider for SeekDbFtsProvider {
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
        _state: &dyn Session,
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

        let mut exec = SeekDbFtsExec::new(
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

fn extract_int(expr: &Expr, name: &str) -> DFResult<Option<usize>> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(v @ 1..)), _) => Ok(Some(*v as usize)),
        Expr::Literal(ScalarValue::Int64(Some(v)), _) => plan_err!(
            "seekdb_fts: '{}' must be a positive integer, got {}",
            name,
            v
        ),
        Expr::Literal(ScalarValue::Int32(Some(v @ 1..)), _) => Ok(Some(*v as usize)),
        Expr::Literal(ScalarValue::Int32(Some(v)), _) => plan_err!(
            "seekdb_fts: '{}' must be a positive integer, got {}",
            name,
            v
        ),
        Expr::Literal(ScalarValue::UInt64(Some(0)), _)
        | Expr::Literal(ScalarValue::UInt32(Some(0)), _) => {
            plan_err!("seekdb_fts: '{}' must be a positive integer, got 0", name)
        }
        Expr::Literal(ScalarValue::UInt64(Some(v)), _) => Ok(Some(*v as usize)),
        Expr::Literal(ScalarValue::UInt32(Some(v)), _) => Ok(Some(*v as usize)),
        Expr::Literal(ScalarValue::Null, _) => Ok(None),
        _ => plan_err!("seekdb_fts: '{}' must be an integer literal", name),
    }
}

// ─── Registration ────────────────────────────────────────────────────────────

/// Register the `seekdb_fts` table function with the DataFusion session.
pub fn register_seekdb_fts_udtf(ctx: &SessionContext, registry: DatasetRegistry) {
    ctx.register_udtf(
        "seekdb_fts",
        Arc::new(SeekDbFtsTableFunction::new(registry)),
    );
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::RwLock;

    fn make_fts_function() -> SeekDbFtsTableFunction {
        let registry: DatasetRegistry = Arc::new(RwLock::new(HashMap::new()));
        SeekDbFtsTableFunction::new(registry)
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
            lit_str("query"),
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
            lit_str("t"),
            lit_str("col"),
            lit_str("q"),
            lit_int(MAX_FTS_LIMIT as i64 + 1),
        ]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("limit must be between 1 and 500"),
            "expected limit error, got: {err}"
        );
    }

    #[test]
    fn test_uint_zero_limit_rejected() {
        let func = make_fts_function();
        let result = func.call(&[
            lit_str("t"),
            lit_str("col"),
            lit_str("q"),
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
        let result = func.call(&[lit_str("t"), lit_str("col"), lit_str("q")]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("expects 4 arguments"),
            "expected arg count error, got: {err}"
        );
    }

    #[test]
    fn test_negative_limit_rejected() {
        let func = make_fts_function();
        let result = func.call(&[lit_str("t"), lit_str("col"), lit_str("q"), lit_int(-1)]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("positive integer"),
            "expected positive-integer error, got: {err}"
        );
    }

    #[test]
    fn test_zero_limit_rejected() {
        let func = make_fts_function();
        let result = func.call(&[lit_str("t"), lit_str("col"), lit_str("q"), lit_int(0)]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("positive integer"),
            "expected positive-integer error, got: {err}"
        );
    }

    // ─── Integration tests ──────────────────────────────────────────────────
    //
    // Require a live SeekDB instance on localhost:2881 with the README
    // quick-start schema seeded (docs/seekdb/README.md):
    //
    //     docker run -d --name seekdb -p 2881:2881 -p 2886:2886 oceanbase/seekdb:latest
    //     # then apply the quick-start CREATE TABLE + INSERT block
    //
    // Credentials are read from SEEKDB_USER / SEEKDB_PASSWORD via the
    // `user_env` / `pass_env` options (same as the CRUD tests in mod.rs). They
    // default to `root@sys` / empty on stock SeekDB.
    //
    // Run with: `cargo test -p skardi -- --ignored seekdb`

    use arrow::array::{Array, Float64Array, RecordBatch};

    use super::super::register_seekdb_tables;
    use crate::sources::hierarchy::HierarchyLevel;

    async fn register_ci_fts(ctx: &mut SessionContext, table: &str) -> DatasetRegistry {
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

        register_seekdb_fts_udtf(ctx, Arc::clone(&registry));
        registry
    }

    async fn query_all(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        let df = ctx.sql(sql).await.expect("parse sql");
        df.collect().await.expect("collect results")
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    fn first_score(batches: &[RecordBatch]) -> f64 {
        batches[0]
            .column_by_name("_score")
            .expect("_score column")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("_score is Float64")
            .value(0)
    }

    #[tokio::test]
    #[ignore]
    async fn test_fts_basic_search() {
        let mut ctx = SessionContext::new();
        let _reg = register_ci_fts(&mut ctx, "articles").await;

        let batches = query_all(
            &ctx,
            "SELECT title, category, _score \
             FROM seekdb_fts('articles', 'body', 'machine learning', 10) \
             ORDER BY _score DESC",
        )
        .await;

        let rows = total_rows(&batches);
        assert!(
            rows >= 2,
            "expected at least 2 rows matching 'machine learning', got {rows}"
        );
        // MySQL's MATCH(...) AGAINST(...) score is strictly positive for a hit.
        assert!(
            first_score(&batches) > 0.0,
            "top _score should be positive, got {}",
            first_score(&batches)
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_fts_with_where_filter() {
        let mut ctx = SessionContext::new();
        let _reg = register_ci_fts(&mut ctx, "articles").await;

        // WHERE pushdown: only the three 'ai' articles are eligible; of those
        // at least two mention 'machine' or 'neural' in the body.
        let batches = query_all(
            &ctx,
            "SELECT title, category \
             FROM seekdb_fts('articles', 'body', 'neural network', 10) \
             WHERE category = 'ai'",
        )
        .await;

        assert!(
            total_rows(&batches) >= 1,
            "expected at least one ai-category match"
        );
        // Every surviving row must carry the filtered category.
        for batch in &batches {
            let cats = batch
                .column_by_name("category")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("category is Utf8");
            for i in 0..cats.len() {
                assert_eq!(cats.value(i), "ai", "row {i} escaped the WHERE filter");
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_fts_no_results() {
        let mut ctx = SessionContext::new();
        let _reg = register_ci_fts(&mut ctx, "articles").await;

        let batches = query_all(
            &ctx,
            "SELECT title FROM seekdb_fts('articles', 'body', \
             'xyzzyquuxplughfrobnicate', 10)",
        )
        .await;

        assert_eq!(
            total_rows(&batches),
            0,
            "nonsense query should return zero rows"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_fts_respects_limit() {
        let mut ctx = SessionContext::new();
        let _reg = register_ci_fts(&mut ctx, "articles").await;

        // The seed has 5 articles; cap at 2 and ensure the table function
        // honours the k-arg bound (not just a post-hoc LIMIT).
        let batches = query_all(
            &ctx,
            "SELECT id FROM seekdb_fts('articles', 'body', 'learning network', 2)",
        )
        .await;

        assert!(
            total_rows(&batches) <= 2,
            "limit=2 must cap the result, got {}",
            total_rows(&batches)
        );
    }
}
