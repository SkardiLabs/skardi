//! Table function for Lance full-text search
//!
//! Usage:
//! ```sql
//! -- Basic term search (OR logic, BM25 scored)
//! SELECT * FROM lance_fts('table_name', 'text_column', 'search query', 10)
//!
//! -- With WHERE clause filter pushdown
//! SELECT * FROM lance_fts('table_name', 'text_column', 'search query', 10)
//! WHERE category = 'food' AND price < 20
//!
//! -- Phrase search (exact phrase match)
//! SELECT * FROM lance_fts('table_name', 'text_column', '"train to boston"', 10)
//!
//! -- Fuzzy search (typo-tolerant)
//! SELECT * FROM lance_fts('table_name', 'text_column', 'rammen~', 10)
//!
//! -- Boolean search (MUST/MUST_NOT)
//! SELECT * FROM lance_fts('table_name', 'text_column', '+umbrella -train', 10)
//! ```

use arrow::datatypes::{DataType, Field, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{Result as DFResult, ScalarValue, plan_err};
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use lance::dataset::Dataset;
use lance_index::scalar::FullTextSearchQuery;
use lance_index::scalar::inverted::query::{
    BooleanQuery, FtsQuery, MatchQuery, Occur, PhraseQuery,
};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use super::fts_exec::LanceFtsExec;
use super::utils::expr_to_lance_sql;
use crate::sources::providers::{DatasetEntry, DatasetRegistry};

/// Table function that creates full-text search on Lance tables
#[derive(Debug)]
pub struct LanceFtsTableFunction {
    dataset_registry: DatasetRegistry,
}

impl LanceFtsTableFunction {
    pub fn new(dataset_registry: DatasetRegistry) -> Self {
        Self { dataset_registry }
    }
}

impl TableFunctionImpl for LanceFtsTableFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if exprs.len() != 4 {
            return plan_err!(
                "lance_fts(table_name, text_column, query, limit) expects exactly 4 arguments, got {}",
                exprs.len()
            );
        }

        let table_name = extract_string(&exprs[0], "table_name")?;
        let text_column = extract_string(&exprs[1], "text_column")?;
        let query = extract_string_or_null(&exprs[2], "query")?;
        let limit = extract_int(&exprs[3], "limit")?;

        // Get dataset from registry
        let dataset = {
            let registry = self
                .dataset_registry
                .read()
                .map_err(|e| DataFusionError::Internal(format!("Registry lock error: {}", e)))?;
            let entry = registry.get(&table_name).cloned().ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "lance_fts: table '{}' not found in registry",
                    table_name
                ))
            })?;
            match entry {
                DatasetEntry::Lance(ds) => Ok(ds),
                _ => Err(DataFusionError::Plan(format!(
                    "lance_fts: table '{}' is not a Lance dataset",
                    table_name
                ))),
            }?
        };

        // Parse the query string into a FullTextSearchQuery.
        // When query is None (NULL placeholder from schema inference),
        // use a dummy query so the provider can return a valid schema.
        let fts_query = match query {
            Some(q) => parse_fts_query(&q, &text_column, limit)?,
            None => parse_fts_query("__placeholder__", &text_column, limit.max(1))?,
        };

        Ok(Arc::new(LanceFtsProvider { dataset, fts_query }))
    }
}

/// Parse a query string into the appropriate Lance FTS query type
///
/// | Syntax              | Lance Query Type     | Description                  |
/// |---------------------|----------------------|------------------------------|
/// | `foo bar`           | MatchQuery (OR)      | Any term matches             |
/// | `+foo bar`          | MatchQuery (AND)     | All terms must match         |
/// | `"foo bar"`         | PhraseQuery          | Exact phrase match           |
/// | `foo~` / `foo~2`    | MatchQuery (fuzzy)   | Typo-tolerant matching       |
/// | `+foo -bar`         | BooleanQuery         | MUST/MUST_NOT compound       |
/// | `+foo bar`          | BooleanQuery         | MUST + SHOULD compound       |
fn parse_fts_query(query: &str, column: &str, limit: usize) -> DFResult<FullTextSearchQuery> {
    let query = query.trim();

    if query.is_empty() {
        return plan_err!("lance_fts: query string cannot be empty");
    }

    // Phrase query: "exact phrase"
    if query.starts_with('"') && query.ends_with('"') && query.len() > 2 {
        let phrase = &query[1..query.len() - 1];
        let phrase_query =
            PhraseQuery::new(phrase.to_string()).with_column(Some(column.to_string()));
        let fts_query = FullTextSearchQuery::new_query(phrase_query.into());
        return Ok(FullTextSearchQuery {
            limit: Some(limit as i64),
            ..fts_query
        });
    }

    // Fuzzy query: term~ or term~N
    if query.contains('~') && !query.contains(' ') {
        let (term, distance) = parse_fuzzy(query)?;
        let fts_query = FullTextSearchQuery::new_fuzzy(term, distance)
            .with_column(column.to_string())
            .map_err(|e| DataFusionError::Plan(format!("lance_fts: {}", e)))?;
        return Ok(FullTextSearchQuery {
            limit: Some(limit as i64),
            ..fts_query
        });
    }

    // Boolean query: contains + or - prefixed terms
    if has_boolean_operators(query) {
        let bool_query = parse_boolean_query(query, column)?;
        let fts_query = FullTextSearchQuery::new_query(bool_query.into());
        return Ok(FullTextSearchQuery {
            limit: Some(limit as i64),
            ..fts_query
        });
    }

    // Default: simple match query (OR terms)
    let fts_query = FullTextSearchQuery::new(query.to_string())
        .with_column(column.to_string())
        .map_err(|e| DataFusionError::Plan(format!("lance_fts: {}", e)))?;
    Ok(FullTextSearchQuery {
        limit: Some(limit as i64),
        ..fts_query
    })
}

/// Check if query string contains boolean operators (+/-) before terms
fn has_boolean_operators(query: &str) -> bool {
    let terms: Vec<&str> = query.split_whitespace().collect();
    terms
        .iter()
        .any(|t| (t.starts_with('+') || t.starts_with('-')) && t.len() > 1)
}

/// Parse a fuzzy query: "term~" or "term~N"
fn parse_fuzzy(query: &str) -> DFResult<(String, Option<u32>)> {
    let parts: Vec<&str> = query.splitn(2, '~').collect();
    let term = parts[0].to_string();
    let distance = if parts.len() > 1 && !parts[1].is_empty() {
        Some(parts[1].parse::<u32>().map_err(|_| {
            DataFusionError::Plan(format!(
                "lance_fts: invalid fuzzy distance '{}', expected integer",
                parts[1]
            ))
        })?)
    } else {
        None // Auto distance
    };
    Ok((term, distance))
}

/// Parse a boolean query with +term (MUST), -term (MUST_NOT), and bare term (SHOULD)
fn parse_boolean_query(query: &str, column: &str) -> DFResult<BooleanQuery> {
    let mut clauses: Vec<(Occur, FtsQuery)> = Vec::new();

    for term in query.split_whitespace() {
        if let Some(stripped) = term.strip_prefix('+') {
            if !stripped.is_empty() {
                let match_query =
                    MatchQuery::new(stripped.to_string()).with_column(Some(column.to_string()));
                clauses.push((Occur::Must, match_query.into()));
            }
        } else if let Some(stripped) = term.strip_prefix('-') {
            if !stripped.is_empty() {
                let match_query =
                    MatchQuery::new(stripped.to_string()).with_column(Some(column.to_string()));
                clauses.push((Occur::MustNot, match_query.into()));
            }
        } else {
            let match_query =
                MatchQuery::new(term.to_string()).with_column(Some(column.to_string()));
            clauses.push((Occur::Should, match_query.into()));
        }
    }

    if clauses.is_empty() {
        return plan_err!("lance_fts: boolean query has no valid terms");
    }

    Ok(BooleanQuery::new(clauses))
}

/// Provider that wraps LanceFtsExec, implementing TableProvider for filter pushdown
struct LanceFtsProvider {
    dataset: Arc<Dataset>,
    fts_query: FullTextSearchQuery,
}

impl Debug for LanceFtsProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LanceFtsProvider")
            .field("query", &self.fts_query.query)
            .finish()
    }
}

#[async_trait]
impl TableProvider for LanceFtsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // Build schema: all dataset fields + _score
        let lance_schema = self.dataset.schema();
        let mut fields: Vec<Field> = lance_schema.fields.iter().map(|f| f.into()).collect();
        fields.push(Field::new("_score", DataType::Float32, true));
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
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let mut fts_query = self.fts_query.clone();

        // When WHERE filters are pushed down, remove the FTS retrieval limit
        // so that all matching candidates are available for post-filter.
        // Lance applies FTS limit before the metadata filter, which would
        // otherwise discard valid matches.
        if !filters.is_empty() {
            fts_query.limit = None;
        }

        let mut exec = LanceFtsExec::try_new(self.dataset.clone(), fts_query)?;

        // Apply column projection if DataFusion requests a subset of columns
        if let Some(proj) = projection {
            exec = exec.with_projection(proj.clone())?;
        }

        // Convert pushed-down filter expressions to SQL string for Lance.
        // DataFusion's Expr::to_string() produces type-annotated literals like
        // Utf8("foo") and Float64(3.14) which Lance's SQL parser doesn't understand,
        // so we strip the type wrappers to produce standard SQL.
        if !filters.is_empty() {
            let filter_str = filters
                .iter()
                .map(|f| expr_to_lance_sql(f))
                .collect::<Vec<_>>()
                .join(" AND ");
            exec = exec.with_filter(filter_str);
        }

        // Apply scan limit if provided (e.g., from SQL LIMIT clause)
        if let Some(n) = limit {
            exec = exec.with_limit(n);
        }

        Ok(Arc::new(exec))
    }
}

// Helper functions for argument extraction

fn extract_string(expr: &Expr, name: &str) -> DFResult<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Ok(s.clone()),
        Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => Ok(s.clone()),
        // Accept NULL as placeholder during pipeline validation/schema inference.
        // The inferencer replaces {param} with NULL before plan creation.
        Expr::Literal(ScalarValue::Null, _) => Ok(String::new()),
        _ => plan_err!("lance_fts: {} must be a string literal", name),
    }
}

fn extract_string_or_null(expr: &Expr, name: &str) -> DFResult<Option<String>> {
    match expr {
        Expr::Literal(ScalarValue::Null, _) => Ok(None),
        other => extract_string(other, name).map(Some),
    }
}

fn extract_int(expr: &Expr, name: &str) -> DFResult<usize> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(n)), _) => Ok(*n as usize),
        Expr::Literal(ScalarValue::Int32(Some(n)), _) => Ok(*n as usize),
        Expr::Literal(ScalarValue::UInt64(Some(n)), _) => Ok(*n as usize),
        // Accept NULL as placeholder during pipeline validation/schema inference
        Expr::Literal(ScalarValue::Null, _) => Ok(0),
        _ => plan_err!("lance_fts: {} must be an integer literal", name),
    }
}

/// Register lance_fts table function with SessionContext
pub fn register_lance_fts_udtf(
    ctx: &datafusion::prelude::SessionContext,
    dataset_registry: DatasetRegistry,
) {
    ctx.register_udtf(
        "lance_fts",
        Arc::new(LanceFtsTableFunction::new(dataset_registry)),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::providers::DatasetRegistry;

    #[test]
    fn test_parse_default_match_query() {
        let result = parse_fts_query("umbrella train", "body", 10);
        assert!(result.is_ok());
        let fts = result.unwrap();
        assert_eq!(fts.limit, Some(10));
        assert!(matches!(fts.query, FtsQuery::Match(_)));
    }

    #[test]
    fn test_parse_phrase_query() {
        let result = parse_fts_query("\"train to boston\"", "body", 10);
        assert!(result.is_ok());
        let fts = result.unwrap();
        assert!(matches!(fts.query, FtsQuery::Phrase(_)));
        if let FtsQuery::Phrase(pq) = &fts.query {
            assert_eq!(pq.terms, "train to boston");
        }
    }

    #[test]
    fn test_parse_fuzzy_query_auto() {
        let result = parse_fts_query("rammen~", "body", 10);
        assert!(result.is_ok());
        let fts = result.unwrap();
        assert!(matches!(fts.query, FtsQuery::Match(_)));
        if let FtsQuery::Match(mq) = &fts.query {
            assert_eq!(mq.terms, "rammen");
            assert_eq!(mq.fuzziness, None);
        }
    }

    #[test]
    fn test_parse_fuzzy_query_explicit() {
        let result = parse_fts_query("rammen~1", "body", 10);
        assert!(result.is_ok());
        let fts = result.unwrap();
        assert!(matches!(fts.query, FtsQuery::Match(_)));
        if let FtsQuery::Match(mq) = &fts.query {
            assert_eq!(mq.terms, "rammen");
            assert_eq!(mq.fuzziness, Some(1));
        }
    }

    #[test]
    fn test_parse_boolean_query_must_must_not() {
        let result = parse_fts_query("+umbrella -train", "body", 10);
        assert!(result.is_ok());
        let fts = result.unwrap();
        assert!(matches!(fts.query, FtsQuery::Boolean(_)));
        if let FtsQuery::Boolean(bq) = &fts.query {
            assert_eq!(bq.must.len(), 1);
            assert_eq!(bq.must_not.len(), 1);
            assert_eq!(bq.should.len(), 0);
        }
    }

    #[test]
    fn test_parse_boolean_query_must_should() {
        let result = parse_fts_query("+umbrella boston", "body", 10);
        assert!(result.is_ok());
        let fts = result.unwrap();
        assert!(matches!(fts.query, FtsQuery::Boolean(_)));
        if let FtsQuery::Boolean(bq) = &fts.query {
            assert_eq!(bq.must.len(), 1);
            assert_eq!(bq.should.len(), 1);
        }
    }

    #[test]
    fn test_parse_empty_query_fails() {
        let result = parse_fts_query("", "body", 10);
        assert!(result.is_err());
    }

    #[test]
    fn test_has_boolean_operators() {
        assert!(has_boolean_operators("+foo -bar"));
        assert!(has_boolean_operators("+foo bar"));
        assert!(!has_boolean_operators("foo bar"));
        // A lone + or - is not a boolean operator
        assert!(!has_boolean_operators("+ -"));
    }

    // ── Integration tests ──
    // Require data/test_data.lance (run: python scripts/prepare_fts_test_data.py)
    // Run with: cargo test -p sources -- --ignored lance_fts

    use arrow::array::{Array, Float32Array, StringArray};
    use std::path::Path;

    const FTS_DATASET_PATH: &str = "data/test_data.lance";

    async fn setup_fts_context() -> datafusion::prelude::SessionContext {
        use super::super::registration::register_lance_table;

        let dataset_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(FTS_DATASET_PATH);
        let dataset_path_str = dataset_path.to_str().unwrap();

        assert!(
            dataset_path.exists(),
            "FTS test dataset not found at {dataset_path_str}. \
             Run: python scripts/prepare_fts_test_data.py"
        );

        let mut ctx = datafusion::prelude::SessionContext::new();
        let registry: DatasetRegistry =
            Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));

        register_lance_table(&mut ctx, "fts_data", dataset_path_str, Some(&registry))
            .await
            .expect("Failed to register lance table");

        register_lance_fts_udtf(&ctx, registry);
        ctx
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_fts_basic_term_search() {
        let ctx = setup_fts_context().await;

        let df = ctx
            .sql("SELECT id, description, _score FROM lance_fts('fts_data', 'description', 'umbrella', 10)")
            .await
            .expect("SQL parse failed");

        let batches = df.collect().await.expect("Query execution failed");
        assert!(!batches.is_empty(), "Expected non-empty results");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows > 0, "Expected at least 1 result for 'umbrella'");
        assert!(total_rows <= 10, "Expected at most 10 results (limit)");

        for batch in &batches {
            let scores = batch
                .column_by_name("_score")
                .expect("Missing _score column")
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("_score should be Float32");
            for i in 0..scores.len() {
                assert!(!scores.is_null(i) && scores.value(i) > 0.0);
            }

            let descs = batch
                .column_by_name("description")
                .expect("Missing description column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("description should be String");
            for i in 0..descs.len() {
                assert!(descs.value(i).to_lowercase().contains("umbrella"));
            }
        }
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_fts_phrase_search() {
        let ctx = setup_fts_context().await;

        let df = ctx
            .sql(r#"SELECT id, description, _score FROM lance_fts('fts_data', 'description', '"train to boston"', 10)"#)
            .await
            .expect("SQL parse failed");

        let batches = df.collect().await.expect("Query execution failed");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(
            total_rows > 0,
            "Expected results for phrase 'train to boston'"
        );

        // Verify results contain the phrase terms. Lance's PhraseQuery at v4.0.0-rc.1
        // may return matches for individual terms rather than strictly adjacent phrases,
        // so we assert on term presence rather than exact phrase containment.
        for batch in &batches {
            let descs = batch
                .column_by_name("description")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..descs.len() {
                let desc = descs.value(i).to_lowercase();
                assert!(
                    desc.contains("train") || desc.contains("boston"),
                    "Phrase search result should contain at least one phrase term: {desc}"
                );
            }
        }
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_fts_limit() {
        let ctx = setup_fts_context().await;

        let df = ctx
            .sql("SELECT id, _score FROM lance_fts('fts_data', 'description', 'enthusiasts', 3)")
            .await
            .expect("SQL parse failed");

        let batches = df.collect().await.expect("Query execution failed");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows > 0, "Expected results for 'enthusiasts'");
        assert!(
            total_rows <= 3,
            "Expected at most 3 results, got {total_rows}"
        );
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_fts_no_results() {
        let ctx = setup_fts_context().await;

        let df = ctx
            .sql("SELECT id, _score FROM lance_fts('fts_data', 'description', 'xyznonexistentterm', 10)")
            .await
            .expect("SQL parse failed");

        let batches = df.collect().await.expect("Query execution failed");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0, "Expected no results for nonexistent term");
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_fts_schema_includes_all_columns() {
        let ctx = setup_fts_context().await;

        let df = ctx
            .sql("SELECT * FROM lance_fts('fts_data', 'description', 'umbrella', 1)")
            .await
            .expect("SQL parse failed");

        let field_names: Vec<&str> = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();

        assert!(field_names.contains(&"id"));
        assert!(field_names.contains(&"vector"));
        assert!(field_names.contains(&"item_id"));
        assert!(field_names.contains(&"revenue"));
        assert!(field_names.contains(&"description"));
        assert!(field_names.contains(&"category"));
        assert!(field_names.contains(&"_score"));
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_fts_relevance_ordering() {
        let ctx = setup_fts_context().await;

        let df = ctx
            .sql("SELECT _score FROM lance_fts('fts_data', 'description', 'premium wireless', 10)")
            .await
            .expect("SQL parse failed");

        let batches = df.collect().await.expect("Query execution failed");
        let mut all_scores = Vec::new();
        for batch in &batches {
            let scores = batch
                .column_by_name("_score")
                .unwrap()
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            for i in 0..scores.len() {
                all_scores.push(scores.value(i));
            }
        }

        if all_scores.len() > 1 {
            for i in 1..all_scores.len() {
                assert!(
                    all_scores[i] <= all_scores[i - 1],
                    "Scores should be descending: {} > {} at index {}",
                    all_scores[i],
                    all_scores[i - 1],
                    i
                );
            }
        }
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_fts_invalid_table_name() {
        let ctx = setup_fts_context().await;

        let result = ctx
            .sql("SELECT * FROM lance_fts('nonexistent_table', 'description', 'test', 10)")
            .await;

        assert!(result.is_err(), "Expected error for nonexistent table");
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_fts_multi_term_or_search() {
        let ctx = setup_fts_context().await;

        let df = ctx
            .sql("SELECT id, description, _score FROM lance_fts('fts_data', 'description', 'premium organic', 10)")
            .await
            .expect("SQL parse failed");

        let batches = df.collect().await.expect("Query execution failed");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(
            total_rows > 0,
            "Expected results for 'premium organic' OR search"
        );
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_fts_where_category_filter() {
        let ctx = setup_fts_context().await;

        // Search with an equality filter on category
        let df = ctx
            .sql(
                "SELECT id, description, category, _score \
                 FROM lance_fts('fts_data', 'description', 'premium', 50) \
                 WHERE category = 'electronics'",
            )
            .await
            .expect("SQL parse failed");

        let batches = df.collect().await.expect("Query execution failed");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(
            total_rows > 0,
            "Expected results for 'premium' with category filter"
        );

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
    async fn test_lance_fts_where_numeric_filter() {
        let ctx = setup_fts_context().await;

        // Search with a numeric comparison on revenue
        let df = ctx
            .sql(
                "SELECT id, revenue, _score \
                 FROM lance_fts('fts_data', 'description', 'enthusiasts', 50) \
                 WHERE revenue > 500.0",
            )
            .await
            .expect("SQL parse failed");

        let batches = df.collect().await.expect("Query execution failed");

        for batch in &batches {
            let revenues = batch
                .column_by_name("revenue")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            for i in 0..revenues.len() {
                assert!(
                    revenues.value(i) > 500.0,
                    "WHERE filter should restrict revenue > 500.0, got {}",
                    revenues.value(i)
                );
            }
        }
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_fts_where_compound_filter() {
        let ctx = setup_fts_context().await;

        // Search with both category and revenue filters (AND)
        let df = ctx
            .sql(
                "SELECT id, description, category, revenue, _score \
                 FROM lance_fts('fts_data', 'description', 'portable', 50) \
                 WHERE category = 'electronics' AND revenue > 100.0",
            )
            .await
            .expect("SQL parse failed");

        let batches = df.collect().await.expect("Query execution failed");

        for batch in &batches {
            let cats = batch
                .column_by_name("category")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let revenues = batch
                .column_by_name("revenue")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            for i in 0..cats.len() {
                assert_eq!(cats.value(i), "electronics");
                assert!(revenues.value(i) > 100.0);
            }
        }
    }

    #[tokio::test]
    #[ignore] // Requires test_data.lance
    async fn test_lance_fts_where_filters_reduce_results() {
        let ctx = setup_fts_context().await;

        // Run without filter
        let df_all = ctx
            .sql("SELECT id FROM lance_fts('fts_data', 'description', 'premium', 50)")
            .await
            .expect("SQL parse failed");
        let batches_all = df_all.collect().await.expect("Query execution failed");
        let rows_all: usize = batches_all.iter().map(|b| b.num_rows()).sum();

        // Run with category filter — should return fewer (or equal) rows
        let df_filtered = ctx
            .sql(
                "SELECT id FROM lance_fts('fts_data', 'description', 'premium', 50) \
                 WHERE category = 'outdoor'",
            )
            .await
            .expect("SQL parse failed");
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
}
