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
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, RwLock};

use super::fts_exec::LanceFtsExec;

/// Table function that creates full-text search on Lance tables
#[derive(Debug)]
pub struct LanceFtsTableFunction {
    dataset_registry: Arc<RwLock<HashMap<String, Arc<Dataset>>>>,
}

impl LanceFtsTableFunction {
    pub fn new(dataset_registry: Arc<RwLock<HashMap<String, Arc<Dataset>>>>) -> Self {
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
        let query = extract_string(&exprs[2], "query")?;
        let limit = extract_int(&exprs[3], "limit")?;

        // Get dataset from registry
        let dataset = {
            let registry = self
                .dataset_registry
                .read()
                .map_err(|e| DataFusionError::Internal(format!("Registry lock error: {}", e)))?;
            registry.get(&table_name).cloned().ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "lance_fts: table '{}' not found in registry",
                    table_name
                ))
            })?
        };

        // Parse the query string into a FullTextSearchQuery
        let fts_query = parse_fts_query(&query, &text_column, limit)?;

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
        _projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let mut exec = LanceFtsExec::try_new(self.dataset.clone(), self.fts_query.clone())?;

        // Convert pushed-down filter expressions to SQL string for Lance
        if !filters.is_empty() {
            let filter_str = filters
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
                .join(" AND ");
            exec = exec.with_filter(filter_str);
        }

        Ok(Arc::new(exec))
    }
}

// Helper functions for argument extraction

fn extract_string(expr: &Expr, name: &str) -> DFResult<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Ok(s.clone()),
        Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => Ok(s.clone()),
        _ => plan_err!("lance_fts: {} must be a string literal", name),
    }
}

fn extract_int(expr: &Expr, name: &str) -> DFResult<usize> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(n)), _) => Ok(*n as usize),
        Expr::Literal(ScalarValue::Int32(Some(n)), _) => Ok(*n as usize),
        Expr::Literal(ScalarValue::UInt64(Some(n)), _) => Ok(*n as usize),
        _ => plan_err!("lance_fts: {} must be an integer literal", name),
    }
}

/// Register lance_fts table function with SessionContext
pub fn register_lance_fts_udtf(
    ctx: &datafusion::prelude::SessionContext,
    dataset_registry: Arc<RwLock<HashMap<String, Arc<Dataset>>>>,
) {
    ctx.register_udtf(
        "lance_fts",
        Arc::new(LanceFtsTableFunction::new(dataset_registry)),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
