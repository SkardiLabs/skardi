//! Table function for MongoDB full-text search (`$text` queries).
//!
//! Usage:
//! ```sql
//! -- Basic full-text search
//! SELECT * FROM mongo_fts('collection_name', 'search query terms', 60)
//!
//! -- With WHERE clause filter pushdown
//! SELECT * FROM mongo_fts('collection_name', 'search terms', 100)
//! WHERE teamId = 'team123' AND datasetId = 'ds456'
//! ```
//!
//! Returns all collection columns plus `_score Float64` (MongoDB textScore).
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
use mongodb::Collection;
use mongodb::bson::{Bson, Document};
use std::any::Any;
use std::sync::Arc;

use super::fts_exec::MongoFtsExec;
use super::{binary_expr_to_mongo, is_pushable_binary_filter};
use crate::sources::providers::udtf_args::string_arg;
use crate::sources::providers::{DatasetEntry, DatasetRegistry};

/// Maximum allowed FTS result limit (matches MAX_KNN_K).
const MAX_FTS_LIMIT: usize = 500;

/// Entry stored in the DatasetRegistry for each MongoDB collection.
#[derive(Clone, Debug)]
pub struct MongoFtsEntry {
    pub collection: Collection<Document>,
    pub schema: SchemaRef,
    pub primary_key: String,
}

// ─── TableFunctionImpl ───────────────────────────────────────────────────────

/// Table function that performs MongoDB full-text search.
#[derive(Debug)]
pub struct MongoFtsTableFunction {
    registry: DatasetRegistry,
}

impl MongoFtsTableFunction {
    pub fn new(registry: DatasetRegistry) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for MongoFtsTableFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if exprs.len() != 3 {
            return plan_err!(
                "mongo_fts(collection, query, limit) expects 3 arguments, got {}",
                exprs.len()
            );
        }

        let collection_name = string_arg(&exprs[0], "mongo_fts", "collection")?;
        let query = string_arg(&exprs[1], "mongo_fts", "query")?;
        let limit = extract_int(&exprs[2], "limit")?;

        // The inferencer replaces {param} with NULL, yielding empty string for
        // strings and 0 for integers. Accept these as placeholders — validate only
        // when real values are provided.
        if !query.is_empty() && limit > MAX_FTS_LIMIT {
            return plan_err!(
                "mongo_fts: limit must be between 1 and {}, got {}",
                MAX_FTS_LIMIT,
                limit
            );
        }
        // Use a safe default when limit is a NULL placeholder (0).
        let limit = if limit == 0 { 1 } else { limit };

        // Look up the MongoFtsEntry from the registry.
        let entry = {
            let reg = self.registry.read().map_err(|e| {
                DataFusionError::Internal(format!("mongo_fts registry lock error: {}", e))
            })?;
            let raw = reg.get(&collection_name).cloned().ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "mongo_fts: collection '{}' not found in registry. \
                     Make sure the data source is declared with type 'mongo'.",
                    collection_name
                ))
            })?;
            match raw {
                DatasetEntry::Mongo(e) => e,
                _ => {
                    return plan_err!(
                        "mongo_fts: '{}' is not a MongoDB collection",
                        collection_name
                    );
                }
            }
        };

        // Build output schema: all collection columns + _score.
        let mut fields: Vec<Field> = entry
            .schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new("_score", DataType::Float64, true));
        let schema: SchemaRef = Arc::new(Schema::new(fields));

        Ok(Arc::new(MongoFtsProvider {
            collection: entry.collection,
            query,
            limit,
            schema,
            primary_key: entry.primary_key,
        }))
    }
}

// ─── TableProvider ───────────────────────────────────────────────────────────

struct MongoFtsProvider {
    collection: Collection<Document>,
    query: String,
    limit: usize,
    schema: SchemaRef,
    primary_key: String,
}

impl std::fmt::Debug for MongoFtsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoFtsProvider")
            .field("query", &self.query)
            .field("limit", &self.limit)
            .finish()
    }
}

#[async_trait]
impl TableProvider for MongoFtsProvider {
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
                if is_pushable_binary_filter(expr) {
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
        // Build the MongoDB filter using $and to avoid key collisions when
        // multiple filters target the same field (e.g. price > 10 AND price < 100).
        let mut filter_parts: Vec<Bson> = Vec::new();

        for expr in filters {
            if let Some(doc) = expr_to_mongo_filter_entry(expr, &self.primary_key) {
                filter_parts.push(Bson::Document(doc));
            }
        }

        let filter = if filter_parts.is_empty() {
            None
        } else if filter_parts.len() == 1 {
            // Single filter — no need to wrap in $and.
            filter_parts.pop().and_then(|b| b.as_document().cloned())
        } else {
            Some(mongodb::bson::doc! { "$and": filter_parts })
        };

        // Build schema respecting column projection.
        let schema = if let Some(proj) = projection {
            let fields: Vec<Field> = proj.iter().map(|&i| self.schema.field(i).clone()).collect();
            Arc::new(Schema::new(fields))
        } else {
            self.schema.clone()
        };

        let exec = MongoFtsExec::new(
            self.collection.clone(),
            self.query.clone(),
            self.limit,
            filter,
            limit,
            schema,
            self.primary_key.clone(),
        );

        Ok(Arc::new(exec))
    }
}

// ─── Filter helpers ──────────────────────────────────────────────────────────

/// Convert a single DataFusion Expr to a MongoDB filter Document entry.
fn expr_to_mongo_filter_entry(expr: &Expr, primary_key: &str) -> Option<Document> {
    match expr {
        Expr::BinaryExpr(binary) => {
            binary_expr_to_mongo(&binary.left, &binary.op, &binary.right, primary_key).ok()
        }
        _ => None,
    }
}

// ─── Argument extraction helpers ─────────────────────────────────────────────

fn extract_int(expr: &Expr, name: &str) -> DFResult<usize> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(v)), _) => Ok(*v as usize),
        Expr::Literal(ScalarValue::Int32(Some(v)), _) => Ok(*v as usize),
        Expr::Literal(ScalarValue::UInt64(Some(v)), _) => Ok(*v as usize),
        // Accept NULL as placeholder during pipeline validation/schema inference
        Expr::Literal(ScalarValue::Null, _) => Ok(0),
        _ => plan_err!(
            "mongo_fts: '{}' must be an integer literal, got {:?}",
            name,
            expr
        ),
    }
}

// ─── Registration ────────────────────────────────────────────────────────────

/// Register the `mongo_fts` table function on the given session context.
pub fn register_mongo_fts_udtf(ctx: &SessionContext, registry: DatasetRegistry) {
    ctx.register_udtf("mongo_fts", Arc::new(MongoFtsTableFunction::new(registry)));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::providers::mongo::register_mongo_tables;
    use arrow::array::{Array, Float64Array, RecordBatch, StringArray};
    use std::collections::HashMap;
    use std::sync::RwLock;

    /// Register a MongoDB collection from the CI docker service and return
    /// the shared dataset registry so mongo_fts() can look it up.
    async fn register_ci_fts(ctx: &mut SessionContext) -> DatasetRegistry {
        let registry: DatasetRegistry = Arc::new(RwLock::new(HashMap::new()));

        let mut options = HashMap::new();
        options.insert("database".to_string(), "mydb".to_string());
        options.insert("collection".to_string(), "dataset_data_texts".to_string());
        options.insert("primary_key".to_string(), "dataId".to_string());
        options.insert("user_env".to_string(), "MONGO_USER".to_string());
        options.insert("pass_env".to_string(), "MONGO_PASS".to_string());

        register_mongo_tables(
            ctx,
            "dataset_data_texts",
            "mongodb://127.0.0.1:27017",
            Some(&options),
            Some(&registry),
        )
        .await
        .expect("register dataset_data_texts failed");

        register_mongo_fts_udtf(ctx, Arc::clone(&registry));
        registry
    }

    async fn query_all(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        let df = ctx.sql(sql).await.expect("parse sql");
        df.collect().await.expect("collect results")
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    // ─── mongo_fts integration tests ─────────────────────────────────────
    // Require CI MongoDB with seeded dataset_data_texts collection + simple
    // text index { fullTextToken: "text" }.

    #[tokio::test]
    #[ignore]
    async fn test_fts_basic_search() {
        let mut ctx = SessionContext::new();
        let _reg = register_ci_fts(&mut ctx).await;

        let batches = query_all(
            &ctx,
            r#"SELECT "dataId", _score FROM mongo_fts('dataset_data_texts', 'machine learning', 10)"#,
        )
        .await;

        let rows = total_rows(&batches);
        assert!(
            rows >= 2,
            "expected at least 2 results for 'machine learning', got {rows}"
        );

        // _score should be present and positive (textScore)
        let scores = batches[0]
            .column_by_name("_score")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(scores.value(0) > 0.0, "textScore should be positive");
    }

    #[tokio::test]
    #[ignore]
    async fn test_fts_respects_limit() {
        let mut ctx = SessionContext::new();
        let _reg = register_ci_fts(&mut ctx).await;

        // "learning" matches multiple docs, but limit to 1
        let batches = query_all(
            &ctx,
            r#"SELECT "dataId" FROM mongo_fts('dataset_data_texts', 'learning', 1)"#,
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
            r#"SELECT "dataId" FROM mongo_fts('dataset_data_texts', 'xyznonexistent', 10)"#,
        )
        .await;
        assert_eq!(total_rows(&batches), 0);
    }

    #[tokio::test]
    #[ignore]
    async fn test_fts_with_where_filter() {
        let mut ctx = SessionContext::new();
        let _reg = register_ci_fts(&mut ctx).await;

        // "machine learning" matches docs in both team1 and team2;
        // filtering by team1 should exclude team2's doc.
        let all_batches = query_all(
            &ctx,
            r#"SELECT "dataId", "teamId" FROM mongo_fts('dataset_data_texts', 'machine learning', 10)"#,
        )
        .await;
        let all_rows = total_rows(&all_batches);

        let filtered_batches = query_all(
            &ctx,
            r#"SELECT "dataId", "teamId" FROM mongo_fts('dataset_data_texts', 'machine learning', 10) WHERE "teamId" = 'team1'"#,
        )
        .await;
        let filtered_rows = total_rows(&filtered_batches);

        assert!(
            filtered_rows < all_rows,
            "filtered rows ({filtered_rows}) should be fewer than unfiltered ({all_rows})"
        );

        // Verify all returned rows have teamId = 'team1'
        for batch in &filtered_batches {
            let teams = batch
                .column_by_name("teamId")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..teams.len() {
                assert_eq!(teams.value(i), "team1");
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
            r#"SELECT "dataId", _score FROM mongo_fts('dataset_data_texts', 'machine learning', 10) ORDER BY _score DESC"#,
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

        // Phrase search: "neural network" should match docs containing the exact phrase
        let batches = query_all(
            &ctx,
            r#"SELECT "dataId" FROM mongo_fts('dataset_data_texts', '"neural network"', 10)"#,
        )
        .await;

        let rows = total_rows(&batches);
        assert!(
            rows >= 1,
            "expected at least 1 result for phrase 'neural network', got {rows}"
        );
    }

    // ─── NULL placeholder tests (unit, no MongoDB needed) ────────────────
    // The pipeline schema inferencer replaces {param} with NULL before
    // planning. These tests verify that mongo_fts accepts NULL placeholders
    // in any combination without erroring during call().

    /// Helper: create a MongoFtsTableFunction backed by an empty registry.
    /// call() will fail at registry lookup, but we only care about argument
    /// validation passing first.
    fn make_fts_function() -> MongoFtsTableFunction {
        let registry: DatasetRegistry = Arc::new(RwLock::new(HashMap::new()));
        MongoFtsTableFunction::new(registry)
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
        // Both params are NULL (e.g. `mongo_fts('t', {query}, {limit})`)
        let func = make_fts_function();
        let result = func.call(&[lit_str("some_table"), lit_null(), lit_null()]);
        // Should fail at registry lookup, NOT at argument validation
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not found in registry"),
            "expected registry error, got: {err}"
        );
    }

    #[test]
    fn test_null_query_with_literal_limit_accepted() {
        // Only query is NULL, limit is literal (e.g. `mongo_fts('t', {text_query}, 60)`)
        // This is the exact case that broke the RAG hybrid search pipeline.
        let func = make_fts_function();
        let result = func.call(&[lit_str("some_table"), lit_null(), lit_int(60)]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not found in registry"),
            "expected registry error, got: {err}"
        );
    }

    #[test]
    fn test_literal_query_with_null_limit_accepted() {
        // Query is literal, limit is NULL
        let func = make_fts_function();
        let result = func.call(&[lit_str("some_table"), lit_str("test query"), lit_null()]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not found in registry"),
            "expected registry error, got: {err}"
        );
    }

    #[test]
    fn test_limit_over_max_rejected() {
        // Real query with limit exceeding MAX_FTS_LIMIT should fail
        let func = make_fts_function();
        let result = func.call(&[lit_str("some_table"), lit_str("test"), lit_int(501)]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("limit must be between 1 and 500"),
            "expected limit error, got: {err}"
        );
    }

    #[test]
    fn test_wrong_arg_count_rejected() {
        let func = make_fts_function();
        let result = func.call(&[lit_str("table"), lit_str("query")]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("expects 3 arguments"),
            "expected arg count error, got: {err}"
        );
    }
}
