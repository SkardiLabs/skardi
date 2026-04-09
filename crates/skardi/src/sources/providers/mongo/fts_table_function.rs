//! Table function for MongoDB full-text search (`$text` queries).
//!
//! Usage:
//! ```sql
//! -- Basic full-text search
//! SELECT * FROM mongo_fts('collection_name', 'search query terms', 60)
//!
//! -- With inline filter (4th argument)
//! SELECT * FROM mongo_fts('collection_name', 'search terms', 60, 'teamId = "xxx"')
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
use mongodb::bson::Document;
use std::any::Any;
use std::sync::Arc;

use super::binary_expr_to_mongo;
use super::fts_exec::MongoFtsExec;
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
        if exprs.len() < 3 || exprs.len() > 4 {
            return plan_err!(
                "mongo_fts(collection, query, limit [, filter]) expects 3-4 arguments, got {}",
                exprs.len()
            );
        }

        let collection_name = extract_string(&exprs[0], "collection")?;
        let query = extract_string(&exprs[1], "query")?;
        let limit = extract_int(&exprs[2], "limit")?;

        // Skip validation when values are NULL placeholders from pipeline schema inference.
        // The inferencer replaces {param} with NULL, yielding empty string / 0.
        let is_inference = query.is_empty() && limit == 0;
        if !is_inference {
            if query.is_empty() {
                return plan_err!("mongo_fts: query string must not be empty");
            }
            if limit == 0 || limit > MAX_FTS_LIMIT {
                return plan_err!(
                    "mongo_fts: limit must be between 1 and {}, got {}",
                    MAX_FTS_LIMIT,
                    limit
                );
            }
        }
        // Use a safe default during inference so the plan can be created.
        let limit = if limit == 0 { 1 } else { limit };

        let inline_filter = if exprs.len() == 4 {
            extract_string(&exprs[3], "filter").ok()
        } else {
            None
        };

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
            inline_filter,
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
    inline_filter: Option<String>,
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
                if can_push_to_mongo(expr) {
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
        // Build the MongoDB filter document from inline filter + pushed-down WHERE.
        let mut filter_doc = Document::new();

        // Parse inline filter (4th arg) as simple "field = value" pairs.
        if let Some(ref f) = self.inline_filter {
            if let Some(doc) = parse_inline_filter(f, &self.primary_key) {
                for (key, value) in doc.iter() {
                    filter_doc.insert(key.clone(), value.clone());
                }
            }
        }

        // Convert pushed-down DataFusion Expr filters to MongoDB filter entries.
        for expr in filters {
            if let Some(doc) = expr_to_mongo_filter_entry(expr, &self.primary_key) {
                for (key, value) in doc.iter() {
                    filter_doc.insert(key.clone(), value.clone());
                }
            }
        }

        let filter = if filter_doc.is_empty() {
            None
        } else {
            Some(filter_doc)
        };

        // Build schema respecting column projection.
        let schema = if let Some(proj) = projection {
            let fields: Vec<Field> = proj.iter().map(|&i| self.schema.field(i).clone()).collect();
            Arc::new(Schema::new(fields))
        } else {
            self.schema.clone()
        };

        let mut exec = MongoFtsExec::new(
            self.collection.clone(),
            self.query.clone(),
            self.limit,
            filter,
            schema,
            self.primary_key.clone(),
        );

        if let Some(n) = limit {
            exec = exec.with_scan_limit(Some(n));
        }

        Ok(Arc::new(exec))
    }
}

// ─── Filter helpers ──────────────────────────────────────────────────────────

/// Check if a DataFusion expression can be pushed down to MongoDB.
fn can_push_to_mongo(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(binary) => {
            use datafusion::logical_expr::Operator;
            matches!(
                binary.op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            ) && matches!(
                (binary.left.as_ref(), binary.right.as_ref()),
                (Expr::Column(_), Expr::Literal(..)) | (Expr::Literal(..), Expr::Column(_))
            )
        }
        _ => false,
    }
}

/// Convert a single DataFusion Expr to a MongoDB filter Document entry.
fn expr_to_mongo_filter_entry(expr: &Expr, primary_key: &str) -> Option<Document> {
    match expr {
        Expr::BinaryExpr(binary) => {
            binary_expr_to_mongo(&binary.left, &binary.op, &binary.right, primary_key).ok()
        }
        _ => None,
    }
}

/// Parse a simple inline filter string like `teamId = "xxx"` into a MongoDB
/// filter document. Supports simple equality for now.
fn parse_inline_filter(filter: &str, primary_key: &str) -> Option<Document> {
    // Split on '=' for simple equality.
    let parts: Vec<&str> = filter.splitn(2, '=').collect();
    if parts.len() != 2 {
        return None;
    }
    let field = parts[0].trim();
    let value = parts[1].trim().trim_matches('"').trim_matches('\'');

    let key = if field == primary_key { "_id" } else { field };
    Some(mongodb::bson::doc! { key: value })
}

// ─── Argument extraction helpers ─────────────────────────────────────────────

fn extract_string(expr: &Expr, name: &str) -> DFResult<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => Ok(s.clone()),
        // Accept NULL as placeholder during pipeline validation/schema inference.
        // The inferencer replaces {param} with NULL before plan creation.
        Expr::Literal(ScalarValue::Null, _) => Ok(String::new()),
        _ => plan_err!(
            "mongo_fts: '{}' must be a string literal, got {:?}",
            name,
            expr
        ),
    }
}

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
}
