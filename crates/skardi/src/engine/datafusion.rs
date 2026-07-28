//! DataFusion implementation of the Engine trait
//!
//! This module contains the DataFusion-specific implementation of the `Engine` trait,
//! providing SQL query execution capabilities using the DataFusion query engine.

use super::Engine;
use anyhow::Result;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::prelude::*;
use std::sync::Arc;

/// DataFusion implementation of the Engine trait
///
/// `DataFusionEngine` provides SQL query execution using the DataFusion query engine.
/// This implementation uses a pre-configured SessionContext for executing SQL queries
/// against registered data sources.
pub struct DataFusionEngine {
    /// DataFusion session context for query execution (wrapped in Arc for sharing)
    ctx: Arc<SessionContext>,
}

impl DataFusionEngine {
    /// Create a new DataFusion engine instance with the provided SessionContext
    ///
    /// # Arguments
    ///
    /// * `ctx` - Pre-configured DataFusion SessionContext with registered tables
    ///
    /// # Returns
    ///
    /// Returns a new `DataFusionEngine` instance ready for query execution
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use datafusion::prelude::*;
    /// use skardi::engine::datafusion::DataFusionEngine;
    ///
    /// let ctx = SessionContext::new();
    /// // Register tables, configure settings, etc.
    /// // ctx.register_csv("my_table", "data/file.csv", CsvReadOptions::new()).await?;
    /// let engine = DataFusionEngine::new(ctx);
    /// ```
    pub fn new(ctx: SessionContext) -> Self {
        Self { ctx: Arc::new(ctx) }
    }

    /// Create a new DataFusion engine instance with an `Arc<SessionContext>`
    ///
    /// This constructor allows creating an engine with a shared SessionContext.
    ///
    /// # Arguments
    ///
    /// * `ctx` - Arc-wrapped SessionContext for sharing
    ///
    /// # Returns
    ///
    /// Returns a new `DataFusionEngine` instance ready for query execution
    pub fn new_with_arc(ctx: Arc<SessionContext>) -> Self {
        Self { ctx }
    }

    /// Get a reference to the underlying SessionContext
    ///
    /// This method allows access to the SessionContext for operations like
    /// retrieving table schemas and catalog information.
    ///
    /// # Returns
    ///
    /// Returns a reference to the internal SessionContext
    pub fn session_context(&self) -> &SessionContext {
        &self.ctx
    }

    /// Get the `Arc<SessionContext>` for sharing
    ///
    /// This method allows cloning the Arc to share the SessionContext with other components.
    ///
    /// # Returns
    ///
    /// Returns a clone of the `Arc<SessionContext>`
    pub fn session_context_arc(&self) -> Arc<SessionContext> {
        self.ctx.clone()
    }

    /// Execute a SQL query with a row-count cap pushed into the query plan.
    ///
    /// Applies `LIMIT fetch` on top of the query's logical plan before
    /// collecting, so at most `fetch` rows are materialized. Only meaningful
    /// for query statements (SELECT/...); DML plans should go through
    /// [`Engine::execute`] instead.
    pub async fn execute_with_limit(&self, sql: &str, fetch: usize) -> Result<RecordBatch> {
        let dataframe = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute SQL query: {}", e))?
            .limit(0, Some(fetch))
            .map_err(|e| anyhow::anyhow!("Failed to apply row limit: {}", e))?;

        let schema = dataframe.schema().inner().clone();
        let batches = dataframe
            .collect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to collect query results: {}", e))?;

        batches_to_single(schema, batches)
    }
}

#[async_trait]
impl Engine for DataFusionEngine {
    /// Execute a SQL query using DataFusion
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL query string to execute
    ///
    /// # Returns
    ///
    /// Returns a `Result<RecordBatch>` containing the query results
    async fn execute(&self, sql: &str) -> Result<RecordBatch> {
        // Execute the SQL query against the registered tables
        let dataframe = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute SQL query: {}", e))?;

        // Get the schema before collecting (clone to avoid borrow issues)
        let schema = dataframe.schema().inner().clone();
        tracing::debug!("Query schema: {:?}", schema);

        // Collect the results into RecordBatches
        let batches = dataframe.collect().await.map_err(|e| {
            tracing::error!("Failed to collect query results. Schema: {:?}", schema);
            anyhow::anyhow!("Failed to collect query results: {}", e)
        })?;

        tracing::debug!("Collected {} batches", batches.len());
        for (i, batch) in batches.iter().enumerate() {
            tracing::debug!(
                "Batch {}: {} rows, schema: {:?}",
                i,
                batch.num_rows(),
                batch.schema()
            );
        }

        batches_to_single(schema, batches)
    }
}

/// Concatenate collected batches into a single RecordBatch, producing an
/// empty batch with the query's schema when there are no results.
fn batches_to_single(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<RecordBatch> {
    match batches.len() {
        0 => Ok(RecordBatch::new_empty(schema)),
        1 => Ok(batches
            .into_iter()
            .next()
            .expect("len == 1 guarantees first element")),
        _ => {
            let batch_schema = batches[0].schema();
            concat_batches(&batch_schema, &batches)
                .map_err(|e| anyhow::anyhow!("Failed to concatenate result batches: {}", e))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn engine_with_numbers() -> DataFusionEngine {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3, 4, 5]))],
        )
        .unwrap();
        ctx.register_batch("numbers", batch).unwrap();
        DataFusionEngine::new(ctx)
    }

    #[tokio::test]
    async fn execute_with_limit_truncates_to_fetch() {
        let engine = engine_with_numbers();
        let batch = engine
            .execute_with_limit("SELECT n FROM numbers ORDER BY n", 3)
            .await
            .unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[tokio::test]
    async fn execute_with_limit_returns_all_rows_when_under_limit() {
        let engine = engine_with_numbers();
        let batch = engine
            .execute_with_limit("SELECT n FROM numbers", 100)
            .await
            .unwrap();
        assert_eq!(batch.num_rows(), 5);
    }

    #[tokio::test]
    async fn execute_with_limit_empty_result_keeps_schema() {
        let engine = engine_with_numbers();
        let batch = engine
            .execute_with_limit("SELECT n FROM numbers WHERE n > 100", 10)
            .await
            .unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema().fields().len(), 1);
        assert_eq!(batch.schema().field(0).name(), "n");
    }
}
