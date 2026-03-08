//! DataFusion implementation of the Engine trait
//!
//! This module contains the DataFusion-specific implementation of the `Engine` trait,
//! providing SQL query execution capabilities using the DataFusion query engine.

use crate::Engine;
use anyhow::Result;
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
    /// use skardi_engine::datafusion::DataFusionEngine;
    ///
    /// let ctx = SessionContext::new();
    /// // Register tables, configure settings, etc.
    /// // ctx.register_csv("my_table", "data/file.csv", CsvReadOptions::new()).await?;
    /// let engine = DataFusionEngine::new(ctx);
    /// ```
    pub fn new(ctx: SessionContext) -> Self {
        Self { ctx: Arc::new(ctx) }
    }

    /// Create a new DataFusion engine instance with an Arc<SessionContext>
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

    /// Get the Arc<SessionContext> for sharing
    ///
    /// This method allows cloning the Arc to share the SessionContext with other components.
    ///
    /// # Returns
    ///
    /// Returns a clone of the Arc<SessionContext>
    pub fn session_context_arc(&self) -> Arc<SessionContext> {
        self.ctx.clone()
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

        // Handle the result based on the number of batches returned
        match batches.len() {
            0 => {
                // No results - create an empty RecordBatch with the query's schema
                let empty_batch = RecordBatch::new_empty(schema);
                Ok(empty_batch)
            }
            1 => {
                // Single batch - return it directly
                Ok(batches.into_iter().next().unwrap())
            }
            _ => {
                // Multiple batches - concatenate them into a single RecordBatch
                use arrow::compute::concat_batches;
                let batch_schema = batches[0].schema();
                let concatenated = concat_batches(&batch_schema, &batches)
                    .map_err(|e| anyhow::anyhow!("Failed to concatenate result batches: {}", e))?;
                Ok(concatenated)
            }
        }
    }
}
