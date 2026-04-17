//! DataFusion implementation of the Engine trait
//!
//! This module contains the DataFusion-specific implementation of the `Engine` trait,
//! providing SQL query execution capabilities using the DataFusion query engine.

use super::Engine;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::prelude::*;
use datafusion_common::ScalarValue;
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
}

impl DataFusionEngine {
    /// Collect a DataFrame into a single RecordBatch
    fn collect_batches(
        schema: arrow::datatypes::SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<RecordBatch> {
        tracing::debug!("Collected {} batches", batches.len());
        for (i, batch) in batches.iter().enumerate() {
            tracing::debug!(
                "Batch {}: {} rows, schema: {:?}",
                i,
                batch.num_rows(),
                batch.schema()
            );
        }

        match batches.len() {
            0 => Ok(RecordBatch::new_empty(schema)),
            1 => Ok(batches
                .into_iter()
                .next()
                .expect("len == 1 guarantees first element")),
            _ => {
                use arrow::compute::concat_batches;
                let batch_schema = batches[0].schema();
                let concatenated = concat_batches(&batch_schema, &batches)
                    .map_err(|e| anyhow::anyhow!("Failed to concatenate result batches: {}", e))?;
                Ok(concatenated)
            }
        }
    }
}

#[async_trait]
impl Engine for DataFusionEngine {
    async fn execute(&self, sql: &str) -> Result<RecordBatch> {
        let dataframe = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute SQL query: {}", e))?;

        let schema = dataframe.schema().inner().clone();
        tracing::debug!("Query schema: {:?}", schema);

        let batches = dataframe.collect().await.map_err(|e| {
            tracing::error!("Failed to collect query results. Schema: {:?}", schema);
            anyhow::anyhow!("Failed to collect query results: {}", e)
        })?;

        Self::collect_batches(schema, batches)
    }

    async fn execute_with_params(
        &self,
        sql: &str,
        params: Vec<(String, ScalarValue)>,
    ) -> Result<RecordBatch> {
        let dataframe = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute SQL query: {}", e))?;

        let schema = dataframe.schema().inner().clone();
        tracing::debug!("Query schema: {:?}", schema);

        let dataframe = dataframe
            .with_param_values(params)
            .map_err(|e| anyhow::anyhow!("Failed to bind parameter values: {}", e))?;

        let batches = dataframe.collect().await.map_err(|e| {
            tracing::error!("Failed to collect query results. Schema: {:?}", schema);
            anyhow::anyhow!("Failed to collect query results: {}", e)
        })?;

        Self::collect_batches(schema, batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_engine_with_users() -> DataFusionEngine {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
            ],
        )
        .unwrap();
        ctx.register_batch("users", batch).unwrap();
        DataFusionEngine::new(ctx)
    }

    #[tokio::test]
    async fn execute_with_params_binds_named_parameters() {
        let engine = make_engine_with_users();
        let sql = "SELECT name FROM users WHERE id = $user_id";
        let params = vec![("user_id".to_string(), ScalarValue::Int64(Some(2)))];

        let batch = engine.execute_with_params(sql, params).await.unwrap();
        assert_eq!(batch.num_rows(), 1);
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "bob");
    }

    #[tokio::test]
    async fn execute_with_params_resists_prefix_collision() {
        // Regression guard: $user and $user_id must each bind independently,
        // no matter which order the caller passes them in.
        let engine = make_engine_with_users();
        let sql = "SELECT id FROM users WHERE id = $user_id AND name = $user";

        // Pass $user first — this would have corrupted the SQL under the old
        // str::replace approach (replacing {user} inside {user_id}).
        let params = vec![
            (
                "user".to_string(),
                ScalarValue::Utf8(Some("bob".to_string())),
            ),
            ("user_id".to_string(), ScalarValue::Int64(Some(2))),
        ];

        let batch = engine.execute_with_params(sql, params).await.unwrap();
        assert_eq!(batch.num_rows(), 1);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 2);
    }

    #[tokio::test]
    async fn execute_with_params_escapes_sql_injection() {
        // A value containing a closing quote + OR 1=1 must be treated as a
        // literal string, not executed as SQL.
        let engine = make_engine_with_users();
        let sql = "SELECT id FROM users WHERE name = $name";
        let params = vec![(
            "name".to_string(),
            ScalarValue::Utf8(Some("alice' OR '1'='1".to_string())),
        )];

        let batch = engine.execute_with_params(sql, params).await.unwrap();
        // No row matches the literal injection string → empty result
        assert_eq!(batch.num_rows(), 0);
    }
}
