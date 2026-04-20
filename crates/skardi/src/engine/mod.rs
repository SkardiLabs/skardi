//! Engine abstraction layer for query execution

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

pub mod datafusion;

/// Core trait for query execution engines
///
/// The `Engine` trait provides a unified interface for executing SQL queries
/// and returning results as Arrow RecordBatch objects. This abstraction allows
/// the pipeline system to work with different execution engines while maintaining
/// a consistent interface.
#[async_trait]
pub trait Engine: Send + Sync {
    /// Execute a SQL query and return the result as a RecordBatch
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL query string to execute
    ///
    /// # Returns
    ///
    /// Returns a `Result<RecordBatch>` containing the query results or an error
    /// if the query execution fails.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use skardi::engine::Engine;
    ///
    /// async fn example_usage(engine: &dyn Engine) -> anyhow::Result<()> {
    ///     let sql = "SELECT * FROM my_table WHERE id = 1";
    ///     let result = engine.execute(sql).await?;
    ///     println!("Query returned {} rows", result.num_rows());
    ///     Ok(())
    /// }
    /// ```
    async fn execute(&self, sql: &str) -> Result<RecordBatch>;
}
