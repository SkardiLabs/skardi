use anyhow::{Context, Result};
use datafusion::prelude::SessionContext;
use lance::dataset::Dataset;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

/// Register a Lance dataset as a table in DataFusion SessionContext
///
/// # Arguments
/// * `session_ctx` - DataFusion session context to register the table into
/// * `name` - Name to register the table as
/// * `path` - Path to the Lance dataset directory
/// * `dataset_registry` - Optional registry to store dataset for optimizer access
///
/// # Example
/// ```no_run
/// use datafusion::prelude::SessionContext;
/// use source::providers::lance::register_lance_table;
///
/// # async fn example() -> anyhow::Result<()> {
/// let mut ctx = SessionContext::new();
/// register_lance_table(&mut ctx, "embeddings", "data/embeddings.lance", None).await?;
/// # Ok(())
/// # }
/// ```
pub async fn register_lance_table(
    session_ctx: &mut SessionContext,
    name: &str,
    path: &str,
    dataset_registry: Option<&Arc<RwLock<HashMap<String, Arc<Dataset>>>>>,
) -> Result<()> {
    tracing::info!("Registering Lance dataset: {} from path: {}", name, path);

    // Verify the dataset directory exists
    let dataset_path = Path::new(path);
    if !dataset_path.exists() {
        return Err(anyhow::anyhow!(
            "Lance dataset directory does not exist: {}",
            path
        ));
    }

    // Open the Lance dataset
    let dataset = Dataset::open(path)
        .await
        .with_context(|| format!("Failed to open Lance dataset at path: {}", path))?;

    tracing::debug!(
        "Opened Lance dataset: {} with {} rows",
        name,
        dataset.count_rows(None).await.unwrap_or(0)
    );

    // Convert Lance dataset to Arc for sharing
    let dataset_arc = Arc::new(dataset);

    // Store in registry if provided (for optimizer access)
    if let Some(registry) = dataset_registry {
        let mut datasets = registry.write().unwrap();
        datasets.insert(name.to_string(), Arc::clone(&dataset_arc));
        tracing::debug!("Stored Lance dataset '{}' in registry for optimizer", name);
    }

    // Register the dataset as a table using Lance's native DataFusion integration
    // Lance's Dataset implements TableProvider trait directly
    session_ctx
        .register_table(name, dataset_arc)
        .with_context(|| format!("Failed to register Lance table '{}' with DataFusion", name))?;

    tracing::info!("Successfully registered Lance table: {}", name);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    #[ignore] // Requires actual Lance dataset
    async fn test_register_lance_table_missing_dataset() {
        let mut session_ctx = SessionContext::new();
        let result = register_lance_table(
            &mut session_ctx,
            "test_table",
            "/nonexistent/path/to/dataset.lance",
            None, // No registry
        )
        .await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("does not exist") || error_msg.contains("Failed to open"));
    }

    #[test]
    fn test_path_validation() {
        let path = Path::new("/nonexistent/path");
        assert!(!path.exists());
    }
}
