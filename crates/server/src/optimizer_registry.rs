//! Registry for data-source-specific functions and dataset management
//!
//! This module provides a centralized way to:
//! 1. Conditionally register table functions based on data sources
//! 2. Store and provide access to datasets needed by those functions
//!
//! For Lance tables:
//! - Datasets are stored here when tables are registered
//! - The lance_knn table function uses this registry to look up datasets

use anyhow::Result;
use datafusion::prelude::SessionContext;
use lance::dataset::Dataset;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use crate::config::{DataSource, DataSourceType};

/// Registry for data source metadata and functions
///
/// This struct serves as:
/// 1. **Function Manager** - Conditionally registers table functions based on data sources
/// 2. **Dataset Store** - Maintains references to datasets needed by table functions
/// 3. **Lifecycle Coordinator** - Ensures datasets are available when functions need them
pub struct OptimizerRegistry {
    /// Lance datasets indexed by table name
    /// Used by lance_knn table function to access datasets
    lance_datasets: Arc<RwLock<HashMap<String, Arc<Dataset>>>>,
}

impl OptimizerRegistry {
    /// Create a new registry
    pub fn new() -> Self {
        Self {
            lance_datasets: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get all physical optimizer rules that should be registered based on data sources
    ///
    /// Currently returns an empty list as we use table functions instead of optimizers
    /// for Lance KNN search. Kept for future extensibility.
    pub fn get_physical_optimizer_rules(
        &self,
        data_sources: &[DataSource],
    ) -> Vec<Arc<dyn datafusion::physical_optimizer::PhysicalOptimizerRule + Send + Sync>> {
        let source_types: HashSet<_> = data_sources
            .iter()
            .map(|ds| ds.source_type.clone())
            .collect();

        tracing::debug!(
            "Checking physical optimizers for data source types: {:?}",
            source_types
        );

        // Currently no custom physical optimizers needed
        // Lance KNN uses explicit table function instead
        Vec::new()
    }

    /// Register all applicable UDFs and table functions based on data sources
    pub fn register_udfs(
        &self,
        ctx: &mut SessionContext,
        data_sources: &[DataSource],
    ) -> Result<()> {
        let source_types: HashSet<_> = data_sources
            .iter()
            .map(|ds| ds.source_type.clone())
            .collect();

        tracing::info!(
            "Registering functions for data source types: {:?}",
            source_types
        );

        // Register Lance-specific table functions
        if source_types.contains(&DataSourceType::Lance) {
            self.register_lance_functions(ctx)?;
        }

        // Future: Register Postgres-specific UDFs
        if source_types.contains(&DataSourceType::Postgres) {
            self.register_postgres_udfs(ctx)?;
        }

        Ok(())
    }

    /// Register Lance-specific table functions
    ///
    /// Registers the lance_knn table function for explicit KNN search
    fn register_lance_functions(&self, ctx: &mut SessionContext) -> Result<()> {
        tracing::info!("Registering Lance table functions");

        // Register lance_knn table function
        source::lance::knn_table_function::register_lance_knn_udtf(ctx, self.lance_datasets());

        tracing::info!("✓ Registered lance_knn table function");
        Ok(())
    }

    /// Register Postgres-specific UDFs (placeholder for future)
    fn register_postgres_udfs(&self, _ctx: &mut SessionContext) -> Result<()> {
        tracing::debug!("Postgres UDFs not yet implemented");
        Ok(())
    }

    // === Lance Dataset Management ===

    /// Store a Lance dataset in the registry
    ///
    /// Called when registering a Lance table with DataFusion.
    /// The lance_knn table function uses this to look up datasets by name.
    pub fn register_lance_dataset(&self, table_name: &str, dataset: Arc<Dataset>) {
        let mut datasets = self.lance_datasets.write().unwrap();
        datasets.insert(table_name.to_string(), dataset);
        tracing::debug!("Registered Lance dataset '{}' in registry", table_name);
    }

    /// Get a Lance dataset by table name
    pub fn get_lance_dataset(&self, table_name: &str) -> Option<Arc<Dataset>> {
        let datasets = self.lance_datasets.read().unwrap();
        datasets.get(table_name).cloned()
    }

    /// Get a clone of the Lance datasets map
    ///
    /// Returns an Arc<RwLock<>> that can be shared with table functions
    pub fn lance_datasets(&self) -> Arc<RwLock<HashMap<String, Arc<Dataset>>>> {
        Arc::clone(&self.lance_datasets)
    }
}

impl Default for OptimizerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_creation() {
        let registry = OptimizerRegistry::new();
        assert!(registry.get_lance_dataset("nonexistent").is_none());
    }

    #[test]
    fn test_registry_default() {
        let registry = OptimizerRegistry::default();
        assert!(registry.get_lance_dataset("test").is_none());
    }

    #[test]
    fn test_lance_datasets_accessor() {
        let registry = OptimizerRegistry::new();
        let datasets = registry.lance_datasets();

        let datasets_read = datasets.read().unwrap();
        assert_eq!(datasets_read.len(), 0);
    }

    #[tokio::test]
    async fn test_register_udfs_with_empty_sources() {
        let registry = OptimizerRegistry::new();
        let mut ctx = SessionContext::new();
        let data_sources = vec![];

        let result = registry.register_udfs(&mut ctx, &data_sources);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_register_udfs_with_csv_only() {
        use crate::config::DataSource;
        use std::path::PathBuf;

        let registry = OptimizerRegistry::new();
        let mut ctx = SessionContext::new();
        let data_sources = vec![DataSource {
            name: "test_csv".to_string(),
            source_type: DataSourceType::Csv,
            path: PathBuf::from("test.csv"),
            connection_string: None,
            schema: None,
            options: None,
            access_mode: crate::config::AccessMode::default(),
            enable_cache: false,
        }];

        // Should not register Lance functions for CSV-only data sources
        let result = registry.register_udfs(&mut ctx, &data_sources);
        assert!(result.is_ok());
    }
}
