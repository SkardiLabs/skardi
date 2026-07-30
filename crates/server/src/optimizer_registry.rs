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
use skardi::sources::providers::lance::{register_lance_fts_udtf, register_lance_knn_udtf};
use skardi::sources::providers::mongo::fts_table_function::register_mongo_fts_udtf;
use skardi::sources::providers::open_connector::{
    OpenConnectorGateways, register_open_connector_udtfs,
};
use skardi::sources::providers::seekdb::{register_seekdb_fts_udtf, register_seekdb_knn_udtf};
use skardi::sources::providers::sqlite::{
    register_sqlite_fts_udtf, register_sqlite_knn_udtf, register_vec_to_binary_udf,
};
use skardi::sources::providers::sqlx::{register_pg_fts_udtf, register_pg_knn_udtf};
use skardi::sources::providers::{DatasetEntry, DatasetRegistry};
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
    /// Unified dataset registry indexed by table name.
    /// Stores both Lance datasets and Postgres entries, used by
    /// lance_knn, lance_fts, pg_knn, and pg_fts table functions.
    dataset_registry: DatasetRegistry,
    /// Open Connector gateway state indexed by gateway (data source) name,
    /// filled during data-source registration and used by the
    /// open_connector_query / open_connector_scan table functions.
    open_connector_gateways: OpenConnectorGateways,
}

impl OptimizerRegistry {
    /// Create a new registry
    pub fn new() -> Self {
        Self {
            dataset_registry: Arc::new(RwLock::new(HashMap::new())),
            open_connector_gateways: OpenConnectorGateways::default(),
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

        // Register Postgres-specific table functions
        if source_types.contains(&DataSourceType::Postgres) {
            self.register_postgres_functions(ctx)?;
        }

        // Register MongoDB-specific table functions
        if source_types.contains(&DataSourceType::Mongo) {
            self.register_mongo_functions(ctx)?;
        }

        // Register SQLite-specific table functions
        if source_types.contains(&DataSourceType::Sqlite) {
            self.register_sqlite_functions(ctx)?;
        }

        // Register SeekDB-specific table functions
        if source_types.contains(&DataSourceType::Seekdb) {
            self.register_seekdb_functions(ctx)?;
        }

        // Register Open Connector table functions
        if source_types.contains(&DataSourceType::OpenConnector) {
            tracing::info!("Registering Open Connector table functions");
            register_open_connector_udtfs(ctx, self.open_connector_gateways());
            tracing::info!("✓ Registered open_connector_query and open_connector_scan");
        }

        Ok(())
    }

    /// Register Lance-specific table functions
    ///
    /// Registers the lance_knn and lance_fts table functions
    fn register_lance_functions(&self, ctx: &mut SessionContext) -> Result<()> {
        tracing::info!("Registering Lance table functions");

        register_lance_knn_udtf(ctx, self.datasets());
        tracing::info!("✓ Registered lance_knn table function");

        register_lance_fts_udtf(ctx, self.datasets());
        tracing::info!("✓ Registered lance_fts table function");

        Ok(())
    }

    /// Register Postgres-specific table functions.
    fn register_postgres_functions(&self, ctx: &mut SessionContext) -> Result<()> {
        tracing::info!("Registering pg_knn table function");
        register_pg_knn_udtf(ctx, self.datasets());
        tracing::info!("✓ Registered pg_knn table function");

        tracing::info!("Registering pg_fts table function");
        register_pg_fts_udtf(ctx, self.datasets());
        tracing::info!("✓ Registered pg_fts table function");
        Ok(())
    }

    /// Register MongoDB-specific table functions.
    fn register_mongo_functions(&self, ctx: &mut SessionContext) -> Result<()> {
        tracing::info!("Registering mongo_fts table function");
        register_mongo_fts_udtf(ctx, self.datasets());
        tracing::info!("✓ Registered mongo_fts table function");
        Ok(())
    }

    /// Register SQLite-specific table functions.
    fn register_sqlite_functions(&self, ctx: &mut SessionContext) -> Result<()> {
        tracing::info!("Registering sqlite_knn table function");
        register_sqlite_knn_udtf(ctx, self.datasets());
        tracing::info!("✓ Registered sqlite_knn table function");

        tracing::info!("Registering sqlite_fts table function");
        register_sqlite_fts_udtf(ctx, self.datasets());
        tracing::info!("✓ Registered sqlite_fts table function");

        tracing::info!("Registering vec_to_binary scalar UDF");
        register_vec_to_binary_udf(ctx);
        tracing::info!("✓ Registered vec_to_binary scalar UDF");
        Ok(())
    }

    /// Register SeekDB-specific table functions.
    fn register_seekdb_functions(&self, ctx: &mut SessionContext) -> Result<()> {
        tracing::info!("Registering seekdb_knn table function");
        register_seekdb_knn_udtf(ctx, self.datasets());
        tracing::info!("✓ Registered seekdb_knn table function");

        tracing::info!("Registering seekdb_fts table function");
        register_seekdb_fts_udtf(ctx, self.datasets());
        tracing::info!("✓ Registered seekdb_fts table function");
        Ok(())
    }

    // === Dataset Management ===

    /// Store a Lance dataset in the registry
    pub fn register_lance_dataset(&self, table_name: &str, dataset: Arc<Dataset>) {
        let mut reg = self
            .dataset_registry
            .write()
            .unwrap_or_else(|p| p.into_inner());
        reg.insert(table_name.to_string(), DatasetEntry::Lance(dataset));
        tracing::debug!("Registered Lance dataset '{}' in registry", table_name);
    }

    /// Get a Lance dataset by table name
    pub fn get_lance_dataset(&self, table_name: &str) -> Option<Arc<Dataset>> {
        let reg = self
            .dataset_registry
            .read()
            .unwrap_or_else(|p| p.into_inner());
        reg.get(table_name).and_then(|e| match e {
            DatasetEntry::Lance(ds) => Some(Arc::clone(ds)),
            _ => None,
        })
    }

    /// Get a clone of the unified dataset registry to share with table functions.
    pub fn datasets(&self) -> DatasetRegistry {
        Arc::clone(&self.dataset_registry)
    }

    /// Get a clone of the Open Connector gateway map to share with
    /// `register_open_connector_tables` and the Open Connector UDTFs.
    pub fn open_connector_gateways(&self) -> OpenConnectorGateways {
        Arc::clone(&self.open_connector_gateways)
    }

    /// Alias for `datasets()` — used when passing to `register_postgres_tables`.
    pub fn pg_knn_pools(&self) -> DatasetRegistry {
        self.datasets()
    }

    /// Alias for `datasets()` — used when passing to `register_lance_table`.
    pub fn lance_datasets(&self) -> DatasetRegistry {
        self.datasets()
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
    async fn test_register_udfs_with_open_connector_source() {
        use crate::config::DataSource;
        use std::path::PathBuf;

        let registry = OptimizerRegistry::new();
        let mut ctx = SessionContext::new();
        let data_sources = vec![DataSource {
            name: "saas".to_string(),
            source_type: DataSourceType::OpenConnector,
            path: PathBuf::new(),
            connection_string: Some("http://open-connector:3000".to_string()),
            schema: None,
            options: None,
            access_mode: crate::config::AccessMode::default(),
            enable_cache: false,
            hierarchy_level: Default::default(),
            description: None,
            open_connector: None,
            rss: None,
        }];

        registry
            .register_udfs(&mut ctx, &data_sources)
            .expect("register open connector UDTFs");

        // The functions are registered; with no gateway handle published
        // (this test never ran data-source registration) planning fails with
        // the targeted gateway error rather than "function not found".
        let err = ctx
            .sql("SELECT * FROM open_connector_query('saas', 'mock.items', '{}')")
            .await
            .expect_err("no gateway state registered");
        assert!(
            err.to_string().contains("gateway 'saas' is not registered"),
            "unexpected error: {err}"
        );
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
            hierarchy_level: Default::default(),
            description: None,
            open_connector: None,
            rss: None,
        }];

        // Should not register Lance functions for CSV-only data sources
        let result = registry.register_udfs(&mut ctx, &data_sources);
        assert!(result.is_ok());
    }
}
