use anyhow::{Context, Result};
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

/// How much of an upstream database to expose in DataFusion (single table vs whole catalog).
#[derive(Debug, Clone, Copy, Deserialize, Serialize, Hash, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum HierarchyLevel {
    /// One table under the default catalog.
    #[default]
    Table,
    /// Whole database as a named catalog (schemas + tables).
    Catalog,
}

impl HierarchyLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Catalog => "catalog",
        }
    }
}

/// Parse the comma-separated `allowed_schemas` option into a list of schema names.
///
/// Returns `None` if the option is absent or contains only whitespace/empty segments,
/// which signals that all schemas should be included.
pub fn parse_allowed_schemas(options: Option<&HashMap<String, String>>) -> Option<Vec<String>> {
    let value = options.and_then(|opts| opts.get("allowed_schemas"))?;
    let values = value
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

/// Assemble a DataFusion [`MemoryCatalogProvider`] from `(schema, table)` pairs, building each
/// [`TableProvider`] with the supplied async factory, and register it on `session_ctx` under
/// `catalog_name`.
///
/// The factory receives owned `(schema, table_name)` strings and returns a future that resolves
/// to the provider for that table. Errors from the factory are wrapped with context that includes
/// the catalog, schema, and table name.
pub async fn build_catalog<F, Fut>(
    session_ctx: &mut SessionContext,
    catalog_name: &str,
    schema_tables: Vec<(String, String)>,
    mut build_table: F,
) -> Result<()>
where
    F: FnMut(String, String) -> Fut,
    Fut: Future<Output = Result<Arc<dyn TableProvider>>>,
{
    let catalog_provider = Arc::new(MemoryCatalogProvider::new());

    for (schema, table_name) in schema_tables {
        let table_provider = build_table(schema.clone(), table_name.clone())
            .await
            .with_context(|| {
                format!(
                    "Failed to build table provider for '{}.{}' in catalog '{}'",
                    schema, table_name, catalog_name
                )
            })?;

        if catalog_provider.schema(&schema).is_none() {
            catalog_provider
                .register_schema(&schema, Arc::new(MemorySchemaProvider::new()))
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to register schema '{}' for catalog '{}': {}",
                        schema,
                        catalog_name,
                        e
                    )
                })?;
        }

        let schema_provider = catalog_provider.schema(&schema).ok_or_else(|| {
            anyhow::anyhow!(
                "Schema '{}' was not found after registration in catalog '{}'",
                schema,
                catalog_name
            )
        })?;

        schema_provider
            .register_table(table_name.clone(), table_provider)
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to register table '{}.{}' in catalog '{}': {}",
                    schema,
                    table_name,
                    catalog_name,
                    e
                )
            })?;

        tracing::debug!(
            "Prepared '{}.{}' in catalog '{}'",
            schema,
            table_name,
            catalog_name
        );
    }

    session_ctx.register_catalog(catalog_name, catalog_provider);
    Ok(())
}
