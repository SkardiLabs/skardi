use crate::sources::DataSourceType;
use anyhow::{Context, Result};
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use futures::stream::{self, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

/// Upper bound on how many [`TableProvider`] constructions run concurrently inside
/// [`build_catalog`]. Catalogs with many tables amortize network latency across this many
/// in-flight information_schema / schema-introspection round-trips.
const CATALOG_BUILD_CONCURRENCY: usize = 8;

/// Typed identifier for a source being operated on, used by [`retry_with_timeout`] and
/// other catalog-path helpers for tracing and error messages.
///
/// Rendered as `"<kind> <hierarchy> '<name>'"` — for example `postgres catalog 'mydb'`
/// or `mysql table 'users'`. Taking typed [`DataSourceType`] and [`HierarchyLevel`] values
/// (rather than free-form `&str`) makes it impossible to accidentally pass a connection
/// string or other untyped identifier into tracing output.
///
/// `name` is the DataFusion source name in single-table mode, or the catalog name in
/// catalog mode.
#[derive(Debug, Clone, Copy)]
pub struct SourceLabel<'a> {
    pub kind: DataSourceType,
    pub hierarchy: HierarchyLevel,
    pub name: &'a str,
}

impl<'a> SourceLabel<'a> {
    pub fn new(kind: DataSourceType, hierarchy: HierarchyLevel, name: &'a str) -> Self {
        Self {
            kind,
            hierarchy,
            name,
        }
    }
}

impl<'a> fmt::Display for SourceLabel<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} '{}'",
            self.kind,
            self.hierarchy.as_str(),
            self.name
        )
    }
}

/// Default per-attempt timeout for connection and introspection operations that
/// go through [`retry_with_timeout`].
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum attempts (including the first) for [`retry_with_timeout`].
pub const MAX_RETRIES: u32 = 3;

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

/// Run an async operation with a per-attempt timeout and up to [`MAX_RETRIES`] attempts.
///
/// `label` identifies the source being operated on (used in warn-level tracing and the
/// terminal timeout error). `op_name` is a short description of the action being retried
/// (e.g. `"pool creation"`, `"information_schema introspection"`). The operation factory
/// `op` is invoked once per attempt; it should be idempotent.
///
/// Returns the first successful value. On exhaustion, returns the last error (or a
/// synthesized timeout error if the final attempt timed out).
pub async fn retry_with_timeout<T, F, Fut>(
    label: SourceLabel<'_>,
    op_name: &str,
    mut op: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 1..=MAX_RETRIES {
        match timeout(CONNECT_TIMEOUT, op()).await {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(e)) => {
                tracing::warn!(
                    "{}: {} attempt {}/{} failed: {}",
                    label,
                    op_name,
                    attempt,
                    MAX_RETRIES,
                    e
                );
                last_err = Some(e);
            }
            Err(_) => {
                let e = anyhow::anyhow!(
                    "Timed out after {}s during {} for {}. \
                     Check that the upstream is reachable and credentials are correct.",
                    CONNECT_TIMEOUT.as_secs(),
                    op_name,
                    label
                );
                tracing::warn!(
                    "{}: {} attempt {}/{} timed out",
                    label,
                    op_name,
                    attempt,
                    MAX_RETRIES
                );
                last_err = Some(e);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("retry_with_timeout: no attempts were made")))
}

/// Assemble a DataFusion [`MemoryCatalogProvider`] from `(schema, table)` pairs, building each
/// [`TableProvider`] with the supplied async factory, and register it on `session_ctx` under
/// `catalog_name`.
///
/// The factory receives owned `(schema, table_name)` strings and returns a future that resolves
/// to the provider for that table. The factory is invoked once per table in the input order,
/// and up to `CATALOG_BUILD_CONCURRENCY` of the resulting futures are driven in parallel via
/// [`StreamExt::buffer_unordered`]. The first error short-circuits the whole call.
///
/// Providers are registered into the [`MemoryCatalogProvider`] in lexicographic
/// `(schema, table)` order so log output and downstream iteration are deterministic regardless
/// of which future completes first.
pub async fn build_catalog<F, Fut>(
    session_ctx: &SessionContext,
    catalog_name: &str,
    schema_tables: Vec<(String, String)>,
    mut build_table: F,
) -> Result<()>
where
    F: FnMut(String, String) -> Fut,
    Fut: Future<Output = Result<Arc<dyn TableProvider>>>,
{
    let catalog_provider = Arc::new(MemoryCatalogProvider::new());

    // Kick off provider construction concurrently. `build_table` (FnMut) is called eagerly
    // and sequentially in the map — the closures it returns are what run in parallel.
    let provider_futures: Vec<_> = schema_tables
        .into_iter()
        .map(|(schema, table_name)| {
            let fut = build_table(schema.clone(), table_name.clone());
            let catalog_name = catalog_name.to_string();
            async move {
                let provider = fut.await.with_context(|| {
                    format!(
                        "Failed to build table provider for '{}.{}' in catalog '{}'",
                        schema, table_name, catalog_name
                    )
                })?;
                Ok::<_, anyhow::Error>((schema, table_name, provider))
            }
        })
        .collect();

    let mut prepared: Vec<(String, String, Arc<dyn TableProvider>)> =
        stream::iter(provider_futures)
            .buffer_unordered(CATALOG_BUILD_CONCURRENCY)
            .try_collect()
            .await?;

    // Deterministic registration order for log output and downstream iteration.
    prepared.sort_by(|a, b| (a.0.as_str(), a.1.as_str()).cmp(&(b.0.as_str(), b.1.as_str())));

    for (schema, table_name, table_provider) in prepared {
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
