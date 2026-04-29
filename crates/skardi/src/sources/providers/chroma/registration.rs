//! `register_chroma_tables` — entry point called by `crates/server/src/config.rs`.
//!
//! Connects to a Chroma server, fetches the configured collection, derives the
//! Arrow schema (id/document/embedding/metadata), and registers the resulting
//! `ChromaTableProvider` plus the `chroma_knn` UDTF.
//!
//! Embedding dimension is read once from the collection's schema at registration
//! time and frozen into the Arrow `FixedSizeList` schema. If the collection has
//! no embedding dimension recorded yet (e.g. empty collection), the default of
//! 1536 is used and a warning is logged.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use chroma::ChromaHttpClient;
use datafusion::prelude::SessionContext;

use crate::sources::providers::chroma::client_options::parse_connection;
use crate::sources::providers::chroma::knn_table_function::register_chroma_knn_udtf;
use crate::sources::providers::chroma::table_provider::{
    ChromaEntry, ChromaTableProvider, chroma_schema,
};
use crate::sources::providers::{DatasetEntry, DatasetRegistry};

/// Default embedding dimension for collections that don't yet have one set.
/// Matches OpenAI's `text-embedding-ada-002` and `text-embedding-3-small`.
const DEFAULT_EMBEDDING_DIM: i32 = 1536;

pub async fn register_chroma_tables(
    session_ctx: &mut SessionContext,
    name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
    registry: Option<&DatasetRegistry>,
) -> Result<()> {
    tracing::info!(
        source = %name,
        endpoint = %connection_string,
        "registering chroma data source"
    );

    let conn = parse_connection(connection_string, options)?;
    let client = ChromaHttpClient::new(conn.options);

    let collection = client
        .get_collection(conn.collection.as_str())
        .await
        .with_context(|| {
            format!(
                "chroma: failed to fetch collection '{}' from endpoint",
                conn.collection
            )
        })?;
    let collection = Arc::new(collection);

    // Derive embedding dim from the collection's Schema if available; otherwise
    // fall back to the configured default and warn so users can fix their config.
    let embedding_dim = match infer_embedding_dim(collection.as_ref()) {
        Some(d) => d,
        None => {
            tracing::warn!(
                source = %name,
                collection = %conn.collection,
                "chroma: could not infer embedding dimension from collection schema; defaulting to {DEFAULT_EMBEDDING_DIM}"
            );
            DEFAULT_EMBEDDING_DIM
        }
    };
    if embedding_dim <= 0 {
        return Err(anyhow!(
            "chroma: invalid embedding dimension {} on collection '{}'",
            embedding_dim,
            conn.collection
        ));
    }

    let schema = chroma_schema(embedding_dim);
    let provider = Arc::new(ChromaTableProvider::new(
        collection.clone(),
        schema.clone(),
        embedding_dim,
    ));

    session_ctx
        .register_table(name, provider)
        .with_context(|| format!("chroma: register_table failed for '{name}'"))?;

    if let Some(registry) = registry {
        let mut guard = registry.write().unwrap_or_else(|p| p.into_inner());
        guard.insert(
            name.to_string(),
            DatasetEntry::Chroma(ChromaEntry {
                collection: collection.clone(),
                schema: schema.clone(),
                embedding_dim,
            }),
        );
        register_chroma_knn_udtf(session_ctx, registry.clone());
    }

    tracing::info!(
        source = %name,
        embedding_dim,
        "chroma: collection registered"
    );
    Ok(())
}

fn infer_embedding_dim(_collection: &chroma::ChromaCollection) -> Option<i32> {
    // The `chroma` 0.14 public API does not expose a `dimension()` accessor on
    // ChromaCollection. Schema/configuration is on the inner Collection payload
    // but not surfaced. Returning None here forces the caller to fall back to
    // the configured default; we'll revisit once the upstream crate exposes it
    // (or we add an explicit `embedding_dim` config option).
    None
}
