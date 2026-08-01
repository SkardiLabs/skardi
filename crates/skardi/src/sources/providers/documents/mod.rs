//! `documents` data source connector.
//!
//! Turns a directory / object-store prefix of files (PDF, Office, ODF, images)
//! into queryable `(file, page)` rows via the pure-Rust `liteparse` crate.
//! Everything here is behind the `documents` Cargo feature.

// `pub(crate)` rather than private: `llm_extract`'s image fetch reuses
// [`blob::BlobStore`] to read `s3://` `image_ref`s, so that S3 client
// construction and the env-only credential contract live in exactly one place.
pub(crate) mod blob;
mod parse;
mod table;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use datafusion::prelude::SessionContext;

pub use parse::{ImageMode, OcrMode, ParseOptions, ParsedPage, parse_source, preflight};
pub use table::DocumentsTable;

/// Register a `documents` source as a DataFusion table.
///
/// Parses `options` into [`ParseOptions`], runs [`preflight`] so missing external
/// tools surface here (at registration) rather than mid-scan, then registers a
/// [`DocumentsTable`] over `path` under `name`.
pub async fn register_documents_tables(
    session_ctx: &mut SessionContext,
    name: &str,
    path: &str,
    options: Option<&HashMap<String, String>>,
) -> Result<()> {
    tracing::info!("Registering documents source: {} at {}", name, path);

    let opts = ParseOptions::from_map(options);
    preflight(&opts)?;

    session_ctx
        .register_table(name, Arc::new(DocumentsTable::new(path.to_string(), opts)))
        .map_err(|e| anyhow::anyhow!("Failed to register documents table '{}': {}", name, e))?;

    tracing::info!("Successfully registered documents source: {}", name);
    Ok(())
}
