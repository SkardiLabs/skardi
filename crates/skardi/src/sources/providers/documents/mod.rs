//! `documents` data source connector.
//!
//! Turns a directory / object-store prefix of files (PDF, Office, ODF, images)
//! into queryable `(file, page)` rows via the pure-Rust `liteparse` crate —
//! except Markdown and plain text, which are content already and are read
//! directly by the `text` module, never handed to the parser.
//! Everything here is behind the `documents` Cargo feature.

// `pub(crate)` rather than private: `llm_extract`'s image fetch reuses
// [`blob::BlobStore`] to read `s3://` `image_ref`s, so that S3 client
// construction and the env-only credential contract live in exactly one place.
pub(crate) mod blob;
mod parse;
mod table;
mod text;

/// Hostile object names shared by the object-store location tests: each one
/// must list and read back at its exact, raw key. Every entry is accepted by
/// `object_store::path::Path::parse` verbatim (it rejects only `/`, ASCII
/// control characters, and `.`/`..` segments); the tests assert that rather
/// than assume it.
#[cfg(test)]
pub(crate) const HOSTILE_OBJECT_NAMES: &[&str] = &[
    "团体.PDF",
    "my report.pdf",
    "issue#12.pdf",
    "50% off.pdf",
    // Looks percent-encoded but is a literal raw name.
    "%E5%9B%A2.pdf",
    "what?.pdf",
    "[draft] v2.pdf",
    "📄 notes 🎉.pdf",
    // Same visible name, NFC then NFD: two distinct keys, never normalised.
    "caf\u{e9}.pdf",
    "cafe\u{301}.pdf",
    // Exactly 255 bytes (83 x 3-byte CJK + "xx.pdf").
    "团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团团xx.pdf",
];

#[cfg(test)]
const _: () = assert!(HOSTILE_OBJECT_NAMES[HOSTILE_OBJECT_NAMES.len() - 1].len() == 255);

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
