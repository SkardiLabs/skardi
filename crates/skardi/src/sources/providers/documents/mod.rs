//! `documents` data source connector.
//!
//! Turns a directory / object-store prefix of files (PDF, Office, ODF, images)
//! into queryable `(file, page)` rows via the pure-Rust `liteparse` crate.
//! Everything here is behind the `documents` Cargo feature.

mod parse;
mod table;

pub use parse::{ImageMode, OcrMode, ParseOptions, ParsedPage, parse_source, preflight};
pub use table::DocumentsTable;
