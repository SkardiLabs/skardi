//! Chroma vector-database provider.
//!
//! Wires a Chroma collection into DataFusion as a `TableProvider`. Each Skardi
//! `DataSource` of type `chroma` corresponds to one Chroma collection, with the
//! fixed Arrow schema:
//!
//! ```text
//! id:        Utf8 (non-null)
//! document:  Utf8 (nullable)
//! embedding: FixedSizeList<Float32, dim>
//! metadata:  Map<Utf8, Utf8>           // JSON-stringified values; cast in SQL
//! ```
//!
//! KNN queries are exposed via the `chroma_knn(table, embedding_col, vec, k)`
//! UDTF, which returns `id, document, metadata, _distance`. Filter pushdown
//! produces typed `chroma::types::Where` expressions that route metadata
//! predicates and document `LIKE '%x%'` predicates to Chroma in a single call.

pub mod arrow_conv;
pub mod client_options;
pub mod filter;
pub mod knn_exec;
pub mod knn_table_function;
pub mod registration;
pub mod scan_exec;
pub mod table_provider;
pub mod writes;

pub use registration::register_chroma_tables;
pub use table_provider::{ChromaEntry, ChromaTableProvider};
