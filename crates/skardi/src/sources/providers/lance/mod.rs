//! Lance integration module
//!
//! Provides:
//! - Table registration for Lance datasets
//! - Custom execution plan for KNN search (LanceKnnExec)
//! - Table function for explicit KNN search (lance_knn)
//! - Custom execution plan for full-text search (LanceFtsExec)
//! - Table function for explicit full-text search (lance_fts)
//! - Integration with DataFusion query engine

pub mod fts_exec;
pub mod fts_table_function;
pub mod knn_exec;
pub mod knn_table_function;
pub mod registration;
pub mod utils;

pub use fts_exec::LanceFtsExec;
pub use fts_table_function::{LanceFtsTableFunction, register_lance_fts_udtf};
pub use knn_exec::LanceKnnExec;
pub use knn_table_function::{LanceKnnTableFunction, register_lance_knn_udtf};
pub use registration::{
    LanceWriteOutcome, lance_dataset_exists, register_lance_table, write_lance_dataset,
};
