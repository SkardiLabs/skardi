//! Lance integration module
//!
//! Provides:
//! - Table registration for Lance datasets
//! - Custom execution plan for KNN search (LanceKnnExec)
//! - Table function for explicit KNN search (lance_knn)
//! - Integration with DataFusion query engine

pub mod knn_exec;
pub mod knn_table_function;
pub mod registration;

pub use knn_exec::LanceKnnExec;
pub use knn_table_function::{LanceKnnTableFunction, register_lance_knn_udtf};
pub use registration::register_lance_table;
