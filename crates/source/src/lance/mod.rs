//! Lance integration module
//!
//! Provides:
//! - Custom execution plan for KNN search (LanceKnnExec)
//! - Table function for explicit KNN search (lance_knn)
//! - Integration with DataFusion query engine

pub mod knn_exec;
pub mod knn_table_function;

pub use knn_exec::LanceKnnExec;
pub use knn_table_function::{LanceKnnTableFunction, register_lance_knn_udtf};
