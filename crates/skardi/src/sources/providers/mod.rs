pub mod iceberg;
pub mod knn_utils;
pub mod lance;
pub mod mongo;
pub mod mysql;
pub mod redis;
pub mod sqlite;
pub mod sqlx;

use ::lance::dataset::Dataset;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use mongo::fts_table_function::MongoFtsEntry;
use sqlx::pg::knn_table_function::PgKnnEntry;

/// A single entry in the unified dataset registry.
#[derive(Clone, Debug)]
pub enum DatasetEntry {
    Lance(Arc<Dataset>),
    Postgres(PgKnnEntry),
    Mongo(MongoFtsEntry),
}

/// Unified registry mapping table name → dataset entry.
/// Shared by `lance_knn`, `lance_fts`, `pg_knn`, `pg_fts`, and `mongo_fts` table functions.
pub type DatasetRegistry = Arc<RwLock<HashMap<String, DatasetEntry>>>;
