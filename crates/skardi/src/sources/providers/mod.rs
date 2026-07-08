#[cfg(feature = "documents")]
pub mod documents;
pub mod dynamodb;
pub mod iceberg;
pub mod influxdb;
pub mod knn_utils;
pub mod lance;
pub mod mongo;
pub mod mysql;
pub mod mysql_wire;
pub mod redis;
pub mod seekdb;
pub mod sqlite;
pub mod sqlx;

use ::lance::dataset::Dataset;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use mongo::fts_table_function::MongoFtsEntry;
use seekdb::knn_table_function::SeekDbKnnEntry;
use sqlite::knn_table_function::SqliteEntry;
use sqlx::pg::knn_table_function::PgKnnEntry;

/// A single entry in the unified dataset registry.
#[derive(Clone, Debug)]
pub enum DatasetEntry {
    Lance(Arc<Dataset>),
    Postgres(PgKnnEntry),
    Mongo(MongoFtsEntry),
    Sqlite(SqliteEntry),
    Seekdb(SeekDbKnnEntry),
}

/// Unified registry mapping table name → dataset entry.
/// Shared by `lance_knn`, `lance_fts`, `pg_knn`, `pg_fts`, `mongo_fts`,
/// `sqlite_knn`, `sqlite_fts`, `seekdb_knn`, and `seekdb_fts` table functions.
pub type DatasetRegistry = Arc<RwLock<HashMap<String, DatasetEntry>>>;

/// Returns true if the expression is a binary comparison (`=`, `<>`, `<`, `<=`,
/// `>`, `>=`) between a bare column and a literal — the shape both the MongoDB
/// and DynamoDB providers can push into their backends. Shared here so the two
/// providers cannot silently diverge on what counts as pushable.
pub(crate) fn is_pushable_binary_filter(expr: &datafusion::logical_expr::Expr) -> bool {
    use datafusion::logical_expr::{Expr, Operator};
    match expr {
        Expr::BinaryExpr(binary) => {
            matches!(
                binary.op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            ) && matches!(
                (binary.left.as_ref(), binary.right.as_ref()),
                (Expr::Column(_), Expr::Literal(..)) | (Expr::Literal(..), Expr::Column(_))
            )
        }
        _ => false,
    }
}
