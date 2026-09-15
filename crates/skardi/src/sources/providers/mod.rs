// Shared local-vs-object-store I/O for the `documents` and `obsidian`
// connectors. `pub(crate)`: `llm_extract`'s image fetch also reads `s3://`
// refs through it so S3 client construction and the env-only credential
// contract live in exactly one place.
#[cfg(any(feature = "documents", feature = "obsidian"))]
pub(crate) mod blob;
pub mod clickhouse;
#[cfg(feature = "documents")]
pub mod documents;
pub mod dynamodb;
pub mod graph;
pub mod iceberg;
pub mod influxdb;
pub mod knn_utils;
pub mod lance;
pub mod mongo;
pub mod mysql;
pub mod mysql_wire;
#[cfg(feature = "obsidian")]
pub mod obsidian;
pub mod open_connector;
pub mod redis;
// Config/error types compile unconditionally (plain serde/thiserror, no
// heavy deps) so the server and CLI can hold a typed `RssConfig` field even
// in builds without the `rss` feature; feature-gated submodules land later.
pub mod rss;
pub mod seekdb;
pub mod sqlite;
pub mod sqlx;
pub(crate) mod udtf_args;

use ::lance::dataset::Dataset;
use datafusion::datasource::TableProvider;
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

/// Wrapper working around a `datafusion-table-providers` 0.10.1 bug shared by
/// the ClickHouse and Flight (InfluxDB) providers: when DataFusion requests an
/// **empty** projection (e.g. `SELECT count(*)`), the inner table emits
/// batches whose width disagrees with the advertised zero-column schema —
/// ClickHouse's unparsed SQL still selects a column, and the Flight provider's
/// `enforce_schema` returns the original full-width batch — so execution
/// aborts downstream.
///
/// We intercept the empty-projection case: scan a single real column through
/// the inner table, then strip it back to zero columns with a
/// [`ProjectionExec`], which preserves the row count. All other projections
/// delegate straight to the inner table.
///
/// The scanned column still streams in full when the source does no aggregate
/// pushdown (see e.g. `docs/clickhouse/README.md`), so [`Self::scan`] picks
/// the narrowest fixed-width column rather than whatever sits at index 0,
/// which could be an arbitrarily wide String.
#[derive(Debug)]
pub(crate) struct CountSafeTable {
    pub(crate) inner: Arc<dyn TableProvider>,
}

#[async_trait::async_trait]
impl TableProvider for CountSafeTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        self.inner.table_type()
    }

    // Forward the remaining planning hooks so the wrapper is transparent to
    // the optimizer apart from the count(*) interception below.
    fn statistics(&self) -> Option<datafusion::common::Statistics> {
        self.inner.statistics()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&datafusion::logical_expr::Expr],
    ) -> datafusion::common::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>>
    {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        use datafusion::physical_plan::projection::ProjectionExec;
        match projection {
            // Empty projection (count(*) / EXISTS): fetch one column so the
            // inner table produces a correctly-shaped batch, then drop it
            // again to honour the requested zero-column output.
            Some(p) if p.is_empty() => {
                let single = vec![narrowest_column_index(&self.inner.schema())];
                let plan = self
                    .inner
                    .scan(state, Some(&single), filters, limit)
                    .await?;
                let empty: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> =
                    Vec::new();
                Ok(Arc::new(ProjectionExec::try_new(empty, plan)?))
            }
            _ => self.inner.scan(state, projection, filters, limit).await,
        }
    }
}

/// Index of the cheapest column to stream when only a row count is needed:
/// the narrowest fixed-width field, falling back to index 0 when every column
/// is variable-width. Ties resolve to the first such column, so the choice is
/// deterministic.
pub(crate) fn narrowest_column_index(schema: &datafusion::arrow::datatypes::SchemaRef) -> usize {
    use datafusion::arrow::datatypes::DataType;
    schema
        .fields()
        .iter()
        .enumerate()
        .min_by_key(|(_, field)| match field.data_type() {
            // Bit-packed in Arrow, so `primitive_width` reports nothing; it's
            // still the cheapest fixed-width thing a table can hold.
            DataType::Boolean => 1,
            dt => dt.primitive_width().unwrap_or(usize::MAX),
        })
        .map(|(idx, _)| idx)
        .unwrap_or(0)
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};

    #[test]
    fn narrowest_column_index_prefers_narrowest_fixed_width() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("flag", DataType::Boolean, false),
            Field::new("id", DataType::UInt32, false),
        ]));
        assert_eq!(narrowest_column_index(&schema), 2, "Boolean is narrowest");
    }

    #[test]
    fn narrowest_column_index_falls_back_to_first_column() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Binary, false),
        ]));
        assert_eq!(narrowest_column_index(&schema), 0);
    }
}
