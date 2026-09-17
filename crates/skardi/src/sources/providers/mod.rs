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

/// Wrapper working around a `datafusion-table-providers` 0.10.1 bug: when
/// DataFusion requests an **empty** projection (e.g. `SELECT count(*)`), the
/// inner table emits batches whose width disagrees with the advertised
/// zero-column schema, so execution aborts downstream with
/// `Different number of fields: (physical) 1 vs (logical) 0`.
///
/// Every provider in that crate we build on has the defect, by three separate
/// routes: ClickHouse's unparsed SQL still selects a column; the Flight
/// (InfluxDB) provider's `enforce_schema` returns the original full-width
/// batch; and `SqlTable` — the generic SQL provider behind our Postgres and
/// SeekDB sources — unparses an empty projection as `SELECT 1` and reports a
/// one-column Int64 schema for it (`sql_provider_datafusion::project_schema_safe`).
/// Anything built on those tables must go through this wrapper.
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

    // Read-write providers stack on top of this one and forward `constraints`
    // to whatever they hold as their read provider, so dropping the inner
    // table's constraints here would silently disarm the primary-key handling
    // one layer up.
    fn constraints(&self) -> Option<&datafusion::common::Constraints> {
        self.inner.constraints()
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
    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::catalog::Session;
    use datafusion::common::Result as DataFusionResult;
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::{Expr, TableType};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;

    /// Stands in for `datafusion-table-providers`' `SqlTable` in the one
    /// respect that matters here: an EMPTY projection is unparsed as
    /// `SELECT 1`, so the plan it hands back carries a single Int64 column
    /// while the logical scan above it has none. Every other projection
    /// behaves normally.
    ///
    /// This reproduces the real defect without a database, which is the
    /// point — the live Postgres suite that also covers it is `#[ignore]`d
    /// and only runs where CI has a server.
    #[derive(Debug)]
    struct EmptyProjectionReturnsOneColumn {
        table: Arc<MemTable>,
        row_count: usize,
    }

    impl EmptyProjectionReturnsOneColumn {
        fn with_rows(n: usize) -> Self {
            let schema: SchemaRef = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
            ]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())),
                    Arc::new(StringArray::from(vec!["row"; n])),
                ],
            )
            .expect("fixture batch");
            Self {
                table: Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("memtable")),
                row_count: n,
            }
        }
    }

    const FIXTURE_ROWS: usize = 7;

    #[async_trait::async_trait]
    impl TableProvider for EmptyProjectionReturnsOneColumn {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.table.schema()
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            match projection {
                Some(p) if p.is_empty() => {
                    let one: SchemaRef =
                        Arc::new(Schema::new(vec![Field::new("1", DataType::Int64, true)]));
                    let batch = RecordBatch::try_new(
                        Arc::clone(&one),
                        vec![Arc::new(Int64Array::from(vec![1i64; self.row_count]))],
                    )?;
                    MemTable::try_new(one, vec![vec![batch]])?
                        .scan(state, None, &[], None)
                        .await
                }
                _ => self.table.scan(state, projection, filters, limit).await,
            }
        }
    }

    /// The defect this wrapper exists for, pinned so it cannot be mistaken
    /// for a test that passes because nothing happens.
    #[tokio::test]
    async fn count_star_on_the_bare_table_fails_with_a_width_mismatch() {
        let ctx = SessionContext::new();
        ctx.register_table(
            "t",
            Arc::new(EmptyProjectionReturnsOneColumn::with_rows(FIXTURE_ROWS)),
        )
        .expect("register");

        let err = ctx
            .sql("SELECT count(*) FROM t")
            .await
            .expect("the statement PLANS — only execution fails")
            .collect()
            .await
            .expect_err("an empty projection must break the bare table");
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("physical") && msg.contains("logical"),
            "expected the plan/exec schema mismatch, got: {err}"
        );
    }

    #[tokio::test]
    async fn count_star_through_count_safe_table_returns_the_row_count() {
        let ctx = SessionContext::new();
        ctx.register_table(
            "t",
            Arc::new(CountSafeTable {
                inner: Arc::new(EmptyProjectionReturnsOneColumn::with_rows(FIXTURE_ROWS)),
            }),
        )
        .expect("register");

        let batches = ctx
            .sql("SELECT count(*) FROM t")
            .await
            .expect("plan")
            .collect()
            .await
            .expect("an empty projection must survive the wrapper");
        let counted = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count(*) is Int64")
            .value(0);
        assert_eq!(counted, FIXTURE_ROWS as i64);
    }

    /// The interception is confined to the empty projection: an ordinary
    /// `SELECT col` must still reach the inner table untouched, columns and
    /// cardinality intact.
    #[tokio::test]
    async fn the_wrapper_leaves_ordinary_projections_alone() {
        let ctx = SessionContext::new();
        ctx.register_table(
            "t",
            Arc::new(CountSafeTable {
                inner: Arc::new(EmptyProjectionReturnsOneColumn::with_rows(FIXTURE_ROWS)),
            }),
        )
        .expect("register");

        let batches = ctx
            .sql("SELECT name FROM t")
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect");
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            FIXTURE_ROWS
        );
    }

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
