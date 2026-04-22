//! Destination writers — where a job's rows go.
//!
//! Two backends in the MVP:
//!
//! * [`LanceDestination`] — writes the job's RecordBatches to a Lance dataset
//!   on disk, creating the dataset on first run when
//!   `create_if_missing: true`. Uses commit-at-end semantics so a mid-stream
//!   error leaves the previous version as the only visible one.
//! * [`SqlDmlDestination`] — rewrites the SELECT as
//!   `INSERT INTO <table> SELECT ...` (or `DELETE FROM`+`INSERT` for
//!   `overwrite`) and hands it to the registered provider. Transactional
//!   atomicity is whatever the provider supports.
//!
//! A third "kind" — Iceberg — is reserved for v1.1; the enum variant exists
//! but its writer path deliberately errors out today.

use anyhow::{Context, Result};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::{catalog::MemTable, prelude::SessionContext};
use lance::{Dataset, dataset::WriteMode};
use std::sync::Arc;
use uuid::Uuid;

use super::definition::DestinationMode;
use crate::sources::providers::lance::{
    LanceWriteOutcome, lance_dataset_exists, write_lance_dataset,
};

/// Per-destination outcome — how many rows landed and, when available, the
/// version identifier the destination assigned to this commit.
#[derive(Debug, Clone)]
pub struct WriteOutcome {
    pub rows_written: u64,
    pub snapshot_id: Option<String>,
}

/// A destination is either a lake (has commit semantics, schema on write)
/// or a DB (schema out-of-band, writes via `INSERT INTO`). The executor
/// branches on this for the pre-flight check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobDestinationKind {
    Lake,
    Db,
}

/// A sink that accepts a fully-materialized batch set and commits it.
///
/// Jobs are batch-only in the MVP, so the writer takes a `Vec<RecordBatch>`
/// rather than a stream. The executor collects the DataFusion output
/// in-memory (first shipable version) and hands it to the destination.
/// Streaming is a v1.1 concern.
#[async_trait]
pub trait JobDestination: Send + Sync {
    /// Classification used by the pre-flight resolver.
    fn kind(&self) -> JobDestinationKind;

    /// Does the destination already exist in durable storage?
    async fn exists(&self) -> Result<bool>;

    /// Fetch the destination's Arrow schema if it exists (used by the
    /// submit-time pre-flight to diff against the query's output schema).
    async fn schema(&self) -> Result<Option<Arc<Schema>>>;

    /// Commit a batch of rows with the chosen mode.
    async fn write(&self, batches: Vec<RecordBatch>, mode: DestinationMode)
    -> Result<WriteOutcome>;
}

// ---------------------------------------------------------------------------
// Lance destination — disk path, atomic version commit.
// ---------------------------------------------------------------------------

/// Writes a job's output to a Lance dataset on disk. Path is whatever the
/// destination `table:` resolves to via the data source registry. When the
/// dataset does not exist yet, the first run creates it from the query's
/// output schema.
pub struct LanceDestination {
    path: String,
}

impl LanceDestination {
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

#[async_trait]
impl JobDestination for LanceDestination {
    fn kind(&self) -> JobDestinationKind {
        JobDestinationKind::Lake
    }

    async fn exists(&self) -> Result<bool> {
        Ok(lance_dataset_exists(&self.path))
    }

    async fn schema(&self) -> Result<Option<Arc<Schema>>> {
        if !lance_dataset_exists(&self.path) {
            return Ok(None);
        }
        let dataset = Dataset::open(&self.path)
            .await
            .with_context(|| format!("Failed to open Lance dataset at {}", self.path))?;
        Ok(Some(Arc::new(dataset.schema().into())))
    }

    async fn write(
        &self,
        batches: Vec<RecordBatch>,
        mode: DestinationMode,
    ) -> Result<WriteOutcome> {
        let write_mode = match mode {
            DestinationMode::Append => {
                if lance_dataset_exists(&self.path) {
                    WriteMode::Append
                } else {
                    WriteMode::Create
                }
            }
            DestinationMode::Overwrite => WriteMode::Overwrite,
        };
        let LanceWriteOutcome {
            version,
            rows_written,
        } = write_lance_dataset(&self.path, batches, write_mode).await?;
        Ok(WriteOutcome {
            rows_written,
            snapshot_id: Some(version.to_string()),
        })
    }
}

// ---------------------------------------------------------------------------
// SQL DML destination — federated DB table via `INSERT INTO ... SELECT`.
// ---------------------------------------------------------------------------

/// Target a read-write DB table that already has schema. The `table` field
/// is the fully-qualified DataFusion identifier — typically
/// `catalog.schema.table` for a catalog-mode source, or a bare name for a
/// table-mode one.
pub struct SqlDmlDestination {
    ctx: Arc<SessionContext>,
    table: String,
}

impl SqlDmlDestination {
    pub fn new(ctx: Arc<SessionContext>, table: impl Into<String>) -> Self {
        Self {
            ctx,
            table: table.into(),
        }
    }

    pub fn table(&self) -> &str {
        &self.table
    }
}

/// Quote a DataFusion table reference so dotted identifiers survive parsing
/// intact. `foo.bar.baz` → `"foo"."bar"."baz"`. Internal quotes in segment
/// names are doubled per SQL rules.
fn quote_table_ref(name: &str) -> String {
    name.split('.')
        .map(|seg| format!("\"{}\"", seg.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(".")
}

async fn lookup_table_schema(ctx: &SessionContext, table: &str) -> Result<Option<Arc<Schema>>> {
    let sql = format!("SELECT * FROM {} LIMIT 0", quote_table_ref(table));
    match ctx.sql(&sql).await {
        Ok(df) => Ok(Some(Arc::new(df.schema().as_arrow().clone()))),
        Err(e) => {
            // DataFusion surfaces multiple shapes for "table is not
            // registered": `not found`, `does not exist`,
            // `table 'foo' not found`, and `Unsupported compound
            // identifier '<quoted>'. Expected 1, 2 or 3 parts, got 4`
            // when the caller used a four-part identifier that couldn't
            // be resolved. Treat all of these as a missing destination
            // (the pre-flight will then branch on kind).
            let msg = e.to_string();
            let missing = msg.contains("not found")
                || msg.contains("does not exist")
                || msg.contains("Unsupported compound identifier");
            if missing {
                Ok(None)
            } else {
                Err(anyhow::anyhow!(
                    "Failed to resolve destination table '{}': {}",
                    table,
                    e
                ))
            }
        }
    }
}

#[async_trait]
impl JobDestination for SqlDmlDestination {
    fn kind(&self) -> JobDestinationKind {
        JobDestinationKind::Db
    }

    async fn exists(&self) -> Result<bool> {
        Ok(lookup_table_schema(&self.ctx, &self.table).await?.is_some())
    }

    async fn schema(&self) -> Result<Option<Arc<Schema>>> {
        lookup_table_schema(&self.ctx, &self.table).await
    }

    async fn write(
        &self,
        batches: Vec<RecordBatch>,
        mode: DestinationMode,
    ) -> Result<WriteOutcome> {
        let rows_written: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        if rows_written == 0 {
            tracing::info!(
                "SQL DML destination '{}': query returned zero rows; nothing to write",
                self.table
            );
            return Ok(WriteOutcome {
                rows_written: 0,
                snapshot_id: None,
            });
        }

        // Register a transient in-memory table that `INSERT INTO ... SELECT
        // * FROM <staging>` can read from. Random name so concurrent jobs
        // don't clobber each other's staging tables.
        let staging_name = format!("__skardi_jobs_staging_{}", Uuid::new_v4().simple());
        let schema = batches[0].schema();
        let mem_table = MemTable::try_new(schema, vec![batches])
            .context("Failed to build in-memory staging table")?;
        self.ctx
            .register_table(&staging_name, Arc::new(mem_table))
            .context("Failed to register staging table")?;

        let destination_sql = quote_table_ref(&self.table);
        let staging_sql = format!("\"{}\"", staging_name);

        let result: Result<()> = (async {
            if matches!(mode, DestinationMode::Overwrite) {
                let delete_sql = format!("DELETE FROM {destination_sql}");
                self.ctx
                    .sql(&delete_sql)
                    .await
                    .with_context(|| {
                        format!("Failed to plan overwrite DELETE for '{}'", self.table)
                    })?
                    .collect()
                    .await
                    .with_context(|| {
                        format!("Failed to execute overwrite DELETE for '{}'", self.table)
                    })?;
            }
            let insert_sql = format!("INSERT INTO {destination_sql} SELECT * FROM {staging_sql}");
            self.ctx
                .sql(&insert_sql)
                .await
                .with_context(|| format!("Failed to plan INSERT into '{}'", self.table))?
                .collect()
                .await
                .with_context(|| format!("Failed to execute INSERT into '{}'", self.table))?;
            Ok(())
        })
        .await;

        // Always try to drop the staging table even if the write failed.
        if let Err(e) = self.ctx.deregister_table(&staging_name) {
            tracing::warn!(
                "Failed to deregister staging table '{}': {}",
                staging_name,
                e
            );
        }

        result?;

        Ok(WriteOutcome {
            rows_written,
            snapshot_id: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use tempfile::TempDir;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn quote_table_ref_handles_dotted_idents_and_quotes() {
        assert_eq!(quote_table_ref("plain"), "\"plain\"");
        assert_eq!(
            quote_table_ref("cat.schema.tbl"),
            "\"cat\".\"schema\".\"tbl\""
        );
        assert_eq!(quote_table_ref("has\"quote"), "\"has\"\"quote\"");
    }

    #[tokio::test]
    async fn lance_destination_create_then_schema_then_append() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("out.lance");
        let dest = LanceDestination::new(path.to_str().unwrap().to_string());

        // Nothing there yet.
        assert!(!dest.exists().await.unwrap());
        assert!(dest.schema().await.unwrap().is_none());

        // First append → creates the dataset.
        let out = dest
            .write(vec![sample_batch()], DestinationMode::Append)
            .await
            .unwrap();
        assert_eq!(out.rows_written, 2);
        assert!(out.snapshot_id.is_some());
        assert!(dest.exists().await.unwrap());

        // Schema is now visible.
        let schema = dest.schema().await.unwrap().unwrap();
        assert_eq!(schema.fields().len(), 2);

        // Second append → version advances.
        let out2 = dest
            .write(vec![sample_batch()], DestinationMode::Append)
            .await
            .unwrap();
        assert_eq!(out2.rows_written, 2);
        assert_ne!(out.snapshot_id, out2.snapshot_id);
    }

    #[tokio::test]
    async fn sql_dml_destination_missing_table_reports_none() {
        let ctx = Arc::new(SessionContext::new());
        let dest = SqlDmlDestination::new(ctx, "not.a.real.table");
        assert!(!dest.exists().await.unwrap());
        assert!(dest.schema().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn sql_dml_destination_inserts_into_memtable() {
        // MemTable implements `insert_into`, so we can exercise the DML
        // path without a real DB. This covers the happy-path INSERT INTO
        // ... SELECT * FROM staging end to end.
        let ctx = Arc::new(SessionContext::new());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let empty = RecordBatch::new_empty(schema.clone());
        let tbl = datafusion::datasource::MemTable::try_new(schema, vec![vec![empty]]).unwrap();
        ctx.register_table("dest", Arc::new(tbl)).unwrap();

        let dest = SqlDmlDestination::new(Arc::clone(&ctx), "dest");
        assert!(dest.exists().await.unwrap());
        let out = dest
            .write(vec![sample_batch()], DestinationMode::Append)
            .await
            .unwrap();
        assert_eq!(out.rows_written, 2);

        let batches = ctx
            .sql("SELECT COUNT(*) AS n FROM dest")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 2);
    }
}
