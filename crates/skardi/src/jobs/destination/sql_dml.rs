//! SQL DML destination — writes via `INSERT INTO <table> SELECT * FROM <staging>`
//! against an already-registered federated table.

use anyhow::{Context, Result};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::{catalog::MemTable, prelude::SessionContext};
use std::sync::Arc;
use uuid::Uuid;

use super::super::definition::DestinationMode;
use super::{
    JobDestination, JobDestinationKind, WriteOutcome, lookup_table_schema, quote_table_ref,
};

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
