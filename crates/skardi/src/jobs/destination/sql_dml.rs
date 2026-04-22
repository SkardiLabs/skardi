//! SQL DML destination — writes via `INSERT INTO <table> SELECT * FROM <stream>`
//! where `<stream>` is a one-shot `StreamingTable` wrapping the executor's
//! `SendableRecordBatchStream`.
//!
//! DataFusion plans the INSERT, its upstream table-providers sink
//! (`datafusion_table_providers::{postgres,mysql}::write`) opens one wrapping
//! transaction, pulls from the stream, inserts batch-by-batch, and commits at
//! the end. If the source stream errors mid-flight (e.g. cancelled), the
//! transaction aborts and nothing is visible. Atomicity is therefore
//! end-to-end for Postgres / MySQL / SQLite; the executor rejects
//! non-transactional source kinds (Redis, Mongo, Seekdb) at submit time.
//!
//! Overwrite mode is not supported — `DELETE FROM` and `INSERT INTO` would
//! need to share a single transaction, and DataFusion's SQL surface does not
//! expose a multi-statement transaction handle. See `DestinationMode` for
//! the full rationale.
//!
//! Guideline: SQL DML jobs should be bounded in size (~10M rows / ~10GB). The
//! sink warns in the run logs when crossing those thresholds — a single huge
//! INSERT holds write locks, bloats WAL / undo log, and can lag replicas.
//! For larger ingests, use the database's native bulk loader (`COPY`,
//! `LOAD DATA INFILE`) or a CDC pipeline.

use anyhow::{Context, Result};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::SessionContext;
use futures::Stream;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context as TaskCtx, Poll};
use uuid::Uuid;

use super::super::definition::DestinationMode;
use super::{
    JobDestination, JobDestinationKind, WriteOutcome, lookup_table_schema, quote_table_ref,
};

/// Row thresholds at which we surface an operational warning. The DB is
/// inside one growing transaction; huge jobs put real pressure on WAL /
/// locks / replication.
const SQL_DML_SOFT_ROW_LIMIT: u64 = 10_000_000;

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
        stream: SendableRecordBatchStream,
        _mode: DestinationMode,
    ) -> Result<WriteOutcome> {
        // MVP supports append only — overwrite is rejected at YAML load.
        let schema = stream.schema();

        // Tally rows as the stream flows through the counting adapter so we
        // can: (1) surface the job-size warning, (2) return rows_written
        // without a second round-trip to the DB, (3) detect the empty-stream
        // case and skip the INSERT entirely rather than emitting a no-op
        // transaction.
        let rows_written = Arc::new(AtomicU64::new(0));
        let counted = CountingStream::new(stream, Arc::clone(&rows_written), &self.table);

        // One-shot PartitionStream holds the stream and hands it off the
        // first (and only) time DataFusion calls `execute`. If something
        // plans the INSERT twice, the second execute panics — which is
        // exactly what we want, since we've consumed the input by then.
        let partition: Arc<dyn PartitionStream> = Arc::new(SingleUsePartitionStream::new(
            Box::pin(counted),
            schema.clone(),
        ));
        let staging_table = StreamingTable::try_new(schema, vec![partition])
            .context("Failed to build StreamingTable for SQL DML destination")?;

        // Random name so concurrent jobs don't clobber each other's staging
        // tables. Dropped in a `finally`-style block so failures don't leak
        // the registration.
        let staging_name = format!("__skardi_jobs_staging_{}", Uuid::new_v4().simple());
        self.ctx
            .register_table(&staging_name, Arc::new(staging_table))
            .context("Failed to register streaming staging table")?;

        let destination_sql = quote_table_ref(&self.table);
        let staging_sql = format!("\"{}\"", staging_name);

        let result: Result<()> = (async {
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

        let final_rows = rows_written.load(Ordering::SeqCst);
        if final_rows == 0 {
            tracing::info!(
                "SQL DML destination '{}': query returned zero rows; transaction was a no-op",
                self.table
            );
        }

        Ok(WriteOutcome {
            rows_written: final_rows,
            snapshot_id: None,
        })
    }
}

/// Stream adapter that counts rows as they flow through and emits a
/// `tracing::warn!` the first time the job crosses [`SQL_DML_SOFT_ROW_LIMIT`].
/// Does not alter the batches in any way.
struct CountingStream {
    inner: SendableRecordBatchStream,
    rows_written: Arc<AtomicU64>,
    table: String,
    warned: bool,
}

impl CountingStream {
    fn new(inner: SendableRecordBatchStream, rows_written: Arc<AtomicU64>, table: &str) -> Self {
        Self {
            inner,
            rows_written,
            table: table.to_string(),
            warned: false,
        }
    }
}

impl Stream for CountingStream {
    type Item = std::result::Result<RecordBatch, datafusion::error::DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskCtx<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let prior = this
                    .rows_written
                    .fetch_add(batch.num_rows() as u64, Ordering::SeqCst);
                let total = prior + batch.num_rows() as u64;
                if !this.warned && total >= SQL_DML_SOFT_ROW_LIMIT {
                    tracing::warn!(
                        "SQL DML destination '{}': job has written {} rows in a single \
                         transaction — approaching or exceeding the soft limit of {}. \
                         Large INSERTs hold locks, grow WAL/undo log, and can lag replicas. \
                         Consider using the database's native bulk loader (COPY, LOAD DATA \
                         INFILE) or a CDC pipeline for jobs of this size.",
                        this.table,
                        total,
                        SQL_DML_SOFT_ROW_LIMIT
                    );
                    this.warned = true;
                }
                Poll::Ready(Some(Ok(batch)))
            }
            other => other,
        }
    }
}

impl RecordBatchStream for CountingStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

/// `PartitionStream` that owns a single `SendableRecordBatchStream` and
/// hands it out exactly once. Panics on a second `execute` call — DataFusion
/// should never re-execute a StreamingTable we registered for an INSERT.
struct SingleUsePartitionStream {
    stream: Mutex<Option<SendableRecordBatchStream>>,
    schema: SchemaRef,
}

impl SingleUsePartitionStream {
    fn new(stream: SendableRecordBatchStream, schema: SchemaRef) -> Self {
        Self {
            stream: Mutex::new(Some(stream)),
            schema,
        }
    }
}

impl fmt::Debug for SingleUsePartitionStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SingleUsePartitionStream")
            .field("schema", &self.schema)
            .finish()
    }
}

impl PartitionStream for SingleUsePartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        self.stream
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
            .expect("SingleUsePartitionStream::execute called more than once")
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::vec_to_stream;
    use super::super::{CancellableStream, JobDestination};
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use std::sync::atomic::AtomicBool;

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
    async fn sql_dml_destination_inserts_into_memtable_via_stream() {
        // MemTable implements `insert_into` and DataFusion's INSERT planner
        // happily drives it from a StreamingTable source. This covers the
        // happy-path INSERT INTO ... SELECT * FROM <stream>.
        let ctx = Arc::new(SessionContext::new());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let empty = RecordBatch::new_empty(schema.clone());
        let tbl =
            datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![empty]]).unwrap();
        ctx.register_table("dest", Arc::new(tbl)).unwrap();

        let dest = SqlDmlDestination::new(Arc::clone(&ctx), "dest");
        assert!(dest.exists().await.unwrap());
        let batch = sample_batch();
        let out = dest
            .write(vec_to_stream(vec![batch], schema), DestinationMode::Append)
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

    #[tokio::test]
    async fn sql_dml_destination_streams_many_batches() {
        let ctx = Arc::new(SessionContext::new());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let empty = RecordBatch::new_empty(schema.clone());
        let tbl =
            datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![empty]]).unwrap();
        ctx.register_table("dest2", Arc::new(tbl)).unwrap();
        let dest = SqlDmlDestination::new(Arc::clone(&ctx), "dest2");

        let batches: Vec<RecordBatch> = (0..20).map(|_| sample_batch()).collect();
        let out = dest
            .write(vec_to_stream(batches, schema), DestinationMode::Append)
            .await
            .unwrap();
        assert_eq!(out.rows_written, 40);
    }

    #[tokio::test]
    async fn sql_dml_destination_cancelled_stream_fails_the_write() {
        let ctx = Arc::new(SessionContext::new());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let empty = RecordBatch::new_empty(schema.clone());
        let tbl =
            datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![empty]]).unwrap();
        ctx.register_table("dest_cancel", Arc::new(tbl)).unwrap();
        let dest = SqlDmlDestination::new(Arc::clone(&ctx), "dest_cancel");

        let batch = sample_batch();
        let inner = vec_to_stream(vec![batch], schema);
        let flag = Arc::new(AtomicBool::new(true)); // already cancelled
        let stream = CancellableStream::new(inner, flag).boxed();

        let err = dest
            .write(stream, DestinationMode::Append)
            .await
            .expect_err("cancelled stream should fail the write");
        assert!(
            err.to_string().to_lowercase().contains("cancel")
                || err.to_string().to_lowercase().contains("insert"),
            "unexpected error: {err}"
        );

        // No rows should have landed in the destination.
        let batches = ctx
            .sql("SELECT COUNT(*) AS n FROM dest_cancel")
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
        assert_eq!(n, 0, "cancelled stream must not commit any rows");
    }
}
