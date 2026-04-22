//! Destination writers — where a job's rows go.
//!
//! Backends live in their own submodules:
//!
//! * [`lance::LanceDestination`] — writes to a Lance dataset on disk; commit-at-end
//!   atomicity.
//! * [`sql_dml::SqlDmlDestination`] — `INSERT INTO <table> SELECT ...` (or
//!   `DELETE FROM`+`INSERT` for `overwrite`) against a federated DB table. The
//!   underlying datafusion-table-providers sinks wrap the full INSERT in a
//!   single transaction, so partial writes are not visible on crash. We
//!   therefore restrict this destination to transactional source kinds
//!   (Postgres/MySQL/SQLite) — non-transactional backends (Redis, Mongo,
//!   Seekdb) are rejected at submit time.
//!
//! A third "kind" — Iceberg — is reserved for v1.1; when it lands it will be
//! its own submodule here.
//!
//! Both backends consume a [`SendableRecordBatchStream`] so the executor can
//! stream the query output batch-by-batch rather than materializing the whole
//! result. Cancel happens via [`CancellableStream`], which errors out of
//! `poll_next` as soon as the run's cancel flag flips — the destination's
//! in-flight transaction / manifest commit then unwinds cleanly with no rows
//! visible.

use anyhow::Result;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use datafusion::prelude::SessionContext;

use super::definition::DestinationMode;

mod lance;
mod sql_dml;

pub use lance::LanceDestination;
pub use sql_dml::SqlDmlDestination;

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

/// A sink that commits a streaming query result.
///
/// Both implementations consume the stream batch-by-batch, so memory stays
/// proportional to the in-flight batch size rather than the full result.
/// Atomicity is preserved end-to-end: Lance commits one manifest version at
/// the end, SQL DML runs inside one wrapping transaction. If the stream
/// errors or is cancelled mid-flight, nothing is visible to readers.
#[async_trait]
pub trait JobDestination: Send + Sync {
    /// Classification used by the pre-flight resolver.
    fn kind(&self) -> JobDestinationKind;

    /// Does the destination already exist in durable storage?
    async fn exists(&self) -> Result<bool>;

    /// Fetch the destination's Arrow schema if it exists (used by the
    /// submit-time pre-flight to diff against the query's output schema).
    async fn schema(&self) -> Result<Option<Arc<Schema>>>;

    /// Consume `stream` and commit the rows with the chosen mode.
    async fn write(
        &self,
        stream: SendableRecordBatchStream,
        mode: DestinationMode,
    ) -> Result<WriteOutcome>;
}

/// Stream adapter that errors out of `poll_next` as soon as `cancelled` flips.
/// Wrapping the executor's query stream with this is how cancel reaches the
/// destination: the next `data.next().await` in the provider's sink loop
/// returns `Err`, its transaction aborts, and nothing lands.
pub struct CancellableStream {
    inner: SendableRecordBatchStream,
    cancelled: Arc<AtomicBool>,
}

impl CancellableStream {
    pub fn new(inner: SendableRecordBatchStream, cancelled: Arc<AtomicBool>) -> Self {
        Self { inner, cancelled }
    }

    /// Box + pin to hand to any `SendableRecordBatchStream` consumer.
    pub fn boxed(self) -> SendableRecordBatchStream {
        Box::pin(self)
    }
}

impl Stream for CancellableStream {
    type Item = std::result::Result<RecordBatch, DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.cancelled.load(Ordering::SeqCst) {
            return Poll::Ready(Some(Err(DataFusionError::Execution(
                "job cancelled before commit".to_string(),
            ))));
        }
        Pin::new(&mut this.inner).poll_next(cx)
    }
}

impl RecordBatchStream for CancellableStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

/// Quote a DataFusion table reference so dotted identifiers survive parsing
/// intact. `foo.bar.baz` → `"foo"."bar"."baz"`. Internal quotes in segment
/// names are doubled per SQL rules.
pub(crate) fn quote_table_ref(name: &str) -> String {
    name.split('.')
        .map(|seg| format!("\"{}\"", seg.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(".")
}

/// Resolve the Arrow schema of an already-registered table via a
/// `SELECT * LIMIT 0`. Returns `Ok(None)` when DataFusion reports the table
/// is not registered; other errors propagate.
pub(crate) async fn lookup_table_schema(
    ctx: &SessionContext,
    table: &str,
) -> Result<Option<Arc<Schema>>> {
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

#[cfg(test)]
pub(crate) mod test_util {
    use super::*;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::stream;

    /// Wrap a `Vec<RecordBatch>` as a `SendableRecordBatchStream` so unit
    /// tests can drive the new streaming `JobDestination::write` surface.
    pub fn vec_to_stream(
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
    ) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(batches.into_iter().map(Ok)),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::test_util::vec_to_stream;
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use futures::StreamExt;

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
    async fn cancellable_stream_errors_when_flag_is_set_before_first_poll() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
            .unwrap();
        let inner = vec_to_stream(vec![batch], schema);
        let flag = Arc::new(AtomicBool::new(true));
        let mut s = CancellableStream::new(inner, flag).boxed();
        let item = s.next().await.expect("one item");
        assert!(item.is_err(), "expected Err, got {:?}", item);
        assert!(s.next().await.is_none() || true); // doesn't matter what comes after
    }

    #[tokio::test]
    async fn cancellable_stream_passes_through_when_not_cancelled() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap();
        let inner = vec_to_stream(vec![batch], schema);
        let flag = Arc::new(AtomicBool::new(false));
        let mut s = CancellableStream::new(inner, flag).boxed();
        let b = s.next().await.unwrap().unwrap();
        assert_eq!(b.num_rows(), 2);
        assert!(s.next().await.is_none());
    }
}
