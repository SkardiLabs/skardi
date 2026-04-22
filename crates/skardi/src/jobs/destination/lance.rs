//! Lance destination — streams batches from a DataFusion query into
//! `Dataset::write`, which writes data files as it goes and commits a single
//! manifest version at the end. If the source stream errors (e.g. cancelled),
//! no manifest commit happens and the previous version remains the only
//! visible one; orphan data files are reclaimed by Lance on the next
//! compaction.
//!
//! The async-to-sync bridge Lance needs is implemented in
//! [`crate::sources::providers::lance::write_lance_stream`].

use anyhow::{Context, Result};
use arrow::datatypes::Schema;
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use lance::{Dataset, dataset::WriteMode};
use std::sync::Arc;

use super::super::definition::DestinationMode;
use super::{JobDestination, JobDestinationKind, WriteOutcome};
use crate::sources::providers::lance::{lance_dataset_exists, write_lance_stream};

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
        stream: SendableRecordBatchStream,
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
        let outcome = write_lance_stream(&self.path, stream, write_mode).await?;
        Ok(WriteOutcome {
            rows_written: outcome.rows_written,
            snapshot_id: Some(outcome.version.to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::vec_to_stream;
    use super::super::{CancellableStream, JobDestination};
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use std::sync::atomic::AtomicBool;
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

    #[tokio::test]
    async fn lance_destination_create_then_schema_then_append() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("out.lance");
        let dest = LanceDestination::new(path.to_str().unwrap().to_string());

        // Nothing there yet.
        assert!(!dest.exists().await.unwrap());
        assert!(dest.schema().await.unwrap().is_none());

        // First append → creates the dataset.
        let batch = sample_batch();
        let schema = batch.schema();
        let out = dest
            .write(vec_to_stream(vec![batch], schema), DestinationMode::Append)
            .await
            .unwrap();
        assert_eq!(out.rows_written, 2);
        assert!(out.snapshot_id.is_some());
        assert!(dest.exists().await.unwrap());

        // Schema is now visible.
        let got_schema = dest.schema().await.unwrap().unwrap();
        assert_eq!(got_schema.fields().len(), 2);

        // Second append → version advances.
        let batch2 = sample_batch();
        let schema2 = batch2.schema();
        let out2 = dest
            .write(
                vec_to_stream(vec![batch2], schema2),
                DestinationMode::Append,
            )
            .await
            .unwrap();
        assert_eq!(out2.rows_written, 2);
        assert_ne!(out.snapshot_id, out2.snapshot_id);
    }

    #[tokio::test]
    async fn lance_destination_streams_many_batches_without_buffering() {
        // Feed 50 small batches as separate stream items. If the destination
        // were still buffering a Vec<RecordBatch>, the test would pass
        // identically — what we're really verifying here is that the
        // reader/writer bridge doesn't deadlock with a stream that's larger
        // than the bounded channel.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("many.lance");
        let dest = LanceDestination::new(path.to_str().unwrap().to_string());

        let batches: Vec<RecordBatch> = (0..50).map(|_| sample_batch()).collect();
        let schema = batches[0].schema();
        let out = dest
            .write(vec_to_stream(batches, schema), DestinationMode::Append)
            .await
            .unwrap();
        assert_eq!(out.rows_written, 100);
    }

    #[tokio::test]
    async fn lance_destination_cancelled_stream_leaves_dataset_uncommitted() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("cancel.lance");
        let dest = LanceDestination::new(path.to_str().unwrap().to_string());

        // Cancel flag pre-set → first poll of the stream errors. Destination
        // write must surface the error without committing anything.
        let batch = sample_batch();
        let schema = batch.schema();
        let inner = vec_to_stream(vec![batch], schema);
        let flag = Arc::new(AtomicBool::new(true));
        let stream = CancellableStream::new(inner, flag).boxed();

        let err = dest
            .write(stream, DestinationMode::Append)
            .await
            .expect_err("cancelled stream should fail the write");
        assert!(
            err.to_string().to_lowercase().contains("cancel")
                || err.to_string().to_lowercase().contains("error"),
            "unexpected error: {err}"
        );
        assert!(
            !dest.exists().await.unwrap(),
            "cancelled write must not create the dataset"
        );
    }
}
