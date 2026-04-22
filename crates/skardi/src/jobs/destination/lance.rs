//! Lance destination — commit-at-end writes to a Lance dataset on disk.

use anyhow::{Context, Result};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use lance::{Dataset, dataset::WriteMode};
use std::sync::Arc;

use super::super::definition::DestinationMode;
use super::{JobDestination, JobDestinationKind, WriteOutcome};
use crate::sources::providers::lance::{
    LanceWriteOutcome, lance_dataset_exists, write_lance_dataset,
};

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
}
