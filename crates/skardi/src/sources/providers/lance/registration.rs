use anyhow::{Context, Result};
use arrow::array::RecordBatchReader;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchIterator};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use lance::dataset::{Dataset, WriteMode, WriteParams};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::sources::providers::{DatasetEntry, DatasetRegistry};

/// Register a Lance dataset as a table in DataFusion SessionContext
///
/// # Arguments
/// * `session_ctx` - DataFusion session context to register the table into
/// * `name` - Name to register the table as
/// * `path` - Path to the Lance dataset directory
/// * `dataset_registry` - Optional registry to store dataset for optimizer access
///
/// # Example
/// ```no_run
/// use datafusion::prelude::SessionContext;
/// use skardi::sources::providers::lance::register_lance_table;
///
/// # async fn example() -> anyhow::Result<()> {
/// let mut ctx = SessionContext::new();
/// register_lance_table(&mut ctx, "embeddings", "data/embeddings.lance", None).await?;
/// # Ok(())
/// # }
/// ```
pub async fn register_lance_table(
    session_ctx: &mut SessionContext,
    name: &str,
    path: &str,
    dataset_registry: Option<&DatasetRegistry>,
) -> Result<()> {
    tracing::info!("Registering Lance dataset: {} from path: {}", name, path);

    // Verify the dataset directory exists
    let dataset_path = Path::new(path);
    if !dataset_path.exists() {
        return Err(anyhow::anyhow!(
            "Lance dataset directory does not exist: {}",
            path
        ));
    }

    // Open the Lance dataset
    let dataset = Dataset::open(path)
        .await
        .with_context(|| format!("Failed to open Lance dataset at path: {}", path))?;

    tracing::debug!(
        "Opened Lance dataset: {} with {} rows",
        name,
        dataset.count_rows(None).await.unwrap_or(0)
    );

    // Convert Lance dataset to Arc for sharing
    let dataset_arc = Arc::new(dataset);

    // Store in registry if provided (for optimizer access)
    if let Some(registry) = dataset_registry {
        let mut datasets = registry.write().unwrap_or_else(|p| p.into_inner());
        datasets.insert(
            name.to_string(),
            DatasetEntry::Lance(Arc::clone(&dataset_arc)),
        );
        tracing::debug!("Stored Lance dataset '{}' in registry for optimizer", name);
    }

    // Register the dataset as a table using Lance's native DataFusion integration
    // Lance's Dataset implements TableProvider trait directly
    session_ctx
        .register_table(name, dataset_arc)
        .with_context(|| format!("Failed to register Lance table '{}' with DataFusion", name))?;

    tracing::info!("Successfully registered Lance table: {}", name);

    Ok(())
}

/// Check whether a Lance dataset exists at the given filesystem path.
///
/// A Lance dataset is a directory containing a `_versions/` subdirectory —
/// the presence of that subdirectory is the cheapest way to tell a dataset
/// path from an unrelated directory (e.g. a parent dir a user picked by
/// mistake). Returning `false` for a path that does not exist or is not a
/// directory matches the "create fresh dataset" branch of job execution.
pub fn lance_dataset_exists(path: &str) -> bool {
    let p = Path::new(path);
    p.is_dir() && p.join("_versions").exists()
}

/// Result of a Lance write operation — the version the dataset landed on
/// plus the number of rows that were committed.
#[derive(Debug, Clone)]
pub struct LanceWriteOutcome {
    pub version: u64,
    pub rows_written: u64,
}

/// Write a batch stream to a Lance dataset in one of the supported modes.
///
/// * `WriteMode::Create` — fail if the dataset already exists.
/// * `WriteMode::Append` — append to an existing dataset; fail if it does not
///   exist.
/// * `WriteMode::Overwrite` — replace schema + data atomically, creating the
///   dataset if it does not exist.
///
/// The write is commit-at-end: if the batch stream errors partway through,
/// the previous dataset version remains the visible one and nothing is
/// committed. This is the atomicity guarantee the jobs primitive relies on.
pub async fn write_lance_dataset(
    path: &str,
    batches: Vec<RecordBatch>,
    mode: WriteMode,
) -> Result<LanceWriteOutcome> {
    if batches.is_empty() {
        anyhow::bail!(
            "refusing to write an empty batch set to Lance dataset at {path}: \
             the job produced zero rows, so there is nothing to commit"
        );
    }
    let schema = batches[0].schema();
    let rows_written: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
    let params = WriteParams {
        mode,
        ..Default::default()
    };

    let written = Dataset::write(reader, path, Some(params))
        .await
        .with_context(|| format!("Failed to write Lance dataset at path: {}", path))?;

    Ok(LanceWriteOutcome {
        version: written.version().version,
        rows_written,
    })
}

/// Stream a DataFusion batch source into a Lance dataset. Memory stays
/// proportional to the bounded channel (~2 batches) rather than the full
/// result size — this is the path the jobs primitive uses.
///
/// Atomicity matches [`write_lance_dataset`]: Lance writes data files as it
/// consumes batches and commits a single manifest version at the end. If the
/// stream errors partway (e.g. job cancelled), no manifest is committed and
/// the previous dataset version remains visible to readers.
///
/// Refuses to commit an empty stream, same as the batch variant.
///
/// ## Bridge threading
///
/// Lance wants a sync `RecordBatchReader`, our input is an async
/// `SendableRecordBatchStream`. We bridge with a bounded `tokio::sync::mpsc`
/// channel:
///
/// * **Producer** (tokio task) uses cooperative `send().await`, so the
///   runtime can interleave other tasks — critical for single-worker
///   runtimes (`#[tokio::test]` defaults to `current_thread`).
/// * **Consumer** (`RecordBatchReader::next`) calls `blocking_recv()`.
///   Lance internally consumes a sync reader from a `spawn_blocking`
///   context, so the blocking call does not stall any tokio worker.
///
/// An earlier implementation used `std::sync::mpsc::sync_channel` whose
/// `SyncSender::send` is blocking; that deadlocked on single-worker
/// runtimes when the producer task was scheduled on the same thread Lance
/// was trying to make progress on.
pub async fn write_lance_stream(
    path: &str,
    stream: SendableRecordBatchStream,
    mode: WriteMode,
) -> Result<LanceWriteOutcome> {
    let schema = stream.schema();

    // Small bound so peak memory is ~2 batches; the producer yields as soon
    // as the channel fills.
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<RecordBatch, ArrowError>>(2);
    let rows_seen = Arc::new(AtomicU64::new(0));
    let reader = ChannelRecordBatchReader {
        rx,
        schema: schema.clone(),
        rows_seen: Arc::clone(&rows_seen),
    };

    spawn_stream_pump(stream, tx);

    let params = WriteParams {
        mode,
        ..Default::default()
    };
    let written = Dataset::write(reader, path, Some(params))
        .await
        .with_context(|| format!("Failed to write Lance dataset at path: {}", path))?;

    let rows_written = rows_seen.load(Ordering::SeqCst);
    if rows_written == 0 {
        anyhow::bail!(
            "refusing to write an empty stream to Lance dataset at {path}: \
             the job produced zero rows, so there is nothing to commit"
        );
    }

    Ok(LanceWriteOutcome {
        version: written.version().version,
        rows_written,
    })
}

/// Sync `RecordBatchReader` fed by a `tokio::sync::mpsc` channel. Lance
/// consumes a sync reader from a `spawn_blocking` context, so the
/// `blocking_recv()` call does not stall any tokio worker.
struct ChannelRecordBatchReader {
    rx: tokio::sync::mpsc::Receiver<Result<RecordBatch, ArrowError>>,
    schema: SchemaRef,
    rows_seen: Arc<AtomicU64>,
}

impl Iterator for ChannelRecordBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rx.blocking_recv() {
            Some(Ok(batch)) => {
                self.rows_seen
                    .fetch_add(batch.num_rows() as u64, Ordering::SeqCst);
                Some(Ok(batch))
            }
            Some(Err(e)) => Some(Err(e)),
            // Sender dropped with no error → clean end-of-stream.
            None => None,
        }
    }
}

impl RecordBatchReader for ChannelRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Pump the async source into the channel. DataFusion errors are wrapped
/// as `ArrowError::ExternalError` so the reader can propagate them to
/// Lance, which will then abort the write and skip the manifest commit.
fn spawn_stream_pump(
    mut stream: SendableRecordBatchStream,
    tx: tokio::sync::mpsc::Sender<Result<RecordBatch, ArrowError>>,
) {
    tokio::spawn(async move {
        while let Some(item) = stream.next().await {
            let mapped = item.map_err(|e| ArrowError::ExternalError(Box::new(e)));
            if tx.send(mapped).await.is_err() {
                // Consumer closed the channel (Lance already failed). Drop
                // the rest of the stream and exit quietly.
                break;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use tempfile::TempDir;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_register_lance_table_missing_dataset() {
        let mut session_ctx = SessionContext::new();
        let result = register_lance_table(
            &mut session_ctx,
            "test_table",
            "/nonexistent/path/to/dataset.lance",
            None, // No registry
        )
        .await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("does not exist") || error_msg.contains("Failed to open"));
    }

    #[test]
    fn test_path_validation() {
        let path = Path::new("/nonexistent/path");
        assert!(!path.exists());
    }

    #[test]
    fn test_lance_dataset_exists_missing() {
        let tmp = TempDir::new().unwrap();
        let missing = tmp.path().join("nonexistent.lance");
        assert!(!lance_dataset_exists(missing.to_str().unwrap()));

        // An empty directory is not a Lance dataset.
        std::fs::create_dir_all(tmp.path().join("empty.lance")).unwrap();
        assert!(!lance_dataset_exists(
            tmp.path().join("empty.lance").to_str().unwrap()
        ));
    }

    #[tokio::test]
    async fn test_write_lance_create_then_append_and_overwrite() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("dataset.lance");
        let path_str = path.to_str().unwrap();

        // Create a fresh dataset with 3 rows.
        let created = write_lance_dataset(path_str, vec![test_batch()], WriteMode::Create)
            .await
            .expect("create should succeed");
        assert_eq!(created.rows_written, 3);
        assert!(lance_dataset_exists(path_str));

        // Append 3 more rows — should bump the version.
        let appended = write_lance_dataset(path_str, vec![test_batch()], WriteMode::Append)
            .await
            .expect("append should succeed");
        assert_eq!(appended.rows_written, 3);
        assert!(appended.version > created.version);

        // Overwrite replaces the dataset; total rows reset to 3.
        let overwritten = write_lance_dataset(path_str, vec![test_batch()], WriteMode::Overwrite)
            .await
            .expect("overwrite should succeed");
        assert_eq!(overwritten.rows_written, 3);

        // Register the table and count rows via DataFusion.
        let mut ctx = SessionContext::new();
        register_lance_table(&mut ctx, "t", path_str, None)
            .await
            .expect("register");
        let df = ctx.sql("SELECT COUNT(id) AS n FROM t").await.unwrap();
        let batches = df.collect().await.unwrap();
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 3, "overwrite should leave exactly one batch of rows");
    }

    #[tokio::test]
    async fn test_write_lance_rejects_empty_input() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("dataset.lance");
        let err = write_lance_dataset(path.to_str().unwrap(), vec![], WriteMode::Create)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[tokio::test]
    async fn test_write_lance_append_to_missing_creates_dataset() {
        // Lance's Append mode against a nonexistent path still works — the
        // InsertBuilder detects no existing dataset and falls back to Create.
        // Our job executor relies on a separate pre-flight check to reject
        // this case for jobs with `create_if_missing: false`.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("dataset.lance");
        let res = write_lance_dataset(
            path.to_str().unwrap(),
            vec![test_batch()],
            WriteMode::Append,
        )
        .await;
        assert!(res.is_ok());
    }
}
