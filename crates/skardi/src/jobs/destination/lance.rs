//! Lance destination — streams batches from a DataFusion query into
//! `Dataset::write`, which writes data files as it goes and commits a single
//! manifest version at the end. If the source stream errors (e.g. cancelled),
//! no manifest commit happens and the previous version remains the only
//! visible one; orphan data files are reclaimed by Lance on the next
//! compaction.
//!
//! The async-to-sync bridge Lance needs is implemented in
//! [`crate::sources::providers::lance::write_lance_stream`].

use anyhow::{Context, Result, bail};
use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::row::{OwnedRow, RowConverter, SortField};
use arrow::util::display::array_value_to_string;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{StreamExt, TryStreamExt};
use lance::Dataset;
use lance::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched, WriteMode};
use std::collections::HashSet;
use std::sync::Arc;

use super::super::definition::{DestinationMode, RepeatedKeys};
use super::{JobDestination, JobDestinationKind, WriteOutcome};
use crate::sources::providers::lance::{lance_dataset_exists_at, write_lance_stream};

/// Writes a job's output to a Lance dataset, on local disk or at an object-store
/// URI (`s3://...`, credentials from `AWS_*`). Path is whatever the
/// destination `table:` resolves to via the data source registry. When the
/// dataset does not exist yet, the first run creates it from the query's
/// output schema.
///
/// `append` adds every row. `upsert` merges on `merge_keys` with Lance's
/// `merge_insert`: a row whose keys match an existing row replaces it whole,
/// and any other row is inserted, in one commit. Two properties a caller can
/// rely on:
/// - **one row per key.** The same key twice in one run's output fails the
///   write, whether or not the dataset already has that key, rather than
///   keeping an arbitrary one of them. This is checked here, on the stream
///   (`reject_repeated_keys`): Lance's own duplicate check only fires for
///   source rows that MATCH an existing row, so a key new to the dataset
///   would otherwise be inserted twice.
/// - **a commit race is retried, not lost.** A merge that loses a race to
///   another writer replays a buffered copy of its input against the newer
///   version (Lance's default), rather than overwriting that writer's rows.
pub struct LanceDestination {
    path: String,
    merge_keys: Vec<String>,
    repeated_keys: RepeatedKeys,
}

impl LanceDestination {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            merge_keys: Vec::new(),
            repeated_keys: RepeatedKeys::Refuse,
        }
    }

    /// The columns `upsert` matches on. Ignored by `append`.
    ///
    /// # Example
    /// ```
    /// use skardi::jobs::LanceDestination;
    ///
    /// // One row per (path, source_id): a re-read record replaces its row.
    /// let dest = LanceDestination::new("data/corpus.lance")
    ///     .with_merge_keys(vec!["path".to_string(), "source_id".to_string()]);
    /// assert_eq!(dest.path(), "data/corpus.lance");
    /// ```
    pub fn with_merge_keys(mut self, merge_keys: Vec<String>) -> Self {
        self.merge_keys = merge_keys;
        self
    }

    /// What `upsert` does with a key repeated within one run's output
    /// ([`RepeatedKeys`]). Ignored by `append`.
    ///
    /// # Example
    /// ```
    /// use skardi::jobs::{LanceDestination, RepeatedKeys};
    ///
    /// // A source whose listing can repeat a record: keep its last copy.
    /// let dest = LanceDestination::new("data/corpus.lance")
    ///     .with_merge_keys(vec!["path".to_string()])
    ///     .with_repeated_keys(RepeatedKeys::KeepLast);
    /// assert_eq!(dest.path(), "data/corpus.lance");
    /// ```
    pub fn with_repeated_keys(mut self, repeated_keys: RepeatedKeys) -> Self {
        self.repeated_keys = repeated_keys;
        self
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    /// Merge `stream` into the dataset on `merge_keys`.
    ///
    /// The stream is checked for repeated and NULL keys first. A missing
    /// dataset is then created from the checked rows, which are buffered for
    /// that one case so that:
    /// - **an empty first run commits nothing.** Zero rows to merge is not an
    ///   error, and it creates no dataset (an empty one would have no rows to
    ///   take its schema's meaning from, and a reader would be told it exists);
    /// - **a lost creation race becomes a merge.** Two first writers both see
    ///   no dataset; the one whose create loses replays the same rows as a
    ///   merge into the winner's dataset, instead of failing.
    ///
    /// The buffer is one run's output, which `merge_insert` already holds for
    /// its own conflict retry; every later run streams.
    async fn upsert(&self, stream: SendableRecordBatchStream) -> Result<WriteOutcome> {
        if self.merge_keys.is_empty() {
            // The loader and the submit pre-flight both refuse this; reaching
            // it means a destination was built without its keys.
            bail!(
                "upsert into Lance dataset at {} has no merge keys",
                self.path
            );
        }
        if self.repeated_keys == RepeatedKeys::KeepLast {
            return self.upsert_keeping_last(stream).await;
        }
        let checked = reject_repeated_keys(stream, &self.merge_keys)
            .with_context(|| format!("Invalid merge keys for Lance dataset at {}", self.path))?;
        if lance_dataset_exists_at(&self.path).await? {
            return self.merge(checked).await;
        }

        let schema = checked.schema();
        let batches: Vec<RecordBatch> = checked.try_collect().await.with_context(|| {
            format!("Failed to read the rows for Lance dataset at {}", self.path)
        })?;
        self.create_or_merge(schema, batches).await
    }

    /// [`RepeatedKeys::KeepLast`]: buffer the run, keep each key's last row,
    /// merge those, and report every row the query produced.
    async fn upsert_keeping_last(&self, stream: SendableRecordBatchStream) -> Result<WriteOutcome> {
        let schema = stream.schema();
        let batches: Vec<RecordBatch> = stream.try_collect().await.with_context(|| {
            format!("Failed to read the rows for Lance dataset at {}", self.path)
        })?;
        let produced: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let kept = keep_last_per_key(&schema, batches, &self.merge_keys)
            .with_context(|| format!("Invalid merge keys for Lance dataset at {}", self.path))?;
        let mut outcome = if lance_dataset_exists_at(&self.path).await? {
            let replay: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                futures::stream::iter(kept.into_iter().map(Ok)),
            ));
            self.merge(replay).await?
        } else {
            self.create_or_merge(schema, kept).await?
        };
        // Every row the query produced, superseded copies included: the
        // count a caller compares against its LIMIT to tell a full page.
        outcome.rows_written = produced;
        Ok(outcome)
    }

    /// Create the dataset from `batches`, or, when another writer created it
    /// since this run looked, merge them into that writer's dataset.
    async fn create_or_merge(
        &self,
        schema: arrow::datatypes::SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<WriteOutcome> {
        if batches.iter().all(|batch| batch.num_rows() == 0) {
            return Ok(WriteOutcome {
                rows_written: 0,
                snapshot_id: None,
            });
        }
        let replay = |batches: &[RecordBatch]| -> SendableRecordBatchStream {
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                futures::stream::iter(batches.to_vec().into_iter().map(Ok)),
            ))
        };
        match write_lance_stream(&self.path, replay(&batches), WriteMode::Create).await {
            Ok(outcome) => Ok(WriteOutcome {
                rows_written: outcome.rows_written,
                snapshot_id: Some(outcome.version.to_string()),
            }),
            // Another writer created it between the check and the commit.
            Err(error) => {
                if lance_dataset_exists_at(&self.path).await? {
                    self.merge(replay(&batches)).await
                } else {
                    Err(error)
                }
            }
        }
    }

    /// `merge_insert` the checked rows into the existing dataset.
    async fn merge(&self, checked: SendableRecordBatchStream) -> Result<WriteOutcome> {
        let dataset = Arc::new(
            Dataset::open(&self.path)
                .await
                .with_context(|| format!("Failed to open Lance dataset at {}", self.path))?,
        );
        let mut builder = MergeInsertBuilder::try_new(dataset, self.merge_keys.clone())
            .with_context(|| format!("Invalid merge keys for Lance dataset at {}", self.path))?;
        builder
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll);
        let (merged, stats) = builder
            .try_build()
            .context("Failed to build the Lance merge")?
            .execute(checked)
            .await
            .with_context(|| format!("Failed to merge into Lance dataset at {}", self.path))?;
        Ok(WriteOutcome {
            rows_written: stats.num_inserted_rows + stats.num_updated_rows,
            snapshot_id: Some(merged.version().version.to_string()),
        })
    }
}

/// `batches` with only the LAST row of each repeated `keys` value, in their
/// original order otherwise. A NULL key is refused, as in
/// [`reject_repeated_keys`], for the same reason.
fn keep_last_per_key(
    schema: &arrow::datatypes::SchemaRef,
    batches: Vec<RecordBatch>,
    keys: &[String],
) -> DFResult<Vec<RecordBatch>> {
    let indices = keys
        .iter()
        .map(|key| schema.index_of(key))
        .collect::<Result<Vec<usize>, _>>()?;
    let converter = RowConverter::new(
        indices
            .iter()
            .map(|&i| SortField::new(schema.field(i).data_type().clone()))
            .collect(),
    )?;
    let mut encoded = Vec::with_capacity(batches.len());
    for batch in &batches {
        let columns: Vec<ArrayRef> = indices
            .iter()
            .map(|&i| Arc::clone(batch.column(i)))
            .collect();
        for (name, column) in keys.iter().zip(&columns) {
            if column.null_count() > 0 {
                return Err(DataFusionError::Execution(format!(
                    "merge key column '{name}' is NULL in this run's output; an upsert \
                     cannot match a NULL key, so the write is refused"
                )));
            }
        }
        encoded.push(converter.convert_columns(&columns)?);
    }
    // Walk from the end: the first time a key is seen there is its last row.
    let mut seen: HashSet<OwnedRow> = HashSet::new();
    let mut keep: Vec<Vec<bool>> = encoded
        .iter()
        .map(|rows| vec![false; rows.num_rows()])
        .collect();
    for (b, rows) in encoded.iter().enumerate().rev() {
        for r in (0..rows.num_rows()).rev() {
            keep[b][r] = seen.insert(rows.row(r).owned());
        }
    }
    batches
        .into_iter()
        .zip(keep)
        .map(|(batch, mask)| {
            arrow::compute::filter_record_batch(&batch, &arrow::array::BooleanArray::from(mask))
                .map_err(DataFusionError::from)
        })
        .collect()
}

/// `stream`, failing at the first row whose `keys` repeat an earlier row's,
/// or hold a NULL.
///
/// A NULL is refused rather than merged: `merge_insert` matches keys with SQL
/// equality, under which NULL never equals NULL, so a row with a NULL key
/// would be inserted again on every run instead of replacing its copy.
///
/// Every key seen in the run is held in memory until the stream ends, so the
/// cost is one encoded key per output row: bounded by what one run emits, not
/// by the dataset.
fn reject_repeated_keys(
    stream: SendableRecordBatchStream,
    keys: &[String],
) -> DFResult<SendableRecordBatchStream> {
    let schema = stream.schema();
    let indices = keys
        .iter()
        .map(|key| schema.index_of(key))
        .collect::<Result<Vec<usize>, _>>()?;
    let converter = RowConverter::new(
        indices
            .iter()
            .map(|&i| SortField::new(schema.field(i).data_type().clone()))
            .collect(),
    )?;
    let keys = keys.to_vec();
    let mut seen: HashSet<OwnedRow> = HashSet::new();
    let checked = stream.map(move |batch| {
        let batch = batch?;
        let columns: Vec<ArrayRef> = indices
            .iter()
            .map(|&i| Arc::clone(batch.column(i)))
            .collect();
        for (name, column) in keys.iter().zip(&columns) {
            if column.null_count() > 0 {
                return Err(DataFusionError::Execution(format!(
                    "merge key column '{name}' is NULL in this run's output; an upsert \
                     cannot match a NULL key, so the write is refused"
                )));
            }
        }
        let rows = converter.convert_columns(&columns)?;
        for (n, row) in rows.iter().enumerate() {
            if !seen.insert(row.owned()) {
                let shown = keys
                    .iter()
                    .zip(&columns)
                    .map(|(name, column)| {
                        let value = array_value_to_string(column.as_ref(), n)
                            .unwrap_or_else(|_| "<unprintable>".to_string());
                        format!("{name}={value}")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(DataFusionError::Execution(format!(
                    "merge key ({shown}) appears more than once in this run's output; an \
                     upsert keeps one row per key, so the write is refused"
                )));
            }
        }
        Ok(batch)
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, checked)))
}

#[async_trait]
impl JobDestination for LanceDestination {
    fn kind(&self) -> JobDestinationKind {
        JobDestinationKind::Lake
    }

    async fn exists(&self) -> Result<bool> {
        lance_dataset_exists_at(&self.path).await
    }

    async fn schema(&self) -> Result<Option<Arc<Schema>>> {
        if !lance_dataset_exists_at(&self.path).await? {
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
        // Overwrite is rejected at YAML load.
        if mode == DestinationMode::Upsert {
            return self.upsert(stream).await;
        }
        let write_mode = if lance_dataset_exists_at(&self.path).await? {
            WriteMode::Append
        } else {
            WriteMode::Create
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
    use crate::sources::providers::lance::lance_dataset_exists;
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

    fn rows(ids: &[i64], names: &[&str]) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    /// Every (id, name) in the dataset's newest version, sorted by id.
    async fn contents(path: &str) -> Vec<(i64, String)> {
        let batch = Dataset::open(path)
            .await
            .unwrap()
            .scan()
            .try_into_batch()
            .await
            .unwrap();
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut out: Vec<(i64, String)> = (0..batch.num_rows())
            .map(|i| (ids.value(i), names.value(i).to_string()))
            .collect();
        out.sort();
        out
    }

    fn upserting(path: &str) -> LanceDestination {
        LanceDestination::new(path.to_string()).with_merge_keys(vec!["id".to_string()])
    }

    #[tokio::test]
    async fn upsert_creates_then_replaces_matches_and_inserts_the_rest() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("up.lance");
        let path = path.to_str().unwrap();
        let dest = upserting(path);

        let first = rows(&[1, 2], &["a", "b"]);
        let schema = first.schema();
        let out = dest
            .write(vec_to_stream(vec![first], schema), DestinationMode::Upsert)
            .await
            .unwrap();
        assert_eq!(out.rows_written, 2);

        // id 2 is rewritten, id 3 is new, id 1 is untouched.
        let second = rows(&[2, 3], &["B", "c"]);
        let schema = second.schema();
        let out2 = dest
            .write(vec_to_stream(vec![second], schema), DestinationMode::Upsert)
            .await
            .unwrap();
        assert_eq!(out2.rows_written, 2, "one update and one insert");
        assert_ne!(out.snapshot_id, out2.snapshot_id);
        assert_eq!(
            contents(path).await,
            vec![(1, "a".into()), (2, "B".into()), (3, "c".into())]
        );
    }

    #[tokio::test]
    async fn upsert_matches_by_column_name_not_position() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("order.lance");
        let path = path.to_str().unwrap();
        let dest = upserting(path);
        let first = rows(&[1], &["a"]);
        let schema = first.schema();
        dest.write(vec_to_stream(vec![first], schema), DestinationMode::Upsert)
            .await
            .unwrap();

        // The same columns, the other way round — the executor's pre-flight
        // compares schemas by name, so this is a shape a job can send.
        let swapped_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("id", DataType::Int64, false),
        ]));
        let swapped = RecordBatch::try_new(
            Arc::clone(&swapped_schema),
            vec![
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        dest.write(
            vec_to_stream(vec![swapped], swapped_schema),
            DestinationMode::Upsert,
        )
        .await
        .unwrap();
        assert_eq!(contents(path).await, vec![(1, "A".into())]);
    }

    #[tokio::test]
    async fn upsert_refuses_a_key_repeated_in_one_run_and_commits_nothing() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("dup.lance");
        let path = path.to_str().unwrap();
        let dest = upserting(path);
        let first = rows(&[1], &["a"]);
        let schema = first.schema();
        dest.write(vec_to_stream(vec![first], schema), DestinationMode::Upsert)
            .await
            .unwrap();

        let dup = rows(&[1, 1], &["x", "y"]);
        let schema = dup.schema();
        dest.write(vec_to_stream(vec![dup], schema), DestinationMode::Upsert)
            .await
            .expect_err("a key twice in one run must not pick a winner");
        assert_eq!(contents(path).await, vec![(1, "a".into())]);
    }

    #[tokio::test]
    async fn a_new_key_repeated_in_one_run_is_refused_too() {
        // The case Lance's own duplicate check misses: key 5 is not in the
        // dataset yet, so neither copy MATCHES anything, and merge_insert
        // alone would insert both.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("new-dup.lance");
        let path = path.to_str().unwrap();
        let dest = upserting(path);
        let first = rows(&[1], &["a"]);
        let schema = first.schema();
        dest.write(vec_to_stream(vec![first], schema), DestinationMode::Upsert)
            .await
            .unwrap();

        let dup = rows(&[5, 5], &["x", "y"]);
        let schema = dup.schema();
        let err = dest
            .write(vec_to_stream(vec![dup], schema), DestinationMode::Upsert)
            .await
            .expect_err("a key twice in one run must not pick a winner");
        assert!(
            format!("{err:#}").contains("(id=5)"),
            "names the key: {err:#}"
        );
        assert_eq!(contents(path).await, vec![(1, "a".into())]);
    }

    #[tokio::test]
    async fn a_repeated_key_across_batches_is_caught() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("batches-dup.lance");
        let path = path.to_str().unwrap();
        let dest = upserting(path);
        let one = rows(&[1, 2], &["a", "b"]);
        let two = rows(&[3, 2], &["c", "again"]);
        let schema = one.schema();
        dest.write(
            vec_to_stream(vec![one, two], schema),
            DestinationMode::Upsert,
        )
        .await
        .expect_err("the repeat is in a later batch");
        assert!(
            !dest.exists().await.unwrap(),
            "a first run that fails leaves no dataset"
        );
    }

    #[tokio::test]
    async fn a_cancelled_upsert_leaves_the_previous_version() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("cancel-up.lance");
        let path = path.to_str().unwrap();
        let dest = upserting(path);
        let first = rows(&[1], &["a"]);
        let schema = first.schema();
        dest.write(vec_to_stream(vec![first], schema), DestinationMode::Upsert)
            .await
            .unwrap();

        let next = rows(&[1, 2], &["changed", "new"]);
        let schema = next.schema();
        let flag = Arc::new(AtomicBool::new(true));
        let stream = CancellableStream::new(vec_to_stream(vec![next], schema), flag).boxed();
        dest.write(stream, DestinationMode::Upsert)
            .await
            .expect_err("cancelled stream should fail the merge");
        assert_eq!(contents(path).await, vec![(1, "a".into())]);
    }

    /// SQL equality never matches NULL, so a NULL key would be inserted again
    /// every run. Refused, naming the column, and nothing is committed.
    #[tokio::test]
    async fn a_null_merge_key_is_refused_rather_than_duplicated() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("null.lance");
        let path = path.to_str().unwrap();
        let dest = LanceDestination::new(path.to_string())
            .with_merge_keys(vec!["id".to_string(), "name".to_string()]);
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let with_null = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![None::<&str>])),
            ],
        )
        .unwrap();
        let err = dest
            .write(
                vec_to_stream(vec![with_null], schema),
                DestinationMode::Upsert,
            )
            .await
            .expect_err("a NULL key cannot be matched");
        assert!(format!("{err:#}").contains("'name' is NULL"), "{err:#}");
        assert!(!lance_dataset_exists(path), "nothing is committed");
    }

    /// Zero rows to merge is not an error, and a first run with none leaves
    /// no dataset behind; the next run with rows creates it.
    #[tokio::test]
    async fn an_empty_first_upsert_succeeds_and_creates_nothing() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("empty.lance");
        let path = path.to_str().unwrap();
        let dest = upserting(path);
        let none = rows(&[], &[]);
        let schema = none.schema();
        let out = dest
            .write(vec_to_stream(vec![none], schema), DestinationMode::Upsert)
            .await
            .expect("an empty upsert is a no-op");
        assert_eq!(out.rows_written, 0);
        assert_eq!(out.snapshot_id, None);
        assert!(!lance_dataset_exists(path));

        let first = rows(&[1], &["a"]);
        let schema = first.schema();
        dest.write(vec_to_stream(vec![first], schema), DestinationMode::Upsert)
            .await
            .unwrap();
        assert_eq!(contents(path).await, vec![(1, "a".into())]);
    }

    /// A first run whose create finds the dataset already there (another
    /// first writer committed between its check and its create) merges into
    /// it instead of failing. Driven directly: a real race on one process's
    /// local filesystem is timing, and Lance's guarantee that two creates
    /// conflict is the object store's conditional commit, not this test's.
    #[tokio::test]
    async fn a_create_that_finds_the_dataset_created_meanwhile_merges() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("race.lance");
        let path = path.to_str().unwrap().to_string();
        let winner = rows(&[1, 2], &["a", "b"]);
        let schema = winner.schema();
        upserting(&path)
            .write(vec_to_stream(vec![winner], schema), DestinationMode::Upsert)
            .await
            .unwrap();

        let late = rows(&[2, 3], &["B", "c"]);
        let out = upserting(&path)
            .create_or_merge(late.schema(), vec![late])
            .await
            .expect("the late creator merges");
        assert_eq!(out.rows_written, 2);
        assert_eq!(
            contents(&path).await,
            vec![(1, "a".into()), (2, "B".into()), (3, "c".into())]
        );
    }

    /// keep_last: a key repeated in one run (here across batches) keeps its
    /// LAST row, and the reported count is every row the query produced, so a
    /// caller comparing it with a LIMIT still sees a full page as full.
    #[tokio::test]
    async fn keep_last_keeps_the_last_copy_and_counts_every_row() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("keep.lance");
        let path = path.to_str().unwrap();
        let dest = upserting(path).with_repeated_keys(RepeatedKeys::KeepLast);
        let first = rows(&[1, 2], &["a", "old"]);
        let second = rows(&[2, 3], &["new", "c"]);
        let schema = first.schema();
        let out = dest
            .write(
                vec_to_stream(vec![first, second], schema),
                DestinationMode::Upsert,
            )
            .await
            .expect("a repeat is kept, not refused");
        assert_eq!(out.rows_written, 4, "every row the query produced");
        assert_eq!(
            contents(path).await,
            vec![(1, "a".into()), (2, "new".into()), (3, "c".into())]
        );

        // Into the existing dataset too.
        let again = rows(&[3, 3], &["x", "y"]);
        let schema = again.schema();
        dest.write(vec_to_stream(vec![again], schema), DestinationMode::Upsert)
            .await
            .unwrap();
        assert_eq!(
            contents(path).await,
            vec![(1, "a".into()), (2, "new".into()), (3, "y".into())]
        );
    }

    #[tokio::test]
    async fn upsert_without_merge_keys_is_refused() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("nokeys.lance");
        let dest = LanceDestination::new(path.to_str().unwrap().to_string());
        let batch = sample_batch();
        let schema = batch.schema();
        let err = dest
            .write(vec_to_stream(vec![batch], schema), DestinationMode::Upsert)
            .await
            .expect_err("no keys, no merge");
        assert!(err.to_string().contains("no merge keys"), "{err}");
        assert!(!dest.exists().await.unwrap(), "nothing is created first");
    }
}
