//! Opt-in live tests for Lance datasets in an S3-compatible object store.
//!
//! Disabled by default (`#[ignore]`) and armed by the same variables as
//! `documents_s3_live.rs`, so CI's RustFS step (`-E 'test(/live_s3_/)'`) runs
//! them with no further wiring:
//!
//!   DOCUMENTS_S3_LIVE=1 DOCUMENTS_S3_BUCKET=skardi-test \
//!     AWS_ENDPOINT=http://127.0.0.1:9000 AWS_ALLOW_HTTP=true \
//!     AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
//!     cargo test -p skardi --test lance_s3_live -- --ignored
//!
//! The bucket must already exist. Each test writes under its own prefix and
//! deletes it afterwards.

use std::env;
use std::sync::Arc;
use std::thread;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use skardi::jobs::{DestinationMode, JobDestination, LanceDestination};
use skardi::sources::providers::lance::{lance_dataset_exists_at, register_lance_table};
use tokio::runtime::Handle;
use uuid::Uuid;

/// The bucket, or `None` when the live suite is not armed.
fn bucket() -> Option<String> {
    if env::var("DOCUMENTS_S3_LIVE").as_deref() != Ok("1") {
        eprintln!("skipping: DOCUMENTS_S3_LIVE != 1");
        return None;
    }
    Some(env::var("DOCUMENTS_S3_BUCKET").expect("DOCUMENTS_S3_BUCKET is set"))
}

fn rows(ids: &[i64], names: &[&str]) -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .unwrap();
    (schema, batch)
}

fn stream_of(schema: Arc<Schema>, batch: RecordBatch) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        stream::iter(vec![Ok(batch)]),
    ))
}

/// Deletes everything under `prefix` when dropped, so a failed assertion
/// still cleans up.
struct Cleanup {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        let store = Arc::clone(&self.store);
        let prefix = self.prefix.clone();
        let handle = Handle::current();
        thread::spawn(move || {
            handle.block_on(async move {
                let keys: Vec<_> = store
                    .list(Some(&prefix))
                    .map_ok(|meta| meta.location)
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .filter_map(Result::ok)
                    .collect();
                for key in keys {
                    let _ = store.delete(&key).await;
                }
            })
        })
        .join()
        .ok();
    }
}

fn cleanup(bucket: &str, prefix: &str) -> Cleanup {
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .build()
        .expect("S3 store from AWS_* env");
    Cleanup {
        store: Arc::new(store),
        prefix: ObjectPath::from(prefix),
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn live_s3_lance_upsert_round_trips_through_an_object_store() {
    let Some(bucket) = bucket() else { return };
    let prefix = format!("lance-live/{}", Uuid::new_v4().simple());
    let _cleanup = cleanup(&bucket, &prefix);
    let uri = format!("s3://{bucket}/{prefix}/corpus.lance");

    assert!(
        !lance_dataset_exists_at(&uri).await.unwrap(),
        "a fresh prefix holds no dataset"
    );

    let dest = LanceDestination::new(uri.clone()).with_merge_keys(vec!["id".to_string()]);
    let (schema, first) = rows(&[1, 2], &["a", "b"]);
    let out = dest
        .write(stream_of(schema, first), DestinationMode::Upsert)
        .await
        .expect("first upsert creates the remote dataset");
    assert_eq!(out.rows_written, 2);
    assert!(lance_dataset_exists_at(&uri).await.unwrap());
    assert!(dest.exists().await.unwrap());

    // The second run must MERGE, not try to create over the dataset: this is
    // the call a local-only existence check got wrong for every remote URI.
    let (schema, second) = rows(&[2, 3], &["B", "c"]);
    let out = dest
        .write(stream_of(schema, second), DestinationMode::Upsert)
        .await
        .expect("second upsert merges into the remote dataset");
    assert_eq!(out.rows_written, 2);

    // And a reader registered at the same URI sees the merged rows.
    let mut ctx = SessionContext::new();
    register_lance_table(&mut ctx, "corpus", &uri, None)
        .await
        .expect("a remote dataset registers");
    let batches = ctx
        .sql("SELECT id, name FROM corpus ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut got: Vec<(i64, String)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            got.push((ids.value(i), names.value(i).to_string()));
        }
    }
    assert_eq!(got, vec![(1, "a".into()), (2, "B".into()), (3, "c".into())]);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn live_s3_lance_append_creates_then_appends() {
    let Some(bucket) = bucket() else { return };
    let prefix = format!("lance-live/{}", Uuid::new_v4().simple());
    let _cleanup = cleanup(&bucket, &prefix);
    let uri = format!("s3://{bucket}/{prefix}/log.lance");

    let dest = LanceDestination::new(uri.clone());
    for _ in 0..2 {
        let (schema, batch) = rows(&[1], &["a"]);
        dest.write(stream_of(schema, batch), DestinationMode::Append)
            .await
            .expect("append to a remote dataset");
    }
    let mut ctx = SessionContext::new();
    register_lance_table(&mut ctx, "log", &uri, None)
        .await
        .unwrap();
    let n = ctx
        .sql("SELECT count(*) FROM log")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 2, "the second run appended rather than re-creating");
}
