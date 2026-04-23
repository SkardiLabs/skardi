//! End-to-end tests for the pipeline loader and execution path:
//! write a YAML to disk → load through `StandardPipeline::load_from_file`
//! → execute against DataFusion → verify the result.
//!
//! Companion to `jobs_e2e.rs` on the pipeline side. Covers the envelope
//! contract (`kind: pipeline` required, wrong kind rejected, metadata/spec
//! surfaces wired through) and the happy path through the SQL engine.

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use skardi::pipeline::pipeline::{Pipeline, StandardPipeline};
use std::io::Write;
use std::sync::Arc;
use tempfile::TempDir;

fn write_yaml(path: &std::path::Path, content: &str) {
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(content.as_bytes()).unwrap();
}

/// `orders` fact table keyed on user_id, used by the federated test below
/// and the single-source happy path.
fn orders_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![101i64, 102, 103, 104])),
            Arc::new(Int64Array::from(vec![1i64, 1, 2, 3])),
            Arc::new(Int64Array::from(vec![50i64, 75, 200, 10])),
        ],
    )
    .unwrap()
}

/// `users` dimension table — joined with `orders` in the federated test.
fn users_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
        ],
    )
    .unwrap()
}

// ---------------------------------------------------------------------------
// Happy path: load from disk, then execute against a registered MemTable.
// Confirms the envelope round-trips through the loader and that inferred
// placeholders bind correctly when substituted into the SQL.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pipeline_loads_from_yaml_and_executes() {
    let tmp = TempDir::new().unwrap();
    let yaml_path = tmp.path().join("orders-by-user.yaml");
    write_yaml(
        &yaml_path,
        r#"
kind: pipeline
metadata:
  name: "orders-by-user"
  version: "1.0.0"
  description: "Total spend per user, filtered by user_id."
spec:
  query: |
    SELECT user_id, SUM(amount) AS total
    FROM orders
    WHERE user_id = {user_id}
    GROUP BY user_id
"#,
    );

    let ctx = Arc::new(SessionContext::new());
    ctx.register_batch("orders", orders_batch()).unwrap();

    let pipeline = StandardPipeline::load_from_file(&yaml_path, Arc::clone(&ctx))
        .await
        .expect("pipeline should load");
    assert_eq!(pipeline.name(), "orders-by-user");
    assert_eq!(pipeline.version(), "1.0.0");

    // Inferred request schema surfaces the `{user_id}` placeholder.
    let param_names: Vec<&String> = pipeline.request_schema().fields.keys().collect();
    assert_eq!(param_names, vec!["user_id"]);

    // Inline-substitute and execute. User 1 has orders 101+102 = 125.
    let sql = pipeline.query_definition().sql.replace("{user_id}", "1");
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let total = batches[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 125);
}

// ---------------------------------------------------------------------------
// Envelope validation: missing `kind:` at root is rejected with a message
// that points the user at the required shape.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pipeline_rejects_yaml_without_kind() {
    let tmp = TempDir::new().unwrap();
    let yaml_path = tmp.path().join("no-kind.yaml");
    write_yaml(
        &yaml_path,
        r#"
metadata:
  name: "p1"
  version: "1.0.0"
spec:
  query: "SELECT 1"
"#,
    );
    let ctx = Arc::new(SessionContext::new());
    let err = StandardPipeline::load_from_file(&yaml_path, ctx)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.to_lowercase().contains("kind"),
        "error should mention the missing kind, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Envelope validation: `kind: context` is not a pipeline — the loader must
// reject it rather than ignore the mismatch.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pipeline_rejects_wrong_kind() {
    let tmp = TempDir::new().unwrap();
    let yaml_path = tmp.path().join("wrong-kind.yaml");
    write_yaml(
        &yaml_path,
        r#"
kind: context
metadata:
  name: "ctx"
  version: "1.0.0"
spec:
  query: "SELECT 1"
"#,
    );
    let ctx = Arc::new(SessionContext::new());
    let err = StandardPipeline::load_from_file(&yaml_path, ctx)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.to_lowercase().contains("kind") && err.to_lowercase().contains("context"),
        "error should name the wrong kind, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Envelope validation: `spec:` is required. A pre-envelope pipeline (query
// at the root) must fail rather than silently no-op.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pipeline_rejects_legacy_flat_shape() {
    let tmp = TempDir::new().unwrap();
    let yaml_path = tmp.path().join("flat.yaml");
    write_yaml(
        &yaml_path,
        r#"
kind: pipeline
metadata:
  name: "legacy"
  version: "1.0.0"
query: "SELECT 1"
"#,
    );
    let ctx = Arc::new(SessionContext::new());
    let err = StandardPipeline::load_from_file(&yaml_path, ctx)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.to_lowercase().contains("spec"),
        "error should name the missing spec block, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Federated-style pipeline: a JOIN across two registered tables still
// loads and executes end-to-end. Less a feature test than a smoke test
// that the loader's inferred schemas handle multi-source SQL.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pipeline_executes_federated_join() {
    let tmp = TempDir::new().unwrap();
    let yaml_path = tmp.path().join("top-spenders.yaml");
    write_yaml(
        &yaml_path,
        r#"
kind: pipeline
metadata:
  name: "top-spenders"
  version: "1.0.0"
  description: "Users whose total spend exceeds a threshold."
spec:
  query: |
    SELECT u.name, SUM(o.amount) AS total
    FROM users u
    JOIN orders o ON o.user_id = u.user_id
    GROUP BY u.name
    HAVING SUM(o.amount) > {min_total}
    ORDER BY total DESC
"#,
    );

    let ctx = Arc::new(SessionContext::new());
    ctx.register_batch("orders", orders_batch()).unwrap();
    ctx.register_batch("users", users_batch()).unwrap();

    let pipeline = StandardPipeline::load_from_file(&yaml_path, Arc::clone(&ctx))
        .await
        .expect("federated pipeline should load");
    let sql = pipeline
        .query_definition()
        .sql
        .replace("{min_total}", "100");
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    assert_eq!(batches.len(), 1);

    let names = batches[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let totals = batches[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    // Alice has 125, Bob has 200 — both > 100; Carol has 10 and is filtered.
    // Bob's 200 > Alice's 125, so Bob sorts first.
    assert_eq!(names.len(), 2);
    assert_eq!(names.value(0), "Bob");
    assert_eq!(totals.value(0), 200);
    assert_eq!(names.value(1), "Alice");
    assert_eq!(totals.value(1), 125);
}
