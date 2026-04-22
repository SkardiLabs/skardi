//! End-to-end integration tests for the jobs primitive.
//!
//! These exercise the full pipeline: load job YAMLs, set up a
//! `SessionContext` with a source table, run the executor against a
//! Lance destination on disk, and verify lifecycle state in the run
//! ledger. This is the "happy path" the design doc calls out as the
//! primary MVP acceptance test.

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use skardi::jobs::{JobDefinition, JobExecutor, JobRunStatus, JobStore, SqliteJobStore};
use skardi::sources::DataSourceType;
use skardi::sources::providers::lance::{lance_dataset_exists, register_lance_table};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

async fn wait_for_terminal(exec: &JobExecutor, run_id: &str) -> skardi::jobs::JobRun {
    for _ in 0..200 {
        if let Some(run) = exec.store().get_run(run_id).await.unwrap() {
            if run.status.is_terminal() {
                return run;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("run {run_id} never reached terminal state");
}

fn source_batch(count: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from((1..=count).collect::<Vec<i64>>())),
            Arc::new(StringArray::from(
                (1..=count)
                    .map(|i| Some(format!("row-{i}")))
                    .collect::<Vec<Option<String>>>(),
            )),
        ],
    )
    .unwrap()
}

fn write_yaml(path: &std::path::Path, content: &str) {
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(content.as_bytes()).unwrap();
}

async fn build_executor_for_lance(
    ctx: Arc<SessionContext>,
    job: JobDefinition,
    lance_name: &str,
    lance_path: &str,
) -> Arc<JobExecutor> {
    let mut map = HashMap::new();
    map.insert(job.name().to_string(), job);
    let store = Arc::new(SqliteJobStore::open_in_memory().await.unwrap());

    let mut data_source_types = HashMap::new();
    data_source_types.insert(lance_name.to_string(), DataSourceType::Lance);

    let mut source_paths = HashMap::new();
    source_paths.insert(lance_name.to_string(), lance_path.to_string());

    Arc::new(JobExecutor::new(
        map,
        store as Arc<dyn skardi::jobs::JobStore>,
        ctx,
        data_source_types,
        source_paths,
    ))
}

// ---------------------------------------------------------------------------
// Happy path: first run creates dataset, second run appends, Lance sees both.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_creates_then_appends_lance_dataset() {
    let tmp = TempDir::new().unwrap();
    let lake_path = tmp.path().join("wiki_lake.lance");
    let yaml_path = tmp.path().join("ingest.yaml");
    write_yaml(
        &yaml_path,
        r#"
kind: job
metadata:
  name: "ingest-lake"
  version: "1.0.0"
query: |
  SELECT id, name FROM src WHERE id >= {min_id}
destination:
  table: "wiki_lake"
  mode: append
  create_if_missing: true
"#,
    );

    let ctx = Arc::new(SessionContext::new());
    ctx.register_batch("src", source_batch(5)).unwrap();
    let job = JobDefinition::load_from_file(&yaml_path, Arc::clone(&ctx))
        .await
        .unwrap()
        .unwrap();

    let exec = build_executor_for_lance(
        Arc::clone(&ctx),
        job,
        "wiki_lake",
        lake_path.to_str().unwrap(),
    )
    .await;

    // Submit #1 — dataset does not exist yet.
    assert!(!lance_dataset_exists(lake_path.to_str().unwrap()));
    let mut params = HashMap::new();
    params.insert(
        "min_id".to_string(),
        serde_json::Value::Number(serde_json::Number::from(0)),
    );
    let run_id = exec.submit("ingest-lake", params.clone()).await.unwrap();
    let run = wait_for_terminal(&exec, &run_id).await;
    assert_eq!(
        run.status,
        JobRunStatus::Succeeded,
        "error: {:?}",
        run.error
    );
    assert_eq!(run.rows_written, Some(5));
    assert!(lance_dataset_exists(lake_path.to_str().unwrap()));

    // Submit #2 — append mode should double the row count.
    let run_id = exec.submit("ingest-lake", params).await.unwrap();
    let run = wait_for_terminal(&exec, &run_id).await;
    assert_eq!(
        run.status,
        JobRunStatus::Succeeded,
        "error: {:?}",
        run.error
    );
    assert_eq!(run.rows_written, Some(5));

    // Register the resulting dataset and query it.
    let mut ctx2 = SessionContext::new();
    register_lance_table(&mut ctx2, "wiki_lake", lake_path.to_str().unwrap(), None)
        .await
        .unwrap();
    let count = ctx2
        .sql("SELECT COUNT(id) AS n FROM wiki_lake")
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
    assert_eq!(count, 10, "append should accumulate rows across runs");
}

// ---------------------------------------------------------------------------
// Overwrite replaces rows atomically.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_overwrite_replaces_lance_dataset() {
    let tmp = TempDir::new().unwrap();
    let lake_path = tmp.path().join("ow.lance");

    // Seed the dataset with a first `append` run.
    let append_yaml = tmp.path().join("append.yaml");
    write_yaml(
        &append_yaml,
        r#"
kind: job
metadata:
  name: "seed"
  version: "1.0.0"
query: |
  SELECT id, name FROM src
destination:
  table: "t"
  mode: append
"#,
    );
    let overwrite_yaml = tmp.path().join("overwrite.yaml");
    write_yaml(
        &overwrite_yaml,
        r#"
kind: job
metadata:
  name: "replace"
  version: "1.0.0"
query: |
  SELECT id, name FROM src WHERE id > {cutoff}
destination:
  table: "t"
  mode: overwrite
"#,
    );

    let ctx = Arc::new(SessionContext::new());
    ctx.register_batch("src", source_batch(10)).unwrap();

    let seed_job = JobDefinition::load_from_file(&append_yaml, Arc::clone(&ctx))
        .await
        .unwrap()
        .unwrap();
    let replace_job = JobDefinition::load_from_file(&overwrite_yaml, Arc::clone(&ctx))
        .await
        .unwrap()
        .unwrap();

    let mut data_source_types = HashMap::new();
    data_source_types.insert("t".to_string(), DataSourceType::Lance);
    let mut source_paths = HashMap::new();
    source_paths.insert("t".to_string(), lake_path.to_str().unwrap().to_string());
    let store = Arc::new(SqliteJobStore::open_in_memory().await.unwrap());
    let mut jobs = HashMap::new();
    jobs.insert(seed_job.name().to_string(), seed_job);
    jobs.insert(replace_job.name().to_string(), replace_job);
    let exec = Arc::new(JobExecutor::new(
        jobs,
        store as Arc<dyn skardi::jobs::JobStore>,
        Arc::clone(&ctx),
        data_source_types,
        source_paths,
    ));

    // Seed with 10 rows.
    let run_id = exec.submit("seed", HashMap::new()).await.unwrap();
    let run = wait_for_terminal(&exec, &run_id).await;
    assert_eq!(run.status, JobRunStatus::Succeeded);
    assert_eq!(run.rows_written, Some(10));

    // Overwrite keeping only id > 7 — should result in exactly 3 rows.
    let mut params = HashMap::new();
    params.insert(
        "cutoff".to_string(),
        serde_json::Value::Number(serde_json::Number::from(7)),
    );
    let run_id = exec.submit("replace", params).await.unwrap();
    let run = wait_for_terminal(&exec, &run_id).await;
    assert_eq!(
        run.status,
        JobRunStatus::Succeeded,
        "error: {:?}",
        run.error
    );

    let mut ctx2 = SessionContext::new();
    register_lance_table(&mut ctx2, "t", lake_path.to_str().unwrap(), None)
        .await
        .unwrap();
    let count = ctx2
        .sql("SELECT COUNT(id) AS n FROM t")
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
    assert_eq!(count, 3, "overwrite must replace — not accumulate");
}

// ---------------------------------------------------------------------------
// Schema mismatch is rejected at submit time.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn submit_rejects_schema_mismatch_for_existing_lance_dataset() {
    let tmp = TempDir::new().unwrap();
    let lake_path = tmp.path().join("wiki_lake.lance");

    // Seed the Lance dataset via the write helper — gives it a known
    // schema (id:Int64, name:Utf8) that the next submit will not match.
    skardi::sources::providers::lance::write_lance_dataset(
        lake_path.to_str().unwrap(),
        vec![source_batch(2)],
        lance::dataset::WriteMode::Create,
    )
    .await
    .unwrap();

    let yaml_path = tmp.path().join("mismatch.yaml");
    // Query produces a column `id_wrong` that doesn't exist in the
    // destination → pre-flight must reject.
    write_yaml(
        &yaml_path,
        r#"
kind: job
metadata:
  name: "mismatch"
  version: "1.0.0"
query: |
  SELECT id AS id_wrong, name FROM src
destination:
  table: "wiki_lake"
  mode: append
"#,
    );

    let ctx = Arc::new(SessionContext::new());
    ctx.register_batch("src", source_batch(1)).unwrap();
    let job = JobDefinition::load_from_file(&yaml_path, Arc::clone(&ctx))
        .await
        .unwrap()
        .unwrap();

    let exec = build_executor_for_lance(
        Arc::clone(&ctx),
        job,
        "wiki_lake",
        lake_path.to_str().unwrap(),
    )
    .await;
    let err = exec.submit("mismatch", HashMap::new()).await.unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("schema mismatch") || err_str.contains("Destination schema mismatch"),
        "got {err_str}"
    );

    // No ledger row should have been created.
    let runs = exec.store().list_runs(None, 100).await.unwrap();
    assert!(
        runs.is_empty(),
        "schema-rejected submit must not persist a run"
    );
}

// ---------------------------------------------------------------------------
// DB destination must reject a missing table, regardless of
// create_if_missing.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn submit_rejects_missing_db_destination() {
    let tmp = TempDir::new().unwrap();
    let yaml_path = tmp.path().join("db.yaml");
    // `create_if_missing: true` is set but must be ignored for DB
    // destinations per the design doc.
    write_yaml(
        &yaml_path,
        r#"
kind: job
metadata:
  name: "db-ingest"
  version: "1.0.0"
query: |
  SELECT id FROM src
destination:
  table: "missing.public.never_exists"
  mode: append
  create_if_missing: true
"#,
    );

    let ctx = Arc::new(SessionContext::new());
    ctx.register_batch("src", source_batch(1)).unwrap();
    let job = JobDefinition::load_from_file(&yaml_path, Arc::clone(&ctx))
        .await
        .unwrap()
        .unwrap();

    let mut jobs = HashMap::new();
    jobs.insert(job.name().to_string(), job);
    let mut data_source_types = HashMap::new();
    data_source_types.insert("missing".to_string(), DataSourceType::Postgres);
    let store = Arc::new(SqliteJobStore::open_in_memory().await.unwrap());
    let exec = JobExecutor::new(
        jobs,
        store as Arc<dyn skardi::jobs::JobStore>,
        ctx,
        data_source_types,
        HashMap::new(),
    );

    let err = exec.submit("db-ingest", HashMap::new()).await.unwrap_err();
    assert!(
        matches!(
            err,
            skardi::jobs::JobSubmitError::DbDestinationMissing { .. }
        ),
        "got {err}"
    );
}

// ---------------------------------------------------------------------------
// Lake destination with create_if_missing: false rejects a fresh path.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn submit_rejects_lake_destination_when_create_if_missing_is_false() {
    let tmp = TempDir::new().unwrap();
    let lake_path = tmp.path().join("does_not_exist_yet.lance");
    let yaml_path = tmp.path().join("strict.yaml");
    write_yaml(
        &yaml_path,
        r#"
kind: job
metadata:
  name: "strict"
  version: "1.0.0"
query: |
  SELECT id FROM src
destination:
  table: "lake"
  mode: append
  create_if_missing: false
"#,
    );

    let ctx = Arc::new(SessionContext::new());
    ctx.register_batch("src", source_batch(1)).unwrap();
    let job = JobDefinition::load_from_file(&yaml_path, Arc::clone(&ctx))
        .await
        .unwrap()
        .unwrap();

    let exec =
        build_executor_for_lance(Arc::clone(&ctx), job, "lake", lake_path.to_str().unwrap()).await;
    let err = exec.submit("strict", HashMap::new()).await.unwrap_err();
    assert!(
        matches!(
            err,
            skardi::jobs::JobSubmitError::LakeDestinationMissing { .. }
        ),
        "got {err}"
    );
}

// ---------------------------------------------------------------------------
// Crash recovery: a run stuck in `running` is reconciled to `failed`
// when the server restarts (= reconcile_orphaned is called again).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn restart_reconciles_orphaned_running_row_to_failed() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("jobs.db");

    {
        let store = SqliteJobStore::open(&db_path).await.unwrap();
        let run = skardi::jobs::JobRun {
            id: "ghost".to_string(),
            job_name: "ingest".to_string(),
            parameters: "{}".to_string(),
            status: JobRunStatus::Running,
            created_at: chrono::Utc::now().to_rfc3339(),
            started_at: Some(chrono::Utc::now().to_rfc3339()),
            finished_at: None,
            rows_written: None,
            snapshot_id: None,
            error: None,
        };
        store.create_run(&run).await.unwrap();
    }

    // Reopen (simulates server restart) and reconcile.
    let store = SqliteJobStore::open(&db_path).await.unwrap();
    let n = store
        .reconcile_orphaned("server restarted before run completed")
        .await
        .unwrap();
    assert_eq!(n, 1);
    let got = store.get_run("ghost").await.unwrap().unwrap();
    assert_eq!(got.status, JobRunStatus::Failed);
    assert!(got.error.as_deref().unwrap().contains("restart"));
    assert!(got.finished_at.is_some());
}

// ---------------------------------------------------------------------------
// DB DML end-to-end via MemTable — confirms INSERT INTO ... SELECT
// reaches a DataFusion `insert_into`-capable provider.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_inserts_into_memtable_destination() {
    let tmp = TempDir::new().unwrap();
    let yaml_path = tmp.path().join("db.yaml");
    write_yaml(
        &yaml_path,
        r#"
kind: job
metadata:
  name: "db-ingest"
  version: "1.0.0"
query: |
  SELECT id FROM src WHERE id >= {min_id}
destination:
  table: "dest"
  mode: append
"#,
    );

    let ctx = Arc::new(SessionContext::new());
    ctx.register_batch("src", source_batch(4)).unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let dest =
        MemTable::try_new(schema.clone(), vec![vec![RecordBatch::new_empty(schema)]]).unwrap();
    ctx.register_table("dest", Arc::new(dest)).unwrap();

    let job = JobDefinition::load_from_file(&yaml_path, Arc::clone(&ctx))
        .await
        .unwrap()
        .unwrap();
    let mut jobs = HashMap::new();
    jobs.insert(job.name().to_string(), job);
    let store = Arc::new(SqliteJobStore::open_in_memory().await.unwrap());
    let exec = JobExecutor::new(
        jobs,
        store as Arc<dyn skardi::jobs::JobStore>,
        Arc::clone(&ctx),
        HashMap::new(),
        HashMap::new(),
    );

    let mut params = HashMap::new();
    params.insert(
        "min_id".to_string(),
        serde_json::Value::Number(serde_json::Number::from(2)),
    );
    let run_id = exec.submit("db-ingest", params).await.unwrap();
    let run = wait_for_terminal(&exec, &run_id).await;
    assert_eq!(
        run.status,
        JobRunStatus::Succeeded,
        "error: {:?}",
        run.error
    );
    assert_eq!(run.rows_written, Some(3));

    let n = ctx
        .sql("SELECT COUNT(*) AS n FROM dest")
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
    assert_eq!(n, 3);
}
