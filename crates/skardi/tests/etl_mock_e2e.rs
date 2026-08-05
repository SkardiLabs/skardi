//! The mock hybrid-search e2e (spec 1d.3) — the repo's first automated
//! end-to-end hybrid path:
//!
//! generate → `setup` against a real SQLite file (fts5 + vec0 + triggers)
//! → run the generated ingest job through the REAL job executor (positional
//! write, preflight and all) → execute the generated search and
//! get-document pipelines → assert RRF-ranked rows and ordered-chunk
//! reassembly. Read-time `doc_id` dedup is pinned by double-ingesting.
//!
//! Two substitutions, on purpose:
//! - The mock SOURCE is a MemTable with the pack's exact FieldMapping
//!   schema instead of an Open Connector gateway — the OC read path has
//!   its own live/pack test suites; this test owns everything from the
//!   generated SQL down.
//! - `candle` is a deterministic fake with the real UDF's signature and
//!   return type (`List<Float32>`) — model inference is the embedding
//!   suite's job; determinism is what ingest-vs-query consistency needs.
//!
//! Requires the sqlite-vec extension: set `SQLITE_VEC_PATH` to the vec0
//! loadable (e.g. from the `sqlite-vec` pip wheel). Without it the test
//! SKIPS (prints why) rather than failing machines that lack it.

#![cfg(feature = "chunking")]

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, Float32Builder, Float64Array, ListBuilder, RecordBatch, StringArray, StringBuilder,
    TimestampMillisecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
};
use datafusion::datasource::MemTable;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use serde_json::json;
use skardi::etl::EtlConfig;
use skardi::etl::validate::generate_hybrid;
use skardi::jobs::definition::JobDefinition;
use skardi::jobs::executor::JobExecutor;
use skardi::jobs::store::{JobRunStatus, JobStore, SqliteJobStore};
use skardi::model::chunking::ChunkingRegistry;
use skardi::sources::DataSourceType;
use skardi::sources::hierarchy::HierarchyLevel;
use skardi::sources::providers::DatasetRegistry;
use skardi::sources::providers::sqlite::fts_table_function::register_sqlite_fts_udtf;
use skardi::sources::providers::sqlite::knn_table_function::register_sqlite_knn_udtf;
use skardi::sources::providers::sqlite::register_sqlite_tables;
use skardi::sources::providers::sqlite::vec_to_binary::register_vec_to_binary_udf;
use skardi::util::json_pack::register_json_pack_udf;
use tokio_rusqlite::rusqlite;

const DIMS: usize = 8;

/// Deterministic stand-in for `candle`: same signature and planned type,
/// value derived from the text so ingest and query embeddings agree.
#[derive(Debug, PartialEq, Eq, Hash)]
struct FakeCandleUDF {
    signature: Signature,
}

impl FakeCandleUDF {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

fn fake_embed(text: &str) -> Vec<f32> {
    let mut v = [0.0f32; DIMS];
    for (i, b) in text.bytes().enumerate() {
        v[i % DIMS] += (b as f32) / 255.0;
    }
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-6);
    v.iter().map(|x| x / norm).collect()
}

impl ScalarUDFImpl for FakeCandleUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "candle"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::List(Arc::new(Field::new_list_field(
            DataType::Float32,
            true,
        ))))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let texts = match &args.args[1] {
            ColumnarValue::Array(a) => a.clone(),
            ColumnarValue::Scalar(s) => s.to_array()?,
        };
        let texts = texts
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("candle fake: Utf8 text argument");
        let mut builder = ListBuilder::new(Float32Builder::new());
        for i in 0..texts.len() {
            if texts.is_null(i) {
                builder.append_null();
            } else {
                for value in fake_embed(texts.value(i)) {
                    builder.values().append_value(value);
                }
                builder.append(true);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Three items with the mock pack's exact column schema. Item 1's name is
/// long enough for several character/40 chunks and carries the search
/// keyword.
fn mock_items_batch() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "created_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
    ]));

    let names = vec![
        // 3 chunks at character/40: the quantum document.
        "Quantum entanglement lets paired particles share state across any \
         distance, and quantum computers exploit it for search."
            .to_string(),
        "A short note about databases.".to_string(),
        "Vector indexes trade exact recall for speed; text indexes rank by \
         term statistics."
            .to_string(),
    ];

    let mut tags = ListBuilder::new(StringBuilder::new());
    tags.values().append_value("physics");
    tags.values().append_value("qc");
    tags.append(true);
    tags.append_null();
    tags.values().append_value("search");
    tags.append(true);

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![1u64, 2, 3])),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(vec![Some(0.5), None, Some(2.25)])),
            Arc::new(tags.finish()),
            Arc::new(
                TimestampMillisecondArray::from(vec![
                    Some(1_700_000_000_000),
                    None,
                    Some(1_700_000_100_000),
                ])
                .with_timezone("UTC"),
            ),
        ],
    )
    .expect("mock items batch");
    (schema, batch)
}

async fn wait_for_terminal(store: &dyn JobStore, run_id: &str) -> skardi::jobs::store::JobRun {
    for _ in 0..600 {
        let run = store
            .get_run(run_id)
            .await
            .expect("store readable")
            .expect("run exists");
        if run.status.is_terminal() {
            return run;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("job run {run_id} did not finish within 30s");
}

#[tokio::test(flavor = "multi_thread")]
async fn mock_hybrid_e2e_generate_setup_ingest_search_get_document() {
    let Some(vec_path) = std::env::var("SQLITE_VEC_PATH")
        .ok()
        .filter(|p| !p.trim().is_empty())
    else {
        eprintln!(
            "SKIPPED: mock_hybrid_e2e requires the sqlite-vec extension — set \
             SQLITE_VEC_PATH to the vec0 loadable (e.g. `pip install sqlite-vec` \
             and point at .../sqlite_vec/vec0.dylib)"
        );
        return;
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("mock_search.db");
    let out_dir = dir.path().join("bundle");

    // ── generate (all four validation gates, vec0 DDL executed for real
    // because extensions_env resolves here) ─────────────────────────────
    let config = EtlConfig::from_yaml(&format!(
        r#"
kind: etl
metadata:
  name: mock-items-search
spec:
  source: {{ pack: mock, binding: saas.mock_demo, tables: [items] }}
  format: hybrid_search
  destination:
    type: sqlite
    path: {db}
    catalog: mock_search
    sqlite: {{ extensions_env: SQLITE_VEC_PATH }}
  embedding: {{ udf: candle, model: fake-e2e-model, dimensions: {DIMS} }}
  chunking: {{ splitter: character, size: 40, overlap: 0 }}
"#,
        db = db_path.display(),
    ))
    .expect("config");
    let generated = generate_hybrid(&config).await.expect("all four gates");
    assert!(
        generated.warnings.is_empty(),
        "with SQLITE_VEC_PATH set, vec0 DDL executes for real: {:?}",
        generated.warnings
    );
    generated
        .bundle
        .write(&out_dir, false)
        .expect("atomic write");

    // ── setup: apply the generated DDL to the real destination file ─────
    {
        let conn = rusqlite::Connection::open(&db_path).expect("open destination");
        unsafe { conn.load_extension_enable().unwrap() };
        unsafe { conn.load_extension(&vec_path, None::<&str>).unwrap() };
        conn.load_extension_disable().unwrap();
        let setup_sql = std::fs::read_to_string(out_dir.join("setup.sql")).unwrap();
        conn.execute_batch(&setup_sql).expect("setup.sql applies");
        // Idempotent re-apply — the README's promise.
        conn.execute_batch(&setup_sql).expect("re-apply is a no-op");
    }

    // ── the runtime SessionContext, wired like a server would ───────────
    let mut ctx = SessionContext::new();
    Arc::new(ChunkingRegistry::new()).register_chunk_udf(&mut ctx);
    register_json_pack_udf(&mut ctx);
    register_vec_to_binary_udf(&mut ctx);
    ctx.register_udf(ScalarUDF::new_from_impl(FakeCandleUDF::new()));

    let (schema, batch) = mock_items_batch();
    let items = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    let mock_schema = Arc::new(MemorySchemaProvider::new());
    mock_schema
        .register_table("items".to_string(), Arc::new(items))
        .unwrap();
    let saas = MemoryCatalogProvider::new();
    saas.register_schema("mock_demo", mock_schema).unwrap();
    ctx.register_catalog("saas", Arc::new(saas));

    let registry: DatasetRegistry = Arc::new(std::sync::RwLock::new(HashMap::new()));
    let mut options = HashMap::new();
    options.insert("extensions_env".to_string(), "SQLITE_VEC_PATH".to_string());
    register_sqlite_tables(
        &mut ctx,
        "mock_search",
        &db_path.display().to_string(),
        Some(&options),
        true,
        Some(&registry),
        HierarchyLevel::Catalog,
    )
    .await
    .expect("register destination catalog");
    register_sqlite_knn_udtf(&ctx, Arc::clone(&registry));
    register_sqlite_fts_udtf(&ctx, Arc::clone(&registry));
    let ctx = Arc::new(ctx);

    // ── ingest through the REAL executor (twice: dedup pin) ─────────────
    let job_path = out_dir.join("jobs/mock-items-search-ingest-items.yaml");
    let job = JobDefinition::load_from_file(&job_path, Arc::clone(&ctx))
        .await
        .expect("job loads")
        .expect("kind: job");
    let job_name = job.name().to_string();

    let store = Arc::new(SqliteJobStore::open_in_memory().await.expect("job store"));
    let executor = JobExecutor::new(
        HashMap::from([(job_name.clone(), job)]),
        Arc::clone(&store) as Arc<dyn JobStore>,
        Arc::clone(&ctx),
        HashMap::from([("mock_search".to_string(), DataSourceType::Sqlite)]),
        HashMap::new(),
    );

    let mut total_rows = 0u64;
    for pass in 1..=2u32 {
        let run_id = executor
            .submit(
                &job_name,
                HashMap::from([("limit".to_string(), json!(100))]),
            )
            .await
            .unwrap_or_else(|e| panic!("submit pass {pass}: {e}"));
        let run = wait_for_terminal(store.as_ref(), &run_id).await;
        assert_eq!(
            run.status,
            JobRunStatus::Succeeded,
            "pass {pass} failed: {:?}",
            run.error
        );
        let rows = run.rows_written.expect("rows_written recorded");
        assert!(
            rows >= 5,
            "3 items at character/40 chunk to ≥5 rows, got {rows}"
        );
        total_rows += rows;
    }

    // ── search: RRF-ranked, parameterized, doc_id-deduped ────────────────
    let search_sql =
        pipeline_query(&out_dir.join("pipelines/mock-items-search-search-hybrid.yaml"));
    let search_sql = search_sql
        .replace("{query}", "'quantum computers search'")
        .replace("{text_query}", "'quantum'")
        .replace("{vector_weight}", "0.5")
        .replace("{text_weight}", "0.5")
        .replace("{limit}", "10");
    let hits = ctx
        .sql(&search_sql)
        .await
        .expect("search plans")
        .collect()
        .await
        .expect("search runs");
    let hits: Vec<RecordBatch> = hits.into_iter().filter(|b| b.num_rows() > 0).collect();
    assert!(!hits.is_empty(), "search returned no rows");

    let mut doc_ids: Vec<String> = Vec::new();
    let mut scores: Vec<f64> = Vec::new();
    let mut top: Option<(String, String)> = None;
    for batch in &hits {
        let ids = batch
            .column_by_name("doc_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sids = batch
            .column_by_name("source_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let rrf = batch
            .column_by_name("rrf_score")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            if top.is_none() {
                top = Some((ids.value(i).to_string(), sids.value(i).to_string()));
            }
            doc_ids.push(ids.value(i).to_string());
            scores.push(rrf.value(i));
        }
    }
    // Dedup despite the double ingest (2× rows in the table).
    let unique: std::collections::BTreeSet<&String> = doc_ids.iter().collect();
    assert_eq!(unique.len(), doc_ids.len(), "doc_id dedup: {doc_ids:?}");
    assert!(
        scores.windows(2).all(|w| w[0] >= w[1]),
        "RRF-ranked descending: {scores:?}"
    );
    // The quantum document (source_id 1) wins for a quantum query.
    let (top_doc, top_source) = top.unwrap();
    assert_eq!(
        top_source, "1",
        "top hit {top_doc} should come from item 1: {doc_ids:?}"
    );
    let _ = total_rows;

    // ── get-document: ordered chunks, overlap-0 reassembly ──────────────
    let get_sql = pipeline_query(&out_dir.join("pipelines/mock-items-search-get-document.yaml"))
        .replace("{source_table}", "'items'")
        .replace("{source_id}", "'1'");
    let chunks = ctx
        .sql(&get_sql)
        .await
        .expect("get-document plans")
        .collect()
        .await
        .unwrap();
    let mut indexes: Vec<i64> = Vec::new();
    let mut reassembled = String::new();
    for batch in &chunks {
        let idx = batch
            .column_by_name("chunk_index")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let content = batch
            .column_by_name("content")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            indexes.push(idx.value(i));
            reassembled.push_str(content.value(i));
        }
    }
    let expected_indexes: Vec<i64> = (0..indexes.len() as i64).collect();
    assert_eq!(indexes, expected_indexes, "ordered 0-based chunk indexes");
    assert!(
        indexes.len() >= 3,
        "item 1 chunks at character/40: {indexes:?}"
    );
    let (_, batch) = mock_items_batch();
    let original = batch
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .to_string();
    // The character splitter trims whitespace at chunk boundaries, so the
    // boundary spaces are absent from a plain concat; every non-whitespace
    // character must survive in order.
    let squash = |s: &str| s.chars().filter(|c| !c.is_whitespace()).collect::<String>();
    assert_eq!(
        squash(&reassembled),
        squash(&original),
        "overlap-0 reassembly recovers the document's content in order"
    );
}

/// Extract `spec.query` from a generated pipeline YAML.
fn pipeline_query(path: &std::path::Path) -> String {
    let value: serde_yaml::Value =
        serde_yaml::from_str(&std::fs::read_to_string(path).expect("pipeline file")).unwrap();
    value
        .get("spec")
        .and_then(|s| s.get("query"))
        .and_then(|q| q.as_str())
        .expect("spec.query")
        .to_string()
}
