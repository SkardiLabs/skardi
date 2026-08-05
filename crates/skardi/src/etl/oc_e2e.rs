//! The OTHER half of the ingest path the mock e2e substitutes away:
//! a REAL Open Connector read — mock gateway over HTTP, the real
//! provider's discovery / resource inputs / page-number pagination /
//! json→arrow — feeding the generated job through the real executor into
//! a real SQLite file. `tests/etl_mock_e2e.rs` owns everything from the
//! generated SQL down with a MemTable source; this test owns the wire.
//!
//! In-crate (not `tests/`) because [`MockGateway`] is `pub(crate)`.
//! Requires the sqlite-vec loadable via `SQLITE_VEC_PATH` (the sync
//! triggers write the vec0 mirror on every insert); SKIPS with a pointer
//! when unset — CI sets it.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Float32Builder, ListBuilder, StringArray};
use arrow::datatypes::{DataType, Field};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use serde_json::json;

use super::EtlConfig;
use super::validate::generate_hybrid;
use crate::jobs::definition::JobDefinition;
use crate::jobs::executor::JobExecutor;
use crate::jobs::store::{JobRunStatus, JobStore, SqliteJobStore};
use crate::model::chunking::ChunkingRegistry;
use crate::sources::DataSourceType;
use crate::sources::hierarchy::HierarchyLevel;
use crate::sources::providers::DatasetRegistry;
use crate::sources::providers::open_connector::testutil::{
    EnvVarGuard, MockGateway, MockResponse, RecordedRequest, discovery_ok, envelope_ok,
};
use crate::sources::providers::open_connector::{
    OpenConnectorConfig, register_open_connector_tables,
};
use crate::sources::providers::sqlite::fts_table_function::register_sqlite_fts_udtf;
use crate::sources::providers::sqlite::knn_table_function::register_sqlite_knn_udtf;
use crate::sources::providers::sqlite::register_sqlite_tables;
use crate::sources::providers::sqlite::vec_to_binary::register_vec_to_binary_udf;
use crate::util::json_pack::register_json_pack_udf;
use tokio_rusqlite::rusqlite;

const DIMS: usize = 8;
const TOKEN_ENV: &str = "SKARDI_ETL_OC_E2E_TOKEN";

/// Five items, page-number paginated at the pack's per_page = 2 → three
/// execute calls. Item 1's name is long enough for several character/40
/// chunks; item 4 has nulls in every nullable column.
fn items_handler(req: &RecordedRequest) -> MockResponse {
    if req.method == "GET" && req.path == "/v1/health" {
        return MockResponse::ok("{}");
    }
    if req.method == "GET" && req.path == "/v1/actions/mock.list_items" {
        return MockResponse::ok(&discovery_ok("{}", r#"{"type": "object"}"#, true, None));
    }
    if req.method == "POST" && req.path == "/v1/actions/mock.list_items" {
        let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
        // The pack's required binding resource must ride every request.
        assert_eq!(
            body["input"]["workspace"].as_str(),
            Some("demo"),
            "binding resource missing from: {body}"
        );
        let page = body["input"]["page"].as_u64().unwrap_or(1) as usize;
        let items = vec![
            json!({"id": 1,
                   "name": "Quantum entanglement lets paired particles share state across \
                            any distance, and quantum computers exploit it for search.",
                   "value": 0.5, "tags": ["physics", "qc"],
                   "created_at": "2026-01-01T00:00:00Z"}),
            json!({"id": 2, "name": "A short note about databases.", "value": 1.5,
                   "tags": ["db"], "created_at": "2026-01-02T00:00:00Z"}),
            json!({"id": 3,
                   "name": "Vector indexes trade exact recall for speed; text indexes \
                            rank by term statistics.",
                   "value": 2.5, "tags": ["search"],
                   "created_at": "2026-01-03T00:00:00Z"}),
            json!({"id": 4, "name": "Nulls everywhere else.", "value": null,
                   "tags": null, "created_at": null}),
            json!({"id": 5, "name": "The last item on the last page.", "value": 5.0,
                   "tags": [], "created_at": "2026-01-05T00:00:00Z"}),
        ];
        let slice: Vec<_> = items.into_iter().skip((page - 1) * 2).take(2).collect();
        return MockResponse::ok(&envelope_ok(&json!({ "items": slice }).to_string()));
    }
    MockResponse::new(404, "{}")
}

/// Deterministic `candle` stand-in (same contract as the mock e2e's).
#[derive(Debug, PartialEq, Eq, Hash)]
struct FakeCandleUDF {
    signature: Signature,
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
            .expect("Utf8 text argument");
        let mut builder = ListBuilder::new(Float32Builder::new());
        for i in 0..texts.len() {
            if texts.is_null(i) {
                builder.append_null();
                continue;
            }
            let mut v = [0.0f32; DIMS];
            for (j, b) in texts.value(i).bytes().enumerate() {
                v[j % DIMS] += (b as f32) / 255.0;
            }
            let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-6);
            for x in v {
                builder.values().append_value(x / norm);
            }
            builder.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn real_oc_source_paginates_into_the_generated_sqlite_destination() {
    let Some(vec_path) = std::env::var("SQLITE_VEC_PATH")
        .ok()
        .filter(|p| !p.trim().is_empty())
    else {
        eprintln!(
            "SKIPPED: real_oc_source_paginates_into_the_generated_sqlite_destination \
             needs SQLITE_VEC_PATH (the vec0 loadable)"
        );
        return;
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("oc_e2e.db");
    let out_dir = dir.path().join("bundle");

    // ── generate + write the bundle ─────────────────────────────────────
    let config = EtlConfig::from_yaml(&format!(
        r#"
kind: etl
metadata:
  name: oc-items-search
spec:
  source: {{ pack: mock, binding: saas.ws, tables: [items] }}
  format: hybrid_search
  destination:
    type: sqlite
    path: {db}
    catalog: oc_search
  embedding: {{ udf: candle, model: fake-oc-e2e, dimensions: {DIMS} }}
  chunking: {{ splitter: character, size: 40, overlap: 0 }}
"#,
        db = db_path.display(),
    ))
    .expect("config");
    let generated = generate_hybrid(&config).await.expect("all four gates");
    generated
        .bundle
        .write(&out_dir, false)
        .expect("atomic write");

    // ── setup: real DDL on the real destination file ────────────────────
    {
        let conn = rusqlite::Connection::open(&db_path).expect("open destination");
        unsafe { conn.load_extension_enable().unwrap() };
        unsafe { conn.load_extension(&vec_path, None::<&str>).unwrap() };
        conn.load_extension_disable().unwrap();
        conn.execute_batch(&std::fs::read_to_string(out_dir.join("setup.sql")).unwrap())
            .expect("setup.sql applies");
    }

    // ── the runtime ctx: REAL Open Connector source registration ────────
    let gateway = MockGateway::start(items_handler).await;
    let _token = EnvVarGuard::set(TOKEN_ENV, "test-token");
    let oc_config: OpenConnectorConfig = serde_yaml::from_str(&format!(
        r#"
runtime_token_env: {TOKEN_ENV}
cache_ttl_seconds: 0
bindings:
  - name: ws
    source_pack: mock
    resource: {{ workspace: demo }}
    tables: [items]
"#
    ))
    .expect("oc config parses");

    let mut ctx = SessionContext::new();
    Arc::new(ChunkingRegistry::new()).register_chunk_udf(&mut ctx);
    register_json_pack_udf(&mut ctx);
    register_vec_to_binary_udf(&mut ctx);
    ctx.register_udf(ScalarUDF::new_from_impl(FakeCandleUDF {
        signature: Signature::variadic_any(Volatility::Immutable),
    }));
    register_open_connector_tables(
        &mut ctx,
        "saas",
        &gateway.url,
        Some(&oc_config),
        false,
        HierarchyLevel::Catalog,
        None,
    )
    .await
    .expect("OC gateway registration (health + discovery) succeeds");

    let registry: DatasetRegistry = Arc::new(std::sync::RwLock::new(HashMap::new()));
    let options = HashMap::from([("extensions_env".to_string(), "SQLITE_VEC_PATH".to_string())]);
    register_sqlite_tables(
        &mut ctx,
        "oc_search",
        &db_path.display().to_string(),
        Some(&options),
        true,
        Some(&registry),
        HierarchyLevel::Catalog,
    )
    .await
    .expect("destination registration");
    register_sqlite_knn_udtf(&ctx, Arc::clone(&registry));
    register_sqlite_fts_udtf(&ctx, Arc::clone(&registry));
    let ctx = Arc::new(ctx);

    // ── the generated job, through the real executor ────────────────────
    let job = JobDefinition::load_from_file(
        out_dir.join("jobs/oc-items-search-ingest-items.yaml"),
        Arc::clone(&ctx),
    )
    .await
    .expect("job loads against the real OC schema")
    .expect("kind: job");
    let job_name = job.name().to_string();

    let store = Arc::new(SqliteJobStore::open_in_memory().await.expect("job store"));
    let executor = JobExecutor::new(
        HashMap::from([(job_name.clone(), job)]),
        Arc::clone(&store) as Arc<dyn JobStore>,
        Arc::clone(&ctx),
        HashMap::from([("oc_search".to_string(), DataSourceType::Sqlite)]),
        HashMap::new(),
    );
    let run_id = executor
        .submit(
            &job_name,
            HashMap::from([("limit".to_string(), json!(100))]),
        )
        .await
        .expect("submit");
    let run = loop {
        let run = store.get_run(&run_id).await.unwrap().unwrap();
        if run.status.is_terminal() {
            break run;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    assert_eq!(run.status, JobRunStatus::Succeeded, "{:?}", run.error);
    let rows_written = run.rows_written.expect("rows recorded");

    // ── the wire really paginated: 3 execute calls for 5 items @ 2/page ─
    let execute_calls = gateway
        .requests()
        .iter()
        .filter(|r| r.method == "POST" && r.path == "/v1/actions/mock.list_items")
        .count();
    assert_eq!(execute_calls, 3, "5 items at per_page 2 = 3 pages");

    // ── what landed in sqlite is the source data, document-shaped ───────
    // (The verification connection needs vec0 too: a schema containing a
    // vec0 virtual table fails schema parse on ANY statement otherwise.)
    let conn = rusqlite::Connection::open(&db_path).expect("open destination");
    unsafe { conn.load_extension_enable().unwrap() };
    unsafe { conn.load_extension(&vec_path, None::<&str>).unwrap() };
    conn.load_extension_disable().unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM documents", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count as u64, rows_written, "reported rows == stored rows");
    assert!(count >= 7, "5 items, item 1 and 3 multi-chunk: {count}");

    let distinct_sources: i64 = conn
        .query_row("SELECT COUNT(DISTINCT source_id) FROM documents", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(distinct_sources, 5, "every source item arrived");

    // Chunk 0 of item 1: identity, metadata via json_pack, RFC 3339 text.
    let (doc_id, content, metadata, created_at): (String, String, String, Option<String>) = conn
        .query_row(
            "SELECT doc_id, content, metadata, created_at FROM documents \
             WHERE source_id = '1' AND chunk_index = 0",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();
    assert_eq!(doc_id, "items:1:0");
    assert!(content.starts_with("Quantum entanglement"), "{content}");
    let metadata: serde_json::Value = serde_json::from_str(&metadata).expect("metadata is JSON");
    assert_eq!(metadata["value"], json!(0.5));
    assert_eq!(metadata["tags"], json!(["physics", "qc"]));
    let created_at = created_at.expect("item 1 has a timestamp");
    assert!(
        created_at.starts_with("2026-01-01T00:00:00"),
        "{created_at}"
    );

    // The all-nulls item survived the nullable columns.
    let null_created: Option<String> = conn
        .query_row(
            "SELECT created_at FROM documents WHERE source_id = '4'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(null_created.is_none(), "item 4's NULL timestamp stays NULL");

    // Mirrors in lockstep (the sync triggers fired per row).
    for mirror in ["documents_fts", "documents_vec"] {
        let mirrored: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {mirror}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(mirrored, count, "{mirror} tracks the table");
    }

    // ── and the generated search pipeline reads it back ─────────────────
    let search_yaml: serde_yaml::Value = serde_yaml::from_str(
        &std::fs::read_to_string(out_dir.join("pipelines/oc-items-search-search-hybrid.yaml"))
            .unwrap(),
    )
    .unwrap();
    let search_sql = search_yaml["spec"]["query"]
        .as_str()
        .unwrap()
        .replace("{query}", "'quantum computers search'")
        .replace("{text_query}", "'quantum'")
        .replace("{vector_weight}", "0.5")
        .replace("{text_weight}", "0.5")
        .replace("{limit}", "10");
    let batches = ctx
        .sql(&search_sql)
        .await
        .expect("search plans")
        .collect()
        .await
        .expect("search runs");
    let top_source: String = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .map(|b| {
            b.column_by_name("source_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .to_string()
        })
        .expect("search returned rows");
    assert_eq!(top_source, "1", "the quantum document wins a quantum query");
}
