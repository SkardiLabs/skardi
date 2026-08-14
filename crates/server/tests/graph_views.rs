//! Integration tests for `type: graph` data sources wired through the
//! server (milestone 4): config validation, degraded registration, the
//! `/data_source` status surface, and the session-level JSON getter UDFs.
//!
//! No docker required: the graph source points at a closed port
//! (`127.0.0.1:1`), so the backend is unreachable and the source exercises
//! the DEGRADED registration path — startup must succeed, the declared view
//! schema must plan, and the first scan must fail loudly.

use std::fs;

use axum::extract::State;
use skardi_server::config::{CliArgs, load_server_config};
use skardi_server::pipeline_handlers::get_data_sources;
use skardi_server::server::setup_app_state;

/// A `kind: context` envelope with one `type: graph` source whose backend
/// is a closed port (connection refused is instant — no timeout wait).
const GRAPH_CTX: &str = r#"
kind: context
metadata:
  name: graph-views-test
spec:
  data_sources:
    - name: kg
      type: graph
      connection_string: postgres://127.0.0.1:1/none
      hierarchy_level: catalog
      graph:
        backend: age
        graph_name: knowledge
        query_timeout_seconds: 5
        views:
          - name: user_posts
            cypher: MATCH (u:User) RETURN u.name AS user_name
            schema:
              - name: user_name
                type: string
"#;

fn write_ctx(dir: &tempfile::TempDir, contents: &str) -> std::path::PathBuf {
    let path = dir.path().join("ctx.yaml");
    fs::write(&path, contents).expect("write ctx file");
    path
}

fn args_with_ctx(ctx_path: std::path::PathBuf) -> CliArgs {
    CliArgs {
        pipeline_path: None,
        jobs_path: None,
        jobs_db_path: None,
        ctx_file: Some(ctx_path),
        semantics_path: None,
        port: 8080,
        query_audit_db: None,
        query_audit_retention_days: None,
    }
}

/// Degraded end to end: an unreachable backend must not take startup down,
/// the view's declared schema must plan without the backend, the first
/// scan must fail loudly, and `/data_source` must report the source as
/// "degraded" with its catalog table enumerated.
#[tokio::test]
async fn an_unreachable_backend_registers_degraded_and_reports_status() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let args = args_with_ctx(write_ctx(&dir, GRAPH_CTX));

    let config = load_server_config(args)
        .await
        .expect("config loads (validation is pure, no network)");
    let state = setup_app_state(config)
        .await
        .expect("degraded registration must not fail startup");

    // The declared schema is queryable at PLAN time — no backend needed.
    let df = state
        .session_ctx
        .sql("SELECT user_name FROM kg.main.user_posts")
        .await
        .expect("the declared schema plans");
    assert_eq!(df.schema().field(0).name(), "user_name");

    // The first scan retries the validation and fails loudly, naming the
    // view (acquire against the closed port is refused instantly).
    let err = df.collect().await.expect_err("the backend is still gone");
    let msg = err.to_string();
    assert!(msg.contains("user_posts"), "the view is named: {msg}");
    assert!(msg.contains("DEGRADED"), "{msg}");

    // The UDTF path reports the same degraded context — the registration
    // error (Connection refused) must survive next to the fresh failure,
    // never a bare timeout advising to "narrow the traversal".
    let err = state
        .session_ctx
        .sql(
            "SELECT user_name FROM cypher_query('kg', \
             'MATCH (u:User) RETURN u.name AS user_name', '{}', \
             '{\"user_name\": \"string\"}')",
        )
        .await
        .expect("the UDTF plans against the declared columns")
        .collect()
        .await
        .expect_err("the retried query fails");
    let msg = err.to_string();
    assert!(msg.contains("DEGRADED"), "{msg}");
    assert!(msg.contains("Connection refused"), "{msg}");
    assert!(!msg.contains("narrow the traversal"), "{msg}");

    // /data_source: the source reports its degraded status and enumerates
    // the view under its fully-qualified catalog name.
    let axum::Json(body) = get_data_sources(State(state))
        .await
        .expect("data sources list");
    let data = body["data"].as_array().expect("data array");
    let entry = data
        .iter()
        .find(|d| d["name"] == "kg")
        .expect("graph source should be listed");
    assert_eq!(entry["type"], "graph");
    assert_eq!(entry["status"], "degraded");
    assert!(entry["path"].is_null(), "graph exposes no path");
    assert_eq!(entry["url"], "postgres://127.0.0.1:1/none");
    let tables = entry["tables"].as_array().expect("tables array");
    let names: Vec<&str> = tables
        .iter()
        .map(|t| t["name"].as_str().expect("table name"))
        .collect();
    assert_eq!(names, ["kg.main.user_posts"]);
    let columns = tables[0]["schema"].as_array().expect("schema array");
    assert_eq!(columns[0]["name"], "user_name");
}

/// `type: graph` without `hierarchy_level: catalog` is a config error, not
/// a registration error — views register as `<name>.main.<view>` catalog
/// tables, so the hierarchy is not optional.
#[tokio::test]
async fn a_graph_source_without_catalog_hierarchy_is_rejected_at_load() {
    let ctx = GRAPH_CTX.replace("      hierarchy_level: catalog\n", "");
    let dir = tempfile::TempDir::new().expect("temp dir");
    let err = load_server_config(args_with_ctx(write_ctx(&dir, &ctx)))
        .await
        .expect_err("hierarchy is required");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("hierarchy_level to 'catalog'"),
        "GraphHierarchyRequired: {msg}"
    );
}

/// A `graph:` block on any other source type is rejected — the block is
/// only valid for `type: graph`.
#[tokio::test]
async fn a_graph_block_on_a_non_graph_type_is_rejected_at_load() {
    let ctx = GRAPH_CTX.replace("type: graph", "type: postgres");
    let dir = tempfile::TempDir::new().expect("temp dir");
    let err = load_server_config(args_with_ctx(write_ctx(&dir, &ctx)))
        .await
        .expect_err("the graph block is graph-only");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("The 'graph' field is only valid for type 'graph'"),
        "UnexpectedGraphConfig: {msg}"
    );
}

/// The runtime session carries the JSON getter UDFs unconditionally (graph
/// node/relationship `properties` are the consumer) — and carries ONLY the
/// UDFs: `->>` must NOT be silently rewritten to `json_get` (federated
/// pushdown protection; see skardi::util::json_getters). DataFusion 52 has
/// no native Arrow-operator planner either, so the observable contract is a
/// loud planning error naming the operator.
#[tokio::test]
async fn the_runtime_session_has_json_getters_without_the_operator_rewrite() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let config = load_server_config(args_with_ctx(write_ctx(&dir, GRAPH_CTX)))
        .await
        .expect("config loads");
    let state = setup_app_state(config).await.expect("app state");

    let batches = state
        .session_ctx
        .sql("SELECT json_get_str('{\"a\": \"x\"}', 'a') AS v")
        .await
        .expect("json_get_str plans")
        .collect()
        .await
        .expect("json_get_str executes");
    let col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("string column");
    assert_eq!(col.value(0), "x");

    let err = state
        .session_ctx
        .sql("SELECT '{\"a\":1}'::text ->> 'a'")
        .await
        .expect_err("no rewrite means no plan");
    let msg = err.to_string();
    assert!(msg.contains("->>"), "the operator is named: {msg}");
    assert!(
        msg.contains("not yet supported"),
        "native (unsupported), not rewritten to json_get: {msg}"
    );
}
