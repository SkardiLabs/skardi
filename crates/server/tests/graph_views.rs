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

    // The UDTF path reports the same degraded context — the typed
    // unreachable registration error must survive next to the fresh
    // failure, never a bare timeout advising to "narrow the traversal".
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
    // Assert the TYPED unreachable context, not the OS errno text —
    // "Connection refused" is what a normal host says, but a sandboxed
    // runner says "Operation not permitted" for the same unreachable
    // semantics, and the contract under test is ours, not libc's.
    assert!(msg.contains("is unreachable"), "{msg}");
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

/// The `{params}` decision (design Risks #0), proven through the REAL
/// machinery end to end: skardi's pipeline loader must surface `params`
/// in the inferred request_schema (the NULL placeholder plans), and the
/// SERVER'S OWN `substitute_sql_params` — not a hand-rolled replace —
/// must turn a request's params JSON into SQL that re-plans and delivers
/// the bound value to the graph client.
mod params_through_real_substitution {
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    use async_trait::async_trait;
    use futures::StreamExt;
    use futures::stream::{self, BoxStream};
    use serde_json::Value;
    use skardi::pipeline::pipeline::{Pipeline, StandardPipeline};
    use skardi::sources::providers::graph::client::{GraphClient, QueryBounds};
    use skardi::sources::providers::graph::error::GraphError;
    use skardi::sources::providers::graph::udtf::{
        GraphSourceHandle, GraphSourceHealth, GraphSources, register_graph_udtfs,
    };
    use skardi_server::pipeline_handlers::substitute_sql_params;

    /// Echoes the bound `min` (or `name`) param back as the row, so
    /// substitution is observable end to end.
    #[derive(Debug)]
    struct EchoClient;

    #[async_trait]
    impl GraphClient for EchoClient {
        async fn execute(
            &self,
            _cypher: &str,
            params: &Value,
            _arity: usize,
            _bounds: QueryBounds,
            _limit: Option<usize>,
        ) -> Result<BoxStream<'static, Result<Vec<Value>, GraphError>>, GraphError> {
            let echoed = params
                .get("min")
                .or_else(|| params.get("name"))
                .cloned()
                .unwrap_or(Value::Null);
            Ok(stream::iter(vec![Ok(vec![echoed])]).boxed())
        }

        async fn labels(
            &self,
            _bounds: QueryBounds,
            _limit: Option<usize>,
        ) -> Result<Vec<(String, String)>, GraphError> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn the_servers_substitution_binds_params_into_cypher_query() {
        let sources: GraphSources = Arc::new(RwLock::new(HashMap::new()));
        sources.write().unwrap().insert(
            "kg".to_string(),
            Arc::new(GraphSourceHandle {
                client: Arc::new(EchoClient),
                bounds: QueryBounds {
                    timeout: std::time::Duration::from_secs(5),
                    max_rows: 100,
                },
                health: Arc::new(RwLock::new(GraphSourceHealth::Healthy)),
                view_contracts: Arc::new(vec![]),
                recovery_gate: Arc::new(tokio::sync::Mutex::new(())),
                validation_limit: 4,
            }),
        );
        let ctx = Arc::new(datafusion::prelude::SessionContext::new());
        register_graph_udtfs(&ctx, Arc::clone(&sources)).unwrap();

        let dir = tempfile::TempDir::new().unwrap();
        let spec = r#"
kind: pipeline
metadata:
  name: probe
  version: "1"
spec:
  query: |
    SELECT min_age FROM cypher_query('kg', 'MATCH (p) WHERE p.age > $min RETURN p.age', {params}, '{"min_age": "int"}')
"#;
        let path = dir.path().join("probe.yaml");
        std::fs::write(&path, spec).unwrap();

        // Inference through the REAL loader: `{params}` becomes NULL,
        // plans, and `params` lands in the request schema — the exact
        // set the server substitutes on.
        let pipeline = StandardPipeline::load_from_file(&path, Arc::clone(&ctx))
            .await
            .expect("inference plans with the NULL placeholder");
        let expected: Vec<String> = pipeline.request_schema().fields.keys().cloned().collect();
        assert!(
            expected.contains(&"params".to_string()),
            "inference must surface params: {expected:?}"
        );

        // Execution through the SERVER'S substitution — the request
        // carries the params JSON as a string, exactly as documented.
        let raw = std::fs::read_to_string(&path).unwrap();
        let yaml: serde_yaml::Value = serde_yaml::from_str(&raw).unwrap();
        let mut sql = yaml["spec"]["query"].as_str().unwrap().to_string();
        let request: HashMap<String, Value> = HashMap::from([(
            "params".to_string(),
            Value::String("{\"min\": 40}".to_string()),
        )]);
        let (missing, unsupported) = substitute_sql_params(&mut sql, &expected, &request);
        assert!(missing.is_empty(), "{missing:?}");
        assert!(unsupported.is_empty(), "{unsupported:?}");
        assert!(!sql.contains("{params}"), "placeholder fully substituted");

        let batches = ctx
            .sql(&sql)
            .await
            .expect("substituted SQL re-plans")
            .collect()
            .await
            .expect("executes");
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("int column");
        assert_eq!(col.value(0), 40, "the bound param reached the client");
    }

    fn ctx_with_echo() -> Arc<datafusion::prelude::SessionContext> {
        let sources: GraphSources = Arc::new(RwLock::new(HashMap::from([(
            "kg".to_string(),
            Arc::new(GraphSourceHandle {
                client: Arc::new(EchoClient) as Arc<dyn GraphClient>,
                bounds: QueryBounds {
                    timeout: std::time::Duration::from_secs(5),
                    max_rows: 100,
                },
                health: Arc::new(RwLock::new(GraphSourceHealth::Healthy)),
                view_contracts: Arc::new(vec![]),
                recovery_gate: Arc::new(tokio::sync::Mutex::new(())),
                validation_limit: 4,
            }),
        )])));
        let ctx = Arc::new(datafusion::prelude::SessionContext::new());
        register_graph_udtfs(&ctx, sources).unwrap();
        ctx
    }

    #[tokio::test]
    async fn a_single_quote_in_a_param_survives_the_sql_escaping_round_trip() {
        // The substitution quotes the params JSON as a SQL string literal
        // with '' escaping — a value like O'Brien is exactly the input
        // that breaks if that escaping (or the UDTF's re-parse of the
        // literal) is wrong, so pin the full round trip: escape → plan →
        // parse → the client sees the original apostrophe.
        let ctx = ctx_with_echo();
        let mut sql = "SELECT who FROM cypher_query('kg', \
                       'MATCH (p) WHERE p.name = $name RETURN p.name', {params}, \
                       '{\"who\": \"string\"}')"
            .to_string();
        let expected = vec!["params".to_string()];
        let request: HashMap<String, Value> = HashMap::from([(
            "params".to_string(),
            Value::String("{\"name\": \"O'Brien\"}".to_string()),
        )]);
        let (missing, unsupported) = substitute_sql_params(&mut sql, &expected, &request);
        assert!(missing.is_empty(), "{missing:?}");
        assert!(unsupported.is_empty(), "{unsupported:?}");
        assert!(
            sql.contains("O''Brien"),
            "the literal is SQL-escaped: {sql}"
        );

        let batches = ctx
            .sql(&sql)
            .await
            .expect("the escaped literal plans")
            .collect()
            .await
            .expect("executes");
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("string column");
        assert_eq!(
            col.value(0),
            "O'Brien",
            "the apostrophe survives the round trip"
        );
    }

    #[test]
    fn a_json_object_param_is_rejected_as_unsupported_not_stringified() {
        // The documented contract is a STRING carrying JSON — a request
        // that sends `"params": {"min": 40}` as a real JSON object must
        // land in `unsupported` (a typed 400), not be silently
        // Display-formatted into the SQL.
        let mut sql = "SELECT n FROM cypher_query('kg', 'RETURN 1', {params}, '{\"n\": \"int\"}')"
            .to_string();
        let expected = vec!["params".to_string()];
        let request: HashMap<String, Value> =
            HashMap::from([("params".to_string(), serde_json::json!({"min": 40}))]);
        let (missing, unsupported) = substitute_sql_params(&mut sql, &expected, &request);
        assert!(missing.is_empty(), "{missing:?}");
        assert_eq!(
            unsupported,
            vec!["params: unsupported JSON object".to_string()],
            "an object-shaped param is refused by KIND — the value itself \
             never appears in the 400"
        );
        assert!(
            sql.contains("{params}"),
            "the placeholder is left untouched on refusal: {sql}"
        );
    }
}
