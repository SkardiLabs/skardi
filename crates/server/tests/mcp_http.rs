//! End-to-end tests for the `/mcp` streamable-HTTP binding: a real rmcp
//! client over a real TCP socket against the same middleware-wrapped router
//! the binary serves. The transport-level pieces (gate ordering, host
//! allowlist, middleware boundary) are pinned in-crate by `src/mcp/mod.rs`;
//! this suite proves the full stack — rmcp client transport → gate → rmcp
//! server → handler → synthetic dispatch → REST handlers — agrees with it.
//!
//! Harness mirrors `pipelines_http.rs` (products MemTable + one real
//! pipeline) and `jobs_auth_http.rs` (better-auth env lock).

use std::collections::HashMap;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use rmcp::model::{CallToolRequestParams, CallToolResult, ClientInfo, ProtocolVersion};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::{RoleClient, ServiceExt, service::RunningService};
use serde_json::{Value, json};
use skardi::pipeline::pipeline::{Pipeline, StandardPipeline};
use tempfile::TempDir;

use skardi_server::auth::layer::AuthLayer;
use skardi_server::auth::mode::AuthMode;
use skardi_server::config::{CliArgs, ServerConfig};
use skardi_server::query_audit::QueryAuditStore;
use skardi_server::semantics::SemanticsRegistry;
use skardi_server::server::{AppState, configure_middleware, configure_routes};

fn products_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("brand", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("category", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Apple", "Sony", "Apple", "Samsung", "Apple",
            ])),
            Arc::new(Float64Array::from(vec![
                1299.0, 199.0, 999.0, 599.0, 2499.0,
            ])),
            Arc::new(StringArray::from(vec![
                "Electronics",
                "Audio",
                "Electronics",
                "Electronics",
                "Electronics",
            ])),
        ],
    )
    .unwrap()
}

/// `AppState` with a `products` MemTable and a real `product-search`
/// pipeline, so `/mcp` tool calls run actual SQL end-to-end.
async fn make_app_state(
    auth_layer: AuthLayer,
    query_audit: Option<Arc<QueryAuditStore>>,
) -> (AppState, TempDir) {
    let tmp = TempDir::new().unwrap();
    let ctx = Arc::new(SessionContext::new());
    ctx.register_batch("products", products_batch()).unwrap();

    let yaml_path = tmp.path().join("product-search.yaml");
    let mut f = std::fs::File::create(&yaml_path).unwrap();
    f.write_all(
        br#"
kind: pipeline
metadata:
  name: "product-search"
  version: "1.0.0"
  description: "Filter products by brand + max price"
spec:
  parameters:
    brand: "Exact brand name; null matches every brand"
  query: |
    SELECT id, brand, price, category
    FROM products
    WHERE brand = {brand} AND price <= {max_price}
    ORDER BY price DESC
"#,
    )
    .unwrap();
    let pipeline = StandardPipeline::load_from_file(&yaml_path, Arc::clone(&ctx))
        .await
        .unwrap();
    let mut pipelines: HashMap<String, StandardPipeline> = HashMap::new();
    pipelines.insert(pipeline.name().to_string(), pipeline);

    let engine = Arc::new(skardi::engine::datafusion::DataFusionEngine::new_with_arc(
        Arc::clone(&ctx),
    ));
    let config = ServerConfig {
        pipelines,
        jobs: HashMap::new(),
        data_sources: vec![],
        semantics: SemanticsRegistry::default(),
        args: CliArgs {
            pipeline_path: None,
            jobs_path: None,
            jobs_db_path: None,
            ctx_file: None,
            semantics_path: None,
            port: 0,
            query_audit_db: None,
            query_audit_retention_days: None,
            mcp_allowed_hosts: vec![],
        },
    };
    let state = AppState::new(
        config,
        engine,
        Arc::clone(&ctx),
        auth_layer,
        None,
        query_audit,
        Default::default(),
    );
    (state, tmp)
}

/// Serve the full app (routes + middleware, exactly what the binary runs) on
/// an ephemeral loopback port. The rmcp client sends `Host: 127.0.0.1:<port>`,
/// which the portless loopback allowlist entry matches.
async fn serve(state: AppState) -> SocketAddr {
    let app = configure_middleware(configure_routes(state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

fn mcp_uri(addr: SocketAddr) -> String {
    format!("http://{addr}/mcp")
}

/// Default rmcp client: negotiates `ProtocolVersion::LATEST` (2025-11-25),
/// so the server serves it in legacy session mode.
async fn connect(addr: SocketAddr) -> RunningService<RoleClient, ()> {
    let transport = StreamableHttpClientTransport::from_uri(mcp_uri(addr));
    ().serve(transport).await.expect("initialize handshake")
}

/// Tool results carry the REST response JSON verbatim as one text block;
/// parse it back out for row-level assertions.
fn result_json(result: &CallToolResult) -> Value {
    let content = serde_json::to_value(&result.content).unwrap();
    let text = content[0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("expected a text content block: {content}"));
    serde_json::from_str(text)
        .unwrap_or_else(|e| panic!("tool result text is not the REST JSON body ({e}): {text}"))
}

fn call_params(name: &'static str, args: Value) -> CallToolRequestParams {
    CallToolRequestParams::new(name)
        .with_arguments(args.as_object().cloned().expect("literal is an object"))
}

/// Serialized with the same discipline as `jobs_auth_http.rs`: `AuthLayer::
/// build` reads process-global env vars, and `auth::test_env::lock` is
/// `#[cfg(test)]` on the lib, invisible to integration-test binaries.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn better_auth_layer() -> AuthLayer {
    let _env = ENV_LOCK.lock().await;
    unsafe {
        std::env::set_var("AUTH_SECRET", "test-secret-that-is-at-least-32-characters!");
        std::env::set_var("AUTH_DB_PATH", ":memory:");
        std::env::remove_var("AUTH_BASE_URL");
    }
    AuthLayer::build(&AuthMode::BetterAuthDieselSqlite)
        .await
        .unwrap()
}

/// Mint a real user + session, returning the Bearer token.
async fn bearer_token(state: &AppState) -> String {
    use better_auth::{SessionOps, UserOps};
    let auth = state.auth_layer.as_better_auth().unwrap();
    let user = auth
        .database()
        .create_user(better_auth::types_mod::CreateUser {
            name: Some("mcp-e2e".into()),
            email: Some("mcp-e2e@test.com".into()),
            password: Some("password123".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    let session = auth
        .database()
        .create_session(better_auth::types_mod::CreateSession {
            user_id: user.id.clone(),
            expires_at: chrono::Utc::now() + chrono::Duration::hours(1),
            ip_address: None,
            user_agent: None,
            impersonated_by: None,
            active_organization_id: None,
        })
        .await
        .unwrap();
    session.token
}

#[tokio::test]
async fn tools_list_contains_seeded_pipelines_and_builtins() {
    let (state, _tmp) = make_app_state(AuthLayer::None, None).await;
    let client = connect(serve(state).await).await;

    let info = client.peer_info().expect("server info");
    assert!(info.capabilities.tools.is_some(), "tools capability");
    assert!(
        info.instructions.as_deref().is_some_and(|i| !i.is_empty()),
        "instructions should be set"
    );

    let tools = client.list_all_tools().await.unwrap();
    let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
    assert!(names.contains(&"product-search"), "{names:?}");
    assert!(names.contains(&"query"), "{names:?}");
    assert!(names.contains(&"list_data_sources"), "{names:?}");

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn pipeline_call_executes_and_returns_rows() {
    let (state, _tmp) = make_app_state(AuthLayer::None, None).await;
    let client = connect(serve(state).await).await;

    // No tools/list first: per-call name resolution must hold on its own.
    let result = client
        .call_tool(call_params(
            "product-search",
            json!({"brand": "Apple", "max_price": 1500.0}),
        ))
        .await
        .unwrap();
    assert_eq!(result.is_error, Some(false), "{result:?}");
    let body = result_json(&result);
    assert_eq!(body["success"], json!(true), "{body}");
    // Two Apple rows are <= 1500 (ids 1 and 3); id=5 at 2499 is filtered out.
    assert_eq!(body["rows"].as_u64(), Some(2), "{body}");
    assert_eq!(body["data"][0]["id"], json!(1), "{body}");

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn query_round_trips() {
    let (state, _tmp) = make_app_state(AuthLayer::None, None).await;
    let client = connect(serve(state).await).await;

    let result = client
        .call_tool(call_params(
            "query",
            json!({"sql": "SELECT count(*) AS n FROM products"}),
        ))
        .await
        .unwrap();
    assert_eq!(result.is_error, Some(false), "{result:?}");
    let body = result_json(&result);
    assert_eq!(body["data"][0]["n"], json!(5), "{body}");

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn unknown_tool_is_invalid_params() {
    let (state, _tmp) = make_app_state(AuthLayer::None, None).await;
    let client = connect(serve(state).await).await;

    let err = client
        .call_tool(CallToolRequestParams::new("nope"))
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("unknown tool 'nope'"), "{msg}");
    assert!(msg.contains("tools/list"), "should nudge a re-list: {msg}");

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn failing_sql_surfaces_as_is_error_tool_result() {
    let (state, _tmp) = make_app_state(AuthLayer::None, None).await;
    let client = connect(serve(state).await).await;

    // Passes statement validation, fails in the engine — an execution
    // error the model should see and react to, not a protocol error.
    let result = client
        .call_tool(call_params(
            "query",
            json!({"sql": "SELECT * FROM no_such_table"}),
        ))
        .await
        .unwrap();
    assert_eq!(result.is_error, Some(true), "{result:?}");
    let content = serde_json::to_string(&result.content).unwrap();
    assert!(content.contains("no_such_table"), "{content}");

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn auth_forwarding_reaches_the_handlers() {
    let (state, _tmp) = make_app_state(better_auth_layer().await, None).await;
    let token = bearer_token(&state).await;
    let addr = serve(state).await;

    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(mcp_uri(addr)).auth_header(token),
    );
    let client = ().serve(transport).await.expect("bearer initialize");

    // A succeeding query proves `Authorization` survives synthetic dispatch:
    // a dropped header would pass the gate but 401 at handler-level
    // `require_session`, surfacing here as `is_error == Some(true)`.
    let result = client
        .call_tool(call_params("query", json!({"sql": "SELECT 1 AS one"})))
        .await
        .unwrap();
    assert_eq!(result.is_error, Some(false), "{result:?}");

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn anonymous_initialize_is_a_transport_401() {
    let (state, _tmp) = make_app_state(better_auth_layer().await, None).await;
    let addr = serve(state).await;

    // No credential: the handshake itself fails — a transport-level 401,
    // not an open session whose every call errors.
    let transport = StreamableHttpClientTransport::from_uri(mcp_uri(addr));
    assert!(
        ().serve(transport).await.is_err(),
        "anonymous initialize must fail the handshake"
    );

    // An invented Mcp-Session-Id changes nothing: the gate runs before
    // rmcp's session manager, so this is a 401, not a session-lookup error.
    let resp = reqwest::Client::new()
        .post(mcp_uri(addr))
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("mcp-session-id", "11111111-2222-3333-4444-555555555555")
        .body(json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
}

/// The stateless-protocol regression test: a 2026-07-28 client carries no
/// session, so every request stands alone — `tools/call` must resolve the
/// pipeline per-call with no `tools/list` state to lean on.
#[tokio::test]
async fn stateless_2026_07_28_client_lists_and_calls() {
    let (state, _tmp) = make_app_state(AuthLayer::None, None).await;
    let addr = serve(state).await;

    let transport = StreamableHttpClientTransport::from_uri(mcp_uri(addr));
    let client = ClientInfo::default()
        .with_protocol_version(ProtocolVersion::V_2026_07_28)
        .serve(transport)
        .await
        .expect("stateless initialize");
    // Guard the premise: the negotiated version is the stateless protocol
    // (rmcp serves >= 2026-07-28 statelessly regardless of session config).
    assert_eq!(
        client.peer_info().expect("server info").protocol_version,
        ProtocolVersion::V_2026_07_28
    );

    let tools = client.list_all_tools().await.unwrap();
    assert!(
        tools.iter().any(|t| t.name == "product-search"),
        "{tools:?}"
    );

    let result = client
        .call_tool(call_params(
            "product-search",
            json!({"brand": "Sony", "max_price": 500.0}),
        ))
        .await
        .unwrap();
    assert_eq!(result.is_error, Some(false), "{result:?}");
    let body = result_json(&result);
    assert_eq!(body["rows"].as_u64(), Some(1), "{body}");

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn stateless_pipeline_call_records_a_uuid_audit_session_id() {
    let audit_tmp = TempDir::new().unwrap();
    let db_path = audit_tmp.path().join("audit.db");
    let store = Arc::new(QueryAuditStore::open(&db_path).await.unwrap());
    let (state, _tmp) = make_app_state(AuthLayer::None, Some(Arc::clone(&store))).await;
    let addr = serve(state).await;

    // Stateless client: no Mcp-Session-Id anywhere, so the audit id must be
    // the handler's minted per-request UUID.
    let transport = StreamableHttpClientTransport::from_uri(mcp_uri(addr));
    let client = ClientInfo::default()
        .with_protocol_version(ProtocolVersion::V_2026_07_28)
        .serve(transport)
        .await
        .expect("stateless initialize");
    let result = client
        .call_tool(call_params(
            "product-search",
            json!({"brand": "Apple", "max_price": 1500.0}),
        ))
        .await
        .unwrap();
    assert_eq!(result.is_error, Some(false), "{result:?}");
    client.cancel().await.unwrap();

    // The ledger's read API is keyed by an already-known session id, and the
    // minted id is exactly what's under test — so read the column raw.
    let conn = tokio_rusqlite::Connection::open(&db_path).await.unwrap();
    let session_ids = conn
        .call(
            |conn| -> Result<Vec<Option<String>>, tokio_rusqlite::rusqlite::Error> {
                let mut stmt = conn.prepare("SELECT session_id FROM query_audit")?;
                let ids = stmt
                    .query_map([], |row| row.get::<_, Option<String>>(0))?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(ids)
            },
        )
        .await
        .unwrap();
    assert_eq!(session_ids.len(), 1, "{session_ids:?}");
    let sid = session_ids[0]
        .as_deref()
        .expect("the pipeline run must be attributed, not NULL");
    uuid::Uuid::parse_str(sid)
        .unwrap_or_else(|e| panic!("stateless audit id should be a minted UUID ({e}): {sid}"));

    // And it groups: the public read path finds the run under that id.
    let rows = store.list_by_session(sid).await.unwrap();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(
        rows[0]["statement_kind"],
        json!("pipeline"),
        "{:?}",
        rows[0]
    );
}
