//! MCP ⇄ REST bridge: a manual `ServerHandler` whose tools come from the
//! server's pipeline inventory at list time. All REST I/O goes through the
//! CLI's `ApiClient`; stdout belongs to the JSON-RPC transport, so nothing
//! here may print to it.
//!
//! The MCP trait methods are thin wrappers over inherent `do_*` methods:
//! `RequestContext` is `#[non_exhaustive]` and cannot be constructed in
//! tests, so the wiremock tests below drive the `do_*` methods directly and
//! the spawned-binary e2e (`tests/mcp_e2e.rs`) covers the trait wiring.

use std::collections::HashMap;
use std::sync::RwLock;

use rmcp::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, ErrorData,
    Implementation, JsonObject, ListToolsResult, PaginatedRequestParams, ServerCapabilities,
    ServerInfo,
};
use rmcp::service::{RequestContext, RoleServer};
use serde_json::Value;
use uuid::Uuid;

use crate::client::{ApiClient, ApiError, encode_component};
use crate::mcp::projection;

const INSTRUCTIONS: &str = "Skardi is a federated SQL data plane: operator-defined \
pipelines plus an ad-hoc SQL engine over the configured data sources. Prefer the \
pipeline tools for tasks they cover. Before writing ad-hoc SQL with `query`, call \
`list_data_sources` to see tables, schemas, and their plain-English descriptions.";

pub(crate) struct McpBridge {
    client: ApiClient,
    /// Tool name → original pipeline name; rebuilt on every tools/list, so a
    /// host that re-lists (or reconnects) sees a re-configured server's fresh
    /// inventory with no bridge restart.
    tool_map: RwLock<HashMap<String, String>>,
    /// One UUID per MCP connection, sent as `ai_context.session_id` whenever
    /// the model provides a `purpose`. This is the v1 carrier of the
    /// agent-identity passthrough seam.
    session_id: String,
}

impl McpBridge {
    pub(crate) fn new(client: ApiClient) -> Self {
        McpBridge {
            client,
            tool_map: RwLock::new(HashMap::new()),
            session_id: Uuid::new_v4().to_string(),
        }
    }

    pub(crate) async fn do_list_tools(&self) -> Result<ListToolsResult, ErrorData> {
        // Fetched on every call — no cache. Hosts list rarely (typically at
        // connect time) and the hop is usually localhost.
        let inventory = self
            .client
            .get("/pipelines")
            .await
            // ApiError::Connect's Display already names the resolved URL and
            // the three ways to configure it.
            .map_err(|e| ErrorData::internal_error(e.to_string(), None))?;
        let (tools, map) = projection::project(&inventory);
        *self.tool_map.write().unwrap_or_else(|p| p.into_inner()) = map;
        Ok(ListToolsResult::with_all_items(tools))
    }

    pub(crate) async fn do_call_tool(
        &self,
        name: &str,
        args: Option<JsonObject>,
    ) -> Result<CallToolResult, ErrorData> {
        let outcome = match name {
            projection::QUERY => self.call_query(args).await,
            projection::LIST_DATA_SOURCES => self.client.get("/data_source").await,
            _ => {
                let pipeline = self
                    .tool_map
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .get(name)
                    .cloned();
                match pipeline {
                    Some(pipeline) => {
                        // Arguments pass through as the flat execute body —
                        // the server is the validator. The session header
                        // gives pipeline runs the same ledger attribution as
                        // `query`'s ai_context.session_id (the server reads
                        // it in execute_pipeline_by_name), so the surface the
                        // INSTRUCTIONS steer the model toward is not the
                        // unattributed one.
                        let body = Value::Object(args.unwrap_or_default());
                        let path = format!("/{}/execute", encode_component(&pipeline));
                        self.client
                            .post_with_headers(
                                &path,
                                &body,
                                &[("x-skardi-session-id", &self.session_id)],
                            )
                            .await
                    }
                    None => {
                        // Protocol-level: covers host bugs and a server
                        // re-configured since the host last listed.
                        return Err(ErrorData::invalid_params(
                            format!(
                                "unknown tool '{name}' — the pipeline inventory may have \
                                 changed; re-issue tools/list to refresh it"
                            ),
                            None,
                        ));
                    }
                }
            }
        };
        Ok(match outcome {
            // Success: the response JSON verbatim, no client-side reshaping.
            Ok(value) => CallToolResult::success(vec![ContentBlock::text(value.to_string())]),
            // Execution errors are for the model to see and react to (fix a
            // parameter, pick another tool), not protocol errors. ApiError's
            // Display carries the server's message and error_type.
            Err(err) => CallToolResult::error(vec![ContentBlock::text(err.to_string())]),
        })
    }

    /// The one choke-point that assembles the `/query` body; later versions
    /// extend this to full identity injection without touching call sites.
    async fn call_query(&self, args: Option<JsonObject>) -> Result<Value, ApiError> {
        let args = args.unwrap_or_default();
        let mut body = serde_json::Map::new();
        for key in ["sql", "max_rows"] {
            if let Some(value) = args.get(key) {
                body.insert(key.to_string(), value.clone());
            }
        }
        // When `purpose` is absent, ai_context is omitted entirely — the
        // server rejects a partial object.
        if let Some(purpose) = args.get("purpose").and_then(Value::as_str) {
            let purpose = purpose.trim();
            if !purpose.is_empty() {
                body.insert(
                    "ai_context".to_string(),
                    serde_json::json!({"purpose": purpose, "session_id": self.session_id}),
                );
            }
        }
        self.client.post("/query", &Value::Object(body)).await
    }
}

impl ServerHandler for McpBridge {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("skardi", env!("CARGO_PKG_VERSION")))
            .with_instructions(INSTRUCTIONS)
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        self.do_list_tools().await
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        self.do_call_tool(&request.name, request.arguments)
            .await
            .map(CallToolResponse::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClientConfig;
    use serde_json::json;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_config(server: &str) -> ClientConfig {
        ClientConfig {
            server: server.to_string(),
            token: None,
            context: None,
        }
    }

    fn bridge_for(server: &MockServer) -> McpBridge {
        McpBridge::new(ApiClient::new(&test_config(&server.uri())).unwrap())
    }

    fn inventory() -> Value {
        json!({"success": true, "count": 1, "data_sources": 0,
               "pipelines": [{"name": "product-search", "version": "1.0.0",
                 "endpoint": "/product-search/execute",
                 "description": "Filter products",
                 "parameters": [{"name": "brand", "data_type": "Utf8",
                                 "json_schema": {"type": ["string", "null"]}}]}]})
    }

    #[tokio::test]
    async fn list_tools_projects_pipelines_and_builtins() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/pipelines"))
            .respond_with(ResponseTemplate::new(200).set_body_json(inventory()))
            .expect(1)
            .mount(&server)
            .await;
        let bridge = bridge_for(&server);
        let result = bridge.do_list_tools().await.unwrap();
        let names: Vec<&str> = result.tools.iter().map(|t| t.name.as_ref()).collect();
        assert!(names.contains(&"product-search"), "{names:?}");
        assert!(names.contains(&"query"), "{names:?}");
        assert!(names.contains(&"list_data_sources"), "{names:?}");
    }

    #[tokio::test]
    async fn list_tools_maps_connect_failure_to_a_protocol_error() {
        let bridge = McpBridge::new(ApiClient::new(&test_config("http://127.0.0.1:1")).unwrap());
        let err = bridge.do_list_tools().await.unwrap_err();
        assert!(
            err.message.contains("cannot reach skardi-server"),
            "{}",
            err.message
        );
    }

    #[tokio::test]
    async fn pipeline_call_posts_flat_body_and_returns_verbatim_json() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/pipelines"))
            .respond_with(ResponseTemplate::new(200).set_body_json(inventory()))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/product-search/execute"))
            .and(body_json(json!({"brand": "acme"})))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"success": true, "data": [], "rows": 0})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let bridge = bridge_for(&server);
        bridge.do_list_tools().await.unwrap(); // builds the dispatch map
        let args = json!({"brand": "acme"}).as_object().cloned();
        let result = bridge.do_call_tool("product-search", args).await.unwrap();
        assert_eq!(result.is_error, Some(false));
    }

    #[tokio::test]
    async fn pipeline_call_carries_the_connection_session_id_header() {
        // Ledger attribution parity: pipeline runs must land under the same
        // session id that `query` sends in ai_context.session_id.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/pipelines"))
            .respond_with(ResponseTemplate::new(200).set_body_json(inventory()))
            .mount(&server)
            .await;
        let bridge = bridge_for(&server);
        Mock::given(method("POST"))
            .and(path("/product-search/execute"))
            .and(wiremock::matchers::header(
                "x-skardi-session-id",
                bridge.session_id.as_str(),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"success": true})))
            .expect(1)
            .mount(&server)
            .await;
        bridge.do_list_tools().await.unwrap();
        let result = bridge
            .do_call_tool("product-search", json!({"brand": "x"}).as_object().cloned())
            .await
            .unwrap();
        assert_eq!(result.is_error, Some(false));
    }

    #[tokio::test]
    async fn unknown_tool_is_a_protocol_error_nudging_a_relist() {
        let server = MockServer::start().await;
        let bridge = bridge_for(&server);
        let err = bridge.do_call_tool("nope", None).await.unwrap_err();
        assert!(err.message.contains("unknown tool"), "{}", err.message);
        assert!(err.message.contains("tools/list"), "{}", err.message);
    }

    #[tokio::test]
    async fn query_with_purpose_sends_ai_context_with_a_stable_session_id() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/query"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"success": true, "data": [], "rows": 0})),
            )
            .expect(2)
            .mount(&server)
            .await;
        let bridge = bridge_for(&server);
        for _ in 0..2 {
            let args = json!({"sql": "select 1", "purpose": "why"})
                .as_object()
                .cloned();
            let result = bridge.do_call_tool("query", args).await.unwrap();
            assert_eq!(result.is_error, Some(false));
        }
        let requests = server.received_requests().await.unwrap();
        let bodies: Vec<Value> = requests
            .iter()
            .map(|r| serde_json::from_slice(&r.body).unwrap())
            .collect();
        assert_eq!(bodies[0]["ai_context"]["purpose"], json!("why"));
        let sid0 = bodies[0]["ai_context"]["session_id"].as_str().unwrap();
        let sid1 = bodies[1]["ai_context"]["session_id"].as_str().unwrap();
        assert_eq!(sid0, sid1, "session id must be stable per connection");
        assert!(!sid0.is_empty());
    }

    #[tokio::test]
    async fn query_without_purpose_omits_ai_context_entirely() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/query"))
            .and(body_json(json!({"sql": "select 1", "max_rows": 5})))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"success": true, "data": [], "rows": 0})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let bridge = bridge_for(&server);
        let args = json!({"sql": "select 1", "max_rows": 5})
            .as_object()
            .cloned();
        let result = bridge.do_call_tool("query", args).await.unwrap();
        assert_eq!(result.is_error, Some(false));
    }

    #[tokio::test]
    async fn server_error_becomes_is_error_tool_result_with_error_type() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/query"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "success": false,
                "error": "Missing required parameters: brand",
                "error_type": "parameter_validation_error"
            })))
            .mount(&server)
            .await;
        let bridge = bridge_for(&server);
        let args = json!({"sql": "select 1"}).as_object().cloned();
        let result = bridge.do_call_tool("query", args).await.unwrap();
        assert_eq!(result.is_error, Some(true));
        let text = serde_json::to_string(&result.content).unwrap();
        assert!(text.contains("parameter_validation_error"), "{text}");
        assert!(text.contains("Missing required parameters"), "{text}");
    }

    #[tokio::test]
    async fn list_data_sources_proxies_get_data_source_verbatim() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/data_source"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"success": true, "data": [], "count": 0})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let bridge = bridge_for(&server);
        let result = bridge
            .do_call_tool("list_data_sources", None)
            .await
            .unwrap();
        assert_eq!(result.is_error, Some(false));
    }
}
