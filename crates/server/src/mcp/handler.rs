//! MCP ⇄ REST protocol adapter: a stateless `ServerHandler` whose every tool
//! call becomes a synthetic HTTP request dispatched in-process against the
//! server's own REST router (`tower::ServiceExt::oneshot`) — the same
//! mechanism the HTTP tests use. The captured router is the pre-middleware
//! one; see `configure_routes` for the transport-vs-execution boundary.
//!
//! Stateless by protocol: MCP `2026-07-28` removes sessions (SEP-2567) and
//! rmcp constructs this handler once per request on that path, so it owns
//! only the Arc-cheap `Router` clone — no tool map, no per-session state.
//! Pipeline tool names are resolved per call.
//!
//! The MCP trait methods are thin wrappers over inherent `do_*` methods
//! (taking the inbound headers as a plain parameter): `RequestContext` is
//! `#[non_exhaustive]`, so tests drive the `do_*` methods directly.

use std::collections::HashMap;

use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, Method, Request, StatusCode, header};
use rmcp::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, ErrorData,
    Implementation, JsonObject, ListToolsResult, PaginatedRequestParams, ServerCapabilities,
    ServerInfo, Tool,
};
use rmcp::service::{RequestContext, RoleServer};
use serde_json::Value;
use skardi_mcp_core::{encode_component, projection};
use tower::ServiceExt;
use uuid::Uuid;

use crate::session_header::{SESSION_ID_HEADER, validate_session_id};

/// Same wording as the stdio bridge (`crates/cli/src/mcp/bridge.rs`) — the
/// two bindings present one product to hosts. Kept per-binding rather than
/// in skardi-mcp-core so either side can diverge deliberately.
const INSTRUCTIONS: &str = "Skardi is a federated SQL data plane: operator-defined \
pipelines plus an ad-hoc SQL engine over the configured data sources. Prefer the \
pipeline tools for tasks they cover. Before writing ad-hoc SQL with `query`, call \
`list_data_sources` to see tables, schemas, and their plain-English descriptions.";

/// Ceiling on a synthetic response body — the same 256 MB the stdio bridge
/// inherits from the CLI client (`crates/cli/src/client.rs`), kept as a
/// constant so one pipeline behaves identically through both bindings (not
/// shared because the CLI's ceiling is client-wide, not MCP-specific).
/// In-process this bounds only the MCP layer's own copy of the result: the
/// REST handler has already materialized the full result before the
/// collector sees a byte — execution-layer resource governance is a
/// recorded server-wide non-goal, not this cap's job.
const MAX_RESPONSE_BYTES: usize = 256 * 1024 * 1024;

/// The legacy-protocol session header rmcp echoes on every request of a
/// managed session (`legacy_session_mode`). Stateless-protocol requests
/// (`2026-07-28` and later) don't carry it.
const MCP_SESSION_ID: &str = "mcp-session-id";

#[derive(Clone)]
pub(crate) struct McpHandler {
    /// Pre-middleware capture of the REST router (routing table + handlers,
    /// where every execution concern lives: `require_session`, parameter
    /// validation, audit recording). Arc-cheap to clone; the service factory
    /// clones it per request.
    rest: Router,
}

impl McpHandler {
    pub(crate) fn new(rest: Router) -> Self {
        McpHandler { rest }
    }

    /// The audit id for this request, layered: a valid legacy
    /// `Mcp-Session-Id` groups the session's calls together (matching the
    /// stdio bridge's per-connection grouping); anything else — a stateless
    /// request, or a malformed value — gets a per-request UUID, so
    /// attribution stays intact and grouping granularity is honestly
    /// per-request. rmcp mints UUIDs, which always pass the validator, but
    /// the fallback must not lean on that invisible coupling: a malformed
    /// value would otherwise 400 the whole execute at the forwarding
    /// target's strict validator (`session_id_from_headers`).
    fn request_session_id(headers: &HeaderMap) -> String {
        headers
            .get(MCP_SESSION_ID)
            .and_then(|v| v.to_str().ok())
            .filter(|s| validate_session_id(s).is_ok())
            .map(str::to_string)
            .unwrap_or_else(|| Uuid::new_v4().to_string())
    }

    /// Build and dispatch one synthetic request against the captured REST
    /// router. Forwards the inbound `Authorization` verbatim (omitted when
    /// the caller sent none) so handler-level enforcement, authorization
    /// errors, and ledger recording are exactly the REST semantics; sets
    /// `content-type: application/json` when there is a body.
    async fn dispatch(
        &self,
        method: Method,
        path: &str,
        inbound: &HeaderMap,
        extra_headers: &[(&str, &str)],
        body: Option<Value>,
    ) -> Result<(StatusCode, String), ErrorData> {
        let mut builder = Request::builder().method(method).uri(path);
        if let Some(auth) = inbound.get(header::AUTHORIZATION) {
            builder = builder.header(header::AUTHORIZATION, auth.clone());
        }
        for (name, value) in extra_headers {
            builder = builder.header(*name, *value);
        }
        let request = match body {
            Some(v) => builder
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(v.to_string())),
            None => builder.body(Body::empty()),
        }
        .map_err(|e| ErrorData::internal_error(format!("synthetic request: {e}"), None))?;
        // In-process: a dispatch failure is a bug, not a network condition.
        let response = self
            .rest
            .clone()
            .oneshot(request)
            .await
            .map_err(|e| ErrorData::internal_error(format!("synthetic dispatch: {e}"), None))?;
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), MAX_RESPONSE_BYTES)
            .await
            .map_err(|e| {
                ErrorData::internal_error(format!("collecting synthetic response: {e}"), None)
            })?;
        Ok((status, String::from_utf8_lossy(&bytes).into_owned()))
    }

    /// Fetch and project the live inventory. Used by `tools/list` and by
    /// every pipeline `tools/call` — per-call name resolution: one extra
    /// in-process dispatch buys correctness in both protocol modes and
    /// freshness by construction (a stale-map "unknown tool" cannot exist).
    async fn project_inventory(
        &self,
        headers: &HeaderMap,
    ) -> Result<(Vec<Tool>, HashMap<String, String>), ErrorData> {
        let (status, body) = self
            .dispatch(Method::GET, "/pipelines", headers, &[], None)
            .await?;
        if !status.is_success() {
            return Err(ErrorData::internal_error(
                format!("GET /pipelines answered {status}: {body}"),
                None,
            ));
        }
        let inventory: Value = serde_json::from_str(&body).map_err(|e| {
            ErrorData::internal_error(format!("GET /pipelines returned non-JSON: {e}"), None)
        })?;
        Ok(projection::project(&inventory))
    }

    pub(crate) async fn do_list_tools(
        &self,
        headers: &HeaderMap,
    ) -> Result<ListToolsResult, ErrorData> {
        let (tools, _) = self.project_inventory(headers).await?;
        Ok(ListToolsResult::with_all_items(tools))
    }

    pub(crate) async fn do_call_tool(
        &self,
        name: &str,
        args: Option<JsonObject>,
        headers: &HeaderMap,
    ) -> Result<CallToolResult, ErrorData> {
        let session_id = Self::request_session_id(headers);
        let (status, body) = match name {
            projection::QUERY => {
                let body = Self::query_body(args, &session_id);
                self.dispatch(Method::POST, "/query", headers, &[], Some(body))
                    .await?
            }
            projection::LIST_DATA_SOURCES => {
                self.dispatch(Method::GET, "/data_source", headers, &[], None)
                    .await?
            }
            _ => {
                let (_, map) = self.project_inventory(headers).await?;
                let Some(pipeline) = map.get(name) else {
                    // Protocol-level: covers host bugs and a server
                    // re-configured since the host last listed.
                    return Err(ErrorData::invalid_params(
                        format!(
                            "unknown tool '{name}' — the pipeline inventory may have \
                             changed; re-issue tools/list to refresh it"
                        ),
                        None,
                    ));
                };
                // Arguments pass through as the flat execute body — the
                // server is the validator. The session header gives pipeline
                // runs the same ledger attribution as `query`'s
                // ai_context.session_id.
                let body = Value::Object(args.unwrap_or_default());
                let path = format!("/{}/execute", encode_component(pipeline));
                self.dispatch(
                    Method::POST,
                    &path,
                    headers,
                    &[(SESSION_ID_HEADER, &session_id)],
                    Some(body),
                )
                .await?
            }
        };
        Ok(if status.is_success() {
            // The response JSON verbatim, no reshaping.
            CallToolResult::success(vec![ContentBlock::text(body)])
        } else {
            // Execution errors are for the model to see and react to (fix a
            // parameter, pick another tool), not protocol errors.
            CallToolResult::error(vec![ContentBlock::text(body)])
        })
    }

    /// The one choke-point that assembles the `/query` body. When `purpose`
    /// is absent, ai_context is omitted entirely — the server rejects a
    /// partial object.
    fn query_body(args: Option<JsonObject>, session_id: &str) -> Value {
        let args = args.unwrap_or_default();
        let mut body = serde_json::Map::new();
        for key in ["sql", "max_rows"] {
            if let Some(value) = args.get(key) {
                body.insert(key.to_string(), value.clone());
            }
        }
        if let Some(purpose) = args.get("purpose").and_then(Value::as_str) {
            let purpose = purpose.trim();
            if !purpose.is_empty() {
                body.insert(
                    "ai_context".to_string(),
                    serde_json::json!({"purpose": purpose, "session_id": session_id}),
                );
            }
        }
        Value::Object(body)
    }

    /// The inbound request's headers, from the `http::request::Parts` rmcp
    /// injects into every request's extensions (stateless mode included;
    /// verified against the 3.1.4 source).
    fn inbound_headers(context: &RequestContext<RoleServer>) -> HeaderMap {
        context
            .extensions
            .get::<axum::http::request::Parts>()
            .map(|parts| parts.headers.clone())
            .unwrap_or_default()
    }
}

impl ServerHandler for McpHandler {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("skardi", env!("CARGO_PKG_VERSION")))
            .with_instructions(INSTRUCTIONS)
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        self.do_list_tools(&Self::inbound_headers(&context)).await
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        self.do_call_tool(
            &request.name,
            request.arguments,
            &Self::inbound_headers(&context),
        )
        .await
        .map(CallToolResponse::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use axum::response::IntoResponse;
    use axum::routing::{get, post};
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    /// What one synthetic request carried, as the REST layer would see it.
    #[derive(Debug)]
    struct SeenRequest {
        path: String,
        authorization: Option<String>,
        session_header: Option<String>,
        body: Value,
    }

    /// A stand-in for the REST router that records what the handler
    /// dispatched — the in-process analogue of the bridge's wiremock tests.
    #[derive(Clone)]
    struct Probe {
        seen: Arc<Mutex<Vec<SeenRequest>>>,
        inventory: Value,
        respond_status: StatusCode,
        respond_body: Value,
    }

    impl Probe {
        fn new(inventory: Value) -> Self {
            Probe {
                seen: Arc::new(Mutex::new(Vec::new())),
                inventory,
                respond_status: StatusCode::OK,
                respond_body: json!({"success": true}),
            }
        }

        fn take_seen(&self) -> Vec<SeenRequest> {
            std::mem::take(&mut self.seen.lock().unwrap())
        }
    }

    async fn record(State(probe): State<Probe>, req: axum::extract::Request) -> impl IntoResponse {
        let (parts, body) = req.into_parts();
        let bytes = axum::body::to_bytes(body, 1 << 20).await.unwrap();
        let header = |name: &str| {
            parts
                .headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(String::from)
        };
        probe.seen.lock().unwrap().push(SeenRequest {
            path: parts.uri.path().to_string(),
            authorization: header("authorization"),
            session_header: header(SESSION_ID_HEADER),
            body: if bytes.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&bytes).unwrap()
            },
        });
        (probe.respond_status, axum::Json(probe.respond_body.clone()))
    }

    async fn serve_inventory(State(probe): State<Probe>) -> impl IntoResponse {
        axum::Json(probe.inventory.clone())
    }

    fn probe_router(probe: Probe) -> Router {
        Router::new()
            .route("/pipelines", get(serve_inventory))
            .route("/query", post(record))
            .route("/data_source", get(record))
            .route("/:name/execute", post(record))
            .with_state(probe)
    }

    fn inventory() -> Value {
        json!({"success": true, "count": 1, "data_sources": 0,
               "pipelines": [{"name": "product-search", "version": "1.0.0",
                 "endpoint": "/product-search/execute",
                 "description": "Filter products",
                 "parameters": [{"name": "brand", "data_type": "Utf8",
                                 "json_schema": {"type": ["string", "null"]}}]}]})
    }

    fn handler_with_probe() -> (McpHandler, Probe) {
        let probe = Probe::new(inventory());
        (McpHandler::new(probe_router(probe.clone())), probe)
    }

    fn args(v: Value) -> Option<JsonObject> {
        v.as_object().cloned()
    }

    #[tokio::test]
    async fn list_tools_projects_pipelines_and_builtins() {
        let (handler, _) = handler_with_probe();
        let result = handler.do_list_tools(&HeaderMap::new()).await.unwrap();
        let names: Vec<&str> = result.tools.iter().map(|t| t.name.as_ref()).collect();
        assert!(names.contains(&"product-search"), "{names:?}");
        assert!(names.contains(&"query"), "{names:?}");
        assert!(names.contains(&"list_data_sources"), "{names:?}");
    }

    #[tokio::test]
    async fn pipeline_call_resolves_per_call_and_posts_flat_body() {
        // Deliberately NO prior do_list_tools: per-call resolution is the
        // point — a stateless request's handler instance has never listed.
        let (handler, probe) = handler_with_probe();
        let result = handler
            .do_call_tool(
                "product-search",
                args(json!({"brand": "acme"})),
                &HeaderMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(result.is_error, Some(false));
        let seen = probe.take_seen();
        assert_eq!(seen.len(), 1, "{seen:?}");
        assert_eq!(seen[0].path, "/product-search/execute");
        assert_eq!(seen[0].body, json!({"brand": "acme"}));
    }

    #[tokio::test]
    async fn authorization_is_forwarded_verbatim_and_absent_when_missing() {
        let (handler, probe) = handler_with_probe();
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer tok-1".parse().unwrap());
        handler
            .do_call_tool("query", args(json!({"sql": "select 1"})), &headers)
            .await
            .unwrap();
        handler
            .do_call_tool("query", args(json!({"sql": "select 1"})), &HeaderMap::new())
            .await
            .unwrap();
        let seen = probe.take_seen();
        assert_eq!(seen[0].authorization.as_deref(), Some("Bearer tok-1"));
        assert_eq!(seen[1].authorization, None);
    }

    #[tokio::test]
    async fn legacy_mcp_session_id_is_echoed_as_the_audit_id() {
        let (handler, probe) = handler_with_probe();
        let mut headers = HeaderMap::new();
        headers.insert(MCP_SESSION_ID, "legacy-42".parse().unwrap());
        handler
            .do_call_tool("product-search", args(json!({"brand": "x"})), &headers)
            .await
            .unwrap();
        handler
            .do_call_tool(
                "query",
                args(json!({"sql": "select 1", "purpose": "why"})),
                &headers,
            )
            .await
            .unwrap();
        let seen = probe.take_seen();
        assert_eq!(seen[0].session_header.as_deref(), Some("legacy-42"));
        assert_eq!(seen[1].body["ai_context"]["session_id"], json!("legacy-42"));
        assert_eq!(seen[1].body["ai_context"]["purpose"], json!("why"));
    }

    #[tokio::test]
    async fn malformed_mcp_session_id_falls_back_to_a_minted_uuid() {
        // The forwarding target 400s a malformed x-skardi-session-id
        // (session_id_from_headers); the handler must not propagate a
        // caller-minted bad value into that — it loses the grouping, not
        // the call.
        let (handler, probe) = handler_with_probe();
        let mut headers = HeaderMap::new();
        headers.insert(MCP_SESSION_ID, "has space".parse().unwrap());
        let result = handler
            .do_call_tool("product-search", args(json!({"brand": "x"})), &headers)
            .await
            .unwrap();
        assert_eq!(result.is_error, Some(false));
        let seen = probe.take_seen();
        let forwarded = seen[0].session_header.as_deref().unwrap();
        assert!(
            Uuid::parse_str(forwarded).is_ok(),
            "expected a minted UUID, got {forwarded:?}"
        );
    }

    #[tokio::test]
    async fn stateless_request_mints_a_uuid_per_request() {
        let (handler, probe) = handler_with_probe();
        for _ in 0..2 {
            handler
                .do_call_tool(
                    "product-search",
                    args(json!({"brand": "x"})),
                    &HeaderMap::new(),
                )
                .await
                .unwrap();
        }
        let seen = probe.take_seen();
        let first = seen[0].session_header.as_deref().unwrap();
        let second = seen[1].session_header.as_deref().unwrap();
        assert!(Uuid::parse_str(first).is_ok(), "{first:?}");
        assert!(Uuid::parse_str(second).is_ok(), "{second:?}");
        assert_ne!(
            first, second,
            "stateless grouping is honestly per-request — ids must differ"
        );
    }

    #[tokio::test]
    async fn unknown_tool_is_invalid_params_nudging_a_relist() {
        let (handler, _) = handler_with_probe();
        let err = handler
            .do_call_tool("nope", None, &HeaderMap::new())
            .await
            .unwrap_err();
        assert!(err.message.contains("unknown tool"), "{}", err.message);
        assert!(err.message.contains("tools/list"), "{}", err.message);
    }

    #[tokio::test]
    async fn non_2xx_becomes_is_error_with_the_body_text() {
        let mut probe = Probe::new(inventory());
        probe.respond_status = StatusCode::BAD_REQUEST;
        probe.respond_body = json!({
            "success": false,
            "error": "Missing required parameters: brand",
            "error_type": "parameter_validation_error"
        });
        let handler = McpHandler::new(probe_router(probe.clone()));
        let result = handler
            .do_call_tool("query", args(json!({"sql": "select 1"})), &HeaderMap::new())
            .await
            .unwrap();
        assert_eq!(result.is_error, Some(true));
        let text = serde_json::to_string(&result.content).unwrap();
        assert!(text.contains("parameter_validation_error"), "{text}");
        assert!(text.contains("Missing required parameters"), "{text}");
    }

    #[tokio::test]
    async fn query_without_purpose_omits_ai_context_entirely() {
        let (handler, probe) = handler_with_probe();
        handler
            .do_call_tool(
                "query",
                args(json!({"sql": "select 1", "max_rows": 5})),
                &HeaderMap::new(),
            )
            .await
            .unwrap();
        let seen = probe.take_seen();
        assert_eq!(seen[0].body, json!({"sql": "select 1", "max_rows": 5}));
    }

    #[tokio::test]
    async fn list_data_sources_proxies_get_data_source() {
        let (handler, probe) = handler_with_probe();
        let result = handler
            .do_call_tool("list_data_sources", None, &HeaderMap::new())
            .await
            .unwrap();
        assert_eq!(result.is_error, Some(false));
        let seen = probe.take_seen();
        assert_eq!(seen[0].path, "/data_source");
    }
}
