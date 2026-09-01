//! MCP over streamable HTTP at `/mcp` — a protocol adapter over the
//! server's own router, not a second execution path. Design:
//! `docs/superpowers/specs/2026-08-28-mcp-http-transport-design.md`.

mod gate;
pub(crate) mod handler;

use std::sync::Arc;

use axum::Router;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};

use crate::server::AppState;
use gate::SessionGate;
use handler::McpHandler;

/// Nest the gated MCP service at `/mcp`. `rest` is deliberately the
/// pre-middleware router: transport middleware (CORS today) applies exactly
/// once, to the inbound `/mcp` request — capturing the post-middleware
/// router would run it a second time on every synthetic dispatch. Dispatch
/// only ever targets REST paths, so no recursion into `/mcp` is possible.
///
/// Known, accepted shadow: the nest hides REST's `/:name/execute` from
/// every URL-borne caller for a pipeline literally named `mcp`; only `/mcp`
/// itself still reaches it, via this pre-nest capture.
pub(crate) fn attach(rest: Router, state: AppState) -> Router {
    let allowed_hosts = allowed_hosts(&state);
    let handler_rest = rest.clone();
    let service = StreamableHttpService::new(
        move || Ok(McpHandler::new(handler_rest.clone())),
        Arc::new(LocalSessionManager::default()),
        // The config is #[non_exhaustive], so it's built from the defaults
        // via the builder. Everything except allowed_hosts stays at rmcp's
        // defaults, each one a spec decision: sse_keep_alive 15s (bytes
        // keep flowing during long tool calls, resetting reverse-proxy
        // idle timers well under nginx's 60s proxy_read_timeout default),
        // legacy_session_mode true (legacy-protocol clients keep the
        // session behavior they expect; the gate guards the state they
        // create), json_response false (SSE responses, so the keep-alive
        // applies), and allowed_origins empty (MCP hosts are not browsers;
        // the Bearer-only gate is the actual barrier, so there is no
        // CSRF-shaped surface for Origin to guard).
        StreamableHttpServerConfig::default().with_allowed_hosts(allowed_hosts),
    );
    rest.nest_service("/mcp", SessionGate::new(service, state))
}

/// Additive host allowlist: the loopback trio is always allowed (rmcp's
/// DNS-rebinding default, which protects developers running on loopback),
/// declared `--mcp-allowed-host` values are appended. Deliberately no
/// allow-any escape hatch — a public deployment names its hostnames.
fn allowed_hosts(state: &AppState) -> Vec<String> {
    let mut hosts = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ];
    hosts.extend(
        state
            .config
            .read()
            .unwrap()
            .args
            .mcp_allowed_hosts
            .iter()
            .cloned(),
    );
    hosts
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{HeaderMap, Request, StatusCode};
    use datafusion::prelude::SessionContext;
    use serde_json::json;
    use skardi::engine::datafusion::DataFusionEngine;
    use tower::ServiceExt;

    use crate::auth::layer::AuthLayer;
    use crate::auth::mode::AuthMode;
    use crate::config::{CliArgs, ServerConfig};
    use crate::mcp::handler::McpHandler;
    use crate::semantics::SemanticsRegistry;
    use crate::server::{AppState, configure_middleware, configure_routes, rest_router};

    fn make_state(auth_layer: AuthLayer, mcp_allowed_hosts: Vec<String>) -> AppState {
        let config = ServerConfig {
            pipelines: HashMap::new(),
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
                mcp_allowed_hosts,
            },
        };
        let session_ctx = Arc::new(SessionContext::new());
        let engine = Arc::new(DataFusionEngine::new_with_arc(session_ctx.clone()));
        AppState::new(
            config,
            engine,
            session_ctx,
            auth_layer,
            None,
            None,
            Default::default(),
        )
    }

    async fn better_auth_layer() -> AuthLayer {
        // Held only across the env mutation + `build` below: once the layer
        // exists it no longer reads the environment.
        let _env = crate::auth::test_env::lock().await;
        unsafe {
            std::env::set_var("AUTH_SECRET", "test-secret-that-is-at-least-32-characters!");
            std::env::set_var("AUTH_DB_PATH", ":memory:");
            std::env::remove_var("AUTH_BASE_URL");
        }
        AuthLayer::build(&AuthMode::BetterAuthDieselSqlite)
            .await
            .unwrap()
    }

    /// Mint a real user + session and return the Bearer token (and the
    /// cookie name for the cookie-only case).
    async fn bearer_session(state: &AppState) -> (String, String) {
        use better_auth::{SessionOps, UserOps};
        let auth = state.auth_layer.as_better_auth().unwrap();
        let user = auth
            .database()
            .create_user(better_auth::types_mod::CreateUser {
                name: Some("mcp".into()),
                email: Some("mcp@test.com".into()),
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
        let cookie_name = auth.config().session.cookie_name.clone();
        (session.token, cookie_name)
    }

    fn initialize_body() -> String {
        json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "0"}
            }
        })
        .to_string()
    }

    /// POST an `initialize` to `/mcp` on the given router. `Host` is set
    /// explicitly: synthetic `oneshot` requests carry no `Host`, which
    /// rmcp's DNS-rebinding validation rejects with 400 before anything
    /// else runs.
    async fn post_initialize(
        app: axum::Router,
        host: &str,
        extra_headers: &[(&str, &str)],
    ) -> axum::response::Response {
        let mut builder = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("host", host)
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream");
        for (name, value) in extra_headers {
            builder = builder.header(*name, *value);
        }
        app.oneshot(builder.body(Body::from(initialize_body())).unwrap())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn session_gate_is_bearer_only_and_precedes_session_creation() {
        let state = make_state(better_auth_layer().await, vec![]);
        let (token, cookie_name) = bearer_session(&state).await;

        // Anonymous initialize: transport-level 401 — rmcp never runs, so
        // no session state is created for the caller.
        let resp = post_initialize(configure_routes(state.clone()), "127.0.0.1", &[]).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        // Cookie-only: a valid session cookie must NOT pass — synthetic
        // requests forward only Authorization, so admitting it would trade
        // this 401 for every tool call failing inside HTTP 200s.
        let cookie = format!("{cookie_name}={token}");
        let resp = post_initialize(
            configure_routes(state.clone()),
            "127.0.0.1",
            &[("cookie", cookie.as_str())],
        )
        .await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        // The same initialize with the Bearer token passes the gate.
        let auth = format!("Bearer {token}");
        let resp = post_initialize(
            configure_routes(state),
            "127.0.0.1",
            &[("authorization", auth.as_str())],
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn session_gate_is_a_noop_without_auth() {
        let state = make_state(AuthLayer::None, vec![]);
        let resp = post_initialize(configure_routes(state), "127.0.0.1", &[]).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn host_allowlist_is_loopback_plus_declared() {
        // Default configuration: loopback only.
        let state = make_state(AuthLayer::None, vec![]);
        let resp = post_initialize(configure_routes(state.clone()), "api.example.com", &[]).await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        let resp = post_initialize(configure_routes(state), "127.0.0.1", &[]).await;
        assert_eq!(resp.status(), StatusCode::OK);

        // Declared host: allowed, loopback still allowed (additive).
        let state = make_state(AuthLayer::None, vec!["api.example.com".to_string()]);
        let resp = post_initialize(configure_routes(state.clone()), "api.example.com", &[]).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let resp = post_initialize(configure_routes(state.clone()), "localhost", &[]).await;
        assert_eq!(resp.status(), StatusCode::OK);
        // Undeclared non-loopback host stays rejected — no allow-any.
        let resp = post_initialize(configure_routes(state), "evil.example", &[]).await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    /// The transport-vs-execution middleware boundary, pinned two-sided:
    /// transport middleware (CORS) wraps the nested MCP service exactly
    /// once, and the captured router the handler dispatches through is the
    /// pre-middleware one. The second half is a direct pin — the handler
    /// consumes synthetic responses body-only, so a double-wrapped capture
    /// would never show up in `/mcp` response headers.
    #[tokio::test]
    async fn middleware_wraps_mcp_once_and_synthetic_dispatch_not_at_all() {
        let state = make_state(AuthLayer::None, vec![]);

        let app = configure_middleware(configure_routes(state.clone()));
        let resp = post_initialize(app, "127.0.0.1", &[("origin", "https://example.com")]).await;
        let acao: Vec<_> = resp
            .headers()
            .get_all("access-control-allow-origin")
            .iter()
            .collect();
        assert_eq!(
            acao.len(),
            1,
            "transport middleware must wrap /mcp exactly once: {acao:?}"
        );

        let rest = rest_router(state);
        let resp = rest
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/pipelines")
                    .header("origin", "https://example.com")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            resp.headers().get("access-control-allow-origin").is_none(),
            "the captured router must be the pre-middleware one"
        );
    }

    /// The second layer of defense, tested directly: the session gate makes
    /// the no-credential path unreachable end-to-end, so handler-level
    /// `require_session` (reached via synthetic dispatch) needs its own
    /// regression test. If auth ever migrated into `configure_middleware`,
    /// this call would start succeeding — the tripwire for the
    /// middleware-boundary constraint the design depends on.
    #[tokio::test]
    async fn do_call_tool_hits_handler_level_auth_without_a_bearer() {
        let state = make_state(better_auth_layer().await, vec![]);
        let handler = McpHandler::new(rest_router(state));
        let result = handler
            .do_call_tool(
                "query",
                json!({"sql": "select 1"}).as_object().cloned(),
                &HeaderMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(result.is_error, Some(true));
        let text = serde_json::to_string(&result.content).unwrap();
        assert!(
            text.contains("unauthorized") || text.contains("Authentication required"),
            "expected the handler-level 401 to surface in the tool result: {text}"
        );
    }
}
