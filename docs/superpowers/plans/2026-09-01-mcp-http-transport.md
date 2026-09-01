# MCP Streamable HTTP Transport (`/mcp`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** skardi-server speaks MCP over streamable HTTP at `/mcp` — a protocol adapter that translates every tool call into a synthetic in-process request against the server's own REST router.

**Architecture:** A new `skardi-mcp-core` crate holds the projection layer shared with the stdio bridge; a new `crates/server/src/mcp/` module implements a stateless `ServerHandler` that self-dispatches via `tower::ServiceExt::oneshot` over a pre-middleware capture of the REST router, wrapped by a Bearer-only session gate and rmcp's streamable-HTTP service nested at `/mcp`.

**Tech Stack:** Rust, axum 0.7, tower 0.4 (`util`), rmcp `=3.1.4` (`server`, `transport-streamable-http-server`), uuid, percent-encoding.

**Spec:** `docs/superpowers/specs/2026-08-28-mcp-http-transport-design.md` — the plan argues from the spec; executors read both.

## Global Constraints

- **No local test runs** (repo policy): verification is GitHub CI only. Before every push run `cargo fmt` — nothing else. Task-level "verify" steps mean "will be verified by CI on the PR"; write the tests, don't run them.
- CI runs all `#[ignore]` tests bare — nothing in this plan needs `#[ignore]` (spec: all tests self-contained).
- rmcp is pinned `=3.1.4` everywhere; `default-features = false` on every rmcp dependency entry.
- The CLI refactor must be behavior-neutral: existing bridge unit tests and `mcp_e2e` pass unchanged (except import paths).
- All work lands on branch `mcp-http-transport` (created from `main`); commits per task; draft PR at the end.
- Commit messages in English, ending with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

Verified API facts (from rmcp 3.1.4 source, so tasks don't re-derive them):

- `StreamableHttpService::new(factory: impl Fn() -> Result<S, io::Error>, Arc<LocalSessionManager>, StreamableHttpServerConfig)`; the service implements `tower_service::Service<http::Request<B>>` with `Response = BoxResponse` (`http::Response<BoxBody<Bytes, Infallible>>`), `Error = Infallible`, and is `Clone`.
- `StreamableHttpServerConfig::default()`: `sse_keep_alive: Some(15s)`, `legacy_session_mode: true`, `json_response: false`, `allowed_hosts: ["localhost", "127.0.0.1", "::1"]`, `allowed_origins: []` (empty = Origin validation disabled).
- `with_allowed_hosts` **replaces** the list — additive semantics means we build `loopback trio + declared` ourselves.
- Host validation: missing `Host` header AND no `:authority` → 400; disallowed → 403. Tests driving `/mcp` via `oneshot` must set a `Host` header explicitly.
- rmcp injects `http::request::Parts` into `RequestContext::extensions` (`ctx.extensions.get::<http::request::Parts>()`) on every request, stateless mode included.
- Client side for tests: features `client` + `transport-streamable-http-client-reqwest`; `StreamableHttpClientTransport::from_uri(...)` / `StreamableHttpClientTransport::with_client(reqwest::Client, StreamableHttpClientTransportConfig::with_uri(...).auth_header(token))`.
- Legacy session header name is `mcp-session-id` (`rmcp::transport::common::http_header::HEADER_SESSION_ID`).

---

### Task 1: Extract `crates/mcp-core` (package `skardi-mcp-core`) and rewire the CLI

**Files:**
- Create: `crates/mcp-core/Cargo.toml`
- Create: `crates/mcp-core/src/lib.rs`
- Create: `crates/mcp-core/src/projection.rs` (moved from `crates/cli/src/mcp/projection.rs`)
- Modify: `Cargo.toml` (workspace members)
- Modify: `crates/cli/Cargo.toml` (add `skardi-mcp-core`; fix the falsified rmcp comment)
- Delete: `crates/cli/src/mcp/projection.rs`
- Modify: `crates/cli/src/mcp/mod.rs`, `crates/cli/src/mcp/bridge.rs` (imports)
- Modify: `crates/cli/src/client.rs` (drop `encode_component` + `URL_COMPONENT` + their test; re-export from the crate)

**Interfaces:**
- Produces: `skardi_mcp_core::projection::{project, builtin_tools, QUERY, LIST_DATA_SOURCES, RESERVED_NAMES}` (all `pub`), `skardi_mcp_core::encode_component` — signatures unchanged from today: `pub fn project(inventory: &Value) -> (Vec<Tool>, HashMap<String, String>)`, `pub fn builtin_tools() -> Vec<Tool>`, `pub fn encode_component(raw: &str) -> String`.

- [ ] **Step 1: Create the crate**

`crates/mcp-core/Cargo.toml`:

```toml
[package]
name = "skardi-mcp-core"
version.workspace = true
edition.workspace = true
description = "Shared MCP projection layer: pipeline inventory -> MCP tool definitions"
authors.workspace = true
repository.workspace = true
homepage.workspace = true
license.workspace = true

[dependencies]
# Model types only — no features. Preserves the intent behind the CLI's
# deliberate default-features = false pin (rmcp's transports stay out of
# this crate's own graph; each binding picks its transport itself).
rmcp = { version = "=3.1.4", default-features = false }
percent-encoding = "2.3"
serde_json = { workspace = true }

[lints]
workspace = true
```

`crates/mcp-core/src/lib.rs`:

```rust
//! The knowledge shared by every Skardi MCP binding: projecting the enriched
//! `GET /pipelines` inventory into MCP tool definitions, the built-in tool
//! names, and the pipeline-name -> URL path encoding of the tool->REST
//! translation contract. The dispatch matches themselves deliberately stay
//! per-binding (stdio bridge, server `/mcp`): same shape, different error
//! types and identity carriers.

pub mod projection;

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};

/// Characters percent-encoded by [`encode_component`]: everything except
/// ASCII alphanumerics and the RFC 3986 "unreserved" marks (`-`, `.`, `_`,
/// `~`). Deliberately conservative — over-encoding is always valid, while
/// missing a reserved character (`/`, `?`, `#`, `%`, space, …) mis-routes
/// the request.
const URL_COMPONENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Percent-encode one URL path segment or query value (user-supplied
/// pipeline/job names, run ids) so characters like `/`, `?`, `#`, `%`, and
/// spaces cannot alter the request route.
pub fn encode_component(raw: &str) -> String {
    utf8_percent_encode(raw, URL_COMPONENT).to_string()
}

#[cfg(test)]
mod tests {
    #[test]
    fn encode_component_escapes_reserved_and_keeps_unreserved() {
        use super::encode_component;
        assert_eq!(encode_component("simple-name_1.2~x"), "simple-name_1.2~x");
        assert_eq!(encode_component("a/b"), "a%2Fb");
        assert_eq!(encode_component("a b?c#d%e"), "a%20b%3Fc%23d%25e");
    }
}
```

(The `encode_component` test body must match what `crates/cli/src/client.rs` has today — copy it verbatim when deleting it there, don't retype from this plan.)

- [ ] **Step 2: Move projection.rs wholesale**

Copy `crates/cli/src/mcp/projection.rs` → `crates/mcp-core/src/projection.rs` with exactly two mechanical transformations, nothing else:
- every `pub(crate)` → `pub` (items: `QUERY`, `LIST_DATA_SOURCES`, `RESERVED_NAMES`, `builtin_tools`, `project`)
- module doc comment gains one line noting it is shared by the stdio bridge and the server's `/mcp` handler.

All unit tests move with the file unchanged. Delete the original.

- [ ] **Step 3: Workspace + CLI wiring**

`Cargo.toml` (root): add `"crates/mcp-core"` to `[workspace] members`.

`crates/cli/Cargo.toml`: add `skardi-mcp-core = { path = "../mcp-core" }`; replace the rmcp comment (falsified by feature unification once the server pulls rmcp's HTTP stack into workspace builds) with:

```toml
# The MCP-over-stdio bridge (`skardi mcp`). default-features = false keeps
# rmcp's optional stacks out of THIS crate's own dependency graph;
# `transport-io` is the server-side stdio transport. Note: with resolver = "2",
# a whole-workspace build unifies features across members, so skardi-server's
# rmcp HTTP features do get compiled into that build graph — the pin here
# governs what `cargo build -p skardi-cli` alone pulls in. rmcp has no runtime
# axum dependency (verified against the 3.1.4 source), so no axum skew either way.
```

`crates/cli/src/mcp/mod.rs`: drop `mod projection;`.

`crates/cli/src/mcp/bridge.rs`: replace `use crate::mcp::projection;` with `use skardi_mcp_core::projection;` and `use crate::client::{ApiClient, ApiError, encode_component};` stays (re-export below keeps it working).

`crates/cli/src/client.rs`: delete `URL_COMPONENT`, `encode_component`, the `percent_encoding` import, and the `encode_component_escapes_reserved_and_keeps_unreserved` test (moved in Step 1); add near the top:

```rust
// The pipeline-name -> URL path encoding is part of the tool->REST
// translation contract shared with the server's /mcp binding; the CLI
// re-exports it so command modules keep importing from crate::client.
pub use skardi_mcp_core::encode_component;
```

- [ ] **Step 4: `cargo fmt`**

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(mcp): extract skardi-mcp-core — shared projection + encode_component"
```

---

### Task 2: `validate_session_id` — the shared predicate, callable on a plain value

**Files:**
- Modify: `crates/server/src/session_header.rs`

**Interfaces:**
- Produces: `pub fn validate_session_id(s: &str) -> Result<(), String>` — same rules `session_id_from_headers` enforces today (visible ASCII, no comma, non-empty, ≤ `MAX_SESSION_ID_CHARS`). Task 4's handler validates inbound `Mcp-Session-Id` values against it (the third mirror, after the server's header path and the CLI's `crates/cli/src/session.rs`).

- [ ] **Step 1: Extract the predicate**

Refactor `session_id_from_headers` so the value checks live in a new function it calls (header-shape checks — duplicate header, non-ASCII encoding — stay where they are):

```rust
/// Validate one session-id VALUE against the rules above, independent of the
/// header plumbing. Public so the server's `/mcp` handler can vet a
/// caller-minted `Mcp-Session-Id` against the same predicate before
/// forwarding it as `x-skardi-session-id` (falling back to a minted UUID on
/// mismatch — losing that call's session grouping, not the call).
pub fn validate_session_id(s: &str) -> Result<(), String> {
    if !s.chars().all(|c| c.is_ascii_graphic() && c != ',') {
        return Err(format!(
            "{SESSION_ID_HEADER} must contain only visible ASCII characters, \
             with no spaces and no commas (proxies may merge repeated header \
             lines into one comma-separated value)"
        ));
    }
    if s.is_empty() {
        return Err(format!("{SESSION_ID_HEADER} must not be empty"));
    }
    if s.chars().count() > MAX_SESSION_ID_CHARS {
        return Err(format!(
            "{SESSION_ID_HEADER} must be at most {MAX_SESSION_ID_CHARS} characters"
        ));
    }
    Ok(())
}
```

`session_id_from_headers` keeps its exact behavior by delegating: after `to_str()`, call `validate_session_id(s)?` and return `Ok(Some(s.to_string()))`. (Note today's check order is charset → empty → length; keep that order so error messages don't change.)

- [ ] **Step 2: Unit test**

Add to the existing test module (or create one if the file has none — check first):

```rust
#[test]
fn validate_session_id_matches_the_header_rules() {
    assert!(validate_session_id("sess-1").is_ok());
    assert!(validate_session_id("").is_err());
    assert!(validate_session_id("has space").is_err());
    assert!(validate_session_id("a,b").is_err());
    assert!(validate_session_id(&"x".repeat(201)).is_err());
    assert!(validate_session_id(&"x".repeat(200)).is_ok());
}
```

- [ ] **Step 3: `cargo fmt`, commit**

```bash
git add crates/server/src/session_header.rs
git commit -m "refactor(server): extract validate_session_id from the header path"
```

---

### Task 3: Server dependencies + `--mcp-allowed-host` flag

**Files:**
- Modify: `crates/server/Cargo.toml`
- Modify: `crates/server/src/config.rs` (CliArgs)
- Modify: every `CliArgs { ... }` struct literal (grep `CliArgs {`): `crates/server/src/server.rs`, `crates/server/src/config.rs`, `crates/server/src/auth/routes.rs`, `crates/server/src/pipeline_handlers.rs`, `crates/server/tests/{pipelines_http,query_http,jobs_http,jobs_auth_http,jobs_audit_http,pipeline_audit_http,jobs_bridge_startup,graph_views,semantics_endpoint}.rs`

**Interfaces:**
- Produces: `CliArgs.mcp_allowed_hosts: Vec<String>` (empty by default), read by Task 5's service construction.

- [ ] **Step 1: Cargo changes**

In `crates/server/Cargo.toml` `[dependencies]`:

```toml
rmcp = { version = "=3.1.4", default-features = false, features = ["server", "transport-streamable-http-server"] }
skardi-mcp-core = { path = "../mcp-core" }
uuid = { workspace = true }
```

and change the tower entry to promote `oneshot` to production code explicitly rather than leaning on feature unification:

```toml
tower = { workspace = true, features = ["util"] }
```

- [ ] **Step 2: The flag**

Append to `CliArgs` in `crates/server/src/config.rs`:

```rust
/// Extra `Host` header values `/mcp` accepts, appended to the always-allowed
/// loopback trio (localhost / 127.0.0.1 / ::1). rmcp's loopback-only default
/// is a DNS-rebinding defense; a remote deployment declares its public
/// hostnames here instead of an allow-any escape hatch. An entry with a port
/// matches that port exactly; without one it matches any port. Repeatable.
#[arg(
    long = "mcp-allowed-host",
    help = "Allow this Host header value on /mcp, in addition to loopback \
            (repeatable; host or host:port)"
)]
pub mcp_allowed_hosts: Vec<String>,
```

- [ ] **Step 3: Fix every struct literal**

Add `mcp_allowed_hosts: vec![],` to every `CliArgs { ... }` literal found by `grep -rn "CliArgs {" crates`.

- [ ] **Step 4: Clap parse test**

In `crates/server/src/config.rs`'s existing CLI-parse test module (near the `CliArgs::try_parse_from` tests around line 2700):

```rust
#[test]
fn mcp_allowed_host_is_repeatable_and_defaults_empty() {
    let args = CliArgs::try_parse_from([
        "skardi-server",
        "--mcp-allowed-host",
        "api.example.com",
        "--mcp-allowed-host",
        "skardi.internal:8443",
    ])
    .unwrap();
    assert_eq!(
        args.mcp_allowed_hosts,
        vec!["api.example.com".to_string(), "skardi.internal:8443".to_string()]
    );
    let args = CliArgs::try_parse_from(["skardi-server"]).unwrap();
    assert!(args.mcp_allowed_hosts.is_empty());
}
```

- [ ] **Step 5: `cargo fmt`, commit**

```bash
git add -A
git commit -m "feat(server): rmcp + mcp-core deps, --mcp-allowed-host flag"
```

---

### Task 4: The MCP handler (`crates/server/src/mcp/handler.rs`)

**Files:**
- Create: `crates/server/src/mcp/handler.rs`
- Create: `crates/server/src/mcp/mod.rs` (minimal: `pub(crate) mod handler;` — grows in Task 5)
- Modify: `crates/server/src/lib.rs` (`pub(crate) mod mcp;`)

**Interfaces:**
- Consumes: `skardi_mcp_core::{projection, encode_component}` (Task 1), `crate::session_header::validate_session_id` (Task 2).
- Produces: `pub(crate) struct McpHandler` with `pub(crate) fn new(rest: Router) -> Self`, `pub(crate) async fn do_list_tools(&self, headers: &HeaderMap) -> Result<ListToolsResult, ErrorData>`, `pub(crate) async fn do_call_tool(&self, name: &str, args: Option<JsonObject>, headers: &HeaderMap) -> Result<CallToolResult, ErrorData>`; implements `rmcp::ServerHandler`. Task 5 wraps it in the service factory.

- [ ] **Step 1: Write the handler**

```rust
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

use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, Method, Request, StatusCode, header};
use rmcp::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, ErrorData,
    Implementation, JsonObject, ListToolsResult, PaginatedRequestParams, ServerCapabilities,
    ServerInfo,
};
use rmcp::service::{RequestContext, RoleServer};
use serde_json::Value;
use skardi_mcp_core::{encode_component, projection};
use tower::ServiceExt;
use uuid::Uuid;

use crate::session_header::{SESSION_ID_HEADER, validate_session_id};

/// Same wording as the stdio bridge (`crates/cli/src/mcp/bridge.rs`) — the
/// two bindings present one product to hosts. Kept per-binding (not in
/// skardi-mcp-core) so either side can diverge deliberately.
const INSTRUCTIONS: &str = "Skardi is a federated SQL data plane: operator-defined \
pipelines plus an ad-hoc SQL engine over the configured data sources. Prefer the \
pipeline tools for tasks they cover. Before writing ad-hoc SQL with `query`, call \
`list_data_sources` to see tables, schemas, and their plain-English descriptions.";

/// Ceiling on a synthetic response body — the same 256 MB the stdio bridge
/// inherits from the CLI client (`crates/cli/src/client.rs`), kept as a
/// constant so one pipeline behaves identically through both bindings.
/// In-process this bounds only the MCP layer's own copy of the result; the
/// REST handler has already materialized the full result before the
/// collector sees a byte (execution-layer resource governance is a recorded
/// server-wide non-goal).
const MAX_RESPONSE_BYTES: usize = 256 * 1024 * 1024;

/// The legacy-protocol session header rmcp echoes on every request of a
/// managed session (`legacy_session_mode`). Stateless-protocol requests
/// don't carry it.
const MCP_SESSION_ID: &str = "mcp-session-id";

#[derive(Clone)]
pub(crate) struct McpHandler {
    /// Pre-middleware capture of the REST router (routing table + handlers,
    /// where every execution concern lives: `require_session`, validation,
    /// audit). Arc-cheap to clone; the service factory clones it per request.
    rest: Router,
}

impl McpHandler {
    pub(crate) fn new(rest: Router) -> Self {
        McpHandler { rest }
    }

    /// The audit id for this request, layered: a valid legacy
    /// `Mcp-Session-Id` groups the session's calls together; anything else
    /// (stateless protocol, malformed value) gets a per-request UUID —
    /// attribution stays intact, grouping granularity is honestly
    /// per-request.
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
    /// the caller sent none) so handler-level enforcement and ledger
    /// recording are exactly the REST semantics; sets
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

    /// Fetch + project the live inventory. Used by `tools/list` and by every
    /// pipeline `tools/call` (per-call name resolution: one extra in-process
    /// dispatch buys correctness in both protocol modes and freshness by
    /// construction — a stale-map "unknown tool" cannot exist).
    async fn project_inventory(
        &self,
        headers: &HeaderMap,
    ) -> Result<(Vec<rmcp::model::Tool>, std::collections::HashMap<String, String>), ErrorData>
    {
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
```

(Adjust names against the real rmcp 3.1.4 API while implementing — e.g. `CallToolRequestParams`/`CallToolResponse` are what the bridge imports today, keep identical imports.)

- [ ] **Step 2: Unit tests in the same file** (`#[cfg(test)] mod tests`)

Test doubles: a **probe router** — a tiny axum `Router` standing in for the REST router, recording what the synthetic request carried. Pattern:

```rust
use axum::routing::{get, post};
use axum::extract::Request as AxumRequest;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
struct Seen(Arc<Mutex<Vec<(String, Option<String>, Option<String>, String)>>>);
// (path, authorization, x-skardi-session-id, body)

fn probe_router(seen: Seen, inventory: Value) -> Router {
    let record = |seen: Seen| {
        move |req: AxumRequest| {
            let seen = seen.clone();
            async move {
                let (parts, body) = req.into_parts();
                let hdr = |name: &str| {
                    parts.headers.get(name).and_then(|v| v.to_str().ok()).map(String::from)
                };
                let auth = hdr("authorization");
                let sid = hdr("x-skardi-session-id");
                let bytes = axum::body::to_bytes(body, 1 << 20).await.unwrap();
                seen.0.lock().unwrap().push((
                    parts.uri.path().to_string(),
                    auth,
                    sid,
                    String::from_utf8_lossy(&bytes).into_owned(),
                ));
                axum::Json(serde_json::json!({"success": true}))
            }
        }
    };
    Router::new()
        .route("/pipelines", get(move || {
            let inventory = inventory.clone();
            async move { axum::Json(inventory) }
        }))
        .route("/query", post(record(seen.clone())))
        .route("/data_source", get(record(seen.clone())))
        .route("/:name/execute", post(record(seen)))
}
```

Tests (each one bite-sized; write, then move on — CI verifies):

1. `list_tools_projects_pipelines_and_builtins` — probe router with a one-pipeline inventory (same JSON shape as the bridge test's `inventory()`); `do_list_tools(&HeaderMap::new())` contains the pipeline tool + `query` + `list_data_sources`.
2. `pipeline_call_resolves_per_call_and_posts_flat_body` — **no prior `do_list_tools`**: `do_call_tool("product-search", {"brand":"acme"}, empty headers)` succeeds (per-call resolution is the point), probe saw `/product-search/execute` with body `{"brand":"acme"}`.
3. `authorization_is_forwarded_verbatim_and_absent_when_missing` — headers with `authorization: Bearer tok-1` → probe records it on the synthetic request; empty headers → probe records `None`.
4. `legacy_mcp_session_id_is_echoed_as_the_audit_id` — headers with `mcp-session-id: legacy-42` → pipeline call's `x-skardi-session-id` is `legacy-42`; query call with `purpose` gets `ai_context.session_id == "legacy-42"`.
5. `malformed_mcp_session_id_falls_back_to_a_minted_uuid` — `mcp-session-id: "has space"` (build via `HeaderValue::from_static`) → forwarded id parses as a UUID (`Uuid::parse_str` ok), the call succeeds (400 must not propagate).
6. `stateless_request_mints_a_uuid_per_request` — no `mcp-session-id`: two pipeline calls record two DIFFERENT valid UUIDs.
7. `unknown_tool_is_invalid_params_nudging_a_relist` — same assertion text as the bridge test.
8. `non_2xx_becomes_is_error_with_the_body_text` — probe route answering 400 with a JSON error body → `is_error == Some(true)`, content contains the body text.
9. `query_without_purpose_omits_ai_context` — probe body has no `ai_context` key.
10. **The negative-auth tripwire** — `do_call_tool_hits_handler_level_auth_without_a_bearer`: build a REAL auth-enabled AppState (reuse the in-crate pattern from `crates/server/src/auth/routes.rs` tests: `make_better_auth_state` + `auth::test_env::lock`-style env guard), `configure_routes`-style REST router via `crate::server::rest_router(state)` (Task 5) or a locally-built router with the real `/query` handler; `do_call_tool("query", {"sql": "select 1"}, empty headers)` → `is_error == Some(true)` and content contains `"unauthorized"` / 401 wording. Comment: *if auth ever migrated into `configure_middleware`, this call would start succeeding — the tripwire for the middleware-boundary constraint.* (Depends on Task 5's `rest_router`; if writing tests before Task 5, gate on the locally-built real-handler router instead. Prefer landing this test in the same commit as Task 5.)

- [ ] **Step 3: `cargo fmt`, commit**

```bash
git add -A
git commit -m "feat(server): MCP handler — stateless protocol adapter over the REST router"
```

---

### Task 5: Session gate + service construction + router wiring

**Files:**
- Create: `crates/server/src/mcp/gate.rs`
- Modify: `crates/server/src/mcp/mod.rs`
- Modify: `crates/server/src/server.rs` (`configure_routes` split + nest)

**Interfaces:**
- Consumes: `McpHandler` (Task 4), `CliArgs.mcp_allowed_hosts` (Task 3), `crate::auth::routes::verify_session`.
- Produces: `pub(crate) fn attach(rest: Router, state: AppState) -> Router` (nests the gated MCP service at `/mcp`); `pub(crate) fn rest_router(state: AppState) -> Router` in server.rs (today's `configure_routes` body). `configure_routes` public signature unchanged: `pub fn configure_routes(state: AppState) -> Router`.

- [ ] **Step 1: The session gate** (`crates/server/src/mcp/gate.rs`)

```rust
//! `/mcp`-specific transport middleware: every inbound request authenticates
//! via `verify_session` BEFORE rmcp creates any session state — an anonymous
//! `initialize` is a transport-level 401, not a retained session, and missing
//! credentials surface as the 401 host credential flows key on (not as tool
//! errors inside HTTP 200s). Never runs on synthetic dispatches.
//!
//! Bearer only: `verify_session`'s cookie fallback would admit callers whose
//! every tool call then fails handler-level auth (synthetic requests forward
//! only `Authorization`), so the gate strips `cookie` before the check —
//! token validation stays single-home, the accepted carrier narrows to the
//! one MCP hosts actually send. This is also what keeps the open
//! `allowed_origins` posture honest: a browser's ambient cookie cannot
//! authenticate `/mcp`, so there is no CSRF-shaped surface for Origin to
//! guard. On a no-auth deployment `verify_session` always allows and the
//! gate is a no-op.

use std::convert::Infallible;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::http::{Request, header};
use axum::response::{IntoResponse, Response};
use futures::future::BoxFuture;

use crate::server::AppState;

#[derive(Clone)]
pub(crate) struct SessionGate<S> {
    inner: S,
    state: AppState,
}

impl<S> SessionGate<S> {
    pub(crate) fn new(inner: S, state: AppState) -> Self {
        SessionGate { inner, state }
    }
}

impl<S, B> tower::Service<Request<Body>> for SessionGate<S>
where
    S: tower::Service<Request<Body>, Response = axum::http::Response<B>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
    B: http_body::Body<Data = bytes::Bytes> + Send + 'static,
    B::Error: Into<axum::BoxError>,
{
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        // Standard tower pattern: take the ready inner, leave a fresh clone.
        let inner = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, inner);
        let state = self.state.clone();
        Box::pin(async move {
            let mut headers = req.headers().clone();
            headers.remove(header::COOKIE);
            if let Err(unauthorized) = crate::auth::routes::verify_session(&state, &headers).await
            {
                return Ok(unauthorized.into_response());
            }
            Ok(inner.call(req).await.expect("Infallible").into_response())
        })
    }
}
```

(Dependency note: `http_body` / `bytes` are already in the server's graph transitively via axum; if the trait bounds need them as direct deps, prefer bounding on `S::Response: IntoResponse` instead — implementer's choice, whichever compiles cleanly with rmcp's `BoxResponse`.)

- [ ] **Step 2: Service construction + attach** (`crates/server/src/mcp/mod.rs`)

```rust
//! MCP over streamable HTTP at `/mcp` — a protocol adapter, not a second
//! execution path. See docs/superpowers/specs/2026-08-28-mcp-http-transport-design.md.

mod gate;
pub(crate) mod handler;

use std::sync::Arc;

use axum::Router;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::tower::{
    StreamableHttpServerConfig, StreamableHttpService,
};

use crate::server::AppState;
use gate::SessionGate;
use handler::McpHandler;

/// Nest the gated MCP service at `/mcp`. `rest` is deliberately the
/// pre-middleware router (transport middleware — CORS today — applies
/// exactly once, to the inbound `/mcp` request; capturing post-middleware
/// would run it a second time on every synthetic dispatch). Dispatch only
/// ever targets REST paths, so no recursion into `/mcp` is possible.
///
/// Known, accepted shadow: the nest hides REST's `/:name/execute` for a
/// pipeline literally named `mcp` from every URL-borne caller; only `/mcp`
/// itself still reaches it via this pre-nest capture.
pub(crate) fn attach(rest: Router, state: AppState) -> Router {
    let allowed_hosts = allowed_hosts(&state);
    let handler_rest = rest.clone();
    let service = StreamableHttpService::new(
        move || Ok(McpHandler::new(handler_rest.clone())),
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig {
            allowed_hosts,
            // Everything else stays at rmcp's defaults, each one a spec
            // decision: sse_keep_alive 15s (bytes keep flowing during long
            // tool calls, resetting reverse-proxy idle timers well under
            // nginx's 60s default), legacy_session_mode true (legacy clients
            // keep the session behavior they expect; the gate guards the
            // state they create), json_response false (SSE responses so the
            // keep-alive applies), allowed_origins empty (MCP hosts are not
            // browsers; the Bearer-only gate is the actual barrier).
            ..Default::default()
        },
    );
    rest.nest_service("/mcp", SessionGate::new(service, state))
}

/// Additive host allowlist: the loopback trio is always allowed (rmcp's
/// DNS-rebinding default), declared `--mcp-allowed-host` values are
/// appended. Deliberately no allow-any escape hatch.
fn allowed_hosts(state: &AppState) -> Vec<String> {
    let mut hosts = vec!["localhost".to_string(), "127.0.0.1".to_string(), "::1".to_string()];
    hosts.extend(
        state
            .config
            .read()
            .expect("config lock")
            .args
            .mcp_allowed_hosts
            .iter()
            .cloned(),
    );
    hosts
}
```

(Import paths for `StreamableHttpService` etc.: check rmcp 3.1.4's re-exports — `rmcp::transport::StreamableHttpService` may be the public path; use whatever the crate exports.)

- [ ] **Step 3: Split `configure_routes`** in `crates/server/src/server.rs`

Rename today's body to `pub(crate) fn rest_router(state: AppState) -> Router` (identical content, still ends `.with_state(state)` — keep the `state.auth_layer.is_enabled()` branch working by cloning what it needs), and:

```rust
/// Configure all application routes: the REST router exactly as before,
/// plus the MCP service nested at /mcp. The MCP handler captures the
/// PRE-middleware REST router — `configure_middleware` wraps the final
/// router (transport middleware, applied once), while synthetic dispatches
/// traverse routing + handlers only, where every execution concern
/// (require_session, validation, audit) lives. If auth ever moved into
/// configure_middleware, synthetic dispatch would bypass it — see the
/// tripwire test on the MCP handler.
pub fn configure_routes(state: AppState) -> Router {
    let rest = rest_router(state.clone());
    crate::mcp::attach(rest, state)
}
```

- [ ] **Step 4: In-crate wiring tests** (server.rs or mcp/mod.rs test module; reuse the existing test-state builders)

1. `session_gate_returns_401_before_rmcp_sees_an_anonymous_initialize` — auth-enabled AppState; `configure_routes(state)` oneshot `POST /mcp` (`Host: 127.0.0.1`, `content-type: application/json`, `accept: application/json, text/event-stream`, body = a JSON-RPC `initialize` request) with no Authorization → 401. Same with only a valid session **cookie** (create user+session as `verify_session_cookie_fallback` does, send `cookie: {name}={token}`) → 401 (Bearer-only carrier pinned). Same with `authorization: Bearer {token}` → NOT 401.
2. `session_gate_is_a_noop_without_auth` — no-auth AppState: anonymous initialize on `/mcp` is not 401.
3. `mcp_host_allowlist_is_loopback_plus_declared` — no-auth AppState with `mcp_allowed_hosts: vec!["api.example.com".into()]`; oneshot initialize with `Host: evil.example` → 403; `Host: api.example.com` → not 403; `Host: 127.0.0.1` → not 403. And a default state (`vec![]`): `Host: api.example.com` → 403.
4. `middleware_boundary_is_pinned_two_sided` — (a) full app = `configure_middleware(configure_routes(state))`, oneshot initialize on `/mcp` with an `Origin` header: exactly ONE `access-control-allow-origin` value in the response; (b) `rest_router(state)` oneshot `GET /pipelines` with the same `Origin`: NO `access-control-allow-origin` header (the capture is pre-middleware).
5. The Task 4 negative-auth tripwire test lands here if it needed `rest_router`.

A JSON-RPC initialize body for these raw-HTTP tests:

```rust
fn initialize_body() -> String {
    serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "0"}
        }
    })
    .to_string()
}
```

- [ ] **Step 5: `cargo fmt`, commit**

```bash
git add -A
git commit -m "feat(server): /mcp — session gate, host allowlist, router nesting"
```

---

### Task 6: End-to-end integration tests (`crates/server/tests/mcp_http.rs`)

**Files:**
- Modify: `crates/server/Cargo.toml` (`[dev-dependencies]`)
- Create: `crates/server/tests/mcp_http.rs`

**Interfaces:**
- Consumes: `configure_routes`, `configure_middleware`, `AppState` (all pub), rmcp client transport.

- [ ] **Step 1: dev-deps**

```toml
[dev-dependencies]
rmcp = { version = "=3.1.4", default-features = false, features = ["client", "transport-streamable-http-client-reqwest"] }
```

(plus whatever the reqwest transport needs to build plain-HTTP requests — if the feature alone doesn't compile against localhost HTTP, add `reqwest = { version = "0.12", default-features = false }` as a dev-dep; decide at implementation.)

- [ ] **Step 2: Harness**

Test-local `make_app_state` modeled on `crates/server/tests/pipelines_http.rs` (in-memory batch `products`, one real `StandardPipeline` from YAML with a `{brand}`-style parameter, optional query-audit SQLite in a TempDir, optional better-auth layer modeled on `jobs_auth_http.rs` including the `ENV_LOCK` pattern), plus:

```rust
async fn serve(state: AppState) -> std::net::SocketAddr {
    let app = skardi_server::server::configure_middleware(
        skardi_server::server::configure_routes(state),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

async fn connect(addr: std::net::SocketAddr) -> rmcp::service::RunningService<rmcp::RoleClient, ()> {
    let transport = rmcp::transport::StreamableHttpClientTransport::from_uri(
        format!("http://{addr}/mcp"),
    );
    rmcp::ServiceExt::serve((), transport).await.unwrap()
}
```

(Exact client-side types per rmcp 3.1.4 — the CLI's `tests/mcp_e2e.rs` already drives an rmcp client over a child process; mirror its idioms for `list_tools` / `call_tool` and adapt the transport.)

- [ ] **Step 3: The tests**

1. `tools_list_contains_seeded_pipelines_and_builtins` — via rmcp client.
2. `pipeline_call_executes_and_returns_rows` — `call_tool` on the seeded pipeline with its parameter; result `is_error != Some(true)`, content text parses as JSON with `success: true` and row data.
3. `query_round_trips` — `call_tool("query", {"sql": "SELECT count(*) AS n FROM products"})`.
4. `unknown_tool_is_invalid_params` — `call_tool("nope", ...)` errors at the protocol level with the re-list hint.
5. `failing_sql_surfaces_as_is_error_tool_result` — bad SQL → `is_error == Some(true)`.
6. `auth_forwarding_reaches_the_handlers` — auth-enabled state; client transport built with `.auth_header(token)`(via `StreamableHttpClientTransportConfig`); a `query` call succeeds — proving `Authorization` survives synthetic dispatch (a dropped header would 401 at handler-level `require_session` despite the gate having passed).
7. `anonymous_and_cookie_only_initialize_are_transport_401s` — raw `reqwest` (or oneshot in Task 5 covers this; keep here only the end-to-end anonymous case: rmcp client `.serve()` against auth-enabled server fails; and a follow-up raw POST claiming an invented `Mcp-Session-Id` also gets 401).
8. `stateless_protocol_regression` — drive `/mcp` as a stateless `2026-07-28` client (rmcp client with `allow_stateless: true` config / no session; consult rmcp's own stateless client tests for the exact knob): `tools/list` then `tools/call` as independent requests must still resolve and execute — the direct regression test for per-call resolution.
9. `pipeline_call_records_an_audit_id` — state with query-audit SQLite in a TempDir; pipeline call via `/mcp`; open the ledger (`skardi_server::query_audit::QueryAuditStore`) and assert the recorded `session_id` is a valid UUID (stateless path). The legacy `Mcp-Session-Id` echo is already pinned at the `do_*` level (Task 4 test 4).

- [ ] **Step 4: `cargo fmt`, commit**

```bash
git add -A
git commit -m "test(server): /mcp end-to-end — rmcp client, auth, allowlist, stateless"
```

---

### Task 7: Documentation

**Files:**
- Modify: `docs/mcp.md`
- Modify: `docs/pipelines.md`

- [ ] **Step 1: `docs/mcp.md`**

Per the spec's Documentation section, this is a rewrite of existing prose plus a new section:

- Title: `# MCP Binding — \`skardi mcp\`` → `# MCP Binding` (page covers both bindings now). Opening paragraphs state the local-vs-remote split: stdio bridge for hosts that spawn a local binary; `/mcp` for hosts that cannot (claude.ai, mobile, hosted platforms). The stdio bridge is not retired.
- Architecture diagram gains the second path (HTTP: host → `/mcp` on skardi-server directly).
- New section `## Remote (streamable HTTP)` after Host setup:
  - The endpoint: `http://<server>/mcp`, default-on, no config flag; URL-based MCP host configuration example.
  - Bearer guidance: send `Authorization: Bearer <token>`; with auth enabled every `/mcp` request — `initialize` and `tools/list` included — requires the token (deliberate divergence from REST's tokenless `GET /pipelines`, stated). Cookies are never accepted on `/mcp`.
  - Host allowlist: loopback allowed by default; remote deployments declare public hostnames via repeatable `--mcp-allowed-host api.example.com` / `host:port` (port entry matches exactly; portless matches any port). No allow-any option.
  - Reverse proxies: either forward the public `Host` and declare it, or rewrite `Host` to the upstream loopback authority; a mismatch presents as `403 Forbidden: Host header is not allowed`. Proxy read/idle timeouts: long tool calls hold their POST open for the whole run; responses are SSE with a 15 s keep-alive so bytes keep flowing, but deployments running long pipelines should still check proxy read timeouts (nginx `proxy_read_timeout` defaults to 60 s and only helps if it counts any bytes as liveness).
  - Audit grouping: legacy-protocol sessions group by `Mcp-Session-Id`; stateless-protocol (2026-07-28+) requests are attributed per-request with a minted UUID — the protocol removed the conversation-level handle.
- `## Auth notes` gains the per-binding split: bridge REST calls keep today's tokenless `GET /pipelines` note; `/mcp` requires the token for everything once auth is on.
- `## Timeouts & lifecycle` gains the HTTP lifecycle: per-request, no process to exit; disconnect semantics per the SSE response mode; the host's tool-call timeout remains the backstop and the keep-alive is what keeps proxies from firing first.

- [ ] **Step 2: `docs/pipelines.md`**

Find the surface-list MCP bullet reading "via `skardi mcp`" and broaden it to name both bindings (the stdio bridge and the server's `/mcp` endpoint).

- [ ] **Step 3: `cargo fmt` (no-op for docs), commit**

```bash
git add docs/mcp.md docs/pipelines.md
git commit -m "docs(mcp): /mcp remote binding — manual covers both bindings"
```

---

### Task 8: Plan/spec housekeeping, push, draft PR

- [ ] **Step 1:** Commit this plan file (`docs/superpowers/plans/2026-09-01-mcp-http-transport.md`).
- [ ] **Step 2:** `cargo fmt` (final check), verify branch is `mcp-http-transport`, push:

```bash
git push -u origin mcp-http-transport
```

- [ ] **Step 3:** Draft PR against `main`:

```bash
gh pr create --draft --title "feat(server): MCP streamable HTTP transport — /mcp" --body "..."
```

Body: implements the spec (`docs/superpowers/specs/2026-08-28-mcp-http-transport-design.md`, PR #234); summary of the four pieces (mcp-core extraction, handler, gate+wiring, tests+docs); note that R2's "routine CI build is the only remaining confirmation" is confirmed by this PR's CI. End with the standard generation footer.

- [ ] **Step 4:** Watch CI; fix failures on the branch. (rmcp client API details in Task 6 are the most likely to need iteration — that's expected, tests are CI-verified by policy.)

---

## Self-Review Notes

- **Spec coverage**: shared crate (T1), session gate Bearer-only (T5), host allowlist (T3+T5), stateless handler + per-call resolution (T4), audit layering + validator mirror (T2+T4), SSE keep-alive (T5, rmcp default 15 s), response cap (T4), error mapping (T4), all named tests (T4–T6), docs (T7), CLI Cargo comment (T1), tower util promotion (T3). Non-goals untouched.
- **Types**: `McpHandler::new(Router)`; `attach(Router, AppState) -> Router`; `rest_router(AppState) -> Router`; `validate_session_id(&str) -> Result<(), String>` — consistent across tasks.
- **Known implementation-time checks** (not placeholders — verify against the crate while coding): exact rmcp public re-export paths, client-side stateless knob, `SessionGate` trait-bound form.
