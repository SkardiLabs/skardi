# MCP Streamable HTTP Transport — `/mcp` in skardi-server

This is the follow-up milestone named in the stdio binding spec's non-goals
(`docs/superpowers/specs/2026-08-13-mcp-stdio-binding-design.md`, shipped as
PR #227): skardi-server itself speaks MCP over streamable HTTP at `/mcp`, so
hosts that cannot spawn a local binary — claude.ai, mobile clients, hosted
agent platforms — can connect to a Skardi deployment directly.

Landing this does not retire the stdio bridge. `skardi mcp` remains the
zero-friction path for local hosts (stdio is MCP's lowest common
denominator, and the bridge inherits the CLI's whole auth/config
resolution), and `--server` already makes it a local gateway to a remote
deployment. After this milestone the CLI is the *local* entry point and
`/mcp` is the *remote* one.

```
stdio (v1):   host ──spawn──► skardi (CLI) ──HTTP REST──► skardi-server ──in-proc──► engine
HTTP (this):  host ───────────HTTP /mcp──────────────────► skardi-server ──in-proc──► engine
```

---

## Architecture: a protocol adapter over the server's own router

The MCP layer added to skardi-server is a **protocol adapter, not a second
execution path**. The handler holds a clone of the server's own REST
`Router`; every tool call is translated into a synthetic HTTP request and
dispatched in-process via `tower::ServiceExt::oneshot` — the same mechanism
the server's HTTP tests already use. No network hop, no socket, but the
request runs the routing table and the unmodified REST handlers (see
"Transport vs. execution middleware" below for exactly which layers a
synthetic request does and does not traverse).

Why self-dispatch instead of calling handler internals directly:

- **Auth parity with no new auth logic.** Two layers, both reusing the
  existing `verify_session`. A thin tower gate in front of the MCP
  service authenticates every inbound `/mcp` request, so anonymous
  callers get a transport-level 401 before rmcp creates any session
  state (see "Session gate" below). The synthetic request then carries
  the caller's `Authorization` header verbatim, so handler-level
  enforcement, authorization errors, and ledger recording are *exactly*
  the REST semantics (`require_session` is called inside each protected
  handler, not installed as middleware; this is a load-bearing fact, see
  the middleware boundary below). On the data surface `/mcp` adds no
  privilege that `POST /<name>/execute` and `POST /query` do not already
  expose, and `verify_session` always allows on a no-auth deployment —
  which is why `/mcp` ships default-on with no config flag.
- **Zero refactor of the execution path.** The alternative — splitting
  every axum handler into a callable core plus an axum wrapper — is an
  invasive refactor of `pipeline_handlers.rs` / `query_handlers.rs` for no
  behavioral gain.
- **One inventory contract.** `tools/list` issues a synthetic
  `GET /pipelines` and feeds the enriched JSON to the same projection the
  stdio bridge uses. The enriched inventory response stays the single seam
  between the server and every MCP binding; the in-process handler cannot
  drift from what a remote bridge would see.

The cost is accepted: a small amount of synthetic-request construction
boilerplate, and the mild oddness of a server dispatching requests to
itself (mitigated by the fact that the test suite already establishes the
pattern).

### Transport vs. execution middleware

`create_server` assembles in two stages: `configure_routes` builds the
bare routing table, and `configure_middleware` wraps the *final* router
with edge middleware — today CORS, tomorrow perhaps tracing, timeouts, or
rate limits. The captured `rest_router` is deliberately the
pre-middleware one, which draws the boundary:

- **Transport middleware** (`configure_middleware`) applies exactly once,
  to the inbound `/mcp` request itself — the MCP service is nested in the
  final router, inside that wrapping. Capturing the post-middleware
  router instead would run CORS/tracing/rate limiting a second time on
  every synthetic dispatch: double-counted rate limits, duplicated trace
  spans.
- **The session gate is `/mcp`-specific transport middleware**: it wraps
  only the nested MCP service, authenticating requests before they reach
  rmcp. It governs who may hold an MCP session — a transport-layer
  question — and never runs on synthetic dispatches.
- **Execution enforcement** is what synthetic dispatch traverses: routing
  plus the handler layer, which is where every execution concern lives —
  `require_session` auth checks, parameter validation, and audit
  recording are all called inside the handlers, not installed as
  middleware.

This makes a constraint explicit that the design depends on: execution
enforcement — auth above all — stays handler-level. If auth ever moved
into `configure_middleware`, synthetic dispatch would bypass it; the
session gate would still authenticate `/mcp` callers, but the
handler-parity argument of this design would need revisiting.

### Router wiring

`configure_routes` builds in two phases: the REST router is built exactly
as today; the MCP service is then constructed capturing `rest_router.clone()`
and the final router is the REST router with the MCP service nested at
`/mcp`. Dispatch only ever targets REST paths, so no recursion into `/mcp`
is possible. rmcp's streamable-HTTP server service is a tower `Service`, so
`nest_service` mounts it regardless of rmcp's internal axum version.

---

## Shared code: `crates/mcp-core` (package `skardi-mcp-core`)

Only the genuinely drift-prone knowledge is extracted; the v1 spec deferred
this decision to now, and now there are two consumers.

Moves from `crates/cli/src/mcp/` into the new crate:

- `projection.rs` wholesale: `project()`, `pipeline_tool` schema
  generation, `builtin_tools()`, `sanitize`/`assign_tool_names`, the
  `QUERY` / `LIST_DATA_SOURCES` / `RESERVED_NAMES` constants, the
  version-skew fallbacks (missing `parameters` → open schema, missing
  `json_schema` → `{}`), and all its unit tests.
- `encode_component` (currently in `crates/cli/src/client.rs`): the
  pipeline-name → URL path encoding is part of the tool→REST translation
  contract and both dispatchers need it. The CLI re-imports it from the
  crate.

Dependencies of the new crate: `rmcp = "=3.1.4"` (model types only),
`serde_json`. Both `skardi-cli` and `skardi-server` depend on it.

**Deliberately NOT shared:** the dispatch `match` (three arms: `query`,
`list_data_sources`, pipeline fallthrough) stays duplicated between the
stdio bridge and the server handler. The two sides map errors from
different types (`ApiError` vs. an in-process `Response`), inject the
session id through different carriers, and evolve independently — same
shape, different knowledge. The built-in-name constants and the
`builtin_tool_names_match_the_reserved_set` invariant test are what keep
the two matches honest.

---

## Server-side components

New module `crates/server/src/mcp/`:

- `handler.rs` — the `ServerHandler` implementation. Follows the bridge's
  established pattern for testability: trait methods are thin wrappers
  over inherent `do_*` methods (taking the inbound headers as a plain
  parameter) so tests can drive the logic without constructing
  `RequestContext`.
- `mod.rs` — service construction and the `configure_routes` wiring.

### Stateless by protocol — the handler holds no per-session state

MCP protocol version `2026-07-28` removes sessions entirely (SEP-2567);
rmcp serves requests negotiating that version statelessly regardless of
configuration, invoking the service factory **once per request** — a
handler instance does not live past the request that created it. New
remote hosts are exactly the clients that negotiate the newest version,
so the design treats stateless as the ground truth:

- The handler owns only shared immutable dependencies — the `Arc`-cheap
  clone of the REST `Router` — which the factory closure clones per
  request. There is no `tool_map` and no per-instance mutable state.
- Pipeline tool names are resolved **per call**: a `tools/call` that is
  not a built-in issues a synthetic `GET /pipelines`, projects, and looks
  the name up. One extra in-process dispatch per pipeline call buys
  correctness in both protocol modes, freshness by construction, and no
  shared mutable state (a stale-map "unknown tool" cannot exist).
- Per-request data (the `Authorization` header, an `Mcp-Session-Id` if
  the client is a legacy-protocol one) is read from the
  `http::request::Parts` that rmcp injects into every request's
  extensions — verified in the 3.1.4 source, so this is a fact, not a
  probe.
- `legacy_session_mode` stays at rmcp's default (`true`): legacy-protocol
  clients keep the session behavior they expect; the session gate guards
  the session state they create.

### Session gate — authentication before session creation

Handler-level enforcement alone is not enough on this transport: for
legacy-protocol clients rmcp creates and retains session state for an
`initialize` before any tool call happens, so with auth enabled an
anonymous caller could mass-create sessions; and on every protocol
version, missing credentials would otherwise surface only as tool errors
inside HTTP 200 responses instead of the transport-level 401 that host
credential flows key on. A thin tower layer wrapping only the nested
MCP service closes this: every inbound `/mcp` request passes
`verify_session(&state, &headers)` — the same single-home check the REST
handlers call — before reaching rmcp; failures return a transport-level
401, which is also the signal host credential flows key on. On a no-auth
deployment `verify_session` always allows and the gate is a no-op.

One deliberate parity divergence, worth naming: with auth enabled, the
MCP inventory (`tools/list`) requires a token even though REST's
`GET /pipelines` is readable without one. An MCP session is stateful
server-side and is gated as a whole; deployments already weigh
`/pipelines` exposure per the v1 auth notes. Handler-level
`require_session` (reached via synthetic dispatch) stays as the second
layer of defense.

### Public host allowlist — the DNS-rebinding guard, configured

rmcp's streamable-HTTP service validates the `Host` header of every
request against an allowlist defaulting to `localhost` / `127.0.0.1` /
`::1` (verified in the 3.1.4 source; non-matches are 403 before any
handler runs). That default is a DNS-rebinding defense and is kept — a
developer running skardi-server on loopback is exactly who rebinding
attacks target — but unconfigured it would 403 every genuinely remote
caller, defeating this milestone.

skardi-server therefore gains a repeatable flag:

```
skardi-server --mcp-allowed-host api.example.com --mcp-allowed-host skardi.internal:8443 ...
```

Semantics are additive: the loopback trio is always allowed, declared
hosts are appended (mapped to rmcp's `with_allowed_hosts`; an entry with
a port matches that port exactly, without one it matches any port).
There is deliberately no allow-any-hosts escape hatch — a public
deployment names its hostnames, and an escape hatch would become the
default in the wild, stripping the loopback protection the default
exists for.

`allowed_origins` stays at rmcp's default (no restriction): MCP hosts
are not browsers, the session gate is the actual barrier, and an open
Origin posture is consistent with the existing CORS
`mirror_request` configuration.

### Tool call translation

Identical tool surface to the stdio bridge — same projection, same three
built-ins, same pipeline tools:

| Tool call | Synthetic request |
|---|---|
| `tools/list` | `GET /pipelines` → `projection::project` |
| `query` | `POST /query`, body `{sql, max_rows?, ai_context?: {purpose, session_id}}` (`ai_context` only when `purpose` is provided, as in the bridge) |
| `list_data_sources` | `GET /data_source` |
| pipeline tool | `POST /{encoded-name}/execute`, arguments as the flat body, header `x-skardi-session-id` |

Every synthetic request forwards the inbound `Authorization` header
verbatim (omitted when the caller sent none) and sets
`content-type: application/json` on POSTs.

### Request identity and audit correlation

The carriers are unchanged from the bridge — `ai_context.session_id` when
`query` carries a `purpose`, `x-skardi-session-id` on pipeline executes —
but the id itself is layered, because the new protocol has no session to
name:

- A request carrying `Mcp-Session-Id` (legacy-protocol clients under
  `legacy_session_mode`) uses that value: the session's pipeline runs and
  ad-hoc queries group together in the query audit ledger, matching the
  stdio bridge's per-connection grouping.
- A stateless request (protocol `2026-07-28` and later) gets a UUID v4
  minted per request: attribution stays intact, grouping granularity is
  honestly per-request — the protocol removed the conversation-level
  handle, and inventing one (fingerprinting callers) is worse than
  stating the limit. docs/mcp.md documents this difference.

### Error mapping

Same two-level policy as the bridge, adapted to in-process responses:

- Dispatch succeeds but the REST handler answers non-2xx → tool result
  with `isError: true` carrying the response body text (the model can
  self-correct: fix SQL, re-list, supply a missing parameter).
- Unknown tool name → JSON-RPC `invalid_params` with the "re-issue
  tools/list" hint.
- `tools/list` unable to produce an inventory (the synthetic `GET
  /pipelines` fails, which in-process means a bug rather than a network
  condition) → JSON-RPC internal error.

### Response size cap

The server handler collects synthetic-response bodies with the same
256 MB ceiling the stdio bridge inherits from the CLI client, kept as a
constant — deliberately not configurable, so one pipeline behaves
identically through both bindings. The constant lives in the server MCP
module with a comment naming the parity; it is not shared because the
CLI's ceiling is client-wide, not MCP-specific.

Stated honestly: in-process, this cap bounds only the MCP layer's own
copy of the result and the size of the tool result handed to rmcp and
the host. It does not bound execution-layer memory — the REST handler
has already materialized the full result (RecordBatches → JSON) before
the collector sees a byte. That is a pre-existing property of the REST
surface that every binding inherits, addressed by the
resource-governance non-goal below, not by this cap. (On the stdio path
the same ceiling genuinely protects the bridge, which is a separate
process.)

### Freshness

Same as the bridge: the inventory is projected on every `tools/list`, no
caching — and pipeline tool names are additionally re-resolved on every
call (see "Stateless by protocol"). `listChanged` notifications remain out of scope — the server has
no production config hot-reload (`config.write()` appears only in tests),
so the inventory can only change across a restart, and a restart tears
down every MCP session anyway.

---

## Cloud contexts

Unchanged and out of scope: the cloud gateway serves only `query` and
`schema` and does not gain `/mcp`. The stdio bridge's refusal of cloud
contexts stays as is.

---

## Testing

- **Projection unit tests** move to `crates/mcp-core` with the code.
- **Server integration tests** (`crates/server/tests/mcp_http.rs`): serve
  the configured router on an ephemeral-port listener inside the test and
  drive it with rmcp's streamable-HTTP client: `tools/list` contains the
  seeded pipeline tools plus the built-ins; a pipeline call executes and
  returns rows; `query` round-trips; unknown tool → `invalid_params`; a
  failing call (bad SQL) → `isError: true`.
- **Auth forwarding test**: with a token-required server configuration, a
  tool call through `/mcp` with the Bearer token succeeds — which proves
  the header actually reaches the handlers: a dropped `Authorization`
  would make the handler-level `require_session` reject the synthetic
  request even though the session gate had already passed the caller.
- **Session gate tests**: with auth enabled, an anonymous `initialize` is
  rejected with a transport-level 401 and yields no usable session (a
  follow-up request claiming a session id fails the same way); the same
  `initialize` with the Bearer token succeeds. With auth disabled,
  anonymous `initialize` works — pinning the gate's no-op behavior.
- **Host allowlist tests**: a request with a non-loopback `Host` header
  is 403 by default, succeeds once that host is declared via
  `--mcp-allowed-host`, and loopback keeps working in both
  configurations.
- **Stateless protocol regression**: driving `/mcp` as a `2026-07-28`
  stateless client, `tools/list` and a subsequent pipeline `tools/call`
  arrive as independent requests served by independent handler instances —
  the call must still resolve and execute. This is the direct regression
  test for the per-call resolution design.
- **Attribution tests**: a pipeline call through `/mcp` records an id
  (assert via the ledger or a handler-level header check, matching how
  the bridge's wiremock test pins the header) — covering both layers: a
  legacy client's `Mcp-Session-Id` is echoed as the id, a stateless
  request gets a UUID.
- **CLI regression**: the existing bridge unit tests and `mcp_e2e` keep
  passing after the crate extraction — the move must be behavior-neutral.

All tests are self-contained (no external services) and run in CI; nothing
needs `#[ignore]`.

---

## Documentation

- `docs/mcp.md` — new "Remote (streamable HTTP)" section: the `/mcp`
  endpoint, host configuration for URL-based MCP servers, Bearer-token
  guidance, and an explicit statement of the local-vs-remote split (stdio
  for local hosts, `/mcp` for hosts that cannot spawn a binary). The
  architecture diagram gains the second path. Includes a reverse-proxy
  subsection: either forward the public `Host` and declare it via
  `--mcp-allowed-host`, or have the proxy rewrite `Host` to the upstream
  loopback authority — and how the 403 from a mismatch presents.
- `docs/pipelines.md` — roadmap note moves to shipped.

---

## Non-goals

- **OAuth / MCP authorization spec.** `/mcp` authenticates with the
  existing Bearer scheme. Hosts whose remote-MCP flow requires OAuth are
  not served by this milestone; per the OSS/Cloud boundary, an OAuth
  story is a Cloud-side design if it happens.
- **`listChanged` notifications** — see Freshness.
- **Jobs tools** — unchanged from v1: needs its own submit/poll/cancel
  design if demand appears.
- **Retiring or changing the stdio bridge** — the CLI refactor is a pure
  code move to the shared crate.
- **Per-parameter descriptions** — separate proposal (in progress
  independently); whatever the enriched inventory carries, both bindings
  pick up through the shared projection automatically.
- **Cloud gateway `/mcp`** — see Cloud contexts.
- **Execution-layer resource governance** — result-size limits on
  pipeline executes, execution timeouts, and concurrency caps. The REST
  handlers materialize full results with none of these bounds today;
  `/mcp` inherits that property rather than creating it, and bounding it
  belongs to a server-wide design benefiting every binding, not to one
  protocol adapter. Raised by review on this spec — recorded here as the
  follow-up trigger.

---

## Risks / probes — both resolved during spec review

- **R1 — resolved during spec review** (was: header and session access in
  rmcp 3.1.4). Verified against the 3.1.4 source: rmcp injects
  `http::request::Parts` into every request's extensions, so the handler
  reads the inbound `Authorization` (and `Mcp-Session-Id`, when a legacy
  client sends one) per request, stateless mode included. The cargo
  feature is `transport-streamable-http-server`. No fallback needed.
- **R2 — resolved during spec review** (was: axum interop via
  `nest_service`). Verified against the 3.1.4 source and the workspace
  lockfile: rmcp has no runtime axum dependency at all — its
  `StreamableHttpService` implements
  `tower_service::Service<http::Request<B>>` generically over any
  `http-body` 1.x body, with `Response = BoxResponse` and
  `Error = Infallible`, against the same `http = 1` /
  `tower-service = 0.3` that the server's axum 0.7 stack already uses
  (the `http 0.2` in the lockfile belongs to tonic). That is exactly the
  shape `nest_service` mounts. The first implementation PR's routine CI
  build is the only remaining confirmation — a formality, not a probe.

## Decided

- **Auth: existing Bearer, forwarded verbatim** (2026-08-28) — no new
  auth code; enforcement stays in the REST layer. OAuth explicitly
  deferred to the Cloud side.
- **Default-on, no config flag** (2026-08-28) — `/mcp` exposes no
  privilege the REST surface does not already expose under the same
  auth; a switch would be a config surface guarding nothing.
- **Architecture: tower self-dispatch** (2026-08-28) — protocol adapter
  over the server's own router; the alternatives (handler-core refactor,
  projection duplication) are recorded above and rejected.
- **Shared crate: `crates/mcp-core`, package `skardi-mcp-core`**
  (2026-08-28) — projection + built-in constants + `encode_component`;
  dispatch matches deliberately stay per-binding.
- **Session gate in front of rmcp** (2026-08-31) — every `/mcp` request
  authenticates via `verify_session` before rmcp session creation;
  anonymous `initialize` is a transport-level 401, not a retained
  session. Added for a review finding on the initial spec.
- **Host allowlist: additive `--mcp-allowed-host` flag, no
  allow-any escape hatch** (2026-08-31) — rmcp's loopback-only default
  stays as the DNS-rebinding guard; remote deployments declare their
  public hostnames. `allowed_origins` stays unrestricted. Added for a
  review finding on the initial spec; verified against the rmcp 3.1.4
  source, so this is not part of probe R1.
- **Stateless-first handler** (2026-08-31) — protocol `2026-07-28`
  removes sessions (SEP-2567) and rmcp constructs the handler once per
  request on the stateless path, so the handler holds no per-session
  state: pipeline tool names resolve per call, audit ids layer
  `Mcp-Session-Id` over per-request UUIDs, `legacy_session_mode` stays
  at rmcp's default. Added for a review finding on the initial spec.
- **Response cap stays a 256 MB constant** (2026-08-31) — binding parity
  with the stdio bridge over a new config surface; a conservative,
  configurable ceiling is deferred to the execution-resource-governance
  design, and the cap's honest (second-copy) role is stated in its
  section. Added for a review finding on the initial spec.
