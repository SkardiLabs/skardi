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
request runs the full middleware stack and the unmodified REST handlers.

Why self-dispatch instead of calling handler internals directly:

- **Auth and audit parity with zero new code.** The synthetic request
  carries the `/mcp` caller's own `Authorization` header verbatim, so
  authentication, authorization errors, and ledger recording are *exactly*
  the REST semantics — enforced in one place, the existing middleware and
  handlers. `/mcp` adds no privilege that `POST /<name>/execute` and
  `POST /query` do not already expose, which is also why it ships
  default-on with no config flag.
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
  established pattern: trait methods are thin wrappers over inherent
  `do_*` methods so tests can drive the logic without constructing
  `RequestContext`. State: the captured REST `Router`, and per-session
  identity (below).
- `mod.rs` — service construction and the `configure_routes` wiring.

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

### Session identity

One session id per MCP session, used exactly as the bridge uses its
per-connection UUID: sent as `ai_context.session_id` when `query` carries a
`purpose`, and as `x-skardi-session-id` on pipeline executes, so one MCP
session's pipeline runs and ad-hoc queries group together in the query
audit ledger. Preferred carrier is rmcp's own streamable-HTTP session id
(the `Mcp-Session-Id` the protocol already maintains); if that is not
reachable from the handler (see risk R1), a UUID v4 minted at session
initialization is equivalent.

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

The bridge inherits the CLI client's 256 MB response ceiling; the server
handler collects response bodies with the same 256 MB cap so a pipeline
returning more yields an `isError` tool result instead of an unbounded
allocation. The constant lives in the server MCP module with a comment
naming the parity; it is not shared because the CLI's ceiling is
client-wide, not MCP-specific.

### Freshness

Same as the bridge: the inventory is projected on every `tools/list`, no
caching. `listChanged` notifications remain out of scope — the server has
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
  tool call through `/mcp` with the Bearer token succeeds and without it
  yields an `isError` result carrying the REST 401 — proving enforcement
  happens in the REST layer, not in new MCP code.
- **Session attribution test**: a pipeline call through `/mcp` records the
  session id (assert via the ledger or by a handler-level header check,
  matching how the bridge's wiremock test pins the header).
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
  architecture diagram gains the second path.
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

---

## Risks / probes (resolve before or at plan task 1)

- **R1 — header and session access in rmcp 3.1.4.** The design needs, per
  tool call, the inbound `Authorization` header, and per session, a stable
  session id. Verify what rmcp's streamable-HTTP server service exposes
  (request parts via `RequestContext` extensions, a session-manager API,
  or neither). Fallback if per-request headers are unreachable: a thin
  tower layer in front of the service captures `Authorization` keyed by
  `Mcp-Session-Id` at initialize time and the handler looks it up — the
  observable behavior (verbatim forwarding) is identical. This probe also
  confirms the exact cargo feature name for the streamable-HTTP server
  transport.
- **R2 — axum interop.** rmcp's service must mount under the server's
  axum version via `nest_service`. tower `Service` compatibility makes
  this low-risk; the probe in R1 confirms it compiles.

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
