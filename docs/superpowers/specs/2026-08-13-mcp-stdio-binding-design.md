# MCP stdio Binding — `skardi mcp` (v1)

**Status:** Draft for review
**Date:** 2026-08-13
**Branch:** `mcp-stdio-binding-design`

**Scope — touches two crates, not one.** This is not a CLI-only feature.
Most of the work lives in `crates/cli` (the new `skardi mcp` subcommand),
but shipping it also requires a small, additive, **decided** change in
`crates/server`: `GET /pipelines` gains `description` and `parameters`
fields it does not return today (Option A — see "Server-side change" for
the full rationale and the rejected zero-server-change alternative). A
server-side PR is a prerequisite for this design, not an optional
enhancement layered on top of it.

## Summary

Skardi gains its third agent-facing binding: an MCP (Model Context Protocol)
server. v1 ships as a new CLI subcommand — `skardi mcp` — that speaks MCP over
**stdio** to a local host (Claude Desktop, Cursor, any MCP client) and proxies
every operation to a running `skardi-server` over REST, using the CLI's
existing `ApiClient`. The server's *execution* path is untouched — no
pipeline runs, no query executes, any differently than it does today. But
this design does require one small, additive, **decided** server-side
change: `GET /pipelines` gains `description` and `parameters` fields it
doesn't return today (Option A — see "Server-side change"). That change is
a prerequisite for the bridge, not an optional enhancement — without it,
pipeline tools would ship with no LLM-facing description and parameter
types rendered as raw Rust Debug strings.

The tool surface mirrors what REST and shell already expose:

1. **Pipeline tools** — every loaded pipeline projected as one MCP tool
   (name, description, and a JSON Schema input derived from the pipeline's
   inferred parameter types). This is roadmap item `5`'s "same pipeline YAML
   projected to MCP tools".
2. **`query`** — ad-hoc SQL against the federated engine, same governance as
   the REST `/query` endpoint (DML gated by per-source `access_mode`, DDL
   always rejected).
3. **`list_data_sources`** — the discovery surface: tables, schemas, and
   `kind: semantics` plain-English descriptions from `GET /data_source`.

Jobs tools, streamable HTTP transport, and OAuth are explicitly out of scope
for v1 (see Non-goals).

## Motivation

Hosts with a Bash tool (Claude Code, Cursor agent mode) already use the
`skardi` CLI as a tool with zero MCP configuration — the README sells this.
The uncovered population is **hosts without a shell**: Claude Desktop, ChatGPT
desktop, IDE MCP integrations, and any client that only speaks MCP. The
roadmap has promised this binding ("MCP-soon" in the README banner; unchecked
item under `5` Agent-facing bindings).

stdio ships first because it is the cheap half: the CLI is already a thin
HTTP client for skardi-server, so most of the MCP subcommand is a translator
between two existing, stable interfaces — plus one small, decided,
additive server change (below) so descriptions and clean parameter types
survive that translation instead of getting lost in it. The streamable HTTP
transport (an `/mcp` route inside skardi-server, for remote/hosted agents)
is deferred; the tool projection logic designed here is transport-independent
and will be shared when that lands.

## Repo facts the design builds on

Verified against the current tree:

- `GET /pipelines` returns only `name`, `version`, `endpoint` per pipeline —
  **no parameters, no description** (`pipeline_handlers.rs::list_pipelines`).
- `GET /pipeline/:name` returns parameters as
  `{name, type: format!("{:?}", InferredFieldType.field_type)}` — a Debug
  string of a DataFusion `DataType` — and **does not return
  `metadata.description`** even though `ComponentMetadata.description:
  Option<String>` exists and the pipeline YAML documents it.
- `POST /:name/execute` takes a **flat** JSON object (`ExecuteRequest` is
  `#[serde(flatten)] parameters: HashMap<String, Value>`); missing
  placeholders are a `parameter_validation_error` listing
  `missing_parameters` — i.e. every placeholder is a required key, and
  nullable use-sites take an explicit JSON `null`.
- `POST /query` takes `{sql, max_rows?, ai_context?}`; `ai_context`, when
  present, must be an object with non-empty `purpose` and `session_id`
  (recorded for observability, never executed).
- `GET /data_source` returns per-source tables, schemas, and merged semantic
  descriptions.
- The CLI's `ApiClient` (reqwest wrapper) already handles base URL
  (`--server` / `SKARDI_SERVER_URL` / `~/.skardi/config.yaml`) and Bearer
  auth (`SKARDI_API_TOKEN` / config `token`), with uniform `ApiError`
  mapping. `wiremock 0.6` is already a dev-dependency of the CLI crate.
- The CLI binary is named `skardi`; subcommands are `query`, `run`,
  `pipeline`, `schema`, `jobs`, `health`.

## Architecture

```
┌──────────────┐  stdio (JSON-RPC)  ┌────────────────────┐  HTTP (REST)  ┌───────────────┐
│  MCP host     │ ◄───────────────► │  skardi mcp         │ ◄──────────► │  skardi-server │
│ (Claude       │  spawned as a     │  (CLI subcommand:   │  ApiClient,  │  (executes     │
│  Desktop, …)  │  child process    │   MCP ⇄ REST bridge)│  Bearer auth │   SQL)         │
└──────────────┘                    └────────────────────┘               └───────────────┘
```

Host config example (Claude Desktop `claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "skardi": {
      "command": "skardi",
      "args": ["mcp", "--server", "http://localhost:8080"]
    }
  }
}
```

`skardi mcp` takes no new configuration: server URL and token resolve exactly
as for every other subcommand (flag → env → config file). Because the proxy
is transport-level, pointing `--server` at a remote skardi-server also works,
making the subcommand a local MCP gateway to a remote deployment.

### SDK

Use **`rmcp`** (the official Rust MCP SDK, `modelcontextprotocol/rust-sdk`)
with only the `server` + stdio transport features enabled, keeping its HTTP
stack (and any axum version skew) out of the dependency graph. Tools here are
**dynamic** — they come from YAML loaded by a remote server — so the `#[tool]`
macro path does not apply; the bridge implements `ServerHandler` manually:

- `list_tools` → fetch pipeline inventory from the server, project to tool
  definitions, append the two built-in tools.
- `call_tool` → route by tool name: pipeline tool → `POST /:name/execute`;
  `query` → `POST /query`; `list_data_sources` → `GET /data_source`.
- Capabilities: tools only. No resources, no prompts, no `listChanged`
  (pipelines are static per server process).
- `ServerInfo.instructions`: one short paragraph telling the model what
  Skardi is and the intended pattern (prefer pipeline tools; use
  `list_data_sources` to discover tables before writing ad-hoc `query` SQL).

Pin the exact rmcp version at implementation time; verify it builds under the
workspace toolchain. Logging goes **exclusively to stderr** — stdout is the
protocol channel; a single stray `println!` corrupts the session. The
subcommand installs a stderr `tracing`/`env_logger` writer and runs until the
host closes stdin.

## Tool projection

### Pipeline tools

One MCP tool per pipeline. Sources, in order of preference:

| Tool field | Source |
|---|---|
| `name` | pipeline name, sanitized (below) |
| `description` | `metadata.description` from the pipeline YAML (see server-side change) |
| `inputSchema` | inferred parameter set → JSON Schema object |

**Name sanitization.** MCP clients commonly enforce `^[a-zA-Z0-9_-]{1,64}$`
for tool names. Pipeline names are URL-path segments today and may contain
other characters. Rule: replace every character outside `[a-zA-Z0-9_-]` with
`_`, truncate to 64; on collision after sanitization, append `_2`, `_3`, … in
deterministic (sorted) order. The bridge keeps a tool-name → pipeline-name
map for dispatch; the original pipeline name is echoed in the tool
description so the model can correlate with server-side errors.

**Reserved names.** `query` and `list_data_sources` are reserved for the
built-in tools. A pipeline with a colliding sanitized name keeps its function
but its tool is renamed with a `_pipeline` suffix, and a warning is logged to
stderr.

**Input schema mapping.** All placeholders become **required** properties
(matching the server's `missing_parameters` behavior) — required governs
whether the key must appear; nullability (below) governs whether its value
can be `null`; the two are independent axes in the server's own validation
(`InferredFieldType.nullable` vs. key presence in `substitute_sql_params`).
DataFusion type → JSON Schema:

| Inferred `DataType` | JSON Schema |
|---|---|
| `Utf8` / `LargeUtf8` | `{"type": "string"}` |
| integer family (`Int*`, `UInt*`) | `{"type": "integer"}` |
| float/decimal family (incl. `Decimal128`/`Decimal256`) | `{"type": "number"}` |
| `Boolean` | `{"type": "boolean"}` |
| `Date32`/`Date64`/`Timestamp*` | `{"type": "string", "format": "date-time"}` |
| `List(inner)` | `{"type": "array", "items": <map(inner)>}` |
| anything else / unknown | `{}` (any) |

A parameter whose use-site is nullable maps to a type union with `"null"`
(e.g. `{"type": ["string", "null"]}`) — callers must still pass the key, as
the server requires. `additionalProperties: false` on the object, since the
server rejects `unsupported_parameters`.

**Special case: `VALUES {name}` (multi-row tuple-list parameters, e.g.
`{rows}` in a batch `INSERT`).** The DataFusion-type table above does not
apply to this shape. Tracing `SqlSchemaInferrer` confirms why: its AST-based
placeholder→column mapping only walks `SetExpr::Select`
(`extract_placeholder_columns`, inferencer.rs) — a `VALUES (?)` clause is
`SetExpr::Values` and is skipped outright (`_ => continue`). With no AST
mapping, the fallback (`infer_column_from_parameter_name`) echoes the
parameter's own name back as a "column name" (no `_min`/`_max` suffix to
strip), which matches no real column, so type inference bottoms out at its
final default: **`DataType::Utf8`**. Projected naively through the table
above, `{rows}` would get `{"type": "string"}` — not absent guidance
(the "anything else" row's `{}`) but an *actively wrong* constraint, since
the real accepted shape is a JSON array of row-tuples
(`[["d1","doc-a",[1.0,0.0,0.0,0.0]], ...]`, per pipelines.md's multi-row
`VALUES` shape). A host that validates tool arguments against the declared
schema would reject a legitimate array argument outright.

The bridge must detect this shape independently of the inferred type,
reusing the loader's own detection regex (`\bVALUES\s*\{name\}\b` against
the pipeline's SQL template — the same pattern `convert_named_to_placeholders`
already matches) and override the projected schema to
`{"type": "array", "items": {"type": "array"}}` — array-of-arrays, not
attempting to model per-position tuple typing (JSON Schema's `prefixItems`
could express fixed per-column types, but each pipeline's row shape varies
and isn't otherwise tracked; the looser shape is honest about what the
bridge actually knows).

### Built-in tool: `query`

```
name:        query
description: Run ad-hoc SQL against Skardi's federated engine. DML is only
             accepted on data sources configured with access_mode:
             read_write; DDL is always rejected. Use list_data_sources
             first to see available tables.
inputSchema: {
  sql:      {"type": "string"}           (required)
  max_rows: {"type": "integer"}          (optional; server default 1000)
  purpose:  {"type": "string"}           (optional; recorded in the query
                                          audit log — one line on why you
                                          are running this query)
}
```

When `purpose` is provided, the bridge sends
`ai_context: {purpose, session_id}` where `session_id` is a UUID generated
once per MCP connection. When absent, `ai_context` is omitted entirely
(the server rejects a partial object). This is the v1 instantiation of the
**agent identity passthrough seam** (roadmap `6`): the per-connection
session id plus the MCP `clientInfo` (name/version, available from the
initialize handshake) flow through one choke-point function that later
versions can extend to full identity injection without touching call sites.

### Built-in tool: `list_data_sources`

No parameters. `GET /data_source`, returned verbatim (it already includes
table schemas and semantic descriptions).

**Auth note.** Unlike `query` and pipeline tools, this stays unauthenticated
even when `AUTH_MODE=BETTER_AUTH_DIESEL_SQLITE` is set: `get_data_sources`
(`pipeline_handlers.rs`) never calls `require_session`, unlike
`execute_query` and `execute_pipeline_by_name`, which both do. This is
existing REST behavior, not something this binding introduces or changes —
the bridge proxies `/data_source` as-is, and the same gap is reachable today
via a plain unauthenticated `curl`. Worth calling out here because this
design's own `query` tool description tells every MCP client to call
`list_data_sources` first, so it is the one built-in tool nearly every
connection will hit regardless of whether it holds a valid token. What it
returns is schema plus `kind: semantics` descriptions — operator-authored
natural-language notes about what tables and columns *mean*, which can
carry more business context than the raw schema alone. Whether `/data_source`
should require auth is a server-level decision outside this design's
scope; deployments with sensitive semantic descriptions should evaluate it
independently.

### Freshness

`list_tools` fetches the pipeline inventory from the server **on every
call** — no cache. Hosts call it rarely (connect time), the hop is
typically localhost, and this transparently reflects a restarted or
re-configured server without bridge restart logic.

## Server-side change (required — decided: Option A)

**This section is not a menu of options — it documents one required change
to `crates/server` and the rejected alternative that would have avoided it.**
The REST surface does not currently expose what the projection needs in one
round trip: `GET /pipelines` lacks parameters *and* descriptions, and
`GET /pipeline/:name` lacks the description and renders types as Rust Debug
strings. Implementing pipeline tools without this change means shipping
`skardi mcp` with tool descriptions the LLM never sees and parameter types
rendered as opaque Debug strings — so this is scoped as part of the v1
binding, not a "nice to have" deferred to later.

**Decided: enrich `GET /pipelines`.** Each list item gains two fields
(existing fields unchanged — additive, backward compatible):

```json
{
  "name": "product-search-demo",
  "version": "1.0.0",
  "endpoint": "/product-search-demo/execute",
  "description": "Product search and filtering",
  "parameters": [
    {"name": "brand", "type": "Utf8", "json_type": "string", "nullable": true}
  ]
}
```

`json_type` is emitted server-side from the same mapping table above, so the
bridge (and any future binding — the skills generator needs exactly the same
data) never parses Debug strings. `GET /pipeline/:name` gains the same
`description` and `json_type` fields for consistency. This is a ~20-line
change in `pipeline_handlers.rs` plus tests — the only server-side edit in
this design, and it lands in `crates/server` alongside (or ahead of) the
`crates/cli` work, not after it: the bridge's tool-description and type-
mapping tests depend on these fields existing.

**Rejected: zero-server-change alternative (Option B).** The bridge would
instead call `GET /pipeline/:name` per pipeline (N+1, acceptable for
realistic N), parse the Debug `type` string itself, and fall back to a
generic description ("Execute pipeline `<name>`") when none is available.
Rejected because tool descriptions are the single highest-leverage input to
LLM tool-selection quality, and the YAML authors already wrote them — hiding
them from the binding behind a generic fallback wastes exactly the metadata
this feature exists to project.

## Execution flow and error mapping

`tools/call` on a pipeline tool:

1. Look up the pipeline name from the tool-name map; unknown tool →
   JSON-RPC "unknown tool" error (protocol-level, host bug).
2. Arguments are passed through as the flat execute body — the server is
   the validator (missing/unsupported/type errors come back structured).
3. `POST /:name/execute` via `ApiClient` (reusing the same call path as
   `skardi run`).
4. Success → one `text` content block containing the response JSON verbatim
   (`{data, rows, execution_time_ms, …}`). No client-side reshaping in v1.
5. Server-reported failure (4xx/5xx) or connect failure → tool result with
   `isError: true` and the server's error message + `error_type` as text.
   These are *execution* errors the model should see and react to (fix a
   parameter, choose another tool), not protocol errors.

`query` and `list_data_sources` follow the same pattern against their
endpoints. `list_tools` with an unreachable server returns a JSON-RPC error
whose message names the resolved server URL and the three ways to configure
it (mirroring `ApiError::Connect`'s existing wording).

Result size is bounded by the server's `max_rows` cap (default 1000) and the
CLI's existing 256 MB response ceiling; v1 does no additional truncation.

## Testing

Per repo conventions: no local test runs (verification happens on GitHub
CI), `cargo fmt` before push, and CI bare-runs `#[ignore]` tests — nothing
here needs `#[ignore]`, as all tests are self-contained.

- **Unit (cli crate):** projection is implemented as pure functions —
  pipeline-inventory JSON → tool definitions — so sanitization, collision
  suffixing, reserved-name handling, type mapping (incl. nullable unions,
  date/timestamp types, and unknown types), the `VALUES {name}` array-of-
  arrays override, and `ai_context` assembly are table-driven unit tests.
- **Bridge-level (wiremock):** construct the bridge over an `ApiClient`
  pointed at a wiremock server (same pattern as existing CLI command tests);
  drive `list_tools`/`call_tool` directly and assert REST interactions
  (execute body is flat, `purpose` → `ai_context`, error → `isError`).
- **End-to-end (spawned binary):** spawn `env!("CARGO_BIN_EXE_skardi") mcp
  --server <wiremock url>` as a child process and connect an rmcp *client*
  over stdio (rmcp's client feature as a dev-dependency): initialize
  handshake, `tools/list` shows pipelines + built-ins, `tools/call` round
  trip, server-down behavior. This also permanently guards the "stdout is
  protocol-only" invariant — any stray print breaks the handshake test.

## Documentation updates (implementation phase)

- `docs/cli.md` — new `mcp` subcommand section with host config examples.
- `docs/pipelines.md` — MCP moves from "v1.1 roadmap" to shipped in the
  surfaces list.
- `README.md` — check the roadmap `5` MCP box; revise "MCP-soon" phrasing.
- New `docs/mcp.md` — host setup (Claude Desktop, Cursor), tool surface,
  auth notes, troubleshooting.
- `docs/agent_data_plane.md` — MCP binding status + the identity-seam
  paragraph gets its "first carrier" footnote (`ai_context` via `query`).

## Non-goals (v1)

- **Streamable HTTP transport** (`/mcp` route in skardi-server) — the
  follow-up milestone; shares this projection logic. Where the shared code
  eventually lives (extracted crate vs. duplicated module) is decided then,
  not pre-abstracted now.
- **Jobs tools** — MCP tool calls are synchronous; the submit/poll/cancel
  shape needs its own design if demand appears.
- **OAuth / MCP authorization spec** — irrelevant to stdio; the Bearer
  token story is inherited from the CLI unchanged.
- **`listChanged` notifications, resources, prompts, structured
  content/output schemas** — nothing in the current surface needs them.
- **Per-parameter descriptions in pipeline YAML** — would improve tool
  schemas further, but it is a new YAML surface; propose separately if tool
  ergonomics prove insufficient.

## Decided

- **Server-side inventory enrichment: Option A** (2026-08-13) — `GET
  /pipelines` gains `description` and `parameters` fields; see "Server-side
  change". This is a required change to `crates/server`, not deferred —
  the zero-server-change alternative (Option B) is rejected and kept in
  that section only for the record.

## Open decisions

1. **`purpose` parameter on `query`** — include as optional (recommended,
   as specced) or omit entirely from v1.
2. **Built-in tool naming** — `query` / `list_data_sources` as specced, or
   e.g. `skardi_query` prefixing to reduce collision odds with user
   pipeline names (recommendation: unprefixed; MCP hosts already namespace
   tools per server, and reserved-name suffixing covers the edge).
