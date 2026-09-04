# MCP Binding

Skardi serves the [Model Context Protocol](https://modelcontextprotocol.io)
over two transports of the same tool surface — pipelines, ad-hoc `query`,
and `list_data_sources`:

- **Local (stdio)** — `skardi mcp`, a CLI subcommand the host spawns as a
  child process. For hosts that run a local binary: Claude Desktop, Cursor,
  your own agent loop.
- **Remote (streamable HTTP)** — `/mcp` on skardi-server itself. For hosts
  that cannot spawn one: claude.ai, mobile clients, hosted agent platforms.
  See [Remote (streamable HTTP)](#remote-streamable-http).

Both are the same agent-facing binding of the same pipeline YAML, next to
the shell verb (`skardi run`) and REST (`POST /<name>/execute`). The stdio
bridge is not retired by the HTTP endpoint.

Neither transport is a second engine. The bridge is a long-lived child
process speaking JSON-RPC over stdin/stdout that proxies every tool call to
a running `skardi-server` over REST and returns the response verbatim; the
HTTP endpoint is a protocol adapter inside the server that dispatches each
tool call to its own REST routes in-process — same handlers, same
validation, same audit path.

```
┌──────────────┐  stdio (JSON-RPC)  ┌────────────────────┐  HTTP (REST)  ┌───────────────┐
│  MCP host     │ ◄───────────────► │  skardi mcp         │ ◄──────────► │  skardi-server │
│ (Claude       │  spawned as a     │  (CLI subcommand:   │  ApiClient,  │  (executes     │
│  Desktop, …)  │  child process    │   MCP ⇄ REST bridge)│  Bearer auth │   SQL)         │
└──────────────┘                    └────────────────────┘               └───────────────┘

┌──────────────┐  streamable HTTP (JSON-RPC over POST + SSE)  ┌────────────────────────┐
│  MCP host     │ ◄──────────────────────────────────────────► │  skardi-server /mcp     │
│ (claude.ai,   │  Authorization: Bearer <token>               │  (in-process dispatch   │
│  hosted, …)   │                                              │   to its REST handlers) │
└──────────────┘                                               └────────────────────────┘
```

---

## Host setup (stdio bridge)

Claude Desktop (`claude_desktop_config.json`):

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

Cursor (`.cursor/mcp.json`, same `mcpServers` shape):

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

`skardi mcp` takes no subcommand-specific configuration: the server URL and
Bearer token resolve exactly as for every other subcommand — flag (`--server`,
`--token`) → environment (`SKARDI_SERVER_URL`, `SKARDI_API_TOKEN`) → config
file (`~/.skardi/config.yaml`, honoring the current context). Because the
bridge is transport-level, pointing `--server` at a remote skardi-server
also works, making `skardi mcp` a local MCP gateway to a remote deployment.

One exception: **cloud contexts are refused.** The cloud gateway serves only
`query` and `schema`; it does not serve pipeline execution or
`/data_source`, so `skardi mcp` in a cloud context exits with an error
before serving instead of offering tools that would all fail.

---

## Remote (streamable HTTP)

skardi-server serves the same MCP tool surface at `http://<server>/mcp` —
default-on, no config flag. Hosts that take a URL instead of a command
connect directly:

```json
{
  "mcpServers": {
    "skardi": {
      "url": "http://localhost:8080/mcp"
    }
  }
}
```

**Auth is Bearer-only and covers everything.** With auth enabled, every
`/mcp` request — `initialize` and `tools/list` included — requires
`Authorization: Bearer <token>`. This is a deliberate divergence from REST,
where `GET /pipelines` is readable without a token: on `/mcp` the tool
inventory sits behind the same credential as execution. Session cookies are
never accepted on `/mcp`.

**Host allowlist.** As DNS-rebinding protection, a request is accepted only
when its `Host` header is on the allowlist: loopback (`localhost`,
`127.0.0.1`, `::1`) by default, plus any values declared with the
repeatable server flag:

```bash
skardi-server --mcp-allowed-host api.example.com --mcp-allowed-host api.example.com:8443
```

An entry with a port matches that port exactly; a portless entry matches
the host on any port. There is deliberately no allow-any option — a public
deployment names its hostnames.

**Reverse proxies.** Either forward the public `Host` and declare it via
`--mcp-allowed-host`, or rewrite `Host` to the upstream loopback authority.
A mismatch presents as `403 Forbidden: Host header is not allowed`. Long
tool calls hold their POST open for the whole run; responses are SSE with a
15 s keep-alive so bytes keep flowing, but deployments running long
pipelines should still check proxy read timeouts (nginx's
`proxy_read_timeout` defaults to 60 s, and a keep-alive only helps when the
proxy counts any bytes as liveness).

**Audit grouping.** A legacy-protocol session (2025-11-25 and earlier)
groups its pipeline runs — and the queries that carry `purpose` — in the
query-audit ledger under the transport's `Mcp-Session-Id`. Stateless-protocol
requests (2026-07-28 and later) are attributed per-request with a minted
UUID — that protocol revision removed the conversation-level handle, so
there is nothing durable to group by. On either protocol the id reaches a
`query` audit row only inside `ai_context`, which is omitted when `purpose`
is absent: a purpose-less query is recorded with no session id at all.
Pipeline runs always carry the id, via `X-Skardi-Session-Id`.

---

## Tool surface

### Pipeline tools

Every pipeline registered on the server becomes one MCP tool. Tool names
must match `^[a-zA-Z0-9_-]{1,64}$`, so pipeline names are sanitized: any
other character becomes `_` and names are truncated to 64. If two pipelines
sanitize to the same name, the later one (in original-name sort order) gets
a numeric suffix (`_2`, `_3`, …); a pipeline literally named `query` or
`list_data_sources` is exposed as `query_pipeline` /
`list_data_sources_pipeline` so it never shadows the built-ins.

The tool description is the pipeline's `metadata.description` with the
original pipeline name appended (so renamed tools stay correlatable with
server-side errors); pipelines without a description get
``Execute pipeline `<name>` ``. The input schema is generated per parameter
from the inferred Arrow type — strings, integers, numbers, booleans, dates,
timestamps, and arrays all map to their JSON Schema counterparts, each
nullable (`"type": ["string", "null"]`) because explicit `NULL` is a valid
parameter value. Arrays declare `minItems: 1` (the server rejects empty
arrays before execution), and multi-row `VALUES {name}` placeholders are
typed as array-of-arrays with non-empty rows. Every parameter is required
and unknown keys are rejected. A pipeline author can attach a one-line
description to any parameter via `spec.parameters` in the pipeline YAML
(see [pipelines.md](pipelines.md)); it rides inside that parameter's
schema as the standard `description` keyword, so the model no longer has
to guess semantics from the parameter name alone.

Every pipeline call carries a session id as `X-Skardi-Session-Id` — the
same id `query` sends in `ai_context.session_id` when `purpose` is given —
so an MCP session's pipeline runs and purposeful ad-hoc queries group
together in the query audit ledger. A query without `purpose` sends no
`ai_context` and is audited without a session id, so it stands outside the
group. On the stdio bridge the id is one UUID per MCP connection; on `/mcp`
see audit grouping under [Remote (streamable HTTP)](#remote-streamable-http).

### `query`

Ad-hoc SQL against the federated engine — the MCP face of `POST /query`.

| Argument | Type | Notes |
|---|---|---|
| `sql` | string, **required** | One statement. DML only on `access_mode: read_write` sources; DDL is always rejected. |
| `max_rows` | integer, optional | Result row cap; server default 1000. |
| `purpose` | string, optional | One line on why you are running this query. Sent as `ai_context: {purpose, session_id}` and recorded in the server's query audit log; the `session_id` is the same per-connection (bridge) or per-session/per-request (`/mcp`) id pipeline calls carry, so related calls group together in the ledger. Omitted entirely when not provided — a query without `purpose` is audited without a session id and does not group with the session's other calls. |

### `list_data_sources`

No arguments. Returns the server's data sources with tables, column
schemas, and semantic descriptions — the same body as `GET /data_source`.
Call it before writing ad-hoc SQL with `query`.

---

## Freshness

The pipeline inventory is fetched from `GET /pipelines` on **every**
`tools/list` request — a pipeline added to the server appears as soon as the
host re-lists. (`/mcp` goes one further and re-resolves the inventory on
every pipeline `tools/call` too, so a rename between list and call is an
in-band "unknown tool" nudge to re-list, never a stale dispatch.) Hosts
that list once at connect time and never again keep their snapshot until
reconnect. Neither binding emits `listChanged` notifications in v1.

---

## Auth notes

Per binding:

- **stdio bridge** — sends the Bearer token it inherits from normal CLI
  config resolution on every REST call. Note that on today's server,
  `GET /pipelines` (the tool inventory) and `GET /data_source` are readable
  without a token — existing REST behavior, not something the bridge adds.
  Deployments whose table names or column semantics are sensitive should
  weigh access to `/data_source` and `/pipelines` together.
- **`/mcp`** — once auth is on, the Bearer token is required for every
  request, tool inventory included, and cookies are never accepted; see
  [Remote (streamable HTTP)](#remote-streamable-http). The REST inventory
  exception above does not extend here.

---

## Timeouts & lifecycle

Stdio bridge:

- **No bridge-side request timeout.** A hung server hangs the tool call
  until the MCP host's own tool-call timeout fires — every mainstream host
  has one, and stacking a second timeout under it would only create
  ambiguity about which fired.
- **Concurrent tool calls are served in parallel.** A slow `query` does not
  block `tools/list` or other calls.
- **Lifecycle is host-driven.** When the host closes stdin, the bridge exits
  `0`; in-flight REST calls die with the process.

`/mcp`:

- **Per-request lifecycle — there is no process to exit.** Each JSON-RPC
  request is one HTTP exchange; a host that disconnects simply stops
  sending requests.
- **Disconnect semantics follow the SSE response mode.** A tool call's
  response is an SSE stream with a 15 s keep-alive; a client that drops the
  connection abandons that response.
- **The host's tool-call timeout remains the backstop.** The endpoint adds
  no timeout of its own; the keep-alive exists so reverse proxies don't
  fire theirs first (see the reverse-proxy notes above).

Execution failures are reported in-band on both transports: a 4xx/5xx from
the REST handlers becomes a tool result with `isError: true` carrying the
error text, for the model to see and react to. On the bridge, an
unreachable server surfaces the same way (the "cannot reach skardi-server"
wording the CLI prints), and a `tools/list` that can't reach the server is
the one JSON-RPC-level error, since there is no tool result to attach it
to; in-process dispatch on `/mcp` has no unreachable-server case, and its
one JSON-RPC-level tool-call error is an unknown tool name (with a nudge to
re-issue `tools/list`).

---

## Troubleshooting

- **`cannot reach skardi-server at <url>`** — the bridge can't connect.
  Check the three config knobs in resolution order: `--server` in the host's
  `args`, `SKARDI_SERVER_URL` in the host's environment, and `server` in
  `~/.skardi/config.yaml`. Remember the host spawns the bridge with *its*
  environment, which may not be your shell's.
- **Host reports a broken/failed MCP connection at startup** — stdout is
  the JSON-RPC channel, so anything that prints to it corrupts the framing.
  Don't wrap the `command` in a shell script or launcher that echoes
  (version banners, `set -x`, profile output); point the host directly at
  the `skardi` binary.
- **Tool named differently than the pipeline** — see the sanitization and
  collision rules above; the original pipeline name is always echoed in the
  tool description.
- **Result size** — `query` results are capped by `max_rows` (default
  1000). Pipeline executions are not row-capped server-side; both bindings
  refuse response bodies over 256 MB (the CLI-wide client ceiling, restated
  by the `/mcp` handler).
