# MCP Binding — `skardi mcp`

`skardi mcp` serves the [Model Context Protocol](https://modelcontextprotocol.io)
over stdio, making Skardi's pipelines and ad-hoc query surface available to
any MCP host — Claude Desktop, Cursor, or your own agent loop. It is the
third agent-facing binding of the same pipeline YAML, next to the shell verb
(`skardi run`) and REST (`POST /<name>/execute`).

The subcommand is a transport bridge, not a second engine: the host spawns
`skardi mcp` as a long-lived child process and speaks JSON-RPC to it over
stdin/stdout; every tool call is proxied to a running `skardi-server` over
REST and the response is returned verbatim.

```
┌──────────────┐  stdio (JSON-RPC)  ┌────────────────────┐  HTTP (REST)  ┌───────────────┐
│  MCP host     │ ◄───────────────► │  skardi mcp         │ ◄──────────► │  skardi-server │
│ (Claude       │  spawned as a     │  (CLI subcommand:   │  ApiClient,  │  (executes     │
│  Desktop, …)  │  child process    │   MCP ⇄ REST bridge)│  Bearer auth │   SQL)         │
└──────────────┘                    └────────────────────┘               └───────────────┘
```

---

## Host setup

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
parameter value. Multi-row `VALUES {name}` placeholders are typed as
array-of-arrays. Every parameter is required and unknown keys are rejected.

### `query`

Ad-hoc SQL against the federated engine — the MCP face of `POST /query`.

| Argument | Type | Notes |
|---|---|---|
| `sql` | string, **required** | One statement. DML only on `access_mode: read_write` sources; DDL is always rejected. |
| `max_rows` | integer, optional | Result row cap; server default 1000. |
| `purpose` | string, optional | One line on why you are running this query. Sent as `ai_context: {purpose, session_id}` and recorded in the server's query audit log; the `session_id` is one UUID per MCP connection, so a session's queries group together in the ledger. Omitted entirely when not provided. |

### `list_data_sources`

No arguments. Returns the server's data sources with tables, column
schemas, and semantic descriptions — the same body as `GET /data_source`.
Call it before writing ad-hoc SQL with `query`.

---

## Freshness

The pipeline inventory is fetched from `GET /pipelines` on **every**
`tools/list` request — a pipeline added to the server appears as soon as the
host re-lists. Hosts that list once at connect time and never again keep
their snapshot until reconnect. The bridge does not emit `listChanged`
notifications in v1.

---

## Auth notes

The bridge sends the Bearer token it inherits from normal CLI config
resolution on every REST call. Note that on today's server, `GET /pipelines`
(the tool inventory) and `GET /data_source` are readable without a token —
existing REST behavior, not something the bridge adds. Deployments whose
table names or column semantics are sensitive should weigh access to
`/data_source` and `/pipelines` together.

---

## Timeouts & lifecycle

- **No bridge-side request timeout.** A hung server hangs the tool call
  until the MCP host's own tool-call timeout fires — every mainstream host
  has one, and stacking a second timeout under it would only create
  ambiguity about which fired.
- **Concurrent tool calls are served in parallel.** A slow `query` does not
  block `tools/list` or other calls.
- **Lifecycle is host-driven.** When the host closes stdin, the bridge exits
  `0`; in-flight REST calls die with the process.

REST failures during serving are reported in-band, not as crashes: any
failed tool call — a 4xx/5xx from the server or an unreachable server —
becomes a tool result with `isError: true` carrying the error text (the same
"cannot reach skardi-server" wording the CLI prints). A `tools/list` that
can't reach the server is the one JSON-RPC-level error, since there is no
tool result to attach it to.

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
  1000). Pipeline executions are not row-capped server-side; the bridge
  refuses response bodies over 256 MB (the CLI-wide client ceiling).
