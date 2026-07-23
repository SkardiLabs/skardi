# skardi CLI Reframe: Thin HTTP Client

**Date:** 2026-07-23
**Status:** Approved design, pending implementation plan

## Goal

Rewrite `crates/cli` as a pure HTTP client for skardi-server. All query and
pipeline execution moves to the server; the CLI's job is to build requests,
send them, and render responses. The local DataFusion engine, the alias
system, and every embedding/rag feature flag are removed.

## Motivation

skardi-server now serves ad-hoc SQL (`POST /query`) and named pipelines
(`POST /{name}/execute`). Duplicating the execution engine inside the CLI
means duplicated data-source registration, feature-flag plumbing, and a slow
build that compiles the entire engine crate. The `skardi job` command group
already proves the thin-client model.

Aliases existed to give short verbs to parameterized pipeline invocations.
Server-side pipelines already are named, parameterized invocations, so an
alias is now just `skardi run <pipeline-name>`.

## Non-goals

- No offline/local execution mode of any kind (no feature flag, no fallback).
  Users who need local querying can run a local skardi-server.
- No backward compatibility for removed commands/flags. Existing
  `aliases.yaml` files are ignored. No tombstone/migration stubs.
- No changes to skardi-server. The CLI targets the API as it exists today.
- No Arrow/streaming transport. The server speaks JSON; so does the CLI.

## Command surface

```
skardi query  (-e <SQL> | -f <FILE>) [--max-rows N] [--table]     → POST /query
skardi run    <name> [-p NAME=VALUE ...] [--table]                → POST /{name}/execute
skardi pipeline list                                              → GET /pipelines
skardi pipeline show <name>                                       → GET /pipeline/{name}
skardi schema                                                     → GET /data_source
skardi health [name]                                              → GET /health | /health/{name}
skardi job    run|status|list|cancel|show                         → jobs API (unchanged behavior)
```

Global flags on every command: `--server <URL>`, `--token <TOKEN>`.

### query

- `-e/--sql <SQL>` or `-f/--file <PATH>`; file takes precedence when both are
  given (matches current behavior). One of the two is required.
- `--max-rows N` passes through as the request's `max_rows`; omitted means
  the server default (1000) applies.
- When the response has `truncated: true`, print a notice to **stderr**
  (stdout stays clean for piping).

### run

- `skardi run <name> -p user_id=1 -p category=premium` sends a flat JSON
  object `{"user_id": 1, "category": "premium"}` to `POST /{name}/execute`.
- Param values are parsed as JSON first (numbers, booleans, arrays, null),
  falling back to string. This matters because the server substitutes typed
  literals into pipeline SQL.

### Removed

- `alias` command group (`alias.rs`, `alias_store.rs`) and the
  external-subcommand fallback that resolved bare verbs as aliases.
- `query --ctx`, `--semantics`, `--schema`, `--all`, `-t/--table <TABLE>`
  (the old schema-selection flag; `--table` now means output format).
- `run` on local YAML paths and `--pipeline-dir`.
- All cargo features: `candle`, `gguf`, `onnx`, `remote-embed`, `chunking`,
  `embedding`, `rag`.

## Architecture

```
crates/cli/src/
  main.rs        — clap definitions (Cli, Commands), tokio main, dispatch only
  config.rs      — ClientConfig resolution (see Configuration)
  client.rs      — ApiClient: reqwest::Client + base URL + optional token;
                   get/post helpers, Bearer header injection, error mapping
  output.rs      — print_rows(rows, table_mode), truncation notice, table renderer
  commands/
    query.rs     — build {sql, max_rows}, POST /query
    run.rs       — parse -p params to flat JSON map, POST /{name}/execute
    pipeline.rs  — list / show
    schema.rs    — GET /data_source
    health.rs    — GET /health[/name]
    jobs.rs      — existing jobs_cli.rs moved here, rewired onto ApiClient
```

Data flow, identical for every command: clap parses → `ClientConfig::resolve()`
→ `ApiClient` makes one request → success body to `output.rs`; error body
mapped to a human message on stderr with non-zero exit.

### Dependencies

Remaining: `clap`, `reqwest` (json, rustls-tls), `tokio`, `serde`,
`serde_json`, `serde_yaml`, `anyhow`, `dirs`.
Dev: `wiremock` (async-native mock server, fits the reqwest/tokio stack), `tempfile`.

Removed: `datafusion`, `datafusion-catalog`, `datafusion-session`, `arrow`,
`lance`, `object_store`, `url`, `async-trait`, and the `skardi` path
dependency. `crates/cli` no longer depends on the engine crate at all;
`crates/server` becomes the engine's only workspace consumer.

### Deleted code

`alias.rs`, `alias_store.rs`, `pipeline.rs` (local YAML executor),
`tests/influxdb_cli.rs`, and all session-context / register-source /
schema-walk / UDF-registration code in `main.rs` (~2,500 lines plus their
tests).

## Output format

- **Default: raw JSON** — the server's `data` array printed to stdout
  (scripting-first, pipes into `jq`).
- `--table`: aligned ASCII table rendered from the flat JSON rows.
  Hand-rolled column-width alignment (~50 lines), no table crate. Nulls
  render as empty cells; nested values render as compact JSON.
- `pipeline list/show`, `schema`, `health`, and `job` subcommands print
  pretty JSON (job keeps its existing custom `list` layout).

## Configuration

File: `~/.skardi/config.yaml`, in the repo's manifest style:

```yaml
kind: client
metadata:
  name: default
spec:
  server: http://127.0.0.1:8080
  token: <optional bearer token>
```

Per-field precedence: flag (`--server`/`--token`) > env (`SKARDI_SERVER_URL`
/ `SKARDI_API_TOKEN`) > file > default (`http://127.0.0.1:8080`, no token).

A missing file is fine (defaults apply). A present-but-malformed file prints
a stderr warning and falls through to defaults — never a silent ignore.

When a token is resolved, every request carries `Authorization: Bearer <token>`.

## Error handling

The server returns a uniform envelope `{error, error_type, timestamp}`.

| Outcome | Behavior | Exit |
|---|---|---|
| Connection refused / timeout | `error: cannot reach skardi-server at <url> (…)` + hint to check `--server`/config | 2 |
| 401 | `unauthorized — set SKARDI_API_TOKEN or 'token' in ~/.skardi/config.yaml` | 1 |
| 404 from `run` | `pipeline '<name>' not found — try 'skardi pipeline list'` | 1 |
| Other 4xx/5xx with envelope | `error [{error_type}]: {error}` | 1 |
| Non-JSON error body | `error: server returned <status>` + first line of body | 1 |

Code style: `Result` + `anyhow::Context` throughout; no `.unwrap()` outside
`#[cfg(test)]`, even though `crates/cli` is formally exempt — this is a
fresh rewrite and holds to the stricter rule.

## Testing

1. **Unit** (in-module `#[cfg(test)]`): config precedence, `-p` param
   JSON-typing, table rendering, error-envelope mapping.
2. **Integration** (mock HTTP server via `wiremock` dev-dep):
   per-command request shape (path, body, headers incl. Bearer) and response
   handling (success, truncated notice, 404, 401, connection refused).
3. **E2E smoke** (`#[ignore]`): one test against a real local skardi-server
   for manual verification.

## Decisions log

| Decision | Choice |
|---|---|
| Local engine | Removed entirely; no feature flag, no fallback |
| Pipeline invocation | `skardi run <name>` only; no bare-verb fallback |
| Extra commands | `pipeline list/show`, `schema`, `health` all included |
| Connection config | Global flags + env + `~/.skardi/config.yaml`, flag > env > file > default |
| Output | JSON default, `--table` opt-in |
| Migration | Clean break; no tombstones, aliases.yaml ignored |
| Structure | Modular rewrite in place (approach A), async reqwest |
