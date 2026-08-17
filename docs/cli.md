# Skardi CLI

`skardi` is a thin HTTP client for `skardi-server`: every command builds one
HTTP request, sends it to a running server, and renders the response. It
holds no local query engine, no catalog, and no storage access of its own —
all of that lives in the server. Start a server first (see
[docs/server.md](server.md)), point the CLI at it, and every command below
becomes available.

## Install

From the repo root:

```bash
cargo install --locked --path crates/cli
```

Then run `skardi` from anywhere.

> `--locked` tells cargo to respect the checked-in `Cargo.lock` instead of
> re-resolving transitive dependencies. Without it, cargo may pull a newer
> version of a transitive crate whose MSRV is higher than yours. If that
> happens, add `--locked` or upgrade your toolchain.

## Run without installing

From the repo root:

```bash
cargo run -p skardi-cli -- <command> [options]
```

## Connecting

Every command accepts two global flags, `--server <URL>` and `--token
<TOKEN>`. Each is resolved independently from four sources, in this
precedence order (highest wins):

| Precedence | Server URL | API token |
|---|---|---|
| 1 | `--server <URL>` flag | `--token <TOKEN>` flag |
| 2 | `SKARDI_SERVER_URL` env var | `SKARDI_API_TOKEN` env var |
| 3 | `server:` in `~/.skardi/config.yaml` | `token:` in `~/.skardi/config.yaml` |
| 4 | `http://127.0.0.1:8080` (default) | none |

`~/.skardi/config.yaml` is a client manifest in the repo's manifest style:

```yaml
kind: client
metadata:
  name: default
spec:
  server: http://127.0.0.1:8080
  token: <optional bearer token>
```

Both `spec.server` and `spec.token` are optional. A missing file is fine
(defaults apply); a present-but-malformed file prints a `warning:` to
stderr and falls back to defaults — never a silent ignore.

When a token is resolved (from any source), every request carries
`Authorization: Bearer <token>`.

## Ad-hoc SQL — `query`

`skardi query` sends one SQL statement to `POST /query` on the server and
prints the result.

```bash
# Inline SQL
skardi query -e "SELECT * FROM products LIMIT 10"

# SQL from a file (-f wins over -e when both are given)
skardi query -f ./queries/report.sql

# Cap the number of returned rows (server default: 1000)
skardi query -e "SELECT * FROM events" --max-rows 50

# Render as an ASCII table instead of JSON
skardi query -e "SELECT * FROM products LIMIT 10" --table
```

Default output is the response's `data` array, pretty-printed JSON, on
stdout — nothing else — so it pipes cleanly into `jq`:

```bash
skardi query -e "SELECT id, price FROM products WHERE price > 100" \
  | jq '.[] | .id'
```

`products` (and any other table referenced) must already be registered as
a data source on the server you're talking to — see the server's `--ctx`
flag in [docs/server.md](server.md). The CLI does not register sources
itself.

When the server truncates the result set (more rows existed than
`max_rows`), a notice is printed to **stderr** — stdout stays clean for
piping:

```
note: results truncated; pass a higher --max-rows to see the rest
```

## Pipelines

What used to be CLI aliases are now just pipeline names: a pipeline is a
named, parameterized SQL template registered on the server (see
[docs/pipelines.md](pipelines.md)), and `skardi run <name>` calls it
directly — there is no separate alias layer to define or maintain.

```bash
# List every pipeline the server knows about
skardi pipeline list

# Show one pipeline's definition (SQL, inferred params, etc.)
skardi pipeline show daily_report
```

`skardi run <name>` sends `POST /{name}/execute` with a JSON body built
from `-p`/`--param` and/or `-d`/`--data`:

```bash
# Named parameters, one flag per key
skardi run daily_report -p user_id=1 -p category=premium

# Whole body inline as JSON
skardi run daily_report -d '{"user_id": 1, "category": "premium"}'

# Body from a file, with a -p override on top
skardi run daily_report -d @params.json -p category=premium

# Body piped in from stdin
echo '{"user_id": 1}' | skardi run daily_report -d -

# Render as a table
skardi run daily_report -p user_id=1 --table
```

`-d` and `-p` compose: the JSON object from `-d` (inline, `@file`, or `-`
for stdin) is the base, then each `-p NAME=VALUE` sets or overrides that
key. With neither flag, the body is `{}`.

`-p` values are **JSON-first typed**: `-p user_id=1` sends the number `1`,
not the string `"1"`; `-p active=true` sends a boolean; `-p tags=[1,2]`
sends an array. A value only falls back to a plain string when it isn't
valid JSON on its own (e.g. `-p name=hello`). This matters because the
server substitutes typed literals into the pipeline's SQL.

`--session-id <ID>` — sent as `X-Skardi-Session-Id`; groups this
execution with an agent session in the server's query audit ledger. The
value is validated client-side before any request is sent (non-empty,
≤ 200 characters, visible ASCII, no commas — the same rules the server
enforces), so a bad value fails fast instead of surfacing as a connection
error. When the server has auditing enabled, a `503` with `error_type
"query_audit_error"` means the pipeline **did not run** and the call is
safe to retry.

Calling a pipeline that doesn't exist on the server returns a friendly
error instead of a raw 404:

```
error: pipeline 'no_such_pipeline' not found — try 'skardi pipeline list'
```

## Discovery and health

```bash
# Show the server's registered data sources and their schemas
skardi schema

# Overall server health
skardi health

# One pipeline's health (includes upstream data-source status)
skardi health daily_report
```

## Jobs

`skardi job` is a thin client over the server's `/jobs/*` endpoints for
async batch execution (see [docs/jobs.md](jobs.md)). All five subcommands:

```bash
# Submit a new run; returns immediately with a run_id
skardi job run nightly_sync -p day=2026-07-23 -p batch=500

# Poll a run's current status
skardi job status <run_id>

# List recent runs, optionally filtered by job name
skardi job list --job nightly_sync --limit 20

# Request cancellation of an in-progress run
skardi job cancel <run_id>

# List every job the server knows about and its destination
skardi job show
```

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Success. |
| `1` | Client or server error: bad flags/JSON, SQL validation error, pipeline not found, unauthorized, or any other 4xx/5xx from the server. |
| `2` | Server unreachable: connection refused, DNS failure, or timeout. Check `--server`, `SKARDI_SERVER_URL`, or `~/.skardi/config.yaml`. |

## Migrating from the old (local-engine) CLI

The CLI no longer embeds a query engine — every command is now an HTTP
call to `skardi-server`. There is no local/offline mode and no feature
flags to build a bigger CLI binary; everything that used to be a CLI
feature flag now lives server-side, if it lives anywhere.

| Old | New |
|---|---|
| `skardi query --ctx <ctx.yaml> -e "SQL"` | Start `skardi-server` with that same `--ctx <ctx.yaml>` (registers the sources once, server-side), then `skardi query -e "SQL"` against it. |
| `skardi query --schema [--all \| -t TABLE]` | `skardi schema` — the server always describes every registered source; there's no per-table flag, the CLI doesn't filter server output. |
| `skardi run <pipeline.yaml> -p ...` (local YAML file) | Load the pipeline on the server (`--pipeline` at server startup), then `skardi run <pipeline-name> -p ...` calls it by name over HTTP. |
| `skardi alias add <verb> --pipeline <name> ...` / bare-verb dispatch (`skardi <verb>`) | Gone — call the pipeline directly: `skardi run <pipeline-name> -p ...`. Named pipelines already are the short, parameterized verb; there's no separate alias file to maintain. |
| CLI builds with `--features embedding` / `--features rag` (candle, gguf, onnx, remote-embed, chunking) | Gone from the CLI entirely. These UDFs live in `skardi-server` (`cargo build -p skardi-server --features rag`) since inference now happens where the engine runs. |
