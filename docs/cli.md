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

Every command accepts three global flags: `--server <URL>`, `--token
<TOKEN>`, and `--context <NAME>`.

`~/.skardi/config.yaml` holds named **contexts** and a pointer at the
current one, the shape `kubectl` uses:

```yaml
kind: client
current-context: local
contexts:
  - name: local
    server: http://127.0.0.1:8080
    # mode defaults to `server`
  - name: acme/prod
    server: https://gateway.skardi.ai
    mode: cloud
    workspace: acme-prod       # required in cloud mode, sent per request
    token: skardi_pat_…
```

Which context a command uses: `--context NAME` > `$SKARDI_CONTEXT` >
`current-context` > the only context, if the file defines exactly one. With
several contexts and no pointer, no context is selected and the flags, env
vars, and default below apply on their own.

A named context that does not exist is an error listing the ones that do —
never a silent fall back to the default server.

### Precedence, per field

`mode: server` contexts (the default, and every pre-cloud install):

| Precedence | Server URL | API token |
|---|---|---|
| 1 | `--server <URL>` flag | `--token <TOKEN>` flag |
| 2 | `SKARDI_SERVER_URL` env var | `SKARDI_API_TOKEN` env var |
| 3 | the context's `server:` | the context's `token:` |
| 4 | `http://127.0.0.1:8080` (default) | none |

`mode: cloud` contexts are **authoritative** for both fields, and the
environment is refused rather than ranked:

| Precedence | Server URL | API token |
|---|---|---|
| 1 | `--server <URL>` flag | `--token <TOKEN>` flag |
| 2 | the context's `server:` | the context's `token:` |
| — | a set `SKARDI_SERVER_URL` is an **error** unless `--server` overrides it | same for `SKARDI_API_TOKEN` / `--token` |

The reason for the asymmetry: a cloud context's server and token are a
matched pair — the token is a workspace-scoped PAT and the gateway that
honours it is the one `login` wrote beside it. A `SKARDI_SERVER_URL` left
exported from the single-server era would otherwise send that credential to
whatever listens there, so the conflict is named instead of resolved
silently. A flag still wins, because passing one is deliberate at the point
of use.

A cloud context is never defaulted to `http://127.0.0.1:8080`: one with no
`server` is an error, for the same reason.

### Managing contexts — `skardi config`

Pure file edits; none of these touch the network.

```bash
# List contexts; the current one is marked with *
skardi config get-contexts

# Print just the current context's name
skardi config current-context

# Switch
skardi config use-context acme/prod

# Create or update one (only the fields you name are touched)
skardi config set-context local --server http://127.0.0.1:8080 --current
skardi config set-context acme/prod --mode cloud --workspace acme-prod \
  --server https://gateway.skardi.ai --token-stdin < token.txt

# --token also works, but puts the credential on the command line, where
# /proc (on Linux) and your shell history can see it. Prefer --token-stdin.

# Remove one. Does not revoke its credential.
skardi config delete-context acme/prod

# Print the file with tokens redacted (--show-tokens prints them in full)
skardi config view
```

The file is written atomically and `0600`, since it holds credentials; a
looser existing file is reported and rewritten tighter. Keys this CLI does
not recognize are preserved, so a newer CLI's fields survive an older one's
edits.

### Older config files

A file with no `contexts:` key but the pre-contexts `spec:` block still
works — it resolves as a single context named `default`, and the first
`skardi config` edit promotes it into `contexts:` without losing its token:

```yaml
kind: client
spec:
  server: http://127.0.0.1:8080
  token: <optional bearer token>
```

A file with both prefers `contexts:` and warns once.

A missing file is fine (defaults apply). A present-but-malformed one prints
a `warning:` naming the line and column, and read-only commands carry on
with flags and env vars — but any command that would *modify* the file
refuses, because rewriting it would discard credentials it may still hold.
Parse complaints never quote the file's contents, so a token on the broken
line is not echoed.

When a token is resolved (from any source), every request carries
`Authorization: Bearer <token>`.

A `mode: cloud` context also sends `Skardi-Workspace: <workspace>` on every
request. The gateway needs the workspace per request to resolve the
credential, and the header sits deliberately outside the reserved
`x-skardi-*` namespace, which the gateway strips from client-supplied
headers before forwarding upstream.

## Signing in to skardi-cloud — `login` / `logout`

`skardi login` turns a browser sign-in into one workspace-scoped token per
workspace and writes a context for each, so nothing is copied by hand:

```bash
# Sign in; a lone workspace is used automatically, several prompt
skardi login --control-plane https://global.skardi.ai \
  --client-id <your-deployment's-oauth-client-id>

# With both pinned in the environment, the flags go away
export SKARDI_CONTROL_PLANE_URL=https://global.skardi.ai
export SKARDI_OAUTH_CLIENT_ID=<client-id>
skardi login

# Non-interactive selection
skardi login --workspace acme-prod
skardi login --all-workspaces

# Print the sign-in URL instead of opening a browser
skardi login --no-browser

# Shorter-lived credential (default: 90d; `12h` also works)
skardi login --expires 30d
```

Two inputs have no built-in default, and both fail by name rather than
guessing: the **control plane** (`--control-plane` >
`$SKARDI_CONTROL_PLANE_URL` > `control-plane:` in the config file) and the
**OAuth client id** (`--client-id` > `$SKARDI_OAUTH_CLIENT_ID`). The client id
is per deployment — it is the same value the deployment gives its console — so
there is nothing correct to hardcode. Once a `login` succeeds, the control
plane is recorded in the config file and later runs need no flag for it.

What it does, in order:

1. Resolves the control plane, as above. With none of the three sources, it
   stops and says so rather than guessing a host. A plain-`http://`
   non-loopback control plane is warned about here, before the browser opens:
   this leg carries the sign-in assertion up and the minted credential back.
2. Opens a browser against the identity provider, with PKCE (S256) and a
   `state` nonce, redirecting to a single-use listener on `127.0.0.1:<random
   port>`. It waits 120 seconds, then gives up and releases the port. A
   response carrying a `state` this run did not issue is answered and ignored
   rather than ending the wait, so nothing else that can reach the port can
   interrupt a sign-in.
3. Exchanges the code for an ID token that is **held in memory only**. No
   refresh token is requested, and nothing the provider returns is written to
   disk.
4. Reads your workspaces from the control plane. Anything not yet `active` is
   listed and skipped, with its state named.
5. Mints one token per selected workspace, scoped to that workspace at your
   role there — never an unscoped credential.
6. Verifies each one with a `select 1` against the gateway it will actually
   use. A credential that cannot answer is **not written**, and it is revoked.
   The probe is bounded (30s), so a gateway that accepts the connection and
   then goes quiet cannot strand a freshly minted token. `--no-verify` skips
   this.
7. Writes one context per verified token, named `<org>/<workspace>`, points
   `current-context` at the first, and prints a summary with no token values
   in it.

### `--no-browser` and remote shells

`--no-browser` prints the URL instead of launching a browser, for a host that
has none or none the CLI can start. It does **not** on its own make `login`
work over SSH: the redirect goes to `127.0.0.1:<port>` on the machine running
`skardi`, so opening that URL on your laptop sends the callback to your
laptop's port, where nothing is listening.

To sign in against a remote host, the browser must reach that host's loopback
port. Either run a browser there, or forward the port the printed URL names —
it is fresh per run, so with OpenSSH add the forward mid-session (`~C` then
`-L <port>:127.0.0.1:<port>`), or use `ssh -L` on a connection opened after
the URL is shown. A headless flow needing no local listener is the device-code
grant, which is deliberately out of scope for this milestone.

For a loopback control plane (a local or compose stack), `--identity` skips the
browser entirely — see [Working against a local stack](#working-against-a-local-stack).

The gateway URL comes from `--server` > `$SKARDI_GATEWAY_URL` > the control
`gateway_url` on that workspace's membership. There is deliberately no built-in default and no
fall back to `http://127.0.0.1:8080`: a context pointing at a local port
would fail later and further from the cause.

Minting is a **saga**. Each token commits independently at the control plane,
so if a later mint fails — or the config write fails — every token this run
created is revoked before the original failure is reported, and no context is
written. If a rollback cannot complete, the surviving token ids are printed
with a non-zero exit, because a live credential nobody knows about is worse
than a loud failure.

Running `login` again over an existing context replaces it and revokes the
token it replaced. `--keep-old-token` retains the old one — for an agent
that is mid-task — and says so.

If your identity belongs to more than one organization, minting is not
available in v1: `login` prints the organizations and the way round it
(mint in the console, then `skardi config set-context … --token-stdin`).

### Signing out

```bash
# Clear the current context's credential (cloud contexts only)
skardi logout

# Clear every cloud context's credential
skardi logout --all

# Also revoke it at the control plane (re-authenticates first)
skardi logout --revoke
```

Plain `logout` is a local edit: the credential leaves this machine but stays
**valid until it expires**, and the output says so. A token cannot revoke
itself, so `--revoke` signs in again to call the control plane. The context
itself (server, workspace, mode) is kept either way, so a later `login`
refills it; removing the context entirely is `skardi config delete-context`.

`logout` only touches **cloud** contexts — the ones `login` can mint again. A
server-mode context's token was configured by hand and nothing can restore it,
so clearing it is refused, with a pointer at `config delete-context` (or
editing the file); `--all` skips those for the same reason.

The local delete happens first, so an unreachable control plane still gets the
credential off the machine. That means the token id — which lives only in the
config file — is gone by the time a revocation can fail, so **every failure
after that point prints the ids it could not revoke**, with the context each
came from, for the console. A context written by `config set-context` has no
recorded id at all, and `--revoke` names it rather than skipping it quietly.

### What a cloud context cannot do

A skardi-cloud gateway serves `query` and `schema`. `run`, `pipeline`, `job`,
and `health` are engine-local surfaces it does not mount, so they fail
immediately, naming the context, with no request issued:

```
error: 'job' is not available in a cloud context (acme/acme-prod). Available: query, schema.
```

Two credential failures are reported in the context's own terms rather than
the transport's: a rejected token points at `skardi login` (not at
`SKARDI_API_TOKEN`, which a cloud context refuses anyway), and a
`token-expires-at` already in the past is reported without spending a round
trip.

### Working against a local stack

A control plane in dev mode accepts an unverified identity claim instead of a
signed sign-in, which is how the flow is tested without a browser:

```bash
skardi login --control-plane http://localhost:18090 --identity dev:alice
```

This is refused unless the control plane is a **loopback** address — not
"loopback or private", because a shared internal staging cluster lives on an
RFC1918 address and a `dev:` bearer there is impersonation. A remote dev box
needs `--i-know-this-is-dev-auth`, so the decision is visible in the command
that made it. Every run prints a warning naming what it authenticated
against.

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

# Record why this query ran, and which session it belongs to
skardi query -e "SELECT count(*) FROM orders WHERE status = 'paid'" \
  --purpose "weekly paid-order count" --session-id sess-2026-08-27
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

### Recording intent — `--purpose` / `--session-id`

The two flags travel together as the request body's `ai_context` object,
which the server records in its query audit ledger when it was started with
`--query-audit-db`. `--purpose` says why the query ran; `--session-id`
groups it with the rest of one agent session, so a later reader can tell a
repeated question from a one-off. Without them the ledger still records the
SQL, but nothing about intent — the column that makes "we have answered this
before" answerable stays empty.

Either flag requires the other: the server rejects an `ai_context` carrying
only one of the pair, so the CLI refuses it at parse time rather than
spending a round trip on a 400. Values are checked client-side before any
request (non-empty, `--purpose` ≤ 2000 characters, `--session-id` ≤ 200).

Unlike `run --session-id`, which travels as an HTTP header and is therefore
held to header-safe characters, these values ride inside JSON — any
non-empty string within the cap is accepted, spaces and non-ASCII included.

The ledger is opt-in and off by default; see `--query-audit-db` in
[docs/server.md](server.md).

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
≤ 200 characters, visible ASCII, no spaces, tabs or commas — byte-for-byte
the rules the server enforces, so nothing can pass here and be rejected or
silently rewritten there), so a bad value fails fast instead of surfacing as
a connection error. When the server has auditing enabled, a `503` with `error_type
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

`--session-id <ID>` — sent as `X-Skardi-Session-Id`; groups this job
submission with an agent session in the server's query audit ledger. The
value is validated client-side before any request is sent (non-empty,
≤ 200 characters, visible ASCII, no commas — the same rules the server
enforces), so a bad value fails fast instead of surfacing as a connection
error. When the server has auditing enabled, a `503` with `error_type
"query_audit_error"` means the job **was not submitted** and the call is
safe to retry.

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
