# Open Connector + GitHub Pack Demo

This demo stands up the `github` source pack end to end on your machine —
stable catalog tables, both UDTFs (`open_connector_query` /
`open_connector_scan`), filter pushdown, and a federated join — with no
GitHub account and no credentials.

It uses a bundled **stub gateway** ([stub_gateway.py](stub_gateway.py), ~200
lines, Python standard library) the same way the DynamoDB demo uses DynamoDB
Local: a local, offline substitute for the remote service. The stub speaks
the gateway's `/v1` contract (health, action discovery, paginated execution
with GitHub-style `state`/`since` filtering) and serves a fictional
`acme/widgets` repository. Everything on the Skardi side — HTTP client,
action discovery, security gates, scan engine, Arrow conversion — runs
exactly as it would against a real deployment; swapping in one is a
one-line config change ([see below](#running-against-a-real-open-connector-gateway)).

For the full configuration and table reference, see
[docs/open-connector.md](../open-connector.md) and
[docs/open-connector-github.md](../open-connector-github.md).

## Quick Start

```bash
# 1. Start the stub gateway (listens on 127.0.0.1:3000)
python3 docs/open-connector/stub_gateway.py &

# 2. The gateway runtime token — the only secret Skardi ever holds.
#    The stub accepts any non-empty value.
export OPEN_CONNECTOR_TOKEN=demo-token

# 3. Start the Skardi server against the demo context + pipelines
cargo run --bin skardi-server -- \
  --ctx docs/open-connector/ctx_github_demo.yaml \
  --pipeline docs/open-connector/pipelines/ \
  --port 8080
```

Startup logs show the whole registration flow: gateway health check, action
discovery, and the catalog + UDTFs coming up:

```text
INFO skardi::sources::providers::open_connector: Open Connector catalog registered gateway=saas bindings=1 actions=2
INFO skardi_server::optimizer_registry: ✓ Registered open_connector_query and open_connector_scan
```

## The four pipelines

### 1. Stable catalog table with filter pushdown

`saas.github_demo.issues` — the binding in
[ctx_github_demo.yaml](ctx_github_demo.yaml) became a schema in the `saas`
catalog. `state = {state}` is pushed into the gateway call as GitHub's
`state` parameter. (The rows are pure issues — the Open Connector action
filters out the pull requests GitHub's raw issues endpoint mixes in.)

```bash
curl -s -X POST http://localhost:8080/open_issues/execute \
  -H 'Content-Type: application/json' -d '{"state":"open"}'
```

```json
{"success":true,"data":[
  {"number":1,"title":"Scan panics on empty page","author_login":"octocat","labels":["bug","p1"],"comments":3},
  {"number":3,"title":"Add dark mode","author_login":"hubot","labels":["enhancement"],"comments":1},
  {"number":4,"title":"Flaky retry test","author_login":"octocat","labels":["bug"],"comments":7}
],"rows":3}
```

### 2. `open_connector_query` — the same table, ad hoc

No binding required: the UDTF resolves the built-in `github.issues`
definition against the gateway registered at startup. Note issue 2's
`author_login: null` — a GitHub "ghost" user converts to SQL NULL.

```bash
curl -s -X POST http://localhost:8080/adhoc_issues/execute \
  -H 'Content-Type: application/json' -d '{}'
```

```json
{"success":true,"data":[
  {"number":1,"title":"Scan panics on empty page","state":"open","author_login":"octocat"},
  {"number":2,"title":"Docs typo in quick start","state":"closed","author_login":null},
  {"number":3,"title":"Add dark mode","state":"open","author_login":"hubot"},
  {"number":4,"title":"Flaky retry test","state":"open","author_login":"octocat"},
  {"number":5,"title":"Bump arrow to 54","state":"closed","author_login":"dependabot"}
],"rows":5}
```

### 3. `open_connector_scan` — allowlisted raw action

Executes `github.list_repository_issues` directly. The action must be in
the context's `raw_action_allowlist` **and** classified read-only (`execution.readOnly`)
by the gateway's discovery metadata; the row type is derived from the
discovered output schema at `$.issues`. Provider-side filters (here
`"state":"open"`) travel in the input JSON.

```bash
curl -s -X POST http://localhost:8080/raw_issue_scan/execute \
  -H 'Content-Type: application/json' -d '{}'
```

```json
{"success":true,"data":[
  {"number":1,"title":"Scan panics on empty page","state":"open"},
  {"number":3,"title":"Add dark mode","state":"open"},
  {"number":4,"title":"Flaky retry test","state":"open"}
],"rows":3}
```

### 4. Federated join with a local CSV

GitHub issues joined against
[sample_data/team_owners.csv](sample_data/team_owners.csv) in one query —
the point of registering SaaS resources as ordinary tables.

```bash
curl -s -X POST http://localhost:8080/issues_by_team/execute \
  -H 'Content-Type: application/json' -d '{}'
```

```json
{"success":true,"data":[{"team":"platform","open_issues":2}],"rows":1}
```

## Seeing the security gates

The default-deny raw-action policy is easy to poke at. Add a pipeline that
scans an action missing from `raw_action_allowlist` (for example
`github.list_pull_requests`, which the stub serves and the binding uses) —
planning fails before any HTTP request:

```text
Open Connector action 'github.list_pull_requests' is not in the
'raw_action_allowlist' of gateway 'saas'; open_connector_scan executes
explicitly allowlisted actions only (default-deny)
```

Killing the stub gateway and restarting the server shows the fail-fast
registration path (health check exhausts its bounded retries); unsetting
`OPEN_CONNECTOR_TOKEN` fails with the targeted missing-token error before
any connection attempt.

## Running against a real Open Connector gateway

> **Status:** the pack's action IDs, input keys, row paths, and the HTTP
> protocol (endpoint paths, response envelope, alias header) have been
> reconciled against a live
> [Open Connector](https://github.com/oomol-lab/open-connector) gateway
> (v1.3.1) and its provider source. Action-contract fingerprint pins are
> the remaining follow-up. Expect `ActionContractMismatch` / conversion
> errors to be the signal if a future live contract differs, never
> silently wrong rows.

The Skardi side is identical; what changes is who answers the HTTP calls:

1. Deploy Open Connector per its own documentation and create a GitHub
   connection there (PAT or OAuth app — the credential lives in the
   gateway, never in Skardi).
2. Obtain a **runtime token** for the gateway and export it as
   `OPEN_CONNECTOR_TOKEN`.
3. In [ctx_github_demo.yaml](ctx_github_demo.yaml), change
   `connection_string` to the gateway's base URL, set your real
   `owner`/`repo` under `resource:`, and — if the gateway hosts several
   GitHub connections — add `connection_alias:` to the binding.
4. Start the server exactly as above.

Rate limits and authorization are enforced by GitHub and the gateway;
Skardi's own bounds (`max_pages`, `max_rows`, timeouts, bounded retries
honoring `Retry-After`) keep every scan finite regardless. See
[docs/open-connector-github.md](../open-connector-github.md) for
per-table filter behavior and visibility caveats.
