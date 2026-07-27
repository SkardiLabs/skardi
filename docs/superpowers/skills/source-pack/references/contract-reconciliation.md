# Phase 1 — Live contract reconciliation

The provider's public API docs describe the provider. Skardi talks to
**Open Connector**, whose action contracts rename keys (camelCase),
reject unknown inputs (`additionalProperties: false`), sometimes rebuild
rows entirely, and consume provider error envelopes. Everything below is
about observing the real gateway instead of assuming.

## 1. Start a local gateway

Look for a local checkout of
[oomol-lab/open-connector](https://github.com/oomol-lab/open-connector)
(historically `~/Workspace/open-connector`); clone it if absent. Then:

```bash
cd <open-connector-checkout>
npm install          # once
OOMOL_CONNECT_DATA_DIR=/tmp/oc-data \
OOMOL_CONNECT_RUNTIME_TOKEN=skardi-live-test-token \
PORT=3000 npm run start
```

Health check (the runtime token is required once configured):

```bash
curl -s -H 'Authorization: Bearer skardi-live-test-token' \
  http://localhost:3000/v1/health
```

Run the server in the background and remember to stop it when done.

## 2. Know the real HTTP surface (verified v1.3.1)

These facts cost a full client rewrite to learn; do not regress them:

- Every `/v1` response uses the uniform envelope
  `{"success": bool, "message": str, "data": …, "meta": {…}}` (+
  `errorCode` on failures; `meta.executionId` once execution started).
- Execute is `POST /v1/actions/:actionId` — there is **no** `/execute`
  suffix. Success payload is under `data`.
- Discovery is `GET /v1/actions/:actionId` — `data.inputSchema`,
  `data.outputSchema`, `data.execution.locallyExecutable`,
  `data.execution.noAuthRunnable` (all camelCase, `execution` nested).
- The connection-alias header is `x-oo-connector-alias` (or
  `x-oomol-connector-alias`; `?alias=` also works).
- There is **no read/write classification** in action metadata — the
  raw-scan gate (`open_connector_scan`) refuses by default-deny against
  today's gateway. Pack tables are unaffected (read-only by Skardi's
  review).
- Input validation runs BEFORE credential lookup, which enables the
  no-credential trick below.

Skardi's client (`open_connector/client.rs`) and `testutil.rs` mocks
already speak this protocol; if the live gateway ever disagrees with
them, that is a finding to fix in the client, not to absorb in the pack.

## 3. Probe the provider's actions

```bash
TOK='Authorization: Bearer skardi-live-test-token'
# What exists — never assume an action ID:
curl -s -H "$TOK" "http://localhost:3000/v1/actions?service=<provider>"
# Per-action contract:
curl -s -H "$TOK" "http://localhost:3000/v1/actions/<provider>.<action>"
# Human-readable guide (input examples, scopes, connection identity):
curl -s http://localhost:3000/api/actions/<provider>.<action>/agent.md
```

For each candidate action record: exact ID, `inputSchema` properties +
`required` + `additionalProperties`, `outputSchema` top-level keys (row
array key = your row path), pagination inputs (`perPage`/`page`?
`cursor`/`limit`?), and `execution.requiredAuthTypes`.

## 4. Read the executor source — the row-shape authority

`<open-connector-checkout>/src/providers/<service>/` contains the
executors. This is the only reliable answer to:

- **Passthrough vs normalized rows.** GitHub's executors return the
  provider's raw objects (so fields the declared schema omits — e.g.
  `updated_at` — still arrive, because `additionalProperties: true`).
  Slack's executors REBUILD rows (camelCase keys, flattened profiles,
  renamed row arrays, cursor moved to top-level `nextCursor`). Mapping
  Slack's raw field names would have produced all-NULL columns that no
  provider doc would explain.
- **In-band provider errors.** If the executor raises on the provider's
  in-band error (Slack's `assertSlackPayload` throws on `ok: false`),
  the gateway returns a failure envelope and the pack declares
  `error_path: None`. Only a gateway that FORWARDS in-band errors needs
  `error_path` (the engine checks it before row extraction; the mock
  pack models it).
- **Which inputs actually reach the provider** and under what names —
  the executor's `compactObject({...})` mapping is the ground truth for
  filter pushdown fidelity.
- **Emitted output construction** — where the row array, totals, and
  cursors really live.

## 5. Validate generated inputs without provider credentials

The gateway validates input against the action schema BEFORE touching
credentials, so a wrong key is distinguishable from a missing login:

```bash
# Wrong key → HTTP 400, errorCode invalid_input, names the bad property:
curl -s -X POST -H "$TOK" -H 'content-type: application/json' \
  -d '{"input":{"owner":"a","repo":"b","per_page":5}}' \
  http://localhost:3000/v1/actions/github.list_repository_issues
# Correct keys, no connection → HTTP 403, "Configure … credentials first.":
curl -s -X POST -H "$TOK" -H 'content-type: application/json' \
  -d '{"input":{"owner":"a","repo":"b","perPage":5}}' \
  http://localhost:3000/v1/actions/github.list_repository_issues
```

Reaching the credential wall proves the pack's generated inputs
(pagination params, fixed inputs, filter keys, resource keys) pass the
strict schema. Do this for every table's input set. Row-data validation
additionally needs a configured connection
(`PUT /api/connections/<service>` with `{authType, values}` — ask the
user; never handle their credentials yourself) and provider egress
(note: fake-IP DNS environments block egress at the gateway's SSRF guard;
contract-level validation above needs neither).

## 6. Capture the contracts for fingerprint pinning

For each chosen action, save `data.outputSchema` verbatim as a fixture:

```
crates/skardi/src/sources/providers/open_connector/packs/fixtures/<provider>/contracts/<action_name>.json
```

Phase 3 pins each table's `expected_fingerprint` to the BLAKE3 hash of
the canonicalized schema (computed by
`action_registry::fingerprint_schema` — never re-derive the
canonicalization elsewhere) and locks pin ↔ fixture with a sync test.
Record the gateway version you captured against.
