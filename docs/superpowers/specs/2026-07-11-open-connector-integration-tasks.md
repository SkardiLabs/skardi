# Tasks: Open Connector Integration

Design spec: [2026-07-11-open-connector-integration-design.md](./2026-07-11-open-connector-integration-design.md)

This file tracks the implementation of the Open Connector integration as a
series of independently reviewable milestones. Each milestone is one PR.
The integration cannot land in a single PR; this document is the map that
lets reviewers see where any given PR fits in the whole.

Legend: `[x]` done and merged/in PR · `[~]` in progress · `[ ]` not started

---

## Milestone 1 — Typed-config foundation (PR: feature/open-connector-foundation)

The config layer every later piece builds on. No network I/O, nothing
registered.

- [x] 1.1 `DataSourceType::OpenConnector` with explicit serde rename (`open_connector`)
- [x] 1.2 `OpenConnectorConfig` / `OpenConnectorBinding` matching the design-spec YAML (`runtime_token_env`, timeouts, `max_pages`/`max_rows`, `cache_max_bytes`, `cache_ttl_seconds`, `raw_action_allowlist`, `bindings`), with `deny_unknown_fields` so a misspelled key (e.g. `source_pack_versions`) fails loudly instead of silently disabling the pin
- [x] 1.3 Optional `source_pack_version` pin on bindings (schema stability across Skardi upgrades)
- [x] 1.4 `OpenConnectorError` with pre-network validation variants
- [x] 1.5 `OpenConnectorConfig::validate()` — pure, shared by server validation and provider registration (CLI/server parity); server `validate_data_sources` also rejects non-catalog hierarchy at config load (`OpenConnectorHierarchyRequired`) so a minimal config fails cleanly instead of aborting boot with a wrapped provider error
- [x] 1.6 Server wiring: `DataSource.open_connector` (required for that type, rejected elsewhere), `validate_data_sources`, registration dispatch, `/data_source` + dashboard type mapping; `OpenConnector` is in `CATALOG_SUPPORTED_SOURCES` so the catalog-mode guards (no `table`/`schema` options, no empty `allowed_schemas`) fire for it exactly as for postgres/dynamodb
- [x] 1.7 CLI wiring: same typed field and registration arm
- [x] 1.8 Read-only by construction, enforced at the single shared point: `register_open_connector_tables` takes `read_write` and rejects it (`ReadWriteNotSupported`), so server **and** CLI apply the invariant identically (server additionally keeps its typed `UnsupportedWriteMode` at config validation); job destinations rejected as non-transactional
- [x] 1.9 `register_open_connector_tables` entry point (validate → fail `ExecutionNotImplemented`)

**Verification**: 19 skardi + 7 skardi-server + 4 skardi-cli tests; all failure
modes asserted to fire before any network call.

## Milestone 2 — HTTP client + action registry (PR: feature/open-connector-foundation)

Everything network-facing, behind one client; planning-time metadata in memory.

- [x] 2.1 `OpenConnectorClient`: health / discover / execute against the `/v1` contract, endpoint paths centralized in private constants; action IDs validated at the client boundary (`InvalidActionId` — bare `.`/`..` and `/` rejected before any request, so IDs can't escape `/v1/actions/` through `Url::join` dot-segment resolution)
- [x] 2.2 Runtime token from env var as Bearer header, trimmed and validated at client construction (`InvalidRuntimeToken` for control characters / empty-after-trim — the `export TOKEN="$(cat token.txt)"` newline case fails fast instead of three retried "builder error"s; unbuildable requests are terminal via `RequestBuildFailed`); `http(s)` URLs only, with embedded credentials **and** query/fragment rejected (`GatewayUrlWithQueryOrFragment` — a `?token=…` query would leak into logs, `Debug`, and the data-sources API); token excluded from `Debug`
- [x] 2.3 Bounded retries split by idempotency: GET health/discovery retry 429 / transient 5xx / transport errors (exponential backoff + jitter, `Retry-After` honored and capped); POST execute retries **only** a pre-execution 429 — 5xx is terminal and transport failure raises `NonIdempotentAmbiguousFailure`, so a possibly-executed action is never re-sent
- [x] 2.4 Bounded response decoding enforced on declared `Content-Length` + streamed bytes; per-request timeout from config; `max_response_bytes` / `max_attempts` are operator-tunable `OpenConnectorConfig` fields (wired in `from_config`, zero rejected as `ZeroSafetyBound`); terminal error paths read a 4 KiB snippet instead of buffering a worst-case error page; execute serializes a borrowing envelope (no input deep-clone)
- [x] 2.5 Connection-alias header on execute calls; `execute()` is `pub(crate)` so the registry/UDTF allowlist gating is structurally un-bypassable from outside the crate (discovery and health stay public metadata); the execute envelope is strict — an object without `output` is `InvalidGatewayResponse` (error/async envelopes never flow downstream as action output), a non-object body is returned whole, and `map.remove` avoids cloning the envelope
- [x] 2.6 `ActionRegistry`: deduplicated, concurrency-bounded discovery of `raw_action_allowlist`; non-locally-executable actions rejected; missing executability flag rejected as `ActionExecutabilityUnknown` (default-deny, `Option<bool>` so "not declared" is never read as "executable"); no partial registry
- [x] 2.7 Compatibility fingerprint per action (canonicalized output schema → collision-resistant BLAKE3)
- [x] 2.8 Initial registration flow: validate → client → health → registry; milestone 3 replaces the temporary `ExecutionNotImplemented` result with catalog construction
- [x] 2.9 `reqwest` becomes a hard dependency of `skardi`; `remote-embed`/`llm-extract` features only gate UDF code
- [x] 2.10 Hand-rolled mock gateway (`testutil.rs`) — no mock-HTTP crate added

**Verification**: 43 skardi tests (client retry/terminal/bounding paths, registry
dedup/reject/fingerprint, registration stage ordering); local integration run
against a Python stub gateway (healthy path reaches `ExecutionNotImplemented`
after health + discovery; down gateway exhausts retries; missing token, invalid
config, unknown action all fail with targeted errors).

## Milestone 3 — Source packs + scan engine (done, same branch)

The relational core: stable table definitions → Arrow RecordBatches.

- [x] 3.1 `source_pack.rs` / `source_pack_registry.rs`: built-in `SourcePackTable` definitions (stable ID, action ID, row path, Arrow schema, pagination strategy, filter mappings, resource requirements, safety bounds, expected fingerprint), version pins enforced at binding time
- [x] 3.2 `row_path.rs`: JSON row extraction (`$.a.b` object-key paths) with page-scoped errors (`RowPathNotFound` / `RowPathNotArray` carry page + segment, never values)
- [x] 3.3 `json_to_arrow.rs`: fixed-schema conversion — missing required keys/type mismatches fail with (column, page, row, expected, found-kind); nullable → null; RFC 3339/epoch timestamps; `List<Utf8>`; opaque JSON fallback; extra upstream fields ignored
- [x] 3.4 `pagination.rs`: typed strategies (page-number with short/empty-page termination, cursor with `PaginationLoop` repeated-cursor detection); offset/next-link/has-more slot into the same enum when packs need them
- [x] 3.5 `filters.rs`: allowlisted Exact/Unsupported translation — **one operator per `FilterMapping`** by construction (a single `(input_field, literal)` pair can only faithfully represent one operator; `>=` is deliberately *not* mapped to the mock's strictly-greater `min_value`, so the boundary row can never be silently dropped), literal-side normalization, scalar→JSON conversion; `Inexact` reserved for conservative mappings
- [x] 3.6 `cache.rs`: bounded TTL `ScanCache` (canonical keys via shared `util::json::canonical_json`, LRU + byte budget + entry cap; `cache_ttl_seconds: 0` = live reads). Key includes gateway, alias, action, pack version, resource, translated filters, projection, **LIMIT**, and Arrow-schema fingerprint. Documented boundary: completed scans only — overlapping scans (self-join sides) are not deduplicated
- [x] 3.7 `exec.rs`: `OpenConnectorExec` — sequential pages via `try_unfold` (drop = cancel), per-page conversion, LIMIT early stop/truncation, `ScanBoundsExceeded` on max_pages/max_rows (incomplete ≠ success), `ScanTimeout`
- [x] 3.8 `table.rs`: `OpenConnectorTableProvider` — read-only (`TableType::Base`, no `insert_into`), `supports_filters_pushdown` shares the scan's allowlist
- [x] 3.9 Synthetic **mock source pack** (`packs/mock.rs`): `mock.items` with page-number pagination (per_page=2), one Exact filter (`value >` → `min_value`), `workspace` resource
- [x] 3.10 Registration builds the real catalog: bindings → pack resolution → discovery (allowlist + pack actions) → fingerprint gate → `MemoryCatalogProvider` (`<gateway>.<binding>.<table>`); `ExecutionNotImplemented` is gone — the catalog is queryable

**Verification**: 110 open_connector tests (all prior suites plus: multi-page
scan through SQL, Exact filter pushdown verified in gateway request bodies,
LIMIT early stop at one live page, cache replay with zero new requests,
self-join identical-key/concurrent-fetch documentation test, zero-bound and
traversal config rejections). CLI integration against a local Python stub
gateway: full scan pagination, `min_value` pushdown, LIMIT, federated JOIN
with a local CSV — all confirmed end to end.

## Milestone 4 — UDTFs + security/observability (follow-up PR)

The interactive SQL surface for the mock pack. **No SQL DDL**: the approved
design registers stable tables exclusively through context YAML
("registration is a configuration action, not a SQL action"), keeping the
SQL validator's no-DDL invariant and shared-`SessionContext` semantics
intact. `CREATE EXTERNAL TABLE ... STORED AS OPEN_CONNECTOR` is a
documented future extension only, gated on a DDL authorization design —
do not reintroduce it here.

- [x] 4.1 `open_connector_query` UDTF (built-in pack definitions only): compiles into the same
      provider/scan/cache path as the YAML-bound table (identical schema, filter allowlist,
      fingerprint gate, shared per-gateway cache); plans against registration-time discovery,
      so an undiscovered action is a targeted planning error, never a hidden gateway call
- [x] 4.2 `open_connector_scan` UDTF (allowlisted raw actions only): deterministic row type
      derived from the discovered output schema at the row path (primitives typed,
      `["T","null"]` unions nullable, everything else opaque JSON) or a planning error
      recommending a source pack; single-page live execution (`PaginationStrategy::SinglePage`,
      no cache, no filter pushdown)
- [x] 4.3 Security policy enforcement: raw actions require allowlist membership **and** an
      explicit `read_only: true` in discovered metadata (mutating and unclassified actions
      rejected at planning, pre-HTTP, with distinct errors); YAML overrides of pack
      action/row_path/pagination/columns rejected by `deny_unknown_fields` (tests pin it);
      default-deny allowlist unchanged
- [x] 4.4 Observability: scan completion/failure tracing events (gateway, binding, table,
      action, cache hit, pages, rows, duration); completion events carry identity and
      counters only, failure events add the error — whose message may quote a bounded
      (≤512-char) snippet of the gateway's *error* response and a pagination cursor for
      diagnosability, per the design's "no tokens / credentials / authorization headers /
      full sensitive inputs" wording — never tokens, successful-response bodies, or row
      data; client retry warns already carried operation + status
- [x] 4.5 Docs: `docs/open-connector.md` (config reference, three SQL interfaces, security
      model, caching, bounds, observability), ctx/UDTF examples inside it, README
      supported-sources entry (first time the source is actually queryable)

**Verification**: 157 open_connector tests. `open_connector_query` asserted to return the
same schema and values as `saas.ws.items`, replay from the table's cache entry with zero
new gateway requests, and push the same `min_value` filter and connection alias;
`open_connector_scan` asserted to execute exactly one POST, expose derived typed/JSON
columns, and reject unallowlisted, mutating, unclassified, and schema-indeterminate
actions before any HTTP execute; federated join of the mock pack (via the UDTF) against a
local CSV. Scan-completion events are emitted with the final batch (LIMIT-satisfied,
short-final-page exhaustion, and cache-replay scans included), since a satisfied
downstream LIMIT drops the stream without another poll; a test-only tracing capture
(`testutil::capture_events`) pins the emitted events themselves — exactly one
completion per scan (LIMIT-terminated stream dropped without a further poll, empty
scan, cache replay) with the documented field values, and exactly one WARN failure
event carrying the scan identity and error.

## Milestone 5+ — Real source packs (one PR each, per design rollout)

- [x] 5.1 GitHub pack (API-key auth, page-number pagination): repositories, issues, issue
      comments, pull requests, reviews, commits, workflow runs, releases — all 8 as stable
      table definitions (`packs/github.rs`, per_page 100). Engine additions the pack
      required, all sanctioned by the design spec: per-mapping `Fidelity` (issues
      `updated_at >=` → `since` pushes **Inexact** and DataFusion re-applies it — verified
      against a gateway that ignores `since` entirely; commits' strictly-after `since` is
      deliberately NOT mapped since a dropped boundary row is unrecoverable), RFC 3339
      rendering of timestamp filter literals, `Utf8ListFromObjectKey` for the design's
      `$.labels[*].name` / `$.assignees[*].login` flattening, a JSON-null *parent* on a
      nested path is absence → SQL NULL for nullable columns (GitHub `commit.author:
      null` / `issue.user: null`), and `SourcePackTable::fixed_inputs` pinning `state=all`
      on issues/pull_requests so `SELECT *` reads the complete collection while a pushed
      `state` predicate overrides the pin (GitHub defaults to open-only). `issues` exposes
      the `pull_request` marker as nullable opaque JSON (`IS NULL` → pure issues, matching
      GitHub's issues-endpoint-returns-PRs behavior). Redacted per-table fixtures
      (`packs/fixtures/github/`) are the build-time conversion contract (null-bearing,
      null-parent, empty-list, nested, extra-field rows); fingerprint pins stay `None`
      like the mock pack until validated against a live gateway's discovered contracts.
      Docs: `docs/open-connector-github.md` (per-table filter/limit behavior, authz/
      visibility incl. the PR caveat, rate limits, freshness), README row updated.
      Verification: 22 pack tests (8 fixture contract suites incl. empty pages, bind-time
      validation of all 8 contracts, and end-to-end via mock gateway: 150-row two-page
      scan with the `state=all` pin on every request, pushed `state` override (Inexact — faithful only inside the provider's enum domain), Inexact
      `since` narrowing + local re-filter keeping the boundary row, `pull_request IS
      NULL`, LIMIT stopping after one page, `open_connector_query` parity) plus new
      filters/json_to_arrow engine tests. Runnable local demo (`docs/open-connector/`,
      in the db-source demo style): bundled stdlib-Python stub gateway standing in for
      the remote service the way DynamoDB Local does, committed ctx + four pipelines
      (stable table with pushdown, both UDTFs, federated CSV join) — every README
      command and output executed against the real server before being written down;
      a final section documents the real-gateway path and flags it as pending live
      validation (same caveat as the fingerprint pins).
- [x] 5.2 Slack pack (OAuth bot token, cursor pagination): conversations (channels), users,
      and files, per the design's Slack caveat — message/thread tables stay gated on upstream
      complete message-cursor handling and are explicitly documented as absent. The wire
      contract is Open Connector's normalized one, reconciled against a live gateway
      (v1.3.1) and the OC provider source: camelCase rows (`channelId`, `realName`, …),
      row arrays under `conversations`/`users`, top-level `nextCursor` (null at end), and
      Slack's in-band `ok:false` consumed by the executor (so the tables declare no
      `error_path`; the engine mechanism is modeled by the mock pack). Cursor pagination
      (`cursor` / `$.nextCursor`, `limit` 200) terminates on both end-of-collection
      spellings (null cursor and absent key) and fails as `PaginationLoop` on a
      non-advancing gateway; `files` uses Slack's classic `page`/`count` pagination
      terminated by the envelope's authoritative `paging.pages` (a `total_pages_path`
      extension to `PageNumber` — short non-final pages, legal under permission filtering,
      never truncate; missing/non-numeric totals fail loudly).
      `types: ["public_channel","private_channel"]` pinned on conversations as the schema's
      array (the `state=all` move, via the new `FixedValue::StrList`); `includeLocale`
      pinned on users so the declared `locale` column is populated; `files.user_id =` →
      `userId` pushed Inexact per the string-push rule; **no time filter is pushed** — the
      OC `list_files` contract declares no `ts_from` input and its strict schema would 400
      one, so `created` predicates run in DataFusion (the engine's per-mapping
      `ValueFormat` stays for future packs). Engine support:
      `FieldType::TimestampSecondsUtc` (Slack's epoch-second `files.created` — the millis
      reader would silently produce 1970 dates); `files` optionally scopes to one channel
      via the `channelId` optional resource. No fingerprint pins yet, same follow-up as
      GitHub. Live-verified: all three tables' generated inputs pass the gateway's strict
      action schemas (requests reach the credential wall, not `invalid_input`).

      **Verification**: 224 open_connector tests (counted by `cargo test -p skardi --lib
      sources::providers::open_connector`): per-table fixture contract tests against the
      normalized shapes (explicit nulls vs omitted `memberCount`, flattened profiles,
      deleted users, Slack's empty-string convention, epoch-seconds `files.created`, empty
      pages); e2e via mock gateway speaking the real envelope — multi-page cursor scan (no
      cursor on page 1, token afterwards, `limit` hint + `types` array pin on every
      request), both termination spellings, pagination-loop detection bounded at the first
      repeated cursor, LIMIT early stop, empty workspace, `userId` pushed and re-applied
      against an ignoring provider, the negative-space guard that no time key ever reaches
      the wire, gateway-failure surfacing of Slack's `ok:false`, multi-table binding with
      zero required resources, UDTF parity for `slack.users`.
- [ ] 5.3 Notion pack (explicit data-source binding, cursor pagination, dynamic properties with binding-time schema freeze): rows, pages, blocks, users
- [ ] 5.4 Later waves per the design rollout (Google Workspace, Discord, Feishu, HubSpot, Jira, …) through the source-pack admission gate

**Gate for each pack** (from the design spec): complete terminating pagination,
deterministic schema, read-only allowlist, documented authz/rate limits,
bounded safety defaults, null/empty/nested fixtures, docs.

---

## Review notes

- **Current PR**: milestone 4 (UDTFs + security/observability + docs).
  Milestones 1–3 are merged; registration builds a queryable catalog after
  gateway health, action discovery, source-pack validation, and fingerprint
  checks, and now also publishes per-gateway planning state for the two
  UDTFs (shared with the server `OptimizerRegistry` / CLI the way the
  KNN/FTS `DatasetRegistry` is).
- **Invariants to hold in review**: no provider credentials in Skardi;
  read-only until explicitly designed otherwise; pure validation shared by
  CLI and server; no network I/O at query-planning time; no `.unwrap()` in
  production paths.
