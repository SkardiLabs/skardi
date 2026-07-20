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
- [x] 1.8 Read-only by construction: excluded from `WRITABLE_SOURCE_TYPES`; job destinations rejected as non-transactional
- [x] 1.9 `register_open_connector_tables` entry point (validate → fail `ExecutionNotImplemented`)

**Verification**: 19 skardi + 7 skardi-server + 4 skardi-cli tests; all failure
modes asserted to fire before any network call.

## Milestone 2 — HTTP client + action registry (PR: feature/open-connector-foundation)

Everything network-facing, behind one client; planning-time metadata in memory.

- [x] 2.1 `OpenConnectorClient`: health / discover / execute against the `/v1` contract, endpoint paths centralized in private constants; action IDs validated at the client boundary (`InvalidActionId` — bare `.`/`..` and `/` rejected before any request, so IDs can't escape `/v1/actions/` through `Url::join` dot-segment resolution)
- [x] 2.2 Runtime token from env var as Bearer header; `http(s)` URLs only, with embedded credentials **and** query/fragment rejected (`GatewayUrlWithQueryOrFragment` — a `?token=…` query would leak into logs, `Debug`, and the data-sources API); token excluded from `Debug`
- [x] 2.3 Bounded retries split by idempotency: GET health/discovery retry 429 / transient 5xx / transport errors (exponential backoff + jitter, `Retry-After` honored and capped); POST execute retries **only** a pre-execution 429 — 5xx is terminal and transport failure raises `NonIdempotentAmbiguousFailure`, so a possibly-executed action is never re-sent
- [x] 2.4 Bounded response decoding (declared `Content-Length` + streamed bytes), per-request timeout from config
- [x] 2.5 Connection-alias header on execute calls; `execute()` is `pub(crate)` so the registry/UDTF allowlist gating is structurally un-bypassable from outside the crate (discovery and health stay public metadata)
- [x] 2.6 `ActionRegistry`: deduplicated, concurrency-bounded discovery of `raw_action_allowlist`; non-locally-executable actions rejected; missing executability flag rejected as `ActionExecutabilityUnknown` (default-deny, `Option<bool>` so "not declared" is never read as "executable"); no partial registry
- [x] 2.7 Compatibility fingerprint per action (canonicalized output schema → FNV-1a; stable, dependency-free)
- [x] 2.8 Registration flow: validate → client → health → registry → `ExecutionNotImplemented`
- [x] 2.9 `reqwest` becomes a hard dependency of `skardi`; `remote-embed`/`llm-extract` features only gate UDF code
- [x] 2.10 Hand-rolled mock gateway (`testutil.rs`) — no mock-HTTP crate added

**Verification**: 43 skardi tests (client retry/terminal/bounding paths, registry
dedup/reject/fingerprint, registration stage ordering); local integration run
against a Python stub gateway (healthy path reaches `ExecutionNotImplemented`
after health + discovery; down gateway exhausts retries; missing token, invalid
config, unknown action all fail with targeted errors).

## Milestone 3 — Source packs + scan engine (next PR)

The relational core: stable table definitions → Arrow RecordBatches.

- [ ] 3.1 `source_pack.rs` / `source_pack_registry.rs`: built-in `SourcePackTable` definitions (stable ID, action ID, row path, Arrow schema, pagination strategy, filter mappings, resource requirements, safety bounds, expected fingerprint)
- [ ] 3.2 `row_path.rs`: JSON row extraction with action/page/row-path error context
- [ ] 3.3 `json_to_arrow.rs`: fixed-schema conversion (nulls, missing required fields, temporal types, lists/structs, opaque JSON fallback)
- [ ] 3.4 `pagination.rs`: typed strategies (page-number, cursor, offset, next-link, has-more) + repeated-cursor loop detection
- [ ] 3.5 `filters.rs`: allowlisted Exact/Inexact/Unsupported filter translation; limit pushdown (page size + early termination)
- [ ] 3.6 `cache.rs`: bounded in-memory TTL `ScanCache` (canonical keys, LRU, byte budget; `cache_ttl_seconds: 0` = live)
- [ ] 3.7 `exec.rs`: `OpenConnectorExec` physical plan — sequential pages, per-page conversion, LIMIT early stop, cancellation, no partial success
- [ ] 3.8 `table.rs`: `OpenConnectorTableProvider` (read-only `TableProvider` wiring scan → exec)
- [ ] 3.9 Synthetic **mock source pack** (`packs/mock.rs`) proving the abstraction without a real SaaS
- [ ] 3.10 Registration builds catalog from bindings: `<gateway>.<binding>.<table>`; source-pack ↔ action fingerprint compatibility check at bind time

**Verification**: unit tests for conversion/pagination/filters/cache keys;
mock-gateway integration tests (multi-page, empty terminal page, retry,
cancellation, cache hit/TTL, no-partial-success); mock-pack fixtures asserting
Arrow types and values.

## Milestone 4 — UDTFs + security/observability (follow-up PR)

The interactive SQL surface for the mock pack. **No SQL DDL**: the approved
design registers stable tables exclusively through context YAML
("registration is a configuration action, not a SQL action"), keeping the
SQL validator's no-DDL invariant and shared-`SessionContext` semantics
intact. `CREATE EXTERNAL TABLE ... STORED AS OPEN_CONNECTOR` is a
documented future extension only, gated on a DDL authorization design —
do not reintroduce it here.

- [ ] 4.1 `open_connector_query` UDTF (built-in pack definitions only)
- [ ] 4.2 `open_connector_scan` UDTF (allowlisted raw actions only; deterministic row type or planning error)
- [ ] 4.3 Security policy enforcement: mutating actions rejected pre-HTTP; YAML overrides cannot swap actions/row paths; default-deny allowlist
- [ ] 4.4 Observability: scan spans/metrics (gateway, binding, action, cache hit, pages, rows, retries) without tokens or bodies
- [ ] 4.5 Docs: `docs/open-connector.md`, ctx/UDTF examples, README supported-sources entry (first time the source is actually queryable)

**Verification**: both UDTFs return the stable schema and values of their
corresponding YAML-registered tables; federated join of the mock pack
against a local CSV.

## Milestone 5+ — Real source packs (one PR each, per design rollout)

- [ ] 5.1 GitHub pack (API-key auth, page-number pagination): repositories, issues, issue comments, pull requests, reviews, commits, workflow runs, releases
- [ ] 5.2 Slack pack (OAuth bot token, cursor pagination): conversations (channels), users, and files first; complete message/thread tables only after Open Connector provides complete message cursor handling (per the design's Slack caveat)
- [ ] 5.3 Notion pack (explicit data-source binding, cursor pagination, dynamic properties with binding-time schema freeze): rows, pages, blocks, users
- [ ] 5.4 Later waves per the design rollout (Google Workspace, Discord, Feishu, HubSpot, Jira, …) through the source-pack admission gate

**Gate for each pack** (from the design spec): complete terminating pagination,
deterministic schema, read-only allowlist, documented authz/rate limits,
bounded safety defaults, null/empty/nested fixtures, docs.

---

## Review notes

- **Current PR**: milestones 1–2 (`feature/open-connector-foundation`).
  Registration intentionally still fails with `ExecutionNotImplemented`
  after gateway contact succeeds — the switch flips in milestone 3 when
  catalog building replaces the final error.
- **Invariants to hold in review**: no provider credentials in Skardi;
  read-only until explicitly designed otherwise; pure validation shared by
  CLI and server; no network I/O at query-planning time; no `.unwrap()` in
  production paths.
