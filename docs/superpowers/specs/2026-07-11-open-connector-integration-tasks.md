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
- [x] 1.2 `OpenConnectorConfig` / `OpenConnectorBinding` matching the design-spec YAML (`runtime_token_env`, timeouts, `max_pages`/`max_rows`, `cache_max_bytes`, `cache_ttl_seconds`, `raw_action_allowlist`, `bindings`)
- [x] 1.3 Optional `source_pack_version` pin on bindings (schema stability across Skardi upgrades)
- [x] 1.4 `OpenConnectorError` with pre-network validation variants
- [x] 1.5 `OpenConnectorConfig::validate()` — pure, shared by server validation and provider registration (CLI/server parity)
- [x] 1.6 Server wiring: `DataSource.open_connector` (required for that type, rejected elsewhere), `validate_data_sources`, registration dispatch, `/data_source` + dashboard type mapping
- [x] 1.7 CLI wiring: same typed field and registration arm
- [x] 1.8 Read-only by construction: excluded from `WRITABLE_SOURCE_TYPES`; job destinations rejected as non-transactional
- [x] 1.9 `register_open_connector_tables` entry point (validate → fail `ExecutionNotImplemented`)

**Verification**: 19 skardi + 7 skardi-server + 4 skardi-cli tests; all failure
modes asserted to fire before any network call.

## Milestone 2 — HTTP client + action registry (PR: feature/open-connector-foundation)

Everything network-facing, behind one client; planning-time metadata in memory.

- [x] 2.1 `OpenConnectorClient`: health / discover / execute against the `/v1` contract, endpoint paths centralized in private constants
- [x] 2.2 Runtime token from env var as Bearer header; `http(s)` URLs without embedded credentials only; token excluded from `Debug`
- [x] 2.3 Bounded retries on 429 / transient 5xx / transport errors: exponential backoff + jitter, `Retry-After` honored (capped)
- [x] 2.4 Bounded response decoding (declared `Content-Length` + streamed bytes), per-request timeout from config
- [x] 2.5 Connection-alias header on execute calls
- [x] 2.6 `ActionRegistry`: deduplicated, concurrency-bounded discovery of `raw_action_allowlist`; non-locally-executable actions rejected; no partial registry
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

## Milestone 4 — DDL factory + UDTFs + security/observability (follow-up PR)

The full SQL surface for the mock pack.

- [ ] 4.1 `table_factory.rs`: `OPEN_CONNECTOR` `TableProviderFactory` for `CREATE EXTERNAL TABLE ... STORED AS OPEN_CONNECTOR` (session-scoped tables)
- [ ] 4.2 `open_connector_query` UDTF (built-in pack definitions only)
- [ ] 4.3 `open_connector_scan` UDTF (allowlisted raw actions only; deterministic row type or planning error)
- [ ] 4.4 Security policy enforcement: mutating actions rejected pre-HTTP; YAML overrides cannot swap actions/row paths; default-deny allowlist
- [ ] 4.5 Observability: scan spans/metrics (gateway, binding, action, cache hit, pages, rows, retries) without tokens or bodies
- [ ] 4.6 Docs: `docs/open-connector.md`, ctx/DDL/UDTF examples, README supported-sources entry (first time the source is actually queryable)

**Verification**: DDL and YAML registrations produce identical tables; UDTFs
share the stable schema; federated join of mock pack against a local CSV.

## Milestone 5+ — Real source packs (one PR each, per design rollout)

- [ ] 5.1 GitHub pack (API-key auth, page-number pagination): repositories, issues, issue comments, pull requests, reviews, commits, workflow runs, releases
- [ ] 5.2 Jira pack (OAuth, cursor pagination, JQL-backed filters): projects, issues, comments
- [ ] 5.3 Notion pack (explicit data-source binding, cursor pagination, dynamic properties with binding-time schema freeze): rows, pages, blocks, users
- [ ] 5.4 Later waves per the design rollout (Google Workspace, Discord, Feishu, Slack, HubSpot, …) through the source-pack admission gate

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
