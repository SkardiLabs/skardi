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
      table definitions (`packs/github.rs`, `perPage` 100 — Open Connector's camelCase
      action-input contract, reconciled against a live gateway). Engine additions the pack
      required, all sanctioned by the design spec: per-mapping `Fidelity` (issues
      `updated_at >=` → `since` pushes **Inexact** and DataFusion re-applies it — verified
      against a gateway that ignores `since` entirely; commits' strictly-after `since` is
      deliberately NOT mapped since a dropped boundary row is unrecoverable), RFC 3339
      rendering of timestamp filter literals, `Utf8ListFromObjectKey` for the design's
      `$.labels[*].name` / `$.assignees[*].login` flattening, a JSON-null *parent* on a
      nested path is absence → SQL NULL for nullable columns (GitHub `commit.author:
      null` / `issue.user: null`), and `SourcePackTable::fixed_inputs` pinning `state=all`
      on issues/pull_requests so `SELECT *` reads the complete collection while a pushed
      `state` predicate overrides the pin (GitHub defaults to open-only). `issues` is
      pure issues: the Open Connector action filters out the pull requests GitHub's raw
      endpoint mixes in, so the table declares no `pull_request` marker column (it could
      never be non-NULL) and a negative-space guard test
      (`issues_declares_no_pull_request_marker`) pins that absence. Redacted per-table
      fixtures (`packs/fixtures/github/`) are the build-time conversion contract
      (null-bearing, null-parent, empty-list, nested, extra-field rows, and a
      schema-mismatch page whose targeted (column, page, row, expected, found-kind)
      error the contract test asserts, per the admission gate); the action IDs,
      input keys, row paths, and HTTP protocol are reconciled against a live gateway,
      and fingerprint pins are now taken from live-captured contracts
      (`fixtures/github/contracts/`, landed alongside 5.2's pinning recipe: sync
      test, contract-serving mocks, drift-refusal e2e). The `issues` table
      paginates on the gateway's raw page length (`raw_page_size_path:
      $.pageInfo.fetched`, a `PageNumber` extension mirroring `total_pages_path`;
      upstream fix oomol-lab/open-connector#228): the OC action filters pull
      requests out after paginating, so filtered page length is not a termination
      signal — short-page termination would silently truncate on any PR-bearing
      page, and even empty-page termination fails on 100 consecutive PRs. Engine
      unit tests pin continue-on-full-raw/terminate-on-short-raw/missing-and-
      invalid-signal failures plus total/raw mutual exclusion; a pack e2e drives
      a 3-page scan whose middle page is all pull requests.
      Docs: `docs/open-connector-github.md` (per-table filter/limit behavior, authz/
      visibility incl. the pure-issues note, rate limits, freshness), README row updated.
      Verification: 27 pack tests (counted by
      `cargo test -p skardi --lib sources::providers::open_connector::packs::github`;
      203 open_connector tests total) — 8 fixture contract suites incl. empty pages
      and the schema-mismatch page, bind-time validation of all 8 contracts, the
      no-marker negative-space guard, and
      end-to-end via mock gateway: 150-row two-page scan with the `state=all` pin on
      every request, pushed `state` override (Inexact — faithful only inside the
      provider's enum domain), Inexact `since` narrowing + local re-filter keeping the
      boundary row, LIMIT stopping after one page, `open_connector_query` parity — plus
      new filters/json_to_arrow engine tests. Runnable local demo (`docs/open-connector/`,
      in the db-source demo style): bundled stdlib-Python stub gateway standing in for
      the remote service the way DynamoDB Local does — speaking the gateway's real
      protocol (uniform `{success, message, data, meta}` envelope, `POST
      /v1/actions/:id`, camelCase inputs) — committed ctx + four pipelines
      (stable table with pushdown, both UDTFs, federated CSV join) — every README
      command and output executed against the real server before being written down;
      a final section documents the real-gateway path, whose protocol and action
      contracts have since been reconciled live, and fingerprint pins have since
      landed for all eight tables (captured contracts under
      `fixtures/github/contracts/`).
- [x] 5.2 Slack pack (OAuth bot token, cursor pagination): conversations (channels), users,
      and files, per the design's Slack caveat — message/thread tables stay gated on upstream
      complete message-cursor handling and are explicitly documented as absent. The wire
      contract is Open Connector's normalized one, reconciled against a live gateway
      (v1.3.1) and the OC provider source: camelCase rows (`channelId`, `realName`, …),
      row arrays under `conversations`/`users`, top-level `nextCursor` (null at end), and
      Slack's in-band `ok:false` consumed by the executor (so the tables declare no
      `error_path`; the engine mechanism is modeled by the mock pack). Cursor pagination
      (`cursor` / `$.nextCursor`, `limit` 200) terminates ONLY on the end-of-collection
      spellings (null, empty-string, or absent cursor); a present non-string cursor
      fails as `PaginationCursorInvalid` (kind-only, never the value) instead of
      silently truncating, structural path failures propagate as themselves, and a
      non-advancing gateway fails as `PaginationLoop`; `files` uses Slack's classic
      `page`/`count` pagination
      terminated by the envelope's authoritative `paging.pages` (a `total_pages_path`
      extension to `PageNumber` — short non-final pages, legal under permission filtering,
      never truncate; missing/non-numeric totals fail loudly).
      `types: ["public_channel","private_channel"]` pinned on conversations as the schema's
      array (the `state=all` move, via the new `FixedValue::StrList`); `includeLocale`
      pinned on users so the declared `locale` column is populated; `files.user_id =` →
      `userId` pushed Inexact per the string-push rule; **no time filter is pushed** — the
      OC `list_files` contract declares no `ts_from` input and its strict schema would 400
      one, so `created` predicates run in DataFusion (the engine's per-mapping
      `ValueFormat` stays for future packs; non-timestamp mappings declare
      `ValueFormat::Verbatim`, which also refuses to push a timestamp literal in a
      guessed spelling). Engine support:
      `FieldType::TimestampSecondsUtc` (Slack's epoch-second `files.created` — the millis
      reader would silently produce 1970 dates); `files` optionally scopes to one channel
      via the `channelId` optional resource. **Fingerprints are pinned** (a pack
      first): each `expected_fingerprint` is the BLAKE3 hash of the canonicalized
      output schema captured from the live gateway into
      `packs/fixtures/slack/contracts/`; a sync test locks pin ↔ captured contract,
      every mock registration serves the captured contracts (so the pass side of the
      gate is exercised by the whole suite), and a drift e2e proves a differing
      discovered schema fails registration as `ActionContractMismatch` naming the
      table and action. The GitHub pack pins the same way. Live-verified: all
      three tables' generated inputs pass the gateway's strict
      action schemas (requests reach the credential wall, not `invalid_input`).

      **Pack format**: all built-in packs (mock, github, slack) are now
      declarative **embedded YAML assets** (`packs/*.yaml`, `include_str!`-compiled,
      parsed once at first registry access via `packs/loader.rs` and leaked into the
      engine's `&'static` shapes) — the design doc's illustrative pack format, and
      the groundwork for its deferred second tier (user-authored packs from a
      directory). The contract boundary is unchanged: packs stay inside the binary,
      versioned, fingerprint-gated, never user-editable. Parsing is strict
      (`deny_unknown_fields` end to end), table order is deterministic (BTreeMap),
      the loader cross-validates each document (duplicate columns, filters
      referencing undeclared columns, resource/fixed-input/pagination key
      collisions) before converting it, and a malformed asset surfaces as a
      targeted `SourcePackAssetInvalid` registration/UDTF-setup diagnostic —
      never a panic — with a parse-all test keeping shipped assets valid.

      **Verification**: 246 open_connector tests (counted by `cargo test -p skardi --lib
      sources::providers::open_connector`): per-table fixture contract tests against the
      normalized shapes (explicit nulls vs omitted `memberCount`, flattened profiles,
      deleted users, Slack's empty-string convention, epoch-seconds `files.created`, empty
      pages, and a schema-mismatch page whose targeted (column, page, row, expected,
      found-kind) error the contract test asserts, per the admission gate — distinct
      from the legitimate omitted-`memberCount` NULL path); e2e via mock gateway
      speaking the real envelope — multi-page cursor scan (no
      cursor on page 1, token afterwards, `limit` hint + `types` array pin on every
      request), both termination spellings, pagination-loop detection bounded at the first
      repeated cursor, LIMIT early stop, empty workspace, `userId` pushed and re-applied
      against an ignoring provider, the negative-space guard that no time key ever reaches
      the wire, gateway-failure surfacing of Slack's `ok:false`, multi-table binding with
      zero required resources, UDTF parity for `slack.users`, and a two-page
      users cursor scan pinning that table's own wire declarations (`$.users`
      row path, `cursor`/`limit` inputs) independently of conversations'.
- [x] 5.3 Notion pack (integration token, cursor pagination): `users`, `pages`,
      `data_sources`, `block_children` as stable tables (`packs/notion.yaml`).
      The wire contract is Open Connector's raw passthrough of the Notion API
      (rows verbatim under `$.results`, native `$.next_cursor` envelope — null
      terminates, `has_more` deliberately unused; camelCase strict inputs
      `startCursor`/`pageSize`/`blockId`), reconciled against a live gateway.
      `pages`/`data_sources` are two pinned views of `notion.search`: the
      required `query` pinned to `""` plus per-table object `filter` pins —
      which required the one engine extension this milestone adds,
      **`FixedValue::Json`** (object-shaped fixed inputs, loader-parsed and
      leaked, with the non-finite-float guard extended to nested values).
      **No filter pushdown anywhere** (search's free-text relevance `query`
      maps to no SQL predicate faithfully; the other actions declare no filter
      inputs) — a wire guard test pins that requests carry exactly the declared
      inputs. Dynamic property maps stay opaque JSON: the `rows` table over
      `query_data_source` is deliberately absent pending the design's
      binding-time schema freeze, and `block_children` excludes the
      type-keyed payload (a fixed mapping cannot address a key named BY
      `type`). `users` excludes `person.email` (capability-gated, privacy).
      Fingerprints pinned from live capture (`fixtures/notion/contracts/`;
      pages/data_sources share search's pin); the coverage-gap pin records
      that search declares an EMPTY item schema, so both search tables'
      columns ride additionalProperties passthrough. Live-verified: all four
      tables register against a live gateway (pins match discovery) and every
      scan reaches the credential wall with the exact pinned wire inputs
      (`{"filter":{"property":"object","value":"page"},"query":"","pageSize":100}`
      observed in the gateway's own run log). **Verification**: 260
      open_connector tests (counted by `cargo test -p skardi --lib
      sources::providers::open_connector`; 14 new) — four fixture contract
      suites plus the schema-mismatch fixture, fingerprint sync + coverage
      pins, drift-refusal e2e, and per-declaration e2e (users two-page cursor
      with row identity, per-table search filter pins with an
      exactly-the-declared-inputs guard, blockId resource forwarding +
      pre-HTTP enforcement, LIMIT early-stop, UDTF parity).
- [x] 5.4 Feishu pack (OAuth user token, cursor pagination, hybrid wire shape — normalized envelope with raw snake_case items): chats, messages, chat_members, tasks, wiki_spaces, wiki_nodes.
      Live contract reconciliation (gateway v1.3.3; all six input sets
      validated to the credential wall, then all six tables verified against a
      REAL workspace end to end on 2026-08-04 — the loose items schemas make
      real rows the only column truth, and every fixture is a redacted live
      capture). The live pass caught three contract defects no mock could:
      tasks' `completed` boolean does not exist on the wire (status/
      completed_at do; the draft column + pushdown are gone), Feishu wiki
      answers its final page with `has_more:false` beside a NON-empty
      `page_token` (new Cursor `has_more_path` engine field, authoritative
      termination, both failure arms typed), and `im/v1/messages` caps
      page_size at 50 on the wire against a declared max of 100. Engine
      extensions: `timestamp_ms_string_utc`/`timestamp_s_string_utc` column
      types, `epoch_seconds_string` filter format, simplified-boolean pushdown
      normalization, `has_more_path`. Live e2e evidence: registration through
      LIVE discovery; 86 messages over two real cursor pages, zero duplicate
      ids; `create_time >=` pushdown narrowing a live scan to 15 rows; every
      mapped column of every table non-NULL on real rows. Verification: 285
      tests (`cargo test -p skardi --lib sources::providers::open_connector`,
      post-merge with 5.3), 17 pack-scoped (`… packs::feishu`), 842 full
      library suite.
- [x] 5.5 Gmail pack (Google OAuth, page-token cursor pagination): `threads`,
      `messages`, `drafts`, `labels`, `filters` as stable tables
      (`packs/gmail.yaml`) — the first Google Workspace pack. The wire
      contract is Open Connector's normalized rebuild of the Gmail API
      (Slack-style: camelCase identity, headers flattened into
      `subject`/`sender`/`to`, `internalDate` re-emitted as RFC 3339
      `messageTimestamp`; `labels`/`filters` pass provider objects through
      raw), reconciled against a live gateway v1.3.4 and the OC executor
      source. Cursor termination is the executor's explicit
      `nextPageToken: null` (absent and `""` also terminate — one e2e per
      spelling across the tables). `messages` pins `detail: summary` (ids
      too thin, full unbounded) with page size 100 — the executor hydrates
      each listed message (N+1 Gmail calls per page), so the page size
      bounds the burst; `threads`/`drafts` use Gmail's 500 ceiling and
      never send `verbose`. `labels`/`filters` take no pagination inputs at
      all, which required the one engine extension this milestone adds:
      the loader's **`single_page` strategy spelling** (the engine's
      `SinglePage` existed but was unreachable from YAML; braced variant so
      `deny_unknown_fields` still rejects stray keys) plus its optional
      `next_cursor_path`, which turns the strategy's "one request is the
      whole collection" premise into a checked assertion — a live
      continuation fails as `SinglePageIncomplete` rather than returning a
      prefix as a complete table, closing the engine's one silent
      truncation. Raw scans (`open_connector_scan`) declare no path and
      keep the historic one-request behaviour. **No filter pushdown
      anywhere** (Gmail's `q` is a free-text language, `labelIds` an
      AND-semantics array — neither maps to a scalar `column op literal`
      faithfully); `query`/`labelIds`/`includeSpamTrash` are optional
      resources instead, and the default listing is documented as Gmail's
      own (excludes SPAM/TRASH; not pinned away because `list_threads`
      offers no such input — pinning it on `messages` alone would make the
      two tables describe different mailboxes). `to` is mapped as
      `to_addresses` (TO is a reserved SQL keyword); header-derived
      absents stay `""`, never NULL. Excluded, with rationale in the
      module doc: `search_threads` (strict subset), `list_history`
      (checkpoint API), `list_forwarding_addresses` (no output schema to
      fingerprint), `get_profile` (scalar), message bodies (future
      content surface). `error_path: None` everywhere (executors consume
      provider errors; failure envelope pinned by e2e). Fingerprints
      pinned from live capture (`fixtures/gmail/contracts/`, gateway
      v1.3.4) over the whole declared schema, `anyOf` branches included —
      a renamed field inside `fetch_emails`' items moves the hash and
      fails registration. Unlike the other packs' pins, gmail's records a
      limit of the static walker, not a gap in the gate: the `messages`
      paths ARE declared, inside `anyOf` branches the helper cannot
      follow. Input schemas are pinned too (`contracts/inputs/`), since
      the gate is output-only. **Live-verified end to end
      against a real mailbox (2026-08-05, gateway v1.3.4 + Google OAuth
      through the gateway)**: registration passed the fingerprint gate
      against live discovery for all five actions; every mapped column
      of every table extracted a real non-NULL value through
      skardi-server; real multi-page cursor chaining (`maxResults: 1`,
      three pages) and real final-page null-token termination observed;
      `query`/`labelIds`/`includeSpamTrash` resources observed narrowing
      real listings; input bounds `[1,500]` and the `detail` enum
      enforced by the real gateway. The live pass settled two synthetic
      guesses (which system labels omit visibility fields; a fresh
      draft's threadId equals its messageId) and caught one upstream
      bug: a zero-filter mailbox fails `list_filters` with
      `internal_error` (Gmail returns an empty body the executor's
      `response.json()` does not tolerate) — documented in the pack doc,
      fix belongs upstream. Fixtures are redacted live captures from
      that pass (mechanically audited against an allowlist); executor
      absent-spellings the capture lacked are pinned by an inline
      converter test.
      **Verification**: 284 open_connector tests (counted by `cargo test
      -p skardi --lib sources::providers::open_connector`, confirmed
      against the PR's CI run; 23 new) — five
      fixture contract suites plus the schema-mismatch and null-parent
      pins, fingerprint sync + coverage pins, drift-refusal e2e, loader
      single_page pass/refusal, registry pin, and per-declaration e2e
      (threads/messages/drafts two-page cursor scans each pinning its own
      exact wire keys and a distinct termination spelling, single-page
      one-request pins for labels/filters, optional-resource forwarding
      with type fidelity and per-table withholding, no-pushdown guard
      with row identity, LIMIT early-stop, pagination-loop refusal,
      gateway failure-envelope surfacing, UDTF parity).
- [ ] 5.6 Later waves per the design rollout (Google Calendar, Google Drive, Discord, HubSpot, Jira, …) through the source-pack admission gate
- [x] 5.6 Discord pack (OAuth user-identity surface, raw passthrough rows, no
      pagination envelope): guilds, connections, sticker_packs. The provider is
      @me-only (its own get_user rejects any other id), so channels/messages/
      members are out of scope by provider surface, not deferral; entitlements
      is deferred because the upstream executor exposes no pagination inputs
      (first-page-only). Engine extensions: `PaginationStrategy::Keyset`
      (cursor = a field of the previous page's LAST ROW, `after`-style; ONLY an
      empty page terminates — short pages continue, so a silent page-size clamp
      of the kind the Feishu live pass observed cannot read as completion;
      missing or non-string cursor on a non-empty page is typed drift, not a
      quiet stop; a repeated cursor fails with identity only, never quoting the
      row value) and an explicit `single_page` YAML spelling. Live contract
      reconciliation 2026-08-07: action IDs + executor passthrough confirmed in
      source, strict inputs validated to the credential wall via the 403-vs-400
      probe, three output schemas captured and fingerprint-pinned, then all
      three tables verified against a REAL account end to end through
      skardi-server (registration through LIVE discovery; guilds 6 rows and
      sticker_packs 14 rows with every mapped column non-NULL on real rows;
      connections 1 real linked-account row — all nine wire keys mapped, the
      connection_type rename extracting, `revoked` genuinely absent on a
      non-revoked row with its non-NULL arm on a synthetic fixture row, since
      capturing it live would revoke a real account link). The real keyset
      walk (limit 2) covered 3 full pages
      plus the empty terminator, no duplicate/boundary drop, ascending-
      snowflake ordering confirmed. The live pass caught one contract defect no
      mock could: the gateway calls the UNVERSIONED discord.com/api, where
      `permissions` is a truncated NUMBER and the full bitfield string lives in
      `permissions_new` — the column now maps `permissions_new`, and the
      version-coupled risk (a future /api/v10 pin removes that key) is
      documented in the pack doc with an upstream ask to pin the version.
      guilds/sticker_packs fixtures are redacted live captures with a
      mechanical allowlist tripwire test on every person-linked fixture.
      Operational: Discord 429s rapid probes; the gateway surfaces them loudly.
      Keyset failures carry precise, value-free diagnostics
      (`PaginationKeysetCursorInvalid` with per-case reasons,
      `PaginationKeysetLoop` withholding the repeated row value), pinned by
      full rendered-message assertions. Verification: 311 tests (`cargo test
      -p skardi --lib sources::providers::open_connector`; 23 new — 9 keyset
      engine, 1 loader, 13 pack-scoped `… packs::discord` including
      per-table wire-declaration e2e for all three tables, UDTF parity, and
      empty-page schema stability), 868 full library suite.
- [x] 5.7 Outlook pack (OAuth user token, cursor pagination over Graph
      `@odata.nextLink`, raw-passthrough rows): messages, mail_folders. Open
      Connector has no `microsoft365` service; it splits Graph into `outlook`
      (mail only — no calendar or contacts), `one_drive` and `excel`, with one
      OAuth connection each, so Microsoft 365 ships one pack per service:
      `one_drive` follows as its own milestone and PR, and the whole `excel`
      service is deferred at the gate — its list actions emit `nextLink` but
      accept no `nextLink` input, so their pagination cannot be completed.
      Phases 1–2 (live contract reconciliation against gateway v1.3.4, table
      design) are recorded in
      `docs/superpowers/specs/2026-08-14-open-connector-m365-packs-design.md`.
      Phase 3 implemented: `packs/outlook.yaml` + `packs/outlook.rs`, both
      tables cursor-over-`nextLink` (URI-shaped cursors everywhere — the
      gateway pins format/host/path), `messages` pins `select` to exactly the
      mapped fields (bounds responses away from the 16 MiB cap and turns a
      misspelled passthrough column into a loud Graph 400; a test locks the
      pin to the column set) with `page_size: 100`, `mail_folders` pins
      `includeHiddenFolders: true` (complete root-level set, `is_hidden`
      queryable); zero filter mappings (OData `$filter` expressions cannot be
      composed by a scalar mapping — Notion precedent); input schemas captured
      from the pinned gateway (`contracts/inputs/`) with the gmail-style
      acceptance test; fingerprint sync + coverage-gap pins (mail_folders one
      column, `well_known_name`; messages' thirteen passthrough/nested columns
      an explicit set); redacted live-capture fixtures (nine messages, nine
      root folders) re-audited every run by `fixtures_stay_redacted`, with the
      shapes the live wire cannot produce (explicit nulls, a hidden folder, the
      type-mismatch page) kept inline-synthetic; per-table cursor e2e with exact
      key-set wire pins, termination on the executor's one spelling (explicit
      `null` — the engine's tolerance for absent/empty is pinned in
      `pagination.rs`, not re-mocked here), loop/invalid-cursor
      failure arms, LIMIT early-stop, resource forwarding/withholding,
      no-pushdown row identity, gateway failure-envelope surfacing, UDTF
      parity, drift-refusal at registration. Note `outlook.list_messages`
      declares no timestamp field at all, so `receivedDateTime` and every
      other date ride `additionalProperties` passthrough outside the
      fingerprint gate. Phase 4 (live real-row verification) ran
      2026-08-19 against a real MSA mailbox through the pinned gateway:
      every mapped column carried non-NULL values through a skardi-server
      SQL scan, live discovery was byte-identical to the committed
      contracts (both actions, both halves), a forced top=2 walk hit a
      genuinely-null terminal cursor, top=1000 is the real wire bound
      (1001 → schema 400), select misspellings 400 loudly, `mailFolderId`
      forwards verbatim, and the fixtures were re-derived as redacted
      live captures with a `fixtures_stay_redacted` CI tripwire.
      Findings: folder-scoped continuation uses Graph's parenthesized
      `mailFolders('{id}')/messages` form, which the executor's own
      allowlist rejected — scoped scans past one page 400ed until the
      upstream fix (open-connector#372, merged 2026-08-19; tagged
      releases through v1.3.5 predate it); the live mailbox had no
      hidden folders,
      so the `includeHiddenFolders` pin's effect was unobservable
      (recorded as caveat); wire extras `@odata.etag` and `sizeInBytes`
      remain deliberately unmapped, while `wellKnownName` was promoted
      to a `well_known_name` column post-pass (Owen-approved
      2026-08-19: live display names were all CJK, so cross-account
      folder semantics need Graph's locale-independent discriminator;
      mail_folders' coverage gap goes empty → one pinned column). Test
      counts from CI on `ef5125f` (`cargo llvm-cov nextest
      --all-features`: 1864 tests plus the 237-test ignored suite, all
      green): 368 `sources::providers::open_connector` tests, 24 of them
      pack-scoped `…::packs::outlook`, inside a 1716-test skardi library
      binary. Phase 5 (self-review) ran 2026-08-19 — sixteen findings,
      fifteen fixed: vacuous test defenses (cardinality assertions arrow
      satisfies with `""` on null slots, mapped columns with no positive
      witness), a redaction audit that admitted an as-captured cursor or
      `webLink` on a host-prefix match alone, doc claims the pack's own
      fixtures contradict, and two hardcoded builtin-asset rosters
      lagging two shipped packs. Deferred: consolidating the pack-test
      helper quartet into `testutil` — seven-way duplication across the
      packs, so it belongs in its own cross-pack PR rather than a
      half-migration here.
- [x] 5.8 OneDrive pack (OAuth user token, cursor pagination over Graph
      `@odata.nextLink`, raw-passthrough rows): `drive_items`,
      `drive_item_search`. The second Microsoft 365 pack — Open Connector has
      no `microsoft365` service, so M365 ships one pack per service (`outlook`
      is 5.7 above, merged in PR #212; the whole `excel` service stays
      deferred at the gate, its list actions emitting `nextLink` but
      accepting none).
      This pack is independent of that one — it touches no engine code, only
      `packs/` plus the two registration rosters, so whichever of the two
      lands second will conflict merely in those rosters and in
      `docs/open-connector.md`'s status paragraph. Phases 1–2 recorded in
      `docs/superpowers/specs/2026-08-19-open-connector-one-drive-pack-design.md`;
      the phase-1/2 material was reconciled 2026-08-14 alongside outlook's,
      narrowed out of that branch in `0a76447`, restored here and
      **re-verified live 2026-08-19** (both committed contract captures still
      byte-identical to live discovery, so the v1.3.4 / `2410fbe` pin has not
      drifted). Phase 3 implemented: `packs/one_drive.yaml` +
      `packs/one_drive.rs`, both tables cursor-over-`nextLink` with URI-shaped
      cursors everywhere; `page_size: 999` (the declared ceiling — live probes
      confirm 999 passes while 1000 and 0 both 400); zero filter mappings
      because neither action exposes a filter input AT ALL (structural, not an
      omission); zero fixed inputs — notably **no `select` pin**, because
      every mapped column resolves inside the declared item schema, so
      the coverage-gap pin is EMPTY and drift is already loud at registration
      (the sharpest contrast with outlook's thirteen uncovered `messages`
      columns, which is exactly what its `select` pin exists to protect).
      Both tables share ONE contract fingerprint by construction (Graph
      declares the same driveItem collection schema for a listing and a
      search; a test states this so it is not "fixed"). `query` is a required
      resource on the search table, and the live probes pinned down why the
      mechanism is subtler than it looks: the input schema's `required` array
      is empty and `query` is only `minLength: 1`, so an EMPTY string 400s as
      `invalid_input` while a MISSING or whitespace-only query passes
      validation and dies in the executor's own trim check. Declaring it
      required closes only the MISSING case — the check is presence plus
      non-null, so `query: " "` still registers cleanly and fails at scan
      time on the gateway's own 400. Facet presence
      is the item-type discriminator (`file_mime_type` / `folder_child_count`,
      the latter keeping 0 distinct from NULL). Docs:
      `docs/open-connector-one-drive.md` + the status note in
      `docs/open-connector.md`. **Phase 4 (live real-row verification) DONE
      2026-08-21** against a real personal MSA drive under a fresh
      `one_drive` OAuth grant (the outlook authorization does not cover it),
      raw probes plus end-to-end skardi-server scans through the live
      fingerprint gate; the design record's seven-item list is answered item
      by item in its "Phase 4 results" section. Highlights: the pass CHANGED
      the pack once — real search rows are a reduced Substrate projection
      that carried no `eTag`/`cTag` at all on the personal (MSA) drive
      probed (zero across 1800+ hits), so `drive_item_search` dropped both
      columns (16 → 14; the wire wins over the declared contract, exactly
      the always-NULL defect class the pass exists to catch). ONE RESIDUAL
      (design record R1): that reduction rests on a single personal drive
      and a business/SharePoint-backed drive is unprobed. Being wrong is
      asymmetric and silent — the tags are in the shared declared schema,
      so neither the fingerprint gate nor the coverage gap would flag a
      business drive that does return them — so re-probe `search_items`
      there; if it carries the tags, widening the table back to 16 is the
      expected outcome (additive for consumers, bump the pack version),
      not a contract break. The pre-identified silent-truncation risk is
      CLOSED: allowlist-rejected cursors ERROR (400 `invalid_input`, probed
      in both cross-action directions), never null. The search cursor's
      parenthesized form passes the gateway allowlist but Graph itself
      fails real search continuations server-side on personal drives
      ("Error Calling Substrate Search", deterministic) — loud through the
      failure envelope, not a truncation; one-page searches terminate on a
      clean null. Also settled: `top: 999` is a wire bound; real terminal
      pages null explicitly; children paginate in all three path forms;
      every mapped column witnessed non-NULL except `description` (0 of
      2800+ rows — kept, caveat recorded); `folderItemId` beats
      `folderPath` silently when both are set; both row fixtures re-derived
      as redacted live captures under a renamed default-deny redaction
      audit. Following gmail (5.5), both halves of each
      action's contract are committed — `contracts/inputs/` alongside the
      output captures, re-fetched live 2026-08-19 — and a test locks every
      key the pack can send against them, so the output-only fingerprint
      gate's blind spot (a renamed input key registers cleanly, then 400s
      every scan) is at least caught on re-capture; it also turns four
      prose claims into checked facts: `top`'s 1–999 bounds contain the
      pinned 999, `nextLink` is declared `format: uri`, `query` carries
      `minLength: 1` while no `required` array exists, and
      `filter`/`orderby`/`skip`/`page`/`perPage` are absent from the input
      surface entirely. Engine addition: `exclusive_resources` on
      `SourcePackTable` — groups of resources that are ALTERNATIVES, so a
      binding supplying two members is refused at registration by name
      instead of being resolved by the upstream executor's precedence
      (`folderItemId` beats `folderPath`, silently scanning a folder the
      operator did not name). Additive and defaulted empty; the loader
      rejects a group that cannot conflict, names an undeclared resource,
      contains a required key, or overlaps another group. Verification: 30
      new pack-scoped tests
      (`cargo test -p skardi --lib sources::providers::open_connector::packs::one_drive`
      — 17 contract/conversion plus 13 e2e: per-table wire-declaration scans
      with exact key sets, null-cursor termination (the only spelling either
      action can emit, both declaring `nextLink` required), pagination-loop
      and failure-envelope arms, LIMIT early-stop proving `top` is NOT
      narrowed, resource forwarding/withholding, a search binding with no
      `query` refused before any HTTP, a binding naming both folder scopes
      refused before any HTTP (and one naming a single scope plus a
      `driveId` accepted), a contract-legal item with no `id`
      failing the page, no-pushdown row identity, UDTF parity, drift-refusal
      at registration, and a key-scoped default-deny redaction audit whose
      self-trip probes are the leak classes the real captures actually
      contained); conversion tests run against the redacted live captures
      (5 children rows, 3 search rows, both real-page composites);
      CI green on that PR's head `e12ec9c` (run 32729437184): 1981
      workspace tests passed (`cargo llvm-cov --no-report nextest
      --all-features`), plus 237 `--ignored` integration tests and 37
      doc tests.

- [x] 5.9 Google Drive pack (OAuth user token, cursor pagination over
      `pageToken`/`nextPageToken`, NORMALIZED rows — the opposite of 5.8):
      `files`, `drives`, `file_permissions`. **Phases 1–4 done 2026-08-25.**
      Design record risk R1 (whether the test account can see a shared
      drive at all) resolved in phase 4's favor: the account started at
      zero shared drives (that clean empty scan was itself witnessed),
      then proved able to CREATE one, so `drives` was verified on real
      rows instead of deferred. Recorded in
      `docs/superpowers/specs/2026-08-25-open-connector-google-drive-pack-design.md`,
      reconciled live 2026-08-25 against the same v1.3.4 / `2410fbe` pin as
      5.7/5.8. The service is spelled `googledrive` upstream and its action
      IDs carry TWO dots (`googledrive.files.list`) — verified engine-safe
      end to end with no engine changes (this pack touches only `packs/`
      plus the registration rosters). Rows are rebuilt by upstream
      normalizers into closed key sets (`additionalProperties: false`) with
      the executor pinning its own provider-side `fields` projection —
      `files.list` does not even declare a `fields` input — so the declared
      contract is column truth to an unusual degree: `files` (14 columns)
      and `file_permissions` (11) sit entirely inside the fingerprint gate,
      and `drives` (13) escapes it in exactly five columns — the
      `restrictions.*` boolean flags under a bare-open-object declaration,
      mapped KNOWINGLY (operator decision, phase 2): a coverage test pins
      exactly those five as the pack's only uncovered columns, and phase
      4 witnessed all five key spellings verbatim on a real drive row
      (present `false`s, alongside an unmapped nested
      `downloadRestriction`), closing the Notion `archived`/`is_archived`
      defect class for today's upstream — the gate still cannot see
      tomorrow's. The per-caller sibling
      `capabilities` stays unmapped (its values change with the OAuth
      identity doing the scan). One load-bearing fixed-input pair on
      `files`: `supportsAllDrives` + `includeItemsFromAllDrives` pinned
      true, because `files.list` forwards the former verbatim with NO
      upstream default (unlike its siblings, which default it true via
      `resolveSupportsAllDrives`) and an unpinned scan silently omits every
      shared-drive file. `q` is an optional resource on `files`/`drives`
      rather than a filter mapping, structurally: it is a whole query
      language, and a column-op-value literal is never a legal `q`; the
      deprecated `teamDriveId` alias is simply not exposed (avoiding the
      silent-precedence trap 5.8 needed `exclusive_resources` for).
      `fileId` is a required resource on `file_permissions` with the same
      three-way enforcement boundary as 5.8's `query` (upstream declares NO
      `required` array; missing → refused at registration, `""` →
      schema-layer 400, whitespace → executor 400 at scan time). Page sizes
      pinned at the declared ceilings (1000/100/100), confirmed live to be
      WIRE bounds with real credentials, not just declared ones (each
      ceiling 200; 1001/101/0 each 400). Both contract halves
      committed for all three actions (`contracts/` + `contracts/inputs/`,
      gmail-style) — these captures carry a `$schema` key the sibling
      packs' do not, and the fingerprint hashes the whole schema. Trashed
      files deliberately included (`trashed` is a column; filtering belongs
      in SQL). Deferred with reasons in the design record: `changes.list`
      (cursor bootstrapped by a second action — a pagination shape the
      engine has no spelling for), comments/replies/revisions/labels/
      accessproposals (second wave; accessproposals also
      Workspace-approval-gated). Docs: `docs/open-connector-google-drive.md`
      + the status note in `docs/open-connector.md`. Verification — CI
      green on `f763ce8` (run 32854955874): 2125 workspace tests passed
      (`cargo llvm-cov --no-report nextest --all-features`), plus 237
      `--ignored` integration tests and 37 doc tests. CI runs the whole
      workspace through nextest, not the per-module filters, so the two
      filtered figures are static counts of test functions in the tree
      (none `#[ignore]`d): 428 under
      `sources::providers::open_connector`, 402 of them before this pack
      — the 26 new ones being 25 pack-module tests
      (`cargo test -p skardi --lib sources::providers::open_connector::packs::google_drive`
      — 13 contract/conversion: per-table fixture conversions across all
      six admission-gate shapes (explicit null vs absent key vs empty
      string/list, partial `restrictions`, null owner list items,
      `sizeBytes`-as-string type mismatch with row-scoped error identity,
      id-less row failing the page), the default-deny redaction audit
      over every fixture string leaf, fingerprint pins for all three
      contracts, the five-column coverage-gap pin, pagination/resource/
      fixed-input declarations, and the captured-input-contract test
      locking every sendable key plus the declared-but-never-sent negative
      space per action; 12 e2e through MockGateway: per-table
      wire-declaration scans with exact key sets and the all-drives pins on
      every page, opaque-cursor continuation and null termination, a
      fileId-less permissions binding refused before any HTTP, no-pushdown
      row identity, LIMIT early-stop, empty shared-drive scan (the R1
      shape), pagination-loop refusal, failure-envelope surfacing with the
      dotted action id named, UDTF parity, drift-refusal at registration)
      plus 1 registry test asserting the BTreeMap table order; row fixtures
      are REDACTED LIVE CAPTURES (phase 4, 2026-08-25) under the
      default-deny audit. Phase-4 live evidence, in brief: three-table
      end-to-end skardi-server scans registered against LIVE discovery;
      every mapped column non-NULL somewhere except three structural
      residuals (`drives.org_unit_id`, `drives.theme_id`,
      `file_permissions.expiration_time` — keys present-and-null on real
      rows, each with a documented reason); real `pageSize: 1` multi-page
      walks with ~444-char cursors and terminal nulls; LIMIT stopping a
      real three-page scan after two requests; `q` live both as direct
      input and as a bound binding resource; live corrections folded into
      the fixtures (native Docs DO report `sizeBytes`; every grant
      carries `permissionDetails`; shared-drive rows drop exactly
      `owners`+`shared`).
- [x] 5.10 Dropbox pack (OAuth, cursor pagination with SPLIT-ACTION
      continuation, normalized wire shape — `mapDropboxMetadata` rebuilds
      every row into a strict fully-`required` camelCase object): files,
      shared_links, file_search.
      **Live-verified 2026-08-18** against a self-hosted gateway at commit
      `a3efa99` and a real free-tier Dropbox account, per the runbook
      `docs/superpowers/plans/2026-08-18-dropbox-live-evaluation.md`. All five
      actions discovered; all five committed fingerprints matched LIVE
      discovery unchanged, so registration passed the contract gate with no
      re-pinning (the source-derived captures turned out byte-identical to the
      live ones, and the opener/continuation schema identity is a fact about
      the wire). Page sizes probed at the boundary: 2000 and 1000 return rows,
      2001 and 1001 are refused. Termination confirmed as designed — the final
      `list_folder` page carries `hasMore: false` beside a NON-empty
      259-character cursor, so `has_more_path` is load-bearing. Real
      multi-page pagination through `list_folder_continue` (`pages=2
      rows=378`), and `LIMIT` collapsed the same scan to `pages=1 rows=1`.
      Both design-spec open questions answered on the wire: `{path, cursor}`
      together IS accepted by `list_shared_links` (no `cursor_only` needed),
      and `matches[].metadata` can never be null (the executor always returns
      a full object), so `file_search.tag`/`name` stay non-nullable.
      **What the live pass changed:** `shared_links` lost four columns —
      `path_display`, `is_downloadable`, `content_hash`, `sharing_info` —
      which `sharing/list_shared_links` has no field for and which measured
      0/5 non-NULL; a file whose `files` row carries all four returns all four
      NULL on its own `shared_links` row. 15 → 11 columns, pinned by a
      negative-space test. Per-column coverage after the trim: 12/12 on
      `files` (378 rows), 11/11 on `shared_links` bar the labelled
      `expires_at`, 13/13 on `file_search`. Two claims left UNOBSERVED, both
      environmental: `shared_links.expires_at` (paid-tier link expiry) and
      `file_search.match_type = 'content'` (content indexing did not land).
      Row fixtures remain authored shapes rather than redacted captures,
      because the verified account holds personal files.
      **Gateway prerequisite, filed upstream, not worked around:** at commit
      `a3efa99` the gateway's `GET /v1/actions/<id>` omits the `execution`
      block, so Skardi's default-deny action registry refuses every action
      from EVERY pack (reproduced with the merged `github` pack). Filed as
      oomol-lab/open-connector#358. Skardi's gate is correct and was
      deliberately left untouched — no compatibility fallback was added.
      Engine extension (backward-compatible, opt-in, `None` for every
      pre-existing pack): `pagination.continuation` — pages 2..N may target a
      DIFFERENT action (`list_folder` → `list_folder_continue`) and, with
      `inputs: cursor_only`, carry the cursor and nothing else. Both actions
      are discovered and fingerprint-gated at both gate call sites; because a
      fingerprint hashes the OUTPUT schema, the INPUT side is checked
      separately against the continuation action's discovered input schema,
      in BOTH directions of `inputs:` — `cursor_only` (cursor declared,
      nothing else `required`) and `full` when it targets a different action
      (the table's guaranteed keys satisfy `required`; under
      `additionalProperties: false` every key it can send is declared).
      A missing input schema is refused, as is a `required` that is present
      but not an array of strings. Three loader-side authoring refusals
      beyond the parse-level ones: a continuation pinned while the table's
      own action is not; a same-action continuation pinning a DIFFERENT
      fingerprint (unsatisfiable — one action, one contract); and
      `cursor_only` paired with an `Exact`-fidelity filter, which would
      apply the predicate on page one, drop it on pages 2..N, and have no
      `Filter` node left to re-apply it — silently returning rows the query
      excluded.
      Because the rows are normalized and strictly declared, every mapped
      column sits inside the fingerprint gate and the coverage-gap pin is
      empty. That is narrower than it sounds, and the live pass proved it:
      all five list actions publish the SAME normalized 15-key schema, so a
      fingerprint match says nothing about whether a key is populated for a
      given action — which is exactly how the four `shared_links` columns
      passed every contract-level check while being structurally always-NULL.
      Real rows remain the only column truth here too. Verification: 965
      full library suite, 324 open_connector (`cargo test -p skardi --lib
      sources::providers::open_connector`), 25 pack-scoped (`… --lib
      packs::dropbox`).

**Gate for each pack** (from the design spec): complete terminating pagination,
deterministic schema, read-only allowlist, documented authz/rate limits,
bounded safety defaults, null/empty/nested fixtures, docs.

---

## Review notes

- **Current PR**: milestone 5.10 (Dropbox pack + `pagination.continuation`)
  — open as #216, live-verified 2026-08-18; see the entry above.
  Milestones 1–4 and 5.1–5.9 (GitHub, Slack, Notion, Feishu, Gmail,
  Discord, Outlook, OneDrive, Google Drive packs) are merged; this PR adds
  the Dropbox pack against Open Connector's normalized Dropbox contract,
  plus the one engine extension it required: `pagination.continuation`,
  which lets pages 2..N target a different action and, with
  `inputs: cursor_only`, carry the cursor alone.
- **Invariants to hold in review**: no provider credentials in Skardi;
  read-only until explicitly designed otherwise; pure validation shared by
  CLI and server; no network I/O at query-planning time; no `.unwrap()` in
  production paths.
