# Open Connector Integration

[Open Connector](https://github.com/oomol-lab/open-connector) is a separate,
self-hosted SaaS gateway: it owns provider credentials, OAuth flows, token
refresh, action policies, and provider-specific HTTP execution for 1,000+
SaaS providers. Skardi adds the relational layer on top — stable table
definitions, JSON-to-Arrow conversion, pagination, safe filter and limit
pushdown, optional caching, and DataFusion registration — so selected SaaS
resources become ordinary SQL tables that can join against every other
Skardi source.

Provider credentials never enter Skardi. Skardi is configured with only two
things: the gateway URL and the name of an environment variable holding the
gateway **runtime token**.

> **Status:** the shared foundation is complete, and the first real
> provider packs have landed alongside the synthetic `mock` pack used by
> the test suite: [GitHub](open-connector-github.md) (repositories, issues,
> pull requests, reviews, commits, workflow runs, releases),
> [Slack](open-connector-slack.md) (conversations, users, files — message
> tables are gated on upstream cursor support), and
> [Notion](open-connector-notion.md) (users, pages, data sources, block
> children — dynamic-schema rows are gated on binding-time schema
> freeze). Further provider packs (Jira, …) ship one pack per release per the
> [design spec](superpowers/specs/2026-07-11-open-connector-integration-design.md);
> a source is advertised as supported only once its pack passes the
> admission gate there.
>
> A runnable local walkthrough (bundled stub gateway, server, all three SQL
> interfaces, federated join) lives in the
> [GitHub pack demo](open-connector/README.md).

## Configuration

An Open Connector gateway is a `type: open_connector` data source with
`hierarchy_level: catalog` and a typed `open_connector:` block:

```yaml
kind: context

metadata:
  name: saas-example
  version: 1.0.0

spec:
  data_sources:
    - name: saas                                  # catalog name in SQL
      type: open_connector
      connection_string: http://open-connector:3000
      hierarchy_level: catalog

      open_connector:
        # Environment variable holding the gateway runtime token.
        # The token value itself never appears in YAML.
        runtime_token_env: OPEN_CONNECTOR_TOKEN

        # Safety bounds (defaults shown).
        request_timeout_seconds: 30      # one gateway HTTP request
        scan_timeout_seconds: 300        # one whole scan, all pages
        max_pages: 100                   # pages per scan
        max_rows: 100000                 # rows per scan
        max_response_bytes: 16777216     # decoded bytes per response
        max_attempts: 3                  # attempts per gateway call

        # Caching: live reads by default; > 0 enables the bounded
        # in-memory TTL cache shared by all scans of this gateway.
        cache_ttl_seconds: 0
        cache_max_bytes: 268435456

        # Actions open_connector_scan may invoke. Empty by default —
        # raw-action access is default-deny.
        raw_action_allowlist:
          - github.list_repository_issues

        # Persistent stable tables: each binding becomes a schema in the
        # gateway catalog, each listed source-pack table becomes a table.
        bindings:
          - name: github_skardi          # schema name in SQL
            source_pack: github          # built-in pack
            source_pack_version: 1       # optional pin (schema stability)
            connection_alias: work       # optional Open Connector alias
            resource:                    # inputs the pack requires
              owner: SkardiLabs
              repo: skardi
            tables:
              - issues
              - pull_requests
```

Notes:

- Unknown keys anywhere in the block are rejected at load time — a
  misspelled `source_pack_versions` fails loudly instead of silently
  disabling the pin it was meant to set.
- One binding can serve tables with different resource needs: each table's
  requests carry only the resource keys its contract declares (Open
  Connector's strict action schemas reject undeclared inputs), so binding
  `repositories` alongside `issues` under one `owner`/`repo` resource map
  just works. A resource key that *no* bound table declares fails
  registration as a probable typo.
- The gateway URL must be plain `http(s)://` with no embedded credentials,
  query string, or fragment; the runtime token travels only as a Bearer
  header.
- The source is read-only by construction. `access_mode: read_write`, SQL
  DML, and job destinations are all rejected.
- Registration is a configuration action, not a SQL action: bindings change
  only through reviewed context YAML, never through DDL.

At startup Skardi validates the config, health-checks the gateway,
discovers the metadata of every referenced action (bound pack tables plus
the raw-action allowlist), verifies pack version pins, required resource
inputs, and action-contract fingerprints, and only then registers the
catalog. Query planning never performs network I/O.

## Three SQL interfaces

### 1. Stable catalog tables

Each binding is a schema under the gateway catalog:
`<gateway>.<binding>.<table>`. This is the interface for repeatedly queried
resources and federated joins:

```sql
SELECT number, title, author_login
FROM saas.github_skardi.issues
WHERE state = 'open'
LIMIT 50;
```

Filters that the source pack maps faithfully (`Exact`) are pushed into the
provider API call; everything else stays in DataFusion. `LIMIT` stops
pagination as soon as enough rows have been emitted.

### 2. `open_connector_query` — built-in pack tables, ad hoc

Runs a **built-in source-pack table** without a persistent binding.
Arguments: gateway, stable table ID, resource JSON, optional connection
alias.

```sql
SELECT number, title, author_login
FROM open_connector_query(
  'saas',
  'github.issues',
  '{"owner":"SkardiLabs","repo":"skardi"}',
  'work'                    -- optional; defaults to the gateway default
)
WHERE state = 'open'
LIMIT 50;
```

It compiles into exactly the scan the YAML-bound table uses: same stable
Arrow schema, filter allowlist, pagination, safety bounds, and shared
cache. The table's action must have been discovered when the gateway was
registered — bind the table in YAML or add its action to
`raw_action_allowlist`; otherwise planning fails with an error saying so
(planning never contacts the gateway).

### 3. `open_connector_scan` — allowlisted raw read actions

The escape hatch for actions no pack covers yet. Arguments: gateway, action
ID, input JSON, row path, optional connection alias.

```sql
SELECT number, title
FROM open_connector_scan(
  'saas',
  'github.list_repository_issues',
  '{"owner":"SkardiLabs","repo":"skardi","state":"open"}',
  '$.issues'
)
LIMIT 50;
```

Raw scans are deliberately narrow:

- **Default-deny.** The action must be in the gateway's
  `raw_action_allowlist`, *and* its discovered metadata must classify it as
  a non-mutating read (`execution.readOnly`). A missing or ambiguous
  classification is refused with an error naming the gap — the allowlist
  alone never grants execution. Both checks fire at planning time, before
  any HTTP request. **Current-gateway caveat:** Open Connector does not yet
  publish a read/write classification in its action metadata (verified
  against v1.3.1), so raw scans against today's real gateway are refused by
  this gate; built-in pack tables — read-only by Skardi's own review — are
  unaffected. The parse site is forward-compatible for when the upstream
  grows the field.
- **Deterministic row type or planning error.** The Arrow schema is derived
  from the discovered action output schema at the row path: declared
  primitives (`string`, `integer`, `number`, `boolean`, including
  `["T","null"]` unions) become typed nullable columns; objects, arrays,
  wider unions, and undeclared types become JSON-string columns. If the row
  path does not resolve to an array of objects with declared properties,
  planning fails and recommends a built-in pack table or a source-pack
  contribution.
- **One request, one page.** Raw actions declare no pagination contract, so
  the action executes exactly once; pass any paging inputs explicitly in
  the input JSON. Raw scans are always live (never cached) and support no
  filter pushdown — provider-side filters go in the input JSON, SQL
  predicates are evaluated by DataFusion.

## Federated joins

Open Connector tables join like any other source:

```sql
SELECT i.id, i.name, l.label
FROM open_connector_query('saas', 'mock.items', '{"workspace":"demo"}') i
JOIN 'labels.csv' l ON i.id = l.id;
```

## Security model

- Provider credentials stay in Open Connector; Skardi holds only the
  gateway runtime token, read from the environment at registration.
- Tokens never appear in YAML, logs, `Debug` output, error messages, or
  the data-sources API.
- Stable tables and `open_connector_query` can execute only the read
  actions hard-coded in Skardi's source packs; bindings cannot override the
  pack's action, row path, pagination, or schema (unknown keys are rejected
  at parse time).
- `open_connector_scan` requires an explicit allowlist entry **and** a
  read-only classification in the discovered metadata; mutating and
  unclassified actions are rejected before any HTTP request.
- The integration registers no DML: `INSERT`/`UPDATE`/`DELETE` and
  read-write access modes fail with targeted errors.
- The metadata these gates read (read-only classification, executability,
  action-contract fingerprints) is discovered at registration and holds
  until the next restart or configuration reload — query planning never
  re-contacts the gateway. An action whose upstream definition turns
  mutating after registration is therefore not re-checked by Skardi inside
  that window.
- Open Connector's own action policies remain a second, independent
  enforcement boundary — and the live one during the staleness window
  above.

## Caching and freshness

Live reads are the default (`cache_ttl_seconds: 0`). With a positive TTL,
completed scans are cached in a bounded in-memory LRU (byte- and
entry-capped) keyed by gateway, connection alias, action, source-pack
version, resource inputs, translated filters, projection, LIMIT, and the
Arrow schema fingerprint. Only completed scans are stored, so a truncated
result can never serve a fuller query. Both stable tables and
`open_connector_query` share one cache per gateway; raw scans bypass it.

Caching claims no transactional consistency: a live multi-page scan can
observe upstream changes between pages, subject to the provider's own
pagination guarantees.

## Bounds, retries, and errors

Every scan is bounded by `max_pages`, `max_rows`, `request_timeout_seconds`,
`scan_timeout_seconds`, and `max_response_bytes`; hitting a bound fails the
scan rather than returning a partial result as success. Idempotent gateway
calls (health, discovery) retry `429`/transient `5xx` with capped
exponential backoff honoring `Retry-After`; non-idempotent execute calls
retry only a pre-execution `429` and never re-send a request that may have
already run. Cursor pagination that stops advancing fails as a detected
loop instead of spinning forever; a continuation cursor of the wrong JSON
type fails the scan as itself (only an absent, `null`, or empty-string
cursor means end-of-collection — anything else would silently truncate).
Conversion errors report the action, row
path, page, row, column, and expected type — with the offending JSON
*kind*, never the value.

Provider errors reported *in-band* (Slack-style HTTP 200 with `ok: false`
+ `error`) are handled at one of two layers. Open Connector's own
executors consume them and return the gateway's failure envelope
(non-2xx), which Skardi surfaces with the provider's message — this is
the path the built-in slack pack relies on. For a gateway that instead
forwards such envelopes unchanged, a pack table must declare an
`error_path`; the scan checks it before row extraction and fails with the
provider's own code (bounded, value-free), never the misleading
missing-row-array error. A pack whose gateway does neither would
otherwise report `RowPathNotFound` on an in-band error page.

## Compatibility and schema drift

Each pack table pins the full relational contract and an expected
action-contract fingerprint captured from a live gateway (a canonicalized
BLAKE3 hash of the discovered output schema; both the github and slack
packs are pinned, with the captured schemas committed next to each pack
under `fixtures/<provider>/contracts/`). At registration a pinned
table's fingerprint is compared against the discovered contract and any
difference — breaking or additive, since a hash cannot tell them apart —
fails with a targeted error instead of silently changing a table's
schema; upgrading the gateway means re-capturing the contract and
re-pinning. (Additive upstream fields in *row data* are still simply
ignored by conversion — the fingerprint gates the declared schema, not
the rows.) The gate's coverage is exactly the upstream **declaration**:
mapped columns that a provider's schema leaves to `additionalProperties`
passthrough sit outside the fingerprint, and drift there surfaces at scan
time under the conversion rules instead (a shape change fails loudly; a
removed nullable field reads as NULL). Each pack pins its uncovered-column
set in a test, so that gap is a reviewed fact rather than an implicit one. Bindings may pin `source_pack_version` so a Skardi upgrade
cannot silently change a bound table's schema either.

## Observability

Every scan completion emits a structured tracing event with the gateway,
binding, table, action, cache hit/miss, pages fetched, rows returned, and
duration — identifying fields and counters only. Scan failures emit the
same identity plus the error; for failure diagnosability the error message
may quote a bounded (at most 512-character) snippet of the gateway's
*error* response — which can echo request identifiers such as an owner or
repo name — and, on pagination-loop detection, the offending cursor. The
client logs each retry with the operation and status. Tokens,
authorization headers, provider credentials, successful-response bodies,
and row data are never logged; conversion and row-path failures report
JSON *kinds*, never values.
