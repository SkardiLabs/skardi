# Phases 2–3 — Designing and implementing the pack

## Engine baseline — verify before relying

Everything this guide references now ships on `main`: embedded-YAML packs
with the validating loader, fingerprint pinning, `total_pages_path` and
`raw_page_size_path` on `PageNumber`, `PaginationCursorInvalid` /
`PaginationRawPageSizeInvalid`, per-mapping `ValueFormat` (incl.
`Verbatim`), `FieldType::TimestampSecondsUtc`, `FixedValue::StrList`,
`SourcePackTable::error_path`, and `testutil::EnvVarGuard` +
`fingerprint_uncovered_columns`. Still verify rather than assume — every
safety invariant this guide names is a claim about code, and your base
branch may predate one of them:

```bash
git grep -l "raw_page_size_path\|PaginationCursorInvalid\|fingerprint_uncovered_columns" \
  crates/skardi/src/sources/providers/open_connector/
```

Zero hits for any of these means your base predates part of the baseline;
bringing the engine up to it (with its regression tests) is a
PREREQUISITE step of your milestone, not an assumption to inherit.
Documentation snapshots go stale; `git grep` does not.

## Design

### Table selection

- Read-only list actions only. The admission gate requires **complete
  terminating pagination**: if an action's pagination cannot be finished
  deterministically (upstream cursor support incomplete, unbounded
  streams), the table is deferred and its absence documented in the
  module doc and pack doc — never shipped "mostly working". (Slack
  messages/threads were deferred exactly this way in 5.2.)
- A table is a stable contract: `<pack>.<name>` IDs never change
  meaning; schema changes require a pack version bump (bindings can pin
  `source_pack_version`).

### Pagination

Match the strategy to what the executor actually emits (phase 1):

- `PageNumber { page_param, per_page_param, per_page, total_pages_path }`
  — set `total_pages_path` whenever the envelope carries an
  authoritative total (e.g. `$.paging.pages`): the short/empty-page
  heuristic silently truncates on providers that filter rows after
  paginating (permission filtering shortens non-final pages legally).
  Missing/non-numeric totals fail loudly; that is intended.
- `Cursor { cursor_param, next_cursor_path, page_size_param, page_size }`
  — termination is ONLY the end-of-collection spellings: absent (any
  missing segment), `null`, or empty string. A present non-string cursor
  fails as `PaginationCursorInvalid`; a repeated cursor fails as
  `PaginationLoop`. Use the page size the provider recommends as its
  ceiling.
- `raw_page_size_path` (mutually exclusive with `total_pages_path`): for
  gateways that filter rows AFTER paginating and report the raw page
  length (e.g. `$.pageInfo.fetched` on `github.list_repository_issues`,
  upstream oomol-lab/open-connector#228). The scan continues while the
  RAW page was full, no matter how short or empty the filtered rows are —
  the filtered count carries no termination information for such actions,
  and the heuristic would silently truncate.
- The request page size doubles as the limit-pushdown ceiling — use the
  provider's maximum.

### Filters (`FilterMapping`)

- Allowlist-only, one operator per mapping (a `(input_field, literal)`
  pair can faithfully represent exactly one operator).
- **Every string-enum push is `Inexact`.** An Exact claim leans on the
  provider rejecting out-of-domain literals instead of silently
  returning its default listing; DataFusion re-applying the predicate is
  cheap insurance. Reserve `Exact` for mappings faithful across the
  whole value domain.
- `ValueFormat` per mapping: `Verbatim` for non-timestamp inputs (it
  also refuses to push a timestamp literal in a guessed spelling — the
  predicate stays local), `Rfc3339` / `EpochSeconds` for genuine
  timestamp inputs. `EpochSeconds`
  floors sub-second literals — only map it to lower-bound parameters so
  the boundary row can never be dropped. Never map `>=` onto a provider
  parameter with strictly-greater semantics (same boundary-row rule).
- A filter you deliberately do NOT map (semantics mismatch, missing
  input, strict schema would 400 it) gets a module-doc rationale AND a
  negative-space guard test proving no such key ever reaches the wire.

### Fixed inputs and resources

- `fixed_inputs` pin the table to the complete collection when the
  provider defaults to a subset — the `state=all` move (GitHub issues),
  the `types` array pin (Slack conversations), the `includeLocale` pin
  that keeps a declared column populated. A pushed predicate on the same
  input overrides the pin.
- `required_resources` / `optional_resources` names are the OC input
  keys **verbatim** (camelCase — `issueNumber`, not `issue_number`);
  they pass through to the request untranslated and appear in user YAML.

### Fields

- Conservative nullability: identity fields non-null, everything else
  nullable. JSON-null parents on nested paths become SQL NULL for
  nullable columns (the converter's null-parent rule); a nullable column
  must still FAIL on a shape mismatch (string where object expected) —
  quiet all-NULL columns hide breaking drift.
- `FieldType` selection: `TimestampSecondsUtc` for epoch-second fields
  (the millis reader silently produces January-1970 dates),
  `TimestampMillisUtc` for RFC 3339/millis, `Utf8ListFromObjectKey` for
  `$.labels[*].name`-style plucking, `Json` for opaque objects worth
  keeping.
- Empty-string conventions (Slack `topic: ""`) stay empty strings, never
  NULL. Document which spelling of "absent" the normalizer uses
  (explicit null vs omitted key) — both become SQL NULL, but tests must
  pin each.

### In-band errors

`error_path: Some("$.error")` ONLY when the gateway forwards the
provider's in-band error envelope with HTTP 200 (the engine checks it
before row extraction). OC's own executors usually consume these and
return a failure envelope — then the pack declares `None` and an e2e
test proves the provider's code (e.g. `missing_scope`) surfaces through
the gateway-failure path.

## Implementation checklist

Work module by module; the reference packs are the style guide.

1. **`packs/<provider>.yaml` + `packs/<provider>.rs`** — packs are
   embedded YAML assets. Author the declaration in the YAML (`kind: pack`,
   `pack:`, `version:`, `tables:` keyed by bare short names — the id is
   derived as `<pack>.<table>`; per-table `action`, `row_path`,
   `row_shape`, `fingerprint`, `pagination`, `resources`, `fixed_inputs`,
   `columns`, `filters`, `error_path`; design rationale as YAML comments). The `.rs`
   module is a small accessor (`OnceLock` + `loader::builtin` +
   `include_str!`) plus the module doc and the test suite; add the
   registry entry in `source_pack.rs` builtins and a `mod` line in
   `packs/mod.rs`. The loader validates structure FOR you at parse time —
   unknown keys, duplicate columns, filters on undeclared columns,
   duplicate mappings or shared filter inputs, resource/fixed-input/
   pagination key collisions, zero page sizes, non-finite floats, and
   path validity all fail as `SourcePackAssetInvalid` diagnostics — so
   your authoring attention goes to the SEMANTIC choices the loader
   cannot check: which action, which columns, which fidelity, which
   termination signal.
   **Response shape.** `row_shape` defaults to `array`: `row_path` locates
   the row array, which is how every shipped table works. A point-read
   action whose ENTIRE response is the row — `feishu.get_document_content`,
   Notion's rendered Markdown — declares `row_shape: object` instead, and
   then three rules apply, all enforced at load time:

   - `row_path` must be exactly `"$"` (the response root). Any deeper path
     is rejected; `$` remains invalid for array tables.
   - pagination must be `single_page`. An object row IS the whole response,
     so a second page could only re-fetch the same row.
   - a response that is null, an array, or a primitive fails the scan
     loudly. It never degrades to zero rows — "this document has no
     content" would be a lie about a broken action contract.

   ```yaml
   document_content:
     action: feishu.get_document_content
     row_path: "$"
     row_shape: object
     pagination: { strategy: single_page }
     columns:
       - { name: content, path: content, type: utf8, nullable: true }
   ```

   Raw-action scans (`open_connector_scan`) do NOT accept `$`: object rows
   are a pack-declared contract whose response shape a fingerprint pins,
   and deriving a row type from the response root is a different inference
   rule. Asking for it fails at planning time with a targeted error.

2. **Fixtures** (`packs/fixtures/<provider>/*.json`) — redacted,
   provider-shaped pages covering ALL SIX admission-gate categories:
   null-bearing, null-parent, empty-list/empty-page, nested,
   extra-field, and **schema-mismatch** (a valid first row + a row with
   a declared-type violation; the contract test asserts the full
   targeted error identity: column, path, page, row, expected,
   found-kind — proving row-scoped location and value-free reporting).
3. **Fingerprints** — for each table:
   - captured contract at `fixtures/<provider>/contracts/<action>.json`
     (phase 1);
   - `expected_fingerprint: Some("<blake3-hex>")`;
   - a sync test asserting
     `fingerprint_schema(Some(&fixture)) == expected_fingerprint` for
     every table (collect all mismatches, print actual hashes — that is
     also how you obtain them the first time). The helper is
     `pub(crate)` for exactly this caller — import it as
     `use crate::sources::providers::open_connector::action_registry::fingerprint_schema;`
     from the pack's test module;
   - mock discovery serves the captured contracts (a shared
     `<provider>_discovery(path)` helper), so every e2e registration
     exercises the gate's pass side;
   - one drift-refusal e2e: a stub serving a different schema must fail
     registration with `ActionContractMismatch` naming table and action;
   - a coverage-gap pin: `testutil::fingerprint_uncovered_columns` walks
     every mapped path through the captured row-item schema, and the pack
     test asserts the exact uncovered set per table (columns riding
     `additionalProperties` passthrough sit outside the fingerprint gate;
     their drift surfaces at scan time). An empty set is the goal;
     pinning a non-empty one makes the gap a reviewed fact.
4. **End-to-end tests** via `MockGateway` (`testutil.rs` — use
   `envelope_ok` / `envelope_err` / `discovery_ok`; the mocks speak the
   real protocol: uniform envelope, `POST /v1/actions/:id`, camelCase).
   Minimum set, calibrated by the reference packs:
   - multi-page scan for EACH pagination declaration — tables sharing a
     strategy constant still get their own wire pin (row path + input
     keys), so one table's drift cannot hide behind another's coverage;
   - every termination spelling the strategy supports, plus its failure
     modes (loop, invalid cursor/total) at the pack level where
     user-visible;
   - LIMIT early-stop; empty collection; fixed-input pins asserted on
     every request body;
   - each pushed filter: pushed on the wire AND re-applied locally
     against a stub that ignores it (the harshest legal Inexact
     provider), asserting ROW IDENTITY of the survivors, not row counts;
   - negative-space guards for every deliberate absence (unmapped
     filter keys, removed marker columns);
   - resource forwarding (numeric YAML values stay numbers), required-
     resource enforcement failing before any HTTP;
   - in-band error surfacing per the error_path decision above;
   - multi-table binding + `open_connector_query` UDTF parity for at
     least one table.
   Test hygiene: parse request bodies to JSON and assert structurally
   (`input.get("cursor").is_none()`), never substring-match; use
   `EnvVarGuard` for env vars; per-test-unique token variable names.
5. **Docs** — three targets:
   - `docs/open-connector-<provider>.md` modeled on the GitHub/Slack
     docs: binding YAML, per-table reference (action, resources,
     filters), behavior bullets (pagination semantics, normalization,
     error surfacing, fingerprint pinning), authz/rate limits.
   - Tasks-spec milestone entry ticked, with the 5.1/5.2-density
     verification blurb and test counts stated WITH the counting
     command.
   - `docs/open-connector.md` status paragraph updated (supported packs
     list).
6. **Demo (optional but precedented)** — if the user wants a runnable
   walkthrough, extend `docs/open-connector/` the way the GitHub demo
   does (stub gateway speaking the real envelope; a real-gateway section
   honest about what carries over).
