# Phases 2–3 — Designing and implementing the pack

## Engine baseline — verify before relying

This guide references engine capabilities that landed with milestone 5.2
(PR #172); a base branch that predates it lacks ALL of them. Before
designing anything, verify your baseline:

```bash
git grep -l "PaginationCursorInvalid\|total_pages_path\|ValueFormat\|TimestampSecondsUtc\|EnvVarGuard" \
  crates/skardi/src/sources/providers/open_connector/
```

Zero hits means the base still has the pre-5.2 engine, where — most
dangerously — a non-string cursor and every row-path failure read as
"scan complete": a drifted gateway truncates results silently. In that
case, bringing the engine up to this baseline (with its regression
tests) is a PREREQUISITE step of your milestone, not an assumption to
inherit. More generally: every safety invariant this guide names is a
claim about code — verify it exists (and its failure-mode test passes)
on YOUR branch before leaning on it. Documentation snapshots go stale;
`git grep` does not.

The 5.2 baseline features referenced below: `total_pages_path` on
`PageNumber`, `PaginationCursorInvalid`, per-mapping `ValueFormat`,
`FieldType::TimestampSecondsUtc`, `FixedValue::StrList`,
`SourcePackTable::error_path`, `testutil::EnvVarGuard`.

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
- `ValueFormat` per mapping (`Rfc3339` / `EpochSeconds`). `EpochSeconds`
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

1. **`packs/<provider>.rs`** — table statics, a
   `pub(crate) static <PROVIDER>_PACK: SourcePack`, registry entry in
   `source_pack.rs` builtins (short-name uniqueness test will catch
   collisions). Module doc records every design decision with rationale.
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
     registration with `ActionContractMismatch` naming table and action.
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
