# Dropbox Source Pack (milestone 5.5)

**Status:** Draft for review — no implementation started
**Date:** 2026-08-16
**Branch:** `feature/open-connector-dropbox-pack`
**Gateway reconciled against:** oomol-lab/open-connector **v1.3.5** (source
read; live probe still outstanding — see [Open questions](#open-questions))

## Summary

A Dropbox source pack exposing three read-only tables — `dropbox.files`,
`dropbox.shared_links`, `dropbox.file_search` — over Open Connector's
Dropbox provider, plus one backward-compatible **engine extension**
(`pagination.continuation`) without which two of the three tables cannot
pass the admission gate at all.

The headline finding of phase-1 reconciliation: **Dropbox's list actions
paginate through a *different action ID* than the one that starts the
listing.** `dropbox.list_folder` begins a listing; pages 2..N come from
`dropbox.list_folder_continue`, whose input schema accepts *only* a
cursor. Skardi's scan engine issues every page against a single
`action_id` and injects the cursor into the same input map, so today it
cannot page Dropbox at all — and because `list_folder`'s input schema is
`additionalProperties: false`, the attempt would be a hard HTTP 400 on
page 2 rather than a quiet truncation.

The second finding: `list_folder` returns a **non-empty cursor on the
final page** (the executor's `requireString(payload.cursor)` makes the
field mandatory). Cursor-spelling termination would refetch and fail as
`PaginationLoop`. The engine already has the fix — `has_more_path`, added
for Feishu's wiki spaces in 5.4 — so this one costs a declaration, not
code.

## Reconciliation evidence (phase 1)

Read from the v1.3.5 checkout, which the skill designates the row-shape
authority: `src/providers/dropbox/{actions,executors,definition,scopes}.ts`.

### Rows are normalized, not passthrough

Every list executor maps rows through `mapDropboxMetadata` (executors.ts
:797), which rebuilds each entry into a **fixed camelCase shape** with
every key present:

```
tag, name, id, pathDisplay, pathLower, clientModified, serverModified,
rev, sizeBytes, isDownloadable, contentHash, url, expiresAt,
sharingInfo, linkPermissions
```

This is Slack-style normalization, and it matters: `dropboxMetadataSchema`
is declared with `s.object` (→ `additionalProperties: false`, json-schema.ts
:72) and lists all fifteen keys as `required`. So unlike GitHub/Notion/
Feishu passthrough rows, **every mapped column here sits inside the
fingerprint gate**. The coverage-gap pin should come back empty, and
phase 4's column risk drops from "the declared contract is not column
truth" to "which columns are structurally always-NULL for this action".

Mapping Dropbox's raw snake_case (`path_display`, `client_modified`,
`size`) would have produced all-NULL columns — the exact 5.2 failure mode.

### Pagination, per action

| Action | Continuation | Cursor on final page | Terminates on |
|---|---|---|---|
| `list_folder` | **separate action** `list_folder_continue` (cursor-only input) | non-empty (`requireString`) | `hasMore: false` only |
| `search_files` | **separate action** `search_files_continue` (cursor-only input) | `?? null` | `hasMore: false`, cursor also nulls |
| `list_shared_links` | same action, `cursor` input | `?? null` | `hasMore: false`, cursor also nulls |
| `list_revisions` | none — manual `beforeRev` paging | n/a | `hasMore` with nothing to follow |

`hasMore` is `readBoolean(payload.has_more) ?? false` in every one, so the
signal is always a present boolean — safe to declare as authoritative.

### Errors and rate limits

`dropboxRpcRequest` (executors.ts:702) throws `normalizeDropboxHttpError`
on any non-2xx, so Dropbox's in-band `error_summary` envelope and its
`429` rate-limit responses both surface as **gateway failure envelopes**,
never as HTTP 200 rows. Every table therefore declares `error_path: None`,
matching the Slack/Notion/Feishu precedent, and an e2e test pins that a
Dropbox error code surfaces through the gateway-failure path.

### Auth and scopes

OAuth2 only (definition.ts), `token_access_type: offline`. The pack needs
exactly two of the six declared scopes:

- `files.metadata.read` — `files`, `file_search`
- `sharing.read` — `shared_links`

No content scopes (`files.content.*`) and no write scopes are required by
any shipped table. That is a documentable authz boundary: a connection
scoped read-only still serves the whole pack.

## Engine extension: `pagination.continuation`

Required for `files` and `file_search`. Backward-compatible: absent →
today's behavior, byte for byte.

```yaml
pagination:
  strategy: cursor
  cursor_input: cursor
  next_cursor_path: "$.cursor"
  page_size_input: limit
  page_size: 2000
  has_more_path: "$.hasMore"
  continuation:                                  # NEW, optional
    action: dropbox.list_folder_continue         # default: the table's own action
    fingerprint: <blake3-hex>                    # required when `action` is set
    inputs: cursor_only                          # cursor_only | full (default: full)
```

Semantics:

- **Page 1** — unchanged: the table's `action`, with resources + fixed
  inputs + pushed filters + page-size input.
- **Pages 2..N** — issued against `continuation.action` (defaulting to the
  table's own action). With `inputs: cursor_only` the request body is
  *exactly* `{<cursor_input>: <token>}`: no resources, no fixed inputs, no
  pushed filters, no page size. This is not a preference — the continue
  actions' schemas are strict and declare `cursor` as their only property,
  so anything else is a 400.
- Loop detection, `max_pages`, the scan deadline, LIMIT pushdown and
  `has_more_path` are untouched. `page_size` still doubles as the
  limit-pushdown ceiling; Dropbox sizes continuation pages from the
  original request, so the ceiling stays honest.

Code shape (three touch points, all small):

1. `PaginationStrategy::Cursor` gains `continuation: Option<Continuation>`;
   `validate()` rejects `inputs: cursor_only` combined with a `PageNumber`
   strategy and validates the action-ID spelling.
2. `Pagination` exposes `continuation() -> Option<(&str action, &str
   cursor_param, &str token)>` when past page 1; `exec.rs::next_page`
   branches on it to choose the action ID and to build a cursor-only input
   instead of the assembled map.
3. Registration fingerprints **both** actions. A continue action serving
   pages 2..N is exactly as exposed to contract drift as the first — gating
   only the primary would leave most of a large scan unguarded — so
   `continuation.fingerprint` is mandatory whenever `continuation.action`
   is set, and a drift-refusal e2e covers it.

The `inputs` knob is deliberately separate from `action` rather than
implied by it: `list_shared_links` continues through the *same* action and
may still need cursor-only bodies (see [Open questions](#open-questions)),
and a future provider may continue through a separate action that wants
its filters repeated.

### Alternatives rejected

- **Defer `files` and `file_search`, ship only `shared_links`.** Gate-clean
  and zero engine risk, but a Dropbox pack that cannot list a folder is not
  a Dropbox integration. Kept as the fallback if the extension is refused.
- **Upstream: teach `dropbox.list_folder` to accept a `cursor`.** Precedent
  exists (oomol-lab/open-connector#228 added `pageInfo.fetched` for the
  GitHub pack), and it would erase the extension entirely. Rejected as the
  *primary* path because it blocks this milestone on an upstream merge —
  but worth filing as a follow-up regardless, since it would benefit every
  Open Connector consumer. Note it does not fully subsume the extension:
  `search_files_continue` has the same split shape.

## Tables

### `dropbox.files`

| | |
|---|---|
| Action | `dropbox.list_folder` → `dropbox.list_folder_continue` |
| Row path | `$.entries` |
| Pagination | cursor, `has_more_path: $.hasMore`, `page_size: 2000` (schema max), `continuation` cursor-only |
| Optional resource | `path` (folder to list; omitted → account root) |
| Fixed inputs | `recursive: true`, `includeMountedFolders: true`, `includeDeleted: false` |
| Filters | none pushed (see below) |

Columns (12): `tag` (utf8, **not null**), `name` (utf8, **not null**),
`id`, `path_display`, `path_lower`, `rev`, `content_hash` (utf8, nullable);
`client_modified`, `server_modified` (`timestamp_ms_utc` — the executor
emits ISO 8601 strings); `size_bytes` (int64); `is_downloadable`
(boolean); `sharing_info` (json).

Decisions to record in the module doc:

- **`tag` and `name` are non-null** because the executor guarantees them
  (`resolveDropboxMetadataTag` always returns a string; `name` is
  `?? ""`). A null arriving there is drift and should fail the scan, not
  produce a quiet NULL.
- **`recursive: true` is pinned** — this is the `state=all` move from 5.1.
  A SQL table named `files` that returns one directory level is a
  surprising contract; pinned recursion makes `dropbox.files` mean "every
  file under `path`". Cost: a scan of a large account is genuinely large,
  bounded by `max_pages` and the scan deadline like every other pack.
- **`includeMountedFolders: true` is pinned** for the same completeness
  reason (Dropbox's own default, pinned so it cannot drift).
- **`includeDeleted: false` is pinned** — deleted tombstones carry a
  `deleted` tag and null everything else; admitting them would populate
  the table with rows that fail no test and inform no query. Documented as
  out of scope rather than silently defaulted.
- **`url`, `expires_at`, `link_permissions` are deliberately omitted.**
  They exist in `mapDropboxMetadata` but are sourced from
  `record.url` / `record.expires` / `record.link_permissions`, which
  `files/list_folder` never returns — mapping them would ship three
  structurally always-NULL columns. Negative-space guard test pins their
  absence from the schema.
- **No pushed filters.** `list_folder`'s remaining inputs (`recursive`,
  `includeDeleted`, `includeMountedFolders`, `limit`) are scan-shape
  controls, not column predicates; `path` is a resource, not an equality
  on `path_lower` (it selects the listing root, and a `path_lower = '/a/b'`
  predicate means something different from "list under `/a/b`"). Guard
  test proves no filter key ever reaches the wire.

### `dropbox.shared_links`

| | |
|---|---|
| Action | `dropbox.list_shared_links` (same action continues) |
| Row path | `$.links` |
| Pagination | cursor, `next_cursor_path: $.cursor`, `has_more_path: $.hasMore`, no page-size input |
| Optional resources | `path`, `directOnly` |
| Fixed inputs | none |
| Filters | none pushed |

Columns (15): the full `mapDropboxMetadata` set — the shared-link-only
fields (`url`, `expires_at`, `link_permissions`) populate here, which is
precisely why they belong to this table and not to `files`.

`url` is mapped **nullable** even though it is the natural identity: the
executor spells it `optionalString(record.url) ?? null`, so a non-null
declaration would fail scans on a row the gateway considers legal.
Documented rather than "fixed" by tightening.

`path` stays an optional resource rather than an `eq` push onto
`path_lower`: the input accepts paths, file IDs *and* rev IDs, so the
mapping would be unfaithful across most of its value domain — an `Exact`
claim would be wrong and an `Inexact` one would still push a rev ID as if
it were a path. Guard test pins that no `path` key is derived from a
predicate.

**This table needs no engine extension** and is the fallback pack if the
extension is refused.

### `dropbox.file_search`

| | |
|---|---|
| Action | `dropbox.search_files` → `dropbox.search_files_continue` |
| Row path | `$.matches` |
| Pagination | cursor, `has_more_path: $.hasMore`, `page_size_input: maxResults`, `page_size: 1000` (schema max), `continuation` cursor-only |
| Required resource | `query` |
| Optional resource | `path` |
| Fixed inputs | `fileStatus: active` |
| Filters | none pushed |

Columns (13): `match_type` (utf8, **not null** — the executor's
`?? "unknown"`), then the metadata block at nested paths
`metadata.tag`, `metadata.name`, `metadata.id`, `metadata.pathDisplay`,
`metadata.pathLower`, `metadata.clientModified`, `metadata.serverModified`,
`metadata.rev`, `metadata.sizeBytes`, `metadata.isDownloadable`,
`metadata.contentHash`, `metadata.sharingInfo`.

- **`query` is a required resource**, the GitHub `owner`/`repo` precedent:
  a search table without a query is not a table. Required-resource
  enforcement must fail before any HTTP.
- **`fileStatus: active` is pinned** for the same reason `includeDeleted`
  is pinned off on `files`.
- **`highlight_spans` is not mapped and `includeHighlights` is never
  sent.** Two reasons: the field only populates when highlights are
  requested, and the declared schema (`s.nullable(s.array(...))`)
  contradicts the executor (`readObjectArray(...)`, which returns `[]`,
  never null) — a declared-vs-wire contradiction worth recording but not
  worth a column. Negative-space guard on the input key.
- **`orderBy` is left unpinned.** Feishu pinned `ByCreateTimeAsc` because
  its default ordering reshuffles mid-scan; Dropbox's `search/continue_v2`
  pages a server-side snapshot taken at the first call, so relevance order
  is stable within one scan. To be confirmed live.

## Deferred, with reasons

Recorded in the module doc and the pack doc as *absent by decision*, per
the admission gate:

- **`list_revisions`** — pages by passing a `beforeRev` from the previous
  page's rows and answers `hasMore` with no cursor to follow. No pack-side
  strategy can complete it, and `limit` caps at 100. This is the 5.2 Slack
  message-history deferral repeated.
- **`get_current_account`** — returns one object; `RowPath::rows` requires
  an array (row_path.rs:105). A single-row table would need a different
  engine concept.
- **`get_tags`** — its input is an array of paths (resources are scalars)
  and it declares no pagination.
- **All write actions** (`upload_file`, `create_folder`, `move`, `copy`,
  `delete`, `create_shared_link`, `modify_shared_link`,
  `revoke_shared_link`, `save_url`, `restore`) — outside the read-only
  allowlist.
- **Content actions** (`download_file`, `get_temporary_link`,
  `get_shared_link_file`) — base64 payloads and single objects, not rows.

## Verification plan

**Phase 3 (implementation).** Six fixture categories per table including
schema-mismatch; `expected_fingerprint` per table plus
`continuation.fingerprint` for the two split-action tables, each locked to
its captured contract by a sync test; mock discovery serving the captured
contracts; a drift-refusal e2e per table *and* one for a continue action;
a `fingerprint_uncovered_columns` pin (expected empty, given the strict
normalized schema — a non-empty result is itself a finding). E2e set:
multi-page scan per table, every termination spelling,
`PaginationHasMoreInvalid` / `PaginationCursorInvalid` failure modes,
LIMIT early-stop, empty collection, fixed-input pins asserted on the
request body, required-resource enforcement before HTTP, negative-space
guards for every deliberate absence above, gateway-failure surfacing, and
UDTF parity for one table. Plus, new here: an e2e asserting the
continuation request body is **exactly** `{"cursor": …}` — the assertion
that would have caught the 400 this whole extension exists to avoid.

**Phase 4 (live).** Needs from you: a Node runtime on this box (none
installed — `node`/`npm` are absent, which is why phase 1 stopped at
source) and a free Dropbox account with an OAuth app carrying
`files.metadata.read` + `sharing.read`. Then: probe each action at the
pack's exact inputs and at its declared bounds, diff real row keys against
mapped columns in both directions, force multi-page pagination with a
small `limit`, confirm every mapped column extracts a non-NULL value
somewhere, confirm termination on the real final page, and re-derive the
fixtures as redacted live captures.

**Phase 5.** `cargo fmt`, `cargo clippy`, the full `cargo test -p skardi
--lib` (the engine change ripples beyond the pack), counted tests matching
the docs, and a `/code-review` pass on the diff.

## Open questions

1. **`list_shared_links` with both `path` and `cursor`.** The executor
   `compactObject`s them together, but Dropbox may reject the combination.
   If it does, this table needs `continuation: {inputs: cursor_only}` as
   well — which the proposed design already spells without a code change.
   Resolvable only on the live wire.
2. **`list_folder` cursor lifetime.** Dropbox cursors can expire or be
   invalidated mid-listing; a long recursive scan may hit it. Behavior and
   whether it warrants a documented bound is a phase-4 observation.
3. **Whether `recursive: true` is the right pin** — the alternative is
   pinning it off and making the subtree the user's choice via a second
   table. Flagging because it is the single most consequential contract
   decision in this pack.
4. **Engine-extension appetite.** If `pagination.continuation` is not
   wanted in this milestone, the honest fallback is a one-table pack
   (`shared_links`) with `files` and `file_search` deferred as
   gate-failing — worse, but not misleading.

## Scope of this milestone

- `crates/skardi/src/sources/providers/open_connector/pagination.rs`,
  `exec.rs`, `packs/loader.rs`, `source_pack.rs` — the continuation
  extension and its registration gating.
- `packs/dropbox.yaml`, `packs/dropbox.rs`, `packs/mod.rs`,
  `packs/fixtures/dropbox/**`.
- `docs/open-connector-dropbox.md`, the 5.5 entry in
  `2026-07-11-open-connector-integration-tasks.md`, and the supported-pack
  list in `docs/open-connector.md`.
