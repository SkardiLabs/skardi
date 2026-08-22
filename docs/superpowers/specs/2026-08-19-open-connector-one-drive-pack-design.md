# OneDrive Source Pack (Microsoft 365)

Milestone 5.8. Phase 1–2 record for the `one_drive` source pack, plus the
phase-3 implementation decisions.

Microsoft 365 ships **one Skardi pack per Open Connector service**. There
is no `microsoft365` service upstream: Graph is split into `outlook`
(mail only), `one_drive` (files) and `excel`, each with its own OAuth
connection, and a Skardi binding carries exactly one `connection_alias`.
A cross-service pack would silently span two OAuth grants and fail half
its tables at scan time. `outlook` went first as milestone 5.7 (PR #212,
merged into main and now in this base after a rebase); this document
covers `one_drive`, which is independent of it. The phase-1/2 material was
originally reconciled alongside outlook's on 2026-08-14 (commit
`c488ef1`), narrowed out of that branch in `0a76447` so PR #212 reviewed
as exactly one pack, and restored here — then **re-verified against the
live gateway on 2026-08-19** rather than trusted: both committed contract
captures are still byte-identical to live discovery, so the gateway pin
below has not drifted.

## Service landscape

| service | actions | completely-paginating list actions | verdict |
|---|---|---|---|
| `outlook` | 21 | `list_messages`, `list_mail_folders` | 5.7, merged (PR #212) |
| `one_drive` | 13 | `list_folder_children`, `search_items` | this pack |
| `excel` | 31 | none | deferred at the gate |

`excel` is deferred whole: its list actions (`list_worksheets`,
`list_tables`, `list_table_rows`, `list_table_columns`,
`list_drive_item_children`, `search_files`) emit `nextLink` in their
output but accept **no** `nextLink` input, so their pagination cannot be
completed and no table over them can pass the admission gate. Reviving
excel needs an upstream input added, mirroring what `outlook` and
`one_drive` already do.

The other eleven `one_drive` actions are single-item reads
(`get_drive`, `get_root`, `get_item`), downloads
(`download_file`, `download_file_by_path`, `download_item_as_format`) or
writes (`create_folder`, `delete_item`, `upload_file`,
`update_file_content`, `update_item_metadata`). None is a list, and the
writes are outside a read-only pack by construction.

## Wire contract (gateway v1.3.4, open-connector `2410fbe`)

Verified live 2026-08-19. Both actions require `oauth2`
(`needsCredential: true`, `noAuthRunnable: false`).

| action | input properties | required |
|---|---|---|
| `one_drive.list_folder_children` | `driveId`, `folderItemId`, `folderPath`, `top` (1–999), `select[]`, `expand[]`, `orderBy`, `nextLink` (`format: uri`) | none |
| `one_drive.search_items` | `driveId`, `query` (`minLength: 1`), `top` (1–999), `select[]`, `expand[]`, `orderBy`, `nextLink` (`format: uri`) | none |

Both halves of each contract are committed:
`packs/fixtures/one_drive/contracts/` (output, the fingerprint input) and
`contracts/inputs/` (input, re-fetched live 2026-08-19). The fingerprint
gate is output-only — nothing reads `ActionMetadata::input_schema`, so a
renamed input key registers cleanly and then 400s every scan — and
`generated_inputs_are_accepted_by_the_captured_input_contracts` supplies
the missing half by locking every key the pack can send against the
capture, the way gmail (5.5) does. It compares committed artifacts, so it
catches drift on re-capture rather than live; an input fingerprint
compared at registration is the proper fix and stays engine work.

Inputs are `additionalProperties: false`. Verified live: `filter`,
`orderby` (lower-case), `skip`, `page` and `perPage` each 400 as
`invalid_input`. Note the camelCase asymmetry — `one_drive` accepts only
`orderBy`, while the sibling `outlook` service spells the same input
`orderby`. `top: 999` reaches the credential wall; `top: 1000` and
`top: 0` both 400, so the declared bound is a real schema bound (whether
it is a *wire* bound is a phase-4 question — feishu declared 100 and hard
failed above 50, skardi PR #186).

### Rows are raw passthrough

`executors.ts` builds both responses identically:

```ts
return { items: readCollectionItems(payload.value), nextLink: readNextLink(payload) }
```

`readCollectionItems` only re-wraps each element (`value.map(asObject)`)
— no renaming, no rebuilding — so Graph's driveItem objects arrive
verbatim, GitHub-style, and the declared row object carries
`additionalProperties: true`. In-band Graph errors are consumed by the
executor into the gateway's failure envelope, so neither table declares
`error_path`.

### Both actions declare the SAME output schema

Graph returns the same driveItem collection shape for a folder listing
and for a search, so the two captured contracts are byte-identical and
the two tables' fingerprints are **equal by construction**
(`6fc6a6b3…c207b`). This is an upstream fact, not a copy-paste slip, and
a test says so explicitly.

### Pagination

Graph's `@odata.nextLink` re-exposed as a `nextLink` input/output pair:
the cursor is a **complete URL** (`format: uri`, validated before
credentials), null on the final page. The executors ignore every other
query input once `nextLink` is set — the cursor URL embeds its own
`$top`/`$select` — and each action pins its own allowlisted path set, so
**a children cursor handed to `search_items` is rejected and vice
versa**. The executors never filter rows after paginating, so
null-cursor termination is sound. Graph publishes no has-more flag, so
no `has_more_path` override is declared.

## Table design

Both tables read `row_path: "$.items"`, paginate by cursor over
`nextLink`, and pin `page_size: 999` (the declared ceiling; driveItem
rows are metadata-only, so a full page stays orders of magnitude under
the client's 16 MiB response cap). It bounds REQUESTS rather than bytes:
`Pagination::apply` inserts `page_size` verbatim and never narrows it to
the SQL LIMIT, so a `LIMIT 10` costs exactly one request but still
transfers a full page — asserted explicitly by
`limit_stops_cursor_pagination_early`.

### `one_drive.drive_items`

Action `list_folder_children`; optional resources `driveId`,
`folderItemId`, `folderPath`. Every combination names a
completely-terminating collection — with no resource the executor lists
the drive root's children (`buildListFolderChildrenPath` →
`/root/children`) — so all three are optional.

Sixteen columns: `id`, `name`, `web_url`, `description`, `size`, `e_tag`,
`c_tag`, `created_date_time`, `last_modified_date_time`,
`created_by_display_name`, `last_modified_by_display_name`,
`parent_drive_id`, `parent_id`, `parent_path`, `file_mime_type`,
`folder_child_count`.

**Direct children only, non-recursive** — one binding sees one folder
level. That is what makes `drive_item_search` worth shipping.

### `one_drive.drive_item_search`

Action `search_items`; **required resource `query`**, optional `driveId`.
Fourteen columns: `drive_items`' sixteen minus `e_tag`/`c_tag`. The
declared schema is shared with `list_folder_children`, but phase 4 found
real search rows are a reduced Substrate projection that never carries
the concurrency tags (zero occurrences across 1800+ live hits, files and
folders alike), so mapping them here would ship two always-NULL columns.
See **Phase 4 results** below; the pairwise relation is pinned by
`search_columns_are_drive_items_minus_the_two_concurrency_tags`.

The requirement is real but not where it looks. The input schema's
`required` array is EMPTY and `query` is merely `minLength: 1`, which
splits into three live-verified behaviours:

| input | result |
|---|---|
| `{"query": "", "top": 200}` | 400 `invalid_input` (schema layer, `minLength`) |
| `{"top": 200}` — omitted | passes validation, dies in the executor: `ProviderRequestError(400, "query is required")` |
| `{"query": "   ", "top": 200}` | passes validation (length 1+), dies in the same executor trim check |

Declaring `query` a required resource stops Skardi ever generating the
OMITTED row — that binding is refused at registration. It does not stop
the other two: resource validation checks presence and non-null only
(`mod.rs` `contains_key` plus the config layer's null check), so
`query: ""` and `query: "   "` register cleanly and fail at scan time on
the upstream 400s above. Loud either way, but only the omitted case is
caught at config time. Trimming resource values would be an engine-wide
policy change (it would affect every pack), so it is deliberately not a
pack-level decision. Notion's empty-query trick does not transfer:
there is no spelling of "search the whole drive", so the term is a
resource (the binding pins it; the table is "the drive items matching
this binding's query" — the same semantics as `notion.block_children`
requiring a `blockId`) rather than a fixed input.

## Two decisions where this pack diverges from `outlook`

**No `select` pin, and the reason is coverage.** Every mapped column of
both tables (16 on `drive_items`, 14 on `drive_item_search`) resolves
INSIDE the declared item schema — including the nested
`createdBy.user.displayName`, `parentReference.path`, `file.mimeType` and
`folder.childCount` paths — so the fingerprint gate covers every one of
them and the coverage-gap pin is EMPTY. Loudness then comes from two
separate mechanisms, worth keeping apart: an UPSTREAM rename/retype
changes the hash and fails registration, while a mapping typo in this
pack leaves the hash intact and is caught by the coverage test in CI
instead. `outlook.messages` pins `select` precisely
to buy that loudness for its thirteen UNDECLARED columns (and to bound
`body.content`); neither reason applies here, since driveItem rows carry
metadata only.

**Item type is facet presence, not a field.** Graph marks a driveItem's
kind by which facet is present, so `file_mime_type` and
`folder_child_count` double as the discriminator: non-null means file /
means folder. Mapping one scalar out of each facet keeps that queryable
without a JSON column. `childCount: 0` must survive as 0 rather than
collapse to NULL, or "empty folder" becomes indistinguishable from "not a
folder".

Seven other facets stay unmapped — `root`, `deleted`, `shared`,
`specialFolder`, `remoteItem`, `searchResult`, `fileSystemInfo` — all
declared as bare open objects (`properties` absent, so any child path
would be passthrough anyway). Each is presence-as-signal rather than
data; `searchResult` specifically is an opaque relevance hint.
`createdBy`/`lastModifiedBy` are identitySets with user/application/
device arms, of which only the user arm's display name answers "who
touched this" — an application-authored row therefore reads NULL there,
deliberately.

## Filters: none, structurally

Neither action exposes a filter input at all, so there is nothing to map
and the absence is not an omission (contrast `outlook.list_messages`,
which offers `filter` as a raw OData *expression* string that a
`(input_field, literal)` mapping cannot compose). Predicates are
re-applied locally by DataFusion after a bounded scan; the practical
scoping tools are the folder resources and `LIMIT` early-stop. Each
unmapped input has a negative-space guard test proving no `filter`,
`orderBy`, `orderby`, `select`, `expand`, `skip` or `perPage` key ever
reaches the wire.

## Authz and rate limits

**The OAuth consent is read-write for a read-only pack.** The scope union
the gateway requests for `one_drive` is `User.Read`, `Files.ReadWrite`,
`offline_access` — `Files.ReadWrite` because the same connection serves
the service's upload/delete actions. Skardi's tables remain read-only by
construction (`register_open_connector_tables` rejects `read_write`), but
the user is consenting to more than Skardi will use, and the pack doc
must say so plainly. A narrower `Files.Read` union is an upstream ask.

`one_drive` is authorized SEPARATELY from `outlook`: the sibling grant
does not cover it, and a live probe with valid inputs answers
`403 "Connect one_drive with OAuth first."` until it is granted.

Graph enforces per-drive throttling with HTTP 429 plus `Retry-After`.

## What phase 4 must settle

Live verification is not a formality here. The list below is the plan as
written before the pass; it **ran on 2026-08-21** — results follow it,
item by item. It needed the user's own Microsoft account and Azure app
(an Entra app registration with `http://localhost:3000/oauth/callback`
as a redirect URI, `PUT /api/oauth/configs/one_drive` with their
`clientId`/`clientSecret`, and a browser authorization). Credentials
stayed entirely on the user's side.

1. Whether `top: 999` is a wire bound as well as a declared one.
2. Whether the real final page returns a genuinely null `nextLink`, so
   null-cursor termination cannot refetch and trip the loop guard the way
   feishu wiki did.
3. Whether a real cursor round-trips the executor's host/path allowlist —
   and specifically whether the search cursor's
   `/search(q='…')` parenthesized form survives it, since the exact
   analogous shape is what broke outlook's folder-scoped pagination
   (Graph returns `mailFolders('{id}')/messages`, which the gateway's own
   allowlist rejects). **Distinguish the two failure modes**, because only
   one is safe: a rejected link must surface as an ERROR. If the executor
   instead sanitizes it away and returns `nextLink: null`, the engine
   cannot tell that from end-of-collection (`pagination.rs` treats a null
   cursor as done, and Graph publishes no has-more flag to cross-check),
   so page one would be reported as the whole collection — silent
   truncation with a green status, the one outcome the admission gate
   exists to prevent. Scan a folder with more rows than `top`, confirm
   page 2 is actually requested, and confirm a bad cursor errors.
4. Whether every mapped column carries a non-NULL value somewhere,
   including the two identity display names and all three
   `parentReference` paths.
5. Whether a real `folderItemId`/`folderPath` forwards verbatim, and a
   real `query` returns rows.
6. Re-derive both row fixtures as redacted live captures (they are
   SYNTHETIC today). The audit test guarding them,
   `fixtures_stay_synthetic_under_a_default_deny_audit`, is already
   key-scoped default-deny with self-trip probes, so this tightens its
   per-key arms rather than rewriting it.
7. Whether `folderItemId` and `folderPath` set TOGETHER is a real
   configuration: both are declared and optional, so a binding can carry
   both, and which one the executor honours is unprobed. If it silently
   prefers one, that binding scans a folder the operator did not name.

## Phase 4 results (2026-08-21, real personal MSA drive)

Raw probes through the live gateway plus end-to-end skardi-server scans
(registration through the live fingerprint gate, real bindings for the
root drive, a `folderItemId` scope and two search terms). Item by item:

1. **Wire bound confirmed.** A real `top=999` request answered a full
   200 page; 1000 and 0 both 400 (already known from phase 1).
2. **Explicit null confirmed** on real terminal pages of both actions —
   no refetch, no loop-guard trip.
3. **Split verdict, both halves loud.** Children cursors round-trip the
   allowlist in all three path forms (root, `folderItemId`, `driveId`)
   with real multi-page walks. The search cursor's `/search(q='…')`
   parenthesized form PASSES the gateway's allowlist and is forwarded
   byte-identically — but Graph itself then fails the continuation
   server-side on a personal drive ("Error Calling Substrate Search",
   deterministic across retries and across `/me/drive` and
   `/drives/{id}` forms): an upstream Microsoft limitation, surfaced as
   a loud provider_error through the failure envelope, NOT a silent
   truncation. Searches whose hits fit one page (≤ `top` = 999)
   terminate on a clean null and succeed. The dangerous failure mode —
   a rejected link nulled into "end of collection" — does not exist:
   cross-action cursors were probed in both directions and answer 400
   `invalid_input` naming the allowlist ("nextLink must target OneDrive
   search/children pagination endpoints").
4. **All but one column witnessed non-NULL, and one table changed.**
   Every `drive_items` column extracted a real value somewhere except
   `description` (zero occurrences across 2800+ rows; kept, since it is
   declared in-schema and drift stays loud — the caveat is recorded in
   the yaml and pack doc). Search rows are a reduced Substrate
   projection that NEVER carries `eTag`/`cTag`/`isAuthoritative` (zero
   across 1800+ hits), so `drive_item_search` dropped `e_tag`/`c_tag`
   (16 → 14 columns) — the wire wins over the declared contract, and
   this is exactly the always-NULL defect class phase 4 exists to catch.
   Also witnessed: a `remoteItem` stub row (Personal Vault) with neither
   type facet and no `webUrl`; search identities are displayName-only;
   search `parent_drive_id` is lowercase without the leading zero while
   children rows carry the `0…` form (join caveat, in the pack doc);
   `parent_path` spelling varies row-to-row (raw vs percent-encoded).
5. **Confirmed.** Real `folderItemId` and `folderPath` values forwarded
   verbatim and returned real rows; real queries returned real rows
   (with two search quirks recorded in the pack doc: content matching
   beyond filenames, and non-exhaustive recall).
6. **Done.** Both row fixtures are redacted live captures now — each row
   mirrors a real wire row key-for-key under a deterministic redaction
   map, enforced by the renamed
   `fixtures_are_redacted_captures_under_a_default_deny_audit`
   (per-key arms tightened to the captured keys; tripwire probes are
   the leak classes the real captures actually contained, including a
   real ordinal-row `eTag` whose zero runs defeat naive repeated-window
   checks and a `tempauth` bearer URL). The type-mismatch fixture stays
   synthetic on purpose — it encodes a contract violation no capture
   can produce.
7. **`folderItemId` wins silently.** Both set together scans the id's
   folder and the path is dead configuration — verified live and
   structural in the executor (`buildListFolderChildrenPath` checks
   id → path → root). Recorded in the yaml and pack doc.

Provider API version: Microsoft Graph `v1.0`, pinned by the executors in
every URL including the cursors Graph hands back.
