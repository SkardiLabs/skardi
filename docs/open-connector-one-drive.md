# OneDrive Source Pack (Microsoft 365)

Microsoft 365 reaches Skardi as **one source pack per Open Connector
service**. There is no `microsoft365` service upstream: the gateway
splits Microsoft Graph into `outlook` (mail only), `one_drive` (files)
and `excel` (spreadsheets), each with its **own OAuth connection** — and
a Skardi binding carries exactly one connection alias, so a
cross-service pack would silently span two OAuth grants and fail half
its tables at scan time when only one service is connected.

This document covers the built-in **`one_drive` pack** — `drive_items`
and `drive_item_search` over a drive. The `outlook` pack ships as its own
milestone, pack and document, and **authorizing one does not authorize
the other**. The whole `excel`
service is deferred at the source-pack admission gate (its list actions
emit a `nextLink` continuation but accept no `nextLink` input, so their
pagination cannot be completed — a table over such an action would
present page one as the whole collection).

> **Status: live-verified.** The wire contract below is reconciled
> against a live gateway (v1.3.4, open-connector at `2410fbe`; probed
> 2026-08-19), and the live-data pass ran on 2026-08-21 against a real
> personal (MSA) drive — raw probes plus end-to-end skardi-server scans
> of every table through the live fingerprint gate. The pass changed the
> pack once: **`drive_item_search` dropped `e_tag`/`c_tag`** (no search
> hit on that drive carried them), leaving 16 columns on `drive_items`
> and 14 on the search table. Full findings in
> [Live verification](#live-verification).

**The wire contract is Open Connector's, not Microsoft Graph's**: the
gateway's one_drive executors pass Graph's objects through **raw**
(GitHub-style), so rows are genuine Graph `driveItem` resources. Unlike
the sibling `outlook` pack, every column this pack maps is also
*declared* upstream, so contract drift in any of them is caught at
registration rather than surfacing as a silently-empty column. Inputs are
the gateway's camelCase strict schema (`top`, `nextLink`, `driveId`,
`folderItemId`, `folderPath`, `query`; a wrong key is a hard 400 — and
note `orderBy` is camelCase here while the sibling `outlook` service
spells the same input `orderby`).

## Binding

```yaml
spec:
  data_sources:
    - name: saas
      type: open_connector
      connection_string: http://open-connector:3000
      hierarchy_level: catalog
      open_connector:
        runtime_token_env: OPEN_CONNECTOR_TOKEN
        bindings:
          - name: drive                    # schema name in SQL
            source_pack: one_drive
            connection_alias: my-msft-drive  # the gateway's one_drive OAuth connection
            resource:                      # omit entirely for the drive root
              folderItemId: "01ABCDEF..."      # scope drive_items to one folder
            tables: [drive_items]

          # drive_item_search needs its own binding: `query` is required,
          # and the binding is what pins the search term.
          - name: drive_budget
            source_pack: one_drive
            connection_alias: my-msft-drive
            resource:
              query: budget
            tables: [drive_item_search]
```

```sql
-- What is in this folder, biggest first (files have a mime type):
SELECT name, size, last_modified_date_time, last_modified_by_display_name
FROM saas.drive.drive_items
WHERE file_mime_type IS NOT NULL
ORDER BY size DESC
LIMIT 20;

-- Subfolders and how full they are (folders have a child count):
SELECT name, folder_child_count
FROM saas.drive.drive_items
WHERE folder_child_count IS NOT NULL
ORDER BY folder_child_count DESC;

-- Search spans the whole drive, unlike a folder listing. The schema is
-- the BINDING name, so this reads the `drive_budget` binding above:
SELECT name, parent_path, last_modified_date_time
FROM saas.drive_budget.drive_item_search
ORDER BY last_modified_date_time DESC;
```

## Tables

The two tables read the same *declared* Graph `driveItem` shape, but the
real wire differs: search rows are a reduced projection (served by a
different backend, see below), so `drive_items` carries the **sixteen
columns** in the table and `drive_item_search` carries **fourteen** —
the same set minus `e_tag`/`c_tag`.

| column | wire path | type |
|---|---|---|
| `id` | `id` | utf8, non-null |
| `name` | `name` | utf8 |
| `web_url` | `webUrl` | utf8 |
| `description` | `description` | utf8 |
| `size` | `size` | int64 |
| `e_tag` | `eTag` | utf8 — `drive_items` only |
| `c_tag` | `cTag` | utf8 — `drive_items` only |
| `created_date_time` | `createdDateTime` | timestamp(ms, UTC) |
| `last_modified_date_time` | `lastModifiedDateTime` | timestamp(ms, UTC) |
| `created_by_display_name` | `createdBy.user.displayName` | utf8 |
| `last_modified_by_display_name` | `lastModifiedBy.user.displayName` | utf8 |
| `parent_drive_id` | `parentReference.driveId` | utf8 |
| `parent_id` | `parentReference.id` | utf8 |
| `parent_path` | `parentReference.path` | utf8 |
| `file_mime_type` | `file.mimeType` | utf8 |
| `folder_child_count` | `folder.childCount` | int64 |

**File or folder is a facet, not a field.** Graph marks an item's kind by
which facet is present, so those last two columns are the discriminator:
a non-null `file_mime_type` means file, a non-null `folder_child_count`
means folder (`0` for an empty one — distinct from NULL, which means "not
a folder"). The discriminator has one live-witnessed gap: a `remoteItem`
stub (OneDrive's Personal Vault appears as one) carries **neither facet
and no `webUrl`**, so such a row is NULL in all three columns. `e_tag`
changes on any change, `c_tag` only on a *content* change, so the pair
separates a rename from a real edit — children rows carried both on every
live row, while search hits on the same drive carried neither, which is
why the search table drops them (scope and caveat under
[`drive_item_search`](#drive_item_search)).

`description` is declared upstream and stays mapped (so drift stays
loud), but **2800+ live rows never carried it** — an all-NULL
`description` on a personal drive is the expected shape, not the
always-NULL bug the live pass exists to catch. Drives that set
descriptions will populate it.

Both display-name columns read only Graph's **user** identity arm: a row
created by an application or device is NULL there by design, not by
accident. (System rows like Personal Vault show as `System Account`.)

Unmapped on purpose: `root`, `deleted`, `shared`, `specialFolder`,
`remoteItem`, `searchResult` and `fileSystemInfo` are all declared
upstream as bare open objects and carry presence-as-signal rather than
data. Graph's directory IDs under the identity sets are omitted too —
they are opaque without a directory join this pack cannot perform. Real
rows also carry **undeclared passthrough extras** the pack deliberately
ignores: `isAuthoritative`, `@microsoft.graph.downloadUrl` (a short-lived
bearer-token URL — mapping it would put a credential in query results)
and `file.hashes` on children rows; `commentSettings`, `image` and
`photo` on search rows.

### `drive_items`

Action `one_drive.list_folder_children`. Optional resources: `driveId`
(a non-default drive), `folderItemId` or `folderPath` (one folder). With
no resource at all, the table is the **drive root's children**.

`folderItemId` and `folderPath` are alternatives, and **Skardi refuses a
binding that sets both** — the error names the table and both keys, and
it fires at registration, before any request. Upstream would accept such
a binding and **silently prefer `folderItemId`** (verified live, and
structural in the executor source, which checks id → path → root in that
order), scanning the id's folder while the path became dead
configuration: a successful scan of a folder you did not name. Skardi is
deliberately stricter here rather than inheriting a precedence, because
neither key winning is right — a binding carrying both has stated two
different scopes. Set exactly one. `driveId` is not part of the choice
and composes with either. A `folderPath` is a path from the drive
root and must start with `/`, e.g. `folderPath: "/Documents/Finance"`;
upstream enforces only "non-empty", so a slash-less path is accepted at
validation and then fails.

**Direct children only — the listing is not recursive.** One binding sees
one folder level; `folder_child_count` reveals that subfolders have
contents without enumerating them. This is a documented limitation of the
table, not a defect: the collection it claims to be does terminate
completely — upstream, that is; the deployment's own scan bounds still
apply, see [Pagination](#pagination). To see a whole drive, use
`drive_item_search`.

### `drive_item_search`

Action `one_drive.search_items`. **Required resource: `query`.** Optional:
`driveId`. The binding pins the search term, so the table is "the drive
items matching this binding's query" — the same shape as Notion's
`block_children` requiring a `blockId`. There is no spelling of "search
everything", so there is no complete-drive-listing table; bind one search
per term you care about.

**A blank `query` fails at scan time, not at startup.** Omitting the key
entirely (or setting it to `~`) is refused at registration, naming the
binding. But resource values are checked for presence, not for content,
so `query: ""` and `query: "   "` both start up cleanly and then fail
every scan with the gateway's own 400 (`invalid_input` for the empty
string, `query is required` for whitespace — upstream validates the
length but the executor trims). The failure is loud either way, never an
empty table, but a typo'd blank query surfaces later than you would
expect.

Search is served by a different Microsoft backend (Substrate Search)
than folder listings, and the live pass pinned four consequences:

- **Reduced rows.** On the personal (MSA) drive the pass covered, search
  hits carried no `eTag`/`cTag` at all — zero occurrences across 1800+
  hits, files and folders alike — which is why the search table maps 14
  columns. The declared schema does list both, and a
  business/SharePoint-backed drive is unprobed, so read this as a strong
  observation about the Substrate projection rather than a contract
  guarantee. Search hits carry no identity emails either;
  `created_by_display_name` / `last_modified_by_display_name` still
  populate, but from the search profile, which was occasionally observed
  mangled (a stray semicolon-joined value) where the same user's
  children rows were clean.
- **Content matching, not just names.** The query matches file *content*
  as well as filenames, so a query can return files whose names do not
  contain the term. Recall is also not exhaustive — a term that names an
  existing file was observed returning zero hits — so treat the table as
  a search, not an inventory.
- **A join caveat.** Search rows spell `parent_drive_id` **lowercase
  without the leading zero** (`fab1234cd567890`-style) while children
  rows carry the `0FAB…` form of the same drive — a naive
  `drive_items ⋈ drive_item_search` on that column misses. Join on
  `parent_id`/`id` instead, which agree. `parent_path` spelling also
  varies row to row (raw vs percent-encoded non-ASCII), passed through
  verbatim.
- **Continuations can fail upstream on personal drives** — see
  [Pagination](#pagination).

## Pagination

Cursor pagination over Graph's `@odata.nextLink`, which the gateway
re-exposes as a `nextLink` input/output pair. The cursor is a **complete
URL**, not an opaque token, and the gateway validates its shape before
credentials while the executor additionally pins the host and an
allowlisted path set. Consequences worth knowing:

- The two tables' cursors are **not interchangeable** — each action's
  executor allowlists its own paths. Confirmed live in both directions:
  a children cursor handed to `search_items` (and vice versa) is a 400
  `invalid_input` naming the allowlist ("nextLink must target OneDrive
  search/children pagination endpoints"). Skardi never crosses them.
- Page size is `top: 999`, the declared ceiling **and a confirmed wire
  bound** (a real `top=999` request answers a full 200 page). It bounds
  **requests, not bytes**: a `LIMIT` smaller than a page costs exactly
  one request instead of draining the folder, but `top` is never narrowed
  to the `LIMIT`, so `LIMIT 10` still transfers up to 999 items and the
  rows are discarded locally.
- The scan ends when `nextLink` comes back null. Both actions declare it
  a **required** output key, so it is always present; Graph publishes no
  separate has-more flag, which means a null cursor is the *only*
  end-of-collection signal. The corollary was the one truncation risk
  worth naming — a gateway that answered a link its own allowlist rejects
  with null instead of an error would make a partial scan look complete —
  and the live pass confirmed it **errors** (the 400 above), never nulls.
  Real terminal pages return an explicit null on both actions.
- **Children pagination is fully live-verified** (root, `folderItemId`
  and `driveId` forms, small `top` forcing real multi-page walks, rows
  identical to the unpaged listing). **Search continuations currently
  fail server-side on personal (MSA) drives**: following a real search
  cursor answers Graph's own "Error Calling Substrate Search" —
  deterministic across retries and across the `/me/drive` and
  `/drives/{id}` forms, with the gateway forwarding the cursor
  byte-identically, so it is an upstream Microsoft limitation, not a
  gateway or pack defect. The scan **fails loudly** through the failure
  envelope rather than truncating; a query whose hits fit one page
  (≤ `top`, i.e. up to 999 hits) terminates on a clean null and succeeds.

The default safety bounds fail (never truncate) an unfiltered scan past
`max_pages` × page-size rows. At the defaults that is 100 pages × 999
rows = **99 900 items per scan**, failing with `ScanBoundsExceeded` — so
`max_pages` binds first here by barely a hundred rows, closer than in any
sibling pack, because this pack's large page size brings the two ceilings
almost exactly together (`max_rows` defaults to 100 000). A drive big
enough to matter will hit that before it finishes; raise
`max_pages`/`max_rows` in the `open_connector:` block or narrow with
`LIMIT` — see
[the integration guide](open-connector.md#bounds-retries-and-errors).

No filter is pushed down, and that is structural rather than an omission:
**neither action exposes a filter input at all.** Predicates run locally
in DataFusion after a bounded fetch, so a `WHERE` clause narrows results
but not the amount fetched. The tools that genuinely scope a scan are the
folder resources and `LIMIT`.

## Authorization

`one_drive` is authorized **separately from `outlook`** — the sibling
grant does not cover it. Until it is connected, a scan fails with the
gateway's own `403 "Connect one_drive with OAuth first."`

> **The OAuth consent is read-write even though this pack is read-only.**
> The scope union the gateway requests for `one_drive` is `User.Read`,
> `Files.ReadWrite`, `offline_access` — `Files.ReadWrite` because the same
> connection also serves the service's upload, delete and metadata-update
> actions. Skardi's tables stay read-only by construction (the registrar
> rejects `read_write` for source packs, and no write action is in any
> table), but you are consenting to more than Skardi will use. A narrower
> `Files.Read` union is an upstream ask.

Graph throttles per drive with HTTP 429 plus `Retry-After`; the gateway
surfaces that as a failure envelope rather than silently truncating.

## Fingerprints and drift

Each table pins the BLAKE3 fingerprint of its action's declared output
schema, compared against live discovery at **registration** — a changed
upstream contract fails there, naming the table and action, instead of
producing wrong rows later.

The two pins are **identical**, and deliberately so: Graph declares the
same `driveItem` collection schema for a folder listing and for a search,
so the two captures are byte-identical. That is an upstream fact, not a
copy-paste slip.

Because every mapped column resolves inside the declared schema, the
fingerprint gate covers the whole table — this pack has no
outside-the-gate passthrough surface, and therefore no `select` pin (the
lever the `outlook` pack needs for its undeclared columns). Two different
mechanisms deliver that loudness, and only the first is registration: an
**upstream** rename or retype changes the schema hash and fails
registration, while a **mapping typo inside the pack** leaves the hash
untouched and is caught instead by the pack's coverage test in CI. Both
are loud; neither leaves you with a silently always-NULL column.

Two operational consequences worth knowing. First, the pin hashes the
whole declared schema, so **any** upstream change fails registration —
additive ones included; a gateway upgrade means re-capturing the
contracts and re-pinning, not a silent widening. Second, the gate is
**output-only**: a gateway that renamed an input key would register
cleanly and then fail every scan. This pack therefore also commits the
input schemas (`packs/fixtures/one_drive/contracts/inputs/`), with a test
checking every key, page size and cursor spelling it sends against them.
Both sides are committed files, so that check catches input drift when
the contracts are re-captured after a gateway upgrade — not while the
gateway drifts underneath a deployed binary. Gating inputs at
registration, as output already is, remains engine work.

## Live verification

**Ran 2026-08-21** against a real personal (MSA) OneDrive through the
same gateway build the contract was reconciled on. Reproducing it needs
your own Microsoft account and Azure app registration — Skardi never
handles the credential:

1. Register an Entra app with `http://localhost:3000/oauth/callback` as a
   redirect URI.
2. `PUT /api/oauth/configs/one_drive` on the gateway with its
   `clientId`/`clientSecret`.
3. Authorize `one_drive` in a browser (separately from `outlook`).

What the pass covered and settled:

- **End-to-end scans through skardi-server**: registration passed the
  fingerprint gate against *live* discovery; both tables scanned under
  real bindings (root drive, `folderItemId`-scoped, two search terms);
  `LIMIT` stopped pagination after one request.
- **Every column extracted a real non-NULL value somewhere, on both
  tables, with the same single exception: `description`.** It was never
  witnessed on either — 2800+ children rows carried none, and neither
  did the search hits (kept, caveat recorded above) — so the pass
  witnessed 15 of `drive_items`' 16 columns and 13 of
  `drive_item_search`'s 14. That second denominator is the pass's
  headline catch: the declared contract said sixteen columns, the wire
  said fourteen, and the wire wins.
- **Pagination**: `top` bounds (999 ok, 1000/0 both 400), real
  multi-page children walks in all three path forms, explicit null on
  real terminal pages, allowlist rejections erroring (never nulling),
  cursor non-interchangeability in both directions, and the upstream
  search-continuation failure — all as documented under
  [Pagination](#pagination).
- **Resources**: real `folderItemId`, `folderPath` and `query` values
  returned real rows (not just HTTP 200); both folder resources set
  together proved `folderItemId` wins silently.
- **Fixtures**: both row fixtures are now redacted live captures —
  every row mirrors a real wire row key-for-key with deterministic
  synthetic identities, enforced by a default-deny per-key audit test.

Provider API version: Microsoft Graph `v1.0` (the executors pin it in
every URL, including the cursors Graph hands back).
