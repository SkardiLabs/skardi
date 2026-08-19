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

> **Status: implementation complete, live verification pending.** The
> wire contract below is reconciled against a live gateway (v1.3.4,
> open-connector at `2410fbe`) — action inventory, both discovery
> schemas, the full input surface and the `top` bounds were all probed
> live on 2026-08-19 — but **no real drive has been scanned yet**,
> because `one_drive` needs its own OAuth grant. Until that pass runs the
> pack's row fixtures are synthetic rather than redacted live captures,
> and the column set should not be read as live-confirmed. See
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

-- Search spans the whole drive, unlike a folder listing:
SELECT name, parent_path, last_modified_date_time
FROM saas.drive.drive_budget
ORDER BY last_modified_date_time DESC;
```

## Tables

Both tables carry the **same sixteen columns** — they read the same Graph
`driveItem` shape — and differ only in which collection they name.

| column | wire path | type |
|---|---|---|
| `id` | `id` | utf8, non-null |
| `name` | `name` | utf8 |
| `web_url` | `webUrl` | utf8 |
| `description` | `description` | utf8 |
| `size` | `size` | int64 |
| `e_tag` | `eTag` | utf8 |
| `c_tag` | `cTag` | utf8 |
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
a folder"). `e_tag` changes on any change, `c_tag` only on a *content*
change, so the pair separates a rename from a real edit.

Both display-name columns read only Graph's **user** identity arm: a row
created by an application or device is NULL there by design, not by
accident.

Unmapped on purpose: `root`, `deleted`, `shared`, `specialFolder`,
`remoteItem`, `searchResult` and `fileSystemInfo` are all declared
upstream as bare open objects and carry presence-as-signal rather than
data. Graph's directory IDs under the identity sets are omitted too —
they are opaque without a directory join this pack cannot perform.

### `drive_items`

Action `one_drive.list_folder_children`. Optional resources: `driveId`
(a non-default drive), `folderItemId` or `folderPath` (one folder). With
no resource at all, the table is the **drive root's children**.

`folderItemId` and `folderPath` are alternatives — set one, not both
(both are declared upstream, so a binding *can* carry both, and which
one wins is not yet established). A `folderPath` is a path from the drive
root and must start with `/`, e.g. `folderPath: "/Documents/Finance"`;
upstream enforces only "non-empty", so a slash-less path is accepted at
validation and then fails.

**Direct children only — the listing is not recursive.** One binding sees
one folder level; `folder_child_count` reveals that subfolders have
contents without enumerating them. This is a documented limitation of the
table, not a defect: the collection it claims to be does terminate
completely. To see a whole drive, use `drive_item_search`.

### `drive_item_search`

Action `one_drive.search_items`. **Required resource: `query`.** Optional:
`driveId`. The binding pins the search term, so the table is "the drive
items matching this binding's query" — the same shape as Notion's
`block_children` requiring a `blockId`. There is no spelling of "search
everything", so there is no complete-drive-listing table; bind one search
per term you care about.

## Pagination

Cursor pagination over Graph's `@odata.nextLink`, which the gateway
re-exposes as a `nextLink` input/output pair. The cursor is a **complete
URL**, not an opaque token, and the gateway validates its shape before
credentials while the executor additionally pins the host and an
allowlisted path set. Consequences worth knowing:

- The two tables' cursors are **not interchangeable** — each action's
  executor allowlists its own paths, so a folder-listing cursor handed to
  the search action is rejected upstream, and vice versa. Skardi never
  crosses them, and no Skardi-side test can hold an upstream property;
  live verification is what confirms it.
- Page size is `top: 999`, the declared ceiling. It bounds **requests,
  not bytes**: a `LIMIT` smaller than a page costs exactly one request
  instead of draining the folder, but `top` is never narrowed to the
  `LIMIT`, so `LIMIT 10` still transfers up to 999 items and the rows are
  discarded locally.
- The scan ends when `nextLink` comes back null. Both actions declare it
  a **required** output key, so it is always present; Graph publishes no
  separate has-more flag, which means a null cursor is the *only*
  end-of-collection signal. The corollary is the one truncation risk worth
  naming: if the gateway ever answered a link its own allowlist rejects by
  returning null rather than an error, a partial scan would look complete.
  Confirming it errors instead is part of live verification.

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

Because all sixteen columns resolve inside the declared schema, the
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

**Not yet run.** Phase 4 needs your own Microsoft account and Azure app
registration — Skardi never handles the credential:

1. Register an Entra app with `http://localhost:3000/oauth/callback` as a
   redirect URI.
2. `PUT /api/oauth/configs/one_drive` on the gateway with its
   `clientId`/`clientSecret`.
3. Authorize `one_drive` in a browser (separately from `outlook`).

What that pass must settle, and until then remains unconfirmed: whether
`top: 999` is a wire bound as well as a declared one; whether the real
final page returns a genuinely null `nextLink`; whether a real search
cursor survives the executor's path allowlist (the analogous
parenthesized OData form is exactly what breaks `outlook`'s
folder-scoped pagination upstream) **and errors rather than nulling the
cursor** when it does not, since a null cursor is the only
end-of-collection signal here; whether every mapped column carries a
real non-NULL value somewhere; whether a real `folderItemId`,
`folderPath` or `query` returns rows rather than merely HTTP 200, and
what happens when the two folder resources are set together; and
re-deriving both row fixtures as redacted live captures. The design
record's numbered list is the authoritative version.
