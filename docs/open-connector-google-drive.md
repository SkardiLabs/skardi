# Google Drive Source Pack

Google services reach Skardi as **one source pack per Open Connector
service**: upstream splits Google into `gmail`, `googledrive` (and
others), each with its **own OAuth connection** — and a Skardi binding
carries exactly one connection alias, so authorizing Gmail does not
authorize Drive or vice versa. This document covers the built-in
**`google_drive` pack** — `files`, `drives` and `file_permissions` over
the `googledrive` service. The `gmail` pack has
[its own document](open-connector-gmail.md).

Two upstream spellings worth knowing before anything else: the service
is **`googledrive`** (one word), and its action IDs carry **two dots**
(`googledrive.files.list` — the action name is itself dotted). The
dotted IDs are handled end to end; the pack's table IDs stay the normal
shape (`google_drive.files`).

> **Status: live-verified.** The wire contract below is reconciled
> against a live gateway (v1.3.4, open-connector at `2410fbe`) and
> verified against real Google Drive data on 2026-08-25: a seeded
> corpus (six files, one shared drive, three grants) scanned end to end
> through skardi-server, every table's registration passing the
> fingerprint gate against live discovery. The two load-bearing items
> both closed: the five `restrictions.*` spellings were witnessed
> verbatim on a real drive row, and the all-drives pin really surfaces
> shared-drive rows. Three columns remain unwitnessed non-null for
> structural reasons (their keys are present, correctly spelled, and
> null on real rows): `drives.org_unit_id` (Workspace org units only),
> `drives.theme_id` (null even when a drive is created with an explicit
> theme — it materializes as `color_rgb` + `background_image_link`),
> and `file_permissions.expiration_time` (Google rejects expirations on
> domain/anyone grants, and a user-grant expiration needs a second real
> account).

**The wire contract is Open Connector's, not Google's REST API**: the
gateway's googledrive executors rebuild every row **normalized**
(Slack-style) into a fixed key set, declare the row objects
`additionalProperties: false`, and pin their own `fields` projection on
the provider request — `files.list` does not even declare a `fields`
input (sending one is a hard 400). So the row shape is fully determined
upstream, and the wire keys are the normalizer's, not Google's REST
spellings: `size_bytes` is a real number here (Google's own `size` is a
decimal *string*), and columns named from Google's docs
(`webContentLink`, …) simply do not exist on this wire. Inputs are the
gateway's camelCase strict schema (`pageSize`, `pageToken`, `driveId`,
`q`, `fileId`; a wrong key is a hard 400).

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
          - name: gdrive                    # schema name in SQL
            source_pack: google_drive
            connection_alias: my-google-drive  # the gateway's googledrive OAuth connection
            tables: [files, drives]         # no resource: files = the whole corpus

          # `q` pins a Drive query-language string; the table then means
          # "the files matching this query". It composes with `driveId`
          # (a query within one shared drive is meaningful).
          - name: gdrive_docs
            source_pack: google_drive
            connection_alias: my-google-drive
            resource:
              q: "mimeType = 'application/vnd.google-apps.document' and trashed = false"
            tables: [files]

          # One binding per audited file: `fileId` is required, and rows
          # carry no file identity of their own — the binding is what
          # names the file. A full Drive URL works as the value too
          # (upstream extracts the ID from it).
          - name: contract_acl
            source_pack: google_drive
            connection_alias: my-google-drive
            resource:
              fileId: "1AbCdEfGhIjKlMnOpQrStUvWxYz1234567890abcd"
            tables: [file_permissions]
```

```sql
-- Largest files by stored bytes (folders have no size of their own):
SELECT name, mime_type, size_bytes, modified_time
FROM saas.gdrive.files
WHERE size_bytes IS NOT NULL
ORDER BY size_bytes DESC
LIMIT 20;

-- Shared-drive files join back to their drive:
SELECT d.name AS drive, f.name, f.modified_time
FROM saas.gdrive.files f
JOIN saas.gdrive.drives d ON f.drive_id = d.id;

-- Who can reach the audited file (schema = the binding name above):
SELECT type, role, email_address, domain, allow_file_discovery
FROM saas.contract_acl.file_permissions;
```

## Tables

### `files`

Action `googledrive.files.list` — the only action that sees the whole
corpus. With no resource, the table is **every file the connected
account can reach**. Fourteen columns:

| column | wire path | type |
|---|---|---|
| `id` | `id` | utf8, non-null |
| `name` | `name` | utf8 |
| `mime_type` | `mimeType` | utf8 |
| `web_view_link` | `webViewLink` | utf8 |
| `created_time` | `createdTime` | timestamp(ms, UTC) |
| `modified_time` | `modifiedTime` | timestamp(ms, UTC) |
| `size_bytes` | `sizeBytes` | int64 |
| `drive_id` | `driveId` | utf8 |
| `parents` | `parents` | list of utf8 |
| `owner_display_names` | `owners[].displayName` | list of utf8 |
| `owner_email_addresses` | `owners[].emailAddress` | list of utf8 |
| `shared` | `shared` | boolean |
| `starred` | `starred` | boolean |
| `trashed` | `trashed` | boolean |

**`mime_type` is the de-facto type discriminator**: folders are
`application/vnd.google-apps.folder`, native Workspace files are
`application/vnd.google-apps.*`, everything else is an ordinary MIME
type. **`size_bytes` is null for anything without a byte size of its
own** — live, that means folders. Native Docs/Sheets/Slides *do* report
a size (Workspace files count against storage quota), so an all-NULL
`size_bytes` over real files is a defect to chase, not an expected
shape.

**Ownership doubles as a second discriminator.** My Drive files carry
exactly one owner; **shared-drive items carry none** (the drive owns
them), so both owner columns are NULL there while `drive_id` is set —
the join key to `drives.id`. The two owner columns are plucked from one
`owners` array; they are lists rather than scalars because the engine
has no array indexing, not because multi-owner is expected.

**A NULL boolean means "not reported", never false.** `shared`,
`starred` and `trashed` are only present on the wire when upstream
reports a real boolean.

**Trashed files are included.** Drive's listing does not exclude them by
default and the pack does not quietly pin a `q` to change that —
`trashed` is a column; filter in SQL (`WHERE trashed IS NOT TRUE`).

**The pack pins `supportsAllDrives: true` +
`includeItemsFromAllDrives: true` on every request.** Unpinned, the
action inherits Google's own default and **silently omits every
shared-drive file** — a `files` table that quietly excludes a class of
files is the failure mode the pin exists to prevent. (The sibling
actions need no pin: their executors default it upstream.)

Optional resources: `q` (a Drive
[query-language](https://developers.google.com/drive/api/guides/search-files)
string — see [why it is not a filter](#pagination)) and `driveId` (scope
the scan to one shared drive). They compose. The deprecated
`teamDriveId` alias is deliberately not exposed.

### `drives`

Action `googledrive.drives.list`. **Shared drives only** — My Drive is
not a drive resource and never appears as a row, so on an account with
no shared-drive membership this table is **legitimately empty** (a
successful empty scan, not an error). Thirteen columns: `id` (non-null),
`name`, `hidden` (boolean), `color_rgb`, `created_time`, `org_unit_id`,
`theme_id`, `background_image_link`, plus five boolean flags from the
drive's `restrictions` object:

| column | meaning (Google Drive v3 docs) |
|---|---|
| `admin_managed_restrictions` | administrative privileges required to modify restrictions |
| `copy_requires_writer_permission` | readers/commenters cannot copy, print, download |
| `domain_users_only` | access limited to the drive's domain |
| `drive_members_only` | access limited to drive members |
| `sharing_folders_requires_organizer_permission` | only organizers can share folders |

The restriction flags carry an explicit caveat: upstream declares
`restrictions` as a **bare open object**, so these five key spellings
are outside the fingerprint gate (a coverage test pins exactly these
five as the pack's only uncovered columns) — an upstream inner-key
rename would not change the pinned fingerprint. The live pass witnessed
all five spellings verbatim on a real drive row (as present `false`s,
alongside a nested `downloadRestriction` object the pack leaves
unmapped), which verifies today's upstream; the gate still cannot see
tomorrow's. NULL in any of them means "unreported", never false. If all
five ever read NULL on drives that plainly have restrictions set, the
raw action scan is the check — it types every object as an opaque JSON
column, so `SELECT id, restrictions FROM open_connector_scan(<gateway>,
'googledrive.drives.list', '{}', '$.drives')` shows the spellings
upstream currently emits (the action must be in the gateway's
`raw_action_allowlist`). The per-caller sibling
`capabilities` ("capabilities the *current user* has") is deliberately
unmapped: its values change with the OAuth identity doing the scan, so
the same table under two connections would disagree.

Optional resource: `q` (the drives query language, e.g.
`name contains 'x'`). `useDomainAdminAccess` is never sent — it requires
a Workspace admin and 403s for everyone else.

### `file_permissions`

Action `googledrive.permissions.list`. **Required resource: `fileId`.**
The rows carry no file identity of their own, so the table is "the
permissions of the file this binding names" — the same shape as Notion's
`block_children`. Auditing many files means one binding per file. Eleven
columns: `id` (non-null), `role` (owner / organizer / fileOrganizer /
writer / commenter / reader), `type` (user / group / domain / anyone),
`email_address`, `display_name`, `domain`, `photo_link`,
`expiration_time` (timestamp; set only on expiring grants), and three
booleans — `allow_file_discovery`, `deleted`, `pending_owner` (NULL =
not reported, never false).

The `type` column changes what identity means: for `domain` and `anyone`
grants the identity columns are null and `domain` /
`allow_file_discovery` carry the meaning — a `type = 'anyone'` row with
`allow_file_discovery` false is a link-only share, the row a sharing
audit usually exists to find.

**A blank `fileId` fails at scan time, not at startup.** Omitting the
key is refused at registration, naming the binding; but resource values
are checked for presence, not content, so `fileId: ""` starts up and
then 400s every scan (`invalid_input` at the gateway's schema layer),
and a whitespace-only value dies on the executor's own check. Loud
either way, never a silent empty table. Convenience: upstream runs the
value through `extractFileId`, so a full
`https://drive.google.com/file/d/…/view` URL works as the binding value.

`permissionDetails` (inherited-permission records) and the constant
`kind` are on the wire and deliberately unmapped.

## Pagination

Cursor pagination, uniform across all three actions: a `pageToken`
input against a top-level `nextPageToken` output that is **always
present and null exactly on the final page** (declared nullable *and*
required). The cursor is an opaque token — unlike OneDrive's
complete-URL `nextLink`, there is no shape to preserve and no per-action
allowlist to trip. No executor filters rows after paginating, so
null-cursor termination is complete.

- Page sizes are pinned at each action's **declared ceiling**: `files`
  1000, `drives` and `file_permissions` 100 — and the live pass
  confirmed the ceilings are *wire* bounds, not just declared ones
  (1000/100/100 each returned 200; 1001/101/0 each 400). Page size
  bounds **requests, not bytes**: `LIMIT 10` on `files` costs one
  request but still transfers up to 1000 rows (measured live, a fully
  populated row is ~700 bytes).
- At the default safety bounds, an unfiltered `files` scan fails (never
  truncates) past `max_pages` × 1000 = 100 000 rows — exactly the
  default `max_rows`, so the two ceilings coincide here. Raise them in
  the `open_connector:` block or narrow with `LIMIT` — see
  [the integration guide](open-connector.md#bounds-retries-and-errors).
- **No filter is pushed down, structurally.** The one candidate input,
  `q`, is a whole query *language* — Skardi's filter facility maps a
  `column op value` comparison onto an input field, and a bare column
  literal is never a legal `q`. So `q` is a **resource** (the binding
  pins the whole query; the table means "the rows matching it") and
  every SQL predicate runs locally after the bounded fetch.

## Authorization

`googledrive` is authorized separately from `gmail`. Until it is
connected, a scan fails with the gateway's own
`403 "Connect googledrive with OAuth first."`

> **The OAuth consent is read-write even though this pack is read-only.**
> All three actions declare only `drive.readonly` +
> `drive.metadata.readonly`, but the provider-level grant requests the
> full read-write `https://www.googleapis.com/auth/drive` as well,
> because the same connection serves the service's upload, update and
> delete actions. Skardi's tables stay read-only by construction, but
> you are consenting to more than Skardi will use — an upstream
> connection property a pack cannot narrow.

Google throttles per project and per user (403/429 with backoff); the
gateway surfaces that as a failure envelope rather than silently
truncating. The live pass never approached a throttle — its seeded
corpus is a handful of files — so behaviour under real quota pressure
is unobserved rather than characterized here.

## Fingerprints and drift

Each table pins the BLAKE3 fingerprint of its action's declared output
schema, compared against live discovery at **registration** — a changed
upstream contract fails there, naming the table and action, instead of
producing wrong rows later.

Because rows are normalized and declared closed
(`additionalProperties: false`), the declared contract is column truth
to an unusual degree: on `files` and `file_permissions` **every** mapped
path resolves inside the declared item schema, so the gate covers the
whole table. The one exception is deliberate: the five `restrictions.*`
flags on `drives` sit under a bare open object, outside the gate — an
upstream rename of an *inner* key there would keep the hash unchanged
and surface as a silently-NULL column, which is why a coverage test pins
exactly those five and the live pass owes them real rows.

The gate is **output-only**, so this pack also commits the input
schemas (`packs/fixtures/google_drive/contracts/inputs/`), with a test
checking every key, page size and cursor spelling it can send against
them — including the negative space: the keys that 400ed live
(`fields` on `files.list`, `filter`, `page`, `perPage`, `maxResults`,
`cursor`, lowercase `orderby`) and the declared keys the pack promises
never to send (`corpora`, `corpus`, `spaces`, `teamDriveId`, `orderBy`,
`includeLabels`, `includePermissionsForView` on files; `fields`,
`includePermissionsForView`, `useDomainAdminAccess`,
`supportsAllDrives` on permissions; `useDomainAdminAccess` on drives).
One capture detail: these schemas carry a `$schema` key (the sibling
packs' captures do not), and the fingerprint hashes the whole schema, so
the captures keep it verbatim.

## Live verification

The phase-4 pass ran 2026-08-25 against a real Workspace account with a
seeded corpus (a folder, four files spanning every shape, one shared
drive, one shared-drive file, and three grant types on one file). What
it established, following the
[design record](superpowers/specs/2026-08-25-open-connector-google-drive-pack-design.md):

1. **R1 resolved in the best direction**: the account started with zero
   shared drives (that clean empty scan was itself witnessed), then
   turned out to be able to *create* one — so the `drives` columns were
   verified on real rows instead of deferred. All five restriction
   spellings came back verbatim as present `false`s; flipping any to
   `true` is admin-gated on this tenant, which does not affect the
   extraction path.
2. Real rows under every mapped column on all three tables, through
   end-to-end skardi-server scans registered against live discovery —
   every column except the three structural residuals above extracted a
   real non-NULL value somewhere.
3. The all-drives pin surfaced the seeded shared-drive file with
   `drive_id` carrying the real drive id, and shared-drive rows showed
   their real absence pattern: exactly `owners` and `shared` drop
   (NULL in SQL) while `parents`/`starred`/`trashed` stay.
4. Wire bounds confirmed (1000/100/100 pass, 1001/101/0 all 400); real
   multi-page walks at `pageSize: 1` on `files` (five pages) and
   `file_permissions` (two pages) with ~444-char cursors and a real
   terminal-page `null` each; through skardi-server with a small page
   size, `LIMIT` stopped a three-page scan after two requests.
5. Real `q` values filtered live (both as a direct input and through a
   bound binding resource); the required `fileId` forwarded verbatim on
   every page; a nonexistent `fileId` produced the documented gateway
   failure envelope.
6. Row fixtures re-derived as redacted live captures under a
   default-deny redaction audit (one_drive-style). Live corrections the
   captures folded in: native Google Docs DO report `sizeBytes` (the
   null-size row is a folder), every grant carries `permissionDetails`
   (not just shared-drive ones), and owner objects always carry four
   keys.

Deferred surfaces, recorded in the design record: `changes.list` (its
cursor must be bootstrapped by a second action, a pagination shape the
engine has no spelling for), and the comments / replies / revisions /
labels / accessproposals actions (out of the first wave's scope;
accessproposals is additionally Workspace-approval-gated).

Provider API version: Google Drive `v3` (the executors pin it in every
URL).
