# Google Drive Source Pack

Milestone 5.9. Design record for the `google_drive` source pack: the wire
contract as reconciled against a **live** Open Connector gateway on
2026-08-25 (gateway v1.3.4, open-connector `2410fbe` — the same upstream
pin milestone 5.8 captured against). Table design (phase 2) and the
implementation decisions (phase 3) are appended as those phases run.

**Status: phases 1–4 done (2026-08-25); risk R1 resolved — see its
section.** Phase 3 shipped `packs/google_drive.yaml` +
`packs/google_drive.rs` (three tables: `files` 14 columns, `drives` 13,
`file_permissions` 11), the six contract captures under
`packs/fixtures/google_drive/contracts/` (output and input halves,
`$schema` key kept verbatim — these captures carry one and the
fingerprint hashes the whole schema), and the registry entries. Phase 4
ran against a real Workspace account with a seeded corpus: every
load-bearing claim verified live (restriction spellings witnessed
verbatim, wire page-size bounds confirmed, real cursors and terminal
nulls, the all-drives pin surfacing a real shared-drive row,
three-table end-to-end skardi-server scans against live discovery,
LIMIT stopping real pagination early), and the row fixtures are now
redacted live captures under a default-deny redaction audit. Three
columns remain unwitnessed non-null for structural reasons, keys
present-and-null on real rows: `drives.org_unit_id` (Workspace org
units only), `drives.theme_id` (null even when a drive is created with
an explicit theme), `file_permissions.expiration_time` (Google 403s
expirations on domain/anyone grants; a user-grant expiration needs a
second real account).

Google Drive is the second Google service to get a pack. `gmail` went
first as milestone 5.5 (PR #192); this is a separate Open Connector
service with its own OAuth connection, so — exactly as Microsoft 365
ships one pack per Graph service — it is one pack per Google service and
a Skardi binding carries exactly one `connection_alias`.

## Service landscape

Upstream the service is spelled **`googledrive`** (one word, no
underscore), unlike `one_drive`. It publishes **43 actions**, of which
ten are list-shaped reads:

| action | scope of one call | pageSize bound | requires |
|---|---|---|---|
| `googledrive.files.list` | the whole corpus, `q`-filterable | 1–1000 | — |
| `googledrive.drives.list` | shared drives | 1–100 | — |
| `googledrive.changes.list` | incremental change feed | 1–1000 | — |
| `googledrive.permissions.list` | one file's permissions | 1–100 | `fileId` |
| `googledrive.revisions.list` | one file's revisions | 1–1000 | `fileId` |
| `googledrive.comments.list` | one file's comments | 1–100 | `fileId` |
| `googledrive.replies.list` | one comment's replies | 1–100 | `fileId`, `commentId` |
| `googledrive.files.listLabels` | one file's labels | 1–100 (`maxResults`) | `fileId` |
| `googledrive.accessproposals.list` | one file's access proposals | 1–100 | `fileId` |
| `googledrive.approvals.list` | one file's approvals | 1–100 | `fileId` |

The other 33 are single-item reads (`files.get`, `drives.get`,
`about.get`, `apps.get`, the four `*.get` collaboration reads,
`changes.getStartPageToken`), an export (`files.export`), or writes —
`files.create/copy/update/delete/emptyTrash/generateIds/modifyLabels`,
`drives.create/update/delete/hide/unhide`, and the create/update/delete
triples on comments, replies, permissions and revisions. None is a list,
and the writes are outside a read-only pack by construction.

## Wire contract (gateway v1.3.4, open-connector `2410fbe`)

Verified live 2026-08-25. Every list action above declares
`requiredAuthTypes: ["oauth2"]`, `needsCredential: true`,
`noAuthRunnable: false`, `locallyExecutable: true`.

### Action IDs carry two dots — and that is already safe

`googledrive.files.list` is `service.name` where the *name* itself is
dotted (`files.list`), so pack action IDs here have a shape no shipped
pack has used. Checked on this base rather than assumed:

- `config.rs#validate_action_id` rejects only `/` and the bare `.` / `..`
  segments; an interior dot passes.
- `client.rs#action_url` percent-encodes with `ACTION_ID_SET =
  NON_ALPHANUMERIC.remove(b'.').remove(b'-').remove(b'_')`, so dots
  survive verbatim into the path.
- The `rsplit('.')` calls in `source_pack.rs` and the pack accessors
  operate on **table** IDs, not action IDs, so nothing parses a service
  prefix out of the action.
- Live: `GET`/`POST /v1/actions/googledrive.files.list` both route.

**No engine change is needed for the ID shape.**

### Rows are NORMALIZED, not passthrough

This is the single most consequential phase-1 finding and it is the
opposite of `one_drive`. `executors.ts` rebuilds every row:

```ts
return {
  files: files.map((file) => normalizeDriveFile(asObject(file))),
  nextPageToken: optionalString(payload.nextPageToken) ?? null,
}
```

`normalizeDriveFile` emits a **fixed key set** — `id`, `name`,
`mimeType`, `webViewLink`, `createdTime`, `modifiedTime`, `sizeBytes`,
`driveId`, plus conditionally `parents`, `owners`, `shared`, `starred`,
`trashed` — and every declared row object is
`additionalProperties: false`. Mapping Google's own field names
(`webContentLink`, `size` as a string, `owners[].me`, …) would produce
all-NULL columns, the Slack-pack lesson.

Two consequences worth pinning now:

- The executor **pins its own `fields` projection** for `files.list`,
  `drives.list`, `changes.list` and `revisions.list`
  (`fields: nextPageToken,files(id,name,mimeType,webViewLink,…)` — the
  `driveFileFields` constant). There is no `fields` input on
  `files.list` at all (verified: sending one 400s), so the row shape is
  fully determined upstream and cannot be widened from Skardi.
  `comments.list`, `replies.list` and `permissions.list` DO accept a
  `fields` input that overrides their default projection — a lever this
  pack will not pull, since overriding it can only narrow the declared
  contract.
- **Key absence vs null is not uniform.** `compactObject` (core/cast.ts)
  drops only `undefined`, so the `?? null` fields are always present as
  explicit nulls, while the conditionally-spread ones (`parents`,
  `owners`, `shared`, `starred`, `trashed` on files; `hidden`,
  `capabilities`, `restrictions` on drives; `file`, `removed` on
  changes) are **absent** from the object entirely when upstream omits
  them. Both extract as NULL in Skardi, but the fixtures must model
  both shapes.

`id` / `name` / `mimeType` are `String(payload.x ?? "")` — never null,
so they are sound identity columns (an empty string is possible in
principle and is a phase-4 question, not a nullability one).

### In-band errors are consumed

`runtime-shared.ts#assertGoogleResponse` throws `ProviderRequestError` on
any non-2xx, so Google's `{"error": {...}}` envelope never reaches the
row payload — it becomes the gateway's failure envelope. **No table
declares `error_path`.**

### Pagination

Uniform across all ten actions: a `pageToken` input (`minLength: 1`)
and a sibling top-level `nextPageToken` output declared
`anyOf: [string, null]` **and listed in the output schema's `required`
array**, so the key is always present and null exactly on the final
page. Google publishes no has-more flag and no total count, so no
`has_more_path` / `total_pages_path` override applies.

No executor filters rows after paginating — every one is a bare
`payload.<key>.map(normalize…)` — so **null-cursor termination is
sound** for all ten. (The `.filter()`-after-paginate hazard that forced
the `pageInfo.fetched` contribution upstream for GitHub issues does not
exist here.)

`changes.list` is the one exception to "a scan is a collection scan",
and it is a semantic exception rather than a pagination one: when no
`pageToken` is supplied the executor first calls
`changes.getStartPageToken` and starts from **now**, so a fresh scan
reads changes that happen *after* the scan begins — normally an
immediately-terminating empty page. It is an incremental-sync feed, not
a collection. Phase 2 decides whether it ships at all and, if so,
whether `pageToken` becomes a required resource.

### Inputs are strict camelCase

Every list action declares `additionalProperties: false` and — this is
worth stating precisely — an **empty/absent `required` array, even for
the actions that cannot work without a `fileId`**. The same subtlety
`one_drive.search_items`'s `query` had: enforcement lives in the
executor's own check (`resolveFileId` → `ProviderRequestError(400,
"fileId is required")`), not in the schema, so a missing `fileId` passes
validation and dies at scan time. `fileId: ""` is the one case the
schema does catch, via `minLength: 1`.

Calibrated per the no-credential recipe (default connection, no alias,
no action policy): a bad key answers **400 `invalid_input`**, a good one
**403 `authorization_failed` — "Connect googledrive with OAuth first."**
Two different responses, so the discrimination is valid. Probed live:

| probe | result |
|---|---|
| `files.list` `pageSize` 1 / 1000 | 403 (accepted) |
| `files.list` `pageSize` 0 / 1001 | 400 `invalid_input` |
| `files.list` `pageToken: ""` | 400 (`minLength: 1`) |
| `files.list` `q`+`orderBy`+`spaces`+`corpora`+`supportsAllDrives`+`includeItemsFromAllDrives` | 403 (all accepted) |
| `files.list` `fields` / `page` / `perPage` / `limit` / `orderby` / `maxResults` / `cursor` | 400 each |
| `drives.list` `pageSize` 100 / 101 | 403 / 400 |
| `drives.list` `driveId` | 400 (not an input here) |
| `revisions.list` `pageSize: 1000` | 403 (accepted) |
| `files.listLabels` `maxResults: 100` | 403; `pageSize` → 400 |
| `changes.list` `fileId` | 400 (not an input here) |
| `comments.list` with / without `fileId` | 403 both — schema cannot see it |
| `comments.list` `fileId: ""` | 400 |

Note the per-action page-size asymmetry (`pageSize` 1–1000 on files,
revisions and changes; 1–100 on drives, comments, replies, permissions,
proposals and approvals; **`maxResults`**, not `pageSize`, on
`files.listLabels`) and that `orderby` lower-case 400s where `orderBy`
passes. A *declared* bound is not a *wire* bound (feishu declared 100
and hard-failed above 50, skardi PR #186) — confirming the real ceiling
is phase-4 work.

### Coverage of the fingerprint gate

Every declared row object is `additionalProperties: false` with all keys
under a plain `properties` map; the scalars are `anyOf: [T, null]`
wrappers, which `fingerprint_uncovered_columns` resolves fine (it fails
only when a path *segment* is missing, or when descending *into* an
`anyOf` branch — the gmail case). So mapped columns are expected to be
**fully covered by the fingerprint**, like `one_drive` and unlike
`outlook`: an upstream rename fails registration, an author's typo shows
up as an uncovered column in CI. No `select`-style coverage pin is
needed.

## Authz and rate limits

Every one of the ten list actions declares exactly
`drive.readonly` + `drive.metadata.readonly`. But the **provider-level
OAuth grant requests all three declared scopes**, including the full
read-write `https://www.googleapis.com/auth/drive`
(`providers/googledrive/scopes.ts`). An operator connecting Google Drive
therefore grants write access that this pack never exercises; that is an
upstream connection property, not something a Skardi pack can narrow,
and it belongs in the pack docs as a caveat. Google Drive's own quota
behaviour (per-project and per-user query limits, 403/429 with backoff)
is a phase-4 observation, not something the gateway declares.

## Contract captures

Live discovery for all ten actions was captured (input and output
halves); the three shipped actions' captures are committed under
`packs/fixtures/google_drive/contracts/` — output as the fingerprint
input, `contracts/inputs/` alongside — following gmail (5.5) and
one_drive (5.8).

**The captured output schema is fingerprint input, not column truth.**
Rows here are normalized rather than passthrough, which makes the
declared schema a much stronger predictor than it was for `one_drive` —
but conditional key spreads mean a declared key can still be absent on
every real row, so no column set is final until phase 4 scans real data.

---

# Phase 2 — Table design

Three tables in the first wave, the same count Slack (5.2) shipped. The
shape of the choice: **two zero-resource tables that any account can
scan end to end**, so phase 4 can verify them with no setup beyond the
OAuth grant, plus **exactly one file-scoped table** to put the `fileId`
resource shape through review before the six other file-scoped actions
follow it mechanically.

| table | action | row path | resources | page size |
|---|---|---|---|---|
| `files` | `googledrive.files.list` | `$.files` | optional `driveId`, `q` | 1000 |
| `drives` | `googledrive.drives.list` | `$.drives` | optional `q` | 100 |
| `file_permissions` | `googledrive.permissions.list` | `$.permissions` | **required** `fileId` | 100 |

No table declares `error_path` (phase 1: in-band errors are consumed
upstream), and no table needs an `exclusive_resources` group — unlike
`one_drive`, no two inputs here are alternatives with a silent
precedence. `driveId` and `q` compose.

## Engine facilities this design needs — all of them already exist

Verified on this base before designing around them:

- `utf8_list` and `utf8_list_from_object_key` (+ `key`) for array
  columns, the github precedent. **Scalar array indexing does not
  exist and is deliberately out of scope**
  (`json_to_arrow.rs`: "array indexing is out of scope for relational
  mappings"), so `owners` becomes a list of display names and a list of
  emails rather than `owners[0].displayName`.
- `timestamp_ms_utc`, whose converter accepts RFC 3339 — which is what
  Google emits — as well as epoch millis.
- Optional resources, required resources, and the fixed-input surface.

**No engine change is needed for this pack.** That is a deliberate goal,
not a coincidence: it keeps the diff reviewable as one pack.

## `files`

The flagship. `files.list` is the only action that sees the whole
corpus, and it needs no resource at all, so the default binding scans
every file the connected account can reach.

### Columns (14)

| column | path | type | note |
|---|---|---|---|
| `id` | `id` | utf8, **non-null** | identity; upstream `String(x ?? "")`, never null |
| `name` | `name` | utf8 | |
| `mime_type` | `mimeType` | utf8 | the folder discriminator — `application/vnd.google-apps.folder` |
| `web_view_link` | `webViewLink` | utf8 | |
| `created_time` | `createdTime` | timestamp_ms_utc | RFC 3339 upstream |
| `modified_time` | `modifiedTime` | timestamp_ms_utc | |
| `size_bytes` | `sizeBytes` | int64 | see below |
| `drive_id` | `driveId` | utf8 | null on My Drive items |
| `parents` | `parents` | utf8_list | |
| `owner_display_names` | `owners` | utf8_list_from_object_key, `key: displayName` | |
| `owner_email_addresses` | `owners` | utf8_list_from_object_key, `key: emailAddress` | |
| `shared` | `shared` | boolean | conditionally spread upstream |
| `starred` | `starred` | boolean | conditionally spread upstream |
| `trashed` | `trashed` | boolean | conditionally spread upstream |

Identity non-null, everything else nullable — the convention every pack
follows (rationale recorded in gmail.yaml's messages table).

`size_bytes` is worth a note as a **normalization win**: Google returns
`size` as a decimal *string*, and the executor's `parseSizeBytes` turns
it into a number, rejecting anything that is not a safe non-negative
integer. So the column is a real `int64` rather than a string that
looks like one — but it is null for every file with no byte size of its
own. Phase 2 expected that set to include native Docs/Sheets/Slides;
**phase 4 corrected it** (see the phase-4 notes below): only FOLDERS
came back null, because Workspace files count against storage quota. So
an all-null `size_bytes` over real files is a defect to chase, not an
expected shape.

Two columns come from the same `owners` array under different pluck
keys. Drive files normally have exactly one owner (shared-drive items
have none — the drive owns them), so both lists are usually length 0 or
1; they are lists rather than scalars because the engine has no scalar
indexing, not because multi-owner is expected.

`permissionId` and `photoLink` are declared on the owner objects and
left unmapped: a per-owner opaque ID useful only for a join this pack
cannot perform, and an avatar URL.

### Inputs: `q` and `driveId` as optional resources, and why there is no filter pushdown

**`q` cannot be a filter mapping, structurally.** The filter facility
maps one `column <eq|gt|gt_eq> value` comparison onto one input field,
sending the column's value as the input's value. Drive's `q` is a whole
query *language* (`name = 'x' and trashed = false`), so the value the
facility would send is never a legal `q`. It is therefore an **optional
resource**: the binding pins the whole query string and the table means
"the files matching this binding's query" — the `one_drive.search_items`
and `notion.block_children` shape. With no `q`, the table is every file
the account can see.

`driveId` is the other optional resource: it scopes the scan to one
shared drive. It composes with `q` rather than competing with it, so no
exclusive group.

Nothing is filter-mapped on any of the three tables. That is a
structural fact about this service's inputs, not an omission.

### Fixed inputs: `supportsAllDrives` and `includeItemsFromAllDrives`, both `true`

The one place this pack pins behaviour, and the reasoning matters.
`files.list` forwards `supportsAllDrives` verbatim with **no default**
(`optionalBoolean(input.supportsAllDrives)`), unlike the sibling
actions, where `resolveSupportsAllDrives` defaults it to `true`. So
unpinned, the scan inherits Google's own default and **silently omits
every shared-drive file**. A table named `files` that quietly excludes a
whole class of the account's files is the confidently-wrong-rows failure
mode, so both flags are pinned `true` (Google requires the pair
together to return shared-drive items).

This is the pack's most load-bearing assumption and phase 4 must check
it directly: that the pinned pair is accepted, that shared-drive items
actually appear, and that it does not conflict with a `driveId`
resource, where Google's `corpora` rules interact.

### Negative space — inputs deliberately never sent

- `includeLabels` and `includePermissionsForView` are **structural
  no-ops**: the executor pins its own `fields` projection
  (`driveFileFields`) and that projection contains neither `labelInfo`
  nor `permissions`, so the requested data has no way back out. Sending
  them would cost a request parameter and change nothing.
- `orderBy` would only reorder a set the scan reads in full.
- `corpora` and the deprecated `corpus` are superseded by the pinned
  all-drives pair.
- `teamDriveId` is the deprecated alias of `driveId`; declaring both
  would create exactly the silent-precedence trap `one_drive` needed
  `exclusive_resources` for, so only `driveId` is declared.
- `fields` is not an input on this action at all (verified live: 400).

### Trashed files are included

Drive's `files.list` does not exclude trashed files by default, and this
pack does not pin `q: "trashed = false"` to make it do so — that would
be a semantic narrowing the operator never asked for, and it would
collide with `q` as a resource (the loader rejects a fixed input that
collides with a declared resource). `trashed` is a mapped column, so
the filtering belongs in SQL where the operator can see it.

## `drives`

Shared drives the account can reach — no required resource, and a
natural join partner for `files.drive_id`.

**Read what this table does and does not contain.** `drives.list`
returns **shared drives** (formerly Team Drives) only; **`My Drive` is
not a drive resource and never appears as a row.** Shared drives are a
Google Workspace feature — a personal Gmail account cannot create one
and sees rows here only when it has been invited into someone else's.

That makes this table's verifiability account-dependent, and the first
draft of this document got it wrong: it called the table "cheap, clean,
any account can scan", which failed to apply the very standard used two
sections down to defer `accessproposals` and `approvals` (do not ship a
table whose every column is unverifiable).

**The table is kept in this wave by the operator's decision; the
account question (risk R1) resolved in phase 4's favor** — the account
proved able to create a shared drive, and the columns were verified on
its real rows. See R1 for the outcome detail.

### Columns (13)

Eight from the declared scalar surface — `id` (utf8, non-null), `name`,
`color_rgb`, `created_time` (timestamp_ms_utc), `org_unit_id`,
`theme_id`, `background_image_link`, `hidden` (boolean) — plus the five
restriction flags:

| column | path | type |
|---|---|---|
| `admin_managed_restrictions` | `restrictions.adminManagedRestrictions` | boolean |
| `copy_requires_writer_permission` | `restrictions.copyRequiresWriterPermission` | boolean |
| `domain_users_only` | `restrictions.domainUsersOnly` | boolean |
| `drive_members_only` | `restrictions.driveMembersOnly` | boolean |
| `sharing_folders_requires_organizer_permission` | `restrictions.sharingFoldersRequiresOrganizerPermission` | boolean |

**These five are the only columns in the pack outside the fingerprint
gate, and the reason deserves stating exactly.** `restrictions` is
declared as a bare open object —
`{"type":"object","properties":{},"additionalProperties":{}}` — so the
contract promises the key exists and says *nothing whatsoever* about its
contents. Three consequences, worst first:

1. **The five key spellings above appear in no capture.** They come from
   Google's own Drive v3 documentation, which is exactly the source this
   skill's opening lesson warns about: plausible from the provider's
   docs, wrong against the real gateway. `normalizeDrive` passes the
   object through raw (`asOptionalObject(payload.restrictions)`, no
   renaming), so Google's camelCase spelling is what arrives — which
   makes the doc-derived names credible but **not verified**. Phase-4
   real rows are the only thing standing between these five columns and
   the Notion `archived`/`is_archived` defect.
2. **Upstream drift stays silent here.** The fingerprint hashes the
   whole output schema, so a change to the *declaration* fails
   registration — but the inner keys are undeclared, so upstream can
   rename one at runtime without touching the declaration: hash
   unchanged, registration passes, column silently all-NULL.
   `fingerprint_uncovered_columns` will report all five, and that is a
   genuine case-1 gap, not the case-2 "declared but the walker cannot
   descend an `anyOf`" that covers every gmail column.
3. `restrictions` is **not** in the item's `required` array, and
   upstream drops the key when absent (`asOptionalObject` → `undefined`
   → dropped by `compactObject`), so a drive with no restrictions set
   may carry no key at all. All five columns are nullable.

The trade is taken deliberately: these five answer "how open is this
drive" (`domain_users_only`, `drive_members_only`), squarely the same
sharing-audit question `file_permissions` exists for. What buys it is
that phase-4 real rows close the *authoring-error* class. Nothing closes
the *future-drift* class, and the pack doc must say so rather than let
the reader assume the fingerprint covers them.

Left unmapped, with reasons:

- `kind` — the constant `"drive#drive"` on every row. A per-resource
  constant carries no information, the same reasoning that left
  `one_drive`'s `driveType` unmapped. (It is also declared on comments,
  replies, revisions and permissions rows, and unmapped there for the
  same reason.)
- `capabilities` — about 20-30 booleans (`canAddChildren`,
  `canComment`, `canDeleteDrive`, `canManageMembers`, `canRename`,
  `canShare`, …). Unmapped for a reason stronger than "it is a blob":
  the values are **relative to the authenticated account**, so scanning
  the same drive under a different OAuth identity yields different
  values. It is a per-user permission view, not a property of the
  drive, and putting it in a table that claims to describe drives would
  mislead every reader of that column.
`restrictions` is the counterpart that IS mapped — five boolean
scalars, in the column table above. It is drive-owned configuration
that does not vary by caller, so the argument against `capabilities`
does not touch it; what it costs instead is fingerprint coverage, and
that cost is spelled out at the column table rather than glossed.
Mapping it as one `json` column was rejected: a blob no SQL predicate
can reach without JSON functions is not a usable answer to "which
drives are domain-restricted".

`useDomainAdminAccess` is never sent: it requires a Workspace admin and
403s for everyone else, so pinning it would break the common case to
serve the rare one. `q` is an optional resource here too.

## `file_permissions`

Who can reach one file. The sharing-audit surface, and the table that
establishes the file-scoped shape.

### `fileId` is a required resource — and the enforcement boundary is narrow

Same mechanism as `one_drive.search_items`'s `query`, and worth stating
precisely because it is easy to over-read. The input schema's `required`
array is **empty** and `fileId` is merely `minLength: 1`, so:

- `fileId` **missing** → passes schema validation, dies in the
  executor's own `resolveFileId` check (`400 "fileId is required"`).
  Declaring it a required resource closes exactly this case, at
  registration, before any HTTP.
- `fileId: ""` → 400 `invalid_input` at the schema layer (verified
  live).
- `fileId: "   "` → resource validation is presence-plus-non-null only
  (`contains_key` plus the config layer's null check), so it registers
  cleanly and fails at **scan** time on the upstream 400. Loud either
  way, never a silent empty table — but a scan-time failure, not a
  config-time one. Trimming resource values would be an engine-wide
  policy change, not a pack decision.

One more upstream quirk to document rather than fight: `resolveFileId`
runs the value through `extractFileId`, which accepts a **Drive URL**
and pulls the ID out of it. So a binding whose `fileId` is
`https://drive.google.com/file/d/ABC/view` works. Convenient, and worth
naming so it does not read as a bug when someone tries it.

### Columns (11)

`id` (utf8, non-null), `role`, `type`, `email_address`,
`display_name`, `domain`, `photo_link`, `expiration_time`
(timestamp_ms_utc), `allow_file_discovery` (boolean), `deleted`
(boolean), `pending_owner` (boolean).

`kind` is unmapped (constant). `permissionDetails` is unmapped: an
array of inherited-permission records whose useful content is a
per-entry role and inheritance source — a second table's worth of shape,
not a column.

### The rows carry no file identity, and the engine cannot add one

`permissions.list` rows have no `fileId` field, and **a resource's value
cannot become a column** — the loader wires resources into the request,
and there is no injection path into the row. So the table is
"the permissions of the one file this binding names", exactly as
`notion.block_children` is the children of one block. An operator
wanting permissions across many files binds the pack once per file, and
the binding name is what distinguishes the resulting schemas. This is a
limitation to document in the pack doc, not to work around.

## Deferred, with a reason each

| action | why not in this wave |
|---|---|
| `changes.list` | **Semantically not a collection.** With no `pageToken` the executor first calls `changes.getStartPageToken` and starts from *now*, so a full scan reads changes occurring after it begins and terminates on an empty page. Shipping it would need `pageToken` as a required resource and a story for what a re-scan means — an incremental-sync design, not a table. |
| `comments.list`, `revisions.list` | Fine actions, file-scoped exactly like `file_permissions`. Held for a second wave so the first carries one instance of the shape rather than three. `comments` additionally needs a decision on its nested `replies` array and its bare-object `quotedFileContent`. |
| `replies.list` | Needs `fileId` **and** `commentId` — a two-level binding that only earns its place next to a comments table. |
| `files.listLabels` | Workspace-only label taxonomy: empty on any non-Workspace account, so phase 4 could not witness a single non-NULL column. It also spells its page size **`maxResults`** rather than `pageSize`, the one pagination-input asymmetry in the service. |
| `accessproposals.list`, `approvals.list` | Rare surfaces that are empty on ordinary accounts, so phase 4 cannot verify any column against real data. Shipping a table whose every column is unverifiable is the defect the live pass exists to prevent. |

## Page sizes, and what phase 4 confirmed

`files` pins 1000, `drives` and `file_permissions` pin 100 — each the
declared ceiling. A declared bound is not a wire bound (feishu declared
100 and hard-failed above 50, skardi PR #186), so phase 4 probed the
real wire: **all three ceilings hold** (1000/100/100 each 200 with real
credentials; 1001/101/0 each 400), and real multi-page walks at
`pageSize: 1` returned ~444-char cursors with a terminal-page `null` on
every table.

Response size, measured from the live captures: a fully-populated
My Drive row serializes to ~650–690 bytes (shared-drive rows are
smaller), so the largest page (1000 files) is under 1 MiB — far below
the client's 16 MiB cap.

The other phase-4 items beyond the per-table scan, all witnessed: the
pinned all-drives pair really surfaces shared-drive files (the seeded
one came back with its real `drive_id` through skardi-server); it
composes with a `driveId` resource and with `q` (both probed live, and
`q` also verified through a bound binding resource); `size_bytes` is
non-null on real files INCLUDING native Google Docs (a live correction:
Docs report byte sizes; the null-size row is a folder); and `owners` is
absent on shared-drive items exactly as expected — the live absence
pattern is `owners`+`shared`, with `parents`/`starred`/`trashed`
staying present.


---

## Decisions locked at the end of phase 2

Settled with the operator; phase 3 implements exactly these and nothing
wider.

1. **Three tables** — `files`, `drives`, `file_permissions`. The six
   other file-scoped list actions and `changes.list` stay deferred with
   the reasons tabulated above.
2. **`supportsAllDrives` and `includeItemsFromAllDrives` are pinned
   `true`** as fixed inputs on `files`, so the table does not silently
   omit shared-drive files. The pack's most load-bearing assumption;
   phase 4 verifies it directly.
3. **`q` is an optional resource** on `files` and on `drives`. It
   cannot be a filter mapping (it is a query language, not a value), and
   no table on this pack declares any filter mapping.
4. **`capabilities` unmapped, `restrictions` mapped as five boolean
   columns.** These were first treated as one decision, which hid that
   their reasons differ in kind. `capabilities` is a *per-caller*
   permission view ("Capabilities the current user has on this shared
   drive") — its values change with the OAuth identity doing the scan,
   so the same table under two bindings would disagree. That is not
   data, and no amount of declaration would make it a column.
   `restrictions` is drive-owned configuration answering a real
   sharing-audit question, so it ships — at the price of being the
   pack's only fingerprint-uncovered columns, a price paid explicitly
   with phase-4 real rows closing the authoring-error class and the pack
   doc stating plainly that nothing closes future drift.

## Risk register

### R1 — the phase-4 account may see no shared drive at all (RESOLVED 2026-08-25)

**Outcome — a fourth path the three below did not anticipate.** The
account began at the third outcome: `drives.list` returned zero rows
(that clean empty scan was itself witnessed end to end). But
`drives.create` succeeded — the account is a Workspace account able to
create its own shared drive — converting the situation to the first
outcome. The created drive's real row carried all five `restrictions.*`
spellings verbatim (present `false`s, alongside an unmapped nested
`downloadRestriction`), a file seeded into the drive came back through
a live skardi-server scan with `drive_id` carrying the real drive id
(closing decision 2's residual as well), and the shared-drive row
absence pattern was captured (`owners`+`shared` drop; the rest stay).
What could NOT be witnessed on this tenant: flipping any restriction to
`true` (admin-gated, 403 `userCannotChangeDriveRestrictionsUnderOrg` —
a value question, not an extraction-path question), `org_unit_id`
(org-unit feature unused) and `theme_id` (null even when the drive was
created with an explicit `themeId`; the theme materializes as
`color_rgb` + `background_image_link`). The original analysis follows,
kept for the record.

`drives` ships in this wave, but whether the test account is a Google
Workspace account, or a personal account invited into someone else's
shared drive, is **unconfirmed**. If it is neither, `drives.list`
returns zero rows and not one of the table's thirteen columns can be
witnessed non-NULL — which is exactly the standard this document uses
to defer `accessproposals`, `approvals` and `files.listLabels`.

Being wrong here is quiet rather than loud: an empty scan is a
*successful* scan. Registration passes (the fingerprint gate reads the
declared contract, not rows), pagination terminates immediately and
correctly on a null `nextPageToken`, and nothing anywhere fails. A
reader would see a working table and an empty result and conclude the
account has no shared drives — which is true, but says nothing about
whether the thirteen column mappings are right.

**Phase 4 must establish this FIRST, before the per-table scans**, and
report the account type it actually observed. Three outcomes:

- Shared drives visible → scan normally, verify all thirteen columns,
  R1 closes.
- Visible but the account is a non-organizer member → expect
  `color_rgb`, `org_unit_id`, `theme_id` and `background_image_link` to
  come back null (they are organizer-managed); record which columns
  went unwitnessed and why, rather than reporting them as verified.
- No shared drive at all → do NOT report `drives` as verified. Surface
  it and let the operator choose between deferring the table (the
  consistent call) and shipping it with the unverifiable-columns caveat
  recorded here.

**Mapping `restrictions` made R1 load-bearing rather than merely
inconvenient.** Before that decision, a bad R1 outcome meant eight
unwitnessed columns that the fingerprint gate still protected. Now it
also means five columns that are *both* unwitnessed *and* unprotected —
and those five carry key spellings taken from provider documentation
that no capture can check. If R1 lands on the third outcome, the five
restriction columns are the first thing to reconsider, ahead of the
table as a whole.

This also partly gates decision 2: the pinned all-drives pair cannot be
shown to actually surface shared-drive files on an account with no
shared drive. If R1 lands on the third outcome, the pin stays (it is
still the right default) but its verification becomes a residual too.
