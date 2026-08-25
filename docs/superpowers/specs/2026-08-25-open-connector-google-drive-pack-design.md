# Google Drive Source Pack

Milestone 5.9. Phase-1 record for the `google_drive` source pack: the wire
contract as reconciled against a **live** Open Connector gateway on
2026-08-25 (gateway v1.3.4, open-connector `2410fbe` — the same upstream
pin milestone 5.8 captured against). Table design (phase 2) and the
implementation decisions (phase 3) are appended to this document as those
phases run.

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

Live discovery for all ten actions is captured (input and output halves)
and will be committed under
`packs/fixtures/google_drive/contracts/` — output as the fingerprint
input, `contracts/inputs/` alongside — once phase 2 settles which
actions ship, following gmail (5.5) and one_drive (5.8).

**The captured output schema is fingerprint input, not column truth.**
Rows here are normalized rather than passthrough, which makes the
declared schema a much stronger predictor than it was for `one_drive` —
but conditional key spreads mean a declared key can still be absent on
every real row, so no column set is final until phase 4 scans real data.
