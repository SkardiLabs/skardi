# Outlook Source Pack (Microsoft 365)

**Status:** Phases 1–4 complete (phase 3 implemented 2026-08-19; phase 4
live-verified the same day against a real MSA mailbox — outcomes
recorded in the tasks doc's 5.7 entry and packs/outlook.rs's module
doc). Phase 5 review pending.
**Date:** 2026-08-14
**Branch:** `feature/open-connector-m365-packs`
**Milestone:** 5.7 (see [Milestone numbering](#milestone-numbering))

## Summary

Microsoft 365 reaches Skardi as **two source packs, not one**: `outlook`
(mail) and `one_drive` (files). Open Connector has no `microsoft365`
service — it splits Microsoft Graph into three separate services with
three separate OAuth connections, and every existing Skardi pack is 1:1
with one Open Connector service. **This document covers the `outlook`
pack only** — the `one_drive` pack ships as its own milestone and PR,
and its phase-1/2 reconciliation and table design live in this
document's original two-pack revision in git history. Two tables pass
the source-pack admission gate:

| Table | Action | Pagination | Resources |
|---|---|---|---|
| `outlook.messages` | `outlook.list_messages` | cursor `nextLink` → `$.nextLink` | optional `mailFolderId` |
| `outlook.mail_folders` | `outlook.list_mail_folders` | cursor `nextLink` → `$.nextLink` | — |

The whole `excel` service is **deferred at the gate**: its list actions
emit a `nextLink` but do not accept one, so their pagination cannot be
completed. Teams, SharePoint, Calendar, OneNote, To Do and Planner are
absent from Open Connector entirely — there is nothing to bind.

Two properties of this provider set the shape of the work. First, both
list actions are **raw passthrough** over `additionalProperties:
true` item schemas, and `outlook.list_messages` declares *no timestamp
field at all* — `receivedDateTime` and every other date live wholly
outside the fingerprint gate. A mail table without `receivedDateTime` is
useless, so phase 4 (live real-row verification) is a hard prerequisite
here rather than a final check. Second, Outlook exposes filtering only as
a raw OData `$filter` **expression**, which a `(input_field, literal)`
filter mapping cannot compose; the pack therefore ships with zero
predicate pushdown, following the Notion pack's precedent.

## What Open Connector actually exposes for Microsoft 365

Probed live against a local gateway (v1.3.4, `open-connector` at
`2410fbe`). No assumption below is taken from Microsoft's own docs.

| Service | Actions | List-shaped read actions | Gate verdict |
|---|---|---|---|
| `outlook` | 11 — **mail only**, no calendar or contacts | `list_messages`, `list_mail_folders` | ✅ both paginate completely |
| `one_drive` | 13 | `list_folder_children`, `search_items` | ✅ both paginate completely |
| `excel` | 31 | `list_worksheets`, `list_tables`, `list_table_rows`, `list_table_columns`, `list_drive_item_children`, `search_files` | ❌ all deferred |

```bash
curl -s -H "$TOK" "http://localhost:3000/v1/actions?service=outlook"
```

`outlook` is mail-only. The provider is named for the product but backed
purely by Graph's `/me/messages`, `/me/mailFolders` and
`/me/mailboxSettings` endpoints; there is no `list_events`,
`list_calendars` or `list_contacts` action to bind. Anyone expecting an
"Outlook calendar" table gets nothing, and that absence belongs in the
pack doc rather than in a reviewer's head.

### Why every Excel table is deferred

`excel.list_worksheets`, `list_tables`, `list_table_rows`,
`list_table_columns`, `list_drive_item_children` and `search_files` all
declare `nextLink` in their **output** schema and omit it from their
**input** schema:

```
excel.list_worksheets
  inputSchema.additionalProperties: False
  required: ['itemId']
    - itemId: string
    - driveId: string
    - sessionId: string
  outputSchema top keys: ['worksheets', 'nextLink']
```

The continuation token is emitted and then unusable. A pack table over
such an action would read the first page and silently present it as the
whole collection — precisely the "mostly working" table the admission
gate forbids ("complete terminating pagination"). Slack's message
history was deferred for the same class of reason in milestone 5.2.

The fix is upstream: add a `nextLink` input to the Excel list actions,
mirroring what `outlook` and `one_drive` already do. Until then Excel
tables stay absent and documented as absent. This also removes any
`list_table_rows`-shaped "spreadsheet as SQL table" story from this
milestone — worth stating plainly, because that is the first thing most
people expect a "Microsoft 365 pack" to deliver.

## Phase 1 — live contract reconciliation

Gateway v1.3.4. Discovery and input validation probed without any
provider credential; row shapes read from executor source.

### Input schemas, verbatim

Every action is `additionalProperties: false`, so a wrong key is a hard
400 and the camelCase spellings below are the contract:

| Action | Inputs | `required` |
|---|---|---|
| `outlook.list_messages` | `mailFolderId`, `top` (1–1000), `filter`, `orderby`, `select[]`, `nextLink` (`format: uri`), `bodyContentType` (`text`\|`html`) | none |
| `outlook.list_mail_folders` | `nextLink` (`format: uri`), `includeHiddenFolders`, `top` (1–1000), `select[]` | none |

Note `outlook.list_messages` spells its sort input `orderby` (all
lower-case) while `one_drive` spells it `orderBy`. Neither is mapped by
this design, but the inconsistency is a live trap for anything that
later wants to.

### Credential-wall calibration

The no-credential validation trick was calibrated before being trusted,
per the reconciliation reference:

| Probe | Result |
|---|---|
| `{"per_page":5}` → `outlook.list_messages` | 400 `invalid_input` |
| `{"top":50}` → `outlook.list_messages` | 403 `authorization_failed`, "Connect outlook with OAuth first." |
| `{"top":100,"includeHiddenFolders":true}` → `outlook.list_mail_folders` | 403 (reaches credential wall) |
| `{"top":1000}` / `{"top":1001}` → `outlook.list_messages` | 403 / 400 — declared bound is enforced |

The two responses differ, so a 403 genuinely proves the input passed the
strict schema.

### `nextLink` is a URI-validated, host-pinned cursor

`nextLink` carries `format: uri` and the gateway enforces it *before*
credentials: `{"nextLink":"not-a-url"}` → 400 `invalid_input`. The
executor then pins the host and the path:

```ts
if (target.hostname !== graphHost) {
  throw new ProviderRequestError(400, "nextLink must target graph.microsoft.com");
}
```

`{"nextLink":"https://evil.example.com/v1.0/me/messages"}` reaches the
credential wall (403) rather than being accepted, and `assertAllowed…`
additionally restricts the path to `/v1.0/me/messages`,
`/v1.0/me/mailFolders/{id}/messages` and `/v1.0/me/mailFolders`.

Consequence for phase 3: **every cursor in a fixture or a `MockGateway`
stub must be URI-shaped.** A `"cursor-2"`-style token would pass the
mocks and be rejected by the real gateway with a 400 — the exact class of
mock-encoded-wrong-assumption failure that shipped in the original
GitHub pack.

### Rows are raw Graph objects, and the mail schema declares no dates

Both executors pass Graph's objects through untouched:

```ts
return {
  messages: Array.isArray(payload.value) ? payload.value : [],
  nextLink: typeof payload["@odata.nextLink"] === "string" ? payload["@odata.nextLink"] : null,
};
```

The declared item schemas are `looseObject` (`additionalProperties:
true`) and under-declare heavily. Declared properties are:

- **messages**: `id`, `subject`, `bodyPreview`, `importance`, `isRead`,
  `isDraft`, `webLink`, `body`, `sender`, `from`, `toRecipients`,
  `ccRecipients`, `bccRecipients`, `replyTo`, `flag` — **no date field
  of any kind**, no `hasAttachments`, no `conversationId`, no
  `parentFolderId`, no `categories`.
- **mailFolders**: `id`, `displayName`, `parentFolderId`,
  `childFolderCount`, `unreadItemCount`, `totalItemCount`, `isHidden`.

This is the Notion `archived`/`is_archived` trap at a larger scale.
Passthrough columns ride `additionalProperties`, so a mis-spelled or
non-existent field raises **no error at registration and no error at
scan** — it is simply NULL forever. Every message column beyond the 15
declared names is therefore unverifiable until phase 4, and the columns
that matter most (`receivedDateTime`, `sentDateTime`, `hasAttachments`,
`conversationId`) are all in that gap. The coverage-gap pin
(`fingerprint_uncovered_columns`) will make the gap an explicitly
reviewed set rather than an accident.

### In-band errors: `error_path: None`

`assertOutlookResponse` throws
`ProviderRequestError` on any non-2xx, so Graph's error envelope never
reaches Skardi as an HTTP-200 body. The pack declares `error_path:
None`, and an e2e test will pin that a Graph failure (e.g. a scope
error) surfaces through the gateway-failure path instead.

### Pagination: cursor with a null terminator

`nextLink` is `null` on the final page (`readNextLink` normalizes a
missing `@odata.nextLink` to `null`), so termination is the plain
null/absent/empty cursor spelling and `has_more_path` is **not**
declared — Graph carries no separate has-more boolean, unlike Feishu
wiki. Phase 4 must still follow the real final page's token to confirm
no non-empty terminal cursor exists.

The engine sends `page_size_param` on every request, including cursor
pages (`pagination.rs`). The Outlook executors ignore every
other input once `nextLink` is present, because Graph embeds `$top`,
`$select` and `$filter` in the link itself:

```ts
query: nextLink ? undefined : compactObject({ $top: …, $select: … })
```

So a redundant `top` rides along on continuation requests. It is a
declared property, so it cannot 400, and it changes nothing — but the
module doc must say so, otherwise the next reader assumes the page size
is being re-applied per page.

## Phase 2 — table design

Column sets below are **provisional**: for passthrough rows only phase 4
settles them. Identity columns are non-null; everything else is
nullable.

### `outlook.messages`

`row_path: "$.messages"`, optional resource `mailFolderId` (the verbatim
OC input key — a binding scoping the table to one folder, e.g. Inbox).

Declared-schema columns: `id`, `subject`, `body_preview`, `importance`,
`is_read`, `is_draft`, `web_link`, `flag` (json).
`from`/`sender` resolve through nested scalar paths
(`from.emailAddress.address`, `from.emailAddress.name`) — those work
today.
Recipient lists (`to_recipients`, `cc_recipients`, `bcc_recipients`) map
as **`json`** columns: Graph's shape is
`[{"emailAddress":{"address","name"}}]`, and
`utf8_list_from_object_key` plucks exactly one key level, which
`emailAddress` (an object) does not satisfy. Extending the plucker to a
nested key path was considered and deliberately not taken in this
milestone — it is an engine change for one column shape, and `json`
loses nothing but ergonomics.
`body` is deliberately **not** mapped: it is the full message body (HTML
for most mail) and belongs behind an explicit projection, not in every
`SELECT *`.

Passthrough columns to confirm live: `received_date_time`,
`sent_date_time`, `created_date_time`, `last_modified_date_time`,
`has_attachments`, `conversation_id`, `parent_folder_id`, `categories`,
`internet_message_id`.

### `outlook.mail_folders`

`row_path: "$.mailFolders"`, no resources.
`fixed_inputs: { includeHiddenFolders: true }` — the `state=all` move:
Graph hides hidden folders by default, so the pin makes the table the
complete root-level folder set while the `is_hidden` column keeps the
distinction queryable.
Columns: `id`, `display_name`, `parent_folder_id`, `child_folder_count`,
`unread_item_count`, `total_item_count`, `is_hidden`; `size_in_bytes` is
a phase-4 candidate.

**Root-level only.** The executor calls `me/mailFolders` with no
recursion, so nested folders are not enumerated; `child_folder_count`
reveals their existence without listing them. This is a documented
limitation of the table, not a defect — the collection the table claims
to be (root-level folders) does terminate completely.

### Filters: none, deliberately

The pack maps no filter, and the reason is structural rather than an
omission: `outlook.list_messages` exposes filtering only as `filter`, a
raw OData **expression** string
(`receivedDateTime ge 2026-01-01T00:00:00Z`). A `FilterMapping` renders
one value into one input field; it cannot compose an expression, and
pushing a bare value into `$filter` would produce a 400 or, worse, a
silently wrong query.

The Notion pack ships zero filters for comparable reasons, so this is
precedented. Consequences to state honestly in the pack doc: predicates
are re-applied locally by DataFusion after a full scan, bounded by
`max_rows`/`max_pages`; the practical scoping tools are the
`mailFolderId` resource and `LIMIT` early-stop.

Each unmapped input needs a negative-space guard test proving no
`filter`, `orderby`, `orderBy`, `select` or `expand` key ever reaches
the wire.

## Decided (2026-08-19): `select` is pinned on `outlook.messages`

**Approved and shipped** — both levers below, as recommended:
`fixed_inputs.select` pins exactly the mapped fields (22 entries) and
`page_size: 100`. Phase-4 live verification confirmed the property the
pin leans on: a misspelled select field returns a Graph 400 naming the
property, never a silently always-NULL column. The analysis that
produced the decision is kept below as the record.

Unpinned, the pack would send no `select`, so Graph returns its
**default full message representation on every row — including
`body.content`**. Skardi's client caps a response at
`DEFAULT_MAX_RESPONSE_BYTES` = 16 MiB. At the declared maximum
`top: 1000`, a page of ordinary HTML mail (100 KB of body each is
unremarkable) is far past that cap, and the scan fails on a
response-size error rather than on anything the user did.

Two levers, and they interact:

1. **Pin `fixed_inputs: select: [...]`** to exactly the mapped fields.
   Graph then omits everything else: payloads shrink by one to two
   orders of magnitude, and rows become deterministic instead of
   "whatever the default representation carries". Cost: any column not
   in the list is always-NULL, so the pin and the column set must be
   maintained together, and the passthrough surface the coverage-gap pin
   reports becomes intentional rather than incidental. Precedent exists
   (Slack's `types` pin, Notion's `filter` pin).
2. **Choose a conservative `page_size`** well under 1000 (100 is the
   natural candidate) so a single page cannot approach the cap even
   unpinned. Note `page_size` doubles as the LIMIT-pushdown ceiling.

Recommendation, since taken: do both — pin `select` to the mapped fields
*and* set `page_size: 100`. The cost lever 1 names — the pin and the
column set must be maintained together — is carried mechanically by
`select_pin_mirrors_the_mapped_columns`, which fails the moment the two
drift apart.

## What phase 4 must settle

Live verification is not a formality for this pack. It must:

1. Confirm the real spelling of every undeclared message column
   (`receivedDateTime` et al.) — the whole set is invisible to the
   fingerprint gate and would be silently always-NULL if misspelled.
2. Confirm the wire's real `top` ceiling against the declared 1000.
   Feishu declared 100 and hard-failed above 50 (skardi PR #186); a
   declared bound is not a wire bound.
3. Follow the token returned by the **real final page** and confirm
   `nextLink` is genuinely null there, so null-cursor termination cannot
   refetch and trip the loop guard the way Feishu wiki did.
4. Force multi-page pagination with a small `top` and confirm the cursor
   round-trips through the executor's host/path allowlist.
5. Confirm `includeHiddenFolders: true` actually returns rows (a pinned
   input that returns nothing looks identical to an empty account).
6. Confirm a real `mailFolderId` forwards verbatim.
7. Re-derive every fixture as a redacted live capture, with a mechanical
   audit of surviving string values.

**This needs the user's own Microsoft account and Azure app** — an Entra
app registration with `http://localhost:3000/oauth/callback` as a
redirect URI, `PUT /api/oauth/configs/outlook` with its
`clientId`/`clientSecret`, and a browser authorization for the service.
Credentials stay entirely on the user's side.

## Authz and rate limits

**The OAuth consent is read-write for a read-only pack.** The scope
unions the gateway requests are:

- `outlook`: `User.Read`, `Mail.ReadWrite`, `Mail.Send`,
  `MailboxSettings.ReadWrite`, `offline_access`

Read-only scopes exist in the provider's own scope map (`Mail.Read`,
`Mail.ReadBasic`, `MailboxSettings.Read`) and go unused by
the union. Worse, the *read* actions declare
`requiredScopes: [User.Read, Mail.ReadWrite]`, so the gateway's own
scope check would refuse a correctly-scoped read-only token. A user
binding this pack must therefore grant send-mail
permission to read their mailbox. Skardi's tables remain
read-only by construction, but the consent screen is not, and the pack
doc must say so rather than let the user discover it at the Microsoft
consent prompt.

Two OC services means **two OAuth connections**: one Entra app can back
both, but the user authorizes `outlook` and `one_drive` separately, and
a `connection_alias` must exist under each service that a binding's
tables touch (connections are keyed by `(service, connectionName)`).
This is also the strongest argument for the two-pack split — one binding
maps to exactly one connection.

Graph enforces per-mailbox and per-drive throttling with HTTP 429 plus
`Retry-After`. Skardi's client already honors `Retry-After` on
retryable statuses and retries pre-execution 429s for POST execute
(`MAX_ATTEMPTS = 3`), so the pack adds no throttling logic of its own;
the pack doc cites Microsoft's throttling guidance and the concrete
limits get recorded during phase 4.

## Decisions and rejected alternatives

**Two packs (`outlook`, `one_drive`) rather than one `microsoft365`
pack.** A single cross-service pack was considered: it matches the
product name, and it is technically viable since alias resolution is
per-`(service, connectionName)`. It was rejected because a binding
carries exactly one `connection_alias`, so a `microsoft365` binding
silently spans two independent OAuth grants and half its tables fail at
scan time when only one service is connected — a failure that surfaces
late and reads as a Skardi bug. Every existing pack (`github`, `slack`,
`notion`, `feishu`) is 1:1 with an OC service, and table IDs are a
stable contract that a later split would have to break. A single
`docs/open-connector-microsoft-365.md` holds the Microsoft 365 story for
both packs — it ships covering `outlook` alone and gains the `one_drive`
tables with that milestone — so the product-level story survives the
split.

**Recipients as `json` rather than extending
`utf8_list_from_object_key` to nested key paths.** The extension is
small and backward-compatible, but it is an engine surface added for one
column shape; `json` costs ergonomics only. Revisit if more providers
want nested plucking.

**Excel deferred rather than shipped single-page.** Covered above: the
gate requires complete terminating pagination and the actions cannot
provide it. Upstream `nextLink` inputs are the fix.

## Upstream issue candidates (oomol-lab/open-connector)

1. Excel list actions emit `nextLink` but accept no `nextLink` input —
   pagination is unusable. Precedent for such a contribution:
   open-connector#228 (`pageInfo.fetched` on
   `github.list_repository_issues`).
2. `outlook` / `one_drive` OAuth scope unions request write and send
   scopes for read-only use, and read actions declare
   `Mail.ReadWrite`/`Files.ReadWrite` in `requiredScopes` although
   `Mail.Read`/`Files.Read` suffice at the Graph level. Same class as
   open-connector#268 (Feishu's `requiredScopes` naming the wrong
   scope).
3. `outlook.list_messages` spells its sort input `orderby` while
   `one_drive` spells it `orderBy`.

Neither of the first two blocks this milestone; both belong in the PR
body as known-upstream facts.

## Milestone numbering

#192 (Gmail) and #198 (Discord) merged as 5.5 and 5.6. The `outlook`
pack takes **5.7**; the `one_drive` pack follows as its own milestone
and PR, restoring its design from this document's two-pack revision in
git history.
