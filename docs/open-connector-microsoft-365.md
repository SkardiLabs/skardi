# Microsoft 365 Source Packs

Microsoft 365 reaches Skardi as **one source pack per Open Connector
service**. There is no `microsoft365` service upstream: the gateway
splits Microsoft Graph into `outlook` (mail only), `one_drive` (files)
and `excel` (spreadsheets), each with its **own OAuth connection** — and
a Skardi binding carries exactly one connection alias, so a
cross-service pack would silently span two OAuth grants and fail half
its tables at scan time when only one service is connected.

This document covers the built-in **`outlook` pack** — `messages` and
`mail_folders` over a mailbox. The `one_drive` pack has its own guide:
**[OneDrive source pack](open-connector-one-drive.md)** (`drive_items`,
`drive_item_search`). Both are Microsoft 365, but each needs its own
OAuth connection and each guide is long enough to stand alone. The
**whole `excel` service is deferred** at the source-pack admission
gate (its list actions emit a `nextLink` continuation but accept no
`nextLink` input, so their pagination cannot be completed — a table
over such an action would present page one as the whole collection).
Teams, SharePoint, Calendar, OneNote, To Do and Planner do not exist in
Open Connector at all; notably, **`outlook` is mail-only** — there is
no calendar or contacts action to bind.

> **Status: live-verified.** The wire contract below is reconciled
> against a live gateway (v1.3.4, open-connector at `2410fbe`) and was
> verified against a real mailbox on 2026-08-19 — every mapped column
> carried real values end to end, and the pack's fixtures are redacted
> live captures. Details in [Live verification](#live-verification-phase-4).

**The wire contract is Open Connector's, not Microsoft Graph's**: the
gateway's outlook executors pass Graph's objects through **raw**
(GitHub-style), so rows are genuine Graph message/mailFolder resources —
but the *declared* schemas under-declare them heavily (`list_messages`
declares no date field at all), which is why fingerprint coverage is
pinned per table and live verification is a prerequisite rather than a
formality. Inputs are the gateway's camelCase strict schema (`top`,
`nextLink`, `mailFolderId`, `includeHiddenFolders`; a wrong key is a
hard 400).

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
          - name: m365                   # schema name in SQL
            source_pack: outlook
            connection_alias: my-msft    # the gateway's outlook OAuth connection
            resource:                    # optional; omit for the whole mailbox
              mailFolderId: "AQMkAGE1..."    # scope messages to one folder
            tables: [messages, mail_folders]
```

```sql
-- A bounded first page: a bare LIMIT stops pagination early, so this
-- reads one page, not the mailbox. Graph returned newest-first in the
-- live pass, but nothing enforces that order — add ORDER BY when the
-- ordering matters, and accept the whole-table cost below.
SELECT from_address, subject, received_date_time
FROM saas.m365.messages
LIMIT 20;

-- The same shape with a predicate or ORDER BY reads the WHOLE table:
-- nothing pushes down (see "No filter pushdown" below), the sort turns
-- LIMIT into a top-k above the scan, and at the default bounds the
-- scan fails past 10 000 messages. Scope it with a mailFolderId
-- binding before running it against a real mailbox (folder-scoped
-- continuation needs a gateway with the open-connector#372 fix; see
-- the upstream-defect note under Pagination).
SELECT from_address, subject, received_date_time
FROM saas.m365.messages
WHERE is_read = false
ORDER BY received_date_time DESC
LIMIT 20;

-- Folder accounting, joined to the messages that live there. Note that
-- `mail_folders` is root-level only, so this inner join silently drops
-- messages sitting in nested folders; use a LEFT JOIN and keep the NULL
-- bucket when total completeness matters more than folder names.
SELECT f.display_name, count(*) AS mails
FROM saas.m365.messages m
JOIN saas.m365.mail_folders f ON m.parent_folder_id = f.id
GROUP BY f.display_name;
```

## Tables

| Table | Action | Resources (optional) | Pagination | Filter pushdown |
|---|---|---|---|---|
| `messages` | `outlook.list_messages` (pinned `select`) | `mailFolderId` | cursor (`top` 100) | none |
| `mail_folders` | `outlook.list_mail_folders` (pinned `includeHiddenFolders: true`) | — | cursor (`top` 1000) | none |

**`messages`** is the mailbox's mail listing — the whole mailbox when
unbound, one folder's listing when the binding pins `mailFolderId`
(Graph swaps `/me/messages` for `/me/mailFolders/{id}/messages`).
Columns: identity (`id`, `web_link`, `internet_message_id`), envelope
(`subject`, `body_preview`, `from_address`/`from_name`,
`sender_address`/`sender_name`, recipient lists as opaque JSON), state (`is_read`, `is_draft`,
`importance`, `flag` as JSON, `categories`), threading
(`conversation_id`, `parent_folder_id`, `has_attachments`) and the four
Graph timestamps (`received_date_time`, `sent_date_time`,
`created_date_time`, `last_modified_date_time`).

The pack **pins `select` to exactly the mapped fields**. Unpinned,
Graph's default representation carries the full (usually HTML) message
body on every row — far past the client's 16 MiB response cap at any
realistic page size — and the row shape becomes "whatever the default
carries". The pin shrinks payloads by orders of magnitude and turns a
misspelled column path into a loud Graph 400 instead of a silently
always-NULL column. Consequence: **`body` is not a column**, by design;
full message content belongs behind a future content-oriented surface,
exactly as on the Gmail pack.

**`body_preview` is body content**: Graph's ~255-character excerpt of
every message's body text, in the select pin and on every row of every
`SELECT *`. The "no bodies" shape above is `body`'s posture, not the
pack's — size content/PII exposure with this column included (the same
call-out the Gmail guide makes for `threads.snippet`).

**`mail_folders`** is the **complete root-level folder set**: the pack
pins `includeHiddenFolders: true` (Graph hides hidden folders by
default) and keeps the distinction queryable through `is_hidden`.
Root-level only — the executor does not recurse; `child_folder_count`
reveals nested folders without enumerating them. Folder ids from this
table are what a `mailFolderId` resource wants. `well_known_name`
carries Graph's locale-independent folder discriminator
(`inbox`/`sentitems`/`drafts`/…, null on custom folders) — prefer it
over `display_name` for cross-account queries, since Graph renders
display names in the account's language (a real MSA mailbox says
"收件箱", not "Inbox").

**No filter pushdown anywhere** — Outlook exposes filtering only as
`filter`, a raw OData *expression* string
(`receivedDateTime ge 2026-01-01T00:00:00Z`), which a per-column
`(input_field, literal)` mapping cannot compose; OneDrive-style list
actions carry none at all. Every predicate runs in DataFusion after the
bounded fetch; `LIMIT` stops pagination early; the practical scoping
tools are the `mailFolderId` resource and `LIMIT` (for `mailFolderId`
past one page, see the upstream-defect note under Pagination). The default safety
bounds fail (never truncate) an unfiltered scan past `max_pages` ×
page-size rows — at the defaults, 100 pages × 100 rows = **10 000
messages per scan** (`max_rows`' 100 000 never binds first here).

## Pagination

Graph paginates with `@odata.nextLink`, re-exposed by the gateway as a
`nextLink` input/output pair. The cursor is a **complete URL**: the
gateway validates it as `format: uri` before credentials, and the
executor pins the host to `graph.microsoft.com` plus an allowlisted
path set — a binding can neither follow a foreign link nor be given
one. Termination is the executor's explicit `nextLink: null` on the
final page, and that is the only spelling this service produces — the
executor writes the key unconditionally, so it never omits it and never
sends `""` (Skardi's engine accepts those spellings from providers that
do use them). A non-advancing
gateway fails as a pagination loop; a present-but-non-string cursor
fails as cursor drift, never as end-of-collection. Continuation
requests still carry `top` (the engine sends the page size on every
request); the executor ignores it there because the link embeds its own
`$top` — cosmetic, but pinned in tests so nobody assumes the page size
is re-applied mid-scan.

**Upstream defect found in the phase-4 live pass, since fixed:**
Graph's continuation URL for a *folder-scoped* listing uses the OData
parenthesized form `/v1.0/me/mailFolders('{id}')/messages`, which the
executor's path allowlist used to reject — a `mailFolderId`-bound scan
whose folder exceeded one page (`page_size: 100`) failed loudly with a
gateway 400 on its second page. Fixed upstream in
[open-connector#372](https://github.com/oomol-lab/open-connector/pull/372)
(merged 2026-08-19, live-verified in that PR); gateways predating the
fix — every tagged release through v1.3.5 — still carry the defect.
Whole-mailbox scans are unaffected either way (`/v1.0/me/messages`
paginates cleanly).

## Authorization

**The OAuth consent is read-write for a read-only pack.** The gateway's
`outlook` scope union requests `User.Read`, `Mail.ReadWrite`,
`Mail.Send`, `MailboxSettings.ReadWrite` and `offline_access` — it must
cover the service's write/send actions, which this pack never calls —
and the *read* actions themselves declare
`requiredScopes: [User.Read, Mail.ReadWrite]` although `Mail.Read`
suffices at the Graph level, so the gateway would refuse a
correctly-scoped read-only token. Expect the Microsoft consent screen
to ask for send-mail and mailbox-write permission; Skardi's tables
remain read-only by construction (the pack binds two list actions and
nothing else), but the consent screen is not, and the narrowing fix
belongs upstream (issue candidate, same class as open-connector#268).

Graph throttles per mailbox with HTTP 429 + `Retry-After`; Skardi's
client honors `Retry-After` on retryable statuses, so the pack adds no
throttling logic of its own.

## Fingerprints and drift

Both actions' output schemas are captured from gateway discovery
(v1.3.4) and pinned; registration re-hashes live discovery and refuses
on mismatch. Because rows are raw passthrough, the fingerprint covers
less than the column set: `mail_folders` has a pinned one-column gap
(`well_known_name`, undeclared upstream), while `messages` has a
pinned thirteen-column
gap — the four timestamps, threading fields, `categories`,
`internet_message_id`, and the `emailAddress` nesting under the
declared-but-loose `from`/`sender`. Drift in those surfaces at scan
time (or as a Graph 400 through the select pin), not at registration.
Input schemas are captured too (`contracts/inputs/`), locked to the
pack's generated keys by test — the registration gate itself remains
output-only, an engine-wide limit tracked for separate work.

## Live verification (phase 4)

Verified on 2026-08-19 against the pinned gateway (v1.3.4, open-connector
`2410fbe`) and a real personal (MSA) mailbox over OAuth, end to end
through a skardi-server SQL scan:

- **Every mapped column carried real non-NULL values** across a seeded
  mailbox (9 messages, 9 root folders): all 22 selected message fields
  arrived with the pinned spellings — including all four passthrough
  timestamps, `hasAttachments`, `conversationId`, `categories`, and the
  `emailAddress` nesting — and all 8 folder columns. Under the select
  pin no message field was ever absent or null; emptiness is spelled
  `""`/`[]`. The single null of the whole pass is `wellKnownName` on the
  custom folder, which is Graph's spelling for "not a well-known
  folder" — preserved as an explicit null in the fixture.
- **Live discovery schemas were byte-identical** to the committed
  contract captures (both actions, both halves); registration passed
  the fingerprint gate against live discovery.
- **Pagination**: a forced `top=2` walk followed real cursors five
  pages to a genuinely-null terminal `nextLink`; no non-empty terminal
  token exists. `top=1000` is accepted on the wire; `top=1001` is a
  schema 400 before credentials. A foreign-host cursor is refused with
  "nextLink must target graph.microsoft.com".
- **Select misspellings fail loudly**: a bad field name returns a Graph
  400 naming the property, surfaced through the gateway failure
  envelope — the behavior the select pin's design leans on.
- **`mailFolderId` forwards verbatim** (an inbox-scoped binding
  returned exactly that folder's rows), with the multi-page upstream
  caveat above.
- **Caveats recorded honestly**: this mailbox had no hidden folders, so
  `includeHiddenFolders: true` was accepted but its on/off responses
  were identical — the pin's effect is unobservable against a folder
  set with nothing hidden. Wire rows carry unmapped extras
  (`@odata.etag` on messages, `sizeInBytes` on folders), left out
  deliberately; `wellKnownName` was promoted to the `well_known_name`
  column as a direct consequence of this pass (the live mailbox's
  display names were all CJK).

The bundled `messages.json` and `mail_folders.json` fixtures are
redacted derivations of these captures (deterministic id maps,
placeholder identities, minute-coarsened timestamps), re-audited on
every test run by `fixtures_stay_redacted`.
