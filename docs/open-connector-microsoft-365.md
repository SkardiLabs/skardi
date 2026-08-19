# Microsoft 365 Source Packs (Outlook)

Microsoft 365 reaches Skardi as **one source pack per Open Connector
service**. There is no `microsoft365` service upstream: the gateway
splits Microsoft Graph into `outlook` (mail only), `one_drive` (files)
and `excel` (spreadsheets), each with its **own OAuth connection** — and
a Skardi binding carries exactly one connection alias, so a
cross-service pack would silently span two OAuth grants and fail half
its tables at scan time when only one service is connected.

This document covers the built-in **`outlook` pack** — `messages` and
`mail_folders` over a mailbox. The `one_drive` pack ships separately;
the **whole `excel` service is deferred** at the source-pack admission
gate (its list actions emit a `nextLink` continuation but accept no
`nextLink` input, so their pagination cannot be completed — a table
over such an action would present page one as the whole collection).
Teams, SharePoint, Calendar, OneNote, To Do and Planner do not exist in
Open Connector at all; notably, **`outlook` is mail-only** — there is
no calendar or contacts action to bind.

> **Status: live verification pending.** The wire contract below is
> reconciled against a live gateway (v1.3.4, open-connector at
> `2410fbe`; inputs validated to the credential wall, row shapes read
> from the executor source, output schemas fingerprint-pinned from
> discovery), but no real mailbox has been scanned yet. The passthrough
> column set — including every timestamp — is provisional until that
> pass runs, and the pack's fixtures are synthetic. This banner comes
> down when phase 4 completes.

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
SELECT from_address, subject, received_date_time
FROM saas.m365.messages
WHERE is_read = false
ORDER BY received_date_time DESC
LIMIT 20;

-- Folder accounting, joined to the messages that live there:
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
(`subject`, `from_address`/`from_name`, `sender_address`/`sender_name`,
recipient lists as opaque JSON), state (`is_read`, `is_draft`,
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

**`mail_folders`** is the **complete root-level folder set**: the pack
pins `includeHiddenFolders: true` (Graph hides hidden folders by
default) and keeps the distinction queryable through `is_hidden`.
Root-level only — the executor does not recurse; `child_folder_count`
reveals nested folders without enumerating them. Folder ids from this
table are what a `mailFolderId` resource wants.

**No filter pushdown anywhere** — Outlook exposes filtering only as
`filter`, a raw OData *expression* string
(`receivedDateTime ge 2026-01-01T00:00:00Z`), which a per-column
`(input_field, literal)` mapping cannot compose; OneDrive-style list
actions carry none at all. Every predicate runs in DataFusion after the
bounded fetch; `LIMIT` stops pagination early; the practical scoping
tools are the `mailFolderId` resource and `LIMIT`. The default safety
bounds fail (never truncate) an unfiltered scan past `max_pages` ×
page-size rows.

## Pagination

Graph paginates with `@odata.nextLink`, re-exposed by the gateway as a
`nextLink` input/output pair. The cursor is a **complete URL**: the
gateway validates it as `format: uri` before credentials, and the
executor pins the host to `graph.microsoft.com` plus an allowlisted
path set — a binding can neither follow a foreign link nor be given
one. Termination is the executor's explicit `nextLink: null` on the
final page (absent and empty spellings also terminate). A non-advancing
gateway fails as a pagination loop; a present-but-non-string cursor
fails as cursor drift, never as end-of-collection. Continuation
requests still carry `top` (the engine sends the page size on every
request); the executor ignores it there because the link embeds its own
`$top` — cosmetic, but pinned in tests so nobody assumes the page size
is re-applied mid-scan.

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
less than the column set: `mail_folders` is fully declared (empty
coverage gap, pinned), while `messages` has a pinned thirteen-column
gap — the four timestamps, threading fields, `categories`,
`internet_message_id`, and the `emailAddress` nesting under the
declared-but-loose `from`/`sender`. Drift in those surfaces at scan
time (or as a Graph 400 through the select pin), not at registration.
Input schemas are captured too (`contracts/inputs/`), locked to the
pack's generated keys by test — the registration gate itself remains
output-only, an engine-wide limit tracked for separate work.
