# Zoom Source Pack

The built-in `zoom` source pack exposes a Zoom user's **AI Companion
meeting summaries** as three stable SQL tables through an
[Open Connector gateway](open-connector.md): `meetings`, `meeting_notes`
and `meeting_action_items`. The Zoom OAuth token lives in Open Connector;
Skardi holds only the gateway runtime token.

**The tables are the meeting-minutes contract's, not Zoom's.** Every
meeting pack (Zoom, Google Meet, Microsoft Teams, Feishu meetings) writes
the same three tables with the same column names, so "what did the last
meetings record" and "which action items name me" are asked once, not
once per tool. The rule behind every column: **the tool is the author of
the minutes, and the pack keeps its record.** A column is filled from
exactly one field of Zoom's answer; a column Zoom has no field for is NULL
on every Zoom row; prose stays prose in `meeting_notes.body`; an action
item is a row only because Zoom returned it as one item. Nothing here
calls a model or reads a name out of a sentence.

**The wire contract is Open Connector's, not Zoom's REST API's.** The
gateway's `zoom` executors rebuild rows with camelCase keys, re-emit Zoom's
`next_page_token` as a top-level `nextPageToken` that is `null` on the last
page, and nest the summary under `summary` beside an `available` flag.
Inputs are camelCase strict (`additionalProperties: false`; a wrong key is
a hard 400). Everything below is reconciled against a local gateway built
from [SkardiLabs/open-connector#21](https://github.com/SkardiLabs/open-connector/pull/21)
(merged to Open Connector main as `dc3cc1c3`, 2026-11-09),
where every input set the pack generates passed the strict schemas.
**Live verification against a real Zoom account has not been done yet**;
the verification section at the end says exactly what has and has not been
observed on the wire.

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
          - name: minutes                  # schema name in SQL
            source_pack: zoom
            connection_alias: owen-zoom      # the OC connection holding the user's token
            # Zoom's listing window, UTC `yyyy-MM-ddTHH:mm:ssZ`; omit both to
            # read Zoom's default window.
            resource: { from: "2026-11-01T00:00:00Z", to: "2026-11-08T00:00:00Z" }
            tables: [meetings]

          - name: minutes_one                # one binding per meeting UUID
            source_pack: zoom
            connection_alias: owen-zoom
            resource: { meetingUuid: "Zm9vYmFyMTIzNDU2Nzg5MA==" }
            tables: [meeting_notes, meeting_action_items]
```

```sql
SELECT meeting_id, series_id, title, started_at, organizer_email
FROM saas.minutes.meetings
ORDER BY started_at DESC;

SELECT n.title, n.body, i.item_id, i.text
FROM saas.minutes_one.meeting_notes n
LEFT JOIN saas.minutes_one.meeting_action_items i USING (meeting_id);
```

## Tables

### `meetings` — `zoom.list_user_meeting_summaries`

One row per meeting the connecting user's AI Companion summarised. The
listing (`GET /users/me/meeting_summaries`) carries ids, topic, host and
times, never content.

| Column | Type | From |
| --- | --- | --- |
| `provider` | utf8, not null | the constant `zoom` |
| `meeting_id` | utf8, not null | `meetingUuid` — the meeting instance UUID, one per occurrence |
| `series_id` | utf8 | `meetingId` — the numeric id a recurring meeting keeps across occurrences, rendered as text |
| `title` | utf8 | `meetingTopic` |
| `started_at`, `ended_at` | timestamp (ms, UTC) | `meetingStartTime`, `meetingEndTime` — UTC RFC 3339 seconds |
| `organizer_email`, `organizer_id` | utf8 | `meetingHostEmail`, `meetingHostId` |
| `attendee_emails`, `attendee_ids` | list\<utf8\> | **always NULL** — a Zoom summary carries no attendees |
| `url`, `calendar_event_id` | utf8 | **always NULL** — no meeting link or calendar event on a summary |

- Cursor pagination: `nextPageToken` in, `$.nextPageToken` out, `pageSize`
  30 (Zoom's default). The gateway always sends the key and spells the end
  as `null`.
- **Zoom's page token expires fifteen minutes after it is issued.** An
  expired token is Zoom's 400, forwarded by the gateway as `invalid_input`;
  the scan fails naming the code and the action rather than returning a
  truncated listing. Walk a window in one go.
- Resources: `from`, `to` (optional), forwarded verbatim. `userId` is never
  sent (the connecting user, `me`); `timeFilterField` is not declared
  (Zoom's default, `summary_start_time`).
- No filter pushdown: predicates run in DataFusion over the fetched window.

### `meeting_notes` — `zoom.get_meeting_summary`

One row per summary, the one Zoom keeps per meeting instance
(`GET /meetings/{uuid}/meeting_summary`). The response is the row
(`row_shape: object`); bind one `meetingUuid` per binding.

| Column | Type | From |
| --- | --- | --- |
| `provider` | utf8, not null | `zoom` |
| `meeting_id` | utf8, not null | `summary.meetingUuid` |
| `meeting_started_at` | timestamp (ms, UTC) | `summary.meetingStartTime` — copied onto the note so a storage cap evicts the oldest meetings' notes first |
| `note_id` | utf8, not null | `summary.meetingUuid` — Zoom gives the summary no id of its own |
| `kind` | utf8 | `summary` |
| `producer` | utf8 | `zoom_ai_companion` |
| `title` | utf8 | `summary.summaryTitle` |
| `body` | utf8 | `summary.summaryContent` — Zoom's unified Markdown summary |
| `document_path` | utf8 | **always NULL** — the summary is text, not a document |
| `url` | utf8 | `summary.summaryDocUrl` |

- The legacy `summaryOverview` / `summaryDetails` are not columns: Zoom
  marks them deprecated and the Markdown holds them.
- **A summary Zoom will not return is zero rows, not an error.** Zoom's
  `403` code `2305` covers three causes it does not tell apart by code —
  summary access restricted to IP ranges, summaries shared by email only,
  or the summary in Trash — and comes back from the action as a successful
  answer with `available: false`, `unavailableReason: not_accessible`
  (Zoom's own wording in `unavailableMessage`) and `summary: null`; the
  pack skips it, so the meeting has no note and no items while its
  `meetings` row still stands. Any other `401`/`403` is the gateway's
  `authorization_failed` and fails the scan.
- A meeting UUID can start with `/` or contain `//`; the pack passes it
  verbatim and the action encodes it into Zoom's path (twice, as Zoom
  requires).

### `meeting_action_items` — `zoom.get_meeting_summary`

One row per next step Zoom returned **as an item**: the `nextSteps[]`
strings of the same summary response.

| Column | Type | From |
| --- | --- | --- |
| `provider` | utf8, not null | `zoom` |
| `meeting_id` | utf8, not null | `summary.meetingUuid` |
| `note_id` | utf8 | `summary.meetingUuid` |
| `item_id` | utf8, not null | `<note_id>#<position>` — never a bare position |
| `text` | utf8 | the string, verbatim |
| `owner_name`, `owner_email`, `owner_id` | utf8 | **always NULL** — nothing is read out of the text |
| `due` | timestamp (ms, UTC) | **always NULL** |
| `done` | boolean | **always NULL** |

- Zoom marks `next_steps` deprecated and also returns
  `edited_summary.next_steps` when someone edited the summary. The pack
  reads the edited list when it is present and non-empty, else the legacy
  list; when both are absent or empty it writes **zero rows** and the note
  still stands. The Markdown body is never split into items.

## How the contract shape is produced: `derive:`

Zoom's wire rows carry neither a `provider` label nor one row per next
step, so this pack introduces a declarative `derive:` block in the pack
format, applied after a page's rows are extracted and before the ordinary
converter reads them, in the scan shared by the engine and its other
consumers:

- `constants` — keys written on every row (`provider: zoom`, or an explicit
  `null` for a contract column the tool has no field for); a derived key
  always wins over a wire key of the same name.
- `explode` — one row per element of the first non-empty array among the
  listed paths; the element lands under `into`, its position under `index`.
  Explode consumes its paths: a derived row carries the element and the
  row's other keys (`summary.meetingUuid` beside `summary.nextSteps`), but
  none of the listed arrays, so an array of `n` elements is never copied
  onto each of its own `n` rows. A column or a `format` placeholder that
  reads a listed path is refused when the pack loads.
- `format` — a key rendered from a template over the row
  (`"{summary.meetingUuid}#{position}"`); a missing part makes the whole
  value NULL, so a non-null identity fails loudly.
- `skip_when_null` — drop the row when the named path is missing or null.

Pagination advances on the raw page, so an explode that yields nothing
from a non-empty page cannot end a listing early. Every pre-existing pack
declares no `derive:` and loads exactly as before.

## Authorization and limits

- Scopes (granular): `meeting:read:list_summaries` for the listing,
  `meeting:read:summary` for the summary; both optional scopes of the
  gateway's `zoom` provider, requested at connect.
- A user-managed Zoom app reads only what the connecting user may see;
  whether a participant's token (not the host's) lists a summary the host
  shared is **not answered** yet.
- Rate label LIGHT; limits are per Zoom account, shared by every installed
  app.
- Summaries exist only on Pro-or-higher accounts with AI Companion on;
  end-to-end-encrypted meetings get none.

## Verification

What has been observed, against a local gateway built from Open
Connector main `dc3cc1c3` (open-connector#21 merged, 2026-11-09), without
Zoom credentials:

- Discovery of both actions; their output and input schemas captured under
  `fixtures/zoom/contracts/` and pinned by fingerprint.
- Every input set the pack generates (`pageSize`+`from`+`to`, the same
  plus `nextPageToken`, `meetingUuid` alone) accepted by the strict
  schemas, reaching the credential wall; a snake_case key refused as
  `invalid_input`.

What has **not** been observed, and is pending the live pass against a
real Zoom account: real rows through skardi-server (every mapped column
extracting a real non-NULL value somewhere), real multi-page pagination
and the real final page, a real `403 2305`, whether `next_steps` is still
returned and how `edited_summary.next_steps` behaves when an editor
deletes every step. The fixtures under `fixtures/zoom/` are **synthetic**:
shaped by the executor source and its tests, with invented UUIDs,
`host@example.com` and topics like "Weekly sync". Zoom summaries are
customer content; the live pass must redact before it replaces them.
