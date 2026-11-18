# Feishu Meetings Source Pack

The built-in `feishu_meetings` source pack exposes a Feishu user's **video
meetings and the documents Feishu wrote for them** — the 智能纪要 (smart
notes) and the 逐字稿 (verbatim transcript) — as SQL tables through an
[Open Connector gateway](open-connector.md): `meeting_search`, `meetings`
and `meeting_notes`. The Feishu OAuth token, from the customer's own Feishu
app, lives in Open Connector; Skardi holds only the gateway runtime token.

**`meetings` and `meeting_notes` are the meeting-minutes contract's
tables**, with the column names every meeting pack writes. The rule behind every column: **the tool is
the author of the minutes, and the pack keeps its record.** A column is
filled from exactly one field of Feishu's answer; a column Feishu has no
field for is NULL on every row; a note is a row only because Feishu
answered a document for it. Nothing here calls a model or reads text.

**Its own pack, not tables of the [`feishu` pack](open-connector-feishu.md).**
Feishu meetings are connected as their own provider with their own consent,
over the same `feishu` Open Connector provider.

**The wire contract is Open Connector's.** The gateway's minutes executors
rebuild the page envelope (`items`, `hasMore`, and a `pageToken` that is
`null` on the last page) and wrap a meeting as `{meeting: ...}`, while
passing Feishu's own objects through raw: snake_case keys, times as
Unix-second digit strings. Inputs are camelCase strict
(`additionalProperties: false`; a wrong key such as `query_mode` is a hard
400). Everything below is reconciled against a local gateway built from
Open Connector main `ea4e040e`
([SkardiLabs/open-connector#24](https://github.com/SkardiLabs/open-connector/pull/24)
merged, which gave `get_vc_meeting` its `queryMode`), where every input
set the pack generates passed the strict schemas.

> **No real Feishu tenant has been read.** The row shapes come from
> Feishu's API reference and the executor source; the gateway declares both
> answers loose, so the fingerprint gate protects no column. The
> verification section at the end lists every assumption and the probe that
> settles it.

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
          - name: minutes                    # schema name in SQL
            source_pack: feishu_meetings
            connection_alias: owen-feishu    # the OC connection holding the user's token
            # The search window, RFC 3339 with an offset; both required.
            resource: { startTime: "2026-11-01T00:00:00Z", endTime: "2026-11-08T00:00:00Z" }
            tables: [meeting_search]

          - name: minutes_one                # one binding per meeting id
            source_pack: feishu_meetings
            connection_alias: owen-feishu
            resource: { meetingId: "7000000000000000001" }
            tables: [meetings, meeting_notes]
```

```sql
SELECT meeting_id FROM saas.minutes.meeting_search;

SELECT m.title, m.started_at, n.kind, n.document_path
FROM saas.minutes_one.meetings m
LEFT JOIN saas.minutes_one.meeting_notes n USING (meeting_id);
```

## Tables

### `meeting_search` — `feishu.search_vc_meetings`

One row per meeting in a time window (`POST /vc/v1/meetings/search`): the
listing of meeting ids to read. Not a contract table.

| Column | Type | From |
| --- | --- | --- |
| `meeting_id` | utf8, not null | `id` |

- A search item's only structured field is the meeting `id`. Topic, time
  and organizer exist only inside card text with highlight tags
  (`display_info`, `meta_data.description`), which is prose and never
  parsed; that is why the meeting row comes from a per-meeting read.
- Cursor pagination: `pageToken` in, `$.pageToken` out, `$.hasMore` as the
  end flag, `pageSize` 30 (Feishu's documented cap; the action declares 50).
- Resources: `startTime`, `endTime`, **both required** (the action refuses
  a search with neither a query nor a filter), sent verbatim as
  `meeting_filter.start_time`.
- Feishu's reference says a search returns at most 150 results; walk a
  narrow window.

### `meetings` — `feishu.get_vc_meeting`, `queryMode` 0

One row per meeting, read by id (`GET /vc/v1/meetings/{id}`) with
`queryMode: 0` (the meeting's information) and `includeParticipants: true`.
The response is the row (`row_shape: object`); bind one `meetingId` per
binding. **Feishu answers only the member's own meetings, 90 days back.**

| Column | Type | From |
| --- | --- | --- |
| `provider` | utf8, not null | the constant `feishu` |
| `meeting_id` | utf8, not null | `meeting.id` |
| `series_id` | utf8 | **always NULL** — Feishu documents no series id |
| `title` | utf8 | `meeting.topic` |
| `started_at`, `ended_at` | timestamp (ms, UTC) | `meeting.start_time`, `meeting.end_time` — Unix seconds as digit strings |
| `organizer_email` | utf8 | **always NULL** — no email on a VC meeting |
| `organizer_id` | utf8 | `meeting.host_user.id` — the host's open_id |
| `attendee_emails` | list\<utf8\> | **always NULL** |
| `attendee_ids` | list\<utf8\> | each `meeting.participants[].id` — who joined, as Feishu returns them |
| `url` | utf8 | `meeting.url` |
| `calendar_event_id` | utf8 | **always NULL** — no calendar link on a VC meeting |

### `meeting_notes` — `feishu.get_vc_meeting`, `queryMode` 1

One row per document Feishu wrote for a meeting, from the same action at
`queryMode: 1`, which answers only the meeting's documents:
`related_artifacts.note_doc_token` (the 智能纪要) and
`related_artifacts.verbatim_doc_token` (the 逐字稿).

| Column | Type | From |
| --- | --- | --- |
| `provider` | utf8, not null | `feishu` |
| `meeting_id` | utf8, not null | `meeting.id` |
| `meeting_started_at` | timestamp (ms, UTC) | `meeting.start_time` — the meeting's start, copied onto every note |
| `note_id` | utf8, not null | the document token — the artifact's own id |
| `kind` | utf8 | `notes` for the 智能纪要, `transcript` for the 逐字稿 |
| `producer` | utf8 | `feishu` |
| `title`, `body`, `url` | utf8 | **always NULL** — the answer carries no title, text or link |
| `document_path` | utf8 | the document token, verbatim |

- **The two tokens become two rows through `derive.unpivot`**, the
  loader step added for this pack (see
  [Row derivation: `derive:`](open-connector.md#row-derivation-derive)).
  Notes come first, then the transcript.
- **A token that is absent or `""` is no row.** Per Feishu's reference the
  key is absent when the app lacks its field scope, and `""` when the
  document is permitted but not generated yet. A meeting with neither has
  zero notes; its `meetings` row still stands.
- **The document is reached by `document_path`, never copied.** No body is
  read: that would take a second read per document and the
  `docx:document:readonly` scope, which covers every document the member
  can read. Nothing here reads a transcript.

### No `meeting_action_items`

Feishu returns no action items this pack can tie to a meeting. A VC
meeting carries no minutes token and Feishu documents no meeting-to-minutes
link; `feishu.get_minutes_todos` needs a minutes token, reads an
undocumented endpoint, and answers rows with no meeting id. Feishu yields
no action-item rows, as Google Meet does.

## Authorization and limits

- Scopes: `vc:meeting.search:read` (the search),
  `vc:meeting.meetingevent:read` (the meeting read), and the field scopes
  `vc:meeting.artifact.note:read` and `vc:meeting.artifact.verbatim:read`.
  Without a field scope its token is absent and `meeting_notes` is
  silently empty for that kind — grant both.
- The search needs a user token from a custom app.
- Rate limits per Feishu's reference: search 100 requests a minute; the
  meeting read 1000 a minute, 50 a second.
- Reading both tables costs two meeting reads per meeting, one per mode.
- Provider refusals are not absorbed (`error_path` is unset): they arrive
  as the gateway's failure envelope and fail the scan with its code and
  the action named. That includes reading a listed meeting the member did
  not own.

## Verification

What has been observed, against a local gateway built from Open
Connector main `ea4e040e` (2026-11-18), without Feishu credentials:

- Discovery of both actions; their output and input schemas captured under
  `crates/skardi/src/sources/providers/open_connector/packs/fixtures/feishu_meetings/contracts/`
  and pinned by fingerprint. The `lark.*` twins of both actions have
  identical schemas on the same gateway.
- Every input set the pack generates (`startTime`+`endTime`+`pageSize`,
  the same plus `pageToken`, `meetingId`+`queryMode` 0+`includeParticipants`,
  `meetingId`+`queryMode` 1) accepted by the strict schemas, reaching the
  credential wall (`authorization_failed`, HTTP 403 with `data.status`
  401); a snake_case `query_mode` refused as an input-schema mismatch.

What has **not** been observed — each item is a probe against a real
Feishu tenant:

1. Whether `queryMode` 1 returns `meeting.id` and `meeting.start_time`. If
   not, every note fails on the non-null `meeting_id`, loudly.
2. Whether `queryMode` 0 also returns `note_id` / `related_artifacts`
   (Open Connector's own `get_vc_meeting_note` assumes `note_id` there).
3. That a token is absent without its field scope and `""` before the
   document exists.
4. That a document token belongs to one meeting, so `note_id` is unique
   (a recurring series could share one notes document).
5. The search item shape, and that its `id` is what the meeting read
   accepts.
6. That the window reads `…Z` and `…+08:00` alike.
7. The search page cap (30, or 50, or 200 — the reference says both 30 and
   200) and the 150-result cap.
8. The envelope of an expired search token (`121020`; the reference lists
   search errors as HTTP 500).
9. The envelope for a listed meeting the member did not own (`403 121005`).
10. The envelope for a meeting older than 90 days (`404 121004`).
11. `end_time` for a meeting still running (`"0"` would read as 1970).
12. What `participants[].id` holds for an external guest, a room or a
    phone participant.
13. That `host_user.id` is the host's open_id.
14. That both field scopes are grantable without tenant-admin approval.
15. That Lark answers the same shapes.

The fixtures under
`crates/skardi/src/sources/providers/open_connector/packs/fixtures/feishu_meetings/`
are **synthetic**: shaped by Feishu's reference and the executor source,
with invented meeting ids, open_ids, document tokens and topics like
"Weekly sync". Meetings and their documents are customer content; the live
pass must redact before it replaces them.
