# Google Meet Source Pack

The built-in `google_meet` source pack exposes the **Google Meet
conferences a user organized and the notes Gemini wrote for them** ("Take
notes for me") as SQL tables through an
[Open Connector gateway](open-connector.md): `meetings` and
`meeting_notes`. The Google OAuth token lives in Open Connector; Skardi
holds only the gateway runtime token.

**`meetings` and `meeting_notes` are the meeting-minutes contract's
tables**, with the column names every meeting pack writes. The rule behind
every column: **the tool is the author of the minutes, and the pack keeps
its record.** A column is filled from exactly one field of Google's answer;
a column Google has no field for is NULL on every row; a note is a row only
because Google answered a smart note for it. Nothing here calls a model or
reads text.

**Two limits come from Google, not from the pack:**

- **Only meetings the user organized.** Google's list method "only returns
  conferences where you're the meeting organizer". A meeting the user only
  attended does not appear.
- **30 days at most.** Google deletes a conference record 30 days after the
  conference ends, so no older meeting can be listed, whatever window is
  asked for.

**The wire contract is Open Connector's.** The gateway rebuilds the list
envelope (`conferenceRecords` always present, `nextPageToken` `null` on
the last page) and passes Google's own objects through raw: camelCase keys,
times as RFC 3339 strings with up to nine fractional digits. Inputs are
strict (`additionalProperties: false`). Both tables' contracts were
captured from a local gateway built from Open Connector
`27e6b0fbff35ae06858651a2b2bf31251053d7a8`. That is the head of
[SkardiLabs/open-connector#30](https://github.com/SkardiLabs/open-connector/pull/30)
(`get_conference_artifacts`) until it merges. Its base is main
`ce9952024d004895ffa80b6525361fa9c2a01eed`, which includes
[SkardiLabs/open-connector#22](https://github.com/SkardiLabs/open-connector/pull/22)
(the start-time window). Every input set the pack generates passed the
strict schemas.

> **No real Google account has been read.** The row shapes come from
> Google's Meet REST v2 discovery document and guides and the executor
> source. The conference record and the smart note are declared with their
> fields, so the fingerprint gate protects the wire columns.

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
          - name: meet                       # schema name in SQL
            source_pack: google_meet
            connection_alias: owen-google    # the OC connection holding the user's token
            # Optional window on the conference's start, RFC 3339.
            resource: { startTime: "2026-11-01T00:00:00Z", endTime: "2026-11-08T00:00:00Z" }
            tables: [meetings]

          - name: meet_one                   # one binding per conference record
            source_pack: google_meet
            connection_alias: owen-google
            resource: { name: "conferenceRecords/abc-123" }
            tables: [meeting_notes]
```

```sql
SELECT meeting_id, started_at, ended_at FROM saas.meet.meetings;

SELECT note_id, document_path, url FROM saas.meet_one.meeting_notes;
```

## Tables

### `meetings` — `googlemeet.list_conference_records`

One row per conference the user organized: one occurrence of a call in a
Meet space, newest first. Every column is on the listed record, so the
table needs no per-meeting read.

| Column | Type | From |
| --- | --- | --- |
| `provider` | utf8, not null | the constant `google_meet` |
| `meeting_id` | utf8, not null | `name` — `conferenceRecords/{id}`, one per occurrence |
| `series_id` | utf8 | **always NULL** — a Meet space is a reusable room, not a series |
| `title` | utf8 | **always NULL** — a conference record has no title |
| `started_at` | timestamp (ms, UTC) | `startTime` (always set) |
| `ended_at` | timestamp (ms, UTC) | `endTime` — NULL while the conference is still running |
| `organizer_email`, `organizer_id` | utf8 | **always NULL** — no organizer field on a conference record |
| `attendee_emails`, `attendee_ids` | list\<utf8\> | **always NULL** — participants are a per-meeting read this version does not make |
| `url` | utf8 | **always NULL** — the join link is on the space, not the record |
| `calendar_event_id` | utf8 | **always NULL** — no calendar link on a conference record |

- Cursor pagination: `pageToken` in, `$.nextPageToken` out, `null` on the
  last page; `pageSize` 100, Google's maximum.
- Resources: `startTime`, `endTime`, **both optional**, RFC 3339, sent
  verbatim. The action turns them into
  `start_time>="…" AND start_time<="…"` — inclusive at both ends — and
  refuses a bound that is not RFC 3339 before any request.
- Times carry up to nine fractional digits; the millisecond column
  truncates the rest.

### `meeting_notes` — `googlemeet.get_conference_artifacts`

One row per Gemini smart note of one conference, read by the conference
record's `name`. The action answers `{conferenceRecord, smartNotes}`: the
record beside every smart-note session of it, oldest first, so every note
carries its meeting's id and start from a field. The gateway walks the
smart notes at 100 a page for at most ten pages. Past that it answers a
provider error rather than a short list.

| Column | Type | From |
| --- | --- | --- |
| `provider` | utf8, not null | `google_meet` |
| `meeting_id` | utf8, not null | `conferenceRecord.name` |
| `meeting_started_at` | timestamp (ms, UTC) | `conferenceRecord.startTime` — the meeting's start, copied onto every note |
| `note_id` | utf8, not null | the smart note's `name`, `conferenceRecords/{c}/smartNotes/{s}` |
| `kind` | utf8 | `notes` |
| `producer` | utf8 | `gemini` |
| `title`, `body` | utf8 | **always NULL** — the answer carries no title and no text |
| `document_path` | utf8 | `docsDestination.document` — the Google Docs id, which is also the Drive file id |
| `url` | utf8 | `docsDestination.exportUri` |

- **Each smart note becomes one row through `derive.explode`** (see
  [Row derivation: `derive:`](open-connector.md#row-derivation-derive)). A
  conference without notes is zero rows, not an error; its `meetings` row
  still stands.
- **A note Google has not written yet** has no `docsDestination`: its row
  stands with NULL `document_path` and `url`, and a later read of the same
  conference fills them.
- **The notes Doc is reached by `document_path`, never copied.** The Doc
  lives in the organizer's Drive, so the [Google Drive
  pack](open-connector-google-drive.md) lists it under the same file id.
  Reading its text would take `documents.readonly` (a sensitive scope) or
  `drive.readonly` (a restricted one) and a second read per Doc.
- **Transcripts are not read.** No `transcript` kind in this version.

### No `meeting_action_items`

Gemini's "Next steps" are a section of the notes Doc — prose — and the
Meet API's smart note has no item field. Google Meet yields no action-item
rows.

## Authorization and limits

- Scope: `https://www.googleapis.com/auth/meetings.space.readonly` for both
  actions, a Google **sensitive** scope (a production OAuth client needs
  Google's sensitive-scope verification for it). No Drive, Docs or
  Calendar scope.
- Smart notes need the meeting **organizer** on a Google Workspace edition
  or Google AI plan with "Take notes for me", turned on by the admin.
- Google's read quota: 600 requests a minute per user and 6000 per project;
  a 429 arrives as the gateway's `rate_limited`.
- Reading both tables costs one gateway call per list page and one per
  conference, which makes two Google requests (the record, then its smart
  notes).
- Provider refusals are not absorbed (`error_path` is unset): they arrive
  as the gateway's failure envelope and fail the scan with its code and the
  action named. A record that expired answers Google's 404, which the
  gateway reports as `invalid_input` with HTTP 404.

## Verification

What has been observed on 2026-11-24, without Google credentials, against
a local gateway built from Open Connector
`27e6b0fbff35ae06858651a2b2bf31251053d7a8` (the head of
open-connector#30 until it merges):

- Discovery of `googlemeet.list_conference_records` and
  `googlemeet.get_conference_artifacts`. Their output and input schemas
  are captured under
  `crates/skardi/src/sources/providers/open_connector/packs/fixtures/google_meet/contracts/`
  and pinned by fingerprint. `list_conference_records` hashes the same as
  on open-connector#22's own branch.
- Every input set the pack generates was accepted by the strict schemas
  and reached the credential wall (`authorization_failed`, `data.status`
  401). The sets are:
  - for `meetings`: `startTime`+`endTime`+`pageSize`, the same plus
    `pageToken`, and `pageSize` alone;
  - for `meeting_notes`: `name`.
- An undeclared key was refused as an input-schema mismatch.

What has **not** been observed — each item is a probe against a real
Google Workspace account:

1. Whether reading a meeting the user only attended answers or refuses.
2. That a smart note lacks `docsDestination` until its state is
   `FILE_GENERATED`, and how long after the meeting that takes.
3. That the organizer's token lists smart notes an internal non-organizer
   started.
4. Whether two note-taking sessions in one meeting are two Docs or one.
5. That a meeting without notes answers an empty `smartNotes`, not an
   error.
6. That a running conference has no `endTime` and the window lists it.
7. That an expired or deleted record answers 404, not 403.
8. The answer to a stale `pageToken`, and how long a token lives.
9. That the window accepts whole seconds with `Z` and is inclusive at both
   ends.
10. That an instant meeting (meet.new) is listed for whoever opened it.
11. That the Doc id is the Drive file id the Google Drive pack lists.
12. The 429 envelope at Google's quota.

The fixtures under
`crates/skardi/src/sources/providers/open_connector/packs/fixtures/google_meet/`
are **synthetic**: shaped by Google's discovery document, with invented
record names, spaces and Doc ids. Meetings and their notes are customer
content; the live pass must redact before it replaces them.
