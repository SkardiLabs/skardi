# Slack Source Pack

The built-in `slack` source pack exposes Slack workspace metadata —
conversations (channels), users, and files — plus one conversation's
**message history** as stable SQL tables through an
[Open Connector gateway](open-connector.md). The Slack OAuth bot token lives
in Open Connector; Skardi holds only the gateway runtime token.

This pack validates **cursor pagination** (`cursor` in, `nextCursor` out),
complementing the GitHub pack's page-number validation.

**The wire contract is Open Connector's, not Slack's raw Web API**: the
gateway's Slack executors normalize rows (camelCase fields, flattened
profiles), move the next cursor to a top-level `nextCursor`, and consume
Slack's in-band `ok:false` errors themselves. Column names below reflect
that normalized contract, reconciled against a live gateway (v1.3.1).

## Binding

`conversations`, `users` and `files` need no resource input — they cover
whatever the bot's token can see. `messages` is the exception: it reads
**one** conversation, so it requires a `channelId` resource and therefore its
own binding.

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
          - name: acme_workspace       # schema name in SQL
            source_pack: slack
            connection_alias: work     # optional Open Connector alias
            tables:
              - conversations
              - users
              - files
          # `messages` needs a channelId, so it needs its own binding —
          # adding it to `acme_workspace` fails startup with
          # `missing required resource input`.
          - name: standup              # per-channel binding for history
            source_pack: slack
            resource:
              channelId: C0123456789   # `id` from the conversations table
            tables:
              - messages
```

```sql
SELECT name, member_count, topic
FROM saas.acme_workspace.conversations
WHERE NOT is_archived
ORDER BY member_count DESC;

-- One channel's history since a point in time. The lower bound reaches
-- Slack as `oldest`, so a scheduled run fetches the delta rather than the
-- channel's whole history.
SELECT ts, user_id, text
FROM saas.standup.messages
WHERE sent_at >= TIMESTAMP '2026-07-01T00:00:00Z'
ORDER BY sent_at;

-- The same definition, ad hoc, without a binding:
SELECT id, real_name, display_name
FROM open_connector_query('saas', 'slack.users', '{}')
WHERE NOT is_bot AND NOT deleted;
```

`messages` requires a `channelId` resource; `files` optionally takes one to
scope the listing to a single channel; `conversations` and `users` take none.

## Tables

| Table | Action | Resources | Pagination | Filter pushdown |
|---|---|---|---|---|
| `conversations` | `slack.list_conversations` | — | cursor (`limit` 200) | none |
| `messages` | `slack.get_channel_messages` | `channelId` (required) | cursor (`limit` 999) | `sent_at >=` → `oldest` (inexact, re-applied locally) |
| `users` | `slack.list_users` | — | cursor (`limit` 200) | none |
| `files` | `slack.list_files` | `channelId` (optional) | classic `page`/`count` (100), ends at `paging.pages` | `user_id =` → `userId` (inexact, re-applied locally) |

Every other SQL predicate is valid — DataFusion evaluates it locally after
the bounded fetch, and `LIMIT` stops pagination as soon as enough rows have
been emitted. Cursor scans terminate on both end-of-collection spellings
(`nextCursor: null`, or the key absent entirely), and a gateway that
repeats a cursor fails the scan as a detected pagination loop instead of
spinning. `files` scans trust the envelope's authoritative `paging.pages`
count, so a short non-final page (permission filtering can legally produce
one) never truncates the scan.

The default safety bounds put a hard ceiling on an unfiltered scan: with
`max_pages: 100`, `conversations` and `users` reach at most
200 × 100 = 20,000 rows, `messages` at most 999 × 100 = 99,900, and `files`
at most 100 × 100 = 10,000 rows before the scan **fails** with
`ScanBoundsExceeded` — per the fail-don't-truncate rule, a workspace
larger than the ceiling surfaces as an error, never as a silently partial
result. A busy channel therefore reaches the ceiling on an unfiltered
`messages` scan; a `sent_at >=` bound is the cheap way to stay under it.
Raise `max_pages` (and `max_rows`, default 100,000) in the
gateway's `open_connector:` block for larger workspaces, or push a
narrowing predicate/`LIMIT`; the knobs are documented in
[the integration guide](open-connector.md#bounds-retries-and-errors).

Column references live in the pack definition
(`crates/skardi/src/sources/providers/open_connector/packs/slack.yaml`,
the pack's embedded declarative definition);
highlights and caveats:

- **`conversations` pins `types: ["public_channel", "private_channel"]`**
  (the action schema takes an array) so the table reads as the complete
  collection the bot can see, not Slack's public-only default. IMs and
  MPIMs are deliberately excluded — they are message-shaped, not channels.
  The `type` column carries the gateway's classification per row.
- **`users` pins `includeLocale: true`** so the `locale` column is
  populated — Slack omits the field without the flag.
- **`messages` splits Slack's `ts` into two columns.** `ts` keeps the raw
  fractional-epoch-seconds string, which is the message's identity — unique
  within a conversation, and the handle `conversations.replies` and
  permalinks take. `sent_at` reads the *same* field as a
  `Timestamp(ms, UTC)`, because a pushdown needs a timestamp-typed column
  and Arrow's millisecond unit cannot hold Slack's microseconds. Two
  messages a microsecond apart therefore share one `sent_at`; join and
  de-duplicate on `ts`.
- **`sent_at >=` is pushed as `conversations.history`'s `oldest`**, and it
  is the table's whole incremental story: without it a scheduled run
  re-reads the channel's entire history through Slack's API. The bound is
  rendered as a digit **string** (the action types `oldest` as a Slack `ts`
  string under `additionalProperties: false`, where a JSON number is a 400,
  not a coercion), which floors it to a whole second. That makes the fetch
  *wider* than the predicate, so the mapping is `Inexact` and DataFusion
  re-applies the filter locally. The pack also pins `inclusive: true`,
  because Slack's `oldest` is exclusive by default and a message sitting
  exactly on the floored second would otherwise never arrive — and local
  re-filtering can drop rows, never recover them. No upper bound is pushed:
  `endTime`-style exclusivity plus flooring would drop rows.
- **`messages` requests `limit: 999`**, `conversations.history`'s
  documented ceiling, not the 100 default.
- **`messages` carries thread metadata but not thread replies.**
  `thread_ts`, `parent_user_id`, `reply_count`, `reply_users_count`,
  `latest_reply` and `is_locked` describe a thread parent; the replies
  themselves live behind `conversations.replies` and have no table yet
  (see below).
- **`files.created` is Slack epoch seconds**, converted to a
  `Timestamp(ms, UTC)` column. The normalized conversation and user rows
  carry no timestamps.
- **Slack uses empty strings, not nulls** (`topic = ''` for an unset
  topic); those stay empty strings. The gateway's explicit nulls and
  omitted keys both become SQL NULL.
- **Slack's in-band errors surface as gateway failures**: the executor
  consumes the HTTP-200 `ok: false` envelope and the gateway returns a
  failure envelope whose message carries Slack's own error code
  (`missing_scope`, `not_authed`, …) — the scan fails naming that code
  and the action, never a misleading missing-row-array error.
- **No time filter is pushed on `files`**: the gateway's `list_files`
  contract declares no `ts_from`-style input (its strict schema would
  reject one), so `created` predicates are evaluated by DataFusion after
  the bounded fetch.
- **`messages` needs a gateway build whose `get_channel_messages` returns
  a cursor.** Its fingerprint is pinned from the vendored Open Connector
  build `b2b33e57` (branch `skardi/slack-acl`), not from an upstream
  oomol-lab release: the upstream action dropped Slack's `next_cursor` on
  the floor, which the source-pack admission gate's complete-pagination
  requirement rejects. A gateway without that change fails registration
  with `ActionContractMismatch` on this table — the other three tables are
  unaffected and can be bound on their own.
- **Action-contract fingerprints are pinned** against a live gateway
  (v1.3.1, and the vendored build above for `messages`). Registration
  compares each pinned fingerprint with the
  discovered output schema and refuses a differing contract with
  `ActionContractMismatch` — schema drift fails at startup, never as
  silently reshaped rows mid-query. Upgrading Open Connector may change
  these schemas (even compatibly); the pack then needs its captured
  contracts re-taken and pins refreshed.

## Authorization and visibility

Everything is bounded by the **bot token's** membership and scopes,
configured in Open Connector:

- `conversations` needs `channels:read` (+ `groups:read` for private
  channels); private channels appear only where the bot is a member.
- `messages` reads `conversations.history`, whose scope follows the
  channel's type: `channels:history` for a public channel, `groups:history`
  for a private one (and `im:history` / `mpim:history` for the DM shapes
  `conversations` deliberately excludes). The bot must also be a member of
  the channel it reads. A missing scope surfaces as Slack's own
  `missing_scope` code on the failing scan, per the in-band error rule
  below — not as an empty result.
- `users` needs `users:read`. Deleted members stay listed with
  `deleted = true`. (Emails are not part of the gateway's normalized user
  contract, so there is no `email` column.)
- `files` needs `files:read` and lists files visible to the bot.

`messages` is real conversation content, so a binding that includes it is
no longer **Slack workspace metadata** in the design's marketing sense —
scope the `channelId` deliberately and treat the resulting tables as
governed content.

## No thread table (yet)

`messages` covers a conversation's top-level history. Thread *replies* are
a separate Slack endpoint (`conversations.replies`) and have no table in
this pack version. That is no longer an upstream blocker — the vendored
build gives `get_thread` the same cursor contract (`cursor` in,
`nextCursor` out, the same time window) that let `messages` clear the
admission gate's complete-pagination requirement — so a `threads` table is
a pack change rather than a gateway one. Until it lands, the action stays
reachable ad hoc through
[`open_connector_scan`](open-connector.md#3-open_connector_scan--allowlisted-raw-read-actions).

## Rate limits and freshness

Slack Web API methods are rate-limited per method tier; the gateway's
`429` + `Retry-After` responses are honored with bounded backoff inside the
scan deadline. Reads are live by default; the gateway-level TTL cache
(`cache_ttl_seconds`) applies to these tables exactly as documented in the
[general guide](open-connector.md#caching-and-freshness).
