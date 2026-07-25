# Slack Source Pack

The built-in `slack` source pack exposes Slack workspace metadata —
conversations (channels), users, and files — as stable SQL tables through an
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

No resource inputs are required — the tables cover whatever the bot's token
can see:

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
```

```sql
SELECT name, member_count, topic
FROM saas.acme_workspace.conversations
WHERE NOT is_archived
ORDER BY member_count DESC;

-- The same definition, ad hoc, without a binding:
SELECT id, real_name, display_name
FROM open_connector_query('saas', 'slack.users', '{}')
WHERE NOT is_bot AND NOT deleted;
```

`files` optionally takes a `channelId` resource to scope the listing to
one channel; the other tables take none.

## Tables

| Table | Action | Resources | Pagination | Filter pushdown |
|---|---|---|---|---|
| `conversations` | `slack.list_conversations` | — | cursor (`limit` 200) | none |
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

Column references live in the pack definition
(`crates/skardi/src/sources/providers/open_connector/packs/slack.rs`);
highlights and caveats:

- **`conversations` pins `types: ["public_channel", "private_channel"]`**
  (the action schema takes an array) so the table reads as the complete
  collection the bot can see, not Slack's public-only default. IMs and
  MPIMs are deliberately excluded — they are message-shaped, not channels.
  The `type` column carries the gateway's classification per row.
- **`users` pins `includeLocale: true`** so the `locale` column is
  populated — Slack omits the field without the flag.
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

## Authorization and visibility

Everything is bounded by the **bot token's** membership and scopes,
configured in Open Connector:

- `conversations` needs `channels:read` (+ `groups:read` for private
  channels); private channels appear only where the bot is a member.
- `users` needs `users:read`. Deleted members stay listed with
  `deleted = true`. (Emails are not part of the gateway's normalized user
  contract, so there is no `email` column.)
- `files` needs `files:read` and lists files visible to the bot.

Advertised per the design's marketing rule as **Slack workspace metadata**
— not full Slack access.

## No message or thread tables (yet)

Per the integration design's Slack caveat, complete message-history cursor
handling is not yet available through Open Connector, and an incomplete
message table would violate the source-pack admission gate's
complete-pagination requirement. Message and thread tables land in a later
pack version once upstream support exists; until then, allowlisted read
actions remain reachable ad hoc through
[`open_connector_scan`](open-connector.md#3-open_connector_scan--allowlisted-raw-read-actions).

## Rate limits and freshness

Slack Web API methods are rate-limited per method tier; the gateway's
`429` + `Retry-After` responses are honored with bounded backoff inside the
scan deadline. Reads are live by default; the gateway-level TTL cache
(`cache_ttl_seconds`) applies to these tables exactly as documented in the
[general guide](open-connector.md#caching-and-freshness).
