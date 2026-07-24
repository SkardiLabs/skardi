# Slack Source Pack

The built-in `slack` source pack exposes Slack workspace metadata —
conversations (channels), users, and files — as stable SQL tables through an
[Open Connector gateway](open-connector.md). The Slack OAuth bot token lives
in Open Connector; Skardi holds only the gateway runtime token.

This pack validates **cursor pagination** (Slack's
`cursor` / `response_metadata.next_cursor` contract), complementing the
GitHub pack's page-number validation.

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
SELECT name, num_members, topic
FROM saas.acme_workspace.conversations
WHERE NOT is_archived
ORDER BY num_members DESC;

-- The same definition, ad hoc, without a binding:
SELECT id, real_name, email
FROM open_connector_query('saas', 'slack.users', '{}')
WHERE NOT is_bot AND NOT deleted;
```

## Tables

| Table | Action | Pagination | Filter pushdown |
|---|---|---|---|
| `conversations` | `slack.list_conversations` | cursor (`limit` 200) | none |
| `users` | `slack.list_users` | cursor (`limit` 200) | none |
| `files` | `slack.list_files` | classic `page`/`count` (100) | `user_id =` → `user` (inexact, re-applied locally) |

Every other SQL predicate is valid — DataFusion evaluates it locally after
the bounded fetch, and `LIMIT` stops pagination as soon as enough rows have
been emitted. Cursor scans terminate on both of Slack's end-of-collection
spellings (`next_cursor: ""` or no `response_metadata` at all), and a
gateway that repeats a cursor fails the scan as a detected pagination loop
instead of spinning.

Column references live in the pack definition
(`crates/skardi/src/sources/providers/open_connector/packs/slack.rs`);
highlights and caveats:

- **`conversations` pins `types=public_channel,private_channel`** so the
  table reads as the complete collection the bot can see, not Slack's
  public-only default. IMs and MPIMs are deliberately excluded — they are
  message-shaped, not channels.
- **Timestamps (`created`, `updated`) are Slack epoch seconds**, converted
  to `Timestamp(ms, UTC)` columns.
- **Slack uses empty strings, not nulls** (`topic = ''` for an unset
  topic); those stay empty strings. Genuinely absent keys become SQL NULL.
- **`files.created >= …` is not pushed down**: Slack's `ts_from` takes
  epoch seconds and the filter engine renders timestamp literals as
  RFC 3339 only. The predicate still works — locally.

## Authorization and visibility

Everything is bounded by the **bot token's** membership and scopes,
configured in Open Connector:

- `conversations` needs `channels:read` (+ `groups:read` for private
  channels); private channels appear only where the bot is a member.
- `users` needs `users:read`; the `email` column additionally requires
  `users:read.email` and is NULL without it. Deleted members stay listed
  with `deleted = true`.
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
