# Discord source pack

The current OAuth user's guilds, external-account connections, and the
public Nitro sticker-pack catalog, as SQL tables over Open Connector's
`discord` provider.

> **Status: DRAFT pending live verification.** The wire contract below
> is reconciled against a live gateway's registered schemas and executor
> source (2026-08-07), and inputs were validated without credentials via
> the 403-vs-400 probe — but no table has scanned real workspace rows
> yet. Column sets follow Discord's documented resources and may still
> move when the live pass runs (loose item schemas leave real rows as
> the only column truth).

## What this provider can see

Open Connector's `discord` provider is the OAuth **user-identity**
surface — its own `get_user` executor rejects any id but `@me`. Every
table is the authorizing user's view. Channels, messages, and guild
members are Discord **bot-token** surface the provider does not carry:
those tables cannot exist in this pack, by provider scope, not by
deferral.

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
          # No table needs a resource — everything is @me or public.
          - name: me                # schema name in SQL
            source_pack: discord
            tables: [guilds, connections, sticker_packs]
```

```sql
SELECT name, approximate_member_count
FROM saas.me.guilds
ORDER BY approximate_member_count DESC;

SELECT connection_type, name, verified FROM saas.me.connections;
```

## Tables

| Table | Action | Resources | Pagination | Filter pushdown |
|---|---|---|---|---|
| `guilds` | `discord.list_my_guilds` | — | keyset (`after` = last row id), 200/page | — |
| `connections` | `discord.list_my_connections` | — | single page (API has none) | — |
| `sticker_packs` | `discord.list_sticker_packs` | — | single page (public catalog) | — |

Design notes:

- **`guilds` paginates by keyset** — Discord emits no pagination
  envelope; the next request's `after` is the previous page's last
  guild id, and a short or empty page ends the scan. A single page at
  the 200 cap would coincidentally also cover today's account limit
  (200 joined guilds with Nitro), but that equality is a coincidence,
  not a contract — keyset stays complete if either cap moves.
- **`with_counts: true` is pinned** so `approximate_member_count` /
  `approximate_presence_count` exist on every row: a column whose
  presence depends on request shape would be a per-scan schema coin
  flip.
- **`connection_type`, not `type`**: the wire key collides with a SQL
  keyword and would force quoting into every query. The wire key is
  unchanged (`path: type`).
- **`permissions` stays a decimal string** — Discord serializes the
  permission bitfield as a string because it exceeds JSON's
  safe-integer range; decoding bits is query-side work.
- **`entitlements` is deferred, not shipped incomplete**: Discord's
  entitlements API paginates (`before`/`after`/`limit`), but the
  gateway's executor exposes only `exclude_ended`/`exclude_deleted` —
  first-page-only through no fault of a pack. Tracked upstream; the
  table joins when the executor grows the pagination inputs.
- No table declares `error_path`: the provider's executors consume
  Discord's error responses themselves and return the gateway's
  failure envelope.

## Auth

The provider is OAuth2 (`authTypes: ["oauth2"]`) against a Discord
application. Scopes by table: `guilds` needs the `guilds` scope
(gateway permission `discord.guilds.read`), `connections` needs
`connections` (`discord.connections.read`); `sticker_packs` hits a
public endpoint (the gateway still requires a connected credential to
execute). `identify` is the baseline every OAuth app carries.
