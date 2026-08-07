# Discord source pack

The current OAuth user's guilds, external-account connections, and the
public Nitro sticker-pack catalog, as SQL tables over Open Connector's
`discord` provider.

> **Status: live-verified 2026-08-07** against a real Discord account
> through skardi-server: registration through LIVE discovery passed the
> fingerprint gate; `guilds` (6 rows) and `sticker_packs` (14 rows)
> scanned end to end with every mapped column non-NULL on real rows; the
> real keyset walk (`limit: 2`) covered 3 full pages plus the empty
> terminator with no duplicate and no boundary drop. `connections`
> scanned live with a real linked account (1 row): all nine wire keys
> mapped, `revoked` genuinely absent on a non-revoked row (its non-NULL
> arm rides a synthetic fixture row — capturing it live would mean
> revoking a real account link). The live pass caught one contract
> defect no mock could: `permissions` is a NUMBER on the real wire (see
> below).

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
            source_pack_version: 1  # optional pin
            tables: [guilds, connections, sticker_packs]
```

```sql
SELECT name, approximate_member_count
FROM saas.me.guilds
ORDER BY approximate_member_count DESC;

SELECT connection_type, name, verified FROM saas.me.connections;

-- The same contract, ad hoc, without a binding:
SELECT name, sku_id
FROM open_connector_query('saas', 'discord.sticker_packs', '{}');
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
  guild id, and only an *empty* page ends the scan (short pages
  continue, so a silently clamped page size cannot read as completion;
  the cost is one extra empty request per scan). A single page at
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
- **`permissions` maps the wire key `permissions_new`** (live-pass
  correction). The gateway calls the *unversioned* `discord.com/api`,
  which Discord serves as its legacy default version: there
  `permissions` is a truncated **number** and `permissions_new` is the
  full bitfield as a decimal string (it exceeds JSON's safe-integer
  range). The column keeps the natural name and maps the authoritative
  string; the legacy number is deliberately unmapped; decoding bits is
  query-side work. **Version-coupled risk**: if the gateway ever pins
  `/api/v10` — where `permissions` *is* the string and
  `permissions_new` does not exist — this column goes always-NULL, and
  the loose item schemas mean no fingerprint pin can catch the move.
  Upstream issue pending (the gateway should pin an API version); this
  doc links it once filed.
- **Rate limits are tight**: rapid successive calls to
  `/users/@me/guilds` return HTTP 429, which the gateway surfaces as a
  loud scan failure (not a silent stop). A full scan of *n* guilds
  makes `ceil(n / 200) + 1` requests — the `+1` is the terminating
  empty page keyset requires — so a typical account costs two requests
  and stays comfortably clear.
- **`entitlements` is deferred, not shipped incomplete**: Discord's
  entitlements API paginates (`before`/`after`/`limit`), but the
  gateway's executor exposes only `exclude_ended`/`exclude_deleted` —
  first-page-only through no fault of a pack. Filed upstream as
  [oomol-lab/open-connector#283](https://github.com/oomol-lab/open-connector/issues/283);
  the table joins when the executor grows the pagination inputs.
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
