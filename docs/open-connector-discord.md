# Discord source pack

The current OAuth user's guilds, external-account connections, and the
public Nitro sticker-pack catalog, as SQL tables over Open Connector's
`discord` provider.

> **Status: live-verified 2026-08-07** against a real Discord account
> through skardi-server: registration through LIVE discovery passed the
> fingerprint gate (for `sticker_packs` that pin is honest bookkeeping,
> not a gate — see Design notes); `guilds` (6 rows) and `sticker_packs` (14 rows)
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

A raw one-liner through
[`open_connector_scan`](open-connector.md#3-open_connector_scan--allowlisted-raw-read-actions)
(requires the action in the gateway entry's `raw_action_allowlist`):

```sql
SELECT id, name
FROM open_connector_scan('saas', 'discord.list_my_guilds',
                         '{"with_counts": false}', '$.guilds')
LIMIT 10;
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
- **`sticker_packs`' fingerprint is pinnable but unfalsifiable**: the
  captured contract is a bare
  `{type: object, properties: {}, additionalProperties: true}` — a
  schema every output satisfies, so the pin can only catch the gateway
  DECLARING a different schema, never the rows changing shape under the
  same declaration. "Passed the fingerprint gate" therefore means less
  for this table than for `guilds`/`connections`; the row shape rests
  on the live pass and the conversion tests, and the gate's refusal arm
  is exercised via `guilds` (`drifted_contract_fails_registration_not_
  the_scan`).
- **`permissions` maps the wire key `permissions_new`** (live-pass
  correction). The gateway calls the *unversioned* `discord.com/api`,
  which Discord serves as its legacy default version: there
  `permissions` is a truncated **number** and `permissions_new` is the
  full bitfield as a decimal string (it exceeds JSON's safe-integer
  range). The column keeps the natural name and maps the authoritative
  string; the legacy number is deliberately unmapped; decoding bits is
  query-side work. **Version-coupled risk**: if the gateway ever pins
  `/api/v10` — where `permissions` *is* the string and
  `permissions_new` does not exist — this mapping breaks, and the loose
  item schemas mean no fingerprint pin can catch the move. The column
  is therefore declared **non-nullable** (the legacy API attaches
  `permissions_new` to every guild object; 6/6 live rows carried it):
  the drift surfaces as a hard `ConversionFailed: missing key` with
  full column/page/row identity instead of a silently always-NULL
  column. **The blast radius is chosen, not accidental**: the converter
  fails the PAGE, so on that drift every `me.guilds` query goes down —
  including ones that never touch `permissions` — rather than one
  column degrading to NULL. A table-wide loud outage was picked over a
  quiet per-column hole because the alternative fails only when someone
  finally reads `permissions` (possibly weeks after the move), while
  the outage points at the cause on the FIRST query after it. The
  tripwire is enforced by
  `a_missing_permissions_new_key_fails_naming_the_permissions_column`.
  Upstream issue pending (the gateway should pin an API version); this
  doc links it once filed.
- **`entitlements` is deferred, not shipped incomplete**: Discord's
  entitlements API paginates (`before`/`after`/`limit`), but the
  gateway's executor exposes only `exclude_ended`/`exclude_deleted` —
  first-page-only through no fault of a pack. Filed upstream as
  [oomol-lab/open-connector#283](https://github.com/oomol-lab/open-connector/issues/283);
  the table joins when the executor grows the pagination inputs.
- No table declares `error_path`: the provider's executors consume
  Discord's error responses themselves and return the gateway's
  failure envelope.

## Rate limits and freshness

- **429s are retried, then loud.** Rapid successive calls to
  `/users/@me/guilds` rate-limit quickly, and the relay shape is
  verified end to end in the gateway's own code: the Discord executor
  maps a provider 429 to its `rate_limited` error code, and the
  gateway's runtime API returns that as **HTTP 429** (pinned by its
  `runtime-api` test). skardi's client retries HTTP 429 with bounded
  backoff on every call class — for POST execute it is the one
  retryable status (`RetryPolicy::NonIdempotent`) — so the back-to-back
  requests keyset issues (including the empty terminator) ride the
  retry budget rather than failing the scan. Only retry exhaustion
  surfaces as a scan failure, and loudly.
- **Request cost**: a full `guilds` scan of *n* rows makes
  `ceil(n / 200) + 1` requests — the `+1` is keyset's terminating empty
  page. That terminator **counts against the same `max_pages` budget**
  (the bound is checked before each fetch), so a keyset table's
  practical capacity is `max_pages − 1` full pages; a collection of
  exactly `max_pages × 200` rows fails loudly with
  `ScanBoundsExceeded` on the terminator rather than completing —
  pinned by `max_pages_budget_includes_the_keyset_terminator`. Raise
  `max_pages` if you genuinely have that many guilds.
- **Freshness**: scans are live by default. Completed scans are cached
  under the scan cache's usual key — binding, table, pushed inputs,
  **and LIMIT** (a `LIMIT 5` result is complete *for that key* and is
  never reused to answer an unlimited query). `connections` and
  `sticker_packs` are single-request tables; `guilds` fetches pages on
  demand and a satisfied LIMIT stops after page 1 without the
  terminator (`limit_stops_keyset_pagination_after_one_request`).

## Auth

The provider is OAuth2 (`authTypes: ["oauth2"]`) against a Discord
application. Scopes by table: `guilds` needs the `guilds` scope
(gateway permission `discord.guilds.read`), `connections` needs
`connections` (`discord.connections.read`); `sticker_packs` hits a
public endpoint (the gateway still requires a connected credential to
execute). `identify` is the baseline every OAuth app carries.
