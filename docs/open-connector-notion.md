# Notion Source Pack

The built-in `notion` source pack exposes Notion workspace data — users,
pages, data sources, and block children — as stable SQL tables through an
[Open Connector gateway](open-connector.md). The Notion integration token
lives in Open Connector; Skardi holds only the gateway runtime token.

**The wire contract is Open Connector's raw passthrough of the Notion
API**: rows are Notion objects verbatim under `$.results`, with Notion's
native cursor envelope beside them (`$.next_cursor`, null at
end-of-collection; `has_more` is redundant and unused). Inputs are the
gateway's camelCase strict schema (`startCursor`/`pageSize`/`blockId`).
Everything below is reconciled against a live gateway **and verified end
to end against a real Notion workspace** (2026-08-03). The gateway pins
`Notion-Version: 2026-03-11`; that version is what makes the
`data_source` filter spelling valid (pre-2025-09-03 versions spell it
`database`) and what renames the page flag to `is_archived`. If the
gateway's pinned Notion version ever changes, the action-contract
fingerprints refuse registration rather than drifting silently.

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
          - name: notion_ws              # schema name in SQL
            source_pack: notion
            resource:
              blockId: <page-or-block-uuid>   # only block_children uses it
            tables: [users, pages, data_sources, block_children]
```

```sql
SELECT id, name, type FROM saas.notion_ws.users WHERE type = 'person';

-- The same definition, ad hoc, without a binding:
SELECT id, url FROM open_connector_query('saas', 'notion.pages', '{}')
WHERE NOT in_trash LIMIT 20;
```

## Tables

| Table | Action | Resources | Pagination | Filter pushdown |
|---|---|---|---|---|
| `users` | `notion.list_users` | — | cursor (`pageSize` 100) | none |
| `pages` | `notion.search` (pinned `query: ""`, `filter: {object: page}`) | — | cursor (`pageSize` 100) | none |
| `data_sources` | `notion.search` (pinned `filter: {object: data_source}`) | — | cursor (`pageSize` 100) | none |
| `block_children` | `notion.list_block_children` | `blockId` (required) | cursor (`pageSize` 100) | none |
| `page_markdown` | `notion.retrieve_page_markdown` | `pageId` (required) | single row | none |

**No filter pushdown anywhere** — `notion.search`'s only narrowing input
is a free-text relevance `query`, which no SQL predicate maps to
faithfully; the other actions declare no filter inputs. Every predicate
runs in DataFusion after the bounded fetch; `LIMIT` stops pagination as
soon as enough rows have been emitted. Cursor scans terminate on the
null-cursor spelling; a non-advancing gateway fails as a pagination loop.

The default safety bounds cap an unfiltered scan at 100 × 100 = 10,000
rows before it **fails** with `ScanBoundsExceeded` (fail-don't-truncate);
raise `max_pages`/`max_rows` in the `open_connector:` block or narrow with
`LIMIT` — see [the integration guide](open-connector.md#bounds-retries-and-errors).

Column references live in the pack definition
(`crates/skardi/src/sources/providers/open_connector/packs/notion.yaml`);
highlights and caveats:

- **`pages`/`data_sources` are everything the integration can see, as
  search reports it**: the required search `query` is pinned to `""` and
  the object `filter` is pinned per table. Visibility is what has been
  shared with the integration in Notion — but a drained walk is **not an
  inventory**. Notion documents that search may omit an accessible page,
  lag after a share, and change while paging, so a page's absence from
  the listing is not evidence it is gone. Add on presence; prove removal
  with a direct read (`page_markdown` refusing is one).
- **Dynamic property maps stay opaque JSON** (`properties`, `parent`):
  typed projection of user-defined schemas is the deferred
  `query_data_source` work (binding-time schema freeze per the design).
  There is deliberately **no rows table yet**.
- **`block_children` returns block metadata only** — the type-specific
  payload lives under a key named by `type`, which a fixed mapping cannot
  address. **The page's text is `page_markdown`**: one row per page,
  rendered as Markdown by Open Connector's normalized
  `retrieve_page_markdown`, with `truncated` and `unknown_block_ids`
  saying how complete the render was and `last_edited_time` (read off the
  page object, since the markdown endpoint reports none) for windowing an
  ingest. Its output schema declares every field, so it is the one notion
  table whose columns are fully inside the fingerprint gate.
- **`users` excludes `person.email`** and the raw `person`/`bot` objects
  (capability-gated, privacy-sensitive).
- **Deletion flags follow the real wire, not the declared contract**: on
  Notion-Version 2026-03-11 pages carry `is_archived` + `in_trash`, while
  data sources and blocks carry `in_trash` only — there is no `archived`
  field on any live object, so the pack does not map one.
- **Action-contract fingerprints are pinned** from a live capture
  (`packs/fixtures/notion/contracts/`); `pages`/`data_sources` share
  `notion.search`'s pin. Note: the gateway declares the search item
  schema as an `anyOf` (page | data_source), which the coverage walker
  does not descend, so the two search tables' columns sit outside the
  fingerprint gate entirely (pinned as such by the coverage test) — their
  drift surfaces at scan time under the conversion rules.

## Authorization and visibility

Everything is bounded by the Open Connector Notion integration's access:
pages and data sources appear only where the integration has been shared;
`users` requires the integration's user-information capability.

## Rate limits and freshness

Notion's API averages ~3 requests/second per integration; each scanned
page is one request, retried on `429` with `Retry-After` honored inside
the scan deadline. Reads are live by default; the gateway-level TTL cache
applies as documented in the [general guide](open-connector.md#caching-and-freshness).
