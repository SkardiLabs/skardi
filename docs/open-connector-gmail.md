# Gmail Source Pack

The built-in `gmail` source pack exposes a Gmail mailbox — threads,
messages, drafts, labels, and filters — as stable SQL tables through an
[Open Connector gateway](open-connector.md). The Google OAuth credential
lives in Open Connector; Skardi holds only the gateway runtime token.

**The wire contract is Open Connector's, not the Gmail API's**: the
gateway's gmail executors rebuild list rows the way the Slack ones do —
camelCase identity (`threadId`/`messageId`), the `From`/`To`/`Subject`
headers flattened into `sender`/`to`/`subject`, and Gmail's epoch-millis
`internalDate` re-emitted as an RFC 3339 `messageTimestamp`. The two
exceptions are `labels` and `filters`, whose rows are the provider
objects passed through raw. Inputs are the gateway's camelCase strict
schema (`pageToken`/`maxResults`/`labelIds`). Everything below is
reconciled against a live gateway (v1.3.4); end-to-end verification
against a real mailbox is the pack's phase-4 gate before general use.

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
          - name: mail                   # schema name in SQL
            source_pack: gmail
            resource:                    # all optional; omit for the default listing
              query: "newer_than:90d"        # Gmail search syntax; threads + messages
              labelIds: [INBOX]              # messages only
              includeSpamTrash: false        # messages only
            tables: [threads, messages, drafts, labels, filters]
```

```sql
SELECT sender, subject, message_timestamp
FROM saas.mail.messages
WHERE message_timestamp > now() - INTERVAL '7 days'
ORDER BY message_timestamp DESC;

-- The same definition, ad hoc, without a binding:
SELECT name, type FROM open_connector_query('saas', 'gmail.labels', '{}');
```

## Tables

| Table | Action | Resources (optional) | Pagination | Filter pushdown |
|---|---|---|---|---|
| `threads` | `gmail.list_threads` | `query` | cursor (`maxResults` 500) | none |
| `messages` | `gmail.fetch_emails` (pinned `detail: summary`) | `query`, `labelIds`, `includeSpamTrash` | cursor (`maxResults` 100) | none |
| `drafts` | `gmail.list_drafts` | — | cursor (`maxResults` 500) | none |
| `labels` | `gmail.list_labels` | — | single page | none |
| `filters` | `gmail.list_filters` | — | single page | none |

**No filter pushdown anywhere** — Gmail's `q` is a free-text search
language no SQL predicate maps to faithfully, and `labelIds` is an
AND-semantics array a scalar mapping cannot represent. Every predicate
runs in DataFusion after the bounded fetch; `LIMIT` stops pagination as
soon as enough rows have been emitted. Cursor scans terminate on the
executor's explicit `nextPageToken: null` (absent and empty-string
tokens also terminate); a non-advancing gateway fails as a pagination
loop. `labels` and `filters` take no pagination inputs at all — each is
one request returning the complete collection.

The default safety bounds cap an unfiltered scan at `max_pages` ×
page-size rows before it **fails** with `ScanBoundsExceeded`
(fail-don't-truncate); raise `max_pages`/`max_rows` in the
`open_connector:` block or narrow with `LIMIT` — see
[the integration guide](open-connector.md#bounds-retries-and-errors).

Column references live in the pack definition
(`crates/skardi/src/sources/providers/open_connector/packs/gmail.yaml`);
highlights and caveats:

- **The default listing is Gmail's own**: it excludes `SPAM` and `TRASH`.
  Set the `includeSpamTrash` resource to sweep those into `messages`;
  `threads` offers no such input on the gateway's schema, which is also
  why the pack does not pin `includeSpamTrash: true` — the two tables
  would otherwise describe different mailboxes.
- **`messages` pins `detail: summary`** — the bounded row shape (no
  bodies, no attachment trees). The gateway hydrates each listed message
  with a metadata `messages.get`, so a scanned page of 100 rows costs 101
  Gmail API calls; the reduced page size bounds that burst.
- **Header-derived fields spell "absent" as `''`**, never NULL
  (`subject`, `sender`, `to_addresses`) — filter with `<> ''`, not
  `IS NOT NULL`. `to_addresses` is the raw `To` header (display names
  and all); it is the wire's `to`, renamed because `TO` is a reserved
  SQL keyword.
- **`query` resources use Gmail search syntax** (`from:`, `label:`,
  `newer_than:`, …) and scope `threads`/`messages` at the source; the
  result is still the complete listing *of that query*, paginated to
  termination.
- **`filters.criteria` / `filters.action` are opaque JSON** — Gmail's
  own sparse matcher and mutation objects.
- **Excluded actions, and why**: `search_threads` (a strict subset of
  `list_threads`), `list_history` (an incremental-sync checkpoint API,
  not a collection), `list_forwarding_addresses` (no output schema to
  fingerprint; needs the `settings.sharing` scope), `get_profile` (a
  scalar endpoint). Message bodies (`detail: full`) are deferred to a
  future content-oriented surface.
- **Action-contract fingerprints are pinned** from a live capture
  (`packs/fixtures/gmail/contracts/`, gateway v1.3.4). The gateway
  declares `fetch_emails` row items as an `anyOf` (ids | summary |
  full) the coverage walker does not descend, so the `messages` columns
  sit outside the fingerprint gate (pinned as such by the coverage
  test) — their drift surfaces at scan time under the conversion rules.

## Authorization and scopes

Everything is bounded by the Google OAuth connection configured in Open
Connector. The gateway's scope metadata per action: `threads` and
`messages` read under `gmail.readonly`; `labels` under `gmail.labels`
(or `gmail.readonly`); `drafts` is declared under `gmail.compose`;
`filters` requires `gmail.settings.basic`. A connection missing a scope
fails the affected table's scan with the gateway's `authorization_failed`
envelope — the other tables are unaffected.

## Rate limits and freshness

Gmail's API enforces per-user quota units per second (list ≈ 5 units,
metadata get ≈ 5 units — a hydrated `messages` page is by far the most
expensive scan); `429` responses are retried with backoff inside the
scan deadline. Reads are live by default; the gateway-level TTL cache
applies as documented in the
[general guide](open-connector.md#caching-and-freshness).
