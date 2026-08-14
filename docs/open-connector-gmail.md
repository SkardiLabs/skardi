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
reconciled against a live gateway (v1.3.4) **and verified end to end
against a real mailbox** (2026-08-05): live registration through the
fingerprint gate, every mapped column extracting real non-NULL values,
real multi-page cursor pagination and final-page termination, and the
binding resources observed narrowing real listings.

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
              query: "newer_than:90d"        # Gmail search syntax; threads + messages (drafts: not narrowed)
              labelIds: [INBOX]              # messages only
              includeSpamTrash: false        # messages only
            tables: [threads, messages, drafts, labels]
            # `filters` is bindable too, but omitted here: it fails on a
            # mailbox with zero filters (see the caveat below).
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
- **Resources narrow only the tables that declare them** (the Resources
  column above). A partially-applicable key passes registration and is
  silently withheld from every non-declaring table's requests, so the
  tables of one binding can describe *different mailbox slices*. The
  example binding above does: `query: "newer_than:90d"` scopes `threads`
  and `messages`, while `drafts` — whose gateway action accepts no
  narrowing inputs at all — lists the mailbox's drafts in full. A join
  across them is a join across two different slices.
  The silent withholding stops at *no* declaring table: a key no bound
  table accepts fails registration outright, so binding `labelIds` while
  `tables:` omits `messages` is an error, not a no-op — narrow the
  binding's tables and its resources together.
- **`messages` pins `detail: summary`** — the bounded row shape (no
  bodies, no attachment trees). The gateway hydrates each listed message
  with a metadata `messages.get`, so a scanned page of 100 rows costs 101
  Gmail API calls; the reduced page size bounds that burst. It also
  lowers the effective scan ceiling: at the default `max_pages: 100`,
  `messages` hits `ScanBoundsExceeded` at 10,000 rows where
  `threads`/`drafts` reach 50,000 — and on this table `max_pages` is the
  knob that multiplies the hydration burst, so narrow with `query` or
  `LIMIT` before reaching for it.
- **`threads.snippet` is body content**: Gmail's own excerpt of the
  latest message's text (roughly its first hundred characters), returned
  under `gmail.readonly`. The "no bodies" shape above is `messages`'
  posture, not the pack's — size content/PII exposure with this column
  included.
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
- **A mailbox with zero filters currently fails the `filters` scan**
  with the gateway's `internal_error`: Gmail answers `settings/filters`
  with an empty body when no filters exist, which the upstream
  executor's JSON parsing does not tolerate (verified live; one existing
  filter makes the scan succeed). Fix pending upstream in
  open-connector; bind `filters` only for mailboxes that have at least
  one filter until then.
- **Excluded actions, and why**: `search_threads` (a strict subset of
  `list_threads`), `list_history` (an incremental-sync checkpoint API,
  not a collection), `list_forwarding_addresses` (no output schema to
  fingerprint; needs the `settings.sharing` scope), `get_profile` (a
  scalar endpoint). Full message bodies (`detail: full`) are deferred to
  a future content-oriented surface.
- **Action-contract fingerprints are pinned** from a live capture
  (`packs/fixtures/gmail/contracts/`, gateway v1.3.4). The fingerprint
  hashes the whole declared schema — `anyOf` branches included — so a
  renamed or retyped field in `fetch_emails`' row items fails
  registration before any scan runs. It compares *declarations*, so what
  it cannot see is a key upstream stops emitting without changing the
  schema; the `messages` columns are non-null so that drift fails the
  scan rather than yielding an all-NULL column.

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
scan deadline. That covers Gmail-origin rate limits too: the gateway
classifies a provider 429 as `rate_limited` and re-emits it as its own
HTTP 429 (gateway source, verified at v1.3.4), which is the one status
the execute retry policy retries. Reads are live by default; the
gateway-level TTL cache applies as documented in the
[general guide](open-connector.md#caching-and-freshness).
