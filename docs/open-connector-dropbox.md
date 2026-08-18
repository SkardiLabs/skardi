# Dropbox Source Pack

The built-in `dropbox` source pack exposes a Dropbox account's own files,
folders and shared links as stable SQL tables through an
[Open Connector gateway](open-connector.md). Dropbox credentials are an
OAuth token obtained through the gateway's flow against a Dropbox app the
operator creates; Skardi holds only the gateway runtime token. Visibility
is exactly the authorizing account's.

**The wire contract is Open Connector's NORMALIZED shape.** Unlike the
GitHub/Notion/Feishu packs, which pass provider objects through, every
Dropbox list executor rebuilds each entry through `mapDropboxMetadata`
into a fixed camelCase shape — `tag`, `name`, `id`, `pathDisplay`,
`pathLower`, `clientModified`, `serverModified`, `rev`, `sizeBytes`,
`isDownloadable`, `contentHash`, `url`, `expiresAt`, `sharingInfo`,
`linkPermissions` — whose fifteen keys are all declared `required` under
`additionalProperties: false`. Two consequences worth knowing:

- Dropbox's own snake_case spellings (`path_display`, `client_modified`,
  `size`) never reach a row, so mapping them would have produced
  always-NULL columns.
- Every column below sits **inside** the contract-fingerprint gate, and
  the fingerprint coverage gap is empty — the opposite of the passthrough
  packs, where real rows are the only column truth.

> **⚠ NOT live-verified.** Reconciled against the Open Connector v1.3.5
> provider *source* only. No live gateway has answered any of this pack
> and no Dropbox account has been read, so every committed fingerprint is
> the hash of a source-derived schema rather than a captured one, and the
> bundled fixtures are authored shapes rather than redacted captures.
> **Registration against a real gateway is expected to fail the contract
> gate until the pins are re-captured** — that is the gate working. The
> runbook that closes this out is
> [the live-evaluation plan](superpowers/plans/2026-08-18-dropbox-live-evaluation.md);
> its acceptance table lists every statement here that is currently an
> inference.

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
          # files and shared_links need no resource at all.
          - name: me
            source_pack: dropbox
            tables: [files, shared_links]
          # file_search REQUIRES a query, so it needs its own binding
          # (one query per binding, the GitHub owner/repo precedent).
          - name: reports
            source_pack: dropbox
            resource:
              query: report
            tables: [file_search]
          # Optional resources narrow a listing: `path` selects the
          # listing ROOT for files, or one file/folder for shared_links.
          - name: invoices
            source_pack: dropbox
            resource:
              path: /Finance/Invoices
              directOnly: true          # shared_links only; a JSON boolean
            tables: [files, shared_links]
```

```sql
-- Every file and folder under the account root, recursively.
SELECT path_display, size_bytes, server_modified
FROM saas.me.files
WHERE tag = 'file'
ORDER BY server_modified DESC
LIMIT 20;

-- Shared links, newest first, with their visibility.
SELECT name, url, expires_at, link_permissions
FROM saas.me.shared_links;

-- The same definition, ad hoc, without a binding:
SELECT name, path_display
FROM open_connector_query('saas', 'dropbox.file_search',
                          '{"query":"invoice"}');
```

## Tables

| Table | Action(s) | Row path | Required resource | Optional resources | Columns |
|---|---|---|---|---|---|
| `files` | `dropbox.list_folder` → `dropbox.list_folder_continue` | `$.entries` | — | `path` | 12 |
| `shared_links` | `dropbox.list_shared_links` (continues on itself) | `$.links` | — | `path`, `directOnly` | 15 |
| `file_search` | `dropbox.search_files` → `dropbox.search_files_continue` | `$.matches` | `query` | `path` | 13 |

**No table pushes any filter.** Dropbox's remaining list inputs are
scan-shape controls (`recursive`, `includeDeleted`, `limit`,
`filenameOnly`), not column predicates. `path` is deliberately a resource
rather than an `eq` push onto `path_lower`: on `files` it selects the
listing *root*, which is a different claim from a path equality; on
`shared_links` the input accepts paths, file IDs **and** rev IDs, so no
fidelity level would be honest across its value domain. Guard tests pin
that no filter key ever reaches the wire.

`LIMIT` pushdown works on all three tables and stops pagination early —
including before the continuation request.

### `files` — every file and folder under `path`

Pinned inputs, so the table means the complete collection rather than one
directory level:

| Input | Pinned to | Why |
|---|---|---|
| `recursive` | `true` | A table named `files` that returns one directory level is a surprising contract |
| `includeMountedFolders` | `true` | Pins Dropbox's own default so it cannot drift |
| `includeDeleted` | `false` | Deleted tombstones carry a `deleted` tag and null everything else, informing no query |

Columns: `tag`, `name` (both non-nullable — the executor guarantees a
string), `id`, `path_display`, `path_lower`, `client_modified`,
`server_modified` (ISO 8601 on the wire, read as UTC timestamps), `rev`,
`size_bytes`, `is_downloadable`, `content_hash`, `sharing_info` (JSON
text).

`url`, `expires_at` and `link_permissions` exist in `mapDropboxMetadata`
but are **deliberately absent here**: the normalizer sources them from
fields `list_folder` never returns, so they would be structurally
always-NULL. They are mapped on `shared_links`, where they populate.

A folder row nulls the file-only columns rather than reporting zero
values — `size_bytes IS NULL` for a folder, `= 0` for an empty file.

### `shared_links` — links for the account, or for one path

Columns: everything `files` has, plus `url`, `expires_at` and
`link_permissions`. `url` is the natural identity but stays **nullable**:
the executor spells it `optionalString(record.url) ?? null`, so a
non-null declaration would fail scans on a row the gateway considers
legal.

`directOnly` is exposed as a binding resource rather than pinned because
neither setting is an honest default: pinned `true`, links a file
inherits from a shared ancestor disappear; pinned `false`, one file can
surface through several ancestors.

### `file_search` — Dropbox `search_v2` over files and folders

`query` is **required** — a search table without one is not a table.
`fileStatus` is pinned to `active`. `orderBy` is deliberately *not*
pinned: `search/continue_v2` pages a server-side snapshot taken at the
opening call, so relevance order is stable within one scan.

Columns: `match_type` (non-nullable, `filename` / `content`) plus the
twelve metadata columns read through the nested `metadata` block.

`highlight_spans` is unmapped and `includeHighlights` is never sent: the
field populates only when highlights are requested, and the declared
schema (nullable array) contradicts the executor (`readObjectArray`,
which returns `[]` and never null). Recorded as a wire-vs-contract
contradiction rather than mapped.

## Pagination: split-action continuation

**This pack is why `pagination.continuation` exists.** Dropbox continues
a listing through a *different action* than the one that opened it, and
each continue action declares `cursor` as its only property under
`additionalProperties: false`:

```
dropbox.list_folder   → dropbox.list_folder_continue     { cursor }
dropbox.search_files  → dropbox.search_files_continue    { cursor }
```

So page 1 goes to the opening action with resources, pinned inputs and
the page size; pages 2..N go to the continue action carrying the cursor
and nothing else. Feeding the cursor back to the opening action would be
a hard 400, not a quiet truncation. `shared_links` needs none of this —
it takes the cursor on its own action.

Both actions of a split-action table are discovered and
fingerprint-gated at registration, so an undiscovered or drifted continue
action fails at startup rather than on page two of the first scan. A
fingerprint hashes the **output** schema, though, so the `cursor_only`
claim — which is about *inputs* — is checked separately against the
continue action's discovered input schema: the cursor input must be a
declared property, and no other input may be `required`. A continue
action that publishes no input schema at all is refused rather than
trusted.

**Termination.** `list_folder` answers its final page with a **non-empty**
cursor, so cursor-spelling termination alone would refetch and fail as a
detected pagination loop. All three tables therefore declare
`has_more_path: "$.hasMore"` and treat it as authoritative — and because
it is declared, a page that omits it fails as contract drift rather than
guessing.

Page sizes are the schemas' declared maxima (`limit: 2000`,
`maxResults: 1000`) and are **unprobed at the boundary**; a declared cap
can exceed the wire's. Continuation pages carry no page-size input at
all — Dropbox sizes them from the request that opened the listing.
`shared_links` has no page-size input, so its `page_size` is an inert
placeholder.

## Authorization

The gateway's dropbox provider uses OAuth against a Dropbox app the
operator creates at https://www.dropbox.com/developers/apps (*Scoped
access*, *Full Dropbox* — App-folder access hides everything outside the
app folder and makes `files` and `file_search` untestable).

Two scopes cover the whole pack:

| Scope | Needed by |
|---|---|
| `files.metadata.read` | `files`, `file_search` |
| `sharing.read` | `shared_links` |

No content scope and no write scope is required by any shipped table, so
a read-only connection serves all three. Every permission change
invalidates the grant snapshot — re-run the authorization rather than
debugging a stale token.

Self-check that the gateway build carries all five actions before
debugging anything else (a too-old gateway fails registration with
`action 'dropbox.list_folder_continue' was not found`, which reads like a
typo and means "upgrade the gateway"):

```bash
for a in dropbox.list_folder dropbox.list_folder_continue \
         dropbox.list_shared_links dropbox.search_files dropbox.search_files_continue; do
  printf '%-34s %s\n' "$a" \
    "$(curl -s -o /dev/null -w '%{http_code}' \
       -H "Authorization: Bearer $OPEN_CONNECTOR_TOKEN" "$GATEWAY/v1/actions/$a")"
done   # expect 200 five times
```

## Errors, rate limits and freshness

`dropboxRpcRequest` throws on any non-2xx, so Dropbox's in-band
`error_summary` envelope **and** its 429 rate limiting both surface as
gateway *failure* envelopes rather than as HTTP 200 rows. No table
declares an `error_path`, and the provider's own code arrives through the
gateway-failure path.

The client's bounded retry/backoff handles transient 429/5xx envelopes.
Scans fetch pages on demand and stop early under `LIMIT`; completed scans
are cached per the scan cache's usual keying (binding, table, pushed
inputs, projection, LIMIT).

Dropbox cursors can expire or be invalidated mid-listing, and a long
recursive scan of a large account may outlive one. Behavior at that
boundary is an open question the live pass records.

## Tables deliberately not shipped

| Action | Why |
|---|---|
| `list_revisions` | Pages by feeding a `beforeRev` from the previous page's rows and answers `hasMore` with no cursor to follow — no pack-side strategy can complete it |
| `get_current_account` | Returns a single object; a row path requires an array |
| `get_tags` | Takes an array of paths, and resources are scalars; declares no pagination |
| `download_file`, `get_temporary_link`, `get_shared_link_file` | Base64 content payloads, not rows |
| `upload_file`, `create_folder`, `move`, `copy`, `delete`, `create_shared_link`, `modify_shared_link`, `revoke_shared_link`, `save_url`, `restore` | Writes — outside the read-only allowlist |
