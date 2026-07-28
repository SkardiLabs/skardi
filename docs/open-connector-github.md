# GitHub Source Pack

The built-in `github` source pack exposes GitHub repositories, issues, pull
requests, reviews, commits, workflow runs, and releases as stable SQL tables
through an [Open Connector gateway](open-connector.md). GitHub credentials
(API key / OAuth) live in Open Connector; Skardi holds only the gateway
runtime token.

For a runnable local walkthrough — stub gateway, server, all three SQL
interfaces, federated join — see the [demo](open-connector/README.md).

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
          - name: github_skardi          # schema name in SQL
            source_pack: github
            source_pack_version: 1       # optional pin
            connection_alias: work       # optional Open Connector alias
            resource:
              owner: SkardiLabs
              repo: skardi
            tables:
              - issues
              - pull_requests
              - commits
```

```sql
SELECT number, title, author_login
FROM saas.github_skardi.issues
WHERE state = 'open'
LIMIT 50;

-- The same definition, ad hoc, without a binding:
SELECT number, title
FROM open_connector_query('saas', 'github.issues',
                          '{"owner":"SkardiLabs","repo":"skardi"}')
WHERE state = 'open'
LIMIT 50;
```

Resource values keep their YAML types: `issueNumber: 42` reaches the
gateway as the JSON number 42, exactly as the UDTF's resource JSON
(`'{"owner":"acme","repo":"widgets","issueNumber":42}'`) sends it — so a
binding and an identical UDTF invocation also share one scan-cache entry.
Each table's requests carry only the resource keys its contract declares:
`repositories` (which takes none) can share a binding with the
repo-scoped tables without tripping the gateway's strict input schemas.

## Tables

All tables use page-number pagination at GitHub's 100-row maximum, sent as
the gateway's camelCase `page`/`perPage` inputs (Open Connector's action
schemas are strict — snake_case keys are rejected); a short or empty page
terminates the scan (GitHub's documented end-of-collection signal), so
every scan is complete and bounded by `max_pages` / `max_rows`.

| Table | Action | Resources | Filter pushdown |
|---|---|---|---|
| `repositories` | `github.list_my_repositories` | — (connected account) | none |
| `issues` | `github.list_repository_issues` | `owner`, `repo` | `state =` (inexact, re-applied locally); `updated_at >=` → `since` (inexact, re-applied locally) |
| `issue_comments` | `github.list_issue_comments` | `owner`, `repo`, `issueNumber` | none |
| `pull_requests` | `github.list_pull_requests` | `owner`, `repo` | `state =` (inexact, re-applied locally) |
| `reviews` | `github.list_pull_request_reviews` | `owner`, `repo`, `pullNumber` | none |
| `commits` | `github.list_commits` | `owner`, `repo` | none (see below) |
| `workflow_runs` | `github.list_workflow_runs` | `owner`, `repo` | none |
| `releases` | `github.list_releases` | `owner`, `repo` | none |

Every other SQL predicate is valid — DataFusion evaluates it locally after
the bounded fetch. `LIMIT` always stops pagination as soon as enough rows
have been emitted.

The default safety bounds put a hard ceiling on an unfiltered scan: with
`max_pages: 100` at 100 rows per page, any table reaches at most 10,000
rows before the scan **fails** with `ScanBoundsExceeded` — per the
fail-don't-truncate rule, a larger collection surfaces as an error, never
as a silently partial result. Raise `max_pages` (and `max_rows`, default
100,000) in the gateway's `open_connector:` block, or push a narrowing
predicate/`LIMIT`; the knobs are documented in
[the integration guide](open-connector.md#bounds-retries-and-errors).

Column references live in the pack definition
(`crates/skardi/src/sources/providers/open_connector/packs/github.yaml`,
the pack's embedded declarative definition);
highlights and caveats:

- **`issues` is pure issues.** GitHub's raw issues endpoint mixes pull
  requests in, but the Open Connector action filters them out before
  returning, so no `pull_request` marker column exists (it could never be
  non-NULL). Pull requests live in the `pull_requests` table.
- **`issues` and `pull_requests` read the complete collection.** GitHub
  lists only open items by default; the pack pins `state=all` on every
  request, and a pushed `state` predicate overrides the pin — so `SELECT *`
  and `WHERE state = 'closed'` are consistent.
- **`issues.updated_at >=` maps to GitHub's `since`** as an *inexact* push:
  GitHub documents issue `since` as "updated at or after" (a superset of
  the predicate), the fetch narrows, and DataFusion re-applies the
  predicate so a fuzzy provider can never leak wrong rows.
- **`commits` deliberately maps no time filter**: GitHub's commits
  `since`/`until` are documented as strictly after/before the date, which
  cannot guarantee a superset of a `>=` predicate — a dropped boundary
  commit would be unrecoverable.
- **Null-heavy fields are nullable and become SQL NULL**: `commit.author`
  and `issue.user` are JSON null when no GitHub account is linked
  (`author_login IS NULL`), `workflow_runs.conclusion` is NULL while a run
  is in progress, `releases.published_at` is NULL for drafts.
- `assignees` and `labels` flatten to `List<Utf8>` (`login` / `name`).

## Authorization and visibility

- Visibility is exactly the Open Connector connection's GitHub credential:
  private repositories appear only if the token can see them, and
  `repositories` lists the connected account's visible repositories.
- The pack executes only the read actions hard-coded above; bindings cannot
  swap actions or row paths, and no mutating GitHub action is reachable.

## Rate limits and freshness

GitHub's REST API allows 5,000 authenticated requests per hour (per the
Open Connector connection's credential); each scanned page is one request.
`429` responses retry with capped backoff honoring `Retry-After` inside the
scan deadline. Reads are live by default; a positive `cache_ttl_seconds`
serves repeated identical scans from the bounded in-memory cache. Multi-page
scans can observe upstream changes between pages, subject to GitHub's own
pagination guarantees.

## Compatibility

The pack is version 1; bindings may pin `source_pack_version: 1` so a
Skardi upgrade cannot silently change bound schemas. Contract fingerprints
are not yet pinned (the pack has not been validated against a live
gateway's discovered contracts); the bundled fixtures under
`packs/fixtures/github/` are the build-time conversion contract, and pins
land once a live catalog validates them.
