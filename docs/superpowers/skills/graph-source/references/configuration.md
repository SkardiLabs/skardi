# Declaring the source: the full contract

## The context YAML

```yaml
kind: context
spec:
  data_sources:
    - name: kg                        # the catalog name: views live at kg.main.<view>
      type: graph
      hierarchy_level: catalog        # REQUIRED — anything else is refused at registration
      connection_string: postgres://localhost:5432/graphrag   # no credentials here (enforced)
      graph:
        backend: age                  # the shipped backend; neo4j/kuzu are later milestones
        graph_name: knowledge         # AGE graphs are named per database
        username_env: KG_READER_USER  # env-var NAMES, never values
        password_env: KG_READER_PASS
        query_timeout_seconds: 30     # default 30; valid 1..=86400
        max_rows: 10000               # default 10000; valid 1..=1000000
        max_connections: 4            # default 4; valid 1..=64
        views:
          - name: people
            cypher: |
              MATCH (p:Person)
              WHERE p.active
              RETURN p.name AS name, p.age AS age
              LIMIT 5000
            schema:
              - name: name
                type: string
              - name: age
                type: int
```

Notes that prevent re-derivation:

- `hierarchy_level: catalog` is required — the error if you omit it says
  so, but say it here too: views are catalog tables, so the source must
  register in catalog mode.
- The view above carries `WHERE` and `LIMIT` **inside its Cypher**. That
  is not decoration — see "A view's bound lives in its Cypher" below.
- Column `type` vocabulary (same for views and the ad-hoc `columns`
  argument, lowercase): `string|str|utf8`, `int|integer|bigint`,
  `float|double`, `bool|boolean`, `json`, `node`, `relationship`,
  `path`.
- **View names must be lowercase identifiers** (`[a-z_][a-z0-9_]*`) —
  enforced at config load, with a reason worth knowing: the name becomes
  the catalog table `<source>.main.<view>`, and DataFusion folds
  unquoted SQL identifiers to lowercase, so a view named `userPosts`
  would register fine, show up in `/data_source`, and be unreachable
  from any unquoted `SELECT`. The validator rejects the spelling rather
  than silently lowercasing it.
- Every column defaults to `nullable: true`. Declaring
  `nullable: false` is an author's assertion: a null arriving in such a
  column is a **typed error naming the column and row**, not silent
  corruption. Assert it only where the graph genuinely guarantees it.

## A view's bound lives in its Cypher

SQL-side predicates over a view do NOT push down into its Cypher.
`SELECT * FROM kg.main.people WHERE name = 'ada'` fetches the view's
rows (up to `max_rows`) and filters locally. A SQL `LIMIT` does push to
the consumption side — the fetch stops early — but a bare `WHERE` over
a view larger than `max_rows` fails with a typed `RowCapExceeded`,
**even when the query wants one row**.

So: write selective reads INTO the view's Cypher (`WHERE` / `LIMIT`
there), or use ad-hoc `cypher_query` where the predicate is the
caller's to place. Filter→Cypher parameterization is a named possible
future improvement; today the bound lives in the view. Raising
`max_rows` to make the error go away is almost always the wrong fix —
it converts a typed refusal into an unbounded fetch.

## Bounds, and what each one actually does

- `query_timeout_seconds` becomes the server-side `statement_timeout`
  PLUS a client-side wrap. A query past it fails with a typed timeout.
- `max_rows` caps **consumed** rows with a typed `RowCapExceeded`, and
  the fetch is a real SQL LIMIT of `min(limit, max_rows + 1)` — the
  wire is bounded too, not just the buffer.
- `max_connections` sizes the pool; pool queueing is bounded by the
  same timeout.

## Registration semantics: healthy, degraded, refused

Availability and contract violations part ways deliberately. The graph
backend is a shared external database whose transient blip must not
hold every unrelated source hostage at startup — this diverges from
Open Connector's hard-fail health check, on purpose.

**Healthy** — backend reachable, every view validated with one live
call (the Cypher runs fetching at most one row; the result must convert
against the declared schema). Be honest about what that proves:
**arity always** (the backend raises it regardless of row order);
**type and `nullable: false` only for the sampled row** — Cypher
without `ORDER BY` guarantees nothing about which row comes back, so a
view over heterogeneous data can register healthy and still fail a
later scan on a different row. Validation is a bounded preflight, not a
proof of the contract over the whole graph; per-row enforcement lives
in execution.

**Refused (server does not start)** — the backend ANSWERED and
something is wrong that retrying cannot fix: a view whose RETURN arity
or types disagree with its declared schema (the error names the view
and carries the backend's complaint), wrong credentials, AGE not
installed, the graph missing, or a view whose validation hits the
statement timeout (the server accepted the query; a too-slow view is a
boot-time diagnosis, not an outage). Fix the declaration or the
deployment and restart.

**Degraded** — only genuine connectivity failures qualify: DNS, a
refused dial, a network timeout — no server answered. (A pool acquire
timeout counts: sqlx retries a refused dial until the acquire deadline,
so an unreachable backend surfaces as `PoolTimedOut`.) Views still
register with their declared (planning-sufficient) schemas;
`GET /data_source` reports `status: "degraded"` plus `status_reason`
(the registration error) and `status_changed_at`; the first scan
retries. The registration preflight is bounded by
`min(query_timeout_seconds, 30s)` — the traversal timeout may be hours,
but boot never blocks longer than 30 s on a dead host.

**Recovery answers exactly one question: did the backend come back?** A
retry that gets ANY response flips the source healthy — including a
response in which a view fails its contract. The still-broken view then
fails its OWN scans with the typed error execution already produces,
while every other view works. Recovery deliberately does not hold
registration's all-or-nothing line: keeping the whole source degraded
over one un-provable view would disable every view permanently with no
startup signal, and `/data_source`'s "degraded" would misread as
"backend unreachable". Availability failures arm a backoff of
`query_timeout_seconds` clamped to `[30s, 300s]`; inside the window
scans fail fast with the cached diagnosis instead of re-paying the full
N-view re-validation. An ad-hoc `cypher_query` / `graph_schema` call on
a degraded source IS the retry.
