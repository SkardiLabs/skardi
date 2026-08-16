# Graph sources (Cypher over AGE)

Read-only Cypher against a property graph, surfaced as SQL tables
(design: `docs/superpowers/specs/2026-08-08-graph-engine-bypass-design.md`).
Skardi does not parse or store graphs — the graph engine owns storage
and traversal; Skardi forwards read-only Cypher and maps results into
Arrow rows with a planning-time-stable schema.

**Milestone status: M4.** `type: graph` data sources register from
context YAML, `views:` become catalog tables (`kg.main.people`), the
`cypher_query` / `graph_schema` UDTFs are available in every server
session, and pipeline parameters can reach Cypher parameters. Backend:
Apache AGE (openCypher inside Postgres). Neo4j and Kuzu are later
milestones.

## Server configuration

```yaml
kind: context
spec:
  data_sources:
    - name: kg
      type: graph
      hierarchy_level: catalog        # required: views live at kg.main.<view>
      connection_string: postgres://localhost:5432/graphrag
      graph:
        backend: age
        graph_name: knowledge         # AGE graphs are named per database
        username_env: KG_READER_USER  # env-var NAMES, never values
        password_env: KG_READER_PASS
        query_timeout_seconds: 30     # server-side statement_timeout + client wrap
        max_rows: 10000               # typed overflow error, never silent truncation
        views:
          - name: people
            cypher: |
              MATCH (p:Person) RETURN p.name AS name, p.age AS age
            schema:
              - name: name
                type: string
              - name: age
                type: int
```

Then:

```sql
SELECT * FROM kg.main.people ORDER BY name;
```

View columns use the same lowercase type vocabulary as the ad-hoc
`columns` argument: `string|str|utf8`, `int|integer|bigint`,
`float|double`, `bool|boolean`, `json`, `node`, `relationship`, `path`.
Every column defaults to `nullable: true`; a view may declare
`nullable: false` as an author's assertion — a null arriving in such a
column is a typed error naming the column and row, not a silent
corruption.

## A view's Cypher must carry its own bound

SQL-side predicates over a view do NOT push down into its Cypher:
`SELECT * FROM kg.main.people WHERE name = 'ada'` fetches the view's
rows (up to `max_rows`) and filters locally. A SQL `LIMIT` does push to
the consumption side (the fetch stops early), but a bare `WHERE` over a
view larger than `max_rows` fails with a typed `RowCapExceeded` — even
when the query wants one row. So write selective reads INTO the view's
Cypher (`WHERE` / `LIMIT` there), or use the ad-hoc `cypher_query`
where the predicate is yours to place. Filter→Cypher parameterization
is a possible future improvement; today the bound lives in the view.

## Registration semantics: healthy, degraded, refused

Availability and contract violations part ways deliberately (this
diverges from Open Connector's hard-fail health check — the graph
backend is a shared external database whose transient blip must not
hold every unrelated source hostage at startup):

- **Reachable backend, views validate** → the source registers
  healthy. Each view is proven at registration with one live call (the
  Cypher runs fetching at most one row; the result must convert against
  the declared schema).
- **Reachable backend, a view FAILS validation** → registration is
  REFUSED and the server does not start. A view whose `RETURN` arity or
  types disagree with its declared schema is a contract violation, not
  an outage; the error names the view and the backend's complaint.
- **Unreachable backend** → the source registers DEGRADED — but only
  genuine connectivity failures qualify (DNS, refused dial, network
  timeout: no server answered). A server that ANSWERED with an error —
  wrong credentials, AGE not installed, the graph missing — fails
  startup loudly, because that is a configuration problem, not an
  outage. When degraded does apply: views still register with their
  declared (planning-sufficient) schemas, `GET /data_source` reports
  `status: "degraded"`, and the first scan retries the validation of
  ALL the source's views — failing loudly with the failing view's name
  and the registration error if the backend is still gone or a view no
  longer matches, flipping the source back to `healthy` only when every
  view re-validates (the same all-or-nothing line reachable
  registration holds). The ad-hoc UDTF path behaves the same way: a
  `cypher_query` / `graph_schema` call on a degraded source IS the
  retry — a failure reports the registration error (the real cause,
  e.g. connection refused) next to the fresh failure rather than a bare
  timeout, and a success flips the source back to `healthy` once the
  view contracts re-prove (immediately, for a view-less source).

## Ad-hoc queries

```sql
-- Declared columns (REQUIRED on AGE; listed IN RETURN ORDER — the
-- binding is positional, and two same-typed columns declared out of
-- order swap silently):
SELECT name, n
FROM cypher_query(
  'kg',
  'MATCH (n:Person) WHERE n.age > $min RETURN n.name, n',
  '{"min": 30}',
  '{"name": "string", "n": "node"}'
);

-- Discovery: one (label, kind) row per label off ag_catalog.
SELECT * FROM graph_schema('kg');

-- Node/relationship `properties` are JSON text; the json_get family is
-- registered on every server session:
SELECT json_get_str(n.properties, 'name')
FROM cypher_query('kg', 'MATCH (n) RETURN n', '{}', '{"n": "node"}');
```

Writes are rejected twice: a keyword guard at plan time (UX), and the
backend's `READ ONLY` transaction (the actual boundary).

## Pipelines: request parameters → Cypher parameters

Pipeline parameters are substituted into SQL textually, and the two
passes (inference vs execution) disagree about nested-literal
positions — a `{param}` inside the params JSON string literal cannot
work. The settled spelling is **the placeholder occupies the whole
`params` argument**:

```yaml
kind: pipeline
metadata:
  name: people-over-age
spec:
  query: |
    SELECT name, age
    FROM cypher_query(
      'kg',
      'MATCH (p:Person) WHERE p.age > $min RETURN p.name AS name, p.age AS age',
      {params},
      '{"name": "string", "age": "int"}'
    )
```

At pipeline-load time `{params}` becomes `NULL`, which the UDTF accepts
as "no parameters" (schema inference needs only the literal `columns`).
At request time the caller passes the params JSON **as a string**:

```bash
curl -X POST localhost:8080/people-over-age/execute \
  -H 'Content-Type: application/json' \
  -d '{"params": "{\"min\": 40}"}'
```

The connection, cypher, and columns arguments stay strict literals —
they determine the plan, so a placeholder cannot produce one.

## JSON getters, without the operator rewrite

The server session registers the `datafusion-functions-json` getter
UDFs (`json_get`, `json_get_str`, `json_get_int`, …) unconditionally —
they are the extraction tool for every JSON column, graph `properties`
first among them. It deliberately does NOT install the crate's
`->` / `->>` / `?` operator rewrite: the rewrite would convert those
operators into `json_get(...)` calls at planning time, session-wide,
which datafusion-table-providers' unparser cannot translate back for
federated sources. Use `json_get_str(col, 'key')` explicitly.

Pick the getter that matches the property's JSON type: `json_get_str`
returns NULL (not a coercion) when the stored value is a number, so a
numeric property like `age` or `since` needs `json_get_int` /
`json_get_float` — a silently-NULL column is the usual symptom of the
wrong getter.

## Least-privilege deployment recipe

The read-only guarantee is backend-enforced, so run it with the least
privilege that works — **never a superuser**:

```sql
-- As the administrator, once:
CREATE ROLE kg_reader LOGIN PASSWORD '…';
GRANT USAGE ON SCHEMA ag_catalog TO kg_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA ag_catalog TO kg_reader;
-- Per graph (AGE stores each graph in a schema of the same name):
GRANT USAGE ON SCHEMA your_graph TO kg_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA your_graph TO kg_reader;
```

The client's `LOAD 'age'` is best-effort, deliberately: `LOAD` is
superuser-only for libraries outside `$libdir/plugins`, and the
official `apache/age` image already ships
`shared_preload_libraries = age`, so a reader role works as-is. On a
self-managed Postgres, set `shared_preload_libraries = 'age'` (or
`session_preload_libraries`) in postgresql.conf — do NOT solve a
failing registration by upgrading the credential to superuser. If AGE
is genuinely absent, registration fails with a named
`ag_catalog.ag_graph` probe error.

Config carries credentials as environment-variable NAMES only; a
password embedded in `connection_string` is rejected at config load.

## Bounds

Every query is bounded: `query_timeout_seconds` (1..=86400) becomes the
server-side `statement_timeout` plus a client-side wrap;
`max_rows` (1..=1000000) caps consumed rows with a typed
`RowCapExceeded`, and the fetch is a real SQL LIMIT of
`min(limit, max_rows + 1)` so the wire is bounded too. `max_connections`
(1..=64) sizes the pool; pool queueing is bounded by the same timeout.

## Troubleshooting

| Symptom | Cause → fix |
|---|---|
| `Operator ->> is not yet supported` (also `->`, `?`) | The `->`/`->>` operator rewrite is deliberately NOT installed on the server session (it would rewrite those operators session-wide, ahead of federated pushdown planning). Use the getter UDFs instead: `properties->>'name'` → `json_get_str(properties, 'name')`; `properties->'a'->>'b'` → `json_get_str(properties, 'a', 'b')`; `properties ? 'key'` → `json_contains(properties, 'key')`. |
| `graph source is registered DEGRADED (registration error: ... Connection refused ...)` | The backend was unreachable at startup and the retried query still can't reach it. Check the host/port and credentials in the env vars; the source flips back to `healthy` on its own once a query succeeds. |
| `graph view '<name>' failed validation: ...` (at startup) | A reachable backend rejected the view's contract — the `RETURN` clause's arity or types disagree with the declared `schema`. The error carries the backend's complaint; align the declaration with the Cypher and restart. |
| `graph scan exceeded max_rows = …` on a view scan | The view returned more rows than the source allows. SQL-side `WHERE` over a view does not push into Cypher — bound the view's Cypher itself (`WHERE`/`LIMIT`), or raise `max_rows`. See "A view's Cypher must carry its own bound". |
| `graph query timed out after Ns` | The statement ran past `query_timeout_seconds` server-side; narrow the traversal or raise the timeout. (If the backend was never reached, you'll see `could not acquire a connection …` instead — that one is connectivity, not query shape.) |
