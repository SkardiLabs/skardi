# Graph sources (Cypher over AGE) — milestone status

Read-only Cypher against a property graph, surfaced as SQL tables
(design: `docs/superpowers/specs/2026-08-08-graph-engine-bypass-design.md`).
Skardi does not parse or store graphs — the graph engine owns storage
and traversal; Skardi forwards read-only Cypher and maps results into
Arrow rows with a planning-time-stable schema.

**Milestone status: M1 (Apache AGE) is engine-level API.** The
`cypher_query` / `graph_schema` UDTFs are registered per session via
`register_graph_source` + `register_graph_udtfs`; `type: graph` YAML
data sources, catalog views, and server wiring land with milestone 4.
Nothing here is reachable through a stock `skardi-server` yet.

## Call shapes

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
-- registered alongside:
SELECT json_get_str(n.properties, 'name')
FROM cypher_query('kg', 'MATCH (n) RETURN n', '{}', '{"n": "node"}');
```

Accepted column types: `string|str|utf8`, `int|integer|bigint`,
`float|double`, `bool|boolean`, `json`, `node`, `relationship`, `path`.
Every column is nullable. Writes are rejected twice: a keyword guard at
plan time (UX), and the backend's `READ ONLY` transaction (the actual
boundary).

## Session-wide side effect, stated plainly

`register_graph_udtfs` also runs `datafusion-functions-json`'s
`register_all`, which installs 12 UDFs **plus `->` / `->>` / `?`
operator rewrites for every query in the session** — not just graph
ones. Additive today; before milestone 4 wires this into the server
session it will be re-homed next to skardi's other UDF registrations
and checked against datafusion-federation (the rewrite runs before
federation planning). Know this before calling it on a shared session.

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

Config carries credentials as environment-variable NAMES only:

```yaml
# The shape milestone 4 will register; today these values feed
# register_graph_source directly.
backend: age
graph_name: knowledge
username_env: KG_READER_USER
password_env: KG_READER_PASS
query_timeout_seconds: 30   # server-side statement_timeout + client wrap
max_rows: 10000             # typed overflow error, never silent truncation
```

## Bounds

Every query is bounded: `query_timeout_seconds` (1..=86400) becomes the
server-side `statement_timeout` plus a client-side wrap;
`max_rows` (1..=1000000) caps consumed rows with a typed
`RowCapExceeded`, and the fetch is a real SQL LIMIT of
`min(limit, max_rows + 1)` so the wire is bounded too. `max_connections`
(1..=64) sizes the pool; pool queueing is bounded by the same timeout.
