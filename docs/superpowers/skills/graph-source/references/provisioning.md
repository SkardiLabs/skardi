# Provisioning the AGE backend

The read-only guarantee is **backend-enforced** (a `READ ONLY`
transaction wraps every query; the plan-time keyword guard is UX, not
the boundary). That is why the deployment posture matters: Skardi's
credential defines what a compromised or buggy query could ever do.

## The least-privilege reader role

Run Skardi's graph connection as a role that can read the graph and
nothing else — **never a superuser**:

```sql
-- As the administrator, once:
CREATE ROLE kg_reader LOGIN PASSWORD '…';
GRANT CONNECT ON DATABASE graphrag TO kg_reader;  -- explicit, in case PUBLIC's default was revoked
GRANT USAGE ON SCHEMA ag_catalog TO kg_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA ag_catalog TO kg_reader;

-- Per graph (AGE stores each graph in a schema of the same name):
GRANT USAGE ON SCHEMA your_graph TO kg_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA your_graph TO kg_reader;
```

`ag_catalog` access is what lets `graph_schema()` enumerate labels and
what registration probes (`ag_catalog.ag_graph`) to distinguish "AGE is
absent" from other failures.

## Why `LOAD 'age'` is best-effort, and what to do instead

The client issues `LOAD 'age'` on each connection as best-effort,
deliberately: `LOAD` is superuser-only for libraries outside
`$libdir/plugins`, and requiring it would force the exact credential
this recipe exists to avoid.

- The official `apache/age` image ships
  `shared_preload_libraries = age` — a reader role works as-is.
- On self-managed Postgres, set `shared_preload_libraries = 'age'` (or
  `session_preload_libraries`) in postgresql.conf.
- If registration fails with an `ag_catalog.ag_graph` probe error, AGE
  is genuinely absent from the server. Install it there. Do NOT solve
  this by upgrading Skardi's credential to superuser so `LOAD` works —
  that trades a one-line postgresql.conf fix for a standing privilege
  escalation.

## Credentials: env-var names only

Config carries credentials as environment-variable **names**
(`username_env`, `password_env`), never values. A password embedded in
`connection_string` is rejected at config load — this is enforcement,
not convention. Consequences for how you work:

- Export the variables in the server's environment (or the operator's
  Secret on cloud deployments); the YAML names them.
- Never echo a connection string into logs, error reports, or chat —
  on deployments that violate the rule elsewhere, the string is where
  the credential hides.

## A local AGE for development

```bash
docker run -d --name age -p 5455:5432 \
  -e POSTGRES_PASSWORD=devonly apache/age
```

Then create the graph and the reader role inside it:

```sql
SELECT * FROM ag_catalog.create_graph('knowledge');
-- …CREATE ROLE kg_reader… as above, plus your seed data via cypher()
```

The container image already preloads AGE, so the reader role works
without any superuser step. Seed data goes in with AGE's own
`cypher('knowledge', $$ CREATE (:Person {name: 'ada', age: 36}) $$)`
as the admin role — Skardi's surface is read-only and cannot seed.
