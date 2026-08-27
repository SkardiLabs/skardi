---
name: graph-source
description: >-
  Connect a property graph (knowledge graph, GraphRAG corpus, Apache AGE /
  openCypher data) to Skardi and query it through SQL, end to end: provision
  the AGE backend with a least-privilege role, declare the `type: graph`
  source and its views in context YAML, verify registration health, write
  correct queries (`cypher_query`, `graph_schema`, the JSON getters), and
  wire Cypher parameters into pipelines. Use this skill whenever the user
  wants graph data queryable in Skardi, mentions graphs, Cypher, AGE,
  Neo4j-style data, knowledge graphs, GraphRAG, `kg.*` tables,
  `cypher_query`, or is debugging a degraded graph source, a
  `RowCapExceeded`, or a silently-NULL property column — even if they never
  say the words "graph source".
---

# Connecting a graph source to Skardi

You are wiring a property graph into Skardi as SQL tables. Skardi does
not parse or store graphs — the graph engine (Apache AGE: openCypher
inside Postgres) owns storage and traversal; Skardi forwards **read-only**
Cypher and maps results into Arrow rows with a planning-time-stable
schema. Design: `docs/superpowers/specs/2026-08-08-graph-engine-bypass-design.md`;
operational truth: `docs/graph.md`. When this skill and those documents
disagree, the documents win — check them, then fix this skill.

**Scope, stated so you don't promise what doesn't exist:** the shipped
backend is AGE (milestone status M4 — `type: graph` sources, YAML
catalog views, the `cypher_query` / `graph_schema` UDTFs, pipeline
parameter passthrough). Neo4j and Kuzu are later milestones: the config
carries a `backend:` field for them, but do not write configuration or
guidance for a backend that has no implementation. Milestone one is
read-only end to end; write paths are deferred by design.

The reason this skill exists: every trap below is real, was hit, and is
documented — but scattered. The two that cost the most debugging time:
**the ad-hoc `columns` argument binds positionally** (two same-typed
columns declared out of RETURN order swap silently — no error, wrong
data), and **a view's SQL `WHERE` does not push into its Cypher** (a
query that wants one row still fails `RowCapExceeded` if the view is
unbounded). Both are invisible until they bite.

## The workflow

Work the phases in order. Each phase names its reference file — read it
before doing that phase's work, not after it goes wrong.

### Phase 1 — Provision the backend

Read [references/provisioning.md](references/provisioning.md).

Stand up (or locate) the AGE instance and create a **least-privilege
reader role** — never a superuser, and never "fix" a failing
registration by upgrading the credential. Credentials enter Skardi as
environment-variable NAMES only; a password embedded in
`connection_string` is rejected at config load.

### Phase 2 — Declare the source

Read [references/configuration.md](references/configuration.md).

Write the `type: graph` entry in context YAML: `hierarchy_level:
catalog` (required — views register as `<name>.main.<view>`), the
`graph:` block (backend, graph_name, credential env-var names, bounds),
and the views. Decide view vs ad-hoc deliberately:

- **Declare a view** when agents need a predictable table name and the
  query shape is stable. A view's Cypher **must carry its own bound**
  (`WHERE` / `LIMIT` inside the Cypher) — SQL predicates over the view
  do not push down.
- **Leave it to ad-hoc `cypher_query`** when the predicate belongs to
  the caller. The caller places the bound where it works.

### Phase 3 — Verify registration

Read the registration-semantics section of
[references/configuration.md](references/configuration.md), then start
the server and check which of the three states you got:

- **healthy** — backend reachable, every view validated with one live
  call. Note what that proves: arity always; type and `nullable: false`
  only for the sampled row. Per-row enforcement lives in execution.
- **refused** (server does not start) — the backend ANSWERED and a view's
  contract is broken (RETURN arity/types disagree with the declared
  schema), or the server answered with an error (bad credentials, AGE
  absent, graph missing). This is a bug in your declaration or
  deployment, not an outage — fix it, don't retry it.
- **degraded** — genuine connectivity failure only (DNS, refused dial,
  network timeout). Views still register on declared schemas; the first
  scan retries; recovery flips healthy on ANY server response.

Then prove the read path with the discovery UDTF and one view scan:

```sql
SELECT * FROM graph_schema('kg');          -- one (label, kind) row per label
SELECT * FROM kg.main.people LIMIT 5;
```

### Phase 4 — Write the queries

Read [references/querying.md](references/querying.md) — it carries the
positional-binding rule, the getter table, and the patterns. The three
rules that prevent silent wrong answers:

1. **`columns` is positional.** Declare ad-hoc columns in RETURN order;
   the names are labels for SQL, not a lookup key.
2. **Properties are JSON text; pick the getter by the stored JSON
   type.** `json_get_str` on a numeric property returns NULL — not an
   error, not a coercion. `age` needs `json_get_int`.
3. **`->` / `->>` / `?` are deliberately not installed** (the rewrite
   would break federated pushdown session-wide). Use the getter UDFs.

### Phase 5 — Pipeline it (when a pipeline is asked for)

Read the pipelines section of
[references/querying.md](references/querying.md). One spelling works:
the `{param}` placeholder occupies the **whole `params` argument** of
`cypher_query`, and the caller passes the params JSON **as a string**.
Connection, cypher, and columns stay strict literals — they determine
the plan, so a placeholder cannot produce one.

### When something breaks

Read [references/troubleshooting.md](references/troubleshooting.md) —
symptom → diagnosis, including how graph health surfaces through
`GET /data_source` (and what the skardi-cloud gateway's projection
deliberately hides from tenants).

## Working style

- **Verify against the running system, not this skill.** One
  `graph_schema('kg')` call answers more than an hour of config
  re-reading. The milestone status and bounds cited here were true when
  written; `docs/graph.md` and the config code are the authority.
- **No credentials anywhere but env-var names.** Not in YAML, not in
  logs, not in error reports you paste. Connection strings are never
  echoed — they may carry credentials on other deployments even though
  this one must not.
- **Errors are typed and name their cause.** If you see an untyped or
  misleading graph error, that's a bug worth filing, not working around.
- **Don't widen bounds to make symptoms disappear.** `max_rows` and
  `query_timeout_seconds` are protections; the fix for `RowCapExceeded`
  is a bound in the view's Cypher, and the fix for a timeout is usually
  a narrower traversal.
