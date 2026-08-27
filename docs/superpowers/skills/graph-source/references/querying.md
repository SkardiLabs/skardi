# Writing the queries

## Discovery first

```sql
SELECT * FROM graph_schema('kg');
```

One `(label, kind)` row per label, straight off `ag_catalog` — kinds
are node vs relationship labels. Run this before writing Cypher against
an unfamiliar graph; it is also the cheapest liveness probe a degraded
source can get (the call IS the recovery retry).

## Ad-hoc `cypher_query`

```sql
SELECT name, n
FROM cypher_query(
  'kg',                                                    -- source name
  'MATCH (n:Person) WHERE n.age > $min RETURN n.name, n',  -- read-only Cypher
  '{"min": 30}',                                           -- params JSON (or NULL)
  '{"name": "string", "n": "node"}'                        -- declared columns
);
```

**The `columns` argument is REQUIRED on AGE, and it binds
POSITIONALLY.** The declared entries pair with the RETURN clause's
items **in order**; the names are labels for the SQL side, not a lookup
key into the Cypher. Two same-typed columns declared out of RETURN
order swap **silently** — no error, plausible-looking wrong data. When
you touch a query, re-check the declaration order against the RETURN
clause; when you review one, check it there first.

Type mismatches, by contrast, fail loudly: each returned value converts
against its declared type, and a mismatch is a typed error carrying
column name, row index, expected type, and found JSON kind.

Writes are rejected twice — a keyword guard at plan time (UX) and the
backend's `READ ONLY` transaction (the actual boundary). Don't try to
smuggle mutations; milestone one is read-only by design.

## Properties are JSON text: the getter rules

A `node` column is a struct — `STRUCT<id, labels, properties>` (a
relationship likewise, with endpoint ids) — and `properties` is the
JSON-text field INSIDE it. Address the field, then the key:
`json_get_str(n.properties, 'name')`. Passing the whole struct column
to a getter (`json_get_str(n, 'properties', 'name')`) is the natural
wrong guess and does not work — the getters take JSON text, not a
struct. The `datafusion-functions-json` getters are registered on every
server session:

```sql
SELECT json_get_str(n.properties, 'name'),
       json_get_int(n.properties, 'age')
FROM cypher_query('kg', 'MATCH (n:Person) RETURN n', '{}', '{"n": "node"}');
```

Two rules, both of which fail silently when broken:

1. **Pick the getter by the STORED JSON type.** `json_get_str` on a
   numeric property returns NULL — not a coercion, not an error. A
   silently-NULL column is the classic symptom of the wrong getter:
   `age`, `since`, counts → `json_get_int` / `json_get_float`.
2. **The `->` / `->>` / `?` operators are deliberately NOT installed.**
   The crate's operator rewrite would convert them into `json_get(...)`
   calls at planning time, session-wide, which the federation unparser
   cannot translate back for federated sources. Spell it out:
   `properties->>'name'` → `json_get_str(properties, 'name')`;
   `properties->'a'->>'b'` → `json_get_str(properties, 'a', 'b')`;
   `properties ? 'key'` → `json_contains(properties, 'key')`.

Nested access passes multiple keys to one getter, as above — do not
chain getters.

## Views vs ad-hoc: the decision, restated as a rule

| You control the predicate | Use |
|---|---|
| No — agents/dashboards need a stable table name and a fixed shape | A view, whose Cypher carries its own `WHERE`/`LIMIT` |
| Yes — the caller knows what it's filtering on | `cypher_query`, with the predicate (and `$params`) in the Cypher |

The failure mode this table prevents: a view declared without a bound,
consumed with SQL `WHERE`, works in the demo (small graph) and dies in
production with `RowCapExceeded` the day the graph outgrows `max_rows`.

## Pipelines: request parameters → Cypher parameters

Pipeline parameters substitute into SQL textually, and the two passes
(inference vs execution) disagree about nested-literal positions — a
`{param}` INSIDE the params JSON string literal cannot work. The one
spelling that works: **the placeholder occupies the whole `params`
argument**.

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
they determine the plan, so a placeholder cannot produce one. If you
need a parameterized column set, that is two pipelines, not one clever
one.
