# Troubleshooting

Work symptom-first. Every graph error is typed and names its cause;
an error that doesn't is itself a bug worth filing.

## Symptom table

| Symptom | Cause → fix |
|---|---|
| `Operator ->> is not yet supported` (also `->`, `?`) | The operator rewrite is deliberately not installed (it breaks federated pushdown session-wide). Use the getters: `json_get_str(properties, 'name')`, `json_get_str(properties, 'a', 'b')`, `json_contains(properties, 'key')`. |
| A property column is all NULL, no error | Wrong getter for the stored JSON type — `json_get_str` on a numeric property returns NULL, not a coercion. Switch to `json_get_int` / `json_get_float`. |
| Two columns look swapped | The ad-hoc `columns` argument binds positionally against the RETURN clause. Re-order the declaration to match RETURN — the names never did the matching. |
| `graph scan exceeded max_rows = …` on a view scan | SQL `WHERE` over a view does not push into Cypher; the view fetched everything. Bound the view's own Cypher (`WHERE`/`LIMIT`) or move the read to `cypher_query`. Raising `max_rows` is the last resort, not the fix. |
| `graph query timed out after Ns` | The statement ran past `query_timeout_seconds` server-side. Narrow the traversal or raise the timeout. If the backend was never reached you'd see `could not acquire a connection …` instead — that one is connectivity, not query shape. |
| `graph source is registered DEGRADED (registration error: … Connection refused …)` | Backend unreachable at startup and still unreachable on retry. Check host/port and the env-var credentials. The source flips healthy on its own once any query gets a response; inside the backoff window (`query_timeout_seconds` clamped to 30–300 s) scans fail fast with the cached diagnosis. |
| `graph view '<name>' failed validation: …` at startup, server refuses to start | A reachable backend rejected the view's contract — RETURN arity/types disagree with the declared `schema`. The error carries the backend's complaint; align the declaration with the Cypher and restart. Not retryable. |
| Startup failure naming `ag_catalog.ag_graph` | AGE is absent from that Postgres. Install it / set `shared_preload_libraries = 'age'`. Never fix this by granting Skardi superuser so `LOAD` works. |
| `type: graph requires hierarchy: catalog` | Set `hierarchy_level: catalog` on the source — views are catalog tables. |
| A null in a `nullable: false` view column | Typed error naming column and row. The declaration was an author's assertion the graph doesn't honor — either fix the data or stop asserting. |

## Distinguishing the three failure families

1. **Connectivity** (degraded, retried, self-healing): no server
   answered — DNS, refused dial, network timeout, `PoolTimedOut`.
2. **Contract** (refused at boot, or typed per-scan errors after
   recovery): the server answered and disagreed — view validation,
   type mismatches, nullability assertions.
3. **Bounds** (typed per-query errors): `RowCapExceeded`, statement
   timeout. These are protections doing their job; the fix is in the
   query or view, not in removing the protection.

Recovery semantics worth re-reading when confused about state: a retry
that gets ANY response flips the source healthy — a still-broken view
then fails its own scans while the others work. Only "no answer at all"
keeps a source degraded. And health is written only at registration and
recovery: `status_changed_at` is when the status last TRANSITIONED, not
a liveness heartbeat — "healthy" is one-way until a scan fails.

## Health through the gateway (cloud deployments)

Graph sources are today the only sources that populate the
`status` / `status_reason` / `status_changed_at` trio on the OSS
`GET /data_source`. On a **skardi-cloud gateway**, know two things:

- The gateway's response projection **deliberately drops the whole
  status trio** (skardi-cloud's contexts/login design §7.4.1 —
  `status_reason` is operator text that can carry a DSN or hostname).
  A tenant's agent sees a degraded graph source as a source with its
  declared tables and no health fields — `skardi schema` cannot tell
  "degraded" from "healthy and empty".
- Diagnosing health on cloud therefore happens on the operator side:
  the OSS engine's own `/data_source` (in-cluster), or the server logs,
  which carry the registration error and every transition.

Do not file "the gateway lost my status_reason" as a bug — it is a
recorded projection decision, and the place to revisit it is that
design's §7.4.1, not the gateway code.
