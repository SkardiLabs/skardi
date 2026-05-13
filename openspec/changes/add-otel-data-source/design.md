## Context

Skardi already federates ten storage backends (CSV, Parquet, Postgres, MySQL, SQLite, Iceberg, MongoDB, Redis, Lance, Seekdb) behind a single DataFusion `SessionContext`. Each lives in [crates/skardi/src/sources/providers/](crates/skardi/src/sources/providers/) and registers either a `TableProvider` (for static tables) or a `TableFunctionImpl` (for parameterized queries — see `prom_query`-style usage in `lance_knn`, `pg_fts`, etc.).

Observability data is the obvious next gap. Skardi's own `observability/` directory already runs Prometheus, Loki, Tempo, and an OTel collector to instrument the server. The data agents need to reason about (latency, error rates, structured logs) lives in those same systems — but agents can't read them well today:

- **Grafana / Datadog** dashboards encode the analytical work in JSON panels, not in a tabular API an LLM can chain on.
- **PromQL / LogQL / TraceQL / Datadog query syntax** are non-trivial DSLs; each agent project ends up writing a thin "send this PromQL string" tool that loses every advantage of the federated SQL plane.
- **OTLP** as a wire format is push-shaped (collectors → backends) and never agent-shaped (query-and-answer).

The cleanest fit with Skardi's existing model is: treat OTEL backends as just another federated source — the agent writes SQL, Skardi translates to backend-native API calls, returns Arrow batches, and DataFusion handles joins/filters/aggregation. No new query language to teach. This is the same shape we use for Postgres or MongoDB; we're applying it to time-series and log stores.

Stakeholders: agent builders (the README's "ship your first agent" persona), platform/SRE operators who already have Prometheus + Loki and want a way to expose them to agents safely, and Skardi's existing pipeline-author audience who want OTEL-derived `service_health` / `recent_errors` endpoints without writing custom Go/Python services. **All three audiences interact with this feature through pipeline files** (`kind: pipeline` YAML rendered into REST endpoints) — there is no separate OTEL surface and the existing demos under `demo/*/pipelines/` are the authoring pattern to copy. See Decision 10 for the contract.

## Goals / Non-Goals

**Goals:**

- One `otel` data source type that operators can declare in `ctx.yaml` exactly like any other source, with `backend: prometheus | loki` selection.
- A **plain-SQL default path** that lets an LLM agent answer the common observability questions (current value of a metric, error logs from service X in the last hour, request rate per service grouped by route) without ever emitting a PromQL or LogQL string. This is implemented as predicate / projection / aggregate pushdown on first-class `metrics` and `logs` `TableProvider`s.
- A **parametric escape hatch** — `prom_query`, `prom_range`, `loki_query`, `loki_range` — for queries that fall outside the supported pushdown set (e.g. `rate()`, `histogram_quantile()`, LogQL `| json | line_format`, `topk()`). Errors raised by the translator on unsupported patterns SHALL name the offending predicate and explicitly point the caller at the escape hatch.
- Federation with all existing sources via the standard DataFusion `SessionContext`.
- Safety defaults — per-query row caps, default/maximum time windows, per-request timeouts, env-only credentials — that bound the blast radius of any single agent-issued query against a production Prometheus.
- Compatibility with the broad Prometheus-API ecosystem (Mimir, Thanos, VictoriaMetrics, Grafana Cloud, Datadog's Prometheus-compat endpoint) without per-vendor code.
- Off-by-default cargo feature so non-OTEL users don't pay the dependency cost.

**Non-Goals:**

- Receiving OTLP push traffic into Skardi storage. The OTLP receiver path is interesting but is a separate change with very different operational properties (long-running ingest, buffering, backpressure, schema evolution).
- Tempo / trace span querying. Worth doing later but the schema model (span trees) maps poorly to flat SQL rows and we'd ship a worse version of TraceQL. Punt.
- Datadog's native (non-Prometheus-compat) query API. Datadog exposes a Prometheus-compatible endpoint we can use; the native API is vendor-specific.
- Exemplars, native histograms, and `/api/v1/metadata` enrichment. Phase 2.
- A new write path for any OTEL backend. OTEL sources are read-only.
- Schema discovery via OTel semantic conventions (e.g. resolving `service.name` to a canonical label). Useful but better handled in Skardi's semantic layer once the raw plumbing exists.
- **PromQL-aware SQL UDFs** — `rate(metrics, '5m')`, `histogram_quantile(0.99, ...)`, `topk(n, ...)` — that fold into the translator. These give the "best of both worlds" for the gnarly cases but are real engineering on top of the v1 translator. Callers reach for `prom_query` in v1; we revisit once usage tells us which UDFs would pay back the implementation cost.
- **LogQL pipeline pushdown.** v1 translates only stream selectors and simple `LIKE`-style line filters into LogQL; `| json`, `| line_format`, `| unwrap`, and aggregation pipelines stay in `loki_query`.

## Decisions

### Decision 1 — Federate, don't ingest

We register OTEL backends as DataFusion `TableProvider`s + `TableFunctionImpl`s that issue HTTP requests on `scan()`, rather than building an OTLP receiver that buffers data into Lance/Parquet.

**Why:** The user's stated value is "agents can already SELECT — let them SELECT over telemetry". Ingestion adds an operational burden (long-running collector, buffering, retention policy) that doesn't serve that value. Operators already run Prometheus/Loki for retention and downsampling; Skardi has no business duplicating that.

**Alternatives considered:**

- *OTLP gRPC/HTTP receiver into Lance.* Real product, wrong scope for this change. Filed as a follow-up.
- *Object-store reader for OTEL-exported Parquet.* Works but assumes a non-default export pipeline. Doesn't help the 99% of users whose data is in Prometheus today.

### Decision 2 — One `Otel` enum variant with a `backend` discriminator, not separate `Prometheus` and `Loki` variants

`DataSourceType::Otel` plus a `backend: prometheus | loki` config field, rather than `DataSourceType::Prometheus` and `DataSourceType::Loki` as separate enum variants.

**Why:** The OpenTelemetry data model is what unifies metrics, logs, and (later) traces — they share label semantics, resource attributes, and time-series conventions. Bundling under `Otel` keeps the `DataSourceType` enum from sprawling as we add `traces` later and lets us share HTTP-client code, credential handling, time-window guardrails, and error types across backends. Operators also tend to think of "the OTEL stack" rather than each component in isolation.

**Trade-off:** Slightly more nested config (`type: otel` + `backend: prometheus` instead of just `type: prometheus`). Acceptable; the cost is one extra field at declaration time.

### Decision 3 — Use `prometheus-http-query` for Prometheus, hand-rolled `reqwest` for Loki

Adopt `prometheus-http-query` (well-maintained, typed responses, async, supports range/instant/label endpoints) for the Prometheus backend. Loki's HTTP API is small enough that a thin `reqwest` wrapper over `/loki/api/v1/query` and `/loki/api/v1/query_range` is simpler than pulling in a Loki-specific crate.

**Why:** `prometheus-http-query` handles the schema gymnastics (`vector`, `matrix`, `scalar` result types, label maps) that we'd otherwise replicate. For Loki, there's no equivalent mature crate, and the response shape is straightforward (`streams` and `matrix` types over JSON).

**Alternatives considered:**

- *Hand-rolled Prometheus client.* More dependencies-minimized but reimplements typed parsing that's already a solved problem.
- *`grafana-rs` or a unified Grafana-API client.* Too broad; we want backend-native APIs, not Grafana proxying.

### Decision 4 — Predicate-pushdown SQL tables for the common path, parametric table functions as the escape hatch

Each registered OTEL source contributes two surfaces:

1. **Tier-1 — first-class SQL tables with predicate pushdown.** `metrics` (for Prometheus-backed sources) and `logs` (for Loki-backed sources) are real `TableProvider`s with a fixed schema (see Decision 6). Their `scan(state, projection, filters, limit)` implementations inspect the incoming DataFusion plan, recognize a documented set of predicate / projection / aggregate shapes, and translate them into a single backend HTTP call. Predicates the translator does not recognize cause the scan to fail fast with `OtelError::UnsupportedPushdown { source, predicate, hint }` — we do **not** fetch the universe and filter in DataFusion, because the cardinality risk is too high.
2. **Tier-3 — parametric escape-hatch table functions.** `prom_query(q)`, `prom_range(q, start?, end?, step?)`, `loki_query(q)`, `loki_range(q, start?, end?, limit?)` take the backend-native query string verbatim and remain in the API surface for queries that the translator can't express — anything involving `rate()`, `histogram_quantile()`, `topk()`, LogQL `| json` / `| line_format` pipelines, etc.

We are **deliberately skipping Tier-2** (DataFusion UDFs like `rate(metrics, '5m')` that fold into the translator) for v1. See Non-Goals.

**Supported v1 pushdown for `metrics` (Prometheus):**

| SQL shape                                                                | Translates to                                                      | Endpoint                |
| ------------------------------------------------------------------------ | ------------------------------------------------------------------ | ----------------------- |
| `WHERE name = '<metric>'` (REQUIRED — translator errors without it)      | metric selector `<metric>{}`                                       | both                    |
| `WHERE labels['k'] = '<v>'` (any number, ANDed)                          | `k="<v>"`                                                          | both                    |
| `WHERE labels['k'] != '<v>'`                                             | `k!="<v>"`                                                         | both                    |
| `WHERE labels['k'] LIKE '<pat>'` (SQL `%` → regex `.*`, `_` → `.`)       | `k=~"<re>"`                                                        | both                    |
| `WHERE labels['k'] NOT LIKE '<pat>'`                                     | `k!~"<re>"`                                                        | both                    |
| `WHERE ts BETWEEN <start> AND <end>` or two `>=`/`<=` inequalities on `ts` | `start=<s>&end=<e>&step=<default_step>`                          | `/api/v1/query_range`   |
| no `ts` filter                                                           | `default_window`, `default_step` (Decision 7)                      | `/api/v1/query_range`   |
| `ts = <single_timestamp>`                                                | `time=<t>`                                                         | `/api/v1/query`         |
| `SELECT name, labels, sum(value) … GROUP BY name, labels['k1'], labels['k2']` (and `avg`, `min`, `max`, `count`) | `sum by(k1, k2) (<selector>)`                                      | `/api/v1/query` (instant) |
| `LIMIT n`                                                                | client-side cap after translation; does **not** become PromQL `topk` | both                    |

**Supported v1 pushdown for `logs` (Loki):**

| SQL shape                                                                | Translates to                                                      | Endpoint                |
| ------------------------------------------------------------------------ | ------------------------------------------------------------------ | ----------------------- |
| `WHERE labels['app'] = '<v>'` etc. (at least one stream-label equality REQUIRED) | LogQL stream selector `{app="<v>"}`                          | `/loki/api/v1/query_range` |
| `WHERE labels['k'] LIKE '<pat>'`                                         | `k=~"<re>"`                                                        | range                   |
| `WHERE line LIKE '%<substr>%'`                                           | LogQL line filter `\|= "<substr>"`                                 | range                   |
| `WHERE line NOT LIKE '%<substr>%'`                                       | LogQL `\|!~ "<substr>"`                                            | range                   |
| `WHERE ts BETWEEN <start> AND <end>`                                     | `start`, `end` params                                              | range                   |
| `LIMIT n`                                                                | `limit=n` (Loki has a hard server cap; we forward what the operator's `max_result_rows` allows) | range |

**Unsupported → fail with a pointer.** Examples that v1 rejects with `OtelUnsupportedPushdown`:
- `SELECT * FROM metrics` with no `name = '...'` predicate (would require a fan-out across all metrics — refuse and tell the caller to use `prom_query('{__name__=~".+"}')` if they really mean it).
- `SELECT value * 100 FROM metrics …` (arithmetic on projected columns isn't pushed; DataFusion handles it after the scan, which is fine — this is *projected* expressions, not predicates, and the table provider returns raw rows for DataFusion to compute on top of).
- `WHERE value > 0.5` (PromQL value-filter `> 0.5` exists but routes via the *evaluation* engine, not selectors; deferred — translator errors with a pointer to `prom_query('<expr> > 0.5')`).
- `GROUP BY ts_bucket(...)` (range-vector aggregation; needs `rate()`-style operators; escape hatch).
- LogQL `| json` / `| line_format` / `unwrap` — any LogQL pipeline beyond stream + line filter goes through `loki_query` / `loki_range`.

**Why this shape:** The original "browse table + parametric function" design left agents writing PromQL for *every* real query, which contradicts the proposal's "agent-friendly" goal. Predicate pushdown for the patterns above covers the canonical agent verbs ("current value", "last hour by service", "error rate per route") in plain SQL. The escape hatch keeps us honest: PromQL / LogQL have semantics (counter resets, exemplars, log pipelines) that don't have native SQL analogues, and pretending otherwise via fragile auto-translation produces wrong answers silently. A clear `OtelUnsupportedPushdown` error with the offending predicate quoted is a much better failure mode than a misleading translation.

**Alternatives considered:**

- *Function-only surface (the previous draft).* Rejected — defeats the proposal's premise that agents shouldn't need to learn PromQL/LogQL for common questions.
- *Fetch-then-filter (no pushdown — pull samples, filter in DataFusion).* Rejected — Prometheus and Loki are append-only firehoses; pulling without selectors will OOM the server or DOS the upstream.
- *Tier-2 UDFs in v1 (`rate(...)` as a SQL function).* Deferred — see Non-Goals. Real engineering on top of the v1 translator and we don't yet know which UDFs would pay back. `prom_query` covers the same cases in v1 with a known cost.

**v1 sharp edge — aggregations happen at the DataFusion level.**

Until task 4.2 lands, `GROUP BY` + `SUM`/`AVG`/`MIN`/`MAX`/`COUNT` over `metrics` rows is **not** pushed down to PromQL. The translator's `agg` / `group_by` parameters exist on the function signature but reject any non-`None` value with `UnsupportedPushdown` (verified in `metrics_aggregate_pushdown_is_explicitly_not_yet_supported`). DataFusion plans an Aggregate node on top of the raw scan and runs the aggregation in-process after `(name, labels, ts, value)` rows arrive from the upstream.

This has three consequences agents and operators need to know about:

1. **Counter-vs-gauge semantic mismatch.** For counter metrics (`http_requests_total`, `*_total`), summing raw samples gives the sum of monotonically-increasing counter values across the time window — *not a rate*. With a 15-minute default window and 30-second step, a single series produces ~30 samples whose sum is mathematically meaningless. The PromQL-correct query is `sum by(service)(rate(http_requests_total[5m]))`, which v1 surfaces only via `prom_query('rate(...)')`. For **gauges + instant queries** (`WHERE ts = <time>`) the SUM/AVG over one-sample-per-series is semantically correct, so the simple case works.
2. **Cardinality risk.** `GROUP BY labels['k']` against a million-series metric over an hour-long window fetches every raw sample before DataFusion can aggregate, tripping `max_result_rows` (default 50_000) long before any grouping happens.
3. **The honest workaround for v1.** For counter-style aggregation, agents should use the tier-3 escape hatch (`SELECT * FROM prom_query('sum by(service)(rate(http_requests_total[5m]))')`) rather than rely on DataFusion-side `GROUP BY`. The `docs/otel/` page (task 9.1) calls this out under "when to reach for the escape hatch".

Task 4.2 closes this gap: the translator will recognize `GROUP BY labels['k'] SUM/AVG/MIN/MAX(value)` and emit `<agg> by(k)(<selector>)` against `/api/v1/query` (instant), so the most-common shape — "current value summed by service" — gets correct PromQL semantics in plain SQL. The full fix depends on 3.5.2's `labels['k']` recognition landing first.

### Decision 5 — Credentials only via env vars, never inline

`auth.env: PROMETHEUS_BEARER` is supported; `auth.token: "abc"` is a hard error at config load.

**Why:** `ctx.yaml` lives in repos and gets emailed around. We don't want to be the source of a Prometheus token leak. Env-only matches how we handle Postgres passwords today and is consistent across the codebase.

### Decision 6 — Static schema for `metrics` / `logs` tables and the escape-hatch functions, with dynamic labels carried in a `Map<Utf8, Utf8>` column

Every row exposed by an OTEL source — whether returned by the `metrics` / `logs` `TableProvider` or by the `prom_query` / `loki_query` family — uses one of two fixed schemas:

- **`metrics` / `prom_query` / `prom_range`:** `name: Utf8`, `labels: Map<Utf8, Utf8>`, `ts: Timestamp(Millisecond, UTC)`, `value: Float64`. `GROUP BY` aggregations (`sum`, `avg`, etc.) project an arbitrary subset of these plus the aggregate column; DataFusion handles that on top of the scan's fixed schema.
- **`logs` / `loki_query` / `loki_range`:** `ts: Timestamp(Nanosecond, UTC)`, `labels: Map<Utf8, Utf8>`, `line: Utf8`, `stream: Utf8`.

**Why:** A fixed schema is what makes predicate pushdown work at all — DataFusion needs to plan over a stable schema, and any unknown-at-planning-time columns would have to live behind a Map. Prometheus labels are also query-dependent (one query returns `{job, instance}`, another returns `{service, route, status}`); we cannot infer them without executing the query, which would defeat planning. The `Map<Utf8, Utf8>` cell keeps the schema stable while preserving access via `labels['service']`, which is the same idiom we'll use whether the row came from the tier-1 table scan or the tier-3 escape hatch.

**Trade-offs:**

- Agents need to know `labels['key']` syntax. Mitigation: document prominently in the OTEL doc page and demo pipelines; demo `service_health` shows the canonical patterns.
- DataFusion's Map support has rough edges (e.g. joining on map elements is awkward). Mitigation: if real-world queries hit walls, add a sibling `labels_json: Utf8` column as an escape hatch (deferred — not in v1).

### Decision 7 — Default-window guardrails on every range-shaped scan

When a query against the `metrics` / `logs` tables omits a `ts` predicate, **and** when `prom_range` / `loki_range` are called without explicit start/end/step, we apply a configured default (15 min window, 30 s step). Caller-supplied values — whether a SQL `WHERE ts BETWEEN …` or an explicit function argument — override completely (no merging). Windows exceeding `max_window` (24 h default) are rejected before the upstream call with `OtelError::WindowTooLarge`.

**Why:** Agents loop. A loop that defaults to "all of history" against Prometheus is a self-DOS. The default window pushes the cost of "I want more data" onto the caller as an explicit argument, which is the right place for it. Applying the same default to both the tier-1 SQL path and the tier-3 escape hatch keeps the guardrail uniform regardless of which surface the agent reaches for.

### Decision 8 — Off-by-default `otel` cargo feature on `crates/skardi`

`reqwest` and `prometheus-http-query` are pulled in only when the `otel` feature is enabled. `skardi-server` re-exports the feature so server builds opt in explicitly.

**Why:** Keeps the default build slim. Matches how we handle `embedding`, `gguf`, `candle`, etc.

### Decision 9 — Read-only access mode

OTEL sources register with `AccessMode::ReadOnly`; `sql_validator` rejects `INSERT`/`UPDATE`/`DELETE` at parse time.

**Why:** Neither Prometheus's remote-write nor Loki's push endpoint is the right place for ad-hoc agent writes; ingestion belongs upstream of Skardi.

### Decision 10 — Pipelines are the canonical authoring + invocation surface; no new YAML kind

OTEL-derived data reaches agents through the existing pipeline mechanism (`kind: pipeline` YAML files with a `spec.query` SQL string, rendered into REST endpoints), exactly the same as every other Skardi source. We do **not** add a new YAML kind, a new endpoint type, or a special-case `kind: otel-pipeline`. We also do not document `skardi sql` ad-hoc queries against the OTEL tables as a *user-facing* path; the CLI remains available for operator validation during source bring-up, but the agent-facing contract is the pipeline endpoint.

**Why:**

- **Consistency with existing demos.** [demo/simple_backend/pipelines/list_tasks.yaml](demo/simple_backend/pipelines/list_tasks.yaml) and [demo/rag/server/pipelines/search_hybrid.yaml](demo/rag/server/pipelines/search_hybrid.yaml) are exactly the model we want operators to copy. An OTEL `service_health` pipeline is structurally identical to those: declare params, write SQL against named sources, ship the file. Zero new authoring concepts.
- **Stable contract for agents.** Agents call a named endpoint (`POST /pipelines/service-health` with a JSON body); the SQL behind the endpoint is operator-owned implementation. This survives schema evolution, translator improvements, and the eventual tier-2 `rate()` UDF without changing the agent contract.
- **Parameter binding is already solved.** The pipeline engine's `{param}` substitution becomes the only param surface operators have to think about — they don't separately deal with the OTEL source's `default_window` config and the pipeline's `{window}` parameter.

**Trade-off:** Operators who want a fully ad-hoc agent (one that synthesizes SQL on the fly and runs it through a generic SQL endpoint) get less from this change than operators authoring pipelines. That is the right priority: the README's "ship your first agent in 60 seconds" persona is pipeline-shaped, and the alternative — telling agents to write SQL against a fixed schema — adds a learning curve we don't otherwise need.

**Implications for tasks:** The demo (`demo/otel_service_health/`) ships three pipeline files, not three ad-hoc queries. The end-to-end smoke test (task 8.3) boots the server and hits pipeline endpoints. Documentation (task 9.1) leads with a pipeline example, not with `skardi sql`.

## Risks / Trade-offs

- **Unbounded label cardinality** → Mitigation: `max_result_rows` cap (default 50_000) + `OtelResultTooLarge` error with a clear message. Documented in the OTEL page.
- **Slow Prometheus queries blocking the DataFusion executor thread** → Mitigation: every backend call goes through `tokio::spawn` + a per-request timeout (default 10 s, configurable). The HTTP client is `reqwest` async; we never block.
- **Vendor-specific Prometheus-compat quirks (e.g. Mimir tenant headers, Datadog rate limits)** → Mitigation: a generic `extra_headers: { X-Scope-OrgID: tenant-a }` config knob covers tenant identification without per-vendor code.
- **Map<Utf8,Utf8> ergonomics in SQL** → DataFusion's Map support has gaps (e.g. JOIN on map elements is awkward). Mitigation: document the `labels['key']` access pattern; if real-world queries hit walls, add a `labels_json` Utf8 escape hatch.
- **Leaky translator abstraction** → A SQL query that *looks* valid but trips an unsupported predicate (e.g. `WHERE value > 0.5`, or a missing `name = '…'`) produces an error rather than results. Mitigation: every `OtelUnsupportedPushdown` error quotes the offending predicate and suggests the equivalent `prom_query` / `loki_query` call; demo pipelines deliberately exercise both surfaces so agents (and humans) see the boundary. We are also explicit in `docs/otel/` about the supported-pattern matrix.
- **PromQL/LogQL still required for the escape hatch** → Mitigation: example pipelines in `demo/` show the canonical agent queries; agents can copy from them rather than synthesizing from scratch. The tier-1 SQL surface is intended to cover the common questions so the escape hatch is reached only when the agent genuinely needs PromQL semantics (counter rates, histogram quantiles, log pipelines).
- **Schema drift between Prometheus versions** → `prometheus-http-query`'s typed responses contain the surface area; if a future Prometheus changes wire format we pin the crate version and bump deliberately.
- **Credential rotation requires a restart** → Acceptable for v1. Hot-reload of env-sourced credentials is a separate concern.
- **Discoverability of the new feature** → Mitigation: README "Roadmap" bullet, an entry in `docs/observability.md`, and a `demo/otel_service_health/` runnable example with the existing `observability/docker-compose.yml`.

## Migration Plan

- **Deploy:** Land behind the `otel` cargo feature; server users opt in via build flag. No DB migrations, no schema changes to existing sources.
- **Rollback:** Disable the feature flag or remove the `type: otel` block from `ctx.yaml`; nothing else depends on it.
- **Adoption:** Demo pipeline + docs page + a paragraph in the README "Sources supported" table. After ~one release of soak time on the demo, consider flipping the feature to default-on in `crates/server/Cargo.toml`.

## Open Questions

- **Default cargo feature?** Start opt-in; revisit after one release of usage. Leaning opt-in to keep server binary size predictable.
- **Should `prom_range` accept relative-time strings (`'-1h'`) in addition to absolute timestamps?** Strong yes for ergonomics; mild risk of confusion with SQL `INTERVAL` literals. Will pick one canonical form (absolute timestamps + an optional `interval` arg) and document the agent pattern.
- **Where does query history / cache go?** Out of scope for v1 — every call hits upstream. A small in-memory LRU keyed by (source, query, time bucket) would help loopy agents; defer.
- **Pipeline parameter binding for PromQL strings** — current pipeline machinery substitutes `{{param}}` before SQL parse. PromQL strings contain `{` and `}` characters that may collide. Need to confirm the template engine escapes / leaves them alone correctly during task 4 wiring.
- **Tempo / traces** — defer, but track the schema question: traces are tree-shaped and don't flatten cleanly. Likely needs a `spans` table plus `trace_id`-keyed lookup function rather than a single `traces` table.
- **When does the translator graduate from "documented matrix" to "extensible registry"?** v1 hard-codes the supported predicate shapes in `otel/translator.rs`. If the supported set keeps growing per-release (e.g. a Mimir-specific predicate, a vendor-specific label-matcher form) we'll want a small visitor/registry abstraction. Don't pre-build it — wait until we add the third pattern.
- **Whether `WHERE labels['k'] IN ('a','b','c')` should translate to `k=~"a|b|c"` in v1.** It's a small extension to the equality matcher and shows up in agent-generated SQL constantly. Strong lean toward yes; flagged for the implementation pass.
