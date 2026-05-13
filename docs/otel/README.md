# OTEL Source (Prometheus + Loki Consumer)

Skardi can federate over OpenTelemetry-backed metrics and logs the same
way it federates over Postgres, SQLite, Iceberg, Lance, and the rest.
Declare a `type: otel` source in `ctx.yaml`, point it at a
Prometheus-compatible API and/or a Loki API, and your pipelines gain
two new tables (`metrics`, `logs`) plus four parametric escape-hatch
table functions (`prom_query`, `prom_range`, `loki_query`,
`loki_range`).

This doc is the operator-facing reference. For the design rationale and
the v1 sharp edges baked into the implementation, see
[`openspec/changes/add-otel-data-source/`](../../openspec/changes/add-otel-data-source/).

> **Skardi *emitting* OTEL telemetry about itself** is a different feature —
> see [`docs/observability.md`](../observability.md). This page covers
> the consumer side: Skardi pulling metrics/logs from external Prometheus
> or Loki backends.

## Quick start

```yaml
# ctx.yaml
kind: context
metadata:
  name: my-agent-ctx
spec:
  data_sources:
    - name: prom
      type: otel
      otel:
        backend: prometheus
        url: http://localhost:9090
```

Build with the feature on, boot the server, and a pipeline like the one
below works end-to-end:

```yaml
# pipelines/service_health.yaml
kind: pipeline
metadata:
  name: "service-health"
spec:
  query: |
    SELECT name, labels, ts, value
    FROM metrics
    WHERE name = 'http_requests_total'
    ORDER BY ts DESC
    LIMIT 1000
```

```bash
cargo build --release --bin skardi-server --features otel
./target/release/skardi-server --ctx ctx.yaml --pipelines pipelines/
curl -X POST http://localhost:3000/service-health/execute \
    -H 'Content-Type: application/json' -d '{}'
```

The runnable end-to-end example lives at
[`demo/otel_service_health/`](../../demo/otel_service_health/).

## Supported backends

Any HTTP API that speaks the documented Prometheus or Loki query
endpoints. Concretely:

| Backend | Endpoint compat | Notes |
|---|---|---|
| Prometheus | `/api/v1/query` + `/api/v1/query_range` | Reference implementation. |
| Mimir | Prometheus-compatible | Set `extra_headers: { X-Scope-OrgID: <tenant> }`. |
| Thanos | Prometheus-compatible | Works against the Querier component. |
| VictoriaMetrics | Prometheus-compatible | `vmselect` endpoint. |
| Grafana Cloud Metrics | Prometheus-compatible | Auth via `bearer`. |
| Datadog (Prom-compat) | Prometheus-compatible | Use Datadog's Prom-compat endpoint, not the native API. |
| Loki | `/loki/api/v1/query` + `/loki/api/v1/query_range` | Reference implementation. |
| Grafana Cloud Logs | Loki-compatible | Auth via `bearer`. |

## Config reference

```yaml
- name: prom-prod          # required; the source's identity in logs + dashboard
  type: otel               # required
  description: "..."       # optional; surfaces on the Semantics dashboard tab
  otel:
    backend: prometheus    # required: prometheus | loki
    url: http://...        # required
    auth:                  # optional; default = no auth
      kind: bearer
      env: PROMETHEUS_BEARER
    extra_headers:         # optional; for tenant headers etc.
      X-Scope-OrgID: tenant-a
    max_result_rows: 50000     # optional; default 50_000
    default_window: 15m        # optional; humantime — default 15 min
    default_step: 30s          # optional; default 30 s
    max_window: 24h            # optional; default 24 h
    request_timeout: 10s       # optional; default 10 s
```

### Credentials

Inline `token:` / `password:` / `username:` are rejected at config
load with a clear error pointing operators at the `env:` form.
Three auth modes:

```yaml
auth: { kind: none }                                    # default
auth: { kind: bearer, env: PROMETHEUS_BEARER }          # Authorization: Bearer <env>
auth: { kind: basic, username_env: U, password_env: P } # Authorization: Basic …
```

`env`-named variables are read at server startup. Missing or empty
values fail registration with a typed `MissingCredential` error
naming both the source and the variable.

## Schema reference

Both surfaces have a fixed schema (see
[design.md Decision 6](../../openspec/changes/add-otel-data-source/design.md#decision-6) —
fixed source-layer schema enables predicate pushdown).

### `metrics` (Prometheus-backed)

| Column | Type | Notes |
|---|---|---|
| `name` | `Utf8` | Metric name (`__name__` label promoted). |
| `labels` | `Map<Utf8, Utf8>` | Remaining labels for this series. |
| `ts` | `Timestamp(Millisecond, UTC)` | Sample timestamp. |
| `value` | `Float64` | Sample value. Parses `NaN`, `+Inf`, `-Inf`. |

### `logs` (Loki-backed)

| Column | Type | Notes |
|---|---|---|
| `ts` | `Timestamp(Nanosecond, UTC)` | Log entry timestamp. |
| `labels` | `Map<Utf8, Utf8>` | Loki stream labels. |
| `line` | `Utf8` | Log line. For matrix responses (metric-style LogQL like `rate({...}[5m])`), this is the value-as-string. |
| `stream` | `Utf8` | Stable rendered selector (e.g. `{app="checkout"}`). `"matrix"` for metric-style responses. |

## Predicate pushdown matrix (v1)

The translator at
[`crates/skardi/src/sources/providers/otel/translator.rs`](../../crates/skardi/src/sources/providers/otel/translator.rs)
recognizes the following SQL shapes. Anything outside this matrix
surfaces as `OtelUnsupportedPushdown` with a clear hint pointing at the
escape hatch.

### `metrics`

| SQL | Translation | Endpoint |
|---|---|---|
| `WHERE name = '<metric>'` **(REQUIRED)** | selector `<metric>{}` | both |
| `WHERE ts BETWEEN <start> AND <end>` | `start=`, `end=`, `step=<default>` | `/api/v1/query_range` |
| `WHERE ts >= <start> AND ts <= <end>` | same | `/api/v1/query_range` |
| `WHERE ts > <start>` (no upper) | `start=<v>`, `end=now`, `step=…` | `/api/v1/query_range` |
| `WHERE ts = <ts>` | `time=<ts>` | `/api/v1/query` |
| no `ts` predicate | `default_window` applied | `/api/v1/query_range` |
| `LIMIT n` | client-side cap | both |

### `logs`

| SQL | Translation |
|---|---|
| `WHERE ts BETWEEN <start> AND <end>` | `start=`, `end=` |
| `WHERE line LIKE '%<substr>%'` | LogQL `\|= "<substr>"` |
| `WHERE line LIKE 'foo%'` | LogQL `\|~ "^foo.*$"` |
| `WHERE line NOT LIKE '%<substr>%'` | LogQL `!= "<substr>"` |
| `LIMIT n` | LogQL `limit=n` |

## When to reach for the escape hatch

Use the parametric `prom_query` / `prom_range` / `loki_query` /
`loki_range` table functions whenever:

- You need a PromQL operator with no SQL analogue: `rate(…)`,
  `irate(…)`, `histogram_quantile(…)`, `topk(…)`, range-vector
  selectors. The translator rejects these with
  `OtelUnsupportedPushdown` rather than producing wrong answers.
- You need LogQL pipeline stages: `| json`, `| line_format`,
  `| unwrap`, metric-style aggregations like `rate({...}[5m])`.
- A `labels['k']` matcher is required (v1 translator doesn't yet
  recognize them — see "v1 sharp edges" below).

The escape-hatch functions return the same fixed schema as the
`metrics` / `logs` tables, so downstream SQL (joins, projections,
filtering) is identical regardless of which surface produced the rows.

## v1 sharp edges

1. **`labels['k']` matchers aren't pushed down yet.** Tasks 3.5.2 and
   3.5.3 of the change. Until they land, any `SELECT FROM logs` with
   a label-key predicate hits `UnsupportedPushdown`; `SELECT FROM
   metrics WHERE labels['service']='X'` does too. Reach for
   `loki_range('{app="X"} …', …)` and `prom_query('…{service="X"}')`
   respectively.
2. **Aggregations on `metrics` happen at the DataFusion level**, not in
   PromQL. For counter metrics, `SELECT SUM(value) FROM metrics …
   GROUP BY labels['service']` sums raw counter samples — not a
   per-second rate. Use `prom_query('sum by(service)(rate(…[5m]))')`
   for counter-style aggregation. Task 4.2 closes this gap when it
   lands. See [design.md Decision 4](../../openspec/changes/add-otel-data-source/design.md#decision-4) "v1 sharp edge".
3. **Pipeline param substitution has rough edges for timestamps and
   string-literal-embedded params.** The v1 substituter (a) replaces
   `{param}` with `NULL` at plan-time validation, breaking
   `INTERVAL {window}`-style usage, and (b) wraps JSON string values
   in single quotes, producing nested quotes when `{param}` appears
   inside a SQL string literal. The demo pipelines hardcode these
   values rather than parameterize them. Fixing the substituter is a
   separate piece of work (not part of this change).

## Guardrails

Every OTEL query is bounded by four caps configured per-source:

- **`max_result_rows`** (default 50 000): per-query row cap. Exceeding
  it returns `OtelResultTooLarge { source, cap }`.
- **`max_window`** (default 24h): caps the range a single query can
  span. Caller-supplied windows larger than this surface
  `OtelWindowTooLarge { source, requested, max }` *before* any
  upstream call.
- **`default_window`** (default 15m), **`default_step`** (default 30s):
  applied when the caller doesn't supply explicit time bounds.
- **`request_timeout`** (default 10s): per-HTTP-call timeout. Timeouts
  surface as `OtelTimeout { source, timeout }`.

Per-source **rate limiting is intentionally not part of v1**. For
strict-SLA upstreams, place a reverse-proxy rate limit in front of
Skardi.

## Read-only access

OTEL sources register as `AccessMode::ReadOnly`. The SQL validator
rejects `INSERT` / `UPDATE` / `DELETE` against `metrics` / `logs` at
config load (and per-pipeline). The dashboard renders OTEL sources
with a `read-only` badge.

## Observability of the OTEL provider itself

Two OpenTelemetry instruments are emitted per upstream query and
share whichever meter provider `skardi-server`'s `telemetry::init`
configures:

| Name | Type | Labels |
|---|---|---|
| `skardi_otel_queries_total` | counter | `source`, `backend`, `outcome` |
| `skardi_otel_query_duration_seconds` | histogram | `source`, `backend` |

`outcome` is one of `success`, `upstream_error`, `parse_error`,
`transport_error`, `timeout`, `result_too_large`, `window_too_large`,
`missing_credential`, `unsupported_pushdown`, `other`. The label
strings are pinned by unit tests so dashboards relying on them won't
silently break on refactor.

## Two worked examples

### Tier-1 — plain SQL against the `metrics` table

```yaml
# pipelines/service_health.yaml
kind: pipeline
metadata:
  name: "service-health"
spec:
  query: |
    SELECT name, labels, ts, value
    FROM metrics
    WHERE name = 'http_requests_total'
    ORDER BY ts DESC
    LIMIT 1000
```

Caller: `POST /service-health/execute` with body `{}`. Behind the
scenes: filters lower to `/api/v1/query_range?query=http_requests_total{}&start=…&end=…&step=30`,
the response's `matrix` shape becomes one row per `(series, sample)`,
DataFusion's `ORDER BY` + `LIMIT` apply on top. No PromQL in the
pipeline file. The agent sees a stable JSON shape with `name`, `labels`
(as a JSON object), `ts`, and `value`.

### Tier-3 — escape hatch via `prom_query`

```yaml
# pipelines/latency_p99_by_route.yaml
kind: pipeline
metadata:
  name: "latency-p99-by-route"
spec:
  query: |
    SELECT name, labels, ts, value
    FROM prom_query(
      'histogram_quantile(0.99, sum by(route, le) (rate(http_request_duration_seconds_bucket[5m])))'
    )
    ORDER BY value DESC
    LIMIT 50
```

Caller: `POST /latency-p99-by-route/execute` with body `{}`. Behind
the scenes: `prom_query` ships the literal string to
`/api/v1/query`, the vector response becomes one row per series with
`labels` carrying `route` and `value` carrying the quantile estimate.
PromQL stays inside the pipeline file — the agent's API contract is
purely the JSON response shape.

## Operator validation during source bring-up

`skardi sql` works against OTEL sources too once the CLI is built with
the otel feature:

```bash
cargo build --bin skardi --features otel
./target/debug/skardi sql --ctx ctx.yaml \
    "SELECT * FROM prom_query('up') LIMIT 5"
```

This is intended as a development-time sanity check, not the agent
contract. For agent integration, use the rendered pipeline endpoints.
