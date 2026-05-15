# `otel_service_health` — Prometheus + Loki via Skardi pipelines

This demo wires Skardi's OTEL consumer into the bundled
`observability/` stack (Prometheus + Loki running in Docker) and
exposes three REST endpoints that an agent can call without ever
writing PromQL or LogQL.

## Layout

```
demo/otel_service_health/
├── ctx.yaml                            # declares prom + loki sources
├── pipelines/
│   ├── service_health.yaml             # tier-1 SQL against `metrics`
│   ├── top_error_logs.yaml             # tier-3 via loki_range (hardcoded selector)
│   └── latency_p99_by_route.yaml       # tier-3 via prom_query (rate + histogram_quantile)
└── README.md                           # you are here
```

## Run it

```bash
# 1. Boot Prometheus + Loki (from the repo root). The full
#    observability/docker-compose.yml stack works (`docker compose -f
#    observability/docker-compose.yml up -d`), or just the demo subset:
docker compose -f observability/docker-compose.yml up -d prometheus loki

# 2. Build skardi-server with the otel feature
cargo build --release --bin skardi-server --features otel

# 3. Run the server pointing at this demo
./target/release/skardi-server \
    --ctx demo/otel_service_health/ctx.yaml \
    --pipelines demo/otel_service_health/pipelines/

# 4. Call the rendered endpoints — all three pipelines take no
# parameters in v1 (see "v1 sharp edges" below).
curl -X POST http://localhost:3000/service-health/execute \
    -H 'Content-Type: application/json' -d '{}'

curl -X POST http://localhost:3000/top-error-logs/execute \
    -H 'Content-Type: application/json' -d '{}'

curl -X POST http://localhost:3000/latency-p99-by-route/execute \
    -H 'Content-Type: application/json' -d '{}'
```

You can also issue ad-hoc SQL through the CLI:

```bash
cargo run --bin skardi --features otel -- sql \
    --ctx demo/otel_service_health/ctx.yaml \
    "SELECT name, COUNT(*) FROM metrics WHERE name='up' AND ts > NOW() - INTERVAL '5 minutes' GROUP BY name"
```

## v1 sharp edges

These are documented in `design.md` Decision 4 and `docs/otel/README.md`,
but worth flagging here because they shape what the pipelines look like:

1. **`labels['k']` matchers aren't pushed down yet** (task 3.5.2 / 3.5.3).
   That's why `top_error_logs.yaml` uses `loki_range` instead of
   `SELECT * FROM logs WHERE labels['app']='checkout'`. Once those
   tasks land, the pipeline rewrites to plain SQL without changing the
   pipeline endpoint contract.
2. **Aggregations on `metrics` happen at the DataFusion level, not in
   PromQL** (task 4.2). For counter metrics, summing raw samples is
   not the same thing as `rate(...)`. Use `prom_query('rate(...)')`
   for rate / `histogram_quantile` semantics — `latency_p99_by_route.yaml`
   is the worked example.
3. **`type: otel` requires `--features otel` on both `skardi-server` and
   `skardi` (the CLI binary)**. The default builds don't include OTEL.
4. **None of the demo pipelines take parameters.** The v1 pipeline
   loader validates SQL by substituting `NULL` for each `{param}` —
   which breaks plan-time validation for the timestamp/INTERVAL
   shapes a `metrics` query needs, and string-quotes JSON values
   when they appear inside SQL string literals (producing nested
   quotes inside `prom_query('…[5m]…')`). For v1 the demo hardcodes
   time windows and selectors; operators copy the pipeline file and
   edit. Parameterization for time bounds requires a future fix to
   the pipeline substituter (separate from this change).
5. **`SELECT labels['k']` from `prom_query` / `loki_query` results
   currently tickles a DataFusion projection bug** ("Input field name
   does not match with the projection expression"). Workaround:
   `SELECT name, labels, ts, value` (no `labels[...]` access) and have
   the agent destructure the Map client-side. Tracked separately.

## What the pipelines demonstrate

| Pipeline | Tier | Why |
|----------|------|-----|
| `service_health` | Tier-1 (plain SQL on `metrics`) | Shows that an agent can SELECT from `metrics` with `name=` + `ts` filters and get raw counter samples without learning PromQL. Best for "what's the latest value per series" use cases. |
| `top_error_logs` | Tier-3 (`loki_range`) | Shows the escape-hatch pattern: a stable pipeline endpoint that internally embeds verbatim LogQL. Operator copies + customises the selector. |
| `latency_p99_by_route` | Tier-3 (`prom_query`) | Shows how to expose PromQL operators (`rate`, `histogram_quantile`) that have no native SQL equivalent, while still hiding the PromQL from the agent caller. |
