# Observability

`skardi-server` exports traces and metrics via [OpenTelemetry](https://opentelemetry.io/) (OTLP gRPC), giving you full visibility into query execution inside Grafana.

## What is Instrumented

- **Traces** — Every DataFusion execution plan node is wrapped with a span. You can see `elapsed_compute`, `output_rows`, spill counts, and optimizer rule timings per query.
- **Pipeline metrics** — Per-pipeline request count, latency, and error rate exported via OTLP on every `/execute` call.
- **Logs** — Structured logs forwarded to Loki via the OTel Collector.

## Pipeline Metrics

Every call to `/:name/execute` records two OTel metrics:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `pipeline_requests_total` | Counter | `pipeline`, `status` (`success`/`error`), `error_type` | Total requests by outcome |
| `pipeline_latency_ms` | Histogram | `pipeline` | End-to-end handler latency in milliseconds |

**Verify metrics are flowing** — the OTel Collector exposes a Prometheus scrape endpoint at `:8889`. After firing at least one request, run:

```bash
curl -s http://localhost:8889/metrics | grep pipeline
```

Expected output:
```
pipeline_latency_ms_bucket{pipeline="my-pipeline",le="..."} 2
pipeline_latency_ms_count{pipeline="my-pipeline"} 2
pipeline_latency_ms_sum{pipeline="my-pipeline"} 45.3
pipeline_requests_total{pipeline="my-pipeline",status="success"} 1
pipeline_requests_total{pipeline="my-pipeline",status="error",error_type="parameter_validation_error"} 1
```

> **Note:** The periodic exporter flushes every 60 seconds. If the output is empty, wait a moment and retry.

Example PromQL queries for Grafana dashboards:

```promql
# Requests per second per pipeline
rate(pipeline_requests_total[5m])

# Error rate (fraction of failing requests)
rate(pipeline_requests_total{status="error"}[5m])
  / rate(pipeline_requests_total[5m])

# p99 latency per pipeline
histogram_quantile(0.99, rate(pipeline_latency_ms_bucket[5m]))

# p50 latency per pipeline
histogram_quantile(0.50, rate(pipeline_latency_ms_bucket[5m]))
```

## Webhook Metrics

Every `/webhooks/:name` request records:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `webhook_requests_total` | Counter | `name`, `outcome` | Deliveries and handshakes by outcome |
| `webhook_rows_total` | Counter | `name`, `field` | Rows appended, per route field |
| `webhook_latency_ms` | Histogram | `name` | Handler latency (verification through commit) |

`outcome` is `ok`, `handshake_ok`, `handshake_rejected`, `signature_rejected`,
`payload_too_large`, `invalid_payload`, `conversion_error`, or `write_error`.
Requests for a webhook name that is not configured are not recorded: the name
is caller-controlled and would be an unbounded label. A sustained
`signature_rejected` rate means the app secret the server holds is not the
one Meta signs with; `write_error` means deliveries are being retried by Meta
and will keep arriving until the destination accepts them.

## Local Observability Stack

A `docker-compose.yml` is included with a pre-configured Grafana stack:

| Service | Role | Port |
|---------|------|------|
| OTel Collector | Receives OTLP from the server, fans out to backends | 4317 (gRPC), 4318 (HTTP) |
| Grafana Tempo | Trace storage | — |
| Prometheus | Metric storage (scrapes collector at :8889) | 9090 |
| Grafana Loki | Log storage | — |
| Grafana | Visualization (Tempo + Prometheus + Loki pre-provisioned) | 3000 |

```bash
# Start the stack
docker-compose -f observability/docker-compose.yml up -d

# Run the server pointing at the collector
OTLP_ENDPOINT=http://localhost:4317 RUST_LOG=debug cargo run -p skardi-server -- --port 8080
```

Then open Grafana at **http://localhost:3000** — all three datasources (Tempo, Prometheus, Loki) are pre-provisioned.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OTLP_ENDPOINT` | `http://localhost:4317` | OTLP gRPC endpoint to export to |
| `RUST_LOG` | `info` | Log level. Use `debug` to see per-query DataFusion span detail |

## Log Levels

- `RUST_LOG=info` — normal production operation; high-level server events only
- `RUST_LOG=debug` — shows per-query DataFusion execution spans (plan nodes, row counts, optimizer timing)
