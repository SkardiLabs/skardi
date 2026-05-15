## Why

Coding agents and general AI agents struggle with graph-shaped metrics and structured logs as they live in Grafana, Prometheus, Loki, Tempo, and Datadog: each backend speaks its own query language (PromQL, LogQL, TraceQL, Datadog query syntax), the responses are nested JSON tuned for dashboards rather than tabular analysis, and the time-series semantics (labels, exemplars, downsampling) aren't obvious from the surface API. Skardi already federates relational, document, lakehouse, and vector sources behind one SQL surface — adding OpenTelemetry-shaped data closes the last big gap so an agent that already knows `SELECT … WHERE … GROUP BY` can reason about service health the same way it reasons about Postgres rows or Parquet files, without needing a PromQL-trained subagent or a hand-rolled tool wrapper per metric.

## What Changes

- Add a new `Otel` variant to `DataSourceType` covering OpenTelemetry-shaped metrics and logs (traces deferred to a follow-up).
- Introduce `crates/skardi/src/sources/providers/otel/` with two read-only sub-providers:
  - **`prometheus`** — federated reader over a Prometheus-compatible HTTP API (Prometheus, Mimir, Thanos, VictoriaMetrics, Grafana Cloud Metrics, Datadog's Prometheus-compatible endpoint). Surfaces a first-class `metrics` SQL table with a fixed schema (`name`, `labels`, `ts`, `value`). A documented set of SQL predicates and group-bys is translated into PromQL behind the `TableProvider` so that the **default agent path is plain SQL**, not PromQL. `prom_query(...)` / `prom_range(...)` table functions stay as an **escape hatch** for queries that fall outside the supported translator (e.g. `rate()`, `histogram_quantile()`).
  - **`loki`** — federated reader over Grafana Loki's HTTP API. Surfaces a first-class `logs` SQL table with a fixed schema (`ts`, `labels`, `line`, `stream`); SQL `WHERE`/time-range predicates are translated into LogQL stream selectors + line filters. `loki_query(...)` / `loki_range(...)` remain as the escape hatch for full LogQL pipelines (`| json`, `| line_format`, metric-style aggregations).
- Implement a small **SQL → backend-query translator** in `otel/translator.rs` shared between Prometheus and Loki, with a documented matrix of supported predicate shapes (`name = '...'`, `labels['k'] = '...'`, `labels['k'] LIKE '...'`, time-range filters, `GROUP BY` over labels with `sum/avg/min/max/count`) and a typed `UnsupportedPushdown` error that names the failing predicate and points the caller at the escape-hatch function.
- Register both providers with the DataFusion engine so they participate in joins, predicate / projection / aggregate pushdown, and the standard SQL pipeline machinery used by every other Skardi source.
- Operators expose OTEL-derived data to agents **via pipeline files** (`kind: pipeline` YAML with `spec.query`), exactly as today's [demo/simple_backend/pipelines/](demo/simple_backend/pipelines/) and [demo/rag/server/pipelines/](demo/rag/server/pipelines/) examples do. There is no new authoring surface — declaring `type: otel` in `ctx.yaml` makes the `metrics` / `logs` tables available to any pipeline's `query` field, where they participate in joins, parameter substitution, and the existing pipeline → REST endpoint render path.
- Extend `ctx.yaml` source loader to accept `type: otel` entries with `backend: prometheus | loki` and connection options (URL, auth header, default time window, label allowlist).
- Add response-row caps, default + maximum time windows, and per-request timeouts so a single runaway agent query cannot fan out into an unbounded backend scrape.
- Document the new source in `docs/observability.md` (existing skardi-emitting-telemetry guide) plus a new `docs/otel/` directory mirroring the per-source doc convention.
- Ship example pipelines under `demo/` that expose canonical agent verbs: `service_health`, `top_error_logs`, `latency_p99_by_route`.
- **Out of scope (this change):** writing/ingesting OTLP into Skardi storage; Tempo/trace support; Datadog's native (non-Prometheus) query API; exemplars and metadata APIs; PromQL-aware SQL UDFs that wrap `rate()` / `histogram_quantile()` / `topk()` (callers reach for `prom_query` instead); SQL-side predicate pushdown for LogQL pipelines beyond stream-selector + simple line-substring filters. Each is queued as a follow-up.

## Capabilities

### New Capabilities
- `otel-source`: A read-only federated data source that exposes Prometheus-compatible metrics endpoints and Loki log endpoints as queryable SQL tables — `metrics` and `logs` — with predicate / time-range / `GROUP BY` pushdown into the backend HTTP API, plus `prom_query` / `prom_range` / `loki_query` / `loki_range` escape-hatch table functions for queries that fall outside the supported pushdown set. Includes credential handling, per-query row caps, default / maximum time-window enforcement, and per-request timeouts suitable for agent-driven workloads.

### Modified Capabilities
<!-- No existing spec files in openspec/specs/, so nothing to modify. -->

## Impact

- **Code**
  - `crates/skardi/src/sources/data_source_type.rs` — new `Otel` enum variant.
  - `crates/skardi/src/sources/providers/mod.rs` + new `otel/` module tree (`prometheus.rs`, `loki.rs`, shared `http.rs`, `time.rs`, `error.rs`, `translator.rs`).
  - `crates/skardi/src/sources/providers/otel/` predicate-pushdown `TableProvider`s for `metrics` and `logs`, plus `TableFunctionImpl`s for the `prom_query`, `prom_range`, `loki_query`, `loki_range` escape hatches.
  - `crates/server/src/config.rs` + `handlers.rs` — accept `type: otel` and surface registration errors.
  - `crates/cli/` — register the new source kind so `skardi sql` can query it locally.
- **APIs**
  - Pipelines (REST endpoints rendered from YAML) gain access to the new `metrics` / `logs` tables and the `prom_query` / `loki_query` family of escape-hatch table functions. Pipelines remain the canonical authoring + invocation surface for end-users; no breaking changes to existing endpoints, no new endpoint types, and no new YAML kinds.
- **Dependencies (new)**
  - `reqwest` (already optional; promote to default for the `otel` feature) for HTTP.
  - `prometheus-http-query` for typed Prometheus query/response handling.
  - `url`, `humantime-serde` for connection-config ergonomics (already in tree via transitive deps where possible).
- **Features**
  - New cargo feature `otel = ["dep:reqwest", "dep:prometheus-http-query"]` on the `skardi` crate (off by default); `skardi-server` exposes it through a matching feature.
- **Operational**
  - Outbound HTTP calls to user-configured Prometheus/Loki URLs. Credentials read from env (`PROMETHEUS_BEARER`, `LOKI_BEARER`) or AWS-style chain, never inlined in `ctx.yaml`.
  - Per-query row cap (default 50_000) and per-request timeout (default 10 s) bound the blast radius of any single query. A `max_window` (default 24 h) prevents oversized range scans before the upstream HTTP call.
- **Docs**
  - New `docs/otel/` directory and section in `docs/observability.md` clarifying the difference between *Skardi emitting* OTEL telemetry (existing) and *Skardi consuming* OTEL telemetry (this change).
