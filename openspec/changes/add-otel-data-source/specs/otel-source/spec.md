## ADDED Requirements

### Requirement: OTEL data source type registration

The system SHALL recognize `otel` as a first-class data source type alongside the existing `csv`, `parquet`, `postgres`, `mysql`, `sqlite`, `iceberg`, `mongo`, `redis`, `lance`, and `seekdb` types, with backend selection (`prometheus` or `loki`) supplied via a required `backend` field on the source configuration.

#### Scenario: ctx.yaml declares a Prometheus-backed OTEL source

- **WHEN** a `ctx.yaml` source entry sets `type: otel`, `backend: prometheus`, and a valid `url`
- **THEN** the server SHALL load the source, register a predicate-pushdown `metrics` `TableProvider` with the fixed schema `(name: Utf8, labels: Map<Utf8,Utf8>, ts: Timestamp(Millisecond, UTC), value: Float64)` and the `prom_query` / `prom_range` escape-hatch table functions in the DataFusion session scoped to that source name, and report the source as healthy in the dashboard's Semantics tab

#### Scenario: ctx.yaml declares a Loki-backed OTEL source

- **WHEN** a `ctx.yaml` source entry sets `type: otel`, `backend: loki`, and a valid `url`
- **THEN** the server SHALL register a predicate-pushdown `logs` `TableProvider` with the fixed schema `(ts: Timestamp(Nanosecond, UTC), labels: Map<Utf8,Utf8>, line: Utf8, stream: Utf8)` and the `loki_query` / `loki_range` escape-hatch table functions in the DataFusion session scoped to that source name

#### Scenario: ctx.yaml omits the backend field

- **WHEN** a `ctx.yaml` source entry sets `type: otel` without a `backend` field
- **THEN** the server SHALL refuse to start and SHALL emit an error message that names the source and lists the supported backends (`prometheus`, `loki`)

#### Scenario: ctx.yaml uses an unsupported backend value

- **WHEN** a `ctx.yaml` source entry sets `type: otel` with `backend: tempo` (or any value outside the supported set)
- **THEN** the server SHALL refuse to start and SHALL emit an error message naming the unsupported backend and listing the supported values

### Requirement: Prometheus-compatible metrics querying via SQL predicate pushdown

The system SHALL expose any Prometheus-compatible HTTP API (Prometheus, Mimir, Thanos, VictoriaMetrics, Grafana Cloud Metrics, Datadog Prometheus-compat) as a queryable SQL `metrics` table whose `TableProvider` translates a documented set of SQL predicate / projection / aggregate shapes into native PromQL HTTP calls, so that an agent without PromQL knowledge can answer common observability questions in plain SQL.

#### Scenario: filtered range scan via the metrics table

- **WHEN** an agent runs `SELECT name, labels, ts, value FROM metrics WHERE name='http_requests_total' AND labels['service']='api' AND ts BETWEEN NOW() - INTERVAL '1 hour' AND NOW()` against a Prometheus-backed OTEL source
- **THEN** the engine SHALL translate the predicates into a single call to `/api/v1/query_range` with `query=http_requests_total{service="api"}`, `start=` and `end=` derived from the `ts` bounds, and `step=<default_step>`, and SHALL return one row per `(series, sample)` pair using the fixed `(name, labels, ts, value)` schema

#### Scenario: instant query via the metrics table

- **WHEN** an agent runs `SELECT * FROM metrics WHERE name='up' AND ts = NOW()` against the Prometheus-backed source
- **THEN** the engine SHALL issue a single instant query to `/api/v1/query` with `query=up{}` and `time=<the supplied ts>`, returning one row per series

#### Scenario: label-regex pushdown via LIKE

- **WHEN** an agent runs `SELECT * FROM metrics WHERE name='http_requests_total' AND labels['route'] LIKE '/api/%'`
- **THEN** the engine SHALL emit the PromQL selector `http_requests_total{route=~"^/api/.*$"}`, with SQL `%` translated to regex `.*`, regex metacharacters in the literal portion escaped, and the pattern anchored with `^…$`

#### Scenario: GROUP BY aggregate pushdown

- **WHEN** an agent runs `SELECT labels['service'] AS service, SUM(value) AS total FROM metrics WHERE name='http_requests_total' AND ts > NOW() - INTERVAL '5 minutes' GROUP BY labels['service']`
- **THEN** the engine SHALL emit the PromQL `sum by(service)(http_requests_total{})` instead of fetching raw samples, issue an instant query, and return one row per group

#### Scenario: default time window when no ts predicate is supplied

- **WHEN** an agent runs `SELECT * FROM metrics WHERE name='up'` with no `ts` predicate
- **THEN** the engine SHALL apply the source's `default_window` (default: last 15 minutes) and `default_step` (default: 30 seconds) when issuing `/api/v1/query_range`

#### Scenario: unsupported predicate falls back with a pointer to prom_query

- **WHEN** an agent runs `SELECT * FROM metrics WHERE name='http_requests_total' AND value > 0.5` (or any other predicate outside the supported pushdown matrix)
- **THEN** the engine SHALL NOT issue any upstream call and SHALL return an `OtelUnsupportedPushdown` DataFusion error whose `Display` impl quotes the offending predicate (`value > 0.5`) and includes a `hint` containing the substring `prom_query(` showing an equivalent PromQL-string call (`prom_query('http_requests_total > 0.5')`)

#### Scenario: missing required name predicate refuses fan-out

- **WHEN** an agent runs `SELECT * FROM metrics` with no `name = '<metric>'` predicate
- **THEN** the engine SHALL return `OtelUnsupportedPushdown` with a hint that the `name` predicate is required, pointing to `prom_query('{__name__=~".+"}')` for callers who explicitly want a fan-out

### Requirement: Prometheus escape-hatch table functions

The system SHALL provide `prom_query` and `prom_range` table functions accepting verbatim PromQL strings, so agents can express queries that the `metrics`-table translator does not support (notably `rate()`, `histogram_quantile()`, `topk()`, range-vector operators).

#### Scenario: instant query via prom_query

- **WHEN** an agent runs `SELECT * FROM prom_query('rate(http_requests_total[5m])')` against a Prometheus-backed OTEL source
- **THEN** the engine SHALL issue an instant query to the source's `/api/v1/query` endpoint and return one row per series with the same fixed schema as the `metrics` table (`name`, `labels`, `ts`, `value`)

#### Scenario: range query via prom_range

- **WHEN** an agent runs `SELECT * FROM prom_range('rate(http_requests_total[5m])', NOW() - INTERVAL '1 hour', NOW(), INTERVAL '30 seconds')`
- **THEN** the engine SHALL issue a range query to `/api/v1/query_range` with `start`, `end`, and `step` derived from the SQL arguments and return one row per `(series, timestamp)` pair with a `value` per step

#### Scenario: PromQL syntax error surfaces as SQL error

- **WHEN** `prom_query('not valid promql {{')` is executed and the upstream returns HTTP 400 with a PromQL parse error
- **THEN** the engine SHALL surface the parser message as a DataFusion error whose `Display` impl includes the source name, the offending PromQL string (truncated to 200 chars), and the upstream error text

### Requirement: Loki log querying via SQL predicate pushdown

The system SHALL expose Loki's HTTP query API as a queryable SQL `logs` table whose `TableProvider` translates stream-label predicates, simple line filters, and time-range bounds into LogQL stream selectors and pipeline stages, so that the common "errors from service X in the last hour" question is plain SQL.

#### Scenario: stream-label + time-range scan

- **WHEN** an agent runs `SELECT * FROM logs WHERE labels['app']='checkout' AND ts > NOW() - INTERVAL '15 minutes'`
- **THEN** the engine SHALL issue `/loki/api/v1/query_range` with the LogQL selector `{app="checkout"}`, `start=` and `end=` derived from the `ts` bound, and SHALL return one row per log line with the fixed `(ts, labels, line, stream)` schema

#### Scenario: substring filter on log line via LIKE

- **WHEN** an agent runs `SELECT * FROM logs WHERE labels['app']='checkout' AND line LIKE '%error%' AND ts > NOW() - INTERVAL '15 minutes'`
- **THEN** the engine SHALL emit LogQL `{app="checkout"} |= "error"` (substring filter), forwarding `start` / `end` accordingly

#### Scenario: chained line filters

- **WHEN** an agent runs `SELECT * FROM logs WHERE labels['app']='checkout' AND line LIKE '%error%' AND line NOT LIKE '%retry%' AND ts > NOW() - INTERVAL '15 minutes'`
- **THEN** the engine SHALL emit `{app="checkout"} |= "error" != "retry"` with both line filters chained in declaration order

#### Scenario: missing stream-label predicate refuses fan-out

- **WHEN** an agent runs `SELECT * FROM logs WHERE line LIKE '%panic%'` with no stream-label equality on `labels['…']`
- **THEN** the engine SHALL return `OtelUnsupportedPushdown` whose `hint` contains the substring `loki_query(` and explains that at least one stream-label predicate is required

#### Scenario: unsupported LogQL pipeline falls back to loki_query

- **WHEN** an agent attempts a SQL shape that would require a LogQL pipeline beyond stream-selector + line filter (for example projecting a parsed JSON field with `SELECT json_extract(line, '$.status') FROM logs WHERE …`, or grouping over `ts_bucket`)
- **THEN** the engine SHALL either complete the scan correctly (treating any post-scan SQL projection as DataFusion-level computation over the raw `line` column) OR — if the predicate itself cannot be expressed — return `OtelUnsupportedPushdown` with a hint pointing the caller at `loki_query('{…} | json …')`

### Requirement: Loki escape-hatch table functions

The system SHALL provide `loki_query` and `loki_range` table functions accepting verbatim LogQL strings, so agents can express queries that the `logs`-table translator does not support (LogQL pipelines such as `| json`, `| line_format`, `| unwrap`, metric-style aggregations like `rate({...}[5m])`, and stream-selector regex shapes the translator omits).

#### Scenario: range query via loki_range

- **WHEN** an agent runs `SELECT * FROM loki_range('{app="checkout"} |= "error"', NOW() - INTERVAL '15 minutes', NOW())`
- **THEN** the engine SHALL call `/loki/api/v1/query_range` and return one row per log line with columns `ts` (Timestamp(Nanosecond, UTC)), `labels` (Map<UTF8, UTF8>), `line` (Utf8), and `stream` (Utf8)

#### Scenario: structured logs preserve parsed fields

- **WHEN** a Loki response includes structured metadata (e.g. via LogQL `| json` parsing) and the agent calls `loki_query('{app="checkout"} | json')`
- **THEN** the engine SHALL include the parsed key/value pairs in the `labels` column without flattening them into separate columns (since column count would otherwise be query-dependent)

#### Scenario: empty result set is not an error

- **WHEN** a Loki query matches zero log lines in the requested window
- **THEN** the engine SHALL return an empty record batch with the documented schema and SHALL NOT raise an error

### Requirement: Credential and connection handling

The system SHALL load credentials for OTEL backends from environment variables or process environment chains, SHALL NOT accept inline credentials in `ctx.yaml`, and SHALL refuse to start if a configured source declares an auth scheme whose required credential is missing.

#### Scenario: bearer token loaded from env

- **WHEN** an OTEL source declares `auth: { kind: bearer, env: PROMETHEUS_BEARER }` and the env var is set to a non-empty value
- **THEN** the HTTP client SHALL send `Authorization: Bearer <value>` on every request to that source

#### Scenario: inline secret rejected

- **WHEN** an OTEL source declares `auth: { kind: bearer, token: "abc123..." }` directly in `ctx.yaml`
- **THEN** the server SHALL refuse to start and SHALL emit an error stating that inline secrets are not permitted and pointing the operator to the `env:` form

#### Scenario: required env var missing

- **WHEN** an OTEL source declares `auth: { kind: bearer, env: PROMETHEUS_BEARER }` and `PROMETHEUS_BEARER` is unset or empty
- **THEN** the server SHALL refuse to start and SHALL emit an error naming both the source and the missing env var

#### Scenario: basic auth supported as a fallback

- **WHEN** an OTEL source declares `auth: { kind: basic, username_env: PROM_USER, password_env: PROM_PASS }` with both env vars set
- **THEN** the HTTP client SHALL send a `Basic` `Authorization` header derived from those env values

### Requirement: Per-query result-size and timeout guardrails

The system SHALL apply per-query response-size caps and per-request HTTP timeouts to bound the blast radius of any single agent-driven OTEL query, with safe defaults that the operator can tighten or relax via `ctx.yaml`.

#### Scenario: default row cap

- **WHEN** an OTEL source is declared without an explicit `max_result_rows`
- **THEN** the system SHALL cap each query's returned rows at 50_000 and SHALL surface an `OtelResultTooLarge` error when the cap is exceeded, with the message naming the source and the configured cap

#### Scenario: operator raises the row cap

- **WHEN** `ctx.yaml` sets `max_result_rows: 1_000_000` on a specific OTEL source
- **THEN** that source SHALL allow up to 1_000_000 rows per query while other sources keep their own caps

#### Scenario: default per-request timeout

- **WHEN** an OTEL source is declared without an explicit `request_timeout`
- **THEN** every outbound HTTP call from that source SHALL be subject to a 10-second timeout, and exceeding it SHALL surface as an `OtelTimeout` DataFusion error naming the source and the elapsed duration

### Requirement: Time-window defaulting for range queries

The system SHALL apply a configurable default time window and step to range queries when the SQL caller omits them, so that pipelines and ad-hoc agent queries do not fall back to "scrape all of history" by accident.

#### Scenario: prom_range called with only a query string

- **WHEN** an agent runs `SELECT * FROM prom_range('up')` with no explicit start/end/step
- **THEN** the engine SHALL substitute the source's configured `default_window` (default: last 15 minutes) and `default_step` (default: 30 seconds)

#### Scenario: caller-provided window overrides defaults

- **WHEN** the caller supplies explicit start, end, or step arguments
- **THEN** those values SHALL be honoured exactly and SHALL NOT be merged with the defaults

#### Scenario: window exceeds configured maximum

- **WHEN** a caller-supplied window is larger than the source's `max_window` (default: 24 hours)
- **THEN** the engine SHALL return an `OtelWindowTooLarge` error naming the source, the requested window, and the cap, and SHALL NOT issue the upstream query

### Requirement: Read-only access mode

The system SHALL register OTEL sources with read-only access mode in the existing `AccessMode` framework so that the SQL validator rejects writes and the dashboard surfaces the source as read-only.

#### Scenario: INSERT against an OTEL table is rejected

- **WHEN** an agent runs `INSERT INTO metrics VALUES (...)` against an OTEL source
- **THEN** the SQL validator SHALL reject the statement before it reaches the engine and SHALL return an error naming the source and stating that OTEL sources are read-only

#### Scenario: dashboard shows read-only badge

- **WHEN** the Semantics tab renders an OTEL source's card
- **THEN** the card SHALL display a "read-only" badge consistent with how other read-only providers (e.g. Iceberg in read-only mode) are presented

### Requirement: Federation with other Skardi sources

The system SHALL allow OTEL-derived tables and table functions to be joined with any other registered Skardi source so an agent can correlate telemetry with relational, document, or lakehouse data in a single SQL statement.

#### Scenario: joining Prometheus metrics with a Postgres dimension table via the metrics table

- **WHEN** an agent runs `SELECT m.value, s.team FROM metrics m JOIN services s ON m.labels['service'] = s.name WHERE m.name='up' AND m.ts > NOW() - INTERVAL '5 minutes'` where `services` is a Postgres-backed table
- **THEN** the engine SHALL push the metric/label/time predicates down to Prometheus, execute the Postgres scan in parallel, perform the join in DataFusion, and return the combined result — without the agent ever writing PromQL

#### Scenario: joining Loki logs with an Iceberg deployments table via the logs table

- **WHEN** an agent runs `SELECT l.line, d.version FROM logs l JOIN deployments d ON l.labels['app'] = d.app AND l.ts BETWEEN d.started_at AND COALESCE(d.ended_at, NOW()) WHERE l.labels['app']='checkout' AND l.line LIKE '%panic%' AND l.ts > NOW() - INTERVAL '1 hour'`
- **THEN** the engine SHALL push the stream selector + line filter + time bound down to Loki, execute the Iceberg scan, and perform the temporal join in DataFusion

#### Scenario: joining via the escape hatch still works

- **WHEN** an agent uses `prom_query('rate(http_requests_total[5m])')` (because `rate` is not pushdown-supported) and joins its result with a Postgres `services` table
- **THEN** the engine SHALL execute the PromQL call and the Postgres scan and perform the join in DataFusion — i.e. tier-1 and tier-3 results remain interchangeable inside larger SQL plans

### Requirement: Pipeline-first authoring surface

The system SHALL expose OTEL-derived data to end users through the existing pipeline mechanism (`kind: pipeline` YAML files with a `spec.query` SQL string, rendered into REST endpoints), with no new YAML kind, no new endpoint type, and no special-case authoring path. The OTEL source contributes tables and table functions to the same DataFusion `SessionContext` that backs every pipeline, so any pipeline's `query` field can reference `metrics` / `logs` / `prom_query` / `prom_range` / `loki_query` / `loki_range` exactly as it references any other source.

#### Scenario: pipeline file declares an OTEL-backed query

- **WHEN** an operator authors a `kind: pipeline` YAML file whose `spec.query` references the `metrics` or `logs` table (e.g. `SELECT labels['service'] AS service, SUM(value) AS request_rate FROM metrics WHERE name = 'http_requests_total' AND ts > NOW() - INTERVAL {window} GROUP BY labels['service']`)
- **THEN** the server SHALL render the pipeline into a REST endpoint at `POST /pipelines/<pipeline-name>` using the same pipeline machinery used for Postgres-backed and SQLite-backed pipelines today, with no OTEL-specific handler, response format, or wrapper

#### Scenario: pipeline parameter substitution composes with OTEL tables

- **WHEN** the pipeline body is invoked with `{"window": "1 hour", "service": "checkout"}` and its SQL contains `{window}` / `{service}` placeholders
- **THEN** the server SHALL substitute the parameters before SQL parse, the resulting SQL SHALL pushdown into the OTEL backend via the translator, and SHALL return the rendered rows as the pipeline response — i.e. no special escape rules apply for tier-1 OTEL pipelines

#### Scenario: pipeline embeds an escape-hatch PromQL string

- **WHEN** a pipeline's `spec.query` contains `SELECT * FROM prom_query('rate(http_requests_total[{rate_window}])')` with a `{rate_window}` parameter
- **THEN** the pipeline engine SHALL substitute `{rate_window}` inside the SQL string before parse, and the substituted PromQL braces (`http_requests_total{}`) SHALL NOT collide with the `{param}` template engine. (If the chosen template engine cannot leave non-parameter braces alone, the implementation SHALL escape them; this is the open-question wiring tracked under task 8.1.)

#### Scenario: agents call pipeline endpoints, not raw SQL

- **WHEN** an external agent client wants metric or log data from an OTEL source
- **THEN** the documented contract SHALL be to call the rendered pipeline endpoint (`POST /pipelines/<name>` with a JSON body of parameter values), and the agent SHALL NOT be expected to author SQL directly against `metrics` / `logs`. (Ad-hoc `skardi sql --ctx ctx.yaml '<sql>'` via the CLI remains available for operator validation during source bring-up but is NOT documented as an agent-facing surface.)

#### Scenario: pipeline file lives under the same directory layout as existing sources

- **WHEN** an operator adds an OTEL-backed pipeline to a project
- **THEN** the file SHALL live under a `pipelines/` directory next to the project's `ctx.yaml` (matching [demo/simple_backend/pipelines/](demo/simple_backend/pipelines/) and [demo/rag/server/pipelines/](demo/rag/server/pipelines/)), and the server's pipeline-discovery mechanism SHALL pick it up without OTEL-specific configuration

### Requirement: Documentation and discoverability

The system SHALL ship reference documentation under `docs/otel/` and update `docs/observability.md` to distinguish "Skardi emitting OTEL telemetry about itself" from "Skardi consuming external OTEL telemetry as a source", and SHALL ship at least one runnable example under `demo/` that exposes the new source via a pipeline.

#### Scenario: docs explain the two roles of OTEL

- **WHEN** a reader opens `docs/observability.md`
- **THEN** the page SHALL contain a section that points to `docs/otel/` for the consumer-side OTEL source and clarifies that the existing content covers Skardi's own telemetry emission

#### Scenario: demo pipeline returns service health

- **WHEN** an operator runs the new `demo/otel_service_health/` example with the bundled `docker-compose` (Prometheus + Loki)
- **THEN** invoking the rendered pipeline endpoint SHALL return a row per service with current request rate, error rate, and recent error log count
