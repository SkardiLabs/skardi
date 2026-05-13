//! Prometheus-compatible backend.
//!
//! Three surfaces, all backed by the same execution-plan core
//! ([`PromExec`]):
//!
//! 1. **Tier-1 — `metrics` table.** [`PromMetricsTable`] is a
//!    DataFusion `TableProvider` with a fixed `(name, labels, ts, value)`
//!    schema. Its `scan` calls
//!    [`super::translator::translate_metrics_filters`] to fold the
//!    incoming `WHERE` predicates into a single backend HTTP call. v1
//!    supports `name='<metric>'` (required), `ts` ranges, and `LIMIT`;
//!    label-key matchers and `GROUP BY` aggregate pushdown are tracked
//!    under section 3.5.2 in
//!    `openspec/changes/add-otel-data-source/tasks.md`.
//! 2. **Tier-3 — `prom_query(promql)` / `prom_range(promql, …)`.**
//!    Escape-hatch `TableFunctionImpl`s that accept verbatim PromQL
//!    strings. Output the same fixed schema as `metrics`.
//! 3. **Registration helper.** [`register_prometheus_source`] wires
//!    both surfaces into a `SessionContext` under names scoped to the
//!    `ctx.yaml` `data_sources[].name` field.
//!
//! ## HTTP layer
//!
//! All outbound calls go through [`super::http::OtelHttpClient`] so
//! auth, `extra_headers`, and per-request timeout are handled in one
//! place (and a future rate-limiter can drop in there too — see
//! `design.md`). JSON parsing is hand-rolled against the Prometheus
//! [HTTP API response shape] rather than going through
//! `prometheus-http-query`, because the latter constructs its own
//! `reqwest::Client` and would bypass our auth/timeout configuration.
//!
//! [HTTP API response shape]: https://prometheus.io/docs/prometheus/latest/querying/api/

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use arrow::array::{
    Float64Builder, MapBuilder, RecordBatch, StringBuilder, TimestampMillisecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{DataFusionError, ScalarValue, plan_err};
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::SessionContext;
use futures::stream;
use serde::Deserialize;

use super::OtelSourceConfig;
use super::error::OtelError;
use super::http::OtelHttpClient;
use super::metrics::{OtelMetrics, Outcome};
use super::translator::{PromQuerySpec, QueryKind, translate_metrics_filters};

// ---------------------------------------------------------------------------
// Constants — column / endpoint names
// ---------------------------------------------------------------------------

/// Reserved label key the Prometheus API uses for the metric name.
/// Stripped from the per-row `labels` map and promoted into the `name`
/// column.
const NAME_LABEL: &str = "__name__";

/// `metrics` table name inside the DataFusion session, scoped per source.
/// Multiple OTEL sources currently coexist via per-source qualified
/// names (e.g. `prom.metrics`). Single-source-per-process is the
/// recommended v1 deployment shape (see tasks.md 4.6).
pub const METRICS_TABLE: &str = "metrics";

// ---------------------------------------------------------------------------
// Prometheus HTTP response types
// ---------------------------------------------------------------------------

/// Top-level Prometheus HTTP API response envelope.
#[derive(Debug, Deserialize)]
struct PromResponse {
    status: String,
    #[serde(default)]
    data: Option<PromData>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default, rename = "errorType")]
    error_type: Option<String>,
}

/// `data` block when `status == "success"`. Tagged on `resultType`.
#[derive(Debug, Deserialize)]
#[serde(tag = "resultType", rename_all = "camelCase")]
enum PromData {
    Vector {
        result: Vec<VectorSeries>,
    },
    Matrix {
        result: Vec<MatrixSeries>,
    },
    Scalar {
        result: PromSample,
    },
    /// Returned by `query('"literal"')`. We pass it through as a single
    /// row with `name=""` and `value=NaN`; agents will usually never
    /// hit this branch.
    String {
        result: (f64, String),
    },
}

#[derive(Debug, Deserialize)]
struct VectorSeries {
    metric: BTreeMap<String, String>,
    value: PromSample,
}

#[derive(Debug, Deserialize)]
struct MatrixSeries {
    metric: BTreeMap<String, String>,
    values: Vec<PromSample>,
}

/// Prometheus encodes a sample as `[<timestamp_seconds_f64>, "<value_string>"]`
/// — the value is a string so the wire preserves full f64 precision
/// (including `NaN`, `+Inf`, `-Inf`).
type PromSample = (f64, String);

// ---------------------------------------------------------------------------
// Prometheus HTTP client wrapper
// ---------------------------------------------------------------------------

/// Backend-specific wrapper over [`OtelHttpClient`]. Owns the JSON
/// parsing and translates an HTTP error response into [`OtelError`]
/// variants that downstream callers can match on. Also records the
/// `skardi_otel_queries_total` counter + `skardi_otel_query_duration_seconds`
/// histogram on every call (task 7.3).
#[derive(Debug, Clone)]
pub struct PromHttpClient {
    inner: OtelHttpClient,
    metrics: OtelMetrics,
}

impl PromHttpClient {
    pub fn new(inner: OtelHttpClient) -> Self {
        let metrics = OtelMetrics::new(inner.source_name(), "prometheus");
        Self { inner, metrics }
    }

    fn source_name(&self) -> &str {
        self.inner.source_name()
    }

    /// `GET /api/v1/query?query=<promql>&time=<unix_ts>`.
    async fn query_instant(
        &self,
        promql: &str,
        time: Option<DateTime<Utc>>,
    ) -> Result<PromData, OtelError> {
        let time_str;
        let mut query: Vec<(&str, &str)> = vec![("query", promql)];
        if let Some(t) = time {
            time_str = format!("{}", t.timestamp_millis() as f64 / 1000.0);
            query.push(("time", &time_str));
        }
        self.run("/api/v1/query", &query).await
    }

    /// `GET /api/v1/query_range?query=<promql>&start=<ts>&end=<ts>&step=<sec>`.
    async fn query_range(
        &self,
        promql: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        step: std::time::Duration,
    ) -> Result<PromData, OtelError> {
        let start_s = format!("{}", start.timestamp_millis() as f64 / 1000.0);
        let end_s = format!("{}", end.timestamp_millis() as f64 / 1000.0);
        let step_s = format!("{}", step.as_secs_f64());
        let query = [
            ("query", promql),
            ("start", start_s.as_str()),
            ("end", end_s.as_str()),
            ("step", step_s.as_str()),
        ];
        self.run("/api/v1/query_range", &query).await
    }

    async fn run(&self, path: &str, query: &[(&str, &str)]) -> Result<PromData, OtelError> {
        let started = std::time::Instant::now();
        let result = self.run_inner(path, query).await;
        let elapsed_secs = started.elapsed().as_secs_f64();
        let outcome = match &result {
            Ok(_) => Outcome::Success,
            Err(e) => Outcome::from_error(e),
        };
        self.metrics.record(outcome, elapsed_secs);
        result
    }

    async fn run_inner(&self, path: &str, query: &[(&str, &str)]) -> Result<PromData, OtelError> {
        let resp = self.inner.get(path, query).await?;
        let status = resp.status();
        let body = resp.text().await.map_err(|e| OtelError::Backend {
            source_name: self.source_name().to_string(),
            message: format!("failed to read response body: {e}"),
        })?;

        let parsed: PromResponse = serde_json::from_str(&body).map_err(|e| OtelError::Parse {
            source_name: self.source_name().to_string(),
            message: format!(
                "failed to parse Prometheus response (HTTP {status}): {e}; \
                     body (truncated): {}",
                truncate(&body, 200)
            ),
        })?;

        if parsed.status != "success" {
            let etype = parsed.error_type.unwrap_or_default();
            let err = parsed.error.unwrap_or_default();
            return Err(OtelError::UpstreamStatus {
                source_name: self.source_name().to_string(),
                status: status.as_u16(),
                body: format!("{etype}: {err}"),
            });
        }
        if !status.is_success() {
            return Err(OtelError::UpstreamStatus {
                source_name: self.source_name().to_string(),
                status: status.as_u16(),
                body: truncate(&body, 200),
            });
        }
        parsed.data.ok_or_else(|| OtelError::Parse {
            source_name: self.source_name().to_string(),
            message: "Prometheus response was status=success but had no data field".to_string(),
        })
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let head: String = s.chars().take(max).collect();
        format!("{head}... [truncated]")
    }
}

// ---------------------------------------------------------------------------
// Arrow schema + batch builder
// ---------------------------------------------------------------------------

/// Fixed Arrow schema returned by every Prometheus surface — see
/// `design.md` Decision 6.
pub fn metrics_schema() -> SchemaRef {
    let entry_field = Field::new(
        "entries",
        DataType::Struct(
            vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Utf8, true),
            ]
            .into(),
        ),
        false,
    );
    let labels_type = DataType::Map(Arc::new(entry_field), false);

    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("labels", labels_type, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("value", DataType::Float64, true),
    ]))
}

/// Build a `RecordBatch` from a parsed Prometheus response, applying
/// `max_rows` as a hard cap (task 4.5). Returns
/// [`OtelError::ResultTooLarge`] once the cap is exceeded.
fn build_record_batch(
    data: &PromData,
    source_name: &str,
    max_rows: usize,
) -> Result<RecordBatch, OtelError> {
    let schema = metrics_schema();
    let mut name_b = StringBuilder::new();
    let mut labels_b = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    let mut ts_b = TimestampMillisecondBuilder::new().with_timezone("UTC");
    let mut value_b = Float64Builder::new();

    let mut row_count: usize = 0;
    let mut append = |name: &str,
                      labels: &BTreeMap<String, String>,
                      sample: &PromSample|
     -> Result<(), OtelError> {
        if row_count + 1 > max_rows {
            return Err(OtelError::ResultTooLarge {
                source_name: source_name.to_string(),
                cap: max_rows,
            });
        }
        let (ts_secs, value_str) = sample;
        name_b.append_value(name);
        for (k, v) in labels {
            if k == NAME_LABEL {
                continue;
            }
            labels_b.keys().append_value(k);
            labels_b.values().append_value(v);
        }
        labels_b.append(true).map_err(|e| OtelError::Backend {
            source_name: source_name.to_string(),
            message: format!("failed to finalize labels map row: {e}"),
        })?;
        let ts_ms = (ts_secs * 1000.0) as i64;
        ts_b.append_value(ts_ms);
        value_b.append_value(parse_prom_value(value_str));
        row_count += 1;
        Ok(())
    };

    match data {
        PromData::Vector { result } => {
            for series in result {
                let metric_name = series.metric.get(NAME_LABEL).cloned().unwrap_or_default();
                append(&metric_name, &series.metric, &series.value)?;
            }
        }
        PromData::Matrix { result } => {
            for series in result {
                let metric_name = series.metric.get(NAME_LABEL).cloned().unwrap_or_default();
                for sample in &series.values {
                    append(&metric_name, &series.metric, sample)?;
                }
            }
        }
        PromData::Scalar { result } => {
            append("", &BTreeMap::new(), result)?;
        }
        PromData::String { result } => {
            // Surface as a single zero-value row; agents almost never
            // reach this branch and the only natural mapping is
            // "timestamp + value-as-NaN".
            let (ts, _s) = result;
            append("", &BTreeMap::new(), &(*ts, "NaN".to_string()))?;
        }
    }

    let name_arr = name_b.finish();
    let labels_arr = labels_b.finish();
    let ts_arr = ts_b.finish();
    let value_arr = value_b.finish();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(name_arr),
            Arc::new(labels_arr),
            Arc::new(ts_arr),
            Arc::new(value_arr),
        ],
    )
    .map_err(|e| OtelError::Backend {
        source_name: source_name.to_string(),
        message: format!("failed to build RecordBatch: {e}"),
    })
}

/// Prometheus encodes values as strings to preserve full precision —
/// notably "NaN", "+Inf", "-Inf". `parse::<f64>()` handles all three
/// natively in Rust.
fn parse_prom_value(s: &str) -> f64 {
    s.parse::<f64>().unwrap_or(f64::NAN)
}

// ---------------------------------------------------------------------------
// Render PromQuerySpec → PromQL string
// ---------------------------------------------------------------------------

/// v1 emits selectors with no label-key matchers since 3.5.2 does not
/// yet recognize them; section 4 will extend this once map_extract
/// Expr shapes are validated against the real planner.
fn render_promql(spec: &PromQuerySpec) -> String {
    spec.selector.clone()
}

// ---------------------------------------------------------------------------
// PromExec — leaf ExecutionPlan that calls Prometheus and yields one
// RecordBatch
// ---------------------------------------------------------------------------

/// Shared backing `ExecutionPlan` for both the `metrics` table and the
/// escape-hatch table functions. Owns enough state to issue exactly one
/// HTTP call when `execute` is called.
struct PromExec {
    client: PromHttpClient,
    spec: PromExecSpec,
    schema: SchemaRef,
    properties: PlanProperties,
    max_rows: usize,
}

#[derive(Debug, Clone)]
enum PromExecSpec {
    /// Tier-1: PromQuerySpec rendered to PromQL + time window.
    Translated(PromQuerySpec),
    /// Tier-3 instant: `prom_query('<promql>')`.
    InstantString { promql: String },
    /// Tier-3 range: `prom_range('<promql>', start, end, step)`.
    RangeString {
        promql: String,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        step: std::time::Duration,
    },
}

impl PromExec {
    fn new(client: PromHttpClient, spec: PromExecSpec, max_rows: usize) -> Self {
        let schema = metrics_schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            client,
            spec,
            schema,
            properties,
            max_rows,
        }
    }
}

impl fmt::Debug for PromExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PromExec")
            .field("source", &self.client.source_name())
            .field("spec", &self.spec)
            .field("max_rows", &self.max_rows)
            .finish()
    }
}

impl DisplayAs for PromExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PromExec(source={}, max_rows={})",
            self.client.source_name(),
            self.max_rows
        )
    }
}

impl ExecutionPlan for PromExec {
    fn name(&self) -> &str {
        "PromExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _ctx: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let client = self.client.clone();
        let spec = self.spec.clone();
        let max_rows = self.max_rows;
        let source_name = client.source_name().to_string();

        let fut = async move {
            let data = match &spec {
                PromExecSpec::Translated(prom_spec) => {
                    let promql = render_promql(prom_spec);
                    match prom_spec.query_kind {
                        QueryKind::Instant { time } => {
                            client.query_instant(&promql, Some(time)).await
                        }
                        QueryKind::Range => {
                            client
                                .query_range(
                                    &promql,
                                    prom_spec.window.start,
                                    prom_spec.window.end,
                                    prom_spec.window.step,
                                )
                                .await
                        }
                    }
                }
                PromExecSpec::InstantString { promql } => client.query_instant(promql, None).await,
                PromExecSpec::RangeString {
                    promql,
                    start,
                    end,
                    step,
                } => client.query_range(promql, *start, *end, *step).await,
            };
            let data = data.map_err(DataFusionError::from)?;
            let batch =
                build_record_batch(&data, &source_name, max_rows).map_err(DataFusionError::from)?;
            Ok::<_, DataFusionError>(batch)
        };

        let stream = stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

// ---------------------------------------------------------------------------
// 4.1 PromMetricsTable — tier-1 TableProvider with translator-driven scan
// ---------------------------------------------------------------------------

/// Tier-1 `metrics` table. Implements DataFusion's `TableProvider`
/// trait and delegates predicate pushdown to
/// [`super::translator::translate_metrics_filters`].
#[derive(Clone)]
pub struct PromMetricsTable {
    client: PromHttpClient,
    config: Arc<OtelSourceConfig>,
    source_name: String,
    schema: SchemaRef,
}

impl PromMetricsTable {
    pub fn new(client: PromHttpClient, config: Arc<OtelSourceConfig>, source_name: String) -> Self {
        Self {
            client,
            config,
            source_name,
            schema: metrics_schema(),
        }
    }
}

impl fmt::Debug for PromMetricsTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PromMetricsTable")
            .field("source", &self.source_name)
            .finish()
    }
}

#[async_trait]
impl TableProvider for PromMetricsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Claim every predicate is pushed down `Exact`ly; the translator
    /// in `scan()` is the single source of truth and will surface
    /// `OtelError::UnsupportedPushdown` (which propagates back to the
    /// caller as a DataFusion error) if any predicate falls outside
    /// the supported matrix. Mirroring the translator's decisions
    /// per-filter is hard because the translator validates the
    /// *combined* filter set (`name=` is required across the whole
    /// `WHERE` clause, not on any single predicate in isolation).
    /// Per design.md Decision 4, fast-fail on unsupported predicates
    /// is the intended behavior — we deliberately don't want
    /// DataFusion silently fetching everything and filtering on top.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let spec = translate_metrics_filters(
            filters,
            None,
            None,
            limit,
            &self.config.time_defaults(),
            &self.source_name,
        )
        .map_err(DataFusionError::from)?;

        let max_rows = effective_max_rows(&self.config, limit);
        let exec = PromExec::new(
            self.client.clone(),
            PromExecSpec::Translated(spec),
            max_rows,
        );
        Ok(Arc::new(exec))
    }
}

fn effective_max_rows(config: &OtelSourceConfig, limit: Option<usize>) -> usize {
    let cap = config.max_result_rows_or_default();
    match limit {
        Some(n) => n.min(cap),
        None => cap,
    }
}

// ---------------------------------------------------------------------------
// 4.3 / 4.4 — prom_query and prom_range escape-hatch table functions
// ---------------------------------------------------------------------------

/// `prom_query('<promql>')` — issues `/api/v1/query`.
pub struct PromQueryFunction {
    client: PromHttpClient,
    config: Arc<OtelSourceConfig>,
}

impl fmt::Debug for PromQueryFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PromQueryFunction")
            .field("source", &self.client.source_name())
            .finish()
    }
}

impl PromQueryFunction {
    pub fn new(client: PromHttpClient, config: Arc<OtelSourceConfig>) -> Self {
        Self { client, config }
    }
}

impl TableFunctionImpl for PromQueryFunction {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let promql = extract_string_arg(args, 0, "prom_query")?;
        Ok(Arc::new(PromEscapeProvider {
            client: self.client.clone(),
            config: self.config.clone(),
            spec: PromExecSpec::InstantString { promql },
        }))
    }
}

/// `prom_range('<promql>', start, end, step)` — issues
/// `/api/v1/query_range`. Missing trailing arguments fall back to the
/// source's configured defaults via [`super::time::resolve_window`].
pub struct PromRangeFunction {
    client: PromHttpClient,
    config: Arc<OtelSourceConfig>,
}

impl fmt::Debug for PromRangeFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PromRangeFunction")
            .field("source", &self.client.source_name())
            .finish()
    }
}

impl PromRangeFunction {
    pub fn new(client: PromHttpClient, config: Arc<OtelSourceConfig>) -> Self {
        Self { client, config }
    }
}

impl TableFunctionImpl for PromRangeFunction {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let promql = extract_string_arg(args, 0, "prom_range")?;
        let start_ts = args
            .get(1)
            .map(|e| extract_timestamp_arg(e, "prom_range", "start"))
            .transpose()?;
        let end_ts = args
            .get(2)
            .map(|e| extract_timestamp_arg(e, "prom_range", "end"))
            .transpose()?;
        let step = args
            .get(3)
            .map(|e| extract_duration_arg(e, "prom_range", "step"))
            .transpose()?;

        let window = super::time::resolve_window(
            start_ts,
            end_ts,
            step,
            &self.config.time_defaults(),
            self.client.source_name(),
        )
        .map_err(DataFusionError::from)?;

        Ok(Arc::new(PromEscapeProvider {
            client: self.client.clone(),
            config: self.config.clone(),
            spec: PromExecSpec::RangeString {
                promql,
                start: window.start,
                end: window.end,
                step: window.step,
            },
        }))
    }
}

/// Glue `TableProvider` returned by [`PromQueryFunction`] /
/// [`PromRangeFunction`]: pure schema + a single `scan` that returns a
/// `PromExec` with a pre-built spec.
struct PromEscapeProvider {
    client: PromHttpClient,
    config: Arc<OtelSourceConfig>,
    spec: PromExecSpec,
}

impl fmt::Debug for PromEscapeProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PromEscapeProvider")
            .field("source", &self.client.source_name())
            .field("spec", &self.spec)
            .finish()
    }
}

#[async_trait]
impl TableProvider for PromEscapeProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        metrics_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let max_rows = effective_max_rows(&self.config, limit);
        Ok(Arc::new(PromExec::new(
            self.client.clone(),
            self.spec.clone(),
            max_rows,
        )))
    }
}

// ---------------------------------------------------------------------------
// Argument extraction helpers for the escape-hatch table functions
// ---------------------------------------------------------------------------

fn extract_string_arg(args: &[Expr], idx: usize, fn_name: &str) -> DFResult<String> {
    let Some(arg) = args.get(idx) else {
        return plan_err!(
            "{fn_name}: missing argument at position {idx}; expected a PromQL string"
        );
    };
    match arg {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(s)), _) => Ok(s.clone()),
        other => plan_err!(
            "{fn_name}: argument at position {idx} must be a string literal, got `{other}`"
        ),
    }
}

fn extract_timestamp_arg(arg: &Expr, fn_name: &str, pos: &str) -> DFResult<DateTime<Utc>> {
    let Expr::Literal(scalar, _) = arg else {
        return plan_err!(
            "{fn_name}: `{pos}` must be a timestamp literal or ISO-8601 string, got `{arg}`"
        );
    };
    let nanos = match scalar {
        ScalarValue::TimestampNanosecond(Some(v), _) => *v,
        ScalarValue::TimestampMicrosecond(Some(v), _) => v.saturating_mul(1_000),
        ScalarValue::TimestampMillisecond(Some(v), _) => v.saturating_mul(1_000_000),
        ScalarValue::TimestampSecond(Some(v), _) => v.saturating_mul(1_000_000_000),
        // String shapes — accept ISO-8601 / RFC-3339 so pipelines can
        // pass timestamps as JSON-friendly strings (the pipeline
        // substituter doesn't emit `TIMESTAMP '…'` casts).
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => {
            return parse_iso8601(s, fn_name, pos);
        }
        // Null is what the pipeline load-time validator substitutes for
        // un-bound params. Treat as a sentinel that downstream defaults
        // (via `time::resolve_window`) will replace; surface a plan
        // error here only if the caller really did pass an unsupported
        // value at request time.
        ScalarValue::Null => {
            return plan_err!(
                "{fn_name}: `{pos}` is NULL — make sure the pipeline parameter \
                 is supplied at request time as an ISO-8601 timestamp string"
            );
        }
        other => {
            return plan_err!(
                "{fn_name}: `{pos}` must be a timestamp literal or ISO-8601 string, got `{other:?}`"
            );
        }
    };
    DateTime::<Utc>::from_timestamp(
        nanos.div_euclid(1_000_000_000),
        nanos.rem_euclid(1_000_000_000) as u32,
    )
    .ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{fn_name}: `{pos}` timestamp is out of chrono range"
        ))
    })
}

fn parse_iso8601(s: &str, fn_name: &str, pos: &str) -> DFResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| {
            DataFusionError::Plan(format!(
                "{fn_name}: `{pos}` is not a valid RFC-3339/ISO-8601 timestamp `{s}`: {e}"
            ))
        })
}

fn extract_duration_arg(arg: &Expr, fn_name: &str, pos: &str) -> DFResult<std::time::Duration> {
    let Expr::Literal(scalar, _) = arg else {
        return plan_err!("{fn_name}: `{pos}` must be an INTERVAL literal, got `{arg}`");
    };
    let nanos = match scalar {
        ScalarValue::DurationNanosecond(Some(v)) => *v,
        ScalarValue::DurationMicrosecond(Some(v)) => v.saturating_mul(1_000),
        ScalarValue::DurationMillisecond(Some(v)) => v.saturating_mul(1_000_000),
        ScalarValue::DurationSecond(Some(v)) => v.saturating_mul(1_000_000_000),
        ScalarValue::IntervalDayTime(Some(dt)) => {
            // INTERVAL '5 minutes' typically lowers to IntervalDayTime
            // — milliseconds portion + days.
            (dt.days as i64).saturating_mul(86_400 * 1_000_000_000)
                + (dt.milliseconds as i64).saturating_mul(1_000_000)
        }
        ScalarValue::IntervalMonthDayNano(Some(idn)) => {
            // Months portion ignored — pure month durations don't have
            // a fixed wall-clock length; flag explicitly.
            if idn.months != 0 {
                return plan_err!(
                    "{fn_name}: `{pos}` cannot use month/year components — PromQL \
                     range step requires a fixed-length duration"
                );
            }
            (idn.days as i64).saturating_mul(86_400 * 1_000_000_000) + idn.nanoseconds
        }
        other => {
            return plan_err!("{fn_name}: `{pos}` must be an INTERVAL literal, got `{other:?}`");
        }
    };
    if nanos <= 0 {
        return plan_err!("{fn_name}: `{pos}` must be a positive duration");
    }
    Ok(std::time::Duration::from_nanos(nanos as u64))
}

// ---------------------------------------------------------------------------
// 4.6 register_prometheus_source — wire the table + functions into a
// SessionContext
// ---------------------------------------------------------------------------

/// Register a Prometheus-backed OTEL source. Adds:
/// - the `metrics` table (qualified as `<source>.metrics` if the
///   ctx has multiple OTEL sources),
/// - the `prom_query` UDTF (currently global — single-source-per-process
///   is the recommended v1 deployment shape per `tasks.md` 4.6),
/// - the `prom_range` UDTF.
pub fn register_prometheus_source(
    ctx: &SessionContext,
    name: &str,
    config: &OtelSourceConfig,
    client: OtelHttpClient,
) -> Result<(), OtelError> {
    let prom_client = PromHttpClient::new(client);
    let cfg = Arc::new(config.clone());

    // Register the `metrics` table. Per design.md Decision 4 / tasks.md
    // 4.6, v1 recommends single-source-per-process so the bare `metrics`
    // identifier resolves unambiguously. Multiple OTEL sources will
    // collide on the second registration — when that's a real
    // operational need, follow up by promoting to qualified names
    // (e.g. registering a per-source schema provider).
    let table = Arc::new(PromMetricsTable::new(
        prom_client.clone(),
        cfg.clone(),
        name.to_string(),
    ));
    ctx.register_table(METRICS_TABLE, table)
        .map_err(|e| OtelError::Backend {
            source_name: name.to_string(),
            message: format!("failed to register `{METRICS_TABLE}` table: {e}"),
        })?;

    ctx.register_udtf(
        "prom_query",
        Arc::new(PromQueryFunction::new(prom_client.clone(), cfg.clone())),
    );
    ctx.register_udtf(
        "prom_range",
        Arc::new(PromRangeFunction::new(prom_client, cfg)),
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    #[test]
    fn metrics_schema_has_expected_columns() {
        let schema = metrics_schema();
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().clone()).collect();
        assert_eq!(names, vec!["name", "labels", "ts", "value"]);
        assert!(matches!(schema.field(0).data_type(), DataType::Utf8));
        assert!(matches!(
            schema.field(1).data_type(),
            DataType::Map(_, false)
        ));
        assert!(matches!(
            schema.field(2).data_type(),
            DataType::Timestamp(TimeUnit::Millisecond, Some(_))
        ));
        assert!(matches!(schema.field(3).data_type(), DataType::Float64));
    }

    #[test]
    fn parse_prom_value_handles_nan_and_inf() {
        assert!(parse_prom_value("NaN").is_nan());
        assert!(parse_prom_value("+Inf").is_infinite());
        assert!(parse_prom_value("-Inf").is_infinite());
        assert_eq!(parse_prom_value("1.5"), 1.5);
        assert!(parse_prom_value("garbage").is_nan());
    }

    #[test]
    fn build_record_batch_from_vector_produces_one_row_per_series() {
        let data = PromData::Vector {
            result: vec![
                VectorSeries {
                    metric: BTreeMap::from([
                        (NAME_LABEL.to_string(), "up".to_string()),
                        ("job".to_string(), "prom".to_string()),
                    ]),
                    value: (1_700_000_000.5, "1".to_string()),
                },
                VectorSeries {
                    metric: BTreeMap::from([(NAME_LABEL.to_string(), "up".to_string())]),
                    value: (1_700_000_000.5, "0".to_string()),
                },
            ],
        };
        let batch = build_record_batch(&data, "prom", 100).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);
    }

    #[test]
    fn build_record_batch_from_matrix_produces_one_row_per_sample() {
        let data = PromData::Matrix {
            result: vec![MatrixSeries {
                metric: BTreeMap::from([(NAME_LABEL.to_string(), "up".to_string())]),
                values: vec![
                    (1_700_000_000.0, "1".to_string()),
                    (1_700_000_060.0, "1".to_string()),
                    (1_700_000_120.0, "0".to_string()),
                ],
            }],
        };
        let batch = build_record_batch(&data, "prom", 100).unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn build_record_batch_strips_name_label_from_labels_map() {
        let data = PromData::Vector {
            result: vec![VectorSeries {
                metric: BTreeMap::from([
                    (NAME_LABEL.to_string(), "up".to_string()),
                    ("job".to_string(), "prom".to_string()),
                ]),
                value: (1_700_000_000.0, "1".to_string()),
            }],
        };
        let batch = build_record_batch(&data, "prom", 100).unwrap();
        // The labels column should contain only "job" (no `__name__`).
        let labels = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::MapArray>()
            .unwrap();
        let entries = labels.entries();
        assert_eq!(entries.len(), 1, "exactly one key/value pair expected");
    }

    #[test]
    fn build_record_batch_enforces_row_cap() {
        let data = PromData::Matrix {
            result: vec![MatrixSeries {
                metric: BTreeMap::from([(NAME_LABEL.to_string(), "up".to_string())]),
                values: (0..1000).map(|i| (i as f64, "1".to_string())).collect(),
            }],
        };
        let err = build_record_batch(&data, "prom", 5).unwrap_err();
        match err {
            OtelError::ResultTooLarge { source_name, cap } => {
                assert_eq!(source_name, "prom");
                assert_eq!(cap, 5);
            }
            other => panic!("expected ResultTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn effective_max_rows_uses_smaller_of_config_and_limit() {
        let cfg = OtelSourceConfig {
            backend: super::super::OtelBackend::Prometheus,
            url: url::Url::parse("http://x").unwrap(),
            auth: Default::default(),
            extra_headers: Default::default(),
            max_result_rows: Some(100),
            default_window: None,
            default_step: None,
            max_window: None,
            request_timeout: None,
        };
        assert_eq!(effective_max_rows(&cfg, None), 100);
        assert_eq!(effective_max_rows(&cfg, Some(50)), 50);
        assert_eq!(effective_max_rows(&cfg, Some(200)), 100);
    }
}
