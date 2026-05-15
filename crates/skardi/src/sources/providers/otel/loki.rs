//! Loki backend.
//!
//! Mirrors the Prometheus provider structure (see [`super::prometheus`])
//! but against the Loki HTTP API. Three surfaces share a single
//! [`LokiExec`]:
//!
//! 1. **Tier-1 — `logs` table.** [`LokiLogsTable`] is a DataFusion
//!    `TableProvider` with the fixed `(ts, labels, line, stream)`
//!    schema. Its `scan` calls
//!    [`super::translator::translate_logs_filters`]. v1 recognizes
//!    `line LIKE/NOT LIKE`, `ts` range predicates, and `LIMIT` — label-key
//!    matchers (`labels['app']='X'`) land alongside 3.5.3 in a
//!    follow-up. Until they do, the v1 translator returns the empty
//!    selector `{}` and the table rejects empty-selector queries
//!    *before* issuing an upstream call (Loki itself errors on empty
//!    selectors and the message is confusing).
//! 2. **Tier-3 — `loki_query` / `loki_range`.** Escape-hatch
//!    `TableFunctionImpl`s accepting verbatim LogQL.
//! 3. **Registration helper.** [`register_loki_source`] wires both
//!    surfaces into a `SessionContext`.
//!
//! ## Response shapes
//!
//! Loki's `/loki/api/v1/query` and `/loki/api/v1/query_range` return
//! one of two `resultType` shapes:
//!
//! - **`streams`** — log lines. `values` is `[ts_nanoseconds_string, line]`
//!   pairs. We populate `(ts, labels, line, stream)` with one row per
//!   `(stream, value)` pair, where `labels` is the stream's label map
//!   and `stream` is a stable rendered selector (e.g.
//!   `{app="checkout"}`).
//! - **`matrix`** — only returned for metric-style LogQL like
//!   `rate({app="x"}[5m])`. Values are `[ts_seconds_f64, value_string]`
//!   like Prometheus. We pack the numeric value into the `line` column
//!   as a string and set `stream = "matrix"`, since the fixed schema
//!   has no `value` column. Agents wanting metric semantics from logs
//!   typically run `prom_query` against the Cortex/Mimir derived from
//!   the same stack, so this branch is mostly a safety net.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use arrow::array::{MapBuilder, RecordBatch, StringBuilder, TimestampNanosecondBuilder};
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
use super::translator::{LokiQuerySpec, translate_logs_filters};

/// `logs` table name inside the DataFusion session.
pub const LOGS_TABLE: &str = "logs";

// ---------------------------------------------------------------------------
// JSON response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct LokiResponse {
    status: String,
    #[serde(default)]
    data: Option<LokiData>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default, rename = "errorType")]
    error_type: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "resultType", rename_all = "camelCase")]
enum LokiData {
    Streams { result: Vec<LokiStreamSeries> },
    Matrix { result: Vec<LokiMatrixSeries> },
}

#[derive(Debug, Deserialize)]
struct LokiStreamSeries {
    stream: BTreeMap<String, String>,
    /// `[timestamp_nanoseconds_as_string, log_line]`. The timestamp is
    /// a string because it's an i64-precision ns value that doesn't
    /// fit safely in a JSON number.
    values: Vec<(String, String)>,
}

#[derive(Debug, Deserialize)]
struct LokiMatrixSeries {
    metric: BTreeMap<String, String>,
    /// `[timestamp_seconds_f64, value_as_string]` — same shape as
    /// Prometheus.
    values: Vec<(f64, String)>,
}

// ---------------------------------------------------------------------------
// LokiHttpClient
// ---------------------------------------------------------------------------

/// Backend-specific wrapper over [`OtelHttpClient`]. Records the
/// shared `skardi_otel_queries_total` counter +
/// `skardi_otel_query_duration_seconds` histogram on every call
/// (task 7.3).
#[derive(Debug, Clone)]
pub struct LokiHttpClient {
    inner: OtelHttpClient,
    metrics: OtelMetrics,
}

impl LokiHttpClient {
    pub fn new(inner: OtelHttpClient) -> Self {
        let metrics = OtelMetrics::new(inner.source_name(), "loki");
        Self { inner, metrics }
    }

    fn source_name(&self) -> &str {
        self.inner.source_name()
    }

    /// `GET /loki/api/v1/query?query=<logql>&time=<ts_ns_string>`.
    async fn query_instant(
        &self,
        logql: &str,
        time: Option<DateTime<Utc>>,
        limit: Option<usize>,
    ) -> Result<LokiData, OtelError> {
        let time_str;
        let limit_str;
        let mut query: Vec<(&str, &str)> = vec![("query", logql)];
        if let Some(t) = time {
            // Loki accepts RFC3339 or nanoseconds as a string.
            time_str = format!(
                "{}",
                (t.timestamp() as i128 * 1_000_000_000 + t.timestamp_subsec_nanos() as i128)
            );
            query.push(("time", &time_str));
        }
        if let Some(n) = limit {
            limit_str = n.to_string();
            query.push(("limit", &limit_str));
        }
        self.run("/loki/api/v1/query", &query).await
    }

    /// `GET /loki/api/v1/query_range?query=<logql>&start=<ns>&end=<ns>&limit=<n>`.
    async fn query_range(
        &self,
        logql: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: Option<usize>,
    ) -> Result<LokiData, OtelError> {
        let start_s = format!(
            "{}",
            start.timestamp() as i128 * 1_000_000_000 + start.timestamp_subsec_nanos() as i128
        );
        let end_s = format!(
            "{}",
            end.timestamp() as i128 * 1_000_000_000 + end.timestamp_subsec_nanos() as i128
        );
        let limit_str;
        let mut query: Vec<(&str, &str)> = vec![
            ("query", logql),
            ("start", start_s.as_str()),
            ("end", end_s.as_str()),
        ];
        if let Some(n) = limit {
            limit_str = n.to_string();
            query.push(("limit", &limit_str));
        }
        self.run("/loki/api/v1/query_range", &query).await
    }

    async fn run(&self, path: &str, query: &[(&str, &str)]) -> Result<LokiData, OtelError> {
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

    async fn run_inner(&self, path: &str, query: &[(&str, &str)]) -> Result<LokiData, OtelError> {
        let resp = self.inner.get(path, query).await?;
        let status = resp.status();
        let body = resp.text().await.map_err(|e| OtelError::Backend {
            source_name: self.source_name().to_string(),
            message: format!("failed to read response body: {e}"),
        })?;

        let parsed: LokiResponse = serde_json::from_str(&body).map_err(|e| OtelError::Parse {
            source_name: self.source_name().to_string(),
            message: format!(
                "failed to parse Loki response (HTTP {status}): {e}; \
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
            message: "Loki response was status=success but had no data field".to_string(),
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

/// Fixed Arrow schema for the `logs` table and the
/// `loki_query`/`loki_range` escape-hatch functions — see `design.md`
/// Decision 6.
pub fn logs_schema() -> SchemaRef {
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
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("labels", labels_type, false),
        Field::new("line", DataType::Utf8, true),
        Field::new("stream", DataType::Utf8, false),
    ]))
}

/// Render a Loki label map as a stable selector string like
/// `{app="checkout",level="info"}`. Used to populate the `stream`
/// column with a deterministic, comparable per-row identifier.
fn render_stream(labels: &BTreeMap<String, String>) -> String {
    let mut out = String::from("{");
    let mut first = true;
    for (k, v) in labels {
        if !first {
            out.push(',');
        }
        first = false;
        out.push_str(k);
        out.push('=');
        out.push('"');
        // Escape `"` and `\` inside the value.
        for ch in v.chars() {
            if ch == '"' || ch == '\\' {
                out.push('\\');
            }
            out.push(ch);
        }
        out.push('"');
    }
    out.push('}');
    out
}

fn build_record_batch(
    data: &LokiData,
    source_name: &str,
    max_rows: usize,
) -> Result<RecordBatch, OtelError> {
    let schema = logs_schema();
    let mut ts_b = TimestampNanosecondBuilder::new().with_timezone("UTC");
    let mut labels_b = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    let mut line_b = StringBuilder::new();
    let mut stream_b = StringBuilder::new();

    let mut row_count: usize = 0;
    let mut row = |ts_ns: i64,
                   labels: &BTreeMap<String, String>,
                   line: &str,
                   stream: &str|
     -> Result<(), OtelError> {
        if row_count + 1 > max_rows {
            return Err(OtelError::ResultTooLarge {
                source_name: source_name.to_string(),
                cap: max_rows,
            });
        }
        ts_b.append_value(ts_ns);
        for (k, v) in labels {
            labels_b.keys().append_value(k);
            labels_b.values().append_value(v);
        }
        labels_b.append(true).map_err(|e| OtelError::Backend {
            source_name: source_name.to_string(),
            message: format!("failed to finalize labels map row: {e}"),
        })?;
        line_b.append_value(line);
        stream_b.append_value(stream);
        row_count += 1;
        Ok(())
    };

    match data {
        LokiData::Streams { result } => {
            for series in result {
                let stream_str = render_stream(&series.stream);
                for (ts_str, line) in &series.values {
                    let ts_ns = ts_str.parse::<i64>().map_err(|_| OtelError::Parse {
                        source_name: source_name.to_string(),
                        message: format!("non-numeric Loki timestamp `{ts_str}`"),
                    })?;
                    row(ts_ns, &series.stream, line, &stream_str)?;
                }
            }
        }
        LokiData::Matrix { result } => {
            // Metric-style log query (e.g. `rate({...}[5m])`). Pack the
            // value into `line` as a string since the fixed schema has
            // no float column.
            for series in result {
                for (ts_secs, value_str) in &series.values {
                    let ts_ns = (ts_secs * 1e9) as i64;
                    row(ts_ns, &series.metric, value_str, "matrix")?;
                }
            }
        }
    }

    let ts_arr = ts_b.finish();
    let labels_arr = labels_b.finish();
    let line_arr = line_b.finish();
    let stream_arr = stream_b.finish();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ts_arr),
            Arc::new(labels_arr),
            Arc::new(line_arr),
            Arc::new(stream_arr),
        ],
    )
    .map_err(|e| OtelError::Backend {
        source_name: source_name.to_string(),
        message: format!("failed to build RecordBatch: {e}"),
    })
}

// ---------------------------------------------------------------------------
// LokiExec — leaf ExecutionPlan
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum LokiExecSpec {
    /// Tier-1 translated query: stream selector + line filters → LogQL
    /// string + time window from `LokiQuerySpec`.
    Translated(LokiQuerySpec),
    /// Tier-3 instant: `loki_query('<logql>')`.
    InstantString {
        logql: String,
        time: Option<DateTime<Utc>>,
        limit: Option<usize>,
    },
    /// Tier-3 range: `loki_range('<logql>', start, end, limit)`.
    RangeString {
        logql: String,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: Option<usize>,
    },
}

struct LokiExec {
    client: LokiHttpClient,
    spec: LokiExecSpec,
    /// Output schema — equal to [`logs_schema`] when no projection was
    /// pushed down, or the projected subset otherwise. Must match the
    /// emitted `RecordBatch` exactly (see equivalent comment in
    /// [`super::prometheus::PromExec`]).
    schema: SchemaRef,
    /// Indices into [`logs_schema`] selected by the planner's projection
    /// pushdown. `None` means "all columns, in declared order".
    projection: Option<Vec<usize>>,
    properties: PlanProperties,
    max_rows: usize,
}

impl LokiExec {
    fn new(
        client: LokiHttpClient,
        spec: LokiExecSpec,
        max_rows: usize,
        projection: Option<Vec<usize>>,
    ) -> DFResult<Self> {
        let full_schema = logs_schema();
        let schema: SchemaRef = match &projection {
            Some(indices) => Arc::new(full_schema.project(indices)?),
            None => full_schema,
        };
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Ok(Self {
            client,
            spec,
            schema,
            projection,
            properties,
            max_rows,
        })
    }
}

impl fmt::Debug for LokiExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LokiExec")
            .field("source", &self.client.source_name())
            .field("spec", &self.spec)
            .field("max_rows", &self.max_rows)
            .finish()
    }
}

impl DisplayAs for LokiExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LokiExec(source={}, max_rows={})",
            self.client.source_name(),
            self.max_rows
        )
    }
}

impl ExecutionPlan for LokiExec {
    fn name(&self) -> &str {
        "LokiExec"
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
        let projection = self.projection.clone();
        let source_name = client.source_name().to_string();

        let fut = async move {
            let data = match &spec {
                LokiExecSpec::Translated(loki_spec) => {
                    let logql = render_logql(loki_spec);
                    client
                        .query_range(
                            &logql,
                            loki_spec.window.start,
                            loki_spec.window.end,
                            loki_spec.limit,
                        )
                        .await
                }
                LokiExecSpec::InstantString { logql, time, limit } => {
                    client.query_instant(logql, *time, *limit).await
                }
                LokiExecSpec::RangeString {
                    logql,
                    start,
                    end,
                    limit,
                } => client.query_range(logql, *start, *end, *limit).await,
            };
            let data = data.map_err(DataFusionError::from)?;
            let batch =
                build_record_batch(&data, &source_name, max_rows).map_err(DataFusionError::from)?;
            // See `PromExec::execute` for the rationale — projection
            // pushdown obliges us to emit only the requested columns.
            let batch = match &projection {
                Some(indices) => batch.project(indices)?,
                None => batch,
            };
            Ok::<_, DataFusionError>(batch)
        };

        let stream = stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

/// Render `LokiQuerySpec` back to a LogQL string: `<selector> <filters>`.
fn render_logql(spec: &LokiQuerySpec) -> String {
    if spec.line_filters.is_empty() {
        spec.selector.clone()
    } else {
        let mut out = spec.selector.clone();
        for lf in &spec.line_filters {
            out.push(' ');
            out.push_str(&lf.to_logql());
        }
        out
    }
}

// ---------------------------------------------------------------------------
// 5.1 / 5.2 LokiLogsTable — tier-1 TableProvider
// ---------------------------------------------------------------------------

/// Tier-1 `logs` table. v1 hard-rejects any scan whose translator
/// output has an empty selector — the v1 translator only emits an
/// empty selector when no stream-label predicate was recognized, and
/// Loki's API rejects empty selectors with confusing errors. We
/// short-circuit here and point the caller at the escape hatch.
#[derive(Clone)]
pub struct LokiLogsTable {
    client: LokiHttpClient,
    config: Arc<OtelSourceConfig>,
    source_name: String,
    schema: SchemaRef,
}

impl LokiLogsTable {
    pub fn new(client: LokiHttpClient, config: Arc<OtelSourceConfig>, source_name: String) -> Self {
        Self {
            client,
            config,
            source_name,
            schema: logs_schema(),
        }
    }
}

impl fmt::Debug for LokiLogsTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LokiLogsTable")
            .field("source", &self.source_name)
            .finish()
    }
}

#[async_trait]
impl TableProvider for LokiLogsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Same rationale as `PromMetricsTable`: the translator in `scan()`
    /// is the single source of truth. Mark every predicate `Exact` and
    /// let unsupported shapes surface a clear error.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let spec = translate_logs_filters(
            filters,
            limit,
            &self.config.time_defaults(),
            &self.source_name,
        )
        .map_err(DataFusionError::from)?;

        if spec.selector == "{}" {
            // v1 translator can't yet recognize `labels['k']` matchers
            // (deferred under task 3.5.3). Reject before issuing an
            // upstream call so the error message is clear instead of
            // Loki returning a generic "stream selector required".
            return Err(DataFusionError::from(OtelError::UnsupportedPushdown {
                source_name: self.source_name.clone(),
                predicate: "logs query has no recognized stream-label predicate".to_string(),
                hint: "v1's `metrics`/`logs` translator does not yet recognize \
                       `labels['k']` predicates (deferred under task 3.5.3). Use \
                       loki_range('{app=\"...\"} |= \"...\"', start, end) instead — \
                       it accepts verbatim LogQL stream selectors."
                    .to_string(),
            }));
        }

        let max_rows = effective_max_rows(&self.config, limit);
        let exec = LokiExec::new(
            self.client.clone(),
            LokiExecSpec::Translated(spec),
            max_rows,
            projection.cloned(),
        )?;
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
// 5.3 / 5.4 — loki_query and loki_range escape-hatch table functions
// ---------------------------------------------------------------------------

/// `loki_query('<logql>')` — issues `/loki/api/v1/query`.
pub struct LokiQueryFunction {
    client: LokiHttpClient,
    config: Arc<OtelSourceConfig>,
}

impl fmt::Debug for LokiQueryFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LokiQueryFunction")
            .field("source", &self.client.source_name())
            .finish()
    }
}

impl LokiQueryFunction {
    pub fn new(client: LokiHttpClient, config: Arc<OtelSourceConfig>) -> Self {
        Self { client, config }
    }
}

impl TableFunctionImpl for LokiQueryFunction {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let logql = extract_string_arg(args, 0, "loki_query")?;
        Ok(Arc::new(LokiEscapeProvider {
            client: self.client.clone(),
            config: self.config.clone(),
            spec: LokiExecSpec::InstantString {
                logql,
                time: None,
                limit: None,
            },
        }))
    }
}

/// `loki_range('<logql>', start, end, limit?)`.
pub struct LokiRangeFunction {
    client: LokiHttpClient,
    config: Arc<OtelSourceConfig>,
}

impl fmt::Debug for LokiRangeFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LokiRangeFunction")
            .field("source", &self.client.source_name())
            .finish()
    }
}

impl LokiRangeFunction {
    pub fn new(client: LokiHttpClient, config: Arc<OtelSourceConfig>) -> Self {
        Self { client, config }
    }
}

impl TableFunctionImpl for LokiRangeFunction {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let logql = extract_string_arg(args, 0, "loki_range")?;
        let start_ts = args
            .get(1)
            .map(|e| extract_timestamp_arg(e, "loki_range", "start"))
            .transpose()?;
        let end_ts = args
            .get(2)
            .map(|e| extract_timestamp_arg(e, "loki_range", "end"))
            .transpose()?;
        let user_limit = match args.get(3) {
            Some(e) => Some(extract_usize_arg(e, "loki_range", "limit")?),
            None => None,
        };

        let window = super::time::resolve_window(
            start_ts,
            end_ts,
            None,
            &self.config.time_defaults(),
            self.client.source_name(),
        )
        .map_err(DataFusionError::from)?;

        Ok(Arc::new(LokiEscapeProvider {
            client: self.client.clone(),
            config: self.config.clone(),
            spec: LokiExecSpec::RangeString {
                logql,
                start: window.start,
                end: window.end,
                limit: user_limit,
            },
        }))
    }
}

struct LokiEscapeProvider {
    client: LokiHttpClient,
    config: Arc<OtelSourceConfig>,
    spec: LokiExecSpec,
}

impl fmt::Debug for LokiEscapeProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LokiEscapeProvider")
            .field("source", &self.client.source_name())
            .field("spec", &self.spec)
            .finish()
    }
}

#[async_trait]
impl TableProvider for LokiEscapeProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        logs_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let max_rows = effective_max_rows(&self.config, limit);
        Ok(Arc::new(LokiExec::new(
            self.client.clone(),
            self.spec.clone(),
            max_rows,
            projection.cloned(),
        )?))
    }
}

// ---------------------------------------------------------------------------
// Argument extraction helpers (mirror prometheus.rs)
// ---------------------------------------------------------------------------

fn extract_string_arg(args: &[Expr], idx: usize, fn_name: &str) -> DFResult<String> {
    let Some(arg) = args.get(idx) else {
        return plan_err!("{fn_name}: missing argument at position {idx}; expected a LogQL string");
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
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => {
            return DateTime::parse_from_rfc3339(s)
                .map(|dt| dt.with_timezone(&Utc))
                .map_err(|e| {
                    DataFusionError::Plan(format!(
                        "{fn_name}: `{pos}` is not a valid RFC-3339/ISO-8601 timestamp `{s}`: {e}"
                    ))
                });
        }
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

fn extract_usize_arg(arg: &Expr, fn_name: &str, pos: &str) -> DFResult<usize> {
    let Expr::Literal(scalar, _) = arg else {
        return plan_err!("{fn_name}: `{pos}` must be an integer literal, got `{arg}`");
    };
    let n = match scalar {
        ScalarValue::Int64(Some(v)) if *v >= 0 => *v as usize,
        ScalarValue::UInt64(Some(v)) => *v as usize,
        ScalarValue::Int32(Some(v)) if *v >= 0 => *v as usize,
        ScalarValue::UInt32(Some(v)) => *v as usize,
        other => {
            return plan_err!(
                "{fn_name}: `{pos}` must be a non-negative integer literal, got `{other:?}`"
            );
        }
    };
    Ok(n)
}

// ---------------------------------------------------------------------------
// 5.6 register_loki_source
// ---------------------------------------------------------------------------

/// Register a Loki-backed OTEL source: the `logs` table plus the
/// `loki_query` / `loki_range` UDTFs.
pub fn register_loki_source(
    ctx: &SessionContext,
    name: &str,
    config: &OtelSourceConfig,
    client: OtelHttpClient,
) -> Result<(), OtelError> {
    let loki_client = LokiHttpClient::new(client);
    let cfg = Arc::new(config.clone());

    let table = Arc::new(LokiLogsTable::new(
        loki_client.clone(),
        cfg.clone(),
        name.to_string(),
    ));
    ctx.register_table(LOGS_TABLE, table)
        .map_err(|e| OtelError::Backend {
            source_name: name.to_string(),
            message: format!("failed to register `{LOGS_TABLE}` table: {e}"),
        })?;

    ctx.register_udtf(
        "loki_query",
        Arc::new(LokiQueryFunction::new(loki_client.clone(), cfg.clone())),
    );
    ctx.register_udtf(
        "loki_range",
        Arc::new(LokiRangeFunction::new(loki_client, cfg)),
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
    fn logs_schema_has_expected_columns() {
        let schema = logs_schema();
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().clone()).collect();
        assert_eq!(names, vec!["ts", "labels", "line", "stream"]);
        assert!(matches!(
            schema.field(0).data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some(_))
        ));
        assert!(matches!(
            schema.field(1).data_type(),
            DataType::Map(_, false)
        ));
        assert!(matches!(schema.field(2).data_type(), DataType::Utf8));
        assert!(matches!(schema.field(3).data_type(), DataType::Utf8));
    }

    #[test]
    fn render_stream_produces_stable_selector_form() {
        let mut labels = BTreeMap::new();
        labels.insert("app".to_string(), "checkout".to_string());
        labels.insert("level".to_string(), "info".to_string());
        assert_eq!(render_stream(&labels), r#"{app="checkout",level="info"}"#);
    }

    #[test]
    fn render_stream_escapes_quotes_and_backslashes() {
        let mut labels = BTreeMap::new();
        labels.insert("path".to_string(), r#"C:\Program Files\"App""#.to_string());
        let rendered = render_stream(&labels);
        // Backslashes and quotes inside the value must be escaped.
        assert!(rendered.contains(r#"\\"#));
        assert!(rendered.contains(r#"\""#));
    }

    #[test]
    fn build_record_batch_from_streams_produces_one_row_per_value() {
        let data = LokiData::Streams {
            result: vec![LokiStreamSeries {
                stream: BTreeMap::from([
                    ("app".to_string(), "checkout".to_string()),
                    ("level".to_string(), "error".to_string()),
                ]),
                values: vec![
                    ("1700000000000000000".to_string(), "ouch".to_string()),
                    ("1700000000123456789".to_string(), "still ouch".to_string()),
                ],
            }],
        };
        let batch = build_record_batch(&data, "loki", 100).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);
    }

    #[test]
    fn build_record_batch_from_matrix_packs_value_into_line() {
        let data = LokiData::Matrix {
            result: vec![LokiMatrixSeries {
                metric: BTreeMap::from([("app".to_string(), "x".to_string())]),
                values: vec![(1_700_000_000.0, "0.5".to_string())],
            }],
        };
        let batch = build_record_batch(&data, "loki", 100).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let line_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(line_col.value(0), "0.5");
        let stream_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(stream_col.value(0), "matrix");
    }

    #[test]
    fn build_record_batch_enforces_row_cap() {
        let data = LokiData::Streams {
            result: vec![LokiStreamSeries {
                stream: BTreeMap::from([("app".to_string(), "x".to_string())]),
                values: (0..100)
                    .map(|i| {
                        (
                            format!("{}", 1_700_000_000_000_000_000_i64 + i),
                            "line".to_string(),
                        )
                    })
                    .collect(),
            }],
        };
        let err = build_record_batch(&data, "loki", 3).unwrap_err();
        match err {
            OtelError::ResultTooLarge { source_name, cap } => {
                assert_eq!(source_name, "loki");
                assert_eq!(cap, 3);
            }
            other => panic!("expected ResultTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn build_record_batch_rejects_non_numeric_timestamp() {
        let data = LokiData::Streams {
            result: vec![LokiStreamSeries {
                stream: BTreeMap::new(),
                values: vec![("not-a-number".to_string(), "x".to_string())],
            }],
        };
        let err = build_record_batch(&data, "loki", 100).unwrap_err();
        match err {
            OtelError::Parse {
                source_name,
                message,
            } => {
                assert_eq!(source_name, "loki");
                assert!(message.contains("not-a-number"), "got: {message}");
            }
            other => panic!("expected Parse, got {other:?}"),
        }
    }
}
