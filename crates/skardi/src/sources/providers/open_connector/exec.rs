//! Physical execution for Open Connector scans.
//!
//! One [`OpenConnectorExec`] streams a table page by page: build the action
//! input (resource + translated filters + page parameters), execute, extract
//! rows at the fixed row path, convert to Arrow, then advance pagination.
//! LIMIT stops fetching as soon as it is satisfied, safety bounds fail the
//! scan rather than returning an incomplete result, and dropping the stream
//! stops further pages (cancellation).

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::Statistics;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream;
use serde_json::Value;

use super::cache::{ScanCache, ScanKeyParts, scan_cache_key, schema_fingerprint};
use super::client::OpenConnectorClient;
use super::error::OpenConnectorError;
use super::json_to_arrow::RowConverter;
use super::pagination::{Pagination, PaginationStrategy};
use super::row_path::RowPath;
use super::source_pack::{FixedValue, SourcePackTable};

/// The scanned collection's identity and pagination contract — the shape
/// shared by YAML-bound source-pack tables and `open_connector_scan` raw
/// actions, which have no static [`SourcePackTable`] to point at.
#[derive(Debug, Clone)]
pub struct ScanTarget {
    /// Stable table ID (`mock.items`) or a raw-action label, for errors and
    /// tracing.
    pub table_id: Arc<str>,
    /// Open Connector action to execute.
    pub action_id: Arc<str>,
    /// Pagination contract.
    pub pagination: PaginationStrategy,
    /// In-band provider-error location (see `SourcePackTable::error_path`);
    /// `None` for raw scans and packs whose providers error at HTTP level.
    pub error_path: Option<&'static str>,
    /// Fixed action inputs sent with every request (see
    /// [`SourcePackTable::fixed_inputs`]); empty for raw scans, whose whole
    /// input is caller-supplied.
    pub fixed_inputs: &'static [(&'static str, FixedValue)],
    /// Source-pack version, part of the cache key (0 for raw scans, which
    /// have no pack and bypass the cache).
    pub source_pack_version: u32,
}

impl ScanTarget {
    /// The target of a bound source-pack table.
    pub fn from_pack_table(table: &SourcePackTable, source_pack_version: u32) -> Self {
        Self {
            table_id: Arc::from(table.id),
            action_id: Arc::from(table.action_id),
            pagination: table.pagination,
            error_path: table.error_path,
            fixed_inputs: table.fixed_inputs,
            source_pack_version,
        }
    }
}

/// Everything a scan needs, bound once at planning time.
pub struct OpenConnectorExec {
    client: Arc<OpenConnectorClient>,
    cache: Option<Arc<ScanCache>>,
    gateway: String,
    /// Binding (catalog schema) name for tracing; `None` for UDTF scans.
    binding: Option<String>,
    connection_alias: Option<String>,
    target: ScanTarget,
    converter: Arc<RowConverter>,
    row_path: RowPath,
    resource: Value,
    filter_inputs: Vec<(String, Value)>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    max_pages: u32,
    max_rows: u64,
    scan_timeout: Duration,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl fmt::Debug for OpenConnectorExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenConnectorExec")
            .field("table", &self.target.table_id)
            .field("action", &self.target.action_id)
            .field("limit", &self.limit)
            .field("projection", &self.projection)
            .finish()
    }
}

impl OpenConnectorExec {
    /// Build a scan. `filter_inputs` are the Exact-translated predicates;
    /// `projection` indexes into the converter's fixed schema; `limit` is
    /// the SQL limit after pushdown.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: Arc<OpenConnectorClient>,
        cache: Option<Arc<ScanCache>>,
        gateway: String,
        binding: Option<String>,
        connection_alias: Option<String>,
        target: ScanTarget,
        converter: Arc<RowConverter>,
        row_path: RowPath,
        resource: Value,
        filter_inputs: Vec<(String, Value)>,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        max_pages: u32,
        max_rows: u64,
        scan_timeout: Duration,
    ) -> DFResult<Self> {
        let full_schema = Arc::clone(converter.schema());
        let schema = match &projection {
            Some(indices) => Arc::new(full_schema.project(indices)?),
            None => full_schema,
        };
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Self {
            client,
            cache,
            gateway,
            binding,
            connection_alias,
            target,
            converter,
            row_path,
            resource,
            filter_inputs,
            projection,
            limit,
            max_pages,
            max_rows,
            scan_timeout,
            schema,
            properties,
        })
    }
}

impl DisplayAs for OpenConnectorExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "OpenConnectorExec: table={} action={} limit={:?}",
            self.target.table_id, self.target.action_id, self.limit
        )
    }
}

impl ExecutionPlan for OpenConnectorExec {
    fn name(&self) -> &str {
        "OpenConnectorExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "OpenConnectorExec is a leaf plan and takes no children".to_string(),
            ))
        }
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![]
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "OpenConnectorExec has a single partition, got partition {partition}"
            )));
        }

        // `try_unfold` owns the scan state between pages; the stream is
        // pull-driven, so dropping it stops further requests — cancellation
        // for free, and a failed page fails the whole scan (no partial
        // success).
        let state = ScanState::new(self).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let stream = stream::try_unfold(state, |mut state| async move {
            match state.next_page().await {
                Ok(Some(batch)) => Ok(Some((batch, state))),
                Ok(None) => Ok(None),
                Err(e) => {
                    // The scan-failure counterpart of the completion event in
                    // `next_page` — same identifying fields plus the error.
                    // The error display may quote a bounded (≤512-char)
                    // snippet of the gateway's *error* response and a
                    // pagination cursor; it never carries tokens,
                    // authorization headers, successful-response bodies, or
                    // row data (conversion and row-path failures report JSON
                    // *kinds* only).
                    tracing::warn!(
                        gateway = %state.gateway,
                        binding = state.binding.as_deref().unwrap_or("<udtf>"),
                        table = %state.target.table_id,
                        action = %state.target.action_id,
                        pages_fetched = state.pages_fetched,
                        error = %e,
                        "Open Connector scan failed"
                    );
                    Err(DataFusionError::External(Box::new(e)))
                }
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }
}

/// Mutable state of one running scan.
struct ScanState {
    client: Arc<OpenConnectorClient>,
    cache: Option<Arc<ScanCache>>,
    cache_key: String,
    gateway: String,
    binding: Option<String>,
    connection_alias: Option<String>,
    target: ScanTarget,
    converter: Arc<RowConverter>,
    row_path: RowPath,
    resource: Value,
    filter_inputs: Vec<(String, Value)>,
    projection: Option<Vec<usize>>,
    limit_remaining: Option<usize>,
    max_pages: u32,
    max_rows: u64,
    scan_timeout: Duration,
    deadline: Instant,
    pagination: Pagination,
    /// Pre-parsed in-band provider-error path, checked before each page's
    /// row extraction.
    error_path: Option<RowPath>,
    rows_emitted: u64,
    /// Cached batches to replay (non-empty only on a cache hit).
    replay: VecDeque<RecordBatch>,
    /// Batches fetched live, for the cache store on completion.
    fetched: Vec<RecordBatch>,
    done: bool,
    /// Idempotence guard for the two store sites (LIMIT-satisfied and
    /// exhaustion), which can both fire on the same page.
    cache_stored: bool,
    // Observability (see `log_completion`).
    started: Instant,
    cache_hit: bool,
    pages_fetched: u32,
    rows_returned: u64,
    completion_logged: bool,
}

impl ScanState {
    fn new(exec: &OpenConnectorExec) -> Result<Self, OpenConnectorError> {
        // Projection indices index into the converter's *fixed* schema, so
        // resolve names through it — not through the (already projected)
        // plan schema, which is shorter.
        let converter_schema = exec.converter.schema();
        let projection_names: Vec<String> = match &exec.projection {
            Some(indices) => indices
                .iter()
                .map(|&i| converter_schema.field(i).name().clone())
                .collect(),
            None => converter_schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
        };
        // Key parts follow the design's cache-key list, with two deliberate
        // wrinkles: `limit` IS in the key (that membership is what makes
        // caching a LIMIT-satisfied scan safe — see the store sites below),
        // and `fixed_inputs` is absent because pack-pinned inputs are
        // functionally determined by (action_id, source_pack_version), both
        // already keyed. If fixed inputs ever become binding-configurable or
        // vary within a pack version, they must join the key.
        let cache_key = scan_cache_key(&ScanKeyParts {
            gateway: &exec.gateway,
            connection_alias: exec.connection_alias.as_deref(),
            action_id: &exec.target.action_id,
            source_pack_version: exec.target.source_pack_version,
            resource: &exec.resource,
            filter_inputs: &exec.filter_inputs,
            projection: &projection_names,
            limit: exec.limit,
            schema_fingerprint: &schema_fingerprint(&exec.schema),
        });

        // A cache hit replays whole batches (LIMIT is part of the key, so a
        // truncated cached scan only ever serves an identical query).
        let cached = exec
            .cache
            .as_ref()
            .and_then(|cache| cache.get(&cache_key))
            .map(VecDeque::from);
        // An empty batch list is a valid completed scan, so preserve the
        // Option to distinguish an empty cache hit from a cache miss.
        let done = cached.is_some();
        let replay = cached.unwrap_or_default();

        Ok(Self {
            client: exec.client.clone(),
            cache: exec.cache.clone(),
            cache_key,
            gateway: exec.gateway.clone(),
            binding: exec.binding.clone(),
            connection_alias: exec.connection_alias.clone(),
            target: exec.target.clone(),
            converter: exec.converter.clone(),
            row_path: exec.row_path.clone(),
            resource: exec.resource.clone(),
            filter_inputs: exec.filter_inputs.clone(),
            projection: exec.projection.clone(),
            limit_remaining: exec.limit,
            max_pages: exec.max_pages,
            max_rows: exec.max_rows,
            scan_timeout: exec.scan_timeout,
            deadline: Instant::now() + exec.scan_timeout,
            pagination: Pagination::new(exec.target.pagination)?,
            error_path: exec.target.error_path.map(RowPath::parse).transpose()?,
            rows_emitted: 0,
            replay,
            fetched: Vec::new(),
            done,
            cache_stored: false,
            started: Instant::now(),
            cache_hit: done,
            pages_fetched: 0,
            rows_returned: 0,
            completion_logged: false,
        })
    }

    fn store_cache(&mut self) {
        if self.cache_stored {
            return;
        }
        if let Some(cache) = &self.cache {
            cache.put(self.cache_key.clone(), self.fetched.clone());
        }
        self.cache_stored = true;
    }

    fn timeout_error(&self) -> OpenConnectorError {
        OpenConnectorError::ScanTimeout {
            table: self.target.table_id.to_string(),
            seconds: self.scan_timeout.as_secs(),
        }
    }

    /// Emit the scan-completion event exactly once. Identifying fields and
    /// counters only — never tokens, headers, inputs, or row values.
    fn log_completion(&mut self) {
        if self.completion_logged {
            return;
        }
        self.completion_logged = true;
        tracing::info!(
            gateway = %self.gateway,
            binding = self.binding.as_deref().unwrap_or("<udtf>"),
            table = %self.target.table_id,
            action = %self.target.action_id,
            cache_hit = self.cache_hit,
            pages = self.pages_fetched,
            rows = self.rows_returned,
            duration_ms = self.started.elapsed().as_millis() as u64,
            "Open Connector scan completed"
        );
    }

    /// Fetch and convert one page; returns None when the scan is complete.
    async fn next_page(&mut self) -> Result<Option<RecordBatch>, OpenConnectorError> {
        if let Some(batch) = self.replay.pop_front() {
            self.rows_returned += batch.num_rows() as u64;
            // The last replayed batch may satisfy a downstream LIMIT, which
            // drops this stream without ever polling again — log with the
            // final batch, not on a poll that may never come.
            if self.replay.is_empty() {
                self.log_completion();
            }
            return Ok(Some(batch));
        }
        if self.done || self.limit_remaining == Some(0) {
            self.done = true;
            self.log_completion();
            return Ok(None);
        }
        if Instant::now() >= self.deadline {
            return Err(self.timeout_error());
        }
        if self.pagination.page() > self.max_pages as usize {
            return Err(OpenConnectorError::ScanBoundsExceeded {
                table: self.target.table_id.to_string(),
                bound: "max_pages",
                limit: u64::from(self.max_pages),
            });
        }

        // Assemble the action input: resource inputs, the pack's fixed
        // inputs, pushed-down filters (which may override a fixed input —
        // `state=all` yields to a pushed `state='open'`), then page
        // parameters.
        let mut input = self.resource.as_object().cloned().expect(
            "resource is a JSON object by construction (registration always builds Value::Object)",
        );
        for (field, value) in self.target.fixed_inputs {
            input.insert((*field).to_string(), value.to_json());
        }
        for (field, value) in &self.filter_inputs {
            input.insert(field.clone(), value.clone());
        }
        self.pagination.apply(&mut input);

        let page = self.pagination.page();
        // The scan deadline covers the whole gateway operation, including
        // request I/O and any retry/backoff inside the client. Dropping this
        // future on timeout also prevents another retry from being sent.
        let envelope = tokio::time::timeout_at(
            tokio::time::Instant::from_std(self.deadline),
            self.client.execute(
                &self.target.action_id,
                &Value::Object(input),
                self.connection_alias.as_deref(),
            ),
        )
        .await
        .map_err(|_| self.timeout_error())??;
        // Counted at fetch time on purpose: `pages_fetched` measures gateway
        // traffic (requests actually made, rate-limit budget actually spent),
        // not pages emitted downstream. A page that lands right at the
        // deadline — or fails extraction/conversion below — was still a real
        // gateway call, and the failure event should say so.
        self.pages_fetched += 1;
        if Instant::now() >= self.deadline {
            return Err(self.timeout_error());
        }
        // Some gateways forward a provider's in-band application errors
        // unchanged (Slack-style HTTP 200, `ok: false` + `error`). Packs
        // targeting such a gateway declare `error_path` so the provider's
        // own code surfaces instead of the misleading row-path error the
        // missing row array would raise. (Open Connector's own executors
        // consume Slack's `ok:false` and return a failure envelope, so its
        // slack pack declares none — the mock pack models this mechanism.)
        if let Some(error_path) = &self.error_path
            && let Ok(code) = error_path.extract(&envelope, page)
            && !code.is_null()
        {
            let code = match code.as_str() {
                Some(text) => text.chars().take(128).collect(),
                None => format!(
                    "<{}>",
                    crate::sources::providers::open_connector::row_path::json_kind(code)
                ),
            };
            return Err(OpenConnectorError::ProviderReportedError {
                action_id: self.target.action_id.to_string(),
                page,
                code,
            });
        }
        let rows = self.row_path.rows(&envelope, page)?;
        let batch = self.converter.convert(rows, page)?;
        // Conversion is synchronous, so it cannot be preempted by Tokio; do
        // not emit its result if it consumed the remaining scan budget.
        if Instant::now() >= self.deadline {
            return Err(self.timeout_error());
        }
        let batch = match &self.projection {
            Some(indices) => {
                batch
                    .project(indices)
                    .map_err(|e| OpenConnectorError::ConversionFailed {
                        path: self.row_path.as_str().to_string(),
                        column: "<projection>".to_string(),
                        page,
                        row: 0,
                        expected: "a valid projection".to_string(),
                        found: e.to_string(),
                    })?
            }
            None => batch,
        };

        // LIMIT pushdown: truncate the page and stop after it.
        let batch = match &mut self.limit_remaining {
            Some(remaining) => {
                let take = (*remaining).min(batch.num_rows());
                let sliced = batch.slice(0, take);
                *remaining -= take;
                sliced
            }
            None => batch,
        };
        self.rows_emitted += batch.num_rows() as u64;
        if self.rows_emitted > self.max_rows {
            return Err(OpenConnectorError::ScanBoundsExceeded {
                table: self.target.table_id.to_string(),
                bound: "max_rows",
                limit: self.max_rows,
            });
        }
        if batch.num_rows() > 0 {
            self.fetched.push(batch.clone());
        }

        // LIMIT satisfied: the truncated batches are the COMPLETE result for
        // this key (LIMIT is part of the key), so store them now even though
        // pagination is not exhausted — DataFusion's limit operator may never
        // poll this stream again, which is exactly why the advance-based
        // store below is not enough.
        if self.limit_remaining == Some(0) {
            self.done = true;
            self.store_cache();
        }

        // Pagination advances only while the scan is still going. After a
        // LIMIT-satisfied page there is no next request to prepare — and
        // advance() also parses and validates continuation state, so a
        // repeated cursor or a missing/malformed page total on that final
        // page would fail a scan whose result is already complete for its
        // key.
        if !self.done {
            let more = self.pagination.advance(&envelope, rows.len())?;
            if !more {
                self.done = true;
                self.store_cache();
            }
        }

        // A terminal empty page is completion, not output.
        if batch.num_rows() == 0 && self.done {
            self.log_completion();
            return Ok(None);
        }
        self.rows_returned += batch.num_rows() as u64;
        // A completing scan must log with its final batch: a satisfied
        // downstream LIMIT drops this stream immediately (DataFusion's
        // LimitStream clears its input), so the `self.done` early return
        // above may never run — the same reason the LIMIT-satisfied cache
        // store is eager. Logged after the row count so the event carries
        // the full total.
        if self.done {
            self.log_completion();
        }
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::providers::open_connector::packs::mock::MOCK_PACK;
    use crate::sources::providers::open_connector::testutil::{
        CapturedEvent, MockGateway, MockResponse, RecordedRequest, capture_events, envelope_ok,
    };
    use futures::StreamExt;
    use serde_json::json;
    use std::sync::Mutex;

    fn build_exec(
        client: Arc<OpenConnectorClient>,
        cache: Option<Arc<ScanCache>>,
        limit: Option<usize>,
        source_pack_version: u32,
    ) -> OpenConnectorExec {
        let table = &MOCK_PACK.tables[0];
        OpenConnectorExec::new(
            client,
            cache,
            "saas".to_string(),
            Some("ws".to_string()),
            None,
            ScanTarget::from_pack_table(table, source_pack_version),
            Arc::new(RowConverter::new(table.fields).expect("converter")),
            RowPath::parse(table.row_path).expect("row path"),
            json!({}),
            vec![],
            None,
            limit,
            10,
            1000,
            Duration::from_secs(30),
        )
        .expect("build exec")
    }

    fn exec_with_version(source_pack_version: u32) -> OpenConnectorExec {
        let client = Arc::new(
            OpenConnectorClient::new("http://127.0.0.1:1", "t", Duration::from_secs(1))
                .expect("build client"),
        );
        build_exec(client, None, None, source_pack_version)
    }

    /// Execute-only mock gateway for `mock.list_items` (per_page = 2), the
    /// only call `ScanState` makes (discovery/health happen at registration).
    fn items_handler(req: &RecordedRequest, total: usize) -> MockResponse {
        if req.method == "POST" && req.path == "/v1/actions/mock.list_items" {
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            let page = body
                .get("input")
                .and_then(|input| input.get("page"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(1) as usize;
            let items: Vec<_> = (1..=total)
                .map(|id| json!({"id": id, "name": format!("item-{id}")}))
                .skip((page - 1) * 2)
                .take(2)
                .collect();
            return MockResponse::ok(&envelope_ok(&json!({"items": items}).to_string()));
        }
        MockResponse::new(404, "{}")
    }

    async fn online_client(gateway: &MockGateway) -> Arc<OpenConnectorClient> {
        Arc::new(
            OpenConnectorClient::new(&gateway.url, "t", Duration::from_secs(5))
                .expect("build client"),
        )
    }

    #[tokio::test]
    async fn limit_satisfied_scan_logs_completion_with_the_final_batch() {
        // A satisfied downstream LIMIT drops the stream without another poll
        // (DataFusion's LimitStream clears its input), so the completion
        // event must be emitted together with the final batch — the
        // early-return branch at the top of next_page is never reached.
        let gateway = MockGateway::start(|req| items_handler(req, 5)).await;
        let exec = build_exec(online_client(&gateway).await, None, Some(1), 1);

        let mut state = ScanState::new(&exec).expect("state");
        let batch = state.next_page().await.expect("page").expect("batch");
        assert_eq!(batch.num_rows(), 1);
        assert!(
            state.completion_logged,
            "LIMIT-satisfied scan must log completion on its final batch, \
             not on a poll that never comes"
        );
        assert_eq!(
            state.rows_returned, 1,
            "the event must count the final batch"
        );
    }

    #[tokio::test]
    async fn exhausted_scan_logs_completion_with_a_nonempty_final_page() {
        // 3 items at per_page = 2: page 2 is short (1 row), so the scan
        // completes while still returning a batch — the event must not
        // depend on the consumer polling once more for the None.
        let gateway = MockGateway::start(|req| items_handler(req, 3)).await;
        let exec = build_exec(online_client(&gateway).await, None, None, 1);

        let mut state = ScanState::new(&exec).expect("state");
        state.next_page().await.expect("page 1").expect("full page");
        assert!(
            !state.completion_logged,
            "scan is still running after page 1"
        );
        state
            .next_page()
            .await
            .expect("page 2")
            .expect("short page");
        assert!(
            state.completion_logged,
            "exhaustion must log with the final batch"
        );
        assert_eq!(state.rows_returned, 3);
    }

    #[tokio::test]
    async fn cached_replay_logs_completion_with_the_last_replayed_batch() {
        let gateway = MockGateway::start(|req| items_handler(req, 3)).await;
        let cache = Arc::new(ScanCache::new(Duration::from_secs(60), usize::MAX));
        let exec = build_exec(online_client(&gateway).await, Some(cache), None, 1);

        // Live scan to completion populates the cache.
        let mut state = ScanState::new(&exec).expect("live state");
        while state.next_page().await.expect("live page").is_some() {}

        // The replayed scan must log once its queue drains — a satisfied
        // downstream LIMIT would never poll again, exactly as in the live
        // case (LIMIT queries are cached; LIMIT is part of the key).
        let mut state = ScanState::new(&exec).expect("replay state");
        assert!(state.cache_hit, "second identical scan replays from cache");
        let batches = state.replay.len();
        assert!(batches > 0);
        for index in 1..=batches {
            state
                .next_page()
                .await
                .expect("replayed page")
                .expect("batch");
            assert_eq!(
                state.completion_logged,
                index == batches,
                "completion logs exactly when the replay queue drains"
            );
        }
        assert_eq!(state.rows_returned, 3);
    }

    // ── Emitted-event assertions ─────────────────────────────────────────
    // The flag tests above pin the state machine; these pin the actual
    // tracing output — exactly one event per scan, with the documented
    // fields — by consuming the real `execute()` stream.

    const COMPLETED: &str = "Open Connector scan completed";
    const FAILED: &str = "Open Connector scan failed";

    /// Captured events with the given message, in emission order.
    fn events_with_message(
        events: &Arc<Mutex<Vec<CapturedEvent>>>,
        message: &str,
    ) -> Vec<CapturedEvent> {
        events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|event| event.message == message)
            .cloned()
            .collect()
    }

    #[tokio::test]
    async fn limit_satisfied_stream_emits_exactly_one_completion_event() {
        let gateway = MockGateway::start(|req| items_handler(req, 5)).await;
        let exec = build_exec(online_client(&gateway).await, None, Some(1), 1);
        let (_guard, events) = capture_events();

        let mut stream = exec
            .execute(0, Arc::new(TaskContext::default()))
            .expect("stream");
        let batch = stream.next().await.expect("a batch").expect("no error");
        assert_eq!(batch.num_rows(), 1);
        // A satisfied LimitStream drops its input without polling again.
        drop(stream);

        let completed = events_with_message(&events, COMPLETED);
        assert_eq!(completed.len(), 1, "exactly one completion event");
        let event = &completed[0];
        assert_eq!(event.level, tracing::Level::INFO);
        assert_eq!(event.field("gateway"), Some("saas"));
        assert_eq!(event.field("binding"), Some("ws"));
        assert_eq!(event.field("table"), Some("mock.items"));
        assert_eq!(event.field("action"), Some("mock.list_items"));
        assert_eq!(event.field("cache_hit"), Some("false"));
        assert_eq!(event.field("pages"), Some("1"));
        assert_eq!(event.field("rows"), Some("1"));
        assert!(event.fields.contains_key("duration_ms"));
        assert!(events_with_message(&events, FAILED).is_empty());
    }

    #[tokio::test]
    async fn empty_scan_emits_exactly_one_completion_event() {
        let gateway = MockGateway::start(|req| items_handler(req, 0)).await;
        let exec = build_exec(online_client(&gateway).await, None, None, 1);
        let (_guard, events) = capture_events();

        let mut stream = exec
            .execute(0, Arc::new(TaskContext::default()))
            .expect("stream");
        assert!(
            stream.next().await.is_none(),
            "empty scan yields no batches"
        );
        assert!(stream.next().await.is_none(), "stream stays terminated");

        let completed = events_with_message(&events, COMPLETED);
        assert_eq!(completed.len(), 1, "exactly one completion event");
        assert_eq!(completed[0].field("rows"), Some("0"));
        assert_eq!(completed[0].field("pages"), Some("1"));
        assert_eq!(completed[0].field("cache_hit"), Some("false"));
    }

    #[tokio::test]
    async fn cache_replay_emits_exactly_one_completion_event_marked_cache_hit() {
        let gateway = MockGateway::start(|req| items_handler(req, 3)).await;
        let cache = Arc::new(ScanCache::new(Duration::from_secs(60), usize::MAX));
        let exec = build_exec(online_client(&gateway).await, Some(cache), None, 1);
        let (_guard, events) = capture_events();

        for round in 1..=2 {
            let mut stream = exec
                .execute(0, Arc::new(TaskContext::default()))
                .expect("stream");
            let mut rows = 0;
            while let Some(batch) = stream.next().await {
                rows += batch.expect("no error").num_rows();
            }
            assert_eq!(rows, 3, "round {round}");
        }

        let completed = events_with_message(&events, COMPLETED);
        assert_eq!(
            completed.len(),
            2,
            "one completion event per scan, replay included"
        );
        assert_eq!(completed[0].field("cache_hit"), Some("false"));
        assert_eq!(completed[0].field("pages"), Some("2"));
        assert_eq!(completed[1].field("cache_hit"), Some("true"));
        assert_eq!(
            completed[1].field("pages"),
            Some("0"),
            "a replay fetches no live pages"
        );
        assert_eq!(completed[1].field("rows"), Some("3"));
    }

    #[tokio::test]
    async fn failed_scan_emits_one_failure_event_and_no_completion() {
        let gateway = MockGateway::start(|req| {
            if req.method == "POST" {
                // 5xx is terminal for the non-idempotent execute: no retry.
                MockResponse::new(500, r#"{"message":"boom"}"#)
            } else {
                MockResponse::new(404, "{}")
            }
        })
        .await;
        let exec = build_exec(online_client(&gateway).await, None, None, 1);
        let (_guard, events) = capture_events();

        let mut stream = exec
            .execute(0, Arc::new(TaskContext::default()))
            .expect("stream");
        let result = stream.next().await.expect("one item");
        assert!(result.is_err(), "scan must fail");

        let failed = events_with_message(&events, FAILED);
        assert_eq!(failed.len(), 1, "exactly one failure event");
        let event = &failed[0];
        assert_eq!(event.level, tracing::Level::WARN);
        assert_eq!(event.field("gateway"), Some("saas"));
        assert_eq!(event.field("binding"), Some("ws"));
        assert_eq!(event.field("table"), Some("mock.items"));
        assert_eq!(event.field("action"), Some("mock.list_items"));
        assert_eq!(event.field("pages_fetched"), Some("0"));
        let error = event.field("error").expect("error field");
        assert!(
            error.contains("HTTP 500"),
            "error carries the terminal status: {error}"
        );
        assert!(events_with_message(&events, COMPLETED).is_empty());
    }

    #[test]
    fn cache_key_uses_the_bound_pack_version() {
        // The key component exists so a pack upgrade (v1 → v2) cannot serve
        // stale rows from the old schema's cache entry; hardcoding 1 would
        // silently defeat it.
        let state_v1 = ScanState::new(&exec_with_version(1)).expect("state v1");
        let state_v2 = ScanState::new(&exec_with_version(2)).expect("state v2");
        assert!(
            state_v1.cache_key.contains(r#""source_pack_version":1"#),
            "v1 key: {}",
            state_v1.cache_key
        );
        assert!(
            state_v2.cache_key.contains(r#""source_pack_version":2"#),
            "v2 key: {}",
            state_v2.cache_key
        );
        assert_ne!(state_v1.cache_key, state_v2.cache_key);
    }
}
