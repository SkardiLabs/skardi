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
use super::pagination::Pagination;
use super::row_path::RowPath;
use super::source_pack::SourcePackTable;

/// Everything a scan needs, bound once at planning time.
pub struct OpenConnectorExec {
    client: Arc<OpenConnectorClient>,
    cache: Option<Arc<ScanCache>>,
    gateway: String,
    connection_alias: Option<String>,
    table: &'static SourcePackTable,
    source_pack_version: u32,
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
            .field("table", &self.table.id)
            .field("action", &self.table.action_id)
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
        connection_alias: Option<String>,
        table: &'static SourcePackTable,
        source_pack_version: u32,
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
            connection_alias,
            table,
            source_pack_version,
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
            self.table.id, self.table.action_id, self.limit
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
                Err(e) => Err(DataFusionError::External(Box::new(e))),
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
    connection_alias: Option<String>,
    table: &'static SourcePackTable,
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
    rows_emitted: u64,
    /// Cached batches to replay (non-empty only on a cache hit).
    replay: VecDeque<RecordBatch>,
    /// Batches fetched live, for the cache store on completion.
    fetched: Vec<RecordBatch>,
    done: bool,
    /// Idempotence guard for the two store sites (LIMIT-satisfied and
    /// exhaustion), which can both fire on the same page.
    cache_stored: bool,
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
        let cache_key = scan_cache_key(&ScanKeyParts {
            gateway: &exec.gateway,
            connection_alias: exec.connection_alias.as_deref(),
            action_id: exec.table.action_id,
            source_pack_version: exec.source_pack_version,
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
            connection_alias: exec.connection_alias.clone(),
            table: exec.table,
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
            pagination: Pagination::new(exec.table.pagination)?,
            rows_emitted: 0,
            replay,
            fetched: Vec::new(),
            done,
            cache_stored: false,
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
            table: self.table.id.to_string(),
            seconds: self.scan_timeout.as_secs(),
        }
    }

    /// Fetch and convert one page; returns None when the scan is complete.
    async fn next_page(&mut self) -> Result<Option<RecordBatch>, OpenConnectorError> {
        if let Some(batch) = self.replay.pop_front() {
            return Ok(Some(batch));
        }
        if self.done || self.limit_remaining == Some(0) {
            self.done = true;
            return Ok(None);
        }
        if Instant::now() >= self.deadline {
            return Err(self.timeout_error());
        }
        if self.pagination.page() > self.max_pages as usize {
            return Err(OpenConnectorError::ScanBoundsExceeded {
                table: self.table.id.to_string(),
                bound: "max_pages",
                limit: u64::from(self.max_pages),
            });
        }

        // Assemble the action input: fixed resource inputs, Exact filters,
        // then page parameters.
        let mut input = self.resource.as_object().cloned().expect(
            "resource is a JSON object by construction (registration always builds Value::Object)",
        );
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
                self.table.action_id,
                &Value::Object(input),
                self.connection_alias.as_deref(),
            ),
        )
        .await
        .map_err(|_| self.timeout_error())??;
        if Instant::now() >= self.deadline {
            return Err(self.timeout_error());
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
                table: self.table.id.to_string(),
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

        let more = self.pagination.advance(&envelope, rows.len())?;
        if !more {
            self.done = true;
            self.store_cache();
        }

        // A terminal empty page is completion, not output.
        if batch.num_rows() == 0 && self.done {
            return Ok(None);
        }
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::providers::open_connector::packs::mock::MOCK_PACK;
    use serde_json::json;

    fn exec_with_version(source_pack_version: u32) -> OpenConnectorExec {
        let table = &MOCK_PACK.tables[0];
        let client = Arc::new(
            OpenConnectorClient::new("http://127.0.0.1:1", "t", Duration::from_secs(1))
                .expect("build client"),
        );
        OpenConnectorExec::new(
            client,
            None,
            "saas".to_string(),
            None,
            table,
            source_pack_version,
            Arc::new(RowConverter::new(table.fields).expect("converter")),
            RowPath::parse(table.row_path).expect("row path"),
            json!({}),
            vec![],
            None,
            None,
            10,
            1000,
            Duration::from_secs(30),
        )
        .expect("build exec")
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
