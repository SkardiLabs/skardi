//! DataFusion [`TableProvider`] for one bound source-pack table.

use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::Statistics;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use serde_json::Value;

use super::cache::ScanCache;
use super::client::OpenConnectorClient;
use super::exec::OpenConnectorExec;
use super::filters::translate_filters;
use super::json_to_arrow::RowConverter;
use super::row_path::RowPath;
use super::source_pack::SourcePackTable;

/// A source-pack table bound to a concrete resource (one binding).
///
/// Read-only by construction: `insert_into` is not implemented, matching the
/// milestone-one "no mutating actions" rule.
pub struct OpenConnectorTableProvider {
    client: Arc<OpenConnectorClient>,
    cache: Option<Arc<ScanCache>>,
    gateway: String,
    connection_alias: Option<String>,
    table: &'static SourcePackTable,
    source_pack_version: u32,
    converter: Arc<RowConverter>,
    row_path: RowPath,
    resource: Value,
    max_pages: u32,
    max_rows: u64,
    scan_timeout: Duration,
}

impl std::fmt::Debug for OpenConnectorTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenConnectorTableProvider")
            .field("table", &self.table.id)
            .field("action", &self.table.action_id)
            .field("gateway", &self.gateway)
            .finish()
    }
}

impl OpenConnectorTableProvider {
    /// Bind a source-pack table to its resource inputs.
    ///
    /// # Errors
    /// Returns [`super::error::OpenConnectorError::InvalidRowPath`] when the
    /// pack's row path or a field mapping path is malformed (a pack bug, not
    /// user input).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: Arc<OpenConnectorClient>,
        cache: Option<Arc<ScanCache>>,
        gateway: String,
        connection_alias: Option<String>,
        table: &'static SourcePackTable,
        source_pack_version: u32,
        resource: Value,
        max_pages: u32,
        max_rows: u64,
        scan_timeout: Duration,
    ) -> Result<Self, super::error::OpenConnectorError> {
        let converter = Arc::new(RowConverter::new(table.fields)?);
        let row_path = RowPath::parse(table.row_path)?;
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
            max_pages,
            max_rows,
            scan_timeout,
        })
    }
}

#[async_trait::async_trait]
impl TableProvider for OpenConnectorTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(self.converter.schema())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Classify each filter with the same allowlist the scan uses, so
    /// planning and execution never disagree about what was pushed.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        let owned: Vec<Expr> = filters.iter().map(|e| (*e).clone()).collect();
        let translated = translate_filters(&owned, self.table.filters);
        Ok(translated.pushdown)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let translated = translate_filters(filters, self.table.filters);
        let exec = OpenConnectorExec::new(
            Arc::clone(&self.client),
            self.cache.clone(),
            self.gateway.clone(),
            self.connection_alias.clone(),
            self.table,
            self.source_pack_version,
            Arc::clone(&self.converter),
            self.row_path.clone(),
            self.resource.clone(),
            translated.inputs,
            projection.cloned(),
            limit,
            self.max_pages,
            self.max_rows,
            self.scan_timeout,
        )?;
        Ok(Arc::new(exec))
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}
