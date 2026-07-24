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
use super::exec::{OpenConnectorExec, ScanTarget};
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
    /// Binding (catalog schema) name for tracing; `None` for UDTF-planned
    /// tables, which have no persistent binding.
    binding: Option<String>,
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
        binding: Option<String>,
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
        // Same bind-time guarantee as the row path: a malformed pack-authored
        // pagination path fails here at registration, not mid-scan.
        table.pagination.validate()?;
        if let Some(error_path) = table.error_path {
            RowPath::parse(error_path)?;
        }
        Ok(Self {
            client,
            cache,
            gateway,
            binding,
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
            self.binding.clone(),
            self.connection_alias.clone(),
            ScanTarget::from_pack_table(self.table, self.source_pack_version),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::providers::open_connector::filters::{
        Fidelity, FilterMapping, ValueFormat,
    };
    use crate::sources::providers::open_connector::json_to_arrow::{FieldMapping, FieldType};
    use crate::sources::providers::open_connector::pagination::PaginationStrategy;
    use datafusion::logical_expr::Operator;

    fn offline_client() -> Arc<OpenConnectorClient> {
        Arc::new(
            OpenConnectorClient::new("http://127.0.0.1:1", "t", Duration::from_secs(1))
                .expect("build client"),
        )
    }

    fn table_with_pagination(pagination: PaginationStrategy) -> &'static SourcePackTable {
        // Leak a tiny test table; tests are few and the value is static-sized.
        Box::leak(Box::new(SourcePackTable {
            id: "test.t",
            action_id: "test.action",
            row_path: "$.items",
            fields: &[FieldMapping {
                name: "id",
                path: "id",
                field_type: FieldType::UInt64,
                nullable: false,
            }],
            pagination,
            required_resources: &[],
            fixed_inputs: &[],
            filters: &[FilterMapping {
                column: "id",
                operator: Operator::Gt,
                input_field: "min_id",
                fidelity: Fidelity::Exact,
                value_format: ValueFormat::Rfc3339,
            }],
            error_path: None,
            expected_fingerprint: None,
        }))
    }

    #[test]
    fn malformed_pagination_path_fails_at_bind_time() {
        // A pack bug must surface here — alongside the row path — not
        // mid-scan on page two.
        let table = table_with_pagination(PaginationStrategy::Cursor {
            cursor_param: "cursor",
            next_cursor_path: "not-a-path",
            page_size_param: None,
            page_size: 50,
        });
        let err = OpenConnectorTableProvider::new(
            offline_client(),
            None,
            "saas".to_string(),
            None,
            None,
            table,
            1,
            serde_json::json!({}),
            10,
            1000,
            Duration::from_secs(30),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            super::super::error::OpenConnectorError::InvalidRowPath { .. }
        ));
    }

    #[test]
    fn valid_cursor_pagination_binds() {
        let table = table_with_pagination(PaginationStrategy::Cursor {
            cursor_param: "cursor",
            next_cursor_path: "$.next_cursor",
            page_size_param: None,
            page_size: 50,
        });
        OpenConnectorTableProvider::new(
            offline_client(),
            None,
            "saas".to_string(),
            None,
            None,
            table,
            1,
            serde_json::json!({}),
            10,
            1000,
            Duration::from_secs(30),
        )
        .expect("valid cursor pagination binds");
    }
}
