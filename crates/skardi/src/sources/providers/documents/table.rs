//! `DocumentsTable` — a read-only DataFusion `TableProvider` exposing a folder
//! of files as `(file, page)` rows. Structure mirrors the Redis provider's
//! `RedisScanExec`: a fixed schema, a single-partition scan, and a
//! `MemoryStream` of one `RecordBatch`.

use std::any::Any;
use std::fmt::{self, Formatter};
use std::sync::{Arc, LazyLock};

use arrow::array::{ArrayRef, Int32Array, RecordBatch, RecordBatchOptions, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::Expr;

use super::parse::{ParseOptions, ParsedPage, parse_source};

/// The fixed row schema produced by every `documents` source (spec §2).
static DOCUMENTS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("doc_id", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("page", DataType::Int32, false),
        Field::new("markdown", DataType::Utf8, false),
        Field::new("tables_json", DataType::Utf8, false),
        Field::new("page_image_ref", DataType::Utf8, true),
        Field::new("image_refs", DataType::Utf8, false),
        Field::new("file_type", DataType::Utf8, false),
    ]))
});

/// Read-only `TableProvider` over a directory / object-store prefix of files.
#[derive(Debug)]
pub struct DocumentsTable {
    root: String,
    opts: ParseOptions,
}

impl DocumentsTable {
    pub fn new(root: String, opts: ParseOptions) -> Self {
        Self { root, opts }
    }

    fn full_schema() -> SchemaRef {
        DOCUMENTS_SCHEMA.clone()
    }
}

#[async_trait]
impl TableProvider for DocumentsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Self::full_schema()
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
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let full = Self::full_schema();
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..full.fields().len()).collect());
        let projected_schema = Arc::new(full.project(&projection)?);

        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Both,
            Boundedness::Bounded,
        );

        Ok(Arc::new(DocumentsScanExec {
            root: self.root.clone(),
            opts: self.opts.clone(),
            projected_schema,
            projection,
            limit,
            properties,
        }))
    }
}

/// Single-partition scan that parses the source and emits one `RecordBatch`.
#[derive(Debug)]
struct DocumentsScanExec {
    root: String,
    opts: ParseOptions,
    projected_schema: SchemaRef,
    projection: Vec<usize>,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl DocumentsScanExec {
    fn fetch_partition(&self, _partition: usize) -> datafusion::common::Result<RecordBatch> {
        // TODO(scale): parse_source runs eagerly here and parses the entire
        // directory into memory in one shot, blocking this tokio worker for the
        // duration. For large sources this should move to spawn_blocking and/or
        // stream batches per file (a RecordBatchStream that yields as each file
        // parses) instead of materializing one big batch. v1: eager is fine.
        let mut rows = parse_source(&self.root, &self.opts)
            .map_err(|e| DataFusionError::Execution(format!("documents parse failed: {:#}", e)))?;

        // TODO(pushdown): `limit` is applied after parsing everything, then
        // truncating — there is no limit/predicate pushdown into parse_source, so
        // a `LIMIT 1` still parses the whole directory. Acceptable for v1; a
        // future version could stop parsing once `limit` rows are produced.
        if let Some(max) = self.limit {
            rows.truncate(max);
        }

        let row_count = rows.len();
        let arrays: Vec<ArrayRef> = self
            .projection
            .iter()
            .map(|&col_idx| build_column(col_idx, &rows))
            .collect();

        if arrays.is_empty() {
            // Empty projection (e.g. `count(*)`): supply the row count explicitly
            // so aggregates see the real cardinality.
            let options = RecordBatchOptions::new().with_row_count(Some(row_count));
            RecordBatch::try_new_with_options(self.projected_schema.clone(), arrays, &options)
                .map_err(|e| DataFusionError::Execution(format!("building RecordBatch: {}", e)))
        } else {
            RecordBatch::try_new(self.projected_schema.clone(), arrays)
                .map_err(|e| DataFusionError::Execution(format!("building RecordBatch: {}", e)))
        }
    }
}

/// Build the Arrow array for the schema column at `col_idx` from parsed rows.
fn build_column(col_idx: usize, rows: &[ParsedPage]) -> ArrayRef {
    match col_idx {
        0 => Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| r.doc_id.as_str()),
        )),
        1 => Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| r.path.as_str()),
        )),
        2 => Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.page))),
        3 => Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| r.markdown.as_str()),
        )),
        4 => Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| r.tables_json.as_str()),
        )),
        5 => Arc::new(StringArray::from_iter(
            rows.iter().map(|r| r.page_image_ref.clone()),
        )),
        6 => Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| image_refs_json(&r.image_refs)),
        )),
        7 => Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| r.file_type.as_str()),
        )),
        other => unreachable!("documents schema has 8 columns, got index {other}"),
    }
}

/// Render the page's image refs as a JSON array string.
fn image_refs_json(refs: &[String]) -> String {
    serde_json::to_string(refs).unwrap_or_else(|_| "[]".to_string())
}

impl DisplayAs for DocumentsScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "DocumentsScanExec: root={}, projected_cols={:?}, limit={:?}",
                self.root, self.projection, self.limit
            ),
            DisplayFormatType::TreeRender => write!(f, "DocumentsScanExec({})", self.root),
        }
    }
}

impl ExecutionPlan for DocumentsScanExec {
    fn name(&self) -> &str {
        "DocumentsScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
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
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let batch = self.fetch_partition(partition)?;
        let schema = self.schema();
        Ok(Box::pin(MemoryStream::try_new(vec![batch], schema, None)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn documents_table_queryable() -> datafusion::error::Result<()> {
        let opts = ParseOptions::from_map(None);
        let table = DocumentsTable::new("tests/fixtures/documents".into(), opts);
        let ctx = SessionContext::new();
        ctx.register_table("documents", Arc::new(table))?;
        let batches = ctx
            .sql("SELECT path, page FROM documents ORDER BY page")
            .await?
            .collect()
            .await?;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn documents_table_count_star() -> datafusion::error::Result<()> {
        let opts = ParseOptions::from_map(None);
        let table = DocumentsTable::new("tests/fixtures/documents".into(), opts);
        let ctx = SessionContext::new();
        ctx.register_table("documents", Arc::new(table))?;
        let batches = ctx
            .sql("SELECT count(*) AS n FROM documents")
            .await?
            .collect()
            .await?;
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 2);
        Ok(())
    }

    #[tokio::test]
    async fn documents_table_select_star_builds_every_column() -> datafusion::error::Result<()> {
        // `documents_table_queryable` only projects `path, page`, so most of
        // `build_column`'s per-index branches (and `image_refs_json`) never
        // run. `SELECT *` forces every column through the projection.
        let opts = ParseOptions::from_map(None);
        let table = DocumentsTable::new("tests/fixtures/documents".into(), opts);
        let ctx = SessionContext::new();
        ctx.register_table("documents", Arc::new(table))?;
        let batches = ctx.sql("SELECT * FROM documents").await?.collect().await?;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        let schema = batches[0].schema();
        for name in [
            "doc_id",
            "path",
            "page",
            "markdown",
            "tables_json",
            "page_image_ref",
            "image_refs",
            "file_type",
        ] {
            assert!(
                schema.field_with_name(name).is_ok(),
                "missing column {name}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn documents_table_explain_renders_scan_exec() -> datafusion::error::Result<()> {
        // Exercises `DisplayAs::fmt_as` (Default format) and `ExecutionPlan::name`,
        // neither of which a plain SELECT's row-fetch path touches.
        let opts = ParseOptions::from_map(None);
        let table = DocumentsTable::new("tests/fixtures/documents".into(), opts);
        let ctx = SessionContext::new();
        ctx.register_table("documents", Arc::new(table))?;
        let batches = ctx
            .sql("EXPLAIN SELECT * FROM documents")
            .await?
            .collect()
            .await?;
        let plan_text: String = batches
            .iter()
            .map(|b| {
                arrow::util::pretty::pretty_format_batches(&[b.clone()])
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert!(
            plan_text.contains("DocumentsScanExec"),
            "expected DocumentsScanExec in plan: {plan_text}"
        );
        Ok(())
    }
}
