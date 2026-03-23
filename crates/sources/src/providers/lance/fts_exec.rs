//! Custom execution plan for Lance Full-Text Search
//!
//! This execution plan calls Lance's Scanner::full_text_search() API
//! for efficient BM25-scored text search over inverted indexes.

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::stats::Precision;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
    execution_plan::{Boundedness, EmissionType},
};
use futures::{StreamExt, stream};
use lance::dataset::Dataset;
use lance_index::scalar::FullTextSearchQuery;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// Physical execution plan for Lance full-text search
///
/// Uses Lance's Scanner::full_text_search() API with BM25 scoring.
/// Output includes a `_score` column (Float32) where higher = better match.
#[derive(Debug, Clone)]
pub struct LanceFtsExec {
    /// Lance dataset to search
    dataset: Arc<Dataset>,
    /// The full-text search query to execute
    fts_query: FullTextSearchQuery,
    /// Schema of the output (dataset fields + _score)
    schema: SchemaRef,
    /// Optional metadata filter from WHERE clause pushdown
    filter: Option<String>,
    /// Plan properties
    plan_properties: PlanProperties,
}

impl LanceFtsExec {
    /// Create a new LanceFtsExec
    pub fn try_new(dataset: Arc<Dataset>, fts_query: FullTextSearchQuery) -> DFResult<Self> {
        // Build schema: all dataset fields + _score
        let lance_schema = dataset.schema();
        let mut fields: Vec<Field> = lance_schema.fields.iter().map(|f| f.into()).collect();
        fields.push(Field::new("_score", DataType::Float32, true));
        let schema: SchemaRef = Arc::new(Schema::new(fields));

        let plan_properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Self {
            dataset,
            fts_query,
            schema,
            filter: None,
            plan_properties,
        })
    }

    /// Add a metadata filter predicate (e.g., "category = 'food'")
    pub fn with_filter(mut self, filter: String) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Execute the full-text search
    async fn execute_fts(&self) -> Result<RecordBatch> {
        let mut scanner = self.dataset.scan();

        // Apply metadata filter if present
        if let Some(ref filter_expr) = self.filter {
            scanner.filter(filter_expr)?;
        }

        // Execute full-text search
        scanner.full_text_search(self.fts_query.clone())?;

        // Collect results
        let mut stream = scanner.try_into_stream().await?;
        let mut batches: Vec<RecordBatch> = Vec::new();
        while let Some(batch_result) = stream.next().await {
            batches.push(batch_result?);
        }

        if batches.is_empty() {
            Ok(RecordBatch::new_empty(self.schema.clone()))
        } else if batches.len() == 1 {
            Ok(batches.into_iter().next().unwrap())
        } else {
            let batch_schema = batches[0].schema();
            arrow::compute::concat_batches(&batch_schema, &batches)
                .map_err(|e| anyhow::anyhow!("Failed to concatenate batches: {}", e))
        }
    }
}

impl DisplayAs for LanceFtsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LanceFtsExec: query={}", self.fts_query.query,)
    }
}

impl ExecutionPlan for LanceFtsExec {
    fn name(&self) -> &str {
        "LanceFtsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
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
            Err(DataFusionError::Internal(format!(
                "LanceFtsExec expected 0 children, got {}",
                children.len()
            )))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                "LanceFtsExec only supports single partition".to_string(),
            ));
        }

        let schema = self.schema.clone();
        let fts_exec = self.clone();

        let stream = stream::once(async move {
            fts_exec
                .execute_fts()
                .await
                .map_err(|e| DataFusionError::Execution(e.to_string()))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        })
    }
}
