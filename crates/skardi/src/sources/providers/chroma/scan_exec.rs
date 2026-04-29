//! `ChromaScanExec` — single-partition scan against `ChromaCollection::get()`.
//!
//! Status: scaffolding compiles end-to-end; Arrow conversion of
//! `GetResponse { ids, documents, embeddings, metadatas }` into a `RecordBatch`
//! is the next focused task. See TODOs in `execute_scan`.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use chroma::ChromaCollection;
use chroma::types::Where;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use futures::stream;

#[derive(Debug)]
pub struct ChromaScanExec {
    collection: Arc<ChromaCollection>,
    schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    where_filter: Option<Where>,
    limit: Option<usize>,
    plan_properties: PlanProperties,
}

impl ChromaScanExec {
    pub fn try_new(
        collection: Arc<ChromaCollection>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        where_filter: Option<Where>,
        limit: Option<usize>,
    ) -> DFResult<Self> {
        let projected_schema = match &projection {
            None => schema.clone(),
            Some(indices) => Arc::new(schema.project(indices)?),
        };
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Self {
            collection,
            schema,
            projected_schema,
            projection,
            where_filter,
            limit,
            plan_properties,
        })
    }
}

impl DisplayAs for ChromaScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ChromaScanExec: collection={} limit={:?} filter={}",
            self.collection.name(),
            self.limit,
            self.where_filter.is_some()
        )
    }
}

impl ExecutionPlan for ChromaScanExec {
    fn name(&self) -> &'static str {
        "ChromaScanExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }
    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
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
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                "ChromaScanExec only supports a single partition".into(),
            ));
        }
        let collection = self.collection.clone();
        let where_filter = self.where_filter.clone();
        let limit = self.limit;
        let schema = self.projected_schema.clone();
        let projection = self.projection.clone();
        let full_schema = self.schema.clone();

        let fut = async move {
            execute_scan(collection, full_schema, projection, schema, where_filter, limit).await
        };

        let stream = stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.projected_schema.clone(),
            stream,
        )))
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(&self.projected_schema))
    }
}

async fn execute_scan(
    collection: Arc<ChromaCollection>,
    _full_schema: SchemaRef,
    _projection: Option<Vec<usize>>,
    projected_schema: SchemaRef,
    where_filter: Option<Where>,
    limit: Option<usize>,
) -> DFResult<RecordBatch> {
    // Issue the get() request — minimum viable hook so wiring compiles and
    // a no-op scan returns an empty batch with the right schema. Arrow
    // conversion of ids/documents/embeddings/metadatas is the next task
    // (see plan: "Net-new: schema model" item).
    let _response = collection
        .get(
            None,
            where_filter,
            limit.map(|n| n as u32),
            None,
            None,
        )
        .await
        .map_err(|e| DataFusionError::Execution(format!("chroma: get failed: {e}")))?;

    // TODO: convert _response.{ids,documents,embeddings,metadatas} → RecordBatch.
    // For now, return an empty batch with the projected schema so plans run end-to-end.
    Ok(RecordBatch::new_empty(projected_schema))
}
