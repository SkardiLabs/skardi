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
use chroma::types::{Include, IncludeList, Where};
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

use crate::sources::providers::chroma::arrow_conv::get_response_to_batch;

#[derive(Debug)]
pub struct ChromaScanExec {
    collection: Arc<ChromaCollection>,
    schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    where_filter: Option<Where>,
    limit: Option<usize>,
    embedding_dim: i32,
    plan_properties: PlanProperties,
}

impl ChromaScanExec {
    pub fn try_new(
        collection: Arc<ChromaCollection>,
        schema: SchemaRef,
        embedding_dim: i32,
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
            embedding_dim,
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
        let projected_schema = self.projected_schema.clone();
        let projection = self.projection.clone();
        let full_schema = self.schema.clone();
        let embedding_dim = self.embedding_dim;

        let fut = async move {
            execute_scan(
                collection,
                full_schema,
                embedding_dim,
                projection,
                projected_schema,
                where_filter,
                limit,
            )
            .await
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
    full_schema: SchemaRef,
    embedding_dim: i32,
    projection: Option<Vec<usize>>,
    projected_schema: SchemaRef,
    where_filter: Option<Where>,
    limit: Option<usize>,
) -> DFResult<RecordBatch> {
    // Always include all four fields; the user's SQL projection is applied
    // after we materialize the full batch. Cheaper to ask Chroma for them
    // unconditionally than to fan WHERE/LIMIT semantics out across two paths.
    let include = IncludeList(vec![
        Include::Document,
        Include::Embedding,
        Include::Metadata,
    ]);

    let response = collection
        .get(
            None,
            where_filter,
            limit.map(|n| n as u32),
            None,
            Some(include),
        )
        .await
        .map_err(|e| DataFusionError::Execution(format!("chroma: get failed: {e}")))?;

    let full_batch = get_response_to_batch(
        full_schema,
        embedding_dim,
        response.ids,
        response.documents,
        response.embeddings,
        response.metadatas,
    )
    .map_err(|e| DataFusionError::Execution(format!("chroma: arrow conversion failed: {e}")))?;

    match projection {
        None => Ok(full_batch),
        Some(indices) => {
            let cols: Vec<_> = indices
                .iter()
                .map(|&i| full_batch.column(i).clone())
                .collect();
            Ok(RecordBatch::try_new(projected_schema, cols)?)
        }
    }
}
