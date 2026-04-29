//! `ChromaKnnExec` — physical operator backing the `chroma_knn` UDTF.
//!
//! Status: scaffolding compiles; Arrow conversion of `QueryResponse` and the
//! deferred-vector (subquery) extraction path are TODO. The structural shape
//! mirrors `LanceKnnExec` so it stays familiar to reviewers.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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

use crate::sources::providers::chroma::arrow_conv::knn_response_to_batch;

#[derive(Debug)]
pub struct ChromaKnnExec {
    collection: Arc<ChromaCollection>,
    query_vector: Option<Vec<f32>>,
    query_vector_plan: Option<Arc<dyn ExecutionPlan>>,
    k: usize,
    where_filter: Option<Where>,
    schema: SchemaRef,
    plan_properties: PlanProperties,
}

/// Build the KNN output schema: id, document, metadata, _distance. Embedding
/// is intentionally omitted from KNN output (it's input, not interesting in
/// the result; matches Lance's convention).
pub fn knn_output_schema() -> SchemaRef {
    let entry_struct = DataType::Struct(
        vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]
        .into(),
    );
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("document", DataType::Utf8, true),
        Field::new(
            "metadata",
            DataType::Map(Arc::new(Field::new("entries", entry_struct, false)), false),
            true,
        ),
        Field::new("_distance", DataType::Float32, true),
    ]))
}

impl ChromaKnnExec {
    pub fn try_new_literal(
        collection: Arc<ChromaCollection>,
        query_vector: Vec<f32>,
        k: usize,
        where_filter: Option<Where>,
    ) -> DFResult<Self> {
        let schema = knn_output_schema();
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Self {
            collection,
            query_vector: Some(query_vector),
            query_vector_plan: None,
            k,
            where_filter,
            schema,
            plan_properties,
        })
    }
}

impl DisplayAs for ChromaKnnExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ChromaKnnExec: collection={} k={} filter={}",
            self.collection.name(),
            self.k,
            self.where_filter.is_some()
        )
    }
}

impl ExecutionPlan for ChromaKnnExec {
    fn name(&self) -> &'static str {
        "ChromaKnnExec"
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
        match &self.query_vector_plan {
            Some(p) => vec![p],
            None => vec![],
        }
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        match (self.query_vector_plan.is_some(), children.len()) {
            (false, 0) => Ok(self),
            (true, 1) => Ok(Arc::new(ChromaKnnExec {
                collection: self.collection.clone(),
                query_vector: self.query_vector.clone(),
                query_vector_plan: Some(children.into_iter().next().unwrap()),
                k: self.k,
                where_filter: self.where_filter.clone(),
                schema: self.schema.clone(),
                plan_properties: self.plan_properties.clone(),
            })),
            _ => Err(DataFusionError::Internal(
                "ChromaKnnExec child count mismatch".into(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                "ChromaKnnExec only supports single partition".into(),
            ));
        }
        let collection = self.collection.clone();
        let query_vector = self.query_vector.clone();
        let k = self.k as u32;
        let where_filter = self.where_filter.clone();
        let schema = self.schema.clone();

        let fut = async move {
            let v = query_vector.ok_or_else(|| {
                DataFusionError::NotImplemented(
                    "chroma: deferred-vector KNN (subquery embedding) not yet implemented".into(),
                )
            })?;
            let include = IncludeList(vec![
                Include::Document,
                Include::Metadata,
                Include::Distance,
            ]);
            let response = collection
                .query(vec![v], Some(k), where_filter, None, Some(include))
                .await
                .map_err(|e| DataFusionError::Execution(format!("chroma: query failed: {e}")))?;

            // We sent one query vector, so each top-level Vec has exactly one slot.
            let ids = response.ids.into_iter().next().unwrap_or_default();
            let documents = response
                .documents
                .and_then(|mut v| v.pop())
                .unwrap_or_else(|| vec![None; ids.len()]);
            let metadatas = response
                .metadatas
                .and_then(|mut v| v.pop())
                .unwrap_or_else(|| vec![None; ids.len()]);
            let distances = response
                .distances
                .and_then(|mut v| v.pop())
                .unwrap_or_else(|| vec![None; ids.len()]);

            knn_response_to_batch(schema, ids, documents, metadatas, distances).map_err(|e| {
                DataFusionError::Execution(format!("chroma: arrow conversion failed: {e}"))
            })
        };
        let stream = stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics {
            num_rows: datafusion::common::stats::Precision::Exact(self.k),
            total_byte_size: datafusion::common::stats::Precision::Absent,
            column_statistics: vec![],
        })
    }
}
