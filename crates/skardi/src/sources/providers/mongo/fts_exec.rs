//! Physical execution plan for MongoDB full-text search (`$text` queries).

use arrow::array::{ArrayRef, Float64Array, RecordBatch};
use arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use futures::StreamExt;
use futures::stream;
use mongodb::Collection;
use mongodb::bson::{Bson, Document, doc};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use super::bson_values_to_arrow_array;

/// Physical execution plan that runs a MongoDB `$text` search and returns
/// matching documents with a `_score` column derived from `textScore`.
#[derive(Debug, Clone)]
pub struct MongoFtsExec {
    collection: Collection<Document>,
    query: String,
    limit: usize,
    filter: Option<Document>,
    scan_limit: Option<usize>,
    schema: SchemaRef,
    primary_key: String,
    plan_properties: PlanProperties,
}

impl MongoFtsExec {
    pub fn new(
        collection: Collection<Document>,
        query: String,
        limit: usize,
        filter: Option<Document>,
        schema: SchemaRef,
        primary_key: String,
    ) -> Self {
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            collection,
            query,
            limit,
            filter,
            scan_limit: None,
            schema,
            primary_key,
            plan_properties,
        }
    }

    pub fn with_scan_limit(mut self, limit: Option<usize>) -> Self {
        self.scan_limit = limit;
        self
    }

    /// Execute the MongoDB `$text` query and convert results to a RecordBatch.
    async fn run(&self) -> DFResult<RecordBatch> {
        // Build the filter document with $text at the top level.
        let mut filter_doc = doc! { "$text": { "$search": &self.query } };

        // Merge additional field filters as siblings to $text.
        if let Some(ref extra) = self.filter {
            for (key, value) in extra.iter() {
                filter_doc.insert(key.clone(), value.clone());
            }
        }

        // Project textScore as "score".
        let projection = doc! { "score": { "$meta": "textScore" } };

        let effective_limit = self
            .scan_limit
            .map(|sl| sl.min(self.limit))
            .unwrap_or(self.limit);

        let mut cursor = self
            .collection
            .find(filter_doc)
            .projection(projection)
            .sort(doc! { "score": { "$meta": "textScore" } })
            .limit(effective_limit as i64)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut docs = Vec::new();
        while let Some(result) = cursor.next().await {
            let doc = result.map_err(|e| DataFusionError::External(Box::new(e)))?;
            docs.push(doc);
        }

        self.documents_to_record_batch(docs)
    }

    /// Convert MongoDB documents (with textScore) into an Arrow RecordBatch
    /// matching `self.schema`.
    fn documents_to_record_batch(&self, docs: Vec<Document>) -> DFResult<RecordBatch> {
        let num_rows = docs.len();

        // Collect column values from documents.
        let mut columns: HashMap<String, Vec<Option<Bson>>> = HashMap::new();
        for field in self.schema.fields() {
            columns.insert(field.name().clone(), Vec::with_capacity(num_rows));
        }

        for doc in &docs {
            for field in self.schema.fields() {
                let name = field.name();
                let value = if name == "_score" {
                    // Map the "score" field (from $meta textScore) to _score.
                    doc.get("score").cloned()
                } else if name == &self.primary_key {
                    doc.get("_id").or_else(|| doc.get(name)).cloned()
                } else {
                    doc.get(name).cloned()
                };
                columns.get_mut(name).unwrap().push(value);
            }
        }

        // Build Arrow arrays for each column.
        let arrays: Vec<ArrayRef> = self
            .schema
            .fields()
            .iter()
            .map(|field| {
                let values = columns.get(field.name()).unwrap();
                if field.name() == "_score" {
                    // textScore is always a double — build Float64Array directly.
                    let scores: Vec<Option<f64>> = values
                        .iter()
                        .map(|v| match v {
                            Some(Bson::Double(d)) => Some(*d),
                            Some(Bson::Int32(i)) => Some(*i as f64),
                            Some(Bson::Int64(i)) => Some(*i as f64),
                            _ => None,
                        })
                        .collect();
                    Arc::new(Float64Array::from(scores)) as ArrayRef
                } else {
                    bson_values_to_arrow_array(values, field.data_type())
                }
            })
            .collect();

        RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl DisplayAs for MongoFtsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MongoFtsExec: query={}, limit={}",
            self.query, self.limit
        )
    }
}

impl ExecutionPlan for MongoFtsExec {
    fn name(&self) -> &str {
        "MongoFtsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
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
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let exec = self.clone();
        let schema = self.schema.clone();

        let fut = async move { exec.run().await };

        let stream = stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
