//! Write paths: insert (add/upsert) and delete.
//!
//! Status: scaffolding compiles end-to-end; Arrow→Chroma row-conversion in
//! `ChromaInsertExec::execute` and the actual `delete()` call in
//! `ChromaDmlExec::execute` are stubbed out and will return an
//! `unimplemented` error at runtime. This unblocks the rest of the wiring;
//! flesh these out next.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use chroma::ChromaCollection;
use chroma::types::Where;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use futures::stream;

fn count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("count", DataType::UInt64, false)]))
}

fn one_row(count: u64) -> DFResult<RecordBatch> {
    let arr = UInt64Array::from(vec![count]);
    RecordBatch::try_new(count_schema(), vec![Arc::new(arr)])
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

#[derive(Debug)]
pub struct ChromaInsertExec {
    collection: Arc<ChromaCollection>,
    input_schema: SchemaRef,
    input: Arc<dyn ExecutionPlan>,
    op: InsertOp,
    plan_properties: PlanProperties,
}

impl ChromaInsertExec {
    pub fn new(
        collection: Arc<ChromaCollection>,
        input_schema: SchemaRef,
        input: Arc<dyn ExecutionPlan>,
        op: InsertOp,
    ) -> Self {
        let count_schema = count_schema();
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(count_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            collection,
            input_schema,
            input,
            op,
            plan_properties,
        }
    }
}

impl DisplayAs for ChromaInsertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ChromaInsertExec: collection={} op={:?}",
            self.collection.name(),
            self.op
        )
    }
}

impl ExecutionPlan for ChromaInsertExec {
    fn name(&self) -> &'static str {
        "ChromaInsertExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        count_schema()
    }
    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "ChromaInsertExec expects exactly 1 child".into(),
            ));
        }
        Ok(Arc::new(ChromaInsertExec {
            collection: self.collection.clone(),
            input_schema: self.input_schema.clone(),
            input: children.into_iter().next().unwrap(),
            op: self.op,
            plan_properties: self.plan_properties.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                "ChromaInsertExec only supports single partition".into(),
            ));
        }
        let stream = stream::once(async move {
            // TODO: pull batches from `self.input`, convert id/document/embedding/metadata
            // columns into Vec<String>/Vec<Option<String>>/Vec<Vec<f32>>/Vec<Option<Metadata>>,
            // and call collection.add(...) or .upsert(...) based on op.
            Err::<RecordBatch, _>(DataFusionError::NotImplemented(
                "chroma: INSERT row conversion is not yet implemented".into(),
            ))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(count_schema(), stream)))
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(&count_schema()))
    }
}

#[derive(Clone, Debug)]
pub enum ChromaWriteOp {
    Delete { where_filter: Option<Where> },
}

#[derive(Debug)]
pub struct ChromaDmlExec {
    collection: Arc<ChromaCollection>,
    op: ChromaWriteOp,
    plan_properties: PlanProperties,
}

impl ChromaDmlExec {
    pub fn new(collection: Arc<ChromaCollection>, op: ChromaWriteOp) -> Self {
        let cs = count_schema();
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(cs.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            collection,
            op,
            plan_properties,
        }
    }
}

impl DisplayAs for ChromaDmlExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ChromaDmlExec: collection={} op={:?}",
            self.collection.name(),
            self.op
        )
    }
}

impl ExecutionPlan for ChromaDmlExec {
    fn name(&self) -> &'static str {
        "ChromaDmlExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        count_schema()
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
                "ChromaDmlExec only supports single partition".into(),
            ));
        }
        let collection = self.collection.clone();
        let op = self.op.clone();
        let stream = stream::once(async move {
            match op {
                ChromaWriteOp::Delete { where_filter } => {
                    collection
                        .delete(None, where_filter, None)
                        .await
                        .map_err(|e| {
                            DataFusionError::Execution(format!("chroma: delete failed: {e}"))
                        })?;
                    one_row(0)
                }
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(count_schema(), stream)))
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(&count_schema()))
    }
}

// Keep `input_schema` referenced so the field isn't pruned and we have a hook
// for the upcoming row-conversion code.
impl ChromaInsertExec {
    #[allow(dead_code)]
    pub(crate) fn input_schema(&self) -> &SchemaRef {
        &self.input_schema
    }
}
