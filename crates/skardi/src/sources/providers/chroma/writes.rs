//! Write paths: insert (add/upsert) and delete.
//!
//! Status: scaffolding compiles end-to-end; Arrow→Chroma row-conversion in
//! `ChromaInsertExec::execute` and the actual `delete()` call in
//! `ChromaDmlExec::execute` are stubbed out and will return an
//! `unimplemented` error at runtime. This unblocks the rest of the wiring;
//! flesh these out next.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::array::{
    Array, FixedSizeListArray, Float32Array, MapArray, RecordBatch, StringArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use chroma::ChromaCollection;
use chroma::types::{Metadata, MetadataValue, UpdateMetadata, UpdateMetadataValue, Where};
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
use futures::{StreamExt, stream};

fn count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

fn one_row(count: u64) -> DFResult<RecordBatch> {
    let arr = UInt64Array::from(vec![count]);
    RecordBatch::try_new(count_schema(), vec![Arc::new(arr)])
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

#[derive(Debug)]
pub struct ChromaInsertExec {
    collection: Arc<ChromaCollection>,
    input: Arc<dyn ExecutionPlan>,
    op: InsertOp,
    plan_properties: PlanProperties,
}

impl ChromaInsertExec {
    pub fn new(
        collection: Arc<ChromaCollection>,
        _input_schema: SchemaRef,
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
            input: children.into_iter().next().unwrap(),
            op: self.op,
            plan_properties: self.plan_properties.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                "ChromaInsertExec only supports single partition".into(),
            ));
        }
        let collection = self.collection.clone();
        let input = self.input.clone();
        let op = self.op;

        let fut = async move {
            let mut total: u64 = 0;
            let mut input_stream = input.execute(0, context)?;
            while let Some(batch) = input_stream.next().await {
                let batch = batch?;
                let (ids, embeddings, documents, metadatas) = extract_insert_columns(&batch)
                    .map_err(|e| DataFusionError::Execution(format!("chroma: {e}")))?;
                let n = ids.len() as u64;
                let docs_opt = if documents.iter().any(Option::is_some) {
                    Some(documents)
                } else {
                    None
                };
                match op {
                    InsertOp::Append => {
                        let metas_opt: Option<Vec<Option<Metadata>>> =
                            if metadatas.iter().any(Option::is_some) {
                                Some(metadatas)
                            } else {
                                None
                            };
                        collection
                            .add(ids, embeddings, docs_opt, None, metas_opt)
                            .await
                            .map_err(|e| {
                                DataFusionError::Execution(format!("chroma: add failed: {e}"))
                            })?;
                    }
                    InsertOp::Overwrite | InsertOp::Replace => {
                        let metas_opt: Option<Vec<Option<UpdateMetadata>>> =
                            if metadatas.iter().any(Option::is_some) {
                                Some(metadatas.into_iter().map(metadata_to_update).collect())
                            } else {
                                None
                            };
                        collection
                            .upsert(ids, embeddings, docs_opt, None, metas_opt)
                            .await
                            .map_err(|e| {
                                DataFusionError::Execution(format!("chroma: upsert failed: {e}"))
                            })?;
                    }
                }
                total += n;
            }
            one_row(total)
        };

        let stream = stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            count_schema(),
            stream,
        )))
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
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            count_schema(),
            stream,
        )))
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(&count_schema()))
    }
}

#[allow(clippy::type_complexity)]
fn extract_insert_columns(
    batch: &RecordBatch,
) -> anyhow::Result<(
    Vec<String>,
    Vec<Vec<f32>>,
    Vec<Option<String>>,
    Vec<Option<Metadata>>,
)> {
    let n = batch.num_rows();
    let id = batch
        .column_by_name("id")
        .ok_or_else(|| anyhow::anyhow!("INSERT input is missing required column 'id'"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("INSERT 'id' column must be Utf8"))?;
    let embedding = batch
        .column_by_name("embedding")
        .ok_or_else(|| anyhow::anyhow!("INSERT input is missing required column 'embedding'"))?
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| {
            anyhow::anyhow!("INSERT 'embedding' column must be FixedSizeList<Float32>")
        })?;
    let document = batch
        .column_by_name("document")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>().cloned());
    let metadata = batch
        .column_by_name("metadata")
        .and_then(|c| c.as_any().downcast_ref::<MapArray>().cloned());

    let mut ids = Vec::with_capacity(n);
    let mut embeddings = Vec::with_capacity(n);
    let mut documents = Vec::with_capacity(n);
    let mut metadatas: Vec<Option<Metadata>> = Vec::with_capacity(n);

    let dim = embedding.value_length() as usize;
    for i in 0..n {
        if id.is_null(i) {
            anyhow::bail!("INSERT row {i}: 'id' must be non-null");
        }
        ids.push(id.value(i).to_string());

        if embedding.is_null(i) {
            anyhow::bail!("INSERT row {i}: 'embedding' must be non-null");
        }
        let values = embedding.value(i);
        let f32_arr = values
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| anyhow::anyhow!("INSERT row {i}: embedding child must be Float32"))?;
        if f32_arr.len() != dim {
            anyhow::bail!(
                "INSERT row {i}: embedding length {} does not match collection dim {}",
                f32_arr.len(),
                dim
            );
        }
        embeddings.push((0..dim).map(|j| f32_arr.value(j)).collect());

        documents.push(document.as_ref().and_then(|d| {
            if d.is_null(i) {
                None
            } else {
                Some(d.value(i).to_string())
            }
        }));

        metadatas.push(extract_metadata_row(metadata.as_ref(), i)?);
    }

    Ok((ids, embeddings, documents, metadatas))
}

fn extract_metadata_row(map: Option<&MapArray>, i: usize) -> anyhow::Result<Option<Metadata>> {
    let Some(map) = map else { return Ok(None) };
    if map.is_null(i) {
        return Ok(None);
    }
    let entries = map.value(i);
    let key_arr = map.keys().slice(entries.offset(), entries.len());
    let val_arr = map.values().slice(entries.offset(), entries.len());
    let keys = key_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("metadata keys must be Utf8"))?;
    let vals = val_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("metadata values must be Utf8"))?;
    let mut m: HashMap<String, MetadataValue> = HashMap::with_capacity(keys.len());
    for j in 0..keys.len() {
        if keys.is_null(j) {
            continue;
        }
        let k = keys.value(j).to_string();
        let v = if vals.is_null(j) {
            MetadataValue::Str(String::new())
        } else {
            // Best-effort type coercion: try int, then float, then bool, else string.
            let raw = vals.value(j);
            if let Ok(n) = raw.parse::<i64>() {
                MetadataValue::Int(n)
            } else if let Ok(f) = raw.parse::<f64>() {
                MetadataValue::Float(f)
            } else if let Ok(b) = raw.parse::<bool>() {
                MetadataValue::Bool(b)
            } else {
                MetadataValue::Str(raw.to_string())
            }
        };
        m.insert(k, v);
    }
    Ok(Some(m))
}

fn metadata_to_update(m: Option<Metadata>) -> Option<UpdateMetadata> {
    m.map(|inner| {
        inner
            .into_iter()
            .map(|(k, v)| (k, metadata_value_to_update(v)))
            .collect()
    })
}

fn metadata_value_to_update(v: MetadataValue) -> UpdateMetadataValue {
    match v {
        MetadataValue::Bool(b) => UpdateMetadataValue::Bool(b),
        MetadataValue::Int(i) => UpdateMetadataValue::Int(i),
        MetadataValue::Float(f) => UpdateMetadataValue::Float(f),
        MetadataValue::Str(s) => UpdateMetadataValue::Str(s),
        // Less-common variants — fall back to a stringified form rather than fail
        // the whole upsert.
        other => UpdateMetadataValue::Str(format!("{other:?}")),
    }
}
