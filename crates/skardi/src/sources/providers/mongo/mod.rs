pub mod fts_exec;
pub mod fts_table_function;

use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
    stream::RecordBatchStreamAdapter,
};
use datafusion::physical_plan::{Partitioning, PlanProperties};
use datafusion::prelude::SessionContext;
use futures::stream::StreamExt;
use mongodb::bson::{Bson, Document, doc};
use mongodb::{Client, Collection, Database};
use percent_encoding::NON_ALPHANUMERIC;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;
use tokio::sync::RwLock;

/// MongoDB Table Provider for DataFusion
/// Supports read (scan), write (insert), update, and delete operations
pub struct MongoTableProvider {
    pub(crate) collection: Collection<Document>,
    pub(crate) schema: SchemaRef,
    pub(crate) primary_key: String,
    collection_name: String,
}

impl Debug for MongoTableProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MongoTableProvider")
            .field("collection_name", &self.collection_name)
            .field("primary_key", &self.primary_key)
            .field("schema", &self.schema)
            .finish()
    }
}

impl MongoTableProvider {
    pub async fn new(
        connection_uri: &str,
        database: &str,
        collection_name: &str,
        primary_key: &str,
        schema: Option<SchemaRef>,
    ) -> Result<Self> {
        let client = Client::with_uri_str(connection_uri)
            .await
            .with_context(|| "Failed to connect to MongoDB")?;

        let db = client.database(database);
        let collection = db.collection::<Document>(collection_name);

        let schema = match schema {
            Some(s) => s,
            None => {
                let inferred =
                    Self::infer_schema(&db, &collection, collection_name, primary_key).await?;
                Arc::new(inferred)
            }
        };

        Ok(Self {
            collection,
            schema,
            primary_key: primary_key.to_string(),
            collection_name: collection_name.to_string(),
        })
    }

    async fn infer_schema(
        db: &Database,
        collection: &Collection<Document>,
        collection_name: &str,
        primary_key: &str,
    ) -> Result<Schema> {
        // Try to get schema from validator first (more accurate)
        if let Some(schema) =
            Self::infer_schema_from_validator(db, collection_name, primary_key).await?
        {
            tracing::debug!(
                "Inferred schema from validator for collection '{}'",
                collection_name
            );
            return Ok(schema);
        }

        tracing::debug!(
            "No schema validator found for collection '{}', falling back to document sampling",
            collection_name
        );

        // Fall back to sampling a document
        let sample = collection
            .find_one(doc! {})
            .await
            .with_context(|| "Failed to sample document for schema inference")?;

        let mut fields = vec![];

        // Always add the primary key first
        fields.push(Field::new(primary_key, DataType::Utf8, false));

        if let Some(doc) = sample {
            for (key, value) in doc.iter() {
                if key == "_id" || key == primary_key {
                    continue;
                }

                let data_type = bson_to_arrow_type(value);
                fields.push(Field::new(key, data_type, true));
            }
        }

        if fields.len() == 1 {
            tracing::warn!(
                "No documents found in collection for schema inference, using minimal schema"
            );
        }

        Ok(Schema::new(fields))
    }

    async fn infer_schema_from_validator(
        db: &Database,
        collection_name: &str,
        primary_key: &str,
    ) -> Result<Option<Schema>> {
        let command = doc! {
            "listCollections": 1,
            "filter": { "name": collection_name }
        };

        let result = db
            .run_command(command)
            .await
            .with_context(|| "Failed to run listCollections command")?;

        let cursor = match result.get_document("cursor") {
            Ok(c) => c,
            Err(_) => return Ok(None),
        };

        let first_batch = match cursor.get_array("firstBatch") {
            Ok(b) => b,
            Err(_) => return Ok(None),
        };

        let coll_info = match first_batch.first() {
            Some(Bson::Document(d)) => d,
            _ => return Ok(None),
        };

        let options = match coll_info.get_document("options") {
            Ok(o) => o,
            Err(_) => return Ok(None),
        };

        let validator = match options.get_document("validator") {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };

        // Handle $jsonSchema validator
        let json_schema = match validator.get_document("$jsonSchema") {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };

        let properties = match json_schema.get_document("properties") {
            Ok(p) => p,
            Err(_) => return Ok(None),
        };

        let required_fields: Vec<String> = json_schema
            .get_array("required")
            .map(|arr| {
                arr.iter()
                    .filter_map(|b| b.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let mut fields = vec![];

        // Always add the primary key first
        let pk_nullable = !required_fields.contains(&primary_key.to_string());
        fields.push(Field::new(primary_key, DataType::Utf8, pk_nullable));

        for (key, value) in properties.iter() {
            if key == "_id" || key == primary_key {
                continue;
            }

            let data_type = if let Bson::Document(prop_doc) = value {
                json_schema_type_to_arrow(prop_doc)
            } else {
                DataType::Utf8
            };

            let nullable = !required_fields.contains(&key.to_string());
            fields.push(Field::new(key, data_type, nullable));
        }

        if fields.len() <= 1 {
            return Ok(None);
        }

        Ok(Some(Schema::new(fields)))
    }

    async fn full_scan(&self, limit: Option<usize>) -> Result<Vec<Document>> {
        self.filtered_scan(doc! {}, limit).await
    }

    async fn filtered_scan(&self, filter: Document, limit: Option<usize>) -> Result<Vec<Document>> {
        let mut find = self.collection.find(filter);
        if let Some(n) = limit {
            find = find.limit(n as i64);
        }
        let mut cursor = find
            .await
            .with_context(|| "Failed to execute filtered scan")?;

        let mut results = vec![];
        while let Some(result) = cursor.next().await {
            let doc = result.with_context(|| "Failed to read document")?;
            results.push(doc);
        }
        Ok(results)
    }

    fn documents_to_record_batch(&self, docs: Vec<Document>) -> Result<RecordBatch> {
        let mut columns: HashMap<String, Vec<Option<Bson>>> = HashMap::new();

        for field in self.schema.fields() {
            columns.insert(field.name().clone(), Vec::with_capacity(docs.len()));
        }

        for doc in &docs {
            for field in self.schema.fields() {
                let name = field.name();
                let value = if name == &self.primary_key {
                    doc.get("_id").or_else(|| doc.get(name)).cloned()
                } else {
                    doc.get(name).cloned()
                };
                columns.get_mut(name).unwrap().push(value);
            }
        }

        let arrays: Vec<ArrayRef> = self
            .schema
            .fields()
            .iter()
            .map(|field| {
                let values = columns.get(field.name()).unwrap();
                bson_values_to_arrow_array(values, field.data_type())
            })
            .collect();

        RecordBatch::try_new(self.schema.clone(), arrays)
            .with_context(|| "Failed to create RecordBatch")
    }

    pub async fn insert(&self, batch: &RecordBatch) -> Result<usize> {
        let docs = self.record_batch_to_documents(batch)?;
        let count = docs.len();

        if docs.is_empty() {
            return Ok(0);
        }

        for doc in docs {
            let pk_value = doc
                .get(&self.primary_key)
                .ok_or_else(|| {
                    anyhow::anyhow!("Document missing primary key: {}", self.primary_key)
                })?
                .clone();

            let filter = doc! { &self.primary_key: pk_value };
            let options = mongodb::options::ReplaceOptions::builder()
                .upsert(true)
                .build();

            self.collection
                .replace_one(filter, doc)
                .with_options(options)
                .await
                .with_context(|| "Failed to upsert document")?;
        }

        Ok(count)
    }

    /// Convert Arrow RecordBatch to MongoDB documents
    fn record_batch_to_documents(&self, batch: &RecordBatch) -> Result<Vec<Document>> {
        let mut docs = Vec::with_capacity(batch.num_rows());

        for row in 0..batch.num_rows() {
            let mut doc = Document::new();

            for (idx, field) in self.schema.fields().iter().enumerate() {
                let array = batch.column(idx);
                let value = arrow_value_to_bson(array, row, field.data_type())?;

                if let Some(v) = value {
                    if field.name() == &self.primary_key {
                        doc.insert("_id", v.clone());
                    }
                    doc.insert(field.name().clone(), v);
                }
            }

            docs.push(doc);
        }

        Ok(docs)
    }
}

#[async_trait]
impl TableProvider for MongoTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|expr| {
                if is_pushable_binary_filter(expr) {
                    // Use Inexact so the filter is still present in the logical
                    // plan. DataFusion's UPDATE/DELETE physical planner extracts
                    // filters from the logical plan and passes them to
                    // TableProvider::update/delete_from. With Exact, the
                    // optimizer removes the filter before the physical planner
                    // sees it, causing unfiltered updates.
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Build a MongoDB filter from all pushable expressions.
        let mut filter_doc = Document::new();
        for expr in filters {
            if let Expr::BinaryExpr(binary) = expr {
                if let Ok(part) =
                    binary_expr_to_mongo(&binary.left, &binary.op, &binary.right, &self.primary_key)
                {
                    for (key, value) in part.iter() {
                        filter_doc.insert(key.clone(), value.clone());
                    }
                }
            }
        }

        let docs = if filter_doc.is_empty() {
            tracing::debug!(
                "MongoDB full scan for collection {} (no pushable filters)",
                self.collection_name
            );
            self.full_scan(limit)
                .await
                .map_err(|e| DataFusionError::External(e.into()))?
        } else {
            tracing::debug!(
                "MongoDB filtered scan for collection {} with filter: {:?}",
                self.collection_name,
                filter_doc
            );
            self.filtered_scan(filter_doc, limit)
                .await
                .map_err(|e| DataFusionError::External(e.into()))?
        };

        let batch = self
            .documents_to_record_batch(docs)
            .map_err(|e| DataFusionError::External(e.into()))?;

        let batch = if let Some(proj) = projection {
            let projected_schema = Arc::new(self.schema.project(proj)?);
            let columns: Vec<ArrayRef> = proj.iter().map(|&i| batch.column(i).clone()).collect();
            RecordBatch::try_new(projected_schema, columns)?
        } else {
            batch
        };

        let schema = batch.schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Ok(Arc::new(MongoExecPlan {
            schema,
            batch: Arc::new(RwLock::new(Some(batch))),
            properties,
        }))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _insert_op: datafusion::logical_expr::dml::InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MongoInsertExec {
            input,
            schema: self.schema.clone(),
            collection: self.collection.clone(),
            primary_key: self.primary_key.clone(),
        }))
    }

    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let filter_doc = exprs_to_mongo_filter(&filters, &self.primary_key)?;
        Ok(Arc::new(MongoDmlExec::new(
            self.collection.clone(),
            MongoDmlOp::Delete(filter_doc),
        )))
    }

    async fn update(
        &self,
        _state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if assignments.is_empty() {
            return Err(DataFusionError::Plan(
                "UPDATE requires at least one assignment".to_string(),
            ));
        }

        let filter_doc = exprs_to_mongo_filter(&filters, &self.primary_key)?;
        let mut set_doc = Document::new();
        for (col, expr) in &assignments {
            if col == &self.primary_key {
                return Err(DataFusionError::Plan(format!(
                    "Cannot modify primary key column '{}' — MongoDB disallows updating _id",
                    self.primary_key
                )));
            }
            let value = expr_to_bson_value(expr)?;
            set_doc.insert(col.clone(), value);
        }
        let update_doc = doc! { "$set": set_doc };

        Ok(Arc::new(MongoDmlExec::new(
            self.collection.clone(),
            MongoDmlOp::Update(filter_doc, update_doc),
        )))
    }
}

/// Returns true if the expression is a binary comparison (=, !=, <, <=, >, >=)
/// between a column and a literal value, which can be pushed down to MongoDB.
fn is_pushable_binary_filter(expr: &Expr) -> bool {
    use datafusion::logical_expr::Operator;
    match expr {
        Expr::BinaryExpr(binary) => {
            matches!(
                binary.op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            ) && matches!(
                (binary.left.as_ref(), binary.right.as_ref()),
                (Expr::Column(_), Expr::Literal(..)) | (Expr::Literal(..), Expr::Column(_))
            )
        }
        _ => false,
    }
}

pub(crate) fn bson_to_arrow_type(value: &Bson) -> DataType {
    match value {
        Bson::String(_) | Bson::ObjectId(_) => DataType::Utf8,
        Bson::Int32(_) => DataType::Int32,
        Bson::Int64(_) => DataType::Int64,
        Bson::Double(_) => DataType::Float64,
        Bson::Boolean(_) => DataType::Boolean,
        Bson::DateTime(_) => DataType::Utf8,
        Bson::Null => DataType::Utf8,
        _ => DataType::Utf8,
    }
}

fn json_schema_type_to_arrow(prop: &Document) -> DataType {
    let bson_type = prop.get("bsonType").or_else(|| prop.get("type"));

    match bson_type {
        Some(Bson::String(t)) => match t.as_str() {
            "string" | "objectId" => DataType::Utf8,
            "int" => DataType::Int32,
            "long" => DataType::Int64,
            "double" | "decimal" | "number" => DataType::Float64,
            "bool" | "boolean" => DataType::Boolean,
            "date" => DataType::Utf8,
            _ => DataType::Utf8,
        },
        Some(Bson::Array(types)) => {
            for t in types {
                if let Bson::String(s) = t {
                    if s != "null" {
                        return json_schema_type_to_arrow(&doc! { "bsonType": s.clone() });
                    }
                }
            }
            DataType::Utf8
        }
        _ => DataType::Utf8,
    }
}

pub(crate) fn bson_values_to_arrow_array(
    values: &[Option<Bson>],
    data_type: &DataType,
) -> ArrayRef {
    match data_type {
        DataType::Utf8 => {
            let arr: StringArray = values
                .iter()
                .map(|v| v.as_ref().map(bson_to_string))
                .collect();
            Arc::new(arr)
        }
        DataType::Int32 => {
            let arr: Int32Array = values
                .iter()
                .map(|v| v.as_ref().and_then(bson_to_i32))
                .collect();
            Arc::new(arr)
        }
        DataType::Int64 => {
            let arr: Int64Array = values
                .iter()
                .map(|v| v.as_ref().and_then(bson_to_i64))
                .collect();
            Arc::new(arr)
        }
        DataType::Float64 => {
            let arr: Float64Array = values
                .iter()
                .map(|v| v.as_ref().and_then(bson_to_f64))
                .collect();
            Arc::new(arr)
        }
        DataType::Boolean => {
            let arr: BooleanArray = values
                .iter()
                .map(|v| v.as_ref().and_then(bson_to_bool))
                .collect();
            Arc::new(arr)
        }
        _ => {
            let arr: StringArray = values
                .iter()
                .map(|v| v.as_ref().map(bson_to_string))
                .collect();
            Arc::new(arr)
        }
    }
}

fn bson_to_string(v: &Bson) -> String {
    match v {
        Bson::String(s) => s.clone(),
        Bson::ObjectId(oid) => oid.to_hex(),
        Bson::Int32(i) => i.to_string(),
        Bson::Int64(i) => i.to_string(),
        Bson::Double(f) => f.to_string(),
        Bson::Boolean(b) => b.to_string(),
        Bson::DateTime(dt) => dt.to_string(),
        Bson::Null => String::new(),
        _ => format!("{:?}", v),
    }
}

fn bson_to_i32(v: &Bson) -> Option<i32> {
    match v {
        Bson::Int32(i) => Some(*i),
        Bson::Int64(i) => Some(*i as i32),
        Bson::Double(f) => Some(*f as i32),
        _ => None,
    }
}

fn bson_to_i64(v: &Bson) -> Option<i64> {
    match v {
        Bson::Int64(i) => Some(*i),
        Bson::Int32(i) => Some(*i as i64),
        Bson::Double(f) => Some(*f as i64),
        _ => None,
    }
}

fn bson_to_f64(v: &Bson) -> Option<f64> {
    match v {
        Bson::Double(f) => Some(*f),
        Bson::Int32(i) => Some(*i as f64),
        Bson::Int64(i) => Some(*i as f64),
        _ => None,
    }
}

fn bson_to_bool(v: &Bson) -> Option<bool> {
    match v {
        Bson::Boolean(b) => Some(*b),
        _ => None,
    }
}

fn arrow_value_to_bson(array: &ArrayRef, row: usize, data_type: &DataType) -> Result<Option<Bson>> {
    if array.is_null(row) {
        return Ok(None);
    }

    let value = match data_type {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Some(Bson::String(arr.value(row).to_string()))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Some(Bson::Int32(arr.value(row)))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Some(Bson::Int64(arr.value(row)))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Some(Bson::Double(arr.value(row)))
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Some(Bson::Boolean(arr.value(row)))
        }
        _ => {
            let arr = array.as_any().downcast_ref::<StringArray>();
            arr.map(|a| Bson::String(a.value(row).to_string()))
        }
    };

    Ok(value)
}

/// MongoDB Execution Plan for scans
#[derive(Debug)]
struct MongoExecPlan {
    schema: SchemaRef,
    batch: Arc<RwLock<Option<RecordBatch>>>,
    properties: PlanProperties,
}

impl DisplayAs for MongoExecPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "MongoExecPlan")
    }
}

impl ExecutionPlan for MongoExecPlan {
    fn name(&self) -> &str {
        "MongoExecPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
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
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let schema = self.schema.clone();
        let batch = self.batch.clone();

        let stream = futures::stream::once(async move {
            let guard = batch.read().await;
            match guard.as_ref() {
                Some(b) => Ok(b.clone()),
                None => Err(DataFusionError::Execution(
                    "Batch already consumed".to_string(),
                )),
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// MongoDB Insert Execution Plan
struct MongoInsertExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    collection: Collection<Document>,
    primary_key: String,
}

impl Debug for MongoInsertExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MongoInsertExec")
            .field("primary_key", &self.primary_key)
            .finish()
    }
}

impl DisplayAs for MongoInsertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "MongoInsertExec")
    }
}

impl ExecutionPlan for MongoInsertExec {
    fn name(&self) -> &str {
        "MongoInsertExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MongoInsertExec {
            input: children[0].clone(),
            schema: self.schema.clone(),
            collection: self.collection.clone(),
            primary_key: self.primary_key.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let collection = self.collection.clone();
        let primary_key = self.primary_key.clone();
        let schema = self.schema.clone();
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));

        let output_stream = futures::stream::unfold(
            (input_stream, collection, primary_key, schema),
            |(mut stream, collection, pk, schema)| async move {
                match stream.next().await {
                    Some(Ok(batch)) => {
                        let docs = match record_batch_to_docs(&batch, &pk, &schema) {
                            Ok(d) => d,
                            Err(e) => {
                                return Some((
                                    Err(DataFusionError::External(e.into())),
                                    (stream, collection, pk, schema),
                                ));
                            }
                        };

                        let count = docs.len() as u64;

                        for doc in docs {
                            let pk_value = match doc.get(&pk) {
                                Some(v) => v.clone(),
                                None => {
                                    return Some((
                                        Err(DataFusionError::Execution(format!(
                                            "Document missing primary key: {}",
                                            pk
                                        ))),
                                        (stream, collection, pk, schema),
                                    ));
                                }
                            };

                            let filter = doc! { &pk: pk_value };
                            let options = mongodb::options::ReplaceOptions::builder()
                                .upsert(true)
                                .build();

                            if let Err(e) = collection
                                .replace_one(filter, doc)
                                .with_options(options)
                                .await
                            {
                                return Some((
                                    Err(DataFusionError::External(Box::new(std::io::Error::new(
                                        std::io::ErrorKind::Other,
                                        format!("MongoDB insert failed: {}", e),
                                    )))),
                                    (stream, collection, pk, schema),
                                ));
                            }
                        }

                        let count_batch = create_count_batch(count);
                        Some((count_batch, (stream, collection, pk, schema)))
                    }
                    Some(Err(e)) => Some((Err(e), (stream, collection, pk, schema))),
                    None => None,
                }
            },
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            output_stream,
        )))
    }
}

fn record_batch_to_docs(
    batch: &RecordBatch,
    primary_key: &str,
    schema: &Schema,
) -> Result<Vec<Document>> {
    let mut docs = Vec::with_capacity(batch.num_rows());

    for row in 0..batch.num_rows() {
        let mut doc = Document::new();

        for (idx, field) in schema.fields().iter().enumerate() {
            let array = batch.column(idx);
            let value = arrow_value_to_bson(array, row, field.data_type())?;

            if let Some(v) = value {
                if field.name() == primary_key {
                    doc.insert("_id", v.clone());
                }
                doc.insert(field.name().clone(), v);
            }
        }

        docs.push(doc);
    }

    Ok(docs)
}

fn create_count_batch(count: u64) -> DFResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]));
    let array: arrow::array::UInt64Array = vec![count].into();
    RecordBatch::try_new(schema, vec![Arc::new(array)]).map_err(DataFusionError::from)
}

// ─── DML support (DELETE / UPDATE) ──────────────────────────────────────────

/// Converts a DataFusion literal expression to a BSON value.
pub(crate) fn expr_to_bson_value(expr: &Expr) -> DFResult<Bson> {
    match expr {
        Expr::Literal(scalar, _) => scalar_to_bson(scalar),
        Expr::Column(col) => Ok(Bson::String(col.name.clone())),
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported expression for MongoDB value: {expr}"
        ))),
    }
}

pub(crate) fn scalar_to_bson(scalar: &datafusion::common::ScalarValue) -> DFResult<Bson> {
    use datafusion::common::ScalarValue;
    match scalar {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Ok(Bson::String(s.clone())),
        ScalarValue::Int8(Some(v)) => Ok(Bson::Int32(*v as i32)),
        ScalarValue::Int16(Some(v)) => Ok(Bson::Int32(*v as i32)),
        ScalarValue::Int32(Some(v)) => Ok(Bson::Int32(*v)),
        ScalarValue::Int64(Some(v)) => Ok(Bson::Int64(*v)),
        ScalarValue::UInt8(Some(v)) => Ok(Bson::Int32(*v as i32)),
        ScalarValue::UInt16(Some(v)) => Ok(Bson::Int32(*v as i32)),
        ScalarValue::UInt32(Some(v)) => Ok(Bson::Int64(*v as i64)),
        ScalarValue::UInt64(Some(v)) => Ok(Bson::Int64(*v as i64)),
        ScalarValue::Float32(Some(v)) => Ok(Bson::Double(*v as f64)),
        ScalarValue::Float64(Some(v)) => Ok(Bson::Double(*v)),
        ScalarValue::Boolean(Some(v)) => Ok(Bson::Boolean(*v)),
        ScalarValue::Null => Ok(Bson::Null),
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported scalar type for MongoDB: {scalar}"
        ))),
    }
}

/// Converts a single DataFusion binary expression to a MongoDB filter entry.
pub(crate) fn binary_expr_to_mongo(
    left: &Expr,
    op: &datafusion::logical_expr::Operator,
    right: &Expr,
    primary_key: &str,
) -> DFResult<Document> {
    use datafusion::logical_expr::Operator;

    let (col_name, value) = match (left, right) {
        (Expr::Column(col), expr) | (expr, Expr::Column(col)) => {
            let field = if col.name == primary_key {
                "_id".to_string()
            } else {
                col.name.clone()
            };
            (field, expr_to_bson_value(expr)?)
        }
        _ => {
            return Err(DataFusionError::Plan(format!(
                "MongoDB filter must compare a column to a value, got: {left} {op} {right}"
            )));
        }
    };

    let filter = match op {
        Operator::Eq => doc! { &col_name: value },
        Operator::NotEq => doc! { &col_name: { "$ne": value } },
        Operator::Lt => doc! { &col_name: { "$lt": value } },
        Operator::LtEq => doc! { &col_name: { "$lte": value } },
        Operator::Gt => doc! { &col_name: { "$gt": value } },
        Operator::GtEq => doc! { &col_name: { "$gte": value } },
        _ => {
            return Err(DataFusionError::Plan(format!(
                "Unsupported MongoDB filter operator: {op}"
            )));
        }
    };

    Ok(filter)
}

/// Converts a list of DataFusion filter expressions into a single MongoDB
/// filter document (implicit `$and`).
pub(crate) fn exprs_to_mongo_filter(filters: &[Expr], primary_key: &str) -> DFResult<Document> {
    if filters.is_empty() {
        return Ok(doc! {});
    }

    let mut parts = Vec::with_capacity(filters.len());
    for expr in filters {
        match expr {
            Expr::BinaryExpr(binary) => {
                let part =
                    binary_expr_to_mongo(&binary.left, &binary.op, &binary.right, primary_key)?;
                parts.push(Bson::Document(part));
            }
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "Unsupported MongoDB filter expression: {expr}"
                )));
            }
        }
    }

    Ok(doc! { "$and": parts })
}

/// The kind of DML operation to execute.
#[derive(Debug, Clone)]
enum MongoDmlOp {
    Delete(Document),
    Update(Document, Document), // (filter, update)
}

/// A leaf [`ExecutionPlan`] that executes a MongoDB DML operation and returns
/// a single row `{ count: u64 }` with the number of affected documents.
struct MongoDmlExec {
    collection: Collection<Document>,
    op: MongoDmlOp,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl MongoDmlExec {
    fn new(collection: Collection<Document>, op: MongoDmlOp) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            collection,
            op,
            schema,
            properties,
        }
    }
}

impl Debug for MongoDmlExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "MongoDmlExec(op={:?})", self.op)
    }
}

impl DisplayAs for MongoDmlExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "MongoDmlExec")
    }
}

impl ExecutionPlan for MongoDmlExec {
    fn name(&self) -> &str {
        "MongoDmlExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
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
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let collection = self.collection.clone();
        let op = self.op.clone();

        let future = async move {
            let affected = match op {
                MongoDmlOp::Delete(filter) => {
                    let result = collection.delete_many(filter).await.map_err(|e| {
                        DataFusionError::Execution(format!("MongoDB delete error: {e}"))
                    })?;
                    result.deleted_count
                }
                MongoDmlOp::Update(filter, update) => {
                    let result = collection.update_many(filter, update).await.map_err(|e| {
                        DataFusionError::Execution(format!("MongoDB update error: {e}"))
                    })?;
                    result.modified_count
                }
            };

            create_count_batch(affected)
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::once(future),
        )))
    }
}

fn build_connection_uri(
    base_connection_string: &str,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<String> {
    let url = url::Url::parse(base_connection_string).with_context(|| {
        format!(
            "Invalid MongoDB connection string: {}",
            base_connection_string
        )
    })?;

    let scheme = url.scheme();
    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("MongoDB connection string missing host"))?;
    let port = url.port();

    let host_port = if let Some(p) = port {
        format!("{}:{}", host, p)
    } else if scheme == "mongodb" {
        format!("{}:27017", host)
    } else {
        host.to_string()
    };

    let uri = match (username, password) {
        (Some(user), Some(pass)) => {
            let encoded_user = percent_encoding::utf8_percent_encode(user, NON_ALPHANUMERIC);
            let encoded_pass = percent_encoding::utf8_percent_encode(pass, NON_ALPHANUMERIC);
            format!(
                "{}://{}:{}@{}",
                scheme, encoded_user, encoded_pass, host_port
            )
        }
        (Some(user), None) => {
            let encoded_user = percent_encoding::utf8_percent_encode(user, NON_ALPHANUMERIC);
            format!("{}://{}@{}", scheme, encoded_user, host_port)
        }
        _ => format!("{}://{}", scheme, host_port),
    };

    Ok(uri)
}

/// Register MongoDB collection as a DataFusion table
///
/// # Arguments
/// * `session_ctx` - DataFusion session context to register tables into
/// * `name` - Name to register the table as
/// * `connection_string` - MongoDB connection string (e.g., "mongodb://host:port")
///   Note: Username and password should NOT be included in the connection string.
///   Use `user_env` and `pass_env` options instead.
/// * `options` - Configuration options
///
/// # Options
/// * `database` - Database name (required)
/// * `collection` - Collection name (required)
/// * `primary_key` - Primary key field name (required)
/// * `user_env` - Environment variable name for username (optional)
/// * `pass_env` - Environment variable name for password (optional)
pub async fn register_mongo_tables(
    session_ctx: &mut SessionContext,
    name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
    dataset_registry: Option<&crate::sources::providers::DatasetRegistry>,
) -> Result<()> {
    tracing::info!(
        "Registering MongoDB collection: {} with connection: {}",
        name,
        connection_string
    );

    let opts = options.ok_or_else(|| {
        anyhow::anyhow!(
            "MongoDB data source '{}' requires options (database, collection, primary_key)",
            name
        )
    })?;

    let database = opts.get("database").ok_or_else(|| {
        anyhow::anyhow!("MongoDB data source '{}' requires 'database' option", name)
    })?;

    let collection = opts.get("collection").ok_or_else(|| {
        anyhow::anyhow!(
            "MongoDB data source '{}' requires 'collection' option",
            name
        )
    })?;

    let primary_key = opts.get("primary_key").ok_or_else(|| {
        anyhow::anyhow!(
            "MongoDB data source '{}' requires 'primary_key' option",
            name
        )
    })?;

    let username = if let Some(user_env) = opts.get("user_env") {
        Some(std::env::var(user_env).with_context(|| {
            format!(
                "Environment variable '{}' not found for MongoDB user",
                user_env
            )
        })?)
    } else {
        None
    };

    let password = if let Some(pass_env) = opts.get("pass_env") {
        Some(std::env::var(pass_env).with_context(|| {
            format!(
                "Environment variable '{}' not found for MongoDB password",
                pass_env
            )
        })?)
    } else {
        None
    };

    let connection_uri =
        build_connection_uri(connection_string, username.as_deref(), password.as_deref())?;

    tracing::debug!(
        "Connecting to MongoDB database: {}, collection: {}, primary_key: {}",
        database,
        collection,
        primary_key
    );

    let provider =
        MongoTableProvider::new(&connection_uri, database, collection, primary_key, None)
            .await
            .with_context(|| format!("Failed to create MongoDB table provider for '{}'", name))?;

    // Store a MongoFtsEntry in the dataset registry (if provided) so that
    // the mongo_fts() table function can look up this collection later.
    if let Some(registry) = dataset_registry {
        use crate::sources::providers::DatasetEntry;
        use crate::sources::providers::mongo::fts_table_function::MongoFtsEntry;

        let entry = MongoFtsEntry {
            collection: provider.collection.clone(),
            schema: provider.schema.clone(),
            primary_key: primary_key.to_string(),
        };
        let mut reg = registry
            .write()
            .map_err(|e| anyhow::anyhow!("Failed to acquire dataset registry write lock: {}", e))?;
        reg.insert(name.to_string(), DatasetEntry::Mongo(entry));
        tracing::debug!("Registered MongoFtsEntry '{}' in dataset registry", name);
    }

    session_ctx
        .register_table(name, Arc::new(provider))
        .with_context(|| {
            format!(
                "Failed to register MongoDB table '{}' with DataFusion",
                name
            )
        })?;

    tracing::info!(
        "Successfully registered MongoDB collection '{}.{}' as '{}'",
        database,
        collection,
        name
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bson_to_arrow_type() {
        assert_eq!(
            bson_to_arrow_type(&Bson::String("test".to_string())),
            DataType::Utf8
        );
        assert_eq!(bson_to_arrow_type(&Bson::Int32(42)), DataType::Int32);
        assert_eq!(bson_to_arrow_type(&Bson::Int64(42)), DataType::Int64);
        assert_eq!(bson_to_arrow_type(&Bson::Double(3.14)), DataType::Float64);
        assert_eq!(bson_to_arrow_type(&Bson::Boolean(true)), DataType::Boolean);
    }

    #[test]
    fn test_bson_to_string() {
        assert_eq!(bson_to_string(&Bson::String("hello".to_string())), "hello");
        assert_eq!(bson_to_string(&Bson::Int32(42)), "42");
        assert_eq!(bson_to_string(&Bson::Boolean(true)), "true");
    }

    #[test]
    fn test_bson_to_i32() {
        assert_eq!(bson_to_i32(&Bson::Int32(42)), Some(42));
        assert_eq!(bson_to_i32(&Bson::Int64(42)), Some(42));
        assert_eq!(bson_to_i32(&Bson::String("42".to_string())), None);
    }

    #[test]
    fn test_bson_to_f64() {
        assert_eq!(bson_to_f64(&Bson::Double(3.14)), Some(3.14));
        assert_eq!(bson_to_f64(&Bson::Int32(42)), Some(42.0));
        assert_eq!(bson_to_f64(&Bson::Int64(42)), Some(42.0));
    }

    #[test]
    fn test_build_connection_uri_no_auth() {
        let uri = build_connection_uri("mongodb://localhost:27017", None, None).unwrap();
        assert_eq!(uri, "mongodb://localhost:27017");
    }

    #[test]
    fn test_build_connection_uri_with_auth() {
        let uri =
            build_connection_uri("mongodb://localhost:27017", Some("user"), Some("pass")).unwrap();
        assert_eq!(uri, "mongodb://user:pass@localhost:27017");
    }

    #[test]
    fn test_build_connection_uri_special_chars() {
        let uri = build_connection_uri(
            "mongodb://localhost:27017",
            Some("user@domain"),
            Some("p@ss:word"),
        )
        .unwrap();
        assert!(uri.contains("user%40domain"));
        assert!(uri.contains("p%40ss%3Aword"));
    }

    #[test]
    fn test_build_connection_uri_default_port() {
        let uri = build_connection_uri("mongodb://localhost", None, None).unwrap();
        assert_eq!(uri, "mongodb://localhost:27017");
    }

    #[test]
    fn test_build_connection_uri_srv() {
        let uri = build_connection_uri(
            "mongodb+srv://cluster0.example.mongodb.net",
            Some("user"),
            Some("pass"),
        )
        .unwrap();
        assert_eq!(uri, "mongodb+srv://user:pass@cluster0.example.mongodb.net");
    }

    #[test]
    fn test_build_connection_uri_srv_no_auth() {
        let uri =
            build_connection_uri("mongodb+srv://cluster0.example.mongodb.net", None, None).unwrap();
        assert_eq!(uri, "mongodb+srv://cluster0.example.mongodb.net");
    }

    #[test]
    fn test_build_connection_uri_invalid() {
        let result = build_connection_uri("not-a-valid-url", None, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_options() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut session_ctx = SessionContext::new();
            let result = register_mongo_tables(
                &mut session_ctx,
                "test_mongo",
                "mongodb://localhost:27017",
                None,
                None,
            )
            .await;

            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("requires options"));
        });
    }

    #[test]
    fn test_missing_database_option() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut session_ctx = SessionContext::new();
            let mut options = HashMap::new();
            options.insert("collection".to_string(), "users".to_string());
            options.insert("primary_key".to_string(), "user_id".to_string());

            let result = register_mongo_tables(
                &mut session_ctx,
                "test_mongo",
                "mongodb://localhost:27017",
                Some(&options),
                None,
            )
            .await;

            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("requires 'database' option")
            );
        });
    }

    #[test]
    fn test_missing_primary_key_option() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut session_ctx = SessionContext::new();
            let mut options = HashMap::new();
            options.insert("database".to_string(), "mydb".to_string());
            options.insert("collection".to_string(), "users".to_string());

            let result = register_mongo_tables(
                &mut session_ctx,
                "test_mongo",
                "mongodb://localhost:27017",
                Some(&options),
                None,
            )
            .await;

            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("requires 'primary_key' option")
            );
        });
    }

    #[test]
    fn test_missing_user_env_var() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut session_ctx = SessionContext::new();
            let mut options = HashMap::new();
            options.insert("database".to_string(), "mydb".to_string());
            options.insert("collection".to_string(), "users".to_string());
            options.insert("primary_key".to_string(), "user_id".to_string());
            options.insert("user_env".to_string(), "NONEXISTENT_MONGO_USER".to_string());

            let result = register_mongo_tables(
                &mut session_ctx,
                "test_mongo",
                "mongodb://localhost:27017",
                Some(&options),
                None,
            )
            .await;

            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("Environment variable 'NONEXISTENT_MONGO_USER' not found")
            );
        });
    }

    #[test]
    fn test_missing_pass_env_var() {
        unsafe {
            std::env::set_var("TEST_MONGO_USER", "testuser");
        }
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut session_ctx = SessionContext::new();
            let mut options = HashMap::new();
            options.insert("database".to_string(), "mydb".to_string());
            options.insert("collection".to_string(), "users".to_string());
            options.insert("primary_key".to_string(), "user_id".to_string());
            options.insert("user_env".to_string(), "TEST_MONGO_USER".to_string());
            options.insert("pass_env".to_string(), "NONEXISTENT_MONGO_PASS".to_string());

            let result = register_mongo_tables(
                &mut session_ctx,
                "test_mongo",
                "mongodb://localhost:27017",
                Some(&options),
                None,
            )
            .await;

            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("Environment variable 'NONEXISTENT_MONGO_PASS' not found")
            );
        });
        unsafe {
            std::env::remove_var("TEST_MONGO_USER");
        }
    }

    // ─── Integration test helpers ────────────────────────────────────────

    /// Register a MongoDB collection from the CI docker service.
    /// Expects MONGO_USER and MONGO_PASS env vars to be set.
    async fn register_ci_collection(ctx: &mut SessionContext, collection: &str, primary_key: &str) {
        let mut options = HashMap::new();
        options.insert("database".to_string(), "mydb".to_string());
        options.insert("collection".to_string(), collection.to_string());
        options.insert("primary_key".to_string(), primary_key.to_string());
        options.insert("user_env".to_string(), "MONGO_USER".to_string());
        options.insert("pass_env".to_string(), "MONGO_PASS".to_string());
        register_mongo_tables(
            ctx,
            collection,
            "mongodb://127.0.0.1:27017",
            Some(&options),
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("register {} failed: {}", collection, e));
    }

    async fn query_all(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        let df = ctx.sql(sql).await.expect("parse sql");
        df.collect().await.expect("collect results")
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    // ─── Scan tests (integration) ───────────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_scan_all_rows() {
        let mut ctx = SessionContext::new();
        register_ci_collection(&mut ctx, "products", "product_id").await;

        let batches = query_all(
            &ctx,
            "SELECT product_id, name, category, price, in_stock FROM products ORDER BY product_id",
        )
        .await;
        assert!(
            total_rows(&batches) >= 5,
            "expected at least 5 seeded rows, got {}",
            total_rows(&batches)
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_scan_with_projection() {
        let mut ctx = SessionContext::new();
        register_ci_collection(&mut ctx, "products", "product_id").await;

        let batches = query_all(&ctx, "SELECT name FROM products ORDER BY product_id").await;
        assert!(
            total_rows(&batches) >= 5,
            "expected at least 5 seeded rows, got {}",
            total_rows(&batches)
        );
        assert_eq!(batches[0].num_columns(), 1);
    }

    #[tokio::test]
    #[ignore]
    async fn test_scan_with_filter() {
        let mut ctx = SessionContext::new();
        register_ci_collection(&mut ctx, "products", "product_id").await;

        let batches = query_all(
            &ctx,
            "SELECT product_id, name FROM products WHERE product_id = 'PROD001'",
        )
        .await;
        assert_eq!(total_rows(&batches), 1);

        let names = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Laptop");
    }

    #[tokio::test]
    #[ignore]
    async fn test_scan_with_limit() {
        let mut ctx = SessionContext::new();
        register_ci_collection(&mut ctx, "products", "product_id").await;

        let batches = query_all(&ctx, "SELECT product_id FROM products LIMIT 2").await;
        assert_eq!(total_rows(&batches), 2);
    }

    // ─── Insert test (integration) ──────────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_insert_into() {
        let mut ctx = SessionContext::new();
        register_ci_collection(&mut ctx, "products", "product_id").await;

        ctx.sql(
            "INSERT INTO products (product_id, name, category, price, in_stock)
             VALUES ('PROD_TEST_INS', 'TestProduct', 'TestCat', 49.99, true)",
        )
        .await
        .expect("parse insert")
        .collect()
        .await
        .expect("execute insert");

        let batches = query_all(
            &ctx,
            "SELECT product_id, name FROM products WHERE product_id = 'PROD_TEST_INS'",
        )
        .await;
        assert_eq!(total_rows(&batches), 1);
    }

    // ─── Delete tests (integration) ─────────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_delete_with_filter() {
        let mut ctx = SessionContext::new();
        register_ci_collection(&mut ctx, "products", "product_id").await;

        // Insert a row to delete
        ctx.sql(
            "INSERT INTO products (product_id, name, category, price, in_stock)
             VALUES ('PROD_DEL', 'DeleteMe', 'TestCat', 1.0, true)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

        let before = query_all(
            &ctx,
            "SELECT product_id FROM products WHERE product_id = 'PROD_DEL'",
        )
        .await;
        assert_eq!(total_rows(&before), 1);

        ctx.sql("DELETE FROM products WHERE product_id = 'PROD_DEL'")
            .await
            .expect("parse delete")
            .collect()
            .await
            .expect("execute delete");

        let after = query_all(
            &ctx,
            "SELECT product_id FROM products WHERE product_id = 'PROD_DEL'",
        )
        .await;
        assert_eq!(total_rows(&after), 0);
    }

    #[tokio::test]
    #[ignore]
    async fn test_delete_no_matching_rows() {
        let mut ctx = SessionContext::new();
        register_ci_collection(&mut ctx, "products", "product_id").await;

        // Verify a known row exists before and survives a no-op delete
        let before = query_all(
            &ctx,
            "SELECT product_id FROM products WHERE product_id = 'PROD001'",
        )
        .await;
        assert_eq!(total_rows(&before), 1);

        ctx.sql("DELETE FROM products WHERE product_id = 'NONEXISTENT'")
            .await
            .expect("parse delete")
            .collect()
            .await
            .expect("execute delete");

        let after = query_all(
            &ctx,
            "SELECT product_id FROM products WHERE product_id = 'PROD001'",
        )
        .await;
        assert_eq!(total_rows(&after), 1);
    }

    // ─── Update tests (integration) ─────────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_update_single_column_with_filter() {
        let mut ctx = SessionContext::new();
        register_ci_collection(&mut ctx, "products", "product_id").await;

        ctx.sql("UPDATE products SET price = 899.99 WHERE product_id = 'PROD001'")
            .await
            .expect("parse update")
            .collect()
            .await
            .expect("execute update");

        let batches = query_all(
            &ctx,
            "SELECT price FROM products WHERE product_id = 'PROD001'",
        )
        .await;
        assert_eq!(total_rows(&batches), 1);

        let prices = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((prices.value(0) - 899.99).abs() < 0.01);
    }

    #[tokio::test]
    #[ignore]
    async fn test_update_no_matching_rows() {
        let mut ctx = SessionContext::new();
        register_ci_collection(&mut ctx, "products", "product_id").await;

        // Verify a known row is unchanged after a no-op update
        let before = query_all(
            &ctx,
            "SELECT price FROM products WHERE product_id = 'PROD002'",
        )
        .await;
        assert_eq!(total_rows(&before), 1);
        let price_before = before[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);

        ctx.sql("UPDATE products SET price = 0.0 WHERE product_id = 'NONEXISTENT'")
            .await
            .expect("parse update")
            .collect()
            .await
            .expect("execute update");

        let after = query_all(
            &ctx,
            "SELECT price FROM products WHERE product_id = 'PROD002'",
        )
        .await;
        assert_eq!(total_rows(&after), 1);
        let price_after = after[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((price_before - price_after).abs() < 0.01);
    }

    #[tokio::test]
    #[ignore]
    async fn test_update_multiple_columns() {
        let mut ctx = SessionContext::new();
        register_ci_collection(&mut ctx, "products", "product_id").await;

        ctx.sql(
            "INSERT INTO products (product_id, name, category, price, in_stock)
             VALUES ('PROD_TEST_MULTI', 'MultiUpdate', 'TestCat', 11.0, true)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

        ctx.sql(
            "UPDATE products
             SET name = 'MultiUpdateRenamed',
                 in_stock = false
             WHERE product_id = 'PROD_TEST_MULTI'",
        )
        .await
        .expect("parse update")
        .collect()
        .await
        .expect("execute update");

        let batches = query_all(
            &ctx,
            "SELECT name, in_stock FROM products WHERE product_id = 'PROD_TEST_MULTI'",
        )
        .await;
        assert_eq!(total_rows(&batches), 1);

        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let in_stock = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(names.value(0), "MultiUpdateRenamed");
        assert!(!in_stock.value(0));

        ctx.sql("DELETE FROM products WHERE product_id = 'PROD_TEST_MULTI'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
    }

    // ─── Combined DML test (integration) ────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_insert_update_delete_round_trip() {
        let mut ctx = SessionContext::new();
        register_ci_collection(&mut ctx, "products", "product_id").await;

        // 1. Insert
        ctx.sql(
            "INSERT INTO products (product_id, name, category, price, in_stock)
             VALUES ('PROD_RT', 'RoundTrip', 'Test', 10.0, true)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        let after_insert = query_all(
            &ctx,
            "SELECT product_id FROM products WHERE product_id = 'PROD_RT'",
        )
        .await;
        assert_eq!(total_rows(&after_insert), 1);

        // 2. Update
        ctx.sql("UPDATE products SET price = 20.0, name = 'RoundTripUpdated' WHERE product_id = 'PROD_RT'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batches = query_all(
            &ctx,
            "SELECT name, price FROM products WHERE product_id = 'PROD_RT'",
        )
        .await;
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "RoundTripUpdated");

        // 3. Delete
        ctx.sql("DELETE FROM products WHERE product_id = 'PROD_RT'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let after_delete = query_all(
            &ctx,
            "SELECT product_id FROM products WHERE product_id = 'PROD_RT'",
        )
        .await;
        assert_eq!(total_rows(&after_delete), 0);
    }

    // ─── Category filter test (integration) ─────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_filter_by_category() {
        let mut ctx = SessionContext::new();
        register_ci_collection(&mut ctx, "products", "product_id").await;

        let batches = query_all(
            &ctx,
            "SELECT product_id, name FROM products WHERE category = 'Electronics' ORDER BY product_id",
        )
        .await;
        // PROD001..PROD004 are Electronics
        assert_eq!(total_rows(&batches), 4);
    }

    #[tokio::test]
    #[ignore]
    async fn test_filter_by_boolean() {
        let mut ctx = SessionContext::new();
        register_ci_collection(&mut ctx, "products", "product_id").await;

        let batches = query_all(
            &ctx,
            "SELECT product_id FROM products WHERE in_stock = false",
        )
        .await;
        // Only PROD003 (Monitor) is out of stock
        assert_eq!(total_rows(&batches), 1);
    }

    // ─── Aggregation test (integration) ─────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_aggregation_query() {
        let mut ctx = SessionContext::new();
        register_ci_collection(&mut ctx, "products", "product_id").await;

        let batches = query_all(
            &ctx,
            "SELECT category, COUNT(*) as cnt, SUM(price) as total
             FROM products
             GROUP BY category
             ORDER BY category",
        )
        .await;
        assert!(total_rows(&batches) >= 2); // at least Electronics and Furniture
    }

    // ─── Filter pushdown tests (integration) ────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_filter_pushdown_greater_than() {
        let mut ctx = SessionContext::new();
        register_ci_collection(&mut ctx, "products", "product_id").await;

        // price > 200 should match Laptop (999.99), Monitor (299.99)
        let batches = query_all(
            &ctx,
            "SELECT product_id, price FROM products WHERE price > 200 ORDER BY product_id",
        )
        .await;
        assert_eq!(total_rows(&batches), 2);
    }

    #[tokio::test]
    #[ignore]
    async fn test_filter_pushdown_less_than_equal() {
        let mut ctx = SessionContext::new();
        register_ci_collection(&mut ctx, "products", "product_id").await;

        // price <= 79.99 should match Keyboard (79.99), Mouse (29.99)
        let batches = query_all(
            &ctx,
            "SELECT product_id FROM products WHERE price <= 79.99 ORDER BY product_id",
        )
        .await;
        assert_eq!(total_rows(&batches), 2);
    }

    #[tokio::test]
    #[ignore]
    async fn test_filter_pushdown_not_equal() {
        let mut ctx = SessionContext::new();
        register_ci_collection(&mut ctx, "products", "product_id").await;

        // category != 'Electronics' should match only Desk Chair (Furniture)
        let batches = query_all(
            &ctx,
            "SELECT product_id FROM products WHERE category != 'Electronics'",
        )
        .await;
        assert_eq!(total_rows(&batches), 1);
    }

    #[tokio::test]
    #[ignore]
    async fn test_filter_pushdown_multiple_filters() {
        let mut ctx = SessionContext::new();
        register_ci_collection(&mut ctx, "products", "product_id").await;

        // category = 'Electronics' AND price > 100 should match Laptop (999.99), Monitor (299.99)
        let batches = query_all(
            &ctx,
            "SELECT product_id FROM products WHERE category = 'Electronics' AND price > 100 ORDER BY product_id",
        )
        .await;
        assert_eq!(total_rows(&batches), 2);
    }

    #[tokio::test]
    #[ignore]
    async fn test_limit_pushdown() {
        let mut ctx = SessionContext::new();
        register_ci_collection(&mut ctx, "products", "product_id").await;

        let batches = query_all(&ctx, "SELECT product_id FROM products LIMIT 3").await;
        assert_eq!(total_rows(&batches), 3);
    }

    #[tokio::test]
    #[ignore]
    async fn test_filter_and_limit_combined() {
        let mut ctx = SessionContext::new();
        register_ci_collection(&mut ctx, "products", "product_id").await;

        // 4 Electronics products, but LIMIT 2 should return only 2
        let batches = query_all(
            &ctx,
            "SELECT product_id FROM products WHERE category = 'Electronics' LIMIT 2",
        )
        .await;
        assert_eq!(total_rows(&batches), 2);
    }
}
