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
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
    stream::RecordBatchStreamAdapter,
};
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
/// Supports read (scan) and write (insert) operations
pub struct MongoTableProvider {
    collection: Collection<Document>,
    schema: SchemaRef,
    primary_key: String,
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

    async fn point_lookup(&self, key_value: &str) -> Result<Vec<Document>> {
        let filter = doc! { &self.primary_key: key_value };
        let mut cursor = self
            .collection
            .find(filter)
            .await
            .with_context(|| "Failed to execute point lookup")?;

        let mut results = vec![];
        while let Some(result) = cursor.next().await {
            let doc = result.with_context(|| "Failed to read document")?;
            results.push(doc);
        }
        Ok(results)
    }

    async fn full_scan(&self) -> Result<Vec<Document>> {
        let mut cursor = self
            .collection
            .find(doc! {})
            .await
            .with_context(|| "Failed to execute full scan")?;

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
                if is_primary_key_equality_filter(expr, &self.primary_key) {
                    TableProviderFilterPushDown::Exact
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
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let point_lookup_value = extract_primary_key_value(filters, &self.primary_key);

        let docs = if let Some(key_value) = point_lookup_value {
            tracing::debug!(
                "MongoDB point lookup on {}={} for collection {}",
                self.primary_key,
                key_value,
                self.collection_name
            );
            self.point_lookup(&key_value)
                .await
                .map_err(|e| DataFusionError::External(e.into()))?
        } else {
            tracing::debug!(
                "MongoDB full scan for collection {} (no point lookup filter)",
                self.collection_name
            );
            self.full_scan()
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

        Ok(Arc::new(MongoExecPlan {
            schema: batch.schema(),
            batch: Arc::new(RwLock::new(Some(batch))),
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
}

fn is_primary_key_equality_filter(expr: &Expr, primary_key: &str) -> bool {
    match expr {
        Expr::BinaryExpr(binary) => {
            if binary.op == datafusion::logical_expr::Operator::Eq {
                match (binary.left.as_ref(), binary.right.as_ref()) {
                    (Expr::Column(col), Expr::Literal(..))
                    | (Expr::Literal(..), Expr::Column(col)) => col.name == primary_key,
                    _ => false,
                }
            } else {
                false
            }
        }
        _ => false,
    }
}

fn extract_primary_key_value(filters: &[Expr], primary_key: &str) -> Option<String> {
    for filter in filters {
        if let Expr::BinaryExpr(binary) = filter {
            if binary.op == datafusion::logical_expr::Operator::Eq {
                match (binary.left.as_ref(), binary.right.as_ref()) {
                    (Expr::Column(col), Expr::Literal(lit, _)) if col.name == primary_key => {
                        return Some(lit.to_string().trim_matches('\'').to_string());
                    }
                    (Expr::Literal(lit, _), Expr::Column(col)) if col.name == primary_key => {
                        return Some(lit.to_string().trim_matches('\'').to_string());
                    }
                    _ => {}
                }
            }
        }
    }
    None
}

fn bson_to_arrow_type(value: &Bson) -> DataType {
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

fn bson_values_to_arrow_array(values: &[Option<Bson>], data_type: &DataType) -> ArrayRef {
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

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        static PROPS: std::sync::OnceLock<datafusion::physical_plan::PlanProperties> =
            std::sync::OnceLock::new();
        PROPS.get_or_init(|| {
            datafusion::physical_plan::PlanProperties::new(
                datafusion::physical_expr::EquivalenceProperties::new(self.schema.clone()),
                datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
                datafusion::physical_plan::execution_plan::EmissionType::Final,
                datafusion::physical_plan::execution_plan::Boundedness::Bounded,
            )
        })
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

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
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
}
