//! Amazon DynamoDB table provider for DataFusion.
//!
//! DynamoDB is a managed NoSQL document/key-value store. A DynamoDB table maps
//! to one Skardi table: each item becomes a row and each top-level attribute a
//! column. The shape mirrors the MongoDB provider (`super::mongo`) — a sample
//! item drives schema inference, binary filters push down into the backend, and
//! INSERT/UPDATE/DELETE are supported because DynamoDB is a read-write store.
//!
//! DynamoDB has no native full-text or vector search, so no FTS/KNN table
//! functions are registered (unlike Lance or the pgvector-backed Postgres
//! provider).

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, RecordBatchOptions,
    StringArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::config::{Credentials, Region};
use aws_sdk_dynamodb::types::{
    AttributeValue, DeleteRequest, KeyType, PutRequest, Select, WriteRequest,
};
use datafusion::catalog::Session;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use futures::stream::{self, Stream};

use super::is_pushable_binary_filter;
use crate::sources::DataSourceType;
use crate::sources::hierarchy::{HierarchyLevel, SourceLabel, retry_with_timeout};

/// Schema name used for DynamoDB catalog mode. DynamoDB has no native schema/database
/// layer; all tables live under a single endpoint/region. We expose them under a fixed
/// schema so SQL references are consistently three-part: `catalog.tables.<table>`.
const DYNAMODB_CATALOG_SCHEMA: &str = "tables";

/// Maximum items sampled to infer a schema when no explicit `columns` option is
/// given. Merging several items (rather than one) makes the inferred column set
/// deterministic and complete enough for most tables.
const SCHEMA_SAMPLE_SIZE: i32 = 50;

/// DynamoDB `BatchWriteItem` accepts at most 25 write requests per call.
const BATCH_WRITE_CHUNK: usize = 25;

/// Maximum tables returned per `ListTables` page. AWS allows up to 100; we use the
/// default and follow `last_evaluated_table_name` when present.
const LIST_TABLES_PAGE_SIZE: i32 = 100;

/// Upper bound on concurrent DynamoDB table introspection during catalog registration.
const DYNAMODB_CATALOG_BUILD_CONCURRENCY: usize = 8;

/// A DynamoDB table exposed to DataFusion as a `TableProvider`.
pub struct DynamoTableProvider {
    client: Client,
    table_name: String,
    schema: SchemaRef,
    /// Partition (hash) key attribute name. Always present and not nullable.
    partition_key: String,
    /// Sort (range) key attribute name, if the table has a composite key.
    sort_key: Option<String>,
    /// When false, INSERT/UPDATE/DELETE are rejected at plan time so a
    /// `read_only` source can never mutate a live table.
    read_write: bool,
}

impl Debug for DynamoTableProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamoTableProvider")
            .field("table_name", &self.table_name)
            .field("partition_key", &self.partition_key)
            .field("sort_key", &self.sort_key)
            .field("read_write", &self.read_write)
            .field("schema", &self.schema)
            .finish()
    }
}

impl DynamoTableProvider {
    /// Build a provider against an existing DynamoDB table, inferring the schema
    /// from sampled items unless one is supplied. `read_write` gates the DML
    /// methods; a read-only provider rejects INSERT/UPDATE/DELETE at plan time.
    pub async fn new(
        client: Client,
        table_name: &str,
        partition_key: &str,
        sort_key: Option<&str>,
        schema: Option<SchemaRef>,
        read_write: bool,
    ) -> Result<Self> {
        let schema = match schema {
            Some(s) => s,
            None => {
                Arc::new(Self::infer_schema(&client, table_name, partition_key, sort_key).await?)
            }
        };

        Ok(Self {
            client,
            table_name: table_name.to_string(),
            schema,
            partition_key: partition_key.to_string(),
            sort_key: sort_key.map(str::to_string),
            read_write,
        })
    }

    /// Infer an Arrow schema by sampling several items and merging their
    /// attribute sets. The partition key (then the sort key, if any) are always
    /// emitted first and non-nullable; remaining attributes are inferred from
    /// the samples and marked nullable, since DynamoDB items are schemaless and
    /// any attribute may be absent on other items.
    async fn infer_schema(
        client: &Client,
        table_name: &str,
        partition_key: &str,
        sort_key: Option<&str>,
    ) -> Result<Schema> {
        let sample = client
            .scan()
            .table_name(table_name)
            .limit(SCHEMA_SAMPLE_SIZE)
            .send()
            .await
            .with_context(|| format!("Failed to sample DynamoDB table '{table_name}'"))?;

        let items = sample.items();
        if items.is_empty() {
            tracing::warn!(
                table = %table_name,
                "DynamoDB table is empty; schema limited to declared key attributes"
            );
        }

        let attrs = merge_sampled_attributes(items);

        Ok(build_schema_fields(partition_key, sort_key, &attrs))
    }

    /// Pick the cheapest physical read strategy the pushable predicates allow.
    fn plan_read(&self, pushable: &[Expr]) -> DFResult<DynamoRead> {
        classify_read(&self.partition_key, self.sort_key.as_deref(), pushable)
    }
}

/// Merge attributes across sampled items; the first observation of an
/// attribute fixes its type. The SDK item maps iterate in arbitrary order, so
/// the merged list is sorted by name to give a deterministic non-key column
/// order across runs (keys are placed explicitly downstream).
fn merge_sampled_attributes(items: &[HashMap<String, AttributeValue>]) -> Vec<(String, DataType)> {
    let mut attrs: Vec<(String, DataType)> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for item in items {
        for (key, value) in item.iter() {
            if seen.insert(key.clone()) {
                attrs.push((key.clone(), attribute_value_to_arrow_type(value)));
            }
        }
    }
    attrs.sort_by(|(a, _), (b, _)| a.cmp(b));
    attrs
}

/// Build an ordered field list: key attributes first (non-nullable, in key
/// order), then every other attribute (nullable). Shared by schema inference
/// and the explicit `columns` option so both produce identical shapes.
fn build_schema_fields(
    partition_key: &str,
    sort_key: Option<&str>,
    attrs: &[(String, DataType)],
) -> Schema {
    let type_of = |name: &str| {
        attrs
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, t)| t.clone())
            .unwrap_or(DataType::Utf8)
    };

    let mut fields: Vec<Field> = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();

    fields.push(Field::new(partition_key, type_of(partition_key), false));
    seen.insert(partition_key);
    if let Some(sk) = sort_key {
        fields.push(Field::new(sk, type_of(sk), false));
        seen.insert(sk);
    }
    for (name, dtype) in attrs {
        if seen.insert(name.as_str()) {
            fields.push(Field::new(name, dtype.clone(), true));
        }
    }
    Schema::new(fields)
}

/// Convert a page of DynamoDB items into an Arrow `RecordBatch` shaped by
/// `schema`. Consumes the items (attribute values are moved, not cloned) and
/// fills missing attributes with NULL.
fn items_to_batch(
    items: Vec<HashMap<String, AttributeValue>>,
    schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    let n_rows = items.len();
    let mut columns: Vec<Vec<Option<AttributeValue>>> = schema
        .fields()
        .iter()
        .map(|_| Vec::with_capacity(n_rows))
        .collect();

    for mut item in items {
        for (idx, field) in schema.fields().iter().enumerate() {
            columns[idx].push(item.remove(field.name()));
        }
    }

    let arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .zip(columns.iter())
        .map(|(field, values)| attribute_values_to_arrow_array(values, field.data_type()))
        .collect();

    let options = RecordBatchOptions::new().with_row_count(Some(n_rows));
    RecordBatch::try_new_with_options(schema.clone(), arrays, &options)
        .map_err(DataFusionError::from)
}

#[async_trait]
impl TableProvider for DynamoTableProvider {
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
                    // Inexact (not Exact) keeps the filter in the logical plan so
                    // DataFusion's UPDATE/DELETE planner can still hand it to
                    // delete_from/update — matching the MongoDB provider.
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
        // Only push filters we can actually convert. A pushable-shaped predicate
        // with an inconvertible literal (e.g. a Timestamp) is skipped rather than
        // failing the whole query — safe because pushdown is Inexact, so
        // DataFusion re-applies every predicate after the fetch.
        let pushable: Vec<Expr> = filters
            .iter()
            .filter(|e| is_convertible_pushdown(e))
            .cloned()
            .collect();
        let read = self.plan_read(&pushable)?;

        // Empty projection (e.g. `count(*)`) means nothing above the scan
        // references any column — including filters, so there are none — and we
        // can read counts server-side. A non-empty projection is fetched with a
        // ProjectionExpression so only those attributes cross the wire.
        let (output_schema, projection_expr, count_only) = match projection {
            Some(p) if p.is_empty() => (Arc::new(self.schema.project(&[])?), None, true),
            Some(p) => {
                let ps = Arc::new(self.schema.project(p)?);
                let pe = build_projection_expression(&ps);
                (ps, Some(pe), false)
            }
            None => {
                let pe = build_projection_expression(&self.schema);
                (self.schema.clone(), Some(pe), false)
            }
        };

        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Arc::new(DynamoScanExec {
            client: self.client.clone(),
            table_name: self.table_name.clone(),
            output_schema,
            read,
            projection_expr,
            count_only,
            limit,
            properties,
        }))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: datafusion::logical_expr::dml::InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !self.read_write {
            return Err(read_only_error("INSERT", &self.table_name));
        }
        use datafusion::logical_expr::dml::InsertOp;
        // Plain INSERT (Append) must not clobber an existing item — DynamoDB
        // PutItem is an upsert, so a duplicate key would silently replace the
        // whole item. Overwrite/Replace opt into upsert semantics.
        let upsert = matches!(insert_op, InsertOp::Overwrite | InsertOp::Replace);
        Ok(Arc::new(DynamoInsertExec {
            input,
            client: self.client.clone(),
            table_name: self.table_name.clone(),
            schema: self.schema.clone(),
            properties: count_plan_properties(),
            partition_key: self.partition_key.clone(),
            upsert,
        }))
    }

    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !self.read_write {
            return Err(read_only_error("DELETE", &self.table_name));
        }
        let plan = self.plan_dml(&filters)?;
        Ok(Arc::new(DynamoDmlExec::new(
            self.clone_handle(),
            DynamoDmlOp::Delete { plan },
        )))
    }

    async fn update(
        &self,
        _state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !self.read_write {
            return Err(read_only_error("UPDATE", &self.table_name));
        }
        if assignments.is_empty() {
            return Err(DataFusionError::Plan(
                "UPDATE requires at least one assignment".to_string(),
            ));
        }
        let mut sets: Vec<(String, AttributeValue)> = Vec::with_capacity(assignments.len());
        for (col, expr) in &assignments {
            if col == &self.partition_key || Some(col) == self.sort_key.as_ref() {
                return Err(DataFusionError::Plan(format!(
                    "Cannot modify key column '{col}' — DynamoDB key attributes are immutable"
                )));
            }
            sets.push((col.clone(), expr_to_attribute_value(expr)?));
        }

        let plan = self.plan_dml(&filters)?;
        Ok(Arc::new(DynamoDmlExec::new(
            self.clone_handle(),
            DynamoDmlOp::Update { plan, sets },
        )))
    }
}

impl DynamoTableProvider {
    /// A cheap clone of the connection handle and key metadata for the DML
    /// execution plans (the schema isn't needed there).
    fn clone_handle(&self) -> DynamoHandle {
        DynamoHandle {
            client: self.client.clone(),
            table_name: self.table_name.clone(),
            partition_key: self.partition_key.clone(),
            sort_key: self.sort_key.clone(),
        }
    }

    /// Plan a DELETE/UPDATE against the key schema.
    ///
    /// DynamoDB cannot mutate by arbitrary predicate, so we first resolve the
    /// matching keys. Every WHERE predicate must be a convertible pushable
    /// comparison: otherwise the residual predicate could not be applied
    /// server-side and we would mutate rows it should have excluded. We refuse
    /// rather than silently over-delete (`DELETE WHERE a = 1 OR b = 2` must not
    /// wipe the table).
    fn plan_dml(&self, filters: &[Expr]) -> DFResult<DmlKeyPlan> {
        if let Some(bad) = filters.iter().find(|e| !is_convertible_pushdown(e)) {
            return Err(DataFusionError::Plan(format!(
                "DynamoDB DELETE/UPDATE requires every WHERE predicate to be a pushable comparison \
                 (a column compared to a literal via =, <>, <, <=, >, >=). Unsupported predicate: {bad}. \
                 Refusing to run to avoid mutating rows the predicate should have excluded."
            )));
        }
        classify_dml(&self.partition_key, self.sort_key.as_deref(), filters)
    }
}

/// Error returned when a write is attempted against a read-only source.
fn read_only_error(op: &str, table: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "{op} not allowed on DynamoDB table '{table}': the data source is configured read_only. \
         Set access_mode: read_write to enable write operations."
    ))
}

/// Connection handle plus key metadata shared with DML execution plans.
#[derive(Clone)]
struct DynamoHandle {
    client: Client,
    table_name: String,
    partition_key: String,
    sort_key: Option<String>,
}

impl DynamoHandle {
    fn key_of(
        &self,
        item: &HashMap<String, AttributeValue>,
    ) -> Result<HashMap<String, AttributeValue>> {
        let mut key = HashMap::new();
        let pk = item.get(&self.partition_key).cloned().ok_or_else(|| {
            anyhow::anyhow!("item missing partition key '{}'", self.partition_key)
        })?;
        key.insert(self.partition_key.clone(), pk);
        if let Some(sk) = &self.sort_key {
            let sk_val = item
                .get(sk)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("item missing sort key '{sk}'"))?;
            key.insert(sk.clone(), sk_val);
        }
        Ok(key)
    }

    /// A `ProjectionExpression` (plus its name bindings) that fetches only the
    /// key attributes — all the DML paths need, since they mutate by key.
    fn key_projection(&self) -> (String, HashMap<String, String>) {
        let mut names = HashMap::new();
        let mut parts = vec!["#p0".to_string()];
        names.insert("#p0".to_string(), self.partition_key.clone());
        if let Some(sk) = &self.sort_key {
            names.insert("#p1".to_string(), sk.clone());
            parts.push("#p1".to_string());
        }
        (parts.join(", "), names)
    }

    /// Resolve the keys of items matching a DML plan, routing to Query when the
    /// partition key is pinned (any residual predicate is applied server-side as
    /// a `FilterExpression`) and falling back to a full Scan otherwise — instead
    /// of always scanning the whole table.
    async fn matching_keys(
        &self,
        plan: &DmlKeyPlan,
    ) -> Result<Vec<HashMap<String, AttributeValue>>> {
        let source = match plan {
            DmlKeyPlan::Query {
                key_condition,
                residual,
            } => PageSource::Query {
                key_condition: key_condition.clone(),
                filter: residual.clone(),
            },
            DmlKeyPlan::Scan { filter } => PageSource::Scan {
                filter: filter.clone(),
            },
        };
        let key_projection = self.key_projection();

        let mut keys = Vec::new();
        let mut start_key: Option<HashMap<String, AttributeValue>> = None;
        loop {
            let (items, last) = fetch_page(
                &self.client,
                &self.table_name,
                &source,
                Some(&key_projection),
                start_key.take(),
                None,
            )
            .await?;
            for item in &items {
                keys.push(self.key_of(item)?);
            }
            match last {
                Some(k) => start_key = Some(k),
                None => break,
            }
        }
        Ok(keys)
    }

    /// Delete a set of keys via `BatchWriteItem` (25 per request), retrying any
    /// unprocessed keys. Returns the number of keys submitted (all of which
    /// correspond to matched items).
    async fn batch_delete(&self, keys: Vec<HashMap<String, AttributeValue>>) -> Result<u64> {
        let total = keys.len() as u64;
        for chunk in keys.chunks(BATCH_WRITE_CHUNK) {
            let requests: Vec<WriteRequest> = chunk
                .iter()
                .map(|k| {
                    let del = DeleteRequest::builder()
                        .set_key(Some(k.clone()))
                        .build()
                        .expect("DeleteRequest requires only a key, which is set");
                    WriteRequest::builder().delete_request(del).build()
                })
                .collect();
            batch_write(&self.client, &self.table_name, requests).await?;
        }
        Ok(total)
    }
}

/// Submit one chunk of write requests, retrying `UnprocessedItems` (which
/// DynamoDB returns under throttling) up to a bounded number of times.
async fn batch_write(client: &Client, table: &str, requests: Vec<WriteRequest>) -> Result<()> {
    let mut pending: HashMap<String, Vec<WriteRequest>> =
        HashMap::from([(table.to_string(), requests)]);
    for _ in 0..8 {
        let out = client
            .batch_write_item()
            .set_request_items(Some(pending))
            .send()
            .await
            .with_context(|| format!("DynamoDB batch_write_item failed for '{table}'"))?;
        match out.unprocessed_items {
            Some(u) if !u.is_empty() => pending = u,
            _ => return Ok(()),
        }
    }
    anyhow::bail!("DynamoDB batch_write_item left items unprocessed after retries for '{table}'")
}

// ─── Schema inference & type mapping ────────────────────────────────────────

/// Map a sampled DynamoDB attribute to an Arrow type. Numbers are always
/// inferred as `Float64`: a single sampled item can't prove a column is
/// integer-only, and a later fractional value would otherwise be silently
/// truncated (or drop the row via the Inexact re-filter). Everything non-scalar
/// (maps, lists, sets, binary) falls back to `Utf8`.
pub(crate) fn attribute_value_to_arrow_type(value: &AttributeValue) -> DataType {
    match value {
        AttributeValue::S(_) => DataType::Utf8,
        AttributeValue::Bool(_) => DataType::Boolean,
        AttributeValue::N(_) => DataType::Float64,
        AttributeValue::Null(_) => DataType::Utf8,
        _ => DataType::Utf8,
    }
}

/// Build a typed Arrow array from a column of optional DynamoDB attributes.
pub(crate) fn attribute_values_to_arrow_array(
    values: &[Option<AttributeValue>],
    data_type: &DataType,
) -> ArrayRef {
    match data_type {
        DataType::Utf8 => {
            let arr: StringArray = values
                .iter()
                .map(|v| v.as_ref().and_then(av_to_string))
                .collect();
            Arc::new(arr)
        }
        DataType::Int32 => {
            let arr: Int32Array = values
                .iter()
                .map(|v| v.as_ref().and_then(av_to_i64).map(|n| n as i32))
                .collect();
            Arc::new(arr)
        }
        DataType::Int64 => {
            let arr: Int64Array = values
                .iter()
                .map(|v| v.as_ref().and_then(av_to_i64))
                .collect();
            Arc::new(arr)
        }
        DataType::Float64 => {
            let arr: Float64Array = values
                .iter()
                .map(|v| v.as_ref().and_then(av_to_f64))
                .collect();
            Arc::new(arr)
        }
        DataType::Boolean => {
            let arr: BooleanArray = values
                .iter()
                .map(|v| v.as_ref().and_then(av_to_bool))
                .collect();
            Arc::new(arr)
        }
        _ => {
            let arr: StringArray = values
                .iter()
                .map(|v| v.as_ref().and_then(av_to_string))
                .collect();
            Arc::new(arr)
        }
    }
}

/// Render a scalar attribute as the string form for a Utf8 column. An explicit
/// DynamoDB `NULL` maps to `None` (Arrow null) rather than an empty string, so a
/// SQL NULL written via `UPDATE ... SET col = NULL` round-trips as NULL instead
/// of `""`.
fn av_to_string(v: &AttributeValue) -> Option<String> {
    match v {
        AttributeValue::S(s) => Some(s.clone()),
        AttributeValue::N(n) => Some(n.clone()),
        AttributeValue::Bool(b) => Some(b.to_string()),
        AttributeValue::Null(_) => None,
        other => Some(format!("{other:?}")),
    }
}

/// Coerce to `i64` only from an exactly-integer `N`. A fractional value
/// (`N("7.9")`) or a string (`S("7")`) yields `None` (SQL NULL) rather than a
/// silently truncated or cross-type value — which would violate the Inexact
/// pushdown superset contract, since DynamoDB's server-side filter is type- and
/// value-strict.
fn av_to_i64(v: &AttributeValue) -> Option<i64> {
    match v {
        AttributeValue::N(n) => n.parse::<i64>().ok(),
        _ => None,
    }
}

/// Coerce to `f64` only from a numeric `N`. A string is not coerced (see
/// `av_to_i64` for why cross-type coercion is unsafe under Inexact pushdown).
fn av_to_f64(v: &AttributeValue) -> Option<f64> {
    match v {
        AttributeValue::N(n) => n.parse::<f64>().ok(),
        _ => None,
    }
}

fn av_to_bool(v: &AttributeValue) -> Option<bool> {
    match v {
        AttributeValue::Bool(b) => Some(*b),
        _ => None,
    }
}

/// Convert one Arrow cell into a DynamoDB attribute. Returns `None` for NULLs so
/// the caller can omit the attribute (DynamoDB has no typed NULL columns).
fn arrow_value_to_attribute(
    array: &ArrayRef,
    row: usize,
    data_type: &DataType,
) -> Result<Option<AttributeValue>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let value = match data_type {
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .with_context(|| "expected StringArray for DataType::Utf8")?;
            AttributeValue::S(arr.value(row).to_string())
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .with_context(|| "expected Int32Array for DataType::Int32")?;
            AttributeValue::N(arr.value(row).to_string())
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .with_context(|| "expected Int64Array for DataType::Int64")?;
            AttributeValue::N(arr.value(row).to_string())
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .with_context(|| "expected Float64Array for DataType::Float64")?;
            AttributeValue::N(arr.value(row).to_string())
        }
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .with_context(|| "expected BooleanArray for DataType::Boolean")?;
            AttributeValue::Bool(arr.value(row))
        }
        _ => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .with_context(|| "unsupported Arrow type for DynamoDB write")?;
            AttributeValue::S(arr.value(row).to_string())
        }
    };
    Ok(Some(value))
}

fn record_batch_to_items(
    batch: &RecordBatch,
    schema: &Schema,
) -> Result<Vec<HashMap<String, AttributeValue>>> {
    let mut items = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut item = HashMap::new();
        for (idx, field) in schema.fields().iter().enumerate() {
            let array = batch.column(idx);
            if let Some(v) = arrow_value_to_attribute(array, row, field.data_type())? {
                item.insert(field.name().clone(), v);
            }
        }
        items.push(item);
    }
    Ok(items)
}

// ─── Filter pushdown ────────────────────────────────────────────────────────

/// A DynamoDB `FilterExpression` plus its attribute-name/value placeholder maps.
#[derive(Clone, Debug)]
struct DynamoFilter {
    expression: String,
    names: HashMap<String, String>,
    values: HashMap<String, AttributeValue>,
}

/// A pushable-shaped filter (see `is_pushable_binary_filter`) whose literal can
/// also be converted to a DynamoDB attribute value. The shape check alone is not
/// enough: a comparison against e.g. a Timestamp literal is pushable-shaped but
/// not convertible, and pushing it would fail the request.
fn is_convertible_pushdown(expr: &Expr) -> bool {
    is_pushable_binary_filter(expr) && normalize_binary(expr).is_some()
}

/// Convert a list of pushable binary filters into one ANDed DynamoDB
/// `FilterExpression`. Attribute names and values are passed as `#n`/`:v`
/// placeholders so reserved words and types are handled safely.
fn build_filter_expression(filters: &[Expr]) -> DFResult<Option<DynamoFilter>> {
    if filters.is_empty() {
        return Ok(None);
    }
    let mut parts = Vec::with_capacity(filters.len());
    let mut names = HashMap::new();
    let mut values = HashMap::new();

    for (i, expr) in filters.iter().enumerate() {
        let Expr::BinaryExpr(binary) = expr else {
            return Err(DataFusionError::Plan(format!(
                "Unsupported DynamoDB filter expression: {expr}"
            )));
        };
        let (col, value_expr, flipped) = match (binary.left.as_ref(), binary.right.as_ref()) {
            (Expr::Column(c), v) => (c.name.clone(), v, false),
            (v, Expr::Column(c)) => (c.name.clone(), v, true),
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "DynamoDB filter must compare a column to a literal, got: {expr}"
                )));
            }
        };
        // If the literal is on the left (`5 < col`), invert the operator so the
        // emitted expression keeps `#name <op> :val` form.
        let op = if flipped {
            flip_operator(binary.op)
        } else {
            binary.op
        };
        let name_ph = format!("#n{i}");
        let val_ph = format!(":v{i}");
        names.insert(name_ph.clone(), col);
        values.insert(val_ph.clone(), expr_to_attribute_value(value_expr)?);
        parts.push(format!("{name_ph} {} {val_ph}", operator_symbol(op)?));
    }

    Ok(Some(DynamoFilter {
        expression: parts.join(" AND "),
        names,
        values,
    }))
}

fn flip_operator(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other, // Eq / NotEq are symmetric
    }
}

fn operator_symbol(op: Operator) -> DFResult<&'static str> {
    match op {
        Operator::Eq => Ok("="),
        Operator::NotEq => Ok("<>"),
        Operator::Lt => Ok("<"),
        Operator::LtEq => Ok("<="),
        Operator::Gt => Ok(">"),
        Operator::GtEq => Ok(">="),
        other => Err(DataFusionError::Plan(format!(
            "Unsupported DynamoDB filter operator: {other}"
        ))),
    }
}

/// Build a `ProjectionExpression` naming exactly the fields in `schema`, using a
/// `#p` placeholder namespace (distinct from filter `#n`/`:v` and key-condition
/// `#k`/`:k`) so it can be merged onto the same request without collision.
fn build_projection_expression(schema: &Schema) -> (String, HashMap<String, String>) {
    let mut names = HashMap::new();
    let mut parts = Vec::with_capacity(schema.fields().len());
    for (i, field) in schema.fields().iter().enumerate() {
        let ph = format!("#p{i}");
        names.insert(ph.clone(), field.name().clone());
        parts.push(ph);
    }
    (parts.join(", "), names)
}

// ─── Key-aware read planning ────────────────────────────────────────────────

/// How a `scan()` should physically read DynamoDB given its pushable filters.
#[derive(Clone, Debug)]
enum DynamoRead {
    /// Full primary key pinned by equality — a single-item `GetItem`.
    GetItem {
        key: HashMap<String, AttributeValue>,
    },
    /// Partition key pinned by equality, with an optional sort-key condition —
    /// a `Query` against the key schema.
    Query { key_condition: DynamoFilter },
    /// No usable key constraint — a full `Scan` with an optional filter.
    Scan { filter: Option<DynamoFilter> },
}

/// Operators DynamoDB accepts in a `KeyConditionExpression` on the sort key.
/// `NotEq` (`<>`) is intentionally excluded — it is pushable as a regular filter
/// but illegal on a key.
fn is_key_condition_op(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    )
}

/// Normalize a pushable binary filter into `(column, operator, value)` with the
/// column on the left (operator flipped if the literal was on the left).
fn normalize_binary(expr: &Expr) -> Option<(String, Operator, AttributeValue)> {
    let Expr::BinaryExpr(binary) = expr else {
        return None;
    };
    let (col, value_expr, flipped) = match (binary.left.as_ref(), binary.right.as_ref()) {
        (Expr::Column(c), v) => (c.name.clone(), v, false),
        (v, Expr::Column(c)) => (c.name.clone(), v, true),
        _ => return None,
    };
    let op = if flipped {
        flip_operator(binary.op)
    } else {
        binary.op
    };
    Some((col, op, expr_to_attribute_value(value_expr).ok()?))
}

/// Build a DynamoDB `KeyConditionExpression` (`#k0 = :k0 [AND #k1 <op> :k1]`).
/// Uses a `#k`/`:k` placeholder namespace distinct from the `#n`/`:v` used by
/// filter expressions, so the two never collide if combined on one request.
fn build_key_condition(
    partition_key: &str,
    partition_value: AttributeValue,
    sort_key: &str,
    sort_cond: Option<(Operator, AttributeValue)>,
) -> DynamoFilter {
    let mut names = HashMap::new();
    let mut values = HashMap::new();
    names.insert("#k0".to_string(), partition_key.to_string());
    values.insert(":k0".to_string(), partition_value);
    let mut expression = "#k0 = :k0".to_string();

    if let Some((op, value)) = sort_cond {
        names.insert("#k1".to_string(), sort_key.to_string());
        values.insert(":k1".to_string(), value);
        let symbol =
            operator_symbol(op).expect("sort condition op validated by is_key_condition_op");
        expression.push_str(&format!(" AND #k1 {symbol} :k1"));
    }

    DynamoFilter {
        expression,
        names,
        values,
    }
}

/// Classify pushable filters into the cheapest DynamoDB access pattern.
///
/// Only the *key* portion of the predicate set drives the choice; any other
/// predicate is left for DataFusion to re-apply (filters are pushed `Inexact`),
/// so the result is always correct regardless of which path is chosen.
fn classify_read(
    partition_key: &str,
    sort_key: Option<&str>,
    pushable: &[Expr],
) -> DFResult<DynamoRead> {
    let mut partition_value: Option<AttributeValue> = None;
    let mut sort_cond: Option<(Operator, AttributeValue)> = None;

    for expr in pushable {
        let Some((col, op, value)) = normalize_binary(expr) else {
            continue;
        };
        if col == partition_key {
            // The partition key only narrows the access pattern under equality.
            if op == Operator::Eq && partition_value.is_none() {
                partition_value = Some(value);
            }
        } else if Some(col.as_str()) == sort_key && is_key_condition_op(op) && sort_cond.is_none() {
            sort_cond = Some((op, value));
        }
    }

    let Some(partition_value) = partition_value else {
        // Partition key not pinned by equality → must Scan.
        return Ok(DynamoRead::Scan {
            filter: build_filter_expression(pushable)?,
        });
    };

    match (sort_key, sort_cond) {
        // Single-key table: the partition key IS the full primary key → GetItem.
        (None, _) => {
            let mut key = HashMap::new();
            key.insert(partition_key.to_string(), partition_value);
            Ok(DynamoRead::GetItem { key })
        }
        // Composite key fully pinned by equality → GetItem.
        (Some(sk), Some((Operator::Eq, sort_value))) => {
            let mut key = HashMap::new();
            key.insert(partition_key.to_string(), partition_value);
            key.insert(sk.to_string(), sort_value);
            Ok(DynamoRead::GetItem { key })
        }
        // Composite key with a sort-key range (or no sort constraint) → Query.
        (Some(sk), sort_cond) => Ok(DynamoRead::Query {
            key_condition: build_key_condition(partition_key, partition_value, sk, sort_cond),
        }),
    }
}

/// How a DELETE/UPDATE should resolve the keys it will mutate. Unlike the read
/// path (whose residual predicates DataFusion re-applies via Inexact pushdown),
/// DML must apply *every* predicate server-side, so any residual non-key
/// predicate travels as a `FilterExpression`.
#[derive(Clone, Debug)]
enum DmlKeyPlan {
    /// Partition key pinned by equality → `Query` (with an optional sort-key
    /// condition folded in), plus a residual `FilterExpression` over non-key
    /// attributes. Far cheaper than scanning the whole table.
    Query {
        key_condition: DynamoFilter,
        residual: Option<DynamoFilter>,
    },
    /// Partition key not pinned (or an inexpressible sort predicate) → full
    /// `Scan` carrying every predicate as a `FilterExpression`.
    Scan { filter: Option<DynamoFilter> },
}

/// Plan the key-resolution strategy for a DELETE/UPDATE. Callers must have
/// already ensured every predicate is a convertible pushable comparison (see
/// `plan_dml`), so nothing is silently dropped.
///
/// We can drive a `Query` only when the partition key is pinned by equality and
/// every sort-key predicate is expressible on the key: a `Query`'s
/// `FilterExpression` may not reference key attributes, and its
/// `KeyConditionExpression` allows at most one sort-key condition using a legal
/// operator (so `sk <> …`, or two sort predicates, force a `Scan`). A `Scan`'s
/// filter, by contrast, may reference keys, so it always expresses the full
/// predicate set.
fn classify_dml(
    partition_key: &str,
    sort_key: Option<&str>,
    filters: &[Expr],
) -> DFResult<DmlKeyPlan> {
    let mut partition_value: Option<AttributeValue> = None;
    let mut partition_query_ok = true;
    let mut sort_conds: Vec<(Operator, AttributeValue)> = Vec::new();
    let mut sort_key_expressible = true;
    let mut residual: Vec<Expr> = Vec::new();

    for expr in filters {
        let Some((col, op, value)) = normalize_binary(expr) else {
            // Unreachable after plan_dml's guard, but stay safe: force a Scan.
            partition_query_ok = false;
            residual.push(expr.clone());
            continue;
        };
        if col == partition_key {
            if op == Operator::Eq && partition_value.is_none() {
                partition_value = Some(value);
            } else {
                // A non-equality (or duplicate) partition predicate can't drive
                // a Query and can't live in a Query filter → must Scan.
                partition_query_ok = false;
            }
        } else if Some(col.as_str()) == sort_key {
            if !is_key_condition_op(op) {
                sort_key_expressible = false;
            }
            sort_conds.push((op, value));
        } else {
            residual.push(expr.clone());
        }
    }

    let can_query = partition_query_ok
        && partition_value.is_some()
        && sort_conds.len() <= 1
        && sort_key_expressible;

    if can_query {
        let partition_value = partition_value.expect("checked by can_query");
        let sort_cond = sort_conds.into_iter().next();
        let key_condition = build_key_condition(
            partition_key,
            partition_value,
            sort_key.unwrap_or(""),
            sort_cond,
        );
        Ok(DmlKeyPlan::Query {
            key_condition,
            residual: build_filter_expression(&residual)?,
        })
    } else {
        // Scan carries every predicate; a Scan filter may reference key columns.
        Ok(DmlKeyPlan::Scan {
            filter: build_filter_expression(filters)?,
        })
    }
}

/// Convert a DataFusion literal expression into a DynamoDB attribute value.
pub(crate) fn expr_to_attribute_value(expr: &Expr) -> DFResult<AttributeValue> {
    match expr {
        Expr::Literal(scalar, _) => scalar_to_attribute_value(scalar),
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported expression for DynamoDB value: {expr}"
        ))),
    }
}

fn scalar_to_attribute_value(scalar: &datafusion::common::ScalarValue) -> DFResult<AttributeValue> {
    use datafusion::common::ScalarValue;
    let av = match scalar {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            AttributeValue::S(s.clone())
        }
        ScalarValue::Int8(Some(v)) => AttributeValue::N(v.to_string()),
        ScalarValue::Int16(Some(v)) => AttributeValue::N(v.to_string()),
        ScalarValue::Int32(Some(v)) => AttributeValue::N(v.to_string()),
        ScalarValue::Int64(Some(v)) => AttributeValue::N(v.to_string()),
        ScalarValue::UInt8(Some(v)) => AttributeValue::N(v.to_string()),
        ScalarValue::UInt16(Some(v)) => AttributeValue::N(v.to_string()),
        ScalarValue::UInt32(Some(v)) => AttributeValue::N(v.to_string()),
        ScalarValue::UInt64(Some(v)) => AttributeValue::N(v.to_string()),
        ScalarValue::Float32(Some(v)) => AttributeValue::N(v.to_string()),
        ScalarValue::Float64(Some(v)) => AttributeValue::N(v.to_string()),
        ScalarValue::Boolean(Some(v)) => AttributeValue::Bool(*v),
        ScalarValue::Null => AttributeValue::Null(true),
        _ => {
            return Err(DataFusionError::Plan(format!(
                "Unsupported scalar type for DynamoDB: {scalar}"
            )));
        }
    };
    Ok(av)
}

// ─── Page fetching & streaming ──────────────────────────────────────────────

/// One page source for the shared scan/query paginator.
#[derive(Clone, Debug)]
enum PageSource {
    Scan {
        filter: Option<DynamoFilter>,
    },
    Query {
        key_condition: DynamoFilter,
        filter: Option<DynamoFilter>,
    },
}

/// Fetch a single page from a Scan or Query, merging any filter, key-condition
/// and projection expressions onto one request (their `#n`/`#k`/`#p` namespaces
/// never collide). Returns the page's items — moved, not cloned — and the next
/// `ExclusiveStartKey` (`None` when the table is exhausted).
async fn fetch_page(
    client: &Client,
    table: &str,
    source: &PageSource,
    projection: Option<&(String, HashMap<String, String>)>,
    start_key: Option<HashMap<String, AttributeValue>>,
    page_limit: Option<i32>,
) -> Result<(
    Vec<HashMap<String, AttributeValue>>,
    Option<HashMap<String, AttributeValue>>,
)> {
    let mut names: HashMap<String, String> = HashMap::new();
    if let Some((_, pnames)) = projection {
        names.extend(pnames.clone());
    }

    let (items, last_key) = match source {
        PageSource::Scan { filter } => {
            let mut req = client.scan().table_name(table);
            if let Some(f) = filter {
                req = req
                    .filter_expression(&f.expression)
                    .set_expression_attribute_values(Some(f.values.clone()));
                names.extend(f.names.clone());
            }
            if let Some((expr, _)) = projection {
                req = req.projection_expression(expr);
            }
            if !names.is_empty() {
                req = req.set_expression_attribute_names(Some(names));
            }
            if let Some(l) = page_limit {
                req = req.limit(l);
            }
            if let Some(sk) = start_key {
                req = req.set_exclusive_start_key(Some(sk));
            }
            let out = req
                .send()
                .await
                .with_context(|| format!("DynamoDB scan failed for '{table}'"))?;
            (out.items.unwrap_or_default(), out.last_evaluated_key)
        }
        PageSource::Query {
            key_condition,
            filter,
        } => {
            let mut req = client
                .query()
                .table_name(table)
                .key_condition_expression(&key_condition.expression);
            let mut values = key_condition.values.clone();
            names.extend(key_condition.names.clone());
            if let Some(f) = filter {
                req = req.filter_expression(&f.expression);
                values.extend(f.values.clone());
                names.extend(f.names.clone());
            }
            if let Some((expr, _)) = projection {
                req = req.projection_expression(expr);
            }
            req = req
                .set_expression_attribute_names(Some(names))
                .set_expression_attribute_values(Some(values));
            if let Some(l) = page_limit {
                req = req.limit(l);
            }
            if let Some(sk) = start_key {
                req = req.set_exclusive_start_key(Some(sk));
            }
            let out = req
                .send()
                .await
                .with_context(|| format!("DynamoDB query failed for '{table}'"))?;
            (out.items.unwrap_or_default(), out.last_evaluated_key)
        }
    };

    Ok((items, last_key.filter(|k| !k.is_empty())))
}

/// State carried between paginator steps.
struct PageState {
    start_key: Option<HashMap<String, AttributeValue>>,
    remaining: Option<usize>,
}

/// Stream a Scan/Query one page at a time as projected `RecordBatch`es. Memory
/// is bounded to a single page (~1MB) instead of buffering the whole result,
/// and a SQL `LIMIT` caps both the rows returned and the per-page request size.
fn stream_pages(
    client: Client,
    table: String,
    source: PageSource,
    projection: Option<(String, HashMap<String, String>)>,
    schema: SchemaRef,
    limit: Option<usize>,
) -> impl Stream<Item = DFResult<RecordBatch>> {
    let initial = Some(PageState {
        start_key: None,
        remaining: limit,
    });
    stream::unfold(initial, move |maybe_state| {
        let client = client.clone();
        let table = table.clone();
        let source = source.clone();
        let projection = projection.clone();
        let schema = schema.clone();
        async move {
            let state = maybe_state?;
            let page_limit = state.remaining.map(|r| r.min(i32::MAX as usize) as i32);
            let fetched = fetch_page(
                &client,
                &table,
                &source,
                projection.as_ref(),
                state.start_key,
                page_limit,
            )
            .await
            .map_err(|e| DataFusionError::External(e.into()));

            let (mut items, last_key) = match fetched {
                Ok(v) => v,
                Err(e) => return Some((Err(e), None)),
            };
            if let Some(r) = state.remaining {
                items.truncate(r);
            }
            let got = items.len();
            let batch = items_to_batch(items, &schema);
            let next_remaining = state.remaining.map(|r| r.saturating_sub(got));
            let stop = next_remaining == Some(0) || last_key.is_none();
            let next = if stop {
                None
            } else {
                Some(PageState {
                    start_key: last_key,
                    remaining: next_remaining,
                })
            };
            Some((batch, next))
        }
    })
}

/// Count items server-side via `Scan` with `Select=COUNT`, emitting a single
/// zero-column batch whose row count is the total (drives `count(*)`). Only
/// reached when the projection is empty, i.e. no predicate references any
/// column, so an unfiltered count is exact.
async fn count_scan(client: &Client, table: &str, schema: SchemaRef) -> DFResult<RecordBatch> {
    let mut total = 0usize;
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    loop {
        let mut req = client.scan().table_name(table).select(Select::Count);
        if let Some(sk) = start.take() {
            req = req.set_exclusive_start_key(Some(sk));
        }
        let out = req
            .send()
            .await
            .map_err(|e| DataFusionError::Execution(format!("DynamoDB count scan failed: {e}")))?;
        total += out.count() as usize;
        match out.last_evaluated_key {
            Some(k) if !k.is_empty() => start = Some(k),
            _ => break,
        }
    }
    let options = RecordBatchOptions::new().with_row_count(Some(total));
    RecordBatch::try_new_with_options(schema, vec![], &options).map_err(DataFusionError::from)
}

// ─── Scan execution plan ────────────────────────────────────────────────────

/// Leaf plan that streams a DynamoDB read, page by page, on execution.
#[derive(Debug)]
struct DynamoScanExec {
    client: Client,
    table_name: String,
    output_schema: SchemaRef,
    read: DynamoRead,
    /// `ProjectionExpression` (and its `#p` bindings) for the projected columns,
    /// or `None` for a `count(*)` (empty projection).
    projection_expr: Option<(String, HashMap<String, String>)>,
    count_only: bool,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl DisplayAs for DynamoScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "DynamoScanExec")
    }
}

impl ExecutionPlan for DynamoScanExec {
    fn name(&self) -> &str {
        "DynamoScanExec"
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
        let schema = self.output_schema.clone();
        let client = self.client.clone();
        let table = self.table_name.clone();

        if self.count_only {
            let count_schema = schema.clone();
            let fut = async move { count_scan(&client, &table, count_schema).await };
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                stream::once(fut),
            )));
        }

        match self.read.clone() {
            DynamoRead::GetItem { key } => {
                let projection = self.projection_expr.clone();
                let item_schema = schema.clone();
                let fut = async move {
                    let mut req = client.get_item().table_name(&table).set_key(Some(key));
                    if let Some((expr, names)) = projection {
                        req = req
                            .projection_expression(expr)
                            .set_expression_attribute_names(Some(names));
                    }
                    let out = req.send().await.map_err(|e| {
                        DataFusionError::Execution(format!("DynamoDB get_item failed: {e}"))
                    })?;
                    let items: Vec<_> = out.item.into_iter().collect();
                    items_to_batch(items, &item_schema)
                };
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    stream::once(fut),
                )))
            }
            DynamoRead::Query { key_condition } => {
                let src = PageSource::Query {
                    key_condition,
                    filter: None,
                };
                let s = stream_pages(
                    client,
                    table,
                    src,
                    self.projection_expr.clone(),
                    schema.clone(),
                    self.limit,
                );
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, s)))
            }
            DynamoRead::Scan { filter } => {
                let src = PageSource::Scan { filter };
                let s = stream_pages(
                    client,
                    table,
                    src,
                    self.projection_expr.clone(),
                    schema.clone(),
                    self.limit,
                );
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, s)))
            }
        }
    }
}

// ─── Insert execution plan ──────────────────────────────────────────────────

struct DynamoInsertExec {
    input: Arc<dyn ExecutionPlan>,
    client: Client,
    table_name: String,
    /// Target-table schema, used to shape input batches into items to write.
    schema: SchemaRef,
    /// Properties of this node's own output (`{ count }`), distinct from
    /// `input`'s schema — `execute` streams a count, not the inserted rows.
    properties: PlanProperties,
    partition_key: String,
    /// True for INSERT OVERWRITE/REPLACE (upsert via `BatchWriteItem`); false
    /// for plain INSERT (Append), which uses a conditional `PutItem` so a
    /// duplicate key errors instead of silently replacing the item.
    upsert: bool,
}

impl Debug for DynamoInsertExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamoInsertExec")
            .field("table_name", &self.table_name)
            .field("upsert", &self.upsert)
            .finish()
    }
}

impl DisplayAs for DynamoInsertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "DynamoInsertExec")
    }
}

/// Append INSERT: `PutItem` guarded by `attribute_not_exists` on the partition
/// key so an existing item is not silently overwritten.
async fn put_conditional(
    client: &Client,
    table: &str,
    partition_key: &str,
    item: HashMap<String, AttributeValue>,
) -> DFResult<()> {
    let names = HashMap::from([("#pk".to_string(), partition_key.to_string())]);
    client
        .put_item()
        .table_name(table)
        .set_item(Some(item))
        .condition_expression("attribute_not_exists(#pk)")
        .set_expression_attribute_names(Some(names))
        .send()
        .await
        .map_err(|e| {
            let svc = e.into_service_error();
            if svc.is_conditional_check_failed_exception() {
                DataFusionError::Execution(format!(
                    "DynamoDB INSERT: an item with this key already exists in '{table}'. \
                     Use INSERT OVERWRITE to replace it."
                ))
            } else {
                DataFusionError::Execution(format!("DynamoDB put_item failed: {svc}"))
            }
        })?;
    Ok(())
}

/// Upsert INSERT (Overwrite/Replace): batch the items via `BatchWriteItem`
/// (25 per request), cutting a large insert from one round-trip per row.
async fn batch_put(
    client: &Client,
    table: &str,
    items: Vec<HashMap<String, AttributeValue>>,
) -> DFResult<()> {
    for chunk in items.chunks(BATCH_WRITE_CHUNK) {
        let requests: Vec<WriteRequest> = chunk
            .iter()
            .map(|it| {
                let put = PutRequest::builder()
                    .set_item(Some(it.clone()))
                    .build()
                    .expect("PutRequest requires only an item, which is set");
                WriteRequest::builder().put_request(put).build()
            })
            .collect();
        batch_write(client, table, requests)
            .await
            .map_err(|e| DataFusionError::External(e.into()))?;
    }
    Ok(())
}

impl ExecutionPlan for DynamoInsertExec {
    fn name(&self) -> &str {
        "DynamoInsertExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DynamoInsertExec {
            input: children[0].clone(),
            client: self.client.clone(),
            table_name: self.table_name.clone(),
            schema: self.schema.clone(),
            properties: self.properties.clone(),
            partition_key: self.partition_key.clone(),
            upsert: self.upsert,
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let mut input_stream = self.input.execute(partition, context)?;
        let client = self.client.clone();
        let table_name = self.table_name.clone();
        let schema = self.schema.clone();
        let partition_key = self.partition_key.clone();
        let upsert = self.upsert;
        let output_schema = count_schema();

        let future = async move {
            let mut count: u64 = 0;
            // Bound memory for upsert batching: flush every full chunk instead of
            // buffering the entire input.
            let mut buf: Vec<HashMap<String, AttributeValue>> = Vec::new();
            while let Some(batch) = input_stream.next().await {
                let batch = batch?;
                let items = record_batch_to_items(&batch, &schema)
                    .map_err(|e| DataFusionError::External(e.into()))?;
                for item in items {
                    if upsert {
                        buf.push(item);
                        if buf.len() >= BATCH_WRITE_CHUNK {
                            let chunk = std::mem::take(&mut buf);
                            count += chunk.len() as u64;
                            batch_put(&client, &table_name, chunk).await?;
                        }
                    } else {
                        put_conditional(&client, &table_name, &partition_key, item).await?;
                        count += 1;
                    }
                }
            }
            if !buf.is_empty() {
                count += buf.len() as u64;
                batch_put(&client, &table_name, buf).await?;
            }
            count_batch(count)
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream::once(future),
        )))
    }
}

// ─── DELETE / UPDATE execution plan ─────────────────────────────────────────

#[derive(Clone)]
enum DynamoDmlOp {
    Delete {
        plan: DmlKeyPlan,
    },
    Update {
        plan: DmlKeyPlan,
        sets: Vec<(String, AttributeValue)>,
    },
}

/// Leaf plan that runs a key-based DELETE or UPDATE and returns `{ count }`.
///
/// DynamoDB cannot delete or update by arbitrary predicate, so the matching
/// keys are resolved first (via Query or Scan) and then mutated: DELETE batches
/// them through `BatchWriteItem`, UPDATE issues one `UpdateItem` per key
/// (`BatchWriteItem` cannot express updates).
struct DynamoDmlExec {
    handle: DynamoHandle,
    op: DynamoDmlOp,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl DynamoDmlExec {
    fn new(handle: DynamoHandle, op: DynamoDmlOp) -> Self {
        Self {
            handle,
            op,
            schema: count_schema(),
            properties: count_plan_properties(),
        }
    }
}

impl Debug for DynamoDmlExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "DynamoDmlExec")
    }
}

impl DisplayAs for DynamoDmlExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "DynamoDmlExec")
    }
}

impl ExecutionPlan for DynamoDmlExec {
    fn name(&self) -> &str {
        "DynamoDmlExec"
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
        let handle = self.handle.clone();
        let op = self.op.clone();
        let future = async move {
            let affected = match op {
                DynamoDmlOp::Delete { plan } => {
                    let keys = handle
                        .matching_keys(&plan)
                        .await
                        .map_err(|e| DataFusionError::External(e.into()))?;
                    handle
                        .batch_delete(keys)
                        .await
                        .map_err(|e| DataFusionError::External(e.into()))?
                }
                DynamoDmlOp::Update { plan, sets } => {
                    let keys = handle
                        .matching_keys(&plan)
                        .await
                        .map_err(|e| DataFusionError::External(e.into()))?;
                    let (expr, mut names, values) = build_update_expression(&sets);
                    // Guard against resurrecting a row deleted between key
                    // resolution and this update: DynamoDB's UpdateItem creates
                    // the item if absent, so without a condition a concurrently
                    // deleted row would come back as key attributes plus the SET
                    // fields. `attribute_exists` on the partition key (which can
                    // never be a SET target, so `#pk` won't collide with `#s*`)
                    // makes the update a no-op for a vanished row.
                    names.insert("#pk".to_string(), handle.partition_key.clone());
                    let mut n = 0u64;
                    for key in keys {
                        let result = handle
                            .client
                            .update_item()
                            .table_name(&handle.table_name)
                            .set_key(Some(key))
                            .update_expression(&expr)
                            .condition_expression("attribute_exists(#pk)")
                            .set_expression_attribute_names(Some(names.clone()))
                            .set_expression_attribute_values(Some(values.clone()))
                            .send()
                            .await;
                        match result {
                            Ok(_) => n += 1,
                            // Row disappeared after matching_keys(); skip it
                            // rather than recreating a partial item or failing
                            // the whole statement.
                            Err(e)
                                if e.as_service_error().is_some_and(|se| {
                                    se.is_conditional_check_failed_exception()
                                }) => {}
                            Err(e) => {
                                return Err(DataFusionError::Execution(format!(
                                    "DynamoDB update failed: {e}"
                                )));
                            }
                        }
                    }
                    n
                }
            };
            count_batch(affected)
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream::once(future),
        )))
    }
}

/// Build a DynamoDB `SET` update expression from column→value assignments.
fn build_update_expression(
    sets: &[(String, AttributeValue)],
) -> (
    String,
    HashMap<String, String>,
    HashMap<String, AttributeValue>,
) {
    let mut names = HashMap::new();
    let mut values = HashMap::new();
    let mut parts = Vec::with_capacity(sets.len());
    for (i, (col, av)) in sets.iter().enumerate() {
        let name_ph = format!("#s{i}");
        let val_ph = format!(":s{i}");
        names.insert(name_ph.clone(), col.clone());
        values.insert(val_ph.clone(), av.clone());
        parts.push(format!("{name_ph} = {val_ph}"));
    }
    (format!("SET {}", parts.join(", ")), names, values)
}

fn count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

/// `PlanProperties` for an insert/DML leaf whose execution emits a single
/// `{ count }` row. Kept in sync with the schema returned by `execute` so
/// planning/introspection see the real output shape, not the input's.
fn count_plan_properties() -> PlanProperties {
    PlanProperties::new(
        EquivalenceProperties::new(count_schema()),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Final,
        Boundedness::Bounded,
    )
}

fn count_batch(count: u64) -> DFResult<RecordBatch> {
    let array: UInt64Array = vec![count].into();
    RecordBatch::try_new(count_schema(), vec![Arc::new(array)]).map_err(DataFusionError::from)
}

// ─── Registration ───────────────────────────────────────────────────────────

/// Register a DynamoDB table or a whole account/endpoint (catalog) into a DataFusion
/// [`SessionContext`].
///
/// Single-table mode (default) registers one table under `name`. Catalog mode discovers all
/// accessible DynamoDB tables via `ListTables` and registers each one under
/// `name.tables.<table_name>`.
///
/// # Arguments
/// * `session_ctx` - session context to register the table(s) into.
/// * `name` - the SQL table name (table mode) or catalog name (catalog mode) to expose.
/// * `connection_string` - the DynamoDB endpoint URL. For Amazon DynamoDB use
///   the regional endpoint (e.g. `https://dynamodb.us-east-1.amazonaws.com`);
///   for DynamoDB Local use `http://localhost:8000`.
/// * `options` - configuration options (see below).
/// * `read_write` - gates DML for every table registered.
/// * `hierarchy_level` - [`HierarchyLevel::Table`] (default) or [`HierarchyLevel::Catalog`].
///
/// # Options
/// * `table` - DynamoDB table name (required in table mode; not allowed in catalog mode).
/// * `partition_key` - partition (hash) key attribute name. Optional in table mode: the key
///   schema is read authoritatively via `DescribeTable`; this is only a fallback
///   used when `DescribeTable` is unavailable (e.g. restricted IAM permissions).
///   Ignored in catalog mode.
/// * `sort_key` - sort (range) key attribute name. Optional in table mode and likewise
///   auto-detected from `DescribeTable`; ignored in catalog mode.
/// * `region` - AWS region (optional, default `us-east-1`).
/// * `access_key_env` / `secret_key_env` - names of environment variables
///   holding static AWS credentials (optional). When omitted, the default AWS
///   credential provider chain is used.
/// * `columns` - explicit column schema as `name:type[,name:type…]` (types:
///   `string`, `int`, `float`, `bool`). Optional in table mode; ignored in catalog
///   mode because each DynamoDB table has its own attribute set.
/// * `allowed_tables` - Comma-separated table allow-list (catalog mode only). When
///   present, Skardi registers only those tables and skips `ListTables`.
pub async fn register_dynamodb_tables(
    session_ctx: &mut SessionContext,
    name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
    read_write: bool,
    hierarchy_level: HierarchyLevel,
) -> Result<()> {
    let mode_str = if read_write {
        "read-write"
    } else {
        "read-only"
    };
    match hierarchy_level {
        HierarchyLevel::Catalog => {
            register_dynamodb_catalog(
                session_ctx,
                name,
                connection_string,
                options,
                read_write,
                mode_str,
            )
            .await
        }
        HierarchyLevel::Table => {
            register_single_dynamodb_table(
                session_ctx,
                name,
                connection_string,
                options,
                read_write,
                mode_str,
            )
            .await
        }
    }
}

/// Register one DynamoDB table under `name` in the default catalog.
async fn register_single_dynamodb_table(
    session_ctx: &mut SessionContext,
    name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
    read_write: bool,
    mode_str: &str,
) -> Result<()> {
    tracing::info!(
        source = %name,
        endpoint = %connection_string,
        read_write,
        "Registering DynamoDB table"
    );

    let opts = options.ok_or_else(|| {
        anyhow::anyhow!("DynamoDB data source '{name}' requires options (table, partition_key)")
    })?;

    let table = opts
        .get("table")
        .ok_or_else(|| anyhow::anyhow!("DynamoDB data source '{name}' requires 'table' option"))?;
    let region = opts
        .get("region")
        .cloned()
        .unwrap_or_else(|| "us-east-1".to_string());

    let client = build_client(connection_string, &region, opts).await?;

    // Prefer the table's authoritative key schema (this also auto-detects the
    // sort key). Fall back to the configured options if DescribeTable is
    // unavailable, e.g. under restricted IAM permissions.
    let (partition_key, sort_key) = match describe_keys(&client, table).await {
        Ok(keys) => keys,
        Err(e) => {
            tracing::warn!(
                source = %name,
                error = %e,
                "DescribeTable failed; falling back to configured key options"
            );
            let pk = opts.get("partition_key").cloned().ok_or_else(|| {
                anyhow::anyhow!(
                    "DynamoDB data source '{name}': DescribeTable failed ({e}) and no 'partition_key' option was provided"
                )
            })?;
            (pk, opts.get("sort_key").cloned())
        }
    };

    // An explicit `columns` option pins the schema; otherwise it is inferred by
    // sampling inside `DynamoTableProvider::new`.
    let declared_schema = match opts.get("columns") {
        Some(spec) => Some(
            parse_columns_option(spec, &partition_key, sort_key.as_deref())
                .with_context(|| format!("DynamoDB data source '{name}': invalid 'columns'"))?,
        ),
        None => None,
    };

    let provider = DynamoTableProvider::new(
        client,
        table,
        &partition_key,
        sort_key.as_deref(),
        declared_schema,
        read_write,
    )
    .await
    .with_context(|| format!("Failed to create DynamoDB table provider for '{name}'"))?;

    session_ctx
        .register_table(name, Arc::new(provider))
        .with_context(|| format!("Failed to register DynamoDB table '{name}' with DataFusion"))?;

    tracing::info!(
        source = %name,
        table = %table,
        "Successfully registered DynamoDB table '{}' as '{}' ({})",
        table,
        name,
        mode_str
    );
    Ok(())
}

/// Register DynamoDB tables as a named DataFusion catalog.
///
/// Tables are discovered via `ListTables` unless `allowed_tables` is set. Registered
/// tables live under the fixed schema `tables`, so they are addressable as
/// `catalog.tables.<table_name>`.
///
/// Tables whose key schema or sampled schema cannot be determined are skipped with a
/// warning rather than failing the entire catalog registration, because DynamoDB
/// permissions or table states can vary across a large account.
async fn register_dynamodb_catalog(
    session_ctx: &mut SessionContext,
    catalog_name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
    read_write: bool,
    mode_str: &str,
) -> Result<()> {
    tracing::info!(
        catalog = %catalog_name,
        endpoint = %connection_string,
        read_write,
        "Registering DynamoDB catalog"
    );

    let opts = options.cloned().unwrap_or_default();
    let region = opts
        .get("region")
        .cloned()
        .unwrap_or_else(|| "us-east-1".to_string());

    let label = SourceLabel::new(
        DataSourceType::Dynamodb,
        HierarchyLevel::Catalog,
        catalog_name,
    );
    let client = retry_with_timeout(label, "DynamoDB client creation", || async {
        build_client(connection_string, &region, &opts).await
    })
    .await
    .with_context(|| format!("Failed to build DynamoDB client for catalog '{catalog_name}'"))?;

    let table_names = match parse_allowed_tables(Some(&opts))? {
        Some(allowed) => {
            tracing::info!(
                catalog = %catalog_name,
                allowed_tables = ?allowed,
                "Using DynamoDB catalog table allow-list"
            );
            allowed
        }
        None => retry_with_timeout(label, "ListTables", || async {
            list_dynamodb_tables(&client).await
        })
        .await
        .with_context(|| format!("Failed to list DynamoDB tables for catalog '{catalog_name}'"))?,
    };

    if table_names.is_empty() {
        tracing::warn!(catalog = %catalog_name, "No DynamoDB tables found for catalog registration");
    }

    let discovered_count = table_names.len();
    let client = Arc::new(client);
    let (registered_count, skipped_count) = build_dynamodb_catalog_best_effort(
        session_ctx,
        catalog_name,
        table_names,
        client,
        read_write,
    )
    .await
    .with_context(|| format!("Failed to build DynamoDB catalog '{catalog_name}'"))?;

    tracing::info!(
        "Successfully registered DynamoDB catalog '{}' with {} table(s), skipped {} of {} discovered ({})",
        catalog_name,
        registered_count,
        skipped_count,
        discovered_count,
        mode_str
    );
    Ok(())
}

/// Parse the comma-separated `allowed_tables` option.
fn parse_allowed_tables(options: Option<&HashMap<String, String>>) -> Result<Option<Vec<String>>> {
    let Some(value) = options.and_then(|opts| opts.get("allowed_tables")) else {
        return Ok(None);
    };
    let mut tables = value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    tables.sort();
    tables.dedup();
    if tables.is_empty() {
        anyhow::bail!(
            "DynamoDB catalog option 'allowed_tables' must be omitted or contain at least one table name"
        );
    } else {
        Ok(Some(tables))
    }
}

/// Build and register a DynamoDB catalog while skipping individual tables that
/// cannot be described or sampled.
async fn build_dynamodb_catalog_best_effort(
    session_ctx: &SessionContext,
    catalog_name: &str,
    table_names: Vec<String>,
    client: Arc<Client>,
    read_write: bool,
) -> Result<(usize, usize)> {
    let catalog_provider = Arc::new(MemoryCatalogProvider::new());
    let catalog_name_owned = catalog_name.to_string();

    let build_futures = table_names.into_iter().map(|table_name| {
        let client = Arc::clone(&client);
        let catalog_name = catalog_name_owned.clone();
        async move {
            let result =
                build_dynamodb_table_provider(client, &table_name, read_write, &catalog_name).await;
            (table_name, result)
        }
    });

    let prepared = stream::iter(build_futures)
        .buffer_unordered(DYNAMODB_CATALOG_BUILD_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

    let mut registered = 0usize;
    let mut skipped = 0usize;
    let schema_name = DYNAMODB_CATALOG_SCHEMA.to_string();

    for (table_name, provider_result) in prepared {
        match provider_result {
            Ok(provider) => {
                if catalog_provider.schema(&schema_name).is_none() {
                    catalog_provider
                        .register_schema(&schema_name, Arc::new(MemorySchemaProvider::new()))
                        .map_err(|e| {
                            anyhow::anyhow!(
                                "Failed to register schema '{}' for DynamoDB catalog '{}': {}",
                                schema_name,
                                catalog_name,
                                e
                            )
                        })?;
                }
                let schema_provider = catalog_provider.schema(&schema_name).ok_or_else(|| {
                    anyhow::anyhow!(
                        "Schema '{}' was not found after registration in DynamoDB catalog '{}'",
                        schema_name,
                        catalog_name
                    )
                })?;
                schema_provider
                    .register_table(table_name.clone(), Arc::new(provider))
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "Failed to register table '{}.{}' in DynamoDB catalog '{}': {}",
                            schema_name,
                            table_name,
                            catalog_name,
                            e
                        )
                    })?;
                registered += 1;
                tracing::debug!(
                    catalog = %catalog_name,
                    schema = %schema_name,
                    table = %table_name,
                    "Registered DynamoDB catalog table"
                );
            }
            Err(e) => {
                skipped += 1;
                tracing::warn!(
                    catalog = %catalog_name,
                    table = %table_name,
                    error = %e,
                    "Skipping DynamoDB catalog table after provider build failure"
                );
            }
        }
    }

    if registered == 0 && skipped > 0 {
        tracing::warn!(
            catalog = %catalog_name,
            skipped,
            "DynamoDB catalog registered with no tables because every table was skipped"
        );
    }

    session_ctx.register_catalog(catalog_name, catalog_provider);
    Ok((registered, skipped))
}

/// Build a [`DynamoTableProvider`] for `table_name`.
async fn build_dynamodb_table_provider(
    client: Arc<Client>,
    table_name: &str,
    read_write: bool,
    catalog_name: &str,
) -> Result<DynamoTableProvider> {
    let (partition_key, sort_key) =
        describe_keys(&client, table_name).await.with_context(|| {
            format!("DynamoDB catalog '{catalog_name}': DescribeTable failed for '{table_name}'")
        })?;

    DynamoTableProvider::new(
        (*client).clone(),
        table_name,
        &partition_key,
        sort_key.as_deref(),
        None,
        read_write,
    )
    .await
    .with_context(|| {
        format!("DynamoDB catalog '{catalog_name}': failed to create provider for '{table_name}'")
    })
}

/// List all DynamoDB table names accessible through `client`, following pagination.
async fn list_dynamodb_tables(client: &Client) -> Result<Vec<String>> {
    let mut table_names = Vec::new();
    let mut last_evaluated: Option<String> = None;

    loop {
        let mut req = client.list_tables().limit(LIST_TABLES_PAGE_SIZE);
        if let Some(exclusive_start) = last_evaluated.take() {
            req = req.exclusive_start_table_name(exclusive_start);
        }

        let resp = req.send().await.with_context(|| "ListTables failed")?;
        table_names.extend(resp.table_names().iter().cloned());

        match resp.last_evaluated_table_name() {
            Some(name) if !name.is_empty() => last_evaluated = Some(name.to_string()),
            _ => break,
        }
    }

    table_names.sort();
    Ok(table_names)
}

/// Build a DynamoDB client from the endpoint, region, and credential options.
async fn build_client(
    endpoint: &str,
    region: &str,
    opts: &HashMap<String, String>,
) -> Result<Client> {
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(region.to_string()))
        .endpoint_url(endpoint);

    if let (Some(ak_env), Some(sk_env)) = (opts.get("access_key_env"), opts.get("secret_key_env")) {
        let access_key = std::env::var(ak_env).with_context(|| {
            format!("Environment variable '{ak_env}' not found for DynamoDB access key")
        })?;
        let secret_key = std::env::var(sk_env).with_context(|| {
            format!("Environment variable '{sk_env}' not found for DynamoDB secret key")
        })?;
        let creds = Credentials::new(access_key, secret_key, None, None, "skardi-dynamodb");
        loader = loader.credentials_provider(creds);
    }

    let config = loader.load().await;
    Ok(Client::new(&config))
}

/// Read the table's authoritative key schema via `DescribeTable`, returning
/// `(partition_key, sort_key)`. This drives key-aware read planning without
/// trusting (possibly mismatched) configured key names, and auto-detects the
/// sort key for composite-key tables.
async fn describe_keys(client: &Client, table: &str) -> Result<(String, Option<String>)> {
    let out = client
        .describe_table()
        .table_name(table)
        .send()
        .await
        .with_context(|| format!("DescribeTable failed for '{table}'"))?;
    let key_schema = out.table().map(|t| t.key_schema()).unwrap_or_default();

    let mut partition_key: Option<String> = None;
    let mut sort_key: Option<String> = None;
    for element in key_schema {
        match element.key_type() {
            KeyType::Hash => partition_key = Some(element.attribute_name().to_string()),
            KeyType::Range => sort_key = Some(element.attribute_name().to_string()),
            _ => {}
        }
    }

    let partition_key = partition_key
        .ok_or_else(|| anyhow::anyhow!("table '{table}' has no HASH key in its key schema"))?;
    Ok((partition_key, sort_key))
}

/// Parse the `columns` option (`name:type[,name:type…]`) into an explicit
/// schema, ordered with the key attributes first (see `build_schema_fields`).
fn parse_columns_option(
    spec: &str,
    partition_key: &str,
    sort_key: Option<&str>,
) -> Result<SchemaRef> {
    let mut attrs: Vec<(String, DataType)> = Vec::new();
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (name, ty) = part
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("column '{part}' must be 'name:type'"))?;
        let dtype = match ty.trim().to_ascii_lowercase().as_str() {
            "string" | "str" | "utf8" | "text" => DataType::Utf8,
            "int" | "integer" | "bigint" | "int64" | "long" => DataType::Int64,
            "float" | "double" | "float64" | "number" | "num" => DataType::Float64,
            "bool" | "boolean" => DataType::Boolean,
            other => anyhow::bail!(
                "column '{}' has unknown type '{other}' (use string, int, float, or bool)",
                name.trim()
            ),
        };
        attrs.push((name.trim().to_string(), dtype));
    }
    if attrs.is_empty() {
        anyhow::bail!("'columns' option is empty");
    }
    Ok(Arc::new(build_schema_fields(
        partition_key,
        sort_key,
        &attrs,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::hierarchy::HierarchyLevel;
    use arrow::array::Array;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::logical_expr::{BinaryExpr, col, lit};
    use datafusion::physical_plan::empty::EmptyExec;

    fn n(s: &str) -> AttributeValue {
        AttributeValue::N(s.to_string())
    }

    #[test]
    fn attribute_type_inference() {
        assert_eq!(
            attribute_value_to_arrow_type(&AttributeValue::S("x".into())),
            DataType::Utf8
        );
        // Numbers always infer as Float64 — a single sampled whole number can't
        // prove a column is integer-only, and later fractional values would be
        // truncated or dropped by the Inexact re-filter.
        assert_eq!(attribute_value_to_arrow_type(&n("42")), DataType::Float64);
        assert_eq!(attribute_value_to_arrow_type(&n("4.5")), DataType::Float64);
        assert_eq!(
            attribute_value_to_arrow_type(&AttributeValue::Bool(true)),
            DataType::Boolean
        );
        assert_eq!(
            attribute_value_to_arrow_type(&AttributeValue::Null(true)),
            DataType::Utf8
        );
    }

    #[test]
    fn number_coercions() {
        assert_eq!(av_to_i64(&n("7")), Some(7));
        // Fractional N does not silently truncate into an Int64 column; it
        // becomes NULL so the row can't disappear via the Inexact re-filter.
        assert_eq!(av_to_i64(&n("7.9")), None);
        // Cross-type coercion is refused: a string is never parsed into a
        // numeric column (would violate the pushdown superset contract).
        assert_eq!(av_to_i64(&AttributeValue::S("7".into())), None);
        assert_eq!(av_to_f64(&n("2.5")), Some(2.5));
        assert_eq!(av_to_f64(&AttributeValue::S("2.5".into())), None);
        assert_eq!(av_to_bool(&AttributeValue::Bool(false)), Some(false));
        assert_eq!(av_to_bool(&n("1")), None);
    }

    #[test]
    fn null_bearing_column_builds_nullable_array() {
        let values = vec![
            Some(AttributeValue::S("a".into())),
            None,
            Some(AttributeValue::S("c".into())),
        ];
        let arr = attribute_values_to_arrow_array(&values, &DataType::Utf8);
        assert_eq!(arr.len(), 3);
        assert!(arr.is_null(1));
        assert!(!arr.is_null(0));
    }

    #[test]
    fn empty_column_builds_empty_array() {
        let arr = attribute_values_to_arrow_array(&[], &DataType::Int64);
        assert_eq!(arr.len(), 0);
    }

    #[test]
    fn pushable_filter_detection() {
        let pushable = col("a").eq(lit(1i64));
        assert!(is_pushable_binary_filter(&pushable));
        // column-to-column is not pushable
        let not_pushable = col("a").eq(col("b"));
        assert!(!is_pushable_binary_filter(&not_pushable));
    }

    #[test]
    fn filter_expression_emits_placeholders() {
        let filters = vec![
            col("category").eq(lit("Electronics")),
            col("price").gt(lit(100i64)),
        ];
        let f = build_filter_expression(&filters)
            .expect("buildable")
            .expect("non-empty");
        assert!(f.expression.contains(" AND "));
        assert_eq!(f.names.len(), 2);
        assert_eq!(f.values.len(), 2);
        // every #name placeholder in the expression has a binding
        for ph in f.names.keys() {
            assert!(f.expression.contains(ph.as_str()));
        }
    }

    #[test]
    fn literal_on_left_flips_operator() {
        // 100 < price  ⇒  #name > :val
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit(100i64)),
            Operator::Lt,
            Box::new(col("price")),
        ));
        let f = build_filter_expression(&[expr])
            .expect("buildable")
            .expect("non-empty");
        assert!(f.expression.contains('>'), "got: {}", f.expression);
    }

    #[test]
    fn empty_filter_list_is_none() {
        assert!(build_filter_expression(&[]).expect("ok").is_none());
    }

    #[test]
    fn scalar_conversions() {
        assert!(matches!(
            scalar_to_attribute_value(&ScalarValue::Utf8(Some("hi".into()))).unwrap(),
            AttributeValue::S(s) if s == "hi"
        ));
        assert!(matches!(
            scalar_to_attribute_value(&ScalarValue::Int64(Some(5))).unwrap(),
            AttributeValue::N(s) if s == "5"
        ));
        assert!(matches!(
            scalar_to_attribute_value(&ScalarValue::Boolean(Some(true))).unwrap(),
            AttributeValue::Bool(true)
        ));
    }

    #[test]
    fn update_expression_shape() {
        let sets = vec![
            ("price".to_string(), n("9.99")),
            ("in_stock".to_string(), AttributeValue::Bool(true)),
        ];
        let (expr, names, values) = build_update_expression(&sets);
        assert!(expr.starts_with("SET "));
        assert!(expr.contains(", "));
        assert_eq!(names.len(), 2);
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn update_expression_placeholders_cannot_collide_with_pk_guard() {
        // UPDATE execution reserves "#pk" for its attribute_exists() resurrect
        // guard, so SET placeholders must stay in the #s/:s namespace — even
        // for a column literally named "pk".
        let sets = vec![("pk".to_string(), n("1")), ("note".to_string(), n("2"))];
        let (expr, names, values) = build_update_expression(&sets);
        assert_eq!(expr, "SET #s0 = :s0, #s1 = :s1");
        assert!(!names.contains_key("#pk"));
        assert!(names.keys().all(|k| k.starts_with("#s")));
        assert!(values.keys().all(|k| k.starts_with(":s")));
    }

    #[test]
    fn classify_single_key_eq_is_get_item() {
        let filters = vec![col("product_id").eq(lit("PROD001"))];
        match classify_read("product_id", None, &filters).unwrap() {
            DynamoRead::GetItem { key } => {
                assert_eq!(key.len(), 1);
                assert!(key.contains_key("product_id"));
            }
            _ => panic!("expected GetItem for a full single-key lookup"),
        }
    }

    #[test]
    fn classify_composite_full_key_is_get_item() {
        let filters = vec![col("pk").eq(lit("A")), col("sk").eq(lit("B"))];
        match classify_read("pk", Some("sk"), &filters).unwrap() {
            DynamoRead::GetItem { key } => assert_eq!(key.len(), 2),
            _ => panic!("expected GetItem when both key parts are pinned"),
        }
    }

    #[test]
    fn classify_partition_eq_sort_range_is_query() {
        let filters = vec![col("pk").eq(lit("A")), col("sk").gt(lit(5i64))];
        match classify_read("pk", Some("sk"), &filters).unwrap() {
            DynamoRead::Query { key_condition } => {
                assert!(key_condition.expression.contains("#k0 = :k0"));
                assert!(key_condition.expression.contains(" AND "));
                assert!(key_condition.expression.contains('>'));
                assert_eq!(key_condition.names.len(), 2);
            }
            _ => panic!("expected Query for partition-eq + sort-range"),
        }
    }

    #[test]
    fn classify_partition_eq_only_on_composite_is_query() {
        let filters = vec![col("pk").eq(lit("A"))];
        match classify_read("pk", Some("sk"), &filters).unwrap() {
            DynamoRead::Query { key_condition } => {
                assert_eq!(key_condition.expression, "#k0 = :k0");
                assert_eq!(key_condition.names.len(), 1);
            }
            _ => panic!("expected partition-only Query on a composite-key table"),
        }
    }

    #[test]
    fn classify_partition_non_eq_is_scan() {
        // A non-equality predicate on the partition key cannot drive Query/GetItem.
        let filters = vec![col("product_id").gt(lit("PROD000"))];
        assert!(matches!(
            classify_read("product_id", None, &filters).unwrap(),
            DynamoRead::Scan { .. }
        ));
    }

    #[test]
    fn classify_non_key_filter_is_scan() {
        let filters = vec![col("category").eq(lit("Electronics"))];
        match classify_read("product_id", None, &filters).unwrap() {
            DynamoRead::Scan { filter } => assert!(filter.is_some()),
            _ => panic!("expected Scan for a non-key filter"),
        }
    }

    #[test]
    fn classify_sort_key_noteq_is_not_a_key_condition() {
        // `sk <> B` is pushable as a filter but illegal in a KeyConditionExpression,
        // so it must NOT become a key condition. With the partition key pinned this
        // yields a partition-only Query (the `<>` is left for DataFusion).
        let filters = vec![col("pk").eq(lit("A")), col("sk").not_eq(lit("B"))];
        match classify_read("pk", Some("sk"), &filters).unwrap() {
            DynamoRead::Query { key_condition } => {
                assert_eq!(key_condition.expression, "#k0 = :k0");
            }
            _ => panic!("expected partition-only Query; `<>` must not become a key condition"),
        }
    }

    // ─── DML planning (key-aware routing + guard) ───────────────────────────

    #[test]
    fn classify_dml_partition_eq_routes_query_with_residual() {
        // pk pinned + a non-key predicate → Query, with the non-key predicate
        // carried as a residual FilterExpression (a Query filter can't touch keys).
        let filters = vec![col("pk").eq(lit("A")), col("category").eq(lit("x"))];
        match classify_dml("pk", None, &filters).unwrap() {
            DmlKeyPlan::Query {
                key_condition,
                residual,
            } => {
                assert_eq!(key_condition.expression, "#k0 = :k0");
                let residual = residual.expect("non-key predicate becomes a residual filter");
                assert!(residual.expression.contains("#n0"));
            }
            _ => panic!("expected Query when the partition key is pinned"),
        }
    }

    #[test]
    fn classify_dml_no_partition_routes_scan() {
        // Partition key not pinned → Scan carrying the full predicate set.
        let filters = vec![col("category").eq(lit("x"))];
        match classify_dml("pk", None, &filters).unwrap() {
            DmlKeyPlan::Scan { filter } => assert!(filter.is_some()),
            _ => panic!("expected Scan when the partition key is not pinned"),
        }
    }

    #[test]
    fn classify_dml_sort_noteq_forces_scan() {
        // `sk <> B` can live in neither a KeyCondition nor a Query filter, so the
        // whole DML must Scan (a Scan filter may reference key attributes).
        let filters = vec![col("pk").eq(lit("A")), col("sk").not_eq(lit("B"))];
        match classify_dml("pk", Some("sk"), &filters).unwrap() {
            DmlKeyPlan::Scan { filter } => {
                let f = filter.expect("filter present");
                // Both predicates are expressed (two placeholders).
                assert_eq!(f.names.len(), 2);
            }
            _ => panic!("expected Scan when a sort predicate is inexpressible on a Query"),
        }
    }

    #[test]
    fn convertible_pushdown_excludes_inconvertible_literal() {
        // Pushable-shaped but with an inconvertible literal → not convertible.
        let ok = col("a").eq(lit(1i64));
        assert!(is_convertible_pushdown(&ok));
        let ts = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("a")),
            Operator::Eq,
            Box::new(Expr::Literal(
                ScalarValue::TimestampNanosecond(Some(0), None),
                None,
            )),
        ));
        assert!(is_pushable_binary_filter(&ts));
        assert!(!is_convertible_pushdown(&ts));
    }

    #[test]
    fn parse_columns_option_orders_keys_first() {
        let schema = parse_columns_option(
            "price:float, name:string, product_id:string",
            "product_id",
            None,
        )
        .expect("valid columns");
        // Key comes first and is non-nullable; declared cols follow, nullable.
        assert_eq!(schema.field(0).name(), "product_id");
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(1).data_type(), &DataType::Float64);
    }

    #[test]
    fn parse_columns_option_rejects_bad_type() {
        assert!(parse_columns_option("x:widget", "id", None).is_err());
        assert!(parse_columns_option("noColon", "id", None).is_err());
    }

    #[tokio::test]
    async fn delete_with_non_pushable_predicate_is_rejected() {
        // The guard must fire at plan time (no network) rather than silently
        // dropping the OR and deleting every row. Uses an explicit schema so
        // registration doesn't touch DynamoDB.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let mut opts = HashMap::new();
        opts.insert("region".to_string(), "us-east-1".to_string());
        let client = build_client("http://localhost:8000", "us-east-1", &opts)
            .await
            .expect("client");
        let provider = DynamoTableProvider::new(client, "t", "id", None, Some(schema), true)
            .await
            .expect("provider");
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider))
            .expect("register");

        let err = ctx
            .sql("DELETE FROM t WHERE id = 'A' OR name = 'x'")
            .await
            .expect("logical plan")
            .collect()
            .await
            .expect_err("non-pushable DELETE predicate must be rejected");
        assert!(
            err.to_string().contains("pushable comparison"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn missing_options_errors() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut ctx = SessionContext::new();
            let err = register_dynamodb_tables(
                &mut ctx,
                "ddb",
                "http://localhost:8000",
                None,
                false,
                HierarchyLevel::Table,
            )
            .await
            .unwrap_err();
            assert!(err.to_string().contains("requires options"));
        });
    }

    #[test]
    fn missing_table_option_errors() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut ctx = SessionContext::new();
            let mut opts = HashMap::new();
            opts.insert("partition_key".to_string(), "id".to_string());
            let err = register_dynamodb_tables(
                &mut ctx,
                "ddb",
                "http://localhost:8000",
                Some(&opts),
                false,
                HierarchyLevel::Table,
            )
            .await
            .unwrap_err();
            assert!(err.to_string().contains("'table'"));
        });
    }

    #[test]
    fn table_mode_explicitly_requires_options() {
        // Explicit table mode (the default) still requires options so the provider
        // can read the `table` name and credentials.
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut ctx = SessionContext::new();
            let err = register_dynamodb_tables(
                &mut ctx,
                "ddb",
                "http://localhost:8000",
                None,
                false,
                HierarchyLevel::Table,
            )
            .await
            .unwrap_err();
            assert!(err.to_string().contains("requires options"));
        });
    }

    #[test]
    fn parse_allowed_tables_absent_means_all_tables() {
        assert!(parse_allowed_tables(None).unwrap().is_none());
        assert!(
            parse_allowed_tables(Some(&HashMap::new()))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn parse_allowed_tables_trims_sorts_and_deduplicates() {
        let mut opts = HashMap::new();
        opts.insert(
            "allowed_tables".to_string(),
            " orders, products,orders , inventory ".to_string(),
        );

        let tables = parse_allowed_tables(Some(&opts))
            .expect("parse allowed_tables")
            .expect("allowed tables");
        assert_eq!(tables, vec!["inventory", "orders", "products"]);
    }

    #[test]
    fn parse_allowed_tables_empty_segments_error() {
        let mut opts = HashMap::new();
        opts.insert("allowed_tables".to_string(), " , , ".to_string());

        let err = parse_allowed_tables(Some(&opts)).unwrap_err();
        assert!(err.to_string().contains("allowed_tables"));
    }

    #[tokio::test]
    #[ignore = "requires DynamoDB Local on :8000"]
    async fn catalog_mode_accepts_empty_options() {
        // Catalog mode discovers tables via ListTables, so it does not need a
        // `table` option. This should reach the network layer without failing
        // option validation.
        let mut ctx = SessionContext::new();
        register_dynamodb_tables(
            &mut ctx,
            "ddb",
            "http://localhost:8000",
            None,
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("catalog registration should not fail due to missing options");
    }

    async fn ensure_catalog_orders_table(client: &Client) {
        use aws_sdk_dynamodb::types::{
            AttributeDefinition, BillingMode, KeySchemaElement, ScalarAttributeType,
        };

        if client
            .describe_table()
            .table_name("orders")
            .send()
            .await
            .is_err()
        {
            client
                .create_table()
                .table_name("orders")
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name("order_id")
                        .attribute_type(ScalarAttributeType::S)
                        .build()
                        .unwrap(),
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("order_id")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                )
                .billing_mode(BillingMode::PayPerRequest)
                .send()
                .await
                .expect("create orders table");
        }

        let put = |order_id: &str, product_id: &str, quantity: i64, status: &str| {
            let client = client.clone();
            let order_id = order_id.to_string();
            let product_id = product_id.to_string();
            let status = status.to_string();
            async move {
                client
                    .put_item()
                    .table_name("orders")
                    .item("order_id", AttributeValue::S(order_id))
                    .item("product_id", AttributeValue::S(product_id))
                    .item("quantity", AttributeValue::N(quantity.to_string()))
                    .item("status", AttributeValue::S(status))
                    .send()
                    .await
                    .expect("put order");
            }
        };

        put("ORD001", "PROD001", 2, "paid").await;
        put("ORD002", "PROD002", 1, "pending").await;
    }

    #[tokio::test]
    #[ignore = "requires DynamoDB Local on :8000 with `products` and `orders` tables"]
    async fn catalog_mode_registers_tables_under_fixed_schema() {
        let mut ctx = SessionContext::new();
        let mut client_opts = HashMap::new();
        client_opts.insert("region".to_string(), "us-east-1".to_string());

        let client = build_client("http://localhost:8000", "us-east-1", &client_opts)
            .await
            .expect("client");
        ensure_catalog_orders_table(&client).await;

        register_dynamodb_tables(
            &mut ctx,
            "ddb",
            "http://localhost:8000",
            None,
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("register catalog");

        // DynamoDB has no native schema layer; Skardi exposes every discovered
        // table under the fixed `tables` schema.
        let df = ctx
            .sql("SELECT * FROM ddb.tables.products LIMIT 1")
            .await
            .expect("plan products");
        let batches = df.collect().await.expect("collect products");
        assert!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>() >= 1,
            "expected products table under ddb.tables"
        );

        let df = ctx
            .sql("SELECT * FROM ddb.tables.orders LIMIT 1")
            .await
            .expect("plan orders");
        let batches = df.collect().await.expect("collect orders");
        assert!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>() >= 1,
            "expected orders table under ddb.tables"
        );
    }

    // ─── Schema shaping (build_schema_fields) ───────────────────────────────

    #[test]
    fn schema_fields_put_keys_first_and_nullable_rest() {
        // Attributes are given out of order and include the keys; the builder must
        // emit partition key, then sort key (both non-nullable), then the rest in
        // declared order (nullable), deduping any attribute that repeats a key.
        let attrs = vec![
            ("name".to_string(), DataType::Utf8),
            ("sk".to_string(), DataType::Int64),
            ("price".to_string(), DataType::Float64),
            ("pk".to_string(), DataType::Utf8),
        ];
        let schema = build_schema_fields("pk", Some("sk"), &attrs);
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["pk", "sk", "name", "price"]);
        assert!(!schema.field(0).is_nullable(), "partition key non-nullable");
        assert!(!schema.field(1).is_nullable(), "sort key non-nullable");
        assert!(schema.field(2).is_nullable(), "non-key column nullable");
        // The sort key's declared type is honored even though it came from attrs.
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
    }

    #[test]
    fn schema_fields_default_key_type_is_utf8_when_unsampled() {
        // A key never seen among the sampled attributes falls back to Utf8 rather
        // than being dropped (an empty table still needs its key columns).
        let schema = build_schema_fields("pk", None, &[]);
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "pk");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn sampled_attribute_merge_is_sorted_and_first_type_wins() {
        let items = vec![
            HashMap::from([
                ("zeta".to_string(), AttributeValue::S("x".into())),
                ("alpha".to_string(), n("1")),
            ]),
            HashMap::from([
                // Repeats `alpha` with a different type: the first observation
                // fixed it, so this one must not flip the column type.
                ("alpha".to_string(), AttributeValue::S("later".into())),
                ("mid".to_string(), AttributeValue::Bool(true)),
            ]),
        ];
        let attrs = merge_sampled_attributes(&items);
        let names: Vec<&str> = attrs.iter().map(|(name, _)| name.as_str()).collect();
        // HashMap iteration order is arbitrary, so the merged list must come
        // back sorted for the inferred schema to be identical across runs.
        assert_eq!(names, vec!["alpha", "mid", "zeta"]);
        assert_eq!(attrs[0].1, DataType::Float64, "first-seen type wins");
        assert_eq!(attrs[1].1, DataType::Boolean);
        assert_eq!(attrs[2].1, DataType::Utf8);
    }

    // ─── Item ⇄ RecordBatch conversion ──────────────────────────────────────

    #[test]
    fn items_to_batch_fills_missing_attributes_with_null() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("price", DataType::Float64, true),
        ]));
        // Second item omits `price` entirely — it must become a NULL cell, not a
        // dropped row or a shifted column.
        let items = vec![
            HashMap::from([
                ("id".to_string(), AttributeValue::S("A".into())),
                ("price".to_string(), n("9.5")),
            ]),
            HashMap::from([("id".to_string(), AttributeValue::S("B".into()))]),
        ];
        let batch = items_to_batch(items, &schema).expect("batch");
        assert_eq!(batch.num_rows(), 2);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let prices = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(ids.value(0), "A");
        assert_eq!(ids.value(1), "B");
        assert_eq!(prices.value(0), 9.5);
        assert!(prices.is_null(1), "missing attribute reads as NULL");
    }

    #[test]
    fn record_batch_to_items_round_trips_and_omits_nulls() {
        // A batch with a NULL cell must produce an item that simply lacks that
        // attribute (DynamoDB has no typed NULL columns), while typed cells map
        // back to the matching AttributeValue variant.
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("qty", DataType::Int64, true),
            Field::new("active", DataType::Boolean, true),
        ]);
        let ids: StringArray = vec![Some("A"), Some("B")].into_iter().collect();
        let qty: Int64Array = vec![Some(7i64), None].into_iter().collect();
        let active: BooleanArray = vec![Some(true), Some(false)].into_iter().collect();
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ids), Arc::new(qty), Arc::new(active)],
        )
        .unwrap();

        let items = record_batch_to_items(&batch, &schema).expect("items");
        assert_eq!(items.len(), 2);
        assert!(matches!(items[0].get("id"), Some(AttributeValue::S(s)) if s == "A"));
        assert!(matches!(items[0].get("qty"), Some(AttributeValue::N(n)) if n == "7"));
        assert!(matches!(
            items[0].get("active"),
            Some(AttributeValue::Bool(true))
        ));
        // Row 1 had a NULL qty → the attribute is absent, not a typed NULL.
        assert!(
            !items[1].contains_key("qty"),
            "NULL cell omits the attribute"
        );
        assert!(matches!(
            items[1].get("active"),
            Some(AttributeValue::Bool(false))
        ));
    }

    #[test]
    fn arrow_array_builders_cover_numeric_and_bool_types() {
        // Only the Utf8 path was covered before; exercise the Int64/Float64/Boolean
        // arms (including a NULL passthrough) so a regression in any is caught.
        let int_vals = vec![Some(n("3")), None];
        let arr = attribute_values_to_arrow_array(&int_vals, &DataType::Int64);
        let ints = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ints.value(0), 3);
        assert!(ints.is_null(1));

        let bool_vals = vec![Some(AttributeValue::Bool(true)), Some(n("1"))];
        let arr = attribute_values_to_arrow_array(&bool_vals, &DataType::Boolean);
        let bools = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bools.value(0));
        // A non-bool attribute in a Boolean column coerces to NULL, never `true`.
        assert!(
            bools.is_null(1),
            "non-bool attribute is NULL in a Boolean column"
        );
    }

    #[test]
    fn av_to_string_formats_scalars_and_maps_null_to_none() {
        assert_eq!(
            av_to_string(&AttributeValue::S("hi".into())).as_deref(),
            Some("hi")
        );
        assert_eq!(av_to_string(&n("9.99")).as_deref(), Some("9.99"));
        assert_eq!(
            av_to_string(&AttributeValue::Bool(true)).as_deref(),
            Some("true")
        );
        // An explicit DynamoDB NULL is None (an Arrow null), not "".
        assert_eq!(av_to_string(&AttributeValue::Null(true)), None);
    }

    #[test]
    fn explicit_null_attribute_is_arrow_null_in_string_columns() {
        // `UPDATE ... SET col = NULL` stores AttributeValue::Null; reading it
        // back must yield a NULL cell, not the empty string "".
        let values = vec![
            Some(AttributeValue::S("a".into())),
            Some(AttributeValue::Null(true)),
        ];
        let arr = attribute_values_to_arrow_array(&values, &DataType::Utf8);
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strs.value(0), "a");
        assert!(strs.is_null(1), "explicit NULL reads as NULL, not \"\"");

        // The fallback arm (unsupported column types render as strings)
        // applies the same rule.
        let arr = attribute_values_to_arrow_array(&values, &DataType::Date32);
        assert!(!arr.is_null(0));
        assert!(arr.is_null(1));
    }

    // ─── Projection / key-condition expression builders ─────────────────────

    #[test]
    fn projection_expression_names_every_field() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("price", DataType::Float64, true),
        ]);
        let (expr, names) = build_projection_expression(&schema);
        // Uses the #p namespace so it can share a request with #n/#k/:v bindings.
        assert_eq!(expr, "#p0, #p1");
        assert_eq!(names.len(), 2);
        assert_eq!(names.get("#p0").map(String::as_str), Some("id"));
        assert_eq!(names.get("#p1").map(String::as_str), Some("price"));
    }

    #[test]
    fn key_condition_uses_k_namespace_and_folds_sort_cond() {
        // Partition-only.
        let f = build_key_condition("pk", AttributeValue::S("A".into()), "sk", None);
        assert_eq!(f.expression, "#k0 = :k0");
        assert_eq!(f.names.get("#k0").map(String::as_str), Some("pk"));
        assert!(f.values.contains_key(":k0"));

        // Partition + sort range → second clause on the #k1/:k1 pair.
        let f = build_key_condition(
            "pk",
            AttributeValue::S("A".into()),
            "sk",
            Some((Operator::Gt, n("5"))),
        );
        assert_eq!(f.expression, "#k0 = :k0 AND #k1 > :k1");
        assert_eq!(f.names.get("#k1").map(String::as_str), Some("sk"));
        assert!(f.values.contains_key(":k1"));
    }

    // ─── Operator / normalization helpers ───────────────────────────────────

    #[test]
    fn normalize_binary_flips_left_literal_and_rejects_unusable() {
        // `5 < price` normalizes to `(price, >, 5)` with the column on the left.
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit(5i64)),
            Operator::Lt,
            Box::new(col("price")),
        ));
        let (col_name, op, _) = normalize_binary(&expr).expect("normalizable");
        assert_eq!(col_name, "price");
        assert_eq!(op, Operator::Gt);

        // Column-to-column has no literal → None.
        assert!(normalize_binary(&col("a").eq(col("b"))).is_none());
        // Pushable-shaped but with an inconvertible literal → None (not a panic).
        let ts = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("a")),
            Operator::Eq,
            Box::new(Expr::Literal(
                ScalarValue::TimestampNanosecond(Some(0), None),
                None,
            )),
        ));
        assert!(normalize_binary(&ts).is_none());
    }

    #[test]
    fn flip_operator_is_a_mirror() {
        assert_eq!(flip_operator(Operator::Lt), Operator::Gt);
        assert_eq!(flip_operator(Operator::LtEq), Operator::GtEq);
        assert_eq!(flip_operator(Operator::Gt), Operator::Lt);
        assert_eq!(flip_operator(Operator::GtEq), Operator::LtEq);
        // Symmetric comparisons are unchanged.
        assert_eq!(flip_operator(Operator::Eq), Operator::Eq);
        assert_eq!(flip_operator(Operator::NotEq), Operator::NotEq);
    }

    #[test]
    fn is_key_condition_op_excludes_not_eq() {
        // `<>` is a legal filter but illegal in a KeyConditionExpression.
        assert!(is_key_condition_op(Operator::Eq));
        assert!(is_key_condition_op(Operator::Gt));
        assert!(is_key_condition_op(Operator::LtEq));
        assert!(!is_key_condition_op(Operator::NotEq));
    }

    #[test]
    fn operator_symbol_rejects_non_comparison() {
        assert_eq!(operator_symbol(Operator::Eq).unwrap(), "=");
        assert_eq!(operator_symbol(Operator::NotEq).unwrap(), "<>");
        // A logical connective is not a DynamoDB comparison operator.
        assert!(operator_symbol(Operator::And).is_err());
    }

    // ─── Value / count builders ─────────────────────────────────────────────

    #[test]
    fn expr_to_attribute_value_rejects_non_literal() {
        // A bare column reference is not a value.
        assert!(expr_to_attribute_value(&col("x")).is_err());
        assert!(matches!(
            expr_to_attribute_value(&lit(3.5f64)).unwrap(),
            AttributeValue::N(s) if s == "3.5"
        ));
    }

    #[test]
    fn scalar_to_attribute_value_maps_float_and_null() {
        assert!(matches!(
            scalar_to_attribute_value(&ScalarValue::Float64(Some(1.5))).unwrap(),
            AttributeValue::N(s) if s == "1.5"
        ));
        assert!(matches!(
            scalar_to_attribute_value(&ScalarValue::Null).unwrap(),
            AttributeValue::Null(true)
        ));
    }

    #[test]
    fn count_batch_is_single_uint64_row() {
        let batch = count_batch(42).expect("count batch");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema().field(0).name(), "count");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::UInt64);
        let counts = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(counts.value(0), 42);
    }

    #[test]
    fn count_plan_properties_describe_single_count_row() {
        // Must stay in lockstep with count_batch()/count_schema(): one bounded,
        // final partition whose schema is the `{count}` row.
        let props = count_plan_properties();
        assert_eq!(
            props.equivalence_properties().schema().as_ref(),
            count_schema().as_ref()
        );
        assert_eq!(props.output_partitioning().partition_count(), 1);
        assert_eq!(props.emission_type, EmissionType::Final);
        assert_eq!(props.boundedness, Boundedness::Bounded);
    }

    #[test]
    fn parse_columns_option_accepts_int_and_bool_aliases() {
        let schema =
            parse_columns_option("qty:integer, active:boolean", "id", None).expect("valid columns");
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
        assert_eq!(schema.field(2).data_type(), &DataType::Boolean);
    }

    // ─── Key extraction / projection (provider methods) ─────────────────────

    /// Build a provider with an explicit schema (no DynamoDB round-trip needed).
    async fn test_provider(
        partition_key: &str,
        sort_key: Option<&str>,
        read_write: bool,
    ) -> DynamoTableProvider {
        let mut fields = vec![Field::new(partition_key, DataType::Utf8, false)];
        if let Some(sk) = sort_key {
            fields.push(Field::new(sk, DataType::Utf8, false));
        }
        fields.push(Field::new("name", DataType::Utf8, true));
        let schema = Arc::new(Schema::new(fields));
        let mut opts = HashMap::new();
        opts.insert("region".to_string(), "us-east-1".to_string());
        let client = build_client("http://localhost:8000", "us-east-1", &opts)
            .await
            .expect("client");
        DynamoTableProvider::new(
            client,
            "t",
            partition_key,
            sort_key,
            Some(schema),
            read_write,
        )
        .await
        .expect("provider")
    }

    #[tokio::test]
    async fn key_of_extracts_full_composite_key_and_drops_non_key_attrs() {
        let handle = test_provider("pk", Some("sk"), false).await.clone_handle();
        let item = HashMap::from([
            ("pk".to_string(), AttributeValue::S("A".into())),
            ("sk".to_string(), AttributeValue::S("B".into())),
            ("name".to_string(), AttributeValue::S("ignored".into())),
        ]);
        let key = handle.key_of(&item).expect("key");
        assert_eq!(key.len(), 2, "only the two key attributes are kept");
        assert!(key.contains_key("pk") && key.contains_key("sk"));
    }

    #[tokio::test]
    async fn key_of_errors_when_sort_key_missing() {
        let handle = test_provider("pk", Some("sk"), false).await.clone_handle();
        // Composite-key table but the item lacks the sort key.
        let item = HashMap::from([("pk".to_string(), AttributeValue::S("A".into()))]);
        let err = handle
            .key_of(&item)
            .expect_err("missing sort key must error");
        assert!(err.to_string().contains("sort key"), "got: {err}");
    }

    #[tokio::test]
    async fn key_projection_covers_partition_and_sort() {
        let single = test_provider("pk", None, false).await.clone_handle();
        let (expr, names) = single.key_projection();
        assert_eq!(expr, "#p0");
        assert_eq!(names.get("#p0").map(String::as_str), Some("pk"));

        let composite = test_provider("pk", Some("sk"), false).await.clone_handle();
        let (expr, names) = composite.key_projection();
        assert_eq!(expr, "#p0, #p1");
        assert_eq!(names.get("#p1").map(String::as_str), Some("sk"));
    }

    // ─── Plan-time write guards (no network) ────────────────────────────────

    async fn register_provider(provider: DynamoTableProvider) -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider))
            .expect("register");
        ctx
    }

    #[tokio::test]
    async fn read_only_source_rejects_delete_at_plan_time() {
        // access_mode enforcement: a read-only source must block DML before any
        // request reaches DynamoDB.
        let ctx = register_provider(test_provider("id", None, false).await).await;
        let err = ctx
            .sql("DELETE FROM t WHERE id = 'A'")
            .await
            .expect("logical plan")
            .collect()
            .await
            .expect_err("read-only DELETE must be rejected");
        assert!(err.to_string().contains("read_only"), "got: {err}");
    }

    #[tokio::test]
    async fn update_of_key_column_is_rejected_at_plan_time() {
        // DynamoDB key attributes are immutable; the guard must fire during
        // planning rather than issuing an UpdateItem that would fail server-side.
        let ctx = register_provider(test_provider("id", None, true).await).await;
        let err = ctx
            .sql("UPDATE t SET id = 'Z' WHERE id = 'A'")
            .await
            .expect("logical plan")
            .collect()
            .await
            .expect_err("updating the key column must be rejected");
        assert!(err.to_string().contains("immutable"), "got: {err}");
    }

    #[tokio::test]
    async fn insert_plan_reports_count_output_not_input_schema() {
        // DynamoInsertExec's execute() streams a single `{count}` batch, so its
        // advertised plan properties must describe that shape — not the input's
        // schema (which is what `input.properties()` would leak).
        let provider = test_provider("id", None, true).await;
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(provider.schema()));
        let ctx = SessionContext::new();
        let plan = provider
            .insert_into(&ctx.state(), input, InsertOp::Append)
            .await
            .expect("insert plan");
        assert_eq!(plan.schema().as_ref(), count_schema().as_ref());
        assert_eq!(plan.properties().output_partitioning().partition_count(), 1);

        // Optimizer passes rebuild nodes via with_new_children; the count
        // shape must survive the rewrite.
        let rebuilt = plan
            .with_new_children(vec![Arc::new(EmptyExec::new(provider.schema()))])
            .expect("with_new_children");
        assert_eq!(rebuilt.schema().as_ref(), count_schema().as_ref());
    }

    // ─── Integration tests (require DynamoDB Local) ─────────────────────────
    //
    // Run a local endpoint first:
    //   docker run -d -p 8000:8000 amazon/dynamodb-local:2.5.2
    // then seed the `products` table (see docs/dynamodb/README.md) and run:
    //   AWS_ACCESS_KEY_ID=dummy AWS_SECRET_ACCESS_KEY=dummy \
    //     cargo nextest run --all-features -- --ignored

    async fn ci_provider() -> DynamoTableProvider {
        let mut opts = HashMap::new();
        opts.insert("region".to_string(), "us-east-1".to_string());
        let client = build_client("http://localhost:8000", "us-east-1", &opts)
            .await
            .expect("client");
        DynamoTableProvider::new(client, "products", "product_id", None, None, false)
            .await
            .expect("provider")
    }

    async fn query_rows(sql: &str) -> usize {
        let mut ctx = SessionContext::new();
        let mut opts = HashMap::new();
        opts.insert("table".to_string(), "products".to_string());
        opts.insert("partition_key".to_string(), "product_id".to_string());
        opts.insert("region".to_string(), "us-east-1".to_string());
        register_dynamodb_tables(
            &mut ctx,
            "products",
            "http://localhost:8000",
            Some(&opts),
            false,
            HierarchyLevel::Table,
        )
        .await
        .expect("register");
        let df = ctx.sql(sql).await.expect("sql");
        let batches = df.collect().await.expect("collect");
        batches.iter().map(|b| b.num_rows()).sum()
    }

    #[tokio::test]
    #[ignore = "requires DynamoDB Local on :8000 with seeded `products` table"]
    async fn integration_scan_all() {
        let provider = ci_provider().await;
        assert!(!provider.schema.fields().is_empty());
        let rows = query_rows("SELECT * FROM products").await;
        assert!(rows >= 3, "expected seeded rows, got {rows}");
    }

    #[tokio::test]
    #[ignore = "requires DynamoDB Local on :8000 with seeded `products` table"]
    async fn integration_filter_pushdown() {
        let rows = query_rows("SELECT * FROM products WHERE category = 'Electronics'").await;
        assert!(rows >= 1, "expected at least one Electronics row");
    }

    #[tokio::test]
    #[ignore = "requires DynamoDB Local on :8000 with seeded `products` table"]
    async fn integration_count_star() {
        let rows = query_rows("SELECT count(*) FROM products").await;
        // count(*) returns a single aggregate row
        assert_eq!(rows, 1);
    }

    /// Create a throwaway single-key table for write tests (idempotent).
    async fn ensure_dml_table(client: &Client, table: &str) {
        use aws_sdk_dynamodb::types::{
            AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType,
        };
        if client
            .describe_table()
            .table_name(table)
            .send()
            .await
            .is_ok()
        {
            return;
        }
        client
            .create_table()
            .table_name(table)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("id")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("id")
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
            .send()
            .await
            .expect("create dml table");
    }

    #[tokio::test]
    #[ignore = "requires DynamoDB Local on :8000"]
    async fn integration_insert_update_delete_round_trip() {
        let table = "skardi_dml_roundtrip";
        let mut opts = HashMap::new();
        opts.insert("table".to_string(), table.to_string());
        opts.insert("partition_key".to_string(), "id".to_string());
        opts.insert("region".to_string(), "us-east-1".to_string());

        let client = build_client("http://localhost:8000", "us-east-1", &opts)
            .await
            .expect("client");
        ensure_dml_table(&client, table).await;

        // Provider declares an explicit schema so writes have stable types even
        // when the table is empty (inference can't see absent attributes).
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("price", DataType::Float64, true),
        ]));

        async fn ctx_with(table: &str, schema: SchemaRef, client: Client) -> SessionContext {
            let provider = DynamoTableProvider::new(client, table, "id", None, Some(schema), true)
                .await
                .expect("provider");
            let ctx = SessionContext::new();
            ctx.register_table(table, Arc::new(provider))
                .expect("register");
            ctx
        }

        let run = |sql: String| {
            let schema = schema.clone();
            let client = client.clone();
            async move {
                let ctx = ctx_with(table, schema, client).await;
                ctx.sql(&sql)
                    .await
                    .expect("sql")
                    .collect()
                    .await
                    .expect("collect")
            }
        };

        // Clean slate
        run(format!("DELETE FROM {table}")).await;

        // INSERT
        run(format!(
            "INSERT INTO {table} (id, name, price) VALUES ('A1', 'Widget', 9.99)"
        ))
        .await;
        let after_insert: usize = run(format!("SELECT * FROM {table} WHERE id = 'A1'"))
            .await
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(after_insert, 1, "row should exist after insert");

        // UPDATE
        run(format!("UPDATE {table} SET price = 19.99 WHERE id = 'A1'")).await;
        let batches = run(format!("SELECT price FROM {table} WHERE id = 'A1'")).await;
        let price = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("price is f64")
            .value(0);
        assert_eq!(price, 19.99, "price should be updated");

        // DELETE
        run(format!("DELETE FROM {table} WHERE id = 'A1'")).await;
        let after_delete: usize = run(format!("SELECT * FROM {table} WHERE id = 'A1'"))
            .await
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(after_delete, 0, "row should be gone after delete");
    }

    /// Create a composite-key (HASH + RANGE) table for Query-path tests.
    async fn ensure_composite_table(client: &Client, table: &str) {
        use aws_sdk_dynamodb::types::{AttributeDefinition, KeySchemaElement, ScalarAttributeType};
        if client
            .describe_table()
            .table_name(table)
            .send()
            .await
            .is_ok()
        {
            return;
        }
        client
            .create_table()
            .table_name(table)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("pk")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("sk")
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("pk")
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("sk")
                    .key_type(KeyType::Range)
                    .build()
                    .unwrap(),
            )
            .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
            .send()
            .await
            .expect("create composite table");
    }

    /// End-to-end coverage of the Query path (partition-only predicate) and the
    /// composite-key GetItem path (pk + sk equality) against DynamoDB Local. Also
    /// exercises DescribeTable-based sort-key auto-detection via registration.
    #[tokio::test]
    #[ignore = "requires DynamoDB Local on :8000"]
    async fn integration_composite_key_query_and_get() {
        let table = "skardi_composite";
        let mut opts = HashMap::new();
        opts.insert("table".to_string(), table.to_string());
        opts.insert("region".to_string(), "us-east-1".to_string());

        let client = build_client("http://localhost:8000", "us-east-1", &opts)
            .await
            .expect("client");
        ensure_composite_table(&client, table).await;

        // Seed three items in one partition + one in another.
        let put = |pk: &str, sk: i64| {
            let client = client.clone();
            let table = table.to_string();
            let pk = pk.to_string();
            async move {
                client
                    .put_item()
                    .table_name(&table)
                    .item("pk", AttributeValue::S(pk))
                    .item("sk", AttributeValue::N(sk.to_string()))
                    .send()
                    .await
                    .expect("put");
            }
        };
        put("A", 1).await;
        put("A", 2).await;
        put("A", 3).await;
        put("B", 1).await;

        // Register via the public path so DescribeTable auto-detects the sort key
        // (note: no partition_key/sort_key options supplied).
        let mut ctx = SessionContext::new();
        register_dynamodb_tables(
            &mut ctx,
            table,
            "http://localhost:8000",
            Some(&opts),
            false,
            HierarchyLevel::Table,
        )
        .await
        .expect("register");

        let rows = |sql: String| {
            let ctx = &ctx;
            async move {
                ctx.sql(&sql)
                    .await
                    .expect("sql")
                    .collect()
                    .await
                    .expect("collect")
                    .iter()
                    .map(|b| b.num_rows())
                    .sum::<usize>()
            }
        };

        // Partition-only predicate → Query path → all 3 items in partition A.
        assert_eq!(
            rows(format!("SELECT * FROM {table} WHERE pk = 'A'")).await,
            3
        );
        // Full composite key by equality → GetItem path → exactly one item.
        assert_eq!(
            rows(format!("SELECT * FROM {table} WHERE pk = 'A' AND sk = 2")).await,
            1
        );
        // Partition + sort range → Query path with a sort condition → 2 items.
        assert_eq!(
            rows(format!("SELECT * FROM {table} WHERE pk = 'A' AND sk >= 2")).await,
            2
        );
    }
}
