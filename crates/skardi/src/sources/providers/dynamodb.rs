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
use std::collections::HashMap;
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
use aws_sdk_dynamodb::types::AttributeValue;
use datafusion::catalog::Session;
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
use tokio::sync::RwLock;

/// A DynamoDB table exposed to DataFusion as a `TableProvider`.
pub struct DynamoTableProvider {
    client: Client,
    table_name: String,
    schema: SchemaRef,
    /// Partition (hash) key attribute name. Always present and not nullable.
    partition_key: String,
    /// Sort (range) key attribute name, if the table has a composite key.
    sort_key: Option<String>,
}

impl Debug for DynamoTableProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamoTableProvider")
            .field("table_name", &self.table_name)
            .field("partition_key", &self.partition_key)
            .field("sort_key", &self.sort_key)
            .field("schema", &self.schema)
            .finish()
    }
}

impl DynamoTableProvider {
    /// Build a provider against an existing DynamoDB table, inferring the schema
    /// from a sampled item unless one is supplied.
    pub async fn new(
        client: Client,
        table_name: &str,
        partition_key: &str,
        sort_key: Option<&str>,
        schema: Option<SchemaRef>,
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
        })
    }

    /// Infer an Arrow schema by sampling a single item from the table. The
    /// partition key (then the sort key, if any) are always emitted first and
    /// non-nullable; remaining attributes are inferred from the sample and
    /// marked nullable, since DynamoDB items are schemaless and any attribute
    /// may be absent on other items.
    async fn infer_schema(
        client: &Client,
        table_name: &str,
        partition_key: &str,
        sort_key: Option<&str>,
    ) -> Result<Schema> {
        let sample = client
            .scan()
            .table_name(table_name)
            .limit(1)
            .send()
            .await
            .with_context(|| format!("Failed to sample DynamoDB table '{table_name}'"))?;

        let item = sample.items().first();

        let mut fields: Vec<Field> = Vec::new();
        let mut seen: Vec<&str> = Vec::new();

        // Key attributes first, in key order, non-nullable.
        let pk_type = item
            .and_then(|i| i.get(partition_key))
            .map(attribute_value_to_arrow_type)
            .unwrap_or(DataType::Utf8);
        fields.push(Field::new(partition_key, pk_type, false));
        seen.push(partition_key);

        if let Some(sk) = sort_key {
            let sk_type = item
                .and_then(|i| i.get(sk))
                .map(attribute_value_to_arrow_type)
                .unwrap_or(DataType::Utf8);
            fields.push(Field::new(sk, sk_type, false));
            seen.push(sk);
        }

        if let Some(item) = item {
            for (key, value) in item.iter() {
                if seen.contains(&key.as_str()) {
                    continue;
                }
                fields.push(Field::new(key, attribute_value_to_arrow_type(value), true));
            }
        } else {
            tracing::warn!(
                table = %table_name,
                "DynamoDB table is empty; schema limited to declared key attributes"
            );
        }

        Ok(Schema::new(fields))
    }

    /// Run a (optionally filtered) `Scan`, paginating until the table is
    /// exhausted, and return every matching item.
    async fn scan_items(
        &self,
        filter: Option<&DynamoFilter>,
        limit: Option<usize>,
    ) -> Result<Vec<HashMap<String, AttributeValue>>> {
        let mut items: Vec<HashMap<String, AttributeValue>> = Vec::new();
        let mut start_key: Option<HashMap<String, AttributeValue>> = None;

        loop {
            let mut req = self.client.scan().table_name(&self.table_name);
            if let Some(f) = filter {
                req = req
                    .filter_expression(&f.expression)
                    .set_expression_attribute_names(Some(f.names.clone()))
                    .set_expression_attribute_values(Some(f.values.clone()));
            }
            if let Some(sk) = &start_key {
                req = req.set_exclusive_start_key(Some(sk.clone()));
            }

            let out = req
                .send()
                .await
                .with_context(|| format!("DynamoDB scan failed for '{}'", self.table_name))?;

            items.extend(out.items().iter().cloned());

            if matches!(limit, Some(n) if items.len() >= n) {
                if let Some(n) = limit {
                    items.truncate(n);
                }
                break;
            }

            match out.last_evaluated_key() {
                Some(key) if !key.is_empty() => start_key = Some(key.clone()),
                _ => break,
            }
        }

        Ok(items)
    }

    /// Convert scanned DynamoDB items into a single Arrow `RecordBatch` shaped
    /// by this provider's schema. Missing attributes become NULLs.
    fn items_to_record_batch(
        &self,
        items: Vec<HashMap<String, AttributeValue>>,
    ) -> Result<RecordBatch> {
        let mut columns: HashMap<String, Vec<Option<AttributeValue>>> = HashMap::new();
        for field in self.schema.fields() {
            columns.insert(field.name().clone(), Vec::with_capacity(items.len()));
        }

        for item in &items {
            for field in self.schema.fields() {
                let value = item.get(field.name()).cloned();
                columns
                    .get_mut(field.name())
                    .expect("column inserted for every schema field above")
                    .push(value);
            }
        }

        let arrays: Vec<ArrayRef> = self
            .schema
            .fields()
            .iter()
            .map(|field| {
                let values = columns
                    .get(field.name())
                    .expect("column inserted for every schema field above");
                attribute_values_to_arrow_array(values, field.data_type())
            })
            .collect();

        RecordBatch::try_new(self.schema.clone(), arrays)
            .with_context(|| "Failed to build RecordBatch from DynamoDB items")
    }
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
        let pushable: Vec<Expr> = filters
            .iter()
            .filter(|e| is_pushable_binary_filter(e))
            .cloned()
            .collect();
        let filter = build_filter_expression(&pushable)?;

        let items = self
            .scan_items(filter.as_ref(), limit)
            .await
            .map_err(|e| DataFusionError::External(e.into()))?;

        let batch = self
            .items_to_record_batch(items)
            .map_err(|e| DataFusionError::External(e.into()))?;

        let batch = if let Some(proj) = projection {
            let projected_schema = Arc::new(self.schema.project(proj)?);
            let columns: Vec<ArrayRef> = proj.iter().map(|&i| batch.column(i).clone()).collect();
            if proj.is_empty() {
                // Empty projection (e.g. `count(*)`) needs the row count passed
                // explicitly or RecordBatch::try_new rejects the zero-column batch.
                let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
                RecordBatch::try_new_with_options(projected_schema, columns, &options)?
            } else {
                RecordBatch::try_new(projected_schema, columns)?
            }
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
        Ok(Arc::new(DynamoScanExec {
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
        Ok(Arc::new(DynamoInsertExec {
            input,
            client: self.client.clone(),
            table_name: self.table_name.clone(),
            schema: self.schema.clone(),
        }))
    }

    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let pushable: Vec<Expr> = filters
            .iter()
            .filter(|e| is_pushable_binary_filter(e))
            .cloned()
            .collect();
        let filter = build_filter_expression(&pushable)?;
        Ok(Arc::new(DynamoDmlExec::new(
            self.clone_handle(),
            DynamoDmlOp::Delete { filter },
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
        let mut sets: Vec<(String, AttributeValue)> = Vec::with_capacity(assignments.len());
        for (col, expr) in &assignments {
            if col == &self.partition_key || Some(col) == self.sort_key.as_ref() {
                return Err(DataFusionError::Plan(format!(
                    "Cannot modify key column '{col}' — DynamoDB key attributes are immutable"
                )));
            }
            sets.push((col.clone(), expr_to_attribute_value(expr)?));
        }

        let pushable: Vec<Expr> = filters
            .iter()
            .filter(|e| is_pushable_binary_filter(e))
            .cloned()
            .collect();
        let filter = build_filter_expression(&pushable)?;

        Ok(Arc::new(DynamoDmlExec::new(
            self.clone_handle(),
            DynamoDmlOp::Update { filter, sets },
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

    /// Scan keys of items matching `filter` (used to drive key-based DML).
    async fn matching_keys(
        &self,
        filter: Option<&DynamoFilter>,
    ) -> Result<Vec<HashMap<String, AttributeValue>>> {
        let mut keys = Vec::new();
        let mut start_key: Option<HashMap<String, AttributeValue>> = None;
        loop {
            let mut req = self.client.scan().table_name(&self.table_name);
            if let Some(f) = filter {
                req = req
                    .filter_expression(&f.expression)
                    .set_expression_attribute_names(Some(f.names.clone()))
                    .set_expression_attribute_values(Some(f.values.clone()));
            }
            if let Some(sk) = &start_key {
                req = req.set_exclusive_start_key(Some(sk.clone()));
            }
            let out = req
                .send()
                .await
                .with_context(|| format!("DynamoDB scan failed for '{}'", self.table_name))?;
            for item in out.items() {
                keys.push(self.key_of(item)?);
            }
            match out.last_evaluated_key() {
                Some(k) if !k.is_empty() => start_key = Some(k.clone()),
                _ => break,
            }
        }
        Ok(keys)
    }
}

// ─── Schema inference & type mapping ────────────────────────────────────────

/// Map a sampled DynamoDB attribute to an Arrow type. Numbers are inferred as
/// `Int64` when the sample has no fractional part, else `Float64`. Everything
/// non-scalar (maps, lists, sets, binary) falls back to `Utf8`.
pub(crate) fn attribute_value_to_arrow_type(value: &AttributeValue) -> DataType {
    match value {
        AttributeValue::S(_) => DataType::Utf8,
        AttributeValue::Bool(_) => DataType::Boolean,
        AttributeValue::N(n) => {
            if n.contains('.') || n.contains('e') || n.contains('E') {
                DataType::Float64
            } else if n.parse::<i64>().is_ok() {
                DataType::Int64
            } else {
                DataType::Float64
            }
        }
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
                .map(|v| v.as_ref().map(av_to_string))
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
                .map(|v| v.as_ref().map(av_to_string))
                .collect();
            Arc::new(arr)
        }
    }
}

fn av_to_string(v: &AttributeValue) -> String {
    match v {
        AttributeValue::S(s) => s.clone(),
        AttributeValue::N(n) => n.clone(),
        AttributeValue::Bool(b) => b.to_string(),
        AttributeValue::Null(_) => String::new(),
        other => format!("{other:?}"),
    }
}

fn av_to_i64(v: &AttributeValue) -> Option<i64> {
    match v {
        AttributeValue::N(n) => n
            .parse::<i64>()
            .ok()
            .or_else(|| n.parse::<f64>().ok().map(|f| f as i64)),
        AttributeValue::S(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn av_to_f64(v: &AttributeValue) -> Option<f64> {
    match v {
        AttributeValue::N(n) => n.parse::<f64>().ok(),
        AttributeValue::S(s) => s.parse::<f64>().ok(),
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
struct DynamoFilter {
    expression: String,
    names: HashMap<String, String>,
    values: HashMap<String, AttributeValue>,
}

/// Same predicate the MongoDB provider uses: a comparison between a bare column
/// and a literal, with an operator DynamoDB can evaluate server-side.
pub(crate) fn is_pushable_binary_filter(expr: &Expr) -> bool {
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

// ─── Scan execution plan ────────────────────────────────────────────────────

/// Leaf plan holding a pre-fetched scan result batch.
#[derive(Debug)]
struct DynamoScanExec {
    schema: SchemaRef,
    batch: Arc<RwLock<Option<RecordBatch>>>,
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

// ─── Insert execution plan ──────────────────────────────────────────────────

struct DynamoInsertExec {
    input: Arc<dyn ExecutionPlan>,
    client: Client,
    table_name: String,
    schema: SchemaRef,
}

impl Debug for DynamoInsertExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamoInsertExec")
            .field("table_name", &self.table_name)
            .finish()
    }
}

impl DisplayAs for DynamoInsertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "DynamoInsertExec")
    }
}

impl ExecutionPlan for DynamoInsertExec {
    fn name(&self) -> &str {
        "DynamoInsertExec"
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
        Ok(Arc::new(DynamoInsertExec {
            input: children[0].clone(),
            client: self.client.clone(),
            table_name: self.table_name.clone(),
            schema: self.schema.clone(),
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
        let output_schema = count_schema();

        let future = async move {
            let mut count: u64 = 0;
            use futures::StreamExt;
            while let Some(batch) = input_stream.next().await {
                let batch = batch?;
                let items = record_batch_to_items(&batch, &schema)
                    .map_err(|e| DataFusionError::External(e.into()))?;
                for item in items {
                    client
                        .put_item()
                        .table_name(&table_name)
                        .set_item(Some(item))
                        .send()
                        .await
                        .map_err(|e| {
                            DataFusionError::Execution(format!("DynamoDB put_item failed: {e}"))
                        })?;
                    count += 1;
                }
            }
            count_batch(count)
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            futures::stream::once(future),
        )))
    }
}

// ─── DELETE / UPDATE execution plan ─────────────────────────────────────────

enum DynamoDmlOp {
    Delete {
        filter: Option<DynamoFilter>,
    },
    Update {
        filter: Option<DynamoFilter>,
        sets: Vec<(String, AttributeValue)>,
    },
}

/// Leaf plan that runs a key-based DELETE or UPDATE and returns `{ count }`.
///
/// DynamoDB cannot delete or update by arbitrary predicate, so the matching
/// keys are scanned first and then mutated one key at a time.
struct DynamoDmlExec {
    handle: DynamoHandle,
    op: DynamoDmlOp,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl DynamoDmlExec {
    fn new(handle: DynamoHandle, op: DynamoDmlOp) -> Self {
        let schema = count_schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            handle,
            op,
            schema,
            properties,
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
        // Move the op out by cloning its parts (AttributeValue/maps are clonable).
        let op = self.op.clone_op();
        let future = async move {
            let affected = match op {
                DynamoDmlOp::Delete { filter } => {
                    let keys = handle
                        .matching_keys(filter.as_ref())
                        .await
                        .map_err(|e| DataFusionError::External(e.into()))?;
                    let mut n = 0u64;
                    for key in keys {
                        handle
                            .client
                            .delete_item()
                            .table_name(&handle.table_name)
                            .set_key(Some(key))
                            .send()
                            .await
                            .map_err(|e| {
                                DataFusionError::Execution(format!("DynamoDB delete failed: {e}"))
                            })?;
                        n += 1;
                    }
                    n
                }
                DynamoDmlOp::Update { filter, sets } => {
                    let keys = handle
                        .matching_keys(filter.as_ref())
                        .await
                        .map_err(|e| DataFusionError::External(e.into()))?;
                    let (expr, names, values) = build_update_expression(&sets);
                    let mut n = 0u64;
                    for key in keys {
                        handle
                            .client
                            .update_item()
                            .table_name(&handle.table_name)
                            .set_key(Some(key))
                            .update_expression(&expr)
                            .set_expression_attribute_names(Some(names.clone()))
                            .set_expression_attribute_values(Some(values.clone()))
                            .send()
                            .await
                            .map_err(|e| {
                                DataFusionError::Execution(format!("DynamoDB update failed: {e}"))
                            })?;
                        n += 1;
                    }
                    n
                }
            };
            count_batch(affected)
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::once(future),
        )))
    }
}

impl DynamoDmlOp {
    fn clone_op(&self) -> DynamoDmlOp {
        match self {
            DynamoDmlOp::Delete { filter } => DynamoDmlOp::Delete {
                filter: filter.as_ref().map(clone_filter),
            },
            DynamoDmlOp::Update { filter, sets } => DynamoDmlOp::Update {
                filter: filter.as_ref().map(clone_filter),
                sets: sets.clone(),
            },
        }
    }
}

fn clone_filter(f: &DynamoFilter) -> DynamoFilter {
    DynamoFilter {
        expression: f.expression.clone(),
        names: f.names.clone(),
        values: f.values.clone(),
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

fn count_batch(count: u64) -> DFResult<RecordBatch> {
    let array: UInt64Array = vec![count].into();
    RecordBatch::try_new(count_schema(), vec![Arc::new(array)]).map_err(DataFusionError::from)
}

// ─── Registration ───────────────────────────────────────────────────────────

/// Register a DynamoDB table as a DataFusion table.
///
/// # Arguments
/// * `session_ctx` - session context to register the table into.
/// * `name` - the SQL table name to expose.
/// * `connection_string` - the DynamoDB endpoint URL. For Amazon DynamoDB use
///   the regional endpoint (e.g. `https://dynamodb.us-east-1.amazonaws.com`);
///   for DynamoDB Local use `http://localhost:8000`.
/// * `options` - configuration options (see below).
///
/// # Options
/// * `table` - DynamoDB table name (required).
/// * `partition_key` - partition (hash) key attribute name (required).
/// * `sort_key` - sort (range) key attribute name (optional).
/// * `region` - AWS region (optional, default `us-east-1`).
/// * `access_key_env` / `secret_key_env` - names of environment variables
///   holding static AWS credentials (optional). When omitted, the default AWS
///   credential provider chain is used.
pub async fn register_dynamodb_tables(
    session_ctx: &mut SessionContext,
    name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
) -> Result<()> {
    tracing::info!(
        source = %name,
        endpoint = %connection_string,
        "Registering DynamoDB table"
    );

    let opts = options.ok_or_else(|| {
        anyhow::anyhow!("DynamoDB data source '{name}' requires options (table, partition_key)")
    })?;

    let table = opts
        .get("table")
        .ok_or_else(|| anyhow::anyhow!("DynamoDB data source '{name}' requires 'table' option"))?;
    let partition_key = opts.get("partition_key").ok_or_else(|| {
        anyhow::anyhow!("DynamoDB data source '{name}' requires 'partition_key' option")
    })?;
    let sort_key = opts.get("sort_key").map(String::as_str);
    let region = opts
        .get("region")
        .cloned()
        .unwrap_or_else(|| "us-east-1".to_string());

    let client = build_client(connection_string, &region, opts).await?;

    let provider = DynamoTableProvider::new(client, table, partition_key, sort_key, None)
        .await
        .with_context(|| format!("Failed to create DynamoDB table provider for '{name}'"))?;

    session_ctx
        .register_table(name, Arc::new(provider))
        .with_context(|| format!("Failed to register DynamoDB table '{name}' with DataFusion"))?;

    tracing::info!(source = %name, table = %table, "Registered DynamoDB table");
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{BinaryExpr, col, lit};

    fn n(s: &str) -> AttributeValue {
        AttributeValue::N(s.to_string())
    }

    #[test]
    fn attribute_type_inference() {
        assert_eq!(
            attribute_value_to_arrow_type(&AttributeValue::S("x".into())),
            DataType::Utf8
        );
        assert_eq!(attribute_value_to_arrow_type(&n("42")), DataType::Int64);
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
        assert_eq!(av_to_i64(&n("7.9")), Some(7));
        assert_eq!(av_to_f64(&n("2.5")), Some(2.5));
        assert_eq!(av_to_f64(&AttributeValue::S("nope".into())), None);
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
    fn missing_options_errors() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut ctx = SessionContext::new();
            let err = register_dynamodb_tables(&mut ctx, "ddb", "http://localhost:8000", None)
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
            let err =
                register_dynamodb_tables(&mut ctx, "ddb", "http://localhost:8000", Some(&opts))
                    .await
                    .unwrap_err();
            assert!(err.to_string().contains("'table'"));
        });
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
        DynamoTableProvider::new(client, "products", "product_id", None, None)
            .await
            .expect("provider")
    }

    async fn query_rows(sql: &str) -> usize {
        let mut ctx = SessionContext::new();
        let mut opts = HashMap::new();
        opts.insert("table".to_string(), "products".to_string());
        opts.insert("partition_key".to_string(), "product_id".to_string());
        opts.insert("region".to_string(), "us-east-1".to_string());
        register_dynamodb_tables(&mut ctx, "products", "http://localhost:8000", Some(&opts))
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
            let provider = DynamoTableProvider::new(client, table, "id", None, Some(schema))
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
}
