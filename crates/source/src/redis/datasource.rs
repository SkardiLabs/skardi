use std::{
    any::Any,
    collections::HashMap,
    fmt::{self, Debug, Formatter},
    sync::{Arc, RwLock},
};

use anyhow::{Error, Result};
use arrow::{
    array::{
        ArrayRef, RecordBatch, StringBuilder, as_boolean_array, as_largestring_array,
        as_string_array,
    },
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::cast::{
        as_binary_array, as_float16_array, as_float32_array, as_float64_array, as_int8_array,
        as_int16_array, as_int32_array, as_int64_array, as_large_binary_array, as_uint8_array,
        as_uint16_array, as_uint32_array, as_uint64_array,
    },
    datasource::TableType,
    datasource::sink::{DataSink, DataSinkExec},
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::dml::InsertOp,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        memory::MemoryStream,
    },
    prelude::Expr,
};
use derivative::Derivative;
use futures::TryStreamExt;
use redis::{Commands, ConnectionLike, Iter};
use uuid::Uuid;

/// Enum representing the storage format of data in Redis.
/// Currently supports Hash (each row as a Redis hash). Can be extended to JSON, etc.
#[derive(Debug, Clone)]
pub enum RedisStorage {
    Hash,
}

/// The main TableProvider implementation for Redis.
///
#[derive(Derivative)]
#[derivative(Debug)]
pub struct RedisTable<C>
where
    C: ConnectionLike + Commands + Send + Sync + 'static,
{
    #[derivative(Debug = "ignore")]
    conn: Arc<RwLock<C>>,
    key_space: String,
    table_name: String,
    storage: RedisStorage,
    schema: SchemaRef,
    key_column: Option<String>,
}

impl<C> RedisTable<C>
where
    C: ConnectionLike + Commands + Send + Sync + 'static,
{
    /// Create a new RedisTable by connecting to Redis and retrieving/inferencing the schema.
    pub fn new(
        mut conn: C,
        key_space: String,
        table_name: String,
        storage: RedisStorage,
        key_column: Option<String>,
    ) -> Result<Self> {
        let prefix = if key_space.is_empty() {
            table_name.clone()
        } else {
            format!("{}:{}", key_space, table_name)
        };

        let mut fields: Vec<Field> = vec![];
        let mut inferred_fields: Vec<String> = vec![];

        // TODO: Implement read from schema_key

        let pattern = format!("{}:*", prefix);
        let mut iter: Iter<String> = conn
            .scan_match(&pattern)
            .map_err(|e| DataFusionError::Execution(format!("Redis SCAN error: {}", e)))?;
        if let Some(sample_key_result) = iter.next() {
            let sample_key = sample_key_result
                .map_err(|e| DataFusionError::Execution(format!("Redis SCAN error: {}", e)))?;
            // We found a sample key for this table; retrieve its fields
            let entries: Vec<(String, String)> = conn
                .hgetall(sample_key)
                .map_err(|e| DataFusionError::Execution(format!("Redis HGETALL error: {}", e)))?;
            // TODO: Implement refer schema from other RedisStorage other than hash, when added
            for (field_name, _v) in entries.iter() {
                inferred_fields.push(field_name.to_string());
            }
        }
        if inferred_fields.is_empty() {
            return Err(Error::new(DataFusionError::Plan(format!(
                "Could not determine schema for Redis table '{}'; no schema key or data found",
                table_name
            ))));
        }

        // Sort field names to have a deterministic order (not strictly necessary, but for consistency).
        inferred_fields.sort();

        // If a key column is expected (used as part of key, not stored as field), include it in schema.
        if let Some(ref key_col) = key_column {
            // If the key column was not present in the hash fields, add it.
            if !inferred_fields.iter().any(|f| f == key_col) {
                fields.push(Field::new(key_col, DataType::Utf8, false));
            }
        }

        // Add all inferred fields(except maybe the key column if it was part of them? In Spark's case, key column is not in hash).
        for field in &inferred_fields {
            // If key_column is set and equals this field, skip it here because we added it already as non-nullable.
            if let Some(ref key_col) = key_column {
                if field == key_col {
                    continue;
                }
            }
            // All values are stored as strings in Redis; mark as Utf8 (could refine if schema info available).
            // TODO: Support all dataType not only limit to UTF-8
            fields.push(Field::new(field, DataType::Utf8, true));
        }

        // Construct schema
        let schema = Arc::new(Schema::new(fields));
        Ok(RedisTable {
            conn: Arc::new(RwLock::new(conn)),
            key_space,
            table_name,
            storage,
            schema,
            key_column,
        })
    }
}

#[async_trait]
impl<C> TableProvider for RedisTable<C>
where
    C: ConnectionLike + Commands + Send + Sync + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }
    /// Return the schema of the table (Arrow SchemaRef).
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Scan the table: create an ExecutionPlan that will read the data from Redis.
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // TODO: Utilize state
        // Determine projected schema if projection push-down is requested
        let projected_schema = if let Some(indicies) = projection {
            let fieds: Vec<Field> = indicies
                .iter()
                .map(|&i| self.schema.field(i).clone())
                .collect();
            Arc::new(Schema::new(fieds))
        } else {
            self.schema.clone()
        };

        // Create the execution plan for scanning Redis
        let properties = PlanProperties::new(
            EquivalenceProperties::new(self.schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Both,
            Boundedness::Bounded,
        );
        let exec = RedisScanExec {
            conn: self.conn.clone(),
            key_space: self.key_space.clone(),
            table_name: self.table_name.clone(),
            storage: self.storage.clone(),
            projected_schema,
            projection: projection
                .cloned()
                .unwrap_or_else(|| (0..self.schema.fields().len()).collect()),
            filters: filters.to_owned(),
            limit,
            properties,
        };
        Ok(Arc::new(exec))
    }

    /// Insert into the table: return an ExecutionPlan that will write input data to Redis.
    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // TODO: Utilize state
        let sink = RedisSink {
            conn: self.conn.clone(),
            key_space: self.key_space.clone(),
            table_name: self.table_name.clone(),
            storage: self.storage.clone(),
            schema: Arc::clone(&self.schema),
            insert_op,
            key_column: self.key_column.clone(),
        };
        // Wrap in DataSinkExec to execute insertion. The DataSinkExec will handle combining input and writing.
        Ok(Arc::new(DataSinkExec::new(input, Arc::new(sink), None)))
    }
}

/// ExecutionPlan for scanning a Redis table (reads Redis hashes and outputs Arrow RecordBatches).
#[derive(Derivative)]
#[derivative(Debug)]
struct RedisScanExec<C>
where
    C: ConnectionLike + Commands + Send + Sync + 'static,
{
    #[derivative(Debug = "ignore")]
    conn: Arc<RwLock<C>>,
    key_space: String,
    table_name: String,
    storage: RedisStorage,
    projected_schema: SchemaRef,
    projection: Vec<usize>,
    filters: Vec<Expr>,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl<C> RedisScanExec<C>
where
    C: ConnectionLike + Commands + Send + Sync + 'static,
{
    /// Helper to gather all record batches from Redis for a given partition (node).
    fn fetch_partition(&self, _partition_idx: usize) -> datafusion::common::Result<RecordBatch> {
        // TODO: Utilize partition_idx
        let prefix = if self.key_space.is_empty() {
            self.table_name.clone()
        } else {
            format!("{}:{}", self.key_space, self.table_name)
        };
        let pattern = format!("{}:*", prefix);

        // Prepare builders for each projected column
        let mut builders: Vec<StringBuilder> = self
            .projected_schema
            .fields()
            .iter()
            .map(|_| StringBuilder::new())
            .collect();

        // Scan through all keys for this partition
        let mut conn_write = self.conn.try_write().map_err(|e| {
            DataFusionError::Execution(format!(
                "failed to acquire write lock of redis connection {}",
                e
            ))
        })?;
        let keys: Vec<String> = conn_write
            .scan_match(&pattern)
            .map_err(|e| DataFusionError::Execution(format!("Redis SCAN error: {}", e)))?
            .collect::<Result<Vec<String>, _>>()
            .map_err(|e| DataFusionError::Execution(format!("Redis SCAN error: {}", e)))?;

        let mut count: usize = 0;
        for key in keys {
            // Optionally apply a limit to stop early
            if let Some(max) = self.limit {
                if count >= max {
                    break;
                }
            }

            // Fetch the hash fields for this key
            let redis_map: HashMap<String, String> = conn_write
                .hgetall(&key)
                .map_err(|e| DataFusionError::Execution(format!("Redis HGETALL error: {}", e)))?;
            // If a key column was used (i.e., the key itself encodes a field), reconstruct that field if projected.
            // For now, we assume all data fields are in the hash (the key field would have been stored as a hash field if no key_column optimization used).
            // TODO: Add other RedisStorage support other than hash
            for (j, field) in self.projected_schema.fields().iter().enumerate() {
                let field_name = field.name();
                let value = redis_map.get(field_name).map(|v| v.to_string());
                // If the field is not found in the hash, it might be the key itself.
                let cell_value = if let Some(val) = value {
                    val
                } else {
                    "".to_string() // default empty string for missing fields (could also use NULL)
                };
                builders[j].append_value(cell_value);
            }
            count += 1;
        }

        // Finish building arrays and create a RecordBatch
        let arrays: Vec<ArrayRef> = builders
            .into_iter()
            .map(|mut b| Arc::new(b.finish()) as ArrayRef)
            .collect();

        RecordBatch::try_new(self.projected_schema.clone(), arrays)
            .map_err(|e| DataFusionError::Execution(format!("Error building RecordBatch: {}", e)))
    }
}

impl<C> ExecutionPlan for RedisScanExec<C>
where
    C: ConnectionLike + Commands + Send + Sync + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn name(&self) -> &str {
        "redis execution"
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
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(RedisScanExec {
            conn: self.conn.clone(),
            key_space: self.key_space.clone(),
            table_name: self.table_name.clone(),
            storage: self.storage.clone(),
            projected_schema: Arc::clone(&self.projected_schema),
            projection: self.projection.clone(),
            filters: self.filters.clone(),
            limit: self.limit,
            properties: self.properties.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        // Fetch the data for the given partition (synchronously, in this implementation).
        // TODO: Utilize context
        let batch = self.fetch_partition(partition)?;
        let schema = self.schema();
        let output = vec![batch];
        // TODO: Find proper ways to create SendableRecordBatchStream, find proper projections
        Ok(Box::pin(MemoryStream::try_new(output, schema, None)?))
    }
}

impl<C> DisplayAs for RedisScanExec<C>
where
    C: ConnectionLike + Commands + Send + Sync + 'static,
{
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default => write!(
                f,
                "RedisScanExec: table={}, projected_cols={:?}, filters={:?}, limit={:?}",
                self.table_name, self.projection, self.filters, self.limit
            ),
            _ => write!(f, "RedisScanExec"),
        }
    }
}

/// DataSink implementation for writing data into Redis.
#[derive(Derivative)]
#[derivative(Debug)]
struct RedisSink<C>
where
    C: ConnectionLike + Commands + Send + Sync + 'static,
{
    #[derivative(Debug = "ignore")]
    conn: Arc<RwLock<C>>,
    key_space: String,
    table_name: String,
    storage: RedisStorage,
    schema: SchemaRef,
    insert_op: InsertOp,
    key_column: Option<String>,
}

impl<C> DisplayAs for RedisSink<C>
where
    C: ConnectionLike + Commands + Send + Sync + 'static,
{
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        match t {
            // a "compact" or default one‐liner
            DisplayFormatType::Default => {
                write!(f, "RedisSink({})", self.table_name)
            }

            // the "verbose" form, dumping every field
            DisplayFormatType::Verbose => {
                write!(
                    f,
                    "RedisSink {{ \
                     key_space:    {:?}, \
                     table:     \"{}\", \
                     storage:   {:?}, \
                     schema:    {:?}, \
                     insert_op: {:?}, \
                     key_col:   {:?} \
                     }}",
                    self.key_space,
                    self.table_name,
                    self.storage,
                    self.schema,
                    self.insert_op,
                    self.key_column
                )
            }

            // tree render format (new in datafusion 50)
            DisplayFormatType::TreeRender => {
                write!(f, "RedisSink({})", self.table_name)
            }
        }
    }
}

#[async_trait]
impl<C> DataSink for RedisSink<C>
where
    C: ConnectionLike + Commands + Send + Sync + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Consume a stream of RecordBatches and write all rows to Redis. Returns the count of rows written.
    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        // TODO: Utilize context, intergrate underneath scaler, aggregate, window function, etc

        let prefix = if self.key_space.is_empty() {
            self.table_name.clone()
        } else {
            format!("{}:{}", self.key_space, self.table_name)
        };

        // If Overwrite or Replace, delete existing keys for this table first
        if self.insert_op == InsertOp::Overwrite || self.insert_op == InsertOp::Replace {
            let pattern = format!("{}:*", prefix);
            let mut conn_write = self.conn.try_write().map_err(|e| {
                DataFusionError::Execution(format!(
                    "failed to acquire write lock of redis connection: {}",
                    e
                ))
            })?;
            // Use SCAN to find keys and delete them
            let mut iter: Iter<String> = conn_write
                .scan_match(&pattern)
                .map_err(|e| DataFusionError::Execution(format!("Redis SCAN error: {}", e)))?;
            let mut del_keys: Vec<String> = vec![];
            // TODO: Try to add a batch size here, not delete everything at once.
            while let Some(k_result) = iter.next() {
                let k = k_result
                    .map_err(|e| DataFusionError::Execution(format!("Redis SCAN error: {}", e)))?;
                del_keys.push(k);
            }
            if !del_keys.is_empty() {
                let _: () = conn_write
                    .del(&del_keys)
                    .map_err(|e| DataFusionError::Execution(format!("Redis SCAN error: {}", e)))?;
            }
        }

        // Iterate through all record batches and write each row
        let mut total_rows: u64 = 0;
        while let Some(batch) = data.try_next().await? {
            // TODO: Implement write with other RdisStorage type, when added
            let records = batch;
            let num_rows = records.num_rows();
            let num_cols = records.num_columns();
            // Pre-extract arrays for efficiency
            let columns: Vec<ArrayRef> = (0..num_cols).map(|i| records.column(i).clone()).collect();
            // Determine index of key_column if any (to avoid storing it as field if it's the key).
            let key_col_index = self
                .key_column
                .as_ref()
                .and_then(|col_name| records.schema().index_of(col_name).ok());

            for row_idx in 0..num_rows {
                // Determine the Redis key for this row
                let id_value = if let Some(idx) = key_col_index {
                    // Use the value of the key column as the ID
                    array_value_to_string(&columns[idx], row_idx)?
                } else {
                    Uuid::new_v4().to_string()
                };
                let redis_key = format!("{}:{}", prefix, id_value);
                let mut fileds = vec![];
                let schema = records.schema();
                for col_idx in 0..num_cols {
                    if Some(col_idx) == key_col_index {
                        continue;
                    }
                    let field_name = schema.field(col_idx).name();
                    let value_str = array_value_to_string(&columns[col_idx], row_idx)?;
                    fileds.push((field_name.as_str(), value_str))
                }
                let mut conn_write = self.conn.try_write().map_err(|e| {
                    DataFusionError::Execution(format!(
                        "failed to acquire write lock of redis connection: {}",
                        e
                    ))
                })?;
                let _: () = conn_write.hset_multiple(&redis_key, &fileds).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to HSET row to Redis: {}", e))
                })?;
            }
            total_rows += num_rows as u64;
        }

        Ok(total_rows)
    }
}

/// Helper function to convert a value at a given row of an Arrow array to a string.
/// TODO: Move this to a common place instead of only for redis
fn array_value_to_string(array: &ArrayRef, row: usize) -> datafusion::common::Result<String> {
    if array.is_null(row) {
        return Ok("".to_string());
    }
    // Match on common data types and convert to string
    let str = match array.data_type() {
        DataType::Utf8 => {
            let str_arr = as_string_array(array);
            str_arr.value(row).to_string()
        }
        DataType::LargeUtf8 => {
            let str_arr = as_largestring_array(array);
            str_arr.value(row).to_string()
        }
        DataType::Int64 => {
            let int_arr = as_int64_array(array).inspect_err(|e| {
                DataFusionError::Execution(format!("DataType conversion error {}", e));
            })?;
            int_arr.value(row).to_string()
        }
        DataType::Int32 => {
            let int_arr = as_int32_array(array).inspect_err(|e| {
                DataFusionError::Execution(format!("DataType conversion error {}", e));
            })?;
            int_arr.value(row).to_string()
        }
        DataType::Int16 => {
            let int_arr = as_int16_array(array).inspect_err(|e| {
                DataFusionError::Execution(format!("DataType conversion error {}", e));
            })?;
            int_arr.value(row).to_string()
        }
        DataType::Int8 => {
            let int_arr = as_int8_array(array).inspect_err(|e| {
                DataFusionError::Execution(format!("DataType conversion error {}", e));
            })?;
            int_arr.value(row).to_string()
        }
        DataType::UInt64 => {
            let int_arr = as_uint64_array(array).inspect_err(|e| {
                DataFusionError::Execution(format!("DataType conversion error {}", e));
            })?;
            int_arr.value(row).to_string()
        }
        DataType::UInt32 => {
            let int_arr = as_uint32_array(array).inspect_err(|e| {
                DataFusionError::Execution(format!("DataType conversion error {}", e));
            })?;
            int_arr.value(row).to_string()
        }
        DataType::UInt16 => {
            let int_arr = as_uint16_array(array).inspect_err(|e| {
                DataFusionError::Execution(format!("DataType conversion error {}", e));
            })?;
            int_arr.value(row).to_string()
        }
        DataType::UInt8 => {
            let int_arr = as_uint8_array(array).inspect_err(|e| {
                DataFusionError::Execution(format!("DataType conversion error {}", e));
            })?;
            int_arr.value(row).to_string()
        }
        DataType::Float64 => {
            let float_arr = as_float64_array(array).inspect_err(|e| {
                DataFusionError::Execution(format!("DataType conversion error {}", e));
            })?;
            float_arr.value(row).to_string()
        }
        DataType::Float32 => {
            let float_arr = as_float32_array(array).inspect_err(|e| {
                DataFusionError::Execution(format!("DataType conversion error {}", e));
            })?;
            float_arr.value(row).to_string()
        }
        DataType::Float16 => {
            let float_arr = as_float16_array(array).inspect_err(|e| {
                DataFusionError::Execution(format!("DataType conversion error {}", e));
            })?;
            float_arr.value(row).to_string()
        }
        DataType::Boolean => {
            let bool_arr = as_boolean_array(array);
            bool_arr.value(row).to_string()
        }
        DataType::Binary => {
            let bin_array = as_binary_array(array).inspect_err(|e| {
                DataFusionError::Execution(format!("DataType conversion error {}", e));
            })?;
            let bytes = bin_array.value(row);
            hex::encode(bytes)
        }
        DataType::LargeBinary => {
            let bin_array = as_large_binary_array(array).inspect_err(|e| {
                DataFusionError::Execution(format!("DataType conversion error {}", e));
            })?;
            let bytes = bin_array.value(row);
            hex::encode(bytes)
        }
        _other => {
            // Fallback: use debug representation of ScalarValue
            // TODO: Add conversion for all other types
            format!("{:?}", array.slice(row, 1).as_ref())
        }
    };

    Ok(str)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use arrow::{
        array::{Float64Array, Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
        util::pretty,
    };
    use datafusion::{prelude::SessionContext, test_util::bounded_stream};
    use redis::Value;
    use redis_test::{MockCmd, MockRedisConnection};

    /// Mock for the SCAN + HGETALL calls in the "person" read test.
    fn make_person_mock(prefix: &str) -> MockRedisConnection {
        // SCAN → return two keys as BulkStrings in an Array
        let scan = MockCmd::new(
            redis::cmd("SCAN")
                .arg(0)
                .arg("MATCH")
                .arg(format!("{prefix}:*"))
                .clone(),
            Ok(Value::Array(vec![
                Value::BulkString(format!("{prefix}:1").into_bytes()),
                Value::BulkString(format!("{prefix}:2").into_bytes()),
            ])),
        );

        let scan_dup = MockCmd::new(
            redis::cmd("SCAN")
                .arg(0)
                .arg("MATCH")
                .arg(format!("{prefix}:*"))
                .clone(),
            Ok(Value::Array(vec![
                Value::BulkString(format!("{prefix}:1").into_bytes()),
                Value::BulkString(format!("{prefix}:2").into_bytes()),
            ])),
        );

        // HGETALL itest:person:1 → alternating BulkString entries
        let h1 = MockCmd::new(
            redis::cmd("HGETALL").arg(format!("{prefix}:1")).clone(),
            Ok(Value::Array(vec![
                Value::BulkString(b"id".to_vec()),
                Value::BulkString(b"1".to_vec()),
                Value::BulkString(b"name".to_vec()),
                Value::BulkString(b"Alice".to_vec()),
                Value::BulkString(b"age".to_vec()),
                Value::BulkString(b"30".to_vec()),
                Value::BulkString(b"city".to_vec()),
                Value::BulkString(b"seattle".to_vec()),
            ])),
        );

        let h1_dup = MockCmd::new(
            redis::cmd("HGETALL").arg(format!("{prefix}:1")).clone(),
            Ok(Value::Array(vec![
                Value::BulkString(b"id".to_vec()),
                Value::BulkString(b"1".to_vec()),
                Value::BulkString(b"name".to_vec()),
                Value::BulkString(b"Alice".to_vec()),
                Value::BulkString(b"age".to_vec()),
                Value::BulkString(b"30".to_vec()),
                Value::BulkString(b"city".to_vec()),
                Value::BulkString(b"seattle".to_vec()),
            ])),
        );

        // HGETALL itest:person:2
        let h2 = MockCmd::new(
            redis::cmd("HGETALL").arg(format!("{prefix}:2")).clone(),
            Ok(Value::Array(vec![
                Value::BulkString(b"id".to_vec()),
                Value::BulkString(b"2".to_vec()),
                Value::BulkString(b"name".to_vec()),
                Value::BulkString(b"Bob".to_vec()),
                Value::BulkString(b"age".to_vec()),
                Value::BulkString(b"35".to_vec()),
                Value::BulkString(b"city".to_vec()),
                Value::BulkString(b"Denver".to_vec()),
            ])),
        );

        MockRedisConnection::new(vec![scan, h1, scan_dup, h1_dup, h2])
    }

    #[tokio::test]
    async fn manual_write_then_table_read_mock() -> Result<()> {
        let keyspace = "itest";
        let table = "person";
        let prefix = format!("{keyspace}:{table}");

        // inject our mock
        let mock_conn = make_person_mock(&prefix);
        let rtable = RedisTable::new(
            mock_conn,
            keyspace.to_string(),
            table.to_string(),
            RedisStorage::Hash,
            None,
        )?;

        let ctx = SessionContext::new();
        ctx.register_table(table, Arc::new(rtable))?;

        let df = ctx
            .sql("SELECT id, name, age, city FROM person ORDER BY id")
            .await?;
        let batches = df.collect().await?;
        println!("{}", pretty::pretty_format_batches(&batches).unwrap());

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ages = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Alice");
        assert_eq!(names.value(1), "Bob");
        assert_eq!(ages.value(0), "30");
        assert_eq!(ages.value(1), "35");
        Ok(())
    }

    /// Mock the SCAN (empty) + HSET calls in the "orders" write test.
    fn make_orders_write_mock(prefix: &str) -> MockRedisConnection {
        // start with an empty SCAN so purge_prefix thinks nothing to delete
        let scan = MockCmd::new(
            redis::cmd("SCAN")
                .arg(0)
                .arg("MATCH")
                .arg(format!("{prefix}:*"))
                .clone(),
            Ok(Value::Array(vec![])),
        );
        // HSET prefix:A1 ...
        let h1 = MockCmd::new(
            redis::cmd("HMSET")
                .arg(format!("{prefix}:A1"))
                .arg("item")
                .arg("Widget")
                .arg("qty")
                .arg("10")
                .arg("price")
                .arg("19.99")
                .clone(),
            Ok(Value::Okay),
        );
        // HSET prefix:A2 ...
        let h2 = MockCmd::new(
            redis::cmd("HMSET")
                .arg(format!("{prefix}:A2"))
                .arg("item")
                .arg("Gadget")
                .arg("qty")
                .arg("20")
                .arg("price")
                .arg("29.95")
                .clone(),
            Ok(Value::Okay),
        );
        MockRedisConnection::new(vec![scan, h1, h2])
    }

    /// Mock the HGETALL calls for manual verification in the "orders" test.
    fn make_orders_read_mock(prefix: &str) -> MockRedisConnection {
        // HGETALL prefix:A1
        let h1 = MockCmd::new(
            redis::cmd("HGETALL").arg(format!("{prefix}:A1")).clone(),
            Ok(Value::Array(vec![
                Value::BulkString(b"item".to_vec()),
                Value::BulkString(b"Widget".to_vec()),
                Value::BulkString(b"qty".to_vec()),
                Value::BulkString(b"10".to_vec()),
                Value::BulkString(b"price".to_vec()),
                Value::BulkString(b"19.99".to_vec()),
            ])),
        );
        // HGETALL prefix:A2
        let h2 = MockCmd::new(
            redis::cmd("HGETALL").arg(format!("{prefix}:A2")).clone(),
            Ok(Value::Array(vec![
                Value::BulkString(b"item".to_vec()),
                Value::BulkString(b"Gadget".to_vec()),
                Value::BulkString(b"qty".to_vec()),
                Value::BulkString(b"20".to_vec()),
                Value::BulkString(b"price".to_vec()),
                Value::BulkString(b"29.95".to_vec()),
            ])),
        );
        MockRedisConnection::new(vec![h1, h2])
    }

    #[tokio::test]
    async fn table_write_then_manual_read_mock() -> Result<()> {
        let keyspace = "itest";
        let table = "orders";
        let prefix = format!("{keyspace}:{table}");

        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Utf8, false),
            Field::new("item", DataType::Utf8, true),
            Field::new("qty", DataType::Int32, true),
            Field::new("price", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["A1", "A2"])) as _,
                Arc::new(StringArray::from(vec!["Widget", "Gadget"])) as _,
                Arc::new(Int32Array::from(vec![10, 20])) as _,
                Arc::new(Float64Array::from(vec![19.99, 29.95])) as _,
            ],
        )?;

        // 1) write
        let write_conn = make_orders_write_mock(&prefix);
        let sink = RedisSink {
            conn: Arc::new(RwLock::new(write_conn)),
            key_space: keyspace.to_string(),
            table_name: table.into(),
            storage: RedisStorage::Hash,
            schema: schema.clone(),
            insert_op: InsertOp::Overwrite,
            key_column: Some("order_id".into()),
        };
        let stream = bounded_stream(batch, 1);
        let rows_written = sink
            .write_all(stream, &Arc::new(TaskContext::default()))
            .await?;
        assert_eq!(rows_written, 2);

        // 2) read back manually
        let mut read_conn = make_orders_read_mock(&prefix);
        let k1: HashMap<String, String> = read_conn.hgetall(format!("{prefix}:A1"))?;
        assert_eq!(k1.get("item").unwrap(), "Widget");
        assert_eq!(k1.get("qty").unwrap(), "10");
        assert_eq!(k1.get("price").unwrap(), "19.99");

        let k2: HashMap<String, String> = read_conn.hgetall(format!("{prefix}:A2"))?;
        assert_eq!(k2.get("item").unwrap(), "Gadget");
        assert_eq!(k2.get("qty").unwrap(), "20");
        assert_eq!(k2.get("price").unwrap(), "29.95");

        Ok(())
    }
}
