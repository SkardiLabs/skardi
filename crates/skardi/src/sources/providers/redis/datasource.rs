use std::{
    any::Any,
    collections::HashMap,
    fmt::{self, Debug, Formatter},
    sync::{Arc, RwLock},
};

use anyhow::Result;
use arrow::{
    array::{
        ArrayRef, RecordBatch, RecordBatchOptions, StringBuilder, UInt64Array, as_boolean_array,
        as_largestring_array, as_string_array,
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
    logical_expr::{Operator, dml::InsertOp},
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

use super::relation::RedisRelation;

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
    /// Cached schema. When the table is empty at registration, this starts as a minimal
    /// schema (key column only). On each `schema()` call, if the schema has no data fields,
    /// we re-infer from Redis — once data appears, the schema is cached permanently.
    schema: RwLock<SchemaRef>,
    key_column: Option<String>,
}

impl<C> RedisTable<C>
where
    C: ConnectionLike + Commands + Send + Sync + 'static,
{
    /// Create a new RedisTable by connecting to Redis and retrieving/inferencing the schema.
    /// If the table is empty in Redis, registration still succeeds — using `columns` if
    /// provided, or a minimal schema (just the key column) otherwise. The schema is
    /// re-inferred dynamically when empty, so it picks up new fields after the first INSERT.
    ///
    /// `columns` — optional list of column names to declare the schema for empty tables.
    /// This allows INSERT operations to work before any data exists in Redis.
    pub fn new(
        conn: C,
        key_space: String,
        table_name: String,
        storage: RedisStorage,
        key_column: Option<String>,
        columns: Option<Vec<String>>,
    ) -> Result<Self> {
        let conn = Arc::new(RwLock::new(conn));
        let mut schema = Self::infer_schema(&conn, &key_space, &table_name, &key_column)?;

        // If the table is empty and explicit columns were declared, use them as the schema.
        let has_data_fields = match &key_column {
            Some(_) => schema.fields().len() > 1,
            None => !schema.fields().is_empty(),
        };
        if !has_data_fields {
            if let Some(cols) = columns {
                let mut fields: Vec<Field> = vec![];
                for col in &cols {
                    let nullable = key_column.as_ref() != Some(col);
                    fields.push(Field::new(col, DataType::Utf8, nullable));
                }
                schema = Arc::new(Schema::new(fields));
            }
        }

        Ok(RedisTable {
            conn,
            key_space,
            table_name,
            storage,
            schema: RwLock::new(schema),
            key_column,
        })
    }

    /// Infer the schema from existing Redis data. Returns a minimal schema (just the key
    /// column) if the table is empty — SELECT will return empty results and INSERT will
    /// work using the incoming data's schema.
    fn infer_schema(
        conn: &Arc<RwLock<C>>,
        key_space: &str,
        table_name: &str,
        key_column: &Option<String>,
    ) -> Result<SchemaRef> {
        let pattern = RedisRelation::table_data_key_pattern(key_space, table_name);

        let mut fields: Vec<Field> = vec![];
        let mut inferred_fields: Vec<String> = vec![];

        // TODO: Implement read from schema_key

        let mut conn_write = conn.try_write().map_err(|e| {
            DataFusionError::Execution(format!(
                "failed to acquire write lock of redis connection: {}",
                e
            ))
        })?;
        // Collect the first key from SCAN, then drop the iterator to release the borrow
        // so we can call HGETALL on the same connection.
        let sample_key: Option<String> =
            {
                let mut iter: Iter<String> = conn_write
                    .scan_match(&pattern)
                    .map_err(|e| DataFusionError::Execution(format!("Redis SCAN error: {}", e)))?;
                match iter.next() {
                    Some(result) => Some(result.map_err(|e| {
                        DataFusionError::Execution(format!("Redis SCAN error: {}", e))
                    })?),
                    None => None,
                }
            };
        if let Some(sample_key) = sample_key {
            let entries: Vec<(String, String)> = conn_write
                .hgetall(sample_key)
                .map_err(|e| DataFusionError::Execution(format!("Redis HGETALL error: {}", e)))?;
            // TODO: Implement refer schema from other RedisStorage other than hash, when added
            for (field_name, _v) in entries.iter() {
                inferred_fields.push(field_name.to_string());
            }
        }
        drop(conn_write);

        // Sort field names to have a deterministic order (not strictly necessary, but for consistency).
        inferred_fields.sort();

        // If a key column is expected (used as part of key, not stored as field), include it in schema.
        if let Some(key_col) = key_column {
            // If the key column was not present in the hash fields, add it.
            if !inferred_fields.iter().any(|f| f == key_col) {
                fields.push(Field::new(key_col, DataType::Utf8, false));
            }
        }

        // Add all inferred fields (except maybe the key column if it was part of them).
        for field in &inferred_fields {
            // If key_column is set and equals this field, skip it here because we added it already as non-nullable.
            if let Some(key_col) = key_column {
                if field == key_col {
                    continue;
                }
            }
            // All values are stored as strings in Redis; mark as Utf8 (could refine if schema info available).
            // TODO: Support all dataType not only limit to UTF-8
            fields.push(Field::new(field, DataType::Utf8, true));
        }

        Ok(Arc::new(Schema::new(fields)))
    }

    /// Return the cached schema. If the cached schema has no data fields (empty table),
    /// re-infer from Redis — once data appears, the result is cached permanently.
    fn current_schema(&self) -> SchemaRef {
        let cached = match self.schema.read() {
            Ok(guard) => guard.clone(),
            Err(_) => return Arc::new(Schema::empty()),
        };
        let has_data_fields = match &self.key_column {
            Some(_) => cached.fields().len() > 1,
            None => !cached.fields().is_empty(),
        };
        if has_data_fields {
            return cached;
        }
        // Schema has no data fields — try to re-infer from Redis
        if let Ok(new_schema) = Self::infer_schema(
            &self.conn,
            &self.key_space,
            &self.table_name,
            &self.key_column,
        ) {
            let new_has_data = match &self.key_column {
                Some(_) => new_schema.fields().len() > 1,
                None => !new_schema.fields().is_empty(),
            };
            if new_has_data {
                // Data appeared — cache the real schema permanently
                if let Ok(mut guard) = self.schema.write() {
                    *guard = new_schema.clone();
                }
                return new_schema;
            }
        }
        cached
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
    /// Re-infers the schema from Redis on each call so that new fields added by INSERT
    /// are visible without a server restart.
    fn schema(&self) -> SchemaRef {
        self.current_schema()
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
        // Use the current (dynamic) schema so we pick up fields added after registration
        let current_schema = self.current_schema();

        // Determine projected schema if projection push-down is requested
        let projected_schema = if let Some(indicies) = projection {
            let fieds: Vec<Field> = indicies
                .iter()
                .map(|&i| current_schema.field(i).clone())
                .collect();
            Arc::new(Schema::new(fieds))
        } else {
            current_schema.clone()
        };

        // Create the execution plan for scanning Redis
        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
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
                .unwrap_or_else(|| (0..current_schema.fields().len()).collect()),
            key_column: self.key_column.clone(),
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
            schema: self.current_schema(),
            insert_op,
            key_column: self.key_column.clone(),
        };
        // Wrap in DataSinkExec to execute insertion. The DataSinkExec will handle combining input and writing.
        Ok(Arc::new(DataSinkExec::new(input, Arc::new(sink), None)))
    }

    /// Delete rows matching the given filters.
    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: Vec<Expr>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let mut conn = self.conn.try_write().map_err(|e| {
            DataFusionError::Execution(format!(
                "failed to acquire write lock of redis connection: {}",
                e
            ))
        })?;
        let current_schema = self.current_schema();
        let keys = resolve_matching_keys(
            &mut *conn,
            &self.key_space,
            &self.table_name,
            &current_schema,
            &self.key_column,
            &filters,
        )?;
        drop(conn);
        Ok(Arc::new(RedisDmlExec::new(
            self.conn.clone(),
            RedisDmlOp::Delete(keys),
        )))
    }

    /// Update rows matching the given filters with the specified assignments.
    async fn update(
        &self,
        _state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if assignments.is_empty() {
            return Err(DataFusionError::Plan(
                "UPDATE requires at least one assignment".to_string(),
            ));
        }
        let fields = assignments_to_redis_fields(&assignments, &self.key_column)?;
        let mut conn = self.conn.try_write().map_err(|e| {
            DataFusionError::Execution(format!(
                "failed to acquire write lock of redis connection: {}",
                e
            ))
        })?;
        let current_schema = self.current_schema();
        let keys = resolve_matching_keys(
            &mut *conn,
            &self.key_space,
            &self.table_name,
            &current_schema,
            &self.key_column,
            &filters,
        )?;
        drop(conn);
        Ok(Arc::new(RedisDmlExec::new(
            self.conn.clone(),
            RedisDmlOp::Update(keys, fields),
        )))
    }
}

// ─── Filter / Assignment helpers ────────────────────────────────────────────

/// Resolve which Redis keys match the given filter expressions.
///
/// **Fast path**: if the only filter is `key_column = 'literal'`, construct the key directly.
/// **Slow path**: SCAN all keys, HGETALL each, evaluate filters in-memory.
fn resolve_matching_keys<C: ConnectionLike + Commands>(
    conn: &mut C,
    key_space: &str,
    table_name: &str,
    schema: &SchemaRef,
    key_column: &Option<String>,
    filters: &[Expr],
) -> datafusion::common::Result<Vec<String>> {
    let prefix = RedisRelation::prefix(key_space, table_name);

    // Fast path: key_column equality
    if let Some(key_col) = key_column {
        if let Some(literal_val) = extract_key_column_eq(filters, key_col) {
            return Ok(vec![format!("{}:{}", prefix, literal_val)]);
        }
    }

    // Slow path: scan all keys and filter in-memory
    let pattern = RedisRelation::table_data_key_pattern(key_space, table_name);
    let keys: Vec<String> = conn
        .scan_match(&pattern)
        .map_err(|e| DataFusionError::Execution(format!("Redis SCAN error: {}", e)))?
        .collect::<Result<Vec<String>, _>>()
        .map_err(|e| DataFusionError::Execution(format!("Redis SCAN error: {}", e)))?;

    if filters.is_empty() {
        return Ok(keys);
    }

    let mut matched = Vec::new();
    for key in keys {
        let redis_map: HashMap<String, String> = conn
            .hgetall(&key)
            .map_err(|e| DataFusionError::Execution(format!("Redis HGETALL error: {}", e)))?;

        // Reconstruct the full row including key_column if present
        let mut row = redis_map;
        if let Some(key_col) = key_column {
            let id = key
                .strip_prefix(&format!("{}:", prefix))
                .unwrap_or(&key)
                .to_string();
            row.insert(key_col.clone(), id);
        }

        if evaluate_filters(&row, filters, schema)? {
            matched.push(key);
        }
    }

    Ok(matched)
}

/// If filters contain exactly one `Column(key_col) = Literal(string)`, extract the literal value.
fn extract_key_column_eq(filters: &[Expr], key_col: &str) -> Option<String> {
    if filters.len() != 1 {
        return None;
    }
    match &filters[0] {
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(col), Expr::Literal(scalar, _)) if col.name() == key_col => {
                    scalar_to_string(scalar)
                }
                (Expr::Literal(scalar, _), Expr::Column(col)) if col.name() == key_col => {
                    scalar_to_string(scalar)
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Evaluate all filter expressions against a row represented as a HashMap.
fn evaluate_filters(
    row: &HashMap<String, String>,
    filters: &[Expr],
    _schema: &SchemaRef,
) -> datafusion::common::Result<bool> {
    for filter in filters {
        if !evaluate_expr(row, filter)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Evaluate a single filter expression against a row.
fn evaluate_expr(row: &HashMap<String, String>, expr: &Expr) -> datafusion::common::Result<bool> {
    match expr {
        Expr::BinaryExpr(binary) => match binary.op {
            Operator::And => {
                Ok(evaluate_expr(row, &binary.left)? && evaluate_expr(row, &binary.right)?)
            }
            Operator::Or => {
                Ok(evaluate_expr(row, &binary.left)? || evaluate_expr(row, &binary.right)?)
            }
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => {
                let left_val = resolve_value(row, &binary.left)?;
                let right_val = resolve_value(row, &binary.right)?;
                let cmp = left_val.cmp(&right_val);
                let result = match binary.op {
                    Operator::Eq => cmp == std::cmp::Ordering::Equal,
                    Operator::NotEq => cmp != std::cmp::Ordering::Equal,
                    Operator::Lt => cmp == std::cmp::Ordering::Less,
                    Operator::LtEq => cmp != std::cmp::Ordering::Greater,
                    Operator::Gt => cmp == std::cmp::Ordering::Greater,
                    Operator::GtEq => cmp != std::cmp::Ordering::Less,
                    _ => unreachable!(),
                };
                Ok(result)
            }
            _ => Err(DataFusionError::Plan(format!(
                "Unsupported filter operator for Redis: {:?}",
                binary.op
            ))),
        },
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported filter expression for Redis: {expr}"
        ))),
    }
}

/// Resolve the string value of an expression in the context of a row.
fn resolve_value(row: &HashMap<String, String>, expr: &Expr) -> datafusion::common::Result<String> {
    match expr {
        Expr::Column(col) => Ok(row.get(col.name()).cloned().unwrap_or_default()),
        Expr::Literal(scalar, _) => scalar_to_string(scalar).ok_or_else(|| {
            DataFusionError::Plan(format!("Unsupported literal type for Redis: {scalar}"))
        }),
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported expression in Redis filter: {expr}"
        ))),
    }
}

/// Convert a ScalarValue to a String.
fn scalar_to_string(scalar: &datafusion::common::ScalarValue) -> Option<String> {
    use datafusion::common::ScalarValue;
    match scalar {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        ScalarValue::Int8(Some(v)) => Some(v.to_string()),
        ScalarValue::Int16(Some(v)) => Some(v.to_string()),
        ScalarValue::Int32(Some(v)) => Some(v.to_string()),
        ScalarValue::Int64(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt8(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt16(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt32(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt64(Some(v)) => Some(v.to_string()),
        ScalarValue::Float32(Some(v)) => Some(v.to_string()),
        ScalarValue::Float64(Some(v)) => Some(v.to_string()),
        ScalarValue::Boolean(Some(v)) => Some(v.to_string()),
        _ => None,
    }
}

/// Convert UPDATE assignments to Redis hash field-value pairs.
fn assignments_to_redis_fields(
    assignments: &[(String, Expr)],
    key_column: &Option<String>,
) -> datafusion::common::Result<Vec<(String, String)>> {
    let mut fields = Vec::with_capacity(assignments.len());
    for (col, expr) in assignments {
        if let Some(key_col) = key_column {
            if col == key_col {
                return Err(DataFusionError::Plan(format!(
                    "Cannot update key column '{}'; this would require deleting and re-inserting the row",
                    key_col
                )));
            }
        }
        let value = match expr {
            Expr::Literal(scalar, _) => scalar_to_string(scalar).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Unsupported literal type for Redis UPDATE: {scalar}"
                ))
            })?,
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "Redis UPDATE only supports literal values, got: {expr}"
                )));
            }
        };
        fields.push((col.clone(), value));
    }
    Ok(fields)
}

// ─── DML execution plan ─────────────────────────────────────────────────────

/// The kind of DML operation to execute.
#[derive(Debug, Clone)]
enum RedisDmlOp {
    /// Delete the given Redis keys.
    Delete(Vec<String>),
    /// Update the given Redis keys with field-value pairs.
    Update(Vec<String>, Vec<(String, String)>),
}

/// A leaf [`ExecutionPlan`] that executes a Redis DML operation and returns
/// a single row `{ count: u64 }` with the number of affected keys.
#[derive(Derivative)]
#[derivative(Debug)]
struct RedisDmlExec<C>
where
    C: ConnectionLike + Commands + Send + Sync + 'static,
{
    #[derivative(Debug = "ignore")]
    conn: Arc<RwLock<C>>,
    op: RedisDmlOp,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl<C> RedisDmlExec<C>
where
    C: ConnectionLike + Commands + Send + Sync + 'static,
{
    fn new(conn: Arc<RwLock<C>>, op: RedisDmlOp) -> Self {
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
            conn,
            op,
            schema,
            properties,
        }
    }
}

impl<C> DisplayAs for RedisDmlExec<C>
where
    C: ConnectionLike + Commands + Send + Sync + 'static,
{
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "RedisDmlExec(op={:?})", self.op),
            _ => write!(f, "RedisDmlExec"),
        }
    }
}

impl<C> ExecutionPlan for RedisDmlExec<C>
where
    C: ConnectionLike + Commands + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        "RedisDmlExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let mut conn = self.conn.try_write().map_err(|e| {
            DataFusionError::Execution(format!(
                "failed to acquire write lock of redis connection: {}",
                e
            ))
        })?;

        let affected = match &self.op {
            RedisDmlOp::Delete(keys) => {
                if keys.is_empty() {
                    0u64
                } else {
                    let _: () = conn.del(keys).map_err(|e| {
                        DataFusionError::Execution(format!("Redis DEL error: {}", e))
                    })?;
                    keys.len() as u64
                }
            }
            RedisDmlOp::Update(keys, fields) => {
                let field_refs: Vec<(&str, &str)> = fields
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                for key in keys {
                    let _: () = conn.hset_multiple(key, &field_refs).map_err(|e| {
                        DataFusionError::Execution(format!("Redis HSET error: {}", e))
                    })?;
                }
                keys.len() as u64
            }
        };

        let batch = create_count_batch(affected)?;
        Ok(Box::pin(MemoryStream::try_new(
            vec![batch],
            self.schema.clone(),
            None,
        )?))
    }
}

/// Create a single-row RecordBatch with `{ count: u64 }`.
fn create_count_batch(count: u64) -> datafusion::common::Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]));
    let array: UInt64Array = vec![count].into();
    RecordBatch::try_new(schema, vec![Arc::new(array)]).map_err(DataFusionError::from)
}

// ─── Scan execution plan ────────────────────────────────────────────────────

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
    key_column: Option<String>,
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
        let pattern = RedisRelation::table_data_key_pattern(&self.key_space, &self.table_name);

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
        for key in &keys {
            // Optionally apply a limit to stop early
            if let Some(max) = self.limit {
                if count >= max {
                    break;
                }
            }

            // Fetch the hash fields for this key
            let redis_map: HashMap<String, String> = conn_write
                .hgetall(key)
                .map_err(|e| DataFusionError::Execution(format!("Redis HGETALL error: {}", e)))?;
            // Extract the key column value from the Redis key suffix (e.g. "mydb:products:PROD001" → "PROD001")
            let key_col_value = self
                .key_column
                .as_ref()
                .map(|_| RedisRelation::table_key(&pattern, key).to_string());
            // TODO: Add other RedisStorage support other than hash
            for (j, field) in self.projected_schema.fields().iter().enumerate() {
                let field_name = field.name();
                let cell_value = if let Some(val) = redis_map.get(field_name) {
                    val.to_string()
                } else if self.key_column.as_deref() == Some(field_name) {
                    // Field is the key column — value is extracted from the Redis key suffix
                    key_col_value.clone().unwrap_or_default()
                } else {
                    "".to_string()
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

        if arrays.is_empty() {
            // DataFusion pushes an empty projection for `count(*)`-style queries
            // where only the row count matters. `RecordBatch::try_new` rejects a
            // zero-column batch unless we supply the row count explicitly, so
            // pass `count` through so aggregates see the real input cardinality.
            let options = RecordBatchOptions::new().with_row_count(Some(count));
            RecordBatch::try_new_with_options(self.projected_schema.clone(), arrays, &options)
                .map_err(|e| {
                    DataFusionError::Execution(format!("Error building RecordBatch: {}", e))
                })
        } else {
            RecordBatch::try_new(self.projected_schema.clone(), arrays).map_err(|e| {
                DataFusionError::Execution(format!("Error building RecordBatch: {}", e))
            })
        }
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
            key_column: self.key_column.clone(),
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

// ─── DataSink for INSERT ────────────────────────────────────────────────────

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

        let prefix = RedisRelation::prefix(&self.key_space, &self.table_name);

        // If Overwrite or Replace, delete existing keys for this table first
        if self.insert_op == InsertOp::Overwrite || self.insert_op == InsertOp::Replace {
            let pattern = RedisRelation::table_data_key_pattern(&self.key_space, &self.table_name);
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

// ─── Utilities ──────────────────────────────────────────────────────────────

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

// ─── Registration ───────────────────────────────────────────────────────

/// Register a Redis hash table as a DataFusion table.
///
/// # Options
/// * `key_space` - Namespace prefix for Redis keys (required)
/// * `table` - Table name within the key space (required)
/// * `key_column` - Column to use as the Redis key suffix (optional)
pub fn register_redis_tables(
    session_ctx: &mut datafusion::prelude::SessionContext,
    name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
) -> Result<()> {
    tracing::info!(
        "Registering Redis table: {} with connection: {}",
        name,
        connection_string
    );

    let opts = options.ok_or_else(|| {
        anyhow::anyhow!(
            "Redis data source '{}' requires options (key_space, table)",
            name
        )
    })?;

    let key_space = opts.get("key_space").ok_or_else(|| {
        anyhow::anyhow!("Redis data source '{}' requires 'key_space' option", name)
    })?;

    let table = opts
        .get("table")
        .ok_or_else(|| anyhow::anyhow!("Redis data source '{}' requires 'table' option", name))?;

    let key_column = opts.get("key_column").cloned();
    let columns = opts.get("columns").map(|s| {
        s.split(',')
            .map(|c| c.trim().to_string())
            .filter(|c| !c.is_empty())
            .collect::<Vec<String>>()
    });

    let client = redis::Client::open(connection_string)
        .map_err(|e| anyhow::anyhow!("Failed to create Redis client for '{}': {}", name, e))?;

    let conn = client
        .get_connection()
        .map_err(|e| anyhow::anyhow!("Failed to connect to Redis for '{}': {}", name, e))?;

    let redis_table = RedisTable::new(
        conn,
        key_space.clone(),
        table.clone(),
        RedisStorage::Hash,
        key_column,
        columns,
    )?;

    session_ctx
        .register_table(name, Arc::new(redis_table))
        .map_err(|e| anyhow::anyhow!("Failed to register Redis table '{}': {}", name, e))?;

    tracing::info!("Successfully registered Redis table: {}", name);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use arrow::{
        array::{Float64Array, Int32Array, StringArray, UInt64Array},
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

    // ─── DELETE tests ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_delete_by_key_column() -> Result<()> {
        let keyspace = "itest";
        let table = "person";
        let prefix = format!("{keyspace}:{table}");

        // Mock: DEL itest:person:1 -> returns 1
        let del = MockCmd::new(
            redis::cmd("DEL").arg(format!("{prefix}:1")).clone(),
            Ok(Value::Int(1)),
        );
        let mock_conn = MockRedisConnection::new(vec![del]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let conn = Arc::new(RwLock::new(mock_conn));

        // Use the fast path: key_column = "id", filter = id = '1'
        let key_column = Some("id".to_string());
        let filters = vec![Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
            left: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                "id",
            ))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("1".to_string())),
                None,
            )),
        })];

        let keys = {
            let mut conn_guard = conn.try_write().unwrap();
            resolve_matching_keys(
                &mut *conn_guard,
                keyspace,
                table,
                &schema,
                &key_column,
                &filters,
            )?
        };

        assert_eq!(keys, vec![format!("{prefix}:1")]);

        let exec = RedisDmlExec::new(conn, RedisDmlOp::Delete(keys));
        let batch = exec.execute(0, Arc::new(TaskContext::default()))?;

        // Collect the stream
        use futures::TryStreamExt;
        let batches: Vec<RecordBatch> = batch.try_collect().await?;
        assert_eq!(batches.len(), 1);
        let count_arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(count_arr.value(0), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_all_rows() -> Result<()> {
        let keyspace = "itest";
        let table = "person";
        let prefix = format!("{keyspace}:{table}");

        // Mock: SCAN returns 2 keys, then DEL both
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
        let del = MockCmd::new(
            redis::cmd("DEL")
                .arg(format!("{prefix}:1"))
                .arg(format!("{prefix}:2"))
                .clone(),
            Ok(Value::Int(2)),
        );
        let mock_conn = MockRedisConnection::new(vec![scan, del]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let conn = Arc::new(RwLock::new(mock_conn));
        let key_column: Option<String> = None;
        let filters: Vec<Expr> = vec![];

        let keys = {
            let mut conn_guard = conn.try_write().unwrap();
            resolve_matching_keys(
                &mut *conn_guard,
                keyspace,
                table,
                &schema,
                &key_column,
                &filters,
            )?
        };

        assert_eq!(keys.len(), 2);

        let exec = RedisDmlExec::new(conn, RedisDmlOp::Delete(keys));
        let batch = exec.execute(0, Arc::new(TaskContext::default()))?;
        let batches: Vec<RecordBatch> = batch.try_collect().await?;
        let count_arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(count_arr.value(0), 2);

        Ok(())
    }

    // ─── UPDATE tests ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_update_by_key_column() -> Result<()> {
        let keyspace = "itest";
        let table = "person";
        let prefix = format!("{keyspace}:{table}");

        // Mock: HMSET itest:person:1 name "Updated"
        let hset = MockCmd::new(
            redis::cmd("HMSET")
                .arg(format!("{prefix}:1"))
                .arg("name")
                .arg("Updated")
                .clone(),
            Ok(Value::Okay),
        );
        let mock_conn = MockRedisConnection::new(vec![hset]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let conn = Arc::new(RwLock::new(mock_conn));
        let key_column = Some("id".to_string());

        // Filter: id = '1'
        let filters = vec![Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
            left: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                "id",
            ))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("1".to_string())),
                None,
            )),
        })];

        let keys = {
            let mut conn_guard = conn.try_write().unwrap();
            resolve_matching_keys(
                &mut *conn_guard,
                keyspace,
                table,
                &schema,
                &key_column,
                &filters,
            )?
        };
        assert_eq!(keys, vec![format!("{prefix}:1")]);

        // Assignment: name = 'Updated'
        let assignments = vec![(
            "name".to_string(),
            Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("Updated".to_string())),
                None,
            ),
        )];
        let fields = assignments_to_redis_fields(&assignments, &key_column)?;
        assert_eq!(fields, vec![("name".to_string(), "Updated".to_string())]);

        let exec = RedisDmlExec::new(conn, RedisDmlOp::Update(keys, fields));
        let batch = exec.execute(0, Arc::new(TaskContext::default()))?;
        let batches: Vec<RecordBatch> = batch.try_collect().await?;
        let count_arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(count_arr.value(0), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_update_key_column_rejected() -> Result<()> {
        let key_column = Some("id".to_string());
        let assignments = vec![(
            "id".to_string(),
            Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("new_id".to_string())),
                None,
            ),
        )];

        let result = assignments_to_redis_fields(&assignments, &key_column);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Cannot update key column"));

        Ok(())
    }

    // ─── Integration test helpers ────────────────────────────────────────

    /// Register a Redis table from the CI docker service.
    fn register_ci_table(ctx: &mut SessionContext, table: &str) {
        register_ci_table_with_options(ctx, table, None);
    }

    /// Register a Redis table from the CI docker service with optional extra options.
    fn register_ci_table_with_options(
        ctx: &mut SessionContext,
        table: &str,
        extra_options: Option<&HashMap<String, String>>,
    ) {
        let mut options = HashMap::new();
        options.insert("key_space".to_string(), "mydb".to_string());
        options.insert("table".to_string(), table.to_string());
        options.insert("key_column".to_string(), "product_id".to_string());
        if let Some(extra) = extra_options {
            options.extend(extra.clone());
        }
        register_redis_tables(ctx, table, "redis://127.0.0.1:6379", Some(&options))
            .unwrap_or_else(|e| panic!("register {} failed: {}", table, e));
    }

    /// Remove all CI Redis keys for a registered table so tests can start from an empty table.
    fn clear_ci_table(table: &str) {
        let client = redis::Client::open("redis://127.0.0.1:6379").expect("create redis client");
        let mut conn = client.get_connection().expect("connect to redis");
        let pattern = format!("mydb:{table}:*");
        let keys: Vec<String> = conn.keys(&pattern).expect("scan redis keys");
        if !keys.is_empty() {
            let _: usize = conn.del(keys).expect("cleanup redis keys");
        }
    }

    async fn ci_query_all(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        let df = ctx.sql(sql).await.expect("parse sql");
        df.collect().await.expect("collect results")
    }

    fn ci_total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    // ─── Scan tests (integration) ───────────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_scan_all_rows_live() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "products");

        let batches = ci_query_all(
            &ctx,
            "SELECT product_id, name, category, price, in_stock FROM products ORDER BY product_id",
        )
        .await;
        assert!(ci_total_rows(&batches) >= 5);
    }

    #[tokio::test]
    #[ignore]
    async fn test_scan_with_projection_live() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "products");

        let batches = ci_query_all(&ctx, "SELECT name FROM products ORDER BY product_id").await;
        assert!(ci_total_rows(&batches) >= 5);
        assert_eq!(batches[0].num_columns(), 1);
    }

    #[tokio::test]
    #[ignore]
    async fn test_scan_with_filter_live() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "products");

        let batches = ci_query_all(
            &ctx,
            "SELECT product_id, name FROM products WHERE product_id = 'PROD001'",
        )
        .await;
        assert_eq!(ci_total_rows(&batches), 1);

        let names = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Laptop");
    }

    #[tokio::test]
    #[ignore]
    async fn test_scan_with_limit_live() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "products");

        let batches = ci_query_all(&ctx, "SELECT product_id FROM products LIMIT 2").await;
        assert_eq!(ci_total_rows(&batches), 2);
    }

    // ─── Insert test (integration) ──────────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_insert_into_live() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "products");

        ctx.sql(
            "INSERT INTO products (product_id, name, category, price, in_stock)
             VALUES ('PROD_INS_TEST', 'TestInsert', 'TestCat', '49.99', 'true')",
        )
        .await
        .expect("parse insert")
        .collect()
        .await
        .expect("execute insert");

        let batches = ci_query_all(
            &ctx,
            "SELECT product_id, name FROM products WHERE product_id = 'PROD_INS_TEST'",
        )
        .await;
        assert_eq!(ci_total_rows(&batches), 1);
    }

    // ─── Delete tests (integration) ─────────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_delete_with_filter_live() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "products");

        // Insert a row to delete
        ctx.sql(
            "INSERT INTO products (product_id, name, category, price, in_stock)
             VALUES ('PROD_DEL_TEST', 'DeleteMe', 'Test', '1.0', 'true')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

        let before = ci_query_all(
            &ctx,
            "SELECT product_id FROM products WHERE product_id = 'PROD_DEL_TEST'",
        )
        .await;
        assert_eq!(ci_total_rows(&before), 1);

        ctx.sql("DELETE FROM products WHERE product_id = 'PROD_DEL_TEST'")
            .await
            .expect("parse delete")
            .collect()
            .await
            .expect("execute delete");

        let after = ci_query_all(
            &ctx,
            "SELECT product_id FROM products WHERE product_id = 'PROD_DEL_TEST'",
        )
        .await;
        assert_eq!(ci_total_rows(&after), 0);
    }

    #[tokio::test]
    #[ignore]
    async fn test_delete_no_matching_rows_live() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "products");

        let before = ci_query_all(
            &ctx,
            "SELECT product_id FROM products WHERE product_id = 'PROD001'",
        )
        .await;
        assert_eq!(ci_total_rows(&before), 1);

        ctx.sql("DELETE FROM products WHERE product_id = 'NONEXISTENT'")
            .await
            .expect("parse delete")
            .collect()
            .await
            .expect("execute delete");

        let after = ci_query_all(
            &ctx,
            "SELECT product_id FROM products WHERE product_id = 'PROD001'",
        )
        .await;
        assert_eq!(ci_total_rows(&after), 1);
    }

    // ─── Update tests (integration) ─────────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_update_single_column_live() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "products");

        ctx.sql("UPDATE products SET price = '899.99' WHERE product_id = 'PROD001'")
            .await
            .expect("parse update")
            .collect()
            .await
            .expect("execute update");

        let batches = ci_query_all(
            &ctx,
            "SELECT price FROM products WHERE product_id = 'PROD001'",
        )
        .await;
        assert_eq!(ci_total_rows(&batches), 1);

        let prices = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(prices.value(0), "899.99");
    }

    #[tokio::test]
    #[ignore]
    async fn test_update_no_matching_rows_live() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "products");

        let before = ci_query_all(
            &ctx,
            "SELECT name FROM products WHERE product_id = 'PROD001'",
        )
        .await;
        assert_eq!(ci_total_rows(&before), 1);
        let before_name = before[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string();

        ctx.sql("UPDATE products SET price = '0.0' WHERE product_id = 'NONEXISTENT'")
            .await
            .expect("parse update")
            .collect()
            .await
            .expect("execute update");

        let after = ci_query_all(
            &ctx,
            "SELECT name FROM products WHERE product_id = 'PROD001'",
        )
        .await;
        assert_eq!(ci_total_rows(&after), 1);
        let after_name = after[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string();
        assert_eq!(before_name, after_name);
    }

    // ─── Combined DML test (integration) ────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_insert_update_delete_round_trip_live() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "products");

        // 1. Insert
        ctx.sql(
            "INSERT INTO products (product_id, name, category, price, in_stock)
             VALUES ('PROD_RT_TEST', 'RoundTrip', 'Test', '10.0', 'true')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        let after_insert = ci_query_all(
            &ctx,
            "SELECT product_id FROM products WHERE product_id = 'PROD_RT_TEST'",
        )
        .await;
        assert_eq!(ci_total_rows(&after_insert), 1);

        // 2. Update
        ctx.sql("UPDATE products SET name = 'RoundTripUpdated', price = '20.0' WHERE product_id = 'PROD_RT_TEST'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batches = ci_query_all(
            &ctx,
            "SELECT name, price FROM products WHERE product_id = 'PROD_RT_TEST'",
        )
        .await;
        assert_eq!(ci_total_rows(&batches), 1);
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "RoundTripUpdated");

        // 3. Delete
        ctx.sql("DELETE FROM products WHERE product_id = 'PROD_RT_TEST'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let after_delete = ci_query_all(
            &ctx,
            "SELECT product_id FROM products WHERE product_id = 'PROD_RT_TEST'",
        )
        .await;
        assert_eq!(ci_total_rows(&after_delete), 0);
    }

    // ─── Non-key column filter test (integration) ───────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_filter_by_category_live() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "products");

        let batches = ci_query_all(
            &ctx,
            "SELECT product_id, name FROM products WHERE category = 'Electronics' ORDER BY product_id",
        )
        .await;
        // PROD001..PROD004 are Electronics
        assert_eq!(ci_total_rows(&batches), 4);
    }

    #[tokio::test]
    #[ignore]
    async fn test_filter_by_in_stock_live() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "products");

        let batches = ci_query_all(
            &ctx,
            "SELECT product_id FROM products WHERE in_stock = 'false'",
        )
        .await;
        // Only PROD003 (Monitor) is out of stock
        assert_eq!(ci_total_rows(&batches), 1);
    }

    // ─── Aggregation test (integration) ─────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_aggregation_query_live() {
        let mut ctx = SessionContext::new();
        register_ci_table(&mut ctx, "products");

        let batches = ci_query_all(
            &ctx,
            "SELECT category, COUNT(*) as cnt
             FROM products
             GROUP BY category
             ORDER BY category",
        )
        .await;
        assert!(ci_total_rows(&batches) >= 2); // at least Electronics and Furniture
    }

    /// Regression test for #97 (Redis half): projection pushdown emits
    /// `Some([])` for `count(*)`, and `fetch_partition` previously built a
    /// zero-column batch via `RecordBatch::try_new`, which Arrow rejects with
    /// "must either specify a row count or at least one column". Uses a
    /// dedicated table so the bare `count(*)` has a known value regardless of
    /// what other parallel tests do to `products`.
    #[tokio::test]
    #[ignore]
    async fn test_count_star_pushdown_live() {
        let table_name = "count_star_scratch_live";
        clear_ci_table(table_name);

        let mut ctx = SessionContext::new();
        let mut extra_options = HashMap::new();
        extra_options.insert("columns".to_string(), "product_id,name,price".to_string());
        register_ci_table_with_options(&mut ctx, table_name, Some(&extra_options));

        ctx.sql(&format!(
            "INSERT INTO {table_name} (product_id, name, price)
             VALUES ('A', 'a', '1.0'), ('B', 'b', '2.0'), ('C', 'c', '3.0')"
        ))
        .await
        .expect("parse insert")
        .collect()
        .await
        .expect("execute insert");

        let batches = ci_query_all(&ctx, &format!("SELECT count(*) FROM {table_name}")).await;
        assert_eq!(ci_total_rows(&batches), 1);
        let counts = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(counts.value(0), 3);

        clear_ci_table(table_name);
    }

    #[tokio::test]
    #[ignore]
    async fn test_empty_table_declared_schema_live() {
        let table_name = "products_schema_live";
        clear_ci_table(table_name);

        let mut ctx = SessionContext::new();
        let mut extra_options = HashMap::new();
        extra_options.insert(
            "columns".to_string(),
            "product_id,name,category,price,in_stock".to_string(),
        );
        register_ci_table_with_options(&mut ctx, table_name, Some(&extra_options));

        let catalog = ctx.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();
        let table = schema.table(table_name).await.unwrap().unwrap();
        let table_schema = table.schema();
        let field_names: Vec<&str> = table_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            field_names,
            vec!["product_id", "name", "category", "price", "in_stock"]
        );

        ctx.sql(
            "INSERT INTO products_schema_live (product_id, name, category, price, in_stock)
             VALUES ('PROD_SCHEMA', 'SchemaProduct', 'TestCat', '12.34', 'true')",
        )
        .await
        .expect("parse insert")
        .collect()
        .await
        .expect("execute insert");

        let batches = ci_query_all(
            &ctx,
            "SELECT product_id, name, price
             FROM products_schema_live
             WHERE product_id = 'PROD_SCHEMA'",
        )
        .await;
        assert_eq!(ci_total_rows(&batches), 1);

        let product_ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let names = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(product_ids.value(0), "PROD_SCHEMA");
        assert_eq!(names.value(0), "SchemaProduct");

        ctx.sql("DELETE FROM products_schema_live WHERE product_id = 'PROD_SCHEMA'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        clear_ci_table(table_name);
    }
}
