//! Shared utilities for KNN execution plans (Lance and pgvector).

/// Maximum number of nearest neighbours allowed in a single KNN query.
///
/// Enforced at planning time by both `pg_knn` and `lance_knn` table functions
/// to prevent runaway requests from fetching arbitrarily large result sets.
pub const MAX_KNN_K: usize = 500;

use arrow::array::{
    Array, BinaryArray, FixedSizeListArray, Float32Array, Float64Array, ListArray, StringArray,
};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use futures::StreamExt;
use std::sync::Arc;

/// Execute a child plan and extract the first row's first column as `Vec<f32>`.
///
/// Returns `None` if the plan produces no rows (e.g. a subquery filter matched nothing).
///
/// Supported column types:
/// - `FixedSizeList<Float32>` / `FixedSizeList<Float64>` — standard Arrow vector type
/// - `List<Float32>` / `List<Float64>` — variable-length list (e.g. from `candle()` UDF)
/// - `Float32Array` / `Float64Array` — bare float arrays
/// - `BinaryArray` — packed little-endian f32 bytes (sqlite-vec BLOB format)
/// - `StringArray` — pgvector text format (`[0.1,0.2,...]`), used when the vector column
///   is fetched via a `::text` cast to work around datafusion-table-providers' lack of
///   support for the Postgres `vector` OID
pub async fn extract_query_vector(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> DFResult<Option<Vec<f32>>> {
    let mut stream = execute_stream(plan, context)?;

    let batch = match stream.next().await {
        None => return Ok(None),
        Some(res) => {
            res.map_err(|e| DataFusionError::Execution(format!("knn subquery error: {}", e)))?
        }
    };
    if batch.num_rows() == 0 {
        return Ok(None);
    }

    let col = batch.column(0);

    if let Some(list) = col.as_any().downcast_ref::<FixedSizeListArray>() {
        let values = list.value(0);
        if let Some(arr) = values.as_any().downcast_ref::<Float32Array>() {
            return Ok(Some(arr.values().to_vec()));
        }
        if let Some(arr) = values.as_any().downcast_ref::<Float64Array>() {
            return Ok(Some(arr.values().iter().map(|&v| v as f32).collect()));
        }
    }
    // List<Float32/Float64> — variable-length list (e.g. from candle() UDF output).
    if let Some(list) = col.as_any().downcast_ref::<ListArray>() {
        let values = list.value(0);
        if let Some(arr) = values.as_any().downcast_ref::<Float32Array>() {
            return Ok(Some(arr.values().to_vec()));
        }
        if let Some(arr) = values.as_any().downcast_ref::<Float64Array>() {
            return Ok(Some(arr.values().iter().map(|&v| v as f32).collect()));
        }
    }
    if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
        return Ok(Some(arr.values().to_vec()));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
        return Ok(Some(arr.values().iter().map(|&v| v as f32).collect()));
    }
    // sqlite-vec BLOB: packed little-endian f32 bytes.
    if let Some(arr) = col.as_any().downcast_ref::<BinaryArray>() {
        if arr.is_null(0) {
            return Ok(None);
        }
        let bytes = arr.value(0);
        return parse_f32_blob(bytes).map(Some).map_err(|e| {
            DataFusionError::Execution(format!("knn: binary vector parse error: {e}"))
        });
    }
    // pgvector special case: the column was fetched as `embedding::text` to bypass
    // datafusion-table-providers' inability to decode the `vector` Postgres type.
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        if arr.is_null(0) {
            return Ok(None);
        }
        return parse_pgvector_text(arr.value(0))
            .map(Some)
            .map_err(|e| DataFusionError::Execution(format!("knn: pgvector parse error: {e}")));
    }

    Err(DataFusionError::Execution(
        "knn: subquery first column must be a vector \
         (FixedSizeList<Float32>, List<Float32>, Float32Array, Float64Array, \
         Binary (packed f32), or pgvector text String)"
            .to_string(),
    ))
}

/// Parse a pgvector text literal (`[0.1, 0.2, 0.3]`) into `Vec<f32>`.
pub fn parse_pgvector_text(s: &str) -> Result<Vec<f32>, String> {
    let inner = s
        .trim()
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .ok_or_else(|| format!("expected pgvector text like '[x,y,...]', got: {s}"))?;
    inner
        .split(',')
        .map(|v| {
            v.trim()
                .parse::<f32>()
                .map_err(|e| format!("failed to parse vector element '{v}': {e}"))
        })
        .collect()
}

/// Parse a packed little-endian f32 BLOB (sqlite-vec format) into `Vec<f32>`.
pub fn parse_f32_blob(bytes: &[u8]) -> Result<Vec<f32>, String> {
    if bytes.len() % 4 != 0 {
        return Err(format!(
            "expected packed f32 BLOB with length divisible by 4, got {} bytes",
            bytes.len()
        ));
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect())
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, FixedSizeListArray, Float32Array, Float64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream,
    };
    use futures::stream;
    use std::any::Any;
    use std::fmt;

    fn task_ctx() -> Arc<TaskContext> {
        Arc::new(TaskContext::default())
    }

    /// Minimal `ExecutionPlan` that serves a single pre-built `RecordBatch`.
    struct BatchPlan {
        batch: RecordBatch,
        schema: SchemaRef,
        props: PlanProperties,
    }

    impl BatchPlan {
        fn new(batch: RecordBatch) -> Self {
            let schema = batch.schema();
            let props = PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            );
            Self {
                batch,
                schema,
                props,
            }
        }
    }

    impl fmt::Debug for BatchPlan {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "BatchPlan")
        }
    }

    impl DisplayAs for BatchPlan {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "BatchPlan")
        }
    }

    impl ExecutionPlan for BatchPlan {
        fn name(&self) -> &str {
            "BatchPlan"
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
        fn properties(&self) -> &PlanProperties {
            &self.props
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(
            &self,
            _: usize,
            _: Arc<TaskContext>,
        ) -> datafusion::error::Result<SendableRecordBatchStream> {
            let schema = self.schema.clone();
            let batch = self.batch.clone();
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                stream::once(async move { Ok(batch) }),
            )))
        }
    }

    fn plan_from_batch(batch: RecordBatch) -> Arc<dyn ExecutionPlan> {
        Arc::new(BatchPlan::new(batch))
    }

    // ── parse_pgvector_text ───────────────────────────────────────────────

    #[test]
    fn test_parse_basic() {
        let v = parse_pgvector_text("[0.1,0.2,0.3]").unwrap();
        assert_eq!(v.len(), 3);
        assert!((v[0] - 0.1).abs() < 1e-6);
        assert!((v[1] - 0.2).abs() < 1e-6);
        assert!((v[2] - 0.3).abs() < 1e-6);
    }

    #[test]
    fn test_parse_with_spaces() {
        let v = parse_pgvector_text("[ 0.1, 0.2, 0.3 ]").unwrap();
        assert_eq!(v.len(), 3);
        assert!((v[0] - 0.1).abs() < 1e-6);
    }

    #[test]
    fn test_parse_single_element() {
        assert_eq!(parse_pgvector_text("[1.5]").unwrap(), vec![1.5f32]);
    }

    #[test]
    fn test_parse_missing_brackets_errors() {
        assert!(parse_pgvector_text("0.1,0.2,0.3").is_err());
    }

    #[test]
    fn test_parse_invalid_element_errors() {
        assert!(parse_pgvector_text("[0.1,not_a_float,0.3]").is_err());
    }

    // ── extract_query_vector ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_extract_float32_array() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float32Array::from(vec![0.1f32, 0.2, 0.3]))],
        )
        .unwrap();
        let v = extract_query_vector(plan_from_batch(batch), task_ctx())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(v.len(), 3);
        assert!((v[0] - 0.1).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_extract_float64_array_casts_to_f32() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float64Array::from(vec![0.4f64, 0.5]))],
        )
        .unwrap();
        let v = extract_query_vector(plan_from_batch(batch), task_ctx())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(v.len(), 2);
        assert!((v[0] - 0.4f32).abs() < 1e-5);
        assert!((v[1] - 0.5f32).abs() < 1e-5);
    }

    #[tokio::test]
    async fn test_extract_fixed_size_list_float32() {
        let item_field = Arc::new(Field::new("item", DataType::Float32, false));
        let values = Arc::new(Float32Array::from(vec![0.1f32, 0.2, 0.3]));
        let list = FixedSizeListArray::try_new(item_field.clone(), 3, values, None).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::FixedSizeList(item_field, 3),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(list)]).unwrap();
        let v = extract_query_vector(plan_from_batch(batch), task_ctx())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(v, vec![0.1f32, 0.2, 0.3]);
    }

    #[tokio::test]
    async fn test_extract_fixed_size_list_float64_casts_to_f32() {
        let item_field = Arc::new(Field::new("item", DataType::Float64, false));
        let values = Arc::new(Float64Array::from(vec![1.0f64, 2.0]));
        let list = FixedSizeListArray::try_new(item_field.clone(), 2, values, None).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::FixedSizeList(item_field, 2),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(list)]).unwrap();
        let v = extract_query_vector(plan_from_batch(batch), task_ctx())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(v, vec![1.0f32, 2.0f32]);
    }

    #[tokio::test]
    async fn test_extract_string_pgvector_format() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["[0.1,0.2,0.3]"]))],
        )
        .unwrap();
        let v = extract_query_vector(plan_from_batch(batch), task_ctx())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(v.len(), 3);
        assert!((v[0] - 0.1).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_extract_string_null_returns_none() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![None::<&str>]))],
        )
        .unwrap();
        let result = extract_query_vector(plan_from_batch(batch), task_ctx())
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_extract_empty_batch_returns_none() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float32, false)]));
        let result =
            extract_query_vector(plan_from_batch(RecordBatch::new_empty(schema)), task_ctx())
                .await
                .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_extract_no_rows_returns_none() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float32, false)]));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let result = extract_query_vector(plan, task_ctx()).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_extract_unsupported_column_type_returns_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Boolean, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(BooleanArray::from(vec![true, false]))],
        )
        .unwrap();
        let result = extract_query_vector(plan_from_batch(batch), task_ctx()).await;
        assert!(result.is_err());
    }

    // ── parse_f32_blob ──────────────────────────────────────────────────

    #[test]
    fn test_parse_f32_blob_basic() {
        // Pack [1.0, 2.0, 3.0] as little-endian f32 bytes
        let blob: Vec<u8> = [1.0f32, 2.0, 3.0]
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        let v = parse_f32_blob(&blob).unwrap();
        assert_eq!(v, vec![1.0f32, 2.0, 3.0]);
    }

    #[test]
    fn test_parse_f32_blob_empty() {
        let v = parse_f32_blob(&[]).unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn test_parse_f32_blob_bad_length() {
        assert!(parse_f32_blob(&[0, 1, 2]).is_err()); // 3 bytes, not divisible by 4
    }

    // ── extract_query_vector: List<Float32> ─────────────────────────────

    #[tokio::test]
    async fn test_extract_list_float32() {
        use arrow::array::{ArrayRef, Float32Array, ListArray};
        use arrow::buffer::OffsetBuffer;

        let values = Arc::new(Float32Array::from(vec![0.1f32, 0.2, 0.3]));
        let item_field = Arc::new(Field::new("item", DataType::Float32, true));
        let list = ListArray::new(
            item_field.clone(),
            OffsetBuffer::from_lengths([3]),
            values as ArrayRef,
            None,
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::List(item_field),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(list)]).unwrap();
        let v = extract_query_vector(plan_from_batch(batch), task_ctx())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(v.len(), 3);
        assert!((v[0] - 0.1).abs() < 1e-6);
        assert!((v[1] - 0.2).abs() < 1e-6);
        assert!((v[2] - 0.3).abs() < 1e-6);
    }

    // ── extract_query_vector: BinaryArray (packed f32 BLOB) ─────────────

    #[tokio::test]
    async fn test_extract_binary_packed_f32() {
        use arrow::array::BinaryArray;

        let blob: Vec<u8> = [0.5f32, 1.5, 2.5]
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Binary, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(BinaryArray::from_vec(vec![&blob]))])
                .unwrap();
        let v = extract_query_vector(plan_from_batch(batch), task_ctx())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(v, vec![0.5f32, 1.5, 2.5]);
    }

    #[tokio::test]
    async fn test_extract_binary_null_returns_none() {
        use arrow::array::BinaryArray;

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Binary, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(BinaryArray::from_opt_vec(vec![None]))],
        )
        .unwrap();
        let result = extract_query_vector(plan_from_batch(batch), task_ctx())
            .await
            .unwrap();
        assert!(result.is_none());
    }
}
