//! Custom execution plan for Lance KNN search
//!
//! This execution plan directly calls Lance's Scanner::nearest() API
//! for efficient approximate nearest neighbor search.

use anyhow::Result;
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics, execute_stream,
    execution_plan::{Boundedness, EmissionType},
};
use futures::{StreamExt, stream};
use lance::dataset::Dataset;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// Physical execution plan for Lance KNN search
///
/// This plan uses Lance's Scanner::nearest() API to efficiently find
/// the k-nearest neighbors to a query vector.
///
/// Note: Distance metric is determined by the index created on the Lance table,
/// not specified at query time.
#[derive(Debug)]
pub struct LanceKnnExec {
    /// Lance dataset to search
    dataset: Arc<Dataset>,
    /// Name of the vector column to search
    vector_column: String,
    /// Query vector to search for (as Arrow array)
    /// If None, will be extracted from query_vector_plan at execution time
    query_vector: Option<ArrayRef>,
    /// Optional child plan to extract query vector from (for CROSS JOIN cases)
    query_vector_plan: Option<Arc<dyn ExecutionPlan>>,
    /// Column name in query_vector_plan that contains the query vector
    query_vector_column: Option<String>,
    /// Number of nearest neighbors to return (k)
    k: usize,
    /// Schema of the output
    schema: SchemaRef,
    /// Optional filter predicate applied before KNN search
    filter: Option<String>,
    /// Plan properties
    plan_properties: PlanProperties,
}

// Note: DistanceMetric enum removed - distance metric is embedded in the Lance index
// and determined when the index is created, not at query time.

impl LanceKnnExec {
    /// Create a new LanceKnnExec with a pre-computed query vector
    pub fn try_new(
        dataset: Arc<Dataset>,
        vector_column: String,
        query_vector: ArrayRef,
        k: usize,
    ) -> DFResult<Self> {
        // Build schema: dataset fields (excluding vector column) + _distance
        // This matches the actual output from Lance's nearest() API
        let lance_schema = dataset.schema();
        let mut fields: Vec<arrow::datatypes::Field> = lance_schema
            .fields
            .iter()
            .filter(|f| f.name.as_str() != vector_column)
            .map(|f| f.into())
            .collect();
        fields.push(arrow::datatypes::Field::new(
            "_distance",
            arrow::datatypes::DataType::Float32,
            true,
        ));
        let schema: SchemaRef = Arc::new(arrow::datatypes::Schema::new(fields));

        // Create plan properties
        let plan_properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Self {
            dataset,
            vector_column,
            query_vector: Some(query_vector),
            query_vector_plan: None,
            query_vector_column: None,
            k,
            schema,
            filter: None,
            plan_properties,
        })
    }

    /// Create a new LanceKnnExec with deferred query vector extraction
    /// The query vector will be extracted from the child plan at execution time
    pub fn try_new_with_deferred_extraction(
        dataset: Arc<Dataset>,
        vector_column: String,
        query_vector_plan: Arc<dyn ExecutionPlan>,
        query_vector_column: String,
        k: usize,
    ) -> DFResult<Self> {
        // Build schema: dataset fields (excluding vector column) + _distance
        // This matches the actual output from Lance's nearest() API
        let lance_schema = dataset.schema();
        let mut fields: Vec<arrow::datatypes::Field> = lance_schema
            .fields
            .iter()
            .filter(|f| f.name.as_str() != vector_column)
            .map(|f| f.into())
            .collect();
        fields.push(arrow::datatypes::Field::new(
            "_distance",
            arrow::datatypes::DataType::Float32,
            true,
        ));
        let schema: SchemaRef = Arc::new(arrow::datatypes::Schema::new(fields));

        // Create plan properties
        let plan_properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Self {
            dataset,
            vector_column,
            query_vector: None,
            query_vector_plan: Some(query_vector_plan),
            query_vector_column: Some(query_vector_column),
            k,
            schema,
            filter: None,
            plan_properties,
        })
    }

    /// Add a filter predicate (e.g., "category = 'electronics'")
    pub fn with_filter(mut self, filter: String) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Extract query vector from child plan (for deferred extraction)
    async fn extract_query_vector_from_plan(
        plan: &Arc<dyn ExecutionPlan>,
        column_name: &str,
        context: Arc<TaskContext>,
    ) -> Result<ArrayRef> {
        use arrow::array::{FixedSizeListArray, Float32Array};
        use arrow::datatypes::DataType;
        use futures::StreamExt;

        // Some executors (like SqlExec, ProjectionExec with complex subqueries) may not expose
        // the column in their schema but will produce it when executed. We'll check the plan schema
        // first, but if not found, execute and check the result schema.
        let schema = plan.schema();
        let column_index_result = schema.index_of(column_name);
        let plan_name = plan.name();

        // Determine if we need to check result schema after execution
        let check_result_schema = column_index_result.is_err()
            && (plan_name == "SqlExec"
                || plan_name == "ProjectionExec"
                || plan_name == "CooperativeExec");

        // If column not found in plan schema, we'll execute and check result schema
        // (this handles SqlExec, ProjectionExec, and other executors that produce columns through execution)
        let (column_index, field) = if let Ok(idx) = column_index_result {
            (idx, schema.field(idx))
        } else if check_result_schema {
            // For these executors, we'll execute and check the result
            // Placeholder values - will be overwritten after execution
            let placeholder_field = schema
                .fields()
                .first()
                .ok_or_else(|| anyhow::anyhow!("Plan '{}' has no fields in schema", plan_name))?;
            (0, placeholder_field.as_ref())
        } else {
            return Err(anyhow::anyhow!(
                "Column '{}' not found in plan '{}' schema: {}",
                column_name,
                plan_name,
                column_index_result.unwrap_err()
            ));
        };

        // Check if the column is a FixedSizeList of Float32 (vector type)
        // (Will re-check after execution for plans that don't expose schema)
        let is_vector_type = matches!(field.data_type(), DataType::FixedSizeList(_, _));

        // For plans that need result schema check, we'll validate the type after execution
        if !check_result_schema && !is_vector_type {
            return Err(anyhow::anyhow!(
                "Column '{}' is not a FixedSizeList (vector type), got: {:?}",
                column_name,
                field.data_type()
            ));
        }

        // Execute the plan using execute_stream which properly handles multi-partition plans
        let stream = execute_stream(plan.clone(), context.clone())
            .map_err(|e| anyhow::anyhow!("Failed to execute query vector plan: {}", e))?;

        let batches: Vec<DFResult<RecordBatch>> = stream.collect().await;

        if batches.is_empty() {
            return Err(anyhow::anyhow!("Query vector plan returned no batches"));
        }

        // Get the first batch (CROSS JOIN typically returns one row)
        let batch = batches[0]
            .as_ref()
            .map_err(|e| anyhow::anyhow!("Failed to get batch: {}", e))?;

        if batch.num_rows() == 0 {
            // Return a specific error that can be handled by the caller
            // This indicates the subquery returned no rows (valid outcome, not an error)
            return Err(anyhow::anyhow!("EMPTY_QUERY_VECTOR_RESULT"));
        }

        // For plans that don't expose schema upfront, check the result schema instead
        let final_column_index = if check_result_schema {
            let result_schema = batch.schema();
            let result_idx = result_schema.index_of(column_name).map_err(|e| {
                anyhow::anyhow!(
                    "Column '{}' not found in '{}' result schema: {}. Valid fields: {:?}",
                    column_name,
                    plan_name,
                    e,
                    result_schema
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .collect::<Vec<_>>()
                )
            })?;
            result_idx
        } else {
            column_index
        };

        let column_array = batch.column(final_column_index);

        // Validate type from result (for plans without schema) or plan (for others)
        // Get the data type from the appropriate schema - clone it to avoid lifetime issues
        let field_data_type: DataType = if check_result_schema {
            batch.schema().field(final_column_index).data_type().clone()
        } else {
            field.data_type().clone()
        };

        let is_vector_type = matches!(&field_data_type, DataType::FixedSizeList(_, _));

        if !is_vector_type {
            return Err(anyhow::anyhow!(
                "Column '{}' is not a FixedSizeList (vector type), got: {:?}",
                column_name,
                field_data_type
            ));
        }

        // Extract the vector from FixedSizeListArray
        let list_array = column_array
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| {
                anyhow::anyhow!("Column '{}' is not a FixedSizeListArray", column_name)
            })?;

        use arrow::array::Array;
        if list_array.len() == 0 {
            return Err(anyhow::anyhow!("FixedSizeListArray is empty"));
        }

        // Get the first element (CROSS JOIN should have one row)
        let vector_array = list_array.value(0);

        // Verify it's Float32Array
        let float_array = vector_array
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| anyhow::anyhow!("Vector array is not Float32Array"))?;

        let vector = Arc::new(float_array.clone()) as ArrayRef;
        Ok(vector)
    }

    /// Execute Lance KNN search
    async fn execute_knn(&self, context: Arc<TaskContext>) -> Result<RecordBatch> {
        // Extract query vector if needed (deferred extraction from subquery)
        let query_vector = if let Some(ref vector) = self.query_vector {
            vector.clone()
        } else if let (Some(plan), Some(column_name)) =
            (&self.query_vector_plan, &self.query_vector_column)
        {
            // Extract query vector from child plan at execution time
            // IMPORTANT: This executes the plan fresh with the current TaskContext,
            // which ensures parameters are properly bound for this execution
            match Self::extract_query_vector_from_plan(plan, column_name, context.clone()).await {
                Ok(vector) => vector,
                Err(e) => {
                    // Check if this is an empty result error
                    if e.to_string() == "EMPTY_QUERY_VECTOR_RESULT" {
                        // Query vector subquery returned no rows - this is a valid outcome
                        // (e.g., WHERE user_id = $1 matched no rows)
                        // Return empty result set with correct schema
                        tracing::info!(
                            "Query vector subquery returned no rows - returning empty KNN result"
                        );
                        return Ok(RecordBatch::new_empty(self.schema.clone()));
                    }
                    // Other errors should propagate
                    return Err(e);
                }
            }
        } else {
            return Err(anyhow::anyhow!(
                "LanceKnnExec has neither query_vector nor query_vector_plan"
            ));
        };

        // Create scanner
        let mut scanner = self.dataset.scan();

        // Get column names to project (exclude vector column if not needed)
        // Lance will still use the vector column for nearest() search internally
        let lance_schema = self.dataset.schema();
        let mut project_columns: Vec<String> = Vec::new();

        for field in lance_schema.fields.iter() {
            let field_name = field.name.as_str();
            // Exclude the vector column from results (Lance uses it internally for search)
            if field_name != self.vector_column {
                project_columns.push(field_name.to_string());
            }
        }

        // Project to exclude vector column
        if !project_columns.is_empty() {
            scanner.project(&project_columns)?;
        }

        // Apply filter if present
        if let Some(ref filter_expr) = self.filter {
            scanner.filter(filter_expr)?;
        }

        // Configure KNN search
        // Note: Distance metric is determined by the index, not specified here
        // Lance will use the vector column internally even though we didn't project it
        scanner.nearest(&self.vector_column, query_vector.as_ref(), self.k)?;

        // Execute and collect results
        let mut stream = scanner.try_into_stream().await?;

        let mut batches: Vec<RecordBatch> = Vec::new();
        while let Some(batch_result) = stream.next().await {
            batches.push(batch_result?);
        }

        // Combine batches if multiple
        // Note: Lance's nearest() API adds a "_distance" column automatically
        // We return results as-is with Lance's schema (including _distance)
        //
        // TODO: Rename "_distance" to match SQL alias (e.g., "distance", "similarity", "vec_distance")
        // Currently users need to use `SELECT ... _distance AS desired_name` in outer query
        // to rename the column. Ideally, LanceKnnExec should detect the alias from SQL
        // and rename _distance automatically to avoid confusion.
        if batches.is_empty() {
            // Return empty batch with schema
            Ok(RecordBatch::new_empty(self.schema.clone()))
        } else if batches.len() == 1 {
            Ok(batches.into_iter().next().unwrap())
        } else {
            // Concatenate multiple batches
            let batch_schema = batches[0].schema();
            arrow::compute::concat_batches(&batch_schema, &batches)
                .map_err(|e| anyhow::anyhow!("Failed to concatenate batches: {}", e))
        }
    }
}

impl DisplayAs for LanceKnnExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref column) = self.query_vector_column {
            write!(
                f,
                "LanceKnnExec: column={}, k={}, query_vector_column={}",
                self.vector_column, self.k, column
            )
        } else {
            write!(
                f,
                "LanceKnnExec: column={}, k={}",
                self.vector_column, self.k
            )
        }
    }
}

impl ExecutionPlan for LanceKnnExec {
    fn name(&self) -> &str {
        "LanceKnnExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // Return query_vector_plan if present (for deferred extraction)
        if let Some(ref plan) = self.query_vector_plan {
            vec![plan]
        } else {
            vec![]
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Accept one child if we're using deferred extraction
        match (self.query_vector_plan.is_some(), children.len()) {
            (true, 1) => {
                // Update the query_vector_plan
                Ok(Arc::new(LanceKnnExec {
                    query_vector_plan: Some(children[0].clone()),
                    ..(*self).clone()
                }))
            }
            (false, 0) => Ok(self),
            _ => Err(DataFusionError::Internal(format!(
                "LanceKnnExec expected {} children, got {}",
                if self.query_vector_plan.is_some() {
                    1
                } else {
                    0
                },
                children.len()
            ))),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                "LanceKnnExec only supports single partition".to_string(),
            ));
        }

        let schema = self.schema.clone();
        let knn_exec = self.clone();

        // Create async stream that executes KNN search
        let stream = stream::once(async move {
            knn_exec
                .execute_knn(context)
                .await
                .map_err(|e| DataFusionError::Execution(e.to_string()))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn statistics(&self) -> DFResult<Statistics> {
        // Return statistics indicating we'll return k rows
        Ok(Statistics {
            num_rows: datafusion::common::stats::Precision::Exact(self.k),
            total_byte_size: datafusion::common::stats::Precision::Absent,
            column_statistics: vec![],
        })
    }
}

// Implement Clone for LanceKnnExec (needed for execute)
impl Clone for LanceKnnExec {
    fn clone(&self) -> Self {
        Self {
            dataset: self.dataset.clone(),
            vector_column: self.vector_column.clone(),
            query_vector: self.query_vector.clone(),
            query_vector_plan: self.query_vector_plan.clone(),
            query_vector_column: self.query_vector_column.clone(),
            k: self.k,
            schema: self.schema.clone(),
            filter: self.filter.clone(),
            plan_properties: self.plan_properties.clone(),
        }
    }
}
