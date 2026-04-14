//! Custom execution plan for Lance KNN search
//!
//! This execution plan directly calls Lance's Scanner::nearest() API
//! for efficient approximate nearest neighbor search.

use anyhow::Result;
use arrow::array::{ArrayRef, Float32Array, RecordBatch};
use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
    execution_plan::{Boundedness, EmissionType},
};
use futures::{StreamExt, stream};
use lance::dataset::Dataset;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

use crate::sources::providers::knn_utils::extract_query_vector;

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
    /// Full schema (all dataset fields excluding vector column + _distance)
    full_schema: SchemaRef,
    /// Output schema (projected subset, or full if no projection)
    schema: SchemaRef,
    /// Optional column projection indices (into full_schema)
    projection: Option<Vec<usize>>,
    /// Optional filter predicate applied before KNN search
    filter: Option<String>,
    /// Optional row limit applied after KNN + filter (from SQL LIMIT clause)
    scan_limit: Option<usize>,
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
        let mut fields: Vec<Field> = lance_schema
            .fields
            .iter()
            .filter(|f| f.name.as_str() != vector_column)
            .map(|f| f.into())
            .collect();
        fields.push(Field::new(
            "_distance",
            arrow::datatypes::DataType::Float32,
            true,
        ));
        let schema: SchemaRef = Arc::new(Schema::new(fields));

        // Create plan properties
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
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
            full_schema: schema.clone(),
            schema,
            projection: None,
            filter: None,
            scan_limit: None,
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
        let mut fields: Vec<Field> = lance_schema
            .fields
            .iter()
            .filter(|f| f.name.as_str() != vector_column)
            .map(|f| f.into())
            .collect();
        fields.push(Field::new(
            "_distance",
            arrow::datatypes::DataType::Float32,
            true,
        ));
        let schema: SchemaRef = Arc::new(Schema::new(fields));

        // Create plan properties
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
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
            full_schema: schema.clone(),
            schema,
            projection: None,
            filter: None,
            scan_limit: None,
            plan_properties,
        })
    }

    /// Apply a column projection (subset of columns to return)
    pub fn with_projection(mut self, projection: Vec<usize>) -> DFResult<Self> {
        let projected_fields: Vec<Field> = projection
            .iter()
            .map(|&idx| self.full_schema.field(idx).clone())
            .collect();
        let projected_schema: SchemaRef = Arc::new(Schema::new(projected_fields));
        self.schema = projected_schema.clone();
        self.projection = Some(projection);
        self.plan_properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(self)
    }

    /// Add a filter predicate (e.g., "category = 'electronics'")
    pub fn with_filter(mut self, filter: String) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Set a row limit applied after KNN + filter (from SQL LIMIT clause)
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.scan_limit = Some(limit);
        self
    }

    /// Execute Lance KNN search
    async fn execute_knn(&self, context: Arc<TaskContext>) -> Result<RecordBatch> {
        // Resolve query vector: literal or extracted from child plan.
        let query_vector: ArrayRef = if let Some(ref vector) = self.query_vector {
            vector.clone()
        } else if let Some(ref plan) = self.query_vector_plan {
            match extract_query_vector(plan.clone(), context.clone())
                .await
                .map_err(|e| anyhow::anyhow!("{}", e))?
            {
                Some(vec) => Arc::new(Float32Array::from(vec)) as ArrayRef,
                None => {
                    tracing::debug!("lance_knn: subquery returned no rows, returning empty result");
                    return Ok(RecordBatch::new_empty(self.schema.clone()));
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
        let mut batch = if batches.is_empty() {
            // Return empty batch with schema
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        } else if batches.len() == 1 {
            batches
                .into_iter()
                .next()
                .expect("len == 1 guarantees first element")
        } else {
            // Concatenate multiple batches
            let batch_schema = batches[0].schema();
            arrow::compute::concat_batches(&batch_schema, &batches)
                .map_err(|e| anyhow::anyhow!("Failed to concatenate batches: {}", e))?
        };

        // Apply scan limit if provided (from SQL LIMIT clause)
        if let Some(n) = self.scan_limit {
            if batch.num_rows() > n {
                batch = batch.slice(0, n);
            }
        }

        // Project and reorder columns to match the declared output schema.
        // Lance's scanner may return columns in a different order than expected,
        // and we may need only a subset when a projection is applied.
        let columns: Vec<_> = self
            .schema
            .fields()
            .iter()
            .map(|field| {
                batch
                    .column_by_name(field.name())
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Expected column '{}' not found in KNN result",
                            field.name()
                        )
                    })
                    .cloned()
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
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
            full_schema: self.full_schema.clone(),
            schema: self.schema.clone(),
            projection: self.projection.clone(),
            filter: self.filter.clone(),
            scan_limit: self.scan_limit.clone(),
            plan_properties: self.plan_properties.clone(),
        }
    }
}
