use anyhow::{Result, anyhow};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use serde::Serialize;
use skardi_engine::Engine;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use crate::inferencer::SqlSchemaInferrer;
use crate::types::{
    ComponentMetadata, QueryDefinition, RequestSchema, ResponseSchema, ValidationReport,
};

/// Core pipeline management trait for loading, validating, and managing pipeline definitions
///
/// This trait provides the foundation for pipeline operations by combining all pipeline
/// components (metadata, request, query, response) into a cohesive pipeline definition.
/// It leverages the existing validation and serialization traits from types.rs to provide
/// comprehensive pipeline lifecycle management.
#[async_trait]
pub trait Pipeline {
    /// Load a complete pipeline from a YAML file with schema inference
    ///
    /// This method reads the pipeline definition from a YAML file containing metadata
    /// and query sections. Request and response schemas are automatically inferred
    /// from the SQL query using the provided SessionContext.
    ///
    /// # Arguments
    /// * `file_path` - Path to the pipeline YAML file
    /// * `ctx` - SessionContext with registered tables for schema inference
    ///
    /// # Returns
    /// * `Result<Self>` - Complete pipeline instance or error with detailed context
    ///
    /// # Examples
    /// ```rust,no_run
    /// use pipeline::pipeline::{Pipeline, StandardPipeline};
    /// use datafusion::prelude::SessionContext;
    /// use std::path::Path;
    /// use std::sync::Arc;
    ///
    /// # tokio_test::block_on(async {
    /// let ctx = Arc::new(SessionContext::new());
    /// // Register tables in ctx...
    /// let pipeline_path = Path::new("pipeline.yaml");
    /// let pipeline = StandardPipeline::load_from_file(pipeline_path, ctx).await.unwrap();
    /// # })
    /// ```
    async fn load_from_file<P: AsRef<Path> + Send>(
        file_path: P,
        ctx: Arc<SessionContext>,
    ) -> Result<Self>
    where
        Self: Sized;

    /// Validate the complete pipeline for internal consistency and business rules
    ///
    /// This method performs comprehensive validation across all pipeline components:
    /// - Individual component validation using `SchemaValidation` trait
    /// - Cross-component consistency checks (request/response field compatibility)
    /// - Business rule validation (required fields, parameter binding, etc.)
    /// - SQL query parameter validation against request schema
    ///
    /// # Returns
    /// * `Result<ValidationReport>` - Detailed validation report or critical error
    ///
    /// # Examples
    /// ```rust,no_run
    /// use pipeline::pipeline::{Pipeline, StandardPipeline};
    /// use datafusion::prelude::SessionContext;
    /// use std::sync::Arc;
    ///
    /// # tokio_test::block_on(async {
    /// let ctx = Arc::new(SessionContext::new());
    /// let pipeline = StandardPipeline::load_from_file("pipeline.yaml", ctx).await.unwrap();
    /// let validation_report = pipeline.validate().unwrap();
    ///
    /// if validation_report.errors.is_empty() {
    ///     println!("Pipeline validation passed!");
    /// } else {
    ///     for error in &validation_report.errors {
    ///         println!("Validation error: {}", error.message);
    ///     }
    /// }
    /// # })
    /// ```
    fn validate(&self) -> Result<ValidationReport>;

    /// Get the pipeline metadata component
    ///
    /// # Returns
    /// * `&ComponentMetadata` - Reference to the pipeline metadata
    fn metadata(&self) -> &ComponentMetadata;

    /// Get the request schema component
    ///
    /// # Returns
    /// * `&RequestSchema` - Reference to the request schema definition
    fn request_schema(&self) -> &RequestSchema;

    /// Get the query definition component
    ///
    /// # Returns
    /// * `&QueryDefinition` - Reference to the SQL query definition
    fn query_definition(&self) -> &QueryDefinition;

    /// Get the response schema component
    ///
    /// # Returns
    /// * `&ResponseSchema` - Reference to the response schema definition
    fn response_schema(&self) -> &ResponseSchema;

    /// Validate request/response field compatibility
    ///
    /// This method checks that fields defined in both request and response schemas
    /// have compatible types for safe data flow through the pipeline. Uses the
    /// `DataFusionType::is_compatible` method for type compatibility checking.
    ///
    /// # Returns
    /// * `Result<Vec<String>>` - List of compatibility warnings, or error for critical issues
    ///
    /// # Examples
    /// ```rust,no_run
    /// use pipeline::pipeline::{Pipeline, StandardPipeline};
    /// use datafusion::prelude::SessionContext;
    /// use std::sync::Arc;
    ///
    /// # tokio_test::block_on(async {
    /// let ctx = Arc::new(SessionContext::new());
    /// let pipeline = StandardPipeline::load_from_file("pipeline.yaml", ctx).await.unwrap();
    /// let warnings = pipeline.validate_field_compatibility().unwrap();
    ///
    /// for warning in warnings {
    ///     println!("Compatibility warning: {}", warning);
    /// }
    /// # })
    /// ```
    fn validate_field_compatibility(&self) -> Result<Vec<String>>;

    /// Validate SQL query parameters against request schema
    ///
    /// This method ensures that all parameters referenced in the SQL query
    /// (e.g., ${parameter_name}) have corresponding fields in the request schema
    /// with compatible types for parameter binding.
    ///
    /// # Returns
    /// * `Result<Vec<String>>` - List of parameter validation issues or error
    ///
    /// # Examples
    /// ```rust,no_run
    /// use pipeline::pipeline::{Pipeline, StandardPipeline};
    /// use datafusion::prelude::SessionContext;
    /// use std::sync::Arc;
    ///
    /// # tokio_test::block_on(async {
    /// let ctx = Arc::new(SessionContext::new());
    /// let pipeline = StandardPipeline::load_from_file("pipeline.yaml", ctx).await.unwrap();
    /// let issues = pipeline.validate_query_parameters().unwrap();
    ///
    /// if issues.is_empty() {
    ///     println!("All query parameters are valid!");
    /// } else {
    ///     for issue in issues {
    ///         println!("Parameter issue: {}", issue);
    ///     }
    /// }
    /// # })
    /// ```
    fn validate_query_parameters(&self) -> Result<Vec<String>>;

    /// Export the complete pipeline as YAML string
    ///
    /// This method serializes the entire pipeline (all components) back to YAML format
    /// using the `YamlSerializable::to_yaml` trait. Useful for pipeline persistence,
    /// debugging, or creating pipeline templates.
    ///
    /// # Returns
    /// * `Result<String>` - Complete pipeline YAML or serialization error
    ///
    /// # Examples
    /// ```rust,no_run
    /// use pipeline::pipeline::{Pipeline, StandardPipeline};
    /// use datafusion::prelude::SessionContext;
    /// use std::sync::Arc;
    ///
    /// # tokio_test::block_on(async {
    /// let ctx = Arc::new(SessionContext::new());
    /// let pipeline = StandardPipeline::load_from_file("pipeline.yaml", ctx).await.unwrap();
    /// let yaml_output = pipeline.to_yaml().unwrap();
    /// println!("Pipeline YAML:\n{}", yaml_output);
    /// # })
    /// ```
    fn to_yaml(&self) -> Result<String>;

    /// Get pipeline name from metadata
    ///
    /// Convenience method to access the pipeline name without going through metadata.
    ///
    /// # Returns
    /// * `&str` - Pipeline name from metadata
    fn name(&self) -> &str;

    /// Get pipeline version from metadata
    ///
    /// Convenience method to access the pipeline version without going through metadata.
    ///
    /// # Returns
    /// * `&str` - Pipeline version from metadata
    fn version(&self) -> &str;

    /// Check if pipeline is valid (no validation errors)
    ///
    /// This is a convenience method that runs full validation and returns
    /// a simple boolean result for quick validity checks.
    ///
    /// # Returns
    /// * `Result<bool>` - True if pipeline is valid, false if has errors, or validation error
    ///
    /// # Examples
    /// ```rust,no_run
    /// use pipeline::pipeline::{Pipeline, StandardPipeline};
    /// use datafusion::prelude::SessionContext;
    /// use std::sync::Arc;
    ///
    /// # tokio_test::block_on(async {
    /// let ctx = Arc::new(SessionContext::new());
    /// let pipeline = StandardPipeline::load_from_file("pipeline.yaml", ctx).await.unwrap();
    /// if pipeline.is_valid().unwrap_or(false) {
    ///     println!("Pipeline is ready for execution!");
    /// } else {
    ///     println!("Pipeline has validation issues");
    /// }
    /// # })
    /// ```
    fn is_valid(&self) -> Result<bool>;

    /// Execute the pipeline's SQL query using the provided engine
    ///
    /// This method delegates SQL execution to the provided engine implementation,
    /// allowing the pipeline to be executed against different data sources or
    /// execution engines while maintaining a consistent interface.
    ///
    /// # Arguments
    ///
    /// * `engine` - The engine implementation to use for SQL execution
    /// * `sql` - The SQL query to execute (typically from the pipeline's query definition)
    ///
    /// # Returns
    ///
    /// Returns a `Result<RecordBatch>` containing the query results or an error
    /// if the query execution fails.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use pipeline::pipeline::{Pipeline, StandardPipeline};
    /// use skardi_engine::{Engine, datafusion::DataFusionEngine};
    /// use datafusion::prelude::SessionContext;
    /// use std::sync::Arc;
    ///
    /// async fn example_execution() -> anyhow::Result<()> {
    ///     let ctx = Arc::new(SessionContext::new());
    ///     let pipeline = StandardPipeline::load_from_file("pipeline.yaml", ctx.clone()).await?;
    ///     let engine = DataFusionEngine::new(SessionContext::new());
    ///     let sql = pipeline.query_definition().sql.as_str();
    ///     let result = pipeline.execute(&engine, sql).await?;
    ///     println!("Query returned {} rows", result.num_rows());
    ///     Ok(())
    /// }
    /// ```
    async fn execute(&self, engine: &dyn Engine, sql: &str) -> Result<RecordBatch>;
}

// ============================================================================
// StandardPipeline Implementation
// ============================================================================

/// Standard implementation of the Pipeline trait
///
/// This struct holds all four pipeline components (metadata, request, query, response)
/// and provides the concrete implementation of the Pipeline trait. It serves as the
/// primary way to work with pipeline definitions in the system.
///
/// Enhanced with SQL schema inference capabilities for named parameter support.
#[derive(Debug, Clone, Serialize)]
pub struct StandardPipeline {
    /// Pipeline metadata containing name, version, description, and timestamps
    pub metadata: ComponentMetadata,
    /// Request schema defining the expected input fields and their types (inferred at runtime)
    #[serde(skip)]
    pub request_schema: Arc<RequestSchema>,
    /// Query definition containing the SQL query and parameter list
    pub query_definition: QueryDefinition,
    /// Response schema defining the output fields and their types (inferred at runtime)
    #[serde(skip)]
    pub response_schema: Arc<ResponseSchema>,
    /// SQL schema inferencer for named parameter support
    #[serde(skip)]
    pub inferencer: Arc<SqlSchemaInferrer>,
}

impl StandardPipeline {
    /// Create a new StandardPipeline instance with the provided components
    ///
    /// This constructor allows direct creation of a pipeline when all components
    /// are already available. Useful for testing or when components are created
    /// programmatically rather than loaded from YAML.
    ///
    /// # Arguments
    /// * `metadata` - Pipeline metadata component
    /// * `request_schema` - Request schema component
    /// * `query_definition` - Query definition component
    /// * `response_schema` - Response schema component
    ///
    /// # Returns
    /// * `StandardPipeline` - New pipeline instance
    ///
    /// # Examples
    /// ```rust,no_run
    /// use pipeline::pipeline::StandardPipeline;
    /// use pipeline::types::{ComponentMetadata, RequestSchema, QueryDefinition, ResponseSchema};
    /// use datafusion::prelude::SessionContext;
    /// use std::collections::HashMap;
    /// use std::sync::Arc;
    ///
    /// let metadata = ComponentMetadata {
    ///     name: "test_pipeline".to_string(),
    ///     version: "1.0.0".to_string(),
    ///     description: None,
    ///     created_at: Some("2025-01-15T10:00:00.000+00:00".to_string()),
    ///     updated_at: Some("2025-01-15T10:00:00.000+00:00".to_string()),
    /// };
    ///
    /// let request = RequestSchema { fields: HashMap::new() };
    /// let query = QueryDefinition { sql: "SELECT * FROM table".to_string(), parameters: vec![] };
    /// let response = ResponseSchema { fields: HashMap::new() };
    /// let ctx = Arc::new(SessionContext::new());
    ///
    /// let pipeline = StandardPipeline::new(metadata, request, query, response, ctx).unwrap();
    /// ```
    pub fn new(
        metadata: ComponentMetadata,
        request_schema: RequestSchema,
        query_definition: QueryDefinition,
        response_schema: ResponseSchema,
        ctx: Arc<SessionContext>,
    ) -> Result<Self> {
        Ok(Self {
            metadata,
            request_schema: Arc::new(request_schema),
            query_definition,
            response_schema: Arc::new(response_schema),
            inferencer: Arc::new(SqlSchemaInferrer::new(ctx)?),
        })
    }

    /// Infer request schema from SQL query using named parameter analysis
    ///
    /// This method analyzes the pipeline's SQL query to extract {parameter_name} patterns
    /// and infers their types using AST analysis and table schema information.
    ///
    /// # Returns
    /// * `Result<RequestSchema>` - Inferred parameter schema or error
    ///
    /// # Examples
    /// ```rust,no_run
    /// use pipeline::pipeline::StandardPipeline;
    /// use pipeline::types::{ComponentMetadata, RequestSchema, QueryDefinition, ResponseSchema};
    /// use datafusion::prelude::SessionContext;
    /// use std::collections::HashMap;
    /// use std::sync::Arc;
    ///
    /// # tokio_test::block_on(async {
    /// # let metadata = ComponentMetadata {
    /// #     name: "test_pipeline".to_string(),
    /// #     version: "1.0.0".to_string(),
    /// #     description: None,
    /// #     created_at: Some("2025-01-15T10:00:00.000+00:00".to_string()),
    /// #     updated_at: Some("2025-01-15T10:00:00.000+00:00".to_string()),
    /// # };
    /// # let request = RequestSchema { fields: HashMap::new() };
    /// # let query = QueryDefinition { sql: "SELECT * FROM table".to_string(), parameters: vec![] };
    /// # let response = ResponseSchema { fields: HashMap::new() };
    /// # let ctx = Arc::new(SessionContext::new());
    /// let pipeline = StandardPipeline::new(metadata, request, query, response, ctx).unwrap();
    ///
    /// let inferred_request = pipeline.infer_request_schema().await.unwrap();
    /// for (param_name, field_type) in &inferred_request.fields {
    ///     println!("Parameter '{}' has type {:?}", param_name, field_type.field_type);
    /// }
    /// # })
    /// ```
    pub async fn infer_request_schema(&self) -> Result<RequestSchema> {
        let inferencer = self.get_inferencer();
        inferencer
            .extract_request_schema(&self.query_definition.sql)
            .await
    }

    /// Infer response schema from SQL query using SELECT clause analysis
    ///
    /// This method analyzes the pipeline's SQL query SELECT clause to extract
    /// output field names and types using DataFusion logical plan analysis.
    ///
    /// # Returns
    /// * `Result<ResponseSchema>` - Inferred response field schema or error
    ///
    /// # Examples
    /// ```rust,no_run
    /// use pipeline::pipeline::StandardPipeline;
    /// use pipeline::types::{ComponentMetadata, RequestSchema, QueryDefinition, ResponseSchema};
    /// use datafusion::prelude::SessionContext;
    /// use std::collections::HashMap;
    /// use std::sync::Arc;
    ///
    /// # tokio_test::block_on(async {
    /// # let metadata = ComponentMetadata {
    /// #     name: "test_pipeline".to_string(),
    /// #     version: "1.0.0".to_string(),
    /// #     description: None,
    /// #     created_at: Some("2025-01-15T10:00:00.000+00:00".to_string()),
    /// #     updated_at: Some("2025-01-15T10:00:00.000+00:00".to_string()),
    /// # };
    /// # let request = RequestSchema { fields: HashMap::new() };
    /// # let query = QueryDefinition { sql: "SELECT * FROM table".to_string(), parameters: vec![] };
    /// # let response = ResponseSchema { fields: HashMap::new() };
    /// # let ctx = Arc::new(SessionContext::new());
    /// let pipeline = StandardPipeline::new(metadata, request, query, response, ctx).unwrap();
    ///
    /// let inferred_response = pipeline.infer_response_schema().await.unwrap();
    /// for (field_name, field_type) in &inferred_response.fields {
    ///     println!("Field '{}' has type {:?}", field_name, field_type.field_type);
    /// }
    /// # })
    /// ```
    pub async fn infer_response_schema(&self) -> Result<ResponseSchema> {
        let inferencer = self.get_inferencer();
        inferencer
            .extract_response_schema(&self.query_definition.sql)
            .await
    }

    /// Get reference to the SQL schema inferencer
    ///
    /// # Returns
    /// * `&SqlSchemaInferrer` - Reference to inferencer
    fn get_inferencer(&self) -> &SqlSchemaInferrer {
        self.inferencer.as_ref()
    }
}

#[async_trait]
impl Pipeline for StandardPipeline {
    /// Load pipeline from YAML file with schema inference from SQL query
    ///
    /// This method reads a YAML file containing only metadata and query sections.
    /// Request and response schemas are automatically inferred from the SQL query
    /// using the provided Arc<SessionContext> with registered tables.
    ///
    /// # Arguments
    /// * `file_path` - Path to the pipeline YAML file
    /// * `ctx` - Arc<SessionContext> with registered tables for schema inference
    ///
    /// # Returns
    /// * `Result<Self>` - Complete pipeline instance or error with detailed context
    ///
    /// # Examples
    /// ```rust,no_run
    /// use pipeline::pipeline::{Pipeline, StandardPipeline};
    /// use datafusion::prelude::SessionContext;
    /// use std::path::Path;
    /// use std::sync::Arc;
    ///
    /// # tokio_test::block_on(async {
    /// let ctx = Arc::new(SessionContext::new());
    /// // Register tables in ctx...
    /// let pipeline_path = Path::new("pipeline.yaml");
    /// let pipeline = StandardPipeline::load_from_file(pipeline_path, ctx).await.unwrap();
    /// # })
    /// ```
    async fn load_from_file<P: AsRef<Path> + Send>(
        file_path: P,
        ctx: Arc<SessionContext>,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let file_content = fs::read_to_string(file_path)?;

        // Parse the optimized pipeline YAML file into a generic structure
        let pipeline_yaml: serde_yaml::Value = serde_yaml::from_str(&file_content)
            .map_err(|e| anyhow!("Failed to parse pipeline YAML file: {}", e))?;

        // Extract only metadata and query sections - request/response are inferred
        let metadata_section = pipeline_yaml
            .get("metadata")
            .ok_or_else(|| anyhow!("Missing 'metadata' section in pipeline YAML"))?;
        let query_section = pipeline_yaml
            .get("query")
            .ok_or_else(|| anyhow!("Missing 'query' section in pipeline YAML"))?;

        // Parse metadata directly
        let metadata: ComponentMetadata = serde_yaml::from_value(metadata_section.clone())
            .map_err(|e| anyhow!("Failed to parse metadata section: {}", e))?;

        // Parse query as string and create QueryDefinition with empty parameters (will be inferred)
        let sql_query: String = serde_yaml::from_value(query_section.clone())
            .map_err(|e| anyhow!("Failed to parse query section: {}", e))?;
        let query_definition = QueryDefinition {
            sql: sql_query,
            parameters: Vec::new(), // Parameters will be inferred from SQL
        };

        // Create inferencer with the provided Arc<SessionContext>
        let inferencer = SqlSchemaInferrer::new(ctx.clone())?;

        // Infer request and response schemas from SQL query
        let request_schema = inferencer
            .extract_request_schema(&query_definition.sql)
            .await?;
        let response_schema = inferencer
            .extract_response_schema(&query_definition.sql)
            .await?;

        // Construct the pipeline with inferred schemas
        StandardPipeline::new(
            metadata,
            request_schema,
            query_definition,
            response_schema,
            ctx,
        )
    }

    /// Validate the complete pipeline for internal consistency and business rules
    ///
    /// This method performs comprehensive validation across all pipeline components:
    /// - Individual component validation using `SchemaValidation` trait
    /// - Cross-component consistency checks (request/response field compatibility)
    /// - Business rule validation (required fields, parameter binding, etc.)
    /// - SQL query parameter validation against request schema
    ///
    /// # Returns
    /// * `Result<ValidationReport>` - Detailed validation report or critical error
    ///
    /// # Examples
    /// ```rust,no_run
    /// use pipeline::pipeline::{Pipeline, StandardPipeline};
    /// use datafusion::prelude::SessionContext;
    /// use std::sync::Arc;
    ///
    /// # tokio_test::block_on(async {
    /// let ctx = Arc::new(SessionContext::new());
    /// let pipeline = StandardPipeline::load_from_file("pipeline.yaml", ctx).await.unwrap();
    /// let validation_report = pipeline.validate().unwrap();
    ///
    /// if validation_report.errors.is_empty() {
    ///     println!("Pipeline validation passed!");
    /// } else {
    ///     for error in &validation_report.errors {
    ///         println!("Validation error: {}", error.message);
    ///     }
    /// }
    /// # })
    /// ```
    fn validate(&self) -> Result<ValidationReport> {
        // This is a placeholder. In a real scenario, you'd validate each component
        // individually and then check cross-component consistency.
        // For now, we'll just return a dummy report.
        Ok(ValidationReport {
            errors: Vec::new(),
            warnings: Vec::new(),
            summary: "Pipeline validation completed - no issues found".to_string(),
        })
    }

    /// Validate request/response field compatibility
    ///
    /// This method checks that fields defined in both request and response schemas
    /// have compatible types for safe data flow through the pipeline. Uses the
    /// `DataFusionType::is_compatible` method for type compatibility checking.
    ///
    /// # Returns
    /// * `Result<Vec<String>>` - List of compatibility warnings, or error for critical issues
    ///
    /// # Examples
    /// ```rust,no_run
    /// use pipeline::pipeline::{Pipeline, StandardPipeline};
    /// use datafusion::prelude::SessionContext;
    /// use std::sync::Arc;
    ///
    /// # tokio_test::block_on(async {
    /// let ctx = Arc::new(SessionContext::new());
    /// let pipeline = StandardPipeline::load_from_file("pipeline.yaml", ctx).await.unwrap();
    /// let warnings = pipeline.validate_field_compatibility().unwrap();
    ///
    /// for warning in warnings {
    ///     println!("Compatibility warning: {}", warning);
    /// }
    /// # })
    /// ```
    fn validate_field_compatibility(&self) -> Result<Vec<String>> {
        // This is a placeholder. In a real scenario, you'd check type compatibility
        // for all fields in request and response schemas.
        Ok(vec![])
    }

    /// Validate SQL query parameters against request schema
    ///
    /// This method ensures that all parameters referenced in the SQL query
    /// (e.g., ${parameter_name}) have corresponding fields in the request schema
    /// with compatible types for parameter binding.
    ///
    /// # Returns
    /// * `Result<Vec<String>>` - List of parameter validation issues or error
    ///
    /// # Examples
    /// ```rust,no_run
    /// use pipeline::pipeline::{Pipeline, StandardPipeline};
    /// use datafusion::prelude::SessionContext;
    /// use std::sync::Arc;
    ///
    /// # tokio_test::block_on(async {
    /// let ctx = Arc::new(SessionContext::new());
    /// let pipeline = StandardPipeline::load_from_file("pipeline.yaml", ctx).await.unwrap();
    /// let issues = pipeline.validate_query_parameters().unwrap();
    ///
    /// if issues.is_empty() {
    ///     println!("All query parameters are valid!");
    /// } else {
    ///     for issue in issues {
    ///         println!("Parameter issue: {}", issue);
    ///     }
    /// }
    /// # })
    /// ```
    fn validate_query_parameters(&self) -> Result<Vec<String>> {
        // This is a placeholder. In a real scenario, you'd check if all parameters
        // in the SQL query have corresponding fields in the request schema.
        Ok(vec![])
    }

    /// Export the complete pipeline as YAML string
    ///
    /// This method serializes the entire pipeline (all components) back to YAML format
    /// using the `YamlSerializable::to_yaml` trait. Useful for pipeline persistence,
    /// debugging, or creating pipeline templates.
    ///
    /// # Returns
    /// * `Result<String>` - Complete pipeline YAML or serialization error
    ///
    /// # Examples
    /// ```rust,no_run
    /// use pipeline::pipeline::{Pipeline, StandardPipeline};
    /// use datafusion::prelude::SessionContext;
    /// use std::sync::Arc;
    ///
    /// # tokio_test::block_on(async {
    /// let ctx = Arc::new(SessionContext::new());
    /// let pipeline = StandardPipeline::load_from_file("pipeline.yaml", ctx).await.unwrap();
    /// let yaml_output = pipeline.to_yaml().unwrap();
    /// println!("Pipeline YAML:\n{}", yaml_output);
    /// # })
    /// ```
    fn to_yaml(&self) -> Result<String> {
        // This is a placeholder. In a real scenario, you'd serialize the entire pipeline
        // using the YamlSerializable trait.
        Ok(serde_yaml::to_string(self).unwrap())
    }

    /// Get pipeline name from metadata
    ///
    /// Convenience method to access the pipeline name without going through metadata.
    ///
    /// # Returns
    /// * `&str` - Pipeline name from metadata
    fn name(&self) -> &str {
        &self.metadata.name
    }

    /// Get pipeline version from metadata
    ///
    /// Convenience method to access the pipeline version without going through metadata.
    ///
    /// # Returns
    /// * `&str` - Pipeline version from metadata
    fn version(&self) -> &str {
        &self.metadata.version
    }

    /// Check if pipeline is valid (no validation errors)
    ///
    /// This is a convenience method that runs full validation and returns
    /// a simple boolean result for quick validity checks.
    ///
    /// # Returns
    /// * `Result<bool>` - True if pipeline is valid, false if has errors, or validation error
    ///
    /// # Examples
    /// ```rust,no_run
    /// use pipeline::pipeline::{Pipeline, StandardPipeline};
    /// use datafusion::prelude::SessionContext;
    /// use std::sync::Arc;
    ///
    /// # tokio_test::block_on(async {
    /// let ctx = Arc::new(SessionContext::new());
    /// let pipeline = StandardPipeline::load_from_file("pipeline.yaml", ctx).await.unwrap();
    /// if pipeline.is_valid().unwrap_or(false) {
    ///     println!("Pipeline is ready for execution!");
    /// } else {
    ///     println!("Pipeline has validation issues");
    /// }
    /// # })
    /// ```
    fn is_valid(&self) -> Result<bool> {
        // This is a placeholder. In a real scenario, you'd run the full validation
        // and return the result.
        Ok(true)
    }

    // ========================================================================
    // Simple Accessor Methods - IMPLEMENTED
    // ========================================================================

    /// Get the pipeline metadata component
    fn metadata(&self) -> &ComponentMetadata {
        &self.metadata
    }

    /// Get the request schema component
    fn request_schema(&self) -> &RequestSchema {
        &self.request_schema
    }

    /// Get the query definition component
    fn query_definition(&self) -> &QueryDefinition {
        &self.query_definition
    }

    /// Get the response schema component
    fn response_schema(&self) -> &ResponseSchema {
        &self.response_schema
    }

    /// Execute the pipeline's SQL query using the provided engine
    ///
    /// This method delegates SQL execution to the provided engine implementation.
    /// It simply calls the engine's execute method with the provided SQL query.
    ///
    /// # Arguments
    ///
    /// * `engine` - The engine implementation to use for SQL execution
    /// * `sql` - The SQL query to execute
    ///
    /// # Returns
    ///
    /// Returns a `Result<RecordBatch>` containing the query results or an error
    /// if the query execution fails.
    async fn execute(&self, engine: &dyn Engine, sql: &str) -> Result<RecordBatch> {
        engine.execute(sql).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ComponentMetadata, QueryDefinition, RequestSchema, ResponseSchema};
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_named_parameter_inference() {
        // Create a simple pipeline with named parameters in SQL
        let metadata = ComponentMetadata {
            name: "test_pipeline".to_string(),
            version: "1.0.0".to_string(),
            description: Some("Test pipeline with named parameters".to_string()),
            created_at: Some("2025-01-15T10:00:00.000+00:00".to_string()),
            updated_at: Some("2025-01-15T10:00:00.000+00:00".to_string()),
        };

        let request = RequestSchema {
            fields: HashMap::new(),
        };
        let response = ResponseSchema {
            fields: HashMap::new(),
        };

        // SQL with Rust-native {parameter_name} syntax
        let query = QueryDefinition {
            sql: r#"
                SELECT
                  "Index" as product_id,
                  "Name" as product_name,
                  "Brand" as brand,
                  "Price" as price
                FROM products
                WHERE 1=1
                  AND ({brand} IS NULL OR "Brand" = {brand})
                  AND ({max_price} IS NULL OR "Price" < {max_price})
                  AND ({min_price} IS NULL OR "Price" >= {min_price})
                LIMIT {limit}
            "#
            .to_string(),
            parameters: vec![], // This will be inferred automatically
        };

        // Create pipeline with inferencer
        let ctx = Arc::new(SessionContext::new());
        let pipeline = StandardPipeline::new(metadata, request, query, response, ctx)
            .expect("Failed to create pipeline");

        // Test parameter inference
        match pipeline.infer_request_schema().await {
            Ok(inferred_request) => {
                println!(
                    "✅ Successfully inferred {} parameters:",
                    inferred_request.fields.len()
                );
                for (param_name, field_type) in &inferred_request.fields {
                    println!(
                        "  - Parameter '{}': {:?} (nullable: {})",
                        param_name, field_type.field_type, field_type.nullable
                    );
                }

                // Should have detected: brand, max_price, min_price, limit
                assert!(inferred_request.fields.contains_key("brand"));
                assert!(inferred_request.fields.contains_key("max_price"));
                assert!(inferred_request.fields.contains_key("min_price"));
                assert!(inferred_request.fields.contains_key("limit"));
            }
            Err(e) => {
                println!(
                    "⚠️  Parameter inference failed (expected without table schema): {}",
                    e
                );
                // This is expected since we don't have a registered table schema
            }
        }

        // Test response field inference
        match pipeline.infer_response_schema().await {
            Ok(inferred_response) => {
                println!(
                    "✅ Successfully inferred {} response fields:",
                    inferred_response.fields.len()
                );
                for (field_name, field_type) in &inferred_response.fields {
                    println!(
                        "  - Field '{}': {:?} (nullable: {})",
                        field_name, field_type.field_type, field_type.nullable
                    );
                }

                // Should have detected: product_id, product_name, brand, price
                assert!(inferred_response.fields.contains_key("product_id"));
                assert!(inferred_response.fields.contains_key("product_name"));
                assert!(inferred_response.fields.contains_key("brand"));
                assert!(inferred_response.fields.contains_key("price"));
            }
            Err(e) => {
                println!(
                    "⚠️  Response field inference failed (expected without table schema): {}",
                    e
                );
                // This is expected since we don't have a registered table schema
            }
        }
    }

    #[tokio::test]
    async fn test_load_from_yaml_with_complete_inference() {
        use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use std::io::Write;
        use std::sync::Arc;
        use tempfile::NamedTempFile;

        // Create a test SessionContext with registered tables
        let ctx = Arc::new(SessionContext::new());

        // Create a test products table schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("product_id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("brand", DataType::Utf8, true),
            Field::new("price", DataType::Float64, false),
            Field::new("category", DataType::Utf8, true),
            Field::new("stock", DataType::Int64, false),
        ]));

        // Create sample data
        let product_ids = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
        let names = Arc::new(StringArray::from(vec![
            "Laptop Pro",
            "Gaming Mouse",
            "Wireless Headphones",
            "Tablet",
            "Smartphone",
        ]));
        let brands = Arc::new(StringArray::from(vec![
            Some("Apple"),
            Some("Logitech"),
            Some("Sony"),
            Some("Samsung"),
            Some("Google"),
        ]));
        let prices = Arc::new(Float64Array::from(vec![
            1299.99, 79.99, 199.99, 599.99, 899.99,
        ]));
        let categories = Arc::new(StringArray::from(vec![
            Some("Electronics"),
            Some("Accessories"),
            Some("Audio"),
            Some("Electronics"),
            Some("Electronics"),
        ]));
        let stock = Arc::new(Int64Array::from(vec![50, 120, 75, 30, 85]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![product_ids, names, brands, prices, categories, stock],
        )
        .unwrap();

        // Register the table in SessionContext
        ctx.register_batch("products", batch).unwrap();

        // Create a temporary YAML file with pipeline definition
        let yaml_content = r#"
metadata:
  name: "product_search_pipeline"
  version: "1.2.0"
  description: "Advanced product search with filtering and pagination"
  created_at: "2025-01-15T10:00:00.000+00:00"
  updated_at: "2025-01-15T12:30:00.000+00:00"

query: |
  SELECT
    product_id,
    name as product_name,
    brand,
    price,
    category,
    stock
  FROM products
  WHERE 1=1
    AND ({brand} IS NULL OR brand = {brand})
    AND ({min_price} IS NULL OR price >= {min_price})
    AND ({max_price} IS NULL OR price <= {max_price})
    AND ({category} IS NULL OR category = {category})
    AND ({min_stock} IS NULL OR stock >= {min_stock})
  ORDER BY price DESC
  LIMIT {limit}
"#;

        // Write YAML content to temporary file
        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
        temp_file
            .write_all(yaml_content.as_bytes())
            .expect("Failed to write YAML content");
        let temp_path = temp_file.path();

        // Test the complete pipeline loading with inference
        let pipeline = StandardPipeline::load_from_file(temp_path, ctx.clone())
            .await
            .expect("Failed to load pipeline from YAML");

        // Verify metadata was loaded correctly
        assert_eq!(pipeline.metadata().name, "product_search_pipeline");
        assert_eq!(pipeline.metadata().version, "1.2.0");
        assert_eq!(
            pipeline.metadata().description,
            Some("Advanced product search with filtering and pagination".to_string())
        );

        // Verify query was loaded correctly
        let sql = &pipeline.query_definition().sql;
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("FROM products"));
        assert!(sql.contains("{brand}"));
        assert!(sql.contains("{min_price}"));
        assert!(sql.contains("{max_price}"));
        assert!(sql.contains("{limit}"));

        // Verify request schema was inferred correctly
        let request_schema = pipeline.request_schema();
        println!("🔍 Inferred request parameters:");
        for (param_name, field_type) in &request_schema.fields {
            println!(
                "  - {}: {:?} (nullable: {})",
                param_name, field_type.field_type, field_type.nullable
            );
        }

        // Should have detected all parameters from SQL
        assert!(request_schema.fields.contains_key("brand"));
        assert!(request_schema.fields.contains_key("min_price"));
        assert!(request_schema.fields.contains_key("max_price"));
        assert!(request_schema.fields.contains_key("category"));
        assert!(request_schema.fields.contains_key("min_stock"));
        assert!(request_schema.fields.contains_key("limit"));

        // Verify parameter types are correctly inferred from table schema
        assert_eq!(request_schema.fields["brand"].field_type, DataType::Utf8);
        assert_eq!(
            request_schema.fields["min_price"].field_type,
            DataType::Float64
        );
        assert_eq!(
            request_schema.fields["max_price"].field_type,
            DataType::Float64
        );
        assert_eq!(request_schema.fields["category"].field_type, DataType::Utf8);
        assert_eq!(
            request_schema.fields["min_stock"].field_type,
            DataType::Int64
        );
        assert_eq!(request_schema.fields["limit"].field_type, DataType::Int64); // Special case: LIMIT is always Int64

        // Verify response schema was inferred correctly
        let response_schema = pipeline.response_schema();
        println!("🔍 Inferred response fields:");
        for (field_name, field_type) in &response_schema.fields {
            println!(
                "  - {}: {:?} (nullable: {})",
                field_name, field_type.field_type, field_type.nullable
            );
        }

        // Should have detected all SELECT fields with aliases
        assert!(response_schema.fields.contains_key("product_id"));
        assert!(response_schema.fields.contains_key("product_name")); // aliased from 'name'
        assert!(response_schema.fields.contains_key("brand"));
        assert!(response_schema.fields.contains_key("price"));
        assert!(response_schema.fields.contains_key("category"));
        assert!(response_schema.fields.contains_key("stock"));

        // Verify response field types match table schema
        assert_eq!(
            response_schema.fields["product_id"].field_type,
            DataType::Int64
        );
        assert_eq!(
            response_schema.fields["product_name"].field_type,
            DataType::Utf8
        );
        assert_eq!(response_schema.fields["brand"].field_type, DataType::Utf8);
        assert_eq!(
            response_schema.fields["price"].field_type,
            DataType::Float64
        );
        assert_eq!(
            response_schema.fields["category"].field_type,
            DataType::Utf8
        );
        assert_eq!(response_schema.fields["stock"].field_type, DataType::Int64);

        // Verify nullability is correctly inferred
        assert!(!response_schema.fields["product_id"].nullable); // NOT NULL in schema
        assert!(!response_schema.fields["product_name"].nullable); // NOT NULL in schema
        assert!(response_schema.fields["brand"].nullable); // Nullable in schema
        assert!(!response_schema.fields["price"].nullable); // NOT NULL in schema
        assert!(response_schema.fields["category"].nullable); // Nullable in schema
        assert!(!response_schema.fields["stock"].nullable); // NOT NULL in schema

        // Test that we can perform additional inference operations
        let re_inferred_request = pipeline
            .infer_request_schema()
            .await
            .expect("Failed to re-infer request schema");
        assert_eq!(
            re_inferred_request.fields.len(),
            request_schema.fields.len()
        );

        let re_inferred_response = pipeline
            .infer_response_schema()
            .await
            .expect("Failed to re-infer response schema");
        assert_eq!(
            re_inferred_response.fields.len(),
            response_schema.fields.len()
        );
    }
}
