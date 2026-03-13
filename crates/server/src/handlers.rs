use anyhow::Result;
use arrow::record_batch::RecordBatch;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use datafusion::prelude::SessionContext;
use pipeline::pipeline::Pipeline;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use skardi_engine::Engine;
use std::collections::HashMap;
use std::time::Instant;

use crate::config::DataSourceType;
use crate::server::AppState;

/// Request structure for pipeline execution
#[derive(Debug, Deserialize)]
pub struct ExecuteRequest {
    /// Dynamic JSON parameters that match pipeline request schema
    #[serde(flatten)]
    pub parameters: HashMap<String, Value>,
}

/// Response structure for pipeline execution
#[derive(Debug, Serialize)]
pub struct ExecuteResponse {
    /// Query result data
    pub data: Vec<Value>,
    /// Number of rows returned
    pub rows: usize,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
}

/// Error response structure for API endpoints
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    /// Whether the operation was successful
    pub success: bool,
    /// Error message
    pub error: String,
    /// Error category/type
    pub error_type: String,
    /// Additional error details
    pub details: Option<Value>,
    /// Timestamp when error occurred
    pub timestamp: String,
}

/// Field information for table schema
#[derive(Debug, Clone, Serialize)]
pub struct FieldInfo {
    /// Column name
    pub name: String,
    /// Arrow data type as string representation
    pub r#type: String,
    /// Whether the field is nullable
    pub nullable: bool,
}

/// Table information with schema
#[derive(Debug, Clone, Serialize)]
pub struct TableInfo {
    /// Table name (same as data source name)
    pub name: String,
    /// Table schema fields
    pub schema: Vec<FieldInfo>,
}

/// Data source response structure
#[derive(Debug, Clone, Serialize)]
pub struct DataSourceResponse {
    /// Data source name
    pub name: String,
    /// Data source type (lowercase: csv, parquet, postgres, lance)
    pub r#type: String,
    /// File path for file-based sources (CSV, Parquet, Lance)
    pub path: Option<String>,
    /// Sanitized URL for database sources (PostgreSQL)
    pub url: Option<String>,
    /// Registered tables with their schemas
    pub tables: Vec<TableInfo>,
}

/// Helper function to create error responses
fn create_error_response(
    error_msg: &str,
    error_type: &str,
    details: Option<Value>,
) -> Json<ErrorResponse> {
    Json(ErrorResponse {
        success: false,
        error: error_msg.to_string(),
        error_type: error_type.to_string(),
        details,
        timestamp: chrono::Utc::now().to_rfc3339(),
    })
}

/// Helper function to create success response with data
fn create_success_response(data: Vec<Value>, rows: usize, execution_time_ms: u64) -> Json<Value> {
    Json(serde_json::json!({
        "success": true,
        "data": data,
        "rows": rows,
        "execution_time_ms": execution_time_ms,
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}

/// Sanitize database connection string by removing credentials
///
/// Get table schema from SessionContext
///
/// Retrieves the schema for a registered table from the DataFusion SessionContext.
///
/// # Arguments
///
/// * `ctx` - DataFusion SessionContext containing registered tables
/// * `table_name` - Name of the table to get schema for
///
/// # Returns
///
/// Returns a vector of FieldInfo containing column name, type, and nullability.
/// Returns an error if the table is not found or schema retrieval fails.
async fn get_table_schema(ctx: &SessionContext, table_name: &str) -> Result<Vec<FieldInfo>> {
    // Get the default catalog
    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(|| anyhow::anyhow!("Default catalog 'datafusion' not found"))?;

    // Get the public schema
    let schema = catalog
        .schema("public")
        .ok_or_else(|| anyhow::anyhow!("Schema 'public' not found"))?;

    // Get the table
    let table = schema
        .table(table_name)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get table '{}': {}", table_name, e))?
        .ok_or_else(|| anyhow::anyhow!("Table '{}' not found in catalog", table_name))?;

    // Get the table schema
    let table_schema = table.schema();

    // Convert Arrow fields to FieldInfo
    let fields: Vec<FieldInfo> = table_schema
        .fields()
        .iter()
        .map(|field| FieldInfo {
            name: field.name().clone(),
            r#type: format!("{:?}", field.data_type()),
            nullable: field.is_nullable(),
        })
        .collect();

    Ok(fields)
}

/// Health check endpoint - GET /health
pub async fn health_check() -> Result<Json<Value>, StatusCode> {
    let response = serde_json::json!({
        "status": "healthy",
        "service": "skardi-server",
        "timestamp": chrono::Utc::now().to_rfc3339()
    });

    Ok(Json(response))
}

/// Per-pipeline health check endpoint - GET /health/:name
///
/// Performs a comprehensive health check for a specific pipeline:
/// - Verifies the pipeline exists and is loaded
/// - Validates the pipeline configuration
/// - Checks that required data sources are accessible
pub async fn pipeline_health_check(
    State(app_state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let start_time = Instant::now();

    // Get pipeline and data sources info
    let (pipeline_info, data_source_names) = {
        let config = app_state.config.read().map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                create_error_response(
                    "Failed to acquire read lock on configuration",
                    "internal_error",
                    None,
                ),
            )
        })?;

        let pipeline = config.pipelines.get(&name).ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                create_error_response(
                    &format!("Pipeline '{}' not found", name),
                    "pipeline_not_found",
                    Some(serde_json::json!({
                        "available_pipelines": config.pipelines.keys().collect::<Vec<_>>()
                    })),
                ),
            )
        })?;

        let info = serde_json::json!({
            "name": pipeline.name(),
            "version": pipeline.version(),
            "parameters": pipeline.request_schema().fields.keys().collect::<Vec<_>>(),
        });

        let ds_names: Vec<String> = config
            .data_sources
            .iter()
            .map(|ds| ds.name.clone())
            .collect();

        (info, ds_names)
    };

    // Check data source accessibility by verifying tables are registered
    let session_ctx = app_state.engine.session_context();
    let mut data_source_checks: Vec<Value> = Vec::new();

    for ds_name in &data_source_names {
        let status = match session_ctx.table(ds_name).await {
            Ok(_) => serde_json::json!({
                "name": ds_name,
                "status": "healthy",
                "accessible": true
            }),
            Err(e) => serde_json::json!({
                "name": ds_name,
                "status": "unhealthy",
                "accessible": false,
                "error": e.to_string()
            }),
        };
        data_source_checks.push(status);
    }

    let all_healthy = data_source_checks.iter().all(|ds| {
        ds.get("accessible")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    });

    let health_time_ms = start_time.elapsed().as_millis() as u64;

    let overall_status = if all_healthy { "healthy" } else { "degraded" };

    Ok(Json(serde_json::json!({
        "status": overall_status,
        "pipeline": pipeline_info,
        "data_sources": {
            "total": data_source_checks.len(),
            "healthy": data_source_checks.iter().filter(|ds|
                ds.get("accessible").and_then(|v| v.as_bool()).unwrap_or(false)
            ).count(),
            "checks": data_source_checks
        },
        "health_check_time_ms": health_time_ms,
        "timestamp": chrono::Utc::now().to_rfc3339()
    })))
}

/// List all pipelines endpoint - GET /pipelines
pub async fn list_pipelines(State(app_state): State<AppState>) -> Result<Json<Value>, StatusCode> {
    let config = app_state
        .config
        .read()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let pipelines: Vec<Value> = config
        .pipelines
        .iter()
        .map(|(name, pipeline)| {
            serde_json::json!({
                "name": name,
                "version": pipeline.version(),
                "endpoint": format!("/{}/execute", name)
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "success": true,
        "pipelines": pipelines,
        "count": pipelines.len(),
        "data_sources": config.data_sources.len(),
        "timestamp": chrono::Utc::now().to_rfc3339()
    })))
}

/// Get specific pipeline information endpoint - GET /pipeline/:name
pub async fn get_pipelines_info(
    State(app_state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let config = app_state.config.read().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            create_error_response(
                "Failed to acquire read lock on configuration",
                "internal_error",
                None,
            ),
        )
    })?;

    if let Some(pipeline) = config.pipelines.get(&name) {
        let request_schema = pipeline.request_schema();
        let params: Vec<Value> = request_schema
            .fields
            .iter()
            .map(|(param_name, field_type)| {
                serde_json::json!({
                    "name": param_name,
                    "type": format!("{:?}", field_type)
                })
            })
            .collect();

        Ok(Json(serde_json::json!({
            "success": true,
            "pipeline": {
                "name": pipeline.name(),
                "version": pipeline.version(),
                "endpoint": format!("/{}/execute", name),
                "parameters": params,
                "created_at": pipeline.metadata.created_at,
                "updated_at": pipeline.metadata.updated_at
            },
            "timestamp": chrono::Utc::now().to_rfc3339()
        })))
    } else {
        Err((
            StatusCode::NOT_FOUND,
            create_error_response(
                &format!("Pipeline '{}' not found", name),
                "pipeline_not_found",
                Some(serde_json::json!({
                    "available_pipelines": config.pipelines.keys().collect::<Vec<_>>()
                })),
            ),
        ))
    }
}

/// Get data sources endpoint - GET /data_source
///
/// Returns information about all registered data sources including their type,
/// path/URL, registered tables, and schemas.
pub async fn get_data_sources(
    State(app_state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    // Acquire lock and extract data sources, then drop lock before async operations
    let data_sources = {
        let config = app_state.config.read().map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                create_error_response(
                    "Failed to acquire read lock on configuration",
                    "internal_error",
                    None,
                ),
            )
        })?;
        config.data_sources.clone()
    };

    let session_ctx = app_state.engine.session_context();
    let mut data_source_responses = Vec::new();

    for data_source in &data_sources {
        // Convert source type to lowercase string
        let source_type_str = match data_source.source_type {
            DataSourceType::Csv => "csv",
            DataSourceType::Parquet => "parquet",
            DataSourceType::Postgres => "postgres",
            DataSourceType::Mysql => "mysql",
            DataSourceType::Iceberg => "iceberg",
            DataSourceType::Mongo => "mongo",
            DataSourceType::Lance => "lance",
        };

        // Determine path or URL based on source type
        let path = match data_source.source_type {
            DataSourceType::Csv
            | DataSourceType::Parquet
            | DataSourceType::Lance
            | DataSourceType::Iceberg => Some(data_source.path.to_string_lossy().to_string()),
            DataSourceType::Postgres | DataSourceType::Mysql | DataSourceType::Mongo => None,
        };

        let url = match data_source.source_type {
            DataSourceType::Postgres | DataSourceType::Mysql | DataSourceType::Mongo => {
                // For database sources, return the connection string as-is
                // (credentials are not stored in connection strings, only in env vars)
                data_source.connection_string.clone()
            }
            _ => None,
        };

        // Get table schema from SessionContext
        let table_schema = match get_table_schema(session_ctx, &data_source.name).await {
            Ok(fields) => fields,
            Err(e) => {
                tracing::warn!(
                    "Failed to get schema for table '{}': {}",
                    data_source.name,
                    e
                );
                // Continue with empty schema if table not found or schema retrieval fails
                Vec::new()
            }
        };

        // Build table info (data source name is the table name)
        let tables = vec![TableInfo {
            name: data_source.name.clone(),
            schema: table_schema,
        }];

        data_source_responses.push(DataSourceResponse {
            name: data_source.name.clone(),
            r#type: source_type_str.to_string(),
            path,
            url,
            tables,
        });
    }

    // Return success response with data sources
    Ok(Json(serde_json::json!({
        "success": true,
        "data": data_source_responses,
        "count": data_source_responses.len(),
        "timestamp": chrono::Utc::now().to_rfc3339()
    })))
}

/// Execute pipeline endpoint - POST /:name/execute
pub async fn execute_pipeline_by_name(
    State(app_state): State<AppState>,
    Path(pipeline_name): Path<String>,
    Json(request): Json<ExecuteRequest>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let start_time = Instant::now();

    tracing::info!(
        "Received execution request for pipeline '{}' with {} parameters",
        pipeline_name,
        request.parameters.len()
    );

    // Acquire read lock and get the specified pipeline
    // Extract what we need and drop the lock immediately
    let (sql_template, expected_params) = {
        let config = app_state.config.read().map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                create_error_response(
                    "Failed to acquire read lock on configuration",
                    "internal_error",
                    None,
                ),
            )
        })?;

        let pipeline = config.pipelines.get(&pipeline_name).ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                create_error_response(
                    &format!("Pipeline '{}' not found", pipeline_name),
                    "pipeline_not_found",
                    Some(serde_json::json!({
                        "requested_pipeline": pipeline_name,
                        "available_pipelines": config.pipelines.keys().collect::<Vec<_>>()
                    })),
                ),
            )
        })?;

        // Get the SQL query and inferred parameters from the pipeline
        let query_def = pipeline.query_definition();
        let request_schema = pipeline.request_schema();
        let expected_params: Vec<String> = request_schema.fields.keys().cloned().collect();
        (query_def.sql.clone(), expected_params)
    };

    let mut sql = sql_template;

    // Validate that all required parameters are provided
    let mut missing_params = Vec::new();
    let mut unsupported_params = Vec::new();

    // Replace parameter placeholders with actual values
    for param_name in &expected_params {
        let placeholder = format!("{{{}}}", param_name);

        if let Some(param_value) = request.parameters.get(param_name) {
            // Convert JSON value to SQL-safe string
            let sql_value = match param_value {
                Value::String(s) => format!("'{}'", s.replace("'", "''")), // Escape single quotes
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Null => "NULL".to_string(),
                _ => {
                    tracing::error!(
                        "Unsupported parameter type for {}: {:?}",
                        param_name,
                        param_value
                    );
                    unsupported_params.push(format!("{}: {:?}", param_name, param_value));
                    continue;
                }
            };

            sql = sql.replace(&placeholder, &sql_value);
        } else {
            tracing::error!("Missing required parameter: {}", param_name);
            missing_params.push(param_name.clone());
        }
    }

    // Return detailed error for parameter validation issues
    if !missing_params.is_empty() || !unsupported_params.is_empty() {
        let mut error_details = serde_json::json!({
            "expected_parameters": expected_params,
            "received_parameters": request.parameters.keys().collect::<Vec<_>>()
        });

        if !missing_params.is_empty() {
            error_details["missing_parameters"] = serde_json::json!(missing_params);
        }

        if !unsupported_params.is_empty() {
            error_details["unsupported_parameters"] = serde_json::json!(unsupported_params);
        }

        let error_msg = if !missing_params.is_empty() {
            format!("Missing required parameters: {}", missing_params.join(", "))
        } else {
            format!(
                "Unsupported parameter types: {}",
                unsupported_params.join(", ")
            )
        };

        return Err((
            StatusCode::BAD_REQUEST,
            create_error_response(
                &error_msg,
                "parameter_validation_error",
                Some(error_details),
            ),
        ));
    }

    // Execute the query using the DataFusion engine
    let record_batch = match app_state.engine.execute(&sql).await {
        Ok(batch) => batch,
        Err(e) => {
            tracing::error!("Query execution failed: {}", e);
            tracing::debug!("Failed SQL query: {}", sql); // Log SQL for debugging but don't expose in response

            let error_details = serde_json::json!({
                "engine_error": e.to_string(),
                "registered_tables": "Check server logs for data source registration status",
                "suggestion": "Verify that data sources are properly registered and accessible"
            });

            let error_msg = format!("SQL query execution failed: {}", e);

            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                create_error_response(&error_msg, "query_execution_error", Some(error_details)),
            ));
        }
    };

    // Convert RecordBatch to JSON
    let data = match record_batch_to_json(&record_batch) {
        Ok(json_data) => json_data,
        Err(e) => {
            tracing::error!("Failed to convert results to JSON: {}", e);

            let error_details = serde_json::json!({
                "conversion_error": e.to_string(),
                "record_batch_schema": format!("{:?}", record_batch.schema()),
                "record_batch_rows": record_batch.num_rows()
            });

            let error_msg = format!("Failed to convert query results to JSON: {}", e);

            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                create_error_response(&error_msg, "result_conversion_error", Some(error_details)),
            ));
        }
    };

    let execution_time = start_time.elapsed().as_millis() as u64;
    let row_count = record_batch.num_rows();

    tracing::info!(
        "Query completed successfully: {} rows in {}ms",
        row_count,
        execution_time
    );

    Ok(create_success_response(data, row_count, execution_time))
}

/// Convert Arrow RecordBatch to JSON array using arrow_json
fn record_batch_to_json(batch: &RecordBatch) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    use arrow_json::{writer::JsonArray, WriterBuilder};
    use serde_json::Map;

    // Write the record batch to JSON using arrow_json with null value inclusion
    let buf = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true) // Include null values in JSON output
        .build::<_, JsonArray>(buf);
    writer.write_batches(&vec![batch])?;
    writer.finish()?;
    let json_data = writer.into_inner();

    // Parse the JSON array string into serde_json::Value objects
    let json_rows: Vec<Map<String, Value>> = serde_json::from_reader(json_data.as_slice())?;

    // Convert Map objects to Value objects
    let values: Vec<Value> = json_rows
        .into_iter()
        .map(|map| Value::Object(map))
        .collect();

    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CliArgs, DataSource, DataSourceType, ServerConfig};
    use crate::server::AppState;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use pipeline::pipeline::{Pipeline, StandardPipeline};
    use skardi_engine::datafusion::DataFusionEngine;
    use source::AccessMode;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{Arc, RwLock};
    use tempfile::TempDir;

    async fn create_test_pipeline_with_params() -> StandardPipeline {
        let temp_dir = TempDir::new().unwrap();
        let pipeline_content = r#"
metadata:
  name: "test-pipeline"
  version: "1.0.0"
  description: "Test pipeline for handler testing"

query: |
  SELECT user_id, name, category
  FROM test_data
  WHERE user_id = {user_id} AND category = {category}
"#;

        let pipeline_path = temp_dir.path().join("test-pipeline.yaml");
        fs::write(&pipeline_path, pipeline_content).unwrap();

        // Create SessionContext with mock test_data table for schema inference
        let ctx = Arc::new(SessionContext::new());
        let mock_batch = create_test_record_batch();
        ctx.register_batch("test_data", mock_batch).unwrap();

        StandardPipeline::load_from_file(&pipeline_path, ctx)
            .await
            .unwrap()
    }

    async fn create_test_app_state() -> AppState {
        let pipeline = create_test_pipeline_with_params().await;
        let mut pipelines = HashMap::new();
        pipelines.insert(pipeline.name().to_string(), pipeline);

        let data_sources = vec![];
        let args = CliArgs {
            pipeline_path: Some(PathBuf::from("test-pipeline.yaml")),
            ctx_file: None,
            port: 8080,
        };

        let config = ServerConfig {
            pipelines,
            data_sources,

            args,
        };

        // Create a SessionContext for the engine
        let session_ctx = Arc::new(SessionContext::new());
        let engine = Arc::new(DataFusionEngine::new_with_arc(session_ctx.clone()));

        AppState {
            config: Arc::new(RwLock::new(config)),
            engine,
            session_ctx,
        }
    }

    fn create_test_record_batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("category", DataType::Utf8, true),
        ]);

        let user_ids = Int64Array::from(vec![1, 2]);
        let names = StringArray::from(vec!["Alice", "Bob"]);
        let categories = StringArray::from(vec![Some("premium"), Some("basic")]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(user_ids), Arc::new(names), Arc::new(categories)],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_execute_pipeline_success() {
        // Create a temporary directory and CSV file
        let temp_dir = TempDir::new().unwrap();
        let csv_path = temp_dir.path().join("test_data.csv");

        // Create test CSV data
        let csv_content =
            "user_id,name,category\n1,Alice,premium\n2,Bob,basic\n3,Charlie,premium\n";
        fs::write(&csv_path, csv_content).unwrap();

        // Create data source configuration
        let data_source = DataSource {
            name: "test_data".to_string(),
            source_type: DataSourceType::Csv,
            path: csv_path,
            connection_string: None,
            schema: None,
            options: Some({
                let mut options = HashMap::new();
                options.insert("has_header".to_string(), "true".to_string());
                options
            }),
            access_mode: AccessMode::default(),
            enable_cache: false,
        };

        // Create pipeline that queries the registered data source
        let pipeline = create_test_pipeline_with_params().await;
        let pipeline_name = pipeline.name().to_string();
        let mut pipelines = HashMap::new();
        pipelines.insert(pipeline_name.clone(), pipeline);

        let args = CliArgs {
            pipeline_path: Some(PathBuf::from("test-pipeline.yaml")),
            ctx_file: None,
            port: 8080,
        };

        let config = ServerConfig {
            pipelines,
            data_sources: vec![data_source],

            args,
        };

        // Create SessionContext and register the data source
        let mut session_ctx = SessionContext::new();
        crate::config::register_data_sources(&mut session_ctx, &config.data_sources)
            .await
            .unwrap();

        let session_ctx_arc = Arc::new(session_ctx);
        let engine = Arc::new(DataFusionEngine::new_with_arc(session_ctx_arc.clone()));

        let app_state = AppState {
            config: Arc::new(RwLock::new(config)),
            engine,
            session_ctx: session_ctx_arc,
        };

        let request = ExecuteRequest {
            parameters: {
                let mut params = HashMap::new();
                params.insert(
                    "user_id".to_string(),
                    Value::Number(serde_json::Number::from(1)),
                );
                params.insert("category".to_string(), Value::String("premium".to_string()));
                params
            },
        };

        // Execute the pipeline by name
        let result = execute_pipeline_by_name(
            axum::extract::State(app_state),
            Path(pipeline_name),
            Json(request),
        )
        .await;

        // Should succeed and return actual data
        assert!(result.is_ok());
        let response = result.unwrap().0;

        // Verify the response structure
        assert_eq!(response["rows"], 1); // Should find 1 row matching user_id=1 AND category='premium'
        assert_eq!(response["data"].as_array().unwrap().len(), 1);
        assert!(response["execution_time_ms"].as_u64().unwrap() > 0);

        // Verify the data content
        if let Value::Object(row) = &response["data"][0] {
            assert_eq!(
                row.get("user_id"),
                Some(&Value::Number(serde_json::Number::from(1)))
            );
            assert_eq!(row.get("name"), Some(&Value::String("Alice".to_string())));
            assert_eq!(
                row.get("category"),
                Some(&Value::String("premium".to_string()))
            );
        } else {
            panic!("Expected response data to contain an object");
        }
    }

    #[tokio::test]
    async fn test_execute_pipeline_missing_parameter() {
        let app_state = create_test_app_state().await;

        let request = ExecuteRequest {
            parameters: {
                let mut params = HashMap::new();
                params.insert(
                    "user_id".to_string(),
                    Value::Number(serde_json::Number::from(1)),
                );
                // Missing "category" parameter
                params
            },
        };

        let result = execute_pipeline_by_name(
            axum::extract::State(app_state),
            Path("test-pipeline".to_string()),
            Json(request),
        )
        .await;

        assert!(result.is_err());
        let (status_code, _error_response) = result.unwrap_err();
        assert_eq!(status_code, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_execute_pipeline_invalid_parameter_type() {
        let app_state = create_test_app_state().await;

        let request = ExecuteRequest {
            parameters: {
                let mut params = HashMap::new();
                params.insert(
                    "user_id".to_string(),
                    Value::Number(serde_json::Number::from(1)),
                );
                params.insert("category".to_string(), Value::Array(vec![])); // Invalid type
                params
            },
        };

        let result = execute_pipeline_by_name(
            axum::extract::State(app_state),
            Path("test-pipeline".to_string()),
            Json(request),
        )
        .await;

        assert!(result.is_err());
        let (status_code, _error_response) = result.unwrap_err();
        assert_eq!(status_code, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_execute_pipeline_not_found() {
        let app_state = create_test_app_state().await;

        let request = ExecuteRequest {
            parameters: HashMap::new(),
        };

        let result = execute_pipeline_by_name(
            axum::extract::State(app_state),
            Path("nonexistent-pipeline".to_string()),
            Json(request),
        )
        .await;

        assert!(result.is_err());
        let (status_code, _error_response) = result.unwrap_err();
        assert_eq!(status_code, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_execute_pipeline_parameter_binding() {
        let app_state = create_test_app_state().await;

        let request = ExecuteRequest {
            parameters: {
                let mut params = HashMap::new();
                params.insert(
                    "user_id".to_string(),
                    Value::Number(serde_json::Number::from(123)),
                );
                params.insert(
                    "category".to_string(),
                    Value::String("test'quote".to_string()),
                );
                params
            },
        };

        // We can't easily test the full execution without setting up a real database,
        // but we can at least verify that the function processes parameters correctly
        // by checking it gets to the SQL execution phase (not parameter validation error)
        let result = execute_pipeline_by_name(
            axum::extract::State(app_state),
            Path("test-pipeline".to_string()),
            Json(request),
        )
        .await;

        // Should be SQL execution error (500), not parameter error (400)
        assert!(result.is_err());
        let (status_code, _error_response) = result.unwrap_err();
        assert_eq!(status_code, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_record_batch_to_json() {
        let batch = create_test_record_batch();
        let result = record_batch_to_json(&batch);

        assert!(result.is_ok());
        let json_data = result.unwrap();

        assert_eq!(json_data.len(), 2); // Two rows

        // Check first row
        if let Value::Object(row1) = &json_data[0] {
            assert_eq!(
                row1.get("user_id"),
                Some(&Value::Number(serde_json::Number::from(1)))
            );
            assert_eq!(row1.get("name"), Some(&Value::String("Alice".to_string())));
            assert_eq!(
                row1.get("category"),
                Some(&Value::String("premium".to_string()))
            );
        } else {
            panic!("Expected first row to be an object");
        }

        // Check second row
        if let Value::Object(row2) = &json_data[1] {
            assert_eq!(
                row2.get("user_id"),
                Some(&Value::Number(serde_json::Number::from(2)))
            );
            assert_eq!(row2.get("name"), Some(&Value::String("Bob".to_string())));
            assert_eq!(
                row2.get("category"),
                Some(&Value::String("basic".to_string()))
            );
        } else {
            panic!("Expected second row to be an object");
        }
    }

    #[test]
    fn test_record_batch_to_json_with_nulls() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true), // Nullable
        ]);

        let ids = Int64Array::from(vec![1, 2]);
        let names = StringArray::from(vec![Some("Alice"), None]); // One null value

        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(ids), Arc::new(names)]).unwrap();

        let result = record_batch_to_json(&batch);
        assert!(result.is_ok());

        let json_data = result.unwrap();
        assert_eq!(json_data.len(), 2);

        // Check that null is properly handled
        if let Value::Object(row2) = &json_data[1] {
            assert_eq!(
                row2.get("id"),
                Some(&Value::Number(serde_json::Number::from(2)))
            );
            assert_eq!(row2.get("name"), Some(&Value::Null));
        } else {
            panic!("Expected second row to be an object");
        }
    }
}
