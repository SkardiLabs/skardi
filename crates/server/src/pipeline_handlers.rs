//! Axum handlers for the pipeline primitive — the synchronous
//! request/response half of the server. Jobs live in `jobs_handlers.rs`;
//! server-wide endpoints (`/health`, `/`) live in `handlers.rs`.
//!
//! Endpoints mounted here:
//!
//! * `GET  /health/:name`   — per-pipeline health (existence + data source accessibility)
//! * `GET  /pipelines`      — list every loaded pipeline with its parameters
//! * `GET  /pipeline/:name` — metadata for one pipeline
//! * `GET  /data_source`    — registered data sources with their schemas
//! * `POST /:name/execute`  — run a pipeline's SELECT with bound parameters

use anyhow::Result;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use skardi::engine::Engine;
use skardi::pipeline::pipeline::Pipeline;
use std::collections::HashMap;
use std::time::Instant;

use crate::auth::routes::require_session;
use crate::config::DataSourceType;
use crate::response::{
    ErrorResponse, create_error_response, create_success_response, record_batch_to_json,
};
use crate::semantics::SemanticsRegistry;
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

/// Field information for table schema
#[derive(Debug, Clone, Serialize)]
pub struct FieldInfo {
    /// Column name
    pub name: String,
    /// Arrow data type as string representation
    pub r#type: String,
    /// Whether the field is nullable
    pub nullable: bool,
    /// Natural-language description sourced from the loaded `kind: semantics`
    /// overlay, if any. Omitted from the JSON response when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Table information with schema
#[derive(Debug, Clone, Serialize)]
pub struct TableInfo {
    /// Table name (same as data source name)
    pub name: String,
    /// Natural-language description for the table (semantics overlay first,
    /// ctx-inline `description` second). Omitted when neither supplies one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
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
pub(crate) async fn get_table_schema(
    ctx: &SessionContext,
    table_name: &str,
    semantics: &SemanticsRegistry,
) -> Result<Vec<FieldInfo>> {
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

    // Convert Arrow fields to FieldInfo, attaching any column-level
    // semantics overlay registered for this (table, column) pair.
    let fields: Vec<FieldInfo> = table_schema
        .fields()
        .iter()
        .map(|field| FieldInfo {
            name: field.name().clone(),
            r#type: format!("{:?}", field.data_type()),
            nullable: field.is_nullable(),
            description: semantics
                .column_description(table_name, field.name())
                .map(str::to_string),
        })
        .collect();

    Ok(fields)
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
    // Acquire lock and extract data sources + semantics, then drop lock
    // before async operations.
    let (data_sources, semantics) = {
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
        (config.data_sources.clone(), config.semantics.clone())
    };

    let session_ctx = app_state.engine.session_context();
    let mut data_source_responses = Vec::new();

    for data_source in &data_sources {
        // Convert source type to lowercase string (single source of truth:
        // `DataSourceType::as_str`, so a new variant needs no change here).
        let source_type_str = data_source.source_type.as_str();

        // Determine path or URL based on source type
        let path = match data_source.source_type {
            DataSourceType::Csv
            | DataSourceType::Parquet
            | DataSourceType::Lance
            | DataSourceType::Sqlite
            | DataSourceType::Iceberg
            | DataSourceType::Documents => Some(data_source.path.to_string_lossy().to_string()),
            DataSourceType::Postgres
            | DataSourceType::Mysql
            | DataSourceType::Mongo
            | DataSourceType::Redis
            | DataSourceType::Seekdb
            | DataSourceType::Influxdb
            | DataSourceType::OpenConnector
            | DataSourceType::Dynamodb => None,
        };

        let url = match data_source.source_type {
            DataSourceType::Postgres
            | DataSourceType::Mysql
            | DataSourceType::Mongo
            | DataSourceType::Redis
            | DataSourceType::Seekdb
            | DataSourceType::Influxdb
            | DataSourceType::OpenConnector
            | DataSourceType::Dynamodb => {
                // For database sources, return the connection string as-is
                // (credentials are not stored in connection strings, only in env vars)
                data_source.connection_string.clone()
            }
            _ => None,
        };

        // Get table schema from SessionContext, with column descriptions
        // merged in from the semantics registry.
        let table_schema = match get_table_schema(session_ctx, &data_source.name, &semantics).await
        {
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

        // Build table info (data source name is the table name). The table
        // description is the merged view: a `kind: semantics` overlay wins
        // when present, falling back to the ctx-inline `description` field
        // (this fallback is seeded into the registry at boot, so the
        // single lookup here covers both cases).
        let tables = vec![TableInfo {
            name: data_source.name.clone(),
            description: semantics
                .table_description(&data_source.name)
                .map(str::to_string),
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

/// Render a single scalar JSON value as the SQL literal form used inside an
/// array placeholder or a row tuple cell.
///
/// Number/Bool/Null render verbatim; String is single-quoted and escaped.
/// Anything else falls back to `Value::to_string()`, matching the previous
/// behaviour for unexpected nested shapes.
fn scalar_to_sql(v: &Value) -> String {
    match v {
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("'{}'", s.replace("'", "''")),
        Value::Bool(b) => b.to_string(),
        Value::Null => "NULL".to_string(),
        other => other.to_string(),
    }
}

/// Render one cell of a row tuple. Scalars use `scalar_to_sql`; a nested
/// array is rendered as the bracketed scalar form `[a, b, c]` so VECTOR /
/// pgvector columns accept the literal as a row-cell value (the same shape
/// `Value::Array` of scalars produces at the top level).
fn row_cell_to_sql(v: &Value) -> String {
    match v {
        Value::Array(inner) => {
            let elements: Vec<String> = inner.iter().map(scalar_to_sql).collect();
            format!("[{}]", elements.join(", "))
        }
        _ => scalar_to_sql(v),
    }
}

/// Substitute `{param}` placeholders in `sql` with their SQL-safe values.
///
/// `expected_params` must be sorted longest-first so that a shorter name (e.g. `user`) cannot
/// corrupt a longer one that shares it as a prefix (e.g. `user_id`).
///
/// Supported JSON parameter shapes:
/// - String / Number / Bool / Null → the obvious literal.
/// - `Array` of scalars → `[a, b, c]` (vector / pgvector text literal form).
/// - `Array` whose every element is itself an `Array` → a multi-row tuple
///   list `(c1, c2, …), (c1, c2, …)` for `INSERT … VALUES {rows}` shapes.
///   Inner arrays inside a tuple cell render as the scalar bracket form
///   so a vector column can sit alongside scalar columns in the same row.
///
/// Mixed-shape arrays (some elements scalar, some array) are rejected as
/// `Unsupported` so a caller can't accidentally emit malformed SQL by
/// passing an array-of-arrays with a stray scalar.
///
/// Tuple lists with empty rows or rows of inconsistent width are also
/// rejected — every row must be non-empty and have the same number of
/// cells, so the caller catches a malformed batch at the renderer rather
/// than as a less-specific arity error from the database.
///
/// A zero-length top-level array (`{"rows": []}` or `{"embedding": []}`)
/// is rejected with a specific "empty array" error rather than the
/// generic "Unsupported parameter type" so CDC consumers that may emit
/// empty batches see a clear instruction to filter client-side.
///
/// Returns `(missing_params, unsupported_params)` — both empty on full success.
fn substitute_sql_params(
    sql: &mut String,
    expected_params: &[String],
    parameters: &HashMap<String, Value>,
) -> (Vec<String>, Vec<String>) {
    let mut missing_params = Vec::new();
    let mut unsupported_params = Vec::new();

    for param_name in expected_params {
        let placeholder = format!("{{{}}}", param_name);

        if let Some(param_value) = parameters.get(param_name) {
            let sql_value = match param_value {
                Value::String(s) => format!("'{}'", s.replace("'", "''")),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Null => "NULL".to_string(),
                Value::Array(arr) if arr.is_empty() => {
                    // CDC consumers commonly produce empty batches; surface a
                    // specific, actionable error (rather than the generic
                    // "Unsupported parameter type") so the caller knows to
                    // filter zero-row batches client-side. There is no SQL
                    // expansion that makes `VALUES` with zero rows valid.
                    tracing::error!(
                        "Empty array for {}: cannot expand into a VALUES tuple \
                         list with zero rows, or into a vector literal of zero \
                         elements. Filter empty batches client-side.",
                        param_name
                    );
                    unsupported_params.push(format!(
                        "{}: empty array — provide at least one row/element, \
                         or filter empty batches client-side",
                        param_name
                    ));
                    continue;
                }
                Value::Array(arr) => {
                    if arr.iter().all(|v| v.is_array()) {
                        // Multi-row tuple list — `(c1, c2, …), (c1, c2, …)`.
                        let row_arrays: Vec<&Vec<Value>> = arr
                            .iter()
                            .map(|row| row.as_array().expect("checked above"))
                            .collect();
                        let width = row_arrays[0].len();
                        if width == 0 || row_arrays.iter().any(|r| r.len() != width) {
                            tracing::error!(
                                "Inconsistent or empty row widths for {}: every \
                                 row in a VALUES tuple list must have the same \
                                 non-zero number of cells.",
                                param_name
                            );
                            unsupported_params.push(format!("{}: {:?}", param_name, param_value));
                            continue;
                        }
                        let rows: Vec<String> = row_arrays
                            .iter()
                            .map(|cells| {
                                let rendered: Vec<String> =
                                    cells.iter().map(row_cell_to_sql).collect();
                                format!("({})", rendered.join(", "))
                            })
                            .collect();
                        rows.join(", ")
                    } else if arr.iter().any(|v| v.is_array()) {
                        tracing::error!(
                            "Mixed-shape array for {} (some elements are arrays, \
                             some are scalars). Pass either an all-scalar array \
                             (vector literal) or an all-array array (VALUES tuple \
                             list).",
                            param_name
                        );
                        unsupported_params.push(format!("{}: {:?}", param_name, param_value));
                        continue;
                    } else {
                        // Flat scalar array — the original vector-literal form.
                        let elements: Vec<String> = arr.iter().map(scalar_to_sql).collect();
                        format!("[{}]", elements.join(", "))
                    }
                }
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
            *sql = sql.replace(&placeholder, &sql_value);
        } else {
            tracing::error!("Missing required parameter: {}", param_name);
            missing_params.push(param_name.clone());
        }
    }

    (missing_params, unsupported_params)
}

/// Execute pipeline endpoint - POST /:name/execute
pub async fn execute_pipeline_by_name(
    State(app_state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(pipeline_name): Path<String>,
    Json(request): Json<ExecuteRequest>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    require_session(&app_state, &headers).await?;

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
        let mut expected_params: Vec<String> = request_schema.fields.keys().cloned().collect();
        // Sort longest-first so a shorter name (e.g. `{user}`) cannot corrupt a longer one
        // (`{user_id}`) during str::replace when both appear in the same SQL template.
        expected_params.sort_by_key(|b| std::cmp::Reverse(b.len()));
        (query_def.sql.clone(), expected_params)
    };

    let mut sql = sql_template;

    let (missing_params, unsupported_params) =
        substitute_sql_params(&mut sql, &expected_params, &request.parameters);

    // Return detailed error for parameter validation issues
    if !missing_params.is_empty() || !unsupported_params.is_empty() {
        let elapsed_ms = start_time.elapsed().as_millis() as f64;
        app_state
            .metrics
            .record_error(&pipeline_name, elapsed_ms, "parameter_validation_error");

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

            let elapsed_ms = start_time.elapsed().as_millis() as f64;
            app_state
                .metrics
                .record_error(&pipeline_name, elapsed_ms, "query_execution_error");

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

            let elapsed_ms = start_time.elapsed().as_millis() as f64;
            app_state
                .metrics
                .record_error(&pipeline_name, elapsed_ms, "result_conversion_error");

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

    app_state
        .metrics
        .record_success(&pipeline_name, execution_time as f64);

    tracing::info!(
        "Query completed successfully: {} rows in {}ms",
        row_count,
        execution_time
    );

    Ok(create_success_response(
        data,
        row_count,
        execution_time,
        None,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CliArgs, DataSource, DataSourceType, ServerConfig};
    use crate::metrics::PipelineMetrics;
    use crate::server::AppState;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use skardi::engine::datafusion::DataFusionEngine;
    use skardi::pipeline::pipeline::{Pipeline, StandardPipeline};
    use skardi::sources::AccessMode;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{Arc, RwLock};
    use tempfile::TempDir;

    async fn create_test_pipeline_with_params() -> StandardPipeline {
        let temp_dir = TempDir::new().unwrap();
        let pipeline_content = r#"
kind: pipeline
metadata:
  name: "test-pipeline"
  version: "1.0.0"
  description: "Test pipeline for handler testing"
spec:
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
            jobs_path: None,
            jobs_db_path: None,
            ctx_file: None,
            semantics_path: None,
            port: 8080,
        };

        let config = ServerConfig {
            pipelines,
            jobs: HashMap::new(),
            data_sources,
            semantics: SemanticsRegistry::default(),
            args,
        };

        // Create a SessionContext for the engine
        let session_ctx = Arc::new(SessionContext::new());
        let engine = Arc::new(DataFusionEngine::new_with_arc(session_ctx.clone()));

        let validator_config = Arc::new(crate::config::validator_config_from_sources(
            &config.data_sources,
        ));
        AppState {
            config: Arc::new(RwLock::new(config)),
            engine,
            session_ctx,
            metrics: PipelineMetrics::new(),
            auth_layer: crate::auth::layer::AuthLayer::None,
            jobs: None,
            validator_config,
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
            hierarchy_level: Default::default(),
            description: None,
            open_connector: None,
        };

        // Create pipeline that queries the registered data source
        let pipeline = create_test_pipeline_with_params().await;
        let pipeline_name = pipeline.name().to_string();
        let mut pipelines = HashMap::new();
        pipelines.insert(pipeline_name.clone(), pipeline);

        let args = CliArgs {
            pipeline_path: Some(PathBuf::from("test-pipeline.yaml")),
            jobs_path: None,
            jobs_db_path: None,
            ctx_file: None,
            semantics_path: None,
            port: 8080,
        };

        let config = ServerConfig {
            pipelines,
            jobs: HashMap::new(),
            data_sources: vec![data_source],
            semantics: SemanticsRegistry::default(),
            args,
        };

        // Create SessionContext and register the data source
        let mut session_ctx = SessionContext::new();
        crate::config::register_data_sources(&mut session_ctx, &config.data_sources)
            .await
            .unwrap();

        let session_ctx_arc = Arc::new(session_ctx);
        let engine = Arc::new(DataFusionEngine::new_with_arc(session_ctx_arc.clone()));

        let validator_config = Arc::new(crate::config::validator_config_from_sources(
            &config.data_sources,
        ));
        let app_state = AppState {
            config: Arc::new(RwLock::new(config)),
            engine,
            session_ctx: session_ctx_arc,
            metrics: PipelineMetrics::new(),
            auth_layer: crate::auth::layer::AuthLayer::None,
            jobs: None,
            validator_config,
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
            axum::http::HeaderMap::new(),
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
            axum::http::HeaderMap::new(),
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
            axum::http::HeaderMap::new(),
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
            axum::http::HeaderMap::new(),
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
            axum::http::HeaderMap::new(),
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

    // -------------------------------------------------------------------------
    // Unit tests for substitute_sql_params
    // -------------------------------------------------------------------------

    fn params(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn sorted_keys(map: &HashMap<String, Value>) -> Vec<String> {
        let mut v: Vec<String> = map.keys().cloned().collect();
        v.sort_by_key(|b| std::cmp::Reverse(b.len()));
        v
    }

    #[test]
    fn test_substitute_prefix_params_brace_notation_prevents_corruption() {
        // The bug report claimed that replacing `{user}` before `{user_id}` would corrupt
        // the longer placeholder. In practice this does NOT happen with `{param}` notation:
        // `{user}` ends with `}`, so it is never a literal substring of `{user_id}` (whose
        // 5th character after `{` is `_`, not `}`). Both orderings produce correct SQL.
        let template = "SELECT * FROM t WHERE id = {user_id} AND name = {user}";
        let params_map = params(&[
            ("user_id", Value::Number(42.into())),
            ("user", Value::String("alice".to_string())),
        ]);

        // Shorter-name-first ordering — would corrupt `$user_id`-style placeholders but not `{user_id}`.
        let mut sql = template.to_string();
        let short_first = vec!["user".to_string(), "user_id".to_string()];
        let (missing, unsupported) = substitute_sql_params(&mut sql, &short_first, &params_map);
        assert!(missing.is_empty());
        assert!(unsupported.is_empty());
        assert_eq!(sql, "SELECT * FROM t WHERE id = 42 AND name = 'alice'");

        // Longer-name-first ordering (the sorted order we enforce) — also correct.
        let mut sql2 = template.to_string();
        let long_first = sorted_keys(&params_map);
        let (missing2, unsupported2) = substitute_sql_params(&mut sql2, &long_first, &params_map);
        assert!(missing2.is_empty());
        assert!(unsupported2.is_empty());
        assert_eq!(sql2, "SELECT * FROM t WHERE id = 42 AND name = 'alice'");
    }

    #[test]
    fn test_substitute_no_prefix_conflict() {
        // Unrelated parameter names — any ordering is safe.
        let mut sql = "SELECT * FROM t WHERE id = {id} AND cat = {category}".to_string();
        let params_map = params(&[
            ("id", Value::Number(7.into())),
            ("category", Value::String("premium".to_string())),
        ]);
        let expected_params = sorted_keys(&params_map);
        let (missing, unsupported) = substitute_sql_params(&mut sql, &expected_params, &params_map);

        assert!(missing.is_empty());
        assert!(unsupported.is_empty());
        assert_eq!(sql, "SELECT * FROM t WHERE id = 7 AND cat = 'premium'");
    }

    #[test]
    fn test_substitute_missing_param() {
        let mut sql = "SELECT {a} AND {b}".to_string();
        let params_map = params(&[("a", Value::Number(1.into()))]);
        let expected_params = vec!["a".to_string(), "b".to_string()];
        let (missing, unsupported) = substitute_sql_params(&mut sql, &expected_params, &params_map);

        assert_eq!(missing, vec!["b"]);
        assert!(unsupported.is_empty());
        assert_eq!(sql, "SELECT 1 AND {b}"); // {b} left untouched
    }

    #[test]
    fn test_substitute_quote_escaping() {
        let mut sql = "SELECT * FROM t WHERE name = {name}".to_string();
        let params_map = params(&[("name", Value::String("o'brien".to_string()))]);
        let expected_params = sorted_keys(&params_map);
        let (missing, unsupported) = substitute_sql_params(&mut sql, &expected_params, &params_map);

        assert!(missing.is_empty());
        assert!(unsupported.is_empty());
        assert_eq!(sql, "SELECT * FROM t WHERE name = 'o''brien'");
    }

    // -------------------------------------------------------------------------
    // Multi-row VALUES tuple list (array-of-arrays) tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_substitute_tuple_list_scalar_only() {
        let mut sql = "INSERT INTO t (a, b) VALUES {rows}".to_string();
        let params_map = params(&[(
            "rows",
            Value::Array(vec![
                Value::Array(vec![
                    Value::Number(1.into()),
                    Value::String("x".to_string()),
                ]),
                Value::Array(vec![
                    Value::Number(2.into()),
                    Value::String("y".to_string()),
                ]),
            ]),
        )]);
        let (missing, unsupported) =
            substitute_sql_params(&mut sql, &sorted_keys(&params_map), &params_map);

        assert!(missing.is_empty());
        assert!(unsupported.is_empty());
        assert_eq!(sql, "INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')");
    }

    #[test]
    fn test_substitute_tuple_list_with_nested_vector() {
        // A row tuple with a nested array cell (the vector column) renders
        // as `(scalar, [v1, v2, v3], scalar)` — VECTOR / pgvector text form
        // sits inside the row exactly as it does at the top level.
        let mut sql = "INSERT INTO docs (id, embedding, title) VALUES {rows}".to_string();
        let params_map = params(&[(
            "rows",
            Value::Array(vec![
                Value::Array(vec![
                    Value::String("a".to_string()),
                    Value::Array(vec![
                        Value::Number(serde_json::Number::from_f64(0.1).unwrap()),
                        Value::Number(serde_json::Number::from_f64(0.2).unwrap()),
                    ]),
                    Value::String("alpha".to_string()),
                ]),
                Value::Array(vec![
                    Value::String("b".to_string()),
                    Value::Array(vec![
                        Value::Number(serde_json::Number::from_f64(0.3).unwrap()),
                        Value::Number(serde_json::Number::from_f64(0.4).unwrap()),
                    ]),
                    Value::String("beta".to_string()),
                ]),
            ]),
        )]);
        let (missing, unsupported) =
            substitute_sql_params(&mut sql, &sorted_keys(&params_map), &params_map);

        assert!(missing.is_empty());
        assert!(unsupported.is_empty());
        assert_eq!(
            sql,
            "INSERT INTO docs (id, embedding, title) VALUES \
             ('a', [0.1, 0.2], 'alpha'), ('b', [0.3, 0.4], 'beta')"
        );
    }

    #[test]
    fn test_substitute_tuple_list_single_row_batch() {
        // Batch size of 1 still goes through the tuple-list path and emits a
        // single `(…)` clause — a multi-row pipeline with `VALUES {rows}`
        // therefore handles edge-of-batch and steady-state uniformly.
        let mut sql = "INSERT INTO t (a, b) VALUES {rows}".to_string();
        let params_map = params(&[(
            "rows",
            Value::Array(vec![Value::Array(vec![
                Value::Number(42.into()),
                Value::String("solo".to_string()),
            ])]),
        )]);
        let (missing, unsupported) =
            substitute_sql_params(&mut sql, &sorted_keys(&params_map), &params_map);

        assert!(missing.is_empty());
        assert!(unsupported.is_empty());
        assert_eq!(sql, "INSERT INTO t (a, b) VALUES (42, 'solo')");
    }

    #[test]
    fn test_substitute_tuple_list_quote_escaping() {
        let mut sql = "INSERT INTO t (a) VALUES {rows}".to_string();
        let params_map = params(&[(
            "rows",
            Value::Array(vec![Value::Array(vec![Value::String(
                "o'brien".to_string(),
            )])]),
        )]);
        let (missing, unsupported) =
            substitute_sql_params(&mut sql, &sorted_keys(&params_map), &params_map);

        assert!(missing.is_empty());
        assert!(unsupported.is_empty());
        assert_eq!(sql, "INSERT INTO t (a) VALUES ('o''brien')");
    }

    #[test]
    fn test_substitute_flat_array_still_renders_as_vector_literal() {
        // Pre-existing behaviour for a flat scalar array (e.g. a single
        // vector parameter) is preserved — the tuple-list path only fires
        // when *every* element is itself an array.
        let mut sql = "SELECT vector_to_text({embedding})".to_string();
        let params_map = params(&[(
            "embedding",
            Value::Array(vec![
                Value::Number(serde_json::Number::from_f64(0.1).unwrap()),
                Value::Number(serde_json::Number::from_f64(0.2).unwrap()),
                Value::Number(serde_json::Number::from_f64(0.3).unwrap()),
            ]),
        )]);
        let (missing, unsupported) =
            substitute_sql_params(&mut sql, &sorted_keys(&params_map), &params_map);

        assert!(missing.is_empty());
        assert!(unsupported.is_empty());
        assert_eq!(sql, "SELECT vector_to_text([0.1, 0.2, 0.3])");
    }

    #[test]
    fn test_substitute_tuple_list_covers_bool_null_and_fallback_cells() {
        // Locks down the three less-common scalar_to_sql arms that the other
        // tuple-list tests don't reach: Bool, Null, and the fallback for an
        // unexpected JSON shape (Object). All three flow through
        // row_cell_to_sql's `_ => scalar_to_sql(v)` branch.
        let mut sql = "INSERT INTO t (active, last_login, raw) VALUES {rows}".to_string();
        let mut obj = serde_json::Map::new();
        obj.insert("k".to_string(), Value::String("v".to_string()));
        let params_map = params(&[(
            "rows",
            Value::Array(vec![Value::Array(vec![
                Value::Bool(true),
                Value::Null,
                Value::Object(obj),
            ])]),
        )]);
        let (missing, unsupported) =
            substitute_sql_params(&mut sql, &sorted_keys(&params_map), &params_map);

        assert!(missing.is_empty());
        assert!(unsupported.is_empty());
        // Bool → `true`, Null → `NULL`, Object falls through to
        // `Value::to_string()` which emits the JSON form `{"k":"v"}`.
        // The Object case isn't useful SQL on most engines, but it's the
        // pre-existing behaviour for "any other JSON shape" — locking it
        // here means a future change has to be deliberate.
        assert_eq!(
            sql,
            "INSERT INTO t (active, last_login, raw) VALUES (true, NULL, {\"k\":\"v\"})"
        );
    }

    #[test]
    fn test_substitute_mixed_array_is_unsupported() {
        // An array with both scalar and array elements is ambiguous (vector
        // literal? broken tuple list?) and must be rejected rather than
        // silently dropped into the scalar-fallback branch.
        let mut sql = "INSERT INTO t (a) VALUES {rows}".to_string();
        let params_map = params(&[(
            "rows",
            Value::Array(vec![
                Value::Array(vec![Value::Number(1.into())]),
                Value::Number(2.into()),
            ]),
        )]);
        let (missing, unsupported) =
            substitute_sql_params(&mut sql, &sorted_keys(&params_map), &params_map);

        assert!(missing.is_empty());
        assert_eq!(unsupported.len(), 1);
        assert!(unsupported[0].starts_with("rows: "));
        // Placeholder left untouched on rejection.
        assert_eq!(sql, "INSERT INTO t (a) VALUES {rows}");
    }

    #[test]
    fn test_substitute_tuple_list_rejects_uneven_row_widths() {
        // Different cell counts per row would render as `(1, 2), (3)` and
        // fail at the database with a generic arity error. Reject at the
        // renderer so the caller sees a precise parameter_validation_error.
        let mut sql = "INSERT INTO t (a, b) VALUES {rows}".to_string();
        let params_map = params(&[(
            "rows",
            Value::Array(vec![
                Value::Array(vec![Value::Number(1.into()), Value::Number(2.into())]),
                Value::Array(vec![Value::Number(3.into())]),
            ]),
        )]);
        let (missing, unsupported) =
            substitute_sql_params(&mut sql, &sorted_keys(&params_map), &params_map);

        assert!(missing.is_empty());
        assert_eq!(unsupported.len(), 1);
        assert!(unsupported[0].starts_with("rows: "));
        assert_eq!(sql, "INSERT INTO t (a, b) VALUES {rows}");
    }

    #[test]
    fn test_substitute_empty_top_level_array_is_rejected_with_specific_error() {
        // `"rows": []` is a common CDC-consumer shape (empty batch). Surface
        // a specific, actionable error rather than the generic "Unsupported
        // parameter type" so callers can fix it without digging through logs.
        let mut sql = "INSERT INTO t (a) VALUES {rows}".to_string();
        let params_map = params(&[("rows", Value::Array(vec![]))]);
        let (missing, unsupported) =
            substitute_sql_params(&mut sql, &sorted_keys(&params_map), &params_map);

        assert!(missing.is_empty());
        assert_eq!(unsupported.len(), 1);
        assert!(
            unsupported[0].contains("empty array"),
            "expected error to mention 'empty array', got: {}",
            unsupported[0]
        );
        assert!(
            unsupported[0].contains("filter empty batches"),
            "expected error to point at client-side filtering, got: {}",
            unsupported[0]
        );
        // Placeholder left untouched on rejection.
        assert_eq!(sql, "INSERT INTO t (a) VALUES {rows}");
    }

    #[test]
    fn test_substitute_tuple_list_rejects_empty_inner_rows() {
        // A row with zero cells would render as `()` — invalid SQL on every
        // engine. Reject so the parameter renderer is the source of truth
        // for "this batch is malformed".
        let mut sql = "INSERT INTO t (a) VALUES {rows}".to_string();
        let params_map = params(&[(
            "rows",
            Value::Array(vec![Value::Array(vec![]), Value::Array(vec![])]),
        )]);
        let (missing, unsupported) =
            substitute_sql_params(&mut sql, &sorted_keys(&params_map), &params_map);

        assert!(missing.is_empty());
        assert_eq!(unsupported.len(), 1);
        assert!(unsupported[0].starts_with("rows: "));
        assert_eq!(sql, "INSERT INTO t (a) VALUES {rows}");
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

    #[tokio::test]
    async fn get_data_sources_reports_dynamodb_as_url_source() {
        let app_state = create_test_app_state().await;
        {
            let mut config = app_state.config.write().unwrap();
            config.data_sources.push(DataSource {
                name: "products".to_string(),
                source_type: DataSourceType::Dynamodb,
                path: PathBuf::new(),
                connection_string: Some("http://localhost:8000".to_string()),
                schema: None,
                options: None,
                hierarchy_level: Default::default(),
                access_mode: AccessMode::ReadWrite,
                enable_cache: false,
                description: None,
                open_connector: None,
            });
        }

        let Json(body) = get_data_sources(State(app_state)).await.unwrap();
        let data = body["data"].as_array().unwrap();
        let entry = data
            .iter()
            .find(|d| d["name"] == "products")
            .expect("dynamodb source should be listed");
        assert_eq!(entry["type"], "dynamodb");
        assert!(entry["path"].is_null(), "db sources expose no path");
        assert_eq!(entry["url"], "http://localhost:8000");
    }
}
