//! Shared HTTP response helpers used by the pipeline and query handlers:
//! success/error envelopes and Arrow → JSON conversion.

use arrow::record_batch::RecordBatch;
use arrow_json::{WriterBuilder, writer::JsonArray};
use axum::Json;
use serde::Serialize;
use serde_json::{Map, Value};

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

/// Helper function to create error responses
pub(crate) fn create_error_response(
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

/// Helper function to create success response with data.
///
/// `truncated: None` omits the field (pipeline responses are unchanged);
/// `Some(_)` includes it (the ad-hoc query endpoint reports row-cap hits).
pub(crate) fn create_success_response(
    data: Vec<Value>,
    rows: usize,
    execution_time_ms: u64,
    truncated: Option<bool>,
) -> Json<Value> {
    let mut body = serde_json::json!({
        "success": true,
        "data": data,
        "rows": rows,
        "execution_time_ms": execution_time_ms,
        "timestamp": chrono::Utc::now().to_rfc3339()
    });
    if let Some(truncated) = truncated {
        body["truncated"] = Value::Bool(truncated);
    }
    Json(body)
}

/// Convert Arrow RecordBatch to JSON array using arrow_json
pub(crate) fn record_batch_to_json(
    batch: &RecordBatch,
) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    // Write the record batch to JSON using arrow_json with null value inclusion
    let buf = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true) // Include null values in JSON output
        .build::<_, JsonArray>(buf);
    writer.write_batches(&[batch])?;
    writer.finish()?;
    let json_data = writer.into_inner();

    // Parse the JSON array string into serde_json::Value objects
    let json_rows: Vec<Map<String, Value>> = serde_json::from_reader(json_data.as_slice())?;

    // Convert Map objects to Value objects
    let values: Vec<Value> = json_rows.into_iter().map(Value::Object).collect();

    Ok(values)
}
