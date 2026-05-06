//! Server-wide HTTP handlers — endpoints that are neither pipeline- nor
//! job-specific.
//!
//! Currently this module only owns the `/health` liveness probe.
//!
//! * Pipeline endpoints live in [`crate::pipeline_handlers`].
//! * Job endpoints live in [`crate::jobs_handlers`].
//! * The dashboard UI (`GET /`) lives in [`crate::gui`].

use axum::{Json, http::StatusCode};
use serde_json::Value;

/// Health check endpoint - GET /health
pub async fn health_check() -> Result<Json<Value>, StatusCode> {
    let response = serde_json::json!({
        "status": "healthy",
        "service": "skardi-server",
        "timestamp": chrono::Utc::now().to_rfc3339()
    });

    Ok(Json(response))
}
