//! Axum handler for the ad-hoc SQL endpoint.
//!
//! Endpoint mounted here:
//!
//! * `POST /query` — execute one SQL statement against the data sources
//!   registered from the ctx file. DDL and COPY are always rejected; DML is
//!   allowed only against sources configured with `access_mode: read_write`.
//!   Query results are capped at `max_rows` (default 1000) and the response
//!   carries a `truncated` flag.

use axum::{Json, extract::State, http::StatusCode};
use serde::Deserialize;
use serde_json::Value;
use skardi::engine::Engine;
use skardi::sources::sql_validator::{SqlValidationError, StatementKind, validate_single_sql};
use std::time::Instant;

use crate::auth::routes::require_session;
use crate::config::validator_config_from_sources;
use crate::response::{
    ErrorResponse, create_error_response, create_success_response, record_batch_to_json,
};
use crate::server::AppState;

/// Default row cap applied when the request does not specify `max_rows`.
const DEFAULT_MAX_ROWS: usize = 1000;

/// Metrics label for ad-hoc queries (pipelines record under their own name).
const QUERY_METRICS_LABEL: &str = "query";

/// Request structure for ad-hoc query execution
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    /// A single SQL statement to execute
    pub sql: String,
    /// Result row cap; defaults to [`DEFAULT_MAX_ROWS`]. Must be >= 1.
    pub max_rows: Option<usize>,
}

/// Execute ad-hoc SQL endpoint - POST /query
pub async fn execute_query(
    State(app_state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(request): Json<QueryRequest>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    require_session(&app_state, &headers).await?;

    let start_time = Instant::now();

    let max_rows = match request.max_rows {
        Some(0) => {
            let elapsed_ms = start_time.elapsed().as_millis() as f64;
            app_state.metrics.record_error(
                QUERY_METRICS_LABEL,
                elapsed_ms,
                "parameter_validation_error",
            );

            return Err((
                StatusCode::BAD_REQUEST,
                create_error_response(
                    "max_rows must be a positive integer",
                    "parameter_validation_error",
                    None,
                ),
            ));
        }
        Some(n) => n,
        None => DEFAULT_MAX_ROWS,
    };

    // Build the validator config from the current data sources on every
    // request so runtime config updates are respected.
    let validator_config = {
        let config = app_state
            .config
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        validator_config_from_sources(&config.data_sources)
    };

    let statement_kind = match validate_single_sql(&request.sql, &validator_config) {
        Ok(kind) => kind,
        Err(e) => {
            tracing::info!("Rejected ad-hoc query: {}", e);
            tracing::debug!("Rejected SQL: {}", request.sql);

            let elapsed_ms = start_time.elapsed().as_millis() as f64;
            app_state
                .metrics
                .record_error(QUERY_METRICS_LABEL, elapsed_ms, "sql_validation_error");

            let details = match &e {
                SqlValidationError::DdlNotAllowed { operation } => {
                    Some(serde_json::json!({ "operation": operation }))
                }
                SqlValidationError::WriteNotAllowed { operation, table } => {
                    Some(serde_json::json!({ "operation": operation, "table": table }))
                }
                SqlValidationError::NotExactlyOneStatement { count } => {
                    Some(serde_json::json!({ "statement_count": count }))
                }
                SqlValidationError::CopyNotAllowed
                | SqlValidationError::StatementNotAllowed { .. }
                | SqlValidationError::ParseError(_) => None,
            };

            return Err((
                StatusCode::BAD_REQUEST,
                create_error_response(&e.to_string(), "sql_validation_error", details),
            ));
        }
    };

    // Queries get the row cap pushed into the plan (fetch cap + 1 so
    // truncation is detectable). Writes and other statements return small
    // result batches (e.g. an insert count) and run uncapped.
    let result = match statement_kind {
        StatementKind::Query => {
            // DataFusion's LIMIT plan stores `fetch` as an `i64` literal
            // internally, so a `usize` fetch that doesn't fit in `i64`
            // (reachable via a client-supplied `max_rows` near `usize::MAX`)
            // round-trips to a negative literal and fails query planning.
            // `saturating_add` avoids the arithmetic overflow from `+ 1`;
            // the `min` additionally clamps to `i64::MAX`, a cap far beyond
            // any result set this endpoint could ever materialize, so no
            // real request is affected.
            let fetch = max_rows.saturating_add(1).min(i64::MAX as usize);
            app_state
                .engine
                .execute_with_limit(&request.sql, fetch)
                .await
        }
        StatementKind::Other => app_state.engine.execute(&request.sql).await,
    };

    let record_batch = match result {
        Ok(batch) => batch,
        Err(e) => {
            tracing::error!("Ad-hoc query execution failed: {}", e);
            tracing::debug!("Failed SQL query: {}", request.sql);

            let elapsed_ms = start_time.elapsed().as_millis() as f64;
            app_state.metrics.record_error(
                QUERY_METRICS_LABEL,
                elapsed_ms,
                "query_execution_error",
            );

            let error_details = serde_json::json!({
                "engine_error": e.to_string(),
                "registered_tables": "Check server logs for data source registration status",
                "suggestion": "Verify that data sources are properly registered and accessible"
            });

            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                create_error_response(
                    &format!("SQL query execution failed: {}", e),
                    "query_execution_error",
                    Some(error_details),
                ),
            ));
        }
    };

    let truncated = statement_kind == StatementKind::Query && record_batch.num_rows() > max_rows;
    let record_batch = if truncated {
        record_batch.slice(0, max_rows)
    } else {
        record_batch
    };

    let data = match record_batch_to_json(&record_batch) {
        Ok(json_data) => json_data,
        Err(e) => {
            tracing::error!("Failed to convert results to JSON: {}", e);

            let elapsed_ms = start_time.elapsed().as_millis() as f64;
            app_state.metrics.record_error(
                QUERY_METRICS_LABEL,
                elapsed_ms,
                "result_conversion_error",
            );

            let error_details = serde_json::json!({
                "conversion_error": e.to_string(),
                "record_batch_schema": format!("{:?}", record_batch.schema()),
                "record_batch_rows": record_batch.num_rows()
            });

            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                create_error_response(
                    &format!("Failed to convert query results to JSON: {}", e),
                    "result_conversion_error",
                    Some(error_details),
                ),
            ));
        }
    };

    let execution_time = start_time.elapsed().as_millis() as u64;
    let row_count = record_batch.num_rows();

    app_state
        .metrics
        .record_success(QUERY_METRICS_LABEL, execution_time as f64);

    tracing::info!(
        "Ad-hoc query completed: {} rows in {}ms (truncated: {})",
        row_count,
        execution_time,
        truncated
    );

    Ok(create_success_response(
        data,
        row_count,
        execution_time,
        Some(truncated),
    ))
}
