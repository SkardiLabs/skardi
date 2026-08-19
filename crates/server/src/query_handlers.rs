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
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use skardi::engine::Engine;
use skardi::sources::sql_validator::{SqlValidationError, StatementKind, validate_single_sql};
use std::time::Instant;

use crate::auth::routes::require_session;
use crate::query_audit::{
    MAX_SESSION_ID_CHARS, OTHER_STATEMENT_KIND, QUERY_STATEMENT_KIND, QueryAuditStatus,
    finish_audit,
};
use crate::response::{
    ErrorResponse, create_error_response, create_success_response, record_batch_to_json,
};
use crate::server::AppState;

/// Default row cap applied when the request does not specify `max_rows`.
const DEFAULT_MAX_ROWS: usize = 1000;

/// Metrics label for ad-hoc queries (pipelines record under their own name).
const QUERY_METRICS_LABEL: &str = "query";

/// Upper bound on the `purpose` field length (characters). Callers document
/// intent, not payloads; the cap keeps a runaway string out of the logs.
const MAX_PURPOSE_CHARS: usize = 2000;

// The session-id length cap (`MAX_SESSION_ID_CHARS`) lives in `query_audit`,
// shared with the pipeline endpoint's header check so audited paths can't
// drift apart.

/// Upper bound on the serialized size of the whole `ai_context` object (bytes).
/// The object is free-form beyond its two required fields; the cap keeps a
/// runaway blob out of the logs.
const MAX_AI_CONTEXT_BYTES: usize = 4096;

/// Request structure for ad-hoc query execution
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    /// A single SQL statement to execute
    pub sql: String,
    /// Result row cap; defaults to [`DEFAULT_MAX_ROWS`]. Must be >= 1.
    pub max_rows: Option<usize>,
    /// Optional agent-supplied context describing and grouping this query.
    /// Application/console queries omit it. When present it must be a JSON
    /// object carrying a non-empty `purpose` and `session_id` (see
    /// [`validate_ai_context`]); other keys are free-form. Recorded for
    /// observability; never executed.
    ///
    /// [`deserialize_present`] keeps *absent* distinguishable from an explicit
    /// `"ai_context": null` — a plain `Option<Value>` collapses both to `None`
    /// and would wave the null past validation.
    #[serde(default, deserialize_with = "deserialize_present")]
    pub ai_context: Option<Value>,
}

/// Deserialize a present field into `Some`, including an explicit JSON `null`
/// (which becomes `Some(Value::Null)`). Combined with `#[serde(default)]`, an
/// omitted field is the only thing that yields `None`.
fn deserialize_present<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: Deserializer<'de>,
{
    Value::deserialize(deserializer).map(Some)
}

/// Validate an optional `ai_context`. `Ok(())` when absent or well-formed;
/// `Err(msg)` (a client-safe message) when present but malformed. An explicit
/// `null` counts as present and is rejected like any other non-object.
///
/// Rules, first failure wins: must be a JSON object; serialized size within
/// [`MAX_AI_CONTEXT_BYTES`]; a non-empty string `purpose` within
/// [`MAX_PURPOSE_CHARS`]; a non-empty string `session_id` within
/// [`MAX_SESSION_ID_CHARS`].
fn validate_ai_context(ai_context: Option<&Value>) -> Result<(), String> {
    let Some(value) = ai_context else {
        return Ok(());
    };

    let Some(object) = value.as_object() else {
        return Err("ai_context must be a JSON object".to_string());
    };

    // Serialized length bounds the whole free-form object, required fields
    // included. `to_string` on a serde value cannot fail.
    if value.to_string().len() > MAX_AI_CONTEXT_BYTES {
        return Err(format!(
            "ai_context must serialize to at most {MAX_AI_CONTEXT_BYTES} bytes"
        ));
    }

    validate_context_string(object.get("purpose"), "purpose", MAX_PURPOSE_CHARS)?;
    validate_context_string(object.get("session_id"), "session_id", MAX_SESSION_ID_CHARS)?;

    Ok(())
}

/// Require a context field to be present, a string, non-empty, and within
/// `max_chars`.
fn validate_context_string(
    field: Option<&Value>,
    name: &str,
    max_chars: usize,
) -> Result<(), String> {
    match field.and_then(Value::as_str) {
        None => Err(format!(
            "ai_context.{name} is required and must be a non-empty string"
        )),
        Some("") => Err(format!("ai_context.{name} must not be empty")),
        Some(s) if s.chars().count() > max_chars => Err(format!(
            "ai_context.{name} must be at most {max_chars} characters"
        )),
        Some(_) => Ok(()),
    }
}

// `finish_audit` lives in `query_audit`: its contract (no-op when auditing is
// off; a failed outcome write leaves the row `started` for startup reconcile)
// is the ledger's, not `/query`'s, and it is shared with `pipeline_handlers`.

/// Execute ad-hoc SQL endpoint - POST /query
/// The ledger's `statement_kind` marker for an ad-hoc statement of the given
/// kind.
///
/// Lives here rather than in `skardi-query-audit` because [`StatementKind`]
/// comes from `skardi`, which that crate deliberately does not depend on
/// (#206). The marker *strings* are still owned by the ledger — this only
/// translates the classifier onto them, so the two cannot drift into two
/// vocabularies.
fn adhoc_statement_kind(kind: StatementKind) -> &'static str {
    match kind {
        StatementKind::Query => QUERY_STATEMENT_KIND,
        StatementKind::Other => OTHER_STATEMENT_KIND,
    }
}

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

    // `ai_context` is caller-supplied metadata, not a payload. Enforce its
    // minimum structure and keep it bounded so a runaway blob can't bloat the
    // marker log or the query-log file.
    if let Err(message) = validate_ai_context(request.ai_context.as_ref()) {
        let elapsed_ms = start_time.elapsed().as_millis() as f64;
        app_state.metrics.record_error(
            QUERY_METRICS_LABEL,
            elapsed_ms,
            "parameter_validation_error",
        );

        return Err((
            StatusCode::BAD_REQUEST,
            create_error_response(&message, "parameter_validation_error", None),
        ));
    }

    let statement_kind = match validate_single_sql(&request.sql, &app_state.adhoc_policy) {
        Ok(kind) => kind,
        Err(e) => {
            // The rejection reason is logged, but never the SQL text itself:
            // it may inline literal secrets/PII. Raw SQL goes only to the
            // opt-in `--query-log` file, and only for statements we execute.
            tracing::info!("Rejected ad-hoc query: {}", e);

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
                SqlValidationError::SchemaNotAllowed { schema, table } => {
                    Some(serde_json::json!({ "schema": schema, "table": table }))
                }
                SqlValidationError::StatementNotAllowed { .. }
                | SqlValidationError::ParseError(_) => None,
            };

            return Err((
                StatusCode::BAD_REQUEST,
                create_error_response(&e.to_string(), "sql_validation_error", details),
            ));
        }
    };

    // Audit marker: value-free by design. The raw SQL is *not* logged here —
    // it may inline secrets/PII and this line fans out to any OTLP collector.
    // Only `ai_context` (caller metadata) and non-sensitive fields are recorded.
    tracing::info!(
        max_rows,
        kind = ?statement_kind,
        ai_context = request
            .ai_context
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default(),
        "Executing ad-hoc query"
    );

    // Raw SQL goes only to the opt-in audit ledger, which the operator asked
    // for and which is created owner-only. Committed *before* execution, so a
    // crash mid-query still leaves a record — and a write failure means the
    // statement does not run at all: an audited server must not execute
    // anything it cannot account for.
    let audit_id = match &app_state.query_audit {
        Some(store) => {
            let kind = adhoc_statement_kind(statement_kind);
            match store
                .record_started(&request.sql, request.ai_context.as_ref(), max_rows, kind)
                .await
            {
                Ok(id) => Some(id),
                Err(e) => {
                    tracing::error!("Query audit write failed; refusing to execute: {e}");

                    let elapsed_ms = start_time.elapsed().as_millis() as f64;
                    app_state.metrics.record_error(
                        QUERY_METRICS_LABEL,
                        elapsed_ms,
                        "query_audit_error",
                    );

                    return Err((
                        StatusCode::SERVICE_UNAVAILABLE,
                        create_error_response(
                            "Query auditing is enabled but the audit record could not be \
                             written; the query was not executed",
                            "query_audit_error",
                            None,
                        ),
                    ));
                }
            }
        }
        None => None,
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
            // Log the engine error server-side; do not echo it to the client.
            // DataFusion errors can quote row/column values and internal
            // schema, so the response stays generic. The SQL text is not
            // logged here — it lives only in the opt-in audit ledger.
            tracing::error!("Ad-hoc query execution failed: {}", e);
            finish_audit(
                app_state.query_audit.as_deref(),
                audit_id.as_deref(),
                QueryAuditStatus::Failed,
                None,
                Some(&e.to_string()),
            )
            .await;

            let elapsed_ms = start_time.elapsed().as_millis() as f64;
            app_state.metrics.record_error(
                QUERY_METRICS_LABEL,
                elapsed_ms,
                "query_execution_error",
            );

            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                create_error_response(
                    "SQL query execution failed; see server logs for details",
                    "query_execution_error",
                    None,
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

    // `record_batch_to_json` yields a `Box<dyn Error>`, which is not `Send`.
    // Flatten it to a message up front: holding the box across the audit await
    // below would make the whole handler future non-`Send`.
    let data = match record_batch_to_json(&record_batch).map_err(|e| e.to_string()) {
        Ok(json_data) => json_data,
        Err(e) => {
            let reason = format!("result conversion failed: {e}");
            // Keep the schema/error detail in the server log only; the client
            // response must not echo internal schema back.
            tracing::error!(
                "Failed to convert results to JSON: {} (schema: {:?}, rows: {})",
                e,
                record_batch.schema(),
                record_batch.num_rows()
            );
            finish_audit(
                app_state.query_audit.as_deref(),
                audit_id.as_deref(),
                QueryAuditStatus::Failed,
                Some(record_batch.num_rows()),
                Some(&reason),
            )
            .await;

            let elapsed_ms = start_time.elapsed().as_millis() as f64;
            app_state.metrics.record_error(
                QUERY_METRICS_LABEL,
                elapsed_ms,
                "result_conversion_error",
            );

            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                create_error_response(
                    "Failed to convert query results to JSON; see server logs for details",
                    "result_conversion_error",
                    None,
                ),
            ));
        }
    };

    let execution_time = start_time.elapsed().as_millis() as u64;
    let row_count = record_batch.num_rows();

    finish_audit(
        app_state.query_audit.as_deref(),
        audit_id.as_deref(),
        QueryAuditStatus::Succeeded,
        Some(row_count),
        None,
    )
    .await;

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
