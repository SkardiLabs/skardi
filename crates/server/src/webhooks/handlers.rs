//! The `GET|POST /webhooks/:name` handlers.
//!
//! These routes are deliberately outside the session check every other
//! mutating endpoint performs: the caller is the provider, which carries no
//! Skardi token. Authentication is the provider's own — the handshake token
//! on GET, the body signature on POST — and nothing is read, converted, or
//! written before it has passed.
//!
//! Status codes are the protocol with the provider's retry:
//!
//! | status | meaning | provider retries? |
//! |---|---|---|
//! | 200 | rows committed (possibly zero) | no |
//! | 401 | signature missing or wrong | yes, and it will keep failing — check the app secret |
//! | 403 | handshake token wrong | n/a (subscription setup) |
//! | 404 | no such webhook | yes — check the route name |
//! | 413 | body over `max_body_bytes` | yes |
//! | 400 | verified body is not a JSON envelope | yes |
//! | 500 | conversion or write failed after verification | yes, up to seven days |
//!
//! Nothing is acknowledged early: a 200 means the transaction committed.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use axum::Json;
use axum::body::{Body, to_bytes};
use axum::extract::{Path, Query, State};
use axum::http::{Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use serde_json::{Value, json};
use skardi::jobs::{DestinationMode, JobDestination, SqlDmlDestination};

use super::meta_whatsapp::{self, HandshakeError, SIGNATURE_HEADER, SignatureError, UnwrapError};
use super::registry::CompiledWebhook;
use crate::response::create_error_response;
use crate::server::AppState;

/// Outcome labels recorded on `webhook_requests_total`.
mod outcome {
    pub const OK: &str = "ok";
    pub const HANDSHAKE_OK: &str = "handshake_ok";
    pub const HANDSHAKE_REJECTED: &str = "handshake_rejected";
    pub const SIGNATURE_REJECTED: &str = "signature_rejected";
    pub const PAYLOAD_TOO_LARGE: &str = "payload_too_large";
    pub const INVALID_PAYLOAD: &str = "invalid_payload";
    pub const CONVERSION_ERROR: &str = "conversion_error";
    pub const WRITE_ERROR: &str = "write_error";
}

/// `GET /webhooks/:name` — the provider's subscription handshake.
pub async fn webhook_verify(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let Some(webhook) = state.webhooks.get(&name) else {
        return not_found(&name);
    };
    let started = Instant::now();
    match meta_whatsapp::handshake(&query, webhook.definition.verify_token.expose()) {
        Ok(challenge) => {
            tracing::info!("webhook {name}: subscription handshake accepted");
            state
                .webhook_metrics
                .record_request(&name, outcome::HANDSHAKE_OK, elapsed_ms(started));
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                challenge.to_string(),
            )
                .into_response()
        }
        Err(reason) => {
            let why = match reason {
                HandshakeError::NotASubscribe => "hub.mode is not 'subscribe'",
                HandshakeError::TokenMismatch => "hub.verify_token does not match",
                HandshakeError::MissingChallenge => "hub.challenge is missing",
            };
            tracing::warn!("webhook {name}: subscription handshake rejected: {why}");
            state.webhook_metrics.record_request(
                &name,
                outcome::HANDSHAKE_REJECTED,
                elapsed_ms(started),
            );
            (
                StatusCode::FORBIDDEN,
                create_error_response(why, outcome::HANDSHAKE_REJECTED, None),
            )
                .into_response()
        }
    }
}

/// `POST /webhooks/:name` — one delivery.
pub async fn webhook_receive(
    State(state): State<AppState>,
    Path(name): Path<String>,
    request: Request<Body>,
) -> Response {
    let Some(webhook) = state.webhooks.get(&name) else {
        return not_found(&name);
    };
    let started = Instant::now();
    let (parts, body) = request.into_parts();

    // Read the signature before the body so a mismatch costs nothing more
    // than the bytes already on the wire.
    let signature = parts
        .headers
        .get(SIGNATURE_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    let bytes = match to_bytes(body, webhook.definition.max_body_bytes).await {
        Ok(bytes) => bytes,
        Err(_) => {
            tracing::warn!(
                "webhook {name}: body exceeded max_body_bytes={}",
                webhook.definition.max_body_bytes
            );
            return finish_error(
                &state,
                &name,
                started,
                StatusCode::PAYLOAD_TOO_LARGE,
                outcome::PAYLOAD_TOO_LARGE,
                "request body exceeds the webhook's max_body_bytes",
                None,
            );
        }
    };

    if let Err(reason) = meta_whatsapp::verify_signature(
        signature.as_deref(),
        &bytes,
        webhook.definition.app_secret.expose(),
    ) {
        let why = match reason {
            SignatureError::Missing => "X-Hub-Signature-256 header is missing",
            SignatureError::Malformed => "X-Hub-Signature-256 header is not 'sha256=<hex>'",
            SignatureError::Mismatch => "X-Hub-Signature-256 does not match the body",
        };
        tracing::warn!("webhook {name}: delivery rejected: {why}");
        return finish_error(
            &state,
            &name,
            started,
            StatusCode::UNAUTHORIZED,
            outcome::SIGNATURE_REJECTED,
            why,
            None,
        );
    }

    let envelope: Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(e) => {
            tracing::warn!("webhook {name}: verified body is not JSON: {e}");
            return finish_error(
                &state,
                &name,
                started,
                StatusCode::BAD_REQUEST,
                outcome::INVALID_PAYLOAD,
                "verified body is not valid JSON",
                None,
            );
        }
    };

    match apply_routes(&state, &webhook, &envelope).await {
        Ok(written) => {
            let total: usize = written.iter().map(|w| w.rows).sum();
            tracing::info!(
                "webhook {name}: delivery accepted: {} row(s) across {} route(s) [{}] waba_ids={:?} in {}ms",
                total,
                written.len(),
                written
                    .iter()
                    .map(|w| format!("{}:{}", w.field, w.rows))
                    .collect::<Vec<_>>()
                    .join(","),
                waba_ids(&envelope),
                started.elapsed().as_millis()
            );
            for w in &written {
                state
                    .webhook_metrics
                    .record_rows(&name, &w.field, w.rows as u64);
            }
            state
                .webhook_metrics
                .record_request(&name, outcome::OK, elapsed_ms(started));
            Json(json!({
                "success": true,
                "webhook": name,
                "rows": total,
                "routes": written
                    .iter()
                    .map(|w| json!({"field": w.field, "table": w.table, "rows": w.rows}))
                    .collect::<Vec<_>>(),
            }))
            .into_response()
        }
        Err(RouteFailure::NotAnEnvelope) => {
            tracing::warn!("webhook {name}: verified body is not a webhook envelope");
            finish_error(
                &state,
                &name,
                started,
                StatusCode::BAD_REQUEST,
                outcome::INVALID_PAYLOAD,
                "verified body is JSON but not a webhook envelope (no `entry` array)",
                None,
            )
        }
        Err(RouteFailure::Conversion { route, error }) => {
            tracing::error!(
                "webhook {name}: route {route}: rows could not be mapped to the declared columns: {error}"
            );
            finish_error(
                &state,
                &name,
                started,
                StatusCode::INTERNAL_SERVER_ERROR,
                outcome::CONVERSION_ERROR,
                "rows could not be mapped to the declared columns",
                Some(json!({"route": route, "error": error})),
            )
        }
        Err(RouteFailure::Write {
            route,
            table,
            error,
        }) => {
            tracing::error!("webhook {name}: route {route}: write to {table} failed: {error}");
            finish_error(
                &state,
                &name,
                started,
                StatusCode::INTERNAL_SERVER_ERROR,
                outcome::WRITE_ERROR,
                "rows could not be written to the destination table",
                Some(json!({"route": route, "table": table, "error": error})),
            )
        }
    }
}

struct Written {
    field: String,
    table: String,
    rows: usize,
}

enum RouteFailure {
    NotAnEnvelope,
    Conversion {
        route: String,
        error: String,
    },
    Write {
        route: String,
        table: String,
        error: String,
    },
}

/// Apply every route in order. A later route's failure after an earlier
/// route's commit answers 500; the provider's retry then re-appends the
/// earlier route's rows, which the merge job's upsert collapses. That is
/// the documented trade-off for keeping one transaction per table.
async fn apply_routes(
    state: &AppState,
    webhook: &CompiledWebhook,
    envelope: &Value,
) -> Result<Vec<Written>, RouteFailure> {
    let ingested_at_ms = chrono::Utc::now().timestamp_millis();
    let mut written = Vec::with_capacity(webhook.routes.len());
    for route in &webhook.routes {
        let rows = meta_whatsapp::unwrap_rows(envelope, &route.field, route.rows)
            .map_err(|UnwrapError::NotAnEnvelope| RouteFailure::NotAnEnvelope)?;
        let count = rows.len();
        if count > 0 {
            let batch =
                route
                    .convert(&rows, ingested_at_ms)
                    .map_err(|e| RouteFailure::Conversion {
                        route: route.field.clone(),
                        error: format!("{e:#}"),
                    })?;
            let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                batch.schema(),
                futures::stream::iter(vec![Ok(batch)]),
            ));
            let destination =
                SqlDmlDestination::new(Arc::clone(&state.session_ctx), route.table.clone());
            destination
                .write(stream, DestinationMode::Append)
                .await
                .map_err(|e| RouteFailure::Write {
                    route: route.field.clone(),
                    table: route.table.clone(),
                    error: format!("{e:#}"),
                })?;
        }
        written.push(Written {
            field: route.field.clone(),
            table: route.table.clone(),
            rows: count,
        });
    }
    Ok(written)
}

/// Distinct `entry[].id` values, for the one-line log. Bounded so a batched
/// delivery cannot bloat a log line.
fn waba_ids(envelope: &Value) -> Vec<&str> {
    let mut ids: Vec<&str> = envelope
        .get("entry")
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|e| e.get("id").and_then(Value::as_str))
                .collect()
        })
        .unwrap_or_default();
    ids.sort_unstable();
    ids.dedup();
    ids.truncate(8);
    ids
}

fn not_found(name: &str) -> Response {
    // No metric here: the name is attacker-controlled and would be an
    // unbounded label.
    tracing::debug!("webhook {name}: not configured");
    (
        StatusCode::NOT_FOUND,
        create_error_response(
            &format!("webhook '{name}' is not configured"),
            "webhook_not_found",
            None,
        ),
    )
        .into_response()
}

#[allow(clippy::too_many_arguments)]
fn finish_error(
    state: &AppState,
    name: &str,
    started: Instant,
    status: StatusCode,
    outcome: &'static str,
    message: &str,
    details: Option<Value>,
) -> Response {
    state
        .webhook_metrics
        .record_request(name, outcome, elapsed_ms(started));
    (status, create_error_response(message, outcome, details)).into_response()
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}
