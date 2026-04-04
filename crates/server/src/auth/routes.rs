//! Axum handlers that bridge HTTP requests to BetterAuth's framework-agnostic
//! `handle_request` API.
//!
//! All auth traffic arriving at `/api/auth/*` is forwarded here.  The
//! response (status, headers, body) is reconstructed as a native Axum
//! `Response` before being returned to the caller.

use std::collections::HashMap;

use axum::{
    body::Body,
    extract::State,
    http::{HeaderName, HeaderValue, Request, Response, StatusCode},
    response::IntoResponse,
};
use better_auth::{AuthRequest, AuthSession, HttpMethod, SessionOps};
use cookie::Cookie;

use crate::server::AppState;

/// Convert an Axum 0.7 `Request<Body>` into a better-auth `AuthRequest`.
///
/// The full request body is buffered (capped at 4 MiB) so better-auth can
/// parse JSON payloads in its plugin handlers.
async fn to_auth_request(req: Request<Body>) -> Result<AuthRequest, String> {
    let method = match req.method().as_str() {
        "GET" => HttpMethod::Get,
        "POST" => HttpMethod::Post,
        "PUT" => HttpMethod::Put,
        "DELETE" => HttpMethod::Delete,
        "PATCH" => HttpMethod::Patch,
        "OPTIONS" => HttpMethod::Options,
        "HEAD" => HttpMethod::Head,
        other => return Err(format!("Unsupported HTTP method: {}", other)),
    };

    let path = req.uri().path().to_string();

    let headers: HashMap<String, String> = req
        .headers()
        .iter()
        .filter_map(|(k, v)| {
            v.to_str()
                .ok()
                .map(|v_str| (k.as_str().to_lowercase(), v_str.to_string()))
        })
        .collect();

    let query: HashMap<String, String> = req
        .uri()
        .query()
        .map(|q| {
            url::form_urlencoded::parse(q.as_bytes())
                .into_owned()
                .collect()
        })
        .unwrap_or_default();

    // Buffer the body (limit: 4 MiB).
    let body_bytes = axum::body::to_bytes(req.into_body(), 4 * 1024 * 1024)
        .await
        .map_err(|e| format!("Failed to read request body: {}", e))?;
    let body = if body_bytes.is_empty() {
        None
    } else {
        Some(body_bytes.to_vec())
    };

    Ok(AuthRequest::from_parts(method, path, headers, body, query))
}

/// Convert a better-auth `AuthResponse` into an Axum `Response<Body>`.
fn from_auth_response(auth_res: better_auth::AuthResponse) -> Response<Body> {
    let status = StatusCode::from_u16(auth_res.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

    let mut builder = Response::builder().status(status);

    for (key, value) in &auth_res.headers {
        if let (Ok(name), Ok(val)) = (
            HeaderName::from_bytes(key.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            builder = builder.header(name, val);
        }
    }

    builder.body(Body::from(auth_res.body)).unwrap_or_else(|_| {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::empty())
            .unwrap()
    })
}

/// Catch-all handler mounted at `/api/auth/*path`.
///
/// Forwards every method and path to `BetterAuth::handle_request`, then
/// converts the response back into Axum's native response type.
pub async fn auth_handler(State(state): State<AppState>, req: Request<Body>) -> impl IntoResponse {
    let auth = match state.auth_layer.as_better_auth() {
        Some(a) => a.clone(),
        None => {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(r#"{"error":"auth not enabled"}"#))
                .unwrap()
                .into_response()
        }
    };

    let auth_req = match to_auth_request(req).await {
        Ok(r) => r,
        Err(e) => {
            let body = format!(r#"{{"error":"{}"}}"#, e);
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(body))
                .unwrap()
                .into_response();
        }
    };

    match auth.handle_request(auth_req).await {
        Ok(res) => from_auth_response(res).into_response(),
        Err(e) => {
            let body = format!(r#"{{"error":"{}"}}"#, e);
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(body))
                .unwrap()
                .into_response()
        }
    }
}

/// Verify that an incoming pipeline request carries a valid, non-expired
/// session token.
///
/// Accepts the token either as `Authorization: Bearer <token>` or as the
/// configured session cookie (parsed via BetterAuth's session manager).
/// Returns `Ok(())` if the session is valid, or an `(StatusCode, Body)` error
/// response that callers can return directly.
pub async fn verify_session(
    state: &AppState,
    headers: &axum::http::HeaderMap,
) -> Result<(), Response<Body>> {
    let auth = match state.auth_layer.as_better_auth() {
        Some(a) => a,
        None => return Ok(()), // auth disabled — always allow
    };

    // Extract Bearer token from Authorization header.
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|s| s.to_string())
        // Fall back to session cookie if no Authorization header.
        .or_else(|| {
            let cookie_header = headers.get("cookie").and_then(|v| v.to_str().ok())?;
            let cookie_name = &auth.config().session.cookie_name;
            for c in Cookie::split_parse(cookie_header).flatten() {
                if c.name() == cookie_name && !c.value().is_empty() {
                    return Some(c.value().to_string());
                }
            }
            None
        });

    let token = match token {
        Some(t) => t,
        None => {
            return Err(Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"error":"Authentication required"}"#))
                .unwrap())
        }
    };

    let session = auth.database().get_session(&token).await.ok().flatten();

    let valid = session
        .as_ref()
        .map(|s| s.expires_at() > chrono::Utc::now())
        .unwrap_or(false);

    if valid {
        Ok(())
    } else {
        Err(Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header("Content-Type", "application/json")
            .body(Body::from(r#"{"error":"Invalid or expired session"}"#))
            .unwrap())
    }
}
