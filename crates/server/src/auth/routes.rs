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
use better_auth::{AuthRequest, HttpMethod, SessionOps};
use cookie::Cookie;

use crate::server::AppState;

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
            .expect("empty body with valid status always builds")
    })
}

pub async fn auth_handler(State(state): State<AppState>, req: Request<Body>) -> impl IntoResponse {
    let auth = match state.auth_layer.as_better_auth() {
        Some(a) => a.clone(),
        None => {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(r#"{"error":"auth not enabled"}"#))
                .expect("static response always builds")
                .into_response();
        }
    };

    let auth_req = match to_auth_request(req).await {
        Ok(r) => r,
        Err(e) => {
            let body = serde_json::json!({"error": e.to_string()}).to_string();
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(body))
                .expect("static response always builds")
                .into_response();
        }
    };

    match auth.handle_request(auth_req).await {
        Ok(res) => from_auth_response(res).into_response(),
        Err(e) => {
            let body = serde_json::json!({"error": e.to_string()}).to_string();
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(body))
                .expect("static response always builds")
                .into_response()
        }
    }
}

pub async fn verify_session(
    state: &AppState,
    headers: &axum::http::HeaderMap,
) -> Result<(), Response<Body>> {
    let auth = match state.auth_layer.as_better_auth() {
        Some(a) => a,
        None => return Ok(()),
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
                let c: Cookie<'_> = c;
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
                .expect("static response always builds"));
        }
    };

    let session = auth.database().get_session(&token).await.ok().flatten();

    let valid = session
        .as_ref()
        .map(|s| s.expires_at > chrono::Utc::now() && s.active)
        .unwrap_or(false);

    if valid {
        Ok(())
    } else {
        Err(Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header("Content-Type", "application/json")
            .body(Body::from(r#"{"error":"Invalid or expired session"}"#))
            .expect("static response always builds"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, Method};

    // ─── to_auth_request ────────────────────────────────────────────────

    #[tokio::test]
    async fn to_auth_request_get_basic() {
        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/auth/session")
            .body(Body::empty())
            .unwrap();
        let auth_req = to_auth_request(req).await.unwrap();
        assert_eq!(auth_req.path, "/api/auth/session");
    }

    #[tokio::test]
    async fn to_auth_request_post_with_body() {
        let json_body = r#"{"email":"a@b.com","password":"pass"}"#;
        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/auth/sign-up/email")
            .header("content-type", "application/json")
            .body(Body::from(json_body))
            .unwrap();
        let auth_req = to_auth_request(req).await.unwrap();
        assert_eq!(auth_req.path, "/api/auth/sign-up/email");
        assert!(auth_req.body.is_some());
        assert_eq!(auth_req.body.as_deref().unwrap(), json_body.as_bytes());
    }

    #[tokio::test]
    async fn to_auth_request_empty_body_is_none() {
        let req = Request::builder()
            .method(Method::GET)
            .uri("/test")
            .body(Body::empty())
            .unwrap();
        let auth_req = to_auth_request(req).await.unwrap();
        assert!(auth_req.body.is_none());
    }

    #[tokio::test]
    async fn to_auth_request_query_params() {
        let req = Request::builder()
            .method(Method::GET)
            .uri("/test?foo=bar&baz=qux")
            .body(Body::empty())
            .unwrap();
        let auth_req = to_auth_request(req).await.unwrap();
        assert_eq!(auth_req.query.get("foo").map(String::as_str), Some("bar"));
        assert_eq!(auth_req.query.get("baz").map(String::as_str), Some("qux"));
    }

    #[tokio::test]
    async fn to_auth_request_unsupported_method() {
        let req = Request::builder()
            .method(Method::TRACE)
            .uri("/test")
            .body(Body::empty())
            .unwrap();
        let result = to_auth_request(req).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unsupported HTTP method"));
    }

    #[tokio::test]
    async fn to_auth_request_all_supported_methods() {
        for method in [
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::PATCH,
            Method::OPTIONS,
            Method::HEAD,
        ] {
            let req = Request::builder()
                .method(method.clone())
                .uri("/test")
                .body(Body::empty())
                .unwrap();
            assert!(
                to_auth_request(req).await.is_ok(),
                "method {method} should be supported"
            );
        }
    }

    #[tokio::test]
    async fn to_auth_request_headers_lowercased() {
        let req = Request::builder()
            .method(Method::GET)
            .uri("/test")
            .header("X-Custom-Header", "value123")
            .body(Body::empty())
            .unwrap();
        let auth_req = to_auth_request(req).await.unwrap();
        assert_eq!(
            auth_req.headers.get("x-custom-header").map(String::as_str),
            Some("value123")
        );
    }

    // ─── from_auth_response ─────────────────────────────────────────────

    #[test]
    fn from_auth_response_basic() {
        let auth_res = better_auth::AuthResponse {
            status: 200,
            headers: HashMap::new(),
            body: b"ok".to_vec(),
        };
        let resp = from_auth_response(auth_res);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn from_auth_response_with_headers() {
        let mut headers = HashMap::new();
        headers.insert("x-request-id".to_string(), "abc123".to_string());
        headers.insert("content-type".to_string(), "application/json".to_string());

        let auth_res = better_auth::AuthResponse {
            status: 201,
            headers,
            body: b"{}".to_vec(),
        };

        let resp = from_auth_response(auth_res);
        assert_eq!(resp.status(), StatusCode::CREATED);
        assert_eq!(
            resp.headers()
                .get("x-request-id")
                .unwrap()
                .to_str()
                .unwrap(),
            "abc123"
        );
    }

    #[test]
    fn from_auth_response_invalid_status_falls_back() {
        let auth_res = better_auth::AuthResponse {
            status: 9999,
            headers: HashMap::new(),
            body: vec![],
        };
        let resp = from_auth_response(auth_res);
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    // ─── verify_session ─────────────────────────────────────────────────

    fn make_no_auth_state() -> AppState {
        use crate::auth::layer::AuthLayer;
        use crate::config::{CliArgs, ServerConfig};
        use crate::metrics::PipelineMetrics;
        use crate::server::AppState;
        use datafusion::prelude::SessionContext;
        use skardi::engine::datafusion::DataFusionEngine;
        use std::path::PathBuf;
        use std::sync::{Arc, RwLock};

        let config = ServerConfig {
            pipelines: Default::default(),
            jobs: Default::default(),
            data_sources: vec![],
            args: CliArgs {
                pipeline_path: Some(PathBuf::from("p.yaml")),
                jobs_path: None,
                jobs_db_path: None,
                ctx_file: None,
                port: 8080,
            },
        };
        let session_ctx = Arc::new(SessionContext::new());
        let engine = Arc::new(DataFusionEngine::new_with_arc(session_ctx.clone()));
        AppState {
            config: Arc::new(RwLock::new(config)),
            engine,
            session_ctx,
            metrics: PipelineMetrics::new(),
            auth_layer: AuthLayer::None,
            jobs: None,
        }
    }

    #[tokio::test]
    async fn verify_session_no_auth_always_allows() {
        let state = make_no_auth_state();
        let headers = HeaderMap::new();
        assert!(verify_session(&state, &headers).await.is_ok());
    }

    async fn make_better_auth_state() -> AppState {
        use crate::auth::layer::AuthLayer;
        use crate::config::{CliArgs, ServerConfig};
        use crate::metrics::PipelineMetrics;
        use crate::server::AppState;
        use datafusion::prelude::SessionContext;
        use skardi::engine::datafusion::DataFusionEngine;
        use std::path::PathBuf;
        use std::sync::{Arc, RwLock};

        unsafe {
            std::env::set_var("AUTH_SECRET", "test-secret-that-is-at-least-32-characters!");
            std::env::set_var("AUTH_DB_PATH", ":memory:");
            std::env::remove_var("AUTH_BASE_URL");
        }

        let layer = AuthLayer::build(&crate::auth::mode::AuthMode::BetterAuthDieselSqlite)
            .await
            .unwrap();

        let config = ServerConfig {
            pipelines: Default::default(),
            jobs: Default::default(),
            data_sources: vec![],
            args: CliArgs {
                pipeline_path: Some(PathBuf::from("p.yaml")),
                jobs_path: None,
                jobs_db_path: None,
                ctx_file: None,
                port: 8080,
            },
        };
        let session_ctx = Arc::new(SessionContext::new());
        let engine = Arc::new(DataFusionEngine::new_with_arc(session_ctx.clone()));
        AppState {
            config: Arc::new(RwLock::new(config)),
            engine,
            session_ctx,
            metrics: PipelineMetrics::new(),
            auth_layer: layer,
            jobs: None,
        }
    }

    #[tokio::test]
    async fn verify_session_missing_token_returns_401() {
        let state = make_better_auth_state().await;
        let headers = HeaderMap::new();
        let err = verify_session(&state, &headers).await.unwrap_err();
        assert_eq!(err.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn verify_session_invalid_bearer_returns_401() {
        let state = make_better_auth_state().await;
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer bogus-token".parse().unwrap());
        let err = verify_session(&state, &headers).await.unwrap_err();
        assert_eq!(err.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn verify_session_valid_bearer() {
        use better_auth::{SessionOps, UserOps};

        let state = make_better_auth_state().await;
        let auth = state.auth_layer.as_better_auth().unwrap();

        let user = auth
            .database()
            .create_user(better_auth::types_mod::CreateUser {
                name: Some("test".into()),
                email: Some("t@t.com".into()),
                password: Some("password123".into()),
                ..Default::default()
            })
            .await
            .unwrap();

        let session = auth
            .database()
            .create_session(better_auth::types_mod::CreateSession {
                user_id: user.id.clone(),
                expires_at: chrono::Utc::now() + chrono::Duration::hours(1),
                ip_address: None,
                user_agent: None,
                impersonated_by: None,
                active_organization_id: None,
            })
            .await
            .unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Bearer {}", session.token).parse().unwrap(),
        );
        assert!(verify_session(&state, &headers).await.is_ok());
    }

    #[tokio::test]
    async fn verify_session_cookie_fallback() {
        use better_auth::{SessionOps, UserOps};

        let state = make_better_auth_state().await;
        let auth = state.auth_layer.as_better_auth().unwrap();

        let user = auth
            .database()
            .create_user(better_auth::types_mod::CreateUser {
                name: Some("cookie-user".into()),
                email: Some("cookie@test.com".into()),
                password: Some("password123".into()),
                ..Default::default()
            })
            .await
            .unwrap();

        let session = auth
            .database()
            .create_session(better_auth::types_mod::CreateSession {
                user_id: user.id.clone(),
                expires_at: chrono::Utc::now() + chrono::Duration::hours(1),
                ip_address: None,
                user_agent: None,
                impersonated_by: None,
                active_organization_id: None,
            })
            .await
            .unwrap();

        let cookie_name = &auth.config().session.cookie_name;
        let mut headers = HeaderMap::new();
        headers.insert(
            "cookie",
            format!("{}={}", cookie_name, session.token)
                .parse()
                .unwrap(),
        );
        assert!(verify_session(&state, &headers).await.is_ok());
    }
}
