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
use better_auth::{AuthRequest, HttpMethod};

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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Method;

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
}
