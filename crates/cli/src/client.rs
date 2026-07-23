//! Thin HTTP client for talking to skardi-server: a small wrapper around
//! `reqwest` that attaches Bearer auth when a token is configured and maps
//! non-success responses (and transport failures) into a uniform `ApiError`.

use crate::config::ClientConfig;
use reqwest::{Client, RequestBuilder, StatusCode};
use serde_json::Value;
use std::error::Error as StdError;
use std::fmt;

/// A thin HTTP client for a single skardi-server instance.
///
/// Holds the underlying `reqwest::Client`, the base URL with any trailing
/// `/` stripped (so callers can safely append a `path` starting with `/`),
/// and an optional bearer token attached to every request.
pub struct ApiClient {
    http: Client,
    base_url: String,
    token: Option<String>,
}

/// Uniform error type for all `ApiClient` request failures.
///
/// Converts into `anyhow::Error` via `std::error::Error`, and `main` can
/// `downcast_ref::<ApiError>` on the resulting `anyhow::Error` to pick an
/// exit code.
#[derive(Debug)]
pub enum ApiError {
    /// The request never got a response: connect/DNS/timeout failure, or
    /// the response body could not be read.
    Connect { url: String, message: String },
    /// A non-success HTTP status, or a success status with an unparsable
    /// body.
    Http {
        status: u16,
        error_type: Option<String>,
        message: String,
    },
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiError::Connect { url, message } => write!(
                f,
                "cannot reach skardi-server at {url} ({message}) — check --server, SKARDI_SERVER_URL, or ~/.skardi/config.yaml"
            ),
            ApiError::Http { status, .. } if *status == StatusCode::UNAUTHORIZED.as_u16() => {
                write!(
                    f,
                    "unauthorized — set SKARDI_API_TOKEN or 'token' in ~/.skardi/config.yaml"
                )
            }
            ApiError::Http {
                status,
                error_type: Some(error_type),
                message,
            } => write!(f, "[{error_type}] {message} (HTTP {status})"),
            ApiError::Http {
                status,
                error_type: None,
                message,
            } => write!(f, "server returned HTTP {status}: {message}"),
        }
    }
}

impl StdError for ApiError {}

/// The server's JSON error envelope, e.g.
/// `{ "success": false, "error": "msg", "error_type": "sql_validation_error", ... }`.
///
/// Only the two fields consumed by error mapping are modeled; the rest
/// (`success`, `details`, `timestamp`) are left unmodeled and ignored.
#[derive(serde::Deserialize)]
struct ErrorEnvelope {
    error: String,
    #[serde(default)]
    error_type: Option<String>,
}

impl ApiClient {
    /// Build a client from a resolved `ClientConfig`. Strips any trailing
    /// `/` from the server URL so `path` (which starts with `/`) can be
    /// appended directly without producing a double slash.
    pub fn new(cfg: &ClientConfig) -> anyhow::Result<ApiClient> {
        // The server address is explicit (flag/env/config file), so route
        // to it directly rather than honoring system HTTP_PROXY/HTTPS_PROXY
        // env vars intended for general web browsing.
        let http = Client::builder().no_proxy().build()?;
        let base_url = cfg.server.trim_end_matches('/').to_string();

        Ok(ApiClient {
            http,
            base_url,
            token: cfg.token.clone(),
        })
    }

    /// Issue a GET request against `path` (which must start with `/`) and
    /// return the parsed JSON body, or an `ApiError` on failure.
    pub async fn get(&self, path: &str) -> Result<Value, ApiError> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = self.http.get(&url);
        request = self.with_auth(request);

        self.send(request, url).await
    }

    /// Issue a POST request with a JSON `body` against `path` (which must
    /// start with `/`) and return the parsed JSON body, or an `ApiError` on
    /// failure.
    pub async fn post(&self, path: &str, body: &Value) -> Result<Value, ApiError> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = self.http.post(&url).json(body);
        request = self.with_auth(request);

        self.send(request, url).await
    }

    /// Attach `Authorization: Bearer <token>` when a token is configured;
    /// otherwise leave the request untouched (no auth header at all).
    fn with_auth(&self, request: RequestBuilder) -> RequestBuilder {
        match &self.token {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }

    /// Send `request`, mapping transport failures and non-success/unparsable
    /// responses to `ApiError`. `url` is the request URL, kept for
    /// `ApiError::Connect` messages.
    async fn send(&self, request: RequestBuilder, url: String) -> Result<Value, ApiError> {
        let response = request.send().await.map_err(|err| ApiError::Connect {
            url: url.clone(),
            message: err.to_string(),
        })?;

        let status = response.status();
        let body_text = response.text().await.map_err(|err| ApiError::Connect {
            url: url.clone(),
            message: err.to_string(),
        })?;

        if !status.is_success() {
            return Err(Self::map_error_body(status, &body_text));
        }

        serde_json::from_str(&body_text).map_err(|err| ApiError::Http {
            status: status.as_u16(),
            error_type: None,
            message: format!("failed to parse response body as JSON: {err}"),
        })
    }

    /// Map a non-success response body to `ApiError::Http`: try the
    /// server's JSON error envelope first (a string `error` field, with an
    /// optional `error_type`); fall back to the first line of the raw body
    /// with no `error_type` when the body isn't that envelope.
    fn map_error_body(status: StatusCode, body_text: &str) -> ApiError {
        match serde_json::from_str::<ErrorEnvelope>(body_text) {
            Ok(envelope) => ApiError::Http {
                status: status.as_u16(),
                error_type: envelope.error_type,
                message: envelope.error,
            },
            Err(_) => ApiError::Http {
                status: status.as_u16(),
                error_type: None,
                message: body_text.lines().next().unwrap_or("").to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ApiClient, ApiError};
    use crate::config::ClientConfig;
    use serde_json::json;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn config(server: &str, token: Option<&str>) -> ClientConfig {
        ClientConfig {
            server: server.to_string(),
            token: token.map(|t| t.to_string()),
        }
    }

    #[tokio::test]
    async fn get_returns_parsed_json() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/status"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;

        let client = ApiClient::new(&config(&server.uri(), None)).unwrap();
        let body = client.get("/status").await.unwrap();

        assert_eq!(body, json!({"ok": true}));
    }

    #[tokio::test]
    async fn post_sends_body_and_bearer_token_header() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/query"))
            .and(header("Authorization", "Bearer secret-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"rows": []})))
            .expect(1)
            .mount(&server)
            .await;

        let client = ApiClient::new(&config(&server.uri(), Some("secret-token"))).unwrap();
        let body = client
            .post("/query", &json!({"sql": "select 1"}))
            .await
            .unwrap();

        assert_eq!(body, json!({"rows": []}));
    }

    #[tokio::test]
    async fn no_token_sends_no_authorization_header() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/status"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;

        let client = ApiClient::new(&config(&server.uri(), None)).unwrap();
        client.get("/status").await.unwrap();

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        assert!(!requests[0].headers.contains_key("authorization"));
    }

    #[tokio::test]
    async fn error_envelope_maps_to_http_with_error_type() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/query"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "success": false,
                "error": "column foo does not exist",
                "error_type": "sql_validation_error",
                "details": null,
                "timestamp": "2026-07-23T00:00:00Z",
            })))
            .mount(&server)
            .await;

        let client = ApiClient::new(&config(&server.uri(), None)).unwrap();
        let err = client
            .post("/query", &json!({"sql": "select foo"}))
            .await
            .unwrap_err();

        match &err {
            ApiError::Http {
                status,
                error_type,
                message,
            } => {
                assert_eq!(*status, 400);
                assert_eq!(error_type.as_deref(), Some("sql_validation_error"));
                assert_eq!(message, "column foo does not exist");
            }
            other => panic!("expected Http, got {other:?}"),
        }

        let display = err.to_string();
        assert!(
            display.contains("sql_validation_error"),
            "display was: {display}"
        );
    }

    #[tokio::test]
    async fn plain_text_error_body_maps_to_first_line_message() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/status"))
            .respond_with(
                ResponseTemplate::new(503)
                    .set_body_string("service unavailable\nretry later\nmore detail"),
            )
            .mount(&server)
            .await;

        let client = ApiClient::new(&config(&server.uri(), None)).unwrap();
        let err = client.get("/status").await.unwrap_err();

        match &err {
            ApiError::Http {
                status,
                error_type,
                message,
            } => {
                assert_eq!(*status, 503);
                assert_eq!(*error_type, None);
                assert_eq!(message, "service unavailable");
            }
            other => panic!("expected Http, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unauthorized_display_mentions_skardi_api_token() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/status"))
            .respond_with(ResponseTemplate::new(401).set_body_json(json!({
                "success": false,
                "error": "missing credentials",
                "error_type": "unauthorized",
                "details": null,
                "timestamp": "2026-07-23T00:00:00Z",
            })))
            .mount(&server)
            .await;

        let client = ApiClient::new(&config(&server.uri(), None)).unwrap();
        let err = client.get("/status").await.unwrap_err();

        let display = err.to_string();
        assert!(
            display.contains("SKARDI_API_TOKEN"),
            "display was: {display}"
        );
    }

    #[tokio::test]
    async fn connect_failure_maps_to_connect_error() {
        let client = ApiClient::new(&config("http://127.0.0.1:1", None)).unwrap();
        let err = client.get("/status").await.unwrap_err();

        match &err {
            ApiError::Connect { .. } => {}
            other => panic!("expected Connect, got {other:?}"),
        }

        let display = err.to_string();
        assert!(
            display.contains("cannot reach skardi-server"),
            "display was: {display}"
        );
    }

    #[test]
    fn base_url_strips_trailing_slash() {
        let client = ApiClient::new(&config("http://h:1/", None)).unwrap();
        assert_eq!(client.base_url, "http://h:1");
    }
}
