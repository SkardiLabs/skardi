//! Thin HTTP client for talking to skardi-server: a small wrapper around
//! `reqwest` that attaches Bearer auth when a token is configured and maps
//! non-success responses (and transport failures) into a uniform `ApiError`.

use crate::config::ClientConfig;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::{Client, RequestBuilder, Response, StatusCode};
use serde_json::Value;
use std::error::Error as StdError;
use std::fmt;
use std::net::Ipv4Addr;

/// Characters percent-encoded by [`encode_component`]: everything except
/// ASCII alphanumerics and the RFC 3986 "unreserved" marks (`-`, `.`, `_`,
/// `~`). Deliberately conservative — over-encoding is always valid, while
/// missing a reserved character (`/`, `?`, `#`, `%`, space, …) mis-routes
/// the request.
const URL_COMPONENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Ceiling on buffered response-body size. The server's `max_rows` cap
/// bounds honest responses far below this; the client-side cap is
/// defense-in-depth so a runaway or misconfigured endpoint fails cleanly
/// instead of exhausting memory.
const MAX_RESPONSE_BYTES: usize = 256 * 1024 * 1024;

/// Percent-encode one URL path segment or query value (user-supplied
/// pipeline/job names, run ids) so characters like `/`, `?`, `#`, `%`, and
/// spaces cannot alter the request route.
pub fn encode_component(raw: &str) -> String {
    utf8_percent_encode(raw, URL_COMPONENT).to_string()
}

/// A thin HTTP client for a single skardi-server instance.
///
/// Holds the underlying `reqwest::Client`, the base URL with any trailing
/// `/` stripped (so callers can safely append a `path` starting with `/`),
/// and an optional bearer token attached to every request.
pub struct ApiClient {
    http: Client,
    base_url: String,
    token: Option<String>,
    max_response_bytes: usize,
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

        if cfg.token.is_some() && is_cleartext_remote(&base_url) {
            eprintln!(
                "warning: bearer token will be sent over cleartext http to a non-loopback host — prefer an https:// server URL"
            );
        }

        Ok(ApiClient {
            http,
            base_url,
            token: cfg.token.clone(),
            max_response_bytes: MAX_RESPONSE_BYTES,
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

    /// Shrink the response-body cap so tests can exercise the limit without
    /// buffering hundreds of megabytes.
    #[cfg(test)]
    fn with_max_response_bytes(mut self, cap: usize) -> Self {
        self.max_response_bytes = cap;
        self
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
        let body_bytes = Self::read_body_capped(response, self.max_response_bytes, &url).await?;
        let body_text = String::from_utf8_lossy(&body_bytes);

        if !status.is_success() {
            return Err(Self::map_error_body(status, &body_text));
        }

        serde_json::from_str(&body_text).map_err(|err| ApiError::Http {
            status: status.as_u16(),
            error_type: None,
            message: format!("failed to parse response body as JSON: {err}"),
        })
    }

    /// Read the whole response body, failing once it exceeds `cap` bytes.
    /// Streaming chunk-by-chunk (instead of `Response::text`) enforces the
    /// cap even on chunked responses that carry no `Content-Length`.
    async fn read_body_capped(
        mut response: Response,
        cap: usize,
        url: &str,
    ) -> Result<Vec<u8>, ApiError> {
        let status = response.status();
        let mut buf: Vec<u8> = Vec::new();
        loop {
            let chunk = response.chunk().await.map_err(|err| ApiError::Connect {
                url: url.to_string(),
                message: err.to_string(),
            })?;
            let Some(chunk) = chunk else {
                return Ok(buf);
            };
            if buf.len().saturating_add(chunk.len()) > cap {
                return Err(ApiError::Http {
                    status: status.as_u16(),
                    error_type: None,
                    message: format!(
                        "response body exceeded the client cap of {cap} bytes — refusing to buffer it"
                    ),
                });
            }
            buf.extend_from_slice(&chunk);
        }
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

/// True when `base_url` is plain `http://` to a host other than loopback
/// (`localhost`, `127.0.0.0/8`, or `::1`) — the case where a configured
/// bearer token would travel in cleartext across a real network.
fn is_cleartext_remote(base_url: &str) -> bool {
    let Some(rest) = base_url.strip_prefix("http://") else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host_port = authority.rsplit('@').next().unwrap_or(authority);
    let host = if let Some(v6) = host_port.strip_prefix('[') {
        v6.split(']').next().unwrap_or("")
    } else {
        host_port.split(':').next().unwrap_or("")
    };

    !(host.eq_ignore_ascii_case("localhost")
        || host == "::1"
        || host.parse::<Ipv4Addr>().is_ok_and(|ip| ip.is_loopback()))
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

    #[test]
    fn encode_component_escapes_reserved_and_keeps_unreserved() {
        use super::encode_component;

        assert_eq!(encode_component("a/b"), "a%2Fb");
        assert_eq!(encode_component("a b?c#d%e"), "a%20b%3Fc%23d%25e");
        assert_eq!(encode_component("a&b=c"), "a%26b%3Dc");
        // Unreserved characters pass through untouched.
        assert_eq!(
            encode_component("daily-report_v2.1~x"),
            "daily-report_v2.1~x"
        );
        // Non-ASCII is UTF-8 percent-encoded.
        assert_eq!(encode_component("café"), "caf%C3%A9");
    }

    #[test]
    fn cleartext_remote_detection() {
        use super::is_cleartext_remote;

        // Loopback and https are fine.
        assert!(!is_cleartext_remote("http://127.0.0.1:8080"));
        assert!(!is_cleartext_remote("http://127.9.9.9"));
        assert!(!is_cleartext_remote("http://localhost:8080"));
        assert!(!is_cleartext_remote("http://LOCALHOST"));
        assert!(!is_cleartext_remote("http://[::1]:8080"));
        assert!(!is_cleartext_remote("https://example.com"));
        // Plain http to a real host is not.
        assert!(is_cleartext_remote("http://10.0.0.5:8080"));
        assert!(is_cleartext_remote("http://example.com/api"));
        assert!(is_cleartext_remote("http://user@example.com"));
    }

    #[tokio::test]
    async fn oversized_response_body_is_rejected() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/status"))
            .respond_with(ResponseTemplate::new(200).set_body_string("x".repeat(4096)))
            .mount(&server)
            .await;

        let client = ApiClient::new(&config(&server.uri(), None))
            .unwrap()
            .with_max_response_bytes(1024);
        let err = client.get("/status").await.unwrap_err();

        match &err {
            ApiError::Http {
                status, message, ..
            } => {
                assert_eq!(*status, 200);
                assert!(message.contains("exceeded the client cap"), "{message}");
            }
            other => panic!("expected Http cap error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn small_response_passes_under_cap() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/status"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;

        let client = ApiClient::new(&config(&server.uri(), None))
            .unwrap()
            .with_max_response_bytes(1024);
        let body = client.get("/status").await.unwrap();
        assert_eq!(body["ok"], true);
    }
}
