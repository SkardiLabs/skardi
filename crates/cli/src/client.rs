//! Thin HTTP client for talking to skardi-server: a small wrapper around
//! `reqwest` that attaches Bearer auth when a token is configured, names the
//! workspace when a cloud context selected one, and maps non-success
//! responses (and transport failures) into a uniform `ApiError`.

use crate::config::ClientConfig;
use reqwest::header::{AUTHORIZATION, HeaderName, HeaderValue};
use reqwest::{Client, RequestBuilder, Response, StatusCode};
use serde_json::Value;
use std::error::Error as StdError;
use std::fmt;
use std::net::{Ipv4Addr, Ipv6Addr};

// The pipeline-name → URL path encoding is part of the tool→REST translation
// contract shared with the server's /mcp binding; the CLI re-exports it so
// command modules keep importing from crate::client.
pub use skardi_mcp_core::encode_component;

/// [`WORKSPACE_HEADER`] lowercased, for `HeaderName::from_static` — which
/// panics on any uppercase byte, and is the only constructor that cannot fail
/// at runtime.
const WORKSPACE_HEADER_LOWER: &str = "skardi-workspace";

/// The workspace selector a `mode: cloud` context sends on every request
/// (§7.3). Deliberately outside the reserved `x-skardi-*` prefix, which the
/// gateway strips from client-supplied headers before forwarding upstream.
pub const WORKSPACE_HEADER: &str = "Skardi-Workspace";

/// Ceiling on buffered response-body size. The server's `max_rows` cap
/// bounds honest responses far below this; the client-side cap is
/// defense-in-depth so a runaway or misconfigured endpoint fails cleanly
/// instead of exhausting memory.
const MAX_RESPONSE_BYTES: usize = 256 * 1024 * 1024;

/// Parse `Retry-After` in its delta-seconds form.
///
/// The header's other legal form is an HTTP-date, which is deliberately NOT
/// parsed: rendering it as "retry in Ns" needs a clock and a date parser to
/// produce a number the caller could read off the header themselves, and the
/// route that emits it (§7.4.2's schema-read limiter) sends delta-seconds.
/// An unparsable value reads as absent, so the caller falls back to the plain
/// error rather than printing a guess.
fn retry_after_seconds(response: &Response) -> Option<u64> {
    response
        .headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// A thin HTTP client for a single skardi-server instance.
///
/// Holds the underlying `reqwest::Client`, the base URL with any trailing
/// `/` stripped (so callers can safely append a `path` starting with `/`),
/// and an optional bearer token attached to every request.
pub struct ApiClient {
    http: Client,
    base_url: String,
    /// `Authorization: Bearer <token>`, pre-built so an unsendable token fails
    /// at construction with one clear message instead of at every request.
    /// Marked sensitive, as `RequestBuilder::bearer_auth` does, so it stays
    /// out of reqwest's own debug output.
    auth: Option<HeaderValue>,
    /// The `Skardi-Workspace` value, set only for a cloud context. See
    /// [`ClientConfig::workspace`] for why it is not an `x-skardi-*` header.
    workspace: Option<HeaderValue>,
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
        /// `Retry-After` in whole seconds, when the response carried one in
        /// delta-seconds form. Kept because §8 turns a `503` WITH this value
        /// into a load message and one without it into a plain failure — the
        /// distinction is the header, so it has to survive this far.
        retry_after: Option<u64>,
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
                ..
            } => write!(f, "[{error_type}] {message} (HTTP {status})"),
            ApiError::Http {
                status,
                error_type: None,
                message,
                ..
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

        // Built here, not per request: `HeaderValue` conversion is the only
        // way a config value can be unsendable, and the caller deserves that
        // as a config error rather than as a request failure. Neither message
        // includes the value — `InvalidHeaderValue` does not carry it.
        let auth = match &cfg.token {
            Some(token) => {
                let mut value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
                    anyhow::anyhow!(
                        "the configured token cannot be sent in an HTTP header (it contains a control character or a non-ASCII byte)"
                    )
                })?;
                value.set_sensitive(true);
                Some(value)
            }
            None => None,
        };
        let workspace = match cfg.workspace() {
            Some(workspace) => Some(HeaderValue::from_str(workspace).map_err(|_| {
                anyhow::anyhow!(
                    "context workspace '{workspace}' cannot be sent in the {WORKSPACE_HEADER} header"
                )
            })?),
            None => None,
        };

        if cfg.token.is_some() && is_cleartext_remote(&base_url) {
            eprintln!(
                "warning: bearer token will be sent over cleartext http to a non-loopback host — prefer an https:// server URL"
            );
        }

        Ok(ApiClient {
            http,
            base_url,
            auth,
            workspace,
            max_response_bytes: MAX_RESPONSE_BYTES,
        })
    }

    /// Issue a GET request against `path` (which must start with `/`) and
    /// return the parsed JSON body, or an `ApiError` on failure.
    pub async fn get(&self, path: &str) -> Result<Value, ApiError> {
        let url = format!("{}{}", self.base_url, path);
        let request = self.http.get(&url);
        self.send(request, url).await
    }

    /// Issue a POST request with a JSON `body` against `path` (which must
    /// start with `/`) and return the parsed JSON body, or an `ApiError` on
    /// failure.
    pub async fn post(&self, path: &str, body: &Value) -> Result<Value, ApiError> {
        self.post_with_headers(path, body, &[]).await
    }

    /// Like [`Self::post`], but attaches each `(name, value)` pair in
    /// `headers` to the request before auth — e.g. the `x-skardi-session-id`
    /// header for `skardi run --session-id`.
    pub async fn post_with_headers(
        &self,
        path: &str,
        body: &Value,
        headers: &[(&str, &str)],
    ) -> Result<Value, ApiError> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = self.http.post(&url).json(body);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        self.send(request, url).await
    }

    /// Shrink the response-body cap so tests can exercise the limit without
    /// buffering hundreds of megabytes.
    #[cfg(test)]
    fn with_max_response_bytes(mut self, cap: usize) -> Self {
        self.max_response_bytes = cap;
        self
    }

    /// Set the headers every request carries: `Authorization: Bearer <token>`
    /// when a token is configured, and `Skardi-Workspace` when a cloud context
    /// named one (§7.3).
    ///
    /// `insert`, on a built `Request`, rather than `RequestBuilder::header`:
    /// the builder APPENDS, so a per-call header of the same name would
    /// produce two values and leave which one the peer honours up to its
    /// parser. For the workspace selector that is an authorization-relevant
    /// difference, so these two names are set here and cannot be displaced.
    fn set_reserved_headers(&self, request: &mut reqwest::Request) {
        let headers = request.headers_mut();
        if let Some(auth) = &self.auth {
            headers.insert(AUTHORIZATION, auth.clone());
        }
        if let Some(workspace) = &self.workspace {
            headers.insert(
                HeaderName::from_static(WORKSPACE_HEADER_LOWER),
                workspace.clone(),
            );
        }
    }

    /// Send `request`, mapping transport failures and non-success/unparsable
    /// responses to `ApiError`. `url` is the request URL, kept for
    /// `ApiError::Connect` messages.
    async fn send(&self, request: RequestBuilder, url: String) -> Result<Value, ApiError> {
        let mut request = request.build().map_err(|err| ApiError::Connect {
            url: url.clone(),
            message: err.to_string(),
        })?;
        self.set_reserved_headers(&mut request);
        let response = self
            .http
            .execute(request)
            .await
            .map_err(|err| ApiError::Connect {
                url: url.clone(),
                message: err.to_string(),
            })?;

        let status = response.status();
        // Read before the body: `read_body_capped` consumes the response.
        let retry_after = retry_after_seconds(&response);
        let body_bytes = Self::read_body_capped(response, self.max_response_bytes, &url).await?;
        let body_text = String::from_utf8_lossy(&body_bytes);

        if !status.is_success() {
            return Err(Self::map_error_body(status, &body_text, retry_after));
        }

        serde_json::from_str(&body_text).map_err(|err| ApiError::Http {
            status: status.as_u16(),
            error_type: None,
            message: format!("failed to parse response body as JSON: {err}"),
            retry_after: None,
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
                    retry_after: None,
                });
            }
            buf.extend_from_slice(&chunk);
        }
    }

    /// Map a non-success response body to `ApiError::Http`: try the
    /// server's JSON error envelope first (a string `error` field, with an
    /// optional `error_type`); fall back to the first line of the raw body
    /// with no `error_type` when the body isn't that envelope.
    fn map_error_body(status: StatusCode, body_text: &str, retry_after: Option<u64>) -> ApiError {
        match serde_json::from_str::<ErrorEnvelope>(body_text) {
            Ok(envelope) => ApiError::Http {
                status: status.as_u16(),
                error_type: envelope.error_type,
                message: envelope.error,
                retry_after,
            },
            Err(_) => ApiError::Http {
                status: status.as_u16(),
                error_type: None,
                message: body_text.lines().next().unwrap_or("").to_string(),
                retry_after,
            },
        }
    }
}

/// True when `base_url` is plain `http://` to a host other than loopback
/// (`localhost`, `127.0.0.0/8`, or `::1`) — the case where a configured
/// bearer token would travel in cleartext across a real network.
pub(crate) fn is_cleartext_remote(base_url: &str) -> bool {
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

    !is_local_host(host)
}

/// True for hosts whose traffic never leaves the machine: `localhost`,
/// IPv4 loopback (`127.0.0.0/8`), IPv6 loopback (`::1`) including its
/// IPv4-mapped form (`::ffff:127.x.x.x`), and the unspecified addresses
/// (`0.0.0.0` / `::`), which the usual platforms route to localhost when
/// used as a connect target.
fn is_local_host(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    if let Ok(v4) = host.parse::<Ipv4Addr>() {
        return v4.is_loopback() || v4.is_unspecified();
    }
    if let Ok(v6) = host.parse::<Ipv6Addr>() {
        return v6.is_loopback()
            || v6.is_unspecified()
            || v6
                .to_ipv4_mapped()
                .is_some_and(|v4| v4.is_loopback() || v4.is_unspecified());
    }
    false
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
            context: None,
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
                retry_after,
            } => {
                assert_eq!(*status, 400);
                assert_eq!(error_type.as_deref(), Some("sql_validation_error"));
                assert_eq!(message, "column foo does not exist");
                assert_eq!(*retry_after, None);
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
                retry_after,
            } => {
                assert_eq!(*status, 503);
                assert_eq!(*error_type, None);
                assert_eq!(message, "service unavailable");
                // No `Retry-After` on the wire reads as absent, which is what
                // keeps §8's schema-limit message off a plain outage.
                assert_eq!(*retry_after, None);
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
    fn cleartext_remote_detection() {
        use super::is_cleartext_remote;

        // Loopback and https are fine.
        assert!(!is_cleartext_remote("http://127.0.0.1:8080"));
        assert!(!is_cleartext_remote("http://127.9.9.9"));
        assert!(!is_cleartext_remote("http://localhost:8080"));
        assert!(!is_cleartext_remote("http://LOCALHOST"));
        assert!(!is_cleartext_remote("http://[::1]:8080"));
        assert!(!is_cleartext_remote("http://[::ffff:127.0.0.1]:8080"));
        assert!(!is_cleartext_remote("http://0.0.0.0:8080"));
        assert!(!is_cleartext_remote("http://[::]:8080"));
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

#[cfg(test)]
mod retry_after_tests {
    use super::{ApiClient, ApiError};
    use crate::config::ClientConfig;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Delta-seconds is parsed; an HTTP-date and any other unparsable value
    /// read as ABSENT, so §8 falls back to the plain error instead of printing
    /// a guessed retry interval.
    #[tokio::test]
    async fn only_delta_seconds_are_read_as_a_retry_interval() {
        for (header, expected) in [
            ("7", Some(7)),
            (" 7 ", Some(7)),
            ("Wed, 21 Oct 2026 07:28:00 GMT", None),
            ("soon", None),
            ("-1", None),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .respond_with(
                    ResponseTemplate::new(503)
                        .insert_header("Retry-After", header)
                        .set_body_string("unavailable"),
                )
                .mount(&server)
                .await;

            let config = ClientConfig {
                server: server.uri(),
                token: None,
                context: None,
            };
            let err = ApiClient::new(&config)
                .unwrap()
                .get("/x")
                .await
                .unwrap_err();
            match err {
                ApiError::Http { retry_after, .. } => {
                    assert_eq!(retry_after, expected, "Retry-After: {header:?}")
                }
                other => panic!("expected Http, got {other:?}"),
            }
        }
    }
}

#[cfg(test)]
mod workspace_selector_tests {
    use super::{ApiClient, ApiError, WORKSPACE_HEADER};
    use crate::config::{ClientConfig, ContextMode, SelectedContext};
    use serde_json::json;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn cloud_config(server: &str, workspace: Option<&str>, mode: ContextMode) -> ClientConfig {
        ClientConfig {
            server: server.to_string(),
            token: Some("pat-value".to_string()),
            context: Some(SelectedContext {
                name: "acme/prod".to_string(),
                mode,
                workspace: workspace.map(str::to_string),
                token_expires_at: None,
            }),
        }
    }

    #[tokio::test]
    async fn cloud_context_names_its_workspace_on_every_request() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/data_source"))
            .and(header(WORKSPACE_HEADER, "acme-prod"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .expect(1)
            .mount(&server)
            .await;

        let config = cloud_config(&server.uri(), Some("acme-prod"), ContextMode::Cloud);
        let client = ApiClient::new(&config).unwrap();
        client.get("/data_source").await.unwrap();
    }

    #[tokio::test]
    async fn server_context_sends_no_workspace_selector() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;

        // A server-mode context with a workspace set anyway: the header is
        // keyed off `mode`, not off the field being populated.
        let config = cloud_config(&server.uri(), Some("acme-prod"), ContextMode::Server);
        ApiClient::new(&config)
            .unwrap()
            .get("/status")
            .await
            .unwrap();

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        assert!(
            requests[0].headers.get(WORKSPACE_HEADER).is_none(),
            "a server-mode context must not send the gateway's workspace selector"
        );
    }

    /// Both header values are built at construction, so a value that cannot
    /// be sent is one clear config error instead of a failure at every
    /// request — and neither message may echo the value.
    #[test]
    fn a_credential_that_cannot_be_sent_as_a_header_fails_at_construction() {
        let mut config = cloud_config("https://gw.example", Some("acme-prod"), ContextMode::Cloud);
        config.token = Some("secret-with-a\nnewline".to_string());
        let err = construction_error(&config);
        assert!(err.contains("cannot be sent in an HTTP header"), "{err}");
        assert!(
            !err.contains("secret-with-a"),
            "the token must not be echoed: {err}"
        );

        let config = cloud_config(
            "https://gw.example",
            Some("bad\nworkspace"),
            ContextMode::Cloud,
        );
        assert!(construction_error(&config).contains(WORKSPACE_HEADER));
    }

    /// Sending a bearer over cleartext to a non-loopback host is warned about
    /// once, at construction — the token still goes, because refusing would
    /// break a deployment behind a TLS-terminating proxy, but silence would
    /// hide it.
    #[test]
    fn a_cleartext_remote_server_warns_when_a_token_is_configured() {
        let mut config = cloud_config("http://gateway.example", None, ContextMode::Server);
        config.token = Some("a-token".to_string());
        assert!(ApiClient::new(&config).is_ok());
    }

    /// A server URL that is not a URL surfaces as `Connect` — so `main` exits
    /// 2 and the message names the flags, rather than panicking somewhere
    /// inside reqwest.
    #[tokio::test]
    async fn an_unusable_server_url_is_a_connect_failure() {
        let config = ClientConfig {
            server: "not even a url".to_string(),
            token: None,
            context: None,
        };
        let err = ApiClient::new(&config)
            .unwrap()
            .get("/status")
            .await
            .unwrap_err();
        assert!(
            matches!(err, ApiError::Connect { .. }),
            "expected Connect, got {err:?}"
        );
        assert!(err.to_string().contains("--server"), "{err}");
    }

    /// A 200 whose body is not JSON is an `Http` error naming the parse
    /// failure, not a panic and not a silent empty result.
    #[tokio::test]
    async fn a_success_status_with_an_unparsable_body_is_reported() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("<html>not json</html>"))
            .mount(&server)
            .await;

        let config = ClientConfig {
            server: server.uri(),
            token: None,
            context: None,
        };
        let err = ApiClient::new(&config)
            .unwrap()
            .get("/status")
            .await
            .unwrap_err();
        match err {
            ApiError::Http {
                status,
                message,
                retry_after,
                ..
            } => {
                assert_eq!(status, 200);
                assert!(
                    message.contains("failed to parse response body"),
                    "{message}"
                );
                assert_eq!(retry_after, None);
            }
            other => panic!("expected Http, got {other:?}"),
        }
    }

    /// `unwrap_err` would require `ApiClient: Debug`, which it deliberately
    /// does not derive — it holds the bearer header value.
    fn construction_error(config: &ClientConfig) -> String {
        match ApiClient::new(config) {
            Ok(_) => panic!("expected ApiClient::new to refuse this config"),
            Err(err) => err.to_string(),
        }
    }

    #[tokio::test]
    async fn per_call_headers_cannot_displace_auth_or_the_selector() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;

        let config = cloud_config(&server.uri(), Some("acme-prod"), ContextMode::Cloud);
        ApiClient::new(&config)
            .unwrap()
            .post_with_headers(
                "/query",
                &json!({"sql": "select 1"}),
                &[
                    (WORKSPACE_HEADER, "someone-elses-workspace"),
                    ("Authorization", "Bearer forged"),
                ],
            )
            .await
            .unwrap();

        let requests = server.received_requests().await.unwrap();
        let headers = &requests[0].headers;
        assert_eq!(
            headers.get_all(WORKSPACE_HEADER).iter().count(),
            1,
            "selector must appear exactly once: {:?}",
            headers.get_all(WORKSPACE_HEADER).iter().collect::<Vec<_>>()
        );
        assert_eq!(headers.get(WORKSPACE_HEADER).unwrap(), "acme-prod");
        assert_eq!(headers.get("Authorization").unwrap(), "Bearer pat-value");
    }
}
