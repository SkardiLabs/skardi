//! HTTP client for the Open Connector gateway.
//!
//! The client owns everything network-facing in the integration:
//!
//! - gateway health and action discovery calls;
//! - runtime-token authentication (`Authorization: Bearer …`);
//! - connection-alias headers on action execution;
//! - action execution envelopes;
//! - bounded retries on 429 / transient 5xx / transport errors, honoring
//!   `Retry-After`;
//! - bounded response decoding;
//! - per-request timeouts (from [`OpenConnectorConfig::request_timeout_seconds`]).
//!
//! It does not understand Arrow or DataFusion, and it never logs the runtime
//! token, provider credentials, or response bodies at `INFO` level.
//!
//! ## Gateway HTTP contract
//!
//! The paths below are centralized in private constants — they are the one
//! place to reconcile if the upstream Open Connector API evolves:
//!
//! - `GET {base}/v1/health` — any 2xx is healthy.
//! - `GET {base}/v1/actions/{action_id}` — action metadata:
//!   `{ "input_schema": …, "output_schema": …, "locally_executable": bool,
//!      "connection_aliases": […] }` (all fields optional but the last two).
//! - `POST {base}/v1/actions/{action_id}/execute` — body `{"input": …}`;
//!   the response envelope's `output` field is returned (a body without an
//!   `output` field is returned whole).

use std::time::Duration;

use futures::StreamExt;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::{RequestBuilder, Response, StatusCode};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use serde_json::Value;
use url::Url;

use super::config::OpenConnectorConfig;
use super::error::OpenConnectorError;

/// Health endpoint path (relative to the gateway base URL).
const HEALTH_PATH: &str = "v1/health";
/// Action metadata endpoint prefix; the percent-encoded action ID is appended.
const ACTIONS_PATH: &str = "v1/actions";
/// Suffix for the action execution endpoint.
const EXECUTE_SUFFIX: &str = "execute";

/// Header carrying the Open Connector connection alias on execute calls.
const CONNECTION_ALIAS_HEADER: &str = "x-openconnector-connection-alias";

/// Maximum attempts for one call (including the first) before
/// [`OpenConnectorError::RetriesExhausted`] is raised.
const MAX_ATTEMPTS: u32 = 3;

/// Default bound on decoded response bodies (16 MiB).
const DEFAULT_MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// Base delay for exponential backoff between attempts.
const BACKOFF_BASE: Duration = Duration::from_millis(200);

/// Upper bound on a single wait between attempts, whether the wait came from
/// `Retry-After` or from exponential backoff.
const MAX_RETRY_WAIT: Duration = Duration::from_secs(10);

/// Retryable HTTP statuses: rate limiting plus transient server errors.
const RETRYABLE_STATUSES: &[u16] = &[429, 500, 502, 503, 504];

/// Percent-encode set for action IDs in URL paths: everything outside
/// `[A-Za-z0-9._-]` is encoded, so dots and underscores survive verbatim.
const ACTION_ID_SET: &AsciiSet = &NON_ALPHANUMERIC.remove(b'.').remove(b'-').remove(b'_');

/// HTTP client for one Open Connector gateway.
///
/// Construct with [`OpenConnectorClient::from_config`]; the runtime token is
/// read from the environment variable named by the config, never from YAML.
pub struct OpenConnectorClient {
    http: reqwest::Client,
    base_url: Url,
    token: SecretString,
    max_response_bytes: usize,
    max_attempts: u32,
}

impl std::fmt::Debug for OpenConnectorClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately excludes the token and the reqwest client.
        f.debug_struct("OpenConnectorClient")
            .field("base_url", &self.base_url.as_str())
            .field("max_response_bytes", &self.max_response_bytes)
            .field("max_attempts", &self.max_attempts)
            .finish()
    }
}

/// Raw action metadata as discovered from the gateway.
///
/// This is the client-level shape; the action registry turns it into
/// `ActionMetadata` with a compatibility fingerprint.
#[derive(Debug, Clone)]
pub struct DiscoveredAction {
    /// Declared input JSON Schema, if the gateway provides one.
    pub input_schema: Option<Value>,
    /// Declared output JSON Schema, if the gateway provides one.
    pub output_schema: Option<Value>,
    /// Whether the action can execute on this gateway runtime.
    pub locally_executable: bool,
    /// Connection aliases available for this action.
    pub connection_aliases: Vec<String>,
}

/// Wire shape of the action discovery response. Unknown fields are ignored
/// so additive gateway changes don't break discovery.
#[derive(Debug, Deserialize)]
struct RawDiscoveredAction {
    input_schema: Option<Value>,
    output_schema: Option<Value>,
    #[serde(default = "default_locally_executable")]
    locally_executable: bool,
    #[serde(default)]
    connection_aliases: Vec<String>,
}

fn default_locally_executable() -> bool {
    true
}

impl OpenConnectorClient {
    /// Build a client from the gateway URL and the typed config.
    ///
    /// Reads the runtime token from the environment variable named by
    /// `config.runtime_token_env`. The gateway URL must be `http(s)://`
    /// without embedded credentials — the token is sent as a Bearer header,
    /// so credentials in the URL would only leak into logs.
    ///
    /// # Example
    /// ```no_run
    /// use skardi::sources::providers::open_connector::{
    ///     OpenConnectorClient, OpenConnectorConfig,
    /// };
    ///
    /// # async fn example() -> Result<(), skardi::sources::providers::open_connector::OpenConnectorError> {
    /// let config: OpenConnectorConfig =
    ///     serde_yaml::from_str("runtime_token_env: OPEN_CONNECTOR_TOKEN").unwrap();
    /// let client = OpenConnectorClient::from_config("http://open-connector:3000", &config)?;
    /// client.health().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_config(
        gateway_url: &str,
        config: &OpenConnectorConfig,
    ) -> Result<Self, OpenConnectorError> {
        let token = std::env::var(&config.runtime_token_env).map_err(|_| {
            OpenConnectorError::MissingRuntimeToken {
                env: config.runtime_token_env.clone(),
            }
        })?;

        Self::new(
            gateway_url,
            token,
            Duration::from_secs(config.request_timeout_seconds),
        )
    }

    /// Build a client from explicit parts. Kept crate-private so production
    /// construction always goes through the validated config; tests use it to
    /// inject tokens and short timeouts without touching the environment.
    pub(crate) fn new(
        gateway_url: &str,
        token: impl Into<String>,
        request_timeout: Duration,
    ) -> Result<Self, OpenConnectorError> {
        let mut base_url =
            Url::parse(gateway_url).map_err(|_| OpenConnectorError::InvalidGatewayUrl {
                url: gateway_url.to_string(),
            })?;

        match base_url.scheme() {
            "http" | "https" => {}
            _ => {
                return Err(OpenConnectorError::InvalidGatewayUrl {
                    url: gateway_url.to_string(),
                });
            }
        }
        if !base_url.username().is_empty() || base_url.password().is_some() {
            return Err(OpenConnectorError::InvalidGatewayUrl {
                url: gateway_url.to_string(),
            });
        }
        // `Url::join` replaces the last path segment unless the base ends in
        // '/', so normalize once here.
        if !base_url.path().ends_with('/') {
            let path = format!("{}/", base_url.path());
            base_url.set_path(&path);
        }

        let http = reqwest::Client::builder()
            .timeout(request_timeout)
            .build()
            .map_err(|e| OpenConnectorError::HttpClientBuild {
                reason: e.to_string(),
            })?;

        Ok(Self {
            http,
            base_url,
            token: SecretString::new(token.into().into_boxed_str()),
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            max_attempts: MAX_ATTEMPTS,
        })
    }

    /// Override the response-size decoding bound (default 16 MiB).
    pub fn with_max_response_bytes(mut self, max_response_bytes: usize) -> Self {
        self.max_response_bytes = max_response_bytes;
        self
    }

    /// Override the per-call attempt cap (default 3).
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts.max(1);
        self
    }

    /// The gateway base URL this client talks to.
    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    /// `GET /v1/health` — any 2xx means the gateway is reachable and the
    /// runtime token is accepted.
    pub async fn health(&self) -> Result<(), OpenConnectorError> {
        let url = self.endpoint(HEALTH_PATH);
        let operation = "health check".to_string();
        self.send_with_retry(
            &operation,
            || self.http.get(&url),
            |status, body| OpenConnectorError::HealthCheckFailed {
                url: url.clone(),
                reason: terminal_reason(status, &body),
            },
        )
        .await?;
        Ok(())
    }

    /// `GET /v1/actions/{action_id}` — fetch one action's metadata.
    pub async fn discover_action(
        &self,
        action_id: &str,
    ) -> Result<DiscoveredAction, OpenConnectorError> {
        let url = self.action_url(action_id, None);
        let operation = format!("discover action '{action_id}'");
        let response = self
            .send_with_retry(
                &operation,
                || self.http.get(&url),
                |status, body| {
                    if status == StatusCode::NOT_FOUND {
                        OpenConnectorError::ActionNotFound {
                            action_id: action_id.to_string(),
                        }
                    } else {
                        OpenConnectorError::ActionDiscoveryFailed {
                            action_id: action_id.to_string(),
                            reason: terminal_reason(status, &body),
                        }
                    }
                },
            )
            .await?;

        let body = self.read_body_bounded(response, &operation).await?;
        let raw: RawDiscoveredAction = serde_json::from_str(&body).map_err(|e| {
            OpenConnectorError::InvalidGatewayResponse {
                operation: operation.clone(),
                reason: format!("action metadata is not valid JSON: {e}"),
            }
        })?;

        Ok(DiscoveredAction {
            input_schema: raw.input_schema,
            output_schema: raw.output_schema,
            locally_executable: raw.locally_executable,
            connection_aliases: raw.connection_aliases,
        })
    }

    /// `POST /v1/actions/{action_id}/execute` — execute one action and return
    /// the response envelope's `output` value.
    pub async fn execute(
        &self,
        action_id: &str,
        input: &Value,
        connection_alias: Option<&str>,
    ) -> Result<Value, OpenConnectorError> {
        let url = self.action_url(action_id, Some(EXECUTE_SUFFIX));
        let operation = format!("execute action '{action_id}'");
        let body = serde_json::json!({ "input": input });

        let response = self
            .send_with_retry(
                &operation,
                || {
                    let mut req = self.http.post(&url).json(&body);
                    if let Some(alias) = connection_alias {
                        req = req.header(CONNECTION_ALIAS_HEADER, alias);
                    }
                    req
                },
                |status, body| OpenConnectorError::ActionExecutionFailed {
                    action_id: action_id.to_string(),
                    reason: terminal_reason(status, &body),
                },
            )
            .await?;

        let text = self.read_body_bounded(response, &operation).await?;
        let value: Value = serde_json::from_str(&text).map_err(|e| {
            OpenConnectorError::InvalidGatewayResponse {
                operation: operation.clone(),
                reason: format!("execution envelope is not valid JSON: {e}"),
            }
        })?;

        // The contract wraps results in an `output` field; tolerate gateways
        // that return the bare value.
        Ok(match value {
            Value::Object(ref map) => map.get("output").cloned().unwrap_or(value.clone()),
            other => other,
        })
    }

    /// Join a relative path onto the gateway base URL.
    fn endpoint(&self, path: &str) -> String {
        self.base_url
            .join(path)
            .unwrap_or_else(|_| self.base_url.clone())
            .to_string()
    }

    /// Build the URL for one action, optionally with a path suffix (`execute`).
    fn action_url(&self, action_id: &str, suffix: Option<&str>) -> String {
        let encoded = utf8_percent_encode(action_id, ACTION_ID_SET);
        match suffix {
            Some(suffix) => self.endpoint(&format!("{ACTIONS_PATH}/{encoded}/{suffix}")),
            None => self.endpoint(&format!("{ACTIONS_PATH}/{encoded}")),
        }
    }

    /// Send one request with bounded retries on 429 / transient 5xx /
    /// transport errors. `terminal_error` maps a non-retryable HTTP status
    /// and its (bounded) body to the method-specific error variant.
    ///
    /// Retried statuses wait for `Retry-After` when present, else
    /// exponential backoff with jitter; both are capped at [`MAX_RETRY_WAIT`].
    async fn send_with_retry(
        &self,
        operation: &str,
        build: impl Fn() -> RequestBuilder,
        terminal_error: impl Fn(StatusCode, String) -> OpenConnectorError,
    ) -> Result<Response, OpenConnectorError> {
        let mut last_reason = String::new();
        for attempt in 1..=self.max_attempts {
            let request = build().bearer_auth(self.token.expose_secret());
            match request.send().await {
                Ok(response) if response.status().is_success() => return Ok(response),
                Ok(response) => {
                    let status = response.status();
                    if RETRYABLE_STATUSES.contains(&status.as_u16()) {
                        last_reason = format!("HTTP {}", status.as_u16());
                        if attempt < self.max_attempts {
                            let wait = retry_after(&response).unwrap_or_else(|| backoff(attempt));
                            tracing::warn!(
                                operation = %operation,
                                attempt,
                                status = status.as_u16(),
                                "Open Connector request failed with retryable status; retrying"
                            );
                            tokio::time::sleep(wait).await;
                            continue;
                        }
                    } else {
                        let body = self
                            .read_body_bounded(response, operation)
                            .await
                            .unwrap_or_else(|_| "<body unreadable>".to_string());
                        return Err(terminal_error(status, body));
                    }
                }
                Err(e) => {
                    last_reason = e.to_string();
                    if attempt < self.max_attempts {
                        tracing::warn!(
                            operation = %operation,
                            attempt,
                            error = %last_reason,
                            "Open Connector request failed with a transport error; retrying"
                        );
                        tokio::time::sleep(backoff(attempt)).await;
                        continue;
                    }
                }
            }
        }
        Err(OpenConnectorError::RetriesExhausted {
            operation: operation.to_string(),
            attempts: self.max_attempts,
            reason: last_reason,
        })
    }

    /// Read a response body as UTF-8, enforcing the decoding bound both on
    /// a declared `Content-Length` and on the actual streamed bytes.
    async fn read_body_bounded(
        &self,
        response: Response,
        operation: &str,
    ) -> Result<String, OpenConnectorError> {
        let limit = self.max_response_bytes as u64;
        if let Some(len) = response.content_length()
            && len > limit
        {
            return Err(OpenConnectorError::ResponseTooLarge {
                operation: operation.to_string(),
                limit_bytes: self.max_response_bytes,
            });
        }

        let mut buf: Vec<u8> = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| OpenConnectorError::InvalidGatewayResponse {
                operation: operation.to_string(),
                reason: format!("failed to read response body: {e}"),
            })?;
            if buf.len() as u64 + chunk.len() as u64 > limit {
                return Err(OpenConnectorError::ResponseTooLarge {
                    operation: operation.to_string(),
                    limit_bytes: self.max_response_bytes,
                });
            }
            buf.extend_from_slice(&chunk);
        }

        String::from_utf8(buf).map_err(|e| OpenConnectorError::InvalidGatewayResponse {
            operation: operation.to_string(),
            reason: format!("response body is not valid UTF-8: {e}"),
        })
    }
}

/// Render a terminal HTTP failure as `"HTTP <status>: <truncated body>"`.
/// Bodies are truncated so a noisy gateway can't flood the error chain.
fn terminal_reason(status: StatusCode, body: &str) -> String {
    const MAX_BODY: usize = 512;
    let trimmed: String = body.chars().take(MAX_BODY).collect();
    format!("HTTP {}: {}", status.as_u16(), trimmed)
}

/// Exponential backoff with time-derived jitter: `200ms * 2^(attempt-1)`
/// plus up to 100ms of jitter, capped at [`MAX_RETRY_WAIT`]. Jitter comes
/// from the system clock's sub-second nanoseconds — good enough for retry
/// decorrelation without a randomness dependency.
fn backoff(attempt: u32) -> Duration {
    let shift = attempt.saturating_sub(1).min(5);
    let base = BACKOFF_BASE.saturating_mul(1 << shift);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let jitter = Duration::from_millis(u64::from(nanos % 100));
    base.saturating_add(jitter).min(MAX_RETRY_WAIT)
}

/// Parse a `Retry-After` header (integer-seconds form), capped at
/// [`MAX_RETRY_WAIT`]. HTTP-date form is ignored.
fn retry_after(response: &Response) -> Option<Duration> {
    let value = response.headers().get(reqwest::header::RETRY_AFTER)?;
    let seconds: u64 = value.to_str().ok()?.trim().parse().ok()?;
    Some(Duration::from_secs(seconds).min(MAX_RETRY_WAIT))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::providers::open_connector::testutil::{
        MockGateway, MockResponse, RecordedRequest,
    };
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn test_client(gateway: &MockGateway, max_attempts: u32) -> OpenConnectorClient {
        OpenConnectorClient::new(&gateway.url, "test-token", Duration::from_secs(2))
            .expect("build client")
            .with_max_attempts(max_attempts)
    }

    fn bearer(req: &RecordedRequest) -> Option<String> {
        req.header("authorization")
    }

    #[tokio::test]
    async fn health_ok_sends_bearer_token() {
        let gateway = MockGateway::start(|_| MockResponse::ok("{}")).await;
        test_client(&gateway, 3).health().await.expect("health");

        let requests = gateway.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "GET");
        assert_eq!(requests[0].path, "/v1/health");
        assert_eq!(bearer(&requests[0]).as_deref(), Some("Bearer test-token"));
    }

    #[tokio::test]
    async fn health_retries_on_500_then_succeeds() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls2 = Arc::clone(&calls);
        let gateway = MockGateway::start(move |_| {
            let n = calls2.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                MockResponse::new(500, "{}")
            } else {
                MockResponse::ok("{}")
            }
        })
        .await;

        test_client(&gateway, 3).health().await.expect("health");
        assert_eq!(gateway.requests().len(), 3);
    }

    #[tokio::test]
    async fn health_429_honors_retry_after() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls2 = Arc::clone(&calls);
        let gateway = MockGateway::start(move |_| {
            if calls2.fetch_add(1, Ordering::SeqCst) == 0 {
                MockResponse::new(429, "{}").with_header("retry-after", "1")
            } else {
                MockResponse::ok("{}")
            }
        })
        .await;

        test_client(&gateway, 3).health().await.expect("health");
        assert_eq!(gateway.requests().len(), 2);
    }

    #[tokio::test]
    async fn health_404_is_terminal_without_retry() {
        let gateway = MockGateway::start(|_| MockResponse::new(404, "no health here")).await;
        let err = test_client(&gateway, 3).health().await.unwrap_err();
        assert!(
            matches!(err, OpenConnectorError::HealthCheckFailed { ref reason, .. } if reason.contains("404")),
            "got {err}"
        );
        assert_eq!(gateway.requests().len(), 1);
    }

    #[tokio::test]
    async fn health_unreachable_exhausts_retries() {
        let client = OpenConnectorClient::new(
            "http://127.0.0.1:1",
            "test-token",
            Duration::from_millis(200),
        )
        .expect("build client")
        .with_max_attempts(2);
        let err = client.health().await.unwrap_err();
        assert!(
            matches!(
                err,
                OpenConnectorError::RetriesExhausted { ref operation, attempts: 2, .. }
                    if operation == "health check"
            ),
            "got {err}"
        );
    }

    #[tokio::test]
    async fn constructor_rejects_bad_urls() {
        let err = OpenConnectorClient::new("ftp://x", "t", Duration::from_secs(1)).unwrap_err();
        assert!(matches!(err, OpenConnectorError::InvalidGatewayUrl { .. }));

        let err = OpenConnectorClient::new("http://user:pass@x", "t", Duration::from_secs(1))
            .unwrap_err();
        assert!(matches!(err, OpenConnectorError::InvalidGatewayUrl { .. }));
    }

    #[tokio::test]
    async fn missing_runtime_token_env_is_an_error() {
        let config: OpenConnectorConfig =
            serde_yaml::from_str("runtime_token_env: SKARDI_TEST_OC_TOKEN_DEFINITELY_UNSET")
                .expect("parse config");
        let err = OpenConnectorClient::from_config("http://localhost:3000", &config).unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::MissingRuntimeToken { ref env }
                if env == "SKARDI_TEST_OC_TOKEN_DEFINITELY_UNSET"
        ));
    }

    #[tokio::test]
    async fn discover_action_parses_metadata() {
        let gateway = MockGateway::start(|req| {
            if req.path == "/v1/actions/github.list_repository_issues" {
                MockResponse::ok(
                    r#"{
                        "input_schema": {"type": "object"},
                        "output_schema": {"type": "object", "properties": {"issues": {"type": "array"}}},
                        "locally_executable": true,
                        "connection_aliases": ["work", "personal"]
                    }"#,
                )
            } else {
                MockResponse::new(404, "{}")
            }
        })
        .await;

        let action = test_client(&gateway, 3)
            .discover_action("github.list_repository_issues")
            .await
            .expect("discover");
        assert!(action.locally_executable);
        assert_eq!(action.connection_aliases, vec!["work", "personal"]);
        assert!(action.input_schema.is_some());
        assert!(action.output_schema.is_some());
    }

    #[tokio::test]
    async fn discover_action_404_maps_to_action_not_found() {
        let gateway = MockGateway::start(|_| MockResponse::new(404, "{}")).await;
        let err = test_client(&gateway, 3)
            .discover_action("github.missing")
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::ActionNotFound { ref action_id } if action_id == "github.missing"
        ));
    }

    #[tokio::test]
    async fn discover_action_malformed_json_is_invalid_response() {
        let gateway = MockGateway::start(|_| MockResponse::ok("this is not json")).await;
        let err = test_client(&gateway, 3)
            .discover_action("github.x")
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::InvalidGatewayResponse { .. }
        ));
    }

    #[tokio::test]
    async fn response_beyond_bound_is_rejected() {
        // The bound is enforced while streaming the body, so exercise a
        // body-reading call (discover), not health.
        let big = format!(r#"{{"pad": "{}"}}"#, "x".repeat(1024));
        let gateway = MockGateway::start(move |_| MockResponse::ok(&big)).await;
        let client = test_client(&gateway, 3).with_max_response_bytes(64);
        let err = client.discover_action("github.x").await.unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::ResponseTooLarge {
                limit_bytes: 64,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn execute_returns_output_and_sends_alias_header() {
        let gateway = MockGateway::start(|req| {
            assert_eq!(req.method, "POST");
            MockResponse::ok(r#"{"output": {"issues": [1, 2, 3]}}"#)
        })
        .await;

        let value = test_client(&gateway, 3)
            .execute(
                "github.list_repository_issues",
                &serde_json::json!({"owner": "SkardiLabs"}),
                Some("work"),
            )
            .await
            .expect("execute");
        assert_eq!(value, serde_json::json!({"issues": [1, 2, 3]}));

        let requests = gateway.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].path,
            "/v1/actions/github.list_repository_issues/execute"
        );
        assert_eq!(
            requests[0]
                .header("x-openconnector-connection-alias")
                .as_deref(),
            Some("work")
        );
        assert!(
            requests[0]
                .body
                .contains(r#""input":{"owner":"SkardiLabs"}"#),
            "execute wraps the input in an envelope: {}",
            requests[0].body
        );
    }

    #[tokio::test]
    async fn execute_without_output_field_returns_bare_body() {
        let gateway = MockGateway::start(|_| MockResponse::ok(r#"{"rows": []}"#)).await;
        let value = test_client(&gateway, 3)
            .execute("github.x", &serde_json::json!({}), None)
            .await
            .expect("execute");
        assert_eq!(value, serde_json::json!({"rows": []}));
    }

    #[tokio::test]
    async fn execute_400_is_terminal_without_retry() {
        let gateway =
            MockGateway::start(|_| MockResponse::new(400, r#"{"error": "bad input"}"#)).await;
        let err = test_client(&gateway, 3)
            .execute("github.x", &serde_json::json!({}), None)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::ActionExecutionFailed { ref reason, .. } if reason.contains("400")
        ));
        assert_eq!(gateway.requests().len(), 1);
    }
}
