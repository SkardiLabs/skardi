//! HTTP client for the Open Connector gateway.
//!
//! The client owns everything network-facing in the integration:
//!
//! - gateway health and action discovery calls;
//! - runtime-token authentication (`Authorization: Bearer …`);
//! - connection-alias headers on action execution;
//! - action execution envelopes;
//! - bounded retries on 429 / transient 5xx / transport errors for
//!   idempotent calls (health, discovery), honoring `Retry-After`;
//!   POST execute only retries a pre-execution 429 — 5xx and transport
//!   failures are terminal so a possibly-executed action is never re-sent;
//! - bounded response decoding;
//! - per-request timeouts (from [`OpenConnectorConfig::request_timeout_seconds`]).
//!
//! It does not understand Arrow or DataFusion, and it never logs the runtime
//! token, provider credentials, or response bodies at `INFO` level.
//!
//! ## Gateway HTTP contract
//!
//! Verified against a live Open Connector gateway (v1.3.1) and its source.
//! Every `/v1` response uses one uniform JSON envelope:
//!
//! ```json
//! { "success": true, "message": "OK", "data": …, "meta": {} }
//! ```
//!
//! Failures carry `success: false`, a human-readable `message`, a stable
//! `errorCode` (e.g. `invalid_input`, `authorization_failed`), and
//! `meta.executionId` once execution started. The paths below are
//! centralized in private constants — they are the one place to reconcile
//! if the upstream API evolves again:
//!
//! - `GET {base}/v1/health` — any 2xx is healthy (the Bearer token is
//!   required whenever the gateway has runtime auth configured).
//! - `GET {base}/v1/actions/{action_id}` — action metadata under `data`:
//!   `{ "inputSchema": …, "outputSchema": …,
//!      "execution": { "locallyExecutable": bool, … } }`.
//! - `POST {base}/v1/actions/{action_id}` — body `{"input": …}`; on
//!   success `data` is the action output. There is **no** `/execute`
//!   suffix, and a named connection is selected with the
//!   `x-oo-connector-alias` header.

use std::time::Duration;

use futures::StreamExt;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::{RequestBuilder, Response, StatusCode};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

use super::config::{OpenConnectorConfig, validate_action_id};
use super::error::OpenConnectorError;
use crate::util::http::{clock_jitter_nanos, parse_retry_after};
use crate::util::text::truncate_chars;

/// Health endpoint path (relative to the gateway base URL).
const HEALTH_PATH: &str = "v1/health";
/// Action endpoint prefix; the percent-encoded action ID is appended.
/// `GET` fetches metadata, `POST` executes — same path, no suffix.
const ACTIONS_PATH: &str = "v1/actions";

/// Header selecting a named Open Connector connection on execute calls.
/// (`x-oomol-connector-alias` is an accepted alias; the gateway checks
/// this spelling second, and its docs use it.)
const CONNECTION_ALIAS_HEADER: &str = "x-oo-connector-alias";

/// Maximum attempts for one call (including the first) before
/// [`OpenConnectorError::RetriesExhausted`] is raised. Also the serde
/// default for `OpenConnectorConfig::max_attempts`.
pub(crate) const MAX_ATTEMPTS: u32 = 3;

/// Default bound on decoded response bodies (16 MiB). Also the serde
/// default for `OpenConnectorConfig::max_response_bytes`.
pub(crate) const DEFAULT_MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// Bytes read of a terminal error body — plenty for the 512-char message
/// `terminal_reason` keeps, without buffering a worst-case 16 MiB error page.
const ERROR_SNIPPET_BYTES: usize = 4 * 1024;

/// Base delay for exponential backoff between attempts.
const BACKOFF_BASE: Duration = Duration::from_millis(200);

/// Upper bound on a single wait between attempts, whether the wait came from
/// `Retry-After` or from exponential backoff.
const MAX_RETRY_WAIT: Duration = Duration::from_secs(10);

/// Retryable HTTP statuses: rate limiting plus transient server errors.
const RETRYABLE_STATUSES: &[u16] = &[429, 500, 502, 503, 504];

/// Whether a call may be retried after an ambiguous failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetryPolicy {
    /// GET-style idempotent calls (health, discovery): 429, transient 5xx,
    /// and transport errors are all safe to retry.
    Idempotent,
    /// POST execute: a 5xx or transport error may mean the action already
    /// ran against the SaaS provider — re-sending could execute it again.
    /// Only 429 (a pre-execution rate-limit rejection) is retried.
    NonIdempotent,
}

impl RetryPolicy {
    /// Whether an HTTP status may be retried under this policy.
    fn allows_status_retry(self, status: StatusCode) -> bool {
        match self {
            Self::Idempotent => RETRYABLE_STATUSES.contains(&status.as_u16()),
            Self::NonIdempotent => status == StatusCode::TOO_MANY_REQUESTS,
        }
    }
}

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
    ///
    /// Kept as `Option` so "the gateway did not say" is never confused with
    /// "the gateway said yes" — default-deny consumers (the action registry)
    /// must treat `None` as not executable.
    pub locally_executable: Option<bool>,
    /// Whether the gateway classifies the action as a non-mutating read.
    ///
    /// Same `Option` discipline as `locally_executable`: `None` means the
    /// gateway did not classify the action, and default-deny consumers (the
    /// raw-action UDTF) must refuse to execute it. Source-pack actions are
    /// read-only by Skardi's own review instead, so packs do not consult
    /// this flag.
    ///
    /// Today's Open Connector publishes **no** read/write classification
    /// (verified against the gateway and its source), so this is always
    /// `None` against a real gateway and raw scans are refused; the parse
    /// site (`execution.readOnly`) is forward-compatible for when the
    /// upstream grows one.
    pub read_only: Option<bool>,
}

/// Uniform `/v1` response envelope. Unknown fields (`meta`, additive keys)
/// are ignored; `success` has no default so a body that isn't an envelope
/// fails to parse instead of masquerading as a failed one.
#[derive(Debug, Deserialize)]
struct GatewayEnvelope {
    success: bool,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    data: Option<Value>,
    #[serde(default, rename = "errorCode")]
    error_code: Option<String>,
    #[serde(default)]
    meta: Option<Value>,
}

impl GatewayEnvelope {
    /// Render a failed envelope as `"<errorCode>: <message> (execution
    /// <id>)"`, bounded — the pieces the gateway's own audit log keys on.
    fn failure_reason(&self) -> String {
        const MAX_MESSAGE: usize = 512;
        let code = self.error_code.as_deref().unwrap_or("error");
        let message: String = self
            .message
            .as_deref()
            .unwrap_or("(no message)")
            .chars()
            .take(MAX_MESSAGE)
            .collect();
        match self
            .meta
            .as_ref()
            .and_then(|meta| meta.get("executionId"))
            .and_then(Value::as_str)
        {
            Some(id) => format!("{code}: {message} (execution {id})"),
            None => format!("{code}: {message}"),
        }
    }
}

/// Wire shape of the discovery envelope's `data`. Unknown fields are
/// ignored so additive gateway changes don't break discovery;
/// `locallyExecutable` deliberately has no default: a missing field must
/// stay missing.
#[derive(Debug, Deserialize)]
struct RawDiscoveredAction {
    #[serde(rename = "inputSchema")]
    input_schema: Option<Value>,
    #[serde(rename = "outputSchema")]
    output_schema: Option<Value>,
    execution: Option<RawExecution>,
}

/// The discovery `execution` block: runtime executability plus the
/// (not-yet-published) read-only classification.
#[derive(Debug, Deserialize)]
struct RawExecution {
    #[serde(rename = "locallyExecutable")]
    locally_executable: Option<bool>,
    #[serde(rename = "readOnly")]
    read_only: Option<bool>,
}

/// Execute request envelope. Borrowing `input` lets reqwest serialize the
/// struct directly, avoiding a deep clone of the caller's `Value`.
#[derive(Debug, Serialize)]
struct ExecuteEnvelope<'a> {
    input: &'a Value,
}

impl OpenConnectorClient {
    /// Build a client from the gateway URL and the typed config.
    ///
    /// Reads the runtime token from the environment variable named by
    /// `config.runtime_token_env`. The gateway URL must be `http(s)://`
    /// without embedded credentials, query parameters, or a fragment —
    /// the token is sent as a Bearer header, so credentials in the URL
    /// would only leak into logs and API responses.
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
        let raw = std::env::var(&config.runtime_token_env).map_err(|_| {
            OpenConnectorError::MissingRuntimeToken {
                env: config.runtime_token_env.clone(),
            }
        })?;

        // `export TOKEN="$(cat token.txt)"` leaves a trailing newline on the
        // value — trim that away, then reject anything still not header-legal
        // (a control character would turn every request into a reqwest
        // builder error, surfacing as an opaque retryable transport error).
        let token = raw.trim();
        if token.is_empty() {
            return Err(OpenConnectorError::InvalidRuntimeToken {
                env: config.runtime_token_env.clone(),
                reason: "empty after trimming whitespace".to_string(),
            });
        }
        if let Some(bad) = token.chars().find(|c| c.is_ascii_control()) {
            return Err(OpenConnectorError::InvalidRuntimeToken {
                env: config.runtime_token_env.clone(),
                reason: format!("contains control character U+{:04X}", bad as u32),
            });
        }

        let client = Self::new(
            gateway_url,
            token,
            Duration::from_secs(config.request_timeout_seconds),
        )?;
        Ok(client
            .with_max_response_bytes(
                usize::try_from(config.max_response_bytes).unwrap_or(usize::MAX),
            )
            .with_max_attempts(config.max_attempts))
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
        // A query string or fragment has no meaning for a base URL, and query
        // strings are the classic way to smuggle tokens (`?token=…`) into a
        // URL that later shows up in Debug output, logs, and the
        // data-sources API response. Reject both outright.
        if base_url.query().is_some() || base_url.fragment().is_some() {
            return Err(OpenConnectorError::GatewayUrlWithQueryOrFragment {
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
            RetryPolicy::Idempotent,
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
        // Boundary check before URL construction: a dot segment would be
        // resolved by Url::join and escape the /v1/actions/ namespace.
        validate_action_id(action_id)?;
        let url = self.action_url(action_id);
        let operation = format!("discover action '{action_id}'");
        let response = self
            .send_with_retry(
                &operation,
                RetryPolicy::Idempotent,
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
        let envelope = parse_envelope(&body, &operation)?;
        if !envelope.success {
            return Err(OpenConnectorError::ActionDiscoveryFailed {
                action_id: action_id.to_string(),
                reason: envelope.failure_reason(),
            });
        }
        let data = envelope
            .data
            .ok_or_else(|| OpenConnectorError::InvalidGatewayResponse {
                operation: operation.clone(),
                reason: "successful discovery envelope has no 'data'".to_string(),
            })?;
        let raw: RawDiscoveredAction = serde_json::from_value(data).map_err(|e| {
            OpenConnectorError::InvalidGatewayResponse {
                operation: operation.clone(),
                reason: format!("action metadata does not match the discovery shape: {e}"),
            }
        })?;

        Ok(DiscoveredAction {
            input_schema: raw.input_schema,
            output_schema: raw.output_schema,
            locally_executable: raw.execution.as_ref().and_then(|e| e.locally_executable),
            read_only: raw.execution.as_ref().and_then(|e| e.read_only),
        })
    }

    /// `POST /v1/actions/{action_id}` — execute one action and return the
    /// response envelope's `data` value (the executor output).
    ///
    /// Crate-internal on purpose: execution is the *dangerous* half of the
    /// gateway contract (mutating actions exist upstream), and the
    /// default-deny gating lives one layer up — `ActionRegistry::load` admits
    /// only explicitly allowlisted, locally-executable actions, and the scan
    /// engine / UDTFs check membership before calling this. Keeping the
    /// method `pub(crate)` makes that gating structurally un-bypassable from
    /// outside the crate.
    pub(crate) async fn execute(
        &self,
        action_id: &str,
        input: &Value,
        connection_alias: Option<&str>,
    ) -> Result<Value, OpenConnectorError> {
        // Same namespace-escape guard as discover_action.
        validate_action_id(action_id)?;
        let url = self.action_url(action_id);
        let operation = format!("execute action '{action_id}'");
        // Borrowing envelope: reqwest serializes it directly, so the caller's
        // input Value is not deep-cloned into an intermediate `json!` value.
        let body = ExecuteEnvelope { input };

        let response = self
            .send_with_retry(
                &operation,
                // POST execute is non-idempotent: 5xx/transport failures are
                // terminal, only a pre-execution 429 is retried.
                RetryPolicy::NonIdempotent,
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
        // A 2xx body that isn't a gateway envelope is a contract violation —
        // failing loudly keeps it out of Arrow rows. A 2xx envelope with
        // `success: false` should not occur (failures come with 4xx/5xx),
        // but a gateway that did send one must surface as the failure it
        // reports, never as action output.
        let envelope = parse_envelope(&text, &operation)?;
        if !envelope.success {
            return Err(OpenConnectorError::ActionExecutionFailed {
                action_id: action_id.to_string(),
                reason: envelope.failure_reason(),
            });
        }
        Ok(envelope.data.unwrap_or(Value::Null))
    }

    /// Join a relative path onto the gateway base URL.
    fn endpoint(&self, path: &str) -> String {
        self.base_url
            .join(path)
            .unwrap_or_else(|_| self.base_url.clone())
            .to_string()
    }

    /// Build the URL for one action (`GET` discovers, `POST` executes).
    fn action_url(&self, action_id: &str) -> String {
        let encoded = utf8_percent_encode(action_id, ACTION_ID_SET);
        self.endpoint(&format!("{ACTIONS_PATH}/{encoded}"))
    }

    /// Send one request with bounded retries. `terminal_error` maps a
    /// non-retryable HTTP status and its (bounded) body to the
    /// method-specific error variant; `policy` decides which failures may be
    /// retried at all (see [`RetryPolicy`]).
    ///
    /// Retried statuses wait for `Retry-After` when present, else
    /// exponential backoff with jitter; both are capped at [`MAX_RETRY_WAIT`].
    async fn send_with_retry(
        &self,
        operation: &str,
        policy: RetryPolicy,
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
                    if policy.allows_status_retry(status) {
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
                        // Terminal status: read only a snippet for the
                        // message — the caller keeps 512 chars, so there is
                        // no reason to buffer a worst-case 16 MiB error page.
                        let body = read_snippet(response, ERROR_SNIPPET_BYTES).await;
                        return Err(terminal_error(status, body));
                    }
                }
                Err(e) => {
                    // A request that cannot even be built (e.g. an illegal
                    // header value from a malformed token) will never succeed
                    // on retry — fail fast with the real cause.
                    if e.is_builder() {
                        return Err(OpenConnectorError::RequestBuildFailed {
                            operation: operation.to_string(),
                            reason: e.to_string(),
                        });
                    }
                    last_reason = e.to_string();
                    // A transport error on a non-idempotent call is ambiguous:
                    // the request may have reached the gateway and the action
                    // may already have run. Do not re-send.
                    if policy == RetryPolicy::NonIdempotent {
                        return Err(OpenConnectorError::NonIdempotentAmbiguousFailure {
                            operation: operation.to_string(),
                            reason: last_reason,
                        });
                    }
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

/// Read at most `limit` bytes of a response body for error diagnostics.
/// Unlike `read_body_bounded`, exceeding the limit is not an error — reading
/// simply stops, since the caller only keeps the first few hundred chars.
/// Lossy on purpose: a partial or non-UTF-8 body degrades to replacement
/// characters rather than another error.
async fn read_snippet(response: Response, limit: usize) -> String {
    let mut buf: Vec<u8> = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else { break };
        let remaining = limit.saturating_sub(buf.len());
        if remaining == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
    }
    String::from_utf8_lossy(&buf).into_owned()
}

/// Parse a body as the uniform gateway envelope, mapping a non-envelope
/// body to [`OpenConnectorError::InvalidGatewayResponse`].
fn parse_envelope(body: &str, operation: &str) -> Result<GatewayEnvelope, OpenConnectorError> {
    serde_json::from_str(body).map_err(|e| OpenConnectorError::InvalidGatewayResponse {
        operation: operation.to_string(),
        reason: format!("response is not a gateway envelope: {e}"),
    })
}

/// Render a terminal HTTP failure. A body that parses as a failed gateway
/// envelope is rendered structurally (`HTTP 403: authorization_failed: …`);
/// anything else is truncated raw so a noisy gateway can't flood the error
/// chain.
fn terminal_reason(status: StatusCode, body: &str) -> String {
    const MAX_BODY: usize = 512;
    if let Ok(envelope) = serde_json::from_str::<GatewayEnvelope>(body)
        && !envelope.success
    {
        return format!("HTTP {}: {}", status.as_u16(), envelope.failure_reason());
    }
    let trimmed = truncate_chars(body, MAX_BODY);
    format!("HTTP {}: {}", status.as_u16(), trimmed)
}

/// Exponential backoff with time-derived jitter: `200ms * 2^(attempt-1)`
/// plus up to 100ms of jitter, capped at [`MAX_RETRY_WAIT`]. Jitter comes
/// from the crate's shared [`clock_jitter_nanos`] — good enough for retry
/// decorrelation without a randomness dependency. The flat 0-100ms addition
/// is this client's own shape; the rss fetcher spreads the same source
/// ±50%, per its spec.
fn backoff(attempt: u32) -> Duration {
    let shift = attempt.saturating_sub(1).min(5);
    let base = BACKOFF_BASE.saturating_mul(1 << shift);
    let jitter = Duration::from_millis(clock_jitter_nanos() % 100);
    base.saturating_add(jitter).min(MAX_RETRY_WAIT)
}

/// Parse a `Retry-After` header (integer-seconds form), capped at
/// [`MAX_RETRY_WAIT`]. HTTP-date form is ignored.
/// Parse a `Retry-After` header, capped at [`MAX_RETRY_WAIT`]. Shared
/// parsing lives in [`crate::util::http::parse_retry_after`]; only the cap
/// is Open Connector-specific.
fn retry_after(response: &Response) -> Option<Duration> {
    parse_retry_after(response).map(|wait| wait.min(MAX_RETRY_WAIT))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::providers::open_connector::testutil::{
        MockGateway, MockResponse, RecordedRequest, discovery_ok, envelope_err, envelope_ok,
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
    async fn constructor_rejects_query_and_fragment() {
        // Query strings are the classic way to smuggle tokens into a URL
        // that later lands in Debug output, logs, and the data-sources API.
        for url in [
            "https://gateway/?token=abc",
            "https://gateway/?access_token=abc",
            "https://gateway:8443/prefix?api_key=abc",
            "https://gateway/#token=abc",
        ] {
            let err = OpenConnectorClient::new(url, "t", Duration::from_secs(1)).unwrap_err();
            assert!(
                matches!(
                    err,
                    OpenConnectorError::GatewayUrlWithQueryOrFragment { .. }
                ),
                "{url} should be rejected, got {err}"
            );
        }

        // A clean URL with a path prefix remains accepted.
        OpenConnectorClient::new("https://gateway:8443/prefix", "t", Duration::from_secs(1))
            .expect("clean URL with path is accepted");
    }

    #[tokio::test]
    async fn dot_segment_action_ids_are_rejected_before_any_request() {
        // `Url::join` resolves bare dot segments even when dots survive
        // percent-encoding, so `..` would escape /v1/actions/ onto {base}/v1/.
        // The boundary check must fire before any request is sent.
        let gateway = MockGateway::start(|_| MockResponse::ok("{}")).await;
        let client = test_client(&gateway, 3);

        for bad in ["..", ".", "a/b"] {
            let err = client.discover_action(bad).await.unwrap_err();
            assert!(
                matches!(err, OpenConnectorError::InvalidActionId { .. }),
                "{bad} should be rejected, got {err}"
            );
        }

        let err = client
            .execute("..", &serde_json::json!({}), None)
            .await
            .unwrap_err();
        assert!(matches!(err, OpenConnectorError::InvalidActionId { .. }));

        assert!(
            gateway.requests().is_empty(),
            "no request may be sent for a rejected action ID"
        );
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
    async fn from_config_trims_padded_token() {
        // `export T="$(cat token.txt)"` leaves a trailing newline — trimming
        // makes that case work, and the trimmed value is what goes on the wire.
        let env = "SKARDI_TEST_OC_TOKEN_PADDED";
        unsafe {
            std::env::set_var(env, "  test-token \n");
        }
        let config: OpenConnectorConfig =
            serde_yaml::from_str(&format!("runtime_token_env: {env}")).expect("parse config");
        let gateway = MockGateway::start(|_| MockResponse::ok("{}")).await;
        let client = OpenConnectorClient::from_config(&gateway.url, &config)
            .expect("padded token is accepted after trimming")
            .with_max_attempts(1);
        unsafe {
            std::env::remove_var(env);
        }

        client.health().await.expect("health");
        let requests = gateway.requests();
        assert_eq!(
            requests[0].header("authorization").as_deref(),
            Some("Bearer test-token")
        );
    }

    #[tokio::test]
    async fn from_config_rejects_malformed_token() {
        // Internal control characters can't be trimmed away — fail fast
        // with a targeted error naming the token, not a builder error.
        let env = "SKARDI_TEST_OC_TOKEN_MALFORMED";
        let config: OpenConnectorConfig =
            serde_yaml::from_str(&format!("runtime_token_env: {env}")).expect("parse config");

        unsafe {
            std::env::set_var(env, "abc\ndef");
        }
        let err = OpenConnectorClient::from_config("http://localhost:3000", &config).unwrap_err();
        assert!(
            matches!(err, OpenConnectorError::InvalidRuntimeToken { .. }),
            "control character should be rejected, got {err}"
        );

        unsafe {
            std::env::set_var(env, "  \n ");
        }
        let err = OpenConnectorClient::from_config("http://localhost:3000", &config).unwrap_err();
        assert!(
            matches!(err, OpenConnectorError::InvalidRuntimeToken { .. }),
            "whitespace-only token should be rejected, got {err}"
        );

        unsafe {
            std::env::remove_var(env);
        }
    }

    #[tokio::test]
    async fn builder_error_is_terminal_not_retried() {
        // Anything that still reaches send() with an unbuildable request
        // (defense in depth behind from_config's token check) must fail
        // immediately, not burn three backoff retries on a permanent error.
        let gateway = MockGateway::start(|_| MockResponse::ok("{}")).await;
        let client = OpenConnectorClient::new(&gateway.url, "bad\ntoken", Duration::from_secs(1))
            .expect("build client")
            .with_max_attempts(3);
        let err = client.health().await.unwrap_err();
        assert!(
            matches!(err, OpenConnectorError::RequestBuildFailed { .. }),
            "got {err}"
        );
        assert!(
            gateway.requests().is_empty(),
            "an unbuildable request must never hit the network"
        );
    }

    #[tokio::test]
    async fn from_config_wires_max_attempts() {
        let env = "SKARDI_TEST_OC_WIRE_ATTEMPTS";
        unsafe {
            std::env::set_var(env, "test-token");
        }
        let config: OpenConnectorConfig =
            serde_yaml::from_str(&format!("runtime_token_env: {env}\nmax_attempts: 1"))
                .expect("parse config");
        let gateway = MockGateway::start(|_| MockResponse::new(500, "{}")).await;
        let client = OpenConnectorClient::from_config(&gateway.url, &config).expect("build client");
        unsafe {
            std::env::remove_var(env);
        }

        let err = client.health().await.unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::RetriesExhausted { attempts: 1, .. }
        ));
        assert_eq!(
            gateway.requests().len(),
            1,
            "max_attempts: 1 must disable retries"
        );
    }

    #[tokio::test]
    async fn from_config_wires_max_response_bytes() {
        let env = "SKARDI_TEST_OC_WIRE_BYTES";
        unsafe {
            std::env::set_var(env, "test-token");
        }
        let config: OpenConnectorConfig =
            serde_yaml::from_str(&format!("runtime_token_env: {env}\nmax_response_bytes: 64"))
                .expect("parse config");
        let big = format!(r#"{{"pad": "{}"}}"#, "x".repeat(1024));
        let gateway = MockGateway::start(move |_| MockResponse::ok(&big)).await;
        let client = OpenConnectorClient::from_config(&gateway.url, &config).expect("build client");
        unsafe {
            std::env::remove_var(env);
        }

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
    async fn terminal_error_reads_only_a_snippet() {
        // An 8 KiB error page must not be buffered whole for a 512-char
        // message; the reason stays tightly bounded.
        let big = "e".repeat(8 * 1024);
        let gateway = MockGateway::start(move |_| MockResponse::new(400, big.clone())).await;
        let err = test_client(&gateway, 3)
            .execute("github.x", &serde_json::json!({}), None)
            .await
            .unwrap_err();
        match err {
            OpenConnectorError::ActionExecutionFailed { reason, .. } => {
                assert!(
                    reason.len() < 600,
                    "reason should be bounded by the snippet + 512-char cap, got {} bytes",
                    reason.len()
                );
            }
            other => panic!("expected ActionExecutionFailed, got {other}"),
        }
    }

    #[tokio::test]
    async fn discover_action_parses_the_envelope_metadata() {
        // The live gateway's discovery shape: camelCase schemas and the
        // execution block, all inside the uniform envelope's `data`.
        let gateway = MockGateway::start(|req| {
            if req.path == "/v1/actions/github.list_repository_issues" {
                MockResponse::ok(&discovery_ok(
                    r#"{"type": "object"}"#,
                    r#"{"type": "object", "properties": {"issues": {"type": "array"}}}"#,
                    true,
                    None,
                ))
            } else {
                MockResponse::new(404, "{}")
            }
        })
        .await;

        let action = test_client(&gateway, 3)
            .discover_action("github.list_repository_issues")
            .await
            .expect("discover");
        assert_eq!(action.locally_executable, Some(true));
        assert_eq!(
            action.read_only, None,
            "today's gateway publishes no read-only classification"
        );
        assert!(action.input_schema.is_some());
        assert!(action.output_schema.is_some());
    }

    #[tokio::test]
    async fn discover_action_forward_compatible_read_only_is_parsed() {
        // If a future gateway publishes execution.readOnly, the client picks
        // it up without changes; until then the field stays None (above).
        let gateway =
            MockGateway::start(|_| MockResponse::ok(&discovery_ok("{}", "{}", true, Some(true))))
                .await;

        let action = test_client(&gateway, 3)
            .discover_action("github.x")
            .await
            .expect("discover");
        assert_eq!(action.read_only, Some(true));
    }

    #[tokio::test]
    async fn discover_action_missing_executability_stays_none() {
        // The client must not invent a default: "gateway did not say" is
        // preserved as None so default-deny consumers can reject it.
        let gateway = MockGateway::start(|_| {
            MockResponse::ok(&envelope_ok(r#"{"inputSchema": {}, "outputSchema": {}}"#))
        })
        .await;

        let action = test_client(&gateway, 3)
            .discover_action("github.x")
            .await
            .expect("discover");
        assert_eq!(action.locally_executable, None);
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
    async fn execute_returns_envelope_data_and_sends_alias_header() {
        let gateway = MockGateway::start(|req| {
            assert_eq!(req.method, "POST");
            MockResponse::ok(&envelope_ok(r#"{"issues": [1, 2, 3]}"#))
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
            requests[0].path, "/v1/actions/github.list_repository_issues",
            "execute posts to the action path itself — the gateway has no /execute suffix"
        );
        assert_eq!(
            requests[0].header("x-oo-connector-alias").as_deref(),
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
    async fn execute_non_envelope_body_is_invalid_response() {
        // A 2xx body without the uniform envelope (no `success` bool) is a
        // contract violation — it must fail loudly rather than flow
        // downstream as action output. Bare arrays included: the real
        // gateway always wraps output in the envelope's `data`.
        for body in [
            r#"{"output": {"issues": []}}"#,
            r#"{"status": "pending", "job_id": "j-1"}"#,
            r#"[{"id": 1}, {"id": 2}]"#,
        ] {
            let owned = body.to_string();
            let gateway = MockGateway::start(move |_| MockResponse::ok(&owned)).await;
            let err = test_client(&gateway, 3)
                .execute("github.x", &serde_json::json!({}), None)
                .await
                .unwrap_err();
            assert!(
                matches!(err, OpenConnectorError::InvalidGatewayResponse { .. }),
                "{body} should be rejected, got {err}"
            );
        }
    }

    #[tokio::test]
    async fn execute_2xx_failed_envelope_surfaces_the_error_code() {
        // Failures normally arrive with a 4xx/5xx status, but a 2xx
        // `success: false` envelope must still surface as the failure it
        // reports, never as action output.
        let gateway = MockGateway::start(|_| {
            MockResponse::ok(&envelope_err("provider_error", "GitHub said no"))
        })
        .await;
        let err = test_client(&gateway, 3)
            .execute("github.x", &serde_json::json!({}), None)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                OpenConnectorError::ActionExecutionFailed { ref reason, .. }
                    if reason.contains("provider_error") && reason.contains("GitHub said no")
            ),
            "got {err}"
        );
    }

    #[tokio::test]
    async fn execute_400_is_terminal_and_renders_the_envelope_error() {
        // The live gateway rejects schema-invalid input with HTTP 400 and a
        // failed envelope; the error must carry its errorCode and message,
        // not a raw JSON dump.
        let gateway = MockGateway::start(|_| {
            MockResponse::new(
                400,
                envelope_err(
                    "invalid_input",
                    "Action input does not match the action schema.",
                ),
            )
        })
        .await;
        let err = test_client(&gateway, 3)
            .execute("github.x", &serde_json::json!({}), None)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                OpenConnectorError::ActionExecutionFailed { ref reason, .. }
                    if reason.contains("400")
                        && reason.contains("invalid_input")
                        && reason.contains("does not match the action schema")
            ),
            "got {err}"
        );
        assert_eq!(gateway.requests().len(), 1);
    }

    #[tokio::test]
    async fn execute_500_is_terminal_without_retry() {
        // A 5xx on POST execute may mean the action already ran — the client
        // must not re-send and risk re-executing it against the provider.
        let gateway = MockGateway::start(|_| MockResponse::new(502, "{}")).await;
        let err = test_client(&gateway, 3)
            .execute("github.x", &serde_json::json!({}), None)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::ActionExecutionFailed { ref reason, .. } if reason.contains("502")
        ));
        assert_eq!(
            gateway.requests().len(),
            1,
            "non-idempotent 5xx must not be retried"
        );
    }

    #[tokio::test]
    async fn execute_429_is_still_retried() {
        // 429 is a pre-execution rate-limit rejection, safe to retry even
        // for non-idempotent calls.
        let calls = Arc::new(AtomicUsize::new(0));
        let calls2 = Arc::clone(&calls);
        let gateway = MockGateway::start(move |_| {
            if calls2.fetch_add(1, Ordering::SeqCst) == 0 {
                MockResponse::new(429, "{}").with_header("retry-after", "1")
            } else {
                MockResponse::ok(&envelope_ok(r#"{"ok": true}"#))
            }
        })
        .await;

        let value = test_client(&gateway, 3)
            .execute("github.x", &serde_json::json!({}), None)
            .await
            .expect("execute");
        assert_eq!(value, serde_json::json!({"ok": true}));
        assert_eq!(gateway.requests().len(), 2);
    }

    #[tokio::test]
    async fn execute_transport_error_is_not_retried() {
        let client = OpenConnectorClient::new(
            "http://127.0.0.1:1",
            "test-token",
            Duration::from_millis(200),
        )
        .expect("build client")
        .with_max_attempts(3);
        let err = client
            .execute("github.x", &serde_json::json!({}), None)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                OpenConnectorError::NonIdempotentAmbiguousFailure { ref operation, .. }
                    if operation.contains("execute")
            ),
            "got {err}"
        );
    }
}
