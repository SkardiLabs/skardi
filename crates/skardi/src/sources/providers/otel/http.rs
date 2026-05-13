//! Shared HTTP client for OTEL backends (Prometheus + Loki).
//!
//! `OtelHttpClient` wraps `reqwest::Client` with the cross-cutting
//! concerns common to every OTEL backend call:
//!
//! - per-request timeout from `OtelSourceConfig::request_timeout`
//! - bearer / basic auth resolved from env at construction time
//! - `extra_headers` (e.g. `X-Scope-OrgID` for Mimir tenant routing)
//!   pre-baked into the `reqwest::Client::default_headers`
//! - source-name tagging so every error message tells operators which
//!   `ctx.yaml` source the failure came from
//!
//! Per-source rate limiting is intentionally not part of v1 — see
//! `design.md` Non-Goals. The `send` method below is the single
//! chokepoint for outbound HTTP so that future protections (rate limit,
//! retry policy, request logging) can land in one place.

use std::collections::BTreeMap;
use std::time::Duration;

use reqwest::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use url::Url;

use super::error::OtelError;

/// Auth resolved against the process environment at startup. The
/// `OtelSourceConfig::load_credentials` helper (task 3.3) produces this
/// from the on-disk `OtelAuth`; `OtelHttpClient` then applies it to
/// every outbound request without re-reading env vars.
#[derive(Debug, Clone)]
pub enum ResolvedAuth {
    None,
    Bearer(String),
    Basic { username: String, password: String },
}

/// Construction-time inputs for [`OtelHttpClient::new`]. Kept as a
/// distinct struct so callers (Prometheus + Loki + tests) don't have to
/// remember a long positional argument list.
#[derive(Debug, Clone)]
pub struct OtelHttpClientConfig {
    pub source_name: String,
    pub base_url: Url,
    pub auth: ResolvedAuth,
    pub extra_headers: BTreeMap<String, String>,
    pub request_timeout: Duration,
}

/// HTTP client bound to a single OTEL source.
#[derive(Debug, Clone)]
pub struct OtelHttpClient {
    client: Client,
    base_url: Url,
    auth: ResolvedAuth,
    source_name: String,
    request_timeout: Duration,
}

impl OtelHttpClient {
    /// Build a `reqwest::Client` with `extra_headers` baked into the
    /// default header map and the configured timeout applied. Returns
    /// `OtelError::Backend` if `extra_headers` contains an invalid HTTP
    /// header name or value (rejected at construction time so a bad
    /// `ctx.yaml` fails the server at startup rather than at first
    /// scan).
    pub fn new(config: OtelHttpClientConfig) -> Result<Self, OtelError> {
        let OtelHttpClientConfig {
            source_name,
            base_url,
            auth,
            extra_headers,
            request_timeout,
        } = config;

        let mut header_map = HeaderMap::new();
        for (key, value) in &extra_headers {
            let name = HeaderName::from_bytes(key.as_bytes()).map_err(|e| OtelError::Backend {
                source_name: source_name.clone(),
                message: format!("invalid extra_headers key `{key}`: {e}"),
            })?;
            let val = HeaderValue::from_str(value).map_err(|e| OtelError::Backend {
                source_name: source_name.clone(),
                message: format!("invalid extra_headers value for `{key}`: {e}"),
            })?;
            header_map.insert(name, val);
        }

        let client = Client::builder()
            .timeout(request_timeout)
            .default_headers(header_map)
            .build()
            .map_err(|e| OtelError::Backend {
                source_name: source_name.clone(),
                message: format!("failed to build reqwest client: {e}"),
            })?;

        Ok(Self {
            client,
            base_url,
            auth,
            source_name,
            request_timeout,
        })
    }

    /// Source name this client is bound to. Used by callers to enrich
    /// their own error messages with a consistent identifier.
    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    /// Issue an HTTP GET against `<base_url>/<path>` with the given
    /// query string pairs. Auth is applied per-request from the
    /// resolved auth captured at construction time.
    ///
    /// This is the single chokepoint for outbound OTEL HTTP traffic —
    /// see the module-level doc comment.
    pub async fn get(
        &self,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<reqwest::Response, OtelError> {
        let url = self.base_url.join(path).map_err(|e| OtelError::Backend {
            source_name: self.source_name.clone(),
            message: format!("invalid path `{path}`: {e}"),
        })?;

        let builder = self.client.get(url).query(query);
        let builder = match &self.auth {
            ResolvedAuth::None => builder,
            ResolvedAuth::Bearer(token) => builder.bearer_auth(token),
            ResolvedAuth::Basic { username, password } => {
                builder.basic_auth(username, Some(password))
            }
        };

        builder.send().await.map_err(|e| self.map_send_error(e))
    }

    fn map_send_error(&self, err: reqwest::Error) -> OtelError {
        if err.is_timeout() {
            OtelError::Timeout {
                source_name: self.source_name.clone(),
                timeout: self.request_timeout,
            }
        } else {
            OtelError::Http {
                source_name: self.source_name.clone(),
                source: err,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(extra: BTreeMap<String, String>) -> OtelHttpClientConfig {
        OtelHttpClientConfig {
            source_name: "prom".to_string(),
            base_url: Url::parse("http://localhost:9090").unwrap(),
            auth: ResolvedAuth::None,
            extra_headers: extra,
            request_timeout: Duration::from_secs(10),
        }
    }

    #[test]
    fn builds_client_with_no_extra_headers() {
        let client = OtelHttpClient::new(cfg(BTreeMap::new())).unwrap();
        assert_eq!(client.source_name(), "prom");
    }

    #[test]
    fn builds_client_with_valid_extra_headers() {
        let mut extra = BTreeMap::new();
        extra.insert("X-Scope-OrgID".to_string(), "tenant-a".to_string());
        let client = OtelHttpClient::new(cfg(extra)).unwrap();
        assert_eq!(client.source_name(), "prom");
    }

    #[test]
    fn rejects_invalid_extra_header_name_at_construction() {
        let mut extra = BTreeMap::new();
        extra.insert("Has Spaces".to_string(), "value".to_string());
        let err = OtelHttpClient::new(cfg(extra)).unwrap_err();
        match err {
            OtelError::Backend {
                source_name,
                message,
            } => {
                assert_eq!(source_name, "prom");
                assert!(message.contains("extra_headers key"));
            }
            other => panic!("expected Backend, got {other:?}"),
        }
    }

    #[test]
    fn rejects_invalid_extra_header_value_at_construction() {
        let mut extra = BTreeMap::new();
        // Control char in value triggers reqwest header validation.
        extra.insert("X-Custom".to_string(), "bad\nvalue".to_string());
        let err = OtelHttpClient::new(cfg(extra)).unwrap_err();
        match err {
            OtelError::Backend {
                source_name,
                message,
            } => {
                assert_eq!(source_name, "prom");
                assert!(message.contains("extra_headers value"));
            }
            other => panic!("expected Backend, got {other:?}"),
        }
    }
}
