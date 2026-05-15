//! OpenTelemetry consumer data source.
//!
//! Federated reader over Prometheus-compatible HTTP APIs (metrics) and
//! Loki HTTP APIs (logs). Surfaces queryable SQL tables (`metrics`, `logs`)
//! with predicate / time-range / `GROUP BY` pushdown into the backend,
//! plus `prom_query` / `prom_range` / `loki_query` / `loki_range`
//! escape-hatch table functions.
//!
//! See `openspec/changes/add-otel-data-source/design.md` for the full design.

pub mod error;
pub mod http;
pub mod loki;
pub mod metrics;
pub mod prometheus;
pub mod time;
pub mod translator;

use std::collections::BTreeMap;
use std::time::Duration;

use serde::Deserialize;
use url::Url;

use error::OtelError;
use http::ResolvedAuth;
use time::TimeDefaults;

/// Selects which OTEL backend a `type: otel` source talks to.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OtelBackend {
    /// Prometheus-compatible HTTP API (Prometheus, Mimir, Thanos,
    /// VictoriaMetrics, Grafana Cloud Metrics, Datadog Prom-compat).
    Prometheus,
    /// Grafana Loki HTTP API.
    Loki,
}

/// Auth scheme declared in `ctx.yaml`. Inline secrets (`token:`,
/// `password:`, bare `username:`) are rejected at deserialize time
/// with a clear error message — see [`RawOtelAuth`].
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum OtelAuth {
    #[default]
    None,
    Bearer {
        env: String,
    },
    Basic {
        username_env: String,
        password_env: String,
    },
}

/// Wire format for `auth:` blocks in `ctx.yaml`. Carries every field
/// (including the forbidden inline ones) so the validating `TryFrom`
/// can return a precise error pointing at the offending field name.
#[derive(Debug, Deserialize)]
struct RawOtelAuth {
    kind: String,
    // bearer
    env: Option<String>,
    /// FORBIDDEN: inline secret.
    token: Option<String>,
    // basic
    username_env: Option<String>,
    password_env: Option<String>,
    /// FORBIDDEN: inline secret.
    username: Option<String>,
    /// FORBIDDEN: inline secret.
    password: Option<String>,
}

impl<'de> Deserialize<'de> for OtelAuth {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = RawOtelAuth::deserialize(deserializer)?;

        // Reject inline secrets first — they're the security-sensitive case
        // and we want the clearest possible message pointing at `env:`.
        if raw.token.is_some() {
            return Err(serde::de::Error::custom(
                "inline secrets are not permitted; use `env:` instead (got `auth.token`)",
            ));
        }
        if raw.password.is_some() {
            return Err(serde::de::Error::custom(
                "inline secrets are not permitted; use `password_env:` instead (got `auth.password`)",
            ));
        }
        if raw.username.is_some() {
            return Err(serde::de::Error::custom(
                "inline auth fields are not permitted; use `username_env:` instead (got `auth.username`)",
            ));
        }

        match raw.kind.as_str() {
            "none" => Ok(OtelAuth::None),
            "bearer" => {
                let env = raw.env.ok_or_else(|| {
                    serde::de::Error::custom(
                        "auth kind `bearer` requires an `env:` field naming the environment variable holding the token",
                    )
                })?;
                if env.is_empty() {
                    return Err(serde::de::Error::custom(
                        "auth `bearer` field `env:` cannot be empty",
                    ));
                }
                Ok(OtelAuth::Bearer { env })
            }
            "basic" => {
                let username_env = raw.username_env.ok_or_else(|| {
                    serde::de::Error::custom("auth kind `basic` requires a `username_env:` field")
                })?;
                let password_env = raw.password_env.ok_or_else(|| {
                    serde::de::Error::custom("auth kind `basic` requires a `password_env:` field")
                })?;
                if username_env.is_empty() || password_env.is_empty() {
                    return Err(serde::de::Error::custom(
                        "auth `basic` fields `username_env:` / `password_env:` cannot be empty",
                    ));
                }
                Ok(OtelAuth::Basic {
                    username_env,
                    password_env,
                })
            }
            other => Err(serde::de::Error::custom(format!(
                "unknown auth kind `{other}`; supported: none, bearer, basic"
            ))),
        }
    }
}

/// Source-specific configuration parsed from the `otel:` block of a
/// `ctx.yaml` `data_sources` entry. Per design.md Decision 11, this is
/// declared in `ctx.yaml` and consumed by the server source-loader;
/// pipelines never reference these fields directly.
#[derive(Debug, Clone, Deserialize)]
pub struct OtelSourceConfig {
    pub backend: OtelBackend,
    pub url: Url,

    #[serde(default)]
    pub auth: OtelAuth,

    #[serde(default)]
    pub extra_headers: BTreeMap<String, String>,

    /// Per-query response-row cap. Default 50_000.
    #[serde(default)]
    pub max_result_rows: Option<usize>,

    /// Default range-query window. Default 15 minutes.
    #[serde(default, with = "humantime_serde::option")]
    pub default_window: Option<Duration>,

    /// Default step for range queries. Default 30 seconds.
    #[serde(default, with = "humantime_serde::option")]
    pub default_step: Option<Duration>,

    /// Maximum allowed range-query window. Default 24 hours.
    #[serde(default, with = "humantime_serde::option")]
    pub max_window: Option<Duration>,

    /// Per-HTTP-request timeout. Default 10 seconds.
    #[serde(default, with = "humantime_serde::option")]
    pub request_timeout: Option<Duration>,
}

/// Hard-coded defaults documented in `design.md` Decision 7 and
/// `tasks.md` 3.1. Used to fill in any field the operator omitted.
mod defaults {
    use std::time::Duration;

    pub const MAX_RESULT_ROWS: usize = 50_000;
    pub const DEFAULT_WINDOW: Duration = Duration::from_secs(15 * 60);
    pub const DEFAULT_STEP: Duration = Duration::from_secs(30);
    pub const MAX_WINDOW: Duration = Duration::from_secs(24 * 60 * 60);
    pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
}

impl OtelSourceConfig {
    pub fn max_result_rows_or_default(&self) -> usize {
        self.max_result_rows.unwrap_or(defaults::MAX_RESULT_ROWS)
    }

    pub fn request_timeout_or_default(&self) -> Duration {
        self.request_timeout.unwrap_or(defaults::REQUEST_TIMEOUT)
    }

    pub fn time_defaults(&self) -> TimeDefaults {
        TimeDefaults {
            default_window: self.default_window.unwrap_or(defaults::DEFAULT_WINDOW),
            default_step: self.default_step.unwrap_or(defaults::DEFAULT_STEP),
            max_window: self.max_window.unwrap_or(defaults::MAX_WINDOW),
        }
    }

    /// Read every env var named by `self.auth` and return the resolved
    /// credentials. Surfaces `OtelError::MissingCredential` if any
    /// required var is unset or empty, naming both the source and the
    /// var so operators can fix `ctx.yaml` without digging.
    pub fn load_credentials(&self, source_name: &str) -> Result<ResolvedAuth, OtelError> {
        match &self.auth {
            OtelAuth::None => Ok(ResolvedAuth::None),
            OtelAuth::Bearer { env } => {
                let token = read_env(env, source_name)?;
                Ok(ResolvedAuth::Bearer(token))
            }
            OtelAuth::Basic {
                username_env,
                password_env,
            } => {
                let username = read_env(username_env, source_name)?;
                let password = read_env(password_env, source_name)?;
                Ok(ResolvedAuth::Basic { username, password })
            }
        }
    }
}

/// Read an env var and treat empty values as unset — operators
/// sometimes export blank vars by accident and we want a clear startup
/// failure rather than a confusing 401 from the upstream later.
fn read_env(var: &str, source_name: &str) -> Result<String, OtelError> {
    match std::env::var(var) {
        Ok(value) if !value.is_empty() => Ok(value),
        _ => Err(OtelError::MissingCredential {
            source_name: source_name.to_string(),
            var: var.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: deserialize a YAML snippet into OtelSourceConfig and
    /// return the result (or the formatted error message).
    fn parse(yaml: &str) -> Result<OtelSourceConfig, String> {
        serde_yaml::from_str(yaml).map_err(|e| e.to_string())
    }

    #[test]
    fn minimal_prometheus_config_parses() {
        let cfg = parse(
            r#"
backend: prometheus
url: http://localhost:9090
"#,
        )
        .unwrap();
        assert_eq!(cfg.backend, OtelBackend::Prometheus);
        assert_eq!(cfg.url.as_str(), "http://localhost:9090/");
        assert_eq!(cfg.auth, OtelAuth::None);
        assert!(cfg.extra_headers.is_empty());
        assert_eq!(cfg.max_result_rows_or_default(), 50_000);
        assert_eq!(cfg.request_timeout_or_default(), Duration::from_secs(10));
    }

    #[test]
    fn full_loki_config_parses() {
        let cfg = parse(
            r#"
backend: loki
url: http://loki:3100
auth:
  kind: bearer
  env: LOKI_BEARER
extra_headers:
  X-Scope-OrgID: tenant-a
max_result_rows: 100000
default_window: 1h
default_step: 60s
max_window: 7d
request_timeout: 30s
"#,
        )
        .unwrap();
        assert_eq!(cfg.backend, OtelBackend::Loki);
        assert!(matches!(cfg.auth, OtelAuth::Bearer { ref env } if env == "LOKI_BEARER"));
        assert_eq!(
            cfg.extra_headers.get("X-Scope-OrgID").map(String::as_str),
            Some("tenant-a")
        );
        assert_eq!(cfg.max_result_rows_or_default(), 100_000);
        assert_eq!(
            cfg.time_defaults().default_window,
            Duration::from_secs(3600)
        );
        assert_eq!(
            cfg.time_defaults().max_window,
            Duration::from_secs(7 * 24 * 60 * 60)
        );
    }

    #[test]
    fn basic_auth_parses() {
        let cfg = parse(
            r#"
backend: prometheus
url: http://localhost:9090
auth:
  kind: basic
  username_env: PROM_USER
  password_env: PROM_PASS
"#,
        )
        .unwrap();
        assert!(matches!(
            cfg.auth,
            OtelAuth::Basic { ref username_env, ref password_env }
                if username_env == "PROM_USER" && password_env == "PROM_PASS"
        ));
    }

    #[test]
    fn inline_bearer_token_is_rejected() {
        let err = parse(
            r#"
backend: prometheus
url: http://localhost:9090
auth:
  kind: bearer
  token: "abc123"
"#,
        )
        .unwrap_err();
        assert!(
            err.contains("inline secrets are not permitted"),
            "got: {err}"
        );
        assert!(err.contains("auth.token"));
    }

    #[test]
    fn inline_password_is_rejected() {
        let err = parse(
            r#"
backend: prometheus
url: http://localhost:9090
auth:
  kind: basic
  username_env: PROM_USER
  password: "secret"
"#,
        )
        .unwrap_err();
        assert!(
            err.contains("inline secrets are not permitted"),
            "got: {err}"
        );
        assert!(err.contains("auth.password"));
    }

    #[test]
    fn inline_username_is_rejected() {
        let err = parse(
            r#"
backend: prometheus
url: http://localhost:9090
auth:
  kind: basic
  username: admin
  password_env: PROM_PASS
"#,
        )
        .unwrap_err();
        assert!(err.contains("inline auth fields"), "got: {err}");
        assert!(err.contains("auth.username"));
    }

    #[test]
    fn bearer_without_env_is_rejected() {
        let err = parse(
            r#"
backend: prometheus
url: http://localhost:9090
auth:
  kind: bearer
"#,
        )
        .unwrap_err();
        assert!(err.contains("requires an `env:`"), "got: {err}");
    }

    #[test]
    fn empty_bearer_env_is_rejected() {
        let err = parse(
            r#"
backend: prometheus
url: http://localhost:9090
auth:
  kind: bearer
  env: ""
"#,
        )
        .unwrap_err();
        assert!(err.contains("cannot be empty"), "got: {err}");
    }

    #[test]
    fn unknown_auth_kind_is_rejected_with_supported_list() {
        let err = parse(
            r#"
backend: prometheus
url: http://localhost:9090
auth:
  kind: oauth2
"#,
        )
        .unwrap_err();
        assert!(err.contains("unknown auth kind"), "got: {err}");
        assert!(err.contains("none, bearer, basic"), "got: {err}");
    }

    #[test]
    fn missing_backend_is_rejected() {
        let err = parse(
            r#"
url: http://localhost:9090
"#,
        )
        .unwrap_err();
        assert!(err.contains("backend"), "got: {err}");
    }

    #[test]
    fn unsupported_backend_is_rejected() {
        let err = parse(
            r#"
backend: tempo
url: http://localhost:3200
"#,
        )
        .unwrap_err();
        // Serde will produce "unknown variant ... expected one of `prometheus`, `loki`"
        assert!(
            err.contains("tempo") || err.contains("unknown variant"),
            "got: {err}"
        );
    }

    /// Avoid interfering with other tests by using a uniquely-named
    /// env var and clearing it inside the test scope.
    #[test]
    fn load_credentials_bearer_reads_env() {
        let var = "SKARDI_OTEL_TEST_BEARER";
        // SAFETY: tests in this binary run on a single thread per the
        // libtest model; this env-var manipulation is local to this test.
        // SAFETY: this is a single-threaded #[test] context; concurrent
        // env-var mutation is well-defined here.
        unsafe { std::env::set_var(var, "tok-12345") };

        let cfg = parse(&format!(
            r#"
backend: prometheus
url: http://localhost:9090
auth:
  kind: bearer
  env: {var}
"#,
        ))
        .unwrap();

        let resolved = cfg.load_credentials("prom").unwrap();
        match resolved {
            ResolvedAuth::Bearer(t) => assert_eq!(t, "tok-12345"),
            other => panic!("expected Bearer, got {other:?}"),
        }
        unsafe { std::env::remove_var(var) };
    }

    #[test]
    fn load_credentials_missing_env_returns_named_error() {
        let var = "SKARDI_OTEL_TEST_MISSING_BEARER";
        unsafe { std::env::remove_var(var) };
        let cfg = parse(&format!(
            r#"
backend: prometheus
url: http://localhost:9090
auth:
  kind: bearer
  env: {var}
"#,
        ))
        .unwrap();

        let err = cfg.load_credentials("prom").unwrap_err();
        match err {
            OtelError::MissingCredential {
                source_name,
                var: v,
            } => {
                assert_eq!(source_name, "prom");
                assert_eq!(v, var);
            }
            other => panic!("expected MissingCredential, got {other:?}"),
        }
    }

    #[test]
    fn load_credentials_empty_env_is_treated_as_missing() {
        let var = "SKARDI_OTEL_TEST_EMPTY_BEARER";
        unsafe { std::env::set_var(var, "") };
        let cfg = parse(&format!(
            r#"
backend: prometheus
url: http://localhost:9090
auth:
  kind: bearer
  env: {var}
"#,
        ))
        .unwrap();

        let err = cfg.load_credentials("prom").unwrap_err();
        assert!(matches!(err, OtelError::MissingCredential { .. }));
        unsafe { std::env::remove_var(var) };
    }

    #[test]
    fn load_credentials_basic_reads_both_env_vars() {
        let user_var = "SKARDI_OTEL_TEST_USER";
        let pass_var = "SKARDI_OTEL_TEST_PASS";
        unsafe { std::env::set_var(user_var, "alice") };
        unsafe { std::env::set_var(pass_var, "s3cret") };

        let cfg = parse(&format!(
            r#"
backend: prometheus
url: http://localhost:9090
auth:
  kind: basic
  username_env: {user_var}
  password_env: {pass_var}
"#,
        ))
        .unwrap();

        let resolved = cfg.load_credentials("prom").unwrap();
        match resolved {
            ResolvedAuth::Basic { username, password } => {
                assert_eq!(username, "alice");
                assert_eq!(password, "s3cret");
            }
            other => panic!("expected Basic, got {other:?}"),
        }
        unsafe { std::env::remove_var(user_var) };
        unsafe { std::env::remove_var(pass_var) };
    }

    #[test]
    fn load_credentials_none_returns_none() {
        let cfg = parse(
            r#"
backend: prometheus
url: http://localhost:9090
"#,
        )
        .unwrap();
        assert!(matches!(
            cfg.load_credentials("prom").unwrap(),
            ResolvedAuth::None
        ));
    }
}
