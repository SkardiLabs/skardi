//! Typed configuration for a `type: graph` data source (design
//! §GraphConfig typed YAML). Milestone 1 supports the `age` backend;
//! views land with milestone 4.

use serde::Deserialize;

use super::error::GraphError;

/// Default per-query timeout (design §Security and operational bounds).
pub const DEFAULT_QUERY_TIMEOUT_SECONDS: u64 = 30;
/// Upper bound on the configurable timeout: one day. Keeps the value
/// well inside Postgres's statement_timeout range (int4 milliseconds)
/// and makes the client-side `+5s` wrap arithmetic trivially safe.
pub const MAX_QUERY_TIMEOUT_SECONDS: u64 = 86_400;
/// Default per-query row cap.
pub const DEFAULT_MAX_ROWS: usize = 10_000;
/// Default connection-pool size.
pub const DEFAULT_MAX_CONNECTIONS: u32 = 4;

/// The `graph:` block of a `type: graph` data source.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphConfig {
    /// Backend engine. Milestone 1: `age` (openCypher inside Postgres).
    pub backend: String,
    /// AGE graphs are named per database.
    pub graph_name: String,
    /// Environment variable NAMES holding credentials — values never
    /// appear in YAML (Open Connector's hygiene; the topology differs:
    /// there is no gateway, so Skardi holds the credential in memory).
    #[serde(default)]
    pub username_env: Option<String>,
    #[serde(default)]
    pub password_env: Option<String>,
    /// Passed to the backend as the transaction timeout, so runaway
    /// traversals die server-side.
    #[serde(default = "default_timeout")]
    pub query_timeout_seconds: u64,
    /// Rows Skardi will consume per query; exceeding it is a typed error,
    /// never a silent truncation.
    #[serde(default = "default_max_rows")]
    pub max_rows: usize,
    /// Connection-pool size against the backend.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
}

fn default_timeout() -> u64 {
    DEFAULT_QUERY_TIMEOUT_SECONDS
}

fn default_max_rows() -> usize {
    DEFAULT_MAX_ROWS
}

fn default_max_connections() -> u32 {
    DEFAULT_MAX_CONNECTIONS
}

impl GraphConfig {
    /// Pure validation (no network I/O): backend and scheme allowlists,
    /// identifier shape for the graph name, env-var NAME shape for
    /// credentials. `connection_string` is operator trust (the same tier
    /// as a Postgres connection string in the same file) — no SSRF guard,
    /// only a scheme allowlist (design §Security).
    pub fn validate(&self, name: &str, connection_string: &str) -> Result<(), GraphError> {
        if self.backend != "age" {
            return Err(GraphError::InvalidConfig {
                name: name.to_string(),
                reason: format!(
                    "backend '{}' is not supported (milestone 1 supports: age; \
                     neo4j and kuzu are later milestones)",
                    self.backend
                ),
            });
        }
        let scheme_ok = connection_string.starts_with("postgres://")
            || connection_string.starts_with("postgresql://");
        if !scheme_ok {
            return Err(GraphError::InvalidConfig {
                name: name.to_string(),
                reason: "the age backend requires a postgres:// or postgresql:// \
                         connection_string"
                    .to_string(),
            });
        }
        // Credentials travel as env-var NAMES only (username_env /
        // password_env) — a password embedded in the URL would sit in
        // config repos, deploy logs, and diagnostics. Parsed, not
        // substring-matched, so `:` in a database name cannot
        // false-positive. The error never echoes the URL (it may carry
        // the very secret being rejected).
        if let Ok(parsed) = url::Url::parse(connection_string) {
            if parsed.password().is_some() {
                return Err(GraphError::InvalidConfig {
                    name: name.to_string(),
                    reason: "connection_string must not embed a password — set \
                             password_env to the NAME of an environment variable \
                             instead"
                        .to_string(),
                });
            }
            if parsed
                .query_pairs()
                .any(|(k, _)| k.eq_ignore_ascii_case("password"))
            {
                return Err(GraphError::InvalidConfig {
                    name: name.to_string(),
                    reason: "connection_string must not carry a password= query \
                             parameter — set password_env instead"
                        .to_string(),
                });
            }
        }
        // The graph name is spliced into `cypher('<name>', …)` as a SQL
        // literal — identifier shape keeps it inert belt-and-braces (the
        // literal is also quote-escaped at the call site).
        if self.graph_name.is_empty()
            || !self
                .graph_name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(GraphError::InvalidConfig {
                name: name.to_string(),
                reason: format!(
                    "graph_name '{}' must be a bare identifier ([A-Za-z0-9_]+)",
                    self.graph_name
                ),
            });
        }
        for (field, value) in [
            ("username_env", &self.username_env),
            ("password_env", &self.password_env),
        ] {
            if let Some(v) = value
                && !is_env_var_name(v)
            {
                return Err(GraphError::InvalidConfig {
                    name: name.to_string(),
                    reason: format!(
                        "{field} '{v}' must be an environment variable NAME \
                         ([A-Za-z_][A-Za-z0-9_]*)"
                    ),
                });
            }
        }
        if self.max_rows == 0 {
            return Err(GraphError::InvalidConfig {
                name: name.to_string(),
                reason: "max_rows must be positive".to_string(),
            });
        }
        if self.query_timeout_seconds == 0 || self.query_timeout_seconds > MAX_QUERY_TIMEOUT_SECONDS
        {
            return Err(GraphError::InvalidConfig {
                name: name.to_string(),
                reason: format!(
                    "query_timeout_seconds must be in 1..={MAX_QUERY_TIMEOUT_SECONDS} \
                     (got {}) — the value feeds Postgres's statement_timeout and the \
                     client-side wrap",
                    self.query_timeout_seconds
                ),
            });
        }
        if self.max_connections == 0 {
            return Err(GraphError::InvalidConfig {
                name: name.to_string(),
                reason: "max_connections must be positive".to_string(),
            });
        }
        Ok(())
    }
}

fn is_env_var_name(s: &str) -> bool {
    let mut chars = s.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> GraphConfig {
        serde_yaml::from_str(
            r#"
backend: age
graph_name: knowledge
username_env: AGE_PG_USER
password_env: AGE_PG_PASS
"#,
        )
        .expect("parses")
    }

    #[test]
    fn defaults_and_valid_config_pass() {
        let c = base();
        assert_eq!(c.query_timeout_seconds, DEFAULT_QUERY_TIMEOUT_SECONDS);
        assert_eq!(c.max_rows, DEFAULT_MAX_ROWS);
        c.validate("kg", "postgres://localhost:5432/graphrag")
            .expect("valid");
        c.validate("kg", "postgresql://h/db").expect("valid");
    }

    #[test]
    fn non_age_backends_and_wrong_schemes_are_named_errors() {
        let mut c = base();
        c.backend = "neo4j".into();
        let err = c.validate("kg", "postgres://h/db").unwrap_err();
        assert!(err.to_string().contains("later milestones"), "{err}");

        let c = base();
        let err = c.validate("kg", "bolt://localhost:7687").unwrap_err();
        assert!(err.to_string().contains("postgres://"), "{err}");
    }

    #[test]
    fn graph_name_and_env_names_are_shape_checked() {
        let mut c = base();
        c.graph_name = "bad-name".into();
        assert!(c.validate("kg", "postgres://h/db").is_err());

        let mut c = base();
        c.username_env = Some("BAD NAME".into());
        let err = c.validate("kg", "postgres://h/db").unwrap_err();
        assert!(err.to_string().contains("environment variable NAME"));
    }

    #[test]
    fn url_embedded_passwords_are_rejected_without_echoing_the_url() {
        let c = base();
        for url in [
            "postgres://user:s3cret@localhost:5432/db",
            "postgresql://h/db?password=s3cret",
            "postgres://h/db?PASSWORD=s3cret",
        ] {
            let err = c.validate("kg", url).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("password_env"), "{msg}");
            assert!(!msg.contains("s3cret"), "the secret never echoes: {msg}");
        }
        // A bare username is identity, not a secret — allowed.
        c.validate("kg", "postgres://postgres@localhost:5432/db")
            .expect("username-only URL is fine");
    }

    #[test]
    fn timeout_has_a_hard_ceiling() {
        let mut c = base();
        c.query_timeout_seconds = MAX_QUERY_TIMEOUT_SECONDS + 1;
        let err = c.validate("kg", "postgres://h/db").unwrap_err();
        assert!(err.to_string().contains("1..=86400"), "{err}");
        c.query_timeout_seconds = u64::MAX; // would overflow the +5s wrap
        assert!(c.validate("kg", "postgres://h/db").is_err());
        c.query_timeout_seconds = MAX_QUERY_TIMEOUT_SECONDS;
        c.validate("kg", "postgres://h/db")
            .expect("the ceiling itself is legal");
    }

    #[test]
    fn unknown_fields_are_rejected_at_parse() {
        let err = serde_yaml::from_str::<GraphConfig>("backend: age\ngraph_name: g\nviews: []\n")
            .unwrap_err();
        assert!(err.to_string().contains("views"), "{err}");
    }
}
