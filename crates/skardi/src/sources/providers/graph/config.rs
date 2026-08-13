//! Typed configuration for a `type: graph` data source (design
//! §GraphConfig typed YAML). Milestones 1-2 support the `age` and
//! `neo4j` backends; views land with milestone 4.

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
/// Upper bound on the configurable row cap: one million rows. The knob
/// maps to MEMORY, not just wire traffic — the milestone-1 client
/// buffers the whole result (JSON rows, then the collected stream, then
/// every RecordBatch) before the first batch is emitted, so peak
/// resident memory is a small multiple of the result set and scales
/// linearly with this value. Bounded by construction beats bounded by
/// review: an accidental `max_rows: 50000000` must be a config error,
/// not an in-process OOM.
pub const MAX_MAX_ROWS: usize = 1_000_000;
/// Default connection-pool size.
pub const DEFAULT_MAX_CONNECTIONS: u32 = 4;
/// Upper bound on the pool size — same bounded-by-construction
/// principle; anything past this is far beyond a Postgres default
/// (`max_connections = 100`) and reads like a typo'd row cap.
pub const MAX_MAX_CONNECTIONS: u32 = 64;

/// The `graph:` block of a `type: graph` data source.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphConfig {
    /// Backend engine: `age` (openCypher inside Postgres) or `neo4j`
    /// (Bolt).
    pub backend: String,
    /// What it names is per backend: AGE graphs are named per database,
    /// so `age` REQUIRES it; on `neo4j` it selects the database and may
    /// be omitted for the server default (the design's neo4j example
    /// carries no graph_name).
    #[serde(default)]
    pub graph_name: Option<String>,
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
    /// never a silent truncation. This is a MEMORY knob: the client
    /// buffers the whole result before emitting (see [`MAX_MAX_ROWS`]).
    #[serde(default = "default_max_rows")]
    pub max_rows: usize,
    /// Connection-pool size against the backend.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// PARSED but not yet supported: YAML catalog views are milestone 4.
    /// The field exists so an operator copying the design doc's example
    /// gets "views arrive with milestone 4" from validation — not
    /// serde's "unknown field `views`", which reads as "skardi doesn't
    /// support views". Accepted-and-ignored would be worse: a declared
    /// view that silently does nothing.
    #[serde(default)]
    pub views: Option<serde_yaml::Value>,
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
        // ONE rules row per backend — the scheme list drives both the
        // check and the error text, and the graph_name rule rides along,
        // so milestone 3's kuzu is one new row here instead of parallel
        // matches that can drift.
        let rules = match self.backend.as_str() {
            "age" => BackendRules {
                schemes: &["postgres://", "postgresql://"],
                // Spliced into `cypher('<name>', …)` as a SQL literal —
                // identifier shape keeps it inert belt-and-braces (the
                // literal is also quote-escaped at the call site).
                name_required: true,
                name_extra_chars: &[],
                name_shape: "a bare identifier ([A-Za-z0-9_]+)",
            },
            "neo4j" => BackendRules {
                schemes: &["bolt://", "bolt+s://", "neo4j://", "neo4j+s://"],
                // Optional (the server default database); travels as
                // driver-bound Bolt metadata, never query text — the
                // shape check (Neo4j's own name alphabet) keeps typos
                // loud, nothing more.
                name_required: false,
                name_extra_chars: &['.', '-'],
                name_shape: "a database name ([A-Za-z0-9_.-]+)",
            },
            other => {
                return Err(GraphError::InvalidConfig {
                    name: name.to_string(),
                    reason: format!(
                        "backend '{other}' is not supported (milestones 1-2 support: \
                         age, neo4j; kuzu is a later milestone)"
                    ),
                });
            }
        };
        if !rules
            .schemes
            .iter()
            .any(|scheme| connection_string.starts_with(scheme))
        {
            return Err(GraphError::InvalidConfig {
                name: name.to_string(),
                reason: format!(
                    "the {} backend requires a {} connection_string",
                    self.backend,
                    rules.schemes.join(" / ")
                ),
            });
        }
        // Credentials travel as env-var NAMES only (username_env /
        // password_env) — a password embedded in the URL would sit in
        // config repos, deploy logs, and diagnostics. Parsed, not
        // substring-matched, so `:` in a database name cannot
        // false-positive. FAIL-CLOSED: a string this parser cannot read
        // is a config error, never a skipped check — the url crate and
        // the driver's own parser are different implementations, and a
        // value only the driver accepts would otherwise carry a secret
        // past a validator that reported success. The scheme allowlist
        // above already guarantees the string is URL-shaped, so nothing
        // legitimate lands here. The error never echoes the URL (it may
        // carry the very secret being rejected).
        let parsed = url::Url::parse(connection_string).map_err(|_| GraphError::InvalidConfig {
            name: name.to_string(),
            reason: "connection_string is not a parseable URL (the embedded-credential \
                     check could not run, so the value is rejected rather than passed \
                     through unvetted)"
                .to_string(),
        })?;
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
        match self.graph_name.as_deref() {
            None if rules.name_required => {
                return Err(GraphError::InvalidConfig {
                    name: name.to_string(),
                    reason: "the age backend requires graph_name (AGE graphs are named \
                             per database)"
                        .to_string(),
                });
            }
            Some(graph_name)
                if graph_name.is_empty()
                    || !graph_name.chars().all(|c| {
                        c.is_ascii_alphanumeric() || c == '_' || rules.name_extra_chars.contains(&c)
                    }) =>
            {
                return Err(GraphError::InvalidConfig {
                    name: name.to_string(),
                    reason: format!(
                        "graph_name '{graph_name}' must be {} on the {} backend",
                        rules.name_shape, self.backend
                    ),
                });
            }
            _ => {}
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
        if self.max_rows == 0 || self.max_rows > MAX_MAX_ROWS {
            return Err(GraphError::InvalidConfig {
                name: name.to_string(),
                reason: format!(
                    "max_rows must be in 1..={MAX_MAX_ROWS} (got {}) — the milestone-1 \
                     client buffers the whole result, so this knob is peak memory, and \
                     it gets the same hard ceiling the timeout got",
                    self.max_rows
                ),
            });
        }
        if self.query_timeout_seconds == 0 || self.query_timeout_seconds > MAX_QUERY_TIMEOUT_SECONDS
        {
            return Err(GraphError::InvalidConfig {
                name: name.to_string(),
                reason: format!(
                    "query_timeout_seconds must be in 1..={MAX_QUERY_TIMEOUT_SECONDS} \
                     (got {}) — the value feeds the backend's server-side timeout \
                     (statement_timeout / tx_timeout) and the client-side wrap",
                    self.query_timeout_seconds
                ),
            });
        }
        if self.max_connections == 0 || self.max_connections > MAX_MAX_CONNECTIONS {
            return Err(GraphError::InvalidConfig {
                name: name.to_string(),
                reason: format!(
                    "max_connections must be in 1..={MAX_MAX_CONNECTIONS} (got {})",
                    self.max_connections
                ),
            });
        }
        if self.views.is_some() {
            return Err(GraphError::InvalidConfig {
                name: name.to_string(),
                reason: "`views` arrive with milestone 4 (YAML catalog views); the \
                         ad-hoc surface today is cypher_query(...) — remove the views \
                         block until then"
                    .to_string(),
            });
        }
        Ok(())
    }
}

/// Per-backend validation rules — one row per backend in `validate`, so
/// the scheme allowlist, its error text, and the graph_name contract
/// cannot drift apart across parallel matches.
struct BackendRules {
    schemes: &'static [&'static str],
    name_required: bool,
    name_extra_chars: &'static [char],
    name_shape: &'static str,
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
    fn unknown_backends_and_wrong_schemes_are_named_errors() {
        let mut c = base();
        c.backend = "kuzu".into();
        let err = c.validate("kg", "postgres://h/db").unwrap_err();
        assert!(err.to_string().contains("later milestone"), "{err}");

        // Cross-backend URLs are misconfigurations, both directions.
        let c = base();
        let err = c.validate("kg", "bolt://localhost:7687").unwrap_err();
        assert!(err.to_string().contains("postgres://"), "{err}");
        let mut c = base();
        c.backend = "neo4j".into();
        let err = c.validate("kg", "postgres://h/db").unwrap_err();
        assert!(err.to_string().contains("bolt://"), "{err}");
    }

    #[test]
    fn neo4j_accepts_bolt_schemes_and_an_optional_database() {
        let mut c = base();
        c.backend = "neo4j".into();
        for url in [
            "bolt://localhost:7687",
            "bolt+s://h:7687",
            "neo4j://h:7687",
            "neo4j+s://h:7687",
        ] {
            c.validate("kg", url).expect(url);
        }
        // graph_name is the database selector there — optional, and
        // Neo4j's own name alphabet (dots, dashes) is legal.
        c.graph_name = None;
        c.validate("kg", "bolt://h:7687")
            .expect("default database needs no graph_name");
        c.graph_name = Some("my-db.prod".into());
        c.validate("kg", "bolt://h:7687")
            .expect("neo4j db alphabet");
        c.graph_name = Some("bad name".into());
        let err = c.validate("kg", "bolt://h:7687").unwrap_err();
        assert!(err.to_string().contains("database name"), "{err}");
        // The +ssc self-signed variants are deliberately NOT allowlisted.
        c.graph_name = None;
        let err = c.validate("kg", "bolt+ssc://h:7687").unwrap_err();
        assert!(err.to_string().contains("bolt://"), "{err}");
    }

    #[test]
    fn age_requires_a_graph_name_and_neo4j_dots_stay_illegal_there() {
        let mut c = base();
        c.graph_name = None;
        let err = c.validate("kg", "postgres://h/db").unwrap_err();
        assert!(err.to_string().contains("requires graph_name"), "{err}");
        let mut c = base();
        c.graph_name = Some("my.graph".into());
        let err = c.validate("kg", "postgres://h/db").unwrap_err();
        assert!(err.to_string().contains("bare identifier"), "{err}");
    }

    #[test]
    fn graph_name_and_env_names_are_shape_checked() {
        let mut c = base();
        c.graph_name = Some("bad-name".into());
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
        // FAIL-CLOSED: a URL the checker cannot parse is rejected, not
        // waved past the credential checks (the url crate and the
        // driver parse differently). Scheme-prefixed so it reaches the
        // parse step, then malformed.
        let err = c
            .validate("kg", "postgres://[not-a-host/db?password=s3cret")
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("not a parseable URL"), "{msg}");
        assert!(!msg.contains("s3cret"), "never echoed: {msg}");
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
    fn remaining_bounds_are_validated() {
        let mut c = base();
        c.max_rows = 0;
        assert!(
            c.validate("kg", "postgres://h/db")
                .unwrap_err()
                .to_string()
                .contains("max_rows")
        );
        let mut c = base();
        c.max_connections = 0;
        assert!(
            c.validate("kg", "postgres://h/db")
                .unwrap_err()
                .to_string()
                .contains("max_connections")
        );
        let mut c = base();
        c.graph_name = Some(String::new());
        assert!(
            c.validate("kg", "postgres://h/db")
                .unwrap_err()
                .to_string()
                .contains("bare identifier")
        );
        let mut c = base();
        c.password_env = Some("2BAD".into());
        assert!(c.validate("kg", "postgres://h/db").is_err());
    }

    #[test]
    fn unknown_fields_are_rejected_at_parse() {
        let err = serde_yaml::from_str::<GraphConfig>("backend: age\ngraph_name: g\nviewz: []\n")
            .unwrap_err();
        assert!(err.to_string().contains("viewz"), "{err}");
    }

    #[test]
    fn views_parse_but_are_named_as_milestone_4_work() {
        // The design doc's own example carries `views:` — an operator
        // copying it must get "milestone 4", not serde's unknown-field
        // error (which reads as "skardi doesn't support views").
        let c: GraphConfig = serde_yaml::from_str(
            "backend: age\ngraph_name: g\nviews:\n  - name: user_posts\n    cypher: MATCH (n) RETURN n\n",
        )
        .expect("the field parses");
        let err = c.validate("kg", "postgres://h/db").unwrap_err();
        assert!(err.to_string().contains("milestone 4"), "{err}");
    }

    #[test]
    fn max_rows_and_max_connections_have_hard_ceilings() {
        // The row cap is a MEMORY knob under the fully-buffering client
        // — an accidental 50M must be a config error, not an OOM.
        let mut c = base();
        c.max_rows = MAX_MAX_ROWS + 1;
        let err = c.validate("kg", "postgres://h/db").unwrap_err();
        assert!(err.to_string().contains("1..=1000000"), "{err}");
        c.max_rows = MAX_MAX_ROWS;
        c.validate("kg", "postgres://h/db")
            .expect("the ceiling itself is legal");

        let mut c = base();
        c.max_connections = MAX_MAX_CONNECTIONS + 1;
        let err = c.validate("kg", "postgres://h/db").unwrap_err();
        assert!(err.to_string().contains("1..=64"), "{err}");
    }
}
