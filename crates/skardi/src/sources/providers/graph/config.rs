//! Typed configuration for a `type: graph` data source (design
//! §GraphConfig typed YAML). Milestone 1 supports the `age` backend;
//! milestone 4 adds YAML catalog views.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use super::error::GraphError;
use super::value::{ACCEPTED_TYPES, DeclaredColumn, GraphType};

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
///
/// `Serialize` because the server's `DataSource` struct serializes its
/// typed blocks (the rss/open_connector precedent).
#[derive(Debug, Clone, Deserialize, Serialize)]
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
    /// never a silent truncation. This is a MEMORY knob: the client
    /// buffers the whole result before emitting (see [`MAX_MAX_ROWS`]).
    #[serde(default = "default_max_rows")]
    pub max_rows: usize,
    /// Connection-pool size against the backend.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// YAML catalog views (milestone 4): each becomes the catalog table
    /// `<source>.main.<name>` — fixed Cypher plus a declared schema.
    #[serde(default)]
    pub views: Vec<GraphView>,
}

/// One YAML-declared catalog view.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphView {
    /// Table name inside the source's `main` schema.
    pub name: String,
    /// The Cypher executed at scan time. Screened by the keyword guard
    /// at validation, like every caller-authored Cypher — see
    /// [`GraphConfig::validate`].
    pub cypher: String,
    /// Declared output columns, in RETURN order (the binding to the
    /// Cypher RETURN clause is positional — same rule as the ad-hoc
    /// `columns` argument).
    pub schema: Vec<GraphViewColumn>,
}

/// One declared view column.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphViewColumn {
    pub name: String,
    /// The friendly lowercase type vocabulary (`string`, `int`, `node`,
    /// …) — one spelling shared with the ad-hoc `columns` argument.
    #[serde(rename = "type")]
    pub r#type: String,
    /// Defaults to true; `false` is the author's assertion that the
    /// view's Cypher never yields null here, enforced at scan time.
    #[serde(default = "default_nullable")]
    pub nullable: bool,
}

fn default_nullable() -> bool {
    true
}

impl GraphView {
    /// The declared columns in conversion form. Type names re-parse here
    /// (the error path is defensive: [`GraphConfig::validate`] runs
    /// before registration and rejects unparseable types first).
    pub fn declared_columns(&self) -> Result<Vec<DeclaredColumn>, GraphError> {
        self.schema
            .iter()
            .map(|c| {
                Ok(DeclaredColumn {
                    name: c.name.clone(),
                    ty: GraphType::parse(&c.r#type).ok_or_else(|| GraphError::InvalidConfig {
                        name: self.name.clone(),
                        reason: format!(
                            "column '{}' declares unknown type '{}' (accepted types: {})",
                            c.name, c.r#type, ACCEPTED_TYPES
                        ),
                    })?,
                    nullable: c.nullable,
                })
            })
            .collect()
    }
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
    /// credentials, and structural checks for every declared view.
    /// `connection_string` is operator trust (the same tier
    /// as a Postgres connection string in the same file) — no SSRF guard,
    /// only a scheme allowlist (design §Security).
    ///
    /// View Cypher IS screened by the keyword guard, per the design's
    /// §Security scope ("the text passed to `cypher_query` or declared
    /// in a view"): the backend's READ ONLY transaction stops writes,
    /// but the guard's `CALL`/`LOAD` arms exist for what a read
    /// transaction does NOT cover — procedure escape hatches and
    /// backend-side URL fetches. The known tax carries over from the
    /// ad-hoc surface: a keyword-shaped string literal in a view's
    /// Cypher false-positives, and the author rephrases.
    pub fn validate(&self, name: &str, connection_string: &str) -> Result<(), GraphError> {
        // The source name becomes a CATALOG name; `datafusion` and
        // `information_schema` are DataFusion's built-ins, and
        // `register_catalog` replaces unconditionally — a source with
        // either name would silently clobber the default catalog (and
        // every table in it).
        if matches!(name, "datafusion" | "information_schema") {
            return Err(GraphError::InvalidConfig {
                name: name.to_string(),
                reason: "the source name is reserved: it would replace DataFusion's \
                         built-in catalog of the same name (register_catalog replaces \
                         unconditionally); choose another name"
                    .to_string(),
            });
        }
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
                && !is_identifier(v)
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
                     (got {}) — the value feeds Postgres's statement_timeout and the \
                     client-side wrap",
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
        let mut view_names = HashSet::new();
        for view in &self.views {
            // The view name becomes the catalog table name
            // `<source>.main.<name>` — bare identifier shape keeps it
            // addressable without quoting everywhere downstream.
            if !is_identifier(&view.name) {
                return Err(GraphError::InvalidConfig {
                    name: name.to_string(),
                    reason: format!(
                        "view name '{}' must be an identifier ([A-Za-z_][A-Za-z0-9_]*) — \
                         it becomes the catalog table name {name}.main.{}",
                        view.name, view.name
                    ),
                });
            }
            if !view_names.insert(&view.name) {
                return Err(GraphError::InvalidConfig {
                    name: name.to_string(),
                    reason: format!("duplicate view name '{}'", view.name),
                });
            }
            if view.cypher.trim().is_empty() {
                return Err(GraphError::InvalidConfig {
                    name: name.to_string(),
                    reason: format!("view '{}' declares empty cypher", view.name),
                });
            }
            // The keyword guard, same as the ad-hoc surface (design
            // §Security names views explicitly). READ ONLY transactions
            // stop writes; this stops what they don't — CALL procedure
            // escapes and LOAD's server-side URL fetch.
            super::guard::reject_mutations(&view.cypher).map_err(|e| {
                GraphError::InvalidConfig {
                    name: name.to_string(),
                    reason: format!("view '{}': {e}", view.name),
                }
            })?;
            if view.schema.is_empty() {
                return Err(GraphError::InvalidConfig {
                    name: name.to_string(),
                    reason: format!(
                        "view '{}' declares an empty schema — at least one column is \
                         required (the declared schema is the planning-time contract)",
                        view.name
                    ),
                });
            }
            let mut column_names = HashSet::new();
            for column in &view.schema {
                if column.name.trim().is_empty() {
                    return Err(GraphError::InvalidConfig {
                        name: name.to_string(),
                        reason: format!(
                            "view '{}' declares a column with an empty name",
                            view.name
                        ),
                    });
                }
                if !column_names.insert(&column.name) {
                    return Err(GraphError::InvalidConfig {
                        name: name.to_string(),
                        reason: format!(
                            "view '{}' declares column '{}' twice",
                            view.name, column.name
                        ),
                    });
                }
                if GraphType::parse(&column.r#type).is_none() {
                    return Err(GraphError::InvalidConfig {
                        name: name.to_string(),
                        reason: format!(
                            "view '{}' column '{}': unknown type '{}' (accepted types: {})",
                            view.name, column.name, column.r#type, ACCEPTED_TYPES
                        ),
                    });
                }
            }
        }
        Ok(())
    }
}

fn is_identifier(s: &str) -> bool {
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
    fn reserved_catalog_names_are_rejected() {
        // The source name becomes a catalog name, and register_catalog
        // replaces unconditionally — `datafusion` would clobber the
        // built-in catalog with every table in it.
        let c = base();
        for reserved in ["datafusion", "information_schema"] {
            let err = c.validate(reserved, "postgres://h/db").unwrap_err();
            assert!(err.to_string().contains("reserved"), "{err}");
        }
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
        c.graph_name = String::new();
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
    fn view_cypher_is_screened_by_the_keyword_guard() {
        // Design §Security names view-declared Cypher as guard scope:
        // READ ONLY transactions stop writes, the guard stops CALL/LOAD —
        // the escape hatches a read transaction does not cover.
        for (cypher, keyword) in [
            ("CREATE (n:X) RETURN n", "'CREATE'"),
            ("CALL db.labels()", "'CALL'"),
            ("LOAD CSV FROM 'https://x' AS row RETURN row", "'LOAD'"),
        ] {
            let c: GraphConfig = serde_yaml::from_str(&format!(
                "backend: age
graph_name: g
views:
  - name: v
    cypher: \"{cypher}\"
    schema:
      - name: x
        type: string
"
            ))
            .expect("parses");
            let err = c.validate("kg", "postgres://h/db").unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("view 'v'"), "{cypher}: {msg}");
            assert!(msg.contains(keyword), "{cypher}: {msg}");
        }
    }

    #[test]
    fn views_parse_validate_and_default_nullable_to_true() {
        // The design doc's own example shape — parse, validate, and the
        // nullable default all in one.
        let c: GraphConfig = serde_yaml::from_str(
            r#"
backend: age
graph_name: g
views:
  - name: user_posts
    cypher: MATCH (u:User)-[:POSTED]->(p:Post) RETURN u.name AS user_name, p.title AS post_title
    schema:
      - name: user_name
        type: string
      - name: post_title
        type: string
        nullable: false
"#,
        )
        .expect("views parse");
        assert_eq!(c.views.len(), 1);
        assert!(c.views[0].schema[0].nullable, "nullable defaults to true");
        assert!(!c.views[0].schema[1].nullable);
        c.validate("kg", "postgres://h/db").expect("valid");
        let columns = c.views[0].declared_columns().expect("types parse");
        assert_eq!(columns[1].name, "post_title");
        assert!(!columns[1].nullable);
    }

    #[test]
    fn view_names_must_be_unique_identifiers() {
        for bad in ["user-posts", "1posts", "", "user posts"] {
            let mut c = base();
            c.views = vec![view(bad, "MATCH (n) RETURN n", vec![column("n", "int")])];
            let err = c.validate("kg", "postgres://h/db").unwrap_err();
            assert!(err.to_string().contains("identifier"), "{bad}: {err}");
        }
        let mut c = base();
        c.views = vec![
            view("v", "MATCH (n) RETURN n", vec![column("n", "int")]),
            view("v", "MATCH (m) RETURN m", vec![column("m", "int")]),
        ];
        let err = c.validate("kg", "postgres://h/db").unwrap_err();
        assert!(err.to_string().contains("duplicate view name 'v'"), "{err}");
    }

    #[test]
    fn view_cypher_and_schema_must_be_nonempty() {
        let mut c = base();
        c.views = vec![view("v", "   \n ", vec![column("n", "int")])];
        let err = c.validate("kg", "postgres://h/db").unwrap_err();
        assert!(err.to_string().contains("empty cypher"), "{err}");

        let mut c = base();
        c.views = vec![view("v", "MATCH (n) RETURN n", vec![])];
        let err = c.validate("kg", "postgres://h/db").unwrap_err();
        assert!(err.to_string().contains("empty schema"), "{err}");
    }

    #[test]
    fn view_columns_must_be_named_unique_and_typed() {
        let mut c = base();
        c.views = vec![view("v", "MATCH (n) RETURN n", vec![column(" ", "int")])];
        let err = c.validate("kg", "postgres://h/db").unwrap_err();
        assert!(err.to_string().contains("empty name"), "{err}");

        let mut c = base();
        c.views = vec![view(
            "v",
            "MATCH (n) RETURN n",
            vec![column("n", "int"), column("n", "string")],
        )];
        let err = c.validate("kg", "postgres://h/db").unwrap_err();
        assert!(err.to_string().contains("twice"), "{err}");

        let mut c = base();
        c.views = vec![view("v", "MATCH (n) RETURN n", vec![column("n", "Utf8")])];
        let err = c.validate("kg", "postgres://h/db").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown type 'Utf8'"), "{msg}");
        assert!(msg.contains("node, relationship, path"), "{msg}");
    }

    fn view(name: &str, cypher: &str, schema: Vec<GraphViewColumn>) -> GraphView {
        GraphView {
            name: name.to_string(),
            cypher: cypher.to_string(),
            schema,
        }
    }

    fn column(name: &str, ty: &str) -> GraphViewColumn {
        GraphViewColumn {
            name: name.to_string(),
            r#type: ty.to_string(),
            nullable: true,
        }
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
