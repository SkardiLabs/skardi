//! Error taxonomy for the graph engine bypass (design
//! `docs/superpowers/specs/2026-08-08-graph-engine-bypass-design.md`
//! §Error handling).
//!
//! Errors carry identity — connection, column, row, keyword — and JSON
//! *kinds*, never values: row data and query text (whose inline literals
//! are values) never appear in a rendered message, so nothing sensitive
//! leaks into logs or agent prompts.

use thiserror::Error;

/// How many bytes of a backend error message survive into ours. Backend
/// messages can embed query text, so the snippet is length-capped, never
/// forwarded whole.
const BACKEND_MESSAGE_CAP: usize = 300;

/// Errors surfaced by graph sources and the `cypher_query` /
/// `graph_schema` UDTFs.
#[derive(Debug, Error)]
pub enum GraphError {
    /// The `connection` argument named no registered `type: graph` source.
    #[error(
        "graph connection '{name}' is not registered (known connections: {known}); \
         declare a `type: graph` data source with that name"
    )]
    ConnectionNotFound { name: String, known: String },

    /// A config field failed validation at load/registration time.
    #[error("graph source '{name}': {reason}")]
    InvalidConfig { name: String, reason: String },

    /// A YAML view failed its live validation — at registration, or at
    /// the degraded recovery retry. The failing identity is a VIEW, not
    /// a source, so it does not ride [`GraphError::InvalidConfig`]
    /// (whose rendering would call it one).
    #[error("graph view '{view}' failed validation: {reason}")]
    ViewValidationFailed { view: String, reason: String },

    /// The fast-path keyword guard blocked caller-supplied Cypher. Names
    /// the blocked keyword and its byte offset — NEVER the query text
    /// (inline Cypher literals are values).
    #[error(
        "cypher_query is read-only: the keyword '{keyword}' at byte {offset} is not \
         allowed (writes and procedure calls are rejected; the backend's READ ONLY \
         transaction enforces this regardless)"
    )]
    MutationRejected {
        keyword: &'static str,
        offset: usize,
    },

    /// The declared `columns` argument failed to parse or used an unknown
    /// type name.
    #[error("cypher_query 'columns': {reason}; accepted types: {accepted}")]
    InvalidColumns {
        reason: String,
        accepted: &'static str,
    },

    /// The `params` argument was not a JSON object.
    #[error("cypher_query 'params' must be a JSON object, got {found}")]
    InvalidParams { found: String },

    /// A returned value did not convert to its declared column type.
    /// Carries the JSON *kind* found, never the value.
    #[error(
        "graph column '{column}' at result row {row}: declared '{expected}' but the \
         backend returned {found}; declare the column as 'json' (verbatim) or \
         normalize in Cypher (toString()/toInteger())"
    )]
    TypeMismatch {
        column: String,
        row: usize,
        expected: &'static str,
        found: &'static str,
    },

    /// A column declared `nullable: false` (YAML views only — the ad-hoc
    /// surface cannot declare it) met a null from the backend. Identity
    /// only, never the value.
    #[error(
        "graph column '{column}' at result row {row}: declared nullable: false but the \
         backend returned null; relax the declaration to nullable: true or filter \
         nulls in the view's Cypher (e.g. WHERE ... IS NOT NULL)"
    )]
    NotNullViolation { column: String, row: usize },

    /// The scan hit the per-source row cap. Loud and typed — never a
    /// silent truncation.
    #[error(
        "graph scan exceeded max_rows = {max_rows}: the backend returned more rows \
         than the source allows per query; add a Cypher LIMIT or raise the \
         source's max_rows"
    )]
    RowCapExceeded { max_rows: usize },

    /// The backend did not answer within the per-source timeout.
    #[error(
        "graph query timed out after {seconds}s (the source's query_timeout_seconds); \
         narrow the traversal or raise the timeout"
    )]
    Timeout { seconds: u64 },

    /// No pooled connection became available within the bound. sqlx
    /// retries a refused dial until the acquire deadline and then
    /// surfaces `PoolTimedOut`, so an UNREACHABLE backend lands here —
    /// not in a dial error. Distinct from [`GraphError::Timeout`]: the
    /// query never started, so "narrow the traversal" would be
    /// actively misleading advice.
    #[error(
        "could not acquire a connection to the graph backend within {seconds}s: the \
         backend may be unreachable, or every pooled connection is checked out (the \
         query never started)"
    )]
    ConnectionAcquireTimeout { seconds: u64 },

    /// The backend could not be REACHED at registration — a connectivity
    /// failure (DNS, refused dial, network timeout), meaning no server
    /// answered at all. This is the ONLY variant that qualifies for
    /// degraded registration: anything the server answered (bad
    /// credentials, missing extension, missing graph) is a configuration
    /// problem that must fail startup loudly, never degrade.
    #[error("graph backend '{source_name}' is unreachable: {reason}")]
    Unavailable { source_name: String, reason: String },

    /// A driver/backend failure. `code` is the backend's error code
    /// verbatim; `message` is a bounded snippet (see
    /// [`GraphError::backend`]).
    #[error("graph backend error on '{source_name}' [{code}]: {message}")]
    Backend {
        source_name: String,
        code: String,
        message: String,
    },

    /// A backend row carried a different column count than the declared
    /// schema — driver drift, surfaced as a typed error instead of an
    /// index panic.
    #[error(
        "graph result row {row} carries {found} columns but {expected} were declared — \
         the backend and the declared schema disagree"
    )]
    RowArityMismatch {
        row: usize,
        expected: usize,
        found: usize,
    },

    /// A response cell was not parseable agtype/JSON. Carries position
    /// only.
    #[error(
        "graph response cell at row {row}, column {column} is not parseable agtype \
         ({reason})"
    )]
    MalformedCell {
        row: usize,
        column: usize,
        reason: String,
    },
}

impl GraphError {
    /// Build a [`GraphError::Backend`] with the message snippet bounded at
    /// a fixed byte cap (on a char boundary) — backend messages can embed
    /// query text, and query text can embed values.
    pub fn backend(source_name: &str, code: &str, message: &str) -> Self {
        let mut end = message.len().min(BACKEND_MESSAGE_CAP);
        while end < message.len() && !message.is_char_boundary(end) {
            end += 1;
        }
        let mut snippet = message[..end].to_string();
        if end < message.len() {
            snippet.push('…');
        }
        GraphError::Backend {
            source_name: source_name.to_string(),
            code: code.to_string(),
            message: snippet,
        }
    }
}

/// The JSON kind of a value, for type-mismatch diagnostics — kinds only,
/// never the value itself.
pub fn json_kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "a boolean",
        serde_json::Value::Number(_) => "a number",
        serde_json::Value::String(_) => "a string",
        serde_json::Value::Array(_) => "an array",
        serde_json::Value::Object(_) => "an object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_messages_are_bounded_on_char_boundaries() {
        let long = "颱".repeat(200); // 3 bytes each — 600 bytes
        let err = GraphError::backend("kg", "XX000", &long);
        let GraphError::Backend { message, .. } = &err else {
            panic!("backend variant");
        };
        assert!(message.len() <= BACKEND_MESSAGE_CAP + '…'.len_utf8() + 3);
        assert!(message.ends_with('…'), "truncation is visible");
        // No panic on the boundary is the real assertion; the display
        // renders the identity pieces.
        assert!(err.to_string().contains("[XX000]"));
    }

    #[test]
    fn short_backend_messages_pass_untruncated_and_kinds_cover_json() {
        let err = GraphError::backend("kg", "42601", "syntax error");
        assert!(err.to_string().contains("syntax error"), "{err}");
        assert!(!err.to_string().contains('…'), "no ellipsis when unclipped");
        for (v, kind) in [
            (serde_json::json!(null), "null"),
            (serde_json::json!(true), "a boolean"),
            (serde_json::json!(1), "a number"),
            (serde_json::json!("s"), "a string"),
            (serde_json::json!([1]), "an array"),
            (serde_json::json!({}), "an object"),
        ] {
            assert_eq!(json_kind(&v), kind);
        }
    }

    #[test]
    fn mutation_rejected_never_carries_query_text() {
        let err = GraphError::MutationRejected {
            keyword: "CREATE",
            offset: 42,
        };
        let msg = err.to_string();
        assert!(msg.contains("'CREATE'"));
        assert!(msg.contains("byte 42"));
    }
}
