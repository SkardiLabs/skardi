//! The shared `X-Skardi-Session-Id` request header.
//!
//! Lives in the server rather than beside the ledger it feeds: the header is
//! an HTTP concern (`axum::http::HeaderMap`), and `skardi-query-audit` was
//! extracted precisely so downstream distributions could adopt the ledger
//! without inheriting this crate's dependency tree (#206). The cap it
//! validates against, `MAX_SESSION_ID_CHARS`, still comes from the ledger —
//! that is the value all three attribution paths must agree on.
//!
//! Three consumers: the pipeline execute endpoint, the jobs submit endpoint,
//! and — for the cap only — `/query`'s `ai_context.session_id`.

use axum::http::HeaderMap;

use crate::query_audit::MAX_SESSION_ID_CHARS;

/// Optional caller-supplied session header. A header (not a body field)
/// because the pipeline-execute body IS the flattened parameter map — a
/// reserved key could collide with a legitimate SQL parameter of the same
/// name. The jobs submit endpoint shares the header for the same reason:
/// its body is the job's parameter map.
///
/// Not an auth credential: unrelated to `require_session`. The value is
/// caller-asserted, so ledger session attribution is self-reported rather
/// than derived from an authenticated principal — the same property as
/// `/query`'s `ai_context.session_id`.
pub const SESSION_ID_HEADER: &str = "x-skardi-session-id";

/// Extract and validate the session header. `Ok(None)` when absent; `Err`
/// when present but malformed — silently dropping a malformed value would
/// corrupt session stitching, the one job this field has.
///
/// Allowed characters are visible ASCII graphic characters, excluding comma —
/// no space, no tab, no other whitespace or control character. Space is
/// rejected (not just trimmed) because `httparse` strips leading/trailing
/// whitespace per RFC 9110 §5.5 before this code ever sees the value: if
/// spaces were merely tolerated, `"  sess-1  "` would be recorded under the
/// different key `sess-1` (silently breaking the session stitching this
/// field exists for) while `"   "` would 400 here — the exact fail-late
/// round trip a client-side check exists to prevent. A grouping key has no
/// legitimate use for spaces, so this rejects them outright instead of
/// defining trimming semantics. Commas are rejected because RFC 9110 §5.3
/// lets any intermediary merge repeated header lines into one comma-joined
/// value, which would otherwise slip a duplicate past the check below as a
/// single merged `"id1, id2"`. This predicate is mirrored exactly by the
/// CLI's own check in `crates/cli/src/session.rs`.
pub fn session_id_from_headers(headers: &HeaderMap) -> Result<Option<String>, String> {
    let mut values = headers.get_all(SESSION_ID_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(format!(
            "{SESSION_ID_HEADER} must not be sent more than once"
        ));
    }
    let s = value
        .to_str()
        .map_err(|_| format!("{SESSION_ID_HEADER} must contain only visible ASCII characters"))?;
    validate_session_id(s)?;
    Ok(Some(s.to_string()))
}

/// Validate one session-id VALUE against the rules above, independent of the
/// header plumbing. Public so the server's `/mcp` handler can vet a
/// caller-minted `Mcp-Session-Id` against the same predicate before
/// forwarding it as `x-skardi-session-id` (falling back to a minted UUID on
/// mismatch — losing that call's session grouping, not the call).
pub fn validate_session_id(s: &str) -> Result<(), String> {
    if !s.chars().all(|c| c.is_ascii_graphic() && c != ',') {
        return Err(format!(
            "{SESSION_ID_HEADER} must contain only visible ASCII characters, \
             with no spaces and no commas (proxies may merge repeated header \
             lines into one comma-separated value)"
        ));
    }
    if s.is_empty() {
        return Err(format!("{SESSION_ID_HEADER} must not be empty"));
    }
    if s.chars().count() > MAX_SESSION_ID_CHARS {
        return Err(format!(
            "{SESSION_ID_HEADER} must be at most {MAX_SESSION_ID_CHARS} characters"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_session_id_matches_the_header_rules() {
        assert!(validate_session_id("sess-1").is_ok());
        assert!(validate_session_id("").is_err());
        assert!(validate_session_id("has space").is_err());
        assert!(validate_session_id("a,b").is_err());
        assert!(validate_session_id(&"x".repeat(MAX_SESSION_ID_CHARS + 1)).is_err());
        assert!(validate_session_id(&"x".repeat(MAX_SESSION_ID_CHARS)).is_ok());
    }
}
