//! Shared `--session-id` validation for commands that record an execution
//! against a session in the server's audit ledger (`skardi run`,
//! `skardi job run`). Both send the same `x-skardi-session-id` header on a
//! successful validation, so the rules live here once instead of being
//! duplicated per command.

use anyhow::{Result, anyhow};

/// Maximum session-id length, in characters. Restates the server's
/// `query_audit::MAX_SESSION_ID_CHARS` under the same name — `skardi-cli`
/// does not depend on the server crate, so this cannot be a shared item, but
/// an identical name keeps `grep MAX_SESSION_ID_CHARS` finding every site
/// that must move together.
pub(crate) const MAX_SESSION_ID_CHARS: usize = 200;

/// Validate a `--session-id` flag value before it is sent as the
/// `x-skardi-session-id` header.
///
/// Callers must validate before building the request: reqwest defers an
/// invalid header value to `send()`, whose errors are all mapped to
/// `ApiError::Connect` — so without this check a bad `--session-id` prints a
/// connection error naming a server that was never contacted, and exits
/// with the "server unreachable" code.
///
/// The rules are an EXACT mirror of the server's `session_id_from_headers`:
/// visible ASCII graphic characters, no comma, no space, plus the length cap
/// above — and no trimming semantics on either side. Space is rejected
/// rather than trimmed because the server sees whatever `httparse` hands it
/// after stripping leading/trailing whitespace (RFC 9110 §5.5): if space
/// were tolerated here, `--session-id "  sess-1  "` would be recorded
/// server-side under the different key `sess-1`, silently breaking the
/// session stitching this field exists for, while `--session-id "   "` would
/// pass here and 400 there — the exact fail-late round trip this check
/// exists to prevent. Commas are rejected because proxies may merge repeated
/// header lines into one comma-separated value (RFC 9110 §5.3).
pub(crate) fn validate_session_id(sid: &str) -> Result<()> {
    let invalid = sid.is_empty()
        || sid.chars().count() > MAX_SESSION_ID_CHARS
        || !sid.chars().all(|c| c.is_ascii_graphic() && c != ',');
    if invalid {
        return Err(anyhow!(
            "--session-id must be non-empty, at most {MAX_SESSION_ID_CHARS} \
             characters, and contain only visible ASCII with no spaces and no commas"
        ));
    }
    Ok(())
}
