//! The knowledge shared by every Skardi MCP binding: projecting the enriched
//! `GET /pipelines` inventory into MCP tool definitions, the built-in tool
//! names, and the pipeline-name → URL path encoding of the tool→REST
//! translation contract. The dispatch matches themselves deliberately stay
//! per-binding (stdio bridge, server `/mcp`): same shape, different error
//! types and identity carriers.

pub mod projection;

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};

/// Characters percent-encoded by [`encode_component`]: everything except
/// ASCII alphanumerics and the RFC 3986 "unreserved" marks (`-`, `.`, `_`,
/// `~`). Deliberately conservative — over-encoding is always valid, while
/// missing a reserved character (`/`, `?`, `#`, `%`, space, …) mis-routes
/// the request.
const URL_COMPONENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Percent-encode one URL path segment or query value (user-supplied
/// pipeline/job names, run ids) so characters like `/`, `?`, `#`, `%`, and
/// spaces cannot alter the request route.
pub fn encode_component(raw: &str) -> String {
    utf8_percent_encode(raw, URL_COMPONENT).to_string()
}

#[cfg(test)]
mod tests {
    // Moved with the function from `crates/cli/src/client.rs`.
    #[test]
    fn encode_component_escapes_reserved_and_keeps_unreserved() {
        use super::encode_component;

        assert_eq!(encode_component("a/b"), "a%2Fb");
        assert_eq!(encode_component("a b?c#d%e"), "a%20b%3Fc%23d%25e");
        assert_eq!(encode_component("a&b=c"), "a%26b%3Dc");
        // Unreserved characters pass through untouched.
        assert_eq!(
            encode_component("daily-report_v2.1~x"),
            "daily-report_v2.1~x"
        );
        // Non-ASCII is UTF-8 percent-encoded.
        assert_eq!(encode_component("café"), "caf%C3%A9");
    }
}
