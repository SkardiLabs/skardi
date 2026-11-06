//! `--scope`, the install's selection (design 2026-10-27 §2.4.2).
//!
//! A JSON object in the install API's `content-types-v1` shape: one key per
//! content type, each either `null` (everything the credential reaches) or a
//! list of rules. **Positive rules only** — `all`, `subtree`, `leaf` — because
//! a grant's install only adds (§2.2.4). An `exclude` narrows whatever it
//! lands beside, so it is refused here, with the sentence that sends the
//! person to Manage, before the server refuses it as
//! `scope_exclude_not_agent_callable`.
//!
//! ```text
//! {"issues":[{"kind":"leaf","coordinate":{"owner":"acme","repo":"api"}}],
//!  "pull_requests":[]}
//! {"pages":[{"kind":"subtree","coordinate":{"page_id":"…"}}]}
//! {"issues":null,"pull_requests":null}
//! ```
//!
//! Each `coordinate` is copied verbatim from a `candidates` row, which is why
//! the agent lists candidates first; this checks only that it is an object,
//! because what a coordinate holds is the provider's business and the server
//! validates it against the listing. Which content types exist is the
//! server's too: a key it does not know is its refusal to make, not a guess
//! to make here.

use super::AgentError;
use serde_json::{Map, Value};

/// The rule kinds a grant may send.
const POSITIVE_KINDS: [&str; 3] = ["all", "subtree", "leaf"];

/// The sentence for an `exclude`, the server's own reason in the CLI's words.
pub const EXCLUDE_REFUSAL: &str = "an agent's install may only add, and an `exclude` rule narrows what is synced. Remove the rule; to stop syncing something, open the source's Manage page in the console";

/// Parse and check `--scope`, returning the object to send as `scope`.
pub fn parse(raw: &str) -> Result<Map<String, Value>, AgentError> {
    let value: Value = serde_json::from_str(raw).map_err(|err| {
        AgentError::usage(
            "invalid_scope",
            format!("--scope is not JSON ({err}); it is an object of content type to rules, like '{{\"issues\":null}}'"),
        )
    })?;
    let Value::Object(map) = value else {
        return Err(AgentError::usage(
            "invalid_scope",
            "--scope is a JSON object of content type to rules, like '{\"issues\":null}'",
        ));
    };
    for (content_type, rules) in &map {
        if content_type.trim().is_empty() {
            return Err(AgentError::usage(
                "invalid_scope",
                "--scope names an empty content type",
            ));
        }
        match rules {
            // Everything this content type's credential reaches.
            Value::Null => {}
            Value::Array(rules) => {
                for (index, rule) in rules.iter().enumerate() {
                    check_rule(content_type, index, rule)?;
                }
            }
            _ => {
                return Err(AgentError::usage(
                    "invalid_scope",
                    format!(
                        "--scope's \"{content_type}\" must be null (all of it) or a list of rules"
                    ),
                ));
            }
        }
    }
    Ok(map)
}

fn check_rule(content_type: &str, index: usize, rule: &Value) -> Result<(), AgentError> {
    let at = || format!("--scope's \"{content_type}\"[{index}]");
    let Some(rule) = rule.as_object() else {
        return Err(AgentError::usage(
            "invalid_scope",
            format!(
                "{} is not a rule: write {{\"kind\":\"leaf\",\"coordinate\":{{…}}}}",
                at()
            ),
        ));
    };
    let kind = rule.get("kind").and_then(Value::as_str).unwrap_or_default();
    if kind == "exclude" {
        return Err(AgentError::usage(
            "scope_exclude_not_agent_callable",
            EXCLUDE_REFUSAL,
        ));
    }
    if !POSITIVE_KINDS.contains(&kind) {
        return Err(AgentError::usage(
            "invalid_scope",
            format!(
                "{} has kind {:?}; a rule's kind is all, subtree or leaf",
                at(),
                rule.get("kind").cloned().unwrap_or(Value::Null)
            ),
        ));
    }
    if kind != "all" && !rule.get("coordinate").is_some_and(Value::is_object) {
        return Err(AgentError::usage(
            "invalid_scope",
            format!(
                "{} is a {kind} rule with no coordinate object: copy the coordinate from a `skardi integrations candidates` row",
                at()
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::EXIT_USAGE;
    use rstest::rstest;

    /// The design's three examples, and the shapes around them.
    #[rstest]
    #[case::github_two_repos(
        r#"{"issues":[{"kind":"leaf","coordinate":{"owner":"acme","repo":"api"}},{"kind":"leaf","coordinate":{"owner":"acme","repo":"web"}}],"pull_requests":[]}"#
    )]
    #[case::notion_subtree(r#"{"pages":[{"kind":"subtree","coordinate":{"page_id":"p1"}}]}"#)]
    #[case::everything(r#"{"issues":null,"pull_requests":null}"#)]
    #[case::tagged_all(r#"{"files":[{"kind":"all"}]}"#)]
    #[case::empty_list_keeps_what_is_stored(r#"{"issues":[]}"#)]
    // An empty object adds nothing and is still an install: it activates the
    // version the agent was handed, with the stored selection carried across.
    #[case::nothing_new("{}")]
    fn valid_scopes_parse(#[case] raw: &str) {
        let parsed = parse(raw).unwrap();
        let reparsed: Value = serde_json::from_str(raw).unwrap();
        assert_eq!(Value::Object(parsed), reparsed, "sent verbatim");
    }

    #[rstest]
    #[case::not_json("{issues: null}", "invalid_scope")]
    #[case::not_an_object(r#"["issues"]"#, "invalid_scope")]
    #[case::bare_string(r#"{"issues":"all"}"#, "invalid_scope")]
    #[case::rule_not_object(r#"{"issues":["acme/api"]}"#, "invalid_scope")]
    #[case::unknown_kind(r#"{"issues":[{"kind":"everything"}]}"#, "invalid_scope")]
    #[case::missing_kind(r#"{"issues":[{"coordinate":{"owner":"a"}}]}"#, "invalid_scope")]
    #[case::leaf_without_coordinate(r#"{"issues":[{"kind":"leaf"}]}"#, "invalid_scope")]
    #[case::coordinate_not_object(
        r#"{"issues":[{"kind":"subtree","coordinate":"acme"}]}"#,
        "invalid_scope"
    )]
    #[case::empty_key(r#"{"":null}"#, "invalid_scope")]
    #[case::exclude(
        r#"{"pages":[{"kind":"subtree","coordinate":{"page_id":"p"}},{"kind":"exclude","coordinate":{"page_id":"q"}}]}"#,
        "scope_exclude_not_agent_callable"
    )]
    fn invalid_scopes_are_usage_errors(#[case] raw: &str, #[case] code: &str) {
        let err = parse(raw).unwrap_err();
        assert_eq!(err.exit, EXIT_USAGE, "{err:?}");
        assert_eq!(err.code(), code, "{err:?}");
    }

    #[test]
    fn an_exclude_names_the_manage_page() {
        let err =
            parse(r#"{"pages":[{"kind":"exclude","coordinate":{"page_id":"q"}}]}"#).unwrap_err();
        assert!(err.message().contains("Manage"), "{err:?}");
    }
}
