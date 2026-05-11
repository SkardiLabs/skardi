//! Scope strings and matching for the bearer-token RBAC layer.
//!
//! A scope is a colon-separated string like `pipeline:execute:my_pipe`.
//! Wildcard `*` matches any single component or — as the trailing
//! component — any tail (`pipeline:*` matches both `pipeline:execute:foo`
//! and `pipeline:read:foo`). The bare scope `*` matches everything.
//!
//! Examples:
//!
//! | Granted        | Required                       | Match? |
//! |----------------|--------------------------------|--------|
//! | `*`            | `pipeline:execute:foo`         | yes    |
//! | `pipeline:*`   | `pipeline:execute:foo`         | yes    |
//! | `pipeline:execute:*` | `pipeline:execute:foo`   | yes    |
//! | `pipeline:execute:foo` | `pipeline:execute:foo` | yes    |
//! | `pipeline:read:*`     | `pipeline:execute:foo`  | no     |
//! | `jobs:*`              | `pipeline:execute:foo`  | no     |
//!
//! Built-in scope vocabulary used across the server:
//!
//! * `pipeline:read:<name|*>`   — `GET /pipelines`, `/pipeline/:name`, `/health/:name`, `/data_source`
//! * `pipeline:execute:<name|*>` — `POST /:name/execute`
//! * `jobs:read:<name|*>`       — `GET /jobs`, `/jobs/runs`, `/jobs/runs/:id`
//! * `jobs:submit:<name|*>`     — `POST /jobs/:name/run`
//! * `jobs:cancel:<name|*>`     — `POST /jobs/runs/:id/cancel`
//! * `keys:manage`              — admin endpoints under `/api/keys`

/// Returns true when `granted` confers the privilege described by
/// `required`. Both strings are colon-separated; `*` is a wildcard. See
/// the module docs for the matching rules.
pub fn scope_matches(granted: &str, required: &str) -> bool {
    if granted == "*" {
        return true;
    }
    let g_parts: Vec<&str> = granted.split(':').collect();
    let r_parts: Vec<&str> = required.split(':').collect();

    for (i, g) in g_parts.iter().enumerate() {
        // A trailing `*` means "and anything that follows" — even if the
        // required scope has more components than the grant.
        if *g == "*" && i == g_parts.len() - 1 {
            return true;
        }
        match r_parts.get(i) {
            Some(r) if g == r || *g == "*" => continue,
            _ => return false,
        }
    }

    // Grant exhausted with no trailing `*` — only matches when the
    // required scope has the exact same number of components.
    g_parts.len() == r_parts.len()
}

/// Returns true when any granted scope confers `required`.
pub fn any_scope_matches(granted: &[String], required: &str) -> bool {
    granted.iter().any(|g| scope_matches(g, required))
}

/// Map a coarse better-auth role string to an implicit scope set. Unknown
/// roles get nothing, so a stray DB value never silently grants access.
///
/// Three roles, three resources (`pipeline`, `jobs`, `data_source`):
///
/// * `admin`    — `*` (everything, including `keys:manage`)
/// * `operator` — read+execute on pipelines, read+submit on jobs, read on data sources
/// * `viewer`   — read-only across pipelines, jobs, and data sources
pub fn scopes_for_role(role: Option<&str>) -> Vec<String> {
    match role.map(str::trim).filter(|s| !s.is_empty()) {
        Some("admin") => vec!["*".to_string()],
        Some("operator") => vec![
            "pipeline:*".to_string(),
            "jobs:*".to_string(),
            "data_source:read:*".to_string(),
        ],
        Some("viewer") => vec![
            "pipeline:read:*".to_string(),
            "jobs:read:*".to_string(),
            "data_source:read:*".to_string(),
        ],
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn star_matches_anything() {
        assert!(scope_matches("*", "pipeline:execute:foo"));
        assert!(scope_matches("*", "anything"));
        assert!(scope_matches("*", "a:b:c:d:e"));
    }

    #[test]
    fn exact_match() {
        assert!(scope_matches(
            "pipeline:execute:foo",
            "pipeline:execute:foo"
        ));
        assert!(!scope_matches(
            "pipeline:execute:foo",
            "pipeline:execute:bar"
        ));
        assert!(!scope_matches("pipeline:execute:foo", "pipeline:read:foo"));
    }

    #[test]
    fn trailing_wildcard_matches_tail() {
        assert!(scope_matches("pipeline:*", "pipeline:execute:foo"));
        assert!(scope_matches("pipeline:execute:*", "pipeline:execute:foo"));
        assert!(scope_matches("pipeline:execute:*", "pipeline:execute:bar"));
        assert!(!scope_matches("pipeline:*", "jobs:read:foo"));
    }

    #[test]
    fn middle_wildcard_matches_one_component() {
        assert!(scope_matches("pipeline:*:foo", "pipeline:execute:foo"));
        assert!(scope_matches("pipeline:*:foo", "pipeline:read:foo"));
        assert!(!scope_matches("pipeline:*:foo", "pipeline:execute:bar"));
    }

    #[test]
    fn arity_mismatch_without_trailing_star() {
        // Grant has fewer components and no trailing `*` → no match.
        assert!(!scope_matches("pipeline:execute", "pipeline:execute:foo"));
        // Grant has more components → no match.
        assert!(!scope_matches("pipeline:execute:foo", "pipeline:execute"));
    }

    #[test]
    fn any_scope_matches_picks_first_grant() {
        let granted = vec![
            "jobs:read:*".to_string(),
            "pipeline:execute:foo".to_string(),
        ];
        assert!(any_scope_matches(&granted, "pipeline:execute:foo"));
        assert!(any_scope_matches(&granted, "jobs:read:bar"));
        assert!(!any_scope_matches(&granted, "pipeline:execute:bar"));
        assert!(!any_scope_matches(&granted, "jobs:submit:bar"));
    }

    #[test]
    fn role_to_scopes_admin_is_star() {
        let scopes = scopes_for_role(Some("admin"));
        assert!(any_scope_matches(&scopes, "pipeline:execute:foo"));
        assert!(any_scope_matches(&scopes, "jobs:cancel:bar"));
        assert!(any_scope_matches(&scopes, "keys:manage"));
    }

    #[test]
    fn role_to_scopes_operator_covers_pipeline_jobs_data_source() {
        let scopes = scopes_for_role(Some("operator"));
        assert!(any_scope_matches(&scopes, "pipeline:execute:foo"));
        assert!(any_scope_matches(&scopes, "jobs:submit:bar"));
        assert!(any_scope_matches(&scopes, "data_source:read:bar"));
        assert!(!any_scope_matches(&scopes, "keys:manage"));
    }

    #[test]
    fn role_to_scopes_viewer_is_read_only() {
        let scopes = scopes_for_role(Some("viewer"));
        assert!(any_scope_matches(&scopes, "pipeline:read:foo"));
        assert!(any_scope_matches(&scopes, "jobs:read:bar"));
        assert!(any_scope_matches(&scopes, "data_source:read:bar"));
        assert!(!any_scope_matches(&scopes, "pipeline:execute:foo"));
        assert!(!any_scope_matches(&scopes, "jobs:submit:foo"));
    }

    #[test]
    fn role_to_scopes_unknown_or_blank_is_empty() {
        assert!(scopes_for_role(None).is_empty());
        assert!(scopes_for_role(Some("")).is_empty());
        assert!(scopes_for_role(Some("   ")).is_empty());
        assert!(scopes_for_role(Some("god")).is_empty());
    }
}
