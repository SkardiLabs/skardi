//! Built-in source packs: stable relational contracts for SaaS providers.
//!
//! A source pack is Skardi-maintained Rust code (never user YAML). Each
//! table definition pins the full relational contract — action, row path,
//! fixed schema, pagination strategy, allowlisted filters, required
//! resources — so users bind packs to concrete resources without being able
//! to alter that contract. Packs are versioned; bindings may pin a version
//! so a Skardi upgrade cannot silently change a table's schema.

use std::collections::HashMap;

use super::error::OpenConnectorError;
use super::filters::FilterMapping;
use super::json_to_arrow::FieldMapping;
use super::pagination::PaginationStrategy;

/// A fixed action-input value a pack pins at compile time — a
/// const-friendly stand-in for the JSON scalar set (`serde_json::Value`'s
/// string and number variants cannot be built in `static` initializers).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FixedValue {
    /// JSON string.
    Str(&'static str),
    /// JSON integer.
    Int(i64),
    /// JSON number. Non-finite values serialize as JSON null (a pack bug —
    /// there is no JSON spelling for NaN/inf), so packs pin finite numbers.
    Float(f64),
    /// JSON boolean.
    Bool(bool),
}

impl FixedValue {
    /// The JSON value sent in the action input.
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Self::Str(text) => serde_json::Value::from(*text),
            Self::Int(value) => serde_json::Value::from(*value),
            Self::Float(value) => serde_json::Value::from(*value),
            Self::Bool(value) => serde_json::Value::from(*value),
        }
    }
}

/// One stable table definition inside a source pack.
#[derive(Debug, Clone, Copy)]
pub struct SourcePackTable {
    /// Stable table identifier, e.g. `github.issues`.
    pub id: &'static str,
    /// Open Connector action backing the table (read-only by construction).
    pub action_id: &'static str,
    /// Fixed row path of the row array in the action response.
    pub row_path: &'static str,
    /// Fixed Arrow schema and field mappings.
    pub fields: &'static [FieldMapping],
    /// Pagination strategy.
    pub pagination: PaginationStrategy,
    /// Resource inputs a binding must supply (e.g. `owner`, `repo`).
    pub required_resources: &'static [&'static str],
    /// Fixed action inputs sent with every request, e.g. `state=all` where
    /// a provider endpoint defaults to a filtered listing (GitHub issues
    /// default to open ones). A pushed-down filter targeting the same input
    /// field overrides the fixed value, so the table reads as the complete
    /// collection while predicates still narrow it.
    pub fixed_inputs: &'static [(&'static str, FixedValue)],
    /// Allowlisted filter translations.
    pub filters: &'static [FilterMapping],
    /// Expected action-contract fingerprint. When set, registration compares
    /// it with the discovered action's fingerprint and fails on mismatch.
    pub expected_fingerprint: Option<&'static str>,
}

/// A versioned set of stable table definitions for one provider.
#[derive(Debug)]
pub struct SourcePack {
    /// Provider name, e.g. `github`.
    pub name: &'static str,
    /// Pack version; bump on any stable-schema change (with release notes).
    pub version: u32,
    /// Stable tables in this pack.
    pub tables: &'static [SourcePackTable],
}

/// Registry of built-in source packs.
#[derive(Debug, Default)]
pub struct SourcePackRegistry {
    packs: HashMap<&'static str, &'static SourcePack>,
}

impl SourcePackRegistry {
    /// The built-in packs shipped with this Skardi build.
    pub fn builtins() -> Self {
        let mut packs = HashMap::new();
        packs.insert(
            super::packs::mock::MOCK_PACK.name,
            &super::packs::mock::MOCK_PACK,
        );
        packs.insert(
            super::packs::github::GITHUB_PACK.name,
            &super::packs::github::GITHUB_PACK,
        );
        packs.insert(
            super::packs::slack::SLACK_PACK.name,
            &super::packs::slack::SLACK_PACK,
        );
        Self { packs }
    }

    /// Look up a pack by provider name.
    pub fn get(&self, name: &str) -> Option<&'static SourcePack> {
        self.packs.get(name).copied()
    }

    /// Resolve `pack` + `table` to a table definition, with targeted errors
    /// for unknown packs and unknown tables.
    pub fn table(
        &self,
        pack: &'static SourcePack,
        table: &str,
    ) -> Result<&'static SourcePackTable, OpenConnectorError> {
        // Exact full-ID match first (`github.issues`), then the short-name
        // convention (`issues` = the ID's last segment, whole-segment
        // equality). A short name matching several tables is an error, not
        // first-wins — silently binding the wrong contract would defeat
        // every schema guarantee downstream. Built-in packs keep last
        // segments unique (pinned by a test below), so ambiguity can only
        // come from future multi-segment or user-authored packs.
        if let Some(exact) = pack.tables.iter().find(|candidate| candidate.id == table) {
            return Ok(exact);
        }
        let mut matches = pack
            .tables
            .iter()
            .filter(|candidate| candidate.id.rsplit('.').next() == Some(table));
        match (matches.next(), matches.next()) {
            (Some(only), None) => Ok(only),
            (None, _) => Err(OpenConnectorError::SourcePackTableNotFound {
                pack: pack.name.to_string(),
                table: table.to_string(),
            }),
            (Some(first), Some(second)) => {
                let mut candidates = vec![first.id, second.id];
                candidates.extend(matches.map(|candidate| candidate.id));
                Err(OpenConnectorError::SourcePackTableAmbiguous {
                    pack: pack.name.to_string(),
                    table: table.to_string(),
                    candidates: candidates.join(", "),
                })
            }
        }
    }

    /// Resolve a pack by name with a targeted error.
    pub fn require(&self, name: &str) -> Result<&'static SourcePack, OpenConnectorError> {
        self.get(name)
            .ok_or_else(|| OpenConnectorError::SourcePackNotFound {
                name: name.to_string(),
            })
    }

    /// Enforce a binding's optional version pin.
    pub fn check_version_pin(
        pack: &'static SourcePack,
        pinned: Option<u32>,
    ) -> Result<(), OpenConnectorError> {
        if let Some(pinned) = pinned
            && pinned != pack.version
        {
            return Err(OpenConnectorError::SourcePackVersionMismatch {
                pack: pack.name.to_string(),
                pinned,
                actual: pack.version,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_mock_pack_is_registered() {
        let registry = SourcePackRegistry::builtins();
        let pack = registry.require("mock").unwrap();
        assert_eq!(pack.name, "mock");
        assert_eq!(pack.version, 1);
        assert_eq!(pack.tables.len(), 1);
        assert_eq!(pack.tables[0].id, "mock.items");
    }

    #[test]
    fn fixed_values_convert_to_their_json_scalars() {
        // The const-friendly stand-in must round-trip every JSON scalar a
        // pack could pin — numeric/boolean pins are never stringified.
        assert_eq!(FixedValue::Str("all").to_json(), serde_json::json!("all"));
        assert_eq!(FixedValue::Int(-3).to_json(), serde_json::json!(-3));
        assert_eq!(FixedValue::Float(2.5).to_json(), serde_json::json!(2.5));
        assert_eq!(FixedValue::Bool(true).to_json(), serde_json::json!(true));
    }

    #[test]
    fn unknown_pack_is_a_targeted_error() {
        let registry = SourcePackRegistry::builtins();
        let err = registry.require("jira").unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::SourcePackNotFound { ref name } if name == "jira"
        ));
    }

    #[test]
    fn builtin_github_pack_is_registered() {
        let registry = SourcePackRegistry::builtins();
        let pack = registry.require("github").unwrap();
        assert_eq!(pack.name, "github");
        assert_eq!(pack.version, 1);
        let ids: Vec<&str> = pack.tables.iter().map(|table| table.id).collect();
        assert_eq!(
            ids,
            vec![
                "github.repositories",
                "github.issues",
                "github.issue_comments",
                "github.pull_requests",
                "github.reviews",
                "github.commits",
                "github.workflow_runs",
                "github.releases",
            ]
        );
    }

    #[test]
    fn unknown_table_is_a_targeted_error() {
        let registry = SourcePackRegistry::builtins();
        let pack = registry.require("mock").unwrap();
        let err = registry.table(pack, "users").unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::SourcePackTableNotFound { ref pack, ref table }
                if pack == "mock" && table == "users"
        ));
    }

    #[test]
    fn full_table_ids_resolve_exactly() {
        let registry = SourcePackRegistry::builtins();
        let pack = registry.require("github").unwrap();
        let by_short = registry.table(pack, "issues").unwrap();
        let by_full = registry.table(pack, "github.issues").unwrap();
        assert_eq!(by_short.id, by_full.id);
    }

    #[test]
    fn ambiguous_short_names_are_an_error_not_first_match() {
        // Multi-segment IDs sharing a last segment: first-match would
        // silently bind the wrong contract; the full ID disambiguates.
        let tables = vec![
            leaked_table("t.issue.comments"),
            leaked_table("t.pr.comments"),
        ];
        let pack: &'static SourcePack = Box::leak(Box::new(SourcePack {
            name: "t",
            version: 1,
            tables: Box::leak(tables.into_boxed_slice()),
        }));

        let registry = SourcePackRegistry::builtins();
        let err = registry.table(pack, "comments").unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::SourcePackTableAmbiguous { ref candidates, .. }
                if candidates == "t.issue.comments, t.pr.comments"
        ));

        let resolved = registry.table(pack, "t.pr.comments").unwrap();
        assert_eq!(resolved.id, "t.pr.comments");
    }

    #[test]
    fn builtin_pack_short_names_stay_unambiguous() {
        // The short-name convention (`tables: [issues]`) is only sound while
        // every built-in pack keeps `<pack>.<table>` IDs with unique last
        // segments. New packs must keep this invariant or bindings hit the
        // ambiguity error above.
        let registry = SourcePackRegistry::builtins();
        for name in ["mock", "github", "slack"] {
            let pack = registry.require(name).unwrap();
            let mut seen = std::collections::HashSet::new();
            for table in pack.tables {
                let prefix = format!("{}.", pack.name);
                assert!(
                    table.id.starts_with(&prefix),
                    "table ID '{}' must be namespaced under '{prefix}'",
                    table.id
                );
                let short = table.id.rsplit('.').next().unwrap();
                assert!(
                    seen.insert(short),
                    "duplicate short name '{short}' in pack '{name}'"
                );
            }
        }
    }

    fn leaked_table(id: &'static str) -> SourcePackTable {
        SourcePackTable {
            id,
            action_id: "t.action",
            row_path: "$.items",
            fields: &[],
            pagination: PaginationStrategy::SinglePage,
            required_resources: &[],
            fixed_inputs: &[],
            filters: &[],
            expected_fingerprint: None,
        }
    }

    #[test]
    fn version_pin_enforcement() {
        let registry = SourcePackRegistry::builtins();
        let pack = registry.require("mock").unwrap();
        SourcePackRegistry::check_version_pin(pack, None).unwrap();
        SourcePackRegistry::check_version_pin(pack, Some(1)).unwrap();
        let err = SourcePackRegistry::check_version_pin(pack, Some(2)).unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::SourcePackVersionMismatch {
                pinned: 2,
                actual: 1,
                ..
            }
        ));
    }
}
