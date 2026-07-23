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
    pub fixed_inputs: &'static [(&'static str, &'static str)],
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
        pack.tables
            .iter()
            .find(|candidate| candidate.id.rsplit('.').next() == Some(table))
            .ok_or_else(|| OpenConnectorError::SourcePackTableNotFound {
                pack: pack.name.to_string(),
                table: table.to_string(),
            })
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
