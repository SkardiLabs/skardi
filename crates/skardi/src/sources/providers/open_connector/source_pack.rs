//! Built-in source packs: stable relational contracts for SaaS providers.
//!
//! A source pack is a Skardi-maintained declarative asset — embedded YAML
//! compiled into the binary and parsed once at first registry access (see
//! `packs::loader`), never user-editable configuration. Each
//! table definition pins the full relational contract — action, row path,
//! fixed schema, pagination strategy, allowlisted filters, required
//! resources — so users bind packs to concrete resources without being able
//! to alter that contract. Packs are versioned; bindings may pin a version
//! so a Skardi upgrade cannot silently change a table's schema.

use std::collections::HashMap;

use super::error::OpenConnectorError;
use super::filters::FilterMapping;
use super::json_to_arrow::FieldMapping;
use super::pagination::{CursorContinuation, PaginationStrategy};

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
    /// JSON array of strings — e.g. Slack's `types:
    /// ["public_channel", "private_channel"]`, whose action schema takes an
    /// array, not a comma-joined string.
    StrList(&'static [&'static str]),
    /// An arbitrary JSON value (typically an object) — e.g. Notion's search
    /// `filter: {"property": "object", "value": "page"}`, whose action
    /// schema takes an object. Pre-parsed and leaked by the pack loader.
    Json(&'static serde_json::Value),
}

impl FixedValue {
    /// The JSON value sent in the action input.
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Self::Str(text) => serde_json::Value::from(*text),
            Self::Int(value) => serde_json::Value::from(*value),
            Self::Float(value) => serde_json::Value::from(*value),
            Self::Bool(value) => serde_json::Value::from(*value),
            Self::StrList(items) => {
                serde_json::Value::from(items.iter().map(|s| *s).collect::<Vec<_>>())
            }
            Self::Json(value) => (*value).clone(),
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
    /// Resource inputs a binding *may* supply (e.g. a Slack `channelId`
    /// scoping a file listing). Everything else in the binding's resource
    /// map is withheld from this table's requests: Open Connector's action
    /// schemas reject undeclared input keys (`additionalProperties:
    /// false`), so one binding can serve tables with different resource
    /// needs — each table receives exactly the keys it declares.
    pub optional_resources: &'static [&'static str],
    /// Resource inputs that are ALTERNATIVES: at most one member of each
    /// group may reach a request. Each member names a complete collection
    /// on its own (OneDrive's `folderItemId` and `folderPath` each scope
    /// `list_folder_children` to one folder), so both are legitimately
    /// optional — but the upstream executor resolves a binding carrying
    /// both by its own precedence, and the loser becomes dead
    /// configuration. That is the one misconfiguration shape that yields
    /// confidently wrong rows rather than an error: the scan succeeds
    /// against a scope the operator did not name. Registration refuses the
    /// ambiguity instead of picking a side.
    pub exclusive_resources: &'static [&'static [&'static str]],
    /// Fixed action inputs sent with every request, e.g. `state=all` where
    /// a provider endpoint defaults to a filtered listing (GitHub issues
    /// default to open ones). A pushed-down filter targeting the same input
    /// field overrides the fixed value, so the table reads as the complete
    /// collection while predicates still narrow it.
    pub fixed_inputs: &'static [(&'static str, FixedValue)],
    /// Allowlisted filter translations.
    pub filters: &'static [FilterMapping],
    /// Row-path of an in-band provider error code in an otherwise
    /// successful envelope (Slack's HTTP-200 `ok: false` + `error`
    /// pattern). When declared and present in a page, the scan fails with
    /// the provider's own code instead of a misleading row-path error.
    pub error_path: Option<&'static str>,
    /// Expected action-contract fingerprint. When set, registration compares
    /// it with the discovered action's fingerprint and fails on mismatch.
    pub expected_fingerprint: Option<&'static str>,
    /// Split-action cursor continuation, for providers that serve pages
    /// 2..N from a different action than the one that began the listing
    /// (see [`CursorContinuation`]). `None` for every table whose provider
    /// accepts the cursor on its own action.
    pub continuation: Option<CursorContinuation>,
}

impl SourcePackTable {
    /// Whether this table's action declares `key` as a resource input
    /// (required or optional). Undeclared keys must never reach the wire:
    /// Open Connector's strict action schemas reject them.
    pub fn declares_resource(&self, key: &str) -> bool {
        self.required_resources.contains(&key) || self.optional_resources.contains(&key)
    }

    /// The first two members of a declared alternative group that `has`
    /// reports as both supplied, in declaration order. `None` when every
    /// group has at most one member — the only configuration whose scope
    /// is unambiguous.
    pub fn conflicting_resources(
        &self,
        has: impl Fn(&str) -> bool,
    ) -> Option<(&'static str, &'static str)> {
        for group in self.exclusive_resources {
            let mut supplied = group.iter().copied().filter(|key| has(key));
            if let (Some(first), Some(second)) = (supplied.next(), supplied.next()) {
                return Some((first, second));
            }
        }
        None
    }

    /// Every action this table executes: its own, plus a split-action
    /// continuation's. Registration discovers all of them, so a
    /// continuation action missing from the gateway fails at startup rather
    /// than on page two of the first scan.
    pub fn actions(&self) -> impl Iterator<Item = &'static str> + '_ {
        std::iter::once(self.action_id).chain(self.continuation.map(|c| c.action_id))
    }

    /// Every `(action, expected fingerprint)` pair the compatibility gate
    /// must verify. Both gate call sites — YAML binding registration and
    /// the `open_connector_query` UDTF — iterate THIS, so a drifted
    /// continuation action cannot be refused by one path and admitted by
    /// the other.
    pub fn gated_actions(&self) -> impl Iterator<Item = (&'static str, &'static str)> + '_ {
        self.expected_fingerprint
            .map(|fingerprint| (self.action_id, fingerprint))
            .into_iter()
            .chain(
                self.continuation
                    .map(|c| (c.action_id, c.expected_fingerprint)),
            )
    }
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
    ///
    /// # Errors
    /// [`OpenConnectorError::SourcePackAssetInvalid`] when an embedded pack
    /// asset fails to parse or validate — a build defect surfaced as a
    /// registration diagnostic (the parse-all test pins shipped assets as
    /// valid).
    pub fn builtins() -> Result<Self, OpenConnectorError> {
        let mut packs = HashMap::new();
        for pack in [
            super::packs::mock::pack()?,
            super::packs::dropbox::pack()?,
            super::packs::github::pack()?,
            super::packs::gmail::pack()?,
            super::packs::notion::pack()?,
            super::packs::slack::pack()?,
            super::packs::feishu::pack()?,
            super::packs::discord::pack()?,
            super::packs::outlook::pack()?,
            super::packs::one_drive::pack()?,
            super::packs::google_drive::pack()?,
        ] {
            packs.insert(pack.name, pack);
        }
        Ok(Self { packs })
    }

    /// Look up a pack by provider name.
    pub fn get(&self, name: &str) -> Option<&'static SourcePack> {
        self.packs.get(name).copied()
    }

    /// Every built-in pack, name-sorted so enumeration is deterministic —
    /// the etl generator's recipe contract suite and its `recipes` coverage
    /// listing both iterate this (the map itself is private and unordered).
    pub fn packs(&self) -> impl Iterator<Item = &'static SourcePack> + '_ {
        let mut packs: Vec<&'static SourcePack> = self.packs.values().copied().collect();
        packs.sort_by_key(|pack| pack.name);
        packs.into_iter()
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
        let registry = SourcePackRegistry::builtins().expect("embedded assets parse");
        let pack = registry.require("mock").unwrap();
        assert_eq!(pack.name, "mock");
        assert_eq!(pack.version, 1);
        assert_eq!(pack.tables.len(), 1);
        assert_eq!(pack.tables[0].id, "mock.items");
    }

    #[test]
    fn packs_iterates_every_builtin_in_name_order() {
        // The enumeration surface the etl generator's contract suite and
        // `recipes` listing depend on: complete and deterministic — the
        // backing map is unordered, so the sort here is load-bearing.
        let registry = SourcePackRegistry::builtins().expect("embedded assets parse");
        let names: Vec<&str> = registry.packs().map(|p| p.name).collect();
        // Sortedness, asserted independently of the roster so THIS pin
        // survives future pack additions untouched…
        assert!(
            names.windows(2).all(|w| w[0] < w[1]),
            "packs() must iterate name-sorted with no duplicates: {names:?}"
        );
        // …and completeness as an explicit roster, the one line a new pack
        // must extend (a stale list here means the generator's coverage
        // listing silently omits the newcomer).
        assert_eq!(
            names,
            vec![
                "discord",
                "dropbox",
                "feishu",
                "github",
                "gmail",
                "google_drive",
                "mock",
                "notion",
                "one_drive",
                "outlook",
                "slack"
            ]
        );
    }

    #[test]
    fn fixed_values_convert_to_their_json_scalars() {
        // The const-friendly stand-in must round-trip every JSON scalar a
        // pack could pin — numeric/boolean pins are never stringified.
        assert_eq!(FixedValue::Str("all").to_json(), serde_json::json!("all"));
        assert_eq!(FixedValue::Int(-3).to_json(), serde_json::json!(-3));
        assert_eq!(FixedValue::Float(2.5).to_json(), serde_json::json!(2.5));
        assert_eq!(FixedValue::Bool(true).to_json(), serde_json::json!(true));
        assert_eq!(
            FixedValue::StrList(&["public_channel", "private_channel"]).to_json(),
            serde_json::json!(["public_channel", "private_channel"])
        );
    }

    #[test]
    fn unknown_pack_is_a_targeted_error() {
        let registry = SourcePackRegistry::builtins().expect("embedded assets parse");
        let err = registry.require("jira").unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::SourcePackNotFound { ref name } if name == "jira"
        ));
    }

    #[test]
    fn builtin_github_pack_is_registered() {
        let registry = SourcePackRegistry::builtins().expect("embedded assets parse");
        let pack = registry.require("github").unwrap();
        assert_eq!(pack.name, "github");
        assert_eq!(pack.version, 1);
        // Sorted by table name: the loader stores tables in a BTreeMap so
        // registry (and catalog) order is deterministic regardless of how
        // the YAML asset is laid out.
        let ids: Vec<&str> = pack.tables.iter().map(|table| table.id).collect();
        assert_eq!(
            ids,
            vec![
                "github.commits",
                "github.issue_comments",
                "github.issues",
                "github.pull_requests",
                "github.releases",
                "github.repositories",
                "github.reviews",
                "github.workflow_runs",
            ]
        );
    }

    #[test]
    fn builtin_gmail_pack_is_registered() {
        let registry = SourcePackRegistry::builtins().expect("embedded assets parse");
        let pack = registry.require("gmail").unwrap();
        assert_eq!(pack.name, "gmail");
        assert_eq!(pack.version, 1);
        let ids: Vec<&str> = pack.tables.iter().map(|table| table.id).collect();
        assert_eq!(
            ids,
            vec![
                "gmail.drafts",
                "gmail.filters",
                "gmail.labels",
                "gmail.messages",
                "gmail.threads",
            ]
        );
    }

    #[test]
    fn builtin_google_drive_pack_is_registered() {
        let registry = SourcePackRegistry::builtins().expect("embedded assets parse");
        let pack = registry.require("google_drive").unwrap();
        assert_eq!(pack.name, "google_drive");
        assert_eq!(pack.version, 1);
        // BTreeMap order, not the yaml's authoring order: drives sorts
        // ahead of the files table it exists to join against.
        let ids: Vec<&str> = pack.tables.iter().map(|table| table.id).collect();
        assert_eq!(
            ids,
            vec![
                "google_drive.drives",
                "google_drive.file_permissions",
                "google_drive.files",
            ]
        );
    }

    #[test]
    fn unknown_table_is_a_targeted_error() {
        let registry = SourcePackRegistry::builtins().expect("embedded assets parse");
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
        let registry = SourcePackRegistry::builtins().expect("embedded assets parse");
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

        let registry = SourcePackRegistry::builtins().expect("embedded assets parse");
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
        let registry = SourcePackRegistry::builtins().expect("embedded assets parse");
        for name in [
            "mock",
            "github",
            "gmail",
            "slack",
            "notion",
            "feishu",
            "discord",
            "outlook",
            "one_drive",
            "google_drive",
        ] {
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
            pagination: PaginationStrategy::SinglePage {
                next_cursor_path: None,
            },
            required_resources: &[],
            optional_resources: &[],
            exclusive_resources: &[],
            fixed_inputs: &[],
            filters: &[],
            error_path: None,
            expected_fingerprint: None,
            continuation: None,
        }
    }

    #[test]
    fn version_pin_enforcement() {
        let registry = SourcePackRegistry::builtins().expect("embedded assets parse");
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
