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

use serde_json::Value;

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

    /// Verify a `cursor_only` continuation against the continuation action's
    /// discovered INPUT schema. `Ok(())` for every table that declares no
    /// continuation, or whose continuation sends the full input.
    ///
    /// The contract fingerprint cannot cover this: it hashes the OUTPUT
    /// schema (`action_registry::fingerprint_schema`), while
    /// `cursor_only` is a claim about inputs — that a request carrying the
    /// cursor alone is accepted. Left ungated, a wrong or drifted claim
    /// surfaces as a hard 400 on page two of a live scan, after N pages of
    /// gateway budget are already spent. Two properties are checked, and
    /// only two, because they are exactly the ways the claim can break:
    ///
    /// 1. the cursor input is a DECLARED property — otherwise our cursor is
    ///    the undeclared extra that Open Connector's
    ///    `additionalProperties: false` schemas reject;
    /// 2. every REQUIRED input is one the pack sends, i.e. `required` is a
    ///    subset of `{cursor}` — otherwise the request is missing a
    ///    mandatory field.
    ///
    /// Deliberately NOT checked: whether the cursor is the action's *only*
    /// property. An upstream release that adds an optional input alongside
    /// it does not break a cursor-only request, and refusing to start over
    /// it would be a false alarm.
    ///
    /// A continuation action publishing no input schema at all is refused
    /// rather than waved through: an unverifiable claim about inputs is the
    /// same default-deny case as an action with no read/write
    /// classification (see `OpenConnectorScanFunction`).
    ///
    /// # Errors
    /// [`OpenConnectorError::ActionContractMismatch`] naming the
    /// continuation action and the specific disagreement.
    pub fn check_continuation_inputs(
        &self,
        input_schema: Option<&Value>,
    ) -> Result<(), OpenConnectorError> {
        let Some(continuation) = self.continuation.filter(|c| c.cursor_only) else {
            return Ok(());
        };
        // The loader nests `continuation` under the cursor strategy, so a
        // non-cursor table cannot declare one through YAML; a hand-built
        // table that does is a pack bug and says so rather than passing.
        let PaginationStrategy::Cursor { cursor_param, .. } = self.pagination else {
            return Err(OpenConnectorError::ActionContractMismatch {
                table: self.id.to_string(),
                reason: format!(
                    "action '{}' declares `inputs: cursor_only` on a non-cursor pagination \
                     strategy, which has no cursor input to send",
                    continuation.action_id
                ),
            });
        };
        let mismatch = |reason: String| OpenConnectorError::ActionContractMismatch {
            table: self.id.to_string(),
            reason,
        };
        let Some(properties) = input_schema
            .and_then(|schema| schema.get("properties"))
            .and_then(Value::as_object)
        else {
            return Err(mismatch(format!(
                "action '{}' publishes no input schema, so `inputs: cursor_only` cannot be \
                 verified; a claim about inputs that the gateway will not confirm is refused \
                 rather than discovered as a 400 on page two",
                continuation.action_id
            )));
        };
        if !properties.contains_key(cursor_param) {
            let mut declared: Vec<&str> = properties.keys().map(String::as_str).collect();
            declared.sort_unstable();
            return Err(mismatch(format!(
                "action '{}' does not declare the cursor input '{cursor_param}' (declares [{}]), \
                 so a cursor-only continuation request would be rejected as an undeclared input",
                continuation.action_id,
                declared.join(", ")
            )));
        }
        // Only inputs the pack actually sends may be mandatory. `required`
        // is optional in JSON Schema; ABSENT means nothing is mandatory and
        // is the one permissive reading this gate accepts. A `required`
        // that is PRESENT but unparseable is refused instead, for the same
        // reason a missing `properties` is: both come from the same
        // untrusted discovery payload, and a gate that cannot read its
        // input has not verified anything. Failing closed on one and open
        // on the other was the defect.
        let mut unsatisfiable =
            required_beyond_cursor(input_schema, cursor_param).map_err(|reason| {
                mismatch(format!(
                    "action '{}' publishes a `required` this gate cannot read ({reason}), so \
                     `inputs: cursor_only` cannot be verified; an unreadable schema is \
                     refused rather than waved through",
                    continuation.action_id
                ))
            })?;
        if !unsatisfiable.is_empty() {
            unsatisfiable.sort_unstable();
            return Err(mismatch(format!(
                "action '{}' requires input(s) [{}] that a cursor-only continuation does not \
                 send; pages 2..N would fail as a missing mandatory input",
                continuation.action_id,
                unsatisfiable.join(", ")
            )));
        }
        Ok(())
    }

    /// The pagination inputs this table's strategy injects. Sent on every
    /// request the strategy applies to, so they count as guaranteed.
    fn pagination_input_keys(&self) -> Vec<&'static str> {
        match self.pagination {
            PaginationStrategy::Cursor {
                cursor_param,
                page_size_param,
                ..
            } => std::iter::once(cursor_param)
                .chain(page_size_param)
                .collect(),
            PaginationStrategy::PageNumber {
                page_param,
                per_page_param,
                ..
            } => vec![page_param, per_page_param],
            PaginationStrategy::Keyset {
                cursor_param,
                page_size_param,
                ..
            } => vec![cursor_param, page_size_param],
            PaginationStrategy::SinglePage { .. } => Vec::new(),
        }
    }

    /// Keys the pack sends on EVERY request: the complete-collection pins,
    /// the resources a binding MUST supply, and the pagination inputs.
    /// Optional resources and pushed filters are excluded — a binding may
    /// omit them, so they cannot satisfy an action's `required`.
    fn guaranteed_input_keys(&self) -> Vec<&'static str> {
        let mut keys: Vec<&'static str> = self
            .fixed_inputs
            .iter()
            .map(|(key, _)| *key)
            .chain(self.required_resources.iter().copied())
            .chain(self.pagination_input_keys())
            .collect();
        keys.sort_unstable();
        keys.dedup();
        keys
    }

    /// Every key the pack COULD put in a request: the guaranteed set plus
    /// the optional resources and allowlisted filter inputs. Deliberately
    /// the widest set — this drives the `additionalProperties: false`
    /// check, where sending a key the action does not declare is the
    /// failure, so an "only sometimes" key is still a rejection.
    fn possible_input_keys(&self) -> Vec<&'static str> {
        let mut keys: Vec<&'static str> = self
            .guaranteed_input_keys()
            .into_iter()
            .chain(self.optional_resources.iter().copied())
            .chain(self.filters.iter().map(|f| f.input_field))
            .collect();
        keys.sort_unstable();
        keys.dedup();
        keys
    }

    /// Verify a FULL-input continuation that targets a DIFFERENT action
    /// than the table's own, against that action's discovered input
    /// schema. `Ok(())` for every other table.
    ///
    /// `inputs: full` is the default, so this is the shape a pack lands in
    /// by omission. When the continuation repeats the table's own action
    /// the skip is sound by construction — one action, one input schema,
    /// and page one already satisfied it. When it names a different
    /// action, "the opener's inputs satisfy the continue action" stops
    /// being a fact and becomes an assumption about two independently
    /// discovered schemas; unchecked, a disagreement is a 400 on every
    /// page-2 request, found by an operator mid-scan.
    ///
    /// Two directions, matching the two ways an action schema rejects a
    /// request:
    ///
    /// 1. every `required` input is one the pack sends on every request
    ///    (`guaranteed_input_keys`);
    /// 2. under `additionalProperties: false`, every key the pack COULD
    ///    send (`possible_input_keys`) is a declared property.
    ///
    /// # Errors
    /// [`OpenConnectorError::ActionContractMismatch`] naming the
    /// continuation action and the specific disagreement.
    pub fn check_full_continuation_inputs(
        &self,
        input_schema: Option<&Value>,
    ) -> Result<(), OpenConnectorError> {
        let Some(continuation) = self
            .continuation
            .filter(|c| !c.cursor_only && c.action_id != self.action_id)
        else {
            return Ok(());
        };
        let mismatch = |reason: String| OpenConnectorError::ActionContractMismatch {
            table: self.id.to_string(),
            reason,
        };
        let Some(schema) = input_schema else {
            return Err(mismatch(format!(
                "action '{}' publishes no input schema, so a full-input continuation to a \
                 DIFFERENT action cannot be verified; an unverifiable claim about inputs is \
                 refused rather than discovered as a 400 on page two",
                continuation.action_id
            )));
        };
        let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
            return Err(mismatch(format!(
                "action '{}' publishes no input properties, so a full-input continuation to \
                 a DIFFERENT action cannot be verified",
                continuation.action_id
            )));
        };
        let sends = self.guaranteed_input_keys();
        let mut missing: Vec<&str> = required_keys(schema)
            .map_err(|reason| {
                mismatch(format!(
                    "action '{}' publishes a `required` this gate cannot read ({reason})",
                    continuation.action_id
                ))
            })?
            .into_iter()
            .filter(|key| !sends.contains(key))
            .collect();
        if !missing.is_empty() {
            missing.sort_unstable();
            missing.dedup();
            return Err(mismatch(format!(
                "action '{}' requires input(s) [{}] that this table does not send on every \
                 request; pages 2..N would fail as a missing mandatory input",
                continuation.action_id,
                missing.join(", ")
            )));
        }
        // `additionalProperties` absent defaults to `true` in JSON Schema,
        // which permits the extra key — only an explicit `false` rejects.
        if schema.get("additionalProperties") == Some(&Value::Bool(false)) {
            let mut undeclared: Vec<&str> = self
                .possible_input_keys()
                .into_iter()
                .filter(|key| !properties.contains_key(*key))
                .collect();
            if !undeclared.is_empty() {
                undeclared.sort_unstable();
                return Err(mismatch(format!(
                    "action '{}' declares `additionalProperties: false` and does not declare \
                     input(s) [{}] that this table can send; pages 2..N would be rejected as \
                     undeclared inputs",
                    continuation.action_id,
                    undeclared.join(", ")
                )));
            }
        }
        Ok(())
    }
}

/// A discovered input schema's `required` list, as declared.
///
/// `Ok(vec![])` when `required` is absent — JSON Schema's "nothing is
/// mandatory". `Err(reason)` when it is present but not an array of
/// strings: that is a schema this gate does not understand, and "do not
/// understand" is the case a gate exists to refuse.
fn required_keys(schema: &Value) -> Result<Vec<&str>, &'static str> {
    let Some(required) = schema.get("required") else {
        return Ok(Vec::new());
    };
    let Some(items) = required.as_array() else {
        return Err("`required` is present but not an array");
    };
    items
        .iter()
        .map(|item| item.as_str().ok_or("`required` holds a non-string element"))
        .collect()
}

/// `required_keys` minus the cursor the pack does send.
fn required_beyond_cursor<'a>(
    input_schema: Option<&'a Value>,
    cursor_param: &str,
) -> Result<Vec<&'a str>, &'static str> {
    let Some(schema) = input_schema else {
        return Ok(Vec::new());
    };
    Ok(required_keys(schema)?
        .into_iter()
        .filter(|key| *key != cursor_param)
        .collect())
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

    /// A cursor table continuing through a cursor-only action, for the
    /// input-gate arms no pack-level e2e reaches.
    fn cursor_only_table() -> SourcePackTable {
        SourcePackTable {
            pagination: PaginationStrategy::Cursor {
                cursor_param: "cursor",
                next_cursor_path: "$.cursor",
                page_size_param: Some("limit"),
                page_size: 10,
                has_more_path: Some("$.hasMore"),
            },
            expected_fingerprint: Some("aa"),
            continuation: Some(CursorContinuation {
                action_id: "t.action_continue",
                expected_fingerprint: "aa",
                cursor_only: true,
            }),
            ..leaked_table("t.entries")
        }
    }

    #[test]
    fn cursor_only_inputs_accept_a_declaring_schema_and_ignore_optional_extras() {
        let table = cursor_only_table();
        // The exact Dropbox shape.
        table
            .check_continuation_inputs(Some(&serde_json::json!({
                "type": "object",
                "properties": {"cursor": {"type": "string"}},
                "required": ["cursor"],
                "additionalProperties": false
            })))
            .expect("cursor declared and nothing else required");
        // An additive upstream release that grows an OPTIONAL input must not
        // fail startup: a cursor-only request is still valid against it.
        table
            .check_continuation_inputs(Some(&serde_json::json!({
                "type": "object",
                "properties": {"cursor": {"type": "string"}, "hint": {"type": "string"}},
                "required": ["cursor"]
            })))
            .expect("an optional extra input is not a breaking change");
        // `required` absent entirely means nothing is mandatory.
        table
            .check_continuation_inputs(Some(&serde_json::json!({
                "type": "object",
                "properties": {"cursor": {"type": "string"}}
            })))
            .expect("no required list means nothing is required");
    }

    #[test]
    fn cursor_only_inputs_reject_every_way_the_claim_can_break() {
        let table = cursor_only_table();
        for (schema, expected) in [
            // The cursor input is not a declared property, so the request's
            // one field is the undeclared extra a strict schema rejects.
            (
                Some(serde_json::json!({
                    "type": "object",
                    "properties": {"pageToken": {"type": "string"}},
                    "additionalProperties": false
                })),
                "does not declare the cursor input 'cursor'",
            ),
            // A mandatory input the pack never sends on a continuation page.
            (
                Some(serde_json::json!({
                    "type": "object",
                    "properties": {"cursor": {"type": "string"}, "path": {"type": "string"}},
                    "required": ["cursor", "path"]
                })),
                "requires input(s) [path]",
            ),
            // Unverifiable: default-deny rather than trust.
            (
                Some(serde_json::json!({"type": "object"})),
                "no input schema",
            ),
            (None, "no input schema"),
        ] {
            let err = table
                .check_continuation_inputs(schema.as_ref())
                .expect_err(expected);
            let rendered = err.to_string();
            assert!(
                rendered.contains(expected),
                "want {expected:?} in: {rendered}"
            );
            assert!(
                rendered.contains("t.action_continue"),
                "the continuation action names itself: {rendered}"
            );
        }
    }

    #[test]
    fn a_continuation_on_a_non_cursor_strategy_is_a_pack_bug_not_a_pass() {
        // Unreachable through YAML (the loader nests `continuation` under the
        // cursor strategy), so this pins that a hand-built table cannot slip
        // through the gate by having no cursor input to check.
        let table = SourcePackTable {
            pagination: PaginationStrategy::SinglePage {
                next_cursor_path: None,
            },
            ..cursor_only_table()
        };
        let err = table
            .check_continuation_inputs(Some(&serde_json::json!({
                "properties": {"cursor": {"type": "string"}}
            })))
            .expect_err("a continuation without a cursor strategy cannot be honored");
        assert!(
            err.to_string().contains("non-cursor pagination strategy"),
            "{err}"
        );
    }

    #[test]
    fn tables_without_a_cursor_only_continuation_are_never_gated_on_inputs() {
        // Every pre-existing pack: no continuation at all.
        leaked_table("t.plain")
            .check_continuation_inputs(None)
            .expect("no continuation, nothing to check");
        // A `full`-input continuation sends the assembled input, so the
        // CURSOR-ONLY reasoning does not apply to it. It is not ungated —
        // `check_full_continuation_inputs` covers it; this only pins that
        // the two gates do not overlap.
        let full = full_continuation_table();
        full.check_continuation_inputs(None)
            .expect("inputs: full is not an input-shape claim");
    }

    #[test]
    fn a_malformed_required_is_refused_rather_than_read_as_empty() {
        // The asymmetry this closes: a missing `properties` always
        // default-DENIED, while a `required` that could not be parsed
        // default-ALLOWED. Both come from the same untrusted discovery
        // payload, so both fail closed now.
        let table = cursor_only_table();
        for required in [
            serde_json::json!("cursor"),
            serde_json::json!({"0": "cursor"}),
            serde_json::json!(["cursor", 7]),
            serde_json::json!([null]),
        ] {
            let err = table
                .check_continuation_inputs(Some(&serde_json::json!({
                    "type": "object",
                    "properties": {"cursor": {"type": "string"}},
                    "required": required,
                })))
                .expect_err("an unreadable `required` is not an empty one");
            assert!(
                err.to_string().contains("cannot read"),
                "the gate says it did not understand the schema: {err}"
            );
        }
    }

    /// A cursor table whose continuation targets a DIFFERENT action with
    /// the full input — the `inputs:` default, and the shape a new pack
    /// lands in by omission.
    fn full_continuation_table() -> SourcePackTable {
        SourcePackTable {
            required_resources: &["path"],
            optional_resources: &["directOnly"],
            fixed_inputs: &[("recursive", FixedValue::Bool(true))],
            continuation: Some(CursorContinuation {
                action_id: "t.action_continue",
                expected_fingerprint: "aa",
                cursor_only: false,
            }),
            ..cursor_only_table()
        }
    }

    #[test]
    fn a_full_continuation_to_another_action_accepts_a_schema_that_admits_the_input() {
        let table = full_continuation_table();
        table
            .check_full_continuation_inputs(Some(&serde_json::json!({
                "type": "object",
                "properties": {
                    "cursor": {"type": "string"}, "limit": {"type": "integer"},
                    "path": {"type": "string"}, "directOnly": {"type": "boolean"},
                    "recursive": {"type": "boolean"}
                },
                "required": ["path"],
                "additionalProperties": false
            })))
            .expect("every sendable key is declared and every required key is sent");
        // `additionalProperties` absent defaults to `true`, so an undeclared
        // key the pack may send is not a rejection — no false alarm.
        table
            .check_full_continuation_inputs(Some(&serde_json::json!({
                "type": "object",
                "properties": {"cursor": {"type": "string"}}
            })))
            .expect("a permissive schema accepts extras by JSON Schema default");
        // A same-action continuation is sound by construction: one action,
        // one input schema, and page one already satisfied it.
        SourcePackTable {
            continuation: Some(CursorContinuation {
                action_id: "t.action",
                expected_fingerprint: "aa",
                cursor_only: false,
            }),
            ..full_continuation_table()
        }
        .check_full_continuation_inputs(None)
        .expect("the same action cannot disagree with itself");
        // And a cursor-only continuation belongs to the other gate.
        cursor_only_table()
            .check_full_continuation_inputs(None)
            .expect("cursor_only is checked by check_continuation_inputs");
    }

    #[test]
    fn a_full_continuation_to_another_action_rejects_both_ways_a_request_can_400() {
        let table = full_continuation_table();
        for (schema, expected) in [
            // Mandatory on the continue action, and NOT something the pack
            // sends on every request: `directOnly` is optional, so a
            // binding may omit it.
            (
                Some(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "cursor": {"type": "string"}, "limit": {"type": "integer"},
                        "path": {"type": "string"}, "directOnly": {"type": "boolean"},
                        "recursive": {"type": "boolean"}
                    },
                    "required": ["directOnly"]
                })),
                "requires input(s) [directOnly]",
            ),
            // Strict schema that does not declare keys the pack can send.
            (
                Some(serde_json::json!({
                    "type": "object",
                    "properties": {"cursor": {"type": "string"}},
                    "additionalProperties": false
                })),
                "does not declare input(s) [directOnly, limit, path, recursive]",
            ),
            // Unverifiable: default-deny rather than trust.
            (
                Some(serde_json::json!({"type": "object"})),
                "no input properties",
            ),
            (None, "no input schema"),
        ] {
            let err = table
                .check_full_continuation_inputs(schema.as_ref())
                .expect_err(expected);
            let rendered = err.to_string();
            assert!(
                rendered.contains(expected),
                "want {expected:?} in: {rendered}"
            );
            assert!(
                rendered.contains("t.action_continue"),
                "the continuation action names itself: {rendered}"
            );
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
