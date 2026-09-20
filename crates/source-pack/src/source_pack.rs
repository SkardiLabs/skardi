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

use crate::error::OpenConnectorError;
use crate::filters::FilterMapping;
use crate::pagination::{CursorContinuation, PaginationStrategy};
use crate::schema::FieldMapping;

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

/// How an action's response carries its rows.
///
/// Every table mapped before 2026-09 locates a row *array* (`$.items`), and
/// that stays the default: a pack that says nothing about shape behaves
/// exactly as it did. [`RowShape::Object`] covers point-read actions whose
/// entire response IS one row — `feishu.get_document_content`, Notion's
/// rendered Markdown — which have no array anywhere to point a row path at.
///
/// This is a response-shape declaration, not a query feature: the object is
/// handed to the same converter as an array of one, so there is a single
/// conversion path and object rows get the same schema, projection, and
/// error handling as every other row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RowShape {
    /// The row path resolves to an array of row objects.
    #[default]
    Array,
    /// The row path resolves to a single object that IS the row. Valid only
    /// with single-page pagination.
    Object,
}

/// One stable table definition inside a source pack.
#[derive(Debug, Clone, Copy)]
pub struct SourcePackTable {
    /// Stable table identifier, e.g. `github.issues`.
    pub id: &'static str,
    /// Open Connector action backing the table (read-only by construction).
    pub action_id: &'static str,
    /// Fixed row path of the rows in the action response. For
    /// [`RowShape::Array`] this locates the row array; for
    /// [`RowShape::Object`] it is the root `$`.
    pub row_path: &'static str,
    /// Whether `row_path` resolves to an array of rows or to a single row
    /// object. Defaults to [`RowShape::Array`]; see [`RowShape`].
    pub row_shape: RowShape,
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

    /// The ONE input-side gate a registration path calls: verify this
    /// table's continuation declaration against the continuation action's
    /// discovered INPUT schema. `Ok(())` for a table with no continuation.
    ///
    /// Deliberately a single entry point rather than two public checks the
    /// caller picks between. `inputs:` has two spellings and each needs a
    /// different argument, but "did we verify the continuation's inputs?"
    /// is one question, and two call sites per entry point were two
    /// independently droppable lines — a review round found both deletable
    /// with the suite still green. Dispatching here means an entry point
    /// either asks the question or visibly does not.
    ///
    /// # Errors
    /// [`OpenConnectorError::ActionContractMismatch`] naming the
    /// continuation action and the specific disagreement.
    pub fn check_continuation_inputs(
        &self,
        input_schema: Option<&Value>,
    ) -> Result<(), OpenConnectorError> {
        let Some(continuation) = self.continuation else {
            return Ok(());
        };
        if continuation.cursor_only {
            self.check_cursor_only_continuation_inputs(input_schema)
        } else {
            self.check_full_continuation_inputs(input_schema)
        }
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
    fn check_cursor_only_continuation_inputs(
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
        // Two failures, two diagnostics: an action that publishes NOTHING
        // and one that publishes a schema this gate cannot read from are
        // both refused, but they live in different layers of the operator's
        // gateway. Collapsing them sent someone hunting for a missing
        // schema that was in fact present and shapeless.
        let Some(schema) = input_schema else {
            return Err(mismatch(format!(
                "action '{}' publishes no input schema, so `inputs: cursor_only` cannot be \
                 verified; a claim about inputs that the gateway will not confirm is refused \
                 rather than discovered as a 400 on page two",
                continuation.action_id
            )));
        };
        let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
            return Err(mismatch(format!(
                "action '{}' publishes no input properties, so `inputs: cursor_only` cannot \
                 be verified; a schema with no readable `properties` confirms nothing and is \
                 refused rather than discovered as a 400 on page two",
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
            required_beyond_cursor(Some(schema), cursor_param).map_err(|reason| {
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
    ///
    /// `pub` because `packs::loader::validate_table` needs the SAME
    /// mapping for its input-namespace collision gates. It used to carry
    /// its own copy of this match, and the two had already drifted once —
    /// `Keyset` and `SinglePage` landed with the Discord pack and only one
    /// side was widened. One definition, so a fifth strategy (or a new
    /// page-size field on an existing one) cannot weaken one consumer
    /// silently.
    pub fn pagination_input_keys(&self) -> Vec<&'static str> {
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

    /// Keys the pack sends on every request the CONTINUATION gates govern:
    /// the complete-collection pins, the resources a binding MUST supply,
    /// and the pagination inputs. Optional resources and pushed filters are
    /// excluded — a binding may omit them, so they cannot satisfy an
    /// action's `required`.
    ///
    /// Scoped to continuation requests deliberately: the pagination half
    /// includes the cursor, which `Pagination::apply` inserts only once
    /// `next_token` is `Some` — never on page one. On a continuation page
    /// the cursor genuinely is always present, which is what makes it
    /// guaranteed here. A future gate checking the OPENING action's
    /// `required` must NOT reuse this set: it would report a mandatory
    /// cursor as satisfied on page one, which is exactly the hard 400
    /// these gates exist to prevent.
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
    /// `pub` so the engine's builtin-pack suite can walk the registered
    /// set — it did when these tests lived beside this type, and moving
    /// the tests to sit with the CONTENT they assert about did not change
    /// who reads this.
    pub packs: HashMap<&'static str, &'static SourcePack>,
}

impl SourcePackRegistry {
    /// A registry over an explicit set of packs.
    ///
    /// The caller supplies the content. This crate deliberately ships no
    /// built-in list: eleven hardcoded provider assets would make a library
    /// carry its consumer's choices, and cloud's set is not the engine's. The
    /// engine's `builtins()` is one caller of this.
    /// # Errors
    ///
    /// [`OpenConnectorError::DuplicateSourcePack`] when two packs claim the
    /// same name. This is public so a consumer can combine sets — its own
    /// packs with an override, say — and that is exactly when a collision is
    /// reachable. Keeping the last silently made iterator order decide which
    /// definition every later registration, version pin and action contract
    /// resolved against, with nothing said.
    pub fn from_packs(
        packs: impl IntoIterator<Item = &'static SourcePack>,
    ) -> Result<Self, OpenConnectorError> {
        let mut map: HashMap<&'static str, &'static SourcePack> = HashMap::new();
        for pack in packs {
            if map.insert(pack.name, pack).is_some() {
                return Err(OpenConnectorError::DuplicateSourcePack {
                    pack: pack.name.to_string(),
                });
            }
        }
        Ok(Self { packs: map })
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
