//! The built-in source packs this Skardi build ships, and their tests.
//!
//! `skardi-source-pack` owns `SourcePackRegistry`; this owns what goes in it.
//! The split is content versus machinery: eleven provider assets are this
//! binary's choices, and a library that hardcoded them would be making its
//! consumers' choices too — cloud registers a different set against the same
//! type.
//!
//! The tests came here with the content rather than staying with the type they
//! call. Every one of them asserts something about the eleven assets — that
//! github's pack is registered, that iteration is in name order, that the
//! reconciled set matches what the cloud pin carried — which is a fact about
//! this build, not about a registry.

use skardi_source_pack::OpenConnectorError;
use skardi_source_pack::source_pack::SourcePackRegistry;

use super::packs;

/// The built-in source packs this Skardi build ships.
///
/// The list lives here rather than in `skardi-source-pack` because it is
/// CONTENT, not machinery: eleven provider assets are this binary's choices,
/// and a library that hardcoded them would be making its consumers' choices
/// too. The pack owns `SourcePackRegistry`; callers supply what goes in it,
/// which is what lets cloud register a different set against the same type.
///
/// # Errors
/// [`OpenConnectorError::SourcePackAssetInvalid`] when an embedded asset fails
/// to parse or validate — a build defect surfaced as a registration
/// diagnostic.
pub fn builtin_pack_registry() -> Result<SourcePackRegistry, OpenConnectorError> {
    Ok(SourcePackRegistry::from_packs([
        packs::mock::pack()?,
        packs::dropbox::pack()?,
        packs::github::pack()?,
        packs::gmail::pack()?,
        packs::notion::pack()?,
        packs::slack::pack()?,
        packs::feishu::pack()?,
        packs::discord::pack()?,
        packs::outlook::pack()?,
        packs::one_drive::pack()?,
        packs::google_drive::pack()?,
    ]))
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use skardi_source_pack::pagination::*;
    use skardi_source_pack::source_pack::*;

    #[test]
    fn only_the_named_shipped_tables_are_object_shaped() {
        // Object rows stay opt-in and enumerated: a table may not acquire
        // the shape by accident, because a `row_shape: object` typo turns a
        // list endpoint into a silent one-row table. This began life as
        // `every_shipped_table_is_array_shaped`, asserting the list was
        // EMPTY, with a comment promising to name the first legitimate
        // declaration rather than be deleted. `feishu.document_content` is
        // that declaration — the point-read whose response object IS the
        // row — so the empty assertion became this allowlist.
        const OBJECT_ROW_TABLES: &[&str] = &["feishu.document_content"];

        let registry = super::builtin_pack_registry().expect("embedded assets parse");
        let mut object_tables = Vec::new();
        for pack in registry.packs.values() {
            for table in pack.tables {
                if table.row_shape == RowShape::Object {
                    object_tables.push(table.id);
                }
            }
        }
        object_tables.sort_unstable();
        assert_eq!(
            object_tables, OBJECT_ROW_TABLES,
            "object rows are enumerated; add a table here only deliberately"
        );
    }

    /// The reconcile's whole point, asserted against what ships.
    ///
    /// skardi-cloud pinned `skardi` at a rev on
    /// `claude/slack-messages-ts-pushdown` because main lacked these tables,
    /// and a live corpus reads them. Main and that branch had independently
    /// implemented the same design contract (a pack table may be one
    /// object), so the branch could not simply merge; main's `RowShape`
    /// kept, the branch's pack content re-landed on top. If a later edit
    /// drops one of these, the pin move that this test exists to unblock
    /// silently empties the corpus instead of failing — so assert the
    /// declarations, not merely the names.
    #[test]
    fn the_reconciled_pack_set_keeps_what_the_cloud_pin_carried() {
        let registry = super::builtin_pack_registry().expect("embedded assets parse");

        // slack.messages, with the `conversations.history` time window that
        // makes an incremental scan a delta instead of a full history.
        let slack = registry.require("slack").expect("slack pack ships");
        let messages = registry
            .table(slack, "messages")
            .expect("slack.messages survived the reconcile");
        assert_eq!(messages.action_id, "slack.get_channel_messages");
        assert!(
            messages
                .filters
                .iter()
                .any(|f| f.input_field == "oldest" && f.column == "sent_at"),
            "the sent_at -> oldest pushdown is the table's incremental story"
        );

        // feishu's two document tables: the structural block listing, and
        // the point-read whose response object IS the row.
        let feishu = registry.require("feishu").expect("feishu pack ships");
        let blocks = registry
            .table(feishu, "document_blocks")
            .expect("feishu.document_blocks survived the reconcile");
        assert_eq!(blocks.row_shape, RowShape::Array);

        let content = registry
            .table(feishu, "document_content")
            .expect("feishu.document_content survived the reconcile");
        assert_eq!(content.row_shape, RowShape::Object);
        assert_eq!(content.row_path, "$");
    }

    #[test]
    fn builtin_mock_pack_is_registered() {
        let registry = super::builtin_pack_registry().expect("embedded assets parse");
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
        let registry = super::builtin_pack_registry().expect("embedded assets parse");
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
        let registry = super::builtin_pack_registry().expect("embedded assets parse");
        let err = registry.require("jira").unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::SourcePackNotFound { ref name } if name == "jira"
        ));
    }

    #[test]
    fn builtin_github_pack_is_registered() {
        let registry = super::builtin_pack_registry().expect("embedded assets parse");
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
        let registry = super::builtin_pack_registry().expect("embedded assets parse");
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
        let registry = super::builtin_pack_registry().expect("embedded assets parse");
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
        let registry = super::builtin_pack_registry().expect("embedded assets parse");
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
        let registry = super::builtin_pack_registry().expect("embedded assets parse");
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

        let registry = super::builtin_pack_registry().expect("embedded assets parse");
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
        let registry = super::builtin_pack_registry().expect("embedded assets parse");
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
            row_shape: RowShape::Array,
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
            // Unverifiable: default-deny rather than trust — and the two
            // ways it can be unverifiable get different diagnostics, so a
            // present-but-shapeless schema is not reported as absent.
            (
                Some(serde_json::json!({"type": "object"})),
                "no input properties",
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
    fn a_table_without_a_continuation_is_never_gated_on_inputs() {
        // Every pre-existing pack: no continuation at all, so the single
        // gate short-circuits before either arm.
        leaked_table("t.plain")
            .check_continuation_inputs(None)
            .expect("no continuation, nothing to check");
    }

    #[test]
    fn the_single_gate_routes_each_inputs_spelling_to_its_own_arm() {
        // What the dispatcher must NOT do is apply the cursor-only
        // reasoning to `inputs: full` or vice versa. Same argument
        // (`None`), two different verdicts, which is only possible if the
        // routing is real:
        //   - cursor_only refuses an unverifiable claim about inputs;
        //   - full to a DIFFERENT action refuses it too, but for its own
        //     reason and with its own wording.
        let cursor_only = cursor_only_table()
            .check_continuation_inputs(None)
            .expect_err("cursor_only cannot be verified against no schema");
        assert!(
            cursor_only.to_string().contains("`inputs: cursor_only`"),
            "the cursor-only arm answered: {cursor_only}"
        );
        let full = full_continuation_table()
            .check_continuation_inputs(None)
            .expect_err("a full continuation to another action needs its schema");
        assert!(
            full.to_string().contains("full-input continuation"),
            "the full arm answered: {full}"
        );
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
        let registry = super::builtin_pack_registry().expect("embedded assets parse");
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
