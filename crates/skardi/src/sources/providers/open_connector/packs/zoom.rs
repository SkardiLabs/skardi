//! Zoom source pack: AI Companion meeting summaries over the Open
//! Connector `zoom.*` summary actions, landed in the meeting-minutes
//! contract shape — `meetings`, `meeting_notes`, `meeting_action_items` —
//! that every meeting pack writes (skardi-cloud
//! `skardi-source-contract/src/meetings.rs`; meeting-minutes design §3.2).
//!
//! **The tool is the author of the minutes and this pack keeps its
//! record.** Nothing here calls a model, extracts a name from a sentence
//! or splits Zoom's Markdown into items: a column is filled from exactly
//! one field of the action's response, a contract column Zoom has no
//! field for is an explicit NULL on every row, and an action item is a
//! row only because Zoom returned it as one string of `nextSteps[]`
//! (design §0.3, D13, D14).
//!
//! **The wire contract is Open Connector's, not Zoom's REST API's.** The
//! zoom executors (SkardiLabs/open-connector#21, branch
//! `claude/zoom-meeting-summaries`, ab02d6da) REBUILD rows: camelCase keys
//! (`meetingUuid`, `summaryContent`), Zoom's `next_page_token` re-emitted
//! as a top-level `nextPageToken` that is `null` on the last page (Zoom
//! sends `""`), the summary nested under `summary` beside an `available`
//! flag. Inputs are camelCase strict (`additionalProperties: false`): a
//! wrong key (`page_size`, `meeting_uuid`) is a hard 400, and every input
//! set this pack generates reached the credential wall against a local
//! gateway built from that branch on 2026-11-09 (`pageSize`+`from`+`to`,
//! the same plus `nextPageToken`, `meetingUuid` alone), which is where the
//! contracts under `fixtures/zoom/contracts/` were captured.
//!
//! Design decisions, per the integration design spec, the source-pack
//! admission gate and the meeting-minutes design:
//!
//! - **Three contract tables, not Zoom-shaped ones.** The cloud's OKF
//!   recipe keys, its `item_tables` and its ontology recipe all name pack
//!   tables, so the pack's tables ARE `meetings`, `meeting_notes` and
//!   `meeting_action_items` with the contract's column names, and the two
//!   actions are their sources: the listing fills `meetings`; the
//!   per-meeting summary fills `meeting_notes` and `meeting_action_items`.
//! - **Derived rows, declared, not computed.** The contract needs what no
//!   wire key gives — `provider: zoom` and `kind: summary` on every row,
//!   one row per `nextSteps[]` string, an item id formatted from the note
//!   id and the position, zero rows for an unavailable summary. This pack
//!   adds the loader's `derive:` block (`skardi_source_pack::derive`) for
//!   exactly those four — `constants`, `explode`, `format`,
//!   `skip_when_null` — applied after a page is extracted and before the
//!   ordinary converter reads it, in the shared scan so the engine and the
//!   cloud's consumers see the same rows. Derived keys overwrite wire keys
//!   of the same name (the pack's label is the pack's), and pagination
//!   advances on the RAW page, so an explode that yields nothing cannot
//!   read as an empty page.
//! - **`meetings` is a cursor listing** (`nextPageToken` in, top-level
//!   `$.nextPageToken` out, `pageSize` 30 — Zoom's own default). The
//!   executor always sends the key and spells the end as `null`; the
//!   engine also accepts absent and `""`. **The token expires fifteen
//!   minutes after Zoom issues it** (findings §2.1): an expired token is
//!   Zoom's 400, which the gateway forwards as `invalid_input` at HTTP
//!   400 — a failed scan with the gateway's code and the action named,
//!   never a truncated one. The cloud list work walks a time window inside
//!   one work for that reason; the pack only declares the cursor.
//! - **`from`/`to` are optional resources**, Zoom's own window inputs in
//!   UTC `yyyy-MM-ddTHH:mm:ssZ`, forwarded verbatim. A binding that omits
//!   them reads Zoom's default window. They are not pushed filters: the
//!   loader refuses a filter input that is also a resource, and a window
//!   is a pair the caller sets. `userId` is never sent (the action
//!   defaults to `me`: the connecting user's own summaries, which is what
//!   §3.4's reader rule wants) and `timeFilterField` is not declared
//!   (Zoom's default, `summary_start_time`; a binding cannot switch to
//!   `summary_created_time` today — a deliberate non-surface, recorded).
//! - **`series_id` is the numeric meeting id, rendered as text.** The UUID
//!   is per occurrence; the numeric `meetingId` is what a recurring meeting
//!   keeps across occurrences (design §3.3). The wire spells it as an
//!   integer or a string (the action's `anyOf`), the contract column is
//!   text, so a `format` template renders either. Null stays null.
//! - **No attendees, no url, no calendar link on a Zoom meeting.** A
//!   summary carries none (findings §2.1); `attendee_emails`,
//!   `attendee_ids`, `url` and `calendar_event_id` are explicit-NULL
//!   constants, present so one `meetings` table serves every tool and
//!   never read from a wire key Zoom might one day grow.
//! - **`meeting_notes` and `meeting_action_items` are object-row tables**
//!   over `zoom.get_meeting_summary`, one binding per meeting UUID
//!   (`meetingUuid`, required). A UUID can start with `/` or contain `//`;
//!   encoding it into Zoom's path — twice, as Zoom requires — is the
//!   action's job, and the pack passes it verbatim. The cloud fetches the
//!   summary per listed meeting.
//! - **`note_id` is the meeting UUID.** Zoom keeps one summary per meeting
//!   instance and gives it no id of its own, so the UUID is the note's
//!   identity too; `kind` is `summary`, `producer` is `zoom_ai_companion`,
//!   `body` is `summaryContent` (Zoom's unified Markdown — "the current
//!   field"), `title` is `summaryTitle`, `url` is `summaryDocUrl`,
//!   `meeting_started_at` is `meetingStartTime` (so the storage cap evicts
//!   the oldest meetings' notes first), `document_path` is NULL. The
//!   legacy `summaryOverview` / `summaryDetails` are not columns: Zoom
//!   marks them deprecated and the Markdown holds them.
//! - **One action item per `nextSteps[]` string, verbatim.** Zoom marks
//!   `next_steps` deprecated and also returns `edited_summary.next_steps`
//!   when someone edited the summary. The pack reads the edited list when
//!   it is present and non-empty, else the legacy list; when both are
//!   absent or empty it writes zero item rows and the note still stands
//!   (design §3.3: when Zoom stops returning the array, Zoom yields no
//!   action-item rows, like Meet). The choice of "non-empty edited wins"
//!   over "edited present wins" is deliberate and unverified live: it
//!   keeps a summary whose editor touched only the overview from losing
//!   its items, at the cost of resurrecting items an editor deleted to
//!   none — findings §5 question 2 carries it to the live pass.
//!   `item_id` is `<note_id>#<position>`, never a bare position (the
//!   identity is one namespace per source); `text` is the string;
//!   `owner_name`, `owner_email`, `owner_id`, `due`, `done` are NULL — no
//!   extraction from the text, ever (D13/D14).
//! - **Zoom's `403 2305` is zero rows, not a failure.** The account setting
//!   "Only share meeting summaries by email", or a summary in Trash, makes
//!   Zoom answer 403 with code 2305; the action turns THAT one into a
//!   successful envelope `{available: false, unavailableReason:
//!   "shared_by_email_only", summary: null}` (open-connector#21). The
//!   tables `skip_when_null: summary`, so such a meeting has no note and
//!   no items; the engine's object-row rule — a null root fails loudly —
//!   is untouched because the root is still an object. Every other 403
//!   stays a gateway failure envelope (`authorization_failed`) and fails
//!   the scan with its code and the action named.
//! - **Timestamps are UTC RFC 3339 to the second** (`2026-11-02T09:00:00Z`),
//!   read as `timestamp_ms_utc` (Arrow `Timestamp(ms, UTC)`).
//! - **Nullability follows the contract exactly**: `provider` and each
//!   table's identity (`meeting_id`; `note_id` on notes; `item_id` on
//!   items) non-null, everything else nullable — including descriptive
//!   fields the action declares `nullable`, so a summary without a topic
//!   or a host email is a row with NULLs, not a failed scan.
//! - **`error_path: None`**: the executors consume Zoom's in-band errors
//!   (`createZoomError` throws), so provider failures arrive as gateway
//!   failure envelopes, pinned by e2e tests for both actions.
//! - **Fingerprints are pinned** from the gateway capture above and cover
//!   the whole declared schema, `anyOf` branches included. The coverage
//!   walker (`fingerprint_uncovered_columns`) reads `properties` only: on
//!   `meetings` it resolves every wire-mapped column and reports the six
//!   derived ones (`provider`, `series_id`, the four NULL constants),
//!   which no contract can declare; on the two summary tables it reports
//!   EVERY column, because `summary` is `anyOf: [object, null]` and the
//!   walker does not descend branches (gmail's case 2). Pinned so the
//!   blind spot stays visible, not a hole in the gate. The input halves of
//!   the contracts are captured too and the generated input sets are
//!   locked against them by a test, as gmail does.
//! - **Not yet done, and stated**: the live pass against a real Zoom
//!   account (phase 4 of the source-pack skill) — real rows through
//!   skardi-server, the participant-token question (findings §5.1), and
//!   whether `next_steps` is still returned (§5.2). The fixtures are
//!   SYNTHETIC: shaped by the executor source and its own tests, with
//!   invented UUIDs, `host@example.com`, "Weekly sync", "Send the agenda".
//!   No real Zoom data appears anywhere in this pack; summaries are
//!   customer content and the live pass must redact before it replaces
//!   them.

use std::sync::OnceLock;

use crate::sources::providers::open_connector::error::OpenConnectorError;
use crate::sources::providers::open_connector::source_pack::SourcePack;

use super::loader;

static PACK: OnceLock<Result<SourcePack, String>> = OnceLock::new();

/// The Zoom pack, parsed once from the embedded YAML asset.
///
/// # Examples
///
/// ```
/// use skardi::sources::providers::open_connector::packs::zoom;
///
/// let pack = zoom::pack().expect("the shipped asset is valid");
/// let names: Vec<&str> = pack.tables.iter().map(|t| t.id).collect();
/// assert_eq!(
///     names,
///     ["zoom.meeting_action_items", "zoom.meeting_notes", "zoom.meetings"]
/// );
/// ```
pub fn pack() -> Result<&'static SourcePack, OpenConnectorError> {
    loader::builtin("zoom.yaml", include_str!("zoom.yaml"), &PACK)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::hierarchy::HierarchyLevel;
    use crate::sources::providers::open_connector::action_registry::fingerprint_schema;
    use crate::sources::providers::open_connector::json_to_arrow::RowConverter;
    use crate::sources::providers::open_connector::pagination::PaginationStrategy;
    use crate::sources::providers::open_connector::row_path::RowPath;
    use crate::sources::providers::open_connector::source_pack::{FixedValue, SourcePackTable};
    use crate::sources::providers::open_connector::testutil::{
        EnvVarGuard, MockGateway, MockResponse, boolean, collect, column_values,
        convert_first_page, discovery_ok, envelope_err, envelope_ok, execute_inputs,
        fingerprint_uncovered_columns, input_keys, utf8,
    };
    use crate::sources::providers::open_connector::{
        OpenConnectorConfig, OpenConnectorGateways, register_open_connector_tables,
        register_open_connector_udtfs,
    };
    use arrow::array::{Array, ListArray, TimestampMillisecondArray};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use serde_json::{Value, json};

    const UUID: &str = "Zm9vYmFyMTIzNDU2Nzg5MA==";
    const SLASH_UUID: &str = "/aB3cD4eF5gH6iJ7kL8mN9==";

    /// Look up a table by short name; the assets are test-pinned to parse.
    fn table(short: &str) -> &'static SourcePackTable {
        pack()
            .expect("embedded asset is test-pinned to parse")
            .tables
            .iter()
            .find(|t| t.id.rsplit('.').next() == Some(short))
            .unwrap_or_else(|| panic!("table {short}"))
    }

    /// Discovery serving the captured contracts, so every mock
    /// registration exercises the fingerprint gate's pass side.
    fn zoom_discovery(path: &str) -> MockResponse {
        let output_schema = if path.ends_with("zoom.list_user_meeting_summaries") {
            include_str!("fixtures/zoom/contracts/list_user_meeting_summaries.json")
        } else if path.ends_with("zoom.get_meeting_summary") {
            include_str!("fixtures/zoom/contracts/get_meeting_summary.json")
        } else {
            r#"{"type": "object"}"#
        };
        MockResponse::ok(&discovery_ok("{}", output_schema, true, None))
    }

    fn timestamps<'a>(batch: &'a RecordBatch, name: &str) -> &'a TimestampMillisecondArray {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("column {name}"))
            .as_any()
            .downcast_ref()
            .unwrap_or_else(|| panic!("column {name} is not a timestamp"))
    }

    fn list<'a>(batch: &'a RecordBatch, name: &str) -> &'a ListArray {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("column {name}"))
            .as_any()
            .downcast_ref()
            .unwrap_or_else(|| panic!("column {name} is not a list"))
    }

    fn convert_fixture(table: &SourcePackTable, fixture: &str) -> RecordBatch {
        let page: Value = serde_json::from_str(fixture).expect("fixture parses");
        convert_first_page(table, &page)
    }

    // ── Contract tests: SYNTHETIC fixtures shaped by the executor source
    // and its tests (no live Zoom data exists for this pack yet — the
    // module doc says so). They pin the conversion contract per the
    // admission gate: null-bearing, null-parent, nested, extra-field,
    // empty, and schema-mismatch — plus the derivation the contract
    // shape needs. ─

    #[test]
    fn meetings_fixture_converts_with_the_contract_columns() {
        let batch = convert_fixture(
            table("meetings"),
            include_str!("fixtures/zoom/summaries.json"),
        );
        assert_eq!(batch.num_rows(), 3);
        // The pack's label on every row, whatever the wire says.
        assert!((0..3).all(|i| utf8(&batch, "provider").value(i) == "zoom"));
        assert_eq!(utf8(&batch, "meeting_id").value(0), UUID);
        assert_eq!(utf8(&batch, "meeting_id").value(1), SLASH_UUID);
        // series_id: the numeric meeting id as text, from an integer AND
        // from a string spelling; null stays null.
        assert_eq!(utf8(&batch, "series_id").value(0), "81234567890");
        assert_eq!(utf8(&batch, "series_id").value(1), "81234567891");
        assert!(utf8(&batch, "series_id").is_null(2));
        assert_eq!(utf8(&batch, "title").value(0), "Weekly sync");
        assert!(utf8(&batch, "title").is_null(2));
        assert_eq!(utf8(&batch, "organizer_email").value(0), "host@example.com");
        assert!(utf8(&batch, "organizer_email").is_null(1));
        assert_eq!(
            utf8(&batch, "organizer_id").value(0),
            "AbCdEfGhIjKlMnOpQrStUv"
        );
        assert!(utf8(&batch, "organizer_id").is_null(2));
        // UTC RFC 3339 seconds parse; a null end stays null.
        let started = timestamps(&batch, "started_at");
        assert_eq!(started.value(0), 1_793_610_000_000, "2026-11-02T09:00:00Z");
        assert!(!timestamps(&batch, "ended_at").is_null(0));
        assert!(timestamps(&batch, "ended_at").is_null(1));
        // What Zoom has no field for is NULL on every row.
        for column in ["url", "calendar_event_id"] {
            assert!((0..3).all(|i| utf8(&batch, column).is_null(i)), "{column}");
        }
        for column in ["attendee_emails", "attendee_ids"] {
            assert!((0..3).all(|i| list(&batch, column).is_null(i)), "{column}");
        }
    }

    #[test]
    fn a_wire_row_cannot_shadow_the_derived_columns() {
        // A derived key wins over a wire key of the same name: the pack's
        // `provider` is zoom even if upstream ever emits one, and the
        // attendee columns stay NULL even if upstream grows them.
        let batch = convert_first_page(
            table("meetings"),
            &json!({"summaries": [{
                "meetingUuid": "u-1", "meetingId": 1, "provider": "spoofed",
                "attendee_ids": ["someone"], "series_id": "wrong",
                "meetingStartTime": "2026-11-02T09:00:00Z",
            }]}),
        );
        assert_eq!(utf8(&batch, "provider").value(0), "zoom");
        assert_eq!(utf8(&batch, "series_id").value(0), "1");
        assert!(list(&batch, "attendee_ids").is_null(0));
    }

    #[test]
    fn meeting_notes_fixture_converts_one_summary_into_one_note() {
        let batch = convert_fixture(
            table("meeting_notes"),
            include_str!("fixtures/zoom/summary.json"),
        );
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(utf8(&batch, "provider").value(0), "zoom");
        assert_eq!(utf8(&batch, "meeting_id").value(0), UUID);
        assert_eq!(
            utf8(&batch, "note_id").value(0),
            UUID,
            "the UUID is the note's id"
        );
        assert_eq!(utf8(&batch, "kind").value(0), "summary");
        assert_eq!(utf8(&batch, "producer").value(0), "zoom_ai_companion");
        assert_eq!(utf8(&batch, "title").value(0), "Weekly sync summary");
        assert!(
            utf8(&batch, "body").value(0).starts_with("## Summary\n\n"),
            "summaryContent is the body, as Markdown"
        );
        assert_eq!(
            utf8(&batch, "url").value(0),
            "https://docs.example.com/doc/summary-1"
        );
        assert!(utf8(&batch, "document_path").is_null(0));
        assert_eq!(
            timestamps(&batch, "meeting_started_at").value(0),
            1_793_610_000_000,
            "the meeting's start, copied onto the note"
        );
    }

    #[test]
    fn action_items_fixture_converts_one_row_per_next_step() {
        let batch = convert_fixture(
            table("meeting_action_items"),
            include_str!("fixtures/zoom/summary.json"),
        );
        assert_eq!(batch.num_rows(), 2);
        for i in 0..2 {
            assert_eq!(utf8(&batch, "provider").value(i), "zoom");
            assert_eq!(utf8(&batch, "meeting_id").value(i), UUID);
            assert_eq!(utf8(&batch, "note_id").value(i), UUID);
            // Nothing is read out of the text, ever.
            for column in ["owner_name", "owner_email", "owner_id"] {
                assert!(utf8(&batch, column).is_null(i), "{column}");
            }
            assert!(timestamps(&batch, "due").is_null(i));
            assert!(boolean(&batch, "done").is_null(i));
        }
        assert_eq!(utf8(&batch, "item_id").value(0), format!("{UUID}#0"));
        assert_eq!(utf8(&batch, "item_id").value(1), format!("{UUID}#1"));
        assert_eq!(utf8(&batch, "text").value(0), "Send the agenda");
        assert_eq!(utf8(&batch, "text").value(1), "Book the room");
    }

    #[test]
    fn an_edited_summarys_next_steps_replace_the_legacy_list() {
        // The editor left one next step; the two legacy ones are not rows.
        // The note itself tolerates the fixture's null title and url (a
        // null-bearing row), and the UUID that starts with `/` is carried
        // verbatim into every identity.
        let items = convert_fixture(
            table("meeting_action_items"),
            include_str!("fixtures/zoom/summary_edited.json"),
        );
        assert_eq!(items.num_rows(), 1);
        assert_eq!(utf8(&items, "text").value(0), "Publish the design");
        assert_eq!(utf8(&items, "item_id").value(0), format!("{SLASH_UUID}#0"));
        let notes = convert_fixture(
            table("meeting_notes"),
            include_str!("fixtures/zoom/summary_edited.json"),
        );
        assert_eq!(notes.num_rows(), 1);
        assert_eq!(utf8(&notes, "note_id").value(0), SLASH_UUID);
        assert!(utf8(&notes, "title").is_null(0));
        assert!(utf8(&notes, "url").is_null(0));
        assert!(timestamps(&notes, "meeting_started_at").value(0) > 0);
    }

    #[test]
    fn an_empty_edited_list_falls_back_to_the_legacy_list() {
        // Deliberate and recorded in the module doc: "non-empty edited
        // wins", so an editor who touched only the overview keeps the
        // items. Both lists empty: zero items, the note still stands.
        let mut page: Value =
            serde_json::from_str(include_str!("fixtures/zoom/summary_edited.json"))
                .expect("fixture parses");
        page["summary"]["editedSummary"]["nextSteps"] = json!([]);
        let items = convert_first_page(table("meeting_action_items"), &page);
        assert_eq!(
            column_values(std::slice::from_ref(&items), "text"),
            vec!["Legacy step one", "Legacy step two"]
        );

        page["summary"]["nextSteps"] = json!([]);
        let items = convert_first_page(table("meeting_action_items"), &page);
        assert_eq!(
            items.num_rows(),
            0,
            "no items anywhere: zero rows, no error"
        );
        let notes = convert_first_page(table("meeting_notes"), &page);
        assert_eq!(notes.num_rows(), 1, "the note is written regardless");

        // The legacy key gone entirely (the day Zoom drops it) and the
        // edited summary null: still zero rows, still no error.
        page["summary"]["editedSummary"] = Value::Null;
        page["summary"]
            .as_object_mut()
            .expect("summary object")
            .remove("nextSteps");
        assert_eq!(
            convert_first_page(table("meeting_action_items"), &page).num_rows(),
            0
        );
    }

    #[test]
    fn an_unavailable_summary_converts_to_zero_rows_on_both_tables() {
        // Zoom's 403 2305, as the action answers it: available false,
        // summary null. Not a failure, not a row of NULLs.
        for short in ["meeting_notes", "meeting_action_items"] {
            let batch = convert_fixture(
                table(short),
                include_str!("fixtures/zoom/summary_unavailable.json"),
            );
            assert_eq!(batch.num_rows(), 0, "{short}");
            assert_eq!(
                batch.schema().fields().len(),
                table(short).fields.len(),
                "{short}: an empty batch still carries the fixed schema"
            );
        }
    }

    #[test]
    fn body_mismatch_fixture_fails_with_the_targeted_error() {
        // Admission-gate schema-mismatch fixture (synthetic by nature): a
        // number where Utf8 is declared fails with the row-scoped identity
        // and never the offending value. Derivation runs first, as in the
        // scan, so the error names the derived row.
        let page: Value =
            serde_json::from_str(include_str!("fixtures/zoom/summary_type_mismatch.json"))
                .expect("fixture parses");
        let t = table("meeting_notes");
        let row = RowPath::parse_object_root(t.row_path)
            .expect("object root")
            .row_object(&page, 1)
            .expect("row object");
        let rows = t
            .derive
            .expect("declared")
            .apply(t.id, std::slice::from_ref(row), 1)
            .expect("derives");
        let err = RowConverter::new(t.fields)
            .expect("converter")
            .convert(&rows, 1)
            .expect_err("a number where Utf8 is declared must fail conversion");
        match err {
            OpenConnectorError::ConversionFailed {
                column,
                path,
                page,
                row,
                found,
                ..
            } => {
                assert_eq!(column, "body");
                assert_eq!(path, "$.summary.summaryContent");
                assert_eq!(page, 1);
                assert_eq!(row, 0);
                assert_eq!(found, "a number");
            }
            other => panic!("expected ConversionFailed, got {other}"),
        }
    }

    #[test]
    fn a_next_steps_value_that_is_not_an_array_fails_the_derivation_by_kind() {
        // Upstream shape drift on the exploded path — a string where the
        // array was — fails loudly with the table, step and kind, never
        // quietly as zero items.
        let mut page: Value = serde_json::from_str(include_str!("fixtures/zoom/summary.json"))
            .expect("fixture parses");
        page["summary"]["nextSteps"] = json!("Send the agenda");
        let t = table("meeting_action_items");
        let err = t
            .derive
            .expect("declared")
            .apply(t.id, std::slice::from_ref(&page), 1)
            .expect_err("a non-array next_steps must fail");
        match err {
            OpenConnectorError::RowDerivationFailed {
                table,
                step,
                path,
                found,
                ..
            } => {
                assert_eq!(table, "zoom.meeting_action_items");
                assert_eq!(step, "explode");
                assert_eq!(path, "summary.nextSteps");
                assert_eq!(found, "a string");
            }
            other => panic!("expected RowDerivationFailed, got {other}"),
        }
    }

    #[test]
    fn pinned_fingerprints_match_the_captured_contracts() {
        // Pin <-> captured-contract lock through the SAME function
        // registration uses; mismatch output is also how pins are
        // (re)taken after an upstream upgrade.
        let contracts = [
            (
                "meetings",
                include_str!("fixtures/zoom/contracts/list_user_meeting_summaries.json"),
            ),
            (
                "meeting_notes",
                include_str!("fixtures/zoom/contracts/get_meeting_summary.json"),
            ),
            (
                "meeting_action_items",
                include_str!("fixtures/zoom/contracts/get_meeting_summary.json"),
            ),
        ];
        let mut mismatches = Vec::new();
        for (short, contract) in contracts {
            let schema: Value = serde_json::from_str(contract).expect("contract fixture parses");
            let actual = fingerprint_schema(Some(&schema));
            let t = table(short);
            if t.expected_fingerprint != Some(actual.as_str()) {
                mismatches.push(format!(
                    "{}: pinned {:?}, contract fixture hashes to {actual}",
                    t.id, t.expected_fingerprint
                ));
            }
        }
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }

    #[test]
    fn generated_inputs_are_accepted_by_the_captured_input_contracts() {
        // The fingerprint gate hashes OUTPUT schemas only; both actions are
        // `additionalProperties: false`, so an undeclared key is a 400 on
        // every scan. Both sides here are committed artifacts (gateway
        // built from open-connector#21 at ab02d6da): this locks the
        // declarations against the captured input halves and catches
        // drift on re-capture, as gmail's twin does.
        for (short, contract) in [
            (
                "meetings",
                include_str!("fixtures/zoom/contracts/inputs/list_user_meeting_summaries.json"),
            ),
            (
                "meeting_notes",
                include_str!("fixtures/zoom/contracts/inputs/get_meeting_summary.json"),
            ),
            (
                "meeting_action_items",
                include_str!("fixtures/zoom/contracts/inputs/get_meeting_summary.json"),
            ),
        ] {
            let schema: Value =
                serde_json::from_str(contract).expect("input contract fixture parses");
            let properties = &schema["properties"];
            let t = table(short);
            assert_eq!(
                schema["additionalProperties"],
                json!(false),
                "{short}: the action's input schema is strict"
            );
            let mut generated: Vec<&str> = t
                .required_resources
                .iter()
                .chain(t.optional_resources)
                .copied()
                .collect();
            generated.extend(t.fixed_inputs.iter().map(|(key, _)| *key));
            generated.extend(t.filters.iter().map(|f| f.input_field));
            match t.pagination {
                PaginationStrategy::Cursor {
                    cursor_param,
                    page_size_param,
                    ..
                } => {
                    generated.push(cursor_param);
                    generated.extend(page_size_param);
                }
                PaginationStrategy::PageNumber {
                    page_param,
                    per_page_param,
                    ..
                } => {
                    generated.push(page_param);
                    generated.push(per_page_param);
                }
                PaginationStrategy::Keyset {
                    cursor_param,
                    page_size_param,
                    ..
                } => {
                    generated.push(cursor_param);
                    generated.push(page_size_param);
                }
                PaginationStrategy::SinglePage { .. } => {}
            }
            for key in &generated {
                assert!(
                    !properties[*key].is_null(),
                    "{short}: `{key}` is not declared by the action's input schema"
                );
            }
            if let Some(required) = schema["required"].as_array() {
                for entry in required {
                    let entry = entry.as_str().expect("required entries are strings");
                    assert!(
                        generated.contains(&entry),
                        "{short}: the action requires `{entry}`, which this table never sends"
                    );
                }
            }
            if let PaginationStrategy::Cursor {
                page_size_param: Some(param),
                page_size,
                ..
            } = t.pagination
            {
                let declared = &properties[param];
                if let Some(minimum) = declared["minimum"].as_u64() {
                    assert!(u64::from(page_size) >= minimum, "{short}: below minimum");
                }
                if let Some(maximum) = declared["maximum"].as_u64() {
                    assert!(u64::from(page_size) <= maximum, "{short}: above maximum");
                }
            }
            for (key, value) in t.fixed_inputs {
                if let (FixedValue::Str(value), Some(variants)) =
                    (value, properties[*key]["enum"].as_array())
                {
                    assert!(
                        variants.iter().any(|v| v.as_str() == Some(*value)),
                        "{short}: fixed input `{key}: {value}` is outside the declared enum"
                    );
                }
            }
        }
    }

    #[test]
    fn columns_the_coverage_walker_cannot_resolve_are_pinned() {
        // `meetings`: every wire-mapped column resolves under the row
        // item's `properties`; the six derived columns cannot (no contract
        // declares a pack's own label), so they are the pin. The two
        // summary tables: `summary` is `anyOf: [object, null]` and the
        // walker reads `properties` only, so EVERY column is reported —
        // gmail's case 2, a tooling blind spot pinned so it stays visible;
        // the fingerprint hashes the whole schema, branches included.
        for (short, contract, expected) in [
            (
                "meetings",
                include_str!("fixtures/zoom/contracts/list_user_meeting_summaries.json"),
                &[
                    "provider",
                    "series_id",
                    "attendee_emails",
                    "attendee_ids",
                    "url",
                    "calendar_event_id",
                ] as &[&str],
            ),
            (
                "meeting_notes",
                include_str!("fixtures/zoom/contracts/get_meeting_summary.json"),
                &[
                    "provider",
                    "meeting_id",
                    "meeting_started_at",
                    "note_id",
                    "kind",
                    "producer",
                    "title",
                    "body",
                    "document_path",
                    "url",
                ],
            ),
            (
                "meeting_action_items",
                include_str!("fixtures/zoom/contracts/get_meeting_summary.json"),
                &[
                    "provider",
                    "meeting_id",
                    "note_id",
                    "item_id",
                    "text",
                    "owner_name",
                    "owner_email",
                    "owner_id",
                    "due",
                    "done",
                ],
            ),
        ] {
            let t = table(short);
            assert_eq!(
                fingerprint_uncovered_columns(contract, t.row_path, t.fields),
                expected,
                "fingerprint coverage changed for {short}"
            );
        }
    }

    #[test]
    fn the_three_tables_carry_the_contracts_column_names_in_order() {
        // The cloud's recipe, item tables and ontology recipe name these
        // columns; a rename here is a contract break there.
        let names =
            |short: &str| -> Vec<&str> { table(short).fields.iter().map(|f| f.name).collect() };
        assert_eq!(
            names("meetings"),
            [
                "provider",
                "meeting_id",
                "series_id",
                "title",
                "started_at",
                "ended_at",
                "organizer_email",
                "organizer_id",
                "attendee_emails",
                "attendee_ids",
                "url",
                "calendar_event_id",
            ]
        );
        assert_eq!(
            names("meeting_notes"),
            [
                "provider",
                "meeting_id",
                "meeting_started_at",
                "note_id",
                "kind",
                "producer",
                "title",
                "body",
                "document_path",
                "url",
            ]
        );
        assert_eq!(
            names("meeting_action_items"),
            [
                "provider",
                "meeting_id",
                "note_id",
                "item_id",
                "text",
                "owner_name",
                "owner_email",
                "owner_id",
                "due",
                "done",
            ]
        );
        for short in ["meetings", "meeting_notes", "meeting_action_items"] {
            let t = table(short);
            let non_null: Vec<&str> = t
                .fields
                .iter()
                .filter(|f| !f.nullable)
                .map(|f| f.name)
                .collect();
            let identity = match short {
                "meetings" => vec!["provider", "meeting_id"],
                "meeting_notes" => vec!["provider", "meeting_id", "note_id"],
                _ => vec!["provider", "meeting_id", "item_id"],
            };
            assert_eq!(non_null, identity, "{short}: only identity is non-null");
            assert!(
                t.error_path.is_none(),
                "{short}: executors consume in-band errors"
            );
        }
    }

    // ── Integration: the pack against a mock gateway, end to end. ───────

    fn zoom_config(token_env: &str, tables: &str, resource: &str) -> OpenConnectorConfig {
        let resource_line = if resource.is_empty() {
            String::new()
        } else {
            format!("resource: {resource}")
        };
        serde_yaml::from_str(&format!(
            r#"
runtime_token_env: {token_env}
bindings:
  - name: minutes
    source_pack: zoom
    {resource_line}
    tables: [{tables}]
"#
        ))
        .expect("config parses")
    }

    async fn setup_with_gateway(
        gateway: MockGateway,
        token_env: &'static str,
        tables: &str,
        resource: &str,
    ) -> (MockGateway, SessionContext) {
        let _token = EnvVarGuard::set(token_env, "test-token");
        let gateways = OpenConnectorGateways::default();
        let mut ctx = SessionContext::new();
        register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&zoom_config(token_env, tables, resource)),
            false,
            HierarchyLevel::Catalog,
            Some(&gateways),
        )
        .await
        .expect("gateway registration succeeds");
        register_open_connector_udtfs(&ctx, gateways).expect("UDTF registration succeeds");
        (gateway, ctx)
    }

    fn listed_meeting(uuid: &str, meeting_id: Value) -> Value {
        json!({
            "meetingUuid": uuid,
            "meetingId": meeting_id,
            "meetingHostId": "AbCdEfGhIjKlMnOpQrStUv",
            "meetingHostEmail": "host@example.com",
            "meetingTopic": "Weekly sync",
            "meetingStartTime": "2026-11-02T09:00:00Z",
            "meetingEndTime": "2026-11-02T09:45:00Z",
            "summaryStartTime": "2026-11-02T09:01:00Z",
            "summaryEndTime": "2026-11-02T09:44:00Z",
            "summaryCreatedTime": "2026-11-02T09:46:00Z",
            "summaryLastModifiedTime": "2026-11-02T10:00:00Z",
        })
    }

    const WINDOW: &str = r#"{ from: "2026-11-01T00:00:00Z", to: "2026-11-08T00:00:00Z" }"#;

    #[tokio::test]
    async fn meetings_cursor_scan_pages_with_its_own_declared_inputs() {
        // Two-page cursor scan pinning the listing's wire declaration: no
        // nextPageToken on page 1, the stub's token afterwards, pageSize 30
        // and the bound window on every request, explicit-null termination
        // (the executor's spelling), row identity across pages, exact key
        // sets — `userId` and `timeFilterField` never travel.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return zoom_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/zoom.list_user_meeting_summaries" {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                let page = match body["input"].get("nextPageToken").and_then(Value::as_str) {
                    None => json!({
                        "summaries": [listed_meeting("u-1", json!(1)), listed_meeting("u-2", json!("2"))],
                        "nextPageToken": "tok-2", "pageSize": 30,
                        "from": "2026-11-01T00:00:00Z", "to": "2026-11-08T00:00:00Z"}),
                    Some("tok-2") => json!({
                        "summaries": [listed_meeting("u-3", json!(3))],
                        "nextPageToken": null, "pageSize": 30,
                        "from": "2026-11-01T00:00:00Z", "to": "2026-11-08T00:00:00Z"}),
                    Some(other) => return MockResponse::new(400, format!("bad token {other}")),
                };
                return MockResponse::ok(&envelope_ok(&page.to_string()));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_ZOOM_MEETINGS", "meetings", WINDOW).await;

        let batches = collect(
            &ctx,
            "SELECT meeting_id, series_id, provider FROM saas.minutes.meetings ORDER BY meeting_id",
        )
        .await;
        assert_eq!(
            column_values(&batches, "meeting_id"),
            vec!["u-1", "u-2", "u-3"]
        );
        // An integer and a string meetingId both render as text (the null
        // case is the fixture test's; `column_values` refuses NULLs).
        assert_eq!(column_values(&batches, "series_id"), vec!["1", "2", "3"]);
        assert_eq!(
            column_values(&batches, "provider"),
            vec!["zoom", "zoom", "zoom"]
        );

        let inputs = execute_inputs(&gateway, "zoom.list_user_meeting_summaries");
        assert_eq!(inputs.len(), 2, "two cursor pages");
        assert_eq!(inputs[1]["nextPageToken"], "tok-2");
        for (page, (input, expected_keys)) in inputs
            .iter()
            .zip([
                vec!["from", "pageSize", "to"],
                vec!["from", "nextPageToken", "pageSize", "to"],
            ])
            .enumerate()
        {
            assert_eq!(input["pageSize"], 30, "page-size hint: {input}");
            assert_eq!(input["from"], "2026-11-01T00:00:00Z");
            assert_eq!(input["to"], "2026-11-08T00:00:00Z");
            assert_eq!(input_keys(input), expected_keys, "page {} keys", page + 1);
        }
    }

    #[tokio::test]
    async fn meetings_without_a_window_send_only_the_page_size_and_scan_empty_cleanly() {
        // A binding that omits from/to reads Zoom's default window with
        // exactly one input key; an empty listing is an empty result set
        // after one request, and the absent-token spelling terminates too.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return zoom_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/zoom.list_user_meeting_summaries" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"summaries": [], "pageSize": 30}).to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_ZOOM_NO_WINDOW", "meetings", "").await;
        let batches = collect(&ctx, "SELECT meeting_id FROM saas.minutes.meetings").await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        let inputs = execute_inputs(&gateway, "zoom.list_user_meeting_summaries");
        assert_eq!(inputs.len(), 1);
        assert_eq!(input_keys(&inputs[0]), vec!["pageSize"]);
    }

    #[tokio::test]
    async fn summary_tables_scan_one_summary_into_a_note_and_its_items() {
        // Both object-row tables bound to one meeting UUID: each issues
        // exactly one request carrying exactly `meetingUuid`, verbatim —
        // including a UUID that starts with `/`, whose path encoding is
        // the action's job, not the pack's.
        let summary = include_str!("fixtures/zoom/summary_edited.json");
        let gateway = MockGateway::start(move |req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return zoom_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/zoom.get_meeting_summary" {
                return MockResponse::ok(&envelope_ok(summary));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_ZOOM_SUMMARY",
            "meeting_notes, meeting_action_items",
            &format!(r#"{{ meetingUuid: "{SLASH_UUID}" }}"#),
        )
        .await;

        let notes = collect(
            &ctx,
            "SELECT note_id, kind, producer, body FROM saas.minutes.meeting_notes",
        )
        .await;
        assert_eq!(column_values(&notes, "note_id"), vec![SLASH_UUID]);
        assert_eq!(column_values(&notes, "kind"), vec!["summary"]);
        assert_eq!(column_values(&notes, "producer"), vec!["zoom_ai_companion"]);
        assert_eq!(
            column_values(&notes, "body"),
            vec!["## Summary\n\nThe design was reviewed."]
        );
        let items = collect(
            &ctx,
            "SELECT item_id, text, note_id FROM saas.minutes.meeting_action_items ORDER BY item_id",
        )
        .await;
        assert_eq!(
            column_values(&items, "item_id"),
            vec![format!("{SLASH_UUID}#0")]
        );
        assert_eq!(column_values(&items, "text"), vec!["Publish the design"]);
        assert_eq!(column_values(&items, "note_id"), vec![SLASH_UUID]);

        let inputs = execute_inputs(&gateway, "zoom.get_meeting_summary");
        assert_eq!(
            inputs.len(),
            2,
            "one request per table, never a second page"
        );
        for input in &inputs {
            assert_eq!(input_keys(input), vec!["meetingUuid"]);
            assert_eq!(input["meetingUuid"], SLASH_UUID);
        }
    }

    #[tokio::test]
    async fn an_unavailable_summary_scans_as_zero_rows_not_a_failure() {
        // The 403 2305 shape end to end: the action's successful envelope
        // with `summary: null` is an empty result on both tables, after
        // one request each.
        let unavailable = include_str!("fixtures/zoom/summary_unavailable.json");
        let gateway = MockGateway::start(move |req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return zoom_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/zoom.get_meeting_summary" {
                return MockResponse::ok(&envelope_ok(unavailable));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_ZOOM_UNAVAILABLE",
            "meeting_notes, meeting_action_items",
            &format!(r#"{{ meetingUuid: "{UUID}" }}"#),
        )
        .await;
        for sql in [
            "SELECT note_id FROM saas.minutes.meeting_notes",
            "SELECT item_id FROM saas.minutes.meeting_action_items",
        ] {
            let batches = collect(&ctx, sql).await;
            assert_eq!(
                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                0,
                "{sql}"
            );
        }
        assert_eq!(
            execute_inputs(&gateway, "zoom.get_meeting_summary").len(),
            2
        );
    }

    #[tokio::test]
    async fn binding_missing_meeting_uuid_fails_before_discovery() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let token_env = "SKARDI_TEST_OC_ZOOM_MISSING_RESOURCE";
        let _token = EnvVarGuard::set(token_env, "test-token");
        let mut ctx = SessionContext::new();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&zoom_config(token_env, "meeting_notes", "")),
            false,
            HierarchyLevel::Catalog,
            None,
        )
        .await
        .expect_err("a required resource is enforced")
        .downcast::<OpenConnectorError>()
        .expect("typed error");
        assert!(matches!(
            err,
            OpenConnectorError::MissingResourceInput { ref binding, ref key }
                if binding == "minutes" && key == "meetingUuid"
        ));
        assert!(
            gateway.requests().iter().all(|r| r.path == "/v1/health"),
            "resource enforcement precedes every discovery call"
        );
    }

    #[tokio::test]
    async fn limit_stops_cursor_pagination_early() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return zoom_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/zoom.list_user_meeting_summaries" {
                // Every page advertises another; only LIMIT can stop this.
                return MockResponse::ok(&envelope_ok(
                    &json!({"summaries": [listed_meeting("u-1", json!(1)), listed_meeting("u-2", json!(2))],
                             "nextPageToken": "again"})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_ZOOM_LIMIT", "meetings", "").await;
        let batches = collect(&ctx, "SELECT meeting_id FROM saas.minutes.meetings LIMIT 2").await;
        assert_eq!(column_values(&batches, "meeting_id").len(), 2);
        assert_eq!(
            execute_inputs(&gateway, "zoom.list_user_meeting_summaries").len(),
            1,
            "one page satisfied LIMIT"
        );
    }

    #[tokio::test]
    async fn a_repeated_cursor_fails_as_a_pagination_loop() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return zoom_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/zoom.list_user_meeting_summaries" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"summaries": [listed_meeting("u-1", json!(1))], "nextPageToken": "stuck"})
                        .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_ZOOM_LOOP", "meetings", "").await;
        let err = ctx
            .sql("SELECT meeting_id FROM saas.minutes.meetings")
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("a non-advancing cursor must fail the scan");
        let message = err.to_string();
        assert!(
            message.contains("pagination loop") && message.contains("stuck"),
            "loop identity is named: {message}"
        );
    }

    #[tokio::test]
    async fn an_expired_page_token_fails_the_scan_with_the_gateways_code() {
        // Zoom's token lives fifteen minutes; an expired one is Zoom's 400,
        // which the gateway forwards as `invalid_input`. The scan fails
        // naming the code and the action — never a truncated listing.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return zoom_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/zoom.list_user_meeting_summaries" {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                if body["input"].get("nextPageToken").is_some() {
                    return MockResponse::new(
                        400,
                        envelope_err("invalid_input", "The next page token is invalid or expired."),
                    );
                }
                return MockResponse::ok(&envelope_ok(
                    &json!({"summaries": [listed_meeting("u-1", json!(1))], "nextPageToken": "stale"})
                        .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_ZOOM_EXPIRED", "meetings", "").await;
        let err = ctx
            .sql("SELECT meeting_id FROM saas.minutes.meetings")
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("an expired token must fail the scan");
        let message = err.to_string();
        assert!(
            message.contains("invalid_input")
                && message.contains("zoom.list_user_meeting_summaries"),
            "the gateway's error code and the action are named: {message}"
        );
    }

    #[tokio::test]
    async fn other_provider_errors_surface_through_the_gateway_failure_envelope() {
        // error_path is None on purpose: any 403 other than Zoom's 2305 is
        // the gateway's authorization failure, whose code reaches the user.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return zoom_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/zoom.get_meeting_summary" {
                return MockResponse::new(
                    401,
                    envelope_err(
                        "authorization_failed",
                        "Invalid access token, does not contain scopes.",
                    ),
                );
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_ZOOM_SCOPE",
            "meeting_notes",
            &format!(r#"{{ meetingUuid: "{UUID}" }}"#),
        )
        .await;
        let err = ctx
            .sql("SELECT note_id FROM saas.minutes.meeting_notes")
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("a missing-scope failure must fail the scan");
        let message = err.to_string();
        assert!(
            message.contains("authorization_failed")
                && message.contains("zoom.get_meeting_summary"),
            "the gateway's error code and the action are named: {message}"
        );
    }

    #[tokio::test]
    async fn udtf_parity_for_meetings() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return zoom_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/zoom.list_user_meeting_summaries" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"summaries": [listed_meeting("u-1", json!(1)), listed_meeting("u-2", json!(2))],
                             "nextPageToken": null})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_ZOOM_UDTF", "meetings", "").await;
        let from_table = collect(
            &ctx,
            "SELECT meeting_id, series_id, provider FROM saas.minutes.meetings ORDER BY meeting_id",
        )
        .await;
        let from_udtf = collect(
            &ctx,
            "SELECT meeting_id, series_id, provider FROM open_connector_query('saas', 'zoom.meetings', '{}') ORDER BY meeting_id",
        )
        .await;
        assert_eq!(from_table[0].schema(), from_udtf[0].schema());
        assert_eq!(
            arrow::util::pretty::pretty_format_batches(&from_table)
                .expect("renders")
                .to_string(),
            arrow::util::pretty::pretty_format_batches(&from_udtf)
                .expect("renders")
                .to_string()
        );
    }

    #[tokio::test]
    async fn drifted_contract_fails_registration_not_the_scan() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return MockResponse::ok(&discovery_ok("{}", r#"{"type": "object"}"#, true, None));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let token_env = "SKARDI_TEST_OC_ZOOM_DRIFT";
        let _token = EnvVarGuard::set(token_env, "test-token");
        let mut ctx = SessionContext::new();
        let gateways = OpenConnectorGateways::default();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&zoom_config(token_env, "meetings", "")),
            false,
            HierarchyLevel::Catalog,
            Some(&gateways),
        )
        .await
        .expect_err("a drifted contract must fail registration")
        .downcast::<OpenConnectorError>()
        .expect("typed error");
        match err {
            OpenConnectorError::ActionContractMismatch { table, reason } => {
                assert_eq!(table, "zoom.meetings");
                assert!(
                    reason.contains("zoom.list_user_meeting_summaries"),
                    "the action is named: {reason}"
                );
            }
            other => panic!("expected ActionContractMismatch, got {other}"),
        }
    }
}
