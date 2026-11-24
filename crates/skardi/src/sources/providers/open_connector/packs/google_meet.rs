//! Google Meet source pack: the conferences a member organized in Google
//! Meet and the notes Gemini wrote for them ("Take notes for me"), over the
//! Open Connector `googlemeet` actions, landed in the meeting-minutes
//! contract shape — `meetings` and `meeting_notes` — that every meeting pack
//! writes (skardi-cloud `skardi-source-contract/src/meetings.rs`;
//! meeting-minutes design §3.2, §3.3's Google Meet column).
//!
//! **The tool is the author and this pack keeps its record.** Nothing here
//! calls a model or reads text: a column is filled from exactly one field
//! of the action's answer, a contract column Google has no field for is an
//! explicit NULL on every row, and a note is a row only because Google
//! answered a smart note for it (design §0.3, D14).
//!
//! **The wire contract is Open Connector's.** Both actions' output and input
//! contracts under `fixtures/google_meet/contracts/` were captured on
//! 2026-11-24 from a local gateway built from SkardiLabs/open-connector
//! main `02ba682a322e35636f1b927eb1abee1d79af6249`, which carries
//! open-connector#22 (the start-time window and `get_current_user`) and
//! open-connector#30 (`get_conference_artifacts`).
//! Every input set this pack generates reached the credential wall
//! (`authorization_failed`, "Connect googlemeet with OAuth or service
//! account credentials first.", `data.status` 401):
//!
//! - the window with and without `pageToken`;
//! - no window at all;
//! - the notes read's `{name}`.
//!
//! An undeclared key was refused as `additionalProperties`.
//!
//! `get_conference_artifacts` takes the `name` `get_conference_record`
//! takes and answers `{conferenceRecord, smartNotes}`:
//!
//! - the record, with `get_conference_record`'s schema;
//! - beside it, every smart-note session, oldest first, always an array.
//!
//! The gateway walks smart notes at `pageSize` 100 for at most ten pages,
//! and past that answers a provider error rather than a short list.
//!
//! The executors rebuild the list envelope — `conferenceRecords` always present
//! (`looseArray` answers Google's `{}` page as `[]`), `nextPageToken` null
//! on the last page — and pass Google's own objects through RAW: camelCase
//! keys, times as RFC 3339 strings with up to nine fractional digits.
//!
//! **UNPROBED: no real Google account has been read.** Every row-shape fact
//! below comes from Google's Meet REST v2 discovery document
//! (`https://meet.googleapis.com/$discovery/rest?version=v2`, revision
//! 20261117: `ConferenceRecord`, `SmartNote`, `DocsDestination`), Google's
//! Meet API guides (conferences, artifacts, overview, limits; read
//! 2026-11-24) and the executor source. The conference record and the
//! smart note are declared with their fields, so the fingerprint gate
//! protects every wire column of `meetings` and the meeting columns of
//! `meeting_notes`. The fixtures are SYNTHETIC. The numbered list at the
//! end is every assumption the pack leans on and the probe that settles it.
//!
//! Design decisions, per the integration design spec, the source-pack
//! admission gate and the meeting-minutes design (the assessment is
//! `meetings-meet-assessment.md`, approved 2026-11-24 in its smallest shape):
//!
//! - **Two contract tables, no listing helper.** The cloud's recipe, item
//!   tables and ontology recipe name pack tables, so `meetings` and
//!   `meeting_notes` carry the contract's names and columns. Unlike Feishu's
//!   search, Meet's list answers whole conference records, so `meetings` IS
//!   the listing and needs no per-meeting read.
//! - **The member's organized meetings only.** Google: the list method "only
//!   returns conferences where you're the meeting organizer" (conferences
//!   guide). A meeting the member only attended never appears; nothing here
//!   can widen that.
//! - **30 days at most.** A conference record "is deleted 30 days after the
//!   conference ends" (`ConferenceRecord.expireTime`), so the list holds no
//!   older meeting, whatever window is asked for.
//! - **`meetings` is a cursor listing** (`pageToken` in, `$.nextPageToken`
//!   out, null on the last page; `pageSize` 100, Google's maximum — "values
//!   above 100 are coerced to 100"; newest first). The window
//!   `startTime`/`endTime` is optional and RFC 3339, sent verbatim; the
//!   action turns it into `start_time>="…" AND start_time<="…"`, inclusive
//!   at both ends, and refuses a bound that is not RFC 3339 before any
//!   request. Optional rather than required (Feishu's search requires its
//!   window): without one the list is bounded by the 30 days anyway.
//! - **`meetings` columns**: `name` (`conferenceRecords/{id}`, one per
//!   occurrence of a call in a space) → `meeting_id`; `startTime` ("Always
//!   set") and `endTime` (unset while the conference runs) → the times,
//!   `timestamp_ms_utc` (chrono reads all nine fractional digits; the
//!   millisecond column truncates the rest). A conference record carries no
//!   title, no organizer, no attendee, no link and no calendar event; a Meet
//!   space is a reusable room, not a series (ruling 2026-11-08). So
//!   `series_id`, `title`, `organizer_email`, `organizer_id`,
//!   `attendee_emails`, `attendee_ids`, `url` and `calendar_event_id` are
//!   explicit-NULL constants. The organizer is the connecting account by
//!   the list rule above, but filling it from the account would be a lookup
//!   this pack invents (§0.3). Attendees are participants, a per-meeting
//!   read this version does not make (assessment Q1).
//! - **One note row per smart note, by `derive.explode`.** The notes read
//!   answers `{conferenceRecord, smartNotes}`: the record beside every
//!   smart note of it, all pages read. Without the record a note would
//!   carry its meeting only inside its own resource name, which this pack
//!   never parses. Each smart note is a row; a conference without notes
//!   (an empty array, or no `smartNotes` key) is zero rows, not an error,
//!   and its `meetings` row still stands. Transcripts are not read
//!   (assessment Q2): no `transcript` kind here.
//! - **Note columns**: `note.name`
//!   (`conferenceRecords/{c}/smartNotes/{s}`, "a 1:1 mapping to each unique
//!   smart notes session") → `note_id`; `note.docsDestination.document` (the
//!   Docs document id, which is also the Drive file id) → `document_path`;
//!   `note.docsDestination.exportUri` → `url`; `conferenceRecord.name` →
//!   `meeting_id`; `conferenceRecord.startTime` → `meeting_started_at`.
//!   `kind` is `notes` (Gemini's Doc holds a summary, next steps and
//!   details: the tool's notes, not a summary field); `producer` is `gemini`
//!   ("notes generated from Take Notes with Gemini"). `title` and `body` are
//!   NULL: the Doc is reached by `document_path`, never copied (design §3.2
//!   as revised 2026-11-08). Reading its text would need `documents.readonly`
//!   (sensitive) or `drive.readonly` (restricted) and a second read keyed by
//!   the Doc id.
//! - **A note Google has not written yet** has no `docsDestination`
//!   (UNPROBED, item 2): its row stands with NULL `document_path` and `url`,
//!   and a later read of the same meeting fills them. Accepted for this
//!   version (assessment Q7).
//! - **No action items.** Gemini's "Next steps" are a section of the notes
//!   Doc, prose; the API's `SmartNote` has no item field. Google Meet yields
//!   no `meeting_action_items` rows (design §3.3).
//! - **Nullability follows the contract**: `provider` and each table's
//!   identity non-null (`meeting_id`; `note_id` on notes), everything else
//!   nullable.
//! - **`error_path: None`**: Google's refusals arrive as gateway failure
//!   envelopes — a 404 as `invalid_input` with HTTP 404 and `data.status`
//!   404, a 403 as `authorization_failed`, a 400 as `invalid_input` —
//!   pinned by e2e tests.
//! - **Fingerprints are pinned.** The coverage walker reports the derived
//!   columns of both tables (no contract declares a pack's own label) and,
//!   on `meeting_notes`, every column read under the explode target `note`
//!   (the walker resolves wire paths, and `note` is the pack's key, not the
//!   wire's); the smart-note fields stay hashed under `smartNotes.items`, so
//!   their declared drift still fails registration.
//! - **Scopes**: `meetings.space.readonly` for both actions, a Google
//!   *sensitive* scope (Meet's authorization guide). No Drive, Docs or
//!   Calendar scope.
//!
//! **UNPROBED, with the probe that settles each:**
//!
//! 1. The list omits a meeting the member only attended (Google says so);
//!    whether the notes read of such a record by name answers or refuses.
//!    Probe: a meeting the member joined but did not organize.
//! 2. A smart note lacks `docsDestination` until its `state` is
//!    `FILE_GENERATED`, and how long after the meeting that takes. Probe:
//!    read a meeting right after it ends, then again later.
//! 3. The organizer's token lists smart notes that an internal
//!    non-organizer started. Probe: such a meeting.
//! 4. Two note-taking sessions in one meeting are two smart notes with two
//!    Docs, or one Doc. Probe: stop and restart notes in one meeting.
//! 5. A meeting without notes answers an empty `smartNotes`, not an error.
//!    Probe: a meeting with notes off.
//! 6. A running conference has no `endTime`, and the window lists it.
//!    Probe: list during a meeting.
//! 7. An expired or deleted record answers Google's 404 `NOT_FOUND`
//!    (gateway: `invalid_input`, HTTP 404), not 403. Probe: read a record
//!    older than 30 days by name.
//! 8. A stale `pageToken` answers 400 `INVALID_ARGUMENT`, and how long a
//!    token lives (Google documents neither). Probe: replay a token later.
//! 9. The window accepts whole seconds with `Z` and is inclusive at both
//!    ends. Probe: a meeting starting exactly on a bound.
//! 10. An instant meeting (meet.new) is organized by whoever opened it and
//!     listed for them. Probe: such a meeting.
//! 11. The Doc id in `docsDestination.document` is the Drive file id the
//!     Drive source lists. Probe: the same id through `google_drive.files`.
//! 12. Read quota: 600 a minute per user, 6000 per project (limits guide);
//!     a 429 arrives as the gateway's `rate_limited`. Probe: not needed
//!     before the cloud run.
//!
//! The fixtures are SYNTHETIC: shaped by Google's discovery document, with
//! invented record names (`conferenceRecords/synthetic-conference-0001`),
//! spaces and Doc ids (`1SyntheticNotesDoc000000000000001`). No real Google
//! data appears anywhere in this pack; meetings and their notes are
//! customer content and the live pass must redact before it replaces them.

use std::sync::OnceLock;

use crate::sources::providers::open_connector::error::OpenConnectorError;
use crate::sources::providers::open_connector::source_pack::SourcePack;

use super::loader;

static PACK: OnceLock<Result<SourcePack, String>> = OnceLock::new();

/// The Google Meet pack, parsed once from the embedded YAML asset.
///
/// # Examples
///
/// ```
/// use skardi::sources::providers::open_connector::packs::google_meet;
///
/// let pack = google_meet::pack().expect("the shipped asset is valid");
/// let names: Vec<&str> = pack.tables.iter().map(|t| t.id).collect();
/// assert_eq!(names, ["google_meet.meeting_notes", "google_meet.meetings"]);
/// ```
pub fn pack() -> Result<&'static SourcePack, OpenConnectorError> {
    loader::builtin("google_meet.yaml", include_str!("google_meet.yaml"), &PACK)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::hierarchy::HierarchyLevel;
    use crate::sources::providers::open_connector::action_registry::fingerprint_schema;
    use crate::sources::providers::open_connector::json_to_arrow::RowConverter;
    use crate::sources::providers::open_connector::pagination::PaginationStrategy;
    use crate::sources::providers::open_connector::row_path::RowPath;
    use crate::sources::providers::open_connector::source_pack::SourcePackTable;
    use crate::sources::providers::open_connector::testutil::{
        EnvVarGuard, MockGateway, MockResponse, collect, column_values, convert_first_page,
        discovery_ok, envelope_ok, execute_inputs, fingerprint_uncovered_columns, input_keys, utf8,
    };
    use crate::sources::providers::open_connector::{
        OpenConnectorConfig, OpenConnectorGateways, register_open_connector_tables,
        register_open_connector_udtfs,
    };
    use arrow::array::{Array, ListArray, TimestampMillisecondArray};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use rstest::rstest;
    use serde_json::{Value, json};
    use std::error::Error;
    use std::iter::successors;

    const PACK_NAME: &str = "google_meet";
    const PROVIDER: &str = "google_meet";
    const LIST: &str = "googlemeet.list_conference_records";
    const ARTIFACTS: &str = "googlemeet.get_conference_artifacts";

    const RECORD: &str = "conferenceRecords/synthetic-conference-0001";
    const RUNNING: &str = "conferenceRecords/synthetic-conference-0002";
    const NOTE_1: &str =
        "conferenceRecords/synthetic-conference-0001/smartNotes/synthetic-note-0001";
    const NOTE_2: &str =
        "conferenceRecords/synthetic-conference-0001/smartNotes/synthetic-note-0002";
    const DOC_1: &str = "1SyntheticNotesDoc000000000000001";
    const DOC_2: &str = "1SyntheticNotesDoc000000000000002";
    /// `2026-11-02T09:00:00.123456789Z`: nine fractional digits, truncated
    /// to the millisecond.
    const STARTED_MS: i64 = 1_793_610_000_123;
    /// `2026-11-02T09:45:00Z`.
    const ENDED_MS: i64 = 1_793_612_700_000;
    /// `2026-11-03T15:30:00.000000001Z`: one nanosecond, truncated away.
    const RUNNING_STARTED_MS: i64 = 1_793_719_800_000;

    const LIST_CONTRACT: &str =
        include_str!("fixtures/google_meet/contracts/list_conference_records.json");
    const ARTIFACTS_CONTRACT: &str =
        include_str!("fixtures/google_meet/contracts/get_conference_artifacts.json");

    /// Look up a table by short name; the asset is test-pinned to parse.
    fn table(short: &str) -> &'static SourcePackTable {
        pack()
            .expect("embedded asset is test-pinned to parse")
            .tables
            .iter()
            .find(|t| t.id.rsplit('.').next() == Some(short))
            .unwrap_or_else(|| panic!("table {short}"))
    }

    /// The action an execute POST names, if any.
    fn action_of(path: &str) -> Option<&str> {
        path.strip_prefix("/v1/actions/")
    }

    /// Discovery serving the pinned contracts, so every mock registration
    /// exercises the fingerprint gate's pass side.
    fn meet_discovery(path: &str) -> MockResponse {
        let output_schema = match action_of(path) {
            Some(LIST) => LIST_CONTRACT,
            Some(ARTIFACTS) => ARTIFACTS_CONTRACT,
            _ => r#"{"type": "object"}"#,
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

    fn fixture(text: &str) -> Value {
        serde_json::from_str(text).expect("fixture parses")
    }

    fn row_count(batches: &[RecordBatch]) -> usize {
        batches.iter().map(RecordBatch::num_rows).sum()
    }

    // ── Contract tests: SYNTHETIC fixtures shaped by Google's discovery
    // document (no live Google data exists for this pack — the module doc
    // says so). They pin the conversion contract per the admission gate:
    // null-bearing, nested, extra-field, empty, and schema-mismatch — plus
    // the derivation the contract shape needs. ─

    #[test]
    fn meetings_fixture_converts_with_the_contract_columns() {
        let batch = convert_first_page(
            table("meetings"),
            &fixture(include_str!("fixtures/google_meet/conference_records.json")),
        );
        assert_eq!(batch.num_rows(), 2);
        for i in 0..2 {
            assert_eq!(utf8(&batch, "provider").value(i), PROVIDER);
            // What Google has no field for is NULL on every row.
            for column in [
                "series_id",
                "title",
                "organizer_email",
                "organizer_id",
                "url",
                "calendar_event_id",
            ] {
                assert!(utf8(&batch, column).is_null(i), "{column} row {i}");
            }
            for column in ["attendee_emails", "attendee_ids"] {
                assert!(list(&batch, column).is_null(i), "{column} row {i}");
            }
        }
        // Newest first, as Google lists them. The running conference has no
        // end yet.
        assert_eq!(utf8(&batch, "meeting_id").value(0), RUNNING);
        assert_eq!(
            timestamps(&batch, "started_at").value(0),
            RUNNING_STARTED_MS
        );
        assert!(timestamps(&batch, "ended_at").is_null(0));
        // Nine fractional digits read, truncated to the millisecond; the
        // field a later API revision adds is ignored.
        assert_eq!(utf8(&batch, "meeting_id").value(1), RECORD);
        assert_eq!(timestamps(&batch, "started_at").value(1), STARTED_MS);
        assert_eq!(timestamps(&batch, "ended_at").value(1), ENDED_MS);
    }

    #[test]
    fn an_empty_page_converts_to_zero_meetings() {
        // Google answers `{}` for an empty page; the executor answers
        // `conferenceRecords: []`.
        let batch = convert_first_page(
            table("meetings"),
            &fixture(include_str!(
                "fixtures/google_meet/conference_records_empty.json"
            )),
        );
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(
            batch.schema().fields().len(),
            table("meetings").fields.len(),
            "an empty batch still carries the fixed schema"
        );
    }

    #[test]
    fn a_wire_row_cannot_shadow_the_derived_columns() {
        // A derived key wins over a wire key of the same name.
        let batch = convert_first_page(
            table("meetings"),
            &json!({
                "conferenceRecords": [{
                    "name": RECORD,
                    "startTime": "2026-11-02T09:00:00Z",
                    "provider": "spoofed",
                    "title": "a key Google never sends",
                }],
                "nextPageToken": null,
            }),
        );
        assert_eq!(utf8(&batch, "provider").value(0), PROVIDER);
        assert!(utf8(&batch, "title").is_null(0));
    }

    #[test]
    fn meeting_notes_fixture_converts_two_smart_notes_into_two_notes() {
        let batch = convert_first_page(
            table("meeting_notes"),
            &fixture(include_str!(
                "fixtures/google_meet/conference_artifacts.json"
            )),
        );
        assert_eq!(batch.num_rows(), 2);
        for i in 0..2 {
            assert_eq!(utf8(&batch, "provider").value(i), PROVIDER);
            assert_eq!(utf8(&batch, "meeting_id").value(i), RECORD);
            assert_eq!(
                timestamps(&batch, "meeting_started_at").value(i),
                STARTED_MS,
                "the conference's start, copied onto every note"
            );
            assert_eq!(utf8(&batch, "kind").value(i), "notes");
            assert_eq!(utf8(&batch, "producer").value(i), "gemini");
            // No title and no text: the Doc is reached by path.
            for column in ["title", "body"] {
                assert!(utf8(&batch, column).is_null(i), "{column}");
            }
        }
        // In the order Google answers them.
        assert_eq!(utf8(&batch, "note_id").value(0), NOTE_1);
        assert_eq!(utf8(&batch, "document_path").value(0), DOC_1);
        assert_eq!(
            utf8(&batch, "url").value(0),
            format!("https://docs.google.com/document/d/{DOC_1}/view")
        );
        assert_eq!(utf8(&batch, "note_id").value(1), NOTE_2);
        assert_eq!(utf8(&batch, "document_path").value(1), DOC_2);
        assert_eq!(
            utf8(&batch, "url").value(1),
            format!("https://docs.google.com/document/d/{DOC_2}/view")
        );
    }

    #[test]
    fn a_conference_without_smart_notes_has_no_notes() {
        // Notes off, or nobody took notes: an empty array.
        let none = fixture(include_str!(
            "fixtures/google_meet/conference_artifacts_none.json"
        ));
        let batch = convert_first_page(table("meeting_notes"), &none);
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(
            batch.schema().fields().len(),
            table("meeting_notes").fields.len(),
            "an empty batch still carries the fixed schema"
        );
        // No `smartNotes` key at all: zero rows too, never a failure.
        let mut absent = none;
        absent
            .as_object_mut()
            .expect("answer object")
            .remove("smartNotes");
        assert_eq!(
            convert_first_page(table("meeting_notes"), &absent).num_rows(),
            0
        );
    }

    #[test]
    fn a_smart_note_google_has_not_written_yet_has_no_document() {
        // `STARTED`, no `docsDestination`: the row stands with NULL path and
        // link (UNPROBED item 2); a later read fills them.
        let batch = convert_first_page(
            table("meeting_notes"),
            &fixture(include_str!(
                "fixtures/google_meet/conference_artifacts_pending.json"
            )),
        );
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(utf8(&batch, "meeting_id").value(0), RUNNING);
        assert_eq!(
            utf8(&batch, "note_id").value(0),
            "conferenceRecords/synthetic-conference-0002/smartNotes/synthetic-note-0003"
        );
        assert_eq!(
            timestamps(&batch, "meeting_started_at").value(0),
            RUNNING_STARTED_MS
        );
        assert!(utf8(&batch, "document_path").is_null(0));
        assert!(utf8(&batch, "url").is_null(0));
    }

    #[test]
    fn name_mismatch_fixture_fails_with_the_targeted_error() {
        // Admission-gate schema-mismatch fixture (synthetic by nature): a
        // number where Utf8 is declared fails with the row-scoped identity
        // and never the offending value.
        let page = fixture(include_str!(
            "fixtures/google_meet/conference_record_type_mismatch.json"
        ));
        let t = table("meetings");
        let rows = RowPath::parse(t.row_path)
            .expect("row path")
            .rows(&page, 1)
            .expect("row array");
        let rows = t
            .derive
            .expect("declared")
            .apply(t.id, rows, 1)
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
                assert_eq!(column, "meeting_id");
                assert_eq!(path, "$.name");
                assert_eq!(page, 1);
                assert_eq!(row, 0);
                assert_eq!(found, "a number");
            }
            other => panic!("expected ConversionFailed, got {other}"),
        }
    }

    #[test]
    fn smart_notes_that_are_not_an_array_fail_the_derivation_by_kind() {
        // Upstream drift — an object where the array was — fails loudly
        // with the table, step and kind, never as zero notes.
        let mut page = fixture(include_str!(
            "fixtures/google_meet/conference_artifacts.json"
        ));
        page["smartNotes"] = json!({"name": NOTE_1});
        let t = table("meeting_notes");
        let err = t
            .derive
            .expect("declared")
            .apply(t.id, std::slice::from_ref(&page), 1)
            .expect_err("a non-array smartNotes must fail");
        match err {
            OpenConnectorError::RowDerivationFailed {
                table,
                step,
                path,
                found,
                ..
            } => {
                assert_eq!(table, format!("{PACK_NAME}.meeting_notes"));
                assert_eq!(step, "explode");
                assert_eq!(path, "smartNotes");
                assert_eq!(found, "an object");
            }
            other => panic!("expected RowDerivationFailed, got {other}"),
        }
    }

    /// A note must never land under no meeting or under no id of its own:
    /// both are non-null columns, so a missing one fails the scan.
    #[rstest]
    #[case::without_the_conference_record_name("/conferenceRecord", "name", "meeting_id")]
    #[case::without_the_smart_note_name("/smartNotes/0", "name", "note_id")]
    fn a_notes_answer_without_an_identity_fails_loudly(
        #[case] parent: &str,
        #[case] key: &str,
        #[case] column: &str,
    ) {
        let mut page = fixture(include_str!(
            "fixtures/google_meet/conference_artifacts.json"
        ));
        page.pointer_mut(parent)
            .and_then(Value::as_object_mut)
            .expect("parent object")
            .remove(key);
        let t = table("meeting_notes");
        let rows = t
            .derive
            .expect("declared")
            .apply(t.id, std::slice::from_ref(&page), 1)
            .expect("derives");
        let err = RowConverter::new(t.fields)
            .expect("converter")
            .convert(&rows, 1)
            .expect_err("a note without its identity must fail");
        match err {
            OpenConnectorError::ConversionFailed {
                column: failed,
                found,
                ..
            } => {
                assert_eq!(failed, column);
                assert_eq!(found, "missing key");
            }
            other => panic!("expected ConversionFailed, got {other}"),
        }
    }

    #[test]
    fn pinned_fingerprints_match_the_contracts() {
        // Pin <-> contract lock through the SAME function registration
        // uses; mismatch output is also how pins are (re)taken after an
        // upstream upgrade.
        let mut mismatches = Vec::new();
        for (short, contract) in [
            ("meetings", LIST_CONTRACT),
            ("meeting_notes", ARTIFACTS_CONTRACT),
        ] {
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
    fn generated_inputs_are_accepted_by_the_input_contracts() {
        // The fingerprint gate hashes OUTPUT schemas only; both actions are
        // `additionalProperties: false`, so an undeclared key is a 400 on
        // every scan.
        let list_inputs =
            include_str!("fixtures/google_meet/contracts/inputs/list_conference_records.json");
        let artifacts_inputs =
            include_str!("fixtures/google_meet/contracts/inputs/get_conference_artifacts.json");
        for (short, contract) in [
            ("meetings", list_inputs),
            ("meeting_notes", artifacts_inputs),
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
            assert!(t.fixed_inputs.is_empty(), "{short}: no fixed inputs");
            let mut generated: Vec<&str> = t
                .required_resources
                .iter()
                .chain(t.optional_resources)
                .copied()
                .collect();
            generated.extend(t.filters.iter().map(|f| f.input_field));
            if let PaginationStrategy::Cursor {
                cursor_param,
                page_size_param,
                ..
            } = t.pagination
            {
                generated.push(cursor_param);
                generated.extend(page_size_param);
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
        }
    }

    #[test]
    fn each_table_reads_its_own_action_with_its_own_inputs() {
        let meetings = table("meetings");
        assert_eq!(meetings.action_id, LIST);
        assert!(meetings.required_resources.is_empty());
        assert_eq!(meetings.optional_resources, ["startTime", "endTime"]);
        match meetings.pagination {
            PaginationStrategy::Cursor {
                cursor_param,
                page_size_param,
                page_size,
                ..
            } => {
                assert_eq!(cursor_param, "pageToken");
                assert_eq!(page_size_param, Some("pageSize"));
                assert_eq!(page_size, 100, "Google's maximum");
            }
            other => panic!("meetings must page by cursor, got {other:?}"),
        }
        let notes = table("meeting_notes");
        assert_eq!(notes.action_id, ARTIFACTS);
        assert_eq!(notes.required_resources, ["name"]);
        assert!(notes.optional_resources.is_empty());
    }

    #[test]
    fn columns_the_coverage_walker_cannot_resolve_are_pinned() {
        // `meetings`: every wire-mapped column resolves under the record's
        // declared `properties`; the derived constants cannot (no contract
        // declares a pack's own label), so they are the pin.
        // `meeting_notes`: the two meeting columns resolve under
        // `conferenceRecord`; the derived constants do not, and neither do
        // the columns read under the explode target `note` — the walker
        // resolves wire paths and `note` is the pack's key. The smart-note
        // fields are still declared (under `smartNotes.items`), so their
        // declared drift fails registration.
        for (short, contract, expected) in [
            (
                "meetings",
                LIST_CONTRACT,
                &[
                    "provider",
                    "series_id",
                    "title",
                    "organizer_email",
                    "organizer_id",
                    "attendee_emails",
                    "attendee_ids",
                    "url",
                    "calendar_event_id",
                ] as &[&str],
            ),
            (
                "meeting_notes",
                ARTIFACTS_CONTRACT,
                &[
                    "provider",
                    "note_id",
                    "kind",
                    "producer",
                    "title",
                    "body",
                    "document_path",
                    "url",
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
    fn the_contract_tables_carry_the_contracts_column_names_in_order() {
        // The cloud's recipe, item tables and ontology recipe name these
        // columns; a rename here is a contract break there. The lists are
        // the contract's (design §3.2), the same for every meeting pack.
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
        for short in ["meetings", "meeting_notes"] {
            let t = table(short);
            let non_null: Vec<&str> = t
                .fields
                .iter()
                .filter(|f| !f.nullable)
                .map(|f| f.name)
                .collect();
            let identity = match short {
                "meetings" => vec!["provider", "meeting_id"],
                _ => vec!["provider", "meeting_id", "note_id"],
            };
            assert_eq!(non_null, identity, "{short}: only identity is non-null");
            assert!(
                t.error_path.is_none(),
                "{short}: refusals arrive as gateway envelopes"
            );
        }
        // No action-item table: Gemini's next steps are prose inside the
        // notes Doc (module doc).
        assert!(
            pack()
                .expect("parses")
                .tables
                .iter()
                .all(|t| !t.id.ends_with(".meeting_action_items"))
        );
    }

    // ── Integration: the pack against a mock gateway, end to end. ───────

    fn meet_config(token_env: &str, tables: &str, resource: &str) -> OpenConnectorConfig {
        let resource_line = if resource.is_empty() {
            String::new()
        } else {
            format!("resource: {resource}")
        };
        serde_yaml::from_str(&format!(
            r#"
runtime_token_env: {token_env}
bindings:
  - name: meet
    source_pack: {PACK_NAME}
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
            Some(&meet_config(token_env, tables, resource)),
            false,
            HierarchyLevel::Catalog,
            Some(&gateways),
        )
        .await
        .expect("gateway registration succeeds");
        register_open_connector_udtfs(&ctx, gateways).expect("UDTF registration succeeds");
        (gateway, ctx)
    }

    fn record(name: &str) -> Value {
        json!({"name": name, "space": "spaces/syntheticSpace0001", "startTime": "2026-11-02T09:00:00Z"})
    }

    fn records_page(names: &[&str], next: Option<&str>) -> MockResponse {
        let page = json!({
            "conferenceRecords": names.iter().map(|name| record(name)).collect::<Vec<_>>(),
            "nextPageToken": next,
        });
        MockResponse::ok(&envelope_ok(&page.to_string()))
    }

    const WINDOW: &str =
        r#"{ startTime: "2026-11-01T00:00:00Z", endTime: "2026-11-08T00:00:00Z" }"#;
    const WINDOW_JSON: &str =
        r#"{"startTime": "2026-11-01T00:00:00Z", "endTime": "2026-11-08T00:00:00Z"}"#;

    #[tokio::test]
    async fn meetings_cursor_scan_pages_with_its_own_declared_inputs() {
        // Two-page cursor scan pinning the listing's wire declaration: no
        // pageToken on page 1, the stub's token afterwards, pageSize 100 and
        // the bound window on every request, a null token ending it, exact
        // key sets.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return meet_discovery(&req.path);
            }
            if req.method == "POST" && action_of(&req.path) == Some(LIST) {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                return match body["input"].get("pageToken").and_then(Value::as_str) {
                    None => records_page(
                        &["conferenceRecords/m-3", "conferenceRecords/m-2"],
                        Some("tok-2"),
                    ),
                    Some("tok-2") => records_page(&["conferenceRecords/m-1"], None),
                    Some(other) => MockResponse::new(400, format!("bad token {other}")),
                };
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_GOOGLE_MEET_LIST",
            "meetings",
            WINDOW,
        )
        .await;

        let batches = collect(
            &ctx,
            "SELECT meeting_id, provider FROM saas.meet.meetings ORDER BY meeting_id",
        )
        .await;
        assert_eq!(
            column_values(&batches, "meeting_id"),
            vec![
                "conferenceRecords/m-1",
                "conferenceRecords/m-2",
                "conferenceRecords/m-3"
            ]
        );
        assert_eq!(
            column_values(&batches, "provider"),
            vec![PROVIDER, PROVIDER, PROVIDER]
        );

        let inputs = execute_inputs(&gateway, LIST);
        assert_eq!(inputs.len(), 2, "two cursor pages");
        assert_eq!(inputs[1]["pageToken"], "tok-2");
        for (page, (input, expected_keys)) in inputs
            .iter()
            .zip([
                vec!["endTime", "pageSize", "startTime"],
                vec!["endTime", "pageSize", "pageToken", "startTime"],
            ])
            .enumerate()
        {
            assert_eq!(input["pageSize"], 100, "page-size hint: {input}");
            assert_eq!(input["startTime"], "2026-11-01T00:00:00Z");
            assert_eq!(input["endTime"], "2026-11-08T00:00:00Z");
            assert_eq!(input_keys(input), expected_keys, "page {} keys", page + 1);
        }
    }

    #[tokio::test]
    async fn a_listing_without_a_window_sends_no_window_keys() {
        // The window is optional: unbound, the request carries the page
        // size alone and the action lists every record Google still holds.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return meet_discovery(&req.path);
            }
            if req.method == "POST" && action_of(&req.path) == Some(LIST) {
                return records_page(&[RECORD], None);
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_GOOGLE_MEET_NO_WINDOW",
            "meetings",
            "",
        )
        .await;
        let batches = collect(&ctx, "SELECT meeting_id FROM saas.meet.meetings").await;
        assert_eq!(column_values(&batches, "meeting_id"), vec![RECORD]);
        let inputs = execute_inputs(&gateway, LIST);
        assert_eq!(inputs.len(), 1);
        assert_eq!(input_keys(&inputs[0]), vec!["pageSize"]);
    }

    #[tokio::test]
    async fn an_empty_window_scans_empty_after_one_request() {
        let empty = include_str!("fixtures/google_meet/conference_records_empty.json");
        let gateway = MockGateway::start(move |req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return meet_discovery(&req.path);
            }
            if req.method == "POST" && action_of(&req.path) == Some(LIST) {
                return MockResponse::ok(&envelope_ok(empty));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_GOOGLE_MEET_EMPTY",
            "meetings",
            WINDOW,
        )
        .await;
        let batches = collect(&ctx, "SELECT meeting_id FROM saas.meet.meetings").await;
        assert_eq!(row_count(&batches), 0);
        assert_eq!(execute_inputs(&gateway, LIST).len(), 1);
    }

    #[tokio::test]
    async fn the_notes_read_sends_exactly_the_record_name() {
        // One request for one conference, keyed by its record name; the
        // window keys of the same binding never travel to the notes read.
        let artifacts = include_str!("fixtures/google_meet/conference_artifacts.json");
        let gateway = MockGateway::start(move |req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return meet_discovery(&req.path);
            }
            if req.method == "POST" && action_of(&req.path) == Some(ARTIFACTS) {
                return MockResponse::ok(&envelope_ok(artifacts));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let resource = format!(
            r#"{{ name: "{RECORD}", startTime: "2026-11-01T00:00:00Z", endTime: "2026-11-08T00:00:00Z" }}"#
        );
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_GOOGLE_MEET_NOTES",
            // `meetings` is bound (never scanned) so the window keys are
            // declared by some table: registration refuses a resource key no
            // bound table consumes.
            "meeting_notes, meetings",
            &resource,
        )
        .await;

        let notes = collect(
            &ctx,
            "SELECT note_id, kind, producer, document_path, meeting_id \
             FROM saas.meet.meeting_notes ORDER BY note_id",
        )
        .await;
        assert_eq!(column_values(&notes, "note_id"), vec![NOTE_1, NOTE_2]);
        assert_eq!(column_values(&notes, "document_path"), vec![DOC_1, DOC_2]);
        assert_eq!(column_values(&notes, "kind"), vec!["notes", "notes"]);
        assert_eq!(column_values(&notes, "producer"), vec!["gemini", "gemini"]);
        assert_eq!(column_values(&notes, "meeting_id"), vec![RECORD, RECORD]);

        let inputs = execute_inputs(&gateway, ARTIFACTS);
        assert_eq!(inputs.len(), 1, "one request, never a second page");
        assert_eq!(input_keys(&inputs[0]), vec!["name"]);
        assert_eq!(inputs[0]["name"], RECORD);
        assert!(
            execute_inputs(&gateway, LIST).is_empty(),
            "the notes read lists nothing"
        );
    }

    #[tokio::test]
    async fn a_conference_without_smart_notes_scans_as_zero_notes() {
        let none = include_str!("fixtures/google_meet/conference_artifacts_none.json");
        let gateway = MockGateway::start(move |req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return meet_discovery(&req.path);
            }
            if req.method == "POST" && action_of(&req.path) == Some(ARTIFACTS) {
                return MockResponse::ok(&envelope_ok(none));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_GOOGLE_MEET_NO_NOTES",
            "meeting_notes",
            r#"{ name: "conferenceRecords/synthetic-conference-0003" }"#,
        )
        .await;
        let batches = collect(&ctx, "SELECT note_id FROM saas.meet.meeting_notes").await;
        assert_eq!(row_count(&batches), 0);
        assert_eq!(execute_inputs(&gateway, ARTIFACTS).len(), 1);
    }

    #[tokio::test]
    async fn a_binding_without_the_record_name_fails_before_discovery() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let token_env = "SKARDI_TEST_OC_GOOGLE_MEET_NO_NAME";
        let _token = EnvVarGuard::set(token_env, "test-token");
        let mut ctx = SessionContext::new();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&meet_config(token_env, "meeting_notes, meetings", WINDOW)),
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
                if binding == "meet" && key == "name"
        ));
        assert!(
            gateway.requests().iter().all(|r| r.path == "/v1/health"),
            "resource enforcement precedes every discovery call"
        );
    }

    #[tokio::test]
    async fn a_repeated_cursor_fails_as_a_pagination_loop() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return meet_discovery(&req.path);
            }
            if req.method == "POST" && action_of(&req.path) == Some(LIST) {
                return records_page(&[RECORD], Some("stuck"));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_GOOGLE_MEET_LOOP",
            "meetings",
            WINDOW,
        )
        .await;
        let err = ctx
            .sql("SELECT meeting_id FROM saas.meet.meetings")
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

    /// error_path is None on purpose: Google's refusals arrive as gateway
    /// failure envelopes and the scan fails with the gateway's code and the
    /// action named, never as a truncated or empty result. The shapes are
    /// the gateway's mapping of Google's statuses, read from its source
    /// (`googledrive/runtime-request.ts`, `provider-runtime.ts`,
    /// `runtime-api.ts`); Google's own answers are UNPROBED (items 7, 8).
    #[rstest]
    #[case::expired_conference_record(
        "SKARDI_TEST_OC_GOOGLE_MEET_EXPIRED_RECORD",
        "meeting_notes",
        r#"{ name: "conferenceRecords/synthetic-conference-0009" }"#,
        ARTIFACTS,
        404,
        "invalid_input",
        "Requested entity was not found."
    )]
    #[case::stale_page_token(
        "SKARDI_TEST_OC_GOOGLE_MEET_STALE_TOKEN",
        "meetings",
        WINDOW,
        LIST,
        400,
        "invalid_input",
        "Invalid page token."
    )]
    #[case::scope_not_granted(
        "SKARDI_TEST_OC_GOOGLE_MEET_FORBIDDEN",
        "meeting_notes",
        r#"{ name: "conferenceRecords/synthetic-conference-0001" }"#,
        ARTIFACTS,
        403,
        "authorization_failed",
        "Request had insufficient authentication scopes."
    )]
    #[tokio::test]
    async fn google_refusals_surface_through_the_gateway_failure_envelope(
        #[case] token_env: &'static str,
        #[case] short: &'static str,
        #[case] resource: &str,
        #[case] action: &'static str,
        #[case] status: u16,
        #[case] code: &'static str,
        #[case] message: &'static str,
    ) {
        // The gateway's full failure shape as it forwards a provider
        // refusal: Google's status under `data.status`, Google's error body
        // under `data.details`, the action echoed under `meta`.
        let body = json!({
            "success": false,
            "message": message,
            "errorCode": code,
            "data": {
                "status": status,
                "details": {"error": {"code": status, "message": message}}
            },
            "meta": {"actionId": action}
        })
        .to_string();
        let gateway = MockGateway::start(move |req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return meet_discovery(&req.path);
            }
            if req.method == "POST" && action_of(&req.path) == Some(action) {
                let input: Value = serde_json::from_str(&req.body).unwrap_or_default();
                // The listing's first page succeeds and its continuation is
                // refused; the notes read is refused outright.
                if action == LIST && input["input"].get("pageToken").is_none() {
                    return records_page(&[RECORD], Some("stale"));
                }
                return MockResponse::new(status, body.clone());
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) = setup_with_gateway(gateway, token_env, short, resource).await;
        let err = ctx
            .sql(&format!("SELECT meeting_id FROM saas.meet.{short}"))
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("a refusal must fail the scan");
        let rendered = err.to_string();
        assert!(
            rendered.contains(code) && rendered.contains(action),
            "the gateway's error code and the action are named: {rendered}"
        );
        // The typed failure survives DataFusion's wrapping: walk the chain
        // from the error itself. `(*cause)`, not `cause`: on the
        // `&&dyn Error` the closure receives, `source` resolves to the
        // `Error` impl for `&T` and borrows the closure's argument rather
        // than the error behind it.
        let typed = successors(Some(&err as &(dyn Error + 'static)), |cause| {
            (*cause).source()
        })
        .find_map(|cause| cause.downcast_ref::<OpenConnectorError>())
        .unwrap_or_else(|| panic!("an OpenConnectorError in the chain: {rendered}"));
        match typed {
            OpenConnectorError::ActionExecutionFailed {
                action_id,
                status: got_status,
                error_code,
                ..
            } => {
                assert_eq!(action_id, action);
                assert_eq!(*got_status, Some(status));
                assert_eq!(error_code.as_deref(), Some(code));
            }
            other => panic!("expected ActionExecutionFailed, got {other}"),
        }
    }

    #[tokio::test]
    async fn udtf_parity_for_meetings() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return meet_discovery(&req.path);
            }
            if req.method == "POST" && action_of(&req.path) == Some(LIST) {
                return records_page(&["conferenceRecords/m-2", "conferenceRecords/m-1"], None);
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_GOOGLE_MEET_UDTF",
            "meetings",
            WINDOW,
        )
        .await;
        let from_table = collect(
            &ctx,
            "SELECT meeting_id, started_at FROM saas.meet.meetings ORDER BY meeting_id",
        )
        .await;
        let from_udtf = collect(
            &ctx,
            &format!(
                "SELECT meeting_id, started_at FROM open_connector_query('saas', \
                 '{PACK_NAME}.meetings', '{WINDOW_JSON}') ORDER BY meeting_id"
            ),
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
        let token_env = "SKARDI_TEST_OC_GOOGLE_MEET_DRIFT";
        let _token = EnvVarGuard::set(token_env, "test-token");
        let mut ctx = SessionContext::new();
        let gateways = OpenConnectorGateways::default();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&meet_config(token_env, "meetings", WINDOW)),
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
                assert_eq!(table, format!("{PACK_NAME}.meetings"));
                assert!(reason.contains(LIST), "the action is named: {reason}");
            }
            other => panic!("expected ActionContractMismatch, got {other}"),
        }
    }
}
