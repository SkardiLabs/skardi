//! Feishu meetings source pack: Feishu VC meetings and the documents Feishu
//! wrote for them, over the Open Connector `feishu.*` minutes-module VC
//! actions, landed in the meeting-minutes contract shape — `meetings` and
//! `meeting_notes` — that every meeting pack writes (skardi-cloud
//! `skardi-source-contract/src/meetings.rs`; meeting-minutes design §3.2,
//! §3.3's Feishu column).
//!
//! **The tool is the author and this pack keeps its record.** Nothing here
//! calls a model or reads text: a column is filled from exactly one field
//! of the action's answer, a contract column Feishu has no field for is an
//! explicit NULL on every row, and a note is a row only because Feishu
//! answered a document token for it (design §0.3, D14).
//!
//! **Its own pack, not tables of `feishu`.** The cloud connects Feishu
//! meetings as its own provider row `feishu_meetings` with its own consent
//! (design D18), so the tables are their own pack on the same `feishu`
//! Open Connector provider and the customer's own Feishu app.
//!
//! **The wire contract is Open Connector's.** Reconciled against a local
//! gateway built from SkardiLabs/open-connector main ea4e040e
//! (open-connector#24 merged: `get_vc_meeting` gained `queryMode`) on
//! 2026-11-18. The minutes executors rebuild the page envelope —
//! `{items, hasMore, pageToken}`, `pageToken` null on the last page — and
//! wrap a meeting as `{meeting: ...}`, while passing Feishu's own objects
//! through RAW: snake_case keys, times as Unix-second DIGIT STRINGS.
//! Inputs are camelCase strict (`additionalProperties: false`; a wrong key
//! such as `query_mode` was checked to be a hard 400), and every input set
//! this pack generates — the search window with and without `pageToken`,
//! the meeting read at mode 0 with participants and at mode 1 — reached
//! the credential wall on that gateway (`authorization_failed`, "Connect
//! feishu with OAuth first.", HTTP 403 with `data.status` 401). The
//! contracts under `fixtures/feishu_meetings/contracts/` were captured
//! there.
//!
//! **UNPROBED: no real Feishu tenant has been read.** Every row-shape fact
//! below comes from Feishu's API reference (`GET /vc/v1/meetings/:id` and
//! `POST /vc/v1/meetings/search`, read 2026-11-18) and the executor source.
//! Both action outputs are declared LOOSE (`meeting` and each search item
//! are objects with empty `properties`), so the fingerprint gate protects
//! no column; real rows are the only column truth, and the fixtures are
//! SYNTHETIC. The numbered list at the end is every assumption the pack
//! leans on and the probe that settles it.
//!
//! Design decisions, per the integration design spec, the source-pack
//! admission gate and the meeting-minutes design:
//!
//! - **Two contract tables and one listing.** The cloud's recipe, item
//!   tables and ontology recipe name pack tables, so `meetings` and
//!   `meeting_notes` carry the contract's names and columns.
//!   `meeting_search` is not a contract table: it is the listing, the ids
//!   of the member's meetings in a window, so the listing's contract is
//!   pinned (fingerprint, input lock) and a skardi-server user can
//!   enumerate meetings to read.
//! - **The listing gives ids and nothing else.** A search item is
//!   `{id, display_info, meta_data{app_link, avatar, description}}`;
//!   topic, time and organizer exist only inside card text with highlight
//!   tags, which is prose and never parsed (§0.3). So `meeting_search` has
//!   one column, and the `meetings` row comes from a per-meeting read. The
//!   cloud's `meetings` head therefore reads each listed meeting, unlike
//!   Zoom's, whose listing carries the meeting.
//! - **`meeting_search` is a cursor listing** (`pageToken` in, top-level
//!   `$.pageToken` out, `pageSize` 30 — Feishu's documented cap; the action
//!   declares 50), with `$.hasMore` as the end flag beside the null token.
//!   The window `startTime`/`endTime` is required: the action refuses a
//!   search with neither a query nor a filter. Both are RFC 3339 with an
//!   offset, sent verbatim as `meeting_filter.start_time` (open-connector#24;
//!   Feishu's reference shows `2026-03-21T16:15:30+08:00`).
//! - **One action, two modes.** `feishu.get_vc_meeting` answers a meeting's
//!   information at `queryMode` 0 ("只查询会议信息") and only its documents at
//!   1 ("只查询会议产物（纪要、逐字稿）"). `meetings` pins mode 0 with
//!   `includeParticipants: true`; `meeting_notes` pins mode 1. Chosen over
//!   `feishu.get_vc_meeting_note`, which makes two Feishu calls, needs
//!   `vc:note:read`, and fails outright (`invalid_input`) for a meeting
//!   that has no note, where mode 1 answers no token and the table yields
//!   zero rows. Two reads per meeting, as Zoom's two summary reads.
//! - **`meetings` columns**: `id` → `meeting_id`, `topic` → `title`,
//!   `start_time`/`end_time` → the times (`timestamp_s_string_utc`),
//!   `host_user.id` (an open_id under the default `user_id_type`) →
//!   `organizer_id`, each `participants[].id` → `attendee_ids` (who joined,
//!   as Feishu returns them; no name, no email), `url` → `url`. Feishu
//!   documents no series, no organizer email, no attendee email and no
//!   calendar link on a VC meeting: `series_id`, `organizer_email`,
//!   `attendee_emails`, `calendar_event_id` are explicit-NULL constants.
//! - **One note row per document token, by `derive.unpivot`.** Mode 1
//!   answers `related_artifacts.note_doc_token` (the 智能纪要) and
//!   `verbatim_doc_token` (the 逐字稿) as sibling keys. Each is a note of its
//!   own `kind` — `notes`, `transcript` — through the loader's `unpivot`
//!   step, added for this pack. Per Feishu's reference a token is ABSENT
//!   when the app lacks its field scope and `""` when permitted but not
//!   generated yet: neither is a row, so a meeting without documents has
//!   zero notes and its `meetings` row still stands.
//! - **`note_id` and `document_path` are the document token.** The token is
//!   the artifact's own id — present on every note row by construction,
//!   independent of any other field — and the document's address, passed
//!   verbatim; resolving it to a document is the consumer's job. `body` is
//!   NULL: the document is reached by `document_path`, never copied
//!   (design §3.2 as revised 2026-11-08). Reading the text would need
//!   `docx:document:readonly` — every document the member can read — and a
//!   second read keyed by the token. `title` and `url` are NULL: the
//!   answer carries neither. `producer` is `feishu` on both kinds.
//! - **`meeting_id` and `meeting_started_at` on a note come from the mode-1
//!   answer itself** (`meeting.id`, `meeting.start_time`). That mode 1
//!   returns them is UNPROBED (item 1). If it does not, the failure is
//!   loud: a missing id fails the scan on the non-null column, and a note
//!   without its meeting's start is refused by the cloud's listing.
//! - **No action items.** The meeting carries no minute token and Feishu
//!   documents no meeting → minute link; `feishu.get_minutes_todos` needs a
//!   minute token, reads an undocumented endpoint, and answers rows with no
//!   meeting id, so the contract's non-null `meeting_id` could not be
//!   filled. Feishu yields no `meeting_action_items` rows, as Meet.
//! - **Feishu answers only the member's own meetings**, 90 days back
//!   ("只能获取归属于自己的会议，支持查询最近90天内的会议"); a listed meeting the
//!   member only attended is the gateway's refusal on the read, which the
//!   pack does not absorb (see item 9).
//! - **Nullability follows the contract**: `provider` and each table's
//!   identity non-null (`meeting_id`; `note_id` on notes), everything else
//!   nullable.
//! - **`error_path: None`**: the Feishu client throws on a non-zero code,
//!   so provider failures arrive as gateway failure envelopes, pinned by
//!   e2e tests.
//! - **Fingerprints are pinned** from the gateway capture. Both outputs are
//!   loose, so the coverage walker reports every column of every table
//!   (case 1 of `fingerprint_uncovered_columns`: passthrough the schema
//!   does not declare) — pinned so the gap stays visible.
//! - **Scopes**: `vc:meeting.search:read` (search),
//!   `vc:meeting.meetingevent:read` (the meeting read), and the field
//!   scopes `vc:meeting.artifact.note:read` and
//!   `vc:meeting.artifact.verbatim:read`, without which the tokens are
//!   absent and `meeting_notes` is silently empty.
//! - **Lark** is meant to be a near copy: `lark_meetings` changes the pack
//!   name, the action prefix and the `provider` constant (the `lark.*`
//!   twins of both actions have identical schemas on the same gateway). The
//!   tests here name the provider through the constants at the top of the
//!   test module for that reason.
//!
//! **UNPROBED, with the probe that settles each:**
//!
//! 1. Mode 1 returns `meeting.id` and `meeting.start_time`. Probe:
//!    `get_vc_meeting {meetingId, queryMode: 1}` on an own meeting with
//!    notes; print the key set.
//! 2. Mode 0 returns no `note_id`/`related_artifacts` (Open Connector's own
//!    `get_vc_meeting_note` reads `note_id` at mode 0). Probe: the same
//!    meeting at mode 0.
//! 3. A token is absent without its field scope and `""` before Feishu
//!    generates the document. Probe: a grant without
//!    `vc:meeting.artifact.verbatim:read`; a meeting read right after it
//!    ends.
//! 4. A document token belongs to one meeting (no notes document shared
//!    across a recurring series), so `note_id` is unique. Probe: two
//!    occurrences of one recurring meeting.
//! 5. A search item is `{id, display_info, meta_data}` and its `id` is what
//!    the meeting read accepts. Probe: first search page, then a read of
//!    `items[0].id`.
//! 6. Search accepts the window as `…Z` and as `…+08:00` alike. Probe: one
//!    window both ways, compare ids.
//! 7. A search page holds at most 30 (the reference also says 200 in its
//!    error table, the action declares 50) and a search returns at most
//!    150 results (`121022` past that). Probe: `pageSize` 30 and 31; a
//!    window with more than 150 meetings.
//! 8. Search errors are HTTP 500 per the reference, so an expired
//!    `page_token` (`121020`) arrives as `provider_error`, not 400. Probe:
//!    replay a stale token.
//! 9. A listed meeting the member did not own answers `403 121005`; which
//!    gateway envelope. Probe: read a meeting the member joined but did not
//!    host.
//! 10. A meeting older than 90 days answers `404 121004`. Probe.
//! 11. `start_time`/`end_time` are second digit strings; what `end_time` is
//!     for a meeting still running (`"0"` reads as 1970). Probe: mode 0
//!     during a meeting.
//! 12. What `participants[].id` holds for an external guest, a room or a
//!     phone participant (`user_type`). Probe: such a meeting.
//! 13. `host_user.id` is the host's open_id. Probe: compare with
//!     `feishu.get_current_user`.
//! 14. The two field scopes are grantable on a user-OAuth custom app
//!     without tenant-admin approval. Probe: at install.
//! 15. Lark's `lark.*` twins answer the same shapes. Probe: a Lark tenant.
//!
//! The fixtures are SYNTHETIC: shaped by Feishu's reference and the
//! executor source, with invented ids (`7000000000000000001`), open_ids
//! (`ou_000…001`), tokens (`docx_synthetic_notes_0001`) and topics
//! ("Weekly sync"). No real Feishu data appears anywhere in this pack;
//! meetings and their documents are customer content and the live pass
//! must redact before it replaces them.

use std::sync::OnceLock;

use crate::sources::providers::open_connector::error::OpenConnectorError;
use crate::sources::providers::open_connector::source_pack::SourcePack;

use super::loader;

static PACK: OnceLock<Result<SourcePack, String>> = OnceLock::new();

/// The Feishu meetings pack, parsed once from the embedded YAML asset.
///
/// # Examples
///
/// ```
/// use skardi::sources::providers::open_connector::packs::feishu_meetings;
///
/// let pack = feishu_meetings::pack().expect("the shipped asset is valid");
/// let names: Vec<&str> = pack.tables.iter().map(|t| t.id).collect();
/// assert_eq!(
///     names,
///     [
///         "feishu_meetings.meeting_notes",
///         "feishu_meetings.meeting_search",
///         "feishu_meetings.meetings",
///     ]
/// );
/// ```
pub fn pack() -> Result<&'static SourcePack, OpenConnectorError> {
    loader::builtin(
        "feishu_meetings.yaml",
        include_str!("feishu_meetings.yaml"),
        &PACK,
    )
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
        EnvVarGuard, MockGateway, MockResponse, collect, column_values, convert_first_page,
        discovery_ok, envelope_err, envelope_ok, execute_inputs, fingerprint_uncovered_columns,
        input_keys, utf8,
    };
    use crate::sources::providers::open_connector::{
        OpenConnectorConfig, OpenConnectorGateways, register_open_connector_tables,
        register_open_connector_udtfs,
    };
    use arrow::array::{Array, ListArray, StringArray, TimestampMillisecondArray};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use rstest::rstest;
    use serde_json::{Value, json};

    // The provider-specific literals, in one place: `lark_meetings` is this
    // module with these four changed (the fixtures are shared).
    const PACK_NAME: &str = "feishu_meetings";
    const PROVIDER: &str = "feishu";
    const SEARCH: &str = "feishu.search_vc_meetings";
    const GET: &str = "feishu.get_vc_meeting";

    const MEETING: &str = "7000000000000000001";
    const HOST: &str = "ou_00000000000000000000000000000001";
    const NOTES_TOKEN: &str = "docx_synthetic_notes_0001";
    const VERBATIM_TOKEN: &str = "docx_synthetic_verbatim_0001";
    /// `"1793610000"`: 2026-11-02T09:00:00Z.
    const STARTED_MS: i64 = 1_793_610_000_000;

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

    /// Discovery serving the captured contracts, so every mock
    /// registration exercises the fingerprint gate's pass side.
    fn feishu_discovery(path: &str) -> MockResponse {
        let output_schema = match action_of(path) {
            Some(SEARCH) => {
                include_str!("fixtures/feishu_meetings/contracts/search_vc_meetings.json")
            }
            Some(GET) => include_str!("fixtures/feishu_meetings/contracts/get_vc_meeting.json"),
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

    /// The strings of one list cell.
    fn list_values(batch: &RecordBatch, name: &str, row: usize) -> Vec<String> {
        let cell = list(batch, name).value(row);
        let strings = cell
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("column {name} is not a list of strings"));
        (0..strings.len())
            .map(|i| strings.value(i).to_string())
            .collect()
    }

    fn fixture(text: &str) -> Value {
        serde_json::from_str(text).expect("fixture parses")
    }

    // ── Contract tests: SYNTHETIC fixtures shaped by Feishu's reference and
    // the executor source (no live Feishu data exists for this pack — the
    // module doc says so). They pin the conversion contract per the
    // admission gate: null-bearing, nested, extra-field, empty, and
    // schema-mismatch — plus the derivation the contract shape needs. ─

    #[test]
    fn meeting_search_fixture_converts_to_meeting_ids_only() {
        // The card text beside each id is prose: no column reads it.
        let batch = convert_first_page(
            table("meeting_search"),
            &fixture(include_str!("fixtures/feishu_meetings/search.json")),
        );
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(utf8(&batch, "meeting_id").value(0), MEETING);
        assert_eq!(utf8(&batch, "meeting_id").value(1), "7000000000000000002");
    }

    #[test]
    fn meetings_fixture_converts_with_the_contract_columns() {
        let batch = convert_first_page(
            table("meetings"),
            &fixture(include_str!("fixtures/feishu_meetings/meeting.json")),
        );
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(utf8(&batch, "provider").value(0), PROVIDER);
        assert_eq!(utf8(&batch, "meeting_id").value(0), MEETING);
        assert_eq!(utf8(&batch, "title").value(0), "Weekly sync");
        // Unix-second digit strings, read as milliseconds.
        assert_eq!(timestamps(&batch, "started_at").value(0), STARTED_MS);
        assert_eq!(
            timestamps(&batch, "ended_at").value(0),
            STARTED_MS + 45 * 60 * 1000
        );
        assert_eq!(utf8(&batch, "organizer_id").value(0), HOST);
        assert_eq!(
            list_values(&batch, "attendee_ids", 0),
            vec![
                HOST.to_string(),
                "ou_00000000000000000000000000000002".to_string(),
                "ou_00000000000000000000000000000003".to_string(),
            ],
            "every participant who joined, the external one included"
        );
        assert_eq!(
            utf8(&batch, "url").value(0),
            "https://vc.example.com/j/100000001"
        );
        // What Feishu has no field for is NULL.
        for column in ["series_id", "organizer_email", "calendar_event_id"] {
            assert!(utf8(&batch, column).is_null(0), "{column}");
        }
        assert!(list(&batch, "attendee_emails").is_null(0));
    }

    #[test]
    fn a_sparse_meeting_converts_with_nulls() {
        // No topic (null), no end, no host, no url, no participants key:
        // a row with NULLs, not a failed scan.
        let batch = convert_first_page(
            table("meetings"),
            &fixture(include_str!("fixtures/feishu_meetings/meeting_sparse.json")),
        );
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(utf8(&batch, "meeting_id").value(0), "7000000000000000002");
        assert!(utf8(&batch, "title").is_null(0));
        assert!(!timestamps(&batch, "started_at").is_null(0));
        assert!(timestamps(&batch, "ended_at").is_null(0));
        assert!(utf8(&batch, "organizer_id").is_null(0));
        assert!(utf8(&batch, "url").is_null(0));
        assert!(list(&batch, "attendee_ids").is_null(0));
    }

    #[test]
    fn a_wire_row_cannot_shadow_the_derived_columns() {
        // A derived key wins over a wire key of the same name, at the row
        // root where the constants are written.
        let batch = convert_first_page(
            table("meetings"),
            &json!({
                "meeting": {"id": "m-1", "start_time": "1793610000"},
                "provider": "spoofed",
                "series_id": "wrong",
            }),
        );
        assert_eq!(utf8(&batch, "provider").value(0), PROVIDER);
        assert!(utf8(&batch, "series_id").is_null(0));
    }

    #[test]
    fn meeting_notes_fixture_converts_two_documents_into_two_notes() {
        let batch = convert_first_page(
            table("meeting_notes"),
            &fixture(include_str!(
                "fixtures/feishu_meetings/meeting_artifacts.json"
            )),
        );
        assert_eq!(batch.num_rows(), 2);
        for i in 0..2 {
            assert_eq!(utf8(&batch, "provider").value(i), PROVIDER);
            assert_eq!(utf8(&batch, "meeting_id").value(i), MEETING);
            assert_eq!(utf8(&batch, "producer").value(i), PROVIDER);
            assert_eq!(
                timestamps(&batch, "meeting_started_at").value(i),
                STARTED_MS,
                "the meeting's start, copied onto every note"
            );
            // No text, no title, no link: the document is reached by path.
            for column in ["title", "body", "url"] {
                assert!(utf8(&batch, column).is_null(i), "{column}");
            }
        }
        // Notes first, then the transcript: the order `from` declares.
        assert_eq!(utf8(&batch, "kind").value(0), "notes");
        assert_eq!(utf8(&batch, "note_id").value(0), NOTES_TOKEN);
        assert_eq!(utf8(&batch, "document_path").value(0), NOTES_TOKEN);
        assert_eq!(utf8(&batch, "kind").value(1), "transcript");
        assert_eq!(utf8(&batch, "note_id").value(1), VERBATIM_TOKEN);
        assert_eq!(utf8(&batch, "document_path").value(1), VERBATIM_TOKEN);
    }

    #[test]
    fn a_meeting_without_documents_has_no_notes() {
        // Permitted but not generated yet: both tokens `""`.
        let pending = fixture(include_str!(
            "fixtures/feishu_meetings/meeting_artifacts_pending.json"
        ));
        let batch = convert_first_page(table("meeting_notes"), &pending);
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(
            batch.schema().fields().len(),
            table("meeting_notes").fields.len(),
            "an empty batch still carries the fixed schema"
        );
        // No field scope: the key is absent. Still zero rows, no error.
        let mut unscoped = pending.clone();
        unscoped["meeting"]
            .as_object_mut()
            .expect("meeting object")
            .remove("related_artifacts");
        assert_eq!(
            convert_first_page(table("meeting_notes"), &unscoped).num_rows(),
            0
        );
        // One scope held (notes only): one row, of that kind.
        let mut notes_only = pending;
        notes_only["meeting"]["related_artifacts"] = json!({"note_doc_token": NOTES_TOKEN});
        let batch = convert_first_page(table("meeting_notes"), &notes_only);
        assert_eq!(
            column_values(std::slice::from_ref(&batch), "kind"),
            vec!["notes"]
        );
    }

    #[test]
    fn title_mismatch_fixture_fails_with_the_targeted_error() {
        // Admission-gate schema-mismatch fixture (synthetic by nature): a
        // number where Utf8 is declared fails with the row-scoped identity
        // and never the offending value.
        let page = fixture(include_str!(
            "fixtures/feishu_meetings/meeting_type_mismatch.json"
        ));
        let t = table("meetings");
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
                assert_eq!(column, "title");
                assert_eq!(path, "$.meeting.topic");
                assert_eq!(page, 1);
                assert_eq!(row, 0);
                assert_eq!(found, "a number");
            }
            other => panic!("expected ConversionFailed, got {other}"),
        }
    }

    #[test]
    fn a_document_token_that_is_not_a_scalar_fails_the_derivation_by_kind() {
        // Upstream drift on a token — an object where the string was —
        // fails loudly with the table, step and kind, never as zero notes.
        let mut page = fixture(include_str!(
            "fixtures/feishu_meetings/meeting_artifacts.json"
        ));
        page["meeting"]["related_artifacts"]["verbatim_doc_token"] = json!({"token": "x"});
        let t = table("meeting_notes");
        let err = t
            .derive
            .expect("declared")
            .apply(t.id, std::slice::from_ref(&page), 1)
            .expect_err("a non-scalar token must fail");
        match err {
            OpenConnectorError::RowDerivationFailed {
                table,
                step,
                path,
                found,
                ..
            } => {
                assert_eq!(table, format!("{PACK_NAME}.meeting_notes"));
                assert_eq!(step, "unpivot");
                assert_eq!(path, "meeting.related_artifacts.verbatim_doc_token");
                assert_eq!(found, "an object");
            }
            other => panic!("expected RowDerivationFailed, got {other}"),
        }
    }

    #[test]
    fn a_documents_answer_without_the_meeting_id_fails_loudly() {
        // UNPROBED item 1: if mode 1 omits `meeting.id`, every note fails
        // on the non-null column rather than landing under no meeting.
        let mut page = fixture(include_str!(
            "fixtures/feishu_meetings/meeting_artifacts.json"
        ));
        page["meeting"]
            .as_object_mut()
            .expect("meeting object")
            .remove("id");
        let t = table("meeting_notes");
        let rows = t
            .derive
            .expect("declared")
            .apply(t.id, std::slice::from_ref(&page), 1)
            .expect("derives");
        let err = RowConverter::new(t.fields)
            .expect("converter")
            .convert(&rows, 1)
            .expect_err("a note without its meeting id must fail");
        match err {
            OpenConnectorError::ConversionFailed { column, found, .. } => {
                assert_eq!(column, "meeting_id");
                assert_eq!(found, "missing key");
            }
            other => panic!("expected ConversionFailed, got {other}"),
        }
    }

    #[test]
    fn pinned_fingerprints_match_the_captured_contracts() {
        // Pin <-> captured-contract lock through the SAME function
        // registration uses; mismatch output is also how pins are
        // (re)taken after an upstream upgrade.
        let search = include_str!("fixtures/feishu_meetings/contracts/search_vc_meetings.json");
        let get = include_str!("fixtures/feishu_meetings/contracts/get_vc_meeting.json");
        let mut mismatches = Vec::new();
        for (short, contract) in [
            ("meeting_search", search),
            ("meetings", get),
            ("meeting_notes", get),
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
    fn generated_inputs_are_accepted_by_the_captured_input_contracts() {
        // The fingerprint gate hashes OUTPUT schemas only; both actions are
        // `additionalProperties: false`, so an undeclared key is a 400 on
        // every scan. Both sides here are committed artifacts (gateway
        // built from open-connector main ea4e040e).
        let search =
            include_str!("fixtures/feishu_meetings/contracts/inputs/search_vc_meetings.json");
        let get = include_str!("fixtures/feishu_meetings/contracts/inputs/get_vc_meeting.json");
        for (short, contract) in [
            ("meeting_search", search),
            ("meetings", get),
            ("meeting_notes", get),
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
            // Each pinned integer lies inside the declared bounds, and each
            // pinned value has the declared JSON type.
            for (key, value) in t.fixed_inputs {
                let declared = &properties[*key];
                match value {
                    FixedValue::Int(n) => {
                        assert_eq!(declared["type"], "integer", "{short}: `{key}`");
                        if let Some(minimum) = declared["minimum"].as_i64() {
                            assert!(*n >= minimum, "{short}: `{key}` below minimum");
                        }
                        if let Some(maximum) = declared["maximum"].as_i64() {
                            assert!(*n <= maximum, "{short}: `{key}` above maximum");
                        }
                    }
                    FixedValue::Bool(_) => {
                        assert_eq!(declared["type"], "boolean", "{short}: `{key}`");
                    }
                    other => panic!("{short}: unexpected fixed input `{key}`: {other:?}"),
                }
            }
        }
    }

    #[test]
    fn the_two_meeting_reads_pin_their_query_modes() {
        // Mode 0 is the meeting's information, mode 1 only its documents;
        // swapping them would leave each table reading fields its answer
        // never carries.
        let fixed = |short: &str| -> Vec<(&str, Value)> {
            let mut pairs: Vec<(&str, Value)> = table(short)
                .fixed_inputs
                .iter()
                .map(|(key, value)| (*key, value.to_json()))
                .collect();
            pairs.sort_by_key(|(key, _)| *key);
            pairs
        };
        assert_eq!(
            fixed("meetings"),
            vec![
                ("includeParticipants", json!(true)),
                ("queryMode", json!(0))
            ]
        );
        assert_eq!(fixed("meeting_notes"), vec![("queryMode", json!(1))]);
        assert!(table("meeting_search").fixed_inputs.is_empty());
        for short in ["meetings", "meeting_notes"] {
            assert_eq!(table(short).action_id, GET, "{short}");
            assert_eq!(table(short).required_resources, ["meetingId"], "{short}");
        }
        assert_eq!(table("meeting_search").action_id, SEARCH);
        assert_eq!(
            table("meeting_search").required_resources,
            ["startTime", "endTime"]
        );
    }

    #[test]
    fn columns_the_coverage_walker_cannot_resolve_are_pinned() {
        // Both outputs are loose objects (empty `properties`), so no column
        // resolves under them: case 1 of the walker, a real gap only real
        // rows can close. Pinned so it stays visible.
        let search = include_str!("fixtures/feishu_meetings/contracts/search_vc_meetings.json");
        let get = include_str!("fixtures/feishu_meetings/contracts/get_vc_meeting.json");
        for (short, contract) in [
            ("meeting_search", search),
            ("meetings", get),
            ("meeting_notes", get),
        ] {
            let t = table(short);
            let every_column: Vec<&str> = t.fields.iter().map(|f| f.name).collect();
            assert_eq!(
                fingerprint_uncovered_columns(contract, t.row_path, t.fields),
                every_column,
                "fingerprint coverage changed for {short}"
            );
        }
    }

    #[test]
    fn the_contract_tables_carry_the_contracts_column_names_in_order() {
        // The cloud's recipe, item tables and ontology recipe name these
        // columns; a rename here is a contract break there. They are
        // Zoom's, column for column.
        let names =
            |short: &str| -> Vec<&str> { table(short).fields.iter().map(|f| f.name).collect() };
        let zoom = |short: &str| -> Vec<&str> {
            super::super::zoom::pack()
                .expect("zoom pack parses")
                .tables
                .iter()
                .find(|t| t.id.rsplit('.').next() == Some(short))
                .unwrap_or_else(|| panic!("zoom table {short}"))
                .fields
                .iter()
                .map(|f| f.name)
                .collect()
        };
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
            assert_eq!(names(short), zoom(short), "{short}: the same shape as Zoom");
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
                "{short}: the executors consume in-band errors"
            );
        }
        // No action-item table: Feishu returns no items this pack can tie
        // to a meeting (module doc).
        assert!(
            pack()
                .expect("parses")
                .tables
                .iter()
                .all(|t| !t.id.ends_with(".meeting_action_items"))
        );
    }

    // ── Integration: the pack against a mock gateway, end to end. ───────

    fn feishu_config(token_env: &str, tables: &str, resource: &str) -> OpenConnectorConfig {
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
        let config = feishu_config(token_env, tables, resource);
        setup_with_config(gateway, token_env, &config).await
    }

    async fn setup_with_config(
        gateway: MockGateway,
        token_env: &'static str,
        config: &OpenConnectorConfig,
    ) -> (MockGateway, SessionContext) {
        let _token = EnvVarGuard::set(token_env, "test-token");
        let gateways = OpenConnectorGateways::default();
        let mut ctx = SessionContext::new();
        register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(config),
            false,
            HierarchyLevel::Catalog,
            Some(&gateways),
        )
        .await
        .expect("gateway registration succeeds");
        register_open_connector_udtfs(&ctx, gateways).expect("UDTF registration succeeds");
        (gateway, ctx)
    }

    fn search_item(id: &str) -> Value {
        json!({"id": id, "display_info": "Weekly sync", "meta_data": {"description": "card text"}})
    }

    const WINDOW: &str =
        r#"{ startTime: "2026-11-01T00:00:00Z", endTime: "2026-11-08T00:00:00Z" }"#;
    const WINDOW_JSON: &str =
        r#"{"startTime": "2026-11-01T00:00:00Z", "endTime": "2026-11-08T00:00:00Z"}"#;

    /// One meeting answer per mode, keyed on the request's `queryMode`.
    fn meeting_answer(body: &str) -> MockResponse {
        let body: Value = serde_json::from_str(body).unwrap_or_default();
        let answer = match body["input"]["queryMode"].as_i64() {
            Some(0) => include_str!("fixtures/feishu_meetings/meeting.json"),
            Some(1) => include_str!("fixtures/feishu_meetings/meeting_artifacts.json"),
            _ => return MockResponse::new(400, "queryMode must be pinned"),
        };
        MockResponse::ok(&envelope_ok(answer))
    }

    #[tokio::test]
    async fn meeting_search_cursor_scan_pages_with_its_own_declared_inputs() {
        // Two-page cursor scan pinning the listing's wire declaration: no
        // pageToken on page 1, the stub's token afterwards, pageSize 30 and
        // the bound window on every request, `hasMore: false` with a null
        // token ending it, exact key sets.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return feishu_discovery(&req.path);
            }
            if req.method == "POST" && action_of(&req.path) == Some(SEARCH) {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                let page = match body["input"].get("pageToken").and_then(Value::as_str) {
                    None => json!({
                        "items": [search_item("m-1"), search_item("m-2")],
                        "hasMore": true, "pageToken": "tok-2"}),
                    Some("tok-2") => json!({
                        "items": [search_item("m-3")],
                        "hasMore": false, "pageToken": null}),
                    Some(other) => return MockResponse::new(400, format!("bad token {other}")),
                };
                return MockResponse::ok(&envelope_ok(&page.to_string()));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_FEISHU_MEETINGS_SEARCH",
            "meeting_search",
            WINDOW,
        )
        .await;

        let batches = collect(
            &ctx,
            "SELECT meeting_id FROM saas.minutes.meeting_search ORDER BY meeting_id",
        )
        .await;
        assert_eq!(
            column_values(&batches, "meeting_id"),
            vec!["m-1", "m-2", "m-3"]
        );

        let inputs = execute_inputs(&gateway, SEARCH);
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
            assert_eq!(input["pageSize"], 30, "page-size hint: {input}");
            assert_eq!(input["startTime"], "2026-11-01T00:00:00Z");
            assert_eq!(input["endTime"], "2026-11-08T00:00:00Z");
            assert_eq!(input_keys(input), expected_keys, "page {} keys", page + 1);
        }
    }

    #[tokio::test]
    async fn an_empty_window_scans_empty_after_one_request() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return feishu_discovery(&req.path);
            }
            if req.method == "POST" && action_of(&req.path) == Some(SEARCH) {
                return MockResponse::ok(&envelope_ok(
                    &json!({"items": [], "hasMore": false, "pageToken": null}).to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_FEISHU_MEETINGS_EMPTY",
            "meeting_search",
            WINDOW,
        )
        .await;
        let batches = collect(&ctx, "SELECT meeting_id FROM saas.minutes.meeting_search").await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        assert_eq!(execute_inputs(&gateway, SEARCH).len(), 1);
    }

    #[tokio::test]
    async fn the_meeting_reads_send_exactly_their_mode_and_the_meeting_id() {
        // Both object-row tables bound to one meeting: each issues exactly
        // one request, `meetings` at mode 0 with participants, the notes at
        // mode 1 without; the window keys of the same binding never travel.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return feishu_discovery(&req.path);
            }
            if req.method == "POST" && action_of(&req.path) == Some(GET) {
                return meeting_answer(&req.body);
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let resource = format!(
            r#"{{ meetingId: "{MEETING}", startTime: "2026-11-01T00:00:00Z", endTime: "2026-11-08T00:00:00Z" }}"#
        );
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_FEISHU_MEETINGS_READS",
            "meetings, meeting_notes",
            &resource,
        )
        .await;

        let meetings = collect(
            &ctx,
            "SELECT meeting_id, title, organizer_id, provider FROM saas.minutes.meetings",
        )
        .await;
        assert_eq!(column_values(&meetings, "meeting_id"), vec![MEETING]);
        assert_eq!(column_values(&meetings, "title"), vec!["Weekly sync"]);
        assert_eq!(column_values(&meetings, "organizer_id"), vec![HOST]);
        assert_eq!(column_values(&meetings, "provider"), vec![PROVIDER]);

        let notes = collect(
            &ctx,
            "SELECT note_id, kind, document_path, meeting_id FROM saas.minutes.meeting_notes \
             ORDER BY kind",
        )
        .await;
        assert_eq!(
            column_values(&notes, "note_id"),
            vec![NOTES_TOKEN, VERBATIM_TOKEN]
        );
        assert_eq!(column_values(&notes, "kind"), vec!["notes", "transcript"]);
        assert_eq!(
            column_values(&notes, "document_path"),
            vec![NOTES_TOKEN, VERBATIM_TOKEN]
        );
        assert_eq!(column_values(&notes, "meeting_id"), vec![MEETING, MEETING]);

        let inputs = execute_inputs(&gateway, GET);
        assert_eq!(
            inputs.len(),
            2,
            "one request per table, never a second page"
        );
        let mut by_mode: Vec<(i64, Vec<&str>)> = inputs
            .iter()
            .map(|input| {
                assert_eq!(input["meetingId"], MEETING);
                (
                    input["queryMode"].as_i64().expect("queryMode is pinned"),
                    input_keys(input),
                )
            })
            .collect();
        by_mode.sort_by_key(|(mode, _)| *mode);
        assert_eq!(
            by_mode,
            vec![
                (0, vec!["includeParticipants", "meetingId", "queryMode"]),
                (1, vec!["meetingId", "queryMode"]),
            ]
        );
        assert_eq!(
            inputs
                .iter()
                .filter(|i| i["includeParticipants"] == true)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn a_meeting_without_documents_scans_as_zero_notes() {
        let pending = include_str!("fixtures/feishu_meetings/meeting_artifacts_pending.json");
        let gateway = MockGateway::start(move |req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return feishu_discovery(&req.path);
            }
            if req.method == "POST" && action_of(&req.path) == Some(GET) {
                return MockResponse::ok(&envelope_ok(pending));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_FEISHU_MEETINGS_PENDING",
            "meeting_notes",
            &format!(r#"{{ meetingId: "{MEETING}" }}"#),
        )
        .await;
        let batches = collect(&ctx, "SELECT note_id FROM saas.minutes.meeting_notes").await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        assert_eq!(execute_inputs(&gateway, GET).len(), 1);
    }

    #[tokio::test]
    async fn the_two_reads_sharing_one_cache_never_replay_each_others_rows() {
        // With the scan cache on, the two tables are the same action and
        // resource; the mode and the table differ, and so do the rows.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return feishu_discovery(&req.path);
            }
            if req.method == "POST" && action_of(&req.path) == Some(GET) {
                return meeting_answer(&req.body);
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let token_env = "SKARDI_TEST_OC_FEISHU_MEETINGS_CACHE";
        let mut config = feishu_config(
            token_env,
            "meetings, meeting_notes",
            &format!(r#"{{ meetingId: "{MEETING}" }}"#),
        );
        config.cache_ttl_seconds = 60;
        let (gateway, ctx) = setup_with_config(gateway, token_env, &config).await;

        let row_count =
            |batches: &[RecordBatch]| batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        let meetings_sql = "SELECT provider, meeting_id FROM saas.minutes.meetings";
        let notes_sql = "SELECT provider, meeting_id FROM saas.minutes.meeting_notes";
        assert_eq!(row_count(&collect(&ctx, meetings_sql).await), 1);
        assert_eq!(row_count(&collect(&ctx, notes_sql).await), 2);
        assert_eq!(row_count(&collect(&ctx, meetings_sql).await), 1);
        assert_eq!(row_count(&collect(&ctx, notes_sql).await), 2);
        assert_eq!(
            execute_inputs(&gateway, GET).len(),
            2,
            "each table fetched once; the repeats were cache hits under their own keys"
        );
    }

    #[rstest]
    #[case::meeting_read_without_its_id(
        "SKARDI_TEST_OC_FEISHU_MEETINGS_NO_ID",
        "meeting_notes",
        r#"{ startTime: "2026-11-01T00:00:00Z", endTime: "2026-11-08T00:00:00Z" }"#,
        "meetingId"
    )]
    #[case::search_without_its_window_end(
        "SKARDI_TEST_OC_FEISHU_MEETINGS_NO_END",
        "meeting_search",
        r#"{ startTime: "2026-11-01T00:00:00Z" }"#,
        "endTime"
    )]
    #[tokio::test]
    async fn a_binding_missing_a_required_resource_fails_before_discovery(
        #[case] token_env: &'static str,
        #[case] tables: &str,
        #[case] resource: &str,
        #[case] missing: &str,
    ) {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let _token = EnvVarGuard::set(token_env, "test-token");
        let mut ctx = SessionContext::new();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&feishu_config(token_env, tables, resource)),
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
                if binding == "minutes" && key == missing
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
                return feishu_discovery(&req.path);
            }
            if req.method == "POST" && action_of(&req.path) == Some(SEARCH) {
                return MockResponse::ok(&envelope_ok(
                    &json!({"items": [search_item("m-1")], "hasMore": true, "pageToken": "stuck"})
                        .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_FEISHU_MEETINGS_LOOP",
            "meeting_search",
            WINDOW,
        )
        .await;
        let err = ctx
            .sql("SELECT meeting_id FROM saas.minutes.meeting_search")
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

    /// error_path is None on purpose: provider refusals arrive as gateway
    /// failure envelopes and the scan fails with the gateway's code and the
    /// action named, never as a truncated or empty result. The first case is
    /// the shape Feishu's reference documents for an expired search token
    /// (`121020`, HTTP 500 → the gateway's `provider_error`, UNPROBED item
    /// 8); the second a meeting the member does not own (`403 121005`,
    /// UNPROBED item 9).
    #[rstest]
    #[case::expired_search_token(
        "SKARDI_TEST_OC_FEISHU_MEETINGS_EXPIRED",
        "meeting_search",
        WINDOW,
        SEARCH,
        500,
        envelope_err("provider_error", "page_token is invalid or expired")
    )]
    #[case::meeting_not_owned(
        "SKARDI_TEST_OC_FEISHU_MEETINGS_NOT_OWNED",
        "meetings",
        r#"{ meetingId: "7000000000000000009" }"#,
        GET,
        403,
        envelope_err("authorization_failed", "no permission")
    )]
    #[tokio::test]
    async fn provider_refusals_surface_through_the_gateway_failure_envelope(
        #[case] token_env: &'static str,
        #[case] short: &'static str,
        #[case] resource: &str,
        #[case] action: &'static str,
        #[case] status: u16,
        #[case] body: String,
    ) {
        let gateway = MockGateway::start(move |req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return feishu_discovery(&req.path);
            }
            if req.method == "POST" && action_of(&req.path) == Some(action) {
                let input: Value = serde_json::from_str(&req.body).unwrap_or_default();
                // The search's first page succeeds; its continuation is
                // refused. The meeting read is refused outright.
                if action == SEARCH && input["input"].get("pageToken").is_none() {
                    return MockResponse::ok(&envelope_ok(
                        &json!({"items": [search_item("m-1")], "hasMore": true, "pageToken": "stale"})
                            .to_string(),
                    ));
                }
                return MockResponse::new(status, body.clone());
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) = setup_with_gateway(gateway, token_env, short, resource).await;
        let err = ctx
            .sql(&format!("SELECT meeting_id FROM saas.minutes.{short}"))
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("a refusal must fail the scan");
        let message = err.to_string();
        let code = if status == 500 {
            "provider_error"
        } else {
            "authorization_failed"
        };
        assert!(
            message.contains(code) && message.contains(action),
            "the gateway's error code and the action are named: {message}"
        );
    }

    #[tokio::test]
    async fn udtf_parity_for_meeting_search() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return feishu_discovery(&req.path);
            }
            if req.method == "POST" && action_of(&req.path) == Some(SEARCH) {
                return MockResponse::ok(&envelope_ok(
                    &json!({"items": [search_item("m-1"), search_item("m-2")],
                            "hasMore": false, "pageToken": null})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_FEISHU_MEETINGS_UDTF",
            "meeting_search",
            WINDOW,
        )
        .await;
        let from_table = collect(
            &ctx,
            "SELECT meeting_id FROM saas.minutes.meeting_search ORDER BY meeting_id",
        )
        .await;
        let from_udtf = collect(
            &ctx,
            &format!(
                "SELECT meeting_id FROM open_connector_query('saas', '{PACK_NAME}.meeting_search', \
                 '{WINDOW_JSON}') ORDER BY meeting_id"
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
        let token_env = "SKARDI_TEST_OC_FEISHU_MEETINGS_DRIFT";
        let _token = EnvVarGuard::set(token_env, "test-token");
        let mut ctx = SessionContext::new();
        let gateways = OpenConnectorGateways::default();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&feishu_config(
                token_env,
                "meetings",
                &format!(r#"{{ meetingId: "{MEETING}" }}"#),
            )),
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
                assert!(reason.contains(GET), "the action is named: {reason}");
            }
            other => panic!("expected ActionContractMismatch, got {other}"),
        }
    }
}
