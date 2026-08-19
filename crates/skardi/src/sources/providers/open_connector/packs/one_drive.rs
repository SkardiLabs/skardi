//! OneDrive (Microsoft 365) source pack: `drive_items` and
//! `drive_item_search` over Open Connector's `one_drive` service,
//! reconciled against a live gateway (v1.3.4, open-connector at
//! `2410fbe`). Design record:
//! `docs/superpowers/specs/2026-08-19-open-connector-one-drive-pack-design.md`;
//! user documentation: `docs/open-connector-one-drive.md`.
//!
//! **Status: phases 1–3 complete; phase 4 (live verification) NOT done.**
//! The contract reconciliation below is live — action inventory, both
//! discovery schemas (byte-identical to the committed captures), the
//! whole input surface of both tables, and the `top` bounds were all
//! probed against a running gateway on 2026-08-19 — but no real drive
//! has been scanned yet, because `one_drive` needs its own OAuth grant
//! (the sibling `outlook` authorization does not cover it; the live
//! probe answers `403 "Connect one_drive with OAuth first."`). Until
//! that pass runs, the fixtures here are SYNTHETIC rather than redacted
//! live captures, and what only real rows can settle stays open: whether
//! `top: 999` is a wire bound as well as a declared one, whether the
//! real terminal page returns a genuinely null `nextLink`, whether a
//! real cursor round-trips the executor's host/path allowlist, whether
//! every mapped column carries a non-NULL value somewhere, and
//! re-deriving the fixtures as redacted live captures. Do not read the
//! column set as live-confirmed. The design record's "what phase 4 must
//! settle" list is the authoritative version.
//!
//! Design decisions and their rationale. Most are held by a named test
//! below; the two that are upstream properties no Skardi-side test can
//! hold — cursor non-interchangeability and the executor's own input
//! validation — say so where they appear, and fall to phase 4:
//!
//! - **Its own pack, not part of a `microsoft365` one.** There is no
//!   `microsoft365` service upstream: Graph is split into `outlook`,
//!   `one_drive` and `excel`, each with a separate OAuth connection,
//!   and a Skardi binding carries exactly one `connection_alias` — so a
//!   cross-service pack would silently span two grants and fail half
//!   its tables at scan time. The `excel` service is deferred whole at
//!   the admission gate (its list actions emit `nextLink` but accept
//!   none, so pagination cannot be completed).
//! - **Rows are RAW Graph passthrough** (`readCollectionItems` only
//!   re-wraps each element of `payload.value`; no renaming, no
//!   rebuilding) and the row object declares `additionalProperties:
//!   true` — but unlike the sibling `outlook` pack, this costs nothing
//!   here: all 16 columns of both tables resolve INSIDE the declared
//!   item schema, so the fingerprint gate covers every one of them and
//!   the coverage-gap pin is EMPTY
//!   (`no_column_escapes_the_fingerprint_gate`). Two distinct mechanisms
//!   follow from that, and they are worth keeping apart: if UPSTREAM
//!   renames or retypes a mapped key, the schema hash changes and
//!   registration fails; if an AUTHOR here mistypes a path, the hash is
//!   unchanged and registration is clean — what catches that is the
//!   coverage test in CI, which would report the column as uncovered.
//!   Either way the failure is loud instead of a silently always-NULL
//!   column, which is why this pack needs no `select` pin — that is the
//!   lever `outlook.messages` uses to buy the same loudness for its
//!   thirteen undeclared columns, and driveItem rows carry metadata
//!   only, so payload size does not force one either.
//! - **Both halves of each contract are committed.** The fingerprint gate
//!   is OUTPUT-only — nothing reads `ActionMetadata::input_schema`, so a
//!   renamed input key would register cleanly and then 400 every scan.
//!   That blind spot is engine-wide, not a one_drive trait; following
//!   gmail, this pack captures the input schemas too
//!   (`contracts/inputs/`) and
//!   `generated_inputs_are_accepted_by_the_captured_input_contracts`
//!   locks every key it can send against them. Both sides are committed
//!   artifacts, so it catches drift on RE-CAPTURE, not live; closing it
//!   properly means an input fingerprint compared at registration, the
//!   same way output is.
//! - **Both tables share one contract fingerprint, by construction.**
//!   Graph returns the same driveItem collection shape for a folder
//!   listing and for a search, so `list_folder_children` and
//!   `search_items` declare byte-identical output schemas (verified
//!   live, both halves). The two equal pins are a fact about upstream,
//!   not a copy-paste slip, and
//!   `both_tables_share_one_contract_fingerprint` states that so a
//!   future reviewer does not "fix" it.
//! - **Cursor pagination over a complete-URL cursor.** Graph's
//!   `@odata.nextLink` is re-exposed as a `nextLink` input/output pair,
//!   `format: uri`, null on the final page — and both output contracts
//!   declare `nextLink` REQUIRED, so the key is always present and the
//!   absent-key spelling is not a shape this gateway can produce (the
//!   engine tolerates it anyway; that tolerance is tested where it
//!   lives, in `pagination`). Two consequences, one pinned here and one
//!   not: every mock and fixture cursor is URI-shaped, which the input
//!   contract test enforces via `nextLink`'s declared `format: uri`; and
//!   the two tables' cursors are NOT interchangeable, because each
//!   executor pins its own allowlisted path set upstream — that one is
//!   an upstream property no Skardi-side test can hold, so the two
//!   cursor constants below simply use their own action's path and
//!   phase 4 confirms it. The engine sends `top` on continuation
//!   requests too; the executors ignore it there because the cursor URL
//!   embeds its own `$top`.
//! - **`query` is a required resource on `drive_item_search`,** and the
//!   enforcement is not where it looks. The input schema's `required`
//!   array is EMPTY and `query` is merely `minLength: 1`, so an empty
//!   string 400s as `invalid_input` at the schema layer, while a
//!   MISSING query and a whitespace-only query both pass validation and
//!   die in the executor's own trim check (`ProviderRequestError(400,
//!   "query is required")`) — all three verified live. Declaring it
//!   required is what keeps Skardi from ever generating the latter two.
//!   Notion's empty-query trick does not transfer: there is no spelling
//!   of "search the whole drive", which is why the term is a resource
//!   (the binding pins it; the table is "the items matching this
//!   binding's query") rather than a fixed input.
//! - **`drive_items` resources are all optional** because every
//!   combination names a completely-terminating collection: with none,
//!   the executor lists the drive root's children
//!   (`buildListFolderChildrenPath` → `/root/children`);
//!   `folderItemId`/`folderPath` scope it to one folder; `driveId`
//!   selects a non-default drive. The listing is NON-RECURSIVE — one
//!   binding sees one folder level, which is exactly what makes
//!   `drive_item_search` worth shipping as the only way to see a whole
//!   drive.
//! - **No filter pushdown, structurally.** Neither action exposes a
//!   filter input at all — there is nothing to map, so the absence is
//!   not an omission. Predicates run in DataFusion after the bounded
//!   fetch; the scoping tools are the folder resources and `LIMIT`
//!   early-stop (which caps requests, not bytes — `top` is sent verbatim,
//!   so a `LIMIT 10` still transfers a full page). Guard tests pin the
//!   exact input key set of every request, so no `filter`, `orderby`,
//!   `skip`, `page` or `perPage` key can reach the wire — those five are
//!   UNDECLARED and were each verified to 400 live — and equally none of
//!   `select`, `expand` or `orderBy`, which upstream does declare but
//!   this pack deliberately never sends. Note the camelCase asymmetry in
//!   that split: `one_drive` declares only `orderBy`, while the sibling
//!   `outlook` service spells the same input `orderby`.
//! - **In-band Graph errors never reach the engine.** The executors
//!   consume them into the gateway's failure envelope, so neither table
//!   declares `error_path` — the spelling reserved for gateways that
//!   FORWARD provider errors.
//! - **Item type is facet presence, not a field.** Graph marks a
//!   driveItem's kind by which facet is present, so `file_mime_type`
//!   and `folder_child_count` double as the discriminator: non-null
//!   means file / means folder. Mapping one scalar out of each facet
//!   keeps that queryable without a JSON column. The seven other
//!   facets (`root`, `deleted`, `shared`, `specialFolder`,
//!   `remoteItem`, `searchResult`, `fileSystemInfo`) are declared as
//!   bare open objects and stay unmapped: presence-as-signal, not data,
//!   and any child path under them would be passthrough anyway.

use std::sync::OnceLock;

use crate::sources::providers::open_connector::error::OpenConnectorError;
use crate::sources::providers::open_connector::source_pack::SourcePack;

use super::loader;

static PACK: OnceLock<Result<SourcePack, String>> = OnceLock::new();

/// The OneDrive pack, parsed once from the embedded YAML asset.
pub fn pack() -> Result<&'static SourcePack, OpenConnectorError> {
    loader::builtin("one_drive.yaml", include_str!("one_drive.yaml"), &PACK)
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
        EnvVarGuard, MockGateway, MockResponse, discovery_ok, envelope_err, envelope_ok,
        fingerprint_uncovered_columns,
    };
    use crate::sources::providers::open_connector::{
        OpenConnectorConfig, OpenConnectorGateways, register_open_connector_tables,
        register_open_connector_udtfs,
    };
    use arrow::array::{Array, Int64Array, StringArray, TimestampMillisecondArray};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use serde_json::{Value, json};

    /// Every cursor here is URI-shaped on purpose: the gateway validates
    /// `nextLink` as `format: uri` BEFORE credentials, and each executor
    /// additionally pins host `graph.microsoft.com` plus its own
    /// allowlisted path set — an opaque token would sail through these
    /// mocks and 400 live. The two constants use their own action's path
    /// because the allowlists are per-action: a children cursor handed
    /// to `search_items` is rejected upstream, and vice versa.
    const CHILDREN_PAGE2_URI: &str = "https://graph.microsoft.com/v1.0/me/drive/root/children?%24top=999&%24skiptoken=SyntheticChildren2";
    const SEARCH_PAGE2_URI: &str = "https://graph.microsoft.com/v1.0/me/drive/root/search(q='budget')?%24top=999&%24skiptoken=SyntheticSearch2";

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
    /// registration exercises the fingerprint gate's pass side. Both
    /// actions serve the SAME capture — that is the upstream fact, not a
    /// shortcut (see `both_tables_share_one_contract_fingerprint`).
    fn one_drive_discovery(path: &str) -> MockResponse {
        let output_schema = if path.ends_with("one_drive.list_folder_children") {
            include_str!("fixtures/one_drive/contracts/list_folder_children.json")
        } else if path.ends_with("one_drive.search_items") {
            include_str!("fixtures/one_drive/contracts/search_items.json")
        } else {
            r#"{"type": "object"}"#
        };
        MockResponse::ok(&discovery_ok("{}", output_schema, true, None))
    }

    // ── Contract tests. UNLIKE the sibling outlook pack's, the row
    // fixtures here are SYNTHETIC: phase 4 has not run, because
    // `one_drive` needs its own OAuth grant and the live gateway answers
    // `403 "Connect one_drive with OAuth first."`. They encode the
    // DECLARED schema (all 16 columns resolve inside it) plus Graph's
    // documented facet/identitySet shapes — not observed rows. Phase 4
    // must re-derive them as redacted live captures and re-check every
    // assertion below against real data. ─────────────────────────────

    fn convert_fixture(table: &SourcePackTable, fixture: &str) -> RecordBatch {
        let page: Value = serde_json::from_str(fixture).expect("fixture parses");
        convert_page(table, &page)
    }

    fn convert_page(table: &SourcePackTable, page: &Value) -> RecordBatch {
        let rows = RowPath::parse(table.row_path)
            .expect("row path")
            .rows(page, 1)
            .expect("row array");
        RowConverter::new(table.fields)
            .expect("converter")
            .convert(rows, 1)
            .expect("page converts")
    }

    fn utf8<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("column {name}"))
            .as_any()
            .downcast_ref()
            .expect("Utf8 column")
    }

    fn int64<'a>(batch: &'a RecordBatch, name: &str) -> &'a Int64Array {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("column {name}"))
            .as_any()
            .downcast_ref()
            .expect("Int64 column")
    }

    fn timestamp<'a>(batch: &'a RecordBatch, name: &str) -> &'a TimestampMillisecondArray {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("column {name}"))
            .as_any()
            .downcast_ref()
            .expect("Timestamp column")
    }

    #[test]
    fn drive_items_fixture_converts_every_designed_row_shape() {
        let batch = convert_fixture(
            table("drive_items"),
            include_str!("fixtures/one_drive/drive_items.json"),
        );
        assert_eq!(batch.num_rows(), 7);

        // Row 0 — a fully-populated file, including both concurrency
        // tags and both nested identity/reference paths. It also carries
        // an undeclared `@odata.etag`, which must simply be ignored
        // rather than break conversion (passthrough rows always may).
        assert_eq!(utf8(&batch, "name").value(0), "quarterly-report.xlsx");
        assert_eq!(int64(&batch, "size").value(0), 48213);
        assert!(utf8(&batch, "e_tag").value(0).contains("11111111"));
        assert!(utf8(&batch, "c_tag").value(0).starts_with("\"c:"));
        assert_eq!(
            utf8(&batch, "created_by_display_name").value(0),
            "Person One"
        );
        assert_eq!(
            utf8(&batch, "last_modified_by_display_name").value(0),
            "Person Two"
        );
        assert_eq!(
            utf8(&batch, "parent_path").value(0),
            "/drive/root:/Documents"
        );
        assert!(utf8(&batch, "parent_drive_id").value(0).starts_with("b!"));
        assert!(timestamp(&batch, "created_date_time").is_valid(0));
        assert!(timestamp(&batch, "last_modified_date_time").is_valid(0));

        // Rows 0/1 — facet presence IS the type discriminator, which is
        // the whole reason these two columns are mapped: the file has a
        // mime type and no child count, the folder the reverse.
        assert!(utf8(&batch, "file_mime_type").is_valid(0));
        assert!(int64(&batch, "folder_child_count").is_null(0));
        assert!(utf8(&batch, "file_mime_type").is_null(1));
        assert_eq!(int64(&batch, "folder_child_count").value(1), 7);

        // Row 2 — an empty folder. `childCount: 0` must survive as 0,
        // not collapse into NULL, or "empty folder" becomes
        // indistinguishable from "not a folder".
        assert_eq!(int64(&batch, "folder_child_count").value(2), 0);
        // Absent optional keys become SQL NULL.
        assert!(utf8(&batch, "description").is_null(2));
        assert!(utf8(&batch, "e_tag").is_null(2));
        assert!(utf8(&batch, "created_by_display_name").is_null(2));

        // Row 3 — explicit nulls, including nulls PART WAY down a mapped
        // path (`createdBy.user` null, `parentReference` null,
        // `file.mimeType` null). Every one must land as SQL NULL rather
        // than fail the page.
        assert!(utf8(&batch, "description").is_null(3));
        assert!(int64(&batch, "size").is_null(3));
        assert!(timestamp(&batch, "created_date_time").is_null(3));
        assert!(utf8(&batch, "created_by_display_name").is_null(3));
        assert!(utf8(&batch, "last_modified_by_display_name").is_null(3));
        assert!(utf8(&batch, "parent_drive_id").is_null(3));
        assert!(utf8(&batch, "parent_path").is_null(3));
        assert!(utf8(&batch, "file_mime_type").is_null(3));
        assert!(int64(&batch, "folder_child_count").is_null(3));
        // …but identity is non-nullable and still present.
        assert!(utf8(&batch, "id").is_valid(3));

        // Row 4 — the empty-string arm, distinct from NULL at every
        // level including through the nested paths.
        assert_eq!(utf8(&batch, "name").value(4), "");
        assert_eq!(utf8(&batch, "description").value(4), "");
        assert_eq!(utf8(&batch, "created_by_display_name").value(4), "");
        assert_eq!(utf8(&batch, "parent_path").value(4), "");
        assert!(utf8(&batch, "description").is_valid(4), "empty is not null");

        // Row 6 — a special folder still reads as an ordinary folder
        // row; `specialFolder` itself is deliberately unmapped.
        assert_eq!(utf8(&batch, "name").value(6), "Photos");
        assert_eq!(int64(&batch, "folder_child_count").value(6), 128);
        assert_eq!(utf8(&batch, "parent_path").value(6), "/drive/root:");
        assert!(batch.column_by_name("special_folder").is_none());
    }

    #[test]
    fn identity_arms_other_than_user_leave_the_display_name_null() {
        // Graph's identitySet has user/application/device arms and this
        // pack maps ONLY the user arm (see the yaml rationale). Row 5 was
        // created by an application, so both display-name columns must be
        // NULL — mapping the whole identitySet would have hidden this
        // distinction behind a JSON blob.
        let batch = convert_fixture(
            table("drive_items"),
            include_str!("fixtures/one_drive/drive_items.json"),
        );
        assert_eq!(utf8(&batch, "name").value(5), "sync-log.json");
        assert!(utf8(&batch, "created_by_display_name").is_null(5));
        assert!(utf8(&batch, "last_modified_by_display_name").is_null(5));
        // The row is otherwise intact — a null identity is not a broken row.
        assert_eq!(utf8(&batch, "file_mime_type").value(5), "application/json");
        assert_eq!(int64(&batch, "size").value(5), 902);
    }

    #[test]
    fn search_fixture_converts_and_carries_a_uri_cursor() {
        let t = table("drive_item_search");
        let fixture = include_str!("fixtures/one_drive/drive_item_search.json");
        let batch = convert_fixture(t, fixture);
        assert_eq!(batch.num_rows(), 3);

        // Search spans folder levels — that is the point of the table,
        // since `list_folder_children` is non-recursive. The three rows
        // sit at three different depths.
        assert_eq!(
            utf8(&batch, "parent_path").value(0),
            "/drive/root:/Documents/Finance"
        );
        assert_eq!(
            utf8(&batch, "parent_path").value(1),
            "/drive/root:/Documents/Finance/2026"
        );
        assert_eq!(
            utf8(&batch, "parent_path").value(2),
            "/drive/root:/Documents"
        );
        // Folders come back from search too, discriminated the same way.
        assert!(utf8(&batch, "file_mime_type").is_valid(0));
        assert_eq!(int64(&batch, "folder_child_count").value(2), 3);
        // `searchResult` is present on the wire and deliberately unmapped.
        assert!(batch.column_by_name("search_result").is_none());

        // The declared cursor path finds a URI-shaped cursor.
        let page: Value = serde_json::from_str(fixture).expect("fixture parses");
        assert!(
            page["nextLink"]
                .as_str()
                .expect("cursor present")
                .starts_with("https://graph.microsoft.com/"),
            "fixture cursors must be URI-shaped like the real wire"
        );
    }

    #[test]
    fn fixtures_stay_synthetic_under_a_default_deny_audit() {
        // These fixtures are SYNTHETIC, not redacted captures (phase 4
        // pending) — but the audit that guards them has to be able to fail,
        // or it guards nothing. So: every string leaf must satisfy an
        // allowlist FOR ITS KEY, default-deny. Key scoping is what makes
        // that enforce anything, because shape alone proves nothing here —
        // a real drive item's `name` is an ordinary filename and a real
        // `webUrl` is an ordinary https URL, so both would coast through a
        // shape check. When phase 4 re-derives these from live captures
        // this test TIGHTENS (new arms per key) rather than being rewritten.
        fn audit(name: &str, key: &str, value: &Value) {
            // A synthetic GUID here is a repeated hex digit
            // (`{22222222-2222-…}`); a real one is not.
            fn repeated_hex_guid(s: &str) -> bool {
                s.as_bytes()
                    .windows(8)
                    .any(|w| w.iter().all(|b| *b == w[0] && b.is_ascii_hexdigit()))
            }
            match value {
                Value::String(s) => {
                    let allowed = match key {
                        // driveItem ids carry the synthetic prefix; the
                        // identitySet ids under createdBy/lastModifiedBy
                        // arrive under this same key and use a placeholder
                        // UUID. The empty arm is the empty-string row.
                        "id" => {
                            s.is_empty()
                                || s.starts_with("01SYNTHETIC")
                                || s.ends_with("-1111-2222-3333-444444444444")
                        }
                        // No shape can vouch for a filename, so the set is
                        // explicit — a real one fails.
                        "name" => [
                            "",
                            "2026",
                            "Archive",
                            "Budget archive",
                            "Documents",
                            "Finance",
                            "Photos",
                            "Projects",
                            "budget-2026.xlsx",
                            "budget-notes.docx",
                            "photos",
                            "quarterly-report.xlsx",
                            "sync-log.json",
                            "untitled.txt",
                            "wrong-types.bin",
                        ]
                        .contains(&s.as_str()),
                        "description" => [
                            "",
                            "Numbers for the quarterly review",
                            "Search hit in a nested folder",
                        ]
                        .contains(&s.as_str()),
                        // The one place a real tenant name would hide.
                        "webUrl" => {
                            s.is_empty()
                                || s.starts_with("https://example-my.sharepoint.com/personal/user/")
                        }
                        "displayName" => ["", "Person One", "Person Two", "Synthetic Sync App"]
                            .contains(&s.as_str()),
                        "driveId" => s.is_empty() || s.starts_with("b!Synthetic"),
                        "path" => s.is_empty() || s.starts_with("/drive/root:"),
                        "eTag" | "cTag" | "@odata.etag" => s.is_empty() || repeated_hex_guid(s),
                        // A cursor must be URI-shaped AND visibly synthetic.
                        "nextLink" => {
                            s.starts_with("https://graph.microsoft.com/") && s.contains("Synthetic")
                        }
                        "quickXorHash" => s.starts_with("Synthetic"),
                        "onClickTelemetryUrl" => s.starts_with("https://example.invalid/"),
                        // Public platform constants and enums.
                        "mimeType" => s.is_empty() || s.contains('/'),
                        "driveType" => s == "personal",
                        "scope" => s == "users",
                        "sortBy" => s == "name",
                        "sortOrder" => s == "ascending",
                        "viewType" => s == "thumbnails",
                        // RFC 3339, UTC, second precision — the spelling
                        // Graph emits.
                        "createdDateTime" | "lastModifiedDateTime" => {
                            s.len() == 20 && s.ends_with('Z')
                        }
                        // The type-mismatch probe's string-where-int64.
                        "size" => !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()),
                        _ => false,
                    };
                    assert!(
                        allowed,
                        "{name}: {key} = {s:?} is not on the redaction allowlist"
                    );
                }
                Value::Array(items) => items.iter().for_each(|v| audit(name, key, v)),
                Value::Object(map) => map.iter().for_each(|(k, v)| audit(name, k, v)),
                _ => {}
            }
        }

        for (name, fixture) in [
            (
                "drive_items",
                include_str!("fixtures/one_drive/drive_items.json"),
            ),
            (
                "drive_item_search",
                include_str!("fixtures/one_drive/drive_item_search.json"),
            ),
            (
                "drive_items_type_mismatch",
                include_str!("fixtures/one_drive/drive_items_type_mismatch.json"),
            ),
        ] {
            assert!(
                fixture.is_ascii(),
                "{name}: fixtures stay ASCII so redaction is auditable by eye"
            );
            let root: Value = serde_json::from_str(fixture).expect("fixture parses");
            audit(name, "$", &root);
        }

        // The tripwire must TRIP. Each probe is a leak class a phase-4
        // re-capture could plausibly reintroduce.
        for (key, leak) in [
            // A real tenant host instead of the example one.
            (
                "webUrl",
                "https://contoso-my.sharepoint.com/personal/real.person/Documents/x.xlsx",
            ),
            ("displayName", "Real Person"), // identity off the list
            ("name", "Acme Q3 headcount.xlsx"), // a real filename shape
            ("id", "01BYZ5EMFAKEREALLOOKINGITEMID"), // id without the prefix
            (
                "nextLink",
                "https://graph.microsoft.com/v1.0/me/drive/root/children?%24skiptoken=REAL",
            ), // URI-shaped but not synthetic
            ("ownerEmail", "person@example.com"), // a key the allowlist never saw
        ] {
            let probe = json!({ key: leak });
            assert!(
                std::panic::catch_unwind(|| audit("probe", "$", &probe)).is_err(),
                "audit must reject {key} = {leak:?}"
            );
        }
    }

    #[test]
    fn empty_page_keeps_schema_stable() {
        // An empty drive folder must still produce the full 16-column
        // schema, or a first-page-empty scan would change shape.
        for short in ["drive_items", "drive_item_search"] {
            let t = table(short);
            let batch = convert_page(t, &json!({"items": [], "nextLink": null}));
            assert_eq!(batch.num_rows(), 0, "{short}");
            assert_eq!(batch.num_columns(), 16, "{short} keeps its declared schema");
            assert!(batch.column_by_name("id").is_some(), "{short}");
        }
    }

    #[test]
    fn type_mismatch_fixture_fails_with_the_targeted_error() {
        // Admission-gate schema-mismatch fixture: `size` arrives as a
        // STRING where the pack declares int64. The failure must carry the
        // full row-scoped identity and the JSON KIND — never a silent NULL,
        // and never the offending value (Graph sizes are not secrets, but
        // the discipline is uniform because rows can be).
        let t = table("drive_items");
        let page: Value = serde_json::from_str(include_str!(
            "fixtures/one_drive/drive_items_type_mismatch.json"
        ))
        .expect("fixture parses");
        let rows = RowPath::parse(t.row_path)
            .expect("row path")
            .rows(&page, 1)
            .expect("row array");
        let err = RowConverter::new(t.fields)
            .expect("converter")
            .convert(rows, 1)
            .expect_err("a string in an int64 column must fail the page");
        match err {
            OpenConnectorError::ConversionFailed {
                ref path,
                ref column,
                page,
                row,
                ref expected,
                ref found,
            } => {
                assert_eq!(column, "size");
                // Row-relative in the yaml, `$.`-rooted in the error: the
                // converter compiles each column path as `$.{path}`.
                assert_eq!(path, "$.size");
                assert_eq!(page, 1);
                assert_eq!(row, 0, "the fixture's only row is the failing one");
                assert_eq!(expected, "integer");
                assert_eq!(found, "a string");
                assert!(
                    !err.to_string().contains("48213"),
                    "row values never appear in errors"
                );
            }
            other => panic!("expected ConversionFailed, got {other}"),
        }
    }

    #[test]
    fn an_item_without_an_id_fails_the_page_rather_than_yielding_a_null_key() {
        // `id` is the one non-nullable column, and the failing arm matters
        // because an item without `id` is CONTRACT-LEGAL: the declared item
        // schema carries no `required` array, so the fingerprint gate would
        // never object. Losing the key silently would make rows
        // unjoinable; the page fails instead, naming the column.
        let t = table("drive_items");
        let mut page: Value =
            serde_json::from_str(include_str!("fixtures/one_drive/drive_items.json"))
                .expect("fixture parses");
        page["items"][1]
            .as_object_mut()
            .expect("row is an object")
            .remove("id");
        let rows = RowPath::parse(t.row_path)
            .expect("row path")
            .rows(&page, 3)
            .expect("row array");
        let err = RowConverter::new(t.fields)
            .expect("converter")
            .convert(rows, 3)
            .expect_err("a missing id must fail the page");
        match err {
            OpenConnectorError::ConversionFailed {
                ref column,
                page,
                row,
                ref found,
                ..
            } => {
                assert_eq!(column, "id");
                assert_eq!(page, 3, "the page number travels with the error");
                assert_eq!(row, 1, "the row index is named");
                assert_eq!(found, "missing key");
            }
            other => panic!("expected ConversionFailed, got {other}"),
        }
    }

    #[test]
    fn pinned_fingerprints_match_the_reconciled_contracts() {
        for (short, contract) in [
            (
                "drive_items",
                include_str!("fixtures/one_drive/contracts/list_folder_children.json"),
            ),
            (
                "drive_item_search",
                include_str!("fixtures/one_drive/contracts/search_items.json"),
            ),
        ] {
            let t = table(short);
            let schema: Value = serde_json::from_str(contract).expect("contract fixture parses");
            let actual = fingerprint_schema(Some(&schema));
            assert_eq!(
                t.expected_fingerprint,
                Some(actual.as_str()),
                "{}: pinned {:?}, contract fixture hashes to {actual}",
                t.id,
                t.expected_fingerprint
            );
        }
    }

    #[test]
    fn both_tables_share_one_contract_fingerprint() {
        // Graph returns the same driveItem collection shape for a folder
        // listing and for a search, so the two captures are
        // byte-identical and the two pins are EQUAL BY CONSTRUCTION.
        // Stated explicitly so a future reviewer reads the duplicate pin
        // as the upstream fact it is, rather than "fixing" it.
        let children = include_str!("fixtures/one_drive/contracts/list_folder_children.json");
        let search = include_str!("fixtures/one_drive/contracts/search_items.json");
        let children_schema: Value = serde_json::from_str(children).expect("parses");
        let search_schema: Value = serde_json::from_str(search).expect("parses");
        assert_eq!(
            children_schema, search_schema,
            "the two captured contracts are the same declared schema"
        );
        assert_eq!(
            table("drive_items").expected_fingerprint,
            table("drive_item_search").expected_fingerprint,
        );
    }

    #[test]
    fn no_column_escapes_the_fingerprint_gate() {
        // The pack's central structural claim, and its sharpest contrast
        // with the sibling outlook pack (thirteen uncovered columns on
        // `messages`): every mapped path here resolves inside the
        // declared item schema, so drift in ANY column is caught at
        // registration rather than surfacing as a silently-NULL column.
        // This is also why no `select` pin is needed. If this list ever
        // becomes non-empty, the pack has grown a passthrough surface
        // that needs its own deliberate review.
        for (short, contract) in [
            (
                "drive_items",
                include_str!("fixtures/one_drive/contracts/list_folder_children.json"),
            ),
            (
                "drive_item_search",
                include_str!("fixtures/one_drive/contracts/search_items.json"),
            ),
        ] {
            let t = table(short);
            assert_eq!(
                fingerprint_uncovered_columns(contract, t.row_path, t.fields),
                &[] as &[&str],
                "{short}: every column must stay inside the fingerprint gate"
            );
        }
    }

    #[test]
    fn both_tables_declare_terminating_cursor_pagination() {
        for short in ["drive_items", "drive_item_search"] {
            let t = table(short);
            match t.pagination {
                PaginationStrategy::Cursor {
                    cursor_param,
                    next_cursor_path,
                    page_size_param,
                    page_size,
                    has_more_path,
                } => {
                    assert_eq!(cursor_param, "nextLink", "{short}");
                    assert_eq!(next_cursor_path, "$.nextLink", "{short}");
                    assert_eq!(page_size_param, Some("top"), "{short}");
                    // The declared ceiling; 1000 and 0 both 400 live.
                    // Phase 4 must confirm 999 is a WIRE bound too
                    // (feishu declared 100 and hard-failed above 50).
                    assert_eq!(page_size, 999, "{short}");
                    // Termination is the cursor going null — Graph has no
                    // has-more flag, so the feishu-style override that
                    // rescues a non-empty terminal cursor is not declared,
                    // and phase 4 must confirm the real final page really
                    // does return null here.
                    assert!(has_more_path.is_none(), "{short}");
                }
                other => panic!("{short} must paginate by cursor, got {other:?}"),
            }
            assert_eq!(t.row_path, "$.items", "{short}");
            // In-band Graph errors are consumed by the executor into the
            // gateway's failure envelope, so no table forwards them.
            assert!(t.error_path.is_none(), "{short}");
        }
    }

    #[test]
    fn query_is_required_on_search_and_nothing_is_required_on_children() {
        // `drive_items` is well-defined with no resource at all (the
        // executor lists the drive root's children), so every resource
        // is optional. `drive_item_search` cannot be: there is no
        // spelling of "search everything", and a missing or
        // whitespace-only query dies in the executor's own trim check
        // rather than as `invalid_input` — declaring it required is what
        // stops Skardi ever generating that call.
        let children = table("drive_items");
        assert!(children.required_resources.is_empty());
        assert_eq!(
            children.optional_resources,
            &["driveId", "folderItemId", "folderPath"]
        );
        let search = table("drive_item_search");
        assert_eq!(search.required_resources, &["query"]);
        assert_eq!(search.optional_resources, &["driveId"]);
    }

    #[test]
    fn no_table_pins_a_fixed_input() {
        // Deliberate absence, unlike every sibling pack: `select` is not
        // pinned (declared coverage already makes drift loud, and
        // driveItem rows carry no body-sized payload), and there is no
        // `state`/`includeHidden` equivalent to pin. `orderBy`,
        // `expand`, `filter` and friends must never reach the wire —
        // asserted end to end below.
        for short in ["drive_items", "drive_item_search"] {
            assert!(
                table(short).fixed_inputs.is_empty(),
                "{short} sends no pinned input"
            );
        }
    }

    #[test]
    fn generated_inputs_are_accepted_by_the_captured_input_contracts() {
        // The output fingerprint gate is OUTPUT-only — nothing reads
        // `ActionMetadata::input_schema`, so a renamed input key would
        // register cleanly and then 400 every scan. This test supplies the
        // missing half from the captured input contracts (gateway v1.3.4,
        // re-fetched 2026-08-19), the way the gmail pack does. It compares
        // committed artifacts, NOT the live gateway: drift nobody
        // re-captures stays invisible to it. Its value is the upgrade path
        // — re-capturing after an upstream bump makes a renamed
        // `nextLink`, a narrowed `top` bound or a newly-required key fail
        // HERE. Closing it properly needs an input fingerprint checked at
        // registration (engine work, tracked in the source-pack skill).
        for (short, contract) in [
            (
                "drive_items",
                include_str!("fixtures/one_drive/contracts/inputs/list_folder_children.json"),
            ),
            (
                "drive_item_search",
                include_str!("fixtures/one_drive/contracts/inputs/search_items.json"),
            ),
        ] {
            let schema: Value =
                serde_json::from_str(contract).expect("input contract fixture parses");
            let properties = &schema["properties"];
            let t = table(short);

            // Strictness is this test's premise, and the reason every
            // "never sent" claim below is load-bearing rather than
            // cosmetic: a lenient action would ignore an undeclared key
            // instead of 400ing on it.
            assert_eq!(
                schema["additionalProperties"],
                json!(false),
                "{short}: the action's input schema is strict"
            );

            // Every key this table can put on the wire must be declared.
            let mut generated: Vec<&str> = t
                .required_resources
                .iter()
                .chain(t.optional_resources)
                .copied()
                .collect();
            generated.extend(t.fixed_inputs.iter().map(|(key, _)| *key));
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

            // …and anything the action requires must be among them. Both
            // actions declare NO `required` array today (which is why
            // `query` needs the resource-level requirement instead); the
            // check exists so a newly-required key breaks here.
            if let Some(required) = schema["required"].as_array() {
                for entry in required {
                    let entry = entry.as_str().expect("required entries are strings");
                    assert!(
                        generated.contains(&entry),
                        "{short}: the action requires `{entry}`, which this table never sends"
                    );
                }
            }

            // The requested page size must sit inside the declared bounds
            // (`top`: 1–999, so the pinned 999 is the ceiling exactly).
            if let PaginationStrategy::Cursor {
                page_size_param: Some(param),
                page_size,
                ..
            } = t.pagination
            {
                let declared = &properties[param];
                assert!(
                    u64::from(page_size) >= declared["minimum"].as_u64().expect("declared minimum")
                        && u64::from(page_size)
                            <= declared["maximum"].as_u64().expect("declared maximum"),
                    "{short}: page size {page_size} is outside `{param}`'s declared bounds"
                );
            }

            // The cursor is a COMPLETE URL, not an opaque token — which is
            // why every mock and fixture cursor in this module is
            // URI-shaped, and why the two tables' cursors cannot be
            // swapped (each executor also pins its own path allowlist).
            assert_eq!(
                properties["nextLink"]["format"],
                json!("uri"),
                "{short}: the cursor is declared as a URI"
            );

            // Negative space, straight from the contract: there is no
            // filter input to map, so "no filter pushdown" is structural
            // rather than an omission. And the camelCase is real — the
            // lower-case `orderby` the sibling outlook service uses is
            // undeclared here (a live 400), which is exactly the kind of
            // near-miss a strict schema turns into a runtime failure.
            for absent in ["filter", "orderby", "skip", "page", "perPage", "pageSize"] {
                assert!(
                    properties[absent].is_null(),
                    "{short}: `{absent}` is not part of this action's input surface"
                );
            }
            assert!(
                !properties["orderBy"].is_null(),
                "{short}: the declared spelling is camelCase `orderBy`"
            );
        }

        // The three-way `query` enforcement this pack documents: declared
        // with `minLength: 1` (so an EMPTY string is a schema-layer
        // `invalid_input` 400) but absent from any `required` array (so a
        // MISSING or whitespace-only query passes validation and dies in
        // the executor's own trim check). Declaring it a required RESOURCE
        // is what stops Skardi ever generating those two.
        let search: Value = serde_json::from_str(include_str!(
            "fixtures/one_drive/contracts/inputs/search_items.json"
        ))
        .expect("input contract fixture parses");
        assert_eq!(search["properties"]["query"]["minLength"], json!(1));
        assert!(
            search["required"].is_null(),
            "the action does not require `query` itself: {}",
            search["required"]
        );
        assert_eq!(table("drive_item_search").required_resources, &["query"]);
    }

    // ── Integration: the pack against a mock gateway, end to end. ───────

    fn one_drive_config(token_env: &str, tables: &str, resource: &str) -> OpenConnectorConfig {
        let resource_line = if resource.is_empty() {
            String::new()
        } else {
            format!("resource: {resource}")
        };
        serde_yaml::from_str(&format!(
            r#"
runtime_token_env: {token_env}
bindings:
  - name: drive
    source_pack: one_drive
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
            Some(&one_drive_config(token_env, tables, resource)),
            false,
            HierarchyLevel::Catalog,
            Some(&gateways),
        )
        .await
        .expect("gateway registration succeeds");
        register_open_connector_udtfs(&ctx, gateways).expect("UDTF registration succeeds");
        (gateway, ctx)
    }

    async fn collect(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        ctx.sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect")
    }

    fn column_values(batches: &[RecordBatch], name: &str) -> Vec<String> {
        batches
            .iter()
            .flat_map(|batch| {
                let values = batch
                    .column_by_name(name)
                    .unwrap_or_else(|| panic!("column {name}"))
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Utf8 column")
                    .clone();
                (0..values.len()).map(move |i| values.value(i).to_string())
            })
            .collect()
    }

    fn execute_inputs(gateway: &MockGateway, action_path: &str) -> Vec<Value> {
        gateway
            .requests()
            .into_iter()
            .filter(|r| r.method == "POST" && r.path.ends_with(action_path))
            .map(|r| {
                serde_json::from_str::<Value>(&r.body).expect("request body is JSON")["input"]
                    .clone()
            })
            .collect()
    }

    fn input_keys(input: &Value) -> Vec<&str> {
        let mut keys: Vec<&str> = input
            .as_object()
            .expect("input object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        keys
    }

    /// A minimal file row: enough keys to exercise identity plus one
    /// nested path, deliberately not a full driveItem.
    fn item_row(id: &str) -> Value {
        json!({
            "id": id,
            "name": format!("{id}.txt"),
            "size": 12,
            "createdDateTime": "2026-08-14T09:15:42Z",
            "lastModifiedDateTime": "2026-08-14T09:15:42Z",
            "parentReference": {"driveId": "b!drive", "id": "01root", "path": "/drive/root:"},
            "file": {"mimeType": "text/plain"}
        })
    }

    #[tokio::test]
    async fn drive_items_cursor_scan_pages_with_its_own_declared_inputs() {
        // Two-page cursor scan pinning the wire declaration: no nextLink
        // on page 1, the URI cursor verbatim afterwards, `top` on EVERY
        // request (the engine sends the page size on continuations too;
        // the executor ignores it there, and this pins that shape),
        // explicit null termination, row identity across pages, and
        // exact key sets — no filter/orderBy/select/expand ever.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return one_drive_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/one_drive.list_folder_children" {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                let page = match body["input"].get("nextLink").and_then(Value::as_str) {
                    None => json!({"items": [item_row("i-1"), item_row("i-2")],
                                    "nextLink": CHILDREN_PAGE2_URI}),
                    Some(uri) if uri == CHILDREN_PAGE2_URI => {
                        json!({"items": [item_row("i-3")], "nextLink": null})
                    }
                    Some(other) => return MockResponse::new(400, format!("bad cursor {other}")),
                };
                return MockResponse::ok(&envelope_ok(&page.to_string()));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_ONEDRIVE_ITEMS", "drive_items", "").await;

        let batches = collect(&ctx, "SELECT id FROM saas.drive.drive_items ORDER BY id").await;
        assert_eq!(column_values(&batches, "id"), vec!["i-1", "i-2", "i-3"]);

        let inputs = execute_inputs(&gateway, "one_drive.list_folder_children");
        assert_eq!(inputs.len(), 2, "two cursor pages");
        assert_eq!(
            inputs[1]["nextLink"], CHILDREN_PAGE2_URI,
            "the URI cursor verbatim"
        );
        for (page, (input, expected_keys)) in inputs
            .iter()
            .zip([vec!["top"], vec!["nextLink", "top"]])
            .enumerate()
        {
            assert_eq!(input["top"], 999, "declared ceiling: {input}");
            assert_eq!(input_keys(input), expected_keys, "page {} keys", page + 1);
        }
    }

    #[tokio::test]
    async fn search_scan_forwards_its_required_query_and_terminates_on_a_null_cursor() {
        // The search table's own wire pin: `query` from the binding on
        // every request including continuations, and termination on the
        // spelling this action really emits. Both captured output
        // contracts declare `required: ["items", "nextLink"]` with
        // `additionalProperties: false`, so the key is ALWAYS present and
        // an absent-`nextLink` page is a shape the gateway cannot produce
        // — mocking one here would encode a contract violation as if it
        // were the wire. The engine's tolerance of the absent and
        // empty-string spellings is real but engine-level, and covered by
        // `pagination`'s own unit tests rather than faked at this seam.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return one_drive_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/one_drive.search_items" {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                let page = match body["input"].get("nextLink").and_then(Value::as_str) {
                    None => {
                        json!({"items": [item_row("s-1")], "nextLink": SEARCH_PAGE2_URI})
                    }
                    Some(uri) if uri == SEARCH_PAGE2_URI => {
                        json!({"items": [item_row("s-2")], "nextLink": null})
                    }
                    Some(other) => return MockResponse::new(400, format!("bad cursor {other}")),
                };
                return MockResponse::ok(&envelope_ok(&page.to_string()));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_ONEDRIVE_SEARCH",
            "drive_item_search",
            "{query: budget}",
        )
        .await;

        let batches = collect(
            &ctx,
            "SELECT id FROM saas.drive.drive_item_search ORDER BY id",
        )
        .await;
        assert_eq!(column_values(&batches, "id"), vec!["s-1", "s-2"]);

        let inputs = execute_inputs(&gateway, "one_drive.search_items");
        assert_eq!(inputs.len(), 2, "two cursor pages");
        for (page, (input, expected_keys)) in inputs
            .iter()
            .zip([vec!["query", "top"], vec!["nextLink", "query", "top"]])
            .enumerate()
        {
            assert_eq!(input["query"], "budget", "the pinned term: {input}");
            assert_eq!(input["top"], 999, "declared ceiling: {input}");
            assert_eq!(input_keys(input), expected_keys, "page {} keys", page + 1);
        }
    }

    #[tokio::test]
    async fn a_search_binding_without_a_query_fails_before_any_http() {
        // The failing arm of this pack's headline safety claim. Declaring
        // `query` a required resource is what keeps Skardi from ever
        // generating the two executor-layer 400s a missing or
        // whitespace-only query produces upstream — and that only holds if
        // the binding is refused. It is refused before discovery, so the
        // gateway sees nothing but the health probe: no action is even
        // looked up, let alone executed.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return one_drive_discovery(&req.path);
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let _token = EnvVarGuard::set("SKARDI_TEST_OC_ONEDRIVE_NO_QUERY", "test-token");
        let mut ctx = SessionContext::new();
        let gateways = OpenConnectorGateways::default();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&one_drive_config(
                "SKARDI_TEST_OC_ONEDRIVE_NO_QUERY",
                "drive_item_search",
                "",
            )),
            false,
            HierarchyLevel::Catalog,
            Some(&gateways),
        )
        .await
        .expect_err("a search binding with no query must fail registration");
        assert!(
            err.to_string().contains("query"),
            "the missing resource is named: {err}"
        );
        assert!(
            gateway.requests().iter().all(|r| r.path == "/v1/health"),
            "resource enforcement precedes discovery"
        );
    }

    #[tokio::test]
    async fn optional_resources_forward_verbatim_and_only_where_declared() {
        // `folderItemId` scopes the listing to one folder and must reach
        // the wire byte-for-byte. The same binding also carries no
        // `driveId`, proving an unbound optional resource is WITHHELD
        // rather than sent as null — a strict schema would 400 on
        // `{"driveId": null}`.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return one_drive_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/one_drive.list_folder_children" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"items": [item_row("i-1")], "nextLink": null}).to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_ONEDRIVE_RESOURCE",
            "drive_items",
            "{folderItemId: 01FOLDERSCOPED}",
        )
        .await;

        let batches = collect(&ctx, "SELECT id FROM saas.drive.drive_items").await;
        assert_eq!(column_values(&batches, "id"), vec!["i-1"]);

        let inputs = execute_inputs(&gateway, "one_drive.list_folder_children");
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0]["folderItemId"], "01FOLDERSCOPED");
        assert_eq!(
            input_keys(&inputs[0]),
            vec!["folderItemId", "top"],
            "unbound optional resources are withheld, not nulled: {}",
            inputs[0]
        );
    }

    #[tokio::test]
    async fn predicates_stay_local_against_a_provider_that_cannot_narrow() {
        // Neither action exposes a filter input at all, so the pack maps
        // no filters and every predicate runs in DataFusion after the
        // bounded fetch. The wire request must be identical to an
        // unfiltered one — in particular no `filter`/`orderBy` key
        // invented from the SQL.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return one_drive_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/one_drive.list_folder_children" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"items": [item_row("keep"), item_row("drop")], "nextLink": null})
                        .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_ONEDRIVE_FILTER", "drive_items", "").await;

        let batches = collect(
            &ctx,
            "SELECT id FROM saas.drive.drive_items WHERE id = 'keep'",
        )
        .await;
        assert_eq!(column_values(&batches, "id"), vec!["keep"]);

        let inputs = execute_inputs(&gateway, "one_drive.list_folder_children");
        assert_eq!(inputs.len(), 1);
        assert_eq!(
            input_keys(&inputs[0]),
            vec!["top"],
            "a WHERE clause must not invent a wire input: {}",
            inputs[0]
        );
    }

    #[tokio::test]
    async fn limit_stops_cursor_pagination_early() {
        // LIMIT must stop the walk rather than drain the collection: one
        // page fetched, the cursor never followed.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return one_drive_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/one_drive.list_folder_children" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"items": [item_row("i-1"), item_row("i-2")],
                            "nextLink": CHILDREN_PAGE2_URI})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_ONEDRIVE_LIMIT", "drive_items", "").await;

        // Cardinality, not identity, is the assertion here on purpose:
        // WHICH row a LIMIT keeps without an ORDER BY is not something SQL
        // promises, while "one row, one request" is exactly the claim.
        let batches = collect(&ctx, "SELECT id FROM saas.drive.drive_items LIMIT 1").await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let inputs = execute_inputs(&gateway, "one_drive.list_folder_children");
        assert_eq!(inputs.len(), 1, "LIMIT stops before following the cursor");
        assert_eq!(
            inputs[0]["top"], 999,
            "a full page still crosses the wire: {}",
            inputs[0]
        );
        assert_eq!(
            input_keys(&inputs[0]),
            vec!["top"],
            "the page size stays the declared ceiling — LIMIT narrows rows \
             locally, it is not pushed into `top`: {}",
            inputs[0]
        );
    }

    #[tokio::test]
    async fn scan_of_an_empty_drive_is_clean() {
        // An empty first page is a successful empty scan with the full
        // schema, not an error and not a truncation.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return one_drive_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/one_drive.list_folder_children" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"items": [], "nextLink": null}).to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_ONEDRIVE_EMPTY", "drive_items", "").await;

        // An empty result set may arrive as zero batches, so the row sum
        // (not an index) is the assertion — schema stability on an empty
        // page is pinned by `empty_page_keeps_schema_stable` instead.
        let batches = collect(&ctx, "SELECT id, name FROM saas.drive.drive_items").await;
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            0,
            "an empty collection is an empty result set, not an error"
        );
        assert_eq!(
            execute_inputs(&gateway, "one_drive.list_folder_children").len(),
            1,
            "an empty page still costs exactly one request"
        );
    }

    #[tokio::test]
    async fn a_repeated_cursor_fails_as_a_pagination_loop() {
        // A gateway that hands back the cursor it was given must fail
        // loudly instead of spinning: incomplete is never success.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return one_drive_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/one_drive.list_folder_children" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"items": [item_row("i-1")], "nextLink": CHILDREN_PAGE2_URI})
                        .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_ONEDRIVE_LOOP", "drive_items", "").await;

        let err = ctx
            .sql("SELECT id FROM saas.drive.drive_items")
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("a repeated cursor must fail the scan");
        let message = err.to_string();
        assert!(
            message.contains("pagination loop") && message.contains(CHILDREN_PAGE2_URI),
            "the loop names the cursor the gateway would not advance: {message}"
        );
    }

    #[tokio::test]
    async fn provider_errors_surface_through_the_gateway_failure_envelope() {
        // Graph's in-band errors are consumed by the executor, so the
        // pack sees them as a failure envelope — including the scope
        // failure a read-only pack hits when the OAuth grant is missing
        // (exactly what phase 4 is still blocked on).
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return one_drive_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/one_drive.list_folder_children" {
                return MockResponse::new(
                    403,
                    envelope_err(
                        "authorization_failed",
                        "Connect one_drive with OAuth first.",
                    ),
                );
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_ONEDRIVE_AUTHZ", "drive_items", "").await;

        let err = ctx
            .sql("SELECT id FROM saas.drive.drive_items")
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("a failure envelope must fail the scan");
        let message = err.to_string();
        assert!(
            message.contains("authorization_failed")
                && message.contains("one_drive.list_folder_children"),
            "the gateway's error code and the action are named: {message}"
        );
        assert!(
            !message.contains("row path"),
            "never the misleading row-path error: {message}"
        );
    }

    #[tokio::test]
    async fn udtf_parity_for_drive_items() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return one_drive_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/one_drive.list_folder_children" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"items": [item_row("i-1")], "nextLink": null}).to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_ONEDRIVE_UDTF", "drive_items", "").await;

        let from_table = collect(
            &ctx,
            "SELECT id, name, file_mime_type FROM saas.drive.drive_items",
        )
        .await;
        let from_udtf = collect(
            &ctx,
            "SELECT id, name, file_mime_type \
             FROM open_connector_query('saas', 'one_drive.drive_items', '{}')",
        )
        .await;
        assert_eq!(from_table[0].schema(), from_udtf[0].schema());
        assert_eq!(
            arrow::util::pretty::pretty_format_batches(&from_table)
                .unwrap()
                .to_string(),
            arrow::util::pretty::pretty_format_batches(&from_udtf)
                .unwrap()
                .to_string()
        );
    }

    #[tokio::test]
    async fn drifted_contract_fails_registration_not_the_scan() {
        // The pin's refusal side: a gateway whose discovered output
        // schema differs from the captured contract is refused at
        // REGISTRATION, table and action named. (Every other e2e proves
        // the pass side via one_drive_discovery's captured contracts.)
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
        let _token = EnvVarGuard::set("SKARDI_TEST_OC_ONEDRIVE_DRIFT", "test-token");
        let mut ctx = SessionContext::new();
        let gateways = OpenConnectorGateways::default();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&one_drive_config(
                "SKARDI_TEST_OC_ONEDRIVE_DRIFT",
                "drive_items",
                "",
            )),
            false,
            HierarchyLevel::Catalog,
            Some(&gateways),
        )
        .await
        .expect_err("a drifted contract must fail registration");
        let message = err.to_string();
        assert!(
            message.contains("one_drive.drive_items")
                && message.contains("one_drive.list_folder_children")
                && message.contains("fingerprint mismatch"),
            "table, action, and cause are named: {message}"
        );
    }
}
