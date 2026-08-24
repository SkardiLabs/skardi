//! OneDrive (Microsoft 365) source pack: `drive_items` and
//! `drive_item_search` over Open Connector's `one_drive` service,
//! reconciled against a live gateway (v1.3.4, open-connector at
//! `2410fbe`). Design record:
//! `docs/superpowers/specs/2026-08-19-open-connector-one-drive-pack-design.md`;
//! user documentation: `docs/open-connector-one-drive.md`.
//!
//! **Status: phases 1–4 complete.** The contract reconciliation is live
//! (action inventory, both discovery schemas byte-identical to the
//! committed captures, the whole input surface, the `top` bounds —
//! probed 2026-08-19), and the live-data pass ran on 2026-08-21 against
//! a real personal (MSA) drive through both raw probes and end-to-end
//! skardi-server scans. What it settled: `top: 999` is a wire bound as
//! well as a declared one (a real 999 request answers a full 200 page);
//! the real terminal page returns a genuinely null `nextLink`; a real
//! children cursor round-trips the executor's host/path allowlist in
//! all three path forms (root, `folderItemId`, `driveId`); every
//! `drive_items` column except `description` carried a real non-NULL
//! value somewhere (2800+ rows never carried `description` — kept, with
//! the caveat recorded in the yaml); and the fixtures below are now
//! REDACTED LIVE CAPTURES. Two findings changed the pack: search rows
//! are a reduced Substrate projection that never carries
//! `eTag`/`cTag`/`isAuthoritative`, so `drive_item_search` dropped
//! those two columns (16 → 14); and following a search continuation
//! cursor fails SERVER-SIDE on a personal drive ("Error Calling
//! Substrate Search", deterministic, cursor forwarded byte-identically)
//! — a loud provider_error through the failure envelope, never a silent
//! truncation. Details in the yaml header and
//! `docs/open-connector-one-drive.md`.
//!
//! Design decisions and their rationale. Most are held by a named test
//! below; the two that are upstream properties no Skardi-side test can
//! hold — cursor non-interchangeability and the executor's own input
//! validation — say so where they appear, and were confirmed live in
//! phase 4:
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
//!   here: every mapped column (16 on `drive_items`, 14 on
//!   `drive_item_search`) resolves INSIDE the declared item schema, so
//!   the fingerprint gate covers every one of them and
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
//!   cursor constants below simply use their own action's path, and
//!   phase 4 confirmed it live in BOTH directions (400 `invalid_input`,
//!   "nextLink must target OneDrive search/children pagination
//!   endpoints"). The engine sends `top` on continuation
//!   requests too; the executors ignore it there because the cursor URL
//!   embeds its own `$top`.
//! - **`query` is a required resource on `drive_item_search`,** and the
//!   enforcement is not where it looks. The input schema's `required`
//!   array is EMPTY and `query` is merely `minLength: 1`, so an empty
//!   string 400s as `invalid_input` at the schema layer, while a
//!   MISSING query and a whitespace-only query both pass validation and
//!   die in the executor's own trim check (`ProviderRequestError(400,
//!   "query is required")`) — all three verified live. Declaring it
//!   required closes exactly one of the three: a binding with no
//!   `query` is refused at REGISTRATION. It does not close the other
//!   two, because resource validation is presence-and-non-null only, so
//!   `query: ""` and `query: "   "` register cleanly and fail at SCAN
//!   time on the upstream 400s. Loud either way, but config-time only
//!   for the missing case.
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
//!   keeps that queryable without a JSON column. The discriminator has
//!   one live-witnessed gap: a `remoteItem` stub (the Personal Vault
//!   row) carries NEITHER facet — and no `webUrl` — so both columns are
//!   NULL there. The seven other facets (`root`, `deleted`, `shared`,
//!   `specialFolder`, `remoteItem`, `searchResult`, `fileSystemInfo`)
//!   are declared as bare open objects and stay unmapped:
//!   presence-as-signal, not data, and any child path under them would
//!   be passthrough anyway. Real rows also carry undeclared passthrough
//!   extras the conversion must simply ignore (`isAuthoritative`,
//!   `@microsoft.graph.downloadUrl`, `file.hashes` on children rows;
//!   `commentSettings`, `image`, `photo` on search rows) — the redacted
//!   captures keep them so that stays exercised.

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
    use crate::sources::providers::open_connector::json_to_arrow::{FieldType, RowConverter};
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
    /// to `search_items` is rejected upstream and vice versa — phase 4
    /// confirmed both directions live (400 `invalid_input`, "nextLink
    /// must target OneDrive search/children pagination endpoints").
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

    // ── Contract tests. The row fixtures are REDACTED LIVE CAPTURES
    // (phase 4, 2026-08-21, personal MSA drive): every row mirrors one
    // real wire row key-for-key, with identities substituted
    // deterministically — cid → `0FAB1234CD567890` (and its lowercase /
    // leading-zero-stripped forms, preserving the real case asymmetry
    // between children and search rows), item GUIDs → per-row repeated
    // digits carried consistently through `id`/`eTag`/`cTag`/URLs,
    // display name → a placeholder that keeps the real CJK-ness, email
    // → `user@example.com`, tempauth → `v1e.SYNTHETIC.SYNTHETIC`.
    // Structural constants (`System Account`, `copilotUploads`,
    // `Microsoft Office for MSA`, mime types, `driveType`, view enums)
    // stay verbatim. `drive_items.json` is a composite of two captured
    // pages (the root listing plus one folder's children) so both the
    // folder shapes and the file shapes appear; `drive_item_search.json`
    // mirrors real search hits including the real cursor shape. CJK
    // travels as `\u` escapes so the files stay ASCII (see the audit).
    // `drive_items_type_mismatch.json` remains synthetic on purpose —
    // it encodes a contract VIOLATION no live capture can produce. ────

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
    fn drive_items_fixture_converts_every_captured_row_shape() {
        let batch = convert_fixture(
            table("drive_items"),
            include_str!("fixtures/one_drive/drive_items.json"),
        );
        assert_eq!(batch.num_rows(), 5);

        // Row 0 — the Copilot special folder: an EMPTY folder whose
        // `childCount: 0` must survive as 0, not collapse into NULL, or
        // "empty folder" becomes indistinguishable from "not a folder".
        // Its identitySet carries BOTH an application arm and a user arm;
        // only the user arm is mapped, and it wins.
        assert_eq!(
            utf8(&batch, "name").value(0),
            "Microsoft Copilot Chat 文件",
            "CJK survives the fixture's \\u escapes and the conversion"
        );
        assert_eq!(int64(&batch, "folder_child_count").value(0), 0);
        assert!(utf8(&batch, "file_mime_type").is_null(0));
        assert_eq!(
            utf8(&batch, "created_by_display_name").value(0),
            "示例 用户"
        );
        assert_eq!(utf8(&batch, "parent_path").value(0), "/drive/root:");
        assert_eq!(utf8(&batch, "parent_drive_id").value(0), "0FAB1234CD567890");
        assert!(utf8(&batch, "e_tag").value(0).contains("11111111"));
        assert!(utf8(&batch, "c_tag").value(0).starts_with("\"c:"));
        assert!(utf8(&batch, "web_url").is_valid(0));
        assert_eq!(int64(&batch, "size").value(0), 0, "size 0 is 0, not NULL");
        assert!(timestamp(&batch, "created_date_time").is_valid(0));
        assert!(timestamp(&batch, "last_modified_date_time").is_valid(0));

        // Row 1 — the Personal Vault `remoteItem` stub, the row that
        // makes three nullability claims real rather than defensive: it
        // carries NO `webUrl` and NEITHER type facet, so the facet
        // discriminator has a live-witnessed gap (a row that is neither
        // "file" nor "folder" by column test). Its identities are the
        // displayName-only "System Account" arm — no email, no id — and
        // the whole `remoteItem` facet is passthrough to ignore.
        assert_eq!(utf8(&batch, "name").value(1), "Personal Vault");
        assert!(utf8(&batch, "web_url").is_null(1));
        assert!(utf8(&batch, "file_mime_type").is_null(1));
        assert!(int64(&batch, "folder_child_count").is_null(1));
        assert_eq!(
            utf8(&batch, "created_by_display_name").value(1),
            "System Account"
        );
        assert!(utf8(&batch, "id").is_valid(1));

        // Every live row lacked `description` — 2800+ rows, zero
        // occurrences. The column stays mapped (declared in-schema, so
        // drift stays loud) and all-NULL is the EXPECTED live shape.
        for row in 0..batch.num_rows() {
            assert!(utf8(&batch, "description").is_null(row), "row {row}");
        }

        // Row 2 — an ordinal-id folder (`!103`, not a `!s<hex32>` id)
        // with a CJK name; a special folder still reads as an ordinary
        // folder row, `specialFolder` itself deliberately unmapped.
        assert_eq!(utf8(&batch, "name").value(2), "文档");
        assert_eq!(utf8(&batch, "id").value(2), "FAB1234CD567890!103");
        assert_eq!(int64(&batch, "folder_child_count").value(2), 21);

        // Row 3 — a fully-populated file: both concurrency tags, the
        // facet discriminator pointing the other way, a CJK parent path,
        // and the join key back to row 2's folder. The undeclared
        // passthrough extras (`@microsoft.graph.downloadUrl`,
        // `isAuthoritative`, `file.hashes`) must simply be ignored.
        assert_eq!(utf8(&batch, "name").value(3), "Document1.docx");
        assert!(utf8(&batch, "file_mime_type").value(3).contains("word"));
        assert!(int64(&batch, "folder_child_count").is_null(3));
        assert_eq!(int64(&batch, "size").value(3), 23348);
        assert!(utf8(&batch, "e_tag").value(3).contains("33333333"));
        assert!(utf8(&batch, "c_tag").value(3).starts_with("\"c:"));
        assert_eq!(utf8(&batch, "parent_path").value(3), "/drive/root:/文档");
        assert_eq!(
            utf8(&batch, "parent_id").value(3),
            utf8(&batch, "id").value(2),
            "a child row's parent_id joins back to its folder's id"
        );

        // Row 4 — a second file whose created-by and modified-by
        // APPLICATION arms differ (sync client vs Office); the mapped
        // user arm is the same person on both, which is exactly the
        // "who touched this" answer the yaml maps the user arm for.
        assert_eq!(utf8(&batch, "name").value(4), "工作簿1.xlsx");
        assert_eq!(
            utf8(&batch, "created_by_display_name").value(4),
            "示例 用户"
        );
        assert_eq!(
            utf8(&batch, "last_modified_by_display_name").value(4),
            "示例 用户"
        );
        assert!(
            utf8(&batch, "file_mime_type")
                .value(4)
                .contains("spreadsheet")
        );

        // Unmapped wire keys never become columns.
        for absent in ["special_folder", "remote_item", "is_authoritative"] {
            assert!(batch.column_by_name(absent).is_none(), "{absent}");
        }
    }

    #[test]
    fn wire_nulls_and_empty_strings_convert_without_failing_the_page() {
        // INLINE synthetic, deliberately, and the two halves are here for
        // different reasons. The live captures cannot carry a wire null
        // in a mapped position (2800+ real rows simply had none), and the
        // redaction audit's allowlists bar `""` from re-entering through
        // a fixture at all — but a fixture is also a claim about what the
        // wire looked like, and inventing rows inside one would make that
        // claim false. So the shapes the pre-live fixtures used to pin
        // live here instead.
        //
        // Both are distinct code paths from the ABSENT key every live row
        // exercises. A present null must reach SQL as NULL rather than
        // failing the page; the sharp one is the nested null
        // (`parentReference: null` under the mapped path
        // `$.parentReference.driveId`), which traverses THROUGH a null
        // instead of reading one. And `""` must survive as `""`, or an
        // empty display name and a missing one stop being distinguishable.
        let page = json!({
            "items": [
                {
                    "id": "FAB1234CD567890!900",
                    "name": null,
                    "webUrl": null,
                    "description": null,
                    "size": null,
                    "eTag": null,
                    "cTag": null,
                    "createdDateTime": null,
                    "lastModifiedDateTime": null,
                    "createdBy": { "user": null },
                    "lastModifiedBy": null,
                    "parentReference": null,
                    "file": { "mimeType": null },
                    "folder": null
                },
                {
                    "id": "FAB1234CD567890!901",
                    "name": "",
                    "webUrl": "",
                    "description": "",
                    "eTag": "",
                    "cTag": "",
                    "createdBy": { "user": { "displayName": "" } },
                    "lastModifiedBy": { "user": { "displayName": "" } },
                    "parentReference": { "driveId": "", "id": "", "path": "" },
                    "file": { "mimeType": "" }
                }
            ],
            "nextLink": null
        });
        let batch = convert_page(table("drive_items"), &page);
        assert_eq!(batch.num_rows(), 2);

        // Every nullable column, whatever its arrow type, and whether the
        // null is the leaf itself or an ancestor of it.
        const TEXT_COLUMNS: [&str; 11] = [
            "name",
            "web_url",
            "description",
            "e_tag",
            "c_tag",
            "created_by_display_name",
            "last_modified_by_display_name",
            "parent_drive_id",
            "parent_id",
            "parent_path",
            "file_mime_type",
        ];
        assert_eq!(utf8(&batch, "id").value(0), "FAB1234CD567890!900");
        for column in TEXT_COLUMNS {
            assert!(utf8(&batch, column).is_null(0), "{column} must be NULL");
        }
        assert!(int64(&batch, "size").is_null(0));
        assert!(int64(&batch, "folder_child_count").is_null(0));
        assert!(timestamp(&batch, "created_date_time").is_null(0));
        assert!(timestamp(&batch, "last_modified_date_time").is_null(0));

        // Empty is NOT null — at the leaf and through a nest alike.
        for column in TEXT_COLUMNS {
            let values = utf8(&batch, column);
            assert!(values.is_valid(1), "{column}: empty is not null");
            assert_eq!(values.value(1), "", "{column}");
        }
    }

    #[test]
    fn identity_arms_other_than_user_leave_the_display_name_null() {
        // Graph's identitySet has user/application/device arms and this
        // pack maps ONLY the user arm (see the yaml rationale). An
        // application-only identity is CONTRACT-LEGAL (the arms are all
        // optional) but was never witnessed on the live MSA drive —
        // every real children row carried a user arm — so this shape is
        // pinned with an inline synthetic page rather than smuggled into
        // the captured fixture. Both display-name columns must be NULL;
        // mapping the whole identitySet would have hidden the
        // distinction behind a JSON blob.
        let page = json!({
            "items": [{
                "id": "FAB1234CD567890!s55555555555555555555555555555555",
                "name": "sync-log.json",
                "size": 902,
                "createdBy": {
                    "application": {"id": "66666666-6666-6666-6666-666666666666",
                                    "displayName": "Synthetic Sync App"}
                },
                "lastModifiedBy": {
                    "application": {"id": "66666666-6666-6666-6666-666666666666",
                                    "displayName": "Synthetic Sync App"}
                },
                "file": {"mimeType": "application/json"}
            }],
            "nextLink": null
        });
        let batch = convert_page(table("drive_items"), &page);
        assert!(utf8(&batch, "created_by_display_name").is_null(0));
        assert!(utf8(&batch, "last_modified_by_display_name").is_null(0));
        // The row is otherwise intact — a null identity is not a broken row.
        assert_eq!(utf8(&batch, "file_mime_type").value(0), "application/json");
        assert_eq!(int64(&batch, "size").value(0), 902);
    }

    #[test]
    fn search_fixture_converts_and_carries_a_uri_cursor() {
        let t = table("drive_item_search");
        let fixture = include_str!("fixtures/one_drive/drive_item_search.json");
        let batch = convert_fixture(t, fixture);
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(
            batch.num_columns(),
            14,
            "no live search hit carried eTag/cTag, so the table maps 14 columns"
        );
        for dropped in ["e_tag", "c_tag"] {
            assert!(batch.column_by_name(dropped).is_none(), "{dropped}");
        }

        // Search spans folder levels — the point of the table, since
        // `list_folder_children` is non-recursive — and folders come
        // back discriminated the same way (row 0 is a folder hit).
        assert_eq!(int64(&batch, "folder_child_count").value(0), 4);
        assert!(utf8(&batch, "file_mime_type").is_null(0));
        assert_eq!(utf8(&batch, "file_mime_type").value(1), "image/png");
        assert!(int64(&batch, "folder_child_count").is_null(1));

        // The live wire mixes path spellings BETWEEN ROWS of one page:
        // row 0 came back with the CJK folder name verbatim, rows 1-2
        // percent-encoded. Both spellings pass through UNTOUCHED — the
        // converter must not normalize either way, so equality on the
        // same drive location is not guaranteed across rows.
        assert_eq!(utf8(&batch, "parent_path").value(0), "/drive/root:/文档");
        assert_eq!(
            utf8(&batch, "parent_path").value(1),
            "/drive/root:/%E6%A1%8C%E9%9D%A2"
        );

        // The join caveat, pinned from real rows: search rows spell
        // `parentReference.driveId` LOWERCASE WITHOUT the leading zero,
        // while children rows carry `0FAB…`-style — so a naive
        // drive_items ⋈ drive_item_search on parent_drive_id misses.
        assert_eq!(utf8(&batch, "parent_drive_id").value(0), "fab1234cd567890");
        assert_ne!(
            utf8(&batch, "parent_drive_id").value(0),
            "0FAB1234CD567890",
            "the two tables' driveId spellings differ on the real wire"
        );

        // Search identities are displayName-only (no email/id — a
        // reduced Substrate projection, like the missing tags).
        assert_eq!(
            utf8(&batch, "created_by_display_name").value(2),
            "示例 用户"
        );

        // `searchResult`, `commentSettings`, `image`, `photo` are on
        // the wire and deliberately unmapped.
        for absent in ["search_result", "comment_settings", "image", "photo"] {
            assert!(batch.column_by_name(absent).is_none(), "{absent}");
        }

        // The declared cursor path finds the real cursor shape: a
        // complete Graph URL that re-embeds the query and `$top`.
        let page: Value = serde_json::from_str(fixture).expect("fixture parses");
        let cursor = page["nextLink"].as_str().expect("cursor present");
        assert!(cursor.starts_with("https://graph.microsoft.com/"));
        assert!(cursor.contains("search(q=") && cursor.contains("$skiptoken="));
    }

    #[test]
    fn fixtures_are_redacted_captures_under_a_default_deny_audit() {
        // The row fixtures are REDACTED LIVE CAPTURES, and this audit is
        // the redaction's enforcement: every string leaf must satisfy an
        // allowlist FOR ITS KEY, default-deny, or the test fails. Key
        // scoping is what makes that enforce anything — a real drive
        // item's `name` is an ordinary filename and a real `webUrl` is an
        // ordinary https URL, so both would coast through a shape check.
        // The redaction scheme it pins: one synthetic cid
        // (`0FAB1234CD567890`, lowercase and leading-zero-stripped forms
        // included — the real wire's case asymmetry is data, see the
        // search conversion test), per-row repeated-digit GUIDs carried
        // consistently through id/eTag/cTag/URLs, placeholder identities,
        // and `v1e.SYNTHETIC` tempauth tokens. Structural constants
        // (product names, mime types, enums) stay verbatim. CJK values
        // travel as `\u` escapes so the files themselves stay ASCII and
        // the redaction stays auditable by eye.
        fn audit(name: &str, key: &str, value: &Value) {
            // A synthetic GUID is ONE repeated hex digit in canonical
            // 8-4-4-4-12 form. Checking the whole GUID matters: real
            // ordinal-row tags contain long zero RUNS (and embed the real
            // cid), so a "has a repeated window" check would wave real
            // tags through.
            fn repeated_digit_guid(s: &str) -> bool {
                let b = s.as_bytes();
                b.len() == 36
                    && [8, 13, 18, 23].iter().all(|&i| b[i] == b'-')
                    && b[0].is_ascii_hexdigit()
                    && b.iter()
                        .enumerate()
                        .all(|(i, c)| *c == b'-' || (*c == b[0] && ![8, 13, 18, 23].contains(&i)))
            }
            // A personal-drive driveItem id: `<cid>!<ordinal>` or
            // `<cid>!s<hex32>`. The SUFFIX is the per-item half and varies
            // per row, so a prefix check alone admits exactly the
            // partial-redaction class the URL arms default-deny on
            // `resid`/`UniqueId`: the one global cid scrubbed, the per-row
            // identifier still real. Both suffix families are pinned WHOLE
            // — every committed id is an ordinal or a run of one repeated
            // hex digit, so this costs nothing and closes the shape.
            fn synthetic_item_id(s: &str) -> bool {
                let Some(rest) = s.strip_prefix("FAB1234CD567890!") else {
                    return false;
                };
                let b = rest.as_bytes();
                let ordinal = !b.is_empty() && b.iter().all(|c| c.is_ascii_digit());
                let session = b.len() == 33
                    && b[0] == b's'
                    && b[1].is_ascii_hexdigit()
                    && b[1..].iter().all(|c| *c == b[1]);
                ordinal || session
            }
            // eTag/cTag shape: `"{GUID},N"` / `"c:{GUID},N"` with a
            // synthetic GUID inside the braces.
            fn synthetic_tag(s: &str) -> bool {
                match (s.find('{'), s.find('}')) {
                    (Some(open), Some(close)) if open < close => {
                        repeated_digit_guid(&s[open + 1..close])
                    }
                    _ => false,
                }
            }
            const SYNTHETIC_CID: &str = "fab1234cd567890";
            // No shape can vouch for a name, so the set is explicit — a
            // real one fails. Shared by every place a name can travel:
            // the `name` key, a `path` segment, and a `webUrl` segment.
            // Product constants and OS-default folder/file names stay
            // verbatim; `copilotUploads`/`documents` arrive via
            // `specialFolder.name`.
            const REDACTED_NAMES: &[&str] = &[
                "Microsoft Copilot Chat 文件",
                "Personal Vault",
                "Documents",
                "文档",
                "桌面",
                "Document1.docx",
                "工作簿1.xlsx",
                "WeChat Files",
                "图片1.png",
                "示例报告.docx",
                "copilotUploads",
                "documents",
                "wrong-types.bin",
            ];
            // Search rows percent-encode CJK path segments where children
            // rows send them literally, so one allowlist covers both
            // spellings only after decoding. Invalid UTF-8 decodes to the
            // empty string, which is on no allowlist — default-deny holds.
            fn percent_decode(seg: &str) -> String {
                let b = seg.as_bytes();
                let mut out = Vec::with_capacity(b.len());
                let mut i = 0;
                while i < b.len() {
                    match (b[i], b.get(i + 1), b.get(i + 2)) {
                        (b'%', Some(hi), Some(lo)) => {
                            match (char::from(*hi).to_digit(16), char::from(*lo).to_digit(16)) {
                                (Some(hi), Some(lo)) => {
                                    out.push((hi * 16 + lo) as u8);
                                    i += 3;
                                }
                                _ => {
                                    out.push(b[i]);
                                    i += 1;
                                }
                            }
                        }
                        _ => {
                            out.push(b[i]);
                            i += 1;
                        }
                    }
                }
                String::from_utf8(out).unwrap_or_default()
            }
            // A URL is only as redacted as its PARTS. The host and the cid
            // are what the arms below pin, but the per-item ids and the
            // human-readable segments ride in the same string and vary per
            // ROW — which is exactly what a partial re-redaction leaves
            // behind (scrub the one global cid, miss the per-row `resid`).
            // So every path segment and every query value is default-deny
            // here too, on the same doctrine as the keys.
            fn url_parts_are_redacted(s: &str) -> bool {
                // Route, not identity: these carry no per-tenant data.
                const STRUCTURAL_SEGMENTS: &[&str] = &[
                    "personal",
                    "user",
                    "_layouts",
                    "15",
                    "doc.aspx",
                    "download.aspx",
                ];
                let (path, query) = s.split_once('?').unwrap_or((s, ""));
                let path_ok = path
                    .split('/')
                    // scheme, the empty authority slot, host — the arms
                    // below pin the host itself.
                    .skip(3)
                    .filter(|seg| !seg.is_empty())
                    .all(|seg| {
                        let seg = percent_decode(seg);
                        STRUCTURAL_SEGMENTS.contains(&seg.as_str())
                            || REDACTED_NAMES.contains(&seg.as_str())
                            || seg
                                .trim_start_matches('0')
                                .eq_ignore_ascii_case(SYNTHETIC_CID)
                    });
                let query_ok = query
                    .split('&')
                    .filter(|pair| !pair.is_empty())
                    .all(|pair| match pair.split_once('=') {
                        Some(("cid", v)) => v
                            .trim_start_matches('0')
                            .eq_ignore_ascii_case(SYNTHETIC_CID),
                        // The two live id families, same as the `id` arm.
                        Some(("id", v)) => synthetic_item_id(v) || repeated_digit_guid(v),
                        Some(("resid" | "UniqueId", v)) => repeated_digit_guid(v),
                        Some(("tempauth", v)) => v.starts_with("v1e.SYNTHETIC"),
                        // Non-identifying request knobs Graph appends.
                        Some(("Translate", v)) => v == "false",
                        Some(("ApiVersion", v)) => v == "2.0",
                        _ => false,
                    });
                path_ok && query_ok
            }
            match value {
                Value::String(s) => {
                    let allowed = match key {
                        // One key, several id families: driveItem ids
                        // (`FAB…!103`, `FAB…!s<hex32>`), the user id (the
                        // cid), application ids (repeated-digit GUIDs plus
                        // Office's well-known first-party constant), the
                        // remoteItem's business-shaped id, and the
                        // type-mismatch fixture's marked synthetic ids.
                        "id" => {
                            synthetic_item_id(s)
                                || s == "0FAB1234CD567890"
                                || repeated_digit_guid(s)
                                || s == "00000000-0000-0000-0000-0000480728c5"
                                || (s.len() == 34
                                    && s.starts_with("01")
                                    && s[2..].bytes().all(|b| b == b'A'))
                                || s.starts_with("01SYNTHETIC")
                        }
                        "name" => REDACTED_NAMES.contains(&s.as_str()),
                        // The places a real cid or tenant would hide: known
                        // host shapes AND the synthetic cid in the URL —
                        // plus every remaining part, because the host and
                        // the cid being right says nothing about the
                        // per-row `resid` or a filename segment.
                        "webUrl" => {
                            (((s.starts_with("https://onedrive.live.com")
                                || s.starts_with(
                                    "https://my.microsoftpersonalcontent.com/personal/",
                                ))
                                && s.to_ascii_lowercase().contains(SYNTHETIC_CID))
                                || s.starts_with("https://example-my.sharepoint.com/personal/user/"))
                                && url_parts_are_redacted(s)
                        }
                        "@microsoft.graph.downloadUrl" => {
                            s.starts_with(
                                "https://my.microsoftpersonalcontent.com/personal/0fab1234cd567890/_layouts/15/download.aspx",
                            ) && s.contains("tempauth=v1e.SYNTHETIC")
                                && url_parts_are_redacted(s)
                        }
                        "siteUrl" => {
                            s == "https://my.microsoftpersonalcontent.com/personal/0fab1234cd567890"
                        }
                        "displayName" => [
                            "示例 用户",
                            "System Account",
                            "M365Chat",
                            "Microsoft Office for MSA",
                            "Microsoft OneDrive desktop sync client",
                        ]
                        .contains(&s.as_str()),
                        "email" => s == "user@example.com",
                        "driveId" => {
                            s == "0FAB1234CD567890"
                                || s == SYNTHETIC_CID
                                || s.starts_with("b!Synthetic")
                        }
                        // Ancestor folder names live ONLY here — a nested
                        // path is the sole carrier of its intermediate
                        // segments, so nothing else in the row would catch
                        // one. A prefix check is the shape check this
                        // audit exists to reject, so the segments get the
                        // same explicit treatment as `name`.
                        "path" => s.strip_prefix("/drive/root:").is_some_and(|rest| {
                            rest.split('/')
                                .filter(|seg| !seg.is_empty())
                                .all(|seg| {
                                    REDACTED_NAMES.contains(&percent_decode(seg).as_str())
                                })
                        }),
                        "eTag" | "cTag" => synthetic_tag(s),
                        "siteId" | "listId" | "listItemUniqueId" | "webId" => {
                            repeated_digit_guid(s)
                        }
                        // The captured cursor, byte-exact: URI-shaped like
                        // the real wire but with the trivial first
                        // skiptoken (`Mg`), never a real continuation.
                        "nextLink" => {
                            s == "https://graph.microsoft.com/v1.0/me/drive/root/search(q='docx')?$top=999&$skiptoken=Mg"
                        }
                        "quickXorHash" => s.starts_with("SyntheticQuickXorHash"),
                        "sha1Hash" => s.len() == 40 && s.bytes().all(|b| b == s.as_bytes()[0]),
                        "sha256Hash" => s.len() == 64 && s.bytes().all(|b| b == s.as_bytes()[0]),
                        // Public platform constants and enums.
                        "mimeType" => s.contains('/'),
                        "driveType" => s == "personal",
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

        // ENUMERATED, not listed. A hand-written roster audits whatever
        // someone remembered to add to it, and this repo has already paid
        // for that once — `loader.rs`'s builtin-asset test records
        // discord.yaml shipping unlisted for a full milestone. Reading the
        // directory means a fixture added later is audited by existing
        // code, or fails loudly here.
        let dir = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/sources/providers/open_connector/packs/fixtures/one_drive"
        );
        // `contracts/` holds declared JSON Schemas, not captured rows —
        // the only subtree outside this audit, asserted rather than
        // assumed so a future data-bearing subdirectory cannot hide.
        let mut audited = Vec::new();
        for entry in std::fs::read_dir(dir).expect("the fixtures directory exists") {
            let entry = entry.expect("readable directory entry");
            let name = entry.file_name().to_string_lossy().into_owned();
            if entry.file_type().expect("file type").is_dir() {
                assert_eq!(
                    name, "contracts",
                    "an un-audited subdirectory appeared under fixtures/one_drive"
                );
                continue;
            }
            let fixture = std::fs::read_to_string(entry.path()).expect("fixture is readable");
            assert!(
                fixture.is_ascii(),
                "{name}: fixtures stay ASCII so redaction is auditable by eye \
                 (CJK travels as \\u escapes)"
            );
            let root: Value = serde_json::from_str(&fixture).expect("fixture parses");
            audit(&name, "$", &root);
            audited.push(name);
        }
        // Without this the whole audit passes vacuously on a bad path.
        audited.sort();
        assert_eq!(
            audited,
            [
                "drive_item_search.json",
                "drive_items.json",
                "drive_items_type_mismatch.json"
            ],
            "every committed row fixture must be audited"
        );

        // The tripwire must TRIP. Each probe is a leak class the real
        // captures actually contained before redaction.
        for (key, leak) in [
            // The real cid in a URL on an allowed host.
            (
                "webUrl",
                "https://onedrive.live.com?cid=0BADC0FFEE123456&id=BADC0FFEE123456!103",
            ),
            ("displayName", "Real Person"), // identity off the list
            ("name", "Acme Q3 headcount.xlsx"), // a real filename shape
            ("id", "BADC0FFEE123456!103"),  // a cid-prefixed id off the synthetic cid
            // A real ordinal-row tag: its zero RUNS would pass a naive
            // repeated-window check, and its head embeds the real cid.
            ("eTag", "\"{AB12CD34-28DB-2034-800D-680000000000},3\""),
            // URI-shaped, right host, but a real continuation token.
            (
                "nextLink",
                "https://graph.microsoft.com/v1.0/me/drive/root/search(q='docx')?$top=999&$skiptoken=UkVBTA",
            ),
            // A real tempauth bearer token in a download URL.
            (
                "@microsoft.graph.downloadUrl",
                "https://my.microsoftpersonalcontent.com/personal/0fab1234cd567890/_layouts/15/download.aspx?tempauth=v1e.eyJGYWtlIjoxfQ.real",
            ),
            ("email", "1234567890@example.net"), // a real-shaped address
            ("ownerEmail", "person@example.com"), // a key the allowlist never saw
            // PARTIAL redaction — the class every probe above misses,
            // because each of those is wrong all the way through. These
            // are correct exactly where the old arms looked and real
            // everywhere else, which is what a re-capture actually
            // produces: the one global cid gets scrubbed, the per-row
            // identifiers and the folder names do not.
            //
            // An ancestor folder name, reachable through no other key.
            ("path", "/drive/root:/Acme Merger"),
            // Right host, synthetic cid — real per-row `resid`.
            (
                "webUrl",
                "https://onedrive.live.com/personal/0fab1234cd567890/_layouts/15/doc.aspx?resid=AB12CD34-28DB-2034-800D-680000000000&cid=0fab1234cd567890",
            ),
            // Right host, synthetic cid — real filename in the path.
            (
                "webUrl",
                "https://my.microsoftpersonalcontent.com/personal/0fab1234cd567890/Documents/Acme Q3 headcount.xlsx",
            ),
            // Synthetic tempauth — real `UniqueId` beside it.
            (
                "@microsoft.graph.downloadUrl",
                "https://my.microsoftpersonalcontent.com/personal/0fab1234cd567890/_layouts/15/download.aspx?UniqueId=AB12CD34-28DB-2034-800D-680000000000&Translate=false&tempauth=v1e.SYNTHETIC.SYNTHETIC&ApiVersion=2.0",
            ),
            // Synthetic cid — real per-item suffix. The `id` key carries
            // the same per-row half as `resid`, so a prefix-only check
            // would wave through the one leak shape this whole group is
            // about. Both places the family travels get a probe.
            ("id", "FAB1234CD567890!s1a2b3c4d5e6f708192a3b4c5d6e7f809"),
            (
                "webUrl",
                "https://onedrive.live.com/?cid=0fab1234cd567890&id=FAB1234CD567890!s1a2b3c4d5e6f708192a3b4c5d6e7f809",
            ),
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
        // An empty drive folder must still produce the full declared
        // schema, or a first-page-empty scan would change shape. The two
        // tables' widths differ on purpose: no live search hit carried
        // the concurrency tags (phase 4), so drive_item_search maps 14.
        for (short, columns) in [("drive_items", 16), ("drive_item_search", 14)] {
            let t = table(short);
            let batch = convert_page(t, &json!({"items": [], "nextLink": null}));
            assert_eq!(batch.num_rows(), 0, "{short}");
            assert_eq!(
                batch.num_columns(),
                columns,
                "{short} keeps its declared schema"
            );
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
    fn search_columns_are_drive_items_minus_the_two_concurrency_tags() {
        // The yaml derives `drive_item_search`'s column list from
        // `drive_items`' in prose ("MINUS the two concurrency tags"), and
        // phase 4 made that literally true: same names, paths, types,
        // nullability, same order. Prose cannot hold it. The width pins in
        // `empty_page_keeps_schema_stable` catch an added or dropped
        // column and the coverage gate above catches a path that leaves
        // the declared schema, but a retype, a rename, a re-path to
        // another declared key, or a reorder applied to ONE table slips
        // past both and quietly turns that comment into a lie. Pin the
        // pairwise relation itself, so any such edit must either land on
        // both tables or consciously rewrite this test — and with it the
        // claim it guards.
        fn shape(t: &SourcePackTable) -> Vec<(&'static str, &'static str, FieldType, bool)> {
            t.fields
                .iter()
                .map(|f| (f.name, f.path, f.field_type, f.nullable))
                .collect()
        }
        const DROPPED: [&str; 2] = ["e_tag", "c_tag"];
        let expected: Vec<_> = shape(table("drive_items"))
            .into_iter()
            .filter(|(name, ..)| !DROPPED.contains(name))
            .collect();
        assert_eq!(
            shape(table("drive_item_search")),
            expected,
            "drive_item_search must be drive_items minus exactly {DROPPED:?}"
        );
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
                    // The declared ceiling; 1000 and 0 both 400 live, and
                    // phase 4 confirmed 999 is a WIRE bound too — a real
                    // top=999 request answered a full 200 page (feishu
                    // declared 100 and hard-failed above 50).
                    assert_eq!(page_size, 999, "{short}");
                    // Termination is the cursor going null — Graph has no
                    // has-more flag, so the feishu-style override that
                    // rescues a non-empty terminal cursor is not declared.
                    // Phase 4 confirmed real terminal pages return an
                    // explicit null on both actions.
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
    fn the_two_folder_scopes_are_declared_as_alternatives() {
        // `folderItemId` and `folderPath` each name one folder, so both are
        // optional — but upstream resolves a binding carrying both by
        // precedence (id wins), which would scan a folder the operator did
        // not name and leave the path as dead configuration. Declaring the
        // group is what turns that into a registration failure; without it
        // the pack inherits a silent precedence. `driveId` is NOT in the
        // group: it selects the drive, and composes with either scope.
        let t = table("drive_items");
        let groups: Vec<Vec<&str>> = t
            .exclusive_resources
            .iter()
            .map(|group| group.to_vec())
            .collect();
        assert_eq!(groups, vec![vec!["folderItemId", "folderPath"]]);
        assert_eq!(
            t.conflicting_resources(|key| ["folderItemId", "folderPath", "driveId"].contains(&key)),
            Some(("folderItemId", "folderPath"))
        );
        for scope in ["folderItemId", "folderPath"] {
            assert_eq!(
                t.conflicting_resources(|key| key == scope || key == "driveId"),
                None,
                "{scope} alongside driveId is unambiguous"
            );
        }
        // The search table takes no folder scope at all, so it must not
        // acquire the group by copy-paste.
        assert!(table("drive_item_search").exclusive_resources.is_empty());
    }

    #[test]
    fn query_is_required_on_search_and_nothing_is_required_on_children() {
        // `drive_items` is well-defined with no resource at all (the
        // executor lists the drive root's children), so every resource
        // is optional. `drive_item_search` cannot be: there is no
        // spelling of "search everything", and a missing query dies in
        // the executor's own trim check rather than as `invalid_input`
        // — declaring it required is what stops Skardi ever generating
        // that call. It stops only that one: a whitespace-only value
        // passes resource validation (presence and non-null only) and
        // fails at scan time instead
        // (`a_search_binding_without_a_query_fails_before_any_http`
        // covers the closed arm).
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
        // the executor's own trim check). Declaring it a required
        // RESOURCE stops Skardi generating the MISSING one; the
        // whitespace-only one still reaches the wire, because resource
        // validation checks presence and non-null, not emptiness.
        let search: Value = serde_json::from_str(include_str!(
            "fixtures/one_drive/contracts/inputs/search_items.json"
        ))
        .expect("input contract fixture parses");
        assert_eq!(search["properties"]["query"]["minLength"], json!(1));
        // This pins an upstream DEFECT, so read a failure here the right
        // way round: if a re-captured contract grows `required:
        // ["query"]`, upstream has FIXED the gap and this assertion is
        // what should change — the resource-level requirement below can
        // then lean on schema validation instead of standing alone.
        assert!(
            search["required"].is_null(),
            "upstream now declares a `required` array ({}) — if it contains \
             `query`, the executor-layer gap this pack works around is fixed; \
             update this assertion and the `query` rationale in the yaml",
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
        // spelling this action really emits. The cursor mechanics here
        // are engine-real even though live MSA search continuations
        // currently fail server-side upstream (see the yaml header) —
        // queries whose hits fit one page terminate on a clean null,
        // which is the live-witnessed passing path. Both captured output
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
    async fn a_binding_naming_both_folder_scopes_fails_before_any_http() {
        // The failing arm of the alternatives declaration. Upstream this
        // binding is legal and returns rows — from the id's folder, with
        // the operator's path ignored — so this is the one place the pack
        // is STRICTER than the gateway, deliberately: a successful scan of
        // an unnamed scope is worse than a refusal. Refused before
        // discovery, so the gateway sees only the health probe.
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
        let _token = EnvVarGuard::set("SKARDI_TEST_OC_ONEDRIVE_BOTH_SCOPES", "test-token");
        let mut ctx = SessionContext::new();
        let gateways = OpenConnectorGateways::default();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&one_drive_config(
                "SKARDI_TEST_OC_ONEDRIVE_BOTH_SCOPES",
                "drive_items",
                "{ folderItemId: \"FAB1234CD567890!103\", folderPath: \"/Documents\" }",
            )),
            false,
            HierarchyLevel::Catalog,
            Some(&gateways),
        )
        .await
        .expect_err("a binding setting both folder scopes must fail registration");
        let text = err.to_string();
        for key in ["folderItemId", "folderPath", "drive_items"] {
            assert!(text.contains(key), "the error names {key}: {text}");
        }
        assert!(
            gateway.requests().iter().all(|r| r.path == "/v1/health"),
            "the ambiguity is caught before discovery"
        );
    }

    #[tokio::test]
    async fn a_binding_naming_one_folder_scope_registers() {
        // The passing arm: the group must not make a legitimate single
        // scope unusable. `driveId` alongside it stays legal — it selects
        // the drive, not the folder.
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
        let _token = EnvVarGuard::set("SKARDI_TEST_OC_ONEDRIVE_ONE_SCOPE", "test-token");
        let mut ctx = SessionContext::new();
        let gateways = OpenConnectorGateways::default();
        register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&one_drive_config(
                "SKARDI_TEST_OC_ONEDRIVE_ONE_SCOPE",
                "drive_items",
                "{ folderPath: \"/Documents\", driveId: \"0FAB1234CD567890\" }",
            )),
            false,
            HierarchyLevel::Catalog,
            Some(&gateways),
        )
        .await
        .expect("one folder scope plus a driveId is unambiguous");
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
        // pack sees them as a failure envelope — the same surface phase 4
        // witnessed live twice: this exact 403 before the OAuth grant
        // existed, and the server-side "Error Calling Substrate Search"
        // failure on real search continuations (loud, never a silent
        // truncation).
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
