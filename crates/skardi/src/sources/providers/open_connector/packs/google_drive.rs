//! Google Drive source pack: `files`, `drives` and `file_permissions`
//! over Open Connector's `googledrive` service, reconciled against a
//! live gateway (v1.3.4, open-connector at `2410fbe`, 2026-08-25).
//! Design record:
//! `docs/superpowers/specs/2026-08-25-open-connector-google-drive-pack-design.md`;
//! user documentation: `docs/open-connector-google-drive.md`.
//!
//! **Status: phases 1–4 complete (live-verified 2026-08-25).** Phase 1
//! reconciled the contract without provider credentials: action
//! inventory, all six schema captures byte-exact from discovery, the
//! input surfaces, the declared page-size bounds and the
//! undeclared-key 400s (the gateway validates inputs before authz).
//! Phase 4 then verified every load-bearing claim against a real
//! Workspace account through a seeded corpus (6 files, 1 shared drive,
//! 3 grants): the five `restrictions.*` spellings witnessed verbatim on
//! a real drive row; the pinned page sizes real WIRE bounds (1000/100/
//! 100 accepted; 1001/101/0 all 400); real multi-page walks at
//! `pageSize: 1` with ~444-char cursors and terminal-page `null` on
//! every table; the all-drives pin actually surfacing shared-drive
//! rows; registration passing the fingerprint gate against LIVE
//! discovery; a full three-table scan through skardi-server with LIMIT
//! stopping real pagination one page early; and the row fixtures
//! re-derived as redacted live captures under a default-deny audit.
//! Three columns remain live-unwitnessed non-null, each key PRESENT
//! (null) and correctly spelled on real rows, each structurally out of
//! reach: `drives.org_unit_id` (reports only under Workspace org
//! units), `drives.theme_id` (null even when a drive is CREATED with an
//! explicit theme — the theme materializes as color + background link),
//! and `file_permissions.expiration_time` (Google 403s expirations on
//! domain/anyone grants and a user-grant expiration needs a second real
//! account).
//!
//! Design decisions and their rationale. Each is held by a named test
//! below unless marked as an upstream property no Skardi-side test can
//! hold:
//!
//! - **The service is spelled `googledrive` and its action IDs carry
//!   TWO dots** (`googledrive.files.list` — the upstream action name is
//!   itself dotted). New to shipped packs, verified safe end to end in
//!   phase 1: `validate_action_id` rejects only `/` and empty dot
//!   segments, the URL path keeps dots verbatim, and the `rsplit('.')`
//!   short-name lookups read TABLE ids (`google_drive.files`), never
//!   action ids. Every e2e test below exercises the dotted id against
//!   the mock gateway, and the drift/error tests pin that it survives
//!   into error identity too.
//! - **Rows are NORMALIZED, the opposite of `one_drive`.** Every
//!   executor rebuilds rows through a `normalize*` function into a
//!   fixed key set, the declared row objects are `additionalProperties:
//!   false`, and each executor pins its own provider-side `fields`
//!   projection — `files.list` does not even declare a `fields` input
//!   (sending one 400s, probed live). So the wire keys are the
//!   normalizer's (`sizeBytes`, not Google's string-typed `size`), the
//!   declared contract is column truth to an unusual degree, and every
//!   mapped path except the five `restrictions.*` flags resolves inside
//!   the declared item schema
//!   (`only_the_five_restriction_flags_escape_the_fingerprint_gate`).
//! - **Two spellings of "no value", both modeled in the fixtures.** The
//!   `?? null` normalizer fields are always present (explicit JSON
//!   null); the conditionally-spread ones (`parents`/`owners`/`shared`/
//!   `starred`/`trashed` on files, `hidden`/`capabilities`/
//!   `restrictions` on drives, the three `optionalBoolean` flags on
//!   permissions) vanish from the row entirely. Both convert to SQL
//!   NULL; a present `false` stays `false`
//!   (`file_permissions_fixture_converts_every_grant_shape`). The live
//!   absence patterns are narrower than the declarations allow:
//!   shared-drive file rows drop exactly `owners`+`shared` (keeping
//!   `parents`/`starred`/`trashed`), non-user grants carry
//!   `allowFileDiscovery` while user grants carry
//!   `deleted`+`pendingOwner`, and every grant carries
//!   `permissionDetails` — the fixtures pin these as captured.
//! - **Three tables, scoped by the operator in phase 2.** `files` (the
//!   corpus), `drives` (shared-drive inventory — legitimately empty on
//!   an account with no shared-drive membership, design record R1), and
//!   `file_permissions` (per-file sharing audit). Design record R1
//!   resolved in phase 4: the account turned out to be Workspace and
//!   able to CREATE a shared drive, so the drives table was verified on
//!   real rows instead of deferred. Deferred at the
//!   admission gate, recorded in the design record: `changes.list`
//!   (its cursor must be bootstrapped by a second action,
//!   `changes.getStartPageToken` — a pagination shape the engine has no
//!   spelling for), plus the comments/replies/labels/accessproposals
//!   surfaces (out of the requested scope; accessproposals is also
//!   Workspace-approval-gated).
//! - **The all-drives pin is the pack's one load-bearing fixed input.**
//!   `files.list` forwards `supportsAllDrives` verbatim with NO default
//!   — unlike `drives.list`/`permissions.list`, whose executors run
//!   `resolveSupportsAllDrives` and default it TRUE — so an unpinned
//!   `files` scan would inherit Google's own default and silently omit
//!   every shared-drive file: the confidently-wrong-rows failure mode.
//!   Both halves of the pair are pinned `true` (Google requires them
//!   together). Phase 4 witnessed the pin doing its job: a seeded
//!   shared-drive file came back through a live skardi-server scan with
//!   `drive_id` carrying the real drive id.
//! - **`q` is an optional RESOURCE, not a filter mapping,
//!   structurally.** It is a whole query language (`name = 'x' and
//!   trashed = false`); a FilterMapping sends a column's literal as the
//!   input value, which is never a legal `q`. With no binding the scan
//!   is the complete corpus, so no capability is lost. Trashed files
//!   are deliberately IN (no `q: "trashed = false"` pin — it would
//!   collide with the resource and silently narrow the table);
//!   `trashed` is a column, so the filtering happens in SQL.
//! - **`fileId` is a required resource on `file_permissions`, and the
//!   enforcement boundary is narrow in exactly one_drive's `query`
//!   way:** upstream's `required` array is EMPTY and `fileId` is merely
//!   `minLength: 1`, so a missing fileId passes schema validation and
//!   dies in the executor's own `resolveFileId` check. Declaring the
//!   resource closes the missing case at registration; `""` dies at the
//!   schema layer (400 `invalid_input`, probed live); whitespace-only
//!   passes resource validation and fails at scan time. Loud all three
//!   ways. Rows carry no file identity (and a resource value has no
//!   path into a row), so the table means "the permissions of the file
//!   this binding names" — the `notion.block_children` shape. Upstream
//!   convenience worth knowing: `extractFileId` accepts a full Drive
//!   URL as the `fileId` value.
//! - **The five `restrictions.*` columns trade fingerprint coverage for
//!   audit value, knowingly.** `restrictions` is declared as a bare
//!   open object, so the five flags are outside the gate (an upstream
//!   inner-key rename would be silent — the reviewed case-1 gap, pinned
//!   by the coverage test). Phase 4 closed the spelling question the
//!   gate cannot: a real drive row carried all five documented keys
//!   verbatim (as present `false`s), plus a nested
//!   `downloadRestriction` object the pack leaves unmapped. Their
//!   per-caller
//!   sibling `capabilities` stays unmapped: its values change with the
//!   OAuth identity doing the scan, so the same table under two
//!   bindings would disagree.
//! - **In-band Google errors never reach the engine.** Every executor
//!   runs `assertGoogleResponse` and throws into the gateway's failure
//!   envelope, so no table declares `error_path`.
//! - **Both halves of each contract are committed** (`contracts/` and
//!   `contracts/inputs/`), gmail-style, because the fingerprint gate is
//!   output-only. Note these captures carry a `$schema` key — the
//!   sibling packs' do not — and the fingerprint hashes the WHOLE
//!   schema, so the captures keep it verbatim.

use std::sync::OnceLock;

use crate::sources::providers::open_connector::error::OpenConnectorError;
use crate::sources::providers::open_connector::source_pack::SourcePack;

use super::loader;

static PACK: OnceLock<Result<SourcePack, String>> = OnceLock::new();

/// The Google Drive pack, parsed once from the embedded YAML asset.
pub fn pack() -> Result<&'static SourcePack, OpenConnectorError> {
    loader::builtin(
        "google_drive.yaml",
        include_str!("google_drive.yaml"),
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
        EnvVarGuard, MockGateway, MockResponse, discovery_ok, envelope_err, envelope_ok,
        fingerprint_uncovered_columns,
    };
    use crate::sources::providers::open_connector::{
        OpenConnectorConfig, OpenConnectorGateways, register_open_connector_tables,
        register_open_connector_udtfs,
    };
    use arrow::array::{
        Array, BooleanArray, Int64Array, ListArray, StringArray, TimestampMillisecondArray,
    };
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use serde_json::{Value, json};

    /// Drive cursors are OPAQUE tokens (`pageToken`, `minLength: 1`) —
    /// unlike one_drive's URI-shaped `nextLink`, there is no format to
    /// round-trip and no per-action path allowlist to respect, so a
    /// realistic token shape is all a mock cursor owes.
    const FILES_PAGE2_TOKEN: &str = "~!!~AI9SyntheticFilesToken2";

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
    /// registration exercises the fingerprint gate's pass side. The
    /// dotted action ids are matched as full path suffixes — which is
    /// itself part of what these tests exercise: a two-dot action id
    /// travels the discovery URL verbatim.
    fn google_drive_discovery(path: &str) -> MockResponse {
        let output_schema = if path.ends_with("googledrive.files.list") {
            include_str!("fixtures/google_drive/contracts/files.list.json")
        } else if path.ends_with("googledrive.drives.list") {
            include_str!("fixtures/google_drive/contracts/drives.list.json")
        } else if path.ends_with("googledrive.permissions.list") {
            include_str!("fixtures/google_drive/contracts/permissions.list.json")
        } else {
            r#"{"type": "object"}"#
        };
        MockResponse::ok(&discovery_ok("{}", output_schema, true, None))
    }

    // ── Contract tests. The row fixtures are REDACTED LIVE CAPTURES
    // (phase 4, 2026-08-25), provider-shaped key-for-key: the
    // always-present `?? null` keys carry explicit nulls, the
    // conditionally-spread keys are absent exactly where upstream
    // dropped them on the wire, and every identity, name and URL is
    // synthetic — a property enforced by
    // `fixtures_are_redacted_captures_under_a_default_deny_audit`
    // rather than asserted by hand. The deliberately-broken
    // mismatch fixture stays synthetic by design, and says so. ──

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

    fn boolean<'a>(batch: &'a RecordBatch, name: &str) -> &'a BooleanArray {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("column {name}"))
            .as_any()
            .downcast_ref()
            .expect("Boolean column")
    }

    fn timestamp<'a>(batch: &'a RecordBatch, name: &str) -> &'a TimestampMillisecondArray {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("column {name}"))
            .as_any()
            .downcast_ref()
            .expect("Timestamp column")
    }

    fn utf8_list<'a>(batch: &'a RecordBatch, name: &str) -> &'a ListArray {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("column {name}"))
            .as_any()
            .downcast_ref()
            .expect("List column")
    }

    /// One row of a string-list column, item nullability included — the
    /// owner plucks need `Some`/`None` items, not just values.
    fn list_items(lists: &ListArray, row: usize) -> Vec<Option<String>> {
        let values = lists.value(row);
        let values = values
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 items");
        (0..values.len())
            .map(|i| values.is_valid(i).then(|| values.value(i).to_string()))
            .collect()
    }

    #[test]
    fn files_fixture_converts_every_row_shape() {
        // The fixture is a REDACTED LIVE CAPTURE (2026-08-25; the
        // redaction audit below is its enforcement): six rows spanning
        // every shape the seeded phase-4 corpus produced — a shared
        // My Drive file, a shared-drive file, a trashed file, a native
        // Google Doc, a starred CJK-named file and a folder.
        let batch = convert_fixture(
            table("files"),
            include_str!("fixtures/google_drive/files.json"),
        );
        assert_eq!(batch.num_rows(), 6);

        // Row 0 — a shared My Drive file, fully populated: real byte
        // size (the normalizer's `sizeBytes` NUMBER, not Google's
        // string `size`), one parent, one owner whose four keys all
        // carry values, all three boolean flags present. `driveId` is
        // the explicit-null spelling: present on the wire, null in SQL
        // (live, every My Drive row carries `driveId: null`).
        assert_eq!(utf8(&batch, "name").value(0), "notes.txt");
        assert_eq!(utf8(&batch, "mime_type").value(0), "text/plain");
        assert_eq!(int64(&batch, "size_bytes").value(0), 63);
        assert!(utf8(&batch, "drive_id").is_null(0));
        assert!(
            utf8(&batch, "web_view_link")
                .value(0)
                .starts_with("https://drive.google.com/file/d/")
        );
        assert!(timestamp(&batch, "created_time").is_valid(0));
        assert!(timestamp(&batch, "modified_time").is_valid(0));
        assert_eq!(
            list_items(utf8_list(&batch, "parents"), 0),
            vec![Some("1SyntheticFolderAAAAAAAAAAAAAAAAA".into())]
        );
        // The two columns plucked from ONE `owners` array.
        assert_eq!(
            list_items(utf8_list(&batch, "owner_display_names"), 0),
            vec![Some("Example User".into())]
        );
        assert_eq!(
            list_items(utf8_list(&batch, "owner_email_addresses"), 0),
            vec![Some("user@example.com".into())]
        );
        assert!(boolean(&batch, "shared").value(0));
        assert!(!boolean(&batch, "starred").value(0));
        assert!(!boolean(&batch, "trashed").value(0));

        // Row 1 — the shared-drive file: `driveId` non-null (the join
        // key to `drives.id`) and the drive itself as the parent. The
        // live absence pattern is NARROWER than phase 3 guessed:
        // shared-drive rows drop exactly `owners` and `shared` (the
        // drive owns its items) while `parents`/`starred`/`trashed`
        // stay present. Absent key and explicit null land in the same
        // SQL NULL.
        assert_eq!(utf8(&batch, "name").value(1), "shared-drive-doc.txt");
        assert_eq!(utf8(&batch, "drive_id").value(1), "0ASyntheticDrivePVA");
        assert_eq!(
            list_items(utf8_list(&batch, "parents"), 1),
            vec![Some("0ASyntheticDrivePVA".into())]
        );
        assert!(utf8_list(&batch, "owner_display_names").is_null(1));
        assert!(utf8_list(&batch, "owner_email_addresses").is_null(1));
        assert!(boolean(&batch, "shared").is_null(1));
        assert!(!boolean(&batch, "starred").value(1));
        assert!(!boolean(&batch, "trashed").value(1));

        // Row 2 — a trashed file, still a row: the yaml deliberately
        // does not pin `q: "trashed = false"`, so the live wire really
        // returns trash and the `trashed` column is how SQL filters it.
        assert_eq!(utf8(&batch, "name").value(2), "obsolete.txt");
        assert!(boolean(&batch, "trashed").value(2));

        // Row 3 — a native Google Doc. Live correction to a phase-3
        // guess: native docs DO report `sizeBytes` (Docs count against
        // storage quota since 2021); the null-size row is the FOLDER
        // below. The 44-char doc id (My Drive file ids are 33) and the
        // docs.google.com link are the real shapes.
        assert_eq!(utf8(&batch, "name").value(3), "Design memo");
        assert_eq!(
            utf8(&batch, "mime_type").value(3),
            "application/vnd.google-apps.document"
        );
        assert_eq!(int64(&batch, "size_bytes").value(3), 1024);
        assert_eq!(utf8(&batch, "id").value(3).len(), 44);
        assert!(
            utf8(&batch, "web_view_link")
                .value(3)
                .starts_with("https://docs.google.com/document/d/")
        );

        // Row 4 — CJK survives the capture, the redaction (`\u`
        // escapes keep the fixture ASCII) and the conversion; `starred`
        // carries a real true.
        assert_eq!(utf8(&batch, "name").value(4), "预算表.csv");
        assert!(boolean(&batch, "starred").value(4));

        // Row 5 — the folder: `sizeBytes` is the EXPLICIT wire null
        // (folders have no byte size of their own), and the mime type
        // is the de-facto type discriminator.
        assert_eq!(utf8(&batch, "name").value(5), "corpus-folder");
        assert!(utf8(&batch, "mime_type").value(5).ends_with("apps.folder"));
        assert!(int64(&batch, "size_bytes").is_null(5));

        // Unmapped wire keys never become columns: `owners` maps to two
        // pluck columns under its own names, and the per-owner
        // `permissionId`/`photoLink` stay unmapped. `normalizeDriveFile`
        // emits no `kind` at all — confirmed live on every row.
        for absent in ["owners", "kind", "permission_id", "photo_link"] {
            assert!(batch.column_by_name(absent).is_none(), "{absent}");
        }
    }

    #[test]
    fn drives_fixture_converts_including_the_restriction_flags() {
        // REDACTED LIVE CAPTURE of the one shared drive the phase-4
        // account created. It settles what phase 3 could not: the real
        // `restrictions` object carries all five documented flag
        // spellings verbatim (plus a nested `downloadRestriction`
        // object the pack leaves unmapped), so the documentation-derived
        // paths extract real values.
        let batch = convert_fixture(
            table("drives"),
            include_str!("fixtures/google_drive/drives.json"),
        );
        assert_eq!(batch.num_rows(), 1);

        assert_eq!(utf8(&batch, "id").value(0), "0ASyntheticDrivePVA");
        assert_eq!(utf8(&batch, "name").value(0), "example-shared-drive");
        assert!(!boolean(&batch, "hidden").value(0));
        assert_eq!(utf8(&batch, "color_rgb").value(0), "#e91e63");
        assert!(timestamp(&batch, "created_time").is_valid(0));
        // The two live residual columns, keys present and null:
        // `orgUnitId` reports only under Workspace org units, and
        // `themeId` came back null even when a drive was CREATED with
        // an explicit theme (the theme materializes as `colorRgb` +
        // `backgroundImageLink` instead; probed live).
        assert!(utf8(&batch, "org_unit_id").is_null(0));
        assert!(utf8(&batch, "theme_id").is_null(0));
        assert!(
            utf8(&batch, "background_image_link")
                .value(0)
                .starts_with("https://ssl.gstatic.com/")
        );
        // A fresh drive reports all five restriction flags PRESENT and
        // false — real extracted values, not NULLs. (Org policy blocked
        // flipping any to true on the phase-4 tenant — admin-only — but
        // a boolean's extraction path does not depend on which boolean
        // it carries.)
        for flag in [
            "admin_managed_restrictions",
            "copy_requires_writer_permission",
            "domain_users_only",
            "drive_members_only",
            "sharing_folders_requires_organizer_permission",
        ] {
            assert!(boolean(&batch, flag).is_valid(0), "{flag}");
            assert!(!boolean(&batch, flag).value(0), "{flag}");
        }

        // `kind` (constant), `capabilities` (a 21-flag per-caller view,
        // live) and the raw `restrictions` object are on the wire and
        // deliberately not columns.
        for absent in ["kind", "capabilities", "restrictions"] {
            assert!(batch.column_by_name(absent).is_none(), "{absent}");
        }

        // SYNTHETIC boundary page — shapes one live drive cannot show,
        // held at the engine level. `hidden`/`capabilities`/
        // `restrictions` are conditionally spread upstream, so a drive
        // may omit them entirely (all five flags NULL — "unreported",
        // never false), and a partially-populated `restrictions` object
        // must yield one present false among four NULL siblings.
        let page = json!({
            "drives": [
                {
                    "id": "0ASyntheticMinimlPVA",
                    "kind": "drive#drive",
                    "name": "minimal",
                    "colorRgb": null,
                    "createdTime": null,
                    "orgUnitId": null,
                    "themeId": null,
                    "backgroundImageLink": null
                },
                {
                    "id": "0ASyntheticPartilPVA",
                    "kind": "drive#drive",
                    "name": "partial",
                    "hidden": true,
                    "colorRgb": null,
                    "createdTime": null,
                    "orgUnitId": null,
                    "themeId": null,
                    "backgroundImageLink": null,
                    "restrictions": { "copyRequiresWriterPermission": false }
                }
            ],
            "nextPageToken": null
        });
        let batch = convert_page(table("drives"), &page);
        assert!(boolean(&batch, "hidden").is_null(0));
        for flag in [
            "admin_managed_restrictions",
            "copy_requires_writer_permission",
            "domain_users_only",
            "drive_members_only",
            "sharing_folders_requires_organizer_permission",
        ] {
            assert!(boolean(&batch, flag).is_null(0), "{flag}");
        }
        assert!(boolean(&batch, "hidden").value(1));
        assert!(
            boolean(&batch, "copy_requires_writer_permission").is_valid(1),
            "present false is false, not NULL"
        );
        assert!(!boolean(&batch, "copy_requires_writer_permission").value(1));
        for flag in [
            "admin_managed_restrictions",
            "domain_users_only",
            "drive_members_only",
            "sharing_folders_requires_organizer_permission",
        ] {
            assert!(boolean(&batch, flag).is_null(1), "{flag}");
        }
    }

    #[test]
    fn file_permissions_fixture_converts_every_grant_shape() {
        // REDACTED LIVE CAPTURE of the seeded corpus file's grants: the
        // `anyone` link-share, a `domain` grant, and the `user` owner.
        // Live correction to a phase-3 guess: EVERY grant carries
        // `permissionDetails` (not just shared-drive ones) — the
        // conversion must simply ignore it on all three rows.
        let batch = convert_fixture(
            table("file_permissions"),
            include_str!("fixtures/google_drive/file_permissions.json"),
        );
        assert_eq!(batch.num_rows(), 3);

        // Row 0 — the `anyone` grant, the row a sharing audit exists to
        // find: identity columns all null (nobody in particular holds
        // it) and `allowFileDiscovery: false` PRESENT — a link-only
        // share, distinguishable from "not reported" NULL only because
        // false survives as false. The two user-shaped booleans are
        // ABSENT on non-user grants → NULL.
        assert_eq!(utf8(&batch, "id").value(0), "anyoneWithLink");
        assert_eq!(utf8(&batch, "role").value(0), "reader");
        assert_eq!(utf8(&batch, "type").value(0), "anyone");
        assert!(utf8(&batch, "email_address").is_null(0));
        assert!(utf8(&batch, "display_name").is_null(0));
        assert!(utf8(&batch, "domain").is_null(0));
        assert!(utf8(&batch, "photo_link").is_null(0));
        assert!(boolean(&batch, "allow_file_discovery").is_valid(0));
        assert!(!boolean(&batch, "allow_file_discovery").value(0));
        assert!(boolean(&batch, "deleted").is_null(0));
        assert!(boolean(&batch, "pending_owner").is_null(0));

        // Row 1 — a `domain` grant: `domain` carries the meaning, the
        // display name is the org's (not a person's), and the grant is
        // search-discoverable — `allowFileDiscovery` carries both
        // values across these two rows.
        assert_eq!(utf8(&batch, "type").value(1), "domain");
        assert_eq!(utf8(&batch, "domain").value(1), "example.com");
        assert_eq!(utf8(&batch, "display_name").value(1), "Example Org");
        assert!(utf8(&batch, "email_address").is_null(1));
        assert!(boolean(&batch, "allow_file_discovery").value(1));

        // Row 2 — the `user` owner: identity columns populated, the two
        // reported booleans present-and-false, `allowFileDiscovery`
        // ABSENT (meaningless for user grants) → NULL.
        assert_eq!(utf8(&batch, "role").value(2), "owner");
        assert_eq!(utf8(&batch, "type").value(2), "user");
        assert_eq!(utf8(&batch, "email_address").value(2), "user@example.com");
        assert_eq!(utf8(&batch, "display_name").value(2), "Example User");
        assert!(utf8(&batch, "domain").is_null(2));
        assert!(utf8(&batch, "photo_link").is_valid(2));
        assert!(!boolean(&batch, "deleted").value(2));
        assert!(!boolean(&batch, "pending_owner").value(2));
        assert!(boolean(&batch, "allow_file_discovery").is_null(2));

        // `expirationTime` is null on every live row and CANNOT be
        // otherwise here: Google rejects expirations on domain/anyone
        // grants outright (probed live — 403, "Expiration dates cannot
        // be set on domain or anyone type permissions"), and setting
        // one on a user grant needs a second real account. So the
        // timestamp conversion is held by a SYNTHETIC user grant
        // carrying the field's documented RFC 3339 shape instead.
        for row in 0..3 {
            assert!(timestamp(&batch, "expiration_time").is_null(row));
        }
        let page = json!({
            "permissions": [{
                "id": "00000000000000000003",
                "kind": "drive#permission",
                "role": "reader",
                "type": "user",
                "domain": null,
                "deleted": false,
                "photoLink": null,
                "displayName": null,
                "emailAddress": "user@example.com",
                "pendingOwner": false,
                "expirationTime": "2026-12-31T23:59:59.000Z"
            }],
            "nextPageToken": null
        });
        let synthetic = convert_page(table("file_permissions"), &page);
        assert!(timestamp(&synthetic, "expiration_time").is_valid(0));

        // `kind` (constant) and `permissionDetails` (a second table's
        // worth of nested shape) are on the wire and deliberately not
        // columns.
        for absent in ["kind", "permission_details"] {
            assert!(batch.column_by_name(absent).is_none(), "{absent}");
        }
    }

    #[test]
    fn wire_nulls_and_empty_strings_convert_without_failing_the_page() {
        // The fixture rows model the wire the normalizers produce; this
        // inline page models the boundary shapes regardless of whether
        // today's upstream can produce them all — the engine's promise
        // is per-declaration, not per-normalizer-version. A null in any
        // nullable mapped position must reach SQL as NULL rather than
        // failing the page, and `""` must survive as `""` (upstream's
        // `String(x ?? "")` really produces it for id/name/mimeType).
        let page = json!({
            "files": [
                {
                    "id": "1SyntheticAllNull",
                    "name": "n",
                    "mimeType": "text/plain",
                    "webViewLink": null,
                    "createdTime": null,
                    "modifiedTime": null,
                    "sizeBytes": null,
                    "driveId": null
                },
                {
                    "id": "1SyntheticAllEmpty",
                    "name": "",
                    "mimeType": "",
                    "webViewLink": "",
                    "createdTime": null,
                    "modifiedTime": null,
                    "sizeBytes": 0,
                    "driveId": "",
                    "parents": [],
                    "owners": []
                },
                {
                    "id": "1SyntheticNullPluck",
                    "name": "n",
                    "mimeType": "text/plain",
                    "webViewLink": null,
                    "createdTime": null,
                    "modifiedTime": null,
                    "sizeBytes": null,
                    "driveId": null,
                    "owners": [
                        { "displayName": null, "emailAddress": "user@example.com" }
                    ]
                }
            ],
            "nextPageToken": null
        });
        let batch = convert_page(table("files"), &page);
        assert_eq!(batch.num_rows(), 3);

        for column in ["web_view_link", "drive_id"] {
            assert!(utf8(&batch, column).is_null(0), "{column} must be NULL");
        }
        assert!(int64(&batch, "size_bytes").is_null(0));
        assert!(timestamp(&batch, "created_time").is_null(0));
        assert!(timestamp(&batch, "modified_time").is_null(0));
        for column in ["parents", "owner_display_names", "owner_email_addresses"] {
            assert!(utf8_list(&batch, column).is_null(0), "{column}");
        }

        // Empty is NOT null, and size 0 is 0 — and an EMPTY
        // `parents`/`owners` array is an empty list, not NULL (an
        // orphaned file really has `parents: []`).
        for column in ["name", "mime_type", "web_view_link", "drive_id"] {
            let values = utf8(&batch, column);
            assert!(values.is_valid(1), "{column}: empty is not null");
            assert_eq!(values.value(1), "", "{column}");
        }
        assert_eq!(int64(&batch, "size_bytes").value(1), 0);
        for column in ["parents", "owner_display_names", "owner_email_addresses"] {
            assert!(utf8_list(&batch, column).is_valid(1), "{column}");
            assert_eq!(list_items(utf8_list(&batch, column), 1), vec![], "{column}");
        }

        // An owner object with a null field: the pluck yields a null
        // list ITEM — it neither fails the page nor drops the item, so
        // the two plucked columns stay index-aligned.
        assert_eq!(
            list_items(utf8_list(&batch, "owner_display_names"), 2),
            vec![None]
        );
        assert_eq!(
            list_items(utf8_list(&batch, "owner_email_addresses"), 2),
            vec![Some("user@example.com".into())]
        );
    }

    #[test]
    fn fixtures_are_redacted_captures_under_a_default_deny_audit() {
        // The row fixtures are REDACTED LIVE CAPTURES (2026-08-25), and
        // this audit is the redaction's enforcement: every string leaf
        // must satisfy an allowlist FOR ITS KEY, default-deny, or the
        // test fails. The scheme it pins: `1Synthetic…`/`0ASynthetic…`
        // ids with the live LENGTHS preserved (33- and 44-char file
        // ids, 19-char drive ids), all-zero-prefixed 20-digit
        // permission ids, placeholder identities under example.com, one
        // synthetic avatar URL, and Google URL shapes rebuilt around
        // the synthetic ids. Structural constants (kinds, mime types,
        // role/type enums, Google's own theme-asset URL) and timestamps
        // stay verbatim; CJK travels as `\u` escapes so the fixtures
        // stay ASCII and the redaction stays auditable by eye. The
        // deliberately-broken mismatch fixture is SYNTHETIC by design
        // (the admission gate wants it that way) and is audited under
        // the same arms.
        fn synthetic_id(s: &str) -> bool {
            s.starts_with("1Synthetic")
                || s.starts_with("0ASynthetic")
                || s == "anyoneWithLink"
                || (s.len() == 20 && s.starts_with("0000000000000000000"))
        }
        fn rfc3339(s: &str) -> bool {
            s.len() == 24
                && s.ends_with('Z')
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || b"-:T.Z".contains(&b))
        }
        // A URL is only as redacted as its parts: known Google prefix,
        // then a SYNTHETIC id, then a known structural tail — anything
        // else (a real id, a title-bearing slug, an extra query) fails.
        fn synthetic_url(s: &str) -> bool {
            for prefix in [
                "https://drive.google.com/file/d/",
                "https://drive.google.com/drive/folders/",
                "https://docs.google.com/document/d/",
            ] {
                if let Some(rest) = s.strip_prefix(prefix) {
                    let (id, tail) = rest.split_once('/').unwrap_or((rest, ""));
                    return synthetic_id(id)
                        && ["", "view", "view?usp=drivesdk", "edit?usp=drivesdk"].contains(&tail);
                }
            }
            false
        }
        // No shape can vouch for a name, so the set is explicit — a
        // real one fails. These are the names the phase-4 corpus was
        // SEEDED with (authored for the test, carrying no tenant data);
        // the CJK one keeps the capture's non-ASCII coverage honest.
        const REDACTED_NAMES: &[&str] = &[
            "notes.txt",
            "shared-drive-doc.txt",
            "obsolete.txt",
            "Design memo",
            "预算表.csv",
            "corpus-folder",
            "example-shared-drive",
            "wrong-types.bin",
        ];
        fn audit(fixture: &str, key: &str, value: &Value) {
            match value {
                Value::String(s) => {
                    let allowed = match key {
                        "id" | "driveId" | "parents" | "inheritedFrom" | "permissionId" => {
                            synthetic_id(s)
                        }
                        "name" => REDACTED_NAMES.contains(&s.as_str()),
                        "displayName" => s == "Example User" || s == "Example Org",
                        "emailAddress" => s == "user@example.com",
                        "domain" => s == "example.com",
                        "photoLink" => s == "https://lh3.googleusercontent.com/a/SYNTHETIC=s64",
                        "backgroundImageLink" => {
                            s == "https://ssl.gstatic.com/team_drive_themes/clams_background.jpg"
                        }
                        "webViewLink" => synthetic_url(s),
                        "mimeType" => [
                            "text/plain",
                            "text/csv",
                            "application/vnd.google-apps.document",
                            "application/vnd.google-apps.folder",
                            "application/octet-stream",
                        ]
                        .contains(&s.as_str()),
                        "createdTime" | "modifiedTime" | "expirationTime" => rfc3339(s),
                        "kind" => s == "drive#drive" || s == "drive#permission",
                        "role" => ["owner", "reader", "writer"].contains(&s.as_str()),
                        "type" | "permissionType" => {
                            ["anyone", "domain", "user", "file"].contains(&s.as_str())
                        }
                        "colorRgb" => {
                            s.len() == 7
                                && s.starts_with('#')
                                && s[1..].bytes().all(|b| b.is_ascii_hexdigit())
                        }
                        // The mismatch fixture's broken `sizeBytes` is a
                        // bare digit string — synthetic, carrying nothing.
                        "sizeBytes" => !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()),
                        _ => false,
                    };
                    assert!(
                        allowed,
                        "{fixture}: unredacted string at key `{key}`: {s:?}"
                    );
                }
                Value::Array(items) => {
                    for item in items {
                        audit(fixture, key, item);
                    }
                }
                Value::Object(map) => {
                    for (k, v) in map {
                        audit(fixture, k, v);
                    }
                }
                _ => {}
            }
        }
        for (fixture, content) in [
            ("files", include_str!("fixtures/google_drive/files.json")),
            ("drives", include_str!("fixtures/google_drive/drives.json")),
            (
                "file_permissions",
                include_str!("fixtures/google_drive/file_permissions.json"),
            ),
            (
                "files_type_mismatch",
                include_str!("fixtures/google_drive/files_type_mismatch.json"),
            ),
        ] {
            let page: Value = serde_json::from_str(content).expect("fixture parses");
            audit(fixture, "$", &page);
        }
    }

    #[test]
    fn empty_page_keeps_schema_stable() {
        // An empty first page must still produce the full declared
        // schema, or a first-page-empty scan would change shape. The
        // widths are this pack's column counts: 14 / 13 / 11.
        for (short, row_key, columns) in [
            ("files", "files", 14),
            ("drives", "drives", 13),
            ("file_permissions", "permissions", 11),
        ] {
            let t = table(short);
            let batch = convert_page(t, &json!({row_key: [], "nextPageToken": null}));
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
        // Admission-gate schema-mismatch fixture, and not a hypothetical
        // one: Google's own REST API returns `size` as a decimal STRING;
        // the normalized wire is a number only because the executor's
        // `parseSizeBytes` converts it. If that ever regressed toward
        // passthrough, this is the loud failure it must produce — full
        // row-scoped identity and the JSON kind, never a silent NULL and
        // never the offending value.
        let t = table("files");
        let page: Value = serde_json::from_str(include_str!(
            "fixtures/google_drive/files_type_mismatch.json"
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
                assert_eq!(column, "size_bytes");
                assert_eq!(path, "$.sizeBytes");
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
    fn a_row_without_an_id_fails_the_page_rather_than_yielding_a_null_key() {
        // `id` is the one non-nullable column on every table. Unlike
        // one_drive (whose item schema has no `required` array), an
        // id-less row here is contract-ILLEGAL — declared `required`
        // plus `String(x ?? "")` upstream — so this arm is defense in
        // depth: even a gateway violating its own contract cannot hand
        // SQL a NULL join key. The page fails, naming the column.
        let t = table("files");
        let mut page: Value =
            serde_json::from_str(include_str!("fixtures/google_drive/files.json"))
                .expect("fixture parses");
        page["files"][1]
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
                "files",
                include_str!("fixtures/google_drive/contracts/files.list.json"),
            ),
            (
                "drives",
                include_str!("fixtures/google_drive/contracts/drives.list.json"),
            ),
            (
                "file_permissions",
                include_str!("fixtures/google_drive/contracts/permissions.list.json"),
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
    fn only_the_five_restriction_flags_escape_the_fingerprint_gate() {
        // The pack's coverage ledger, both directions. `files` and
        // `file_permissions` are fully inside the gate: upstream
        // renaming any mapped key changes the hash and fails
        // registration. On `drives`, exactly the five `restrictions.*`
        // flags are outside it — `restrictions` is declared as a bare
        // open object, so an upstream INNER-key rename is silent (hash
        // unchanged, column quietly all-NULL): the reviewed case-1 gap
        // this pin makes deliberate. If this list grows, a mapped path
        // left the declared surface; if it shrinks, upstream started
        // declaring the flags and the yaml's rationale is stale — either
        // way the change must be conscious.
        for (short, contract) in [
            (
                "files",
                include_str!("fixtures/google_drive/contracts/files.list.json"),
            ),
            (
                "file_permissions",
                include_str!("fixtures/google_drive/contracts/permissions.list.json"),
            ),
        ] {
            let t = table(short);
            assert_eq!(
                fingerprint_uncovered_columns(contract, t.row_path, t.fields),
                &[] as &[&str],
                "{short}: every column must stay inside the fingerprint gate"
            );
        }
        let drives = table("drives");
        assert_eq!(
            fingerprint_uncovered_columns(
                include_str!("fixtures/google_drive/contracts/drives.list.json"),
                drives.row_path,
                drives.fields
            ),
            [
                "admin_managed_restrictions",
                "copy_requires_writer_permission",
                "domain_users_only",
                "drive_members_only",
                "sharing_folders_requires_organizer_permission",
            ],
            "drives: exactly the five restriction flags trade coverage for audit value"
        );
    }

    #[test]
    fn every_table_declares_terminating_cursor_pagination() {
        // One pagination grammar across all three actions: opaque
        // `pageToken` in, top-level `nextPageToken` out — declared
        // `anyOf [string, null]` AND required, so the key is always
        // present and null exactly on the final page. No executor
        // filters rows after paginating, so null-cursor termination is
        // complete; Google publishes no has-more flag, so no override.
        for (short, row_path, page_size) in [
            ("files", "$.files", 1000),
            ("drives", "$.drives", 100),
            ("file_permissions", "$.permissions", 100),
        ] {
            let t = table(short);
            match t.pagination {
                PaginationStrategy::Cursor {
                    cursor_param,
                    next_cursor_path,
                    page_size_param,
                    page_size: declared,
                    has_more_path,
                } => {
                    assert_eq!(cursor_param, "pageToken", "{short}");
                    assert_eq!(next_cursor_path, "$.nextPageToken", "{short}");
                    assert_eq!(page_size_param, Some("pageSize"), "{short}");
                    // The declared ceilings, confirmed WIRE bounds in
                    // phase 4 (1000/100/100 each 200 with real
                    // credentials; 1001/101/0 each 400).
                    assert_eq!(declared, page_size, "{short}");
                    assert!(has_more_path.is_none(), "{short}");
                }
                other => panic!("{short} must paginate by cursor, got {other:?}"),
            }
            assert_eq!(t.row_path, row_path, "{short}");
            // In-band Google errors are consumed by `assertGoogleResponse`
            // upstream into the failure envelope; nothing to forward.
            assert!(t.error_path.is_none(), "{short}");
        }
    }

    #[test]
    fn resources_and_fixed_inputs_are_exactly_as_designed() {
        // `files`: the two all-drives pins (BTreeMap order — the loader
        // sorts fixed inputs alphabetically) plus two composable optional
        // scopes; no exclusive group, because `driveId` and `q` compose
        // (a query within one drive is meaningful) — and the deprecated
        // `teamDriveId` alias that WOULD have needed one is simply not
        // declared as a resource.
        let files = table("files");
        assert_eq!(
            files.fixed_inputs,
            &[
                ("includeItemsFromAllDrives", FixedValue::Bool(true)),
                ("supportsAllDrives", FixedValue::Bool(true)),
            ]
        );
        assert!(files.required_resources.is_empty());
        assert_eq!(files.optional_resources, &["driveId", "q"]);
        assert!(files.exclusive_resources.is_empty());

        // `drives`: `q` only; `useDomainAdminAccess` never becomes a
        // knob (Workspace-admin-only, 403s for everyone else).
        let drives = table("drives");
        assert!(drives.fixed_inputs.is_empty());
        assert!(drives.required_resources.is_empty());
        assert_eq!(drives.optional_resources, &["q"]);

        // `file_permissions`: the binding names the file; nothing else.
        // No `supportsAllDrives` pin needed — THIS executor (unlike
        // files.list) defaults it true upstream.
        let permissions = table("file_permissions");
        assert!(permissions.fixed_inputs.is_empty());
        assert_eq!(permissions.required_resources, &["fileId"]);
        assert!(permissions.optional_resources.is_empty());

        // No table maps a filter: `q` is a query LANGUAGE, so pushdown
        // is structurally impossible (a column-op-value literal is
        // never a legal `q`) — a resource, not a FilterMapping.
        for short in ["files", "drives", "file_permissions"] {
            assert!(table(short).filters.is_empty(), "{short}");
        }
    }

    #[test]
    fn generated_inputs_are_accepted_by_the_captured_input_contracts() {
        // The fingerprint gate is OUTPUT-only, so the input half lives
        // in committed captures, gmail-style: every key this pack can
        // put on the wire must be declared, every declared bound must
        // contain the pinned page size, and the negative space — both
        // the undeclared keys that 400ed live and the declared keys the
        // yaml promises never to send — is pinned per action. Compares
        // committed artifacts, not the live gateway: its value is on
        // RE-CAPTURE after an upstream bump.
        for (short, contract, declared_never_sent) in [
            (
                "files",
                include_str!("fixtures/google_drive/contracts/inputs/files.list.json"),
                // Superseded by the all-drives pins (`corpora`, the
                // deprecated `corpus`), the deprecated `driveId` alias
                // (`teamDriveId` — the silent-precedence trap), the
                // default space (`spaces`), a reorder of a fully-read
                // scan (`orderBy`), and two structural no-ops — the
                // executor's pinned `fields` projection carries neither
                // `labelInfo` nor `permissions` back out
                // (`includeLabels`, `includePermissionsForView`).
                vec![
                    "corpora",
                    "corpus",
                    "spaces",
                    "teamDriveId",
                    "orderBy",
                    "includeLabels",
                    "includePermissionsForView",
                ],
            ),
            (
                "drives",
                include_str!("fixtures/google_drive/contracts/inputs/drives.list.json"),
                // Workspace-admin-only; 403s for everyone else.
                vec!["useDomainAdminAccess"],
            ),
            (
                "file_permissions",
                include_str!("fixtures/google_drive/contracts/inputs/permissions.list.json"),
                // `fields` would REPLACE the executor's projection and
                // can only narrow the normalized surface; the published
                // view and the admin knob are out of scope; and
                // `supportsAllDrives` already defaults true upstream in
                // THIS executor.
                vec![
                    "fields",
                    "includePermissionsForView",
                    "useDomainAdminAccess",
                    "supportsAllDrives",
                ],
            ),
        ] {
            let schema: Value =
                serde_json::from_str(contract).expect("input contract fixture parses");
            let properties = &schema["properties"];
            let t = table(short);

            // Strictness is the premise: a lenient action would ignore
            // an undeclared key instead of 400ing on it.
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

            // …and anything the action requires must be among them. All
            // three actions declare NO `required` array today — which is
            // why `fileId` needs the resource-level requirement (see
            // below); a re-captured contract growing one breaks here.
            if let Some(required) = schema["required"].as_array() {
                for entry in required {
                    let entry = entry.as_str().expect("required entries are strings");
                    assert!(
                        generated.contains(&entry),
                        "{short}: the action requires `{entry}`, which this table never sends"
                    );
                }
            }

            // The pinned page size sits inside the declared bounds — at
            // the ceiling exactly (1–1000 / 1–100 / 1–100).
            if let PaginationStrategy::Cursor {
                page_size_param: Some(param),
                page_size,
                ..
            } = t.pagination
            {
                let declared = &properties[param];
                assert_eq!(
                    u64::from(page_size),
                    declared["maximum"].as_u64().expect("declared maximum"),
                    "{short}: the pinned size is the declared ceiling"
                );
                assert!(
                    u64::from(page_size) >= declared["minimum"].as_u64().expect("declared minimum")
                );
            }

            // The cursor is an OPAQUE token, not one_drive's URI: no
            // format, just non-empty — the engine never sends an empty
            // cursor, so `minLength: 1` costs nothing.
            assert_eq!(
                properties["pageToken"]["minLength"],
                json!(1),
                "{short}: the cursor is a non-empty opaque token"
            );

            // Negative space, undeclared side: each of these 400ed live
            // as `invalid_input` (probed 2026-08-25 — inputs are
            // validated before credentials). `fields` is in this list
            // for files.list ONLY: the sibling actions declare it.
            let undeclared: &[&str] = if short == "files" {
                &[
                    "fields",
                    "filter",
                    "orderby",
                    "maxResults",
                    "cursor",
                    "page",
                    "perPage",
                    "limit",
                ]
            } else {
                &[
                    "filter",
                    "orderby",
                    "maxResults",
                    "cursor",
                    "page",
                    "perPage",
                    "limit",
                ]
            };
            for absent in undeclared {
                assert!(
                    properties[*absent].is_null(),
                    "{short}: `{absent}` is not part of this action's input surface"
                );
            }

            // Negative space, declared side: the yaml's "never sent
            // though declared" claims stay honest — each key really is
            // declared (so the omission is a choice, not fiction) and
            // really is outside the generated set.
            for key in declared_never_sent {
                assert!(
                    !properties[key].is_null(),
                    "{short}: `{key}` should be declared upstream"
                );
                assert!(
                    !generated.contains(&key),
                    "{short}: `{key}` must never be generated"
                );
            }
        }

        // The `fileId` enforcement boundary, pinned the way one_drive
        // pins `query`: declared `minLength: 1` (so `""` is a
        // schema-layer 400) but absent from any `required` array (so a
        // MISSING fileId would pass validation and die in the executor's
        // own `resolveFileId` check). The resource-level requirement
        // closes the missing case at registration. This pins an upstream
        // DEFECT — if a re-captured contract grows `required:
        // ["fileId"]`, upstream fixed the gap: update this assertion and
        // the yaml rationale.
        let permissions: Value = serde_json::from_str(include_str!(
            "fixtures/google_drive/contracts/inputs/permissions.list.json"
        ))
        .expect("input contract fixture parses");
        assert_eq!(permissions["properties"]["fileId"]["minLength"], json!(1));
        assert!(
            permissions["required"].is_null(),
            "upstream now declares a `required` array ({}) — if it contains \
             `fileId`, the executor-layer gap this pack works around is fixed; \
             update this assertion and the `fileId` rationale in the yaml",
            permissions["required"]
        );
        assert_eq!(table("file_permissions").required_resources, &["fileId"]);
    }

    // ── Integration: the pack against a mock gateway, end to end. ───────

    fn google_drive_config(token_env: &str, tables: &str, resource: &str) -> OpenConnectorConfig {
        let resource_line = if resource.is_empty() {
            String::new()
        } else {
            format!("resource: {resource}")
        };
        serde_yaml::from_str(&format!(
            r#"
runtime_token_env: {token_env}
bindings:
  - name: gdrive
    source_pack: google_drive
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
            Some(&google_drive_config(token_env, tables, resource)),
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

    /// A minimal normalized file row: identity plus enough to prove the
    /// row travelled, deliberately not a full driveFile.
    fn file_row(id: &str) -> Value {
        json!({
            "id": id,
            "name": format!("{id}.txt"),
            "mimeType": "text/plain",
            "sizeBytes": 12
        })
    }

    #[tokio::test]
    async fn files_cursor_scan_pages_with_the_pinned_all_drives_inputs() {
        // Two-page cursor scan pinning the wire declaration: the two
        // all-drives pins and `pageSize` on EVERY request (continuations
        // included), the opaque cursor verbatim on page 2, explicit null
        // termination, row identity across pages, and exact key sets —
        // no `q`, `fields`, `corpora` or `orderBy` ever.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return google_drive_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/googledrive.files.list" {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                let page = match body["input"].get("pageToken").and_then(Value::as_str) {
                    None => json!({"files": [file_row("f-1"), file_row("f-2")],
                                    "nextPageToken": FILES_PAGE2_TOKEN}),
                    Some(token) if token == FILES_PAGE2_TOKEN => {
                        json!({"files": [file_row("f-3")], "nextPageToken": null})
                    }
                    Some(other) => return MockResponse::new(400, format!("bad cursor {other}")),
                };
                return MockResponse::ok(&envelope_ok(&page.to_string()));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_GDRIVE_FILES", "files", "").await;

        let batches = collect(&ctx, "SELECT id FROM saas.gdrive.files ORDER BY id").await;
        assert_eq!(column_values(&batches, "id"), vec!["f-1", "f-2", "f-3"]);

        let inputs = execute_inputs(&gateway, "googledrive.files.list");
        assert_eq!(inputs.len(), 2, "two cursor pages");
        assert_eq!(
            inputs[1]["pageToken"], FILES_PAGE2_TOKEN,
            "the opaque cursor verbatim"
        );
        for (page, (input, expected_keys)) in inputs
            .iter()
            .zip([
                vec!["includeItemsFromAllDrives", "pageSize", "supportsAllDrives"],
                vec![
                    "includeItemsFromAllDrives",
                    "pageSize",
                    "pageToken",
                    "supportsAllDrives",
                ],
            ])
            .enumerate()
        {
            assert_eq!(input["pageSize"], 1000, "declared ceiling: {input}");
            assert_eq!(input["supportsAllDrives"], true, "{input}");
            assert_eq!(input["includeItemsFromAllDrives"], true, "{input}");
            assert_eq!(input_keys(input), expected_keys, "page {} keys", page + 1);
        }
    }

    #[tokio::test]
    async fn optional_resources_forward_verbatim_and_compose() {
        // `driveId` and `q` on one binding: both reach the wire
        // byte-for-byte alongside the pins — the two scopes COMPOSE (a
        // query within one shared drive), which is why the yaml declares
        // no exclusive group. The `q` value is a whole Drive
        // query-language expression: exactly the shape a filter mapping
        // could never generate.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return google_drive_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/googledrive.files.list" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"files": [file_row("f-1")], "nextPageToken": null}).to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_GDRIVE_SCOPES",
            "files",
            "{driveId: 0ASyntheticSharedDriveUk9PVA, q: \"mimeType != 'application/vnd.google-apps.folder' and trashed = false\"}",
        )
        .await;

        let batches = collect(&ctx, "SELECT id FROM saas.gdrive.files").await;
        assert_eq!(column_values(&batches, "id"), vec!["f-1"]);

        let inputs = execute_inputs(&gateway, "googledrive.files.list");
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0]["driveId"], "0ASyntheticSharedDriveUk9PVA");
        assert_eq!(
            inputs[0]["q"],
            "mimeType != 'application/vnd.google-apps.folder' and trashed = false"
        );
        assert_eq!(
            input_keys(&inputs[0]),
            vec![
                "driveId",
                "includeItemsFromAllDrives",
                "pageSize",
                "q",
                "supportsAllDrives"
            ],
            "both scopes ride alongside the pins: {}",
            inputs[0]
        );
    }

    #[tokio::test]
    async fn drives_scan_sends_only_its_page_size_and_terminates_on_null() {
        // The leanest wire in the pack: an unbound `drives` scan is
        // `{"pageSize": 100}` and nothing else — no all-drives pins
        // (drives ARE the shared drives), no `useDomainAdminAccess`.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return google_drive_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/googledrive.drives.list" {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                let page = match body["input"].get("pageToken").and_then(Value::as_str) {
                    None => json!({"drives": [{"id": "d-1", "name": "Engineering"}],
                                    "nextPageToken": "drives-page-2"}),
                    Some("drives-page-2") => {
                        json!({"drives": [{"id": "d-2", "name": "Legal"}],
                               "nextPageToken": null})
                    }
                    Some(other) => return MockResponse::new(400, format!("bad cursor {other}")),
                };
                return MockResponse::ok(&envelope_ok(&page.to_string()));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_GDRIVE_DRIVES", "drives", "").await;

        let batches = collect(&ctx, "SELECT id, name FROM saas.gdrive.drives ORDER BY id").await;
        assert_eq!(column_values(&batches, "id"), vec!["d-1", "d-2"]);

        let inputs = execute_inputs(&gateway, "googledrive.drives.list");
        assert_eq!(inputs.len(), 2, "two cursor pages");
        assert_eq!(input_keys(&inputs[0]), vec!["pageSize"]);
        assert_eq!(
            inputs[0]["pageSize"], 100,
            "declared ceiling: {}",
            inputs[0]
        );
        assert_eq!(input_keys(&inputs[1]), vec!["pageSize", "pageToken"]);
    }

    #[tokio::test]
    async fn drives_q_resource_forwards_verbatim() {
        // `drives.list` has its own query language surface (`name` and
        // `hidden`, rather than the files corpus fields). Pin the binding-to-
        // wire path independently of `files.q`: the complete expression must
        // survive verbatim alongside the action's page size and no other key.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return google_drive_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/googledrive.drives.list" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"drives": [{"id": "d-1", "name": "Engineering"}],
                            "nextPageToken": null})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_GDRIVE_DRIVES_Q",
            "drives",
            "{q: \"name contains 'x'\"}",
        )
        .await;

        let batches = collect(&ctx, "SELECT id FROM saas.gdrive.drives").await;
        assert_eq!(column_values(&batches, "id"), vec!["d-1"]);

        let inputs = execute_inputs(&gateway, "googledrive.drives.list");
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0]["q"], "name contains 'x'");
        assert_eq!(inputs[0]["pageSize"], 100);
        assert_eq!(input_keys(&inputs[0]), vec!["pageSize", "q"]);
    }

    #[tokio::test]
    async fn file_permissions_scan_forwards_its_required_file_id_on_every_page() {
        // The binding names the file, and the value rides EVERY request
        // including continuations — rows carry no file identity of their
        // own, so the binding is the only place the file is named.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return google_drive_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/googledrive.permissions.list" {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                let page = match body["input"].get("pageToken").and_then(Value::as_str) {
                    None => json!({"permissions": [{"id": "p-1", "role": "owner"}],
                                    "nextPageToken": "perms-page-2"}),
                    Some("perms-page-2") => {
                        json!({"permissions": [{"id": "p-2", "role": "reader"}],
                               "nextPageToken": null})
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
            "SKARDI_TEST_OC_GDRIVE_PERMS",
            "file_permissions",
            "{fileId: 1SyntheticMyDriveDocAAAAAAAAAAAAAAAAAAAAAAA}",
        )
        .await;

        let batches = collect(
            &ctx,
            "SELECT id, role FROM saas.gdrive.file_permissions ORDER BY id",
        )
        .await;
        assert_eq!(column_values(&batches, "id"), vec!["p-1", "p-2"]);

        let inputs = execute_inputs(&gateway, "googledrive.permissions.list");
        assert_eq!(inputs.len(), 2, "two cursor pages");
        for (page, (input, expected_keys)) in inputs
            .iter()
            .zip([
                vec!["fileId", "pageSize"],
                vec!["fileId", "pageSize", "pageToken"],
            ])
            .enumerate()
        {
            assert_eq!(
                input["fileId"], "1SyntheticMyDriveDocAAAAAAAAAAAAAAAAAAAAAAA",
                "the bound file on every page: {input}"
            );
            assert_eq!(input["pageSize"], 100, "declared ceiling: {input}");
            assert_eq!(input_keys(input), expected_keys, "page {} keys", page + 1);
        }
    }

    #[tokio::test]
    async fn a_permissions_binding_without_a_file_id_fails_before_any_http() {
        // The failing arm of the required resource. Upstream, a missing
        // fileId passes schema validation (empty `required` array) and
        // dies in the executor — declaring the resource is what refuses
        // the binding at REGISTRATION instead, before discovery: the
        // gateway sees nothing but the health probe.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return google_drive_discovery(&req.path);
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let _token = EnvVarGuard::set("SKARDI_TEST_OC_GDRIVE_NO_FILE", "test-token");
        let mut ctx = SessionContext::new();
        let gateways = OpenConnectorGateways::default();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&google_drive_config(
                "SKARDI_TEST_OC_GDRIVE_NO_FILE",
                "file_permissions",
                "",
            )),
            false,
            HierarchyLevel::Catalog,
            Some(&gateways),
        )
        .await
        .expect_err("a permissions binding with no fileId must fail registration");
        assert!(
            err.to_string().contains("fileId"),
            "the missing resource is named: {err}"
        );
        assert!(
            gateway.requests().iter().all(|r| r.path == "/v1/health"),
            "resource enforcement precedes discovery"
        );
    }

    #[tokio::test]
    async fn predicates_stay_local_against_a_provider_that_cannot_narrow() {
        // No filter mapping exists (see the resources test), so every
        // predicate runs in DataFusion after the bounded fetch and the
        // wire request is identical to an unfiltered one — in particular
        // no `q` invented from the SQL, even though a human could write
        // this WHERE clause as one.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return google_drive_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/googledrive.files.list" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"files": [file_row("keep"), file_row("drop")],
                            "nextPageToken": null})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_GDRIVE_FILTER", "files", "").await;

        let batches = collect(&ctx, "SELECT id FROM saas.gdrive.files WHERE id = 'keep'").await;
        assert_eq!(column_values(&batches, "id"), vec!["keep"]);

        let inputs = execute_inputs(&gateway, "googledrive.files.list");
        assert_eq!(inputs.len(), 1);
        assert_eq!(
            input_keys(&inputs[0]),
            vec!["includeItemsFromAllDrives", "pageSize", "supportsAllDrives"],
            "a WHERE clause must not invent a wire input: {}",
            inputs[0]
        );
    }

    #[tokio::test]
    async fn limit_stops_cursor_pagination_early() {
        // LIMIT must stop the walk rather than drain the corpus: one
        // page fetched, the cursor never followed — and the page size
        // stays the declared ceiling, because `pageSize` bounds
        // REQUESTS, not bytes (a LIMIT 1 still transfers a full page).
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return google_drive_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/googledrive.files.list" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"files": [file_row("f-1"), file_row("f-2")],
                            "nextPageToken": FILES_PAGE2_TOKEN})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_GDRIVE_LIMIT", "files", "").await;

        // Cardinality, not identity: WHICH row a LIMIT keeps without an
        // ORDER BY is not something SQL promises.
        let batches = collect(&ctx, "SELECT id FROM saas.gdrive.files LIMIT 1").await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let inputs = execute_inputs(&gateway, "googledrive.files.list");
        assert_eq!(inputs.len(), 1, "LIMIT stops before following the cursor");
        assert_eq!(
            inputs[0]["pageSize"], 1000,
            "a full page still crosses the wire: {}",
            inputs[0]
        );
    }

    #[tokio::test]
    async fn scan_of_an_account_with_no_shared_drives_is_clean() {
        // `drives` lists SHARED drives only — My Drive is not a drive
        // resource — so on an account with no shared-drive membership
        // the table is legitimately empty: a successful empty scan, not
        // an error. This is also the shape design record R1 warns about:
        // "empty and correct" and "empty because the account cannot see
        // shared drives at all" are indistinguishable from here, which
        // is exactly why phase 4 needed an account that could see one
        // — R1, resolved: the phase-4 account created its own.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return google_drive_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/googledrive.drives.list" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"drives": [], "nextPageToken": null}).to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_GDRIVE_EMPTY", "drives", "").await;

        let batches = collect(&ctx, "SELECT id, name FROM saas.gdrive.drives").await;
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            0,
            "an empty collection is an empty result set, not an error"
        );
        assert_eq!(
            execute_inputs(&gateway, "googledrive.drives.list").len(),
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
                return google_drive_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/googledrive.files.list" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"files": [file_row("f-1")], "nextPageToken": FILES_PAGE2_TOKEN})
                        .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_GDRIVE_LOOP", "files", "").await;

        let err = ctx
            .sql("SELECT id FROM saas.gdrive.files")
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("a repeated cursor must fail the scan");
        let message = err.to_string();
        assert!(
            message.contains("pagination loop") && message.contains(FILES_PAGE2_TOKEN),
            "the loop names the cursor the gateway would not advance: {message}"
        );
    }

    #[tokio::test]
    async fn provider_errors_surface_through_the_gateway_failure_envelope() {
        // In-band Google errors are consumed upstream, so the pack sees
        // them as a failure envelope — the exact surface the phase-1
        // no-credential calibration witnessed live (403
        // `authorization_failed` on a valid input with no connection).
        // The assertion also pins that the TWO-DOT action id survives
        // intact into error identity.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return google_drive_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/googledrive.files.list" {
                return MockResponse::new(
                    403,
                    envelope_err(
                        "authorization_failed",
                        "Connect googledrive with OAuth first.",
                    ),
                );
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_GDRIVE_AUTHZ", "files", "").await;

        let err = ctx
            .sql("SELECT id FROM saas.gdrive.files")
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("a failure envelope must fail the scan");
        let message = err.to_string();
        assert!(
            message.contains("authorization_failed") && message.contains("googledrive.files.list"),
            "the gateway's error code and the dotted action id are named: {message}"
        );
        assert!(
            !message.contains("row path"),
            "never the misleading row-path error: {message}"
        );
    }

    #[tokio::test]
    async fn udtf_parity_for_files() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return google_drive_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/googledrive.files.list" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"files": [file_row("f-1")], "nextPageToken": null}).to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_GDRIVE_UDTF", "files", "").await;

        let from_table = collect(&ctx, "SELECT id, name, mime_type FROM saas.gdrive.files").await;
        let from_udtf = collect(
            &ctx,
            "SELECT id, name, mime_type \
             FROM open_connector_query('saas', 'google_drive.files', '{}')",
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
        // REGISTRATION, table and action named — the dotted action id
        // again travelling the error intact. (Every other e2e proves the
        // pass side via google_drive_discovery's captured contracts.)
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
        let _token = EnvVarGuard::set("SKARDI_TEST_OC_GDRIVE_DRIFT", "test-token");
        let mut ctx = SessionContext::new();
        let gateways = OpenConnectorGateways::default();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&google_drive_config(
                "SKARDI_TEST_OC_GDRIVE_DRIFT",
                "files",
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
            message.contains("google_drive.files")
                && message.contains("googledrive.files.list")
                && message.contains("fingerprint mismatch"),
            "table, action, and cause are named: {message}"
        );
    }
}
