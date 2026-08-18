//! Dropbox source pack: stable relational contracts over the Open
//! Connector `dropbox.*` read actions (OAuth2, cursor pagination with
//! split-action continuation).
//!
//! # Provenance: live-verified 2026-08-18
//!
//! Authored against the Open Connector provider source
//! (`src/providers/dropbox/`) and then verified end to end against a
//! self-hosted Open Connector gateway at commit `a3efa99` and a real
//! (free-tier) Dropbox account. What the live pass established:
//!
//! - the five `fixtures/dropbox/contracts/*.json` captures came back
//!   byte-identical to the source-derived schemas they replaced, so every
//!   `fingerprint:` in `dropbox.yaml` matched LIVE discovery unchanged and
//!   registration passed the contract gate on the first try. The
//!   `*_continue.json` files being identical to their openers is a fact
//!   about the wire, not an artifact of how they were derived;
//! - all three tables scanned real rows, and every mapped column carried a
//!   real non-NULL value somewhere — 12/12 on `files` (378 rows), 13/13 on
//!   `file_search`, and 11/11 on `shared_links` AFTER the live pass removed
//!   four columns that cannot populate (see the `shared_links` note below);
//! - the declared page sizes are the wire's maxima, probed at the boundary:
//!   `limit: 2000` and `maxResults: 1000` return rows, 2001 and 1001 are
//!   refused;
//! - real multi-page pagination ran through `list_folder_continue`
//!   (`pages=2 rows=378`), and `LIMIT` pushdown collapsed the same scan to
//!   `pages=1 rows=1`, stopping before the continuation request.
//!
//! What the live pass did NOT change: the row fixtures are still
//! hand-authored pages in the executor's shape rather than redacted live
//! captures, because the account read during the pass holds personal
//! files. `shared_links.json` was corrected to the live shape (the four
//! removed keys now null, as the wire has them), and
//! `files_type_mismatch.json` stays deliberately synthetic. Re-deriving
//! the fixtures from redacted captures remains open.
//!
//! Two claims remain UNOBSERVED rather than confirmed, and neither is a
//! defect: `shared_links.expires_at` needs paid-tier link expiry (a free
//! account is refused with `settings_error/not_authorized`), and
//! `file_search.match_type = 'content'` needs Dropbox content indexing,
//! which never landed during the pass. Both columns stay mapped because
//! both are structurally reachable.
//!
//! One gateway defect the pass uncovered, which is NOT specific to this
//! pack: `GET /v1/actions/<id>` — the endpoint `discover_action` reads —
//! does not serialize the `execution` block, so Skardi's default-deny
//! action registry refuses every action from every pack against a
//! stock gateway at that commit. Reproduced with the merged `github` pack,
//! so it is not this pack's bug; filed upstream as
//! oomol-lab/open-connector#358 (the gateway's catalog routes already
//! compute the field). Skardi's default-deny gate is correct and is left
//! untouched — no fallback reads the classification from another route.
//!
//! **Rows are NORMALIZED, not passed through.** Every list executor maps
//! entries through `mapDropboxMetadata`, which rebuilds each one into a
//! fixed camelCase shape — `tag` / `name` / `id` / `pathDisplay` /
//! `pathLower` / `clientModified` / `serverModified` / `rev` /
//! `sizeBytes` / `isDownloadable` / `contentHash` / `url` / `expiresAt` /
//! `sharingInfo` / `linkPermissions` — with all fifteen keys declared
//! `required` under `additionalProperties: false`. Mapping Dropbox's own
//! snake_case (`path_display`, `client_modified`, `size`) would have
//! produced all-NULL columns no Dropbox doc would explain, the exact
//! Slack 5.2 failure mode. The upside of that strictness, and the reason
//! this pack carries less column risk than the passthrough packs: every
//! mapped column sits INSIDE the fingerprint gate, so the coverage-gap
//! pin below is empty rather than a list of unguarded fields.
//!
//! Design decisions, per the integration design spec and the source-pack
//! admission gate:
//!
//! - **Split-action continuation on `files` and `file_search`.** Dropbox
//!   continues a listing through a DIFFERENT action than the one that
//!   opened it: `list_folder` → `list_folder_continue`, `search_files` →
//!   `search_files_continue`, each continue action declaring `cursor` as
//!   its ONLY property under `additionalProperties: false`. This is not a
//!   style difference: `list_folder` does not declare `cursor`, and its
//!   input schema is `additionalProperties: false`, so feeding the cursor
//!   back to the opening action is a hard 400 rather than a quiet
//!   truncation — read off the declared schema and CONFIRMED on the wire
//!   2026-08-18, bare and alongside the full input.
//!   The engine's `continuation: {action, fingerprint, inputs:
//!   cursor_only}` exists for this shape.
//!
//!   Both actions are fingerprint-gated, but be precise about what that
//!   buys: a fingerprint hashes the OUTPUT schema, and an opener and its
//!   continuation publish the same one here, so the continuation pin
//!   guards the row shape of pages 2..N and cannot fail alone. The claim
//!   that actually keeps the scan alive is `cursor_only`, an INPUT claim,
//!   and registration checks it separately against the continuation
//!   action's discovered input schema
//!   (`SourcePackTable::check_continuation_inputs`): the cursor input must
//!   be a declared property and no other input may be required. A
//!   continuation action publishing no input schema is refused rather
//!   than trusted, the same default-deny posture raw scans take toward a
//!   missing read/write classification.
//!
//! - **`has_more_path` is load-bearing on `files`, not decorative.**
//!   `list_folder` answers its FINAL page with a NON-EMPTY cursor —
//!   OBSERVED live 2026-08-18, where the final page carried
//!   `hasMore: false` beside a 259-character cursor and following that
//!   cursor returned an empty page. Null-cursor termination alone would
//!   refetch and fail as a `PaginationLoop`. `shared_links` and `file_search` DO null
//!   their cursors (`anyOf: [string, null]` in their contracts), but
//!   declare `$.hasMore` too: it is the provider's authoritative signal
//!   in all three, and one termination rule across the pack beats three.
//!
//! - **`files` pins the complete collection.** `recursive: true` is the
//!   `state=all` move from 5.1 — a table named `files` that returns one
//!   directory level is a surprising contract, so the table means "every
//!   file under `path`". `includeMountedFolders: true` pins Dropbox's own
//!   default so it cannot drift. `includeDeleted` stays off
//!   deliberately: deleted tombstones carry a `deleted` tag and null
//!   everything else, informing no query.
//!
//! - **Three columns are deliberately absent from `files`.** `url`,
//!   `expires_at` and `link_permissions` exist in `mapDropboxMetadata`
//!   but are sourced from `record.url` / `.expires` /
//!   `.link_permissions`, which `files/list_folder` never returns —
//!   mapping them would ship three structurally always-NULL columns.
//!   They live on `shared_links`, where they populate. A negative-space
//!   test pins their absence.
//!
//! - **Four columns are deliberately absent from `shared_links`** — the
//!   mirror image of the three above, and the one defect the live pass
//!   caught. `sharing/list_shared_links` returns SharedLinkMetadata,
//!   which carries `path_lower` but has NO `path_display`,
//!   `is_downloadable`, `content_hash` or `sharing_info` field at all, so
//!   the shared normalizer reads those four off keys the payload never
//!   has. They were mapped here until 2026-08-18, when the live pass
//!   measured them at 0/5 non-NULL and then settled it decisively: a file
//!   whose `files` row carries all four came back with all four NULL on
//!   its own `shared_links` row — same file, same normalizer, different
//!   endpoint. The fingerprint gate cannot catch this class, because both
//!   actions publish the same normalized 15-key schema; only real rows
//!   can. A negative-space test pins the absence.
//!
//! - **No filter is pushed by any table.** Dropbox's remaining list
//!   inputs are scan-shape controls (`recursive`, `includeDeleted`,
//!   `limit`, `filenameOnly`), not column predicates. `path` is a
//!   resource rather than an `eq` push onto `path_lower` for two
//!   reasons: on `files` it selects the listing ROOT, which is a
//!   different claim from a path equality; and on `shared_links` the
//!   input accepts paths, file IDs AND rev IDs, so the mapping would be
//!   unfaithful across most of its value domain — Exact would be wrong
//!   and Inexact would still push a rev ID as though it were a path.
//!   Guard tests prove no filter key ever reaches the wire.
//!
//! - **`shared_links` continues through its own action.** The executor
//!   `compactObject`s `path` and `cursor` together and the input schema
//!   declares both, so no `continuation` block is declared and pages
//!   2..N repeat the action with the full input. This was design spec
//!   open question 1; the live pass answered it YES on 2026-08-18 —
//!   `list_shared_links` accepts `path` and `cursor` in one request — so
//!   no `cursor_only` continuation is needed here.
//!
//! - **`directOnly` is a binding-level resource, not a pin.** It is a
//!   scan-shape boolean — the class of input this pack otherwise pins
//!   (`recursive`, `includeDeleted`, `fileStatus`) — and it is exposed
//!   because neither setting is an honest default for a table named
//!   `shared_links`: pinned `true`, links a file inherits from a shared
//!   ancestor disappear; pinned `false`, one file can surface through
//!   several ancestors. Which of those a deployment wants is not a pack
//!   decision. Resource values keep their YAML type
//!   (`OpenConnectorBinding::resource` is a `BTreeMap<String, Value>`),
//!   so `directOnly: true` reaches the strict schema as a JSON boolean
//!   rather than the string `"true"`; a forwarding test pins that.
//!
//! - **`file_search` requires `query`.** A search table without one is
//!   not a table (the GitHub `owner`/`repo` precedent), and the
//!   requirement is enforced before any HTTP. `fileStatus: active` is
//!   pinned for the same reason `includeDeleted` is pinned off.
//!   `orderBy` is deliberately NOT pinned: `search/continue_v2` pages a
//!   server-side snapshot taken at the opening call, so relevance order
//!   is stable within one scan — unlike Feishu's chats, whose default
//!   ordering reshuffles mid-scan and forced `ByCreateTimeAsc`.
//!
//! - **`highlight_spans` is unmapped and `includeHighlights` is never
//!   sent.** The field only populates when highlights are requested, and
//!   the declared schema (`anyOf: [array, null]`) contradicts the
//!   executor (`readObjectArray`, which returns `[]` and never null).
//!   Recorded as a wire-vs-contract contradiction; not worth a column.
//!
//! - **`file_search`'s `metadata` nullability is UNRESOLVED, and it is
//!   the one open question that can fail a live scan.** The derived
//!   contract declares `matches[].metadata` a required, non-nullable
//!   object, and `tag` / `name` are mapped `nullable: false` on that
//!   basis — so a real `metadata: null` row would fail the scan rather
//!   than yield NULLs. `fixtures/dropbox/file_search_null_parent.json` is
//!   a SYNTHETIC probe of that path (it encodes a shape the derived
//!   contract says cannot occur, deliberately, like the schema-mismatch
//!   fixture): it proves the converter names the offending non-nullable
//!   column instead of quietly emitting an all-NULL row. If the live pass
//!   finds that Dropbox can null a match's metadata, `tag` and `name`
//!   become nullable and that fixture graduates from synthetic to
//!   captured.
//!
//! - **No `error_path` anywhere.** `dropboxRpcRequest` throws on any
//!   non-2xx, so Dropbox's in-band `error_summary` envelope AND its 429
//!   rate limiting both surface as gateway FAILURE envelopes, never as
//!   HTTP 200 rows.
//!
//! - **Tables deferred, with reasons.** `list_revisions` pages by
//!   feeding a `beforeRev` from the previous page's rows and answers
//!   `hasMore` with no cursor to follow — no pack-side strategy can
//!   complete it, so it is absent rather than shipped incomplete (the
//!   5.2 Slack message-history deferral repeated). `get_current_account`
//!   returns a single object and `RowPath::rows` requires an array.
//!   `get_tags` takes an array of paths (resources are scalars) and
//!   declares no pagination. Every write action (`upload_file`, `move`,
//!   `copy`, `delete`, `create_folder`, the shared-link mutators,
//!   `save_url`, `restore`) is outside the read-only allowlist, and the
//!   content actions (`download_file`, `get_temporary_link`,
//!   `get_shared_link_file`) return base64 payloads, not rows.
//!
//! Authorization: `files` and `file_search` need the
//! `files.metadata.read` scope; `shared_links` needs `sharing.read` —
//! both confirmed against the live gateway's per-action `requiredScopes`.
//! No content or write scope is required by any shipped table.
//!
//! That is a statement about the ACTIONS, not about what an operator can
//! provision. The gateway's dropbox provider builds its OAuth scope list
//! as the union of every `dropbox.*` action's permissions, so its
//! authorize request asks for all six — including `files.content.write`
//! and `sharing.write` — and a connection created through that flow
//! carries write access this pack never exercises. Narrowing it is a
//! gateway-side change; see `docs/open-connector-dropbox.md`.

use std::sync::OnceLock;

use crate::sources::providers::open_connector::error::OpenConnectorError;
use crate::sources::providers::open_connector::source_pack::SourcePack;

use super::loader;

static PACK: OnceLock<Result<SourcePack, String>> = OnceLock::new();

/// The Dropbox pack, parsed once from the embedded YAML asset.
pub fn pack() -> Result<&'static SourcePack, OpenConnectorError> {
    loader::builtin("dropbox.yaml", include_str!("dropbox.yaml"), &PACK)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::hierarchy::HierarchyLevel;
    use crate::sources::providers::open_connector::action_registry::fingerprint_schema;
    use crate::sources::providers::open_connector::json_to_arrow::RowConverter;
    use crate::sources::providers::open_connector::row_path::RowPath;
    use crate::sources::providers::open_connector::source_pack::SourcePackTable;
    use crate::sources::providers::open_connector::testutil::{
        EnvVarGuard, MockGateway, MockResponse, discovery_ok, envelope_ok,
        fingerprint_uncovered_columns,
    };
    use crate::sources::providers::open_connector::{
        OpenConnectorConfig, OpenConnectorGateways, register_open_connector_tables,
        register_open_connector_udtfs,
    };
    use arrow::array::{Array, BooleanArray, Int64Array, StringArray, TimestampMillisecondArray};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use serde_json::{Value, json};

    /// Look up a table by short name; the assets are test-pinned to parse.
    fn table(short: &str) -> &'static SourcePackTable {
        pack()
            .expect("embedded asset is test-pinned to parse")
            .tables
            .iter()
            .find(|t| t.id.rsplit('.').next() == Some(short))
            .unwrap_or_else(|| panic!("table {short}"))
    }

    /// Every table's captured contract, keyed by the action it belongs to.
    fn contracts() -> Vec<(&'static str, &'static str)> {
        vec![
            (
                "dropbox.list_folder",
                include_str!("fixtures/dropbox/contracts/list_folder.json"),
            ),
            (
                "dropbox.list_folder_continue",
                include_str!("fixtures/dropbox/contracts/list_folder_continue.json"),
            ),
            (
                "dropbox.list_shared_links",
                include_str!("fixtures/dropbox/contracts/list_shared_links.json"),
            ),
            (
                "dropbox.search_files",
                include_str!("fixtures/dropbox/contracts/search_files.json"),
            ),
            (
                "dropbox.search_files_continue",
                include_str!("fixtures/dropbox/contracts/search_files_continue.json"),
            ),
        ]
    }

    #[test]
    fn pinned_fingerprints_match_the_committed_contracts() {
        // Locks pin ↔ fixture for BOTH actions of a split-action table.
        // Collects every mismatch and prints the actual hash, which is
        // also how the pins are obtained the first time.
        let mut mismatches = Vec::new();
        for (action, contract) in contracts() {
            let schema: Value = serde_json::from_str(contract).expect("contract parses");
            let actual = fingerprint_schema(Some(&schema));
            let expected: Vec<&str> = pack()
                .expect("parses")
                .tables
                .iter()
                .flat_map(SourcePackTable::gated_actions)
                .filter(|(id, _)| *id == action)
                .map(|(_, fingerprint)| fingerprint)
                .collect();
            assert!(
                !expected.is_empty(),
                "{action} has a captured contract but no table pins it"
            );
            for pin in expected {
                if pin != actual {
                    mismatches.push(format!("{action}: pinned {pin}, actual {actual}"));
                }
            }
        }
        assert!(mismatches.is_empty(), "fingerprint drift:\n{mismatches:#?}");
    }

    #[test]
    fn every_mapped_column_is_inside_the_fingerprint_gate() {
        // Dropbox's row schemas are strict with all keys required, so
        // unlike the passthrough packs NO column rides
        // `additionalProperties` outside the gate. An empty set here is
        // the goal; a non-empty one is a finding, not a pin to update.
        for (short, contract) in [
            (
                "files",
                include_str!("fixtures/dropbox/contracts/list_folder.json"),
            ),
            (
                "shared_links",
                include_str!("fixtures/dropbox/contracts/list_shared_links.json"),
            ),
            (
                "file_search",
                include_str!("fixtures/dropbox/contracts/search_files.json"),
            ),
        ] {
            let table = table(short);
            let uncovered = fingerprint_uncovered_columns(contract, table.row_path, table.fields);
            assert!(
                uncovered.is_empty(),
                "{short}: columns outside the fingerprint gate: {uncovered:?}"
            );
        }
    }

    #[test]
    fn split_action_tables_declare_a_cursor_only_continuation() {
        // The 400 this pack exists to avoid: `cursor` is not a declared
        // property of `list_folder` / `search_files`, so pages 2..N must
        // target the continue action with the cursor ALONE.
        for (short, opener, continues) in [
            (
                "files",
                "dropbox.list_folder",
                "dropbox.list_folder_continue",
            ),
            (
                "file_search",
                "dropbox.search_files",
                "dropbox.search_files_continue",
            ),
        ] {
            let table = table(short);
            assert_eq!(table.action_id, opener);
            let continuation = table
                .continuation
                .unwrap_or_else(|| panic!("{short} must declare a continuation"));
            assert_eq!(continuation.action_id, continues);
            assert!(
                continuation.cursor_only,
                "{short}: the continue action accepts the cursor and nothing else"
            );
            // Both actions gated, so drift on either fails registration.
            assert_eq!(
                table.gated_actions().count(),
                2,
                "{short}: both actions must be fingerprint-gated"
            );
        }

        // shared_links takes the cursor on its OWN action — the executor
        // `compactObject`s `{path, cursor}` together and the schema
        // declares both — so it needs no continuation. Inference from the
        // source, not a wire observation: design spec open question 1.
        let shared = table("shared_links");
        assert!(shared.continuation.is_none());
        assert_eq!(shared.gated_actions().count(), 1);
    }

    #[test]
    fn no_table_pushes_a_filter_or_maps_shared_link_only_columns_onto_files() {
        // Negative space, per the module doc: every deliberate absence
        // gets a guard so a later edit cannot quietly reintroduce it.
        for short in ["files", "shared_links", "file_search"] {
            assert!(
                table(short).filters.is_empty(),
                "{short}: Dropbox exposes no faithful column predicate; \
                 pushing one needs a documented rationale first"
            );
        }

        let files: Vec<&str> = table("files").fields.iter().map(|f| f.name).collect();
        for absent in ["url", "expires_at", "link_permissions"] {
            assert!(
                !files.contains(&absent),
                "files must not map '{absent}': files/list_folder never returns it, \
                 so the column would be structurally always-NULL"
            );
        }
        // ...and they ARE mapped where they populate.
        let shared: Vec<&str> = table("shared_links")
            .fields
            .iter()
            .map(|f| f.name)
            .collect();
        for present in ["url", "expires_at", "link_permissions"] {
            assert!(shared.contains(&present), "shared_links must map {present}");
        }
        // The mirror image, and the defect the live pass caught:
        // `sharing/list_shared_links` returns SharedLinkMetadata, which
        // has no `path_display`, `is_downloadable`, `content_hash` or
        // `sharing_info` field at all, so these four were four
        // structurally always-NULL columns. Observed live 2026-08-18 as
        // 0/5 non-NULL, and decisively: a file whose files-row carries
        // all four returns all four NULL on its own shared_links row.
        for absent in [
            "path_display",
            "is_downloadable",
            "content_hash",
            "sharing_info",
        ] {
            assert!(
                !shared.contains(&absent),
                "shared_links must not map '{absent}': SharedLinkMetadata never \
                 carries it, so the column would be structurally always-NULL"
            );
        }

        // `includeHighlights` is never sent, so `highlight_spans` stays
        // unmapped (declared nullable, but the executor emits [] — a
        // contradiction recorded rather than mapped).
        let search: Vec<&str> = table("file_search").fields.iter().map(|f| f.name).collect();
        assert!(!search.contains(&"highlight_spans"));
        for (key, _) in table("file_search").fixed_inputs {
            assert_ne!(*key, "includeHighlights");
        }
    }

    // ── Contract tests. The fixtures are provider-SHAPED pages authored
    // from the executor source, not redacted live captures (module doc,
    // provenance banner); they cover all six admission-gate
    // categories. ─────────────────────────────────────────────────────

    fn convert_fixture(table: &SourcePackTable, fixture: &str) -> RecordBatch {
        let page: Value = serde_json::from_str(fixture).expect("fixture parses");
        let rows = RowPath::parse(table.row_path)
            .expect("row path")
            .rows(&page, 1)
            .expect("row array");
        RowConverter::new(table.fields)
            .expect("converter")
            .convert(rows, 1)
            .expect("fixture converts")
    }

    fn utf8<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("column {name}"))
            .as_any()
            .downcast_ref()
            .expect("Utf8 column")
    }

    #[test]
    fn files_fixture_converts_nulls_nested_and_extra_fields() {
        // Null-bearing (the folder row nulls ten columns), nested
        // (sharingInfo as JSON), and extra-field (propertyGroups /
        // hasExplicitSharedMembers, which the contract never declares)
        // in one page.
        let batch = convert_fixture(table("files"), include_str!("fixtures/dropbox/files.json"));
        assert_eq!(batch.num_rows(), 3);

        assert_eq!(
            utf8(&batch, "tag").iter().collect::<Vec<_>>(),
            vec![Some("folder"), Some("file"), Some("file")]
        );
        // A folder carries no file metadata; every one of those columns
        // must be SQL NULL rather than a zero value.
        let sizes: &Int64Array = batch
            .column_by_name("size_bytes")
            .expect("size_bytes")
            .as_any()
            .downcast_ref()
            .expect("Int64 column");
        assert!(sizes.is_null(0), "a folder has no size");
        assert_eq!(sizes.value(1), 284_913);
        assert_eq!(sizes.value(2), 0, "an empty file is 0, not NULL");

        let downloadable: &BooleanArray = batch
            .column_by_name("is_downloadable")
            .expect("is_downloadable")
            .as_any()
            .downcast_ref()
            .expect("Boolean column");
        assert!(downloadable.is_null(0));
        assert!(downloadable.value(1));

        let modified: &TimestampMillisecondArray = batch
            .column_by_name("server_modified")
            .expect("server_modified")
            .as_any()
            .downcast_ref()
            .expect("Timestamp column");
        assert!(modified.is_null(0), "folders carry no serverModified");
        assert!(modified.value(1) > 0, "ISO 8601 parses through RFC 3339");

        // Nested object kept whole as JSON text.
        let sharing = utf8(&batch, "sharing_info");
        assert!(sharing.is_null(0));
        assert!(
            sharing.value(1).contains("parent_shared_folder_id"),
            "nested object preserved: {}",
            sharing.value(1)
        );
    }

    #[test]
    fn empty_page_converts_to_zero_rows() {
        let batch = convert_fixture(
            table("files"),
            include_str!("fixtures/dropbox/files_empty.json"),
        );
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema().fields().len(), table("files").fields.len());
    }

    #[test]
    fn schema_mismatch_fails_with_the_full_error_identity() {
        // A valid first row followed by a declared-type violation: the
        // error must locate the failure by column, path, page AND row,
        // and name the found KIND without echoing the value.
        let table = table("files");
        let page: Value =
            serde_json::from_str(include_str!("fixtures/dropbox/files_type_mismatch.json"))
                .expect("fixture parses");
        let rows = RowPath::parse(table.row_path)
            .expect("row path")
            .rows(&page, 7)
            .expect("row array");
        let err = RowConverter::new(table.fields)
            .expect("converter")
            .convert(rows, 7)
            .expect_err("a string where int64 is declared must fail");

        let rendered = err.to_string();
        for fragment in [
            "size_bytes",
            "sizeBytes",
            "page 7",
            "row 1",
            "expected integer",
            "found string",
        ] {
            assert!(
                rendered.contains(fragment),
                "error must carry {fragment:?}: {rendered}"
            );
        }
        assert!(
            !rendered.contains("not-a-number"),
            "the offending VALUE must never appear: {rendered}"
        );
    }

    #[test]
    fn shared_links_fixture_populates_the_link_only_columns() {
        let batch = convert_fixture(
            table("shared_links"),
            include_str!("fixtures/dropbox/shared_links.json"),
        );
        assert_eq!(batch.num_rows(), 2);
        let urls = utf8(&batch, "url");
        assert!(urls.value(0).starts_with("https://www.dropbox.com/scl/fi/"));
        assert!(urls.value(1).starts_with("https://www.dropbox.com/scl/fo/"));

        let expires: &TimestampMillisecondArray = batch
            .column_by_name("expires_at")
            .expect("expires_at")
            .as_any()
            .downcast_ref()
            .expect("Timestamp column");
        assert!(expires.value(0) > 0, "a link with an expiry");
        assert!(expires.is_null(1), "a link without one");

        let permissions = utf8(&batch, "link_permissions");
        assert!(permissions.value(0).contains("resolved_visibility"));
    }

    #[test]
    fn file_search_fixture_reads_through_the_nested_metadata_block() {
        let batch = convert_fixture(
            table("file_search"),
            include_str!("fixtures/dropbox/file_search.json"),
        );
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(
            utf8(&batch, "match_type").iter().collect::<Vec<_>>(),
            vec![Some("filename"), Some("content")]
        );
        assert_eq!(
            utf8(&batch, "name").iter().collect::<Vec<_>>(),
            vec![Some("redacted-report.pdf"), Some("notes.txt")]
        );
    }

    #[test]
    fn a_null_metadata_parent_nulls_its_children_not_the_scan() {
        // The null-parent category. The fixture is SYNTHETIC on purpose:
        // the derived contract declares `matches[].metadata` a required,
        // non-nullable object, so this shape is one the gateway is not
        // supposed to produce — like the schema-mismatch fixture. What it
        // pins is the converter's behavior IF it ever does: `metadata:
        // null` makes every nested NULLABLE column SQL NULL, and the
        // non-nullable ones (tag, name) fail loudly, because a quiet
        // all-NULL row would hide the drift. If the live pass finds that
        // Dropbox can null a match's metadata, `tag`/`name` become
        // nullable and this fixture graduates to a real capture.
        let table = table("file_search");
        let page: Value = serde_json::from_str(include_str!(
            "fixtures/dropbox/file_search_null_parent.json"
        ))
        .expect("fixture parses");
        let rows = RowPath::parse(table.row_path)
            .expect("row path")
            .rows(&page, 1)
            .expect("row array");
        let err = RowConverter::new(table.fields)
            .expect("converter")
            .convert(rows, 1)
            .expect_err("a null parent under a non-nullable column must fail");
        assert!(
            err.to_string().contains("tag"),
            "the non-nullable column names itself: {err}"
        );
    }

    #[test]
    fn complete_collection_pins_ride_every_files_request() {
        // The loader keys fixed inputs by a BTreeMap, so they arrive
        // name-sorted rather than in authoring order.
        let files = table("files");
        let pinned: Vec<(&str, Value)> = files
            .fixed_inputs
            .iter()
            .map(|(key, value)| (*key, value.to_json()))
            .collect();
        assert_eq!(
            pinned,
            vec![
                ("includeDeleted", Value::Bool(false)),
                ("includeMountedFolders", Value::Bool(true)),
                ("recursive", Value::Bool(true)),
            ],
            "files means every file under `path`, tombstones excluded"
        );

        // file_search pins the active-only listing for the same reason.
        let search: Vec<(&str, Value)> = table("file_search")
            .fixed_inputs
            .iter()
            .map(|(key, value)| (*key, value.to_json()))
            .collect();
        assert_eq!(search, vec![("fileStatus", Value::from("active"))]);
    }

    // ── Integration: the pack against a mock gateway, end to end. ───────

    /// Discovery serving the committed contracts (source-DERIVED, not
    /// captured — see the module doc's provenance banner), so every mock
    /// registration exercises the fingerprint gate's PASS side — for the
    /// continuation actions too.
    /// The continue actions' INPUT schema, as `additionalProperties:
    /// false` with `cursor` its only property. Serving this — rather than
    /// the `{}` a lazier mock would send — is what makes every
    /// registration below exercise the PASS side of the `cursor_only`
    /// input gate. Derived from the v1.3.5 executor source, like everything
    /// else in this pack; the live pass replaces it with the real
    /// `data.inputSchema`.
    const CURSOR_ONLY_INPUT_SCHEMA: &str = r#"{
        "type": "object",
        "properties": { "cursor": { "type": "string" } },
        "required": ["cursor"],
        "additionalProperties": false
    }"#;

    fn dropbox_discovery(path: &str) -> MockResponse {
        // Each action reads its OWN contract file even though the two
        // continue files are byte-identical to their openers today: they
        // were DERIVED that way, so keeping the files separate is what
        // lets the live capture split them without touching this helper.
        let action = path.rsplit('/').next().unwrap_or_default();
        // Only the continue actions carry a declared input schema here:
        // the openers' inputs are not gated, and leaving them `{}` keeps
        // the difference visible.
        let input_schema = match action {
            "dropbox.list_folder_continue" | "dropbox.search_files_continue" => {
                CURSOR_ONLY_INPUT_SCHEMA
            }
            _ => "{}",
        };
        let output_schema = match action {
            "dropbox.list_folder" => include_str!("fixtures/dropbox/contracts/list_folder.json"),
            "dropbox.list_folder_continue" => {
                include_str!("fixtures/dropbox/contracts/list_folder_continue.json")
            }
            "dropbox.list_shared_links" => {
                include_str!("fixtures/dropbox/contracts/list_shared_links.json")
            }
            "dropbox.search_files" => include_str!("fixtures/dropbox/contracts/search_files.json"),
            "dropbox.search_files_continue" => {
                include_str!("fixtures/dropbox/contracts/search_files_continue.json")
            }
            _ => r#"{"type": "object"}"#,
        };
        MockResponse::ok(&discovery_ok(input_schema, output_schema, true, None))
    }

    fn dropbox_config(token_env: &str, tables: &str) -> OpenConnectorConfig {
        // `query` is required by file_search and declared by no other
        // table; supplying it only when that table is bound keeps the
        // undeclared-resource guard satisfied both ways.
        let resource = if tables.contains("file_search") {
            "resource: { query: redacted }"
        } else {
            ""
        };
        serde_yaml::from_str(&format!(
            r#"
runtime_token_env: {token_env}
bindings:
  - name: ws
    source_pack: dropbox
    {resource}
    tables: [{tables}]
"#
        ))
        .expect("config parses")
    }

    async fn setup_with_gateway(
        gateway: MockGateway,
        token_env: &'static str,
        tables: &str,
    ) -> (MockGateway, SessionContext) {
        let _token = EnvVarGuard::set(token_env, "test-token");
        let gateways = OpenConnectorGateways::default();
        let mut ctx = SessionContext::new();
        register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&dropbox_config(token_env, tables)),
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

    /// Every execute request as `(action path, input object)`.
    fn execute_calls(gateway: &MockGateway) -> Vec<(String, Value)> {
        gateway
            .requests()
            .into_iter()
            .filter(|r| r.method == "POST")
            .map(|r| {
                let body: Value = serde_json::from_str(&r.body).expect("request body is JSON");
                (r.path.clone(), body["input"].clone())
            })
            .collect()
    }

    fn entry(name: &str) -> Value {
        json!({
            "tag": "file", "name": name, "id": format!("id:{name}"),
            "pathDisplay": format!("/{name}"), "pathLower": format!("/{name}"),
            "clientModified": null, "serverModified": null, "rev": null,
            "sizeBytes": null, "isDownloadable": null, "contentHash": null,
            "url": null, "expiresAt": null, "sharingInfo": null,
            "linkPermissions": null
        })
    }

    fn names_of(batches: &[RecordBatch]) -> Vec<String> {
        batches
            .iter()
            .flat_map(|b| {
                utf8(b, "name")
                    .iter()
                    .map(|v| v.expect("name is non-null").to_string())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[tokio::test]
    async fn files_pages_through_the_continue_action_with_only_a_cursor() {
        // THE test this pack exists for. Page one opens the listing on
        // dropbox.list_folder with the pinned complete-collection inputs
        // and the page-size hint; page two goes to
        // dropbox.list_folder_continue carrying the cursor and NOTHING
        // else — against the real gateway anything more is a 400, since
        // the continue action declares `cursor` as its only property under
        // additionalProperties: false.
        // The final page answers hasMore:false with a NON-EMPTY cursor,
        // the Dropbox shape that makes has_more_path load-bearing.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return dropbox_discovery(&req.path);
            }
            match req.path.as_str() {
                "/v1/actions/dropbox.list_folder" => MockResponse::ok(&envelope_ok(
                    &json!({"entries": [entry("a"), entry("b")],
                                "cursor": "cur-2", "hasMore": true})
                    .to_string(),
                )),
                "/v1/actions/dropbox.list_folder_continue" => {
                    let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                    match body["input"].get("cursor").and_then(Value::as_str) {
                        Some("cur-2") => MockResponse::ok(&envelope_ok(
                            // Non-empty cursor on the FINAL page.
                            &json!({"entries": [entry("c")],
                                    "cursor": "cur-3", "hasMore": false})
                            .to_string(),
                        )),
                        other => MockResponse::new(400, format!("bad cursor {other:?}")),
                    }
                }
                _ => MockResponse::new(404, "{}"),
            }
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_DROPBOX_FILES", "files").await;

        let batches = collect(&ctx, "SELECT name FROM saas.ws.files ORDER BY name").await;
        assert_eq!(names_of(&batches), vec!["a", "b", "c"]);

        let calls = execute_calls(&gateway);
        assert_eq!(calls.len(), 2, "two pages");

        let (path, input) = &calls[0];
        assert_eq!(path, "/v1/actions/dropbox.list_folder");
        assert_eq!(input["recursive"], json!(true));
        assert_eq!(input["includeMountedFolders"], json!(true));
        assert_eq!(input["includeDeleted"], json!(false));
        assert_eq!(input["limit"], json!(2000));
        assert!(
            input.get("cursor").is_none(),
            "no cursor exists on page one"
        );

        let (path, input) = &calls[1];
        assert_eq!(
            path, "/v1/actions/dropbox.list_folder_continue",
            "page two targets the CONTINUE action"
        );
        assert_eq!(
            input,
            &json!({"cursor": "cur-2"}),
            "the continue action declares `cursor` as its only property, so \
             anything else here is a 400 on the real wire"
        );
    }

    #[tokio::test]
    async fn shared_links_pages_through_its_own_action_with_the_full_input() {
        // The contrast case: this action takes the cursor itself, so no
        // continuation is declared and page two repeats the action with
        // its resources intact.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return dropbox_discovery(&req.path);
            }
            if req.path == "/v1/actions/dropbox.list_shared_links" {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                let page = match body["input"].get("cursor").and_then(Value::as_str) {
                    None => json!({"links": [entry("l1")], "cursor": "sl-2", "hasMore": true}),
                    // Nulls its cursor at end-of-collection, unlike files.
                    Some("sl-2") => {
                        json!({"links": [entry("l2")], "cursor": null, "hasMore": false})
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
            "SKARDI_TEST_OC_DROPBOX_SHARED_LINKS",
            "shared_links",
        )
        .await;

        let batches = collect(&ctx, "SELECT name FROM saas.ws.shared_links ORDER BY name").await;
        assert_eq!(names_of(&batches), vec!["l1", "l2"]);

        let calls = execute_calls(&gateway);
        assert_eq!(calls.len(), 2);
        for (path, _) in &calls {
            assert_eq!(
                path, "/v1/actions/dropbox.list_shared_links",
                "both pages use the same action"
            );
        }
        assert!(calls[0].1.get("cursor").is_none());
        assert_eq!(calls[1].1["cursor"], json!("sl-2"));
        assert!(
            calls[0].1.get("limit").is_none() && calls[1].1.get("limit").is_none(),
            "this action declares no page-size input"
        );
    }

    #[tokio::test]
    async fn file_search_forwards_its_required_query_and_pinned_status() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return dropbox_discovery(&req.path);
            }
            if req.path == "/v1/actions/dropbox.search_files" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"matches": [{"matchType": "filename", "metadata": entry("hit"),
                                          "highlightSpans": []}],
                            "cursor": null, "hasMore": false})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_DROPBOX_SEARCH", "file_search").await;

        let batches = collect(&ctx, "SELECT name FROM saas.ws.file_search").await;
        assert_eq!(names_of(&batches), vec!["hit"]);

        let calls = execute_calls(&gateway);
        assert_eq!(calls.len(), 1, "a null cursor ends the scan");
        let input = &calls[0].1;
        assert_eq!(input["query"], json!("redacted"), "required resource");
        assert_eq!(input["fileStatus"], json!("active"), "pinned");
        assert_eq!(input["maxResults"], json!(1000));
        assert!(
            input.get("includeHighlights").is_none(),
            "negative space: highlights are never requested"
        );
    }

    #[tokio::test]
    async fn a_missing_required_query_fails_before_any_action_call() {
        // Health is the only call that precedes the guard; no action is
        // ever discovered or executed for a table that cannot be bound.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let _token = EnvVarGuard::set("SKARDI_TEST_OC_DROPBOX_NO_QUERY", "test-token");
        let config: OpenConnectorConfig = serde_yaml::from_str(
            r#"
runtime_token_env: SKARDI_TEST_OC_DROPBOX_NO_QUERY
bindings:
  - name: ws
    source_pack: dropbox
    tables: [file_search]
"#,
        )
        .expect("config parses");
        let mut ctx = SessionContext::new();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&config),
            false,
            HierarchyLevel::Catalog,
            None,
        )
        .await
        .expect_err("a search table without a query must not register");
        assert!(err.to_string().contains("query"), "{err}");
        assert!(
            gateway
                .requests()
                .iter()
                .all(|r| r.path == "/v1/health" && r.method == "GET"),
            "the resource guard runs before any action is discovered or executed"
        );
    }

    #[tokio::test]
    async fn a_drifted_continuation_contract_fails_registration() {
        // The continuation action serves most of a long scan, so its
        // drift must be refused at registration exactly like the opening
        // action's — not discovered on page two of a live query.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                // Only the CONTINUE action drifts; the opener still
                // serves its captured contract and passes the gate.
                if req.path.ends_with("dropbox.list_folder_continue") {
                    return MockResponse::ok(&discovery_ok(
                        "{}",
                        r#"{"type":"object","properties":{"entries":{"type":"array"}}}"#,
                        true,
                        None,
                    ));
                }
                return dropbox_discovery(&req.path);
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let _token = EnvVarGuard::set("SKARDI_TEST_OC_DROPBOX_DRIFT", "test-token");
        let mut ctx = SessionContext::new();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&dropbox_config("SKARDI_TEST_OC_DROPBOX_DRIFT", "files")),
            false,
            HierarchyLevel::Catalog,
            None,
        )
        .await
        .expect_err("a drifted continuation contract must fail registration");
        let rendered = err.to_string();
        assert!(
            rendered.contains("dropbox.files"),
            "names the table: {rendered}"
        );
        assert!(
            rendered.contains("dropbox.list_folder_continue"),
            "names the CONTINUATION action, not the opener: {rendered}"
        );
    }

    #[tokio::test]
    async fn limit_pushdown_stops_the_scan_before_the_continue_action() {
        // A satisfied LIMIT must end the scan on page one — the
        // continuation request is never made.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return dropbox_discovery(&req.path);
            }
            if req.path == "/v1/actions/dropbox.list_folder" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"entries": [entry("a"), entry("b")],
                            "cursor": "cur-2", "hasMore": true})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_DROPBOX_LIMIT", "files").await;

        let batches = collect(&ctx, "SELECT name FROM saas.ws.files LIMIT 1").await;
        assert_eq!(names_of(&batches).len(), 1);

        let calls = execute_calls(&gateway);
        assert_eq!(
            calls.len(),
            1,
            "the LIMIT was satisfied on page one; no continuation fetch"
        );
    }

    #[tokio::test]
    async fn a_gateway_failure_surfaces_the_providers_code() {
        // Dropbox errors at HTTP level (dropboxRpcRequest throws on any
        // non-2xx), so there is no in-band error_path — the provider's
        // code must arrive through the gateway-failure path instead.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return dropbox_discovery(&req.path);
            }
            if req.path == "/v1/actions/dropbox.list_folder" {
                return MockResponse::new(
                    409,
                    crate::sources::providers::open_connector::testutil::envelope_err(
                        "provider_error",
                        "path/not_found",
                    ),
                );
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_DROPBOX_ERR", "files").await;

        let err = ctx
            .sql("SELECT name FROM saas.ws.files")
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("a provider failure must fail the scan");
        let rendered = err.to_string();
        assert!(
            rendered.contains("path/not_found") || rendered.contains("provider_error"),
            "the provider's own code must surface: {rendered}"
        );
        assert!(
            table("files").error_path.is_none(),
            "no in-band error path is declared for Dropbox"
        );
    }

    #[tokio::test]
    async fn file_search_pages_through_its_own_continue_action() {
        // `files`' two-page e2e does not cover this table: the action id,
        // the page-size input (`maxResults`, not `limit`) and the
        // continuation pin are all file_search's own declarations, and
        // shared engine constants are not shared coverage.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return dropbox_discovery(&req.path);
            }
            match req.path.as_str() {
                "/v1/actions/dropbox.search_files" => MockResponse::ok(&envelope_ok(
                    &json!({"matches": [
                                {"matchType": "filename", "metadata": entry("s1"),
                                 "highlightSpans": []}],
                            "cursor": "s-2", "hasMore": true})
                    .to_string(),
                )),
                "/v1/actions/dropbox.search_files_continue" => {
                    let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                    match body["input"].get("cursor").and_then(Value::as_str) {
                        Some("s-2") => MockResponse::ok(&envelope_ok(
                            &json!({"matches": [
                                        {"matchType": "content", "metadata": entry("s2"),
                                         "highlightSpans": []}],
                                    "cursor": null, "hasMore": false})
                            .to_string(),
                        )),
                        other => MockResponse::new(400, format!("bad cursor {other:?}")),
                    }
                }
                _ => MockResponse::new(404, "{}"),
            }
        })
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_DROPBOX_SEARCH_PAGES",
            "file_search",
        )
        .await;

        let batches = collect(&ctx, "SELECT name FROM saas.ws.file_search ORDER BY name").await;
        assert_eq!(names_of(&batches), vec!["s1", "s2"]);

        let calls = execute_calls(&gateway);
        assert_eq!(calls.len(), 2, "two pages");

        let (path, input) = &calls[0];
        assert_eq!(path, "/v1/actions/dropbox.search_files");
        let mut keys: Vec<&str> = input
            .as_object()
            .expect("object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec!["fileStatus", "maxResults", "query"],
            "page one carries EXACTLY the required resource, the pin and the page size"
        );
        assert_eq!(
            input["maxResults"],
            json!(1000),
            "this table's own page size"
        );

        let (path, input) = &calls[1];
        assert_eq!(
            path, "/v1/actions/dropbox.search_files_continue",
            "page two targets THIS table's continue action"
        );
        assert_eq!(
            input,
            &json!({"cursor": "s-2"}),
            "no query, no fileStatus, no maxResults — the continue action declares none of them"
        );
    }

    #[tokio::test]
    async fn shared_links_forwards_both_of_its_optional_resources() {
        // `directOnly` is the pack's only boolean resource, so this also
        // pins that resource values keep their YAML type: a string "true"
        // would be rejected by the action's strict schema.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return dropbox_discovery(&req.path);
            }
            if req.path == "/v1/actions/dropbox.list_shared_links" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"links": [entry("l1")], "cursor": null, "hasMore": false}).to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let _token = EnvVarGuard::set("SKARDI_TEST_OC_DROPBOX_RESOURCES", "test-token");
        let config: OpenConnectorConfig = serde_yaml::from_str(
            r#"
runtime_token_env: SKARDI_TEST_OC_DROPBOX_RESOURCES
bindings:
  - name: ws
    source_pack: dropbox
    resource:
      path: /Redacted Folder/redacted-report.pdf
      directOnly: true
    tables: [shared_links]
"#,
        )
        .expect("config parses");
        let mut ctx = SessionContext::new();
        let gateways = OpenConnectorGateways::default();
        register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&config),
            false,
            HierarchyLevel::Catalog,
            Some(&gateways),
        )
        .await
        .expect("registration succeeds");

        let batches = collect(&ctx, "SELECT name FROM saas.ws.shared_links").await;
        assert_eq!(names_of(&batches), vec!["l1"]);

        let calls = execute_calls(&gateway);
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].1,
            json!({"path": "/Redacted Folder/redacted-report.pdf", "directOnly": true}),
            "both resources forward verbatim, and directOnly is a JSON boolean"
        );
    }

    #[tokio::test]
    async fn a_files_page_without_the_has_more_signal_fails_the_scan() {
        // `has_more_path` is called load-bearing in three places in this
        // pack; this is the failure arm of that declaration reached through
        // SQL, not through the engine's own unit tests. A page missing the
        // signal must fail as contract drift — never terminate quietly,
        // which is how a truncated scan would look green.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return dropbox_discovery(&req.path);
            }
            if req.path == "/v1/actions/dropbox.list_folder" {
                // A well-formed page in every respect EXCEPT the declared
                // has-more flag.
                return MockResponse::ok(&envelope_ok(
                    &json!({"entries": [entry("a")], "cursor": "cur-2"}).to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_DROPBOX_NO_HAS_MORE", "files").await;

        let err = ctx
            .sql("SELECT name FROM saas.ws.files")
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("a page missing the declared has-more signal must fail");
        let rendered = err.to_string();
        assert!(
            rendered.contains("$.hasMore") && rendered.contains("absent"),
            "the error names the declared path and what was found: {rendered}"
        );
        assert_eq!(
            execute_calls(&gateway).len(),
            1,
            "the scan stops at the drifted page instead of continuing"
        );
    }

    #[tokio::test]
    async fn a_drifted_opening_contract_fails_registration_for_every_table() {
        // The continuation-drift case is covered above; this is the arm
        // each table owns, so no table rides another's pin.
        for (short, action) in [
            ("files", "dropbox.list_folder"),
            ("shared_links", "dropbox.list_shared_links"),
            ("file_search", "dropbox.search_files"),
        ] {
            let drifted = action.to_string();
            let gateway = MockGateway::start(move |req| {
                if req.method == "GET" && req.path == "/v1/health" {
                    return MockResponse::ok("{}");
                }
                if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                    if req.path.ends_with(&drifted) {
                        return MockResponse::ok(&discovery_ok(
                            "{}",
                            r#"{"type":"object","properties":{"drifted":{"type":"string"}}}"#,
                            true,
                            None,
                        ));
                    }
                    return dropbox_discovery(&req.path);
                }
                MockResponse::new(404, "{}")
            })
            .await;
            let env = format!("SKARDI_TEST_OC_DROPBOX_DRIFT_{}", short.to_uppercase());
            let _token = EnvVarGuard::set(&env, "test-token");
            let mut ctx = SessionContext::new();
            let err = register_open_connector_tables(
                &mut ctx,
                "saas",
                &gateway.url,
                Some(&dropbox_config(&env, short)),
                false,
                HierarchyLevel::Catalog,
                None,
            )
            .await
            .expect_err("a drifted opening contract must fail registration");
            let rendered = err.to_string();
            assert!(
                rendered.contains(&format!("dropbox.{short}")) && rendered.contains(action),
                "{short}: error must name the table and the drifted action: {rendered}"
            );
        }
    }

    #[tokio::test]
    async fn a_continue_action_demanding_more_than_a_cursor_fails_registration() {
        // The fail arm of the `cursor_only` INPUT gate, reached through the
        // public registration entry point. The fingerprint cannot catch
        // this — the output schema is untouched — and without the gate the
        // only symptom is a 400 on page two of a live scan.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                if req.path.ends_with("dropbox.list_folder_continue") {
                    // Same OUTPUT schema as the captured contract, so the
                    // fingerprint gate passes; the INPUT schema now also
                    // requires `path`, which a cursor-only page never sends.
                    return MockResponse::ok(&discovery_ok(
                        r#"{"type":"object",
                             "properties":{"cursor":{"type":"string"},"path":{"type":"string"}},
                             "required":["cursor","path"],
                             "additionalProperties":false}"#,
                        include_str!("fixtures/dropbox/contracts/list_folder_continue.json"),
                        true,
                        None,
                    ));
                }
                return dropbox_discovery(&req.path);
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let _token = EnvVarGuard::set("SKARDI_TEST_OC_DROPBOX_CURSOR_ONLY", "test-token");
        let mut ctx = SessionContext::new();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&dropbox_config(
                "SKARDI_TEST_OC_DROPBOX_CURSOR_ONLY",
                "files",
            )),
            false,
            HierarchyLevel::Catalog,
            None,
        )
        .await
        .expect_err("a continue action requiring more than the cursor must fail registration");
        let rendered = err.to_string();
        assert!(
            rendered.contains("dropbox.list_folder_continue") && rendered.contains("path"),
            "the error names the continuation action and the unsatisfiable input: {rendered}"
        );
    }

    #[tokio::test]
    async fn a_continue_action_without_an_input_schema_is_refused() {
        // Default-deny on an unverifiable claim, mirroring how raw scans
        // treat a missing read/write classification: `cursor_only` asserts
        // something about inputs, and a gateway that declares no inputs
        // cannot confirm it.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                if req.path.ends_with("dropbox.list_folder_continue") {
                    return MockResponse::ok(&discovery_ok(
                        "{}",
                        include_str!("fixtures/dropbox/contracts/list_folder_continue.json"),
                        true,
                        None,
                    ));
                }
                return dropbox_discovery(&req.path);
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let _token = EnvVarGuard::set("SKARDI_TEST_OC_DROPBOX_NO_INPUT_SCHEMA", "test-token");
        let mut ctx = SessionContext::new();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&dropbox_config(
                "SKARDI_TEST_OC_DROPBOX_NO_INPUT_SCHEMA",
                "files",
            )),
            false,
            HierarchyLevel::Catalog,
            None,
        )
        .await
        .expect_err("an unverifiable cursor_only claim must be refused");
        assert!(
            err.to_string().contains("no input schema"),
            "the error says why it cannot be verified: {err}"
        );
    }

    #[tokio::test]
    async fn udtf_parity_for_files() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return dropbox_discovery(&req.path);
            }
            if req.path == "/v1/actions/dropbox.list_folder" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"entries": [entry("via-udtf")], "cursor": "c", "hasMore": false})
                        .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_DROPBOX_UDTF", "files").await;

        let batches = collect(
            &ctx,
            "SELECT name FROM open_connector_query('saas', 'dropbox.files', '{}')",
        )
        .await;
        assert_eq!(names_of(&batches), vec!["via-udtf"]);
    }
}
