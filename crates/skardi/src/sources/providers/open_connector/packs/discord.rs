//! Discord source pack (milestone 5.5): the current OAuth user's guilds,
//! external-account connections, and the public Nitro sticker-pack
//! catalog, as SQL tables.
//!
//! ## What this provider can and cannot see
//!
//! Open Connector's `discord` provider is the OAuth **user-identity**
//! surface: its own `get_user` executor rejects any id but `@me`, so
//! everything here is the authorizing user's view. Channels, messages,
//! and guild members are Discord **bot-token** surface the provider does
//! not carry — those tables cannot exist in this pack, by provider
//! scope, not by deferral.
//!
//! ## Wire shape (reconciled against a live gateway, 2026-08-07)
//!
//! Every list executor passes Discord's API objects through RAW
//! (GitHub-style: `.then((guilds) => ({guilds}))` in the gateway's
//! `executors.ts`) under a snake_case envelope key, and emits **no
//! pagination envelope at all**. Inputs are strict
//! (`additionalProperties: false`) — validated without credentials via
//! the 403-vs-400 probe: valid shapes (`limit: 200`,
//! `after` + `with_counts`) reach the authorization wall, while a stray
//! key, `limit: 300`, or an empty-string snowflake are hard 400s.
//!
//! ## Design decisions
//!
//! - **`guilds` paginates by KEYSET** (the engine strategy this pack
//!   introduced): Discord's `/users/@me/guilds` takes
//!   `after = <last guild id>` and publishes no cursor of its own. A
//!   single page at the 200 cap would coincidentally also cover today's
//!   account limit (200 joined guilds with Nitro), but that equality is
//!   a coincidence, not a contract — keyset keeps the scan complete and
//!   terminating if either cap ever moves, where single-page would
//!   silently truncate at exactly the moment it matters.
//! - **`with_counts: true` is a fixed input**: the `approximate_*`
//!   columns exist only when the request asks for them; a column whose
//!   presence depends on request shape would be a per-scan schema coin
//!   flip, so the pack pins the richer shape.
//! - **`connection_type`, not `type`**: the wire key collides with a SQL
//!   keyword and would force quoting into every query that touches it.
//! - **`connections` and `sticker_packs` are single_page**: neither API
//!   paginates — connections are bounded by the fixed set of connectable
//!   platforms, and the sticker catalog is one small public collection.
//! - **`entitlements` is DEFERRED, not shipped incomplete**: Discord's
//!   entitlements API paginates (`before`/`after`/`limit`), but the
//!   gateway's executor exposes only `exclude_ended`/`exclude_deleted` —
//!   first-page-only through no fault of a pack. Filed upstream; the
//!   table joins when the executor grows the pagination inputs.
//! - **`error_path: None`**: the provider's executors consume Discord's
//!   error responses themselves and return the gateway failure envelope;
//!   nothing in-band reaches the row path.
//!
//! ## Column status
//!
//! Item schemas are declared LOOSE by the gateway (empty `properties` +
//! `additionalProperties: true`; `sticker_packs`' whole output schema is
//! a bare raw object), so **no column here is protected by the
//! fingerprint gate** — the pinned fingerprints freeze the envelope
//! contract only, and real rows are the only column truth. Fixtures are
//! DRAFT (documented Discord resource shapes) pending this milestone's
//! live pass.

use std::sync::OnceLock;

use crate::sources::providers::open_connector::error::OpenConnectorError;
use crate::sources::providers::open_connector::source_pack::SourcePack;

use super::loader;

static PACK: OnceLock<Result<SourcePack, String>> = OnceLock::new();

/// The Discord pack, parsed once from the embedded YAML asset.
pub fn pack() -> Result<&'static SourcePack, OpenConnectorError> {
    loader::builtin("discord.yaml", include_str!("discord.yaml"), &PACK)
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
        OpenConnectorConfig, register_open_connector_tables,
    };
    use arrow::array::{Array, BooleanArray, ListArray, StringArray, UInt64Array};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use serde_json::{Value, json};

    fn table(short: &str) -> &'static SourcePackTable {
        pack()
            .expect("embedded asset is test-pinned to parse")
            .tables
            .iter()
            .find(|t| t.id.rsplit('.').next() == Some(short))
            .unwrap_or_else(|| panic!("table {short}"))
    }

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

    fn boolean<'a>(batch: &'a RecordBatch, name: &str) -> &'a BooleanArray {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("column {name}"))
            .as_any()
            .downcast_ref()
            .expect("Boolean column")
    }

    fn uint64<'a>(batch: &'a RecordBatch, name: &str) -> &'a UInt64Array {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("column {name}"))
            .as_any()
            .downcast_ref()
            .expect("UInt64 column")
    }

    // ── Contract tests: bundled fixtures are the build-time conversion
    // contract (null-bearing, absent-key, empty-list, nested, and a
    // schema mismatch per the admission gate). DRAFT: synthetic shapes,
    // re-derived from redacted live captures in the real-data phase. ────

    #[test]
    fn guilds_fixture_converts_raw_passthrough_with_counts() {
        let batch = convert_fixture(
            table("guilds"),
            include_str!("fixtures/discord/guilds.json"),
        );
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(utf8(&batch, "id").value(0), "100000000000000001");
        assert_eq!(utf8(&batch, "name").value(1), "Redacted Guild Two");
        // Present-null and absent both decode to SQL NULL: row 0 carries
        // `banner: null`, row 1 omits the key entirely.
        assert!(utf8(&batch, "banner").is_null(0));
        assert!(utf8(&batch, "banner").is_null(1));
        assert!(!utf8(&batch, "icon").is_null(0));
        assert!(utf8(&batch, "icon").is_null(1));
        assert!(boolean(&batch, "owner").value(0));
        // The permission bitfield stays a verbatim decimal string —
        // Discord serializes it as a string because it exceeds JSON's
        // safe-integer range.
        assert_eq!(utf8(&batch, "permissions").value(0), "2251799813685247");
        // features: a populated list and an EMPTY list (not NULL).
        let features = batch
            .column_by_name("features")
            .expect("features column")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("List column");
        assert_eq!(features.value(0).len(), 2);
        assert_eq!(features.value(1).len(), 0);
        assert!(!features.is_null(1), "an empty list is not NULL");
        assert_eq!(uint64(&batch, "approximate_member_count").value(0), 42);
        assert_eq!(uint64(&batch, "approximate_presence_count").value(1), 1);
    }

    #[test]
    fn connections_fixture_converts_and_renames_the_type_keyword() {
        let batch = convert_fixture(
            table("connections"),
            include_str!("fixtures/discord/connections.json"),
        );
        assert_eq!(batch.num_rows(), 2);
        // The SQL-keyword collision is resolved at the COLUMN name; the
        // wire key stays `type`.
        assert_eq!(utf8(&batch, "connection_type").value(0), "github");
        assert_eq!(utf8(&batch, "connection_type").value(1), "steam");
        assert!(boolean(&batch, "verified").value(0));
        // `revoked` is present only on revoked connections: absent on the
        // live row, `true` on the revoked one.
        assert!(boolean(&batch, "revoked").is_null(0));
        assert!(boolean(&batch, "revoked").value(1));
        assert_eq!(uint64(&batch, "visibility").value(1), 0);
        assert_eq!(uint64(&batch, "metadata_visibility").value(0), 1);
    }

    #[test]
    fn sticker_packs_fixture_converts_with_opaque_stickers_json() {
        let batch = convert_fixture(
            table("sticker_packs"),
            include_str!("fixtures/discord/sticker_packs.json"),
        );
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(utf8(&batch, "name").value(0), "Wumpus Beyond");
        // Present-null description; absent cover/banner ids.
        assert!(utf8(&batch, "description").is_null(1));
        assert!(utf8(&batch, "cover_sticker_id").is_null(1));
        assert!(utf8(&batch, "banner_asset_id").is_null(1));
        // Sticker objects survive as opaque JSON, including the empty set.
        let stickers: Value =
            serde_json::from_str(utf8(&batch, "stickers").value(0)).expect("valid JSON");
        assert_eq!(stickers.as_array().map(Vec::len), Some(1));
        let empty: Value =
            serde_json::from_str(utf8(&batch, "stickers").value(1)).expect("valid JSON");
        assert_eq!(empty.as_array().map(Vec::len), Some(0));
    }

    #[test]
    fn a_number_where_utf8_is_declared_fails_with_row_identity() {
        // The admission gate's schema-mismatch case: drift into a declared
        // column fails with the full identity, never a quiet null and
        // never the offending value.
        let page: Value =
            serde_json::from_str(include_str!("fixtures/discord/guilds_type_mismatch.json"))
                .expect("fixture parses");
        let t = table("guilds");
        let rows = RowPath::parse(t.row_path)
            .expect("row path")
            .rows(&page, 1)
            .expect("row array");
        let err = RowConverter::new(t.fields)
            .expect("converter")
            .convert(rows, 1)
            .expect_err("a number where Utf8 is declared must fail conversion");
        match err {
            OpenConnectorError::ConversionFailed {
                column,
                page,
                row,
                found,
                ..
            } => {
                assert_eq!(column, "id");
                assert_eq!(page, 1);
                assert_eq!(row, 0);
                assert_eq!(found, "number");
            }
            other => panic!("expected ConversionFailed, got {other}"),
        }
    }

    #[test]
    fn pinned_fingerprints_match_the_reconciled_contracts() {
        // Pin <-> captured-contract lock through the SAME function
        // registration uses; the mismatch output is also how pins are
        // (re)taken after an upstream upgrade.
        for (short, contract) in [
            (
                "guilds",
                include_str!("fixtures/discord/contracts/list_my_guilds.json"),
            ),
            (
                "connections",
                include_str!("fixtures/discord/contracts/list_my_connections.json"),
            ),
            (
                "sticker_packs",
                include_str!("fixtures/discord/contracts/list_sticker_packs.json"),
            ),
        ] {
            let schema: Value = serde_json::from_str(contract).expect("contract parses");
            assert_eq!(
                table(short).expected_fingerprint,
                Some(fingerprint_schema(Some(&schema)).as_str()),
                "{short}: pinned fingerprint must match the captured contract"
            );
        }
    }

    #[test]
    fn the_fingerprint_coverage_gap_is_every_column() {
        // Loose item schemas (and sticker_packs' completely loose output)
        // mean the fingerprint gate protects the ENVELOPE only. Pinning
        // the gap makes it a reviewed fact instead of an implicit one.
        for (short, contract) in [
            (
                "guilds",
                include_str!("fixtures/discord/contracts/list_my_guilds.json"),
            ),
            (
                "connections",
                include_str!("fixtures/discord/contracts/list_my_connections.json"),
            ),
            (
                "sticker_packs",
                include_str!("fixtures/discord/contracts/list_sticker_packs.json"),
            ),
        ] {
            let t = table(short);
            let all_columns: Vec<&str> = t.fields.iter().map(|f| f.name).collect();
            assert_eq!(
                fingerprint_uncovered_columns(contract, t.row_path, t.fields),
                all_columns,
                "every {short} column is expected to be uncovered (loose item schema)"
            );
        }
    }

    // ── Integration: the pack against a mock gateway, end to end. ───────

    fn discord_config(token_env: &str, tables: &str) -> OpenConnectorConfig {
        serde_yaml::from_str(&format!(
            r#"
runtime_token_env: {token_env}
bindings:
  - name: me
    source_pack: discord
    tables: [{tables}]
"#
        ))
        .expect("config parses")
    }

    fn discord_discovery(path: &str) -> MockResponse {
        let output_schema = if path.ends_with("discord.list_my_guilds") {
            include_str!("fixtures/discord/contracts/list_my_guilds.json")
        } else if path.ends_with("discord.list_my_connections") {
            include_str!("fixtures/discord/contracts/list_my_connections.json")
        } else if path.ends_with("discord.list_sticker_packs") {
            include_str!("fixtures/discord/contracts/list_sticker_packs.json")
        } else {
            r#"{"type": "object"}"#
        };
        MockResponse::ok(&discovery_ok("{}", output_schema, true, None))
    }

    async fn setup_with_gateway(
        gateway: MockGateway,
        token_env: &'static str,
        tables: &str,
    ) -> (MockGateway, SessionContext) {
        let _token = EnvVarGuard::set(token_env, "test-token");
        let mut ctx = SessionContext::new();
        register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&discord_config(token_env, tables)),
            false,
            HierarchyLevel::Catalog,
            None,
        )
        .await
        .expect("gateway registration succeeds");
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

    fn execute_inputs(gateway: &MockGateway) -> Vec<Value> {
        gateway
            .requests()
            .into_iter()
            .filter(|r| r.method == "POST")
            .map(|r| {
                serde_json::from_str::<Value>(&r.body).expect("request body is JSON")["input"]
                    .clone()
            })
            .collect()
    }

    fn sorted_keys(input: &Value) -> Vec<String> {
        let mut keys: Vec<String> = input
            .as_object()
            .expect("input object")
            .keys()
            .cloned()
            .collect();
        keys.sort_unstable();
        keys
    }

    fn guild_row(id: &str) -> Value {
        json!({
            "id": id,
            "name": format!("guild {id}"),
            "owner": false,
            "permissions": "0",
            "features": [],
            "approximate_member_count": 1,
            "approximate_presence_count": 0
        })
    }

    #[tokio::test]
    async fn guilds_keyset_scan_walks_pages_and_terminates_on_the_short_page() {
        // Page 1 is exactly FULL (200 rows), so the scan must continue
        // from the last row's id; page 2 is short, ending the walk. This
        // is the multi-page shape a single-page design would truncate.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return discord_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/discord.list_my_guilds" {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                let rows: Vec<Value> = match body["input"]["after"].as_str() {
                    None => (1..=200).map(|i| guild_row(&format!("g-{i:04}"))).collect(),
                    Some("g-0200") => vec![guild_row("g-0201")],
                    Some(other) => panic!("unexpected after cursor {other}"),
                };
                return MockResponse::ok(&envelope_ok(&json!({ "guilds": rows }).to_string()));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_DISCORD_KEYSET", "guilds").await;

        let batches = collect(&ctx, "SELECT id FROM saas.me.guilds").await;
        // Row identity, not just cardinality: the exact ids of both pages
        // survive, in wire order, with no duplicate and no boundary drop.
        let ids: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                let col: &StringArray = b.column(0).as_any().downcast_ref().expect("Utf8 ids");
                (0..col.len())
                    .map(|i| col.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        let expected: Vec<String> = (1..=201).map(|i| format!("g-{i:04}")).collect();
        assert_eq!(ids, expected, "both pages scanned, boundary row intact");

        let inputs = execute_inputs(&gateway);
        assert_eq!(inputs.len(), 2, "exactly two pages requested");
        // Exact input key sets: page 1 carries no cursor; the fixed input
        // and the page size ride every request.
        assert_eq!(sorted_keys(&inputs[0]), vec!["limit", "with_counts"]);
        assert_eq!(inputs[0]["limit"], 200, "declared page size rides the wire");
        assert_eq!(inputs[0]["with_counts"], true, "fixed input pinned");
        assert_eq!(
            sorted_keys(&inputs[1]),
            vec!["after", "limit", "with_counts"]
        );
        assert_eq!(
            inputs[1]["after"], "g-0200",
            "the cursor is the previous page's LAST row id"
        );
    }

    #[tokio::test]
    async fn connections_single_page_sends_no_pagination_inputs() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return discord_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/discord.list_my_connections" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"connections": [
                        {"id": "c-1", "name": "n", "type": "github", "verified": true,
                         "friend_sync": false, "show_activity": true, "two_way_link": false,
                         "visibility": 1}
                    ]})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_DISCORD_CONN", "connections").await;

        let batches = collect(&ctx, "SELECT connection_type FROM saas.me.connections").await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

        let inputs = execute_inputs(&gateway);
        assert_eq!(inputs.len(), 1, "single_page = exactly one request");
        // The EXACT key set is empty: the action declares no inputs, and
        // single_page injects none — a stray key would be a strict-schema
        // 400 on the real gateway.
        assert_eq!(sorted_keys(&inputs[0]), Vec::<String>::new());
    }

    #[tokio::test]
    async fn drifted_contract_fails_registration_not_the_scan() {
        // The fingerprint gate's fail side: discovery serving a schema
        // other than the pinned contract refuses at registration.
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
        let _token = EnvVarGuard::set("SKARDI_TEST_OC_DISCORD_DRIFT", "test-token");
        let mut ctx = SessionContext::new();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&discord_config("SKARDI_TEST_OC_DISCORD_DRIFT", "guilds")),
            false,
            HierarchyLevel::Catalog,
            None,
        )
        .await
        .expect_err("a drifted contract must refuse registration");
        let message = err.to_string();
        assert!(
            message.contains("fingerprint") || message.contains("contract"),
            "the refusal names the gate: {message}"
        );
    }
}
