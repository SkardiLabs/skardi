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
//!   `after = <last guild id>` and publishes no cursor of its own; only
//!   an EMPTY page terminates (short pages continue, so a silent
//!   page-size clamp cannot read as completion). A single page at the
//!   200 cap would coincidentally also cover today's account limit
//!   (200 joined guilds with Nitro), but that equality is a
//!   coincidence, not a contract — keyset keeps the scan complete and
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
//!   first-page-only through no fault of a pack. Filed upstream
//!   (oomol-lab/open-connector#283); the table joins when the executor
//!   grows the pagination inputs.
//! - **`error_path: None`**: the provider's executors consume Discord's
//!   error responses themselves and return the gateway failure envelope;
//!   nothing in-band reaches the row path.
//! - **`permissions` maps the wire key `permissions_new`** (live-pass
//!   correction): the gateway calls the UNVERSIONED `discord.com/api`,
//!   which Discord serves as its legacy default version — there
//!   `permissions` is a truncated NUMBER and `permissions_new` carries
//!   the full bitfield as a decimal string. The draft mapped
//!   `permissions` as utf8 and failed conversion on every real row.
//!   Version-coupled risk, documented in the pack doc: if the gateway
//!   ever pins `/api/v10` (where `permissions` IS the string and
//!   `permissions_new` is gone), this mapping breaks — the loose item
//!   schemas mean no fingerprint can catch that move, so the column is
//!   declared NON-nullable (the legacy API attaches `permissions_new`
//!   to every guild object; 6/6 live rows) and the converter's
//!   missing-key failure, with full row identity, is the tripwire.
//!
//! ## Column status (live pass, 2026-08-07)
//!
//! Item schemas are declared LOOSE by the gateway (empty `properties` +
//! `additionalProperties: true`; `sticker_packs`' whole output schema is
//! a bare raw object), so **no column here is protected by the
//! fingerprint gate** — the pinned fingerprints freeze the envelope
//! contract only, and real rows are the only column truth. `guilds` and
//! `sticker_packs` are live-verified end to end through skardi-server
//! against a real account (registration through LIVE discovery, every
//! mapped column non-NULL on real rows, real keyset walk `limit: 2`
//! over 3 full pages + the empty terminator, no duplicate and no
//! boundary drop, ascending-snowflake ordering confirmed); their
//! fixtures are redacted live captures. `connections` is live-verified
//! on a real linked account (1 row through skardi-server: all nine wire
//! keys present and mapped, the `connection_type` rename extracting,
//! `revoked` genuinely ABSENT on a non-revoked row — its non-NULL arm
//! rides a synthetic fixture row, since capturing it live would mean
//! revoking a real account link). Discord rate-limits these routes
//! aggressively (rapid probes hit HTTP 429, surfaced loudly by the
//! gateway).

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
        OpenConnectorConfig, OpenConnectorGateways, register_open_connector_tables,
        register_open_connector_udtfs,
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
    // schema mismatch per the admission gate). guilds, sticker_packs,
    // and connections row 1 are REDACTED LIVE CAPTURES (2026-08-07):
    // real key sets and spellings, synthetic ids/names/hashes/permission
    // bits, lists truncated. The live wire always carries every guild
    // key (with_counts pinned), so absent-key coverage rides
    // connections' `revoked` — genuinely absent on the live row.
    // connections row 2 (the revoked arm) and the schema-mismatch
    // fixture stay synthetic by design: capturing a real revoked row
    // would mean revoking a real account link. ──────────────────────────

    #[test]
    fn guilds_fixture_converts_raw_passthrough_with_counts() {
        let batch = convert_fixture(
            table("guilds"),
            include_str!("fixtures/discord/guilds.json"),
        );
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(utf8(&batch, "id").value(0), "100000000000000001");
        assert_eq!(utf8(&batch, "name").value(1), "Redacted Guild Two");
        // The wire's null spelling is present-null (`banner: null`), never
        // an omitted key.
        assert!(!utf8(&batch, "banner").is_null(0));
        assert!(utf8(&batch, "banner").is_null(1));
        assert!(!utf8(&batch, "icon").is_null(0));
        assert!(!boolean(&batch, "owner").value(0));
        // LIVE-PASS CORRECTION pinned: the column reads `permissions_new`
        // (the full bitfield as a decimal string) — the legacy sibling
        // `permissions` is a NUMBER on this fixture row, and mapping it
        // as utf8 failed on every real row.
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
        assert_eq!(uint64(&batch, "approximate_member_count").value(0), 43);
        assert_eq!(uint64(&batch, "approximate_presence_count").value(1), 2);
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
        // Public Nitro catalog rows, captured live (stickers truncated to
        // one element each): every optional field is populated on the
        // real catalog, so the null arms of the four doc-derived nullable
        // columns are declared in the yaml, not exercised here.
        let batch = convert_fixture(
            table("sticker_packs"),
            include_str!("fixtures/discord/sticker_packs.json"),
        );
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(utf8(&batch, "name").value(0), "Mallow The Rascal");
        assert_eq!(utf8(&batch, "name").value(1), "Wumpus Beyond");
        assert!(!utf8(&batch, "description").is_null(1));
        assert!(!utf8(&batch, "cover_sticker_id").is_null(0));
        assert!(!utf8(&batch, "banner_asset_id").is_null(0));
        assert!(!utf8(&batch, "sku_id").is_null(0));
        // Sticker objects survive as opaque JSON, and the embedded
        // pack_id stays self-consistent with the row's own id.
        for row in 0..2 {
            let stickers: Value =
                serde_json::from_str(utf8(&batch, "stickers").value(row)).expect("valid JSON");
            let arr = stickers.as_array().expect("array");
            assert_eq!(arr.len(), 1);
            assert_eq!(
                arr[0]["pack_id"],
                Value::String(utf8(&batch, "id").value(row).to_string()),
                "sticker pack_id cross-reference is self-consistent"
            );
        }
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
    fn person_linked_fixtures_stay_redacted() {
        // guilds and connections are the person-linked fixtures (the
        // authorizing user's guild MEMBERSHIP and linked-account lists).
        // Mechanical audit: every string leaf must satisfy the redaction
        // allowlist FOR ITS KEY, default-deny. Key scoping is what makes
        // the tripwire enforce anything: a real snowflake is digit-only
        // and a real guild name can be ALL_CAPS, so shape arms alone
        // would wave both through — ids MUST carry the synthetic prefix,
        // names MUST carry the placeholder marker, and a key this list
        // has never seen fails loudly instead of coasting on a shape
        // coincidence. sticker_packs is deliberately NOT here: it is a
        // public catalog kept verbatim.
        fn audit(name: &str, key: &str, value: &Value) {
            match value {
                Value::String(s) => {
                    let digits = !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
                    let allowed = match key {
                        // Snowflakes must be synthetic — a real id is
                        // digit-only too, so digit shape proves nothing.
                        "id" => s.starts_with("10000000000000000"),
                        // The only keys where a bare digit string is a
                        // public platform value, not an identifier
                        // (permissions_new on real rows; permissions in
                        // the type-mismatch fixture's string-where-number
                        // probe).
                        "permissions" | "permissions_new" => digits,
                        "name" => s.starts_with("Redacted "),
                        "icon" | "banner" => {
                            s == "0123456789abcdef0123456789abcdef"
                                || s == "fedcba9876543210fedcba9876543210"
                        }
                        // Feature flags are public platform constants;
                        // array items arrive under the parent key.
                        "features" => {
                            !s.is_empty() && s.bytes().all(|b| b.is_ascii_uppercase() || b == b'_')
                        }
                        // Public platform enums a connection's `type` takes.
                        "type" => ["github", "steam"].contains(&s.as_str()),
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
        for (name, text) in [
            ("guilds", include_str!("fixtures/discord/guilds.json")),
            (
                "guilds_type_mismatch",
                include_str!("fixtures/discord/guilds_type_mismatch.json"),
            ),
            (
                "connections",
                include_str!("fixtures/discord/connections.json"),
            ),
        ] {
            let root: Value = serde_json::from_str(text).expect("fixture parses");
            audit(name, "$", &root);
        }

        // The tripwire must TRIP — each probe is a leak class a fixture
        // re-capture could plausibly reintroduce, and each must panic.
        for (key, leak) in [
            ("id", "81384788765712384"),   // real snowflake, no prefix
            ("id", ""),                    // vacuous all-digits
            ("name", "MY_REAL_GUILD"),     // ALL_CAPS real name
            ("owner_tag", "someone#1234"), // key the allowlist never saw
        ] {
            let probe = serde_json::json!({ key: leak });
            assert!(
                std::panic::catch_unwind(|| audit("probe", "$", &probe)).is_err(),
                "audit must reject {key} = {leak:?}"
            );
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
        let gateways = OpenConnectorGateways::default();
        let mut ctx = SessionContext::new();
        register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&discord_config(token_env, tables)),
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
        // Wire-faithful shape: legacy `permissions` is a NUMBER and
        // `permissions_new` carries the bitfield string the (now
        // non-nullable) `permissions` column maps.
        json!({
            "id": id,
            "name": format!("guild {id}"),
            "owner": false,
            "permissions": 0,
            "permissions_new": "0",
            "features": [],
            "approximate_member_count": 1,
            "approximate_presence_count": 0
        })
    }

    #[tokio::test]
    async fn guilds_keyset_scan_walks_pages_and_terminates_on_the_empty_page() {
        // Page 1 is full (200 rows) and page 2 is SHORT — but only the
        // EMPTY page 3 ends the walk (short pages continue: a page-size
        // clamp must not read as completion). This is the multi-page
        // shape a single-page design would truncate.
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
                    Some("g-0201") => vec![],
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
        // Three requests: two row pages plus the terminating empty page —
        // the standard keyset tax.
        assert_eq!(inputs.len(), 3, "two row pages plus the empty terminator");
        // Exact input key sets: page 1 carries no cursor; the fixed input
        // and the page size ride every request.
        assert_eq!(sorted_keys(&inputs[0]), vec!["limit", "with_counts"]);
        assert_eq!(inputs[0]["limit"], 200, "declared page size rides the wire");
        assert_eq!(inputs[0]["with_counts"], true, "fixed input pinned");
        for (input, after) in [(&inputs[1], "g-0200"), (&inputs[2], "g-0201")] {
            assert_eq!(sorted_keys(input), vec!["after", "limit", "with_counts"]);
            assert_eq!(
                input["after"], after,
                "the cursor is the previous page's LAST row id"
            );
        }
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
    async fn sticker_packs_single_page_scan_pins_its_own_wire_declarations() {
        // Per-declaration coverage: shared constants are not shared
        // coverage — this table's OWN row path, action ID, and exact
        // (empty) input set get their own e2e.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return discord_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/discord.list_sticker_packs" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"sticker_packs": [
                        {"id": "sp-2", "sku_id": "sku-2", "name": "Pack Two",
                         "description": "d", "cover_sticker_id": "cs-2",
                         "banner_asset_id": "ba-2", "stickers": [{"id": "st-9"}]},
                        {"id": "sp-1", "sku_id": "sku-1", "name": "Pack One",
                         "description": "d", "cover_sticker_id": "cs-1",
                         "banner_asset_id": "ba-1", "stickers": []}
                    ]})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_DISCORD_STICKERS", "sticker_packs").await;

        // Row identity in wire order, not cardinality.
        let batches = collect(&ctx, "SELECT id FROM saas.me.sticker_packs").await;
        let ids: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                let col: &StringArray = b.column(0).as_any().downcast_ref().expect("Utf8 ids");
                (0..col.len())
                    .map(|i| col.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(ids, ["sp-2", "sp-1"]);

        let inputs = execute_inputs(&gateway);
        assert_eq!(inputs.len(), 1, "single_page = exactly one request");
        assert_eq!(sorted_keys(&inputs[0]), Vec::<String>::new());
    }

    #[tokio::test]
    async fn udtf_parity_for_sticker_packs() {
        // The bound table and the ad-hoc UDTF spelling of the same action
        // must agree on schema and rows.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return discord_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/discord.list_sticker_packs" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"sticker_packs": [
                        {"id": "sp-1", "sku_id": "sku-1", "name": "Pack One",
                         "description": null, "cover_sticker_id": "cs-1",
                         "banner_asset_id": "ba-1", "stickers": [{"id": "st-1"}]}
                    ]})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_DISCORD_UDTF", "sticker_packs").await;

        let from_table = collect(&ctx, "SELECT * FROM saas.me.sticker_packs").await;
        let from_udtf = collect(
            &ctx,
            "SELECT * FROM open_connector_query('saas', 'discord.sticker_packs', '{}')",
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

    #[test]
    fn empty_pages_preserve_every_table_schema() {
        // A zero-row response is a complete result, not a schema change —
        // connections proved this live (the account had no links at first
        // scan): every declared column must exist on the empty batch.
        for (table, empty) in [
            (table("guilds"), r#"{"guilds":[]}"#),
            (table("connections"), r#"{"connections":[]}"#),
            (table("sticker_packs"), r#"{"sticker_packs":[]}"#),
        ] {
            let batch = convert_fixture(table, empty);
            assert_eq!(batch.num_rows(), 0);
            assert_eq!(batch.num_columns(), table.fields.len(), "{}", table.id);
        }
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
