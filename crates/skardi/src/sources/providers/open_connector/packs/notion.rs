//! Notion source pack: stable relational contracts over the Open Connector
//! `notion.*` read actions (integration token, cursor pagination).
//!
//! **The wire contract is Open Connector's raw passthrough of the Notion
//! API.** The notion executors return Notion's response bodies verbatim
//! (`return payload ?? {}` — no normalization, no post-pagination
//! filtering), so rows are raw Notion objects under `$.results` with the
//! native cursor envelope beside them: `$.next_cursor` (null at
//! end-of-collection) and `has_more` (redundant with the null cursor and
//! deliberately unused). Inputs are Open Connector's camelCase strict
//! schema: `startCursor` / `pageSize` / `blockId` — not Notion's own
//! `start_cursor` / `page_size` query names. Everything below is
//! reconciled against a live gateway and the OC provider source.
//!
//! Design decisions, per the integration design spec and the source-pack
//! admission gate:
//!
//! - **Cursor pagination on every table** (`startCursor` in, top-level
//!   `$.next_cursor` out, page size 100 — Notion's maximum). Termination
//!   is complete on the null-cursor spelling the API documents; a
//!   repeated cursor fails as `PaginationLoop`, a non-string cursor as
//!   `PaginationCursorInvalid` — never a silent truncation.
//! - **`pages` and `data_sources` are the complete visible listing** via
//!   `notion.search` with the empty required `query` pinned to `""` and
//!   an object `filter` pin (`{property: object, value: page |
//!   data_source}`) — the `state=all` move, and the reason
//!   `FixedValue::Json` exists: the search filter is an object, which no
//!   scalar fixed input could express. Visibility is exactly what the
//!   Open Connector integration has been shared with.
//! - **No filter pushdown anywhere.** `notion.search`'s only narrowing
//!   input is the free-text relevance `query`, which no SQL predicate
//!   maps to faithfully; `list_users` / `list_block_children` declare no
//!   filter inputs at all. Every predicate runs in DataFusion after the
//!   bounded fetch, and a guard test pins that requests carry exactly
//!   the declared inputs and nothing else.
//! - **Dynamic property maps stay opaque JSON.** A page's `properties`
//!   (and a data source's schema) are user-defined per workspace; typed
//!   projection requires the design's binding-time schema freeze for
//!   `notion.query_data_source`, which is deferred — this pack ships the
//!   static-schema tables only, and documents the rows table as absent.
//!   `block_children` likewise excludes the type-specific payload (it
//!   lives under a key named BY `type`, unaddressable by a fixed
//!   mapping); rendered content belongs to a future markdown table.
//! - **`users` excludes `person.email`** (and the raw `person`/`bot`
//!   objects): capability-gated on the integration and privacy-sensitive
//!   — same call the Slack pack made.
//! - **Nullability is conservative**: only `id` is non-null. Notion nulls
//!   or omits most metadata fields depending on object age and API
//!   version; both spellings become SQL NULL, pinned by fixtures.
//! - **Fingerprints are pinned** from a live gateway capture
//!   (`fixtures/notion/contracts/`); `pages` and `data_sources` share
//!   `notion.search`'s contract and therefore its pin. The declared
//!   search item schema is EMPTY (`results.items` declares no
//!   properties), so every mapped column of the two search tables rides
//!   `additionalProperties` passthrough — the coverage-gap pin records
//!   that honestly; their drift surfaces at scan time per conversion
//!   rules.

use std::sync::OnceLock;

use crate::sources::providers::open_connector::error::OpenConnectorError;
use crate::sources::providers::open_connector::source_pack::SourcePack;

use super::loader;

static PACK: OnceLock<Result<SourcePack, String>> = OnceLock::new();

/// The Notion pack, parsed once from the embedded YAML asset.
pub fn pack() -> Result<&'static SourcePack, OpenConnectorError> {
    loader::builtin("notion.yaml", include_str!("notion.yaml"), &PACK)
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
        EnvVarGuard, MockGateway, MockResponse, RecordedRequest, discovery_ok, envelope_ok,
        fingerprint_uncovered_columns,
    };
    use crate::sources::providers::open_connector::{
        OpenConnectorConfig, OpenConnectorGateways, register_open_connector_tables,
        register_open_connector_udtfs,
    };
    use arrow::array::{Array, BooleanArray, ListArray, StringArray, TimestampMillisecondArray};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use serde_json::{Value, json};

    /// Look up a table by short name; the assets are test-pinned to parse.
    fn table(
        short: &str,
    ) -> &'static crate::sources::providers::open_connector::source_pack::SourcePackTable {
        pack()
            .expect("embedded asset is test-pinned to parse")
            .tables
            .iter()
            .find(|t| t.id.rsplit('.').next() == Some(short))
            .unwrap_or_else(|| panic!("table {short}"))
    }

    /// Discovery serving the live-captured contracts, so every mock
    /// registration exercises the fingerprint gate's pass side.
    fn notion_discovery(path: &str) -> MockResponse {
        let output_schema = if path.ends_with("notion.list_users") {
            include_str!("fixtures/notion/contracts/list_users.json")
        } else if path.ends_with("notion.search") {
            include_str!("fixtures/notion/contracts/search.json")
        } else if path.ends_with("notion.list_block_children") {
            include_str!("fixtures/notion/contracts/list_block_children.json")
        } else {
            r#"{"type": "object"}"#
        };
        MockResponse::ok(&discovery_ok("{}", output_schema, true, None))
    }

    // ── Contract tests: bundled redacted fixtures are the build-time
    // conversion contract (null-bearing, nested, empty, extra upstream
    // fields, and a schema mismatch per the source-pack admission gate). ─

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

    #[test]
    fn users_fixture_converts_with_nulls_and_omissions() {
        let batch = convert_fixture(table("users"), include_str!("fixtures/notion/users.json"));
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(utf8(&batch, "id").value(0), "u-0001");
        assert_eq!(utf8(&batch, "name").value(0), "Ada Lovelace");
        // Explicit null and omitted key both land as SQL NULL.
        assert!(utf8(&batch, "name").is_null(1));
        assert!(utf8(&batch, "avatar_url").is_null(1));
        assert!(utf8(&batch, "avatar_url").is_null(2));
        assert_eq!(utf8(&batch, "type").value(1), "bot");
    }

    #[test]
    fn pages_fixture_converts_with_dynamic_properties_as_json() {
        let batch = convert_fixture(table("pages"), include_str!("fixtures/notion/pages.json"));
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(utf8(&batch, "id").value(0), "p-0001");
        assert!(!boolean(&batch, "archived").value(0));
        assert!(boolean(&batch, "archived").value(1));
        assert!(utf8(&batch, "public_url").is_null(0));
        // Dynamic property map survives as opaque JSON text; JSON null →
        // SQL NULL.
        let properties: Value =
            serde_json::from_str(utf8(&batch, "properties").value(0)).expect("valid JSON");
        assert_eq!(properties["title"]["type"], "title");
        assert!(utf8(&batch, "properties").is_null(1));
        let ts: &TimestampMillisecondArray = batch
            .column_by_name("last_edited_time")
            .expect("column")
            .as_any()
            .downcast_ref()
            .expect("timestamp");
        assert!(!ts.is_null(0));
        assert!(ts.is_null(1));
    }

    #[test]
    fn data_sources_fixture_converts_with_title_plucking() {
        let batch = convert_fixture(
            table("data_sources"),
            include_str!("fixtures/notion/data_sources.json"),
        );
        assert_eq!(batch.num_rows(), 2);
        let titles: &ListArray = batch
            .column_by_name("title")
            .expect("column")
            .as_any()
            .downcast_ref()
            .expect("List column");
        let first = titles.value(0);
        let first = first
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 items");
        assert_eq!(
            (0..first.len()).map(|i| first.value(i)).collect::<Vec<_>>(),
            vec!["Tasks", " DB"]
        );
        assert_eq!(titles.value(1).len(), 0, "empty title list stays empty");
    }

    #[test]
    fn block_children_fixture_converts_without_type_payload() {
        let batch = convert_fixture(
            table("block_children"),
            include_str!("fixtures/notion/block_children.json"),
        );
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(utf8(&batch, "type").value(1), "heading_1");
        assert!(boolean(&batch, "has_children").value(1));
        assert!(boolean(&batch, "archived").is_null(1));
    }

    #[test]
    fn users_mismatch_fixture_fails_with_the_targeted_error() {
        // Admission-gate schema-mismatch fixture: a number where Utf8 is
        // declared fails with the full row-scoped identity, never a quiet
        // null and never the offending value.
        let page: Value =
            serde_json::from_str(include_str!("fixtures/notion/users_type_mismatch.json"))
                .expect("fixture parses");
        let t = table("users");
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
                assert_eq!(row, 1, "the valid first row converts");
                assert_eq!(found, "number");
            }
            other => panic!("expected ConversionFailed, got {other}"),
        }
    }

    #[test]
    fn pinned_fingerprints_match_the_reconciled_contracts() {
        // Pin <-> captured-contract lock through the SAME function
        // registration uses; mismatch output is also how pins are
        // (re)taken after an upstream upgrade. pages and data_sources
        // share notion.search's contract, hence one pin.
        let contracts = [
            (
                "users",
                include_str!("fixtures/notion/contracts/list_users.json"),
            ),
            (
                "pages",
                include_str!("fixtures/notion/contracts/search.json"),
            ),
            (
                "data_sources",
                include_str!("fixtures/notion/contracts/search.json"),
            ),
            (
                "block_children",
                include_str!("fixtures/notion/contracts/list_block_children.json"),
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
    fn fingerprint_coverage_gap_is_pinned() {
        // notion.search declares an EMPTY results-item schema, so every
        // mapped column of the two search tables rides
        // additionalProperties passthrough — outside the fingerprint
        // gate, drift surfacing at scan time. users/block_children are
        // partially declared. Pinned so any change is a conscious
        // decision.
        for (short, contract, expected) in [
            (
                "users",
                include_str!("fixtures/notion/contracts/list_users.json"),
                &[] as &[&str],
            ),
            (
                "pages",
                include_str!("fixtures/notion/contracts/search.json"),
                &[
                    "id",
                    "created_time",
                    "last_edited_time",
                    "archived",
                    "in_trash",
                    "url",
                    "public_url",
                    "parent",
                    "properties",
                ],
            ),
            (
                "data_sources",
                include_str!("fixtures/notion/contracts/search.json"),
                &[
                    "id",
                    "created_time",
                    "last_edited_time",
                    "title",
                    "archived",
                    "url",
                    "parent",
                    "properties",
                ],
            ),
            (
                "block_children",
                include_str!("fixtures/notion/contracts/list_block_children.json"),
                &["created_time", "last_edited_time", "archived"],
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

    // ── Integration: the pack against a mock gateway, end to end. ───────

    fn notion_config(token_env: &str, tables: &str) -> OpenConnectorConfig {
        // blockId is declared only by block_children; sending it to a
        // binding without that table trips the undeclared-resource guard.
        let resource = if tables.contains("block_children") {
            "resource: { blockId: b-root }"
        } else {
            ""
        };
        serde_yaml::from_str(&format!(
            r#"
runtime_token_env: {token_env}
bindings:
  - name: ws
    source_pack: notion
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
            Some(&notion_config(token_env, tables)),
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

    fn ids_of(batches: &[RecordBatch]) -> Vec<String> {
        batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column_by_name("id")
                    .expect("id column")
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Utf8 ids")
                    .clone();
                (0..ids.len()).map(move |i| ids.value(i).to_string())
            })
            .collect()
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

    fn user_row(id: &str) -> Value {
        json!({"object": "user", "id": id, "name": id, "type": "person"})
    }

    #[tokio::test]
    async fn users_cursor_scan_pages_with_its_own_declared_inputs() {
        // Two-page cursor scan pinning USERS' wire declarations: no
        // startCursor on page 1, the stub's token afterwards, pageSize
        // 100 on every request, null-cursor termination, row identity
        // across pages.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return notion_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/notion.list_users" {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                let page = match body["input"].get("startCursor").and_then(Value::as_str) {
                    None => json!({"results": [user_row("u-1"), user_row("u-2")],
                                    "next_cursor": "cur-2", "has_more": true}),
                    Some("cur-2") => json!({"results": [user_row("u-3")],
                                             "next_cursor": null, "has_more": false}),
                    Some(other) => return MockResponse::new(400, &format!("bad cursor {other}")),
                };
                return MockResponse::ok(&envelope_ok(&page.to_string()));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_NOTION_USERS", "users").await;

        let batches = collect(&ctx, "SELECT id FROM saas.ws.users ORDER BY id").await;
        assert_eq!(ids_of(&batches), vec!["u-1", "u-2", "u-3"]);

        let inputs = execute_inputs(&gateway);
        assert_eq!(inputs.len(), 2, "two cursor pages");
        assert!(inputs[0].get("startCursor").is_none(), "{}", inputs[0]);
        assert_eq!(inputs[1]["startCursor"], "cur-2");
        for input in &inputs {
            assert_eq!(input["pageSize"], 100, "page-size hint: {input}");
        }
    }

    #[tokio::test]
    async fn search_tables_pin_their_object_filters_on_the_wire() {
        // pages and data_sources share notion.search but pin DIFFERENT
        // object filters — and the empty query — on every request. The
        // stub honors the filter, proving each table sees only its kind;
        // requests carry exactly the declared inputs and nothing else
        // (the no-pushdown guard).
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return notion_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/notion.search" {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                let kind = body["input"]["filter"]["value"].as_str().unwrap_or("");
                let rows = match kind {
                    "page" => json!([{"object": "page", "id": "p-1"}]),
                    "data_source" => json!([{"object": "data_source", "id": "ds-1"}]),
                    other => return MockResponse::new(400, &format!("bad filter {other}")),
                };
                return MockResponse::ok(&envelope_ok(
                    &json!({"results": rows, "next_cursor": null, "has_more": false}).to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_NOTION_SEARCH",
            "pages, data_sources",
        )
        .await;

        let batches = collect(&ctx, "SELECT id FROM saas.ws.pages").await;
        assert_eq!(ids_of(&batches), vec!["p-1"]);
        let batches = collect(&ctx, "SELECT id FROM saas.ws.data_sources").await;
        assert_eq!(ids_of(&batches), vec!["ds-1"]);

        for input in execute_inputs(&gateway) {
            assert_eq!(input["query"], "", "empty query pin: {input}");
            assert_eq!(input["filter"]["property"], "object", "{input}");
            let mut keys: Vec<&str> = input
                .as_object()
                .expect("input object")
                .keys()
                .map(String::as_str)
                .collect();
            keys.sort_unstable();
            assert_eq!(
                keys,
                vec!["filter", "pageSize", "query"],
                "exactly the declared inputs, nothing else (page 1 has no cursor)"
            );
        }
    }

    #[tokio::test]
    async fn block_children_forwards_its_required_resource() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return notion_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/notion.list_block_children" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"results": [{"object": "block", "id": "b-1", "type": "paragraph"}],
                             "next_cursor": null, "has_more": false})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_NOTION_BLOCKS", "block_children").await;

        let batches = collect(&ctx, "SELECT id, type FROM saas.ws.block_children").await;
        assert_eq!(ids_of(&batches), vec!["b-1"]);
        let inputs = execute_inputs(&gateway);
        assert_eq!(
            inputs[0]["blockId"], "b-root",
            "resource forwarded verbatim"
        );
    }

    #[tokio::test]
    async fn missing_required_resource_fails_before_any_http() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let _token = EnvVarGuard::set("SKARDI_TEST_OC_NOTION_NO_RES", "test-token");
        let config: OpenConnectorConfig = serde_yaml::from_str(
            r#"
runtime_token_env: SKARDI_TEST_OC_NOTION_NO_RES
bindings:
  - name: ws
    source_pack: notion
    tables: [block_children]
"#,
        )
        .expect("config parses");
        let mut ctx = SessionContext::new();
        let gateways = OpenConnectorGateways::default();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&config),
            false,
            HierarchyLevel::Catalog,
            Some(&gateways),
        )
        .await
        .expect_err("missing blockId must fail registration");
        assert!(err.to_string().contains("blockId"), "{err}");
        assert!(
            gateway.requests().iter().all(|r| r.path == "/v1/health"),
            "resource enforcement precedes discovery"
        );
    }

    #[tokio::test]
    async fn limit_stops_cursor_pagination_early() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return notion_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/notion.list_users" {
                // Every page advertises another; only LIMIT can stop this.
                return MockResponse::ok(&envelope_ok(
                    &json!({"results": [user_row("u-1"), user_row("u-2")],
                             "next_cursor": "again", "has_more": true})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_NOTION_LIMIT", "users").await;

        let batches = collect(&ctx, "SELECT id FROM saas.ws.users LIMIT 2").await;
        assert_eq!(ids_of(&batches).len(), 2);
        assert_eq!(
            execute_inputs(&gateway).len(),
            1,
            "one page satisfied LIMIT"
        );
    }

    #[tokio::test]
    async fn udtf_parity_for_block_children() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return notion_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/notion.list_block_children" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"results": [{"object": "block", "id": "b-9", "type": "toggle"}],
                             "next_cursor": null, "has_more": false})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_NOTION_UDTF", "block_children").await;

        let from_table = collect(&ctx, "SELECT id, type FROM saas.ws.block_children").await;
        let from_udtf = collect(
            &ctx,
            "SELECT id, type FROM open_connector_query('saas', 'notion.block_children', \
             '{\"blockId\":\"b-root\"}')",
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
        // The pin's refusal side: a gateway whose discovered output schema
        // differs from the captured contract is refused at REGISTRATION,
        // table and action named. (Every other e2e proves the pass side
        // via notion_discovery's captured contracts.)
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
        let _token = EnvVarGuard::set("SKARDI_TEST_OC_NOTION_DRIFT", "test-token");
        let mut ctx = SessionContext::new();
        let gateways = OpenConnectorGateways::default();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&notion_config("SKARDI_TEST_OC_NOTION_DRIFT", "users")),
            false,
            HierarchyLevel::Catalog,
            Some(&gateways),
        )
        .await
        .expect_err("a drifted contract must fail registration");
        let message = err.to_string();
        assert!(
            message.contains("notion.users")
                && message.contains("notion.list_users")
                && message.contains("fingerprint mismatch"),
            "table, action, and cause are named: {message}"
        );
    }
}
