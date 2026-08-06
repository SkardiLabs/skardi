//! Feishu source pack: stable relational contracts over the Open Connector
//! `feishu.*` read actions (OAuth user_access_token, cursor pagination).
//!
//! **The wire contract is Open Connector's HYBRID shape.** Every feishu
//! list executor rebuilds the pagination envelope — camelCase `$.items` /
//! `$.pageToken` / `$.hasMore`, with the provider's `page_token`
//! normalized to a null `pageToken` at end-of-collection — while passing
//! the Feishu API's item objects through RAW (`optionalObjectArray(...)`
//! / `Array.isArray(...)` in the executors, no per-item rebuild). Rows
//! therefore keep Feishu's snake_case keys and its epoch-digit-STRING
//! timestamps: millis for im and tasks (`create_time: "1609296809000"`,
//! `completed_at: "1780322529638"`), seconds for wiki
//! (`obj_create_time: "1642402790"`) — the reason
//! `TimestampMillisStringUtc` / `TimestampSecondsStringUtc` exist.
//! Inputs are Open Connector's camelCase strict schemas (`pageSize`,
//! `pageToken` with `minLength: 1`, `chatId`, …). Everything below is
//! reconciled against a live gateway (v1.3.3) and the OC provider
//! source.
//!
//! Design decisions, per the integration design spec and the source-pack
//! admission gate:
//!
//! - **Cursor pagination on every table** (`pageToken` in, top-level
//!   `$.pageToken` out; page size 100 for chats/chat_members/tasks, 50
//!   for wiki — and 50 for `messages`, whose REAL wire cap is 50 despite
//!   the schema's declared 100: Feishu hard-fails larger values with
//!   99992402, live-verified 2026-08-04), with `$.hasMore` declared as
//!   the AUTHORITATIVE
//!   termination signal (`has_more_path`). That is a live-verification
//!   correction, not a nicety: Feishu's wiki space listing answers its
//!   final page with `has_more: false` beside a NON-empty `page_token`
//!   (`"0||…"`, captured 2026-08-04), so the null-token spelling alone
//!   would refetch a finished scan and die as `PaginationLoop`. With the
//!   signal declared, `hasMore: true` without a usable token is contract
//!   drift (`PaginationCursorInvalid`), a non-boolean signal is
//!   `PaginationHasMoreInvalid` — never a silent truncation. No feishu
//!   executor filters fetched pages, so the signals are undamaged.
//! - **Orderings are pinned for cursor stability**: `chats` and
//!   `messages` pin `sortType: ByCreateTimeAsc` because the API's
//!   activity-ordered default reshuffles rows mid-scan (skips and
//!   duplicates across pages); creation order is immutable.
//! - **`messages` is chat history**: `containerIdType` pinned to `chat`
//!   (threads are a different action), the chat itself a required
//!   binding resource — the per-container shape Notion's
//!   `block_children` established. One filter pushes: `create_time >=`
//!   → `startTime`, inclusive epoch seconds typed as a digit STRING in
//!   the strict schema (the reason `ValueFormat::EpochSecondsString`
//!   exists), `Inexact` so DataFusion re-trims the floored bound.
//!   `endTime` is deliberately unmapped: it is EXCLUSIVE, and flooring
//!   an upper bound drops rows.
//! - **`tasks` pins `type: my_tasks`** (the executor default, declared
//!   rather than inherited) and omits the action's `completed` input —
//!   Feishu's state=all. Live rows (2026-08-04) carry NO `completed`
//!   boolean: completion is `status` (todo|done) plus `completed_at`,
//!   whose "not completed" spelling is the digit string "0" (epoch-zero
//!   sentinel, documented at the column). The `completed` input is
//!   therefore deliberately unmapped — a `status` string cannot render
//!   into a boolean input without a value transform the filter engine
//!   deliberately lacks, and a mapping on an always-NULL draft column
//!   would have re-trimmed every row to zero.
//! - **`wiki_nodes` lists ONE level** (children of `parentNodeToken`,
//!   space root when omitted) — the action's own shape; full-tree
//!   traversal is client-side recursion, documented in the pack doc.
//! - **In-band provider errors never reach rows**: executors throw on
//!   Feishu's non-zero `code` envelope, so the gateway returns a failure
//!   envelope and `error_path` is `None` for every table.
//! - **Fingerprints are pinned** from the live capture
//!   (`fixtures/feishu/contracts/`), but the gateway declares every
//!   item schema LOOSE (`additionalProperties: true`, zero declared
//!   properties) — so ALL mapped columns ride passthrough outside the
//!   fingerprint gate, and the coverage-gap pin records that honestly.
//!   Column truth is therefore settled ONLY by real rows, and ALL SIX
//!   tables are reconciled against a live workspace (2026-08-04; every
//!   fixture is a redacted live capture). What the pass changed: chats
//!   gained `chat_mode`/`chat_status`; tasks lost the nonexistent
//!   `completed` boolean for `status`/`completed_at`; wiki tables
//!   gained `open_sharing`/`creator`/`url`; messages' page size dropped
//!   to the real 50 cap. Two operational findings the pack doc records:
//!   reading messages under the user identity requires the
//!   `im:message:readonly` (or `im:message` /
//!   `im:message.history:readonly`) scope — the `get_as_user` scopes
//!   the gateway's actions declare are NOT honored for this path
//!   (99991679 names the real set) — plus the app's bot capability
//!   (232025). `message_position` (a digit string on every live row) is
//!   deliberately unmapped: no public Feishu doc pins its semantics.
//!   Live e2e evidence: 86 messages over two real cursor pages with
//!   zero duplicate ids; the `create_time >=` pushdown narrowing a
//!   live scan; wiki's non-empty final token terminating cleanly.

use std::sync::OnceLock;

use crate::sources::providers::open_connector::error::OpenConnectorError;
use crate::sources::providers::open_connector::source_pack::SourcePack;

use super::loader;

static PACK: OnceLock<Result<SourcePack, String>> = OnceLock::new();

/// The Feishu pack, parsed once from the embedded YAML asset.
pub fn pack() -> Result<&'static SourcePack, OpenConnectorError> {
    loader::builtin("feishu.yaml", include_str!("feishu.yaml"), &PACK)
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
    use arrow::array::{Array, BooleanArray, StringArray, TimestampMillisecondArray};
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

    /// Discovery serving the live-captured contracts, so every mock
    /// registration exercises the fingerprint gate's pass side.
    fn feishu_discovery(path: &str) -> MockResponse {
        let output_schema = if path.ends_with("feishu.list_chats") {
            include_str!("fixtures/feishu/contracts/list_chats.json")
        } else if path.ends_with("feishu.list_messages") {
            include_str!("fixtures/feishu/contracts/list_messages.json")
        } else if path.ends_with("feishu.list_chat_members") {
            include_str!("fixtures/feishu/contracts/list_chat_members.json")
        } else if path.ends_with("feishu.list_tasks") {
            include_str!("fixtures/feishu/contracts/list_tasks.json")
        } else if path.ends_with("feishu.list_wiki_spaces") {
            include_str!("fixtures/feishu/contracts/list_wiki_spaces.json")
        } else if path.ends_with("feishu.list_wiki_nodes") {
            include_str!("fixtures/feishu/contracts/list_wiki_nodes.json")
        } else {
            r#"{"type": "object"}"#
        };
        MockResponse::ok(&discovery_ok("{}", output_schema, true, None))
    }

    // ── Contract tests: bundled fixtures are the build-time conversion
    // contract (null-bearing, nested, empty, extra upstream fields, and a
    // schema mismatch per the admission gate). DRAFT status: synthetic,
    // re-derived from live captures in the real-data phase. ─────────────

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

    fn millis<'a>(batch: &'a RecordBatch, name: &str) -> &'a TimestampMillisecondArray {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("column {name}"))
            .as_any()
            .downcast_ref()
            .expect("Timestamp column")
    }

    #[test]
    fn chats_fixture_converts_live_shapes() {
        // Redacted live capture (2026-08-04): an assistant chat without an
        // owner, an internal group with an EMPTY description, and a topic
        // chat.
        let batch = convert_fixture(table("chats"), include_str!("fixtures/feishu/chats.json"));
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(utf8(&batch, "id").value(0), "oc_0001");
        // The assistant chat carries no owner_id at all — absence lands as
        // SQL NULL while its neighbors keep theirs.
        assert!(utf8(&batch, "owner_id").is_null(0));
        assert!(!utf8(&batch, "owner_id").is_null(1));
        // An empty description is DATA, preserved verbatim, never coerced
        // to NULL.
        assert_eq!(utf8(&batch, "description").value(1), "");
        assert_eq!(utf8(&batch, "chat_mode").value(2), "topic");
        assert_eq!(utf8(&batch, "chat_status").value(0), "normal");
        assert!(boolean(&batch, "external").value(0));
        assert!(!boolean(&batch, "external").value(1));
    }

    #[test]
    fn messages_fixture_converts_digit_string_times_and_nested_content() {
        // Redacted live capture (2026-08-04), one row per verified field
        // family: a system message (empty-string sender ids — data, not
        // NULL), a reply (root_id), a topic-group message (thread_id),
        // and an @-mention (mentions array).
        let batch = convert_fixture(
            table("messages"),
            include_str!("fixtures/feishu/messages.json"),
        );
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(utf8(&batch, "id").value(0), "om_0002");
        // Epoch-millis digit strings become real timestamps.
        assert_eq!(millis(&batch, "create_time").value(0), 1_593_441_000_000);
        // body.content stays the provider's own JSON-encoded string, whose
        // inner shape varies by msg_type (a system template here).
        let content: Value =
            serde_json::from_str(utf8(&batch, "content").value(0)).expect("valid inner JSON");
        assert!(content.get("template").is_some(), "system-message payload");
        // sender survives as opaque JSON; a system message's sender ids
        // are EMPTY STRINGS on the wire, preserved verbatim.
        let sender: Value =
            serde_json::from_str(utf8(&batch, "sender").value(0)).expect("valid JSON");
        assert_eq!(sender["sender_type"], "");
        assert_eq!(
            serde_json::from_str::<Value>(utf8(&batch, "sender").value(1)).unwrap()["sender_type"],
            "user"
        );
        // Reply / thread / mention fields: absent on most rows (SQL NULL),
        // real on the rows that carry them.
        assert!(utf8(&batch, "root_id").is_null(0));
        assert_eq!(utf8(&batch, "root_id").value(1), "om_0005");
        assert_eq!(utf8(&batch, "thread_id").value(2), "omt_0009");
        assert!(utf8(&batch, "mentions").is_null(0));
        let mentions: Value =
            serde_json::from_str(utf8(&batch, "mentions").value(3)).expect("valid JSON");
        assert_eq!(mentions[0]["key"], "@_user_1");
    }

    #[test]
    fn tasks_fixture_converts_status_and_the_epoch_zero_sentinel() {
        // Redacted live capture (2026-08-04): completion is `status`
        // (todo|done) plus `completed_at` — there is NO `completed`
        // boolean on the wire, which is why that draft column is gone.
        let batch = convert_fixture(table("tasks"), include_str!("fixtures/feishu/tasks.json"));
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(utf8(&batch, "status").value(0), "todo");
        assert_eq!(utf8(&batch, "status").value(2), "done");
        // Feishu spells "not completed" as "0" — the epoch-zero sentinel
        // the YAML documents; a done task carries a real instant.
        assert_eq!(millis(&batch, "completed_at").value(0), 0);
        assert_eq!(millis(&batch, "completed_at").value(2), 1_780_322_529_638);
        assert_eq!(millis(&batch, "created_at").value(0), 1_775_202_331_888);
        // No fixture row carries a `due` key at all (the live pass saw it
        // on 2 of 9 rows; none survived the redacted subset) — ABSENT
        // decodes to SQL NULL; the column itself is doc-derived, as the
        // YAML notes. members survive as opaque JSON.
        assert!(utf8(&batch, "due").is_null(0));
        let members: Value =
            serde_json::from_str(utf8(&batch, "members").value(0)).expect("valid JSON");
        assert!(members.is_array());
    }

    #[test]
    fn fixtures_stay_redacted_one_json_level_deep() {
        // Round-2 review blind spot: real member names survived inside the
        // JSON-encoded `body.content` payload — strings one decode level
        // BELOW the outer tree the redaction pass walked. Two tripwires:
        // no CJK text anywhere in any feishu fixture (the live workspace's
        // real names were Chinese), and every membership entry inside a
        // decoded message payload is a `member-NNNN` placeholder.
        let fixtures = [
            (
                "chat_members",
                include_str!("fixtures/feishu/chat_members.json"),
            ),
            ("chats", include_str!("fixtures/feishu/chats.json")),
            (
                "chats_type_mismatch",
                include_str!("fixtures/feishu/chats_type_mismatch.json"),
            ),
            ("messages", include_str!("fixtures/feishu/messages.json")),
            ("tasks", include_str!("fixtures/feishu/tasks.json")),
            (
                "wiki_nodes",
                include_str!("fixtures/feishu/wiki_nodes.json"),
            ),
            (
                "wiki_spaces",
                include_str!("fixtures/feishu/wiki_spaces.json"),
            ),
        ];
        for (name, text) in fixtures {
            assert!(
                !text.chars().any(|c| ('\u{4e00}'..='\u{9fff}').contains(&c)),
                "{name}.json: CJK text survived redaction"
            );
        }

        let messages: Value =
            serde_json::from_str(include_str!("fixtures/feishu/messages.json")).expect("json");
        let mut audited = 0;
        for item in messages["items"].as_array().expect("items") {
            let Some(content) = item["body"]["content"].as_str() else {
                continue;
            };
            let Ok(inner) = serde_json::from_str::<Value>(content) else {
                continue;
            };
            for key in ["from_user", "to_chatters"] {
                let Some(entries) = inner.get(key).and_then(Value::as_array) else {
                    continue;
                };
                for entry in entries {
                    let member = entry.as_str().unwrap_or_default();
                    let placeholder = member
                        .strip_prefix("member-")
                        .is_some_and(|d| !d.is_empty() && d.bytes().all(|b| b.is_ascii_digit()));
                    assert!(
                        placeholder,
                        "messages.json: '{member}' in {key} is not a member-NNNN placeholder"
                    );
                    audited += 1;
                }
            }
        }
        assert!(audited >= 3, "the audit reached the nested payloads");
    }

    #[test]
    fn wiki_nodes_fixture_converts_epoch_second_strings() {
        // Redacted live capture (2026-08-04).
        let batch = convert_fixture(
            table("wiki_nodes"),
            include_str!("fixtures/feishu/wiki_nodes.json"),
        );
        assert_eq!(batch.num_rows(), 3);
        // Epoch-SECONDS digit strings scale to millis.
        assert_eq!(
            millis(&batch, "obj_create_time").value(0),
            1_636_114_726_000
        );
        assert!(!boolean(&batch, "has_child").value(0));
        // Feishu spells "no parent" as the empty string — that is data,
        // preserved verbatim, not coerced to NULL.
        assert_eq!(utf8(&batch, "parent_node_token").value(0), "");
        assert!(utf8(&batch, "creator").value(0).starts_with("ou_"));
        assert!(utf8(&batch, "url").value(0).starts_with("https://"));
    }

    #[test]
    fn chat_members_and_wiki_spaces_fixtures_convert() {
        let members = convert_fixture(
            table("chat_members"),
            include_str!("fixtures/feishu/chat_members.json"),
        );
        // Redacted live capture (2026-08-04): the wire carries exactly the
        // four mapped columns, every one populated.
        assert_eq!(members.num_rows(), 2);
        assert_eq!(utf8(&members, "member_id").value(0), "ou_0003");
        assert_eq!(utf8(&members, "member_id_type").value(0), "open_id");
        assert!(!utf8(&members, "name").is_null(1));

        let spaces = convert_fixture(
            table("wiki_spaces"),
            include_str!("fixtures/feishu/wiki_spaces.json"),
        );
        // Redacted live capture (2026-08-04): note the envelope carries a
        // NON-empty pageToken beside hasMore:false — the shape the
        // has_more_path termination exists for, pinned end to end by
        // `wiki_spaces_terminate_on_has_more_false_despite_a_token`.
        assert_eq!(spaces.num_rows(), 1);
        assert_eq!(utf8(&spaces, "space_id").value(0), "700000038");
        assert_eq!(utf8(&spaces, "open_sharing").value(0), "closed");
    }

    #[test]
    fn chats_mismatch_fixture_fails_with_the_targeted_error() {
        // Admission-gate schema-mismatch fixture: a number where Utf8 is
        // declared fails with the full row-scoped identity, never a quiet
        // null and never the offending value.
        let page: Value =
            serde_json::from_str(include_str!("fixtures/feishu/chats_type_mismatch.json"))
                .expect("fixture parses");
        let t = table("chats");
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
        // registration uses; the mismatch output is also how pins are
        // (re)taken after an upstream upgrade.
        let contracts = [
            (
                "chats",
                include_str!("fixtures/feishu/contracts/list_chats.json"),
            ),
            (
                "messages",
                include_str!("fixtures/feishu/contracts/list_messages.json"),
            ),
            (
                "chat_members",
                include_str!("fixtures/feishu/contracts/list_chat_members.json"),
            ),
            (
                "tasks",
                include_str!("fixtures/feishu/contracts/list_tasks.json"),
            ),
            (
                "wiki_spaces",
                include_str!("fixtures/feishu/contracts/list_wiki_spaces.json"),
            ),
            (
                "wiki_nodes",
                include_str!("fixtures/feishu/contracts/list_wiki_nodes.json"),
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
        // The gateway declares every feishu items schema LOOSE (zero
        // declared properties, additionalProperties: true), so EVERY
        // mapped column of EVERY table rides passthrough — outside the
        // fingerprint gate, drift surfacing at scan time per conversion
        // rules. Pinned so any change is a conscious decision; the
        // real-data phase is what actually vouches for these columns.
        for short in [
            "chats",
            "messages",
            "chat_members",
            "tasks",
            "wiki_spaces",
            "wiki_nodes",
        ] {
            let t = table(short);
            let contract = match short {
                "chats" => include_str!("fixtures/feishu/contracts/list_chats.json"),
                "messages" => include_str!("fixtures/feishu/contracts/list_messages.json"),
                "chat_members" => include_str!("fixtures/feishu/contracts/list_chat_members.json"),
                "tasks" => include_str!("fixtures/feishu/contracts/list_tasks.json"),
                "wiki_spaces" => include_str!("fixtures/feishu/contracts/list_wiki_spaces.json"),
                "wiki_nodes" => include_str!("fixtures/feishu/contracts/list_wiki_nodes.json"),
                other => panic!("table {other}"),
            };
            let all_columns: Vec<&str> = t.fields.iter().map(|f| f.name).collect();
            assert_eq!(
                fingerprint_uncovered_columns(contract, t.row_path, t.fields),
                all_columns,
                "every {short} column is expected to be uncovered (loose item schema)"
            );
        }
    }

    // ── Integration: the pack against a mock gateway, end to end. ───────

    fn feishu_config(token_env: &str, tables: &str) -> OpenConnectorConfig {
        // Resources are declared only where a table requires them;
        // sending one to a binding without that table trips the
        // undeclared-resource guard.
        let resource = if tables.contains("messages") {
            "resource: { containerId: oc_root }"
        } else if tables.contains("chat_members") {
            "resource: { chatId: oc_root }"
        } else if tables.contains("wiki_nodes") {
            "resource: { spaceId: sp_root }"
        } else {
            ""
        };
        serde_yaml::from_str(&format!(
            r#"
runtime_token_env: {token_env}
bindings:
  - name: ws
    source_pack: feishu
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
            Some(&feishu_config(token_env, tables)),
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

    fn chat_row(id: &str) -> Value {
        json!({"chat_id": id, "name": id})
    }

    #[tokio::test]
    async fn chats_cursor_scan_pins_ordering_and_pages_with_null_token_termination() {
        // Two-page cursor scan pinning CHATS' wire declarations: no
        // pageToken on page 1, the stub's token afterwards, pageSize 100
        // and the ByCreateTimeAsc pin on every request, null-token
        // termination, row identity across pages.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return feishu_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/feishu.list_chats" {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                let page = match body["input"].get("pageToken").and_then(Value::as_str) {
                    None => json!({"items": [chat_row("oc-1"), chat_row("oc-2")],
                                    "pageToken": "tok-2", "hasMore": true}),
                    Some("tok-2") => json!({"items": [chat_row("oc-3")],
                                             "pageToken": null, "hasMore": false}),
                    Some(other) => return MockResponse::new(400, &format!("bad token {other}")),
                };
                return MockResponse::ok(&envelope_ok(&page.to_string()));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_FEISHU_CHATS", "chats").await;

        let batches = collect(&ctx, "SELECT id FROM saas.ws.chats ORDER BY id").await;
        assert_eq!(ids_of(&batches), vec!["oc-1", "oc-2", "oc-3"]);

        let inputs = execute_inputs(&gateway);
        assert_eq!(inputs.len(), 2, "two cursor pages");
        assert!(inputs[0].get("pageToken").is_none(), "{}", inputs[0]);
        assert_eq!(inputs[1]["pageToken"], "tok-2");
        for input in &inputs {
            assert_eq!(input["pageSize"], 100, "page-size hint: {input}");
            assert_eq!(
                input["sortType"], "ByCreateTimeAsc",
                "ordering pin: {input}"
            );
        }
    }

    #[tokio::test]
    async fn messages_push_start_time_as_digit_string_with_pinned_container() {
        // The messages table's whole wire declaration in one scan: the
        // chat container pin + resource forwarding, the ByCreateTimeAsc
        // pin, and the create_time >= pushdown rendered as EPOCH-SECONDS
        // DIGIT STRING under the strict schema — plus the no-pushdown
        // guard (requests carry exactly the declared inputs).
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return feishu_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/feishu.list_messages" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"items": [
                        {"message_id": "om-1", "create_time": "1735689600000"},
                        {"message_id": "om-2", "create_time": "1704067200000"}],
                        "pageToken": null, "hasMore": false})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_FEISHU_MSGS", "messages").await;

        let batches = collect(
            &ctx,
            "SELECT id FROM saas.ws.messages \
             WHERE create_time >= TIMESTAMP '2025-01-01T00:00:00Z'",
        )
        .await;
        // Inexact pushdown: DataFusion re-trims, so only the matching row
        // survives even though the stub returned both.
        assert_eq!(ids_of(&batches), vec!["om-1"]);

        let inputs = execute_inputs(&gateway);
        assert_eq!(inputs.len(), 1);
        let input = &inputs[0];
        assert_eq!(input["containerIdType"], "chat", "container kind pin");
        assert_eq!(input["containerId"], "oc_root", "resource forwarded");
        assert_eq!(input["sortType"], "ByCreateTimeAsc");
        assert_eq!(
            input["pageSize"], 50,
            "Feishu's im/v1/messages caps page_size at 50 on the wire \
             (99992402 above it), despite the schema's declared 100"
        );
        assert_eq!(
            input["startTime"], "1735689600",
            "epoch seconds as a digit STRING, not a number"
        );
        let mut keys: Vec<&str> = input
            .as_object()
            .expect("input object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "containerId",
                "containerIdType",
                "pageSize",
                "sortType",
                "startTime"
            ],
            "exactly the declared inputs, nothing else"
        );
    }

    #[tokio::test]
    async fn tasks_pin_their_population_and_keep_status_predicates_local() {
        // The action HAS a `completed` boolean input, but real rows carry
        // `status` (todo|done) and no boolean — so nothing is mapped and a
        // status predicate must run entirely in DataFusion while the
        // request carries exactly the pins and pagination, nothing else.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return feishu_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/feishu.list_tasks" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"items": [
                        {"guid": "t-1", "status": "done"},
                        {"guid": "t-2", "status": "todo"}],
                        "pageToken": null, "hasMore": false})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_FEISHU_TASKS", "tasks").await;

        let batches = collect(&ctx, "SELECT guid FROM saas.ws.tasks WHERE status = 'done'").await;
        let guids: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                let col = b
                    .column_by_name("guid")
                    .expect("guid")
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Utf8")
                    .clone();
                (0..col.len()).map(move |i| col.value(i).to_string())
            })
            .collect();
        assert_eq!(guids, vec!["t-1"], "status filtering happened locally");

        let input = &execute_inputs(&gateway)[0];
        assert_eq!(input["type"], "my_tasks", "population pin");
        let mut keys: Vec<&str> = input
            .as_object()
            .expect("input object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec!["pageSize", "type"],
            "the status predicate is not pushed and no `completed` input is invented"
        );
        assert_eq!(input["pageSize"], 100, "declared page size rides the wire");
    }

    #[tokio::test]
    async fn wiki_spaces_terminate_on_has_more_false_despite_a_token() {
        // The live wire shape this pack's has_more_path exists for
        // (captured 2026-08-04): Feishu wiki answers the FINAL page with
        // hasMore:false but a NON-empty pageToken ("0||…"). Null-token
        // termination alone would refetch that token and fail as a
        // PaginationLoop; the has-more signal must end the scan after one
        // request.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return feishu_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/feishu.list_wiki_spaces" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"items": [{"space_id": "700000038", "name": "s"}],
                             "pageToken": "0||7000000000000000001", "hasMore": false})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_FEISHU_WIKI_HM", "wiki_spaces").await;

        let batches = collect(&ctx, "SELECT space_id FROM saas.ws.wiki_spaces").await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let inputs = execute_inputs(&gateway);
        assert_eq!(
            inputs.len(),
            1,
            "hasMore:false ended the scan; the non-empty token was never refetched"
        );
        assert_eq!(
            inputs[0]["pageSize"], 50,
            "declared page size rides the wire"
        );
    }

    #[tokio::test]
    async fn wiki_nodes_forward_required_and_optional_resources() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return feishu_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/feishu.list_wiki_nodes" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"items": [{"node_token": "wik-1", "has_child": false}],
                             "pageToken": null, "hasMore": false})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let _token = EnvVarGuard::set("SKARDI_TEST_OC_FEISHU_WIKI", "test-token");
        let config: OpenConnectorConfig = serde_yaml::from_str(
            r#"
runtime_token_env: SKARDI_TEST_OC_FEISHU_WIKI
bindings:
  - name: ws
    source_pack: feishu
    resource: { spaceId: sp-1, parentNodeToken: wikcn-parent }
    tables: [wiki_nodes]
"#,
        )
        .expect("config parses");
        let gateways = OpenConnectorGateways::default();
        let mut ctx = SessionContext::new();
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

        let batches = collect(&ctx, "SELECT node_token FROM saas.ws.wiki_nodes").await;
        assert_eq!(batches[0].num_rows(), 1);
        let input = &execute_inputs(&gateway)[0];
        assert_eq!(input["spaceId"], "sp-1", "required resource forwarded");
        assert_eq!(
            input["parentNodeToken"], "wikcn-parent",
            "optional resource forwarded when configured"
        );
        // Page size honors the wiki maximum, not the im maximum.
        assert_eq!(input["pageSize"], 50);
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
        let _token = EnvVarGuard::set("SKARDI_TEST_OC_FEISHU_NO_RES", "test-token");
        let config: OpenConnectorConfig = serde_yaml::from_str(
            r#"
runtime_token_env: SKARDI_TEST_OC_FEISHU_NO_RES
bindings:
  - name: ws
    source_pack: feishu
    tables: [messages]
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
        .expect_err("missing containerId must fail registration");
        assert!(err.to_string().contains("containerId"), "{err}");
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
                return feishu_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/feishu.list_chats" {
                // Every page advertises another; only LIMIT can stop this.
                return MockResponse::ok(&envelope_ok(
                    &json!({"items": [chat_row("oc-1"), chat_row("oc-2")],
                             "pageToken": "again", "hasMore": true})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_FEISHU_LIMIT", "chats").await;

        let batches = collect(&ctx, "SELECT id FROM saas.ws.chats LIMIT 2").await;
        assert_eq!(ids_of(&batches).len(), 2);
        assert_eq!(
            execute_inputs(&gateway).len(),
            1,
            "one page satisfied LIMIT"
        );
    }

    #[tokio::test]
    async fn udtf_parity_for_chat_members() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return feishu_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/feishu.list_chat_members" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"items": [{"member_id": "ou-9", "name": "Ada"}],
                             "pageToken": null, "hasMore": false})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_FEISHU_UDTF", "chat_members").await;

        let from_table = collect(&ctx, "SELECT member_id, name FROM saas.ws.chat_members").await;
        assert_eq!(
            execute_inputs(&gateway)[0]["pageSize"],
            100,
            "declared page size rides the wire"
        );
        let from_udtf = collect(
            &ctx,
            "SELECT member_id, name FROM open_connector_query('saas', 'feishu.chat_members', \
             '{\"chatId\":\"oc_root\"}')",
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
        // via feishu_discovery's captured contracts.)
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
        let _token = EnvVarGuard::set("SKARDI_TEST_OC_FEISHU_DRIFT", "test-token");
        let mut ctx = SessionContext::new();
        let gateways = OpenConnectorGateways::default();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&feishu_config("SKARDI_TEST_OC_FEISHU_DRIFT", "chats")),
            false,
            HierarchyLevel::Catalog,
            Some(&gateways),
        )
        .await
        .expect_err("a drifted contract must fail registration");
        let message = err.to_string();
        assert!(
            message.contains("feishu.chats")
                && message.contains("feishu.list_chats")
                && message.contains("fingerprint mismatch"),
            "table, action, and cause are named: {message}"
        );
    }
}
