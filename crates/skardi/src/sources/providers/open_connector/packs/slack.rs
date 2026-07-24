//! Slack source pack: stable relational contracts over the Open Connector
//! `slack.*` read actions (OAuth bot token, cursor pagination).
//!
//! Design decisions, per the integration design spec and the source-pack
//! admission gate:
//!
//! - **Cursor pagination for conversations and users** — Slack's
//!   `cursor` / `response_metadata.next_cursor` contract, the pagination
//!   mode this pack exists to validate. Termination is complete on both of
//!   Slack's end-of-collection spellings: a final page carrying
//!   `next_cursor: ""` and a final page with no `response_metadata` at all.
//!   The engine's repeated-cursor detection turns a non-advancing gateway
//!   into a targeted `PaginationLoop` error instead of an infinite scan.
//!   `files` uses Slack's classic `page`/`count` pagination — that endpoint
//!   never adopted cursors.
//! - **`types` is pinned on conversations** so the table reads as every
//!   channel the bot can see (`public_channel,private_channel`), not
//!   Slack's public-only default — the `state=all` move from the GitHub
//!   pack. IMs and MPIMs are deliberately out: they have no name and belong
//!   to a message-shaped table, not a channels table.
//! - **Filters are allowlisted only where faithful.** `files.user_id` maps
//!   to the `user` query parameter as [`Fidelity::Inexact`], per the
//!   module-wide string-push rule (an Exact claim would lean on the
//!   provider rejecting unknown user IDs instead of silently returning its
//!   default listing). `files.created >=` is deliberately **not** mapped to
//!   `ts_from`: Slack takes epoch seconds there, and the filter engine
//!   renders timestamp literals as RFC 3339 only — mapping it would send a
//!   string Slack cannot parse. It becomes a candidate once per-mapping
//!   value rendering exists.
//! - **Timestamps are epoch seconds** (`created`, `updated`), read through
//!   [`FieldType::TimestampSecondsUtc`] — the millis reader would silently
//!   produce January-1970 dates.
//! - **No message or thread tables**, per the design's Slack caveat:
//!   Open Connector does not yet provide complete message-history cursor
//!   handling, and an incomplete message table would violate the admission
//!   gate's complete-pagination requirement. They land in a later pack
//!   version once upstream support exists; until then `open_connector_scan`
//!   can reach allowlisted read actions ad hoc.
//! - **Nullability is conservative**: only `id` is non-null on every table.
//!   Slack leans on empty strings rather than nulls (`topic.value: ""`),
//!   which surface as empty strings, not NULL; genuinely absent keys
//!   (e.g. `num_members` on some shapes) become NULL.
//! - **`users.email` requires the `users:read.email` bot scope**; without
//!   it Slack omits the field and the column is NULL. Deleted users stay in
//!   the table with `deleted = true`, matching `users.list`.
//! - **No fingerprint pins yet** (`expected_fingerprint: None`) — same
//!   rationale, operational consequence, and live-validation follow-up as
//!   the GitHub pack (see the comment block in `github.rs`).

use datafusion::logical_expr::Operator;

use crate::sources::providers::open_connector::filters::{Fidelity, FilterMapping};
use crate::sources::providers::open_connector::json_to_arrow::{FieldMapping, FieldType};
use crate::sources::providers::open_connector::pagination::PaginationStrategy;
use crate::sources::providers::open_connector::source_pack::{
    FixedValue, SourcePack, SourcePackTable,
};

/// Slack cursor pagination: `cursor` in, `response_metadata.next_cursor`
/// out, 200 rows per page (Slack's recommended ceiling).
const SLACK_CURSOR_PAGINATION: PaginationStrategy = PaginationStrategy::Cursor {
    cursor_param: "cursor",
    next_cursor_path: "$.response_metadata.next_cursor",
    page_size_param: Some("limit"),
    page_size: 200,
};

/// Channels the bot can see (public, plus private ones it is a member of).
static CONVERSATIONS: SourcePackTable = SourcePackTable {
    id: "slack.conversations",
    action_id: "slack.list_conversations",
    row_path: "$.channels",
    fields: &[
        FieldMapping {
            name: "id",
            path: "id",
            field_type: FieldType::Utf8,
            nullable: false,
        },
        FieldMapping {
            name: "name",
            path: "name",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "is_private",
            path: "is_private",
            field_type: FieldType::Boolean,
            nullable: true,
        },
        FieldMapping {
            name: "is_archived",
            path: "is_archived",
            field_type: FieldType::Boolean,
            nullable: true,
        },
        FieldMapping {
            name: "is_general",
            path: "is_general",
            field_type: FieldType::Boolean,
            nullable: true,
        },
        FieldMapping {
            name: "is_shared",
            path: "is_shared",
            field_type: FieldType::Boolean,
            nullable: true,
        },
        FieldMapping {
            name: "is_member",
            path: "is_member",
            field_type: FieldType::Boolean,
            nullable: true,
        },
        FieldMapping {
            name: "creator",
            path: "creator",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "num_members",
            path: "num_members",
            field_type: FieldType::UInt64,
            nullable: true,
        },
        FieldMapping {
            name: "topic",
            path: "topic.value",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "purpose",
            path: "purpose.value",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "created",
            path: "created",
            field_type: FieldType::TimestampSecondsUtc,
            nullable: true,
        },
    ],
    pagination: SLACK_CURSOR_PAGINATION,
    required_resources: &[],
    // Slack lists public channels only by default; pin both channel kinds so
    // the table reads as the complete collection the bot can see. IMs/MPIMs
    // are message-shaped and deliberately out of a channels table.
    fixed_inputs: &[("types", FixedValue::Str("public_channel,private_channel"))],
    filters: &[],
    expected_fingerprint: None,
};

/// Workspace members, including bots and deleted users (`deleted = true`).
static USERS: SourcePackTable = SourcePackTable {
    id: "slack.users",
    action_id: "slack.list_users",
    row_path: "$.members",
    fields: &[
        FieldMapping {
            name: "id",
            path: "id",
            field_type: FieldType::Utf8,
            nullable: false,
        },
        FieldMapping {
            name: "team_id",
            path: "team_id",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "name",
            path: "name",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "real_name",
            path: "real_name",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "display_name",
            path: "profile.display_name",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        // NULL without the `users:read.email` bot scope (Slack omits the
        // field entirely).
        FieldMapping {
            name: "email",
            path: "profile.email",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "tz",
            path: "tz",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "is_bot",
            path: "is_bot",
            field_type: FieldType::Boolean,
            nullable: true,
        },
        FieldMapping {
            name: "is_admin",
            path: "is_admin",
            field_type: FieldType::Boolean,
            nullable: true,
        },
        FieldMapping {
            name: "deleted",
            path: "deleted",
            field_type: FieldType::Boolean,
            nullable: true,
        },
        FieldMapping {
            name: "updated",
            path: "updated",
            field_type: FieldType::TimestampSecondsUtc,
            nullable: true,
        },
    ],
    pagination: SLACK_CURSOR_PAGINATION,
    required_resources: &[],
    fixed_inputs: &[],
    filters: &[],
    expected_fingerprint: None,
};

/// Files visible to the bot.
static FILES: SourcePackTable = SourcePackTable {
    id: "slack.files",
    action_id: "slack.list_files",
    row_path: "$.files",
    fields: &[
        FieldMapping {
            name: "id",
            path: "id",
            field_type: FieldType::Utf8,
            nullable: false,
        },
        FieldMapping {
            name: "name",
            path: "name",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "title",
            path: "title",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "filetype",
            path: "filetype",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "mimetype",
            path: "mimetype",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "size",
            path: "size",
            field_type: FieldType::UInt64,
            nullable: true,
        },
        FieldMapping {
            name: "user_id",
            path: "user",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        // Channel IDs the file is shared into — a plain string array.
        FieldMapping {
            name: "channels",
            path: "channels",
            field_type: FieldType::Utf8List,
            nullable: true,
        },
        FieldMapping {
            name: "is_public",
            path: "is_public",
            field_type: FieldType::Boolean,
            nullable: true,
        },
        FieldMapping {
            name: "created",
            path: "created",
            field_type: FieldType::TimestampSecondsUtc,
            nullable: true,
        },
        FieldMapping {
            name: "permalink",
            path: "permalink",
            field_type: FieldType::Utf8,
            nullable: true,
        },
    ],
    // files.list never adopted cursors; classic page/count pagination.
    pagination: PaginationStrategy::PageNumber {
        page_param: "page",
        per_page_param: "count",
        per_page: 100,
    },
    required_resources: &[],
    fixed_inputs: &[],
    // Inexact per the string-push rule: user IDs are arbitrary strings, and
    // an Exact claim would lean on the provider rejecting unknown ones.
    // `created >= X` → `ts_from` is deliberately NOT mapped — Slack takes
    // epoch seconds there and the filter engine renders timestamp literals
    // as RFC 3339 only; a candidate once per-mapping value rendering exists.
    filters: &[FilterMapping {
        column: "user_id",
        operator: Operator::Eq,
        input_field: "user",
        fidelity: Fidelity::Inexact,
    }],
    expected_fingerprint: None,
};

/// The Slack source pack (version 1): conversations, users, files. Message
/// and thread tables are gated on upstream cursor support (module docs).
pub static SLACK_PACK: SourcePack = SourcePack {
    name: "slack",
    version: 1,
    tables: &[CONVERSATIONS, USERS, FILES],
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::hierarchy::HierarchyLevel;
    use crate::sources::providers::open_connector::json_to_arrow::RowConverter;
    use crate::sources::providers::open_connector::row_path::RowPath;
    use crate::sources::providers::open_connector::testutil::{
        MockGateway, MockResponse, RecordedRequest,
    };
    use crate::sources::providers::open_connector::{
        OpenConnectorConfig, OpenConnectorGateways, register_open_connector_tables,
        register_open_connector_udtfs,
    };
    use arrow::array::{
        Array, BooleanArray, ListArray, StringArray, TimestampMillisecondArray, UInt64Array,
    };
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use serde_json::{Value, json};

    // ── Contract tests: bundled redacted fixtures are the build-time
    // conversion contract (null-bearing, nested, empty, and extra upstream
    // fields per the source-pack admission gate). ───────────────────────

    /// Convert one bundled fixture page through a table's declared contract.
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
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
    }

    fn boolean<'a>(batch: &'a RecordBatch, name: &str) -> &'a BooleanArray {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
    }

    fn timestamp<'a>(batch: &'a RecordBatch, name: &str) -> &'a TimestampMillisecondArray {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
    }

    #[test]
    fn conversations_fixture_converts_with_nested_and_absent_fields() {
        let batch = convert_fixture(
            &CONVERSATIONS,
            include_str!("fixtures/slack/conversations.json"),
        );
        assert_eq!(batch.num_rows(), 3);

        let ids = utf8(&batch, "id");
        assert_eq!(ids.value(0), "C0001");

        // Nested topic.value; Slack's empty-string convention stays an
        // empty string, never NULL.
        let topics = utf8(&batch, "topic");
        assert_eq!(topics.value(0), "Company-wide announcements");
        assert_eq!(topics.value(1), "");

        // Absent keys become NULL: purpose on row 2, num_members on row 2,
        // creator on row 3, is_general on row 2.
        assert!(utf8(&batch, "purpose").is_null(1));
        assert!(
            batch
                .column_by_name("num_members")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .is_null(1)
        );
        assert!(utf8(&batch, "creator").is_null(2));
        assert!(boolean(&batch, "is_general").is_null(1));
        assert!(boolean(&batch, "is_private").value(1));

        // Epoch seconds scaled to millis.
        assert_eq!(timestamp(&batch, "created").value(0), 1_735_689_600_000);
    }

    #[test]
    fn users_fixture_converts_with_scope_gated_and_deleted_rows() {
        let batch = convert_fixture(&USERS, include_str!("fixtures/slack/users.json"));
        assert_eq!(batch.num_rows(), 3);

        // Email present only where the users:read.email scope exposed it.
        let emails = utf8(&batch, "email");
        assert_eq!(emails.value(0), "ada@acme.example");
        assert!(emails.is_null(1), "bot profile has no email field");
        assert!(emails.is_null(2), "deleted profile has no email field");

        assert!(boolean(&batch, "is_bot").value(1));
        assert!(boolean(&batch, "deleted").value(2));
        assert!(utf8(&batch, "tz").is_null(2), "deleted users lose tz");
        assert_eq!(utf8(&batch, "display_name").value(1), "");
        assert_eq!(timestamp(&batch, "updated").value(2), 1_704_067_200_000);
    }

    #[test]
    fn files_fixture_converts_with_channel_lists_and_absent_fields() {
        let batch = convert_fixture(&FILES, include_str!("fixtures/slack/files.json"));
        assert_eq!(batch.num_rows(), 3);

        let channels = batch
            .column_by_name("channels")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(channels.value(0).len(), 2, "shared into two channels");
        assert_eq!(
            channels.value(1).len(),
            0,
            "empty list stays empty, not NULL"
        );

        assert_eq!(utf8(&batch, "user_id").value(0), "U0001");
        assert!(utf8(&batch, "title").is_null(1));
        assert!(utf8(&batch, "mimetype").is_null(2));
        assert!(
            batch
                .column_by_name("size")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .is_null(2)
        );
        assert!(utf8(&batch, "permalink").is_null(2));
        assert!(boolean(&batch, "is_public").is_null(2));
    }

    #[test]
    fn empty_pages_preserve_every_table_schema() {
        for (table, empty) in [
            (&CONVERSATIONS, r#"{"ok":true,"channels":[]}"#),
            (&USERS, r#"{"ok":true,"members":[]}"#),
            (&FILES, r#"{"ok":true,"files":[]}"#),
        ] {
            let batch = convert_fixture(table, empty);
            assert_eq!(batch.num_rows(), 0);
            assert_eq!(batch.num_columns(), table.fields.len(), "{}", table.id);
        }
    }

    #[test]
    fn every_table_binds_and_declares_a_complete_contract() {
        for table in SLACK_PACK.tables {
            RowPath::parse(table.row_path).unwrap_or_else(|e| panic!("{}: {e}", table.id));
            RowConverter::new(table.fields).unwrap_or_else(|e| panic!("{}: {e}", table.id));
            table
                .pagination
                .validate()
                .unwrap_or_else(|e| panic!("{}: {e}", table.id));
            assert!(
                table.id.starts_with("slack."),
                "{} must be namespaced",
                table.id
            );
        }
        assert_eq!(SLACK_PACK.tables.len(), 3, "messages/threads stay gated");
    }

    // ── Integration: the pack against a mock gateway, end to end. ───────

    /// How the stub ends (or refuses to end) its cursor sequence.
    #[derive(Clone, Copy, PartialEq)]
    enum Terminal {
        /// Final page carries `next_cursor: ""` (Slack's usual spelling).
        EmptyCursor,
        /// Final page omits `response_metadata` entirely.
        NoMetadata,
        /// Every page returns the same cursor — a non-advancing gateway.
        Stuck,
    }

    /// A cursor-paginated stub for `slack.list_conversations`, serving
    /// `rows` two per page regardless of the requested `limit` (the stub
    /// controls pagination; the engine must follow the cursors).
    fn conversations_gateway(
        rows: Vec<Value>,
        terminal: Terminal,
    ) -> impl Fn(&RecordedRequest) -> MockResponse {
        move |req: &RecordedRequest| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return MockResponse::ok(
                    r#"{"input_schema": {}, "output_schema": {"type": "object"},
                        "locally_executable": true, "connection_aliases": []}"#,
                );
            }
            if req.method == "POST" && req.path == "/v1/actions/slack.list_conversations/execute" {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                let input = body.get("input").cloned().unwrap_or_default();
                let start = input
                    .get("cursor")
                    .and_then(Value::as_str)
                    .and_then(|c| c.strip_prefix("cur-"))
                    .and_then(|n| n.parse::<usize>().ok())
                    .unwrap_or(0);
                let slice: Vec<Value> = rows.iter().skip(start).take(2).cloned().collect();
                let mut output = json!({"ok": true, "channels": slice});
                let next = start + 2;
                match terminal {
                    Terminal::Stuck => {
                        output["response_metadata"] = json!({"next_cursor": "cur-stuck"});
                    }
                    Terminal::EmptyCursor => {
                        let cursor = if next < rows.len() {
                            format!("cur-{next}")
                        } else {
                            String::new()
                        };
                        output["response_metadata"] = json!({"next_cursor": cursor});
                    }
                    Terminal::NoMetadata => {
                        if next < rows.len() {
                            output["response_metadata"] =
                                json!({"next_cursor": format!("cur-{next}")});
                        }
                    }
                }
                return MockResponse::ok(&json!({"output": output}).to_string());
            }
            MockResponse::new(404, "{}")
        }
    }

    fn channel(id: usize) -> Value {
        json!({"id": format!("C{id:04}"), "name": format!("chan-{id}"), "created": 1735689600})
    }

    /// Register `saas` with the given binding tables against `gateway`.
    async fn setup(gateway: &MockGateway, tables: &str, token_env: &str) -> SessionContext {
        unsafe {
            std::env::set_var(token_env, "test-token");
        }
        let config: OpenConnectorConfig = serde_yaml::from_str(&format!(
            r#"
runtime_token_env: {token_env}
bindings:
  - name: ws
    source_pack: slack
    tables: [{tables}]
"#
        ))
        .expect("parse config");
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
        .expect("gateway registration succeeds");
        unsafe {
            std::env::remove_var(token_env);
        }
        register_open_connector_udtfs(&ctx, gateways);
        ctx
    }

    async fn collect(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        ctx.sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect")
    }

    fn rows_of(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    fn execute_bodies(gateway: &MockGateway) -> Vec<String> {
        gateway
            .requests()
            .into_iter()
            .filter(|r| r.method == "POST")
            .map(|r| r.body)
            .collect()
    }

    #[tokio::test]
    async fn cursor_scan_paginates_and_terminates_on_the_empty_cursor() {
        // 5 channels at 2 per stub page → 3 pages, ended by `next_cursor:
        // ""`. The first request carries no cursor; every later one carries
        // the stub's token; the `limit` hint and the `types` pin ride every
        // request.
        let rows: Vec<Value> = (1..=5).map(channel).collect();
        let gateway = MockGateway::start(conversations_gateway(rows, Terminal::EmptyCursor)).await;
        let ctx = setup(&gateway, "conversations", "SKARDI_TEST_OC_SLACK_CURSOR").await;

        let batches = collect(&ctx, "SELECT id FROM saas.ws.conversations ORDER BY id").await;
        assert_eq!(rows_of(&batches), 5);

        let bodies = execute_bodies(&gateway);
        assert_eq!(bodies.len(), 3, "three cursor pages");
        assert!(!bodies[0].contains("cursor"), "page 1 sends no cursor");
        assert!(bodies[1].contains(r#""cursor":"cur-2""#), "{}", bodies[1]);
        assert!(bodies[2].contains(r#""cursor":"cur-4""#), "{}", bodies[2]);
        for body in &bodies {
            assert!(body.contains(r#""limit":200"#), "page-size hint: {body}");
            assert!(
                body.contains(r#""types":"public_channel,private_channel""#),
                "the types pin rides every request: {body}"
            );
        }
    }

    #[tokio::test]
    async fn cursor_scan_terminates_when_response_metadata_is_absent() {
        // Slack's other end-of-collection spelling: no response_metadata at
        // all on the final page.
        let rows: Vec<Value> = (1..=3).map(channel).collect();
        let gateway = MockGateway::start(conversations_gateway(rows, Terminal::NoMetadata)).await;
        let ctx = setup(&gateway, "conversations", "SKARDI_TEST_OC_SLACK_NOMETA").await;

        let batches = collect(&ctx, "SELECT id FROM saas.ws.conversations").await;
        assert_eq!(rows_of(&batches), 3);
        assert_eq!(execute_bodies(&gateway).len(), 2, "two pages, then done");
    }

    #[tokio::test]
    async fn non_advancing_cursor_fails_as_a_pagination_loop() {
        // A gateway that repeats its cursor must be a targeted error, never
        // an unbounded scan.
        let rows: Vec<Value> = (1..=4).map(channel).collect();
        let gateway = MockGateway::start(conversations_gateway(rows, Terminal::Stuck)).await;
        let ctx = setup(&gateway, "conversations", "SKARDI_TEST_OC_SLACK_LOOP").await;

        let err = ctx
            .sql("SELECT id FROM saas.ws.conversations")
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("the loop must fail the scan");
        assert!(
            err.to_string().contains("already seen"),
            "targeted PaginationLoop error: {err}"
        );
        assert_eq!(
            execute_bodies(&gateway).len(),
            2,
            "detected on the first repeated cursor"
        );
    }

    #[tokio::test]
    async fn limit_stops_cursor_pagination_early() {
        let rows: Vec<Value> = (1..=5).map(channel).collect();
        let gateway = MockGateway::start(conversations_gateway(rows, Terminal::EmptyCursor)).await;
        let ctx = setup(&gateway, "conversations", "SKARDI_TEST_OC_SLACK_LIMIT").await;

        let batches = collect(&ctx, "SELECT id FROM saas.ws.conversations LIMIT 2").await;
        assert_eq!(rows_of(&batches), 2);
        assert_eq!(
            execute_bodies(&gateway).len(),
            1,
            "LIMIT 2 is satisfied by the first stub page"
        );
    }

    #[tokio::test]
    async fn empty_workspace_yields_an_empty_scan() {
        let gateway =
            MockGateway::start(conversations_gateway(Vec::new(), Terminal::EmptyCursor)).await;
        let ctx = setup(&gateway, "conversations", "SKARDI_TEST_OC_SLACK_EMPTY").await;

        let batches = collect(&ctx, "SELECT id FROM saas.ws.conversations").await;
        assert_eq!(rows_of(&batches), 0);
        assert_eq!(execute_bodies(&gateway).len(), 1, "one empty page");
    }

    /// Stub for users + files alongside conversations: users cursor-paged
    /// in one page; files classic-paged, IGNORING the pushed `user` filter
    /// (the Inexact contract's hostile-provider case).
    fn workspace_gateway(req: &RecordedRequest) -> MockResponse {
        if req.method == "GET" && req.path == "/v1/health" {
            return MockResponse::ok("{}");
        }
        if req.method == "GET" && req.path.starts_with("/v1/actions/") {
            return MockResponse::ok(
                r#"{"input_schema": {}, "output_schema": {"type": "object"},
                    "locally_executable": true, "connection_aliases": []}"#,
            );
        }
        if req.method == "POST" && req.path == "/v1/actions/slack.list_users/execute" {
            return MockResponse::ok(
                &json!({"output": {"ok": true, "members": [
                    {"id": "U0001", "name": "ada", "is_bot": false},
                    {"id": "U0002", "name": "deploybot", "is_bot": true}
                ], "response_metadata": {"next_cursor": ""}}})
                .to_string(),
            );
        }
        if req.method == "POST" && req.path == "/v1/actions/slack.list_files/execute" {
            return MockResponse::ok(
                &json!({"output": {"ok": true, "files": [
                    {"id": "F0001", "user": "U0001", "name": "roadmap.pdf"},
                    {"id": "F0002", "user": "U0002", "name": "notes.txt"}
                ]}})
                .to_string(),
            );
        }
        MockResponse::new(404, "{}")
    }

    #[tokio::test]
    async fn files_user_filter_is_pushed_and_reapplied_locally() {
        // The stub ignores the `user` input entirely — the harshest legal
        // Inexact provider. DataFusion must trim the superset back to the
        // predicate; the push still narrows the fetch on providers that
        // honor it.
        let gateway = MockGateway::start(workspace_gateway).await;
        let ctx = setup(&gateway, "files", "SKARDI_TEST_OC_SLACK_FILES").await;

        let batches = collect(&ctx, "SELECT id FROM saas.ws.files WHERE user_id = 'U0001'").await;
        assert_eq!(
            rows_of(&batches),
            1,
            "the ignoring provider's extra row is trimmed"
        );

        let bodies = execute_bodies(&gateway);
        assert!(!bodies.is_empty());
        assert!(
            bodies.iter().all(|body| body.contains(r#""user":"U0001""#)),
            "the predicate is pushed as Slack's user parameter: {bodies:?}"
        );
        assert!(
            bodies.iter().all(|body| body.contains(r#""count":100"#)),
            "files uses classic page/count pagination: {bodies:?}"
        );
    }

    #[tokio::test]
    async fn multi_table_binding_and_udtf_parity() {
        // One binding exposes all three tables (no required resources — a
        // pack first), and the query UDTF returns exactly the bound table.
        let gateway = MockGateway::start(workspace_gateway).await;
        let ctx = setup(&gateway, "users, files", "SKARDI_TEST_OC_SLACK_MULTI").await;

        let batches = collect(&ctx, "SELECT id FROM saas.ws.users WHERE is_bot").await;
        assert_eq!(rows_of(&batches), 1);

        let from_table = collect(&ctx, "SELECT id, name FROM saas.ws.users ORDER BY id").await;
        let from_udtf = collect(
            &ctx,
            "SELECT id, name FROM open_connector_query('saas', 'slack.users', '{}') ORDER BY id",
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
}
