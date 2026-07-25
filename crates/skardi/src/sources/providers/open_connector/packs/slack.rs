//! Slack source pack: stable relational contracts over the Open Connector
//! `slack.*` read actions (OAuth bot token, cursor pagination).
//!
//! **The wire contract is Open Connector's, not Slack's raw Web API.**
//! Unlike the GitHub executors (raw passthrough), OC's Slack executors
//! normalize: conversation and user rows are rebuilt with camelCase fields
//! (`channelId`, `isArchived`, `realName`, …), the next cursor moves from
//! Slack's `response_metadata.next_cursor` to a top-level `nextCursor`
//! (`null` at end-of-collection), row arrays live under `conversations` /
//! `users` (not `channels` / `members`), and Slack's in-band `ok:false` /
//! `error` envelope is consumed by the executor and surfaced as a gateway
//! failure envelope — it never appears in action output. File rows are the
//! raw Slack file object *plus* normalized aliases (`fileId`,
//! `urlPrivate`), so raw fields like epoch-seconds `created` survive.
//! Everything below is reconciled against a live gateway (v1.3.1) and the
//! OC provider source.
//!
//! Design decisions, per the integration design spec and the source-pack
//! admission gate:
//!
//! - **Cursor pagination for conversations and users** — `cursor` in,
//!   top-level `nextCursor` out, the pagination mode this pack exists to
//!   validate. Termination is complete on both end-of-collection
//!   spellings: a final page carrying `nextCursor: null` (what the OC
//!   executor emits) and a final page omitting the key entirely. The
//!   engine's repeated-cursor detection turns a non-advancing gateway into
//!   a targeted `PaginationLoop` error instead of an infinite scan.
//!   `files` uses Slack's classic `page`/`count` pagination — that
//!   endpoint never adopted cursors — terminated by the envelope's
//!   authoritative `paging.pages`, not the short-page heuristic:
//!   permission filtering can legally shorten non-final pages, which the
//!   heuristic would misread as end-of-collection and silently truncate.
//! - **`types` is pinned on conversations** so the table reads as every
//!   channel the bot can see (`["public_channel", "private_channel"]` —
//!   the action schema takes an array, `additionalProperties: false`
//!   strict), not Slack's public-only default — the `state=all` move from
//!   the GitHub pack. IMs and MPIMs are deliberately out: they have no
//!   name and belong to a message-shaped table, not a channels table.
//! - **`includeLocale` is pinned on users** so the `locale` column the
//!   normalized contract declares is actually populated — Slack omits the
//!   field without the flag, which would leave a permanently NULL column.
//! - **Filters are allowlisted only where faithful.** `files.user_id`
//!   maps to the `userId` input as [`Fidelity::Inexact`], per the
//!   module-wide string-push rule (an Exact claim would lean on the
//!   provider rejecting unknown user IDs instead of silently returning
//!   its default listing). The previous `created >= → ts_from` push is
//!   **gone**: OC's `slack.list_files` contract declares no time input at
//!   all, and its strict schema would 400 any request carrying one — time
//!   predicates are evaluated by DataFusion after the bounded fetch.
//! - **`files.created` is epoch seconds**, read through
//!   [`FieldType::TimestampSecondsUtc`] — the millis reader would
//!   silently produce January-1970 dates. (The normalized conversation
//!   and user rows carry no timestamps at all.)
//! - **`files` may be scoped to one channel** with the optional
//!   `channelId` resource — declared as an optional resource so a shared
//!   binding's key reaches only this table.
//! - **No message or thread tables**, per the design's Slack caveat:
//!   Open Connector does not yet provide complete message-history cursor
//!   handling, and an incomplete message table would violate the admission
//!   gate's complete-pagination requirement. They land in a later pack
//!   version once upstream support exists; until then `open_connector_scan`
//!   can reach allowlisted read actions ad hoc.
//! - **Nullability is conservative**: only `id` is non-null on every
//!   table. The normalizer emits explicit `null`s for absent strings and
//!   booleans and *omits* `memberCount` when Slack didn't send a number;
//!   both surface as SQL NULL. Slack's empty-string convention
//!   (`topic: ""`) stays an empty string, never NULL. Deleted users stay
//!   in the table with `deleted = true`, matching `users.list`.
//! - **No fingerprint pins yet** (`expected_fingerprint: None`) — same
//!   rationale and operational consequence as the GitHub pack (see the
//!   comment block in `github.rs`); like GitHub, the action IDs, input
//!   keys, and row paths here are live-reconciled, the pins are not.

use datafusion::logical_expr::Operator;

use crate::sources::providers::open_connector::filters::{Fidelity, FilterMapping, ValueFormat};
use crate::sources::providers::open_connector::json_to_arrow::{FieldMapping, FieldType};
use crate::sources::providers::open_connector::pagination::PaginationStrategy;
use crate::sources::providers::open_connector::source_pack::{
    FixedValue, SourcePack, SourcePackTable,
};

/// Slack cursor pagination through Open Connector: `cursor` in, top-level
/// `nextCursor` out (`null` at end-of-collection), 200 rows per page
/// (Slack's recommended ceiling, sent as the `limit` input).
const SLACK_CURSOR_PAGINATION: PaginationStrategy = PaginationStrategy::Cursor {
    cursor_param: "cursor",
    next_cursor_path: "$.nextCursor",
    page_size_param: Some("limit"),
    page_size: 200,
};

/// Channels the bot can see (public, plus private ones it is a member of).
/// Rows are Open Connector's normalized conversation shape, not Slack's
/// raw `conversations.list` objects.
static CONVERSATIONS: SourcePackTable = SourcePackTable {
    id: "slack.conversations",
    action_id: "slack.list_conversations",
    row_path: "$.conversations",
    fields: &[
        FieldMapping {
            name: "id",
            path: "channelId",
            field_type: FieldType::Utf8,
            nullable: false,
        },
        FieldMapping {
            name: "name",
            path: "name",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        // The normalizer's classification: public_channel / private_channel
        // (im/mpim never appear here — the types pin excludes them).
        FieldMapping {
            name: "type",
            path: "type",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "is_private",
            path: "isPrivate",
            field_type: FieldType::Boolean,
            nullable: true,
        },
        FieldMapping {
            name: "is_archived",
            path: "isArchived",
            field_type: FieldType::Boolean,
            nullable: true,
        },
        FieldMapping {
            name: "is_member",
            path: "isMember",
            field_type: FieldType::Boolean,
            nullable: true,
        },
        // Omitted (not null) by the normalizer when Slack sends no number.
        FieldMapping {
            name: "member_count",
            path: "memberCount",
            field_type: FieldType::UInt64,
            nullable: true,
        },
        FieldMapping {
            name: "topic",
            path: "topic",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "purpose",
            path: "purpose",
            field_type: FieldType::Utf8,
            nullable: true,
        },
    ],
    pagination: SLACK_CURSOR_PAGINATION,
    required_resources: &[],
    optional_resources: &[],
    // Slack lists public channels only by default; pin both channel kinds so
    // the table reads as the complete collection the bot can see. IMs/MPIMs
    // are message-shaped and deliberately out of a channels table. The
    // action schema takes an array of enum strings.
    fixed_inputs: &[(
        "types",
        FixedValue::StrList(&["public_channel", "private_channel"]),
    )],
    filters: &[],
    // Slack's in-band ok:false envelope is consumed by the OC executor and
    // surfaced as a gateway failure — it never reaches action output.
    error_path: None,
    expected_fingerprint: None,
};

/// Workspace members, including bots and deleted users (`deleted = true`).
/// Rows are Open Connector's normalized user shape — profile fields are
/// already flattened (`realName`, `displayName`), and Slack extras the
/// normalizer drops (email, tz, team_id, updated) are not part of the
/// contract.
static USERS: SourcePackTable = SourcePackTable {
    id: "slack.users",
    action_id: "slack.list_users",
    row_path: "$.users",
    fields: &[
        FieldMapping {
            name: "id",
            path: "userId",
            field_type: FieldType::Utf8,
            nullable: false,
        },
        FieldMapping {
            name: "name",
            path: "username",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "real_name",
            path: "realName",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "display_name",
            path: "displayName",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "is_bot",
            path: "isBot",
            field_type: FieldType::Boolean,
            nullable: true,
        },
        FieldMapping {
            name: "is_admin",
            path: "isAdmin",
            field_type: FieldType::Boolean,
            nullable: true,
        },
        FieldMapping {
            name: "is_owner",
            path: "isOwner",
            field_type: FieldType::Boolean,
            nullable: true,
        },
        FieldMapping {
            name: "deleted",
            path: "isDeleted",
            field_type: FieldType::Boolean,
            nullable: true,
        },
        // Populated only because includeLocale is pinned below — Slack
        // omits the field without the flag.
        FieldMapping {
            name: "locale",
            path: "locale",
            field_type: FieldType::Utf8,
            nullable: true,
        },
    ],
    pagination: SLACK_CURSOR_PAGINATION,
    required_resources: &[],
    optional_resources: &[],
    // Without the flag Slack omits locale entirely and the declared column
    // would be permanently NULL.
    fixed_inputs: &[("includeLocale", FixedValue::Bool(true))],
    filters: &[],
    // In-band errors are consumed by the OC executor (see CONVERSATIONS).
    error_path: None,
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
    // Slack's envelope carries an authoritative `paging.pages`, so the scan
    // trusts it instead of the short-page heuristic: permission filtering
    // can legally shorten non-final pages, which the heuristic would read
    // as end-of-collection and silently truncate.
    pagination: PaginationStrategy::PageNumber {
        page_param: "page",
        per_page_param: "count",
        per_page: 100,
        total_pages_path: Some("$.paging.pages"),
    },
    required_resources: &[],
    // A binding may scope the listing to one channel; declared here so a
    // shared binding's channelId reaches only this table.
    optional_resources: &["channelId"],
    fixed_inputs: &[],
    filters: &[
        // Inexact per the string-push rule: user IDs are arbitrary strings,
        // and an Exact claim would lean on the provider rejecting unknown
        // ones. The OC input key is userId (camelCase, strict schema).
        //
        // No time filter is mapped: the OC list_files contract declares no
        // ts_from/ts_to inputs, and its strict schema would 400 a request
        // carrying one. Time predicates run in DataFusion.
        FilterMapping {
            column: "user_id",
            operator: Operator::Eq,
            input_field: "userId",
            fidelity: Fidelity::Inexact,
            value_format: ValueFormat::Rfc3339,
        },
    ],
    // In-band errors are consumed by the OC executor (see CONVERSATIONS).
    error_path: None,
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
        MockGateway, MockResponse, RecordedRequest, discovery_ok, envelope_err, envelope_ok,
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
    fn conversations_fixture_converts_with_null_and_absent_fields() {
        let batch = convert_fixture(
            &CONVERSATIONS,
            include_str!("fixtures/slack/conversations.json"),
        );
        assert_eq!(batch.num_rows(), 3);

        // The normalized shape: channelId feeds the id column.
        let ids = utf8(&batch, "id");
        assert_eq!(ids.value(0), "C0001");
        assert_eq!(utf8(&batch, "type").value(1), "private_channel");

        // Slack's empty-string convention stays an empty string, never NULL.
        let topics = utf8(&batch, "topic");
        assert_eq!(topics.value(0), "Company-wide announcements");
        assert_eq!(topics.value(1), "");

        // The normalizer emits explicit nulls for absent strings/booleans
        // (purpose, isMember) and OMITS memberCount without a number —
        // both must land as SQL NULL.
        assert!(utf8(&batch, "purpose").is_null(1));
        assert!(boolean(&batch, "is_member").is_null(2));
        let member_counts = batch
            .column_by_name("member_count")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert!(member_counts.is_null(1), "omitted memberCount is NULL");
        assert_eq!(member_counts.value(0), 42);
        assert!(boolean(&batch, "is_private").value(1));
    }

    #[test]
    fn users_fixture_converts_with_flattened_profile_and_deleted_rows() {
        let batch = convert_fixture(&USERS, include_str!("fixtures/slack/users.json"));
        assert_eq!(batch.num_rows(), 3);

        // The normalized shape is already flat: userId/username/realName.
        assert_eq!(utf8(&batch, "id").value(0), "U0001");
        assert_eq!(utf8(&batch, "name").value(1), "deploybot");
        assert_eq!(utf8(&batch, "real_name").value(0), "Ada Lovelace");
        assert!(utf8(&batch, "real_name").is_null(2), "explicit null");

        assert!(boolean(&batch, "is_bot").value(1));
        assert!(boolean(&batch, "is_owner").value(0));
        assert!(boolean(&batch, "deleted").value(2));
        assert!(boolean(&batch, "is_admin").is_null(2), "explicit null");
        assert_eq!(utf8(&batch, "display_name").value(1), "");

        // locale rides only with the includeLocale pin; a row without it
        // (the bot) is NULL.
        assert_eq!(utf8(&batch, "locale").value(0), "en-GB");
        assert!(utf8(&batch, "locale").is_null(1));
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
        // Raw Slack epoch seconds survive the normalizer's spread and are
        // scaled to millis by TimestampSecondsUtc.
        assert_eq!(timestamp(&batch, "created").value(0), 1_735_689_600_000);
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
            (&CONVERSATIONS, r#"{"conversations":[],"nextCursor":null}"#),
            (&USERS, r#"{"users":[],"nextCursor":null}"#),
            (&FILES, r#"{"files":[],"paging":{"pages":0}}"#),
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
        /// Final page carries `nextCursor: null` — what the OC executor
        /// emits at end-of-collection.
        NullCursor,
        /// Final page omits `nextCursor` entirely (a lenient reading of
        /// the same contract).
        Omitted,
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
                return MockResponse::ok(&discovery_ok("{}", r#"{"type": "object"}"#, true, None));
            }
            if req.method == "POST" && req.path == "/v1/actions/slack.list_conversations" {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                let input = body.get("input").cloned().unwrap_or_default();
                let start = input
                    .get("cursor")
                    .and_then(Value::as_str)
                    .and_then(|c| c.strip_prefix("cur-"))
                    .and_then(|n| n.parse::<usize>().ok())
                    .unwrap_or(0);
                let slice: Vec<Value> = rows.iter().skip(start).take(2).cloned().collect();
                let mut output = json!({"conversations": slice});
                let next = start + 2;
                match terminal {
                    Terminal::Stuck => {
                        output["nextCursor"] = json!("cur-stuck");
                    }
                    Terminal::NullCursor => {
                        output["nextCursor"] = if next < rows.len() {
                            json!(format!("cur-{next}"))
                        } else {
                            Value::Null
                        };
                    }
                    Terminal::Omitted => {
                        if next < rows.len() {
                            output["nextCursor"] = json!(format!("cur-{next}"));
                        }
                    }
                }
                return MockResponse::ok(&envelope_ok(&output.to_string()));
            }
            MockResponse::new(404, "{}")
        }
    }

    fn channel(id: usize) -> Value {
        json!({
            "channelId": format!("C{id:04}"),
            "name": format!("chan-{id}"),
            "type": "public_channel"
        })
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

    /// The `id` column of collected batches, in emission order.
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

    fn execute_bodies(gateway: &MockGateway) -> Vec<String> {
        gateway
            .requests()
            .into_iter()
            .filter(|r| r.method == "POST")
            .map(|r| r.body)
            .collect()
    }

    #[tokio::test]
    async fn cursor_scan_paginates_and_terminates_on_the_null_cursor() {
        // 5 channels at 2 per stub page → 3 pages, ended by `nextCursor:
        // null` (the OC executor's end-of-collection spelling). The first
        // request carries no cursor; every later one carries the stub's
        // token; the `limit` hint and the `types` pin ride every request.
        let rows: Vec<Value> = (1..=5).map(channel).collect();
        let gateway = MockGateway::start(conversations_gateway(rows, Terminal::NullCursor)).await;
        let ctx = setup(&gateway, "conversations", "SKARDI_TEST_OC_SLACK_CURSOR").await;

        let batches = collect(&ctx, "SELECT id FROM saas.ws.conversations ORDER BY id").await;
        assert_eq!(rows_of(&batches), 5);

        let bodies = execute_bodies(&gateway);
        assert_eq!(bodies.len(), 3, "three cursor pages");
        let inputs: Vec<Value> = bodies
            .iter()
            .map(|body| {
                serde_json::from_str::<Value>(body).expect("request body is JSON")["input"].clone()
            })
            .collect();
        assert!(
            inputs[0].get("cursor").is_none(),
            "page 1 sends no cursor: {}",
            inputs[0]
        );
        assert_eq!(inputs[1]["cursor"], "cur-2");
        assert_eq!(inputs[2]["cursor"], "cur-4");
        for input in &inputs {
            assert_eq!(input["limit"], 200, "page-size hint: {input}");
            assert_eq!(
                input["types"],
                json!(["public_channel", "private_channel"]),
                "the types pin rides every request as the schema's array: {input}"
            );
        }
    }

    #[tokio::test]
    async fn cursor_scan_terminates_when_next_cursor_is_absent() {
        // The lenient spelling of the same contract: the final page omits
        // `nextCursor` entirely.
        let rows: Vec<Value> = (1..=3).map(channel).collect();
        let gateway = MockGateway::start(conversations_gateway(rows, Terminal::Omitted)).await;
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
        let gateway = MockGateway::start(conversations_gateway(rows, Terminal::NullCursor)).await;
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
    async fn limit_satisfied_on_a_repeated_cursor_page_still_succeeds() {
        // The stuck stub repeats its cursor on every page — page 2's advance
        // would trip loop detection. But LIMIT 4 is satisfied ON page 2, and
        // a complete-for-its-key result must not be failed by continuation
        // state the scan will never use: pagination does not advance once
        // the scan is done.
        let rows: Vec<Value> = (1..=6).map(channel).collect();
        let gateway = MockGateway::start(conversations_gateway(rows, Terminal::Stuck)).await;
        let ctx = setup(
            &gateway,
            "conversations",
            "SKARDI_TEST_OC_SLACK_LIMIT_STUCK",
        )
        .await;

        let batches = collect(&ctx, "SELECT id FROM saas.ws.conversations LIMIT 4").await;
        assert_eq!(rows_of(&batches), 4, "the satisfied LIMIT wins");
        assert_eq!(
            execute_bodies(&gateway).len(),
            2,
            "two pages of two rows; no third request"
        );
    }

    #[tokio::test]
    async fn slack_ok_false_arrives_as_the_gateway_failure_it_becomes() {
        // Slack reports application errors as HTTP 200 + ok:false + error —
        // but the OC executor consumes that envelope and the gateway
        // returns a *failure* envelope with a provider-derived message.
        // The user must see that message, not a row-path error. (The
        // pack-level error_path mechanism stays for providers whose
        // executors pass in-band errors through; Slack's does not, so
        // these tables declare none — see the mock-pack test for the
        // engine feature itself.)
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return MockResponse::ok(&discovery_ok("{}", r#"{"type": "object"}"#, true, None));
            }
            if req.method == "POST" && req.path == "/v1/actions/slack.list_conversations" {
                return MockResponse::new(
                    502,
                    &envelope_err("provider_error", "slack error: missing_scope"),
                );
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let ctx = setup(&gateway, "conversations", "SKARDI_TEST_OC_SLACK_OKFALSE").await;

        let err = ctx
            .sql("SELECT id FROM saas.ws.conversations")
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("the provider error must fail the scan");
        let message = err.to_string();
        assert!(
            message.contains("missing_scope") && message.contains("slack.list_conversations"),
            "the provider's own code and the action are named: {message}"
        );
        assert!(
            !message.contains("row path"),
            "never the misleading row-path error: {message}"
        );
    }

    #[tokio::test]
    async fn empty_workspace_yields_an_empty_scan() {
        let gateway =
            MockGateway::start(conversations_gateway(Vec::new(), Terminal::NullCursor)).await;
        let ctx = setup(&gateway, "conversations", "SKARDI_TEST_OC_SLACK_EMPTY").await;

        let batches = collect(&ctx, "SELECT id FROM saas.ws.conversations").await;
        assert_eq!(rows_of(&batches), 0);
        assert_eq!(execute_bodies(&gateway).len(), 1, "one empty page");
    }

    /// Stub for users + files alongside conversations: users cursor-paged
    /// in one page; files classic-paged, IGNORING the pushed `userId`
    /// filter (the Inexact contract's hostile-provider case).
    fn workspace_gateway(req: &RecordedRequest) -> MockResponse {
        if req.method == "GET" && req.path == "/v1/health" {
            return MockResponse::ok("{}");
        }
        if req.method == "GET" && req.path.starts_with("/v1/actions/") {
            return MockResponse::ok(&discovery_ok("{}", r#"{"type": "object"}"#, true, None));
        }
        if req.method == "POST" && req.path == "/v1/actions/slack.list_users" {
            return MockResponse::ok(&envelope_ok(
                &json!({"users": [
                    {"userId": "U0001", "username": "ada", "isBot": false},
                    {"userId": "U0002", "username": "deploybot", "isBot": true}
                ], "nextCursor": null})
                .to_string(),
            ));
        }
        if req.method == "POST" && req.path == "/v1/actions/slack.list_files" {
            return MockResponse::ok(&envelope_ok(
                &json!({"files": [
                    {"id": "F0001", "user": "U0001", "name": "roadmap.pdf"},
                    {"id": "F0002", "user": "U0002", "name": "notes.txt"}
                ], "paging": {"count": 100, "total": 2, "page": 1, "pages": 1}})
                .to_string(),
            ));
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
            ids_of(&batches),
            vec!["F0001"],
            "exactly U0001's file survives; the ignoring provider's extra row is trimmed"
        );

        let bodies = execute_bodies(&gateway);
        assert!(!bodies.is_empty());
        assert!(
            bodies
                .iter()
                .all(|body| body.contains(r#""userId":"U0001""#)),
            "the predicate is pushed as OC's userId input: {bodies:?}"
        );
        assert!(
            bodies.iter().all(|body| body.contains(r#""count":100"#)),
            "files uses classic page/count pagination: {bodies:?}"
        );
    }

    #[tokio::test]
    async fn created_predicate_is_evaluated_locally_and_never_pushed() {
        // Negative-space guard: OC's list_files contract declares no time
        // input (its strict schema would 400 one), so a `created`
        // predicate must run in DataFusion — correct rows out, and no
        // invented time key in any request.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return MockResponse::ok(&discovery_ok("{}", r#"{"type": "object"}"#, true, None));
            }
            if req.method == "POST" && req.path == "/v1/actions/slack.list_files" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"files": [
                        {"id": "F0001", "created": 1735689599},
                        {"id": "F0002", "created": 1735689600},
                        {"id": "F0003", "created": 1735689601}
                    ], "paging": {"count": 100, "total": 3, "page": 1, "pages": 1}})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let ctx = setup(&gateway, "files", "SKARDI_TEST_OC_SLACK_TSLOCAL").await;

        let batches = collect(
            &ctx,
            "SELECT id FROM saas.ws.files \
             WHERE created >= TIMESTAMP '2025-01-01T00:00:00Z' ORDER BY id",
        )
        .await;
        assert_eq!(
            ids_of(&batches),
            vec!["F0002", "F0003"],
            "the boundary row stays; the older row is filtered locally"
        );

        let bodies = execute_bodies(&gateway);
        assert!(!bodies.is_empty());
        for body in &bodies {
            let input =
                serde_json::from_str::<Value>(body).expect("request body is JSON")["input"].clone();
            assert!(
                input.get("ts_from").is_none()
                    && input.get("tsFrom").is_none()
                    && input.get("created").is_none(),
                "no time key may reach the strict schema: {input}"
            );
        }
    }

    #[tokio::test]
    async fn short_middle_pages_do_not_truncate_a_total_pages_scan() {
        // The motivating case for trusting Slack's authoritative
        // `paging.pages`: permission filtering can legally shorten a
        // non-final page, which the short-page heuristic would read as
        // end-of-collection and silently truncate. Three pages of sizes
        // 2 / 1 / 2 — the short middle page must not end the scan.
        let pages: Vec<Vec<Value>> = vec![
            vec![
                json!({"id": "F0001", "user": "U0001"}),
                json!({"id": "F0002", "user": "U0001"}),
            ],
            vec![json!({"id": "F0003", "user": "U0001"})],
            vec![
                json!({"id": "F0004", "user": "U0001"}),
                json!({"id": "F0005", "user": "U0001"}),
            ],
        ];
        let gateway = MockGateway::start(move |req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return MockResponse::ok(&discovery_ok("{}", r#"{"type": "object"}"#, true, None));
            }
            if req.method == "POST" && req.path == "/v1/actions/slack.list_files" {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                let page = body
                    .get("input")
                    .and_then(|input| input.get("page"))
                    .and_then(Value::as_u64)
                    .unwrap_or(1) as usize;
                let slice = pages.get(page - 1).cloned().unwrap_or_default();
                return MockResponse::ok(&envelope_ok(
                    &json!({"files": slice,
                        "paging": {"count": 100, "total": 5, "page": page, "pages": 3}})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let ctx = setup(&gateway, "files", "SKARDI_TEST_OC_SLACK_SHORT_MIDDLE").await;

        let batches = collect(&ctx, "SELECT id FROM saas.ws.files ORDER BY id").await;
        assert_eq!(
            rows_of(&batches),
            5,
            "the short middle page must not truncate the scan"
        );
        assert_eq!(
            execute_bodies(&gateway).len(),
            3,
            "all three pages fetched, ending at paging.pages"
        );
    }

    #[tokio::test]
    async fn multi_table_binding_and_udtf_parity() {
        // One binding exposes all three tables (no required resources — a
        // pack first), and the query UDTF returns exactly the bound table.
        let gateway = MockGateway::start(workspace_gateway).await;
        let ctx = setup(
            &gateway,
            "conversations, users, files",
            "SKARDI_TEST_OC_SLACK_MULTI",
        )
        .await;

        // All three tables registered under the binding schema.
        let mut tables = ctx
            .catalog("saas")
            .expect("gateway catalog")
            .schema("ws")
            .expect("binding schema")
            .table_names();
        tables.sort();
        assert_eq!(tables, vec!["conversations", "files", "users"]);

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
