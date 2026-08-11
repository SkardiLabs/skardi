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
//!   maps to the `userId` input as [`Fidelity::Inexact`](crate::sources::providers::open_connector::filters::Fidelity::Inexact), per the
//!   module-wide string-push rule (an Exact claim would lean on the
//!   provider rejecting unknown user IDs instead of silently returning
//!   its default listing). The previous `created >= → ts_from` push is
//!   **gone**: OC's `slack.list_files` contract declares no time input at
//!   all, and its strict schema would 400 any request carrying one — time
//!   predicates are evaluated by DataFusion after the bounded fetch.
//! - **`files.created` is epoch seconds**, read through
//!   [`FieldType::TimestampSecondsUtc`](crate::sources::providers::open_connector::json_to_arrow::FieldType::TimestampSecondsUtc) — the millis reader would
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
//! - **Fingerprints are pinned** from a live gateway (v1.3.1): each
//!   `expected_fingerprint` is the BLAKE3 hash of the canonicalized
//!   output schema captured into `fixtures/slack/contracts/`, and a test
//!   keeps pin and captured contract locked together. Registration
//!   compares the pin against the discovered contract and fails with
//!   `ActionContractMismatch` on drift — including additive or
//!   doc-comment-only schema changes, which is the designed tradeoff
//!   (re-capture the contract and re-pin on upstream upgrades). The
//!   GitHub pack pins the same way.

use std::sync::OnceLock;

use crate::sources::providers::open_connector::error::OpenConnectorError;
use crate::sources::providers::open_connector::source_pack::SourcePack;

use super::loader;

static PACK: OnceLock<Result<SourcePack, String>> = OnceLock::new();

/// The Slack pack, parsed once from the embedded YAML asset.
pub fn pack() -> Result<&'static SourcePack, OpenConnectorError> {
    loader::builtin("slack.yaml", include_str!("slack.yaml"), &PACK)
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
        EnvVarGuard, MockGateway, MockResponse, RecordedRequest, discovery_ok, envelope_err,
        envelope_ok,
    };
    use crate::sources::providers::open_connector::{
        OpenConnectorConfig, OpenConnectorError, OpenConnectorGateways,
        register_open_connector_tables, register_open_connector_udtfs,
    };
    use arrow::array::{
        Array, BooleanArray, ListArray, StringArray, TimestampMillisecondArray, UInt64Array,
    };
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

    /// Discovery response carrying the live-captured output schema for the
    /// three slack actions (`fixtures/slack/contracts/`), so every mock
    /// registration exercises the same fingerprint gate the real gateway
    /// does — a stub schema would fail the pinned tables at registration.
    fn slack_discovery(path: &str) -> MockResponse {
        let output_schema = if path.ends_with("slack.list_conversations") {
            include_str!("fixtures/slack/contracts/list_conversations.json")
        } else if path.ends_with("slack.list_users") {
            include_str!("fixtures/slack/contracts/list_users.json")
        } else if path.ends_with("slack.list_files") {
            include_str!("fixtures/slack/contracts/list_files.json")
        } else {
            r#"{"type": "object"}"#
        };
        MockResponse::ok(&discovery_ok("{}", output_schema, true, None))
    }

    #[test]
    fn fingerprint_coverage_gap_is_pinned() {
        // conversations and users are fully covered — the normalizing
        // executors declare every emitted field. files rows are raw Slack
        // passthrough plus aliases, so most mapped columns ride
        // additionalProperties and sit outside the fingerprint gate; their
        // drift surfaces at scan time per conversion rules. Pinned so any
        // change is a conscious decision.
        use crate::sources::providers::open_connector::testutil::fingerprint_uncovered_columns;
        for (short, contract, expected) in [
            (
                "conversations",
                include_str!("fixtures/slack/contracts/list_conversations.json"),
                &[] as &[&str],
            ),
            (
                "users",
                include_str!("fixtures/slack/contracts/list_users.json"),
                &[],
            ),
            (
                "files",
                include_str!("fixtures/slack/contracts/list_files.json"),
                &[
                    "id",
                    "filetype",
                    "size",
                    "user_id",
                    "channels",
                    "is_public",
                    "created",
                    "permalink",
                ],
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

    #[test]
    fn pinned_fingerprints_match_the_reconciled_contracts() {
        // Each pin is the BLAKE3 hash of the canonicalized output schema
        // captured from a live gateway (v1.3.1) into
        // `fixtures/slack/contracts/`. This test locks pin and captured
        // contract together: refreshing one without the other fails here,
        // and drift in the live gateway fails registration with
        // `ActionContractMismatch` instead of surfacing at scan time.
        let mut mismatches = Vec::new();
        for (table, contract) in [
            (
                table("conversations"),
                include_str!("fixtures/slack/contracts/list_conversations.json"),
            ),
            (
                table("users"),
                include_str!("fixtures/slack/contracts/list_users.json"),
            ),
            (
                table("files"),
                include_str!("fixtures/slack/contracts/list_files.json"),
            ),
        ] {
            let schema: Value = serde_json::from_str(contract).expect("contract fixture parses");
            let actual = fingerprint_schema(Some(&schema));
            if table.expected_fingerprint != Some(actual.as_str()) {
                mismatches.push(format!(
                    "{}: pinned {:?}, contract fixture hashes to {actual}",
                    table.id, table.expected_fingerprint
                ));
            }
        }
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }

    #[tokio::test]
    async fn drifted_contract_fails_registration_not_the_scan() {
        // The other half of the pin: a gateway whose discovered output
        // schema differs from the captured contract must be refused at
        // REGISTRATION with the table and action named — before any scan
        // could return silently reshaped data. (Every other test in this
        // module proves the pass side: their stubs serve the captured
        // contracts and register successfully.)
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                // A drifted contract: not the captured schema.
                return MockResponse::ok(&discovery_ok("{}", r#"{"type": "object"}"#, true, None));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let _token = EnvVarGuard::set("SKARDI_TEST_OC_SLACK_DRIFT", "test-token");
        let config: OpenConnectorConfig = serde_yaml::from_str(
            r#"
runtime_token_env: SKARDI_TEST_OC_SLACK_DRIFT
bindings:
  - name: ws
    source_pack: slack
    tables: [conversations]
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
        .expect_err("a drifted contract must fail registration");
        let message = err.to_string();
        assert!(
            message.contains("slack.conversations")
                && message.contains("slack.list_conversations")
                && message.contains("fingerprint mismatch"),
            "the table, action, and mismatch are named: {message}"
        );
    }

    // ── Contract tests: bundled redacted fixtures are the build-time
    // conversion contract (null-bearing, nested, empty, extra upstream
    // fields, and a schema mismatch per the source-pack admission gate). ─

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
            table("conversations"),
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
    fn conversations_mismatch_fixture_fails_with_the_targeted_error() {
        // Admission-gate schema-mismatch fixture: the normalizer turning
        // memberCount into a string is incompatible drift and must fail the
        // scan with the full (column, page, row, expected, found-kind)
        // identity — never a quiet null (the legitimate NULL path is an
        // OMITTED memberCount, pinned above), and never the value itself.
        let page: Value = serde_json::from_str(include_str!(
            "fixtures/slack/conversations_type_mismatch.json"
        ))
        .expect("fixture parses");
        let rows = RowPath::parse(table("conversations").row_path)
            .expect("row path")
            .rows(&page, 1)
            .expect("row array");
        let err = RowConverter::new(table("conversations").fields)
            .expect("converter")
            .convert(rows, 1)
            .expect_err("a string where UInt64 is declared must fail conversion");
        match err {
            OpenConnectorError::ConversionFailed {
                column,
                path,
                page,
                row,
                expected,
                found,
            } => {
                assert_eq!(column, "member_count");
                assert_eq!(path, "$.memberCount");
                assert_eq!(page, 1);
                assert_eq!(
                    row, 1,
                    "the valid first row converts; the error names the bad row"
                );
                assert_eq!(expected, "non-negative integer");
                assert_eq!(found, "string");
            }
            other => panic!("expected ConversionFailed, got {other}"),
        }
    }

    #[test]
    fn users_fixture_converts_with_flattened_profile_and_deleted_rows() {
        let batch = convert_fixture(table("users"), include_str!("fixtures/slack/users.json"));
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
        let batch = convert_fixture(table("files"), include_str!("fixtures/slack/files.json"));
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
            (
                table("conversations"),
                r#"{"conversations":[],"nextCursor":null}"#,
            ),
            (table("users"), r#"{"users":[],"nextCursor":null}"#),
            (table("files"), r#"{"files":[],"paging":{"pages":0}}"#),
        ] {
            let batch = convert_fixture(table, empty);
            assert_eq!(batch.num_rows(), 0);
            assert_eq!(batch.num_columns(), table.fields.len(), "{}", table.id);
        }
    }

    #[test]
    fn every_table_binds_and_declares_a_complete_contract() {
        for table in pack().expect("embedded asset parses").tables {
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
        assert_eq!(
            pack().expect("embedded asset parses").tables.len(),
            3,
            "messages/threads stay gated"
        );
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
                return slack_discovery(&req.path);
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
        // Registration reads the token from the environment; the guard
        // restores the prior state when setup returns (or panics).
        let _token = EnvVarGuard::set(token_env, "test-token");
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
        register_open_connector_udtfs(&ctx, gateways).expect("UDTF registration succeeds");
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
                return slack_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/slack.list_conversations" {
                return MockResponse::new(
                    502,
                    envelope_err("provider_error", "slack error: missing_scope"),
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
    async fn non_string_cursor_fails_the_scan_instead_of_truncating() {
        // A gateway that hands back `nextCursor: 123` has broken the cursor
        // contract. The page's rows must NOT be returned as a complete
        // result — that would silently truncate the collection — and the
        // error must name the cursor path, not a row problem.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return slack_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/slack.list_conversations" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"conversations": [
                        {"channelId": "C0001", "name": "general", "type": "public_channel"}
                    ], "nextCursor": 123})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let ctx = setup(&gateway, "conversations", "SKARDI_TEST_OC_SLACK_BAD_CURSOR").await;

        let err = ctx
            .sql("SELECT id FROM saas.ws.conversations")
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("a non-string cursor must fail the scan, not truncate it");
        let message = err.to_string();
        assert!(
            message.contains("$.nextCursor") && message.contains("not a string"),
            "the cursor path and the type problem are named: {message}"
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
            return slack_discovery(&req.path);
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
                return slack_discovery(&req.path);
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
                return slack_discovery(&req.path);
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

    #[tokio::test]
    async fn users_cursor_scan_pages_with_its_own_declared_inputs() {
        // The multi-page cursor path is pinned end-to-end for conversations;
        // users shares the strategy CONSTANT but not the wire declarations.
        // This scan pins USERS' own row path ($.users) and inputs (cursor /
        // limit 200) across two pages, so a drifted declaration on either
        // table cannot hide behind the other's coverage.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return slack_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/slack.list_users" {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                let page = match body["input"].get("cursor").and_then(Value::as_str) {
                    None => json!({"users": [
                        {"userId": "U0001", "username": "ada", "isBot": false},
                        {"userId": "U0002", "username": "deploybot", "isBot": true}
                    ], "nextCursor": "users-page-2"}),
                    Some("users-page-2") => json!({"users": [
                        {"userId": "U0003", "username": "grace", "isBot": false}
                    ], "nextCursor": null}),
                    Some(other) => {
                        return MockResponse::new(400, format!("unexpected cursor {other}"));
                    }
                };
                return MockResponse::ok(&envelope_ok(&page.to_string()));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let ctx = setup(&gateway, "users", "SKARDI_TEST_OC_SLACK_USERS_CURSOR").await;

        let batches = collect(&ctx, "SELECT id FROM saas.ws.users ORDER BY id").await;
        assert_eq!(ids_of(&batches), vec!["U0001", "U0002", "U0003"]);

        let inputs: Vec<Value> = execute_bodies(&gateway)
            .iter()
            .map(|body| {
                serde_json::from_str::<Value>(body).expect("request body is JSON")["input"].clone()
            })
            .collect();
        assert_eq!(inputs.len(), 2, "two cursor pages");
        assert!(
            inputs[0].get("cursor").is_none(),
            "page 1 sends no cursor: {}",
            inputs[0]
        );
        assert_eq!(inputs[1]["cursor"], "users-page-2");
        for input in &inputs {
            assert_eq!(input["limit"], 200, "the users page-size hint: {input}");
        }
    }
}
