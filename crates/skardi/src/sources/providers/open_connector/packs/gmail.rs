//! Gmail source pack: stable relational contracts over the Open Connector
//! `gmail.*` read actions (Google OAuth, page-token cursor pagination).
//!
//! **The wire contract is Open Connector's, not the Gmail API's.** The
//! gmail executors REBUILD rows the way Slack's do: list rows are
//! normalized objects (`threadId`/`messageId` camelCase identity, the
//! `From`/`To`/`Subject` headers flattened into `sender`/`to`/`subject`,
//! Gmail's epoch-millis `internalDate` re-emitted as an RFC 3339
//! `messageTimestamp`) — mapping Gmail's own field names would have
//! produced all-NULL columns. The two exceptions are `list_labels` and
//! `list_filters`, whose executors pass the provider objects through raw.
//! Inputs are camelCase strict (`pageToken`/`maxResults`/`labelIds`;
//! `additionalProperties: false`, so a wrong key is a hard 400).
//! Everything below is reconciled against a live gateway (v1.3.4) and the
//! OC provider source (`src/providers/gmail/`).
//!
//! Design decisions, per the integration design spec and the source-pack
//! admission gate:
//!
//! - **Cursor pagination on the three listing tables** (`pageToken` in,
//!   top-level `$.nextPageToken` out). The executors never post-filter
//!   rows and explicitly re-emit Gmail's token as `nextPageToken: null`
//!   at end-of-collection (`payload.nextPageToken ?? null`), so both the
//!   null and absent spellings terminate; a repeated token fails as
//!   `PaginationLoop`, a non-string one as `PaginationCursorInvalid` —
//!   never a silent truncation. Page size is Gmail's 500 ceiling, except
//!   `messages` (below).
//! - **`labels` and `filters` are single-page tables.** Their actions
//!   declare no pagination inputs at all (injecting one would be rejected
//!   as `invalid_input` by the strict schema), and the Gmail endpoints
//!   return the complete collection in one response. This pack adds the
//!   loader's `single_page` strategy spelling for exactly this shape —
//!   the engine's `SinglePage` strategy predates it but was unreachable
//!   from YAML.
//! - **`messages` pins `detail: summary` and a page size of 100.**
//!   `summary` is the bounded row shape: `ids` carries no metadata worth
//!   a table, and `full` hydrates entire decoded bodies plus attachment
//!   trees (unbounded row size; content extraction is a future
//!   content-oriented surface, not a listing table). The executor
//!   hydrates every listed message with a metadata `messages.get`
//!   (batches of 10), so a page of N rows costs N+1 Gmail calls — 100
//!   bounds that burst against Gmail's per-user quota; 500 would be a
//!   501-request page.
//! - **No filter pushdown anywhere.** Gmail's `q` is a free-text search
//!   language no SQL predicate maps to faithfully (same call as Notion's
//!   `query`), and `labelIds` is an array with AND semantics that a
//!   scalar `column op literal` mapping cannot represent. Every predicate
//!   runs in DataFusion after the bounded fetch; guard tests pin that
//!   requests carry exactly the declared inputs and nothing else.
//! - **`query`, `labelIds`, `includeSpamTrash` are optional resources**
//!   (on `threads` its schema offers `query` only): the binding-level
//!   spelling for "this mailbox slice", mirroring Slack's optional
//!   `channelId`. They pass through verbatim (arrays stay arrays,
//!   booleans stay booleans). Absent, the tables read as Gmail's default
//!   listing, which EXCLUDES `SPAM` and `TRASH` — documented rather than
//!   pinned away, because `list_threads` offers no spam/trash input, so
//!   pinning `includeSpamTrash: true` on `messages` would make the two
//!   tables describe different mailboxes.
//! - **`verbose` is deliberately never sent** on `threads`/`drafts`: the
//!   executor treats omitted as the summary shape (`=== true` check), and
//!   the hydrated variant costs one `get` per row with unbounded bodies.
//! - **`to` → `to_addresses`**: TO is a reserved SQL keyword; a column
//!   requiring quotes on every reference is hostile to use. The value is
//!   the raw `To` header string (display names and all), exactly as the
//!   executor emits it.
//! - **Header-derived fields spell "absent" as `""`**, never null (the
//!   executor's `readHeader` fallback): empty subjects/recipients stay
//!   empty strings in SQL. `messageTimestamp` always carries an RFC 3339
//!   instant in practice (Gmail's `internalDate` is set on every real
//!   message); the executor's `""` fallback for a hypothetical
//!   dateless message would fail conversion loudly rather than corrupt
//!   the column — the same loud-on-mismatch rule every pack follows.
//!   `threads.historyId` is the one explicitly nullable normalized field
//!   (`?? null`).
//! - **`filters.criteria` / `filters.action` stay opaque JSON**: Gmail's
//!   own sparse matcher/mutation objects, open-ended by design. The
//!   `filters` table needs the `gmail.settings.basic` OAuth scope on the
//!   gateway's connection; every other table reads under
//!   `gmail.readonly` (drafts listing is declared under `gmail.compose`
//!   by the gateway's scope metadata).
//! - **Excluded actions, and why**: `search_threads` (a strict subset of
//!   `list_threads` — required `query`, no cursor, truncated row shape);
//!   `list_history` (requires a `startHistoryId` checkpoint — an
//!   incremental-sync API, not a collection listing);
//!   `list_forwarding_addresses` (declares no output schema to
//!   fingerprint and needs the `gmail.settings.sharing` scope);
//!   `get_profile` (a scalar endpoint, not a row collection).
//! - **`error_path: None` everywhere**: the executors consume Gmail's
//!   in-band error envelope (`assertGmailResponse` throws on non-2xx),
//!   so provider failures arrive as gateway failure envelopes, pinned by
//!   an e2e test.
//! - **Nullability is conservative**: identity fields non-null
//!   (`thread_id`, `message_id` + its `thread_id`, the raw `id`s),
//!   everything else nullable.
//! - **Fingerprints are pinned** from a live gateway capture
//!   (`fixtures/gmail/contracts/`, gateway v1.3.4). `fetch_emails`
//!   declares its row items as an `anyOf` (ids | summary | full) the
//!   coverage walker does not descend, so every `messages` column rides
//!   outside the fingerprint gate — the coverage-gap pin records that
//!   honestly; drift there surfaces at scan time per conversion rules.
//! - **Column sets are verified against REAL wire rows**, end to end
//!   against a live mailbox through the gateway (2026-08-05, gateway
//!   v1.3.4): registration passed the fingerprint gate against live
//!   discovery for all five actions; every mapped column of every table
//!   extracted a real non-NULL value through skardi-server; real
//!   multi-page cursor chaining and real final-page termination were
//!   observed at the wire (`maxResults: 1` chained three pages; the full
//!   listing terminated on the null token); and the
//!   `query`/`labelIds`/`includeSpamTrash` resources were seen narrowing
//!   real listings. Live rows also settled two guesses: which system
//!   labels omit visibility fields (SENT/INBOX/DRAFT/STARRED/UNREAD do;
//!   CHAT does not), and a fresh draft's `threadId` equals its
//!   `messageId`.
//! - **A mailbox with zero filters currently fails the `filters` scan**
//!   with the gateway's `internal_error`: Gmail answers
//!   `settings/filters` with an empty body when no filters exist, and
//!   the upstream executor's unconditional `response.json()` throws
//!   before its own null-tolerant normalization can run — verified live
//!   (creating one filter makes the same call succeed). An upstream fix
//!   belongs in open-connector; until it lands, the pack doc documents
//!   the empty-mailbox caveat.
//! - **Fixtures are redacted live captures** (2026-08-05, same live
//!   pass): real envelope keys, field presence/absence, and timestamp
//!   spellings, with synthetic identifiers and placeholder text, audited
//!   mechanically against an allowlist. The absent-spellings the
//!   executor source guarantees but the capture happened not to contain
//!   (null `historyId`, `""` headers, empty `labelIds`) are pinned by an
//!   inline converter test; the schema-mismatch fixture stays synthetic
//!   and says so.

use std::sync::OnceLock;

use crate::sources::providers::open_connector::error::OpenConnectorError;
use crate::sources::providers::open_connector::source_pack::SourcePack;

use super::loader;

static PACK: OnceLock<Result<SourcePack, String>> = OnceLock::new();

/// The Gmail pack, parsed once from the embedded YAML asset.
pub fn pack() -> Result<&'static SourcePack, OpenConnectorError> {
    loader::builtin("gmail.yaml", include_str!("gmail.yaml"), &PACK)
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
        EnvVarGuard, MockGateway, MockResponse, discovery_ok, envelope_err, envelope_ok,
        fingerprint_uncovered_columns,
    };
    use crate::sources::providers::open_connector::{
        OpenConnectorConfig, OpenConnectorGateways, register_open_connector_tables,
        register_open_connector_udtfs,
    };
    use arrow::array::{Array, ListArray, StringArray, TimestampMillisecondArray};
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
    fn gmail_discovery(path: &str) -> MockResponse {
        let output_schema = if path.ends_with("gmail.list_threads") {
            include_str!("fixtures/gmail/contracts/list_threads.json")
        } else if path.ends_with("gmail.fetch_emails") {
            include_str!("fixtures/gmail/contracts/fetch_emails.json")
        } else if path.ends_with("gmail.list_drafts") {
            include_str!("fixtures/gmail/contracts/list_drafts.json")
        } else if path.ends_with("gmail.list_labels") {
            include_str!("fixtures/gmail/contracts/list_labels.json")
        } else if path.ends_with("gmail.list_filters") {
            include_str!("fixtures/gmail/contracts/list_filters.json")
        } else {
            r#"{"type": "object"}"#
        };
        MockResponse::ok(&discovery_ok("{}", output_schema, true, None))
    }

    // ── Contract tests: provider-shaped fixture pages derived from the
    // executor source and the captured contracts (synthetic until the
    // phase-4 live pass re-derives them as redacted captures). They pin
    // the conversion contract per the admission gate: null-bearing,
    // null-parent, nested, extra-field, and schema-mismatch. ─

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

    #[test]
    fn threads_fixture_converts_the_live_page_shape() {
        // Redacted live capture (2026-08-05): every listed thread carried
        // all three fields — the null/empty spellings the executor can
        // emit are pinned separately by the inline edge-spelling test.
        let batch = convert_fixture(
            table("threads"),
            include_str!("fixtures/gmail/threads.json"),
        );
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(utf8(&batch, "thread_id").value(0), "1900000000000a00");
        assert_eq!(utf8(&batch, "history_id").value(0), "4200001");
        assert!((0..3).all(|i| !utf8(&batch, "snippet").is_null(i)));
    }

    #[test]
    fn messages_fixture_converts_the_live_summary_shape() {
        // Redacted live capture: detail=summary rows carry exactly the
        // seven mapped fields, labelIds is a non-empty array on real mail,
        // and messageTimestamp is the executor's RFC 3339 re-emission of
        // internalDate (millisecond spelling, `.000Z` on real rows).
        let batch = convert_fixture(
            table("messages"),
            include_str!("fixtures/gmail/messages.json"),
        );
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(utf8(&batch, "message_id").value(0), "1900000000000b00");
        let labels: &ListArray = batch
            .column_by_name("label_ids")
            .expect("column")
            .as_any()
            .downcast_ref()
            .expect("List column");
        let third = labels.value(2);
        let third = third
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 items");
        assert_eq!(
            (0..third.len()).map(|i| third.value(i)).collect::<Vec<_>>(),
            vec!["IMPORTANT", "CATEGORY_UPDATES", "INBOX"]
        );
        let ts: &TimestampMillisecondArray = batch
            .column_by_name("message_timestamp")
            .expect("column")
            .as_any()
            .downcast_ref()
            .expect("timestamp");
        assert!((0..3).all(|i| !ts.is_null(i)));
    }

    #[test]
    fn drafts_fixture_converts_through_nested_paths() {
        // Redacted live capture: a draft's message identity nests under
        // `message`, and on the live wire a fresh draft's threadId equals
        // its messageId.
        let batch = convert_fixture(table("drafts"), include_str!("fixtures/gmail/drafts.json"));
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(utf8(&batch, "id").value(0), "r-4000000000000000001");
        assert_eq!(utf8(&batch, "message_id").value(0), "1900000000000c01");
        assert_eq!(utf8(&batch, "thread_id").value(0), "1900000000000c01");
    }

    #[test]
    fn executor_absent_spellings_convert_as_pinned() {
        // Converter pins for the absent spellings the executor source
        // guarantees but the live capture happened not to contain: an
        // absent history checkpoint is an explicit null (`?? null`),
        // header-derived fields and a missing draft-message identity fall
        // back to "" (kept verbatim, never NULL), labelIds defaults to an
        // empty array (an empty list, not NULL), and an undeclared
        // upstream field rides along ignored.
        let batch = convert_page(
            table("threads"),
            &json!({"threads": [
                {"threadId": "t-1", "snippet": "", "historyId": null, "extra": true},
            ]}),
        );
        assert!(utf8(&batch, "history_id").is_null(0));
        assert_eq!(utf8(&batch, "snippet").value(0), "");

        let batch = convert_page(
            table("messages"),
            &json!({"messages": [{
                "messageId": "m-1", "threadId": "t-1", "labelIds": [],
                "subject": "", "sender": "", "to": "",
                "messageTimestamp": "2026-07-30T08:15:42.000Z",
            }]}),
        );
        assert_eq!(utf8(&batch, "subject").value(0), "");
        assert_eq!(utf8(&batch, "to_addresses").value(0), "");
        let labels: &ListArray = batch
            .column_by_name("label_ids")
            .expect("column")
            .as_any()
            .downcast_ref()
            .expect("List column");
        assert!(!labels.is_null(0));
        assert_eq!(labels.value(0).len(), 0);

        let batch = convert_page(
            table("drafts"),
            &json!({"drafts": [{"id": "r-1", "message": {"messageId": "", "threadId": ""}}]}),
        );
        assert_eq!(utf8(&batch, "message_id").value(0), "");
    }

    #[test]
    fn null_parent_on_a_nested_path_becomes_sql_null() {
        // Converter-contract pin, not a wire shape: today's executor
        // always emits `message`, but a nullable column behind a null
        // parent must become SQL NULL (not an error, not a panic) if that
        // ever drifts — the admission gate's null-parent category.
        let batch = convert_page(
            table("drafts"),
            &json!({"drafts": [{"id": "r-1", "message": null}]}),
        );
        assert!(utf8(&batch, "message_id").is_null(0));
        assert!(utf8(&batch, "thread_id").is_null(0));
    }

    #[test]
    fn labels_fixture_converts_with_absent_visibility_and_color() {
        // Redacted live capture (verbatim system labels): raw passthrough
        // rows OMIT keys instead of nulling them, and which labels omit
        // visibility surprised the synthetic guess — on the real wire
        // SENT/INBOX/DRAFT/STARRED/UNREAD carry no visibility fields
        // while CHAT does (hide/labelHide). Omitted keys become SQL NULL;
        // no system label carries color.
        let batch = convert_fixture(table("labels"), include_str!("fixtures/gmail/labels.json"));
        assert_eq!(batch.num_rows(), 15);
        assert_eq!(utf8(&batch, "id").value(0), "CHAT");
        assert_eq!(utf8(&batch, "message_list_visibility").value(0), "hide");
        assert_eq!(utf8(&batch, "id").value(1), "SENT");
        assert!(utf8(&batch, "message_list_visibility").is_null(1));
        assert!(utf8(&batch, "label_list_visibility").is_null(1));
        assert_eq!(utf8(&batch, "id").value(2), "INBOX");
        assert!(utf8(&batch, "message_list_visibility").is_null(2));
        assert!(utf8(&batch, "color").is_null(0));
        // The one user label's color survives as opaque JSON text.
        assert_eq!(utf8(&batch, "type").value(14), "user");
        let color: Value =
            serde_json::from_str(utf8(&batch, "color").value(14)).expect("valid JSON");
        assert_eq!(color["backgroundColor"], "#fb4c2f");
    }

    #[test]
    fn filters_fixture_converts_with_opaque_json() {
        // Redacted live capture: Gmail's sparse criteria/action objects
        // survive as opaque JSON.
        let batch = convert_fixture(
            table("filters"),
            include_str!("fixtures/gmail/filters.json"),
        );
        assert_eq!(batch.num_rows(), 1);
        let criteria: Value =
            serde_json::from_str(utf8(&batch, "criteria").value(0)).expect("valid JSON");
        assert_eq!(criteria["from"], "alerts@example.com");
        let action: Value =
            serde_json::from_str(utf8(&batch, "action").value(0)).expect("valid JSON");
        assert_eq!(action["addLabelIds"][0], "Label_1");
    }

    #[test]
    fn messages_mismatch_fixture_fails_with_the_targeted_error() {
        // Admission-gate schema-mismatch fixture: a number where Utf8 is
        // declared fails with the full row-scoped identity, never a quiet
        // null and never the offending value.
        let page: Value =
            serde_json::from_str(include_str!("fixtures/gmail/messages_type_mismatch.json"))
                .expect("fixture parses");
        let t = table("messages");
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
                assert_eq!(column, "message_id");
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
        // (re)taken after an upstream upgrade.
        let contracts = [
            (
                "threads",
                include_str!("fixtures/gmail/contracts/list_threads.json"),
            ),
            (
                "messages",
                include_str!("fixtures/gmail/contracts/fetch_emails.json"),
            ),
            (
                "drafts",
                include_str!("fixtures/gmail/contracts/list_drafts.json"),
            ),
            (
                "labels",
                include_str!("fixtures/gmail/contracts/list_labels.json"),
            ),
            (
                "filters",
                include_str!("fixtures/gmail/contracts/list_filters.json"),
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
        // fetch_emails declares its row items as an anyOf (ids | summary |
        // full) the coverage walker does not descend, so every messages
        // column rides additionalProperties passthrough — outside the
        // fingerprint gate, drift surfacing at scan time. The other four
        // tables' item schemas are plain objects and fully cover the
        // mapped paths. Pinned so any change is a conscious decision.
        for (short, contract, expected) in [
            (
                "threads",
                include_str!("fixtures/gmail/contracts/list_threads.json"),
                &[] as &[&str],
            ),
            (
                "messages",
                include_str!("fixtures/gmail/contracts/fetch_emails.json"),
                &[
                    "message_id",
                    "thread_id",
                    "label_ids",
                    "subject",
                    "sender",
                    "to_addresses",
                    "message_timestamp",
                ],
            ),
            (
                "drafts",
                include_str!("fixtures/gmail/contracts/list_drafts.json"),
                &[],
            ),
            (
                "labels",
                include_str!("fixtures/gmail/contracts/list_labels.json"),
                &[],
            ),
            (
                "filters",
                include_str!("fixtures/gmail/contracts/list_filters.json"),
                &[],
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

    fn gmail_config(token_env: &str, tables: &str, resource: &str) -> OpenConnectorConfig {
        let resource_line = if resource.is_empty() {
            String::new()
        } else {
            format!("resource: {resource}")
        };
        serde_yaml::from_str(&format!(
            r#"
runtime_token_env: {token_env}
bindings:
  - name: mail
    source_pack: gmail
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
            Some(&gmail_config(token_env, tables, resource)),
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

    fn thread_row(id: &str) -> Value {
        json!({"threadId": id, "snippet": "", "historyId": null})
    }

    fn message_row(id: &str) -> Value {
        json!({
            "messageId": id,
            "threadId": format!("t-{id}"),
            "labelIds": ["INBOX"],
            "subject": "s",
            "sender": "a@example.com",
            "to": "b@example.com",
            "messageTimestamp": "2026-07-30T08:15:42.000Z"
        })
    }

    #[tokio::test]
    async fn threads_cursor_scan_pages_with_its_own_declared_inputs() {
        // Two-page cursor scan pinning THREADS' wire declaration: no
        // pageToken on page 1, the stub's token afterwards, maxResults 500
        // on every request, explicit-null termination (the executor's
        // spelling), row identity across pages, exact key sets.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return gmail_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/gmail.list_threads" {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                let page = match body["input"].get("pageToken").and_then(Value::as_str) {
                    None => json!({"threads": [thread_row("t-1"), thread_row("t-2")],
                                    "nextPageToken": "tok-2", "resultSizeEstimate": 3}),
                    Some("tok-2") => json!({"threads": [thread_row("t-3")],
                                             "nextPageToken": null, "resultSizeEstimate": 3}),
                    Some(other) => return MockResponse::new(400, format!("bad token {other}")),
                };
                return MockResponse::ok(&envelope_ok(&page.to_string()));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_GMAIL_THREADS", "threads", "").await;

        let batches = collect(
            &ctx,
            "SELECT thread_id FROM saas.mail.threads ORDER BY thread_id",
        )
        .await;
        assert_eq!(
            column_values(&batches, "thread_id"),
            vec!["t-1", "t-2", "t-3"]
        );

        let inputs = execute_inputs(&gateway, "gmail.list_threads");
        assert_eq!(inputs.len(), 2, "two cursor pages");
        assert_eq!(inputs[1]["pageToken"], "tok-2");
        for (page, (input, expected_keys)) in inputs
            .iter()
            .zip([vec!["maxResults"], vec!["maxResults", "pageToken"]])
            .enumerate()
        {
            assert_eq!(input["maxResults"], 500, "page-size hint: {input}");
            // Exactly the declared inputs, nothing else — `verbose` and
            // `query` (unbound resource) must never reach the wire.
            assert_eq!(input_keys(input), expected_keys, "page {} keys", page + 1);
        }
    }

    #[tokio::test]
    async fn messages_scan_pins_detail_summary_on_every_page() {
        // Two-page messages scan: the detail=summary fixed input and the
        // bounded maxResults=100 ride EVERY request, and the absent-token
        // spelling of end-of-collection also terminates (the executor
        // emits an explicit null, but the engine accepts both — pinned
        // here so the wire contract has a test per spelling across the
        // pack's tables).
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return gmail_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/gmail.fetch_emails" {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                let page = match body["input"].get("pageToken").and_then(Value::as_str) {
                    None => json!({"messages": [message_row("m-1")],
                                    "nextPageToken": "tok-2", "resultSizeEstimate": 2}),
                    // Final page omits nextPageToken entirely.
                    Some("tok-2") => json!({"messages": [message_row("m-2")],
                                             "resultSizeEstimate": 2}),
                    Some(other) => return MockResponse::new(400, format!("bad token {other}")),
                };
                return MockResponse::ok(&envelope_ok(&page.to_string()));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_GMAIL_MESSAGES", "messages", "").await;

        let batches = collect(
            &ctx,
            "SELECT message_id FROM saas.mail.messages ORDER BY message_id",
        )
        .await;
        assert_eq!(column_values(&batches, "message_id"), vec!["m-1", "m-2"]);

        let inputs = execute_inputs(&gateway, "gmail.fetch_emails");
        assert_eq!(inputs.len(), 2, "two cursor pages");
        for (page, (input, expected_keys)) in inputs
            .iter()
            .zip([
                vec!["detail", "maxResults"],
                vec!["detail", "maxResults", "pageToken"],
            ])
            .enumerate()
        {
            assert_eq!(input["detail"], "summary", "fixed input pin: {input}");
            assert_eq!(input["maxResults"], 100, "bounded page size: {input}");
            assert_eq!(input_keys(input), expected_keys, "page {} keys", page + 1);
        }
    }

    #[tokio::test]
    async fn drafts_cursor_scan_pages_with_its_own_declared_inputs() {
        // Drafts' own wire pin (row path, input keys, maxResults 500) plus
        // the third end-of-collection spelling: an empty-string token.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return gmail_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/gmail.list_drafts" {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                let page = match body["input"].get("pageToken").and_then(Value::as_str) {
                    None => json!({"drafts": [
                        {"id": "d-1", "message": {"messageId": "m-1", "threadId": "t-1"}}],
                        "nextPageToken": "tok-2"}),
                    Some("tok-2") => json!({"drafts": [
                        {"id": "d-2", "message": {"messageId": "m-2", "threadId": "t-2"}}],
                        "nextPageToken": ""}),
                    Some(other) => return MockResponse::new(400, format!("bad token {other}")),
                };
                return MockResponse::ok(&envelope_ok(&page.to_string()));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_GMAIL_DRAFTS", "drafts", "").await;

        let batches = collect(
            &ctx,
            "SELECT id, message_id FROM saas.mail.drafts ORDER BY id",
        )
        .await;
        assert_eq!(column_values(&batches, "id"), vec!["d-1", "d-2"]);
        assert_eq!(column_values(&batches, "message_id"), vec!["m-1", "m-2"]);

        let inputs = execute_inputs(&gateway, "gmail.list_drafts");
        assert_eq!(inputs.len(), 2, "empty-string token terminates");
        assert_eq!(input_keys(&inputs[0]), vec!["maxResults"]);
        assert_eq!(inputs[0]["maxResults"], 500);
        assert_eq!(input_keys(&inputs[1]), vec!["maxResults", "pageToken"]);
    }

    #[tokio::test]
    async fn single_page_tables_issue_exactly_one_request_with_no_inputs() {
        // labels and filters declare the single_page strategy: one POST
        // each, an EMPTY input object (no pagination keys — the strict
        // schema would 400 them — and no userId), and no second request
        // no matter what the response carries.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return gmail_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/gmail.list_labels" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"labels": [
                        {"id": "INBOX", "name": "INBOX", "type": "system"},
                        {"id": "Label_1", "name": "P/Redacted", "type": "user"}]})
                    .to_string(),
                ));
            }
            if req.method == "POST" && req.path == "/v1/actions/gmail.list_filters" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"filters": [{"id": "f-1", "criteria": {"from": "x@example.com"},
                                          "action": {"addLabelIds": ["Label_1"]}}]})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_GMAIL_SINGLE",
            "labels, filters",
            "",
        )
        .await;

        let batches = collect(&ctx, "SELECT id FROM saas.mail.labels ORDER BY id").await;
        assert_eq!(column_values(&batches, "id"), vec!["INBOX", "Label_1"]);
        let batches = collect(&ctx, "SELECT id FROM saas.mail.filters").await;
        assert_eq!(column_values(&batches, "id"), vec!["f-1"]);

        for action in ["gmail.list_labels", "gmail.list_filters"] {
            let inputs = execute_inputs(&gateway, action);
            assert_eq!(inputs.len(), 1, "{action}: single page means one request");
            assert_eq!(
                input_keys(&inputs[0]),
                Vec::<&str>::new(),
                "{action}: empty input object"
            );
        }
    }

    #[tokio::test]
    async fn optional_resources_forward_verbatim_and_only_where_declared() {
        // One binding carries query + labelIds + includeSpamTrash:
        // messages receives all three with their YAML types intact
        // (string, array, boolean), threads receives only its declared
        // `query`, and drafts — which declares no resources — receives
        // none of them. Undeclared keys must never reach a strict schema.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return gmail_discovery(&req.path);
            }
            if req.method == "POST" {
                let empty = match req.path.as_str() {
                    "/v1/actions/gmail.fetch_emails" => {
                        json!({"messages": [], "resultSizeEstimate": 0, "nextPageToken": null})
                    }
                    "/v1/actions/gmail.list_threads" => {
                        json!({"threads": [], "resultSizeEstimate": 0, "nextPageToken": null})
                    }
                    "/v1/actions/gmail.list_drafts" => {
                        json!({"drafts": [], "nextPageToken": null})
                    }
                    _ => return MockResponse::new(404, "{}"),
                };
                return MockResponse::ok(&envelope_ok(&empty.to_string()));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_GMAIL_RESOURCES",
            "threads, messages, drafts",
            r#"{ query: "in:inbox -category:promotions", labelIds: [INBOX, IMPORTANT], includeSpamTrash: true }"#,
        )
        .await;

        for sql in [
            "SELECT * FROM saas.mail.messages",
            "SELECT * FROM saas.mail.threads",
            "SELECT * FROM saas.mail.drafts",
        ] {
            let batches = collect(&ctx, sql).await;
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        }

        let inputs = execute_inputs(&gateway, "gmail.fetch_emails");
        assert_eq!(
            input_keys(&inputs[0]),
            vec![
                "detail",
                "includeSpamTrash",
                "labelIds",
                "maxResults",
                "query"
            ]
        );
        assert_eq!(inputs[0]["query"], "in:inbox -category:promotions");
        assert_eq!(inputs[0]["labelIds"], json!(["INBOX", "IMPORTANT"]));
        assert_eq!(inputs[0]["includeSpamTrash"], json!(true));

        let inputs = execute_inputs(&gateway, "gmail.list_threads");
        assert_eq!(input_keys(&inputs[0]), vec!["maxResults", "query"]);

        let inputs = execute_inputs(&gateway, "gmail.list_drafts");
        assert_eq!(input_keys(&inputs[0]), vec!["maxResults"]);
    }

    #[tokio::test]
    async fn predicates_stay_local_against_a_provider_that_cannot_narrow() {
        // The no-pushdown guard, row identity included: a subject equality
        // predicate never reaches the wire (no filter mappings exist) and
        // DataFusion applies it locally over the full fetched page.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return gmail_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/gmail.fetch_emails" {
                let mut wanted = message_row("m-1");
                wanted["subject"] = json!("needle");
                return MockResponse::ok(&envelope_ok(
                    &json!({"messages": [wanted, message_row("m-2"), message_row("m-3")],
                             "nextPageToken": null, "resultSizeEstimate": 3})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_GMAIL_LOCAL", "messages", "").await;

        let batches = collect(
            &ctx,
            "SELECT message_id FROM saas.mail.messages WHERE subject = 'needle'",
        )
        .await;
        assert_eq!(column_values(&batches, "message_id"), vec!["m-1"]);

        let inputs = execute_inputs(&gateway, "gmail.fetch_emails");
        assert_eq!(
            input_keys(&inputs[0]),
            vec!["detail", "maxResults"],
            "the predicate stayed local; no subject/query key was pushed"
        );
    }

    #[tokio::test]
    async fn limit_stops_cursor_pagination_early() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return gmail_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/gmail.list_threads" {
                // Every page advertises another; only LIMIT can stop this.
                return MockResponse::ok(&envelope_ok(
                    &json!({"threads": [thread_row("t-1"), thread_row("t-2")],
                             "nextPageToken": "again", "resultSizeEstimate": 100})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_GMAIL_LIMIT", "threads", "").await;

        let batches = collect(&ctx, "SELECT thread_id FROM saas.mail.threads LIMIT 2").await;
        assert_eq!(column_values(&batches, "thread_id").len(), 2);
        assert_eq!(
            execute_inputs(&gateway, "gmail.list_threads").len(),
            1,
            "one page satisfied LIMIT"
        );
    }

    #[tokio::test]
    async fn a_repeated_cursor_fails_as_a_pagination_loop() {
        // A gateway that stops advancing must fail loudly, not spin: the
        // engine's already-seen-token guard surfaces through the scan.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return gmail_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/gmail.list_threads" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"threads": [thread_row("t-1")],
                             "nextPageToken": "stuck", "resultSizeEstimate": 2})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_GMAIL_LOOP", "threads", "").await;

        let err = ctx
            .sql("SELECT thread_id FROM saas.mail.threads")
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("a non-advancing cursor must fail the scan");
        let message = err.to_string();
        assert!(
            message.contains("pagination loop") && message.contains("stuck"),
            "loop identity is named: {message}"
        );
    }

    #[tokio::test]
    async fn provider_errors_surface_through_the_gateway_failure_envelope() {
        // error_path is None on purpose: the gmail executors consume the
        // provider's in-band errors and the gateway returns a failure
        // envelope — whose errorCode must reach the user, named.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return gmail_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/gmail.list_filters" {
                return MockResponse::new(
                    403,
                    envelope_err(
                        "authorization_failed",
                        "Request had insufficient authentication scopes.",
                    ),
                );
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_GMAIL_SCOPE", "filters", "").await;

        let err = ctx
            .sql("SELECT id FROM saas.mail.filters")
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("a missing-scope failure must fail the scan");
        let message = err.to_string();
        assert!(
            message.contains("authorization_failed") && message.contains("gmail.list_filters"),
            "the gateway's error code and the action are named: {message}"
        );
        assert!(
            !message.contains("row path"),
            "never the misleading row-path error: {message}"
        );
    }

    #[tokio::test]
    async fn udtf_parity_for_labels() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return gmail_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/gmail.list_labels" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"labels": [{"id": "INBOX", "name": "INBOX", "type": "system"}]})
                        .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_GMAIL_UDTF", "labels", "").await;

        let from_table = collect(&ctx, "SELECT id, name, type FROM saas.mail.labels").await;
        let from_udtf = collect(
            &ctx,
            "SELECT id, name, type FROM open_connector_query('saas', 'gmail.labels', '{}')",
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
        // via gmail_discovery's captured contracts.)
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
        let _token = EnvVarGuard::set("SKARDI_TEST_OC_GMAIL_DRIFT", "test-token");
        let mut ctx = SessionContext::new();
        let gateways = OpenConnectorGateways::default();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&gmail_config("SKARDI_TEST_OC_GMAIL_DRIFT", "threads", "")),
            false,
            HierarchyLevel::Catalog,
            Some(&gateways),
        )
        .await
        .expect_err("a drifted contract must fail registration");
        let message = err.to_string();
        assert!(
            message.contains("gmail.threads")
                && message.contains("gmail.list_threads")
                && message.contains("fingerprint mismatch"),
            "table, action, and cause are named: {message}"
        );
    }
}
