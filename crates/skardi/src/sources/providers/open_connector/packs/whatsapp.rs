//! WhatsApp Cloud API source pack: the account side of a WhatsApp Business
//! Account as SQL tables — phone numbers, business profile, message
//! templates, and the three analytics surfaces — over Open Connector's
//! `whatsapp` provider.
//!
//! **The wire contract is Open Connector's, not the Graph API's.** Every
//! action id, input key, row path, and column below was read off a live
//! gateway (v1.9.0, commit `9a2ac5b6`, 2026-10-15) and the provider's
//! executor source (`src/providers/whatsapp/runtime.ts`), then every table's
//! generated input set was sent to that gateway and reached the credential
//! wall — the strict schemas (`additionalProperties: false`) accepted them.
//!
//! Design decisions, per the integration design spec and the source-pack
//! admission gate:
//!
//! - **Messages are not a table, by platform fact.** The Cloud API has no
//!   endpoint for message history: Meta delivers an inbound message once,
//!   by webhook, and the account's only copy is the one the receiver keeps.
//!   OC's `whatsapp` provider faithfully offers no list-messages action.
//!   Messages are the `kind: webhook` receiver's job (`docs/webhooks.md`);
//!   this pack is everything else, and the two are complementary halves of
//!   the WhatsApp design (`docs/superpowers/specs/2026-10-13-whatsapp-integration-design.md`).
//! - **snake_case inputs.** Unlike every pack before it, this provider's
//!   executors forward the Graph API's own parameter spellings:
//!   `waba_id`, `phone_number_id`, `template_ids`, `name_or_content`.
//!   Resource keys are those OC keys verbatim; a camelCase guess would be
//!   a hard 400 against the strict schemas.
//! - **Rows are normalized, and absent means omitted.** Every executor
//!   rebuilds its rows through `compactObject(...)`, which drops keys whose
//!   value is undefined — so a phone number without a webhook
//!   configuration simply lacks the key, and the converter's missing-key
//!   rule turns it into SQL NULL. Explicit nulls do not occur on the wire;
//!   the tests pin the omitted spelling, and the null-parent rule is
//!   pinned as well because the converter's contract covers both.
//! - **Cursor tables ride Meta's Graph cursor.** `get_phone_numbers`,
//!   `get_message_templates`, and `get_template_analytics` forward Meta's
//!   `after` cursor and re-emit Meta's `paging` object
//!   (`cursors.before/after`, `previous`, `next`) through `normalizePaging`,
//!   which drops absent keys. OC emits no has-more boolean, so termination
//!   is Meta's end-of-collection spelling as the engine reads it: an absent
//!   `paging.cursors.after` ends the scan. Which spelling Meta uses on its
//!   real final page — an omitted cursor, or a cursor beside an omitted
//!   `next` — is what the live pass must observe with a one-row page size;
//!   see the pack doc's verification section. A repeated cursor fails as
//!   `PaginationLoop`, never as a silent refetch.
//! - **`template_analytics` takes no page-size input.** The edge pages by
//!   cursor only; its `page_size: 100` is the LIMIT ceiling and never
//!   reaches the wire (pinned by the exact-key e2e).
//! - **Single-page analytics fields.** `analytics` and `pricing_analytics`
//!   are fields on the WABA node that return the whole window in one
//!   response; their contracts declare no cursor, so the tables are
//!   `single_page` with no tripwire path to name.
//! - **`business_profile` is an object row.** The action returns the profile
//!   itself, so the table declares `row_shape: object`, `row_path: "$"`,
//!   and one binding per `phone_number_id` (precedent: Feishu's
//!   `document_content`).
//! - **Window arguments are resources, not pushed filters.** `start`/`end`
//!   are a pair, and the loader refuses a filter whose input is also a
//!   declared resource; declaring them as optional resources lets a binding
//!   fix a window in YAML and an ad hoc `open_connector_query` pass one in
//!   JSON, while a binding that omits them gets OC's rolling 30-day
//!   default. The breakdown lists (`dimensions`, `metric_types`, …) are
//!   arrays and pass through with their JSON types intact.
//! - **Template pushdown is Inexact.** `status`, `category`, and `language`
//!   are forwarded to Meta's server-side filters as string enums; an
//!   out-of-domain literal may be answered with the unfiltered listing, so
//!   DataFusion re-applies every predicate. `name_or_content` is Meta's
//!   substring search, a resource rather than a filter because no scalar
//!   `column op literal` maps to it faithfully. A `name = …` predicate is
//!   deliberately NOT pushed (guard test): the executor has no equality
//!   input for it.
//! - **Opaque JSON where Meta's shape is sparse and type-tagged:**
//!   template `components` (header/body/footer/buttons), and the template
//!   analytics `clicked` / `cost` metric lists.
//! - **Excluded actions, and why:** `get_phone_number` and
//!   `get_template_status` (point reads of rows the listings already
//!   carry), `get_media_info` (a 5-minute download URL that needs a Bearer
//!   header — not table material), every `send_*` / `upload_media` /
//!   `create_*` / `delete_*` (writes; OC sources are read-only in Skardi).
//! - **`error_path: None` everywhere.** `whatsappRequest` throws
//!   `ProviderRequestError` on any non-2xx or in-band Graph error, so the
//!   gateway answers a failure envelope; the e2e pins that a Graph error
//!   surfaces as the gateway's own code, never as a row-path error.
//! - **Nullability:** identity columns non-null (`id`, `name` on
//!   templates, `template_id`, the window bounds), everything else nullable
//!   — the convention every pack follows, kept here because the
//!   normalizers drop unknown-at-Meta fields rather than nulling them.
//! - **Fingerprints pinned from live discovery** (gateway v1.9.0, commit
//!   `9a2ac5b6`), input schemas pinned beside them
//!   (`fixtures/whatsapp/contracts/inputs/`) because the gate is
//!   output-only; a renamed input would otherwise pass registration and
//!   400 every scan.
//! - **Fixtures** are executor-shaped pages pending the live pass, which
//!   replaces them with redacted live captures; the schema-mismatch page is
//!   synthetic by design and says so.

use std::sync::OnceLock;

use crate::sources::providers::open_connector::error::OpenConnectorError;
use crate::sources::providers::open_connector::source_pack::SourcePack;

use super::loader;

static PACK: OnceLock<Result<SourcePack, String>> = OnceLock::new();

/// The WhatsApp pack, parsed once from the embedded YAML asset.
pub fn pack() -> Result<&'static SourcePack, OpenConnectorError> {
    loader::builtin("whatsapp.yaml", include_str!("whatsapp.yaml"), &PACK)
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
        EnvVarGuard, MockGateway, MockResponse, RecordedRequest, collect, column_values,
        convert_first_page, discovery_ok, envelope_err, envelope_ok, execute_inputs,
        fingerprint_uncovered_columns, input_keys, utf8,
    };
    use crate::sources::providers::open_connector::{
        OpenConnectorConfig, OpenConnectorGateways, register_open_connector_tables,
        register_open_connector_udtfs,
    };
    use arrow::array::{Array, Float64Array, Int64Array, ListArray, TimestampMillisecondArray};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use serde_json::{Value, json};

    const ACTIONS: [(&str, &str); 6] = [
        ("phone_numbers", "whatsapp.get_phone_numbers"),
        ("business_profile", "whatsapp.get_business_profile"),
        ("message_templates", "whatsapp.get_message_templates"),
        ("message_analytics", "whatsapp.get_message_analytics"),
        ("pricing_analytics", "whatsapp.get_pricing_analytics"),
        ("template_analytics", "whatsapp.get_template_analytics"),
    ];

    /// Look up a table by short name; the assets are test-pinned to parse.
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
        convert_first_page(table, &page)
    }

    fn contract(action: &str) -> &'static str {
        match action {
            "whatsapp.get_phone_numbers" => {
                include_str!("fixtures/whatsapp/contracts/get_phone_numbers.json")
            }
            "whatsapp.get_business_profile" => {
                include_str!("fixtures/whatsapp/contracts/get_business_profile.json")
            }
            "whatsapp.get_message_templates" => {
                include_str!("fixtures/whatsapp/contracts/get_message_templates.json")
            }
            "whatsapp.get_message_analytics" => {
                include_str!("fixtures/whatsapp/contracts/get_message_analytics.json")
            }
            "whatsapp.get_pricing_analytics" => {
                include_str!("fixtures/whatsapp/contracts/get_pricing_analytics.json")
            }
            "whatsapp.get_template_analytics" => {
                include_str!("fixtures/whatsapp/contracts/get_template_analytics.json")
            }
            _ => r#"{"type": "object"}"#,
        }
    }

    fn input_contract(action: &str) -> &'static str {
        match action {
            "whatsapp.get_phone_numbers" => {
                include_str!("fixtures/whatsapp/contracts/inputs/get_phone_numbers.json")
            }
            "whatsapp.get_business_profile" => {
                include_str!("fixtures/whatsapp/contracts/inputs/get_business_profile.json")
            }
            "whatsapp.get_message_templates" => {
                include_str!("fixtures/whatsapp/contracts/inputs/get_message_templates.json")
            }
            "whatsapp.get_message_analytics" => {
                include_str!("fixtures/whatsapp/contracts/inputs/get_message_analytics.json")
            }
            "whatsapp.get_pricing_analytics" => {
                include_str!("fixtures/whatsapp/contracts/inputs/get_pricing_analytics.json")
            }
            "whatsapp.get_template_analytics" => {
                include_str!("fixtures/whatsapp/contracts/inputs/get_template_analytics.json")
            }
            _ => "{}",
        }
    }

    /// Discovery serving the live-captured contracts, so every mock
    /// registration exercises the fingerprint gate's pass side.
    fn whatsapp_discovery(path: &str) -> MockResponse {
        let action = path.rsplit('/').next().unwrap_or_default();
        MockResponse::ok(&discovery_ok(
            input_contract(action),
            contract(action),
            true,
            None,
        ))
    }

    fn whatsapp_config(token_env: &str, tables: &str, resource: &str) -> OpenConnectorConfig {
        let resource_line = if resource.is_empty() {
            String::new()
        } else {
            format!("resource: {resource}")
        };
        serde_yaml::from_str(&format!(
            r#"
runtime_token_env: {token_env}
bindings:
  - name: wa
    source_pack: whatsapp
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
            Some(&whatsapp_config(token_env, tables, resource)),
            false,
            HierarchyLevel::Catalog,
            Some(&gateways),
        )
        .await
        .expect("gateway registration succeeds");
        register_open_connector_udtfs(&ctx, gateways).expect("UDTF registration succeeds");
        (gateway, ctx)
    }

    /// Health + discovery, then `execute` for every POST.
    fn gateway_with(
        execute: impl Fn(&RecordedRequest) -> Option<MockResponse> + Send + Sync + 'static,
    ) -> impl Fn(&RecordedRequest) -> MockResponse + Send + Sync + 'static {
        move |req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return whatsapp_discovery(&req.path);
            }
            if req.method == "POST"
                && let Some(response) = execute(req)
            {
                return response;
            }
            MockResponse::new(404, "{}")
        }
    }

    fn request_input(req: &RecordedRequest) -> Value {
        let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
        body["input"].clone()
    }

    fn phone_row(id: &str) -> Value {
        json!({"id": id, "display_phone_number": "+1 555-0100", "platform_type": "CLOUD_API"})
    }

    fn template_row(id: &str, status: &str) -> Value {
        json!({"id": id, "name": format!("tpl_{id}"), "status": status, "category": "UTILITY",
               "language": "en_US", "components": []})
    }

    fn timestamps<'a>(batch: &'a RecordBatch, name: &str) -> &'a TimestampMillisecondArray {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("column {name}"))
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap_or_else(|| panic!("{name} is Timestamp(ms)"))
    }

    // ---------------------------------------------------------------
    // Fixture conversions
    // ---------------------------------------------------------------

    #[test]
    fn phone_numbers_fixture_converts_the_normalized_shape() {
        let batch = convert_fixture(
            table("phone_numbers"),
            include_str!("fixtures/whatsapp/phone_numbers.json"),
        );
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(utf8(&batch, "id").value(0), "100000000000001");
        assert_eq!(utf8(&batch, "display_phone_number").value(0), "+1 555-0100");
        assert_eq!(utf8(&batch, "quality_rating").value(0), "GREEN");
        assert_eq!(utf8(&batch, "throughput_level").value(0), "STANDARD");
        assert_eq!(
            utf8(&batch, "webhook_application").value(0),
            "https://hooks.example.com/webhooks/whatsapp-inbound"
        );
        assert_eq!(
            utf8(&batch, "last_onboarded_time").value(0),
            "2026-01-15T08:30:00+0000"
        );
        // Row 2 is the compactObject shape: nested objects omitted → NULL.
        assert!(utf8(&batch, "throughput_level").is_null(1));
        assert!(utf8(&batch, "webhook_application").is_null(1));
        assert!(utf8(&batch, "last_onboarded_time").is_null(1));
        assert_eq!(utf8(&batch, "quality_rating").value(1), "UNKNOWN");
    }

    #[test]
    fn business_profile_fixture_converts_as_one_object_row() {
        let t = table("business_profile");
        assert_eq!(t.row_path, "$");
        let page: Value =
            serde_json::from_str(include_str!("fixtures/whatsapp/business_profile.json"))
                .expect("fixture parses");
        let batch = RowConverter::new(t.fields)
            .expect("converter")
            .convert(std::slice::from_ref(&page), 1)
            .expect("object row converts");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(utf8(&batch, "messaging_product").value(0), "whatsapp");
        assert_eq!(utf8(&batch, "email").value(0), "shop@example.com");
        assert_eq!(utf8(&batch, "vertical").value(0), "RETAIL");
        let websites = batch
            .column_by_name("websites")
            .expect("websites")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("utf8_list");
        assert_eq!(websites.value(0).len(), 2);
    }

    #[test]
    fn message_templates_fixture_converts_with_opaque_components() {
        let batch = convert_fixture(
            table("message_templates"),
            include_str!("fixtures/whatsapp/message_templates.json"),
        );
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(utf8(&batch, "name").value(0), "hello_world");
        assert_eq!(utf8(&batch, "status").value(1), "PENDING");
        assert_eq!(utf8(&batch, "language").value(1), "en");
        let components: Value =
            serde_json::from_str(utf8(&batch, "components").value(0)).expect("json column parses");
        assert_eq!(components[0]["type"], "HEADER");
        assert_eq!(components.as_array().map(Vec::len), Some(3));
    }

    #[test]
    fn message_analytics_fixture_converts_epoch_seconds_to_timestamps() {
        let batch = convert_fixture(
            table("message_analytics"),
            include_str!("fixtures/whatsapp/message_analytics.json"),
        );
        assert_eq!(batch.num_rows(), 2);
        let start = timestamps(&batch, "period_start");
        assert_eq!(start.value(0), 1_759_276_800_000, "seconds × 1000");
        assert_eq!(start.timezone(), Some("UTC"));
        let sent = batch
            .column_by_name("sent")
            .expect("sent")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64");
        assert_eq!(sent.value(0), 196);
        assert_eq!(sent.value(1), 147);
    }

    #[test]
    fn pricing_analytics_fixture_converts_with_optional_dimensions() {
        let batch = convert_fixture(
            table("pricing_analytics"),
            include_str!("fixtures/whatsapp/pricing_analytics.json"),
        );
        assert_eq!(batch.num_rows(), 2);
        let cost = batch
            .column_by_name("cost")
            .expect("cost")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float64");
        assert_eq!(cost.value(0), 40.0);
        assert_eq!(cost.value(1), 0.0);
        assert_eq!(utf8(&batch, "tier").value(0), "0:MAX");
        // Free rows carry no tier: an omitted key, not a null, becomes NULL.
        assert!(utf8(&batch, "tier").is_null(1));
        assert!(
            utf8(&batch, "phone_number").is_null(0),
            "PHONE dimension not requested"
        );
        assert_eq!(utf8(&batch, "pricing_category").value(1), "SERVICE");
    }

    #[test]
    fn template_analytics_fixture_converts_with_opaque_metric_lists() {
        let batch = convert_fixture(
            table("template_analytics"),
            include_str!("fixtures/whatsapp/template_analytics.json"),
        );
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(utf8(&batch, "template_id").value(1), "200000000000002");
        assert!(
            utf8(&batch, "clicked").is_null(0),
            "no buttons clicked → key omitted"
        );
        let clicked: Value =
            serde_json::from_str(utf8(&batch, "clicked").value(1)).expect("json parses");
        assert_eq!(clicked[0]["button_content"], "Contact Support");
        assert_eq!(clicked[0]["count"], 108);
        let cost: Value = serde_json::from_str(utf8(&batch, "cost").value(0)).expect("json parses");
        assert_eq!(cost[1]["type"], "cost_per_delivered");
    }

    #[test]
    fn null_parent_on_a_nested_path_becomes_sql_null() {
        let page = json!({"phone_numbers": [
            {"id": "p-1", "throughput": null, "webhook_configuration": null}
        ]});
        let batch = convert_first_page(table("phone_numbers"), &page);
        assert!(utf8(&batch, "throughput_level").is_null(0));
        assert!(utf8(&batch, "webhook_application").is_null(0));
    }

    #[test]
    fn a_nested_parent_of_the_wrong_shape_fails_rather_than_nulling() {
        let page = json!({"phone_numbers": [{"id": "p-1", "throughput": "STANDARD"}]});
        let t = table("phone_numbers");
        let rows = RowPath::parse(t.row_path)
            .expect("row path")
            .rows(&page, 1)
            .expect("row array");
        let err = RowConverter::new(t.fields)
            .expect("converter")
            .convert(rows, 1)
            .expect_err("a string where an object parent is declared must fail");
        assert!(
            err.to_string().contains("throughput_level"),
            "the column is named: {err}"
        );
    }

    #[test]
    fn every_array_table_converts_an_empty_page_and_keeps_its_schema() {
        for (short, _) in ACTIONS {
            let t = table(short);
            if t.row_path == "$" {
                continue;
            }
            let key = t.row_path.trim_start_matches("$.");
            let page = json!({ key: [] });
            let batch = convert_first_page(t, &page);
            assert_eq!(batch.num_rows(), 0, "{short}");
            assert_eq!(batch.schema().fields().len(), t.fields.len(), "{short}");
        }
    }

    #[test]
    fn message_templates_mismatch_fixture_fails_with_the_targeted_error() {
        let page: Value = serde_json::from_str(include_str!(
            "fixtures/whatsapp/message_templates_type_mismatch.json"
        ))
        .expect("fixture parses");
        let t = table("message_templates");
        let rows = RowPath::parse(t.row_path)
            .expect("row path")
            .rows(&page, 1)
            .expect("row array");
        let err = RowConverter::new(t.fields)
            .expect("converter")
            .convert(rows, 1)
            .expect_err("a number where Utf8 is declared must fail conversion");
        let message = err.to_string();
        assert!(
            !message.contains("42"),
            "the offending value never appears: {message}"
        );
        match err {
            OpenConnectorError::ConversionFailed {
                column,
                path,
                page,
                row,
                expected,
                found,
            } => {
                assert_eq!(column, "id");
                assert_eq!(
                    path, "$.id",
                    "the converter spells row-relative paths from the root"
                );
                assert_eq!(page, 1);
                assert_eq!(row, 1, "the valid first row converts");
                assert_eq!(expected, "string", "the declared type label");
                assert_eq!(found, "a number");
            }
            other => panic!("expected ConversionFailed, got {other}"),
        }
    }

    // ---------------------------------------------------------------
    // Contract pins
    // ---------------------------------------------------------------

    #[test]
    fn pinned_fingerprints_match_the_reconciled_contracts() {
        let mut mismatches = Vec::new();
        for (short, action) in ACTIONS {
            let schema: Value =
                serde_json::from_str(contract(action)).expect("contract fixture parses");
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
    fn generated_inputs_are_accepted_by_the_captured_input_contracts() {
        for (short, action) in ACTIONS {
            let schema: Value =
                serde_json::from_str(input_contract(action)).expect("input contract parses");
            let properties = &schema["properties"];
            let t = table(short);

            assert_eq!(
                schema["additionalProperties"],
                json!(false),
                "{short}: the action's input schema is strict"
            );

            let mut generated: Vec<&str> = t
                .required_resources
                .iter()
                .chain(t.optional_resources)
                .copied()
                .collect();
            generated.extend(t.fixed_inputs.iter().map(|(key, _)| *key));
            generated.extend(t.filters.iter().map(|f| f.input_field));
            match t.pagination {
                PaginationStrategy::Cursor {
                    cursor_param,
                    page_size_param,
                    ..
                } => {
                    generated.push(cursor_param);
                    generated.extend(page_size_param);
                }
                PaginationStrategy::PageNumber {
                    page_param,
                    per_page_param,
                    ..
                } => {
                    generated.push(page_param);
                    generated.push(per_page_param);
                }
                PaginationStrategy::Keyset {
                    cursor_param,
                    page_size_param,
                    ..
                } => {
                    generated.push(cursor_param);
                    generated.push(page_size_param);
                }
                PaginationStrategy::SinglePage { .. } => {}
            }
            for key in &generated {
                assert!(
                    !properties[*key].is_null(),
                    "{short}: `{key}` is not declared by the action's input schema"
                );
            }

            if let Some(required) = schema["required"].as_array() {
                for entry in required {
                    let entry = entry.as_str().expect("required entries are strings");
                    assert!(
                        generated.contains(&entry),
                        "{short}: the action requires `{entry}`, which this table never sends"
                    );
                }
            }

            if let PaginationStrategy::Cursor {
                page_size_param: Some(param),
                page_size,
                ..
            } = t.pagination
            {
                let declared = &properties[param];
                if let Some(minimum) = declared["minimum"].as_u64() {
                    assert!(
                        u64::from(page_size) >= minimum,
                        "{short}: page size below minimum"
                    );
                }
                if let Some(maximum) = declared["maximum"].as_u64() {
                    assert!(
                        u64::from(page_size) <= maximum,
                        "{short}: page size above maximum"
                    );
                }
            }

            for (key, value) in t.fixed_inputs {
                if let (FixedValue::Str(value), Some(variants)) =
                    (value, properties[*key]["enum"].as_array())
                {
                    assert!(
                        variants.iter().any(|v| v.as_str() == Some(*value)),
                        "{short}: fixed input `{key}: {value}` is outside the declared enum"
                    );
                }
            }
        }
    }

    #[test]
    fn columns_the_coverage_walker_cannot_resolve_are_pinned() {
        // Every mapped path is declared by its item schema (the executors'
        // `looseObject`s list the keys they emit), so the fingerprint gate
        // covers every column: an empty gap per table is the pinned fact.
        for (short, action) in ACTIONS {
            let t = table(short);
            assert_eq!(
                fingerprint_uncovered_columns(contract(action), t.row_path, t.fields),
                Vec::<&str>::new(),
                "fingerprint coverage changed for {short}"
            );
        }
    }

    // ---------------------------------------------------------------
    // Wire behaviour through the mock gateway
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn phone_numbers_cursor_scan_pages_with_its_own_declared_inputs() {
        let gateway = MockGateway::start(gateway_with(|req| {
            if req.path != "/v1/actions/whatsapp.get_phone_numbers" {
                return None;
            }
            let input = request_input(req);
            let page = match input.get("after").and_then(Value::as_str) {
                None => json!({"phone_numbers": [phone_row("p-1"), phone_row("p-2")],
                               "paging": {"cursors": {"before": "b1", "after": "c2"}, "next": "https://graph/…"}}),
                // Meta's terminal page: cursors without `after`, no `next`.
                Some("c2") => json!({"phone_numbers": [phone_row("p-3")],
                                     "paging": {"cursors": {"before": "b2"}}}),
                Some(other) => return Some(MockResponse::new(400, format!("bad cursor {other}"))),
            };
            Some(MockResponse::ok(&envelope_ok(&page.to_string())))
        }))
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_WA_PHONES", "phone_numbers", "").await;

        let batches = collect(&ctx, "SELECT id FROM saas.wa.phone_numbers ORDER BY id").await;
        assert_eq!(column_values(&batches, "id"), vec!["p-1", "p-2", "p-3"]);

        let inputs = execute_inputs(&gateway, "whatsapp.get_phone_numbers");
        assert_eq!(inputs.len(), 2, "two cursor pages");
        assert_eq!(inputs[1]["after"], "c2");
        for (page, (input, expected_keys)) in inputs
            .iter()
            .zip([vec!["limit"], vec!["after", "limit"]])
            .enumerate()
        {
            assert_eq!(input["limit"], 100, "page-size on the wire: {input}");
            assert_eq!(input_keys(input), expected_keys, "page {} keys", page + 1);
        }
    }

    #[tokio::test]
    async fn message_templates_scan_terminates_on_an_absent_paging_object() {
        let gateway = MockGateway::start(gateway_with(|req| {
            if req.path != "/v1/actions/whatsapp.get_message_templates" {
                return None;
            }
            let input = request_input(req);
            let page = match input.get("after").and_then(Value::as_str) {
                None => json!({"templates": [template_row("t-1", "APPROVED")],
                               "paging": {"cursors": {"before": "b1", "after": "c2"}}}),
                Some("c2") => json!({"templates": [template_row("t-2", "APPROVED")]}),
                Some(other) => return Some(MockResponse::new(400, format!("bad cursor {other}"))),
            };
            Some(MockResponse::ok(&envelope_ok(&page.to_string())))
        }))
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_WA_TEMPLATES",
            "message_templates",
            r#"{ waba_id: "1234567890" }"#,
        )
        .await;

        let batches = collect(
            &ctx,
            "SELECT id, name FROM saas.wa.message_templates ORDER BY id",
        )
        .await;
        assert_eq!(column_values(&batches, "id"), vec!["t-1", "t-2"]);
        assert_eq!(column_values(&batches, "name"), vec!["tpl_t-1", "tpl_t-2"]);

        let inputs = execute_inputs(&gateway, "whatsapp.get_message_templates");
        assert_eq!(inputs.len(), 2);
        assert_eq!(input_keys(&inputs[0]), vec!["limit", "waba_id"]);
        assert_eq!(input_keys(&inputs[1]), vec!["after", "limit", "waba_id"]);
        assert_eq!(
            inputs[0]["waba_id"], "1234567890",
            "the resource forwards verbatim"
        );
    }

    #[tokio::test]
    async fn template_analytics_scan_pages_by_cursor_and_never_sends_a_page_size() {
        let gateway = MockGateway::start(gateway_with(|req| {
            if req.path != "/v1/actions/whatsapp.get_template_analytics" {
                return None;
            }
            let input = request_input(req);
            let point = |id: &str| {
                json!({"template_id": id, "start": 1759276800, "end": 1759363200,
                       "sent": 1, "delivered": 1, "read": 0})
            };
            let page = match input.get("after").and_then(Value::as_str) {
                None => json!({"data_points": [point("t-1")],
                               "paging": {"cursors": {"before": "b", "after": "c2"}}}),
                Some("c2") => json!({"data_points": [point("t-2")],
                                     "paging": {"cursors": {"before": "b2"}}}),
                Some(other) => return Some(MockResponse::new(400, format!("bad cursor {other}"))),
            };
            Some(MockResponse::ok(&envelope_ok(&page.to_string())))
        }))
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_WA_TEMPLATE_ANALYTICS",
            "template_analytics",
            r#"{ template_ids: ["200000000000001", "200000000000002"], use_waba_timezone: true }"#,
        )
        .await;

        let batches = collect(
            &ctx,
            "SELECT template_id FROM saas.wa.template_analytics ORDER BY template_id",
        )
        .await;
        assert_eq!(column_values(&batches, "template_id"), vec!["t-1", "t-2"]);

        let inputs = execute_inputs(&gateway, "whatsapp.get_template_analytics");
        assert_eq!(inputs.len(), 2);
        assert_eq!(
            input_keys(&inputs[0]),
            vec!["template_ids", "use_waba_timezone"]
        );
        assert_eq!(
            input_keys(&inputs[1]),
            vec!["after", "template_ids", "use_waba_timezone"]
        );
        assert_eq!(
            inputs[0]["template_ids"],
            json!(["200000000000001", "200000000000002"])
        );
        assert_eq!(
            inputs[0]["use_waba_timezone"],
            json!(true),
            "booleans keep their type"
        );
        assert!(
            inputs.iter().all(|i| i.get("limit").is_none()),
            "the edge has no page-size input; page_size is the LIMIT ceiling only"
        );
    }

    #[tokio::test]
    async fn single_page_analytics_tables_issue_exactly_one_request_with_their_resources() {
        let gateway = MockGateway::start(gateway_with(|req| {
            let input = request_input(req);
            match req.path.as_str() {
                "/v1/actions/whatsapp.get_message_analytics" => {
                    assert_eq!(input["granularity"], "MONTH");
                    assert_eq!(input["start"], 1759276800, "integers keep their type");
                    Some(MockResponse::ok(&envelope_ok(
                        &json!({"granularity": "MONTH", "data_points": [
                            {"start": 1759276800, "end": 1761955200, "sent": 10, "delivered": 9}
                        ]})
                        .to_string(),
                    )))
                }
                "/v1/actions/whatsapp.get_pricing_analytics" => {
                    assert_eq!(
                        input["dimensions"],
                        json!(["PHONE", "COUNTRY", "PRICING_CATEGORY", "PRICING_TYPE", "TIER"])
                    );
                    Some(MockResponse::ok(&envelope_ok(
                        &json!({"data_points": [
                            {"start": 1759276800, "end": 1759363200, "phone_number": "15550100",
                             "country": "US", "pricing_category": "UTILITY", "pricing_type": "REGULAR",
                             "tier": "0:2", "volume": 1, "cost": 10}
                        ]})
                        .to_string(),
                    )))
                }
                _ => None,
            }
        }))
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_WA_ANALYTICS",
            "message_analytics, pricing_analytics",
            r#"{ start: 1759276800, end: 1761955200, granularity: MONTH, dimensions: [PHONE, COUNTRY, PRICING_CATEGORY, PRICING_TYPE, TIER] }"#,
        )
        .await;

        let batches = collect(&ctx, "SELECT sent FROM saas.wa.message_analytics").await;
        let sent = batches[0]
            .column_by_name("sent")
            .expect("sent")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64");
        assert_eq!(sent.values(), &[10]);
        let batches = collect(
            &ctx,
            "SELECT phone_number, tier FROM saas.wa.pricing_analytics",
        )
        .await;
        assert_eq!(column_values(&batches, "tier"), vec!["0:2"]);

        let message_inputs = execute_inputs(&gateway, "whatsapp.get_message_analytics");
        assert_eq!(
            message_inputs.len(),
            1,
            "a single-page table makes one request"
        );
        // Only the resources THIS table declares reach its action: the
        // pricing breakdown list is withheld from the messaging field.
        assert_eq!(
            input_keys(&message_inputs[0]),
            vec!["end", "granularity", "start"]
        );
        let pricing_inputs = execute_inputs(&gateway, "whatsapp.get_pricing_analytics");
        assert_eq!(pricing_inputs.len(), 1);
        assert_eq!(
            input_keys(&pricing_inputs[0]),
            vec!["dimensions", "end", "granularity", "start"]
        );
    }

    #[tokio::test]
    async fn business_profile_scans_one_object_row_for_its_phone_number() {
        let profile = include_str!("fixtures/whatsapp/business_profile.json");
        let gateway = MockGateway::start(gateway_with(move |req| {
            if req.path != "/v1/actions/whatsapp.get_business_profile" {
                return None;
            }
            let input = request_input(req);
            assert_eq!(input_keys(&input), vec!["phone_number_id"]);
            assert_eq!(input["phone_number_id"], "100000000000001");
            Some(MockResponse::ok(&envelope_ok(profile)))
        }))
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_WA_PROFILE",
            "business_profile",
            r#"{ phone_number_id: "100000000000001" }"#,
        )
        .await;

        let batches = collect(&ctx, "SELECT vertical, email FROM saas.wa.business_profile").await;
        assert_eq!(column_values(&batches, "vertical"), vec!["RETAIL"]);
        assert_eq!(column_values(&batches, "email"), vec!["shop@example.com"]);
        assert_eq!(
            execute_inputs(&gateway, "whatsapp.get_business_profile").len(),
            1,
            "an object row is one request, never a second page"
        );
    }

    #[tokio::test]
    async fn binding_missing_phone_number_id_fails_before_discovery() {
        let gateway = MockGateway::start(gateway_with(|_| None)).await;
        let token_env = "SKARDI_TEST_OC_WA_MISSING_RESOURCE";
        let _token = EnvVarGuard::set(token_env, "test-token");
        let mut ctx = SessionContext::new();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&whatsapp_config(token_env, "business_profile", "")),
            false,
            HierarchyLevel::Catalog,
            None,
        )
        .await
        .expect_err("a required resource is enforced")
        .downcast::<OpenConnectorError>()
        .expect("typed error");
        assert!(matches!(
            err,
            OpenConnectorError::MissingResourceInput { ref binding, ref key }
                if binding == "wa" && key == "phone_number_id"
        ));
        assert!(
            gateway.requests().iter().all(|r| r.path == "/v1/health"),
            "resource enforcement precedes every discovery call"
        );
    }

    #[tokio::test]
    async fn template_status_filter_is_pushed_and_reapplied_locally() {
        // The stub IGNORES the pushed status (the Inexact contract's hostile
        // provider), so only DataFusion's re-application trims the rows.
        let gateway = MockGateway::start(gateway_with(|req| {
            if req.path != "/v1/actions/whatsapp.get_message_templates" {
                return None;
            }
            Some(MockResponse::ok(&envelope_ok(
                &json!({"templates": [template_row("t-1", "APPROVED"), template_row("t-2", "REJECTED")],
                        "paging": {"cursors": {"before": "b"}}})
                .to_string(),
            )))
        }))
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_WA_FILTER", "message_templates", "").await;

        let batches = collect(
            &ctx,
            "SELECT id FROM saas.wa.message_templates WHERE status = 'REJECTED'",
        )
        .await;
        assert_eq!(column_values(&batches, "id"), vec!["t-2"]);

        let inputs = execute_inputs(&gateway, "whatsapp.get_message_templates");
        assert_eq!(inputs.len(), 1);
        assert_eq!(
            inputs[0]["status"], "REJECTED",
            "pushed as Meta's status input"
        );
        assert_eq!(input_keys(&inputs[0]), vec!["limit", "status"]);
    }

    #[tokio::test]
    async fn a_name_predicate_stays_local_and_never_reaches_the_wire() {
        let gateway = MockGateway::start(gateway_with(|req| {
            if req.path != "/v1/actions/whatsapp.get_message_templates" {
                return None;
            }
            Some(MockResponse::ok(&envelope_ok(
                &json!({"templates": [template_row("t-1", "APPROVED"), template_row("t-2", "APPROVED")],
                        "paging": {"cursors": {"before": "b"}}})
                .to_string(),
            )))
        }))
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_WA_NAME", "message_templates", "").await;

        let batches = collect(
            &ctx,
            "SELECT id FROM saas.wa.message_templates WHERE name = 'tpl_t-2'",
        )
        .await;
        assert_eq!(column_values(&batches, "id"), vec!["t-2"]);
        let inputs = execute_inputs(&gateway, "whatsapp.get_message_templates");
        assert_eq!(
            input_keys(&inputs[0]),
            vec!["limit"],
            "no equality input exists for name; the predicate stays in DataFusion"
        );
    }

    #[tokio::test]
    async fn limit_stops_cursor_pagination_early() {
        let gateway = MockGateway::start(gateway_with(|req| {
            if req.path != "/v1/actions/whatsapp.get_phone_numbers" {
                return None;
            }
            Some(MockResponse::ok(&envelope_ok(
                &json!({"phone_numbers": [phone_row("p-1"), phone_row("p-2")],
                        "paging": {"cursors": {"before": "b", "after": "more"}}})
                .to_string(),
            )))
        }))
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_WA_LIMIT", "phone_numbers", "").await;
        let batches = collect(&ctx, "SELECT id FROM saas.wa.phone_numbers LIMIT 2").await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert_eq!(
            execute_inputs(&gateway, "whatsapp.get_phone_numbers").len(),
            1,
            "LIMIT satisfied by page one stops the scan"
        );
    }

    #[tokio::test]
    async fn a_repeated_cursor_fails_as_a_pagination_loop() {
        let gateway = MockGateway::start(gateway_with(|req| {
            if req.path != "/v1/actions/whatsapp.get_phone_numbers" {
                return None;
            }
            Some(MockResponse::ok(&envelope_ok(
                &json!({"phone_numbers": [phone_row("p-1")],
                        "paging": {"cursors": {"before": "b", "after": "same"}}})
                .to_string(),
            )))
        }))
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_WA_LOOP", "phone_numbers", "").await;
        let err = ctx
            .sql("SELECT id FROM saas.wa.phone_numbers")
            .await
            .expect("plans")
            .collect()
            .await
            .expect_err("a repeated cursor must fail, not refetch forever");
        let message = err.to_string();
        assert!(
            message.contains("pagination loop") && message.contains("same"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn provider_errors_surface_through_the_gateway_failure_envelope() {
        let gateway = MockGateway::start(gateway_with(|req| {
            if req.path != "/v1/actions/whatsapp.get_phone_numbers" {
                return None;
            }
            Some(MockResponse::new(
                403,
                envelope_err(
                    "authorization_failed",
                    "whatsapp get_phone_numbers request failed: (#200) Permissions error",
                ),
            ))
        }))
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_WA_ERR", "phone_numbers", "").await;
        let err = ctx
            .sql("SELECT id FROM saas.wa.phone_numbers")
            .await
            .expect("plans")
            .collect()
            .await
            .expect_err("the gateway's failure envelope is an error");
        let message = err.to_string();
        assert!(
            message.contains("authorization_failed")
                && message.contains("whatsapp.get_phone_numbers"),
            "the gateway's own code and the action are named: {message}"
        );
        assert!(
            !message.contains("row path"),
            "never a row-path error: {message}"
        );
    }

    #[tokio::test]
    async fn udtf_parity_for_phone_numbers() {
        let gateway = MockGateway::start(gateway_with(|req| {
            if req.path != "/v1/actions/whatsapp.get_phone_numbers" {
                return None;
            }
            Some(MockResponse::ok(&envelope_ok(
                &json!({"phone_numbers": [phone_row("p-1"), phone_row("p-2")],
                        "paging": {"cursors": {"before": "b"}}})
                .to_string(),
            )))
        }))
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_WA_UDTF", "phone_numbers", "").await;
        let from_table = collect(
            &ctx,
            "SELECT id, display_phone_number FROM saas.wa.phone_numbers ORDER BY id",
        )
        .await;
        let from_udtf = collect(
            &ctx,
            "SELECT id, display_phone_number FROM open_connector_query('saas', 'whatsapp.phone_numbers', '{}') ORDER BY id",
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
        let token_env = "SKARDI_TEST_OC_WA_DRIFT";
        let _token = EnvVarGuard::set(token_env, "test-token");
        let mut ctx = SessionContext::new();
        let gateways = OpenConnectorGateways::default();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&whatsapp_config(token_env, "phone_numbers", "")),
            false,
            HierarchyLevel::Catalog,
            Some(&gateways),
        )
        .await
        .expect_err("a drifted contract must fail registration")
        .downcast::<OpenConnectorError>()
        .expect("typed error");
        assert!(matches!(
            err,
            OpenConnectorError::ActionContractMismatch { ref table, ref reason }
                if table == "whatsapp.phone_numbers"
                    && reason.contains("whatsapp.get_phone_numbers")
                    && reason.contains("fingerprint mismatch")
        ));
    }
}
