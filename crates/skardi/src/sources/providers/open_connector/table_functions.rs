//! The two Open Connector UDTFs — the ad-hoc SQL surface of the gateway.
//!
//! - `open_connector_query(gateway, table_id, resource_json[, alias])` runs
//!   a **built-in source-pack table** without a persistent YAML binding. It
//!   compiles into exactly the scan a YAML-bound table uses: same stable
//!   schema, filter allowlist, pagination, safety bounds, and shared cache.
//! - `open_connector_scan(gateway, action_id, input_json, row_path[, alias])`
//!   invokes an **explicitly allowlisted raw read action** once (no
//!   pagination contract), deriving a deterministic row type from the
//!   discovered action output schema or failing at planning time.
//!
//! Both resolve against planning-time state captured when the gateway was
//! registered ([`GatewayHandle`]) — query planning never performs network
//! I/O, so an action that was not discovered at registration is a targeted
//! planning error, not a hidden gateway call.
//!
//! Security is default-deny and enforced here, before any HTTP request:
//! `open_connector_query` accepts only built-in pack definitions (their
//! read actions are hard-coded in Skardi), and `open_connector_scan`
//! requires the action to be allowlisted **and** classified as a
//! non-mutating read by its discovered gateway metadata — absent or
//! ambiguous classification is refused, naming the gap.
//!
//! Staleness boundary: because planning never re-discovers, the metadata
//! these gates read (read-only classification, executability, contract
//! fingerprints) is a snapshot from registration. An upstream action that
//! turns mutating *after* registration keeps passing the Skardi-side gate
//! until the next restart or configuration reload; Open Connector's own
//! action policies are the live, independent enforcement boundary for that
//! window.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{plan_datafusion_err, plan_err};
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use serde_json::Value;

use super::action_registry::{ActionMetadata, ActionRegistry};
use super::cache::ScanCache;
use super::client::OpenConnectorClient;
use super::config::OpenConnectorConfig;
use super::error::OpenConnectorError;
use super::exec::{OpenConnectorExec, ScanTarget};
use super::json_to_arrow::RowConverter;
use super::pagination::PaginationStrategy;
use super::raw_schema::derive_raw_columns;
use super::row_path::RowPath;
use super::source_pack::SourcePackRegistry;
use super::table::OpenConnectorTableProvider;
use crate::sources::providers::udtf_args::strict_string_arg;

/// Planning-time state of one registered gateway, captured by
/// `register_open_connector_tables` and shared with both UDTFs.
pub struct GatewayHandle {
    client: Arc<OpenConnectorClient>,
    cache: Arc<ScanCache>,
    actions: Arc<ActionRegistry>,
    raw_action_allowlist: HashSet<String>,
    max_pages: u32,
    max_rows: u64,
    scan_timeout: Duration,
}

impl std::fmt::Debug for GatewayHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayHandle")
            .field("actions", &self.actions.len())
            .field("raw_action_allowlist", &self.raw_action_allowlist)
            .finish()
    }
}

impl GatewayHandle {
    /// Capture one registered gateway's planning-time state.
    pub(crate) fn new(
        client: Arc<OpenConnectorClient>,
        cache: Arc<ScanCache>,
        actions: Arc<ActionRegistry>,
        config: &OpenConnectorConfig,
    ) -> Self {
        Self {
            client,
            cache,
            actions,
            raw_action_allowlist: config.raw_action_allowlist.iter().cloned().collect(),
            max_pages: config.max_pages,
            max_rows: config.max_rows,
            scan_timeout: Duration::from_secs(config.scan_timeout_seconds),
        }
    }
}

/// Shared map of gateway (data source) name → [`GatewayHandle`]. Owned by
/// the front-end (server `OptimizerRegistry` / CLI main) the way the KNN/FTS
/// `DatasetRegistry` is, filled by `register_open_connector_tables`, and
/// read by both UDTFs at planning time.
pub type OpenConnectorGateways = Arc<RwLock<HashMap<String, Arc<GatewayHandle>>>>;

/// Register `open_connector_query` and `open_connector_scan` on a session.
pub fn register_open_connector_udtfs(ctx: &SessionContext, gateways: OpenConnectorGateways) {
    ctx.register_udtf(
        "open_connector_query",
        Arc::new(OpenConnectorQueryFunction::new(Arc::clone(&gateways))),
    );
    ctx.register_udtf(
        "open_connector_scan",
        Arc::new(OpenConnectorScanFunction::new(gateways)),
    );
}

/// `open_connector_query('gateway', 'pack.table', '{resource json}'[, 'alias'])`.
#[derive(Debug)]
pub struct OpenConnectorQueryFunction {
    gateways: OpenConnectorGateways,
    packs: SourcePackRegistry,
}

impl OpenConnectorQueryFunction {
    /// Build the function over the shared gateway map.
    pub fn new(gateways: OpenConnectorGateways) -> Self {
        Self {
            gateways,
            packs: SourcePackRegistry::builtins(),
        }
    }
}

impl TableFunctionImpl for OpenConnectorQueryFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if exprs.len() < 3 || exprs.len() > 4 {
            return plan_err!(
                "open_connector_query(gateway, table_id, resource_json, [connection_alias]) \
                 expects 3-4 arguments, got {}",
                exprs.len()
            );
        }
        let gateway = strict_string_arg(&exprs[0], "open_connector_query", "gateway")?;
        let table_id = strict_string_arg(&exprs[1], "open_connector_query", "table_id")?;
        let resource_json = strict_string_arg(&exprs[2], "open_connector_query", "resource_json")?;
        let alias = exprs
            .get(3)
            .map(|expr| strict_string_arg(expr, "open_connector_query", "connection_alias"))
            .transpose()?;

        let handle = lookup_gateway(&self.gateways, &gateway)?;

        // `pack.table` resolves against the built-in pack registry only —
        // there is no way to name an action here, so the function can execute
        // nothing but the read actions hard-coded in Skardi's packs.
        let Some((pack_name, table_name)) = table_id.split_once('.') else {
            return plan_err!(
                "open_connector_query: table_id '{table_id}' must be '<pack>.<table>', \
                 e.g. 'github.issues'"
            );
        };
        let pack = self.packs.require(pack_name).map_err(plan_error)?;
        let table = self.packs.table(pack, table_name).map_err(plan_error)?;

        let resource = parse_json_object(
            "open_connector_query",
            "resource_json",
            &resource_json,
            "resource inputs",
        )?;
        for key in table.required_resources {
            if resource.get(*key).is_none() {
                return Err(plan_error(OpenConnectorError::MissingResourceInput {
                    binding: format!("open_connector_query('{gateway}', '{table_id}')"),
                    key: (*key).to_string(),
                }));
            }
        }

        // Same discovery and compatibility gates as YAML registration: the
        // action must have been discovered when the gateway registered, and
        // must still match the pack's expected contract fingerprint.
        let meta = discovered_action(&handle, &gateway, table.action_id)?;
        if let Some(expected) = table.expected_fingerprint
            && meta.fingerprint() != expected
        {
            return Err(plan_error(OpenConnectorError::ActionContractMismatch {
                table: table.id.to_string(),
                reason: format!(
                    "action '{}' fingerprint mismatch (expected {expected}, discovered {})",
                    table.action_id,
                    meta.fingerprint()
                ),
            }));
        }

        let provider = OpenConnectorTableProvider::new(
            Arc::clone(&handle.client),
            Some(Arc::clone(&handle.cache)),
            gateway,
            None,
            alias,
            table,
            pack.version,
            resource,
            handle.max_pages,
            handle.max_rows,
            handle.scan_timeout,
        )
        .map_err(plan_error)?;
        Ok(Arc::new(provider))
    }
}

/// `open_connector_scan('gateway', 'action.id', '{input json}', '$.rows'[, 'alias'])`.
#[derive(Debug)]
pub struct OpenConnectorScanFunction {
    gateways: OpenConnectorGateways,
}

impl OpenConnectorScanFunction {
    /// Build the function over the shared gateway map.
    pub fn new(gateways: OpenConnectorGateways) -> Self {
        Self { gateways }
    }
}

impl TableFunctionImpl for OpenConnectorScanFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if exprs.len() < 4 || exprs.len() > 5 {
            return plan_err!(
                "open_connector_scan(gateway, action_id, input_json, row_path, \
                 [connection_alias]) expects 4-5 arguments, got {}",
                exprs.len()
            );
        }
        let gateway = strict_string_arg(&exprs[0], "open_connector_scan", "gateway")?;
        let action_id = strict_string_arg(&exprs[1], "open_connector_scan", "action_id")?;
        let input_json = strict_string_arg(&exprs[2], "open_connector_scan", "input_json")?;
        let row_path = strict_string_arg(&exprs[3], "open_connector_scan", "row_path")?;
        let alias = exprs
            .get(4)
            .map(|expr| strict_string_arg(expr, "open_connector_scan", "connection_alias"))
            .transpose()?;

        let handle = lookup_gateway(&self.gateways, &gateway)?;

        // Default-deny, before anything else: only explicitly allowlisted
        // actions are even looked up.
        if !handle.raw_action_allowlist.contains(&action_id) {
            return Err(plan_error(OpenConnectorError::RawActionNotAllowlisted {
                gateway,
                action_id,
            }));
        }
        // The allowlist alone does not grant execution: the discovered
        // metadata must classify the action as a non-mutating read. Both
        // rejections fire at planning time, before any HTTP request.
        let meta = discovered_action(&handle, &gateway, &action_id)?;
        match meta.read_only() {
            Some(true) => {}
            Some(false) => {
                return Err(plan_error(OpenConnectorError::RawActionMutating {
                    action_id,
                }));
            }
            None => {
                return Err(plan_error(OpenConnectorError::RawActionReadOnlyUnknown {
                    action_id,
                }));
            }
        }

        let input = parse_json_object(
            "open_connector_scan",
            "input_json",
            &input_json,
            "action inputs",
        )?;

        let row_path = RowPath::parse(&row_path).map_err(plan_error)?;
        // Deterministic row type or planning error — derived purely from the
        // in-memory discovered output schema.
        let columns =
            derive_raw_columns(&action_id, meta.output_schema(), &row_path).map_err(plan_error)?;
        let converter = Arc::new(RowConverter::from_columns(columns).map_err(plan_error)?);

        Ok(Arc::new(RawScanProvider {
            client: Arc::clone(&handle.client),
            gateway,
            connection_alias: alias,
            target: ScanTarget {
                table_id: Arc::from(format!("raw:{action_id}")),
                action_id: Arc::from(action_id),
                pagination: PaginationStrategy::SinglePage,
                source_pack_version: 0,
            },
            converter,
            row_path,
            input,
            max_pages: handle.max_pages,
            max_rows: handle.max_rows,
            scan_timeout: handle.scan_timeout,
        }))
    }
}

/// One planned raw-action scan: a single live execution of an allowlisted
/// read action, converted through the planning-time derived schema.
///
/// No cache (raw scans are always live reads — with no pagination contract
/// there is no "complete result" to store) and no filter pushdown (raw
/// actions declare no filter allowlist; callers pass provider filters in the
/// input JSON, and SQL predicates stay in DataFusion).
struct RawScanProvider {
    client: Arc<OpenConnectorClient>,
    gateway: String,
    connection_alias: Option<String>,
    target: ScanTarget,
    converter: Arc<RowConverter>,
    row_path: RowPath,
    input: Value,
    max_pages: u32,
    max_rows: u64,
    scan_timeout: Duration,
}

impl std::fmt::Debug for RawScanProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawScanProvider")
            .field("action", &self.target.action_id)
            .field("gateway", &self.gateway)
            .finish()
    }
}

#[async_trait]
impl TableProvider for RawScanProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(self.converter.schema())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let exec = OpenConnectorExec::new(
            Arc::clone(&self.client),
            None,
            self.gateway.clone(),
            None,
            self.connection_alias.clone(),
            self.target.clone(),
            Arc::clone(&self.converter),
            self.row_path.clone(),
            self.input.clone(),
            Vec::new(),
            projection.cloned(),
            limit,
            self.max_pages,
            self.max_rows,
            self.scan_timeout,
        )?;
        Ok(Arc::new(exec))
    }
}

/// Resolve a gateway name against the shared map.
fn lookup_gateway(gateways: &OpenConnectorGateways, name: &str) -> DFResult<Arc<GatewayHandle>> {
    gateways
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .get(name)
        .cloned()
        .ok_or_else(|| {
            plan_error(OpenConnectorError::UdtfGatewayNotRegistered {
                name: name.to_string(),
            })
        })
}

/// Look up one discovered action's metadata, cloning it out of the handle.
fn discovered_action(
    handle: &GatewayHandle,
    gateway: &str,
    action_id: &str,
) -> DFResult<ActionMetadata> {
    handle.actions.get(action_id).cloned().ok_or_else(|| {
        plan_error(OpenConnectorError::ActionNotDiscovered {
            gateway: gateway.to_string(),
            action_id: action_id.to_string(),
        })
    })
}

/// UDTF failures surface as planning errors; the typed error's message is
/// the user-facing diagnostic.
fn plan_error(e: OpenConnectorError) -> DataFusionError {
    DataFusionError::Plan(e.to_string())
}

/// Parse a UDTF argument that must carry a JSON object, shared by both
/// functions so the two diagnostics stay in lockstep. `noun` names what the
/// object holds in the caller's vocabulary ("resource inputs" / "action
/// inputs"), so sharing the implementation doesn't flatten the context.
fn parse_json_object(fn_name: &str, arg: &str, raw: &str, noun: &str) -> DFResult<Value> {
    let value: Value = serde_json::from_str(raw)
        .map_err(|e| plan_datafusion_err!("{fn_name}: {arg} is not valid JSON: {e}"))?;
    if !value.is_object() {
        return plan_err!(
            "{fn_name}: {arg} must be a JSON object of {noun}, \
             e.g. '{{\"owner\":\"SkardiLabs\",\"repo\":\"skardi\"}}'"
        );
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::hierarchy::HierarchyLevel;
    use crate::sources::providers::open_connector::register_open_connector_tables;
    use crate::sources::providers::open_connector::testutil::{
        MockGateway, MockResponse, RecordedRequest,
    };
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::prelude::CsvReadOptions;
    use std::io::Write;

    /// Discovery metadata for `mock.list_items`, with a controllable
    /// read-only classification and an output schema matching the items the
    /// mock gateway serves.
    fn discovery_response(read_only: Option<bool>, output_schema: &str) -> String {
        let read_only = match read_only {
            Some(flag) => format!(r#""read_only": {flag},"#),
            None => String::new(),
        };
        format!(
            r#"{{"input_schema": {{}}, "output_schema": {output_schema},
                "locally_executable": true, {read_only} "connection_aliases": ["work"]}}"#
        )
    }

    /// Output schema describing the mock item rows.
    const ITEMS_OUTPUT_SCHEMA: &str = r#"{
        "type": "object",
        "properties": {
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {"type": "string"},
                        "value": {"type": "number"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "created_at": {"type": "string"}
                    }
                }
            }
        }
    }"#;

    /// All items the mock gateway serves, 1-based ids.
    fn mock_items() -> Vec<serde_json::Value> {
        (1..=5)
            .map(|id| {
                serde_json::json!({
                    "id": id,
                    "name": format!("item-{id}"),
                    "value": id as f64,
                    "tags": ["t1", "t2"],
                    "created_at": "2026-01-01T00:00:00Z"
                })
            })
            .collect()
    }

    /// Mock gateway: health, discovery (with the given read-only flag), and
    /// page-number `mock.list_items` execution (2 items per page).
    fn gateway_handler(
        req: &RecordedRequest,
        total: usize,
        read_only: Option<bool>,
        output_schema: &str,
    ) -> MockResponse {
        if req.method == "GET" && req.path == "/v1/health" {
            return MockResponse::ok("{}");
        }
        if req.method == "GET" && req.path == "/v1/actions/mock.list_items" {
            return MockResponse::ok(&discovery_response(read_only, output_schema));
        }
        if req.method == "POST" && req.path == "/v1/actions/mock.list_items/execute" {
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            let input = body.get("input").cloned().unwrap_or_default();
            let page = input
                .get("page")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(1) as usize;
            let min_value = input.get("min_value").and_then(serde_json::Value::as_f64);
            let slice: Vec<_> = mock_items()
                .into_iter()
                .take(total)
                .filter(|item| {
                    min_value.is_none_or(|min| {
                        item.get("value").and_then(serde_json::Value::as_f64) > Some(min)
                    })
                })
                .skip((page - 1) * 2)
                .take(2)
                .collect();
            return MockResponse::ok(
                &serde_json::json!({ "output": { "items": slice } }).to_string(),
            );
        }
        MockResponse::new(404, "{}")
    }

    const BOUND_CONFIG: &str = "
runtime_token_env: {env}
cache_ttl_seconds: {ttl}
bindings:
  - name: ws
    source_pack: mock
    resource: { workspace: demo }
    tables: [items]
";

    const ALLOWLIST_CONFIG: &str = "
runtime_token_env: {env}
raw_action_allowlist:
  - mock.list_items
";

    fn parse_config(
        template: &str,
        token_env: &str,
        cache_ttl_seconds: u64,
    ) -> OpenConnectorConfig {
        let yaml = template
            .replace("{env}", token_env)
            .replace("{ttl}", &cache_ttl_seconds.to_string());
        serde_yaml::from_str(&yaml).expect("parse config")
    }

    /// Register the gateway (tables + UDTF state) and both UDTFs.
    async fn setup(
        gateway: &MockGateway,
        config: &OpenConnectorConfig,
        token_env: &str,
    ) -> SessionContext {
        unsafe {
            std::env::set_var(token_env, "test-token");
        }
        let gateways = OpenConnectorGateways::default();
        let mut ctx = SessionContext::new();
        register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(config),
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

    async fn collect(ctx: &SessionContext, sql: &str) -> Vec<arrow::record_batch::RecordBatch> {
        ctx.sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect")
    }

    /// Planning must fail and the error must carry the given fragment.
    async fn expect_plan_error(ctx: &SessionContext, sql: &str, fragment: &str) {
        let err = match ctx.sql(sql).await {
            Err(e) => e.to_string(),
            Ok(df) => df.collect().await.expect_err("query must fail").to_string(),
        };
        assert!(err.contains(fragment), "expected '{fragment}' in: {err}");
    }

    fn execute_requests(gateway: &MockGateway) -> Vec<RecordedRequest> {
        gateway
            .requests()
            .into_iter()
            .filter(|r| r.method == "POST")
            .collect()
    }

    #[tokio::test]
    async fn query_udtf_matches_yaml_registered_table() {
        let gateway =
            MockGateway::start(|req| gateway_handler(req, 5, None, ITEMS_OUTPUT_SCHEMA)).await;
        let config = parse_config(BOUND_CONFIG, "SKARDI_TEST_OC_UDTF_QUERY_PARITY", 0);
        let ctx = setup(&gateway, &config, "SKARDI_TEST_OC_UDTF_QUERY_PARITY").await;

        let from_table = collect(&ctx, "SELECT * FROM saas.ws.items ORDER BY id").await;
        let from_udtf = collect(
            &ctx,
            r#"SELECT * FROM open_connector_query('saas', 'mock.items', '{"workspace":"demo"}')
               ORDER BY id"#,
        )
        .await;

        // Same stable Arrow schema and same values as the YAML-bound table.
        assert_eq!(from_table[0].schema(), from_udtf[0].schema());
        assert_eq!(
            pretty_format_batches(&from_table).unwrap().to_string(),
            pretty_format_batches(&from_udtf).unwrap().to_string()
        );
        let rows: usize = from_udtf.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 5);
    }

    #[tokio::test]
    async fn query_udtf_pushes_filters_and_sends_alias() {
        let gateway =
            MockGateway::start(|req| gateway_handler(req, 5, None, ITEMS_OUTPUT_SCHEMA)).await;
        let config = parse_config(BOUND_CONFIG, "SKARDI_TEST_OC_UDTF_QUERY_FILTER", 0);
        let ctx = setup(&gateway, &config, "SKARDI_TEST_OC_UDTF_QUERY_FILTER").await;

        let batches = collect(
            &ctx,
            r#"SELECT id, value
               FROM open_connector_query('saas', 'mock.items', '{"workspace":"demo"}', 'work')
               WHERE value > 3.0"#,
        )
        .await;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "values 4.0 and 5.0");

        // The UDTF goes through the same filter allowlist and connection
        // alias plumbing as the stable table.
        let executes = execute_requests(&gateway);
        assert!(!executes.is_empty());
        assert!(
            executes.iter().all(|r| r.body.contains(r#""min_value":3"#)),
            "Exact filter pushed on every page"
        );
        assert!(
            executes
                .iter()
                .all(|r| r.header("x-openconnector-connection-alias").as_deref() == Some("work")),
            "explicit connection alias sent on every execute"
        );
    }

    #[tokio::test]
    async fn query_udtf_shares_the_gateway_scan_cache() {
        let gateway =
            MockGateway::start(|req| gateway_handler(req, 3, None, ITEMS_OUTPUT_SCHEMA)).await;
        let config = parse_config(BOUND_CONFIG, "SKARDI_TEST_OC_UDTF_QUERY_CACHE", 60);
        let ctx = setup(&gateway, &config, "SKARDI_TEST_OC_UDTF_QUERY_CACHE").await;

        // Warm the cache through the YAML-bound table…
        let batches = collect(&ctx, "SELECT id, name FROM saas.ws.items ORDER BY id").await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        let live_pages = execute_requests(&gateway).len();
        assert_eq!(live_pages, 2, "3 items at per_page=2");

        // …and replay it through the UDTF: identical scan spec → same key.
        let batches = collect(
            &ctx,
            r#"SELECT id, name
               FROM open_connector_query('saas', 'mock.items', '{"workspace":"demo"}')
               ORDER BY id"#,
        )
        .await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        assert_eq!(
            execute_requests(&gateway).len(),
            live_pages,
            "the UDTF scan replays from the shared cache with zero new requests"
        );
    }

    #[tokio::test]
    async fn query_udtf_rejects_bad_arguments_and_unknown_names() {
        let gateway =
            MockGateway::start(|req| gateway_handler(req, 5, None, ITEMS_OUTPUT_SCHEMA)).await;
        let config = parse_config(BOUND_CONFIG, "SKARDI_TEST_OC_UDTF_QUERY_ERRORS", 0);
        let ctx = setup(&gateway, &config, "SKARDI_TEST_OC_UDTF_QUERY_ERRORS").await;
        let live = execute_requests(&gateway).len();

        expect_plan_error(
            &ctx,
            "SELECT * FROM open_connector_query('nope', 'mock.items', '{}')",
            "gateway 'nope' is not registered",
        )
        .await;
        expect_plan_error(
            &ctx,
            "SELECT * FROM open_connector_query('saas', 'github.issues', '{}')",
            "unknown source pack 'github'",
        )
        .await;
        expect_plan_error(
            &ctx,
            "SELECT * FROM open_connector_query('saas', 'mock.users', '{}')",
            "has no table 'users'",
        )
        .await;
        expect_plan_error(
            &ctx,
            "SELECT * FROM open_connector_query('saas', 'mock-items', '{}')",
            "must be '<pack>.<table>'",
        )
        .await;
        expect_plan_error(
            &ctx,
            "SELECT * FROM open_connector_query('saas', 'mock.items', '{}')",
            "missing required resource input 'workspace'",
        )
        .await;
        expect_plan_error(
            &ctx,
            "SELECT * FROM open_connector_query('saas', 'mock.items', 'not json')",
            "resource_json is not valid JSON",
        )
        .await;
        expect_plan_error(
            &ctx,
            "SELECT * FROM open_connector_query('saas', 'mock.items', '[1, 2]')",
            "resource_json must be a JSON object of resource inputs",
        )
        .await;
        expect_plan_error(
            &ctx,
            "SELECT * FROM open_connector_query('saas', 'mock.items')",
            "expects 3-4 arguments",
        )
        .await;
        expect_plan_error(
            &ctx,
            "SELECT * FROM open_connector_query(1, 'mock.items', '{}')",
            "'gateway' must be a string literal",
        )
        .await;
        // NULL is rejected outright for schema-determining arguments — a
        // placeholder cannot produce a plan (strict_string_arg semantics).
        expect_plan_error(
            &ctx,
            "SELECT * FROM open_connector_query(NULL, 'mock.items', '{}')",
            "'gateway' must be a string literal, not NULL",
        )
        .await;
        expect_plan_error(
            &ctx,
            "SELECT * FROM open_connector_query('saas', NULL, '{}')",
            "'table_id' must be a string literal, not NULL",
        )
        .await;
        expect_plan_error(
            &ctx,
            "SELECT * FROM open_connector_query('saas', 'mock.items', NULL)",
            "'resource_json' must be a string literal, not NULL",
        )
        .await;

        assert_eq!(
            execute_requests(&gateway).len(),
            live,
            "every rejection fires at planning time, before any HTTP execute"
        );
    }

    #[tokio::test]
    async fn query_udtf_requires_registration_time_discovery() {
        // No binding and no allowlist entry → mock.list_items was never
        // discovered, and planning (which performs no network I/O) must say
        // how to fix that rather than silently contacting the gateway.
        let gateway =
            MockGateway::start(|req| gateway_handler(req, 5, None, ITEMS_OUTPUT_SCHEMA)).await;
        let config = parse_config(
            "runtime_token_env: {env}\ncache_ttl_seconds: {ttl}\n",
            "SKARDI_TEST_OC_UDTF_QUERY_UNDISCOVERED",
            0,
        );
        let ctx = setup(&gateway, &config, "SKARDI_TEST_OC_UDTF_QUERY_UNDISCOVERED").await;

        expect_plan_error(
            &ctx,
            r#"SELECT * FROM open_connector_query('saas', 'mock.items', '{"workspace":"demo"}')"#,
            "was not discovered when gateway 'saas' was registered",
        )
        .await;
        assert!(
            execute_requests(&gateway).is_empty(),
            "no execute call may be attempted"
        );
    }

    #[tokio::test]
    async fn scan_udtf_executes_allowlisted_read_action_once() {
        let gateway =
            MockGateway::start(|req| gateway_handler(req, 5, Some(true), ITEMS_OUTPUT_SCHEMA))
                .await;
        let config = parse_config(ALLOWLIST_CONFIG, "SKARDI_TEST_OC_UDTF_SCAN_OK", 0);
        let ctx = setup(&gateway, &config, "SKARDI_TEST_OC_UDTF_SCAN_OK").await;

        let batches = collect(
            &ctx,
            r#"SELECT id, name, value
               FROM open_connector_scan('saas', 'mock.list_items',
                                        '{"workspace":"demo"}', '$.items')
               ORDER BY id"#,
        )
        .await;
        // A raw action has no pagination contract: exactly one execution,
        // returning the gateway's first page (2 of 5 items).
        let rendered = pretty_format_batches(&batches).unwrap().to_string();
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            2,
            "{rendered}"
        );
        assert!(
            rendered.contains("item-1") && rendered.contains("item-2"),
            "{rendered}"
        );
        assert_eq!(
            execute_requests(&gateway).len(),
            1,
            "single page, single POST"
        );

        // The derived schema is deterministic: sorted, conservatively typed.
        let schema = batches[0].schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "name", "value"]);
        assert_eq!(
            schema.field(0).data_type(),
            &arrow::datatypes::DataType::Int64
        );
    }

    #[tokio::test]
    async fn scan_udtf_exposes_complex_fields_as_json_and_honors_limit() {
        let gateway =
            MockGateway::start(|req| gateway_handler(req, 5, Some(true), ITEMS_OUTPUT_SCHEMA))
                .await;
        let config = parse_config(ALLOWLIST_CONFIG, "SKARDI_TEST_OC_UDTF_SCAN_JSON", 0);
        let ctx = setup(&gateway, &config, "SKARDI_TEST_OC_UDTF_SCAN_JSON").await;

        let batches = collect(
            &ctx,
            r#"SELECT tags
               FROM open_connector_scan('saas', 'mock.list_items',
                                        '{"workspace":"demo"}', '$.items')
               LIMIT 1"#,
        )
        .await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        let tags = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("tags is an opaque JSON string column");
        assert_eq!(tags.value(0), r#"["t1","t2"]"#);
    }

    #[tokio::test]
    async fn scan_udtf_treats_json_null_fields_as_sql_null() {
        // Raw scans type nested fields as opaque Json; a provider null
        // (assignee: null and friends) must behave as SQL NULL — `IS NULL`
        // matches it, `= 'null'` does not.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path == "/v1/actions/mock.list_items" {
                return MockResponse::ok(&discovery_response(Some(true), ITEMS_OUTPUT_SCHEMA));
            }
            if req.method == "POST" && req.path == "/v1/actions/mock.list_items/execute" {
                return MockResponse::ok(
                    &serde_json::json!({"output": {"items": [
                        {"id": 1, "name": "tagged", "tags": ["t1"]},
                        {"id": 2, "name": "untagged", "tags": null}
                    ]}})
                    .to_string(),
                );
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let config = parse_config(ALLOWLIST_CONFIG, "SKARDI_TEST_OC_UDTF_SCAN_JSON_NULL", 0);
        let ctx = setup(&gateway, &config, "SKARDI_TEST_OC_UDTF_SCAN_JSON_NULL").await;

        let batches = collect(
            &ctx,
            r#"SELECT id
               FROM open_connector_scan('saas', 'mock.list_items',
                                        '{"workspace":"demo"}', '$.items')
               WHERE tags IS NULL"#,
        )
        .await;
        let rendered = pretty_format_batches(&batches).unwrap().to_string();
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            1,
            "IS NULL must match the provider null: {rendered}"
        );
        assert!(
            rendered.contains('2'),
            "row id=2 is the null-tagged one: {rendered}"
        );

        let batches = collect(
            &ctx,
            r#"SELECT id
               FROM open_connector_scan('saas', 'mock.list_items',
                                        '{"workspace":"demo"}', '$.items')
               WHERE tags = 'null'"#,
        )
        .await;
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            0,
            "the string 'null' must not match a provider null"
        );
    }

    #[tokio::test]
    async fn scan_udtf_denies_unallowlisted_actions_before_http() {
        // mock.list_items IS discovered (the binding uses it) — but raw
        // access is a separate, default-deny grant.
        let gateway =
            MockGateway::start(|req| gateway_handler(req, 5, Some(true), ITEMS_OUTPUT_SCHEMA))
                .await;
        let config = parse_config(BOUND_CONFIG, "SKARDI_TEST_OC_UDTF_SCAN_DENY", 0);
        let ctx = setup(&gateway, &config, "SKARDI_TEST_OC_UDTF_SCAN_DENY").await;

        expect_plan_error(
            &ctx,
            r#"SELECT * FROM open_connector_scan('saas', 'mock.list_items',
                                                 '{"workspace":"demo"}', '$.items')"#,
            "is not in the 'raw_action_allowlist'",
        )
        .await;
        assert!(execute_requests(&gateway).is_empty(), "rejected pre-HTTP");
    }

    #[tokio::test]
    async fn scan_udtf_rejects_unclassified_and_mutating_actions_before_http() {
        // No read_only flag at all → classification gap, named as such.
        let gateway =
            MockGateway::start(|req| gateway_handler(req, 5, None, ITEMS_OUTPUT_SCHEMA)).await;
        let config = parse_config(ALLOWLIST_CONFIG, "SKARDI_TEST_OC_UDTF_SCAN_UNCLASSIFIED", 0);
        let ctx = setup(&gateway, &config, "SKARDI_TEST_OC_UDTF_SCAN_UNCLASSIFIED").await;
        expect_plan_error(
            &ctx,
            r#"SELECT * FROM open_connector_scan('saas', 'mock.list_items',
                                                 '{"workspace":"demo"}', '$.items')"#,
            "does not declare a read-only classification",
        )
        .await;
        assert!(execute_requests(&gateway).is_empty(), "rejected pre-HTTP");

        // Explicitly mutating → rejected with the other targeted error.
        let gateway =
            MockGateway::start(|req| gateway_handler(req, 5, Some(false), ITEMS_OUTPUT_SCHEMA))
                .await;
        let config = parse_config(ALLOWLIST_CONFIG, "SKARDI_TEST_OC_UDTF_SCAN_MUTATING", 0);
        let ctx = setup(&gateway, &config, "SKARDI_TEST_OC_UDTF_SCAN_MUTATING").await;
        expect_plan_error(
            &ctx,
            r#"SELECT * FROM open_connector_scan('saas', 'mock.list_items',
                                                 '{"workspace":"demo"}', '$.items')"#,
            "is classified as mutating",
        )
        .await;
        assert!(execute_requests(&gateway).is_empty(), "rejected pre-HTTP");
    }

    #[tokio::test]
    async fn scan_udtf_requires_a_deterministic_row_type_at_planning() {
        // The action is allowlisted and read-only, but its output schema
        // gives the row path no deterministic object rows.
        let gateway =
            MockGateway::start(|req| gateway_handler(req, 5, Some(true), r#"{"type": "object"}"#))
                .await;
        let config = parse_config(ALLOWLIST_CONFIG, "SKARDI_TEST_OC_UDTF_SCAN_NOSCHEMA", 0);
        let ctx = setup(&gateway, &config, "SKARDI_TEST_OC_UDTF_SCAN_NOSCHEMA").await;

        expect_plan_error(
            &ctx,
            r#"SELECT * FROM open_connector_scan('saas', 'mock.list_items',
                                                 '{"workspace":"demo"}', '$.items')"#,
            "cannot derive a deterministic row type",
        )
        .await;
        expect_plan_error(
            &ctx,
            r#"SELECT * FROM open_connector_scan('saas', 'mock.list_items',
                                                 '{"workspace":"demo"}', 'items')"#,
            "must start with '$.'",
        )
        .await;
        expect_plan_error(
            &ctx,
            r#"SELECT * FROM open_connector_scan('saas', 'mock.list_items',
                                                 '[1, 2]', '$.items')"#,
            "input_json must be a JSON object of action inputs",
        )
        .await;
        assert!(execute_requests(&gateway).is_empty(), "rejected pre-HTTP");
    }

    #[tokio::test]
    async fn udtf_joins_the_mock_pack_with_a_local_csv() {
        let gateway =
            MockGateway::start(|req| gateway_handler(req, 5, None, ITEMS_OUTPUT_SCHEMA)).await;
        let config = parse_config(BOUND_CONFIG, "SKARDI_TEST_OC_UDTF_JOIN", 0);
        let ctx = setup(&gateway, &config, "SKARDI_TEST_OC_UDTF_JOIN").await;

        // A local CSV source to federate with.
        let dir = tempfile::tempdir().expect("tempdir");
        let csv_path = dir.path().join("labels.csv");
        let mut file = std::fs::File::create(&csv_path).expect("create csv");
        writeln!(file, "id,label").unwrap();
        writeln!(file, "1,alpha").unwrap();
        writeln!(file, "3,gamma").unwrap();
        drop(file);
        ctx.register_csv("labels", csv_path.to_str().unwrap(), CsvReadOptions::new())
            .await
            .expect("register csv");

        let batches = collect(
            &ctx,
            r#"SELECT i.id, i.name, l.label
               FROM open_connector_query('saas', 'mock.items', '{"workspace":"demo"}') i
               JOIN labels l ON i.id = l.id
               ORDER BY i.id"#,
        )
        .await;
        let rendered = pretty_format_batches(&batches).unwrap().to_string();
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            2,
            "{rendered}"
        );
        assert!(
            rendered.contains("alpha") && rendered.contains("gamma"),
            "{rendered}"
        );
    }
}
