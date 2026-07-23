//! Open Connector integration — config, gateway client, and scan engine.
//!
//! Open Connector is a separate authenticated SaaS gateway: it owns provider
//! credentials, OAuth flows, token refresh, action policies, and
//! provider-specific HTTP execution. Skardi adds the relational layer
//! (stable table definitions, JSON-to-Arrow conversion, pagination, filter
//! and limit pushdown, DataFusion registration) on top.
//!
//! **Status: typed config, HTTP client, action registry, source packs, and
//! the scan engine have landed.** A configured gateway registers as a real
//! catalog (`<gateway>.<binding>.<table>`) and is queryable today — with the
//! synthetic `mock` source pack. Real provider packs (GitHub, Slack, Notion)
//! and the raw-action UDTF land next.
//!
//! - [`OpenConnectorConfig`] / [`OpenConnectorBinding`] — the typed
//!   `open_connector:` block of a `type: open_connector` data source, shared
//!   by the server and the CLI;
//! - [`OpenConnectorError`] — pre-network and gateway-contact errors;
//! - [`OpenConnectorClient`] — health checks, action discovery, action
//!   execution, idempotency-aware bounded retries, bounded decoding;
//! - [`ActionRegistry`] — in-memory action metadata with compatibility
//!   fingerprints, so query planning never performs network I/O;
//! - [`SourcePackRegistry`] — built-in stable table definitions;
//! - [`register_open_connector_tables`] — the registration entry point both
//!   front-ends wire to.
//!
//! See `docs/superpowers/specs/2026-07-11-open-connector-integration-design.md`.

pub mod action_registry;
pub mod cache;
pub mod client;
pub mod config;
mod error;
pub mod exec;
pub mod filters;
pub mod json_to_arrow;
pub mod packs;
pub mod pagination;
pub mod row_path;
pub mod source_pack;
pub mod table;

#[cfg(test)]
pub(crate) mod testutil;

pub use action_registry::{ActionMetadata, ActionRegistry};
pub use client::OpenConnectorClient;
pub use config::{OpenConnectorBinding, OpenConnectorConfig};
pub use error::OpenConnectorError;
pub use source_pack::{SourcePack, SourcePackRegistry, SourcePackTable};
pub use table::OpenConnectorTableProvider;

use std::sync::Arc;
use std::time::Duration;

use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
};
use datafusion::prelude::SessionContext;
use serde_json::Value;

use crate::sources::hierarchy::HierarchyLevel;
use anyhow::Result;

/// Register an Open Connector gateway into a DataFusion [`SessionContext`].
///
/// One configured gateway is exposed as one catalog; each binding in
/// [`OpenConnectorConfig::bindings`] becomes a schema beneath it, and
/// built-in source-pack tables become tables under those schemas:
/// `<gateway>.<binding>.<table>`.
///
/// The function:
///
/// 1. requires [`HierarchyLevel::Catalog`], read-only access, a present
///    typed config, and a non-empty gateway URL (the single enforcement
///    point both front-ends share);
/// 2. runs [`OpenConnectorConfig::validate`];
/// 3. health-checks the gateway with an [`OpenConnectorClient`] built from
///    the environment-held runtime token;
/// 4. discovers every action the bindings and the raw allowlist reference
///    into an [`ActionRegistry`], enforcing version pins, required
///    resources, and action-contract fingerprints;
/// 5. builds one [`OpenConnectorTableProvider`] per bound table and
///    registers the catalog.
///
/// # Example
/// ```no_run
/// use datafusion::prelude::SessionContext;
/// use skardi::sources::hierarchy::HierarchyLevel;
/// use skardi::sources::providers::open_connector::{
///     register_open_connector_tables, OpenConnectorConfig,
/// };
///
/// # async fn example() -> anyhow::Result<()> {
/// let mut ctx = SessionContext::new();
/// let config: OpenConnectorConfig = serde_yaml::from_str(
///     r#"
/// runtime_token_env: OPEN_CONNECTOR_TOKEN
/// bindings:
///   - name: ws
///     source_pack: mock
///     resource: { workspace: demo }
///     tables: [items]
/// "#,
/// )?;
///
/// register_open_connector_tables(
///     &mut ctx,
///     "saas",
///     "http://open-connector:3000",
///     Some(&config),
///     false,
///     HierarchyLevel::Catalog,
/// )
/// .await?;
///
/// // The mock table is now queryable as saas.ws.items.
/// # Ok(())
/// # }
/// ```
pub async fn register_open_connector_tables(
    session_ctx: &mut SessionContext,
    name: &str,
    connection_string: &str,
    config: Option<&OpenConnectorConfig>,
    read_write: bool,
    hierarchy_level: HierarchyLevel,
) -> Result<()> {
    // All invariant checks live here so both front-ends (server and CLI)
    // get identical behavior; front-ends may add earlier typed errors, but
    // this is the single enforcement point.
    if hierarchy_level != HierarchyLevel::Catalog {
        return Err(OpenConnectorError::CatalogHierarchyRequired {
            name: name.to_string(),
        }
        .into());
    }
    if read_write {
        return Err(OpenConnectorError::ReadWriteNotSupported {
            name: name.to_string(),
        }
        .into());
    }
    let config = config.ok_or_else(|| OpenConnectorError::MissingConfig {
        name: name.to_string(),
    })?;
    if connection_string.trim().is_empty() {
        return Err(OpenConnectorError::EmptyGatewayUrl {
            name: name.to_string(),
        }
        .into());
    }
    config.validate()?;

    let client = Arc::new(OpenConnectorClient::from_config(connection_string, config)?);
    client.health().await?;

    // Resolve bindings to pack table definitions first, so discovery covers
    // the allowlist *and* every action a bound table needs.
    let pack_registry = SourcePackRegistry::builtins();
    let mut action_ids = config.raw_action_allowlist.clone();
    for binding in &config.bindings {
        let pack = pack_registry.require(&binding.source_pack)?;
        SourcePackRegistry::check_version_pin(pack, binding.source_pack_version)?;
        for table_name in &binding.tables {
            let table = pack_registry.table(pack, table_name)?;
            action_ids.push(table.action_id.to_string());
            for key in table.required_resources {
                if !binding.resource.contains_key(*key) {
                    return Err(OpenConnectorError::MissingResourceInput {
                        binding: binding.name.clone(),
                        key: (*key).to_string(),
                    }
                    .into());
                }
            }
        }
    }
    let registry = ActionRegistry::load(&client, &action_ids).await?;

    let catalog = Arc::new(MemoryCatalogProvider::new());
    let cache = Arc::new(cache::ScanCache::new(
        Duration::from_secs(config.cache_ttl_seconds),
        usize::try_from(config.cache_max_bytes).unwrap_or(usize::MAX),
    ));
    let scan_timeout = Duration::from_secs(config.scan_timeout_seconds);

    for binding in &config.bindings {
        let pack = pack_registry.require(&binding.source_pack)?;
        let schema_provider = Arc::new(MemorySchemaProvider::new());

        for table_name in &binding.tables {
            let table = pack_registry.table(pack, table_name)?;

            // Compatibility gate: the discovered action contract must match
            // the fingerprint the pack was built against.
            if let Some(expected) = table.expected_fingerprint {
                let actual = registry
                    .get(table.action_id)
                    .map(ActionMetadata::fingerprint);
                if actual != Some(expected) {
                    return Err(OpenConnectorError::ActionContractMismatch {
                        table: table.id.to_string(),
                        reason: format!(
                            "action '{}' fingerprint mismatch (expected {expected}, discovered {})",
                            table.action_id,
                            actual.unwrap_or("<none>")
                        ),
                    }
                    .into());
                }
            }

            let provider = OpenConnectorTableProvider::new(
                Arc::clone(&client),
                Some(Arc::clone(&cache)),
                name.to_string(),
                binding.connection_alias.clone(),
                table,
                pack.version,
                Value::Object(
                    binding
                        .resource
                        .clone()
                        .into_iter()
                        .map(|(k, v)| (k, Value::from(v)))
                        .collect(),
                ),
                config.max_pages,
                config.max_rows,
                scan_timeout,
            )?;
            schema_provider
                .register_table(table_name.clone(), Arc::new(provider))
                .map_err(|e| OpenConnectorError::CatalogRegistrationFailed {
                    name: format!("{name}.{}.{table_name}", binding.name),
                    reason: format!("failed to register table into catalog schema: {e}"),
                })?;
        }

        catalog
            .register_schema(&binding.name, schema_provider)
            .map_err(|e| OpenConnectorError::CatalogRegistrationFailed {
                name: format!("{name}.{}", binding.name),
                reason: format!("failed to register schema in catalog: {e}"),
            })?;
    }

    session_ctx.register_catalog(name, catalog);

    tracing::info!(
        gateway = %name,
        bindings = config.bindings.len(),
        actions = registry.len(),
        "Open Connector catalog registered"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::providers::open_connector::testutil::{
        MockGateway, MockResponse, RecordedRequest,
    };

    const TOKEN_ENV_HEALTH_FAIL: &str = "SKARDI_TEST_OC_REGISTER_TOKEN_HEALTH_FAIL";

    fn valid_config(token_env: &str) -> OpenConnectorConfig {
        serde_yaml::from_str(&format!(
            r#"
runtime_token_env: {token_env}
raw_action_allowlist:
  - github.list_repository_issues
bindings:
  - name: github_skardi
    source_pack: github
    resource: {{ owner: SkardiLabs, repo: skardi }}
    tables: [issues]
"#
        ))
        .expect("parse config")
    }

    #[tokio::test]
    async fn register_rejects_table_hierarchy_before_any_network() {
        // Deliberately unroutable endpoint: the hierarchy check must fire
        // before any connection attempt.
        let mut ctx = SessionContext::new();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            "http://127.0.0.1:1",
            Some(&valid_config("UNUSED_ENV")),
            false,
            HierarchyLevel::Table,
        )
        .await
        .unwrap_err();
        let err = err.downcast::<OpenConnectorError>().unwrap();
        assert!(matches!(
            err,
            OpenConnectorError::CatalogHierarchyRequired { ref name } if name == "saas"
        ));
    }

    #[tokio::test]
    async fn register_rejects_empty_gateway_url_before_any_network() {
        let mut ctx = SessionContext::new();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            "   ",
            Some(&valid_config("UNUSED_ENV")),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap_err();
        let err = err.downcast::<OpenConnectorError>().unwrap();
        assert!(matches!(
            err,
            OpenConnectorError::EmptyGatewayUrl { ref name } if name == "saas"
        ));
    }

    #[tokio::test]
    async fn register_rejects_read_write_before_any_network() {
        // The read-only invariant is enforced here — the single point both
        // front-ends funnel through — not left to each front-end.
        let mut ctx = SessionContext::new();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            "http://127.0.0.1:1",
            Some(&valid_config("UNUSED_ENV")),
            true,
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap_err();
        let err = err.downcast::<OpenConnectorError>().unwrap();
        assert!(matches!(
            err,
            OpenConnectorError::ReadWriteNotSupported { ref name } if name == "saas"
        ));
    }

    #[tokio::test]
    async fn register_rejects_missing_config_block_before_any_network() {
        let mut ctx = SessionContext::new();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            "http://127.0.0.1:1",
            None,
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap_err();
        let err = err.downcast::<OpenConnectorError>().unwrap();
        assert!(matches!(
            err,
            OpenConnectorError::MissingConfig { ref name } if name == "saas"
        ));
    }

    #[tokio::test]
    async fn register_rejects_invalid_config_before_any_network() {
        let mut ctx = SessionContext::new();
        let invalid: OpenConnectorConfig = serde_yaml::from_str(
            "runtime_token_env: ''\nbindings:\n  - name: b\n    source_pack: github\n    tables: [issues]",
        )
        .expect("parse config");
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            "http://127.0.0.1:1",
            Some(&invalid),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap_err();
        let err = err.downcast::<OpenConnectorError>().unwrap();
        assert!(matches!(err, OpenConnectorError::EmptyRuntimeTokenEnv));
    }

    #[tokio::test]
    async fn register_fails_missing_token_env_before_network() {
        let mut ctx = SessionContext::new();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            "http://127.0.0.1:1",
            Some(&valid_config("SKARDI_TEST_OC_TOKEN_DEFINITELY_UNSET")),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap_err();
        let err = err.downcast::<OpenConnectorError>().unwrap();
        assert!(matches!(
            err,
            OpenConnectorError::MissingRuntimeToken { ref env }
                if env == "SKARDI_TEST_OC_TOKEN_DEFINITELY_UNSET"
        ));
    }

    #[tokio::test]
    async fn register_fails_health_check_before_discovery() {
        let gateway = MockGateway::start(|_| MockResponse::new(503, "{}")).await;

        // Per-test env-var name: #[tokio::test]s run on parallel threads, and
        // a shared name lets one test's remove_var land in the other's await
        // window (intermittent MissingRuntimeToken).
        unsafe {
            std::env::set_var(TOKEN_ENV_HEALTH_FAIL, "test-token");
        }
        let mut ctx = SessionContext::new();
        let result = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&valid_config(TOKEN_ENV_HEALTH_FAIL)),
            false,
            HierarchyLevel::Catalog,
        )
        .await;
        unsafe {
            std::env::remove_var(TOKEN_ENV_HEALTH_FAIL);
        }

        let err = result
            .unwrap_err()
            .downcast::<OpenConnectorError>()
            .unwrap();
        assert!(
            matches!(err, OpenConnectorError::RetriesExhausted { .. }),
            "got {err}"
        );

        let requests = gateway.requests();
        assert!(
            !requests.is_empty() && requests.iter().all(|r| r.path == "/v1/health"),
            "only health calls were attempted: {:?}",
            requests.iter().map(|r| &r.path).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn register_builds_queryable_catalog_with_mock_pack() {
        let gateway = MockGateway::start(|req| mock_gateway_handler(req, 5)).await;

        unsafe {
            std::env::set_var(TOKEN_ENV_CATALOG_BASIC, "test-token");
        }
        let mut ctx = SessionContext::new();
        register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&mock_config(TOKEN_ENV_CATALOG_BASIC, 0)),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("catalog registration succeeds");
        unsafe {
            std::env::remove_var(TOKEN_ENV_CATALOG_BASIC);
        }

        // The bound table is queryable through <gateway>.<binding>.<table>.
        let df = ctx
            .sql("SELECT id, name FROM saas.ws.items ORDER BY id")
            .await
            .expect("plan");
        let batches = df.collect().await.expect("collect");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 5);

        // Page-number pagination walked 3 pages (per_page = 2, 5 items).
        let executes = execute_requests(&gateway);
        assert_eq!(executes.len(), 3, "3 pages for 5 items at per_page=2");
        assert!(executes[0].body.contains(r#""page":1"#));
        assert!(executes[2].body.contains(r#""page":3"#));
    }

    #[tokio::test]
    async fn scan_pushes_allowlisted_filter_and_stops_at_limit() {
        let gateway = MockGateway::start(|req| mock_gateway_handler(req, 5)).await;

        unsafe {
            std::env::set_var(TOKEN_ENV_CATALOG_FILTER, "test-token");
        }
        let mut ctx = SessionContext::new();
        register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&mock_config(TOKEN_ENV_CATALOG_FILTER, 0)),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("catalog registration succeeds");
        unsafe {
            std::env::remove_var(TOKEN_ENV_CATALOG_FILTER);
        }

        // Exact-mapped filter is pushed into the action input…
        // (projection [0, 2] also covers cache-key name resolution against
        // the fixed schema — a non-contiguous projection used to panic)
        let df = ctx
            .sql("SELECT id, value FROM saas.ws.items WHERE value > 3.0")
            .await
            .expect("plan");
        let batches = df.collect().await.expect("collect");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "values 4.0 and 5.0");
        assert!(
            execute_requests(&gateway)
                .iter()
                .all(|r| r.body.contains(r#""min_value":3"#)),
            "min_value pushed on every page"
        );

        // …and LIMIT stops pagination after the first page.
        let gateway2 = MockGateway::start(|req| mock_gateway_handler(req, 5)).await;
        unsafe {
            std::env::set_var(TOKEN_ENV_CATALOG_FILTER, "test-token");
        }
        let mut ctx2 = SessionContext::new();
        register_open_connector_tables(
            &mut ctx2,
            "saas",
            &gateway2.url,
            Some(&mock_config(TOKEN_ENV_CATALOG_FILTER, 0)),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("catalog registration succeeds");
        unsafe {
            std::env::remove_var(TOKEN_ENV_CATALOG_FILTER);
        }
        let df = ctx2
            .sql("SELECT id FROM saas.ws.items LIMIT 1")
            .await
            .expect("plan");
        let batches = df.collect().await.expect("collect");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1);
        assert_eq!(
            execute_requests(&gateway2).len(),
            1,
            "LIMIT 1 must stop after the first page"
        );
    }

    #[tokio::test]
    async fn scan_deadline_bounds_retry_waits() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path == "/v1/actions/mock.list_items" {
                return MockResponse::ok(
                    r#"{"input_schema": {}, "output_schema": {"type": "object"},
                       "locally_executable": true, "connection_aliases": []}"#,
                );
            }
            if req.method == "POST" && req.path == "/v1/actions/mock.list_items/execute" {
                // The client would wait two seconds before retrying this 429,
                // but the one-second scan deadline must cut that wait short.
                return MockResponse::new(429, "{}").with_header("retry-after", "2");
            }
            MockResponse::new(404, "{}")
        })
        .await;

        unsafe {
            std::env::set_var(TOKEN_ENV_CATALOG_TIMEOUT, "test-token");
        }
        let mut config = mock_config(TOKEN_ENV_CATALOG_TIMEOUT, 0);
        config.scan_timeout_seconds = 1;
        config.request_timeout_seconds = 30;
        let mut ctx = SessionContext::new();
        register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&config),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("catalog registration succeeds");
        unsafe {
            std::env::remove_var(TOKEN_ENV_CATALOG_TIMEOUT);
        }

        let df = ctx.sql("SELECT id FROM saas.ws.items").await.expect("plan");
        let err = df.collect().await.expect_err("scan must time out");
        assert!(err.to_string().contains("timed out after 1s"), "got {err}");
        assert_eq!(
            execute_requests(&gateway).len(),
            1,
            "retry wait was cancelled"
        );
    }

    #[tokio::test]
    async fn gteq_is_not_pushed_to_strict_gt_input() {
        // The gateway's `min_value` is strictly greater-than, so `>=` has no
        // faithful pushdown and must stay in DataFusion. Pushing it as Exact
        // would silently drop the boundary row (value == 3.0).
        let gateway = MockGateway::start(|req| mock_gateway_handler(req, 5)).await;

        unsafe {
            std::env::set_var(TOKEN_ENV_CATALOG_FILTER, "test-token");
        }
        let mut ctx = SessionContext::new();
        register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&mock_config(TOKEN_ENV_CATALOG_FILTER, 0)),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("catalog registration succeeds");
        unsafe {
            std::env::remove_var(TOKEN_ENV_CATALOG_FILTER);
        }

        let before = execute_requests(&gateway).len();
        let df = ctx
            .sql("SELECT id FROM saas.ws.items WHERE value >= 3.0 ORDER BY id")
            .await
            .expect("plan");
        let batches = df.collect().await.expect("collect");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 3, "boundary row id=3 must be present (ids 3,4,5)");

        let new_requests = &execute_requests(&gateway)[before..];
        assert!(
            new_requests.iter().all(|r| !r.body.contains("min_value")),
            "no min_value may be pushed for >=: {:?}",
            new_requests.iter().map(|r| &r.body).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn cached_scan_replays_without_new_requests() {
        let gateway = MockGateway::start(|req| mock_gateway_handler(req, 3)).await;

        unsafe {
            std::env::set_var(TOKEN_ENV_CATALOG_CACHE, "test-token");
        }
        let mut ctx = SessionContext::new();
        register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&mock_config(TOKEN_ENV_CATALOG_CACHE, 60)),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("catalog registration succeeds");
        unsafe {
            std::env::remove_var(TOKEN_ENV_CATALOG_CACHE);
        }

        for round in 1..=2 {
            let df = ctx
                .sql("SELECT id, name FROM saas.ws.items ORDER BY id")
                .await
                .expect("plan");
            let batches = df.collect().await.expect("collect");
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, 3, "round {round}");
        }

        let executes = execute_requests(&gateway);
        assert_eq!(
            executes.len(),
            2,
            "second identical scan must be served from cache (3 items at per_page=2 → 2 live pages)"
        );
    }

    #[tokio::test]
    async fn limited_scan_is_cached_and_replayed() {
        // A LIMIT-satisfied scan is complete *for its key* (LIMIT is part of
        // the key), so it must be stored eagerly — repeated identical LIMIT
        // queries replay with zero new gateway requests.
        let gateway = MockGateway::start(|req| mock_gateway_handler(req, 5)).await;

        unsafe {
            std::env::set_var(TOKEN_ENV_CATALOG_CACHE, "test-token");
        }
        let mut ctx = SessionContext::new();
        register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&mock_config(TOKEN_ENV_CATALOG_CACHE, 60)),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("catalog registration succeeds");
        unsafe {
            std::env::remove_var(TOKEN_ENV_CATALOG_CACHE);
        }

        for round in 1..=2 {
            let df = ctx
                // No ORDER BY: a sort would force a full scan for TopK and
                // defeat LIMIT pushdown, which is what we're testing.
                .sql("SELECT id FROM saas.ws.items LIMIT 2")
                .await
                .expect("plan");
            let batches = df.collect().await.expect("collect");
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, 2, "round {round}");
        }

        assert_eq!(
            execute_requests(&gateway).len(),
            1,
            "first LIMIT scan fetches one page; the replay adds none"
        );
    }

    #[tokio::test]
    async fn cached_empty_scan_replays_without_new_requests() {
        let gateway = MockGateway::start(|req| mock_gateway_handler(req, 0)).await;

        unsafe {
            std::env::set_var(TOKEN_ENV_CATALOG_EMPTY_CACHE, "test-token");
        }
        let mut ctx = SessionContext::new();
        register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&mock_config(TOKEN_ENV_CATALOG_EMPTY_CACHE, 60)),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("catalog registration succeeds");
        unsafe {
            std::env::remove_var(TOKEN_ENV_CATALOG_EMPTY_CACHE);
        }

        for round in 1..=2 {
            let df = ctx.sql("SELECT id FROM saas.ws.items").await.expect("plan");
            let batches = df.collect().await.expect("collect");
            assert!(batches.is_empty(), "round {round} should be empty");
        }

        assert_eq!(
            execute_requests(&gateway).len(),
            1,
            "second empty scan must be served from cache"
        );
    }

    #[tokio::test]
    async fn self_join_scans_compute_identical_keys_but_fetch_live() {
        // Documents the whole-scan cache boundary: both sides of a self-join
        // compute the SAME canonical key, but because they run concurrently,
        // each starts before the other completes — so both fetch live. The
        // cache dedups repeated queries over time, not overlapping scans
        // (see cache.rs module docs).
        let gateway = MockGateway::start(|req| mock_gateway_handler(req, 3)).await;

        unsafe {
            std::env::set_var(TOKEN_ENV_CATALOG_SELFJOIN, "test-token");
        }
        let mut ctx = SessionContext::new();
        register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&mock_config(TOKEN_ENV_CATALOG_SELFJOIN, 60)),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("catalog registration succeeds");
        unsafe {
            std::env::remove_var(TOKEN_ENV_CATALOG_SELFJOIN);
        }

        let df = ctx
            .sql(
                "SELECT count(*) AS n FROM saas.ws.items i1 JOIN saas.ws.items i2 ON i1.id = i2.id",
            )
            .await
            .expect("plan");
        let batches = df.collect().await.expect("collect");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "count(*) returns one row");

        // Both sides fetched live (2 pages each for 3 items at per_page=2),
        // and a subsequent identical query replays from cache instead.
        let before = execute_requests(&gateway).len();
        assert_eq!(before, 4, "concurrent join sides both fetch live");

        let df = ctx.sql("SELECT id FROM saas.ws.items").await.expect("plan");
        let batches = df.collect().await.expect("collect");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 3);
        assert_eq!(
            execute_requests(&gateway).len(),
            before,
            "no new live pages once earlier scans have completed and cached"
        );
    }
    /// Env var for the catalog tests (unique per test file section).
    #[cfg(test)]
    const TOKEN_ENV_CATALOG_BASIC: &str = "SKARDI_TEST_OC_REGISTER_CATALOG_BASIC";

    #[cfg(test)]
    const TOKEN_ENV_CATALOG_FILTER: &str = "SKARDI_TEST_OC_REGISTER_CATALOG_FILTER";

    #[cfg(test)]
    const TOKEN_ENV_CATALOG_CACHE: &str = "SKARDI_TEST_OC_REGISTER_CATALOG_CACHE";

    #[cfg(test)]
    const TOKEN_ENV_CATALOG_SELFJOIN: &str = "SKARDI_TEST_OC_REGISTER_CATALOG_SELFJOIN";

    #[cfg(test)]
    const TOKEN_ENV_CATALOG_EMPTY_CACHE: &str = "SKARDI_TEST_OC_REGISTER_CATALOG_EMPTY_CACHE";

    #[cfg(test)]
    const TOKEN_ENV_CATALOG_TIMEOUT: &str = "SKARDI_TEST_OC_REGISTER_CATALOG_TIMEOUT";

    #[cfg(test)]
    fn mock_config(token_env: &str, cache_ttl_seconds: u64) -> OpenConnectorConfig {
        serde_yaml::from_str(&format!(
            r#"
runtime_token_env: {token_env}
cache_ttl_seconds: {cache_ttl_seconds}
bindings:
  - name: ws
    source_pack: mock
    resource: {{ workspace: demo }}
    tables: [items]
"#
        ))
        .expect("parse config")
    }

    /// All items the mock gateway serves, 1-based ids.
    #[cfg(test)]
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

    /// Mock gateway handler: health, discovery, and page-number paginated
    /// `mock.list_items` execution (per_page = 2 per the mock pack).
    #[cfg(test)]
    fn mock_gateway_handler(req: &RecordedRequest, total: usize) -> MockResponse {
        if req.method == "GET" && req.path == "/v1/health" {
            return MockResponse::ok("{}");
        }
        if req.method == "GET" && req.path == "/v1/actions/mock.list_items" {
            return MockResponse::ok(
                r#"{"input_schema": {}, "output_schema": {"type": "object"},
               "locally_executable": true, "connection_aliases": []}"#,
            );
        }
        if req.method == "POST" && req.path == "/v1/actions/mock.list_items/execute" {
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            let input = body.get("input").cloned().unwrap_or_default();
            let page = input
                .get("page")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(1) as usize;
            let min_value = input.get("min_value").and_then(serde_json::Value::as_f64);
            let items = mock_items();
            let start = (page - 1) * 2;
            let slice: Vec<_> = items
                .into_iter()
                .take(total)
                .filter(|item| {
                    min_value.is_none_or(|min| {
                        item.get("value").and_then(serde_json::Value::as_f64) > Some(min)
                    })
                })
                .skip(start)
                .take(2)
                .collect();
            return MockResponse::ok(
                &serde_json::json!({ "output": { "items": slice } }).to_string(),
            );
        }
        MockResponse::new(404, "{}")
    }

    #[cfg(test)]
    fn execute_requests(gateway: &MockGateway) -> Vec<RecordedRequest> {
        gateway
            .requests()
            .into_iter()
            .filter(|r| r.method == "POST")
            .collect()
    }
}
