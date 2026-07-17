//! Open Connector integration — config foundation + gateway client.
//!
//! Open Connector is a separate authenticated SaaS gateway: it owns provider
//! credentials, OAuth flows, token refresh, action policies, and
//! provider-specific HTTP execution. Skardi adds the relational layer
//! (stable table definitions, JSON-to-Arrow conversion, pagination, filter
//! and limit pushdown, DataFusion registration) on top.
//!
//! **Status: typed config + HTTP client + action registry have landed.**
//!
//! - [`OpenConnectorConfig`] / [`OpenConnectorBinding`] — the typed
//!   `open_connector:` block of a `type: open_connector` data source, shared
//!   by the server and the CLI;
//! - [`OpenConnectorError`] — pre-network and gateway-contact errors;
//! - [`OpenConnectorClient`] — health checks, action discovery, action
//!   execution, bounded retries, bounded decoding;
//! - [`ActionRegistry`] — in-memory action metadata with compatibility
//!   fingerprints, so query planning never performs network I/O;
//! - [`register_open_connector_tables`] — the registration entry point both
//!   front-ends wire to. It validates the config, contacts the gateway, and
//!   loads the registry, then fails with
//!   [`OpenConnectorError::ExecutionNotImplemented`] until source packs and
//!   the scan engine land.
//!
//! See `docs/superpowers/specs/2026-07-11-open-connector-integration-design.md`.

pub mod action_registry;
pub mod client;
pub mod config;
mod error;

#[cfg(test)]
pub(crate) mod testutil;

pub use action_registry::{ActionMetadata, ActionRegistry};
pub use client::OpenConnectorClient;
pub use config::{OpenConnectorBinding, OpenConnectorConfig};
pub use error::OpenConnectorError;

use crate::sources::hierarchy::HierarchyLevel;
use anyhow::Result;
use datafusion::prelude::SessionContext;

/// Register an Open Connector gateway into a DataFusion [`SessionContext`].
///
/// One configured gateway is exposed as one catalog; each binding in
/// [`OpenConnectorConfig::bindings`] becomes a schema beneath it, and
/// built-in source-pack tables become tables under those schemas:
/// `<gateway>.<binding>.<table>`.
///
/// # Current behavior (config + client milestone)
///
/// No tables are registered yet. The function:
///
/// 1. requires [`HierarchyLevel::Catalog`] (a gateway is a catalog, never a
///    single table);
/// 2. requires a non-empty `connection_string` (the gateway URL);
/// 3. runs [`OpenConnectorConfig::validate`];
/// 4. builds an [`OpenConnectorClient`] (runtime token from the environment)
///    and health-checks the gateway;
/// 5. discovers every `raw_action_allowlist` action into an
///    [`ActionRegistry`];
/// 6. fails with [`OpenConnectorError::ExecutionNotImplemented`] — catalog
///    registration arrives with the source-pack milestone.
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
///   - name: github_skardi
///     source_pack: github
///     resource: { owner: SkardiLabs, repo: skardi }
///     tables: [issues]
/// "#,
/// )?;
///
/// // Today this contacts the gateway, then fails with
/// // OpenConnectorError::ExecutionNotImplemented.
/// let result = register_open_connector_tables(
///     &mut ctx,
///     "saas",
///     "http://open-connector:3000",
///     &config,
///     HierarchyLevel::Catalog,
/// )
/// .await;
/// assert!(result.is_err());
/// # Ok(())
/// # }
/// ```
pub async fn register_open_connector_tables(
    session_ctx: &mut SessionContext,
    name: &str,
    connection_string: &str,
    config: &OpenConnectorConfig,
    hierarchy_level: HierarchyLevel,
) -> Result<()> {
    // Nothing to register yet — keep the parameter so the final signature
    // (used by the server and the CLI) does not change when execution lands.
    let _ = session_ctx;

    if hierarchy_level != HierarchyLevel::Catalog {
        return Err(OpenConnectorError::CatalogHierarchyRequired {
            name: name.to_string(),
        }
        .into());
    }
    if connection_string.trim().is_empty() {
        return Err(OpenConnectorError::EmptyGatewayUrl {
            name: name.to_string(),
        }
        .into());
    }
    config.validate()?;

    let client = OpenConnectorClient::from_config(connection_string, config)?;
    client.health().await?;
    let registry = ActionRegistry::load(&client, &config.raw_action_allowlist).await?;

    tracing::info!(
        gateway = %name,
        actions = registry.len(),
        "Open Connector gateway reachable; action metadata loaded"
    );

    Err(OpenConnectorError::ExecutionNotImplemented {
        name: name.to_string(),
    }
    .into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::providers::open_connector::testutil::{MockGateway, MockResponse};

    const TOKEN_ENV: &str = "SKARDI_TEST_OC_REGISTER_TOKEN";

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
            &valid_config("UNUSED_ENV"),
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
            &valid_config("UNUSED_ENV"),
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
            &invalid,
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
            &valid_config("SKARDI_TEST_OC_TOKEN_DEFINITELY_UNSET"),
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

        unsafe {
            std::env::set_var(TOKEN_ENV, "test-token");
        }
        let mut ctx = SessionContext::new();
        let result = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            &valid_config(TOKEN_ENV),
            HierarchyLevel::Catalog,
        )
        .await;
        unsafe {
            std::env::remove_var(TOKEN_ENV);
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
    async fn register_reaches_not_implemented_with_healthy_gateway() {
        let gateway = MockGateway::start(|req| {
            if req.path == "/v1/health" {
                MockResponse::ok("{}")
            } else if req.path == "/v1/actions/github.list_repository_issues" {
                MockResponse::ok(
                    r#"{"input_schema": {}, "output_schema": {"type": "object"},
                       "locally_executable": true, "connection_aliases": ["work"]}"#,
                )
            } else {
                MockResponse::new(404, "{}")
            }
        })
        .await;

        unsafe {
            std::env::set_var(TOKEN_ENV, "test-token");
        }
        let mut ctx = SessionContext::new();
        let result = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            &valid_config(TOKEN_ENV),
            HierarchyLevel::Catalog,
        )
        .await;
        unsafe {
            std::env::remove_var(TOKEN_ENV);
        }

        let err = result
            .unwrap_err()
            .downcast::<OpenConnectorError>()
            .unwrap();
        assert!(
            matches!(
                err,
                OpenConnectorError::ExecutionNotImplemented { ref name } if name == "saas"
            ),
            "got {err}"
        );

        let requests = gateway.requests();
        let paths: Vec<&str> = requests.iter().map(|r| r.path.as_str()).collect();
        assert!(
            paths.contains(&"/v1/health"),
            "health was called: {paths:?}"
        );
        assert!(
            paths.contains(&"/v1/actions/github.list_repository_issues"),
            "allowlist action was discovered: {paths:?}"
        );
    }
}
