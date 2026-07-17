//! Open Connector integration — config foundation.
//!
//! Open Connector is a separate authenticated SaaS gateway: it owns provider
//! credentials, OAuth flows, token refresh, action policies, and
//! provider-specific HTTP execution. Skardi adds the relational layer
//! (stable table definitions, JSON-to-Arrow conversion, pagination, filter
//! and limit pushdown, DataFusion registration) on top.
//!
//! **Status: typed-config foundation only.** This module currently provides:
//!
//! - [`OpenConnectorConfig`] / [`OpenConnectorBinding`] — the typed
//!   `open_connector:` block of a `type: open_connector` data source, shared
//!   by the server and the CLI;
//! - [`OpenConnectorError`] — pre-network validation errors;
//! - [`register_open_connector_tables`] — the registration entry point both
//!   front-ends wire to. It validates hierarchy level and config, then fails
//!   with [`OpenConnectorError::ExecutionNotImplemented`].
//!
//! The HTTP client, action registry, source-pack registry, scan engine,
//! cache, table factory, and UDTFs arrive in later milestones. See
//! `docs/superpowers/specs/2026-07-11-open-connector-integration-design.md`.

pub mod config;
mod error;

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
/// # Current behavior (foundation milestone)
///
/// No network I/O is performed and nothing is registered. The function:
///
/// 1. requires [`HierarchyLevel::Catalog`] (a gateway is a catalog, never a
///    single table);
/// 2. requires a non-empty `connection_string` (the gateway URL);
/// 3. runs [`OpenConnectorConfig::validate`];
/// 4. fails with [`OpenConnectorError::ExecutionNotImplemented`].
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
/// // Today this fails with OpenConnectorError::ExecutionNotImplemented —
/// // the gateway registration lands with the HTTP client milestone.
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

    Err(OpenConnectorError::ExecutionNotImplemented {
        name: name.to_string(),
    }
    .into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> OpenConnectorConfig {
        serde_yaml::from_str(
            r#"
runtime_token_env: OPEN_CONNECTOR_TOKEN
bindings:
  - name: github_skardi
    source_pack: github
    resource: { owner: SkardiLabs, repo: skardi }
    tables: [issues]
"#,
        )
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
            &valid_config(),
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
            &valid_config(),
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
    async fn register_valid_config_fails_not_implemented_without_network() {
        let mut ctx = SessionContext::new();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            "http://127.0.0.1:1",
            &valid_config(),
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap_err();
        let err = err.downcast::<OpenConnectorError>().unwrap();
        assert!(matches!(
            err,
            OpenConnectorError::ExecutionNotImplemented { ref name } if name == "saas"
        ));
    }
}
