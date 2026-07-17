//! Error types for the Open Connector integration.

use thiserror::Error;

/// Errors surfaced while validating or registering an Open Connector gateway
/// data source.
///
/// All variants are raised **before** any network I/O: a misconfigured
/// gateway fails at config-load or registration time with a targeted message
/// rather than an opaque failure at first query.
///
/// # Example
/// ```
/// use skardi::sources::providers::open_connector::{OpenConnectorConfig, OpenConnectorError};
///
/// // A config with no bindings is valid; one with a duplicate binding name is not.
/// let yaml = r#"
/// runtime_token_env: OPEN_CONNECTOR_TOKEN
/// bindings:
///   - name: github
///     source_pack: github
///     tables: [issues]
///   - name: github
///     source_pack: github
///     tables: [commits]
/// "#;
/// let config: OpenConnectorConfig = serde_yaml::from_str(yaml).unwrap();
/// let err = config.validate().unwrap_err();
/// assert!(matches!(
///     err,
///     OpenConnectorError::DuplicateBindingName { ref name } if name == "github"
/// ));
/// ```
#[derive(Debug, Error)]
pub enum OpenConnectorError {
    /// `runtime_token_env` was empty or whitespace-only.
    #[error("Open Connector config field 'runtime_token_env' must not be empty")]
    EmptyRuntimeTokenEnv,

    /// The gateway URL (`connection_string`) was empty or whitespace-only.
    #[error(
        "Open Connector data source '{name}' requires a non-empty connection_string \
         (the gateway URL, e.g. http://open-connector:3000)"
    )]
    EmptyGatewayUrl { name: String },

    /// A binding had an empty or whitespace-only name.
    #[error("Open Connector binding names must not be empty")]
    EmptyBindingName,

    /// Two bindings share a name; binding names become schema names in the
    /// gateway catalog and must be unique.
    #[error(
        "Open Connector config has duplicate binding name '{name}'; \
         binding names become catalog schema names and must be unique"
    )]
    DuplicateBindingName { name: String },

    /// A binding did not name its source pack.
    #[error("Open Connector binding '{binding}' must name a 'source_pack'")]
    EmptySourcePack { binding: String },

    /// A binding exposed no tables.
    #[error("Open Connector binding '{binding}' must expose at least one table")]
    EmptyTableList { binding: String },

    /// A binding listed an empty table name.
    #[error("Open Connector binding '{binding}' contains an empty table name")]
    EmptyTableName { binding: String },

    /// A binding listed the same table twice.
    #[error("Open Connector binding '{binding}' lists table '{table}' more than once")]
    DuplicateTableName { binding: String, table: String },

    /// `raw_action_allowlist` contained an empty entry.
    #[error("Open Connector 'raw_action_allowlist' must not contain empty entries")]
    EmptyAllowlistEntry,

    /// A safety bound (`max_pages` / `max_rows`) was set to zero, which would
    /// make every scan fail.
    #[error("Open Connector safety bound '{field}' must be greater than zero")]
    ZeroSafetyBound { field: &'static str },

    /// The source was registered with `hierarchy_level: table`; a gateway is
    /// always exposed as a catalog.
    #[error(
        "Open Connector data source '{name}' must use hierarchy_level 'catalog' — \
         a gateway is exposed as a DataFusion catalog, not a single table"
    )]
    CatalogHierarchyRequired { name: String },

    /// The config passed validation but execution has not landed yet.
    #[error(
        "Open Connector source '{name}' passed validation, but gateway execution is \
         not implemented yet — only the typed-config foundation has landed; \
         registration arrives with the HTTP client milestone"
    )]
    ExecutionNotImplemented { name: String },
}
