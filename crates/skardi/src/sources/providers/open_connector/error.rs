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

    /// The environment variable holding the gateway runtime token was unset.
    #[error(
        "Environment variable '{env}' not found: it must contain the Open Connector \
         gateway runtime token"
    )]
    MissingRuntimeToken { env: String },

    /// The runtime token is not usable as an HTTP header value — the classic
    /// cause is a trailing newline from `export TOKEN="$(cat token.txt)"`.
    /// Checked at client construction so a malformed credential fails fast
    /// with the actual cause instead of three retried "builder error"s.
    #[error(
        "Open Connector runtime token from '{env}' is invalid: {reason} \
         (check for a trailing newline or other control characters)"
    )]
    InvalidRuntimeToken { env: String, reason: String },

    /// reqwest could not build the request (e.g. an illegal header value).
    /// A permanent client-side failure — never retried.
    #[error("Open Connector {operation} could not build the request: {reason}")]
    RequestBuildFailed { operation: String, reason: String },

    /// The gateway URL used a non-HTTP(S) scheme or embedded credentials.
    #[error(
        "Open Connector gateway URL '{url}' must use http:// or https:// and must \
         not embed credentials (the runtime token is sent as a Bearer header)"
    )]
    InvalidGatewayUrl { url: String },

    /// The gateway URL carried a query string or fragment. Neither has any
    /// meaning for a base URL, and query strings are a classic way to smuggle
    /// tokens (`?token=…`, `?access_token=…`) into logs, `Debug` output, and
    /// the data-sources API response.
    #[error(
        "Open Connector gateway URL '{url}' must not contain query parameters or a \
         fragment (credentials in the URL would leak into logs and the data-sources \
         API; the runtime token belongs in the configured environment variable)"
    )]
    GatewayUrlWithQueryOrFragment { url: String },

    /// `reqwest::Client` construction failed.
    #[error("Failed to build the Open Connector HTTP client: {reason}")]
    HttpClientBuild { reason: String },

    /// The gateway health check returned a terminal (non-retryable) failure.
    #[error("Open Connector gateway health check failed for '{url}': {reason}")]
    HealthCheckFailed { url: String, reason: String },

    /// The gateway answered action discovery with 404.
    #[error("Open Connector action '{action_id}' was not found on the gateway")]
    ActionNotFound { action_id: String },

    /// Action discovery failed for a reason other than "not found".
    #[error("Failed to discover Open Connector action '{action_id}': {reason}")]
    ActionDiscoveryFailed { action_id: String, reason: String },

    /// The action exists but cannot execute on this gateway runtime.
    #[error("Open Connector action '{action_id}' is not locally executable on this gateway")]
    ActionNotLocallyExecutable { action_id: String },

    /// The gateway omitted the executability flag entirely; default-deny
    /// treats "did not say" as "not executable".
    #[error(
        "Open Connector action '{action_id}' does not declare whether it is locally \
         executable; refusing to treat it as executable (default-deny)"
    )]
    ActionExecutabilityUnknown { action_id: String },

    /// An action ID could escape the `/v1/actions/` namespace: `/` moves
    /// path segments, and a bare `.` / `..` is resolved away by `Url::join`
    /// even after percent-encoding (dots are preserved by the encode set).
    #[error(
        "Open Connector action ID '{action_id}' is invalid: {reason} \
         (action IDs must be single path segments; '/', '.', and '..' are not allowed)"
    )]
    InvalidActionId { action_id: String, reason: String },

    /// An action execution call returned a terminal (non-retryable) failure.
    #[error("Open Connector action '{action_id}' execution failed: {reason}")]
    ActionExecutionFailed { action_id: String, reason: String },

    /// Retries on 429 / transient 5xx / transport errors were exhausted.
    #[error("Open Connector {operation} failed after {attempts} attempt(s); last error: {reason}")]
    RetriesExhausted {
        operation: String,
        attempts: u32,
        reason: String,
    },

    /// A non-idempotent call (POST execute) failed with an ambiguous
    /// transport error: the request may have reached the gateway, so the
    /// client does not retry it — re-sending could re-execute the action
    /// against the SaaS provider.
    #[error(
        "Open Connector {operation} failed with a transport error ({reason}); \
         not retried because the request may have reached the gateway and \
         re-execution is not safe"
    )]
    NonIdempotentAmbiguousFailure { operation: String, reason: String },

    /// A response body grew past the configured decoding bound.
    #[error("Open Connector {operation} response exceeded the {limit_bytes}-byte bound")]
    ResponseTooLarge {
        operation: String,
        limit_bytes: usize,
    },

    /// A response was not the JSON shape the client contract expects.
    #[error("Open Connector {operation} returned an invalid response: {reason}")]
    InvalidGatewayResponse { operation: String, reason: String },

    /// The gateway is reachable and its action metadata loaded, but table
    /// registration (source packs, scan engine) has not landed yet.
    #[error(
        "Open Connector source '{name}': gateway contact and action discovery succeeded, \
         but table registration is not implemented yet — source packs and the scan \
         engine arrive in the next milestone"
    )]
    ExecutionNotImplemented { name: String },
}
