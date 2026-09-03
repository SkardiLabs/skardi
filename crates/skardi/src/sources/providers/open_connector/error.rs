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

    /// The source requested `read_write` access; the integration is read-only
    /// (milestone one exposes no mutating actions).
    #[error(
        "Open Connector data source '{name}' requests access_mode 'read_write', but \
         Open Connector is read-only — no mutating actions are exposed"
    )]
    ReadWriteNotSupported { name: String },

    /// The data source has no `open_connector` config block at all.
    #[error(
        "Open Connector data source '{name}' requires an 'open_connector' config block \
         (runtime_token_env, bindings, …)"
    )]
    MissingConfig { name: String },

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

    /// The provider reported an in-band error inside an otherwise
    /// successful response envelope — Slack's HTTP-200 `ok: false` +
    /// `error` pattern. The code is a short provider-authored identifier
    /// (`missing_scope`, `not_authed`), bounded before display.
    #[error(
        "Open Connector action '{action_id}' page {page}: the provider reported \
         error '{code}'"
    )]
    ProviderReportedError {
        action_id: String,
        page: usize,
        code: String,
    },

    /// A declared total-pages location resolved to a non-numeric value, so
    /// the scan cannot know when the collection ends.
    #[error(
        "Open Connector pagination total at '{path}' on page {page} is {found}, \
         expected a non-negative integer"
    )]
    PaginationTotalInvalid {
        path: String,
        page: usize,
        found: String,
    },

    /// The declared raw-page-size signal was present but not a non-negative
    /// integer. Treating it as anything else would either truncate the scan
    /// or loop it; carries the JSON *kind* only, never the value.
    #[error(
        "Open Connector pagination raw page size at '{path}' on page {page} is {found}, \
         expected a non-negative integer"
    )]
    PaginationRawPageSizeInvalid {
        path: String,
        page: usize,
        found: String,
    },

    /// A continuation cursor was present at the declared path but was not a
    /// string. Treating it as end-of-collection would silently truncate the
    /// scan, so it fails instead. Carries the JSON *kind* only, never the
    /// value.
    #[error(
        "Open Connector pagination cursor at '{path}' on page {page} is {found}, \
         not a string; refusing to treat it as end-of-collection"
    )]
    PaginationCursorInvalid {
        path: String,
        page: usize,
        found: String,
    },

    /// The declared has-more signal was absent or not a boolean. Guessing
    /// either way could truncate or loop the scan; carries the JSON *kind*
    /// only, never the value.
    #[error(
        "Open Connector pagination has-more signal at '{path}' on page {page} is {found}, \
         expected a boolean"
    )]
    PaginationHasMoreInvalid {
        path: String,
        page: usize,
        found: String,
    },

    /// Pagination failed to advance: the gateway returned an already-seen
    /// cursor, which would loop the scan forever.
    #[error(
        "Open Connector pagination loop detected: cursor '{token}' was already seen \
         (the gateway is not advancing pagination)"
    )]
    PaginationLoop { token: String },

    /// A keyset cursor that cannot continue the scan: the declared field is
    /// absent from the page's last row, an empty string, or the wrong JSON
    /// kind. Stopping instead would silently truncate the scan. The reason
    /// is value-free — keyset cursors are ROW data, and row values never
    /// appear in errors.
    #[error(
        "Open Connector keyset cursor field '{path}' on page {page} {reason}; \
         refusing to treat it as end-of-collection"
    )]
    PaginationKeysetCursorInvalid {
        path: String,
        page: usize,
        reason: String,
    },

    /// A keyset provider re-served a cursor already consumed by an earlier
    /// page — an ordering violation that would loop the scan forever.
    /// Unlike [`Self::PaginationLoop`], the repeated value is deliberately
    /// withheld: it is a field of a provider row, and row values never
    /// appear in errors.
    #[error(
        "Open Connector keyset cursor field '{path}' on page {page} repeats a value \
         already consumed by an earlier page; refusing to loop the scan"
    )]
    PaginationKeysetLoop { path: String, page: usize },

    /// A `single_page` table's premise — one request IS the complete
    /// collection — was contradicted by the response: the provider signalled
    /// more data at the declared path. Ending the scan here would be the
    /// engine's only silent truncation, so it fails instead. Carries the
    /// continuation's kind, never a row value.
    #[error(
        "Open Connector single-page scan is incomplete: the response carries {found} at \
         '{path}' on page {page}, so one request is not the whole collection; the table's \
         `single_page` strategy no longer matches the action's behaviour"
    )]
    SinglePageIncomplete {
        path: String,
        page: usize,
        found: String,
    },

    /// A binding named a source pack that is not built in.
    #[error(
        "Open Connector binding references unknown source pack '{name}' \
         (built-in packs are versioned with Skardi)"
    )]
    SourcePackNotFound { name: String },

    /// A binding exposed a table the source pack does not define.
    #[error("Open Connector source pack '{pack}' has no table '{table}'")]
    SourcePackTableNotFound { pack: String, table: String },

    /// A short table name matched more than one table in the pack;
    /// first-match would silently bind the wrong relational contract.
    #[error(
        "Open Connector source pack '{pack}' has multiple tables matching '{table}' \
         ({candidates}); use the full table ID"
    )]
    SourcePackTableAmbiguous {
        pack: String,
        table: String,
        candidates: String,
    },

    /// A binding pinned a source-pack version that is not the built-in one.
    #[error(
        "Open Connector source pack '{pack}' pinned to version {pinned}, \
         but this Skardi build ships version {actual}"
    )]
    SourcePackVersionMismatch {
        pack: String,
        pinned: u32,
        actual: u32,
    },

    /// A binding omitted a resource input the source-pack table requires.
    #[error(
        "Open Connector binding '{binding}' is missing required resource input '{key}' \
         for the bound source-pack table"
    )]
    MissingResourceInput { binding: String, key: String },

    /// A binding set a resource value to null, which would satisfy the
    /// required-key presence check while sending `null` to the gateway.
    #[error("Open Connector binding '{binding}' sets resource input '{key}' to null")]
    NullResourceValue { binding: String, key: String },

    /// A binding supplied a resource key that none of its bound tables
    /// declare. Requests carry only declared keys (the gateway's strict
    /// action schemas reject the rest), so an unconsumed key is dead
    /// configuration — most likely a typo — and fails registration.
    #[error(
        "Open Connector binding '{binding}' supplies resource key '{key}' that none of its \
         bound tables declare; each table's requests carry only the resource inputs its \
         action contract lists"
    )]
    UnknownResourceKey { binding: String, key: String },

    /// A binding supplied two resource inputs a bound table declares as
    /// alternatives. The upstream executor resolves them by precedence, so
    /// one would silently win and the other become dead configuration —
    /// the scan would then succeed against a scope the operator did not
    /// name, which is worse than failing.
    #[error(
        "Open Connector binding '{binding}' supplies both '{first}' and '{second}' for \
         source-pack table '{table}', which declares them as alternative ways to scope the \
         same collection; set exactly one"
    )]
    ConflictingResourceInputs {
        binding: String,
        table: String,
        first: String,
        second: String,
    },

    /// The discovered action contract does not match the source pack's
    /// expected fingerprint — the upstream action changed incompatibly.
    #[error("Open Connector source-pack table '{table}' failed its compatibility check: {reason}")]
    ActionContractMismatch { table: String, reason: String },

    /// Assembling the DataFusion catalog/schema failed (e.g. a duplicate
    /// registration). An internal consistency error, unrelated to the action
    /// contract.
    #[error("Open Connector catalog registration failed for '{name}': {reason}")]
    CatalogRegistrationFailed { name: String, reason: String },

    /// A scan hit a safety bound before the collection was exhausted, so the
    /// result would be incomplete — fail rather than return partial rows.
    #[error(
        "Open Connector scan of '{table}' exceeded {bound} (limit {limit}); \
         the result would be incomplete"
    )]
    ScanBoundsExceeded {
        table: String,
        bound: &'static str,
        limit: u64,
    },

    /// A scan exceeded its total time budget.
    #[error("Open Connector scan of '{table}' timed out after {seconds}s")]
    ScanTimeout { table: String, seconds: u64 },

    /// A row path was malformed (must be `$.key[.key…]` object-key segments).
    #[error("Open Connector row path '{path}' is invalid: {reason}")]
    InvalidRowPath { path: String, reason: String },

    /// A row path segment was absent from its parent object while extracting
    /// rows from a page. Absence is a *missing* key (nullable columns become
    /// null) — not a structural change.
    #[error(
        "Open Connector row path '{path}' failed at segment '{segment}' on page {page}: \
         the key is missing from an object"
    )]
    RowPathNotFound {
        path: String,
        segment: String,
        page: usize,
    },

    /// A row path had to traverse into a value that is not an object — a
    /// structural mismatch (e.g. `user` changed from object to string
    /// upstream), which must fail even for nullable columns.
    #[error(
        "Open Connector row path '{path}' cannot descend into segment '{segment}' on page {page}: \
         expected an object, found {found}"
    )]
    RowPathNotObject {
        path: String,
        segment: String,
        page: usize,
        found: String,
    },

    /// The row path resolved to a non-array value; relational rows must be an
    /// array of objects.
    #[error(
        "Open Connector row path '{path}' on page {page} resolved to {found}, expected an array of row objects"
    )]
    RowPathNotArray {
        path: String,
        page: usize,
        found: String,
    },

    /// A single-object table's row path resolved to something that is not an
    /// object. Distinct from [`Self::RowPathNotArray`] on purpose: the two
    /// name opposite expectations, and one message covering both would leave
    /// a reader unable to tell a table whose upstream started returning a
    /// list from one whose declared shape is simply wrong.
    #[error(
        "Open Connector row path '{path}' on page {page} resolved to {found}, expected a single \
         row object (this table declares `row_shape: object`)"
    )]
    RowPathNotObjectRow {
        path: String,
        page: usize,
        found: String,
    },

    /// A row could not be converted to the table's fixed Arrow schema. The
    /// `found` carries the JSON *kind* only — never the value itself, which
    /// may contain sensitive data.
    #[error(
        "Open Connector conversion failed for column '{column}' (path '{path}') \
         at page {page}, row {row}: expected {expected}, found {found}"
    )]
    ConversionFailed {
        path: String,
        column: String,
        page: usize,
        row: usize,
        expected: String,
        found: String,
    },

    /// An embedded source-pack asset failed to parse or validate. A build
    /// defect (assets ship inside the binary), surfaced as a registration /
    /// UDTF-setup diagnostic instead of a panic.
    #[error("embedded source pack asset '{asset}' is invalid: {reason}")]
    SourcePackAssetInvalid { asset: String, reason: String },

    /// A response body grew past the configured decoding bound.
    #[error("Open Connector {operation} response exceeded the {limit_bytes}-byte bound")]
    ResponseTooLarge {
        operation: String,
        limit_bytes: usize,
    },

    /// A response was not the JSON shape the client contract expects.
    #[error("Open Connector {operation} returned an invalid response: {reason}")]
    InvalidGatewayResponse { operation: String, reason: String },

    /// A UDTF named a gateway that is not a registered `open_connector` data
    /// source (or whose registration failed).
    #[error(
        "Open Connector gateway '{name}' is not registered; the first UDTF argument \
         must name a 'type: open_connector' data source from the context configuration"
    )]
    UdtfGatewayNotRegistered { name: String },

    /// A UDTF referenced an action whose metadata was never discovered.
    /// Discovery happens only at registration (query planning performs no
    /// network I/O), and only for bound source-pack tables and allowlisted
    /// raw actions.
    #[error(
        "Open Connector action '{action_id}' was not discovered when gateway '{gateway}' \
         was registered; bind its source-pack table in the context YAML or add the \
         action to 'raw_action_allowlist' (query planning never contacts the gateway)"
    )]
    ActionNotDiscovered { gateway: String, action_id: String },

    /// `open_connector_scan` was called with an action outside the gateway's
    /// `raw_action_allowlist`. Raw-action access is default-deny.
    #[error(
        "Open Connector action '{action_id}' is not in the 'raw_action_allowlist' of \
         gateway '{gateway}'; open_connector_scan executes explicitly allowlisted \
         actions only (default-deny)"
    )]
    RawActionNotAllowlisted { gateway: String, action_id: String },

    /// An allowlisted raw action's metadata classifies it as mutating.
    /// The allowlist alone never grants execution.
    #[error(
        "Open Connector action '{action_id}' is classified as mutating by its gateway \
         metadata; open_connector_scan executes non-mutating reads only"
    )]
    RawActionMutating { action_id: String },

    /// An allowlisted raw action's metadata does not say whether it mutates;
    /// default-deny refuses to execute it, naming the classification gap.
    #[error(
        "Open Connector action '{action_id}' does not declare a read-only \
         classification in its gateway metadata; refusing to execute it \
         (default-deny — the allowlist alone does not grant execution)"
    )]
    RawActionReadOnlyUnknown { action_id: String },

    /// A raw scan's row path does not resolve to a deterministic object row
    /// type in the discovered action output schema, so no stable Arrow
    /// schema can be planned.
    #[error(
        "Open Connector raw scan of action '{action_id}' cannot derive a deterministic \
         row type at '{row_path}': {reason}; use a built-in source-pack table \
         (open_connector_query) or contribute a source-pack definition instead"
    )]
    RawRowTypeIndeterminate {
        action_id: String,
        row_path: String,
        reason: String,
    },
}
