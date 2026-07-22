//! Typed configuration for an Open Connector gateway data source.
//!
//! Skardi's generic `options: HashMap<String, String>` cannot safely
//! represent nested gateway bindings, resources, and overrides, so
//! `type: open_connector` sources carry this typed struct instead. It is
//! shared by the server (`DataSource::open_connector`) and the CLI so both
//! parse and validate identically.
//!
//! The YAML shape matches the design spec
//! (`docs/superpowers/specs/2026-07-11-open-connector-integration-design.md`):
//!
//! ```yaml
//! open_connector:
//!   runtime_token_env: OPEN_CONNECTOR_TOKEN
//!   request_timeout_seconds: 30
//!   scan_timeout_seconds: 300
//!   max_pages: 100
//!   max_rows: 100000
//!   cache_max_bytes: 268435456
//!   raw_action_allowlist:
//!     - github.list_repository_issues
//!   bindings:
//!     - name: github_skardi
//!       source_pack: github
//!       connection_alias: work
//!       resource:
//!         owner: SkardiLabs
//!         repo: skardi
//!       tables:
//!         - issues
//!         - pull_requests
//! ```

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

use super::error::OpenConnectorError;

/// Default timeout for a single gateway HTTP request.
const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 30;
/// Default deadline for one full scan (all pages).
const DEFAULT_SCAN_TIMEOUT_SECONDS: u64 = 300;
/// Default cap on pages fetched per scan.
const DEFAULT_MAX_PAGES: u32 = 100;
/// Default cap on rows emitted per scan.
const DEFAULT_MAX_ROWS: u64 = 100_000;
/// Default byte budget of the shared scan cache (256 MiB).
const DEFAULT_CACHE_MAX_BYTES: u64 = 256 * 1024 * 1024;

fn default_request_timeout_seconds() -> u64 {
    DEFAULT_REQUEST_TIMEOUT_SECONDS
}

fn default_scan_timeout_seconds() -> u64 {
    DEFAULT_SCAN_TIMEOUT_SECONDS
}

fn default_max_pages() -> u32 {
    DEFAULT_MAX_PAGES
}

fn default_max_rows() -> u64 {
    DEFAULT_MAX_ROWS
}

fn default_cache_max_bytes() -> u64 {
    DEFAULT_CACHE_MAX_BYTES
}

fn default_max_response_bytes() -> u64 {
    super::client::DEFAULT_MAX_RESPONSE_BYTES as u64
}

fn default_max_attempts() -> u32 {
    super::client::MAX_ATTEMPTS
}

/// Typed configuration for `type: open_connector` data sources.
///
/// Provider credentials never appear here — Skardi only learns the gateway
/// URL (via `connection_string` on the data source) and the name of the
/// environment variable holding the gateway runtime token.
///
/// Unknown fields are rejected: a misspelled key (e.g.
/// `raw_action_allowlists`) must fail loudly instead of being silently
/// dropped and changing the config's meaning.
///
/// # Example
/// ```
/// use skardi::sources::providers::open_connector::OpenConnectorConfig;
///
/// let yaml = r#"
/// runtime_token_env: OPEN_CONNECTOR_TOKEN
/// bindings:
///   - name: github_skardi
///     source_pack: github
///     resource:
///       owner: SkardiLabs
///       repo: skardi
///     tables: [issues, pull_requests]
/// "#;
/// let config: OpenConnectorConfig = serde_yaml::from_str(yaml).unwrap();
/// config.validate().unwrap();
///
/// // Defaults follow the design spec's safety bounds.
/// assert_eq!(config.request_timeout_seconds, 30);
/// assert_eq!(config.scan_timeout_seconds, 300);
/// assert_eq!(config.max_pages, 100);
/// assert_eq!(config.max_rows, 100_000);
/// assert_eq!(config.cache_ttl_seconds, 0); // live reads by default
/// assert_eq!(config.bindings.len(), 1);
/// ```
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OpenConnectorConfig {
    /// Name of the environment variable holding the gateway runtime token.
    /// The token value is read from the process environment at registration
    /// time, never from this file.
    pub runtime_token_env: String,

    /// Timeout for one gateway HTTP request, in seconds.
    #[serde(default = "default_request_timeout_seconds")]
    pub request_timeout_seconds: u64,

    /// Total deadline for one scan across all pages, in seconds.
    #[serde(default = "default_scan_timeout_seconds")]
    pub scan_timeout_seconds: u64,

    /// Maximum pages a single scan may fetch.
    #[serde(default = "default_max_pages")]
    pub max_pages: u32,

    /// Maximum rows a single scan may emit.
    #[serde(default = "default_max_rows")]
    pub max_rows: u64,

    /// Byte budget of the shared bounded scan cache.
    #[serde(default = "default_cache_max_bytes")]
    pub cache_max_bytes: u64,

    /// Byte bound on one decoded gateway response body. Bodies are decoded
    /// while streaming, so this caps memory per request, not per scan.
    #[serde(default = "default_max_response_bytes")]
    pub max_response_bytes: u64,

    /// Maximum attempts per gateway call (including the first). Retried
    /// statuses and transport errors consume attempts per the client's
    /// idempotency-aware retry policy.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,

    /// Default TTL for shared scan-cache entries, in seconds. `0` disables
    /// caching, i.e. every scan is a live read.
    #[serde(default)]
    pub cache_ttl_seconds: u64,

    /// Action IDs that `open_connector_scan` may invoke directly. Empty by
    /// default — raw-action access is default-deny.
    #[serde(default)]
    pub raw_action_allowlist: Vec<String>,

    /// Named resource bindings. Each binding becomes a schema in the gateway
    /// catalog: `<gateway>.<binding>.<table>`.
    #[serde(default)]
    pub bindings: Vec<OpenConnectorBinding>,
}

impl OpenConnectorConfig {
    /// Validate the configuration, failing on the first problem found.
    ///
    /// Pure and allocation-light: no network I/O, so it runs identically in
    /// the server's config validation and in the provider's registration
    /// path (which also covers the CLI).
    pub fn validate(&self) -> Result<(), OpenConnectorError> {
        if self.runtime_token_env.trim().is_empty() {
            return Err(OpenConnectorError::EmptyRuntimeTokenEnv);
        }
        // Zero timeouts make every request fail instantly (as an opaque
        // retryable transport error), so they are config bugs, not bounds.
        if self.request_timeout_seconds == 0 {
            return Err(OpenConnectorError::ZeroSafetyBound {
                field: "request_timeout_seconds",
            });
        }
        if self.scan_timeout_seconds == 0 {
            return Err(OpenConnectorError::ZeroSafetyBound {
                field: "scan_timeout_seconds",
            });
        }
        if self.max_pages == 0 {
            return Err(OpenConnectorError::ZeroSafetyBound { field: "max_pages" });
        }
        if self.max_rows == 0 {
            return Err(OpenConnectorError::ZeroSafetyBound { field: "max_rows" });
        }
        // A zero response bound rejects every body; zero attempts means no
        // call is ever made — both are config bugs, not bounds.
        if self.max_response_bytes == 0 {
            return Err(OpenConnectorError::ZeroSafetyBound {
                field: "max_response_bytes",
            });
        }
        if self.max_attempts == 0 {
            return Err(OpenConnectorError::ZeroSafetyBound {
                field: "max_attempts",
            });
        }
        if self
            .raw_action_allowlist
            .iter()
            .any(|entry| entry.trim().is_empty())
        {
            return Err(OpenConnectorError::EmptyAllowlistEntry);
        }
        // Allowlist entries become URL path segments at discovery time, so
        // validate them here for an early config error (the client re-checks
        // at its own boundary for UDTF-supplied IDs).
        for entry in &self.raw_action_allowlist {
            validate_action_id(entry)?;
        }

        let mut names = HashSet::with_capacity(self.bindings.len());
        for binding in &self.bindings {
            binding.validate()?;
            if !names.insert(binding.name.as_str()) {
                return Err(OpenConnectorError::DuplicateBindingName {
                    name: binding.name.clone(),
                });
            }
        }
        Ok(())
    }
}

/// Validate one action ID for safe use as a URL path segment.
///
/// `/` would move path segments, and a bare `.` / `..` is resolved away by
/// `Url::join` even after percent-encoding (the encode set preserves dots) —
/// either one escapes the `/v1/actions/` namespace onto a misrouted endpoint.
/// Shared by config validation (early error on `raw_action_allowlist`) and
/// the client boundary (defense in depth for UDTF-supplied IDs).
pub(crate) fn validate_action_id(action_id: &str) -> Result<(), OpenConnectorError> {
    let reason = if action_id.contains('/') {
        Some("must not contain '/'")
    } else if action_id == "." || action_id == ".." {
        Some("a bare dot segment is resolved by URL joining")
    } else {
        None
    };
    match reason {
        Some(reason) => Err(OpenConnectorError::InvalidActionId {
            action_id: action_id.to_string(),
            reason: reason.to_string(),
        }),
        None => Ok(()),
    }
}

/// A named binding of a built-in source pack to a concrete SaaS resource.
///
/// Users bind packs to resources but do not define the pack's internal
/// relational contract (action, row path, pagination, stable schema) — that
/// stays in the Skardi-maintained source pack itself.
///
/// Unknown fields are rejected: a misspelled key (e.g.
/// `source_pack_versions`) must fail loudly instead of being silently
/// dropped, which would quietly disable the version pin it was meant to set.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OpenConnectorBinding {
    /// Binding name; becomes the schema name under the gateway catalog.
    pub name: String,

    /// Built-in source-pack identifier, e.g. `github`.
    pub source_pack: String,

    /// Optional explicit source-pack version pin. When omitted, the latest
    /// built-in version is used at registration time. Pinning keeps a Skardi
    /// upgrade from silently changing a bound table's schema after restart.
    #[serde(default)]
    pub source_pack_version: Option<u32>,

    /// Open Connector connection alias. When omitted, the gateway's default
    /// connection is used.
    #[serde(default)]
    pub connection_alias: Option<String>,

    /// Resource inputs required by the source pack (e.g. `owner` / `repo`
    /// for GitHub). Required keys are defined by the pack and checked when
    /// the binding is registered against a real source-pack registry.
    #[serde(default)]
    pub resource: BTreeMap<String, String>,

    /// Source-pack tables to expose under this binding.
    pub tables: Vec<String>,
}

impl OpenConnectorBinding {
    /// Validate one binding (called from [`OpenConnectorConfig::validate`],
    /// which additionally checks name uniqueness across bindings).
    fn validate(&self) -> Result<(), OpenConnectorError> {
        if self.name.trim().is_empty() {
            return Err(OpenConnectorError::EmptyBindingName);
        }
        if self.source_pack.trim().is_empty() {
            return Err(OpenConnectorError::EmptySourcePack {
                binding: self.name.clone(),
            });
        }
        if self.tables.is_empty() {
            return Err(OpenConnectorError::EmptyTableList {
                binding: self.name.clone(),
            });
        }
        let mut tables = HashSet::with_capacity(self.tables.len());
        for table in &self.tables {
            if table.trim().is_empty() {
                return Err(OpenConnectorError::EmptyTableName {
                    binding: self.name.clone(),
                });
            }
            if !tables.insert(table.as_str()) {
                return Err(OpenConnectorError::DuplicateTableName {
                    binding: self.name.clone(),
                    table: table.clone(),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> OpenConnectorConfig {
        serde_yaml::from_str(yaml).expect("parse config")
    }

    #[test]
    fn parses_design_spec_example() {
        // The full YAML shape from the integration design spec.
        let config = parse(
            r#"
runtime_token_env: OPEN_CONNECTOR_TOKEN
request_timeout_seconds: 30
scan_timeout_seconds: 300
max_pages: 100
max_rows: 100000
cache_max_bytes: 268435456
raw_action_allowlist:
  - github.list_repository_issues
  - github.search_code
bindings:
  - name: github_skardi
    source_pack: github
    connection_alias: work
    resource:
      owner: SkardiLabs
      repo: skardi
    tables:
      - issues
      - pull_requests
      - commits
"#,
        );
        config.validate().expect("spec example is valid");
        assert_eq!(config.raw_action_allowlist.len(), 2);

        let binding = &config.bindings[0];
        assert_eq!(binding.name, "github_skardi");
        assert_eq!(binding.source_pack, "github");
        assert_eq!(binding.connection_alias.as_deref(), Some("work"));
        assert_eq!(binding.source_pack_version, None);
        assert_eq!(
            binding.resource.get("owner").map(String::as_str),
            Some("SkardiLabs")
        );
        assert_eq!(binding.tables, vec!["issues", "pull_requests", "commits"]);
    }

    #[test]
    fn defaults_applied_to_minimal_config() {
        let config = parse("runtime_token_env: OPEN_CONNECTOR_TOKEN");
        config.validate().expect("minimal config is valid");
        assert_eq!(config.request_timeout_seconds, 30);
        assert_eq!(config.scan_timeout_seconds, 300);
        assert_eq!(config.max_pages, 100);
        assert_eq!(config.max_rows, 100_000);
        assert_eq!(config.cache_max_bytes, 256 * 1024 * 1024);
        assert_eq!(config.max_response_bytes, 16 * 1024 * 1024);
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.cache_ttl_seconds, 0);
        assert!(config.raw_action_allowlist.is_empty());
        assert!(config.bindings.is_empty());
    }

    #[test]
    fn binding_optional_fields_default() {
        let config = parse(
            r#"
runtime_token_env: T
bindings:
  - name: b
    source_pack: github
    tables: [issues]
"#,
        );
        let binding = &config.bindings[0];
        assert_eq!(binding.connection_alias, None);
        assert_eq!(binding.source_pack_version, None);
        assert!(binding.resource.is_empty());
    }

    #[test]
    fn source_pack_version_pin_parses() {
        let config = parse(
            r#"
runtime_token_env: T
bindings:
  - name: b
    source_pack: github
    source_pack_version: 1
    tables: [issues]
"#,
        );
        assert_eq!(config.bindings[0].source_pack_version, Some(1));
    }

    #[test]
    fn validate_rejects_empty_runtime_token_env() {
        let config = parse("runtime_token_env: '  '");
        assert!(matches!(
            config.validate(),
            Err(OpenConnectorError::EmptyRuntimeTokenEnv)
        ));
    }

    #[test]
    fn validate_rejects_zero_safety_bounds() {
        let config = parse("runtime_token_env: T\nmax_pages: 0");
        assert!(matches!(
            config.validate(),
            Err(OpenConnectorError::ZeroSafetyBound { field: "max_pages" })
        ));

        let config = parse("runtime_token_env: T\nmax_rows: 0");
        assert!(matches!(
            config.validate(),
            Err(OpenConnectorError::ZeroSafetyBound { field: "max_rows" })
        ));
    }

    #[test]
    fn validate_rejects_zero_timeouts() {
        // A zero timeout fails every request instantly, surfacing as an
        // opaque retryable transport error at registration — it must be a
        // targeted config error instead.
        let config = parse("runtime_token_env: T\nrequest_timeout_seconds: 0");
        assert!(matches!(
            config.validate(),
            Err(OpenConnectorError::ZeroSafetyBound {
                field: "request_timeout_seconds"
            })
        ));

        let config = parse("runtime_token_env: T\nscan_timeout_seconds: 0");
        assert!(matches!(
            config.validate(),
            Err(OpenConnectorError::ZeroSafetyBound {
                field: "scan_timeout_seconds"
            })
        ));
    }

    #[test]
    fn validate_rejects_zero_client_bounds() {
        // Zero response bound rejects every body; zero attempts means no
        // call is ever made.
        let config = parse("runtime_token_env: T\nmax_response_bytes: 0");
        assert!(matches!(
            config.validate(),
            Err(OpenConnectorError::ZeroSafetyBound {
                field: "max_response_bytes"
            })
        ));

        let config = parse("runtime_token_env: T\nmax_attempts: 0");
        assert!(matches!(
            config.validate(),
            Err(OpenConnectorError::ZeroSafetyBound {
                field: "max_attempts"
            })
        ));
    }

    #[test]
    fn validate_rejects_empty_allowlist_entry() {
        let config = parse("runtime_token_env: T\nraw_action_allowlist: ['github.x', ' ']");
        assert!(matches!(
            config.validate(),
            Err(OpenConnectorError::EmptyAllowlistEntry)
        ));
    }

    #[test]
    fn validate_rejects_traversal_allowlist_entries() {
        // Allowlist entries become URL path segments at discovery time; dot
        // segments and slashes would escape the /v1/actions/ namespace.
        for bad in ["..", ".", "a/b"] {
            let config = parse(&format!(
                "runtime_token_env: T\nraw_action_allowlist: ['{bad}']"
            ));
            assert!(
                matches!(
                    config.validate(),
                    Err(OpenConnectorError::InvalidActionId { .. })
                ),
                "'{bad}' should be rejected"
            );
        }

        // Dots inside a normal namespaced ID stay legal.
        let config =
            parse("runtime_token_env: T\nraw_action_allowlist: ['github.list_repository_issues']");
        config.validate().expect("namespaced ID is valid");
    }

    #[test]
    fn validate_rejects_duplicate_binding_names() {
        let config = parse(
            r#"
runtime_token_env: T
bindings:
  - name: dup
    source_pack: github
    tables: [issues]
  - name: dup
    source_pack: jira
    tables: [issues]
"#,
        );
        assert!(matches!(
            config.validate(),
            Err(OpenConnectorError::DuplicateBindingName { ref name }) if name == "dup"
        ));
    }

    #[test]
    fn validate_rejects_empty_binding_name() {
        let config = parse(
            "runtime_token_env: T\nbindings:\n  - name: ' '\n    source_pack: github\n    tables: [issues]",
        );
        assert!(matches!(
            config.validate(),
            Err(OpenConnectorError::EmptyBindingName)
        ));
    }

    #[test]
    fn validate_rejects_empty_source_pack() {
        let config = parse(
            "runtime_token_env: T\nbindings:\n  - name: b\n    source_pack: ''\n    tables: [issues]",
        );
        assert!(matches!(
            config.validate(),
            Err(OpenConnectorError::EmptySourcePack { ref binding }) if binding == "b"
        ));
    }

    #[test]
    fn validate_rejects_empty_table_list() {
        let config = parse(
            "runtime_token_env: T\nbindings:\n  - name: b\n    source_pack: github\n    tables: []",
        );
        assert!(matches!(
            config.validate(),
            Err(OpenConnectorError::EmptyTableList { ref binding }) if binding == "b"
        ));
    }

    #[test]
    fn validate_rejects_empty_table_name() {
        let config = parse(
            "runtime_token_env: T\nbindings:\n  - name: b\n    source_pack: github\n    tables: ['']",
        );
        assert!(matches!(
            config.validate(),
            Err(OpenConnectorError::EmptyTableName { ref binding }) if binding == "b"
        ));
    }

    #[test]
    fn validate_rejects_duplicate_table_names() {
        let config = parse(
            "runtime_token_env: T\nbindings:\n  - name: b\n    source_pack: github\n    tables: [issues, issues]",
        );
        assert!(matches!(
            config.validate(),
            Err(OpenConnectorError::DuplicateTableName { ref binding, ref table })
                if binding == "b" && table == "issues"
        ));
    }

    #[test]
    fn parse_rejects_unknown_top_level_field() {
        // `raw_action_allowlists` (plural typo) must fail loudly, not be
        // silently dropped — a silently dropped allowlist entry would
        // unexpectedly deny a raw action the operator meant to permit.
        let err = serde_yaml::from_str::<OpenConnectorConfig>(
            "runtime_token_env: T\nraw_action_allowlists: [github.x]",
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("raw_action_allowlists"),
            "error should name the unknown field: {err}"
        );
    }

    #[test]
    fn parse_rejects_misspelled_source_pack_version() {
        // The exact failure mode the version pin exists to prevent: a typo'd
        // `source_pack_versions` would otherwise parse as "no pin", silently
        // falling back to the latest pack on the next upgrade.
        let err = serde_yaml::from_str::<OpenConnectorConfig>(
            "runtime_token_env: T\nbindings:\n  - name: b\n    source_pack: github\n    source_pack_versions: 1\n    tables: [issues]",
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("source_pack_versions"),
            "error should name the unknown field: {err}"
        );
    }

    #[test]
    fn parse_rejects_unknown_binding_field() {
        let err = serde_yaml::from_str::<OpenConnectorConfig>(
            "runtime_token_env: T\nbindings:\n  - name: b\n    source_pack: github\n    table: issues\n    tables: [issues]",
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("table"),
            "error should name the unknown field: {err}"
        );
    }

    #[test]
    fn parse_rejects_relational_contract_overrides_on_bindings() {
        // The relational contract (action, row path, schema, pagination)
        // belongs to the Skardi-maintained source pack. A binding that tries
        // to swap any of it must fail loudly — accepting-and-ignoring would
        // let a config claim a different action than the one that runs.
        for (field, value) in [
            ("action", "github.delete_repository"),
            ("row_path", "$.other"),
            ("pagination", "cursor"),
            ("columns", "[]"),
        ] {
            let err = serde_yaml::from_str::<OpenConnectorConfig>(&format!(
                "runtime_token_env: T\nbindings:\n  - name: b\n    source_pack: github\n    {field}: {value}\n    tables: [issues]",
            ))
            .unwrap_err();
            assert!(
                err.to_string().contains(field),
                "override '{field}' should be rejected by name: {err}"
            );
        }
    }

    #[test]
    fn parse_rejects_misspelled_cache_field() {
        let err = serde_yaml::from_str::<OpenConnectorConfig>(
            "runtime_token_env: T\ncache_ttl_second: 60",
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("cache_ttl_second"),
            "error should name the unknown field: {err}"
        );
    }
}
