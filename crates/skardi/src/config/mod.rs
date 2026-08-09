//! Workspace configuration: the ctx (`kind: context` / `spec.data_sources[]`)
//! and pipeline (`metadata.name` / `spec.query`) model, their YAML parsing, and
//! the structural + reference validation that does not need a live source.
//!
//! This is the single source of truth for that model and those checks. The
//! `skardi-server` binary reuses it: it re-exports [`DataSource`] and
//! [`ConfigError`], parses ctx/pipeline YAML through [`parse_context`] /
//! [`parse_pipeline`], and runs [`validate_source_decls`] before layering on the
//! I/O-bound checks it alone owns (S3 credential config, connection probes,
//! table registration).
//!
//! [`validate_config`] composes the credential-free pieces into one entry point
//! a control plane can call to gate a deploy — "does this config parse, and do
//! its pipelines only read sources the ctx declares?" — with no connection
//! strings read, no tables registered, and no
//! [`SessionContext`](datafusion::prelude::SessionContext) built. The reference
//! resolution is computed with the engine's own re-exported sqlparser (via
//! [`referenced_sources`](crate::sources::sql_validator::referenced_sources)),
//! so it can never disagree with how the engine later resolves the same SQL.

use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::sources::providers::open_connector::OpenConnectorConfig;
use crate::sources::sql_validator::{SqlValidatorConfig, referenced_sources, validate_sql};
use crate::sources::{AccessMode, DataSourceType, HierarchyLevel};

/// Data source configuration for context loading.
///
/// The one data-source model: parsed from a ctx `spec.data_sources[]`, reused by
/// the server for registration and by [`validate_config`] for static checks.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DataSource {
    /// Unique name for the data source (used as table name in SQL)
    pub name: String,
    /// Type of data source (CSV, Parquet, etc.)
    #[serde(rename = "type")]
    pub source_type: DataSourceType,
    /// File path to the data source (for file-based sources)
    #[serde(default)]
    pub path: PathBuf,
    /// Connection string for database sources (e.g., PostgreSQL)
    pub connection_string: Option<String>,
    /// Optional explicit schema (field name -> type mapping)
    pub schema: Option<HashMap<String, String>>,
    /// Optional format-specific options
    pub options: Option<HashMap<String, String>>,
    /// Registration hierarchy level for database sources (omitted → table)
    #[serde(default)]
    pub hierarchy_level: HierarchyLevel,
    /// Access mode: read_only (default) or read_write
    #[serde(default)]
    pub access_mode: AccessMode,
    /// If true, load the entire table into memory at startup (only for Csv, Parquet, Iceberg)
    #[serde(default)]
    pub enable_cache: bool,
    /// Optional natural-language description of the table this data source exposes.
    /// Used as a fallback table-level description on the catalog endpoint when no
    /// matching entry is present in a loaded `kind: semantics` file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Typed Open Connector gateway configuration. Required when `type` is
    /// `open_connector`, rejected for every other type: nested bindings and
    /// resources do not fit the flat `options` map.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_connector: Option<OpenConnectorConfig>,
}

/// Top-level envelope for context YAML files:
/// `{ kind: context, metadata: {...}, spec: { data_sources: [...] } }`.
///
/// `kind` is an `Option` so the parser can distinguish "missing kind" from
/// "wrong kind" and produce a targeted error for each. `metadata` is accepted
/// but unread, so it is not modelled.
#[derive(Debug, Deserialize)]
struct ContextFile {
    #[serde(default)]
    kind: Option<String>,
    spec: ContextSpec,
}

/// Context configuration file structure (`spec:` block).
#[derive(Debug, Deserialize)]
struct ContextSpec {
    data_sources: Vec<DataSource>,
}

/// A pipeline reduced to what static validation needs: its name and its SQL.
#[derive(Debug, Clone)]
pub struct ParsedPipeline {
    pub name: String,
    pub sql: String,
}

/// The outcome of [`validate_config`]: whether the config is valid, and every
/// problem found (structural verdict plus per-pipeline problems).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationReport {
    pub ok: bool,
    pub errors: Vec<String>,
}

impl ValidationReport {
    fn from_errors(errors: Vec<String>) -> Self {
        Self {
            ok: errors.is_empty(),
            errors,
        }
    }
}

/// Configuration-related errors.
#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Pipeline file not found: {path}")]
    PipelineFileNotFound { path: PathBuf },

    #[error("Pipeline directory not found: {path}")]
    PipelineDirectoryNotFound { path: PathBuf },

    #[error("No pipeline files found in directory: {path}")]
    NoPipelineFilesInDirectory { path: PathBuf },

    #[error("Context file not found: {path}")]
    ContextFileNotFound { path: PathBuf },

    #[error("Invalid YAML in context file: {error}")]
    InvalidContextYaml { error: String },

    #[error("Invalid YAML in pipeline file: {error}")]
    InvalidPipelineYaml { error: String },

    #[error("Data source file not found: {name} -> {path}")]
    DataSourceFileNotFound { name: String, path: PathBuf },

    #[error("Duplicate data source name: {name}")]
    DuplicateDataSourceName { name: String },

    #[error("Data source registration failed: {name} - {error}")]
    DataSourceRegistrationFailed { name: String, error: String },

    #[error("Invalid schema type: {field} -> {type_name}")]
    InvalidSchemaType { field: String, type_name: String },

    #[error("Missing connection string for data source: {name}")]
    MissingConnectionString { name: String },

    #[error("PostgreSQL connection failed: {name} - {error}")]
    PostgresConnectionFailed { name: String, error: String },

    #[error("MySQL connection failed: {name} - {error}")]
    MySQLConnectionFailed { name: String, error: String },

    #[error("SQLite connection failed: {name} - {error}")]
    SQLiteConnectionFailed { name: String, error: String },

    #[error("SeekDB connection failed: {name} - {error}")]
    SeekDbConnectionFailed { name: String, error: String },

    #[error("S3 path must start with 's3://' prefix: {path}")]
    InvalidS3Path { path: String },

    #[error("Missing required AWS configuration for S3 data source: {name} - missing {field}")]
    MissingAwsConfig { name: String, field: String },

    #[error("S3 object store registration failed: {name} - {error}")]
    S3ObjectStoreRegistrationFailed { name: String, error: String },

    #[error(
        "Data source '{name}' has access_mode 'read_write' but type '{source_type:?}' does not support write operations. Only 'postgres', 'mysql', 'sqlite', 'mongo', 'redis', 'seekdb', and 'dynamodb' sources support read_write mode."
    )]
    UnsupportedWriteMode {
        name: String,
        source_type: DataSourceType,
    },

    #[error(
        "DDL operation not allowed: {operation} on data source '{table_name}'. DDL operations (CREATE, DROP, ALTER, etc.) are not permitted."
    )]
    DdlOperationNotAllowed {
        operation: String,
        table_name: String,
    },

    #[error(
        "Write operation not allowed on data source '{table_name}'. The data source is configured with 'read_only' access mode. Set access_mode to 'read_write' to enable write operations."
    )]
    WriteOperationNotAllowed { table_name: String },

    #[error(
        "Data source '{name}' uses hierarchy_level 'catalog' but also specifies the '{option}' option. In catalog mode use 'allowed_schemas' to filter schemas; 'table' and 'schema' are not allowed."
    )]
    CatalogModeConflictingOptions { name: String, option: String },

    #[error(
        "Data source '{name}' has an empty 'allowed_schemas' option. Either omit it to load all schemas, or provide a non-empty comma-separated list such as \"public,analytics\"."
    )]
    EmptyAllowedSchemas { name: String },

    #[error(
        "Data source '{name}' has an empty 'allowed_tables' option. Either omit it to load all DynamoDB tables, or provide a non-empty comma-separated list such as \"products,orders\"."
    )]
    EmptyAllowedTables { name: String },

    #[error(
        "Data source '{name}' has type 'open_connector' but no 'open_connector' config block. The typed gateway configuration (runtime_token_env, bindings, …) is required."
    )]
    MissingOpenConnectorConfig { name: String },

    #[error(
        "Data source '{name}' sets an 'open_connector' config block but its type is '{source_type}'. The 'open_connector' field is only valid for type 'open_connector'."
    )]
    UnexpectedOpenConnectorConfig {
        name: String,
        source_type: DataSourceType,
    },

    #[error("Data source '{name}' has an invalid 'open_connector' config: {reason}")]
    InvalidOpenConnectorConfig { name: String, reason: String },

    #[error(
        "Data source '{name}' has type 'open_connector' but does not set hierarchy_level to 'catalog'. Open Connector gateways are exposed as DataFusion catalogs; add `hierarchy_level: catalog`."
    )]
    OpenConnectorHierarchyRequired { name: String },

    #[error("Data source '{name}' has a non-UTF8 path: {path:?}")]
    NonUtf8Path { name: String, path: PathBuf },
}

/// Data source types that support catalog (whole-database) registration mode.
const CATALOG_SUPPORTED_SOURCES: &[DataSourceType] = &[
    DataSourceType::Postgres,
    DataSourceType::Mysql,
    DataSourceType::Sqlite,
    DataSourceType::Seekdb,
    DataSourceType::Dynamodb,
    DataSourceType::Clickhouse,
    // OpenConnector is catalog-only; its tables come from typed bindings, so
    // the same catalog-mode guards (no per-table `options`) must apply.
    DataSourceType::OpenConnector,
];

/// Data source types that support read_write access mode.
const WRITABLE_SOURCE_TYPES: &[DataSourceType] = &[
    DataSourceType::Postgres,
    DataSourceType::Mysql,
    DataSourceType::Sqlite,
    DataSourceType::Mongo,
    DataSourceType::Redis,
    DataSourceType::Seekdb,
    DataSourceType::Dynamodb,
];

/// Database source types that require a connection string.
const CONNECTION_STRING_SOURCE_TYPES: &[DataSourceType] = &[
    DataSourceType::Postgres,
    DataSourceType::Mysql,
    DataSourceType::Mongo,
    DataSourceType::Redis,
    DataSourceType::Seekdb,
    DataSourceType::Influxdb,
    DataSourceType::Clickhouse,
    DataSourceType::OpenConnector,
    DataSourceType::Dynamodb,
];

/// Parse a ctx YAML into its declared data sources. Enforces the `kind: context`
/// discriminator so a misfiled pipeline/job is rejected rather than silently
/// read as an empty context. Does no I/O — the caller owns reading the file.
pub fn parse_context(yaml: &str) -> Result<Vec<DataSource>, ConfigError> {
    let context_file: ContextFile =
        serde_yaml::from_str(yaml).map_err(|e| ConfigError::InvalidContextYaml {
            error: e.to_string(),
        })?;

    match context_file.kind.as_deref() {
        Some("context") => {}
        Some(other) => {
            return Err(ConfigError::InvalidContextYaml {
                error: format!("Expected `kind: context`, got `kind: {other}`"),
            });
        }
        None => {
            return Err(ConfigError::InvalidContextYaml {
                error: "Missing `kind: context` at the root of the context file".to_string(),
            });
        }
    }

    Ok(context_file.spec.data_sources)
}

/// Parse a pipeline YAML into its name and SQL — `metadata.name` + `spec.query`,
/// the minimal read the server does before full pipeline loading. Does no I/O.
pub fn parse_pipeline(yaml: &str) -> Result<ParsedPipeline, ConfigError> {
    #[derive(Deserialize)]
    struct PipelineMetadata {
        name: String,
    }
    #[derive(Deserialize)]
    struct MinimalSpec {
        query: String,
    }
    #[derive(Deserialize)]
    struct MinimalPipeline {
        metadata: PipelineMetadata,
        spec: MinimalSpec,
    }

    let pipeline: MinimalPipeline =
        serde_yaml::from_str(yaml).map_err(|e| ConfigError::InvalidPipelineYaml {
            error: e.to_string(),
        })?;

    Ok(ParsedPipeline {
        name: pipeline.metadata.name,
        sql: pipeline.spec.query,
    })
}

/// Validate the credential-free structural invariants of a set of declared data
/// sources: unique names, access-mode/type compatibility, Open Connector config
/// presence, catalog-mode option conflicts, and a connection string for database
/// sources. Contacts nothing — the server layers its I/O-bound checks (S3
/// credential config, connection probes) on top of this.
pub fn validate_source_decls(data_sources: &[DataSource]) -> Result<(), ConfigError> {
    // Unique names.
    let mut names = std::collections::HashSet::new();
    for source in data_sources {
        if !names.insert(&source.name) {
            return Err(ConfigError::DuplicateDataSourceName {
                name: source.name.clone(),
            });
        }
    }

    for source in data_sources {
        // access_mode/type compatibility.
        if source.access_mode.is_read_write()
            && !WRITABLE_SOURCE_TYPES.contains(&source.source_type)
        {
            return Err(ConfigError::UnsupportedWriteMode {
                name: source.name.clone(),
                source_type: source.source_type,
            });
        }

        // Open Connector typed config: required for that type, rejected for every
        // other type. `config.validate()` is pure (no network I/O).
        match (&source.source_type, &source.open_connector) {
            (DataSourceType::OpenConnector, Some(config)) => {
                if source.hierarchy_level != HierarchyLevel::Catalog {
                    return Err(ConfigError::OpenConnectorHierarchyRequired {
                        name: source.name.clone(),
                    });
                }
                config
                    .validate()
                    .map_err(|e| ConfigError::InvalidOpenConnectorConfig {
                        name: source.name.clone(),
                        reason: e.to_string(),
                    })?;
            }
            (DataSourceType::OpenConnector, None) => {
                return Err(ConfigError::MissingOpenConnectorConfig {
                    name: source.name.clone(),
                });
            }
            (_, Some(_)) => {
                return Err(ConfigError::UnexpectedOpenConnectorConfig {
                    name: source.name.clone(),
                    source_type: source.source_type,
                });
            }
            (_, None) => {}
        }

        // Catalog mode must not mix with per-table / per-schema options
        // ("database" is ClickHouse's schema-analog spelling).
        if CATALOG_SUPPORTED_SOURCES.contains(&source.source_type)
            && source.hierarchy_level == HierarchyLevel::Catalog
        {
            for conflicting in &["table", "schema", "database"] {
                if source
                    .options
                    .as_ref()
                    .map(|o| o.contains_key(*conflicting))
                    .unwrap_or(false)
                {
                    return Err(ConfigError::CatalogModeConflictingOptions {
                        name: source.name.clone(),
                        option: conflicting.to_string(),
                    });
                }
            }

            if let Some(value) = source
                .options
                .as_ref()
                .and_then(|o| o.get("allowed_schemas"))
            {
                let has_entry = value.split(',').any(|s| !s.trim().is_empty());
                if !has_entry {
                    return Err(ConfigError::EmptyAllowedSchemas {
                        name: source.name.clone(),
                    });
                }
            }

            if source.source_type == DataSourceType::Dynamodb
                && let Some(value) = source
                    .options
                    .as_ref()
                    .and_then(|o| o.get("allowed_tables"))
            {
                let has_entry = value.split(',').any(|s| !s.trim().is_empty());
                if !has_entry {
                    return Err(ConfigError::EmptyAllowedTables {
                        name: source.name.clone(),
                    });
                }
            }
        }

        // Database sources need a connection string. (File sources' remote/S3
        // configuration is the server's to validate — it needs credentials and
        // object-store setup this credential-free check deliberately avoids.)
        if CONNECTION_STRING_SOURCE_TYPES.contains(&source.source_type)
            && source.connection_string.is_none()
        {
            return Err(ConfigError::MissingConnectionString {
                name: source.name.clone(),
            });
        }
    }

    Ok(())
}

/// Build the per-source access-mode map the SQL validator enforces against.
pub fn validator_config_from_sources(data_sources: &[DataSource]) -> SqlValidatorConfig {
    let mut validator_config = SqlValidatorConfig::new();
    for ds in data_sources {
        validator_config = validator_config.with_table(&ds.name, ds.access_mode);
    }
    validator_config
}

/// Statically validate a ctx and its pipelines with no live-source contact.
///
/// Reports, collecting every problem:
/// - a ctx that does not parse (references then all read as undeclared, the
///   honest verdict for a broken ctx);
/// - a structural source violation (unique names, access modes, Open Connector
///   config, catalog-mode options, connection strings) via
///   [`validate_source_decls`];
/// - a pipeline that does not parse;
/// - DDL, or a write to a `read_only` source, in a pipeline's SQL (via
///   [`validate_sql`]);
/// - a pipeline that references a source the ctx does not declare.
pub fn validate_config(ctx_yaml: &str, pipeline_yamls: &[&str]) -> ValidationReport {
    let mut errors = Vec::new();

    let sources = match parse_context(ctx_yaml) {
        Ok(s) => s,
        Err(e) => {
            errors.push(format!("ctx.yaml: {e}"));
            Vec::new()
        }
    };

    // Structural verdict — the same one the server enforces before loading.
    if let Err(e) = validate_source_decls(&sources) {
        errors.push(format!("ctx.yaml: {e}"));
    }

    let declared: std::collections::HashSet<String> =
        sources.iter().map(|s| s.name.to_lowercase()).collect();
    let validator_config = validator_config_from_sources(&sources);

    for (i, yaml) in pipeline_yamls.iter().enumerate() {
        let pipeline = match parse_pipeline(yaml) {
            Ok(p) => p,
            Err(e) => {
                errors.push(format!("pipeline[{i}]: {e}"));
                continue;
            }
        };
        let label = &pipeline.name;

        // Resolve references first: a SQL parse error surfaces here, and
        // reporting it once (rather than again from `validate_sql`, which shares
        // the same parser) keeps the message set clean.
        let refs = match referenced_sources(&pipeline.sql) {
            Ok(refs) => refs,
            Err(e) => {
                errors.push(format!("pipeline `{label}`: {e}"));
                continue;
            }
        };

        if let Err(e) = validate_sql(&pipeline.sql, &validator_config) {
            errors.push(format!("pipeline `{label}`: {e}"));
        }

        for source in refs {
            if !declared.contains(&source) {
                errors.push(format!(
                    "pipeline `{label}` references source `{source}`, which is not declared in ctx.yaml"
                ));
            }
        }
    }

    ValidationReport::from_errors(errors)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CTX: &str = "\
kind: context
metadata:
  name: test
spec:
  data_sources:
    - name: users
      type: csv
      path: /tmp/users.csv
    - name: events
      type: csv
      path: /tmp/events.csv
";

    fn pipeline(name: &str, sql: &str) -> String {
        // Single-line double-quoted scalar so the SQL is embedded verbatim;
        // test SQL avoids `"` to keep the YAML trivial.
        format!("kind: pipeline\nmetadata:\n  name: {name}\nspec:\n  query: \"{sql}\"\n")
    }

    #[test]
    fn parse_context_reads_declared_sources() {
        let sources = parse_context(CTX).unwrap();
        let names: Vec<&str> = sources.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["users", "events"]);
    }

    #[test]
    fn parse_context_rejects_the_wrong_kind() {
        let err = parse_context("kind: pipeline\nspec:\n  data_sources: []\n").unwrap_err();
        assert!(err.to_string().contains("kind: context"), "{err}");
    }

    #[test]
    fn parse_context_rejects_a_missing_kind() {
        let err = parse_context("spec:\n  data_sources: []\n").unwrap_err();
        assert!(err.to_string().contains("Missing `kind: context`"), "{err}");
    }

    #[test]
    fn well_formed_config_validates_ok() {
        let report = validate_config(
            CTX,
            &[
                &pipeline("p1", "SELECT * FROM users"),
                &pipeline(
                    "p2",
                    "SELECT * FROM events JOIN users ON events.uid = users.id",
                ),
            ],
        );
        assert!(report.ok, "expected ok, errors: {:?}", report.errors);
        assert!(report.errors.is_empty());
    }

    #[test]
    fn a_dangling_source_reference_fails_with_a_descriptive_error() {
        let report = validate_config(CTX, &[&pipeline("p", "SELECT * FROM ghosts")]);
        assert!(!report.ok);
        assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
        let msg = &report.errors[0];
        assert!(
            msg.contains("`p`") && msg.contains("ghosts") && msg.contains("not declared"),
            "{msg}"
        );
    }

    #[test]
    fn ddl_in_a_pipeline_is_rejected() {
        let report = validate_config(CTX, &[&pipeline("p", "DROP TABLE users")]);
        assert!(!report.ok);
        assert!(
            report.errors.iter().any(|e| e.contains("DDL")),
            "{:?}",
            report.errors
        );
    }

    #[test]
    fn a_write_to_a_read_only_source_is_rejected() {
        let report = validate_config(CTX, &[&pipeline("p", "INSERT INTO users (id) VALUES (1)")]);
        assert!(!report.ok);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.contains("read_only") || e.contains("read-only")),
            "{:?}",
            report.errors
        );
    }

    #[test]
    fn duplicate_source_names_are_rejected() {
        let ctx = "\
kind: context
metadata:
  name: t
spec:
  data_sources:
    - name: users
      type: csv
    - name: users
      type: csv
";
        let report = validate_config(ctx, &[]);
        assert!(!report.ok);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.contains("Duplicate") && e.contains("users")),
            "{:?}",
            report.errors
        );
    }

    #[test]
    fn a_database_source_without_a_connection_string_is_rejected() {
        let ctx = "\
kind: context
metadata:
  name: t
spec:
  data_sources:
    - name: db
      type: postgres
";
        let report = validate_config(ctx, &[]);
        assert!(!report.ok);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.contains("connection string") && e.contains("db")),
            "{:?}",
            report.errors
        );
    }

    #[test]
    fn a_broken_ctx_reports_the_ctx_error_and_flags_references_as_undeclared() {
        let report = validate_config(
            "kind: pipeline\nspec:\n  data_sources: []\n",
            &[&pipeline("p", "SELECT * FROM users")],
        );
        assert!(!report.ok);
        assert!(
            report.errors.iter().any(|e| e.contains("ctx.yaml")),
            "{:?}",
            report.errors
        );
        assert!(
            report.errors.iter().any(|e| e.contains("users")),
            "{:?}",
            report.errors
        );
    }

    #[test]
    fn an_unparseable_pipeline_reports_a_parse_error() {
        let report = validate_config(CTX, &[&pipeline("p", "SELEKT * FROM users")]);
        assert!(!report.ok);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.to_lowercase().contains("parse")),
            "{:?}",
            report.errors
        );
    }
}
