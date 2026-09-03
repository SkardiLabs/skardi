use anyhow::{Context, Result};
use clap::Parser;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use skardi::jobs::JobDefinition;
use skardi::pipeline::pipeline::{Pipeline, StandardPipeline};
use skardi::sources::providers::clickhouse::register_clickhouse_tables;
use skardi::sources::providers::dynamodb::register_dynamodb_tables;
use skardi::sources::providers::graph::register_graph_tables;
use skardi::sources::providers::iceberg::register_iceberg_table;
use skardi::sources::providers::influxdb::register_influxdb_tables;
use skardi::sources::providers::lance::register_lance_table;
use skardi::sources::providers::mongo::register_mongo_tables;
use skardi::sources::providers::mysql::register_mysql_tables;
#[cfg(feature = "obsidian")]
use skardi::sources::providers::obsidian::register_obsidian_tables;
use skardi::sources::providers::open_connector::{
    OpenConnectorConfig, register_open_connector_tables,
};
use skardi::sources::providers::redis::datasource::register_redis_tables;
#[cfg(feature = "rss")]
use skardi::sources::providers::rss::register_rss_tables;
use skardi::sources::providers::seekdb::register_seekdb_tables;
use skardi::sources::providers::sqlite::register_sqlite_tables;
use skardi::sources::providers::sqlx::postgres::register_postgres_tables;
use skardi::sources::sql_validator::{AdhocSqlPolicy, SqlValidatorConfig, validate_sql};
use skardi::util::json_getters::register_json_getter_udfs;
use skardi::util::json_pack::register_json_pack_udf;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;

use crate::OptimizerRegistry;
use crate::remote_storage::{RemoteStorage, S3Storage};
use crate::semantics::{SemanticsRegistry, resolve_semantics_source};
pub use skardi::sources::AccessMode;
pub use skardi::sources::DataSourceType;
pub use skardi::sources::HierarchyLevel;

/// CLI arguments for the Skardi server
#[derive(Parser, Debug, Default)]
#[command(name = "skardi-server")]
#[command(about = "Skardi Online Serving Pipeline Server")]
pub struct CliArgs {
    /// Path to pipeline YAML file or directory containing pipeline files
    /// If a directory is provided, all .yaml and .yml files in it will be loaded as pipelines
    #[arg(
        long = "pipeline",
        help = "Path to pipeline YAML file or directory containing pipeline files"
    )]
    pub pipeline_path: Option<PathBuf>,

    /// Path to job YAML file or directory containing job files
    /// (files must have `kind: job` at the root). If a directory is
    /// provided, every .yaml / .yml file in it is scanned; files that
    /// are not `kind: job` are ignored.
    #[arg(
        long = "jobs",
        help = "Path to job YAML file or directory containing job files"
    )]
    pub jobs_path: Option<PathBuf>,

    /// Path to the SQLite job run ledger. Defaults to
    /// `~/.skardi/jobs.db`. Ignored when --jobs is not provided.
    #[arg(
        long = "jobs-db",
        help = "Path to the SQLite job run ledger (default: ~/.skardi/jobs.db)"
    )]
    pub jobs_db_path: Option<PathBuf>,

    /// Path to context YAML configuration file (optional)
    #[arg(
        long = "ctx",
        help = "Path to context YAML configuration file (optional)"
    )]
    pub ctx_file: Option<PathBuf>,

    /// Path to a `kind: semantics` YAML file or directory of them.
    /// Files in a directory whose root `kind:` is not `semantics` are
    /// silently skipped, mirroring `--jobs`.
    #[arg(
        long = "semantics",
        help = "Path to semantics YAML file or directory containing semantics overlays"
    )]
    pub semantics_path: Option<PathBuf>,

    /// Server port number
    #[arg(long, default_value = "8080", help = "Server port number")]
    pub port: u16,

    /// Path to the SQLite audit ledger for ad-hoc `/query` statements. When
    /// unset (the default), raw SQL is never persisted and never written to
    /// logs or traces. The ledger records raw SQL (which may embed
    /// secrets/PII); it is created owner-only, and a failure to open it is
    /// fatal rather than a silent downgrade. See [`crate::query_audit`].
    ///
    /// The same ledger can live in Postgres instead: set
    /// `SKARDI_QUERY_AUDIT_PG_DSN` (an env var, never a flag — the DSN
    /// carries a credential). Mutually exclusive with this flag; the server
    /// refuses to start with both.
    #[arg(
        long = "query-audit-db",
        help = "Record /query statements in this SQLite audit ledger (off by default; created 0600). \
                For a Postgres ledger set SKARDI_QUERY_AUDIT_PG_DSN instead"
    )]
    pub query_audit_db: Option<PathBuf>,

    /// Delete audit records older than this many days, at startup and hourly
    /// thereafter. Unset means keep everything. Applies to whichever audit
    /// backend is selected; ignored when neither is.
    #[arg(
        long = "query-audit-retention-days",
        help = "Prune /query audit records older than N days (default: keep forever)"
    )]
    pub query_audit_retention_days: Option<u32>,
}

/// Main server configuration containing pipelines and data sources
#[derive(Debug)]
pub struct ServerConfig {
    /// Loaded pipeline definitions keyed by name (can be registered later via API)
    pub pipelines: HashMap<String, StandardPipeline>,
    /// Loaded job definitions keyed by name. `kind: job` YAMLs populate
    /// this map; plain pipeline YAMLs do not.
    pub jobs: HashMap<String, JobDefinition>,
    /// Data sources to register with DataFusion
    pub data_sources: Vec<DataSource>,
    /// Natural-language descriptions overlaid on top of the catalog,
    /// merged from all `kind: semantics` files plus any inline `description`
    /// fields on `data_sources[]` in the ctx (the latter is the fallback).
    pub semantics: SemanticsRegistry,
    /// CLI arguments
    pub args: CliArgs,
}

/// Data source configuration for context loading
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
    /// Typed RSS/Atom subscription configuration. Required when `type` is
    /// `rss`, rejected for every other type: a list of feed subscriptions
    /// does not fit the flat `options` map.
    ///
    /// Not behind `#[cfg(feature = "rss")]`: `RssConfig` compiles without the
    /// feature (`sources/providers/rss/config.rs:1-10`), so a featureless
    /// build still parses the block and fails at registration with a message
    /// naming the missing feature rather than an opaque serde error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rss: Option<skardi::sources::providers::rss::RssConfig>,
    /// Typed graph-engine configuration. Required when `type` is `graph`,
    /// rejected for every other type: backend name, graph name, credential
    /// env-var names, and the `views:` list do not fit the flat `options`
    /// map.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<skardi::sources::providers::graph::config::GraphConfig>,
}

/// Top-level envelope for context YAML files:
/// `{ kind: context, metadata: {...}, spec: { data_sources: [...] } }`.
///
/// `kind` is an `Option` so the loader can distinguish "missing kind" from
/// "wrong kind" and produce a targeted error for each. `metadata` is
/// required — making it mandatory means a missing or typo'd key (e.g.
/// `metdata:`) surfaces at parse time rather than being silently dropped.
/// The value is kept as an opaque `serde_yaml::Value` because nothing at
/// runtime reads inside it.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct ContextFile {
    #[serde(default)]
    kind: Option<String>,
    metadata: serde_yaml::Value,
    spec: ContextSpec,
}

/// Context configuration file structure (`spec:` block).
#[derive(Debug, Deserialize)]
struct ContextSpec {
    data_sources: Vec<DataSource>,
}

/// Configuration-related errors
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

    #[error("Data source file not found: {name} -> {path}")]
    DataSourceFileNotFound { name: String, path: PathBuf },

    #[error("Duplicate data source name: {name}")]
    DuplicateDataSourceName { name: String },

    #[error(
        "Data source '{name}' uses a reserved name: a catalog-mode source registers a DataFusion catalog under its own name, and register_catalog replaces the built-in '{name}' catalog (and every table in it) unconditionally. Choose another name."
    )]
    ReservedCatalogSourceName { name: String },

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

    #[error(
        "Data source '{name}' has type 'rss' but no 'rss' config block. The typed subscription configuration (a `feeds` list or an `opml` path) is required."
    )]
    MissingRssConfig { name: String },

    #[error(
        "Data source '{name}' sets an 'rss' config block but its type is '{source_type}'. The 'rss' field is only valid for type 'rss'."
    )]
    UnexpectedRssConfig {
        name: String,
        source_type: DataSourceType,
    },

    #[error("Data source '{name}' has an invalid 'rss' config: {reason}")]
    InvalidRssConfig { name: String, reason: String },

    #[error(
        "Data source '{name}' has type 'rss' but does not set hierarchy_level to 'catalog'. RSS sources are exposed as DataFusion catalogs ('{name}.main.feeds' and '{name}.main.items'); add `hierarchy_level: catalog`."
    )]
    RssHierarchyRequired { name: String },

    #[error(
        "Data source '{name}' has type 'graph' but no 'graph' config block. The typed graph configuration (backend, graph_name, views, …) is required."
    )]
    MissingGraphConfig { name: String },

    #[error(
        "Data source '{name}' sets a 'graph' config block but its type is '{source_type}'. The 'graph' field is only valid for type 'graph'."
    )]
    UnexpectedGraphConfig {
        name: String,
        source_type: DataSourceType,
    },

    #[error("Data source '{name}' has an invalid 'graph' config: {reason}")]
    InvalidGraphConfig { name: String, reason: String },

    #[error(
        "Data source '{name}' has type 'graph' but does not set hierarchy_level to 'catalog'. Graph views are registered as catalog tables ('{name}.main.<view>'); add `hierarchy_level: catalog`."
    )]
    GraphHierarchyRequired { name: String },

    #[error("Data source '{name}' has a non-UTF8 path: {path:?}")]
    NonUtf8Path { name: String, path: PathBuf },
}

// ============================================================================
// FUNCTION SIGNATURES - Implementation to be added later
// ============================================================================

/// Resolve pipeline files from a path that can be either a single file or a directory
///
/// If the path is a file, returns a vector containing just that file.
/// If the path is a directory, returns all .yaml and .yml files in that directory.
/// If the path is None, returns an empty vector.
fn resolve_pipeline_files(path: Option<&PathBuf>) -> Result<Vec<PathBuf>> {
    let Some(path) = path else {
        tracing::info!("No pipeline path specified");
        return Ok(Vec::new());
    };

    if !path.exists() {
        // Check if it looks like a file or directory based on extension
        if path.extension().is_some() {
            return Err(ConfigError::PipelineFileNotFound { path: path.clone() }.into());
        } else {
            return Err(ConfigError::PipelineDirectoryNotFound { path: path.clone() }.into());
        }
    }

    if path.is_file() {
        tracing::info!("Pipeline path is a single file: {:?}", path);
        return Ok(vec![path.clone()]);
    }

    if path.is_dir() {
        tracing::info!("Pipeline path is a directory: {:?}", path);
        let mut pipeline_files = Vec::new();

        for entry in std::fs::read_dir(path)
            .with_context(|| format!("Failed to read pipeline directory: {:?}", path))?
        {
            let entry = entry.with_context(|| "Failed to read directory entry")?;
            let file_path = entry.path();

            if file_path.is_file()
                && let Some(ext) = file_path.extension()
            {
                let ext = ext.to_string_lossy().to_lowercase();
                if ext == "yaml" || ext == "yml" {
                    tracing::debug!("Found pipeline file: {:?}", file_path);
                    pipeline_files.push(file_path);
                }
            }
        }

        // Sort for consistent ordering
        pipeline_files.sort();

        if pipeline_files.is_empty() {
            return Err(ConfigError::NoPipelineFilesInDirectory { path: path.clone() }.into());
        }

        tracing::info!(
            "Found {} pipeline file(s) in directory",
            pipeline_files.len()
        );
        return Ok(pipeline_files);
    }

    // Path exists but is neither file nor directory (e.g., symlink to nothing)
    Err(ConfigError::PipelineFileNotFound { path: path.clone() }.into())
}

/// Load complete server configuration from CLI arguments.
pub async fn load_server_config(args: CliArgs) -> Result<ServerConfig> {
    tracing::info!("Loading server configuration");
    tracing::debug!("Pipeline path: {:?}", args.pipeline_path);
    tracing::debug!("Context file: {:?}", args.ctx_file);

    // Load context configuration first (optional)
    let data_sources = if let Some(ref ctx_file) = args.ctx_file {
        load_context_config(ctx_file)
            .with_context(|| format!("Failed to load context from {:?}", ctx_file))?
    } else {
        tracing::info!("No context file specified, using empty data sources");
        Vec::new()
    };

    // Create optimizer registry before SessionState
    let optimizer_registry = Arc::new(OptimizerRegistry::new());

    // Create federation-enabled SessionState
    let state = datafusion_federation::default_session_state();

    // Get all physical optimizer rules from the registry based on data sources
    let additional_optimizers = optimizer_registry.get_physical_optimizer_rules(&data_sources);

    // Rebuild SessionState with additional physical optimizers if any
    let state = if !additional_optimizers.is_empty() {
        tracing::info!(
            "Adding {} physical optimizer(s) to SessionState",
            additional_optimizers.len()
        );

        // Get current physical optimizers and add our additional ones
        let mut physical_optimizer_rules = state.physical_optimizers().to_vec();
        physical_optimizer_rules.extend(additional_optimizers);

        // Rebuild SessionState with the new optimizer rules
        datafusion::execution::SessionStateBuilder::new_from_existing(state)
            .with_physical_optimizer_rules(physical_optimizer_rules)
            .build()
    } else {
        state
    };

    let mut session_ctx = SessionContext::new_with_state(state);

    // Register data sources with optimizer registry support. This is
    // the PLANNING context: it validates pipeline SQL and is discarded
    // for execution purposes, so expensive-startup providers (graph)
    // register lazily from declared schemas (see RegistrationPass).
    register_data_sources_for_pass(
        &mut session_ctx,
        &data_sources,
        &optimizer_registry,
        RegistrationPass::Planning,
    )
    .await
    .with_context(|| "Failed to register data sources")?;

    // Register UDFs
    optimizer_registry
        .register_udfs(&mut session_ctx, &data_sources)
        .with_context(|| "Failed to register UDFs")?;

    // Register onnx_predict UDF (lazy — models loaded on first call from inline path)
    #[cfg(feature = "onnx")]
    register_onnx_predict_udf(&mut session_ctx);

    // Register remote_embed UDF (OpenAI, Gemini, Voyage, Mistral)
    #[cfg(feature = "remote-embed")]
    register_remote_embed_udf(&mut session_ctx);

    // Register llm_extract UDF (Anthropic structured extraction)
    #[cfg(feature = "llm-extract")]
    register_llm_extract_udf(&mut session_ctx);
    // Register gguf UDF (lazy — GGUF models loaded on first call from inline path)
    #[cfg(feature = "gguf")]
    register_gguf_udf(&mut session_ctx);
    // Register candle UDF (lazy — models loaded on first call from inline path)
    #[cfg(feature = "candle")]
    register_candle_udf(&mut session_ctx);
    // Register chunk UDF (text-splitter wrapper for inline ingestion)
    #[cfg(feature = "chunking")]
    register_chunk_udf(&mut session_ctx);
    // Register json_pack UDF (SQL-side JSON encoding; the etl generator's
    // metadata/frontmatter serialization boundary)
    register_json_pack_udf(&mut session_ctx);
    // The json_get family is the extraction tool for every JSON column,
    // graph node/relationship properties included; UDFs only, never the
    // `->` operator rewrite — see util::json_getters.
    register_json_getter_udfs(&session_ctx)?;

    // This auth layer is used only for SQL planning and is discarded after current function returns.
    // The live auth layer is built separately in setup_app_state.
    let planning_auth =
        crate::auth::layer::AuthLayer::build(&crate::auth::mode::AuthMode::from_env())
            .await
            .with_context(|| "Failed to build auth layer for pipeline planning")?;

    if let Some(auth) = planning_auth.as_better_auth() {
        crate::auth::bridge::register_auth_tables(&mut session_ctx, auth.clone())
            .with_context(|| "Failed to register auth tables for pipeline planning")?;
    }

    let ctx = Arc::new(session_ctx);

    // Resolve pipeline files from path (can be file or directory)
    let pipeline_files = resolve_pipeline_files(args.pipeline_path.as_ref())
        .with_context(|| "Failed to resolve pipeline files")?;

    for pipeline_file in &pipeline_files {
        let (pipeline_name, sql) = extract_pipeline_sql(pipeline_file)
            .with_context(|| format!("Failed to load pipeline from {:?}", pipeline_file))?;

        validate_pipeline_sql(&pipeline_name, &sql, &data_sources)?;
    }

    // Load pipeline configurations with the populated SessionContext
    let mut pipelines: HashMap<String, StandardPipeline> = HashMap::new();

    for pipeline_file in &pipeline_files {
        let loaded_pipeline = load_pipeline_config(pipeline_file, ctx.clone())
            .await
            .with_context(|| format!("Failed to load pipeline from {:?}", pipeline_file))?;

        let pipeline_name = loaded_pipeline.name().to_string();
        tracing::info!("Pipeline loaded successfully: {}", pipeline_name);

        // Check for duplicate pipeline names
        if pipelines.contains_key(&pipeline_name) {
            return Err(anyhow::anyhow!(
                "Duplicate pipeline name '{}' found. Each pipeline must have a unique name.",
                pipeline_name
            ));
        }

        pipelines.insert(pipeline_name, loaded_pipeline);
    }

    if pipelines.is_empty() {
        tracing::info!("No pipeline files specified, pipelines map is empty");
    } else {
        tracing::info!(
            "Loaded {} pipeline(s): {:?}",
            pipelines.len(),
            pipelines.keys().collect::<Vec<_>>()
        );
    }

    // Load jobs (if a --jobs path was specified). The jobs directory can
    // also contain plain pipelines — those are silently skipped here and
    // belong in --pipeline instead. `resolve_pipeline_files` is reused
    // because the directory discovery rules are identical.
    let job_files = resolve_pipeline_files(args.jobs_path.as_ref())
        .with_context(|| "Failed to resolve job files")?;
    let mut jobs: HashMap<String, JobDefinition> = HashMap::new();
    for job_file in &job_files {
        match JobDefinition::load_from_file(job_file, ctx.clone()).await {
            Ok(Some(job)) => {
                let name = job.name().to_string();
                if pipelines.contains_key(&name) {
                    return Err(anyhow::anyhow!(
                        "Job name '{name}' collides with an existing pipeline name; \
                         both primitives share the same namespace in this server"
                    ));
                }
                if jobs.contains_key(&name) {
                    return Err(anyhow::anyhow!(
                        "Duplicate job name '{name}' found. Each job must have a unique name."
                    ));
                }
                tracing::info!(
                    "Job loaded: {} → destination `{}`",
                    name,
                    job.destination.table
                );
                jobs.insert(name, job);
            }
            Ok(None) => {
                tracing::debug!(
                    "Skipping {:?}: no `kind: job` at root (plain pipeline)",
                    job_file
                );
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Failed to load job from {:?}: {}",
                    job_file,
                    e
                ));
            }
        }
    }

    if let Some(jobs_path) = args.jobs_path.as_ref()
        && !job_files.is_empty()
        && jobs.is_empty()
    {
        tracing::warn!(
            "--jobs {:?} scanned {} YAML file(s) but none had `kind: job` at the root; \
             /jobs/* endpoints will return 503 until at least one job definition loads. \
             Did you forget the `kind: job` discriminator?",
            jobs_path,
            job_files.len(),
        );
    }

    // Load semantics overlays. The path is either:
    //   1. `--semantics <path>` (explicit override), or
    //   2. auto-discovered next to the ctx file (`<ctx_dir>/semantics/`
    //      directory or `<ctx_dir>/semantics.yaml` single file).
    // The registry is then seeded with the ctx-inline `description` fields
    // as a fallback for any source not covered by the overlay.
    let ctx_dir = args.ctx_file.as_deref().and_then(Path::parent);
    let semantics_path = resolve_semantics_source(ctx_dir, args.semantics_path.as_deref())
        .with_context(|| "Failed to resolve semantics source")?;
    let ctx_descriptions: Vec<(String, Option<String>)> = data_sources
        .iter()
        .map(|ds| (ds.name.clone(), ds.description.clone()))
        .collect();
    let semantics = SemanticsRegistry::build(semantics_path.as_deref(), &ctx_descriptions)
        .with_context(|| "Failed to load semantics")?;

    tracing::info!(
        "Configuration loaded successfully: pipelines={}, jobs={}, data_sources={}",
        pipelines.len(),
        jobs.len(),
        data_sources.len()
    );

    Ok(ServerConfig {
        pipelines,
        jobs,
        data_sources,
        semantics,
        args,
    })
}

/// Extract SQL query from pipeline file for early validation
/// This reads just the query field without full pipeline loading
fn extract_pipeline_sql(path: &Path) -> Result<(String, String)> {
    use serde::Deserialize;

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

    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read pipeline file: {:?}", path))?;

    let pipeline: MinimalPipeline = serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse pipeline YAML: {:?}", path))?;

    Ok((pipeline.metadata.name, pipeline.spec.query))
}

/// Load pipeline configuration from YAML file
async fn load_pipeline_config(path: &Path, ctx: Arc<SessionContext>) -> Result<StandardPipeline> {
    tracing::debug!("Loading pipeline from: {:?}", path);

    if !path.exists() {
        return Err(ConfigError::PipelineFileNotFound {
            path: path.to_path_buf(),
        }
        .into());
    }

    // Use existing StandardPipeline infrastructure with provided context
    StandardPipeline::load_from_file(path, ctx)
        .await
        .map_err(|e| anyhow::anyhow!("Pipeline loading failed: {}", e))
}

/// Register the onnx_predict UDF with the session context.
///
/// The UDF loads models lazily from file paths provided inline in SQL:
///   onnx_predict('path/to/model.onnx', input1, input2, ...)
///
/// No pre-configuration needed — ORT runtime and models are initialized on first call.
#[cfg(feature = "onnx")]
pub fn register_onnx_predict_udf(ctx: &mut SessionContext) {
    let registry = Arc::new(skardi::model::OnnxModelRegistry::new());
    registry.register_onnx_predict_udf(ctx);
}

/// Register the `remote_embed` UDF for calling remote embedding APIs.
///
/// The UDF dispatches to OpenAI, Gemini, Voyage, or Mistral based on the
/// provider name passed as the first SQL argument:
///   remote_embed('openai', 'text-embedding-3-small', text_col)
#[cfg(feature = "remote-embed")]
pub fn register_remote_embed_udf(ctx: &mut SessionContext) {
    let registry = Arc::new(skardi::model::RemoteEmbedRegistry::new());
    registry.register_remote_embed_udf(ctx);
}

/// Register the `llm_extract` scalar UDF for structured entity extraction.
///
/// Builds an Anthropic-backed registry from the environment
/// (`ANTHROPIC_API_KEY`, `LLM_EXTRACT_MODEL`, `LLM_EXTRACT_THRESHOLD`):
///   SELECT UNNEST(llm_extract(text_col, image_ref_col, '{json schema}'))
#[cfg(feature = "llm-extract")]
pub fn register_llm_extract_udf(ctx: &mut SessionContext) {
    let registry = Arc::new(skardi::model::LlmExtractRegistry::from_env());
    registry.register(ctx);
}
/// Register the gguf UDF with the session context.
///
/// The UDF loads GGUF models lazily from directory paths provided inline in SQL:
///   gguf('path/to/model_dir', text_col)
///
/// No pre-configuration needed — llama.cpp backend and models are initialized on first call.
#[cfg(feature = "gguf")]
pub fn register_gguf_udf(ctx: &mut SessionContext) {
    let registry = Arc::new(skardi::model::GgufModelRegistry::new());
    registry.register_gguf_udf(ctx);
}
/// Register the `candle` UDF with the session context.
///
/// The UDF loads BERT-style SafeTensors models lazily from file paths provided
/// inline in SQL:
///   candle('path/to/model.safetensors', text_col)
///
/// `config.json` and `tokenizer.json` must live alongside the weights file.
/// Models are loaded and cached on first call.
#[cfg(feature = "candle")]
pub fn register_candle_udf(ctx: &mut SessionContext) {
    let registry = Arc::new(skardi::model::CandleModelRegistry::new());
    registry.register_candle_udf(ctx);
}

/// Register the `chunk` UDF for inline text chunking.
///
/// SQL:
///   chunk('character', text_col, 1000)
///   chunk('character', text_col, 1000, 200)
///   chunk('markdown',  text_col, 1000, 200)
///
/// Returns `List<Utf8>`; combine with `UNNEST(...)` to expand into rows.
#[cfg(feature = "chunking")]
pub fn register_chunk_udf(ctx: &mut SessionContext) {
    let registry = Arc::new(skardi::model::ChunkingRegistry::new());
    registry.register_chunk_udf(ctx);
}

/// Load context configuration from YAML file
fn load_context_config(path: &Path) -> Result<Vec<DataSource>> {
    tracing::debug!("Loading context from: {:?}", path);

    if !path.exists() {
        return Err(ConfigError::ContextFileNotFound {
            path: path.to_path_buf(),
        }
        .into());
    }

    // Read and parse YAML
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read context file: {:?}", path))?;

    let context_file: ContextFile =
        serde_yaml::from_str(&content).map_err(|e| ConfigError::InvalidContextYaml {
            error: e.to_string(),
        })?;

    // The envelope is uniform with pipelines/jobs/aliases: `kind: context`,
    // then `metadata:` and `spec:`. The kind check is strict — unknown or
    // missing values are rejected so a misfiled pipeline/job does not get
    // silently partially-loaded as a context.
    match context_file.kind.as_deref() {
        Some("context") => {}
        Some(other) => {
            return Err(ConfigError::InvalidContextYaml {
                error: format!("Expected `kind: context`, got `kind: {other}`"),
            }
            .into());
        }
        None => {
            return Err(ConfigError::InvalidContextYaml {
                error: "Missing `kind: context` at the root of the context file".to_string(),
            }
            .into());
        }
    }

    // Validate data sources
    validate_data_sources(&context_file.spec.data_sources)?;

    tracing::info!(
        "Loaded {} data sources from context",
        context_file.spec.data_sources.len()
    );

    Ok(context_file.spec.data_sources)
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
    // Rss is catalog-only too: one source is one catalog exposing the fixed
    // pair `main.feeds`/`main.items`, so per-table `options` must be rejected
    // by the same guard.
    DataSourceType::Rss,
    // Obsidian is catalog-only too: one vault is one catalog exposing the
    // fixed trio `main.notes`/`main.links`/`main.tags`, so per-table
    // `options` must be rejected by the same guard.
    DataSourceType::Obsidian,
    // Graph is catalog-only as well: one source is one catalog exposing the
    // declared views as `main.<view>`, with per-table `options` rejected by
    // the same guard.
    DataSourceType::Graph,
];

/// Data source types that support read_write access mode
const WRITABLE_SOURCE_TYPES: &[DataSourceType] = &[
    DataSourceType::Postgres,
    DataSourceType::Mysql,
    DataSourceType::Sqlite,
    DataSourceType::Mongo,
    DataSourceType::Redis,
    DataSourceType::Seekdb,
    DataSourceType::Dynamodb,
];

/// Validate data source configurations
fn validate_data_sources(data_sources: &[DataSource]) -> Result<()> {
    tracing::debug!("Validating {} data sources", data_sources.len());

    // Check for duplicate names
    let mut names = std::collections::HashSet::new();
    for source in data_sources {
        if !names.insert(&source.name) {
            return Err(ConfigError::DuplicateDataSourceName {
                name: source.name.clone(),
            }
            .into());
        }
    }

    // Initialize S3 storage handler for remote validation
    let s3_storage = S3Storage::new();

    // Validate each data source based on its path type
    for source in data_sources {
        // Validate access_mode compatibility
        if source.access_mode.is_read_write()
            && !WRITABLE_SOURCE_TYPES.contains(&source.source_type)
        {
            return Err(ConfigError::UnsupportedWriteMode {
                name: source.name.clone(),
                source_type: source.source_type,
            }
            .into());
        }

        // Open Connector typed config: required for that type, rejected for
        // every other type. `config.validate()` is pure (no network I/O), so
        // misconfigurations surface here at config load, not at first query.
        match (&source.source_type, &source.open_connector) {
            (DataSourceType::OpenConnector, Some(config)) => {
                // Hierarchy defaults to Table, so a minimal config would
                // otherwise pass validation and fail at registration with a
                // Debug-wrapped CatalogHierarchyRequired — catch it here.
                if source.hierarchy_level != HierarchyLevel::Catalog {
                    return Err(ConfigError::OpenConnectorHierarchyRequired {
                        name: source.name.clone(),
                    }
                    .into());
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
                }
                .into());
            }
            (_, Some(_)) => {
                return Err(ConfigError::UnexpectedOpenConnectorConfig {
                    name: source.name.clone(),
                    source_type: source.source_type,
                }
                .into());
            }
            (_, None) => {}
        }

        // The RSS typed config, on the same terms as `open_connector` above:
        // required for `type: rss`, rejected for every other type. A parallel
        // match rather than a merged one, so each block's "wrong type" case
        // still names the block the operator has to remove.
        // `RssConfig::validate()` is pure — no file reads, no network
        // (`sources/providers/rss/config.rs:166-171`) — so it is safe on this
        // path; an `opml:` path is read later, at registration.
        match (&source.source_type, &source.rss) {
            (DataSourceType::Rss, Some(config)) => {
                // Hierarchy defaults to Table, so without this a minimal
                // config would pass validation and fail at registration with
                // the provider's wrapped CatalogHierarchyRequired.
                if source.hierarchy_level != HierarchyLevel::Catalog {
                    return Err(ConfigError::RssHierarchyRequired {
                        name: source.name.clone(),
                    }
                    .into());
                }
                config
                    .validate()
                    .map_err(|e| ConfigError::InvalidRssConfig {
                        name: source.name.clone(),
                        reason: e.to_string(),
                    })?;
            }
            (DataSourceType::Rss, None) => {
                return Err(ConfigError::MissingRssConfig {
                    name: source.name.clone(),
                }
                .into());
            }
            (_, Some(_)) => {
                return Err(ConfigError::UnexpectedRssConfig {
                    name: source.name.clone(),
                    source_type: source.source_type,
                }
                .into());
            }
            (_, None) => {}
        }

        // The graph typed config, on the same terms as `rss` above:
        // required for `type: graph`, rejected for every other type.
        // `GraphConfig::validate()` is pure (no network I/O — a reachable
        // backend's view validation happens at registration), so it is
        // safe on this path; it needs the connection string for the scheme
        // allowlist and the embedded-credential rejection.
        match (&source.source_type, &source.graph) {
            (DataSourceType::Graph, Some(config)) => {
                // Hierarchy defaults to Table, so without this a minimal
                // config would pass validation and fail at registration
                // with the provider's wrapped hierarchy error.
                if source.hierarchy_level != HierarchyLevel::Catalog {
                    return Err(ConfigError::GraphHierarchyRequired {
                        name: source.name.clone(),
                    }
                    .into());
                }
                let connection_string = source.connection_string.as_ref().ok_or_else(|| {
                    ConfigError::MissingConnectionString {
                        name: source.name.clone(),
                    }
                })?;
                config
                    .validate(&source.name, connection_string)
                    .map_err(|e| ConfigError::InvalidGraphConfig {
                        name: source.name.clone(),
                        reason: e.to_string(),
                    })?;
            }
            (DataSourceType::Graph, None) => {
                return Err(ConfigError::MissingGraphConfig {
                    name: source.name.clone(),
                }
                .into());
            }
            (_, Some(_)) => {
                return Err(ConfigError::UnexpectedGraphConfig {
                    name: source.name.clone(),
                    source_type: source.source_type,
                }
                .into());
            }
            (_, None) => {}
        }

        // Catalog mode must not mix with per-table / per-schema options
        // ("database" is ClickHouse's schema-analog spelling)
        if CATALOG_SUPPORTED_SOURCES.contains(&source.source_type)
            && source.hierarchy_level == HierarchyLevel::Catalog
        {
            // A catalog-mode source's name becomes a DataFusion CATALOG
            // name, and register_catalog replaces unconditionally — a
            // source named after a built-in would silently swallow the
            // default catalog and every table in it. This guards EVERY
            // catalog-registering type (rss, open_connector, clickhouse …),
            // not just graph: the hazard is the catalog path, not any one
            // provider. (Engine-API paths that register no catalog — e.g.
            // graph's register_graph_source — are deliberately exempt.)
            if matches!(source.name.as_str(), "datafusion" | "information_schema") {
                return Err(ConfigError::ReservedCatalogSourceName {
                    name: source.name.clone(),
                }
                .into());
            }
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
                    }
                    .into());
                }
            }

            // allowed_schemas, if present, must not be an empty string
            if let Some(value) = source
                .options
                .as_ref()
                .and_then(|o| o.get("allowed_schemas"))
            {
                let has_entry = value.split(',').any(|s| !s.trim().is_empty());
                if !has_entry {
                    return Err(ConfigError::EmptyAllowedSchemas {
                        name: source.name.clone(),
                    }
                    .into());
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
                    }
                    .into());
                }
            }
        }

        match (&source.source_type, s3_storage.is_remote_path(&source.path)) {
            (DataSourceType::Csv | DataSourceType::Parquet | DataSourceType::Lance, true) => {
                // Validate S3 configuration for S3 paths
                s3_storage.validate_configuration(source)?;
            }
            (
                DataSourceType::Postgres
                | DataSourceType::Mysql
                | DataSourceType::Mongo
                | DataSourceType::Redis
                | DataSourceType::Seekdb
                | DataSourceType::Influxdb
                | DataSourceType::Clickhouse
                | DataSourceType::OpenConnector
                | DataSourceType::Dynamodb,
                false,
            ) => {
                // For database connections, ensure connection string is provided
                if source.connection_string.is_none() {
                    return Err(ConfigError::MissingConnectionString {
                        name: source.name.clone(),
                    }
                    .into());
                }
            }
            (DataSourceType::Iceberg, _) => {
                // Validation happened during data source registration
            }
            _ => {
                // Other combinations are valid without additional checks
            }
        }

        let location_type = if s3_storage.is_remote_path(&source.path) {
            "remote_s3"
        } else {
            "local"
        };
        let access_mode_str = if source.access_mode.is_read_write() {
            "read_write"
        } else {
            "read_only"
        };
        tracing::debug!(
            "✓ Validated data source: {} (type: {:?}, location: {}, access: {})",
            source.name,
            source.source_type,
            location_type,
            access_mode_str
        );
    }

    tracing::info!(
        "Validated {} data sources successfully ✅",
        data_sources.len()
    );
    Ok(())
}

/// Validate schema type mappings
#[allow(dead_code)]
fn validate_schema_types(_schema: &HashMap<String, String>) -> Result<()> {
    // TODO: Add schema type validation later - for now assume schema is valid
    tracing::debug!("Skipping schema type validation for MVP");
    Ok(())
}

/// Build the access-mode map for every data source. Shared by both the
/// trusted pipeline-load path and the untrusted `/query` policy below.
pub fn validator_config_from_sources(data_sources: &[DataSource]) -> SqlValidatorConfig {
    let mut validator_config = SqlValidatorConfig::new();
    for ds in data_sources {
        validator_config = validator_config.with_table(&ds.name, ds.access_mode);
    }
    validator_config
}

/// Build the statement policy for the untrusted ad-hoc `/query` endpoint:
/// the access-mode map plus the reserved [`AUTH_SCHEMA`] denial (auth.users /
/// auth.sessions register on the same `SessionContext` and hold live bearer
/// tokens, so ad-hoc SQL must never reach them). The denial is scoped to this
/// policy, so operator-authored pipeline SQL may still read auth tables.
///
/// Callers snapshot this once at startup into `AppState`. That is correct only
/// as long as nothing mutates a source's `access_mode` at runtime — there is
/// no such writer today. If one is ever added, it must rebuild this policy (or
/// the snapshot will serve a stale, potentially more-permissive gate).
pub fn adhoc_policy_from_sources(data_sources: &[DataSource]) -> AdhocSqlPolicy {
    AdhocSqlPolicy::new(validator_config_from_sources(data_sources))
        .with_denied_schema(crate::auth::bridge::AUTH_SCHEMA)
}

/// Validate pipeline SQL against data source access modes
fn validate_pipeline_sql(
    pipeline_name: &str,
    sql: &str,
    data_sources: &[DataSource],
) -> Result<()> {
    let validator_config = validator_config_from_sources(data_sources);

    // Validate the SQL against access mode restrictions
    validate_sql(sql, &validator_config).map_err(|e| {
        anyhow::anyhow!("Pipeline '{}' SQL validation failed: {}", pipeline_name, e)
    })?;

    tracing::info!(
        "✅ Pipeline '{}' SQL validated against access modes",
        pipeline_name
    );
    Ok(())
}

/// Register data sources with DataFusion SessionContext
pub async fn register_data_sources(
    session_ctx: &mut SessionContext,
    data_sources: &[DataSource],
) -> Result<()> {
    tracing::info!(
        "Registering {} data sources with DataFusion",
        data_sources.len()
    );

    for source in data_sources {
        register_data_source(session_ctx, source, None)
            .await
            .with_context(|| format!("Failed to register data source: {}", source.name))?;
    }

    tracing::info!("All data sources registered successfully");
    Ok(())
}

/// Which of the two startup contexts a registration serves. The
/// PLANNING context (load_server_config) only validates pipeline SQL —
/// it plans, never executes — so providers whose startup work is
/// expensive network I/O can register lazily there from declared
/// schemas; the RUNTIME context (setup_app_state) is the one queries
/// execute on and the one /data_source reports, so it pays the real
/// preflight. Today only graph branches on this (it is the one type
/// with per-view startup probes on top of the dial); the other
/// twice-dialing types (mongodb, dynamodb, open_connector) are
/// pre-existing candidates for the same treatment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationPass {
    Planning,
    Runtime,
}

/// Register data sources with DataFusion SessionContext and OptimizerRegistry
pub async fn register_data_sources_with_registry(
    session_ctx: &mut SessionContext,
    data_sources: &[DataSource],
    optimizer_registry: &Arc<crate::optimizer_registry::OptimizerRegistry>,
) -> Result<()> {
    register_data_sources_for_pass(
        session_ctx,
        data_sources,
        optimizer_registry,
        RegistrationPass::Runtime,
    )
    .await
}

/// [`register_data_sources_with_registry`], with the pass named — the
/// planning context registers graph sources lazily (no dial, no
/// per-view probes; see [`RegistrationPass`]).
pub async fn register_data_sources_for_pass(
    session_ctx: &mut SessionContext,
    data_sources: &[DataSource],
    optimizer_registry: &Arc<crate::optimizer_registry::OptimizerRegistry>,
    pass: RegistrationPass,
) -> Result<()> {
    tracing::info!(
        "Registering {} data sources with DataFusion and optimizer registry ({pass:?} pass)",
        data_sources.len()
    );

    for source in data_sources {
        register_data_source_for_pass(session_ctx, source, Some(optimizer_registry), pass)
            .await
            .with_context(|| format!("Failed to register data source: {}", source.name))?;
    }

    tracing::info!("All data sources registered successfully");
    Ok(())
}

/// Register a single data source with DataFusion
async fn register_data_source(
    session_ctx: &mut SessionContext,
    source: &DataSource,
    optimizer_registry: Option<&Arc<crate::optimizer_registry::OptimizerRegistry>>,
) -> Result<()> {
    register_data_source_for_pass(
        session_ctx,
        source,
        optimizer_registry,
        RegistrationPass::Runtime,
    )
    .await
}

async fn register_data_source_for_pass(
    session_ctx: &mut SessionContext,
    source: &DataSource,
    optimizer_registry: Option<&Arc<crate::optimizer_registry::OptimizerRegistry>>,
    pass: RegistrationPass,
) -> Result<()> {
    tracing::info!(
        "Registering data source: {} (type: {:?})",
        source.name,
        source.source_type
    );

    // Initialize S3 storage handler for remote operations
    let s3_storage = S3Storage::new();

    // Validate data source configuration based on path type
    match (&source.source_type, s3_storage.is_remote_path(&source.path)) {
        (DataSourceType::Csv | DataSourceType::Parquet | DataSourceType::Sqlite, false) => {
            // For local files, verify the file exists
            if !source.path.exists() {
                return Err(ConfigError::DataSourceFileNotFound {
                    name: source.name.clone(),
                    path: source.path.clone(),
                }
                .into());
            }
        }
        (DataSourceType::Lance, false) => {
            // Lance paths may legitimately not exist yet — a job with
            // `create_if_missing: true` will create the dataset on its
            // first successful run. Deferred registration is logged
            // further down; the missing-path check is deliberately
            // omitted here.
        }
        (DataSourceType::Csv | DataSourceType::Parquet | DataSourceType::Lance, true) => {
            // For S3 files, validate S3 configuration and setup object store
            s3_storage.validate_configuration(source)?;
            let s3_path = source.path.to_str().unwrap_or("");
            s3_storage
                .setup_object_store(session_ctx, &source.name, s3_path)
                .await?;
        }
        (
            DataSourceType::Postgres
            | DataSourceType::Mysql
            | DataSourceType::Mongo
            | DataSourceType::Redis
            | DataSourceType::Seekdb
            | DataSourceType::Influxdb
            | DataSourceType::Clickhouse
            | DataSourceType::OpenConnector
            | DataSourceType::Dynamodb,
            _,
        ) => {
            // Database sources don't need file path validation
        }
        (DataSourceType::Iceberg, _) => {
            // Validation happened during data source registration
        }
        _ => {
            // Other combinations are valid without additional checks
        }
    }

    match source.source_type {
        DataSourceType::Csv => {
            tracing::debug!("Registering CSV file: {} at {:?}", source.name, source.path);

            // Create CSV format options from source configuration
            let mut csv_read_options = datafusion::prelude::CsvReadOptions::new();

            // Apply options if specified
            if let Some(ref options) = source.options {
                if let Some(has_header) = options.get("has_header") {
                    csv_read_options =
                        csv_read_options.has_header(has_header.parse::<bool>().unwrap_or(true));
                }
                if let Some(delimiter) = options.get("delimiter")
                    && let Some(delimiter_char) = delimiter.chars().next()
                {
                    csv_read_options = csv_read_options.delimiter(delimiter_char as u8);
                }
                if let Some(schema_infer_max) = options.get("schema_infer_max_records")
                    && let Ok(max_records) = schema_infer_max.parse::<usize>()
                {
                    csv_read_options = csv_read_options.schema_infer_max_records(max_records);
                }
            }

            let path_str = source
                .path
                .to_str()
                .ok_or_else(|| ConfigError::NonUtf8Path {
                    name: source.name.clone(),
                    path: source.path.clone(),
                })?;

            // Register the CSV file as a table
            session_ctx
                .register_csv(&source.name, path_str, csv_read_options)
                .await
                .map_err(|e| ConfigError::DataSourceRegistrationFailed {
                    name: source.name.clone(),
                    error: e.to_string(),
                })?;
        }
        DataSourceType::Parquet => {
            tracing::debug!(
                "Registering Parquet file: {} at {:?}",
                source.name,
                source.path
            );

            let path_str = source
                .path
                .to_str()
                .ok_or_else(|| ConfigError::NonUtf8Path {
                    name: source.name.clone(),
                    path: source.path.clone(),
                })?;

            // Register the Parquet file as a table
            session_ctx
                .register_parquet(
                    &source.name,
                    path_str,
                    datafusion::prelude::ParquetReadOptions::default(),
                )
                .await
                .map_err(|e| ConfigError::DataSourceRegistrationFailed {
                    name: source.name.clone(),
                    error: e.to_string(),
                })?;
        }
        DataSourceType::Postgres => {
            tracing::info!(
                "Registering PostgreSQL table: {} (access_mode: {:?})",
                source.name,
                source.access_mode
            );

            // Get connection string
            let connection_string = source.connection_string.as_ref().ok_or_else(|| {
                ConfigError::MissingConnectionString {
                    name: source.name.clone(),
                }
            })?;

            tracing::debug!(
                "Connection string for {}: {} (options: {:?})",
                source.name,
                connection_string,
                source.options
            );

            // Register PostgreSQL table using the sqlx-based provider
            let pg_knn_registry = optimizer_registry.map(|r| r.pg_knn_pools());
            register_postgres_tables(
                session_ctx,
                &source.name,
                connection_string,
                source.options.as_ref(),
                source.access_mode.is_read_write(),
                pg_knn_registry.as_ref(),
                source.hierarchy_level,
            )
            .await
            .map_err(|e| {
                tracing::error!(
                    "PostgreSQL registration failed for '{}': {:?}",
                    source.name,
                    e
                );
                ConfigError::PostgresConnectionFailed {
                    name: source.name.clone(),
                    error: format!("{:?}", e),
                }
            })?;
        }
        DataSourceType::Mysql => {
            tracing::info!(
                "Registering MySQL table: {} (access_mode: {:?})",
                source.name,
                source.access_mode
            );

            let connection_string = source.connection_string.as_ref().ok_or_else(|| {
                ConfigError::MissingConnectionString {
                    name: source.name.clone(),
                }
            })?;

            tracing::debug!(
                "Connection string for {}: {} (options: {:?})",
                source.name,
                connection_string,
                source.options
            );

            register_mysql_tables(
                session_ctx,
                &source.name,
                connection_string,
                source.options.as_ref(),
                source.access_mode.is_read_write(),
                source.hierarchy_level,
            )
            .await
            .map_err(|e| {
                tracing::error!("MySQL registration failed for '{}': {:?}", source.name, e);
                ConfigError::MySQLConnectionFailed {
                    name: source.name.clone(),
                    error: format!("{:?}", e),
                }
            })?;
        }
        DataSourceType::Seekdb => {
            tracing::info!(
                "Registering SeekDB table: {} (access_mode: {:?})",
                source.name,
                source.access_mode
            );

            let connection_string = source.connection_string.as_ref().ok_or_else(|| {
                ConfigError::MissingConnectionString {
                    name: source.name.clone(),
                }
            })?;

            let seekdb_registry = optimizer_registry.map(|r| r.datasets());
            register_seekdb_tables(
                session_ctx,
                &source.name,
                connection_string,
                source.options.as_ref(),
                source.access_mode.is_read_write(),
                seekdb_registry.as_ref(),
                source.hierarchy_level,
            )
            .await
            .map_err(|e| {
                tracing::error!("SeekDB registration failed for '{}': {:?}", source.name, e);
                ConfigError::SeekDbConnectionFailed {
                    name: source.name.clone(),
                    error: format!("{:?}", e),
                }
            })?;
        }
        DataSourceType::Sqlite => {
            tracing::info!(
                "Registering SQLite table: {} from {:?} (access_mode: {:?})",
                source.name,
                source.path,
                source.access_mode
            );

            let db_path =
                source
                    .path
                    .to_str()
                    .ok_or_else(|| ConfigError::DataSourceRegistrationFailed {
                        name: source.name.clone(),
                        error: "Invalid SQLite database path".to_string(),
                    })?;

            let sqlite_registry = optimizer_registry.map(|r| r.datasets());
            register_sqlite_tables(
                session_ctx,
                &source.name,
                db_path,
                source.options.as_ref(),
                source.access_mode.is_read_write(),
                sqlite_registry.as_ref(),
                source.hierarchy_level,
            )
            .await
            .map_err(|e| {
                tracing::error!("SQLite registration failed for '{}': {:?}", source.name, e);
                ConfigError::SQLiteConnectionFailed {
                    name: source.name.clone(),
                    error: format!("{:?}", e),
                }
            })?;
        }
        DataSourceType::Iceberg => {
            tracing::info!(
                "Registering Iceberg table: {} from warehouse {:?}",
                source.name,
                source.path
            );

            let warehouse_path =
                source
                    .path
                    .to_str()
                    .ok_or_else(|| ConfigError::DataSourceRegistrationFailed {
                        name: source.name.clone(),
                        error: "Invalid warehouse path".to_string(),
                    })?;

            register_iceberg_table(
                session_ctx,
                &source.name,
                warehouse_path,
                source.options.as_ref(),
            )
            .await
            .map_err(|e| {
                tracing::error!("Iceberg registration failed for '{}': {:?}", source.name, e);
                ConfigError::DataSourceRegistrationFailed {
                    name: source.name.clone(),
                    error: format!("{:?}", e),
                }
            })?;
        }
        DataSourceType::Mongo => {
            tracing::info!("Registering MongoDB collection: {}", source.name);

            let connection_string = source.connection_string.as_ref().ok_or_else(|| {
                ConfigError::MissingConnectionString {
                    name: source.name.clone(),
                }
            })?;

            tracing::debug!(
                "Connection string for {}: {} (options: {:?})",
                source.name,
                connection_string,
                source.options
            );

            let mongo_registry = optimizer_registry.map(|r| r.datasets());
            register_mongo_tables(
                session_ctx,
                &source.name,
                connection_string,
                source.options.as_ref(),
                mongo_registry.as_ref(),
            )
            .await
            .map_err(|e| {
                tracing::error!("MongoDB registration failed for '{}': {:?}", source.name, e);
                ConfigError::DataSourceRegistrationFailed {
                    name: source.name.clone(),
                    error: format!("{:?}", e),
                }
            })?;
        }
        DataSourceType::Dynamodb => {
            tracing::info!("Registering DynamoDB table: {}", source.name);

            let connection_string = source.connection_string.as_ref().ok_or_else(|| {
                ConfigError::MissingConnectionString {
                    name: source.name.clone(),
                }
            })?;

            tracing::debug!(
                "Endpoint for {}: {} (options: {:?})",
                source.name,
                connection_string,
                source.options
            );

            register_dynamodb_tables(
                session_ctx,
                &source.name,
                connection_string,
                source.options.as_ref(),
                source.access_mode.is_read_write(),
                source.hierarchy_level,
            )
            .await
            .map_err(|e| {
                tracing::error!(
                    "DynamoDB registration failed for '{}': {:?}",
                    source.name,
                    e
                );
                ConfigError::DataSourceRegistrationFailed {
                    name: source.name.clone(),
                    error: format!("{:?}", e),
                }
            })?;
        }
        DataSourceType::OpenConnector => {
            tracing::info!(
                "Registering Open Connector gateway: {} (hierarchy_level: {:?})",
                source.name,
                source.hierarchy_level
            );

            let connection_string = source.connection_string.as_ref().ok_or_else(|| {
                ConfigError::MissingConnectionString {
                    name: source.name.clone(),
                }
            })?;

            // `validate_data_sources` already guarantees the config block is
            // present and access is read-only; the provider re-checks both so
            // the CLI path (which has no such validation layer) is covered.
            let oc_gateways = optimizer_registry.map(|r| r.open_connector_gateways());
            register_open_connector_tables(
                session_ctx,
                &source.name,
                connection_string,
                source.open_connector.as_ref(),
                source.access_mode.is_read_write(),
                source.hierarchy_level,
                oc_gateways.as_ref(),
            )
            .await
            .map_err(|e| {
                tracing::error!(
                    "Open Connector registration failed for '{}': {:?}",
                    source.name,
                    e
                );
                ConfigError::DataSourceRegistrationFailed {
                    name: source.name.clone(),
                    error: format!("{:?}", e),
                }
            })?;
        }
        DataSourceType::Graph => {
            tracing::info!(
                "Registering graph source: {} (hierarchy_level: {:?})",
                source.name,
                source.hierarchy_level
            );

            let connection_string = source.connection_string.as_ref().ok_or_else(|| {
                ConfigError::MissingConnectionString {
                    name: source.name.clone(),
                }
            })?;

            // Same contract as open_connector above: validate_data_sources
            // already enforced the invariants; the provider re-checks them so
            // the CLI path is covered. Without a registry (the embedder seam)
            // a throwaway map stands in — the same behaviour open_connector
            // has with no gateways map: view providers hold Arc'd handles
            // regardless, and UDTF resolution had no registry there anyway.
            let graph_sources = optimizer_registry
                .map(|r| r.graph_sources())
                .unwrap_or_default();
            // Startup builds a PLANNING context (load_server_config) and
            // a RUNTIME context (setup_app_state), and both register
            // every source. Graph would be the most expensive occupant
            // of that doubling — the preflight dial PLUS every view's
            // LIMIT-1 validation Cypher — so the planning pass registers
            // it LAZILY: pure validation and declared schemas only, no
            // network I/O, no second pool dial (planning only plans;
            // pipelines execute on the runtime engine, and /data_source
            // reports the runtime registration's health). This also
            // removes the two-pass disagreement window a flapping
            // backend had: exactly one pass classifies health.
            let result = match pass {
                RegistrationPass::Runtime => {
                    register_graph_tables(
                        session_ctx,
                        &graph_sources,
                        &source.name,
                        connection_string,
                        source.graph.as_ref(),
                        source.access_mode.is_read_write(),
                        source.hierarchy_level,
                    )
                    .await
                }
                RegistrationPass::Planning => {
                    skardi::sources::providers::graph::register_graph_tables_lazy(
                        session_ctx,
                        &graph_sources,
                        &source.name,
                        connection_string,
                        source.graph.as_ref(),
                        source.access_mode.is_read_write(),
                        source.hierarchy_level,
                    )
                    .await
                }
            };
            result.map_err(|e| {
                tracing::error!("Graph registration failed for '{}': {:?}", source.name, e);
                ConfigError::DataSourceRegistrationFailed {
                    name: source.name.clone(),
                    error: format!("{:?}", e),
                }
            })?;
        }
        DataSourceType::Redis => {
            tracing::info!("Registering Redis table: {}", source.name);

            let connection_string = source.connection_string.as_ref().ok_or_else(|| {
                ConfigError::MissingConnectionString {
                    name: source.name.clone(),
                }
            })?;

            tracing::debug!(
                "Connection string for {}: {} (options: {:?})",
                source.name,
                connection_string,
                source.options
            );

            register_redis_tables(
                session_ctx,
                &source.name,
                connection_string,
                source.options.as_ref(),
            )
            .map_err(|e| {
                tracing::error!("Redis registration failed for '{}': {:?}", source.name, e);
                ConfigError::DataSourceRegistrationFailed {
                    name: source.name.clone(),
                    error: format!("{:?}", e),
                }
            })?;
        }
        DataSourceType::Influxdb => {
            tracing::info!("Registering InfluxDB table: {}", source.name);

            let connection_string = source.connection_string.as_ref().ok_or_else(|| {
                ConfigError::MissingConnectionString {
                    name: source.name.clone(),
                }
            })?;

            register_influxdb_tables(
                session_ctx,
                &source.name,
                connection_string,
                source.options.as_ref(),
            )
            .await
            .map_err(|e| {
                tracing::error!(
                    "InfluxDB registration failed for '{}': {:?}",
                    source.name,
                    e
                );
                ConfigError::DataSourceRegistrationFailed {
                    name: source.name.clone(),
                    error: format!("{:?}", e),
                }
            })?;
        }
        DataSourceType::Clickhouse => {
            tracing::info!(
                "Registering ClickHouse table: {} (hierarchy_level: {:?})",
                source.name,
                source.hierarchy_level
            );

            let connection_string = source.connection_string.as_ref().ok_or_else(|| {
                ConfigError::MissingConnectionString {
                    name: source.name.clone(),
                }
            })?;

            register_clickhouse_tables(
                session_ctx,
                &source.name,
                connection_string,
                source.options.as_ref(),
                source.access_mode.is_read_write(),
                source.hierarchy_level,
            )
            .await
            .map_err(|e| {
                tracing::error!(
                    "ClickHouse registration failed for '{}': {:?}",
                    source.name,
                    e
                );
                ConfigError::DataSourceRegistrationFailed {
                    name: source.name.clone(),
                    error: format!("{:?}", e),
                }
            })?;
        }
        DataSourceType::Lance => {
            tracing::info!(
                "Registering Lance dataset: {} at {:?}",
                source.name,
                source.path
            );

            // Get the dataset registry if optimizer registry is provided
            let dataset_registry = optimizer_registry.map(|reg| reg.lance_datasets());

            let path_str = source
                .path
                .to_str()
                .ok_or_else(|| ConfigError::NonUtf8Path {
                    name: source.name.clone(),
                    path: source.path.clone(),
                })?;

            // Jobs may declare a Lance destination whose dataset doesn't
            // exist yet (first run will create it). Registering would fail;
            // instead, log and defer — the jobs executor has its own
            // lance_paths map and doesn't need the source registered as a
            // table. A plain SELECT against the name will still error,
            // which is the right UX for "this dataset hasn't been written
            // to yet".
            if !skardi::sources::providers::lance::lance_dataset_exists(path_str) {
                tracing::warn!(
                    "Lance dataset '{}' at {} does not exist yet — skipping table \
                     registration. A job with `create_if_missing: true` will create it \
                     on its first successful run.",
                    source.name,
                    path_str
                );
                return Ok(());
            }

            // Register Lance dataset using the providers module
            register_lance_table(
                session_ctx,
                &source.name,
                path_str,
                dataset_registry.as_ref(),
            )
            .await
            .map_err(|e| ConfigError::DataSourceRegistrationFailed {
                name: source.name.clone(),
                error: e.to_string(),
            })?;
        }
        DataSourceType::Documents => {
            #[cfg(feature = "documents")]
            {
                tracing::info!(
                    "Registering documents source: {} at {:?}",
                    source.name,
                    source.path
                );

                let path_str = source
                    .path
                    .to_str()
                    .ok_or_else(|| ConfigError::NonUtf8Path {
                        name: source.name.clone(),
                        path: source.path.clone(),
                    })?;

                // S3 registration checks (no-op when both `path` and
                // `image_store` are local): reject credentials in config, enforce
                // the same-bucket constraint, then verify read connectivity and
                // image_store writability up front.
                let image_store = source
                    .options
                    .as_ref()
                    .and_then(|o| o.get("image_store"))
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty());
                let s3 = crate::remote_storage::S3Storage::new();
                s3.validate_documents_configuration(path_str, image_store.as_deref(), source)
                    .map_err(|e| ConfigError::DataSourceRegistrationFailed {
                        name: source.name.clone(),
                        error: e.to_string(),
                    })?;
                s3.preflight_documents_s3(path_str, image_store.as_deref(), &source.name)
                    .await
                    .map_err(|e| ConfigError::DataSourceRegistrationFailed {
                        name: source.name.clone(),
                        error: e.to_string(),
                    })?;

                skardi::sources::providers::documents::register_documents_tables(
                    session_ctx,
                    &source.name,
                    path_str,
                    source.options.as_ref(),
                )
                .await
                .map_err(|e| {
                    tracing::error!(
                        "documents registration failed for '{}': {:?}",
                        source.name,
                        e
                    );
                    ConfigError::DataSourceRegistrationFailed {
                        name: source.name.clone(),
                        error: format!("{:?}", e),
                    }
                })?;
            }
            #[cfg(not(feature = "documents"))]
            {
                return Err(ConfigError::DataSourceRegistrationFailed {
                    name: source.name.clone(),
                    error: "documents data source type requires the `documents` feature to be \
                            enabled at build time"
                        .to_string(),
                }
                .into());
            }
        }
        DataSourceType::Rss => {
            #[cfg(feature = "rss")]
            {
                tracing::info!(
                    "Registering RSS source: {} (hierarchy_level: {:?})",
                    source.name,
                    source.hierarchy_level
                );

                // Config presence, catalog-only, and read-only are re-checked
                // inside the provider: `register_with_policy` in
                // `sources/providers/rss/mod.rs` is the single enforcement
                // point that this arm and the public embedder seam
                // (`register_rss_tables_with_policy`) both feed into.
                register_rss_tables(
                    session_ctx,
                    &source.name,
                    source.rss.as_ref(),
                    source.access_mode.is_read_write(),
                    source.hierarchy_level,
                )
                .await
                .map_err(|e| ConfigError::DataSourceRegistrationFailed {
                    name: source.name.clone(),
                    error: e.to_string(),
                })?;
            }
            #[cfg(not(feature = "rss"))]
            {
                return Err(ConfigError::DataSourceRegistrationFailed {
                    name: source.name.clone(),
                    error: "rss data source type requires the `rss` feature to be enabled at \
                            build time"
                        .to_string(),
                }
                .into());
            }
        }
        DataSourceType::Obsidian => {
            #[cfg(feature = "obsidian")]
            {
                tracing::info!(
                    "Registering Obsidian source: {} at {:?} (hierarchy_level: {:?})",
                    source.name,
                    source.path,
                    source.hierarchy_level
                );

                let path_str = source
                    .path
                    .to_str()
                    .ok_or_else(|| ConfigError::NonUtf8Path {
                        name: source.name.clone(),
                        path: source.path.clone(),
                    })?;

                // Catalog-only and read-only are re-checked inside the
                // provider, which is the single enforcement point shared by
                // this arm and the embedder-facing entry point — the same
                // arrangement as the RSS arm above.
                register_obsidian_tables(
                    session_ctx,
                    &source.name,
                    path_str,
                    source.options.as_ref(),
                    source.access_mode.is_read_write(),
                    source.hierarchy_level,
                )
                .await
                .map_err(|e| ConfigError::DataSourceRegistrationFailed {
                    name: source.name.clone(),
                    error: format!("{e:#}"),
                })?;
            }
            #[cfg(not(feature = "obsidian"))]
            {
                return Err(ConfigError::DataSourceRegistrationFailed {
                    name: source.name.clone(),
                    error: "obsidian data source type requires the `obsidian` feature to be \
                            enabled at build time"
                        .to_string(),
                }
                .into());
            }
        }
    }

    // If enable_cache is set for Csv/Parquet/Iceberg, load the table into a MemTable
    if source.enable_cache
        && matches!(
            source.source_type,
            DataSourceType::Csv | DataSourceType::Parquet | DataSourceType::Iceberg
        )
    {
        tracing::info!(
            "Caching data source '{}' into memory (enable_cache=true)",
            source.name
        );

        let df = session_ctx
            .sql(&format!("SELECT * FROM {}", source.name))
            .await
            .map_err(|e| ConfigError::DataSourceRegistrationFailed {
                name: source.name.clone(),
                error: format!("Failed to read table for caching: {}", e),
            })?;

        let batches =
            df.collect()
                .await
                .map_err(|e| ConfigError::DataSourceRegistrationFailed {
                    name: source.name.clone(),
                    error: format!("Failed to collect batches for caching: {}", e),
                })?;

        let schema = if let Some(batch) = batches.first() {
            batch.schema()
        } else {
            // Empty table — get schema from the dataframe
            let df = session_ctx
                .sql(&format!("SELECT * FROM {} LIMIT 0", source.name))
                .await
                .map_err(|e| ConfigError::DataSourceRegistrationFailed {
                    name: source.name.clone(),
                    error: format!("Failed to infer schema for caching: {}", e),
                })?;
            Arc::new(df.schema().as_arrow().clone())
        };

        // Deregister the original table and replace with MemTable
        session_ctx.deregister_table(&source.name).map_err(|e| {
            ConfigError::DataSourceRegistrationFailed {
                name: source.name.clone(),
                error: format!("Failed to deregister table for caching: {}", e),
            }
        })?;

        let mem_table = MemTable::try_new(schema, vec![batches]).map_err(|e| {
            ConfigError::DataSourceRegistrationFailed {
                name: source.name.clone(),
                error: format!("Failed to create MemTable for caching: {}", e),
            }
        })?;

        session_ctx
            .register_table(&source.name, Arc::new(mem_table))
            .map_err(|e| ConfigError::DataSourceRegistrationFailed {
                name: source.name.clone(),
                error: format!("Failed to register cached MemTable: {}", e),
            })?;

        tracing::info!(
            "Data source '{}' cached in memory successfully",
            source.name
        );
    }

    tracing::info!("Successfully registered data source: {}", source.name);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_pipeline_file(dir: &TempDir) -> PathBuf {
        let pipeline_content = r#"
kind: pipeline
metadata:
  name: "test-pipeline"
  version: "1.0.0"
  description: "Test pipeline for configuration testing"
spec:
  query: |
    SELECT date, category, value
    FROM sample_data
    WHERE date >= {date_filter}
      AND ({category_filter} IS NULL OR category = {category_filter})
"#;

        let pipeline_path = dir.path().join("test-pipeline.yaml");
        fs::write(&pipeline_path, pipeline_content).unwrap();
        pipeline_path
    }

    fn create_test_simple_pipeline_file(dir: &TempDir) -> PathBuf {
        let pipeline_content = r#"
kind: pipeline
metadata:
  name: "test-pipeline"
  version: "1.0.0"
  description: "Simple test pipeline without external table dependencies"
spec:
  query: |
    SELECT 1 as id, 'test' as name
"#;

        let pipeline_path = dir.path().join("simple-pipeline.yaml");
        fs::write(&pipeline_path, pipeline_content).unwrap();
        pipeline_path
    }

    fn create_test_context_file(dir: &TempDir) -> PathBuf {
        // Create the actual CSV file in the temp directory
        let data_dir = dir.path().join("data");
        fs::create_dir_all(&data_dir).unwrap();

        let csv_path = data_dir.join("test.csv");
        let csv_content = "date,value,category\n2023-01-01,1.0,A\n2023-01-02,2.0,B\n";
        fs::write(&csv_path, csv_content).unwrap();

        // Create a second test file (CSV)
        let csv2_path = data_dir.join("reference.csv");
        let ref_content = "id,name\n1,test1\n2,test2\n";
        fs::write(&csv2_path, ref_content).unwrap();

        // Create context content with correct paths (both CSV for simplicity)
        let context_content = format!(
            r#"
kind: context
metadata:
  name: test-context
  version: 1.0.0
spec:
  data_sources:
    - name: "sample_data"
      type: "csv"
      path: "{}"
      schema:
        date: "timestamp"
        value: "float64"
        category: "string"
      options:
        has_header: true
        delimiter: ","
    - name: "reference_data"
      type: "csv"
      path: "{}"
      options:
        has_header: true
        delimiter: ","
"#,
            csv_path.display(),
            csv2_path.display()
        );

        let context_path = dir.path().join("context.yaml");
        fs::write(&context_path, context_content).unwrap();

        // Create the referenced data files
        let data_dir = dir.path().join("data");
        fs::create_dir_all(&data_dir).unwrap();

        let csv_path = data_dir.join("test.csv");
        fs::write(
            &csv_path,
            "date,value,category\n2023-01-01,1.0,A\n2023-01-02,2.0,B\n",
        )
        .unwrap();

        let parquet_path = data_dir.join("reference.parquet");
        fs::write(&parquet_path, "dummy parquet content").unwrap();

        context_path
    }

    #[test]
    fn test_load_context_config_success() {
        let temp_dir = TempDir::new().unwrap();
        let context_path = create_test_context_file(&temp_dir);

        let data_sources = load_context_config(&context_path).unwrap();

        assert_eq!(data_sources.len(), 2);

        // Check first data source (CSV)
        assert_eq!(data_sources[0].name, "sample_data");
        assert!(matches!(data_sources[0].source_type, DataSourceType::Csv));
        assert!(data_sources[0].schema.is_some());
        assert!(data_sources[0].options.is_some());

        // Check second data source (CSV)
        assert_eq!(data_sources[1].name, "reference_data");
        assert!(matches!(data_sources[1].source_type, DataSourceType::Csv));
        assert!(data_sources[1].schema.is_none());
        assert!(data_sources[1].options.is_some());
    }

    #[test]
    fn test_load_context_config_file_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let missing_path = temp_dir.path().join("missing.yaml");

        let result = load_context_config(&missing_path);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("Context file not found"));
    }

    #[test]
    fn test_load_context_config_invalid_yaml() {
        let temp_dir = TempDir::new().unwrap();
        let invalid_yaml_path = temp_dir.path().join("invalid.yaml");

        fs::write(&invalid_yaml_path, "invalid: yaml: content: [").unwrap();

        let result = load_context_config(&invalid_yaml_path);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("Invalid YAML"));
    }

    #[tokio::test]
    async fn test_load_pipeline_config_success() {
        let temp_dir = TempDir::new().unwrap();
        let pipeline_path = create_test_pipeline_file(&temp_dir);
        let ctx = Arc::new(SessionContext::new());

        // Register mock sample_data table for schema inference
        use arrow::array::{Date32Array, Float64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Schema::new(vec![
            Field::new("date", DataType::Date32, false),
            Field::new("category", DataType::Utf8, true),
            Field::new("value", DataType::Float64, false),
            Field::new("timestamp", DataType::Date32, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Date32Array::from(vec![18628])), // 2021-01-01
                Arc::new(StringArray::from(vec![Some("test")])),
                Arc::new(Float64Array::from(vec![1.0])),
                Arc::new(Date32Array::from(vec![18628])),
            ],
        )
        .unwrap();
        ctx.register_batch("sample_data", batch).unwrap();

        let pipeline = load_pipeline_config(&pipeline_path, ctx).await.unwrap();

        // Verify pipeline loaded successfully with correct metadata
        assert_eq!(pipeline.name(), "test-pipeline");
        assert_eq!(pipeline.version(), "1.0.0");
    }

    #[tokio::test]
    async fn test_load_pipeline_config_file_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let missing_path = temp_dir.path().join("missing-pipeline.yaml");
        let ctx = Arc::new(SessionContext::new());

        let result = load_pipeline_config(&missing_path, ctx).await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("Pipeline file not found"));
    }

    #[tokio::test]
    async fn test_load_server_config_with_both_files() {
        let temp_dir = TempDir::new().unwrap();
        let pipeline_path = create_test_pipeline_file(&temp_dir);
        let context_path = create_test_context_file(&temp_dir);

        let args = CliArgs {
            pipeline_path: Some(pipeline_path),
            jobs_path: None,
            jobs_db_path: None,
            ctx_file: Some(context_path),
            semantics_path: None,
            port: 8080,
            query_audit_db: None,
            query_audit_retention_days: None,
        };

        let config = load_server_config(args).await.unwrap();

        // Configuration loaded successfully with pipeline and context data
        assert_eq!(config.data_sources.len(), 2);
        assert_eq!(config.args.port, 8080);
        assert_eq!(config.pipelines.len(), 1);
        let pipeline = config.pipelines.get("test-pipeline").unwrap();
        assert_eq!(pipeline.name(), "test-pipeline");
        assert_eq!(pipeline.version(), "1.0.0");
    }

    #[tokio::test]
    async fn test_load_server_config_pipeline_only() {
        let temp_dir = TempDir::new().unwrap();
        let pipeline_path = create_test_simple_pipeline_file(&temp_dir);

        let args = CliArgs {
            pipeline_path: Some(pipeline_path),
            jobs_path: None,
            jobs_db_path: None,
            ctx_file: None,
            semantics_path: None,
            port: 3000,
            query_audit_db: None,
            query_audit_retention_days: None,
        };

        let config = load_server_config(args).await.unwrap();

        // Configuration loaded successfully with pipeline only (no context data)
        assert_eq!(config.data_sources.len(), 0);
        assert_eq!(config.args.port, 3000);
        assert_eq!(config.pipelines.len(), 1);
        let pipeline = config.pipelines.get("test-pipeline").unwrap();
        assert_eq!(pipeline.name(), "test-pipeline");
        assert_eq!(pipeline.version(), "1.0.0");
    }

    #[tokio::test]
    async fn test_load_server_config_from_directory() {
        let temp_dir = TempDir::new().unwrap();

        // Create a pipelines subdirectory
        let pipelines_dir = temp_dir.path().join("pipelines");
        fs::create_dir_all(&pipelines_dir).unwrap();

        // Create first pipeline
        let pipeline1_content = r#"
kind: pipeline
metadata:
  name: "test-pipeline"
  version: "1.0.0"
  description: "First test pipeline"
spec:
  query: |
    SELECT 1 as id, 'test' as name
"#;
        fs::write(pipelines_dir.join("first-pipeline.yaml"), pipeline1_content).unwrap();

        // Create second pipeline with different name
        let pipeline2_content = r#"
kind: pipeline
metadata:
  name: "second-pipeline"
  version: "2.0.0"
  description: "Second test pipeline"
spec:
  query: |
    SELECT 2 as id, 'test2' as name
"#;
        fs::write(pipelines_dir.join("second-pipeline.yml"), pipeline2_content).unwrap();

        let args = CliArgs {
            pipeline_path: Some(pipelines_dir),
            jobs_path: None,
            jobs_db_path: None,
            ctx_file: None,
            semantics_path: None,
            port: 3000,
            query_audit_db: None,
            query_audit_retention_days: None,
        };

        let config = load_server_config(args).await.unwrap();

        // Configuration loaded successfully with both pipelines from directory
        assert_eq!(config.pipelines.len(), 2);
        assert!(config.pipelines.contains_key("test-pipeline"));
        assert!(config.pipelines.contains_key("second-pipeline"));

        let pipeline1 = config.pipelines.get("test-pipeline").unwrap();
        assert_eq!(pipeline1.version(), "1.0.0");

        let pipeline2 = config.pipelines.get("second-pipeline").unwrap();
        assert_eq!(pipeline2.version(), "2.0.0");
    }

    #[tokio::test]
    async fn test_load_server_config_missing_pipeline() {
        let temp_dir = TempDir::new().unwrap();
        let missing_pipeline = temp_dir.path().join("missing.yaml");

        let args = CliArgs {
            pipeline_path: Some(missing_pipeline),
            jobs_path: None,
            jobs_db_path: None,
            ctx_file: None,
            semantics_path: None,
            port: 8080,
            query_audit_db: None,
            query_audit_retention_days: None,
        };

        let result = load_server_config(args).await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        // Error is wrapped with context, check the chain
        let error_string = format!("{:?}", error);
        assert!(
            error_string.contains("Pipeline file not found"),
            "Expected error to contain 'Pipeline file not found', got: {}",
            error_string
        );
    }

    #[tokio::test]
    async fn test_load_server_config_no_pipelines() {
        let args = CliArgs {
            pipeline_path: None,
            jobs_path: None,
            jobs_db_path: None,
            ctx_file: None,
            semantics_path: None,
            port: 8080,
            query_audit_db: None,
            query_audit_retention_days: None,
        };

        let config = load_server_config(args).await.unwrap();

        // Configuration loaded successfully with no pipelines
        assert_eq!(config.pipelines.len(), 0);
    }

    #[test]
    fn test_resolve_pipeline_files_directory() {
        let temp_dir = TempDir::new().unwrap();
        let pipelines_dir = temp_dir.path().join("pipelines");
        fs::create_dir_all(&pipelines_dir).unwrap();

        // Create multiple pipeline files with different extensions
        fs::write(pipelines_dir.join("a_pipeline.yaml"), "content1").unwrap();
        fs::write(pipelines_dir.join("b_pipeline.yml"), "content2").unwrap();
        fs::write(pipelines_dir.join("c_pipeline.YAML"), "content3").unwrap(); // uppercase extension

        // Create a non-pipeline file that should be ignored
        fs::write(pipelines_dir.join("readme.txt"), "not a pipeline").unwrap();
        fs::write(pipelines_dir.join("config.json"), "{}").unwrap();

        let result = resolve_pipeline_files(Some(&pipelines_dir)).unwrap();

        // Should find 3 pipeline files (yaml, yml, YAML), sorted alphabetically
        assert_eq!(result.len(), 3);
        assert!(
            result[0]
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .contains("a_pipeline")
        );
        assert!(
            result[1]
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .contains("b_pipeline")
        );
        assert!(
            result[2]
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .contains("c_pipeline")
        );
    }

    #[test]
    fn test_data_source_deserialization() {
        let yaml_content = r#"
name: "test_source"
type: "csv"
path: "/path/to/file.csv"
schema:
  col1: "string"
  col2: "int64"
options:
  has_header: "true"
  delimiter: ","
"#;

        let data_source: DataSource = serde_yaml::from_str(yaml_content).unwrap();

        assert_eq!(data_source.name, "test_source");
        assert!(matches!(data_source.source_type, DataSourceType::Csv));
        assert_eq!(data_source.path, PathBuf::from("/path/to/file.csv"));
        assert!(!S3Storage::new().is_remote_path(&data_source.path)); // Should be detected as local path
        assert!(data_source.schema.is_some());
        assert!(data_source.options.is_some());

        let schema = data_source.schema.unwrap();
        assert_eq!(schema.get("col1"), Some(&"string".to_string()));
        assert_eq!(schema.get("col2"), Some(&"int64".to_string()));

        let options = data_source.options.unwrap();
        assert_eq!(options.get("has_header"), Some(&"true".to_string()));
        assert_eq!(options.get("delimiter"), Some(&",".to_string()));
    }

    #[tokio::test]
    async fn test_register_data_sources() {
        let temp_dir = TempDir::new().unwrap();
        let context_path = create_test_context_file(&temp_dir);

        // Create a dummy SessionContext
        let mut session_ctx = SessionContext::new();

        // Use the data sources from the context file which should have the actual files created
        let mut data_sources = load_context_config(&context_path).unwrap();

        // Fix the paths to be absolute paths in the temp directory
        for data_source in &mut data_sources {
            if data_source.path.is_relative() {
                data_source.path = temp_dir.path().join(&data_source.path);
            }
        }

        // Only test with CSV files since we create dummy parquet content that's not valid
        let csv_data_sources: Vec<_> = data_sources
            .into_iter()
            .filter(|ds| matches!(ds.source_type, DataSourceType::Csv))
            .collect();

        // Test that we can register the data sources
        let result = register_data_sources(&mut session_ctx, &csv_data_sources).await;
        assert!(result.is_ok());
    }

    /// A context YAML with a `documents` source pointed at a 2-page PDF fixture
    /// loads, registers, and is queryable end-to-end through the dispatch arm.
    #[cfg(feature = "documents")]
    #[tokio::test]
    async fn test_register_documents_source_via_context() {
        let temp_dir = TempDir::new().unwrap();
        let docs_dir = temp_dir.path().join("docs");
        fs::create_dir_all(&docs_dir).unwrap();

        // Copy the committed 2-page PDF fixture into the temp source dir.
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../skardi/tests/fixtures/documents/two_pages.pdf");
        fs::copy(&fixture, docs_dir.join("two_pages.pdf")).unwrap();

        let context_content = format!(
            r#"
kind: context
metadata:
  name: docs-context
  version: 1.0.0
spec:
  data_sources:
    - name: "documents"
      type: "documents"
      path: "{}"
      access_mode: read_only
      options:
        recursive: "true"
        include_globs: "*.pdf"
        ocr: "off"
"#,
            docs_dir.display()
        );
        let context_path = temp_dir.path().join("context.yaml");
        fs::write(&context_path, context_content).unwrap();

        let data_sources = load_context_config(&context_path).unwrap();
        let mut session_ctx = SessionContext::new();
        register_data_sources(&mut session_ctx, &data_sources)
            .await
            .expect("documents source should register");

        let batches = session_ctx
            .sql("SELECT count(*) AS n FROM documents")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 2);
    }

    /// `ocr: on` with no `ocr_server_url` must fail at registration (preflight),
    /// not silently produce empty pages mid-scan. This build links liteparse
    /// without the bundled Tesseract engine, so OCR needs an HTTP engine.
    #[cfg(feature = "documents")]
    #[tokio::test]
    async fn test_register_documents_ocr_on_without_engine_errors() {
        let temp_dir = TempDir::new().unwrap();
        let docs_dir = temp_dir.path().join("docs");
        fs::create_dir_all(&docs_dir).unwrap();
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../skardi/tests/fixtures/documents/two_pages.pdf");
        fs::copy(&fixture, docs_dir.join("two_pages.pdf")).unwrap();

        let context_content = format!(
            r#"
kind: context
metadata:
  name: docs-context
  version: 1.0.0
spec:
  data_sources:
    - name: "documents"
      type: "documents"
      path: "{}"
      access_mode: read_only
      options:
        include_globs: "*.pdf"
        ocr: "on"
"#,
            docs_dir.display()
        );
        let context_path = temp_dir.path().join("context.yaml");
        fs::write(&context_path, context_content).unwrap();

        let data_sources = load_context_config(&context_path).unwrap();
        let mut session_ctx = SessionContext::new();
        let result = register_data_sources(&mut session_ctx, &data_sources).await;
        assert!(
            result.is_err(),
            "ocr=on without ocr_server_url should fail registration"
        );
        let msg = format!("{:?}", result.unwrap_err());
        assert!(msg.contains("ocr_server_url"), "unexpected error: {msg}");
    }

    #[test]
    fn test_cli_args_parsing() {
        use clap::Parser;

        // Test with single pipeline file and context
        let args = CliArgs::try_parse_from([
            "skardi-server",
            "--pipeline",
            "/path/to/pipeline.yaml",
            "--ctx",
            "/path/to/context.yaml",
            "--port",
            "9000",
        ])
        .unwrap();

        assert_eq!(
            args.pipeline_path,
            Some(PathBuf::from("/path/to/pipeline.yaml"))
        );
        assert_eq!(args.ctx_file, Some(PathBuf::from("/path/to/context.yaml")));
        assert_eq!(args.port, 9000);

        // Test with pipeline directory
        let args = CliArgs::try_parse_from(["skardi-server", "--pipeline", "/path/to/pipelines/"])
            .unwrap();

        assert_eq!(
            args.pipeline_path,
            Some(PathBuf::from("/path/to/pipelines/"))
        );
        assert_eq!(args.ctx_file, None);
        assert_eq!(args.port, 8080); // default value

        // Test with no pipelines
        let args = CliArgs::try_parse_from(["skardi-server"]).unwrap();

        assert!(args.pipeline_path.is_none());
        assert_eq!(args.ctx_file, None);
        assert_eq!(args.port, 8080); // default value
    }

    #[test]
    fn test_local_path_detection() {
        // Test that local paths are correctly detected
        let yaml_content = r#"
name: "default_test"
type: "parquet"
path: "/path/to/file.parquet"
"#;

        let data_source: DataSource = serde_yaml::from_str(yaml_content).unwrap();

        assert_eq!(data_source.name, "default_test");
        assert!(matches!(data_source.source_type, DataSourceType::Parquet));
        assert!(!S3Storage::new().is_remote_path(&data_source.path)); // Should be detected as local path
        assert_eq!(data_source.path, PathBuf::from("/path/to/file.parquet"));
    }

    #[test]
    fn test_s3_path_detection() {
        // Test that S3 paths are correctly detected
        let yaml_content = r#"
name: "s3_test"
type: "parquet"
path: "s3://bucket/file.parquet"
options:
  has_header: true
  delimiter: ","
"#;

        let data_source: DataSource = serde_yaml::from_str(yaml_content).unwrap();

        assert_eq!(data_source.name, "s3_test");
        assert!(matches!(data_source.source_type, DataSourceType::Parquet));
        assert!(S3Storage::new().is_remote_path(&data_source.path)); // Should be detected as S3 path
        assert_eq!(data_source.path, PathBuf::from("s3://bucket/file.parquet"));
        assert!(data_source.options.is_some());

        // Verify options don't contain AWS configuration
        let options = data_source.options.unwrap();
        assert!(!options.contains_key("aws_region"));
        assert!(!options.contains_key("aws_access_key_id"));
        assert!(!options.contains_key("aws_secret_access_key"));
    }

    #[test]
    fn test_config_error_display() {
        let error = ConfigError::PipelineFileNotFound {
            path: PathBuf::from("/missing/pipeline.yaml"),
        };
        assert_eq!(
            error.to_string(),
            "Pipeline file not found: /missing/pipeline.yaml"
        );

        let error = ConfigError::DuplicateDataSourceName {
            name: "duplicate_source".to_string(),
        };
        assert_eq!(
            error.to_string(),
            "Duplicate data source name: duplicate_source"
        );
    }

    fn dynamodb_source(
        name: &str,
        connection_string: Option<&str>,
        options: Option<HashMap<String, String>>,
        access_mode: AccessMode,
    ) -> DataSource {
        DataSource {
            name: name.to_string(),
            source_type: DataSourceType::Dynamodb,
            path: PathBuf::new(),
            connection_string: connection_string.map(String::from),
            schema: None,
            options,
            hierarchy_level: HierarchyLevel::default(),
            access_mode,
            enable_cache: false,
            description: None,
            open_connector: None,
            rss: None,
            graph: None,
        }
    }

    #[test]
    fn validate_accepts_dynamodb_read_write() {
        let source = dynamodb_source(
            "products",
            Some("http://localhost:8000"),
            None,
            AccessMode::ReadWrite,
        );
        validate_data_sources(&[source]).expect("dynamodb supports read_write");
    }

    #[test]
    fn validate_rejects_dynamodb_without_connection_string() {
        let source = dynamodb_source("products", None, None, AccessMode::ReadOnly);
        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::MissingConnectionString { name } if name == "products"
            ),
            "got {config_err}"
        );
    }

    #[test]
    fn validate_rejects_dynamodb_catalog_empty_allowed_tables() {
        let mut options = HashMap::new();
        options.insert("allowed_tables".to_string(), " , , ".to_string());
        let mut source = dynamodb_source(
            "ddb",
            Some("http://localhost:8000"),
            Some(options),
            AccessMode::ReadOnly,
        );
        source.hierarchy_level = HierarchyLevel::Catalog;

        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::EmptyAllowedTables { name } if name == "ddb"
            ),
            "got {config_err}"
        );
    }

    #[test]
    fn validate_accepts_dynamodb_catalog_allowed_tables() {
        let mut options = HashMap::new();
        options.insert("allowed_tables".to_string(), "products, orders".to_string());
        let mut source = dynamodb_source(
            "ddb",
            Some("http://localhost:8000"),
            Some(options),
            AccessMode::ReadOnly,
        );
        source.hierarchy_level = HierarchyLevel::Catalog;

        validate_data_sources(&[source]).expect("valid DynamoDB catalog allow-list");
    }

    fn open_connector_source(
        name: &str,
        config_yaml: Option<&str>,
        access_mode: AccessMode,
    ) -> DataSource {
        let open_connector = config_yaml
            .map(|yaml| serde_yaml::from_str(yaml).expect("parse open_connector config"));
        DataSource {
            name: name.to_string(),
            source_type: DataSourceType::OpenConnector,
            path: PathBuf::new(),
            connection_string: Some("http://open-connector:3000".to_string()),
            schema: None,
            options: None,
            hierarchy_level: HierarchyLevel::Catalog,
            access_mode,
            enable_cache: false,
            description: None,
            open_connector,
            rss: None,
            graph: None,
        }
    }

    const VALID_OPEN_CONNECTOR_CONFIG: &str = r#"
runtime_token_env: OPEN_CONNECTOR_TOKEN
bindings:
  - name: github_skardi
    source_pack: github
    resource:
      owner: SkardiLabs
      repo: skardi
    tables:
      - issues
"#;

    #[test]
    fn validate_accepts_open_connector_with_typed_config() {
        let source = open_connector_source(
            "saas",
            Some(VALID_OPEN_CONNECTOR_CONFIG),
            AccessMode::ReadOnly,
        );
        validate_data_sources(&[source]).expect("valid open_connector source");
    }

    #[test]
    fn validate_rejects_open_connector_without_typed_config() {
        let source = open_connector_source("saas", None, AccessMode::ReadOnly);
        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::MissingOpenConnectorConfig { name } if name == "saas"
            ),
            "got {config_err}"
        );
    }

    #[test]
    fn validate_rejects_open_connector_config_on_wrong_type() {
        let mut source = dynamodb_source(
            "products",
            Some("http://localhost:8000"),
            None,
            AccessMode::ReadOnly,
        );
        source.open_connector =
            Some(serde_yaml::from_str(VALID_OPEN_CONNECTOR_CONFIG).expect("parse config"));

        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::UnexpectedOpenConnectorConfig { name, source_type }
                    if name == "products" && *source_type == DataSourceType::Dynamodb
            ),
            "got {config_err}"
        );
    }

    #[test]
    fn validate_rejects_open_connector_invalid_config() {
        let source =
            open_connector_source("saas", Some("runtime_token_env: ''"), AccessMode::ReadOnly);
        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::InvalidOpenConnectorConfig { name, .. } if name == "saas"
            ),
            "got {config_err}"
        );
    }

    #[test]
    fn validate_rejects_open_connector_read_write() {
        // Milestone one is strictly read-only; OpenConnector is not in
        // WRITABLE_SOURCE_TYPES.
        let source = open_connector_source(
            "saas",
            Some(VALID_OPEN_CONNECTOR_CONFIG),
            AccessMode::ReadWrite,
        );
        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::UnsupportedWriteMode { name, source_type }
                    if name == "saas" && *source_type == DataSourceType::OpenConnector
            ),
            "got {config_err}"
        );
    }

    #[test]
    fn validate_rejects_open_connector_without_connection_string() {
        let mut source = open_connector_source(
            "saas",
            Some(VALID_OPEN_CONNECTOR_CONFIG),
            AccessMode::ReadOnly,
        );
        source.connection_string = None;

        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::MissingConnectionString { name } if name == "saas"
            ),
            "got {config_err}"
        );
    }

    #[test]
    fn validate_rejects_open_connector_table_hierarchy() {
        // hierarchy_level defaults to Table; a minimal config must fail at
        // validation, not at registration with a wrapped provider error.
        let mut source = open_connector_source(
            "saas",
            Some(VALID_OPEN_CONNECTOR_CONFIG),
            AccessMode::ReadOnly,
        );
        source.hierarchy_level = HierarchyLevel::Table;

        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::OpenConnectorHierarchyRequired { name } if name == "saas"
            ),
            "got {config_err}"
        );
    }

    #[test]
    fn validate_rejects_open_connector_catalog_table_option() {
        // OpenConnector is catalog-only and its tables come from typed
        // bindings; a flat `table` option must fail as loudly as it would on
        // postgres/dynamodb instead of being silently ignored.
        let mut options = HashMap::new();
        options.insert("table".to_string(), "issues".to_string());
        let mut source = open_connector_source(
            "saas",
            Some(VALID_OPEN_CONNECTOR_CONFIG),
            AccessMode::ReadOnly,
        );
        source.options = Some(options);

        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::CatalogModeConflictingOptions { name, option }
                    if name == "saas" && option == "table"
            ),
            "got {config_err}"
        );
    }

    #[test]
    fn validate_rejects_open_connector_catalog_empty_allowed_schemas() {
        let mut options = HashMap::new();
        options.insert("allowed_schemas".to_string(), " , , ".to_string());
        let mut source = open_connector_source(
            "saas",
            Some(VALID_OPEN_CONNECTOR_CONFIG),
            AccessMode::ReadOnly,
        );
        source.options = Some(options);

        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::EmptyAllowedSchemas { name } if name == "saas"
            ),
            "got {config_err}"
        );
    }

    /// A minimal valid `rss:` block. The host is `.invalid` (RFC 2606 §2
    /// reserves it as never-resolvable) so any accidental fetch on a path
    /// these tests exercise would fail loudly rather than reach a real feed.
    const VALID_RSS_CONFIG: &str = r#"
feeds:
  - url: https://feeds.example.invalid/f.xml
    name: example
"#;

    fn rss_source(name: &str, config_yaml: Option<&str>, access_mode: AccessMode) -> DataSource {
        DataSource {
            name: name.to_string(),
            source_type: DataSourceType::Rss,
            path: PathBuf::new(),
            // Deliberately absent: RSS has no connection string — feed URLs
            // live in the typed block — so `Rss` must not be in the
            // connection-string-required arm of `validate_data_sources`.
            connection_string: None,
            schema: None,
            options: None,
            hierarchy_level: HierarchyLevel::Catalog,
            access_mode,
            enable_cache: false,
            description: None,
            open_connector: None,
            rss: config_yaml.map(|yaml| serde_yaml::from_str(yaml).expect("parse rss config")),
            graph: None,
        }
    }

    #[test]
    fn validate_accepts_rss_with_typed_config() {
        // Also pins the omission above: this source has no
        // `connection_string`, and validation must still accept it.
        let source = rss_source("news", Some(VALID_RSS_CONFIG), AccessMode::ReadOnly);
        validate_data_sources(&[source]).expect("valid rss source");
    }

    #[test]
    fn validate_rejects_rss_config_on_wrong_type() {
        let mut source = dynamodb_source(
            "products",
            Some("http://localhost:8000"),
            None,
            AccessMode::ReadOnly,
        );
        source.rss = Some(serde_yaml::from_str(VALID_RSS_CONFIG).expect("parse rss config"));

        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::UnexpectedRssConfig { name, source_type }
                    if name == "products" && *source_type == DataSourceType::Dynamodb
            ),
            "got {config_err}"
        );
    }

    #[test]
    fn validate_rejects_rss_without_typed_config() {
        let source = rss_source("news", None, AccessMode::ReadOnly);
        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::MissingRssConfig { name } if name == "news"
            ),
            "got {config_err}"
        );
    }

    #[test]
    fn validate_rejects_rss_table_hierarchy() {
        // hierarchy_level defaults to Table; a source that omits
        // `hierarchy_level: catalog` must fail at validation rather than at
        // registration with the provider's wrapped CatalogHierarchyRequired.
        let mut source = rss_source("news", Some(VALID_RSS_CONFIG), AccessMode::ReadOnly);
        source.hierarchy_level = HierarchyLevel::Table;

        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::RssHierarchyRequired { name } if name == "news"
            ),
            "got {config_err}"
        );
    }

    #[test]
    fn validate_rejects_rss_read_write() {
        // The RSS provider is read-only, so `Rss` is absent from
        // WRITABLE_SOURCE_TYPES and the generic access_mode gate rejects
        // read_write — there is no RSS-specific check for it.
        let source = rss_source("news", Some(VALID_RSS_CONFIG), AccessMode::ReadWrite);
        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::UnsupportedWriteMode { name, source_type }
                    if name == "news" && *source_type == DataSourceType::Rss
            ),
            "got {config_err}"
        );
    }

    #[test]
    fn validate_rejects_rss_invalid_config() {
        // `feeds` and `opml` are mutually exclusive (RssConfig::validate);
        // validation must surface that as a typed config error.
        let source = rss_source(
            "news",
            Some("feeds:\n  - url: https://a.example.invalid/f.xml\nopml: subs.opml\n"),
            AccessMode::ReadOnly,
        );
        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::InvalidRssConfig { name, reason }
                    if name == "news" && reason.contains("mutually exclusive")
            ),
            "got {config_err}"
        );
    }

    #[test]
    fn validate_rejects_rss_catalog_table_option() {
        // `Rss` joins CATALOG_SUPPORTED_SOURCES, so the existing catalog-mode
        // guard applies: its tables are fixed (`main.feeds`/`main.items`) and
        // a flat `table` option must fail rather than be silently ignored.
        let mut source = rss_source("news", Some(VALID_RSS_CONFIG), AccessMode::ReadOnly);
        source.options = Some(HashMap::from([("table".to_string(), "items".to_string())]));

        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::CatalogModeConflictingOptions { name, option }
                    if name == "news" && option == "table"
            ),
            "got {config_err}"
        );
    }

    /// An Obsidian source: catalog-level, read-only, `path` is the vault
    /// root. No typed config block — the vault is entirely described by
    /// `path` plus the flat `options` map.
    fn obsidian_source(
        name: &str,
        path: &str,
        options: Option<HashMap<String, String>>,
        access_mode: AccessMode,
    ) -> DataSource {
        DataSource {
            name: name.to_string(),
            source_type: DataSourceType::Obsidian,
            path: PathBuf::from(path),
            // Deliberately absent, like RSS: a vault has no connection
            // string, so `Obsidian` must not be in the
            // connection-string-required arm of `validate_data_sources`.
            connection_string: None,
            schema: None,
            options,
            hierarchy_level: HierarchyLevel::Catalog,
            access_mode,
            enable_cache: false,
            description: None,
            open_connector: None,
            rss: None,
            graph: None,
        }
    }

    #[test]
    fn validate_accepts_obsidian_source() {
        // Pins the omission above: no `connection_string`, and validation
        // must still accept it.
        let source = obsidian_source("vault", "/tmp/vault", None, AccessMode::ReadOnly);
        validate_data_sources(&[source]).expect("valid obsidian source");
    }

    #[test]
    fn validate_rejects_obsidian_reserved_catalog_names() {
        // `Obsidian` joins CATALOG_SUPPORTED_SOURCES, so a vault named after
        // a built-in catalog must be refused before `register_catalog` can
        // replace it.
        for reserved in ["datafusion", "information_schema"] {
            let source = obsidian_source(reserved, "/tmp/vault", None, AccessMode::ReadOnly);
            let err = validate_data_sources(&[source]).unwrap_err();
            let config_err = err.downcast_ref::<ConfigError>().unwrap();
            assert!(
                matches!(
                    config_err,
                    ConfigError::ReservedCatalogSourceName { name } if name == reserved
                ),
                "got {config_err}"
            );
        }
    }

    #[test]
    fn validate_rejects_obsidian_catalog_table_option() {
        // The vault's tables are fixed (`main.notes` / `main.links` /
        // `main.tags`), so a flat `table` option must fail rather than be
        // silently ignored.
        let options = HashMap::from([("table".to_string(), "notes".to_string())]);
        let source = obsidian_source("vault", "/tmp/vault", Some(options), AccessMode::ReadOnly);

        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::CatalogModeConflictingOptions { name, option }
                    if name == "vault" && option == "table"
            ),
            "got {config_err}"
        );
    }

    #[test]
    fn validate_rejects_obsidian_read_write() {
        // The Obsidian provider is read-only, so `Obsidian` is absent from
        // WRITABLE_SOURCE_TYPES and the generic access_mode gate rejects
        // read_write — as with rss, there is no obsidian-specific check.
        let source = obsidian_source("vault", "/tmp/vault", None, AccessMode::ReadWrite);
        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::UnsupportedWriteMode { name, source_type }
                    if name == "vault" && *source_type == DataSourceType::Obsidian
            ),
            "got {config_err}"
        );
    }

    /// A minimal valid `graph:` block with one view. The URL points at a
    /// closed port — validation is pure (no network I/O), so it passes
    /// here; reachability is registration's concern.
    const VALID_GRAPH_CONFIG: &str = r#"
backend: age
graph_name: knowledge
views:
  - name: user_posts
    cypher: MATCH (u:User) RETURN u.name AS user_name
    schema:
      - name: user_name
        type: string
"#;

    fn graph_source(name: &str, config_yaml: Option<&str>, access_mode: AccessMode) -> DataSource {
        DataSource {
            name: name.to_string(),
            source_type: DataSourceType::Graph,
            path: PathBuf::new(),
            connection_string: Some("postgres://127.0.0.1:1/none".to_string()),
            schema: None,
            options: None,
            hierarchy_level: HierarchyLevel::Catalog,
            access_mode,
            enable_cache: false,
            description: None,
            open_connector: None,
            rss: None,
            graph: config_yaml.map(|yaml| serde_yaml::from_str(yaml).expect("parse graph config")),
        }
    }

    #[test]
    fn validate_accepts_graph_with_typed_config() {
        let source = graph_source("kg", Some(VALID_GRAPH_CONFIG), AccessMode::ReadOnly);
        validate_data_sources(&[source]).expect("valid graph source");
    }

    #[test]
    fn validate_rejects_graph_without_typed_config() {
        let source = graph_source("kg", None, AccessMode::ReadOnly);
        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::MissingGraphConfig { name } if name == "kg"
            ),
            "got {config_err}"
        );
    }

    #[test]
    fn validate_rejects_graph_config_on_wrong_type() {
        let mut source = dynamodb_source(
            "products",
            Some("http://localhost:8000"),
            None,
            AccessMode::ReadOnly,
        );
        source.graph = Some(serde_yaml::from_str(VALID_GRAPH_CONFIG).expect("parse graph config"));

        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::UnexpectedGraphConfig { name, source_type }
                    if name == "products" && *source_type == DataSourceType::Dynamodb
            ),
            "got {config_err}"
        );
    }

    #[test]
    fn validate_rejects_graph_table_hierarchy() {
        // hierarchy_level defaults to Table; a minimal config must fail at
        // validation, not at registration with the provider's wrapped error.
        let mut source = graph_source("kg", Some(VALID_GRAPH_CONFIG), AccessMode::ReadOnly);
        source.hierarchy_level = HierarchyLevel::Table;

        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::GraphHierarchyRequired { name } if name == "kg"
            ),
            "got {config_err}"
        );
    }

    #[test]
    fn validate_rejects_graph_invalid_config() {
        // `backend: neo4j` is a later milestone; GraphConfig::validate names
        // it, and validation must surface that as a typed config error.
        let source = graph_source(
            "kg",
            Some("backend: neo4j\ngraph_name: knowledge\n"),
            AccessMode::ReadOnly,
        );
        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::InvalidGraphConfig { name, reason }
                    if name == "kg" && reason.contains("not supported")
            ),
            "got {config_err}"
        );
    }

    #[test]
    fn validate_rejects_graph_without_connection_string() {
        let mut source = graph_source("kg", Some(VALID_GRAPH_CONFIG), AccessMode::ReadOnly);
        source.connection_string = None;

        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::MissingConnectionString { name } if name == "kg"
            ),
            "got {config_err}"
        );
    }

    #[test]
    fn validate_rejects_graph_read_write() {
        // The graph milestone is read-only, so `Graph` is absent from
        // WRITABLE_SOURCE_TYPES and the generic access_mode gate rejects
        // read_write — there is no graph-specific check for it.
        let source = graph_source("kg", Some(VALID_GRAPH_CONFIG), AccessMode::ReadWrite);
        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::UnsupportedWriteMode { name, source_type }
                    if name == "kg" && *source_type == DataSourceType::Graph
            ),
            "got {config_err}"
        );
    }

    /// A `type: rss` source in a build without the `rss` feature must fail at
    /// registration with a message naming the feature — the typed `rss:` block
    /// still parses (RssConfig compiles featureless), so the failure is a
    /// build-capability error, not a serde error.
    #[cfg(not(feature = "rss"))]
    #[tokio::test]
    async fn test_register_rss_source_without_feature_names_the_feature() {
        let source = rss_source("news", Some(VALID_RSS_CONFIG), AccessMode::ReadOnly);
        let mut session_ctx = SessionContext::new();
        let err = register_data_sources(&mut session_ctx, &[source])
            .await
            .unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("`rss` feature"), "unexpected error: {msg}");
    }

    /// End-to-end through a real `kind: context` file: parse, validate,
    /// register, query. Zero network — registration performs no I/O beyond an
    /// `opml:` path (unused here), and a `feeds` scan serves a synchronous
    /// state row with no request at all (`rss/exec.rs:322-327`), so the
    /// unreachable `.invalid` host is never contacted.
    #[cfg(feature = "rss")]
    #[tokio::test]
    async fn test_register_rss_source_via_context() {
        let temp_dir = TempDir::new().unwrap();
        let context_content = r#"
kind: context
metadata:
  name: rss-context
  version: 1.0.0
spec:
  data_sources:
    - name: "news"
      type: "rss"
      hierarchy_level: catalog
      access_mode: read_only
      rss:
        feeds:
          - url: "https://feeds.example.invalid/f.xml"
"#;
        let context_path = temp_dir.path().join("context.yaml");
        fs::write(&context_path, context_content).unwrap();

        let data_sources = load_context_config(&context_path).unwrap();
        assert!(matches!(data_sources[0].source_type, DataSourceType::Rss));

        let mut session_ctx = SessionContext::new();
        register_data_sources(&mut session_ctx, &data_sources)
            .await
            .expect("rss source should register");

        let batches = session_ctx
            .sql("SELECT name, url, last_status FROM news.main.feeds")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "one subscription → one feeds row");

        let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
        let col = |i: usize| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .value(0)
                .to_string()
        };
        // `name` defaults to the URL when the subscription omits it.
        assert_eq!(col(0), "https://feeds.example.invalid/f.xml");
        assert_eq!(col(1), "https://feeds.example.invalid/f.xml");
        assert_eq!(col(2), "never");
    }

    /// The checked-in vault the registration tests below read. Relative to
    /// `crates/server`, so it resolves the same in every CI checkout.
    #[cfg(feature = "obsidian")]
    const OBSIDIAN_FIXTURE_VAULT: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../skardi/src/sources/providers/obsidian/fixtures/vault"
    );

    /// A `type: obsidian` source in a build without the `obsidian` feature
    /// must fail at registration with a message naming the feature — the
    /// config parses fine (nothing in the block is feature-gated), so the
    /// failure is a build-capability error, not a serde one.
    #[cfg(not(feature = "obsidian"))]
    #[tokio::test]
    async fn test_register_obsidian_source_without_feature_names_the_feature() {
        let source = obsidian_source("vault", "/tmp/vault", None, AccessMode::ReadOnly);
        let mut session_ctx = SessionContext::new();
        let err = register_data_sources(&mut session_ctx, &[source])
            .await
            .unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("`obsidian` feature"), "unexpected error: {msg}");
    }

    /// Registration through the server's own entry point, then a query
    /// against the catalog it registered. Zero network: the fixture vault is
    /// a local directory.
    #[cfg(feature = "obsidian")]
    #[tokio::test]
    async fn test_register_obsidian_source_and_query_notes() {
        let source = obsidian_source(
            "vault",
            OBSIDIAN_FIXTURE_VAULT,
            None,
            AccessMode::ReadOnly,
        );
        let mut session_ctx = SessionContext::new();
        register_data_sources(&mut session_ctx, &[source])
            .await
            .expect("obsidian source should register");

        let batches = session_ctx
            .sql("SELECT count(*) FROM vault.main.notes")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        // 12 `.md` files under the fixture vault; `.obsidian/` and `.trash/`
        // are excluded by the provider.
        assert_eq!(count, 12);
    }

    /// `access_mode: read_write` never reaches the tables: the provider is
    /// the single enforcement point, so registration fails even though this
    /// path skips `validate_data_sources` (which would reject it earlier via
    /// WRITABLE_SOURCE_TYPES).
    #[cfg(feature = "obsidian")]
    #[tokio::test]
    async fn test_register_obsidian_read_write_fails() {
        let source = obsidian_source(
            "vault",
            OBSIDIAN_FIXTURE_VAULT,
            None,
            AccessMode::ReadWrite,
        );
        let mut session_ctx = SessionContext::new();
        let err = register_data_sources(&mut session_ctx, &[source])
            .await
            .unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("read-only"), "unexpected error: {msg}");
    }

    #[test]
    fn validate_rejects_dynamodb_catalog_table_option() {
        let mut options = HashMap::new();
        options.insert("table".to_string(), "products".to_string());
        let mut source = dynamodb_source(
            "ddb",
            Some("http://localhost:8000"),
            Some(options),
            AccessMode::ReadOnly,
        );
        source.hierarchy_level = HierarchyLevel::Catalog;

        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::CatalogModeConflictingOptions { name, option }
                    if name == "ddb" && option == "table"
            ),
            "got {config_err}"
        );
    }

    fn clickhouse_source(
        name: &str,
        options: Option<HashMap<String, String>>,
        access_mode: AccessMode,
    ) -> DataSource {
        DataSource {
            name: name.to_string(),
            source_type: DataSourceType::Clickhouse,
            path: PathBuf::new(),
            connection_string: Some("http://localhost:8123".to_string()),
            schema: None,
            options,
            open_connector: None,
            rss: None,
            graph: None,
            hierarchy_level: HierarchyLevel::default(),
            access_mode,
            enable_cache: false,
            description: None,
        }
    }

    #[test]
    fn validate_rejects_clickhouse_read_write() {
        let mut options = HashMap::new();
        options.insert("table".to_string(), "events".to_string());
        let source = clickhouse_source("events", Some(options), AccessMode::ReadWrite);
        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::UnsupportedWriteMode { name, source_type }
                    if name == "events" && *source_type == DataSourceType::Clickhouse
            ),
            "got {config_err}"
        );
    }

    #[test]
    fn validate_accepts_clickhouse_table_mode_with_database_option() {
        let mut options = HashMap::new();
        options.insert("table".to_string(), "events".to_string());
        options.insert("database".to_string(), "analytics".to_string());
        let source = clickhouse_source("events", Some(options), AccessMode::ReadOnly);
        validate_data_sources(&[source]).expect("table mode accepts a database option");
    }

    #[test]
    fn validate_rejects_clickhouse_catalog_database_option() {
        // "database" is ClickHouse's schema-analog option; letting it through
        // in catalog mode would silently change the pool's default database.
        let mut options = HashMap::new();
        options.insert("database".to_string(), "analytics".to_string());
        let mut source = clickhouse_source("ch", Some(options), AccessMode::ReadOnly);
        source.hierarchy_level = HierarchyLevel::Catalog;

        let err = validate_data_sources(&[source]).unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::CatalogModeConflictingOptions { name, option }
                    if name == "ch" && option == "database"
            ),
            "got {config_err}"
        );
    }

    #[test]
    fn unsupported_write_mode_error_lists_dynamodb() {
        let err = ConfigError::UnsupportedWriteMode {
            name: "events".to_string(),
            source_type: DataSourceType::Csv,
        };
        assert!(err.to_string().contains("'dynamodb'"), "got {err}");
    }

    #[tokio::test]
    async fn register_dynamodb_without_connection_string_errors() {
        let mut session_ctx = SessionContext::new();
        let source = dynamodb_source("products", None, None, AccessMode::ReadOnly);
        let err = register_data_sources(&mut session_ctx, &[source])
            .await
            .unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        assert!(
            matches!(
                config_err,
                ConfigError::MissingConnectionString { name } if name == "products"
            ),
            "got {config_err}"
        );
    }

    #[tokio::test]
    async fn register_dynamodb_without_options_reports_registration_failure() {
        let mut session_ctx = SessionContext::new();
        // `register_dynamodb_tables` fails before any network call when no
        // options are provided, so this exercises the registration arm and
        // its error mapping without a live endpoint.
        let source = dynamodb_source(
            "products",
            Some("http://localhost:8000"),
            None,
            AccessMode::ReadOnly,
        );
        let err = register_data_sources(&mut session_ctx, &[source])
            .await
            .unwrap_err();
        let config_err = err.downcast_ref::<ConfigError>().unwrap();
        match config_err {
            ConfigError::DataSourceRegistrationFailed { name, error } => {
                assert_eq!(name, "products");
                assert!(error.contains("requires options"), "got {error}");
            }
            other => panic!("expected DataSourceRegistrationFailed, got {other}"),
        }
    }
}
