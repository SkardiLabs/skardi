use anyhow::{Context, Result};
use clap::Parser;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use skardi::jobs::JobDefinition;
use skardi::pipeline::pipeline::{Pipeline, StandardPipeline};
use skardi::sources::providers::clickhouse::register_clickhouse_tables;
use skardi::sources::providers::dynamodb::register_dynamodb_tables;
use skardi::sources::providers::iceberg::register_iceberg_table;
use skardi::sources::providers::influxdb::register_influxdb_tables;
use skardi::sources::providers::lance::register_lance_table;
use skardi::sources::providers::mongo::register_mongo_tables;
use skardi::sources::providers::mysql::register_mysql_tables;
use skardi::sources::providers::open_connector::{
    OpenConnectorConfig, register_open_connector_tables,
};
use skardi::sources::providers::redis::datasource::register_redis_tables;
use skardi::sources::providers::seekdb::register_seekdb_tables;
use skardi::sources::providers::sqlite::register_sqlite_tables;
use skardi::sources::providers::sqlx::postgres::register_postgres_tables;
use skardi::sources::sql_validator::{AdhocSqlPolicy, SqlValidatorConfig, validate_sql};
use skardi::util::json_pack::register_json_pack_udf;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;

use crate::OptimizerRegistry;
use crate::remote_storage::{RemoteStorage, S3Storage};
use crate::semantics::{SemanticsRegistry, resolve_semantics_source};
pub use skardi::config::{ConfigError, DataSource, validator_config_from_sources};
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
    #[arg(
        long = "query-audit-db",
        help = "Record /query statements in this SQLite audit ledger (off by default; created 0600)"
    )]
    pub query_audit_db: Option<PathBuf>,

    /// Delete audit records older than this many days, at startup and hourly
    /// thereafter. Unset means keep everything. Ignored without
    /// `--query-audit-db`.
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

// `DataSource` and the `kind: context` envelope now live in `skardi::config`
// (re-exported above), so this crate and the control plane share one model and
// one parser. See `load_context_config` / `extract_pipeline_sql`, which parse
// through `skardi::config::parse_context` / `parse_pipeline`.

// `ConfigError` moved to `skardi::config` (re-exported above) so the shared
// parse/structural validation and this crate's registration paths raise the same
// error type.

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

    // Register data sources with optimizer registry support
    register_data_sources_with_registry(&mut session_ctx, &data_sources, &optimizer_registry)
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

/// Extract SQL query from pipeline file for early validation.
/// Reads the file, then parses just `metadata.name` + `spec.query` through the
/// shared `skardi::config::parse_pipeline` so the server and the control plane
/// agree on the pipeline envelope.
fn extract_pipeline_sql(path: &Path) -> Result<(String, String)> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read pipeline file: {:?}", path))?;

    let pipeline = skardi::config::parse_pipeline(&content)
        .with_context(|| format!("Failed to parse pipeline YAML: {:?}", path))?;

    Ok((pipeline.name, pipeline.sql))
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

    // Read and parse YAML. The `kind: context` envelope parse is shared with the
    // control plane via `skardi::config::parse_context` (strict kind check: a
    // misfiled pipeline/job is rejected rather than silently loaded as an empty
    // context).
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read context file: {:?}", path))?;

    let data_sources = skardi::config::parse_context(&content)?;

    // Validate data sources
    validate_data_sources(&data_sources)?;

    tracing::info!("Loaded {} data sources from context", data_sources.len());

    Ok(data_sources)
}

/// Validate data source configurations.
///
/// The credential-free structural checks — unique names, access-mode/type
/// compatibility, Open Connector config, catalog-mode option conflicts, and a
/// connection string for database sources — are shared with the control plane in
/// [`skardi::config::validate_source_decls`]. This function adds the one check
/// that needs this crate's remote-storage layer: S3 credential configuration for
/// remote file-backed sources.
fn validate_data_sources(data_sources: &[DataSource]) -> Result<()> {
    tracing::debug!("Validating {} data sources", data_sources.len());

    skardi::config::validate_source_decls(data_sources)?;

    let s3_storage = S3Storage::new();
    for source in data_sources {
        if matches!(
            source.source_type,
            DataSourceType::Csv | DataSourceType::Parquet | DataSourceType::Lance
        ) && s3_storage.is_remote_path(&source.path)
        {
            s3_storage.validate_configuration(source)?;
        }
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

// `validator_config_from_sources` (the access-mode map builder) now lives in
// `skardi::config` and is re-exported above — it is the same map the shared SQL
// validator consumes.

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

/// Register data sources with DataFusion SessionContext and OptimizerRegistry
pub async fn register_data_sources_with_registry(
    session_ctx: &mut SessionContext,
    data_sources: &[DataSource],
    optimizer_registry: &Arc<crate::optimizer_registry::OptimizerRegistry>,
) -> Result<()> {
    tracing::info!(
        "Registering {} data sources with DataFusion and optimizer registry",
        data_sources.len()
    );

    for source in data_sources {
        register_data_source(session_ctx, source, Some(optimizer_registry))
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
