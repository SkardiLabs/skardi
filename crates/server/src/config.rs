use anyhow::{Context, Result};
use clap::Parser;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use pipeline::pipeline::{Pipeline, StandardPipeline};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;

use crate::remote_storage::{RemoteStorage, S3Storage};
pub use source::AccessMode;

/// CLI arguments for the Skardi server
#[derive(Parser, Debug)]
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

    /// Path to context YAML configuration file (optional)
    #[arg(
        long = "ctx",
        help = "Path to context YAML configuration file (optional)"
    )]
    pub ctx_file: Option<PathBuf>,

    /// Server port number
    #[arg(long, default_value = "8080", help = "Server port number")]
    pub port: u16,
}

/// Main server configuration containing pipelines and data sources
#[derive(Debug)]
pub struct ServerConfig {
    /// Loaded pipeline definitions keyed by name (can be registered later via API)
    pub pipelines: HashMap<String, StandardPipeline>,
    /// Data sources to register with DataFusion
    pub data_sources: Vec<DataSource>,
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
    /// Access mode: read_only (default) or read_write
    #[serde(default)]
    pub access_mode: AccessMode,
    /// If true, load the entire table into memory at startup (only for Csv, Parquet, Iceberg)
    #[serde(default)]
    pub enable_cache: bool,
}

/// Supported data source types
#[derive(Debug, Clone, Deserialize, Serialize, Hash, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DataSourceType {
    Csv,
    Parquet,
    Postgres,
    Mysql,
    Iceberg,
    Mongo,
    Lance,
}

/// Context configuration file structure
#[derive(Debug, Deserialize)]
struct ContextConfig {
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

    #[error("S3 path must start with 's3://' prefix: {path}")]
    InvalidS3Path { path: String },

    #[error("Missing required AWS configuration for S3 data source: {name} - missing {field}")]
    MissingAwsConfig { name: String, field: String },

    #[error("S3 object store registration failed: {name} - {error}")]
    S3ObjectStoreRegistrationFailed { name: String, error: String },

    #[error("Data source '{name}' has access_mode 'read_write' but type '{source_type:?}' does not support write operations. Only 'postgres' and 'mysql' sources support read_write mode.")]
    UnsupportedWriteMode {
        name: String,
        source_type: DataSourceType,
    },

    #[error("DDL operation not allowed: {operation} on data source '{table_name}'. DDL operations (CREATE, DROP, ALTER, etc.) are not permitted.")]
    DdlOperationNotAllowed {
        operation: String,
        table_name: String,
    },

    #[error("Write operation not allowed on data source '{table_name}'. The data source is configured with 'read_only' access mode. Set access_mode to 'read_write' to enable write operations.")]
    WriteOperationNotAllowed { table_name: String },
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

            if file_path.is_file() {
                if let Some(ext) = file_path.extension() {
                    let ext = ext.to_string_lossy().to_lowercase();
                    if ext == "yaml" || ext == "yml" {
                        tracing::debug!("Found pipeline file: {:?}", file_path);
                        pipeline_files.push(file_path);
                    }
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

/// Load complete server configuration from CLI arguments
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
    let optimizer_registry = Arc::new(crate::optimizer_registry::OptimizerRegistry::new());

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
    register_onnx_predict_udf(&mut session_ctx);

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

    tracing::info!(
        "Configuration loaded successfully: pipelines={}, data_sources={}",
        pipelines.len(),
        data_sources.len()
    );

    Ok(ServerConfig {
        pipelines,
        data_sources,
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
    struct MinimalPipeline {
        metadata: PipelineMetadata,
        query: String,
    }

    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read pipeline file: {:?}", path))?;

    let pipeline: MinimalPipeline = serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse pipeline YAML: {:?}", path))?;

    Ok((pipeline.metadata.name, pipeline.query))
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
pub fn register_onnx_predict_udf(ctx: &mut SessionContext) {
    let registry = Arc::new(model::OnnxModelRegistry::new());
    registry.register_onnx_predict_udf(ctx);
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

    let context_config: ContextConfig =
        serde_yaml::from_str(&content).map_err(|e| ConfigError::InvalidContextYaml {
            error: e.to_string(),
        })?;

    // Validate data sources
    validate_data_sources(&context_config.data_sources)?;

    tracing::info!(
        "Loaded {} data sources from context",
        context_config.data_sources.len()
    );

    Ok(context_config.data_sources)
}

/// Data source types that support read_write access mode
const WRITABLE_SOURCE_TYPES: &[DataSourceType] = &[
    DataSourceType::Postgres,
    DataSourceType::Mysql,
    DataSourceType::Mongo,
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
                source_type: source.source_type.clone(),
            }
            .into());
        }

        match (&source.source_type, s3_storage.is_remote_path(&source.path)) {
            (DataSourceType::Csv | DataSourceType::Parquet | DataSourceType::Lance, true) => {
                // Validate S3 configuration for S3 paths
                s3_storage.validate_configuration(source)?;
            }
            (DataSourceType::Postgres | DataSourceType::Mysql | DataSourceType::Mongo, false) => {
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

/// Validate pipeline SQL against data source access modes
fn validate_pipeline_sql(
    pipeline_name: &str,
    sql: &str,
    data_sources: &[DataSource],
) -> Result<()> {
    use source::sql_validator::{validate_sql, SqlValidatorConfig};

    // Build validator config from data sources
    let mut validator_config = SqlValidatorConfig::new();
    for ds in data_sources {
        let mode = if ds.access_mode.is_read_write() {
            source::sql_validator::AccessMode::ReadWrite
        } else {
            source::sql_validator::AccessMode::ReadOnly
        };
        validator_config = validator_config.with_table(&ds.name, mode);
    }

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
        (DataSourceType::Csv | DataSourceType::Parquet | DataSourceType::Lance, false) => {
            // For local files, verify the file exists
            if !source.path.exists() {
                return Err(ConfigError::DataSourceFileNotFound {
                    name: source.name.clone(),
                    path: source.path.clone(),
                }
                .into());
            }
        }
        (DataSourceType::Csv | DataSourceType::Parquet | DataSourceType::Lance, true) => {
            // For S3 files, validate S3 configuration and setup object store
            s3_storage.validate_configuration(source)?;
            let s3_path = source.path.to_str().unwrap_or("");
            s3_storage
                .setup_object_store(session_ctx, &source.name, s3_path)
                .await?;
        }
        (DataSourceType::Postgres | DataSourceType::Mysql | DataSourceType::Mongo, _) => {
            // Database sources don't need file path validation
        }
        (DataSourceType::Iceberg, _) => {
            // Validation happened during data source registration
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
                if let Some(delimiter) = options.get("delimiter") {
                    if let Some(delimiter_char) = delimiter.chars().next() {
                        csv_read_options = csv_read_options.delimiter(delimiter_char as u8);
                    }
                }
                if let Some(schema_infer_max) = options.get("schema_infer_max_records") {
                    if let Ok(max_records) = schema_infer_max.parse::<usize>() {
                        csv_read_options = csv_read_options.schema_infer_max_records(max_records);
                    }
                }
            }

            // Register the CSV file as a table
            session_ctx
                .register_csv(
                    &source.name,
                    source.path.to_str().unwrap(),
                    csv_read_options,
                )
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

            // Register the Parquet file as a table
            session_ctx
                .register_parquet(
                    &source.name,
                    source.path.to_str().unwrap(),
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

            // Register PostgreSQL table using the providers module
            source::providers::postgres::register_postgres_tables(
                session_ctx,
                &source.name,
                connection_string,
                source.options.as_ref(),
                source.access_mode.is_read_write(),
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

            source::providers::mysql::register_mysql_tables(
                session_ctx,
                &source.name,
                connection_string,
                source.options.as_ref(),
                source.access_mode.is_read_write(),
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

            source::providers::iceberg::register_iceberg_table(
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

            source::providers::mongo::register_mongo_tables(
                session_ctx,
                &source.name,
                connection_string,
                source.options.as_ref(),
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
        DataSourceType::Lance => {
            tracing::info!(
                "Registering Lance dataset: {} at {:?}",
                source.name,
                source.path
            );

            // Get the dataset registry if optimizer registry is provided
            let dataset_registry = optimizer_registry.map(|reg| reg.lance_datasets());

            // Register Lance dataset using the providers module
            source::providers::lance::register_lance_table(
                session_ctx,
                &source.name,
                source.path.to_str().unwrap(),
                dataset_registry.as_ref(),
            )
            .await
            .map_err(|e| ConfigError::DataSourceRegistrationFailed {
                name: source.name.clone(),
                error: e.to_string(),
            })?;
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
metadata:
  name: "test-pipeline"
  version: "1.0.0"
  description: "Test pipeline for configuration testing"

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
metadata:
  name: "test-pipeline"
  version: "1.0.0"
  description: "Simple test pipeline without external table dependencies"

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
            ctx_file: Some(context_path),
            port: 8080,
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
            ctx_file: None,
            port: 3000,
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
metadata:
  name: "test-pipeline"
  version: "1.0.0"
  description: "First test pipeline"

query: |
  SELECT 1 as id, 'test' as name
"#;
        fs::write(pipelines_dir.join("first-pipeline.yaml"), pipeline1_content).unwrap();

        // Create second pipeline with different name
        let pipeline2_content = r#"
metadata:
  name: "second-pipeline"
  version: "2.0.0"
  description: "Second test pipeline"

query: |
  SELECT 2 as id, 'test2' as name
"#;
        fs::write(pipelines_dir.join("second-pipeline.yml"), pipeline2_content).unwrap();

        let args = CliArgs {
            pipeline_path: Some(pipelines_dir),
            ctx_file: None,
            port: 3000,
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
            ctx_file: None,
            port: 8080,
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
            ctx_file: None,
            port: 8080,
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
        assert!(result[0]
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .contains("a_pipeline"));
        assert!(result[1]
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .contains("b_pipeline"));
        assert!(result[2]
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .contains("c_pipeline"));
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

    #[test]
    fn test_cli_args_parsing() {
        use clap::Parser;

        // Test with single pipeline file and context
        let args = CliArgs::try_parse_from(&[
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
        let args = CliArgs::try_parse_from(&["skardi-server", "--pipeline", "/path/to/pipelines/"])
            .unwrap();

        assert_eq!(
            args.pipeline_path,
            Some(PathBuf::from("/path/to/pipelines/"))
        );
        assert_eq!(args.ctx_file, None);
        assert_eq!(args.port, 8080); // default value

        // Test with no pipelines
        let args = CliArgs::try_parse_from(&["skardi-server"]).unwrap();

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
}
