mod alias;
mod alias_store;
mod pipeline;

use alias::{AliasDef, resolve_alias};
use alias_store::{AliasMap, resolve_aliases_path};
use anyhow::{Context, Result};
use arrow::util::pretty::pretty_format_batches;
use async_trait::async_trait;
use clap::{Parser, Subcommand};
use datafusion::catalog::UrlTableFactory;
use datafusion::common::ScalarValue;
use datafusion::datasource::TableProvider;
use datafusion::datasource::dynamic_file::DynamicListTableFactory;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::*;
use datafusion_catalog::DynamicFileCatalog;
use datafusion_session::SessionStore;
use lance::dataset::Dataset;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::http::HttpBuilder;
use pipeline::{
    discover_pipelines, extract_param_names, parse_param_flag, render_sql_with_inline_params,
};
use serde::Deserialize;
#[cfg(feature = "candle")]
use skardi::model::CandleModelRegistry;
#[cfg(feature = "gguf")]
use skardi::model::GgufModelRegistry;
#[cfg(feature = "onnx")]
use skardi::model::OnnxModelRegistry;
#[cfg(feature = "remote-embed")]
use skardi::model::RemoteEmbedRegistry;
use skardi::sources::HierarchyLevel;
use skardi::sources::providers::lance::fts_table_function::register_lance_fts_udtf;
use skardi::sources::providers::lance::knn_table_function::register_lance_knn_udtf;
use skardi::sources::providers::mongo::fts_table_function::register_mongo_fts_udtf;
use skardi::sources::providers::sqlx::{register_pg_fts_udtf, register_pg_knn_udtf};
use skardi::sources::providers::{
    DatasetRegistry,
    iceberg::register_iceberg_table,
    lance::register_lance_table,
    mongo::register_mongo_tables,
    mysql::register_mysql_tables,
    sqlite::{
        register_sqlite_fts_udtf, register_sqlite_knn_udtf, register_sqlite_tables,
        register_vec_to_binary_udf,
    },
    sqlx::postgres::register_postgres_tables,
};
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use url::Url;

#[derive(Parser)]
#[command(name = "skardi")]
#[command(about = "CLI tool for managing Skardi pipelines and data sources", long_about = None)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Execute a SQL query or show schema for registered data sources
    #[command(name = "query")]
    Query {
        /// Path to context YAML (default: SKARDICONFIG env or ~/.skardi/config/ctx.yaml)
        #[arg(long)]
        ctx: Option<PathBuf>,
        /// Show schema instead of running a query (use with --all or -t TABLE)
        #[arg(long = "schema")]
        schema: bool,
        /// With --schema: show schemas for all tables
        #[arg(long = "all")]
        all: bool,
        /// With --schema: show schema for this table
        #[arg(short = 't', long = "table")]
        table: Option<String>,
        /// SQL query to execute (use --file for long queries)
        #[arg(short = 'e', long = "sql")]
        sql: Option<String>,
        /// Path to .sql file to execute (takes precedence over --sql)
        #[arg(short = 'f', long = "file")]
        file: Option<PathBuf>,
    },
    /// Execute a pipeline YAML by name with named parameters
    #[command(name = "run")]
    Run {
        /// Pipeline name (from `metadata.name` in the YAML)
        pipeline: String,
        /// Bind a parameter: NAME=VALUE or NAME:TYPE=VALUE (types: str, int, float, bool)
        #[arg(short = 'p', long = "param", value_name = "NAME=VALUE")]
        params: Vec<String>,
        /// Path to context YAML (default: SKARDICONFIG env or ~/.skardi/config/ctx.yaml)
        #[arg(long)]
        ctx: Option<PathBuf>,
        /// Override pipeline discovery directory (else uses ctx `pipelines_dir` or CWD)
        #[arg(long = "pipeline-dir", value_name = "DIR")]
        pipeline_dir: Option<PathBuf>,
    },
    /// Manage CLI aliases (alias a short verb → `run <pipeline>`)
    #[command(name = "alias")]
    Alias {
        #[command(subcommand)]
        cmd: AliasCmd,
    },
    /// Any unknown subcommand is looked up as a user-defined alias and
    /// dispatched to `run <pipeline>` with the alias's parameter bindings.
    #[command(external_subcommand)]
    External(Vec<String>),
}

#[derive(Subcommand)]
enum AliasCmd {
    /// Add a new alias (or overwrite an existing one with --force)
    Add {
        /// Short verb, e.g. `grep`
        name: String,
        /// Pipeline name this alias invokes
        #[arg(long)]
        pipeline: String,
        /// Positional pipeline-param names (comma-separated), e.g. `query` or `query,text_query`
        #[arg(long, value_name = "NAME[,NAME...]")]
        positional: Option<String>,
        /// Default param value: NAME=VALUE (may contain `{other_param}` references)
        #[arg(short = 'd', long = "default", value_name = "NAME=VALUE")]
        defaults: Vec<String>,
        /// Optional short description
        #[arg(long)]
        description: Option<String>,
        /// Overwrite if the alias already exists
        #[arg(long)]
        force: bool,
        /// Override aliases file path
        #[arg(long)]
        aliases: Option<PathBuf>,
        /// Ctx file used to derive default aliases file location
        #[arg(long)]
        ctx: Option<PathBuf>,
        /// Pipeline discovery dir (else uses ctx `pipelines_dir`). Used to
        /// validate positional/default names against the pipeline's real
        /// `{param}` placeholders.
        #[arg(long = "pipeline-dir", value_name = "DIR")]
        pipeline_dir: Option<PathBuf>,
        /// Skip pipeline lookup — save the alias without validating
        /// positional/default names against the pipeline YAML. Useful when
        /// the target pipeline is not yet on disk.
        #[arg(long)]
        no_validate: bool,
    },
    /// List all known aliases
    List {
        #[arg(long)]
        aliases: Option<PathBuf>,
        #[arg(long)]
        ctx: Option<PathBuf>,
    },
    /// Show one alias in YAML form
    Show {
        name: String,
        #[arg(long)]
        aliases: Option<PathBuf>,
        #[arg(long)]
        ctx: Option<PathBuf>,
    },
    /// Remove an alias
    Remove {
        name: String,
        #[arg(long)]
        aliases: Option<PathBuf>,
        #[arg(long)]
        ctx: Option<PathBuf>,
    },
}

#[derive(Debug, Deserialize)]
struct LocalContextConfig {
    data_sources: Vec<LocalDataSource>,
    /// Directory containing pipeline YAMLs for `skardi run`. Relative paths
    /// are resolved against the ctx file's parent directory.
    #[serde(default)]
    pipelines_dir: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct LocalDataSource {
    name: String,
    #[serde(rename = "type")]
    source_type: String,
    #[serde(default)]
    path: Option<String>,
    connection_string: Option<String>,
    options: Option<HashMap<String, String>>,
    #[serde(default)]
    hierarchy_level: HierarchyLevel,
    /// "read" (default) or "read_write". Currently honored by the SQLite source.
    #[serde(default)]
    access_mode: Option<String>,
}

impl LocalDataSource {
    fn is_read_write(&self) -> bool {
        matches!(
            self.access_mode.as_deref(),
            Some("read_write") | Some("readwrite") | Some("rw")
        )
    }
}

fn resolve_ctx_path(override_path: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(p) = override_path {
        return Ok(p);
    }
    if let Ok(env_path) = std::env::var("SKARDICONFIG") {
        return Ok(PathBuf::from(env_path));
    }
    let home = dirs::home_dir().ok_or_else(|| {
        anyhow::anyhow!("Could not determine home directory for default ctx path")
    })?;
    Ok(home.join(".skardi").join("config").join("ctx.yaml"))
}

/// Check if a path string refers to a remote object store location.
fn is_remote_path(path: &str) -> bool {
    path.starts_with("s3://")
        || path.starts_with("gs://")
        || path.starts_with("gcs://")
        || path.starts_with("az://")
        || path.starts_with("azure://")
        || path.starts_with("abfs://")
        || path.starts_with("abfss://")
        || path.starts_with("http://")
        || path.starts_with("https://")
        || path.starts_with("oss://")
        || path.starts_with("cos://")
}

/// Register an object store for a remote URL with the session context.
/// Credentials are read from standard environment variables.
fn register_object_store_for_url(ctx: &SessionContext, url_str: &str) -> Result<()> {
    let url = Url::parse(url_str).with_context(|| format!("Invalid URL: {url_str}"))?;
    let scheme = url.scheme();

    // Build a base URL for the object store (scheme + host/bucket)
    let store_url = match scheme {
        "s3" | "oss" | "cos" => {
            let bucket = url
                .host_str()
                .ok_or_else(|| anyhow::anyhow!("Missing bucket in URL: {url_str}"))?;
            format!("{scheme}://{bucket}")
        }
        "gs" | "gcs" => {
            let bucket = url
                .host_str()
                .ok_or_else(|| anyhow::anyhow!("Missing bucket in URL: {url_str}"))?;
            format!("{scheme}://{bucket}")
        }
        "az" | "azure" | "abfs" | "abfss" => {
            let container = url
                .host_str()
                .ok_or_else(|| anyhow::anyhow!("Missing container in URL: {url_str}"))?;
            format!("{scheme}://{container}")
        }
        "http" | "https" => {
            let host = url
                .host_str()
                .ok_or_else(|| anyhow::anyhow!("Missing host in URL: {url_str}"))?;
            let port_part = url.port().map(|p| format!(":{p}")).unwrap_or_default();
            format!("{scheme}://{host}{port_part}")
        }
        _ => return Ok(()),
    };

    let parsed_store_url =
        Url::parse(&store_url).with_context(|| format!("Invalid store URL: {store_url}"))?;

    // Check if already registered
    if ctx
        .runtime_env()
        .object_store(ObjectStoreUrl::parse(&store_url)?)
        .is_ok()
    {
        return Ok(());
    }

    let object_store: Arc<dyn ObjectStore> = match scheme {
        "s3" | "oss" | "cos" => {
            let bucket = url.host_str().unwrap();
            let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);

            if scheme == "oss" {
                builder = builder.with_virtual_hosted_style_request(true);
            } else if scheme == "cos" {
                builder = builder.with_virtual_hosted_style_request(false);
            }

            Arc::new(
                builder
                    .build()
                    .with_context(|| format!("Failed to build S3 store for {store_url}"))?,
            )
        }
        "gs" | "gcs" => {
            let bucket = url.host_str().unwrap();
            Arc::new(
                GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .with_context(|| format!("Failed to build GCS store for {store_url}"))?,
            )
        }
        "az" | "azure" | "abfs" | "abfss" => {
            let container = url.host_str().unwrap();
            Arc::new(
                MicrosoftAzureBuilder::from_env()
                    .with_container_name(container)
                    .build()
                    .with_context(|| format!("Failed to build Azure store for {store_url}"))?,
            )
        }
        "http" | "https" => Arc::new(
            HttpBuilder::new()
                .with_url(&store_url)
                .build()
                .with_context(|| format!("Failed to build HTTP store for {store_url}"))?,
        ),
        _ => return Ok(()),
    };

    ctx.register_object_store(&parsed_store_url, object_store);

    Ok(())
}

// ---------------------------------------------------------------------------
// Custom UrlTableFactory: extends DataFusion's default with Lance support
// ---------------------------------------------------------------------------

/// A [UrlTableFactory] that handles Lance datasets (`.lance` paths) and delegates
/// all other file types (CSV, Parquet, JSON, Avro, etc.) to DataFusion's built-in
/// [DynamicListTableFactory]. Opened Lance datasets are stored in the shared
/// registry so `lance_knn` can reference them.
struct SkardiUrlTableFactory {
    inner: DynamicListTableFactory,
    dataset_registry: DatasetRegistry,
}

impl fmt::Debug for SkardiUrlTableFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SkardiUrlTableFactory").finish()
    }
}

impl SkardiUrlTableFactory {
    fn new(session_store: SessionStore, dataset_registry: DatasetRegistry) -> Self {
        Self {
            inner: DynamicListTableFactory::new(session_store),
            dataset_registry,
        }
    }

    fn session_store(&self) -> &SessionStore {
        self.inner.session_store()
    }
}

#[async_trait]
impl UrlTableFactory for SkardiUrlTableFactory {
    async fn try_new(
        &self,
        url: &str,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        // Handle SQLite databases: detect patterns like "path/to/file.db.table_name"
        let sqlite_extensions = [".db.", ".sqlite.", ".sqlite3."];
        if let Some(ext) = sqlite_extensions.iter().find(|ext| url.contains(*ext)) {
            let pos = url.find(ext).unwrap();
            let db_path = &url[..pos + ext.len() - 1]; // include .db but not trailing dot
            let table_name = &url[pos + ext.len()..];

            if !table_name.is_empty() {
                let provider = skardi::sources::providers::sqlite::create_sqlite_table_provider(
                    db_path, table_name,
                )
                .await
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

                return Ok(Some(provider));
            }
        }

        // Handle Lance datasets by path suffix
        if url.ends_with(".lance") || url.contains(".lance/") {
            let dataset = Dataset::open(url)
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            let dataset_arc = Arc::new(dataset);

            // Derive a table name from the path and store in registry for lance_knn
            let table_name = Path::new(url)
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or(url)
                .to_string();
            if let Ok(mut reg) = self.dataset_registry.write() {
                reg.insert(
                    table_name,
                    skardi::sources::providers::DatasetEntry::Lance(Arc::clone(&dataset_arc)),
                );
            }

            let provider: Arc<dyn TableProvider> = dataset_arc;
            return Ok(Some(provider));
        }

        // Delegate all other formats to the default factory
        self.inner.try_new(url).await
    }
}

/// Create a new SessionContext with custom URL table support (built-in files + Lance)
/// and the `lance_knn` / `pg_knn` UDTFs registered.
fn new_session_context() -> (SessionContext, DatasetRegistry) {
    let dataset_registry: DatasetRegistry = Arc::new(RwLock::new(HashMap::new()));
    let session_store = SessionStore::new();
    let factory = Arc::new(SkardiUrlTableFactory::new(
        session_store,
        Arc::clone(&dataset_registry),
    ));

    let base_ctx = SessionContext::new();
    let current_catalog_list = Arc::clone(base_ctx.state().catalog_list());
    let catalog_list = Arc::new(DynamicFileCatalog::new(
        current_catalog_list,
        Arc::clone(&factory) as Arc<dyn UrlTableFactory>,
    ));

    let session_id = base_ctx.session_id().to_string();
    let mut ctx: SessionContext = base_ctx
        .into_state_builder()
        .with_session_id(session_id)
        .with_catalog_list(catalog_list)
        .build()
        .into();

    factory.session_store().with_state(ctx.state_weak_ref());

    // Register table functions (lance_knn, lance_fts, pg_knn, pg_fts,
    // mongo_fts, sqlite_knn, sqlite_fts) and the vec_to_binary scalar UDF,
    // all sharing one registry.
    register_lance_knn_udtf(&ctx, Arc::clone(&dataset_registry));
    register_lance_fts_udtf(&ctx, Arc::clone(&dataset_registry));
    register_pg_knn_udtf(&ctx, Arc::clone(&dataset_registry));
    register_pg_fts_udtf(&ctx, Arc::clone(&dataset_registry));
    register_mongo_fts_udtf(&ctx, Arc::clone(&dataset_registry));
    register_sqlite_knn_udtf(&ctx, Arc::clone(&dataset_registry));
    register_sqlite_fts_udtf(&ctx, Arc::clone(&dataset_registry));
    register_vec_to_binary_udf(&mut ctx);

    // Embedding UDFs (gated by feature flags, lazy model loading on first call).
    #[cfg(feature = "onnx")]
    {
        let registry = Arc::new(OnnxModelRegistry::new());
        registry.register_onnx_predict_udf(&mut ctx);
    }
    #[cfg(feature = "remote-embed")]
    {
        let registry = Arc::new(RemoteEmbedRegistry::new());
        registry.register_remote_embed_udf(&mut ctx);
    }
    #[cfg(feature = "gguf")]
    {
        let registry = Arc::new(GgufModelRegistry::new());
        registry.register_gguf_udf(&mut ctx);
    }
    #[cfg(feature = "candle")]
    {
        let registry = Arc::new(CandleModelRegistry::new());
        registry.register_candle_udf(&mut ctx);
    }

    (ctx, dataset_registry)
}

/// Resolve a path string: if relative (and not remote), resolve against cwd.
fn resolve_path(path_str: &str) -> Result<String> {
    if is_remote_path(path_str) {
        return Ok(path_str.to_string());
    }
    let p = Path::new(path_str);
    if p.is_relative() {
        let cwd = std::env::current_dir().context("Failed to get current directory")?;
        Ok(cwd
            .join(p)
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid path: {path_str}"))?
            .to_string())
    } else {
        Ok(path_str.to_string())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Query {
            ctx,
            schema,
            all,
            table,
            sql,
            file,
        } => {
            if schema {
                if all && table.is_some() {
                    anyhow::bail!("Use either --all or -t TABLE, not both");
                }
                if !all && table.is_none() {
                    anyhow::bail!("With --schema, provide --all or -t TABLE");
                }
                let ctx_path = resolve_ctx_path(ctx)?;
                let table_filter = if all { None } else { table.as_deref() };
                show_schema(&ctx_path, table_filter).await
            } else {
                let sql_content = if let Some(path) = &file {
                    std::fs::read_to_string(path)
                        .with_context(|| format!("Failed to read SQL file: {}", path.display()))?
                } else if let Some(s) = &sql {
                    s.clone()
                } else {
                    anyhow::bail!("Provide --sql or --file with the query to execute");
                };
                run_query(ctx, &sql_content).await
            }
        }
        Commands::Run {
            pipeline,
            params,
            ctx,
            pipeline_dir,
        } => {
            let param_bindings = parse_param_flags(&params)?;
            run_pipeline_by_name(ctx, pipeline_dir, &pipeline, param_bindings).await
        }
        Commands::Alias { cmd } => handle_alias_cmd(cmd).await,
        Commands::External(args) => {
            // The first token is the alias name; the rest are the alias's args
            // (plus any control flags we strip out before resolution).
            if args.is_empty() {
                anyhow::bail!("No subcommand provided");
            }
            let alias_name = args[0].clone();
            let rest = &args[1..];
            let (ctx_override, pipeline_dir_override, aliases_override, alias_args) =
                split_control_flags(rest)?;

            let default_ctx = resolve_ctx_path_opt();
            let aliases_path = resolve_aliases_path(
                aliases_override.as_deref(),
                ctx_override.as_deref().or_else(|| default_ctx.as_deref()),
            );
            let alias_map = match &aliases_path {
                Some(p) => alias_store::load(p)?,
                None => AliasMap::new(),
            };

            let alias_def = alias_map.get(&alias_name).ok_or_else(|| {
                anyhow::anyhow!(
                    "Unknown command or alias '{}'. Run `skardi alias list` to see available aliases.",
                    alias_name
                )
            })?;

            let resolved = resolve_alias(alias_def, &alias_args)?;
            run_pipeline_with_params(
                ctx_override,
                pipeline_dir_override,
                &resolved.pipeline,
                resolved.params,
            )
            .await
        }
    }
}

/// Parse a list of `NAME=VALUE` / `NAME:TYPE=VALUE` strings into typed pairs.
fn parse_param_flags(raw: &[String]) -> Result<Vec<(String, ScalarValue)>> {
    raw.iter()
        .map(|s| parse_param_flag(s))
        .collect::<Result<Vec<_>>>()
}

/// Strip control flags (`--ctx`, `--pipeline-dir`, `--aliases`) out of a raw
/// alias-argument vector so they aren't forwarded to the alias resolver. These
/// three flags are the CLI's "global" concerns; everything else is left
/// verbatim and passed to the alias as a pipeline-param binding.
fn split_control_flags(
    args: &[String],
) -> Result<(
    Option<PathBuf>,
    Option<PathBuf>,
    Option<PathBuf>,
    Vec<String>,
)> {
    let mut ctx: Option<PathBuf> = None;
    let mut pipeline_dir: Option<PathBuf> = None;
    let mut aliases: Option<PathBuf> = None;
    let mut rest: Vec<String> = Vec::new();

    let take = |target: &mut Option<PathBuf>, value: &str| {
        *target = Some(PathBuf::from(value));
    };

    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        let maybe_take = |name: &str, target: &mut Option<PathBuf>| -> Option<usize> {
            let eq = format!("--{name}=");
            if let Some(v) = a.strip_prefix(&eq) {
                take(target, v);
                return Some(1);
            }
            if a == &format!("--{name}") {
                let next = args.get(i + 1)?;
                take(target, next);
                return Some(2);
            }
            None
        };

        if let Some(n) = maybe_take("ctx", &mut ctx) {
            i += n;
            continue;
        }
        if let Some(n) = maybe_take("pipeline-dir", &mut pipeline_dir) {
            i += n;
            continue;
        }
        if let Some(n) = maybe_take("aliases", &mut aliases) {
            i += n;
            continue;
        }
        rest.push(a.clone());
        i += 1;
    }

    Ok((ctx, pipeline_dir, aliases, rest))
}

/// Resolve the ctx path using the same defaults as [`resolve_ctx_path`], but
/// returning None (instead of an error) when no home directory is available.
fn resolve_ctx_path_opt() -> Option<PathBuf> {
    if let Ok(env_path) = std::env::var("SKARDICONFIG") {
        return Some(PathBuf::from(env_path));
    }
    dirs::home_dir().map(|h| h.join(".skardi").join("config").join("ctx.yaml"))
}

/// Load a context YAML and register all data sources (local files, remote files, databases).
async fn load_and_register_all(
    ctx_path: &Path,
    session_ctx: &mut SessionContext,
    dataset_registry: &DatasetRegistry,
) -> Result<LocalContextConfig> {
    let content = std::fs::read_to_string(ctx_path)
        .with_context(|| format!("Failed to read context file: {}", ctx_path.display()))?;
    let config: LocalContextConfig =
        serde_yaml::from_str(&content).context("Failed to parse context YAML")?;

    for source in &config.data_sources {
        register_source(session_ctx, source, dataset_registry)
            .await
            .with_context(|| format!("Failed to register data source '{}'", source.name))?;
    }

    Ok(config)
}

/// Register a single data source into the session context.
async fn register_source(
    session_ctx: &mut SessionContext,
    source: &LocalDataSource,
    dataset_registry: &DatasetRegistry,
) -> Result<()> {
    let source_type = source.source_type.to_lowercase();

    match source_type.as_str() {
        "csv" => {
            let path_str = source
                .path
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("CSV source '{}': path required", source.name))?;
            let resolved = resolve_path(path_str)?;

            if is_remote_path(&resolved) {
                register_object_store_for_url(session_ctx, &resolved)?;
            } else if !Path::new(&resolved).exists() {
                anyhow::bail!(
                    "Data source '{}': path not found: {}",
                    source.name,
                    resolved
                );
            }

            let mut opts = CsvReadOptions::new();
            if let Some(ref options) = source.options {
                if let Some(h) = options.get("has_header") {
                    opts = opts.has_header(h.parse().unwrap_or(true));
                }
                if let Some(d) = options.get("delimiter") {
                    if let Some(c) = d.chars().next() {
                        opts = opts.delimiter(c as u8);
                    }
                }
                if let Some(m) = options.get("schema_infer_max_records") {
                    if let Ok(n) = m.parse::<usize>() {
                        opts = opts.schema_infer_max_records(n);
                    }
                }
            }
            session_ctx
                .register_csv(&source.name, &resolved, opts)
                .await
                .with_context(|| format!("Failed to register CSV '{}'", source.name))?;
        }
        "parquet" => {
            let path_str = source.path.as_deref().ok_or_else(|| {
                anyhow::anyhow!("Parquet source '{}': path required", source.name)
            })?;
            let resolved = resolve_path(path_str)?;

            if is_remote_path(&resolved) {
                register_object_store_for_url(session_ctx, &resolved)?;
            } else if !Path::new(&resolved).exists() {
                anyhow::bail!(
                    "Data source '{}': path not found: {}",
                    source.name,
                    resolved
                );
            }

            session_ctx
                .register_parquet(&source.name, &resolved, ParquetReadOptions::default())
                .await
                .with_context(|| format!("Failed to register Parquet '{}'", source.name))?;
        }
        "json" | "ndjson" => {
            let path_str = source
                .path
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("JSON source '{}': path required", source.name))?;
            let resolved = resolve_path(path_str)?;

            if is_remote_path(&resolved) {
                register_object_store_for_url(session_ctx, &resolved)?;
            } else if !Path::new(&resolved).exists() {
                anyhow::bail!(
                    "Data source '{}': path not found: {}",
                    source.name,
                    resolved
                );
            }

            session_ctx
                .register_json(&source.name, &resolved, NdJsonReadOptions::default())
                .await
                .with_context(|| format!("Failed to register JSON '{}'", source.name))?;
        }
        "postgres" => {
            let conn_str = source.connection_string.as_deref().ok_or_else(|| {
                anyhow::anyhow!(
                    "Postgres source '{}': connection_string required",
                    source.name
                )
            })?;
            register_postgres_tables(
                session_ctx,
                &source.name,
                conn_str,
                source.options.as_ref(),
                false,
                Some(dataset_registry),
                source.hierarchy_level,
            )
            .await
            .with_context(|| format!("Failed to register Postgres '{}'", source.name))?;
        }
        "mysql" => {
            let conn_str = source.connection_string.as_deref().ok_or_else(|| {
                anyhow::anyhow!("MySQL source '{}': connection_string required", source.name)
            })?;
            register_mysql_tables(
                session_ctx,
                &source.name,
                conn_str,
                source.options.as_ref(),
                false,
                source.hierarchy_level,
            )
            .await
            .with_context(|| format!("Failed to register MySQL '{}'", source.name))?;
        }
        "mongo" => {
            let conn_str = source.connection_string.as_deref().ok_or_else(|| {
                anyhow::anyhow!(
                    "MongoDB source '{}': connection_string required",
                    source.name
                )
            })?;
            register_mongo_tables(
                session_ctx,
                &source.name,
                conn_str,
                source.options.as_ref(),
                Some(dataset_registry),
            )
            .await
            .with_context(|| format!("Failed to register MongoDB '{}'", source.name))?;
        }
        "lance" => {
            let path_str = source
                .path
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("Lance source '{}': path required", source.name))?;
            let resolved = resolve_path(path_str)?;

            if !Path::new(&resolved).exists() {
                anyhow::bail!(
                    "Data source '{}': path not found: {}",
                    source.name,
                    resolved
                );
            }

            register_lance_table(session_ctx, &source.name, &resolved, Some(dataset_registry))
                .await
                .with_context(|| format!("Failed to register Lance '{}'", source.name))?;
        }
        "sqlite" => {
            let path_str = source
                .path
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("SQLite source '{}': path required", source.name))?;
            let resolved = resolve_path(path_str)?;

            register_sqlite_tables(
                session_ctx,
                &source.name,
                &resolved,
                source.options.as_ref(),
                source.is_read_write(),
                Some(dataset_registry),
                source.hierarchy_level,
            )
            .await
            .with_context(|| format!("Failed to register SQLite '{}'", source.name))?;
        }
        "iceberg" => {
            let path_str = source.path.as_deref().ok_or_else(|| {
                anyhow::anyhow!(
                    "Iceberg source '{}': path (warehouse) required",
                    source.name
                )
            })?;
            register_iceberg_table(session_ctx, &source.name, path_str, source.options.as_ref())
                .await
                .with_context(|| format!("Failed to register Iceberg '{}'", source.name))?;
        }
        _ => {
            anyhow::bail!(
                "Unsupported data source type '{}' for source '{}'",
                source.source_type,
                source.name
            );
        }
    }

    Ok(())
}

async fn show_schema(ctx_path: &Path, table_filter: Option<&str>) -> Result<()> {
    let (mut session_ctx, dataset_registry) = new_session_context();
    let config = load_and_register_all(ctx_path, &mut session_ctx, &dataset_registry).await?;

    let all_names: Vec<String> = config.data_sources.iter().map(|s| s.name.clone()).collect();

    let table_names: Vec<String> = match table_filter {
        Some(t) => {
            if all_names.contains(&t.to_string()) {
                vec![t.to_string()]
            } else {
                anyhow::bail!(
                    "Table '{}' not found in context. Available tables: {}",
                    t,
                    all_names.join(", ")
                );
            }
        }
        None => all_names,
    };

    for name in &table_names {
        let df = session_ctx
            .sql(&format!(
                "SELECT * FROM \"{}\" LIMIT 0",
                name.replace('"', "\"\"")
            ))
            .await
            .with_context(|| format!("Failed to read table '{}'", name))?;
        let schema = df.schema().inner();
        println!("table: {name}");
        for field in schema.fields() {
            println!("  {}: {:?}", field.name(), field.data_type());
        }
        println!();
    }
    Ok(())
}

/// Execute a SQL query. If a context file is provided (or found via defaults), register its
/// data sources first. If no context file is found, run the query in a bare session with
/// URL table support (allowing direct file/lance paths in SQL).
async fn run_query(ctx_override: Option<PathBuf>, sql: &str) -> Result<()> {
    let (mut session_ctx, dataset_registry) = new_session_context();

    // Try to load context file, but don't fail if not found when no explicit --ctx was given
    let ctx_path_result = resolve_ctx_path(ctx_override.clone());
    match ctx_path_result {
        Ok(ctx_path) if ctx_path.exists() => {
            load_and_register_all(&ctx_path, &mut session_ctx, &dataset_registry).await?;
        }
        Ok(_) if ctx_override.is_some() => {
            let path = resolve_ctx_path(ctx_override)?;
            anyhow::bail!("Context file not found: {}", path.display());
        }
        _ => {
            // No context file found via defaults — that's fine, run without
        }
    }

    // Scan SQL for remote URLs and register object stores automatically
    auto_register_object_stores_from_sql(&session_ctx, sql)?;

    let df = session_ctx
        .sql(sql.trim())
        .await
        .context("SQL execution failed")?;
    let batches = df.collect().await.context("Failed to collect results")?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    if total_rows > 0 {
        let formatted = pretty_format_batches(&batches).context("Failed to format results")?;
        println!("{formatted}");
    }
    println!("\n{total_rows} row(s) returned");
    Ok(())
}

/// Scan SQL text for remote URLs (s3://, gs://, az://, http(s)://) and pre-register
/// the corresponding object stores so DataFusion can resolve them.
fn auto_register_object_stores_from_sql(ctx: &SessionContext, sql: &str) -> Result<()> {
    let prefixes = [
        "s3://", "gs://", "gcs://", "az://", "azure://", "abfs://", "abfss://", "http://",
        "https://", "oss://", "cos://",
    ];

    for prefix in &prefixes {
        let mut search_from = 0;
        while let Some(start) = sql[search_from..].find(prefix) {
            let abs_start = search_from + start;
            let url_slice = &sql[abs_start..];
            let end = url_slice
                .find(|c: char| c.is_whitespace() || c == '\'' || c == '"' || c == ')' || c == ';')
                .unwrap_or(url_slice.len());
            let url_str = &url_slice[..end];
            let _ = register_object_store_for_url(ctx, url_str);
            search_from = abs_start + end;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Pipeline execution
// ---------------------------------------------------------------------------

/// `skardi run <name>` entrypoint — parse `--param` flags, resolve the pipeline
/// from the discovery dir, substitute `{name}` → `$name`, and execute.
async fn run_pipeline_by_name(
    ctx_override: Option<PathBuf>,
    pipeline_dir_override: Option<PathBuf>,
    pipeline_name: &str,
    params: Vec<(String, ScalarValue)>,
) -> Result<()> {
    run_pipeline_with_params(ctx_override, pipeline_dir_override, pipeline_name, params).await
}

/// Shared execution path used by both `skardi run` and alias dispatch.
async fn run_pipeline_with_params(
    ctx_override: Option<PathBuf>,
    pipeline_dir_override: Option<PathBuf>,
    pipeline_name: &str,
    params: Vec<(String, ScalarValue)>,
) -> Result<()> {
    // 1. Resolve ctx path: if a --ctx was given, it must exist; otherwise
    //    default to whatever `resolve_ctx_path` returns (may be missing, and
    //    that's fine — pipelines that need no data sources can still run).
    let ctx_path = resolve_ctx_path(ctx_override.clone()).ok();
    let ctx_path_for_load: Option<PathBuf> = match &ctx_path {
        Some(p) if p.exists() => Some(p.clone()),
        Some(p) if ctx_override.is_some() => {
            anyhow::bail!("Context file not found: {}", p.display());
        }
        _ => None,
    };

    // Read ctx (if present) up front so we can discover pipelines before
    // registering data sources — errors on the pipeline side should surface
    // before we pay the cost of connecting to Postgres/Mongo/etc.
    let ctx_cfg: Option<LocalContextConfig> = match &ctx_path_for_load {
        Some(p) => {
            let content = std::fs::read_to_string(p)
                .with_context(|| format!("Failed to read context file: {}", p.display()))?;
            Some(serde_yaml::from_str(&content).context("Failed to parse context YAML")?)
        }
        None => None,
    };

    let pipeline_dirs = resolve_pipeline_dirs(
        pipeline_dir_override.as_ref(),
        ctx_cfg.as_ref(),
        ctx_path_for_load.as_deref(),
    );

    let pipelines = discover_pipelines(&pipeline_dirs).with_context(|| {
        format!(
            "Failed to discover pipelines in {:?}",
            pipeline_dirs
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
        )
    })?;

    let (pipeline_path, pipeline_file) = pipelines.get(pipeline_name).ok_or_else(|| {
        let mut names: Vec<&String> = pipelines.keys().collect();
        names.sort();
        anyhow::anyhow!(
            "Pipeline '{}' not found. Searched: {:?}. Known pipelines: {:?}",
            pipeline_name,
            pipeline_dirs
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>(),
            names
        )
    })?;

    // 2. Validate every placeholder has a bound value, then inline-substitute
    //    values as SQL literals. We inline rather than use DataFusion's `$name`
    //    binding because some UDTFs (e.g. `sqlite_fts`) require a string
    //    literal at plan time — they run before `with_param_values` can
    //    substitute Placeholder exprs. See `render_sql_with_inline_params`.
    let expected = extract_param_names(&pipeline_file.query);
    let params_map: HashMap<String, ScalarValue> = params.into_iter().collect();
    let missing: Vec<&String> = expected
        .iter()
        .filter(|n| !params_map.contains_key(n.as_str()))
        .collect();
    if !missing.is_empty() {
        anyhow::bail!(
            "Pipeline '{}' (from {}) is missing required parameter(s): {}",
            pipeline_name,
            pipeline_path.display(),
            missing
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    let sql = render_sql_with_inline_params(&pipeline_file.query, &params_map)
        .with_context(|| format!("Failed to render SQL for pipeline '{}'", pipeline_name))?;

    // 3. Build a SessionContext with ctx data sources registered, then execute.
    let (mut session_ctx, dataset_registry) = new_session_context();
    if let Some(p) = &ctx_path_for_load {
        load_and_register_all(p, &mut session_ctx, &dataset_registry).await?;
    }

    auto_register_object_stores_from_sql(&session_ctx, &sql)?;

    let df = session_ctx
        .sql(&sql)
        .await
        .with_context(|| format!("Failed to plan pipeline '{}'", pipeline_name))?;
    let batches = df
        .collect()
        .await
        .context("Failed to collect pipeline results")?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    if total_rows > 0 {
        let formatted = pretty_format_batches(&batches).context("Failed to format results")?;
        println!("{formatted}");
    }
    println!("\n{total_rows} row(s) returned");
    Ok(())
}

/// Resolve the list of directories to scan for pipeline YAMLs.
///
/// Priority:
/// 1. Explicit `--pipeline-dir` flag.
/// 2. `pipelines_dir` from ctx.yaml (relative paths resolved against ctx dir).
/// 3. No default — returns empty vec.
fn resolve_pipeline_dirs(
    override_dir: Option<&PathBuf>,
    ctx_cfg: Option<&LocalContextConfig>,
    ctx_path: Option<&Path>,
) -> Vec<PathBuf> {
    if let Some(d) = override_dir {
        return vec![d.clone()];
    }
    if let Some(cfg) = ctx_cfg {
        if let Some(rel) = &cfg.pipelines_dir {
            let resolved = if rel.is_absolute() {
                rel.clone()
            } else if let Some(ctx_dir) = ctx_path.and_then(|p| p.parent()) {
                ctx_dir.join(rel)
            } else {
                rel.clone()
            };
            return vec![resolved];
        }
    }
    Vec::new()
}

// ---------------------------------------------------------------------------
// Alias management (`skardi alias add/list/show/remove`)
// ---------------------------------------------------------------------------

async fn handle_alias_cmd(cmd: AliasCmd) -> Result<()> {
    match cmd {
        AliasCmd::Add {
            name,
            pipeline,
            positional,
            defaults,
            description,
            force,
            aliases,
            ctx,
            pipeline_dir,
            no_validate,
        } => alias_add(
            name,
            pipeline,
            positional,
            defaults,
            description,
            force,
            aliases,
            ctx,
            pipeline_dir,
            no_validate,
        ),
        AliasCmd::List { aliases, ctx } => alias_list(aliases, ctx),
        AliasCmd::Show { name, aliases, ctx } => alias_show(name, aliases, ctx),
        AliasCmd::Remove { name, aliases, ctx } => alias_remove(name, aliases, ctx),
    }
}

#[allow(clippy::too_many_arguments)]
fn alias_add(
    name: String,
    pipeline: String,
    positional: Option<String>,
    defaults: Vec<String>,
    description: Option<String>,
    force: bool,
    aliases: Option<PathBuf>,
    ctx: Option<PathBuf>,
    pipeline_dir: Option<PathBuf>,
    no_validate: bool,
) -> Result<()> {
    let path = resolve_aliases_path(aliases.as_deref(), ctx.as_deref())
        .ok_or_else(|| anyhow::anyhow!("Could not resolve aliases file path"))?;
    let mut map = alias_store::load(&path)?;
    if map.contains_key(&name) && !force {
        anyhow::bail!(
            "Alias '{name}' already exists. Use --force to overwrite. \
             Current target: {}",
            map[&name].pipeline
        );
    }

    let positional_vec: Vec<String> = positional
        .as_deref()
        .map(|s| {
            s.split(',')
                .map(|p| p.trim().to_string())
                .filter(|p| !p.is_empty())
                .collect()
        })
        .unwrap_or_default();

    let mut defaults_map: HashMap<String, String> = HashMap::new();
    for raw in &defaults {
        let (k, v) = raw
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("--default must be NAME=VALUE, got: {raw}"))?;
        if k.is_empty() {
            anyhow::bail!("--default NAME must not be empty: {raw}");
        }
        defaults_map.insert(k.to_string(), v.to_string());
    }

    // Validate positional/default names against the pipeline's real `{param}`
    // placeholders (unless the user opted out via --no-validate). Missing
    // pipeline → we only warn, since the alias file is allowed to exist
    // before its target pipeline is authored.
    if !no_validate {
        if let Some(pipeline_params) =
            try_load_pipeline_params(&pipeline, ctx.as_deref(), pipeline_dir.as_deref())?
        {
            validate_alias_against_pipeline(
                &pipeline,
                &pipeline_params,
                &positional_vec,
                &defaults_map,
            )?;
            report_alias_coverage(&pipeline, &pipeline_params, &positional_vec, &defaults_map);
        } else {
            eprintln!(
                "warning: pipeline '{pipeline}' not found in the configured pipelines dir — \
                 skipping param validation. Pass --no-validate to silence this, or set \
                 `pipelines_dir:` in ctx / pass --pipeline-dir."
            );
        }
    }

    map.insert(
        name.clone(),
        AliasDef {
            pipeline: pipeline.clone(),
            positional: positional_vec,
            defaults: defaults_map,
            description,
        },
    );
    alias_store::save(&path, &map)?;
    println!(
        "Alias '{}' → pipeline '{}' saved to {}",
        name,
        pipeline,
        path.display()
    );
    Ok(())
}

/// Load the named pipeline's `{name}` placeholder list, or `None` if the
/// pipeline file cannot be located via the caller's ctx / pipeline-dir.
fn try_load_pipeline_params(
    pipeline: &str,
    ctx_override: Option<&Path>,
    pipeline_dir_override: Option<&Path>,
) -> Result<Option<Vec<String>>> {
    // Parse ctx if provided so `pipelines_dir` can act as the default.
    let ctx_path = ctx_override
        .map(|p| p.to_path_buf())
        .or_else(resolve_ctx_path_opt)
        .filter(|p| p.exists());
    let ctx_cfg: Option<LocalContextConfig> = match &ctx_path {
        Some(p) => {
            let content = std::fs::read_to_string(p)
                .with_context(|| format!("Failed to read context file: {}", p.display()))?;
            Some(serde_yaml::from_str(&content).context("Failed to parse context YAML")?)
        }
        None => None,
    };

    let pipeline_dirs = resolve_pipeline_dirs(
        pipeline_dir_override.map(|p| p.to_path_buf()).as_ref(),
        ctx_cfg.as_ref(),
        ctx_path.as_deref(),
    );
    if pipeline_dirs.is_empty() {
        return Ok(None);
    }
    let pipelines = discover_pipelines(&pipeline_dirs)?;
    Ok(pipelines
        .get(pipeline)
        .map(|(_, file)| extract_param_names(&file.query)))
}

/// Fail fast if any `--positional` / `--default` name refers to a param the
/// pipeline does not actually have (almost always a typo).
fn validate_alias_against_pipeline(
    pipeline: &str,
    pipeline_params: &[String],
    positional: &[String],
    defaults: &HashMap<String, String>,
) -> Result<()> {
    let known: std::collections::HashSet<&str> =
        pipeline_params.iter().map(|s| s.as_str()).collect();
    for p in positional {
        if !known.contains(p.as_str()) {
            anyhow::bail!(
                "Pipeline '{pipeline}' has no parameter '{p}'. Known parameters: {}",
                pipeline_params.join(", ")
            );
        }
    }
    for k in defaults.keys() {
        if !known.contains(k.as_str()) {
            anyhow::bail!(
                "Pipeline '{pipeline}' has no parameter '{k}'. Known parameters: {}",
                pipeline_params.join(", ")
            );
        }
    }
    Ok(())
}

/// Print which of the pipeline's params are covered by this alias and which
/// the user will still need to pass at call time (or add via --default now).
fn report_alias_coverage(
    pipeline: &str,
    pipeline_params: &[String],
    positional: &[String],
    defaults: &HashMap<String, String>,
) {
    let positional_set: std::collections::HashSet<&str> =
        positional.iter().map(|s| s.as_str()).collect();
    let default_set: std::collections::HashSet<&str> =
        defaults.keys().map(|s| s.as_str()).collect();
    let unbound: Vec<&str> = pipeline_params
        .iter()
        .map(|s| s.as_str())
        .filter(|p| !positional_set.contains(p) && !default_set.contains(p))
        .collect();

    println!(
        "Pipeline '{}' has {} parameter(s): {}",
        pipeline,
        pipeline_params.len(),
        pipeline_params.join(", ")
    );
    if !unbound.is_empty() {
        println!(
            "  Unbound by this alias: {} (pass at call time with --name=value, \
             or re-run `alias add --force` with --default/--positional)",
            unbound.join(", ")
        );
    }
}

fn alias_list(aliases: Option<PathBuf>, ctx: Option<PathBuf>) -> Result<()> {
    let path = resolve_aliases_path(aliases.as_deref(), ctx.as_deref())
        .ok_or_else(|| anyhow::anyhow!("Could not resolve aliases file path"))?;
    let map = alias_store::load(&path)?;
    if map.is_empty() {
        println!("No aliases defined (file: {})", path.display());
        return Ok(());
    }
    println!("Aliases from {}:", path.display());
    for (name, def) in &map {
        let desc = def.description.as_deref().unwrap_or("");
        println!(
            "  {:<12} → run {}{}{}",
            name,
            def.pipeline,
            if desc.is_empty() { "" } else { "  — " },
            desc
        );
    }
    Ok(())
}

fn alias_show(name: String, aliases: Option<PathBuf>, ctx: Option<PathBuf>) -> Result<()> {
    let path = resolve_aliases_path(aliases.as_deref(), ctx.as_deref())
        .ok_or_else(|| anyhow::anyhow!("Could not resolve aliases file path"))?;
    let map = alias_store::load(&path)?;
    let def = map
        .get(&name)
        .ok_or_else(|| anyhow::anyhow!("Alias '{name}' not found in {}", path.display()))?;
    let mut single = AliasMap::new();
    single.insert(name, def.clone());
    let yaml = serde_yaml::to_string(&single).context("Failed to render alias to YAML")?;
    print!("{yaml}");
    Ok(())
}

fn alias_remove(name: String, aliases: Option<PathBuf>, ctx: Option<PathBuf>) -> Result<()> {
    let path = resolve_aliases_path(aliases.as_deref(), ctx.as_deref())
        .ok_or_else(|| anyhow::anyhow!("Could not resolve aliases file path"))?;
    let mut map = alias_store::load(&path)?;
    if map.remove(&name).is_none() {
        anyhow::bail!("Alias '{name}' not found in {}", path.display());
    }
    alias_store::save(&path, &map)?;
    println!("Alias '{}' removed from {}", name, path.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_alias_rejects_unknown_positional() {
        let params = vec!["query".to_string(), "limit".to_string()];
        let defaults = HashMap::new();
        let positional = vec!["qeury".to_string()]; // typo
        let err = validate_alias_against_pipeline("p", &params, &positional, &defaults)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("no parameter 'qeury'"),
            "unexpected error: {err}"
        );
        assert!(err.contains("query"), "error must list known params: {err}");
    }

    #[test]
    fn validate_alias_rejects_unknown_default() {
        let params = vec!["query".to_string(), "limit".to_string()];
        let mut defaults = HashMap::new();
        defaults.insert("limmit".to_string(), "10".to_string()); // typo
        let positional: Vec<String> = Vec::new();
        let err = validate_alias_against_pipeline("p", &params, &positional, &defaults)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("no parameter 'limmit'"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_alias_accepts_fully_known_names() {
        let params = vec!["query".to_string(), "limit".to_string()];
        let mut defaults = HashMap::new();
        defaults.insert("limit".to_string(), "10".to_string());
        let positional = vec!["query".to_string()];
        validate_alias_against_pipeline("p", &params, &positional, &defaults).unwrap();
    }
}
