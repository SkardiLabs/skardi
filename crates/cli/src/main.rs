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
use std::collections::{BTreeMap, HashMap};
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
        /// Ctx file used to derive default aliases file location (and to
        /// locate the pipeline YAML for param validation).
        #[arg(long)]
        ctx: Option<PathBuf>,
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

/// Resolve the ctx YAML file path.
///
/// `override_path` (`--ctx`) and `SKARDICONFIG` both accept either a file
/// (used directly) or a directory (we append `ctx.yaml` by convention). The
/// default when neither is given is `~/.skardi/config/ctx.yaml`.
///
/// Returns `None` only when no override / env is set and the platform has no
/// home directory — the rare case where we have no path to fall back to.
/// Callers that need to fail on that can use `.ok_or_else(...)`; callers
/// that can tolerate no ctx (e.g. alias dispatch, `skardi run` with a
/// pipeline that needs no data sources) can use `.ok()`.
fn resolve_ctx_path(override_path: Option<&Path>) -> Option<PathBuf> {
    let source = override_path
        .map(|p| p.to_path_buf())
        .or_else(|| std::env::var("SKARDICONFIG").ok().map(PathBuf::from));
    if let Some(p) = source {
        return Some(if p.is_dir() { p.join("ctx.yaml") } else { p });
    }
    Some(
        dirs::home_dir()?
            .join(".skardi")
            .join("config")
            .join("ctx.yaml"),
    )
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
                let ctx_path = resolve_ctx_path(ctx.as_deref())
                    .ok_or_else(|| anyhow::anyhow!("Could not resolve a ctx path (no --ctx, no SKARDICONFIG, no home directory)"))?;
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

            let default_ctx = resolve_ctx_path(None);
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

            // `skardi <alias> --help|-h` short-circuits to a usage view that
            // lists the alias's positional slots, the pipeline's params, and
            // each param's binding source.
            if alias_args.iter().any(|a| a == "--help" || a == "-h") {
                let pipeline_params =
                    try_load_pipeline_params(&alias_def.pipeline, ctx_override.as_deref())?;
                print_alias_help(&alias_name, alias_def, pipeline_params.as_deref());
                return Ok(());
            }

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

/// A `(catalog, schema, table)` triple discovered by walking the DataFusion
/// catalog tree. Used by `--schema` to render every registered table — including
/// those nested under a catalog-mode source like SQLite (`wiki.main.wiki_pages`),
/// which the old `data_sources[].name` iteration could not see.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TableEntry {
    catalog: String,
    schema: String,
    table: String,
}

/// Default catalog/schema names read from the active `SessionConfig`. Used to
/// decide whether a `TableEntry` renders bare or fully-qualified — rather than
/// hard-coding DataFusion's `datafusion`/`public` literals.
#[derive(Debug, Clone)]
struct CatalogDefaults {
    catalog: String,
    schema: String,
}

impl CatalogDefaults {
    fn from_ctx(ctx: &SessionContext) -> Self {
        let config = ctx.copied_config();
        let opts = &config.options().catalog;
        Self {
            catalog: opts.default_catalog.clone(),
            schema: opts.default_schema.clone(),
        }
    }
}

impl TableEntry {
    /// Render for display: bare table name when in the session's default
    /// catalog/schema (where table-mode sources land), otherwise the
    /// fully-qualified `catalog.schema.table` form.
    fn display_name(&self, defaults: &CatalogDefaults) -> String {
        if self.catalog == defaults.catalog && self.schema == defaults.schema {
            self.table.clone()
        } else {
            self.qualified_name()
        }
    }

    /// Same form the user would type to `--schema -t`.
    fn qualified_name(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

/// Schemas populated automatically by DataFusion (or by some providers) that
/// we don't want `--schema --all` to surface. Treating these as a blocklist
/// keeps catalog walks focused on user-registered tables even if
/// `datafusion.catalog.information_schema` is ever enabled upstream.
const SYSTEM_SCHEMAS: &[&str] = &["information_schema", "pg_catalog"];

/// Walk every catalog → schema → table registered on `ctx` and collect them.
/// Skips well-known system schemas (see `SYSTEM_SCHEMAS`). Sorted for
/// deterministic output.
fn enumerate_tables(ctx: &SessionContext) -> Vec<TableEntry> {
    let mut entries = Vec::new();
    for catalog_name in ctx.catalog_names() {
        let Some(catalog) = ctx.catalog(&catalog_name) else {
            continue;
        };
        for schema_name in catalog.schema_names() {
            if SYSTEM_SCHEMAS.contains(&schema_name.as_str()) {
                continue;
            }
            let Some(schema) = catalog.schema(&schema_name) else {
                continue;
            };
            for table_name in schema.table_names() {
                entries.push(TableEntry {
                    catalog: catalog_name.clone(),
                    schema: schema_name.clone(),
                    table: table_name,
                });
            }
        }
    }
    entries.sort_by(|a, b| {
        (a.catalog.as_str(), a.schema.as_str(), a.table.as_str()).cmp(&(
            b.catalog.as_str(),
            b.schema.as_str(),
            b.table.as_str(),
        ))
    });
    entries
}

/// Resolve a `--schema -t TABLE` filter against the discovered tables.
///
/// Accepted forms (exact part count — anything else is rejected up front with
/// a hint, so a user typing `-t mydb.table` doesn't get a confusing
/// "not found"):
/// - **Fully qualified** `catalog.schema.table` — must match exactly.
/// - **Unqualified bare name** — must match exactly one table across all
///   catalogs/schemas. If the same table name appears in multiple places we
///   refuse rather than guess, and tell the user to disambiguate.
fn select_tables(
    all: &[TableEntry],
    filter: &str,
    defaults: &CatalogDefaults,
) -> Result<Vec<TableEntry>> {
    let parts: Vec<&str> = filter.split('.').collect();
    match parts.len() {
        3 => {
            let (catalog, schema, table) = (parts[0], parts[1], parts[2]);
            let hit = all
                .iter()
                .find(|e| e.catalog == catalog && e.schema == schema && e.table == table);
            match hit {
                Some(e) => Ok(vec![e.clone()]),
                None => anyhow::bail!(
                    "Table '{}' not found. Available tables: {}",
                    filter,
                    available_list(all, defaults)
                ),
            }
        }
        1 => {
            let matches: Vec<&TableEntry> = all.iter().filter(|e| e.table == filter).collect();
            match matches.as_slice() {
                [] => anyhow::bail!(
                    "Table '{}' not found. Available tables: {}",
                    filter,
                    available_list(all, defaults)
                ),
                [only] => Ok(vec![(*only).clone()]),
                many => anyhow::bail!(
                    "Table name '{}' is ambiguous — matches: {}. Re-run with the fully-qualified form (catalog.schema.table).",
                    filter,
                    many.iter()
                        .map(|e| e.qualified_name())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            }
        }
        _ => anyhow::bail!(
            "Table filter '{}' must be either a bare name or fully-qualified `catalog.schema.table` (got {} part{}).",
            filter,
            parts.len(),
            if parts.len() == 1 { "" } else { "s" }
        ),
    }
}

fn available_list(all: &[TableEntry], defaults: &CatalogDefaults) -> String {
    if all.is_empty() {
        return "(none)".to_string();
    }
    all.iter()
        .map(|e| e.display_name(defaults))
        .collect::<Vec<_>>()
        .join(", ")
}

async fn show_schema(ctx_path: &Path, table_filter: Option<&str>) -> Result<()> {
    let (mut session_ctx, dataset_registry) = new_session_context();
    load_and_register_all(ctx_path, &mut session_ctx, &dataset_registry).await?;

    let defaults = CatalogDefaults::from_ctx(&session_ctx);
    let all_tables = enumerate_tables(&session_ctx);
    let selected = match table_filter {
        Some(t) => select_tables(&all_tables, t, &defaults)?,
        None => all_tables,
    };

    if selected.is_empty() {
        println!("No tables registered.");
        return Ok(());
    }

    for entry in &selected {
        let catalog = session_ctx.catalog(&entry.catalog).ok_or_else(|| {
            anyhow::anyhow!("Catalog '{}' disappeared during enumeration", entry.catalog)
        })?;
        let schema = catalog.schema(&entry.schema).ok_or_else(|| {
            anyhow::anyhow!(
                "Schema '{}.{}' disappeared during enumeration",
                entry.catalog,
                entry.schema
            )
        })?;
        let provider = schema
            .table(&entry.table)
            .await
            .with_context(|| format!("Failed to load table '{}'", entry.qualified_name()))?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Table '{}' disappeared during enumeration",
                    entry.qualified_name()
                )
            })?;
        let table_schema = provider.schema();
        println!("table: {}", entry.display_name(&defaults));
        for field in table_schema.fields() {
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
    match resolve_ctx_path(ctx_override.as_deref()) {
        Some(ctx_path) if ctx_path.exists() => {
            load_and_register_all(&ctx_path, &mut session_ctx, &dataset_registry).await?;
        }
        Some(ctx_path) if ctx_override.is_some() => {
            anyhow::bail!("Context file not found: {}", ctx_path.display());
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
    let ctx_path = resolve_ctx_path(ctx_override.as_deref());
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
/// 3. `<ctx_dir>/pipelines/` convention — used when it exists on disk.
/// 4. No default — returns empty vec.
fn resolve_pipeline_dirs(
    override_dir: Option<&PathBuf>,
    ctx_cfg: Option<&LocalContextConfig>,
    ctx_path: Option<&Path>,
) -> Vec<PathBuf> {
    if let Some(d) = override_dir {
        return vec![d.clone()];
    }
    let ctx_dir: Option<PathBuf> = ctx_path.and_then(|p| p.parent().map(|x| x.to_path_buf()));
    if let Some(cfg) = ctx_cfg {
        if let Some(rel) = &cfg.pipelines_dir {
            let resolved = if rel.is_absolute() {
                rel.clone()
            } else if let Some(ctx_dir) = &ctx_dir {
                ctx_dir.join(rel)
            } else {
                rel.clone()
            };
            return vec![resolved];
        }
    }
    // Convention fallback: `<ctx_dir>/pipelines/`.
    if let Some(d) = ctx_dir {
        let conv = d.join("pipelines");
        if conv.is_dir() {
            return vec![conv];
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
        } => alias_add(
            name,
            pipeline,
            positional,
            defaults,
            description,
            force,
            aliases,
            ctx,
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

    let mut defaults_map: BTreeMap<String, String> = BTreeMap::new();
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
    // placeholders when we can find the pipeline. A missing pipeline is a
    // non-fatal warning so aliases can be authored before their target.
    match try_load_pipeline_params(&pipeline, ctx.as_deref())? {
        Some(pipeline_params) => {
            validate_alias_against_pipeline(
                &pipeline,
                &pipeline_params,
                &positional_vec,
                &defaults_map,
            )?;
            report_alias_coverage(&pipeline, &pipeline_params, &positional_vec, &defaults_map);
        }
        None => {
            eprintln!(
                "note: pipeline '{pipeline}' not found in `pipelines_dir` — saving alias without \
                 param validation. Once the pipeline is on disk, re-run `alias show {name}` to \
                 confirm bindings."
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
/// pipeline file cannot be located via the caller's ctx.
fn try_load_pipeline_params(
    pipeline: &str,
    ctx_override: Option<&Path>,
) -> Result<Option<Vec<String>>> {
    // Parse ctx if provided so `pipelines_dir` can point us at the pipelines.
    let ctx_path = resolve_ctx_path(ctx_override).filter(|p| p.exists());
    let ctx_cfg: Option<LocalContextConfig> = match &ctx_path {
        Some(p) => {
            let content = std::fs::read_to_string(p)
                .with_context(|| format!("Failed to read context file: {}", p.display()))?;
            Some(serde_yaml::from_str(&content).context("Failed to parse context YAML")?)
        }
        None => None,
    };

    let pipeline_dirs = resolve_pipeline_dirs(None, ctx_cfg.as_ref(), ctx_path.as_deref());
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
    defaults: &BTreeMap<String, String>,
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
    defaults: &BTreeMap<String, String>,
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

    // Print the stored alias YAML verbatim.
    let mut single = AliasMap::new();
    single.insert(name.clone(), def.clone());
    let yaml = serde_yaml::to_string(&single).context("Failed to render alias to YAML")?;
    print!("{yaml}");

    // If we can find the target pipeline, print an annotated view of each
    // `{param}` showing where it gets bound from (positional slot, default
    // value, or "flag-only at call time").
    if let Some(pipeline_params) = try_load_pipeline_params(&def.pipeline, ctx.as_deref())? {
        let annotations = annotate_alias_bindings(def, &pipeline_params);
        println!();
        println!(
            "Pipeline '{}' has {} parameter(s):",
            def.pipeline,
            pipeline_params.len()
        );
        let width = pipeline_params.iter().map(|s| s.len()).max().unwrap_or(0);
        for (param, note) in annotations {
            println!("  {param:<width$}  {note}");
        }
    }
    Ok(())
}

/// For each pipeline param, produce a short note describing how this alias
/// binds it: by positional slot, by default value, or "unbound" (the user
/// must pass it as a flag at call time). Extra positional/default entries in
/// the alias that do NOT match a real pipeline param are ignored here — they
/// are already rejected up front by [`validate_alias_against_pipeline`].
fn annotate_alias_bindings<'a>(
    alias: &'a AliasDef,
    pipeline_params: &'a [String],
) -> Vec<(&'a str, String)> {
    pipeline_params
        .iter()
        .map(|p| {
            let note = if let Some(idx) = alias.positional.iter().position(|n| n == p) {
                format!("positional[{idx}]")
            } else if let Some(v) = alias.defaults.get(p) {
                format!("default: {v:?}")
            } else {
                "flag-only (pass --{name}=VALUE at call time)".replace("{name}", p)
            };
            (p.as_str(), note)
        })
        .collect()
}

/// Print a `skardi <alias> --help`–style usage message: short description,
/// positional slots, flag-callable params (with defaults if any), control
/// flags, and a one-line example.
fn print_alias_help(alias_name: &str, def: &AliasDef, pipeline_params: Option<&[String]>) {
    println!("skardi {alias_name} — runs pipeline `{}`", def.pipeline);
    if let Some(desc) = &def.description {
        println!();
        println!("{desc}");
    }

    println!();
    if def.positional.is_empty() {
        println!("Positional args: (none)");
    } else {
        println!("Positional args:");
        for (i, p) in def.positional.iter().enumerate() {
            println!("  <{p}>   binds pipeline param `{p}` (positional[{i}])");
        }
    }

    if let Some(params) = pipeline_params {
        println!();
        println!("Pipeline params (override at call time with --name=VALUE):");
        let width = params.iter().map(|s| s.len()).max().unwrap_or(0);
        for p in params {
            let note = if let Some(idx) = def.positional.iter().position(|n| n == p) {
                format!("bound positionally (positional[{idx}])")
            } else if let Some(v) = def.defaults.get(p) {
                format!("default: {v:?}")
            } else {
                "required at call time".to_string()
            };
            println!("  --{p:<width$}  {note}");
        }
    }

    println!();
    println!("Control flags:");
    println!("  --ctx <PATH>       Context YAML (SKARDICONFIG env / ~/.skardi/config/ctx.yaml)");
    println!("  --aliases <PATH>   Override aliases file");
    println!("  --pipeline-dir <DIR>  Override pipeline discovery directory");

    println!();
    print!("Example: skardi {alias_name}");
    for p in &def.positional {
        print!(" <{p}>");
    }
    for p in pipeline_params.unwrap_or(&[]).iter() {
        let handled_positionally = def.positional.iter().any(|n| n == p);
        let has_default = def.defaults.contains_key(p);
        if !handled_positionally && !has_default {
            print!(" --{p}=...");
        }
    }
    println!();
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
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::{
        CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
    };
    use datafusion::datasource::MemTable;

    fn entry(catalog: &str, schema: &str, table: &str) -> TableEntry {
        TableEntry {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        }
    }

    fn defaults(catalog: &str, schema: &str) -> CatalogDefaults {
        CatalogDefaults {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
        }
    }

    fn df_defaults() -> CatalogDefaults {
        defaults("datafusion", "public")
    }

    #[test]
    fn display_name_uses_bare_for_default_catalog() {
        assert_eq!(
            entry("datafusion", "public", "users").display_name(&df_defaults()),
            "users"
        );
    }

    #[test]
    fn display_name_qualifies_non_default_catalog() {
        assert_eq!(
            entry("wiki", "main", "wiki_pages").display_name(&df_defaults()),
            "wiki.main.wiki_pages"
        );
    }

    #[test]
    fn display_name_honors_custom_defaults() {
        // If the session config names a non-`datafusion`/`public` default, the
        // rendering should follow it rather than hard-coded literals.
        let custom = defaults("my_app", "core");
        assert_eq!(
            entry("my_app", "core", "events").display_name(&custom),
            "events"
        );
        assert_eq!(
            entry("datafusion", "public", "events").display_name(&custom),
            "datafusion.public.events"
        );
    }

    #[test]
    fn select_tables_unqualified_unique_match() {
        let all = vec![
            entry("datafusion", "public", "users"),
            entry("wiki", "main", "wiki_pages"),
        ];
        let got = select_tables(&all, "wiki_pages", &df_defaults()).unwrap();
        assert_eq!(got, vec![entry("wiki", "main", "wiki_pages")]);
    }

    #[test]
    fn select_tables_unqualified_unknown_lists_available() {
        let all = vec![entry("datafusion", "public", "users")];
        let err = select_tables(&all, "ghost", &df_defaults())
            .unwrap_err()
            .to_string();
        assert!(err.contains("'ghost' not found"), "msg: {err}");
        assert!(err.contains("users"), "should list available: {err}");
    }

    #[test]
    fn select_tables_unqualified_ambiguous_requires_qualified_form() {
        // Same bare name in two different catalogs.
        let all = vec![entry("a", "main", "events"), entry("b", "main", "events")];
        let err = select_tables(&all, "events", &df_defaults())
            .unwrap_err()
            .to_string();
        assert!(err.contains("ambiguous"), "msg: {err}");
        assert!(err.contains("a.main.events"), "msg: {err}");
        assert!(err.contains("b.main.events"), "msg: {err}");
    }

    #[test]
    fn select_tables_qualified_exact_match() {
        let all = vec![
            entry("wiki", "main", "wiki_pages"),
            entry("wiki", "main", "wiki_pages_fts"),
        ];
        let got = select_tables(&all, "wiki.main.wiki_pages_fts", &df_defaults()).unwrap();
        assert_eq!(got, vec![entry("wiki", "main", "wiki_pages_fts")]);
    }

    #[test]
    fn select_tables_qualified_unknown_errors() {
        let all = vec![entry("wiki", "main", "wiki_pages")];
        let err = select_tables(&all, "wiki.main.ghost", &df_defaults())
            .unwrap_err()
            .to_string();
        assert!(err.contains("'wiki.main.ghost' not found"), "msg: {err}");
    }

    #[test]
    fn select_tables_partial_qualification_is_rejected_with_hint() {
        // `a.b` has a dot but isn't fully qualified — make sure the error tells
        // the user what form is expected rather than the old silent
        // fall-through to a confusing "not found".
        let all = vec![entry("wiki", "main", "wiki_pages")];
        let err = select_tables(&all, "wiki.wiki_pages", &df_defaults())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("bare name or fully-qualified"),
            "error should hint at expected form: {err}"
        );
        assert!(err.contains("2 parts"), "should count the parts: {err}");
    }

    #[test]
    fn select_tables_four_part_filter_is_rejected() {
        let all = vec![entry("wiki", "main", "wiki_pages")];
        let err = select_tables(&all, "a.b.c.d", &df_defaults())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("bare name or fully-qualified"),
            "error should hint at expected form: {err}"
        );
    }

    fn make_int_provider() -> Arc<MemTable> {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&arrow_schema),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap();
        Arc::new(MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap())
    }

    #[tokio::test]
    async fn enumerate_tables_walks_catalogs_schemas_and_default_table() {
        let ctx = SessionContext::new();
        // Table-mode source: lands in datafusion.public.
        ctx.register_table("flat", make_int_provider()).unwrap();

        // Catalog-mode source: simulate what register_sqlite_catalog produces.
        let catalog = Arc::new(MemoryCatalogProvider::new());
        let schema = Arc::new(MemorySchemaProvider::new());
        schema
            .register_table("wiki_pages".to_string(), make_int_provider())
            .unwrap();
        schema
            .register_table("wiki_pages_fts".to_string(), make_int_provider())
            .unwrap();
        catalog.register_schema("main", schema).unwrap();
        ctx.register_catalog("wiki", catalog);

        let got = enumerate_tables(&ctx);
        assert_eq!(
            got,
            vec![
                entry("datafusion", "public", "flat"),
                entry("wiki", "main", "wiki_pages"),
                entry("wiki", "main", "wiki_pages_fts"),
            ],
            "should surface tables under both default catalog and named catalog"
        );
    }

    #[tokio::test]
    async fn enumerate_tables_skips_system_schemas() {
        // A catalog that exposes an `information_schema` (as DataFusion does
        // when `datafusion.catalog.information_schema` is enabled, or as some
        // providers surface verbatim) should not leak those entries into
        // `--schema --all` output.
        let ctx = SessionContext::new();
        let catalog = Arc::new(MemoryCatalogProvider::new());

        let user_schema = Arc::new(MemorySchemaProvider::new());
        user_schema
            .register_table("wiki_pages".to_string(), make_int_provider())
            .unwrap();
        catalog.register_schema("main", user_schema).unwrap();

        let info_schema = Arc::new(MemorySchemaProvider::new());
        info_schema
            .register_table("tables".to_string(), make_int_provider())
            .unwrap();
        catalog
            .register_schema("information_schema", info_schema)
            .unwrap();

        let pg_schema = Arc::new(MemorySchemaProvider::new());
        pg_schema
            .register_table("pg_class".to_string(), make_int_provider())
            .unwrap();
        catalog.register_schema("pg_catalog", pg_schema).unwrap();

        ctx.register_catalog("wiki", catalog);

        let got = enumerate_tables(&ctx);
        assert_eq!(
            got,
            vec![entry("wiki", "main", "wiki_pages")],
            "system schemas should be filtered out"
        );
    }

    #[tokio::test]
    async fn catalog_defaults_from_ctx_matches_datafusion_defaults() {
        // Guards against the hard-coded `datafusion`/`public` literals we used
        // to rely on: if a future DataFusion version changes them, this test
        // will start failing and flag the rendering logic.
        let ctx = SessionContext::new();
        let defs = CatalogDefaults::from_ctx(&ctx);
        assert_eq!(defs.catalog, "datafusion");
        assert_eq!(defs.schema, "public");
    }

    #[test]
    fn validate_alias_rejects_unknown_positional() {
        let params = vec!["query".to_string(), "limit".to_string()];
        let defaults = BTreeMap::new();
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
        let mut defaults = BTreeMap::new();
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
        let mut defaults = BTreeMap::new();
        defaults.insert("limit".to_string(), "10".to_string());
        let positional = vec!["query".to_string()];
        validate_alias_against_pipeline("p", &params, &positional, &defaults).unwrap();
    }

    #[test]
    fn annotate_bindings_labels_positional_default_and_unbound() {
        let mut defaults = BTreeMap::new();
        defaults.insert("limit".to_string(), "10".to_string());
        let alias = AliasDef {
            pipeline: "p".to_string(),
            positional: vec!["query".to_string()],
            defaults,
            description: None,
        };
        let params = vec![
            "query".to_string(),
            "limit".to_string(),
            "text_query".to_string(),
        ];
        let got: Vec<(String, String)> = annotate_alias_bindings(&alias, &params)
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        assert_eq!(got[0], ("query".to_string(), "positional[0]".to_string()));
        assert_eq!(got[1].0, "limit");
        assert!(got[1].1.contains("default"));
        assert!(got[1].1.contains("10"));
        assert_eq!(got[2].0, "text_query");
        assert!(got[2].1.contains("flag-only"));
        assert!(got[2].1.contains("--text_query"));
    }

    // End-to-end guards for `skardi run` — drive `run_pipeline_with_params`
    // with a tempdir of pipeline YAMLs, no ctx, and assert we actually make
    // it through discovery → param validation → inline render → DataFusion
    // execution. These close the gap that existed when only the pure helpers
    // (extract_param_names, render_sql_with_inline_params, …) were
    // individually unit-tested.
    mod run_pipeline_e2e {
        use super::*;
        use tempfile::TempDir;

        fn write_pipeline(dir: &Path, filename: &str, yaml: &str) {
            std::fs::write(dir.join(filename), yaml).unwrap();
        }

        #[tokio::test]
        async fn runs_pipeline_with_inline_params() {
            let tmp = TempDir::new().unwrap();
            write_pipeline(
                tmp.path(),
                "echo.yaml",
                r#"metadata:
  name: "echo"
query: |
  SELECT {x} AS val, {msg} AS note
"#,
            );

            let params = vec![
                ("x".to_string(), ScalarValue::Int64(Some(42))),
                (
                    "msg".to_string(),
                    ScalarValue::Utf8(Some("hi there".to_string())),
                ),
            ];

            run_pipeline_with_params(None, Some(tmp.path().to_path_buf()), "echo", params)
                .await
                .expect("pipeline should execute cleanly");
        }

        #[tokio::test]
        async fn errors_when_required_param_missing() {
            let tmp = TempDir::new().unwrap();
            write_pipeline(
                tmp.path(),
                "needs.yaml",
                r#"metadata:
  name: "needs"
query: |
  SELECT {required} AS val
"#,
            );

            let err =
                run_pipeline_with_params(None, Some(tmp.path().to_path_buf()), "needs", vec![])
                    .await
                    .unwrap_err();
            let msg = format!("{err:?}");
            assert!(
                msg.contains("required"),
                "error should name the missing param: {msg}"
            );
        }

        #[tokio::test]
        async fn errors_when_pipeline_name_unknown() {
            let tmp = TempDir::new().unwrap();
            write_pipeline(
                tmp.path(),
                "only_this_one.yaml",
                r#"metadata:
  name: "only-this-one"
query: "SELECT 1"
"#,
            );

            let err = run_pipeline_with_params(
                None,
                Some(tmp.path().to_path_buf()),
                "does-not-exist",
                vec![],
            )
            .await
            .unwrap_err();
            let msg = format!("{err:?}");
            assert!(
                msg.contains("does-not-exist"),
                "error should name the missing pipeline: {msg}"
            );
            assert!(
                msg.contains("only-this-one"),
                "error should list known pipelines: {msg}"
            );
        }
    }
}
