use anyhow::{Context, Result};
use arrow::util::pretty::pretty_format_batches;
use async_trait::async_trait;
use clap::{Parser, Subcommand};
use datafusion::catalog::UrlTableFactory;
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
use serde::Deserialize;
use source::lance::knn_table_function::register_lance_knn_udtf;
use source::providers::{
    iceberg::register_iceberg_table, lance::register_lance_table, mongo::register_mongo_tables,
    mysql::register_mysql_tables, sqlx::postgres::register_postgres_tables,
};
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use url::Url;

/// Shared registry mapping table names to Lance datasets, used by the `lance_knn` UDTF.
type DatasetRegistry = Arc<RwLock<HashMap<String, Arc<Dataset>>>>;

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
}

#[derive(Debug, Deserialize)]
struct LocalContextConfig {
    data_sources: Vec<LocalDataSource>,
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
                reg.insert(table_name, Arc::clone(&dataset_arc));
            }

            let provider: Arc<dyn TableProvider> = dataset_arc;
            return Ok(Some(provider));
        }

        // Delegate all other formats to the default factory
        self.inner.try_new(url).await
    }
}

/// Create a new SessionContext with custom URL table support (built-in files + Lance)
/// and the `lance_knn` UDTF registered. Returns the context and the shared dataset registry.
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
    let ctx: SessionContext = base_ctx
        .into_state_builder()
        .with_session_id(session_id)
        .with_catalog_list(catalog_list)
        .build()
        .into();

    factory.session_store().with_state(ctx.state_weak_ref());

    // Register the lance_knn table function
    register_lance_knn_udtf(&ctx, Arc::clone(&dataset_registry));

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
    }
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
            register_mongo_tables(session_ctx, &source.name, conn_str, source.options.as_ref())
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
