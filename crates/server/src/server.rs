use anyhow::{Context, Result};
use axum::{
    Router,
    routing::{get, post},
};
use datafusion::prelude::SessionContext;
use skardi::engine::datafusion::DataFusionEngine;
use skardi::jobs::{JobExecutor, JobStore, SqliteJobStore};
use skardi::sources::DataSourceType;
use skardi::sources::sql_validator::AdhocSqlPolicy;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio::net::TcpListener;
use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer};

use crate::auth::mode::AuthMode;
use crate::config::ServerConfig;
#[cfg(feature = "candle")]
use crate::config::register_candle_udf;
#[cfg(feature = "chunking")]
use crate::config::register_chunk_udf;
#[cfg(feature = "gguf")]
use crate::config::register_gguf_udf;
#[cfg(feature = "llm-extract")]
use crate::config::register_llm_extract_udf;
#[cfg(feature = "onnx")]
use crate::config::register_onnx_predict_udf;
#[cfg(feature = "remote-embed")]
use crate::config::register_remote_embed_udf;
use crate::gui::serve_dashboard;
use crate::handlers::health_check;
use crate::jobs_handlers::{cancel_job_run, get_job_run, list_job_runs, list_jobs, submit_job_run};
use crate::logging::QUERY_PLAN_TARGET;
use crate::metrics::PipelineMetrics;
use crate::pipeline_handlers::{
    execute_pipeline_by_name, get_data_sources, get_pipelines_info, list_pipelines,
    pipeline_health_check,
};
use crate::query_audit::QueryAuditStore;
use crate::query_handlers::execute_query;
#[cfg(test)]
use crate::semantics::SemanticsRegistry;
use crate::{OptimizerRegistry, auth::layer::AuthLayer};

/// Shared application state containing pipeline and engine
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<RwLock<ServerConfig>>,
    pub engine: Arc<DataFusionEngine>,
    /// SessionContext for pipeline loading (shared with engine)
    pub session_ctx: Arc<SessionContext>,
    /// OTel metrics instruments (request counter + latency histogram)
    pub metrics: PipelineMetrics,
    /// Active authentication layer (NoAuth by default)
    pub auth_layer: AuthLayer,
    /// Jobs executor + run ledger. `None` when the server was started
    /// without `--jobs`, which disables every `/jobs/*` endpoint.
    pub jobs: Option<Arc<JobExecutor>>,
    /// Statement policy for the ad-hoc `/query` endpoint. Derived from the
    /// data sources' access modes once at startup by [`AppState::new`]; see
    /// [`crate::config::adhoc_policy_from_sources`] for why a snapshot is
    /// safe (no runtime writer mutates access modes).
    pub adhoc_policy: Arc<AdhocSqlPolicy>,
    /// Durable audit ledger for `/query`. `Some` only when the operator passed
    /// `--query-audit-db`. `None` means raw SQL is never persisted anywhere.
    /// See [`crate::query_audit`].
    pub query_audit: Option<Arc<QueryAuditStore>>,
}

impl AppState {
    /// Assemble the shared state, deriving the ad-hoc `/query` policy from the
    /// config's data sources. Centralizing construction here keeps the derived
    /// policy from drifting across the many call sites that build an
    /// `AppState` (handlers, tests, integration harnesses).
    /// `query_audit` is opened by [`setup_app_state`] rather than here: opening
    /// it is async and *fallible by design* (an operator who configured an
    /// audit trail must never get a server that silently runs without one), so
    /// it cannot be derived inside this infallible constructor.
    pub fn new(
        config: ServerConfig,
        engine: Arc<DataFusionEngine>,
        session_ctx: Arc<SessionContext>,
        auth_layer: AuthLayer,
        jobs: Option<Arc<JobExecutor>>,
        query_audit: Option<Arc<QueryAuditStore>>,
    ) -> Self {
        let adhoc_policy = Arc::new(crate::config::adhoc_policy_from_sources(
            &config.data_sources,
        ));
        Self {
            config: Arc::new(RwLock::new(config)),
            engine,
            session_ctx,
            metrics: PipelineMetrics::new(),
            auth_layer,
            jobs,
            adhoc_policy,
            query_audit,
        }
    }
}

/// Main server creation function - Primary public interface
pub async fn create_server(config: ServerConfig) -> Result<()> {
    tracing::info!("Creating Skardi server");

    // Extract port before moving config
    let port = config.args.port;

    // Step 1: Setup application state with engine and data sources
    let app_state = setup_app_state(config).await?;
    tracing::info!("Application state setup completed");

    // Step 2: Configure routes with the application state
    let router = configure_routes(app_state);
    tracing::info!("Routes configured successfully");

    // Step 3: Apply middleware stack
    let router = configure_middleware(router);
    tracing::info!("Middleware configured successfully");

    // Step 4: Start HTTP listener and serve requests
    tracing::info!("Starting server on port {}", port);
    start_listener(router, port).await?;

    Ok(())
}

/// Setup application state with engine and data source registration.
pub async fn setup_app_state(config: ServerConfig) -> Result<AppState> {
    tracing::info!("Setting up application state");

    // Create optimizer registry for conditional optimizer registration
    let optimizer_registry = Arc::new(OptimizerRegistry::new());

    // Create federation-enabled DataFusion SessionState
    let state = datafusion_federation::default_session_state();

    // Get all physical optimizer rules from the registry based on data sources
    let additional_optimizers =
        optimizer_registry.get_physical_optimizer_rules(&config.data_sources);

    if !additional_optimizers.is_empty() {
        tracing::info!(
            "Adding {} physical optimizer(s) to SessionState",
            additional_optimizers.len()
        );
    }
    let state = instrument_session_state(state, additional_optimizers);

    let mut session_ctx = SessionContext::new_with_state(state);

    // Register data sources from configuration
    crate::config::register_data_sources_with_registry(
        &mut session_ctx,
        &config.data_sources,
        &optimizer_registry,
    )
    .await?;

    tracing::info!(
        "Registered {} data sources with DataFusion",
        config.data_sources.len()
    );

    // Register UDFs (user-defined functions) for data sources
    optimizer_registry
        .register_udfs(&mut session_ctx, &config.data_sources)
        .map_err(|e| anyhow::anyhow!("Failed to register UDFs: {}", e))?;

    // Register onnx_predict UDF (lazy — models loaded on first call from inline path)
    #[cfg(feature = "onnx")]
    register_onnx_predict_udf(&mut session_ctx);

    // Register remote_embed UDF (OpenAI, Gemini, Voyage, Mistral)
    #[cfg(feature = "remote-embed")]
    register_remote_embed_udf(&mut session_ctx);
    // Register llm_extract UDF (structured extraction: DeepSeek/GLM/Gemini/OpenAI/Anthropic)
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

    // Build auth layer and register auth.users / auth.sessions on the runtime SessionContext.
    let auth_layer = AuthLayer::build(&AuthMode::from_env()).await?;
    if let Some(auth) = auth_layer.as_better_auth() {
        crate::auth::bridge::register_auth_tables(&mut session_ctx, auth.clone())?;
    }

    // Wrap SessionContext in Arc for sharing between engine and pipeline loading
    let session_ctx_arc = Arc::new(session_ctx);

    // Create DataFusion engine with the shared Arc<SessionContext>
    let engine = Arc::new(DataFusionEngine::new_with_arc(session_ctx_arc.clone()));

    // Build the jobs executor + run ledger bundle if --jobs was given.
    // Runs this *after* the SessionContext is populated so destinations
    // resolve against the real table registry.
    let jobs_bundle = if !config.jobs.is_empty() {
        let jobs_db_path = resolve_jobs_db_path(config.args.jobs_db_path.as_ref())
            .map_err(|e| anyhow::anyhow!("Failed to resolve jobs.db path: {e}"))?;
        tracing::info!("Opening jobs ledger at {}", jobs_db_path.display());
        let store = Arc::new(SqliteJobStore::open(&jobs_db_path).await?);
        let reconciled = store
            .reconcile_orphaned("server restarted before run completed")
            .await?;
        if reconciled > 0 {
            tracing::warn!(
                "Reconciled {} orphaned job run(s) from a previous server crash",
                reconciled
            );
        }

        let data_source_types = config
            .data_sources
            .iter()
            .map(|ds| (ds.name.clone(), ds.source_type))
            .collect();
        // Only Lance destinations care about a physical path today; other
        // source kinds can opt in later by adding their own entries here.
        let source_paths: HashMap<String, String> = config
            .data_sources
            .iter()
            .filter(|ds| matches!(ds.source_type, DataSourceType::Lance))
            .filter_map(|ds| ds.path.to_str().map(|p| (ds.name.clone(), p.to_string())))
            .collect();

        Some(Arc::new(JobExecutor::new(
            config.jobs.clone(),
            store as Arc<dyn skardi::jobs::JobStore>,
            session_ctx_arc.clone(),
            data_source_types,
            source_paths,
        )))
    } else {
        None
    };

    // Open the `/query` audit ledger if the operator asked for one. Every
    // failure here is fatal: a configured audit trail that silently doesn't
    // record would be worse than none at all.
    let query_audit = match config.args.query_audit_db.as_ref() {
        Some(path) => {
            tracing::info!("Opening /query audit ledger at {}", path.display());
            let store = Arc::new(QueryAuditStore::open(path).await.with_context(|| {
                format!("Failed to open --query-audit-db at {}", path.display())
            })?);
            let orphaned = store
                .reconcile_orphaned("server restarted before the query completed")
                .await?;
            if orphaned > 0 {
                tracing::warn!(
                    "Reconciled {} /query audit record(s) left in flight by a previous run",
                    orphaned
                );
            }
            if let Some(days) = config.args.query_audit_retention_days {
                spawn_query_audit_retention(Arc::clone(&store), days).await?;
            }
            Some(store)
        }
        None => None,
    };

    // Create shared application state with RwLock for runtime updates
    let app_state = AppState::new(
        config,
        engine,
        session_ctx_arc,
        auth_layer,
        jobs_bundle,
        query_audit,
    );

    tracing::info!("Application state setup completed successfully");

    Ok(app_state)
}

/// Prune audit records older than `retention_days` now, then hourly for the
/// life of the process.
///
/// The first prune is awaited so a broken retention setup surfaces at startup
/// alongside every other audit failure; subsequent ones only warn, since a
/// transient prune failure must not take a running server down.
async fn spawn_query_audit_retention(
    store: Arc<QueryAuditStore>,
    retention_days: u32,
) -> Result<()> {
    let retention = chrono::Duration::days(retention_days as i64);
    let pruned = store.prune_before(chrono::Utc::now() - retention).await?;
    tracing::info!(
        "Query-audit retention: {} day(s); pruned {} record(s) at startup",
        retention_days,
        pruned
    );

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(3600));
        ticker.tick().await; // fires immediately; the startup prune covered it
        loop {
            ticker.tick().await;
            match store.prune_before(chrono::Utc::now() - retention).await {
                Ok(n) if n > 0 => tracing::info!("Query-audit retention pruned {n} record(s)"),
                Ok(_) => {}
                Err(e) => tracing::warn!("Query-audit retention prune failed: {e}"),
            }
        }
    });
    Ok(())
}

/// Attach datafusion-tracing instrumentation (execution nodes + analyzer /
/// optimizer rule phases) to `state`, appending `additional_optimizers` ahead
/// of the tracing rule so tracing sees the fully optimized plan.
///
/// Both instrumentations are pinned to [`QUERY_PLAN_TARGET`] rather than the
/// macros' default of `module_path!()`: the spans record plan node displays
/// (`ProjectionExec: expr=[…]`), which reproduce the statement's literal
/// values, and a dedicated target is what lets
/// [`crate::logging::build_env_filter`] hold them at INFO. Exposed so
/// `tests/query_plan_logging.rs` can assert against the real instrumentation
/// instead of a lookalike.
pub fn instrument_session_state(
    state: datafusion::execution::SessionState,
    additional_optimizers: Vec<
        Arc<dyn datafusion::physical_optimizer::PhysicalOptimizerRule + Send + Sync>,
    >,
) -> datafusion::execution::SessionState {
    // skardi rules first, tracing rule last.
    let mut physical_optimizer_rules = state.physical_optimizers().to_vec();
    physical_optimizer_rules.extend(additional_optimizers);
    physical_optimizer_rules.push(datafusion_tracing::instrument_with_debug_spans!(
        target: QUERY_PLAN_TARGET,
        options: datafusion_tracing::InstrumentationOptions::default()
    ));

    let state = datafusion::execution::SessionStateBuilder::new_from_existing(state)
        .with_physical_optimizer_rules(physical_optimizer_rules)
        .build();

    datafusion_tracing::instrument_rules_with_debug_spans!(
        target: QUERY_PLAN_TARGET,
        options: datafusion_tracing::RuleInstrumentationOptions::full(),
        state: state
    )
}

/// Resolve where the SQLite run ledger should live. Explicit `--jobs-db`
/// beats `$HOME/.skardi/jobs.db`; if neither is available we error out
/// rather than silently falling back to the cwd.
fn resolve_jobs_db_path(explicit: Option<&PathBuf>) -> Result<PathBuf> {
    if let Some(p) = explicit {
        return Ok(p.clone());
    }
    let home = dirs::home_dir().ok_or_else(|| {
        anyhow::anyhow!("Could not locate a home directory; pass --jobs-db explicitly")
    })?;
    Ok(home.join(".skardi").join("jobs.db"))
}

/// Configure all application routes
pub fn configure_routes(state: AppState) -> Router {
    tracing::info!("Configuring HTTP routes");

    let mut router = Router::new()
        .route("/", get(serve_dashboard))
        .route("/health", get(health_check))
        .route("/health/:name", get(pipeline_health_check))
        .route("/pipelines", get(list_pipelines))
        .route("/pipeline/:name", get(get_pipelines_info))
        .route("/data_source", get(get_data_sources))
        .route("/query", post(execute_query))
        .route("/:name/execute", post(execute_pipeline_by_name));

    // Jobs endpoints — mounted unconditionally so the CLI can discover
    // "jobs disabled" via a 503 rather than a 404 when the server was
    // started without --jobs. Handlers short-circuit on `state.jobs = None`.
    router = router
        .route("/jobs", get(list_jobs))
        .route("/jobs/:name/run", post(submit_job_run))
        .route("/jobs/runs", get(list_job_runs))
        .route("/jobs/runs/:run_id", get(get_job_run))
        .route("/jobs/runs/:run_id/cancel", post(cancel_job_run));

    if state.auth_layer.is_enabled() {
        tracing::info!("Auth enabled: mounting /api/auth/* routes");
        router = router.route(
            "/api/auth/*path",
            axum::routing::any(crate::auth::routes::auth_handler),
        );
    }

    router.with_state(state)
}

/// Setup middleware stack (tracing, CORS, etc.)
pub fn configure_middleware(router: Router) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::any())
        .allow_methods(AllowMethods::mirror_request())
        .allow_headers(AllowHeaders::mirror_request());

    router.layer(cors)
}

/// Start HTTP listener and serve requests
pub async fn start_listener(router: Router, port: u16) -> Result<()> {
    let addr = format!("0.0.0.0:{}", port);

    tracing::info!("Starting HTTP server on {}", addr);

    // Create TCP listener
    let listener = TcpListener::bind(&addr)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to bind to {}: {}", addr, e))?;

    tracing::info!("Server successfully bound to {}", addr);
    tracing::info!("Server is ready to accept connections");

    // Start serving requests
    axum::serve(listener, router)
        .await
        .map_err(|e| anyhow::anyhow!("Server error: {}", e))?;

    tracing::info!("Server shutdown completed");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AccessMode, CliArgs, DataSource, DataSourceType};
    use datafusion::prelude::SessionContext;
    use skardi::pipeline::pipeline::{Pipeline, StandardPipeline};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn create_test_pipeline() -> StandardPipeline {
        let temp_dir = TempDir::new().unwrap();
        let pipeline_content = r#"
kind: pipeline
metadata:
  name: "test-pipeline"
  version: "1.0.0"
  description: "Test pipeline for server testing"
spec:
  query: |
    SELECT date
    FROM test_data
    WHERE date >= {date_filter}
"#;

        let pipeline_path = temp_dir.path().join("test-pipeline.yaml");
        fs::write(&pipeline_path, pipeline_content).unwrap();

        // Create SessionContext with mock test_data table for schema inference
        let ctx = Arc::new(SessionContext::new());

        // Create a simple mock table with date column
        use arrow::array::Date32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        let schema = Schema::new(vec![Field::new("date", DataType::Date32, false)]);
        let date_array = Date32Array::from(vec![18628]); // 2021-01-01
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(date_array)]).unwrap();

        ctx.register_batch("test_data", batch).unwrap();
        StandardPipeline::load_from_file(&pipeline_path, ctx)
            .await
            .unwrap()
    }

    async fn create_test_config_with_data_sources_and_temp_dir() -> (ServerConfig, TempDir) {
        let temp_dir = TempDir::new().unwrap();

        // Create test data files
        let data_dir = temp_dir.path().join("data");
        fs::create_dir_all(&data_dir).unwrap();

        let csv_path = data_dir.join("test.csv");
        fs::write(&csv_path, "date,value\n2023-01-01,1.0\n2023-01-02,2.0\n").unwrap();

        let data_sources = vec![DataSource {
            name: "test_data".to_string(),
            source_type: DataSourceType::Csv,
            path: csv_path,
            connection_string: None,
            schema: None,
            options: None,
            access_mode: AccessMode::default(),
            enable_cache: false,
            hierarchy_level: Default::default(),
            description: None,
            open_connector: None,
        }];

        let pipeline = create_test_pipeline().await;
        let mut pipelines = std::collections::HashMap::new();
        pipelines.insert(pipeline.name().to_string(), pipeline);

        let config = ServerConfig {
            pipelines,
            jobs: std::collections::HashMap::new(),
            data_sources,
            semantics: SemanticsRegistry::default(),
            args: CliArgs {
                pipeline_path: Some(PathBuf::from("test-pipeline.yaml")),
                jobs_path: None,
                jobs_db_path: None,
                ctx_file: None,
                semantics_path: None,
                port: 8080,
                query_audit_db: None,
                query_audit_retention_days: None,
            },
        };

        (config, temp_dir)
    }

    async fn create_test_config_without_data_sources() -> ServerConfig {
        let pipeline = create_test_pipeline().await;
        let mut pipelines = std::collections::HashMap::new();
        pipelines.insert(pipeline.name().to_string(), pipeline);

        ServerConfig {
            pipelines,
            jobs: std::collections::HashMap::new(),
            data_sources: vec![],
            semantics: SemanticsRegistry::default(),
            args: CliArgs {
                pipeline_path: Some(PathBuf::from("test-pipeline.yaml")),
                jobs_path: None,
                jobs_db_path: None,
                ctx_file: None,
                semantics_path: None,
                port: 8080,
                query_audit_db: None,
                query_audit_retention_days: None,
            },
        }
    }

    fn assert_no_auth(state: &AppState) {
        assert!(!state.auth_layer.is_enabled());
    }

    #[tokio::test]
    async fn test_setup_app_state_with_data_sources() {
        let (config, _temp_dir) = create_test_config_with_data_sources_and_temp_dir().await;
        let data_source_count = config.data_sources.len();

        let result = setup_app_state(config).await;

        assert!(result.is_ok());
        let app_state = result.unwrap();
        assert_no_auth(&app_state);

        // Verify AppState structure
        let config = app_state.config.read().unwrap();
        assert_eq!(config.data_sources.len(), data_source_count);
        assert_eq!(config.pipelines.len(), 1);
        let pipeline = config.pipelines.get("test-pipeline").unwrap();
        assert_eq!(pipeline.name(), "test-pipeline");
        assert_eq!(pipeline.version(), "1.0.0");
        assert_eq!(config.args.port, 8080);

        // Verify engine was created
        // Note: Can't test engine functionality easily since register_data_sources is stubbed in MVP
    }

    #[tokio::test]
    async fn test_setup_app_state_without_data_sources() {
        let config = create_test_config_without_data_sources().await;

        let result = setup_app_state(config).await;

        assert!(result.is_ok());
        let app_state = result.unwrap();
        assert_no_auth(&app_state);

        // Verify AppState with empty data sources
        let config = app_state.config.read().unwrap();
        assert_eq!(config.data_sources.len(), 0);
        assert_eq!(config.pipelines.len(), 1);
        let pipeline = config.pipelines.get("test-pipeline").unwrap();
        assert_eq!(pipeline.name(), "test-pipeline");
        assert_eq!(pipeline.version(), "1.0.0");
        assert_eq!(config.args.port, 8080);

        // Verify engine was still created successfully
        // Note: Can't test engine functionality easily since register_data_sources is stubbed in MVP
    }

    #[tokio::test]
    async fn test_app_state_clone() {
        let config = create_test_config_without_data_sources().await;
        let app_state = setup_app_state(config).await.unwrap();

        // Test that AppState can be cloned (important for Axum shared state)
        let cloned_state = app_state.clone();

        let config1 = app_state.config.read().unwrap();
        let config2 = cloned_state.config.read().unwrap();
        assert_eq!(config1.pipelines.len(), config2.pipelines.len());
        assert_eq!(config1.args.port, config2.args.port);
    }
}
