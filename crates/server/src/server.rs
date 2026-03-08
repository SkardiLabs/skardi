use anyhow::Result;
use axum::{
    routing::{get, post},
    Router,
};
use datafusion::prelude::SessionContext;
use skardi_engine::datafusion::DataFusionEngine;
use std::sync::{Arc, RwLock};
use tokio::net::TcpListener;
use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer};

use crate::config::{register_onnx_predict_udf, ServerConfig};
use crate::handlers::{
    execute_pipeline_by_name, get_data_sources, get_pipelines_info, health_check, list_pipelines,
    pipeline_health_check, register_pipeline,
};

/// Shared application state containing pipeline and engine
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<RwLock<ServerConfig>>,
    pub engine: Arc<DataFusionEngine>,
    /// SessionContext for pipeline loading (shared with engine)
    pub session_ctx: Arc<SessionContext>,
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

/// Setup application state with engine and data source registration
pub async fn setup_app_state(config: ServerConfig) -> Result<AppState> {
    tracing::info!("Setting up application state");

    // Create optimizer registry for conditional optimizer registration
    let optimizer_registry = Arc::new(crate::OptimizerRegistry::new());

    // Create federation-enabled DataFusion SessionState
    let state = datafusion_federation::default_session_state();

    // Get all physical optimizer rules from the registry based on data sources
    let additional_optimizers =
        optimizer_registry.get_physical_optimizer_rules(&config.data_sources);

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
    register_onnx_predict_udf(&mut session_ctx);

    // Wrap SessionContext in Arc for sharing between engine and pipeline loading
    let session_ctx_arc = Arc::new(session_ctx);

    // Create DataFusion engine with the shared Arc<SessionContext>
    let engine = Arc::new(DataFusionEngine::new_with_arc(session_ctx_arc.clone()));

    // Create shared application state with RwLock for runtime updates
    let app_state = AppState {
        config: Arc::new(RwLock::new(config)),
        engine,
        session_ctx: session_ctx_arc,
    };

    tracing::info!("Application state setup completed successfully");

    Ok(app_state)
}

/// Configure all application routes
pub fn configure_routes(state: AppState) -> Router {
    tracing::info!("Configuring HTTP routes");

    Router::new()
        .route("/health", get(health_check))
        .route("/health/:name", get(pipeline_health_check))
        .route("/pipelines", get(list_pipelines))
        .route("/pipeline/:name", get(get_pipelines_info))
        .route("/register_pipeline", post(register_pipeline))
        .route("/data_source", get(get_data_sources))
        .route("/:name/execute", post(execute_pipeline_by_name))
        .with_state(state)
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
    use pipeline::pipeline::{Pipeline, StandardPipeline};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn create_test_pipeline() -> StandardPipeline {
        let temp_dir = TempDir::new().unwrap();
        let pipeline_content = r#"
metadata:
  name: "test-pipeline"
  version: "1.0.0"
  description: "Test pipeline for server testing"

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
        }];

        let pipeline = create_test_pipeline().await;
        let mut pipelines = std::collections::HashMap::new();
        pipelines.insert(pipeline.name().to_string(), pipeline);

        let config = ServerConfig {
            pipelines,
            data_sources,

            args: CliArgs {
                pipeline_path: Some(PathBuf::from("test-pipeline.yaml")),
                ctx_file: None,
                port: 8080,
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
            data_sources: vec![],

            args: CliArgs {
                pipeline_path: Some(PathBuf::from("test-pipeline.yaml")),
                ctx_file: None,
                port: 8080,
            },
        }
    }

    #[tokio::test]
    async fn test_setup_app_state_with_data_sources() {
        let (config, _temp_dir) = create_test_config_with_data_sources_and_temp_dir().await;
        let data_source_count = config.data_sources.len();

        let result = setup_app_state(config).await;

        assert!(result.is_ok());
        let app_state = result.unwrap();

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
