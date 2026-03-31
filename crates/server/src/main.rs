use anyhow::Result;
use clap::Parser;
use skardi::pipeline::pipeline::Pipeline;
use skardi_server::{create_server, load_server_config, telemetry, CliArgs};
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialise OpenTelemetry (traces + metrics) before the tracing subscriber
    let otlp_endpoint = std::env::var("OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string());
    let (_telemetry_guard, tracer) = telemetry::init(&otlp_endpoint)?;

    // Initialize tracing subscriber: fmt to stdout + OTel trace export
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "info".into()
            }),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE),
        )
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();

    info!("🚀 Starting Skardi Online Serving Pipeline Server");

    // Parse command-line arguments
    let args = CliArgs::parse();
    info!("📋 CLI Arguments parsed successfully");
    info!("   Pipeline path: {:?}", args.pipeline_path);
    info!("   Context file: {:?}", args.ctx_file);
    info!("   Port: {}", args.port);

    // Load server configuration (pipelines + context)
    info!("⚙️ Loading server configuration...");
    let config = match load_server_config(args).await {
        Ok(config) => {
            info!("✅ Server configuration loaded successfully");
            if config.pipelines.is_empty() {
                info!("   Pipelines: None (register pipelines via config at server start)");
            } else {
                info!("   Pipelines loaded: {}", config.pipelines.len());
                for (name, pipeline) in &config.pipelines {
                    info!(
                        "     - {} (version: {}) -> /{}/execute",
                        name,
                        pipeline.version(),
                        name
                    );
                }
            }
            info!("   Data sources: {}", config.data_sources.len());
            config
        }
        Err(e) => {
            error!("❌ Failed to load server configuration: {}", e);
            // Print the full error chain for debugging
            let mut source = e.source();
            let mut depth = 1;
            while let Some(err) = source {
                error!("   Caused by (level {}): {}", depth, err);
                source = err.source();
                depth += 1;
            }
            error!("💡 Please check your pipeline and context files");
            std::process::exit(1);
        }
    };

    // Start the server
    info!("🌐 Starting HTTP server...");
    if let Err(e) = create_server(config).await {
        error!("❌ Server failed to start: {}", e);
        std::process::exit(1);
    }

    info!("👋 Skardi server shutting down");
    Ok(())
}
