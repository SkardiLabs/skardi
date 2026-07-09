use anyhow::Result;
use clap::Parser;
use skardi::pipeline::pipeline::Pipeline;
use skardi_server::{CliArgs, create_server, load_server_config, telemetry};
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Build the tracing filter from the given `RUST_LOG` value (`None` when the
/// variable is unset or invalid unicode), defaulting to `info`.
///
/// aws_config logs the AWS access key id in plaintext at INFO when resolving
/// credentials, so cap it at WARN unless RUST_LOG explicitly opts in.
fn build_env_filter(rust_log: Option<&str>) -> tracing_subscriber::EnvFilter {
    let mut env_filter = rust_log
        .and_then(|v| tracing_subscriber::EnvFilter::try_new(v).ok())
        .unwrap_or_else(|| "info".into());
    if !rust_log.is_some_and(|v| v.contains("aws_config")) {
        env_filter = env_filter.add_directive("aws_config=warn".parse().expect("valid directive"));
    }
    env_filter
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialise OpenTelemetry only when OTLP_ENDPOINT is explicitly set.
    let otlp_endpoint = std::env::var("OTLP_ENDPOINT").ok();
    let (_telemetry_guard, otel_layer) = match telemetry::init(otlp_endpoint.as_deref())? {
        Some((guard, tracer)) => (
            Some(guard),
            Some(tracing_opentelemetry::layer().with_tracer(tracer)),
        ),
        None => (None, None),
    };

    // Initialize tracing subscriber: fmt to stdout, plus OTel layer when enabled.
    tracing_subscriber::registry()
        .with(build_env_filter(std::env::var("RUST_LOG").ok().as_deref()))
        .with(
            tracing_subscriber::fmt::layer()
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE),
        )
        .with(otel_layer)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_filter_defaults_to_info_and_caps_aws_config() {
        let filter = build_env_filter(None).to_string();
        assert!(filter.contains("info"), "got {filter}");
        assert!(filter.contains("aws_config=warn"), "got {filter}");
    }

    #[test]
    fn env_filter_caps_aws_config_for_unrelated_rust_log() {
        let filter = build_env_filter(Some("debug")).to_string();
        assert!(filter.contains("debug"), "got {filter}");
        assert!(filter.contains("aws_config=warn"), "got {filter}");
    }

    #[test]
    fn env_filter_honors_explicit_aws_config_opt_in() {
        let filter = build_env_filter(Some("info,aws_config=debug")).to_string();
        assert!(filter.contains("aws_config=debug"), "got {filter}");
        assert!(!filter.contains("aws_config=warn"), "got {filter}");
    }

    #[test]
    fn env_filter_falls_back_to_info_on_invalid_rust_log() {
        let filter = build_env_filter(Some("not a [valid directive")).to_string();
        assert!(filter.contains("info"), "got {filter}");
        assert!(filter.contains("aws_config=warn"), "got {filter}");
    }
}
