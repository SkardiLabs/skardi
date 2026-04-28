pub mod auth;
pub mod config;
pub mod handlers;
pub mod jobs_handlers;
pub mod metrics;
pub mod optimizer_registry;
pub mod pipeline_handlers;
pub mod remote_storage;
pub mod server;
pub mod telemetry;

// Re-export the relocated semantics module under its old path so existing
// `crate::semantics::...` imports keep working without churning every
// caller. The implementation lives in `crates/skardi`.
pub use skardi::semantics;

// Re-export public types for easy access
pub use config::{
    CliArgs, DataSource, DataSourceType, ServerConfig, load_server_config, register_data_sources,
};

// Re-export optimizer registry
pub use optimizer_registry::OptimizerRegistry;

// Re-export server functions for main.rs
pub use server::create_server;
