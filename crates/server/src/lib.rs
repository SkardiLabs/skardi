pub mod auth;
pub mod config;
pub mod handlers;
pub mod metrics;
pub mod optimizer_registry;
pub mod remote_storage;
pub mod server;
pub mod telemetry;

// Re-export public types for easy access
pub use config::{
    load_server_config, register_data_sources, CliArgs, DataSource, DataSourceType, ServerConfig,
};

// Re-export optimizer registry
pub use optimizer_registry::OptimizerRegistry;

// Re-export server functions for main.rs
pub use server::create_server;
