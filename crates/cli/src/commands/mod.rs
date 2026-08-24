//! Subcommand implementations. Each submodule owns one subcommand's request
//! construction and response handling; `main.rs` owns the `Commands` enum,
//! clap surface, and dispatch.

pub mod config;
pub mod health;
pub mod jobs;
pub mod pipeline;
pub mod query;
pub mod run;
pub mod schema;
