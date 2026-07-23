//! Subcommand implementations. Each submodule owns one subcommand's request
//! construction and response handling; `main.rs` owns the `Commands` enum,
//! clap surface, and dispatch.

pub mod query;
pub mod run;
