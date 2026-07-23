//! `skardi` is a thin HTTP client for skardi-server: every command issues one
//! HTTP request to a running server and renders the response. It holds no
//! local query engine, catalog, or storage access — all of that lives in
//! skardi-server. This module defines the global connection flags shared by
//! all subcommands, the `Commands` enum, and dispatch; each subcommand's
//! request construction and response handling lives in `commands::*`.

use clap::{Parser, Subcommand};
use client::{ApiClient, ApiError};
use commands::jobs::JobCmd;
use commands::pipeline::PipelineCmd;
use config::ClientConfig;
use std::path::PathBuf;
use std::process::ExitCode;

mod client;
mod commands;
mod config;
mod output;
mod params;

/// Command-line interface for interacting with a skardi-server instance.
#[derive(Parser, Debug)]
#[command(name = "skardi", version, about, long_about = None)]
struct Cli {
    /// overrides $SKARDI_SERVER_URL and ~/.skardi/config.yaml; default http://127.0.0.1:8080
    #[arg(long, global = true, value_name = "URL")]
    server: Option<String>,

    /// overrides $SKARDI_API_TOKEN and ~/.skardi/config.yaml
    #[arg(long, global = true, value_name = "TOKEN")]
    token: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

/// Subcommands. `Query` and `Run` exist so far — Task 9 adds the rest.
#[derive(Subcommand, Debug)]
enum Commands {
    /// Run ad-hoc SQL against the server and print the result.
    Query {
        /// inline SQL text
        #[arg(short = 'e', long, value_name = "SQL")]
        sql: Option<String>,

        /// read SQL from a file (takes precedence over -e when both are given)
        #[arg(short = 'f', long, value_name = "PATH")]
        file: Option<PathBuf>,

        /// cap the number of returned rows (server default: 1000)
        #[arg(long, value_name = "N")]
        max_rows: Option<usize>,

        /// render results as a table instead of JSON
        #[arg(long)]
        table: bool,
    },

    /// Execute a named server pipeline and print the result.
    Run {
        /// pipeline name (see `skardi pipeline list`)
        name: String,

        /// inline JSON body, @FILE to read from a file, or - to read stdin
        #[arg(short = 'd', long, value_name = "JSON|@FILE|-")]
        data: Option<String>,

        /// NAME=VALUE parameter (repeatable); values are parsed as JSON
        /// first (numbers, booleans, arrays, null, quoted strings) and fall
        /// back to a plain string otherwise; -p overrides matching --data keys
        #[arg(short = 'p', long = "param", value_name = "NAME=VALUE")]
        params: Vec<String>,

        /// render results as a table instead of JSON
        #[arg(long)]
        table: bool,
    },

    /// List pipelines, or show one pipeline's definition.
    Pipeline {
        #[command(subcommand)]
        cmd: PipelineCmd,
    },

    /// Submit, inspect, list, or cancel job runs on the server.
    Job {
        #[command(subcommand)]
        cmd: JobCmd,
    },

    /// Show the server's data source schema.
    Schema,

    /// Show overall server health, or one pipeline's health.
    Health {
        /// pipeline name (omit for overall server health)
        name: Option<String>,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let config = ClientConfig::resolve(cli.server, cli.token);

    match dispatch(cli.command, &config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err:#}");
            match err.downcast_ref::<ApiError>() {
                Some(ApiError::Connect { .. }) => ExitCode::from(2),
                _ => ExitCode::FAILURE,
            }
        }
    }
}

/// Construct one `ApiClient` from `config` and dispatch `command` to its
/// implementation.
async fn dispatch(command: Commands, config: &ClientConfig) -> anyhow::Result<()> {
    let client = ApiClient::new(config)?;

    match command {
        Commands::Query {
            sql,
            file,
            max_rows,
            table,
        } => commands::query::run(&client, sql, file, max_rows, table).await,

        Commands::Run {
            name,
            data,
            params,
            table,
        } => commands::run::run(&client, &name, data.as_deref(), &params, table).await,

        Commands::Pipeline { cmd } => commands::pipeline::run(&client, cmd).await,

        Commands::Job { cmd } => commands::jobs::run(&client, cmd).await,

        Commands::Schema => commands::schema::run(&client).await,

        Commands::Health { name } => commands::health::run(&client, name.as_deref()).await,
    }
}
