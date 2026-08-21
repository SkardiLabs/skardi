//! `skardi` is a thin HTTP client for skardi-server: every command issues one
//! HTTP request to a running server and renders the response. It holds no
//! local query engine, catalog, or storage access — all of that lives in
//! skardi-server. This module defines the global connection flags shared by
//! all subcommands, the `Commands` enum, and dispatch; each subcommand's
//! request construction and response handling lives in `commands::*`.

use clap::error::ErrorKind;
use clap::{Parser, Subcommand};
use client::{ApiClient, ApiError};
use commands::config::ConfigCmd;
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
mod session;

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

    /// select a context from ~/.skardi/config.yaml; overrides $SKARDI_CONTEXT
    /// and the file's current-context
    #[arg(long, global = true, value_name = "NAME")]
    context: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

/// Subcommands supported by the CLI.
#[derive(Subcommand, Debug)]
enum Commands {
    /// Manage contexts in ~/.skardi/config.yaml (no network).
    Config {
        #[command(subcommand)]
        cmd: ConfigCmd,
    },

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

        /// Session id recorded with this execution in the server's audit
        /// ledger (sent as the X-Skardi-Session-Id header).
        #[arg(long)]
        session_id: Option<String>,
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
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => {
            // `--help`/`--version` are not usage errors: clap renders them
            // to stdout and we exit 0. Everything else (bogus flags,
            // missing args, ...) is a usage error: `e.print()` renders
            // clap's usage text to stderr (as `Cli::parse()` would have),
            // and we exit 1 rather than clap's default 2, since exit code 2
            // is reserved for "server unreachable" in this CLI's contract.
            match e.kind() {
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion => {
                    let _ = e.print();
                    return ExitCode::SUCCESS;
                }
                _ => {
                    let _ = e.print();
                    return ExitCode::FAILURE;
                }
            }
        }
    };
    // `config` subcommands edit the file that resolution reads, so they must
    // not be gated on that resolution succeeding — `set-context` is how an
    // operator FIXES a config whose cloud context is missing its workspace.
    if let Commands::Config { cmd } = cli.command {
        return match commands::config::run(cmd) {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("error: {err:#}");
                ExitCode::FAILURE
            }
        };
    }

    let config = match ClientConfig::resolve(cli.server, cli.token, cli.context) {
        Ok(config) => config,
        Err(err) => {
            eprintln!("error: {err}");
            return ExitCode::FAILURE;
        }
    };

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
            session_id,
        } => commands::run::run(&client, &name, data.as_deref(), &params, table, session_id).await,

        Commands::Pipeline { cmd } => commands::pipeline::run(&client, cmd).await,

        Commands::Job { cmd } => commands::jobs::run(&client, cmd).await,

        Commands::Schema => commands::schema::run(&client).await,

        Commands::Health { name } => commands::health::run(&client, name.as_deref()).await,

        // Handled before resolution in `main`, so the file can be repaired
        // even when resolving it would fail.
        Commands::Config { .. } => unreachable!("config is dispatched before resolution"),
    }
}
