//! `skardi` is a thin HTTP client for skardi-server: every command issues one
//! HTTP request to a running server and renders the response. It holds no
//! local query engine, catalog, or storage access — all of that lives in
//! skardi-server. This module defines the global connection flags shared by
//! all subcommands, the `Commands` enum, and dispatch; each subcommand's
//! request construction and response handling lives in `commands::*`.

use clap::error::ErrorKind;
use clap::{Parser, Subcommand};
use client::{ApiClient, ApiError};
use cloud::Capability;
use commands::config::ConfigCmd;
use commands::jobs::JobCmd;
use commands::login::{LoginArgs, LogoutArgs};
use commands::pipeline::PipelineCmd;
use config::ClientConfig;
use std::path::PathBuf;
use std::process::ExitCode;

mod client;
mod cloud;
mod commands;
mod config;
mod login;
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

/// `login`'s long help. Two GLOBAL flags mean something different under it, and
/// a third is refused, so the subcommand has to say so where the user looks.
const LOGIN_LONG_ABOUT: &str = "\
Sign in to skardi-cloud and write a context per workspace.

Two of the global flags mean something different here:

  --server <URL>    the GATEWAY url written into every context this run creates,
                    ahead of $SKARDI_GATEWAY_URL and the control plane's answer
  --context <NAME>  the NAME to write instead of <org>/<workspace>; only valid
                    when a single workspace is selected

--token is not accepted: login mints the credential. To store one by hand, use
'skardi config set-context <name> --token-stdin'.";

/// `logout`'s long help, for the same reason.
const LOGOUT_LONG_ABOUT: &str = "\
Drop the local credential, and optionally revoke it at the control plane.

  --context <NAME>  which context to clear (default: the current one)

--server and --token are not accepted: logout reads the credential it removes
from ~/.skardi/config.yaml.";

/// Subcommands supported by the CLI.
#[derive(Subcommand, Debug)]
enum Commands {
    /// Manage contexts in ~/.skardi/config.yaml (no network).
    Config {
        #[command(subcommand)]
        cmd: ConfigCmd,
    },

    /// Sign in to skardi-cloud and write a context per workspace.
    #[command(long_about = LOGIN_LONG_ABOUT)]
    Login(LoginArgs),

    /// Drop the local credential, and optionally revoke it at the control
    /// plane.
    #[command(long_about = LOGOUT_LONG_ABOUT)]
    Logout(LogoutArgs),

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
    match dispatch(cli).await {
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

/// Resolve the connection config and dispatch `cli.command` to its
/// implementation.
///
/// Resolution is deliberately LAZY — inside this function, after the `config`
/// arm — because `skardi config` edits the very file resolution reads.
/// `set-context` is how an operator repairs a config whose cloud context is
/// missing its workspace, so it must not be gated on that resolution
/// succeeding. Handling it here rather than short-circuiting in `main` keeps
/// one dispatch point and leaves no structurally unreachable arm behind.
async fn dispatch(cli: Cli) -> anyhow::Result<()> {
    // These three edit the very file resolution reads, so they run BEFORE it:
    // `login` writes a context that does not exist yet, and `logout` must work
    // on a context whose credential has already expired.
    match cli.command {
        Commands::Config { cmd } => return commands::config::run(cmd, cli.context),
        Commands::Login(args) => {
            // Refused rather than ignored: `login` mints the credential, so a
            // `--token` here is a misunderstanding worth naming, and silently
            // dropping a flag someone typed is how they conclude it worked.
            if cli.token.is_some() {
                anyhow::bail!(
                    "--token is not accepted by 'login': login mints the credential. To store one by hand, use 'skardi config set-context <name> --token-stdin'"
                );
            }
            return commands::login::run(args, cli.context, cli.server).await;
        }
        Commands::Logout(args) => {
            if cli.token.is_some() || cli.server.is_some() {
                anyhow::bail!(
                    "--server and --token are not accepted by 'logout': it reads the credential it removes from ~/.skardi/config.yaml"
                );
            }
            return commands::logout::run(args, cli.context).await;
        }
        _ => {}
    }

    let config = ClientConfig::resolve(cli.server, cli.token, cli.context)?;
    let capability = match capability_of(&cli.command) {
        Some(capability) => capability,
        // Unreachable: `Config` is the only capability-less command and it
        // returned above. Refusing beats defaulting — a remote command added
        // without an entry must fail loudly rather than quietly skip the
        // cloud gating in `cloud::ensure_available`.
        None => anyhow::bail!("internal error: command has no capability entry"),
    };
    // Both checks precede `ApiClient::new`, so a gated command and an expired
    // credential issue no request at all (§8).
    cloud::ensure_available(capability, &config)?;
    cloud::ensure_credential_fresh(&config, chrono::Utc::now())?;
    let client = ApiClient::new(&config)?;

    let outcome = match cli.command {
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

        // Returned above, before resolution.
        Commands::Config { .. } | Commands::Login(_) | Commands::Logout(_) => Ok(()),
    };

    outcome.map_err(|err| cloud::diagnose(err, capability, &config))
}

/// The [`Capability`] a command exercises, or `None` for the purely local
/// `config` command. Kept as one table so gating and the route-specific error
/// mapping cannot disagree about what a command is.
fn capability_of(command: &Commands) -> Option<Capability> {
    match command {
        Commands::Query { .. } => Some(Capability::Query),
        Commands::Schema => Some(Capability::Schema),
        Commands::Run { .. } => Some(Capability::Run),
        Commands::Pipeline { .. } => Some(Capability::Pipeline),
        Commands::Job { .. } => Some(Capability::Job),
        Commands::Health { .. } => Some(Capability::Health),
        // Local, and returned before resolution: no capability to gate.
        Commands::Config { .. } | Commands::Login(_) | Commands::Logout(_) => None,
    }
}
