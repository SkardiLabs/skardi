//! `skardi` is a thin HTTP client for skardi-server: every command issues one
//! HTTP request to a running server and renders the response. It holds no
//! local query engine, catalog, or storage access — all of that lives in
//! skardi-server. Subcommands are added in later tasks; this module only
//! defines the global connection flags shared by all of them.

use clap::Parser;

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
}

fn main() {
    let _cli = Cli::parse();

    eprintln!("error: no command specified (see --help)");
    std::process::exit(1);
}
