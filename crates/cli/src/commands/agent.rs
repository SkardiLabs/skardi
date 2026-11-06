//! The clap surface of the agent-onboarding commands, and their dispatch.
//!
//! What each command does lives in [`crate::agent`]; this module reads flags,
//! resolves the context, and hands the outcome to [`crate::agent::finish`],
//! which owns the output contract and the exit code.

use crate::agent::control::Control;
use crate::agent::documents::{self, DataPlane};
use crate::agent::handoff::{self, Polling};
use crate::agent::integrations::{self, CandidateArgs};
use crate::agent::{AgentContext, AgentError, EXIT_NO_CONTROL, Invocation, LOGIN_CONTROL, Outcome};
use crate::config;
use clap::{Args, Subcommand};
use std::path::PathBuf;

/// `--json` on every agent command: one object on stdout, §2.4.2's shape.
#[derive(Args, Debug)]
pub struct IntegrationsArgs {
    /// print one JSON object: {"ok", "result"|"error", "next"}
    #[arg(long, global = true)]
    pub json: bool,

    #[command(subcommand)]
    pub cmd: IntegrationsCmd,
}

#[derive(Subcommand, Debug)]
pub enum IntegrationsCmd {
    /// The workspace's state, what this org may install, and what is
    /// connected and syncing.
    List,

    /// Open a handoff for one provider and print its page link, for the
    /// person to consent or paste a secret there.
    Connect {
        /// the provider, as `integrations list` names it (notion, github, …)
        saas: String,

        /// `new` for a new connection, or the alias of one to reconnect
        #[arg(long, value_name = "new|ALIAS")]
        target: String,
    },

    /// Check what the connection's credential can do, and whether the
    /// install may proceed.
    Permissions {
        /// the connection alias `handoff wait` printed
        alias: String,

        /// the connection version `handoff wait` printed
        #[arg(long, value_name = "ID")]
        connection_version: String,
    },

    /// One page of what the connection can install.
    Candidates {
        /// the connection alias `handoff wait` printed
        alias: String,

        /// the connection version `handoff wait` printed
        #[arg(long, value_name = "ID")]
        connection_version: String,

        /// which content type to list, for a provider with more than one
        #[arg(long, value_name = "TYPE")]
        content_type: Option<String>,

        /// open this level: a row's id from a previous page
        #[arg(long, value_name = "ID")]
        parent: Option<String>,

        /// continue a listing: the previous page's next_cursor
        #[arg(long, value_name = "CURSOR")]
        cursor: Option<String>,
    },

    /// Add to what the connection syncs, then wait for the install.
    Install {
        /// the connection alias `handoff wait` printed
        alias: String,

        /// the connection version `handoff wait` printed
        #[arg(long, value_name = "ID")]
        connection_version: String,

        /// what to add, per content type: '{"issues":null}' for everything,
        /// or rules whose coordinates are copied from `candidates` rows
        #[arg(long, value_name = "JSON")]
        scope: String,

        /// the install's idempotency key, to resume one a previous run
        /// started (it printed this in its `next`)
        #[arg(long, value_name = "ID")]
        request_id: Option<String>,

        /// how long to wait for the install (default 10m)
        #[arg(long, value_name = "DURATION")]
        timeout: Option<String>,
    },

    /// Drop what a handoff staged, when the person declined or the
    /// permissions check closed the door. Never disconnects anything.
    Cancel {
        /// the handoff id `connect` printed
        handoff: String,
    },

    /// A source's phase, health, runs and items; with no source, a summary
    /// of every source.
    Status {
        /// the source's name (omit for every source)
        source: Option<String>,

        /// with no source: every SaaS and database source in full
        #[arg(long)]
        detail: bool,
    },

    /// Ask for one run of a source now.
    Sync {
        /// the source's name
        source: String,
    },
}

#[derive(Args, Debug)]
pub struct HandoffArgs {
    /// print one JSON object: {"ok", "result"|"error", "next"}
    #[arg(long, global = true)]
    pub json: bool,

    #[command(subcommand)]
    pub cmd: HandoffCmd,
}

#[derive(Subcommand, Debug)]
pub enum HandoffCmd {
    /// Wait until the person has finished on the handoff page, then print the
    /// connection alias and version it produced.
    Wait {
        /// the handoff id `integrations connect` printed
        id: String,

        /// how long to wait (default 30m, the handoff's own lifetime)
        #[arg(long, value_name = "DURATION")]
        timeout: Option<String>,
    },
}

#[derive(Args, Debug)]
pub struct DocumentsArgs {
    /// print one JSON object: {"ok", "result"|"error", "next"}
    #[arg(long, global = true)]
    pub json: bool,

    #[command(subcommand)]
    pub cmd: DocumentsCmd,
}

#[derive(Subcommand, Debug)]
pub enum DocumentsCmd {
    /// Upload a local file into this (personal) workspace as a Documents
    /// source, and start its first run.
    Add {
        /// the file: pdf, docx, xlsx, pptx, md, txt, png or jpg
        file: PathBuf,
    },
}

/// One of the three agent command groups, unwrapped.
#[derive(Debug)]
pub enum AgentCmd {
    Integrations(IntegrationsCmd),
    Handoff(HandoffCmd),
    Documents(DocumentsCmd),
}

/// Resolve the context, run the command, print its outcome.
pub async fn run(
    cmd: AgentCmd,
    json: bool,
    flag_server: Option<String>,
    flag_token: Option<String>,
    flag_context: Option<String>,
) -> anyhow::Result<()> {
    let invocation = Invocation {
        json,
        context: flag_context.clone(),
    };
    let path = config::default_config_path();
    let context = AgentContext::resolve(flag_server, flag_token, flag_context, path.as_deref());
    let (outcome, secret) = match context {
        Ok(context) => {
            let secret = context.control_token.clone();
            (execute(cmd, &context).await, secret)
        }
        Err(err) => (Err(err), None),
    };
    crate::agent::finish(outcome, &invocation, secret.as_deref())
}

/// Run one command against a resolved context. Separate from [`run`] so the
/// binary-level tests and the unit tests share one path.
pub async fn execute(cmd: AgentCmd, context: &AgentContext) -> Result<Outcome, AgentError> {
    // Every command needs a live control token, `documents add` included
    // (its declaration and sync are control routes), so it is checked before
    // anything is read or sent.
    let token = context.control(chrono::Utc::now())?;
    let control = Control::new(&context.gateway, &context.workspace, token)?;
    match cmd {
        AgentCmd::Integrations(cmd) => match cmd {
            IntegrationsCmd::List => integrations::list(&control).await,
            IntegrationsCmd::Connect { saas, target } => {
                integrations::connect(&control, &saas, &target).await
            }
            IntegrationsCmd::Permissions {
                alias,
                connection_version,
            } => integrations::permissions(&control, &alias, &connection_version).await,
            IntegrationsCmd::Candidates {
                alias,
                connection_version,
                content_type,
                parent,
                cursor,
            } => {
                let args = CandidateArgs {
                    content_type,
                    parent,
                    cursor,
                };
                integrations::candidates(&control, &alias, &connection_version, &args).await
            }
            IntegrationsCmd::Install {
                alias,
                connection_version,
                scope,
                request_id,
                timeout,
            } => {
                let deadline = timeout.as_deref().map(handoff::parse_timeout).transpose()?;
                integrations::install(
                    &control,
                    &alias,
                    &connection_version,
                    &scope,
                    request_id.as_deref(),
                    integrations::install_polling(deadline),
                )
                .await
            }
            IntegrationsCmd::Cancel { handoff } => integrations::cancel(&control, &handoff).await,
            IntegrationsCmd::Status { source, detail } => {
                integrations::status(&control, source.as_deref(), detail).await
            }
            IntegrationsCmd::Sync { source } => integrations::sync(&control, &source).await,
        },
        AgentCmd::Handoff(HandoffCmd::Wait { id, timeout }) => {
            let polling = match timeout.as_deref() {
                Some(raw) => Polling::HANDOFF.with_deadline(handoff::parse_timeout(raw)?),
                None => Polling::HANDOFF,
            };
            handoff::wait(&control, &id, polling).await
        }
        AgentCmd::Documents(DocumentsCmd::Add { file }) => {
            let Some(mcp_token) = context.mcp_token.clone() else {
                return Err(AgentError::local(
                    EXIT_NO_CONTROL,
                    "query_token_missing",
                    format!(
                        "context '{}' holds no query token to upload with; run `{LOGIN_CONTROL}`",
                        context.name
                    ),
                )
                .with_next(LOGIN_CONTROL));
            };
            let data = DataPlane {
                gateway: context.gateway.clone(),
                workspace: context.workspace.clone(),
                mcp_token,
                retry_wait: documents::UPLOAD_RETRY_WAIT,
            };
            documents::add(&control, &data, &file).await
        }
    }
}

/// The subcommand names that publish §2.4.2's exit-code table, so `main` can
/// give a clap usage error under them the table's `2` (and, under `--json`,
/// the envelope) rather than the CLI's general `1`.
pub const AGENT_COMMANDS: [&str; 3] = ["integrations", "handoff", "documents"];

/// The first subcommand word in `args` (without the binary name), skipping
/// the global flags and the values they take.
pub fn subcommand_of<I: IntoIterator<Item = String>>(args: I) -> Option<String> {
    const TAKES_VALUE: [&str; 3] = ["--server", "--token", "--context"];
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        if TAKES_VALUE.contains(&arg.as_str()) {
            args.next();
            continue;
        }
        if arg.starts_with('-') {
            continue;
        }
        return Some(arg);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::{EXIT_HANDOFF_ENDED, EXIT_USAGE};
    use serde_json::json;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const HANDOFF: &str = "4b9f3c1e-8a2d-4c6b-9e0f-1a2b3c4d5e6f";

    fn context(gateway: &str, mcp_token: Option<&str>) -> AgentContext {
        AgentContext {
            name: "acme/prod".to_string(),
            gateway: gateway.to_string(),
            workspace: "acme-prod".to_string(),
            control_token: Some("skardi_pat_ctl".to_string()),
            control_expires_at: None,
            mcp_token: mcp_token.map(str::to_string),
        }
    }

    /// `handoff wait --timeout` replaces the 30-minute default: a handoff
    /// still open is `still_open` after the budget given, not after half an
    /// hour.
    #[tokio::test]
    async fn handoff_wait_takes_its_timeout() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"state": "opened"})))
            .mount(&server)
            .await;
        let cmd = AgentCmd::Handoff(HandoffCmd::Wait {
            id: HANDOFF.to_string(),
            timeout: Some("1s".to_string()),
        });
        let started = std::time::Instant::now();
        let err = execute(cmd, &context(&server.uri(), Some("q")))
            .await
            .unwrap_err();
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        assert_eq!(err.exit, EXIT_HANDOFF_ENDED);
        assert_eq!(err.code(), "still_open");
    }

    /// A malformed `--timeout` is a usage error, before any request.
    #[rstest::rstest]
    #[case::wait(AgentCmd::Handoff(HandoffCmd::Wait { id: HANDOFF.to_string(), timeout: Some("soon".to_string()) }))]
    #[case::install(AgentCmd::Integrations(IntegrationsCmd::Install {
        alias: "a".to_string(),
        connection_version: "0e6a3d2c-0000-4000-8000-000000000001".to_string(),
        scope: r#"{"issues":null}"#.to_string(),
        request_id: None,
        timeout: Some("0".to_string()),
    }))]
    #[tokio::test]
    async fn a_bad_timeout_is_a_usage_error_before_any_request(#[case] cmd: AgentCmd) {
        let server = MockServer::start().await;
        let err = execute(cmd, &context(&server.uri(), Some("q")))
            .await
            .unwrap_err();
        assert_eq!(err.exit, EXIT_USAGE);
        assert_eq!(err.code(), "invalid_timeout");
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    /// `documents add` uploads with the query token; a context without one
    /// is exit 6, before the file is read or anything is sent.
    #[tokio::test]
    async fn documents_add_without_a_query_token_is_exit_6() {
        let server = MockServer::start().await;
        let cmd = AgentCmd::Documents(DocumentsCmd::Add {
            file: PathBuf::from("/nonexistent/notes.md"),
        });
        let err = execute(cmd, &context(&server.uri(), None))
            .await
            .unwrap_err();
        assert_eq!(err.exit, EXIT_NO_CONTROL);
        assert_eq!(err.code(), "query_token_missing");
        assert_eq!(err.next.as_deref(), Some(LOGIN_CONTROL));
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[test]
    fn the_subcommand_is_found_past_global_flags() {
        let args = |list: &[&str]| list.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(
            subcommand_of(args(&["--context", "acme/prod", "integrations", "list"])).as_deref(),
            Some("integrations")
        );
        assert_eq!(
            subcommand_of(args(&["--server=http://x", "handoff"])).as_deref(),
            Some("handoff")
        );
        assert_eq!(subcommand_of(args(&["--json"])), None);
    }
}
