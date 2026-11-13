//! The clap surface of the agent-onboarding commands, and their dispatch.
//!
//! What each command does lives in [`crate::agent`]; this module reads flags,
//! resolves the context, and hands the outcome to [`crate::agent::finish`],
//! which owns the output contract and the exit code.

use crate::agent::control::Control;
use crate::agent::documents::{self, DataPlane};
use crate::agent::handoff::{self, Polling};
use crate::agent::integrations::{self, CandidateArgs};
use crate::agent::memory::{self, CopyOptions, Labels, Memory, SaveOptions, Target, branch};
use crate::agent::{AgentContext, AgentError, EXIT_NO_CONTROL, Invocation, LOGIN_CONTROL, Outcome};
use crate::config::{self, ClientConfig, ConfigError, ContextsFile};
use chrono::{DateTime, Utc};
use clap::{Args, Subcommand, ValueEnum};
use std::env;
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

#[derive(Args, Debug)]
pub struct MemoryArgs {
    /// print one JSON object: {"ok", "result"|"error", "next"}
    #[arg(long, global = true)]
    pub json: bool,

    /// the workspace (its slug) instead of the context's; the context's
    /// token must reach it
    #[arg(long, global = true, value_name = "SLUG")]
    pub workspace: Option<String>,

    /// why this call is made, recorded with it in the workspace's query
    /// ledger
    #[arg(long, global = true, value_name = "TEXT")]
    pub purpose: Option<String>,

    /// the session saves are labelled with (versions saved in one session
    /// within ten minutes merge) and the ledger groups calls by; default
    /// $SKARDI_SESSION_ID, else a fresh one per run
    #[arg(long, global = true, value_name = "ID")]
    pub session: Option<String>,

    #[command(subcommand)]
    pub cmd: MemoryCmd,
}

/// `consent on|off`.
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Switch {
    On,
    Off,
}

#[derive(Subcommand, Debug)]
pub enum MemoryCmd {
    /// Save your working memory of a task: a new version of the record.
    Save {
        /// url:<issue, PR or document URL>, label:<slug> or ontology:<id>;
        /// a bare https:// URL is read as url:
        anchor: String,

        /// the record as JSON (goal, status, done, decisions, findings,
        /// next, notes), or - for stdin
        #[arg(short = 'f', long, value_name = "PATH|-")]
        file: PathBuf,

        /// the version this save is based on; required once the memory
        /// exists (recall prints it)
        #[arg(long, value_name = "N")]
        if_version: Option<i64>,

        /// the parent project's anchor, set on the first save or when the
        /// person says so
        #[arg(long, value_name = "ANCHOR")]
        parent: Option<String>,

        /// the agent saving it, shown to the next reader; default
        /// $SKARDI_AGENT, else skardi-cli
        #[arg(long, value_name = "NAME")]
        agent: Option<String>,
    },

    /// Your memory of a task and every memory teammates shared for it, with
    /// your remembered consent.
    Recall {
        /// the task's anchor
        anchor: String,

        /// one kept version of your own memory instead (see `history`)
        #[arg(long, value_name = "N")]
        version: Option<i64>,
    },

    /// The memories you can read, newest first, and the workspace's count
    /// against its cap.
    List {
        /// only this anchor
        #[arg(long, value_name = "ANCHOR")]
        anchor: Option<String>,

        /// only memories whose goal or status contains this text
        #[arg(long, value_name = "TEXT")]
        text: Option<String>,
    },

    /// Copy your memory to another workspace or another anchor; the
    /// original stays as it is.
    Copy {
        /// the anchor your memory is under
        anchor: String,

        /// the workspace to copy into: a context's name, or a workspace
        /// slug
        #[arg(long, value_name = "WORKSPACE|CONTEXT")]
        to: Option<String>,

        /// save the copy under this anchor instead
        #[arg(long = "as", value_name = "ANCHOR")]
        as_anchor: Option<String>,

        /// save over your memory already at the target, as its next version
        #[arg(long)]
        replace: bool,

        /// the agent saving it; default $SKARDI_AGENT, else skardi-cli
        #[arg(long, value_name = "NAME")]
        agent: Option<String>,
    },

    /// Share your memory with every member of the workspace, or unshare it.
    /// Without --confirm it only shows what sharing would expose.
    Share {
        /// the anchor your memory is under
        anchor: String,

        /// share it, after the person has seen the warning
        #[arg(long)]
        confirm: bool,

        /// unshare it
        #[arg(long, conflicts_with = "confirm")]
        off: bool,
    },

    /// Delete your memory of a task: hidden at once, removed with its
    /// versions after 3 days.
    Rm {
        /// the anchor your memory is under
        anchor: String,
    },

    /// Your memory's kept versions, newest first.
    History {
        /// the anchor your memory is under
        anchor: String,
    },

    /// Empty one kept version of your memory for good, for something that
    /// got past the secret scan.
    Redact {
        /// the anchor your memory is under
        anchor: String,

        /// the version to redact (see `history`)
        #[arg(long, value_name = "N")]
        version: i64,
    },

    /// Remember whether memory is saved without asking: for one task, or
    /// with --workspace-wide for all your work in the workspace.
    Consent {
        /// on or off
        state: Switch,

        /// the task's anchor
        #[arg(required_unless_present = "workspace_wide")]
        anchor: Option<String>,

        /// every task of yours in this workspace that has no answer of its
        /// own
        #[arg(long, conflicts_with = "anchor")]
        workspace_wide: bool,
    },

    /// The memory of the current branch's issue or pull request, in a few
    /// lines: for a hook to add to an agent's context.
    Status {
        /// this anchor instead of the branch's
        #[arg(long, value_name = "ANCHOR")]
        anchor: Option<String>,
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
    let token = context.control(Utc::now())?;
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

/// Resolve the context, run one `skardi memory` command, print its outcome.
///
/// Separate from [`run`]: memory reaches the data plane under the context's
/// query token, so it needs no control token, and `--token` is that query
/// token's override exactly as it is for `query`.
pub async fn run_memory(
    args: MemoryArgs,
    flag_server: Option<String>,
    flag_token: Option<String>,
    flag_context: Option<String>,
) -> anyhow::Result<()> {
    let invocation = Invocation {
        json: args.json,
        context: flag_context.clone(),
    };
    let file = config::default_config_path()
        .as_deref()
        .and_then(config::load);
    let resolved = ClientConfig::resolve(flag_server, flag_token, flag_context);
    let secret = resolved.as_ref().ok().and_then(|r| r.token.clone());
    let outcome = execute_memory(args, resolved, file.as_ref(), Utc::now()).await;
    crate::agent::finish(outcome, &invocation, secret.as_deref())
}

/// Run one memory command against a resolved configuration. Separate from
/// [`run_memory`] so tests drive it with a stub gateway and no config file.
pub async fn execute_memory(
    args: MemoryArgs,
    resolved: Result<ClientConfig, ConfigError>,
    file: Option<&ContextsFile>,
    now: DateTime<Utc>,
) -> Result<Outcome, AgentError> {
    let MemoryArgs {
        workspace,
        purpose,
        session,
        cmd,
        ..
    } = args;
    let target = Target::from_resolved(resolved, file, workspace.as_deref(), now);
    // A hook runs status on every session start, in every directory: a
    // context that has no memory is an answer for it, not a failure.
    if let MemoryCmd::Status { .. } = &cmd
        && let Err(err) = &target
        && err.code() == "not_a_cloud_context"
    {
        return Ok(memory::status_off(err.code(), err.message(), None));
    }
    let target = target?;
    // One session for the whole run: every call's ledger header and a save's
    // label name the same one, the flag's, the environment's or a fresh id.
    let session = Some(memory::session_of(session)?);
    let connect = |target: &Target| {
        Memory::new(
            target,
            session.as_deref(),
            purpose.as_deref(),
            workspace.as_deref(),
        )
    };
    let anchor = |raw: &str| memory::anchor_arg(raw);
    match cmd {
        MemoryCmd::Save {
            anchor: raw,
            file: path,
            if_version,
            parent,
            agent,
        } => {
            let anchor = anchor(&raw)?;
            let parent = parent.as_deref().map(memory::anchor_arg).transpose()?;
            let labels = Labels::from_flags(agent, session.clone())?;
            let content = memory::read_content(&path).await?;
            let options = SaveOptions { if_version, parent };
            memory::save(&connect(&target)?, &anchor, content, &options, &labels).await
        }
        MemoryCmd::Recall {
            anchor: raw,
            version,
        } => {
            let anchor = anchor(&raw)?;
            let memory = connect(&target)?;
            match version {
                Some(version) => memory::recall_version(&memory, &anchor, version).await,
                None => memory::recall(&memory, &anchor).await,
            }
        }
        MemoryCmd::List { anchor: raw, text } => {
            let anchor = raw.as_deref().map(memory::anchor_arg).transpose()?;
            memory::list(&connect(&target)?, anchor.as_deref(), text.as_deref()).await
        }
        MemoryCmd::Copy {
            anchor: raw,
            to,
            as_anchor,
            replace,
            agent,
        } => {
            let anchor = anchor(&raw)?;
            let as_anchor = as_anchor.as_deref().map(memory::anchor_arg).transpose()?;
            let labels = Labels::from_flags(agent, session.clone())?;
            let from = connect(&target)?;
            let destination = match to.as_deref() {
                Some(to) => target.copy_target(to, file, now)?,
                None => target.clone(),
            };
            // A hint from the target's side (a save refused there) must run
            // against the target: through its context when `--to` named
            // another one, else with its workspace when that differs.
            let into = if destination.context != target.context {
                Memory::new(&destination, session.as_deref(), purpose.as_deref(), None)?
                    .through_context(&destination.context)
            } else if destination.workspace != target.workspace {
                Memory::new(
                    &destination,
                    session.as_deref(),
                    purpose.as_deref(),
                    Some(&destination.workspace),
                )?
            } else {
                connect(&destination)?
            };
            let options = CopyOptions {
                as_anchor,
                replace,
                to,
            };
            memory::copy(&from, &into, &anchor, &options, &labels).await
        }
        MemoryCmd::Share {
            anchor: raw,
            confirm,
            off,
        } => {
            let anchor = anchor(&raw)?;
            memory::share(&connect(&target)?, &anchor, !off, confirm).await
        }
        MemoryCmd::Rm { anchor: raw } => {
            let anchor = anchor(&raw)?;
            memory::rm(&connect(&target)?, &anchor).await
        }
        MemoryCmd::History { anchor: raw } => {
            let anchor = anchor(&raw)?;
            memory::history(&connect(&target)?, &anchor).await
        }
        MemoryCmd::Redact {
            anchor: raw,
            version,
        } => {
            let anchor = anchor(&raw)?;
            memory::redact(&connect(&target)?, &anchor, version).await
        }
        MemoryCmd::Consent {
            state,
            anchor: raw,
            workspace_wide,
        } => {
            let anchor = if workspace_wide {
                None
            } else {
                let Some(raw) = raw else {
                    return Err(AgentError::usage(
                        "anchor_required",
                        "name the task's anchor, or pass --workspace-wide",
                    ));
                };
                Some(memory::anchor_arg(&raw)?)
            };
            memory::consent(&connect(&target)?, anchor.as_deref(), state == Switch::On).await
        }
        MemoryCmd::Status { anchor: raw } => {
            let memory = connect(&target)?;
            let resolved = match raw {
                Some(raw) => branch::Resolved {
                    branch: None,
                    anchor: Some((memory::anchor_arg(&raw)?, branch::AnchorSource::Flag)),
                },
                None => {
                    let dir = env::current_dir().map_err(|err| {
                        AgentError::usage(
                            "no_working_directory",
                            format!("cannot read the working directory: {err}"),
                        )
                    })?;
                    branch::from_facts(&branch::gather(&dir).await)
                }
            };
            memory::status(&memory, resolved).await
        }
    }
}

/// The subcommand names that publish §2.4.2's exit-code table, so `main` can
/// give a clap usage error under them the table's `2` (and, under `--json`,
/// the envelope) rather than the CLI's general `1`.
pub const AGENT_COMMANDS: [&str; 4] = ["integrations", "handoff", "documents", "memory"];

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

    // --- memory -------------------------------------------------------------

    const MEMORY_TOKEN: &str = "skardi_pat_mcp_token";
    const MEMORY_WS: &str = "acme-eng";
    const MEMORY_ANCHOR: &str = "url:github.com/SkardiLabs/skardi-cloud/issues/1400";

    /// One of the contract's fixtures (`tests/fixtures/memory`).
    fn memory_fixture(name: &str) -> serde_json::Value {
        let file: PathBuf = [
            env!("CARGO_MANIFEST_DIR"),
            "tests",
            "fixtures",
            "memory",
            name,
        ]
        .iter()
        .collect();
        serde_json::from_slice(&std::fs::read(file).unwrap()).unwrap()
    }

    /// A gateway that answers every memory route from the fixtures.
    async fn memory_gateway() -> MockServer {
        use wiremock::matchers::{path_regex, query_param};
        let server = MockServer::start().await;
        let routes: [(&str, &str, &str, u8); 8] = [
            ("PUT", "^/memory/consent$", "consent_response.json", 1),
            ("PUT", "^/memory/[^/]+$", "save_response.json", 2),
            (
                "GET",
                "^/memory/[^/]+/versions$",
                "versions_response.json",
                2,
            ),
            (
                "POST",
                "^/memory/[^/]+/versions/[0-9]+/redact$",
                "redact_response.json",
                1,
            ),
            (
                "POST",
                "^/memory/[^/]+/share$",
                "share_response_needs_confirm.json",
                1,
            ),
            ("DELETE", "^/memory/[^/]+$", "delete_response.json", 2),
            ("GET", "^/memory/[^/]+$", "recall_response.json", 2),
            ("GET", "^/memory$", "list_response.json", 2),
        ];
        Mock::given(method("GET"))
            .and(path_regex("^/memory/[^/]+/versions$"))
            .and(query_param("version", "3"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(memory_fixture("version_response.json")),
            )
            .with_priority(1)
            .mount(&server)
            .await;
        for (verb, route, fixture, priority) in routes {
            Mock::given(method(verb))
                .and(path_regex(route))
                .respond_with(ResponseTemplate::new(200).set_body_json(memory_fixture(fixture)))
                .with_priority(priority)
                .mount(&server)
                .await;
        }
        server
    }

    fn memory_config(server: &MockServer) -> Result<ClientConfig, ConfigError> {
        Ok(ClientConfig {
            server: server.uri(),
            token: Some(MEMORY_TOKEN.to_string()),
            context: Some(config::SelectedContext {
                name: "acme/eng".to_string(),
                mode: config::ContextMode::Cloud,
                workspace: Some(MEMORY_WS.to_string()),
                token_expires_at: None,
            }),
        })
    }

    fn memory_args(cmd: MemoryCmd) -> MemoryArgs {
        MemoryArgs {
            json: true,
            workspace: None,
            purpose: Some("hand over".to_string()),
            session: None,
            cmd,
        }
    }

    /// Every memory command runs through `execute_memory` against the
    /// contract's answers, and every call of one run carries one session:
    /// the ledger header of each request, and a save's own label, are the
    /// same id even with no `--session`.
    #[tokio::test]
    async fn every_memory_command_runs_and_one_run_is_one_session() {
        let server = memory_gateway().await;
        let record = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            record.path(),
            serde_json::to_vec(&memory_fixture("save_request.json")["content"]).unwrap(),
        )
        .unwrap();
        let anchor = || MEMORY_ANCHOR.to_string();
        let commands = vec![
            MemoryCmd::Save {
                anchor: anchor(),
                file: record.path().to_path_buf(),
                if_version: Some(4),
                parent: None,
                agent: Some("codex".to_string()),
            },
            MemoryCmd::Recall {
                anchor: anchor(),
                version: None,
            },
            MemoryCmd::Recall {
                anchor: anchor(),
                version: Some(3),
            },
            MemoryCmd::List {
                anchor: Some(anchor()),
                text: None,
            },
            MemoryCmd::Copy {
                anchor: anchor(),
                to: Some("acme-ops".to_string()),
                as_anchor: None,
                replace: true,
                agent: None,
            },
            MemoryCmd::Share {
                anchor: anchor(),
                confirm: false,
                off: false,
            },
            MemoryCmd::Rm { anchor: anchor() },
            MemoryCmd::History { anchor: anchor() },
            MemoryCmd::Redact {
                anchor: anchor(),
                version: 3,
            },
            MemoryCmd::Consent {
                state: Switch::On,
                anchor: Some(anchor()),
                workspace_wide: false,
            },
            MemoryCmd::Consent {
                state: Switch::Off,
                anchor: None,
                workspace_wide: true,
            },
            MemoryCmd::Status {
                anchor: Some(anchor()),
            },
        ];
        for cmd in commands {
            let name = format!("{cmd:?}");
            let before = server.received_requests().await.unwrap().len();
            let outcome =
                execute_memory(memory_args(cmd), memory_config(&server), None, Utc::now()).await;
            assert!(outcome.is_ok(), "{name}: {:?}", outcome.err());
            let requests = server.received_requests().await.unwrap();
            let run = &requests[before..];
            assert!(!run.is_empty(), "{name} sent nothing");
            let sessions: Vec<&str> = run
                .iter()
                .map(|r| {
                    r.headers
                        .get("x-skardi-session-id")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                })
                .collect();
            assert!(!sessions[0].is_empty(), "{name}: no session header");
            assert!(
                sessions.iter().all(|s| *s == sessions[0]),
                "{name}: one run, several sessions: {sessions:?}"
            );
            for r in run.iter().filter(|r| r.method.as_str() == "PUT") {
                let body: serde_json::Value = serde_json::from_slice(&r.body).unwrap();
                if let Some(label) = body.get("session") {
                    assert_eq!(
                        label, sessions[0],
                        "{name}: the save's label is another session"
                    );
                }
            }
        }
    }

    /// `copy --to` another context runs the target side through it, and
    /// still as one session.
    #[tokio::test]
    async fn copy_to_another_context_runs_through_it() {
        let server = memory_gateway().await;
        let context = |name: &str, workspace: &str| config::Context {
            name: name.to_string(),
            mode: config::ContextMode::Cloud,
            server: Some(server.uri()),
            workspace: Some(workspace.to_string()),
            token: Some(MEMORY_TOKEN.to_string()),
            ..config::Context::default()
        };
        let file = ContextsFile {
            contexts: vec![
                context("acme/eng", MEMORY_WS),
                context("acme/team", "acme-team"),
            ],
            ..ContextsFile::default()
        };
        let cmd = MemoryCmd::Copy {
            anchor: MEMORY_ANCHOR.to_string(),
            to: Some("acme/team".to_string()),
            as_anchor: None,
            replace: true,
            agent: None,
        };
        let outcome = execute_memory(
            memory_args(cmd),
            memory_config(&server),
            Some(&file),
            Utc::now(),
        )
        .await;
        assert!(outcome.is_ok(), "{:?}", outcome.err());
        let requests = server.received_requests().await.unwrap();
        let workspaces: Vec<&str> = requests
            .iter()
            .filter_map(|r| r.headers.get("skardi-workspace")?.to_str().ok())
            .collect();
        assert_eq!(
            workspaces.first(),
            Some(&MEMORY_WS),
            "recalled at the source"
        );
        assert_eq!(workspaces.last(), Some(&"acme-team"), "saved at the target");
    }

    /// `consent` names a task or says `--workspace-wide`.
    #[tokio::test]
    async fn consent_without_an_anchor_is_a_usage_error() {
        let server = memory_gateway().await;
        let cmd = MemoryCmd::Consent {
            state: Switch::On,
            anchor: None,
            workspace_wide: false,
        };
        let err = execute_memory(memory_args(cmd), memory_config(&server), None, Utc::now())
            .await
            .unwrap_err();
        assert_eq!((err.exit, err.code()), (EXIT_USAGE, "anchor_required"));
    }

    /// `status` in a context that has no memory is an answer for a hook,
    /// not a failure.
    #[tokio::test]
    async fn status_outside_a_cloud_context_says_memory_is_off() {
        let mut config = memory_config(&MockServer::start().await).unwrap();
        if let Some(context) = config.context.as_mut() {
            context.mode = config::ContextMode::Server;
        }
        let outcome = execute_memory(
            memory_args(MemoryCmd::Status { anchor: None }),
            Ok(config),
            None,
            Utc::now(),
        )
        .await
        .unwrap();
        assert_eq!(outcome.result["enabled"], false);
    }

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
