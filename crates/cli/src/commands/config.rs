//! `skardi config …` — read and edit `~/.skardi/config.yaml`.
//!
//! Every subcommand here is a pure file edit: no network, no server contact.
//! That is a deliberate boundary — `login` is the only module that knows a
//! control plane exists, so these commands keep working (and keep being
//! testable) with no cloud reachable at all.
//!
//! Mutations go through [`config::load_for_mutation`], which refuses a file it
//! cannot parse. Reads use the tolerant loader, so `view` can still show the
//! operator what is wrong with a broken file rather than only failing.

use crate::config::{self, Context, ContextMode, ContextsFile};
use anyhow::{Context as _, Result, bail};
use clap::Subcommand;
use std::path::Path;

#[derive(Subcommand, Debug)]
pub enum ConfigCmd {
    /// List every context, marking the current one with `*`.
    GetContexts,

    /// Print the current context's name.
    CurrentContext,

    /// Switch the current context.
    UseContext {
        /// context name (see `skardi config get-contexts`)
        name: String,
    },

    /// Create or update one context, then optionally make it current.
    SetContext {
        /// context name; created if it does not exist
        name: String,

        #[arg(long, value_name = "URL")]
        server: Option<String>,

        /// `cloud` reaches a skardi-cloud gateway with a workspace-scoped PAT;
        /// `server` reaches a skardi-server directly
        #[arg(long, value_name = "MODE")]
        mode: Option<String>,

        /// workspace slug — required by a cloud context, sent per request
        #[arg(long, value_name = "SLUG")]
        workspace: Option<String>,

        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,

        #[arg(long, value_name = "EMAIL")]
        user: Option<String>,

        /// also set this context as current
        #[arg(long)]
        current: bool,
    },

    /// Delete one context. Does not revoke its credential — see `skardi logout
    /// --revoke` for that.
    DeleteContext {
        /// context name
        name: String,
    },

    /// Print the config file, with tokens redacted.
    View {
        /// print tokens in full instead of redacting them
        #[arg(long)]
        show_tokens: bool,
    },
}

/// Dispatch one `skardi config` subcommand.
pub fn run(cmd: ConfigCmd) -> Result<()> {
    let path = config::default_config_path()
        .context("cannot determine the home directory for ~/.skardi/config.yaml")?;

    match cmd {
        ConfigCmd::GetContexts => get_contexts(&path),
        ConfigCmd::CurrentContext => current_context(&path),
        ConfigCmd::UseContext { name } => use_context(&path, &name),
        ConfigCmd::SetContext {
            name,
            server,
            mode,
            workspace,
            token,
            user,
            current,
        } => set_context(&path, &name, server, mode, workspace, token, user, current),
        ConfigCmd::DeleteContext { name } => delete_context(&path, &name),
        ConfigCmd::View { show_tokens } => view(&path, show_tokens),
    }
}

fn get_contexts(path: &Path) -> Result<()> {
    let file = config::load(path).unwrap_or_default();
    let contexts = file.effective_contexts();
    if contexts.is_empty() {
        println!("no contexts configured ({})", path.display());
        return Ok(());
    }
    let current = file.current_context.as_deref().unwrap_or("");
    println!(
        "{:<2} {:<24} {:<8} {:<20} SERVER",
        "", "NAME", "MODE", "WORKSPACE"
    );
    for context in contexts {
        println!(
            "{:<2} {:<24} {:<8} {:<20} {}",
            if context.name == current { "*" } else { "" },
            context.name,
            context.mode,
            context.workspace.as_deref().unwrap_or("-"),
            context.server.as_deref().unwrap_or("-"),
        );
    }
    Ok(())
}

fn current_context(path: &Path) -> Result<()> {
    let file = config::load(path).unwrap_or_default();
    match file.current_context.as_deref().map(str::trim) {
        Some(name) if !name.is_empty() => {
            println!("{name}");
            Ok(())
        }
        // Exit non-zero: a script asking "which context" and getting silence
        // plus success would proceed against the wrong one.
        _ => bail!("no current context is set ({})", path.display()),
    }
}

fn use_context(path: &Path, name: &str) -> Result<()> {
    let mut file = config::load_for_mutation(path)?;
    if file.find(name).is_none() {
        bail!("no context named '{name}'. Available: {}", available(&file));
    }
    file.current_context = Some(name.to_string());
    config::save(path, &file)?;
    println!("switched to context '{name}'");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn set_context(
    path: &Path,
    name: &str,
    server: Option<String>,
    mode: Option<String>,
    workspace: Option<String>,
    token: Option<String>,
    user: Option<String>,
    current: bool,
) -> Result<()> {
    let mode = match mode.as_deref() {
        None => None,
        Some("server") => Some(ContextMode::Server),
        Some("cloud") => Some(ContextMode::Cloud),
        Some(other) => bail!("--mode must be 'server' or 'cloud', not '{other}'"),
    };

    let mut file = config::load_for_mutation(path)?;
    // A legacy `spec:`-only file gains its `default` context on first edit,
    // so the mutation does not silently drop the credential it holds.
    if file.contexts.is_empty() {
        file.contexts = file.effective_contexts();
        file.spec = None;
    }

    let existing = file.contexts.iter().position(|c| c.name == name);
    let created = existing.is_none();
    let index = existing.unwrap_or_else(|| {
        file.contexts.push(Context {
            name: name.to_string(),
            ..Context::default()
        });
        file.contexts.len() - 1
    });
    let context = &mut file.contexts[index];
    // Only fields the caller named are touched: `set-context --server X` must
    // not clear a token the same context already holds.
    if let Some(server) = server {
        context.server = Some(server);
    }
    if let Some(mode) = mode {
        context.mode = mode;
    }
    if let Some(workspace) = workspace {
        context.workspace = Some(workspace);
    }
    if let Some(token) = token {
        context.token = Some(token);
    }
    if let Some(user) = user {
        context.user = Some(user);
    }

    // Refused here rather than at the next command: a cloud context with no
    // workspace cannot issue a request, so writing one is writing a dud.
    if context.mode == ContextMode::Cloud && context.workspace.is_none() {
        bail!(
            "context '{name}' is mode: cloud, so it needs a workspace: pass \
             --workspace SLUG"
        );
    }

    if current {
        file.current_context = Some(name.to_string());
    }
    config::save(path, &file)?;
    println!(
        "{} context '{name}'{}",
        if created { "created" } else { "updated" },
        if current { " (now current)" } else { "" }
    );
    Ok(())
}

fn delete_context(path: &Path, name: &str) -> Result<()> {
    let mut file = config::load_for_mutation(path)?;
    if file.contexts.is_empty() {
        file.contexts = file.effective_contexts();
        file.spec = None;
    }
    let before = file.contexts.len();
    file.contexts.retain(|c| c.name != name);
    if file.contexts.len() == before {
        bail!("no context named '{name}'. Available: {}", available(&file));
    }
    // A dangling current-context would make every later command a hard error
    // (§5.1's unknown-context rule), so clear the pointer with the context.
    if file.current_context.as_deref() == Some(name) {
        file.current_context = None;
        println!("note: '{name}' was the current context; no context is current now");
    }
    config::save(path, &file)?;
    println!("deleted context '{name}'");
    Ok(())
}

fn view(path: &Path, show_tokens: bool) -> Result<()> {
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(err) => bail!("cannot read {}: {err}", path.display()),
    };
    let mut file: ContextsFile = match serde_yaml::from_str(&content) {
        Ok(file) => file,
        // `view` is the diagnostic for a broken file, so it prints the parse
        // error and the path rather than the tolerant loader's warning.
        Err(err) => bail!("{} does not parse: {err}", path.display()),
    };
    if !show_tokens {
        for context in &mut file.contexts {
            if let Some(token) = &context.token {
                context.token = Some(redact(token));
            }
        }
        if let Some(spec) = file.spec.as_mut().filter(|s| s.token.is_some()) {
            spec.token = spec.token.as_deref().map(redact);
        }
    }
    print!(
        "{}",
        serde_yaml::to_string(&file).context("serialize config")?
    );
    Ok(())
}

/// Keep enough of a token to recognize WHICH credential it is, without
/// printing anything usable — the same reason `git` shows short hashes.
fn redact(token: &str) -> String {
    let visible: String = token.chars().take(12).collect();
    format!("{visible}…(redacted)")
}

fn available(file: &ContextsFile) -> String {
    let names = file.context_names();
    if names.is_empty() {
        "(none)".to_string()
    } else {
        names.join(", ")
    }
}
