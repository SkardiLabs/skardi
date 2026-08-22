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
#[cfg(test)]
use std::path::PathBuf;

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

    // Refused here rather than at the next command: a cloud context missing
    // either field cannot issue a request, so writing one is writing a dud.
    // The server matters as much as the workspace — without it resolution
    // would default to a LOCAL server while still sending the workspace PAT.
    if context.mode == ContextMode::Cloud {
        if context.workspace.is_none() {
            bail!(
                "context '{name}' is mode: cloud, so it needs a workspace: pass \
                 --workspace SLUG"
            );
        }
        if context.server.is_none() {
            bail!(
                "context '{name}' is mode: cloud, so it needs a server: pass \
                 --server URL (a cloud context is never defaulted to a local \
                 server — that would send its token there)"
            );
        }
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
        // `view` is the diagnostic for a broken file — position and path, not
        // the raw serde message, which quotes the offending scalar and would
        // print a token from the very command that promises to redact them.
        Err(err) => bail!(
            "{} does not parse ({}). Open it to see the offending line",
            path.display(),
            config::describe_parse_error(&err)
        ),
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

/// Replace a token entirely. No prefix survives, deliberately.
///
/// An earlier version kept the first twelve characters so a reader could tell
/// WHICH credential a context held. That is safe for a
/// `skardi_pat_<random>` token and unsafe for anything shorter: a legacy
/// `spec:` token is an arbitrary user string predating that format, so a
/// six-character one printed in full — labelled `(redacted)`, which is worse
/// than printing it plainly. The context NAME already identifies the
/// credential, `--show-tokens` prints it when that is what you want, and
/// `config view` is the command people run while screen-sharing.
fn redact(_token: &str) -> String {
    "(redacted)".to_string()
}

fn available(file: &ContextsFile) -> String {
    let names = file.context_names();
    if names.is_empty() {
        "(none)".to_string()
    } else {
        names.join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ContextsFile, LegacySpec};
    use tempfile::TempDir;

    /// Each subcommand takes the config path, so every one is testable
    /// IN-PROCESS against a temp file. That matters beyond speed: the
    /// integration suite drives these same paths through the real binary, and
    /// a spawned child's coverage is not instrumented by the coverage run, so
    /// without these the whole module reads as untested.
    fn temp() -> (TempDir, PathBuf) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.yaml");
        (dir, path)
    }

    fn write(path: &Path, contents: &str) {
        std::fs::write(path, contents).unwrap();
    }

    fn reload(path: &Path) -> ContextsFile {
        config::load(path).expect("parses")
    }

    #[test]
    fn set_context_creates_updates_and_touches_only_named_fields() {
        let (_dir, path) = temp();

        set_context(
            &path,
            "local",
            Some("http://127.0.0.1:9999".to_string()),
            None,
            None,
            Some("keep-me".to_string()),
            None,
            true,
        )
        .unwrap();
        let file = reload(&path);
        assert_eq!(file.current_context.as_deref(), Some("local"));
        assert_eq!(file.contexts[0].token.as_deref(), Some("keep-me"));

        // A later --server must NOT clear the token the context already holds.
        set_context(
            &path,
            "local",
            Some("http://127.0.0.1:1234".to_string()),
            None,
            None,
            None,
            None,
            false,
        )
        .unwrap();
        let file = reload(&path);
        assert_eq!(file.contexts.len(), 1, "updated, not duplicated");
        assert_eq!(
            file.contexts[0].server.as_deref(),
            Some("http://127.0.0.1:1234")
        );
        assert_eq!(file.contexts[0].token.as_deref(), Some("keep-me"));
    }

    #[test]
    fn set_context_rejects_an_unknown_mode_and_a_workspaceless_cloud_context() {
        let (_dir, path) = temp();

        let err = set_context(
            &path,
            "x",
            None,
            Some("kloud".to_string()),
            None,
            None,
            None,
            false,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("must be 'server' or 'cloud'"),
            "{err}"
        );
        assert!(!path.exists(), "a rejected mode must not create the file");

        // Refused at write time rather than left to fail at the next command.
        let err = set_context(
            &path,
            "c",
            None,
            Some("cloud".to_string()),
            None,
            None,
            None,
            false,
        )
        .unwrap_err();
        assert!(err.to_string().contains("needs a workspace"), "{err}");

        // And the server, for the same reason: without it resolution would
        // default to a LOCAL server while still sending the workspace PAT.
        let err = set_context(
            &path,
            "c",
            None,
            Some("cloud".to_string()),
            Some("ws".to_string()),
            Some("skardi_pat_x".to_string()),
            None,
            false,
        )
        .unwrap_err();
        assert!(err.to_string().contains("needs a server"), "{err}");
        assert!(
            !path.exists(),
            "a rejected cloud context must not be written"
        );

        // With both it lands.
        set_context(
            &path,
            "c",
            Some("https://gw".to_string()),
            Some("cloud".to_string()),
            Some("ws".to_string()),
            None,
            None,
            false,
        )
        .unwrap();
    }

    #[test]
    fn use_context_switches_and_refuses_an_unknown_name() {
        let (_dir, path) = temp();
        set_context(
            &path,
            "a",
            Some("http://a:1".to_string()),
            None,
            None,
            None,
            None,
            false,
        )
        .unwrap();
        set_context(
            &path,
            "b",
            Some("http://b:2".to_string()),
            None,
            None,
            None,
            None,
            false,
        )
        .unwrap();

        use_context(&path, "b").unwrap();
        assert_eq!(reload(&path).current_context.as_deref(), Some("b"));

        let err = use_context(&path, "nope").unwrap_err();
        assert!(err.to_string().contains("no context named 'nope'"), "{err}");
        assert!(err.to_string().contains("Available: a, b"), "{err}");
        assert_eq!(
            reload(&path).current_context.as_deref(),
            Some("b"),
            "a refused switch leaves the pointer alone"
        );
    }

    #[test]
    fn use_context_on_an_empty_file_says_there_are_none() {
        let (_dir, path) = temp();
        let err = use_context(&path, "any").unwrap_err();
        // The "(none)" arm of `available` — a listing with nothing to list.
        assert!(err.to_string().contains("Available: (none)"), "{err}");
    }

    #[test]
    fn delete_context_clears_a_dangling_current_pointer() {
        let (_dir, path) = temp();
        set_context(
            &path,
            "a",
            Some("http://a:1".to_string()),
            None,
            None,
            None,
            None,
            true,
        )
        .unwrap();

        delete_context(&path, "a").unwrap();
        let file = reload(&path);
        assert!(file.contexts.is_empty());
        // Left set, every later command would be a hard unknown-context error.
        assert_eq!(file.current_context, None);

        let err = delete_context(&path, "a").unwrap_err();
        assert!(err.to_string().contains("no context named 'a'"), "{err}");
    }

    #[test]
    fn delete_context_promotes_a_legacy_spec_before_removing_from_it() {
        let (_dir, path) = temp();
        write(
            &path,
            "kind: client\nspec:\n  server: http://legacy:8080\n  token: legacy-token\n",
        );

        // The legacy block has no `contexts:`, so the delete has to promote it
        // first — otherwise `default` is invisible and the delete is a no-op.
        delete_context(&path, config::LEGACY_CONTEXT_NAME).unwrap();
        let file = reload(&path);
        assert!(file.contexts.is_empty());
        assert!(file.spec.is_none(), "the promoted block is not left behind");
    }

    #[test]
    fn get_contexts_and_current_context_report_what_the_file_holds() {
        let (_dir, path) = temp();

        // Empty file: both are informative rather than crashing.
        get_contexts(&path).unwrap();
        assert!(
            current_context(&path).is_err(),
            "no pointer is an error exit"
        );

        set_context(
            &path,
            "acme/prod",
            Some("https://gw".to_string()),
            Some("cloud".to_string()),
            Some("acme-prod".to_string()),
            None,
            Some("xin@skardi.ai".to_string()),
            true,
        )
        .unwrap();
        // Exercises the row renderer, including the `*` marker and the
        // `-` fallbacks for absent fields.
        get_contexts(&path).unwrap();
        current_context(&path).unwrap();

        // A whitespace-only pointer counts as unset.
        let mut file = reload(&path);
        file.current_context = Some("   ".to_string());
        config::save(&path, &file).unwrap();
        assert!(current_context(&path).is_err());
    }

    #[test]
    fn view_redacts_both_context_and_legacy_tokens_and_reports_a_missing_file() {
        let (_dir, path) = temp();

        // Missing file: `view` names it rather than printing nothing.
        let err = view(&path, false).unwrap_err();
        assert!(err.to_string().contains("cannot read"), "{err}");

        // A legacy `spec:` token is redacted too — it is just as live as a
        // context's, and only this path reaches it.
        write(
            &path,
            "kind: client\nspec:\n  server: http://legacy:8080\n  token: legacy-secret\n",
        );
        view(&path, false).unwrap();
        view(&path, true).unwrap();

        // And an unparsable file is the one case `view` exists to diagnose.
        write(&path, "contexts: [broken\n");
        let err = view(&path, false).unwrap_err();
        assert!(err.to_string().contains("does not parse"), "{err}");
    }

    #[test]
    fn redaction_never_reveals_any_of_the_token_however_short_it_is() {
        // The regression: a prefix-preserving redactor printed a SHORT token
        // in full and labelled it `(redacted)`. Legacy `spec:` tokens are
        // arbitrary user strings predating the `skardi_pat_` format, so short
        // ones are ordinary — and `config view` is the screen-sharing command.
        for token in ["abc123", "x", "skardi_pat_0123456789abcdef", "hunter2"] {
            let redacted = redact(token);
            assert_eq!(redacted, "(redacted)", "input {token}");
            assert!(!redacted.contains(token), "token survived: {redacted}");
        }
    }

    #[test]
    fn available_lists_names_or_says_none() {
        let empty = ContextsFile::default();
        assert_eq!(available(&empty), "(none)");

        let legacy = ContextsFile {
            spec: Some(LegacySpec {
                server: Some("http://x:1".to_string()),
                token: None,
            }),
            ..ContextsFile::default()
        };
        assert_eq!(available(&legacy), config::LEGACY_CONTEXT_NAME);
    }
}
