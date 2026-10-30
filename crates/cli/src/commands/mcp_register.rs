//! `skardi mcp register <client>` (agent onboarding design 2026-10-27
//! §2.4.2): add the workspace's MCP server to an agent's configuration, with
//! the token that has no control-plane reach.
//!
//! The URL is the `mcp-url` the login's exchange named (`gateway_url +
//! "/mcp"`, §2.9.2) and the bearer is the context's `token` — the pair's
//! `mcp_token` after a `login --control`, and the one token there is after a
//! login without it. **Never the control token**: an MCP configuration is
//! read by every MCP host and often lands in a dotfiles repository, and the
//! control token exists precisely to stay out of such places (§2.3.4).
//!
//! For `claude-code` the CLI runs `claude mcp add-json --scope user skardi
//! <json>` itself — staged so that a failure never leaves less than there
//! was, see [`register_claude_code`] — and prints only "registered", so the
//! token never passes through the agent's transcript. It is in that child's arguments while it
//! runs, which only the same user's processes can read. `--print` shows the
//! same configuration, for a person adding it to any other client by hand.
//!
//! `skardi mcp` with no subcommand is still the stdio bridge, and still
//! refused on a cloud context — the gateway serves `/mcp` itself.

use crate::config::{self, ContextMode};
use anyhow::{Context as _, Result, bail};
use clap::{Args, Subcommand};
use serde_json::{Value, json};
use std::ffi::OsStr;
use std::path::Path;
use std::process::Command;

/// The server name every registration uses, so a re-login's re-run replaces
/// the entry rather than adding a second.
pub const SERVER_NAME: &str = "skardi";

/// The one client registered directly.
pub const CLAUDE_CODE: &str = "claude-code";

#[derive(Subcommand, Debug)]
pub enum McpCmd {
    /// Add this workspace's MCP server to an agent, with the query token
    /// (never the control token). `claude-code` is registered directly;
    /// `--print` shows the configuration for any other client.
    Register(RegisterArgs),
}

#[derive(Args, Debug)]
pub struct RegisterArgs {
    /// the agent to register with: `claude-code`. Omit it with --print
    pub client: Option<String>,

    /// print the MCP server configuration instead of registering it
    #[arg(long)]
    pub print: bool,
}

/// What `register` needs from the selected context. `Debug` is hand-written:
/// it carries the token.
struct Server {
    url: String,
    token: String,
    workspace: Option<String>,
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("url", &self.url)
            .field("token", &"(redacted)")
            .field("workspace", &self.workspace)
            .finish()
    }
}

impl Server {
    /// The server entry: HTTP transport, the URL, and the bearer. The
    /// workspace header pins the connection to the one workspace this login
    /// approved, as the console's own snippet does for a one-workspace token.
    fn entry(&self) -> Value {
        let mut headers = json!({ "Authorization": format!("Bearer {}", self.token) });
        if let Some(workspace) = &self.workspace {
            headers["Skardi-Workspace"] = json!(workspace);
        }
        json!({ "type": "http", "url": self.url, "headers": headers })
    }
}

pub fn run(cmd: McpCmd, flag_context: Option<String>) -> Result<()> {
    let path = config::default_config_path()
        .context("cannot determine the home directory for ~/.skardi/config.yaml")?;
    let McpCmd::Register(args) = cmd;
    let printed = register_at(
        &path,
        &args,
        flag_context.as_deref(),
        std::env::var("SKARDI_CONTEXT").ok().as_deref(),
        OsStr::new("claude"),
    )?;
    print!("{printed}");
    Ok(())
}

/// The whole command, with the config path, the context selection and the
/// `claude` binary injected, so it runs in tests against a fake `claude`.
/// Returns what to print.
pub(crate) fn register_at(
    path: &Path,
    args: &RegisterArgs,
    flag_context: Option<&str>,
    env_context: Option<&str>,
    claude: &OsStr,
) -> Result<String> {
    let client = args.client.as_deref().map(str::trim);
    match (client, args.print) {
        (None, false) => bail!(
            "name the agent to register with (`skardi mcp register {CLAUDE_CODE}`), or pass --print to see the configuration for another one"
        ),
        (Some(other), false) if other != CLAUDE_CODE => bail!(
            "'{other}' is not registered directly — only {CLAUDE_CODE} is. Run `skardi mcp register --print` and add what it shows as an HTTP MCP server named {SERVER_NAME}"
        ),
        _ => {}
    }
    let server = server_for(path, flag_context, env_context)?;
    if args.print {
        let config = json!({ "mcpServers": { SERVER_NAME: server.entry() } });
        return Ok(format!(
            "{}\n",
            serde_json::to_string_pretty(&config).expect("a Value always serializes")
        ));
    }
    register_claude_code(&server, claude)?;
    Ok("registered\n".to_string())
}

/// The selected context's MCP URL and query token.
fn server_for(
    path: &Path,
    flag_context: Option<&str>,
    env_context: Option<&str>,
) -> Result<Server> {
    let file = config::load(path)
        .with_context(|| format!("no config at {} — run `skardi login` first", path.display()))?;
    let context = config::select_context(
        &file.effective_contexts(),
        file.current_context.as_deref(),
        flag_context,
        env_context,
    )?
    .context("no context selected: pass --context <NAME>, or run `skardi login`")?;
    if context.mode != ContextMode::Cloud {
        bail!(
            "context '{}' is not a skardi-cloud context; `mcp register` adds a cloud workspace's MCP server. For a local skardi-server, see `skardi mcp`",
            context.name
        );
    }
    let Some(url) = context.mcp_url.clone().filter(|u| !u.trim().is_empty()) else {
        bail!(
            "context '{}' has no MCP URL: the control plane named none at login, because it has no gateway URL set (SKARDI_GLOBAL_GATEWAY_URL; helm: skardiGlobal.gatewayUrl). Set it, then run `skardi login` again",
            context.name
        );
    };
    // `token` is the pair's mcp_token after `login --control`, and the only
    // token after a login without it. The control token is never a candidate.
    let Some(token) = context.token.clone().filter(|t| !t.trim().is_empty()) else {
        bail!(
            "context '{}' holds no token; run `skardi login`",
            context.name
        );
    };
    Ok(Server {
        url,
        token,
        workspace: context.workspace.clone(),
    })
}

/// Where a new registration is added first, before anything is removed.
pub const STAGED_NAME: &str = "skardi-pending";

/// What `claude mcp remove` did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Removal {
    Removed,
    /// There was nothing by that name in that scope: the usual answer on a
    /// first registration, and not a failure.
    Absent,
}

/// Replace the `skardi` entry with `server`, so that **no failure leaves the
/// user with less than they had**.
///
/// `add-json` refuses a name its scope already holds, and a local-scope
/// entry from an older setup outranks the user-scope one in its folder, so a
/// re-registration has to remove the old entries — and `claude` offers no way
/// to read one back to restore it (`mcp get` is prose, health-checks the
/// server, and prints the bearer). So the new entry is added FIRST, under
/// [`STAGED_NAME`], and the swap happens only once Claude Code has accepted
/// it:
///
/// 1. clear a staged entry an interrupted run left behind;
/// 2. add the new entry as `skardi-pending` — refused (an old Claude, a
///    malformed entry, a policy, a write error) and nothing has changed;
/// 3. remove the old `skardi` at local, then user scope — a removal that
///    fails for any reason but absence stops here, and the staged copy is
///    taken back out, so the old entry stays in charge;
/// 4. add the new entry as `skardi` — refused now, and the new server is
///    still registered, as `skardi-pending`, which the error says;
/// 5. remove `skardi-pending`.
///
/// Every step's result is checked; any failure is an error, so "registered"
/// is printed only when all five held. Nothing `claude` prints on success is
/// passed through, since it could quote the configuration it was given, and
/// what it prints on failure has the token scrubbed out of it.
fn register_claude_code(server: &Server, claude: &OsStr) -> Result<()> {
    let entry = serde_json::to_string(&server.entry()).expect("a Value always serializes");
    let token = server.token.as_str();

    remove(claude, "user", STAGED_NAME, token).map_err(|err| {
        explain(
            err,
            format!(
                "could not clear the `{STAGED_NAME}` entry an earlier run left behind; nothing was changed"
            ),
        )
    })?;
    add(claude, STAGED_NAME, &entry, token).map_err(|err| {
        explain(
            err,
            format!(
                "Claude Code did not accept the new MCP server, so nothing was changed: any `{SERVER_NAME}` entry you had is still there"
            ),
        )
    })?;

    for scope in ["local", "user"] {
        if let Err(err) = remove(claude, scope, SERVER_NAME, token) {
            let cleanup = match remove(claude, "user", STAGED_NAME, token) {
                Ok(_) => String::new(),
                Err(_) => {
                    format!(", and the new server is left registered as `{STAGED_NAME}` beside it")
                }
            };
            return Err(explain(
                err,
                format!(
                    "could not remove the existing `{SERVER_NAME}` entry at {scope} scope, so the new one was not put in its place{cleanup}. Remove it with `claude mcp remove --scope {scope} {SERVER_NAME}`, then run this again"
                ),
            ));
        }
    }

    add(claude, SERVER_NAME, &entry, token).map_err(|err| {
        explain(
            err,
            format!(
                "the old `{SERVER_NAME}` entry was removed but the new one could not take its name; it is registered as `{STAGED_NAME}` and works under that name. Run `claude mcp remove --scope user {STAGED_NAME}`, then this again"
            ),
        )
    })?;
    remove(claude, "user", STAGED_NAME, token).map_err(|err| {
        explain(
            err,
            format!(
                "`{SERVER_NAME}` is registered, but the staged copy `{STAGED_NAME}` could not be removed — remove it with `claude mcp remove --scope user {STAGED_NAME}`"
            ),
        )
    })?;
    Ok(())
}

/// `claude` could not be started at all. Nothing ran, so none of the
/// step-by-step explanations applies and it is reported as it is.
#[derive(Debug)]
struct NotRunnable(String);

impl std::fmt::Display for NotRunnable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "could not run `claude` ({}) — is Claude Code installed and on PATH? Or run `skardi mcp register --print` and add the server by hand",
            self.0
        )
    }
}

impl std::error::Error for NotRunnable {}

/// Say what a failed step leaves the user with — unless `claude` never ran.
fn explain(err: anyhow::Error, meaning: String) -> anyhow::Error {
    if err.is::<NotRunnable>() {
        err
    } else {
        err.context(meaning)
    }
}

/// `claude mcp add-json --scope user <name> <entry>`.
fn add(claude: &OsStr, name: &str, entry: &str, token: &str) -> Result<()> {
    let args = ["mcp", "add-json", "--scope", "user", name, entry];
    let output = run_claude(claude, &args)?;
    if output.status.success() {
        return Ok(());
    }
    Err(failed(
        &format!("claude mcp add-json --scope user {name}"),
        &output,
        token,
    ))
}

/// `claude mcp remove --scope <scope> <name>`, with "there is none" told
/// apart from a removal that failed.
///
/// `claude` exits 1 either way, so absence is read from what it says: `No
/// MCP server named "…" in … scope` (Claude Code 2.x). Read leniently, and
/// failing safe: should a later Claude word it differently, absence reads
/// as a failure, which stops the registration loudly rather than replacing
/// an entry that may not have gone.
fn remove(claude: &OsStr, scope: &str, name: &str, token: &str) -> Result<Removal> {
    let output = run_claude(claude, &["mcp", "remove", "--scope", scope, name])?;
    if output.status.success() {
        return Ok(Removal::Removed);
    }
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
    .to_ascii_lowercase();
    if said.contains("no mcp server") || said.contains("not found") {
        return Ok(Removal::Absent);
    }
    Err(failed(
        &format!("claude mcp remove --scope {scope} {name}"),
        &output,
        token,
    ))
}

fn run_claude(claude: &OsStr, args: &[&str]) -> Result<std::process::Output> {
    Command::new(claude)
        .args(args)
        .output()
        .map_err(|err| NotRunnable(err.to_string()).into())
}

/// A failed `claude` call, with what it said on stderr and the token
/// scrubbed from it: `add-json` can echo the entry it refused.
fn failed(call: &str, output: &std::process::Output, token: &str) -> anyhow::Error {
    let stderr = String::from_utf8_lossy(&output.stderr)
        .replace(token, "(redacted)")
        .trim()
        .to_string();
    anyhow::anyhow!(
        "`{call}` failed ({}){}",
        output.status,
        if stderr.is_empty() {
            String::new()
        } else {
            format!(": {stderr}")
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const MCP: &str = "skardi_pat_mcp_token";
    const CONTROL: &str = "skardi_pat_control_secret";

    fn config(dir: &TempDir, mcp_url: Option<&str>) -> std::path::PathBuf {
        let path = dir.path().join("config.yaml");
        let url = mcp_url
            .map(|u| format!("    mcp-url: {u}\n"))
            .unwrap_or_default();
        std::fs::write(
            &path,
            format!(
                "current-context: acme/prod\ncontexts:\n  - name: acme/prod\n    mode: cloud\n    server: https://gw.example\n    workspace: acme-prod\n    token: {MCP}\n    control-token: {CONTROL}\n    pair-id: pair-1\n{url}"
            ),
        )
        .unwrap();
        path
    }

    fn args(client: Option<&str>, print: bool) -> RegisterArgs {
        RegisterArgs {
            client: client.map(str::to_string),
            print,
        }
    }

    /// `--print` shows the mcp token and never the control token.
    #[test]
    fn print_shows_the_mcp_token_only() {
        let dir = TempDir::new().unwrap();
        let path = config(&dir, Some("https://gw.example/mcp"));
        let out = register_at(&path, &args(None, true), None, None, OsStr::new("claude")).unwrap();
        assert!(!out.contains(CONTROL), "{out}");
        let parsed: Value = serde_json::from_str(&out).unwrap();
        let entry = &parsed["mcpServers"]["skardi"];
        assert_eq!(entry["type"], "http");
        assert_eq!(entry["url"], "https://gw.example/mcp");
        assert_eq!(entry["headers"]["Authorization"], format!("Bearer {MCP}"));
        assert_eq!(entry["headers"]["Skardi-Workspace"], "acme-prod");
    }

    /// An approval without `control` leaves one token, under `token`, and it
    /// is the one registered.
    #[test]
    fn without_a_pair_the_one_token_is_registered() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(
            &path,
            "current-context: acme/prod\ncontexts:\n  - name: acme/prod\n    mode: cloud\n    server: https://gw.example\n    workspace: acme-prod\n    token: skardi_pat_only\n    mcp-url: https://gw.example/mcp\n",
        )
        .unwrap();
        let out = register_at(&path, &args(None, true), None, None, OsStr::new("claude")).unwrap();
        assert!(out.contains("Bearer skardi_pat_only"), "{out}");
    }

    #[test]
    fn no_mcp_url_names_the_missing_setting() {
        let dir = TempDir::new().unwrap();
        let path = config(&dir, None);
        let err = register_at(&path, &args(None, true), None, None, OsStr::new("claude"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("SKARDI_GLOBAL_GATEWAY_URL"), "{err}");
    }

    #[test]
    fn another_client_is_pointed_at_print() {
        let dir = TempDir::new().unwrap();
        let path = config(&dir, Some("https://gw.example/mcp"));
        let err = register_at(
            &path,
            &args(Some("cursor"), false),
            None,
            None,
            OsStr::new("claude"),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("--print"), "{err}");
        let err = register_at(&path, &args(None, false), None, None, OsStr::new("claude"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("claude-code"), "{err}");
    }

    /// A fake `claude` with state: each entry is a file `<scope>.<name>`
    /// under `state/` holding its JSON, so a test asserts what the user is
    /// left with, not only what was called. It answers as Claude Code 2.x
    /// does — `remove` of nothing says `No MCP server named …` and exits 1,
    /// `add-json` of a name its scope holds is refused — and every call is
    /// logged, one argument per line, `--` between calls.
    ///
    /// A file `fail-<verb>-<scope>-<name>` makes that call fail with what a
    /// refusal prints, echoing its arguments to stderr the way a real error
    /// can; a removal fails only when there is something to remove.
    #[cfg(unix)]
    struct FakeClaude {
        dir: TempDir,
    }

    #[cfg(unix)]
    impl FakeClaude {
        fn new() -> FakeClaude {
            use std::os::unix::fs::PermissionsExt as _;
            let dir = TempDir::new().unwrap();
            let state = dir.path().join("state");
            std::fs::create_dir(&state).unwrap();
            let log = dir.path().join("argv.log");
            let script = format!(
                r#"#!/bin/sh
for a in "$@"; do printf '%s\n' "$a" >> '{log}'; done
printf -- '--\n' >> '{log}'
f='{state}'/"$4.$5"
fail='{state}'/"fail-$2-$4-$5"
case "$2" in
  remove)
    if [ ! -e "$f" ]; then echo "No MCP server named \"$5\" in $4 scope" >&2; exit 1; fi
    if [ -e "$fail" ]; then echo "refused: $*" >&2; exit 1; fi
    rm "$f"; echo "Removed MCP server $5 from $4 config";;
  add-json)
    if [ -e "$fail" ]; then echo "Invalid configuration: $6" >&2; exit 1; fi
    if [ -e "$f" ]; then echo "MCP server $5 already exists in $4 config" >&2; exit 1; fi
    printf '%s' "$6" > "$f"; echo "Added http MCP server $5 with $6";;
  *) exit 2;;
esac
"#,
                log = log.display(),
                state = state.display(),
            );
            let fake = dir.path().join("claude");
            std::fs::write(&fake, script).unwrap();
            std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
            FakeClaude { dir }
        }

        fn path(&self) -> std::path::PathBuf {
            self.dir.path().join("claude")
        }

        fn entry_path(&self, scope: &str, name: &str) -> std::path::PathBuf {
            self.dir
                .path()
                .join("state")
                .join(format!("{scope}.{name}"))
        }

        fn holding(self, scope: &str, name: &str, json: &str) -> FakeClaude {
            std::fs::write(self.entry_path(scope, name), json).unwrap();
            self
        }

        fn failing(self, verb: &str, scope: &str, name: &str) -> FakeClaude {
            let marker = self
                .dir
                .path()
                .join("state")
                .join(format!("fail-{verb}-{scope}-{name}"));
            std::fs::write(marker, "").unwrap();
            self
        }

        /// The entry `<scope>.<name>` now holds, if any.
        fn entry(&self, scope: &str, name: &str) -> Option<String> {
            std::fs::read_to_string(self.entry_path(scope, name)).ok()
        }

        /// Each call, as `verb scope name`.
        fn calls(&self) -> Vec<String> {
            let log = std::fs::read_to_string(self.dir.path().join("argv.log")).unwrap_or_default();
            log.split("--\n")
                .filter(|call| !call.is_empty())
                .map(|call| {
                    let argv: Vec<&str> = call.lines().collect();
                    argv[1..5.min(argv.len())]
                        .iter()
                        .filter(|a| **a != "--scope")
                        .copied()
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .collect()
        }

        fn log(&self) -> String {
            std::fs::read_to_string(self.dir.path().join("argv.log")).unwrap_or_default()
        }

        fn register(&self, path: &Path) -> Result<String> {
            register_at(
                path,
                &args(Some("claude-code"), false),
                None,
                None,
                self.path().as_os_str(),
            )
        }
    }

    #[cfg(unix)]
    const OLD: &str = r#"{"type":"http","url":"https://old.example/mcp"}"#;

    /// A re-registration replaces both earlier entries: the new one is
    /// staged, the old ones removed, the new one put in their place, and the
    /// staged copy dropped — with the mcp token, at user scope, printing only
    /// "registered".
    #[cfg(unix)]
    #[test]
    fn claude_code_is_registered_in_place_of_the_old_entries() {
        let dir = TempDir::new().unwrap();
        let path = config(&dir, Some("https://gw.example/mcp"));
        let fake = FakeClaude::new()
            .holding("local", "skardi", OLD)
            .holding("user", "skardi", OLD);

        assert_eq!(fake.register(&path).unwrap(), "registered\n");

        assert_eq!(
            fake.calls(),
            [
                "remove user skardi-pending",
                "add-json user skardi-pending",
                "remove local skardi",
                "remove user skardi",
                "add-json user skardi",
                "remove user skardi-pending",
            ]
        );
        let entry: Value = serde_json::from_str(&fake.entry("user", "skardi").unwrap()).unwrap();
        assert_eq!(entry["type"], "http");
        assert_eq!(entry["url"], "https://gw.example/mcp");
        assert_eq!(entry["headers"]["Authorization"], format!("Bearer {MCP}"));
        assert_eq!(fake.entry("local", "skardi"), None);
        assert_eq!(fake.entry("user", "skardi-pending"), None);
        assert!(!fake.log().contains(CONTROL), "{}", fake.log());
    }

    /// A first registration: every removal finds nothing, which is fine.
    #[cfg(unix)]
    #[test]
    fn a_first_registration_needs_nothing_to_remove() {
        let dir = TempDir::new().unwrap();
        let path = config(&dir, Some("https://gw.example/mcp"));
        let fake = FakeClaude::new();
        assert_eq!(fake.register(&path).unwrap(), "registered\n");
        assert!(fake.entry("user", "skardi").is_some());
        assert_eq!(fake.entry("user", "skardi-pending"), None);
    }

    /// Codex on #311: the add used to run after the removals, so a refused
    /// add left no server at all. Now it is refused while the old entries
    /// are untouched — and the error carries no token, though `claude`
    /// echoed the entry it refused.
    #[cfg(unix)]
    #[test]
    fn a_refused_add_leaves_the_old_entries_in_place() {
        let dir = TempDir::new().unwrap();
        let path = config(&dir, Some("https://gw.example/mcp"));
        let fake = FakeClaude::new()
            .holding("local", "skardi", OLD)
            .holding("user", "skardi", OLD)
            .failing("add-json", "user", "skardi-pending");

        let err = format!("{:#}", fake.register(&path).unwrap_err());
        assert!(err.contains("nothing was changed"), "{err}");
        assert!(err.contains("add-json"), "{err}");
        assert!(!err.contains(MCP), "{err}");
        assert!(err.contains("(redacted)"), "{err}");
        assert_eq!(fake.entry("local", "skardi").as_deref(), Some(OLD));
        assert_eq!(fake.entry("user", "skardi").as_deref(), Some(OLD));
        assert!(
            !fake
                .calls()
                .iter()
                .any(|c| c.ends_with(" skardi") && c.starts_with("remove")),
            "{:?}",
            fake.calls()
        );
    }

    /// A removal that fails for any reason but absence stops the swap: no
    /// "registered", the staged copy is taken back out, and the entry the
    /// removal could not touch is still there.
    #[cfg(unix)]
    #[test]
    fn a_failed_removal_is_reported_and_the_staged_copy_withdrawn() {
        let dir = TempDir::new().unwrap();
        let path = config(&dir, Some("https://gw.example/mcp"));
        let fake = FakeClaude::new()
            .holding("user", "skardi", OLD)
            .failing("remove", "user", "skardi");

        let err = format!("{:#}", fake.register(&path).unwrap_err());
        assert!(
            err.contains("could not remove the existing `skardi` entry at user scope"),
            "{err}"
        );
        assert!(err.contains("refused"), "{err}");
        assert!(!err.contains(MCP), "{err}");
        assert_eq!(fake.entry("user", "skardi").as_deref(), Some(OLD));
        assert_eq!(fake.entry("user", "skardi-pending"), None);
        assert!(
            !fake.calls().contains(&"add-json user skardi".to_string()),
            "{:?}",
            fake.calls()
        );
    }

    /// The old entry is gone but the new one cannot take its name: the
    /// server is still registered, as `skardi-pending`, and the error says
    /// so rather than "registered".
    #[cfg(unix)]
    #[test]
    fn a_refused_final_add_leaves_the_new_server_staged_and_says_so() {
        let dir = TempDir::new().unwrap();
        let path = config(&dir, Some("https://gw.example/mcp"));
        let fake = FakeClaude::new()
            .holding("user", "skardi", OLD)
            .failing("add-json", "user", "skardi");

        let err = format!("{:#}", fake.register(&path).unwrap_err());
        assert!(err.contains("registered as `skardi-pending`"), "{err}");
        assert!(!err.contains(MCP), "{err}");
        let staged = fake.entry("user", "skardi-pending").unwrap();
        assert!(staged.contains("https://gw.example/mcp"), "{staged}");
    }

    /// Everything held but the last cleanup: the registration is real, and
    /// the leftover is named, not hidden behind "registered".
    #[cfg(unix)]
    #[test]
    fn a_staged_copy_that_cannot_be_removed_is_named() {
        let dir = TempDir::new().unwrap();
        let path = config(&dir, Some("https://gw.example/mcp"));
        let fake = FakeClaude::new().failing("remove", "user", "skardi-pending");

        let err = format!("{:#}", fake.register(&path).unwrap_err());
        assert!(err.contains("`skardi` is registered"), "{err}");
        assert!(
            err.contains("claude mcp remove --scope user skardi-pending"),
            "{err}"
        );
        assert!(fake.entry("user", "skardi").is_some());
    }

    /// A staged entry an interrupted run left behind is cleared first, so
    /// it cannot make the staging add refuse.
    #[cfg(unix)]
    #[test]
    fn a_leftover_staged_entry_is_cleared_first() {
        let dir = TempDir::new().unwrap();
        let path = config(&dir, Some("https://gw.example/mcp"));
        let fake = FakeClaude::new().holding("user", "skardi-pending", OLD);
        assert_eq!(fake.register(&path).unwrap(), "registered\n");
        assert_eq!(fake.entry("user", "skardi-pending"), None);
    }

    #[test]
    fn a_missing_claude_names_print_as_the_way_out() {
        let dir = TempDir::new().unwrap();
        let path = config(&dir, Some("https://gw.example/mcp"));
        let missing = dir.path().join("no-such-claude");
        let err = register_at(
            &path,
            &args(Some("claude-code"), false),
            None,
            None,
            missing.as_os_str(),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("is Claude Code installed"), "{err}");
        assert!(err.contains("--print"), "{err}");
        // Nothing ran, so no step's account of what it left applies.
        assert!(!err.contains("earlier run"), "{err}");
    }
}
