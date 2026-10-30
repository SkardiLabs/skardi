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
//! <json>` itself and prints only "registered", so the token never passes
//! through the agent's transcript. It is in that child's arguments while it
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

/// `claude mcp add-json --scope user skardi <json>`, after removing any
/// earlier `skardi` entry: `add-json` refuses a name its scope already holds,
/// so a re-login's re-run would otherwise fail, and a local-scope entry from
/// an older setup outranks the user-scope one in its folder. A failed remove
/// is the usual case on a first registration and is not reported.
///
/// Nothing `claude` prints is passed through: its output could quote the
/// configuration it was given, and "registered" is the whole answer.
fn register_claude_code(server: &Server, claude: &OsStr) -> Result<()> {
    let entry = serde_json::to_string(&server.entry()).expect("a Value always serializes");
    for scope in ["local", "user"] {
        let _ = Command::new(claude)
            .args(["mcp", "remove", "--scope", scope, SERVER_NAME])
            .output();
    }
    let output = Command::new(claude)
        .args(["mcp", "add-json", "--scope", "user", SERVER_NAME])
        .arg(&entry)
        .output()
        .map_err(|err| {
            anyhow::anyhow!(
                "could not run `claude` ({err}) — is Claude Code installed and on PATH? Or run `skardi mcp register --print` and add the server by hand"
            )
        })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr)
            .replace(&server.token, "(redacted)")
            .trim()
            .to_string();
        bail!(
            "`claude mcp add-json` failed ({}){}",
            output.status,
            if stderr.is_empty() {
                String::new()
            } else {
                format!(": {stderr}")
            }
        );
    }
    Ok(())
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

    /// A fake `claude` that records its arguments: the registration replaces
    /// any earlier entry, writes the mcp token at user scope, and prints only
    /// "registered".
    #[cfg(unix)]
    #[test]
    fn claude_code_is_registered_with_add_json_and_prints_only_registered() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = TempDir::new().unwrap();
        let path = config(&dir, Some("https://gw.example/mcp"));
        let log = dir.path().join("argv.log");
        let fake = dir.path().join("claude");
        std::fs::write(
            &fake,
            format!(
                "#!/bin/sh\nfor a in \"$@\"; do printf '%s\\n' \"$a\" >> '{}'; done\nprintf -- '--\\n' >> '{}'\necho \"Added HTTP MCP server skardi with $5\"\n",
                log.display(),
                log.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

        let out = register_at(
            &path,
            &args(Some("claude-code"), false),
            None,
            None,
            fake.as_os_str(),
        )
        .unwrap();
        assert_eq!(out, "registered\n");

        let argv = std::fs::read_to_string(&log).unwrap();
        let calls: Vec<&str> = argv.split("--\n").filter(|c| !c.is_empty()).collect();
        assert_eq!(calls.len(), 3, "{argv}");
        assert_eq!(calls[0], "mcp\nremove\n--scope\nlocal\nskardi\n");
        assert_eq!(calls[1], "mcp\nremove\n--scope\nuser\nskardi\n");
        let add: Vec<&str> = calls[2].lines().collect();
        assert_eq!(&add[..5], ["mcp", "add-json", "--scope", "user", "skardi"]);
        let entry: Value = serde_json::from_str(add[5]).unwrap();
        assert_eq!(entry["url"], "https://gw.example/mcp");
        assert_eq!(entry["headers"]["Authorization"], format!("Bearer {MCP}"));
        assert!(!argv.contains(CONTROL), "{argv}");
    }

    #[cfg(unix)]
    #[test]
    fn a_failing_claude_is_reported_without_the_token() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = TempDir::new().unwrap();
        let path = config(&dir, Some("https://gw.example/mcp"));
        let fake = dir.path().join("claude");
        std::fs::write(
            &fake,
            "#!/bin/sh\n[ \"$2\" = add-json ] || exit 1\necho \"bad config: $6\" >&2\nexit 3\n",
        )
        .unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        let err = register_at(
            &path,
            &args(Some("claude-code"), false),
            None,
            None,
            fake.as_os_str(),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("add-json"), "{err}");
        assert!(!err.contains(MCP), "{err}");
    }
}
