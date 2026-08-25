//! Connection config resolution for the CLI: which server the command talks
//! to, with which credential, and — since contexts landed — which *context*
//! supplies them.
//!
//! Two file shapes are supported. The current one is a kubectl-shaped list of
//! named contexts with a `current-context` pointer; the legacy one is a single
//! `spec: {server, token}` block, which resolves as a lone context named
//! `default` so existing installs keep working with no migration step.
//!
//! Context SELECTION is `--context` > `$SKARDI_CONTEXT` > `current-context`,
//! and then one step the design's §5.1 chain does not list: a file with
//! exactly ONE context and no pointer selects it. That step is not a
//! convenience — §5.2's legacy back-compat depends on it. A pre-contexts
//! `spec:` file has no `current-context` to write, so without auto-selection
//! its server and token would never be consulted and every existing install
//! would silently start talking to the built-in default. Several contexts and
//! no pointer selects NOTHING, so flags, env, and the default still apply.
//!
//! Precedence is per-field and differs by context mode, which is the one
//! subtlety worth reading before changing anything here:
//!
//! ```text
//! mode: server   server: --server > $SKARDI_SERVER_URL > context > default
//!                token:  --token  > $SKARDI_API_TOKEN  > context > (none)
//!
//! mode: cloud    server: --server > context            (env is a hard error)
//!                token:  --token  > context            (env is a hard error)
//! ```
//!
//! A cloud context is authoritative because its `server` and `token` are a
//! matched pair: the PAT is scoped to one workspace at one role, and the
//! gateway that honours it is the one `login` wrote alongside it. A stray
//! `SKARDI_SERVER_URL` left exported from the single-server era would send a
//! workspace-scoped PAT to whatever listens there, so the conflict is refused
//! by name rather than silently resolved. Flags still win: passing `--server`
//! is a deliberate act at the point of use.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

/// Server URL used when no flag, env var, or context supplies one.
pub const DEFAULT_SERVER_URL: &str = "http://127.0.0.1:8080";

const SERVER_URL_ENV: &str = "SKARDI_SERVER_URL";
const API_TOKEN_ENV: &str = "SKARDI_API_TOKEN";
const CONTEXT_ENV: &str = "SKARDI_CONTEXT";

/// How old a temp file must be before the sweep treats it as abandoned
/// rather than as a concurrent writer's work in progress.
const STALE_TEMP_AGE: std::time::Duration = std::time::Duration::from_secs(60);

/// The name a legacy `spec:`-only file resolves under, so that a file with no
/// `contexts:` key still has a context to select and to print.
pub const LEGACY_CONTEXT_NAME: &str = "default";

/// How a context reaches its server, and therefore which capabilities it has.
///
/// `mode` never selects a URL — the gateway mounts skardi-server's own paths —
/// and is consulted for exactly two things: pre-flight capability messages,
/// and whether login/expiry logic applies.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ContextMode {
    /// A skardi-server, reached directly. The default, and the only mode with
    /// the full command surface.
    #[default]
    Server,
    /// A skardi-cloud gateway, reached with a workspace-scoped PAT.
    Cloud,
}

impl ContextMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Server => "server",
            Self::Cloud => "cloud",
        }
    }
}

impl fmt::Display for ContextMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `pad`, not `write_str`: a Display that writes directly ignores the
        // formatter's width, so `{:<8}` silently did nothing and
        // `get-contexts` printed ragged columns.
        f.pad(self.as_str())
    }
}

/// One named context. `server` is the only field every mode needs; the rest
/// carry the cloud dimension `login` writes and `logout` needs.
///
/// `extra` preserves keys this binary does not model, so a newer CLI's fields
/// survive an older CLI's `use-context` rewrite — the file is shared state
/// between versions, and a rewrite that dropped unknown keys would quietly
/// downgrade it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct Context {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    #[serde(default, skip_serializing_if = "is_default_mode")]
    pub mode: ContextMode,
    /// LOAD-BEARING in cloud mode: sent per request as `Skardi-Workspace`, and
    /// a cloud context without one is refused at resolution rather than
    /// producing an ambiguous request at the gateway.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// The PAT's id, needed by `logout --revoke` — the token itself cannot
    /// name itself to the revoke endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_expires_at: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_yaml::Value>,
}

fn is_default_mode(mode: &ContextMode) -> bool {
    *mode == ContextMode::Server
}

/// The whole config file. `spec` is the legacy single-server block, kept for
/// back-compat (§5.2); `extra` preserves unmodeled top-level keys for the same
/// reason `Context::extra` does.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct ContextsFile {
    /// Present so a rewrite keeps the manifest recognizable; not read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Where `login` talks. Optional: only the cloud flow reads it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_plane: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_context: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contexts: Vec<Context>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<LegacySpec>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_yaml::Value>,
}

/// The pre-contexts file shape: one server, one token.
///
/// `extra` for the same reason the other two structs have it: `use-context`
/// re-serializes `spec:` WITHOUT promoting it, so anything unmodeled inside
/// the block would be dropped by a command that never meant to touch it.
/// The documented legacy shape only ever had `server`/`token`, so this is
/// insurance rather than a live case — but it was the one place the
/// "unknown keys survive an older CLI" promise did not hold.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct LegacySpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_yaml::Value>,
}

impl ContextsFile {
    /// Every context the file defines, with the legacy `spec:` block folded in
    /// as `default` when there is no `contexts:` list. A file carrying BOTH
    /// prefers `contexts:`.
    ///
    /// PURE and idempotent, deliberately. This used to `eprintln!` the
    /// both-shapes warning, which made "warns once" a rule every caller had
    /// to obey — and two callers didn't, printing it twice on the
    /// unknown-context path and again in `use_context`. Both were patched at
    /// the call site, which is the fragile kind of fix. The warning now fires
    /// where the file is READ (see `warn_about_file`), because carrying both
    /// shapes is a property of the file rather than of each access.
    pub fn effective_contexts(&self) -> Vec<Context> {
        if !self.contexts.is_empty() {
            return self.contexts.clone();
        }
        match &self.spec {
            Some(spec) => vec![Context {
                name: LEGACY_CONTEXT_NAME.to_string(),
                server: spec.server.clone(),
                token: spec.token.clone(),
                ..Context::default()
            }],
            None => Vec::new(),
        }
    }

    /// Fold a legacy `spec:` block into `contexts:` so a mutation cannot drop
    /// the credential it holds. No-op once `contexts:` is populated.
    ///
    /// The ordering matters and is why this is one function rather than two
    /// lines at each call site: promote FIRST, then clear `spec`. It was
    /// duplicated verbatim in `set-context` and `delete-context`, and M2's
    /// `login` needs it too.
    pub fn promote_legacy_spec(&mut self) {
        if self.contexts.is_empty() {
            self.contexts = self.effective_contexts();
            self.spec = None;
        }
    }

    pub fn context_names(&self) -> Vec<String> {
        self.effective_contexts()
            .into_iter()
            .map(|c| c.name)
            .collect()
    }
}

/// Why a config could not be turned into a usable `{server, token}` pair.
///
/// Every variant is a HARD error by deliberate choice: each one describes a
/// situation where guessing would send a credential somewhere the operator did
/// not name. Read failures are the opposite — see [`load`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// `--context` / `$SKARDI_CONTEXT` / `current-context` named something the
    /// file does not define. Never falls through to the built-in default:
    /// that is the one failure mode that would silently send a cloud query to
    /// a local server.
    UnknownContext {
        name: String,
        available: Vec<String>,
    },
    /// A cloud context with no `workspace` — the request would be ambiguous at
    /// the gateway, which answers `workspace_required`.
    CloudContextWithoutWorkspace { name: String },
    /// A cloud context with no `server`. Without this the resolved server
    /// falls through to [`DEFAULT_SERVER_URL`] while the token stays the
    /// workspace-scoped PAT — the exact "quietly send a cloud query to a
    /// local server" failure the unknown-context guard exists to prevent,
    /// reached by a different route (found in review, reproduced against the
    /// real binary).
    CloudContextWithoutServer { name: String },
    /// An environment variable would override a cloud context's matched
    /// `{server, token}` pair. Named rather than silently applied or silently
    /// ignored (§5.1).
    EnvConflictsWithCloudContext { name: String, variable: String },
    /// A mutating command met a file it could not parse (§5.4).
    UnparsableForMutation { path: PathBuf, error: String },
    /// A mutating command met a file it could not READ. Distinct from the
    /// parse case because the advice is the same but the diagnosis is not:
    /// reporting "does not parse (Permission denied)" sends the operator
    /// hunting for a YAML error that is not there.
    UnreadableForMutation { path: PathBuf, error: String },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownContext { name, available } => {
                write!(f, "no context named '{name}' in the config")?;
                if available.is_empty() {
                    write!(f, " (the config defines no contexts)")
                } else {
                    write!(f, ". Available: {}", available.join(", "))
                }
            }
            Self::CloudContextWithoutWorkspace { name } => write!(
                f,
                "context '{name}' is mode: cloud but names no workspace; \
                 add one with 'skardi config set-context {name} --workspace SLUG' \
                 or re-run 'skardi login'"
            ),
            Self::CloudContextWithoutServer { name } => write!(
                f,
                "context '{name}' is mode: cloud but names no server, and a cloud \
                 context is never defaulted to {DEFAULT_SERVER_URL} — that would send \
                 its workspace-scoped token to a local server. Set one with 'skardi \
                 config set-context {name} --server URL', pass --server, or re-run \
                 'skardi login'"
            ),
            Self::EnvConflictsWithCloudContext { name, variable } => write!(
                f,
                "${variable} is set, but context '{name}' is mode: cloud and is \
                 authoritative for its server and token. Unset ${variable}, or pass \
                 --server/--token to override deliberately"
            ),
            Self::UnparsableForMutation { path, error } => write!(
                f,
                "refusing to modify {}: it exists but does not parse ({error}). \
                 Fix or move the file first — rewriting it would discard the \
                 credentials it may still hold",
                path.display()
            ),
            Self::UnreadableForMutation { path, error } => write!(
                f,
                "refusing to modify {}: it exists but cannot be read ({error}). \
                 Fix the permissions or move the file first — rewriting it would \
                 discard the credentials it may still hold",
                path.display()
            ),
        }
    }
}

impl std::error::Error for ConfigError {}

/// The context a command resolved to, carried alongside the connection pair so
/// callers can gate capabilities and render messages that name the context.
///
/// `token_expires_at` is populated but not yet read — M2's expiry check is its
/// only consumer. That is a different case from the `workspace()` accessor
/// removed from this milestone: a method with no callers is dead code, whereas
/// this is a value resolution already computed while validating the context,
/// so dropping it would mean re-deriving it in M2 for nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedContext {
    pub name: String,
    pub mode: ContextMode,
    /// Always `Some` for `ContextMode::Cloud` — resolution refuses otherwise.
    pub workspace: Option<String>,
    pub token_expires_at: Option<String>,
}

/// Resolved connection settings for talking to a skardi-server instance or a
/// skardi-cloud gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    pub server: String,
    pub token: Option<String>,
    /// `None` when no config file defined a context — the flags/env/default
    /// path, which is every pre-contexts install and every CI one-liner.
    pub context: Option<SelectedContext>,
}

impl ClientConfig {
    /// The selected context when it is a CLOUD one — the single guard for
    /// everything §8 changes (capability gating, credential expiry, the
    /// gateway's typed errors). Server-mode and file-less resolutions answer
    /// `None`, so those paths cannot accidentally inherit cloud behaviour.
    pub fn cloud_context(&self) -> Option<&SelectedContext> {
        self.context
            .as_ref()
            .filter(|c| c.mode == ContextMode::Cloud)
    }

    /// The workspace a cloud context names, sent per request as
    /// `Skardi-Workspace` (§7.3).
    ///
    /// Deliberately OUTSIDE the reserved `x-skardi-*` prefix: the gateway
    /// strips client-supplied headers in that namespace before forwarding to
    /// the engine, so a selector wearing it would be silently dropped.
    ///
    /// Always `Some` for a cloud context — resolution refuses one without a
    /// workspace — so this is `Some` exactly when the header applies.
    pub fn workspace(&self) -> Option<&str> {
        self.cloud_context()?.workspace.as_deref()
    }

    /// Resolve the effective client config from flags, environment, and the
    /// user's `~/.skardi/config.yaml`.
    ///
    /// Not unit-tested directly: it reads process-global environment variables,
    /// which race under parallel test execution. All precedence and selection
    /// logic lives in [`resolve_from`], which is tested exhaustively.
    pub fn resolve(
        flag_server: Option<String>,
        flag_token: Option<String>,
        flag_context: Option<String>,
    ) -> Result<ClientConfig, ConfigError> {
        let inputs = ResolveInputs {
            flag_server,
            flag_token,
            flag_context,
            env_server: std::env::var(SERVER_URL_ENV).ok(),
            env_token: std::env::var(API_TOKEN_ENV).ok(),
            env_context: std::env::var(CONTEXT_ENV).ok(),
            file: default_config_path().as_deref().and_then(load),
        };
        resolve_from(inputs)
    }
}

/// Everything [`resolve_from`] needs, passed in rather than read, so the
/// function is pure and the precedence matrix is testable without touching
/// process-global state.
#[derive(Debug, Default)]
pub struct ResolveInputs {
    pub flag_server: Option<String>,
    pub flag_token: Option<String>,
    pub flag_context: Option<String>,
    pub env_server: Option<String>,
    pub env_token: Option<String>,
    pub env_context: Option<String>,
    pub file: Option<ContextsFile>,
}

/// The context-selection chain: `--context` > `$SKARDI_CONTEXT` >
/// `current-context` > a lone context.
///
/// Extracted so the commands that REPORT the selection use the same code that
/// makes it. `get-contexts`'s `*` marker and `current-context` previously
/// compared the raw `current-context` field, which disagreed with resolution
/// in three ways: no lone-context step (so a legacy `spec:` install showed no
/// marker while every command happily used `default`), no trimming, and no
/// awareness of `--context`/`$SKARDI_CONTEXT`.
pub fn select_context(
    contexts: &[Context],
    current_context: Option<&str>,
    flag_context: Option<&str>,
    env_context: Option<&str>,
) -> Result<Option<Context>, ConfigError> {
    let names = || contexts.iter().map(|c| c.name.clone()).collect::<Vec<_>>();
    let lookup = |name: &str| contexts.iter().find(|c| c.name == name).cloned();

    let requested =
        non_empty(flag_context.map(str::to_string)).or(non_empty(env_context.map(str::to_string)));
    if let Some(name) = requested {
        return lookup(&name)
            .ok_or(ConfigError::UnknownContext {
                name,
                available: names(),
            })
            .map(Some);
    }
    if let Some(current) = non_empty(current_context.map(str::to_string)) {
        // An explicit current-context that does not resolve is as much a typo
        // as an explicit --context, and gets the same hard error.
        return lookup(&current)
            .ok_or(ConfigError::UnknownContext {
                name: current,
                available: names(),
            })
            .map(Some);
    }
    // No pointer: a single-context file (including a legacy `spec:` one) is
    // unambiguous, so use it — §5.2's back-compat depends on this step.
    // Several with no pointer selects NOTHING, so flags, env, and the default
    // still apply.
    Ok((contexts.len() == 1).then(|| contexts[0].clone()))
}

/// Pure precedence + selection resolution. No I/O and no env reads.
///
/// Empty and whitespace-only values count as unset at every level — an
/// exported-but-empty `SKARDI_SERVER_URL=` (typical of wrapper scripts that
/// export unconditionally) must fall through rather than produce an empty base
/// URL — and kept values are trimmed.
pub fn resolve_from(inputs: ResolveInputs) -> Result<ClientConfig, ConfigError> {
    let ResolveInputs {
        flag_server,
        flag_token,
        flag_context,
        env_server,
        env_token,
        env_context,
        file,
    } = inputs;

    let flag_server = non_empty(flag_server);
    let flag_token = non_empty(flag_token);
    let env_server = non_empty(env_server);
    let env_token = non_empty(env_token);

    // Selection: --context > $SKARDI_CONTEXT > current-context.
    let file = file.unwrap_or_default();
    let selected = select_context(
        &file.effective_contexts(),
        file.current_context.as_deref(),
        flag_context.as_deref(),
        env_context.as_deref(),
    )?;

    let Some(context) = selected else {
        return Ok(ClientConfig {
            server: flag_server
                .or(env_server)
                .unwrap_or_else(|| DEFAULT_SERVER_URL.to_string()),
            token: flag_token.or(env_token),
            context: None,
        });
    };

    let mode = context.mode;
    let workspace = non_empty(context.workspace.clone());
    if mode == ContextMode::Cloud && workspace.is_none() {
        return Err(ConfigError::CloudContextWithoutWorkspace {
            name: context.name.clone(),
        });
    }

    let (server, token) = if mode == ContextMode::Cloud {
        // The env is refused rather than ranked (see the module doc), but
        // only per field and only where it would actually WIN. A flag for
        // the same field is a deliberate act at the point of use and takes
        // precedence per §5.1 — erroring anyway made this error's own advice
        // ("pass --server/--token to override") impossible to follow, which
        // review caught by trying it.
        for (env, flag, variable) in [
            (&env_server, &flag_server, SERVER_URL_ENV),
            (&env_token, &flag_token, API_TOKEN_ENV),
        ] {
            if env.is_some() && flag.is_none() {
                return Err(ConfigError::EnvConflictsWithCloudContext {
                    name: context.name.clone(),
                    variable: variable.to_string(),
                });
            }
        }
        let server = flag_server.or_else(|| non_empty(context.server.clone()));
        // Refused HERE, not left to the `unwrap_or_else` below: that default
        // is correct for a server-mode context and catastrophic for a cloud
        // one, which carries a workspace-scoped PAT.
        if server.is_none() {
            return Err(ConfigError::CloudContextWithoutServer {
                name: context.name.clone(),
            });
        }
        (
            server,
            flag_token.or_else(|| non_empty(context.token.clone())),
        )
    } else {
        (
            flag_server
                .or(env_server)
                .or_else(|| non_empty(context.server.clone())),
            flag_token
                .or(env_token)
                .or_else(|| non_empty(context.token.clone())),
        )
    };

    Ok(ClientConfig {
        server: server.unwrap_or_else(|| DEFAULT_SERVER_URL.to_string()),
        token,
        context: Some(SelectedContext {
            name: context.name,
            mode,
            workspace,
            token_expires_at: non_empty(context.token_expires_at),
        }),
    })
}

/// Treat an empty (or whitespace-only) string as unset, and trim what is kept
/// — a padded `SKARDI_SERVER_URL=" http://x "` must not survive into request
/// URLs.
fn non_empty(value: Option<String>) -> Option<String> {
    value
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// `~/.skardi/config.yaml`, or `None` when the home directory is unknowable.
pub fn default_config_path() -> Option<PathBuf> {
    dirs::home_dir().map(|home| home.join(".skardi").join("config.yaml"))
}

/// The per-context keys this binary reads. Used only to spot a typo in
/// [`warn_about_file`]; `Context::extra` still preserves whatever it is given.
const KNOWN_CONTEXT_KEYS: [&str; 8] = [
    "name",
    "server",
    "mode",
    "workspace",
    "user",
    "token",
    "token-id",
    "token-expires-at",
];

/// Warn about a freshly-read file's shape. Called once per read, which is what
/// makes "warns once" structural rather than a rule callers must remember.
fn warn_about_file(file: &ContextsFile) {
    if !file.contexts.is_empty() && file.spec.is_some() {
        eprintln!(
            "warning: config has both 'contexts:' and a legacy 'spec:' block; \
             using 'contexts:' and ignoring 'spec:'"
        );
    }
    // A key that differs from a real one only in case (or by a stray
    // character) is preserved faithfully by `extra` and then IGNORED — and
    // for `mode` that fails in the UNSAFE direction: `Mode: cloud` leaves the
    // context at its `server` default, so the cloud-authoritative rule stops
    // applying and $SKARDI_SERVER_URL can redirect a workspace PAT again.
    // Preservation is still the right default for forward compatibility, so
    // this makes the typo visible instead of refusing the key.
    for context in &file.contexts {
        for key in context.extra.keys() {
            if let Some(known) = KNOWN_CONTEXT_KEYS
                .iter()
                .find(|k| k.eq_ignore_ascii_case(key))
            {
                eprintln!(
                    "warning: context '{}' has key '{key}', which differs only in case \
                     from '{known}'; it is being preserved but NOT read",
                    context.name
                );
            }
        }
    }
}

/// Describe a YAML parse failure by POSITION, never by content.
///
/// `serde_yaml`'s own Display quotes the offending scalar, so a file whose
/// broken line holds a credential prints it — and the read-side warning below
/// fires on EVERY command for as long as the file stays broken, which is
/// exactly the state these paths exist to survive. Line and column are what
/// an operator needs in order to fix it; the value is already in the file in
/// front of them.
pub fn describe_parse_error(err: &serde_yaml::Error) -> String {
    match err.location() {
        Some(location) => format!(
            "invalid YAML at line {}, column {}",
            location.line(),
            location.column()
        ),
        None => "invalid YAML".to_string(),
    }
}

/// Load `path` for READING. Tolerant by design (§5.4): a missing file is
/// `None`, and an unparsable one warns and resolves to `None` so that
/// `--server`/`--token` still work while the operator fixes it. Never fatal.
///
/// Mutations use [`load_for_mutation`] instead, which refuses the same file.
pub fn load(path: &Path) -> Option<ContextsFile> {
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        // Absence is the common case and says nothing.
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return None,
        // Anything else is a file the operator believes is in effect. Falling
        // through in silence is how a root-owned config (a `sudo` run, a
        // restored backup, a container volume with another uid) ends up
        // resolving to http://127.0.0.1:8080 while they think they are
        // talking to their gateway — the same failure the unknown-context and
        // CloudContextWithoutServer guards exist to prevent, by a route
        // nobody was watching. Still non-fatal per §5.4.
        Err(err) => {
            eprintln!(
                "warning: ignoring unreadable config file {}: {err}",
                path.display()
            );
            return None;
        }
    };
    match serde_yaml::from_str::<ContextsFile>(&content) {
        Ok(file) => {
            warn_about_file(&file);
            Some(file)
        }
        Err(err) => {
            eprintln!(
                "warning: ignoring malformed config file {}: {}",
                path.display(),
                describe_parse_error(&err)
            );
            None
        }
    }
}

/// Load `path` for WRITING, refusing a file that exists but does not parse.
///
/// The asymmetry with [`load`] is the whole point. A rewrite built from an
/// empty parse tree would atomically replace a malformed-but-credential-bearing
/// file: the rename is torn-write-safe, not data-loss-safe. Recovery is
/// deliberately manual — no `--force` — because the file may hold the only copy
/// of a PAT nobody can re-mint without re-authenticating.
///
/// A missing file yields an empty tree, which is how the first `login` or
/// `set-context` creates one.
pub fn load_for_mutation(path: &Path) -> Result<ContextsFile, ConfigError> {
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        // Any read failure (absent, or unreadable) yields a fresh tree only
        // when the file truly is absent; an unreadable-but-present file must
        // not be clobbered either.
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ContextsFile {
                kind: Some("client".to_string()),
                ..ContextsFile::default()
            });
        }
        Err(err) => {
            return Err(ConfigError::UnreadableForMutation {
                path: path.to_path_buf(),
                error: err.to_string(),
            });
        }
    };
    let file = serde_yaml::from_str::<ContextsFile>(&content).map_err(|err| {
        ConfigError::UnparsableForMutation {
            path: path.to_path_buf(),
            error: describe_parse_error(&err),
        }
    })?;
    warn_about_file(&file);
    Ok(file)
}

/// Write `file` to `path` atomically, owner-readable only.
///
/// NOT serialized against a concurrent writer. Two `skardi config` processes
/// racing on the same file both read, both build a tree, and the later rename
/// wins, so one edit is silently lost. Each individual write stays atomic and
/// mode-correct, which is what §5.3 promises; whole-operation exclusion needs
/// file locking and is deliberately not added here — it would be this crate's
/// first locking dependency, and `login` is where the race actually matters.
/// Recorded so the gap is visible rather than assumed absent.
///
/// Temp file in the SAME directory then rename: a cross-filesystem rename is
/// not atomic, and `~/.skardi` is where the target lives. The temp file is
/// created `0600` before any bytes are written, so a token never exists in a
/// world-readable file even briefly.
pub fn save(path: &Path, file: &ContextsFile) -> anyhow::Result<()> {
    use anyhow::Context as _;

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    create_config_dir(parent)
        .with_context(|| format!("create config directory {}", parent.display()))?;

    // A pre-existing group- or world-readable config is a real exposure of a
    // live credential, so it is reported rather than silently tightened —
    // the operator may want to know their file has been readable.
    warn_if_loose_permissions(path);

    let yaml = serde_yaml::to_string(file).context("serialize config")?;
    let stem = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "config.yaml".to_string());
    // A crash between create and rename leaves a temp file holding a full copy
    // of every token, and nothing else ever removed it. Sweep the strays we
    // can see before adding another.
    remove_stale_temp_files(parent, &stem, path);
    // pid alone is both predictable and REUSABLE — two saves in one process
    // collided on the same name — so the nanosecond disambiguates, and
    // `create_new` below refuses an existing file rather than inheriting its
    // mode.
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or_default();
    let temp = parent.join(format!(".{stem}.tmp-{}-{unique}", std::process::id()));

    write_owner_only(&temp, yaml.as_bytes())
        .with_context(|| format!("write {}", temp.display()))?;
    if let Err(err) = std::fs::rename(&temp, path) {
        let _ = std::fs::remove_file(&temp);
        return Err(
            anyhow::Error::new(err).context(format!("replace {} atomically", path.display()))
        );
    }
    // The rename is durable only once the DIRECTORY entry is; without this a
    // crash can leave neither the old file nor the new one. Best-effort: some
    // filesystems refuse to open a directory for sync, and a config write must
    // not fail for that.
    if let Ok(dir) = std::fs::File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}

/// Create the config directory owner-only.
///
/// `create_dir_all` applies the process umask, so a first `set-context` on a
/// typical `umask 022` box left `~/.skardi` world-listable — the directory
/// whose whole purpose is holding credentials. `~/.ssh` and `~/.gnupg` are
/// 0700 for the same reason. `mode` applies only to directories this call
/// CREATES, so a pre-existing loose one is untouched, mirroring
/// [`warn_if_loose_permissions`]'s asymmetry for the file.
#[cfg(unix)]
fn create_config_dir(parent: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt as _;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(parent)
}

#[cfg(not(unix))]
fn create_config_dir(parent: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(parent)
}

/// Delete leftover temp files for `stem` in `parent`.
///
/// Only names matching the shape this module writes, only regular files, and
/// never the target itself. Best-effort: a stray we cannot remove must not
/// fail the save, but it IS reported, because the file holds a token.
fn remove_stale_temp_files(parent: &Path, stem: &str, target: &Path) {
    let prefix = format!(".{stem}.tmp-");
    let Ok(entries) = std::fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        let candidate = entry.path();
        if candidate == target || !entry.file_type().is_ok_and(|t| t.is_file()) {
            continue;
        }
        let matches_shape = candidate
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with(&prefix));
        if !matches_shape {
            continue;
        }
        // Only what no live writer could still own. Without this the sweep
        // deletes a CONCURRENT process's temp file between its create and its
        // rename, turning the documented lost-edit race into that process
        // failing with "replace … atomically: No such file or directory" —
        // an error pointing at entirely the wrong thing.
        let stale = entry
            .metadata()
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.elapsed().ok())
            .is_some_and(|age| age > STALE_TEMP_AGE);
        if !stale {
            continue;
        }
        if std::fs::remove_file(&candidate).is_err() {
            eprintln!(
                "warning: could not remove leftover {} — it may hold a copy of a token",
                candidate.display()
            );
        }
    }
}

#[cfg(unix)]
fn write_owner_only(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;

    // `create_new`, not `create` + `truncate`: `mode` applies only when the
    // file is CREATED, so truncating a pre-existing group-readable temp file
    // kept its mode and then renamed it over the config. Failing on an
    // existing name is the safe direction, and the caller's name is unique.
    let mut handle = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    handle.write_all(bytes)?;
    handle.sync_all()
}

#[cfg(not(unix))]
fn write_owner_only(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    // No mode bits to set: on Windows the file inherits the directory ACL.
    std::fs::write(path, bytes)
}

#[cfg(unix)]
fn warn_if_loose_permissions(path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;

    if let Ok(meta) = std::fs::metadata(path) {
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            eprintln!(
                "warning: {} was mode {:o} (readable beyond its owner); \
                 rewriting it as 0600",
                path.display(),
                mode
            );
        }
    }
}

#[cfg(not(unix))]
fn warn_if_loose_permissions(_path: &Path) {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use tempfile::{NamedTempFile, TempDir};

    /// A two-context file: one cloud, one local — the shape `login` writes.
    fn file() -> ContextsFile {
        ContextsFile {
            kind: Some("client".to_string()),
            current_context: Some("acme/prod".to_string()),
            contexts: vec![
                Context {
                    name: "acme/prod".to_string(),
                    server: Some("https://gw.skardi.ai".to_string()),
                    mode: ContextMode::Cloud,
                    workspace: Some("acme-prod".to_string()),
                    token: Some("skardi_pat_live".to_string()),
                    ..Context::default()
                },
                Context {
                    name: "local".to_string(),
                    server: Some("http://127.0.0.1:9999".to_string()),
                    ..Context::default()
                },
            ],
            ..ContextsFile::default()
        }
    }

    fn inputs(file: Option<ContextsFile>) -> ResolveInputs {
        ResolveInputs {
            file,
            ..ResolveInputs::default()
        }
    }

    fn write(contents: &str) -> NamedTempFile {
        let mut handle = NamedTempFile::new().unwrap();
        handle.write_all(contents.as_bytes()).unwrap();
        handle
    }

    // ── selection ────────────────────────────────────────────────────────

    #[test]
    fn current_context_selects_when_no_flag_or_env_names_one() {
        let resolved = resolve_from(inputs(Some(file()))).unwrap();

        assert_eq!(resolved.server, "https://gw.skardi.ai");
        assert_eq!(resolved.token.as_deref(), Some("skardi_pat_live"));
        let context = resolved.context.unwrap();
        assert_eq!(context.name, "acme/prod");
        assert_eq!(context.mode, ContextMode::Cloud);
        assert_eq!(context.workspace.as_deref(), Some("acme-prod"));
    }

    #[test]
    fn context_selection_prefers_flag_then_env_then_current() {
        // --context wins over both.
        let resolved = resolve_from(ResolveInputs {
            flag_context: Some("local".to_string()),
            env_context: Some("acme/prod".to_string()),
            file: Some(file()),
            ..ResolveInputs::default()
        })
        .unwrap();
        assert_eq!(resolved.context.unwrap().name, "local");

        // $SKARDI_CONTEXT wins over current-context.
        let resolved = resolve_from(ResolveInputs {
            env_context: Some("local".to_string()),
            file: Some(file()),
            ..ResolveInputs::default()
        })
        .unwrap();
        assert_eq!(resolved.context.unwrap().name, "local");
    }

    #[test]
    fn an_unknown_context_is_a_hard_error_listing_what_exists() {
        // The failure mode this guards: falling through to the built-in
        // default would send a cloud query to a local server.
        let err = resolve_from(ResolveInputs {
            flag_context: Some("typo".to_string()),
            file: Some(file()),
            ..ResolveInputs::default()
        })
        .unwrap_err();

        assert_eq!(
            err,
            ConfigError::UnknownContext {
                name: "typo".to_string(),
                available: vec!["acme/prod".to_string(), "local".to_string()],
            }
        );
        assert!(err.to_string().contains("no context named 'typo'"));
        assert!(err.to_string().contains("Available: acme/prod, local"));

        // A dangling current-context is the same typo, and gets the same
        // error rather than silently resolving to the default.
        let mut dangling = file();
        dangling.current_context = Some("removed".to_string());
        let err = resolve_from(inputs(Some(dangling))).unwrap_err();
        assert!(matches!(err, ConfigError::UnknownContext { .. }));
    }

    #[test]
    fn a_lone_context_needs_no_current_context_pointer_but_several_do() {
        let mut lone = file();
        lone.contexts.truncate(1);
        lone.current_context = None;
        assert_eq!(
            resolve_from(inputs(Some(lone)))
                .unwrap()
                .context
                .unwrap()
                .name,
            "acme/prod"
        );

        // Several with no pointer: nothing is selected, and flags/env/default
        // still apply — `--server` keeps working on an unpointed file.
        let mut unpointed = file();
        unpointed.current_context = None;
        let resolved = resolve_from(inputs(Some(unpointed))).unwrap();
        assert!(resolved.context.is_none());
        assert_eq!(resolved.server, DEFAULT_SERVER_URL);
    }

    // ── the cloud-authoritative rule (§5.1) ──────────────────────────────

    #[test]
    fn env_vars_are_refused_while_a_cloud_context_is_selected() {
        // The scenario: SKARDI_SERVER_URL left exported from the
        // single-server era would send a workspace-scoped PAT to whatever
        // listens there. Refused by name, and NO request is issued.
        for variable in [SERVER_URL_ENV, API_TOKEN_ENV] {
            let mut probe = inputs(Some(file()));
            if variable == SERVER_URL_ENV {
                probe.env_server = Some("http://evil:8080".to_string());
            } else {
                probe.env_token = Some("other-token".to_string());
            }
            assert_eq!(
                resolve_from(probe).unwrap_err(),
                ConfigError::EnvConflictsWithCloudContext {
                    name: "acme/prod".to_string(),
                    variable: variable.to_string(),
                }
            );
        }
    }

    #[test]
    fn a_flag_defuses_the_env_conflict_for_its_own_field_only() {
        // The error told the operator to "pass --server/--token to override",
        // and the check ran before flags were considered — so following that
        // advice produced the same error. Per field, and only where the env
        // would actually win.
        let resolved = resolve_from(ResolveInputs {
            flag_server: Some("https://explicit".to_string()),
            env_server: Some("http://stale:1".to_string()),
            file: Some(file()),
            ..ResolveInputs::default()
        })
        .unwrap();
        assert_eq!(resolved.server, "https://explicit");

        // The OTHER field is still refused: --server says nothing about
        // $SKARDI_API_TOKEN.
        assert_eq!(
            resolve_from(ResolveInputs {
                flag_server: Some("https://explicit".to_string()),
                env_token: Some("stale-token".to_string()),
                file: Some(file()),
                ..ResolveInputs::default()
            })
            .unwrap_err(),
            ConfigError::EnvConflictsWithCloudContext {
                name: "acme/prod".to_string(),
                variable: API_TOKEN_ENV.to_string(),
            }
        );
    }

    #[test]
    fn a_parse_complaint_without_a_position_still_says_something_useful() {
        // Not every serde error carries a location — a `custom` one (which
        // `Deserialize` impls raise) has none — and the fallback must not
        // render an empty or misleading position.
        let err = <serde_yaml::Error as serde::de::Error>::custom("no location here");
        assert_eq!(describe_parse_error(&err), "invalid YAML");
    }

    #[test]
    fn a_parse_complaint_names_a_position_never_the_offending_value() {
        // The read-side warning fires on EVERY command while the file stays
        // broken, and serde_yaml's Display quotes the offending scalar — so a
        // token on the broken line was printed to stderr repeatedly.
        let err =
            serde_yaml::from_str::<ContextsFile>("kind: client\ncontexts: skardi_pat_leakme123\n")
                .expect_err("this does not parse");
        let complaint = describe_parse_error(&err);
        assert!(
            !complaint.contains("skardi_pat_leakme123"),
            "the value leaked: {complaint}"
        );
        assert!(complaint.contains("line 2"), "{complaint}");

        // And through the loader, which is what commands actually hit.
        let handle = write("kind: client\ncontexts: skardi_pat_leakme123\n");
        let err = load_for_mutation(handle.path()).unwrap_err();
        assert!(
            !err.to_string().contains("skardi_pat_leakme123"),
            "the value leaked: {err}"
        );
    }

    #[test]
    fn flags_still_override_a_cloud_context_because_they_are_deliberate() {
        let resolved = resolve_from(ResolveInputs {
            flag_server: Some("https://staging.gw".to_string()),
            flag_token: Some("flag-token".to_string()),
            file: Some(file()),
            ..ResolveInputs::default()
        })
        .unwrap();

        assert_eq!(resolved.server, "https://staging.gw");
        assert_eq!(resolved.token.as_deref(), Some("flag-token"));
        // Still the cloud context: the workspace it names is unaffected.
        assert_eq!(
            resolved.context.unwrap().workspace.as_deref(),
            Some("acme-prod")
        );
    }

    #[test]
    fn env_vars_still_beat_a_server_mode_context() {
        let resolved = resolve_from(ResolveInputs {
            env_server: Some("http://env:9000".to_string()),
            env_context: Some("local".to_string()),
            file: Some(file()),
            ..ResolveInputs::default()
        })
        .unwrap();

        assert_eq!(resolved.server, "http://env:9000");
        assert_eq!(resolved.context.unwrap().mode, ContextMode::Server);
    }

    #[test]
    fn a_cloud_context_without_a_server_is_refused_never_defaulted() {
        // The regression this exists for: `server: None` fell through to the
        // built-in default while `token` stayed the workspace PAT, so
        // `skardi query` really did POST a cloud credential to
        // http://127.0.0.1:8080. Reproduced against the real binary in review.
        let mut serverless = file();
        serverless.contexts[0].server = None;
        let err = resolve_from(inputs(Some(serverless.clone()))).unwrap_err();
        assert_eq!(
            err,
            ConfigError::CloudContextWithoutServer {
                name: "acme/prod".to_string()
            }
        );
        assert!(err.to_string().contains(DEFAULT_SERVER_URL), "{err}");

        // Whitespace counts as unset here as everywhere else.
        let mut blank = file();
        blank.contexts[0].server = Some("  ".to_string());
        assert!(matches!(
            resolve_from(inputs(Some(blank))).unwrap_err(),
            ConfigError::CloudContextWithoutServer { .. }
        ));

        // --server satisfies it: a flag is a deliberate act at the point of
        // use, and the resolved server is the flag's.
        let resolved = resolve_from(ResolveInputs {
            flag_server: Some("https://gw.example".to_string()),
            file: Some(serverless),
            ..ResolveInputs::default()
        })
        .unwrap();
        assert_eq!(resolved.server, "https://gw.example");

        // A SERVER-mode context with no server still defaults, as it always
        // has — the default is only catastrophic for a cloud credential.
        let mut local_only = file();
        local_only.contexts[1].server = None;
        local_only.current_context = Some("local".to_string());
        let resolved = resolve_from(inputs(Some(local_only))).unwrap();
        assert_eq!(resolved.server, DEFAULT_SERVER_URL);
        assert_eq!(resolved.token, None);
    }

    #[test]
    fn a_cloud_context_without_a_workspace_is_refused() {
        let mut broken = file();
        broken.contexts[0].workspace = None;
        let err = resolve_from(inputs(Some(broken))).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::CloudContextWithoutWorkspace { .. }
        ));
        // The message names the repair, not just the problem.
        assert!(err.to_string().contains("skardi config set-context"));

        // Whitespace counts as unset here too.
        let mut blank = file();
        blank.contexts[0].workspace = Some("   ".to_string());
        assert!(resolve_from(inputs(Some(blank))).is_err());
    }

    #[test]
    fn a_key_that_differs_only_in_case_is_preserved_but_not_read() {
        // `Mode: cloud` lands in `extra`, `mode` takes its Server default, and
        // the cloud-authoritative rule stops applying — so $SKARDI_SERVER_URL
        // can redirect a workspace PAT again, one capital letter from correct.
        // Preservation stays (forward compat); the typo becomes visible.
        let handle = write(
            "kind: client\ncontexts:\n\
             \x20 - name: c\n    Mode: cloud\n    server: https://gw\n    token: t\n",
        );
        let loaded = load(handle.path()).expect("parses");
        assert!(loaded.contexts[0].extra.contains_key("Mode"));
        assert_eq!(loaded.contexts[0].mode, ContextMode::Server);
    }

    #[test]
    fn an_unreadable_file_is_not_mistaken_for_an_absent_one() {
        // A root-owned or wrong-uid config used to fall through in total
        // silence, so the operator talked to :8080 believing it was their
        // gateway. Still non-fatal per §5.4, but no longer invisible. A
        // directory stands in for any unreadable path, no privileges needed.
        let dir = TempDir::new().unwrap();
        assert_eq!(load(dir.path()), None);
        assert_eq!(load(&dir.path().join("absent.yaml")), None);
    }

    #[test]
    fn selection_is_shared_by_resolution_and_by_the_commands_that_report_it() {
        let contexts = file().effective_contexts();
        // The lone-context step, which the reporting commands used to miss.
        assert_eq!(
            select_context(&contexts[..1], None, None, None)
                .unwrap()
                .map(|c| c.name),
            Some("acme/prod".to_string())
        );
        // A padded name resolves, where a raw-field comparison did not.
        assert_eq!(
            select_context(&contexts, Some("  local  "), None, None)
                .unwrap()
                .map(|c| c.name),
            Some("local".to_string())
        );
        // The env var is honoured, and the flag outranks it.
        assert_eq!(
            select_context(&contexts, None, None, Some("local"))
                .unwrap()
                .map(|c| c.name),
            Some("local".to_string())
        );
        assert_eq!(
            select_context(&contexts, None, Some("acme/prod"), Some("local"))
                .unwrap()
                .map(|c| c.name),
            Some("acme/prod".to_string())
        );
        // Several with no pointer selects nothing.
        assert!(
            select_context(&contexts, None, None, None)
                .unwrap()
                .is_none()
        );
        // An unknown name is the same hard error resolution raises.
        assert!(matches!(
            select_context(&contexts, None, Some("typo"), None).unwrap_err(),
            ConfigError::UnknownContext { .. }
        ));
    }

    // ── per-field precedence, unchanged for server mode ──────────────────

    #[test]
    fn nothing_set_uses_the_default_server_and_no_token() {
        let resolved = resolve_from(inputs(None)).unwrap();
        assert_eq!(resolved.server, DEFAULT_SERVER_URL);
        assert_eq!(resolved.token, None);
        assert!(resolved.context.is_none());
    }

    #[test]
    fn empty_and_padded_values_are_unset_and_trimmed() {
        let resolved = resolve_from(ResolveInputs {
            flag_server: Some("  ".to_string()),
            env_server: Some(String::new()),
            env_token: Some("\tenv-token\n".to_string()),
            env_context: Some("   ".to_string()),
            file: Some(file()),
            ..ResolveInputs::default()
        })
        .unwrap_err();
        // The empty --context/env-context fell through to current-context,
        // which is cloud — so the non-empty env token is the conflict.
        assert!(matches!(
            resolved,
            ConfigError::EnvConflictsWithCloudContext { .. }
        ));

        let resolved = resolve_from(ResolveInputs {
            env_context: Some("local".to_string()),
            env_server: Some(" http://env:9000 ".to_string()),
            file: Some(file()),
            ..ResolveInputs::default()
        })
        .unwrap();
        assert_eq!(resolved.server, "http://env:9000");
    }

    // ── back-compat (§5.2) ───────────────────────────────────────────────

    #[test]
    fn a_legacy_spec_file_resolves_as_one_context_named_default() {
        let legacy = ContextsFile {
            spec: Some(LegacySpec {
                server: Some("http://legacy:8080".to_string()),
                token: Some("legacy-token".to_string()),
                ..LegacySpec::default()
            }),
            ..ContextsFile::default()
        };

        let resolved = resolve_from(inputs(Some(legacy))).unwrap();
        assert_eq!(resolved.server, "http://legacy:8080");
        assert_eq!(resolved.token.as_deref(), Some("legacy-token"));
        let context = resolved.context.unwrap();
        assert_eq!(context.name, LEGACY_CONTEXT_NAME);
        assert_eq!(context.mode, ContextMode::Server);
    }

    #[test]
    fn contexts_win_over_a_legacy_spec_present_in_the_same_file() {
        let mut both = file();
        both.spec = Some(LegacySpec {
            server: Some("http://legacy:8080".to_string()),
            token: None,
            ..LegacySpec::default()
        });
        let resolved = resolve_from(inputs(Some(both))).unwrap();
        assert_eq!(resolved.server, "https://gw.skardi.ai");
    }

    // ── the file surface ─────────────────────────────────────────────────

    #[test]
    fn load_parses_the_documented_file_shape_including_unknown_keys() {
        let handle = write(
            "kind: client\n\
             control-plane: https://api.skardi.ai\n\
             current-context: acme/prod\n\
             future-top-level: kept\n\
             contexts:\n\
             \x20 - name: acme/prod\n\
             \x20   server: https://gw.skardi.ai\n\
             \x20   mode: cloud\n\
             \x20   workspace: acme-prod\n\
             \x20   user: xin@skardi.ai\n\
             \x20   token: skardi_pat_x\n\
             \x20   token-id: 4f1c\n\
             \x20   token-expires-at: 2026-11-18T00:00:00Z\n\
             \x20   future-per-context: kept\n",
        );

        let loaded = load(handle.path()).expect("parses");
        assert_eq!(
            loaded.control_plane.as_deref(),
            Some("https://api.skardi.ai")
        );
        let context = &loaded.contexts[0];
        assert_eq!(context.mode, ContextMode::Cloud);
        assert_eq!(context.token_id.as_deref(), Some("4f1c"));
        assert_eq!(
            context.token_expires_at.as_deref(),
            Some("2026-11-18T00:00:00Z")
        );
        // Unknown keys survive a parse → serialize round trip, so an older
        // CLI's `use-context` cannot downgrade a newer CLI's file.
        assert!(loaded.extra.contains_key("future-top-level"));
        assert!(context.extra.contains_key("future-per-context"));
        let round_tripped = serde_yaml::to_string(&loaded).unwrap();
        assert!(round_tripped.contains("future-top-level"));
        assert!(round_tripped.contains("future-per-context"));
    }

    #[test]
    fn reads_tolerate_a_broken_file_but_mutations_refuse_it() {
        let handle = write("contexts: [this is: not, valid yaml\n");

        // Read: warn, ignore, fall through (§5.4) — `--server` still works.
        assert_eq!(load(handle.path()), None);

        // Mutation: refuse, naming the file and the parse error. Rewriting
        // from an empty tree would replace a credential-bearing file.
        let err = load_for_mutation(handle.path()).unwrap_err();
        assert!(
            matches!(&err, ConfigError::UnparsableForMutation { path, .. } if path == handle.path()),
            "{err:?}"
        );
        assert!(err.to_string().contains("refusing to modify"));
    }

    #[test]
    fn load_for_mutation_on_a_missing_file_starts_a_fresh_tree() {
        let dir = TempDir::new().unwrap();
        let absent = dir.path().join("config.yaml");
        let fresh = load_for_mutation(&absent).unwrap();
        assert_eq!(fresh.kind.as_deref(), Some("client"));
        assert!(fresh.contexts.is_empty());
    }

    #[test]
    fn save_writes_atomically_owner_only_and_leaves_no_temp_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nested").join("config.yaml");

        save(&path, &file()).unwrap();

        let reloaded = load(&path).expect("round trips");
        assert_eq!(reloaded.contexts.len(), 2);
        assert_eq!(reloaded.current_context.as_deref(), Some("acme/prod"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "a file holding a PAT must be owner-only");
        }

        // The temp file is renamed, never left behind.
        let leftovers: Vec<_> = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|name| name.contains(".tmp-"))
            .collect();
        assert!(leftovers.is_empty(), "left {leftovers:?}");
    }

    #[test]
    fn save_sweeps_stale_temp_files_and_never_inherits_their_permissions() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.yaml");

        // A crash between create and rename leaves one of these behind, and it
        // holds a full copy of every token. Nothing else ever removed them.
        let stale = dir.path().join(".config.yaml.tmp-99999-1");
        std::fs::write(&stale, "token: skardi_pat_from_a_crashed_run\n").unwrap();
        // Backdated past the staleness threshold: the sweep deliberately
        // leaves recent temp files alone, because one may belong to a
        // concurrent writer between its create and its rename.
        let old = std::time::SystemTime::now() - std::time::Duration::from_secs(3600);
        std::fs::File::options()
            .write(true)
            .open(&stale)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(old))
            .unwrap();
        // World-readable, to prove the new file does not inherit the mode: the
        // old `create`+`truncate` path kept an existing file's bits and then
        // renamed it over the config.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&stale, std::fs::Permissions::from_mode(0o644)).unwrap();
        }

        // A DIRECTORY whose name matches the temp shape must be left alone:
        // the sweep deletes files, and a blind remove would either fail the
        // save or destroy something it does not own.
        let decoy_dir = dir.path().join(".config.yaml.tmp-dir");
        std::fs::create_dir(&decoy_dir).unwrap();
        // So must an unrelated dotfile that merely shares the prefix's start.
        let unrelated = dir.path().join(".config.yaml.bak");
        std::fs::write(&unrelated, b"keep me").unwrap();

        save(&path, &file()).unwrap();

        assert!(!stale.exists(), "the leftover token copy must be removed");
        assert!(decoy_dir.is_dir(), "a matching DIRECTORY must survive");
        assert!(unrelated.is_file(), "a non-temp sibling must survive");

        // A FRESH temp file is left alone — it may be a live writer's, and
        // deleting it would make that process fail its rename with a message
        // pointing at the wrong thing entirely.
        let live = dir.path().join(".config.yaml.tmp-424242-7");
        std::fs::write(&live, b"in flight").unwrap();
        save(&path, &file()).unwrap();
        assert!(live.is_file(), "a recent temp file must not be swept");
        std::fs::remove_file(&live).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
        // Two saves in one process must not collide on the temp name.
        save(&path, &file()).unwrap();
        let leftovers = leftover_temp_files(dir.path());
        assert!(leftovers.is_empty(), "left {leftovers:?}");
    }

    /// Collect leftover temp FILES (a directory that happens to match the
    /// name is not a leftover — see the decoy in the sweep test).
    fn leftover_temp_files(dir: &Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_ok_and(|t| t.is_file()))
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.contains(".tmp-"))
            .collect()
    }

    #[test]
    fn save_over_an_existing_file_keeps_it_owner_only() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(&path, "kind: client\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        }

        save(&path, &file()).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "a loose file is rewritten tighter");
        }
    }

    #[test]
    fn an_unknown_context_on_a_file_with_none_says_so_instead_of_listing_nothing() {
        // The empty-`available` arm: "Available: " with a bare comma-join
        // would read as a truncated message.
        let err = resolve_from(ResolveInputs {
            flag_context: Some("anything".to_string()),
            file: Some(ContextsFile::default()),
            ..ResolveInputs::default()
        })
        .unwrap_err();
        assert!(
            err.to_string().contains("the config defines no contexts"),
            "{err}"
        );
    }

    #[test]
    fn load_for_mutation_refuses_a_path_it_cannot_read_at_all() {
        // Present but unreadable is NOT the same as absent: only absence may
        // yield a fresh tree. A directory stands in for any unreadable path
        // without depending on the test user's privileges.
        let dir = TempDir::new().unwrap();
        let err = load_for_mutation(dir.path()).unwrap_err();
        // Reported as UNREADABLE, not unparsable: "does not parse (Is a
        // directory)" sent the operator hunting for a YAML error.
        assert!(
            matches!(&err, ConfigError::UnreadableForMutation { path, .. } if path == dir.path()),
            "{err:?}"
        );
        assert!(err.to_string().contains("cannot be read"), "{err}");
        assert!(!err.to_string().contains("does not parse"), "{err}");
    }

    #[test]
    fn save_reports_a_failed_rename_and_cleans_up_its_temp_file() {
        // Renaming a file over a non-empty DIRECTORY fails, which is the one
        // portable way to reach the cleanup branch. The temp file must not be
        // left behind for the next run to trip over.
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("config.yaml");
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("occupant"), b"x").unwrap();

        let err = save(&target, &file()).unwrap_err();
        assert!(
            format!("{err:#}").contains("atomically"),
            "the failure names what it was doing: {err:#}"
        );
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|name| name.contains(".tmp-"))
            .collect();
        assert!(leftovers.is_empty(), "temp file left behind: {leftovers:?}");
    }

    #[test]
    fn context_mode_renders_padded_so_listings_line_up() {
        assert_eq!(ContextMode::Server.as_str(), "server");
        assert_eq!(ContextMode::Cloud.as_str(), "cloud");
        // The column `get-contexts` prints. A Display that writes directly
        // would ignore the width and return "cloud" here.
        assert_eq!(format!("[{:<8}]", ContextMode::Cloud), "[cloud   ]");
        assert_eq!(format!("{}", ContextMode::Server), "server");
    }
}
