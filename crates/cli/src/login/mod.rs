//! `skardi login`: turn a browser sign-in (or a dev bearer) into one
//! workspace-scoped PAT per workspace, verified against the gateway before
//! anything is written (§6).
//!
//! The flow is a SAGA, not a loop (§6.5). Each mint commits independently at
//! the control plane, so a failure partway through `--all-workspaces` would
//! otherwise leave live PATs whose ids die with the process — credentials
//! nobody knows exist. Every `{token_id, workspace}` is retained in memory,
//! contexts are written only after all selected mints succeed, and any failure
//! (including the config write) revokes everything this run created before
//! reporting the original cause.
//!
//! Everything that talks to a network is reachable through a parameter — the
//! control-plane URL, both OAuth endpoints, the identity acquirer — so the
//! whole flow runs in tests against `wiremock` with no browser and no Google.

#[cfg(test)]
mod tests;

pub mod control_plane;
pub mod loopback;
pub mod oauth;
pub mod pkce;

use crate::client::ApiClient;
use crate::config::{self, Context, ContextMode, ContextsFile, SelectedContext};
use anyhow::{Context as _, Result, anyhow, bail};
use chrono::{DateTime, Duration, Utc};
use control_plane::{ControlPlane, CpError, Membership, Minted};
use std::path::Path;

/// Per-request ceiling on control-plane and token-endpoint calls.
///
/// Bounded because of the ROLLBACK, not the happy path: a revoke that hangs
/// forever leaves the operator watching a silent terminal while a live
/// credential goes unreported, which is precisely the outcome §6.5 exists to
/// prevent. Generous enough that a cold control plane still answers.
const CONTROL_PLANE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Default PAT lifetime (§6.1 step 5), as `--expires` spells it. A string so
/// clap's default and this documented value are the same token, parsed by the
/// same function a user-supplied one goes through.
pub const DEFAULT_EXPIRES: &str = "90d";

/// Which memberships to mint for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selection {
    /// A lone membership is used automatically; several prompt (§6.1 step 5).
    Auto,
    /// `--workspace SLUG`.
    Named(String),
    /// `--all-workspaces`.
    All,
}

/// Everything `login` needs, with every network endpoint injectable.
pub struct LoginOptions {
    pub control_plane: String,
    /// The OAuth client id. Required for the browser path, unused with
    /// `--identity`.
    pub client_id: Option<String>,
    /// `dev:<external-id>[:<email>]` (§6.3), which skips the browser.
    pub identity: Option<String>,
    /// `--i-know-this-is-dev-auth`, the only way `--identity` is allowed
    /// against a non-loopback control plane.
    pub allow_dev_auth_off_loopback: bool,
    pub selection: Selection,
    /// `--context NAME`, overriding the `<org>/<workspace>` default. Only
    /// legal for a single-workspace login: several contexts cannot share a
    /// name.
    pub context_name: Option<String>,
    pub expires: Duration,
    pub no_browser: bool,
    pub no_verify: bool,
    pub keep_old_token: bool,
    /// `--server`, the top of §6.2's precedence.
    pub server_override: Option<String>,
    /// `$SKARDI_GATEWAY_URL`, the second step.
    pub env_gateway_url: Option<String>,
    pub endpoints: oauth::Endpoints,
    pub callback_timeout: std::time::Duration,
    /// The PAT's name at the control plane, `cli@<hostname>`.
    pub token_name: String,
    /// Injected so the expiry a test asserts is not the wall clock.
    pub now: DateTime<Utc>,
}

/// One context this run wrote.
#[derive(Debug)]
pub struct WrittenContext {
    pub name: String,
    pub server: String,
    pub workspace: String,
    pub role: String,
    pub expires_at: Option<String>,
}

/// What happened, for the caller to print.
///
/// `Debug`-able because it carries NO token values — only names, ids, and
/// expiry stamps — so a report can be printed by a test or a future `--json`
/// without a redaction step.
#[derive(Debug, Default)]
pub struct LoginReport {
    pub written: Vec<WrittenContext>,
    /// `(context name, provisioning_state)` for every non-`active` workspace.
    pub skipped: Vec<(String, String)>,
    pub current_context: Option<String>,
    /// Token ids of replaced credentials that were revoked.
    pub replaced_revoked: Vec<String>,
    /// Token ids retained because of `--keep-old-token`.
    pub replaced_kept: Vec<String>,
    /// `(token id, reason)` for a replacement revocation that failed. Reported
    /// but not fatal: the new context is already good (§6.5).
    pub revoke_failures: Vec<(String, String)>,
}

/// A PAT this run created, retained so the saga can revoke it.
struct MintedRef {
    workspace: String,
    token_id: String,
}

/// Run the whole flow. `config_path` is a parameter so tests write to a temp
/// directory instead of the developer's own config.
pub async fn login(options: LoginOptions, config_path: &Path) -> Result<LoginReport> {
    let http = reqwest::Client::builder()
        .no_proxy()
        .timeout(CONTROL_PLANE_TIMEOUT)
        .build()?;
    let bearer = acquire_bearer(&http, &options).await?;
    let cp = ControlPlane::new(http.clone(), &options.control_plane, bearer);

    let memberships = cp.memberships().await.map_err(describe_cp_failure)?;
    let mut report = LoginReport::default();
    let mut active = Vec::new();
    for membership in memberships {
        if membership.is_active() {
            active.push(membership);
        } else {
            report
                .skipped
                .push((membership.context_name(), membership.provisioning_state));
        }
    }

    let selected = select_memberships(&options.selection, active, &report.skipped)?;
    if options.context_name.is_some() && selected.len() > 1 {
        bail!(
            "--context names ONE context, but {} workspaces were selected — drop it, or select a single workspace with --workspace",
            selected.len()
        );
    }

    // §6.5: mint, then verify, retaining every id. Any failure past the first
    // mint rolls the whole run back before reporting.
    let expires_at = (options.now + options.expires).to_rfc3339();
    let mut minted: Vec<MintedRef> = Vec::new();
    let mut pending: Vec<(Membership, Minted, String)> = Vec::new();
    for membership in selected {
        let server = match resolve_server(&options, &membership) {
            Ok(server) => server,
            Err(err) => return Err(rollback(&cp, minted, err).await),
        };
        let token = match cp
            .mint(
                &options.token_name,
                &membership.tenant_slug,
                &membership.role,
                &expires_at,
            )
            .await
        {
            Ok(token) => token,
            Err(err) => {
                let err = describe_cp_failure(err).context(format!(
                    "minting a credential for workspace '{}' failed",
                    membership.tenant_slug
                ));
                return Err(rollback(&cp, minted, err).await);
            }
        };
        minted.push(MintedRef {
            workspace: membership.tenant_slug.clone(),
            token_id: token.token_id.clone(),
        });

        if !options.no_verify
            && let Err(err) = verify(&server, &membership, &token).await
        {
            return Err(rollback(&cp, minted, err).await);
        }
        pending.push((membership, token, server));
    }

    match write_contexts(config_path, &options, &pending) {
        Ok(replaced) => {
            report.written = pending
                .iter()
                .map(|(membership, token, server)| WrittenContext {
                    name: context_name(&options, membership),
                    server: server.clone(),
                    workspace: membership.tenant_slug.clone(),
                    role: membership.role.clone(),
                    expires_at: token.expires_at.clone(),
                })
                .collect();
            report.current_context = report.written.first().map(|w| w.name.clone());
            revoke_replaced(&cp, replaced, &options, &mut report).await;
            Ok(report)
        }
        Err(err) => Err(rollback(&cp, minted, err).await),
    }
}

/// Step 1-3: the credential presented to the control plane.
async fn acquire_bearer(http: &reqwest::Client, options: &LoginOptions) -> Result<String> {
    if let Some(identity) = &options.identity {
        check_dev_identity(
            identity,
            &options.control_plane,
            options.allow_dev_auth_off_loopback,
        )?;
        return Ok(identity.clone());
    }

    let client_id = options
        .client_id
        .as_deref()
        .filter(|id| !id.trim().is_empty());
    let Some(client_id) = client_id else {
        bail!(
            "no OAuth client id: pass --client-id, set $SKARDI_OAUTH_CLIENT_ID, or use --identity dev:<id> against a loopback control plane"
        )
    };
    oauth::acquire_id_token(
        http,
        &options.endpoints,
        client_id,
        options.no_browser,
        options.callback_timeout,
        &oauth::open_in_browser,
    )
    .await
}

/// §6.3's guard on the dev-auth path, shared by `login` and
/// `logout --revoke` so neither can drift into accepting more than the other.
///
/// Loopback ONLY, deliberately not loopback-or-private: a `dev:<external-id>`
/// bearer is unauthenticated impersonation of any user against a control plane
/// with `SKARDI_GLOBAL_DEV_AUTH=1`, and RFC1918 is exactly where a shared
/// internal staging cluster lives. It is also the simpler predicate, with no
/// CIDR table to get wrong.
pub fn check_dev_identity(
    identity: &str,
    control_plane: &str,
    allow_off_loopback: bool,
) -> Result<()> {
    if !identity.starts_with("dev:") {
        bail!("--identity must be a 'dev:<external-id>[:<email>]' bearer, not '{identity}'");
    }
    if !is_loopback_url(control_plane)? && !allow_off_loopback {
        bail!(
            "--identity is a dev-auth bearer and {control_plane} is not a loopback address — pass --i-know-this-is-dev-auth to use it against a remote control plane"
        );
    }
    // Printed on every run, naming the control plane it authenticated against:
    // the decision to trust an unverified claim should be visible in the
    // terminal that made it.
    eprintln!(
        "warning: authenticating with a dev-auth identity against {control_plane} — this presents an unverified identity claim, not a signed sign-in"
    );
    Ok(())
}

/// Whether `url`'s host is a loopback address. `localhost` counts: it is the
/// NAME for loopback, and it is what a compose stack and a dev README use.
fn is_loopback_url(url: &str) -> Result<bool> {
    let parsed = reqwest::Url::parse(url)
        .with_context(|| format!("'{url}' is not a valid control-plane URL"))?;
    let host = parsed.host_str().unwrap_or_default();
    // `host_str` brackets an IPv6 literal, which `IpAddr` will not parse.
    let host = host.trim_start_matches('[').trim_end_matches(']');
    Ok(host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|addr| addr.is_loopback()))
}

/// §6.1 step 5's selection rule.
fn select_memberships(
    selection: &Selection,
    active: Vec<Membership>,
    skipped: &[(String, String)],
) -> Result<Vec<Membership>> {
    if active.is_empty() {
        // The skip list is the explanation when it is the reason.
        if skipped.is_empty() {
            bail!("this identity has no workspace memberships at the control plane");
        }
        let states = skipped
            .iter()
            .map(|(name, state)| format!("{name} ({state})"))
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "no active workspace to log in to — every membership is still provisioning: {states}"
        );
    }

    match selection {
        Selection::All => Ok(active),
        Selection::Named(slug) => {
            let matched: Vec<Membership> = active
                .iter()
                .filter(|m| &m.tenant_slug == slug)
                .cloned()
                .collect();
            if matched.is_empty() {
                bail!(
                    "no active workspace named '{slug}' — available: {}",
                    active
                        .iter()
                        .map(|m| m.tenant_slug.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
            Ok(matched)
        }
        Selection::Auto if active.len() == 1 => Ok(active),
        Selection::Auto => {
            let chosen = prompt_for_workspace(&active)?;
            Ok(vec![chosen])
        }
    }
}

/// Ask which workspace to use. On EOF — a pipe, a CI job, a `< /dev/null` —
/// this is a non-interactive run, so it names the flags that do the same job
/// rather than looping on an input that will never come.
fn prompt_for_workspace(active: &[Membership]) -> Result<Membership> {
    use std::io::Write as _;

    eprintln!("this identity has {} active workspaces:", active.len());
    for (index, membership) in active.iter().enumerate() {
        let display = membership
            .display_name
            .as_deref()
            .filter(|name| !name.is_empty())
            .map(|name| format!(" — {name}"))
            .unwrap_or_default();
        eprintln!(
            "  {}) {}{display} (role: {})",
            index + 1,
            membership.context_name(),
            membership.role
        );
    }
    eprint!("select one [1-{}]: ", active.len());
    let _ = std::io::stderr().flush();

    let mut line = String::new();
    let read = std::io::stdin()
        .read_line(&mut line)
        .context("read the workspace selection")?;
    if read == 0 {
        bail!(
            "several workspaces are available and there is no terminal to ask — pass --workspace <slug> or --all-workspaces"
        );
    }
    let choice: usize = line
        .trim()
        .parse()
        .ok()
        .filter(|n| (1..=active.len()).contains(n))
        .ok_or_else(|| anyhow!("'{}' is not one of 1-{}", line.trim(), active.len()))?;
    Ok(active[choice - 1].clone())
}

/// §6.2: `--server` > `$SKARDI_GATEWAY_URL` > the membership's `gateway_url` >
/// hard error. Deliberately no built-in default and no fall-through to
/// `http://127.0.0.1:8080` — writing a context that points at a local port
/// would fail later and further from the cause.
fn resolve_server(options: &LoginOptions, membership: &Membership) -> Result<String> {
    let candidate = options
        .server_override
        .as_deref()
        .or(options.env_gateway_url.as_deref())
        .or(membership.gateway_url.as_deref())
        .map(str::trim)
        .filter(|url| !url.is_empty());
    let Some(server) = candidate else {
        bail!(
            "the control plane did not say which gateway serves workspace '{}' — pass --server <URL>, set $SKARDI_GATEWAY_URL, or configure gateway_url for the org",
            membership.tenant_slug
        )
    };
    Ok(server.to_string())
}

/// §6.1 step 6: one authenticated probe at the resolved gateway before the
/// context is written. `select 1` is the cheapest governed round trip, and it
/// travels the exact path a real query will — same client, same
/// `Skardi-Workspace` header — because a login that reports success while its
/// first query would 403 is the failure this step exists to remove.
async fn verify(server: &str, membership: &Membership, token: &Minted) -> Result<()> {
    let probe = config::ClientConfig {
        server: server.to_string(),
        token: Some(token.token.clone()),
        context: Some(SelectedContext {
            name: membership.context_name(),
            mode: ContextMode::Cloud,
            workspace: Some(membership.tenant_slug.clone()),
            token_expires_at: token.expires_at.clone(),
        }),
    };
    let client = ApiClient::new(&probe)?;
    client
        .post("/query", &serde_json::json!({"sql": "select 1"}))
        .await
        .map_err(|err| {
            anyhow!(
                "the new credential for workspace '{}' could not query {server}: {err} — the context was not written (pass --no-verify to skip this check)",
                membership.tenant_slug
            )
        })?;
    Ok(())
}

/// The context name for a membership: `--context` when given, else
/// `<org_slug>/<tenant_slug>` (§6.1 step 7).
fn context_name(options: &LoginOptions, membership: &Membership) -> String {
    options
        .context_name
        .clone()
        .unwrap_or_else(|| membership.context_name())
}

/// Write every verified context and point `current-context` at the first.
///
/// Returns the token ids of credentials this write REPLACED, for §6.5's
/// replacement revocation. The whole file is written once, so a partial set of
/// contexts cannot land.
fn write_contexts(
    path: &Path,
    options: &LoginOptions,
    pending: &[(Membership, Minted, String)],
) -> Result<Vec<String>> {
    let mut file: ContextsFile = config::load_for_mutation(path)?;
    file.promote_legacy_spec();
    let mut replaced = Vec::new();

    for (membership, token, server) in pending {
        let name = context_name(options, membership);
        let existing = file.contexts.iter().position(|c| c.name == name);
        if let Some(index) = existing {
            // Retained BEFORE the overwrite: after it, the id is gone.
            if let Some(token_id) = file.contexts[index].token_id.clone() {
                replaced.push(token_id);
            }
        }
        let index = existing.unwrap_or_else(|| {
            file.contexts.push(Context {
                name: name.clone(),
                ..Context::default()
            });
            file.contexts.len() - 1
        });
        let context = &mut file.contexts[index];
        context.server = Some(server.clone());
        context.mode = ContextMode::Cloud;
        context.workspace = Some(membership.tenant_slug.clone());
        context.token = Some(token.token.clone());
        context.token_id = Some(token.token_id.clone());
        context.token_expires_at = token.expires_at.clone();
    }

    if let Some((membership, _, _)) = pending.first() {
        file.current_context = Some(context_name(options, membership));
    }
    // Recorded so a later `login` with no --control-plane reaches the same
    // control plane this one did.
    file.control_plane = Some(options.control_plane.clone());
    config::save(path, &file)?;
    Ok(replaced)
}

/// §6.5's rollback: revoke everything this run minted, then report the ORIGINAL
/// failure. A rollback that itself fails escalates — the ids it could not
/// revoke are printed, because silence there would leave live credentials
/// nobody knows exist.
async fn rollback(
    cp: &ControlPlane,
    minted: Vec<MintedRef>,
    cause: anyhow::Error,
) -> anyhow::Error {
    if minted.is_empty() {
        return cause;
    }
    let mut stranded = Vec::new();
    for reference in minted.iter().rev() {
        if let Err(err) = cp.revoke(&reference.token_id).await {
            stranded.push(format!(
                "  {} (workspace '{}'): {err}",
                reference.token_id, reference.workspace
            ));
        }
    }
    if stranded.is_empty() {
        eprintln!(
            "rolled back {} credential(s) minted by this login",
            minted.len()
        );
        return cause;
    }
    cause.context(format!(
        "LOGIN ROLLBACK INCOMPLETE — these credentials are live and are not in any context. Revoke them in the console:\n{}",
        stranded.join("\n")
    ))
}

/// §6.5's replacement rule: a re-login over an existing context revokes the
/// token it replaced, unless `--keep-old-token` retains it for an agent that
/// is mid-task. A failure here is reported, not fatal — the new context is
/// already good.
async fn revoke_replaced(
    cp: &ControlPlane,
    replaced: Vec<String>,
    options: &LoginOptions,
    report: &mut LoginReport,
) {
    for token_id in replaced {
        if options.keep_old_token {
            report.replaced_kept.push(token_id);
            continue;
        }
        match cp.revoke(&token_id).await {
            Ok(()) => report.replaced_revoked.push(token_id),
            Err(err) => report.revoke_failures.push((token_id, format!("{err}"))),
        }
    }
}

/// §6.4: `POST /v1/me/tokens` answers `400 org_ambiguous` for an identity in
/// more than one org, and v1 takes no org selector. Print the org list and the
/// escape hatch rather than the raw code.
fn describe_cp_failure(err: anyhow::Error) -> anyhow::Error {
    let Some(cp_error) = err.downcast_ref::<CpError>() else {
        return err;
    };
    if cp_error.code.as_deref() != Some("org_ambiguous") {
        return err;
    }
    let orgs = if cp_error.orgs.is_empty() {
        "(the control plane returned no org list)".to_string()
    } else {
        cp_error.orgs.join(", ")
    };
    anyhow!(
        "this identity belongs to more than one organization ({orgs}), and minting for multi-org identities is not available in v1. Mint a token in the console, then run: skardi config set-context <name> --mode cloud --server <URL> --workspace <SLUG> --token-stdin"
    )
}

/// The PAT's name at the control plane: `cli@<hostname>`, so a token list
/// says which machine holds it.
pub fn default_token_name() -> String {
    format!("cli@{}", hostname())
}

/// The machine's name, best effort. `$HOSTNAME` is not exported by every
/// shell, so `hostname(1)` is the fallback, and an unnamed host still logs in.
fn hostname() -> String {
    if let Ok(name) = std::env::var("HOSTNAME") {
        let name = name.trim();
        if !name.is_empty() {
            return name.to_string();
        }
    }
    std::process::Command::new("hostname")
        .output()
        .ok()
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "unknown-host".to_string())
}

/// Parse `--expires`: `90d`, `12h`, or a bare number of days.
///
/// Days and hours only. `m` would be read as minutes by one person and months
/// by the next, and a PAT lifetime is not a place for that ambiguity.
pub fn parse_expires(raw: &str) -> Result<Duration> {
    let trimmed = raw.trim();
    let (digits, unit) = match trimmed.strip_suffix(['d', 'D']) {
        Some(digits) => (digits, 'd'),
        None => match trimmed.strip_suffix(['h', 'H']) {
            Some(digits) => (digits, 'h'),
            None => (trimmed, 'd'),
        },
    };
    let amount: i64 = digits.trim().parse().map_err(|_| {
        anyhow!("--expires takes a number of days or hours, like '90d' or '12h', not '{raw}'")
    })?;
    if amount <= 0 {
        bail!("--expires must be positive, not '{raw}'");
    }
    let duration = match unit {
        'h' => Duration::try_hours(amount),
        _ => Duration::try_days(amount),
    };
    duration.ok_or_else(|| anyhow!("--expires '{raw}' is longer than any usable credential"))
}
