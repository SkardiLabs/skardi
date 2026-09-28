//! `skardi login` — the flag surface, the URL precedence around it, and the
//! summary it prints (§6.1, §6.2).
//!
//! The flow itself lives in [`crate::login`]; this module is the part that
//! reads flags and environment, so the flow stays a pure function of its
//! options and remains testable without a process.

use crate::config::{self, ContextsFile};
use crate::login::{self, LoginOptions, LoginReport, Selection, control_plane, oauth};
use anyhow::{Context as _, Result, bail};
use clap::Args;
use std::path::Path;

/// `--control-plane`'s environment step (§6.1 step 1).
const CONTROL_PLANE_ENV: &str = "SKARDI_CONTROL_PLANE_URL";

/// The hosted console, and the ONLY compiled-in URL in this binary.
///
/// It closes the chain for the brokered flow alone. The direct flow appends
/// bare `/v1/me/...` and needs skardi-global's own API address, which the
/// hosted deployment does not expose — global answers only behind the console
/// at `/api/global/v1/...` — so there is nothing correct to write here for it,
/// and it keeps failing by name.
///
/// The value is prod's `deploy/gitops/envs/prod/origin.yaml`, the one file in
/// skardi-cloud allowed to name a public host. Change it there first.
pub(super) const DEFAULT_CONSOLE: &str = "https://console.skardi.ai";
/// `--server`'s environment step for the gateway URL (§6.2).
const GATEWAY_URL_ENV: &str = "SKARDI_GATEWAY_URL";
/// The OAuth client id, so a deployment can pin it once per shell instead of
/// per command.
const CLIENT_ID_ENV: &str = "SKARDI_OAUTH_CLIENT_ID";
/// `--identity`'s environment step (§6.3).
const DEV_IDENTITY_ENV: &str = "SKARDI_DEV_IDENTITY";

#[derive(Args, Debug)]
pub struct LoginArgs {
    /// control-plane URL; overrides $SKARDI_CONTROL_PLANE_URL and
    /// `control-plane:` in ~/.skardi/config.yaml
    #[arg(long, value_name = "URL")]
    pub control_plane: Option<String>,

    /// log in to one workspace by slug (non-interactive). Needs --client-id or
    /// --identity: a browser-brokered login is approved for one workspace in
    /// the console, so the terminal does not choose
    #[arg(long, value_name = "SLUG", conflicts_with = "all_workspaces")]
    pub workspace: Option<String>,

    /// log in to every active workspace this identity belongs to. Needs
    /// --client-id or --identity, for the same reason as --workspace
    #[arg(long)]
    pub all_workspaces: bool,

    /// PAT lifetime, as days (`90d`, `90`) or hours (`12h`)
    #[arg(long, value_name = "DURATION", default_value = login::DEFAULT_EXPIRES)]
    pub expires: String,

    /// print the sign-in URL instead of opening a browser. On the default
    /// console-brokered path this is enough to log in from another machine
    #[arg(long)]
    pub no_browser: bool,

    /// OAuth client id, selecting the direct provider flow; overrides
    /// $SKARDI_OAUTH_CLIENT_ID. Omit it and the console brokers the sign-in,
    /// which needs no client id and works over SSH
    #[arg(long, value_name = "ID")]
    pub client_id: Option<String>,

    /// dev-auth bearer (`dev:<external-id>[:<email>]`), skipping the browser.
    /// Refused unless the control plane is loopback
    #[arg(long, value_name = "IDENTITY")]
    pub identity: Option<String>,

    /// allow --identity against a non-loopback control plane
    #[arg(long)]
    pub i_know_this_is_dev_auth: bool,

    /// skip the post-mint gateway probe (for an air-gapped mint)
    #[arg(long)]
    pub no_verify: bool,

    /// keep the credential a re-login replaces, instead of revoking it
    #[arg(long)]
    pub keep_old_token: bool,
}

#[derive(Args, Debug)]
pub struct LogoutArgs {
    /// every cloud context, rather than just the selected one
    #[arg(long)]
    pub all: bool,

    /// also revoke the PAT at the control plane. A PAT cannot revoke itself,
    /// so this re-authenticates first
    #[arg(long)]
    pub revoke: bool,

    /// control-plane URL for --revoke
    #[arg(long, value_name = "URL")]
    pub control_plane: Option<String>,

    /// OAuth client id for --revoke
    #[arg(long, value_name = "ID")]
    pub client_id: Option<String>,

    /// dev-auth bearer for --revoke (loopback control planes only)
    #[arg(long, value_name = "IDENTITY")]
    pub identity: Option<String>,

    /// allow --identity against a non-loopback control plane
    #[arg(long)]
    pub i_know_this_is_dev_auth: bool,
}

/// Run `skardi login`, reading the ambient environment for the steps flags do
/// not cover.
pub async fn run(
    args: LoginArgs,
    flag_context: Option<String>,
    flag_server: Option<String>,
) -> Result<()> {
    let path = config::default_config_path()
        .context("cannot determine the home directory for ~/.skardi/config.yaml")?;
    let options = options_from(&args, flag_context, flag_server, &path)?;
    let report = login::login(options, &path).await?;
    print_report(&report);
    Ok(())
}

/// Assemble [`LoginOptions`] from flags, environment, and the config file.
///
/// Split out so the precedence is testable without running the flow: it is the
/// part §6.2 pins, and the part a stray exported variable changes.
fn options_from(
    args: &LoginArgs,
    flag_context: Option<String>,
    flag_server: Option<String>,
    path: &Path,
) -> Result<LoginOptions> {
    let file = config::load(path);
    let client_id = args
        .client_id
        .clone()
        .or_else(|| std::env::var(CLIENT_ID_ENV).ok());
    let identity = args
        .identity
        .clone()
        .or_else(|| std::env::var(DEV_IDENTITY_ENV).ok());
    // Which URL this run falls back to depends on which flow it will take, so
    // the credential flags are resolved FIRST. The predicate is `login`'s own,
    // shared rather than restated, so the key written by a brokered login is
    // always the key the next brokered login reads.
    let kind = if login::selects_console_broker(identity.as_deref(), client_id.as_deref()) {
        UrlKind::Console
    } else {
        UrlKind::ControlPlane
    };
    let resolved = resolve_control_plane(
        args.control_plane.clone(),
        std::env::var(CONTROL_PLANE_ENV).ok(),
        file.as_ref(),
        kind,
    )?;
    // Said BEFORE the browser opens, and only when nothing chose the URL.
    //
    // The risk a default introduces is not that it points at prod; it is that
    // someone on a dev cluster forgets to configure theirs and signs in to prod
    // without noticing. Configured URLs are the person's own choice and need no
    // announcement; the default is the one case where the CLI decided for
    // them, so it says so, names the three ways to override, and does it while
    // there is still time to Ctrl-C. On stderr, like the cleartext warning, so
    // a script capturing the report is not disturbed.
    if resolved.defaulted {
        eprintln!("{}", default_notice(&resolved.url));
    }
    let control_plane = resolved.url;
    let selection = match (&args.workspace, args.all_workspaces) {
        (Some(slug), _) => Selection::Named(slug.clone()),
        (None, true) => Selection::All,
        (None, false) => Selection::Auto,
    };
    Ok(LoginOptions {
        control_plane,
        client_id,
        identity,
        allow_dev_auth_off_loopback: args.i_know_this_is_dev_auth,
        selection,
        context_name: flag_context,
        expires: login::parse_expires(&args.expires)?,
        no_browser: args.no_browser,
        no_verify: args.no_verify,
        keep_old_token: args.keep_old_token,
        server_override: flag_server,
        env_gateway_url: std::env::var(GATEWAY_URL_ENV).ok(),
        endpoints: oauth::Endpoints::default(),
        open_browser: oauth::open_in_browser,
        callback_timeout: oauth::CALLBACK_TIMEOUT,
        poll_interval: login::console_broker::POLL_INTERVAL,
        verify_timeout: control_plane::CONTROL_PLANE_TIMEOUT,
        token_name: login::default_token_name(),
        now: chrono::Utc::now(),
    })
}

/// §6.1 step 1: `--control-plane` > `$SKARDI_CONTROL_PLANE_URL` > the flow's
/// key in the file > a built-in default for the brokered flow, or a hard error
/// for the direct one.
///
/// The design's chain always ended in a built-in default. This comment used to
/// explain why there was none — no hosted control plane existed, and inventing
/// a hostname that answered nothing would have failed at DNS with no mention
/// of the three real inputs — and promised that "a one-line constant replaces
/// it the day the hosted URL exists". That day is `console.skardi.ai`, and the
/// constant is [`DEFAULT_CONSOLE`].
///
/// Only the brokered flow gets it. The direct flow still ends the way §6.2's
/// does, a typed error naming the three inputs, because there is no hosted
/// address for what it calls (see the constant's doc).
/// Which recorded URL a flow should fall back to.
///
/// Both arrive through `--control-plane`, and they are NOT interchangeable: a
/// console serves the control-plane API under `/api/global/v1/...` and only to
/// a browser holding a session, while `ControlPlane` appends bare
/// `/v1/me/...`. Reading the wrong one sent `logout --revoke` at a console,
/// which answered with its 404 page after the local credential was already
/// cleared — so the fallback key is part of the flow's identity, not a detail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UrlKind {
    /// The console that brokers a browser login (`console:` in the file).
    Console,
    /// The control-plane API itself (`control-plane:` in the file), for the
    /// OAuth/dev login and for `logout --revoke`.
    ControlPlane,
}

impl UrlKind {
    /// The file key this kind falls back to, named in the error too so the
    /// message and the lookup cannot drift.
    const fn file_key(self) -> &'static str {
        match self {
            UrlKind::Console => "console:",
            UrlKind::ControlPlane => "control-plane:",
        }
    }
}

/// The line printed when the CLI chose the URL. A function, not an inline
/// format, so its wording is a tested value: it deliberately names the same
/// three inputs the no-URL error names, and a test that only grepped for those
/// strings could not tell the notice from the error it replaced.
pub(super) fn default_notice(url: &str) -> String {
    format!(
        "signing in through {url} (the built-in default; pass --control-plane <URL>, set \
         ${CONTROL_PLANE_ENV}, or add 'console:' to ~/.skardi/config.yaml to use another)"
    )
}

/// A resolved URL and whether the CLI chose it.
///
/// `defaulted` is carried rather than recomputed by the caller (`url ==
/// DEFAULT_CONSOLE`) because a person who WRITES the default into their file,
/// or passes it as a flag, has configured it — and the notice exists to flag
/// the one case where nobody did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Resolved {
    pub url: String,
    pub defaulted: bool,
}

fn resolve_control_plane(
    flag: Option<String>,
    env: Option<String>,
    file: Option<&ContextsFile>,
    kind: UrlKind,
) -> Result<Resolved> {
    let from_file = file.and_then(|f| match kind {
        UrlKind::Console => f.console.clone(),
        UrlKind::ControlPlane => f.control_plane.clone(),
    });
    let configured = [flag, env, from_file]
        .into_iter()
        .flatten()
        .map(|url| url.trim().to_string())
        .find(|url| !url.is_empty());
    let (url, defaulted) = match (configured, kind) {
        (Some(url), _) => (url, false),
        (None, UrlKind::Console) => (DEFAULT_CONSOLE.to_string(), true),
        (None, UrlKind::ControlPlane) => {
            let key = kind.file_key();
            bail!(
                "no control plane configured: pass --control-plane <URL>, set ${CONTROL_PLANE_ENV}, or add '{key}' to ~/.skardi/config.yaml"
            )
        }
    };
    // This is the leg carrying the most sensitive traffic in the flow — the ID
    // token goes up on every call and `POST /v1/me/tokens` returns the RAW PAT
    // — and it had no scheme check at all, while `ApiClient` warns for a mere
    // bearer. Said HERE, at resolution, so it lands before the browser opens
    // rather than after a completed sign-in, and covers `logout --revoke` too.
    //
    // A warning, not a refusal, for the same reason `ApiClient`'s is: a
    // deployment may terminate TLS at a proxy the CLI cannot see.
    if crate::client::is_cleartext_remote(&url) {
        eprintln!(
            "warning: {url} is plain http to a non-loopback host — the sign-in assertion and the minted credential would cross the network in the clear; prefer an https:// control plane"
        );
    }
    Ok(Resolved { url, defaulted })
}

/// The control plane `logout --revoke` should talk to, resolved by the same
/// chain `login` uses so the two cannot disagree about where a token lives.
pub(super) fn control_plane_for_revoke(
    args: &LogoutArgs,
    path: &Path,
    env_control_plane: Option<&str>,
) -> Result<String> {
    resolve_control_plane(
        args.control_plane.clone(),
        env_control_plane.map(str::to_string),
        config::load(path).as_ref(),
        // Never the console: `--revoke` calls `DELETE /v1/me/tokens/{id}`
        // directly, which a console does not serve.
        UrlKind::ControlPlane,
    )
    .map(|resolved| resolved.url)
}

/// Print what the run did.
///
/// The summary is rendered by [`render_report`] and only the warnings go to
/// stderr, split the way `config`'s `render_view` is: the text a user reads is
/// then asserted directly, instead of only through a subprocess's stdout.
fn print_report(report: &LoginReport) {
    print!("{}", render_report(report));
    for (token_id, reason) in &report.revoke_failures {
        // Not a failure of the login (§6.5): the new context is already good,
        // so this warns on stderr rather than joining the summary.
        eprintln!(
            "warning: could not revoke the replaced credential {token_id} ({reason}) — revoke it in the console"
        );
    }
}

/// The summary, one newline-terminated line per outcome. Token VALUES never
/// appear — only the context that now holds one, and the id of anything
/// revoked or kept.
fn render_report(report: &LoginReport) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    for (name, state) in &report.skipped {
        let _ = writeln!(out, "skipped {name}: workspace is {state}, not active");
    }
    for context in &report.written {
        let expiry = match &context.expires_at {
            Some(at) => format!(", expires {at}"),
            None => String::new(),
        };
        let _ = writeln!(
            out,
            "wrote context {} → {} (workspace {}, role {}{expiry})",
            context.name, context.server, context.workspace, context.role
        );
    }
    if let Some(current) = &report.current_context {
        let _ = writeln!(out, "current context is now {current}");
    }
    for token_id in &report.replaced_revoked {
        let _ = writeln!(
            out,
            "revoked the credential this login replaced ({token_id})"
        );
    }
    for token_id in &report.replaced_kept {
        let _ = writeln!(
            out,
            "kept the credential this login replaced ({token_id}) — it stays valid until it expires"
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_CONSOLE, LoginArgs, UrlKind, default_notice, options_from, render_report,
        resolve_control_plane,
    };
    use crate::config::ContextsFile;
    use crate::login::{LoginReport, Selection, WrittenContext};
    use chrono::Duration;
    use rstest::rstest;
    use std::path::Path;

    fn login_args() -> LoginArgs {
        LoginArgs {
            control_plane: Some("http://127.0.0.1:18090".to_string()),
            workspace: None,
            all_workspaces: false,
            expires: "90d".to_string(),
            no_browser: false,
            client_id: None,
            identity: None,
            i_know_this_is_dev_auth: false,
            no_verify: false,
            keep_old_token: false,
        }
    }

    /// The flag → option mapping, including the two global flags that mean
    /// something different under `login`: `--server` is the gateway URL to
    /// write, and `--context` is the name to write it under.
    #[test]
    fn flags_map_onto_the_flows_options() {
        let missing = Path::new("/nonexistent/skardi/config.yaml");

        let mut args = login_args();
        args.workspace = Some("acme-prod".to_string());
        let options = options_from(
            &args,
            Some("named-by-hand".to_string()),
            Some("https://gw.example".to_string()),
            missing,
        )
        .unwrap();
        assert_eq!(options.selection, Selection::Named("acme-prod".to_string()));
        assert_eq!(options.context_name.as_deref(), Some("named-by-hand"));
        assert_eq!(
            options.server_override.as_deref(),
            Some("https://gw.example")
        );
        assert_eq!(options.expires, Duration::try_days(90).unwrap());
        assert_eq!(options.token_name, crate::login::default_token_name());

        let mut args = login_args();
        args.all_workspaces = true;
        args.expires = "12h".to_string();
        args.no_verify = true;
        args.keep_old_token = true;
        let options = options_from(&args, None, None, missing).unwrap();
        assert_eq!(options.selection, Selection::All);
        assert_eq!(options.expires, Duration::try_hours(12).unwrap());
        assert!(options.no_verify && options.keep_old_token);

        assert_eq!(
            options_from(&login_args(), None, None, missing)
                .unwrap()
                .selection,
            Selection::Auto
        );
    }

    /// A bad `--expires` fails while the options are assembled — before the
    /// browser opens, not after a credential exists.
    #[test]
    fn an_unparsable_expiry_fails_before_anything_happens() {
        let mut args = login_args();
        args.expires = "next tuesday".to_string();
        // `unwrap_err` would require `LoginOptions: Debug`, which it
        // deliberately does not derive (see its definition).
        let err = match options_from(&args, None, None, Path::new("/nonexistent/c.yaml")) {
            Ok(_) => panic!("'next tuesday' must not parse as a duration"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("--expires"), "{err}");
    }

    #[test]
    fn the_summary_names_every_outcome_and_no_token_value() {
        let report = LoginReport {
            written: vec![WrittenContext {
                name: "acme/acme-prod".to_string(),
                server: "https://gw.example".to_string(),
                workspace: "acme-prod".to_string(),
                role: "admin".to_string(),
                expires_at: Some("2026-11-22T12:00:00Z".to_string()),
            }],
            skipped: vec![("acme/staging".to_string(), "provisioning".to_string())],
            current_context: Some("acme/acme-prod".to_string()),
            replaced_revoked: vec!["tok-old".to_string()],
            replaced_kept: vec!["tok-kept".to_string()],
            revoke_failures: vec![("tok-stuck".to_string(), "HTTP 500".to_string())],
        };

        let rendered = render_report(&report);
        assert_eq!(
            rendered,
            "skipped acme/staging: workspace is provisioning, not active\n\
             wrote context acme/acme-prod → https://gw.example (workspace acme-prod, role admin, expires 2026-11-22T12:00:00Z)\n\
             current context is now acme/acme-prod\n\
             revoked the credential this login replaced (tok-old)\n\
             kept the credential this login replaced (tok-kept) — it stays valid until it expires\n"
        );
        // A revocation failure warns on stderr and is deliberately NOT part of
        // the summary: the login itself succeeded (§6.5).
        assert!(!rendered.contains("tok-stuck"), "{rendered}");
    }

    /// `print_report` writes the summary to stdout and only the revocation
    /// warnings to stderr. Called for the warning path specifically, since
    /// that is the branch `render_report` deliberately does not carry.
    #[test]
    fn print_report_emits_the_summary_and_warns_separately() {
        let report = LoginReport {
            written: vec![WrittenContext {
                name: "acme/prod".to_string(),
                server: "https://gw.example".to_string(),
                workspace: "acme-prod".to_string(),
                role: "admin".to_string(),
                expires_at: None,
            }],
            revoke_failures: vec![("tok-stuck".to_string(), "HTTP 500".to_string())],
            ..LoginReport::default()
        };
        super::print_report(&report);
    }

    /// A control plane that returned no expiry renders without a dangling
    /// clause.
    #[test]
    fn a_context_without_an_expiry_renders_cleanly() {
        let report = LoginReport {
            written: vec![WrittenContext {
                name: "acme/prod".to_string(),
                server: "https://gw.example".to_string(),
                workspace: "acme-prod".to_string(),
                role: "viewer".to_string(),
                expires_at: None,
            }],
            ..LoginReport::default()
        };
        assert_eq!(
            render_report(&report),
            "wrote context acme/prod → https://gw.example (workspace acme-prod, role viewer)\n"
        );
    }

    fn file_with(control_plane: Option<&str>) -> ContextsFile {
        ContextsFile {
            control_plane: control_plane.map(str::to_string),
            ..ContextsFile::default()
        }
    }

    #[test]
    fn control_plane_precedence_is_flag_then_env_then_file() {
        let file = file_with(Some("https://file.example"));
        assert_eq!(
            resolve_control_plane(
                Some("https://flag.example".into()),
                Some("https://env.example".into()),
                Some(&file),
                UrlKind::ControlPlane,
            )
            .unwrap()
            .url,
            "https://flag.example"
        );
        assert_eq!(
            resolve_control_plane(
                None,
                Some("https://env.example".into()),
                Some(&file),
                UrlKind::ControlPlane,
            )
            .unwrap()
            .url,
            "https://env.example"
        );
        assert_eq!(
            resolve_control_plane(None, None, Some(&file), UrlKind::ControlPlane)
                .unwrap()
                .url,
            "https://file.example"
        );
    }

    /// A blank flag or an exported-but-empty variable must not win the chain,
    /// or `SKARDI_CONTROL_PLANE_URL=` would shadow a configured file.
    #[test]
    fn blank_values_are_skipped_not_honoured() {
        let file = file_with(Some("https://file.example"));
        assert_eq!(
            resolve_control_plane(
                Some("   ".into()),
                Some(String::new()),
                Some(&file),
                UrlKind::ControlPlane,
            )
            .unwrap()
            .url,
            "https://file.example"
        );
    }

    #[test]
    fn no_control_plane_names_all_three_inputs() {
        let err = resolve_control_plane(None, None, Some(&file_with(None)), UrlKind::ControlPlane)
            .unwrap_err()
            .to_string();
        assert!(err.contains("--control-plane"), "{err}");
        assert!(err.contains("SKARDI_CONTROL_PLANE_URL"), "{err}");
        assert!(err.contains("control-plane:"), "{err}");
    }

    /// Each flow falls back to ITS OWN recorded URL, and never the other's.
    ///
    /// The defect this pins: a brokered login recorded the console URL as
    /// `control-plane`, so a later `logout --revoke` sent
    /// `DELETE /v1/me/tokens/{id}` at a console — which answered with its 404
    /// page, after the local credential had already been cleared. The two URLs
    /// are different services with different path layouts, so the fallback key
    /// is part of the flow's identity.
    #[rstest]
    #[case::console_reads_console(UrlKind::Console, "https://console.example")]
    #[case::direct_reads_control_plane(UrlKind::ControlPlane, "https://cp.example")]
    fn each_kind_falls_back_to_its_own_key(#[case] kind: UrlKind, #[case] expected: &str) {
        let file = ContextsFile {
            control_plane: Some("https://cp.example".to_string()),
            console: Some("https://console.example".to_string()),
            ..ContextsFile::default()
        };
        assert_eq!(
            resolve_control_plane(None, None, Some(&file), kind)
                .unwrap()
                .url,
            expected
        );
    }

    /// A file holding ONLY a console URL leaves the direct flows unconfigured,
    /// which is the point: they say so by name instead of aiming
    /// `/v1/me/tokens` at a console.
    #[test]
    fn a_console_only_file_does_not_configure_the_direct_flows() {
        let file = ContextsFile {
            console: Some("https://console.example".to_string()),
            ..ContextsFile::default()
        };
        let err = resolve_control_plane(None, None, Some(&file), UrlKind::ControlPlane)
            .unwrap_err()
            .to_string();
        assert!(err.contains("--control-plane"), "{err}");
        assert!(err.contains("control-plane:"), "{err}");
        // And the reverse: the brokered flow is configured by it.
        assert_eq!(
            resolve_control_plane(None, None, Some(&file), UrlKind::Console)
                .unwrap()
                .url,
            "https://console.example"
        );
    }

    /// **With nothing configured, the brokered flow goes to the hosted
    /// console — and says that it decided.** This is the one built-in URL in
    /// the binary, and the notice hangs off `defaulted`, so both halves are
    /// pinned: the value, and the fact that the caller can tell.
    #[test]
    fn the_brokered_flow_defaults_to_the_hosted_console() {
        let resolved = resolve_control_plane(None, None, None, UrlKind::Console).unwrap();
        assert_eq!(resolved.url, DEFAULT_CONSOLE);
        assert!(resolved.defaulted);
        // A file with no `console:` key is the same as no file.
        let resolved =
            resolve_control_plane(None, None, Some(&file_with(None)), UrlKind::Console).unwrap();
        assert_eq!(resolved.url, DEFAULT_CONSOLE);
        assert!(resolved.defaulted);
    }

    /// The default is the LAST step, never a tie-break: anything configured
    /// wins and is reported as configured — including someone who wrote the
    /// default's own value into their file, which is a choice, not a default.
    #[rstest]
    #[case::flag(Some("https://flag.example"), None, None, "https://flag.example")]
    #[case::env(None, Some("https://env.example"), None, "https://env.example")]
    #[case::file(None, None, Some("https://console.example"), "https://console.example")]
    #[case::the_default_written_by_hand(None, None, Some(DEFAULT_CONSOLE), DEFAULT_CONSOLE)]
    fn a_configured_console_beats_the_default_and_is_not_reported_as_one(
        #[case] flag: Option<&str>,
        #[case] env: Option<&str>,
        #[case] file_console: Option<&str>,
        #[case] expected: &str,
    ) {
        let file = ContextsFile {
            console: file_console.map(str::to_string),
            ..ContextsFile::default()
        };
        let resolved = resolve_control_plane(
            flag.map(str::to_string),
            env.map(str::to_string),
            Some(&file),
            UrlKind::Console,
        )
        .unwrap();
        assert_eq!(resolved.url, expected);
        assert!(!resolved.defaulted);
    }

    /// The notice says it is a default and names every way to override it. It
    /// shares the three input names with the no-URL error on purpose — they
    /// are the same three inputs — which is exactly why it also has to carry a
    /// word the error does not, so the two cannot be mistaken for each other by
    /// an assertion that only looks for the inputs.
    #[test]
    fn the_default_notice_says_it_is_one_and_names_every_override() {
        let notice = default_notice(DEFAULT_CONSOLE);
        assert!(notice.contains(DEFAULT_CONSOLE), "{notice}");
        assert!(notice.contains("built-in default"), "{notice}");
        assert!(notice.contains("--control-plane"), "{notice}");
        assert!(notice.contains("SKARDI_CONTROL_PLANE_URL"), "{notice}");
        assert!(notice.contains("console:"), "{notice}");
        // And it is not the error: no "no control plane configured".
        assert!(!notice.contains("no control plane configured"), "{notice}");
    }

    /// The direct flow has NO default — there is no hosted address for what it
    /// calls — so it still fails by name, and the message names the direct
    /// flow's key rather than the brokered one's.
    #[test]
    fn the_direct_flow_still_has_no_default_and_names_its_own_key() {
        let err = resolve_control_plane(None, None, None, UrlKind::ControlPlane)
            .unwrap_err()
            .to_string();
        assert!(err.contains("control-plane:"), "{err}");
        assert!(!err.contains("console:"), "{err}");
    }
}
