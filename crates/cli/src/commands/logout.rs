//! `skardi logout` — drop the local credential, and optionally revoke it at
//! the control plane (§6.4).
//!
//! The two halves are deliberately separate. A PAT cannot manage PATs, so
//! plain `logout` is a purely local edit that says the PAT stays valid, and
//! `--revoke` re-authenticates (browser or `--identity`) to call
//! `DELETE /v1/me/tokens/{id}`. Presenting the local delete as a revocation
//! would be the more dangerous message: a token nobody holds is still a token
//! that works.

use super::login::LogoutArgs;
use crate::config::{self, ContextMode, ContextsFile};
use crate::login::control_plane::ControlPlane;
use crate::login::oauth;
use anyhow::{Context as _, Result, bail};
use std::path::Path;

/// One context's credential, as it was before this command cleared it.
struct Cleared {
    context: String,
    token_id: Option<String>,
}

pub async fn run(args: LogoutArgs, flag_context: Option<String>) -> Result<()> {
    let path = config::default_config_path()
        .context("cannot determine the home directory for ~/.skardi/config.yaml")?;
    run_at(
        &path,
        args,
        flag_context.as_deref(),
        std::env::var("SKARDI_CONTEXT").ok().as_deref(),
        std::env::var("SKARDI_CONTROL_PLANE_URL").ok().as_deref(),
    )
    .await
}

/// The command with every ambient input passed in, so the whole thing —
/// including `--revoke` — is testable in-process against a mock control plane.
pub(crate) async fn run_at(
    path: &Path,
    args: LogoutArgs,
    flag_context: Option<&str>,
    env_context: Option<&str>,
    env_control_plane: Option<&str>,
) -> Result<()> {
    // Read the ids BEFORE clearing them: after the write they are gone, and
    // --revoke needs them. The revoke runs after the local edit so a control
    // plane that cannot be reached still leaves the credential off this disk.
    let cleared = clear_credentials(path, &args, flag_context, env_context)?;
    if cleared.is_empty() {
        println!("no cloud context held a credential — nothing to do");
        return Ok(());
    }

    for entry in &cleared {
        println!("cleared the credential in context {}", entry.context);
    }

    if !args.revoke {
        println!(
            "the credential is gone from this machine but stays VALID until it expires — run 'skardi logout --revoke' to revoke it at the control plane"
        );
        return Ok(());
    }

    let (revocable, unrevocable): (Vec<&Cleared>, Vec<&Cleared>) =
        cleared.iter().partition(|c| c.token_id.is_some());
    // Named individually, not skipped: a context written by `config
    // set-context` has no `token-id`, and a PAT cannot name itself to the
    // revoke endpoint. Clearing it locally while saying nothing would read as
    // a revocation that happened.
    for entry in &unrevocable {
        println!(
            "cannot revoke the credential from context {}: it has no token-id, so only the console can revoke it",
            entry.context
        );
    }
    if revocable.is_empty() {
        bail!(
            "--revoke needs the credential's token id, and no cleared context recorded one (a context written by 'config set-context' has no token-id). Revoke it in the console"
        );
    }

    let cp = authenticate(&args, path, env_control_plane).await?;
    let mut failures = Vec::new();
    for entry in revocable {
        let token_id = entry.token_id.as_deref().unwrap_or_default();
        match cp.revoke(token_id).await {
            Ok(()) => println!("revoked {token_id} ({})", entry.context),
            Err(err) => failures.push(format!("  {token_id} ({}): {err}", entry.context)),
        }
    }
    if !failures.is_empty() {
        bail!(
            "these credentials are still live at the control plane — revoke them in the console:\n{}",
            failures.join("\n")
        );
    }
    Ok(())
}

/// Clear `token`/`token-id`/`token-expires-at` from the selected contexts,
/// leaving the context itself (server, workspace, mode) in place so a later
/// `login` refills it. Deleting the context is `config delete-context`'s job.
fn clear_credentials(
    path: &Path,
    args: &LogoutArgs,
    flag_context: Option<&str>,
    env_context: Option<&str>,
) -> Result<Vec<Cleared>> {
    let mut file: ContextsFile = config::load_for_mutation(path)?;
    file.promote_legacy_spec();

    let targets: Vec<String> = if args.all {
        // Cloud contexts only: a server-mode context's token is one the
        // operator configured by hand, and `logout --all` sweeping it away
        // would be a surprise with no login to undo it.
        file.contexts
            .iter()
            .filter(|c| c.mode == ContextMode::Cloud)
            .map(|c| c.name.clone())
            .collect()
    } else {
        let selected = config::select_context(
            &file.contexts,
            file.current_context.as_deref(),
            flag_context,
            env_context,
        );
        match selected? {
            Some(context) => vec![context.name],
            None => bail!(
                "no context selected: pass --context <NAME>, set a current-context, or use --all"
            ),
        }
    };

    let mut cleared = Vec::new();
    for name in targets {
        let Some(context) = file.contexts.iter_mut().find(|c| c.name == name) else {
            continue;
        };
        if context.token.is_none() && context.token_id.is_none() {
            continue;
        }
        cleared.push(Cleared {
            context: name,
            token_id: context.token_id.clone(),
        });
        context.token = None;
        context.token_id = None;
        context.token_expires_at = None;
    }

    if !cleared.is_empty() {
        config::save(path, &file)?;
    }
    Ok(cleared)
}

/// Re-authenticate for `--revoke`, reusing `login`'s acquirers so the dev path
/// and its loopback guard behave identically here.
async fn authenticate(
    args: &LogoutArgs,
    path: &Path,
    env_control_plane: Option<&str>,
) -> Result<ControlPlane> {
    let http = reqwest::Client::builder().no_proxy().build()?;
    let control_plane = super::login::control_plane_for_revoke(args, path, env_control_plane)?;
    let bearer = match &args.identity {
        Some(identity) => {
            crate::login::check_dev_identity(
                identity,
                &control_plane,
                args.i_know_this_is_dev_auth,
            )?;
            identity.clone()
        }
        None => {
            let client_id = args
                .client_id
                .clone()
                .or_else(|| std::env::var("SKARDI_OAUTH_CLIENT_ID").ok())
                .filter(|id| !id.trim().is_empty())
                .context(
                    "--revoke re-authenticates, which needs --client-id (or $SKARDI_OAUTH_CLIENT_ID), or --identity dev:<id> against a loopback control plane",
                )?;
            oauth::acquire_id_token(
                &http,
                &oauth::Endpoints::default(),
                &client_id,
                false,
                oauth::CALLBACK_TIMEOUT,
                &oauth::open_in_browser,
            )
            .await?
        }
    };
    Ok(ControlPlane::new(http, &control_plane, bearer))
}

#[cfg(test)]
mod tests {
    use super::{LogoutArgs, run_at};
    use serde_json::json;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;
    use wiremock::matchers::{method, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn args(all: bool, revoke: bool) -> LogoutArgs {
        LogoutArgs {
            all,
            revoke,
            control_plane: None,
            client_id: None,
            identity: Some("dev:alice".to_string()),
            i_know_this_is_dev_auth: false,
        }
    }

    /// Two cloud contexts with credentials, one server-mode context with one.
    fn seed(dir: &TempDir, control_plane: &str) -> PathBuf {
        let path = dir.path().join(".skardi").join("config.yaml");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            format!(
                "control-plane: {control_plane}\n\
                 current-context: acme/prod\n\
                 contexts:\n\
                 \x20 - name: acme/prod\n\
                 \x20   mode: cloud\n\
                 \x20   server: http://gw.example\n\
                 \x20   workspace: acme-prod\n\
                 \x20   token: skardi_pat_prod\n\
                 \x20   token-id: tok-prod\n\
                 \x20   token-expires-at: 2026-11-22T12:00:00Z\n\
                 \x20 - name: acme/staging\n\
                 \x20   mode: cloud\n\
                 \x20   server: http://gw.example\n\
                 \x20   workspace: acme-staging\n\
                 \x20   token: skardi_pat_staging\n\
                 \x20   token-id: tok-staging\n\
                 \x20 - name: local\n\
                 \x20   server: http://127.0.0.1:8080\n\
                 \x20   token: a-hand-configured-token\n"
            ),
        )
        .unwrap();
        path
    }

    fn yaml(path: &Path) -> serde_yaml::Value {
        serde_yaml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    /// The selected context loses its credential; the context itself stays, so
    /// a later `login` refills it rather than re-deriving server and workspace.
    #[tokio::test]
    async fn logout_clears_the_selected_contexts_credential_and_keeps_the_context() {
        let home = TempDir::new().unwrap();
        let path = seed(&home, "http://127.0.0.1:1");

        run_at(&path, args(false, false), None, None, None)
            .await
            .unwrap();

        let file = yaml(&path);
        let prod = &file["contexts"][0];
        assert_eq!(prod["name"], "acme/prod");
        assert_eq!(prod["server"], "http://gw.example");
        assert_eq!(prod["workspace"], "acme-prod");
        assert!(prod.get("token").is_none(), "token must be gone");
        assert!(prod.get("token-id").is_none());
        assert!(prod.get("token-expires-at").is_none());
        // Untouched contexts keep theirs.
        assert_eq!(file["contexts"][1]["token"], "skardi_pat_staging");
        assert_eq!(file["contexts"][2]["token"], "a-hand-configured-token");
        assert_eq!(file["current-context"], "acme/prod");
    }

    /// `--all` is scoped to cloud contexts: a server-mode token is one the
    /// operator configured by hand, and there is no `login` to put it back.
    #[tokio::test]
    async fn logout_all_clears_every_cloud_context_and_leaves_server_mode_alone() {
        let home = TempDir::new().unwrap();
        let path = seed(&home, "http://127.0.0.1:1");

        run_at(&path, args(true, false), None, None, None)
            .await
            .unwrap();

        let file = yaml(&path);
        assert!(file["contexts"][0].get("token").is_none());
        assert!(file["contexts"][1].get("token").is_none());
        assert_eq!(
            file["contexts"][2]["token"], "a-hand-configured-token",
            "a hand-configured server token is not login's to remove"
        );
    }

    #[tokio::test]
    async fn revoke_deletes_the_pat_at_the_control_plane() {
        let cp = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path_regex(r"^/v1/me/tokens/.+$"))
            .respond_with(ResponseTemplate::new(204))
            .expect(2)
            .mount(&cp)
            .await;

        let home = TempDir::new().unwrap();
        let path = seed(&home, &cp.uri());

        run_at(&path, args(true, true), None, None, None)
            .await
            .unwrap();

        let mut revoked: Vec<String> = cp
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|r| r.url.path().rsplit('/').next().unwrap().to_string())
            .collect();
        revoked.sort();
        assert_eq!(revoked, ["tok-prod", "tok-staging"]);
        // The local edit happened too, and first: a control plane that cannot
        // be reached must still leave the credential off this disk.
        assert!(yaml(&path)["contexts"][0].get("token").is_none());
    }

    /// A control plane that refuses the revoke is a non-zero exit naming the
    /// live token, not a silent success.
    #[tokio::test]
    async fn a_failed_revocation_reports_the_still_live_token() {
        let cp = MockServer::start().await;
        Mock::given(method("DELETE"))
            .respond_with(
                ResponseTemplate::new(500).set_body_json(
                    json!({"error": {"code": "internal", "message": "internal error"}}),
                ),
            )
            .mount(&cp)
            .await;

        let home = TempDir::new().unwrap();
        let path = seed(&home, &cp.uri());

        let err = run_at(&path, args(false, true), None, None, None)
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("still live"), "{err}");
        assert!(err.contains("tok-prod"), "{err}");
        // Local removal still stands: the credential is off this machine.
        assert!(yaml(&path)["contexts"][0].get("token").is_none());
    }

    /// A context written by `config set-context` has no `token-id`, and a PAT
    /// cannot name itself to the revoke endpoint.
    #[tokio::test]
    async fn revoke_without_a_token_id_says_what_to_do_instead() {
        let home = TempDir::new().unwrap();
        let path = home.path().join(".skardi").join("config.yaml");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            "current-context: acme/prod\n\
             contexts:\n\
             \x20 - name: acme/prod\n\
             \x20   mode: cloud\n\
             \x20   server: http://gw.example\n\
             \x20   workspace: acme-prod\n\
             \x20   token: hand-written\n",
        )
        .unwrap();

        let err = run_at(&path, args(false, true), None, None, None)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("token id"), "{err}");
        assert!(err.contains("console"), "{err}");
    }

    /// A mix: one context has a `token-id`, one does not. The revocable one is
    /// revoked and the other is NAMED, because clearing it locally while
    /// saying nothing reads as a revocation that happened.
    #[tokio::test]
    async fn revoke_names_the_contexts_it_cannot_revoke() {
        let cp = MockServer::start().await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&cp)
            .await;

        let home = TempDir::new().unwrap();
        let path = home.path().join(".skardi").join("config.yaml");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            format!(
                "control-plane: {}\n\
                 contexts:\n\
                 \x20 - name: acme/prod\n\
                 \x20   mode: cloud\n\
                 \x20   server: http://gw.example\n\
                 \x20   workspace: acme-prod\n\
                 \x20   token: skardi_pat_prod\n\
                 \x20   token-id: tok-prod\n\
                 \x20 - name: acme/hand-made\n\
                 \x20   mode: cloud\n\
                 \x20   server: http://gw.example\n\
                 \x20   workspace: acme-other\n\
                 \x20   token: hand-written\n",
                cp.uri()
            ),
        )
        .unwrap();

        run_at(&path, args(true, true), None, None, None)
            .await
            .unwrap();

        // Both credentials left the disk; only one could be revoked.
        let file = yaml(&path);
        assert!(file["contexts"][0].get("token").is_none());
        assert!(file["contexts"][1].get("token").is_none());
        let revoked: Vec<String> = cp
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|r| r.url.path().rsplit('/').next().unwrap().to_string())
            .collect();
        assert_eq!(revoked, ["tok-prod"]);
    }

    #[tokio::test]
    async fn nothing_to_clear_is_reported_rather_than_failing() {
        let home = TempDir::new().unwrap();
        let path = home.path().join(".skardi").join("config.yaml");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            "contexts:\n  - name: local\n    server: http://127.0.0.1:8080\n",
        )
        .unwrap();
        let before = std::fs::read_to_string(&path).unwrap();

        run_at(&path, args(true, false), None, None, None)
            .await
            .unwrap();

        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            before,
            "a no-op must not rewrite the file"
        );
    }

    /// Several contexts and no pointer selects nothing (§5.1), so `logout`
    /// with no `--context` and no `--all` says which flags resolve it.
    #[tokio::test]
    async fn no_selectable_context_names_the_flags_that_would_pick_one() {
        let home = TempDir::new().unwrap();
        let path = home.path().join(".skardi").join("config.yaml");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            "contexts:\n\
             \x20 - name: one\n\
             \x20   mode: cloud\n\
             \x20   server: http://gw.example\n\
             \x20   workspace: w-one\n\
             \x20   token: t1\n\
             \x20 - name: two\n\
             \x20   mode: cloud\n\
             \x20   server: http://gw.example\n\
             \x20   workspace: w-two\n\
             \x20   token: t2\n",
        )
        .unwrap();
        let before = std::fs::read_to_string(&path).unwrap();

        let err = run_at(&path, args(false, false), None, None, None)
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("--context <NAME>"), "{err}");
        assert!(err.contains("--all"), "{err}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), before);
    }

    /// A cloud context that holds no credential is skipped by `--all` rather
    /// than rewritten, so a repeated `logout --all` is a no-op.
    #[tokio::test]
    async fn logout_all_is_idempotent() {
        let home = TempDir::new().unwrap();
        let path = seed(&home, "http://127.0.0.1:1");

        run_at(&path, args(true, false), None, None, None)
            .await
            .unwrap();
        let after_first = std::fs::read_to_string(&path).unwrap();

        run_at(&path, args(true, false), None, None, None)
            .await
            .unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), after_first);
    }

    /// `--revoke` without `--identity` re-authenticates through the browser,
    /// which needs a client id — named, rather than surfacing as an opaque
    /// failure once the credential is already gone locally.
    #[tokio::test]
    async fn revoke_without_an_identity_or_a_client_id_says_which_is_missing() {
        let home = TempDir::new().unwrap();
        let path = seed(&home, "http://127.0.0.1:1");
        let mut args = args(false, true);
        args.identity = None;

        let err = run_at(&path, args, None, None, None)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("--client-id"), "{err}");
        assert!(err.contains("--identity dev:<id>"), "{err}");
        // The local clear still happened: the credential is off this machine
        // whether or not the control plane can be reached.
        assert!(yaml(&path)["contexts"][0].get("token").is_none());
    }

    #[tokio::test]
    async fn an_explicit_context_flag_wins_over_the_current_one() {
        let home = TempDir::new().unwrap();
        let path = seed(&home, "http://127.0.0.1:1");

        run_at(&path, args(false, false), Some("acme/staging"), None, None)
            .await
            .unwrap();

        let file = yaml(&path);
        assert_eq!(file["contexts"][0]["token"], "skardi_pat_prod");
        assert!(file["contexts"][1].get("token").is_none());
    }
}
