//! §10's `login` test list, driven in-process against a `wiremock` control
//! plane and a `wiremock` gateway.
//!
//! In-process rather than through the binary on purpose: the saga's whole
//! point is what happens to credentials on a FAILURE path, and a subprocess
//! can only be observed through its exit code and stderr. Here the control
//! plane records every request, so "the first PAT was revoked" is an
//! assertion rather than an inference.

use super::{
    LoginOptions, Selection, login, oauth, parse_expires, render_workspace_menu, select_memberships,
};
use chrono::{DateTime, Duration, Utc};
use rstest::rstest;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use wiremock::matchers::{body_string_contains, method, path, path_regex};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

fn at(rfc3339: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(rfc3339)
        .unwrap()
        .with_timezone(&Utc)
}

/// One membership as `/v1/me/workspaces` renders it, with `gateway_url`
/// present or absent (§7.1 is M4, so absence is the state today).
fn membership(org: &str, workspace: &str, state: &str, gateway_url: Option<&str>) -> Value {
    let mut value = json!({
        "org_slug": org,
        "tenant_slug": workspace,
        "display_name": workspace,
        "role": "admin",
        "provisioning_state": state,
    });
    if let Some(url) = gateway_url {
        value["gateway_url"] = json!(url);
    }
    value
}

async fn control_plane_with(memberships: Vec<Value>) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/me/workspaces"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"workspaces": memberships})))
        .mount(&server)
        .await;
    server
}

/// Mint `token_id` for any request naming `workspace`.
async fn mint_ok(server: &MockServer, workspace: &str, token_id: &str) {
    Mock::given(method("POST"))
        .and(path("/v1/me/tokens"))
        .and(body_string_contains(format!("\"{workspace}\"")))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "token": format!("skardi_pat_{token_id}"),
            "pat": {
                "token_id": token_id,
                "name": "cli@test-host",
                "expires_at": "2026-11-22T12:00:00Z",
            },
        })))
        .mount(server)
        .await;
}

async fn mint_fails(server: &MockServer, workspace: &str, status: u16, body: Value) {
    Mock::given(method("POST"))
        .and(path("/v1/me/tokens"))
        .and(body_string_contains(format!("\"{workspace}\"")))
        .respond_with(ResponseTemplate::new(status).set_body_json(body))
        .mount(server)
        .await;
}

async fn revoke_answers(server: &MockServer, status: u16) {
    Mock::given(method("DELETE"))
        .and(path_regex(r"^/v1/me/tokens/.+$"))
        .respond_with(ResponseTemplate::new(status))
        .mount(server)
        .await;
}

/// A gateway that answers the `select 1` probe with `status`.
async fn gateway(status: u16) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/query"))
        .respond_with(ResponseTemplate::new(status).set_body_json(json!({
            "success": status == 200,
            "columns": ["n"],
            "rows": [[1]],
            "error": "forbidden",
            "error_type": "forbidden",
        })))
        .mount(&server)
        .await;
    server
}

fn options(control_plane: &str, server_override: Option<&str>) -> LoginOptions {
    LoginOptions {
        control_plane: control_plane.to_string(),
        client_id: None,
        // The dev path (§6.3) is how the flow under test is the real one minus
        // the browser; the control plane is a loopback wiremock, so the guard
        // is satisfied honestly rather than bypassed.
        identity: Some("dev:alice".to_string()),
        allow_dev_auth_off_loopback: false,
        selection: Selection::Auto,
        context_name: None,
        expires: Duration::try_days(90).unwrap(),
        no_browser: true,
        no_verify: false,
        keep_old_token: false,
        server_override: server_override.map(str::to_string),
        env_gateway_url: None,
        endpoints: oauth::Endpoints::default(),
        open_browser: oauth::open_in_browser,
        verify_timeout: std::time::Duration::from_millis(200),
        callback_timeout: std::time::Duration::from_millis(50),
        poll_interval: std::time::Duration::from_millis(1),
        token_name: "cli@test-host".to_string(),
        now: at("2026-08-24T12:00:00Z"),
        control: false,
        setup: None,
    }
}

fn config_in(dir: &TempDir) -> PathBuf {
    dir.path().join(".skardi").join("config.yaml")
}

fn read_yaml(path: &Path) -> serde_yaml::Value {
    serde_yaml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// Every DELETE the control plane saw, as the token id in its path.
fn revoked_ids(requests: &[Request]) -> Vec<String> {
    requests
        .iter()
        .filter(|r| r.method == wiremock::http::Method::DELETE)
        .map(|r| r.url.path().rsplit('/').next().unwrap_or("").to_string())
        .collect()
}

fn mint_bodies(requests: &[Request]) -> Vec<Value> {
    requests
        .iter()
        .filter(|r| r.method == wiremock::http::Method::POST && r.url.path() == "/v1/me/tokens")
        .map(|r| serde_json::from_slice(&r.body).unwrap())
        .collect()
}

#[tokio::test]
async fn a_lone_membership_mints_a_scoped_pat_verifies_it_and_writes_the_context() {
    let gw = gateway(200).await;
    let cp = control_plane_with(vec![membership(
        "acme",
        "acme-prod",
        "active",
        Some(&gw.uri()),
    )])
    .await;
    mint_ok(&cp, "acme-prod", "tok-1").await;

    let home = TempDir::new().unwrap();
    let path = config_in(&home);
    let report = login(options(&cp.uri(), None), &path).await.unwrap();

    // §6.1 step 5: one workspace, the membership's role, the requested expiry.
    let minted = mint_bodies(&cp.received_requests().await.unwrap());
    assert_eq!(minted.len(), 1);
    assert_eq!(minted[0]["name"], json!("cli@test-host"));
    assert_eq!(
        minted[0]["scope"],
        json!({"workspaces": ["acme-prod"], "max_role": "admin"})
    );
    assert!(
        minted[0]["expires_at"]
            .as_str()
            .unwrap()
            .starts_with("2026-11-22"),
        "90 days after 2026-08-24: {}",
        minted[0]["expires_at"]
    );

    // §6.1 step 6: the probe went to the resolved gateway, with the selector.
    let probes = gw.received_requests().await.unwrap();
    assert_eq!(probes.len(), 1);
    assert_eq!(
        probes[0].headers.get("Skardi-Workspace").unwrap(),
        "acme-prod"
    );
    assert_eq!(
        probes[0].headers.get("Authorization").unwrap(),
        "Bearer skardi_pat_tok-1"
    );

    // §6.1 step 7: one context, named <org>/<workspace>, made current.
    assert_eq!(report.written.len(), 1);
    assert_eq!(report.current_context.as_deref(), Some("acme/acme-prod"));
    let file = read_yaml(&path);
    assert_eq!(file["current-context"], "acme/acme-prod");
    assert_eq!(file["control-plane"].as_str().unwrap(), cp.uri());
    let context = &file["contexts"][0];
    assert_eq!(context["name"], "acme/acme-prod");
    assert_eq!(context["mode"], "cloud");
    assert_eq!(context["server"].as_str().unwrap(), gw.uri());
    assert_eq!(context["workspace"], "acme-prod");
    assert_eq!(context["token"], "skardi_pat_tok-1");
    assert_eq!(context["token-id"], "tok-1");
    assert_eq!(context["token-expires-at"], "2026-11-22T12:00:00Z");
    // Nothing was revoked: this was a first login, not a replacement.
    assert!(revoked_ids(&cp.received_requests().await.unwrap()).is_empty());
}

#[tokio::test]
async fn a_workspace_that_is_not_active_is_skipped_with_its_state_named() {
    let gw = gateway(200).await;
    let cp = control_plane_with(vec![
        membership("acme", "acme-prod", "active", Some(&gw.uri())),
        membership("acme", "acme-staging", "provisioning", Some(&gw.uri())),
    ])
    .await;
    mint_ok(&cp, "acme-prod", "tok-1").await;

    let home = TempDir::new().unwrap();
    let mut options = options(&cp.uri(), None);
    options.selection = Selection::All;
    let report = login(options, &config_in(&home)).await.unwrap();

    assert_eq!(
        report.skipped,
        vec![("acme/acme-staging".to_string(), "provisioning".to_string())]
    );
    assert_eq!(report.written.len(), 1);
    // Only the active workspace was minted for.
    assert_eq!(mint_bodies(&cp.received_requests().await.unwrap()).len(), 1);
}

#[tokio::test]
async fn every_membership_only_provisioning_is_a_typed_failure_naming_the_states() {
    let cp = control_plane_with(vec![membership("acme", "acme-prod", "provisioning", None)]).await;
    let home = TempDir::new().unwrap();

    let err = login(options(&cp.uri(), None), &config_in(&home))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("acme/acme-prod (provisioning)"), "{err}");
    assert!(!config_in(&home).exists(), "nothing may be written");
}

/// §7.1's `gateway_url` is read per MEMBERSHIP, so two memberships naming
/// different hosts produce two contexts pointing at them.
///
/// The first control-plane release projects ONE deployment-wide URL onto every
/// membership, so this case does not arise there — which is exactly why it is
/// pinned: when workspace runtimes start supplying their own endpoints through
/// the same field, the CLI must already honour them rather than collapse them
/// to the first one it saw.
#[tokio::test]
async fn each_context_points_at_the_gateway_its_own_membership_names() {
    let first = gateway(200).await;
    let second = gateway(200).await;
    let cp = control_plane_with(vec![
        membership("acme", "acme-prod", "active", Some(&first.uri())),
        membership("globex", "globex-prod", "active", Some(&second.uri())),
    ])
    .await;
    mint_ok(&cp, "acme-prod", "tok-1").await;
    mint_ok(&cp, "globex-prod", "tok-2").await;

    let home = TempDir::new().unwrap();
    let path = config_in(&home);
    let mut options = options(&cp.uri(), None);
    options.selection = Selection::All;
    let report = login(options, &path).await.unwrap();

    assert_eq!(report.written.len(), 2);
    let file = read_yaml(&path);
    let servers: Vec<String> = file["contexts"]
        .as_sequence()
        .unwrap()
        .iter()
        .map(|c| c["server"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(servers, vec![first.uri(), second.uri()]);
    // Each probe reached its own host, so a per-org URL is not just written
    // but actually used for verification.
    assert_eq!(first.received_requests().await.unwrap().len(), 1);
    assert_eq!(second.received_requests().await.unwrap().len(), 1);
}

/// The shape the first control-plane release actually returns (skardi-cloud
/// #355): one deployment-global URL copied onto every membership. §10 names it
/// alongside the per-membership case, because it is the one production hits —
/// several workspaces, one front door, and every context written pointing at it.
///
/// Same org deliberately: a multi-org identity cannot mint at all in v1
/// (`org_ambiguous`, §6.4), so the realistic shared case is several workspaces
/// within one org.
#[tokio::test]
async fn a_deployment_wide_gateway_url_is_used_by_every_context_it_names() {
    let shared = gateway(200).await;
    let cp = control_plane_with(vec![
        membership("acme", "acme-prod", "active", Some(&shared.uri())),
        membership("acme", "acme-staging", "active", Some(&shared.uri())),
    ])
    .await;
    mint_ok(&cp, "acme-prod", "tok-prod").await;
    mint_ok(&cp, "acme-staging", "tok-staging").await;

    let home = TempDir::new().unwrap();
    let path = config_in(&home);
    let mut options = options(&cp.uri(), None);
    options.selection = Selection::All;
    let report = login(options, &path).await.unwrap();

    assert_eq!(report.written.len(), 2);
    let file = read_yaml(&path);
    let contexts = file["contexts"].as_sequence().unwrap();
    assert_eq!(contexts.len(), 2);
    for context in contexts {
        assert_eq!(context["server"].as_str().unwrap(), shared.uri());
        assert_eq!(context["mode"], "cloud");
    }
    // Distinct workspaces and distinct credentials on one host — the selector
    // is what separates them, which is the whole premise of §7.3.
    assert_eq!(contexts[0]["workspace"], "acme-prod");
    assert_eq!(contexts[1]["workspace"], "acme-staging");
    assert_eq!(contexts[0]["token-id"], "tok-prod");
    assert_eq!(contexts[1]["token-id"], "tok-staging");
    // Both probes went to the shared host, each naming its own workspace.
    let probed: Vec<String> = shared
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|r| {
            r.headers
                .get("Skardi-Workspace")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(probed, ["acme-prod", "acme-staging"]);
}

/// §6.2's precedence, including the step that must NOT exist: no source at all
/// is a typed error, never a fall-through to a local port.
#[tokio::test]
async fn the_gateway_url_comes_from_the_flag_then_the_env_then_the_membership() {
    let flagged = gateway(200).await;
    let from_env = gateway(200).await;
    let from_membership = gateway(200).await;

    for (server_override, env, expected) in [
        (Some(flagged.uri()), Some(from_env.uri()), flagged.uri()),
        (None, Some(from_env.uri()), from_env.uri()),
        (None, None, from_membership.uri()),
    ] {
        let cp = control_plane_with(vec![membership(
            "acme",
            "acme-prod",
            "active",
            Some(&from_membership.uri()),
        )])
        .await;
        mint_ok(&cp, "acme-prod", "tok-1").await;

        let home = TempDir::new().unwrap();
        let path = config_in(&home);
        let mut options = options(&cp.uri(), server_override.as_deref());
        options.env_gateway_url = env;
        login(options, &path).await.unwrap();

        assert_eq!(read_yaml(&path)["contexts"][0]["server"], expected);
    }
}

#[tokio::test]
async fn no_gateway_url_anywhere_names_server_and_writes_nothing() {
    let cp = control_plane_with(vec![membership("acme", "acme-prod", "active", None)]).await;
    let home = TempDir::new().unwrap();
    let path = config_in(&home);

    let err = login(options(&cp.uri(), None), &path)
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("--server"), "{err}");
    assert!(err.contains("SKARDI_GATEWAY_URL"), "{err}");
    assert!(
        !err.contains("127.0.0.1:8080"),
        "there is deliberately no local fallback: {err}"
    );
    assert!(!path.exists(), "no context may be written");
    // The failure precedes the mint, so there is nothing to roll back.
    assert!(mint_bodies(&cp.received_requests().await.unwrap()).is_empty());
}

/// §6.1 step 6: "A context that cannot answer it is not written and its PAT is
/// revoked."
#[tokio::test]
async fn a_credential_that_cannot_query_is_revoked_and_no_context_is_written() {
    let gw = gateway(403).await;
    let cp = control_plane_with(vec![membership(
        "acme",
        "acme-prod",
        "active",
        Some(&gw.uri()),
    )])
    .await;
    mint_ok(&cp, "acme-prod", "tok-1").await;
    revoke_answers(&cp, 204).await;

    let home = TempDir::new().unwrap();
    let path = config_in(&home);
    let err = login(options(&cp.uri(), None), &path)
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("could not query"), "{err}");
    assert!(
        err.contains("--no-verify"),
        "the escape hatch is named: {err}"
    );
    assert!(!path.exists(), "no context may be written");
    assert_eq!(
        revoked_ids(&cp.received_requests().await.unwrap()),
        ["tok-1"]
    );
}

#[tokio::test]
async fn no_verify_skips_the_probe_entirely() {
    let gw = gateway(403).await;
    let cp = control_plane_with(vec![membership(
        "acme",
        "acme-prod",
        "active",
        Some(&gw.uri()),
    )])
    .await;
    mint_ok(&cp, "acme-prod", "tok-1").await;

    let home = TempDir::new().unwrap();
    let path = config_in(&home);
    let mut options = options(&cp.uri(), None);
    options.no_verify = true;
    login(options, &path).await.unwrap();

    assert!(gw.received_requests().await.unwrap().is_empty());
    assert_eq!(read_yaml(&path)["contexts"][0]["token-id"], "tok-1");
}

/// §6.5: "If a later mint fails … `login` revokes every retained token."
#[tokio::test]
async fn a_failure_on_the_second_mint_revokes_the_first() {
    let gw = gateway(200).await;
    let cp = control_plane_with(vec![
        membership("acme", "one", "active", Some(&gw.uri())),
        membership("acme", "two", "active", Some(&gw.uri())),
        membership("acme", "three", "active", Some(&gw.uri())),
    ])
    .await;
    mint_ok(&cp, "one", "tok-1").await;
    mint_fails(
        &cp,
        "two",
        500,
        json!({"error": {"code": "internal", "message": "internal error"}}),
    )
    .await;
    mint_ok(&cp, "three", "tok-3").await;
    revoke_answers(&cp, 204).await;

    let home = TempDir::new().unwrap();
    let path = config_in(&home);
    let mut options = options(&cp.uri(), None);
    options.selection = Selection::All;
    let err = login(options, &path).await.unwrap_err();
    let rendered = format!("{err:#}");

    assert!(rendered.contains("workspace 'two'"), "{rendered}");
    assert!(
        rendered.contains("internal error"),
        "the original cause survives the rollback: {rendered}"
    );
    let requests = cp.received_requests().await.unwrap();
    assert_eq!(revoked_ids(&requests), ["tok-1"]);
    // The third workspace is never minted for: the saga stops at the failure.
    assert_eq!(mint_bodies(&requests).len(), 2);
    assert!(!path.exists());
}

/// §6.5: "or the config write fails, `login` revokes every retained token".
#[tokio::test]
async fn a_config_write_failure_revokes_everything_minted() {
    let gw = gateway(200).await;
    let cp = control_plane_with(vec![
        membership("acme", "one", "active", Some(&gw.uri())),
        membership("acme", "two", "active", Some(&gw.uri())),
    ])
    .await;
    mint_ok(&cp, "one", "tok-1").await;
    mint_ok(&cp, "two", "tok-2").await;
    revoke_answers(&cp, 204).await;

    // A regular FILE where the config directory must be, so `save` cannot
    // create the parent — a write failure that needs no permission games.
    let home = TempDir::new().unwrap();
    let blocked = home.path().join(".skardi");
    std::fs::write(&blocked, b"not a directory").unwrap();

    let mut options = options(&cp.uri(), None);
    options.selection = Selection::All;
    let err = login(options, &blocked.join("config.yaml"))
        .await
        .unwrap_err();

    assert!(format!("{err:#}").contains("config"), "{err:#}");
    let mut revoked = revoked_ids(&cp.received_requests().await.unwrap());
    revoked.sort();
    assert_eq!(revoked, ["tok-1", "tok-2"]);
}

/// §6.5: "If rollback itself partially fails, the command exits non-zero and
/// prints the token ids … Silence here would leave credentials nobody knows
/// exist."
#[tokio::test]
async fn a_rollback_that_fails_reports_the_surviving_token_ids() {
    let gw = gateway(403).await;
    let cp = control_plane_with(vec![membership(
        "acme",
        "acme-prod",
        "active",
        Some(&gw.uri()),
    )])
    .await;
    mint_ok(&cp, "acme-prod", "tok-1").await;
    revoke_answers(&cp, 500).await;

    let home = TempDir::new().unwrap();
    let err = login(options(&cp.uri(), None), &config_in(&home))
        .await
        .unwrap_err();
    let rendered = format!("{err:#}");

    assert!(rendered.contains("LOGIN ROLLBACK INCOMPLETE"), "{rendered}");
    assert!(rendered.contains("tok-1"), "{rendered}");
    assert!(rendered.contains("acme-prod"), "{rendered}");
    assert!(
        rendered.contains("could not query"),
        "the original cause is still reported: {rendered}"
    );
}

/// §6.5: "Re-login over an existing context replaces it and revokes the token
/// it replaced", and `--keep-old-token` retains it.
#[tokio::test]
async fn a_relogin_replaces_the_context_and_revokes_or_keeps_the_old_token() {
    for keep_old in [false, true] {
        let gw = gateway(200).await;
        let cp = control_plane_with(vec![membership(
            "acme",
            "acme-prod",
            "active",
            Some(&gw.uri()),
        )])
        .await;
        mint_ok(&cp, "acme-prod", "tok-new").await;
        revoke_answers(&cp, 204).await;

        let home = TempDir::new().unwrap();
        let path = config_in(&home);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            "current-context: acme/acme-prod\n\
             contexts:\n\
             \x20 - name: acme/acme-prod\n\
             \x20   mode: cloud\n\
             \x20   server: http://old.example\n\
             \x20   workspace: acme-prod\n\
             \x20   token: skardi_pat_old\n\
             \x20   token-id: tok-old\n",
        )
        .unwrap();

        let mut options = options(&cp.uri(), None);
        options.keep_old_token = keep_old;
        let report = login(options, &path).await.unwrap();

        let file = read_yaml(&path);
        assert_eq!(
            file["contexts"].as_sequence().unwrap().len(),
            1,
            "replaced, not duplicated"
        );
        assert_eq!(file["contexts"][0]["token-id"], "tok-new");
        assert_eq!(file["contexts"][0]["server"].as_str().unwrap(), gw.uri());

        let revoked = revoked_ids(&cp.received_requests().await.unwrap());
        if keep_old {
            assert!(revoked.is_empty(), "--keep-old-token must retain it");
            assert_eq!(report.replaced_kept, ["tok-old"]);
        } else {
            assert_eq!(revoked, ["tok-old"]);
            assert_eq!(report.replaced_revoked, ["tok-old"]);
        }
    }
}

/// §6.4: a multi-org identity cannot mint in v1, and the message has to say
/// what to do instead.
#[tokio::test]
async fn a_multi_org_identity_gets_the_org_list_and_the_escape_hatch() {
    let gw = gateway(200).await;
    let cp = control_plane_with(vec![membership(
        "acme",
        "acme-prod",
        "active",
        Some(&gw.uri()),
    )])
    .await;
    mint_fails(
        &cp,
        "acme-prod",
        400,
        json!({"error": {
            "code": "org_ambiguous",
            "message": "caller belongs to multiple orgs; minting for multi-org identities is not available in v1",
            "orgs": [
                {"org_slug": "acme", "org_role": "owner"},
                {"org_slug": "globex", "org_role": "member"},
            ],
        }}),
    )
    .await;

    let home = TempDir::new().unwrap();
    let err = login(options(&cp.uri(), None), &config_in(&home))
        .await
        .unwrap_err();
    let rendered = format!("{err:#}");

    assert!(rendered.contains("acme, globex"), "{rendered}");
    assert!(rendered.contains("config set-context"), "{rendered}");
    assert!(rendered.contains("--token-stdin"), "{rendered}");
}

/// §6.3's guard. RFC1918 is included deliberately: that is where a shared
/// internal staging cluster lives, and a `dev:` bearer there is impersonation.
#[tokio::test]
async fn the_dev_identity_path_refuses_a_non_loopback_control_plane() {
    let home = TempDir::new().unwrap();
    for url in [
        "https://global.example.com",
        "http://10.0.0.5:8090",
        "http://192.168.1.10:8090",
        "http://172.16.4.4:8090",
    ] {
        let err = login(options(url, Some("http://gw.example")), &config_in(&home))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("--i-know-this-is-dev-auth"), "{url}: {err}");
    }

    // Loopback in all three spellings is allowed without the flag. These fail
    // LATER (nothing is listening), which is exactly the point: the guard
    // passed.
    for url in ["http://127.0.0.1:1", "http://localhost:1", "http://[::1]:1"] {
        let err = login(options(url, Some("http://gw.example")), &config_in(&home))
            .await
            .unwrap_err()
            .to_string();
        assert!(!err.contains("--i-know-this-is-dev-auth"), "{url}: {err}");
    }
}

#[tokio::test]
async fn a_non_dev_identity_bearer_is_refused_by_shape() {
    let home = TempDir::new().unwrap();
    let mut options = options("http://127.0.0.1:1", Some("http://gw.example"));
    options.identity = Some("eyJhbGciOi.a-real-looking-jwt".to_string());
    let err = login(options, &config_in(&home))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("must be a 'dev:"), "{err}");
}

#[tokio::test]
async fn a_missing_client_id_now_brokers_through_the_console_instead_of_failing() {
    // This case used to be a dead end: no identity and no usable client id
    // meant "no OAuth client id: pass --client-id, set
    // $SKARDI_OAUTH_CLIENT_ID, or use --identity dev:<id>". Every deployment
    // that wanted CLI access therefore had to provision a Google Desktop
    // client first, which is the whole problem design 2026-09-08 removes.
    //
    // It is now the DEFAULT path, so the assertion inverts: the flow proceeds
    // and fails at the console, not at the flag check. `127.0.0.1:1` answers
    // nothing, so the error names the console it could not reach — proof the
    // broker was selected rather than the OAuth path.
    let home = TempDir::new().unwrap();
    let mut options = options("http://127.0.0.1:1", Some("http://gw.example"));
    options.identity = None;
    options.client_id = Some("   ".to_string());
    let err = login(options, &config_in(&home))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("cannot reach the console"), "{err}");
    assert!(err.contains("cli-login/requests"), "{err}");
    assert!(
        !err.contains("SKARDI_OAUTH_CLIENT_ID"),
        "a client id is no longer required: {err}"
    );
}

#[tokio::test]
async fn naming_one_context_for_several_workspaces_is_refused() {
    let gw = gateway(200).await;
    let cp = control_plane_with(vec![
        membership("acme", "one", "active", Some(&gw.uri())),
        membership("acme", "two", "active", Some(&gw.uri())),
    ])
    .await;
    let home = TempDir::new().unwrap();
    let mut options = options(&cp.uri(), None);
    options.selection = Selection::All;
    options.context_name = Some("just-one".to_string());

    let err = login(options, &config_in(&home))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("--context names ONE context"), "{err}");
    // Refused before any mint: nothing to roll back.
    assert!(mint_bodies(&cp.received_requests().await.unwrap()).is_empty());
}

#[tokio::test]
async fn selecting_a_workspace_by_slug_lists_the_alternatives_when_it_is_wrong() {
    let gw = gateway(200).await;
    let cp = control_plane_with(vec![
        membership("acme", "one", "active", Some(&gw.uri())),
        membership("acme", "two", "active", Some(&gw.uri())),
    ])
    .await;
    let home = TempDir::new().unwrap();
    let mut options = options(&cp.uri(), None);
    options.selection = Selection::Named("three".to_string());

    let err = login(options, &config_in(&home))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("no active workspace named 'three'"), "{err}");
    assert!(err.contains("one, two"), "{err}");
}

#[test]
fn expires_accepts_days_and_hours_and_refuses_the_rest() {
    assert_eq!(
        parse_expires("90d").unwrap(),
        Duration::try_days(90).unwrap()
    );
    assert_eq!(
        parse_expires(" 90 ").unwrap(),
        Duration::try_days(90).unwrap()
    );
    assert_eq!(
        parse_expires("12h").unwrap(),
        Duration::try_hours(12).unwrap()
    );
    assert_eq!(parse_expires("7D").unwrap(), Duration::try_days(7).unwrap());
    for bad in ["0d", "-5d", "abc", "", "90m", "90 days"] {
        assert!(parse_expires(bad).is_err(), "'{bad}' must be refused");
    }
}

/// One membership, deserialized through the same path the flow uses, so these
/// tests cannot drift from the wire shape.
fn parsed(org: &str, workspace: &str, display: Option<&str>) -> super::control_plane::Membership {
    let mut value = membership(org, workspace, "active", None);
    match display {
        Some(name) => value["display_name"] = json!(name),
        None => {
            value.as_object_mut().unwrap().remove("display_name");
        }
    }
    serde_json::from_value(value).unwrap()
}

/// §6.1 step 5: "a lone membership is used automatically; several trigger a
/// picker". The picker reads its answer from a parameter, so all three of its
/// outcomes are exercised here rather than by hand.
#[test]
fn the_picker_resolves_a_valid_choice() {
    let active = vec![parsed("acme", "one", None), parsed("acme", "two", None)];
    let chosen = select_memberships(&Selection::Auto, active, &[], &mut &b"2\n"[..]).unwrap();
    assert_eq!(chosen.len(), 1);
    assert_eq!(chosen[0].tenant_slug, "two");
}

/// EOF is a non-interactive run — a pipe, a CI job, `< /dev/null`. Looping on
/// an answer that will never come would hang the command, so it names the
/// flags that do the same job.
#[test]
fn the_picker_treats_eof_as_non_interactive_and_names_the_flags() {
    let active = vec![parsed("acme", "one", None), parsed("acme", "two", None)];
    let err = select_memberships(&Selection::Auto, active, &[], &mut &b""[..])
        .unwrap_err()
        .to_string();
    assert!(err.contains("--workspace <slug>"), "{err}");
    assert!(err.contains("--all-workspaces"), "{err}");
}

#[test]
fn the_picker_refuses_an_answer_outside_the_range() {
    let active = vec![parsed("acme", "one", None), parsed("acme", "two", None)];
    for answer in ["0\n", "3\n", "two\n", "\n"] {
        let err = select_memberships(
            &Selection::Auto,
            active.clone(),
            &[],
            &mut answer.as_bytes(),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("is not one of 1-2"), "{answer:?}: {err}");
    }
}

/// A lone membership never reaches the picker, so a non-interactive `login`
/// with one workspace works with stdin closed.
#[test]
fn a_lone_membership_skips_the_picker_entirely() {
    let chosen = select_memberships(
        &Selection::Auto,
        vec![parsed("acme", "only", None)],
        &[],
        &mut &b""[..],
    )
    .unwrap();
    assert_eq!(chosen[0].tenant_slug, "only");
}

/// The menu names the workspace, the role, and the control plane's
/// `display_name` when it sent one — and reads cleanly when it did not.
#[test]
fn the_menu_names_each_workspace_with_its_role() {
    let menu = render_workspace_menu(&[
        parsed("acme", "one", Some("Production")),
        parsed("acme", "two", None),
        parsed("acme", "three", Some("")),
    ]);
    assert_eq!(
        menu,
        "this identity has 3 active workspaces:\n\
         \x20 1) acme/one — Production (role: admin)\n\
         \x20 2) acme/two (role: admin)\n\
         \x20 3) acme/three (role: admin)\n\
         select one [1-3]: "
    );
}

/// The browser path through the WHOLE flow, with a `fn` playing the user
/// agent: it reads the authorization URL as a browser would and follows the
/// redirect. What this pins beyond the oauth-level test is the handoff — the
/// control plane is presented the **ID token**, never a PAT.
#[tokio::test]
async fn the_browser_path_presents_the_id_token_to_the_control_plane() {
    let gw = gateway(200).await;
    let cp = control_plane_with(vec![membership(
        "acme",
        "acme-prod",
        "active",
        Some(&gw.uri()),
    )])
    .await;
    mint_ok(&cp, "acme-prod", "tok-1").await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id_token": "the-id-token"})))
        .mount(&cp)
        .await;

    let home = TempDir::new().unwrap();
    let path = config_in(&home);
    let mut options = options(&cp.uri(), None);
    options.identity = None;
    options.client_id = Some("client-123.apps.googleusercontent.com".to_string());
    options.endpoints = oauth::Endpoints {
        authorization_url: format!("{}/auth", cp.uri()),
        token_url: format!("{}/token", cp.uri()),
    };
    options.callback_timeout = std::time::Duration::from_secs(5);
    // The shared helper sets `no_browser`, which prints the URL instead of
    // opening it; this test is specifically about the opening path.
    options.no_browser = false;
    // Stands in for the browser: no captures needed, which is why the field is
    // a plain `fn`.
    options.open_browser = |url: &str| {
        let query = url.split_once('?').expect("a query").1;
        let value = |key: &str| {
            let raw = query
                .split('&')
                .find_map(|pair| pair.strip_prefix(&format!("{key}=")))
                .unwrap_or_default();
            percent_encoding::percent_decode_str(raw)
                .decode_utf8_lossy()
                .to_string()
        };
        let (redirect, state) = (value("redirect_uri"), value("state"));
        tokio::spawn(async move {
            let _ = reqwest::get(format!("{redirect}?code=browser-code&state={state}")).await;
        });
        Ok(())
    };

    let report = login(options, &path).await.unwrap();
    assert_eq!(report.written.len(), 1);

    let requests = cp.received_requests().await.unwrap();
    let discovery = requests
        .iter()
        .find(|r| r.url.path() == "/v1/me/workspaces")
        .expect("discovery happened");
    assert_eq!(
        discovery.headers.get("Authorization").unwrap(),
        "Bearer the-id-token",
        "the control plane is presented the ID token, not a PAT"
    );
    // And the ID token never reaches disk (§9.1).
    let written = std::fs::read_to_string(&path).unwrap();
    assert!(!written.contains("the-id-token"), "{written}");
    assert!(written.contains("skardi_pat_tok-1"), "{written}");

    // The CLI identifies itself to the control plane, and not to the identity
    // provider: the token endpoint is Google's in production, a third party.
    let user_agent = |r: &Request| {
        r.headers
            .get("user-agent")
            .map(|v| v.to_str().unwrap().to_string())
    };
    assert!(
        user_agent(discovery).is_some_and(|ua| ua.starts_with("skardi-cli")),
        "{:?}",
        user_agent(discovery)
    );
    let token = requests
        .iter()
        .find(|r| r.url.path() == "/token")
        .expect("the code was redeemed");
    assert_eq!(user_agent(token), None);
    assert!(!token.headers.contains_key("dnt"));
}

#[test]
fn an_identity_with_no_memberships_at_all_says_so() {
    let err = select_memberships(&Selection::Auto, vec![], &[], &mut &b""[..])
        .unwrap_err()
        .to_string();
    assert!(err.contains("no workspace memberships"), "{err}");
}

/// §6.5: "A revocation failure during replacement is reported but does not
/// fail the login — the new context is already good."
#[tokio::test]
async fn a_replacement_whose_old_token_cannot_be_revoked_still_succeeds() {
    let gw = gateway(200).await;
    let cp = control_plane_with(vec![membership(
        "acme",
        "acme-prod",
        "active",
        Some(&gw.uri()),
    )])
    .await;
    mint_ok(&cp, "acme-prod", "tok-new").await;
    revoke_answers(&cp, 500).await;

    let home = TempDir::new().unwrap();
    let path = config_in(&home);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        "contexts:\n\
         \x20 - name: acme/acme-prod\n\
         \x20   mode: cloud\n\
         \x20   server: http://old.example\n\
         \x20   workspace: acme-prod\n\
         \x20   token: skardi_pat_old\n\
         \x20   token-id: tok-old\n",
    )
    .unwrap();

    let report = login(options(&cp.uri(), None), &path).await.unwrap();

    assert_eq!(report.written.len(), 1, "the login itself succeeded");
    assert!(report.replaced_revoked.is_empty());
    assert_eq!(report.revoke_failures.len(), 1);
    assert_eq!(report.revoke_failures[0].0, "tok-old");
    assert_eq!(read_yaml(&path)["contexts"][0]["token-id"], "tok-new");
}

/// `org_ambiguous` without the org list still explains itself rather than
/// rendering an empty parenthesis.
#[tokio::test]
async fn a_multi_org_failure_without_the_list_still_explains_itself() {
    let gw = gateway(200).await;
    let cp = control_plane_with(vec![membership(
        "acme",
        "acme-prod",
        "active",
        Some(&gw.uri()),
    )])
    .await;
    mint_fails(
        &cp,
        "acme-prod",
        400,
        json!({"error": {"code": "org_ambiguous", "message": "several orgs"}}),
    )
    .await;

    let home = TempDir::new().unwrap();
    let err = login(options(&cp.uri(), None), &config_in(&home))
        .await
        .unwrap_err();
    let rendered = format!("{err:#}");
    assert!(rendered.contains("no org list"), "{rendered}");
    assert!(rendered.contains("config set-context"), "{rendered}");
}

/// The control-plane body cap is defence-in-depth: a runaway endpoint must
/// fail cleanly rather than being buffered.
#[tokio::test]
async fn an_oversized_control_plane_response_is_refused_rather_than_buffered() {
    let cp = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/me/workspaces"))
        .respond_with(ResponseTemplate::new(200).set_body_string("x".repeat(1_100_000)))
        .mount(&cp)
        .await;

    let home = TempDir::new().unwrap();
    let err = login(options(&cp.uri(), None), &config_in(&home))
        .await
        .unwrap_err();
    assert!(
        format!("{err:#}").contains("refusing to buffer it"),
        "{err:#}"
    );
}

/// A gateway that accepts the connection and never answers must not hang the
/// flow: the probe is the last point past which a PAT exists and no context
/// does, so the saga has to stay reachable (round-7 P1).
#[tokio::test]
async fn a_gateway_that_never_answers_still_reaches_the_rollback() {
    let stalled = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/query"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"success": true}))
                .set_delay(std::time::Duration::from_secs(30)),
        )
        .mount(&stalled)
        .await;

    let cp = control_plane_with(vec![membership(
        "acme",
        "acme-prod",
        "active",
        Some(&stalled.uri()),
    )])
    .await;
    mint_ok(&cp, "acme-prod", "tok-1").await;
    revoke_answers(&cp, 204).await;

    let home = TempDir::new().unwrap();
    let path = config_in(&home);
    let started = std::time::Instant::now();
    let err = login(options(&cp.uri(), None), &path)
        .await
        .unwrap_err()
        .to_string();

    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "the probe must be bounded, not wait out the gateway"
    );
    assert!(err.contains("got no answer from"), "{err}");
    assert!(err.contains("--no-verify"), "{err}");
    // The saga ran: the PAT this login minted is revoked, and no context was
    // written to point at it.
    assert_eq!(
        revoked_ids(&cp.received_requests().await.unwrap()),
        ["tok-1"]
    );
    assert!(!path.exists());
}

/// A blank value must FALL THROUGH, not shadow: `Option::or` picks the first
/// `Some` and would trim only the winner, so an exported-empty
/// `SKARDI_GATEWAY_URL` blamed the control plane for a silence it did not
/// cause. `resolve_control_plane` has always had the right shape; this side
/// only ever saw populated values.
#[tokio::test]
async fn a_blank_flag_or_env_does_not_shadow_the_control_planes_gateway_url() {
    for (server_override, env) in [
        (Some(String::new()), None),
        (Some("   ".to_string()), None),
        (None, Some(String::new())),
        (Some(String::new()), Some("  ".to_string())),
    ] {
        let gw = gateway(200).await;
        let cp = control_plane_with(vec![membership(
            "acme",
            "acme-prod",
            "active",
            Some(&gw.uri()),
        )])
        .await;
        mint_ok(&cp, "acme-prod", "tok-1").await;

        let home = TempDir::new().unwrap();
        let path = config_in(&home);
        let mut options = options(&cp.uri(), server_override.as_deref());
        options.env_gateway_url = env.clone();
        login(options, &path).await.unwrap();

        assert_eq!(
            read_yaml(&path)["contexts"][0]["server"].as_str().unwrap(),
            gw.uri(),
            "blank override {server_override:?}/{env:?} shadowed the membership"
        );
    }
}

/// `--expires` bounded by `TimeDelta`'s range still overflowed `DateTime`'s,
/// and `DateTime + TimeDelta` PANICS. The refusal must be typed, and must land
/// before anything is minted.
#[tokio::test]
async fn an_expiry_past_the_end_of_time_is_refused_not_a_panic() {
    let gw = gateway(200).await;
    let cp = control_plane_with(vec![membership(
        "acme",
        "acme-prod",
        "active",
        Some(&gw.uri()),
    )])
    .await;
    mint_ok(&cp, "acme-prod", "tok-1").await;

    let home = TempDir::new().unwrap();
    let path = config_in(&home);
    let mut options = options(&cp.uri(), None);
    // Accepted by `parse_expires` (TimeDelta's range is ~1.07e11 days) and far
    // past `NaiveDate::MAX` (~9.5e7 days from now).
    options.expires = parse_expires("100000000d").unwrap();

    let err = login(options, &path).await.unwrap_err().to_string();

    assert!(err.contains("longer than any usable credential"), "{err}");
    assert!(!path.exists());
    assert!(
        mint_bodies(&cp.received_requests().await.unwrap()).is_empty(),
        "the refusal must precede the mint"
    );
}

/// The probe's failure goes through §8's translation: `ApiError`'s own 401
/// tells the caller to set `SKARDI_API_TOKEN`, which resolution REFUSES for a
/// cloud context — and it is worse advice here than at query time, since there
/// is not yet a context for `skardi login` to point at.
#[tokio::test]
async fn a_rejected_probe_does_not_advise_the_env_var_it_refuses() {
    let gw = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/query"))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({
            "success": false,
            "error": "invalid token",
            "error_type": "unauthorized",
        })))
        .mount(&gw)
        .await;
    let cp = control_plane_with(vec![membership(
        "acme",
        "acme-prod",
        "active",
        Some(&gw.uri()),
    )])
    .await;
    mint_ok(&cp, "acme-prod", "tok-1").await;
    revoke_answers(&cp, 204).await;

    let home = TempDir::new().unwrap();
    let path = config_in(&home);
    let err = login(options(&cp.uri(), None), &path)
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("could not query"), "{err}");
    assert!(!err.contains("SKARDI_API_TOKEN"), "{err}");
    assert!(
        err.contains("skardi login"),
        "the actionable half survives: {err}"
    );
    // Still the saga: revoked, and nothing written.
    assert_eq!(
        revoked_ids(&cp.received_requests().await.unwrap()),
        ["tok-1"]
    );
    assert!(!path.exists());
}

// ── The console-brokered flow (design 2026-09-08) ────────────────────────
//
// Driven against a wiremock CONSOLE rather than a control plane, because that
// is what the CLI talks to on this path: skardi-global has no public ingress
// in any deployment we run, which is the reason the flow exists. The two
// session-less routes are forwarded by the console's BFF under
// `/api/global/v1/cli-login/*`.

/// A console whose exchange answers `body` with `status`.
///
/// The open call always succeeds — every case below is about what happens
/// after a request exists.
async fn console(status: u16, exchange_body: Value) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/requests"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "request_id": "7f3a9c0000000000000000000000abcd",
            "confirm": "7f3a9c",
            // Three seconds past `options()`'s frozen clock, not the real
            // five minutes: the deadline is derived from THIS value, so the
            // production TTL here would make the give-up test sleep for five
            // actual minutes.
            "expires_at": "2026-08-24T12:00:03Z",
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/exchange"))
        .respond_with(ResponseTemplate::new(status).set_body_json(exchange_body))
        .mount(&server)
        .await;
    server
}

/// An approved exchange for one workspace, with the membership §11.2 sends
/// along.
fn approved(workspace: &str, gateway_url: &str, token_id: &str) -> Value {
    json!({
        "status": "approved",
        "token": format!("skardi_pat_{token_id}"),
        "token_id": token_id,
        "expires_at": "2026-11-22T12:00:00Z",
        "workspace": workspace,
        "memberships": [membership("acme", workspace, "active", Some(gateway_url))],
    })
}

/// A config file already holding a cloud context for `acme/my-ws`, so the
/// replacement rule has something to replace.
fn existing_cloud_context() -> String {
    [
        "contexts:",
        "  - name: acme/my-ws",
        "    mode: cloud",
        "    server: http://gw.example",
        "    workspace: my-ws",
        "    token: skardi_pat_old",
        "    token-id: tok-old",
    ]
    .join("\n")
        + "\n"
}

/// The brokered path's own options: no identity and no client id, which is
/// what selects it (§9).
fn broker_options(console_base: &str) -> LoginOptions {
    let mut options = options(console_base, None);
    options.identity = None;
    options.client_id = None;
    options
}

#[tokio::test]
async fn a_browser_login_writes_the_context_from_the_token_the_console_brokered() {
    let gw = gateway(200).await;
    let console = console(200, approved("my-ws", &gw.uri(), "tok-1")).await;
    let home = TempDir::new().unwrap();

    let report = login(broker_options(&console.uri()), &config_in(&home))
        .await
        .unwrap();

    assert_eq!(report.written.len(), 1, "one approval, one workspace");
    let written = &report.written[0];
    assert_eq!(written.name, "acme/my-ws");
    assert_eq!(written.workspace, "my-ws");
    assert_eq!(report.current_context.as_deref(), Some("acme/my-ws"));

    // No mint and no membership listing: the console already did both, which
    // is what makes this path work with no client id and no IdP.
    let seen = console.received_requests().await.unwrap();
    assert!(
        mint_bodies(&seen).is_empty(),
        "the CLI must not mint on this path"
    );
    assert!(
        !seen.iter().any(|r| r.url.path() == "/v1/me/workspaces"),
        "memberships ride along with the exchange (§11.2)"
    );

    // The context holds the brokered token AND its id — the id is what a later
    // login names when it replaces this credential.
    let file = read_yaml(&config_in(&home));
    let context = &file["contexts"][0];
    assert_eq!(context["token"].as_str(), Some("skardi_pat_tok-1"));
    assert_eq!(context["token-id"].as_str(), Some("tok-1"));
}

#[tokio::test]
async fn the_open_call_carries_the_pkce_challenge_the_expiry_and_the_machine_name() {
    let gw = gateway(200).await;
    let console = console(200, approved("my-ws", &gw.uri(), "tok-1")).await;
    let home = TempDir::new().unwrap();
    login(broker_options(&console.uri()), &config_in(&home))
        .await
        .unwrap();

    let seen = console.received_requests().await.unwrap();
    let opened: Value = seen
        .iter()
        .find(|r| r.url.path() == "/api/global/v1/cli-login/requests")
        .map(|r| serde_json::from_slice(&r.body).unwrap())
        .expect("the flow opens a request");

    // 43 characters is RFC 7636's floor and what `Pkce` produces; the server
    // refuses anything outside 43..=128.
    let challenge = opened["challenge"].as_str().unwrap();
    assert_eq!(challenge.len(), 43, "an S256 challenge: {challenge}");
    // `--expires` has to ride the OPEN call: the mint happens minutes later in
    // a browser that never sees the flag.
    assert_eq!(
        opened["token_expires_at"].as_str(),
        Some("2026-11-22T12:00:00+00:00"),
        "90 days from the injected clock"
    );
    // What the consent screen shows, and what the PAT is named upstream — one
    // name per machine whichever flow wrote it.
    assert_eq!(opened["client_desc"].as_str(), Some("cli@test-host"));

    // And the exchange proves ownership with the verifier, never the challenge.
    let exchanged: Value = seen
        .iter()
        .find(|r| r.url.path() == "/api/global/v1/cli-login/exchange")
        .map(|r| serde_json::from_slice(&r.body).unwrap())
        .expect("the flow polls the exchange");
    let verifier = exchanged["verifier"].as_str().unwrap();
    assert_ne!(verifier, challenge, "the verifier is not the challenge");
    assert_eq!(
        exchanged["request_id"].as_str(),
        Some("7f3a9c0000000000000000000000abcd")
    );
}

#[rstest]
#[case::workspace(Selection::Named("other".to_string()), "--workspace 'other'")]
#[case::all_workspaces(Selection::All, "--all-workspaces")]
#[tokio::test]
async fn a_browser_login_refuses_the_flags_it_cannot_honour(
    #[case] selection: Selection,
    #[case] expected: &str,
) {
    // The consent screen grants exactly one workspace (§11.1), so neither flag
    // has an honest reading here — and quietly logging into one workspace when
    // `--all-workspaces` was asked for is a success discovered much later.
    let home = TempDir::new().unwrap();
    let mut options = broker_options("http://127.0.0.1:1");
    options.selection = selection;
    let err = login(options, &config_in(&home))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains(expected), "{err}");
    // Points at the flows that CAN do it, rather than only refusing. Both are
    // named, because `--identity` opts out of the broker too.
    assert!(err.contains("--client-id"), "{err}");
    assert!(err.contains("--identity"), "{err}");
}

#[tokio::test]
async fn a_console_without_the_cli_login_route_says_so_and_names_the_alternative() {
    // What an older console looks like from here: its BFF allowlist has no
    // CLI-login entry, so its own proxy answers 403. Nothing the user retypes
    // fixes it, so the message has to name the flag that still works.
    let console = console(
        403,
        json!({ "error": { "code": "route_not_allowed",
        "message": "this endpoint is not part of the console surface" } }),
    )
    .await;
    let home = TempDir::new().unwrap();
    let err = login(broker_options(&console.uri()), &config_in(&home))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("does not support browser-brokered"), "{err}");
    assert!(err.contains("--client-id"), "{err}");
}

#[tokio::test]
async fn an_expired_or_redeemed_request_tells_the_user_to_run_login_again() {
    // One server answer covers expired, unknown and already-redeemed —
    // deliberately, so the endpoint cannot enumerate live ids — so the CLI has
    // to translate it into the action that fixes all three.
    let console = console(
        404,
        json!({ "error": { "code": "no_such_request",
        "message": "no such CLI login request" } }),
    )
    .await;
    let home = TempDir::new().unwrap();
    let err = login(broker_options(&console.uri()), &config_in(&home))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("no longer open"), "{err}");
    assert!(err.contains("skardi login"), "{err}");
}

#[tokio::test]
async fn nobody_approving_expires_with_the_deadline_named() {
    let console = console(200, json!({ "status": "pending" })).await;
    let home = TempDir::new().unwrap();
    let err = login(broker_options(&console.uri()), &config_in(&home))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("nobody approved this login"), "{err}");
    // Nothing was written, and there is no credential to warn about — the
    // exchange never handed one over.
    assert!(
        !config_in(&home).exists(),
        "no context on an unapproved login"
    );
    assert!(!err.contains("Revoke token"), "nothing was minted: {err}");
}

/// The give-up deadline comes from the SERVER's request expiry, not from the
/// local OAuth callback timeout.
///
/// The two differ by design — a brokered request lives five minutes, the
/// loopback listener waits 120 seconds — so using the local one abandoned the
/// poll while the request was still open and told the human they had not
/// approved in time when they had minutes left. `options()` sets a 50ms
/// callback timeout and the console reports an expiry 3s past the frozen
/// clock, so the reported budget names which one was used.
#[tokio::test]
async fn the_poll_deadline_comes_from_the_request_expiry_not_the_callback_timeout() {
    let console = console(200, json!({ "status": "pending" })).await;
    let home = TempDir::new().unwrap();
    let options = broker_options(&console.uri());
    assert_eq!(
        options.callback_timeout,
        std::time::Duration::from_millis(50),
        "the fixture must make the two budgets distinguishable"
    );

    let err = login(options, &config_in(&home))
        .await
        .unwrap_err()
        .to_string();
    // 3s is the console's expiry; the callback timeout would render as 0s.
    assert!(err.contains("within 3s"), "{err}");
    assert!(!err.contains("within 0s"), "{err}");
}

/// A brokered login records the console under `console:`, leaving
/// `control-plane:` alone.
///
/// They are different services: `ControlPlane` appends bare `/v1/me/...`,
/// while a console serves that API under `/api/global/v1/...` and only to a
/// browser holding a session. Recording a console URL as `control-plane`
/// poisoned every later direct call — a `logout --revoke` cleared the local
/// credential and then sent `DELETE /v1/me/tokens/{id}` at the console, which
/// replied with its own 404 page.
#[tokio::test]
async fn a_brokered_login_records_the_console_url_under_its_own_key() {
    let gw = gateway(200).await;
    let console = console(200, approved("my-ws", &gw.uri(), "tok-1")).await;
    let home = TempDir::new().unwrap();
    login(broker_options(&console.uri()), &config_in(&home))
        .await
        .unwrap();

    let file = read_yaml(&config_in(&home));
    assert_eq!(file["console"].as_str(), Some(console.uri().as_str()));
    assert!(
        file["control-plane"].is_null(),
        "a console URL must not become the control-plane API base: {file:?}"
    );
}

#[tokio::test]
async fn a_brokered_credential_that_cannot_query_is_reported_by_id_not_revoked() {
    // The saga's counterpart revokes here. This path cannot: a PAT
    // authenticates to the gateway, and skardi-global's `/v1/*` does not
    // accept one — so the id is the only thing standing between the user and
    // a live credential nobody knows exists.
    let gw = gateway(403).await;
    let console = console(200, approved("my-ws", &gw.uri(), "tok-live")).await;
    let home = TempDir::new().unwrap();

    let err = login(broker_options(&console.uri()), &config_in(&home))
        .await
        .unwrap_err();
    let rendered = format!("{err:#}");
    assert!(rendered.contains("could not query"), "{rendered}");
    assert!(rendered.contains("Revoke token tok-live"), "{rendered}");
    assert!(rendered.contains("cannot revoke"), "{rendered}");
    // And no context was written, so the failure is not half-committed.
    assert!(!config_in(&home).exists(), "{rendered}");
}

#[tokio::test]
async fn a_brokered_relogin_reports_the_replaced_token_it_cannot_revoke() {
    let gw = gateway(200).await;
    let console = console(200, approved("my-ws", &gw.uri(), "tok-2")).await;
    let home = TempDir::new().unwrap();
    let path = config_in(&home);

    // A context already holding a credential from an earlier login.
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, existing_cloud_context()).unwrap();

    let report = login(broker_options(&console.uri()), &config_in(&home))
        .await
        .unwrap();

    // Nothing was revoked, and nothing pretends to have been: the old id is
    // reported through the same channel a FAILED revoke uses, because the
    // outcome for the user is identical — go revoke it in the console.
    assert!(report.replaced_revoked.is_empty());
    assert_eq!(report.revoke_failures.len(), 1);
    let (token_id, reason) = &report.revoke_failures[0];
    assert_eq!(token_id, "tok-old");
    assert!(reason.contains("cannot revoke"), "{reason}");
    assert!(
        revoked_ids(&console.received_requests().await.unwrap()).is_empty(),
        "the CLI must not attempt a revoke it cannot authenticate"
    );

    // The new credential did land.
    let file = read_yaml(&path);
    assert_eq!(file["contexts"][0]["token-id"].as_str(), Some("tok-2"));
}

#[tokio::test]
async fn keep_old_token_reports_the_replacement_as_kept_rather_than_stranded() {
    let gw = gateway(200).await;
    let console = console(200, approved("my-ws", &gw.uri(), "tok-2")).await;
    let home = TempDir::new().unwrap();
    let path = config_in(&home);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, existing_cloud_context()).unwrap();

    let mut options = broker_options(&console.uri());
    options.keep_old_token = true;
    let report = login(options, &config_in(&home)).await.unwrap();

    // Asked for deliberately, so it is not a warning — an agent mid-task is
    // still holding the old credential.
    assert_eq!(report.replaced_kept, vec!["tok-old".to_string()]);
    assert!(report.revoke_failures.is_empty());
}

#[tokio::test]
async fn an_approval_with_no_membership_is_fatal_and_names_the_live_token() {
    // Exceptional: the approved membership vanished between approval and
    // redemption. Fatal because the context name is `<org>/<workspace>` and
    // the org slug has nowhere else to come from — and by this point a
    // credential exists that this CLI cannot revoke.
    let console = console(
        200,
        json!({
            "status": "approved",
            "token": "skardi_pat_orphan",
            "token_id": "tok-orphan",
            "workspace": "my-ws",
            "memberships": [],
        }),
    )
    .await;
    let home = TempDir::new().unwrap();
    let err = login(broker_options(&console.uri()), &config_in(&home))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("no membership"), "{err}");
    assert!(err.contains("revoke token tok-orphan"), "{err}");
}

#[tokio::test]
async fn an_unknown_exchange_status_is_reported_rather_than_polled_forever() {
    // A control plane newer than this CLI. Treating an unrecognised terminal
    // state as "pending" would spin until the deadline and then blame the
    // human for not approving.
    let console = console(200, json!({ "status": "superseded" })).await;
    let home = TempDir::new().unwrap();
    let err = login(broker_options(&console.uri()), &config_in(&home))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("unexpected status"), "{err}");
    assert!(err.contains("older than the control plane"), "{err}");
}

/// `pending → clean gone` is an EXPIRY, and must not claim a credential exists.
///
/// This INVERTS an assertion from the previous review round, because its
/// premise was wrong. A credential can only exist if the exchange committed; a
/// clean `gone` says it did not consume on that call, and `pending` says the
/// same about the call before. Only this process holds the PKCE verifier, so
/// nobody else could have redeemed the request either. What is left is the
/// request expiring between two polls — exactly what the plain message says.
///
/// Warning here would send the reader hunting through Agent access for a token
/// that cannot exist. The real ambiguity is keyed on a lost RESPONSE, covered
/// by the two cases below.
#[tokio::test]
async fn a_request_that_expires_between_polls_is_not_reported_as_ambiguous() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/requests"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "request_id": "7f3a9c0000000000000000000000abcd",
            "confirm": "7f3a9c",
            "expires_at": "2026-08-24T12:00:03Z",
        })))
        .mount(&server)
        .await;
    // Pending once, then gone — with every response arriving cleanly, so
    // nothing was ever lost in transit.
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/exchange"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "status": "pending" })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/exchange"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": { "code": "no_such_request", "message": "no such CLI login request" }
        })))
        .mount(&server)
        .await;

    let home = TempDir::new().unwrap();
    let err = login(broker_options(&server.uri()), &config_in(&home))
        .await
        .unwrap_err();
    let rendered = format!("{err:#}");
    assert!(rendered.contains("no longer open"), "{rendered}");
    assert!(
        !rendered.contains("a credential may have been minted"),
        "no response was lost, so nothing could have been minted: {rendered}"
    );
}

/// A malformed SUCCESS body on the exchange is treated as a lost response.
///
/// The worst case this guards: the server consumed the request and minted the
/// PAT, and the only copy of that token was in a body that will not parse.
/// Reported as a clean error the loop exits silently; marked as transport it is
/// retried, and the `no_such_request` that follows — because the request really
/// was consumed — is reported as the ambiguity it is.
#[tokio::test]
async fn a_truncated_exchange_body_then_gone_warns_a_credential_may_exist() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/requests"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "request_id": "7f3a9c0000000000000000000000abcd",
            "confirm": "7f3a9c",
            "expires_at": "2026-08-24T12:00:03Z",
        })))
        .mount(&server)
        .await;
    // 200 with a truncated body: the mint committed, the token is unreadable.
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/exchange"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{\"status\":\"appro"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/exchange"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": { "code": "no_such_request", "message": "no such CLI login request" }
        })))
        .mount(&server)
        .await;

    let home = TempDir::new().unwrap();
    let err = login(broker_options(&server.uri()), &config_in(&home))
        .await
        .unwrap_err();
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("a credential may have been minted"),
        "{rendered}"
    );
    assert!(rendered.contains("Agent access"), "{rendered}");
    assert!(rendered.contains("no longer open"), "{rendered}");
}

/// Approval landing BEFORE the first poll, with that first response lost.
///
/// The hole the previous fix left: the flag was set only by a `pending`
/// answer, so a run that never sees one — approval already done when the first
/// poll goes out — kept it false and reported a real ambiguity as a plain
/// "rerun login". Keying on the lost response covers it, because there is no
/// earlier poll to have seen anything.
#[tokio::test]
async fn a_lost_first_response_then_gone_warns_without_ever_seeing_pending() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/requests"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "request_id": "7f3a9c0000000000000000000000abcd",
            "confirm": "7f3a9c",
            "expires_at": "2026-08-24T12:00:03Z",
        })))
        .mount(&server)
        .await;
    // The very FIRST exchange: committed server-side, answer unreadable.
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/exchange"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/exchange"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": { "code": "no_such_request", "message": "no such CLI login request" }
        })))
        .mount(&server)
        .await;

    let home = TempDir::new().unwrap();
    let err = login(broker_options(&server.uri()), &config_in(&home))
        .await
        .unwrap_err();
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("a credential may have been minted"),
        "no `pending` was ever seen, and the warning must still fire: {rendered}"
    );
}

/// A first poll that finds the request already gone is NOT the ambiguous case.
///
/// Nothing was ever approved through this run, so claiming a credential might
/// exist would send the reader hunting for one that does not.
#[tokio::test]
async fn a_request_gone_on_the_first_poll_does_not_claim_a_credential_exists() {
    let console = console(
        404,
        json!({ "error": { "code": "no_such_request",
        "message": "no such CLI login request" } }),
    )
    .await;
    let home = TempDir::new().unwrap();
    let err = login(broker_options(&console.uri()), &config_in(&home))
        .await
        .unwrap_err();
    let rendered = format!("{err:#}");
    assert!(rendered.contains("no longer open"), "{rendered}");
    assert!(
        !rendered.contains("a credential may have been minted"),
        "{rendered}"
    );
}

/// An unparseable request expiry falls back to the local window rather than
/// failing the login: a console that changes its timestamp format should
/// shorten this loop, not break signing in.
#[tokio::test]
async fn an_unparseable_request_expiry_falls_back_to_the_local_window() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/requests"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "request_id": "7f3a9c0000000000000000000000abcd",
            "confirm": "7f3a9c",
            "expires_at": "next tuesday",
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/exchange"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "status": "pending" })))
        .mount(&server)
        .await;

    let home = TempDir::new().unwrap();
    let err = login(broker_options(&server.uri()), &config_in(&home))
        .await
        .unwrap_err()
        .to_string();
    // `options()`'s 50ms callback timeout renders as 0s — the local window,
    // not a parsed one.
    assert!(err.contains("nobody approved this login"), "{err}");
    assert!(err.contains("within 0s"), "{err}");
}

/// A browser that will not launch is not a failure: the URL is already on
/// screen, and a headless box is the normal case rather than an error.
#[tokio::test]
async fn a_browser_that_cannot_be_opened_does_not_fail_the_login() {
    let gw = gateway(200).await;
    let console = console(200, approved("my-ws", &gw.uri(), "tok-1")).await;
    let home = TempDir::new().unwrap();
    let mut options = broker_options(&console.uri());
    // `no_browser` off, so the launch is attempted — and it refuses.
    options.no_browser = false;
    options.open_browser = |_| {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "no browser here",
        ))
    };

    let report = login(options, &config_in(&home)).await.unwrap();
    assert_eq!(report.written.len(), 1, "the login still completed");
}

/// A console that answers the exchange with no recognizable error body still
/// produces a message naming the status, not an empty one.
#[tokio::test]
async fn an_unrecognizable_refusal_still_names_the_status() {
    let console = console(500, json!({ "unexpected": "shape" })).await;
    let home = TempDir::new().unwrap();
    let err = login(broker_options(&console.uri()), &config_in(&home))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("HTTP 500"), "{err}");
    assert!(err.contains("refused the CLI-login request"), "{err}");
}

/// An oversized exchange body is refused by the read cap, and that counts as
/// a TRANSPORT failure — so it is retried rather than ending the login, and
/// exhausting the retries says the console stopped answering.
///
/// One test for two guards: the 1MiB ceiling that stops a runaway endpoint
/// exhausting memory, and the retry loop that survives a console restarting
/// mid-approval.
#[tokio::test]
async fn an_oversized_exchange_body_is_capped_retried_and_then_reported() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/requests"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "request_id": "7f3a9c0000000000000000000000abcd",
            "confirm": "7f3a9c",
            "expires_at": "2026-08-24T12:00:03Z",
        })))
        .mount(&server)
        .await;
    // Comfortably past the 1MiB cap.
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/exchange"))
        .respond_with(ResponseTemplate::new(200).set_body_string("x".repeat(1_200_000)))
        .mount(&server)
        .await;

    let home = TempDir::new().unwrap();
    let err = login(broker_options(&server.uri()), &config_in(&home))
        .await
        .unwrap_err();
    let rendered = format!("{err:#}");
    assert!(rendered.contains("stopped answering"), "{rendered}");
    assert!(rendered.contains("refusing to buffer it"), "{rendered}");
}

/// A config write that cannot land strands the credential, and says so by id.
///
/// The exchange already committed, so a PAT exists — and a browser login
/// cannot revoke it. Pointing the config at a path whose parent is a FILE
/// makes the write fail the way a read-only home would.
#[tokio::test]
async fn a_config_write_that_fails_reports_the_stranded_credential_by_id() {
    let gw = gateway(200).await;
    let console = console(200, approved("my-ws", &gw.uri(), "tok-stranded")).await;
    let home = TempDir::new().unwrap();
    let blocker = home.path().join(".skardi");
    std::fs::write(&blocker, "not a directory").unwrap();

    let err = login(broker_options(&console.uri()), &blocker.join("config.yaml"))
        .await
        .unwrap_err();
    let rendered = format!("{err:#}");
    assert!(rendered.contains("Revoke token tok-stranded"), "{rendered}");
    assert!(rendered.contains("cannot revoke"), "{rendered}");
}

/// A reused request id is refused by PKCE, and the message says to start over
/// rather than to retype anything.
#[tokio::test]
async fn a_verifier_mismatch_tells_the_user_to_start_a_fresh_login() {
    let console = console(
        403,
        json!({ "error": { "code": "verifier_mismatch",
        "message": "the PKCE verifier does not match this request" } }),
    )
    .await;
    let home = TempDir::new().unwrap();
    let err = login(broker_options(&console.uri()), &config_in(&home))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("proof of ownership"), "{err}");
    assert!(err.contains("start a fresh"), "{err}");
}

/// An error envelope carrying a code but no message still names the status,
/// rather than rendering an empty sentence.
#[tokio::test]
async fn a_refusal_with_no_message_names_the_status_instead() {
    let console = console(500, json!({ "error": { "code": "internal" } })).await;
    let home = TempDir::new().unwrap();
    let err = login(broker_options(&console.uri()), &config_in(&home))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("HTTP 500"), "{err}");
    assert!(!err.contains(": (HTTP"), "no empty message clause: {err}");
}

/// A console that opens a request with no id is refused, rather than polling
/// an exchange that can never match.
#[tokio::test]
async fn an_opened_request_with_no_id_is_refused() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/requests"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "request_id": "",
            "confirm": "",
            "expires_at": "2026-08-24T12:00:03Z",
        })))
        .mount(&server)
        .await;

    let home = TempDir::new().unwrap();
    let err = login(broker_options(&server.uri()), &config_in(&home))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("no id"), "{err}");
}

/// An exchange answering 200 with an empty body is an unknown state, not a
/// pending one — polling forever on it would blame the human for not
/// approving.
#[tokio::test]
async fn an_empty_exchange_body_is_reported_as_an_unexpected_status() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/requests"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "request_id": "7f3a9c0000000000000000000000abcd",
            "confirm": "7f3a9c",
            "expires_at": "2026-08-24T12:00:03Z",
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/exchange"))
        .respond_with(ResponseTemplate::new(200).set_body_string(""))
        .mount(&server)
        .await;

    let home = TempDir::new().unwrap();
    let err = login(broker_options(&server.uri()), &config_in(&home))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("unexpected status"), "{err}");
}

#[test]
fn the_console_broker_is_the_default_and_either_credential_flag_opts_out() {
    let base = |identity: Option<&str>, client_id: Option<&str>| {
        let mut options = options("http://127.0.0.1:1", None);
        options.identity = identity.map(str::to_string);
        options.client_id = client_id.map(str::to_string);
        options
    };
    // §9: with neither flag, the console brokers — which is what makes
    // `skardi login --control-plane <url>` work with no other argument.
    assert!(super::uses_console_broker(&base(None, None)));
    // An empty client id counts as absent, the same way `acquire_bearer`
    // treats it: `SKARDI_OAUTH_CLIENT_ID=` exported into a shell must not
    // select a path that then fails for want of that variable's value.
    assert!(super::uses_console_broker(&base(None, Some("   "))));
    // Either flag names a way for the CLI to authenticate itself, and keeps
    // the existing flow.
    assert!(!super::uses_console_broker(&base(
        None,
        Some("client-123.apps.googleusercontent.com")
    )));
    assert!(!super::uses_console_broker(&base(Some("dev:alice"), None)));
}

// ─── The token pair (agent onboarding §2.1, §2.5) ──────────────────────────

/// An exchange approved WITH `control`: the pair beside the token every CLI
/// reads, and the MCP URL on the membership.
fn approved_with_control(gateway_url: &str, token_id: &str, pair_id: &str) -> Value {
    let mut body = approved("my-ws", gateway_url, token_id);
    body["control_token"] = json!("skardi_pat_control_secret");
    body["control_token_id"] = json!("tok-control");
    body["control_expires_at"] = json!("2026-08-25T12:00:00Z");
    body["pair_id"] = json!(pair_id);
    body["memberships"][0]["mcp_url"] = json!(format!("{gateway_url}/mcp"));
    body
}

fn opened_body(requests: &[Request]) -> Value {
    requests
        .iter()
        .find(|r| r.url.path() == "/api/global/v1/cli-login/requests")
        .map(|r| serde_json::from_slice(&r.body).unwrap())
        .expect("the flow opens a request")
}

fn control_options(console_base: &str) -> LoginOptions {
    let mut options = broker_options(console_base);
    options.control = true;
    options
}

#[tokio::test]
async fn a_control_login_asks_for_control_and_stores_the_pair() {
    let gw = gateway(200).await;
    let console = console(200, approved_with_control(&gw.uri(), "tok-mcp", "pair-1")).await;
    let home = TempDir::new().unwrap();

    let report = login(control_options(&console.uri()), &config_in(&home))
        .await
        .unwrap();

    let opened = opened_body(&console.received_requests().await.unwrap());
    assert_eq!(opened["control"], json!(true));

    // The existing `token` key holds the mcp_token, so every data-plane
    // command — and every older CLI — keeps working unchanged (§2.4.2).
    let file = read_yaml(&config_in(&home));
    let context = &file["contexts"][0];
    assert_eq!(context["token"].as_str(), Some("skardi_pat_tok-mcp"));
    assert_eq!(context["token-id"].as_str(), Some("tok-mcp"));
    assert_eq!(
        context["control-token"].as_str(),
        Some("skardi_pat_control_secret")
    );
    assert_eq!(
        context["control-token-expires-at"].as_str(),
        Some("2026-08-25T12:00:00Z")
    );
    assert_eq!(context["pair-id"].as_str(), Some("pair-1"));
    assert_eq!(
        context["mcp-url"].as_str(),
        Some(format!("{}/mcp", gw.uri()).as_str())
    );

    assert!(!report.control_missing);
    assert_eq!(
        report.control_expires_at.as_deref(),
        Some("2026-08-25T12:00:00Z")
    );
    // The report carries no token value, so printing it cannot leak one.
    assert!(!format!("{report:?}").contains("control_secret"));

    // The post-login probe used the mcp token, never the control token.
    let probe = &gw.received_requests().await.unwrap()[0];
    assert_eq!(
        probe.headers.get("authorization").unwrap(),
        "Bearer skardi_pat_tok-mcp"
    );
}

/// Approved without `control` — the box unticked, or a member's cap: the
/// context is still written so queries work, and the report says control is
/// missing, which the command turns into exit 6.
#[tokio::test]
async fn a_control_login_approved_without_control_writes_the_context_and_reports_it() {
    let gw = gateway(200).await;
    let console = console(200, approved("my-ws", &gw.uri(), "tok-1")).await;
    let home = TempDir::new().unwrap();

    let report = login(control_options(&console.uri()), &config_in(&home))
        .await
        .unwrap();

    assert!(report.control_missing);
    assert_eq!(report.control_expires_at, None);
    let file = read_yaml(&config_in(&home));
    let context = &file["contexts"][0];
    assert_eq!(context["token"].as_str(), Some("skardi_pat_tok-1"));
    assert!(context.get("control-token").is_none(), "{file:?}");
    assert!(context.get("pair-id").is_none(), "{file:?}");
}

/// A plain login sends exactly today's body: a server older than the pair is
/// `deny_unknown_fields`, so even `"control": false` would break it.
#[tokio::test]
async fn a_plain_login_sends_none_of_the_new_fields() {
    let gw = gateway(200).await;
    let console = console(200, approved("my-ws", &gw.uri(), "tok-1")).await;
    let home = TempDir::new().unwrap();
    let report = login(broker_options(&console.uri()), &config_in(&home))
        .await
        .unwrap();
    assert!(!report.control_missing, "control was not asked for");

    let opened = opened_body(&console.received_requests().await.unwrap());
    let keys: Vec<&String> = opened.as_object().unwrap().keys().collect();
    assert_eq!(keys, ["challenge", "client_desc", "token_expires_at"]);
}

#[tokio::test]
async fn setup_rides_the_open_call() {
    let gw = gateway(200).await;
    let console = console(200, approved_with_control(&gw.uri(), "tok-1", "pair-1")).await;
    let home = TempDir::new().unwrap();
    let mut options = control_options(&console.uri());
    options.setup = Some(super::console_broker::Setup {
        profile: "personal".to_string(),
        name: "Alice".to_string(),
        industry: None,
    });
    login(options, &config_in(&home)).await.unwrap();

    let opened = opened_body(&console.received_requests().await.unwrap());
    assert_eq!(
        opened["setup"],
        json!({"profile": "personal", "name": "Alice", "industry": null})
    );
    assert_eq!(opened["control"], json!(true));
}

/// A re-login names the context's pair as `replaces_pair_id`; the exchange
/// revokes it under the approver's session, so the report counts the old
/// token as revoked rather than as one the user must revoke by hand.
#[tokio::test]
async fn a_relogin_sends_replaces_pair_id_and_reports_the_old_pair_revoked() {
    let gw = gateway(200).await;
    let console = console(200, approved_with_control(&gw.uri(), "tok-new", "pair-new")).await;
    let home = TempDir::new().unwrap();
    let path = config_in(&home);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        format!(
            "current-context: acme/my-ws\n{}    pair-id: pair-old\n    control-token: skardi_pat_old_control\n",
            existing_cloud_context()
        ),
    )
    .unwrap();

    let report = login(control_options(&console.uri()), &path).await.unwrap();

    let opened = opened_body(&console.received_requests().await.unwrap());
    assert_eq!(opened["replaces_pair_id"], json!("pair-old"));
    assert_eq!(report.replaced_revoked, vec!["tok-old".to_string()]);
    assert!(report.revoke_failures.is_empty(), "{report:?}");
    assert_eq!(report.replaced_pair_elsewhere, None);

    let raw = std::fs::read_to_string(&path).unwrap();
    assert!(!raw.contains("skardi_pat_old_control"), "{raw}");
    let file = read_yaml(&path);
    assert_eq!(file["contexts"][0]["pair-id"].as_str(), Some("pair-new"));
}

/// A re-login approved WITHOUT control must not leave the previous pair's
/// control token beside the new query token.
#[tokio::test]
async fn a_relogin_without_control_drops_the_previous_control_token() {
    let gw = gateway(200).await;
    let console = console(200, approved("my-ws", &gw.uri(), "tok-new")).await;
    let home = TempDir::new().unwrap();
    let path = config_in(&home);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        format!(
            "{}    pair-id: pair-old\n    control-token: skardi_pat_old_control\n    control-token-expires-at: 2026-08-25T00:00:00Z\n",
            existing_cloud_context()
        ),
    )
    .unwrap();

    login(broker_options(&console.uri()), &path).await.unwrap();

    let raw = std::fs::read_to_string(&path).unwrap();
    assert!(!raw.contains("control-token"), "{raw}");
    assert!(!raw.contains("pair-old"), "{raw}");
}

/// `--keep-old-token` keeps the predecessor pair too: nothing is named for
/// the exchange to revoke.
#[tokio::test]
async fn keep_old_token_names_no_pair_to_replace() {
    let gw = gateway(200).await;
    let console = console(200, approved_with_control(&gw.uri(), "tok-new", "pair-new")).await;
    let home = TempDir::new().unwrap();
    let path = config_in(&home);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        format!(
            "current-context: acme/my-ws\n{}    pair-id: pair-old\n",
            existing_cloud_context()
        ),
    )
    .unwrap();
    let mut options = control_options(&console.uri());
    options.keep_old_token = true;
    let report = login(options, &path).await.unwrap();

    let opened = opened_body(&console.received_requests().await.unwrap());
    assert!(opened.get("replaces_pair_id").is_none(), "{opened}");
    assert_eq!(report.replaced_kept, vec!["tok-old".to_string()]);
}

/// The pair named is the current context's; when the approval lands in a
/// DIFFERENT context, the one whose pair was revoked is named in the report.
#[tokio::test]
async fn a_pair_replaced_in_another_context_is_reported() {
    let gw = gateway(200).await;
    let console = console(200, approved_with_control(&gw.uri(), "tok-new", "pair-new")).await;
    let home = TempDir::new().unwrap();
    let path = config_in(&home);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        "current-context: acme/other\ncontexts:\n  - name: acme/other\n    mode: cloud\n    server: http://gw.example\n    workspace: other\n    token: skardi_pat_other\n    token-id: tok-other\n    pair-id: pair-other\n",
    )
    .unwrap();
    let report = login(control_options(&console.uri()), &path).await.unwrap();
    assert_eq!(
        report.replaced_pair_elsewhere.as_deref(),
        Some("acme/other")
    );
}

/// A server older than this answers the unknown field with `422
/// invalid_body` (skardi-global's JSON extractor) — the design expected
/// `400` — and either one says the server is too old for the flag.
#[rstest]
#[case::unprocessable(422)]
#[case::bad_request(400)]
#[tokio::test]
async fn an_older_server_refusing_the_new_fields_is_reported_as_too_old(#[case] status: u16) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/requests"))
        .respond_with(ResponseTemplate::new(status).set_body_json(json!({
            "error": {"code": "invalid_body", "message": "unknown field `control`"}
        })))
        .mount(&server)
        .await;
    let home = TempDir::new().unwrap();
    let err = login(control_options(&server.uri()), &config_in(&home))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("too old for --control"), "{err}");
    assert!(!config_in(&home).exists());
}

/// Without a new field in the request, the same refusal is not blamed on the
/// server's age.
#[tokio::test]
async fn a_plain_logins_invalid_body_is_not_called_too_old() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/requests"))
        .respond_with(ResponseTemplate::new(422).set_body_json(json!({
            "error": {"code": "invalid_body", "message": "bad challenge"}
        })))
        .mount(&server)
        .await;
    let home = TempDir::new().unwrap();
    let err = login(broker_options(&server.uri()), &config_in(&home))
        .await
        .unwrap_err()
        .to_string();
    assert!(!err.contains("too old"), "{err}");
    assert!(err.contains("bad challenge"), "{err}");
}

/// `--control` and `--setup` are answered on the console's page, which the
/// direct flow never shows.
#[tokio::test]
async fn the_direct_flow_refuses_control_and_setup() {
    let home = TempDir::new().unwrap();
    let mut options = options("http://127.0.0.1:1", None);
    options.control = true;
    let err = login(options, &config_in(&home))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("--control"), "{err}");
}

/// A half-shaped control grant (a skewed rollout: a console or control plane
/// older than the other) is an invalid answer, not "approved without
/// control". The CLI writes nothing, fails non-zero rather than exit 6 (whose
/// recovery would mint yet another pair), and names every id it was given and
/// the page that revokes them — never the token.
#[rstest]
#[case::token_without_pair(
    &["control_token", "control_token_id", "control_expires_at"],
    "a control token with no pair id",
    None
)]
#[case::pair_without_token(&["pair_id"], "a pair id with no control token", Some("pair-1"))]
#[case::id_alone(&["control_token_id"], "control metadata with no control token", None)]
#[tokio::test]
async fn a_half_shaped_control_grant_fails_the_login_and_names_what_to_revoke(
    #[case] fields: &[&str],
    #[case] what: &str,
    #[case] pair: Option<&str>,
) {
    let gw = gateway(200).await;
    let full = approved_with_control(&gw.uri(), "tok-mcp", "pair-1");
    let mut body = approved("my-ws", &gw.uri(), "tok-mcp");
    for field in fields {
        body[*field] = full[*field].clone();
    }
    let console = console(200, body).await;
    let home = TempDir::new().unwrap();

    let err = login(control_options(&console.uri()), &config_in(&home))
        .await
        .unwrap_err();
    let rendered = format!("{err:#}");
    assert!(rendered.contains(what), "{rendered}");
    assert!(
        !rendered.contains("skardi_pat_control_secret"),
        "{rendered}"
    );
    assert!(rendered.contains("query token tok-mcp"), "{rendered}");
    let page = format!("{}/account/agent-access", console.uri());
    match pair {
        Some(pair) => assert!(
            rendered.contains(&format!("{page}?pair={pair}")),
            "{rendered}"
        ),
        None => assert!(rendered.contains(&page), "{rendered}"),
    }
    if fields.contains(&"control_token_id") {
        assert!(rendered.contains("control token tok-control"), "{rendered}");
    }
    // Not an exit-6 "approved without control": a plain failure.
    assert!(err.downcast_ref::<crate::exit::Exit>().is_none());
    assert!(!config_in(&home).exists(), "nothing is written");
}

/// Codex on #311: a `login --control` context overwritten by the DIRECT flow
/// (`--identity`/`--client-id`), which sends no `replaces_pair_id`. Its
/// `token-id` is the pair's mcp token; revoking it revokes the pair on the
/// server, control token included (`revoke_pat` takes the partner). Whatever
/// becomes of that revoke, the pair id is reported — it is the only handle
/// left on the control token once the context is overwritten.
#[rstest]
#[case::revoked(204, false)]
#[case::revoke_failed(500, false)]
#[case::kept(204, true)]
#[tokio::test]
async fn a_pair_context_overwritten_by_the_direct_flow_keeps_its_pair_in_the_report(
    #[case] revoke_status: u16,
    #[case] keep_old: bool,
) {
    let gw = gateway(200).await;
    let cp = control_plane_with(vec![membership(
        "acme",
        "acme-prod",
        "active",
        Some(&gw.uri()),
    )])
    .await;
    mint_ok(&cp, "acme-prod", "tok-new").await;
    revoke_answers(&cp, revoke_status).await;
    let home = TempDir::new().unwrap();
    let path = config_in(&home);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        "console: https://console.example\n\
         current-context: acme/acme-prod\n\
         contexts:\n\
         \x20 - name: acme/acme-prod\n\
         \x20   mode: cloud\n\
         \x20   server: http://old.example\n\
         \x20   workspace: acme-prod\n\
         \x20   token: skardi_pat_old_mcp\n\
         \x20   token-id: tok-old\n\
         \x20   control-token: skardi_pat_old_control\n\
         \x20   control-token-expires-at: 2026-08-25T00:00:00Z\n\
         \x20   pair-id: pair-old\n",
    )
    .unwrap();

    let mut options = options(&cp.uri(), None);
    options.keep_old_token = keep_old;
    let report = login(options, &path).await.unwrap();

    assert_eq!(
        report.replaced_pairs,
        [("tok-old".to_string(), "pair-old".to_string())]
    );
    assert_eq!(report.console.as_deref(), Some("https://console.example"));
    let revoked = revoked_ids(&cp.received_requests().await.unwrap());
    match (revoke_status, keep_old) {
        (_, true) => {
            assert!(revoked.is_empty());
            assert_eq!(report.replaced_kept, ["tok-old"]);
        }
        (204, false) => {
            // The mcp token's id: the server revokes its partner with it.
            assert_eq!(revoked, ["tok-old"]);
            assert_eq!(report.replaced_revoked, ["tok-old"]);
        }
        _ => {
            assert_eq!(report.revoke_failures[0].0, "tok-old");
            assert!(report.replaced_revoked.is_empty());
        }
    }
    // The old control token is no longer on disk either way.
    let raw = std::fs::read_to_string(&path).unwrap();
    assert!(!raw.contains("skardi_pat_old_control"), "{raw}");
}

/// Codex on #311: with `--server` (or `$SKARDI_GATEWAY_URL`) overriding the
/// membership's gateway, queries go to the override — so the recorded MCP
/// URL must too, or `mcp register` would configure the token against the
/// deployment the person did NOT name. The exchange's URL is reported.
#[tokio::test]
async fn an_overriding_gateway_is_where_the_recorded_mcp_url_points() {
    let gw = gateway(200).await;
    let console = console(
        200,
        approved_with_control("https://elsewhere.example", "tok-1", "pair-1"),
    )
    .await;
    let home = TempDir::new().unwrap();
    let mut options = control_options(&console.uri());
    options.server_override = Some(format!("{}/", gw.uri()));

    let report = login(options, &config_in(&home)).await.unwrap();

    let expected = format!("{}/mcp", gw.uri());
    let file = read_yaml(&config_in(&home));
    assert_eq!(
        file["contexts"][0]["mcp-url"].as_str(),
        Some(expected.as_str())
    );
    assert_eq!(report.mcp_url.as_deref(), Some(expected.as_str()));
    assert_eq!(
        report.mcp_url_set_aside.as_deref(),
        Some("https://elsewhere.example/mcp")
    );
    // The probe went to the override, which is what the URL now follows.
    assert_eq!(gw.received_requests().await.unwrap().len(), 1);
}

/// The env override is the same case, through the direct flow.
#[tokio::test]
async fn an_env_gateway_is_where_the_recorded_mcp_url_points() {
    let gw = gateway(200).await;
    let mut listed = membership(
        "acme",
        "acme-prod",
        "active",
        Some("https://elsewhere.example"),
    );
    listed["mcp_url"] = json!("https://elsewhere.example/mcp");
    let cp = control_plane_with(vec![listed]).await;
    mint_ok(&cp, "acme-prod", "tok-1").await;
    let home = TempDir::new().unwrap();
    let mut options = options(&cp.uri(), None);
    options.env_gateway_url = Some(gw.uri());

    let report = login(options, &config_in(&home)).await.unwrap();

    let expected = format!("{}/mcp", gw.uri());
    let file = read_yaml(&config_in(&home));
    assert_eq!(
        file["contexts"][0]["mcp-url"].as_str(),
        Some(expected.as_str())
    );
    assert_eq!(
        report.mcp_url_set_aside.as_deref(),
        Some("https://elsewhere.example/mcp")
    );
}

/// An override on the SAME origin changes nothing: the exchange's URL is
/// kept as it was named, and nothing is reported as set aside.
#[test]
fn an_override_on_the_same_origin_keeps_the_exchanges_mcp_url() {
    let mut named = parsed("acme", "acme-prod", None);
    named.mcp_url = Some("https://gw.example/mcp".to_string());
    assert_eq!(
        super::mcp_url_for("https://gw.example:443/", &named),
        (Some("https://gw.example/mcp".to_string()), None)
    );
    // A different port is a different origin.
    assert_eq!(
        super::mcp_url_for("https://gw.example:8443", &named),
        (
            Some("https://gw.example:8443/mcp".to_string()),
            Some("https://gw.example/mcp".to_string())
        )
    );
    // No URL from the exchange: none is invented.
    named.mcp_url = None;
    assert_eq!(
        super::mcp_url_for("https://other.example", &named),
        (None, None)
    );
}

/// Every field the older server could not know is named in the "too old"
/// message, so the person knows what to drop — including a re-login's
/// `replaces_pair_id`, which no flag of theirs asked for.
#[tokio::test]
async fn too_old_names_every_new_field_the_request_carried() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/requests"))
        .respond_with(ResponseTemplate::new(422).set_body_json(json!({
            "error": {"code": "invalid_body", "message": "unknown field `setup`"}
        })))
        .mount(&server)
        .await;
    let home = TempDir::new().unwrap();
    let path = config_in(&home);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        format!(
            "current-context: acme/my-ws\n{}    pair-id: pair-old\n",
            existing_cloud_context()
        ),
    )
    .unwrap();
    let mut options = broker_options(&server.uri());
    options.setup = Some(super::console_broker::Setup {
        profile: "personal".to_string(),
        name: "Alice".to_string(),
        industry: None,
    });
    let err = login(options, &path).await.unwrap_err().to_string();
    assert!(
        err.contains("too old for --setup, replacing the previous login's pair"),
        "{err}"
    );
}

/// The pair to replace is read only off a CLOUD context: a server-mode one
/// carrying a stray `pair-id` names nothing for the exchange to revoke.
#[tokio::test]
async fn a_server_context_names_no_pair_to_replace() {
    let gw = gateway(200).await;
    let console = console(200, approved("my-ws", &gw.uri(), "tok-1")).await;
    let home = TempDir::new().unwrap();
    let path = config_in(&home);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        "current-context: local\ncontexts:\n  - name: local\n    server: http://127.0.0.1:8080\n    pair-id: pair-stray\n",
    )
    .unwrap();
    login(broker_options(&console.uri()), &path).await.unwrap();
    let opened = opened_body(&console.received_requests().await.unwrap());
    assert!(opened.get("replaces_pair_id").is_none(), "{opened}");
}
