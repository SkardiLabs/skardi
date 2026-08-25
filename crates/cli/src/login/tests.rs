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
        token_name: "cli@test-host".to_string(),
        now: at("2026-08-24T12:00:00Z"),
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
async fn the_browser_path_needs_a_client_id_and_says_so() {
    let home = TempDir::new().unwrap();
    let mut options = options("http://127.0.0.1:1", Some("http://gw.example"));
    options.identity = None;
    options.client_id = Some("   ".to_string());
    let err = login(options, &config_in(&home))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("--client-id"), "{err}");
    assert!(err.contains("SKARDI_OAUTH_CLIENT_ID"), "{err}");
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
