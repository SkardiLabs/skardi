//! `login` → `query` → `logout` through the REAL binary, against a mock
//! control plane and a mock gateway.
//!
//! This is the automated form of M2's acceptance check ("smoke-tested on the
//! preview compose via `--identity`): the flow under test is the real one
//! minus the browser, and what it proves is the handoff the unit tests cannot
//! — that a context `login` wrote is one `query` then resolves, sends the
//! workspace selector for, and `logout` can undo.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// **A bare `skardi login` now reaches the hosted console.** Since the
/// brokered flow gained a built-in default, running `login` with no
/// `--control-plane`, no `SKARDI_CONTROL_PLANE_URL` and no `console:` in the
/// file dials `console.skardi.ai`, opens a request there, and polls it for
/// five minutes. A test that does that is not a test — it is a stalled CI job
/// leaving a pending login request on production. Every `login` invocation in
/// this file therefore either passes `--control-plane`, seeds `console:` in
/// the temp home, or fails before resolution (`--token`, `--client-id`).
fn skardi(home: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_skardi"))
        .env("HOME", home)
        .env_remove("SKARDI_SERVER_URL")
        .env_remove("SKARDI_API_TOKEN")
        .env_remove("SKARDI_CONTEXT")
        .env_remove("SKARDI_CONTROL_PLANE_URL")
        .env_remove("SKARDI_GATEWAY_URL")
        .env_remove("SKARDI_OAUTH_CLIENT_ID")
        .env_remove("SKARDI_DEV_IDENTITY")
        .args(args)
        .output()
        .expect("spawn skardi")
}

fn out(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn err(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

fn config_path(home: &Path) -> PathBuf {
    home.join(".skardi").join("config.yaml")
}

fn mode_of(path: &Path) -> u32 {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

#[tokio::test]
async fn login_writes_a_context_that_query_then_uses_and_logout_clears() {
    let gateway = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "success": true,
            "columns": ["n"],
            "rows": [[1]],
            "row_count": 1,
        })))
        .mount(&gateway)
        .await;

    let control_plane = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/me/workspaces"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "workspaces": [{
                "org_slug": "acme",
                "tenant_slug": "acme-prod",
                "display_name": "Production",
                "role": "admin",
                "provisioning_state": "active",
                "gateway_url": gateway.uri(),
            }],
        })))
        .mount(&control_plane)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/me/tokens"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "token": "skardi_pat_e2e",
            "pat": {"token_id": "tok-e2e", "expires_at": "2026-11-22T12:00:00Z"},
        })))
        .mount(&control_plane)
        .await;

    let home = TempDir::new().unwrap();
    let home = home.path();

    // 1. login, with the dev acquirer against a loopback control plane.
    let login = skardi(
        home,
        &[
            "login",
            "--control-plane",
            &control_plane.uri(),
            "--identity",
            "dev:alice",
        ],
    );
    assert!(login.status.success(), "{}", err(&login));
    assert!(
        out(&login).contains(&format!("wrote context acme/acme-prod → {}", gateway.uri())),
        "stdout was: {}",
        out(&login)
    );
    assert!(out(&login).contains("current context is now acme/acme-prod"));
    assert!(out(&login).contains("workspace acme-prod, role admin"));
    // The dev path announces itself, every run, naming what it authenticated
    // against.
    assert!(
        err(&login).contains("warning: authenticating with a dev-auth identity"),
        "stderr was: {}",
        err(&login)
    );
    // The raw PAT is never printed, on either stream.
    assert!(!out(&login).contains("skardi_pat_e2e"));
    assert!(!err(&login).contains("skardi_pat_e2e"));
    assert_eq!(mode_of(&config_path(home)), 0o600);

    // 2. query, with no flags at all: the context login wrote is the one
    //    resolution picks, and it carries the selector.
    let query = skardi(home, &["query", "-e", "select 1"]);
    assert!(query.status.success(), "{}", err(&query));
    let probes = gateway.received_requests().await.unwrap();
    // One from login's verification probe, one from the query.
    assert_eq!(probes.len(), 2);
    for request in &probes {
        assert_eq!(
            request.headers.get("Skardi-Workspace").unwrap(),
            "acme-prod"
        );
        assert_eq!(
            request.headers.get("Authorization").unwrap(),
            "Bearer skardi_pat_e2e"
        );
    }

    // 3. a gated command is still refused, in a context that now really works.
    let jobs = skardi(home, &["job", "list"]);
    assert_eq!(jobs.status.code(), Some(1));
    assert!(err(&jobs).contains("not available in a cloud context (acme/acme-prod)"));

    // 4. logout drops the credential and is explicit that the PAT lives on.
    let logout = skardi(home, &["logout"]);
    assert!(logout.status.success(), "{}", err(&logout));
    assert!(out(&logout).contains("cleared the credential in context acme/acme-prod"));
    assert!(
        out(&logout).contains("stays VALID"),
        "stdout was: {}",
        out(&logout)
    );
    let written = std::fs::read_to_string(config_path(home)).unwrap();
    assert!(!written.contains("skardi_pat_e2e"), "{written}");
    assert!(written.contains("workspace: acme-prod"), "{written}");
}

/// `--identity` against a non-loopback control plane is refused before any
/// request, so a dev bearer cannot be presented to a shared cluster by
/// accident.
#[test]
fn the_dev_identity_path_refuses_a_remote_control_plane() {
    let home = TempDir::new().unwrap();
    let output = skardi(
        home.path(),
        &[
            "login",
            "--control-plane",
            "https://global.example.com",
            "--identity",
            "dev:alice",
        ],
    );

    assert_eq!(output.status.code(), Some(1), "{}", err(&output));
    assert!(
        err(&output).contains("--i-know-this-is-dev-auth"),
        "stderr was: {}",
        err(&output)
    );
    assert!(!config_path(home.path()).exists());
}

/// The control-plane leg carries the ID token up and the raw PAT back, so a
/// plain-http non-loopback control plane is warned about — before the browser
/// opens, and on `logout --revoke` too. A warning rather than a refusal, as
/// with `ApiClient`: TLS may terminate at a proxy the CLI cannot see.
#[test]
fn a_cleartext_control_plane_is_warned_about_before_anything_is_sent() {
    let home = TempDir::new().unwrap();
    // `--identity` against a non-loopback host is refused, which is what makes
    // this fast: the warning is printed during resolution, before the refusal.
    let output = skardi(
        home.path(),
        &[
            "login",
            "--control-plane",
            "http://global.example.com",
            "--identity",
            "dev:alice",
        ],
    );
    let stderr = err(&output);

    assert!(
        stderr.contains("plain http to a non-loopback host"),
        "stderr was: {stderr}"
    );
    assert!(stderr.contains("in the clear"), "stderr was: {stderr}");
    // Loopback and https are silent.
    for quiet in ["http://127.0.0.1:1", "https://global.example.com"] {
        let output = skardi(
            home.path(),
            &["login", "--control-plane", quiet, "--identity", "dev:alice"],
        );
        assert!(
            !err(&output).contains("plain http"),
            "{quiet} should not warn: {}",
            err(&output)
        );
    }
}

/// **A recorded `console:` beats the built-in default, and the default's
/// notice stays quiet when it did not choose.**
///
/// This is the hermetic half of the default. The other half — that with
/// nothing recorded the brokered flow goes to `console.skardi.ai` and says so
/// — is pinned at the unit level (`the_brokered_flow_defaults_to_the_hosted_
/// console`, `the_default_notice_says_it_is_one_and_names_every_override`)
/// and deliberately NOT here: an end-to-end run of that case has to dial the
/// hosted service, and this test's predecessor did exactly that once the
/// default existed — it opened a real login request on production and polled
/// it until nextest gave up.
///
/// What CAN be proved end to end without leaving the machine is the property
/// that makes the default safe: precedence. A bare `login` with `console:` in
/// the file goes to that console and nowhere else, and it prints no notice
/// about a default, because none was used.
#[tokio::test]
async fn a_recorded_console_is_dialed_and_the_default_stays_silent() {
    let console = MockServer::start().await;
    // Answer the open with an error, so the run ends at the first round trip
    // — proving where it went is the point, not what happens after.
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/requests"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&console)
        .await;
    let home = TempDir::new().unwrap();
    std::fs::create_dir_all(home.path().join(".skardi")).unwrap();
    std::fs::write(
        config_path(home.path()),
        format!("console: {}\n", console.uri()),
    )
    .unwrap();

    let output = tokio::task::spawn_blocking({
        let home = home.path().to_path_buf();
        move || skardi(&home, &["login", "--no-browser"])
    })
    .await
    .unwrap();

    assert_eq!(output.status.code(), Some(1), "{}", err(&output));
    let stderr = err(&output);
    // Not the default, and not announced as one.
    assert!(!stderr.contains("built-in default"), "{stderr}");
    assert!(!stderr.contains("console.skardi.ai"), "{stderr}");
    // The mock's `expect(1)` is the positive half: the request reached the
    // recorded console. Checked explicitly so the failure names the property.
    let received = console.received_requests().await.unwrap();
    assert_eq!(received.len(), 1, "the recorded console was dialed once");
}

/// The direct-OAuth flow names `control-plane:`, the key IT reads.
///
/// The pair of tests is the point: one flag changes which service the URL
/// belongs to, and so which key the message should send the reader to.
#[test]
fn the_direct_flow_names_the_control_plane_key_instead() {
    let home = TempDir::new().unwrap();
    let output = skardi(
        home.path(),
        &[
            "login",
            "--client-id",
            "client-123.apps.googleusercontent.com",
        ],
    );

    assert_eq!(output.status.code(), Some(1));
    let stderr = err(&output);
    assert!(stderr.contains("control-plane:"), "{stderr}");
    assert!(!stderr.contains("console:"), "{stderr}");
}

/// The global `--token`/`--server` flags mean nothing to these two commands,
/// and a typed flag that is silently dropped reads as one that worked.
#[test]
fn flags_these_commands_cannot_honour_are_refused_by_name() {
    let home = TempDir::new().unwrap();
    let home = home.path();

    let login = skardi(home, &["--token", "skardi_pat_x", "login"]);
    assert_eq!(login.status.code(), Some(1));
    assert!(
        err(&login).contains("--token is not accepted by 'login'"),
        "stderr was: {}",
        err(&login)
    );

    for flag in [["--server", "http://127.0.0.1:9"], ["--token", "t"]] {
        let logout = skardi(home, &[flag[0], flag[1], "logout"]);
        assert_eq!(logout.status.code(), Some(1));
        assert!(
            err(&logout).contains("not accepted by 'logout'"),
            "stderr was: {}",
            err(&logout)
        );
    }
    // Refused before anything is read or written.
    assert!(!config_path(home).exists());
}
