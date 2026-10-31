//! The agent-onboarding commands end to end (skardi-cloud design 2026-10-27
//! §2.4, §6 "CLI output"), through the REAL binary against a mock gateway.
//!
//! What only a process can show: the exit code each outcome produces, that
//! `--json` puts exactly one object on stdout, and — the invariant this file
//! exists for — that **no command, in either output mode, prints the control
//! token** (§5.0 invariant 10). The per-route requests, bodies and `next`
//! hints are unit-tested beside each command in `src/agent/`.

use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::TempDir;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

const CONTROL: &str = "skardi_pat_CONTROL_never_printed";
const MCP: &str = "skardi_pat_mcp_query_token";
const WS: &str = "/control/v1/workspaces/acme-prod";
const HANDOFF: &str = "4b9f3c1e-8a2d-4c6b-9e0f-1a2b3c4d5e6f";
const VERSION: &str = "0e6a3d2c-0000-4000-8000-000000000001";

fn skardi(home: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_skardi"))
        .env("HOME", home)
        .env("SKARDI_CONFIG_DIR", home.join(".skardi"))
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

/// `skardi` off the async runtime: the mock answers on it.
async fn run(home: &Path, args: &[&str]) -> Output {
    let home = home.to_path_buf();
    let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
    tokio::task::spawn_blocking(move || {
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        skardi(&home, &args)
    })
    .await
    .unwrap()
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

fn config_path(home: &Path) -> PathBuf {
    home.join(".skardi").join("config.yaml")
}

/// A `login --control` context pointing at `gateway`, owner-only so the
/// loose-permission warning stays out of the assertions.
fn seed(home: &Path, gateway: &str, control: Option<&str>) {
    let dir = home.join(".skardi");
    std::fs::create_dir_all(&dir).unwrap();
    let control = control
        .map(|token| {
            format!(
                "    control-token: {token}\n    control-token-expires-at: 2999-01-01T00:00:00Z\n    pair-id: pair-1\n"
            )
        })
        .unwrap_or_default();
    std::fs::write(
        config_path(home),
        format!(
            "console: https://console.example\n\
             current-context: acme/prod\n\
             contexts:\n\
             \x20 - name: acme/prod\n\
             \x20   mode: cloud\n\
             \x20   server: {gateway}\n\
             \x20   workspace: acme-prod\n\
             \x20   token: {MCP}\n\
             \x20   token-id: tok-mcp\n\
             \x20   mcp-url: {gateway}/mcp\n\
             {control}"
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(config_path(home), std::fs::Permissions::from_mode(0o600))
            .unwrap();
    }
}

async fn ok(server: &MockServer, verb: &str, route: &str, status: u16, body: Value) {
    Mock::given(method(verb))
        .and(path(format!("{WS}{route}")))
        .respond_with(ResponseTemplate::new(status).set_body_json(body))
        .mount(server)
        .await;
}

/// A gateway answering every route the ten commands call, successfully.
async fn gateway() -> MockServer {
    let server = MockServer::start().await;
    ok(
        &server,
        "GET",
        "",
        200,
        json!({"tenant_slug": "acme-prod", "provisioning_state": "active"}),
    )
    .await;
    ok(
        &server,
        "GET",
        "/integration-availability",
        200,
        json!({"integrations": []}),
    )
    .await;
    ok(&server, "GET", "/connections", 200, json!([])).await;
    ok(&server, "GET", "/saas-sources", 200, json!([])).await;
    ok(
        &server,
        "POST",
        "/handoffs",
        201,
        json!({
            "handoff_id": HANDOFF,
            "kind": "provider_consent",
            "page_url": "https://console.example/w/acme-prod/handoff/x",
            "expires_at": "2026-10-30T12:15:00Z",
        }),
    )
    .await;
    ok(
        &server,
        "GET",
        &format!("/handoffs/{HANDOFF}"),
        200,
        json!({
            "state": "completed",
            "kind": "provider_consent",
            "connection_alias": "acme-notion",
            "connection_version_id": VERSION,
            "expires_at": "2026-10-30T12:30:00Z",
        }),
    )
    .await;
    ok(
        &server,
        "POST",
        &format!("/handoffs/{HANDOFF}/abandon"),
        200,
        json!({
            "state": "abandoned", "kind": "provider_consent", "expires_at": "2026-10-30T12:30:00Z",
        }),
    )
    .await;
    ok(&server, "POST", "/connections/preview/acme-notion/permissions", 200, json!({
        "credential": "alive", "verdicts": [{"verdict": "verified"}], "checked_at": "2026-10-30T12:00:00Z",
    }))
    .await;
    ok(
        &server,
        "POST",
        "/connections/acme-notion/check",
        200,
        json!({"state": "alive"}),
    )
    .await;
    ok(
        &server,
        "GET",
        "/connections/preview/acme-notion/candidates",
        200,
        json!({
            "connection_version_id": VERSION, "candidates": [],
        }),
    )
    .await;
    ok(
        &server,
        "POST",
        "/installs",
        202,
        json!({"operation_id": "op-1", "state": "done"}),
    )
    .await;
    ok(
        &server,
        "GET",
        "/sources/summary",
        200,
        json!({"sources": []}),
    )
    .await;
    ok(&server, "GET", "/sources", 200, json!({"sources": []})).await;
    for leaf in ["", "/health", "/items"] {
        ok(
            &server,
            "GET",
            &format!("/saas-sources/notes-md{leaf}"),
            200,
            json!({"source_name": "notes-md"}),
        )
        .await;
    }
    ok(
        &server,
        "GET",
        "/saas-sources/notes-md/runs",
        200,
        json!([{"run_id": "r1"}]),
    )
    .await;
    ok(
        &server,
        "POST",
        "/saas-sources/notes-md/sync",
        202,
        json!({}),
    )
    .await;
    ok(
        &server,
        "POST",
        "/saas-sources",
        201,
        json!({"source_name": "notes-md", "schema_name": "doc_notes_md"}),
    )
    .await;
    Mock::given(method("PUT"))
        .and(path("/documents/doc_notes_md"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"success": true})))
        .mount(&server)
        .await;
    server
}

/// Every command, in both modes, against a gateway that answers them all.
fn every_command(file: &str) -> Vec<Vec<String>> {
    let commands: Vec<Vec<&str>> = vec![
        vec!["integrations", "list"],
        vec!["integrations", "connect", "notion", "--target", "new"],
        vec!["handoff", "wait", HANDOFF],
        vec![
            "integrations",
            "permissions",
            "acme-notion",
            "--connection-version",
            VERSION,
        ],
        vec![
            "integrations",
            "candidates",
            "acme-notion",
            "--connection-version",
            VERSION,
        ],
        vec![
            "integrations",
            "install",
            "acme-notion",
            "--connection-version",
            VERSION,
            "--scope",
            r#"{"pages":null}"#,
        ],
        vec!["integrations", "cancel", HANDOFF],
        vec!["integrations", "status"],
        vec!["integrations", "status", "--detail"],
        vec!["integrations", "status", "notes-md"],
        vec!["integrations", "sync", "notes-md"],
        vec!["documents", "add", file],
    ];
    let mut out = Vec::new();
    for command in commands {
        let owned: Vec<String> = command.iter().map(|s| s.to_string()).collect();
        let mut json = owned.clone();
        json.push("--json".to_string());
        out.push(owned);
        out.push(json);
    }
    out
}

/// §6 "CLI output": no command, `--json` included, prints the control token.
/// Each command also succeeds, so this is not passing by failing early.
#[tokio::test]
async fn no_command_prints_the_control_token() {
    let gw = gateway().await;
    let home = TempDir::new().unwrap();
    seed(home.path(), &gw.uri(), Some(CONTROL));
    let file = home.path().join("notes.md");
    std::fs::write(&file, "# hello\n").unwrap();

    let mut commands = every_command(file.to_str().unwrap());
    commands.push(vec!["config".into(), "view".into(), "--show-tokens".into()]);
    commands.push(vec!["config".into(), "view".into()]);
    commands.push(vec!["mcp".into(), "register".into(), "--print".into()]);
    for args in commands {
        let argv: Vec<&str> = args.iter().map(String::as_str).collect();
        let output = run(home.path(), &argv).await;
        let (out, err) = (stdout(&output), stderr(&output));
        assert_eq!(
            output.status.code(),
            Some(0),
            "{argv:?}\nstdout: {out}\nstderr: {err}"
        );
        assert!(
            !out.contains(CONTROL),
            "{argv:?} printed the control token:\n{out}"
        );
        assert!(
            !err.contains(CONTROL),
            "{argv:?} printed the control token:\n{err}"
        );
        if argv.contains(&"--json") {
            let lines: Vec<&str> = out.lines().collect();
            assert_eq!(lines.len(), 1, "{argv:?}: one object on stdout\n{out}");
            let envelope: Value = serde_json::from_str(lines[0]).unwrap();
            assert_eq!(envelope["ok"], true, "{argv:?}: {envelope}");
            assert!(envelope.get("next").is_some(), "{argv:?}: {envelope}");
        }
    }

    // The control token went to the control door and nowhere else.
    for request in gw.received_requests().await.unwrap() {
        let bearer = request
            .headers
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        if request.url.path().starts_with("/control/v1/") {
            assert_eq!(bearer, format!("Bearer {CONTROL}"));
        } else {
            assert_eq!(bearer, format!("Bearer {MCP}"), "{}", request.url);
        }
    }
}

/// A server that echoes the bearer into a refusal still cannot make the CLI
/// print it: the output is scrubbed as a last line of defence.
#[tokio::test]
async fn an_echoed_control_token_is_scrubbed_from_a_refusal() {
    let gw = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": {"code": "bad_request", "message": format!("you sent Bearer {CONTROL}")}
        })))
        .mount(&gw)
        .await;
    let home = TempDir::new().unwrap();
    seed(home.path(), &gw.uri(), Some(CONTROL));
    for argv in [
        vec!["integrations", "list"],
        vec!["integrations", "list", "--json"],
    ] {
        let output = run(home.path(), &argv).await;
        assert_eq!(output.status.code(), Some(3));
        assert!(!stdout(&output).contains(CONTROL), "{}", stdout(&output));
        assert!(!stderr(&output).contains(CONTROL), "{}", stderr(&output));
        assert!(
            format!("{}{}", stdout(&output), stderr(&output)).contains("(redacted)"),
            "{output:?}"
        );
    }
}

/// §2.4.2's exit codes, each from a real process.
#[tokio::test]
async fn each_outcome_exits_with_the_designs_code() {
    let gw = MockServer::start().await;
    ok(
        &gw,
        "GET",
        "",
        403,
        json!({"error": {"code": "admin_required", "message": "admins only"}}),
    )
    .await;
    ok(&gw, "GET", &format!("/handoffs/{HANDOFF}"), 200, json!({"state": "expired", "kind": "provider_consent", "expires_at": "2026-10-30T12:30:00Z"})).await;
    ok(
        &gw,
        "POST",
        "/saas-sources/down/sync",
        503,
        json!({"error": {"code": "control_plane_unavailable", "message": "down"}}),
    )
    .await;
    ok(&gw, "POST", "/saas-sources/old/sync", 401, json!({"error": {"code": "token_unknown_or_revoked", "message": "run skardi login --control"}})).await;
    let home = TempDir::new().unwrap();
    seed(home.path(), &gw.uri(), Some(CONTROL));

    let cases: Vec<(Vec<&str>, i32, &str)> = vec![
        // 2: usage — `--target` is required, and a scope with `exclude`.
        (
            vec!["integrations", "connect", "notion", "--json"],
            2,
            "usage",
        ),
        (
            vec![
                "integrations",
                "install",
                "a",
                "--connection-version",
                VERSION,
                "--scope",
                r#"{"pages":[{"kind":"exclude","coordinate":{"page_id":"p"}}]}"#,
                "--json",
            ],
            2,
            "scope_exclude_not_agent_callable",
        ),
        // 3: refused.
        (vec!["integrations", "list", "--json"], 3, "admin_required"),
        // 4: a handoff that ended without a result.
        (vec!["handoff", "wait", HANDOFF, "--json"], 4, "expired"),
        // 5: the control plane answered 5xx.
        (
            vec!["integrations", "sync", "down", "--json"],
            5,
            "control_plane_unavailable",
        ),
        // 6: the control token is no longer accepted.
        (
            vec!["integrations", "sync", "old", "--json"],
            6,
            "token_unknown_or_revoked",
        ),
    ];
    for (argv, code, error) in cases {
        let output = run(home.path(), &argv).await;
        assert_eq!(output.status.code(), Some(code), "{argv:?}: {output:?}");
        let envelope: Value = serde_json::from_str(stdout(&output).trim())
            .unwrap_or_else(|e| panic!("{argv:?}: {e}: {}", stdout(&output)));
        assert_eq!(envelope["ok"], false);
        assert_eq!(envelope["error"]["code"], error, "{argv:?}: {envelope}");
    }

    // 5: nothing answering at all.
    let unreachable = TempDir::new().unwrap();
    seed(unreachable.path(), "http://127.0.0.1:1", Some(CONTROL));
    let output = run(unreachable.path(), &["integrations", "list", "--json"]).await;
    assert_eq!(output.status.code(), Some(5), "{output:?}");
}

/// 6, before any request: a context signed in without `--control`.
#[tokio::test]
async fn a_context_without_a_control_token_exits_6_and_sends_nothing() {
    let gw = MockServer::start().await;
    Mock::given(path_regex(".*"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&gw)
        .await;
    let home = TempDir::new().unwrap();
    seed(home.path(), &gw.uri(), None);
    let output = run(home.path(), &["integrations", "list", "--json"]).await;
    assert_eq!(output.status.code(), Some(6), "{output:?}");
    let envelope: Value = serde_json::from_str(stdout(&output).trim()).unwrap();
    assert_eq!(envelope["error"]["code"], "control_not_granted");
    assert_eq!(envelope["next"], "skardi login --control");
    assert!(gw.received_requests().await.unwrap().is_empty());

    // Plain mode: the sentence and the recovery on stderr, nothing on stdout.
    let output = run(home.path(), &["integrations", "list"]).await;
    assert_eq!(output.status.code(), Some(6));
    assert!(stdout(&output).is_empty(), "{output:?}");
    assert!(
        stderr(&output).contains("next: skardi login --control"),
        "{output:?}"
    );
}

/// `login --control` approved without `control`: the context is written so
/// queries work, and the process exits 6 with a sentence saying what to do.
#[tokio::test]
async fn login_control_approved_without_control_exits_6_and_keeps_the_context() {
    let gw = MockServer::start().await;
    let console = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/requests"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "request_id": "7f3a9c0000000000000000000000abcd",
            "confirm": "7f3a9c",
            "expires_at": "2999-01-01T00:00:00Z",
        })))
        .mount(&console)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/global/v1/cli-login/exchange"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "status": "approved",
            "token": MCP,
            "token_id": "tok-1",
            "expires_at": "2999-01-01T00:00:00Z",
            "workspace": "acme-prod",
            "memberships": [{
                "org_slug": "acme", "tenant_slug": "acme-prod", "role": "member",
                "provisioning_state": "active", "gateway_url": gw.uri(),
            }],
        })))
        .mount(&console)
        .await;
    let home = TempDir::new().unwrap();
    let output = run(
        home.path(),
        &[
            "login",
            "--control-plane",
            &console.uri(),
            "--control",
            "--no-browser",
            "--no-verify",
        ],
    )
    .await;
    assert_eq!(output.status.code(), Some(6), "{output:?}");
    let err = stderr(&output);
    assert!(err.contains("approved without control"), "{err}");
    assert!(err.contains("skardi login --control"), "{err}");
    assert!(
        stdout(&output).contains("wrote context acme/acme-prod"),
        "{output:?}"
    );
    let raw = std::fs::read_to_string(config_path(home.path())).unwrap();
    assert!(raw.contains(MCP), "{raw}");
    assert!(!raw.contains("control-token"), "{raw}");
}

/// The bridge stays refused on a cloud context; `mcp register` is the cloud
/// path, and it is not gated.
#[tokio::test]
async fn mcp_register_is_not_gated_while_the_bridge_still_is() {
    let home = TempDir::new().unwrap();
    seed(home.path(), "https://gw.example", Some(CONTROL));
    let bridge = run(home.path(), &["mcp"]).await;
    assert_eq!(bridge.status.code(), Some(1), "{bridge:?}");
    let register = run(home.path(), &["mcp", "register", "--print"]).await;
    assert_eq!(register.status.code(), Some(0), "{register:?}");
    assert!(stdout(&register).contains("https://gw.example/mcp"));
}

/// `--token` and `--server` would be ignored by `mcp register`, which writes
/// what the context holds — so `--token NEW … --print` would have printed the
/// OLD token as though it were the new one. Refused, before anything is read.
#[tokio::test]
async fn mcp_register_refuses_the_global_flags_it_cannot_honour() {
    let home = TempDir::new().unwrap();
    seed(home.path(), "https://gw.example", Some(CONTROL));
    for argv in [
        vec!["--token", "skardi_pat_NEW", "mcp", "register", "--print"],
        vec![
            "--server",
            "https://other.example",
            "mcp",
            "register",
            "--print",
        ],
    ] {
        let output = run(home.path(), &argv).await;
        assert_eq!(output.status.code(), Some(1), "{argv:?}: {output:?}");
        assert!(stdout(&output).is_empty(), "{argv:?}: {output:?}");
        let err = stderr(&output);
        assert!(err.contains("not accepted by 'mcp register'"), "{err}");
        assert!(!err.contains(MCP), "{err}");
    }
    // `--context` is what it does honour.
    let output = run(
        home.path(),
        &["--context", "acme/prod", "mcp", "register", "--print"],
    )
    .await;
    assert_eq!(output.status.code(), Some(0), "{output:?}");
}

/// A usage error under an agent command is the table's `2` in both modes:
/// clap's text on stderr in plain mode, the envelope on stdout with
/// `--json`. `--token` is one too — the control token comes from `login
/// --control` alone — and is refused before anything is sent.
#[tokio::test]
async fn usage_errors_are_exit_2_in_either_mode() {
    let gw = MockServer::start().await;
    Mock::given(path_regex(".*"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&gw)
        .await;
    let home = TempDir::new().unwrap();
    seed(home.path(), &gw.uri(), Some(CONTROL));

    // clap: `--target` is required.
    let plain = run(home.path(), &["integrations", "connect", "notion"]).await;
    assert_eq!(plain.status.code(), Some(2), "{plain:?}");
    assert!(stdout(&plain).is_empty(), "{plain:?}");
    assert!(stderr(&plain).contains("--target"), "{plain:?}");

    // `--token`, refused by the command itself.
    let output = run(
        home.path(),
        &[
            "--token",
            "skardi_pat_typed",
            "integrations",
            "list",
            "--json",
        ],
    )
    .await;
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    let envelope: Value = serde_json::from_str(stdout(&output).trim()).unwrap();
    assert_eq!(envelope["error"]["code"], "token_flag_not_accepted");
    assert!(gw.received_requests().await.unwrap().is_empty());
}

/// An install whose only failure is the installer's identity proof landed:
/// exit 0, the success envelope with `next`, and the code as `warning`. Any
/// other failed operation is still exit 3 (the review on skardi-cloud #1338).
#[tokio::test]
async fn an_install_whose_only_failure_is_the_installer_link_exits_0_with_a_warning() {
    let cases: Vec<(&str, i32)> = vec![
        ("link_actor_is_not_the_installer", 0),
        ("link_installer_no_longer_a_member", 0),
        ("install_incomplete", 3),
    ];
    for (failure, code) in cases {
        let gw = MockServer::start().await;
        ok(
            &gw,
            "POST",
            "/installs",
            202,
            json!({"operation_id": "op-1", "state": "failed", "last_step": "activated",
                   "remaining": [], "failure": failure}),
        )
        .await;
        let home = TempDir::new().unwrap();
        seed(home.path(), &gw.uri(), Some(CONTROL));
        let output = run(
            home.path(),
            &[
                "integrations",
                "install",
                "acme-github",
                "--connection-version",
                VERSION,
                "--scope",
                r#"{"issues":null}"#,
                "--json",
            ],
        )
        .await;
        assert_eq!(output.status.code(), Some(code), "{failure}: {output:?}");
        let envelope: Value = serde_json::from_str(stdout(&output).trim())
            .unwrap_or_else(|e| panic!("{failure}: {e}: {}", stdout(&output)));
        if code == 0 {
            assert_eq!(envelope["ok"], true, "{envelope}");
            assert_eq!(envelope["warning"]["code"], failure, "{envelope}");
            assert_eq!(envelope["next"], "skardi integrations status --json");
            assert!(
                stderr(&output).contains("warning: ") && stderr(&output).contains(failure),
                "{}",
                stderr(&output)
            );
        } else {
            assert_eq!(envelope["ok"], false, "{envelope}");
            assert_eq!(envelope["error"]["code"], failure, "{envelope}");
            assert!(envelope.get("warning").is_none(), "{envelope}");
        }
        assert!(!stdout(&output).contains(CONTROL));
    }
}
