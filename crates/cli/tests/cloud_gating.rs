//! §8 end to end: what a `mode: cloud` context refuses, and what it sends.
//!
//! Driving the REAL binary against a mock gateway is the only way to assert
//! the half of the contract that is about ABSENCE — a gated command and an
//! expired credential must issue no request at all, and only a process with a
//! server in front of it can demonstrate that. The message wording and the
//! (capability, mode) matrix are unit-tested in `cloud.rs`; these are the
//! wiring tests.

#![cfg(unix)]

use std::path::Path;
use std::process::{Command, Output};
use tempfile::TempDir;
use wiremock::matchers::{method, path as path_matcher};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn skardi(home: &Path, args: &[&str]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_skardi"));
    cmd.env("HOME", home)
        .env_remove("SKARDI_SERVER_URL")
        .env_remove("SKARDI_API_TOKEN")
        .env_remove("SKARDI_CONTEXT")
        .args(args);
    cmd.output().expect("spawn skardi")
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

/// Write a single cloud context pointing at `server`, at mode 0600 so the
/// binary's loose-permission warning stays out of the assertions.
fn write_cloud_context(home: &Path, server: &str, token_expires_at: Option<&str>) {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = home.join(".skardi");
    std::fs::create_dir_all(&dir).unwrap();
    let expiry = match token_expires_at {
        Some(stamp) => format!("    token-expires-at: {stamp}\n"),
        None => String::new(),
    };
    let path = dir.join("config.yaml");
    std::fs::write(
        &path,
        format!(
            "current-context: acme/prod\n\
             contexts:\n\
             \x20 - name: acme/prod\n\
             \x20   mode: cloud\n\
             \x20   server: {server}\n\
             \x20   workspace: acme-prod\n\
             \x20   token: pat-value\n\
             {expiry}"
        ),
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

#[tokio::test]
async fn a_gated_command_refuses_before_issuing_any_request() {
    let gateway = MockServer::start().await;
    // A catch-all that would answer anything: if the CLI dialed at all, the
    // request would be recorded below.
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true})))
        .mount(&gateway)
        .await;

    let home = TempDir::new().unwrap();
    write_cloud_context(home.path(), &gateway.uri(), None);

    let out = skardi(home.path(), &["job", "list"]);

    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert!(
        stderr(&out).contains(
            "'job' is not available in a cloud context (acme/prod). Available: query, schema."
        ),
        "stderr was: {}",
        stderr(&out)
    );
    assert!(
        gateway.received_requests().await.unwrap().is_empty(),
        "a gated command must not reach the gateway"
    );
}

/// `mcp` straddles gateway-served (`query`) and engine-local (pipeline
/// execution, `/pipelines`) surfaces, so a cloud context refuses it whole
/// before the bridge ever starts speaking MCP on stdout.
#[tokio::test]
async fn mcp_is_gated_in_a_cloud_context_before_serving() {
    let gateway = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true})))
        .mount(&gateway)
        .await;

    let home = TempDir::new().unwrap();
    write_cloud_context(home.path(), &gateway.uri(), None);

    let out = skardi(home.path(), &["mcp"]);

    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert!(
        stderr(&out).contains(
            "'mcp' is not available in a cloud context (acme/prod). Available: query, schema."
        ),
        "stderr was: {}",
        stderr(&out)
    );
    assert!(
        out.stdout.is_empty(),
        "a refused mcp must write nothing to stdout (the would-be protocol channel)"
    );
    assert!(
        gateway.received_requests().await.unwrap().is_empty(),
        "a gated command must not reach the gateway"
    );
}

#[tokio::test]
async fn an_expired_credential_refuses_before_issuing_any_request() {
    let gateway = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"rows": []})))
        .mount(&gateway)
        .await;

    let home = TempDir::new().unwrap();
    write_cloud_context(home.path(), &gateway.uri(), Some("2020-01-01T00:00:00Z"));

    let out = skardi(home.path(), &["query", "-e", "select 1"]);

    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert!(
        stderr(&out).contains("expired at 2020-01-01T00:00:00Z. Run 'skardi login'."),
        "stderr was: {}",
        stderr(&out)
    );
    assert!(
        gateway.received_requests().await.unwrap().is_empty(),
        "an expired credential must not spend a round trip"
    );
}

#[tokio::test]
async fn an_allowed_command_names_its_workspace_to_the_gateway() {
    let gateway = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path_matcher("/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "success": true,
            "columns": ["n"],
            "rows": [[1]],
            "row_count": 1,
        })))
        .expect(1)
        .mount(&gateway)
        .await;

    let home = TempDir::new().unwrap();
    write_cloud_context(home.path(), &gateway.uri(), None);

    let out = skardi(home.path(), &["query", "-e", "select 1"]);
    assert!(out.status.success(), "{}", stderr(&out));

    let requests = gateway.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].headers.get("Skardi-Workspace").unwrap(),
        "acme-prod"
    );
    assert_eq!(
        requests[0].headers.get("Authorization").unwrap(),
        "Bearer pat-value"
    );
}

/// The gateway's 401 must not send a cloud user to `SKARDI_API_TOKEN`, which
/// resolution refuses for a cloud context.
#[tokio::test]
async fn a_rejected_credential_points_at_login_not_at_the_env_var() {
    let gateway = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path_matcher("/query"))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "success": false,
            "error": "invalid token",
            "error_type": "unauthorized",
        })))
        .mount(&gateway)
        .await;

    let home = TempDir::new().unwrap();
    write_cloud_context(home.path(), &gateway.uri(), None);

    let out = skardi(home.path(), &["query", "-e", "select 1"]);
    let stderr = stderr(&out);

    assert_eq!(out.status.code(), Some(1), "{stderr}");
    assert!(
        stderr.contains(
            "credential for context 'acme/prod' was rejected — it may be expired or revoked. Run 'skardi login'."
        ),
        "stderr was: {stderr}"
    );
    assert!(!stderr.contains("SKARDI_API_TOKEN"), "stderr was: {stderr}");
}
