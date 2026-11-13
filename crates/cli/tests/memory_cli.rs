//! `skardi memory` end to end, through the REAL binary against a stub
//! gateway that answers with the contract's fixtures
//! (`tests/fixtures/memory/`).
//!
//! What only a process can show: the exit code each outcome produces, that
//! `--json` puts exactly one object on stdout, that a record piped on stdin
//! reaches the save, that status is an answer (not a failure) where memory
//! does not apply, and that the query token is never printed. Each command's
//! requests and rendering are unit-tested in `src/agent/memory.rs`.

use serde_json::{Value, json};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use tempfile::TempDir;
use wiremock::matchers::{body_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const TOKEN: &str = "skardi_pat_mcp_query_token_never_printed";
const ANCHOR: &str = "url:github.com/SkardiLabs/skardi-cloud/issues/1400";
const ANCHOR_PATH: &str = "/memory/url%3Agithub.com%2FSkardiLabs%2Fskardi-cloud%2Fissues%2F1400";

fn fixture(name: &str) -> Value {
    let file: PathBuf = [
        env!("CARGO_MANIFEST_DIR"),
        "tests",
        "fixtures",
        "memory",
        name,
    ]
    .iter()
    .collect();
    serde_json::from_str(&std::fs::read_to_string(file).unwrap()).unwrap()
}

fn skardi(home: &Path, args: &[&str], stdin: Option<&str>) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_skardi"))
        .env("HOME", home)
        .env("SKARDI_CONFIG_DIR", home.join(".skardi"))
        .env_remove("SKARDI_SERVER_URL")
        .env_remove("SKARDI_API_TOKEN")
        .env_remove("SKARDI_CONTEXT")
        .env_remove("SKARDI_AGENT")
        .env_remove("SKARDI_SESSION_ID")
        .args(args)
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn skardi");
    if let Some(text) = stdin {
        child
            .stdin
            .take()
            .unwrap()
            .write_all(text.as_bytes())
            .unwrap();
    }
    child.wait_with_output().unwrap()
}

/// `skardi` off the async runtime: the stub answers on it.
async fn run(home: &Path, args: &[&str], stdin: Option<&str>) -> Output {
    let home = home.to_path_buf();
    let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
    let stdin = stdin.map(str::to_string);
    tokio::task::spawn_blocking(move || {
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        skardi(&home, &args, stdin.as_deref())
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

/// The single JSON object `--json` prints.
fn envelope(output: &Output) -> Value {
    let out = stdout(output);
    assert_eq!(out.lines().count(), 1, "one object on stdout: {out}");
    serde_json::from_str(out.trim()).unwrap()
}

fn assert_token_not_printed(output: &Output) {
    assert!(!stdout(output).contains(TOKEN), "{}", stdout(output));
    assert!(!stderr(output).contains(TOKEN), "{}", stderr(output));
}

/// A cloud context as `skardi login` writes it, pointing at `gateway`.
fn seed(home: &Path, gateway: &str) {
    write_config(
        home,
        &format!(
            "current-context: acme/eng\n\
             contexts:\n\
             \x20 - name: acme/eng\n\
             \x20   mode: cloud\n\
             \x20   server: {gateway}\n\
             \x20   workspace: acme-eng\n\
             \x20   token: {TOKEN}\n"
        ),
    );
}

fn write_config(home: &Path, text: &str) {
    let dir = home.join(".skardi");
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("config.yaml");
    std::fs::write(&file, text).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
}

/// A record piped on stdin is saved with the contract's request body, and
/// `--json` prints the route's response as `result`.
#[tokio::test]
async fn save_from_stdin_sends_the_contracts_request() {
    let server = MockServer::start().await;
    let home = TempDir::new().unwrap();
    seed(home.path(), &server.uri());
    let request = fixture("save_request.json");
    Mock::given(method("PUT"))
        .and(path(ANCHOR_PATH))
        .and(header("authorization", format!("Bearer {TOKEN}").as_str()))
        .and(header("skardi-workspace", "acme-eng"))
        .and(header("x-skardi-session-id", "9f2c1d7e-session"))
        .and(body_json(&request))
        .respond_with(ResponseTemplate::new(200).set_body_json(fixture("save_response.json")))
        .expect(1)
        .mount(&server)
        .await;
    let content = request["content"].to_string();
    let output = run(
        home.path(),
        &[
            "memory",
            "save",
            ANCHOR,
            "--file",
            "-",
            "--if-version",
            "3",
            "--agent",
            "claude-code",
            "--session",
            "9f2c1d7e-session",
            "--json",
        ],
        Some(&content),
    )
    .await;
    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    let parsed = envelope(&output);
    assert_eq!(parsed["ok"], true);
    assert_eq!(parsed["result"], fixture("save_response.json"));
    assert_token_not_printed(&output);
}

/// A stale save is exit 3, with the server's `current_version` in the
/// error and the recall that comes before a merge as `next`.
#[tokio::test]
async fn a_version_conflict_is_exit_3_with_the_current_version() {
    let server = MockServer::start().await;
    let home = TempDir::new().unwrap();
    seed(home.path(), &server.uri());
    Mock::given(method("PUT"))
        .and(path(ANCHOR_PATH))
        .respond_with(
            ResponseTemplate::new(409).set_body_json(fixture("error_version_conflict.json")),
        )
        .mount(&server)
        .await;
    let output = run(
        home.path(),
        &[
            "memory",
            "save",
            ANCHOR,
            "-f",
            "-",
            "--if-version",
            "4",
            "--json",
        ],
        Some(r#"{"goal": "Ship the audit export", "status": "UI next"}"#),
    )
    .await;
    assert_eq!(output.status.code(), Some(3));
    let parsed = envelope(&output);
    assert_eq!(parsed["ok"], false);
    assert_eq!(parsed["error"]["code"], "version_conflict");
    assert_eq!(parsed["error"]["current_version"], 5);
    assert_eq!(
        parsed["next"],
        format!("skardi memory recall {ANCHOR} --json")
    );
}

/// Plain recall: the record as text, and the save that continues it.
#[tokio::test]
async fn recall_prints_the_record_and_the_next_save() {
    let server = MockServer::start().await;
    let home = TempDir::new().unwrap();
    seed(home.path(), &server.uri());
    Mock::given(method("GET"))
        .and(path(ANCHOR_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(fixture("recall_response.json")))
        .mount(&server)
        .await;
    let output = run(home.path(), &["memory", "recall", ANCHOR], None).await;
    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    let out = stdout(&output);
    assert!(out.contains("goal: Ship the audit export"), "{out}");
    assert!(
        out.ends_with(&format!(
            "next: skardi memory save {ANCHOR} --if-version 4 --file -\n"
        )),
        "{out}"
    );
    assert_token_not_printed(&output);
}

/// The hook's command: a few lines, from `--anchor` here (the branch
/// resolution is unit-tested), and memory off is exit 0 with
/// `enabled: false` so the hook stays silent.
#[tokio::test]
async fn status_is_small_and_memory_off_is_an_answer() {
    let server = MockServer::start().await;
    let home = TempDir::new().unwrap();
    seed(home.path(), &server.uri());
    Mock::given(method("GET"))
        .and(path(ANCHOR_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(fixture("recall_response.json")))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/memory"))
        .respond_with(ResponseTemplate::new(200).set_body_json(fixture("list_response.json")))
        .mount(&server)
        .await;
    let output = run(home.path(), &["memory", "status", "--anchor", ANCHOR], None).await;
    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    let out = stdout(&output);
    assert!(out.lines().count() <= 6, "{out}");
    assert!(
        out.starts_with(&format!(
            "memory: {ANCHOR} (given) in acme-eng; consent task; 37/2000 memories\n"
        )),
        "{out}"
    );

    let off = MockServer::start().await;
    seed(home.path(), &off.uri());
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(404).set_body_json(fixture("error_memory_disabled.json")),
        )
        .mount(&off)
        .await;
    let output = run(
        home.path(),
        &["memory", "status", "--anchor", ANCHOR, "--json"],
        None,
    )
    .await;
    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    let parsed = envelope(&output);
    assert_eq!(parsed["result"]["enabled"], false);
    assert_eq!(parsed["result"]["reason"], "memory_disabled");
}

/// A server context, or no config at all, has no memory: status says so and
/// exits 0, while any other memory command is exit 6 naming `skardi login`.
#[tokio::test]
async fn without_a_cloud_context_status_is_off_and_the_rest_ask_for_login() {
    let home = TempDir::new().unwrap();
    write_config(
        home.path(),
        "current-context: local\ncontexts:\n  - name: local\n    server: http://127.0.0.1:9\n",
    );
    let output = run(home.path(), &["memory", "status", "--json"], None).await;
    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    assert_eq!(
        envelope(&output)["result"],
        json!({
            "enabled": false,
            "workspace": null,
            "reason": "not_a_cloud_context",
            "message": "agent memory lives in a skardi-cloud workspace, and the selected context is not a skardi-cloud one",
        })
    );

    let output = run(home.path(), &["memory", "list", "--json"], None).await;
    assert_eq!(output.status.code(), Some(6));
    let parsed = envelope(&output);
    assert_eq!(parsed["error"]["code"], "not_a_cloud_context");
    assert_eq!(parsed["next"], "skardi login");
}

/// `--server` reaches the context's gateway another way (a port-forward when
/// the configured address is not reachable from here), so `copy --to` sends
/// every call there too — here `--to` names the source's own workspace, which
/// finds the source's context, whose configured server answers nothing.
#[tokio::test]
async fn copy_to_dials_the_server_flag_not_the_configured_one() {
    let server = MockServer::start().await;
    let home = TempDir::new().unwrap();
    seed(home.path(), "http://127.0.0.1:9");
    Mock::given(method("GET"))
        .and(path("/memory/label%3Ax"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "anchor": "label:x",
            "memories": [fixture("recall_response.json")["memories"][0].clone()],
            "consent": "task",
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/memory"))
        .and(header("skardi-workspace", "acme-eng"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"memories": [], "total": 0, "workspace_used": 37, "workspace_cap": 2000}),
        ))
        .expect(1)
        .mount(&server)
        .await;
    let mut saved = fixture("save_response.json");
    saved["anchor"] = json!("label:x-copy");
    saved["version"] = json!(1);
    Mock::given(method("PUT"))
        .and(path("/memory/label%3Ax-copy"))
        .and(header("authorization", format!("Bearer {TOKEN}").as_str()))
        .and(header("skardi-workspace", "acme-eng"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&saved))
        .expect(1)
        .mount(&server)
        .await;
    let uri = server.uri();
    let output = run(
        home.path(),
        &[
            "--server",
            &uri,
            "memory",
            "copy",
            "label:x",
            "--to",
            "acme-eng",
            "--as",
            "label:x-copy",
            "--json",
        ],
        None,
    )
    .await;
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}{}",
        stdout(&output),
        stderr(&output)
    );
    assert_eq!(envelope(&output)["result"]["to"], saved);
    assert_token_not_printed(&output);
}

/// Usage errors are the agent table's 2, as the envelope under `--json`,
/// whether clap or the command finds them.
#[tokio::test]
async fn usage_errors_are_exit_2() {
    let server = MockServer::start().await;
    let home = TempDir::new().unwrap();
    seed(home.path(), &server.uri());
    for args in [
        &["memory", "recall", "billing-v2", "--json"][..],
        &["memory", "consent", "on", "--json"],
        &["memory", "share", ANCHOR, "--confirm", "--off", "--json"],
        &["memory", "redact", ANCHOR, "--json"],
    ] {
        let output = run(home.path(), args, None).await;
        assert_eq!(
            output.status.code(),
            Some(2),
            "{args:?}: {}",
            stdout(&output)
        );
        assert_eq!(envelope(&output)["ok"], false, "{args:?}");
    }
    assert!(
        server.received_requests().await.unwrap().is_empty(),
        "no usage error reaches the gateway"
    );
}
