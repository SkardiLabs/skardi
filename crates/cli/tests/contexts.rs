//! Integration tests for the context model, driving the REAL `skardi` binary.
//!
//! Everything here runs with `HOME` pointed at a temp directory, so the tests
//! exercise the actual `~/.skardi/config.yaml` path — file creation, mode bits,
//! atomic rewrite, redaction — without touching the developer's own config and
//! without needing a server. Resolution failures are asserted through the
//! binary too, because "no request is issued" is part of the contract and only
//! a real process can demonstrate it.
//!
//! `dirs::home_dir()` reads `$HOME` on unix, which is what makes the
//! redirection work; these tests are unix-only for that reason.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::TempDir;

/// Run the binary with `HOME` redirected, and with the ambient
/// `SKARDI_*` variables cleared so a developer's exported values cannot
/// change what the test observes.
fn skardi(home: &Path, args: &[&str]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_skardi"));
    cmd.env("HOME", home)
        .env_remove("SKARDI_SERVER_URL")
        .env_remove("SKARDI_API_TOKEN")
        .env_remove("SKARDI_CONTEXT")
        .args(args);
    cmd.output().expect("spawn skardi")
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

fn mode_of(path: &Path) -> u32 {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

/// The whole `config` surface in one pass, in the order an operator meets it:
/// create, list, switch, inspect, delete.
#[test]
fn config_subcommands_create_list_switch_and_delete_contexts() {
    let home = TempDir::new().unwrap();
    let home = home.path();

    // No file yet: listing says so instead of failing.
    let out = skardi(home, &["config", "get-contexts"]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(stdout(&out).contains("no contexts configured"));

    // First set-context creates ~/.skardi/config.yaml, mode 0600.
    let out = skardi(
        home,
        &[
            "config",
            "set-context",
            "local",
            "--server",
            "http://127.0.0.1:9999",
            "--current",
        ],
    );
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(stdout(&out).contains("created context 'local'"));
    let path = config_path(home);
    assert!(path.is_file(), "config file was created");
    assert_eq!(
        mode_of(&path),
        0o600,
        "a file that will hold a PAT is owner-only"
    );

    // A cloud context needs its workspace, and is refused without one rather
    // than written as a dud that fails at the next command.
    let out = skardi(
        home,
        &[
            "config",
            "set-context",
            "acme/prod",
            "--mode",
            "cloud",
            "--server",
            "https://gw.skardi.ai",
        ],
    );
    assert!(!out.status.success());
    assert!(
        stderr(&out).contains("needs a workspace"),
        "{}",
        stderr(&out)
    );

    // With the workspace it lands.
    let out = skardi(
        home,
        &[
            "config",
            "set-context",
            "acme/prod",
            "--mode",
            "cloud",
            "--server",
            "https://gw.skardi.ai",
            "--workspace",
            "acme-prod",
            "--token",
            "skardi_pat_secret_value",
            "--user",
            "xin@skardi.ai",
        ],
    );
    assert!(out.status.success(), "{}", stderr(&out));

    // get-contexts marks the current one and shows the workspace dimension.
    let out = skardi(home, &["config", "get-contexts"]);
    let listing = stdout(&out);
    assert!(
        listing.contains("* "),
        "current context is marked:\n{listing}"
    );
    assert!(listing.contains("acme-prod"), "{listing}");
    assert!(listing.contains("cloud"), "{listing}");

    // use-context switches, and current-context reports it.
    let out = skardi(home, &["config", "use-context", "acme/prod"]);
    assert!(out.status.success(), "{}", stderr(&out));
    let out = skardi(home, &["config", "current-context"]);
    assert_eq!(stdout(&out).trim(), "acme/prod");

    // Deleting the current context clears the pointer, so later commands get
    // "no current context" rather than the hard unknown-context error.
    let out = skardi(home, &["config", "delete-context", "acme/prod"]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(
        stdout(&out).contains("no context is current now"),
        "{}",
        stdout(&out)
    );
    // The POINTER is cleared, but `local` is now the only context left, so
    // resolution's lone-context step selects it — and `current-context`
    // reports that, because it answers with what the next command will
    // actually use rather than with the raw field.
    let out = skardi(home, &["config", "current-context"]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(stdout(&out).trim(), "local");

    // With a SECOND context and no pointer, nothing is selected and the
    // command says so instead of guessing.
    let out = skardi(
        home,
        &["config", "set-context", "third", "--server", "http://c:3"],
    );
    assert!(out.status.success(), "{}", stderr(&out));
    let out = skardi(home, &["config", "current-context"]);
    assert!(
        !out.status.success(),
        "ambiguous selection is an error exit"
    );
    // …and `--context` answers it, since that is what a real command honours.
    let out = skardi(home, &["--context", "third", "config", "current-context"]);
    assert_eq!(stdout(&out).trim(), "third");
    let out = skardi(home, &["config", "delete-context", "third"]);
    assert!(out.status.success(), "{}", stderr(&out));

    // And it is really gone.
    let out = skardi(home, &["config", "delete-context", "acme/prod"]);
    assert!(!out.status.success());
    assert!(
        stderr(&out).contains("no context named"),
        "{}",
        stderr(&out)
    );
}

/// `view` must never print a live credential by default — the command an
/// operator runs while screen-sharing.
#[test]
fn view_redacts_tokens_unless_asked() {
    let home = TempDir::new().unwrap();
    let home = home.path();
    let secret = "skardi_pat_do_not_print_me";

    skardi(
        home,
        &[
            "config",
            "set-context",
            "acme/prod",
            "--mode",
            "cloud",
            "--workspace",
            "acme-prod",
            "--server",
            "https://gw.skardi.ai",
            "--token",
            secret,
            "--current",
        ],
    );

    let out = skardi(home, &["config", "view"]);
    let body = stdout(&out);
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(!body.contains(secret), "the token leaked:\n{body}");
    assert!(body.contains("(redacted)"), "{body}");
    // NO prefix survives. An earlier version kept twelve characters, which
    // printed a short legacy token in full while labelling it redacted.
    assert!(!body.contains("skardi_pat_d"), "a prefix leaked:\n{body}");

    let out = skardi(home, &["config", "view", "--show-tokens"]);
    assert!(stdout(&out).contains(secret), "--show-tokens prints it");
}

/// §5.4: reads tolerate a broken file, mutations refuse it — and the refusal
/// must leave the bytes untouched, because they may hold the only copy of a PAT.
#[test]
fn a_broken_config_is_tolerated_by_reads_and_refused_by_mutations() {
    let home = TempDir::new().unwrap();
    let home = home.path();
    let path = config_path(home);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let broken = "contexts: [name: acme/prod, token: skardi_pat_still_live\n";
    std::fs::write(&path, broken).unwrap();

    // A mutation aborts, names the file, and does NOT rewrite it.
    let out = skardi(home, &["config", "use-context", "acme/prod"]);
    assert!(!out.status.success());
    let complaint = stderr(&out);
    assert!(complaint.contains("refusing to modify"), "{complaint}");
    assert!(complaint.contains("config.yaml"), "{complaint}");
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        broken,
        "the malformed-but-credential-bearing file must survive verbatim"
    );

    // A read-side command still runs: --server keeps working while the
    // operator repairs the file, with a warning rather than a failure.
    let out = skardi(home, &["config", "get-contexts"]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(stderr(&out).contains("warning:"), "{}", stderr(&out));

    // `view` is the diagnostic: it reports the parse error and the path.
    let out = skardi(home, &["config", "view"]);
    assert!(!out.status.success());
    assert!(stderr(&out).contains("does not parse"), "{}", stderr(&out));
}

/// Resolution failures must happen BEFORE any request. Each of these would
/// otherwise send a credential somewhere the operator did not name.
#[test]
fn resolution_refuses_ambiguous_or_conflicting_selections_without_issuing_a_request() {
    let home = TempDir::new().unwrap();
    let home = home.path();

    skardi(
        home,
        &[
            "config",
            "set-context",
            "acme/prod",
            "--mode",
            "cloud",
            "--workspace",
            "acme-prod",
            "--server",
            "https://gw.invalid",
            "--token",
            "skardi_pat_x",
            "--current",
        ],
    );

    // An unknown --context lists what exists instead of falling through to
    // http://127.0.0.1:8080 — the one failure that would send a cloud query
    // to a local server.
    let out = skardi(home, &["--context", "typo", "query", "-e", "select 1"]);
    assert_eq!(out.status.code(), Some(1), "exit 1, not the connect code 2");
    let complaint = stderr(&out);
    assert!(complaint.contains("no context named 'typo'"), "{complaint}");
    assert!(complaint.contains("Available: acme/prod"), "{complaint}");

    // A stray SKARDI_SERVER_URL while a cloud context is selected is refused
    // by name. Exit 1 proves nothing was dialled: the gateway host above is
    // .invalid, so a request would have produced the connect code 2.
    let out = Command::new(env!("CARGO_BIN_EXE_skardi"))
        .env("HOME", home)
        .env("SKARDI_SERVER_URL", "http://127.0.0.1:1")
        .env_remove("SKARDI_API_TOKEN")
        .env_remove("SKARDI_CONTEXT")
        .args(["query", "-e", "select 1"])
        .output()
        .expect("spawn skardi");
    let complaint = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1), "stderr was: {complaint}");
    assert!(
        complaint.contains("SKARDI_SERVER_URL") && complaint.contains("mode: cloud"),
        "{complaint}"
    );

    // An unknown $SKARDI_CONTEXT, not just an unknown --context: the env
    // path had only unit coverage, and it is the one an agent harness sets.
    let out = Command::new(env!("CARGO_BIN_EXE_skardi"))
        .env("HOME", home)
        .env("SKARDI_CONTEXT", "also-typo")
        .env_remove("SKARDI_SERVER_URL")
        .env_remove("SKARDI_API_TOKEN")
        .args(["query", "-e", "select 1"])
        .output()
        .expect("spawn skardi");
    assert_eq!(out.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("no context named 'also-typo'"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // And the TOKEN half of the env conflict, which had no binary-level
    // assertion at all — only the server half did.
    let out = Command::new(env!("CARGO_BIN_EXE_skardi"))
        .env("HOME", home)
        .env("SKARDI_API_TOKEN", "stale-token")
        .env_remove("SKARDI_SERVER_URL")
        .env_remove("SKARDI_CONTEXT")
        .args(["query", "-e", "select 1"])
        .output()
        .expect("spawn skardi");
    assert_eq!(out.status.code(), Some(1));
    let complaint = String::from_utf8_lossy(&out.stderr);
    assert!(complaint.contains("SKARDI_API_TOKEN"), "{complaint}");
    assert!(
        !complaint.contains("stale-token"),
        "the refusal must not echo the value: {complaint}"
    );

    // A cloud context that lost its workspace (hand-edited) is refused with
    // the repair named.
    let path = config_path(home);
    let text = std::fs::read_to_string(&path).unwrap();
    // Drop the WHOLE line, indentation included: stripping just the key
    // leaves an over-indented sibling and the file stops parsing, which the
    // tolerant reader would then treat as "no contexts" — a different code
    // path from the one under test.
    let without_workspace: String = text
        .lines()
        .filter(|line| !line.trim_start().starts_with("workspace:"))
        .map(|line| format!("{line}\n"))
        .collect();
    std::fs::write(&path, &without_workspace).unwrap();
    assert!(
        without_workspace.contains("mode: cloud"),
        "the edit must leave a parsable cloud context:\n{without_workspace}"
    );
    let out = skardi(home, &["query", "-e", "select 1"]);
    assert_eq!(out.status.code(), Some(1), "stderr was: {}", stderr(&out));
    assert!(
        stderr(&out).contains("names no workspace"),
        "{}",
        stderr(&out)
    );
}

/// A cloud context with no `server` must never reach the built-in local
/// default, because its token is a workspace-scoped PAT. Driven through the
/// binary because that is how the bug was found: the unit layer resolved the
/// same config without complaint and `skardi query` really did POST to
/// http://127.0.0.1:8080.
#[test]
fn a_cloud_context_without_a_server_never_falls_back_to_the_local_default() {
    let home = TempDir::new().unwrap();
    let home = home.path();
    let path = config_path(home);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        "kind: client\n\
         current-context: acme/prod\n\
         contexts:\n\
         \x20 - name: acme/prod\n\
         \x20   mode: cloud\n\
         \x20   workspace: acme-prod\n\
         \x20   token: skardi_pat_workspace_scoped\n",
    )
    .unwrap();

    let out = skardi(home, &["query", "-e", "select 1"]);
    // Exit 1, not the connect code 2: nothing was dialled.
    assert_eq!(out.status.code(), Some(1), "stderr was: {}", stderr(&out));
    let complaint = stderr(&out);
    assert!(complaint.contains("names no server"), "{complaint}");
    assert!(
        complaint.contains("127.0.0.1:8080"),
        "the message names the default it refused to use: {complaint}"
    );
    assert!(
        !complaint.contains("skardi_pat_workspace_scoped"),
        "the refusal must not echo the token: {complaint}"
    );

    // `--server` is a deliberate act and satisfies it; .invalid guarantees
    // the failure is the DIAL (exit 2), proving resolution let it through.
    let out = skardi(
        home,
        &["--server", "https://gw.invalid", "query", "-e", "select 1"],
    );
    assert_eq!(out.status.code(), Some(2), "stderr was: {}", stderr(&out));

    // The write path refuses the same shape, so it cannot be created this way.
    let out = skardi(
        home,
        &[
            "config",
            "set-context",
            "b",
            "--mode",
            "cloud",
            "--workspace",
            "w",
            "--token",
            "skardi_pat_y",
        ],
    );
    assert!(!out.status.success());
    assert!(stderr(&out).contains("needs a server"), "{}", stderr(&out));
}

/// A legacy single-server file keeps working, and its first edit promotes it
/// to a context rather than dropping the credential it holds.
#[test]
fn a_legacy_spec_file_keeps_working_and_survives_its_first_edit() {
    let home = TempDir::new().unwrap();
    let home = home.path();
    let path = config_path(home);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        "kind: client\nspec:\n  server: http://legacy:8080\n  token: legacy-token\n",
    )
    .unwrap();

    // It resolves as one context named `default`.
    let out = skardi(home, &["config", "get-contexts"]);
    let listing = stdout(&out);
    assert!(listing.contains("default"), "{listing}");
    assert!(listing.contains("http://legacy:8080"), "{listing}");

    // Editing promotes `spec:` into `contexts:` WITHOUT losing the token.
    let out = skardi(
        home,
        &["config", "set-context", "local", "--server", "http://x:1"],
    );
    assert!(out.status.success(), "{}", stderr(&out));
    let out = skardi(home, &["config", "view", "--show-tokens"]);
    let body = stdout(&out);
    assert!(
        body.contains("legacy-token"),
        "credential preserved:\n{body}"
    );
    assert!(body.contains("name: default"), "{body}");
    assert!(body.contains("name: local"), "{body}");
}

/// Unknown keys survive a rewrite, so an older CLI cannot downgrade a file a
/// newer one wrote.
#[test]
fn unknown_keys_survive_a_mutation() {
    let home = TempDir::new().unwrap();
    let home = home.path();
    let path = config_path(home);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        "kind: client\n\
         future-top-level: keep-me\n\
         current-context: a\n\
         contexts:\n\
         \x20 - name: a\n\
         \x20   server: http://a:1\n\
         \x20   future-per-context: keep-me-too\n",
    )
    .unwrap();

    let out = skardi(
        home,
        &["config", "set-context", "b", "--server", "http://b:2"],
    );
    assert!(out.status.success(), "{}", stderr(&out));

    let rewritten = std::fs::read_to_string(&path).unwrap();
    // Key AND value: a rewrite that kept the key but dropped or blanked the
    // value would still pass a name-only assertion while losing the setting.
    assert!(
        rewritten.contains("future-top-level: keep-me"),
        "{rewritten}"
    );
    assert!(
        rewritten.contains("future-per-context: keep-me-too"),
        "{rewritten}"
    );
}
