//! End-to-end smoke tests for the `skardi` binary against a real, running
//! `skardi-server`. These are integration tests of a binary crate: Cargo
//! sets `CARGO_BIN_EXE_skardi` (from this package's `[[bin]] name = "skardi"`)
//! to the path of the built binary, so each test spawns it as a real
//! subprocess rather than calling library code directly.
//!
//! The two tests that hit a real server are `#[ignore]` — they require a
//! `skardi-server` instance reachable at `SKARDI_SERVER_URL` (or the
//! default `http://127.0.0.1:8080`) and are not run as part of
//! `cargo test -p skardi-cli` (unit and wiremock-backed integration tests
//! cover everything else); run them manually once a server is up:
//!
//! ```bash
//! cargo run -p skardi-server -- --port 8080 &   # adjust flags to your setup
//! SKARDI_SERVER_URL=http://127.0.0.1:8080 \
//!   cargo test -p skardi-cli --test e2e_smoke -- --ignored
//! ```
//!
//! The remaining tests below exercise clap's argument parsing (usage
//! errors, `--help`) and need no server; they run as part of the normal
//! `cargo test -p skardi-cli`.

use std::io::Write;
use std::process::{Command, Stdio};

/// `skardi query -e "SELECT 1 AS one"` against a real server: the process
/// exits successfully and the row `{"one": 1}` shows up in stdout's JSON.
#[test]
#[ignore = "requires a running skardi-server"]
fn query_select_one_succeeds() {
    let output = Command::new(env!("CARGO_BIN_EXE_skardi"))
        .args(["query", "-e", "SELECT 1 AS one"])
        .output()
        .expect("failed to spawn skardi binary");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "expected `skardi query -e \"SELECT 1 AS one\"` to succeed\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("one"),
        "expected stdout to contain the `one` column\nstdout: {stdout}\nstderr: {stderr}"
    );
}

/// `skardi run definitely_not_a_pipeline -d -` with `{"x": 1}` piped to
/// stdin: exercises the `-d -` (read JSON body from stdin) path, and the
/// friendly 404 message, without needing any pipeline configured on the
/// server.
#[test]
#[ignore = "requires a running skardi-server"]
fn run_unknown_pipeline_via_stdin_data_reports_not_found() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_skardi"))
        .args(["run", "definitely_not_a_pipeline", "-d", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn skardi binary");

    child
        .stdin
        .take()
        .expect("child stdin was piped")
        .write_all(br#"{"x": 1}"#)
        .expect("failed to write JSON body to child stdin");

    let output = child
        .wait_with_output()
        .expect("failed to wait for skardi run to finish");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        !output.status.success(),
        "expected `skardi run definitely_not_a_pipeline -d -` to fail\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stderr.contains("not found"),
        "expected stderr to report the pipeline as not found\nstdout: {stdout}\nstderr: {stderr}"
    );
}

/// `skardi query --bogus` is a clap usage error (unknown flag). Per the
/// CLI's exit-code contract, code 2 is reserved for "server unreachable",
/// so usage errors must exit 1, not clap's default of 2. Needs no server.
#[test]
fn bogus_flag_exits_with_code_one() {
    let output = Command::new(env!("CARGO_BIN_EXE_skardi"))
        .args(["query", "--bogus"])
        .output()
        .expect("failed to spawn skardi binary");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(
        output.status.code(),
        Some(1),
        "expected `skardi query --bogus` to exit 1\nstdout: {stdout}\nstderr: {stderr}"
    );
}

/// `skardi --help` exits 0 and prints usage to stdout. Needs no server.
#[test]
fn help_exits_zero_and_prints_usage_to_stdout() {
    let output = Command::new(env!("CARGO_BIN_EXE_skardi"))
        .args(["--help"])
        .output()
        .expect("failed to spawn skardi binary");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(
        output.status.code(),
        Some(0),
        "expected `skardi --help` to exit 0\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("Usage"),
        "expected stdout to contain clap's usage text\nstdout: {stdout}\nstderr: {stderr}"
    );
}
