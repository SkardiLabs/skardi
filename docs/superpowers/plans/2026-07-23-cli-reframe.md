# CLI Reframe (Thin HTTP Client) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite `crates/cli` as a pure HTTP client for skardi-server: `query` posts SQL to `POST /query`, `run <name>` posts params to `POST /{name}/execute`, plus `pipeline`/`schema`/`health` discovery commands; the local DataFusion engine, alias system, and all cargo features are deleted.

**Architecture:** One shared `ApiClient` (reqwest, Bearer auth, uniform error mapping) and `ClientConfig` (flag > env > `~/.skardi/config.yaml` > default) feed one small module per command group. `main.rs` holds only clap definitions and dispatch. Output is raw JSON to stdout by default; `--table` renders a hand-rolled ASCII table; notices go to stderr.

**Tech Stack:** Rust, clap 4 (derive), reqwest 0.12 (json + rustls-tls), tokio, serde/serde_json/serde_yaml, anyhow, dirs. Dev: wiremock 0.6, tempfile.

**Spec:** `docs/superpowers/specs/2026-07-23-cli-reframe-design.md`

## Global Constraints

- No `.unwrap()` outside `#[cfg(test)]` modules and `#[test]` fns — even though `crates/cli` is formally exempt per `.claude/CLAUDE.md`, this rewrite holds to `Result` + `anyhow::Context` (spec: "Error handling" section). `.expect()` only for documented invariants.
- Import types with `use` at the top of the file; never full crate paths inline in function bodies.
- Default server URL is exactly `http://127.0.0.1:8080`. Env vars are exactly `SKARDI_SERVER_URL` and `SKARDI_API_TOKEN`. Config file is exactly `~/.skardi/config.yaml`.
- Exit codes: 0 success, 2 connection failure (server unreachable), 1 everything else.
- stdout carries only result data (JSON or table); warnings/truncation notices go to stderr.
- The server API is used as-is; no server changes in this plan.
- Workspace `serde_json` has no `preserve_order`, so JSON object keys iterate alphabetically — table column order is alphabetical and deterministic. Do not add `preserve_order`.
- Commit after every task. Pre-commit hooks run `cargo fmt` and `cargo check`; run `cargo fmt` before committing.

## Server API contract (verified against `crates/server/src`)

Success envelope (`response.rs:44-61`):
```json
{ "success": true, "data": [ {"col": "val"} ], "rows": 5,
  "execution_time_ms": 12, "timestamp": "RFC3339", "truncated": false }
```
`truncated` present only on `POST /query` responses; absent for pipeline execute.

Error envelope (`response.rs:11-38`):
```json
{ "success": false, "error": "msg", "error_type": "sql_validation_error",
  "details": null, "timestamp": "RFC3339" }
```

Endpoints used: `POST /query` (`{"sql": "...", "max_rows": 1000}`), `POST /{name}/execute` (flat param object), `GET /pipelines`, `GET /pipeline/{name}`, `GET /data_source`, `GET /health`, `GET /health/{name}`, `GET /jobs`, `POST /jobs/{name}/run`, `GET /jobs/runs?limit=&job=`, `GET /jobs/runs/{run_id}`, `POST /jobs/runs/{run_id}/cancel`. All return JSON. Auth (when enabled server-side) is `Authorization: Bearer <token>`; 401 body is the error envelope with `error_type: "unauthorized"`.

## File structure (end state)

```
crates/cli/
  Cargo.toml           — thin dep set, no [features] section
  src/
    main.rs            — clap Cli/Commands, tokio main, dispatch, exit codes
    config.rs          — ClientConfig resolution (flag > env > file > default)
    client.rs          — ApiClient + ApiError
    output.rs          — print_result / render_table
    params.rs          — -p parsing, -d sources, merge
    commands/
      mod.rs           — pub mod declarations
      query.rs         — skardi query
      run.rs           — skardi run <name>
      pipeline.rs      — skardi pipeline list|show
      schema.rs        — skardi schema
      health.rs        — skardi health [name]
      jobs.rs          — skardi job … (ported from jobs_cli.rs)
  tests/
    e2e_smoke.rs       — #[ignore] smoke test against a real server
```

Deleted: `src/alias.rs`, `src/alias_store.rs`, `src/pipeline.rs`, `src/jobs_cli.rs`, `tests/influxdb_cli.rs`, and the old `src/main.rs` content (replaced wholesale).

Note on CI: the `rag`/`embedding` feature matrix in `.github/workflows/release.yml` builds **skardi-server** Docker images, not the CLI. The CLI release build (`cargo build --release -p skardi-cli`, release.yml:118) uses default features and keeps working unchanged. No CI edits in this plan.

---

### Task 1: Demolition + thin crate skeleton

Tear out the engine-based CLI and leave a compiling skeleton with the global connection flags. `skardi job` functionality disappears here and is restored in Task 9 (acceptable: this is a rewrite branch).

**Files:**
- Modify: `crates/cli/Cargo.toml` (full replacement)
- Modify: `crates/cli/src/main.rs` (full replacement)
- Delete: `crates/cli/src/alias.rs`, `crates/cli/src/alias_store.rs`, `crates/cli/src/pipeline.rs`, `crates/cli/src/jobs_cli.rs`, `crates/cli/tests/influxdb_cli.rs`

**Interfaces:**
- Produces: a `skardi-cli` crate that compiles with only thin deps; `Cli` struct with global `--server`/`--token` args that later tasks extend with a `Commands` enum.

- [ ] **Step 1: Replace `crates/cli/Cargo.toml`**

```toml
[package]
name = "skardi-cli"
version.workspace = true
edition.workspace = true
description = "Thin HTTP client CLI for skardi-server"
authors.workspace = true
repository.workspace = true
homepage.workspace = true
license.workspace = true

[[bin]]
name = "skardi"
path = "src/main.rs"

[dependencies]
anyhow = { workspace = true }
clap = { version = "4.5", features = ["derive"] }
dirs = "5.0"
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
serde = { workspace = true }
serde_json = { workspace = true }
serde_yaml = { workspace = true }
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }

[dev-dependencies]
tempfile = { workspace = true }
wiremock = "0.6"

[lints]
workspace = true
```

- [ ] **Step 2: Delete the old source files**

```bash
git rm crates/cli/src/alias.rs crates/cli/src/alias_store.rs \
       crates/cli/src/pipeline.rs crates/cli/src/jobs_cli.rs \
       crates/cli/tests/influxdb_cli.rs
```

- [ ] **Step 3: Replace `crates/cli/src/main.rs` with the skeleton**

```rust
//! skardi — thin HTTP client CLI for skardi-server.
//!
//! Every command builds one HTTP request against the server, sends it,
//! and renders the JSON response. There is no local query engine.

use clap::Parser;

#[derive(Parser)]
#[command(name = "skardi", version, about = "Thin HTTP client for skardi-server")]
struct Cli {
    /// Server base URL (overrides $SKARDI_SERVER_URL and ~/.skardi/config.yaml;
    /// default http://127.0.0.1:8080)
    #[arg(long, global = true, value_name = "URL")]
    server: Option<String>,

    /// Bearer token (overrides $SKARDI_API_TOKEN and ~/.skardi/config.yaml)
    #[arg(long, global = true, value_name = "TOKEN")]
    token: Option<String>,
}

fn main() {
    let _cli = Cli::parse();
    // Subcommands are added task-by-task; until then, parsing succeeds but
    // there is nothing to run.
    eprintln!("error: no command specified (see --help)");
    std::process::exit(1);
}
```

- [ ] **Step 4: Verify it builds and old tests are gone**

Run: `cargo build -p skardi-cli && cargo test -p skardi-cli`
Expected: builds cleanly (fast — no datafusion), zero tests run.

Run: `cargo run -p skardi-cli -- --help`
Expected: help text showing `--server` and `--token`.

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add -A crates/cli
git commit -m "refactor(cli): strip local engine, aliases, and features to thin-client skeleton"
```

---

### Task 2: `config.rs` — connection config resolution

**Files:**
- Create: `crates/cli/src/config.rs`
- Modify: `crates/cli/src/main.rs` (add `mod config;`)

**Interfaces:**
- Produces: `config::ClientConfig { pub server: String, pub token: Option<String> }`, `ClientConfig::resolve(flag_server: Option<String>, flag_token: Option<String>) -> ClientConfig`, `config::DEFAULT_SERVER_URL: &str`. Later tasks call `ClientConfig::resolve` once in `main` and pass `&ClientConfig` to `ApiClient::new`.

- [ ] **Step 1: Write the failing tests**

Create `crates/cli/src/config.rs` with the test module first (the code under test does not exist yet, so put the tests at the bottom of the file and stub nothing — write tests, watch the compile fail, then fill in the implementation above them):

```rust
#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    fn file(server: Option<&str>, token: Option<&str>) -> FileConfig {
        FileConfig {
            server: server.map(String::from),
            token: token.map(String::from),
        }
    }

    #[test]
    fn default_when_nothing_set() {
        let cfg = resolve_from(None, None, None, None, None);
        assert_eq!(cfg.server, DEFAULT_SERVER_URL);
        assert_eq!(cfg.token, None);
    }

    #[test]
    fn file_beats_default() {
        let cfg = resolve_from(None, None, None, None, Some(file(Some("http://f:1"), Some("ft"))));
        assert_eq!(cfg.server, "http://f:1");
        assert_eq!(cfg.token.as_deref(), Some("ft"));
    }

    #[test]
    fn env_beats_file() {
        let cfg = resolve_from(
            None,
            None,
            Some("http://e:1".into()),
            Some("et".into()),
            Some(file(Some("http://f:1"), Some("ft"))),
        );
        assert_eq!(cfg.server, "http://e:1");
        assert_eq!(cfg.token.as_deref(), Some("et"));
    }

    #[test]
    fn flag_beats_env_and_file() {
        let cfg = resolve_from(
            Some("http://flag:1".into()),
            Some("flagt".into()),
            Some("http://e:1".into()),
            Some("et".into()),
            Some(file(Some("http://f:1"), Some("ft"))),
        );
        assert_eq!(cfg.server, "http://flag:1");
        assert_eq!(cfg.token.as_deref(), Some("flagt"));
    }

    #[test]
    fn precedence_is_per_field() {
        // Server from env, token from file.
        let cfg = resolve_from(None, None, Some("http://e:1".into()), None, Some(file(None, Some("ft"))));
        assert_eq!(cfg.server, "http://e:1");
        assert_eq!(cfg.token.as_deref(), Some("ft"));
    }

    #[test]
    fn load_valid_manifest() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(
            f,
            "kind: client\nmetadata:\n  name: default\nspec:\n  server: http://file:9\n  token: secret\n"
        )
        .unwrap();
        let loaded = load_file_config(f.path()).unwrap();
        assert_eq!(loaded.server.as_deref(), Some("http://file:9"));
        assert_eq!(loaded.token.as_deref(), Some("secret"));
    }

    #[test]
    fn load_manifest_without_spec_is_empty() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(f, "kind: client\nmetadata:\n  name: default\n").unwrap();
        assert!(load_file_config(f.path()).is_none());
    }

    #[test]
    fn malformed_file_is_ignored() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(f, "kind: [unclosed").unwrap();
        assert!(load_file_config(f.path()).is_none());
    }

    #[test]
    fn missing_file_is_ignored() {
        assert!(load_file_config(std::path::Path::new("/nonexistent/config.yaml")).is_none());
    }
}
```

- [ ] **Step 2: Add `mod config;` to `main.rs` and verify compile failure**

Add `mod config;` after the `//!` doc comments in `main.rs`.

Run: `cargo test -p skardi-cli`
Expected: FAIL to compile — `resolve_from`, `FileConfig`, `DEFAULT_SERVER_URL`, `load_file_config` not found.

- [ ] **Step 3: Write the implementation (top of `config.rs`, above the tests)**

```rust
//! Client connection configuration: server base URL and optional bearer
//! token, resolved per-field as flag > env var > config file > default.

use std::path::{Path, PathBuf};

use serde::Deserialize;

pub const DEFAULT_SERVER_URL: &str = "http://127.0.0.1:8080";

#[derive(Debug, PartialEq)]
pub struct ClientConfig {
    pub server: String,
    pub token: Option<String>,
}

/// The `spec` section of the `kind: client` manifest at `~/.skardi/config.yaml`.
#[derive(Debug, Default, Deserialize, PartialEq)]
struct FileConfig {
    #[serde(default)]
    server: Option<String>,
    #[serde(default)]
    token: Option<String>,
}

#[derive(Deserialize)]
struct Manifest {
    #[serde(default)]
    spec: Option<FileConfig>,
}

impl ClientConfig {
    /// Resolve from CLI flags, the real environment, and `~/.skardi/config.yaml`.
    pub fn resolve(flag_server: Option<String>, flag_token: Option<String>) -> ClientConfig {
        let file = default_config_path().and_then(|p| load_file_config(&p));
        resolve_from(
            flag_server,
            flag_token,
            std::env::var("SKARDI_SERVER_URL").ok(),
            std::env::var("SKARDI_API_TOKEN").ok(),
            file,
        )
    }
}

fn default_config_path() -> Option<PathBuf> {
    dirs::home_dir().map(|h| h.join(".skardi").join("config.yaml"))
}

/// Pure precedence logic, separated from I/O and env for testability.
fn resolve_from(
    flag_server: Option<String>,
    flag_token: Option<String>,
    env_server: Option<String>,
    env_token: Option<String>,
    file: Option<FileConfig>,
) -> ClientConfig {
    let file = file.unwrap_or_default();
    ClientConfig {
        server: flag_server
            .or(env_server)
            .or(file.server)
            .unwrap_or_else(|| DEFAULT_SERVER_URL.to_string()),
        token: flag_token.or(env_token).or(file.token),
    }
}

/// Load the `spec` section from the manifest at `path`. A missing file is
/// normal (returns None); a present-but-malformed file prints a warning to
/// stderr and is treated as absent — never a silent ignore, never fatal.
fn load_file_config(path: &Path) -> Option<FileConfig> {
    let raw = std::fs::read_to_string(path).ok()?;
    match serde_yaml::from_str::<Manifest>(&raw) {
        Ok(manifest) => manifest.spec,
        Err(e) => {
            eprintln!(
                "warning: ignoring malformed config file {}: {e}",
                path.display()
            );
            None
        }
    }
}
```

Note: `ClientConfig::resolve` reads real env vars and is deliberately untested (env is process-global; mutating it in tests races with parallel test threads). All logic lives in the pure `resolve_from`.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p skardi-cli config`
Expected: 9 tests PASS. (`ClientConfig::resolve` and `DEFAULT_SERVER_URL` may warn as unused until Task 6 wires them into main — silence with nothing; the `#[lints] workspace` config treats warnings per workspace policy. If `cargo check` in the pre-commit hook fails on dead_code, add `#[allow(dead_code)]` on `ClientConfig::resolve` with a `// used from main in Task 6` note and remove it in Task 6.)

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add crates/cli
git commit -m "feat(cli): add ClientConfig resolution (flag > env > file > default)"
```

---

### Task 3: `params.rs` — `-p` parsing and `-d` body building

**Files:**
- Create: `crates/cli/src/params.rs`
- Modify: `crates/cli/src/main.rs` (add `mod params;`)

**Interfaces:**
- Produces: `params::parse_param(raw: &str) -> anyhow::Result<(String, serde_json::Value)>` (JSON-first typing) and `params::build_body(data: Option<&str>, params: &[String]) -> anyhow::Result<serde_json::Map<String, serde_json::Value>>` (`-d` base + `-p` overrides). Task 7 (`run`) and Task 9 (`jobs`) consume both.

- [ ] **Step 1: Write the failing tests** (bottom of new `crates/cli/src/params.rs`)

```rust
#[cfg(test)]
mod tests {
    use std::io::Write;

    use serde_json::{Value, json};

    use super::*;

    #[test]
    fn param_values_are_json_first() {
        assert_eq!(parse_param("n=42").unwrap(), ("n".into(), json!(42)));
        assert_eq!(parse_param("f=0.5").unwrap(), ("f".into(), json!(0.5)));
        assert_eq!(parse_param("b=true").unwrap(), ("b".into(), json!(true)));
        assert_eq!(parse_param("z=null").unwrap(), ("z".into(), Value::Null));
        assert_eq!(parse_param("a=[1,2]").unwrap(), ("a".into(), json!([1, 2])));
        // Not valid JSON → plain string.
        assert_eq!(parse_param("s=hello").unwrap(), ("s".into(), json!("hello")));
        // Quoted JSON string stays a string, unquoted.
        assert_eq!(parse_param(r#"q="42""#).unwrap(), ("q".into(), json!("42")));
        // Value may itself contain '=': split on the first one only.
        assert_eq!(parse_param("expr=a=b").unwrap(), ("expr".into(), json!("a=b")));
    }

    #[test]
    fn param_errors() {
        assert!(parse_param("noequals").is_err());
        assert!(parse_param("=value").is_err());
    }

    #[test]
    fn data_must_be_json_object() {
        assert_eq!(parse_data_object(r#"{"a":1}"#).unwrap(), json!({"a":1}).as_object().unwrap().clone());
        let err = parse_data_object("[1,2]").unwrap_err().to_string();
        assert!(err.contains("must be a JSON object"), "{err}");
        assert!(parse_data_object("not json").is_err());
    }

    #[test]
    fn build_body_merges_p_over_d() {
        let body = build_body(Some(r#"{"a":1,"b":"x"}"#), &["b=override".into(), "c=3".into()]).unwrap();
        assert_eq!(Value::Object(body), json!({"a":1,"b":"override","c":3}));
    }

    #[test]
    fn build_body_with_neither_is_empty() {
        assert!(build_body(None, &[]).unwrap().is_empty());
    }

    #[test]
    fn data_from_file() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(f, r#"{{"user_id": 7}}"#).unwrap();
        let arg = format!("@{}", f.path().display());
        let body = build_body(Some(&arg), &[]).unwrap();
        assert_eq!(Value::Object(body), json!({"user_id": 7}));
    }

    #[test]
    fn data_file_missing_is_error() {
        assert!(build_body(Some("@/nonexistent/params.json"), &[]).is_err());
    }
}
```

- [ ] **Step 2: Add `mod params;` to `main.rs`, verify compile failure**

Run: `cargo test -p skardi-cli params`
Expected: FAIL to compile — functions not found.

- [ ] **Step 3: Write the implementation** (top of `params.rs`)

```rust
//! Pipeline/job parameter handling: `-p NAME=VALUE` flags and the
//! `-d/--data` JSON body, merged into the flat JSON object the server's
//! execute endpoints expect.

use std::io::Read;

use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};

/// Parse one `-p NAME=VALUE` flag. The value is parsed as JSON first
/// (numbers, booleans, arrays, null, quoted strings), falling back to a
/// plain string — the server substitutes typed literals into pipeline SQL,
/// so `-p user_id=1` must arrive as a number, not "1".
pub fn parse_param(raw: &str) -> Result<(String, Value)> {
    let (name, value) = raw
        .split_once('=')
        .with_context(|| format!("invalid --param '{raw}': expected NAME=VALUE"))?;
    if name.is_empty() {
        bail!("invalid --param '{raw}': empty parameter name");
    }
    let parsed =
        serde_json::from_str::<Value>(value).unwrap_or_else(|_| Value::String(value.to_string()));
    Ok((name.to_string(), parsed))
}

/// Build the request body: the `-d/--data` object is the base (or `{}`
/// when absent), then each `-p NAME=VALUE` overrides that key.
pub fn build_body(data: Option<&str>, params: &[String]) -> Result<Map<String, Value>> {
    let mut body = match data {
        Some(arg) => parse_data_object(&read_data_arg(arg)?)?,
        None => Map::new(),
    };
    for raw in params {
        let (name, value) = parse_param(raw)?;
        body.insert(name, value);
    }
    Ok(body)
}

/// Resolve a `-d/--data` argument to its raw text: `@path` reads the file,
/// `-` reads stdin, anything else is the JSON itself.
fn read_data_arg(arg: &str) -> Result<String> {
    if arg == "-" {
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .context("failed to read --data from stdin")?;
        Ok(buf)
    } else if let Some(path) = arg.strip_prefix('@') {
        std::fs::read_to_string(path)
            .with_context(|| format!("failed to read --data file '{path}'"))
    } else {
        Ok(arg.to_string())
    }
}

/// The `--data` payload must be a JSON object: the server's execute
/// endpoints take a flat parameter map.
fn parse_data_object(raw: &str) -> Result<Map<String, Value>> {
    let value: Value = serde_json::from_str(raw).context("--data is not valid JSON")?;
    match value {
        Value::Object(map) => Ok(map),
        other => bail!("--data must be a JSON object, got {}", json_type_name(&other)),
    }
}

fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}
```

The stdin path (`-d -`) is thin I/O glue and is covered by the e2e smoke test (Task 10), not unit tests.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p skardi-cli params`
Expected: 7 tests PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add crates/cli
git commit -m "feat(cli): add JSON-first param parsing and -d/--data body building"
```

---

### Task 4: `output.rs` — JSON/table rendering and truncation notice

**Files:**
- Create: `crates/cli/src/output.rs`
- Modify: `crates/cli/src/main.rs` (add `mod output;`)

**Interfaces:**
- Consumes: the server success envelope shape (see "Server API contract").
- Produces: `output::print_result(body: &serde_json::Value, table: bool)` (stdout data + stderr truncation notice) and `output::render_table(rows: &[serde_json::Value]) -> String` (pure, tested). Tasks 6–7 consume `print_result`.

- [ ] **Step 1: Write the failing tests** (bottom of new `crates/cli/src/output.rs`)

```rust
#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn table_aligns_columns_and_renders_nulls_empty() {
        // Keys are alphabetical in serde_json's Map: age, city, name.
        let rows = vec![
            json!({"name": "amsterdam-office", "age": 3, "city": null}),
            json!({"name": "x", "age": 100, "city": "berlin"}),
        ];
        let out = render_table(&rows);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "age | city   | name");
        assert_eq!(lines[1], "----+--------+-----------------");
        assert_eq!(lines[2], "3   |        | amsterdam-office");
        assert_eq!(lines[3], "100 | berlin | x");
    }

    #[test]
    fn table_renders_nested_values_as_compact_json() {
        let rows = vec![json!({"a": [1, 2], "b": {"k": "v"}})];
        let out = render_table(&rows);
        assert!(out.contains("[1,2]"), "{out}");
        assert!(out.contains(r#"{"k":"v"}"#), "{out}");
    }

    #[test]
    fn empty_rows_render_placeholder() {
        assert_eq!(render_table(&[]), "(no rows)\n");
    }

    #[test]
    fn missing_key_in_later_row_renders_empty() {
        let rows = vec![json!({"a": 1, "b": 2}), json!({"a": 3})];
        let out = render_table(&rows);
        let lines: Vec<&str> = out.lines().collect();
        // Missing key renders as an empty cell; lines are trimmed of
        // trailing whitespace, so the row ends at the separator.
        assert_eq!(lines[3], "3 |");
    }
}
```

Rendering rules the tests encode: cells left-aligned to column width, ` | ` between columns, `-+-` joining the `-`-filled separator row, and every emitted line trimmed of trailing whitespace.

- [ ] **Step 2: Add `mod output;` to `main.rs`, verify compile failure**

Run: `cargo test -p skardi-cli output`
Expected: FAIL to compile.

- [ ] **Step 3: Write the implementation** (top of `output.rs`)

```rust
//! Result rendering: raw JSON on stdout by default (scripting-first),
//! aligned ASCII table with `--table`, truncation notice on stderr.

use serde_json::Value;

/// Print a query/pipeline success envelope. The `data` array goes to
/// stdout; the truncation notice (if any) goes to stderr so piped JSON
/// stays clean.
pub fn print_result(body: &Value, table: bool) {
    let empty = Vec::new();
    let rows = body.get("data").and_then(Value::as_array).unwrap_or(&empty);
    if table {
        print!("{}", render_table(rows));
        let n = body
            .get("rows")
            .and_then(Value::as_u64)
            .unwrap_or(rows.len() as u64);
        println!("{n} row(s) returned");
    } else {
        let text = serde_json::to_string_pretty(rows)
            .expect("serde_json::Value serialization cannot fail");
        println!("{text}");
    }
    if body.get("truncated").and_then(Value::as_bool) == Some(true) {
        eprintln!("note: results truncated by the server row cap (rerun with a higher --max-rows)");
    }
}

/// Render flat JSON rows as an aligned ASCII table. Columns come from the
/// first row's keys (alphabetical — serde_json's Map is ordered); nulls
/// and missing keys render as empty cells; nested values render as
/// compact JSON.
pub fn render_table(rows: &[Value]) -> String {
    let Some(first) = rows.first().and_then(Value::as_object) else {
        return "(no rows)\n".to_string();
    };
    let columns: Vec<String> = first.keys().cloned().collect();
    let cells: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .map(|c| cell_text(row.get(c).unwrap_or(&Value::Null)))
                .collect()
        })
        .collect();
    let widths: Vec<usize> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            cells
                .iter()
                .map(|r| r[i].len())
                .chain(std::iter::once(c.len()))
                .max()
                .unwrap_or(0)
        })
        .collect();

    let mut out = String::new();
    let header: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{:<width$}", c, width = widths[i]))
        .collect();
    out.push_str(header.join(" | ").trim_end());
    out.push('\n');
    let separator: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    out.push_str(&separator.join("-+-"));
    out.push('\n');
    for row in &cells {
        let line: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, cell)| format!("{:<width$}", cell, width = widths[i]))
            .collect();
        out.push_str(line.join(" | ").trim_end());
        out.push('\n');
    }
    out
}

fn cell_text(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p skardi-cli output`
Expected: 4 tests PASS. If an alignment assertion fails, print the actual output (`{out}` is in the assert message), fix whichever of test/impl mismatches the rule "cells left-aligned to column width, ` | ` between columns, `-+-` separator row, lines trimmed of trailing whitespace".

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add crates/cli
git commit -m "feat(cli): add JSON/table output rendering with stderr truncation notice"
```

---

### Task 5: `client.rs` — ApiClient with Bearer auth and error mapping

**Files:**
- Create: `crates/cli/src/client.rs`
- Modify: `crates/cli/src/main.rs` (add `mod client;`)

**Interfaces:**
- Consumes: `config::ClientConfig`.
- Produces:
  - `client::ApiClient` with `ApiClient::new(cfg: &ClientConfig) -> anyhow::Result<ApiClient>`, `async fn get(&self, path: &str) -> Result<serde_json::Value, ApiError>`, `async fn post(&self, path: &str, body: &serde_json::Value) -> Result<serde_json::Value, ApiError>`. Paths start with `/` and are joined to the base URL.
  - `client::ApiError` enum: `Connect { url: String, message: String }` and `Http { status: u16, error_type: Option<String>, message: String }`; implements `Display` + `std::error::Error` (so it converts into `anyhow::Error` and can be `downcast_ref` in main for exit codes).

- [ ] **Step 1: Write the failing tests** (bottom of new `crates/cli/src/client.rs`)

```rust
#[cfg(test)]
mod tests {
    use serde_json::json;
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::config::ClientConfig;

    fn client_for(server: &MockServer, token: Option<&str>) -> ApiClient {
        ApiClient::new(&ClientConfig {
            server: server.uri(),
            token: token.map(String::from),
        })
        .unwrap()
    }

    #[tokio::test]
    async fn get_parses_json() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/health"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "healthy"})))
            .mount(&server)
            .await;
        let resp = client_for(&server, None).get("/health").await.unwrap();
        assert_eq!(resp["status"], "healthy");
    }

    #[tokio::test]
    async fn post_sends_body_and_bearer_token() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/query"))
            .and(header("authorization", "Bearer sekrit"))
            .and(body_json(json!({"sql": "SELECT 1"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"success": true})))
            .expect(1)
            .mount(&server)
            .await;
        let resp = client_for(&server, Some("sekrit"))
            .post("/query", &json!({"sql": "SELECT 1"}))
            .await
            .unwrap();
        assert_eq!(resp["success"], true);
    }

    #[tokio::test]
    async fn no_token_means_no_auth_header() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/health"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .mount(&server)
            .await;
        // wiremock can't match on header absence directly; assert via the
        // received request log instead.
        client_for(&server, None).get("/health").await.unwrap();
        let requests = server.received_requests().await.unwrap();
        assert!(requests[0].headers.get("authorization").is_none());
    }

    #[tokio::test]
    async fn error_envelope_is_mapped() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/query"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "success": false,
                "error": "multi-statement not allowed",
                "error_type": "sql_validation_error",
                "details": null,
                "timestamp": "2026-07-23T00:00:00Z"
            })))
            .mount(&server)
            .await;
        let err = client_for(&server, None)
            .post("/query", &json!({"sql": "bad; bad"}))
            .await
            .unwrap_err();
        match &err {
            ApiError::Http { status, error_type, message } => {
                assert_eq!(*status, 400);
                assert_eq!(error_type.as_deref(), Some("sql_validation_error"));
                assert_eq!(message, "multi-statement not allowed");
            }
            other => panic!("expected Http error, got {other:?}"),
        }
        assert!(err.to_string().contains("sql_validation_error"), "{err}");
    }

    #[tokio::test]
    async fn non_json_error_body_uses_first_line() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/jobs"))
            .respond_with(ResponseTemplate::new(503).set_body_string("jobs_disabled\nsecond line"))
            .mount(&server)
            .await;
        let err = client_for(&server, None).get("/jobs").await.unwrap_err();
        match &err {
            ApiError::Http { status, error_type, message } => {
                assert_eq!(*status, 503);
                assert!(error_type.is_none());
                assert_eq!(message, "jobs_disabled");
            }
            other => panic!("expected Http error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unauthorized_display_mentions_token_setup() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/pipelines"))
            .respond_with(ResponseTemplate::new(401).set_body_json(json!({
                "success": false, "error": "invalid session", "error_type": "unauthorized",
                "details": null, "timestamp": "2026-07-23T00:00:00Z"
            })))
            .mount(&server)
            .await;
        let err = client_for(&server, None).get("/pipelines").await.unwrap_err();
        assert!(err.to_string().contains("SKARDI_API_TOKEN"), "{err}");
    }

    #[tokio::test]
    async fn unreachable_server_is_connect_error() {
        // Port 1 is essentially never listening.
        let client = ApiClient::new(&ClientConfig {
            server: "http://127.0.0.1:1".into(),
            token: None,
        })
        .unwrap();
        let err = client.get("/health").await.unwrap_err();
        assert!(matches!(err, ApiError::Connect { .. }), "{err:?}");
        assert!(err.to_string().contains("cannot reach skardi-server"), "{err}");
    }

    #[test]
    fn trailing_slash_on_base_url_is_normalized() {
        let client = ApiClient::new(&ClientConfig {
            server: "http://h:1/".into(),
            token: None,
        })
        .unwrap();
        assert_eq!(client.base, "http://h:1");
    }
}
```

- [ ] **Step 2: Add `mod client;` to `main.rs`, verify compile failure**

Run: `cargo test -p skardi-cli client`
Expected: FAIL to compile.

- [ ] **Step 3: Write the implementation** (top of `client.rs`)

```rust
//! HTTP client for skardi-server: base-URL joining, bearer-token
//! injection, and mapping transport/HTTP failures onto `ApiError`.

use std::fmt;

use anyhow::{Context, Result};
use reqwest::RequestBuilder;
use serde_json::Value;

use crate::config::ClientConfig;

/// Failures talking to the server. `Connect` (unreachable) maps to exit
/// code 2 in main; everything else exits 1.
#[derive(Debug)]
pub enum ApiError {
    /// The server could not be reached at all (connect/DNS/timeout/body IO).
    Connect { url: String, message: String },
    /// The server answered with a non-success status.
    Http {
        status: u16,
        error_type: Option<String>,
        message: String,
    },
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiError::Connect { url, message } => write!(
                f,
                "cannot reach skardi-server at {url} ({message}) — check --server, \
                 SKARDI_SERVER_URL, or ~/.skardi/config.yaml"
            ),
            ApiError::Http { status: 401, .. } => write!(
                f,
                "unauthorized — set SKARDI_API_TOKEN or 'token' in ~/.skardi/config.yaml"
            ),
            ApiError::Http {
                status,
                error_type: Some(t),
                message,
            } => write!(f, "[{t}] {message} (HTTP {status})"),
            ApiError::Http {
                status,
                error_type: None,
                message,
            } => write!(f, "server returned HTTP {status}: {message}"),
        }
    }
}

impl std::error::Error for ApiError {}

pub struct ApiClient {
    http: reqwest::Client,
    base: String,
    token: Option<String>,
}

impl ApiClient {
    pub fn new(cfg: &ClientConfig) -> Result<Self> {
        Ok(ApiClient {
            http: reqwest::Client::builder()
                .build()
                .context("failed to construct HTTP client")?,
            base: cfg.server.trim_end_matches('/').to_string(),
            token: cfg.token.clone(),
        })
    }

    pub async fn get(&self, path: &str) -> Result<Value, ApiError> {
        let url = format!("{}{}", self.base, path);
        let req = self.authorize(self.http.get(&url));
        Self::handle(url, req.send().await).await
    }

    pub async fn post(&self, path: &str, body: &Value) -> Result<Value, ApiError> {
        let url = format!("{}{}", self.base, path);
        let req = self.authorize(self.http.post(&url).json(body));
        Self::handle(url, req.send().await).await
    }

    fn authorize(&self, req: RequestBuilder) -> RequestBuilder {
        match &self.token {
            Some(token) => req.bearer_auth(token),
            None => req,
        }
    }

    async fn handle(
        url: String,
        sent: Result<reqwest::Response, reqwest::Error>,
    ) -> Result<Value, ApiError> {
        let resp = sent.map_err(|e| ApiError::Connect {
            url: url.clone(),
            message: e.to_string(),
        })?;
        let status = resp.status();
        let body = resp.text().await.map_err(|e| ApiError::Connect {
            url,
            message: format!("failed to read response body: {e}"),
        })?;
        if !status.is_success() {
            return Err(Self::map_http_error(status.as_u16(), &body));
        }
        serde_json::from_str(&body).map_err(|e| ApiError::Http {
            status: status.as_u16(),
            error_type: None,
            message: format!("invalid JSON in response: {e}"),
        })
    }

    /// Prefer the server's error envelope ({error, error_type}); fall back
    /// to the first line of a non-JSON body.
    fn map_http_error(status: u16, body: &str) -> ApiError {
        if let Ok(envelope) = serde_json::from_str::<Value>(body) {
            if let Some(message) = envelope.get("error").and_then(Value::as_str) {
                return ApiError::Http {
                    status,
                    error_type: envelope
                        .get("error_type")
                        .and_then(Value::as_str)
                        .map(String::from),
                    message: message.to_string(),
                };
            }
        }
        ApiError::Http {
            status,
            error_type: None,
            message: body.lines().next().unwrap_or("").to_string(),
        }
    }
}
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p skardi-cli client`
Expected: 8 tests PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add crates/cli
git commit -m "feat(cli): add ApiClient with bearer auth and uniform error mapping"
```

---

### Task 6: `skardi query` + main dispatch and exit codes

**Files:**
- Create: `crates/cli/src/commands/mod.rs`, `crates/cli/src/commands/query.rs`
- Modify: `crates/cli/src/main.rs` (full replacement shown below)

**Interfaces:**
- Consumes: `ApiClient::post`, `output::print_result`, `ClientConfig::resolve`.
- Produces: `commands::query::run(client: &ApiClient, sql: Option<String>, file: Option<PathBuf>, max_rows: Option<usize>, table: bool) -> anyhow::Result<()>`; the `Commands` enum and `dispatch` fn in `main.rs` that Tasks 7–9 extend with more variants/arms.

- [ ] **Step 1: Write the failing tests** (bottom of new `crates/cli/src/commands/query.rs`)

```rust
#[cfg(test)]
mod tests {
    use std::io::Write;

    use serde_json::json;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::config::ClientConfig;

    async fn mock_client(server: &MockServer) -> ApiClient {
        ApiClient::new(&ClientConfig {
            server: server.uri(),
            token: None,
        })
        .unwrap()
    }

    #[test]
    fn body_includes_max_rows_only_when_set() {
        assert_eq!(build_request_body("SELECT 1", None), json!({"sql": "SELECT 1"}));
        assert_eq!(
            build_request_body("SELECT 1", Some(50)),
            json!({"sql": "SELECT 1", "max_rows": 50})
        );
    }

    #[tokio::test]
    async fn posts_sql_to_query_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/query"))
            .and(body_json(json!({"sql": "SELECT 1 AS one", "max_rows": 10})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "success": true, "data": [{"one": 1}], "rows": 1,
                "execution_time_ms": 3, "timestamp": "t", "truncated": false
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = mock_client(&server).await;
        run(&client, Some("SELECT 1 AS one".into()), None, Some(10), false)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn file_takes_precedence_over_inline_sql() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(f, "SELECT 2 AS two").unwrap();
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/query"))
            .and(body_json(json!({"sql": "SELECT 2 AS two"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "success": true, "data": [], "rows": 0,
                "execution_time_ms": 1, "timestamp": "t", "truncated": false
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = mock_client(&server).await;
        run(
            &client,
            Some("SELECT 1".into()),
            Some(f.path().to_path_buf()),
            None,
            false,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn missing_sql_is_a_client_side_error() {
        let server = MockServer::start().await;
        let client = mock_client(&server).await;
        let err = run(&client, None, None, None, false).await.unwrap_err();
        assert!(err.to_string().contains("no SQL given"), "{err}");
    }

    #[tokio::test]
    async fn server_error_propagates() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/query"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "success": false, "error": "DDL not allowed",
                "error_type": "sql_validation_error", "details": null, "timestamp": "t"
            })))
            .mount(&server)
            .await;
        let client = mock_client(&server).await;
        let err = run(&client, Some("DROP TABLE x".into()), None, None, false)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("sql_validation_error"), "{err}");
    }
}
```

- [ ] **Step 2: Create `commands/mod.rs`, wire mods, verify compile failure**

`crates/cli/src/commands/mod.rs`:
```rust
pub mod query;
```

Add `mod commands;` to `main.rs`.

Run: `cargo test -p skardi-cli query`
Expected: FAIL to compile — `run`, `build_request_body` not found.

- [ ] **Step 3: Implement `commands/query.rs`** (above the tests)

```rust
//! `skardi query` — run ad-hoc SQL on the server via POST /query.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use crate::client::ApiClient;
use crate::output;

pub async fn run(
    client: &ApiClient,
    sql: Option<String>,
    file: Option<PathBuf>,
    max_rows: Option<usize>,
    table: bool,
) -> Result<()> {
    let sql = match (file, sql) {
        (Some(path), _) => std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read SQL file '{}'", path.display()))?,
        (None, Some(inline)) => inline,
        (None, None) => bail!("no SQL given: pass -e <SQL> or -f <FILE>"),
    };
    let body = build_request_body(&sql, max_rows);
    let resp = client.post("/query", &body).await?;
    output::print_result(&resp, table);
    Ok(())
}

fn build_request_body(sql: &str, max_rows: Option<usize>) -> Value {
    let mut body = json!({ "sql": sql });
    if let Some(n) = max_rows {
        body["max_rows"] = json!(n);
    }
    body
}
```

- [ ] **Step 4: Replace `main.rs` with the dispatching version**

```rust
//! skardi — thin HTTP client CLI for skardi-server.
//!
//! Every command builds one HTTP request against the server, sends it,
//! and renders the JSON response. There is no local query engine.

mod client;
mod commands;
mod config;
mod output;
mod params;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use client::{ApiClient, ApiError};
use config::ClientConfig;

#[derive(Parser)]
#[command(name = "skardi", version, about = "Thin HTTP client for skardi-server")]
struct Cli {
    /// Server base URL (overrides $SKARDI_SERVER_URL and ~/.skardi/config.yaml;
    /// default http://127.0.0.1:8080)
    #[arg(long, global = true, value_name = "URL")]
    server: Option<String>,

    /// Bearer token (overrides $SKARDI_API_TOKEN and ~/.skardi/config.yaml)
    #[arg(long, global = true, value_name = "TOKEN")]
    token: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run ad-hoc SQL on the server (POST /query)
    Query {
        /// SQL text to execute
        #[arg(short = 'e', long = "sql", value_name = "SQL")]
        sql: Option<String>,
        /// Read SQL from a file (takes precedence over -e)
        #[arg(short = 'f', long = "file", value_name = "PATH")]
        file: Option<PathBuf>,
        /// Row cap passed to the server (server default: 1000)
        #[arg(long, value_name = "N")]
        max_rows: Option<usize>,
        /// Render results as an ASCII table instead of JSON
        #[arg(long)]
        table: bool,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let cfg = ClientConfig::resolve(cli.server, cli.token);
    match dispatch(cli.command, &cfg).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            let unreachable_server = e
                .downcast_ref::<ApiError>()
                .is_some_and(|api| matches!(api, ApiError::Connect { .. }));
            if unreachable_server {
                ExitCode::from(2)
            } else {
                ExitCode::from(1)
            }
        }
    }
}

async fn dispatch(command: Commands, cfg: &ClientConfig) -> anyhow::Result<()> {
    let client = ApiClient::new(cfg)?;
    match command {
        Commands::Query {
            sql,
            file,
            max_rows,
            table,
        } => commands::query::run(&client, sql, file, max_rows, table).await,
    }
}
```

Note: `cli.server`/`cli.token` are moved (not cloned) into `resolve` — the struct fields are consumed before `cli.command`. Destructure carefully: `Cli { server, token, command }` if the borrow checker complains, e.g. `let Cli { server, token, command } = Cli::parse();`.

- [ ] **Step 5: Run tests and try it manually**

Run: `cargo test -p skardi-cli`
Expected: all tests from Tasks 2–6 PASS.

Run: `cargo run -p skardi-cli -- query -e "SELECT 1"`
Expected: exit code 2 with `error: cannot reach skardi-server at http://127.0.0.1:8080 (...)` (no server running). Verify: `echo $?` prints `2`.

- [ ] **Step 6: Commit**

```bash
cargo fmt
git add crates/cli
git commit -m "feat(cli): skardi query posts SQL to the server /query endpoint"
```

---

### Task 7: `skardi run <name>` — execute server pipelines

**Files:**
- Create: `crates/cli/src/commands/run.rs`
- Modify: `crates/cli/src/commands/mod.rs`, `crates/cli/src/main.rs`

**Interfaces:**
- Consumes: `params::build_body`, `ApiClient::post`, `output::print_result`, `ApiError`.
- Produces: `commands::run::run(client: &ApiClient, name: &str, data: Option<&str>, param_flags: &[String], table: bool) -> anyhow::Result<()>`.

- [ ] **Step 1: Write the failing tests** (bottom of new `crates/cli/src/commands/run.rs`)

```rust
#[cfg(test)]
mod tests {
    use serde_json::json;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::config::ClientConfig;

    async fn mock_client(server: &MockServer) -> ApiClient {
        ApiClient::new(&ClientConfig {
            server: server.uri(),
            token: None,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn posts_merged_params_to_execute_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/daily_report/execute"))
            .and(body_json(json!({"user_id": 1, "category": "premium"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "success": true, "data": [{"n": 5}], "rows": 1,
                "execution_time_ms": 2, "timestamp": "t"
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = mock_client(&server).await;
        run(
            &client,
            "daily_report",
            Some(r#"{"user_id": 1, "category": "basic"}"#),
            &["category=premium".into()],
            false,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn no_params_sends_empty_object() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/noop/execute"))
            .and(body_json(json!({})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "success": true, "data": [], "rows": 0,
                "execution_time_ms": 1, "timestamp": "t"
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = mock_client(&server).await;
        run(&client, "noop", None, &[], false).await.unwrap();
    }

    #[tokio::test]
    async fn missing_pipeline_gets_friendly_message() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/ghost/execute"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "success": false, "error": "pipeline not found",
                "error_type": "not_found", "details": null, "timestamp": "t"
            })))
            .mount(&server)
            .await;
        let client = mock_client(&server).await;
        let err = run(&client, "ghost", None, &[], false).await.unwrap_err();
        assert!(
            err.to_string().contains("pipeline 'ghost' not found"),
            "{err}"
        );
        assert!(err.to_string().contains("skardi pipeline list"), "{err}");
    }
}
```

- [ ] **Step 2: Wire the module, verify compile failure**

Add `pub mod run;` to `commands/mod.rs`.

Run: `cargo test -p skardi-cli commands::run`
Expected: FAIL to compile.

- [ ] **Step 3: Implement `commands/run.rs`** (above the tests)

```rust
//! `skardi run <name>` — execute a named server pipeline via
//! POST /{name}/execute with a flat JSON parameter object.

use anyhow::{Result, anyhow};
use serde_json::Value;

use crate::client::{ApiClient, ApiError};
use crate::output;
use crate::params;

pub async fn run(
    client: &ApiClient,
    name: &str,
    data: Option<&str>,
    param_flags: &[String],
    table: bool,
) -> Result<()> {
    let body = Value::Object(params::build_body(data, param_flags)?);
    let resp = client
        .post(&format!("/{name}/execute"), &body)
        .await
        .map_err(|e| match e {
            ApiError::Http { status: 404, .. } => {
                anyhow!("pipeline '{name}' not found — try 'skardi pipeline list'")
            }
            other => other.into(),
        })?;
    output::print_result(&resp, table);
    Ok(())
}
```

- [ ] **Step 4: Add the clap variant and dispatch arm in `main.rs`**

Add to the `Commands` enum:

```rust
    /// Execute a named pipeline on the server (POST /<name>/execute)
    Run {
        /// Pipeline name (see `skardi pipeline list`)
        name: String,
        /// Request body as a JSON object: inline, @FILE, or - for stdin
        #[arg(short = 'd', long = "data", value_name = "JSON|@FILE|-")]
        data: Option<String>,
        /// Set one parameter: NAME=VALUE (value parsed as JSON, else string);
        /// overrides keys from --data
        #[arg(short = 'p', long = "param", value_name = "NAME=VALUE")]
        params: Vec<String>,
        /// Render results as an ASCII table instead of JSON
        #[arg(long)]
        table: bool,
    },
```

Add to the `dispatch` match:

```rust
        Commands::Run {
            name,
            data,
            params,
            table,
        } => commands::run::run(&client, &name, data.as_deref(), &params, table).await,
```

- [ ] **Step 5: Run the tests**

Run: `cargo test -p skardi-cli`
Expected: all PASS (3 new).

- [ ] **Step 6: Commit**

```bash
cargo fmt
git add crates/cli
git commit -m "feat(cli): skardi run executes named server pipelines (replaces aliases)"
```

---

### Task 8: Discovery commands — `pipeline`, `schema`, `health`

**Files:**
- Create: `crates/cli/src/commands/pipeline.rs`, `crates/cli/src/commands/schema.rs`, `crates/cli/src/commands/health.rs`
- Modify: `crates/cli/src/commands/mod.rs`, `crates/cli/src/main.rs`

**Interfaces:**
- Consumes: `ApiClient::get`.
- Produces: `commands::pipeline::PipelineCmd` (clap `Subcommand`: `List`, `Show { name: String }`) and `commands::pipeline::run(client, cmd) -> anyhow::Result<()>`; `commands::schema::run(client) -> anyhow::Result<()>`; `commands::health::run(client, name: Option<&str>) -> anyhow::Result<()>`. All pretty-print the server's JSON to stdout.

- [ ] **Step 1: Write the failing tests**

Bottom of new `crates/cli/src/commands/pipeline.rs`:

```rust
#[cfg(test)]
mod tests {
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::config::ClientConfig;

    async fn mock_client(server: &MockServer) -> ApiClient {
        ApiClient::new(&ClientConfig {
            server: server.uri(),
            token: None,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn list_hits_pipelines_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/pipelines"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"pipelines": []})))
            .expect(1)
            .mount(&server)
            .await;
        run(&mock_client(&server).await, PipelineCmd::List).await.unwrap();
    }

    #[tokio::test]
    async fn show_hits_pipeline_by_name() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/pipeline/daily_report"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"name": "daily_report"})))
            .expect(1)
            .mount(&server)
            .await;
        run(
            &mock_client(&server).await,
            PipelineCmd::Show {
                name: "daily_report".into(),
            },
        )
        .await
        .unwrap();
    }
}
```

Bottom of new `crates/cli/src/commands/health.rs`:

```rust
#[cfg(test)]
mod tests {
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::config::ClientConfig;

    #[tokio::test]
    async fn health_paths() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/health"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "healthy"})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/health/daily_report"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "healthy"})))
            .expect(1)
            .mount(&server)
            .await;
        let client = ApiClient::new(&ClientConfig {
            server: server.uri(),
            token: None,
        })
        .unwrap();
        run(&client, None).await.unwrap();
        run(&client, Some("daily_report")).await.unwrap();
    }
}
```

(`schema.rs` is a one-liner GET; its path is covered by the same pattern — add the analogous single test to its module:)

```rust
#[cfg(test)]
mod tests {
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::config::ClientConfig;

    #[tokio::test]
    async fn schema_hits_data_source_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/data_source"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data_sources": []})))
            .expect(1)
            .mount(&server)
            .await;
        let client = ApiClient::new(&ClientConfig {
            server: server.uri(),
            token: None,
        })
        .unwrap();
        run(&client).await.unwrap();
    }
}
```

- [ ] **Step 2: Wire modules, verify compile failure**

`commands/mod.rs` becomes:
```rust
pub mod health;
pub mod pipeline;
pub mod query;
pub mod run;
pub mod schema;
```

Run: `cargo test -p skardi-cli`
Expected: FAIL to compile.

- [ ] **Step 3: Implement the three modules**

`crates/cli/src/commands/pipeline.rs` (above tests):

```rust
//! `skardi pipeline list|show` — pipeline discovery endpoints.

use anyhow::Result;
use clap::Subcommand;

use crate::client::ApiClient;

#[derive(Subcommand)]
pub enum PipelineCmd {
    /// List pipelines registered on the server (GET /pipelines)
    List,
    /// Show one pipeline's metadata and parameters (GET /pipeline/<name>)
    Show {
        /// Pipeline name
        name: String,
    },
}

pub async fn run(client: &ApiClient, cmd: PipelineCmd) -> Result<()> {
    let resp = match cmd {
        PipelineCmd::List => client.get("/pipelines").await?,
        PipelineCmd::Show { name } => client.get(&format!("/pipeline/{name}")).await?,
    };
    println!("{}", serde_json::to_string_pretty(&resp)?);
    Ok(())
}
```

`crates/cli/src/commands/schema.rs` (above tests):

```rust
//! `skardi schema` — GET /data_source: registered sources and their schemas.

use anyhow::Result;

use crate::client::ApiClient;

pub async fn run(client: &ApiClient) -> Result<()> {
    let resp = client.get("/data_source").await?;
    println!("{}", serde_json::to_string_pretty(&resp)?);
    Ok(())
}
```

`crates/cli/src/commands/health.rs` (above tests):

```rust
//! `skardi health [name]` — server liveness (GET /health) or per-pipeline
//! health including data-source accessibility (GET /health/<name>).

use anyhow::Result;

use crate::client::ApiClient;

pub async fn run(client: &ApiClient, name: Option<&str>) -> Result<()> {
    let path = match name {
        Some(pipeline) => format!("/health/{pipeline}"),
        None => "/health".to_string(),
    };
    let resp = client.get(&path).await?;
    println!("{}", serde_json::to_string_pretty(&resp)?);
    Ok(())
}
```

- [ ] **Step 4: Add clap variants and dispatch arms in `main.rs`**

Add `use commands::pipeline::PipelineCmd;` to the imports. Add to `Commands`:

```rust
    /// Pipeline discovery (list, show)
    Pipeline {
        #[command(subcommand)]
        cmd: PipelineCmd,
    },
    /// Show registered data sources and their schemas (GET /data_source)
    Schema,
    /// Check server health, or one pipeline's health (GET /health[/<name>])
    Health {
        /// Pipeline name (omit for whole-server health)
        name: Option<String>,
    },
```

Add to `dispatch`:

```rust
        Commands::Pipeline { cmd } => commands::pipeline::run(&client, cmd).await,
        Commands::Schema => commands::schema::run(&client).await,
        Commands::Health { name } => commands::health::run(&client, name.as_deref()).await,
```

- [ ] **Step 5: Run the tests**

Run: `cargo test -p skardi-cli`
Expected: all PASS (4 new).

- [ ] **Step 6: Commit**

```bash
cargo fmt
git add crates/cli
git commit -m "feat(cli): add pipeline/schema/health discovery commands"
```

---

### Task 9: `skardi job` — port onto ApiClient

Restores the job commands deleted in Task 1, rewired onto the shared client so global `--server`/`--token`/config-file resolution applies. Behavior change (intentional, spec "clean break"): the old `NAME:TYPE=VALUE` typed-param syntax is gone; values are JSON-first like `skardi run`. Output formats are unchanged from the old `jobs_cli.rs`.

**Files:**
- Create: `crates/cli/src/commands/jobs.rs`
- Modify: `crates/cli/src/commands/mod.rs`, `crates/cli/src/main.rs`

**Interfaces:**
- Consumes: `ApiClient::{get, post}`, `params::build_body`.
- Produces: `commands::jobs::JobCmd` (clap `Subcommand`) and `commands::jobs::run(client: &ApiClient, cmd: JobCmd) -> anyhow::Result<()>`.

- [ ] **Step 1: Write the failing tests** (bottom of new `crates/cli/src/commands/jobs.rs`)

```rust
#[cfg(test)]
mod tests {
    use serde_json::json;
    use wiremock::matchers::{body_json, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::config::ClientConfig;

    async fn mock_client(server: &MockServer) -> ApiClient {
        ApiClient::new(&ClientConfig {
            server: server.uri(),
            token: None,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn job_run_posts_typed_params() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/jobs/nightly_sync/run"))
            .and(body_json(json!({"day": "2026-07-23", "batch": 500})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "run_id": "r-1", "status": "pending"
            })))
            .expect(1)
            .mount(&server)
            .await;
        run(
            &mock_client(&server).await,
            JobCmd::Run {
                job: "nightly_sync".into(),
                params: vec!["day=2026-07-23".into(), "batch=500".into()],
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn job_list_passes_limit_and_filter() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/jobs/runs"))
            .and(query_param("limit", "5"))
            .and(query_param("job", "nightly_sync"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"runs": []})))
            .expect(1)
            .mount(&server)
            .await;
        run(
            &mock_client(&server).await,
            JobCmd::List {
                job: Some("nightly_sync".into()),
                limit: 5,
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn job_status_and_cancel_hit_run_endpoints() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/jobs/runs/r-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"run_id": "r-1"})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/jobs/runs/r-1/cancel"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"cancelled": true})))
            .expect(1)
            .mount(&server)
            .await;
        let client = mock_client(&server).await;
        run(&client, JobCmd::Status { run_id: "r-1".into() }).await.unwrap();
        run(&client, JobCmd::Cancel { run_id: "r-1".into() }).await.unwrap();
    }

    #[tokio::test]
    async fn job_show_lists_jobs() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/jobs"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"jobs": []})))
            .expect(1)
            .mount(&server)
            .await;
        run(&mock_client(&server).await, JobCmd::Show).await.unwrap();
    }
}
```

- [ ] **Step 2: Wire the module, verify compile failure**

Add `pub mod jobs;` to `commands/mod.rs`.

Run: `cargo test -p skardi-cli jobs`
Expected: FAIL to compile.

- [ ] **Step 3: Implement `commands/jobs.rs`** (above the tests; structure and output carried over from the deleted `jobs_cli.rs`, transport swapped to `ApiClient`)

```rust
//! `skardi job ...` subcommands — clients for the server's `/jobs/*` API.
//!
//! Jobs run only inside skardi-server. `job run` returns a run_id
//! immediately; poll with `skardi job status <run_id>`.

use anyhow::Result;
use clap::Subcommand;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::client::ApiClient;
use crate::params;

#[derive(Subcommand)]
pub enum JobCmd {
    /// Submit a new run of the named job; returns the run_id immediately.
    Run {
        /// Job name (from `metadata.name` in the job YAML)
        job: String,
        /// Bind a parameter: NAME=VALUE (value parsed as JSON, else string)
        #[arg(short = 'p', long = "param", value_name = "NAME=VALUE")]
        params: Vec<String>,
    },
    /// Print the current status of one run.
    Status {
        /// Run id returned by `skardi job run`
        run_id: String,
    },
    /// List recent runs. Pass --job to filter by job name.
    List {
        #[arg(long)]
        job: Option<String>,
        #[arg(long, default_value = "20")]
        limit: usize,
    },
    /// Request cancellation of an in-progress run.
    Cancel {
        run_id: String,
    },
    /// List every job the server knows about and its destination.
    Show,
}

#[derive(Debug, Deserialize)]
struct RunIdResponse {
    run_id: String,
    #[serde(default)]
    status: Option<String>,
}

pub async fn run(client: &ApiClient, cmd: JobCmd) -> Result<()> {
    match cmd {
        JobCmd::Run { job, params: flags } => {
            let body = Value::Object(params::build_body(None, &flags)?);
            let resp = client.post(&format!("/jobs/{job}/run"), &body).await?;
            let parsed: RunIdResponse = serde_json::from_value(resp)?;
            println!(
                "submitted: {} ({})",
                parsed.run_id,
                parsed.status.as_deref().unwrap_or("pending")
            );
        }
        JobCmd::Status { run_id } => {
            let resp = client.get(&format!("/jobs/runs/{run_id}")).await?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
        }
        JobCmd::List { job, limit } => {
            let mut path = format!("/jobs/runs?limit={limit}");
            if let Some(name) = job {
                path.push_str(&format!("&job={}", urlencode(&name)));
            }
            let resp = client.get(&path).await?;
            print_run_list(&resp);
        }
        JobCmd::Cancel { run_id } => {
            let resp = client
                .post(&format!("/jobs/runs/{run_id}/cancel"), &Value::Object(Map::new()))
                .await?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
        }
        JobCmd::Show => {
            let resp = client.get("/jobs").await?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
        }
    }
    Ok(())
}

fn urlencode(s: &str) -> String {
    // Minimal url-encoding: only escape the characters most likely to
    // appear in a job name (`/`, `&`, `?`). Good enough for a CLI.
    s.replace('&', "%26").replace('?', "%3F").replace('/', "%2F")
}

fn print_run_list(resp: &Value) {
    let Some(runs) = resp.get("runs").and_then(Value::as_array) else {
        println!("{}", serde_json::to_string_pretty(resp).unwrap_or_default());
        return;
    };
    if runs.is_empty() {
        println!("(no runs)");
        return;
    }
    for run in runs {
        let id = run.get("run_id").and_then(Value::as_str).unwrap_or("?");
        let job = run.get("job").and_then(Value::as_str).unwrap_or("?");
        let status = run.get("status").and_then(Value::as_str).unwrap_or("?");
        let created = run.get("created_at").and_then(Value::as_str).unwrap_or("?");
        let rows = run
            .get("rows_written")
            .and_then(Value::as_u64)
            .map(|n| n.to_string())
            .unwrap_or_else(|| "-".to_string());
        println!("{id}  {status:<10}  {job:<25}  rows={rows:<8}  created_at={created}");
    }
}
```

- [ ] **Step 4: Add the clap variant and dispatch arm in `main.rs`**

Add `use commands::jobs::JobCmd;` to the imports. Add to `Commands`:

```rust
    /// Server-side jobs (submit, poll, cancel)
    Job {
        #[command(subcommand)]
        cmd: JobCmd,
    },
```

Add to `dispatch`:

```rust
        Commands::Job { cmd } => commands::jobs::run(&client, cmd).await,
```

- [ ] **Step 5: Run the tests**

Run: `cargo test -p skardi-cli`
Expected: all PASS (4 new).

- [ ] **Step 6: Commit**

```bash
cargo fmt
git add crates/cli
git commit -m "feat(cli): port job commands onto shared ApiClient with global connection flags"
```

---

### Task 10: E2E smoke test, docs, and final verification

**Files:**
- Create: `crates/cli/tests/e2e_smoke.rs`
- Modify: `docs/cli.md` (full replacement), `README.md` (CLI usage sections only)

**Interfaces:**
- Consumes: the built `skardi` binary via `CARGO_BIN_EXE_skardi`.

- [ ] **Step 1: Add the ignored e2e smoke test**

`crates/cli/tests/e2e_smoke.rs`:

```rust
//! End-to-end smoke test against a real skardi-server.
//!
//! Run manually: start a server with at least one data source, then
//!   SKARDI_SERVER_URL=http://127.0.0.1:8080 \
//!     cargo test -p skardi-cli --test e2e_smoke -- --ignored

use std::io::Write;
use std::process::{Command, Stdio};

#[test]
#[ignore = "requires a running skardi-server"]
fn query_select_1_roundtrip() {
    let out = Command::new(env!("CARGO_BIN_EXE_skardi"))
        .args(["query", "-e", "SELECT 1 AS one"])
        .output()
        .expect("failed to spawn skardi binary");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("\"one\""), "unexpected stdout: {stdout}");
}

#[test]
#[ignore = "requires a running skardi-server"]
fn data_via_stdin_reaches_server() {
    // Exercises the `-d -` stdin path that unit tests skip. Expects a 404
    // (pipeline won't exist) — the point is that stdin parsing works and
    // the friendly not-found message appears.
    let mut child = Command::new(env!("CARGO_BIN_EXE_skardi"))
        .args(["run", "definitely_not_a_pipeline", "-d", "-"])
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn skardi binary");
    child
        .stdin
        .as_mut()
        .expect("stdin piped")
        .write_all(br#"{"x": 1}"#)
        .expect("write stdin");
    let out = child.wait_with_output().expect("wait");
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("not found"), "stderr: {stderr}");
}
```

Run: `cargo test -p skardi-cli` — the new tests compile but are skipped (ignored). Expected: all non-ignored tests PASS.

- [ ] **Step 2: Rewrite `docs/cli.md`**

Replace the entire file with:

```markdown
# skardi CLI

The `skardi` CLI is a thin HTTP client for skardi-server. Every command
sends one request to the server and prints the JSON response — there is no
local query engine. Start a server first (see `docs/server.md`).

## Connecting

Per-field precedence: flag > environment > config file > default.

| | Server URL | Bearer token |
|---|---|---|
| Flag | `--server <URL>` | `--token <TOKEN>` |
| Env | `SKARDI_SERVER_URL` | `SKARDI_API_TOKEN` |
| File | `spec.server` | `spec.token` |
| Default | `http://127.0.0.1:8080` | none |

Config file `~/.skardi/config.yaml`:

```yaml
kind: client
metadata:
  name: default
spec:
  server: http://127.0.0.1:8080
  token: my-secret-token   # optional; sent as Authorization: Bearer
```

## Commands

### Ad-hoc SQL

```bash
skardi query -e "SELECT * FROM users LIMIT 5"
skardi query -f report.sql --max-rows 5000
skardi query -e "SELECT 1" --table          # ASCII table instead of JSON
```

Output is the result rows as pretty-printed JSON on stdout (pipe into
`jq`). If the server truncates results at the row cap, a notice goes to
stderr. `--max-rows` raises/lowers the cap (server default 1000).

### Pipelines

Pipelines are named, parameterized queries defined on the server (YAML
manifests loaded with `--pipeline`). What used to be CLI aliases are now
just pipeline names:

```bash
skardi pipeline list                 # what's available
skardi pipeline show daily_report    # its parameters
skardi run daily_report -p user_id=1 -p category=premium
skardi run daily_report -d '{"user_id": 1, "category": "premium"}'
skardi run daily_report -d @params.json -p user_id=2   # -p overrides -d keys
echo '{"user_id": 3}' | skardi run daily_report -d -
```

`-p` values are parsed as JSON first (`-p user_id=1` sends a number,
`-p ids=[1,2]` an array), falling back to plain strings.

### Discovery and health

```bash
skardi schema            # registered data sources and their schemas
skardi health            # server liveness
skardi health daily_report   # one pipeline incl. data-source access
```

### Jobs

```bash
skardi job show                  # jobs the server knows about
skardi job run nightly_sync -p day=2026-07-23
skardi job status <run_id>
skardi job list --job nightly_sync --limit 10
skardi job cancel <run_id>
```

## Exit codes

| Code | Meaning |
|---|---|
| 0 | success |
| 1 | server rejected the request, or a client-side error |
| 2 | server unreachable (check `--server` / `SKARDI_SERVER_URL`) |

## Migrating from the pre-thin-client CLI

| Old | New |
|---|---|
| `skardi query --ctx ctx.yaml -e "SQL"` (local engine) | register the sources on a server, then `skardi query -e "SQL"` |
| `skardi query --schema` | `skardi schema` |
| `skardi run pipeline.yaml -p k=v` (local YAML) | load the YAML into the server (`--pipeline`), then `skardi run <name> -p k=v` |
| `skardi alias add myverb --pipeline …` + `skardi myverb` | no aliases — `skardi run <pipeline-name>` |
| `--features embedding` / `rag` CLI builds | gone; embedding/rag features live in skardi-server |
```

- [ ] **Step 3: Update `README.md` CLI sections**

Search README.md for old CLI usage:

Run: `grep -n "skardi query\|skardi run\|skardi alias\|--ctx" README.md`

For each hit, apply the migration table from Step 2 (e.g. `skardi query --ctx … -e "SQL"` → `skardi query -e "SQL"` with a note that sources are registered on the server; delete alias examples in favor of `skardi run <name>`). Keep server-related content untouched. Do NOT touch the per-source docs (`docs/lance/`, `docs/sqlite/`, `docs/basic/`, `docs/clickhouse/`, `docs/dynamodb/`, `docs/redis/`, `docs/embeddings/`, `docs/S3_USAGE.md`, `docs/semantics.md`, `docs/agent_data_plane.md`, `docs/jobs.md`) — they document local-engine workflows end-to-end and need their own migration pass; that is explicitly deferred follow-up work, not part of this plan.

- [ ] **Step 4: Final verification**

Run each and confirm:

```bash
cargo build -p skardi-cli                      # builds, fast
cargo test -p skardi-cli                       # all tests pass, ignored tests skipped
cargo clippy -p skardi-cli --all-targets       # no warnings
cargo build --workspace                        # server + engine still build
```

Optional full loop if a server can be started locally:

```bash
cargo run -p skardi-server -- --port 8080 &    # adjust to the server's actual flags/sources
SKARDI_SERVER_URL=http://127.0.0.1:8080 cargo test -p skardi-cli --test e2e_smoke -- --ignored
```

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add crates/cli docs/cli.md README.md
git commit -m "feat(cli): add e2e smoke test and rewrite CLI docs for thin-client surface"
```

---

## Deferred follow-ups (explicitly out of scope)

- Migrating per-source demo docs (`docs/lance/`, `docs/sqlite/`, `docs/basic/`, `docs/clickhouse/`, `docs/dynamodb/`, `docs/redis/`, `docs/embeddings/`, `docs/S3_USAGE.md`, `docs/semantics.md`) from `skardi query --ctx` local-engine workflows to server-based workflows.
- Any release-workflow changes: none are needed (the CLI release build uses default features; the `rag` feature matrix builds skardi-server Docker images).
