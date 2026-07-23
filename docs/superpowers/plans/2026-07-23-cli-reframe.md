# CLI Reframe (Thin HTTP Client) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> This plan intentionally contains no Rust implementation code. Each task specifies files, interfaces (names, parameters, return types), behaviors, and test cases; the implementer writes the code, test-first.

**Goal:** Rewrite `crates/cli` as a pure HTTP client for skardi-server: `query` posts SQL to `POST /query`, `run <name>` posts params to `POST /{name}/execute`, plus `pipeline`/`schema`/`health` discovery commands; the local DataFusion engine, alias system, and all cargo features are deleted.

**Architecture:** One shared `ApiClient` (reqwest, Bearer auth, uniform error mapping) and `ClientConfig` (flag > env > `~/.skardi/config.yaml` > default) feed one small module per command group. `main.rs` holds only clap definitions and dispatch. Output is raw JSON to stdout by default; `--table` renders a hand-rolled ASCII table; notices go to stderr.

**Tech Stack:** Rust, clap 4 (derive), reqwest 0.12 (json + rustls-tls), tokio, serde/serde_json/serde_yaml, anyhow, dirs. Dev: wiremock 0.6, tempfile.

**Spec:** `docs/superpowers/specs/2026-07-23-cli-reframe-design.md` — read it before starting; it is the authority on UX and semantics.

## Global Constraints

- No `.unwrap()` outside `#[cfg(test)]` modules and `#[test]` fns — even though `crates/cli` is formally exempt per `.claude/CLAUDE.md`, this rewrite holds to `Result` + `anyhow::Context` (spec: "Error handling" section). `.expect()` only for documented invariants.
- Import types with `use` at the top of the file; never full crate paths inline in function bodies.
- Default server URL is exactly `http://127.0.0.1:8080`. Env vars are exactly `SKARDI_SERVER_URL` and `SKARDI_API_TOKEN`. Config file is exactly `~/.skardi/config.yaml`.
- Exit codes: 0 success, 2 connection failure (server unreachable), 1 everything else.
- stdout carries only result data (JSON or table); warnings/truncation notices go to stderr.
- The server API is used as-is; no server changes in this plan.
- Workspace `serde_json` has no `preserve_order`, so JSON object keys iterate alphabetically — table column order is alphabetical and deterministic. Do not add `preserve_order`.
- TDD per task: write the failing tests first, watch them fail, implement, watch them pass. Unit/integration tests live in in-module `#[cfg(test)]` blocks (this is a binary-only crate; there is no lib target for external test files to import — only the e2e test in `tests/` runs the built binary).
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
- Produces: a `skardi-cli` crate that compiles with only thin deps; a clap `Cli` struct with global `--server <URL>` and `--token <TOKEN>` args (both `global = true`, both optional) that later tasks extend with a `Commands` subcommand enum.

- [ ] **Step 1: Replace `crates/cli/Cargo.toml`**

Keep the `[package]` metadata (workspace-inherited fields) and the `[[bin]] name = "skardi"` section and `[lints] workspace = true`. Delete the entire `[features]` section. Dependencies become exactly: `anyhow` (workspace), `clap` 4.5 with `derive`, `dirs` 5.0, `reqwest` 0.12 with default-features off and `json` + `rustls-tls`, `serde`/`serde_json`/`serde_yaml` (workspace), `tokio` (workspace, `macros` + `rt-multi-thread`). Dev-dependencies: `tempfile` (workspace), `wiremock` 0.6. Removed relative to today: `arrow`, `async-trait`, `datafusion`, `datafusion-catalog`, `datafusion-session`, `lance`, `object_store`, `skardi`, `url`.

- [ ] **Step 2: Delete the old source files**

```bash
git rm crates/cli/src/alias.rs crates/cli/src/alias_store.rs \
       crates/cli/src/pipeline.rs crates/cli/src/jobs_cli.rs \
       crates/cli/tests/influxdb_cli.rs
```

- [ ] **Step 3: Replace `crates/cli/src/main.rs` with the skeleton**

A module doc comment stating the thin-client contract (every command = one HTTP request; no local engine), the `Cli` parser struct with the two global args and their help text (server: "overrides $SKARDI_SERVER_URL and ~/.skardi/config.yaml; default http://127.0.0.1:8080"; token: "overrides $SKARDI_API_TOKEN and ~/.skardi/config.yaml"), and a plain (non-async) `main` that parses args, prints `error: no command specified (see --help)` to stderr, and exits 1. Subcommands arrive in Task 6.

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
- Produces (all in `config`):
  - `DEFAULT_SERVER_URL: &str` = `"http://127.0.0.1:8080"`.
  - `ClientConfig` with public fields `server: String` and `token: Option<String>`.
  - `ClientConfig::resolve(flag_server: Option<String>, flag_token: Option<String>) -> ClientConfig` — reads the real env vars and the default config path; called once from `main` in Task 6.
  - Private helpers: `resolve_from(...)` (pure precedence logic taking flag/env/file values as parameters — no I/O, no env reads, so it is unit-testable), `load_file_config(path: &Path)` returning the parsed spec or nothing, and a `FileConfig` deserialization struct with optional `server`/`token`.

**Behavior:**
- Config file format is the repo's manifest style; only `spec` matters for parsing:

```yaml
kind: client
metadata:
  name: default
spec:
  server: http://127.0.0.1:8080
  token: optional-bearer-token
```

- Precedence is **per field**: flag > env > file > default. The default token is none.
- A missing file resolves silently to nothing. A present-but-unparsable file prints a `warning: ignoring malformed config file <path>: <parse error>` to stderr and resolves to nothing — never fatal, never silent.
- `ClientConfig::resolve` itself stays untested (env is process-global; mutating it races parallel tests). All logic lives in `resolve_from`.

- [ ] **Step 1: Write the failing tests** (in-module `#[cfg(test)]` in `config.rs`, then `mod config;` in `main.rs`)

Test cases:
1. Nothing set → server is `DEFAULT_SERVER_URL`, token is none.
2. File only → both fields come from the file.
3. Env beats file (both fields).
4. Flag beats env and file (both fields).
5. Per-field independence: server from env while token comes from file.
6. `load_file_config` on a valid tempfile manifest returns the spec values.
7. Manifest without a `spec` section → treated as absent.
8. Malformed YAML tempfile → treated as absent (warning behavior is eyeballed, not asserted).
9. Nonexistent path → treated as absent.

Run: `cargo test -p skardi-cli config`
Expected: FAIL to compile (functions/types not defined yet).

- [ ] **Step 2: Implement** the constants, structs, and the three functions per the behavior above.

- [ ] **Step 3: Run the tests**

Run: `cargo test -p skardi-cli config`
Expected: 9 tests PASS. If the pre-commit `cargo check` complains that `ClientConfig::resolve` is dead code (unused until Task 6), add a temporary `#[allow(dead_code)]` with a comment naming Task 6, and remove it there.

- [ ] **Step 4: Commit**

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
- Produces (in `params`; Tasks 7 and 9 consume the first two):
  - `parse_param(raw: &str) -> anyhow::Result<(String, serde_json::Value)>` — parses one `NAME=VALUE` token.
  - `build_body(data: Option<&str>, params: &[String]) -> anyhow::Result<serde_json::Map<String, serde_json::Value>>` — the full `-d` + `-p` pipeline.
  - Private helpers: one that resolves a `-d` argument to raw text (`@path` reads the file with a contextual error, `-` reads stdin, anything else is the JSON itself) and one that parses that text and requires a JSON **object**, erroring with a message naming the actual type (e.g. "must be a JSON object, got an array").

**Behavior:**
- `parse_param` splits on the **first** `=` (values may contain `=`); empty name is an error; missing `=` is an error naming the token. The value is parsed as JSON first (numbers, booleans, arrays, null, quoted strings), falling back to a plain string — the server substitutes typed literals into pipeline SQL, so `user_id=1` must arrive as a number.
- `build_body`: the `-d` object is the base (or an empty object when absent); each `-p` then overrides that key. Neither flag → empty object.
- The stdin path (`-d -`) is thin I/O glue covered by the e2e smoke test (Task 10), not unit tests.

- [ ] **Step 1: Write the failing tests** (in-module; add `mod params;` to `main.rs`)

Test cases for `parse_param`:
1. `n=42` → number; `f=0.5` → number; `b=true` → bool; `z=null` → null; `a=[1,2]` → array.
2. `s=hello` (not valid JSON) → the string `hello`.
3. `q="42"` (quoted JSON string) → the string `42`, unquoted.
4. `expr=a=b` → name `expr`, string value `a=b` (split on first `=` only).
5. `noequals` and `=value` → errors.

Test cases for the data-object parser and `build_body`:
6. `{"a":1}` parses; `[1,2]` errors mentioning "must be a JSON object"; `not json` errors.
7. Merge: data `{"a":1,"b":"x"}` + params `b=override`, `c=3` → `{"a":1,"b":"override","c":3}`.
8. Neither `-d` nor `-p` → empty object.
9. `-d @<tempfile>` containing `{"user_id": 7}` → that object.
10. `-d @/nonexistent/params.json` → error.

Run: `cargo test -p skardi-cli params` — expected: FAIL to compile.

- [ ] **Step 2: Implement** per the behavior above.

- [ ] **Step 3: Run the tests** — `cargo test -p skardi-cli params`, expected all PASS.

- [ ] **Step 4: Commit**

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
- Produces (in `output`; Tasks 6–7 consume `print_result`):
  - `print_result(body: &serde_json::Value, table: bool)` — renders a success envelope.
  - `render_table(rows: &[serde_json::Value]) -> String` — pure, fully unit-tested.

**Behavior:**
- `print_result` extracts the envelope's `data` array (missing/non-array → empty). Default mode prints exactly that array, pretty-printed, to stdout — nothing else, so pipes into `jq` cleanly. Table mode prints `render_table` output followed by `<n> row(s) returned`, where `n` prefers the envelope's `rows` field, falling back to the array length. In both modes, if the envelope has `truncated: true`, print a notice to **stderr** suggesting a higher `--max-rows`.
- `render_table` rules (the tests encode these exactly):
  - Columns are the first row's keys, in that map's iteration order (alphabetical — see Global Constraints).
  - Cells: null and missing keys render empty; strings render bare (unquoted); numbers/bools via display; nested arrays/objects as **compact** JSON.
  - Layout: cells left-aligned and padded to the column's max width (header included), columns joined with ` | `, a separator row of `-` runs joined with `-+-`, and every emitted line trimmed of trailing whitespace.
  - Empty input renders the literal line `(no rows)`.

- [ ] **Step 1: Write the failing tests** (in-module; add `mod output;` to `main.rs`)

Test cases for `render_table`:
1. Two rows with keys `age`/`city`/`name`, mixed widths, one null city — assert the exact four lines (header, separator, two data rows) including alignment.
2. A row with an array and an object value — assert compact JSON (`[1,2]`, `{"k":"v"}`) appears.
3. Empty slice → exactly `(no rows)\n`.
4. Second row missing a key present in the first → that cell renders empty (and the line is right-trimmed).

`print_result` prints to real stdout/stderr and stays untested at unit level (covered by command tests asserting it doesn't panic, and by the e2e test).

Run: `cargo test -p skardi-cli output` — expected: FAIL to compile.

- [ ] **Step 2: Implement** per the behavior above. When writing test 1's expected strings, derive them by hand from the alignment rules; if implementation and expectation disagree, the rules in **Behavior** win.

- [ ] **Step 3: Run the tests** — `cargo test -p skardi-cli output`, expected all PASS.

- [ ] **Step 4: Commit**

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
- Produces (in `client`):
  - `ApiClient` holding a reqwest client, the base URL (trailing `/` stripped at construction), and the optional token.
  - `ApiClient::new(cfg: &ClientConfig) -> anyhow::Result<ApiClient>`.
  - `async fn get(&self, path: &str) -> Result<serde_json::Value, ApiError>` and `async fn post(&self, path: &str, body: &serde_json::Value) -> Result<serde_json::Value, ApiError>` — `path` starts with `/` and is appended to the base URL.
  - `ApiError` enum, deriving Debug and implementing `Display` + `std::error::Error` (so it converts into `anyhow::Error` and `main` can `downcast_ref` it for exit codes):
    - `Connect { url: String, message: String }` — request never got a response (connect/DNS/timeout), or the response body could not be read.
    - `Http { status: u16, error_type: Option<String>, message: String }` — non-success status, or unparsable success body.

**Behavior:**
- When a token is present, every request carries `Authorization: Bearer <token>`; when absent, no auth header at all.
- Non-success responses: try the server's error envelope first — if the body is JSON with a string `error` field, use it as `message` and lift `error_type`; otherwise `error_type` is none and `message` is the first line of the raw body.
- Success responses with unparsable JSON map to `Http` with a message naming the parse failure.
- `Display` messages (spec "Error handling" table):
  - `Connect` → `cannot reach skardi-server at <url> (<message>) — check --server, SKARDI_SERVER_URL, or ~/.skardi/config.yaml`.
  - `Http` with status 401 (any envelope) → `unauthorized — set SKARDI_API_TOKEN or 'token' in ~/.skardi/config.yaml`.
  - `Http` with an `error_type` → `[<error_type>] <message> (HTTP <status>)`.
  - `Http` without → `server returned HTTP <status>: <message>`.

- [ ] **Step 1: Write the failing tests** (in-module, `#[tokio::test]` + wiremock; add `mod client;` to `main.rs`)

Test cases:
1. GET against a wiremock route returns the parsed JSON.
2. POST sends the JSON body and, with a token configured, the exact `Authorization: Bearer <token>` header (wiremock `header` matcher + `.expect(1)`).
3. With no token, the received request has no `authorization` header (assert via `server.received_requests()`).
4. A 400 whose body is the server error envelope maps to `Http { status: 400, error_type: Some("sql_validation_error"), message: <envelope error> }`, and its Display contains the error_type.
5. A 503 with a plain-text multi-line body maps to `Http` with `error_type: None` and message = first line.
6. A 401 envelope's Display mentions `SKARDI_API_TOKEN`.
7. A client pointed at `http://127.0.0.1:1` (nothing listens) yields `Connect`, and its Display contains "cannot reach skardi-server".
8. Base URL `http://h:1/` is stored without the trailing slash (in-module test may read the private field).

Run: `cargo test -p skardi-cli client` — expected: FAIL to compile.

- [ ] **Step 2: Implement** per the behavior above.

- [ ] **Step 3: Run the tests** — `cargo test -p skardi-cli client`, expected all PASS.

- [ ] **Step 4: Commit**

```bash
cargo fmt
git add crates/cli
git commit -m "feat(cli): add ApiClient with bearer auth and uniform error mapping"
```

---

### Task 6: `skardi query` + main dispatch and exit codes

**Files:**
- Create: `crates/cli/src/commands/mod.rs`, `crates/cli/src/commands/query.rs`
- Modify: `crates/cli/src/main.rs`

**Interfaces:**
- Consumes: `ApiClient::post`, `output::print_result`, `ClientConfig::resolve`.
- Produces:
  - `commands::query::run(client: &ApiClient, sql: Option<String>, file: Option<PathBuf>, max_rows: Option<usize>, table: bool) -> anyhow::Result<()>`.
  - In `main.rs`: the `Commands` subcommand enum (starting with `Query`) and an async `dispatch(command, &ClientConfig) -> anyhow::Result<()>` that constructs one `ApiClient` and matches on the command — Tasks 7–9 extend both.

**Behavior:**
- Clap surface for `Query`: `-e/--sql <SQL>` (optional), `-f/--file <PATH>` (optional PathBuf), `--max-rows <N>` (optional usize), `--table` (bool flag). Help text notes file takes precedence over `-e` and that the server's default row cap is 1000.
- SQL source: file wins when both are given (read with a contextual error naming the path); neither given → client-side error `no SQL given: pass -e <SQL> or -f <FILE>`.
- Request body: `{"sql": <text>}`, plus `"max_rows"` only when the flag was passed (a private, unit-testable body-builder function enforces this).
- Send to `POST /query`, hand the envelope to `print_result`.
- `main` becomes `#[tokio::main]` returning `std::process::ExitCode`: parse, resolve config from the global flags, dispatch; on error print `error: <chain>` to stderr and exit 2 if the error downcasts to `ApiError::Connect`, else 1.

- [ ] **Step 1: Write the failing tests** (in-module in `query.rs`; create `commands/mod.rs` with `pub mod query;`, add `mod commands;` to `main.rs`)

Test cases:
1. Body-builder: without max_rows → exactly `{"sql": ...}`; with → both keys.
2. wiremock: `run` posts the expected body to `/query` (body_json matcher, `.expect(1)`) and succeeds on a full success envelope.
3. File precedence: with both a tempfile containing different SQL and an inline `-e` value, the posted body carries the file's SQL.
4. Neither `-e` nor `-f` → error containing "no SQL given" (no request made).
5. A 400 envelope from the server propagates as an error whose text contains the `error_type`.

Run: `cargo test -p skardi-cli query` — expected: FAIL to compile.

- [ ] **Step 2: Implement** `commands/query.rs` and rewrite `main.rs` per the behavior above (module declarations for all five `mod`s, Cli struct from Task 1 plus the subcommand field, dispatch, exit-code mapping).

- [ ] **Step 3: Run tests and check exit codes manually**

Run: `cargo test -p skardi-cli`
Expected: all tests from Tasks 2–6 PASS.

Run: `cargo run -p skardi-cli -- query -e "SELECT 1"; echo $?`
Expected (no server running): stderr shows `error: cannot reach skardi-server at http://127.0.0.1:8080 (...)`; prints `2`.

- [ ] **Step 4: Commit**

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
- Consumes: `params::build_body`, `ApiClient::post`, `output::print_result`, `client::ApiError`.
- Produces: `commands::run::run(client: &ApiClient, name: &str, data: Option<&str>, param_flags: &[String], table: bool) -> anyhow::Result<()>`.

**Behavior:**
- Clap surface for `Run`: positional `name` (help: "see `skardi pipeline list`"), `-d/--data <JSON|@FILE|->` (optional), `-p/--param <NAME=VALUE>` (repeatable; help notes JSON-first values and that `-p` overrides `--data` keys), `--table`.
- Build the body with `params::build_body`, POST to `/{name}/execute`, render with `print_result`.
- A 404 from the server is remapped to the friendly error `pipeline '<name>' not found — try 'skardi pipeline list'`; all other `ApiError`s pass through unchanged.

- [ ] **Step 1: Write the failing tests** (in-module; add `pub mod run;` to `commands/mod.rs`)

Test cases (wiremock):
1. `-d '{"user_id":1,"category":"basic"}'` + `-p category=premium` posts exactly `{"user_id":1,"category":"premium"}` to `/daily_report/execute`.
2. No `-d`/`-p` posts exactly `{}`.
3. A 404 envelope yields an error containing both `pipeline 'ghost' not found` and `skardi pipeline list`.

Run: `cargo test -p skardi-cli commands::run` — expected: FAIL to compile.

- [ ] **Step 2: Implement** `commands/run.rs`, add the `Run` variant to `Commands`, and its dispatch arm.

- [ ] **Step 3: Run the tests** — `cargo test -p skardi-cli`, expected all PASS.

- [ ] **Step 4: Commit**

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
- Produces:
  - `commands::pipeline::PipelineCmd` — a clap `Subcommand` enum with `List` and `Show { name: String }` — and `commands::pipeline::run(client: &ApiClient, cmd: PipelineCmd) -> anyhow::Result<()>`.
  - `commands::schema::run(client: &ApiClient) -> anyhow::Result<()>`.
  - `commands::health::run(client: &ApiClient, name: Option<&str>) -> anyhow::Result<()>`.

**Behavior:**
- `pipeline list` → `GET /pipelines`; `pipeline show <name>` → `GET /pipeline/<name>`; `schema` → `GET /data_source`; `health` → `GET /health`; `health <name>` → `GET /health/<name>`.
- All four pretty-print the server's JSON response to stdout, nothing more.
- Clap: `Pipeline` nests `PipelineCmd`; `Schema` takes no args; `Health` takes an optional positional pipeline name.

- [ ] **Step 1: Write the failing tests** (in-module per file; extend `commands/mod.rs` with the three `pub mod`s)

Test cases (wiremock, each with `.expect(1)` path assertions):
1. `PipelineCmd::List` hits `/pipelines`; `PipelineCmd::Show` hits `/pipeline/daily_report`.
2. `schema::run` hits `/data_source`.
3. `health::run(None)` hits `/health` and `health::run(Some("daily_report"))` hits `/health/daily_report`.

Run: `cargo test -p skardi-cli` — expected: FAIL to compile.

- [ ] **Step 2: Implement** the three modules, the three `Commands` variants, and their dispatch arms.

- [ ] **Step 3: Run the tests** — `cargo test -p skardi-cli`, expected all PASS.

- [ ] **Step 4: Commit**

```bash
cargo fmt
git add crates/cli
git commit -m "feat(cli): add pipeline/schema/health discovery commands"
```

---

### Task 9: `skardi job` — port onto ApiClient

Restores the job commands deleted in Task 1, rewired onto the shared client so global `--server`/`--token`/config-file resolution applies. Behavior change (intentional, spec "clean break"): the old `NAME:TYPE=VALUE` typed-param syntax is gone; values are JSON-first like `skardi run`. Output formats are unchanged from the old `jobs_cli.rs` — consult it in git history (`git show HEAD~N:crates/cli/src/jobs_cli.rs` from before Task 1) for the exact subcommand help text, the `submitted: <run_id> (<status>)` line, the columnar `job list` layout, and the minimal job-name urlencoding helper; carry those over as-is.

**Files:**
- Create: `crates/cli/src/commands/jobs.rs`
- Modify: `crates/cli/src/commands/mod.rs`, `crates/cli/src/main.rs`

**Interfaces:**
- Consumes: `ApiClient::{get, post}`, `params::build_body`.
- Produces: `commands::jobs::JobCmd` (clap `Subcommand`: `Run { job, params }`, `Status { run_id }`, `List { job: Option<String>, limit: usize (default 20) }`, `Cancel { run_id }`, `Show`) and `commands::jobs::run(client: &ApiClient, cmd: JobCmd) -> anyhow::Result<()>`.

**Behavior:**
- `job run <job> -p k=v…` → `POST /jobs/<job>/run` with the params object; parse the `{run_id, status}` response and print the `submitted:` line (status defaults to "pending" when absent).
- `job status <run_id>` → `GET /jobs/runs/<run_id>`, pretty-printed.
- `job list [--job N] [--limit N]` → `GET /jobs/runs?limit=<limit>[&job=<urlencoded>]`, rendered with the ported columnar layout (`(no runs)` when empty; non-conforming response falls back to pretty JSON).
- `job cancel <run_id>` → `POST /jobs/runs/<run_id>/cancel` with an empty object, pretty-printed.
- `job show` → `GET /jobs`, pretty-printed.

- [ ] **Step 1: Write the failing tests** (in-module; add `pub mod jobs;` to `commands/mod.rs`)

Test cases (wiremock):
1. `job run` posts typed params (`day=2026-07-23` stays a string, `batch=500` becomes a number) to `/jobs/nightly_sync/run` and succeeds on a `{run_id, status}` response.
2. `job list` with a filter sends both `limit=5` and `job=nightly_sync` query params (use wiremock's `query_param` matcher).
3. `job status` GETs `/jobs/runs/r-1` and `job cancel` POSTs `/jobs/runs/r-1/cancel` (each `.expect(1)`).
4. `job show` GETs `/jobs`.

Run: `cargo test -p skardi-cli jobs` — expected: FAIL to compile.

- [ ] **Step 2: Implement** `commands/jobs.rs`, the `Job` variant nesting `JobCmd` in `Commands`, and its dispatch arm.

- [ ] **Step 3: Run the tests** — `cargo test -p skardi-cli`, expected all PASS.

- [ ] **Step 4: Commit**

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
- Consumes: the built `skardi` binary via the `CARGO_BIN_EXE_skardi` env var Cargo sets for integration tests of binary crates.

- [ ] **Step 1: Add the ignored e2e smoke tests**

Two `#[test]` fns in `crates/cli/tests/e2e_smoke.rs`, both `#[ignore = "requires a running skardi-server"]`, with a file doc comment giving the manual invocation (`SKARDI_SERVER_URL=... cargo test -p skardi-cli --test e2e_smoke -- --ignored`):
1. Spawn the binary with `query -e "SELECT 1 AS one"`; assert exit success (including stderr in the failure message) and that stdout contains `"one"`.
2. Spawn `run definitely_not_a_pipeline -d -` with `{"x": 1}` piped to stdin; assert non-zero exit and stderr containing `not found` — this exercises the stdin `-d -` path unit tests skip, plus the friendly 404 message, without needing any pipeline configured.

Run: `cargo test -p skardi-cli`
Expected: the new tests compile and are reported as ignored; all non-ignored tests PASS.

- [ ] **Step 2: Rewrite `docs/cli.md`**

Full replacement describing the thin-client CLI. Required sections:
1. **Intro** — one paragraph: every command is one HTTP request to skardi-server; no local engine; start a server first (link `docs/server.md`).
2. **Connecting** — the flag/env/file/default precedence table for server URL and token, plus the `~/.skardi/config.yaml` example (same YAML as Task 2's Behavior section) and a note that the token is sent as `Authorization: Bearer`.
3. **Ad-hoc SQL** — `query -e` / `-f` / `--max-rows` / `--table` examples; JSON-to-stdout piping into `jq`; truncation notice goes to stderr.
4. **Pipelines** — "what used to be aliases are now pipeline names"; examples for `pipeline list`, `pipeline show`, and `run` with `-p`, inline `-d`, `-d @file` + `-p` override, and `-d -` stdin; note JSON-first `-p` typing.
5. **Discovery and health** — `schema`, `health`, `health <name>` examples.
6. **Jobs** — the five `job` subcommands with one example each.
7. **Exit codes** — the 0/1/2 table.
8. **Migration table** — old → new for: `query --ctx` (register sources on a server instead), `query --schema` → `schema`, `run <yaml> -p` → server-loaded pipeline + `run <name> -p`, `alias add`/bare-verb → `run <pipeline-name>`, `--features embedding/rag` builds → gone (features live server-side).

- [ ] **Step 3: Update `README.md` CLI sections**

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
- `demo/llm_wiki/README.md` and `demo/rag/README.md` reference removed CLI features/aliases (`cargo install --path crates/cli --features candle|rag`, `skardi alias`, `--ctx`) and need migration to the server-based workflow.
