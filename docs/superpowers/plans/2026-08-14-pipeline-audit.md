# Pipeline-Execution Auditing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Record every pipeline execution in the existing `query_audit` ledger so the self-improving loop keeps seeing promoted tools.

**Architecture:** `POST /:pipeline/execute` writes a `started` row (statement_kind=`pipeline`, pipeline name in the `sql` column, optional session from the `X-Skardi-Session-Id` header) before running, mirrors `/query`'s fail-closed 503 on write failure, and stamps the outcome after. The store gains one insert method; everything downstream (reconcile, retention, `list_by_session`) works unchanged. The CLI gains `skardi run --session-id`.

**Tech Stack:** Rust, Axum, tokio-rusqlite (existing audit store), wiremock (CLI tests).

**Spec:** `docs/superpowers/specs/2026-08-14-pipeline-audit-design.md`

## Global Constraints

- Imports via `use` at the top of the file; never full crate paths inline in function bodies (CLAUDE.md).
- No raw `.unwrap()` outside `crates/cli/` and test code; lock poisoning recovers via `.unwrap_or_else(|p| p.into_inner())`; true invariants use `.expect("why")` (CLAUDE.md).
- Parameter values are NEVER written to the ledger, logs, or traces — only the pipeline name.
- Session header cap reuses `MAX_SESSION_ID_CHARS` (= 200) from `query_handlers.rs` — do not hardcode 200 anywhere else.
- Every task ends with `cargo fmt` and a passing targeted test run before commit.

---

### Task 1: Store method `record_pipeline_started`

**Files:**
- Modify: `crates/server/src/query_audit.rs` (insert method next to `record_started` at line ~180; tests in the existing `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: existing `new_id()`, `QueryAuditStatus`, `self.conn`.
- Produces: `pub async fn record_pipeline_started(&self, pipeline_name: &str, session_id: Option<&str>) -> Result<String>` and `pub const PIPELINE_STATEMENT_KIND: &str = "pipeline";` — Task 2 calls both.

- [ ] **Step 1: Write the failing tests** (in `query_audit.rs`'s tests module, matching its existing test style)

```rust
#[tokio::test]
async fn pipeline_row_round_trips() {
    let store = QueryAuditStore::open_in_memory().await.unwrap();
    let id = store
        .record_pipeline_started("weekly-churn", Some("sess-1"))
        .await
        .unwrap();
    store
        .record_outcome(&id, QueryAuditStatus::Succeeded, Some(42), None)
        .await
        .unwrap();
    let row = store.get(&id).await.unwrap().unwrap();
    assert_eq!(row["sql"], "weekly-churn");
    assert_eq!(row["statement_kind"], "pipeline");
    assert_eq!(row["session_id"], "sess-1");
    assert_eq!(row["max_rows"], 0);
    assert_eq!(row["row_count"], 42);
    assert!(row["ai_context"].is_null());
}

#[tokio::test]
async fn pipeline_row_without_session_has_null_session_id() {
    let store = QueryAuditStore::open_in_memory().await.unwrap();
    let id = store
        .record_pipeline_started("weekly-churn", None)
        .await
        .unwrap();
    let row = store.get(&id).await.unwrap().unwrap();
    assert!(row["session_id"].is_null());
}

#[tokio::test]
async fn list_by_session_interleaves_queries_and_pipelines() {
    let store = QueryAuditStore::open_in_memory().await.unwrap();
    let ctx = serde_json::json!({"purpose": "p", "session_id": "sess-mix"});
    store
        .record_started("SELECT 1", Some(&ctx), 10, "Query")
        .await
        .unwrap();
    store
        .record_pipeline_started("weekly-churn", Some("sess-mix"))
        .await
        .unwrap();
    let rows = store.list_by_session("sess-mix").await.unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn orphaned_pipeline_rows_reconcile_to_unknown() {
    let store = QueryAuditStore::open_in_memory().await.unwrap();
    let id = store
        .record_pipeline_started("weekly-churn", None)
        .await
        .unwrap();
    let n = store.reconcile_orphaned("test restart").await.unwrap();
    assert_eq!(n, 1);
    let row = store.get(&id).await.unwrap().unwrap();
    assert_eq!(row["status"], "unknown");
}
```

Adjust field-access assertions to `get`'s actual JSON shape if it differs (read the existing round-trip test first); the *behaviors* asserted are fixed.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p skardi-server --lib query_audit 2>&1 | tail -5`
Expected: compile error — `record_pipeline_started` not found.

- [ ] **Step 3: Implement** (next to `record_started`; same style)

```rust
/// Statement-kind marker distinguishing pipeline rows from ad-hoc SQL rows.
pub const PIPELINE_STATEMENT_KIND: &str = "pipeline";

/// `max_rows` does not apply to pipeline executions, but the column is NOT
/// NULL; pipeline rows store this sentinel.
const PIPELINE_MAX_ROWS_SENTINEL: i64 = 0;

/// Insert a `started` row for a pipeline execution.
///
/// Stores the pipeline *name* in the `sql` column: the template lives on
/// disk with no secrets, and the name is the join key to it and to the
/// pipeline's `description` (which carries the purpose). Parameter values
/// are deliberately never recorded — they are where PII lives. `ai_context`
/// is left NULL rather than synthesized so the column always means
/// "caller-sent object".
pub async fn record_pipeline_started(
    &self,
    pipeline_name: &str,
    session_id: Option<&str>,
) -> Result<String> {
    let id = new_id();
    let created_at = chrono::Utc::now().to_rfc3339();
    let name = pipeline_name.to_string();
    let session_id = session_id.map(str::to_string);
    let row_id = id.clone();

    self.conn
        .call(move |conn| -> SqlResult<()> {
            conn.execute(
                "INSERT INTO query_audit
                    (id, created_at, sql, ai_context, session_id, max_rows,
                     statement_kind, status)
                 VALUES (?1, ?2, ?3, NULL, ?4, ?5, ?6, ?7)",
                params![
                    row_id,
                    created_at,
                    name,
                    session_id,
                    PIPELINE_MAX_ROWS_SENTINEL,
                    PIPELINE_STATEMENT_KIND,
                    QueryAuditStatus::Started.as_str(),
                ],
            )?;
            Ok(())
        })
        .await
        .context("Failed to write pre-execution pipeline-audit record")?;

    Ok(id)
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p skardi-server --lib query_audit 2>&1 | tail -5`
Expected: all query_audit tests pass, including the 4 new ones.

- [ ] **Step 5: Commit**

```bash
git add crates/server/src/query_audit.rs
git commit -m "feat(server): record_pipeline_started — pipeline rows in the query-audit ledger"
```

---

### Task 2: Handler — audit the execute path (success + session header)

**Files:**
- Modify: `crates/server/src/pipeline_handlers.rs` (`execute_pipeline_by_name`, line ~718)
- Modify: `crates/server/src/query_handlers.rs` (make `MAX_SESSION_ID_CHARS` and `finish_audit` `pub(crate)`)
- Create: `crates/server/tests/pipeline_audit_http.rs`

**Interfaces:**
- Consumes: `record_pipeline_started(&str, Option<&str>) -> Result<String>` and `QueryAuditStatus` from Task 1; `finish_audit(&AppState, Option<&str>, QueryAuditStatus, Option<usize>, Option<&str>)` from `query_handlers.rs`.
- Produces: header contract `x-skardi-session-id` (validated: non-empty, ≤ `MAX_SESSION_ID_CHARS` chars, UTF-8) — Tasks 3 and 4 rely on it; test harness `make_app_state(query_audit: Option<Arc<QueryAuditStore>>) -> (AppState, TempDir)` in `pipeline_audit_http.rs` — Task 3 adds tests to it.

- [ ] **Step 1: Write the failing integration tests**

Create `crates/server/tests/pipeline_audit_http.rs`. Copy the harness from `crates/server/tests/pipelines_http.rs` (the `write_yaml` / `products_batch` / `make_app_state` / `body_to_json` block, lines ~27–133) with one change: `make_app_state` takes `query_audit: Option<Arc<QueryAuditStore>>` and passes it as `AppState::new`'s last argument (see `make_state_with_audit` in `tests/query_http.rs:73` for the exact call shape). Always register the test pipeline. Then:

```rust
#[tokio::test]
async fn successful_execution_is_audited_with_session() {
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    let resp = execute_with_headers(
        &state,
        &[("x-skardi-session-id", "sess-9")],
        json!({"limit": 5}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);

    let rows = store.list_by_session("sess-9").await.unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row["statement_kind"], "pipeline");
    assert_eq!(row["sql"], TEST_PIPELINE_NAME);
    assert_eq!(row["status"], "succeeded");
    assert!(row["row_count"].as_u64().is_some());
    assert!(row["ai_context"].is_null());
}

#[tokio::test]
async fn execution_without_header_audits_null_session() {
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    let resp = execute_with_headers(&state, &[], json!({"limit": 5})).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(store.count().await.unwrap(), 1);
    // Not reachable via list_by_session: session_id is NULL.
    assert!(store.list_by_session("").await.unwrap().is_empty());
}

#[tokio::test]
async fn no_store_configured_executes_without_recording() {
    let (state, _tmp) = make_app_state(None).await;
    let resp = execute_with_headers(&state, &[], json!({"limit": 5})).await;
    assert_eq!(resp.status(), StatusCode::OK);
}
```

Helper for the file (adapt the request-building style already used in `pipelines_http.rs` — router vs direct handler call — whichever that file does):

```rust
async fn execute_with_headers(
    state: &AppState,
    headers: &[(&str, &str)],
    params: Value,
) -> axum::response::Response {
    // Build the request exactly the way pipelines_http.rs does, adding the
    // given headers before dispatch.
}
```

`TEST_PIPELINE_NAME` is whatever name the copied harness registers — bind it as a `const` so assertions and YAML stay in sync.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p skardi-server --test pipeline_audit_http 2>&1 | tail -10`
Expected: `successful_execution_is_audited_with_session` and `execution_without_header_audits_null_session` FAIL (0 rows recorded — handler doesn't audit yet). `no_store_configured_executes_without_recording` may already pass.

- [ ] **Step 3: Implement in `execute_pipeline_by_name`**

In `query_handlers.rs`: change `const MAX_SESSION_ID_CHARS` and `async fn finish_audit` to `pub(crate)`.

In `pipeline_handlers.rs` (imports at top: `crate::query_audit::QueryAuditStatus`, `crate::query_handlers::{finish_audit, MAX_SESSION_ID_CHARS}`):

```rust
/// Optional caller-supplied session header. A header (not a body field)
/// because the execute body IS the flattened parameter map — a reserved key
/// could collide with a legitimate SQL parameter of the same name.
const SESSION_ID_HEADER: &str = "x-skardi-session-id";

/// Extract and validate the session header. `Ok(None)` when absent; `Err`
/// when present but malformed — silently dropping a malformed value would
/// corrupt session stitching, the one job this field has.
fn session_id_from_headers(headers: &HeaderMap) -> Result<Option<String>, String> {
    let Some(value) = headers.get(SESSION_ID_HEADER) else {
        return Ok(None);
    };
    let s = value
        .to_str()
        .map_err(|_| format!("{SESSION_ID_HEADER} must be valid UTF-8"))?;
    if s.is_empty() {
        return Err(format!("{SESSION_ID_HEADER} must not be empty"));
    }
    if s.chars().count() > MAX_SESSION_ID_CHARS {
        return Err(format!(
            "{SESSION_ID_HEADER} must be at most {MAX_SESSION_ID_CHARS} characters"
        ));
    }
    Ok(Some(s.to_string()))
}
```

Wire into `execute_pipeline_by_name`:

1. Right after `require_session(...)`: validate the header —

```rust
let session_id = session_id_from_headers(&headers).map_err(|msg| {
    (
        StatusCode::BAD_REQUEST,
        create_error_response(&msg, "parameter_validation_error", None),
    )
})?;
```

2. After param substitution succeeds, immediately before `app_state.engine.execute(&sql)`: the record-before-execute / fail-closed block, mirroring `query_handlers.rs:247-280` —

```rust
let audit_id = match &app_state.query_audit {
    Some(store) => match store
        .record_pipeline_started(&pipeline_name, session_id.as_deref())
        .await
    {
        Ok(id) => Some(id),
        Err(e) => {
            tracing::error!("Pipeline audit write failed; refusing to execute: {e}");
            let elapsed_ms = start_time.elapsed().as_millis() as f64;
            app_state
                .metrics
                .record_error(&pipeline_name, elapsed_ms, "query_audit_error");
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                create_error_response(
                    "Query auditing is enabled but the audit record could not be \
                     written; the pipeline was not executed",
                    "query_audit_error",
                    None,
                ),
            ));
        }
    },
    None => None,
};
```

3. Stamp outcomes on every path after the audit write:
   - engine `Err` branch: `finish_audit(&app_state, audit_id.as_deref(), QueryAuditStatus::Failed, None, Some(&e.to_string())).await;` before returning the error.
   - `record_batch_to_json` `Err` branch: same, with that error.
   - success path (where `row_count` is computed): `finish_audit(&app_state, audit_id.as_deref(), QueryAuditStatus::Succeeded, Some(row_count), None).await;`

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p skardi-server --test pipeline_audit_http --test pipelines_http --test query_http 2>&1 | tail -6`
Expected: all pass (pipelines_http and query_http prove no regression).

- [ ] **Step 5: Commit**

```bash
git add crates/server/src/pipeline_handlers.rs crates/server/src/query_handlers.rs crates/server/tests/pipeline_audit_http.rs
git commit -m "feat(server): audit pipeline executions — record-before-execute, X-Skardi-Session-Id"
```

---

### Task 3: Handler — failure paths

**Files:**
- Modify: `crates/server/tests/pipeline_audit_http.rs` (tests only; implementation exists after Task 2 — these tests pin it)

**Interfaces:**
- Consumes: Task 2's harness, helper, and header contract; `close_for_test` from `QueryAuditStore` (see `tests/query_http.rs:617` for the 503 pattern).
- Produces: nothing new — behavioral lock only.

- [ ] **Step 1: Write the tests**

```rust
#[tokio::test]
async fn engine_failure_is_audited_as_failed() {
    // Register a pipeline whose SQL references a nonexistent table
    // (add a second write_yaml with FROM no_such_table in the harness).
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    let resp = execute_broken_pipeline(&state, &[("x-skardi-session-id", "sess-f")]).await;
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let rows = store.list_by_session("sess-f").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["status"], "failed");
    assert!(rows[0]["error"].as_str().is_some());
}

#[tokio::test]
async fn param_validation_failure_records_nothing() {
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    let resp = execute_with_headers(&state, &[], json!({})).await; // missing required param
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(store.count().await.unwrap(), 0);
}

#[tokio::test]
async fn malformed_session_header_is_400_and_records_nothing() {
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    for bad in [String::new(), "x".repeat(201)] {
        let resp = execute_with_headers(
            &state,
            &[("x-skardi-session-id", bad.as_str())],
            json!({"limit": 5}),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = body_to_json(resp).await;
        assert_eq!(body["error_type"], json!("parameter_validation_error"));
    }
    assert_eq!(store.count().await.unwrap(), 0);
}

#[tokio::test]
async fn audit_write_failure_is_503_and_pipeline_does_not_run() {
    let store = Arc::new(QueryAuditStore::open_in_memory().await.unwrap());
    let (state, _tmp) = make_app_state(Some(Arc::clone(&store))).await;
    store.close_for_test().await;
    let resp = execute_with_headers(&state, &[], json!({"limit": 5})).await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("query_audit_error"));
}
```

(Empty-string header note: if axum/http rejects constructing an empty header value in the test transport, drop the `String::new()` case and keep the oversize case — the emptiness rule is still enforced server-side for clients that do send one.)

- [ ] **Step 2: Run — all four should pass already** (Task 2 implemented the behavior)

Run: `cargo test -p skardi-server --test pipeline_audit_http 2>&1 | tail -6`
Expected: PASS. If any fails, the implementation has a real gap — fix `pipeline_handlers.rs`, don't weaken the test.

- [ ] **Step 3: Commit**

```bash
git add crates/server/tests/pipeline_audit_http.rs
git commit -m "test(server): pin pipeline-audit failure paths — failed rows, 400s unrecorded, fail-closed 503"
```

---

### Task 4: CLI — `skardi run --session-id`

**Files:**
- Modify: `crates/cli/src/main.rs` (Run variant, ~line 54 area)
- Modify: `crates/cli/src/commands/run.rs`
- Modify: `crates/cli/src/client.rs`

**Interfaces:**
- Consumes: header contract `x-skardi-session-id` from Task 2.
- Produces: `Client::post_with_headers(&self, path: &str, body: &Value, headers: &[(&str, &str)]) -> Result<Value, ApiError>`; `run::run` gains `session_id: Option<String>`.

- [ ] **Step 1: Write the failing test** (in `run.rs`'s tests, matching its existing wiremock style)

```rust
#[tokio::test]
async fn run_with_session_id_sets_header() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/my-pipe/execute"))
        .and(header("x-skardi-session-id", "sess-9"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"success": true, "data": [], "rows": 0})),
        )
        .expect(1)
        .mount(&server)
        .await;
    // Build the client + invoke run(...) the same way the neighboring
    // tests in this file do, passing session_id: Some("sess-9".into()).
}

#[tokio::test]
async fn run_without_session_id_sends_no_header() {
    // Same shape; assert with wiremock's header-absent matching used
    // elsewhere in this crate (or match only on path and inspect the
    // received request's headers via server.received_requests()).
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p skardi-cli 2>&1 | tail -5`
Expected: compile error — `run` has no `session_id` parameter.

- [ ] **Step 3: Implement**

`client.rs` — generalize `post` (keep `post`'s signature by delegating):

```rust
pub async fn post(&self, path: &str, body: &Value) -> Result<Value, ApiError> {
    self.post_with_headers(path, body, &[]).await
}

pub async fn post_with_headers(
    &self,
    path: &str,
    body: &Value,
    headers: &[(&str, &str)],
) -> Result<Value, ApiError> {
    let url = format!("{}{}", self.base_url, path);
    let mut request = self.http.post(&url).json(body);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    request = self.with_auth(request);
    self.send(request, url).await
}
```

`run.rs` — add `session_id: Option<String>` to `run`'s signature; replace the `client.post(...)` call:

```rust
let response = match &session_id {
    Some(sid) => {
        client
            .post_with_headers(&path, &Value::Object(body), &[("x-skardi-session-id", sid)])
            .await
    }
    None => client.post(&path, &Value::Object(body)).await,
};
```

(then keep the existing match-arms on the response as they are — only the call moves).

`main.rs` — on the Run command variant add:

```rust
/// Session id recorded with this execution in the server's audit ledger
/// (sent as the X-Skardi-Session-Id header).
#[arg(long)]
session_id: Option<String>,
```

and pass it through the `commands::run::run(...)` call site.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p skardi-cli 2>&1 | tail -5`
Expected: PASS, including both new tests.

- [ ] **Step 5: Commit**

```bash
git add crates/cli/src/main.rs crates/cli/src/commands/run.rs crates/cli/src/client.rs
git commit -m "feat(cli): skardi run --session-id sends X-Skardi-Session-Id"
```

---

### Task 5: Docs + full QA sweep

**Files:**
- Modify: `docs/server.md` (audit-ledger section, after the `/query` request-fields block ~line 124+)
- Modify: `docs/pipelines.md` (execute-endpoint section)
- Modify: `docs/cli.md` (`skardi run` flags)

**Interfaces:** none — prose only.

- [ ] **Step 1: `docs/server.md`** — add under the audit-ledger material:

```markdown
#### Pipeline executions in the ledger

When `--query-audit-db` is configured, `POST /:pipeline/execute` is audited
with the same record-before-execute and fail-closed semantics as `/query`
(a failed pre-execution write returns 503 and the pipeline does not run).
A pipeline row differs from an ad-hoc row in three ways:

- `statement_kind` is `pipeline`, and the `sql` column holds the pipeline
  *name*, not SQL — the template lives on disk, and the pipeline's
  `description` carries its purpose.
- Parameter values are never recorded: params are where PII lives.
  `ai_context` is always NULL on pipeline rows.
- `max_rows` is stored as `0` (not applicable to pipelines).

`session_id` comes from the optional `X-Skardi-Session-Id` request header
(non-empty, ≤ 200 characters). A malformed header is rejected with `400
parameter_validation_error` rather than silently dropped. With the header
present, one agent session's ad-hoc queries and pipeline calls interleave
under a single `session_id` in the ledger, ordered by `created_at`.
```

- [ ] **Step 2: `docs/pipelines.md`** — in the execute-endpoint docs add:

```markdown
### Session attribution

Send `X-Skardi-Session-Id: <id>` (non-empty, ≤ 200 chars) with an execute
request to group this run with the rest of an agent session in the query
audit ledger (`--query-audit-db`). The header is optional and ignored when
auditing is off. It is a header rather than a body field because the
request body is the parameter map itself — a reserved key could collide
with a SQL parameter of the same name.
```

- [ ] **Step 3: `docs/cli.md`** — in the `skardi run` section add:

```markdown
- `--session-id <ID>` — sent as `X-Skardi-Session-Id`; groups this
  execution with an agent session in the server's query audit ledger.
```

- [ ] **Step 4: Full QA sweep** (rust-expert checklist)

```bash
cargo fmt --all
cargo clippy --all-targets -p skardi-server -p skardi-cli 2>&1 | tail -5   # expect no warnings
cargo test -p skardi-server -p skardi-cli 2>&1 | grep -E "^test result" | sort | uniq -c
```

Expected: fmt clean, zero clippy warnings, all suites pass.

- [ ] **Step 5: Commit**

```bash
git add docs/server.md docs/pipelines.md docs/cli.md
git commit -m "docs: pipeline executions in the audit ledger, X-Skardi-Session-Id, skardi run --session-id"
```
