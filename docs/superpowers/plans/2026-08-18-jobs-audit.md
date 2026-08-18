# Job-Submission Attribution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Record every job submission in the `query_audit` ledger with session attribution and a `run_id` bridge to the jobs ledger, so agent-submitted jobs join session mining.

**Architecture:** `POST /jobs/:name/run` writes a `started` row (statement_kind=`job`, `name@version`, optional session header) before `executor.submit`, fail-closed like #213; the outcome stamp carries `run_id` on success or a fixed error kind on rejection. Run detail stays in `job_runs`. One new nullable ledger column (`run_id`) via a guarded migration. `session_id_from_headers` moves to `query_audit` (third consumer). CLI gains `skardi job run --session-id` with the validation extracted to a shared module.

**Tech Stack:** Rust, Axum, tokio-rusqlite, wiremock. Branch is stacked on `worktree-pipeline-audit` (#213) and uses its `bounded`/`AUDIT_WRITE_TIMEOUT`/`finish_audit` machinery.

**Spec:** `docs/superpowers/specs/2026-08-18-jobs-audit-design.md`

## Global Constraints

- Imports via `use` at the top; never full crate paths inline in function bodies (CLAUDE.md).
- No raw `.unwrap()` outside `crates/cli/` and test code (CLAUDE.md rules for `.expect`/lock poisoning apply).
- Parameter values NEVER reach the `query_audit` ledger, its logs, or traces. (`job_runs.parameters` legitimately holds them — out of scope.)
- Session cap and header rules come from `query_audit::MAX_SESSION_ID_CHARS` and the shared `session_id_from_headers` — nothing re-hardcoded. The CLI restates the cap only inside the new shared `crates/cli/src/session.rs`.
- Precedence on `POST /jobs/:name/run`: jobs-disabled 503 → unknown job 404 → malformed header 400 → audit-write 503 → submit. A test pins 404-before-400.
- Every task ends with `cargo fmt --all` and its named test command green before commit. Commit with `git -c commit.gpgsign=false commit` and trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

---

### Task 1: Store — `run_id` migration, `record_job_submitted`, `record_job_outcome`

**Files:**
- Modify: `crates/server/src/query_audit.rs`

**Interfaces:**
- Consumes: existing `bounded`, `new_id`, `QueryAuditStatus`, `PIPELINE_MAX_ROWS_SENTINEL` (reused for job rows — rename NOT wanted; add a doc line that job rows share it).
- Produces (Task 2 calls these exactly):
  - `pub const JOB_STATEMENT_KIND: &str = "job";` — private if nothing outside the module ends up using it (mirror the `PIPELINE_STATEMENT_KIND` narrowing from #213's final round).
  - `pub async fn record_job_submitted(&self, job_name: &str, version: &str, session_id: Option<&str>) -> Result<String>`
  - `pub async fn record_job_outcome(&self, id: &str, run_id: Option<&str>, status: QueryAuditStatus, error: Option<&str>) -> Result<()>`

- [ ] **Step 1: Write the failing tests** (in the existing tests module; match its style)

```rust
#[tokio::test]
async fn job_row_round_trips_with_run_id() {
    let store = QueryAuditStore::open_in_memory().await.unwrap();
    let id = store
        .record_job_submitted("nightly-backfill", "2.1.0", Some("sess-j"))
        .await
        .unwrap();
    store
        .record_job_outcome(&id, Some("run-abc123"), QueryAuditStatus::Succeeded, None)
        .await
        .unwrap();
    let row = store.get(&id).await.unwrap().unwrap();
    assert_eq!(row["sql"], json!("nightly-backfill@2.1.0"));
    assert_eq!(row["statement_kind"], json!("job"));
    assert_eq!(row["session_id"], json!("sess-j"));
    assert_eq!(row["status"], json!("succeeded"));
    assert_eq!(row["run_id"], json!("run-abc123"));
    assert_eq!(row["max_rows"], json!(0));
    assert!(row["ai_context"].is_null());
    assert!(row["row_count"].is_null());
}

#[tokio::test]
async fn rejected_job_submission_records_fixed_kind_and_null_run_id() {
    let store = QueryAuditStore::open_in_memory().await.unwrap();
    let id = store
        .record_job_submitted("nightly-backfill", "2.1.0", None)
        .await
        .unwrap();
    store
        .record_job_outcome(&id, None, QueryAuditStatus::Failed, Some("schema_mismatch"))
        .await
        .unwrap();
    let row = store.get(&id).await.unwrap().unwrap();
    assert_eq!(row["status"], json!("failed"));
    assert_eq!(row["error"], json!("schema_mismatch"));
    assert!(row["run_id"].is_null());
}

#[tokio::test]
async fn list_by_session_interleaves_all_three_kinds_in_order() {
    let store = QueryAuditStore::open_in_memory().await.unwrap();
    let ctx = serde_json::json!({"purpose": "p", "session_id": "sess-all"});
    store
        .record_started("SELECT 1", Some(&ctx), 10, "Query")
        .await
        .unwrap();
    store
        .record_pipeline_started("weekly-churn", "1.0.0", Some("sess-all"))
        .await
        .unwrap();
    store
        .record_job_submitted("nightly-backfill", "2.1.0", Some("sess-all"))
        .await
        .unwrap();
    let rows = store.list_by_session("sess-all").await.unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["statement_kind"], json!("Query"));
    assert_eq!(rows[1]["statement_kind"], json!("pipeline"));
    assert_eq!(rows[2]["statement_kind"], json!("job"));
    assert_eq!(rows[2]["sql"], json!("nightly-backfill@2.1.0"));
}

#[tokio::test]
async fn orphaned_job_rows_reconcile_to_unknown() {
    let store = QueryAuditStore::open_in_memory().await.unwrap();
    let id = store
        .record_job_submitted("nightly-backfill", "2.1.0", None)
        .await
        .unwrap();
    let n = store.reconcile_orphaned("test restart").await.unwrap();
    assert_eq!(n, 1);
    let row = store.get(&id).await.unwrap().unwrap();
    assert_eq!(row["status"], json!("unknown"));
}

#[tokio::test]
async fn open_migrates_old_schema_without_run_id_column() {
    // A database created before the run_id column existed must open and
    // serve job rows after migration.
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("old.db");
    {
        // Pre-#218-era DDL: the INIT_SCHEMA_SQL minus the run_id column.
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE query_audit (
                id TEXT PRIMARY KEY, created_at TEXT NOT NULL,
                finished_at TEXT, sql TEXT NOT NULL, ai_context TEXT,
                session_id TEXT, max_rows INTEGER NOT NULL,
                statement_kind TEXT NOT NULL, status TEXT NOT NULL,
                row_count INTEGER, error TEXT);",
        )
        .unwrap();
    }
    let store = QueryAuditStore::open(&path).await.unwrap();
    let id = store
        .record_job_submitted("nightly-backfill", "2.1.0", None)
        .await
        .unwrap();
    let row = store.get(&id).await.unwrap().unwrap();
    assert_eq!(row["statement_kind"], json!("job"));
    assert!(row["run_id"].is_null());
}
```

(Direct `rusqlite` use inside the test: import whatever `query_audit.rs` already re-exports — `tokio_rusqlite::rusqlite` is in scope via the existing `use`.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p skardi-server --lib query_audit 2>&1 | tail -5`
Expected: compile error — `record_job_submitted` not found.

- [ ] **Step 3: Implement**

1. Schema: add `run_id TEXT` to `INIT_SCHEMA_SQL`'s CREATE TABLE, and in `open()` (after the schema batch) run the guarded migration:

```rust
// Databases created before the run_id column (pre-#218 dev builds) lack
// it, and CREATE TABLE IF NOT EXISTS will not add columns. Idempotent:
// checked against pragma table_info on every open.
conn.call(|conn| -> SqlResult<()> {
    let has_run_id = conn
        .prepare("SELECT 1 FROM pragma_table_info('query_audit') WHERE name = 'run_id'")?
        .exists([])?;
    if !has_run_id {
        conn.execute("ALTER TABLE query_audit ADD COLUMN run_id TEXT", [])?;
    }
    Ok(())
})
.await
.context("Failed to migrate query-audit schema (run_id column)")?;
```

2. Add `run_id` to the SELECT lists in `get()` and `list_by_session`'s row loader, and to the JSON row construction (nullable → `Value::Null`).
3. `const JOB_STATEMENT_KIND: &str = "job";` next to `PIPELINE_STATEMENT_KIND`; extend the sentinel's doc comment to say job rows share `PIPELINE_MAX_ROWS_SENTINEL` (do not rename it in this stacked PR — churn against #213's diff).
4. `record_job_submitted` — clone of `record_pipeline_started` with `JOB_STATEMENT_KIND` and its own doc:

```rust
/// Insert a `started` row for a job *submission*.
///
/// The row's lifecycle is the submission's, not the run's: `succeeded`
/// means "accepted and enqueued" (stamped with the `run_id` that bridges
/// to the jobs ledger, the authority on the run itself), `failed` means
/// the executor rejected it. Stores `name@version`; parameter values are
/// never recorded here — `job_runs.parameters` is a separate concern.
pub async fn record_job_submitted(
    &self,
    job_name: &str,
    version: &str,
    session_id: Option<&str>,
) -> Result<String> { /* mirror record_pipeline_started; kind = JOB_STATEMENT_KIND */ }
```

5. `record_job_outcome` — UPDATE setting `status`, `finished_at`, `run_id`, `error` by id, through `bounded`:

```rust
pub async fn record_job_outcome(
    &self,
    id: &str,
    run_id: Option<&str>,
    status: QueryAuditStatus,
    error: Option<&str>,
) -> Result<()> {
    let id = id.to_string();
    let finished_at = chrono::Utc::now().to_rfc3339();
    let status = status.as_str();
    let run_id = run_id.map(str::to_string);
    let error = error.map(str::to_string);
    bounded(
        self.conn.call(move |conn| -> SqlResult<()> {
            conn.execute(
                "UPDATE query_audit
                    SET status = ?2, finished_at = ?3, run_id = ?4, error = ?5
                  WHERE id = ?1",
                params![id, status, finished_at, run_id, error],
            )?;
            Ok(())
        }),
        "Failed to update job-audit record",
    )
    .await?;
    Ok(())
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p skardi-server --lib query_audit 2>&1 | tail -5`
Expected: all pass including the 5 new tests.

- [ ] **Step 5: Commit**

```bash
git add crates/server/src/query_audit.rs
git commit -m "feat(server): job-submission rows in the query-audit ledger — run_id bridge, guarded migration"
```

---

### Task 2: Handler — audit `POST /jobs/:name/run`; move `session_id_from_headers` to `query_audit`

**Files:**
- Modify: `crates/server/src/jobs_handlers.rs` (`submit_job_run`, ~line 93)
- Modify: `crates/server/src/query_audit.rs` (receive `session_id_from_headers` + `SESSION_ID_HEADER`)
- Modify: `crates/server/src/pipeline_handlers.rs` (import them from `query_audit`; delete local copies)
- Create: `crates/server/tests/jobs_audit_http.rs`

**Interfaces:**
- Consumes: Task 1's `record_job_submitted` / `record_job_outcome`; `session_id_from_headers(&HeaderMap) -> Result<Option<String>, String>` (unchanged semantics, new home); `JobSubmitError::category()`.
- Produces: the endpoint contract — precedence `jobs_disabled 503 → unknown job 404 → malformed header 400 → audit 503 → submit`; audit rows per spec. Task 3 relies on the header name only.

- [ ] **Step 1: Move the helper.** Cut `SESSION_ID_HEADER` + `session_id_from_headers` (with doc comments intact) from `pipeline_handlers.rs` into `query_audit.rs` as `pub(crate)`; `pipeline_handlers.rs` imports them. `query_audit.rs` needs `use axum::http::HeaderMap;`. Run `cargo test -p skardi-server --test pipeline_audit_http` — all green before proceeding (pure move).

- [ ] **Step 2: Write the failing integration tests.** Create `crates/server/tests/jobs_audit_http.rs`. Crib the app-state harness from `crates/server/tests/jobs_http.rs` (it builds a working jobs executor + destination fixtures) and add the `query_audit: Option<Arc<QueryAuditStore>>` parameter exactly as `pipeline_audit_http.rs`'s `make_app_state` does. Reuse `jobs_http.rs`'s job fixture; bind `TEST_JOB_NAME` and `TEST_JOB_VERSION` consts matching its YAML. Tests:

```rust
#[tokio::test]
async fn submission_is_audited_with_session_and_run_id() {
    // POST with x-skardi-session-id: sess-j and a valid body.
    // Assert 200/202 per existing contract; parse run_id from response.
    // Ledger: one row for sess-j; statement_kind "job";
    // sql == format!("{TEST_JOB_NAME}@{TEST_JOB_VERSION}");
    // status "succeeded"; row["run_id"] == response run_id;
    // ai_context null; row_count null.
}

#[tokio::test]
async fn submission_without_header_audits_null_session() {
    // No header → 1 row recorded, count()==1, list_by_session("") empty.
}

#[tokio::test]
async fn no_store_configured_still_submits() {
    // query_audit: None → submission succeeds exactly as today.
}

#[tokio::test]
async fn unknown_job_with_malformed_header_is_404_and_records_nothing() {
    // POST /jobs/no-such-job/run with x-skardi-session-id: "" →
    // 404 (existing unknown-job error shape), store.count() == 0.
    // Pins 404-before-400 precedence.
}

#[tokio::test]
async fn malformed_header_on_real_job_is_400_and_records_nothing() {
    // Oversize header on TEST_JOB_NAME → 400, error mentions
    // "x-skardi-session-id", store.count() == 0.
}

#[tokio::test]
async fn audit_write_failure_is_503_and_job_is_not_submitted() {
    // close_for_test() → POST valid submission → 503 query_audit_error,
    // AND the jobs ledger has no new run:
    // executor.store().list_runs(None, 10) is empty (the "not submitted"
    // half is directly observable — assert it).
}

#[tokio::test]
async fn parameter_values_never_reach_the_audit_row() {
    // Submit with a param value "PII-CANARY-77"; on success fetch the
    // audit row via list_by_session and assert
    // !row.to_string().contains("PII-CANARY-77").
    // (job_runs.parameters legitimately contains it — do not grep that.)
}
```

Match `jobs_http.rs`'s request-building style; the exact fixture/body comes from that file. Every assertion above is fixed; the scaffolding follows the neighbors.

- [ ] **Step 3: Run to verify failure**

Run: `cargo test -p skardi-server --test jobs_audit_http 2>&1 | tail -8`
Expected: the audit-asserting tests FAIL (nothing recorded yet); `no_store_configured_still_submits` may already pass.

- [ ] **Step 4: Implement in `submit_job_run`.** After the existing jobs-disabled guard:

1. Existence + version pre-check (read lock on `app_state.config`, mirroring the pipeline lookup block): unknown name → the endpoint's existing 404 unknown-job error shape (`error_json`, kind `unknown_job`). Extract `version` from `config.jobs.get(&name)`. Note in a comment: this pre-check exists so 404 wins over header validation (metric-cardinality/status-precedence lesson from #213 round 3) and races benignly with `executor.submit`'s own resolution.
2. `session_id_from_headers(&headers)` → on Err, `400` with the endpoint's error shape and kind `parameter_validation_error`. (Handler signature gains `headers: HeaderMap`.)
3. Record-before-submit, fail-closed:

```rust
let audit_id = match &app_state.query_audit {
    Some(store) => match store
        .record_job_submitted(&name, &version, session_id.as_deref())
        .await
    {
        Ok(id) => Some(id),
        Err(e) => {
            tracing::error!("Job audit write failed; refusing to submit: {e}");
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                error_json(
                    "Query auditing is enabled but the audit record could not be \
                     written; the job was not submitted",
                    "query_audit_error",
                    None,
                ),
            ));
        }
    },
    None => None,
};
```

4. Stamp outcomes on both arms of the existing `match executor.submit(...)`:
   - `Ok(run_id)` → `record_job_outcome(id, Some(&run_id), Succeeded, None)` (log-only on failure, like `finish_audit`; a small local helper or inline `if let Some(id) = &audit_id { if let Err(e) = ... { tracing::error!(...) } }`).
   - `Err(err)` → `record_job_outcome(id, None, Failed, Some(err.category()))` — the category string only; the HTTP response keeps the full message as today.
5. INFO marker: add `session_id = session_id.as_deref().unwrap_or_default()` to whatever INFO line exists (or add one mirroring the pipeline marker) — placed after validation.

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p skardi-server --test jobs_audit_http --test jobs_http --test jobs_e2e --test pipeline_audit_http 2>&1 | tail -8`
Expected: all green (jobs + pipeline regressions included).

- [ ] **Step 6: Commit**

```bash
git add crates/server/src/jobs_handlers.rs crates/server/src/query_audit.rs crates/server/src/pipeline_handlers.rs crates/server/tests/jobs_audit_http.rs
git commit -m "feat(server): audit job submissions — record-before-submit, session header, run_id stamp"
```

---

### Task 3: CLI — `skardi job run --session-id` + shared session validation

**Files:**
- Create: `crates/cli/src/session.rs`
- Modify: `crates/cli/src/lib.rs` or `main.rs` (module registration — match how other modules are declared)
- Modify: `crates/cli/src/commands/run.rs` (use the shared helper; delete the local const + predicate)
- Modify: `crates/cli/src/commands/jobs.rs` (flag + header)
- Modify: `crates/cli/src/main.rs` (clap arg on the job-run variant; find it via the `JobCmd` enum)

**Interfaces:**
- Consumes: `Client::post_with_headers` (exists), header name `x-skardi-session-id`.
- Produces: `pub(crate) fn validate_session_id(sid: &str) -> anyhow::Result<()>` and `pub(crate) const MAX_SESSION_ID_CHARS: usize = 200;` in `session.rs` (move the doc comment about mirroring the server from `run.rs`).

- [ ] **Step 1: Write the failing tests.** In `jobs.rs`'s tests (match its wiremock style — if it has none, crib `run.rs`'s): `job_run_with_session_id_sets_header` (mock matches `header("x-skardi-session-id", "sess-9")`, `.expect(1)`); `job_run_with_invalid_session_id_errors_without_request` (`.expect(0)`, err mentions `--session-id`, not an `ApiError`). In `run.rs`'s tests: existing cases keep passing unchanged (they now route through `session.rs`).

- [ ] **Step 2: Run to verify failure** — `cargo test -p skardi-cli 2>&1 | tail -4`. Expected: compile error (no `session_id` param / module).

- [ ] **Step 3: Implement.** `session.rs` holds the const + `validate_session_id` returning the exact error message currently in `run.rs` (parameterized by flag name if trivial — otherwise keep the `--session-id` text; both commands use the same flag name). `run.rs` calls it. `jobs.rs`'s run arm mirrors `run.rs`: validate, then `post_with_headers` when `Some`, plain `post` when `None`. Wire the clap arg.

- [ ] **Step 4: Verify** — `cargo test -p skardi-cli 2>&1 | tail -4`, all green; `cargo clippy --all-targets -p skardi-cli` no new warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/cli/src/session.rs crates/cli/src/commands/run.rs crates/cli/src/commands/jobs.rs crates/cli/src/main.rs crates/cli/src/lib.rs
git commit -m "feat(cli): skardi job run --session-id; shared session-id validation"
```

---

### Task 4: Docs + QA sweep

**Files:**
- Modify: `docs/server.md`, `docs/jobs.md`, `docs/cli.md`, `docs/superpowers/specs/2026-08-14-pipeline-audit-design.md`

**Interfaces:** prose only.

- [ ] **Step 1: `docs/server.md`.** In the ledger section: `statement_kind` value set gains `job` (update the column-table row); new short subsection after the pipeline one:

```markdown
#### Job submissions in the ledger

`POST /jobs/:name/run` is audited as a *submission event*: the row's
lifecycle is the submission's, not the run's. `statement_kind` is `job`,
`sql` holds `name@version`, and on acceptance the row is stamped
`succeeded` with the `run_id` that bridges to the jobs ledger — which
remains the authority on the run itself (parameters, progress, outcome).
A rejected submission is stamped `failed` with the executor's fixed error
category, never its message text. Record-before-submit and the fail-closed
`503 query_audit_error` behave exactly as for pipelines: a job the ledger
cannot account for is not submitted. The same `X-Skardi-Session-Id` header
(same validation) attributes the submission, so `list_by_session` returns
an agent session's ad-hoc queries, pipeline calls, and job submissions in
one ordered read.
```

  Also update the earlier "run_id" mention in the column table (new row: `run_id` — `job_runs.id` bridge, job rows only).

- [ ] **Step 2: `docs/jobs.md`.** Session-attribution paragraph (mirror `docs/pipelines.md`'s, including the always-validated wording and the retryable `503 query_audit_error` meaning "the job was not submitted, retry later").

- [ ] **Step 3: `docs/cli.md`.** `--session-id` on `skardi job run`, referencing the same client-side validation.

- [ ] **Step 4: Supersede the old non-goal.** In `2026-08-14-pipeline-audit-design.md`'s jobs non-goal bullet, append: "Superseded by `2026-08-18-jobs-audit-design.md`, which adds submission-event attribution while leaving run records in the jobs ledger."

- [ ] **Step 5: QA sweep.**

```bash
cargo fmt --all
cargo test -p skardi-server -p skardi-cli 2>&1 | grep -E "^test result" | sort | uniq -c
cargo clippy --all-targets -p skardi-server -p skardi-cli 2>&1 | grep -E "^\s+-->"   # no findings in files this branch touches (gui.rs et al. pre-existing)
```

- [ ] **Step 6: Commit**

```bash
git add docs/server.md docs/jobs.md docs/cli.md docs/superpowers/specs/2026-08-14-pipeline-audit-design.md
git commit -m "docs: job submissions in the audit ledger — run_id bridge, session attribution, retryable 503"
```
