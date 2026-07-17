# POST /query Ad-hoc SQL Endpoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `POST /query` endpoint to skardi-server that executes one ad-hoc SQL statement against the `ctx.yaml`-registered data sources, with DDL/COPY always rejected, DML gated per-source by `access_mode`, and a default 1000-row cap.

**Architecture:** A new `query_handlers.rs` Axum handler reuses the existing `validate_sql` machinery (hardened to also block COPY and multi-statement input), a new `DataFusionEngine::execute_with_limit` that pushes the row cap into the query plan, and response helpers extracted from `pipeline_handlers.rs` into a shared `response.rs`. Spec: `docs/superpowers/specs/2026-07-17-query-endpoint-design.md`.

**Tech Stack:** Rust (edition 2024), Axum, DataFusion 52, sqlparser 0.53 (in `crates/skardi`), arrow-json.

## Global Constraints

- No raw `.unwrap()` in production code (allowed in `crates/cli/`, `#[cfg(test)]` modules, `#[test]` fns, doc examples). Lock poisoning recovers via `.unwrap_or_else(|p| p.into_inner())`. True invariants use `.expect("why this cannot fail")`.
- Imports via `use` at the top of the file — never full crate paths inline in function bodies.
- `crates/skardi` uses sqlparser **0.53** — do NOT add sqlparser to `crates/server` (the workspace root pins 0.55; mixing versions breaks type identity). The validator exposes a sqlparser-free `StatementKind` enum instead.
- Package names: `skardi` (crates/skardi), `skardi-server` (crates/server).
- Run all commands from the repo root `/Users/weixin/workspace/skardi`.
- Commit messages end with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

---

### Task 1: Validator hardening — block COPY, add single-statement validation

**Files:**
- Modify: `crates/skardi/src/sources/sql_validator.rs`

**Interfaces:**
- Consumes: existing `SqlValidatorConfig`, `validate_statement`, `preprocess_parameters` (all already in this file).
- Produces (Task 4 relies on these exact names):
  - `pub enum StatementKind { Query, Other }` (derives `Debug, Clone, Copy, PartialEq, Eq`)
  - `pub fn validate_single_sql(sql: &str, config: &SqlValidatorConfig) -> Result<StatementKind, SqlValidationError>`
  - New `SqlValidationError` variants: `CopyNotAllowed` and `NotExactlyOneStatement { count: usize }`
  - Existing `validate_sql` now also rejects COPY (pipelines get this hardening for free).

- [ ] **Step 1: Write the failing tests**

Append inside the existing `mod tests` in `crates/skardi/src/sources/sql_validator.rs`:

```rust
    #[test]
    fn test_copy_blocked() {
        let config = test_config();
        let result = validate_sql("COPY users TO 'out.csv'", &config);
        assert!(
            matches!(result, Err(SqlValidationError::CopyNotAllowed)),
            "COPY must be rejected, got: {:?}",
            result
        );
    }

    #[test]
    fn test_validate_single_sql_query_ok() {
        let config = test_config();
        let kind = validate_single_sql("SELECT * FROM users", &config).unwrap();
        assert_eq!(kind, StatementKind::Query);
    }

    #[test]
    fn test_validate_single_sql_write_is_other() {
        let config = test_config();
        let kind = validate_single_sql("INSERT INTO orders (id) VALUES (1)", &config).unwrap();
        assert_eq!(kind, StatementKind::Other);
    }

    #[test]
    fn test_validate_single_sql_multi_statement_rejected() {
        let config = test_config();
        let result = validate_single_sql("SELECT 1; SELECT 2", &config);
        assert!(matches!(
            result,
            Err(SqlValidationError::NotExactlyOneStatement { count: 2 })
        ));
    }

    #[test]
    fn test_validate_single_sql_empty_rejected() {
        let config = test_config();
        let result = validate_single_sql("", &config);
        assert!(matches!(
            result,
            Err(SqlValidationError::NotExactlyOneStatement { count: 0 })
        ));
    }

    #[test]
    fn test_validate_single_sql_enforces_existing_rules() {
        let config = test_config();
        assert!(matches!(
            validate_single_sql("DROP TABLE users", &config),
            Err(SqlValidationError::DdlNotAllowed { .. })
        ));
        assert!(matches!(
            validate_single_sql("DELETE FROM users WHERE id = 1", &config),
            Err(SqlValidationError::WriteNotAllowed { .. })
        ));
        assert!(matches!(
            validate_single_sql("COPY users TO 'out.csv'", &config),
            Err(SqlValidationError::CopyNotAllowed)
        ));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p skardi sql_validator`
Expected: compile error — `CopyNotAllowed`, `NotExactlyOneStatement`, `StatementKind`, and `validate_single_sql` are not defined.

- [ ] **Step 3: Implement**

In `crates/skardi/src/sources/sql_validator.rs`:

3a. Add two variants to `SqlValidationError` (after `WriteNotAllowed`):

```rust
    #[error(
        "COPY operation not allowed. COPY can read or write files on the server and is not permitted on any data source."
    )]
    CopyNotAllowed,

    #[error("Expected exactly one SQL statement, found {count}.")]
    NotExactlyOneStatement { count: usize },
```

3b. Add COPY arms to `validate_statement`, immediately after the `Statement::Truncate { .. }` arm and before the DML arms:

```rust
        // File-transfer operations - always blocked (can touch the server's filesystem)
        Statement::Copy { .. } => Err(SqlValidationError::CopyNotAllowed),
        Statement::CopyIntoSnowflake { .. } => Err(SqlValidationError::CopyNotAllowed),
```

3c. Add the `StatementKind` enum and `validate_single_sql` after the existing `validate_sql` function:

```rust
/// Shape of a statement validated by [`validate_single_sql`], so callers can
/// pick an execution path without depending on sqlparser types (crates
/// outside this one may link a different sqlparser version).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementKind {
    /// A query (SELECT/...) — safe to wrap in a plan-level LIMIT.
    Query,
    /// Anything else that passed validation (DML writes, SHOW, EXPLAIN, ...).
    Other,
}

/// Validate SQL that must consist of exactly one statement.
///
/// Applies the same rules as [`validate_sql`] (DDL and COPY always rejected,
/// writes checked against per-table access modes) and additionally rejects
/// input that parses to zero or more than one statement. Returns the
/// statement's [`StatementKind`] on success.
pub fn validate_single_sql(
    sql: &str,
    config: &SqlValidatorConfig,
) -> Result<StatementKind, SqlValidationError> {
    let preprocessed_sql = preprocess_parameters(sql);

    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, &preprocessed_sql)
        .map_err(|e| SqlValidationError::ParseError(e.to_string()))?;

    if statements.len() != 1 {
        return Err(SqlValidationError::NotExactlyOneStatement {
            count: statements.len(),
        });
    }

    let statement = &statements[0];
    validate_statement(statement, config)?;

    Ok(if matches!(statement, Statement::Query(_)) {
        StatementKind::Query
    } else {
        StatementKind::Other
    })
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p skardi sql_validator`
Expected: PASS — all pre-existing validator tests plus the 6 new ones.

- [ ] **Step 5: Commit**

```bash
git add crates/skardi/src/sources/sql_validator.rs
git commit -m "feat(validator): block COPY, add single-statement validation with StatementKind

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: `DataFusionEngine::execute_with_limit`

**Files:**
- Modify: `crates/skardi/src/engine/datafusion.rs`

**Interfaces:**
- Consumes: existing `DataFusionEngine`, `Engine::execute`.
- Produces (Task 4 relies on this exact signature): inherent method
  `pub async fn execute_with_limit(&self, sql: &str, fetch: usize) -> Result<RecordBatch>`
  — applies `LIMIT fetch` on top of the query plan, so at most `fetch` rows are materialized.

- [ ] **Step 1: Write the failing tests**

`crates/skardi/src/engine/datafusion.rs` currently has no test module. Append at the end of the file:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn engine_with_numbers() -> DataFusionEngine {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3, 4, 5]))],
        )
        .unwrap();
        ctx.register_batch("numbers", batch).unwrap();
        DataFusionEngine::new(ctx)
    }

    #[tokio::test]
    async fn execute_with_limit_truncates_to_fetch() {
        let engine = engine_with_numbers();
        let batch = engine
            .execute_with_limit("SELECT n FROM numbers ORDER BY n", 3)
            .await
            .unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[tokio::test]
    async fn execute_with_limit_returns_all_rows_when_under_limit() {
        let engine = engine_with_numbers();
        let batch = engine
            .execute_with_limit("SELECT n FROM numbers", 100)
            .await
            .unwrap();
        assert_eq!(batch.num_rows(), 5);
    }

    #[tokio::test]
    async fn execute_with_limit_empty_result_keeps_schema() {
        let engine = engine_with_numbers();
        let batch = engine
            .execute_with_limit("SELECT n FROM numbers WHERE n > 100", 10)
            .await
            .unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema().fields().len(), 1);
        assert_eq!(batch.schema().field(0).name(), "n");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p skardi engine::datafusion`
Expected: compile error — `execute_with_limit` is not defined.

- [ ] **Step 3: Implement (extract shared collect logic, add the method)**

3a. Move the inline `use arrow::compute::concat_batches;` (currently inside `execute`'s multi-batch arm) to the top of the file, and add a `SchemaRef` import. Top-of-file imports become:

```rust
use super::Engine;
use anyhow::Result;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::prelude::*;
use std::sync::Arc;
```

3b. Add a private helper and the new method inside `impl DataFusionEngine` (after `session_context_arc`):

```rust
    /// Execute a SQL query with a row-count cap pushed into the query plan.
    ///
    /// Applies `LIMIT fetch` on top of the query's logical plan before
    /// collecting, so at most `fetch` rows are materialized. Only meaningful
    /// for query statements (SELECT/...); DML plans should go through
    /// [`Engine::execute`] instead.
    pub async fn execute_with_limit(&self, sql: &str, fetch: usize) -> Result<RecordBatch> {
        let dataframe = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute SQL query: {}", e))?
            .limit(0, Some(fetch))
            .map_err(|e| anyhow::anyhow!("Failed to apply row limit: {}", e))?;

        let schema = dataframe.schema().inner().clone();
        let batches = dataframe
            .collect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to collect query results: {}", e))?;

        batches_to_single(schema, batches)
    }
```

3c. Add the free helper function after the `impl Engine for DataFusionEngine` block:

```rust
/// Concatenate collected batches into a single RecordBatch, producing an
/// empty batch with the query's schema when there are no results.
fn batches_to_single(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<RecordBatch> {
    match batches.len() {
        0 => Ok(RecordBatch::new_empty(schema)),
        1 => Ok(batches
            .into_iter()
            .next()
            .expect("len == 1 guarantees first element")),
        _ => {
            let batch_schema = batches[0].schema();
            concat_batches(&batch_schema, &batches)
                .map_err(|e| anyhow::anyhow!("Failed to concatenate result batches: {}", e))
        }
    }
}
```

3d. Replace the `match batches.len() { ... }` block at the end of `Engine::execute` (the `0 =>`, `1 =>`, `_ =>` arms and their comments) with:

```rust
        batches_to_single(schema, batches)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p skardi engine::datafusion`
Expected: PASS — 3 new tests.

Also run: `cargo test -p skardi`
Expected: PASS — no regressions from the `execute` refactor.

- [ ] **Step 5: Commit**

```bash
git add crates/skardi/src/engine/datafusion.rs
git commit -m "feat(engine): add execute_with_limit pushing row cap into the plan

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Extract shared response helpers into `response.rs`

Pure refactor — no behavior change; existing tests are the safety net. The one signature change: `create_success_response` gains a `truncated: Option<bool>` parameter (`None` omits the field, preserving pipeline responses byte-for-byte).

**Files:**
- Create: `crates/server/src/response.rs`
- Modify: `crates/server/src/pipeline_handlers.rs` (remove moved items at lines ~51-64, ~109-133, ~830-854; add import; update one call site)
- Modify: `crates/server/src/lib.rs` (add module)

**Interfaces:**
- Consumes: nothing new.
- Produces (Task 4 relies on these exact names, importable as `crate::response::{...}`):
  - `pub struct ErrorResponse { pub success: bool, pub error: String, pub error_type: String, pub details: Option<Value>, pub timestamp: String }`
  - `pub(crate) fn create_error_response(error_msg: &str, error_type: &str, details: Option<Value>) -> Json<ErrorResponse>`
  - `pub(crate) fn create_success_response(data: Vec<Value>, rows: usize, execution_time_ms: u64, truncated: Option<bool>) -> Json<Value>`
  - `pub(crate) fn record_batch_to_json(batch: &RecordBatch) -> Result<Vec<Value>, Box<dyn std::error::Error>>`

- [ ] **Step 1: Create `crates/server/src/response.rs`**

```rust
//! Shared HTTP response helpers used by the pipeline and query handlers:
//! success/error envelopes and Arrow → JSON conversion.

use arrow::record_batch::RecordBatch;
use arrow_json::{WriterBuilder, writer::JsonArray};
use axum::Json;
use serde::Serialize;
use serde_json::{Map, Value};

/// Error response structure for API endpoints
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    /// Whether the operation was successful
    pub success: bool,
    /// Error message
    pub error: String,
    /// Error category/type
    pub error_type: String,
    /// Additional error details
    pub details: Option<Value>,
    /// Timestamp when error occurred
    pub timestamp: String,
}

/// Helper function to create error responses
pub(crate) fn create_error_response(
    error_msg: &str,
    error_type: &str,
    details: Option<Value>,
) -> Json<ErrorResponse> {
    Json(ErrorResponse {
        success: false,
        error: error_msg.to_string(),
        error_type: error_type.to_string(),
        details,
        timestamp: chrono::Utc::now().to_rfc3339(),
    })
}

/// Helper function to create success response with data.
///
/// `truncated: None` omits the field (pipeline responses are unchanged);
/// `Some(_)` includes it (the ad-hoc query endpoint reports row-cap hits).
pub(crate) fn create_success_response(
    data: Vec<Value>,
    rows: usize,
    execution_time_ms: u64,
    truncated: Option<bool>,
) -> Json<Value> {
    let mut body = serde_json::json!({
        "success": true,
        "data": data,
        "rows": rows,
        "execution_time_ms": execution_time_ms,
        "timestamp": chrono::Utc::now().to_rfc3339()
    });
    if let Some(truncated) = truncated {
        body["truncated"] = Value::Bool(truncated);
    }
    Json(body)
}

/// Convert Arrow RecordBatch to JSON array using arrow_json
pub(crate) fn record_batch_to_json(
    batch: &RecordBatch,
) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    // Write the record batch to JSON using arrow_json with null value inclusion
    let buf = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true) // Include null values in JSON output
        .build::<_, JsonArray>(buf);
    writer.write_batches(&vec![batch])?;
    writer.finish()?;
    let json_data = writer.into_inner();

    // Parse the JSON array string into serde_json::Value objects
    let json_rows: Vec<Map<String, Value>> = serde_json::from_reader(json_data.as_slice())?;

    // Convert Map objects to Value objects
    let values: Vec<Value> = json_rows
        .into_iter()
        .map(|map| Value::Object(map))
        .collect();

    Ok(values)
}
```

- [ ] **Step 2: Update `pipeline_handlers.rs`**

2a. Delete from `crates/server/src/pipeline_handlers.rs`:
- the `ErrorResponse` struct (lines ~51-64),
- `create_error_response` and `create_success_response` (lines ~109-133),
- `record_batch_to_json` (lines ~830-854).

2b. Add to the `use` block at the top:

```rust
use crate::response::{
    ErrorResponse, create_error_response, create_success_response, record_batch_to_json,
};
```

2c. Update the single `create_success_response` call site (end of `execute_pipeline_by_name`, was line ~827):

```rust
    Ok(create_success_response(data, row_count, execution_time, None))
```

(The `#[cfg(test)]` tests `test_record_batch_to_json` and `test_record_batch_to_json_with_nulls` keep working — they resolve `record_batch_to_json` through the new import via `use super::*;`.)

- [ ] **Step 3: Register the module in `crates/server/src/lib.rs`**

Add to the module list (alphabetical, after `pub mod remote_storage;`):

```rust
pub mod response;
```

- [ ] **Step 4: Run tests to verify no regressions**

Run: `cargo test -p skardi-server`
Expected: PASS — all existing unit and integration tests unchanged.

- [ ] **Step 5: Commit**

```bash
git add crates/server/src/response.rs crates/server/src/pipeline_handlers.rs crates/server/src/lib.rs
git commit -m "refactor(server): extract response helpers into response.rs

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: `POST /query` handler, route, and HTTP integration tests

**Files:**
- Create: `crates/server/src/query_handlers.rs`
- Create: `crates/server/tests/query_http.rs`
- Modify: `crates/server/src/config.rs` (extract `validator_config_from_sources` from `validate_pipeline_sql`, lines ~877-906)
- Modify: `crates/server/src/server.rs` (mount route)
- Modify: `crates/server/src/lib.rs` (add module)

**Interfaces:**
- Consumes:
  - `validate_single_sql`, `StatementKind`, `SqlValidationError`, `SqlValidatorConfig` from Task 1 (`skardi::sources::sql_validator`)
  - `DataFusionEngine::execute_with_limit(sql, fetch)` from Task 2
  - `crate::response::{ErrorResponse, create_error_response, create_success_response, record_batch_to_json}` from Task 3
  - `crate::auth::routes::verify_session(&AppState, &HeaderMap)` (existing)
- Produces:
  - `pub async fn execute_query(State<AppState>, HeaderMap, Json<QueryRequest>) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)>` mounted at `POST /query`
  - `pub(crate) fn validator_config_from_sources(&[DataSource]) -> SqlValidatorConfig` in `config.rs`

- [ ] **Step 1: Write the failing HTTP integration tests**

Create `crates/server/tests/query_http.rs`. It only uses existing public API, so it compiles before the handler exists and every test fails with 404.

```rust
//! HTTP integration tests for the ad-hoc SQL endpoint (`POST /query`) —
//! boots the same `configure_routes` axum router the binary does and
//! exercises the request/response surface. Mirrors `pipelines_http.rs`.

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use datafusion::prelude::SessionContext;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use skardi::engine::datafusion::DataFusionEngine;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tower::ServiceExt;

use skardi_server::auth::layer::AuthLayer;
use skardi_server::auth::mode::AuthMode;
use skardi_server::config::{AccessMode, CliArgs, DataSource, DataSourceType, ServerConfig};
use skardi_server::metrics::PipelineMetrics;
use skardi_server::semantics::SemanticsRegistry;
use skardi_server::server::{AppState, configure_routes};

/// Five-row `products` MemTable: id, brand.
fn products_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("brand", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Apple", "Sony", "Apple", "Samsung", "Apple",
            ])),
        ],
    )
    .unwrap()
}

/// One-row `scratch` MemTable used as the read-write INSERT target.
fn scratch_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64]))]).unwrap()
}

fn data_source(name: &str, access_mode: AccessMode) -> DataSource {
    DataSource {
        name: name.to_string(),
        source_type: DataSourceType::Csv,
        path: Default::default(),
        connection_string: None,
        schema: None,
        options: None,
        hierarchy_level: Default::default(),
        access_mode,
        enable_cache: false,
        description: None,
    }
}

/// AppState with `products` (read_only) and `scratch` (read_write) MemTables.
fn make_state() -> AppState {
    let ctx = Arc::new(SessionContext::new());
    ctx.register_batch("products", products_batch()).unwrap();
    ctx.register_batch("scratch", scratch_batch()).unwrap();
    let engine = Arc::new(DataFusionEngine::new_with_arc(Arc::clone(&ctx)));
    let config = ServerConfig {
        pipelines: HashMap::new(),
        jobs: HashMap::new(),
        data_sources: vec![
            data_source("products", AccessMode::ReadOnly),
            data_source("scratch", AccessMode::ReadWrite),
        ],
        semantics: SemanticsRegistry::default(),
        args: CliArgs {
            pipeline_path: None,
            jobs_path: None,
            jobs_db_path: None,
            ctx_file: None,
            semantics_path: None,
            port: 0,
        },
    };
    AppState {
        config: Arc::new(RwLock::new(config)),
        engine,
        session_ctx: ctx,
        metrics: PipelineMetrics::new(),
        auth_layer: AuthLayer::None,
        jobs: None,
    }
}

async fn post_query(state: AppState, body: Value) -> axum::response::Response {
    let app = configure_routes(state);
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/query")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
    )
    .await
    .unwrap()
}

async fn body_to_json(resp: axum::response::Response) -> Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

// ---------------------------------------------------------------------------
// Happy path

#[tokio::test]
async fn select_returns_rows_with_envelope() {
    let resp = post_query(
        make_state(),
        json!({"sql": "SELECT id, brand FROM products ORDER BY id"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    assert_eq!(body["success"], json!(true));
    assert_eq!(body["rows"], json!(5));
    assert_eq!(body["truncated"], json!(false));
    assert_eq!(body["data"].as_array().unwrap().len(), 5);
    assert_eq!(body["data"][0]["brand"], json!("Apple"));
    assert!(body["execution_time_ms"].is_u64());
}

#[tokio::test]
async fn max_rows_truncates_and_flags() {
    let resp = post_query(
        make_state(),
        json!({"sql": "SELECT id FROM products ORDER BY id", "max_rows": 2}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    assert_eq!(body["rows"], json!(2));
    assert_eq!(body["truncated"], json!(true));
    assert_eq!(body["data"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn result_exactly_at_cap_is_not_truncated() {
    let resp = post_query(
        make_state(),
        json!({"sql": "SELECT id FROM products", "max_rows": 5}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    assert_eq!(body["rows"], json!(5));
    assert_eq!(body["truncated"], json!(false));
}

// ---------------------------------------------------------------------------
// Validation errors → 400

#[tokio::test]
async fn max_rows_zero_rejected() {
    let resp = post_query(make_state(), json!({"sql": "SELECT 1", "max_rows": 0})).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("parameter_validation_error"));
}

#[tokio::test]
async fn ddl_rejected() {
    let resp = post_query(make_state(), json!({"sql": "DROP TABLE products"})).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("sql_validation_error"));
    assert_eq!(body["details"]["operation"], json!("DROP"));
}

#[tokio::test]
async fn copy_rejected() {
    let resp = post_query(
        make_state(),
        json!({"sql": "COPY products TO 'out.csv'"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("sql_validation_error"));
}

#[tokio::test]
async fn multi_statement_rejected() {
    let resp = post_query(make_state(), json!({"sql": "SELECT 1; SELECT 2"})).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("sql_validation_error"));
    assert_eq!(body["details"]["statement_count"], json!(2));
}

#[tokio::test]
async fn unparseable_sql_rejected() {
    let resp = post_query(make_state(), json!({"sql": "SELEKT * FROM products"})).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("sql_validation_error"));
}

// ---------------------------------------------------------------------------
// Access modes

#[tokio::test]
async fn insert_into_read_only_source_rejected() {
    let resp = post_query(
        make_state(),
        json!({"sql": "INSERT INTO products (id, brand) VALUES (6, 'LG')"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("sql_validation_error"));
    assert_eq!(body["details"]["operation"], json!("INSERT"));
    assert_eq!(body["details"]["table"], json!("products"));
}

#[tokio::test]
async fn insert_into_read_write_source_allowed() {
    let state = make_state();
    let resp = post_query(
        state.clone(),
        json!({"sql": "INSERT INTO scratch (id) VALUES (99)"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    assert_eq!(body["success"], json!(true));
    assert_eq!(body["truncated"], json!(false));

    // The write is visible to a follow-up query on the same state.
    let resp = post_query(
        state,
        json!({"sql": "SELECT count(*) AS c FROM scratch WHERE id = 99"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp).await;
    assert_eq!(body["data"][0]["c"], json!(1));
}

// ---------------------------------------------------------------------------
// Execution errors → 500

#[tokio::test]
async fn unknown_table_is_execution_error() {
    let resp = post_query(make_state(), json!({"sql": "SELECT * FROM no_such_table"})).await;
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = body_to_json(resp).await;
    assert_eq!(body["error_type"], json!("query_execution_error"));
}

// ---------------------------------------------------------------------------
// Auth

#[tokio::test]
async fn missing_session_returns_401_when_auth_enabled() {
    unsafe {
        std::env::set_var("AUTH_SECRET", "test-secret-that-is-at-least-32-characters!");
        std::env::set_var("AUTH_DB_PATH", ":memory:");
        std::env::remove_var("AUTH_BASE_URL");
    }
    let layer = AuthLayer::build(&AuthMode::BetterAuthDieselSqlite)
        .await
        .unwrap();
    let mut state = make_state();
    state.auth_layer = layer;
    let resp = post_query(state, json!({"sql": "SELECT 1"})).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p skardi-server --test query_http`
Expected: FAIL — every test gets `404 Not Found` (route not mounted). If compilation fails on an import, fix the import, not the production code.

- [ ] **Step 3: Extract `validator_config_from_sources` in `config.rs`**

3a. Add to the top-of-file `use` block of `crates/server/src/config.rs`:

```rust
use skardi::sources::sql_validator::{SqlValidatorConfig, validate_sql};
```

3b. Add the new function directly above `validate_pipeline_sql` (~line 877):

```rust
/// Build a SQL validator config mapping every data source name to its
/// configured access mode. Used at config load (pipeline SQL) and at
/// request time (`POST /query`).
pub(crate) fn validator_config_from_sources(data_sources: &[DataSource]) -> SqlValidatorConfig {
    let mut validator_config = SqlValidatorConfig::new();
    for ds in data_sources {
        validator_config = validator_config.with_table(&ds.name, ds.access_mode);
    }
    validator_config
}
```

(`ds.access_mode` is `skardi::sources::AccessMode`, the same type `sql_validator` re-exports — no conversion needed.)

3c. Replace the body of `validate_pipeline_sql` (drop its inline `use` line and the manual config-building loop) with:

```rust
fn validate_pipeline_sql(
    pipeline_name: &str,
    sql: &str,
    data_sources: &[DataSource],
) -> Result<()> {
    let validator_config = validator_config_from_sources(data_sources);

    // Validate the SQL against access mode restrictions
    validate_sql(sql, &validator_config).map_err(|e| {
        anyhow::anyhow!("Pipeline '{}' SQL validation failed: {}", pipeline_name, e)
    })?;

    tracing::info!(
        "✅ Pipeline '{}' SQL validated against access modes",
        pipeline_name
    );
    Ok(())
}
```

- [ ] **Step 4: Create `crates/server/src/query_handlers.rs`**

```rust
//! Axum handler for the ad-hoc SQL endpoint.
//!
//! Endpoint mounted here:
//!
//! * `POST /query` — execute one SQL statement against the data sources
//!   registered from the ctx file. DDL and COPY are always rejected; DML is
//!   allowed only against sources configured with `access_mode: read_write`.
//!   Query results are capped at `max_rows` (default 1000) and the response
//!   carries a `truncated` flag.

use axum::{Json, extract::State, http::StatusCode};
use serde::Deserialize;
use serde_json::Value;
use skardi::engine::Engine;
use skardi::sources::sql_validator::{SqlValidationError, StatementKind, validate_single_sql};
use std::time::Instant;

use crate::config::validator_config_from_sources;
use crate::response::{
    ErrorResponse, create_error_response, create_success_response, record_batch_to_json,
};
use crate::server::AppState;

/// Default row cap applied when the request does not specify `max_rows`.
const DEFAULT_MAX_ROWS: usize = 1000;

/// Metrics label for ad-hoc queries (pipelines record under their own name).
const QUERY_METRICS_LABEL: &str = "query";

/// Request structure for ad-hoc query execution
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    /// A single SQL statement to execute
    pub sql: String,
    /// Result row cap; defaults to [`DEFAULT_MAX_ROWS`]. Must be >= 1.
    pub max_rows: Option<usize>,
}

/// Execute ad-hoc SQL endpoint - POST /query
pub async fn execute_query(
    State(app_state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(request): Json<QueryRequest>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    if let Err(unauth_response) = crate::auth::routes::verify_session(&app_state, &headers).await {
        let status = unauth_response.status();
        let body_bytes = axum::body::to_bytes(unauth_response.into_body(), 512)
            .await
            .unwrap_or_default();
        let msg = serde_json::from_slice::<serde_json::Value>(&body_bytes)
            .ok()
            .and_then(|v| v["error"].as_str().map(|s| s.to_string()))
            .unwrap_or_else(|| "Authentication required".to_string());
        return Err((status, create_error_response(&msg, "unauthorized", None)));
    }

    let start_time = Instant::now();

    let max_rows = match request.max_rows {
        Some(0) => {
            return Err((
                StatusCode::BAD_REQUEST,
                create_error_response(
                    "max_rows must be a positive integer",
                    "parameter_validation_error",
                    None,
                ),
            ));
        }
        Some(n) => n,
        None => DEFAULT_MAX_ROWS,
    };

    // Build the validator config from the current data sources on every
    // request so runtime config updates are respected.
    let validator_config = {
        let config = app_state
            .config
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        validator_config_from_sources(&config.data_sources)
    };

    let statement_kind = match validate_single_sql(&request.sql, &validator_config) {
        Ok(kind) => kind,
        Err(e) => {
            tracing::info!("Rejected ad-hoc query: {}", e);
            tracing::debug!("Rejected SQL: {}", request.sql);

            let elapsed_ms = start_time.elapsed().as_millis() as f64;
            app_state
                .metrics
                .record_error(QUERY_METRICS_LABEL, elapsed_ms, "sql_validation_error");

            let details = match &e {
                SqlValidationError::DdlNotAllowed { operation } => {
                    Some(serde_json::json!({ "operation": operation }))
                }
                SqlValidationError::WriteNotAllowed { operation, table } => {
                    Some(serde_json::json!({ "operation": operation, "table": table }))
                }
                SqlValidationError::NotExactlyOneStatement { count } => {
                    Some(serde_json::json!({ "statement_count": count }))
                }
                SqlValidationError::CopyNotAllowed | SqlValidationError::ParseError(_) => None,
            };

            return Err((
                StatusCode::BAD_REQUEST,
                create_error_response(&e.to_string(), "sql_validation_error", details),
            ));
        }
    };

    // Queries get the row cap pushed into the plan (fetch cap + 1 so
    // truncation is detectable). Writes and other statements return small
    // result batches (e.g. an insert count) and run uncapped.
    let result = match statement_kind {
        StatementKind::Query => {
            app_state
                .engine
                .execute_with_limit(&request.sql, max_rows + 1)
                .await
        }
        StatementKind::Other => app_state.engine.execute(&request.sql).await,
    };

    let record_batch = match result {
        Ok(batch) => batch,
        Err(e) => {
            tracing::error!("Ad-hoc query execution failed: {}", e);
            tracing::debug!("Failed SQL query: {}", request.sql);

            let elapsed_ms = start_time.elapsed().as_millis() as f64;
            app_state
                .metrics
                .record_error(QUERY_METRICS_LABEL, elapsed_ms, "query_execution_error");

            let error_details = serde_json::json!({
                "engine_error": e.to_string(),
                "registered_tables": "Check server logs for data source registration status",
                "suggestion": "Verify that data sources are properly registered and accessible"
            });

            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                create_error_response(
                    &format!("SQL query execution failed: {}", e),
                    "query_execution_error",
                    Some(error_details),
                ),
            ));
        }
    };

    let truncated = statement_kind == StatementKind::Query && record_batch.num_rows() > max_rows;
    let record_batch = if truncated {
        record_batch.slice(0, max_rows)
    } else {
        record_batch
    };

    let data = match record_batch_to_json(&record_batch) {
        Ok(json_data) => json_data,
        Err(e) => {
            tracing::error!("Failed to convert results to JSON: {}", e);

            let elapsed_ms = start_time.elapsed().as_millis() as f64;
            app_state
                .metrics
                .record_error(QUERY_METRICS_LABEL, elapsed_ms, "result_conversion_error");

            let error_details = serde_json::json!({
                "conversion_error": e.to_string(),
                "record_batch_schema": format!("{:?}", record_batch.schema()),
                "record_batch_rows": record_batch.num_rows()
            });

            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                create_error_response(
                    &format!("Failed to convert query results to JSON: {}", e),
                    "result_conversion_error",
                    Some(error_details),
                ),
            ));
        }
    };

    let execution_time = start_time.elapsed().as_millis() as u64;
    let row_count = record_batch.num_rows();

    app_state
        .metrics
        .record_success(QUERY_METRICS_LABEL, execution_time as f64);

    tracing::info!(
        "Ad-hoc query completed: {} rows in {}ms (truncated: {})",
        row_count,
        execution_time,
        truncated
    );

    Ok(create_success_response(
        data,
        row_count,
        execution_time,
        Some(truncated),
    ))
}
```

- [ ] **Step 5: Mount the route and register the module**

5a. In `crates/server/src/lib.rs`, add to the module list (after `pub mod pipeline_handlers;`):

```rust
pub mod query_handlers;
```

5b. In `crates/server/src/server.rs`, add the import:

```rust
use crate::query_handlers::execute_query;
```

and add the route in `configure_routes`, after the `/data_source` line:

```rust
        .route("/query", post(execute_query))
```

- [ ] **Step 6: Run the integration tests to verify they pass**

Run: `cargo test -p skardi-server --test query_http`
Expected: PASS — all 12 tests.

- [ ] **Step 7: Run the full server test suite**

Run: `cargo test -p skardi-server`
Expected: PASS — no regressions (the `validate_pipeline_sql` refactor is covered by existing config tests).

- [ ] **Step 8: Commit**

```bash
git add crates/server/src/query_handlers.rs crates/server/src/config.rs crates/server/src/server.rs crates/server/src/lib.rs crates/server/tests/query_http.rs
git commit -m "feat(server): add POST /query endpoint for ad-hoc SQL

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Workspace verification

**Files:** none (verification only; fix anything that surfaces).

- [ ] **Step 1: Format and lint**

Run: `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings`
Expected: no diffs, no warnings. Fix any findings (respecting the Global Constraints) and re-run.

- [ ] **Step 2: Full workspace tests**

Run: `cargo test --workspace`
Expected: PASS.

- [ ] **Step 3: Commit (only if Step 1/2 required fixes)**

```bash
git add -A
git commit -m "chore: fmt/clippy fixes for query endpoint

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```
