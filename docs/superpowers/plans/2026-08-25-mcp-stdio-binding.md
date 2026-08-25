# MCP stdio Binding (`skardi mcp`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship Skardi's third agent-facing binding — an MCP-over-stdio server (`skardi mcp`) that projects every loaded pipeline plus `query` and `list_data_sources` as MCP tools, proxying to skardi-server over REST.

**Architecture:** Two-crate change. `crates/server` enriches `GET /pipelines` and `GET /pipeline/:name` with `description` and per-parameter `json_schema` fragments (Option A, decided in the spec). `crates/cli` gains a `skardi mcp` subcommand: a manual `rmcp::ServerHandler` implementation that fetches the enriched inventory on every `tools/list`, projects it to MCP tools, and routes `tools/call` through the existing `ApiClient`. A small prerequisite lands in `crates/skardi`: the `VALUES {name}` detection regex is exported as a shared static (it is currently transcribed twice, inline, in inferencer.rs).

**Tech Stack:** rmcp 3.1.4 (`default-features = false`), tokio, reqwest (existing `ApiClient`), wiremock for tests, uuid (workspace pin) for the per-connection session id.

**Spec:** `docs/superpowers/specs/2026-08-13-mcp-stdio-binding-design.md`

**Resolved open decisions (Owen, 2026-08-25):**
1. `query` includes the optional `purpose` parameter → `ai_context: {purpose, session_id}`.
2. Built-in tools are unprefixed: `query`, `list_data_sources`.
3. Lifecycle per spec recommendations: concurrent `tools/call` (rmcp's default — it spawns a task per request), stdin close → exit 0, no bridge-side request timeout (host's tool-call timeout is the backstop; documented in docs/mcp.md).

**Spec facts corrected against the current tree (fixed autonomously per review):**
- `docs/agent_data_plane.md` no longer exists (removed in the README repositioning, #205) — that doc task is dropped.
- README no longer has a roadmap checkbox or "MCP-soon" banner — the README task is now: update the "One definition, both bindings" copy (§4, ~line 113) to cover MCP as the third binding.

## Global Constraints

- **No local test runs.** Nobody runs tests locally — verification happens exclusively on GitHub CI. Do NOT run `cargo test` / `cargo nextest` / `cargo check` locally. The only pre-push command is `cargo fmt --all`.
- **No commits by Claude.** Each task ends with the working tree edited and `cargo fmt --all` run. Owen reviews and commits. "Checkpoint" steps below mean: stop editing, summarize, wait.
- CI bare-runs all `#[ignore]` tests (`cargo llvm-cov --no-report nextest --all-features -- --ignored`); nothing in this plan needs `#[ignore]`.
- Workspace: edition 2024, toolchain 1.96.1, `unused_qualifications = "deny"` (don't fully-qualify paths already imported), clippy `large_futures = "warn"`.
- rmcp is pinned at **3.1.4** (MSRV 1.88 — satisfied). Its 3.x API differs from 0.x: `CallToolRequestParams` (plural), `call_tool` returns `CallToolResponse` (enum; `From<CallToolResult>` exists), content is `ContentBlock`, model structs are `#[non_exhaustive]` (use constructors, never struct literals).
- **stdout is the MCP wire.** Nothing in the `skardi mcp` code path may print to stdout — no `output::print_result`, no `println!`. Diagnostics go to stderr via `eprintln!` (matching the CLI's existing house style; the CLI has no tracing/log framework and this plan doesn't add one — a deliberate, minimal deviation from the spec's "tracing/env_logger" wording).
- Server responses stay additive: existing fields (including `GET /pipeline/:name`'s Debug-dump `type` string) are unchanged; new fields are added alongside.
- Parameters in both enriched endpoints are **sorted by name** (RequestSchema.fields is a HashMap; sorting makes responses and tests deterministic).

---

### Task 1: Export the shared `VALUES {name}` regex from `crates/skardi`

The pattern `r"(?i)\bVALUES\s*\{([a-zA-Z_][a-zA-Z0-9_]*)\}"` is currently compiled inline twice in `crates/skardi/src/pipeline/inferencer.rs` (line 178 in `convert_named_to_placeholders`, and line 642 — without the capture group — in `replace_parameters_for_parsing`). The server-side enrichment (Task 2) needs the exact same detection; the spec says to share the compiled regex rather than re-transcribe it. `crates/server` has no `regex` dependency, so the shared item lives in `skardi`.

**Files:**
- Modify: `crates/skardi/src/pipeline/inferencer.rs`
- Test: same file's `#[cfg(test)] mod tests` (add if the module lacks regex-level tests)

**Interfaces:**
- Produces: `pub static VALUES_PLACEHOLDER_RE: LazyLock<Regex>` in `skardi::pipeline::inferencer` — capture group 1 is the parameter name. Task 2 consumes it as `skardi::pipeline::inferencer::VALUES_PLACEHOLDER_RE`.

- [ ] **Step 1: Write the failing test**

In `inferencer.rs`'s test module:

```rust
#[test]
fn values_placeholder_regex_detects_the_multi_row_shape() {
    let re = &*VALUES_PLACEHOLDER_RE;
    // case-insensitive, optional whitespace, captures the name
    let cap = re.captures("INSERT INTO t (a, b) values {rows}").unwrap();
    assert_eq!(&cap[1], "rows");
    let cap = re.captures("INSERT INTO t VALUES{rows}").unwrap();
    assert_eq!(&cap[1], "rows");
    // a parenthesized tuple of scalar params is NOT the multi-row shape
    assert!(re.captures("INSERT INTO t VALUES ({a}, {b})").is_none());
    // a parameter merely named like the keyword is not a match either
    assert!(re.captures("SELECT * FROM t WHERE x = {values_x}").is_none());
}
```

- [ ] **Step 2: Add the static and deduplicate both call sites**

At module level in `inferencer.rs` (near the top, after imports):

```rust
use std::sync::LazyLock;

/// Detects the multi-row tuple-list shape `VALUES {name}` (case-insensitive,
/// no trailing boundary — `}` is a non-word character, so a trailing `\b`
/// would never match). Capture group 1 is the parameter name. Shared by the
/// placeholder converters below and by skardi-server's parameter-schema
/// enrichment, which must agree with the loader on what counts as this shape.
pub static VALUES_PLACEHOLDER_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"(?i)\bVALUES\s*\{([a-zA-Z_][a-zA-Z0-9_]*)\}")
        .expect("VALUES placeholder regex is valid")
});
```

Then:
- In `convert_named_to_placeholders` (lines 171–182): delete the inline `values_pattern` compilation and its error mapping; use `VALUES_PLACEHOLDER_RE.replace_all(&sql_with_placeholders, "VALUES (?)")`. Keep the existing explanatory comment about why the stub is `VALUES (?)`.
- In `replace_parameters_for_parsing` (line 642): delete that inline compilation too and use the same static (the capture group is inert for a `replace_all` whose replacement string doesn't reference it — read the surrounding lines first and keep the replacement string exactly as it is today).

- [ ] **Step 3: `cargo fmt --all`**

- [ ] **Step 4: Checkpoint** — summarize the diff for Owen.

---

### Task 2: Server-side parameter → JSON Schema computation

Pure function in `crates/server`, next to the handlers that will call it. Implements the spec's mapping table, the unconditional null union, and the `VALUES {name}` override.

**Files:**
- Create: `crates/server/src/param_schema.rs`
- Modify: `crates/server/src/lib.rs` (or wherever sibling modules are declared — check how `pipeline_handlers` is declared and mirror it) to add `pub(crate) mod param_schema;`
- Test: unit tests inside `param_schema.rs`

**Interfaces:**
- Consumes: `skardi::pipeline::inferencer::VALUES_PLACEHOLDER_RE` (Task 1); `datafusion::arrow::datatypes::DataType` (same type as the direct `arrow` dep — arrow 57 is unified across the workspace).
- Produces: `pub(crate) fn param_json_schema(field_type: &DataType, sql_template: &str, param_name: &str) -> serde_json::Value` — Task 3 calls it per parameter.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
    use serde_json::json;

    fn schema(dt: DataType) -> serde_json::Value {
        param_json_schema(&dt, "SELECT 1 WHERE x = {p}", "p")
    }

    #[test]
    fn maps_scalar_types_with_null_union() {
        assert_eq!(schema(DataType::Utf8), json!({"type": ["string", "null"]}));
        assert_eq!(schema(DataType::LargeUtf8), json!({"type": ["string", "null"]}));
        assert_eq!(schema(DataType::Int64), json!({"type": ["integer", "null"]}));
        assert_eq!(schema(DataType::UInt32), json!({"type": ["integer", "null"]}));
        assert_eq!(schema(DataType::Float64), json!({"type": ["number", "null"]}));
        assert_eq!(schema(DataType::Decimal128(10, 2)), json!({"type": ["number", "null"]}));
        assert_eq!(schema(DataType::Boolean), json!({"type": ["boolean", "null"]}));
    }

    #[test]
    fn maps_temporal_types_with_format() {
        assert_eq!(
            schema(DataType::Date32),
            json!({"type": ["string", "null"], "format": "date"})
        );
        assert_eq!(
            schema(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            json!({"type": ["string", "null"], "format": "date-time"})
        );
    }

    #[test]
    fn maps_lists_with_typed_items() {
        let dt = DataType::List(Field::new("item", DataType::Utf8, true).into());
        assert_eq!(
            schema(dt),
            json!({"type": ["array", "null"], "items": {"type": "string"}})
        );
    }

    #[test]
    fn unknown_types_map_to_any() {
        assert_eq!(schema(DataType::Binary), json!({}));
    }

    #[test]
    fn values_placeholder_overrides_the_type_table() {
        let sql = "INSERT INTO docs (id, name, vec) values {rows}";
        assert_eq!(
            param_json_schema(&DataType::Utf8, sql, "rows"),
            json!({"type": "array", "items": {"type": "array"}})
        );
        // only the parameter named in the VALUES clause gets the override
        assert_eq!(
            param_json_schema(&DataType::Utf8, sql, "other"),
            json!({"type": ["string", "null"]})
        );
        // a parenthesized tuple list is not the multi-row shape
        assert_eq!(
            param_json_schema(&DataType::Utf8, "INSERT INTO t VALUES ({rows})", "rows"),
            json!({"type": ["string", "null"]})
        );
    }
}
```

- [ ] **Step 2: Implement**

```rust
//! Per-parameter JSON Schema fragments for the enriched pipeline inventory.
//!
//! Computed server-side (the enriched `GET /pipelines` response carries no
//! SQL, so downstream bindings cannot run the `VALUES` detection themselves).
//! The DataType → JSON Schema mapping and the unconditional `"null"` union
//! are specified in docs/superpowers/specs/2026-08-13-mcp-stdio-binding-design.md.

use datafusion::arrow::datatypes::DataType;
use serde_json::{Value, json};
use skardi::pipeline::inferencer::VALUES_PLACEHOLDER_RE;

/// The complete JSON Schema fragment for one pipeline parameter.
pub(crate) fn param_json_schema(field_type: &DataType, sql_template: &str, param_name: &str) -> Value {
    // `VALUES {name}` is a multi-row tuple list at request time; the inferred
    // Utf8 would be an actively wrong constraint, so it overrides wholesale.
    if VALUES_PLACEHOLDER_RE
        .captures_iter(sql_template)
        .any(|cap| &cap[1] == param_name)
    {
        return json!({"type": "array", "items": {"type": "array"}});
    }
    with_null_union(base_fragment(field_type))
}

/// The type-table mapping without the null union (list items use this raw).
fn base_fragment(field_type: &DataType) -> Value {
    match field_type {
        DataType::Utf8 | DataType::LargeUtf8 => json!({"type": "string"}),
        dt if dt.is_integer() => json!({"type": "integer"}),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => json!({"type": "number"}),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => json!({"type": "number"}),
        DataType::Boolean => json!({"type": "boolean"}),
        DataType::Date32 | DataType::Date64 => json!({"type": "string", "format": "date"}),
        DataType::Timestamp(_, _) => json!({"type": "string", "format": "date-time"}),
        DataType::List(inner) => {
            json!({"type": "array", "items": base_fragment(inner.data_type())})
        }
        _ => json!({}),
    }
}

/// Fold `"null"` into the fragment's `type` — unconditionally, because the
/// server accepts an explicit JSON `null` for every parameter (rendered as
/// SQL `NULL`) and the inferrer hardcodes `nullable: true`. The `{}` (any)
/// fragment already admits null and is left alone.
fn with_null_union(mut fragment: Value) -> Value {
    if let Some(ty) = fragment.get("type").cloned() {
        fragment["type"] = json!([ty, "null"]);
    }
    fragment
}
```

Notes for the implementer:
- `DataType::is_integer()` exists on arrow 57's `DataType` and covers `Int8..Int64` and `UInt8..UInt64`.
- If `Decimal32`/`Decimal64` variants exist in arrow 57, add them to the number arm (check the enum; compile will not force it since there's a `_` fallback — grep `Decimal` in the arrow docs/source first).
- If clippy objects to the `dt if dt.is_integer()` guard order vs. concrete patterns, reorder — behavior must keep `Utf8` before the guard.

- [ ] **Step 3: `cargo fmt --all`**

- [ ] **Step 4: Checkpoint** — summarize for Owen.

---

### Task 3: Enrich `GET /pipelines` and `GET /pipeline/:name`

**Files:**
- Modify: `crates/server/src/pipeline_handlers.rs:381-462` (`list_pipelines`, `get_pipelines_info`)
- Test: `crates/server/tests/pipelines_http.rs` (extend the two existing tests; add a no-description fixture and a `VALUES` fixture)

**Interfaces:**
- Consumes: `param_json_schema` (Task 2); `StandardPipeline` fields `metadata.description`, `query_definition.sql`, `request_schema.fields` (all `pub`; trait accessors `pipeline.request_schema()` / `pipeline.query_definition()` exist).
- Produces: each `GET /pipelines` list item and the `GET /pipeline/:name` `pipeline` object gain `"description": <string|null>` and `"parameters": [{"name", "data_type", "json_schema"}]` sorted by name. `/pipeline/:name` parameters additionally KEEP the existing `"type"` Debug string unchanged. This is the exact contract Task 5's projection consumes.

- [ ] **Step 1: Write the failing tests**

In `crates/server/tests/pipelines_http.rs`, extend `http_list_pipelines_returns_registered` (after the existing assertions on the `product-search` entry — its fixture already has `description: "Filter products by brand + max price"` and params `{brand}` → Utf8, `{max_price}` → Float64 via the `max_` prefix strip):

```rust
    assert_eq!(
        entry["description"].as_str(),
        Some("Filter products by brand + max price")
    );
    // parameters are sorted by name: brand, max_price
    let params = entry["parameters"].as_array().expect("parameters array");
    assert_eq!(params.len(), 2);
    assert_eq!(params[0]["name"].as_str(), Some("brand"));
    assert_eq!(params[0]["data_type"].as_str(), Some("Utf8"));
    assert_eq!(params[0]["json_schema"], serde_json::json!({"type": ["string", "null"]}));
    assert_eq!(params[1]["name"].as_str(), Some("max_price"));
    assert_eq!(params[1]["data_type"].as_str(), Some("Float64"));
    assert_eq!(params[1]["json_schema"], serde_json::json!({"type": ["number", "null"]}));
```

Extend `http_get_pipeline_info_returns_metadata_and_params` similarly (description + per-param `data_type`/`json_schema`), and assert the pre-existing `type` key is still present on each parameter (`params[0]["type"].as_str().unwrap().contains("field_type")` — it is the Debug dump of the whole `InferredFieldType`).

Add two new tests:

```rust
#[tokio::test]
async fn http_list_pipelines_description_is_null_when_yaml_omits_it() {
    // Load a second pipeline whose metadata has no `description:` line
    // (mirror make_app_state's fixture-loading, minus that line), insert it
    // into state.config.write().unwrap().pipelines, then GET /pipelines and
    // assert its entry has "description": null (present key, JSON null).
}

#[tokio::test]
async fn http_list_pipelines_values_param_gets_array_of_arrays_schema() {
    // Fixture pipeline:
    //   kind: pipeline
    //   metadata: { name: "bulk-insert", version: "1.0.0" }
    //   spec:
    //     query: |
    //       INSERT INTO products (id, brand, price, category) VALUES {rows}
    // Load it against the same `products` MemTable SessionContext, insert
    // into state, GET /pipelines, find the "bulk-insert" entry and assert
    // its single parameter is:
    //   {"name": "rows", "data_type": "Utf8",
    //    "json_schema": {"type": "array", "items": {"type": "array"}}}
}
```

(Write these fully — the comment bodies above describe intent; the code follows `make_app_state` / `body_to_json` patterns already in the file. The mutate-state-after-construction pattern is at `pipeline_handlers.rs:1987` in the unit tests: `let mut config = app_state.config.write().unwrap();`.)

- [ ] **Step 2: Implement**

Add a private helper in `pipeline_handlers.rs`:

```rust
/// The enriched parameter list for one pipeline, sorted by name so the
/// response is deterministic (RequestSchema.fields is a HashMap).
fn enriched_parameters(pipeline: &StandardPipeline) -> Vec<Value> {
    let sql = &pipeline.query_definition().sql;
    let mut fields: Vec<_> = pipeline.request_schema().fields.iter().collect();
    fields.sort_by(|(a, _), (b, _)| a.cmp(b));
    fields
        .into_iter()
        .map(|(name, field)| {
            serde_json::json!({
                "name": name,
                "data_type": format!("{:?}", field.field_type),
                "json_schema": crate::param_schema::param_json_schema(
                    &field.field_type, sql, name
                ),
            })
        })
        .collect()
}
```

(If `unused_qualifications` complains about the `crate::param_schema::` path with an import present, import `param_json_schema` at the top instead — pick one form, not both.)

In `list_pipelines` (line 392's `json!` block), add:

```rust
"description": pipeline.metadata.description,
"parameters": enriched_parameters(pipeline),
```

In `get_pipelines_info`: add `"description": pipeline.metadata.description` to the `pipeline` object (line 440's block), and rebuild the `params` vec (lines 427–436) from the sorted field list so each entry is:

```rust
serde_json::json!({
    "name": param_name,
    "type": format!("{:?}", field_type),   // unchanged legacy Debug dump
    "data_type": format!("{:?}", field_type.field_type),
    "json_schema": crate::param_schema::param_json_schema(
        &field_type.field_type, &pipeline.query_definition().sql, param_name
    ),
})
```

`StandardPipeline` may need importing into scope for the helper's signature — it's already imported in the test module; check the non-test imports (`skardi::pipeline::pipeline::StandardPipeline`).

- [ ] **Step 3: `cargo fmt --all`**

- [ ] **Step 4: Checkpoint** — server-side change complete; summarize for Owen. (This is the natural boundary if Owen wants the server change as its own commit/PR.)

---

### Task 4: CLI — dependencies, `Mcp` subcommand, capability gate

**Files:**
- Modify: `crates/cli/Cargo.toml`
- Modify: `crates/cli/src/main.rs` (Commands enum, dispatch, `capability_of`, the exhaustiveness test at `main.rs:369-382`)
- Modify: `crates/cli/src/cloud.rs` (`Capability` enum at `cloud.rs:28-63`)
- Test: `crates/cli/tests/cloud_gating.rs` (one new case)

**Interfaces:**
- Consumes: `ClientConfig::resolve` / `cloud::ensure_available` / `ApiClient::new` — the standard dispatch pipeline, unchanged.
- Produces: `Commands::Mcp` variant dispatching to `mcp::run(client)` (Task 6 defines `pub async fn run(client: ApiClient) -> anyhow::Result<()>` in a new top-level `src/mcp/` module, following the `src/login/` precedent); `Capability::Mcp` with `served_by_gateway() == false` (a cloud context refuses `skardi mcp` — the bridge needs pipeline execution and `/data_source`, which the gateway does not serve).

- [ ] **Step 1: Dependencies**

In `crates/cli/Cargo.toml` `[dependencies]`:

```toml
rmcp = { version = "3.1.4", default-features = false, features = ["server", "transport-io"] }
uuid = { workspace = true }
```

In `[dev-dependencies]` (feature unification adds the client features for tests only):

```toml
rmcp = { version = "3.1.4", default-features = false, features = ["client", "transport-child-process"] }
```

Note: rmcp's `server` feature force-enables `schemars 1.x`; the lockfile already contains schemars 1.2.1, so no new major. Manual dynamic tools never touch schemars — `Tool::input_schema` is `Arc<serde_json::Map<String, Value>>`.

- [ ] **Step 2: Write the failing test (cloud gating)**

In `crates/cli/tests/cloud_gating.rs`, add a case following the file's existing pattern (spawn helper `skardi(home, args)` at `:18-26`, cloud-context config fixture written `chmod 0600`): `skardi mcp` against a cloud context must exit non-zero with the same refusal stderr the other non-gateway commands get, and must issue no network traffic (no wiremock expectations needed if the existing cases assert purely on exit code + stderr — mirror whichever assertion style the neighboring cases use).

- [ ] **Step 3: Wire the subcommand**

`main.rs` Commands enum — add after `Health`:

```rust
/// Serve MCP over stdio, proxying tools to the server (for MCP hosts).
///
/// Spawned by an MCP host (Claude Desktop, Cursor, ...) as a long-lived
/// child process; speaks JSON-RPC on stdout, so it prints nothing else there.
Mcp,
```

`cloud.rs` `Capability` enum: add `Mcp` variant; its display/name string (check how the enum renders in refusal messages) is `"mcp"`; `served_by_gateway()` stays `matches!(self, Capability::Query | Capability::Schema)` (i.e. `Mcp` is not added there).

`main.rs` `capability_of`: add `Commands::Mcp => Some(Capability::Mcp)`.

`main.rs` dispatch match: add

```rust
Commands::Mcp => mcp::run(client).await,
```

— note this arm moves `client` by value (the bridge owns it); the other arms borrow, which is fine since match arms are exclusive. Add `mod mcp;` to the module declarations at `main.rs:20-27`. Create a stub `crates/cli/src/mcp/mod.rs` so the crate compiles:

```rust
use crate::client::ApiClient;

pub async fn run(_client: ApiClient) -> anyhow::Result<()> {
    anyhow::bail!("not implemented yet")
}
```

Update the exhaustiveness test `the_command_set_is_covered_exhaustively` (`main.rs:369-382`) with the `Mcp` arm.

- [ ] **Step 4: `cargo fmt --all`**

- [ ] **Step 5: Checkpoint** — summarize for Owen.

---

### Task 5: CLI — tool projection (pure functions)

**Files:**
- Create: `crates/cli/src/mcp/projection.rs`
- Modify: `crates/cli/src/mcp/mod.rs` (add `mod projection;`)
- Test: unit tests inside `projection.rs`

**Interfaces:**
- Consumes: the enriched `GET /pipelines` body (Task 3's contract) as `&serde_json::Value`.
- Produces (Task 6 consumes):
  - `pub(crate) fn project(inventory: &Value) -> (Vec<Tool>, HashMap<String, String>)` — tools = projected pipelines + the two built-ins appended; map = tool name → original pipeline name (pipelines only).
  - `pub(crate) fn builtin_tools() -> Vec<Tool>` (also used standalone in tests).
  - `pub(crate) const RESERVED_NAMES: [&str; 2] = ["query", "list_data_sources"];`

rmcp types used: `rmcp::model::{Tool, JsonObject}`. Build schemas with `serde_json::json!` then `serde_json::from_value::<JsonObject>(...)`; construct tools with `Tool::new(name, description, schema)` (`Tool` is `#[non_exhaustive]` — constructor only).

- [ ] **Step 1: Write the failing tests** (table-driven, in-module)

Cover, with exact expected values:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn inventory(pipelines: serde_json::Value) -> serde_json::Value {
        json!({"success": true, "pipelines": pipelines, "count": 0})
    }

    #[test]
    fn sanitizes_names_to_the_mcp_charset() {
        assert_eq!(sanitize("product-search"), "product-search");
        assert_eq!(sanitize("my.pipe/v2"), "my_pipe_v2");
        assert_eq!(sanitize("空 格"), "___"); // every non-[a-zA-Z0-9_-] byte→char replaced
        assert_eq!(sanitize(&"x".repeat(80)).len(), 64);
    }

    #[test]
    fn reserved_names_get_the_pipeline_suffix() {
        let (tools, map) = project(&inventory(json!([
            {"name": "query", "version": "1", "endpoint": "/query/execute",
             "description": null, "parameters": []}
        ])));
        assert_eq!(map.get("query_pipeline").map(String::as_str), Some("query"));
        assert!(tools.iter().any(|t| t.name == "query_pipeline"));
        // the built-in `query` is still present and distinct
        assert!(tools.iter().any(|t| t.name == "query"));
    }

    #[test]
    fn collisions_suffix_in_sorted_original_name_order() {
        let (_, map) = project(&inventory(json!([
            {"name": "a.b", "version": "1", "endpoint": "/a.b/execute", "description": null, "parameters": []},
            {"name": "a_b", "version": "1", "endpoint": "/a_b/execute", "description": null, "parameters": []},
            {"name": "a_b_2", "version": "1", "endpoint": "/a_b_2/execute", "description": null, "parameters": []}
        ])));
        // sorted originals: "a.b" < "a_b" < "a_b_2"
        assert_eq!(map.get("a_b").map(String::as_str), Some("a.b"));
        // "a_b" collides; "_2" is taken by the literal "a_b_2" pipeline → "_3"
        assert_eq!(map.get("a_b_2").map(String::as_str), Some("a_b_2"));
        assert_eq!(map.get("a_b_3").map(String::as_str), Some("a_b"));
    }

    #[test]
    fn suffixes_never_push_past_64_chars() {
        let long = "x".repeat(64);
        let (_, map) = project(&inventory(json!([
            {"name": long.clone(), "version": "1", "endpoint": "/x/execute", "description": null, "parameters": []},
            {"name": format!("{long}y"), "version": "1", "endpoint": "/xy/execute", "description": null, "parameters": []}
        ])));
        assert!(map.keys().all(|k| k.len() <= 64));
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn description_falls_back_when_yaml_omits_it() {
        let (tools, _) = project(&inventory(json!([
            {"name": "p", "version": "1", "endpoint": "/p/execute", "description": null, "parameters": []}
        ])));
        let tool = tools.iter().find(|t| t.name == "p").unwrap();
        assert_eq!(tool.description.as_deref(), Some("Execute pipeline `p`"));
    }

    #[test]
    fn input_schema_assembles_fragments_with_required_and_closed_object() {
        let (tools, _) = project(&inventory(json!([
            {"name": "p", "version": "1", "endpoint": "/p/execute",
             "description": "Search products",
             "parameters": [
                {"name": "brand", "data_type": "Utf8", "json_schema": {"type": ["string", "null"]}},
                {"name": "max_price", "data_type": "Float64", "json_schema": {"type": ["number", "null"]}}
             ]}
        ])));
        let tool = tools.iter().find(|t| t.name == "p").unwrap();
        let schema = serde_json::to_value(tool.input_schema.as_ref()).unwrap();
        assert_eq!(schema, json!({
            "type": "object",
            "properties": {
                "brand": {"type": ["string", "null"]},
                "max_price": {"type": ["number", "null"]}
            },
            "required": ["brand", "max_price"],
            "additionalProperties": false
        }));
        // original pipeline name echoed in the description for error correlation
        assert!(tool.description.as_deref().unwrap().contains('p'));
    }

    #[test]
    fn builtins_have_the_specified_schemas() {
        let tools = builtin_tools();
        let query = tools.iter().find(|t| t.name == "query").unwrap();
        let schema = serde_json::to_value(query.input_schema.as_ref()).unwrap();
        assert_eq!(schema["required"], json!(["sql"]));
        assert_eq!(schema["properties"]["sql"], json!({"type": "string"}));
        assert_eq!(schema["properties"]["max_rows"]["type"], json!("integer"));
        assert!(schema["properties"]["purpose"].is_object());
        let lds = tools.iter().find(|t| t.name == "list_data_sources").unwrap();
        let schema = serde_json::to_value(lds.input_schema.as_ref()).unwrap();
        assert_eq!(schema["type"], json!("object"));
    }
}
```

(Adjust `t.name == "..."` comparisons for `Cow` — `t.name.as_ref() == "..."` — as the compiler demands. The `sanitize("空 格")` expectation: replacement is per-`char`, so two hanzi + one space → `___`.)

- [ ] **Step 2: Implement**

```rust
//! Projection of the enriched pipeline inventory into MCP tool definitions.
//! Pure functions — everything here is unit-testable without a server.

use std::collections::HashMap;

use rmcp::model::{JsonObject, Tool};
use serde_json::{Value, json};

pub(crate) const RESERVED_NAMES: [&str; 2] = ["query", "list_data_sources"];
const MAX_TOOL_NAME: usize = 64;

/// Replace every char outside [a-zA-Z0-9_-] with '_' and truncate to 64.
fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
        .take(MAX_TOOL_NAME)
        .collect()
}

/// Assign unique tool names in sorted order of original pipeline name.
/// `taken` starts with the reserved built-ins; a candidate equal to a
/// reserved name is first renamed with `_pipeline` (stderr warning), then
/// the collision pass appends `_2`, `_3`, ... re-truncating the base so
/// base + suffix never exceeds 64, iterating until unique.
fn assign_tool_names(original_names: &[&str]) -> Vec<(String, String)> {
    let mut sorted: Vec<&str> = original_names.to_vec();
    sorted.sort_unstable();
    let mut taken: std::collections::HashSet<String> =
        RESERVED_NAMES.iter().map(|s| s.to_string()).collect();
    let mut assigned = Vec::with_capacity(sorted.len());
    for original in sorted {
        let mut candidate = sanitize(original);
        if RESERVED_NAMES.contains(&candidate.as_str()) {
            eprintln!(
                "warning: pipeline '{original}' collides with the built-in `{candidate}` tool; exposing it as `{candidate}_pipeline`"
            );
            candidate = format!("{candidate}_pipeline");
            candidate.truncate(MAX_TOOL_NAME);
        }
        if taken.contains(&candidate) {
            let mut n = 2usize;
            loop {
                let suffix = format!("_{n}");
                let mut base = candidate.clone();
                base.truncate(MAX_TOOL_NAME - suffix.len());
                let renamed = format!("{base}{suffix}");
                if !taken.contains(&renamed) {
                    candidate = renamed;
                    break;
                }
                n += 1;
            }
        }
        taken.insert(candidate.clone());
        assigned.push((candidate, original.to_string()));
    }
    assigned
}

fn object_schema(properties: Value, required: Vec<&str>) -> JsonObject {
    serde_json::from_value(json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false
    }))
    .expect("literal schema is a JSON object")
}

fn pipeline_tool(tool_name: &str, pipeline_name: &str, entry: &Value) -> Tool {
    let description = match entry["description"].as_str() {
        Some(d) if !d.trim().is_empty() => format!("{d} (pipeline `{pipeline_name}`)"),
        _ => format!("Execute pipeline `{pipeline_name}`"),
    };
    let mut properties = serde_json::Map::new();
    let mut required = Vec::new();
    if let Some(params) = entry["parameters"].as_array() {
        for param in params {
            if let Some(name) = param["name"].as_str() {
                properties.insert(name.to_string(), param["json_schema"].clone());
                required.push(name.to_string());
            }
        }
    }
    let schema: JsonObject = serde_json::from_value(json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false
    }))
    .expect("assembled schema is a JSON object");
    Tool::new(tool_name.to_string(), description, schema)
}

pub(crate) fn builtin_tools() -> Vec<Tool> {
    let query_schema = object_schema(
        json!({
            "sql": {"type": "string"},
            "max_rows": {"type": "integer", "description": "Result row cap; server default 1000."},
            "purpose": {"type": "string", "description": "One line on why you are running this query; recorded in the query audit log."}
        }),
        vec!["sql"],
    );
    let lds_schema = object_schema(json!({}), vec![]);
    vec![
        Tool::new(
            "query",
            "Run ad-hoc SQL against Skardi's federated engine. DML is only accepted on data sources configured with access_mode: read_write; DDL is always rejected. Use list_data_sources first to see available tables.",
            query_schema,
        ),
        Tool::new(
            "list_data_sources",
            "List Skardi's data sources: tables, column schemas, and plain-English semantic descriptions. Call this before writing ad-hoc SQL with `query`.",
            lds_schema,
        ),
    ]
}

/// Project the enriched `GET /pipelines` body into (tools, tool→pipeline map).
pub(crate) fn project(inventory: &Value) -> (Vec<Tool>, HashMap<String, String>) {
    let entries: HashMap<&str, &Value> = inventory["pipelines"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|e| e["name"].as_str().map(|n| (n, e)))
                .collect()
        })
        .unwrap_or_default();
    let originals: Vec<&str> = entries.keys().copied().collect();
    let mut tools = Vec::new();
    let mut map = HashMap::new();
    for (tool_name, pipeline_name) in assign_tool_names(&originals) {
        let entry = entries[pipeline_name.as_str()];
        tools.push(pipeline_tool(&tool_name, &pipeline_name, entry));
        map.insert(tool_name, pipeline_name);
    }
    tools.extend(builtin_tools());
    (tools, map)
}
```

Implementation notes:
- The `sanitize` truncation is by `char` count via `.take(64)`, but every kept char is ASCII (`_`, `-`, alphanumeric), so char count == byte count and `String::truncate` in the suffix pass can't split a code point.
- Description fallback text is exactly ``Execute pipeline `<name>` `` per the spec's field-source table; when a real description exists, the original pipeline name is appended in parens (spec: "the original pipeline name is echoed in the tool description so the model can correlate with server-side errors").
- The collision test expectations above pin the algorithm — if the implementation and test disagree, re-derive from the spec's sanitization section (spec lines 171–186), not from the code.

- [ ] **Step 3: `cargo fmt --all`**

- [ ] **Step 4: Checkpoint** — summarize for Owen.

---

### Task 6: CLI — the bridge (`ServerHandler`) and `mcp::run`

**Files:**
- Create: `crates/cli/src/mcp/bridge.rs`
- Modify: `crates/cli/src/mcp/mod.rs` (real `run`, `mod bridge;`)
- Test: wiremock unit tests inside `bridge.rs`

**Interfaces:**
- Consumes: `ApiClient::{get, post}` (both return `Result<Value, ApiError>`), `encode_component`, `projection::project`.
- Produces: `pub(crate) struct McpBridge` with **inherent** async methods `do_list_tools(&self) -> Result<ListToolsResult, McpError>` and `do_call_tool(&self, name: &str, args: Option<JsonObject>) -> Result<CallToolResult, McpError>` — the `ServerHandler` trait impl is a thin wrapper over these, because `RequestContext<RoleServer>` is `#[non_exhaustive]` and cannot be constructed in tests; wiremock tests drive the `do_*` methods directly, and the Task 7 e2e covers the trait wiring.

- [ ] **Step 1: Write the failing tests**

In `bridge.rs`'s test module (pattern: `commands/query.rs:53-183` — local `test_config` helper, `MockServer`, `body_json` matchers):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClientConfig;
    use serde_json::json;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_config(server: &str) -> ClientConfig {
        ClientConfig { server: server.to_string(), token: None, context: None }
    }

    fn bridge(server: &MockServer) -> McpBridge {
        McpBridge::new(ApiClient::new(&test_config(&server.uri())).unwrap())
    }

    fn inventory() -> serde_json::Value {
        json!({"success": true, "count": 1, "data_sources": 0,
               "pipelines": [{"name": "product-search", "version": "1.0.0",
                 "endpoint": "/product-search/execute",
                 "description": "Filter products",
                 "parameters": [{"name": "brand", "data_type": "Utf8",
                                 "json_schema": {"type": ["string", "null"]}}]}]})
    }

    #[tokio::test]
    async fn list_tools_projects_pipelines_and_builtins() {
        let server = MockServer::start().await;
        Mock::given(method("GET")).and(path("/pipelines"))
            .respond_with(ResponseTemplate::new(200).set_body_json(inventory()))
            .expect(1).mount(&server).await;
        let bridge = bridge(&server);
        let result = bridge.do_list_tools().await.unwrap();
        let names: Vec<&str> = result.tools.iter().map(|t| t.name.as_ref()).collect();
        assert!(names.contains(&"product-search"));
        assert!(names.contains(&"query"));
        assert!(names.contains(&"list_data_sources"));
    }

    #[tokio::test]
    async fn list_tools_maps_connect_failure_to_a_protocol_error() {
        let bridge = McpBridge::new(
            ApiClient::new(&test_config("http://127.0.0.1:1")).unwrap(),
        );
        let err = bridge.do_list_tools().await.unwrap_err();
        assert!(err.message.contains("cannot reach skardi-server"), "{}", err.message);
    }

    #[tokio::test]
    async fn pipeline_call_posts_flat_body_and_returns_verbatim_json() {
        let server = MockServer::start().await;
        Mock::given(method("GET")).and(path("/pipelines"))
            .respond_with(ResponseTemplate::new(200).set_body_json(inventory()))
            .mount(&server).await;
        Mock::given(method("POST")).and(path("/product-search/execute"))
            .and(body_json(json!({"brand": "acme"})))
            .respond_with(ResponseTemplate::new(200)
                .set_body_json(json!({"success": true, "data": [], "rows": 0})))
            .expect(1).mount(&server).await;
        let bridge = bridge(&server);
        bridge.do_list_tools().await.unwrap(); // builds the dispatch map
        let args = json!({"brand": "acme"}).as_object().cloned();
        let result = bridge.do_call_tool("product-search", args).await.unwrap();
        assert_eq!(result.is_error, Some(false));
    }

    #[tokio::test]
    async fn unknown_tool_is_a_protocol_error_nudging_a_relist() {
        let server = MockServer::start().await;
        let bridge = bridge(&server);
        let err = bridge.do_call_tool("nope", None).await.unwrap_err();
        assert!(err.message.contains("unknown tool"), "{}", err.message);
        assert!(err.message.contains("tools/list"), "{}", err.message);
    }

    #[tokio::test]
    async fn query_with_purpose_sends_ai_context_with_a_stable_session_id() {
        let server = MockServer::start().await;
        Mock::given(method("POST")).and(path("/query"))
            .respond_with(ResponseTemplate::new(200)
                .set_body_json(json!({"success": true, "data": [], "rows": 0})))
            .expect(2).mount(&server).await;
        let bridge = bridge(&server);
        for _ in 0..2 {
            let args = json!({"sql": "select 1", "purpose": "why"}).as_object().cloned();
            bridge.do_call_tool("query", args).await.unwrap();
        }
        let requests = server.received_requests().await.unwrap();
        let bodies: Vec<serde_json::Value> = requests.iter()
            .map(|r| serde_json::from_slice(&r.body).unwrap()).collect();
        let sid0 = bodies[0]["ai_context"]["session_id"].as_str().unwrap();
        let sid1 = bodies[1]["ai_context"]["session_id"].as_str().unwrap();
        assert_eq!(sid0, sid1);
        assert_eq!(bodies[0]["ai_context"]["purpose"], json!("why"));
    }

    #[tokio::test]
    async fn query_without_purpose_omits_ai_context_entirely() {
        let server = MockServer::start().await;
        Mock::given(method("POST")).and(path("/query"))
            .and(body_json(json!({"sql": "select 1", "max_rows": 5})))
            .respond_with(ResponseTemplate::new(200)
                .set_body_json(json!({"success": true, "data": [], "rows": 0})))
            .expect(1).mount(&server).await;
        let bridge = bridge(&server);
        let args = json!({"sql": "select 1", "max_rows": 5}).as_object().cloned();
        let result = bridge.do_call_tool("query", args).await.unwrap();
        assert_eq!(result.is_error, Some(false));
    }

    #[tokio::test]
    async fn server_error_becomes_is_error_tool_result_with_error_type() {
        let server = MockServer::start().await;
        Mock::given(method("POST")).and(path("/query"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "success": false, "error": "Missing required parameters: brand",
                "error_type": "parameter_validation_error"})))
            .mount(&server).await;
        let bridge = bridge(&server);
        let args = json!({"sql": "select 1"}).as_object().cloned();
        let result = bridge.do_call_tool("query", args).await.unwrap();
        assert_eq!(result.is_error, Some(true));
        let text = serde_json::to_string(&result.content).unwrap();
        assert!(text.contains("parameter_validation_error"), "{text}");
    }

    #[tokio::test]
    async fn list_data_sources_proxies_get_data_source_verbatim() {
        let server = MockServer::start().await;
        Mock::given(method("GET")).and(path("/data_source"))
            .respond_with(ResponseTemplate::new(200)
                .set_body_json(json!({"success": true, "data": [], "count": 0})))
            .expect(1).mount(&server).await;
        let bridge = bridge(&server);
        let result = bridge.do_call_tool("list_data_sources", None).await.unwrap();
        assert_eq!(result.is_error, Some(false));
    }
}
```

(Adjust field-access details — `ErrorData.message` is `Cow<'static, str>`; `result.content` serialization — as the compiler demands, keeping the assertions' meaning.)

- [ ] **Step 2: Implement the bridge**

```rust
//! MCP ⇄ REST bridge: a manual `ServerHandler` whose tools come from the
//! server's pipeline inventory at list time. All REST I/O goes through the
//! CLI's `ApiClient`; stdout belongs to the JSON-RPC transport, so nothing
//! here may print to it.

use std::collections::HashMap;
use std::sync::RwLock;

use rmcp::ErrorData as McpError;
use rmcp::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, Implementation,
    JsonObject, ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool,
};
use rmcp::service::{RequestContext, RoleServer};
use serde_json::Value;

use crate::client::{ApiClient, ApiError, encode_component};
use crate::mcp::projection;

const INSTRUCTIONS: &str = "Skardi is a federated SQL data plane: operator-defined \
pipelines plus an ad-hoc SQL engine over the configured data sources. Prefer the \
pipeline tools for tasks they cover. Before writing ad-hoc SQL with `query`, call \
`list_data_sources` to see tables, schemas, and their plain-English descriptions.";

pub(crate) struct McpBridge {
    client: ApiClient,
    /// tool name → original pipeline name; rebuilt on every tools/list.
    tool_map: RwLock<HashMap<String, String>>,
    /// One UUID per MCP connection; sent as ai_context.session_id when the
    /// model provides a `purpose`. This is the v1 agent-identity seam.
    session_id: String,
}

impl McpBridge {
    pub(crate) fn new(client: ApiClient) -> Self {
        McpBridge {
            client,
            tool_map: RwLock::new(HashMap::new()),
            session_id: uuid::Uuid::new_v4().to_string(),
        }
    }

    pub(crate) async fn do_list_tools(&self) -> Result<ListToolsResult, McpError> {
        let inventory = self
            .client
            .get("/pipelines")
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        let (tools, map) = projection::project(&inventory);
        *self.tool_map.write().expect("tool map lock") = map;
        Ok(ListToolsResult::with_all_items(tools))
    }

    pub(crate) async fn do_call_tool(
        &self,
        name: &str,
        args: Option<JsonObject>,
    ) -> Result<CallToolResult, McpError> {
        let outcome = match name {
            "query" => self.call_query(args).await,
            "list_data_sources" => self.client.get("/data_source").await,
            _ => {
                let pipeline = self.tool_map.read().expect("tool map lock").get(name).cloned();
                match pipeline {
                    Some(pipeline) => {
                        let body = Value::Object(args.unwrap_or_default());
                        let path = format!("/{}/execute", encode_component(&pipeline));
                        self.client.post(&path, &body).await
                    }
                    None => {
                        return Err(McpError::invalid_params(
                            format!(
                                "unknown tool '{name}' — the pipeline inventory may have \
                                 changed; re-issue tools/list to refresh it"
                            ),
                            None,
                        ));
                    }
                }
            }
        };
        Ok(match outcome {
            // Success: the response JSON verbatim, no client-side reshaping.
            Ok(value) => CallToolResult::success(vec![ContentBlock::text(value.to_string())]),
            // Execution errors are for the model to see and react to, not
            // protocol errors: ApiError's Display already carries the
            // server's message and error_type.
            Err(err) => CallToolResult::error(vec![ContentBlock::text(err.to_string())]),
        })
    }

    /// The one choke-point that assembles the query body; later versions
    /// extend this to full identity injection without touching call sites.
    async fn call_query(&self, args: Option<JsonObject>) -> Result<Value, ApiError> {
        let args = args.unwrap_or_default();
        let mut body = serde_json::Map::new();
        for key in ["sql", "max_rows"] {
            if let Some(v) = args.get(key) {
                body.insert(key.to_string(), v.clone());
            }
        }
        if let Some(purpose) = args.get("purpose").and_then(Value::as_str) {
            let purpose = purpose.trim();
            if !purpose.is_empty() {
                body.insert(
                    "ai_context".to_string(),
                    serde_json::json!({"purpose": purpose, "session_id": self.session_id}),
                );
            }
        }
        self.client.post("/query", &Value::Object(body)).await
    }
}

impl ServerHandler for McpBridge {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("skardi", env!("CARGO_PKG_VERSION")))
            .with_instructions(INSTRUCTIONS)
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        self.do_list_tools().await
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        self.do_call_tool(&request.name, request.arguments)
            .await
            .map(CallToolResponse::from)
    }
}
```

Adjust to what rmcp 3.1.4 actually exports (paths verified against docs.rs/rmcp/3.1.4 during research, but re-check on first compile): `McpError` is `rmcp::model::ErrorData` re-exported at crate root as `ErrorData`; `RequestContext`/`RoleServer` live under `rmcp::service`; `ServerInfo::new` / `with_server_info` / `with_instructions` are the constructor chain; trait methods are RPITIT so plain `async fn` implementations work.

- [ ] **Step 3: Implement `mcp::run` in `mod.rs`**

```rust
//! `skardi mcp` — serve MCP over stdio, proxying every tool call to a
//! running skardi-server over REST. The host (Claude Desktop, Cursor, ...)
//! spawns this subcommand as a long-lived child process.

mod bridge;
mod projection;

use rmcp::ServiceExt;
use rmcp::transport::stdio;

use crate::client::ApiClient;

pub async fn run(client: ApiClient) -> anyhow::Result<()> {
    let service = bridge::McpBridge::new(client)
        .serve(stdio())
        .await
        .map_err(|e| anyhow::anyhow!("MCP initialize handshake failed: {e}"))?;
    // Runs until the host closes stdin (or cancels); in-flight request tasks
    // die with the process — nobody is left to read their results.
    service.waiting().await?;
    Ok(())
}
```

Exit behavior: `waiting()` returning `QuitReason::Closed` falls through to `Ok(())` → exit 0, satisfying the decided lifecycle. Concurrency needs no code: rmcp spawns a task per incoming request.

- [ ] **Step 4: `cargo fmt --all`**

- [ ] **Step 5: Checkpoint** — summarize for Owen.

---

### Task 7: End-to-end spawned-binary test

Guards the "stdout is protocol-only" invariant permanently: any stray stdout print breaks the initialize handshake.

**Files:**
- Create: `crates/cli/tests/mcp_e2e.rs`

**Interfaces:**
- Consumes: `env!("CARGO_BIN_EXE_skardi")`; rmcp dev-features `client` + `transport-child-process`; wiremock.

- [ ] **Step 1: Write the test file**

```rust
//! End-to-end: spawn the real `skardi mcp` binary and speak MCP to it over
//! stdio with an rmcp client, against a wiremock "server". Also the
//! permanent guard for the stdout-is-protocol-only invariant — any stray
//! print to stdout breaks the initialize handshake below.
#![cfg(unix)]

use rmcp::ServiceExt;
use rmcp::model::CallToolRequestParams;
use rmcp::transport::{ConfigureCommandExt, TokioChildProcess};
use serde_json::json;
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn spawn_transport(server_url: &str, home: &std::path::Path) -> TokioChildProcess {
    TokioChildProcess::new(tokio::process::Command::new(env!("CARGO_BIN_EXE_skardi")).configure(
        |cmd| {
            cmd.env("HOME", home)
                .env_remove("SKARDI_SERVER_URL")
                .env_remove("SKARDI_API_TOKEN")
                .env_remove("SKARDI_CONTEXT")
                .args(["mcp", "--server", server_url]);
        },
    ))
    .expect("spawn skardi mcp")
}

fn inventory() -> serde_json::Value {
    json!({"success": true, "count": 1, "data_sources": 0,
           "pipelines": [{"name": "product-search", "version": "1.0.0",
             "endpoint": "/product-search/execute",
             "description": "Filter products by brand",
             "parameters": [{"name": "brand", "data_type": "Utf8",
                             "json_schema": {"type": ["string", "null"]}}]}]})
}

#[tokio::test]
async fn initialize_list_and_call_round_trip() {
    let server = MockServer::start().await;
    Mock::given(method("GET")).and(path("/pipelines"))
        .respond_with(ResponseTemplate::new(200).set_body_json(inventory()))
        .mount(&server).await;
    Mock::given(method("POST")).and(path("/product-search/execute"))
        .and(body_json(json!({"brand": "acme"})))
        .respond_with(ResponseTemplate::new(200)
            .set_body_json(json!({"success": true, "data": [{"id": 1}], "rows": 1})))
        .expect(1).mount(&server).await;

    let home = tempfile::TempDir::new().unwrap();
    let client = ()
        .serve(spawn_transport(&server.uri(), home.path()))
        .await
        .expect("initialize handshake (fails on any stray stdout output)");

    let info = client.peer_info().expect("server info");
    assert!(info.capabilities.tools.is_some());

    let tools = client.list_all_tools().await.unwrap();
    let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
    assert!(names.contains(&"product-search"), "{names:?}");
    assert!(names.contains(&"query"), "{names:?}");
    assert!(names.contains(&"list_data_sources"), "{names:?}");

    let result = client
        .call_tool(CallToolRequestParams {
            name: "product-search".into(),
            arguments: json!({"brand": "acme"}).as_object().cloned(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result.is_error, Some(false));

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn unreachable_server_fails_list_tools_but_not_the_handshake() {
    let home = tempfile::TempDir::new().unwrap();
    // Nothing listens on port 1: the handshake still succeeds (no REST call
    // is needed for initialize); tools/list surfaces the connect error.
    let client = ()
        .serve(spawn_transport("http://127.0.0.1:1", home.path()))
        .await
        .expect("handshake needs no server");
    let err = client.list_all_tools().await.unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("cannot reach skardi-server"), "{msg}");
    client.cancel().await.unwrap();
}
```

(Verify against rmcp 3.1.4's actual client API on first compile: `peer_info()` field paths, `list_all_tools`, the error's Display. Adjust assertion plumbing, not assertion meaning. If `ServerInfo`'s instructions are reachable via `peer_info()`, also assert they're non-empty.)

- [ ] **Step 2: `cargo fmt --all`**

- [ ] **Step 3: Checkpoint** — summarize for Owen; good point to push the branch and let CI run everything so far.

---

### Task 8: Documentation

**Files:**
- Create: `docs/mcp.md`
- Modify: `docs/cli.md` (new `## MCP — \`skardi mcp\`` section between `## Jobs` (ends ~line 467) and `## Exit codes` (line 468))
- Modify: `docs/pipelines.md:18` (MCP bullet: "(v1.1 roadmap)" → shipped, link `mcp.md`)
- Modify: `README.md` §4 (~lines 113-124): "One definition, both bindings" copy now covers three bindings; add a `skardi mcp` line to the example block's comment or adjacent text
- Modify: `docs/superpowers/specs/2026-08-13-mcp-stdio-binding-design.md` — move the three "Open decisions" to the "Decided" section, dated 2026-08-25, with the chosen values; correct the two stale doc-task facts (agent_data_plane.md gone; README roadmap gone)

**Interfaces:** none (prose).

- [ ] **Step 1: Write `docs/mcp.md`**

Sections (per spec's doc list, plus the decided lifecycle notes):
- **What it is** — third agent-facing binding; architecture one-liner + the host-spawns-bridge diagram from the spec.
- **Host setup** — Claude Desktop `claude_desktop_config.json` example (verbatim from the spec, lines 112-121) and a Cursor `mcpServers` example; note config resolution is identical to every other subcommand (flag → env → `~/.skardi/config.yaml`), and pointing `--server` at a remote skardi-server makes the bridge a local MCP gateway to it. Note: cloud contexts are refused (the gateway doesn't serve pipeline execution or `/data_source`).
- **Tool surface** — pipeline tools (name sanitization + collision rules in two sentences; description fallback), `query` (schema incl. `purpose` → audit log), `list_data_sources`.
- **Freshness** — inventory fetched on every `tools/list`; hosts that never re-list keep their snapshot; no `listChanged` in v1.
- **Auth notes** — Bearer token inherited from CLI config; `list_data_sources` and the pipeline inventory are readable without a token on today's server (existing REST behavior — deployments with sensitive semantics should weigh `/data_source` and `/pipelines` together).
- **Timeouts & lifecycle** — no bridge-side request timeout: a hung server hangs the tool call until the host's own tool-call timeout fires; bridge exits 0 when the host closes stdin; concurrent tool calls are served in parallel.
- **Troubleshooting** — "server unreachable" error text and the three config knobs; stdout purity (don't wrap the command in anything that prints); result size (query capped by `max_rows`, pipeline executions uncapped server-side, 256 MB client ceiling).

- [ ] **Step 2: Update `docs/cli.md`**

New section with: one-paragraph description, the Claude Desktop config block, a note that `skardi mcp` takes no subcommand-specific flags (global `--server`/`--token`/`--context` apply), link to `mcp.md` for the tool surface, and a sentence in **Exit codes** context: resolution/handshake failures follow the existing exit-code contract (2 = unreachable applies only to errors surfaced before serving; after serving starts, REST failures are reported in-band as tool errors).

- [ ] **Step 3: Update `docs/pipelines.md`, `README.md`, and the spec's Decided section**

- pipelines.md line 18: `- **MCP tools** — same YAML projected to MCP tools for non-Claude hosts via \`skardi mcp\` — see [mcp.md](mcp.md).`
- README §4: change "One definition, both bindings" to cover MCP (e.g. "One definition, every binding — shell, REST, and MCP") and add one comment line to the code block: `# MCP — hosts without a shell (Claude Desktop, …): skardi mcp`. Keep the section's shape; minimal edit.
- Spec: append to `## Decided`:
  - `purpose` on `query`: **included** (2026-08-25).
  - Built-in tool naming: **unprefixed** `query` / `list_data_sources` (2026-08-25).
  - Lifecycle: concurrent calls **yes** (rmcp default), stdin close → **exit 0**, request timeout — **none in the bridge**, host timeout is the backstop, documented in docs/mcp.md (2026-08-25).
  Delete the now-resolved `## Open decisions` section (or annotate each item "→ Decided, see above").

- [ ] **Step 4: Check links** — CI's `Markdown links` job validates relative links and anchors; make sure `docs/mcp.md` exists before anything links to it and anchors match exactly.

- [ ] **Step 5: `cargo fmt --all`** (no-op for docs, run anyway as the pre-push habit)

- [ ] **Step 6: Final checkpoint** — full-branch summary for Owen; Owen commits/pushes; verification happens on GitHub CI.

---

## Self-review notes (spec coverage)

- Spec §Tool projection (sanitization, collision, reserved names, fallback description, schema assembly, VALUES override) → Tasks 2, 5.
- Spec §Server-side change Option A (both endpoints, json_schema fragments, tests in crates/server) → Tasks 2, 3. The shared-regex "ideally" → Task 1.
- Spec §Built-in tools incl. `purpose`/`ai_context` seam → Tasks 5, 6.
- Spec §Freshness (fetch per list_tools, rebuild map) → Task 6.
- Spec §Execution flow & error mapping (flat body, verbatim success JSON, isError for REST failures, protocol error for unknown tool naming tools/list, connect-error wording via ApiError Display) → Task 6.
- Spec §Testing (projection unit tests in cli, mapping tests in server, wiremock bridge tests, spawned-binary e2e guarding stdout purity) → Tasks 2, 3, 5, 6, 7.
- Spec §Documentation updates → Task 8 (two items corrected against the current tree; noted in header).
- Spec §Non-goals — nothing here implements HTTP transport, jobs tools, OAuth, listChanged, resources, prompts, or per-parameter YAML descriptions.
- Not in spec, forced by the tree: `Capability::Mcp` cloud gating (Task 4) — the dispatch pipeline requires every command to declare a capability; `Mcp` is not gateway-served.
