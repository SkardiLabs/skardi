# InfluxDB Write Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Support `INSERT INTO <influxdb_table> ...` through Skardi's query engine, writing rows to InfluxDB 3 via its HTTP line-protocol API, gated by `access_mode: read_write`.

**Architecture:** The existing InfluxDB provider reads via Arrow Flight SQL, which is query-only. Writes take a second wire path: a `WritableInfluxTable` wrapper implements `TableProvider::insert_into`, whose `InfluxInsertExec` streams input batches, serializes each to line protocol with InfluxData's `influxdb-line-protocol` crate, and POSTs it to `<base>/api/v3/write_lp` with `reqwest`. Batch-per-POST: memory-bounded, non-atomic across batches (same contract as Mongo SQL DML).

**Tech Stack:** Rust (edition 2024), DataFusion 52, `datafusion-table-providers` 0.10 (Flight read path, unchanged), `influxdb-line-protocol` 2.0.0 (new), `reqwest` 0.12 (already in tree, becomes non-optional in the `skardi` crate).

**Spec:** `docs/superpowers/specs/2026-07-05-influxdb-write-design.md` (approved). Read it before starting.

## Global Constraints

From the spec and `AGENTS.md` (read `AGENTS.md` in full before starting — these are the highlights, not a replacement):

- **No `.unwrap()` in production code** (library/server). `.expect("why this cannot fail")` only for invariants a prior check rules out, e.g. `"DataType::Float64 guarantees Float64Array"`. `.unwrap()` is fine in `#[cfg(test)]` code and `crates/cli/`.
- **All imports at the top of the file** via `use`. Never inline paths like `std::sync::Arc<...>` in function bodies.
- Logging via `tracing::{info,warn,error,debug}` — never `println!`/`log`.
- Existing measurements only — no auto-create; registration's eager schema fetch is unchanged.
- `read_write` + `query`-defined source is a registration-time error.
- `UPDATE`/`DELETE` return explicit errors (not DataFusion defaults, which would say the misleading "not supported for View table" because `FlightTable` reports `TableType::View`).
- Job destinations for InfluxDB stay rejected — do not touch the executor's rejection logic (comment update only, Task 7).
- Null tag/field → omitted from the line; all-fields-null row → error; null/absent `time` → server-assigned timestamp; otherwise nanosecond precision.
- Pre-commit hooks run `cargo fmt` and `cargo check --all`. Never use `--no-verify`.
- Every commit message: imperative subject ≤70 chars; end body with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

**Verification commands used throughout:**

```bash
cargo check -p skardi                                    # fast compile check
cargo test -p skardi sources::providers::influxdb        # provider unit tests (offline)
cargo check --workspace --all-targets                    # full workspace check
```

Integration tests (`#[ignore]`) need a live InfluxDB 3 at `http://127.0.0.1:8181` with a `metrics` database — see `docs/influxdb/README.md` Quick Start steps 1–2 for the docker command. CI runs them automatically (`cargo llvm-cov --no-report nextest --all-features -- --ignored` after seeding InfluxDB; see `.github/workflows/ci.yml`).

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `crates/skardi/src/sources/providers/influxdb.rs` → `influxdb/mod.rs` | Move | Options parsing, registration, read path (`CountSafeFlightTable`) — content unchanged by the move |
| `crates/skardi/src/sources/providers/influxdb/write.rs` | Create | `InfluxWriteConfig`, column classification, line-protocol serialization, `WritableInfluxTable`, `InfluxInsertExec` |
| `crates/skardi/Cargo.toml` | Modify | `reqwest` non-optional; add `influxdb-line-protocol` |
| `crates/server/src/config.rs` | Modify | `WRITABLE_SOURCE_TYPES` + error text + pass `read_write` to registration |
| `crates/cli/src/main.rs` | Modify | Pass `read_write` to registration |
| `crates/skardi/src/jobs/executor.rs` | Modify | Comment update only (job destinations still rejected) |
| `crates/server/tests/influxdb_e2e.rs` | Modify | Write round-trip e2e; `crates/server/Cargo.toml` gains `reqwest` dev-dep |
| `docs/influxdb/README.md` | Modify | Remove read-only claims; add "Writing data" section |

---

### Task 1: Module restructure and dependency changes

Mechanical preparation: turn the single-file provider into a directory module (the file is ~750 lines and about to grow a write side; this matches the `mongo/` / `sqlite/` layout), and adjust dependencies. No behavior change.

**Files:**
- Move: `crates/skardi/src/sources/providers/influxdb.rs` → `crates/skardi/src/sources/providers/influxdb/mod.rs`
- Modify: `crates/skardi/Cargo.toml`

**Interfaces:**
- Produces: module path `crate::sources::providers::influxdb` unchanged (`providers/mod.rs` needs no edit); `influxdb_line_protocol` and `reqwest` available unconditionally in the `skardi` crate.

- [ ] **Step 1: Move the file with git**

```bash
mkdir crates/skardi/src/sources/providers/influxdb
git mv crates/skardi/src/sources/providers/influxdb.rs crates/skardi/src/sources/providers/influxdb/mod.rs
```

- [ ] **Step 2: Edit `crates/skardi/Cargo.toml`**

Three changes:

1. Line 20 — the `remote-embed` feature no longer toggles the dependency (it still gates the remote-embed *code* via `#[cfg(feature = "remote-embed")]`):

```toml
# before
remote-embed = ["dep:reqwest"]
# after
remote-embed = []
```

2. Line 44 — drop `optional = true` from reqwest (the InfluxDB write path compiles unconditionally):

```toml
# before
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"], optional = true }
# after
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
```

3. In the `# sources` dependency block (near `datafusion-table-providers`), add:

```toml
influxdb-line-protocol = "2.0.0"
```

- [ ] **Step 3: Verify it compiles and existing tests pass**

```bash
cargo check -p skardi
cargo test -p skardi sources::providers::influxdb
```

Expected: check succeeds; all existing influxdb unit tests PASS (the `#[ignore]` integration tests are skipped — that's fine).

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor(influxdb): move provider to directory module, add write-path deps"
```

---

### Task 2: Extract `resolve_token` helper

Behavior-preserving refactor: the token-resolution logic currently lives inline in `build_flight_options`; the write path (Task 3) needs the same logic, so extract it. Existing unit tests (`token_maps_to_bearer_authorization_header`, `token_env_resolves_from_environment`, `token_env_wins_over_inline_token`, `missing_token_env_variable_is_an_error`, `empty_token_env_variable_is_an_error`, `empty_inline_token_is_an_error`) already pin the behavior — they must pass unchanged.

**Files:**
- Modify: `crates/skardi/src/sources/providers/influxdb/mod.rs` (the token block inside `build_flight_options`, currently lines ~132–167)

**Interfaces:**
- Produces: `fn resolve_token(name: &str, options: &HashMap<String, String>) -> Result<Option<String>>` — private to the module; Task 3 uses it via `super::resolve_token`.

- [ ] **Step 1: Add the helper function**

Insert above `build_flight_options` in `mod.rs`:

```rust
/// Resolve the API token from `token_env` (preferred) or inline `token`.
/// Returns `Ok(None)` when neither option is present. Shared by the read
/// path (bearer header on Flight calls) and the write path (bearer header
/// on line-protocol POSTs), so the two can't drift.
fn resolve_token(name: &str, options: &HashMap<String, String>) -> Result<Option<String>> {
    if let Some(token_env) = options.get(OPT_TOKEN_ENV) {
        let value = std::env::var(token_env).with_context(|| {
            format!(
                "Environment variable '{token_env}' (option '{OPT_TOKEN_ENV}') not found \
                 for InfluxDB source '{name}' token"
            )
        })?;
        // A set-but-empty variable would otherwise produce a malformed
        // `authorization: Bearer ` header and a confusing 401 at query time;
        // treat it as a configuration error instead.
        if value.is_empty() {
            return Err(anyhow!(
                "Environment variable '{token_env}' (option '{OPT_TOKEN_ENV}') is set but \
                 empty for InfluxDB source '{name}'; expected an API token"
            ));
        }
        Ok(Some(value))
    } else if let Some(token) = options.get(OPT_TOKEN) {
        if token.is_empty() {
            return Err(anyhow!(
                "InfluxDB source '{name}' has an empty '{OPT_TOKEN}' option; provide a \
                 non-empty API token or omit the option"
            ));
        }
        tracing::warn!(
            "InfluxDB source '{name}' uses an inline '{OPT_TOKEN}' option; prefer \
             '{OPT_TOKEN_ENV}' to keep the API token out of YAML config"
        );
        Ok(Some(token.clone()))
    } else {
        Ok(None)
    }
}
```

- [ ] **Step 2: Replace the inline block in `build_flight_options`**

Delete the entire `let token = if let Some(token_env) = ... } else { None };` block (everything between the `// Bearer-token auth → ...` comment and the `if let Some(token) = token {` line) and replace with:

```rust
    // Bearer-token auth → authorization header on the Flight calls.
    let token = resolve_token(name, options)?;
```

Keep the `if let Some(token) = token { flight_opts.insert(...) }` block that follows.

- [ ] **Step 3: Verify existing tests still pass**

```bash
cargo test -p skardi sources::providers::influxdb
```

Expected: PASS, no test changes needed.

- [ ] **Step 4: Commit**

```bash
git add crates/skardi/src/sources/providers/influxdb/mod.rs
git commit -m "refactor(influxdb): extract resolve_token for reuse by the write path"
```

---

### Task 3: `InfluxWriteConfig` — write-path option parsing

Create `write.rs` with the config struct resolved once at registration time, plus the two new option constants in `mod.rs`.

**Files:**
- Create: `crates/skardi/src/sources/providers/influxdb/write.rs`
- Modify: `crates/skardi/src/sources/providers/influxdb/mod.rs` (add `mod write;` + two consts)
- Test: `#[cfg(test)]` module inside `write.rs`

**Interfaces:**
- Consumes: `super::{OPT_DATABASE, OPT_MEASUREMENT, OPT_QUERY, OPT_TABLE, resolve_token}` (Task 2).
- Produces: `pub(super) struct InfluxWriteConfig { measurement: String, database: String, write_url: String, token: Option<String>, tags_override: Option<Vec<String>> }` with `pub(super) fn from_options(name: &str, connection_string: &str, options: &HashMap<String, String>) -> Result<Self>`. Task 6 stores it in `WritableInfluxTable`; Task 7 constructs it during registration.

- [ ] **Step 1: Add the new option constants and module declaration to `mod.rs`**

After the existing `FLIGHT_PASSTHROUGH_PREFIX` const:

```rust
/// HTTP base URL for the line-protocol write endpoint, when it differs from
/// the Flight `connection_string` (e.g. a managed deployment with split
/// endpoints). Defaults to the connection string — InfluxDB 3 Core serves
/// Flight and the HTTP API on the same port.
const OPT_WRITE_ENDPOINT: &str = "write_endpoint";
/// Comma-separated list of tag columns, overriding the Dictionary-encoding
/// inference used to classify tags on the write path.
const OPT_TAGS: &str = "tags";

mod write;
```

- [ ] **Step 2: Write the failing tests**

Create `crates/skardi/src/sources/providers/influxdb/write.rs`:

```rust
//! Write path for the InfluxDB 3 provider.
//!
//! Flight SQL is query-only, so `INSERT INTO` cannot ride the read
//! connection: writes are translated to InfluxDB's line-protocol ingest API
//! (`POST /api/v3/write_lp`). [`WritableInfluxTable`] wraps the read-side
//! table and adds `insert_into`; [`InfluxInsertExec`] streams the input
//! batches, serializing each to line protocol (via `influxdb-line-protocol`,
//! InfluxData's own builder, so escaping rules stay upstream's problem) and
//! POSTing it. One POST per batch keeps memory bounded, but is NOT atomic
//! across batches: a mid-stream failure leaves earlier batches durable — the
//! same non-transactional contract as the Mongo SQL DML path.

use std::collections::HashMap;

use anyhow::{Result, anyhow};

use super::{OPT_DATABASE, OPT_MEASUREMENT, OPT_QUERY, OPT_TABLE, resolve_token};
use super::{OPT_TAGS, OPT_WRITE_ENDPOINT};

/// Everything the write path needs, resolved once at registration time so a
/// bad write config fails server startup, not the first INSERT.
#[derive(Debug, Clone)]
pub(super) struct InfluxWriteConfig {
    /// Line-protocol measurement written to (the `measurement`/`table` option).
    pub measurement: String,
    /// InfluxDB database, sent as the `db` query parameter on every write.
    pub database: String,
    /// Full write URL, e.g. `http://localhost:8181/api/v3/write_lp`.
    pub write_url: String,
    /// Bearer token, when auth is enabled.
    pub token: Option<String>,
    /// Explicit tag-column override from the `tags` option.
    pub tags_override: Option<Vec<String>>,
}

impl InfluxWriteConfig {
    pub(super) fn from_options(
        name: &str,
        connection_string: &str,
        options: &HashMap<String, String>,
    ) -> Result<Self> {
        let measurement = options
            .get(OPT_MEASUREMENT)
            .or_else(|| options.get(OPT_TABLE))
            .filter(|m| !m.is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "read_write InfluxDB source '{name}' requires a '{OPT_MEASUREMENT}' \
                     (or '{OPT_TABLE}') option; a '{OPT_QUERY}'-defined source has no \
                     insert target"
                )
            })?
            .clone();

        // The write path sends the database as the `db` query parameter, so
        // the friendly option is required here even though the read path can
        // fall back to a `flight.sql.header.database` passthrough.
        let database = options
            .get(OPT_DATABASE)
            .filter(|d| !d.is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "read_write InfluxDB source '{name}' requires a non-empty \
                     '{OPT_DATABASE}' option (sent as the 'db' parameter on writes)"
                )
            })?
            .clone();

        let base = options
            .get(OPT_WRITE_ENDPOINT)
            .map(String::as_str)
            .unwrap_or(connection_string)
            .trim_end_matches('/');
        if base.is_empty() {
            return Err(anyhow!(
                "InfluxDB source '{name}' has an empty '{OPT_WRITE_ENDPOINT}' option; \
                 provide the HTTP base URL of the write endpoint or omit the option"
            ));
        }

        let tags_override = match options.get(OPT_TAGS) {
            Some(raw) => {
                let tags: Vec<String> = raw
                    .split(',')
                    .map(str::trim)
                    .filter(|t| !t.is_empty())
                    .map(str::to_string)
                    .collect();
                if tags.is_empty() {
                    return Err(anyhow!(
                        "InfluxDB source '{name}' has a '{OPT_TAGS}' option with no \
                         tag names; expected a comma-separated column list"
                    ));
                }
                Some(tags)
            }
            None => None,
        };

        Ok(Self {
            measurement,
            database,
            write_url: format!("{base}/api/v3/write_lp"),
            token: resolve_token(name, options)?,
            tags_override,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    const CONN: &str = "http://localhost:8181";

    #[test]
    fn write_config_happy_path_defaults() {
        let cfg = InfluxWriteConfig::from_options(
            "cpu",
            CONN,
            &opts(&[("measurement", "cpu"), ("database", "metrics")]),
        )
        .unwrap();
        assert_eq!(cfg.measurement, "cpu");
        assert_eq!(cfg.database, "metrics");
        assert_eq!(cfg.write_url, "http://localhost:8181/api/v3/write_lp");
        assert!(cfg.token.is_none());
        assert!(cfg.tags_override.is_none());
    }

    #[test]
    fn write_config_accepts_table_alias() {
        let cfg = InfluxWriteConfig::from_options(
            "cpu",
            CONN,
            &opts(&[("table", "disk"), ("database", "metrics")]),
        )
        .unwrap();
        assert_eq!(cfg.measurement, "disk");
    }

    #[test]
    fn write_config_requires_measurement() {
        let err =
            InfluxWriteConfig::from_options("cpu", CONN, &opts(&[("database", "metrics")]))
                .unwrap_err();
        assert!(err.to_string().contains("requires a 'measurement'"), "got {err}");
    }

    #[test]
    fn write_config_requires_database() {
        let err =
            InfluxWriteConfig::from_options("cpu", CONN, &opts(&[("measurement", "cpu")]))
                .unwrap_err();
        assert!(err.to_string().contains("non-empty 'database'"), "got {err}");
    }

    #[test]
    fn write_config_empty_database_is_an_error() {
        let err = InfluxWriteConfig::from_options(
            "cpu",
            CONN,
            &opts(&[("measurement", "cpu"), ("database", "")]),
        )
        .unwrap_err();
        assert!(err.to_string().contains("non-empty 'database'"), "got {err}");
    }

    #[test]
    fn write_endpoint_overrides_connection_string_and_trims_slash() {
        let cfg = InfluxWriteConfig::from_options(
            "cpu",
            CONN,
            &opts(&[
                ("measurement", "cpu"),
                ("database", "metrics"),
                ("write_endpoint", "https://writes.example.com:8181/"),
            ]),
        )
        .unwrap();
        assert_eq!(
            cfg.write_url,
            "https://writes.example.com:8181/api/v3/write_lp"
        );
    }

    #[test]
    fn tags_option_is_parsed_and_trimmed() {
        let cfg = InfluxWriteConfig::from_options(
            "cpu",
            CONN,
            &opts(&[
                ("measurement", "cpu"),
                ("database", "metrics"),
                ("tags", " host , region "),
            ]),
        )
        .unwrap();
        assert_eq!(
            cfg.tags_override,
            Some(vec!["host".to_string(), "region".to_string()])
        );
    }

    #[test]
    fn empty_tags_option_is_an_error() {
        let err = InfluxWriteConfig::from_options(
            "cpu",
            CONN,
            &opts(&[
                ("measurement", "cpu"),
                ("database", "metrics"),
                ("tags", " , "),
            ]),
        )
        .unwrap_err();
        assert!(err.to_string().contains("no tag names"), "got {err}");
    }

    #[test]
    fn write_config_resolves_token_env() {
        let var = "SKARDI_TEST_INFLUX_WRITE_TOKEN_ENV";
        unsafe { std::env::set_var(var, "w-s3cr3t") };
        let cfg = InfluxWriteConfig::from_options(
            "cpu",
            CONN,
            &opts(&[
                ("measurement", "cpu"),
                ("database", "metrics"),
                ("token_env", var),
            ]),
        )
        .unwrap();
        unsafe { std::env::remove_var(var) };
        assert_eq!(cfg.token.as_deref(), Some("w-s3cr3t"));
    }
}
```

- [ ] **Step 3: Run the tests to verify they pass**

(The struct and tests land together in one file — the "failing first" checkpoint here is the compile.)

```bash
cargo test -p skardi sources::providers::influxdb::write
```

Expected: all 9 tests PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/skardi/src/sources/providers/influxdb/
git commit -m "feat(influxdb): add write-path option parsing (InfluxWriteConfig)"
```

---

### Task 4: Column classification and value extraction

Classify a measurement's Arrow schema into line-protocol roles (time / tags / fields), and extract per-row values from Arrow arrays. All planning-time type validation happens in `classify_columns` so execution never hits an unsupported type mid-stream.

**Files:**
- Modify: `crates/skardi/src/sources/providers/influxdb/write.rs`

**Interfaces:**
- Produces (all `pub(super)` or private to `write.rs`, used by Task 5/6):
  - `pub(super) struct LineProtocolSchema { time_idx: Option<usize>, tag_idxs: Vec<usize>, field_idxs: Vec<usize> }`
  - `pub(super) fn classify_columns(schema: &Schema, tags_override: Option<&[String]>) -> Result<LineProtocolSchema>`
  - `enum LpFieldValue<'a> { Float(f64), Int(i64), UInt(u64), Bool(bool), Str(&'a str) }` implementing `influxdb_line_protocol::builder::FieldValue`
  - `fn field_value<'a>(array: &'a dyn Array, row: usize, column: &str) -> Result<Option<LpFieldValue<'a>>>`
  - `fn tag_value<'a>(array: &'a dyn Array, row: usize, column: &str) -> Result<Option<&'a str>>`
  - `fn timestamp_nanos(array: &dyn Array, row: usize) -> Result<Option<i64>>`

- [ ] **Step 1: Add imports to `write.rs`**

Extend the `use` block at the top:

```rust
use std::fmt;

use datafusion::arrow::array::{
    Array, BooleanArray, DictionaryArray, Float16Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Int32Type, Schema, TimeUnit};
use influxdb_line_protocol::builder::FieldValue;
```

- [ ] **Step 2: Write the failing tests**

Append to the `tests` module in `write.rs`:

```rust
    use std::sync::Arc;

    use datafusion::arrow::array::{
        BooleanArray, DictionaryArray, Float64Array, Int64Array, StringArray,
        TimestampMillisecondArray, TimestampNanosecondArray, UInt64Array,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema, TimeUnit};

    fn dict_field(name: &str) -> Field {
        Field::new(
            name,
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        )
    }

    fn time_field() -> Field {
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )
    }

    #[test]
    fn classify_infers_dictionary_columns_as_tags() {
        let schema = Schema::new(vec![
            dict_field("host"),
            time_field(),
            Field::new("usage", DataType::Float64, true),
        ]);
        let lp = classify_columns(&schema, None).unwrap();
        assert_eq!(lp.time_idx, Some(1));
        assert_eq!(lp.tag_idxs, vec![0]);
        assert_eq!(lp.field_idxs, vec![2]);
    }

    #[test]
    fn classify_honors_tags_override_for_plain_utf8_columns() {
        let schema = Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new("usage", DataType::Float64, true),
        ]);
        let tags = vec!["host".to_string()];
        let lp = classify_columns(&schema, Some(&tags)).unwrap();
        assert_eq!(lp.tag_idxs, vec![0]);
        assert_eq!(lp.field_idxs, vec![1]);
    }

    #[test]
    fn classify_rejects_override_naming_a_missing_column() {
        let schema = Schema::new(vec![Field::new("usage", DataType::Float64, true)]);
        let tags = vec!["nope".to_string()];
        let err = classify_columns(&schema, Some(&tags)).unwrap_err();
        assert!(err.to_string().contains("'nope'"), "got {err}");
    }

    #[test]
    fn classify_requires_at_least_one_field_column() {
        let schema = Schema::new(vec![dict_field("host"), time_field()]);
        let err = classify_columns(&schema, None).unwrap_err();
        assert!(err.to_string().contains("no field columns"), "got {err}");
    }

    #[test]
    fn classify_rejects_unsupported_field_types_at_plan_time() {
        let schema = Schema::new(vec![Field::new(
            "blob",
            DataType::Binary,
            true,
        )]);
        let err = classify_columns(&schema, None).unwrap_err();
        assert!(err.to_string().contains("'blob'"), "got {err}");
    }

    #[test]
    fn classify_treats_non_timestamp_time_column_as_field() {
        // A Utf8 column that happens to be named "time" is not the line
        // timestamp — only a Timestamp-typed `time` column is.
        let schema = Schema::new(vec![Field::new("time", DataType::Utf8, true)]);
        let lp = classify_columns(&schema, None).unwrap();
        assert_eq!(lp.time_idx, None);
        assert_eq!(lp.field_idxs, vec![0]);
    }

    #[test]
    fn tag_value_reads_dictionary_and_plain_strings_and_nulls() {
        let dict: DictionaryArray<Int32Type> =
            vec![Some("us-west"), None].into_iter().collect();
        assert_eq!(tag_value(&dict, 0, "region").unwrap(), Some("us-west"));
        assert_eq!(tag_value(&dict, 1, "region").unwrap(), None);

        let plain = StringArray::from(vec![Some("h1"), None]);
        assert_eq!(tag_value(&plain, 0, "host").unwrap(), Some("h1"));
        assert_eq!(tag_value(&plain, 1, "host").unwrap(), None);
    }

    #[test]
    fn field_value_maps_arrow_types_and_nulls() {
        let f = Float64Array::from(vec![Some(1.5), None]);
        assert!(matches!(
            field_value(&f, 0, "v").unwrap(),
            Some(LpFieldValue::Float(x)) if x == 1.5
        ));
        assert!(field_value(&f, 1, "v").unwrap().is_none());

        let i = Int64Array::from(vec![7]);
        assert!(matches!(field_value(&i, 0, "v").unwrap(), Some(LpFieldValue::Int(7))));

        let u = UInt64Array::from(vec![9]);
        assert!(matches!(field_value(&u, 0, "v").unwrap(), Some(LpFieldValue::UInt(9))));

        let b = BooleanArray::from(vec![true]);
        assert!(matches!(field_value(&b, 0, "v").unwrap(), Some(LpFieldValue::Bool(true))));

        let s = StringArray::from(vec!["x"]);
        assert!(matches!(field_value(&s, 0, "v").unwrap(), Some(LpFieldValue::Str("x"))));
    }

    #[test]
    fn timestamp_nanos_converts_units_and_nulls() {
        let ns = TimestampNanosecondArray::from(vec![Some(1_700_000_000_000_000_123), None]);
        assert_eq!(
            timestamp_nanos(&ns, 0).unwrap(),
            Some(1_700_000_000_000_000_123)
        );
        assert_eq!(timestamp_nanos(&ns, 1).unwrap(), None);

        let ms = TimestampMillisecondArray::from(vec![1_700_000_000_000_i64]);
        assert_eq!(
            timestamp_nanos(&ms, 0).unwrap(),
            Some(1_700_000_000_000_000_000)
        );
    }
```

Note: `LpFieldValue` must be visible to the tests module — the enum is private to `write.rs` and the tests module is inside `write.rs`, so `use super::*` (already present) covers it.

- [ ] **Step 3: Run tests to verify they fail**

```bash
cargo test -p skardi sources::providers::influxdb::write
```

Expected: FAIL to compile — `classify_columns`, `tag_value`, `field_value`, `timestamp_nanos`, `LpFieldValue` not found.

- [ ] **Step 4: Implement**

Add to `write.rs` (below `InfluxWriteConfig`):

```rust
/// Column classification for one registered measurement: which column feeds
/// the line's timestamp, which are tags, which are fields.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct LineProtocolSchema {
    pub time_idx: Option<usize>,
    pub tag_idxs: Vec<usize>,
    pub field_idxs: Vec<usize>,
}

/// Classify a measurement's Arrow schema into line-protocol roles.
///
/// - a Timestamp column named `time` is the line timestamp;
/// - without an override, `Dictionary(Int32, Utf8)` columns are tags (that is
///   how InfluxDB 3 encodes tags over Flight);
/// - with a `tags` override, exactly the named columns are tags;
/// - everything else is a field, and its type must map to a line-protocol
///   value. Rejecting unsupported types here keeps failures at plan time
///   rather than per-row during execution.
pub(super) fn classify_columns(
    schema: &Schema,
    tags_override: Option<&[String]>,
) -> Result<LineProtocolSchema> {
    if let Some(tags) = tags_override {
        for tag in tags {
            if schema.field_with_name(tag).is_err() {
                return Err(anyhow!(
                    "tag column '{tag}' (from the 'tags' option) does not exist in the \
                     measurement schema"
                ));
            }
        }
    }

    let mut time_idx = None;
    let mut tag_idxs = Vec::new();
    let mut field_idxs = Vec::new();

    for (idx, field) in schema.fields().iter().enumerate() {
        if field.name() == "time" && matches!(field.data_type(), DataType::Timestamp(_, _)) {
            time_idx = Some(idx);
            continue;
        }

        let is_tag = match tags_override {
            Some(tags) => tags.iter().any(|t| t == field.name()),
            None => is_dictionary_utf8(field.data_type()),
        };

        if is_tag {
            match field.data_type() {
                DataType::Utf8 | DataType::LargeUtf8 => tag_idxs.push(idx),
                dt if is_dictionary_utf8(dt) => tag_idxs.push(idx),
                other => {
                    return Err(anyhow!(
                        "tag column '{}' has type {other}, expected a string type",
                        field.name()
                    ));
                }
            }
            continue;
        }

        match field.data_type() {
            DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Boolean => field_idxs.push(idx),
            other => {
                return Err(anyhow!(
                    "field column '{}' has type {other}, which has no line-protocol \
                     representation",
                    field.name()
                ));
            }
        }
    }

    if field_idxs.is_empty() {
        return Err(anyhow!(
            "measurement schema has no field columns; a line-protocol line requires at \
             least one field"
        ));
    }

    Ok(LineProtocolSchema {
        time_idx,
        tag_idxs,
        field_idxs,
    })
}

fn is_dictionary_utf8(dt: &DataType) -> bool {
    matches!(dt, DataType::Dictionary(key, value)
        if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8)
}

/// A single line-protocol field value, borrowed from an Arrow array.
///
/// `FieldValue` in `influxdb-line-protocol` is implemented for concrete
/// scalar types; this enum gives batch iteration one uniform type while
/// delegating the wire formatting (type suffixes, string escaping) to the
/// crate's own impls.
#[derive(Debug, Clone, Copy)]
enum LpFieldValue<'a> {
    Float(f64),
    Int(i64),
    UInt(u64),
    Bool(bool),
    Str(&'a str),
}

impl FieldValue for LpFieldValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Float(v) => FieldValue::fmt(v, f),
            Self::Int(v) => FieldValue::fmt(v, f),
            Self::UInt(v) => FieldValue::fmt(v, f),
            Self::Bool(v) => FieldValue::fmt(v, f),
            Self::Str(v) => FieldValue::fmt(v, f),
        }
    }
}

/// Extract one field value; `None` for SQL NULL (the field is omitted from
/// the line). The `expect`s are safe: the arm's `DataType` guarantees the
/// concrete array type.
fn field_value<'a>(
    array: &'a dyn Array,
    row: usize,
    column: &str,
) -> Result<Option<LpFieldValue<'a>>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let any = array.as_any();
    let value = match array.data_type() {
        DataType::Float16 => {
            let a = any
                .downcast_ref::<Float16Array>()
                .expect("DataType::Float16 guarantees Float16Array");
            LpFieldValue::Float(f64::from(a.value(row)))
        }
        DataType::Float32 => {
            let a = any
                .downcast_ref::<Float32Array>()
                .expect("DataType::Float32 guarantees Float32Array");
            LpFieldValue::Float(f64::from(a.value(row)))
        }
        DataType::Float64 => {
            let a = any
                .downcast_ref::<Float64Array>()
                .expect("DataType::Float64 guarantees Float64Array");
            LpFieldValue::Float(a.value(row))
        }
        DataType::Int8 => {
            let a = any
                .downcast_ref::<Int8Array>()
                .expect("DataType::Int8 guarantees Int8Array");
            LpFieldValue::Int(i64::from(a.value(row)))
        }
        DataType::Int16 => {
            let a = any
                .downcast_ref::<Int16Array>()
                .expect("DataType::Int16 guarantees Int16Array");
            LpFieldValue::Int(i64::from(a.value(row)))
        }
        DataType::Int32 => {
            let a = any
                .downcast_ref::<Int32Array>()
                .expect("DataType::Int32 guarantees Int32Array");
            LpFieldValue::Int(i64::from(a.value(row)))
        }
        DataType::Int64 => {
            let a = any
                .downcast_ref::<Int64Array>()
                .expect("DataType::Int64 guarantees Int64Array");
            LpFieldValue::Int(a.value(row))
        }
        DataType::UInt8 => {
            let a = any
                .downcast_ref::<UInt8Array>()
                .expect("DataType::UInt8 guarantees UInt8Array");
            LpFieldValue::UInt(u64::from(a.value(row)))
        }
        DataType::UInt16 => {
            let a = any
                .downcast_ref::<UInt16Array>()
                .expect("DataType::UInt16 guarantees UInt16Array");
            LpFieldValue::UInt(u64::from(a.value(row)))
        }
        DataType::UInt32 => {
            let a = any
                .downcast_ref::<UInt32Array>()
                .expect("DataType::UInt32 guarantees UInt32Array");
            LpFieldValue::UInt(u64::from(a.value(row)))
        }
        DataType::UInt64 => {
            let a = any
                .downcast_ref::<UInt64Array>()
                .expect("DataType::UInt64 guarantees UInt64Array");
            LpFieldValue::UInt(a.value(row))
        }
        DataType::Utf8 => {
            let a = any
                .downcast_ref::<StringArray>()
                .expect("DataType::Utf8 guarantees StringArray");
            LpFieldValue::Str(a.value(row))
        }
        DataType::LargeUtf8 => {
            let a = any
                .downcast_ref::<LargeStringArray>()
                .expect("DataType::LargeUtf8 guarantees LargeStringArray");
            LpFieldValue::Str(a.value(row))
        }
        DataType::Boolean => {
            let a = any
                .downcast_ref::<BooleanArray>()
                .expect("DataType::Boolean guarantees BooleanArray");
            LpFieldValue::Bool(a.value(row))
        }
        other => {
            return Err(anyhow!(
                "field column '{column}' has unsupported type {other}"
            ));
        }
    };
    Ok(Some(value))
}

/// Extract one tag value; `None` for SQL NULL (the tag is omitted from the
/// line).
fn tag_value<'a>(array: &'a dyn Array, row: usize, column: &str) -> Result<Option<&'a str>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let any = array.as_any();
    match array.data_type() {
        DataType::Utf8 => {
            let a = any
                .downcast_ref::<StringArray>()
                .expect("DataType::Utf8 guarantees StringArray");
            Ok(Some(a.value(row)))
        }
        DataType::LargeUtf8 => {
            let a = any
                .downcast_ref::<LargeStringArray>()
                .expect("DataType::LargeUtf8 guarantees LargeStringArray");
            Ok(Some(a.value(row)))
        }
        dt if is_dictionary_utf8(dt) => {
            let dict = any
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Dictionary(Int32, _) guarantees DictionaryArray<Int32Type>");
            let values = dict
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Dictionary(_, Utf8) guarantees StringArray values");
            let key = dict.keys().value(row);
            Ok(Some(values.value(key as usize)))
        }
        other => Err(anyhow!("tag column '{column}' has unsupported type {other}")),
    }
}

/// Extract the row's timestamp in nanoseconds; `None` for SQL NULL (the
/// server then assigns arrival time).
fn timestamp_nanos(array: &dyn Array, row: usize) -> Result<Option<i64>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let DataType::Timestamp(unit, _) = array.data_type() else {
        return Err(anyhow!(
            "time column has non-timestamp type {}",
            array.data_type()
        ));
    };
    let any = array.as_any();
    let (raw, factor): (i64, i64) = match unit {
        TimeUnit::Second => (
            any.downcast_ref::<TimestampSecondArray>()
                .expect("TimeUnit::Second guarantees TimestampSecondArray")
                .value(row),
            1_000_000_000,
        ),
        TimeUnit::Millisecond => (
            any.downcast_ref::<TimestampMillisecondArray>()
                .expect("TimeUnit::Millisecond guarantees TimestampMillisecondArray")
                .value(row),
            1_000_000,
        ),
        TimeUnit::Microsecond => (
            any.downcast_ref::<TimestampMicrosecondArray>()
                .expect("TimeUnit::Microsecond guarantees TimestampMicrosecondArray")
                .value(row),
            1_000,
        ),
        TimeUnit::Nanosecond => (
            any.downcast_ref::<TimestampNanosecondArray>()
                .expect("TimeUnit::Nanosecond guarantees TimestampNanosecondArray")
                .value(row),
            1,
        ),
    };
    raw.checked_mul(factor)
        .map(Some)
        .ok_or_else(|| anyhow!("timestamp {raw} overflows the nanosecond range"))
}
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cargo test -p skardi sources::providers::influxdb::write
```

Expected: PASS (Task 3's 9 tests + these 9).

- [ ] **Step 6: Commit**

```bash
git add crates/skardi/src/sources/providers/influxdb/write.rs
git commit -m "feat(influxdb): classify Arrow columns into line-protocol roles"
```

---

### Task 5: Batch → line-protocol serialization

**Files:**
- Modify: `crates/skardi/src/sources/providers/influxdb/write.rs`

**Interfaces:**
- Consumes: `classify_columns` output, `field_value`/`tag_value`/`timestamp_nanos` (Task 4).
- Produces: `pub(super) fn batch_to_line_protocol(measurement: &str, batch: &RecordBatch, lp_schema: &LineProtocolSchema) -> Result<Vec<u8>>` — used by `InfluxInsertExec` (Task 6).

Line-protocol facts the assertions rely on (verified against `influxdb-line-protocol` 2.0.0 source): each `close_line()` appends `\n`; floats render bare (`1.5`), ints as `7i`, uints as `9u`, bools as `true`/`false`, strings double-quoted; measurement escapes `,` and space; tag keys/values escape `,`, `=`, and space.

- [ ] **Step 1: Add imports**

```rust
use datafusion::arrow::record_batch::RecordBatch;
use influxdb_line_protocol::LineProtocolBuilder;
```

- [ ] **Step 2: Write the failing tests**

Append to the `tests` module in `write.rs`:

```rust
    use datafusion::arrow::record_batch::RecordBatch as TestRecordBatch;

    /// cpu-like batch: host (dict tag), time (ns), usage (f64 field).
    fn cpu_batch(
        hosts: Vec<Option<&str>>,
        times: Vec<Option<i64>>,
        usages: Vec<Option<f64>>,
    ) -> (TestRecordBatch, LineProtocolSchema) {
        let schema = Arc::new(Schema::new(vec![
            dict_field("host"),
            time_field(),
            Field::new("usage", DataType::Float64, true),
        ]));
        let host: DictionaryArray<Int32Type> = hosts.into_iter().collect();
        let batch = TestRecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(host),
                Arc::new(TimestampNanosecondArray::from(times)),
                Arc::new(Float64Array::from(usages)),
            ],
        )
        .unwrap();
        let lp = classify_columns(&schema, None).unwrap();
        (batch, lp)
    }

    fn lp_string(bytes: Vec<u8>) -> String {
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn serializes_tags_fields_and_timestamp() {
        let (batch, lp) = cpu_batch(
            vec![Some("h1"), Some("h2")],
            vec![Some(1_700_000_000_000_000_000), Some(1_700_000_060_000_000_000)],
            vec![Some(1.5), Some(2.0)],
        );
        let out = lp_string(batch_to_line_protocol("cpu", &batch, &lp).unwrap());
        assert_eq!(
            out,
            "cpu,host=h1 usage=1.5 1700000000000000000\n\
             cpu,host=h2 usage=2 1700000060000000000\n"
        );
    }

    #[test]
    fn null_tag_and_null_time_are_omitted() {
        let (batch, lp) = cpu_batch(vec![None], vec![None], vec![Some(3.5)]);
        let out = lp_string(batch_to_line_protocol("cpu", &batch, &lp).unwrap());
        assert_eq!(out, "cpu usage=3.5\n");
    }

    #[test]
    fn null_field_is_omitted_but_row_survives_with_other_fields() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let batch = TestRecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(vec![None, Some(1.0)])),
                Arc::new(Int64Array::from(vec![Some(7), None])),
            ],
        )
        .unwrap();
        let lp = classify_columns(&schema, None).unwrap();
        let out = lp_string(batch_to_line_protocol("m", &batch, &lp).unwrap());
        assert_eq!(out, "m b=7i\nm a=1\n");
    }

    #[test]
    fn all_null_fields_row_is_an_error() {
        let (batch, lp) = cpu_batch(vec![Some("h1")], vec![None], vec![None]);
        let err = batch_to_line_protocol("cpu", &batch, &lp).unwrap_err();
        assert!(err.to_string().contains("row 0"), "got {err}");
        assert!(err.to_string().contains("at least one field"), "got {err}");
    }

    #[test]
    fn field_type_suffixes_and_string_quoting() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int64, true),
            Field::new("u", DataType::UInt64, true),
            Field::new("ok", DataType::Boolean, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let batch = TestRecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![7])),
                Arc::new(UInt64Array::from(vec![9])),
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        )
        .unwrap();
        let lp = classify_columns(&schema, None).unwrap();
        let out = lp_string(batch_to_line_protocol("m", &batch, &lp).unwrap());
        assert_eq!(out, "m i=7i,u=9u,ok=true,s=\"x\"\n");
    }

    #[test]
    fn measurement_and_tag_values_are_escaped() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("v", DataType::Float64, true),
        ]));
        let batch = TestRecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["us,west 1"])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        let tags = vec!["region".to_string()];
        let lp = classify_columns(&schema, Some(&tags)).unwrap();
        let out = lp_string(batch_to_line_protocol("my measurement", &batch, &lp).unwrap());
        assert_eq!(out, "my\\ measurement,region=us\\,west\\ 1 v=1\n");
    }
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
cargo test -p skardi sources::providers::influxdb::write
```

Expected: FAIL to compile — `batch_to_line_protocol` not found.

- [ ] **Step 4: Implement**

Add to `write.rs`:

```rust
/// Serialize one Arrow batch to line protocol, one line per row.
pub(super) fn batch_to_line_protocol(
    measurement: &str,
    batch: &RecordBatch,
    lp_schema: &LineProtocolSchema,
) -> Result<Vec<u8>> {
    let schema = batch.schema();
    let mut builder = LineProtocolBuilder::new();

    for row in 0..batch.num_rows() {
        // Collect the row's non-null fields before starting the line: a line
        // with zero fields is invalid, and the builder's typestate cannot
        // back out of a started line.
        let mut fields: Vec<(&str, LpFieldValue)> =
            Vec::with_capacity(lp_schema.field_idxs.len());
        for &idx in &lp_schema.field_idxs {
            let name = schema.field(idx).name().as_str();
            if let Some(value) = field_value(batch.column(idx).as_ref(), row, name)? {
                fields.push((name, value));
            }
        }
        let Some((&(first_name, first_value), rest)) = fields.split_first() else {
            return Err(anyhow!(
                "row {row} has only NULL field values; a line-protocol line requires \
                 at least one field"
            ));
        };

        let mut line = builder.measurement(measurement);
        for &idx in &lp_schema.tag_idxs {
            let name = schema.field(idx).name().as_str();
            if let Some(value) = tag_value(batch.column(idx).as_ref(), row, name)? {
                line = line.tag(name, value);
            }
        }

        let mut line = line.field(first_name, first_value);
        for &(name, value) in rest {
            line = line.field(name, value);
        }

        builder = match lp_schema.time_idx {
            Some(idx) => match timestamp_nanos(batch.column(idx).as_ref(), row)? {
                Some(nanos) => line.timestamp(nanos).close_line(),
                None => line.close_line(),
            },
            None => line.close_line(),
        };
    }

    Ok(builder.build())
}
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cargo test -p skardi sources::providers::influxdb::write
```

Expected: PASS. If the two exact-string assertions fail on formatting minutiae (e.g. float rendering `2` vs `2.0`), inspect the actual output — the crate renders `f64` with Rust's `{}` Display, so `2.0_f64` prints as `2` — and fix the *expected* string only if the actual output is verifiably valid line protocol.

- [ ] **Step 6: Commit**

```bash
git add crates/skardi/src/sources/providers/influxdb/write.rs
git commit -m "feat(influxdb): serialize Arrow batches to line protocol"
```

---

### Task 6: `WritableInfluxTable` and `InfluxInsertExec`

The `TableProvider` wrapper that adds `insert_into`, rejects `UPDATE`/`DELETE`/overwrite with explicit errors, and the `ExecutionPlan` that streams batches to `write_lp`. Mirrors `MongoInsertExec` (`crates/skardi/src/sources/providers/mongo/mod.rs:787-917`): forwards `properties()` from the input, emits one single-row `count` batch per input batch.

**Files:**
- Modify: `crates/skardi/src/sources/providers/influxdb/write.rs`

**Interfaces:**
- Consumes: `InfluxWriteConfig` (Task 3), `classify_columns` + `batch_to_line_protocol` (Tasks 4–5).
- Produces: `pub(super) struct WritableInfluxTable` with `pub(super) fn new(inner: Arc<dyn TableProvider>, config: InfluxWriteConfig) -> Self`, implementing `TableProvider`. Task 7 wraps the read table in it during registration.

- [ ] **Step 1: Add imports**

```rust
use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::Statistics;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use futures::StreamExt;

use anyhow::Context;
```

(Merge with the existing `use anyhow::{Result, anyhow};` line: `use anyhow::{Context, Result, anyhow};`.)

- [ ] **Step 2: Write the failing tests**

Append to the `tests` module in `write.rs`:

```rust
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::prelude::SessionContext;

    fn test_config() -> InfluxWriteConfig {
        InfluxWriteConfig::from_options(
            "cpu",
            "http://localhost:8181",
            &opts(&[("measurement", "cpu"), ("database", "metrics")]),
        )
        .unwrap()
    }

    /// Writable wrapper over an in-memory stand-in for the Flight table.
    fn writable_table(fields: Vec<Field>) -> (WritableInfluxTable, Arc<Schema>) {
        let schema = Arc::new(Schema::new(fields));
        let mem = MemTable::try_new(schema.clone(), vec![vec![]]).unwrap();
        (
            WritableInfluxTable::new(Arc::new(mem), test_config()),
            schema,
        )
    }

    fn cpu_fields() -> Vec<Field> {
        vec![
            dict_field("host"),
            time_field(),
            Field::new("usage", DataType::Float64, true),
        ]
    }

    #[tokio::test]
    async fn delete_is_rejected_with_an_append_only_error() {
        let (table, _) = writable_table(cpu_fields());
        let ctx = SessionContext::new();
        let state = ctx.state();
        let err = table.delete_from(&state, vec![]).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("does not support DELETE"), "got {msg}");
        assert!(msg.contains("append-only"), "got {msg}");
        assert!(msg.contains("cpu"), "got {msg}");
    }

    #[tokio::test]
    async fn update_is_rejected_with_an_append_only_error() {
        let (table, _) = writable_table(cpu_fields());
        let ctx = SessionContext::new();
        let state = ctx.state();
        let err = table.update(&state, vec![], vec![]).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("does not support UPDATE"), "got {msg}");
        assert!(msg.contains("append-only"), "got {msg}");
    }

    #[tokio::test]
    async fn insert_overwrite_is_rejected() {
        let (table, schema) = writable_table(cpu_fields());
        let ctx = SessionContext::new();
        let state = ctx.state();
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let err = table
            .insert_into(&state, input, InsertOp::Overwrite)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("appends only"),
            "got {err}"
        );
    }

    #[tokio::test]
    async fn insert_append_produces_an_influx_insert_plan() {
        let (table, schema) = writable_table(cpu_fields());
        let ctx = SessionContext::new();
        let state = ctx.state();
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let plan = table
            .insert_into(&state, input, InsertOp::Append)
            .await
            .unwrap();
        assert_eq!(plan.name(), "InfluxInsertExec");
    }

    #[tokio::test]
    async fn insert_into_surfaces_classification_errors_as_plan_errors() {
        // Schema with tags/time but no field columns → classification fails,
        // and the error must name the measurement.
        let (table, schema) = writable_table(vec![dict_field("host"), time_field()]);
        let ctx = SessionContext::new();
        let state = ctx.state();
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let err = table
            .insert_into(&state, input, InsertOp::Append)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("cpu"), "got {msg}");
        assert!(msg.contains("no field columns"), "got {msg}");
    }
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
cargo test -p skardi sources::providers::influxdb::write
```

Expected: FAIL to compile — `WritableInfluxTable` not found.

- [ ] **Step 4: Implement**

Add to `write.rs`:

```rust
/// Read-write wrapper around the read-side InfluxDB table.
///
/// Scans delegate to `inner` (Flight SQL, including the count(*) workaround
/// in `CountSafeFlightTable`); `insert_into` produces an [`InfluxInsertExec`]
/// that writes via the line-protocol HTTP API. UPDATE/DELETE are rejected
/// with explicit errors: InfluxDB 3 has no row-level update/delete, and
/// DataFusion's default message would misleadingly blame a "View table"
/// (`FlightTable` reports `TableType::View`).
#[derive(Debug)]
pub(super) struct WritableInfluxTable {
    inner: Arc<dyn TableProvider>,
    config: Arc<InfluxWriteConfig>,
    /// One connection pool per registered source, shared by all inserts.
    client: reqwest::Client,
}

impl WritableInfluxTable {
    pub(super) fn new(inner: Arc<dyn TableProvider>, config: InfluxWriteConfig) -> Self {
        Self {
            inner,
            config: Arc::new(config),
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl TableProvider for WritableInfluxTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if insert_op != InsertOp::Append {
            return Err(DataFusionError::NotImplemented(format!(
                "InfluxDB INSERT supports appends only; {insert_op:?} is not supported"
            )));
        }
        let lp_schema = classify_columns(
            self.schema().as_ref(),
            self.config.tags_override.as_deref(),
        )
        .map_err(|e| {
            DataFusionError::Plan(format!(
                "cannot write to InfluxDB measurement '{}': {e}",
                self.config.measurement
            ))
        })?;
        Ok(Arc::new(InfluxInsertExec {
            input,
            lp_schema: Arc::new(lp_schema),
            config: Arc::clone(&self.config),
            client: self.client.clone(),
        }))
    }

    async fn delete_from(
        &self,
        _state: &dyn Session,
        _filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(format!(
            "InfluxDB does not support DELETE — the line-protocol write path is \
             append-only (measurement '{}')",
            self.config.measurement
        )))
    }

    async fn update(
        &self,
        _state: &dyn Session,
        _assignments: Vec<(String, Expr)>,
        _filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(format!(
            "InfluxDB does not support UPDATE — the line-protocol write path is \
             append-only (measurement '{}')",
            self.config.measurement
        )))
    }
}

/// Streams the input, POSTing each batch as one line-protocol request.
/// Emits one single-row `count` batch per input batch (matching
/// `MongoInsertExec`); DataFusion sums them into the INSERT's row count.
struct InfluxInsertExec {
    input: Arc<dyn ExecutionPlan>,
    lp_schema: Arc<LineProtocolSchema>,
    config: Arc<InfluxWriteConfig>,
    client: reqwest::Client,
}

impl fmt::Debug for InfluxInsertExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InfluxInsertExec")
            .field("measurement", &self.config.measurement)
            .finish()
    }
}

impl DisplayAs for InfluxInsertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InfluxInsertExec: measurement={}", self.config.measurement)
    }
}

impl ExecutionPlan for InfluxInsertExec {
    fn name(&self) -> &str {
        "InfluxInsertExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(InfluxInsertExec {
            input: Arc::clone(&children[0]),
            lp_schema: Arc::clone(&self.lp_schema),
            config: Arc::clone(&self.config),
            client: self.client.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let state = WriteState {
            input: self.input.execute(partition, context)?,
            lp_schema: Arc::clone(&self.lp_schema),
            config: Arc::clone(&self.config),
            client: self.client.clone(),
        };
        let stream = futures::stream::unfold(state, |mut st| async move {
            match st.input.next().await {
                Some(Ok(batch)) => {
                    let result = write_batch(&st, &batch).await;
                    Some((result, st))
                }
                Some(Err(e)) => Some((Err(e), st)),
                None => None,
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(count_schema(), stream)))
    }
}

/// Everything one partition's write stream needs, bundled for `unfold`.
struct WriteState {
    input: SendableRecordBatchStream,
    lp_schema: Arc<LineProtocolSchema>,
    config: Arc<InfluxWriteConfig>,
    client: reqwest::Client,
}

async fn write_batch(st: &WriteState, batch: &RecordBatch) -> DFResult<RecordBatch> {
    let body = batch_to_line_protocol(&st.config.measurement, batch, &st.lp_schema)
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "failed to serialize batch for InfluxDB measurement '{}': {e}",
                st.config.measurement
            ))
        })?;
    send_line_protocol(&st.client, &st.config, body)
        .await
        .map_err(|e| DataFusionError::Execution(format!("{e:#}")))?;
    create_count_batch(batch.num_rows() as u64)
}

async fn send_line_protocol(
    client: &reqwest::Client,
    config: &InfluxWriteConfig,
    body: Vec<u8>,
) -> Result<()> {
    let mut request = client
        .post(&config.write_url)
        .query(&[
            ("db", config.database.as_str()),
            ("precision", "nanosecond"),
        ])
        .body(body);
    if let Some(token) = &config.token {
        request = request.bearer_auth(token);
    }
    let response = request
        .send()
        .await
        .with_context(|| format!("InfluxDB write to {} failed", config.write_url))?;
    let status = response.status();
    if !status.is_success() {
        // InfluxDB returns per-line diagnostics in the body — surface them.
        let detail = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "InfluxDB write to {} returned {status}: {detail}",
            config.write_url
        ));
    }
    Ok(())
}

fn count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

fn create_count_batch(count: u64) -> DFResult<RecordBatch> {
    let array = UInt64Array::from(vec![count]);
    RecordBatch::try_new(count_schema(), vec![Arc::new(array)]).map_err(DataFusionError::from)
}
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cargo test -p skardi sources::providers::influxdb::write
```

Expected: PASS (all write.rs tests so far).

- [ ] **Step 6: Commit**

```bash
git add crates/skardi/src/sources/providers/influxdb/write.rs
git commit -m "feat(influxdb): add WritableInfluxTable and InfluxInsertExec"
```

---

### Task 7: Registration wiring — provider, server, CLI

Thread `read_write` through registration; make `access_mode: read_write` legal for InfluxDB in server config; update all call sites.

**Files:**
- Modify: `crates/skardi/src/sources/providers/influxdb/mod.rs` (`register_influxdb_tables` signature + wrap logic + existing tests)
- Modify: `crates/server/src/config.rs` (`WRITABLE_SOURCE_TYPES` ~line 698, `UnsupportedWriteMode` message ~line 212, InfluxDB call site ~line 1293)
- Modify: `crates/cli/src/main.rs` (InfluxDB call site ~line 913, access-mode doc comment ~line 232)
- Modify: `crates/skardi/src/jobs/executor.rs` (comment only, ~line 338)

**Interfaces:**
- Consumes: `WritableInfluxTable::new` and `InfluxWriteConfig::from_options` (Tasks 3, 6).
- Produces: `pub async fn register_influxdb_tables(session_ctx: &mut SessionContext, name: &str, connection_string: &str, options: Option<&HashMap<String, String>>, read_write: bool) -> Result<()>` — the new fifth parameter. Both call sites pass their source's read-write flag.

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module in `mod.rs`:

```rust
    #[tokio::test]
    async fn read_write_with_query_option_errors_before_connecting() {
        let mut ctx = SessionContext::new();
        let options = opts(&[("database", "metrics"), ("query", "SELECT 1")]);
        let err = register_influxdb_tables(
            &mut ctx,
            "cpu",
            // Deliberately unroutable; validation must fail before dialing.
            "http://127.0.0.1:1",
            Some(&options),
            true,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("cannot combine"),
            "got {err}"
        );
    }

    #[tokio::test]
    async fn read_write_without_measurement_errors_before_connecting() {
        let mut ctx = SessionContext::new();
        let options = opts(&[("database", "metrics")]);
        let err = register_influxdb_tables(
            &mut ctx,
            "cpu",
            "http://127.0.0.1:1",
            Some(&options),
            true,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("requires either a 'query'")
                || err.to_string().contains("requires a 'measurement'"),
            "got {err}"
        );
    }
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p skardi sources::providers::influxdb
```

Expected: FAIL to compile — `register_influxdb_tables` takes 4 arguments, 5 supplied.

- [ ] **Step 3: Change the registration function**

In `mod.rs`, change `register_influxdb_tables`:

1. Import the write types at the top: `use write::{InfluxWriteConfig, WritableInfluxTable};`
2. New signature and body changes:

```rust
/// Register an InfluxDB 3 measurement (or arbitrary SQL query) as a Skardi table
/// backed by the source's Arrow Flight SQL endpoint.
///
/// `connection_string` is the Flight gRPC endpoint URL, e.g.
/// `http://localhost:8181` for a local InfluxDB 3 Core instance. The table is
/// registered under `name`; its Arrow schema is inferred at registration time
/// from the server's `GetFlightInfo` response, matching how the other database
/// providers connect eagerly during config load.
///
/// With `read_write`, the table also accepts `INSERT INTO`, translated to the
/// line-protocol ingest API (see the `write` module). Writes require a
/// `measurement`/`table`-defined source — a `query`-defined source has no
/// insert target — and target existing measurements only.
pub async fn register_influxdb_tables(
    session_ctx: &mut SessionContext,
    name: &str,
    connection_string: &str,
    options: Option<&HashMap<String, String>>,
    read_write: bool,
) -> Result<()> {
    let options = options.ok_or_else(|| {
        anyhow!(
            "InfluxDB data source '{}' requires options (database, and query or measurement)",
            name
        )
    })?;

    // Resolve the write config before dialing, so a bad read_write setup
    // fails startup with a config error rather than a network error.
    let write_config = if read_write {
        if options.contains_key(OPT_QUERY) {
            return Err(anyhow!(
                "InfluxDB source '{name}' cannot combine read_write access with a \
                 '{OPT_QUERY}' option; a query-defined source has no insert target — \
                 define the source with '{OPT_MEASUREMENT}' instead"
            ));
        }
        Some(InfluxWriteConfig::from_options(name, connection_string, options)?)
    } else {
        None
    };

    let flight_opts = build_flight_options(name, options)?;

    // ... (existing tracing::info!, driver, factory, open_table block unchanged) ...

    let read_table = CountSafeFlightTable {
        inner: Arc::new(table),
    };
    let provider: Arc<dyn TableProvider> = match write_config {
        Some(config) => Arc::new(WritableInfluxTable::new(Arc::new(read_table), config)),
        None => Arc::new(read_table),
    };
    session_ctx
        .register_table(name, provider)
        .with_context(|| format!("Failed to register InfluxDB table '{}'", name))?;

    tracing::info!("Successfully registered InfluxDB table: {}", name);
    Ok(())
}
```

(The `session_ctx.register_table(name, Arc::new(table))` line is replaced by the `provider` match above — everything between `open_table` and the final `tracing::info!` otherwise stays.)

3. Update the existing call sites **inside `mod.rs` tests** — `register_without_options_errors`, `register_without_query_or_measurement_errors_before_connecting`, `register_ci_measurement`, and `integration_register_with_explicit_query_option` — to pass `false` as the new last argument.

- [ ] **Step 4: Update the server**

In `crates/server/src/config.rs`:

1. `WRITABLE_SOURCE_TYPES` (~line 698) gains `DataSourceType::Influxdb`:

```rust
/// Data source types that support read_write access mode
const WRITABLE_SOURCE_TYPES: &[DataSourceType] = &[
    DataSourceType::Postgres,
    DataSourceType::Mysql,
    DataSourceType::Sqlite,
    DataSourceType::Mongo,
    DataSourceType::Redis,
    DataSourceType::Seekdb,
    DataSourceType::Influxdb,
];
```

2. The `UnsupportedWriteMode` error text (~line 212) — replace the source list:

```rust
        "Data source '{name}' has access_mode 'read_write' but type '{source_type:?}' does not support write operations. Only 'postgres', 'mysql', 'sqlite', 'mongo', 'redis', 'seekdb', and 'influxdb' sources support read_write mode."
```

3. The InfluxDB registration call (~line 1293) gains the flag:

```rust
            register_influxdb_tables(
                session_ctx,
                &source.name,
                connection_string,
                source.options.as_ref(),
                source.access_mode.is_read_write(),
            )
```

- [ ] **Step 5: Update the CLI**

In `crates/cli/src/main.rs`:

1. The registration call (~line 913):

```rust
            register_influxdb_tables(
                session_ctx,
                &source.name,
                conn_str,
                source.options.as_ref(),
                source.is_read_write(),
            )
```

2. The `access_mode` field doc comment (~line 232) currently reads `/// "read" (default) or "read_write". Currently honored by the SQLite source.` — update to:

```rust
    /// "read" (default) or "read_write". Honored by sources with write
    /// support (SQLite, MySQL, Postgres, InfluxDB, ...).
```

- [ ] **Step 6: Update the executor comment**

In `crates/skardi/src/jobs/executor.rs` (~line 338), extend the comment above the non-transactional rejection arm:

```rust
            // Non-transactional SQL-ish backends — the underlying providers
            // don't wrap an INSERT in a transaction, so a mid-stream failure
            // would leave partial rows visible. Reject at submit time.
            // (InfluxDB writes ARE supported via SQL `INSERT INTO` on
            // read_write sources — that path accepts the non-atomic contract
            // explicitly; job destinations do not.)
```

- [ ] **Step 7: Verify the workspace**

```bash
cargo test -p skardi sources::providers::influxdb
cargo check --workspace --all-targets
```

Expected: provider tests PASS (including the two new ones); workspace check clean.

- [ ] **Step 8: Commit**

```bash
git add crates/skardi/src/sources/providers/influxdb/mod.rs crates/server/src/config.rs crates/cli/src/main.rs crates/skardi/src/jobs/executor.rs
git commit -m "feat(influxdb): allow access_mode read_write, wire write path into registration"
```

---

### Task 8: Integration and e2e tests (live InfluxDB)

`#[ignore]`-gated tests against a live InfluxDB 3. CI runs them automatically after seeding (see `.github/workflows/ci.yml` "Start and seed InfluxDB 3"). **Important:** write tests must NOT write to the seeded `cpu`/`mem` measurements — the read tests assert on their row counts. Each write test seeds its own uniquely-named measurement over HTTP (registration's eager schema fetch requires the measurement to exist).

**Files:**
- Modify: `crates/skardi/src/sources/providers/influxdb/mod.rs` (tests module)
- Modify: `crates/server/tests/influxdb_e2e.rs`
- Modify: `crates/server/Cargo.toml` (dev-dependency)

**Interfaces:**
- Consumes: everything from Tasks 3–7; the existing test helpers `influx_url()`, `influx_database()`, `opts()`, `total_rows()` in `mod.rs`.

- [ ] **Step 1: Add provider-level integration tests**

Append to the tests module in `mod.rs` (after the existing integration tests):

```rust
    /// Seed a uniquely-named measurement over the line-protocol HTTP API so
    /// the write tests (a) can register it — the eager schema fetch requires
    /// it to exist — and (b) never touch the shared `cpu`/`mem` fixtures the
    /// read tests assert on. Returns the measurement name.
    async fn seed_write_measurement(suffix: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos();
        let measurement = format!("wtest_{suffix}_{nanos}");
        let url = format!(
            "{}/api/v3/write_lp?db={}&precision=second",
            influx_url(),
            influx_database()
        );
        reqwest::Client::new()
            .post(&url)
            .body(format!(
                "{measurement},host=seed usage_user=1.0 1700000000"
            ))
            .send()
            .await
            .expect("seed measurement")
            .error_for_status()
            .expect("seed measurement status");
        measurement
    }

    async fn register_write_measurement(
        ctx: &mut SessionContext,
        name: &str,
        measurement: &str,
        read_write: bool,
    ) {
        let options = opts(&[
            ("database", influx_database().as_str()),
            ("measurement", measurement),
        ]);
        register_influxdb_tables(ctx, name, &influx_url(), Some(&options), read_write)
            .await
            .unwrap_or_else(|e| panic!("register {name} failed: {e}"));
    }

    #[tokio::test]
    #[ignore]
    async fn integration_insert_roundtrip_with_explicit_time() {
        let measurement = seed_write_measurement("roundtrip").await;
        let mut ctx = SessionContext::new();
        register_write_measurement(&mut ctx, "wtest", &measurement, true).await;

        let insert = ctx
            .sql(
                "INSERT INTO wtest (host, usage_user, time) VALUES \
                 ('h9', 42.5, TIMESTAMP '2024-01-01T00:00:00Z'), \
                 ('h9', 43.5, TIMESTAMP '2024-01-01T00:01:00Z')",
            )
            .await
            .expect("plan insert")
            .collect()
            .await
            .expect("run insert");
        // DataFusion reports the inserted row count.
        let count = insert[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .expect("count is UInt64")
            .value(0);
        assert_eq!(count, 2);

        let rows = ctx
            .sql("SELECT usage_user FROM wtest WHERE host = 'h9' ORDER BY time")
            .await
            .expect("plan select")
            .collect()
            .await
            .expect("collect select");
        assert_eq!(total_rows(&rows), 2, "inserted rows must read back");
    }

    #[tokio::test]
    #[ignore]
    async fn integration_insert_without_time_gets_server_timestamp() {
        let measurement = seed_write_measurement("notime").await;
        let mut ctx = SessionContext::new();
        register_write_measurement(&mut ctx, "wtest_nt", &measurement, true).await;

        ctx.sql("INSERT INTO wtest_nt (host, usage_user) VALUES ('h8', 7.5)")
            .await
            .expect("plan insert")
            .collect()
            .await
            .expect("run insert");

        let rows = ctx
            .sql("SELECT time FROM wtest_nt WHERE host = 'h8' AND time IS NOT NULL")
            .await
            .expect("plan select")
            .collect()
            .await
            .expect("collect select");
        assert_eq!(
            total_rows(&rows),
            1,
            "server must have assigned a timestamp"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn integration_read_only_source_rejects_insert() {
        let measurement = seed_write_measurement("readonly").await;
        let mut ctx = SessionContext::new();
        register_write_measurement(&mut ctx, "wtest_ro", &measurement, false).await;

        let result = ctx
            .sql("INSERT INTO wtest_ro (host, usage_user) VALUES ('h7', 1.0)")
            .await;
        let err = match result {
            // DataFusion surfaces the provider's insert error either at
            // planning or at execution, depending on version — accept both.
            Ok(df) => df.collect().await.expect_err("insert must fail"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("Insert into not implemented"),
            "got {err}"
        );
    }
```

- [ ] **Step 2: Run the integration tests against a local InfluxDB**

Start InfluxDB per `docs/influxdb/README.md` Quick Start (steps 1–2: docker run + create `metrics` database), then:

```bash
cargo test -p skardi sources::providers::influxdb -- --ignored
```

Expected: all integration tests PASS (the pre-existing read ones need the seeded `cpu`/`mem` data — if you skipped README step 3, only the three new write tests and `integration_register_measurement_and_scan`-style read tests may differ; the three new tests must PASS regardless because they self-seed).

- [ ] **Step 3: Add the server e2e test**

In `crates/server/Cargo.toml`, add to `[dev-dependencies]`:

```toml
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls"] }
```

Append to `crates/server/tests/influxdb_e2e.rs`:

```rust
/// Write round-trip through the server's config dispatch: a `read_write`
/// InfluxDB source must register a writable table.
#[tokio::test]
#[ignore]
async fn influxdb_read_write_source_accepts_insert_through_config_dispatch() {
    // Self-seeded measurement so the shared cpu/mem fixtures stay untouched.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    let measurement = format!("wtest_e2e_{nanos}");
    let url = format!(
        "{}/api/v3/write_lp?db={}&precision=second",
        influx_url(),
        influx_database()
    );
    reqwest::Client::new()
        .post(&url)
        .body(format!("{measurement},host=seed usage_user=1.0 1700000000"))
        .send()
        .await
        .expect("seed measurement")
        .error_for_status()
        .expect("seed measurement status");

    let mut source = influx_source("wtest_e2e", &measurement);
    source.access_mode = AccessMode::ReadWrite;

    let mut ctx = SessionContext::new();
    register_data_sources(&mut ctx, &[source])
        .await
        .expect("register read_write InfluxDB source");

    ctx.sql(
        "INSERT INTO wtest_e2e (host, usage_user, time) VALUES \
         ('e2e-host', 55.5, TIMESTAMP '2024-01-01T00:00:00Z')",
    )
    .await
    .expect("plan insert")
    .collect()
    .await
    .expect("run insert");

    let rows = ctx
        .sql("SELECT usage_user FROM wtest_e2e WHERE host = 'e2e-host'")
        .await
        .expect("plan select")
        .collect()
        .await
        .expect("collect select");
    let total: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1, "inserted row must read back through the server path");
}
```

Note: `influx_source` sets `measurement` in options and `AccessMode` is already imported in this file. If `influx_source`'s signature differs from what's shown at the top of the file, adapt to the existing helper rather than duplicating it.

- [ ] **Step 4: Run the e2e test**

```bash
cargo test -p skardi-server --test influxdb_e2e -- --ignored
```

Expected: PASS (both the pre-existing read e2e and the new write e2e).

- [ ] **Step 5: Commit**

```bash
git add crates/skardi/src/sources/providers/influxdb/mod.rs crates/server/tests/influxdb_e2e.rs crates/server/Cargo.toml
git commit -m "test(influxdb): add live write round-trip integration and e2e tests"
```

---

### Task 9: Documentation

**Files:**
- Modify: `docs/influxdb/README.md`
- Modify: `crates/skardi/src/sources/providers/influxdb/mod.rs` (module doc header)
- Check: root `README.md` (only if it claims InfluxDB is read-only)

- [ ] **Step 1: Update the provider module doc header**

Replace the last paragraph of the module doc in `mod.rs` (currently: "Access is **read-only**: Flight SQL serves `SELECT`s only. Writes to InfluxDB go through the line-protocol ingest API, which is out of scope for a SQL query engine, so InfluxDB sources never participate in CRUD or job destinations.") with:

```rust
//! Reads are served by Flight SQL (`SELECT`s only). With
//! `access_mode: read_write`, `INSERT INTO` is also supported — translated to
//! the line-protocol ingest API by the [`write`] module. UPDATE/DELETE are
//! not (InfluxDB 3 has no row-level update/delete), and InfluxDB sources do
//! not participate in job destinations (the line-protocol path is not
//! transactional across batches).
```

- [ ] **Step 2: Update `docs/influxdb/README.md`**

1. Replace the read-only callout (the `> **Access is read-only.** ...` blockquote near the top) with:

```markdown
> **Reads via Flight SQL; writes via line protocol.** `SELECT`s are served by
> the Flight SQL endpoint. With `access_mode: read_write`, `INSERT INTO` is
> supported too — Skardi translates it to the line-protocol ingest API.
> UPDATE/DELETE are not supported (InfluxDB has no row-level update/delete),
> and InfluxDB cannot be a job destination.
```

2. Add a new `## Writing Data` section (after the Data Model section):

```markdown
## Writing Data

Declare the source with `access_mode: read_write` to enable `INSERT INTO`:

​```yaml
data_sources:
  - name: cpu
    source_type: influxdb
    connection_string: http://localhost:8181
    access_mode: read_write
    options:
      database: metrics
      measurement: cpu
      token_env: INFLUXDB_TOKEN     # if auth is enabled
​```

​```sql
INSERT INTO cpu (host, region, usage_user, time)
VALUES ('host9', 'us-west', 42.5, TIMESTAMP '2024-01-01T00:00:00Z');
​```

How columns map to line protocol:

| Column | Line protocol role |
|---|---|
| `time` (Timestamp) | timestamp (nanosecond precision); omit or insert NULL → server-assigned |
| Dictionary-encoded string columns | tags (override with `options.tags: host,region` if needed) |
| everything else | fields; NULL fields/tags are omitted from the line |

Rules and caveats:

- **Existing measurements only.** Registration fetches the schema eagerly, so
  the measurement must already exist; writes cannot create one.
- **`measurement`-defined sources only.** A `query`-defined source has no
  insert target; `read_write` + `query` is rejected at startup.
- **Append-only.** `UPDATE` / `DELETE` return errors. Overwrite semantics
  don't exist in line protocol (a write with identical measurement + tags +
  timestamp replaces the fields — that's InfluxDB behavior, not Skardi's).
- **Not atomic across batches.** Each Arrow batch is one `write_lp` POST; a
  failure mid-`INSERT INTO ... SELECT` can leave earlier batches durable.
- **Every row needs ≥ 1 non-NULL field**, or the INSERT fails.
- `options.write_endpoint` overrides the HTTP base URL when it differs from
  the Flight `connection_string`.
​```
```

(Strip the zero-width markers `​` around the inner code fences — they're here only to nest fences in this plan document.)

3. Search `docs/influxdb/README.md` for remaining stale claims and fix them:

```bash
grep -n -i "read-only\|read_write" docs/influxdb/README.md
```

Any line still saying `access_mode: read_write` is rejected for InfluxDB must be updated.

- [ ] **Step 3: Check the root README**

```bash
grep -n -i "influx" README.md
```

If the supported-sources table or feature list marks InfluxDB read-only, update it to note SQL INSERT write support. If it doesn't mention access modes, no change.

- [ ] **Step 4: Final verification**

```bash
cargo fmt --all
cargo clippy --workspace --all-targets
cargo test -p skardi sources::providers::influxdb
cargo check --workspace --all-targets
```

Expected: fmt makes no changes (or commit what it fixes), clippy clean, tests PASS.

- [ ] **Step 5: Commit and push**

```bash
git add docs/influxdb/README.md README.md crates/skardi/src/sources/providers/influxdb/mod.rs
git commit -m "docs(influxdb): document SQL INSERT write support"
git push
```

PR #147 (`support_write_to_influxdb`) tracks this work — after pushing, update its checklist (implementation / tests / docs) via `gh pr edit 147 --body ...` or leave for the reviewer.
