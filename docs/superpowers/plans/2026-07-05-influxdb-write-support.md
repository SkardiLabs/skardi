# InfluxDB Write Support Implementation Plan

**Goal:** Support `INSERT INTO <influxdb_table>` through the query engine, writing to InfluxDB 3 via its HTTP line-protocol API, gated by `access_mode: read_write`.

**Spec:** `docs/superpowers/specs/2026-07-05-influxdb-write-design.md` (approved — read it first; it holds the full semantics: tag/field/time classification, null handling, atomicity contract, guardrails).

**Approach:** New `WritableInfluxTable` wrapper implements `TableProvider::insert_into`; its `InfluxInsertExec` streams input batches, serializes each with the `influxdb-line-protocol` crate, and POSTs to `<base>/api/v3/write_lp` via `reqwest`. One POST per batch (memory-bounded, non-atomic across batches — same contract as Mongo SQL DML).

**Conventions:** follow `AGENTS.md` (no `.unwrap()` in production code, imports at top, `tracing`, tests required). TDD per task; commit per task.

**Verified upstream facts** (so nobody re-derives them):
- `influxdb-line-protocol` 2.0.0: typestate builder — tags must precede fields, first `field()` changes the builder type; `close_line()` appends `\n`; ints render `7i`, uints `9u`, strings double-quoted; measurement/tags escape `,`, `=`, space.
- `FlightTable` reports `TableType::View` → DataFusion's default UPDATE/DELETE error says "not supported for View table"; we override with explicit messages.
- InfluxDB 3 Core serves Flight and HTTP on the same port, so `connection_string` doubles as the default write base URL.

---

## Task 1: Module restructure + deps

- `git mv` `crates/skardi/src/sources/providers/influxdb.rs` → `influxdb/mod.rs` (matches `mongo/` layout; module path unchanged).
- `crates/skardi/Cargo.toml`: make `reqwest` non-optional (feature `remote-embed` becomes `[]`); add `influxdb-line-protocol = "2.0.0"`.
- Verify: `cargo check -p skardi` + existing influxdb unit tests pass.
- Commit: `refactor(influxdb): move provider to directory module, add write-path deps`

## Task 2: Extract `resolve_token`

- Pull the token-resolution block (token_env/token precedence, empty-value errors, inline-token warning) out of `build_flight_options` into `fn resolve_token(name, options) -> Result<Option<String>>` so the write path can reuse it.
- Behavior-preserving; the 6 existing token tests pass unchanged.
- Commit: `refactor(influxdb): extract resolve_token for reuse by the write path`

## Task 3: `InfluxWriteConfig` (new `write.rs`)

- `mod.rs`: add consts `OPT_WRITE_ENDPOINT = "write_endpoint"`, `OPT_TAGS = "tags"`; declare `mod write;`.
- `write.rs`: `pub(super) struct InfluxWriteConfig { measurement, database, write_url, token: Option<String>, tags_override: Option<Vec<String>> }` with `from_options(name, connection_string, options) -> Result<Self>`:
  - `measurement`/`table` required (write path has no query target); `database` required non-empty (sent as `db` query param — the `flight.sql.*` passthrough doesn't apply to writes);
  - `write_url` = (`write_endpoint` option, else `connection_string`, trailing `/` trimmed) + `/api/v3/write_lp`;
  - `tags` option parsed as trimmed comma list, empty list → error; token via `resolve_token`.
- Tests (~9): happy path, `table` alias, missing/empty measurement/database, endpoint override + slash trim, tags parse/trim, empty tags error, token_env resolution.
- Commit: `feat(influxdb): add write-path option parsing (InfluxWriteConfig)`

## Task 4: Column classification + value extraction (`write.rs`)

- `pub(super) struct LineProtocolSchema { time_idx: Option<usize>, tag_idxs: Vec<usize>, field_idxs: Vec<usize> }`
- `classify_columns(schema, tags_override) -> Result<LineProtocolSchema>`:
  - Timestamp-typed column named `time` → timestamp; `Dictionary(Int32, Utf8)` → tag (that's how InfluxDB 3 encodes tags over Flight), or exactly the `tags`-override names; rest → fields.
  - Plan-time validation: override names must exist; tag columns must be string-typed; field types restricted to Float16–64 / Int8–64 / UInt8–64 / Utf8 / LargeUtf8 / Boolean; ≥1 field column required.
- `enum LpFieldValue<'a> { Float(f64), Int(i64), UInt(u64), Bool(bool), Str(&'a str) }` implementing the crate's `FieldValue` (delegates wire formatting to the crate's impls).
- Extractors returning `None` for SQL NULL: `field_value`, `tag_value` (plain + dictionary strings), `timestamp_nanos` (all four `TimeUnit`s → nanos with `checked_mul` overflow guard).
- Tests (~9): inference, override incl. plain-Utf8 tags, missing-override-column, no-fields error, unsupported-type error, Utf8 `time` column stays a field, null handling in all three extractors, ms→ns conversion.
- Commit: `feat(influxdb): classify Arrow columns into line-protocol roles`

## Task 5: Batch → line protocol (`write.rs`)

- `batch_to_line_protocol(measurement, batch, lp_schema) -> Result<Vec<u8>>`: per row — collect non-null fields **first** (a zero-field line is invalid and the typestate builder can't back out), error naming the row if none; emit tags (nulls omitted), fields, then timestamp if the `time` value is non-null (else no timestamp → server-assigned).
- Tests (~6) with exact-string assertions: tags+fields+timestamp lines, null tag/time omission, null field omission with surviving row, all-null-fields error, type suffixes + string quoting, measurement/tag-value escaping.
- Commit: `feat(influxdb): serialize Arrow batches to line protocol`

## Task 6: `WritableInfluxTable` + `InfluxInsertExec` (`write.rs`)

- `WritableInfluxTable { inner: Arc<dyn TableProvider>, config: Arc<InfluxWriteConfig>, client: reqwest::Client }` — `inner` is `Arc<dyn TableProvider>` (not the concrete Flight type) so unit tests can use a `MemTable`. Forwards schema/scan/statistics/pushdown to `inner`.
  - `insert_into`: reject non-`Append` `InsertOp`; `classify_columns` at plan time (errors name the measurement); return `InfluxInsertExec`.
  - `delete_from` / `update`: explicit `NotImplemented` errors — "InfluxDB does not support DELETE/UPDATE — the line-protocol write path is append-only (measurement '…')".
- `InfluxInsertExec` mirrors `MongoInsertExec` (`mongo/mod.rs:787-917`): forwards `properties()` from input; `execute` unfolds the input stream — serialize batch → POST (`db` + `precision=nanosecond` params, optional bearer auth) → emit single-row `count` batch. Non-2xx surfaces status + response body (InfluxDB's per-line diagnostics).
- Tests (~5, offline via MemTable/EmptyExec): DELETE/UPDATE messages, Overwrite rejection, Append returns `InfluxInsertExec`, classification failure surfaces as Plan error naming the measurement.
- Commit: `feat(influxdb): add WritableInfluxTable and InfluxInsertExec`

## Task 7: Registration wiring

- `register_influxdb_tables(...)` gains `read_write: bool` (5th param). When true: `query` option present → error ("cannot combine … no insert target"); build `InfluxWriteConfig` **before** dialing so bad config fails startup as a config error; wrap the read table in `WritableInfluxTable`.
- Call sites: `crates/server/src/config.rs` (~1293) and `crates/cli/src/main.rs` (~913) pass their source's read-write flag; existing mod.rs tests pass `false`.
- `crates/server/src/config.rs`: add `Influxdb` to `WRITABLE_SOURCE_TYPES` (~698); add 'influxdb' to the `UnsupportedWriteMode` message (~212).
- `crates/cli/src/main.rs` (~232): update the stale access_mode doc comment.
- `crates/skardi/src/jobs/executor.rs` (~338): comment-only note that SQL INSERT is the supported write path; job-destination rejection unchanged.
- Tests: 2 new offline tests (read_write+query rejected; read_write without measurement rejected — both before any network call). Verify `cargo check --workspace --all-targets`.
- Commit: `feat(influxdb): allow access_mode read_write, wire write path into registration`

## Task 8: Live integration + e2e tests (`#[ignore]`, CI runs them)

- **Isolation rule:** write tests must NOT touch the CI-seeded `cpu`/`mem` measurements (read tests assert their counts). Each test seeds its own uniquely-named measurement (`wtest_<suffix>_<nanos>`) over the HTTP API first — registration's eager schema fetch requires it to exist anyway.
- Provider tests in `mod.rs`: insert 2 rows with explicit time → count=2 + read back; insert without `time` → server-assigned timestamp readable; read-only registration rejects INSERT ("Insert into not implemented", surfaced at plan or exec time).
- Server e2e in `crates/server/tests/influxdb_e2e.rs`: `AccessMode::ReadWrite` source through `register_data_sources` → INSERT → read back. Add `reqwest` to server `[dev-dependencies]` for seeding.
- Verify locally against a docker InfluxDB (README Quick Start steps 1–2): `cargo test -p skardi sources::providers::influxdb -- --ignored` and `cargo test -p skardi-server --test influxdb_e2e -- --ignored`.
- Commit: `test(influxdb): add live write round-trip integration and e2e tests`

## Task 9: Docs + final pass

- `mod.rs` module header: replace the "Access is read-only … never participate in CRUD" paragraph with the read/write split.
- `docs/influxdb/README.md`: replace the read-only callout; add a **Writing Data** section — read_write YAML example, INSERT example, column→line-protocol mapping table, caveats (existing measurements only, measurement-defined sources only, append-only, non-atomic across batches, ≥1 non-null field per row, `write_endpoint` override). Grep for remaining "read-only" claims.
- Root `README.md`: update only if it marks InfluxDB read-only.
- Final: `cargo fmt --all`, `cargo clippy --workspace --all-targets`, full provider tests, workspace check.
- Commit: `docs(influxdb): document SQL INSERT write support`; push and update PR #147 checklist.
