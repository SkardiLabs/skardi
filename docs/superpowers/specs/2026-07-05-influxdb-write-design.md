# InfluxDB Write Support — Design

**Date:** 2026-07-05
**Status:** Approved
**Scope:** SQL `INSERT INTO` support for InfluxDB 3 sources. Job destinations remain out of scope.

## Problem

The InfluxDB 3 provider (`crates/skardi/src/sources/providers/influxdb.rs`) is read-only: it
queries via Arrow Flight SQL, which serves `SELECT`s only. Users cannot write rows back to
InfluxDB through Skardi. InfluxDB 3 accepts writes only through its HTTP line-protocol
endpoint (`POST /api/v3/write_lp`), so write support requires a second wire path alongside
the existing Flight read path.

## Decisions

| Question | Decision |
|---|---|
| Write surface | SQL `INSERT INTO` via the query engine only. Job destinations stay rejected. |
| Target measurements | Existing measurements only — no auto-create; eager schema fetch at registration is unchanged. |
| Implementation | `TableProvider::insert_into` wrapper; serialize with the `influxdb-line-protocol` crate; send with `reqwest`. |

## Architecture

### Module layout

`providers/influxdb.rs` becomes `providers/influxdb/` (following the `mongo/` layout):

- `mod.rs` — option parsing, registration, the existing `CountSafeFlightTable` read wrapper.
- `write.rs` — `WritableInfluxTable`, `InfluxInsertExec`, and batch→line-protocol serialization.

### Components

- **`WritableInfluxTable`** — a `TableProvider` composing over `CountSafeFlightTable`.
  Scans delegate to the inner table (reads still go through Flight SQL). Adds
  `insert_into()`, used only when the source is registered with `access_mode: read_write`.
- **`InfluxInsertExec`** — an `ExecutionPlan` that consumes the input stream batch-by-batch:
  serialize batch → one `POST <base>/api/v3/write_lp?db=<database>&precision=nanosecond`
  (where `<base>` is the `write_endpoint` option, defaulting to `connection_string`)
  → next batch. Reuses the source's bearer token (`token_env` / `token`) as the
  `Authorization: Bearer` header. Returns the standard single-row `count` output (rows
  written), matching `MongoInsertExec`.

InfluxDB 3 serves Flight and the HTTP API on the same port, so the existing
`connection_string` is the default HTTP base URL.

### Config surface

```yaml
- name: cpu
  source_type: influxdb
  connection_string: http://localhost:8181
  access_mode: read_write        # new — previously rejected for influxdb
  options:
    database: metrics
    measurement: cpu
    token_env: INFLUXDB_TOKEN
    # optional:
    # write_endpoint: https://other-host:8181   # HTTP base URL override; defaults to connection_string
    # tags: host,region                         # tag-column override if schema inference misfires
```

- `Influxdb` joins `WRITABLE_SOURCE_TYPES` in `crates/server/src/config.rs`; the
  `UnsupportedWriteMode` error message text is updated accordingly.
- `register_influxdb_tables()` gains a `read_write: bool` parameter (same shape as the
  MySQL/SQLite registration functions). Both call sites — server `config.rs` and CLI
  `main.rs` — pass `source.access_mode.is_read_write()`.
- **Constraint:** `read_write` requires the source be defined by `measurement`/`table`.
  A `query`-defined source has no insert target; `read_write` + `query` is a
  registration-time config error.

### Dependencies

- `influxdb-line-protocol` (InfluxData's own serializer, the IOx implementation) — new.
- `reqwest` — already in the tree as an optional dep of the `skardi` crate; becomes a
  regular dependency for the provider's write path.

## Write-path semantics

### Tag / field / time classification

Derived from the Arrow schema fetched at registration:

- Column named `time` with a Timestamp type → the line's timestamp.
- `Dictionary(Int32, Utf8)` columns → tags (how InfluxDB 3 encodes tags over Flight —
  verify during implementation; the `tags` option is the config override if inference
  misfires).
- Everything else → fields.

### Type mapping (Arrow → line-protocol field values)

| Arrow type | Line protocol |
|---|---|
| `Float16/32/64` | float |
| `Int8/16/32/64` | integer (`i` suffix) |
| `UInt8/16/32/64` | unsigned (`u` suffix) |
| `Utf8` / `LargeUtf8` | quoted string |
| `Boolean` | bool |
| anything else (as a field) | planning-time error, not a per-row failure |

### Null handling

Line protocol has no null: a null tag or null field is omitted from that line. DataFusion
null-fills unspecified `INSERT` columns, so `INSERT INTO cpu (host, usage_user) VALUES (...)`
naturally writes only the named columns. Hard rules:

- A row where **all fields** are null is an execution error (a line must carry ≥ 1 field).
- A null `time` → the line is sent without a timestamp; the server assigns arrival time.
  Otherwise timestamps are sent at nanosecond precision.

### Atomicity

Batch-per-POST is **not atomic across batches**: if batch 3 of 5 fails, batches 1–2 are
already durable. This matches the contract Mongo's SQL DML already has in this codebase and
is why job destinations stay out of scope. Memory stays bounded for
`INSERT INTO ... SELECT` from large sources.

### Errors

- Non-2xx from `write_lp` → execution error including InfluxDB's response body (it carries
  per-line diagnostics).
- `InsertOp::Overwrite` is rejected — appends only (the only mode DataFusion generates for
  `INSERT INTO`).

## Guardrails (what stays blocked)

- `UPDATE` / `DELETE` remain unimplemented — DataFusion's default `TableProvider` error
  covers it; InfluxDB 3 has no row-level update/delete.
- **Job destinations stay rejected.** The executor's `NonTransactionalDestination`
  rejection for InfluxDB is untouched; its comment gains a pointer noting SQL INSERT is the
  supported write path.
- Sources without `access_mode: read_write` register exactly as today (read-only
  `CountSafeFlightTable`); the sql_validator's existing per-table gating rejects INSERTs
  against them.
- `read_write` + `query`-defined source → registration-time error.

## Testing

Follows the provider's existing three-tier pattern:

- **Unit:** option validation (`read_write`+`query` rejection, `write_endpoint` handling,
  `tags` override); batch→line-protocol serialization — type mapping, null omission,
  all-null-fields error, tag/field/time classification from a synthetic Dictionary schema,
  escaping-sensitive names (spaces, commas, quotes).
- **Integration (`#[ignore]`, live InfluxDB 3 in CI):** register read-write,
  `INSERT INTO ... VALUES`, read back via Flight and assert rows + `count(*)` delta;
  INSERT omitting `time` gets a server-assigned timestamp; INSERT into a read-only source
  is rejected by the validator.
- **E2E:** extend `crates/server/tests/influxdb_e2e.rs` with a write round-trip through
  the server API.

## Documentation

- `docs/influxdb/README.md`: remove the "access is read-only" claims; add a *Writing data*
  section (config example, tags/fields/time mapping, batch-level non-atomicity caveat).
- Module doc header in the provider: same update.
