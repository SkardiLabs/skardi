# Durable `/query` audit store + enforced plan-value log suppression

**Date:** 2026-07-31
**Status:** Proposed
**Branch:** `feat/query-parameterization-design`
**Supersedes:** the JSONL file sink in the 2026-07-24 spec; extends the
2026-07-28 `ai_context` spec (whose "Out of Scope: persisting to a
database/store" line no longer holds).

## Goal

Make the two guarantees the earlier specs *claimed* actually hold:

1. **Confidentiality** — no literal value from a `/query` statement reaches the
   log/trace/OTLP stream.
2. **Auditability** — when an operator turns auditing on, the record is
   durable, complete, queryable, and trustworthy.

## Motivation

Review of the implementation found both guarantees unmet.

**Confidentiality.** Removing the handler's `sql` tracing field is necessary
but not sufficient. The server installs DataFusion analyzer/optimizer/physical
instrumentation, and DataFusion's own `log_plan` helper prints plans at DEBUG.
Plans carry literals. With DEBUG tracing on, `SELECT 'TOP_SECRET' AS secret`
emitted `Projection: Utf8("TOP_SECRET")` and `ProjectionExec: expr=[TOP_SECRET
…]` repeatedly — the exact values the design exists to protect, exported to
every collector.

**Auditability.** The JSONL sink was best-effort in ways that made it unfit as
an audit trail:

- `std::sync::Mutex<File>` + synchronous `writeln!` from an async handler —
  filesystem I/O on Tokio worker threads, all requests serialized behind one
  mutex; a slow or network-mounted path stalls the runtime.
- `OpenOptions::create` honours the umask, so a file full of raw SQL is
  typically `0644` — readable by any local user.
- Write failures were swallowed, so an audited server could execute
  unrecorded statements.
- A failure to open the sink at startup downgraded to a warning and the server
  ran on with auditing silently off.
- No outcome, no record id, no session index, no retention story — the file
  said what was *attempted*, never what *happened*.

A related deserialization gap: `ai_context: Option<Value>` collapses an
explicit `"ai_context": null` into `None`, so a null bypassed the documented
validation and returned 200.

## Design

### 1. Plan-value logging floor (`crates/server/src/logging.rs`)

`build_env_filter(rust_log, allow_plan_value_logging)` is the single choke
point, applied to the whole subscriber registry (fmt layer *and* OTLP layer).

- `PLAN_VALUE_TARGET_PREFIXES = ["datafusion", "skardi_query_plan",
  "sqlparser"]`. Prefix matching mirrors `EnvFilter`'s own, so `datafusion`
  covers `datafusion_optimizer`, `datafusion_sql`, `datafusion_federation`,
  `datafusion_tracing`, …
- Any `RUST_LOG` directive that would put one of those targets at DEBUG/TRACE
  is **dropped before parsing** — a targeted `datafusion_optimizer=debug`
  cannot out-specify the floor because it never reaches the filter.
- Each prefix then gets an `=info` directive, unless the operator already set
  that exact target to something at or above the floor (so `datafusion=off`
  stays `off` — the floor raises a minimum, it does not clamp).
- `SKARDI_ALLOW_PLAN_VALUE_LOGGING=1` lifts the whole mechanism.

The datafusion-tracing instrumentations are pinned to an explicit target
(`skardi_query_plan`) rather than the macros' default `module_path!()`, which
would bury them inside `skardi_server::server` where no filter could name them.

Because this is filter-level, it protects *every* value-bearing plan record,
including ones added by future DataFusion versions — as opposed to redacting
literals at each known emission site, which is a list that silently goes stale.

### 2. Durable audit store (`crates/server/src/query_audit.rs`)

SQLite via `tokio_rusqlite` (the same backend and crate as the jobs ledger).

```sql
CREATE TABLE query_audit (
    id TEXT PRIMARY KEY, created_at TEXT NOT NULL, finished_at TEXT,
    sql TEXT NOT NULL, ai_context TEXT, session_id TEXT,
    max_rows INTEGER NOT NULL, statement_kind TEXT NOT NULL,
    status TEXT NOT NULL, row_count INTEGER, error TEXT);
CREATE INDEX idx_query_audit_session_created ON query_audit (session_id, created_at DESC);
CREATE INDEX idx_query_audit_created         ON query_audit (created_at DESC);
CREATE INDEX idx_query_audit_status          ON query_audit (status);
```

- **Async** — `tokio_rusqlite` owns a dedicated blocking thread; no filesystem
  I/O on a Tokio worker and no mutex held across a write.
- **Durable before execution** — `journal_mode=WAL`, `synchronous=FULL`;
  `record_started` commits and returns the id before the engine is called.
- **Private** — the file is created `0600` (before SQLite touches it, so the
  umask never applies) and the WAL sidecars inherit it.
- **Status lifecycle** — `started` → `succeeded`/`failed`; startup rewrites
  leftover `started` rows to `unknown`, mirroring `reconcile_orphaned` in the
  jobs ledger.
- **Retention** — `--query-audit-retention-days <n>` prunes at startup
  (awaited, so misconfiguration surfaces immediately) and hourly after.

Failure semantics:

| when | behavior |
| --- | --- |
| open/migrate at startup | fatal — the server exits rather than run unaudited |
| pre-execution write | `503 query_audit_error`; the statement does not run |
| post-execution update | logged; row stays `started`, reconciled next startup |

CLI: `--query-log <path>` is replaced by `--query-audit-db <path>` and
`--query-audit-retention-days <n>`.

### 3. `ai_context` presence

`#[serde(default, deserialize_with = "deserialize_present")]` maps a present
field — including `null` — to `Some`, leaving `None` to mean *absent*. An
explicit null then fails the existing "must be a JSON object" check.

## Testing

- `tests/query_plan_logging.rs` — runs a real query through the server's own
  instrumented `SessionState`, captures everything the subscriber formats, and
  asserts the sentinel literal is absent under `RUST_LOG=debug`, `trace`, and
  explicitly targeted `datafusion=trace,datafusion_optimizer=debug,
  skardi_query_plan=trace`. A positive control asserts the sentinel *is*
  present with the opt-in enabled, so the suite cannot pass by capturing
  nothing.
- `query_audit.rs` unit tests — record/outcome round trip, failure detail,
  session-ordered lookup, orphan reconciliation, retention pruning, `0600`
  permissions on disk, durability across reopen.
- `tests/query_http.rs` — audit record on success (raw SQL, `ai_context`,
  `session_id`, kind, `succeeded`, row count) and on engine failure; rejected
  statements leave no record; a broken store yields `503` with no results;
  `ai_context: null` → `400`; nothing persisted when auditing is off.

## Rejected alternatives

- **Redact literals at each emission site** — would mean patching or wrapping
  DataFusion's plan `Display`; the set of emitters is upstream-controlled and
  grows silently. Filtering by target is one enforceable rule.
- **Keep JSONL, just add `O_CREAT` mode + a writer task** — fixes leakage and
  blocking but still leaves no outcome, no id, no index, no transaction, and
  no retention. The reviewer asked for a store, not a better file.
- **Postgres/RocksDB backing** — SQLite is already a dependency and already
  the pattern for the jobs ledger; a second embedded engine buys nothing here.
- **Make the audit write best-effort** — an audit trail that can silently miss
  entries answers no compliance question.

## Out of Scope

- Query parameterization / a `params` channel (still out).
- An HTTP surface for reading the ledger — operators query the SQLite file.
