# Add a `purpose` field to `/query`; make raw-SQL logging an operator config

**Date:** 2026-07-24
**Status:** Proposed
**Branch:** `feat/query-parameterization-design`

## Goal

Two small changes to the existing `POST /query` endpoint:

1. Add an optional **`purpose`** field so callers (typically agents) can document
   why a query is run, captured alongside the query for context over time.
2. Make **raw-SQL logging an opt-in operator config** that writes to a local log
   file. When disabled (the default), the raw SQL is not emitted into the general
   log/trace stream.

Parameterization and a second endpoint are **dropped** — see "Rejected
alternatives". The endpoint keeps taking final SQL.

## Motivation

Today `/query` logs the full statement at INFO
(`crates/server/src/query_handlers.rs:108-113`, `sql = %request.sql`). Because it
takes final SQL with literal values inlined and `sql` is a `tracing` field, that
line is a **values log** that fans out to any OTLP collector and log sink — any
secret/PII passed as a literal is exposed to everyone who can read traces/logs.

This is an open-source deployment. The right posture is not to engineer value
stripping into the request shape, but to **let the operator decide** whether and
where raw queries are recorded, and to make them responsible for storing that
sink safely. So:

- By default, raw SQL is **not** logged into the general stream.
- An operator who wants an audit trail opts in via config; raw SQL (plus
  `purpose`) is written to a **local log file** they control and secure.

The `purpose` field is useful regardless of that toggle: it records caller intent
so the query log (when enabled) carries the *why*, not just the *what*.

Note: the query text is never sent to any embedding/LLM/external service — this
concerns observability sinks only.

## API Contract

`POST /query` — request gains one optional field; response unchanged.

```json
{
  "sql": "SELECT * FROM products WHERE price > 10",
  "max_rows": 500,
  "purpose": "Populate the weekly pricing review dashboard"
}
```

- `sql` (string, required) — unchanged; final SQL, one statement.
- `max_rows` (positive integer, optional, default 1000) — unchanged.
- `purpose` (string, optional, capped ~2000 chars) — caller intent. Recorded with
  the query when raw-SQL logging is enabled; never executed. Over-cap → 400
  `parameter_validation_error`.

## Raw-SQL logging config

Add an operator setting (server config, off by default), e.g.:

```yaml
query_log:
  raw_sql_file: /var/log/skardi/queries.log   # unset/empty ⇒ raw SQL not logged
```

Behavior:

- **Unset (default):** the audit line at `:108-113` drops `sql = %request.sql`
  and keeps only a value-free marker (`kind`, `max_rows`, `purpose`, timing). The
  DEBUG `request.sql` lines (`:75`, `:144`) are also gated off. No raw SQL reaches
  logs/traces.
- **Set:** each executed statement is appended to that file as a line carrying
  the raw `sql`, `purpose`, `max_rows`, and timestamp. This file is the
  operator's responsibility to secure, rotate, and retain — the docs state so
  explicitly.

Keeping the raw-SQL sink as a dedicated local file (rather than the OTLP/tracing
pipeline) means enabling an audit trail does not push query text to external
collectors by accident.

## Components

- `crates/server/src/query_handlers.rs` — add `purpose` to `QueryRequest`;
  validate its length; thread it into the execution marker / file log. Gate the
  raw-SQL logging on the new config (drop `sql = %request.sql` from the INFO line
  and the DEBUG SQL lines when disabled).
- Server config (`crates/server/src/config.rs` / config struct) — add the
  `query_log.raw_sql_file` (or equivalent) option and plumb it into `AppState`.
- A small append-to-file writer for the raw-SQL sink (only constructed when the
  path is set).
- Docs: `docs/server.md` — document the `purpose` field, the config, and that
  securing the query-log file is the operator's responsibility.

## Testing

- `purpose` present → appears in the execution marker / query-log line; over-cap
  `purpose` → 400.
- Config unset → captured logs contain **no** raw SQL text on any path.
- Config set to a temp file → the file receives a line with the raw `sql` and
  `purpose` after a request; general logs still omit raw SQL.
- Existing `/query` behavior (no `purpose`, no config) unchanged — backward
  compatible.

## Rejected alternatives

- **Parameterized `{name}` template + `params` channel** — closes injection and
  keeps values out of logs by construction, but adds a substitution layer and
  changes the request contract. Overkill here: an OSS operator can take
  responsibility for a local query-log file instead, and callers already send
  final SQL.
- **A separate `/parameterized_query` endpoint for agents** — more surface area
  than this problem warrants once raw-SQL logging is operator-controlled.
- **Stripping/redacting values from the request** — brittle and unnecessary given
  the config-gated file sink.

## Out of Scope

- Query parameterization / a `params` channel.
- Persisting queries or `purpose` to a database/store (a local file sink only).
- Routing raw SQL to OTLP/tracing when the file sink is enabled.
