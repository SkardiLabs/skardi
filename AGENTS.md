# AGENTS.md

This file gives AI agents and human contributors the minimum context needed to make safe, useful changes to Skardi. Read this before opening a PR.

Skardi is a federated SQL query engine and YAML-driven REST API server, written in Rust on top of Apache DataFusion. The CLI runs SQL against local files, object stores, databases, and lakehouse formats; the server turns SQL pipelines defined in YAML into parameterized HTTP endpoints — zero application code required.

Project-specific Claude Code instructions also live in [.claude/CLAUDE.md](.claude/CLAUDE.md). When the two files disagree, CLAUDE.md wins.

## Repository Layout

Cargo workspace with three crates:

- [crates/skardi/](crates/skardi/) — the core library: query engine, pipelines, model inference, and all data source providers. Everything reusable lives here.
  - `engine/` — DataFusion engine wrapper.
  - `pipeline/` — YAML pipeline parsing, parameter inference, schema inference.
  - `model/` — embedding/ONNX model registries (`candle`, `gguf`, `onnx`).
  - `sources/providers/` — one subdirectory per backend: `lance/`, `iceberg.rs`, `mongo/`, `redis/`, `sqlite/`, `sqlx/pg/`, `mysql.rs`, etc.
- [crates/server/](crates/server/) — the `skardi-server` binary: Axum HTTP server, auth (better-auth), config loading, metrics, telemetry. Wires the core library into a long-running service.
- [crates/cli/](crates/cli/) — the `skardi` CLI binary. Thin wrapper around the core library for one-shot SQL execution.

Other top-level directories: [docs/](docs/) (per-source guides + `server.md`), [demo/](demo/) (end-to-end example apps), [asset/](asset/) (logo, badges, architecture diagram), [data/](data/) (sample data for tests/demos).

## Build, Test, Lint

```bash
# Check the entire workspace (fast feedback)
cargo check --workspace --all-targets

# Build
cargo build --release -p skardi-cli
cargo build --release -p skardi-server

# Full RAG kit: embedding UDFs (ONNX, GGUF, Candle, remote embed) + chunk UDF
cargo build --release -p skardi-server --features rag

# Tests
cargo test --workspace
cargo test -p skardi <test_name>

# Format and lint (also enforced by pre-commit hooks)
cargo fmt --all
cargo clippy --workspace --all-targets
```

Pre-commit hooks (`.pre-commit-config.yaml`) run `cargo fmt` and `cargo check --all` on every commit. Don't bypass them with `--no-verify` — fix the underlying issue.

## Coding Standards

### Imports

- Always import types at the top of the file with `use` statements. **Never use full crate paths inline in function bodies.** This is enforced by code review.

```rust
// GOOD
use std::sync::Arc;
use datafusion::prelude::SessionContext;

fn make_ctx() -> Arc<SessionContext> { Arc::new(SessionContext::new()) }

// BAD
fn make_ctx() -> std::sync::Arc<datafusion::prelude::SessionContext> {
    std::sync::Arc::new(datafusion::prelude::SessionContext::new())
}
```

### Error Handling — No Panics in Production Code

**No raw `.unwrap()` is allowed anywhere outside `crates/cli/` and test code** (`#[cfg(test)]` modules, `#[test]` functions, files under `tests/`, doc-comment examples). Pick the strategy that matches the failure mode:

1. **Recoverable errors** → propagate via `Result` with `?`, `ok_or_else`, `with_context`, or a custom error variant. The library uses `anyhow::Result` for application code and `thiserror`-style enums (e.g. `ConfigError`, `DataFusionError::Internal(...)`) at module boundaries. Add a new error variant rather than stuffing context into a generic string.
2. **Lock poisoning** on `std::sync::RwLock`/`Mutex` → recover with `.unwrap_or_else(|p| p.into_inner())` so a poisoned lock degrades gracefully instead of panicking. Skardi uses this pattern in the optimizer registry, model caches, and Lance dataset registry.
3. **True invariants** that cannot fail at runtime → use `.expect("why this cannot fail")` with a message documenting the invariant. Examples: `"len == 1 checked above"`, `"DataType::List guarantees ListArray"`. `expect` still panics, so reserve it for cases the type system or a prior check has already ruled out.

`.unwrap()` is fine in CLI code, tests, and doc examples. Production library and server code should have none.

### Logging

- Use `tracing::{info, warn, error, debug}` — never the `log` crate, never `println!` / `dbg!` in committed code.
- Keep log messages on a single line. Include identifiers (data source name, pipeline name, table name) so logs are filterable.

### Async

- The runtime is Tokio. Don't block in async functions — use `tokio::fs`, `tokio::time::sleep`, and `tokio::task::spawn_blocking` for sync I/O.
- Don't hold a `std::sync::Mutex` / `RwLock` guard across an `.await`. If you need to hold state across awaits, restructure or use `tokio::sync::Mutex`.

### Adding a Data Source

Most providers follow the same shape — use an existing one (e.g. [crates/skardi/src/sources/providers/sqlite/](crates/skardi/src/sources/providers/sqlite/) or [crates/skardi/src/sources/providers/lance/](crates/skardi/src/sources/providers/lance/)) as a template:

1. Add a module under `crates/skardi/src/sources/providers/<name>/`.
2. Implement DataFusion's `TableProvider` (and `ExecutionPlan` if you need a custom physical operator).
3. Add a variant to `DataSourceType` in [crates/skardi/src/sources/data_source_type.rs](crates/skardi/src/sources/data_source_type.rs) and wire registration in [crates/server/src/config.rs](crates/server/src/config.rs).
4. Add a docs page under [docs/{name}/](docs/) with a runnable example.
5. Add tests covering: schema inference, an empty result, a NULL-bearing row, and (for writable sources) a round-trip insert/read.

### Pipelines

Pipelines are YAML files parsed into `StandardPipeline`. Parameter types and response schemas are **inferred from the SQL** by [crates/skardi/src/pipeline/inferencer.rs](crates/skardi/src/pipeline/inferencer.rs) — when changing inference logic, update the snapshot/example pipelines in [docs/](docs/) and [demo/](demo/) so they stay in sync.

## Testing Standards

- **Bug fixes and features must come with tests.** PRs without tests will be sent back.
- Unit tests live next to the code in `#[cfg(test)] mod tests { ... }`. Integration tests for a crate live in `crates/<crate>/tests/`.
- Use `rstest` for parametric cases — Skardi already depends on it. Name cases with `#[case::descriptive_name(...)]`.
- For data source providers, cover NULL values, empty result sets, type coercions, and schema mismatches — these are the failure modes that bite users.
- Vector / KNN tests should assert on results (top-k row IDs, distance ordering), not just "the query succeeded".
- Snapshot tests use `insta` — prefer named `.snap` files over inline `@r"..."` snapshots so diffs review cleanly.

## Documentation

- Every public API (`pub fn`, `pub struct`, `pub trait`) in the `skardi` crate should have a doc comment with at least one example.
- When you add or rename a feature, update [README.md](README.md) and the relevant page under [docs/](docs/) in the same PR. The README's feature list and supported-sources table are user-facing; let them drift and they become wrong.
- Comments explain **why**, not **what**. If a comment restates the code, delete it. If a comment captures a non-obvious invariant, a workaround for an upstream bug, or a performance trade-off, keep it.

## Commits and PRs

- Branch from `main`. Keep PRs focused — no drive-by reformatting, no unrelated dependency bumps.
- Commit messages: imperative mood, one-line subject under ~70 chars, then a body that explains *why* the change is needed (not what the diff already shows).
- Link the issue or describe the user-visible behavior change in the PR description.
- Don't commit secrets, `.env` files, or large binary blobs. Sample data goes under [data/](data/); large or generated test data should not be committed at all.
- Don't bypass pre-commit hooks. If `cargo check` fails on commit, fix the underlying error.

## Things That Will Get a PR Rejected

- A new `.unwrap()` in production code (see Error Handling above).
- Inline `std::sync::Arc<...>` / `datafusion::prelude::...` paths in function bodies instead of `use` imports.
- A new data source, pipeline feature, or SQL behavior with no tests.
- Holding a sync lock across `.await`.
- Changes to inferred parameter or schema behavior without updating example pipelines and docs.
- Drive-by formatting or refactors mixed into a feature/bugfix PR.
- README feature list out of sync with the actual capabilities of the change.

## Where to Look First

- New to the codebase? Start with [crates/skardi/src/lib.rs](crates/skardi/src/lib.rs), then [crates/skardi/src/engine/datafusion.rs](crates/skardi/src/engine/datafusion.rs) and [crates/skardi/src/pipeline/pipeline.rs](crates/skardi/src/pipeline/pipeline.rs).
- Adding HTTP behavior? [crates/server/src/server.rs](crates/server/src/server.rs) and [crates/server/src/handlers.rs](crates/server/src/handlers.rs).
- Adding CLI behavior? [crates/cli/src/main.rs](crates/cli/src/main.rs).
- Adding a new data source? Copy the shape of an existing provider — [crates/skardi/src/sources/providers/sqlite/](crates/skardi/src/sources/providers/sqlite/) is a good template for a SQL-backed source, [crates/skardi/src/sources/providers/lance/](crates/skardi/src/sources/providers/lance/) for a file/columnar source, and [crates/skardi/src/sources/providers/mongo/](crates/skardi/src/sources/providers/mongo/) for a document store. Then wire it into [crates/skardi/src/sources/data_source_type.rs](crates/skardi/src/sources/data_source_type.rs) and [crates/server/src/config.rs](crates/server/src/config.rs). See the **Adding a Data Source** checklist above.
- User-facing docs: [README.md](README.md) and [docs/server.md](docs/server.md).
