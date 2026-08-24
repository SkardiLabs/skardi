# Project Guidelines

## Python
- Always run Python through the repo's virtualenv at `.venv/`, not the system interpreter. Project Python deps (e.g. `sqlite-vec`, `huggingface_hub`) are installed there and are not on the system path. Use one of:
  - `source .venv/bin/activate && python <script>` for an interactive shell.
  - `.venv/bin/python <script>` for a single invocation.
  This applies to the llm_wiki demo (`demo/llm_wiki/setup.py`), the embedding-model download snippet, and any `sqlite_vec.loadable_path()` lookups.

## Code Style
- When using types from crates, import them at the top of the file with `use` statements. Never use full crate paths inline in function bodies.

## Error Handling
- No raw `.unwrap()` in production code (anywhere outside `crates/cli/` and test code). Pick the strategy that matches the failure mode:
  - **Recoverable errors** → propagate via `Result` (`?`, `ok_or_else`, `with_context`, custom error variants).
  - **Lock poisoning** (`std::sync::RwLock`/`Mutex`) → recover with `.unwrap_or_else(|p| p.into_inner())` so a poisoned lock does not panic.
  - **True invariants** that cannot fail at runtime → use `.expect("why this cannot fail")` with a message documenting the invariant (e.g. "len == 1 checked above", "DataType::List guarantees ListArray").
- `.unwrap()` is allowed in `crates/cli/`, in `#[cfg(test)]` modules, in `#[test]` functions, and inside doc-comment examples (`/// ... .unwrap()`).
