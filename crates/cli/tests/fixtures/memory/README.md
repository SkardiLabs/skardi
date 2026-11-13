# Agent memory API fixtures

Copied verbatim from SkardiLabs/skardi-cloud,
`code/rust/crates/skardi-source-contract/tests/fixtures/memory/`, at
`6cb4aac128c69888d5670c1d3a8c932dff43cd10` (origin/main, 2026-11-13).

They are the request and response JSON of every member memory route, and
skardi-cloud checks them against its own types (`memory/api.rs`). This crate
cannot depend on skardi-cloud, so `skardi memory` is tested against these files
with a stub server (`src/agent/memory.rs`, tests module). When the contract
changes, copy the files again rather than editing them here. The admin
fixture is left out: the console alone reaches the admin routes.
