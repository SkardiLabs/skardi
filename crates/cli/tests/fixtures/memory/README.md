# Agent memory API fixtures

Copied verbatim from SkardiLabs/skardi-cloud,
`code/rust/crates/skardi-source-contract/tests/fixtures/memory/`, at
`4e125e76bb02dd07e2a7a6dfee4ed41bd6facc20` (origin/main, 2026-11-22). That is
after the share answer gained the record's `version` and a confirmed share
its `if_version` (SkardiLabs/skardi-cloud#1658), and after the warning began
listing the sources the memory cites (SkardiLabs/skardi-cloud#1671).

They are the request and response JSON of every member memory route, and
skardi-cloud checks them against its own types (`memory/api.rs`). This crate
cannot depend on skardi-cloud, so `skardi memory` is tested against these files
with a stub server (`src/agent/memory.rs`, tests module). When the contract
changes, copy the files again rather than editing them here. The admin
fixtures (`admin_list_response*.json`) are left out: the console alone
reaches the admin routes.
