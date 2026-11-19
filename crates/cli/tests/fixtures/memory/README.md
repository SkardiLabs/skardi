# Agent memory API fixtures

Copied verbatim from SkardiLabs/skardi-cloud,
`code/rust/crates/skardi-source-contract/tests/fixtures/memory/`, at
`6cb4aac128c69888d5670c1d3a8c932dff43cd10` (origin/main, 2026-11-13).

The share fixtures are newer, from two pull requests that had not merged
when they were copied (2026-11-20). Copy them again from main once both have:

- `share_request_confirmed_at_version.json`, `share_response_needs_confirm.json`,
  `share_response_shared.json` and `share_response_unshared.json` from
  SkardiLabs/skardi-cloud#1658 at `16972e447` (a share answer carries the
  record's `version`, and a confirmed share may pass it back as `if_version`);
- `share_response_needs_confirm_cited.json` from SkardiLabs/skardi-cloud#1671
  at `a6626bafd` (the warning lists the sources the memory cites).

They are the request and response JSON of every member memory route, and
skardi-cloud checks them against its own types (`memory/api.rs`). This crate
cannot depend on skardi-cloud, so `skardi memory` is tested against these files
with a stub server (`src/agent/memory.rs`, tests module). When the contract
changes, copy the files again rather than editing them here. The admin
fixture is left out: the console alone reaches the admin routes.
