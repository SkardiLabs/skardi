# Phase 5 — Self-review checklist

This is the distilled record of every review round the GitHub, Slack,
and Notion packs went through — each item below caught (or would have
caught) a real defect in this repo. Go through it against your actual
diff, not from memory. For each item: verify, fix, or write down a
technical rebuttal with evidence. Severity habits: correctness/data-loss
→ P1, test/doc hygiene → P2.

## Silent-truncation and termination correctness

The worst failure class: wrong results with a green status.

- [ ] Pagination terminates ONLY on the provider's documented
      end-of-collection spellings. Anything else that stops a scan —
      wrong-typed continuation state, structural traversal failure —
      fails loudly (`PaginationCursorInvalid`, `PaginationTotalInvalid`,
      `PaginationRawPageSizeInvalid`, propagated row-path errors), never
      reads as "done". VERIFY these guardrails exist on your branch
      (implementation.md §Engine baseline); on an older base, adding them
      (with regression tests) is prerequisite work, not a checkbox to
      assume.
- [ ] Post-pagination filtering checked: if the executor filters the
      fetched page, the table declares `raw_page_size_path` (or an
      authoritative total) — a filtered count is never a termination
      signal, and a missing signal means upstream contribution or
      deferral, not a heuristic.
- [ ] Termination verified on the REAL final page, not assumed from the
      envelope shape: providers can return a non-empty continuation
      token beside `has_more: false` (Feishu wiki), which null-token
      termination refetches until the loop guard kills the scan.
      Declare `has_more_path` where the envelope carries an
      authoritative has-more boolean, and pin the live final-page shape
      with an e2e.
- [ ] Short/empty non-final pages cannot truncate: if the envelope has
      an authoritative total, the strategy declares `total_pages_path`;
      if not, the heuristic's limits are documented.
- [ ] Boundary rows cannot be dropped: no `>=` mapped onto
      strictly-greater provider params; floored `EpochSeconds` only on
      lower bounds.
- [ ] Would any failure show the user a misleading error? (The
      `RowPathNotFound`-instead-of-`missing_scope` test: the truthful
      error is the provider's/gateway's own, with identity attached.)

## Contract honesty

- [ ] Every action ID, input key, row path, and resource key verified
      against the live gateway (phase 1 evidence, not provider docs).
      camelCase everywhere OC says so; strict schemas mean one wrong key
      is a runtime 400 that CI cannot see.
- [ ] Field mappings match what the EXECUTOR emits (passthrough vs
      normalized), not the provider's raw API.
- [ ] Phase-4 real-data evidence exists for every table: registration
      passed the fingerprint gate against LIVE discovery, real rows
      scanned through skardi-server, and every mapped column extracted
      a real non-NULL value somewhere in a seeded dataset. A column no
      live row ever populated is the always-NULL bug (declared-vs-wire
      spelling, e.g. `archived` vs `is_archived`) until proven
      otherwise.
- [ ] Every pinned/fixed input proven to RETURN ROWS live, not just
      HTTP 200 — version-coupled enum pins (Notion's `data_source` vs
      pre-2025 `database`) look healthy while returning nothing. If the
      gateway pins a provider API version header, it is recorded in the
      pack doc and module doc.
- [ ] Real multi-page pagination exercised live (small page size), the
      continuation token observed at the declared path, and termination
      observed on the documented spelling.
- [ ] `Exact` fidelity only where faithful across the whole value
      domain; every string-enum push is `Inexact`.
- [ ] Fingerprints pinned from the live capture; sync test locks pin ↔
      contract fixture; drift-refusal e2e exists; mocks serve the
      captured contracts so the gate's pass side actually runs; the
      fingerprint coverage gap (uncovered mapped columns) is pinned per
      table.
- [ ] Deliberate gaps (unmapped filters, deferred tables, `error_path:
      None`) each have a module-doc rationale AND a guard test.

## Test quality

- [ ] Structural assertions: parse JSON and assert on the value tree
      (`input.get("cursor").is_none()`), never `contains("cursor")`.
- [ ] Row identity, not cardinality: filter and pagination tests assert
      WHICH rows survived (ids), not how many.
- [ ] Per-declaration coverage: every table's own wire declarations
      (row path, input keys, pagination params) pinned by its own e2e —
      shared constants are not shared coverage.
- [ ] Every wire e2e asserts the EXACT input key set per request
      (sorted keys equal, absence included — page 1 carries no cursor),
      not key presence: presence-only assertions cannot catch an
      undeclared extra leaking onto the wire, and strict action schemas
      turn that extra into a runtime 400.
- [ ] Declared constants asserted by VALUE, not key presence — every
      table's `page_size` pinned to its number on the wire (`pageSize`
      is exactly where a live contract defect surfaced: declared 100,
      wire caps at 50).
- [ ] Both sides of every gate: the pass path (suite-wide) and the fail
      path (targeted test). A gate whose failure arm no test exercises
      is dead code until proven otherwise — and the failing input must
      reach the gate THROUGH THE PUBLIC ENTRY POINT, not by calling the
      guard function directly. Deserialization layers can destroy the
      evidence before a post-hoc guard runs: serde_json's f64 visitor
      converts a nested `.nan` to `null` during untagged buffering, so a
      "reject non-finite" walk over the deserialized value could never
      fire (the `first_non_finite` finding on the Notion PR — the fix
      captures `serde_yaml::Value` and converts fallibly where the
      evidence still exists).
- [ ] Error tests assert the full identity (column/path/page/row/
      expected/found-kind) and that the offending VALUE never appears.
- [ ] Negative-space guards for every "this deliberately doesn't
      happen" claim in comments or docs.
- [ ] Env vars via `testutil::EnvVarGuard` (restore-on-drop, panic
      included), per-test-unique names; no bare `set_var`/`remove_var`.
- [ ] Mocks speak the real gateway protocol (uniform envelope, no
      `/execute` suffix, camelCase discovery via `testutil` helpers) —
      a mock encoding a wrong assumption makes CI complicit.

## Information discipline

- [ ] JSON *kinds* in errors, never values (rows can contain secrets).
- [ ] Bounded quoting: error-response snippets ≤ 512 chars, provider
      error codes ≤ 128; tokens never in YAML/logs/`Debug`/errors.
- [ ] Fixtures are REDACTED LIVE CAPTURES (real envelope keys, field
      presence, timestamp spellings; synthetic UUIDs, placeholder
      names/titles/URLs) — not hand-written shapes; the redaction was
      audited mechanically (every surviving string matched against an
      allowlist — real titles hide in URL slugs). Deliberately-broken
      fixtures (schema-mismatch) stay synthetic and say so.
- [ ] The redaction audit DECODES nested JSON-encoded strings and
      audits their leaves too (real names survived one decode level
      down in a message payload), and it ships as an in-repo tripwire
      test so CI enforces it. Person-linked capture timestamps are
      coarsened; redacted cross-references stay self-consistent (an
      id embedded in the row's own URL matches the row). If PII ever
      reached a commit, the branch history was rewritten, not just the
      tip.
- [ ] Columns with ZERO fixture evidence (no captured row carries the
      key) are annotated doc-derived at the declaration — under a
      loose-schema pack, real rows are the only column truth, so an
      evidence gap must be a reviewed fact, not an implicit one.
- [ ] No real orgs/users/tokens anywhere; if a credential was ever
      pasted into a conversation or log during verification, the user
      was told to rotate it.

## Docs and spec sync

- [ ] Every test count in docs/spec matches a fresh cargo run, with the
      counting command stated next to the number.
- [ ] No stale references: milestone numbers in Review notes, removed
      columns/tests still described, fixture-category lists, "pending"
      markers for work that has since landed.
- [ ] The pack doc's table/pushdown matrix re-derived from the FINAL
      yaml after the live pass — a pushdown the reconciliation dropped
      must read `—`, not survive as a promise (the doc row is the
      easiest artifact to forget when the wire invalidates a draft).
- [ ] Upstream gateway defects found during verification are filed as
      issues on the gateway repo and LINKED from the pack doc.
- [ ] Operational consequences documented where behavior surprises:
      fingerprint pins fail on ANY schema change (additive included —
      re-capture and re-pin on upstream upgrades); raw-scan default-deny
      against gateways without a read/write classification; cache-key
      contents (LIMIT is load-bearing).
- [ ] Module doc, pack doc, and spec entry tell the same story in the
      same vocabulary.

## Code hygiene

- [ ] `cargo fmt` clean; `cargo clippy` clean; FULL `cargo test -p
      skardi --lib` green (engine changes ripple beyond the pack
      filter).
- [ ] No `.unwrap()` in production paths; `unsafe` only with a written
      soundness argument, centralized (see `EnvVarGuard`).
- [ ] Engine extensions backward-compatible: new struct fields optional
      with `None`/default, existing packs untouched semantically.
- [ ] Comments state constraints the code cannot show — not narration,
      not review-facing justification. Match the module's existing
      comment density and voice.
- [ ] Scan-side invariants preserved if touched: planning does no
      network I/O; `pagination.advance` skipped once done; only
      completed scans cached (LIMIT-satisfied counts as complete FOR ITS
      KEY because LIMIT is in the key); completion/failure tracing
      events exactly once.

## Final pass

- [ ] Run the repo's code-review skill on the diff; triage every finding
      (fix, or rebut with evidence — verify claims against code first;
      reviews have cited stale line numbers and miscounted tests
      before).
- [ ] Re-read the PR diff top to bottom as a reviewer would, one last
      time, after everything above. The goal is that a human reviewer
      finds nothing the checklist should have caught.
