---
name: source-pack
description: >-
  Develop a new Open Connector source pack for Skardi end-to-end: research the
  provider's live gateway contract, implement the pack (tables, fixtures,
  fingerprints, tests, docs), verify every table against real provider data
  through a live gateway, self-review against this repo's accumulated
  review standards, and submit a PR. Use this skill whenever the user asks to
  add, support, integrate, or onboard a data source or SaaS provider (Notion,
  Jira, Gmail, HubSpot, Discord, Feishu, …) as SQL tables, mentions "source
  pack", a "milestone 5.x" task, or wants any provider reachable through Open
  Connector — even if they never say the words "source pack".
---

# Developing an Open Connector source pack

You are implementing one milestone of the Open Connector integration: a
**source pack** — static, Skardi-reviewed relational contracts over a
provider's read actions, exposed as SQL tables. The GitHub pack (raw
passthrough rows) and Slack pack (normalized rows) are the two reference
implementations; read them before writing anything.

The single most important lesson baked into this repo, learned the hard
way: **the wire contract is Open Connector's, not the provider's raw
API.** The GitHub pack originally shipped with `per_page`, `issue_number`,
a nonexistent action ID, the wrong execute endpoint, and the wrong
response envelope — all plausible from GitHub's own docs, all wrong
against the real gateway, and all invisible to CI because the mocks
encoded the same wrong assumptions. Every phase below exists to prevent
that class of failure.

The second lesson, learned on the Notion pack (PR #177): **the declared
contract is not column truth either.** A pack can pass every
contract-level check — fingerprints pinned, credential wall reached,
full suite green — and still map columns that are always-NULL on the
real wire, because declared schemas under-declare, misname
(`archived` vs `is_archived`), or `anyOf`-hide the fields passthrough
executors actually emit. Phase 4 exists to prevent THAT class of
failure: no pack is submission-ready until every table has scanned real
rows end to end.

## Required reading (before phase 1)

1. `docs/superpowers/specs/2026-07-11-open-connector-integration-design.md`
   — especially the **source-pack admission gate**: complete terminating
   pagination, deterministic schema, read-only allowlist, documented
   authz/rate limits, bounded safety defaults, null/empty/nested/
   schema-mismatch fixtures, docs. The gate is the definition of done.
2. `docs/superpowers/specs/2026-07-11-open-connector-integration-tasks.md`
   — the milestone map; entries 5.1 (GitHub) and 5.2 (Slack) are the
   template for what your milestone entry must eventually say.
3. `crates/skardi/src/sources/providers/open_connector/packs/github.rs`
   and `packs/slack.rs` — read the module docs top to bottom; every design
   decision a pack makes is recorded there with its rationale, and yours
   must be too.
4. `docs/open-connector.md`, `docs/open-connector-github.md`,
   `docs/open-connector-slack.md` — the documentation shape you will add
   to.

## Phase 1 — Reconcile the contract against a live gateway

Do this FIRST, before designing tables. Read
[references/contract-reconciliation.md](references/contract-reconciliation.md)
for the concrete steps: starting the local gateway, probing the real API,
reading the provider's executor source in the Open Connector repo, and
validating generated inputs without provider credentials.

Non-negotiable outputs of this phase:

- The exact action IDs that exist (never assume a name; `github.
  list_repositories` did not exist).
- Every input key, verbatim from `inputSchema` (camelCase, and
  `additionalProperties: false` means a wrong key is a hard 400).
- The row shape: does the executor pass provider rows through raw
  (GitHub-style) or rebuild them normalized (Slack-style)? Only the
  executor source answers this — declared output schemas can be lax
  (`additionalProperties: true`) while the executor passes through fields
  the schema never mentions.
- How pagination is emitted (top-level `nextCursor`? sibling
  `total_count`? authoritative `paging.pages`?) and how in-band provider
  errors are handled (most OC executors consume them and return a failure
  envelope; `error_path` is only for gateways that forward them).
- Captured output schemas for fingerprint pinning (phase 3). Treat them
  as fingerprint input, NOT column truth — for passthrough rows the
  final column sets are settled by real rows in phase 4.

## Phase 2 — Design the tables

Read [references/implementation.md](references/implementation.md) §Design
before deciding anything. Summary of the rules that have survived review:

- Choose read-only list actions with **complete terminating pagination**
  only. An action whose pagination cannot be completed (Slack message
  history, at the time of 5.2) is deferred and documented as absent, not
  shipped incomplete.
- Every design decision (a pinned input, an unmapped filter, an excluded
  table, a nullability choice) is written into the module doc with its
  why. Reviewers here read module docs as claims to be verified.
- If the user named specific resources ("I want Notion pages and
  databases"), scope to those; otherwise propose the natural first wave
  (list-shaped, high-value, gate-passing) the way 5.1 chose 8 tables and
  5.2 chose 3.

## Phase 3 — Implement

Follow [references/implementation.md](references/implementation.md)
§Implementation for the full checklist: the pack's YAML asset (packs are
declarative embedded YAML validated by `packs/loader.rs` — authoring is
data, not Rust) plus its accessor module and registry entry, the
six fixture categories (including schema-mismatch), fingerprint pinning
(capture → pin → sync test → contract-serving mocks → drift-refusal
e2e), per-declaration end-to-end tests through `MockGateway`, and the
three documentation targets (pack doc, spec entry with counted
verification, `docs/open-connector.md` status).

Engine extensions are allowed when the pack genuinely needs them (5.1
added `Fidelity` and list plucking; 5.2 added `total_pages_path`,
`ValueFormat`, `TimestampSecondsUtc`) — keep them backward-compatible
(optional fields, `None` defaults) and test them at both the engine and
the pack level. Before relying on any engine invariant this skill
names, verify it exists on your base branch — see the implementation
reference's **Engine baseline** section; `main` carries the full
baseline today, but an older base may lack part of it, and adding the
missing invariant (with regression tests) is then prerequisite work.

## Phase 4 — Verify against real data (integration test)

Implementation done and tests green is NOT done. Follow
[references/live-verification.md](references/live-verification.md):
have the user configure a real (free-tier) provider account in the
gateway — you never touch the credential; for OAuth providers the
reference maps the full flow and its layered gates (scope unions the
app cannot enable, gateway-declared scopes the API does not honor,
capability flags and admin approvals beyond scopes, stale-token
snapshots) — then probe every action with
the pack's exact inputs AND the declared input bounds, diff real row
keys against the mapped columns
in both directions, verify termination on the real final page, scan
every table end to end through skardi-server
(registration passes the fingerprint gate against LIVE discovery; every
mapped column extracts a real non-NULL value somewhere; pinned filters
actually return rows; a small page size forces real multi-page
pagination), and re-derive the fixtures as redacted live captures.
Where the wire contradicts the declared contract, the wire wins for
columns; record the contradiction and the provider API version the
gateway pins. Reviewers here block packs that skipped this phase, and
they are right to.

## Phase 5 — Self-review before any PR

This phase is why the submitted code is good. Work through
[references/review-checklist.md](references/review-checklist.md) — it is
the distillation of every review round the existing packs went through.
Treat it the way you would treat a human reviewer's findings: verify each
item against the actual code, fix what fails, and be honest about
severity. Then:

1. `cargo fmt` and `cargo clippy` clean.
2. `cargo test -p skardi --lib` — the FULL library suite, not just the
   pack filter (engine changes ripple).
3. Count tests with the documented methodology
   (`cargo test -p skardi --lib sources::providers::open_connector` and
   the pack-scoped filter) and make every count in docs/spec match.
4. Run the repo's code review on your own diff (the `review` /
   `/code-review` skill if available) and fix or consciously rebut every
   finding. A finding you disagree with gets a verified technical
   rebuttal, not silence — reviews here have been wrong before (cited
   line numbers stale, counts miscounted), and verifying against the
   code before acting is part of the standard.

## Phase 6 — Submit the PR

- Branch `feature/open-connector-<provider>-pack` off latest `main`
  (fetch first). If the work must stack on an unmerged PR's branch,
  stack — and recommend Draft until the base merges.
- Commits: conventional style (`feat(sources): …`), detailed bodies that
  explain *why* (look at `git log` for the house voice), ending with the
  repo's standard co-author trailer.
- Tick the milestone entry in the tasks spec with a verification blurb
  matching 5.1/5.2's density (decisions, live-reconciliation status,
  counted tests with the counting command).
- PR body modeled on the merged pack PRs (#168-level per-module detail):
  what shipped per module, design decisions with rationale, engine
  extensions, verification section with test counts, the phase-4
  real-data evidence (per-table live results, pinned provider API
  version, what the live pass changed), and any deliberate deferrals
  (e.g. fingerprint pins pending, tables gated on upstream support).
- `gh pr create` with that body; use Draft when stacked or when the user
  asked for in-progress visibility.

## Working style

- Evaluate before you fix: when review feedback arrives (from the user or
  your own phase-5 pass), first verify the claim against the code —
  some findings are already fixed, stale, or wrong, and saying so with
  evidence is as valuable as a fix.
- No credentials in Skardi, ever. Provider credentials live in the
  gateway; tests use `EnvVarGuard` with per-test-unique variable names;
  tokens never appear in YAML, logs, `Debug`, or errors.
- Errors carry identity (action, table, page, row, column) and JSON
  *kinds*, never values; snippets stay bounded.
- When you and the user have live-gateway access, prefer one real probe
  over an hour of speculation.
