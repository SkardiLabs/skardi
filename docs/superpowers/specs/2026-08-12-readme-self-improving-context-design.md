# README repositioning: self-improving context framework

**Date:** 2026-08-12
**Scope:** `README.md` (full rewrite)

## Problem

The README positions Skardi as an "agent data plane." The phrase is borrowed
from cloud infrastructure and does not land with the audience we are writing
for — agent builders, who do not think of themselves as operating a plane. It
also undersells what the recent `/query` work made possible: because every
ad-hoc query now arrives with declared intent and lands in a durable ledger,
Skardi can observe how an agent actually uses data and turn recurring usage
into new tools. That is the differentiated story and the page does not tell it.

Secondary problem: the page is 487 lines with two overlapping essays arguing
the same point (`## What is an "agent data plane"?` and `## When does a uniform
data plane earn its keep?`), and a Quick Start that walks a 20-line hybrid
search SQL statement the reader cannot run by copy-paste anyway.

## Positioning

Skardi is an **open-source self-improving context framework** for building
personalized agents. The claim: the agent's data toolset is not fixed at ship
time — it grows out of the agent's own observed behaviour.

The narrative spine is a three-stage flywheel:

| Stage | What it is | Status |
| --- | --- | --- |
| **Observe** | `POST /query` with `ai_context` (`purpose` + `session_id`); every statement recorded in the fail-closed SQLite audit ledger (`--query-audit-db`), indexed on `(session_id, created_at)` | shipped |
| **Learn** | pattern analysis over the ledger — LLM or algorithmic — grouping by session and purpose to find what recurs | by hand today; automated in flight |
| **Act** | promote a recurring query into a named pipeline (REST endpoint + shell verb) the agent then calls as a tool; proactive actions after that | in flight |

`Act` is in flight because it is delivered by the `skardi-query-log` skill
(SkardiLabs/skardi-skills#25), which is held until `--query-audit-db` appears
in a tagged release — it is not in v0.5.0.

## Decisions

1. **"Data plane" is purged from `README.md`.** Not demoted, not kept in a
   badge line — zero occurrences in visible prose. The engine is described
   instead as one federated SQL engine over every registered source.
2. **The loop is both the "why" and the entry path.** A single `## The loop`
   section carries the flywheel diagram and the four-step copy-pasteable
   sequence. It replaces both deleted essays and the top of Quick Start.
3. **Step 3 of that sequence is marked `in flight` inline**, linking to
   skills#25. The other three steps run against v0.5.0 today.
4. **Governance is subordinated, not deleted.** Audit, lineage and
   snapshot-as-branch appear as what makes *acting* on the loop safe — one
   clause each, no dedicated section.
5. **The 20-row source table survives**, with long cells (Open Connector, RSS)
   trimmed to their doc links. It is the load-bearing credibility on the page.
6. **The roadmap and any status table are dropped from the landing page**
   entirely, along with the nav entry pointing at them. The 45-line checklist
   was first relocated to `docs/roadmap.md`, then removed on review — the
   landing README makes its claims in prose and marks the one unshipped step
   inline (step 3 of the loop carries `in flight`), which is where honesty about
   status actually needs to live. Original checklist recoverable from
   `git show HEAD:README.md`; a per-stage version including new
   self-improving-loop items is preserved out of tree for whenever a roadmap
   page is reintroduced.
7. **`## ⭐️ Star the Repository` is removed** as a section; the GIF moves next
   to Community.
8. **Nothing links `docs/agent_data_plane.md` from the top of the page.** It
   remains reachable from the docs index under neutral link text ("Design
   background"). The file is renamed in a later docs pass, not here.
9. **The Observe step is shown with `curl`, not `skardi query`.** The CLI's
   request builder sends only `sql` and `max_rows`
   (`crates/cli/src/commands/query.rs`), so it cannot declare `ai_context`.
   CLI passthrough is listed as not-yet-shipped rather than implied.

## Structure

| Section | Content | Target |
| --- | --- | --- |
| Header | logo, positioning paragraph, Observe/Learn/Act rhythm line, nav, badges, deploy buttons | ~35 lines |
| `## The loop` | flywheel diagram + 4-step sequence | ~45 |
| `## Why it compounds` | 3 bullets: intent recorded not guessed; one chokepoint means one memory; promotion is reviewable YAML | ~14 |
| `## Install` | CLI from source / binary table, then Claude Code skills block (content unchanged) | ~28 |
| `## What's underneath` | federated SQL, pipelines as REST + shell, semantics overlay, jobs; source table | ~40 |
| Tail | `<details>` for architecture, Docker, demos + docs index; Community + License flat | ~40 |

Landed at 305 lines, down from 487 (37%). Above the ~230 first estimate: the
16-row source table, the four-step loop demo and the collapsed tail are what
hold the length, and all three were judged load-bearing.

## Style reference

Compact-header conventions from `NousResearch/hermes-agent`,
`earendil-works/pi` and `openai/codex`: centred logo and badges, a short punchy
intro, a capability table rather than prose paragraphs, install early, long
material behind links or `<details>`.

## Non-goals

- No other doc is rewritten. `docs/agent_data_plane.md`, `docs/semantics.md`
  and the rest keep their current wording and are addressed in a later pass.
- No code changes. Every capability the README claims as shipped is verified
  against the tree at `0.5.0`; anything unverified is marked or omitted.

## Follow-ups surfaced in review

Feedback on the first draft: the headline did not say *what* self-improves, and
promoting queries into pipelines is too small a ceiling for Act.

- **Definition added to the header**: what improves is the agent's context —
  what it can do on your data without rediscovering it — explicitly not model
  weights, serving latency, or infra cost.
- **Act extended from queries to intentions**: recurring intention → standing
  routine (scheduled by the harness — Claude Code routines, cron-driven CLI
  runs), and LLM analysis over the ledger to surface intentions the user never
  noticed. Both verified feasible as skills with the shipped ledger: rows carry
  `created_at`, `ai_context`, and denormalised `session_id`, indexed on
  `(session_id, created_at)`, so cadence detection is a SQL read.
- **Gap to file as a roadmap issue**: pipeline executions are not audited
  (`pipeline_handlers.rs` never writes to the audit store), so once a query is
  promoted its recurrence signal leaves the ledger. The enabling server change
  is `ai_context` on `POST /:pipeline/execute` plus a unified execution ledger.
- **Privacy note for the hidden-intent skill**: ledger rows store raw SQL
  including literals (hence `0600`); feeding a window to a remote LLM must be
  an explicit operator choice, or the skill should strip literals first.

## Verification

- No occurrence of the retired phrase: `grep -ci "data plane" README.md` → 0.
- Every relative link in the new README resolves to a path that exists.
- No dangling reference to the removed roadmap page or status anchor.
- The step-2 request body parses as valid JSON.
