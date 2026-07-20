# RSS Fetcher Probe (Supplement)

**Status:** Supplementary exploration — non-gating
**Date:** 2026-07-20
**Branch:** `add_RSS_Feed`
**Main spec:** `2026-07-20-rss-feed-support-design.md`

## Role

This document preserves the original demo-first exploration as an optional probe. It is **not** on the critical path: the native `type: rss` source and the `auto_news_base` skill are specified and committed to in the main spec regardless of whether this probe runs.

The probe still earns its keep through three evidence outputs:

1. **Parser-tolerance corpus.** Every malformed-feed incident met while running the probe (feedparser `bozo` cases, encoding lies, truncated documents) is captured as a fixture for `providers/rss/fixtures/` — the regression ratchet behind the main spec's Compatibility Strategy. `feedparser` serves as the tolerance baseline that quantifies `feed-rs`'s residual gap.
2. **Composition validation.** The probe exercises the exact archive-and-search composition the skill will render — anti-join ingest, inline chunk + embed, RRF hybrid retrieval, aliases, semantics — against a hand-built SQLite layout, confirming the rendered artifacts before the skill automates them.
3. **UX observations.** Friction notes on verbs, setup ceremony, and agent-discoverability feed the skill's design.

## What the probe is

A single directory, `demo/rss_news/`, runnable end-to-end with only `skardi-cli` — no server, no Docker, no engine changes. A minimal Python fetcher translates feed XML into a SQLite table; everything downstream is existing Skardi primitives.

```
        ┌────────────── user land ──────────────┐   ┌────────────── Skardi ──────────────┐
 feeds ─┼─▶ fetch.py ──▶ news.db: rss_items ────┼───┼─▶ sync (pipeline: anti-join +      │
        │   (feedparser, html→markdown)          │   │    chunk() + candle() + INSERT)    │
        │   setup.py (one-shot DDL: tables +     │   │        ▼                           │
        │   fts5/vec0 mirrors + triggers)        │   │   rss_chunks ─(triggers)→ mirrors  │
        └────────────────────────────────────────┘   │        ▼                           │
                                       agent ◀───────┼─  news (sqlite_knn+sqlite_fts RRF) │
                                                     └─────────────────────────────────────┘
```

Eight files: `README.md`, `setup.py`, `fetch.py`, `ctx.yaml`, `aliases.yaml`, `semantics.yaml`, `pipelines/ingest.yaml`, `pipelines/search_hybrid.yaml`.

## Key mechanics (validated decisions the main spec inherits)

- **Raw zone / search zone split.** `rss_items` (guid PK, written only by the fetcher) and `rss_chunks` (written only by the sync pipeline) — the same boundary the native source draws between its live window and the archive.
- **Idempotency by construction.** Fetch-side: `INSERT OR IGNORE` on guid PK. Ingest-side: `WHERE guid NOT IN (SELECT guid FROM rss_chunks)` anti-join — rerunning either step is a no-op.
- **Mirror maintenance via SQLite triggers** (`AFTER INSERT`/`AFTER DELETE`), copied from `demo/llm_wiki/setup.py`; mirrors stay transactionally consistent with the base table.
- **DDL lives in `setup.py`** because the SQL validator rejects DDL everywhere (`DdlNotAllowed`) — the same constraint that shapes the skill's rendered setup script.
- **Proven pipeline workarounds:** `SELECT … FROM (…) AS t` wrapper for INSERT projection validation; explicit `ingested_at` because column defaults do not fire on the INSERT path.
- **Subscriptions as data, not code.** Evolution over the first draft: the feed list belongs in a `rss_feeds` table (or OPML file) rather than a constant in `fetch.py`, so an agent can manage subscriptions through SQL. Note the asymmetry: this is sound *here*, where feeds are ordinary rows in a user-owned SQLite file; the native source deliberately keeps subscriptions in configuration (main spec Decision 9).

## Fetcher contract (`fetch.py`, ~60 lines)

- `guid` = `entry.id` falling back to `entry.link`; content = `entry.content[0].value` falling back to `entry.summary`; HTML → markdown via `html2text` before storage (the probe has no `html_to_markdown()` UDF).
- Per-feed `try/except`: one dead feed logs and skips, never aborts the run — the behavioral sketch of the native source's per-feed degradation.
- `bozo` feeds are ingested when salvageable and **counted**; the count plus the offending documents go to the pain log / fixture corpus.

## User journey

```bash
pip install feedparser html2text sqlite-vec huggingface_hub
python demo/rss_news/setup.py
export SQLITE_VEC_PATH=... SKARDICONFIG=demo/rss_news

python demo/rss_news/fetch.py      # pull subscriptions → rss_items
skardi sync                        # chunk + embed new items (idempotent)
skardi news "AI agent funding"     # RRF hybrid search, citable results
```

## Probe-specific limits (documented, not engineered around)

Tens of feeds / thousands of items; `NOT IN` anti-joins and single-process SQLite; summary-only feeds stored as summaries; FTS5 reserved characters surface as query errors; no scheduler (cadence is cron/agent territory). None of these limits carry design weight for the native source — they bound only the probe.

## Evidence log

Kept in the probe's `README.md` (**Fetcher pain log** section): every malformed-feed incident with the offending document snapshotted into the fixture corpus, plus setup-ceremony friction notes for the skill. This log is the probe's primary deliverable to the main spec.
