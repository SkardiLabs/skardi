# RSS Feed Support Design (v2 — architecture-first)

**Status:** Draft for review
**Date:** 2026-07-22
**Branch:** `add_RSS_Feed`
**Relation to v1:** Same substance as `2026-07-20-rss-feed-support-design.md`, restructured per review feedback: architecture and interaction walkthrough come first; decisions are grouped into logical blocks. Where a decision carries a v1 number it is cross-referenced as (D*n*). v1 is retained for the full alternatives analysis and reference tables.

## Summary

Skardi gains a native, read-only `type: rss` source. One configured source binds a subscription list and exposes two fixed tables — `feeds` (per-subscription health) and `items` (live union of all current entries) — fetched at query time through a per-feed TTL cache with HTTP conditional requests. All wild-web dialects (RSS 0.9x/1.0/2.0, Atom 0.3/1.0, JSON Feed) normalize to one protocol-pinned schema, with declared-vs-parsed conformance recorded queryably. Everything downstream — archiving, chunking, embedding, hybrid search — composes from existing pipeline primitives, automated by an `auto_news_base` skill.

## Architecture

### System context

```
                 you / your agent
                        │
        edit ctx.yaml   │   SQL over REST / shell verbs
        (subscriptions) │   e.g. SELECT … FROM news.main.items
                        ▼
┌─ skardi engine ────────────────────────────────────────────────┐
│                                                                │
│   ┌─ type: rss source (read-only) ──────────────────────────┐  │
│   │  tables:  <name>.main.feeds    <name>.main.items        │  │
│   │  cache:   per-feed TTL + ETag/Last-Modified state       │  │
│   │  fetch:   bounded HTTP, conditional GET, per-feed       │  │
│   │           partitions, sanitize → parse → conformance    │  │
│   └────────────────────────┬─────────────────────────────────┘  │
│                            │ HTTPS (only at scan time)          │
└────────────────────────────┼────────────────────────────────────┘
                             ▼
              blog feeds · arXiv · CVE · GitHub releases · podcasts
                        (the open, unauthenticated web)

downstream (user-space composition, rendered by the auto_news_base skill):
  items ─▶ archive pipeline (anti-join INSERT + html_to_markdown + chunk
           + candle) ─▶ sqlite archive (fts5/vec0) ─▶ `skardi news "<q>"`
```

The provider's responsibility starts at the feed URL and ends at Arrow batches. It holds no history, runs no scheduler, and writes nothing; retention and retrieval are pipeline compositions over the live window.

### Walkthrough: how you interact with an RSS feed

**1. Subscribe (configuration, zero network).** Add feeds to `ctx.yaml` — inline URLs or an OPML file — under a typed `rss:` block. Registration validates config only; no feed is contacted at startup (D14).

```yaml
data_sources:
  - name: news
    type: rss
    hierarchy_level: catalog
    rss:
      feeds:
        - url: https://blog.rust-lang.org/feed.xml
          name: rust-blog
        - url: https://this-week-in-rust.org/rss.xml
      ttl_seconds: 900
```

**2. Preview before subscribing (optional).** `rss_scan('<url>')` is a registration-free table function that fetches one feed ad hoc and returns the same schema as `items` (D13):

```sql
SELECT title, link, published FROM rss_scan('https://example.com/feed.xml') LIMIT 10;
```

**3. Query.** `SELECT … FROM news.main.items` plans one execution partition per subscription (D6). Each partition independently:

- serves its cached window if within TTL — zero network;
- otherwise issues a conditional GET with stored ETag/Last-Modified — HTTP 304 re-arms the cache without reparsing;
- on HTTP 200, runs strict parse → (on failure) one bounded sanitation pass → conformance check → Arrow (D5, D10, D16).

`WHERE feed = 'rust-blog'` prunes to exactly one partition before any fetch; `LIMIT` stops launching partitions once satisfied (D15).

**4. Observe health.** A dead feed never poisons the scan: its partition serves a stale cached window (marked `stale-error`) or zero rows, while `feeds.last_status` / `last_error` / `conformance_notes` record what happened, queryably (D7, D16):

```sql
SELECT name, last_status, dialect, item_count FROM news.main.feeds;
```

**5. Compose downstream.** History, chunking, embedding, and hybrid search are ordinary pipelines reading `items` — the provider stays a pure protocol adapter (D4). The `auto_news_base` skill renders the whole stack (ctx, archive DDL, pipelines, aliases, semantics) from a natural-language subscription list and self-verifies it end to end.

## Design Decisions, grouped

### A. Data model — one list, two tables

- **One source = one subscription list; a single feed is a list of length one** (D1). RSS is a fixed protocol: every feed has the same shape, differing only by origin — so relational modeling dictates one `items` table with a `feed` discriminator column, not N same-shaped tables. Per-feed tables would fragment the primary query ("search all my subscriptions") into `UNION ALL` and turn subscription edits into registration churn.
- **Two fixed tables, catalog-style naming** — `<name>.main.feeds`, `<name>.main.items` (D2), congruent with the sqlite catalog convention. Schemas are protocol-pinned; nothing is discovered at runtime.
- **Item identity is `(feed, guid)`, guid falling back to `link`** (D3). In-place entry updates simply reflect in the live window.
- **Stable columns for the RSS/Atom core plus enclosures; everything else into `extensions_json`** (D12). Timestamps typed `Timestamp(ms, UTC)`, categories `List<Utf8>`; enclosure columns unlock podcasts at negligible cost.
- **The dialect → unified-schema mapping is a documented, annotated contract** (D17). The Field Mapping table (v1 § Field Mapping) ships in `docs/rss.md` and as semantics-overlay column descriptions, so agents see each column's per-dialect provenance from the schema itself.

### B. Freshness — TTL cache + conditional requests

- **Scan-time fetch through a per-feed TTL cache** (D5), three tiers: within TTL → cached, zero network; expired + 304 → header-only revalidation; 200 → full parse. Cache entries are per feed, so partial hits refetch only what expired. `ttl_seconds: 0` = always live.
- **Completeness invariant:** only a complete, successfully parsed feed window is ever cached — never a half-parsed one (D5).
- **No history in the provider** (D4): the live window is the contract; archiving is the proven anti-join `INSERT` pipeline pattern.
- **Registration performs zero network I/O** (D14): probing dozens of feeds at boot would make startup slow and brittle; the first scan pays the cost.

### C. Execution — parallel, prunable, bounded

- **One DataFusion partition per feed** (D6): parallel fetches without a bespoke pool, streaming batches, and a natural fault boundary.
- **Pushdown: `Exact` on `feed`/`feed_url` equality and `IN`** (D15); nothing else can reach the wire — RSS has no query parameters. `LIMIT` short-circuits partition launch.
- **Bounded everything:** per-request timeout, scan deadline, response-size cap, jittered retries honoring `Retry-After`, `max_concurrent` doubling as the per-host politeness bound; cancellation aborts in-flight requests.

### D. Fault tolerance & conformance — tolerant, never silent

- **Multi-feed scans degrade per feed, visibly; the single-feed `rss_scan` fails fast** (D7). A failing feed serves its stale window (marked) or zero rows; `feeds` records the error; tracing warns. One dead blog must not render a 50-subscription news base unqueryable.
- **Dialect is detected, conformance-checked against the feed's own declaration, and queryable** (D16). Feeds lie routinely (Atom served as `application/rss+xml`; RSS 2.0 missing required channel fields). After each parse the provider records declared vs parsed dialect and spec-required-field violations in `feeds.dialect_declared` / `dialect` / `conformance_notes`. Checks never reject a feed that parsed — they convert silent tolerance into queryable evidence.

### E. Configuration — typed, config-not-data

- **Typed `rss:` block, not the flat options map** (D8): the flat `options: HashMap<String,String>` cannot safely express a nested subscription list; precedent from the Open Connector design.
- **The subscription list is configuration, not data** (D9): no SQL statement can add or remove a feed, preserving the validator's no-DDL invariant. Agents manage subscriptions by editing `ctx.yaml`/OPML and reloading — a flow the skill owns.

### F. Parsing & compatibility — many dialects, one representation

- **`feed-rs` behind an `rss` Cargo feature** (D10) parses RSS 0.9x/1.0/2.0, Atom 0.3/1.0, and JSON Feed. The known risk — tolerance gap versus Python's `feedparser` on two decades of malformed feeds — is handled by:
  1. a **sanitation pre-pass on failure only** (encoding sniff, re-encode, control-char strip, naked-ampersand repair; one retry; repairs recorded in `conformance_notes`);
  2. the **conformance check** after every successful parse (block D);
  3. a **fixture corpus as a regression ratchet**: every real-world failure becomes a fixture; contract tests assert parse-or-visible-degradation, never a panic, never a silent skip;
  4. a **documented tolerance floor** in `docs/rss.md`.
- **Content is stored wire-faithful (HTML)** (D11); transformation is a query-time choice via the `html_to_markdown()` scalar UDF, which closes the gap to `chunk('markdown', …)` and is useful for any HTML-bearing source.

### G. Surfaces — table, function, skill

- **Persistent tables** for repeated queries and joins; **`rss_scan(url)` UDTF** for ad-hoc exploration — both compile to the same scan path (D13).
- **`auto_news_base` skill** (skardi-skills repo): natural-language subscription list → autodiscovery → `rss_scan` preview → rendered ctx/DDL/pipelines/aliases/semantics → self-verification (`skardi sync`, `skardi news "<q>"`, per-feed health report). The skill holds no privileged API; everything it emits is plain user-space configuration.

## Non-goals

No full-article scraping, no scheduler, no push (WebSub — future), no write path, no authenticated feeds (Open Connector territory), no history retention in the provider, no gateway. Rationale in v1 § Non-goals and § Alternatives Considered.

## Reference (unchanged from v1)

Table schemas, the Field Mapping table, failure-mode matrix, testing strategy, acceptance criteria 1–11, rollout milestones M1–M3, and expected repository shape are specified in `2026-07-20-rss-feed-support-design.md` and are normative as written there.
