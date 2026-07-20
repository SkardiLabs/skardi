# RSS Feed Support Design

**Status:** Draft for review
**Date:** 2026-07-20
**Branch:** `add_RSS_Feed`
**Companion:** `2026-07-20-rss-fetcher-probe-supplement.md` (exploratory fetcher probe; non-gating)

## Summary

Skardi will support RSS/Atom subscriptions as a first-class, read-only data source, `type: rss`, together with an `auto_news_base` skill that assembles a searchable, citable news base from a natural-language subscription list.

One configured source binds a subscription list and exposes two fixed tables: `feeds` — one row per subscription carrying fetch health — and `items` — the live union of all current entries across subscriptions. Scans fetch at query time through a per-feed TTL cache with HTTP conditional requests; each feed is an independent execution partition, so a dead feed degrades visibly instead of failing the scan.

The provider is deliberately a pure protocol adapter. History retention, chunking, embedding, and hybrid retrieval are not provider features: they compose from existing primitives — anti-join `INSERT` pipelines, `chunk()`, `candle()`, `sqlite_knn`/`sqlite_fts` — plus one new scalar UDF, `html_to_markdown()`. The `auto_news_base` skill renders and self-verifies that composition end to end.

This spec supersedes the demo-first phasing of the earlier draft. The Python-fetcher probe remains documented in the supplement as exploratory evidence — chiefly to seed the parser-compatibility fixture corpus — but does not gate this design.

## Motivation

Agents need continuously updating external information — industry news, competitor blogs, arXiv categories, CVE announcements, GitHub releases — as *retrievable, citable context* rather than one-shot web searches. RSS/Atom is the only open, machine-readable standard for this class of content, and it remains ubiquitous: nearly every blog platform, news site, paper archive, and status page publishes a feed.

The engine already contains almost every needed primitive: pipelines can `INSERT`, `chunk()` and `candle()` run inline in SQL, `sqlite_knn`/`sqlite_fts` power hybrid search, aliases expose short verbs, and semantics overlays make tables agent-discoverable. What is missing is the protocol translation — feed XML to relational rows — and the packaging that lets a user get from "the blogs I read" to "my agent can search them" in one conversation.

RSS also completes Skardi's source palette alongside the approved Open Connector integration (`2026-07-11-open-connector-integration-design.md`): Open Connector covers *authenticated SaaS* behind a gateway; RSS covers the *unauthenticated open web*. Both follow the same relational-contract design language; neither needs the other's machinery.

## Goals

- A native `type: rss` source: subscription list in, `feeds` + `items` tables out, zero external processes.
- Live-by-default reads with a per-feed TTL cache and HTTP conditional requests (ETag / Last-Modified).
- Per-feed fault isolation; feed health queryable in SQL, never silent.
- `rss_scan(url)` — a registration-free UDTF for previewing any feed ad hoc.
- `html_to_markdown()` scalar UDF so feed HTML flows into `chunk('markdown', …)` pipelines.
- A compatibility strategy for wild-web feeds: sanitation pre-pass, fixture corpus, documented tolerance floor.
- An `auto_news_base` skill that renders and self-verifies the complete subscription → archive → hybrid-search stack.

## Non-goals

- **No full-article scraping.** Feeds that carry only summaries are served as summaries. Fetching and extracting article HTML is crawler territory (anti-bot, rendering, extraction) and out of scope.
- **No scheduler.** Refresh cadence belongs to the caller (agent, cron, future jobs extension). The TTL cache makes repeated polling cheap; nothing in the provider runs unbidden.
- **No push (WebSub).** Recorded as a future extension; it is a cache-invalidation signal, not a different provider shape.
- **No write path.** `INSERT`/`UPDATE`/`DELETE` against a feed is meaningless; the source registers strictly read-only (`WRITABLE_SOURCE_TYPES` untouched).
- **No authenticated feeds.** Cookie-, token-, or basic-auth-protected feeds are out of scope for v1; if demand appears they belong behind the Open Connector gateway or a scoped follow-up.
- **No history retention in the provider.** The live window is the contract; archiving is a pipeline composition (Decision 4).
- **No gateway.** RSS is unauthenticated public HTTP with an open wire format; the Open Connector gateway adds a moving part and buys nothing (carried over from the probe supplement's analysis).

## Decisions

1. **One source = one subscription list; a single feed is a list of length one.** RSS/Atom is a fixed protocol: every feed has the same schema, differing only by origin. Relational modeling therefore dictates one table with a discriminator column, not N same-shaped tables. Per-feed tables would fragment the primary query ("search all my subscriptions") into `UNION ALL`, force table-name aliasing for unstable/unicode feed titles, and turn every subscription edit into registration churn.
2. **Two fixed tables registered as a catalog: `<name>.main.feeds` and `<name>.main.items`.** Naming mirrors the sqlite catalog convention (`sources/providers/sqlite/mod.rs`) so pipeline SQL is congruent whether it reads the native source or the probe's SQLite layout. Schemas are protocol-pinned (see Components); nothing is discovered at runtime.
3. **Item identity is `(feed, guid)`, with `guid` falling back to `link`.** Declared as primary-key metadata (precedent: source-pack `primary_key` in the Open Connector design). In-place updates to an entry (same `guid`, new `updated`) simply reflect in the live window; versioning is an archive concern.
4. **The provider serves a live window only; history composes via pipelines.** Archiving is the proven anti-join `INSERT … SELECT … WHERE key NOT IN (…)` pattern (`docs/sqlite/pipelines/federated_join_and_insert.yaml`, `demo/rag/cli/pipelines/ingest_chunked.yaml`) from `items` into any writable source. Embedding history in the provider would make it stateful and duplicate what pipelines already do; the Open Connector design reached the same conclusion ("scheduled snapshot materialization" deferred to future extensions).
5. **Scan-time fetch through a per-feed TTL cache with conditional requests.** Three freshness tiers: within TTL → serve cache, zero network; TTL expired + HTTP 304 → header-only revalidation, cache re-armed; HTTP 200 → full parse. Cache entries are per feed, not per scan (partial hits: only expired feeds refetch). The Open Connector completeness invariant is adopted at feed granularity: **only a complete, successfully parsed feed window is ever cached** — a half-parsed feed is never served. `ttl_seconds: 0` means always-live.
6. **Each feed is a DataFusion execution partition.** The scan plan exposes one partition per subscription: parallel fetching without a bespoke concurrency pool, streaming batches (fast feeds emit before slow ones), and a natural failure-isolation boundary that aligns with Decision 7. This is the one structural upgrade over the single-partition `MemoryStream` in `sources/providers/documents/table.rs`.
7. **Multi-feed scans degrade per feed, visibly; the single-feed UDTF fails fast.** The Open Connector rule — "failed pages fail the scan; no partial result is returned as a successful query" — protects agents from silently truncated results. For a multi-feed scan the completeness granularity moves to the feed: a failing feed serves its stale cached window when one exists (marked `stale-error`) or contributes zero rows, `feeds.last_status`/`last_error` record the failure queryably, and tracing emits a warning. Nothing is silent. `rss_scan(url)` targets a single origin, so the all-or-nothing rule applies unchanged: it fails with a targeted error.
8. **Typed `rss:` configuration block, not the flat options map.** Precedent: the approved Open Connector design adds a typed `open_connector` field to `DataSource` (`crates/server/src/config.rs:103`) because `options: HashMap<String,String>` cannot safely express nested config. The `rss:` block is valid only for `type: rss` and carries the subscription list (inline or OPML path), TTL, and politeness bounds.
9. **The subscription list is configuration, not data.** Registration is a configuration action, not a SQL action (Open Connector design: "no SQL statement can add, alter, or remove a stable table"), preserving the SQL validator's no-DDL invariant (`crates/skardi/src/sources/sql_validator.rs`). Agents manage subscriptions by editing `ctx.yaml`/OPML and reloading — a flow the `auto_news_base` skill owns.
10. **Parser is `feed-rs` behind an `rss` Cargo feature, hardened by a sanitation pre-pass and a fixture corpus.** See Compatibility Strategy. The feature gate follows the `documents` precedent (`crates/skardi/Cargo.toml` `[features]`).
11. **Content is stored wire-faithful (HTML); `html_to_markdown()` is a separate scalar UDF.** The provider does not transform content — transformation is a query-time choice. The UDF is registered alongside `chunk()` (`crates/skardi/src/model/`) because it is useful beyond RSS (any HTML-bearing source), and it closes the type gap to `chunk('markdown', …)` (`docs/chunk.md`: only `'character'`/`'markdown'` modes exist).
12. **Namespaced extensions collapse into `extensions_json`; enclosures get typed columns.** Stable schema covers the RSS/Atom core plus enclosures (`enclosure_url`/`enclosure_type`/`enclosure_length` — this unlocks podcast/media subscriptions at negligible cost). Everything else (dc:, media:, itunes:, …) is preserved as a JSON string, following the Open Connector rule for non-stable values. Timestamps are typed `Timestamp(Millisecond, UTC)`, categories are `List<Utf8>`.
13. **`rss_scan(url)` shares the same internal scan machinery as the catalog tables.** The Open Connector dual-interface pattern: persistent tables for repeated queries and joins, a UDTF for ad-hoc exploration, both compiling to one scan path. Primary consumer: the skill's subscribe-time preview.
14. **Registration performs zero network I/O.** Startup validates configuration only (URL syntax, bounds, OPML readability). Probing dozens of feeds at boot would make startup slow and brittle; the first scan — or an explicit `rss_scan` — pays the network cost. (Deliberate deviation from Open Connector's registration-time gateway verification: a gateway is one hop, subscriptions are many.)
15. **Filter pushdown: `Exact` on `feed`/`feed_url` equality and `IN`; everything else stays in DataFusion.** Feed predicates prune partitions — `WHERE feed = 'rust-blog'` fetches exactly one feed. `LIMIT` stops launching further fetches once satisfied (documented: items have no global order). No other predicate can reach the wire; RSS has no query parameters.

## Alternatives Considered

### Per-feed tables (table-provider per feed, or a catalog of discovered tables)

Rejected. Discovery-style catalogs (sqlite) exist because table structure is unknown ahead of time; RSS table structure is pinned by the protocol, so there is nothing to discover — only identical schemas repeated N times. See Decision 1 for the costs.

### A generic `type: xml` source

Rejected (carried over from the earlier draft). XML is syntax, not a data contract: a generic XML source cannot declare a fixed schema without a user-authored XPath mapping layer, which is a different and much larger product. RSS/Atom is a protocol with known fields — exactly what a `TableProvider`'s fixed `SchemaRef` wants.

### Riding the Open Connector gateway

Rejected (carried over). RSS is unauthenticated public HTTP; a gateway owns OAuth and credentials that RSS does not have. The two designs share the same relational-contract language — typed config, live-by-default + TTL, completeness invariant, dual interface, bounded execution — without sharing infrastructure.

### Demo-gated phasing (the previous draft's plan)

Repositioned rather than rejected. The earlier draft built a demo first and made the native source conditional on observed friction. The open questions that phasing was meant to answer — fetch/TTL semantics, history stance, error semantics, subscription management — are now answered by this design, largely by adopting the approved Open Connector design language. The one question that genuinely benefits from live evidence, parser tolerance, maps onto a *growing fixture corpus* rather than a go/no-go gate. The probe survives as a supplement: optional, evidence-producing, non-blocking.

### History/archive inside the provider

Rejected. A provider that owns an archive database is stateful, needs retention policy, compaction, and its own dedup — all of which pipelines plus a writable source already provide, governed and inspectable. Decision 4.

### Scan-level all-or-nothing failure for multi-feed scans

Rejected for `items` (adopted for `rss_scan`). One unreachable blog would render a 50-subscription news base unqueryable. Decision 7 keeps the spirit of the rule — no *silent* incompleteness — while moving the granularity to the feed.

## Architecture

```
ctx.yaml ─ rss: {feeds|opml, ttl_seconds, bounds, user_agent}
   │  registration: config validation only, zero network
   ▼
┌─ type: rss source (read-only) ────────────────────────────────┐
│  partition-per-feed ExecutionPlan                             │
│    ├─ partition i: cache fresh? ── yes → emit cached batches  │
│    │                └─ no → conditional GET ──304→ re-arm     │
│    │                          └─200→ sanitize→feed-rs→Arrow   │
│    └─ failure: stale window (marked) or 0 rows + status       │
│  tables: <name>.main.feeds   <name>.main.items                │
└───────────────────────────────────────────────────────────────┘
   │ live window (SQL)                 ▲ rss_scan('<url>') UDTF
   ▼
archive pipeline (anti-join + html_to_markdown() + chunk() + candle() + INSERT)
   ▼
sqlite archive (chunks + fts5/vec0 mirrors + triggers)
   ▼
hybrid search verbs: `skardi sync` / `skardi news "<query>"`

auto_news_base skill: renders ctx.yaml, DDL script, pipelines,
aliases, semantics — then self-verifies the loop end to end
```

Skardi's responsibility boundary now starts at the feed URL. Everything from HTTP to Arrow is engine-guaranteed; everything from archive onward is user-space composition that the skill automates.

## Components

### Source registration

- `DataSourceType::Rss` variant + `as_str()` — `crates/skardi/src/sources/data_source_type.rs`.
- Dispatch arms in `crates/server/src/config.rs` (central `match source.source_type`) and `crates/cli/src/main.rs`. `rss` is added to `CATALOG_SUPPORTED_SOURCES` and deliberately **not** to `WRITABLE_SOURCE_TYPES`.
- Cargo feature `rss = ["dep:feed-rs", …]` — `crates/skardi/Cargo.toml`.
- Typed `rss` field on `DataSource`, valid only for `type: rss` (Decision 8).

```yaml
kind: context
metadata: { name: news-context, version: 1.0.0 }
spec:
  data_sources:
    - name: news
      type: rss
      hierarchy_level: catalog
      rss:
        feeds:
          - url: https://blog.rust-lang.org/feed.xml
            name: rust-blog            # optional; defaults to the URL
          - url: https://this-week-in-rust.org/rss.xml
        # or: opml: subscriptions.opml # mutually exclusive with feeds:
        ttl_seconds: 900               # 0 = always live
        max_concurrent: 6              # fetch parallelism and per-host politeness bound
        request_timeout_seconds: 10
        max_response_bytes: 5242880
        user_agent: "skardi-rss/<version> (+https://github.com/SkardiLabs/skardi)"
```

`user_agent` is not decorative: feed servers routinely ban anonymous clients, and self-identification is long-standing RSS etiquette. The default identifies Skardi and its version; users may override.

### Table schemas

`<name>.main.feeds` — one row per subscription:

| Column | Arrow type | Nullability | Notes |
|---|---|---|---|
| `name` | `Utf8` | not null | configured name; defaults to URL; unique |
| `url` | `Utf8` | not null | subscription URL |
| `title` | `Utf8` | nullable | wire title, once fetched |
| `site_url` | `Utf8` | nullable | feed's HTML alternate |
| `description` | `Utf8` | nullable | |
| `last_fetch` | `Timestamp(ms, UTC)` | nullable | |
| `last_status` | `Utf8` | not null | `never` \| `fresh` \| `revalidated` \| `stale-error` \| `error` |
| `http_status` | `UInt16` | nullable | last HTTP response code |
| `last_error` | `Utf8` | nullable | fetch/parse error, redacted |
| `etag` / `last_modified` | `Utf8` | nullable | conditional-request state |
| `item_count` | `UInt64` | nullable | entries in current window |

`<name>.main.items` — the live union across subscriptions; primary key `(feed, guid)`:

| Column | Arrow type | Nullability | Notes |
|---|---|---|---|
| `feed` | `Utf8` | not null | subscription `name`; pushdown-prunable |
| `feed_url` | `Utf8` | not null | pushdown-prunable |
| `guid` | `Utf8` | not null | `entry.id`, falling back to `link` |
| `title` | `Utf8` | nullable | |
| `link` | `Utf8` | nullable | |
| `author` | `Utf8` | nullable | |
| `published` | `Timestamp(ms, UTC)` | nullable | |
| `updated` | `Timestamp(ms, UTC)` | nullable | |
| `content` | `Utf8` | nullable | wire-faithful HTML (full content when present) |
| `summary` | `Utf8` | nullable | wire-faithful HTML |
| `categories` | `List<Utf8>` | nullable | |
| `enclosure_url` | `Utf8` | nullable | podcast/media support |
| `enclosure_type` | `Utf8` | nullable | MIME type |
| `enclosure_length` | `UInt64` | nullable | bytes |
| `position` | `UInt32` | not null | document order within the feed window |
| `extensions_json` | `Utf8` | nullable | non-core namespaces as JSON |

### Fetch and cache

Bounded everything, scaled down from the Open Connector execution rules: per-request timeout, total scan deadline, response-size cap, bounded retries with jittered backoff honoring `Retry-After` for 429/transient 5xx. Cancellation aborts in-flight requests. Concurrency is capped by `max_concurrent`, which doubles as the per-host politeness bound.

The cache is per-feed, in-memory, bounded (bytes + entries, LRU), behind a small trait so a persistent implementation can be swapped in later without touching the table providers (the Open Connector `ScanCache` seam). An entry stores the parsed Arrow batches, `etag`/`last_modified`, and fetch metadata. Only complete parses are stored (Decision 5).

### Parsing and Compatibility Strategy

`feed-rs` parses RSS 0.9x/1.0/2.0, Atom 0.3/1.0, and JSON Feed 1.0/1.1 — JSON Feed support arrives free. The known risk is the tolerance gap versus Python's `feedparser`, which salvages two decades of malformed feeds (encoding lies, invalid bytes, unescaped entities, HTML soup, truncation). The strategy:

1. **Sanitation pre-pass, on failure only.** A strict parse is attempted first. On failure, a bounded, deterministic sanitation pass runs — encoding sniff (BOM / XML declaration / `encoding_rs`), re-encode to UTF-8, strip control characters, repair naked ampersands — and the parse is retried once. Both attempts and the applied repairs are traced.
2. **Fixture corpus as a regression ratchet.** `providers/rss/fixtures/` holds real-world feed documents: curated wild feeds across dialects plus every failure case harvested from the probe supplement's pain log. Contract tests assert that every fixture either parses or degrades per-feed with a recorded reason — never a panic, never a silent skip. The corpus only grows.
3. **Documented tolerance floor.** Feeds that still fail are visible via `feeds.last_status = 'error'` with `last_error` naming the parse stage. `docs/rss.md` states plainly what Skardi does not salvage.
4. **Evidence loop with the probe.** The supplement's `feedparser` baseline quantifies the residual gap on live feeds. Material gaps extend the sanitation pass; the parser choice itself is revisited only if the gap proves structural rather than case-by-case.

### Execution and pushdown

One partition per subscription (Decision 6). `feed`/`feed_url` equality and `IN` predicates prune partitions before any fetch and are reported `Exact`; all other predicates remain in DataFusion. `LIMIT` prevents launching further partitions once satisfied. A `feeds`-table scan reads cache/state only for feeds within TTL and revalidates the rest — it is the cheap health check.

### `rss_scan` UDTF

```sql
SELECT title, link, published
FROM rss_scan('https://blog.example.com/feed.xml')
LIMIT 10;
```

Returns the `items` schema for a single unregistered feed. Same fetch/sanitize/parse path; default TTL 0 (always live); single-origin, therefore fail-fast (Decision 7). This is the skill's subscribe-time preview and any user's "what is in this feed?" one-liner.

### `html_to_markdown()` UDF

Scalar UDF registered alongside the existing model UDFs (`crates/skardi/src/model/`), backed by a pure-Rust HTML→Markdown crate (selected at implementation; `htmd`/`html2md` class). Property that matters: `chunk('markdown', html_to_markdown(content), …)` is a valid, useful composition. Feature-gated with `rss` initially; usable against any HTML-bearing text column.

### `auto_news_base` skill

Lives in [skardi-skills](https://github.com/SkardiLabs/skardi-skills); this spec defines the contract it renders against. The skill holds no privileged API — everything it produces is plain user-space configuration, so its internals can evolve freely (stable facade).

Flow:

1. **Collect intent** — a natural-language subscription list ("the Rust blog, This Week in Rust, and Simon Willison") or an OPML file.
2. **Autodiscover** — fetch each site's HTML, extract `<link rel="alternate" type="application/rss+xml|atom+xml">`.
3. **Preview** — `rss_scan('<candidate-url>')` per feed; show sample titles; confirm with the user.
4. **Render** — `ctx.yaml` (rss source + sqlite archive), a one-shot DDL script for the archive (chunks table + fts5/vec0 mirrors + triggers, following `demo/llm_wiki/setup.py`; DDL must live outside SQL — the validator rejects it by design), `pipelines/archive_ingest.yaml`, `pipelines/search_hybrid.yaml`, `aliases.yaml` (`sync`, `news` verbs), `semantics.yaml`.
5. **Self-verify** — run `skardi sync`, then `skardi news "<probe query>"`; assert non-empty, citable results (title + link + published); report per-feed health from `SELECT name, last_status, item_count FROM news.main.feeds`.

Subscription management stays a configuration action (Decision 9): the skill edits `ctx.yaml`/OPML and reloads; it never issues DDL.

The rendered ingest pipeline (sketch — final SQL fixed at implementation):

```sql
INSERT INTO archive.main.news_chunks (feed, guid, chunk_idx, chunk_text, embedding, ingested_at)
SELECT feed, guid, chunk_idx, chunk_text,
       vec_to_binary(candle('models/generated/bge-small-en-v1.5', chunk_text)),
       CAST(now() AS VARCHAR)
FROM (
  SELECT i.feed, i.guid,
         (ROW_NUMBER() OVER (PARTITION BY i.feed, i.guid ORDER BY 1) - 1) AS chunk_idx,
         UNNEST(chunk('markdown',
                      html_to_markdown(COALESCE(i.content, i.summary)),
                      {chunk_size}, {overlap})) AS chunk_text
  FROM news.main.items i
  WHERE i.feed || chr(31) || i.guid NOT IN
        (SELECT DISTINCT feed || chr(31) || guid FROM archive.main.news_chunks)
) AS t
```

Notes carried from proven pipelines: the `AS t` wrapper works around DataFusion's INSERT projection validation, explicit `ingested_at` because column defaults do not fire on the INSERT path (both documented in `demo/rag` / `demo/llm_wiki` pipelines), and the composite anti-join uses a `chr(31)` separator (two-column `NOT EXISTS` is evaluated as an alternative at implementation).

## Failure Modes

| Scenario | Behavior |
|---|---|
| Feed down / DNS failure | Partition serves stale cached window (`last_status = 'stale-error'`) or zero rows if never fetched; other partitions unaffected; `feeds` row records error; tracing warns |
| Malformed XML | Strict parse → sanitation pre-pass → retry; success is traced with repairs applied; failure sets `last_status = 'error'`, `last_error` names the parse stage |
| Feed omits `guid` | `link` used as guid; dedup collapses to link identity |
| Response exceeds `max_response_bytes` | That feed's fetch aborts with a targeted error status; never partial-parsed |
| Slow feed | Per-request timeout isolates it; scan deadline bounds the whole query |
| HTTP 304 | Cache re-armed without reparse; `last_status = 'revalidated'` |
| HTTP 429 / transient 5xx | Bounded jittered retries honoring `Retry-After` within the scan deadline |
| `rss_scan` on a bad URL/feed | Fails fast with a targeted HTTP or parse error (single-origin all-or-nothing) |
| Repeated cache misses under `ttl_seconds: 0` | Documented: always-live is a user choice; conditional requests still minimize transfer |
| `LIMIT` satisfied early | Remaining partitions never launch; incomplete scans are never cached |

## Testing Strategy

- **Unit:** typed config parsing/validation (inline vs OPML, bounds), cache keying/TTL/eviction/completeness invariant, sanitation pass determinism, feed-rs → Arrow conversion (nulls, timestamps, categories, enclosures, extensions_json), guid fallback.
- **Fixture corpus contract tests:** every fixture parses or degrades visibly; assertions on row values per dialect (RSS 0.9x/1.0/2.0, Atom 0.3/1.0, JSON Feed); corpus seeded from the probe's pain log.
- **Mock-HTTP integration:** a local server exercises TTL tiers (fresh / 304 / 200), request counting for partition pruning (`WHERE feed = 'x'` → exactly one request), dead-feed isolation, response-size cap, timeout, retry/`Retry-After`, cancellation, zero-network registration.
- **End-to-end:** ctx.yaml registration; `items` × sqlite federated join; the full archive pipeline (`html_to_markdown` → `chunk` → `candle` → INSERT) with rerun-idempotency; `rss_scan` parity with the registered-table schema.
- **Live tests:** opt-in, ignored by default, never in ordinary CI (Open Connector convention).

## Acceptance Criteria

1. A `ctx.yaml` with N subscriptions registers with zero network I/O (mock server observes no requests at startup).
2. `SELECT * FROM news.main.items` fetches all feeds concurrently; `WHERE feed = 'x'` fetches exactly one (verified by mock request counts).
3. Two scans within TTL cause one fetch per feed; after TTL expiry an unchanged feed takes the 304 path with no reparse.
4. With one dead feed among N, `items` returns the other feeds' rows, `feeds.last_status`/`last_error` reflect the failure, and a tracing warning is emitted — nothing silent.
5. `rss_scan(url)` returns the `items` schema without registration and fails fast on a broken feed.
6. Every corpus fixture parses or degrades per-feed with a recorded reason; no fixture panics.
7. An archive pipeline using `html_to_markdown()` + `chunk()` + `candle()` INSERTs into SQLite; rerunning it inserts zero new rows.
8. `items` participates in a federated join with an existing Skardi source.
9. On a clean machine, `auto_news_base` takes a natural-language subscription list to a working news base and its self-verification passes; an unmodified agent session drives `sync`/`news` using only README + `--help`.
10. Timestamps surface as typed Arrow timestamps; enclosures, categories, and `extensions_json` populate per fixtures.

## Rollout

Three milestones, independently reviewable (each gets its own implementation plan):

1. **M1 — provider core.** Enum variant, dispatch, typed config, fetch/cache/sanitize/parse, both tables, partition-per-feed execution, pushdown, fixture corpus + mock-HTTP suite. Acceptance 1–4, 6, 8, 10.
2. **M2 — surfaces.** `rss_scan` UDTF, `html_to_markdown()` UDF, `docs/rss.md`, README supported-sources row. Acceptance 5, 7.
3. **M3 — skill.** `auto_news_base` in skardi-skills, rendering + self-verification, end-to-end validation. Acceptance 9.

The fetcher probe (supplement) may run at any point — before or alongside M1 — to enrich the fixture corpus and record UX evidence. It gates nothing.

## Expected Repository Shape

```
crates/skardi/src/sources/providers/rss/
  mod.rs        # register_rss_tables(), feature-gated
  config.rs     # typed RssConfig: feeds/opml, ttl, bounds, user_agent
  fetch.rs      # HTTP client, conditional GET, retries, bounds
  cache.rs      # per-feed TTL cache behind a swap-friendly trait
  parse.rs      # sanitation pre-pass + feed-rs → Arrow rows
  table.rs      # feeds/items TableProviders (fixed SchemaRef)
  exec.rs       # partition-per-feed ExecutionPlan
  udtf.rs       # rss_scan table function
  fixtures/     # compatibility corpus (tests only)
crates/skardi/src/model/html_markdown.rs   # html_to_markdown() scalar UDF
docs/rss.md
```

Plus the four standard touch-points: `data_source_type.rs` variant, dispatch arms in `crates/server/src/config.rs` and `crates/cli/src/main.rs`, `rss` feature in `crates/skardi/Cargo.toml`, and the typed `rss` field on `DataSource`. The skill lands in the external skardi-skills repository as `auto_news_base/`.

## Documentation Commitments

- README supported-sources table row and architecture mention.
- `docs/rss.md`: configuration reference, freshness/caching semantics, politeness defaults, tolerance floor, `rss_scan` and pipeline examples, troubleshooting.
- Example `ctx.yaml` under `docs/sample_data` or equivalent.
- skardi-skills: `auto_news_base` README with the five-step flow and self-verification contract.

## Future Extensions

- **WebSub (push).** A subscription callback that invalidates cache entries; requires a resident server. The live-window contract is unchanged.
- **Persistent / shared cache.** Behind the existing cache trait; enables serve-stale across restarts.
- **Scheduled snapshot materialization.** When a scheduler primitive exists (jobs extension), the archive pipeline becomes schedulable in-engine.
- **RFC 5005 paged/archived feeds.** Pagination for feeds that publish history; would extend the fetch layer, not the schema.
- **Authenticated feeds.** Behind Open Connector or a scoped credential design.
- **Full-article extraction.** A separate design (crawling, rendering, extraction quality); explicitly not this provider.
