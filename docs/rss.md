# RSS / Atom Feeds (`type: rss`)

> **Build flag:**
> ```bash
> cargo build --release -p skardi-server --features rss
> cargo run -p skardi-cli --features rss -- query --ctx ctx.yaml --sql "..."
> ```
>
> The whole provider is gated behind the `rss` Cargo feature. Without it, a
> context declaring an `rss` source fails registration.

`rss` is a **read-only** Skardi data source that turns a subscription list —
RSS 0.9x/1.0/2.0, Atom, JSON Feed — into two ordinary Arrow-backed DataFusion
tables. One configured source is one subscription list; a single feed is a list
of length one.

Everything downstream of the two tables is user-space SQL. The provider is a
protocol adapter: it fetches, parses, normalizes, and reports health. History,
chunking, embedding, and search compose out of primitives that already ship
(`INSERT … SELECT`, [`chunk()`](chunk.md), `candle()`, sqlite FTS/KNN) — see
[Pipeline examples](#pipeline-examples).

> **Access is read-only.** The subscription list is *configuration*, never
> SQL-mutable data: no statement can add, alter, or remove a subscription, and
> `access_mode: read_write` is rejected at registration. Agents manage
> subscriptions by editing `ctx.yaml` (or the OPML file) and reloading.

---

## Catalog namespace

An `rss` source registers as a **catalog** (`hierarchy_level: catalog` is
required; anything else is rejected). The two tables live beneath the
conventional `main` schema, mirroring the sqlite catalog convention:

```text
<source name>.main.feeds     -- one row per subscription: fetch health
<source name>.main.items     -- the live union of every subscription's window
```

A source named `news` is therefore queried as `news.main.feeds` /
`news.main.items`. This document uses `news` throughout.

**Registration performs no network I/O.** The only I/O at registration is
reading the `opml:` file, when one is configured. Nothing probes a feed, so
startup cost is proportional to the length of the subscription list rather than
to the availability of the hosts on it: fifty subscriptions do not wait on
fifty upstreams to become queryable, and an unreachable host surfaces as that
subscription's `feeds.last_status` instead of failing the whole source. Every
HTTP request happens later, inside an `items` scan.

### Surface version

`feeds` and `items` are a versioned public interface. The current surface
version is **1**, and it is emitted in two places:

- a `tracing::info!` line at registration (`surface_version = 1`), and
- both tables' Arrow schema metadata, under the key
  `skardi.rss.surface_version` — so a client can read the version off a query
  result without access to the log.

The column set evolves *additively* under one version. Removing, renaming, or
retyping a column, tightening nullability, repurposing an enum domain
(`last_status`, `window_status`), or changing `(feed, guid)` identity or window
semantics is a breaking change and bumps the integer.

---

## Configuration

`type: rss` sources carry a typed `rss:` block rather than the flat
`options:` map — a nested subscription list cannot be expressed safely as
`HashMap<String, String>`.

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
        max_concurrent: 6              # fetch parallelism and per-host politeness bound (per process)
        request_timeout_seconds: 10
        scan_timeout_seconds: 60
        max_response_bytes: 5242880
        user_agent: "skardi-rss/<version> (+https://github.com/SkardiLabs/skardi)"
```

A ready-to-run version of this file lives at
[`docs/sample_data/rss_context.yaml`](sample_data/rss_context.yaml).

### Field reference

| Field | Default | Meaning |
|---|---|---|
| `feeds` | — | Inline subscription list. Each entry has a `url` (must be `http`/`https`) and an optional `name`. Mutually exclusive with `opml`; exactly one of the two must be set. |
| `opml` | — | Path to an OPML subscription list. Read once, at registration. Mutually exclusive with `feeds`. |
| `ttl_seconds` | `900` | Per-feed cache TTL. `0` means always-live: every `items` scan revalidates every feed it visits. The only bound where zero is legal. |
| `max_concurrent` | `6` | Feeds fetched concurrently — both the fetch parallelism and the per-host politeness bound. **Per process** (see [Politeness](#politeness-and-bounds)). |
| `request_timeout_seconds` | `10` | Timeout for one feed HTTP request. |
| `scan_timeout_seconds` | `60` | Deadline for one whole scan across every subscribed feed. |
| `max_response_bytes` | `5242880` (5 MiB) | Cap on one **decoded** response body, so a compressed payload cannot inflate past it. |
| `user_agent` | `skardi-rss/<crate version> (+https://github.com/SkardiLabs/skardi)` | Sent on every request. |

Bounds other than `ttl_seconds` must be at least `1`; `user_agent` must be
non-blank. Unknown keys are rejected at both levels — a misspelled
`ttl_secondsss` fails loudly with the offending field named, rather than being
dropped and silently changing the config's meaning.

Subscription **names** must be unique across the resolved list, comparing the
*effective* name (an explicit `name`, else the URL). Two subscriptions may
share a `url` under different names; pruning on that shared URL visits both.

### OPML mode

`opml:` names a file of `<outline xmlUrl="…">` entries. Nesting is irrelevant —
every `outline` carrying an `xmlUrl` becomes a subscription, in document order,
and a grouping outline without one (a folder like `<outline text="Tech">`) is
structure, not a feed, and is skipped. The subscription name is the outline's
`text` attribute, else its `title`, else the URL.

The file is read on the registration path, not during config validation, and
every check the inline form gets is re-applied to what it contains:
non-emptiness, `http`/`https` scheme, name defaulting, and effective-name
uniqueness.

---

## Table schemas

### `<name>.main.feeds`

One row per subscription, always — the table stays *total* over the
subscription list, which is what makes the [absence check](#nothing-came-back)
meaningful. A `feeds` scan is a **pure state read**: it issues zero HTTP
requests at any moment, including immediately after a failure.

| Column | Arrow type | Null | Notes |
|---|---|---|---|
| `name` | `Utf8` | no | Configured name; defaults to the URL; unique. |
| `url` | `Utf8` | no | Subscription URL. |
| `title` | `Utf8` | yes | Wire title, once fetched. |
| `site_url` | `Utf8` | yes | The feed's HTML alternate. |
| `description` | `Utf8` | yes | |
| `last_fetch` | `Timestamp(ms, UTC)` | yes | Time of the last *attempt*, success or not. |
| `last_status` | `Utf8` | no | `never` \| `fresh` \| `revalidated` \| `stale-error` \| `error` |
| `http_status` | `UInt16` | yes | Last HTTP response code; `NULL` when no status was ever observed (e.g. a refused egress target). |
| `last_error` | `Utf8` | yes | Failure reason, bounded and body-free (see [Reading `last_error`](#reading-last_error)). |
| `etag` | `Utf8` | yes | Conditional-request validator. |
| `last_modified` | `Utf8` | yes | Conditional-request validator. |
| `dialect` | `Utf8` | yes | Parsed dialect (see [Conformance](#conformance)). |
| `dialect_declared` | `Utf8` | yes | What the document claimed. |
| `conformance_notes` | `Utf8` | yes | JSON array of note strings; `[]` means clean. |
| `item_count` | `UInt64` | yes | Entries in the current window. |

### `<name>.main.items`

The live union across subscriptions; identity is `(feed, guid)`.

| Column | Arrow type | Null | Notes |
|---|---|---|---|
| `feed` | `Utf8` | no | Subscription `name`; pushdown-prunable. |
| `feed_url` | `Utf8` | no | Pushdown-prunable. |
| `guid` | `Utf8` | no | Stable item identity (see [Field mapping](#field-mapping)). |
| `title` | `Utf8` | yes | |
| `link` | `Utf8` | yes | |
| `author` | `Utf8` | yes | |
| `published` | `Timestamp(ms, UTC)` | yes | |
| `updated` | `Timestamp(ms, UTC)` | yes | |
| `content` | `Utf8` | yes | Markdown, converted at extraction. |
| `summary` | `Utf8` | yes | Markdown, same conversion. |
| `categories` | `List<Utf8>` | yes | |
| `enclosure_url` | `Utf8` | yes | Podcast/media attachment. |
| `enclosure_type` | `Utf8` | yes | MIME type. |
| `enclosure_length` | `UInt64` | yes | Bytes. |
| `position` | `UInt32` | no | Document order within the feed's window. |
| `window_status` | `Utf8` | no | `fresh` \| `revalidated` \| `stale-error` — the serving window's freshness. |
| `extensions_json` | `Utf8` | yes | Non-core fields as JSON (see [the boundary](#what-extensions_json-does-and-does-not-carry)). |

An in-place edit to an entry (same `guid`, new `updated`) simply reflects in the
live window. Versioning is an archive concern, not a provider one.

`window_status` mirrors `feeds.last_status` restricted to the row-serving
states. `never` and `error` produce **zero rows** and therefore cannot appear on
a row at all — those feeds are visible only in `feeds`.

---

## Freshness and caching

Live reads are the default. Each feed independently falls into one of three
tiers on an `items` scan:

| Tier | What happens | `window_status` / `last_status` |
|---|---|---|
| Within TTL | Serve the cached window. Zero network, and no politeness permit taken — there is no side effect to gate. | whatever the last attempt recorded |
| TTL expired, `304 Not Modified` | Header-only revalidation; the cache is re-armed and **not** reparsed. | `revalidated` |
| TTL expired, `200 OK` | Full sanitize → parse → convert; the cached window is replaced. | `fresh` |

Cache entries are per feed, not per scan, so a partial hit refetches only the
expired feeds. Only a **complete, successfully parsed** feed window is ever
cached — a half-parsed feed is never served. Conditional requests still minimize
transfer under `ttl_seconds: 0`.

Every served row carries its window's freshness in-band, so a consumer that
never touches `feeds` still sees when it is reading a stale window. The stamp is
**window-level**: it is identical on every row of one feed within one scan.

### Negative caching and the failure fuse

The TTL re-arms on **every** attempt, not only on success. A failed fetch
records its error state and re-arms the timer, with a shorter fuse:

```text
failure fuse = clamp(ttl_seconds / 4, 30s, 300s)
```

So a dead feed is re-attempted at most once per failure window rather than on
every scan, and `Retry-After` politeness extends across scans instead of
resetting with each one. The floor is 30s **even under `ttl_seconds: 0`** — the
always-live setting does not turn a dead feed into a per-scan hammer.

This is also why reading `feeds` right after a failure is instant: the failure
was recorded, so there is nothing to re-attempt, and `feeds` cannot reach the
fetcher in the first place.

### The cache is process-lifetime state

The window cache — validators included — lives in memory for the life of the
process. A restart empties it, so **the first post-restart scan presents no
validators and re-fetches every feed as a full `200`, never a `304`.** The
conditional-GET savings vanish exactly when a fleet redeploys. They will extend
across restarts only once a persistent cache lands behind the existing cache
trait.

### What caching does not promise

- **No cross-feed consistency.** A multi-feed scan can observe different feeds at
  different freshness. That is visible per row via `window_status` and per feed
  via `feeds.last_fetch`; it is not smoothed over.
- **No in-flight coalescing.** Two concurrent scans that both find the same feed
  expired can both fetch it.
- **No background refresh.** Nothing runs unbidden. Fetches ride on `items`
  scans, so continuity is the caller's cadence — a cron job or scheduled agent
  session running a scan more often than the fastest feed rolls its window. An
  entry that scrolls out between scans is never observed and leaves no in-band
  trace; the symptom is an aging `feeds.last_fetch`.

---

## Politeness and bounds

Every scan is bounded by the per-request timeout, the whole-scan deadline, the
decoded-response cap, and `max_concurrent`.

**`max_concurrent` is per process.** It caps both fetch parallelism and the
per-host request pressure — but each replica counts its own permits
independently, so **N replicas can present one feed host with up to N× this
bound**, and a crash/restart loop the same. Size it against the number of
processes you actually run, not against the number you configured.

A self-identifying `User-Agent` is sent on every request, by default naming the
Skardi version and the project URL. Feed servers routinely ban anonymous
clients, so overriding it with something blank or generic is a way to get
blocked.

**Retries.** `429`, `500`, `502`, `503` and `504` — plus request timeouts and
transport errors — are retried up to 3 attempts per redirect hop, with
exponential backoff plus jitter. A response's `Retry-After` is honored and takes
precedence when longer than the backoff, but it is capped at 10 seconds: an
uncapped `Retry-After` is a one-header denial of service. Waits happen *inside*
the scan, under its deadline. Redirects are followed up to 5 hops, each hop
revalidated against the egress policy.

Any other `4xx` — a `404`, say — is terminal after a single request.

**Terminal, not retried:** a body that exceeds `max_response_bytes` (measured on
the decompressed stream) aborts that feed after a single request, and a refused
egress target never reaches a socket at all.

---

## Field mapping

The normative dialect → unified-schema mapping.

| `items` column | RSS 2.0 | RSS 1.0 (RDF) | Atom 1.0 | JSON Feed 1.x |
|---|---|---|---|---|
| `guid` | `<guid>` → fallback `<link>` | `<link>` — see note | `<id>` | `id` |
| `title` | `<title>` | `<title>` | `<title>` (text/html/xhtml normalized) | `title` |
| `link` | `<link>` | `<link>` | `<link rel="alternate">` (first, else first link) | `url` |
| `author` | `<author>` / `dc:creator` | `dc:creator` | `<author><name>` | `authors[0].name` |
| `published` | `<pubDate>` (RFC 822) | `dc:date` (ISO 8601) | `<published>` (RFC 3339) | `date_published` |
| `updated` | — (extensions) | — | `<updated>` (RFC 3339) | `date_modified` |
| `content` | `content:encoded` | `content:encoded` | `<content>` | `content_html` / `content_text` |
| `summary` | `<description>` | `<description>` | `<summary>` | `summary` |
| `categories` | `<category>*` | `dc:subject*` | `<category term>*` | `tags[]` |
| `enclosure_*` | `<enclosure url/type/length>` | — | `<link rel="enclosure">` | `attachments[0]` |

All date formats normalize to `Timestamp(ms, UTC)` at parse time. Fields a
dialect lacks are simply null — nullability in the schema *is* the
dialect-coverage annotation.

`feed`, `feed_url`, `position`, and `window_status` are **provider-synthesized**,
not wire fields, so they do not appear above. For `content` and `summary` the
table names the wire field the value comes from; HTML-typed values are then
converted to Markdown at extraction, and plain-text values (JSON Feed
`content_text`, Atom `type="text"`) pass through unchanged.

> **RSS 1.0 identity comes from `<link>`, not `rdf:about`.** This is measured,
> not designed: `feed-rs` 2.4 never reads that attribute, so an RSS 1.0 item's
> `guid` is its `<link>`. The corpus fixture gives the two attributes
> *different* values, so a future `feed-rs` that starts mapping `rdf:about`
> fails a test rather than silently changing every archived item's identity.

An entry with **neither an id nor a link** has no usable identity, so it is
dropped rather than given a synthetic one — a random id would give the same item
a new `(feed, guid)` on every scan, breaking window identity and idempotent
archiving. Dropped entries are counted in `conformance_notes` as
`entries-without-identity: <n>`.

---

## Conformance

Two notions of dialect are kept apart on purpose, and disagreement between them
is an observation about the feed, not an error. A feed that deviates still
serves its rows.

**`dialect`** — what `feed-rs` actually parsed, a direct mapping of its
`FeedType`:

```text
rss-0.9x | rss-1.0 | rss-2.0 | atom | json-feed-1.x
```

**`dialect_declared`** — what the document claimed, sniffed lexically from the
root element and version attribute (which still answers on bytes too broken to
parse). It keeps version detail `feed-rs` collapses:

```text
rss-0.9 | rss-0.91 | rss-0.92 | rss-1.0 | rss-2.0
atom-0.3 | atom-1.0
json-feed-1 | json-feed-1.1
unknown:<root element name>
```

Note `atom-0.3` vs `atom-1.0` here against a single `atom` in `dialect` — the
distinction that matters is in `dialect_declared`.

### `conformance_notes`

A JSON array of strings, or `[]` when the document is clean — "parsed with
nothing to note" and "never parsed" (`NULL`) are different states, and the
column distinguishes them. The note shapes produced today:

| Note | Meaning |
|---|---|
| `sanitation: reencoded-to-utf8` | The document was re-encoded to UTF-8 (BOM / XML declaration / encoding sniff). |
| `sanitation: stripped-control-chars` | Control characters illegal in XML were removed. |
| `sanitation: escaped-naked-ampersands` | Bare `&`s that could not open a valid reference were escaped. |
| `content-type-mismatch: served <type>, parsed <family>` | The HTTP `Content-Type` names a different feed family than the parse produced. |
| `missing-required-field: <path>` | A dialect-required field the feed omitted. |
| `entries-without-identity: <n>` | That many entries were dropped for having neither an id nor a link. |

Only sanitation rungs that **actually changed bytes** are recorded, so a
document rescued by ampersand escaping alone does not also claim to have been
re-encoded. Each rung is a byte-level no-op on well-formed input, which is a
contract test rather than an aspiration.

The content-type check only fires for media types that name a family
(`application/rss+xml`, `application/atom+xml`, `application/feed+json`, and
their siblings). Generic types — `text/xml`, `application/xml`,
`application/octet-stream` — carry no opinion and produce no note. The declared
and parsed dialects cannot disagree *in band*: `feed-rs` dispatches its XML
parsers on root element plus version attribute, so a document either parses as
what it declares or fails outright; the mismatch check is therefore against the
Content-Type, not against `dialect_declared`.

Required-field checks currently cover:

| Parsed dialect | Checked |
|---|---|
| `rss-2.0` | `channel/title`, `channel/link`, `channel/description` |
| `atom` | `feed/title`, `feed/updated` |
| `rss-0.9x`, `rss-1.0`, `json-feed-1.x` | none yet — these extend from corpus evidence rather than being guessed |

---

## Tolerance floor

Feeds misdescribe themselves constantly, and a strict parser refusing them is
not useful. Skardi attempts a parse with a bounded, deterministic sanitation
ladder applied first, and records what it took. This section is about what is
*not* rescued — stated plainly, because the failure modes below are mostly quiet
ones.

### Documents with an internal DTD subset are refused outright

A `<!DOCTYPE … [ … ]>` internal subset means the parse is refused before it
starts: `last_status = 'error'`, and `last_error` reads
`parse failed at refused-internal-dtd: internal DTD subset refused`. The guard
runs twice — once on the raw bytes and once again on the sanitized bytes, since
a rung can *reveal* a doctype the first pass could not see (a lowercase
`<!doctype`, a control character splitting the keyword, a UTF-16 document).

**This is defence in depth, not a live rescue.** `feed-rs`'s `quick-xml` backend
does not expand custom entities at the pinned versions, so a billion-laughs
document would not actually inflate today. The guard exists so that a future
dependency change cannot quietly make it inflate.

### A document the ladder cannot rescue

When the ladder is exhausted and the parse still fails, `last_status = 'error'`
and `last_error` reads `parse failed at strict-parse: <reason>`. The reason is
the parser's own structural message (e.g. `unable to parse XML`), never text
lifted out of the document — see [Reading `last_error`](#reading-last_error).

### An Atom 0.3 document parses as an *empty* feed

This one deserves its own paragraph, because a user will otherwise read it as a
bug in Skardi.

An Atom 0.3 document reaches `feed-rs`'s Atom parser by root-element name, but
its namespace (`http://purl.org/atom/ns#`) maps to `NS::Unknown`, so every child
element falls through the parser's Atom match arms and nothing is extracted. The
result is:

- **zero items**,
- **no error** — `last_status = 'fresh'`, not `'error'`,
- `dialect = 'atom'`, `dialect_declared = 'atom-0.3'`,
- and two notes: `missing-required-field: feed/title` and
  `missing-required-field: feed/updated`.

So a user subscribing to an Atom 0.3 feed sees **an empty table, not a
failure**. The diagnosis is in `dialect_declared` and `conformance_notes`. Both
facts are pinned by the fixture corpus; neither is a choice this design made.

### What `extensions_json` does and does not carry

`extensions_json` is **bounded by what the `feed-rs` model exposes**. It is not
a catch-all for arbitrary unknown namespaces: a namespace `feed-rs` does not
model is dropped at parse time and never reaches this provider, so it cannot be
recovered here.

What it does carry, as a compact JSON object with deterministic key order:

- `media` — MediaRSS objects beyond the single content already surfaced as the
  enclosure (each with `url` / `content_type` / `size`, plus the object's
  `title`, `description`, `thumbnails`, `duration_secs` when present),
- `source` — the entry's source feed reference,
- `rights`,
- `language` — including an Atom entry's `xml:lang`.

The column is `NULL` when none of those are present.

### Entries without identity

Covered above under [Field mapping](#field-mapping): dropped, counted, and
reported as a note.

---

## Egress policy (SSRF)

Feed URLs are agent-authored configuration, which makes them
attacker-influenceable input: a prompt-injected agent can add
`http://169.254.169.254/latest/meta-data/` as a "feed" and have the server fetch
cloud instance metadata on its behalf. The fetcher is therefore **default-deny
by destination**, at a single choke point.

| Refused range | Why |
|---|---|
| Loopback (`127.0.0.0/8`, `::1`) | Services bound to localhost are not meant to be reachable from a feed URL. |
| Link-local (`169.254.0.0/16`, `fe80::/10`) | Cloud instance metadata lives at `169.254.169.254`; this is the canonical SSRF target. |
| Private (RFC 1918: `10/8`, `172.16/12`, `192.168/16`) | The deployment's own internal network. |
| CGNAT (`100.64.0.0/10`, RFC 6598) | Carrier-grade NAT shared space — reachable internal addressing on many networks. |
| Unique-local (`fc00::/7`) | The IPv6 analogue of RFC 1918. |
| Unspecified (`0.0.0.0`, `::`) | Resolves to a local interface on most stacks. |
| Multicast, broadcast | Not a feed host under any reading. |
| Documentation (RFC 5737 TEST-NET-1/2/3) | Reserved, never legitimately routable. |

IPv4-mapped (`::ffff:a.b.c.d`), IPv4-compatible (`::a.b.c.d`), and NAT64
well-known-prefix (`64:ff9b::/96`) IPv6 forms are unwrapped to their embedded
IPv4 address and classified as IPv4 — a DNS64/NAT64 gateway synthesizes
`64:ff9b::a9fe:a9fe` for `169.254.169.254`, which is a real routing path rather
than a theoretical one.

Three properties beyond the range list:

- **Redirects are re-checked.** Every hop's target is validated before the next
  request is built, so a public URL that redirects to `10.9.9.9` is refused —
  and the recorded error names the *target*, not the URL that was subscribed.
- **The connection uses the validated IP.** The policy lives inside the
  resolver, so the addresses reqwest connects to are exactly the addresses that
  passed the check. There is no second, independent DNS resolution left for a
  rebinding answer to race.
- **A mixed answer fails the whole lookup.** If one hostname resolves to both a
  public and a private address, the resolution is refused entirely rather than
  narrowed to the address that passed.

A refused subscription degrades exactly like an unreachable one:
`last_status = 'error'`, `http_status` `NULL` (no status was ever observed),
zero rows in `items`, other feeds unaffected. `last_error` reads:

```text
egress blocked: host '192.168.7.7' resolves to private address 192.168.7.7
```

> **There is no opt-in.** No config field, environment variable, or flag reaches
> a private target — production code has exactly one policy constructor. An
> explicit CIDR/host allowlist for intentionally-internal feeds is a recorded
> future extension, not a hidden switch.

---

## Content handling

### The Markdown storage contract

`items.content` and `items.summary` are **Markdown**, converted once at
extraction:

- The conversion is **deterministic** — identical fragment in, byte-identical
  Markdown out. The fixture corpus pins converted output byte-for-byte, so a
  converter upgrade that changes output is a reviewed, visible change.
- The output contains **no raw HTML**. Elements with a Markdown equivalent
  convert (headings, lists, links, emphasis, code, tables, images);
  `<script>`/`<style>` and comments are dropped wholesale; remaining markup is
  reduced to its text content.
- It **never fails a feed**: pathological HTML degrades to text content, not to
  an error.
- Plain-text values pass through untouched.
- **No raw HTML is stored**, and **the source HTML is not retained.**

The last point is a deliberate trade with a real cost: because the wire HTML is
gone, a future better converter **cannot re-render history**. What remains
possible is re-chunking and re-embedding from the stored Markdown — which is why
an archive should keep `content` verbatim (see
[Pipeline examples](#pipeline-examples)).

The chain from wire to storage is: XML transport encoding (entity references,
CDATA) removed at extraction → sanitation repairs, when any ran, already applied
to the XML document and queryable in `conformance_notes` → the extracted HTML
fragment converted to Markdown → stored unaltered. The fidelity claim is
therefore *extraction-plus-conversion faithful*, not wire-faithful: document
structure and text survive, the original markup does not.

### Rendering it

Stored Markdown is inert text, not an executable document — but rendering it is
still a decision about **untrusted input**, because a feed's body is authored by
whoever runs the feed. A consumer that renders the Markdown to HTML should:

1. keep **raw/inline HTML disabled** in the renderer, and
2. **filter link destinations to safe schemes**.

The second is not hypothetical: the conversion preserves a link's destination,
so a feed can write `[click me](javascript:void(0))` and the stored Markdown
will carry that destination. The corpus pins exactly this case. A consumer that
displays the value as plain text has nothing to do.

The XSS surface narrows from "escape arbitrary attacker HTML at every sink" to
those two renderer settings. It does not disappear.

### Feeding it to an LLM

Prompt injection via feed content has no complete fix at any layer today, so the
defense is containment, not prevention: keep the reading agent
**least-privileged** and **gate side effects deterministically**. Skardi
supplies two of those rails — the egress policy caps a hijacked agent's network
reach, and subscription edits are configuration-only and human-visible.

`guid`, `link`, `author`, and `published` are **feed-asserted and unverifiable**.
A `link` pointing at an attacker's page is still a well-formed feed, so
downstream citations inherit the feed's own trust level. What identity *does*
guarantee: it is scoped by the `feed` discriminator, so one feed can neither
collide with another feed's items nor rewrite an already-archived entry by
reusing a `guid`.

---

## Pipeline examples

The live window is the provider's contract. History, chunking and citability are
user-space SQL over it — an ordinary writable sqlite source, ordinary
`INSERT … SELECT`s. Everything below runs in the provider's composition test
suite.

### The archive schema

Two tables. `news_items` keeps one row per entry with `content` exactly as
`items` served it, and is the anti-join target. `news_chunks` is derived and
disposable — it can be dropped and rebuilt from `news_items` alone.

```sql
CREATE TABLE IF NOT EXISTS news_items (
  feed TEXT NOT NULL, guid TEXT NOT NULL, title TEXT, link TEXT, author TEXT,
  published TIMESTAMP, content TEXT, PRIMARY KEY (feed, guid));

CREATE TABLE IF NOT EXISTS news_chunks (
  feed TEXT NOT NULL, guid TEXT NOT NULL, chunk_idx INTEGER NOT NULL,
  chunk_text TEXT NOT NULL, embedding BLOB, ingested_at TIMESTAMP,
  PRIMARY KEY (feed, guid, chunk_idx));
```

### Statement A — append what the archive has not seen

```sql
INSERT INTO archive.main.news_items (feed, guid, title, link, author, published, content)
SELECT i.feed, i.guid, i.title, i.link, i.author, i.published, COALESCE(i.content, i.summary)
FROM news.main.items i
LEFT JOIN archive.main.news_items a ON a.feed = i.feed AND a.guid = i.guid
WHERE a.guid IS NULL;
```

It is the **absence of a matching archive row** — not a timestamp or a
high-water mark — that decides what is new. So a window that rolled backwards, a
feed that re-served an old entry, and a second run five seconds later all add
nothing.

### Statement B — chunk the archived text

```sql
INSERT INTO archive.main.news_chunks (feed, guid, chunk_idx, chunk_text, embedding, ingested_at)
SELECT s.feed, s.guid,
       ROW_NUMBER() OVER (PARTITION BY s.feed, s.guid) - 1 AS chunk_idx,
       s.chunk_text, NULL AS embedding, now() AS ingested_at
FROM (
  SELECT n.feed, n.guid, UNNEST(chunk('markdown', n.content, 1200, 120)) AS chunk_text
  FROM archive.main.news_items n
  LEFT JOIN archive.main.news_chunks e ON e.feed = n.feed AND e.guid = n.guid
  WHERE e.guid IS NULL AND n.content IS NOT NULL
) s;
```

Two things to note:

- It reads `archive.main.news_items`, **never** `news.main.items`. Chunking is a
  pure function of retained content, so it costs no network and survives the
  live window rolling out from under it. Changing the chunk size later is a
  `DELETE FROM news_chunks` plus a re-run with new parameters — no refetch, and
  it works for entries the feed has already dropped.
- DataFusion has no `WITH ORDINALITY`, and the shipped idiom
  ([`docs/chunk.md`](chunk.md), "Inline ingestion") is a plain
  `UNNEST(chunk(...))` subquery — so the chunk index comes from a window
  function. `ROW_NUMBER` with no `ORDER BY` inside the window leaves *which*
  chunk gets which index unspecified; what it guarantees is that the indices are
  a dense `0..n-1` per `(feed, guid)`, which is what makes
  `(feed, guid, chunk_idx)` a usable primary key.

To embed inline, replace `NULL AS embedding` with
`vec_to_binary(candle('models/bge-small-en-v1.5', s.chunk_text))` (requires the
`candle` feature; `vec_to_binary` is what makes the `List<Float32>` storable in
a sqlite `BLOB`).

### The closing health report

A sync run's last statement: the degraded subscriptions, with the reason and the
as-of time.

```sql
SELECT name, last_status, last_error, last_fetch
FROM news.main.feeds
WHERE last_status IN ('error', 'never', 'stale-error')
ORDER BY name;
```

**An empty report means every feed is healthy.** The read is free — `feeds` is
pure state, no fetches — and the report is an observation about the run, not a
gate on it: a degraded feed changes the output, never the exit status, and it
keeps serving its stale window in `items` meanwhile.

### One archive gotcha worth knowing

A sqlite `TIMESTAMP` column comes back through the sqlite provider as Arrow
`Utf8`, so an archived `published` round-trips as a **string**:

```sql
SELECT guid, title, link, published FROM archive.main.news_items WHERE guid = 'news-1';
-- published: '2026-07-20T10:00:00Z'   -- a string, not a Timestamp
```

`ORDER BY published` on the archive is therefore a *string* ordering. That
happens to be correct for the ISO-8601 rendering it stores, but it is not a
timestamp comparison — do not mix it with interval arithmetic or expect it to
survive a different rendering. On `news.main.items`, `published` is a real
`Timestamp(ms, UTC)`.

### Federated joins

`items` joins anything else registered in the same context, on ordinary columns:

```sql
SELECT i.guid, m.tier
FROM news.main.items i
JOIN meta.main.feed_meta m ON m.feed = i.feed
ORDER BY i.guid;
```

Note that a join predicate is not an equality against a literal, so it prunes
nothing — every subscription is fetched. See [Pruning](#pruning-and-what-it-costs).

---

## Pruning and what it costs

`feed` and `feed_url` predicates prune *partitions before any fetch*, which
makes pruning a cost control, not just a plan detail. RSS has no query
parameters, so nothing else can reach the wire; every other predicate stays in
DataFusion.

| Query shape | Feeds fetched |
|---|---|
| `WHERE feed = 'b'` | exactly one |
| `WHERE feed_url = '<url>'` | every subscription using that URL |
| `WHERE feed IN ('a','b','c','d')` (4+ values) | exactly those four |
| `WHERE feed IN ('a','c')` (**fewer than 4**) | **all of them** — see below |
| `WHERE feed = 'a' OR feed = 'c'` | all of them — same reason |
| `WHERE feed IN (…) AND feed = 'b'` | the intersection — one |
| `JOIN … ON m.feed = i.feed` | all of them |
| `LIMIT n` with no `ORDER BY` | as many as it takes to fill `n` — a nondeterministic subset |
| `ORDER BY … LIMIT n` | all of them (Top-K consumes every partition) |

### Current limitation: a short `IN` list does not prune

**`WHERE feed IN (…)` does not prune below four values.** DataFusion's
`ShortenInListSimplifier` rewrites a short `IN` list into a chain of `OR`s
before the predicate reaches the provider, and the provider's filter classifier
does not recognize an `OR` chain, so every subscribed feed is fetched.

The rows are still correct — the rewritten predicate is applied above the scan
by DataFusion. The cost is HTTP requests, not correctness.

The same applies to an `OR` chain written by hand: `WHERE feed = 'a' OR feed =
'c'` is what the rewrite produces, and it prunes nothing either.

This is a **current limitation**, not intended behavior; a decision on closing
it is pending. Until then:

- an `IN` list of **four or more** distinct values prunes to exactly its
  members, and
- **equality on a single feed always prunes**.

So when the fetch cost matters and you want two or three feeds, issue one
single-feed query per feed rather than one `IN` query.

---

## Troubleshooting

### Nothing came back

A subscription that served no rows leaves no trace in `items` — absence has no
in-band carrier in a relational result. `feeds` is the authoritative signal, and
the prescribed check is an anti-join, run alongside any read where completeness
matters:

```sql
SELECT f.name, f.last_status, f.last_error
FROM news.main.feeds f
LEFT JOIN news.main.items i ON i.feed = f.name
WHERE i.feed IS NULL
ORDER BY f.name;
```

The check never fetches, so it is cheap by construction rather than by timing.
**Absence alone is not a verdict** — three different situations produce it, and
`last_status` is what separates them:

| `last_status` | `item_count` | Diagnosis |
|---|---|---|
| `fresh` / `revalidated` | `0` | **Legitimately empty.** The feed was fetched and parsed successfully and simply has no entries right now. Nothing is wrong. (If the feed *does* have entries in a browser, check `dialect_declared` — see [Atom 0.3](#an-atom-03-document-parses-as-an-empty-feed).) |
| `error` | `NULL` | **Dead.** The fetch or parse failed and no window was ever cached, so there is nothing to serve. `last_error` says why. |
| `never` | `NULL` | **Never attempted.** Either no `items` scan has run since registration, or every scan so far pruned this feed away. |

`stale-error` does not normally appear in this check at all: a feed in that
state *has* a cached window and is serving it, so the anti-join does not find
it. It shows up in the [health report](#the-closing-health-report) instead, and
on its rows' `window_status`. A failure never clears a cached window, and it
leaves `item_count` describing that window rather than the failed attempt.

#### The `never` row has a subtlety

Because fetch *and* health refresh both ride on the `items` scan, a query that
prunes also bounds the side effects: an un-launched feed is neither fetched nor
health-refreshed, and its `feeds` observation keeps aging. So `never` does not
only mean "the source was just registered" — it can equally mean "every scan so
far happened to skip this feed".

The shape that bites is a **bare `LIMIT`**:

```sql
SELECT title, link FROM news.main.items LIMIT 20;   -- nondeterministic feed sample
```

Items have no global order, so a `LIMIT` with no `ORDER BY` serves a
nondeterministic subset of feeds — two identical queries can touch different
feeds and do different amounts of work, and the feeds they did not touch stay
`never`. This is requested truncation, not concealed failure: the result's
`feed` column is the scan's coverage manifest. But **completeness-sensitive
reads should not use a bare `LIMIT`** — use `ORDER BY … LIMIT` (a Top-K
consumes every partition) or no `LIMIT` at all.

### A feed's rows look old

Check `window_status` on the rows themselves:

```sql
SELECT feed, window_status, count(*) FROM news.main.items GROUP BY feed, window_status;
```

`stale-error` means the TTL expired, the refetch failed, and the **previous**
window is being served. The rows are real — they are the last complete window
that parsed — but they are not current. `feeds.last_error` and
`feeds.last_fetch` say why and as of when. Everything else in the scan is
unaffected: one dead feed among fifty never fails the scan.

### Reading `last_error`

`last_error` is bounded at 512 characters and **contains no text taken from the
feed body**, with one deliberate exception: a JSON Feed's declared `version`
string when that version is unsupported (an unsupported version is
undiagnosable without it). This is enforced by tests that embed a sentinel in
body content across several document shapes and assert it never surfaces.

By the shape of the message:

| `last_error` starts with | Stage | What to do |
|---|---|---|
| `egress blocked: …` | Egress policy, before connect | The host resolved into a reserved range, or a redirect targeted one. The message names the address and the range. Not a network problem — see [Egress policy](#egress-policy-ssrf). |
| `parse failed at refused-internal-dtd: …` | Pre-parse guard | The document carries an internal DTD subset. Refused by policy; there is no repair path. |
| `parse failed at strict-parse: …` | Parse, ladder exhausted | The document is not recoverable as a feed at these dependency versions. The trailing reason is the parser's structural message. |
| `http status <n>` | Fetch | An HTTP error. `429`/`5xx` were already retried; `4xx` other than `429` is terminal after one request. |
| `request timed out after <n>s` | Fetch | The per-request timeout fired. Distinct from the scan deadline — if you see this, raising `request_timeout_seconds` is the lever, not `scan_timeout_seconds`. |
| `response exceeded <n> bytes` | Fetch | The **decoded** body passed `max_response_bytes`. Terminal, not retried. |
| `too many redirects (limit 5)` | Fetch | The redirect chain was longer than the hop budget. |
| `invalid feed url: …` | Fetch | The URL — or a redirect `Location` — is not a usable `http(s)` URL. |
| `transport error: …` | Fetch | A connection or I/O failure, after retries were exhausted. |
| `revalidated (304) but the cached window had already been evicted …` | Cache | A `304` came back for a window the bounded cache had already evicted. Self-correcting on the next scan — the next attempt refetches unconditionally. |

### A feed serves rows but they are missing fields

Read `conformance_notes` before suspecting the provider:

```sql
SELECT name, dialect, dialect_declared, conformance_notes, item_count
FROM news.main.feeds
WHERE conformance_notes <> '[]'
ORDER BY name;
```

`missing-required-field` entries mean the feed omitted something its own dialect
requires. `entries-without-identity: <n>` means that many entries were dropped
for having neither an id nor a link. `content-type-mismatch` is usually
harmless — a misconfigured feed host, not a broken feed.

### Every feed refetched after a restart

Expected. The cache is process-lifetime state, so the first scan after a restart
is a full `200` for every feed it visits — see
[The cache is process-lifetime state](#the-cache-is-process-lifetime-state).

### A feed host is rate-limiting us

`max_concurrent` is per process. If you run N replicas, that host sees up to N×
the bound you configured — see [Politeness](#politeness-and-bounds).

---

## Next

- **[Text chunking](chunk.md)** — the `chunk()` UDF used in the ingest pipeline.
- **[Catalog semantics](semantics.md)** — how the bundled overlay at
  [`docs/rss/semantics.yaml`](rss/semantics.yaml) is discovered and merged.
- **[Catalog mode](catalog.md)** — the `hierarchy_level: catalog` registration shape.
- **[Federated queries](federated-queries.md)** — joining `items` against other sources.
