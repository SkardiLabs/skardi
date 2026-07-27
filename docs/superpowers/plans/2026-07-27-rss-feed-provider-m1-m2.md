# RSS Feed Provider (M1 + M2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `type: rss` as a first-class read-only data source — one configured subscription list exposed as `<name>.main.feeds` + `<name>.main.items` with scan-time fetching, per-feed TTL cache, dialect conformance, Markdown item content, per-feed fault isolation — plus the M2 documentation surfaces.

**Spec:** `docs/superpowers/specs/2026-07-22-rss-feed-support-design-v2.md` (normative). This plan implements milestones M1 (provider core) and M2 (docs). M3 (auto_news_base skill, statement-sequence pipeline extension, `requires:` handshake, vendored consumer render) is a separate follow-up plan.

**Architecture:** A self-contained provider under `crates/skardi/src/sources/providers/rss/`. A shared `RssEngine` (config + fetcher + cache + politeness semaphore) is built once at registration; two fixed-schema `TableProvider`s dispatch into a shared `RssScanExec` with one DataFusion partition per feed. `items` scans drive fetch/parse/convert through the cache; `feeds` scans are pure state reads. Everything is modeled on the `open_connector` provider (typed config, hand-rolled mock HTTP test server, inline `#[cfg(test)]` modules) and the `documents` provider (Cargo feature gating).

**Tech Stack:** Rust, DataFusion 52 / Arrow 57, reqwest 0.12 (existing hard dep), tokio. New optional deps behind the `rss` feature: `feed-rs` 2.x, `htmd` (HTML→Markdown), `encoding_rs`, `quick-xml` (OPML only). No mock-HTTP crate — hand-rolled test server per repo convention.

## Global Constraints

Copied from the spec; every task's requirements implicitly include these.

- **Read-only, catalog-only.** `Rss` joins `CATALOG_SUPPORTED_SOURCES`, never `WRITABLE_SOURCE_TYPES`. `register_rss_tables` rejects `read_write` and non-`Catalog` hierarchy (mirror `open_connector/mod.rs:132-153`).
- **Zero network I/O at registration.** Registration validates config (URL syntax, bounds, OPML readability) only.
- **Tables:** `<name>.main.feeds` (15 columns) and `<name>.main.items` (17 columns), exact schemas in Task 9. Column set/types/nullability are surface v1 — `RSS_SURFACE_VERSION: u32 = 1`, logged at registration and carried in Arrow schema metadata key `skardi.rss.surface_version`.
- **Enum domains (exact strings):** `feeds.last_status` ∈ `never | fresh | revalidated | stale-error | error`; `items.window_status` ∈ `fresh | revalidated | stale-error`; `feeds.dialect` ∈ `rss-0.9x | rss-1.0 | rss-2.0 | atom | json-feed-1.x` (direct `feed-rs` `FeedType` mapping).
- **Pushdown:** only `feed`/`feed_url` equality and `IN` on `items`, reported `Exact`; everything else `Unsupported`. Pruned partitions are neither fetched nor health-refreshed. `LIMIT` stops *launching* fetches once satisfied.
- **Cache invariants:** only complete, successfully parsed feed windows are stored; the TTL re-arms on *every* attempt (success, 304, failure — negative caching); a feed's health observation survives window eviction; `feeds` scans never fetch.
- **Egress (SSRF) default-deny:** refuse loopback, link-local (incl. `169.254.169.254`), RFC 1918 private, CGNAT `100.64/10`, unique-local `fc00::/7` (plus other non-globally-routable: unspecified, multicast, broadcast, and their IPv4-mapped-IPv6 forms). Re-check every redirect hop; connect only to validated IPs (rebinding-safe). No production opt-in.
- **Markdown conversion:** deterministic (identical input → byte-identical output), applied to HTML-typed `content`/`summary` only; plain-text passes through unchanged; output contains no raw HTML (`<script>`/`<style>` dropped wholesale; markup without a Markdown equivalent reduced to its text content); never fails a feed.
- **Parse-time DoS bounds:** response-size cap measured on the *decompressed* body; documents with an internal DTD subset are refused (entity-expansion class); no custom entity expansion ever.
- **Sanitation is conservative by contract:** each repair rung is a byte-level no-op on well-formed input; rungs apply cumulatively; the ladder stops at the first rung that parses; applied rungs are recorded in `conformance_notes`.
- **Error redaction:** `feeds.last_error` and log lines never contain response-body content; cap stored error strings at 512 chars. Feed URLs are safe to log; bodies are not.
- **Config defaults (exact values):** `ttl_seconds: 900`, `max_concurrent: 6`, `request_timeout_seconds: 10`, `scan_timeout_seconds: 60`, `max_response_bytes: 5242880`, `user_agent: "skardi-rss/<CARGO_PKG_VERSION> (+https://github.com/SkardiLabs/skardi)"`. `ttl_seconds: 0` is valid (always-live). All bounds ≥ 1 except `ttl_seconds` (0 allowed).
- **Failure fuse:** on a failed attempt the TTL re-arms to `clamp(ttl_seconds / 4, 30, 300)` seconds — bounded above zero even under `ttl_seconds: 0`.
- **Every commit compiles and its tests pass** with `--features rss` and without (the workspace must stay green when the feature is off). Run `cargo fmt --all` before every commit (CI gates on it).
- **Negative tests assert the reason, not just the failure.** Every test that expects an error asserts on a substring identifying *that* error (the offending field name, the failing stage, the blocked range). A bare `assert!(result.is_err())` passes when an unrelated error fires and is treated as a test that asserts nothing — the bar `open_connector/config.rs:607-619` sets.
- **Toolchain note (this machine):** `cargo` is not on the default PATH. Prefix every invocation with `export PATH="/opt/homebrew/opt/rustup/bin:$PATH"`.

### Decisions made by this plan (approved with the plan; deviations from spec flagged)

1. **HTML→Markdown crate: `htmd`** (pure Rust, deterministic, per-element handler overrides), selected per the spec's "selected at implementation". Task 6 contains a fixture bake-off step; if `htmd` cannot satisfy the pinned contract (esp. table conversion), fall back to `html2md` and record the choice in the task's commit message. Either way the fixture corpus pins output byte-for-byte, so the contract binds the crate.
2. **`scan_timeout_seconds` config field (default 60).** The spec's Fetcher section mandates a "total scan deadline" bound but the example YAML omits the knob; this plan adds it as a typed field mirroring `open_connector`'s `scan_timeout_seconds`.
3. **Spec's example values are the defaults** (ttl 900, concurrency 6, timeout 10, cap 5 MiB).
4. **`extensions_json` is bounded by the `feed-rs` model.** `feed-rs` does not retain arbitrary unknown namespaces; the column carries what the model exposes beyond the pinned columns (`media` beyond the first enclosure, `source`, `rights`, `language`), `NULL` when empty. Documented as a tolerance-floor item in `docs/rss.md`.
5. **Entries with neither `id` nor `link` are skipped**, counted in a `entries-without-identity: <n>` conformance note (a `(feed, guid)` key cannot be null).
6. **Billion-laughs handling = refuse internal DTD subsets.** Any XML document containing `<!DOCTYPE … [` is refused at the parse stage (`last_error` names it) — satisfying "rejected rather than expanded" deterministically. `feed-rs`/`quick-xml` never expands custom entities regardless; a fixture pins that too.
7. **Egress test injection:** mock servers bind `127.0.0.1`, which the real policy blocks. `EgressPolicy::allowing_loopback_for_tests()` is a `pub(crate)` constructor used only by tests; production construction has exactly one path (`default_deny()`), no config surface.
8. **E2E embedding:** repo precedent is that `candle()` never executes in default CI (composition tests stop at planning; no model on disk). The default-run e2e exercises the full archive composition with `chunk('markdown')` and a NULL embedding column; a `#[ignore]` variant runs the real `chunk + candle` INSERT reading a model dir from `SKARDI_TEST_EMBED_MODEL`. This narrows the spec's e2e wording to what CI can execute; AC6 proper lands in M3 against the vendored render.
9. **`rss` feature is not added to any `default` feature list** (CI runs `--all-features`). One-line follow-up if wanted.
10. **File layout is finer-grained than the spec sketch** (spec: "directional rather than a filename mandate"): `parse.rs` is split into `sanitize.rs`, `conformance.rs`, `parse.rs`; egress lives in `egress.rs`; Arrow building in `schema.rs`; the engine state machine in `engine.rs`. The spec's boundaries (HTTP, caching, parsing/conformance, DataFusion integration) each remain independently testable.
11. **OPML paths** resolve as given (absolute, or relative to the process CWD) — same behavior as `data_sources[].path` today.
12. **`RssConfig` compiles unconditionally** (plain serde types, no heavy deps) so the server/CLI typed field and validation exist without the feature; everything that touches feed-rs/htmd/reqwest-internals is `#[cfg(feature = "rss")]`. A `type: rss` source registered in a build without the feature fails with a clear "requires the `rss` feature" error (the `documents` pattern).
13. **Observability is tracing-only.** The spec's Observability section says "tracing fields and metrics"; no existing provider emits OTel metrics (that layer exists only in the server's pipeline handlers). This plan implements the full structured-tracing field set per scan; dedicated metrics are left to a cross-provider follow-up rather than an RSS-only precedent.

## File Structure

```text
crates/skardi/src/sources/providers/rss/
├── mod.rs          # module wiring; register_rss_tables(); RSS_SURFACE_VERSION       (Task 14)
├── config.rs       # RssConfig + FeedSubscription + validate()   [unconditional]     (Task 1)
├── error.rs        # RssError (thiserror)                        [unconditional]     (Task 1)
├── opml.rs         # OPML → Vec<ResolvedSubscription>; subscription resolution       (Task 2)
├── egress.rs       # EgressPolicy: IP-range checks + policy-enforcing DNS resolver   (Task 3)
├── testutil.rs     # #[cfg(test)] MockFeedServer (hand-rolled, byte bodies, headers) (Task 4)
├── fetch.rs        # FeedFetcher: conditional GET, retries, caps, manual redirects   (Task 4)
├── sanitize.rs     # sanitation ladder rungs + DTD refusal + family detection        (Task 5)
├── convert.rs      # html_to_markdown(): deterministic, no raw HTML in output        (Task 6)
├── conformance.rs  # declared-dialect sniffer, parsed-dialect map, note strings      (Task 7)
├── parse.rs        # ladder driver + feed-rs orchestration + field extraction        (Tasks 7-8)
├── schema.rs       # feeds/items SchemaRefs + batch builders + window_status swap    (Task 9)
├── cache.rs        # FeedCache trait + MemoryFeedCache (TTL, negative cache, LRU)    (Task 10)
├── engine.rs       # RssEngine: freshness state machine, serve_feed(), feeds rows    (Task 11)
├── exec.rs         # RssScanExec: partition-per-feed, LIMIT gating, projection       (Task 12)
├── table.rs        # RssTableProvider (Feeds|Items), pushdown + partition pruning    (Task 13)
└── fixtures/       # corpus: *.xml/*.json + golden/*.md                              (Task 17)

Modified:
crates/skardi/Cargo.toml                        # [features] rss + optional deps      (Tasks 1,2,4,5,6,7)
crates/skardi/src/sources/providers/mod.rs      # pub mod rss;                        (Task 1)
crates/skardi/src/sources/data_source_type.rs   # Rss variant                         (Task 15)
crates/server/Cargo.toml                        # rss = ["skardi/rss"]                (Task 15)
crates/server/src/config.rs                     # DataSource.rss, validation, dispatch arm, CATALOG_SUPPORTED_SOURCES (Task 15)
crates/server/src/pipeline_handlers.rs          # exhaustive path-match arm           (Task 15)
crates/skardi/src/jobs/executor.rs              # resolve_destination read-only arm   (Task 15)
crates/cli/Cargo.toml                           # rss = ["skardi/rss"]                (Task 16)
crates/cli/src/main.rs                          # LocalDataSource.rss, "rss" arm, stray-block guard (Task 16)
README.md                                       # supported-sources row               (Task 20)

New tests / docs:
crates/skardi/tests/rss_composition.rs          # federated join + archive e2e        (Task 19)
docs/rss.md                                     # M2 reference doc                    (Task 20)
docs/sample_data/rss_context.yaml               # example ctx                         (Task 20)
docs/rss/semantics.yaml                         # bundled semantics overlay snippet   (Task 20)
```

Dependency order: Tasks 1→2→3→4 and 5→6→7→8→9 are two mostly-independent chains that join at Task 10 (cache needs schema.rs batches) and Task 11 (engine needs fetch + parse + cache). Tasks 12→13→14 build the DataFusion layer. Tasks 15–16 wire the front-ends. Tasks 17–19 are the corpus/integration/e2e suites. Task 20 is docs.

---

### Task 1: `rss` Cargo feature, module skeleton, typed `RssConfig`

**Files:**
- Modify: `crates/skardi/Cargo.toml` (features block, line 16-31)
- Modify: `crates/skardi/src/sources/providers/mod.rs:16` (add module)
- Create: `crates/skardi/src/sources/providers/rss/mod.rs`
- Create: `crates/skardi/src/sources/providers/rss/config.rs`
- Create: `crates/skardi/src/sources/providers/rss/error.rs`

**Interfaces:**
- Consumes: nothing (first task).
- Produces: `RssConfig { feeds: Option<Vec<FeedSubscription>>, opml: Option<PathBuf>, ttl_seconds: u64, max_concurrent: usize, request_timeout_seconds: u64, scan_timeout_seconds: u64, max_response_bytes: u64, user_agent: String }` with `pub fn validate(&self) -> Result<(), RssError>`; `FeedSubscription { url: String, name: Option<String> }`; `RssError` (thiserror enum). All unconditional (compile without the feature).

- [ ] **Step 1: Write the failing tests** — in `config.rs`'s `#[cfg(test)] mod tests` (model: `open_connector/config.rs`). Test list (all real code in the file; representative bodies shown):

```rust
#[test]
fn minimal_inline_config_parses_with_spec_defaults() {
    let yaml = r#"
feeds:
  - url: https://blog.rust-lang.org/feed.xml
    name: rust-blog
  - url: https://this-week-in-rust.org/rss.xml
"#;
    let config: RssConfig = serde_yaml::from_str(yaml).unwrap();
    config.validate().unwrap();
    assert_eq!(config.ttl_seconds, 900);
    assert_eq!(config.max_concurrent, 6);
    assert_eq!(config.request_timeout_seconds, 10);
    assert_eq!(config.scan_timeout_seconds, 60);
    assert_eq!(config.max_response_bytes, 5_242_880);
    assert_eq!(
        config.user_agent,
        format!("skardi-rss/{} (+https://github.com/SkardiLabs/skardi)", env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(config.feeds.as_ref().unwrap().len(), 2);
    assert_eq!(config.feeds.as_ref().unwrap()[0].name.as_deref(), Some("rust-blog"));
}

#[test]
fn feeds_and_opml_are_mutually_exclusive() {
    let yaml = "feeds:\n  - url: https://a.example/f.xml\nopml: subs.opml\n";
    let config: RssConfig = serde_yaml::from_str(yaml).unwrap();
    let err = config.validate().unwrap_err();
    assert!(err.to_string().contains("mutually exclusive"), "{err}");
}

#[test]
fn neither_feeds_nor_opml_is_rejected() { /* validate() err contains "one of `feeds` or `opml`" */ }

#[test]
fn empty_inline_feed_list_is_rejected() { /* feeds: [] → err contains "at least one subscription" */ }

#[test]
fn non_http_scheme_is_rejected() {
    // url: file:///etc/passwd → err contains "http or https"
}

#[test]
fn malformed_url_is_rejected() { /* url: "not a url" → err contains "invalid subscription URL" */ }

#[test]
fn duplicate_subscription_names_are_rejected() {
    // two entries with name: same → err contains "duplicate subscription name"
    // and: one named "x", one unnamed with url "x" → also duplicate (name defaults to URL)
}

#[test]
fn zero_bounds_are_rejected_except_ttl() {
    // max_concurrent: 0 / request_timeout_seconds: 0 / scan_timeout_seconds: 0 /
    // max_response_bytes: 0 → each rejected with a message naming the field.
    // ttl_seconds: 0 → validate() Ok (always-live is legal).
}

#[test]
fn empty_user_agent_is_rejected() { /* user_agent: "" → err names user_agent */ }

#[test]
fn unknown_fields_are_rejected() {
    // deny_unknown_fields must fire on both RssConfig and FeedSubscription
    // entries. Assert the error NAMES the offending key — a bare is_err()
    // would also pass if some unrelated parse error fired instead
    // (the bar open_connector/config.rs:607-619 sets).
    let err = serde_yaml::from_str::<RssConfig>("feeds: []\nbogus: 1\n").unwrap_err();
    assert!(err.to_string().contains("bogus"), "{err}");
    let err = serde_yaml::from_str::<RssConfig>(
        "feeds:\n  - url: https://a.example/f.xml\n    nam: x\n",
    )
    .unwrap_err();
    assert!(err.to_string().contains("nam"), "{err}");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p skardi rss::config`
Expected: compile error (module does not exist) — that is the failing state for a new module.

- [ ] **Step 3: Implement.** `error.rs`:

```rust
//! Error taxonomy for the RSS provider.
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RssError {
    #[error("rss source '{name}': {reason}")]
    InvalidConfig { name: String, reason: String },
    #[error("rss source '{name}': hierarchy_level must be 'catalog' (one source is one catalog exposing main.feeds and main.items)")]
    CatalogHierarchyRequired { name: String },
    #[error("rss source '{name}': access_mode must be read-only (the subscription list is configuration, not SQL-mutable data)")]
    ReadWriteNotSupported { name: String },
    #[error("rss source '{name}': missing required `rss:` configuration block")]
    MissingConfig { name: String },
    #[error("rss source '{name}': failed to read OPML file '{path}': {reason}")]
    OpmlUnreadable { name: String, path: String, reason: String },
}
```

(`name` is filled with the config-level context by callers; `validate()` uses `name: "<config>"` — `register_rss_tables` re-wraps with the source name in Task 14.)

`config.rs`: struct as in Interfaces, `#[derive(Debug, Clone, Deserialize, Serialize)]`, `#[serde(deny_unknown_fields)]` on both structs, `#[serde(default = "…")]` per bound (constants + default fns exactly like `open_connector/config.rs:40-76`). `validate()` performs, in order: exactly-one-of feeds/opml; non-empty inline list; per-subscription `url::Url::parse` + scheme ∈ {http, https}; effective-name uniqueness (`name.clone().unwrap_or(url)`) via `HashSet`; bounds (`max_concurrent >= 1`, `request_timeout_seconds >= 1`, `scan_timeout_seconds >= 1`, `max_response_bytes >= 1`, `!user_agent.trim().is_empty()`). No file I/O, no network.

`mod.rs` (skeleton for now):

```rust
//! RSS/Atom subscriptions as a read-only data source (`type: rss`).
//!
//! See `docs/superpowers/specs/2026-07-22-rss-feed-support-design-v2.md`.
pub mod config;
pub mod error;

pub use config::{FeedSubscription, RssConfig};
pub use error::RssError;

/// Integer version of the `feeds`/`items` public surface. Bumped only by
/// breaking changes (column removal/rename/retype, nullability tightening,
/// enum-domain repurposing, identity/window semantics changes).
pub const RSS_SURFACE_VERSION: u32 = 1;
```

`providers/mod.rs`: add `pub mod rss;` (NOT feature-gated — config/error are unconditional; gated submodules come later). `Cargo.toml` features block gains:

```toml
# rss source connector: RSS/Atom/JSON-Feed subscriptions as queryable tables.
rss = []
```

(deps join this list in the tasks that use them).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p skardi rss::config && cargo check -p skardi --features rss && cargo check -p skardi`
Expected: all config tests PASS; both feature configurations compile.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/skardi/Cargo.toml crates/skardi/src/sources/providers/mod.rs crates/skardi/src/sources/providers/rss/
git commit -m "feat(sources): add typed RssConfig and rss feature skeleton"
```

---

### Task 2: OPML reader and subscription resolution

**Files:**
- Create: `crates/skardi/src/sources/providers/rss/opml.rs`
- Modify: `crates/skardi/src/sources/providers/rss/mod.rs` (gated module + `ResolvedSubscription`)
- Modify: `crates/skardi/Cargo.toml` (quick-xml optional dep)

**Interfaces:**
- Consumes: `RssConfig`, `RssError::{OpmlUnreadable, InvalidConfig}`.
- Produces: `pub struct ResolvedSubscription { pub name: String, pub url: String }` (in `mod.rs`, unconditional); `#[cfg(feature = "rss")] pub fn resolve_subscriptions(name: &str, config: &RssConfig) -> Result<Vec<ResolvedSubscription>, RssError>` (in `opml.rs`, re-exported from `mod.rs`) — inline list or OPML file, names defaulted to URL, uniqueness re-checked post-OPML, every URL scheme-validated.

- [ ] **Step 1: Write the failing tests** (`opml.rs` `#[cfg(test)]`; OPML content written to `tempfile::TempDir`):

```rust
#[tokio::test] // plain #[test] is fine too — resolution is sync; keep #[test]
fn opml_outlines_resolve_to_subscriptions() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("subs.opml");
    std::fs::write(&path, r#"<?xml version="1.0"?>
<opml version="2.0">
  <head><title>subs</title></head>
  <body>
    <outline text="Tech">
      <outline type="rss" text="Rust Blog" xmlUrl="https://blog.rust-lang.org/feed.xml" htmlUrl="https://blog.rust-lang.org/"/>
      <outline title="TWiR" xmlUrl="https://this-week-in-rust.org/rss.xml"/>
    </outline>
    <outline xmlUrl="https://example.com/no-name.xml"/>
  </body>
</opml>"#).unwrap();
    let config = RssConfig { opml: Some(path), ..inline_config(&[]) };
    let subs = resolve_subscriptions("news", &config).unwrap();
    assert_eq!(subs.len(), 3);
    assert_eq!(subs[0], ResolvedSubscription { name: "Rust Blog".into(), url: "https://blog.rust-lang.org/feed.xml".into() });
    assert_eq!(subs[1].name, "TWiR");                          // title attr fallback
    assert_eq!(subs[2].name, "https://example.com/no-name.xml"); // name defaults to URL
}
```

Plus (one-line specs, each a real `#[test]` with exact inputs):
- `inline_feeds_resolve_without_io` — inline config, no OPML file touched; names default to URL.
- `missing_opml_file_is_opml_unreadable` — nonexistent path → `RssError::OpmlUnreadable` naming the path.
- `malformed_opml_is_invalid_config` — `not xml at all` → `InvalidConfig` with reason containing "OPML".
- `opml_without_any_xmlurl_is_rejected` — valid OPML, zero `xmlUrl` outlines → "at least one subscription".
- `duplicate_names_across_opml_rejected` — two outlines with same `text` → "duplicate subscription name".
- `opml_bad_scheme_rejected` — `xmlUrl="ftp://…"` → "http or https".
- Helper `fn inline_config(feeds: &[(&str, Option<&str>)]) -> RssConfig` builds configs for reuse across the whole provider's tests; put it in `config.rs` under `#[cfg(test)] pub(crate)`.

- [ ] **Step 2: Run to verify failure** — `cargo test -p skardi --features rss rss::opml` → compile error (fn missing).

- [ ] **Step 3: Implement.** Add to `Cargo.toml`: `quick-xml = { version = "0.37", optional = true }` (match feed-rs's transitive major at implementation time via `cargo tree -i quick-xml`; adjust version to dedupe) and `rss = ["dep:quick-xml"]`. Reader: `quick_xml::Reader::from_str` event loop over `Start`/`Empty` events named `outline`; collect `xmlUrl` attribute (case-sensitive per OPML spec, but also accept `xmlurl` — real-world OPML varies; note it in a comment); name = `text` attr, else `title` attr, else the URL. Nested outlines need no recursion — the event stream is flat. After collection (or for the inline branch): default names, validate scheme via `url::Url`, enforce uniqueness; wrap file-read errors in `OpmlUnreadable`.

- [ ] **Step 4: Verify** — `cargo test -p skardi --features rss rss:: && cargo check -p skardi` → PASS / compiles featureless.

- [ ] **Step 5: Commit** — `git add -A crates/skardi && git commit -m "feat(sources): resolve rss subscriptions from inline list or OPML"`

---

### Task 3: Egress policy (SSRF guard)

**Files:**
- Create: `crates/skardi/src/sources/providers/rss/egress.rs` (gated: `#[cfg(feature = "rss")] mod egress;` in `mod.rs`)

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `pub enum BlockedRange { Loopback, LinkLocal, Private, Cgnat, UniqueLocal, Unspecified, Multicast, Broadcast, Documentation }` with `pub fn as_str(&self) -> &'static str` (kebab-case: `"loopback"`, `"link-local"`, `"private"`, `"cgnat"`, `"unique-local"`, …).
  - `pub struct EgressPolicy { allow_loopback: bool }` with `pub fn default_deny() -> Self`, `pub(crate) fn allowing_loopback_for_tests() -> Self`, `pub fn check_ip(&self, ip: IpAddr) -> Result<(), BlockedRange>`.
  - `pub struct PolicyDns { policy: Arc<EgressPolicy> }` implementing `reqwest::dns::Resolve`: resolves via `tokio::net::lookup_host((host, 0))`, then **fails the whole lookup if any returned address is blocked** (an attacker-controlled DNS answer mixing public+private must not race), else returns the validated addrs — reqwest then connects only to those (rebinding-safe by construction; pooled connections reuse already-validated sockets).
  - Error type `pub struct EgressBlocked { pub host: String, pub ip: IpAddr, pub range: BlockedRange }` (Display: `egress blocked: host '<host>' resolves to <range> address <ip>`) — this string is what lands in `feeds.last_error`.

- [ ] **Step 1: Write the failing tests** — pure-function table test plus resolver tests with an injected lookup:

```rust
#[test]
fn reserved_ranges_are_refused_and_public_allowed() {
    let policy = EgressPolicy::default_deny();
    let blocked: &[(&str, BlockedRange)] = &[
        ("127.0.0.1", BlockedRange::Loopback),
        ("::1", BlockedRange::Loopback),
        ("169.254.169.254", BlockedRange::LinkLocal),   // cloud metadata
        ("fe80::1", BlockedRange::LinkLocal),
        ("10.0.0.1", BlockedRange::Private),
        ("172.16.0.1", BlockedRange::Private),
        ("192.168.1.1", BlockedRange::Private),
        ("100.64.0.1", BlockedRange::Cgnat),
        ("fc00::1", BlockedRange::UniqueLocal),
        ("fd12:3456::1", BlockedRange::UniqueLocal),
        ("0.0.0.0", BlockedRange::Unspecified),
        ("224.0.0.1", BlockedRange::Multicast),
        ("255.255.255.255", BlockedRange::Broadcast),
        ("::ffff:10.0.0.1", BlockedRange::Private),     // v4-mapped v6 unmapped first
        ("::ffff:127.0.0.1", BlockedRange::Loopback),
    ];
    for (ip, want) in blocked {
        let got = policy.check_ip(ip.parse().unwrap()).unwrap_err();
        assert_eq!(&got, want, "ip {ip}");
    }
    for ip in ["1.1.1.1", "93.184.215.14", "2606:4700:4700::1111"] {
        policy.check_ip(ip.parse().unwrap()).unwrap();
    }
}

#[test]
fn test_policy_allows_loopback_but_still_blocks_private() {
    let policy = EgressPolicy::allowing_loopback_for_tests();
    policy.check_ip("127.0.0.1".parse().unwrap()).unwrap();
    assert!(policy.check_ip("10.0.0.1".parse().unwrap()).is_err());
}

#[tokio::test]
async fn resolver_fails_lookup_when_any_address_is_blocked() {
    // check_host_addrs is the testable core the Resolve impl delegates to:
    // pub(crate) fn check_addrs(policy, host, addrs: Vec<SocketAddr>) -> Result<Vec<SocketAddr>, EgressBlocked>
    let policy = EgressPolicy::default_deny();
    let mixed = vec!["93.184.215.14:0".parse().unwrap(), "10.0.0.5:0".parse().unwrap()];
    let err = check_addrs(&policy, "evil.example", mixed).unwrap_err();
    assert_eq!(err.range, BlockedRange::Private);
    let clean = vec!["93.184.215.14:0".parse().unwrap()];
    assert_eq!(check_addrs(&policy, "ok.example", clean.clone()).unwrap(), clean);
}
```

- [ ] **Step 2: Verify failure** — `cargo test -p skardi --features rss rss::egress` → compile error.

- [ ] **Step 3: Implement.** `check_ip`: unmap `Ipv6Addr::to_ipv4_mapped()` first, then explicit prefix checks (do **not** rely on unstable `IpAddr::is_global`): v4 — `is_loopback`, `is_link_local`, `is_private`, `100.64.0.0/10` (`octets[0]==100 && (octets[1] & 0xC0)==64`), `is_unspecified`, `is_multicast`, `is_broadcast`, `192.0.2.0/24`/`198.51.100.0/24`/`203.0.113.0/24` (Documentation); v6 — `is_loopback`, `is_unspecified`, `is_multicast`, `fe80::/10` (`segments[0] & 0xffc0 == 0xfe80`), `fc00::/7` (`segments[0] & 0xfe00 == 0xfc00`). `allow_loopback` short-circuits only the `Loopback` verdict. `PolicyDns::resolve` boxes `check_addrs(policy, name, lookup_host((name, 0)).await?)`; the `EgressBlocked` error surfaces through reqwest as a connect error whose source chain we downcast in Task 4.

- [ ] **Step 4: Verify pass** — `cargo test -p skardi --features rss rss::egress`.

- [ ] **Step 5: Commit** — `git commit -m "feat(sources): rss default-deny egress policy with rebinding-safe resolver"`

---

### Task 4: Mock feed server + bounded fetcher with conditional GET

**Files:**
- Create: `crates/skardi/src/sources/providers/rss/testutil.rs` (`#[cfg(test)]`)
- Create: `crates/skardi/src/sources/providers/rss/fetch.rs`
- Modify: `crates/skardi/Cargo.toml` (`rss` feature adds `"reqwest/gzip"`)

**Interfaces:**
- Consumes: `EgressPolicy`, `PolicyDns`, `EgressBlocked` (Task 3); `crate::util::http::parse_retry_after`.
- Produces:
  - `testutil::MockFeedServer` — clone of `open_connector/testutil.rs` adapted for feeds: `start(handler) -> Self` where `handler: Fn(&RecordedRequest) -> MockResponse + Send + Sync + 'static`; `url() -> String`; `requests() -> Vec<RecordedRequest>` (`RecordedRequest { method, path, headers }` with `.header(name)` case-insensitive). **Differences from the OC one:** `MockResponse { status: u16, headers: Vec<(String, String)>, body: Vec<u8> }` — byte bodies (gzip fixtures), fully caller-controlled headers (`content-type`, `etag`, `last-modified`, `location`, `retry-after`, `content-encoding`), `MockResponse::xml(body: &str)` and `::status(u16)` conveniences, optional per-response artificial delay `with_delay(Duration)` (timeout tests).
  - `fetch::Validators { pub etag: Option<String>, pub last_modified: Option<String> }`
  - `fetch::FetchOutcome::{ NotModified { http_status: u16 }, Fetched { body: Vec<u8>, http_status: u16, etag: Option<String>, last_modified: Option<String>, content_type: Option<String> } }`
  - `fetch::FetchError::{ Egress(EgressBlocked), TooLarge { limit: u64 }, Timeout { seconds: u64 }, Status { status: u16 }, TooManyRedirects { hops: u32 }, InvalidUrl { reason: String }, Transport { reason: String } }` (thiserror; Display strings are the `last_error` copy — `Status` renders `"http status {status}"`).
  - `fetch::FeedFetcher` — `pub fn new(policy: Arc<EgressPolicy>, request_timeout: Duration, max_response_bytes: u64, user_agent: String) -> Result<Self, RssError>` (builds ONE shared `reqwest::Client` with `.dns_resolver(Arc<PolicyDns>)`, `.redirect(Policy::none())`, `.gzip(true)`, `.timeout(request_timeout)`); `pub async fn fetch(&self, url: &str, validators: Option<&Validators>) -> Result<FetchOutcome, FetchError>`.
  - Constants: `MAX_REDIRECT_HOPS: u32 = 5`, `MAX_ATTEMPTS: u32 = 3`, `RETRY_BASE_BACKOFF_MS: u64 = 250`.

- [ ] **Step 1: Build `testutil.rs` first** (copy `open_connector/testutil.rs`, apply the byte-body/header changes; keep the `Drop`-aborts-accept-loop design and one-request-per-connection). Its own smoke test: start a server whose handler returns `MockResponse::xml("<x/>").with_header("etag", "\"v1\"")`, reqwest-GET it, assert body/headers/`requests()[0].header("user-agent")`.

- [ ] **Step 2: Write the failing fetcher tests** (in `fetch.rs`, all against `MockFeedServer`, all using `EgressPolicy::allowing_loopback_for_tests()`):

```rust
#[tokio::test]
async fn full_fetch_returns_body_and_validators() {
    let server = MockFeedServer::start(|_req| {
        MockResponse::xml("<rss/>")
            .with_header("etag", "\"v1\"")
            .with_header("last-modified", "Mon, 20 Jul 2026 10:00:00 GMT")
    }).await;
    let f = test_fetcher(); // helper: loopback-allowing policy, 2s timeout, 1 MiB cap, "skardi-test" UA
    let out = f.fetch(&format!("{}/feed.xml", server.url()), None).await.unwrap();
    match out {
        FetchOutcome::Fetched { body, http_status, etag, last_modified, content_type } => {
            assert_eq!(body, b"<rss/>");
            assert_eq!(http_status, 200);
            assert_eq!(etag.as_deref(), Some("\"v1\""));
            assert!(last_modified.is_some());
            assert_eq!(content_type.as_deref(), Some("application/xml"));
        }
        other => panic!("expected Fetched, got {other:?}"),
    }
    assert_eq!(server.requests()[0].header("user-agent").as_deref(), Some("skardi-test"));
}

#[tokio::test]
async fn conditional_get_sends_validators_and_maps_304() {
    let server = MockFeedServer::start(|req| {
        if req.header("if-none-match").as_deref() == Some("\"v1\"") { MockResponse::status(304) }
        else { MockResponse::xml("<rss/>") }
    }).await;
    let f = test_fetcher();
    let v = Validators { etag: Some("\"v1\"".into()), last_modified: Some("Mon, 20 Jul 2026 10:00:00 GMT".into()) };
    let out = f.fetch(&format!("{}/f", server.url()), Some(&v)).await.unwrap();
    assert!(matches!(out, FetchOutcome::NotModified { http_status: 304 }));
    let req = &server.requests()[0];
    assert_eq!(req.header("if-none-match").as_deref(), Some("\"v1\""));
    assert_eq!(req.header("if-modified-since").as_deref(), Some("Mon, 20 Jul 2026 10:00:00 GMT"));
}
```

Plus (each a full test in the file):
- `oversized_body_aborts_with_too_large` — 2 MiB body, 1 MiB cap → `FetchError::TooLarge { limit }`; and a **gzip bomb**: `flate2`-free approach — pre-gzip a 4 MiB zero body in the test via a tiny in-test gzip writer? No new dev-dep: store a pre-compressed fixture `fixtures/bomb.xml.gz` (Task 17 adds it; here generate bytes with `MockResponse` from a `const` embedded via `include_bytes!` once Task 17 lands — for THIS task, cover the uncompressed cap; the gzip variant is added in Task 18's integration pass where the fixture exists).
- `redirect_is_followed_and_validated` — 302 with `location: /moved` → second request served, `Fetched` returned; `server.requests().len() == 2`.
- `too_many_redirects_errors` — handler always 302 → `TooManyRedirects { hops: 5 }` after 6 requests… (assert `requests().len() as u32 == MAX_REDIRECT_HOPS + 1`).
- `redirect_to_blocked_range_is_refused_before_connect` — `location: http://10.255.255.1/f` → `FetchError::Egress(e)` with `e.range == BlockedRange::Private`; `requests().len() == 1` (no second connect anywhere).
- `retryable_statuses_retry_with_retry_after` — handler scripted via `AtomicUsize`: 429 + `retry-after: 1`, then 200 → success; `requests().len() == 2`; elapsed ≥ 1s.
- `retries_exhaust_to_status_error` — always 503 → `FetchError::Status { status: 503 }` after `MAX_ATTEMPTS` requests.
- `non_retryable_status_fails_immediately` — 404 → `Status { 404 }`, 1 request.
- `request_timeout_maps_to_timeout_error` — `with_delay(3s)`, 1s timeout → `Timeout`.
- `direct_ip_literal_in_blocked_range_is_refused` — url `http://192.168.0.1:9/f` (test policy blocks private) → `Egress`, zero mock requests (pre-resolution literal check).
- `https_and_http_only` — `ftp://…` → `InvalidUrl`.

- [ ] **Step 3: Verify failure** — `cargo test -p skardi --features rss rss::fetch` → compile error.

- [ ] **Step 4: Implement `fetch.rs`.** Single client at construction (see Interfaces). `fetch()` algorithm:
  1. Parse URL (`url::Url`); scheme allowlist; if host is an IP literal → `policy.check_ip` now.
  2. Redirect loop up to `MAX_REDIRECT_HOPS`: per hop, attempt loop up to `MAX_ATTEMPTS`:
     - Build GET with `If-None-Match`/`If-Modified-Since` (only on the **original** URL's first hop — validators do not follow redirects), send.
     - Connect errors: downcast source chain for `EgressBlocked` (→ `Egress`, non-retryable), `is_timeout()` (→ retryable), else `Transport` (retryable).
     - Status 304 (validators sent) → `NotModified`. Status 3xx with `Location` → resolve relative, next hop (re-enters resolver, IP-literal pre-check again). 429/500/502/503/504 → retryable. Other non-2xx → `Status` (final).
     - Retry wait: `max(parse_retry_after(&resp), backoff)` where backoff = `RETRY_BASE_BACKOFF_MS * 2^attempt` ± 50% jitter (reuse the open_connector client's jitter helper pattern — see `open_connector/client.rs`; if it is private, copy the two-line implementation, do not refactor OC in this task).
     - 2xx → stream body via `resp.bytes_stream()`, accumulating with a running total; exceeding `max_response_bytes` → `TooLarge` (this measures **decompressed** bytes: reqwest's gzip layer decompresses before the stream yields). Capture `etag`/`last-modified`/`content-type` headers first.
  3. Timeout per request comes from the client default; the total scan deadline is enforced by the engine (Task 11), not here.

- [ ] **Step 5: Verify pass** — `cargo test -p skardi --features rss rss::fetch rss::testutil`.

- [ ] **Step 6: Commit** — `git commit -m "feat(sources): rss bounded fetcher with conditional GET, retries, egress enforcement"`

---

### Task 5: Sanitation ladder

**Files:**
- Create: `crates/skardi/src/sources/providers/rss/sanitize.rs`
- Modify: `crates/skardi/Cargo.toml` (`encoding_rs = { version = "0.8", optional = true }`, add to `rss` list)

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `pub enum DocFamily { Xml, Json }`; `pub fn detect_family(bytes: &[u8]) -> DocFamily` (first non-whitespace byte `{` → Json; BOM-tolerant).
  - `pub enum Repair { ReencodedToUtf8, StrippedControlChars, EscapedNakedAmpersands }` with `pub fn note(&self) -> &'static str` → `"sanitation: reencoded-to-utf8"` / `"sanitation: stripped-control-chars"` / `"sanitation: escaped-naked-ampersands"`.
  - `pub fn refuse_internal_dtd(bytes: &[u8]) -> Result<(), String>` — scans (outside quotes/comments) for `<!DOCTYPE` containing `[` before the first non-`<!`/`<?` element; error string `"internal DTD subset refused (entity-expansion guard)"`.
  - Rungs, each `pub fn rung_*(input: &[u8]) -> (Vec<u8>, bool /*changed*/)`: `rung_reencode_utf8` (strip BOM; honor XML-decl `encoding=` via `encoding_rs::Encoding::for_label`, fall back to sniff; rewrite/remove the decl's encoding token when transcoding; already-valid UTF-8 without BOM → unchanged), `rung_strip_control_chars` (remove bytes/chars illegal in XML 1.0: `< 0x20` except tab/LF/CR, plus U+FFFE/U+FFFF), `rung_escape_naked_ampersands` (lexical scanner detailed below).
- Ladder *driving* (rungs applied cumulatively, re-parse after each, stop at first success) lives in `parse.rs` (Task 7), because it interleaves with `feed-rs` parse attempts.

Ampersand scanner (normative for the implementation): a byte state machine tracking `<![CDATA[ … ]]>`, `<!-- … -->`, and `<? … ?>` regions, which pass through untouched. Everywhere else (character data *and* attribute values), an `&` is kept iff it opens a valid reference: `&(amp|lt|gt|apos|quot);`, `&#[0-9]{1,7};`, or `&#x[0-9A-Fa-f]{1,6};`. Any other `&` (naked, or an undefined name like `&nbsp;`) has just the `&` rewritten to `&amp;` — so `&nbsp;` becomes `&amp;nbsp;` and survives into the extracted HTML for the HTML-side converter to interpret.

- [ ] **Step 1: Write the failing tests:**

```rust
#[test]
fn every_rung_is_a_byte_level_noop_on_wellformed_input() {
    // The conservativeness contract (spec AC16): includes CDATA with legal
    // ampersands, predefined entities, and numeric character references.
    let wellformed: &[&str] = &[
        r#"<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>t &amp; u</title></channel></rss>"#,
        r#"<rss version="2.0"><channel><description><![CDATA[a & b && c]]></description></channel></rss>"#,
        r#"<feed xmlns="http://www.w3.org/2005/Atom"><title>&#169; &#x2014; &lt;ok&gt;</title></feed>"#,
        "<!-- a & naked amp in a comment --><rss version=\"2.0\"/>",
        "<?pi with & inside?><rss version=\"2.0\"/>",
    ];
    for doc in wellformed {
        let b = doc.as_bytes();
        for (name, rung) in RUNGS_FOR_TEST {   // [(&str, fn(&[u8]) -> (Vec<u8>, bool)); 3]
            let (out, changed) = rung(b);
            assert!(!changed, "rung {name} changed well-formed doc: {doc}");
            assert_eq!(out, b, "rung {name} output differs on: {doc}");
        }
    }
}

#[test]
fn naked_and_undefined_ampersands_are_escaped_defined_ones_kept() {
    let input  = br#"<x a="M &nbsp; N">Fish & Chips &amp; more &#169;</x>"#;
    let expect = br#"<x a="M &amp;nbsp; N">Fish &amp; Chips &amp; more &#169;</x>"#;
    let (out, changed) = rung_escape_naked_ampersands(input);
    assert!(changed);
    assert_eq!(out, expect);
}

#[test]
fn latin1_bytes_reencode_to_utf8() {
    // decl claims iso-8859-1 and the bytes are: caf<0xE9>
    let mut doc = br#"<?xml version="1.0" encoding="iso-8859-1"?><x>caf"#.to_vec();
    doc.push(0xE9); doc.extend_from_slice(b"</x>");
    let (out, changed) = rung_reencode_utf8(&doc);
    assert!(changed);
    let s = std::str::from_utf8(&out).unwrap();
    assert!(s.contains("café"));
    assert!(!s.contains("iso-8859-1"), "decl encoding token rewritten: {s}");
}
```

Plus: `bom_is_stripped`, `lying_utf8_decl_over_latin1_bytes_is_sniffed` (decl says utf-8, bytes invalid UTF-8 → transcode via sniff, changed=true), `control_chars_stripped_tab_lf_cr_kept` (0x08 removed; `\t\n\r` kept byte-identically), `internal_dtd_subset_refused` (billion-laughs prolog → Err), `plain_doctype_without_subset_not_refused` (`<!DOCTYPE opml>` → Ok), `json_family_detected` (`{"version": …` → Json; leading BOM+whitespace tolerated), `cdata_and_comment_regions_pass_untouched_even_with_naked_amps` (naked `&` inside CDATA → unchanged).

- [ ] **Step 2: Verify failure** — `cargo test -p skardi --features rss rss::sanitize`.

- [ ] **Step 3: Implement** exactly per Interfaces. Determinism note: all three rungs are pure byte transforms — no maps, no randomness.

- [ ] **Step 4: Verify pass.**

- [ ] **Step 5: Commit** — `git commit -m "feat(sources): rss sanitation rungs — reencode, control-strip, lexical ampersand repair"`

---

### Task 6: HTML→Markdown converter

**Files:**
- Create: `crates/skardi/src/sources/providers/rss/convert.rs`
- Modify: `crates/skardi/Cargo.toml` (`htmd = { version = "0.2", optional = true }` — pin the exact current release at implementation; add to `rss` list)

**Interfaces:**
- Consumes: nothing new.
- Produces: `pub fn html_to_markdown(html: &str) -> String` — deterministic; trims trailing whitespace per line and outer whitespace; **never errors** (pathological input degrades to text content, empty string at worst).

- [ ] **Step 1: Write the failing tests** (these are the contract; goldens move into the fixture corpus in Task 17 — here inline asserts):

```rust
#[test]
fn structural_elements_convert_to_markdown() {
    let html = r#"<h2>Title</h2><p>Some <em>emphasis</em> and <strong>bold</strong>.</p>
<ul><li>one</li><li>two</li></ul>
<p><a href="https://example.com/a">link</a> and <img src="https://example.com/i.png" alt="alt text"></p>
<pre><code>let x = 1;</code></pre>"#;
    let md = html_to_markdown(html);
    assert!(md.contains("## Title"), "{md}");
    assert!(md.contains("*emphasis*") || md.contains("_emphasis_"), "{md}");
    assert!(md.contains("**bold**"), "{md}");
    assert!(md.contains("- one") || md.contains("* one"), "{md}");
    assert!(md.contains("[link](https://example.com/a)"), "{md}");
    assert!(md.contains("![alt text](https://example.com/i.png)"), "{md}");
    assert!(md.contains("let x = 1;"), "{md}");
}

#[test]
fn script_and_style_are_dropped_wholesale() {
    let html = r#"<p>keep</p><script>alert("x")</script><style>p{color:red}</style><!-- comment -->"#;
    let md = html_to_markdown(html);
    assert!(md.contains("keep"));
    assert!(!md.contains("alert"), "{md}");
    assert!(!md.contains("color"), "{md}");
    assert!(!md.contains("comment"), "{md}");
}

#[test]
fn unknown_markup_reduces_to_text_content_no_raw_html_survives() {
    let html = r#"<article data-x="1"><custom-widget>inner text</custom-widget><video controls>fallback</video></article>"#;
    let md = html_to_markdown(html);
    assert!(md.contains("inner text"));
    assert!(md.contains("fallback"));
    assert!(!md.contains('<'), "raw HTML survived: {md}");
}

#[test]
fn conversion_is_deterministic() {
    let html = include_str!("fixtures/golden_probe.html"); // small kitchen-sink written in this task
    let a = html_to_markdown(html);
    let b = html_to_markdown(html);
    assert_eq!(a, b);
}

#[test]
fn javascript_href_is_preserved_as_data_not_executed_markup() {
    // The provider stores it; consumers filter schemes (spec: Security/Rendering).
    let md = html_to_markdown(r#"<a href="javascript:alert(1)">x</a>"#);
    assert!(!md.contains('<'));
    // Either a markdown link with the raw scheme, or reduced to text — pin whichever
    // the chosen crate produces in the golden corpus (Task 17); here just no-raw-HTML.
}
```

Plus: `tables_convert_or_reduce_to_text` (assert all cell text present; pipe-table if the crate supports it — pin the actual form), `empty_and_whitespace_input_yield_empty`, `entities_decode` (`&amp;lt;` in HTML → literal `<` **escaped as text** in Markdown, still no raw-HTML tag).

- [ ] **Step 2: Verify failure.**

- [ ] **Step 3: Implement + bake-off.** Wire `htmd` with element overrides: skip-with-no-output for `script`, `style`, `head`, comments; verify its defaults for unknown elements (children still walked → text preserved). Run the test suite; if a contract test cannot be satisfied with `htmd` options (notably tables), swap the dep for `html2md` and re-run — the tests are crate-agnostic on purpose. Record the outcome in the commit body. Post-condition helper `debug_assert!(!md.contains('<'))`? No — legit Markdown can contain `<` as escaped text (e.g. `\<`); rely on the targeted assertions + goldens instead.

- [ ] **Step 4: Verify pass** — `cargo test -p skardi --features rss rss::convert`.

- [ ] **Step 5: Commit** — `git commit -m "feat(sources): deterministic HTML-to-Markdown conversion for rss item content"`

---

### Task 7: Dialect conformance + ladder-driving parse entry

**Files:**
- Create: `crates/skardi/src/sources/providers/rss/conformance.rs`
- Create: `crates/skardi/src/sources/providers/rss/parse.rs` (entry + ladder; extraction filled by Task 8)
- Modify: `crates/skardi/Cargo.toml` (`feed-rs = { version = "2", optional = true }`, add to `rss` list)

**Interfaces:**
- Consumes: sanitize rungs (Task 5).
- Produces (`conformance.rs`):
  - `pub fn sniff_declared_dialect(bytes: &[u8], family: DocFamily) -> Option<String>` — XML: first start element + version attr / namespace → `"rss-2.0" | "rss-0.91" | "rss-0.92" | "rss-0.9" | "rss-1.0"` (root `rdf:RDF`) `| "atom-1.0"` (ns `http://www.w3.org/2005/Atom`) `| "atom-0.3"` (ns `http://purl.org/atom/ns#` or `version="0.3"`) `| "unknown:<root>"`. JSON: `"version"` field suffix → `"json-feed-1" | "json-feed-1.1"`.
  - `pub fn parsed_dialect(t: feed_rs::model::FeedType) -> &'static str` — `RSS0→"rss-0.9x"`, `RSS1→"rss-1.0"`, `RSS2→"rss-2.0"`, `Atom→"atom"`, `JSON→"json-feed-1.x"`.
  - `pub fn content_type_family_note(content_type: Option<&str>, parsed: feed_rs::model::FeedType) -> Option<String>` — mismatch only (e.g. Atom parsed but `application/rss+xml` served) → `"content-type-mismatch: served application/rss+xml, parsed atom"`. Generic types (`text/xml`, `application/xml`, `application/octet-stream`, absent) → no note.
  - `pub fn required_field_notes(parsed: feed_rs::model::FeedType, feed: &feed_rs::model::Feed) -> Vec<String>` — initial set: RSS 2.0 channel `title`/`link`/`description` presence, Atom feed `title`/`updated` presence; format `"missing-required-field: channel/description"`, `"missing-required-field: feed/updated"`. (Extends via the corpus evidence loop.)
- Produces (`parse.rs`):
  - `pub struct ParseFailure { pub stage: &'static str, pub reason: String, pub dialect_declared: Option<String> }` — stages: `"refused-internal-dtd"`, `"strict-parse"` (ladder exhausted; reason = last feed-rs error).
  - `pub struct ParseSuccess { pub feed: feed_rs::model::Feed, pub dialect: &'static str, pub dialect_declared: Option<String>, pub repairs: Vec<Repair> }` (Task 8 turns this into rows; keeping the raw `Feed` here keeps the ladder testable alone).
  - `pub fn parse_with_ladder(bytes: &[u8]) -> Result<ParseSuccess, ParseFailure>` — family-detect; JSON: strict only (rungs are XML repairs; reencode still applies). XML: `refuse_internal_dtd` → strict `feed_rs::parser::parse(bytes)` → rung 1 → reparse → rungs 1+2 → reparse → rungs 1+2+3 → reparse; first success records exactly the rungs whose *cumulative application changed bytes* (`changed == true`) as `repairs`; every attempt traced at `debug` level.

- [ ] **Step 1: Failing tests** (in each module):

```rust
// conformance.rs
#[test]
fn declared_dialects_sniff_from_root_and_version() {
    let cases: &[(&str, &str)] = &[
        (r#"<rss version="2.0"><channel/></rss>"#, "rss-2.0"),
        (r#"<rss version="0.91"><channel/></rss>"#, "rss-0.91"),
        (r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"/>"#, "rss-1.0"),
        (r#"<feed xmlns="http://www.w3.org/2005/Atom"/>"#, "atom-1.0"),
        (r#"<feed version="0.3" xmlns="http://purl.org/atom/ns#"/>"#, "atom-0.3"),
        (r#"<html/>"#, "unknown:html"),
    ];
    for (doc, want) in cases {
        assert_eq!(sniff_declared_dialect(doc.as_bytes(), DocFamily::Xml).as_deref(), Some(*want), "{doc}");
    }
    assert_eq!(
        sniff_declared_dialect(br#"{"version":"https://jsonfeed.org/version/1.1","items":[]}"#, DocFamily::Json).as_deref(),
        Some("json-feed-1.1")
    );
}

// parse.rs
#[test]
fn strict_parse_records_no_repairs() {
    let doc = br#"<rss version="2.0"><channel><title>t</title><link>https://e.com</link><description>d</description></channel></rss>"#;
    let ok = parse_with_ladder(doc).unwrap();
    assert!(ok.repairs.is_empty());
    assert_eq!(ok.dialect, "rss-2.0");
    assert_eq!(ok.dialect_declared.as_deref(), Some("rss-2.0"));
}

#[test]
fn naked_ampersand_document_is_rescued_with_minimal_repair_set() {
    let doc = br#"<rss version="2.0"><channel><title>Fish & Chips</title><link>https://e.com</link><description>d</description></channel></rss>"#;
    let ok = parse_with_ladder(doc).unwrap();
    assert_eq!(ok.repairs, vec![Repair::EscapedNakedAmpersands]); // rungs 1-2 changed nothing → not recorded
    assert_eq!(ok.feed.title.as_ref().unwrap().content, "Fish & Chips");
}

#[test]
fn billion_laughs_is_refused_not_expanded() {
    let doc = br#"<?xml version="1.0"?><!DOCTYPE lolz [<!ENTITY lol "lol"><!ENTITY lol2 "&lol;&lol;">]><rss version="2.0"><channel><title>&lol2;</title></channel></rss>"#;
    let err = parse_with_ladder(doc).unwrap_err();
    assert_eq!(err.stage, "refused-internal-dtd");
}

#[test]
fn hopeless_document_exhausts_ladder_with_strict_parse_stage() {
    let err = parse_with_ladder(b"<rss version=\"2.0\"><channel><title>truncat").unwrap_err();
    assert_eq!(err.stage, "strict-parse");
    assert!(err.dialect_declared.as_deref() == Some("rss-2.0")); // declared sniff still works on garbage
}
```

Plus conformance tests: `content_type_mismatch_notes` (Atom + `application/rss+xml` → the exact note string; `text/xml` → None; `application/atom+xml` + Atom → None), `rss2_missing_description_noted`, `atom_missing_updated_noted`, `parsed_dialect_maps_all_five_feedtypes`.

- [ ] **Step 2: Verify failure.** — `cargo test -p skardi --features rss rss::conformance rss::parse`

- [ ] **Step 3: Implement.** Sniffer uses the same lexical scanning style as `sanitize.rs` (find first `<name` outside comments/PIs; cheap attribute scan for `version=` / `xmlns`). Ladder exactly as in Interfaces. **feed-rs API pin:** `feed_rs::parser::parse(&bytes[..]) -> Result<Feed, ParseFeedError>`; verify against the vendored crate docs (`cargo doc -p feed-rs --no-deps`) that `Feed.feed_type: FeedType` and variants `{Atom, JSON, RSS0, RSS1, RSS2}` hold for the pinned 2.x version — if the model differs, adapt `parsed_dialect` in this task (it is the single mapping point).

- [ ] **Step 4: Verify pass.**

- [ ] **Step 5: Commit** — `git commit -m "feat(sources): rss sanitation-ladder parse driver and dialect conformance checks"`

---

### Task 8: Field extraction (feed-rs → typed rows, Field Mapping table)

**Files:**
- Modify: `crates/skardi/src/sources/providers/rss/parse.rs`

**Interfaces:**
- Consumes: `ParseSuccess` (Task 7), `html_to_markdown` (Task 6).
- Produces:
  - `pub struct FeedMeta { pub title: Option<String>, pub site_url: Option<String>, pub description: Option<String> }`
  - `pub struct ItemRow { pub guid: String, pub title: Option<String>, pub link: Option<String>, pub author: Option<String>, pub published_ms: Option<i64>, pub updated_ms: Option<i64>, pub content: Option<String>, pub summary: Option<String>, pub categories: Vec<String>, pub enclosure_url: Option<String>, pub enclosure_type: Option<String>, pub enclosure_length: Option<u64>, pub extensions_json: Option<String> }`
  - `pub struct ExtractedFeed { pub meta: FeedMeta, pub items: Vec<ItemRow>, pub skipped_without_identity: usize }`
  - `pub fn extract(feed: feed_rs::model::Feed) -> ExtractedFeed`
  - `pub fn parse_feed_document(bytes: &[u8], content_type: Option<&str>) -> Result<ParsedDocument, ParseFailure>` where `pub struct ParsedDocument { pub meta: FeedMeta, pub items: Vec<ItemRow>, pub dialect: &'static str, pub dialect_declared: Option<String>, pub conformance_notes: Vec<String> }` — the one function the engine calls; notes = sanitation repairs (`Repair::note()`) + content-type note + required-field notes + `format!("entries-without-identity: {n}")` when n > 0.

Normative extraction rules (Field Mapping, spec lines 364-381):
- `guid` = `entry.id` if non-empty, else first alternate/plain link, else **skip entry** (counted).
- `title` = `entry.title.map(|t| t.content)` (feed-rs normalizes text/html/xhtml titles to one string; no Markdown pass on titles).
- `link` = first `entry.links` with `rel == None || rel == Some("alternate")`, else first link; store `href` as-is.
- `author` = first `entry.authors` name, empty→None.
- `published_ms`/`updated_ms` = `entry.published`/`entry.updated` → `dt.timestamp_millis()` (feed-rs already normalized RFC 822/ISO 8601/RFC 3339 to `chrono::DateTime<Utc>`).
- `content`: `entry.content.body` when present. Markdown rule: convert iff HTML-typed (`content_type` is `text/html`/`application/xhtml+xml`, or absent-with-html-heuristic **no** — absent counts as HTML for RSS `content:encoded` because feed-rs types it `text/html`; JSON Feed `content_text` arrives as `text/plain` → pass through). Implement as: `mime.subtype() == "html" || mime.suffix() == Some("xml") && mime.subtype() == "xhtml"` → convert; `text/plain` → unchanged.
- `summary`: `entry.summary` same rule (`Text.content_type`).
- `categories` = `entry.categories[].term` (fall back to `label` when term empty), deduped preserving order.
- `enclosure_*` = first `entry.media` object's first `MediaContent` with a `url`: `content_type.map(|m| m.to_string())`, `size`. (feed-rs folds RSS `<enclosure>`, Atom `rel="enclosure"` links, and JSON Feed attachments into `media` — verify on the pinned version in this task; if Atom enclosure links are NOT folded, take them from `entry.links` `rel=="enclosure"` as fallback. The per-dialect fixture assertions in Task 17 are the arbiter.)
- `extensions_json` = compact JSON object with keys present only when non-empty: `"media"` (remaining media objects/fields beyond the first enclosure — url/content_type/size/title/description/thumbnails/duration per object), `"source"`, `"rights"`, `"language"`. All-empty → `None`. Serialize with `serde_json::to_string` over a `BTreeMap` (deterministic key order).
- `FeedMeta`: `title`/`description` = `feed.title/.description` content; `site_url` = first feed link with `rel None/alternate` whose `media_type`/href isn't the feed itself — simply: first `rel None|alternate` link.

- [ ] **Step 1: Failing tests** — one dialect-golden test per family with a small inline document (the full wild corpus is Task 17; these pin the mapping):

```rust
#[test]
fn rss2_maps_per_field_mapping_table() {
    let doc = br#"<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/" xmlns:dc="http://purl.org/dc/elements/1.1/"><channel>
<title>Chan</title><link>https://site.example/</link><description>D</description>
<item>
  <guid>tag:1</guid><title>Post</title><link>https://site.example/p1</link>
  <dc:creator>Ada</dc:creator><pubDate>Mon, 20 Jul 2026 10:00:00 GMT</pubDate>
  <description>&lt;p&gt;Sum &lt;b&gt;bold&lt;/b&gt;&lt;/p&gt;</description>
  <content:encoded><![CDATA[<h1>Body</h1><p>text</p>]]></content:encoded>
  <category>rust</category><category>news</category>
  <enclosure url="https://site.example/e.mp3" type="audio/mpeg" length="123"/>
</item>
<item><title>NoGuid</title><link>https://site.example/p2</link></item>
<item><title>NoIdentity</title></item>
</channel></rss>"#;
    let parsed = parse_feed_document(doc, Some("application/rss+xml")).unwrap();
    assert_eq!(parsed.dialect, "rss-2.0");
    assert_eq!(parsed.items.len(), 2);
    let it = &parsed.items[0];
    assert_eq!(it.guid, "tag:1");
    assert_eq!(it.author.as_deref(), Some("Ada"));
    assert_eq!(it.published_ms, Some(1_784_541_600_000)); // 2026-07-20T10:00:00Z
    assert_eq!(it.content.as_deref(), Some("# Body\n\ntext"));       // Markdown, not HTML
    assert_eq!(it.summary.as_deref(), Some("Sum **bold**"));
    assert_eq!(it.categories, vec!["rust", "news"]);
    assert_eq!(it.enclosure_url.as_deref(), Some("https://site.example/e.mp3"));
    assert_eq!(it.enclosure_type.as_deref(), Some("audio/mpeg"));
    assert_eq!(it.enclosure_length, Some(123));
    assert_eq!(parsed.items[1].guid, "https://site.example/p2");     // link fallback
    assert!(parsed.conformance_notes.iter().any(|n| n == "entries-without-identity: 1"));
    assert_eq!(parsed.meta.title.as_deref(), Some("Chan"));
}
```

(The exact Markdown strings `"# Body\n\ntext"` / `"Sum **bold**"` are pinned against the Task 6 converter — adjust to its actual output once, then they are frozen.)

Plus same-shape tests: `atom10_maps_fields` (id, `<link rel="alternate">` vs bare link, published+updated, `<content type="html">` converted / `<summary type="text">` passthrough, `<category term=>`), `jsonfeed_maps_fields` (`content_text` passthrough verbatim incl. newlines, `content_html` converted, tags, attachments→enclosure, date_modified→updated), `rss1_rdf_maps_fields` (rdf:about identity, dc:creator, dc:date), `plain_text_content_is_never_converted` (JSON `content_text: "a < b & c"` → stored exactly `"a < b & c"`), `extensions_json_carries_media_and_language_or_none` (bare minimal item → `extensions_json: None`).

- [ ] **Step 2: Verify failure.** `cargo test -p skardi --features rss rss::parse`

- [ ] **Step 3: Implement** per the normative rules above.

- [ ] **Step 4: Verify pass.**

- [ ] **Step 5: Commit** — `git commit -m "feat(sources): rss field extraction to unified rows with Markdown content"`

---

### Task 9: Arrow schemas and batch builders

**Files:**
- Create: `crates/skardi/src/sources/providers/rss/schema.rs`

**Interfaces:**
- Consumes: `ItemRow`, `FeedMeta` (Task 8); `RSS_SURFACE_VERSION` (Task 1).
- Produces:
  - `pub fn items_schema() -> SchemaRef` / `pub fn feeds_schema() -> SchemaRef` — `LazyLock<SchemaRef>` clones; both carry metadata `{"skardi.rss.surface_version": "1"}`.
  - `pub const WINDOW_STATUS_IDX: usize = 15;`
  - `pub fn build_items_batch(feed: &str, feed_url: &str, items: &[ItemRow]) -> RecordBatch` — `window_status` filled `"fresh"`; `position` = index as u32.
  - `pub fn with_window_status(batch: &RecordBatch, status: &str) -> RecordBatch` — swaps column 15 for a constant `StringArray` (arrays are `Arc`ed; cheap).
  - `pub struct FeedsRowInput<'a> { pub name: &'a str, pub url: &'a str, pub observation: &'a FeedObservation, pub etag: Option<&'a str>, pub last_modified: Option<&'a str> }` … deferred: `FeedObservation` is Task 10's type. To keep this task self-contained, `build_feeds_batch` takes a flat struct owned here:
    `pub struct FeedsRow { pub name: String, pub url: String, pub title: Option<String>, pub site_url: Option<String>, pub description: Option<String>, pub last_fetch_ms: Option<i64>, pub last_status: &'static str, pub http_status: Option<u16>, pub last_error: Option<String>, pub etag: Option<String>, pub last_modified: Option<String>, pub dialect: Option<String>, pub dialect_declared: Option<String>, pub conformance_notes: Option<String>, pub item_count: Option<u64> }`
    and `pub fn build_feeds_batch(rows: &[FeedsRow]) -> RecordBatch`.

Exact `items` schema (order is the batch shape):

| # | name | type | nullable |
|---|---|---|---|
| 0 | `feed` | `Utf8` | no |
| 1 | `feed_url` | `Utf8` | no |
| 2 | `guid` | `Utf8` | no |
| 3 | `title` | `Utf8` | yes |
| 4 | `link` | `Utf8` | yes |
| 5 | `author` | `Utf8` | yes |
| 6 | `published` | `Timestamp(Millisecond, Some("UTC"))` | yes |
| 7 | `updated` | `Timestamp(Millisecond, Some("UTC"))` | yes |
| 8 | `content` | `Utf8` | yes |
| 9 | `summary` | `Utf8` | yes |
| 10 | `categories` | `List(Field("item", Utf8, true))` | yes |
| 11 | `enclosure_url` | `Utf8` | yes |
| 12 | `enclosure_type` | `Utf8` | yes |
| 13 | `enclosure_length` | `UInt64` | yes |
| 14 | `position` | `UInt32` | no |
| 15 | `window_status` | `Utf8` | no |
| 16 | `extensions_json` | `Utf8` | yes |

Exact `feeds` schema: `name Utf8 !null`, `url Utf8 !null`, `title/site_url/description Utf8 null`, `last_fetch Timestamp(ms, UTC) null`, `last_status Utf8 !null`, `http_status UInt16 null`, `last_error Utf8 null`, `etag Utf8 null`, `last_modified Utf8 null`, `dialect Utf8 null`, `dialect_declared Utf8 null`, `conformance_notes Utf8 null`, `item_count UInt64 null` (15 columns).

- [ ] **Step 1: Failing tests:** `items_schema_matches_spec` (assert every name/type/nullability by index, metadata value `"1"`, field count 17), `feeds_schema_matches_spec` (15 fields, same checks), `items_batch_round_trips_rows` (2-item batch: values line up; `position` 0,1; `window_status` all `"fresh"`; null propagation for None fields; categories list values), `with_window_status_swaps_only_column_15` (swap → other 16 columns pointer-equal (`Arc::ptr_eq` on arrays), col 15 all `"stale-error"`), `feeds_batch_round_trips` (one row, `never` status: everything else null except name/url/last_status), `empty_items_batch_has_zero_rows_17_cols`.

- [ ] **Step 2: Verify failure.** — `cargo test -p skardi --features rss rss::schema`

- [ ] **Step 3: Implement** with plain array builders (`StringBuilder`, `TimestampMillisecondBuilder::with_timezone("UTC")`, `ListBuilder<StringBuilder>`, …). No projection logic here (exec's job).

- [ ] **Step 4: Verify pass.**

- [ ] **Step 5: Commit** — `git commit -m "feat(sources): rss feeds/items Arrow schemas with surface-version metadata"`

---

### Task 10: Per-feed TTL cache

**Files:**
- Create: `crates/skardi/src/sources/providers/rss/cache.rs`

**Interfaces:**
- Consumes: `RecordBatch` (window batches from Task 9).
- Produces:
  - `pub enum FeedStatus { Never, Fresh, Revalidated, StaleError, Error }` + `pub fn as_str(&self) -> &'static str` (`"never" | "fresh" | "revalidated" | "stale-error" | "error"`) + `pub fn window_status_str(&self) -> Option<&'static str>` (`Fresh→"fresh"`, `Revalidated→"revalidated"`, `StaleError→"stale-error"`, else None).
  - `#[derive(Clone)] pub struct FeedObservation { pub last_fetch_ms: Option<i64>, pub last_status: FeedStatus, pub http_status: Option<u16>, pub last_error: Option<String>, pub dialect: Option<String>, pub dialect_declared: Option<String>, pub conformance_notes: Option<String>, pub title: Option<String>, pub site_url: Option<String>, pub description: Option<String>, pub item_count: Option<u64> }` with `Default` = all-None + `Never`.
  - `#[derive(Clone)] pub struct CachedWindow { pub batch: RecordBatch, pub etag: Option<String>, pub last_modified: Option<String> }`
  - `#[derive(Clone)] pub struct FeedSnapshot { pub observation: FeedObservation, pub window: Option<CachedWindow>, pub within_ttl: bool }`
  - `pub trait FeedCache: Send + Sync { fn snapshot(&self, feed: &str, now: Instant) -> FeedSnapshot; fn record_success(&self, feed: &str, window: CachedWindow, observation: FeedObservation, armed_until: Instant); fn record_not_modified(&self, feed: &str, http_status: u16, last_fetch_ms: i64, armed_until: Instant); fn record_failure(&self, feed: &str, http_status: Option<u16>, error: String, last_fetch_ms: i64, armed_until: Instant); }`
    (sync trait, `Mutex` inside — swap-friendly for a persistent impl later since all state flows through these four methods.)
  - `pub struct MemoryFeedCache::new(max_bytes: usize, max_entries: usize) -> Self` — LRU on window bytes (batch `get_array_memory_size()`), touched on `snapshot` hits; eviction drops `window` (batches **and validators**) but **keeps the observation**; `max_entries` guards the map.
  - Cache-internal invariants: `record_failure` sets `StaleError` when a window is present, `Error` when absent; `record_not_modified` on a missing window is a **no-op status-wise plus a debug assertion** (the engine never sends validators without a window, so a 304 without one is unreachable); `record_success` replaces the window and observation wholesale.
  - `pub fn failure_fuse(ttl: Duration) -> Duration` — `clamp(ttl/4, 30s, 300s)`.

- [ ] **Step 1: Failing tests** (deterministic `Instant` handling: pass explicit `now`/`armed_until` — no sleeping):

```rust
#[test]
fn success_arms_ttl_and_snapshot_reports_within_ttl() {
    let cache = MemoryFeedCache::new(1 << 20, 64);
    let t0 = Instant::now();
    cache.record_success("a", window_with_rows(2), obs_fresh(2), t0 + Duration::from_secs(900));
    let snap = cache.snapshot("a", t0 + Duration::from_secs(1));
    assert!(snap.within_ttl);
    assert!(matches!(snap.observation.last_status, FeedStatus::Fresh));
    assert_eq!(snap.window.as_ref().unwrap().batch.num_rows(), 2);
    assert!(!cache.snapshot("a", t0 + Duration::from_secs(901)).within_ttl);
}

#[test]
fn failure_is_negative_cached_with_window_kept() {
    let cache = MemoryFeedCache::new(1 << 20, 64);
    let t0 = Instant::now();
    cache.record_success("a", window_with_rows(2), obs_fresh(2), t0);          // expired immediately
    cache.record_failure("a", Some(503), "http status 503".into(), 1, t0 + Duration::from_secs(30));
    let snap = cache.snapshot("a", t0 + Duration::from_secs(1));
    assert!(snap.within_ttl, "failure re-armed the timer (negative cache)");
    assert!(matches!(snap.observation.last_status, FeedStatus::StaleError));
    assert!(snap.window.is_some(), "stale window retained for serve-stale");
    assert_eq!(snap.observation.last_error.as_deref(), Some("http status 503"));
}

#[test]
fn failure_without_window_is_error_status() { /* record_failure on unknown feed → Error, window None */ }

#[test]
fn eviction_drops_window_and_validators_but_keeps_observation() {
    // max_bytes sized to hold exactly one window; insert two feeds; snapshot("a")
    // after eviction: window None, observation still Fresh-with-item_count.
}

#[test]
fn unknown_feed_snapshot_is_never() { /* Never, no window, not within_ttl */ }

#[test]
fn failure_fuse_is_clamped() {
    assert_eq!(failure_fuse(Duration::from_secs(0)),    Duration::from_secs(30));
    assert_eq!(failure_fuse(Duration::from_secs(900)),  Duration::from_secs(225));
    assert_eq!(failure_fuse(Duration::from_secs(10_000)), Duration::from_secs(300));
}
```

Plus: `not_modified_rearms_and_flips_to_revalidated`, `lru_touch_order_respected` (snapshot("a") between inserts of b, c → c's insert evicts b not a), `max_entries_bound_evicts_lru_window`.

- [ ] **Step 2: Verify failure.** `cargo test -p skardi --features rss rss::cache`

- [ ] **Step 3: Implement** (interior `Mutex<Inner { map: HashMap<String, Entry>, order: VecDeque<String>, window_bytes: usize }>` — hand-rolled LRU exactly like `open_connector/cache.rs`; no `lru` crate).

- [ ] **Step 4: Verify pass.**

- [ ] **Step 5: Commit** — `git commit -m "feat(sources): rss per-feed TTL cache with negative caching and observation-preserving eviction"`

---

### Task 11: Engine — the freshness state machine

**Files:**
- Create: `crates/skardi/src/sources/providers/rss/engine.rs`

**Interfaces:**
- Consumes: everything from Tasks 2–10.
- Produces:
  - `pub struct RssEngine` — `pub fn new(source_name: String, subscriptions: Vec<ResolvedSubscription>, config: &RssConfig, policy: Arc<EgressPolicy>) -> Result<Self, RssError>` (builds `FeedFetcher`, `MemoryFeedCache` (bytes budget: `64 MiB` const `CACHE_MAX_BYTES`, entries: `subscriptions.len() + 8`), `Arc<Semaphore>` of `max_concurrent`, keeps `ttl: Duration`, `scan_timeout: Duration`). For tests: `pub(crate) fn with_parts(…, fetcher: FeedFetcher, cache: Arc<dyn FeedCache>) -> Self`.
  - `pub fn subscriptions(&self) -> &[ResolvedSubscription]`; `pub fn scan_timeout(&self) -> Duration`.
  - `pub async fn serve_feed(&self, feed: &str, launch_gate: impl Fn() -> bool + Send) -> Option<RecordBatch>` — full 17-column batch with correct `window_status`, or `None` for zero rows (never an `Err`: per-feed degradation is data, not scan failure). **`launch_gate` semantics:** a within-TTL cache hit serves regardless of the gate (no side effects to gate); an expired feed re-checks `launch_gate()` *after acquiring the politeness permit and immediately before fetching* — a `false` gate returns `None` without fetching and without touching health state (the feed behaves as LIMIT-pruned: "neither fetched nor health-refreshed"). This post-acquire re-check is load-bearing: DataFusion polls all partitions concurrently, so every partition passes any pre-check while `emitted == 0` and then queues on the semaphore; only a gate evaluated after the permit is acquired can actually stop launches once LIMIT is satisfied.
  - `pub fn feeds_row(&self, feed: &str) -> RecordBatch` — single-row feeds batch from `cache.snapshot` + config (pure state read).

`serve_feed` state machine (normative):
1. `snapshot(feed, now)`. If `within_ttl`: serve `window.map(|w| with_window_status(&w.batch, status))` where status = `observation.last_status.window_status_str()` (`None` → zero rows, i.e. `Never`/`Error`) — zero network, zero permit, gate not consulted.
2. Expired → `let _permit = semaphore.acquire().await` (politeness; released on drop, incl. cancellation) → **if `!launch_gate()` return `None`** (LIMIT filled while waiting; no fetch, no health write) → `fetcher.fetch(url, window validators)`:
   - `Ok(NotModified)` → `record_not_modified(feed, 304, now_ms, now + ttl_arm)` → serve window stamped `"revalidated"`. (`ttl_arm` = `max(ttl, minimum_rearm)` where `minimum_rearm = Duration::ZERO` — under `ttl_seconds: 0` a success arms to now, i.e. always-live, per spec.)
   - `Ok(Fetched { body, .. })` → `parse_feed_document(&body, content_type)`:
     - `Ok(doc)` → `build_items_batch` → `record_success(feed, CachedWindow { batch, etag, last_modified }, observation, now + ttl)` where observation carries: `last_fetch_ms`, `Fresh`, `http_status`, `None` error, `dialect`, `dialect_declared`, `conformance_notes` (JSON `serde_json::to_string(&notes)` — `Some("[]")` when clean), feed meta, `item_count: Some(rows)`. Serve stamped `"fresh"`. `tracing::debug!` with the observability fields.
     - `Err(pf)` → treat as failure with `error = format!("parse failed at {}: {}", pf.stage, truncate(pf.reason, 512))`; **also** fold `pf.dialect_declared` into the recorded observation (the sniff worked even though parse failed).
   - `Err(fe)` → failure with `error = truncate(fe.to_string(), 512)`, `http_status` from `Status{status}` variants.
   - Failure path: `record_failure(feed, http_status, error, now_ms, now + failure_fuse(ttl))` → `tracing::warn!(source, feed, error, "rss feed degraded")` → serve stale window stamped `"stale-error"` if the (post-record) snapshot still has one, else `None`.
3. Concurrent scans may double-fetch an expired feed (no in-flight coalescing — documented future extension, same as open_connector).

- [ ] **Step 1: Failing tests** — drive with `MockFeedServer` + `allowing_loopback_for_tests` policy + tiny ttl values; a `test_engine(server_url_paths: &[(&str, &str)], ttl: u64) -> RssEngine` helper builds subscriptions against the mock:

```rust
#[tokio::test]
async fn fresh_fetch_parses_and_stamps_fresh() {
    let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
    let engine = test_engine(&server, &[("a", "/f.xml")], 900);
    let batch = engine.serve_feed("a", || true).await.unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(str_col(&batch, "window_status"), vec!["fresh"]);
    assert_eq!(server.requests().len(), 1);
    // Second serve within TTL: zero additional network.
    engine.serve_feed("a", || true).await.unwrap();
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn expired_with_etag_takes_304_and_stamps_revalidated() {
    let server = MockFeedServer::start(|req| {
        if req.header("if-none-match").is_some() { MockResponse::status(304) }
        else { MockResponse::xml(RSS2_MINIMAL).with_header("etag", "\"v1\"") }
    }).await;
    let engine = test_engine(&server, &[("a", "/f.xml")], 0); // always-live
    engine.serve_feed("a", || true).await.unwrap();
    let batch = engine.serve_feed("a", || true).await.unwrap();
    assert_eq!(str_col(&batch, "window_status"), vec!["revalidated"]);
    assert_eq!(server.requests().len(), 2);
    let row = engine.feeds_row("a");
    assert_eq!(str_col(&row, "last_status"), vec!["revalidated"]);
}

#[tokio::test]
async fn failed_refetch_serves_stale_rows_and_records_error() {
    let hits = Arc::new(AtomicUsize::new(0));
    let h = hits.clone();
    let server = MockFeedServer::start(move |_| {
        if h.fetch_add(1, Ordering::SeqCst) == 0 { MockResponse::xml(RSS2_MINIMAL) }
        else { MockResponse::status(500) }
    }).await;
    let engine = test_engine(&server, &[("a", "/f.xml")], 0);
    engine.serve_feed("a", || true).await.unwrap();
    let batch = engine.serve_feed("a", || true).await.unwrap();          // 500 (after retries) → stale
    assert_eq!(str_col(&batch, "window_status"), vec!["stale-error"]);
    let row = engine.feeds_row("a");
    assert_eq!(str_col(&row, "last_status"), vec!["stale-error"]);
    assert!(str_opt_col(&row, "last_error")[0].as_ref().unwrap().contains("500"));
    // Negative cache: an immediate third serve does NOT re-poke the dead feed.
    let n = server.requests().len();
    engine.serve_feed("a", || true).await.unwrap();
    assert_eq!(server.requests().len(), n);
}

#[tokio::test]
async fn never_fetched_failure_yields_zero_rows_and_error_status() {
    let server = MockFeedServer::start(|_| MockResponse::status(500)).await;
    let engine = test_engine(&server, &[("a", "/f.xml")], 900);
    assert!(engine.serve_feed("a", || true).await.is_none());
    assert_eq!(str_col(&engine.feeds_row("a"), "last_status"), vec!["error"]);
}

#[tokio::test]
async fn feeds_row_before_any_scan_is_never_and_issues_no_requests() {
    let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
    let engine = test_engine(&server, &[("a", "/f.xml")], 900);
    let row = engine.feeds_row("a");
    assert_eq!(str_col(&row, "last_status"), vec!["never"]);
    assert_eq!(server.requests().len(), 0);
}
```

Plus: `parse_failure_records_stage_and_declared_dialect` (serve garbage body → `last_error` contains `"parse failed at strict-parse"`, `dialect_declared` populated, zero rows), `egress_blocked_feed_degrades_like_unreachable` (subscription URL `http://10.1.2.3/f` with test policy → `last_error` contains `"egress blocked"`, zero rows, mock got zero requests), `conformance_notes_land_in_feeds_row` (naked-amp feed → notes JSON contains `"sanitation: escaped-naked-ampersands"`), `item_count_and_meta_populate`, and the gate contract: `false_launch_gate_skips_fetch_and_health_write` (expired feed, `serve_feed("a", || false)` → `None`, zero requests, `feeds_row` unchanged — still the pre-call observation) and `false_gate_still_serves_within_ttl_cache` (fresh window + `|| false` → rows served, zero requests).
Shared helpers `str_col`/`str_opt_col` + `RSS2_MINIMAL` const go in `testutil.rs`.

- [ ] **Step 2: Verify failure.** `cargo test -p skardi --features rss rss::engine`

- [ ] **Step 3: Implement** per the state machine above. Observability: one `tracing::debug!` per serve with fields `source`, `feed`, `outcome` (`cache-hit|revalidated|fetched|stale-error|error`), `http_status`, `bytes`, `rows`, `elapsed_ms`, `repairs`, plus the `warn!` on degradation. Bodies never logged.

- [ ] **Step 4: Verify pass.**

- [ ] **Step 5: Commit** — `git commit -m "feat(sources): rss engine freshness state machine with per-feed degradation"`

---

### Task 12: Partition-per-feed ExecutionPlan

**Files:**
- Create: `crates/skardi/src/sources/providers/rss/exec.rs`

**Interfaces:**
- Consumes: `RssEngine::{serve_feed, feeds_row, scan_timeout}`, schemas (Task 9).
- Produces:
  - `pub enum RssTableKind { Feeds, Items }`
  - `pub struct RssScanExec` — `pub fn new(engine: Arc<RssEngine>, kind: RssTableKind, feeds: Vec<String> /*pruned, subscription order*/, projection: Option<Vec<usize>>, limit: Option<usize>) -> DFResult<Self>`; `PlanProperties` with `Partitioning::UnknownPartitioning(feeds.len().max(1))`, `EmissionType::Incremental`, `Boundedness::Bounded` (model: `open_connector/exec.rs:97-102`); projected schema via `full_schema.project(indices)`.
  - Scan-shared state built in `new()`: `Arc<ScanShared { emitted: AtomicUsize, deadline: Instant /* now + engine.scan_timeout() */ }>`.
  - `execute(partition_i)`: one feed per partition.
    - `Items`: an async stream (via `futures::stream::once` + `RecordBatchStreamAdapter`) that: (1) cheap pre-check — if `limit` is `Some(n)` and `shared.emitted.load() >= n` → ends empty without calling `serve_feed`; (2) `tokio::time::timeout_at(deadline, engine.serve_feed(feed, gate))` where `gate = move || limit.map_or(true, |n| shared.emitted.load(Ordering::SeqCst) < n)` — the engine re-evaluates this gate after acquiring its politeness permit, which is what actually stops launches under concurrent partition polling (see Task 11); deadline hit → the partition emits zero rows and a `warn!` (scan-deadline degradation is per-feed, consistent with fault isolation; the feed's own request timeout usually fires first); (3) on `Some(batch)`: `shared.emitted.fetch_add(batch.num_rows())`, apply projection (`batch.project(indices)`); empty projection (`Some([])`, i.e. `count(*)`) → `RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![], &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())))`.
    - `Feeds`: emit `engine.feeds_row(feed)` projected the same way; never touches the fetcher (compile-time guarantee: `feeds_row` is sync and takes no fetcher path).

- [ ] **Step 1: Failing tests:**

```rust
#[tokio::test]
async fn items_partitions_stream_independently_and_stamp_status() {
    let server = MockFeedServer::start(|req| match req.path.as_str() {
        "/a.xml" => MockResponse::xml(RSS2_MINIMAL),
        _ => MockResponse::status(500),
    }).await;
    let engine = Arc::new(test_engine(&server, &[("a", "/a.xml"), ("b", "/b.xml")], 900));
    let exec = RssScanExec::new(engine, RssTableKind::Items, vec!["a".into(), "b".into()], None, None).unwrap();
    assert_eq!(exec.properties().partitioning.partition_count(), 2);
    let ctx = Arc::new(TaskContext::default());
    let a = collect_stream(exec.execute(0, ctx.clone())).await;   // helper: drain to Vec<RecordBatch>
    let b = collect_stream(exec.execute(1, ctx)).await;
    assert_eq!(a.iter().map(|x| x.num_rows()).sum::<usize>(), 1); // healthy feed serves
    assert!(b.is_empty(), "never-fetched dead feed yields zero rows");
}

#[tokio::test]
async fn limit_satisfied_stops_launching_fetches() {
    // 3 feeds, max_concurrent = 1 (serialized launches), limit = 1.
    let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
    let engine = Arc::new(test_engine_with_concurrency(&server, &[("a","/a"),("b","/b"),("c","/c")], 900, 1));
    let exec = RssScanExec::new(engine, RssTableKind::Items, vec!["a".into(),"b".into(),"c".into()], None, Some(1)).unwrap();
    let ctx = Arc::new(TaskContext::default());
    // Drain partitions sequentially (deterministic): after partition 0 emits 1 row,
    // partitions 1 and 2 must not fetch.
    for i in 0..3 { let _ = collect_stream(exec.execute(i, ctx.clone())).await; }
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn empty_projection_preserves_row_count() {
    // count(*) shape: projection Some(vec![]) → zero-column batch with row_count 1.
}

#[tokio::test]
async fn feeds_kind_never_fetches() {
    // Feeds exec over 2 subscriptions → 2 one-row batches, server.requests().is_empty().
}

#[tokio::test]
async fn projection_prunes_columns() { /* projection [0,15] → 2-col batches named feed, window_status */ }
```

- [ ] **Step 2: Verify failure.** `cargo test -p skardi --features rss rss::exec`

- [ ] **Step 3: Implement.** Standard leaf-plan boilerplate mirrors `open_connector/exec.rs` (`children` empty, `with_new_children` guards, `statistics` unknown, `DisplayAs` = `"RssScanExec: kind=items feeds=3 limit=Some(1)"`).

- [ ] **Step 4: Verify pass.**

- [ ] **Step 5: Commit** — `git commit -m "feat(sources): rss partition-per-feed execution plan with limit gating"`

---

### Task 13: Table providers with pushdown and partition pruning

**Files:**
- Create: `crates/skardi/src/sources/providers/rss/table.rs`

**Interfaces:**
- Consumes: `RssScanExec`, `RssTableKind`, schemas, `RssEngine::subscriptions`.
- Produces:
  - `pub struct RssTableProvider` — `pub fn feeds(engine: Arc<RssEngine>) -> Self`, `pub fn items(engine: Arc<RssEngine>) -> Self`; `TableProvider` impl with `TableType::Base`, no `insert_into` (read-only by construction).
  - `pub(crate) fn prune_feeds(filters: &[Expr], subs: &[ResolvedSubscription]) -> Vec<String>` — intersects every prunable predicate; a filter is prunable iff it is `BinaryExpr(col, Eq, lit)`/`(lit, Eq, col)` or non-negated `InList(col, [all-literals])` where col ∈ {`feed`, `feed_url`}; `feed_url` values map to names via the subscription list; unknown values → that predicate contributes the empty set. No prunable predicate → all subscriptions.
  - `supports_filters_pushdown`: `Items` → prunable exprs `Exact`, everything else `Unsupported`; `Feeds` → all `Unsupported`.
  - `scan`: `Items` → `prune_feeds` then `RssScanExec::new(engine, Items, pruned, projection, limit)`; `Feeds` → all subscriptions, `limit` passed through, filters ignored (DataFusion applies them above).

- [ ] **Step 1: Failing tests** (expression construction with `datafusion::logical_expr::{col, lit, Expr}`):

```rust
#[test]
fn pushdown_classification_is_exact_only_for_feed_predicates() {
    let p = items_provider_with_feeds(&["a", "b"]);           // helper: offline engine, no server
    let feed_eq   = col("feed").eq(lit("a"));
    let url_in    = col("feed_url").in_list(vec![lit("http://x/1")], false);
    let title_eq  = col("title").eq(lit("t"));
    let feed_gt   = col("feed").gt(lit("a"));
    let neg_in    = col("feed").in_list(vec![lit("a")], true);
    let got = p.supports_filters_pushdown(&[&feed_eq, &url_in, &title_eq, &feed_gt, &neg_in]).unwrap();
    use TableProviderFilterPushDown::*;
    assert_eq!(got, vec![Exact, Exact, Unsupported, Unsupported, Unsupported]);
}

#[test]
fn prune_intersects_predicates_and_maps_urls() {
    let subs = subs(&[("a", "http://x/a"), ("b", "http://x/b"), ("c", "http://x/c")]);
    assert_eq!(prune_feeds(&[col("feed").eq(lit("b"))], &subs), vec!["b"]);
    assert_eq!(prune_feeds(&[col("feed").in_list(vec![lit("a"), lit("c")], false)], &subs), vec!["a", "c"]);
    assert_eq!(prune_feeds(&[col("feed_url").eq(lit("http://x/b"))], &subs), vec!["b"]);
    // Intersection: feed IN (a, b) AND feed = 'b' → [b]
    assert_eq!(prune_feeds(&[col("feed").in_list(vec![lit("a"), lit("b")], false), col("feed").eq(lit("b"))], &subs), vec!["b"]);
    // Unknown value → empty (zero partitions, zero fetches)
    assert!(prune_feeds(&[col("feed").eq(lit("nope"))], &subs).is_empty());
    // Reversed operands
    assert_eq!(prune_feeds(&[lit("a").eq(col("feed"))], &subs), vec!["a"]);
    // No prunable predicate → all
    assert_eq!(prune_feeds(&[col("title").eq(lit("t"))], &subs).len(), 3);
}

#[tokio::test]
async fn end_to_end_sql_prunes_to_one_fetch() {
    // Register both providers on a SessionContext via a MemoryCatalog (hand-wired here;
    // register_rss_tables proper is Task 14), then:
    //   SELECT guid FROM news.main.items WHERE feed = 'a'
    // → exactly 1 mock request; rows only from feed a.
}
```

- [ ] **Step 2: Verify failure.** `cargo test -p skardi --features rss rss::table`

- [ ] **Step 3: Implement.** Note the `ScalarValue::Utf8` extraction from `Expr::Literal` and DataFusion's filter-splitting (AND arrives as separate `Expr`s; OR arrives as one unsupported expr — correct by construction).

- [ ] **Step 4: Verify pass.**

- [ ] **Step 5: Commit** — `git commit -m "feat(sources): rss table providers with exact feed pushdown and partition pruning"`

---

### Task 14: Registration entry point

**Files:**
- Modify: `crates/skardi/src/sources/providers/rss/mod.rs`

**Interfaces:**
- Consumes: everything above; `HierarchyLevel`; `MemoryCatalogProvider`/`MemorySchemaProvider` (pattern: `open_connector/mod.rs:181-249`).
- Produces:
  ```rust
  #[cfg(feature = "rss")]
  pub async fn register_rss_tables(
      session_ctx: &mut SessionContext,
      name: &str,
      config: Option<&RssConfig>,
      read_write: bool,
      hierarchy_level: HierarchyLevel,
  ) -> anyhow::Result<()>
  ```
  Behavior: require `Catalog` (else `RssError::CatalogHierarchyRequired`), reject `read_write` (`ReadWriteNotSupported`), require config (`MissingConfig`); `config.validate()`; `resolve_subscriptions(name, config)` (OPML read happens here — file I/O only); build `RssEngine::new(name, subs, config, Arc::new(EgressPolicy::default_deny()))`; register `main` schema with `feeds` + `items` providers into a `MemoryCatalogProvider`; `session_ctx.register_catalog(name, catalog)`;
  `tracing::info!(source = %name, subscriptions = subs.len(), surface_version = RSS_SURFACE_VERSION, "RSS source registered");`
  Also `pub use` re-exports: `register_rss_tables`, `RssConfig`, `RssError`, `RSS_SURFACE_VERSION`.

- [ ] **Step 1: Failing tests** (in `mod.rs` `#[cfg(test)]`):

```rust
#[tokio::test]
async fn registration_is_zero_network_and_tables_queryable() {   // AC1 shape
    let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
    let mut ctx = SessionContext::new();
    let config = config_pointing_at(&server, &[("a", "/f.xml")]);
    // Through the test seam: the mock binds loopback, which default_deny blocks.
    register_rss_tables_with_policy(
        &mut ctx, "news", Some(&config), false, HierarchyLevel::Catalog,
        Arc::new(EgressPolicy::allowing_loopback_for_tests()),
    ).await.unwrap();
    assert_eq!(server.requests().len(), 0, "registration performed network I/O");
    let feeds = ctx.sql("SELECT name, last_status FROM news.main.feeds ORDER BY name").await.unwrap().collect().await.unwrap();
    assert_eq!(str_col(&feeds[0], "last_status"), vec!["never"]);
    assert_eq!(server.requests().len(), 0, "feeds scan performed network I/O");
    let items = ctx.sql("SELECT guid, window_status FROM news.main.items").await.unwrap().collect().await.unwrap();
    assert_eq!(items.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn non_catalog_hierarchy_is_rejected() { /* HierarchyLevel::Table → err contains "catalog" */ }

#[tokio::test]
async fn read_write_is_rejected() { /* read_write=true → err contains "read-only" */ }

#[tokio::test]
async fn missing_config_is_rejected() { /* None → err contains "rss:" */ }

#[test]
fn schema_metadata_carries_surface_version() {
    assert_eq!(items_schema().metadata.get("skardi.rss.surface_version").map(String::as_str), Some("1"));
    assert_eq!(feeds_schema().metadata.get("skardi.rss.surface_version").map(String::as_str), Some("1"));
}
```

Test-only seam: `register_rss_tables` uses `default_deny()`, which blocks the loopback mock. Add `#[cfg(test)] pub(crate) async fn register_rss_tables_with_policy(…, policy: Arc<EgressPolicy>) -> …` and make the public fn a thin wrapper — tests register through the seam with the loopback-allowing policy; production has one path.

- [ ] **Step 2: Verify failure.** `cargo test -p skardi --features rss rss::` (mod tests)

- [ ] **Step 3: Implement** per Interfaces.

- [ ] **Step 4: Verify pass** + `cargo check -p skardi` (feature off still compiles).

- [ ] **Step 5: Commit** — `git commit -m "feat(sources): register rss sources as two-table catalogs with zero-IO registration"`

---

### Task 15: Server wiring (`DataSourceType::Rss`, validation, dispatch, e2e)

**Files:**
- Modify: `crates/skardi/src/sources/data_source_type.rs` (variant + `as_str` + roundtrip test)
- Modify: `crates/server/Cargo.toml` (`[features]`: `rss = ["skardi/rss"]`)
- Modify: `crates/server/src/config.rs` — `DataSource.rss` field (~line 141), `ConfigError` variants (~line 257), `validate_data_sources` (~line 798), `CATALOG_SUPPORTED_SOURCES` (line 741-751), dispatch arm (~line 1586, next to `Documents`)
- Modify: `crates/server/src/pipeline_handlers.rs:405-421` (exhaustive `path` match)
- Modify: `crates/skardi/src/jobs/executor.rs:322-382` (`resolve_destination`)

**Interfaces:**
- Consumes: `RssConfig`, `register_rss_tables` (Task 14).
- Produces: `DataSourceType::Rss` (serde string `"rss"` via the existing `rename_all = "lowercase"`); `DataSource { …, pub rss: Option<skardi::sources::providers::rss::RssConfig> }` with `#[serde(default, skip_serializing_if = "Option::is_none")]`; `ConfigError::{MissingRssConfig, UnexpectedRssConfig, InvalidRssConfig, RssHierarchyRequired}` mirroring the OpenConnector variants at `config.rs:257-276`.

Exact edits:
1. **Enum**: `Rss,` variant + `Self::Rss => "rss",` in `as_str()` + roundtrip test (pattern `data_source_type.rs:60-87`).
2. **Validation** (`validate_data_sources`): extend the typed-config presence check (pattern at `config.rs:798-830`) — because there are now two typed blocks, restructure minimally: keep the existing open_connector match, add a parallel match for `(&source.source_type, &source.rss)`:
   - `(Rss, Some(cfg))` → require `hierarchy_level == Catalog` (`RssHierarchyRequired`), `cfg.validate()` → `InvalidRssConfig { name, reason }`;
   - `(Rss, None)` → `MissingRssConfig`;
   - `(_, Some(_))` → `UnexpectedRssConfig { name, source_type }`;
   - `(_, None)` → ok.
   Do **not** add `Rss` to the connection-string-required arm (`config.rs:889-908`) — RSS has no connection string. Add `DataSourceType::Rss` to `CATALOG_SUPPORTED_SOURCES`. (`WRITABLE_SOURCE_TYPES` untouched — the existing gate at `config.rs:785-793` then rejects `read_write` for free.)
3. **Dispatch arm** (in `register_data_source`, following the `Documents` cfg-split shape at `config.rs:1586-1632`):
   ```rust
   DataSourceType::Rss => {
       #[cfg(feature = "rss")]
       {
           tracing::info!("Registering RSS source: {} (hierarchy_level: {:?})", source.name, source.hierarchy_level);
           skardi::sources::providers::rss::register_rss_tables(
               session_ctx, &source.name, source.rss.as_ref(),
               source.access_mode.is_read_write(), source.hierarchy_level,
           ).await.map_err(|e| ConfigError::DataSourceRegistrationFailed {
               name: source.name.clone(), error: e.to_string(),
           })?;
       }
       #[cfg(not(feature = "rss"))]
       {
           return Err(ConfigError::DataSourceRegistrationFailed {
               name: source.name.clone(),
               error: "rss data source type requires the `rss` feature to be enabled at build time".to_string(),
           }.into());
       }
   }
   ```
   Also confirm the pre-match path/connection block (`config.rs:1039-1085`) does not require `path`/`connection_string` for `Rss` (group with the sources that skip both).
4. **`pipeline_handlers.rs:405-421`**: add `Rss` to the no-path group (the `None` arm at 412-420).
5. **`jobs/executor.rs`**: `Some(DataSourceType::Rss) => Err(…read-only destination…)` mirroring the `Documents` arm at 361-363.

- [ ] **Step 1: Write failing tests** (in `config.rs` tests, all `#[cfg(feature = "rss")]` where they need the provider):

```rust
#[test]
fn rss_block_on_wrong_type_is_rejected() {
    // DataSource { source_type: Csv, rss: Some(minimal_rss_config()), .. }
    // validate_data_sources → ConfigError::UnexpectedRssConfig
}

#[test]
fn rss_without_block_is_rejected() { /* MissingRssConfig */ }

#[test]
fn rss_requires_catalog_hierarchy() { /* Table level → RssHierarchyRequired */ }

#[test]
fn rss_read_write_is_unsupported_write_mode() { /* access_mode read_write → UnsupportedWriteMode (via WRITABLE_SOURCE_TYPES) */ }

#[cfg(feature = "rss")]
#[tokio::test]
async fn test_register_rss_source_via_context() {
    // Mirror test_register_documents_source_via_context (config.rs:2176):
    // write ctx.yaml with:
    //   data_sources:
    //     - name: news
    //       type: rss
    //       hierarchy_level: catalog
    //       rss:
    //         feeds: [ { url: "https://feeds.example.invalid/f.xml" } ]
    // load_context_config + register_data_sources (zero network: registration never fetches,
    // so the unreachable URL is harmless), then:
    //   SELECT name, url, last_status FROM news.main.feeds
    // → one row, last_status = "never".
}
```

- [ ] **Step 2: Verify failure** — `cargo test -p skardi-server --features rss rss` → compile errors (missing `Rss` variant, missing `rss` field, missing `ConfigError` variants) are the red state for a wiring task.

- [ ] **Step 3: Implement** edits 1–5. The compiler drives completeness: `DataSourceType::Rss` breaks every exhaustive match listed; fix each with the arm specified above.

- [ ] **Step 4: Verify** — `cargo test -p skardi-server --features rss && cargo check -p skardi-server && cargo check --all` (whole workspace, both with and without features).

- [ ] **Step 5: Commit** — `git commit -m "feat(server): wire type=rss through validation, dispatch, and catalog registries"`

---

### Task 16: CLI wiring

**Files:**
- Modify: `crates/cli/Cargo.toml` (`[features]`: `rss = ["skardi/rss"]`)
- Modify: `crates/cli/src/main.rs` — `LocalDataSource.rss` field (~line 246), stray-block guard (~line 782-788), `"rss"` match arm before the `_` fallback (~line 1049)

**Interfaces:**
- Consumes: `RssConfig`, `register_rss_tables`.
- Produces: CLI `ctx.yaml` support for `type: rss`.

Exact edits:
1. `LocalDataSource` gains `#[serde(default)] rss: Option<skardi::sources::providers::rss::RssConfig>` (unconditional — config type compiles featureless).
2. Extend the existing guard (pattern at `main.rs:782-788`): a `rss:` block on a non-rss type → `anyhow::bail!("data source '{}': `rss:` block is only valid for type: rss", source.name)`; and keep the equivalent open_connector guard intact.
3. Arm:
   ```rust
   "rss" => {
       #[cfg(feature = "rss")]
       {
           if source.hierarchy_level != HierarchyLevel::Catalog {
               anyhow::bail!("RSS source '{}': hierarchy_level must be 'catalog' \
                    (one source exposes <name>.main.feeds and <name>.main.items)", source.name);
           }
           skardi::sources::providers::rss::register_rss_tables(
               session_ctx, &source.name, source.rss.as_ref(),
               source.is_read_write(), source.hierarchy_level,
           ).await.with_context(|| format!("Failed to register RSS source '{}'", source.name))?;
       }
       #[cfg(not(feature = "rss"))]
       anyhow::bail!("RSS source '{}': this build lacks the `rss` feature (rebuild with --features rss)", source.name);
   }
   ```

- [ ] **Step 1: Failing tests** (in `main.rs` tests, near the existing `LocalDataSource` tests at ~2869): `local_data_source_parses_rss_block` (YAML with `rss:` block deserializes; `rss.is_some()`), `stray_rss_block_on_sqlite_bails` (register_source over a `type: sqlite` source with an `rss:` block → error mentions "only valid for type: rss"), `#[cfg(feature = "rss")] rss_source_registers_and_feeds_queryable` (same zero-network shape as Task 15's e2e, through the CLI's `register_source`).

- [ ] **Step 2: Verify failure** — `cargo test -p skardi-cli --features rss rss`.

- [ ] **Step 3: Implement** edits 1–3.

- [ ] **Step 4: Verify** — `cargo test -p skardi-cli --features rss && cargo check -p skardi-cli` (default features still compile; `type: rss` without the feature errors at registration, not at parse).

- [ ] **Step 5: Commit** — `git commit -m "feat(cli): register type=rss sources from ctx.yaml"`

---

### Task 17: Fixture corpus + contract tests (the regression ratchet)

**Files:**
- Create: `crates/skardi/src/sources/providers/rss/fixtures/` — corpus documents (below)
- Create: `crates/skardi/src/sources/providers/rss/fixtures/golden/` — pinned Markdown outputs
- Create: corpus test module `#[cfg(test)] mod corpus_tests` in `parse.rs` (or a `fixtures.rs` test-only module declared in `mod.rs`)

**Interfaces:**
- Consumes: `parse_feed_document` (Task 8).
- Produces: the growing-only corpus + a manifest-driven contract test.

Corpus (each file authored in this task; content requirements in parentheses):

| fixture | expectation |
|---|---|
| `rss2_wellformed.xml` | parses; dialect `rss-2.0`; declared `rss-2.0`; notes `[]`; full field assertions (guid, RFC-822 date, content:encoded → golden md, categories, enclosure, dc:creator) |
| `rss2_missing_channel_description.xml` | parses; notes contain `missing-required-field: channel/description`; rows still served |
| `rss1_rdf.xml` | dialect `rss-1.0`; rdf:about → guid; dc:date ISO-8601 → timestamp; dc:creator → author |
| `atom10.xml` | dialect `atom`; declared `atom-1.0`; id/alternate-link/published+updated; html content → golden md; text summary passthrough |
| `atom03.xml` | declared `atom-0.3`; parses (dialect `atom`) **or** degrades with recorded reason — pin whichever feed-rs 2.x does, never a panic |
| `jsonfeed_11.json` | dialect `json-feed-1.x`; declared `json-feed-1.1`; content_text passthrough byte-exact; content_html → golden md; tags; attachment → enclosure |
| `lying_content_type` (uses `atom10.xml` + content_type param `application/rss+xml`) | notes contain `content-type-mismatch: served application/rss+xml, parsed atom` |
| `encoding_latin1_mislabeled.xml` | rescued; notes contain `sanitation: reencoded-to-utf8`; title's `é` correct in stored value |
| `control_chars.xml` | rescued; notes contain `sanitation: stripped-control-chars` |
| `naked_ampersand.xml` (naked `&`, `&nbsp;`, CDATA with `&&`, `&#169;`, `&amp;`) | rescued; notes exactly `["sanitation: escaped-naked-ampersands"]`; stored md pinned (`&nbsp;` became a space/nbsp char via the HTML pass; CDATA `&&` intact; `©` intact) |
| `billion_laughs.xml` | `ParseFailure` stage `refused-internal-dtd` — never expands |
| `hostile_markup.xml` (`<script>`, `<style>`, `onclick=`, `<a href="javascript:…">`, `<custom-tag>`) | parses; golden md shows script/style dropped, unknown-tag text kept, no `<` in content |
| `markdown_structures.xml` (h1-h3, ol/ul, nested list, table, img, pre/code, blockquote) | golden md pins the full structural conversion |
| `truncated.xml` (cut mid-tag) | `ParseFailure` stage `strict-parse`; declared dialect still sniffed |
| `empty_feed.xml` (valid channel, zero items) | parses; 0 rows; notes `[]` (legitimately-empty case for the absence-check docs) |
| `guidless_items.xml` (items with link only; one with neither) | link-fallback guid; note `entries-without-identity: 1` |
| `bomb.xml.gz` (gzip of ~8 MiB of zeros wrapped in `<rss>…`) | used by Task 18's decompressed-cap test, not by the corpus runner |

- [ ] **Step 1: Write the manifest test first** (it fails because fixtures don't exist yet):

```rust
struct Expect {
    dialect: Option<&'static str>,            // None → must fail
    declared: Option<&'static str>,
    notes_contain: &'static [&'static str],
    notes_exact_empty: bool,
    failure_stage: Option<&'static str>,
    min_items: usize,
    golden_content: Option<(&'static str /*fixture*/, usize /*item idx*/, &'static str /*golden path*/)>,
}

const CORPUS: &[(&str, Option<&str> /*content_type*/, Expect)] = &[ /* one row per fixture above */ ];

#[test]
fn every_corpus_fixture_parses_or_degrades_visibly() {   // AC5, AC10, AC16, AC18
    for (name, content_type, expect) in CORPUS {
        let bytes = fixture_bytes(name);                  // include_bytes! table
        let got = parse_feed_document(&bytes, *content_type);
        match (&got, expect.failure_stage) {
            (Err(pf), Some(stage)) => {
                assert_eq!(pf.stage, stage, "{name}");
                assert!(!pf.reason.is_empty(), "{name}: failure must carry a reason");
            }
            (Ok(doc), None) => {
                assert_eq!(Some(doc.dialect), expect.dialect, "{name}");
                if let Some(d) = expect.declared { assert_eq!(doc.dialect_declared.as_deref(), Some(d), "{name}"); }
                for n in expect.notes_contain {
                    assert!(doc.conformance_notes.iter().any(|x| x.contains(n)), "{name}: missing note {n}: {:?}", doc.conformance_notes);
                }
                if expect.notes_exact_empty { assert!(doc.conformance_notes.is_empty(), "{name}: {:?}", doc.conformance_notes); }
                assert!(doc.items.len() >= expect.min_items, "{name}");
                if let Some((_, idx, golden)) = expect.golden_content {
                    assert_eq!(doc.items[*idx].content.as_deref(), Some(golden_str(golden)), "{name}: golden drift");
                }
            }
            other => panic!("{name}: unexpected outcome {other:?}"),
        }
    }
}
```

Plus `rss2_wellformed_full_row_assertions` (every column value for item 0 — the per-dialect deep check; three more for atom10/jsonfeed/rss1).

- [ ] **Step 2: Author the fixtures + goldens.** Goldens are generated once by running the converter and **reviewed by eye** before committing (they are the contract; a wrong golden pins a bug). Keep each fixture minimal but real-shaped (channel metadata + 1-3 items).

- [ ] **Step 3: Verify pass** — `cargo test -p skardi --features rss corpus`.

- [ ] **Step 4: Commit** — `git commit -m "test(sources): rss fixture corpus contract tests with pinned Markdown goldens"`

---

### Task 18: Mock-HTTP integration suite (acceptance criteria at the SQL surface)

**Files:**
- Create: `#[cfg(test)] mod integration_tests` in `crates/skardi/src/sources/providers/rss/mod.rs` (or a sibling `integration_tests.rs` declared test-only)

**Interfaces:**
- Consumes: `register_rss_tables_with_policy` seam (Task 14), `MockFeedServer`, full provider.
- Produces: the AC-crosswalk suite. Every test drives real SQL through `SessionContext`.

Test list (each is a full `#[tokio::test]`; AC = spec acceptance criterion it realizes):

1. `ac1_registration_is_zero_network` — N=3 subscriptions; after register + `feeds` scan: `server.requests().is_empty()`.
2. `ac2_full_scan_fetches_all_where_prunes_to_one` — `SELECT * FROM news.main.items` → 3 requests; then `WHERE feed = 'b'` → exactly 1 new request, to `/b`'s path.
3. `ac3_ttl_and_304_paths` — ttl 900: two scans → 3 requests total; then ttl 0 engine + etag handler: second scan sends `If-None-Match`, gets 304, rows served with `window_status = 'revalidated'`.
4. `ac4_dead_feed_isolation_with_stale_stamp` — scan (all healthy), flip feed b's handler to 500, ttl 0, scan again: feed a/c rows `fresh`, feed b rows served `stale-error`; `feeds` shows b `stale-error` + `last_error`; other feeds' rows unaffected (row values equal to first scan).
5. `ac13_feeds_scan_zero_requests_even_after_failure` — after the failure in (4): `SELECT * FROM feeds` twice → zero new requests; dead feed not re-poked within the fuse.
6. `ac15_reserved_range_refused_direct_and_redirect` — subscription at `http://192.168.7.7/f` (policy: loopback-allowed, private-blocked) → `items` zero rows for it, `feeds.last_error LIKE '%egress blocked%'`, mock untouched; second subscription whose handler 302s to `http://10.9.9.9/f` → same, with exactly the one pre-redirect request recorded.
7. `decompressed_cap_rejects_gzip_bomb` — handler serves `fixtures/bomb.xml.gz` with `content-encoding: gzip` (on-wire ~50 KiB, decompressed 8 MiB > 5 MiB cap) → feed errors with `last_error` containing "response too large"; healthy sibling feed unaffected.
8. `request_timeout_isolates_slow_feed` — `with_delay(3s)` on one feed, 1s request timeout → slow feed degrades, fast feed serves; whole query returns within the scan deadline.
9. `retry_after_is_honored_within_scan` — 429+`retry-after: 1` then 200 → success, 2 requests, elapsed ≥ 1s.
10. `limit_stops_launching_fetches` — SQL `SELECT guid FROM news.main.items LIMIT 1` with `max_concurrent: 1`, 3 feeds → exactly 1 request (DataFusion polls partitions through the coalesce; the emitted-counter + serialized semaphore guarantee later partitions skip). Also: `ORDER BY guid LIMIT 1` → 3 requests (Top-K consumes every partition — the spec's documented distinction).
11. `cancellation_stops_further_fetches` — 3 slow feeds (`with_delay(500ms)`), `max_concurrent: 1`; spawn the query, abort the task after the first request lands; wait 2s → request count stays 1.
12. `federated_in_predicate_prunes` — `WHERE feed IN ('a','c')` → 2 requests.
13. `user_agent_is_sent` — default config UA on every request (`skardi-rss/…`).
14. `count_star_over_items_is_row_accurate` — `SELECT count(*) FROM news.main.items` → correct count (empty-projection path).
15. `absence_check_pattern_works` — the spec's prescribed anti-join (feeds LEFT JOIN items … WHERE i.feed IS NULL) over one healthy + one dead-never feed → returns exactly the dead feed with `last_status = 'error'`.

- [ ] **Step 1: Write tests 1-5 failing** (infrastructure: a `TestNews` harness struct bundling server + per-feed scripted handlers behind `Arc<Mutex<HashMap<path, MockResponse>>>` + registered `SessionContext` + `sql()` helper). Run: `cargo test -p skardi --features rss rss::integration` → red (harness missing).
- [ ] **Step 2: Build harness, make 1-5 green.**
- [ ] **Step 3: Add 6-10 (red → green).** Note 7 needs the gzip fixture from Task 17.
- [ ] **Step 4: Add 11-15 (red → green).**
- [ ] **Step 5: Full provider suite** — `cargo test -p skardi --features rss rss` → all green; `cargo check -p skardi` featureless.
- [ ] **Step 6: Commit** — `git commit -m "test(sources): rss mock-HTTP integration suite covering acceptance criteria 1-4, 13, 15"`

---

### Task 19: End-to-end composition (federated join + archive pipeline)

**Files:**
- Create: `crates/skardi/tests/rss_composition.rs` (`#![cfg(all(feature = "rss", feature = "chunking"))]`)

**Interfaces:**
- Consumes: `register_rss_tables_with_policy` — **problem:** it is `pub(crate)` and this is an external integration test. Resolution: expose a public, documented test-support constructor gated on the feature combination used only by in-repo tests: `#[doc(hidden)] pub fn register_rss_tables_for_tests(…, allow_loopback: bool)` in `rss/mod.rs` (mirrors how `documents` tests reach internals; keep `#[doc(hidden)]` + a comment). Also consumes `register_sqlite_tables` (writable sqlite), `ChunkingRegistry::register_chunk_udf`, and — in the `#[ignore]` variant — `CandleModelRegistry`.
- Produces: the M1 e2e coverage listed in the spec's Testing Strategy.

Tests:

1. `federated_join_items_with_sqlite` — temp sqlite db with `feed_meta(feed TEXT, tier TEXT)`; mock rss source; SQL:
   ```sql
   SELECT i.guid, m.tier
   FROM news.main.items i JOIN meta.main.feed_meta m ON m.feed = i.feed
   ORDER BY i.guid
   ```
   → joined rows correct (AC7).
2. `archive_ingest_is_idempotent_and_survives_window_roll` — the heart of the downstream contract:
   - Setup: writable sqlite `archive` with the skill's DDL executed via `tokio_rusqlite` in the test:
     ```sql
     CREATE TABLE IF NOT EXISTS news_items (
       feed TEXT NOT NULL, guid TEXT NOT NULL, title TEXT, link TEXT, author TEXT,
       published TIMESTAMP, content TEXT, PRIMARY KEY (feed, guid));
     CREATE TABLE IF NOT EXISTS news_chunks (
       feed TEXT NOT NULL, guid TEXT NOT NULL, chunk_idx INTEGER NOT NULL,
       chunk_text TEXT NOT NULL, embedding BLOB, ingested_at TIMESTAMP,
       PRIMARY KEY (feed, guid, chunk_idx));
     ```
   - Statement A (anti-join INSERT, run via `ctx.sql`):
     ```sql
     INSERT INTO archive.main.news_items (feed, guid, title, link, author, published, content)
     SELECT i.feed, i.guid, i.title, i.link, i.author, i.published, COALESCE(i.content, i.summary)
     FROM news.main.items i
     LEFT JOIN archive.main.news_items a ON a.feed = i.feed AND a.guid = i.guid
     WHERE a.guid IS NULL
     ```
   - Statement B (chunk INSERT; embedding NULL in the default run). DataFusion has no `WITH ORDINALITY`; the shipped idiom (`docs/chunk.md` "Inline ingestion") is a plain `UNNEST(chunk(...))` subquery, so `chunk_idx` comes from a window function:
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
     ) s
     ```
     (Chunk→index assignment within one entry is unspecified without an ORDER BY; assert that `chunk_idx` values are dense `0..n-1` per `(feed, guid)` and that every chunk_text is present — not which text got which index. The canonical rendered idiom is an M3/skill concern.)
   - Assert: run A+B → `news_items` = 3 rows (content is Markdown — assert one known golden value flowed through verbatim), `news_chunks` ≥ 3; **re-run A+B → zero new rows in both** (AC6 shape, in-repo variant).
   - Shrink the mock window to 2 items; ttl 0; run A+B again → counts unchanged except any genuinely-new entries; the dropped entry's title/link/published still SELECTable from `archive.main.news_items` (AC11 shape).
3. `sync_closing_health_report_shape` — after (2) with one feed flipped dead: run the spec's closing SELECT
   ```sql
   SELECT name, last_status, last_error, last_fetch FROM news.main.feeds
   WHERE last_status IN ('error', 'never', 'stale-error')
   ```
   → lists exactly the degraded feed with reason; with all feeds healthy → empty result; both runs exit successfully (AC14's report semantics — the three-statement *pipeline packaging* is M3).
4. `subscription_add_is_config_only` — register config v1 (2 feeds), query; register a second context (same archive) with 3 feeds under a new source name or after re-building the SessionContext; new feed's first `items` scan forces its fetch and its `feeds` row reports health (the M1-visible half of AC12; artifact byte-identity is a skill/M3 assertion).
5. `parameter_change_rebuild_from_retained_content` — `DELETE FROM archive.main.news_chunks` via rusqlite, re-run statement B with size 600/60 → chunks regenerate from `news_items.content` without touching the live window (mock request count unchanged).
6. `#[ignore = "live: requires SKARDI_TEST_EMBED_MODEL pointing at a local embedding model dir"]` `archive_ingest_with_candle_embeddings` — statement B with `candle('<env model dir>', c.chunk_text)` in place of NULL; asserts non-null embedding blobs. (`#[cfg(feature = "candle")]` additionally.)

- [ ] **Step 1: Write tests 1-3 failing** — `cargo test -p skardi --features "rss chunking" --test rss_composition` → red.
- [ ] **Step 2: Implement the `#[doc(hidden)]` test-support registration + make 1-3 green.** (Any engine-side bug this stage uncovers — e.g. timestamp typing across the sqlite INSERT — is fixed here.)
- [ ] **Step 3: Add 4-6, green.**
- [ ] **Step 4: Full suite both ways** — `cargo test -p skardi --features "rss chunking" && cargo test -p skardi --features rss && cargo check --all`.
- [ ] **Step 5: Commit** — `git commit -m "test(sources): rss end-to-end composition — federated join, idempotent archive, citability after window roll"`

---

### Task 20 (M2): Documentation surfaces

**Files:**
- Create: `docs/rss.md`
- Create: `docs/sample_data/rss_context.yaml`
- Create: `docs/rss/semantics.yaml`
- Modify: `README.md` (supported-sources table, ~line 314-333, + architecture mention)

**Interfaces:** none (docs); content contracts below.

- [ ] **Step 1: `docs/rss.md`** — sections, each with runnable examples (SQL/YAML lifted from the passing tests, not invented):
  1. Overview + catalog namespace (`<name>.main.feeds` / `<name>.main.items`), surface version note.
  2. Configuration reference — every `rss:` field, its default, `ttl_seconds: 0` semantics, OPML mode, the full example block from the spec (spec lines 250-269 verbatim).
  3. Freshness & caching — three tiers, negative caching + failure-fuse values, `window_status` semantics, process-lifetime cache caveat (restart → full refetch), no cross-feed consistency claim, no in-flight coalescing.
  4. Politeness defaults — `max_concurrent` per-process scoping (N replicas → N× bound), UA default, retry/`Retry-After` behavior.
  5. Field Mapping — the spec's table (spec lines 368-380) copied verbatim, plus the provider-synthesized columns note.
  6. Conformance — `dialect`/`dialect_declared` domains, the exact note string formats, required-field check list.
  7. Tolerance floor — what is not salvaged: internal-DTD documents, documents the full ladder cannot rescue, extensions feed-rs drops (the `extensions_json` boundary), entries without identity.
  8. Egress policy — the refused ranges table with rationale, redirect/rebinding behavior, "no opt-in; allowlist is a recorded future extension".
  9. Content handling — the Markdown storage contract (converted once at extraction, deterministic, no raw HTML stored, source HTML not retained, re-chunk/re-embed from stored Markdown possible, history not re-convertible); renderer guidance (inline HTML off, filter link schemes); LLM-consumption guidance (least privilege, gated side effects).
  10. Pipeline examples — the Task 19 anti-join INSERT + chunk INSERT verbatim; the closing health-report SELECT; the absence-check anti-join.
  11. Troubleshooting — absence diagnosis: legitimately-empty (`fresh` + `item_count 0`) vs dead (`error`/`never`) vs not-scanned (bare-`LIMIT` pruning; `ORDER BY … LIMIT` is Top-K and prunes nothing); `last_error` reading guide by stage.
- [ ] **Step 2: `docs/sample_data/rss_context.yaml`** — the spec's Persistent Context Binding example, registration-ready.
- [ ] **Step 3: `docs/rss/semantics.yaml`** — `kind: semantics` overlay: table descriptions for `news.main.feeds` (health surface; the absence-check pattern inline) and `news.main.items` (live window; the bare-`LIMIT` caveat; `window_status` freshness semantics), and per-column descriptions carrying the Field Mapping provenance (one line per column, e.g. `guid: "Stable item identity. RSS 2.0 <guid> (falls back to <link>), RDF rdf:about, Atom <id>, JSON Feed id."`). Loading path: standard `<ctx_dir>/semantics/` mechanism; say so in a header comment.
- [ ] **Step 4: README** — add the row to the supported-sources table (columns per that table's existing shape: type `rss`, read-only, catalog, feature `rss`) + one architecture-section sentence.
- [ ] **Step 5: Verify** — smoke-run the sample context (zero network, works offline since `feeds` never fetches):

```bash
cargo run -p skardi-cli --features rss -- query --ctx docs/sample_data/rss_context.yaml --sql "SELECT name, last_status FROM news.main.feeds"
```

Expected: exit 0, one row per sample subscription with `last_status = never` (`query --sql` invocation shape per `docs/cli.md:45`). Proofread that every SQL block in `docs/rss.md` appears in (or trivially derives from) a passing test.
- [ ] **Step 6: Commit** — `git commit -m "docs(rss): reference doc, semantics overlay, sample context, README row"`

---

## Verification (whole-plan gate)

Run after the final task; every line must pass before the branch is offered for review:

```bash
cargo fmt --all -- --check
cargo check --all
cargo check -p skardi --features rss
cargo test -p skardi --features "rss chunking"
cargo test -p skardi-server --features rss
cargo test -p skardi-cli --features rss
cargo test --all            # feature-off suites stay green
```

Acceptance-criteria crosswalk (M1+M2 scope): AC1→T14/T18, AC2→T18, AC3→T18, AC4→T18, AC5→T17, AC7→T19, AC9→T17, AC10→T17, AC13→T18, AC15→T3/T18, AC16→T5/T17, AC18→T6/T17, AC12 (config-only half)→T19, AC14 (report semantics half)→T19. Deferred to M3: AC6/AC11 (vendored render in CI), AC8 (skill flow), AC17 (load-time handshake), AC14's pipeline packaging.

## Out of scope (M3 plan)

`auto_news_base` skill (external skardi-skills repo), pipeline statement-sequence extension, `requires: rss/<version>` load-time handshake, vendored canonical consumer render + AC6/AC11 in CI, `aliases.yaml` for `sync`/`news`.
