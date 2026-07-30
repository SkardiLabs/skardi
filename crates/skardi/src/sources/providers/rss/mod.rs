//! RSS/Atom subscriptions as a read-only data source (`type: rss`).
//!
//! See `docs/superpowers/specs/2026-07-22-rss-feed-support-design.md`.
#[cfg(feature = "rss")]
pub mod cache;
pub mod config;
#[cfg(feature = "rss")]
pub mod conformance;
// The compatibility corpus: committed feed documents in `fixtures/` plus the
// manifest-driven contract test over them. Test-only and additionally gated
// behind `rss`, like `testutil` below, since everything it drives
// (`parse_feed_document`, the sanitation rungs, the HTML→Markdown conversion)
// is.
#[cfg(feature = "rss")]
pub mod convert;
#[cfg(all(test, feature = "rss"))]
mod corpus;
pub mod error;
// Reads OPML files and pulls in `quick-xml`; gated so the config/error types
// above stay parseable — and `ResolvedSubscription` below stays nameable —
// in builds that omit the `rss` feature.
#[cfg(feature = "rss")]
pub mod opml;
// The fetcher's SSRF egress guard: resolves a feed host and refuses
// loopback/link-local/private/CGNAT/unique-local targets before reqwest
// connects (see the module doc for why). Not `pub` — it is an internal
// implementation detail of the fetch engine (`fetch` consumes it via
// `super::egress`), not part of this provider's public surface. Gated
// behind `rss` alongside the rest of the fetch/parse engine, even though its
// own dependencies (reqwest, tokio) are already unconditional crate deps.
#[cfg(feature = "rss")]
mod egress;
// The partition-per-feed execution plan: the engine's only consumer, and the
// layer that enforces the scan deadline and the LIMIT launch gate. `pub` so
// the table provider a later task adds — in this module tree but a different
// file — can construct it.
#[cfg(feature = "rss")]
pub mod exec;
// The freshness state machine that composes every module above: it decides
// per feed whether a scan serves a cached window, revalidates it, refetches
// it, or degrades to stale rows, and it is the sole production consumer of
// `egress`, `fetch`, and `cache`.
#[cfg(feature = "rss")]
pub mod engine;
// The bounded HTTP fetcher (conditional GET, retries, egress enforcement)
// built on top of `egress`. Not `pub` for the same reason `egress` isn't:
// it is an implementation detail of the engine a later task builds on top,
// not part of this provider's public surface.
#[cfg(feature = "rss")]
mod fetch;
// Hand-rolled mock feed server the fetcher's tests drive. Test-only (never
// compiled into a release build) and additionally gated behind `rss` since
// its only consumer, `fetch`'s test module, is.
#[cfg(all(test, feature = "rss"))]
pub(crate) mod testutil;
// The parsing chain: byte-level sanitation rungs, the feed-rs parse driver
// that applies them, and the fixed Arrow schemas the providers serve. These
// were built on a parallel branch (Tasks 5-9) alongside the fetch chain
// above, which is why they land as one merge rather than task by task.
#[cfg(feature = "rss")]
pub mod parse;
#[cfg(feature = "rss")]
pub mod sanitize;
#[cfg(feature = "rss")]
pub mod schema;
// The two `TableProvider`s (`feeds` and `items`): they classify filters for
// pushdown, prune the subscription list to the feeds a scan must visit, and
// construct the `exec` plan above. `pub` so the catalog registration a later
// task adds can name them.
#[cfg(feature = "rss")]
pub mod table;

// The acceptance-criteria crosswalk: full SQL against a registered catalog
// whose feeds live on `testutil::MockFeedServer`. In-crate rather than in
// `crates/skardi/tests/` because it drives `register_rss_tables_with_policy`
// below, which is `#[cfg(test)] pub(crate)` and unreachable from an external
// test crate — see that function's doc, and this module's own.
#[cfg(all(test, feature = "rss"))]
mod integration_tests;

pub use config::{FeedSubscription, RssConfig};
pub use error::RssError;
#[cfg(feature = "rss")]
pub use opml::resolve_subscriptions;

#[cfg(feature = "rss")]
use std::sync::Arc;

#[cfg(feature = "rss")]
use anyhow::Result;
#[cfg(feature = "rss")]
use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
};
#[cfg(feature = "rss")]
use datafusion::prelude::SessionContext;

#[cfg(feature = "rss")]
use crate::sources::hierarchy::HierarchyLevel;
#[cfg(feature = "rss")]
use egress::EgressPolicy;
#[cfg(feature = "rss")]
use engine::RssEngine;
#[cfg(feature = "rss")]
use table::RssTableProvider;

/// Integer version of the `feeds`/`items` public surface. Bumped only by
/// breaking changes (column removal/rename/retype, nullability tightening,
/// enum-domain repurposing, identity/window semantics changes).
pub const RSS_SURFACE_VERSION: u32 = 1;

/// One subscription, fully resolved from either of [`RssConfig`]'s two
/// mutually exclusive input forms — an inline `feeds:` entry or an
/// `<outline>` pulled from an `opml:` file.
///
/// This is the convergence point the rest of the provider is built on:
/// every later stage — the fetcher, the TTL cache, the freshness state
/// machine, the partition-per-feed execution plan — consumes only a
/// `Vec<ResolvedSubscription>` and never looks at `RssConfig`'s input shape
/// again. It is a plain data struct with no parsing logic of its own, so
/// unlike [`opml`] (which requires the `rss` feature for `quick-xml`) it
/// stays nameable in featureless builds — the server and CLI can hold it
/// in a typed field regardless of which features a given build enables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSubscription {
    /// Effective subscription name: an explicit `name`/`text`/`title`, or
    /// the feed's URL when none was given. Unique across the whole
    /// resolved list.
    pub name: String,
    /// Feed URL; already checked to be `http://` or `https://`.
    pub url: String,
}

/// The one schema an `rss` catalog exposes. One source is one catalog holding
/// exactly `<name>.main.feeds` and `<name>.main.items`, and `main` mirrors the
/// sqlite provider's convention, where the single schema every table lands in
/// is also spelled `main` (`sources/providers/sqlite/mod.rs:443-444`).
#[cfg(feature = "rss")]
const RSS_SCHEMA: &str = "main";
/// The per-subscription health table: one row per subscription, always.
#[cfg(feature = "rss")]
const FEEDS_TABLE: &str = "feeds";
/// The feed-entry table: one window per subscription.
#[cfg(feature = "rss")]
const ITEMS_TABLE: &str = "items";

/// Register one `type: rss` data source as the catalog `name`, exposing
/// `<name>.main.feeds` and `<name>.main.items`.
///
/// # Registration performs no network I/O
///
/// The only I/O here is reading the `opml:` file, when one is configured, via
/// [`resolve_subscriptions`]. Nothing probes a feed: the engine and the two
/// table providers are built from the resolved subscription list alone, and
/// every HTTP request happens later, inside an `items` scan. So startup cost
/// is proportional to the size of the subscription list rather than to the
/// availability of the hosts on it — a source with fifty feeds does not wait
/// on fifty upstreams to become queryable, and an unreachable host surfaces
/// as that subscription's `feeds.last_status` instead of failing the whole
/// source. `registration_is_zero_network_and_tables_queryable` pins this by
/// asserting a mock server has observed no requests after registration *and*
/// after a `feeds` scan (`feeds` is a pure state read, so it stays at zero),
/// and exactly one after an `items` scan.
///
/// # Surface version
///
/// [`RSS_SURFACE_VERSION`] is logged here. The same constant is stamped into
/// both tables' Arrow schema metadata under `skardi.rss.surface_version`
/// (`schema.rs:96-100`), so a client can read the version off a query result
/// without access to the log.
///
/// # Egress
///
/// The engine's fetcher is given `EgressPolicy::default_deny` (unlinked: the
/// `egress` module is private, so a doc link to it would not resolve), and
/// this is the only place production code constructs a policy at all — so no
/// deployment path can point a feed subscription at a loopback,
/// private-network, or link-local address. The loopback-allowing policy the
/// tests need is `#[cfg(test)]` and reached through a `pub(crate)` seam, not
/// through this signature.
#[cfg(feature = "rss")]
pub async fn register_rss_tables(
    session_ctx: &mut SessionContext,
    name: &str,
    config: Option<&RssConfig>,
    read_write: bool,
    hierarchy_level: HierarchyLevel,
) -> Result<()> {
    register_with_policy(
        session_ctx,
        name,
        config,
        read_write,
        hierarchy_level,
        Arc::new(EgressPolicy::default_deny()),
    )
    .await
}

/// Test-only seam: [`register_rss_tables`] with a caller-supplied egress
/// policy, so in-crate tests can register a source whose feeds live on
/// [`testutil::MockFeedServer`] — which binds loopback, and loopback is
/// exactly what [`EgressPolicy::default_deny`] refuses.
///
/// `#[cfg(test)]` means this is absent from a release build entirely rather
/// than merely unreachable, and `pub(crate)` keeps it out of the crate's API
/// surface even in a test build. Production therefore has one construction
/// path for the policy, in [`register_rss_tables`] above; there is
/// deliberately no public entry point — not a `policy` argument, not an
/// `allow_loopback` flag — that could relax it.
#[cfg(all(test, feature = "rss"))]
pub(crate) async fn register_rss_tables_with_policy(
    session_ctx: &mut SessionContext,
    name: &str,
    config: Option<&RssConfig>,
    read_write: bool,
    hierarchy_level: HierarchyLevel,
    policy: Arc<EgressPolicy>,
) -> Result<()> {
    register_with_policy(
        session_ctx,
        name,
        config,
        read_write,
        hierarchy_level,
        policy,
    )
    .await
}

/// The shared body of [`register_rss_tables`] and its test seam.
#[cfg(feature = "rss")]
async fn register_with_policy(
    session_ctx: &mut SessionContext,
    name: &str,
    config: Option<&RssConfig>,
    read_write: bool,
    hierarchy_level: HierarchyLevel,
    policy: Arc<EgressPolicy>,
) -> Result<()> {
    // All invariant checks live here so both front-ends (server and CLI) get
    // identical behavior; a front-end may add an earlier typed error, but
    // this is the single enforcement point — the same arrangement
    // `register_open_connector_tables` uses
    // (`sources/providers/open_connector/mod.rs:144-168`).
    if hierarchy_level != HierarchyLevel::Catalog {
        return Err(RssError::CatalogHierarchyRequired {
            name: name.to_string(),
        }
        .into());
    }
    if read_write {
        return Err(RssError::ReadWriteNotSupported {
            name: name.to_string(),
        }
        .into());
    }
    let config = config.ok_or_else(|| RssError::MissingConfig {
        name: name.to_string(),
    })?;
    // `validate()` runs before a source has a name attached, so it reports
    // the `"<config>"` placeholder (`config.rs:232-241`). Re-stamp the real
    // one, so the message names the source an operator has to go edit. The
    // fallthrough arm is a passthrough rather than an assertion about which
    // variants `validate()` can produce.
    config.validate().map_err(|e| match e {
        RssError::InvalidConfig { reason, .. } => RssError::InvalidConfig {
            name: name.to_string(),
            reason,
        },
        other => other,
    })?;

    // The one I/O step: an `opml:` path is read here, not by `validate()`.
    let subscriptions = resolve_subscriptions(name, config)?;
    let engine = Arc::new(RssEngine::new(
        name.to_string(),
        subscriptions,
        config,
        policy,
    )?);
    let subscription_count = engine.subscriptions().len();

    // Built directly rather than through `hierarchy::build_catalog`: that
    // helper's job is to drive many `TableProvider` constructions
    // concurrently and key them by `(schema, table)` name strings, and here
    // there are exactly two providers, both built synchronously from the
    // engine above. Going through it would mean re-dispatching on the two
    // names this function just wrote, with an unreachable third arm. This is
    // the shape `register_open_connector_tables` ends with
    // (`sources/providers/open_connector/mod.rs:212-274`).
    let schema_provider = Arc::new(MemorySchemaProvider::new());
    schema_provider
        .register_table(
            FEEDS_TABLE.to_string(),
            Arc::new(RssTableProvider::feeds(Arc::clone(&engine))),
        )
        .map_err(|e| {
            anyhow::anyhow!(
                "rss source '{name}': failed to register {RSS_SCHEMA}.{FEEDS_TABLE}: {e}"
            )
        })?;
    schema_provider
        .register_table(
            ITEMS_TABLE.to_string(),
            Arc::new(RssTableProvider::items(engine)),
        )
        .map_err(|e| {
            anyhow::anyhow!(
                "rss source '{name}': failed to register {RSS_SCHEMA}.{ITEMS_TABLE}: {e}"
            )
        })?;

    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog
        .register_schema(RSS_SCHEMA, schema_provider)
        .map_err(|e| {
            anyhow::anyhow!("rss source '{name}': failed to register schema '{RSS_SCHEMA}': {e}")
        })?;

    // Publishing the catalog is the last step, after every gate above has
    // passed: `register_catalog` inserts into the context's catalog list and
    // returns whatever was registered under `name` before (datafusion 52.5.0
    // `src/execution/context/mod.rs:1716-1726`, delegating to
    // `MemoryCatalogProviderList::register_catalog`, datafusion-catalog
    // 52.5.0 `src/memory/catalog.rs:54-60`), so a failed registration must
    // not have already replaced a working source's catalog.
    session_ctx.register_catalog(name, catalog);

    tracing::info!(
        source = %name,
        subscriptions = subscription_count,
        surface_version = RSS_SURFACE_VERSION,
        "RSS source registered"
    );

    Ok(())
}

#[cfg(all(test, feature = "rss"))]
mod tests {
    use super::*;
    use crate::sources::providers::rss::config::inline_config;
    use crate::sources::providers::rss::schema::{feeds_schema, items_schema};
    use crate::sources::providers::rss::testutil::{
        MockFeedServer, MockResponse, RSS2_MINIMAL, feed_urls, str_col,
    };
    use arrow::array::RecordBatch;

    /// A config subscribing to `feeds` (`(name, path)` pairs) on `server`.
    ///
    /// `request_timeout_seconds` and `scan_timeout_seconds` are pulled well
    /// below their spec defaults (10 and 60): every server here answers
    /// immediately, so a test that starts hanging has regressed, and it
    /// should say so in seconds rather than sitting on the scan deadline for
    /// a minute.
    fn config_pointing_at(server: &MockFeedServer, feeds: &[(&str, &str)]) -> RssConfig {
        let mut config = inline_config(
            feed_urls(server, feeds)
                .into_iter()
                .map(|(name, url)| FeedSubscription {
                    url,
                    name: Some(name),
                })
                .collect(),
        );
        config.request_timeout_seconds = 5;
        config.scan_timeout_seconds = 10;
        config
    }

    /// A config that never reaches a network at all — the shape the rejection
    /// tests use, since none of them gets far enough to fetch.
    fn unreachable_config() -> RssConfig {
        inline_config(vec![FeedSubscription {
            url: "https://feed.example/f.xml".to_string(),
            name: Some("a".to_string()),
        }])
    }

    async fn query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        ctx.sql(sql)
            .await
            .unwrap_or_else(|e| panic!("plan {sql:?}: {e}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("execute {sql:?}: {e}"))
    }

    #[tokio::test]
    async fn registration_is_zero_network_and_tables_queryable() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        let mut ctx = SessionContext::new();
        let config = config_pointing_at(&server, &[("a", "/f.xml")]);

        // Through the test seam: the mock binds loopback, which the
        // production policy (`default_deny`) blocks.
        register_rss_tables_with_policy(
            &mut ctx,
            "news",
            Some(&config),
            false,
            HierarchyLevel::Catalog,
            Arc::new(EgressPolicy::allowing_loopback_for_tests()),
        )
        .await
        .expect("registration succeeds");

        assert_eq!(
            server.requests().len(),
            0,
            "registration performed network I/O"
        );

        // Both tables are addressable at exactly `<name>.main.<table>`.
        let feeds = query(
            &ctx,
            "SELECT name, last_status FROM news.main.feeds ORDER BY name",
        )
        .await;
        assert_eq!(str_col(&feeds[0], "name"), vec!["a"]);
        assert_eq!(str_col(&feeds[0], "last_status"), vec!["never"]);
        assert_eq!(
            server.requests().len(),
            0,
            "feeds scan performed network I/O"
        );

        let items = query(&ctx, "SELECT guid, window_status FROM news.main.items").await;
        assert_eq!(
            items.iter().map(RecordBatch::num_rows).sum::<usize>(),
            1,
            "the one item in RSS2_MINIMAL"
        );
        assert_eq!(
            server.requests().len(),
            1,
            "an items scan is what fetches the feed"
        );
    }

    #[tokio::test]
    async fn non_catalog_hierarchy_is_rejected() {
        let mut ctx = SessionContext::new();
        let config = unreachable_config();
        let err = register_rss_tables(
            &mut ctx,
            "news",
            Some(&config),
            false,
            HierarchyLevel::Table,
        )
        .await
        .expect_err("hierarchy_level: table must be rejected");
        assert!(
            err.to_string()
                .contains("hierarchy_level must be 'catalog'"),
            "{err}"
        );
        assert!(
            ctx.catalog("news").is_none(),
            "a rejected source must leave no catalog behind"
        );
    }

    #[tokio::test]
    async fn read_write_is_rejected() {
        let mut ctx = SessionContext::new();
        let config = unreachable_config();
        let err = register_rss_tables(
            &mut ctx,
            "news",
            Some(&config),
            true,
            HierarchyLevel::Catalog,
        )
        .await
        .expect_err("read_write must be rejected");
        assert!(
            err.to_string().contains("access_mode must be read-only"),
            "{err}"
        );
        assert!(
            ctx.catalog("news").is_none(),
            "a rejected source must leave no catalog behind"
        );
    }

    #[tokio::test]
    async fn missing_config_is_rejected() {
        let mut ctx = SessionContext::new();
        let err = register_rss_tables(&mut ctx, "news", None, false, HierarchyLevel::Catalog)
            .await
            .expect_err("a source with no `rss:` block must be rejected");
        assert!(
            err.to_string()
                .contains("missing required `rss:` configuration block"),
            "{err}"
        );
        assert!(
            ctx.catalog("news").is_none(),
            "a rejected source must leave no catalog behind"
        );
    }

    #[tokio::test]
    async fn an_invalid_config_is_rejected_before_any_catalog_appears() {
        // `validate()` runs on the registration path too, not only in the
        // server's pure config check: registration is the single enforcement
        // point, so a front-end that skipped validation cannot register a
        // source the config rules forbid.
        let mut ctx = SessionContext::new();
        let mut config = unreachable_config();
        config.max_concurrent = 0;
        let err = register_rss_tables(
            &mut ctx,
            "news",
            Some(&config),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .expect_err("an invalid config must be rejected");
        // The real source name, not `validate()`'s `<config>` placeholder:
        // the message has to name the source an operator must go edit.
        assert!(
            err.to_string()
                .contains("rss source 'news': max_concurrent"),
            "{err}"
        );
        assert!(
            ctx.catalog("news").is_none(),
            "a rejected source must leave no catalog behind"
        );
    }

    #[test]
    fn schema_metadata_carries_surface_version() {
        // Spelled out rather than read from the constant: the key and the
        // rendered value are both wire-visible, so a rename or a silent bump
        // must fail here. `RSS_SURFACE_VERSION` is logged at registration and
        // stamped into these two schemas from the same constant, so this is
        // the query-side half of what that log line reports.
        for schema in [items_schema(), feeds_schema()] {
            assert_eq!(
                schema
                    .metadata()
                    .get("skardi.rss.surface_version")
                    .map(String::as_str),
                Some("1"),
            );
        }
        assert_eq!(RSS_SURFACE_VERSION, 1);
    }
}
