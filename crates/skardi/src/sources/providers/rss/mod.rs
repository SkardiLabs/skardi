//! RSS/Atom subscriptions as a read-only data source (`type: rss`).
//!
//! See `docs/superpowers/specs/2026-07-22-rss-feed-support-design.md`.
#[cfg(feature = "rss")]
pub mod cache;
pub mod config;
#[cfg(feature = "rss")]
pub mod conformance;
#[cfg(feature = "rss")]
pub mod convert;
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

pub use config::{FeedSubscription, RssConfig};
pub use error::RssError;
#[cfg(feature = "rss")]
pub use opml::resolve_subscriptions;

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
