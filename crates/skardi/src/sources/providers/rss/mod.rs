//! RSS/Atom subscriptions as a read-only data source (`type: rss`).
//!
//! See `docs/superpowers/specs/2026-07-22-rss-feed-support-design.md`.
pub mod config;
#[cfg(feature = "rss")]
pub mod conformance;
#[cfg(feature = "rss")]
pub mod convert;
pub mod error;
#[cfg(feature = "rss")]
pub mod parse;
#[cfg(feature = "rss")]
pub mod sanitize;
#[cfg(feature = "rss")]
pub mod schema;

pub use config::{FeedSubscription, RssConfig};
pub use error::RssError;

/// Integer version of the `feeds`/`items` public surface. Bumped only by
/// breaking changes (column removal/rename/retype, nullability tightening,
/// enum-domain repurposing, identity/window semantics changes).
pub const RSS_SURFACE_VERSION: u32 = 1;
