//! Error taxonomy for the RSS provider.

use thiserror::Error;

/// Errors surfaced while validating or registering an RSS/Atom data source.
///
/// [`RssError::InvalidConfig`] is the only variant [`super::config::RssConfig::validate`]
/// can return today, and it runs with zero I/O — no network, no file reads —
/// so a misconfigured source fails at config-load time with a targeted
/// message rather than an opaque failure at first query. The remaining
/// variants belong to the registration path built in later tasks: `name` is
/// filled with the actual data-source name by those callers, while
/// `validate()` itself has no source name to report and uses `"<config>"`.
#[derive(Debug, Error)]
pub enum RssError {
    /// A `validate()` check failed: mutually-exclusive `feeds`/`opml`,
    /// neither set, an empty inline feed list, a malformed or non-http(s)
    /// subscription URL, a duplicate effective subscription name, or a
    /// safety bound (`max_concurrent`, `request_timeout_seconds`,
    /// `scan_timeout_seconds`, `max_response_bytes`, `user_agent`) violated.
    #[error("rss source '{name}': {reason}")]
    InvalidConfig { name: String, reason: String },

    /// The source was registered with `hierarchy_level: table`; one rss
    /// source is always one catalog exposing `main.feeds` and `main.items`.
    #[error(
        "rss source '{name}': hierarchy_level must be 'catalog' (one source is one catalog exposing main.feeds and main.items)"
    )]
    CatalogHierarchyRequired { name: String },

    /// The source requested `read_write` access; the subscription list is
    /// managed as configuration (edit + reload), not through SQL mutation.
    #[error(
        "rss source '{name}': access_mode must be read-only (the subscription list is configuration, not SQL-mutable data)"
    )]
    ReadWriteNotSupported { name: String },

    /// The data source has no `rss` config block at all.
    #[error("rss source '{name}': missing required `rss:` configuration block")]
    MissingConfig { name: String },

    /// The `opml` file could not be read at registration time (the path
    /// resolved by `validate()` without touching the filesystem).
    #[error("rss source '{name}': failed to read OPML file '{path}': {reason}")]
    OpmlUnreadable {
        name: String,
        path: String,
        reason: String,
    },

    /// The fetcher's underlying `reqwest::Client` failed to build.
    /// Reachable in practice when the configured `user_agent` is not a
    /// legal HTTP header value (a control character, say) —
    /// `RssConfig::validate` only checks it is non-empty after trimming,
    /// not that it survives `reqwest::header::HeaderValue`'s stricter
    /// validation. Unlike this enum's other variants, there is no source
    /// name to attribute this to: the fetcher is constructed once from
    /// typed parameters, not from one named data source's registration
    /// path.
    #[error("failed to build the rss fetcher's HTTP client: {reason}")]
    HttpClientBuild { reason: String },
}
