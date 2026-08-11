//! Error taxonomy for the RSS provider, plus the one length bound every
//! feed-influenced diagnostic string in this provider is held to.

use thiserror::Error;

/// Length cap, in characters, on any feed-influenced diagnostic string this
/// provider stores or logs.
///
/// Three call sites will share it — all landing in later phases of this stack,
/// none present in this PR, so the constant is exercised only by its own test
/// for now. They will share it because they are bounded by the same thing —
/// how many characters of feed-chosen text a document can push into a
/// diagnostic — rather than by coincidence:
///
/// - `feeds.last_error` (`engine.rs`, the column's only writer), including the
///   fixed literal `cache.rs` writes for an evicted-window `304`;
/// - `feeds.dialect_declared`'s `unknown:<root element>` form
///   (`conformance.rs`), built from a raw root element name of whatever length
///   the document supplies;
/// - the `debug`-level parse-failure line (`parse.rs`), which logs the
///   dependency's own reason and would otherwise be bounded only by
///   `max_response_bytes`.
///
/// A bound on *length* only. What content may reach `feeds.last_error` at all is
/// a separate question, to be argued in `engine.rs`'s module doc when it lands.
///
/// The RSS docs (`docs/rss.md` and `docs/rss/semantics.yaml`, published later
/// in this stack) will spell the number as a bare `512`; neither is Rust and
/// neither can reference this constant, so both name it as `MAX_ERROR_CHARS`'s
/// value and this is where it is defined.
pub const MAX_ERROR_CHARS: usize = 512;

/// Bound `text` to `max_chars` *characters*, cutting on a char boundary so a
/// multi-byte sequence is never split.
///
/// A length bound only: nothing here removes content, so what may appear in a
/// string this bounds is decided by which strings are passed in, not by this.
pub fn truncate(text: &str, max_chars: usize) -> String {
    match text.char_indices().nth(max_chars) {
        Some((byte_index, _)) => text[..byte_index].to_string(),
        None => text.to_string(),
    }
}

/// Errors surfaced while validating or registering an RSS/Atom data source.
///
/// [`RssError::InvalidConfig`] is the only variant [`super::config::RssConfig::validate`]
/// can return today, and it runs with zero I/O — no network, no file reads —
/// so a misconfigured source fails at config-load time with a targeted
/// message rather than an opaque failure at first query. The remaining
/// variants belong to the registration path, which does know which data
/// source it is registering and fills `name` with it.
#[derive(Debug, Error)]
pub enum RssError {
    /// A config-content check failed: mutually-exclusive `feeds`/`opml`,
    /// neither set, an empty subscription list, a malformed or non-http(s)
    /// subscription URL, a duplicate effective subscription name, a
    /// safety bound (`max_concurrent`, `request_timeout_seconds`,
    /// `scan_timeout_seconds`, `max_response_bytes`, `user_agent`) violated,
    /// or an OPML file whose contents fail those same checks.
    ///
    /// Deliberately nameless, unlike this enum's registration-only variants:
    /// the checks run where no source name is in scope
    /// (`RssConfig::validate` sees only the bare `rss:` block), so the
    /// message carries only the reason, and the layer that does know the
    /// name prefixes it when wrapping — the server's config loader
    /// (`ConfigError::InvalidRssConfig`) and registration wrapper
    /// (`ConfigError::DataSourceRegistrationFailed`) both do. The same
    /// arrangement as `OpenConnectorError`'s validate-time variants, and
    /// what keeps a placeholder name from ever leaking into an
    /// operator-facing message.
    #[error("{reason}")]
    InvalidConfig { reason: String },

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

    /// The fetcher's underlying `reqwest::Client` failed to build. The one
    /// known trigger — a `user_agent` that is not a legal HTTP header value
    /// (a control character, say) — is now caught earlier: `RssConfig::validate`
    /// runs the same `reqwest::header::HeaderValue` check at config load and
    /// rejects it there, with the source name still in scope, as a named
    /// `InvalidConfig`. This variant therefore remains reachable only for a
    /// fetcher built from typed parameters that never passed through
    /// `validate()` (this crate's own tests, or any future caller that skips
    /// it). Unlike this enum's other variants, there is no source name to
    /// attribute it to: the fetcher is constructed once from typed parameters,
    /// not from one named data source's registration path.
    #[error("failed to build the rss fetcher's HTTP client: {reason}")]
    HttpClientBuild { reason: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The published number, spelled literally.
    ///
    /// The RSS docs (`docs/rss.md` and `docs/rss/semantics.yaml`, published
    /// later in this stack) will state 512 as this provider's
    /// diagnostic-length contract, and neither is Rust: neither can reference
    /// the constant, so neither would notice it changing. Every other
    /// assertion in this crate compares against `MAX_ERROR_CHARS` itself and so
    /// agrees with any value it is given — verified by mutation (512 → 200 and
    /// 512 → 999 both left the suite green). Same discipline as `mod.rs`'s
    /// `schema_metadata_carries_surface_version`, which spells `"1"` rather than
    /// reading `RSS_SURFACE_VERSION`. Once those docs land, the phase that adds
    /// them owns keeping their literal in step with this test (see the phase-4
    /// note); until then this pins the value against silent drift.
    #[test]
    fn max_error_chars_is_the_number_the_docs_publish() {
        assert_eq!(
            MAX_ERROR_CHARS, 512,
            "the RSS docs (published later in this stack) will state 512; keep them in step"
        );
    }

    #[test]
    fn truncate_bounds_length_on_char_boundaries() {
        let long = "é".repeat(1_000);
        let cut = truncate(&long, MAX_ERROR_CHARS);
        assert_eq!(cut.chars().count(), MAX_ERROR_CHARS);
        // Characters, not bytes: a 512-char run of 2-byte scalars is 1024 bytes.
        assert_eq!(cut.len(), MAX_ERROR_CHARS * 2);
        assert_eq!(truncate("short", MAX_ERROR_CHARS), "short");
        assert_eq!(truncate("", MAX_ERROR_CHARS), "");
    }
}
