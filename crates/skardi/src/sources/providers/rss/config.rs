//! Typed configuration for an RSS/Atom subscription data source.
//!
//! Skardi's generic `options: HashMap<String, String>` cannot safely
//! represent a list of feed subscriptions or the opt-in OPML alternative, so
//! `type: rss` sources carry this typed struct instead. It compiles and
//! validates independently of the `rss` Cargo feature, so the server and CLI
//! can hold a typed config field even in builds that omit the feature; the
//! registration path that turns a validated config into queryable tables
//! (feed fetching, parsing, `main.feeds`/`main.items`) is built in later
//! tasks on top of it.
//!
//! The YAML shape matches the design spec
//! (`docs/superpowers/specs/2026-07-22-rss-feed-support-design.md`):
//!
//! ```yaml
//! rss:
//!   feeds:
//!     - url: https://blog.rust-lang.org/feed.xml
//!       name: rust-blog            # optional; defaults to the URL
//!     - url: https://this-week-in-rust.org/rss.xml
//!   # or: opml: subscriptions.opml # mutually exclusive with feeds:
//!   ttl_seconds: 900               # 0 = always live
//!   max_concurrent: 6              # in-flight fetches for THIS source; not per-host, not per-process
//!   request_timeout_seconds: 10    # per-request timeout
//!   scan_timeout_seconds: 60       # deadline for one full scan across all feeds
//!   max_response_bytes: 5242880    # decoded-body cap per feed fetch
//!   user_agent: "skardi-rss/<version> (+https://github.com/SkardiLabs/skardi)"
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;
use url::Url;

use super::ResolvedSubscription;
use super::error::RssError;

/// Default per-feed cache TTL, in seconds (15 minutes).
const DEFAULT_TTL_SECONDS: u64 = 900;
/// Default in-flight fetch bound for one `rss` source. See
/// [`RssConfig::max_concurrent`] for what it does and does not bound. `6`
/// borrows the classic HTTP/1.1 browser per-host connection limit as a
/// conservative default — large enough that a sizable subscription list is not
/// fetched one feed at a time, small enough not to hammer, and tunable per
/// source; here it caps total parallelism rather than per-host connections.
const DEFAULT_MAX_CONCURRENT: usize = 6;
/// Default timeout for a single feed HTTP request.
const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 10;
/// Default deadline for one full scan across every subscribed feed.
const DEFAULT_SCAN_TIMEOUT_SECONDS: u64 = 60;
/// Default cap on one decoded feed response body (5 MiB).
const DEFAULT_MAX_RESPONSE_BYTES: u64 = 5_242_880;

fn default_ttl_seconds() -> u64 {
    DEFAULT_TTL_SECONDS
}

fn default_max_concurrent() -> usize {
    DEFAULT_MAX_CONCURRENT
}

fn default_request_timeout_seconds() -> u64 {
    DEFAULT_REQUEST_TIMEOUT_SECONDS
}

fn default_scan_timeout_seconds() -> u64 {
    DEFAULT_SCAN_TIMEOUT_SECONDS
}

fn default_max_response_bytes() -> u64 {
    DEFAULT_MAX_RESPONSE_BYTES
}

/// Default `User-Agent` sent with every feed request: identifies Skardi and
/// its version to upstream feed hosts, with a contact URL per common bot
/// etiquette.
fn default_user_agent() -> String {
    format!(
        "skardi-rss/{} (+https://github.com/SkardiLabs/skardi)",
        env!("CARGO_PKG_VERSION")
    )
}

/// Typed configuration for `type: rss` data sources.
///
/// Exactly one of `feeds` (an inline subscription list) or `opml` (a path to
/// an OPML subscription list) must be set; [`RssConfig::validate`] enforces
/// this, along with per-subscription URL and safety-bound checks. Unknown
/// fields are rejected: a misspelled key (e.g. `ttl_secondsss`) must fail
/// loudly instead of being silently dropped and changing the config's
/// meaning.
///
/// # Example
/// ```
/// use skardi::sources::providers::rss::RssConfig;
///
/// let yaml = r#"
/// feeds:
///   - url: https://blog.rust-lang.org/feed.xml
///     name: rust-blog
/// "#;
/// let config: RssConfig = serde_yaml::from_str(yaml).unwrap();
/// config.validate().unwrap();
///
/// // Defaults follow the design spec's safety bounds.
/// assert_eq!(config.ttl_seconds, 900);
/// assert_eq!(config.max_concurrent, 6);
/// assert_eq!(config.request_timeout_seconds, 10);
/// assert_eq!(config.scan_timeout_seconds, 60);
/// assert_eq!(config.max_response_bytes, 5_242_880);
/// ```
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RssConfig {
    /// Inline subscription list. Mutually exclusive with `opml`; exactly one
    /// of the two must be set (checked by [`RssConfig::validate`]).
    #[serde(default)]
    pub feeds: Option<Vec<FeedSubscription>>,

    /// Path to an OPML file listing subscriptions. `validate()` performs no
    /// I/O, so the path is not read or resolved here — that happens on the
    /// registration path in a later task. Mutually exclusive with `feeds`.
    #[serde(default)]
    pub opml: Option<PathBuf>,

    /// Per-feed cache TTL, in seconds. `0` means always-live (every scan
    /// re-fetches every feed) — the only bound where zero is legal.
    #[serde(default = "default_ttl_seconds")]
    pub ttl_seconds: u64,

    /// Maximum number of feeds fetched concurrently for THIS source — a bound
    /// on fetch parallelism, and only that. It is neither a per-host nor a
    /// per-process bound: the engine (a later phase in this stack) holds one
    /// semaphore per registered source, so two `rss` sources in one process
    /// permit the sum, and nothing anywhere accounts per host — feeds sharing a
    /// host can receive up to `max_concurrent` concurrent requests. A real
    /// per-host politeness bound is left for the engine phase to weigh (the
    /// hostname-vs-resolved-IP-vs-CDN question has to be settled first);
    /// meanwhile host-level politeness rests on honoring `Retry-After` and TTL
    /// pacing, not on this bound.
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: usize,

    /// Timeout for a single feed HTTP request, in seconds.
    #[serde(default = "default_request_timeout_seconds")]
    pub request_timeout_seconds: u64,

    /// Total deadline for one scan across every subscribed feed, in seconds.
    #[serde(default = "default_scan_timeout_seconds")]
    pub scan_timeout_seconds: u64,

    /// Byte bound on one decoded feed response body.
    #[serde(default = "default_max_response_bytes")]
    pub max_response_bytes: u64,

    /// `User-Agent` header sent with every feed request.
    #[serde(default = "default_user_agent")]
    pub user_agent: String,
}

/// One RSS/Atom/JSON-Feed subscription in an inline `feeds:` list.
///
/// Unknown fields are rejected for the same reason as on [`RssConfig`]: a
/// misspelled key must fail loudly rather than being silently dropped.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FeedSubscription {
    /// Feed URL; must be `http://` or `https://` (checked by
    /// [`RssConfig::validate`]).
    pub url: String,

    /// Human-readable subscription name, surfaced as the `feed` column in
    /// `main.items`/`main.feeds`. Defaults to `url` when omitted, empty, or
    /// whitespace-only — the same "blank is absent" normalization the
    /// `user_agent` check applies, and what a title-less OPML outline needs:
    /// OPML 2.0 requires the `text` attribute, so exporters emit `text=""`
    /// rather than omitting it. Duplicate-name detection compares this
    /// *effective* name. A non-blank name is used exactly as written (no
    /// trimming): `"a "` and `"a"` stay two distinct names.
    #[serde(default)]
    pub name: Option<String>,
}

impl RssConfig {
    /// Validate the configuration, failing on the first problem found.
    ///
    /// Performs zero I/O — no file reads, no network — so it is safe to call
    /// from the server's pure `validate_data_sources()` path as well as from
    /// provider registration. OPML files are not read here; that happens on
    /// the registration path in a later task.
    pub fn validate(&self) -> Result<(), RssError> {
        match (&self.feeds, &self.opml) {
            (Some(_), Some(_)) => {
                return Err(invalid_config("`feeds` and `opml` are mutually exclusive"));
            }
            (None, None) => {
                return Err(invalid_config(
                    "exactly one of `feeds` or `opml` must be set",
                ));
            }
            _ => {}
        }

        if let Some(feeds) = &self.feeds {
            let raw = feeds
                .iter()
                .map(|feed| (feed.url.clone(), feed.name.clone()))
                .collect();
            finalize(raw)?;
        }

        if self.max_concurrent == 0 {
            return Err(invalid_config("max_concurrent must be at least 1"));
        }
        if self.request_timeout_seconds == 0 {
            return Err(invalid_config("request_timeout_seconds must be at least 1"));
        }
        if self.scan_timeout_seconds == 0 {
            return Err(invalid_config("scan_timeout_seconds must be at least 1"));
        }
        if self.max_response_bytes == 0 {
            return Err(invalid_config("max_response_bytes must be at least 1"));
        }
        if self.user_agent.trim().is_empty() {
            return Err(invalid_config("user_agent must not be empty"));
        }
        // Reject a UA that is non-empty but not a legal HTTP header value (an
        // embedded control character, say) here, at config load, where the
        // source name is still in scope to attribute it — rather than letting
        // it slip through to fetcher construction and surface as the nameless
        // `HttpClientBuild`. This mirrors the `HeaderValue` conversion reqwest
        // applies to `.user_agent(...)`, so validate() and the fetcher agree
        // on exactly which strings are legal.
        if reqwest::header::HeaderValue::from_str(&self.user_agent).is_err() {
            return Err(invalid_config(
                "user_agent is not a valid HTTP header value (control characters and other illegal bytes are refused)",
            ));
        }

        Ok(())
    }
}

/// Build an `RssError::InvalidConfig`. The variant is nameless by design —
/// see its doc in `error.rs`: the source name is attached by whichever
/// caller has one, not fabricated here.
fn invalid_config(reason: impl Into<String>) -> RssError {
    RssError::InvalidConfig {
        reason: reason.into(),
    }
}

/// Check and resolve a flat `(url, name)` subscription list — the single
/// implementation of the per-subscription rules both input forms share: a
/// non-empty list, name defaulting (an omitted, empty, or whitespace-only
/// name falls back to the URL — see [`FeedSubscription::name`]),
/// http/https scheme validation, and effective-name uniqueness.
///
/// Run twice by design, against the same rules both times:
/// [`RssConfig::validate`] runs it on the inline `feeds:` list at
/// config-load time (discarding the resolved output), and
/// `opml::resolve_subscriptions` runs it on either input form at
/// registration time — an `opml:` file's contents are invisible until that
/// path actually reads the file. One implementation shared by both call
/// sites is the point: a rule added here holds for the inline and OPML
/// forms alike, instead of drifting between two hand-kept copies.
pub(crate) fn finalize(
    raw: Vec<(String, Option<String>)>,
) -> Result<Vec<ResolvedSubscription>, RssError> {
    if raw.is_empty() {
        return Err(invalid_config("at least one subscription is required"));
    }

    let mut seen_names = HashSet::with_capacity(raw.len());
    let mut resolved = Vec::with_capacity(raw.len());

    for (url, sub_name) in raw {
        let parsed = Url::parse(&url)
            .map_err(|e| invalid_config(format!("invalid subscription URL '{url}': {e}")))?;
        if parsed.scheme() != "http" && parsed.scheme() != "https" {
            return Err(invalid_config(format!(
                "subscription URL '{url}' must use http or https"
            )));
        }

        // Blank is absent: only `None` falling back would let a title-less
        // OPML outline (`text=""` — OPML 2.0 requires the attribute, so
        // exporters emit it empty rather than omit it) or a `name: " "` be
        // the effective name, yielding an unqueryable `feed` value and, with
        // two such outlines, a `duplicate subscription name ''` the operator
        // cannot grep their OPML for. Same normalization as the
        // `user_agent` check. Non-blank names stay exactly as written.
        let effective_name = sub_name
            .filter(|name| !name.trim().is_empty())
            .unwrap_or_else(|| url.clone());
        if !seen_names.insert(effective_name.clone()) {
            return Err(invalid_config(format!(
                "duplicate subscription name '{effective_name}'"
            )));
        }

        resolved.push(ResolvedSubscription {
            name: effective_name,
            url,
        });
    }

    Ok(resolved)
}

/// Build a valid inline (`feeds:`-based) [`RssConfig`] from subscriptions,
/// with every other field at its spec default. A test-only convenience —
/// reused by later tasks' tests — for getting a ready-to-validate config
/// without hand-writing YAML.
#[cfg(test)]
pub(crate) fn inline_config(feeds: Vec<FeedSubscription>) -> RssConfig {
    RssConfig {
        feeds: Some(feeds),
        opml: None,
        ttl_seconds: default_ttl_seconds(),
        max_concurrent: default_max_concurrent(),
        request_timeout_seconds: default_request_timeout_seconds(),
        scan_timeout_seconds: default_scan_timeout_seconds(),
        max_response_bytes: default_max_response_bytes(),
        user_agent: default_user_agent(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            format!(
                "skardi-rss/{} (+https://github.com/SkardiLabs/skardi)",
                env!("CARGO_PKG_VERSION")
            )
        );
        assert_eq!(config.feeds.as_ref().unwrap().len(), 2);
        assert_eq!(
            config.feeds.as_ref().unwrap()[0].name.as_deref(),
            Some("rust-blog")
        );
    }

    #[test]
    fn feeds_and_opml_are_mutually_exclusive() {
        let yaml = "feeds:\n  - url: https://a.example/f.xml\nopml: subs.opml\n";
        let config: RssConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("mutually exclusive"), "{err}");
    }

    #[test]
    fn neither_feeds_nor_opml_is_rejected() {
        let config: RssConfig = serde_yaml::from_str("{}\n").unwrap();
        let err = config.validate().unwrap_err();
        assert!(
            err.to_string().contains("one of `feeds` or `opml`"),
            "{err}"
        );
    }

    #[test]
    fn empty_inline_feed_list_is_rejected() {
        let config: RssConfig = serde_yaml::from_str("feeds: []\n").unwrap();
        let err = config.validate().unwrap_err();
        assert!(
            err.to_string().contains("at least one subscription"),
            "{err}"
        );
    }

    #[test]
    fn non_http_scheme_is_rejected() {
        let yaml = "feeds:\n  - url: file:///etc/passwd\n";
        let config: RssConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("http or https"), "{err}");
    }

    #[test]
    fn malformed_url_is_rejected() {
        let yaml = "feeds:\n  - url: \"not a url\"\n";
        let config: RssConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(
            err.to_string().contains("invalid subscription URL"),
            "{err}"
        );
    }

    #[test]
    fn duplicate_subscription_names_are_rejected() {
        // Two entries sharing an explicit name.
        let yaml = "feeds:\n  - url: https://a.example/f.xml\n    name: dup\n  - url: https://b.example/f.xml\n    name: dup\n";
        let config: RssConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(
            err.to_string().contains("duplicate subscription name"),
            "{err}"
        );

        // An explicit name colliding with another entry's URL-derived
        // (unnamed) default name.
        let yaml = "feeds:\n  - url: https://c.example/other.xml\n    name: https://dup.example/feed.xml\n  - url: https://dup.example/feed.xml\n";
        let config: RssConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(
            err.to_string().contains("duplicate subscription name"),
            "{err}"
        );
    }

    #[test]
    fn blank_names_fall_back_to_the_url() {
        // Two entries with empty names — the shape a title-less OPML export
        // produces (OPML 2.0 requires `text`, so exporters emit `text=""`).
        // Before blank-is-absent, both effective names were `""` and the
        // whole source was rejected with `duplicate subscription name ''`,
        // which no operator can grep an OPML file for. Now each falls back
        // to its (distinct) URL and the pair resolves.
        let resolved = finalize(vec![
            ("https://a.example/f.xml".to_string(), Some(String::new())),
            ("https://b.example/f.xml".to_string(), Some(String::new())),
        ])
        .expect("blank names must fall back to distinct URLs");
        assert_eq!(resolved[0].name, "https://a.example/f.xml");
        assert_eq!(resolved[1].name, "https://b.example/f.xml");

        // Whitespace-only is as absent as empty — same normalization the
        // user_agent check applies.
        let resolved = finalize(vec![(
            "https://a.example/f.xml".to_string(),
            Some("   ".to_string()),
        )])
        .expect("a whitespace-only name must fall back to the URL");
        assert_eq!(resolved[0].name, "https://a.example/f.xml");

        // A non-blank name is used exactly as written — no trimming, so
        // `"a "` and `"a"` remain two distinct names.
        let resolved = finalize(vec![
            (
                "https://a.example/f.xml".to_string(),
                Some("a ".to_string()),
            ),
            ("https://b.example/f.xml".to_string(), Some("a".to_string())),
        ])
        .expect("non-blank names are not trimmed into collision");
        assert_eq!(resolved[0].name, "a ");
        assert_eq!(resolved[1].name, "a");
    }

    #[test]
    fn zero_bounds_are_rejected_except_ttl() {
        let base = inline_config(vec![FeedSubscription {
            url: "https://a.example/f.xml".to_string(),
            name: None,
        }]);

        let mut config = base.clone();
        config.max_concurrent = 0;
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("max_concurrent"), "{err}");

        let mut config = base.clone();
        config.request_timeout_seconds = 0;
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("request_timeout_seconds"), "{err}");

        let mut config = base.clone();
        config.scan_timeout_seconds = 0;
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("scan_timeout_seconds"), "{err}");

        let mut config = base.clone();
        config.max_response_bytes = 0;
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("max_response_bytes"), "{err}");

        let mut config = base;
        config.ttl_seconds = 0;
        config
            .validate()
            .expect("ttl_seconds: 0 (always-live) is legal");
    }

    #[test]
    fn empty_user_agent_is_rejected() {
        let mut config = inline_config(vec![FeedSubscription {
            url: "https://a.example/f.xml".to_string(),
            name: None,
        }]);
        config.user_agent = String::new();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("user_agent"), "{err}");
    }

    #[test]
    fn user_agent_with_a_control_char_is_rejected_at_config_load() {
        // A non-empty UA that is not a legal HTTP header value (embedded
        // newline) must fail in validate() — at config load, attributable to
        // the source — not survive to fetcher construction and surface as the
        // nameless `HttpClientBuild`. `fetch.rs`'s
        // `invalid_user_agent_fails_client_construction` covers the residual
        // direct-construction path that bypasses validate().
        let mut config = inline_config(vec![FeedSubscription {
            url: "https://a.example/f.xml".to_string(),
            name: None,
        }]);
        config.user_agent = "bad\nua".to_string();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("user_agent"), "{err}");
    }

    #[test]
    fn unknown_fields_are_rejected() {
        // Unknown top-level field: the error must name the offending field,
        // not just fail generically (a misspelled key must fail loudly with
        // a targeted message, per the same bar `open_connector/config.rs`
        // holds its own `deny_unknown_fields` tests to).
        let err = serde_yaml::from_str::<RssConfig>("feeds: []\nbogus: 1\n").unwrap_err();
        assert!(err.to_string().contains("bogus"), "{err}");

        // Unknown field within a feed subscription entry — same bar applies
        // one level down.
        let err = serde_yaml::from_str::<RssConfig>(
            "feeds:\n  - url: https://a.example/f.xml\n    bogus: 1\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("bogus"), "{err}");
    }

    #[test]
    fn opml_only_config_validates_without_reading_the_file() {
        // validate() performs zero I/O, so a path that does not exist on
        // disk must still pass — OPML reading happens at registration time.
        let config: RssConfig =
            serde_yaml::from_str("opml: does-not-exist.opml\n").expect("opml-only config parses");
        config
            .validate()
            .expect("opml-only config validates without touching the path");
    }
}
