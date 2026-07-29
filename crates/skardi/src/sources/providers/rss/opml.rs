//! OPML subscription-list reader, and the point where [`RssConfig`]'s two
//! mutually exclusive input forms converge into one resolved subscription
//! list.
//!
//! [`RssConfig::validate`] is deliberately zero-I/O, so it can check that
//! an `opml:` path is *present* but never what is inside it. Once this
//! module actually reads that file — on the registration path, not the
//! validate path — every check `validate()` already performs for the
//! inline `feeds:` form has to be re-run against whatever the file
//! contains: non-emptiness, name defaulting, effective-name uniqueness, and
//! http/https scheme validation were all invisible before now. [`finalize`]
//! applies all four uniformly to both input forms so there is exactly one
//! place either one's invariants are enforced.
//!
//! Gated behind the `rss` feature: this is the only file in the provider
//! that links an XML parser (`quick-xml`) today. A later task adds
//! `feed-rs` (whose own XML backend is also `quick-xml`) for parsing feed
//! bodies themselves — see the dependency comment in `Cargo.toml` for how
//! the two crate versions are kept from diverging.

use std::collections::HashSet;
use std::fs;
use std::path::Path;

use quick_xml::Reader;
use quick_xml::events::Event;

use super::ResolvedSubscription;
use super::config::RssConfig;
use super::error::RssError;

/// Resolve `config`'s inline `feeds:` list or `opml:` file into one flat,
/// ready-to-fetch subscription list.
///
/// `name` is the data source's actual name — unlike `validate()`, which
/// runs before a source has one and falls back to a `"<config>"`
/// placeholder, this function's errors are attributed to the real source.
///
/// Every later stage of the provider (fetcher, TTL cache, freshness engine,
/// per-feed partitions) consumes only the [`ResolvedSubscription`]s this
/// returns and never looks at `RssConfig`'s input shape again.
pub fn resolve_subscriptions(
    name: &str,
    config: &RssConfig,
) -> Result<Vec<ResolvedSubscription>, RssError> {
    let raw = if let Some(path) = &config.opml {
        read_opml(name, path)?
    } else if let Some(feeds) = &config.feeds {
        feeds
            .iter()
            .map(|feed| (feed.url.clone(), feed.name.clone()))
            .collect()
    } else {
        // `RssConfig::validate` rejects this shape long before a source is
        // registered; kept as a returned error rather than a panic in case
        // this function is ever reached without validation having run.
        return Err(RssError::InvalidConfig {
            name: name.to_string(),
            reason: "exactly one of `feeds` or `opml` must be set".to_string(),
        });
    };

    finalize(name, raw)
}

/// Read an OPML file into flat `(url, name)` pairs, one per `<outline
/// xmlUrl="…">`.
///
/// OPML lets subscriptions nest inside grouping outlines — an untyped,
/// folder-like `<outline text="Tech">` wrapping its feeds as children.
/// quick-xml's event stream is flat regardless of nesting depth, so no
/// recursion is needed: every `outline` start/empty tag is visited once, in
/// document order, and only those carrying an `xmlUrl` become a
/// subscription. A grouping outline with no `xmlUrl` of its own — like the
/// "Tech" folder above — is silently skipped; it is structure, not a feed.
fn read_opml(name: &str, path: &Path) -> Result<Vec<(String, Option<String>)>, RssError> {
    let content = fs::read_to_string(path).map_err(|e| RssError::OpmlUnreadable {
        name: name.to_string(),
        path: path.display().to_string(),
        reason: e.to_string(),
    })?;

    let invalid = |reason: String| RssError::InvalidConfig {
        name: name.to_string(),
        reason,
    };

    let mut reader = Reader::from_str(&content);
    let mut subs: Vec<(String, Option<String>)> = Vec::new();

    loop {
        let event = reader.read_event().map_err(|e| {
            invalid(format!(
                "OPML file '{}' is not well-formed XML: {e}",
                path.display()
            ))
        })?;

        let tag = match event {
            Event::Eof => break,
            Event::Start(tag) | Event::Empty(tag) if tag.name().as_ref() == b"outline" => tag,
            _ => continue,
        };

        let decoder = reader.decoder();
        let mut xml_url = None;
        let mut text_attr = None;
        let mut title_attr = None;

        for attr in tag.attributes() {
            let attr = attr.map_err(|e| {
                invalid(format!(
                    "OPML file '{}' has a malformed attribute: {e}",
                    path.display()
                ))
            })?;
            let decode = || -> Result<String, RssError> {
                // `decode_and_unescape_value` is deprecated in quick-xml 0.41 in
                // favor of this, and `Implicit1_0` is what its own deprecated
                // body passes. Relative to the 0.37 this bump replaced, though,
                // the behavior is *not* identical: 0.37's
                // `decode_and_unescape_value` was decode + `unescape_with`, i.e.
                // entity resolution only, while 0.41 routes through
                // `normalize_xml10_attribute_value`, which also performs XML
                // attribute-value normalization — `is_xml10_normalization_char`
                // (quick-xml-0.41.0/src/escape.rs) fires on `\t`, `\r`, `\n` and
                // `&`, so a literal tab or newline inside an OPML attribute
                // value now collapses to a space (`a\tb` → `a b`, pinned in this
                // module's tests). That is the spec-correct reading of an
                // attribute value, and it only ever touches whitespace inside a
                // feed's display name or URL, so it is kept deliberately.
                attr.decoded_and_normalized_value(quick_xml::XmlVersion::Implicit1_0, decoder)
                    .map(|v| v.into_owned())
                    .map_err(|e| {
                        invalid(format!(
                            "OPML file '{}' has a malformed attribute value: {e}",
                            path.display()
                        ))
                    })
            };
            // The OPML 2.0 spec (http://opml.org/spec2.opml) names this
            // attribute `xmlUrl`, but some real-world OPML exports lowercase
            // it to `xmlurl`; accept both rather than silently dropping
            // those subscriptions.
            match attr.key.as_ref() {
                b"xmlUrl" | b"xmlurl" => xml_url = Some(decode()?),
                b"text" => text_attr = Some(decode()?),
                b"title" => title_attr = Some(decode()?),
                _ => {}
            }
        }

        // name = `text`, else `title`, else (left as `None` here) the URL —
        // `finalize` applies that last fallback uniformly for both input
        // forms.
        if let Some(url) = xml_url {
            subs.push((url, text_attr.or(title_attr)));
        }
    }

    // Not checked here: an OPML file with zero `xmlUrl` outlines is
    // structurally fine XML, just an empty subscription list — `finalize`
    // rejects that uniformly for both input forms, so an inline `feeds: []`
    // reached without `validate()` having run first gets the same check
    // instead of silently resolving to zero subscriptions.
    Ok(subs)
}

/// Apply the checks [`RssConfig::validate`] already performs on the inline
/// `feeds:` form — a non-empty list, name defaulting, http/https scheme
/// validation, and effective-name uniqueness — to a flat `(url, name)` list
/// from either input form.
///
/// Running this uniformly for both forms (rather than only the OPML path)
/// keeps there being exactly one place either one's invariants are
/// enforced. Without the non-empty check here, `resolve_subscriptions`
/// called with an empty inline `feeds: []` ahead of `validate()` would
/// silently resolve to zero subscriptions instead of erroring the way the
/// equivalent empty-OPML case already does — an asymmetry that would trap
/// whichever later caller hit it first.
fn finalize(
    name: &str,
    raw: Vec<(String, Option<String>)>,
) -> Result<Vec<ResolvedSubscription>, RssError> {
    if raw.is_empty() {
        return Err(RssError::InvalidConfig {
            name: name.to_string(),
            reason: "at least one subscription is required".to_string(),
        });
    }

    let mut seen_names = HashSet::with_capacity(raw.len());
    let mut resolved = Vec::with_capacity(raw.len());

    for (url, sub_name) in raw {
        let parsed = url::Url::parse(&url).map_err(|e| RssError::InvalidConfig {
            name: name.to_string(),
            reason: format!("invalid subscription URL '{url}': {e}"),
        })?;
        if parsed.scheme() != "http" && parsed.scheme() != "https" {
            return Err(RssError::InvalidConfig {
                name: name.to_string(),
                reason: format!("subscription URL '{url}' must use http or https"),
            });
        }

        let effective_name = sub_name.unwrap_or_else(|| url.clone());
        if !seen_names.insert(effective_name.clone()) {
            return Err(RssError::InvalidConfig {
                name: name.to_string(),
                reason: format!("duplicate subscription name '{effective_name}'"),
            });
        }

        resolved.push(ResolvedSubscription {
            name: effective_name,
            url,
        });
    }

    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::providers::rss::FeedSubscription;
    use crate::sources::providers::rss::config::inline_config;

    #[test]
    fn opml_outlines_resolve_to_subscriptions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("subs.opml");
        std::fs::write(
            &path,
            r#"<?xml version="1.0"?>
<opml version="2.0">
  <head><title>subs</title></head>
  <body>
    <outline text="Tech">
      <outline type="rss" text="Rust Blog" xmlUrl="https://blog.rust-lang.org/feed.xml" htmlUrl="https://blog.rust-lang.org/"/>
      <outline title="TWiR" xmlUrl="https://this-week-in-rust.org/rss.xml"/>
    </outline>
    <outline xmlUrl="https://example.com/no-name.xml"/>
  </body>
</opml>"#,
        )
        .unwrap();
        let config = RssConfig {
            opml: Some(path),
            ..inline_config(vec![])
        };
        let subs = resolve_subscriptions("news", &config).unwrap();
        assert_eq!(subs.len(), 3);
        assert_eq!(
            subs[0],
            ResolvedSubscription {
                name: "Rust Blog".into(),
                url: "https://blog.rust-lang.org/feed.xml".into()
            }
        );
        assert_eq!(subs[1].name, "TWiR"); // title attr fallback
        assert_eq!(subs[2].name, "https://example.com/no-name.xml"); // name defaults to URL
    }

    #[test]
    fn literal_tabs_and_newlines_in_attribute_values_collapse_to_spaces() {
        // XML attribute-value normalization, which quick-xml applies from 0.41
        // on (see the `decode` comment above); 0.37 passed both through as-is.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("subs.opml");
        std::fs::write(
            &path,
            "<opml version=\"2.0\"><body>\
             <outline text=\"a\tb\nc\" xmlUrl=\"https://e.com/f\tx\"/>\
             </body></opml>",
        )
        .unwrap();
        let config = RssConfig {
            opml: Some(path),
            ..inline_config(vec![])
        };
        let subs = resolve_subscriptions("news", &config).unwrap();
        assert_eq!(subs[0].name, "a b c");
        assert_eq!(subs[0].url, "https://e.com/f x");
    }

    #[test]
    fn inline_feeds_resolve_without_io() {
        // No tempdir, no filesystem access at all: config.opml is None, so
        // resolve_subscriptions must never touch the filesystem for this
        // input form.
        let config = inline_config(vec![
            FeedSubscription {
                url: "https://blog.rust-lang.org/feed.xml".to_string(),
                name: Some("rust-blog".to_string()),
            },
            FeedSubscription {
                url: "https://this-week-in-rust.org/rss.xml".to_string(),
                name: None,
            },
        ]);
        let subs = resolve_subscriptions("news", &config).unwrap();
        assert_eq!(
            subs,
            vec![
                ResolvedSubscription {
                    name: "rust-blog".into(),
                    url: "https://blog.rust-lang.org/feed.xml".into()
                },
                ResolvedSubscription {
                    name: "https://this-week-in-rust.org/rss.xml".into(),
                    url: "https://this-week-in-rust.org/rss.xml".into()
                },
            ]
        );
    }

    #[test]
    fn missing_opml_file_is_opml_unreadable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("does-not-exist.opml");
        let config = RssConfig {
            opml: Some(path.clone()),
            ..inline_config(vec![])
        };
        let err = resolve_subscriptions("news", &config).unwrap_err();
        match err {
            RssError::OpmlUnreadable { path: p, .. } => assert_eq!(
                p,
                path.display().to_string(),
                "error should name the unreadable path"
            ),
            other => panic!("expected OpmlUnreadable, got {other:?}"),
        }
    }

    #[test]
    fn malformed_opml_is_invalid_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.opml");
        // Genuinely ill-formed XML: the `<outline>` here is opened with a
        // start tag (not self-closed) and never gets a matching `</outline>`
        // before `</body>` appears — an end-tag mismatch quick-xml's
        // tokenizer rejects. Plain non-XML text (e.g. "not xml at all") is
        // *not* a regression case for this branch: quick-xml is a
        // non-validating tokenizer with no root-element requirement, so
        // text with zero `<` bytes parses as one `Text` event followed by
        // `Eof` — no error — and would only exercise the (already covered
        // elsewhere) empty-subscription-list path instead.
        std::fs::write(
            &path,
            r#"<opml><body><outline xmlUrl="https://a.example/f.xml"></body>"#,
        )
        .unwrap();
        let config = RssConfig {
            opml: Some(path),
            ..inline_config(vec![])
        };
        let err = resolve_subscriptions("news", &config).unwrap_err();
        match err {
            // "well-formed XML" is unique to this branch — the
            // empty-subscription-list message (asserted in
            // `opml_without_any_xmlurl_is_rejected`) says "at least one
            // subscription" instead, so this can't pass by accidentally
            // matching that other branch.
            RssError::InvalidConfig { reason, .. } => assert!(
                reason.contains("well-formed XML"),
                "reason should flag the XML syntax error: {reason}"
            ),
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }

    #[test]
    fn opml_without_any_xmlurl_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.opml");
        std::fs::write(
            &path,
            r#"<?xml version="1.0"?>
<opml version="2.0">
  <head><title>subs</title></head>
  <body>
    <outline text="Tech"/>
  </body>
</opml>"#,
        )
        .unwrap();
        let config = RssConfig {
            opml: Some(path),
            ..inline_config(vec![])
        };
        let err = resolve_subscriptions("news", &config).unwrap_err();
        match err {
            RssError::InvalidConfig { reason, .. } => assert!(
                reason.contains("at least one subscription"),
                "reason should require at least one subscription: {reason}"
            ),
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }

    #[test]
    fn inline_empty_feed_list_is_rejected_by_resolve_too() {
        // RssConfig::validate() already rejects an empty inline `feeds: []`
        // with zero I/O, so this path is unreachable through the normal
        // validate-then-resolve registration flow. It covers `finalize`'s
        // own non-empty check directly, so `resolve_subscriptions` doesn't
        // silently resolve to zero subscriptions if it is ever reached
        // ahead of `validate()` — the same invariant
        // `opml_without_any_xmlurl_is_rejected` covers for the OPML form.
        let config = inline_config(vec![]);
        let err = resolve_subscriptions("news", &config).unwrap_err();
        match err {
            RssError::InvalidConfig { reason, .. } => assert!(
                reason.contains("at least one subscription"),
                "reason should require at least one subscription: {reason}"
            ),
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_names_across_opml_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dup.opml");
        std::fs::write(
            &path,
            r#"<?xml version="1.0"?>
<opml version="2.0">
  <head><title>subs</title></head>
  <body>
    <outline text="Same Name" xmlUrl="https://a.example/feed.xml"/>
    <outline text="Same Name" xmlUrl="https://b.example/feed.xml"/>
  </body>
</opml>"#,
        )
        .unwrap();
        let config = RssConfig {
            opml: Some(path),
            ..inline_config(vec![])
        };
        let err = resolve_subscriptions("news", &config).unwrap_err();
        match err {
            RssError::InvalidConfig { reason, .. } => assert!(
                reason.contains("duplicate subscription name"),
                "reason should flag the duplicate name: {reason}"
            ),
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }

    #[test]
    fn opml_bad_scheme_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("badscheme.opml");
        std::fs::write(
            &path,
            r#"<?xml version="1.0"?>
<opml version="2.0">
  <head><title>subs</title></head>
  <body>
    <outline text="FTP Feed" xmlUrl="ftp://example.com/feed.xml"/>
  </body>
</opml>"#,
        )
        .unwrap();
        let config = RssConfig {
            opml: Some(path),
            ..inline_config(vec![])
        };
        let err = resolve_subscriptions("news", &config).unwrap_err();
        match err {
            RssError::InvalidConfig { reason, .. } => assert!(
                reason.contains("http or https"),
                "reason should flag the bad scheme: {reason}"
            ),
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }
}
