//! Dialect sniffing and conformance observations.
//!
//! Two notions of dialect are kept apart on purpose: what the document *claims*
//! (a lexical sniff of the root element, which still works on bytes too broken
//! to parse) and what `feed-rs` actually *parsed*. Disagreement between them is
//! an observation about the feed, not an error.

use feed_rs::model::{Feed, FeedType};

use super::sanitize::{DocFamily, find_sub};

const ATOM_1_0_NS: &str = "http://www.w3.org/2005/Atom";
const ATOM_0_3_NS: &str = "http://purl.org/atom/ns#";
const JSON_FEED_VERSION_MARKER: &[u8] = b"jsonfeed.org/version/";

/// The dialect the document declares, sniffed lexically.
pub fn sniff_declared_dialect(bytes: &[u8], family: DocFamily) -> Option<String> {
    match family {
        DocFamily::Json => sniff_json_feed_version(bytes),
        DocFamily::Xml => {
            let (name, attrs) = first_start_element(bytes)?;
            let local = name.rsplit(':').next().unwrap_or(&name);
            let version = attr_value(&attrs, "version");
            let unknown = || format!("unknown:{name}");

            let dialect = if local.eq_ignore_ascii_case("RDF") {
                "rss-1.0".to_string()
            } else if local.eq_ignore_ascii_case("rss") {
                match version.as_deref() {
                    Some("2.0") => "rss-2.0".to_string(),
                    Some("0.91") => "rss-0.91".to_string(),
                    Some("0.92") => "rss-0.92".to_string(),
                    Some("0.9") => "rss-0.9".to_string(),
                    _ => unknown(),
                }
            } else if local.eq_ignore_ascii_case("feed") {
                let ns = attr_value(&attrs, "xmlns").unwrap_or_default();
                if ns == ATOM_1_0_NS {
                    "atom-1.0".to_string()
                } else if ns == ATOM_0_3_NS || version.as_deref() == Some("0.3") {
                    "atom-0.3".to_string()
                } else {
                    unknown()
                }
            } else {
                unknown()
            };
            Some(dialect)
        }
    }
}

/// `"1.1"` / `"1"` out of a JSON Feed `version` URL, found lexically so that a
/// document too broken to deserialize still reports what it claims to be.
fn sniff_json_feed_version(bytes: &[u8]) -> Option<String> {
    let at = find_sub(bytes, JSON_FEED_VERSION_MARKER)? + JSON_FEED_VERSION_MARKER.len();
    let tail = &bytes[at..];
    let end = tail
        .iter()
        .position(|c| !(c.is_ascii_digit() || *c == b'.'))
        .unwrap_or(tail.len());
    match &tail[..end] {
        b"1.1" => Some("json-feed-1.1".to_string()),
        b"1" => Some("json-feed-1".to_string()),
        _ => None,
    }
}

/// The first start element's name and raw attribute text, skipping the prolog's
/// comments, processing instructions, and declarations.
fn first_start_element(bytes: &[u8]) -> Option<(String, String)> {
    let mut i = 0;
    while i < bytes.len() {
        let rest = &bytes[i..];
        if rest[0] != b'<' {
            i += 1;
            continue;
        }
        if rest.starts_with(b"<!--") {
            i += 4 + find_sub(&rest[4..], b"-->")? + 3;
            continue;
        }
        if rest.starts_with(b"<?") {
            i += 2 + find_sub(&rest[2..], b"?>")? + 2;
            continue;
        }
        if rest.starts_with(b"<!") {
            // A declaration (doctype, cdata); not the root element.
            i += rest.iter().position(|&c| c == b'>')? + 1;
            continue;
        }

        let tag_end = rest.iter().position(|&c| c == b'>')?;
        let inner = &rest[1..tag_end];
        let name_end = inner
            .iter()
            .position(|c| c.is_ascii_whitespace() || *c == b'/')
            .unwrap_or(inner.len());
        return Some((
            String::from_utf8_lossy(&inner[..name_end]).into_owned(),
            String::from_utf8_lossy(&inner[name_end..]).into_owned(),
        ));
    }
    None
}

/// The value of attribute `want` in raw attribute text.
fn attr_value(attrs: &str, want: &str) -> Option<String> {
    let b = attrs.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] != b'=' {
            i += 1;
            continue;
        }
        // The name ends just before the `=`, modulo whitespace.
        let mut name_end = i;
        while name_end > 0 && b[name_end - 1].is_ascii_whitespace() {
            name_end -= 1;
        }
        let mut name_start = name_end;
        while name_start > 0 && is_xml_name_byte(b[name_start - 1]) {
            name_start -= 1;
        }

        let mut j = i + 1;
        while j < b.len() && b[j].is_ascii_whitespace() {
            j += 1;
        }
        let &quote = b.get(j)?;
        if quote != b'"' && quote != b'\'' {
            i += 1;
            continue;
        }
        j += 1;
        let value_start = j;
        while j < b.len() && b[j] != quote {
            j += 1;
        }
        if &attrs[name_start..name_end] == want {
            return Some(attrs[value_start..j].to_string());
        }
        i = j + 1;
    }
    None
}

fn is_xml_name_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.' | b':')
}

/// The dialect `feed-rs` parsed, as a `feeds.dialect` enum-domain value.
pub fn parsed_dialect(t: FeedType) -> &'static str {
    match t {
        FeedType::RSS0 => "rss-0.9x",
        FeedType::RSS1 => "rss-1.0",
        FeedType::RSS2 => "rss-2.0",
        FeedType::Atom => "atom",
        FeedType::JSON => "json-feed-1.x",
    }
}

/// The feed family a parsed `FeedType` belongs to.
fn parsed_family(t: &FeedType) -> &'static str {
    match t {
        FeedType::RSS0 | FeedType::RSS1 | FeedType::RSS2 => "rss",
        FeedType::Atom => "atom",
        FeedType::JSON => "json",
    }
}

/// A note when the served `Content-Type` disagrees with the parsed family.
/// Generic and absent types carry no opinion and produce no note.
pub fn content_type_family_note(content_type: Option<&str>, parsed: FeedType) -> Option<String> {
    // Drop any `; charset=…` parameters before comparing.
    let essence = content_type?.split(';').next()?.trim().to_ascii_lowercase();

    let served = match essence.as_str() {
        "application/rss+xml" | "text/rss+xml" => "rss",
        "application/atom+xml" | "text/atom+xml" => "atom",
        "application/feed+json" | "application/json" | "text/json" => "json",
        // `text/xml`, `application/xml`, `application/octet-stream` and anything
        // unrecognized name no particular family, so they cannot disagree with one.
        _ => return None,
    };

    let family = parsed_family(&parsed);
    if served == family {
        return None;
    }
    Some(format!(
        "content-type-mismatch: served {essence}, parsed {family}"
    ))
}

/// Notes for dialect-required fields the feed omitted.
pub fn required_field_notes(parsed: FeedType, feed: &Feed) -> Vec<String> {
    let mut absent: Vec<&str> = Vec::new();
    match parsed {
        FeedType::RSS2 => {
            if feed.title.is_none() {
                absent.push("channel/title");
            }
            if feed.links.is_empty() {
                absent.push("channel/link");
            }
            if feed.description.is_none() {
                absent.push("channel/description");
            }
        }
        FeedType::Atom => {
            if feed.title.is_none() {
                absent.push("feed/title");
            }
            if feed.updated.is_none() {
                absent.push("feed/updated");
            }
        }
        // RSS 0.9x/1.0 and JSON Feed required-field sets extend via the corpus
        // evidence loop (Task 17) rather than being guessed here.
        FeedType::RSS0 | FeedType::RSS1 | FeedType::JSON => {}
    }
    absent
        .into_iter()
        .map(|path| format!("missing-required-field: {path}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(doc: &str) -> Feed {
        feed_rs::parser::parse(doc.as_bytes()).expect("fixture must parse")
    }

    #[test]
    fn declared_dialects_sniff_from_root_and_version() {
        let cases: &[(&str, &str)] = &[
            (r#"<rss version="2.0"><channel/></rss>"#, "rss-2.0"),
            (r#"<rss version="0.91"><channel/></rss>"#, "rss-0.91"),
            (
                r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"/>"#,
                "rss-1.0",
            ),
            (r#"<feed xmlns="http://www.w3.org/2005/Atom"/>"#, "atom-1.0"),
            (
                r#"<feed version="0.3" xmlns="http://purl.org/atom/ns#"/>"#,
                "atom-0.3",
            ),
            (r#"<html/>"#, "unknown:html"),
        ];
        for (doc, want) in cases {
            assert_eq!(
                sniff_declared_dialect(doc.as_bytes(), DocFamily::Xml).as_deref(),
                Some(*want),
                "{doc}"
            );
        }
        assert_eq!(
            sniff_declared_dialect(
                br#"{"version":"https://jsonfeed.org/version/1.1","items":[]}"#,
                DocFamily::Json
            )
            .as_deref(),
            Some("json-feed-1.1")
        );
    }

    #[test]
    fn declared_sniff_skips_prolog_comments_and_pis() {
        let doc = concat!(
            r#"<?xml version="1.0"?>"#,
            "<!-- <feed> decoy in a comment -->",
            r#"<rss version="0.92"><channel/></rss>"#,
        );
        assert_eq!(
            sniff_declared_dialect(doc.as_bytes(), DocFamily::Xml).as_deref(),
            Some("rss-0.92"),
            "the decoy in the comment must not win"
        );
    }

    #[test]
    fn json_feed_version_1_sniffs_without_minor() {
        assert_eq!(
            sniff_declared_dialect(
                br#"{"version": "https://jsonfeed.org/version/1", "title": "t"}"#,
                DocFamily::Json
            )
            .as_deref(),
            Some("json-feed-1")
        );
    }

    #[test]
    fn parsed_dialect_maps_all_five_feedtypes() {
        assert_eq!(parsed_dialect(FeedType::RSS0), "rss-0.9x");
        assert_eq!(parsed_dialect(FeedType::RSS1), "rss-1.0");
        assert_eq!(parsed_dialect(FeedType::RSS2), "rss-2.0");
        assert_eq!(parsed_dialect(FeedType::Atom), "atom");
        assert_eq!(parsed_dialect(FeedType::JSON), "json-feed-1.x");
    }

    #[test]
    fn content_type_mismatch_notes() {
        assert_eq!(
            content_type_family_note(Some("application/rss+xml"), FeedType::Atom).as_deref(),
            Some("content-type-mismatch: served application/rss+xml, parsed atom")
        );
        // Generic and matching types carry no opinion.
        assert_eq!(
            content_type_family_note(Some("text/xml"), FeedType::Atom),
            None
        );
        assert_eq!(
            content_type_family_note(Some("application/xml"), FeedType::RSS2),
            None
        );
        assert_eq!(
            content_type_family_note(Some("application/octet-stream"), FeedType::JSON),
            None
        );
        assert_eq!(content_type_family_note(None, FeedType::Atom), None);
        assert_eq!(
            content_type_family_note(Some("application/atom+xml"), FeedType::Atom),
            None
        );
        // Charset parameters must not defeat the comparison.
        assert_eq!(
            content_type_family_note(Some("application/atom+xml; charset=utf-8"), FeedType::Atom),
            None
        );
        // Every RSS FeedType belongs to the same served family.
        assert_eq!(
            content_type_family_note(Some("application/rss+xml"), FeedType::RSS1),
            None
        );
    }

    #[test]
    fn rss2_missing_description_noted() {
        let feed = parse(
            r#"<rss version="2.0"><channel><title>t</title><link>https://e.com</link></channel></rss>"#,
        );
        let notes = required_field_notes(FeedType::RSS2, &feed);
        assert!(
            notes
                .iter()
                .any(|n| n == "missing-required-field: channel/description"),
            "{notes:?}"
        );
        assert!(
            !notes.iter().any(|n| n.contains("channel/title")),
            "a present field must not be noted: {notes:?}"
        );
    }

    #[test]
    fn atom_missing_updated_noted() {
        let feed = parse(
            r#"<feed xmlns="http://www.w3.org/2005/Atom"><title>t</title><id>urn:x</id></feed>"#,
        );
        let notes = required_field_notes(FeedType::Atom, &feed);
        assert!(
            notes
                .iter()
                .any(|n| n == "missing-required-field: feed/updated"),
            "{notes:?}"
        );
        assert!(
            !notes.iter().any(|n| n.contains("feed/title")),
            "a present field must not be noted: {notes:?}"
        );
    }

    #[test]
    fn complete_feed_yields_no_required_field_notes() {
        let feed = parse(
            r#"<rss version="2.0"><channel><title>t</title><link>https://e.com</link><description>d</description></channel></rss>"#,
        );
        assert_eq!(
            required_field_notes(FeedType::RSS2, &feed),
            Vec::<String>::new()
        );
    }
}
