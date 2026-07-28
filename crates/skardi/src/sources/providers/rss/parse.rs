//! Sanitation-ladder parse driver.
//!
//! The rungs in `sanitize.rs` are pure byte transforms; this module applies them
//! and turns the result into a feed. Rungs apply cumulatively *before* the parse
//! rather than as a fallback after one fails — `feed-rs` answers malformed lexis
//! by silently dropping the offending element instead of erroring, so there is no
//! failure for a fallback to react to (see `parse_with_ladder`). Only rungs that
//! actually changed bytes are recorded as repairs, so a document fixed by
//! ampersand escaping alone does not claim to have been re-encoded.

use feed_rs::model::Feed;

use super::conformance::{parsed_dialect, sniff_declared_dialect};
use super::sanitize::{
    DocFamily, Repair, detect_family, refuse_internal_dtd, rung_escape_naked_ampersands,
    rung_reencode_utf8, rung_strip_control_chars,
};

/// Why a document could not be turned into a feed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseFailure {
    /// `"refused-internal-dtd"` or `"strict-parse"` (the ladder was exhausted).
    pub stage: &'static str,
    pub reason: String,
    pub dialect_declared: Option<String>,
}

/// A parsed feed plus what it took to get there.
#[derive(Debug, Clone)]
pub struct ParseSuccess {
    pub feed: Feed,
    pub dialect: &'static str,
    pub dialect_declared: Option<String>,
    pub repairs: Vec<Repair>,
}

/// One rung of the ladder: the transform and the repair it records.
type Rung = (fn(&[u8]) -> (Vec<u8>, bool), Repair);

/// Sanitize `bytes` with the applicable rungs, then parse them into a feed.
pub fn parse_with_ladder(bytes: &[u8]) -> Result<ParseSuccess, ParseFailure> {
    let family = detect_family(bytes);
    let dialect_declared = sniff_declared_dialect(bytes, family);

    if family == DocFamily::Xml
        && let Err(reason) = refuse_internal_dtd(bytes)
    {
        tracing::debug!(stage = "refused-internal-dtd", %reason, "rss parse refused");
        return Err(ParseFailure {
            stage: "refused-internal-dtd",
            reason,
            dialect_declared,
        });
    }

    // Rungs 2 and 3 repair XML lexis and would corrupt a JSON document (a naked
    // `&` is legal inside a JSON string), so JSON climbs only the re-encode rung.
    let rungs: &[Rung] = match family {
        DocFamily::Xml => &[
            (rung_reencode_utf8, Repair::ReencodedToUtf8),
            (rung_strip_control_chars, Repair::StrippedControlChars),
            (rung_escape_naked_ampersands, Repair::EscapedNakedAmpersands),
        ],
        DocFamily::Json => &[(rung_reencode_utf8, Repair::ReencodedToUtf8)],
    };

    // Sanitation runs *before* the parse rather than as a failure-triggered
    // fallback. feed-rs does not reject malformed lexis — it succeeds while
    // silently discarding the offending element, so a single naked `&` costs the
    // whole `<title>`. A ladder driven by parse errors would therefore never fire
    // on exactly the documents it exists to rescue. Applying the rungs up front
    // is safe because each one is a byte-level no-op on well-formed input (spec
    // AC16, pinned by the conservativeness test in `sanitize.rs`): a well-formed
    // document comes through byte-identical and parses exactly as it would have.
    let mut current = bytes.to_vec();
    let mut repairs = Vec::new();
    for (rung, repair) in rungs {
        let (next, changed) = rung(&current);
        current = next;
        if changed {
            tracing::debug!(?repair, "rss sanitation rung changed bytes");
            repairs.push(*repair);
        }
    }

    match try_parse(&current) {
        Ok(feed) => {
            tracing::debug!(?repairs, dialect = ?feed.feed_type, "rss document parsed");
            Ok(success(feed, dialect_declared, repairs))
        }
        Err(reason) => {
            tracing::debug!(stage = "strict-parse", %reason, "rss parse exhausted the ladder");
            Err(ParseFailure {
                stage: "strict-parse",
                reason,
                dialect_declared,
            })
        }
    }
}

fn try_parse(bytes: &[u8]) -> Result<Feed, String> {
    feed_rs::parser::parse(bytes).map_err(|e| e.to_string())
}

fn success(feed: Feed, dialect_declared: Option<String>, repairs: Vec<Repair>) -> ParseSuccess {
    let dialect = parsed_dialect(feed.feed_type.clone());
    ParseSuccess {
        feed,
        dialect,
        dialect_declared,
        repairs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_parse_records_no_repairs() {
        let doc = br#"<rss version="2.0"><channel><title>t</title><link>https://e.com</link><description>d</description></channel></rss>"#;
        let ok = parse_with_ladder(doc).unwrap();
        assert!(ok.repairs.is_empty());
        assert_eq!(ok.dialect, "rss-2.0");
        assert_eq!(ok.dialect_declared.as_deref(), Some("rss-2.0"));
    }

    /// Why sanitation runs before the parse instead of after a failure. If a
    /// future feed-rs either rejects this document or preserves the title on its
    /// own, this test fails and the ordering decision deserves revisiting.
    #[test]
    fn feed_rs_alone_silently_drops_the_element_a_naked_ampersand_touches() {
        let doc = br#"<rss version="2.0"><channel><title>Fish & Chips</title><link>https://e.com</link><description>d</description></channel></rss>"#;
        let bare = feed_rs::parser::parse(&doc[..])
            .expect("feed-rs tolerates a naked ampersand rather than erroring");
        assert!(
            bare.title.is_none(),
            "feed-rs no longer drops the title; revisit the sanitize-first ordering"
        );
        // Through the ladder the same document keeps its title.
        let ok = parse_with_ladder(doc).unwrap();
        assert_eq!(ok.feed.title.as_ref().unwrap().content, "Fish & Chips");
    }

    #[test]
    fn naked_ampersand_document_is_rescued_with_minimal_repair_set() {
        let doc = br#"<rss version="2.0"><channel><title>Fish & Chips</title><link>https://e.com</link><description>d</description></channel></rss>"#;
        let ok = parse_with_ladder(doc).unwrap();
        // Rungs 1-2 changed nothing, so they are not recorded.
        assert_eq!(ok.repairs, vec![Repair::EscapedNakedAmpersands]);
        assert_eq!(ok.feed.title.as_ref().unwrap().content, "Fish & Chips");
    }

    #[test]
    fn billion_laughs_is_refused_not_expanded() {
        let doc = br#"<?xml version="1.0"?><!DOCTYPE lolz [<!ENTITY lol "lol"><!ENTITY lol2 "&lol;&lol;">]><rss version="2.0"><channel><title>&lol2;</title></channel></rss>"#;
        let err = parse_with_ladder(doc).unwrap_err();
        assert_eq!(err.stage, "refused-internal-dtd");
        assert!(
            err.reason.contains("internal DTD subset refused"),
            "reason names the guard: {}",
            err.reason
        );
    }

    #[test]
    fn hopeless_document_exhausts_ladder_with_strict_parse_stage() {
        let err = parse_with_ladder(b"<rss version=\"2.0\"><channel><title>truncat").unwrap_err();
        assert_eq!(err.stage, "strict-parse");
        // The declared sniff still works on garbage.
        assert_eq!(err.dialect_declared.as_deref(), Some("rss-2.0"));
        assert!(!err.reason.is_empty(), "the last feed-rs error is carried");
    }

    #[test]
    fn control_char_document_is_rescued_and_records_only_that_rung() {
        let mut doc = br#"<rss version="2.0"><channel><title>ti"#.to_vec();
        doc.push(0x08); // illegal in XML 1.0, legal UTF-8
        doc.extend_from_slice(
            br#"tle</title><link>https://e.com</link><description>d</description></channel></rss>"#,
        );
        let ok = parse_with_ladder(&doc).unwrap();
        assert_eq!(ok.repairs, vec![Repair::StrippedControlChars]);
        assert_eq!(ok.feed.title.as_ref().unwrap().content, "title");
    }

    #[test]
    fn latin1_document_is_rescued_and_records_the_reencode_rung() {
        let mut doc =
            br#"<?xml version="1.0" encoding="iso-8859-1"?><rss version="2.0"><channel><title>caf"#
                .to_vec();
        doc.push(0xE9);
        doc.extend_from_slice(
            br#"</title><link>https://e.com</link><description>d</description></channel></rss>"#,
        );
        let ok = parse_with_ladder(&doc).unwrap();
        assert_eq!(ok.repairs, vec![Repair::ReencodedToUtf8]);
        assert_eq!(ok.feed.title.as_ref().unwrap().content, "café");
    }

    #[test]
    fn json_feed_parses_and_reports_its_dialect() {
        let doc = br#"{"version":"https://jsonfeed.org/version/1.1","title":"t","items":[]}"#;
        let ok = parse_with_ladder(doc).unwrap();
        assert_eq!(ok.dialect, "json-feed-1.x");
        assert_eq!(ok.dialect_declared.as_deref(), Some("json-feed-1.1"));
        assert!(ok.repairs.is_empty());
    }

    #[test]
    fn json_family_never_runs_the_xml_repair_rungs() {
        // Naked `&` is legal inside a JSON string; the XML ampersand rung would
        // corrupt it, so the JSON path must not apply it.
        let doc =
            br#"{"version":"https://jsonfeed.org/version/1.1","title":"Fish & Chips","items":[]}"#;
        let ok = parse_with_ladder(doc).unwrap();
        assert_eq!(ok.feed.title.as_ref().unwrap().content, "Fish & Chips");
        assert!(ok.repairs.is_empty(), "{:?}", ok.repairs);
    }
}
