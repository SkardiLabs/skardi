//! Sanitation-ladder parse driver.
//!
//! The rungs in `sanitize.rs` are pure byte transforms; this module applies them
//! and turns the result into a feed. Rungs apply cumulatively *before* the parse
//! rather than as a fallback after one fails — `feed-rs` answers malformed lexis
//! by silently dropping the offending element instead of erroring, so there is no
//! failure for a fallback to react to (see `parse_with_ladder`). Only rungs that
//! actually changed bytes are recorded as repairs, so a document fixed by
//! ampersand escaping alone does not claim to have been re-encoded.

use std::collections::BTreeMap;

use feed_rs::model::{Category, Entry, Feed, Link};
use serde_json::{Value, json};

use super::conformance::{
    content_type_family_note, parsed_dialect, required_field_notes, sniff_declared_dialect,
};
use super::convert::html_to_markdown;
use super::error::{MAX_ERROR_CHARS, truncate};
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

    // The guard above inspects the *raw* bytes, before the rungs run — cheap,
    // and enough on its own for any document whose input bytes already carry a
    // literal `<!DOCTYPE … [`, in either case: `refuse_internal_dtd` matches the
    // keyword with `starts_with_ignore_ascii_case` (`sanitize.rs:118`, helper at
    // `:147`), so a lowercase `<!doctype` is caught there and needs nothing
    // further. What the raw-bytes pass cannot see is a subset a rung *reveals*:
    // a control character splitting `<!DOCTY\x01PE` (which rung 2 then removes)
    // or a UTF-16 document whose ASCII is interleaved with NUL bytes (which rung
    // 1 then transcodes). Re-running the guard on the sanitized bytes closes
    // both: whatever the rungs uncover, this still sees it before the parse.
    //
    // The earlier pass is therefore defence in depth rather than the only catch
    // for any document that reaches feed-rs carrying a live subset — by
    // construction, sanitized bytes that still hold one are caught here. It
    // earns its place by refusing before three byte-level passes over a body up
    // to `max_response_bytes`, and it is the only guard that can refuse a
    // document whose prolog the rungs mangle past recognition (pinned by
    // `a_mislabelled_utf16_documents_subset_is_refused_before_the_rungs_run`).
    if family == DocFamily::Xml
        && let Err(reason) = refuse_internal_dtd(&current)
    {
        tracing::debug!(stage = "refused-internal-dtd", %reason, "rss parse refused (post-sanitation)");
        return Err(ParseFailure {
            stage: "refused-internal-dtd",
            reason,
            dialect_declared,
        });
    }

    match try_parse(&current) {
        Ok(feed) => {
            tracing::debug!(?repairs, dialect = ?feed.feed_type, "rss document parsed");
            Ok(success(feed, dialect_declared, repairs))
        }
        Err(reason) => {
            // Logged capped, at the same bound the column gets. The reason is
            // the dependency's own string, and the shapes `engine.rs`'s module
            // doc measures include ones that quote a feed-supplied token
            // verbatim and unabbreviated — so untruncated this line was bounded
            // only by `max_response_bytes`. `engine.rs` caps its own copy
            // separately (`parse_error_message` bounds the composed string,
            // prefix included), so the two do not depend on each other.
            tracing::debug!(
                stage = "strict-parse",
                reason = %truncate(&reason, MAX_ERROR_CHARS),
                "rss parse exhausted the ladder"
            );
            Err(ParseFailure {
                stage: "strict-parse",
                reason,
                dialect_declared,
            })
        }
    }
}

/// A parser that never invents an entry id.
///
/// `feed-rs` fills a missing id itself: from a content hash when there is a link
/// to hash, and from a **random UUID** otherwise. The random case is unusable as
/// identity — the same item would get a new `(feed, guid)` on every scan, breaking
/// window identity and idempotent archiving — and both cases mask the
/// "no identity at all" entry that spec decision 5 says to skip and count. Handing
/// `feed-rs` a generator that yields nothing leaves `entry.id` empty unless the
/// feed really supplied one, so the Field Mapping fallback chain (id → link →
/// skip) is what decides identity, deterministically.
fn parser() -> feed_rs::parser::Parser {
    feed_rs::parser::Builder::new()
        .id_generator(|_links, _title, _uri| String::new())
        .build()
}

fn try_parse(bytes: &[u8]) -> Result<Feed, String> {
    parser().parse(bytes).map_err(|e| e.to_string())
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

/// Feed-level fields projected onto the `feeds` table.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FeedMeta {
    pub title: Option<String>,
    pub site_url: Option<String>,
    pub description: Option<String>,
}

/// One `items` row, before Arrow encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ItemRow {
    pub guid: String,
    pub title: Option<String>,
    pub link: Option<String>,
    pub author: Option<String>,
    pub published_ms: Option<i64>,
    pub updated_ms: Option<i64>,
    pub content: Option<String>,
    pub summary: Option<String>,
    pub categories: Vec<String>,
    pub enclosure_url: Option<String>,
    pub enclosure_type: Option<String>,
    pub enclosure_length: Option<u64>,
    pub extensions_json: Option<String>,
}

/// The result of projecting a `feed-rs` model onto our row shapes.
#[derive(Debug, Clone)]
pub struct ExtractedFeed {
    pub meta: FeedMeta,
    pub items: Vec<ItemRow>,
    /// Entries dropped because they had neither an id nor a link.
    pub skipped_without_identity: usize,
}

/// Everything the engine needs from one fetched document.
#[derive(Debug, Clone)]
pub struct ParsedDocument {
    pub meta: FeedMeta,
    pub items: Vec<ItemRow>,
    pub dialect: &'static str,
    pub dialect_declared: Option<String>,
    pub conformance_notes: Vec<String>,
}

/// Project a parsed feed onto `FeedMeta` + `ItemRow`s per the Field Mapping table.
pub fn extract(feed: Feed) -> ExtractedFeed {
    let meta = FeedMeta {
        title: feed.title.as_ref().map(|t| t.content.clone()),
        site_url: preferred_link(&feed.links).map(|l| l.href.clone()),
        description: feed.description.as_ref().map(|t| t.content.clone()),
    };

    let mut items = Vec::with_capacity(feed.entries.len());
    let mut skipped_without_identity = 0;
    for entry in &feed.entries {
        match extract_entry(entry) {
            Some(row) => items.push(row),
            None => skipped_without_identity += 1,
        }
    }

    ExtractedFeed {
        meta,
        items,
        skipped_without_identity,
    }
}

/// Whether a link is an attachment to download rather than a page to visit.
///
/// Atom marks these `rel="enclosure"`. `feed-rs` turns JSON Feed attachments into
/// `rel`-less links instead, which is also how a JSON item's own URL arrives — the
/// `media_type` is what separates them, since `util::handle_link` (the RSS 0.9x/1.0/
/// 2.0 and JSON url path) sets only `href`. Without this distinction an audio
/// attachment can be promoted to the item's link, and from there to its `guid`.
fn is_attachment(link: &Link) -> bool {
    link.rel.as_deref() == Some("enclosure") || (link.rel.is_none() && link.media_type.is_some())
}

/// The link a reader should follow: the first non-attachment `rel`-less or
/// `alternate` link.
fn preferred_link(links: &[Link]) -> Option<&Link> {
    links
        .iter()
        .filter(|l| !is_attachment(l))
        .find(|l| matches!(l.rel.as_deref(), None | Some("alternate")))
}

/// `None` when the entry has neither an id nor a link — a `(feed, guid)` key
/// cannot be null, so such an entry is skipped and counted.
fn extract_entry(entry: &Entry) -> Option<ItemRow> {
    let link = preferred_link(&entry.links)
        .or_else(|| entry.links.iter().find(|l| !is_attachment(l)))
        .map(|l| l.href.clone());

    let guid = match entry.id.trim() {
        "" => link.clone()?,
        id => id.to_string(),
    };

    let enclosure = enclosure(entry);

    Some(ItemRow {
        guid,
        title: entry.title.as_ref().map(|t| t.content.clone()),
        link,
        author: entry
            .authors
            .iter()
            .map(|p| p.name.trim())
            .find(|name| !name.is_empty())
            .map(str::to_string),
        published_ms: entry.published.map(|d| d.timestamp_millis()),
        updated_ms: entry.updated.map(|d| d.timestamp_millis()),
        content: entry.content.as_ref().and_then(|c| {
            let body = c.body.as_ref()?;
            Some(render_text(body, c.content_type.subty().as_str()))
        }),
        summary: entry
            .summary
            .as_ref()
            .map(|t| render_text(&t.content, t.content_type.subty().as_str())),
        categories: categories(&entry.categories),
        enclosure_url: enclosure.url,
        enclosure_type: enclosure.content_type,
        enclosure_length: enclosure.length,
        extensions_json: extensions_json(entry, enclosure.from_media),
    })
}

/// HTML-typed bodies become Markdown; anything else is stored byte-exact.
///
/// MIME subtypes are case-insensitive (RFC 2045 §5.1). `mediatype::Name` (what
/// `.subty()` returns before a call site's `.as_str()` throws it away) already
/// implements a case-insensitive `PartialEq<&str>` — verified in
/// `mediatype-0.21.0/src/name.rs`, whose impl compares via
/// `eq_ignore_ascii_case` — so `eq_ignore_ascii_case` here restores the same
/// comparison instead of `==`, which let e.g. `type="TEXT/HTML"` store raw
/// HTML in `items.content`/`items.summary` rather than converting it.
fn render_text(body: &str, subtype: &str) -> String {
    if subtype.eq_ignore_ascii_case("html") || subtype.eq_ignore_ascii_case("xhtml") {
        html_to_markdown(body)
    } else {
        body.to_string()
    }
}

/// `term`, falling back to `label`, deduped preserving first-seen order.
fn categories(cats: &[Category]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for c in cats {
        let term = match c.term.trim() {
            "" => c.label.as_deref().unwrap_or_default().trim(),
            term => term,
        };
        if term.is_empty() || out.iter().any(|seen| seen == term) {
            continue;
        }
        out.push(term.to_string());
    }
    out
}

struct Enclosure {
    url: Option<String>,
    content_type: Option<String>,
    length: Option<u64>,
    /// Whether `media` supplied it, so `extensions_json` knows to skip that one.
    from_media: bool,
}

/// The item's primary attachment.
///
/// `feed-rs` folds RSS 2.0 `<enclosure>` and MediaRSS into `media`, but *not*
/// Atom `rel="enclosure"` links or JSON Feed attachments — both of those stay in
/// `links` (a JSON attachment arrives `rel`-less but carries a `media_type`,
/// which is what distinguishes it from the item's own URL).
fn enclosure(entry: &Entry) -> Enclosure {
    if let Some(mc) = entry
        .media
        .iter()
        .flat_map(|object| object.content.iter())
        .find(|c| c.url.is_some())
    {
        return Enclosure {
            url: mc.url.as_ref().map(|u| u.to_string()),
            content_type: mc.content_type.as_ref().map(|m| m.to_string()),
            length: mc.size,
            from_media: true,
        };
    }

    if let Some(link) = entry.links.iter().find(|l| is_attachment(l)) {
        return Enclosure {
            url: Some(link.href.clone()),
            content_type: link.media_type.clone(),
            length: link.length,
            from_media: false,
        };
    }

    Enclosure {
        url: None,
        content_type: None,
        length: None,
        from_media: false,
    }
}

/// Whatever the `feed-rs` model exposes beyond the pinned columns, as a compact
/// JSON object with deterministic key order. `None` when nothing is present.
fn extensions_json(entry: &Entry, enclosure_from_media: bool) -> Option<String> {
    let mut fields: BTreeMap<&str, Value> = BTreeMap::new();

    let media = remaining_media(entry, enclosure_from_media);
    if !media.is_empty() {
        fields.insert("media", Value::Array(media));
    }
    if let Some(source) = non_blank(entry.source.as_deref()) {
        fields.insert("source", Value::String(source.to_string()));
    }
    if let Some(rights) = non_blank(entry.rights.as_ref().map(|t| t.content.as_str())) {
        fields.insert("rights", Value::String(rights.to_string()));
    }
    if let Some(language) = non_blank(entry.language.as_deref()) {
        fields.insert("language", Value::String(language.to_string()));
    }

    if fields.is_empty() {
        return None;
    }
    serde_json::to_string(&fields).ok()
}

fn non_blank(s: Option<&str>) -> Option<&str> {
    s.map(str::trim).filter(|s| !s.is_empty())
}

/// Media objects minus the single content already surfaced as the enclosure.
fn remaining_media(entry: &Entry, enclosure_from_media: bool) -> Vec<Value> {
    let mut enclosure_still_to_skip = enclosure_from_media;
    let mut objects = Vec::new();

    for object in &entry.media {
        let mut contents = Vec::new();
        for c in &object.content {
            if enclosure_still_to_skip && c.url.is_some() {
                enclosure_still_to_skip = false;
                continue;
            }
            let mut one: BTreeMap<&str, Value> = BTreeMap::new();
            if let Some(url) = &c.url {
                one.insert("url", Value::String(url.to_string()));
            }
            if let Some(ct) = &c.content_type {
                one.insert("content_type", Value::String(ct.to_string()));
            }
            if let Some(size) = c.size {
                one.insert("size", Value::from(size));
            }
            if !one.is_empty() {
                contents.push(json!(one));
            }
        }

        let mut fields: BTreeMap<&str, Value> = BTreeMap::new();
        if !contents.is_empty() {
            fields.insert("content", Value::Array(contents));
        }
        if let Some(title) = non_blank(object.title.as_ref().map(|t| t.content.as_str())) {
            fields.insert("title", Value::String(title.to_string()));
        }
        if let Some(description) =
            non_blank(object.description.as_ref().map(|t| t.content.as_str()))
        {
            fields.insert("description", Value::String(description.to_string()));
        }
        if !object.thumbnails.is_empty() {
            fields.insert(
                "thumbnails",
                Value::Array(
                    object
                        .thumbnails
                        .iter()
                        .map(|t| Value::String(t.image.uri.clone()))
                        .collect(),
                ),
            );
        }
        if let Some(duration) = object.duration {
            fields.insert("duration_secs", Value::from(duration.as_secs()));
        }

        if !fields.is_empty() {
            objects.push(json!(fields));
        }
    }
    objects
}

/// Parse and extract in one call — the entry point the engine uses.
pub fn parse_feed_document(
    bytes: &[u8],
    content_type: Option<&str>,
) -> Result<ParsedDocument, ParseFailure> {
    let parsed = parse_with_ladder(bytes)?;
    let feed_type = parsed.feed.feed_type.clone();

    let mut conformance_notes: Vec<String> = parsed
        .repairs
        .iter()
        .map(|r| r.note().to_string())
        .collect();
    if let Some(note) = content_type_family_note(content_type, feed_type.clone()) {
        conformance_notes.push(note);
    }
    conformance_notes.extend(required_field_notes(feed_type, &parsed.feed));

    let extracted = extract(parsed.feed);
    if extracted.skipped_without_identity > 0 {
        conformance_notes.push(format!(
            "entries-without-identity: {}",
            extracted.skipped_without_identity
        ));
    }

    Ok(ParsedDocument {
        meta: extracted.meta,
        items: extracted.items,
        dialect: parsed.dialect,
        dialect_declared: parsed.dialect_declared,
        conformance_notes,
    })
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

    /// A lowercase `<!doctype`, which the guard's match used to miss because it
    /// was case-sensitive.
    ///
    /// Unlike the two numbered evasions below, this input needs no sanitation to
    /// become visible: the subset is literal in the raw bytes, and
    /// `starts_with_ignore_ascii_case` (`sanitize.rs:118`, helper at `:147`)
    /// recognizes the keyword in any case, so the refusal comes from the
    /// *pre*-sanitation guard and the rungs never run. Grouped with them because
    /// this spelling once slipped past that guard, not because it needs the
    /// post-sanitation re-run.
    #[test]
    fn lowercase_doctype_with_internal_subset_is_refused() {
        let doc = br#"<!doctype r [<!ENTITY a "b">]><r>x</r>"#;
        let err = parse_with_ladder(doc).unwrap_err();
        assert_eq!(err.stage, "refused-internal-dtd");
        assert!(
            err.reason.contains("internal DTD subset refused"),
            "{}",
            err.reason
        );
    }

    /// The pre-sanitation guard is the only one that can refuse this document, so
    /// deleting it fails a test rather than nothing.
    ///
    /// The two cases below are caught by the post-sanitation re-run, and every
    /// other refusal in this module is caught by *both* passes — sanitized bytes
    /// that still carry a live subset reach the second guard by construction,
    /// which is why the first one could be deleted with the suite staying green
    /// (measured).
    ///
    /// This document escapes that symmetry from the other side: it declares
    /// `utf-16` while carrying a byte that is not valid UTF-8, so rung 1 takes
    /// its transcoding path and decodes the whole body as UTF-16LE, pairing up
    /// ASCII bytes into CJK. `<!DOCTYPE r [` is unrecognizable afterwards — the
    /// second assertion below measures exactly that — so only the raw-bytes pass
    /// can see it.
    ///
    /// Honest about what this is: the mangled bytes hold no DTD for feed-rs to
    /// expand either, so this shape is not a live entity-expansion threat that
    /// the early guard alone averts. What it pins is that the refusal happens
    /// before the rungs, which is the guard's reason to exist (refusing early is
    /// cheaper than three byte-level passes over a body up to
    /// `max_response_bytes`) and was otherwise unobservable.
    #[test]
    fn a_mislabelled_utf16_documents_subset_is_refused_before_the_rungs_run() {
        let mut doc =
            br#"<?xml version="1.0" encoding="utf-16"?><!DOCTYPE r [<!ENTITY a "b">]><r>x"#
                .to_vec();
        doc.push(0xFF); // not valid UTF-8, so rung 1 transcodes rather than passing through
        doc.extend_from_slice(b"</r>");

        let err = parse_with_ladder(&doc).unwrap_err();
        assert_eq!(err.stage, "refused-internal-dtd");
        assert!(
            err.reason.contains("internal DTD subset refused"),
            "{}",
            err.reason
        );

        // And the post-sanitation guard could not have produced that refusal:
        // after rung 1 there is no `<!DOCTYPE` left to find.
        let (sanitized, changed) = rung_reencode_utf8(&doc);
        assert!(changed, "rung 1 must have rewritten the body");
        assert!(
            refuse_internal_dtd(&sanitized).is_ok(),
            "the transcoded body still carries a recognizable subset, so this document \
             no longer isolates the pre-sanitation guard"
        );
    }

    /// Evasion 1: the guard used to run only on raw bytes, before rung 2
    /// strips illegal control characters. `<!DOCTY\x01PE … [` does not match
    /// `<!DOCTYPE` and passes that first check; rung 2 then removes the
    /// control byte, producing a real `<!DOCTYPE … [` — which the
    /// post-sanitation guard now catches.
    #[test]
    fn control_char_split_doctype_with_internal_subset_is_refused_after_sanitation() {
        let mut doc = b"<!DOCTY".to_vec();
        doc.push(0x01);
        doc.extend_from_slice(br#"PE r [<!ENTITY lol "x">]><r>y</r>"#);
        let err = parse_with_ladder(&doc).unwrap_err();
        assert_eq!(err.stage, "refused-internal-dtd");
        assert!(
            err.reason.contains("internal DTD subset refused"),
            "{}",
            err.reason
        );
    }

    /// Evasion 2: a UTF-16LE document's `<!DOCTYPE` is invisible to the
    /// raw-bytes scanner (every ASCII byte is interleaved with a `0x00`);
    /// rung 1 transcodes it to UTF-8, and the post-sanitation guard catches
    /// the doctype rung 1 reveals.
    #[test]
    fn utf16_doctype_with_internal_subset_is_refused_after_reencoding() {
        let text = r#"<!DOCTYPE r [<!ENTITY lol "x">]><r>y</r>"#;
        let mut doc = vec![0xFF, 0xFE]; // UTF-16LE BOM
        for unit in text.encode_utf16() {
            doc.extend_from_slice(&unit.to_le_bytes());
        }
        let err = parse_with_ladder(&doc).unwrap_err();
        assert_eq!(err.stage, "refused-internal-dtd");
        assert!(
            err.reason.contains("internal DTD subset refused"),
            "{}",
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

    /// `last_error` must carry no response-body content (global constraint). The
    /// 512-char cap in the engine bounds length but does not redact, so the
    /// guarantee has to come from `feed-rs` not echoing character data. If a future
    /// version starts quoting it, this fails and a redaction filter is needed.
    #[test]
    fn parse_failure_reason_never_echoes_character_data() {
        // Truncated mid-element, with a sentinel sitting in character data.
        let doc = b"<rss version=\"2.0\"><channel><title>SHOULD-NOT-LEAK secret prose";
        let err = parse_with_ladder(doc).unwrap_err();
        assert_eq!(err.stage, "strict-parse");
        assert!(
            !err.reason.contains("SHOULD-NOT-LEAK"),
            "parse error echoed character data into last_error: {}",
            err.reason
        );
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
    fn lying_non_utf8_decl_over_utf8_bytes_is_repaired_and_noted() {
        // decl claims iso-8859-1 but the title bytes are already UTF-8 (café,
        // not Latin-1) — I3: this used to be a silent byte-level no-op that
        // mojibaked for any consumer trusting the decl.
        let doc = "<?xml version=\"1.0\" encoding=\"iso-8859-1\"?><rss version=\"2.0\"><channel><title>café</title><link>https://e.com</link><description>d</description></channel></rss>".as_bytes();
        let ok = parse_with_ladder(doc).unwrap();
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
        assert_eq!(it.content.as_deref(), Some("# Body\n\ntext")); // Markdown, not HTML
        assert_eq!(it.summary.as_deref(), Some("Sum **bold**"));
        assert_eq!(it.categories, vec!["rust", "news"]);
        assert_eq!(
            it.enclosure_url.as_deref(),
            Some("https://site.example/e.mp3")
        );
        assert_eq!(it.enclosure_type.as_deref(), Some("audio/mpeg"));
        assert_eq!(it.enclosure_length, Some(123));
        assert_eq!(parsed.items[1].guid, "https://site.example/p2"); // link fallback
        assert!(
            parsed
                .conformance_notes
                .iter()
                .any(|n| n == "entries-without-identity: 1")
        );
        assert_eq!(parsed.meta.title.as_deref(), Some("Chan"));
        assert_eq!(
            parsed.meta.site_url.as_deref(),
            Some("https://site.example/")
        );
        assert_eq!(parsed.meta.description.as_deref(), Some("D"));
    }

    #[test]
    fn atom10_maps_fields() {
        // feed-rs does not fold Atom `rel="enclosure"` links into `media`, so this
        // pins the `entry.links` fallback.
        let doc = br#"<feed xmlns="http://www.w3.org/2005/Atom">
<title>Chan</title><updated>2026-07-20T10:00:00Z</updated>
<link rel="alternate" href="https://site.example/"/>
<entry>
  <id>urn:uuid:1</id><title>Post</title>
  <link rel="alternate" href="https://site.example/p1"/>
  <link rel="enclosure" type="audio/mpeg" length="99" href="https://site.example/e.mp3"/>
  <author><name>Ada</name></author>
  <published>2026-07-20T10:00:00Z</published><updated>2026-07-21T11:00:00Z</updated>
  <content type="html">&lt;h1&gt;Body&lt;/h1&gt;</content>
  <summary type="text">plain &lt; text</summary>
  <category term="rust"/>
</entry></feed>"#;
        let parsed = parse_feed_document(doc, Some("application/atom+xml")).unwrap();
        assert_eq!(parsed.dialect, "atom");
        assert_eq!(parsed.dialect_declared.as_deref(), Some("atom-1.0"));
        assert_eq!(parsed.items.len(), 1);
        let it = &parsed.items[0];
        assert_eq!(it.guid, "urn:uuid:1");
        assert_eq!(it.link.as_deref(), Some("https://site.example/p1"));
        assert_eq!(it.author.as_deref(), Some("Ada"));
        assert!(it.published_ms.is_some() && it.updated_ms.is_some());
        assert_ne!(
            it.published_ms, it.updated_ms,
            "published and updated are distinct"
        );
        assert_eq!(it.content.as_deref(), Some("# Body"));
        // `type="text"` must pass through untouched, angle bracket and all.
        assert_eq!(it.summary.as_deref(), Some("plain < text"));
        assert_eq!(it.categories, vec!["rust"]);
        assert_eq!(
            it.enclosure_url.as_deref(),
            Some("https://site.example/e.mp3")
        );
        assert_eq!(it.enclosure_type.as_deref(), Some("audio/mpeg"));
    }

    /// I1: MIME subtypes are case-insensitive (RFC 2045 §5.1); a `type=
    /// "TEXT/HTML"` on `<content>` must still convert to Markdown, not store
    /// raw HTML. Scoped to `<content>` deliberately — feed-rs's Atom
    /// `<summary>`/`<title>`/`<rights>` handler (`atom::handle_text`, in
    /// `feed-rs-2.4.0/src/parser/atom/mod.rs`) matches its `type` attribute
    /// against literal lowercase strings and *errors the whole entry* on
    /// anything else, so a mis-cased `<summary>` never even reaches our
    /// case-sensitive comparison to trigger this bug in the first place; only
    /// `<content>`'s handler has an "unrecognized type" fallback that lets a
    /// mis-cased MIME type through as-is.
    #[test]
    fn atom_content_type_uppercase_html_still_converts_to_markdown() {
        let doc = br#"<feed xmlns="http://www.w3.org/2005/Atom">
<title>Chan</title><updated>2026-07-20T10:00:00Z</updated>
<entry>
  <id>urn:uuid:1</id><title>Post</title>
  <content type="TEXT/HTML">&lt;h1&gt;Body&lt;/h1&gt;</content>
</entry></feed>"#;
        let parsed = parse_feed_document(doc, Some("application/atom+xml")).unwrap();
        assert_eq!(
            parsed.items[0].content.as_deref(),
            Some("# Body"),
            "uppercase MIME subtype must still be treated as HTML"
        );
    }

    #[test]
    fn jsonfeed_maps_fields() {
        // feed-rs turns JSON Feed attachments into `entry.links`, not `media`.
        let doc = br#"{"version":"https://jsonfeed.org/version/1.1","title":"Chan",
"home_page_url":"https://site.example/",
"items":[{"id":"1","url":"https://site.example/p1","title":"Post",
"content_html":"<h1>Body</h1>","date_published":"2026-07-20T10:00:00Z",
"date_modified":"2026-07-21T11:00:00Z","tags":["rust","news"],
"authors":[{"name":"Ada"}],
"attachments":[{"url":"https://site.example/e.mp3","mime_type":"audio/mpeg","size_in_bytes":123}]}]}"#;
        let parsed = parse_feed_document(doc, Some("application/feed+json")).unwrap();
        assert_eq!(parsed.dialect, "json-feed-1.x");
        assert_eq!(parsed.items.len(), 1);
        let it = &parsed.items[0];
        assert_eq!(it.guid, "1");
        assert_eq!(it.link.as_deref(), Some("https://site.example/p1"));
        assert_eq!(it.author.as_deref(), Some("Ada"));
        assert_eq!(it.content.as_deref(), Some("# Body"));
        assert_eq!(it.categories, vec!["rust", "news"]);
        assert_eq!(
            it.enclosure_url.as_deref(),
            Some("https://site.example/e.mp3")
        );
        assert_eq!(it.enclosure_type.as_deref(), Some("audio/mpeg"));
        assert_eq!(it.enclosure_length, Some(123));
        assert_ne!(it.published_ms, it.updated_ms);
    }

    #[test]
    fn rss1_rdf_maps_fields() {
        let doc = br#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns="http://purl.org/rss/1.0/" xmlns:dc="http://purl.org/dc/elements/1.1/">
<channel rdf:about="https://site.example/"><title>Chan</title><link>https://site.example/</link><description>D</description></channel>
<item rdf:about="https://site.example/p1"><title>Post</title><link>https://site.example/p1</link>
<dc:creator>Ada</dc:creator><dc:date>2026-07-20T10:00:00Z</dc:date><description>Sum</description></item>
</rdf:RDF>"#;
        let parsed = parse_feed_document(doc, Some("application/rss+xml")).unwrap();
        assert_eq!(parsed.dialect, "rss-1.0");
        assert_eq!(parsed.items.len(), 1);
        let it = &parsed.items[0];
        assert!(!it.guid.is_empty(), "rdf:about or link supplies identity");
        assert_eq!(it.title.as_deref(), Some("Post"));
        assert_eq!(it.author.as_deref(), Some("Ada"));
        assert_eq!(it.published_ms, Some(1_784_541_600_000));
        assert_eq!(parsed.meta.title.as_deref(), Some("Chan"));
    }

    #[test]
    fn plain_text_content_is_never_converted() {
        let doc = br#"{"version":"https://jsonfeed.org/version/1.1","title":"C",
"items":[{"id":"1","content_text":"a < b & c"}]}"#;
        let parsed = parse_feed_document(doc, None).unwrap();
        assert_eq!(parsed.items.len(), 1);
        assert_eq!(
            parsed.items[0].content.as_deref(),
            Some("a < b & c"),
            "plain text must be stored byte-exact, not Markdown-escaped"
        );
    }

    #[test]
    fn extensions_json_is_none_for_a_bare_item() {
        let bare = br#"<rss version="2.0"><channel><title>C</title><link>https://e.com</link><description>D</description>
<item><guid>1</guid><title>T</title></item></channel></rss>"#;
        let parsed = parse_feed_document(bare, None).unwrap();
        assert_eq!(
            parsed.items[0].extensions_json, None,
            "a bare item carries no extensions"
        );
    }

    /// M1: the previous version of this test only asserted `.expect(...)`
    /// on the whole object, so a `<source>` alone (which never touches
    /// `remaining_media`) satisfied it just as well as real leftover media
    /// would — the brief's most intricate rule went untested. This asserts
    /// on the parsed JSON's actual keys instead, so `media` and `source`
    /// can't stand in for each other.
    #[test]
    fn extensions_json_media_key_is_only_leftover_media_beyond_the_enclosure() {
        let doc = br#"<rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/"><channel>
<title>C</title><link>https://e.com</link><description>D</description>
<item><guid>1</guid><title>T</title>
<enclosure url="https://e.com/a.mp3" type="audio/mpeg" length="1"/>
<media:content url="https://e.com/b.jpg" type="image/jpeg" fileSize="2"/>
</item></channel></rss>"#;
        let parsed = parse_feed_document(doc, None).unwrap();
        let ext = parsed.items[0]
            .extensions_json
            .as_deref()
            .expect("a second media:content beyond the enclosure populates extensions");
        let value: serde_json::Value = serde_json::from_str(ext).expect("valid JSON");
        let obj = value.as_object().expect("extensions_json is a JSON object");
        assert_eq!(
            obj.keys().collect::<Vec<_>>(),
            vec!["media"],
            "only the leftover media, not source/rights/language: {ext}"
        );
        let media = obj["media"].as_array().expect("media is an array");
        assert_eq!(media.len(), 1, "the enclosure's own content is excluded");
        assert_eq!(
            media[0]["content"][0]["url"], "https://e.com/b.jpg",
            "the *other* media:content survives, not the enclosure's: {ext}"
        );
        assert_eq!(ext, serde_json::to_string(&value).unwrap(), "keys sorted");
    }

    /// M1: `<source>` alone — with no media beyond the single enclosure —
    /// was meant to populate `extensions_json` with a `source` key and *no*
    /// `media` key, distinguishing this from the media-key test above. But
    /// `entry.source` (the field this crate's `extensions_json` reads) is
    /// never actually assigned by feed-rs 2.4.0, for *any* dialect: there is
    /// no `.source =` anywhere under `feed-rs-2.4.0/src/parser/**` (checked
    /// `rss2`, `atom`, `rss1`, and `json`), and the RSS2 item handler
    /// (`feed-rs-2.4.0/src/parser/rss2/mod.rs`'s `handle_item` match) has no
    /// arm for `(NS::RSS, "source")` at all — the element this fixture used
    /// is silently ignored, not mapped to the model's `source: Option<String>`
    /// (whose doc describes *Atom's* copied-source-feed-metadata concept, a
    /// different thing from RSS2's `<source url>` attribution element, and
    /// which no parser path sets either). So no XML/JSON fixture can drive
    /// this key through `parse_feed_document` today. This instead calls
    /// `extensions_json` directly against a hand-built `Entry`, pinning the
    /// mapping this crate's own code performs so the property is still
    /// covered if a future feed-rs starts populating the field.
    #[test]
    fn extensions_json_source_key_present_without_a_media_key() {
        let entry = Entry {
            source: Some("Origin".to_string()),
            ..Entry::default()
        };
        let ext = extensions_json(&entry, false)
            .expect("source populates extensions even with no leftover media");
        let value: serde_json::Value = serde_json::from_str(&ext).expect("valid JSON");
        let obj = value.as_object().expect("extensions_json is a JSON object");
        assert_eq!(
            obj.keys().collect::<Vec<_>>(),
            vec!["source"],
            "source only, no media key when nothing is left over: {ext}"
        );
        assert_eq!(obj["source"], "Origin");
    }

    /// M1: `language`, named in the original test, was never actually
    /// exercised by it. `entry.language` is populated only from the
    /// `xml:lang` attribute on an Atom entry's `<content>` *child* element
    /// (verified: `feed-rs-2.4.0/src/parser/atom/mod.rs`'s
    /// `(NS::Atom, "content")` match arm is the only place that assigns
    /// `entry.language`, via `util::handle_language_attr(&child)` where
    /// `child` is that `<content>` element) — not from `xml:lang` on the
    /// `<entry>` element itself, which the original fixture put it on and
    /// which no code path reads for this field.
    #[test]
    fn extensions_json_language_key_from_atom_entry_xml_lang() {
        let doc = br#"<feed xmlns="http://www.w3.org/2005/Atom">
<title>Chan</title><updated>2026-07-20T10:00:00Z</updated>
<entry>
  <id>urn:uuid:1</id><title>Post</title>
  <content type="text" xml:lang="fr">body</content>
</entry></feed>"#;
        let parsed = parse_feed_document(doc, Some("application/atom+xml")).unwrap();
        let ext = parsed.items[0]
            .extensions_json
            .as_deref()
            .expect("xml:lang on <content> populates extensions_json's language key");
        let value: serde_json::Value = serde_json::from_str(ext).expect("valid JSON");
        let obj = value.as_object().expect("extensions_json is a JSON object");
        assert_eq!(obj.keys().collect::<Vec<_>>(), vec!["language"]);
        assert_eq!(obj["language"], "fr");
    }

    /// Identity must be reproducible across scans, so a random id is never
    /// acceptable. Left to itself feed-rs hands an entry with no id and no link a
    /// fresh UUID on every parse; the entry is skipped and counted instead.
    #[test]
    fn item_identity_is_deterministic_and_never_a_random_uuid() {
        let doc = br#"<rss version="2.0"><channel><title>C</title><link>https://e.com</link><description>D</description>
<item><title>NoGuid</title><link>https://site.example/p2</link></item>
<item><title>NoIdentity</title></item>
</channel></rss>"#;

        // Raw feed-rs invents a different id for the identity-less entry each time.
        let bare_a = feed_rs::parser::parse(&doc[..]).unwrap();
        let bare_b = feed_rs::parser::parse(&doc[..]).unwrap();
        assert_ne!(
            bare_a.entries[1].id, bare_b.entries[1].id,
            "feed-rs no longer randomizes the id; revisit the injected generator"
        );

        let a = parse_feed_document(doc, None).unwrap();
        let b = parse_feed_document(doc, None).unwrap();
        assert_eq!(a.items, b.items, "identity is stable across parses");
        assert_eq!(
            a.items.len(),
            1,
            "the identity-less entry is skipped, not handed a UUID"
        );
        assert_eq!(a.items[0].guid, "https://site.example/p2", "link fallback");
        assert!(
            a.conformance_notes
                .iter()
                .any(|n| n == "entries-without-identity: 1")
        );
    }

    /// An attachment is something to download, not the page to visit. feed-rs puts
    /// JSON Feed attachments in `entry.links` `rel`-less, which is also how the
    /// item's own URL arrives — so without care an audio file becomes the item's
    /// link, and (via the identity fallback) its `guid`.
    #[test]
    fn a_json_attachment_is_never_mistaken_for_the_item_link() {
        let doc = br#"{"version":"https://jsonfeed.org/version/1.1","title":"C",
"items":[{"id":"only-id","title":"T",
"attachments":[{"url":"https://e.com/a.mp3","mime_type":"audio/mpeg","size_in_bytes":7}]}]}"#;
        let parsed = parse_feed_document(doc, None).unwrap();
        let it = &parsed.items[0];
        assert_eq!(it.guid, "only-id");
        assert_eq!(it.link, None, "an attachment is not a link to the item");
        assert_eq!(it.enclosure_url.as_deref(), Some("https://e.com/a.mp3"));
        assert_eq!(it.enclosure_length, Some(7));
    }

    #[test]
    fn extraction_is_deterministic_and_dedupes_categories() {
        let doc = br#"<rss version="2.0"><channel><title>C</title><link>https://e.com</link><description>D</description>
<item><guid>1</guid><title>T</title><category>rust</category><category>news</category><category>rust</category></item>
</channel></rss>"#;
        let a = parse_feed_document(doc, None).unwrap();
        let b = parse_feed_document(doc, None).unwrap();
        assert_eq!(a.items, b.items, "identical input, identical rows");
        assert_eq!(
            a.items[0].categories,
            vec!["rust", "news"],
            "deduped, original order preserved"
        );
    }
}
