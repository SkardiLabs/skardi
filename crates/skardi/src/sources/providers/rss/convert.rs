//! HTML → Markdown conversion for item `content`/`summary`.
//!
//! Deterministic by contract: identical input yields byte-identical output, and
//! conversion never fails — pathological input degrades to its text content, or
//! to the empty string at worst. No HTML *tag* survives into the output:
//! `script` and `style` are dropped wholesale, and markup without a Markdown
//! equivalent is reduced to the text it contains. That is narrower than "no
//! raw HTML survives": a literal `<` can still appear as inert text when there
//! is no Markdown place to escape it into (an attribute value, e.g.
//! `title="<script>"` → a literal `<script>` in the output — pinned in this
//! module's tests), and legitimate fenced code contains `<` too
//! (`<pre><code>&lt;div&gt;</code></pre>` → a fence containing `<div>`).
//! Neither is a defect.

use htmd::HtmlToMarkdownBuilder;
use htmd::options::{BrStyle, Options};

use super::sanitize::find_sub;

/// Ceiling on HTML open-tag nesting depth before conversion is handed to `htmd`.
///
/// `htmd`'s DOM walk (`dom_walker::walk_node`/`walk_children` in
/// `htmd-0.5.5/src/dom_walker.rs`) is mutually recursive with no depth limit,
/// and overflows the stack on deeply nested input. Probing the real path
/// (`feed-rs` parse → `render_text` → `html_to_markdown`): a ~6.8 KB feed body
/// with `content:encoded` nested 600 `<div>`s deep aborted the process on a
/// 2 MiB thread stack (Tokio's default for both worker and `spawn_blocking`
/// threads); 500 nested did not. `items.content` is attacker-authored by
/// definition, so any subscribed feed can trigger this — there is no `Err` to
/// return and no unwind a `catch_unwind` could intercept, only a process
/// abort. 100 is a wide margin under that 500-600 danger zone — nothing in
/// this crate's fixtures comes close — so this only ever engages on
/// pathological input; above it, conversion degrades to tag-stripped text
/// (`strip_tags_to_text`) instead of walking the tree at all.
const MAX_HTML_DEPTH: usize = 100;

/// Convert an HTML fragment to Markdown.
pub fn html_to_markdown(html: &str) -> String {
    if max_open_tag_depth(html) > MAX_HTML_DEPTH {
        return tidy(&strip_tags_to_text(html));
    }

    let converter = HtmlToMarkdownBuilder::new()
        .options(Options {
            // A two-space hard break cannot survive the per-line trailing-whitespace
            // trim below; a backslash can.
            br_style: BrStyle::Backslash,
            // htmd defaults to `*   item` / `1.  item`; the conventional single
            // space is what downstream Markdown consumers expect.
            ul_bullet_spacing: 1,
            ol_number_spacing: 1,
            ..Options::default()
        })
        // No Markdown equivalent, and their text content is not document
        // content. A tag with no registered handler still has its children
        // walked in htmd's default (`Pure`) mode, so unknown markup reduces to
        // the text it wraps rather than vanishing — the "no handler" branch of
        // `dom_walker::walk_node` (htmd-0.5.5/src/dom_walker.rs) calls
        // `walk_children` rather than dropping the node. `noscript` and
        // `template` are exceptions to that (see this module's tests), for
        // reasons unrelated to this list. `head` is deliberately *not* here —
        // skipping it discarded a leading `<title>` or a `<noscript>`
        // fallback's text as a side effect (both can land inside an implied
        // `<head>` under html5ever's document-parsing rules), not by design.
        .skip_tags(vec!["script", "style"])
        .build();

    // `convert` is fallible only through `io::Write` against htmd's in-memory
    // buffer, which cannot fail — but the contract is never to error, so degrade.
    let md = converter.convert(html).unwrap_or_default();
    tidy(&md)
}

/// Trim trailing whitespace from every line, then the document's outer whitespace.
fn tidy(md: &str) -> String {
    let mut out = String::with_capacity(md.len());
    for line in md.lines() {
        out.push_str(line.trim_end());
        out.push('\n');
    }
    out.truncate(out.trim_end().len());
    let start = out.len() - out.trim_start().len();
    out.drain(..start);
    out
}

/// HTML void elements (WHATWG HTML spec §13.1.2): always leaves, self-closing
/// syntax or not, so they never increase nesting depth for
/// [`max_open_tag_depth`].
const VOID_ELEMENTS: &[&str] = &[
    "area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source",
    "track", "wbr",
];

fn is_void_element(tag: &str) -> bool {
    VOID_ELEMENTS.iter().any(|v| tag.eq_ignore_ascii_case(v))
}

/// Upper bound on HTML open-tag nesting depth, found in one non-recursive pass.
///
/// This is a lexical scan, not a parse — it never builds a tree, so it is safe
/// to run ahead of `htmd`'s recursive DOM walker no matter how deeply nested
/// the input is (see [`MAX_HTML_DEPTH`]). It skips comments, doctypes, and
/// processing instructions, and does not count void elements (`<br>`, `<img>`,
/// …) as increasing depth, since they never hold children — real feed content
/// commonly chains several of these (e.g. `<br>` for line breaks), and
/// counting them would trip the ceiling on ordinary, unnested content. Any
/// other construct this scan does not specifically recognize is counted as an
/// opening tag: over-counting only costs an unnecessary (but still safe)
/// fallback to tag-stripped text, while under-counting would defeat the
/// ceiling — so this errs toward the side that stays safe.
fn max_open_tag_depth(html: &str) -> usize {
    let bytes = html.as_bytes();
    let mut i = 0;
    let mut depth: usize = 0;
    let mut max_depth: usize = 0;

    while i < bytes.len() {
        if bytes[i] != b'<' {
            i += 1;
            continue;
        }
        if let Some(end) = skip_delimited_region(bytes, i, b"<!--", b"-->") {
            i = end;
            continue;
        }
        if let Some(end) = skip_delimited_region(bytes, i, b"<?", b"?>") {
            i = end;
            continue;
        }
        if bytes[i..].starts_with(b"<!") {
            i = find_byte(bytes, i, b'>').map_or(bytes.len(), |p| p + 1);
            continue;
        }

        let closing = bytes.get(i + 1) == Some(&b'/');
        let name_start = if closing { i + 2 } else { i + 1 };
        if !bytes.get(name_start).is_some_and(u8::is_ascii_alphabetic) {
            // Not tag-shaped: a stray `<` in text. Move past just it.
            i += 1;
            continue;
        }

        let name_end = tag_name_end(bytes, name_start);
        let tag = &html[name_start..name_end];
        let Some(gt) = find_tag_end(bytes, name_end) else {
            break; // unterminated tag: nothing further to scan
        };
        i = gt + 1;

        if is_void_element(tag) {
            continue;
        }
        if closing {
            depth = depth.saturating_sub(1);
        } else {
            depth += 1;
            max_depth = max_depth.max(depth);
        }
    }

    max_depth
}

/// `html` with every tag removed and the text nodes kept verbatim (no entity
/// decoding, no whitespace normalization beyond [`tidy`]'s). Used only when
/// `html` is too deeply nested for `htmd`'s recursive walker to process safely
/// ([`MAX_HTML_DEPTH`]); like [`max_open_tag_depth`], this is a lexical strip,
/// not a parse, so it does not build a tree and cannot itself recurse.
/// `script`/`style` bodies are dropped, matching the normal conversion path.
fn strip_tags_to_text(html: &str) -> String {
    let bytes = html.as_bytes();
    let mut out = String::with_capacity(html.len());
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] != b'<' {
            // `<` never appears as a continuation byte of a multi-byte UTF-8
            // sequence (those are all >= 0x80), so slicing at its position
            // cannot land inside a code point.
            let next_lt = bytes[i..]
                .iter()
                .position(|&b| b == b'<')
                .map_or(bytes.len(), |p| i + p);
            out.push_str(&html[i..next_lt]);
            i = next_lt;
            continue;
        }
        if let Some(end) = skip_delimited_region(bytes, i, b"<!--", b"-->") {
            i = end;
            continue;
        }
        if let Some(end) = skip_delimited_region(bytes, i, b"<?", b"?>") {
            i = end;
            continue;
        }
        if bytes[i..].starts_with(b"<!") {
            i = find_byte(bytes, i, b'>').map_or(bytes.len(), |p| p + 1);
            continue;
        }

        let closing = bytes.get(i + 1) == Some(&b'/');
        let name_start = if closing { i + 2 } else { i + 1 };
        if !bytes.get(name_start).is_some_and(u8::is_ascii_alphabetic) {
            // Stray `<`: drop it, same as any other tag delimiter here.
            i += 1;
            continue;
        }

        let name_end = tag_name_end(bytes, name_start);
        let tag = &html[name_start..name_end];
        let Some(gt) = find_tag_end(bytes, name_end) else {
            break;
        };

        if !closing && (tag.eq_ignore_ascii_case("script") || tag.eq_ignore_ascii_case("style")) {
            i = skip_element_body(bytes, gt + 1, tag);
        } else {
            i = gt + 1;
        }
    }

    out
}

/// End of a tag name starting at `start` (one-past-the-last name byte).
fn tag_name_end(bytes: &[u8], start: usize) -> usize {
    let mut end = start;
    while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'-') {
        end += 1;
    }
    end
}

/// First unquoted `>` at or after `from`, skipping over `"…"`/`'…'` attribute
/// values so a quoted `>` does not end the tag early.
fn find_tag_end(bytes: &[u8], from: usize) -> Option<usize> {
    let mut j = from;
    let mut quote: Option<u8> = None;
    while j < bytes.len() {
        let c = bytes[j];
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None => match c {
                b'"' | b'\'' => quote = Some(c),
                b'>' => return Some(j),
                _ => {}
            },
        }
        j += 1;
    }
    None
}

/// If `bytes[at..]` opens `open`, the index just past the matching `close` —
/// or the end of input for an unterminated region (nothing further to scan).
fn skip_delimited_region(bytes: &[u8], at: usize, open: &[u8], close: &[u8]) -> Option<usize> {
    if !bytes[at..].starts_with(open) {
        return None;
    }
    let rest = &bytes[at + open.len()..];
    Some(match find_sub(rest, close) {
        Some(p) => at + open.len() + p + close.len(),
        None => bytes.len(),
    })
}

/// First occurrence of byte `b` at or after `from` (used only where there are
/// no attribute-style quotes to worry about).
fn find_byte(bytes: &[u8], from: usize, b: u8) -> Option<usize> {
    bytes[from..].iter().position(|&c| c == b).map(|p| from + p)
}

/// Index just past a case-insensitive `</tag>` at or after `from`, or the end
/// of input if none appears (the remainder is then unreachable raw text with
/// no closing tag to resume scanning after).
fn skip_element_body(bytes: &[u8], from: usize, tag: &str) -> usize {
    let mut i = from;
    while i < bytes.len() {
        if bytes[i] == b'<' && bytes.get(i + 1) == Some(&b'/') {
            let name_end = tag_name_end(bytes, i + 2);
            if bytes[i + 2..name_end].eq_ignore_ascii_case(tag.as_bytes()) {
                return find_tag_end(bytes, name_end).map_or(bytes.len(), |gt| gt + 1);
            }
        }
        i += 1;
    }
    bytes.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structural_elements_convert_to_markdown() {
        let html = r#"<h2>Title</h2><p>Some <em>emphasis</em> and <strong>bold</strong>.</p>
<ul><li>one</li><li>two</li></ul>
<p><a href="https://example.com/a">link</a> and <img src="https://example.com/i.png" alt="alt text"></p>
<pre><code>let x = 1;</code></pre>"#;
        let md = html_to_markdown(html);
        assert!(md.contains("## Title"), "{md}");
        assert!(
            md.contains("*emphasis*") || md.contains("_emphasis_"),
            "{md}"
        );
        assert!(md.contains("**bold**"), "{md}");
        assert!(md.contains("- one") || md.contains("* one"), "{md}");
        assert!(md.contains("[link](https://example.com/a)"), "{md}");
        assert!(
            md.contains("![alt text](https://example.com/i.png)"),
            "{md}"
        );
        assert!(md.contains("let x = 1;"), "{md}");
    }

    #[test]
    fn script_and_style_are_dropped_wholesale() {
        let html =
            r#"<p>keep</p><script>alert("x")</script><style>p{color:red}</style><!-- comment -->"#;
        let md = html_to_markdown(html);
        assert!(md.contains("keep"));
        assert!(!md.contains("alert"), "{md}");
        assert!(!md.contains("color"), "{md}");
        assert!(!md.contains("comment"), "{md}");
    }

    #[test]
    fn unknown_markup_reduces_to_text_content_no_raw_html_survives() {
        let html = r#"<article data-x="1"><custom-widget>inner text</custom-widget><video controls>fallback</video></article>"#;
        let md = html_to_markdown(html);
        assert!(md.contains("inner text"));
        assert!(md.contains("fallback"));
        assert!(!md.contains('<'), "raw HTML survived: {md}");
    }

    #[test]
    fn conversion_is_deterministic() {
        let html = include_str!("fixtures/golden_probe.html");
        let a = html_to_markdown(html);
        let b = html_to_markdown(html);
        assert_eq!(a, b);
        assert!(!a.is_empty(), "the probe fixture must produce output");
        // The fixture plants `leak` inside its script, style, and comment; none of
        // the three may reach the output. Its custom element must keep its text.
        assert!(!a.contains("leak"), "script/style/comment leaked: {a}");
        assert!(a.contains("widget text"), "unknown element text lost: {a}");
    }

    #[test]
    fn javascript_href_is_preserved_as_data_not_executed_markup() {
        // The provider stores it; consumers filter schemes (spec: Security/Rendering).
        let md = html_to_markdown(r#"<a href="javascript:alert(1)">x</a>"#);
        assert!(!md.contains('<'));
    }

    #[test]
    fn tables_convert_or_reduce_to_text() {
        let html = r#"<table><thead><tr><th>h1</th><th>h2</th></tr></thead>
<tbody><tr><td>a1</td><td>a2</td></tr><tr><td>b1</td><td>b2</td></tr></tbody></table>"#;
        let md = html_to_markdown(html);
        for cell in ["h1", "h2", "a1", "a2", "b1", "b2"] {
            assert!(md.contains(cell), "cell {cell} lost: {md}");
        }
        assert!(!md.contains('<'), "raw HTML survived: {md}");
        // htmd emits GFM pipe tables — pin the actual form.
        assert!(md.contains('|'), "expected a pipe table: {md}");
    }

    #[test]
    fn empty_and_whitespace_input_yield_empty() {
        assert_eq!(html_to_markdown(""), "");
        assert_eq!(html_to_markdown("   \n\t  "), "");
        assert_eq!(html_to_markdown("<p></p>"), "");
    }

    #[test]
    fn entities_decode_to_text_not_markup() {
        // `&lt;b&gt;` decodes to the characters `<b>`, which must land in the
        // output as *text*, never as a tag a renderer would interpret.
        let md = html_to_markdown("<p>&lt;b&gt;not bold&lt;/b&gt; &amp; &#169;</p>");
        assert!(md.contains("not bold"), "{md}");
        assert!(md.contains('&') || md.contains("amp"), "{md}");
        assert!(
            md.contains('©'),
            "numeric character reference decoded: {md}"
        );
        // Every `<` must be backslash-escaped — inert text, not a tag a renderer
        // would interpret. A plain `contains("<b>")` cannot tell `\<b>` apart.
        let unescaped = md
            .char_indices()
            .filter(|(i, c)| *c == '<' && !md[..*i].ends_with('\\'))
            .count();
        assert_eq!(unescaped, 0, "unescaped `<` survived: {md}");
    }

    #[test]
    fn output_has_no_trailing_whitespace_and_hard_breaks_survive() {
        // The trim contract ("trailing whitespace per line") is incompatible with
        // a two-space hard break, so `<br>` must use a form that survives it.
        let md = html_to_markdown("<p>first line<br>second line</p>");
        assert!(md.contains("first line"), "{md}");
        assert!(md.contains("second line"), "{md}");
        assert!(
            md.lines().all(|l| l == l.trim_end()),
            "line kept trailing whitespace: {md:?}"
        );
        assert!(
            md.lines().count() > 1,
            "the hard break must survive trimming: {md:?}"
        );
        assert_eq!(md, md.trim(), "outer whitespace trimmed: {md:?}");
    }

    // --- C1: recursion ceiling -------------------------------------------

    #[test]
    fn beyond_max_depth_degrades_to_tag_stripped_text_not_markdown() {
        // ~10x MAX_HTML_DEPTH. htmd's recursive DOM walker aborts the whole
        // process well below this depth (see MAX_HTML_DEPTH's doc) — this
        // input must never reach it, so the assertions below can only pass if
        // the tag-stripped fallback ran instead.
        let depth = 1000;
        let html = format!(
            "{}<em>innermost</em>{}",
            "<div>".repeat(depth),
            "</div>".repeat(depth)
        );
        let md = html_to_markdown(&html);
        assert!(md.contains("innermost"), "{md}");
        // A real Markdown conversion of <em> wraps this in `*`/`_`; the
        // tag-stripped fallback does not, so this pins which path ran.
        assert!(
            !md.contains("*innermost*") && !md.contains("_innermost_"),
            "output looks markdown-converted, not tag-stripped: {md}"
        );
    }

    #[test]
    fn many_void_elements_in_a_row_do_not_trip_the_depth_ceiling() {
        // A long run of `<br>` (common for hard line breaks in real feed
        // content) is not nesting — void elements never hold children — so
        // this must still go through htmd's real conversion, not degrade.
        let html = format!("line{}", "<br>".repeat(200));
        let md = html_to_markdown(&html);
        assert!(md.contains("line"), "{md}");
        assert!(
            md.contains('\\'),
            "expected htmd's real <br> (BrStyle::Backslash) conversion: {md:?}"
        );
    }

    // --- I4: "no raw HTML" narrowed to "no HTML tag" ----------------------

    #[test]
    fn no_tag_survives_but_a_bare_lt_can() {
        // htmd only backslash-escapes a `<` that looks like it opens a real
        // construct (`<letter`, `</letter`, `<!…`, `<?…` — see
        // `htmd-0.5.5/src/html_escape.rs`'s `should_escape_html_like_sequence`);
        // followed by `<` or `>` as here, neither qualifies, so both pass
        // through completely unescaped.
        let md = html_to_markdown(r#""><<>>"#);
        assert_eq!(md, r#""><<>>"#);
    }

    #[test]
    fn attribute_value_lt_survives_unescaped_in_the_output() {
        // htmd consumes anything tag-shaped, but an attribute value has
        // nowhere else to put a `<`: it lands in the output unescaped.
        let md = html_to_markdown(r##"<a href="#" title="<script>">t</a>"##);
        assert_eq!(md, r##"[t](# "<script>")"##);
    }

    // --- I5: noscript / template / leading title --------------------------

    #[test]
    fn noscript_fallback_text_is_preserved_now_that_head_is_not_skipped() {
        // `<noscript>` is parsed as raw text and, per html5ever's "in head"
        // insertion-mode rules (html5ever-0.38.0/src/tree_builder/rules.rs,
        // the `<noframes> | <style> | <noscript>` arm), lands inside an
        // implied `<head>` when it is the first content parsed and scripting
        // is enabled (htmd's default). `head` used to be in `skip_tags`,
        // which discards a node's children wholesale with no handler firing
        // at all, so this text vanished as a side effect of that, not by
        // design; not skip-tagging `head` restores it.
        let md = html_to_markdown("<noscript><p>ns fallback text</p></noscript>");
        assert!(md.contains("ns fallback text"), "{md}");
    }

    #[test]
    fn leading_title_text_is_preserved_not_silently_dropped() {
        let md = html_to_markdown("<title>TITLETEXT</title><p>body</p>");
        assert!(md.contains("TITLETEXT"), "{md}");
        assert!(md.contains("body"), "{md}");
    }

    #[test]
    fn template_content_is_lost_not_reduced_to_text() {
        // Unlike other unhandled markup, `<template>`'s content genuinely
        // vanishes regardless of `skip_tags`: markup5ever_rcdom stores it in
        // a separate `template_contents` `Node` field
        // (markup5ever_rcdom-0.38.0/lib.rs), not in `.children`, and htmd's
        // walker only ever iterates `.children` (referenced but never walked
        // in htmd-0.5.5/src/dom_walker.rs's `can_combine`). Pinned here
        // because the module doc used to claim no unhandled markup's text is
        // lost.
        let md = html_to_markdown("<template><p>tpl</p></template>");
        assert_eq!(md, "", "{md}");
    }
}
