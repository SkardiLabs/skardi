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

/// Elements `html5ever` inserts and immediately pops in body content, so they
/// never hold children and never increase nesting depth for
/// [`max_open_tag_depth`].
///
/// Taken from the arms of `tree_builder/rules.rs` (html5ever-0.38.0) that call
/// `insert_and_pop_element_for` in the "in body" insertion mode — `<area> |
/// <br> | <embed> | <img> | <keygen> | <wbr>`, `<input>`, `<param> | <source> |
/// <track>`, `<hr>` — plus `<base> | <basefont> | <bgsound> | <link> | <meta>`,
/// which "in body" routes to the "in head" mode that pops them the same way.
/// Anything not listed here is counted as nesting, which can only over-count —
/// `<col>` is left off on purpose, since it is popped only in the table modes
/// and merely *ignored* in body content (the `<caption> | <col> | … | <tr>` arm
/// there reports it and returns), and paying an unnecessary degrade for a table
/// with more than `MAX_HTML_DEPTH` columns is the cheap side of the trade.
///
/// The list only holds inside HTML content. In foreign content — anywhere under
/// `<svg>` or `<math>` — a tag not on the tree builder's breakout list is an
/// ordinary foreign element that nests, so [`max_open_tag_depth`] suspends the
/// list there; `"<svg>".to_owned() + &"<wbr>".repeat(400)` measures at DOM depth
/// 404 while a scan honoring the list scored 2.
const VOID_ELEMENTS: &[&str] = &[
    "area", "base", "basefont", "bgsound", "br", "embed", "hr", "img", "input", "keygen", "link",
    "meta", "param", "source", "track", "wbr",
];

fn is_void_element(tag: &str) -> bool {
    VOID_ELEMENTS.iter().any(|v| tag.eq_ignore_ascii_case(v))
}

/// The two elements that switch the tree builder into foreign content, where
/// [`VOID_ELEMENTS`] stops applying and a self-closing tag really does close.
fn is_foreign_root(tag: &str) -> bool {
    tag.eq_ignore_ascii_case("svg") || tag.eq_ignore_ascii_case("math")
}

/// Upper bound on the element nesting depth `html5ever` will build from `html`,
/// found in one non-recursive pass and capped at `MAX_HTML_DEPTH + 1` (all the
/// caller needs in order to compare against the ceiling).
///
/// This is a lexical scan, not a parse — it never builds a tree, so it is safe
/// to run ahead of `htmd`'s recursive DOM walker no matter how deeply nested
/// the input is (see [`MAX_HTML_DEPTH`]). Its one obligation is never to
/// *under*-count: under-counting is what lets a payload through to the walker
/// and aborts the process, while over-counting only costs an unnecessary (but
/// still safe) fallback to tag-stripped text. So every construct it does not
/// specifically recognize is counted as an opening tag, and nothing an attacker
/// writes may talk the count downward:
///
/// * A close tag pops only when it matches the name on top of the open-element
///   stack. html5ever's `process_end_tag_in_body`
///   (html5ever-0.38.0/src/tree_builder/mod.rs) walks its open elements looking
///   for the name and gives up without popping once it meets a "special" tag
///   (`div` among them, `tag_sets.rs`), so `<div></b>` leaves the `div` open.
///   Decrementing on *every* close tag, as this scan used to, scored
///   `"<div></b>".repeat(1000)` as depth 1 against a real DOM depth of 1003.
/// * Comments and bogus comments end where the tokenizer ends them, not where
///   symmetry suggests — see [`skip_comment_or_declaration`].
/// * A quote only opens an attribute value directly after an `=`; see
///   [`find_tag_end`].
///
/// A matching close tag can still close *more* than this pops (implied end
/// tags, the adoption agency), and tags inside a `script`/`style`/`title` body
/// are counted even though the tokenizer treats them as raw text: both make the
/// real tree shallower than this estimate. The one direction html5ever goes
/// *deeper* than the open-tag count is implied *start* tags — `<table><td>`
/// builds `table > tbody > tr > td`, four levels from two tags — which
/// inflates the real depth by a bounded factor per tag (measured at 2x for that
/// case), on top of the fixed `document > html > body` wrapper worth 3. The gap
/// between the 100 ceiling and the 500-600 depth where the walker actually
/// overflows absorbs both.
fn max_open_tag_depth(html: &str) -> usize {
    let bytes = html.as_bytes();
    let mut i = 0;
    // Names of the currently open elements, innermost last. Bounded by
    // `MAX_HTML_DEPTH + 1` entries: the scan returns the moment it passes the
    // ceiling, so hostile nesting never allocates proportional to its depth.
    let mut open: Vec<&str> = Vec::with_capacity(16);
    let mut max_depth: usize = 0;
    // How many `svg`/`math` start tags are open and unclosed. This is *not* a
    // reliable answer to "is the tree builder in foreign content here", and is
    // only ever read where believing it too readily costs an over-count:
    //
    // * Trusted to suspend the [`VOID_ELEMENTS`] skip below. Wrong in the
    //   direction of counting a void element as nesting, which over-counts.
    // * *Not* trusted to decide that a self-closing tag closes itself. That
    //   would under-count, and under-counting is the vulnerability.
    //
    // The reason it cannot be trusted is that html5ever leaves foreign content
    // with no close tag for this scan to see, two ways: the breakout arm at
    // `tree_builder/rules.rs:1618-1624` (`<b>`, `<br>`, `<div>`, `<table>`, …)
    // calls `unexpected_start_tag_in_foreign_content` (`mod.rs:1829-1837`),
    // which pops back to HTML content and re-steps the token there; and the
    // HTML integration points (`svg foreignObject|desc|title`, mathml
    // `mi|mo|mn|ms|mtext` — `tag_sets.rs:89-107`) parse their contents as HTML.
    // A mismatched close tag under `<svg>` leaves it stale too. Tracking those
    // exactly would mean replicating both sets, a far larger correctness
    // surface for no safety gain, so this scan does not try.
    let mut foreign: usize = 0;

    while i < bytes.len() {
        if bytes[i] != b'<' {
            i += 1;
            continue;
        }
        if let Some(end) = skip_comment_or_declaration(bytes, i) {
            i = end;
            continue;
        }

        let closing = bytes.get(i + 1) == Some(&b'/');
        let name_start = if closing { i + 2 } else { i + 1 };
        if !bytes.get(name_start).is_some_and(u8::is_ascii_alphabetic) {
            // `<` not followed by an ASCII letter. For `<x`, `states::TagOpen`
            // emits the `<` as text and reconsumes in `Data`, which is exactly
            // this. For `</x`, `states::EndTagOpen` starts a bogus comment
            // running to the next `>`; scanning that comment's body as markup
            // instead can only over-count, so it is left alone.
            i += 1;
            continue;
        }

        let name_end = tag_name_end(bytes, name_start);
        let tag = &html[name_start..name_end];
        let Some((gt, self_closing)) = find_tag_end(bytes, name_end) else {
            break; // EOF inside a tag: html5ever emits no token for it either
        };
        i = gt + 1;

        if foreign == 0 && is_void_element(tag) {
            continue;
        }
        if closing {
            if open.last().is_some_and(|top| top.eq_ignore_ascii_case(tag)) {
                let top = open.pop();
                if top.is_some_and(is_foreign_root) {
                    foreign -= 1;
                }
            }
        } else {
            // A self-closing tag closes itself in foreign content only. In HTML
            // content the flag is acknowledged and then ignored, so `<div/>`
            // opens a `div` there.
            //
            // Honored only for `<svg>`/`<math>` themselves, never for anything
            // merely *under* one: `foreign` goes stale without a close tag (see
            // its declaration above), and skipping the push on a stale count
            // under-counts without bound — `"<svg>" + "<div/>".repeat(1000)`
            // scanned as depth 1 against a real DOM depth of 1003, and reached
            // htmd's walker, which aborted the process. The price is a
            // deliberate over-count on a genuine `<svg>` holding more than
            // `MAX_HTML_DEPTH` self-closing children (`<svg>` + `<path/>`x1000
            // scans 101 against a real depth of 5, so it degrades to text) —
            // the same safe side of the trade [`VOID_ELEMENTS`] takes for
            // `<col>`.
            if self_closing && is_foreign_root(tag) {
                continue;
            }
            if is_foreign_root(tag) {
                foreign += 1;
            }
            open.push(tag);
            max_depth = max_depth.max(open.len());
            if max_depth > MAX_HTML_DEPTH {
                return max_depth;
            }
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
        if let Some(end) = skip_comment_or_declaration(bytes, i) {
            i = end;
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
        let Some((gt, _)) = find_tag_end(bytes, name_end) else {
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

/// Whitespace that ends a tag name or an attribute name in the tokenizer:
/// `'\t' | '\n' | '\x0C' | ' '`, as spelled in every one of the tag states
/// (html5ever-0.38.0/src/tokenizer/mod.rs). `\r` is not among them — it is an
/// ordinary name character there, and appears only in
/// `states::BeforeAttributeValue`'s skip set.
fn is_tag_space(c: u8) -> bool {
    matches!(c, b'\t' | b'\n' | b'\x0C' | b' ')
}

/// End of a tag name starting at `start` (one-past-the-last name byte).
///
/// `states::TagName` consumes everything except whitespace, `/` and `>` into
/// the name, so `_`, `:` and `.` are name characters — stopping at them, as
/// this used to, made `<div_>` and `</div>` compare equal and let
/// `"<div_></div>".repeat(n)` pop a stack html5ever never pops.
fn tag_name_end(bytes: &[u8], start: usize) -> usize {
    let mut end = start;
    while end < bytes.len() && !is_tag_space(bytes[end]) && !matches!(bytes[end], b'/' | b'>') {
        end += 1;
    }
    end
}

/// Index of the `>` that ends the tag whose name ended at `from`, plus whether
/// the tag carries the self-closing flag — or `None` at EOF inside the tag,
/// where html5ever emits no token at all.
///
/// The flag is set only by a `/` consumed in `states::SelfClosingStartTag`'s
/// predecessors, never by one inside an unquoted attribute value, where `/` is
/// an ordinary value character: `<svg a=x/>` is *not* self-closing, and taking
/// it for one would make `"<svg>" + "<path a=x/>".repeat(n)` undercount.
///
/// Quoting starts only in `states::BeforeAttributeValue`, i.e. directly after an
/// attribute name's `=` (html5ever-0.38.0/src/tokenizer/mod.rs). Everywhere else
/// a `"` or `'` is an ordinary attribute-name character:
/// `states::BeforeAttributeName` and `states::AttributeName` push it onto the
/// name and only flag a parse error, and `>` still ends the tag there. Treating
/// every quote as opening a value, as this used to, let a single `<b '>` swallow
/// the rest of the document — `"<b '>"` followed by 1000 `<div>`s scanned as
/// depth 0 against a real DOM depth of 1004.
fn find_tag_end(bytes: &[u8], from: usize) -> Option<(usize, bool)> {
    /// Which of the tokenizer's tag states the scan stands in.
    /// `states::SelfClosingStartTag` and `states::AfterAttributeValueQuoted`
    /// both reconsume in `BeforeAttributeName` for everything except `>`, so
    /// they need no state of their own.
    #[derive(Clone, Copy)]
    enum S {
        BeforeName,
        Name,
        AfterName,
        UnquotedValue,
    }

    let mut j = from;
    let mut state = S::BeforeName;
    let mut self_closing = false;
    while j < bytes.len() {
        let c = bytes[j];
        if c == b'>' {
            // Every one of these states emits the tag on `>`.
            return Some((j, self_closing));
        }
        self_closing = c == b'/' && !matches!(state, S::UnquotedValue);
        j += 1;
        state = match (state, c) {
            // `=` separates a name from its value only once a name has begun.
            // In `states::BeforeAttributeName` it *is* the name's first
            // character, so it does not open a value there.
            (S::Name | S::AfterName, b'=') => {
                // `states::BeforeAttributeValue`: skip whitespace (`\r`
                // included, the one state where it counts), then a `"`/`'`
                // opens a value running to the matching quote — a character
                // reference inside cannot contain one, so a plain search for it
                // is exact. Anything else is an unquoted value.
                while bytes.get(j).is_some_and(|&c| is_tag_space(c) || c == b'\r') {
                    j += 1;
                }
                match bytes.get(j) {
                    Some(&q @ (b'"' | b'\'')) => {
                        // Unterminated: EOF inside a tag emits no token.
                        j = find_byte(bytes, j + 1, q)? + 1;
                        S::BeforeName
                    }
                    _ => S::UnquotedValue,
                }
            }
            // An unquoted value ends at whitespace or `>`; `/` is part of it.
            (S::UnquotedValue, c) if is_tag_space(c) => S::BeforeName,
            (S::UnquotedValue, b'&') => {
                // `states::AttributeValue(Unquoted)` hands `&` to
                // `consume_char_ref`, so a `>` that belongs to a reference does
                // not end the tag. Only a `;`-terminated run is skipped, and
                // the run's character set excludes `>`, so this can never skip
                // *past* a tag end — which is what would let the scan resume
                // inside a tag and pop an element html5ever left open.
                let run = bytes[j..]
                    .iter()
                    .take_while(|b| b.is_ascii_alphanumeric() || **b == b'#')
                    .count();
                if bytes.get(j + run) == Some(&b';') {
                    j += run + 1;
                }
                S::UnquotedValue
            }
            (S::UnquotedValue, _) => S::UnquotedValue,
            // Whitespace and `/` keep these two waiting for a name.
            (S::BeforeName | S::AfterName, c) if is_tag_space(c) || c == b'/' => state,
            (S::Name, c) if is_tag_space(c) => S::AfterName,
            (S::Name, b'/') => S::BeforeName,
            _ => S::Name,
        };
    }
    None
}

/// If a comment, markup declaration or bogus comment starts at `at`, the index
/// just past where html5ever's tokenizer ends it.
///
/// The termination rules are the tokenizer's, not the symmetric ones intuition
/// suggests; each of these divergences was a way to make the depth scan skip
/// the rest of the document (html5ever-0.38.0/src/tokenizer/mod.rs):
///
/// * `<!-->` and `<!--->` are complete comments containing no `-->` at all:
///   `states::CommentStart` and `states::CommentStartDash` both emit the comment
///   on `>`.
/// * `--!>` ends a comment as well as `-->` does: `states::CommentEnd` routes
///   `!` to `states::CommentEndBang`, which emits on `>`.
/// * `<?…>` is a bogus comment ending at the first `>`, never at `?>` — HTML has
///   no processing instructions, so `states::TagOpen`'s `'?'` arm reconsumes in
///   `states::BogusComment`, which emits on `>`.
/// * `<!` anything else is a doctype or (including `<![CDATA[` in HTML content)
///   a bogus comment, and both also end at the first `>` — even one inside a
///   doctype's quoted identifier, which `states::DoctypeIdentifierDoubleQuoted`
///   treats as an abrupt end rather than as quoted content.
fn skip_comment_or_declaration(bytes: &[u8], at: usize) -> Option<usize> {
    if bytes[at..].starts_with(b"<!--") {
        return Some(comment_end(bytes, at + 4));
    }
    if bytes[at..].starts_with(b"<!") || bytes[at..].starts_with(b"<?") {
        return Some(find_byte(bytes, at, b'>').map_or(bytes.len(), |p| p + 1));
    }
    None
}

/// Index just past the end of the comment whose `<!--` ends at `body`.
fn comment_end(bytes: &[u8], body: usize) -> usize {
    // comment-start / comment-start-dash: a `>` closes the comment immediately.
    if bytes.get(body) == Some(&b'>') {
        return body + 1;
    }
    if bytes.get(body) == Some(&b'-') && bytes.get(body + 1) == Some(&b'>') {
        return body + 2;
    }
    // Otherwise the first `-->` or `--!>` ends it, whichever comes first. A
    // longer dash run ends the same way, since its last two dashes supply the
    // `--`. Unterminated: EOF in any comment state emits the comment, so the
    // rest of the input is comment body.
    let close = find_sub(&bytes[body..], b"-->").map(|p| body + p + 3);
    let close_bang = find_sub(&bytes[body..], b"--!>").map(|p| body + p + 4);
    match (close, close_bang) {
        (Some(a), Some(b)) => a.min(b),
        (Some(e), None) | (None, Some(e)) => e,
        (None, None) => bytes.len(),
    }
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
                return find_tag_end(bytes, name_end).map_or(bytes.len(), |(gt, _)| gt + 1);
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

    /// `unit` nested `depth` times, ending in a marker the assertions can find.
    fn deep(prefix: &str, unit: &str) -> String {
        format!("{prefix}{}<em>innermost</em>", unit.repeat(1000))
    }

    /// Pin that `html` took the tag-stripped fallback, not `htmd`.
    ///
    /// Every caller's fixture nests ~1000 deep, past the 500-600 where htmd's
    /// walker overflows a 2 MiB stack, so a scan that stopped seeing the
    /// nesting would abort this test process rather than fail an assertion.
    fn assert_degraded_to_text(html: &str, case: &str) {
        let md = html_to_markdown(html);
        assert!(md.contains("innermost"), "{case}: text lost: {md}");
        assert!(
            !md.contains("*innermost*") && !md.contains("_innermost_"),
            "{case}: output is markdown-converted, not tag-stripped: {md}"
        );
    }

    #[test]
    fn mismatched_close_tags_do_not_reduce_the_scanned_depth() {
        // html5ever discards an end tag matching nothing on its open-element
        // stack, so these divs really do nest 1000 deep.
        assert_degraded_to_text(&deep("", "<div></b>"), "</b>");
        assert_degraded_to_text(&deep("", "<div></p>"), "</p>");
    }

    #[test]
    fn abrupt_closing_empty_comment_does_not_hide_the_nesting() {
        // `<!-->` / `<!--->` are complete comments; a scan demanding a literal
        // `-->` treats everything after them as comment body.
        assert_degraded_to_text(&deep("<!-->", "<div>"), "<!-->");
        assert_degraded_to_text(&deep("<!--->", "<div>"), "<!--->");
    }

    #[test]
    fn bang_comment_end_terminates_the_comment() {
        assert_degraded_to_text(&deep("<!--x--!>", "<div>"), "--!>");
    }

    #[test]
    fn question_mark_opens_a_bogus_comment_ending_at_the_first_gt() {
        // HTML has no processing instructions, so `?>` is not a terminator.
        assert_degraded_to_text(&deep("<?>", "<div>"), "<?>");
        assert_degraded_to_text(&deep("<?php ", "<div>"), "<?php");
    }

    #[test]
    fn a_quote_outside_an_attribute_value_does_not_swallow_the_document() {
        // In `<b '>` the quote is an attribute-*name* character and the `>`
        // ends the tag; treating it as opening a value hid every div after it.
        assert_degraded_to_text(&deep("<b '>", "<div>"), "<b '>");
        assert_degraded_to_text(&deep("<b ='>", "<div>"), "<b ='>");
    }

    #[test]
    fn void_elements_nest_inside_foreign_content() {
        // `<wbr>` is void in HTML content but an ordinary nesting SVG element
        // under `<svg>`, which is not on the tree builder's breakout list.
        assert_degraded_to_text(&deep("<svg>", "<wbr>"), "svg/wbr");
        assert_degraded_to_text(&deep("<math>", "<input>"), "math/input");
    }

    #[test]
    fn a_self_closing_tag_under_foreign_content_still_counts_as_nesting() {
        // html5ever leaves foreign content without any close tag for the scan
        // to see, so "am I in foreign content" cannot be tracked lexically. A
        // self-closing tag is therefore only self-closing when it is `<svg>`
        // or `<math>` itself.
        //
        // `<i/>` breaks out of foreign content (`rules.rs:1618-1624`) and then
        // opens an `i` in HTML content, where the self-closing flag is
        // discarded; `<div/>` inside a `foreignObject` integration point is
        // parsed as HTML for the same reason. Both nest ~1000 deep, so a scan
        // that honored the flag here would hand these to htmd's walker and
        // abort this test process instead of failing an assertion.
        assert_degraded_to_text(&deep("<svg>", "<i/>"), "svg/self-closing i");
        assert_degraded_to_text(
            &deep("<svg><foreignObject>", "<div/>"),
            "svg/foreignObject/self-closing div",
        );
    }

    #[test]
    fn tag_names_are_compared_whole_not_truncated() {
        // `div_` is a distinct name (`_` is a tag-name character), so `</div>`
        // closes nothing here.
        assert_degraded_to_text(&deep("", "<div_></div>"), "div_");
    }

    #[test]
    fn balanced_content_far_past_the_ceiling_still_converts() {
        // The other side of the contract: matching close tags must still pop,
        // or ordinary long feed content would degrade to text.
        let md = html_to_markdown(&format!("{}<em>innermost</em>", "<p>x</p>".repeat(1000)));
        assert!(
            md.contains("*innermost*") || md.contains("_innermost_"),
            "1000 balanced paragraphs are depth 1, not 1000: {md}"
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
