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
/// abort. Above this ceiling, conversion degrades to tag-stripped text
/// (`strip_tags_to_text`) instead of walking the tree at all; nothing in this
/// crate's fixtures comes close, so it only engages on pathological input.
///
/// The margin is narrower than the two numbers suggest, because the real tree
/// can be deeper than the open-tag count (implied start tags — see
/// [`max_open_tag_depth`]). Sweeping 2,057 repeated-unit shapes over a 42-tag
/// pool, the deepest real tree reachable while the scan still reported ≤ 100
/// was 203, always from `<table><td>`. Against the 500-600 overflow zone that
/// is roughly 2.5x, not a wide margin. 203 is an empirical bound over those
/// shapes, not a proof — and the whole argument presupposes the scan really is
/// an upper bound on open-tag depth, which it only became once the stale
/// `foreign` undercount was fixed (see the `foreign` counter in
/// [`max_open_tag_depth`]). The figure also predates the sibling-closing pops
/// ([`implied_end_pops`]): those lower the scan only on shapes html5ever
/// flattens the same way, so they should not widen the gap the sweep measured
/// — an argument, not a re-measurement.
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

/// Whether a start tag `incoming` implicitly closes a still-open `top`, per
/// html5ever's sibling-closing rules — the reason `<p>one<p>two<p>three`
/// builds three siblings at depth ~4, not a nest 3 deep.
///
/// Omitting `</p>`, `</li>`, `</td>`, `</tr>` is both legal HTML5 and the
/// single most common shape of legacy feed HTML, so a scan that never pops on
/// it scored `"<p>para".repeat(150)` as depth 150 against a real DOM depth of
/// ~4 and degraded the whole document to tag-stripped text — an over-count in
/// the technically-safe direction that turned a rare guard into routine
/// content damage.
///
/// The obligation is [`max_open_tag_depth`]'s: never let a pop *under*-count.
/// Each rule below fires only where the same token closes the same element in
/// html5ever — `p` is closed by every sibling-closer here on its way in
/// (`li`/`dd`/`dt` explicitly, the table tags via the cell they close), `li`
/// by `li`, the definition and cell pairs by either sibling, `tr` by the next
/// row, `option` by the next option. The scan consults only the top of its
/// stack, so an element html5ever would *not* close — an `li` under a nested
/// `<ul>`, a `p` sitting below any other element — is shielded by whatever
/// sits above it, and genuinely nested input keeps its full count. And when
/// the scan's stack disagrees with html5ever's, the disagreement is a stale
/// entry for a token html5ever ignored outright (`<td>` outside any table,
/// say) — this scan pushed that entry itself, so popping it retires the
/// scan's own surplus while the push that follows restores it: the estimate
/// stays an upper bound.
fn implied_end_pops(incoming: &str, top: &str) -> bool {
    let is = |t: &str, n: &str| t.eq_ignore_ascii_case(n);
    let cell = |t: &str| is(t, "td") || is(t, "th");
    let definition = |t: &str| is(t, "dd") || is(t, "dt");

    if is(top, "p") {
        return is(incoming, "p")
            || is(incoming, "li")
            || definition(incoming)
            || cell(incoming)
            || is(incoming, "tr");
    }
    if is(top, "li") {
        return is(incoming, "li");
    }
    if definition(top) {
        return definition(incoming);
    }
    if cell(top) {
        return cell(incoming) || is(incoming, "tr");
    }
    if is(top, "tr") {
        return is(incoming, "tr");
    }
    if is(top, "option") {
        return is(incoming, "option");
    }
    false
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
/// real tree shallower than this estimate. One family of implied end tags *is*
/// popped — the sibling-closers (`<p>one<p>two`, unclosed `<li>`/`<td>`/`<tr>`
/// runs), because there the over-count landed on the most common shape of
/// legacy feed HTML rather than on an attacker; see [`implied_end_pops`] for
/// the rules and why they cannot under-count. The one direction html5ever goes
/// *deeper* than the open-tag count is implied *start* tags — `<table><td>`
/// builds `table > tbody > tr > td`, four levels from two tags, roughly
/// doubling it — on top of the fixed `document > html > body` wrapper worth 3.
/// A sweep of 2,057 repeated-unit shapes put the deepest reachable real tree at
/// 203 while this scan still reported ≤ 100, `<table><td>` being the worst; see
/// [`MAX_HTML_DEPTH`] for what that leaves against the overflow zone.
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
            // Implied end tags: `<p>one<p>two` and unclosed `<li>`/`<td>`/`<tr>`
            // runs — the most common legacy feed HTML — build siblings, not
            // nests, so the sibling-closers pop what html5ever would close (see
            // [`implied_end_pops`]). Suspended in foreign content like the
            // [`VOID_ELEMENTS`] skip, and for the same reason: `foreign` can
            // only be trusted where believing it too readily over-counts.
            if foreign == 0 {
                while open.last().is_some_and(|top| implied_end_pops(tag, top)) {
                    open.pop();
                }
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

/// Whitespace that ends a tag name or an attribute name in the tokenizer.
///
/// The tag states spell the set as `'\t' | '\n' | '\x0C' | ' '`
/// (html5ever-0.38.0/src/tokenizer/mod.rs: `states::TagName` at `:923`,
/// `BeforeAttributeName` `:1143`, `AttributeName` `:1166`, `AfterAttributeName`
/// `:1189`). `\r` is included here anyway because no tag state ever sees one:
/// all four read through `get_char!` (`:698`) → `get_char` (`:294-303`) →
/// `get_preprocessed_char` (`:259-290`), whose `//§ preprocessing-the-input-stream`
/// body rewrites every `\r` to `\n` at `:267-270` and sets `ignore_lf` to swallow
/// a following `\n`. So a `\r` arrives as `\n` and ends a name exactly like one.
/// `states::AttributeValue(Unquoted)` (`:1258`) reaches the same result by
/// listing `'\r'` in its `small_char_set`, which routes it through
/// `get_preprocessed_char` (`:318`) onto the `FromSet('\n')` arm.
///
/// The one state that does see a raw `\r` is `states::BeforeAttributeValue`
/// (`:1215-1217`), which is why that set spells `'\r'` out: it reads through
/// `peek!`, and `peek` does not normalize — see the note on `discard_char`
/// (`:606-611`), "peek() deals in un-processed characters (no newline
/// normalization), while get_char() does". [`find_tag_end`] handles that state
/// separately.
///
/// Omitting `\r` was safe in direction (longer names pop less often, which
/// over-counts) but cost real content: `"<p\r\nclass=\"lede\">x</p>".repeat(101)`
/// scanned as depth 101 against a real DOM depth of 4 and degraded to
/// tag-stripped text, while the same document with LF endings scanned as 1 and
/// converted. CRLF inside start tags is ordinary in HTTP-delivered feeds.
fn is_tag_space(c: u8) -> bool {
    matches!(c, b'\t' | b'\n' | b'\r' | b'\x0C' | b' ')
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
                // `states::BeforeAttributeValue` (`:1215-1217`): skip
                // whitespace, then a `"`/`'` opens a value running to the
                // matching quote — a character reference inside cannot contain
                // one, so a plain search for it is exact. Anything else is an
                // unquoted value. This is the one tag state whose set spells
                // `'\r'` out, because it peeks rather than normalizing; the
                // byte is in [`is_tag_space`] either way.
                while bytes.get(j).is_some_and(|&c| is_tag_space(c)) {
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

    /// The ceiling's exact boundary, from both sides.
    ///
    /// Every other test on this path is an order of magnitude away from it: the
    /// degradation cases nest ~1000 and every positive conversion is depth 1, so
    /// `MAX_HTML_DEPTH` could be moved anywhere in roughly `[20, 400]` without
    /// failing anything (measured: 20 and 400 both left the suite green). Both
    /// directions are real. Tightened to 20, ordinary nested feed content —
    /// nested lists, blockquotes, tables — would silently lose its Markdown
    /// structure; loosened to 400, the ceiling walks toward the 500-600 zone
    /// where htmd's recursive walker overflows a 2 MiB stack and aborts the
    /// process, which is the whole reason it exists (see [`MAX_HTML_DEPTH`]).
    ///
    /// 100 and 101 are written as literals rather than derived from the constant,
    /// so this cannot agree with a changed one. The real DOM at the passing end is
    /// the open-tag count plus html5ever's fixed `document > html > body` wrapper
    /// — ~103, nowhere near the overflow zone — so the converting side is safe to
    /// actually run through htmd.
    #[test]
    fn the_depth_ceiling_converts_at_100_and_degrades_at_101() {
        // `depth` is the whole document's open-tag depth: the `<em>` marker is
        // itself an open tag, so it accounts for one of the levels and the divs
        // supply the rest.
        let nested = |depth: usize| {
            format!(
                "{}<em>innermost</em>{}",
                "<div>".repeat(depth - 1),
                "</div>".repeat(depth - 1)
            )
        };
        // Fixture guard, so a drifting fixture cannot quietly stop straddling the
        // boundary. `max_open_tag_depth` short-circuits at `MAX_HTML_DEPTH + 1`,
        // so a ceiling moved below 100 makes these read low and fails here first
        // rather than in the assertions below.
        assert_eq!(max_open_tag_depth(&nested(100)), 100);
        assert_eq!(max_open_tag_depth(&nested(101)), 101);

        let at = html_to_markdown(&nested(100));
        assert!(
            at.contains("*innermost*") || at.contains("_innermost_"),
            "depth 100 is the last depth htmd still converts, and this one degraded \
             to tag-stripped text: {at}"
        );

        let past = html_to_markdown(&nested(101));
        assert!(past.contains("innermost"), "text lost at depth 101: {past}");
        assert!(
            !past.contains("*innermost*") && !past.contains("_innermost_"),
            "depth 101 is past the ceiling and must degrade to tag-stripped text: {past}"
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

    /// `</p>` is optional HTML5 and legacy feeds omit it as a matter of course:
    /// a new `<p>` closes the previous one, so 150 unclosed paragraphs are 150
    /// siblings at real depth ~4. A scan that never popped on the implied end
    /// scored this 150, tripped the ceiling, and degraded ordinary content to
    /// tag-stripped text — the exact shape from review.
    #[test]
    fn unclosed_paragraph_runs_are_siblings_not_nesting() {
        let html = "<p>para".repeat(150);
        assert_eq!(max_open_tag_depth(&html), 1);

        let md = html_to_markdown(&html);
        assert!(
            md.contains("para\n\npara"),
            "unclosed paragraphs must convert as paragraphs, not degrade to a \
             single run of text: {md:?}"
        );
    }

    /// The rest of the sibling-closing set, in the shapes legacy HTML actually
    /// uses them: list items without `</li>`, definition pairs, table rows and
    /// cells without `</td>`/`</tr>`, options without `</option>`.
    #[test]
    fn unclosed_sibling_runs_do_not_trip_the_ceiling() {
        for (name, html) in [
            ("li", format!("<ul>{}</ul>", "<li>item".repeat(150))),
            ("dd/dt", format!("<dl>{}</dl>", "<dt>t<dd>d".repeat(150))),
            (
                "tr/td",
                format!("<table>{}</table>", "<tr><td>a<td>b".repeat(150)),
            ),
            (
                "option",
                format!("<select>{}</select>", "<option>o".repeat(150)),
            ),
            // li closing the p left open inside the previous li.
            ("li over p", format!("<ul>{}</ul>", "<li><p>x".repeat(150))),
        ] {
            let depth = max_open_tag_depth(&html);
            assert!(
                depth <= 10,
                "{name}: unclosed sibling runs are flat, scanned {depth}"
            );
        }
    }

    /// The pops must not fire through a real container: `<ul><li><ul><li>…`
    /// genuinely nests (html5ever's li rule stops at the `ul`, and so does the
    /// scan, which only consults the top of its stack), and deep genuine
    /// nesting is exactly what the ceiling exists to catch.
    #[test]
    fn genuinely_nested_lists_still_count_and_still_degrade() {
        let html = "<ul><li>".repeat(60);
        assert!(
            max_open_tag_depth(&html) > MAX_HTML_DEPTH,
            "60 nested ul/li pairs are real nesting, not siblings"
        );
    }

    #[test]
    fn crlf_inside_a_start_tag_does_not_inflate_the_scanned_depth() {
        // `\r` never reaches a tag state: `get_preprocessed_char` normalizes it
        // to `\n` first (see `is_tag_space`), so it ends a tag or attribute name
        // like any other space and these paragraphs are depth 1, not 101.
        // Treating it as a name character instead scored this document at 101
        // and silently dropped the Markdown structure of ordinary
        // HTTP-delivered feed content.
        let html = format!(
            "{}<em>innermost</em>",
            "<p\r\nclass=\"lede\">x</p>".repeat(101)
        );
        let md = html_to_markdown(&html);
        assert!(
            md.contains("*innermost*") || md.contains("_innermost_"),
            "CRLF in a start tag degraded a depth-1 document: {md}"
        );
        // The LF spelling is the control: both must take the same path.
        let lf = html_to_markdown(&format!(
            "{}<em>innermost</em>",
            "<p\nclass=\"lede\">x</p>".repeat(101)
        ));
        assert_eq!(md, lf, "CRLF and LF spellings converted differently");
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
