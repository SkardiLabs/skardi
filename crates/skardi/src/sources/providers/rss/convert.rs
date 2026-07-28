//! HTML → Markdown conversion for item `content`/`summary`.
//!
//! Deterministic by contract: identical input yields byte-identical output, and
//! conversion never fails — pathological input degrades to its text content, or
//! to the empty string at worst. No raw HTML survives into the output: `script`
//! and `style` are dropped wholesale, and markup without a Markdown equivalent
//! is reduced to the text it contains.

use htmd::HtmlToMarkdownBuilder;
use htmd::options::{BrStyle, Options};

/// Convert an HTML fragment to Markdown.
pub fn html_to_markdown(html: &str) -> String {
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
        // No Markdown equivalent, and their text content is not document content.
        // Everything else without a handler still has its children walked, so
        // unknown markup reduces to the text it wraps rather than vanishing.
        // Comments are already dropped by htmd's default `Pure` translation mode.
        .skip_tags(vec!["script", "style", "head"])
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
}
