//! Tags and raw links from a note body.
//!
//! `pulldown-cmark` supplies two things: the byte ranges of code (fenced,
//! indented, inline — where Obsidian recognizes neither tags nor wikilinks)
//! and the `Link`/`Image` events for Markdown links, images and autolinks.
//! Wikilinks (`[[…]]`) and tags (`#…`) are Obsidian extensions, found by regex
//! over a copy of the body with every code byte blanked to a space so byte
//! offsets — and therefore line numbers — stay exact.

use std::ops::Range;
use std::sync::LazyLock;

use percent_encoding::percent_decode_str;
use pulldown_cmark::{Event, LinkType, Options, Parser, Tag, TagEnd};
use regex::Regex;

/// The syntax a link was written in. Resolution depends on it: wikilink paths
/// are vault-root relative, Markdown paths are note relative.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkSyntax {
    Wikilink,
    Markdown,
    Autolink,
}

/// A link as written, before resolution. `line` is filled by [`extract`] for
/// body links and left `None` by frontmatter scanning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawLink {
    pub syntax: LinkSyntax,
    /// `![[…]]` or `![…](…)`.
    pub embed: bool,
    /// Target as written, trimmed; percent-decoded for Markdown links; the
    /// full destination for URL-scheme targets; empty for `[[#Heading]]`.
    pub target: String,
    pub heading: Option<String>,
    pub block_id: Option<String>,
    pub display_text: Option<String>,
    pub line: Option<u32>,
}

/// Everything [`extract`] finds in one body.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Extracted {
    /// `(tag without '#', 1-based source line)`, in occurrence order, not
    /// deduplicated.
    pub tags: Vec<(String, u32)>,
    /// All links in byte-offset order, `line` set.
    pub links: Vec<RawLink>,
}

// Literal regexes compiled once; `expect` here is the one accepted use in
// library code (a malformed literal is a programming error caught by tests).
static WIKILINK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(!?)\[\[([^\[\]\n]+?)\]\]").expect("valid wikilink regex"));
static TAG: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?m)(?:^|\s)#([\p{L}\p{N}_/-]+)").expect("valid tag regex"));
static SCHEME: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z][A-Za-z0-9+.-]*:\S").expect("valid scheme regex"));

/// `true` for `https://…`, `mailto:…`, `obsidian://…` — a letter-initial
/// scheme, a colon, and at least one non-space character after it (so
/// `[[Note: subtitle]]` is not external).
pub fn has_url_scheme(target: &str) -> bool {
    SCHEME.is_match(target)
}

/// Byte offset → 1-based line number.
pub struct LineIndex {
    starts: Vec<usize>,
}

impl LineIndex {
    pub fn new(text: &str) -> Self {
        let mut starts = vec![0];
        starts.extend(text.match_indices('\n').map(|(i, _)| i + 1));
        Self { starts }
    }

    /// 1-based line containing byte `offset` (offsets past the end map to the
    /// last line).
    pub fn line_of(&self, offset: usize) -> u32 {
        self.starts.partition_point(|&s| s <= offset) as u32
    }
}

/// Find every `[[…]]` / `![[…]]` in `text`, returning `(byte offset of the
/// match, link)` with `line = None`. Shared with frontmatter scanning.
pub fn find_wikilinks(text: &str) -> Vec<(usize, RawLink)> {
    let mut out = Vec::new();
    for caps in WIKILINK.captures_iter(text) {
        let Some(whole) = caps.get(0) else { continue };
        let embed = caps.get(1).is_some_and(|m| !m.as_str().is_empty());
        let inner = caps.get(2).map(|m| m.as_str()).unwrap_or("");
        out.push((whole.start(), parse_wikilink_inner(inner, embed)));
    }
    out
}

/// `Target#Heading|Display` → parts. `#^id` is a block id.
fn parse_wikilink_inner(inner: &str, embed: bool) -> RawLink {
    let (dest, display) = match inner.split_once('|') {
        Some((d, t)) => (d, Some(t.trim())),
        None => (inner, None),
    };
    let (target, fragment) = match dest.split_once('#') {
        Some((t, f)) => (t, Some(f.to_string())),
        None => (dest, None),
    };
    let (heading, block_id) = split_fragment(fragment);
    RawLink {
        syntax: LinkSyntax::Wikilink,
        embed,
        target: target.trim().to_string(),
        heading,
        block_id,
        display_text: display.filter(|d| !d.is_empty()).map(str::to_string),
        line: None,
    }
}

/// A decoded fragment: `^id` is a block id, anything else a heading; empty is
/// neither.
fn split_fragment(fragment: Option<String>) -> (Option<String>, Option<String>) {
    let Some(f) = fragment else {
        return (None, None);
    };
    let f = f.trim();
    if f.is_empty() {
        (None, None)
    } else if let Some(block) = f.strip_prefix('^') {
        (None, Some(block.to_string()))
    } else {
        (Some(f.to_string()), None)
    }
}

/// A `Link`/`Image` whose `End` has not arrived yet.
struct OpenLink {
    start: usize,
    image: bool,
    link_type: LinkType,
    dest: String,
    text: String,
}

fn raw_link_from_markdown(open: OpenLink) -> RawLink {
    let is_auto = matches!(open.link_type, LinkType::Autolink | LinkType::Email);
    let syntax = if is_auto {
        LinkSyntax::Autolink
    } else {
        LinkSyntax::Markdown
    };
    let display_text = if is_auto || open.text.is_empty() {
        None
    } else {
        Some(open.text)
    };
    // pulldown-cmark hands an email autolink its bare address
    // (`<me@example.com>` → `me@example.com`); the `mailto:` scheme lives only
    // in its HTML rendering. Put it back, or the target carries no scheme and
    // resolves as a note name instead of an external link.
    let dest = if matches!(open.link_type, LinkType::Email) && !has_url_scheme(&open.dest) {
        format!("mailto:{}", open.dest)
    } else {
        open.dest
    };
    if has_url_scheme(&dest) {
        return RawLink {
            syntax,
            embed: open.image,
            target: dest,
            heading: None,
            block_id: None,
            display_text,
            line: None,
        };
    }
    // Split at the first LITERAL `#` first, then decode the two halves
    // independently, so `foo%23bar.md` stays the file name `foo#bar.md`.
    let (path, fragment) = match dest.split_once('#') {
        Some((p, f)) => (p, Some(f)),
        None => (dest.as_str(), None),
    };
    let target = percent_decode_str(path).decode_utf8_lossy().into_owned();
    let fragment = fragment.map(|f| percent_decode_str(f).decode_utf8_lossy().into_owned());
    let (heading, block_id) = split_fragment(fragment);
    RawLink {
        syntax,
        embed: open.image,
        target,
        heading,
        block_id,
        display_text,
        line: None,
    }
}

/// Copy of `body` with every byte inside `ranges` replaced by a space.
/// Multi-byte characters become one space per byte, so offsets are preserved
/// and the result is valid UTF-8. Ranges may overlap or arrive unsorted: they
/// are sorted once and swept with a single cursor, so a long note with many
/// code spans costs O(n log n) rather than O(n × spans).
fn mask_ranges(body: &str, ranges: &[Range<usize>]) -> String {
    let mut sorted: Vec<Range<usize>> = ranges.to_vec();
    sorted.sort_by_key(|r| (r.start, r.end));
    let mut pending = sorted.into_iter().peekable();
    // Bytes below this offset are inside some range already passed.
    let mut masked_until = 0usize;
    let mut out = String::with_capacity(body.len());
    for (idx, ch) in body.char_indices() {
        while let Some(range) = pending.next_if(|r| r.start <= idx) {
            masked_until = masked_until.max(range.end);
        }
        if idx < masked_until {
            for _ in 0..ch.len_utf8() {
                out.push(' ');
            }
        } else {
            out.push(ch);
        }
    }
    out
}

/// Extract tags and links from a frontmatter-stripped body. `body_first_line`
/// is the 1-based source line on which `body` starts (1 when the note has no
/// frontmatter), so every reported line names a line of the file.
pub fn extract(body: &str, body_first_line: u32) -> Extracted {
    let lines = LineIndex::new(body);
    let to_file_line = |offset: usize| lines.line_of(offset) + body_first_line - 1;

    let mut options = Options::empty();
    options
        .insert(Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS);

    let mut code_ranges: Vec<Range<usize>> = Vec::new();
    let mut found: Vec<(usize, RawLink)> = Vec::new();
    let mut open: Vec<OpenLink> = Vec::new();
    for (event, range) in Parser::new_ext(body, options).into_offset_iter() {
        match event {
            // A container's Start range covers the whole element (its End
            // repeats the same span), so one push per block is enough.
            Event::Start(Tag::CodeBlock(_)) => {
                code_ranges.push(range);
            }
            Event::Code(text) => {
                code_ranges.push(range);
                if let Some(top) = open.last_mut() {
                    top.text.push_str(&text);
                }
            }
            Event::Text(text) => {
                if let Some(top) = open.last_mut() {
                    top.text.push_str(&text);
                }
            }
            Event::Start(Tag::Link {
                link_type,
                dest_url,
                ..
            }) => open.push(OpenLink {
                start: range.start,
                image: false,
                link_type,
                dest: dest_url.to_string(),
                text: String::new(),
            }),
            Event::Start(Tag::Image {
                link_type,
                dest_url,
                ..
            }) => open.push(OpenLink {
                start: range.start,
                image: true,
                link_type,
                dest: dest_url.to_string(),
                text: String::new(),
            }),
            Event::End(TagEnd::Link) | Event::End(TagEnd::Image) => {
                if let Some(link) = open.pop() {
                    found.push((link.start, raw_link_from_markdown(link)));
                }
            }
            _ => {}
        }
    }

    let masked = mask_ranges(body, &code_ranges);
    found.extend(find_wikilinks(&masked));
    // Stable: equal offsets keep insertion order.
    found.sort_by_key(|(offset, _)| *offset);
    let links = found
        .into_iter()
        .map(|(offset, mut link)| {
            link.line = Some(to_file_line(offset));
            link
        })
        .collect();

    let mut tags = Vec::new();
    for caps in TAG.captures_iter(&masked) {
        let Some(m) = caps.get(1) else { continue };
        let tag = m.as_str();
        // Obsidian: a tag must contain at least one non-digit character.
        if tag.chars().all(char::is_numeric) {
            continue;
        }
        tags.push((tag.to_string(), to_file_line(m.start())));
    }

    Extracted { tags, links }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn links(body: &str) -> Vec<RawLink> {
        extract(body, 1).links
    }
    fn tags(body: &str) -> Vec<(String, u32)> {
        extract(body, 1).tags
    }
    fn wl(target: &str) -> RawLink {
        RawLink {
            syntax: LinkSyntax::Wikilink,
            embed: false,
            target: target.to_string(),
            heading: None,
            block_id: None,
            display_text: None,
            line: Some(1),
        }
    }

    #[test]
    fn wikilink_variants() {
        assert_eq!(links("[[Note]]"), vec![wl("Note")]);
        assert_eq!(
            links("[[Note|Shown]]"),
            vec![RawLink {
                display_text: Some("Shown".into()),
                ..wl("Note")
            }]
        );
        assert_eq!(
            links("[[Note#Some Heading|Shown]]"),
            vec![RawLink {
                heading: Some("Some Heading".into()),
                display_text: Some("Shown".into()),
                ..wl("Note")
            }]
        );
        assert_eq!(
            links("[[Meeting#^abc123]]"),
            vec![RawLink {
                block_id: Some("abc123".into()),
                ..wl("Meeting")
            }]
        );
        assert_eq!(
            links("![[attachments/diagram.png]]"),
            vec![RawLink {
                embed: true,
                ..wl("attachments/diagram.png")
            }]
        );
        assert_eq!(
            links("[[ Folder/Some Note ]]"),
            vec![wl("Folder/Some Note")]
        );
        assert_eq!(links("[[笔记]]"), vec![wl("笔记")]);
        assert_eq!(links("[[A]][[B]]"), vec![wl("A"), wl("B")]);
        assert_eq!(
            links("[[#Goals]]"),
            vec![RawLink {
                heading: Some("Goals".into()),
                ..wl("")
            }]
        );
        // An empty display part is NULL, not "".
        assert_eq!(links("[[Note|]]"), vec![wl("Note")]);
        // Not wikilinks: single brackets, nested brackets, a newline inside.
        assert!(links("[Note]").is_empty());
        assert!(links("[[a\nb]]").is_empty());
    }

    #[test]
    fn markdown_links_and_images() {
        let md = |target: &str| RawLink {
            syntax: LinkSyntax::Markdown,
            embed: false,
            target: target.to_string(),
            heading: None,
            block_id: None,
            display_text: Some("t".into()),
            line: Some(1),
        };
        assert_eq!(
            links("[t](Projects/Design.md)"),
            vec![md("Projects/Design.md")]
        );
        assert_eq!(
            links("![t](attachments/diagram.png)"),
            vec![RawLink {
                embed: true,
                ..md("attachments/diagram.png")
            }]
        );
        assert_eq!(
            links("[t](Note.md#Some%20Heading)"),
            vec![RawLink {
                heading: Some("Some Heading".into()),
                ..md("Note.md")
            }]
        );
        assert_eq!(
            links("[t](Note.md#^blk)"),
            vec![RawLink {
                block_id: Some("blk".into()),
                ..md("Note.md")
            }]
        );
        // Split at the literal `#` first, decode second: `%23` stays in the name.
        assert_eq!(links("[t](foo%23bar.md)"), vec![md("foo#bar.md")]);
        assert_eq!(links("[t](Some%20Note.md)"), vec![md("Some Note.md")]);
        assert_eq!(links("[t](<Some Note.md>)"), vec![md("Some Note.md")]);
        // Same-note link: empty path, heading kept.
        assert_eq!(
            links("[t](#Goals)"),
            vec![RawLink {
                heading: Some("Goals".into()),
                ..md("")
            }]
        );
        // Inline code inside the link text is part of the display text.
        assert_eq!(
            links("[see `x`](Note.md)")[0].display_text.as_deref(),
            Some("see x")
        );
    }

    #[test]
    fn external_targets_keep_the_full_destination() {
        let ext = links("[site](https://example.com/a#frag)");
        assert_eq!(ext[0].target, "https://example.com/a#frag");
        assert_eq!(ext[0].heading, None);
        assert_eq!(ext[0].display_text.as_deref(), Some("site"));
        assert_eq!(ext[0].syntax, LinkSyntax::Markdown);

        let auto = links("<https://auto.example/x>");
        assert_eq!(auto[0].target, "https://auto.example/x");
        assert_eq!(auto[0].syntax, LinkSyntax::Autolink);
        assert_eq!(auto[0].display_text, None);

        let mail = links("<me@example.com>");
        assert_eq!(mail[0].target, "mailto:me@example.com");
        assert_eq!(mail[0].syntax, LinkSyntax::Autolink);

        let obs = links("[o](obsidian://open?vault=x)");
        assert_eq!(obs[0].target, "obsidian://open?vault=x");
    }

    #[test]
    fn url_scheme_detection() {
        assert!(has_url_scheme("https://x"));
        assert!(has_url_scheme("mailto:a@b.c"));
        assert!(has_url_scheme("obsidian://open"));
        assert!(has_url_scheme("s3://bucket/key"));
        assert!(!has_url_scheme("Note"));
        assert!(!has_url_scheme("Projects/Design.md"));
        assert!(!has_url_scheme("Note: subtitle")); // space after the colon
        assert!(!has_url_scheme("C:")); // nothing after the colon
        assert!(!has_url_scheme("2026:plan")); // scheme must start with a letter
        // Recorded decision: with no space after the colon the RFC 3986 scheme
        // grammar wins, so `[[Note:subtitle]]` is `external`. Write
        // `[[Note: subtitle]]` for a note whose title contains a colon.
        assert!(has_url_scheme("Note:subtitle"));
    }

    #[test]
    fn mask_ranges_merges_overlaps_and_keeps_offsets() {
        // Unsorted, overlapping, and duplicated ranges collapse into one mask.
        assert_eq!(mask_ranges("abcdef", &[3..5, 1..2, 2..4, 1..2]), "a    f");
        assert_eq!(mask_ranges("abcdef", &[]), "abcdef");
        assert_eq!(mask_ranges("abcdef", &[0..6]), "      ");
        // Multi-byte characters become one space per byte.
        let masked = mask_ranges("x标签y", &[1..7]);
        assert_eq!(masked, "x      y");
        assert_eq!(masked.len(), "x标签y".len());
    }

    #[test]
    fn tag_rules() {
        assert_eq!(tags("#alpha"), vec![("alpha".into(), 1)]);
        assert_eq!(tags("text #alpha"), vec![("alpha".into(), 1)]);
        assert_eq!(tags("text\t#alpha"), vec![("alpha".into(), 1)]);
        assert_eq!(tags("#project/skardi"), vec![("project/skardi".into(), 1)]);
        assert_eq!(tags("#_x-y"), vec![("_x-y".into(), 1)]);
        assert_eq!(tags("#标签"), vec![("标签".into(), 1)]);
        assert_eq!(tags("#y2026"), vec![("y2026".into(), 1)]);
        // Trailing punctuation is not part of the tag.
        assert_eq!(
            tags("see #alpha, then #beta."),
            vec![("alpha".into(), 1), ("beta".into(), 1)]
        );
        // Rejected forms.
        assert!(tags("C#").is_empty());
        assert!(tags("https://x/#anchor").is_empty());
        assert!(tags("# Heading").is_empty());
        assert!(tags("#2026").is_empty());
        assert!(tags("word#inner").is_empty());
        // Tags in the same note are NOT deduplicated here (scan.rs dedupes).
        assert_eq!(tags("#a #a"), vec![("a".into(), 1), ("a".into(), 1)]);
    }

    #[test]
    fn code_is_not_scanned() {
        let body = "\
before [[Before]] #before

```
#fenced [[Fenced]] [md](Fenced.md)
```

    #indented [[Indented]]

inline `#code [[Code]]` after [[After]] #after
";
        let got = extract(body, 1);
        let targets: Vec<&str> = got.links.iter().map(|l| l.target.as_str()).collect();
        assert_eq!(targets, vec!["Before", "After"]);
        assert_eq!(got.links[1].line, Some(9));
        let tag_names: Vec<&str> = got.tags.iter().map(|(t, _)| t.as_str()).collect();
        assert_eq!(tag_names, vec!["before", "after"]);
        assert_eq!(got.tags[1].1, 9);
    }

    #[test]
    fn line_numbers_are_source_lines() {
        let body = "a\n\n[[X]] #t\n[md](Y.md)";
        let got = extract(body, 1);
        assert_eq!(got.links[0].line, Some(3));
        assert_eq!(got.links[1].line, Some(4));
        assert_eq!(got.tags[0].1, 3);
        // With a 4-line frontmatter block the body starts on line 5.
        let shifted = extract(body, 5);
        assert_eq!(shifted.links[0].line, Some(7));
        assert_eq!(shifted.links[1].line, Some(8));
        assert_eq!(shifted.tags[0].1, 7);
    }

    #[test]
    fn links_are_ordered_by_offset_across_syntaxes() {
        let got = links("[md](A.md) then [[B]] then <https://c.example> then ![[d.png]]");
        let targets: Vec<&str> = got.iter().map(|l| l.target.as_str()).collect();
        assert_eq!(targets, vec!["A.md", "B", "https://c.example", "d.png"]);
    }

    #[test]
    fn line_index_is_one_based() {
        let idx = LineIndex::new("ab\ncd\n\nef");
        assert_eq!(idx.line_of(0), 1);
        assert_eq!(idx.line_of(2), 1); // the newline itself belongs to line 1
        assert_eq!(idx.line_of(3), 2);
        assert_eq!(idx.line_of(6), 3);
        assert_eq!(idx.line_of(7), 4);
        assert_eq!(idx.line_of(99), 4);
    }

    #[test]
    fn find_wikilinks_reports_offsets() {
        let found = find_wikilinks("xx [[A]] yy ![[B|b]]");
        assert_eq!(found.len(), 2);
        assert_eq!(found[0].0, 3);
        assert_eq!(found[0].1.target, "A");
        assert_eq!(found[1].0, 12);
        assert!(found[1].1.embed);
        assert_eq!(found[1].1.display_text.as_deref(), Some("b"));
        assert_eq!(found[1].1.line, None);
    }
}
