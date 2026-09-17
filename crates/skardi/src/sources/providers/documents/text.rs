//! Direct ingest for Markdown and plain text.
//!
//! # Why these formats never reach liteparse
//!
//! liteparse exists to turn a *layout* back into markdown: a PDF page, a
//! slide, a spreadsheet. A `.md` file already **is** that output, and a `.txt`
//! needs no reconstruction at all — so handing either to the parser is not a
//! shortcut, it is a round trip through a representation neither of them has.
//!
//! It is also not possible. `convert_to_pdf` routes only Office, presentation,
//! spreadsheet and image extensions to LibreOffice/ImageMagick; `txt` and `md`
//! appear in liteparse only in `TEXT_ONLY_EXTENSIONS`, a list used to *refuse*
//! screenshots. Adding `*.md` to `include_globs` and nothing else yields
//! `unsupported file format: .txt` for every file, and — because the wholesale
//! failure guard in `parse_source_blocking` treats a listing where everything
//! failed as fatal — a hard-errored scan.
//!
//! So this module reads the bytes, splits them into sections, and emits the
//! same [`ParsedPage`] rows the liteparse path emits. The two paths meet only
//! at that struct, which is what keeps the table schema, the ETL SQL and every
//! reader unaware that there are two paths at all.

use std::path::Path;

use anyhow::{Result, bail};

use super::parse::{ParsedPage, doc_id_for, file_type_for};

/// The largest section this will emit, in bytes.
///
/// A hard ceiling rather than a preference, and not a taste question: a
/// consumer indexing these rows into Postgres hits `to_tsvector`'s refusal to
/// accept a string over 1 MB, and a section an agent must read in one gulp is
/// not a section. 64 KiB is comfortably under the first and comfortably over a
/// normal prose heading's worth of text.
const MAX_SECTION_BYTES: usize = 64 * 1024;

/// Below this, a section is merged into the one after it.
///
/// Without it every document starts with a section holding nothing but its
/// title, and a reader paging through sections spends the first one learning
/// nothing. Merging forward rather than backward keeps a heading with the
/// prose it introduces.
const MIN_SECTION_BYTES: usize = 2 * 1024;

/// Whether this path takes the direct-ingest route instead of liteparse.
///
/// **Extension, not content sniffing.** The globs already select by basename,
/// and a `.txt` holding a PDF is something the uploader can see and fix; a
/// sniffer that second-guessed the name would make the route a file takes
/// depend on its first bytes, which is far harder to reason about when a scan
/// does something unexpected.
pub(super) fn is_text_path(rel_path: &str) -> bool {
    matches!(
        Path::new(rel_path)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_ascii_lowercase()
            .as_str(),
        "md" | "markdown" | "txt"
    )
}

/// Read one text file into `(file, section)` rows.
///
/// Errors are per-file: the caller logs and skips, and the wholesale-failure
/// guard turns "every file failed" into a hard error. That is the same
/// contract the liteparse path has.
pub(super) fn parse_text(bytes: &[u8], rel_path: &str) -> Result<Vec<ParsedPage>> {
    let text = decode(bytes, rel_path)?;
    let file_type = file_type_for(Path::new(rel_path));
    let doc_id = doc_id_for(rel_path);
    let markdown_rules = file_type == "md";

    let sections = split_into_sections(&text, markdown_rules);
    if sections.is_empty() {
        // Not an error: an empty (or whitespace-only) file is a file with
        // nothing in it, not a file that failed. It contributes no rows and
        // still counts as parsed, so it cannot by itself trip the
        // wholesale-failure guard.
        tracing::warn!("documents: {rel_path} has no content; no sections emitted");
        return Ok(Vec::new());
    }

    Ok(sections
        .into_iter()
        .enumerate()
        .map(|(index, markdown)| ParsedPage {
            doc_id: doc_id.clone(),
            path: rel_path.to_string(),
            // 1-based, exactly like a page number. For a text file a "page" is
            // a section; `docs/documents.md` says so where a reader will look.
            page: index as i32 + 1,
            markdown,
            // Neither concept exists for text. Empty rather than NULL so the
            // column's shape does not depend on which path produced the row.
            tables_json: "[]".to_string(),
            page_image_ref: None,
            image_refs: Vec::new(),
            file_type: file_type.clone(),
        })
        .collect())
}

/// Decode to UTF-8, strip a leading BOM, and normalise line endings.
///
/// Three refusals, and each is a failure that would otherwise surface far from
/// the file that caused it:
///
/// - **Invalid UTF-8** is refused rather than lossily decoded. A mojibake
///   section is worse than no section: an agent quotes it back verbatim as
///   though it were the document, and nothing downstream can tell that it is
///   not.
/// - **A NUL byte** is refused although it *is* valid UTF-8, because a
///   Postgres `text` column cannot hold one. A section carrying `\0` fails the
///   consumer's INSERT with `invalid byte sequence for encoding "UTF8": 0x00`
///   — a message that names neither this file nor this format.
/// - A **BOM** is stripped rather than kept, or it becomes a zero-width space
///   at the head of the first section and, for Markdown, stops a leading `#`
///   from being parsed as a heading by whoever renders it.
fn decode(bytes: &[u8], rel_path: &str) -> Result<String> {
    let bytes = bytes.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(bytes);
    let text = match std::str::from_utf8(bytes) {
        Ok(text) => text,
        Err(e) => bail!(
            "documents: {rel_path} is not UTF-8 (invalid byte at offset {})",
            e.valid_up_to()
        ),
    };
    if let Some(at) = text.find('\0') {
        bail!(
            "documents: {rel_path} contains a NUL byte at offset {at}, which no text column can hold"
        );
    }
    // `\r\n` first so a CRLF file does not become a double newline, then a
    // lone `\r` (classic Mac, and some exports) so blank-line detection below
    // sees the breaks that are actually there.
    Ok(text.replace("\r\n", "\n").replace('\r', "\n"))
}

/// Split into sections: primary boundaries, then the small-merge, then the
/// ceiling.
///
/// The order matters. Merging before enforcing the ceiling means a merge can
/// produce an over-long section, which the ceiling pass then splits — whereas
/// the reverse order would let the merge undo the ceiling it had just applied.
fn split_into_sections(text: &str, markdown_rules: bool) -> Vec<String> {
    let primary = if markdown_rules {
        split_at_headings(text)
    } else {
        split_at_blank_lines(text)
    };

    let mut merged: Vec<String> = Vec::new();
    for segment in primary {
        match merged.last_mut() {
            // The PREVIOUS section is the short one, so this segment joins it.
            // Checked on the accumulated length so three 1 KiB segments become
            // one 3 KiB section rather than continuing to absorb forever.
            Some(last) if last.len() < MIN_SECTION_BYTES => last.push_str(segment),
            _ => merged.push(segment.to_string()),
        }
    }

    let mut out = Vec::new();
    for section in merged {
        let mut rest = section.as_str();
        while !rest.is_empty() {
            let at = ceiling_split(rest);
            let (head, tail) = rest.split_at(at);
            if !head.trim().is_empty() {
                out.push(head.to_string());
            }
            rest = tail;
        }
    }
    out
}

/// Byte offsets of the ATX headings of level 1–2 that start a section.
///
/// Fenced code blocks are tracked, because a `# ` inside one is a shell
/// comment or a Python comment far more often than it is a heading, and
/// splitting there cuts a code example in half. Both fence markers count, and
/// a closing fence must be at least as long as the one that opened it — the
/// CommonMark rule, and the reason a ```` ```` ```` inside a ```` ~~~ ````
/// block does not close it.
///
/// A heading must start at column 0. CommonMark allows up to three leading
/// spaces; this does not, because the cost of missing one is a slightly larger
/// section and the cost of a looser rule is splitting inside an indented
/// block.
fn split_at_headings(text: &str) -> Vec<&str> {
    let mut starts = vec![0usize];
    let mut fence: Option<(char, usize)> = None;
    let mut offset = 0usize;
    for line in text.split_inclusive('\n') {
        let trimmed = line.trim_end_matches('\n');
        match fence {
            Some((marker, len)) => {
                let closing = trimmed.trim_end();
                if closing.starts_with(marker)
                    && closing.chars().all(|c| c == marker)
                    && closing.len() >= len
                {
                    fence = None;
                }
            }
            None => {
                if let Some(opened) = fence_open(trimmed) {
                    fence = Some(opened);
                } else if is_section_heading(trimmed) && offset > 0 {
                    starts.push(offset);
                }
            }
        }
        offset += line.len();
    }
    slice_at(text, &starts)
}

/// The fence character and its run length, if this line opens a fence.
fn fence_open(line: &str) -> Option<(char, usize)> {
    let marker = line.chars().next()?;
    if marker != '`' && marker != '~' {
        return None;
    }
    let run = line.chars().take_while(|c| *c == marker).count();
    (run >= 3).then_some((marker, run))
}

/// `# Heading` or `## Heading` at column 0. The space is required: `#tag` is
/// not a heading, and in a plain-text-ish Markdown file it is common.
fn is_section_heading(line: &str) -> bool {
    let rest = line.strip_prefix("##").or_else(|| line.strip_prefix('#'));
    match rest {
        Some(rest) => rest.starts_with(' ') || rest.starts_with('\t'),
        None => false,
    }
}

/// Byte offsets after each run of blank lines.
///
/// Paragraph boundaries are the only structure plain text reliably has. The
/// small-merge downstream is what stops this from emitting a section per
/// paragraph.
fn split_at_blank_lines(text: &str) -> Vec<&str> {
    let mut starts = vec![0usize];
    let mut offset = 0usize;
    let mut previous_blank = false;
    for line in text.split_inclusive('\n') {
        let blank = line.trim().is_empty();
        if !blank && previous_blank && offset > 0 {
            starts.push(offset);
        }
        previous_blank = blank;
        offset += line.len();
    }
    slice_at(text, &starts)
}

fn slice_at<'a>(text: &'a str, starts: &[usize]) -> Vec<&'a str> {
    starts
        .iter()
        .enumerate()
        .map(|(i, start)| {
            let end = starts.get(i + 1).copied().unwrap_or(text.len());
            &text[*start..end]
        })
        .filter(|segment| !segment.is_empty())
        .collect()
}

/// Where to cut a section that is over the ceiling.
///
/// **This is the part that has to terminate on any input.** A minified `.md`,
/// a single-line JSON-ish log export, or a 2 MB paragraph with no blank line
/// in it offers none of the boundaries above — and "split at the nearest blank
/// line" has nothing to return. The search is therefore ordered and ends in a
/// cut that always exists:
///
/// 1. a heading (only reachable when the small-merge joined across one),
/// 2. a blank line,
/// 3. a single newline,
/// 4. the last UTF-8 character boundary at or before the ceiling.
///
/// Each candidate is the LAST one at or before the ceiling, so a section is as
/// large as its best boundary allows rather than as small as its first.
fn ceiling_split(section: &str) -> usize {
    if section.len() <= MAX_SECTION_BYTES {
        return section.len();
    }
    // The widest prefix that could contain a boundary. Clamped to a character
    // boundary so every `&section[..window]` below is a legal slice.
    let mut window = MAX_SECTION_BYTES;
    while !section.is_char_boundary(window) {
        window -= 1;
    }
    let head = &section[..window];

    if let Some(at) = last_heading_start(head) {
        return at;
    }
    if let Some(at) = head.rfind("\n\n") {
        return at + 2;
    }
    if let Some(at) = head.rfind('\n') {
        return at + 1;
    }
    // Nothing to split on. The hard cut is not a nice boundary and is not
    // meant to be — it is the guarantee that this function returns a position
    // strictly inside an over-long section, so the loop that calls it always
    // makes progress.
    window
}

fn last_heading_start(head: &str) -> Option<usize> {
    let mut found = None;
    let mut offset = 0usize;
    for line in head.split_inclusive('\n') {
        if offset > 0 && is_section_heading(line.trim_end_matches('\n')) {
            found = Some(offset);
        }
        offset += line.len();
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sections(text: &str, markdown_rules: bool) -> Vec<String> {
        split_into_sections(text, markdown_rules)
    }

    /// Whatever the rules, the sections must reassemble into the input. A
    /// chunker that drops or duplicates a byte loses document content in a way
    /// no consumer can detect.
    fn assert_lossless(text: &str, markdown_rules: bool) {
        let joined: String = sections(text, markdown_rules).concat();
        // Trailing whitespace-only segments are dropped deliberately, so
        // compare on the content.
        assert_eq!(joined.trim_end(), text.trim_end(), "sections lost content");
    }

    /// Bodies over the merge floor, because that is the only way to see the
    /// heading split at all — see
    /// `a_document_under_the_merge_floor_is_one_section`.
    fn body(tag: &str) -> String {
        format!("{tag} {}\n", "word ".repeat(MIN_SECTION_BYTES / 4))
    }

    #[test]
    fn a_markdown_file_splits_at_top_level_headings() {
        let text = format!(
            "# Title\n\n{}\n## First\n\n{}\n## Second\n\n{}",
            body("intro"),
            body("first"),
            body("second")
        );
        let out = sections(&text, true);
        assert_eq!(out.len(), 3, "{out:#?}");
        assert!(out[0].starts_with("# Title"));
        assert!(out[1].starts_with("## First"));
        assert!(out[2].starts_with("## Second"));
        assert_lossless(&text, true);
    }

    /// A short document is ONE section, headings or not.
    ///
    /// This falls out of the merge floor and is deliberate: sections exist so
    /// an agent can read part of a document, and a document it can read whole
    /// has no part worth addressing separately. The floor is also what keeps a
    /// title-only preamble from being its own section, which is the same rule
    /// seen from the other end.
    #[test]
    fn a_document_under_the_merge_floor_is_one_section() {
        let text = "# Title\n\nintro\n\n## First\n\nbody\n\n## Second\n\nbody\n";
        assert!(text.len() < MIN_SECTION_BYTES, "the premise");
        let out = sections(text, true);
        assert_eq!(out.len(), 1, "{out:#?}");
        assert_lossless(text, true);
    }

    /// Deeper headings are structure WITHIN a section, not between sections.
    /// Splitting at every `###` would produce a section per paragraph in a
    /// typical reference document.
    #[test]
    fn deeper_headings_do_not_start_a_section() {
        let text = format!(
            "# Title\n\n{}\n### Deep\n\n{}\n#### Deeper\n\n{}",
            body("a"),
            body("b"),
            body("c")
        );
        assert_eq!(sections(&text, true).len(), 1, "only levels 1-2 split");
    }

    /// The bug this guards against: a shell comment inside a fence read as a
    /// heading, cutting the example in half.
    #[test]
    fn a_hash_inside_a_fenced_block_is_not_a_heading() {
        let text = format!(
            "# Title\n\n```bash\n# not a heading\necho hi\n## also not\n```\n\n{}\n## Real\n\n{}",
            body("prose"),
            body("real")
        );
        let out = sections(&text, true);
        assert_eq!(out.len(), 2, "{out:#?}");
        assert!(out[0].contains("# not a heading"));
        assert!(out[0].contains("## also not"));
        assert!(out[1].starts_with("## Real"));
        assert_lossless(&text, true);
    }

    /// Tilde fences, and the CommonMark length rule: a shorter run inside a
    /// longer fence does not close it.
    #[test]
    fn tilde_fences_and_longer_fences_close_correctly() {
        let text = format!(
            "# T\n\n~~~\n# inside tilde\n~~~\n\n{}\n## After\n\n{}",
            body("prose"),
            body("after")
        );
        let out = sections(&text, true);
        assert_eq!(out.len(), 2, "{out:#?}");
        assert!(out[0].contains("# inside tilde"));

        // A shorter run inside a longer fence does not close it (CommonMark).
        let nested = format!(
            "# T\n\n````\n```\n# still inside\n```\n````\n\n{}\n## After\n\n{}",
            body("prose"),
            body("after")
        );
        let out = sections(&nested, true);
        assert_eq!(out.len(), 2, "{out:#?}");
        assert!(out[0].contains("# still inside"));
    }

    /// `#tag` and `#!shebang` are not headings; the space is required.
    #[test]
    fn a_hash_with_no_space_is_not_a_heading() {
        let text = format!(
            "# Title\n\n{}\n#tag line\n\n{}\n#!/bin/sh\n\n{}",
            body("a"),
            body("b"),
            body("c")
        );
        assert_eq!(sections(&text, true).len(), 1, "the space is required");
    }

    #[test]
    fn plain_text_splits_at_blank_lines_and_merges_the_small_ones() {
        // Each paragraph is far under the merge floor, so they coalesce.
        let text = "one\n\ntwo\n\nthree\n";
        let out = sections(text, false);
        assert_eq!(out.len(), 1, "{out:#?}");
        assert!(out[0].contains("one") && out[0].contains("three"));
        assert_lossless(text, false);
    }

    #[test]
    fn plain_text_paragraphs_over_the_floor_stay_separate() {
        let big = "x".repeat(MIN_SECTION_BYTES + 10);
        let text = format!("{big}\n\n{big}\n\n{big}\n");
        let out = sections(&text, false);
        assert_eq!(out.len(), 3, "each paragraph is over the merge floor");
        assert_lossless(&text, false);
    }

    /// A title-only first section is the case the merge floor exists for.
    #[test]
    fn a_title_only_preamble_is_not_its_own_section() {
        let body = "y".repeat(MIN_SECTION_BYTES + 10);
        let text = format!("# Title\n\n## First\n\n{body}\n");
        let out = sections(&text, true);
        assert_eq!(out.len(), 1, "{out:#?}");
        assert!(out[0].starts_with("# Title"));
        assert!(out[0].contains("## First"));
    }

    /// The ceiling holds even when there is no boundary to respect — the case
    /// that makes the fallback chain mandatory rather than nice.
    #[test]
    fn one_enormous_line_is_still_cut_into_bounded_sections() {
        let text = "z".repeat(MAX_SECTION_BYTES * 3 + 17);
        let out = sections(&text, false);
        assert!(
            out.len() >= 4,
            "expected several sections, got {}",
            out.len()
        );
        assert!(
            out.iter().all(|s| s.len() <= MAX_SECTION_BYTES),
            "a section exceeded the ceiling"
        );
        assert_eq!(out.concat(), text, "the hard cut lost content");
    }

    /// The hard cut must land on a character boundary. A 3-byte code point
    /// repeated across the ceiling puts one astride it for two offsets in
    /// three, so this fails immediately if the cut is taken on a byte index.
    #[test]
    fn the_hard_cut_never_splits_a_code_point() {
        for pad in 0..4 {
            let text = format!("{}{}", "a".repeat(pad), "€".repeat(MAX_SECTION_BYTES));
            let out = sections(&text, false);
            assert!(out.iter().all(|s| s.len() <= MAX_SECTION_BYTES));
            assert_eq!(out.concat(), text, "pad {pad} lost content");
            // `concat` proves the pieces are valid `String`s, which a split
            // inside a code point could not have produced.
        }
    }

    /// An over-long section prefers a blank line to a bare newline, and either
    /// to a hard cut.
    #[test]
    fn the_ceiling_split_prefers_the_best_boundary_it_can_reach() {
        let filler = "w".repeat(MAX_SECTION_BYTES - 100);
        let text = format!(
            "{filler}\n\ntail paragraph\n{}",
            "v".repeat(MAX_SECTION_BYTES)
        );
        let out = sections(&text, false);
        assert!(
            out[0].ends_with("\n\n"),
            "expected a blank-line boundary, got {:?}",
            &out[0][out[0].len() - 20..]
        );
    }

    #[test]
    fn a_bom_is_stripped_so_the_first_heading_still_parses() {
        let rows = parse_text("\u{feff}# Title\n\nbody\n".as_bytes(), "a.md").unwrap();
        assert_eq!(rows.len(), 1);
        assert!(
            rows[0].markdown.starts_with("# Title"),
            "{:?}",
            rows[0].markdown
        );
    }

    #[test]
    fn crlf_and_lone_cr_become_newlines() {
        let rows = parse_text(b"# A\r\n\r\npara\r\n", "a.md").unwrap();
        assert!(!rows[0].markdown.contains('\r'));
        let rows = parse_text(b"# A\r\rpara\r", "a.md").unwrap();
        assert!(!rows[0].markdown.contains('\r'));
    }

    #[test]
    fn latin_1_is_refused_rather_than_lossily_decoded() {
        // `café` in Latin-1: a bare 0xE9 that starts a sequence never finished.
        let err = parse_text(b"caf\xe9 notes", "notes.txt").unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("is not UTF-8"), "{msg}");
        assert!(
            msg.contains("notes.txt"),
            "the message must name the file: {msg}"
        );
    }

    #[test]
    fn a_nul_byte_is_refused_although_it_is_valid_utf8() {
        assert!(std::str::from_utf8(b"a\0b").is_ok(), "the premise");
        let err = parse_text(b"a\0b", "notes.txt").unwrap_err();
        assert!(format!("{err:#}").contains("NUL byte"), "{err:#}");
    }

    /// `.markdown` is a second spelling of one format, so it must not become a
    /// second `file_type`: a reader filtering `file_type = 'md'` over a mixed
    /// corpus would silently see half of it.
    #[test]
    fn markdown_and_md_report_one_file_type() {
        for path in ["a.md", "a.markdown", "A.MARKDOWN"] {
            let rows = parse_text(b"# T\n\nbody\n", path).unwrap();
            assert_eq!(rows[0].file_type, "md", "{path}");
        }
        let rows = parse_text(b"plain\n", "a.txt").unwrap();
        assert_eq!(rows[0].file_type, "txt");
    }

    #[test]
    fn the_route_is_chosen_by_extension_and_case_does_not_matter() {
        for yes in ["a.md", "a.MD", "a.markdown", "a.txt", "dir/deep/notes.TXT"] {
            assert!(is_text_path(yes), "{yes}");
        }
        for no in [
            "a.pdf", "a.docx", "a.mdx", "a.csv", "a.log", "README", "a.md.pdf",
        ] {
            assert!(!is_text_path(no), "{no}");
        }
    }

    #[test]
    fn rows_carry_the_section_ordinal_and_the_empty_liteparse_columns() {
        let body = "q".repeat(MIN_SECTION_BYTES + 10);
        let text = format!("# One\n\n{body}\n\n## Two\n\n{body}\n");
        let rows = parse_text(text.as_bytes(), "deep/a.md").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].page, 1);
        assert_eq!(rows[1].page, 2);
        assert!(rows.iter().all(|r| r.path == "deep/a.md"));
        assert!(rows.iter().all(|r| r.doc_id == rows[0].doc_id));
        assert!(rows.iter().all(|r| r.tables_json == "[]"));
        assert!(rows.iter().all(|r| r.page_image_ref.is_none()));
        assert!(rows.iter().all(|r| r.image_refs.is_empty()));
    }

    /// The section body is the file's own text, byte for byte. This is the
    /// whole claim of the module: nothing is reconstructed, so nothing can be
    /// reconstructed wrongly.
    #[test]
    fn the_section_body_is_the_file_verbatim() {
        let text = "# Title\n\n| a | b |\n|---|---|\n| 1 | 2 |\n\n```rust\nfn main() {}\n```\n";
        let rows = parse_text(text.as_bytes(), "a.md").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].markdown, text);
    }

    #[test]
    fn an_empty_file_is_no_rows_rather_than_an_error() {
        assert!(parse_text(b"", "a.txt").unwrap().is_empty());
        assert!(parse_text(b"   \n\n \t\n", "a.txt").unwrap().is_empty());
    }

    #[test]
    fn a_one_line_file_is_one_section() {
        let rows = parse_text(b"just one line, no trailing newline", "a.txt").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].markdown, "just one line, no trailing newline");
    }
}
