//! Sanitation ladder: conservative, byte-level repairs for feed documents that
//! are *almost* well-formed.
//!
//! Each rung is a pure byte transform and, by contract (spec AC16), a byte-level
//! no-op on well-formed input. Rungs are applied cumulatively and the ladder
//! stops at the first rung whose output parses — that *driving* logic lives in
//! `parse.rs` (it interleaves with `feed-rs` parse attempts), not here.

/// Which document family a feed body belongs to, decided lexically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocFamily {
    Xml,
    Json,
}

/// A repair that a rung applied, recorded in `feeds.conformance_notes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Repair {
    ReencodedToUtf8,
    StrippedControlChars,
    EscapedNakedAmpersands,
}

impl Repair {
    /// Stable note string carried into `conformance_notes`.
    pub fn note(&self) -> &'static str {
        match self {
            Repair::ReencodedToUtf8 => "sanitation: reencoded-to-utf8",
            Repair::StrippedControlChars => "sanitation: stripped-control-chars",
            Repair::EscapedNakedAmpersands => "sanitation: escaped-naked-ampersands",
        }
    }
}

const UTF8_BOM: &[u8] = &[0xEF, 0xBB, 0xBF];

/// First index of `needle` in `haystack`.
pub(super) fn find_sub(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn strip_utf8_bom(bytes: &[u8]) -> (&[u8], bool) {
    match bytes.strip_prefix(UTF8_BOM) {
        Some(rest) => (rest, true),
        None => (bytes, false),
    }
}

/// A UTF-16 byte-order mark and the bytes after it, if present.
fn utf16_bom(bytes: &[u8]) -> Option<(&'static encoding_rs::Encoding, &[u8])> {
    if let Some(rest) = bytes.strip_prefix(&[0xFF, 0xFE][..]) {
        return Some((encoding_rs::UTF_16LE, rest));
    }
    if let Some(rest) = bytes.strip_prefix(&[0xFE, 0xFF][..]) {
        return Some((encoding_rs::UTF_16BE, rest));
    }
    None
}

/// Decide the document family: first non-whitespace byte `{` → JSON, else XML.
/// Tolerates a leading byte-order mark.
pub fn detect_family(bytes: &[u8]) -> DocFamily {
    let body = match utf16_bom(bytes) {
        Some((_, rest)) => rest,
        None => strip_utf8_bom(bytes).0,
    };
    for &b in body {
        if b.is_ascii_whitespace() {
            continue;
        }
        return if b == b'{' {
            DocFamily::Json
        } else {
            DocFamily::Xml
        };
    }
    DocFamily::Xml
}

/// Refuse documents carrying an internal DTD subset (`<!DOCTYPE … [`), the
/// entity-expansion (billion-laughs) class.
pub fn refuse_internal_dtd(bytes: &[u8]) -> Result<(), String> {
    const REFUSAL: &str = "internal DTD subset refused (entity-expansion guard)";

    let body = strip_utf8_bom(bytes).0;
    let mut i = 0;
    while i < body.len() {
        let rest = &body[i..];
        if rest[0].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if rest[0] != b'<' {
            i += 1;
            continue;
        }
        // Comments and processing instructions may precede the doctype.
        if let Some(skip) = skip_delimited(rest, b"<!--", b"-->") {
            i += skip;
            continue;
        }
        if rest.starts_with(b"<!--") {
            return Ok(()); // unterminated comment: nothing further to inspect
        }
        if let Some(skip) = skip_delimited(rest, b"<?", b"?>") {
            i += skip;
            continue;
        }
        if rest.starts_with(b"<?") {
            return Ok(());
        }
        if rest.starts_with(b"<!DOCTYPE") {
            // Walk the declaration; `[` before its closing `>` opens a subset.
            let mut j = "<!DOCTYPE".len();
            let mut quote: Option<u8> = None;
            while j < rest.len() {
                let c = rest[j];
                match quote {
                    Some(q) if c == q => quote = None,
                    Some(_) => {}
                    None => match c {
                        b'"' | b'\'' => quote = Some(c),
                        b'[' => return Err(REFUSAL.to_string()),
                        b'>' => break,
                        _ => {}
                    },
                }
                j += 1;
            }
            i += j + 1;
            continue;
        }
        // Any other `<` starts the root element — the prolog is over.
        return Ok(());
    }
    Ok(())
}

/// Length to skip past a `open … close` region, if `rest` opens one and it terminates.
fn skip_delimited(rest: &[u8], open: &[u8], close: &[u8]) -> Option<usize> {
    if !rest.starts_with(open) {
        return None;
    }
    find_sub(&rest[open.len()..], close).map(|p| open.len() + p + close.len())
}

/// Rung 1: normalize to UTF-8 — strip any BOM, honor the XML declaration's
/// `encoding=` label, sniff when the declaration is absent or lying.
pub fn rung_reencode_utf8(input: &[u8]) -> (Vec<u8>, bool) {
    // A UTF-16 BOM is authoritative: decode wholesale.
    if let Some((enc, rest)) = utf16_bom(input) {
        let (text, _, _) = enc.decode(rest);
        return (rewrite_decl_encoding(&text), true);
    }

    let (body, had_bom) = strip_utf8_bom(input);

    // Already valid UTF-8: only a BOM counts as a repair. This is what keeps the
    // rung a byte-level no-op on well-formed input, whatever the decl claims.
    if std::str::from_utf8(body).is_ok() {
        return (body.to_vec(), had_bom);
    }

    // Not UTF-8. Trust the declared label unless it claims UTF-8 (a lie, since
    // the bytes just failed UTF-8 validation) — then sniff.
    let declared = xml_decl_encoding_label(body)
        .and_then(|label| encoding_rs::Encoding::for_label(&label))
        .filter(|enc| *enc != encoding_rs::UTF_8);
    // windows-1252 is the sniff fallback: it maps every byte, so decoding cannot fail.
    let enc = declared.unwrap_or(encoding_rs::WINDOWS_1252);
    let (text, _, _) = enc.decode(body);
    (rewrite_decl_encoding(&text), true)
}

/// The `encoding=` label from a leading XML declaration, as raw bytes.
fn xml_decl_encoding_label(body: &[u8]) -> Option<Vec<u8>> {
    if !body.starts_with(b"<?xml") {
        return None;
    }
    let decl = &body[..find_sub(body, b"?>")?];
    let mut j = find_sub(decl, b"encoding")? + "encoding".len();

    while j < decl.len() && decl[j].is_ascii_whitespace() {
        j += 1;
    }
    if decl.get(j) != Some(&b'=') {
        return None;
    }
    j += 1;
    while j < decl.len() && decl[j].is_ascii_whitespace() {
        j += 1;
    }
    let quote = *decl.get(j)?;
    if quote != b'"' && quote != b'\'' {
        return None;
    }
    j += 1;

    let start = j;
    while j < decl.len() && decl[j] != quote {
        j += 1;
    }
    if j >= decl.len() {
        return None;
    }
    Some(decl[start..j].to_vec())
}

/// Point a transcoded document's declaration at UTF-8 so it stops lying.
fn rewrite_decl_encoding(text: &str) -> Vec<u8> {
    let unchanged = || text.as_bytes().to_vec();
    if !text.as_bytes().starts_with(b"<?xml") {
        return unchanged();
    }
    let Some(decl_end) = find_sub(text.as_bytes(), b"?>") else {
        return unchanged();
    };
    let decl = &text[..decl_end];
    let Some(pos) = decl.find("encoding") else {
        return unchanged();
    };

    // `encoding` (ws)* `=` (ws)* quote value quote
    let after = &decl[pos + "encoding".len()..];
    let eq = match after.char_indices().find(|(_, c)| !c.is_whitespace()) {
        Some((i, '=')) => i,
        _ => return unchanged(),
    };
    let rest = &after[eq + 1..];
    let (q_at, quote) = match rest.char_indices().find(|(_, c)| !c.is_whitespace()) {
        Some((i, c @ ('"' | '\''))) => (i, c),
        _ => return unchanged(),
    };
    let val_start = pos + "encoding".len() + eq + 1 + q_at + 1;
    let Some(len) = text[val_start..].find(quote) else {
        return unchanged();
    };

    let mut out = String::with_capacity(text.len() + "UTF-8".len());
    out.push_str(&text[..val_start]);
    out.push_str("UTF-8");
    out.push_str(&text[val_start + len..]);
    out.into_bytes()
}

/// Rung 2: drop bytes/characters that XML 1.0 forbids outright.
pub fn rung_strip_control_chars(input: &[u8]) -> (Vec<u8>, bool) {
    let mut out = Vec::with_capacity(input.len());
    let mut changed = false;
    let mut i = 0;
    while i < input.len() {
        let b = input[i];
        // C0 controls other than tab, LF, CR.
        if b < 0x20 && !matches!(b, b'\t' | b'\n' | b'\r') {
            changed = true;
            i += 1;
            continue;
        }
        // U+FFFE / U+FFFF, the non-characters XML 1.0 also forbids.
        if b == 0xEF
            && input.get(i + 1) == Some(&0xBF)
            && matches!(input.get(i + 2), Some(&0xBE) | Some(&0xBF))
        {
            changed = true;
            i += 3;
            continue;
        }
        out.push(b);
        i += 1;
    }
    (out, changed)
}

/// Rung 3: escape ampersands that do not open a valid entity or character
/// reference, leaving CDATA, comments, and processing instructions untouched.
pub fn rung_escape_naked_ampersands(input: &[u8]) -> (Vec<u8>, bool) {
    let mut out = Vec::with_capacity(input.len());
    let mut changed = false;
    let mut i = 0;
    'scan: while i < input.len() {
        let rest = &input[i..];

        // Pass-through regions, verbatim. An unterminated region takes the tail.
        for (open, close) in [
            (&b"<![CDATA["[..], &b"]]>"[..]),
            (&b"<!--"[..], &b"-->"[..]),
            (&b"<?"[..], &b"?>"[..]),
        ] {
            if rest.starts_with(open) {
                let end = skip_delimited(rest, open, close).unwrap_or(rest.len());
                out.extend_from_slice(&rest[..end]);
                i += end;
                continue 'scan;
            }
        }

        if rest[0] == b'&' {
            match valid_reference_len(rest) {
                Some(n) => {
                    out.extend_from_slice(&rest[..n]);
                    i += n;
                }
                None => {
                    out.extend_from_slice(b"&amp;");
                    i += 1;
                    changed = true;
                }
            }
            continue;
        }

        out.push(rest[0]);
        i += 1;
    }
    (out, changed)
}

/// Byte length of the reference at the start of `rest` (which begins with `&`),
/// or `None` when the `&` is naked or names an entity XML does not define.
fn valid_reference_len(rest: &[u8]) -> Option<usize> {
    debug_assert_eq!(rest[0], b'&');
    let tail = &rest[1..];

    for name in [&b"amp;"[..], b"lt;", b"gt;", b"apos;", b"quot;"] {
        if tail.starts_with(name) {
            return Some(1 + name.len());
        }
    }

    // `&#[0-9]{1,7};` or `&#x[0-9A-Fa-f]{1,6};` (XML allows only a lowercase `x`).
    let digits = tail.strip_prefix(b"#")?;
    let (body, max, is_digit): (&[u8], usize, fn(&u8) -> bool) = match digits.strip_prefix(b"x") {
        Some(hex) => (hex, 6, u8::is_ascii_hexdigit),
        None => (digits, 7, u8::is_ascii_digit),
    };
    let n = body.iter().take(max).take_while(|b| is_digit(b)).count();
    if n == 0 || body.get(n) != Some(&b';') {
        return None;
    }
    Some(rest.len() - body.len() + n + 1)
}

/// A rung: a pure byte transform reporting whether it changed anything.
#[cfg(test)]
type RungFn = fn(&[u8]) -> (Vec<u8>, bool);

/// The three rungs in ladder order, for the conservativeness contract test.
#[cfg(test)]
pub(crate) const RUNGS_FOR_TEST: [(&str, RungFn); 3] = [
    ("reencode_utf8", rung_reencode_utf8),
    ("strip_control_chars", rung_strip_control_chars),
    ("escape_naked_ampersands", rung_escape_naked_ampersands),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_rung_is_a_byte_level_noop_on_wellformed_input() {
        // The conservativeness contract (spec AC16): includes CDATA with legal
        // ampersands, predefined entities, and numeric character references.
        let wellformed: &[&str] = &[
            r#"<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>t &amp; u</title></channel></rss>"#,
            r#"<rss version="2.0"><channel><description><![CDATA[a & b && c]]></description></channel></rss>"#,
            r#"<feed xmlns="http://www.w3.org/2005/Atom"><title>&#169; &#x2014; &lt;ok&gt;</title></feed>"#,
            "<!-- a & naked amp in a comment --><rss version=\"2.0\"/>",
            "<?pi with & inside?><rss version=\"2.0\"/>",
        ];
        for doc in wellformed {
            let b = doc.as_bytes();
            for (name, rung) in RUNGS_FOR_TEST {
                let (out, changed) = rung(b);
                assert!(!changed, "rung {name} changed well-formed doc: {doc}");
                assert_eq!(out, b, "rung {name} output differs on: {doc}");
            }
        }
    }

    #[test]
    fn naked_and_undefined_ampersands_are_escaped_defined_ones_kept() {
        let input = br#"<x a="M &nbsp; N">Fish & Chips &amp; more &#169;</x>"#;
        let expect = br#"<x a="M &amp;nbsp; N">Fish &amp; Chips &amp; more &#169;</x>"#;
        let (out, changed) = rung_escape_naked_ampersands(input);
        assert!(changed);
        assert_eq!(out, expect);
    }

    #[test]
    fn latin1_bytes_reencode_to_utf8() {
        // decl claims iso-8859-1 and the bytes are: caf<0xE9>
        let mut doc = br#"<?xml version="1.0" encoding="iso-8859-1"?><x>caf"#.to_vec();
        doc.push(0xE9);
        doc.extend_from_slice(b"</x>");
        let (out, changed) = rung_reencode_utf8(&doc);
        assert!(changed);
        let s = std::str::from_utf8(&out).unwrap();
        assert!(s.contains("café"));
        assert!(
            !s.contains("iso-8859-1"),
            "decl encoding token rewritten: {s}"
        );
    }

    #[test]
    fn bom_is_stripped() {
        let mut doc = vec![0xEF, 0xBB, 0xBF];
        doc.extend_from_slice(br#"<rss version="2.0"/>"#);
        let (out, changed) = rung_reencode_utf8(&doc);
        assert!(changed, "a leading BOM is a repair");
        assert_eq!(out, br#"<rss version="2.0"/>"#);
    }

    #[test]
    fn lying_utf8_decl_over_latin1_bytes_is_sniffed() {
        // decl says utf-8 but the bytes are not valid UTF-8 → sniff and transcode.
        let mut doc = br#"<?xml version="1.0" encoding="utf-8"?><x>caf"#.to_vec();
        doc.push(0xE9);
        doc.extend_from_slice(b"</x>");
        assert!(
            std::str::from_utf8(&doc).is_err(),
            "fixture must be invalid UTF-8"
        );
        let (out, changed) = rung_reencode_utf8(&doc);
        assert!(changed);
        let s = std::str::from_utf8(&out).expect("output is valid UTF-8");
        assert!(
            s.contains("café"),
            "sniffed transcode recovered the text: {s}"
        );
    }

    #[test]
    fn control_chars_stripped_tab_lf_cr_kept() {
        let mut doc = b"<x>a".to_vec();
        doc.push(0x08); // illegal in XML 1.0
        doc.extend_from_slice(b"b\t c\n d\r e</x>");
        let (out, changed) = rung_strip_control_chars(&doc);
        assert!(changed);
        assert_eq!(out, b"<x>ab\t c\n d\r e</x>");
        assert!(!out.contains(&0x08), "0x08 removed");

        // Tab/LF/CR alone are legal — byte-identical no-op.
        let legal = b"<x>a\tb\nc\rd</x>";
        let (out, changed) = rung_strip_control_chars(legal);
        assert!(!changed, "tab/LF/CR are legal XML 1.0 characters");
        assert_eq!(out, legal);
    }

    #[test]
    fn u_fffe_and_u_ffff_are_stripped() {
        let mut doc = b"<x>a".to_vec();
        doc.extend_from_slice("\u{FFFE}".as_bytes());
        doc.extend_from_slice(b"b");
        doc.extend_from_slice("\u{FFFF}".as_bytes());
        doc.extend_from_slice(b"c</x>");
        let (out, changed) = rung_strip_control_chars(&doc);
        assert!(changed);
        assert_eq!(out, b"<x>abc</x>");
    }

    #[test]
    fn internal_dtd_subset_refused() {
        let doc = br#"<?xml version="1.0"?><!DOCTYPE lolz [ <!ENTITY lol "lol"> <!ENTITY lol2 "&lol;&lol;"> ]><lolz>&lol2;</lolz>"#;
        let err = refuse_internal_dtd(doc).expect_err("billion-laughs prolog must be refused");
        assert!(
            err.contains("internal DTD subset refused"),
            "error names the guard, got: {err}"
        );
        assert!(
            err.contains("entity-expansion guard"),
            "error names the class, got: {err}"
        );
    }

    #[test]
    fn plain_doctype_without_subset_not_refused() {
        refuse_internal_dtd(b"<!DOCTYPE opml><opml version=\"2.0\"/>").unwrap();
    }

    #[test]
    fn json_family_detected() {
        assert_eq!(
            detect_family(br#"{"version": "https://jsonfeed.org/version/1.1"}"#),
            DocFamily::Json
        );
        // leading BOM + whitespace tolerated
        let mut doc = vec![0xEF, 0xBB, 0xBF];
        doc.extend_from_slice(b"  \r\n\t {\"version\": \"1.1\"}");
        assert_eq!(detect_family(&doc), DocFamily::Json);

        assert_eq!(detect_family(br#"<rss version="2.0"/>"#), DocFamily::Xml);
        let mut xml = vec![0xEF, 0xBB, 0xBF];
        xml.extend_from_slice(b"\n <?xml version=\"1.0\"?><feed/>");
        assert_eq!(detect_family(&xml), DocFamily::Xml);
    }

    #[test]
    fn cdata_and_comment_regions_pass_untouched_even_with_naked_amps() {
        for doc in [
            &br#"<x><![CDATA[Tom & Jerry && co]]></x>"#[..],
            &b"<x><!-- Tom & Jerry --></x>"[..],
            &b"<x><?php echo $a & $b; ?></x>"[..],
        ] {
            let (out, changed) = rung_escape_naked_ampersands(doc);
            assert!(
                !changed,
                "CDATA/comment/PI region must pass untouched: {}",
                String::from_utf8_lossy(doc)
            );
            assert_eq!(out, doc);
        }
    }

    #[test]
    fn repair_notes_are_the_contract_strings() {
        assert_eq!(
            Repair::ReencodedToUtf8.note(),
            "sanitation: reencoded-to-utf8"
        );
        assert_eq!(
            Repair::StrippedControlChars.note(),
            "sanitation: stripped-control-chars"
        );
        assert_eq!(
            Repair::EscapedNakedAmpersands.note(),
            "sanitation: escaped-naked-ampersands"
        );
    }
}
