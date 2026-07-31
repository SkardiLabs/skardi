//! The compatibility corpus: a growing-only set of committed feed documents,
//! each pinned to the outcome the parse chain owes it.
//!
//! Test-only — there is no production code in this module. Its purpose is to be
//! a ratchet against the wild web and against the dependency chain
//! (`feed-rs` 2.4 / `quick-xml` 0.41 / `htmd` 0.5.5): every fixture in
//! `fixtures/` either parses or degrades with a recorded reason, never a panic
//! and never a silent skip. `CORPUS` is the manifest, one row per document, and
//! `every_corpus_fixture_parses_or_degrades_visibly` is the runner. (Plain
//! backticks rather than intra-doc links throughout: `rustdoc` never builds a
//! `cfg(test)` module, so a link from here could not be checked by
//! `cargo doc` — measured, by exposing this module to a documenting build and
//! watching the link go unresolved.)
//!
//! Two properties this module exists to enforce, and how:
//!
//! * **A fixture reaches the branch its name claims.** A row expecting failure
//!   names the `stage` *and* a substring of the reason, so a document that
//!   starts failing somewhere else (or for a new reason at the same stage)
//!   fails the test instead of passing for the wrong one. A row expecting a
//!   *rescue* names the exact repair set, so "it parsed" cannot stand in for
//!   "the rung that was supposed to fire fired".
//! * **The Markdown goldens are the contract.** `golden` rows compare
//!   `items[i].content` against a file under `fixtures/golden/` byte for byte,
//!   so any change in the HTML→Markdown conversion has to be reviewed as a diff
//!   to a committed file rather than absorbed by a loose `contains` assertion.
//!
//! The per-dialect deep checks (`rss2_wellformed_full_row_assertions` and its
//! three siblings) assert every column of one row, which the manifest
//! deliberately does not: the manifest's job is breadth over the failure/repair
//! taxonomy, theirs is depth over the Field Mapping table.

use super::parse::{FeedMeta, ItemRow, ParsedDocument, parse_feed_document};

/// What the corpus owes one fixture.
struct Expect {
    /// The dialect `feed-rs` must parse it as; `None` when the document must
    /// fail, in which case `failure` carries the stage and reason.
    dialect: Option<&'static str>,
    /// The dialect the document *claims*, sniffed lexically — asserted on the
    /// failure path too, where it is all that survives.
    declared: Option<&'static str>,
    /// The complete note list. `None` only for a row that must fail, which has
    /// no notes to carry; an empty slice means "parsed, and nothing to observe".
    ///
    /// Deliberately exact rather than a `contains` filter: every document here
    /// is authored in this module's own directory, so its full note list is a
    /// fact about the corpus. A new note appearing on a fixture that was
    /// written not to produce one is exactly the kind of drift this file exists
    /// to surface, and a substring assertion would absorb it.
    notes: Option<&'static [&'static str]>,
    /// `(stage, reason substring)` — a corpus entry expecting failure asserts
    /// both, never a bare "it errored".
    failure: Option<(&'static str, &'static str)>,
    /// Exact row count. Exact rather than a floor: every fixture here is
    /// authored in this module's own directory, so its item count is a fact
    /// about the corpus, and a fixture that starts serving *more* rows than it
    /// was written to serve is as much a regression as one serving fewer.
    items: usize,
    /// `(item index, golden file under `fixtures/golden/`)` for the fixtures
    /// whose `content` conversion is pinned byte-exactly.
    golden: Option<(usize, &'static str)>,
}

impl Expect {
    /// A fixture that must parse.
    const fn parses(dialect: &'static str, declared: &'static str, items: usize) -> Self {
        Expect {
            dialect: Some(dialect),
            declared: Some(declared),
            notes: None,
            failure: None,
            items,
            golden: None,
        }
    }

    /// A fixture that must fail at `stage` with `reason` in its reason string.
    const fn fails(
        stage: &'static str,
        reason: &'static str,
        declared: Option<&'static str>,
    ) -> Self {
        Expect {
            dialect: None,
            declared,
            notes: None,
            failure: Some((stage, reason)),
            items: 0,
            golden: None,
        }
    }

    const fn notes(mut self, notes: &'static [&'static str]) -> Self {
        self.notes = Some(notes);
        self
    }

    const fn golden(mut self, index: usize, path: &'static str) -> Self {
        self.golden = Some((index, path));
        self
    }
}

/// One corpus row: a case name, the fixture it reads, the `Content-Type` the
/// document was served with, and what is expected of it.
struct Case {
    /// Distinct from `fixture` because one document can be a case twice — the
    /// lying-content-type case re-serves `atom10.xml` under an RSS media type.
    case: &'static str,
    fixture: &'static str,
    content_type: Option<&'static str>,
    expect: Expect,
}

const fn case(
    case: &'static str,
    fixture: &'static str,
    content_type: Option<&'static str>,
    expect: Expect,
) -> Case {
    Case {
        case,
        fixture,
        content_type,
        expect,
    }
}

/// The manifest. Rows are added, never removed or loosened.
const CORPUS: &[Case] = &[
    case(
        "rss2_wellformed",
        "rss2_wellformed.xml",
        Some("application/rss+xml"),
        Expect::parses("rss-2.0", "rss-2.0", 2)
            .notes(&[])
            .golden(0, "rss2_wellformed_item0.md"),
    ),
    case(
        "rss2_missing_channel_description",
        "rss2_missing_channel_description.xml",
        Some("application/rss+xml"),
        // The row is still served: an absent required field is an observation
        // about the feed, not a reason to drop its items.
        Expect::parses("rss-2.0", "rss-2.0", 1)
            .notes(&["missing-required-field: channel/description"]),
    ),
    case(
        "rss1_rdf",
        "rss1_rdf.xml",
        Some("application/rss+xml"),
        Expect::parses("rss-1.0", "rss-1.0", 1).notes(&[]),
    ),
    case(
        "atom10",
        "atom10.xml",
        Some("application/atom+xml"),
        Expect::parses("atom", "atom-1.0", 2)
            .notes(&[])
            .golden(0, "atom10_item0.md"),
    ),
    case(
        // The same bytes as the row above, served under an RSS media type.
        "lying_content_type",
        "atom10.xml",
        Some("application/rss+xml"),
        Expect::parses("atom", "atom-1.0", 2)
            .notes(&["content-type-mismatch: served application/rss+xml, parsed atom"]),
    ),
    case(
        "atom03",
        "atom03.xml",
        Some("application/atom+xml"),
        // Measured, not assumed: `feed-rs` 2.4 dispatches on the *root element
        // name* alone (`("feed", _)` in `feed-rs-2.4.0/src/parser/mod.rs`), so
        // an Atom 0.3 document reaches the Atom parser and parses — but its
        // namespace (`http://purl.org/atom/ns#`) maps to `NS::Unknown`
        // (`feed-rs-2.4.0/src/xml/mod.rs:474-484`, which lists only the 1.0
        // namespace), so every child element falls through the parser's
        // `(NS::Atom, …)` match arms and nothing is extracted. The degradation
        // is therefore visible as required-field notes plus zero rows, which is
        // what this row pins.
        Expect::parses("atom", "atom-0.3", 0).notes(&[
            "missing-required-field: feed/title",
            "missing-required-field: feed/updated",
        ]),
    ),
    case(
        "jsonfeed_11",
        "jsonfeed_11.json",
        Some("application/feed+json"),
        Expect::parses("json-feed-1.x", "json-feed-1.1", 2)
            .notes(&[])
            .golden(0, "jsonfeed_11_item0.md"),
    ),
    case(
        "encoding_latin1_mislabeled",
        "encoding_latin1_mislabeled.xml",
        Some("application/rss+xml"),
        Expect::parses("rss-2.0", "rss-2.0", 1).notes(&["sanitation: reencoded-to-utf8"]),
    ),
    case(
        "control_chars",
        "control_chars.xml",
        Some("application/rss+xml"),
        Expect::parses("rss-2.0", "rss-2.0", 1).notes(&["sanitation: stripped-control-chars"]),
    ),
    case(
        "naked_ampersand",
        "naked_ampersand.xml",
        Some("application/rss+xml"),
        // Exactly one repair: the two rungs above the ampersand one must not
        // claim credit for a document that only needed escaping.
        Expect::parses("rss-2.0", "rss-2.0", 1)
            .notes(&["sanitation: escaped-naked-ampersands"])
            .golden(0, "naked_ampersand_item0.md"),
    ),
    case(
        "billion_laughs",
        "billion_laughs.xml",
        Some("application/rss+xml"),
        Expect::fails(
            "refused-internal-dtd",
            "internal DTD subset refused",
            Some("rss-2.0"),
        ),
    ),
    case(
        "hostile_markup",
        "hostile_markup.xml",
        Some("application/rss+xml"),
        Expect::parses("rss-2.0", "rss-2.0", 1)
            .notes(&[])
            .golden(0, "hostile_markup_item0.md"),
    ),
    case(
        "plaintext_typed_markup",
        "plaintext_typed_markup.xml",
        Some("application/atom+xml"),
        // No golden: the point is that nothing converts this value, so the
        // contract is the exact stored string, asserted by
        // `plaintext_typed_content_is_stored_byte_exact` below rather than by a
        // Markdown file that would imply a conversion happened.
        Expect::parses("atom", "atom-1.0", 1).notes(&[]),
    ),
    case(
        "markdown_structures",
        "markdown_structures.xml",
        Some("application/rss+xml"),
        Expect::parses("rss-2.0", "rss-2.0", 1)
            .notes(&[])
            .golden(0, "markdown_structures_item0.md"),
    ),
    case(
        "truncated",
        "truncated.xml",
        Some("application/rss+xml"),
        // The lexical sniff still answers on bytes too broken to parse, which
        // is why `dialect_declared` is asserted here at all.
        Expect::fails("strict-parse", "unable to parse XML", Some("rss-2.0")),
    ),
    case(
        "empty_feed",
        "empty_feed.xml",
        Some("application/rss+xml"),
        // The legitimately-empty case: zero rows is not a fault, and nothing
        // about it may be recorded as one.
        Expect::parses("rss-2.0", "rss-2.0", 0).notes(&[]),
    ),
    case(
        "guidless_items",
        "guidless_items.xml",
        Some("application/rss+xml"),
        Expect::parses("rss-2.0", "rss-2.0", 2).notes(&["entries-without-identity: 1"]),
    ),
];

/// The bytes of one corpus fixture.
///
/// `include_bytes!` rather than a runtime read: the corpus travels inside the
/// test binary, so nothing here depends on the working directory a test run
/// happens to have. The table is the only place a fixture file name is spelled
/// twice, and an unlisted name is a panic rather than a silent skip.
fn fixture_bytes(name: &str) -> &'static [u8] {
    match name {
        "rss2_wellformed.xml" => include_bytes!("fixtures/rss2_wellformed.xml"),
        "rss2_missing_channel_description.xml" => {
            include_bytes!("fixtures/rss2_missing_channel_description.xml")
        }
        "rss1_rdf.xml" => include_bytes!("fixtures/rss1_rdf.xml"),
        "atom10.xml" => include_bytes!("fixtures/atom10.xml"),
        "atom03.xml" => include_bytes!("fixtures/atom03.xml"),
        "jsonfeed_11.json" => include_bytes!("fixtures/jsonfeed_11.json"),
        "encoding_latin1_mislabeled.xml" => {
            include_bytes!("fixtures/encoding_latin1_mislabeled.xml")
        }
        "control_chars.xml" => include_bytes!("fixtures/control_chars.xml"),
        "naked_ampersand.xml" => include_bytes!("fixtures/naked_ampersand.xml"),
        "billion_laughs.xml" => include_bytes!("fixtures/billion_laughs.xml"),
        "hostile_markup.xml" => include_bytes!("fixtures/hostile_markup.xml"),
        "plaintext_typed_markup.xml" => include_bytes!("fixtures/plaintext_typed_markup.xml"),
        "markdown_structures.xml" => include_bytes!("fixtures/markdown_structures.xml"),
        "truncated.xml" => include_bytes!("fixtures/truncated.xml"),
        "empty_feed.xml" => include_bytes!("fixtures/empty_feed.xml"),
        "guidless_items.xml" => include_bytes!("fixtures/guidless_items.xml"),
        other => panic!("corpus fixture {other} is not in the include_bytes! table"),
    }
}

/// A pinned Markdown golden.
///
/// The files are stored with a single trailing newline so they are ordinary text
/// files (and readable as a diff); `html_to_markdown`'s `tidy` guarantees the
/// converter's own output never ends in whitespace, so stripping exactly that
/// newline is the whole of the adjustment — any other trailing whitespace in a
/// golden survives into the comparison and fails it.
fn golden_str(path: &str) -> &'static str {
    let raw = match path {
        "rss2_wellformed_item0.md" => include_str!("fixtures/golden/rss2_wellformed_item0.md"),
        "atom10_item0.md" => include_str!("fixtures/golden/atom10_item0.md"),
        "jsonfeed_11_item0.md" => include_str!("fixtures/golden/jsonfeed_11_item0.md"),
        "naked_ampersand_item0.md" => include_str!("fixtures/golden/naked_ampersand_item0.md"),
        "hostile_markup_item0.md" => include_str!("fixtures/golden/hostile_markup_item0.md"),
        "markdown_structures_item0.md" => {
            include_str!("fixtures/golden/markdown_structures_item0.md")
        }
        other => panic!("golden {other} is not in the include_str! table"),
    };
    raw.strip_suffix('\n').unwrap_or(raw)
}

/// Parse one corpus case, or panic naming it.
fn parse_case(name: &str) -> ParsedDocument {
    let case = CORPUS
        .iter()
        .find(|c| c.case == name)
        .unwrap_or_else(|| panic!("no corpus case named {name}"));
    parse_feed_document(fixture_bytes(case.fixture), case.content_type)
        .unwrap_or_else(|e| panic!("corpus case {name} must parse: {e:?}"))
}

fn item(name: &str, index: usize) -> ItemRow {
    let doc = parse_case(name);
    doc.items
        .get(index)
        .unwrap_or_else(|| panic!("corpus case {name} has no item {index}"))
        .clone()
}

#[test]
fn every_corpus_fixture_parses_or_degrades_visibly() {
    for Case {
        case,
        fixture,
        content_type,
        expect,
    } in CORPUS
    {
        let got = parse_feed_document(fixture_bytes(fixture), *content_type);
        match (&got, expect.failure) {
            (Err(failure), Some((stage, reason))) => {
                assert_eq!(failure.stage, stage, "{case}: wrong failure stage");
                assert!(
                    failure.reason.contains(reason),
                    "{case}: reason must name {reason:?}, got {:?}",
                    failure.reason
                );
                assert_eq!(
                    failure.dialect_declared.as_deref(),
                    expect.declared,
                    "{case}: the declared sniff answers even on a failed parse"
                );
            }
            (Ok(doc), None) => {
                assert_eq!(Some(doc.dialect), expect.dialect, "{case}: wrong dialect");
                assert_eq!(
                    doc.dialect_declared.as_deref(),
                    expect.declared,
                    "{case}: wrong declared dialect"
                );
                assert_eq!(
                    Some(doc.conformance_notes.as_slice()),
                    expect
                        .notes
                        .map(|notes| notes.iter().map(|n| n.to_string()).collect::<Vec<_>>())
                        .as_deref(),
                    "{case}: the conformance notes are pinned exactly"
                );
                assert_eq!(doc.items.len(), expect.items, "{case}: wrong row count");
                if let Some((index, golden)) = expect.golden {
                    assert_eq!(
                        doc.items[index].content.as_deref(),
                        Some(golden_str(golden)),
                        "{case}: golden drift against fixtures/golden/{golden}"
                    );
                }
            }
            (Ok(doc), Some((stage, _))) => panic!(
                "{case}: expected failure at {stage}, parsed as {} with {:?}",
                doc.dialect, doc.conformance_notes
            ),
            (Err(failure), None) => panic!("{case}: expected a parse, got {failure:?}"),
        }
    }
}

/// Every fixture and golden on disk is reachable from the manifest, and every
/// name the manifest uses exists.
///
/// The `include_bytes!`/`include_str!` tables make the second half a compile
/// error already; this covers the first — a fixture committed but never
/// referenced would otherwise sit in the corpus asserting nothing.
#[test]
fn no_fixture_or_golden_is_orphaned() {
    // Not in the manifest by design: `bomb.xml.gz` is consumed by the
    // decompressed-size-cap test rather than by the parse chain (a corpus row
    // would have to decompress it to have an opinion), and `golden_probe.html`
    // is `convert.rs`'s determinism input, not a feed document.
    const NOT_CORPUS_DOCUMENTS: &[&str] = &["bomb.xml.gz", "golden_probe.html"];

    let dir = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/sources/providers/rss/fixtures"
    );
    let mut unreferenced = Vec::new();
    for entry in std::fs::read_dir(dir).expect("the fixtures directory exists") {
        let entry = entry.expect("readable directory entry");
        let name = entry.file_name().to_string_lossy().into_owned();
        if entry.file_type().expect("file type").is_dir() || NOT_CORPUS_DOCUMENTS.contains(&&*name)
        {
            continue;
        }
        if !CORPUS.iter().any(|c| c.fixture == name) {
            unreferenced.push(name);
        }
    }
    assert!(
        unreferenced.is_empty(),
        "fixtures committed but not in CORPUS: {unreferenced:?}"
    );

    let golden_dir = format!("{dir}/golden");
    let mut unreferenced_goldens = Vec::new();
    for entry in std::fs::read_dir(&golden_dir).expect("the golden directory exists") {
        let name = entry.expect("readable directory entry").file_name();
        let name = name.to_string_lossy().into_owned();
        if !CORPUS
            .iter()
            .any(|c| c.expect.golden.is_some_and(|(_, g)| g == name))
        {
            unreferenced_goldens.push(name);
        }
    }
    assert!(
        unreferenced_goldens.is_empty(),
        "goldens committed but not pinned by any corpus row: {unreferenced_goldens:?}"
    );
}

/// Identical bytes must yield identical rows, fixture by fixture.
///
/// Cheap here and worth having: `items.guid` is a cache key and a window
/// identity, so any nondeterminism in the chain (a random id, an unordered
/// map reaching `extensions_json`, a locale-dependent timestamp) would show up
/// as row churn on every scan rather than as a failing assertion anywhere else.
#[test]
fn every_corpus_fixture_extracts_deterministically() {
    for Case {
        case,
        fixture,
        content_type,
        ..
    } in CORPUS
    {
        let bytes = fixture_bytes(fixture);
        let a = parse_feed_document(bytes, *content_type);
        let b = parse_feed_document(bytes, *content_type);
        match (a, b) {
            (Ok(a), Ok(b)) => {
                assert_eq!(a.items, b.items, "{case}: rows differ between parses");
                assert_eq!(a.meta, b.meta, "{case}: feed metadata differs");
                assert_eq!(
                    a.conformance_notes, b.conformance_notes,
                    "{case}: notes differ"
                );
            }
            (Err(a), Err(b)) => assert_eq!(a, b, "{case}: failures differ between parses"),
            (a, b) => panic!("{case}: one parse succeeded and the other did not: {a:?} / {b:?}"),
        }
    }
}

// --- per-dialect deep checks -------------------------------------------------
//
// One whole row per dialect, compared as a struct rather than field by field:
// a column added to `ItemRow` fails these to compile rather than slipping in
// unasserted, which is the point of calling them "full row" assertions.

#[test]
fn rss2_wellformed_full_row_assertions() {
    let doc = parse_case("rss2_wellformed");
    assert_eq!(
        doc.meta,
        FeedMeta {
            title: Some("Corpus Weekly".to_string()),
            site_url: Some("https://corpus.example/".to_string()),
            description: Some("A well-formed RSS 2.0 channel.".to_string()),
        }
    );
    assert_eq!(
        doc.items[0],
        ItemRow {
            guid: "tag:corpus.example,2026:post-1".to_string(),
            title: Some("First post".to_string()),
            link: Some("https://corpus.example/posts/1".to_string()),
            // `<dc:creator>`, since RSS 2.0 has no author element of its own
            // that carries a plain name.
            author: Some("Ada Lovelace".to_string()),
            // `Mon, 20 Jul 2026 10:00:00 GMT`, the RFC-822 form.
            published_ms: Some(1_784_541_600_000), // 2026-07-20T10:00:00Z
            // The item declares no update time. `feed-rs` copies `published`
            // into `updated` for RSS 2.0 when the latter is absent
            // (`feed-rs-2.4.0/src/parser/rss2/mod.rs:279-281`), so the two are
            // equal here rather than the update time being null — pinned
            // because it is a dependency decision, not ours.
            updated_ms: Some(1_784_541_600_000),
            // `<content:encoded>`, HTML, converted.
            content: Some(golden_str("rss2_wellformed_item0.md").to_string()),
            // `<description>` — plain prose, so the HTML pass leaves it alone.
            summary: Some("Plain summary for the first post.".to_string()),
            categories: vec!["rust".to_string(), "news".to_string()],
            enclosure_url: Some("https://corpus.example/audio/1.mp3".to_string()),
            enclosure_type: Some("audio/mpeg".to_string()),
            enclosure_length: Some(12345),
            extensions_json: None,
        }
    );
    // The second item's `pubDate` carries a numeric offset rather than `GMT`,
    // which the RFC-822 parse has to honor: `Tue, 21 Jul 2026 08:30:00 -0400`
    // is 12:30Z, not 08:30Z.
    assert_eq!(
        doc.items[1].published_ms,
        Some(1_784_637_000_000), // 2026-07-21T12:30:00Z
        "an RFC-822 numeric offset must be applied, not ignored"
    );
}

#[test]
fn atom10_full_row_assertions() {
    let doc = parse_case("atom10");
    assert_eq!(
        doc.meta,
        FeedMeta {
            title: Some("Corpus Atom".to_string()),
            // `<link rel="self">` comes first in the document; the site URL is
            // the `alternate`, not the self-reference.
            site_url: Some("https://corpus.example/".to_string()),
            description: Some("An Atom 1.0 feed.".to_string()),
        }
    );
    assert_eq!(
        doc.items[0],
        ItemRow {
            guid: "urn:uuid:8b3f0c3e-0001-4000-8000-000000000000".to_string(),
            title: Some("Atom post".to_string()),
            link: Some("https://corpus.example/atom/1".to_string()),
            author: Some("Radia Perlman".to_string()),
            published_ms: Some(1_784_541_600_000), // 2026-07-20T10:00:00Z
            // Atom carries both timestamps, so unlike RSS 2.0 above these differ.
            updated_ms: Some(1_784_631_600_000), // 2026-07-21T11:00:00Z
            content: Some(golden_str("atom10_item0.md").to_string()),
            // `type="text"` is stored byte-exact: the `<` is content, not markup,
            // and must not be Markdown-escaped or converted on the way in.
            summary: Some("Plain 3 < 4 summary, stored verbatim.".to_string()),
            // The first category carries both `term="rust"` and `label="Rust"`;
            // `term` wins.
            categories: vec!["rust".to_string(), "atom".to_string()],
            enclosure_url: Some("https://corpus.example/audio/atom-1.mp3".to_string()),
            enclosure_type: Some("audio/mpeg".to_string()),
            enclosure_length: Some(4242),
            extensions_json: None,
        }
    );
}

#[test]
fn jsonfeed_11_full_row_assertions() {
    let doc = parse_case("jsonfeed_11");
    assert_eq!(
        doc.meta,
        FeedMeta {
            title: Some("Corpus JSON Feed".to_string()),
            site_url: Some("https://corpus.example/".to_string()),
            description: Some("A JSON Feed 1.1 document.".to_string()),
        }
    );
    assert_eq!(
        doc.items[0],
        ItemRow {
            guid: "corpus-json-1".to_string(),
            title: Some("JSON post".to_string()),
            link: Some("https://corpus.example/json/1".to_string()),
            author: Some("Katherine Johnson".to_string()),
            published_ms: Some(1_784_541_600_000), // 2026-07-20T10:00:00Z
            updated_ms: Some(1_784_631_600_000),   // 2026-07-21T11:00:00Z
            content: Some(golden_str("jsonfeed_11_item0.md").to_string()),
            summary: Some("Summary supplied explicitly.".to_string()),
            categories: vec!["rust".to_string(), "json".to_string()],
            // A JSON Feed attachment, which `feed-rs` puts in `entry.links`
            // rather than in `media` — the `media_type` is what keeps it from
            // being mistaken for the item's own URL.
            enclosure_url: Some("https://corpus.example/audio/json-1.mp3".to_string()),
            enclosure_type: Some("audio/mpeg".to_string()),
            enclosure_length: Some(4096),
            extensions_json: None,
        }
    );
    // `content_text` is stored byte-exact — no Markdown conversion, no
    // escaping, and the double space in the middle intact. It lands in
    // `content` rather than `summary` because the item supplies no
    // `content_html` (`feed-rs-2.4.0/src/parser/json/mod.rs:125-135`: the text
    // fills `content` when `content_html` left it empty).
    assert_eq!(
        doc.items[1].content.as_deref(),
        Some("Stored verbatim: 3 < 4 & *not italic*  <b>not bold</b>"),
        "content_text must pass through byte-exact"
    );
    assert_eq!(doc.items[1].summary, None);
}

#[test]
fn rss1_rdf_full_row_assertions() {
    let doc = parse_case("rss1_rdf");
    assert_eq!(
        doc.meta,
        FeedMeta {
            title: Some("Corpus RDF".to_string()),
            site_url: Some("https://corpus.example/".to_string()),
            description: Some("An RSS 1.0 (RDF) channel.".to_string()),
        }
    );
    assert_eq!(
        doc.items[0],
        ItemRow {
            // Measured, and *not* what the plan predicted: the identity is the
            // item's `<link>`, not its `rdf:about`. `feed-rs` 2.4 never reads
            // that attribute — `handle_item`
            // (`feed-rs-2.4.0/src/parser/rss1/mod.rs:94-128`) matches only
            // element children and has no arm for it, and `entry.id` is
            // assigned nowhere in that module — so `extract_entry`'s
            // link fallback supplies the guid. The fixture deliberately gives
            // the two different values so this test can tell them apart; if a
            // future `feed-rs` starts mapping `rdf:about`, this fails and says
            // so.
            guid: "https://corpus.example/rss1/1".to_string(),
            title: Some("RDF post".to_string()),
            link: Some("https://corpus.example/rss1/1".to_string()),
            author: Some("Grace Hopper".to_string()),
            // `<dc:date>`, ISO-8601 rather than RFC-822.
            published_ms: Some(1_784_541_600_000), // 2026-07-20T10:00:00Z
            // RSS 1.0 has no update time and, unlike the RSS 2.0 path, nothing
            // copies `published` into it.
            updated_ms: None,
            content: None,
            summary: Some("Summary of the RDF post.".to_string()),
            categories: vec![],
            enclosure_url: None,
            enclosure_type: None,
            enclosure_length: None,
            extensions_json: None,
        }
    );
}

// --- the branch each fixture is named for ------------------------------------

#[test]
fn control_chars_fixture_keeps_the_text_the_control_byte_split() {
    // The note alone says a rung changed bytes; this says the change was a
    // rescue. The 0x08 sat between `Inter` and `rupted`, and the 0x1F inside
    // the channel description.
    let doc = parse_case("control_chars");
    assert_eq!(doc.items[0].title.as_deref(), Some("Interrupted title"));
    assert_eq!(
        doc.meta.description.as_deref(),
        Some("A channel description."),
        "the forbidden character is dropped, not replaced by a substitute"
    );
}

#[test]
fn encoding_latin1_fixture_recovers_the_accented_characters() {
    // The manifest row asserts only the `sanitation: reencoded-to-utf8` note,
    // which says a rung changed bytes — not that it changed them *correctly*. A
    // rung that rescued the document into mojibake (sniffing some other
    // single-byte encoding, or replacing each undecodable byte with U+FFFD)
    // would satisfy the note and the row count both. These are the recovered
    // characters, so the note cannot stand in for the repair.
    //
    // Every accented character in the fixture is the single byte 0xE9 —
    // Latin-1's `é`, and not valid UTF-8 (verified against the committed bytes:
    // `<title>Caf\xe9 Corpus</title>`, `<title>Caf\xe9 au lait</title>`,
    // `<description>R\xe9sum\xe9 en latin-1.</description>`).
    let doc = parse_case("encoding_latin1_mislabeled");
    assert_eq!(
        doc.items[0].title.as_deref(),
        Some("Café au lait"),
        "the 0xE9 byte must arrive as U+00E9, not as mojibake or U+FFFD"
    );
    assert_eq!(doc.meta.title.as_deref(), Some("Café Corpus"));
    assert_eq!(
        doc.items[0].summary.as_deref(),
        Some("Résumé en latin-1."),
        "two 0xE9 bytes in one field, both recovered"
    );
}

#[test]
fn naked_ampersand_fixture_keeps_cdata_intact_and_resolves_nbsp() {
    // The golden pins all of this byte-exactly; these assertions name the three
    // properties it encodes, because a no-break space is invisible in a diff.
    let content = item("naked_ampersand", 0).content.expect("content present");
    assert!(
        content.contains("3 && 4"),
        "a CDATA `&&` must survive untouched: {content:?}"
    );
    assert!(
        content.contains('©'),
        "`&#169;` inside CDATA is a valid reference the HTML pass decodes: {content:?}"
    );
    assert!(
        content.contains("hard\u{A0}space"),
        "`&nbsp;` must arrive as U+00A0, not as literal text: {content:?}"
    );
    // The rung escaped the summary's `&nbsp;` to `&amp;nbsp;`, so it reached the
    // HTML pass as text and decoded there too.
    let summary = item("naked_ampersand", 0).summary.expect("summary present");
    assert!(summary.contains('\u{A0}'), "{summary:?}");
    // And the naked `&` that made the rung fire is still an ampersand.
    assert_eq!(
        parse_case("naked_ampersand").meta.title.as_deref(),
        Some("Fish & Chips")
    );
}

#[test]
fn hostile_markup_content_carries_no_tag_and_no_script_or_style_text() {
    let content = item("hostile_markup", 0).content.expect("content present");
    assert!(
        !content.contains('<'),
        "no `<` at all in this fixture's output: {content:?}"
    );
    // Every dropped-or-inert body in the fixture is planted with `leak`: the
    // script body, the style body, the two event handlers, and the iframe's
    // `src`. None of them is document content.
    assert!(
        !content.contains("leak"),
        "a script/style/handler/iframe body reached the stored content: {content:?}"
    );
    assert!(
        content.contains("custom element text"),
        "an unknown element's text is content and must be kept: {content:?}"
    );
    // The `javascript:` URL is stored as link data (consumers filter schemes),
    // which is what keeps this from being a silent content loss.
    assert!(content.contains("javascript:void"), "{content:?}");
}

/// The counterpart to the test above, and the reason `docs/rss.md` claims "no
/// HTML *tag* survives as markup" rather than "no raw HTML is stored".
///
/// An Atom value typed `type="text"` is not markup by the feed's own assertion,
/// so it is not converted — it is stored byte-exact, and a feed that spells out
/// HTML in escaped text gets that HTML back verbatim. The escaping is XML
/// transport encoding and is removed at extraction, so `&lt;script&gt;` becomes
/// a literal `<script>`.
///
/// Pinned as exact equality, both fields, deliberately: this is the shape the
/// documentation's stronger claim denied, and a `contains` assertion would let a
/// converter that started mangling text-typed values pass. Nothing here says the
/// behaviour is *safe* to render — the two renderer rules in `docs/rss.md` are
/// what make it safe, and they were always what the guarantee rested on.
#[test]
fn plaintext_typed_content_is_stored_byte_exact_tags_included() {
    let row = item("plaintext_typed_markup", 0);

    let content = row.content.clone().expect("content present");
    assert_eq!(
        content, "<script>alert(1)</script>",
        "a text-typed value is stored verbatim, tag-shaped text included"
    );

    let summary = row.summary.clone().expect("summary present");
    assert_eq!(
        summary, "<b>not bold</b> & not escaped further",
        "the same holds for `summary`, and the `&amp;` decodes to a bare `&` \
         rather than being re-escaped"
    );
}

#[test]
fn guidless_items_fall_back_to_their_link_for_identity() {
    let doc = parse_case("guidless_items");
    assert_eq!(
        doc.items
            .iter()
            .map(|i| i.guid.as_str())
            .collect::<Vec<_>>(),
        vec![
            "https://corpus.example/guidless/1",
            "https://corpus.example/guidless/2"
        ],
    );
    // The third entry had neither, so it is absent from the rows and present in
    // the notes — never silently dropped.
    assert!(
        doc.conformance_notes
            .iter()
            .any(|n| n == "entries-without-identity: 1"),
        "{:?}",
        doc.conformance_notes
    );
}

/// The refusal is the guard's, not a parse error's — and it has to be, because
/// the parser would not expand these entities anyway.
///
/// Measured at the pinned versions rather than reasoned about: handed the same
/// bytes directly, `feed-rs` 2.4 / `quick-xml` 0.41 parse the document and
/// leave `&lol3;` in the title as literal text (unresolvable general references
/// are written back verbatim — `feed-rs-2.4.0/src/xml/mod.rs:330-346`). So the
/// billion-laughs class costs nothing *today*; the guard is what keeps that
/// true if a future version starts honoring internal DTD declarations, and this
/// test fails if either half of that changes.
#[test]
fn billion_laughs_is_refused_by_the_guard_not_by_the_parser() {
    let bytes = fixture_bytes("billion_laughs.xml");
    let failure = parse_feed_document(bytes, Some("application/rss+xml"))
        .expect_err("an internal DTD subset must be refused");
    assert_eq!(failure.stage, "refused-internal-dtd");
    assert!(
        failure.reason.contains("entity-expansion guard"),
        "the reason names the class: {}",
        failure.reason
    );

    let raw = feed_rs::parser::parse(bytes).expect("feed-rs itself tolerates the document");
    let title = raw
        .title
        .expect("the channel title is present, unexpanded")
        .content;
    assert_eq!(
        title, "&lol3;",
        "feed-rs no longer leaves DTD entities unexpanded; the guard is now load-bearing \
         against real expansion and this test's premise needs revisiting"
    );
}

/// A failed parse's reason is copied into `feeds.last_error`, which must carry
/// no response-body content.
#[test]
fn the_truncated_fixture_reason_echoes_no_document_prose() {
    let failure = parse_feed_document(fixture_bytes("truncated.xml"), Some("application/rss+xml"))
        .expect_err("a document cut off mid-tag must fail");
    for prose in ["Truncated Corpus", "Cut off here", "corpus.example"] {
        assert!(
            !failure.reason.contains(prose),
            "the failure reason echoed document content ({prose:?}): {}",
            failure.reason
        );
    }
}

/// `bomb.xml.gz` is not a corpus row — it is the input to the
/// decompressed-size-cap test, which lives with the HTTP suite. What this
/// checks is that the committed file still *is* a bomb: small on the wire,
/// far past the 5 MiB `max_response_bytes` default once inflated.
///
/// The inflated size is read from the gzip footer's `ISIZE` field — the last
/// four bytes of the member, little-endian, per RFC 1952 §2.3.1 — so this
/// needs no decompressor and no new dependency. It is the size mod 2^32, which
/// is exact here because the file is 8 MiB.
#[test]
fn the_gzip_bomb_fixture_is_still_a_bomb() {
    const CAP: u64 = 5 * 1024 * 1024; // `default_max_response_bytes()`
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/sources/providers/rss/fixtures/bomb.xml.gz"
    );
    let gz = std::fs::read(path).expect("bomb.xml.gz is committed");
    assert_eq!(&gz[..2], &[0x1F, 0x8B], "gzip magic");
    let isize_bytes: [u8; 4] = gz[gz.len() - 4..].try_into().expect("four footer bytes");
    let inflated = u64::from(u32::from_le_bytes(isize_bytes));
    assert!(
        inflated > CAP,
        "inflated to {inflated} bytes, which no longer exceeds the {CAP}-byte cap"
    );
    assert!(
        (gz.len() as u64) < 64 * 1024,
        "the wire form has grown to {} bytes; keep it small enough to commit",
        gz.len()
    );
}
