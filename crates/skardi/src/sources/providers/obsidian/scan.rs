//! The vault scanner: list → filter → read → parse → resolve, producing one
//! `ParsedNote` per `.md` file. `VaultScan::run` is synchronous and must run
//! inside `tokio::task::spawn_blocking` (use [`run_scan`]); it drives the
//! `BlobStore` futures with `Handle::current().block_on`, and resolves the
//! store inside the same task so an S3 client never crosses runtimes.

use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tokio::runtime::Handle;

use super::config::ScanOptions;
use super::frontmatter;
use super::markdown::{self, RawLink};
use super::resolve::{Index, LinkKind, Resolution};
use crate::sources::providers::blob::{
    BlobEntry, BlobStore, ListOptions, ReadOptions, SizeCapExceeded,
};

/// Where a link or tag was found (`links.source`, `tags.source`). `Body`
/// sorts before `Frontmatter`, which is the `tags` row order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Source {
    Body,
    Frontmatter,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Source::Body => "body",
            Source::Frontmatter => "frontmatter",
        }
    }
}

/// One `links` row minus `from_path` (the owning note's path).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkRow {
    pub to_path: Option<String>,
    pub target: String,
    pub kind: LinkKind,
    pub display_text: Option<String>,
    pub heading: Option<String>,
    pub block_id: Option<String>,
    pub resolution: Resolution,
    pub source: Source,
    pub line: Option<u32>,
}

/// One `tags` row minus `path`. Derived `Ord` is `(tag, source)`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TagRow {
    pub tag: String,
    pub source: Source,
}

/// Everything the three tables need about one note.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedNote {
    pub path: String,
    pub name: String,
    pub folder: String,
    pub body: String,
    pub frontmatter_json: Option<String>,
    pub frontmatter_error: Option<String>,
    pub aliases: Option<Vec<String>>,
    pub size_bytes: i64,
    /// Milliseconds since the UNIX epoch, UTC.
    pub modified_ms: i64,
    /// Sorted by `(tag, source)`, distinct.
    pub tags: Vec<TagRow>,
    /// Frontmatter links in traversal order, then body links by offset.
    pub links: Vec<LinkRow>,
}

/// `true` for `.md` / `.MD` (case-insensitive). Byte-based so a trailing
/// multi-byte character cannot land the slice mid-character.
fn is_markdown(rel_key: &str) -> bool {
    let bytes = rel_key.as_bytes();
    bytes.len() > 3 && bytes[bytes.len() - 3..].eq_ignore_ascii_case(b".md")
}

/// Parse one note's text. Pure: no I/O, no runtime.
pub fn parse_note(
    path: &str,
    size: u64,
    modified: DateTime<Utc>,
    text: &str,
    index: &Index,
) -> ParsedNote {
    let split = frontmatter::split(text);
    let (frontmatter_json, frontmatter_error, frontmatter_value) = match split.yaml {
        None => (None, None, None),
        Some(yaml) => match frontmatter::parse(yaml) {
            Ok(value) => (Some(value.to_string()), None, Some(value)),
            Err(message) => (None, Some(message), None),
        },
    };
    let aliases = frontmatter_value.as_ref().and_then(frontmatter::aliases);

    let mut links: Vec<LinkRow> = Vec::new();
    let mut tags: Vec<TagRow> = Vec::new();
    if let Some(value) = &frontmatter_value {
        for raw in frontmatter::links(value) {
            links.push(link_row(path, &raw, Source::Frontmatter, index));
        }
        for tag in frontmatter::tags(value) {
            tags.push(TagRow {
                tag,
                source: Source::Frontmatter,
            });
        }
    }
    let extracted = markdown::extract(split.body, split.body_first_line);
    for raw in &extracted.links {
        links.push(link_row(path, raw, Source::Body, index));
    }
    for (tag, _line) in extracted.tags {
        let row = TagRow {
            tag,
            source: Source::Body,
        };
        if !tags.contains(&row) {
            tags.push(row);
        }
    }
    tags.sort();

    let file_name = path.rsplit('/').next().unwrap_or(path);
    let name = Path::new(file_name)
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| file_name.to_string());
    let folder = path
        .rsplit_once('/')
        .map(|(folder, _)| folder.to_string())
        .unwrap_or_default();

    ParsedNote {
        path: path.to_string(),
        name,
        folder,
        body: split.body.to_string(),
        frontmatter_json,
        frontmatter_error,
        aliases,
        size_bytes: i64::try_from(size).unwrap_or(i64::MAX),
        modified_ms: modified.timestamp_millis(),
        tags,
        links,
    }
}

fn link_row(from_path: &str, raw: &RawLink, source: Source, index: &Index) -> LinkRow {
    let resolved = index.resolve(from_path, raw);
    LinkRow {
        to_path: resolved.to_path,
        target: raw.target.clone(),
        kind: resolved.kind,
        display_text: raw.display_text.clone(),
        heading: raw.heading.clone(),
        block_id: raw.block_id.clone(),
        resolution: resolved.resolution,
        source,
        line: raw.line,
    }
}

/// The scanner. A unit struct so the entry point reads as
/// `VaultScan::run(root, opts)` at the call sites the spec names.
pub struct VaultScan;

impl VaultScan {
    /// Full synchronous scan of `root`. **Blocking**: call only from inside
    /// `spawn_blocking` (see [`run_scan`]); panics outside a Tokio runtime
    /// because it needs `Handle::current()` to drive the S3 arms.
    pub fn run(root: &str, opts: &ScanOptions) -> Result<Vec<ParsedNote>> {
        let started = Instant::now();
        let handle = Handle::current();

        let (store, prefix) = BlobStore::resolve(root)
            .with_context(|| format!("obsidian: resolving vault root {root}"))?;
        let entries = handle
            .block_on(store.list(
                &prefix,
                ListOptions {
                    recursive: true,
                    follow_symlinks: false,
                },
            ))
            .with_context(|| format!("obsidian: listing vault root {root}"))?;
        let listed = entries.len();

        let kept: Vec<BlobEntry> = entries
            .into_iter()
            .filter(|entry| !opts.is_excluded(&entry.rel_key))
            .collect();
        let all_paths: Vec<&str> = kept.iter().map(|e| e.rel_key.as_str()).collect();
        let index = Index::build(&all_paths);

        let mut notes = Vec::new();
        let mut skipped = 0usize;
        let mut attempted = 0usize;
        let mut failed = 0usize;
        let mut first_failure: Option<(String, String)> = None;
        for entry in &kept {
            if !is_markdown(&entry.rel_key) {
                continue;
            }
            if entry.size > opts.max_file_bytes {
                tracing::warn!(
                    path = %entry.rel_key,
                    size = entry.size,
                    max_file_bytes = opts.max_file_bytes,
                    "obsidian: skipping note over max_file_bytes"
                );
                skipped += 1;
                continue;
            }
            attempted += 1;
            let bytes = match handle.block_on(store.get(
                &entry.loc,
                ReadOptions::no_symlinks_beneath(&prefix).with_max_bytes(opts.max_file_bytes),
            )) {
                Ok(bytes) => bytes,
                Err(e) if e.downcast_ref::<SizeCapExceeded>().is_some() => {
                    // The note grew past the cap between listing and read.
                    // Same policy as the listing-time skip, so it is a skip and
                    // not an attempt: an oversized-only vault stays empty
                    // rather than tripping the wholesale-failure guard.
                    tracing::warn!(
                        path = %entry.rel_key,
                        max_file_bytes = opts.max_file_bytes,
                        "obsidian: skipping note that grew past max_file_bytes after listing"
                    );
                    attempted -= 1;
                    skipped += 1;
                    continue;
                }
                Err(e) => {
                    let cause = format!("{e:#}");
                    tracing::warn!(path = %entry.rel_key, error = %cause, "obsidian: skipping unreadable note");
                    failed += 1;
                    if first_failure.is_none() {
                        first_failure = Some((entry.rel_key.clone(), cause));
                    }
                    continue;
                }
            };
            let text = String::from_utf8_lossy(&bytes);
            notes.push(parse_note(
                &entry.rel_key,
                entry.size,
                entry.modified,
                &text,
                &index,
            ));
        }

        // Wholesale-failure guard (spec §Failure Modes): policy skips are not
        // attempts, so an oversized-only vault is empty, not an error.
        if attempted > 0 && failed == attempted {
            let (path, cause) = first_failure.unwrap_or_default();
            anyhow::bail!(
                "obsidian: every note read under {root} failed ({attempted} attempted; first failure {path}: {cause})"
            );
        }

        notes.sort_by(|a, b| a.path.cmp(&b.path));
        tracing::debug!(
            root = %root,
            files = listed,
            notes = notes.len(),
            skipped,
            failed,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "obsidian: scan complete"
        );
        Ok(notes)
    }
}

/// Run [`VaultScan::run`] on the blocking pool. This is the only entry point
/// async code (the `ExecutionPlan`, tests) should use.
pub async fn run_scan(root: String, opts: ScanOptions) -> Result<Vec<ParsedNote>> {
    tokio::task::spawn_blocking(move || VaultScan::run(&root, &opts))
        .await
        .context("obsidian: scan task panicked or was cancelled")?
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    fn fixture_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/sources/providers/obsidian/fixtures/vault")
    }

    fn defaults() -> ScanOptions {
        ScanOptions::from_map(None).unwrap()
    }

    /// Recursive copy so tests can chmod/replace files without touching the
    /// committed fixture.
    fn copy_dir(src: &Path, dst: &Path) {
        std::fs::create_dir_all(dst).unwrap();
        for entry in std::fs::read_dir(src).unwrap() {
            let entry = entry.unwrap();
            let target = dst.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_dir(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }

    async fn scan(root: &Path, opts: ScanOptions) -> anyhow::Result<Vec<ParsedNote>> {
        run_scan(root.to_string_lossy().into_owned(), opts).await
    }

    type LinkSummary<'a> = (
        Option<&'a str>,
        &'a str,
        &'a str,
        &'a str,
        &'a str,
        Option<u32>,
    );
    fn summarize(links: &[LinkRow]) -> Vec<LinkSummary<'_>> {
        links
            .iter()
            .map(|l| {
                (
                    l.to_path.as_deref(),
                    l.target.as_str(),
                    l.kind.as_str(),
                    l.resolution.as_str(),
                    l.source.as_str(),
                    l.line,
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn fixture_notes_are_ordered_and_shaped() {
        let notes = scan(&fixture_root(), defaults()).await.unwrap();
        let paths: Vec<&str> = notes.iter().map(|n| n.path.as_str()).collect();
        assert_eq!(
            paths,
            vec![
                "Archive/Notes.md",
                "Bad Frontmatter.md",
                "CJK.md",
                "Home.md",
                "Large.md",
                "Meeting.md",
                "No Frontmatter.md",
                "People/Alice.md",
                "People/Bob.md",
                "Projects/Design.md",
                "Projects/Notes.md",
                "Rooms/B12.md",
            ]
        );

        let home = notes.iter().find(|n| n.path == "Home.md").unwrap();
        assert_eq!(home.name, "Home");
        assert_eq!(home.folder, "");
        assert!(home.body.starts_with("# Home\n"), "{:?}", home.body);
        assert_eq!(
            home.aliases,
            Some(vec!["Start".to_string(), "Landing".to_string()])
        );
        let fm: serde_json::Value =
            serde_json::from_str(home.frontmatter_json.as_deref().unwrap()).unwrap();
        assert_eq!(fm["title"], "Home");
        assert_eq!(fm["related"], "[[Projects/Design]]");
        assert_eq!(home.frontmatter_error, None);
        let meta = std::fs::metadata(fixture_root().join("Home.md")).unwrap();
        assert_eq!(home.size_bytes, meta.len() as i64);
        assert!(home.modified_ms > 0);

        let design = notes
            .iter()
            .find(|n| n.path == "Projects/Design.md")
            .unwrap();
        assert_eq!(design.name, "Design");
        assert_eq!(design.folder, "Projects");
        assert_eq!(design.aliases, None);

        let bad = notes
            .iter()
            .find(|n| n.path == "Bad Frontmatter.md")
            .unwrap();
        assert_eq!(bad.frontmatter_json, None);
        assert!(bad.frontmatter_error.is_some());
        assert!(bad.body.starts_with("Body survives"));
        assert_eq!(bad.aliases, None);

        let plain = notes
            .iter()
            .find(|n| n.path == "No Frontmatter.md")
            .unwrap();
        assert_eq!(plain.frontmatter_json, None);
        assert_eq!(plain.frontmatter_error, None);
        assert!(plain.body.contains("\n---\n"));

        let empty = notes.iter().find(|n| n.path == "Rooms/B12.md").unwrap();
        assert_eq!(empty.size_bytes, 0);
        assert_eq!(empty.body, "");
        assert!(empty.links.is_empty() && empty.tags.is_empty());

        let meeting = notes.iter().find(|n| n.path == "Meeting.md").unwrap();
        assert_eq!(meeting.aliases, Some(vec!["Standup".to_string()]));
    }

    #[tokio::test]
    async fn fixture_links_match_the_oracle() {
        let notes = scan(&fixture_root(), defaults()).await.unwrap();
        let by_path = |p: &str| notes.iter().find(|n| n.path == p).unwrap();
        assert_eq!(notes.iter().map(|n| n.links.len()).sum::<usize>(), 27);

        let home = by_path("Home.md");
        assert_eq!(
            summarize(&home.links),
            vec![
                (
                    Some("Projects/Design.md"),
                    "Projects/Design",
                    "wikilink",
                    "exact",
                    "frontmatter",
                    None
                ),
                (
                    Some("Projects/Design.md"),
                    "Projects/Design",
                    "wikilink",
                    "exact",
                    "body",
                    Some(9)
                ),
                (
                    Some("Projects/Design.md"),
                    "Design",
                    "wikilink",
                    "name",
                    "body",
                    Some(9)
                ),
                (
                    None,
                    "https://skardi.ai",
                    "external",
                    "external",
                    "body",
                    Some(16)
                ),
                (
                    None,
                    "https://example.com",
                    "external",
                    "external",
                    "body",
                    Some(16)
                ),
                (
                    None,
                    "mailto:hello@example.com",
                    "external",
                    "external",
                    "body",
                    Some(16)
                ),
                (
                    Some("attachments/diagram.png"),
                    "diagram.png",
                    "embed",
                    "name",
                    "body",
                    Some(17)
                ),
                (
                    Some("attachments/diagram.png"),
                    "attachments/diagram.png",
                    "embed",
                    "exact",
                    "body",
                    Some(17)
                ),
                (
                    Some("Meeting.md"),
                    "Meeting",
                    "wikilink",
                    "name",
                    "body",
                    Some(18)
                ),
                (Some("Home.md"), "", "wikilink", "exact", "body", Some(18)),
                (None, "Notes", "wikilink", "ambiguous", "body", Some(19)),
                (None, "Nowhere", "wikilink", "missing", "body", Some(19)),
                (
                    None,
                    "missing/thing.md",
                    "markdown",
                    "missing",
                    "body",
                    Some(19)
                ),
            ]
        );
        assert_eq!(home.links[2].heading.as_deref(), Some("Goals"));
        assert_eq!(home.links[2].display_text.as_deref(), Some("the design"));
        assert_eq!(home.links[3].display_text.as_deref(), Some("Skardi"));
        assert_eq!(home.links[4].display_text, None);
        assert_eq!(home.links[7].display_text.as_deref(), Some("alt"));
        assert_eq!(home.links[8].block_id.as_deref(), Some("abc123"));
        assert_eq!(home.links[9].heading.as_deref(), Some("Goals"));
        assert_eq!(home.links[12].display_text.as_deref(), Some("x"));

        assert_eq!(
            summarize(&by_path("Meeting.md").links),
            vec![
                (
                    Some("People/Alice.md"),
                    "People/Alice",
                    "wikilink",
                    "exact",
                    "frontmatter",
                    None
                ),
                (
                    Some("People/Bob.md"),
                    "People/Bob",
                    "wikilink",
                    "exact",
                    "frontmatter",
                    None
                ),
                (
                    Some("Rooms/B12.md"),
                    "Rooms/B12",
                    "wikilink",
                    "exact",
                    "frontmatter",
                    None
                ),
            ]
        );
        assert_eq!(
            by_path("Meeting.md").links[1].display_text.as_deref(),
            Some("Bob")
        );

        assert_eq!(
            summarize(&by_path("Projects/Design.md").links),
            vec![
                (Some("Home.md"), "Home", "wikilink", "name", "body", Some(9)),
                (
                    Some("Projects/Notes.md"),
                    "Notes.md",
                    "markdown",
                    "exact",
                    "body",
                    Some(9)
                ),
                (
                    Some("Meeting.md"),
                    "../Meeting.md",
                    "markdown",
                    "exact",
                    "body",
                    Some(9)
                ),
                (
                    Some("Projects/Notes.md"),
                    "./Notes",
                    "wikilink",
                    "exact",
                    "body",
                    Some(10)
                ),
                (
                    Some("Home.md"),
                    "../Home",
                    "wikilink",
                    "exact",
                    "body",
                    Some(10)
                ),
                (
                    Some("Home.md"),
                    "Home.md",
                    "markdown",
                    "name",
                    "body",
                    Some(10)
                ),
            ]
        );

        assert_eq!(
            summarize(&by_path("Projects/Notes.md").links),
            vec![
                (Some("Home.md"), "Home", "wikilink", "name", "body", Some(1)),
                (None, "Start", "wikilink", "missing", "body", Some(1)),
            ]
        );
        // Bob links Alice only: `Rooms/B12.md` must stay reachable solely
        // through Meeting.md's frontmatter (the spec's frontmatter-only
        // inbound note), so the graph checks below depend on that extraction.
        assert_eq!(
            summarize(&by_path("People/Bob.md").links),
            vec![(
                Some("People/Alice.md"),
                "Alice",
                "wikilink",
                "name",
                "body",
                Some(1)
            )]
        );
        assert_eq!(
            summarize(&by_path("CJK.md").links),
            vec![(Some("Home.md"), "Home", "wikilink", "name", "body", Some(1))]
        );
        assert!(by_path("Large.md").links.is_empty());

        // Graph facts used by docs/obsidian.md's example queries.
        let in_degree_home = notes
            .iter()
            .flat_map(|n| n.links.iter())
            .filter(|l| l.to_path.as_deref() == Some("Home.md"))
            .count();
        assert_eq!(in_degree_home, 6);
        let in_degree_b12 = notes
            .iter()
            .flat_map(|n| n.links.iter())
            .filter(|l| l.to_path.as_deref() == Some("Rooms/B12.md"))
            .map(|l| l.source)
            .collect::<Vec<_>>();
        assert_eq!(in_degree_b12, vec![Source::Frontmatter]);
        let linked: std::collections::HashSet<&str> = notes
            .iter()
            .flat_map(|n| n.links.iter())
            .filter_map(|l| l.to_path.as_deref())
            .collect();
        let orphans: Vec<&str> = notes
            .iter()
            .map(|n| n.path.as_str())
            .filter(|p| !linked.contains(p))
            .collect();
        assert_eq!(
            orphans,
            vec![
                "Archive/Notes.md",
                "Bad Frontmatter.md",
                "CJK.md",
                "Large.md",
                "No Frontmatter.md"
            ]
        );
    }

    #[tokio::test]
    async fn fixture_tags_match_the_oracle() {
        let notes = scan(&fixture_root(), defaults()).await.unwrap();
        let mut rows: Vec<(&str, &str, &str)> = notes
            .iter()
            .flat_map(|n| {
                n.tags
                    .iter()
                    .map(move |t| (n.path.as_str(), t.tag.as_str(), t.source.as_str()))
            })
            .collect();
        // Notes are already path-ordered and tags (tag, source)-ordered per
        // note, so this sort must be a no-op.
        let unsorted = rows.clone();
        rows.sort();
        assert_eq!(rows, unsorted);
        assert_eq!(
            rows,
            vec![
                ("Bad Frontmatter.md", "bad", "body"),
                ("CJK.md", "标签", "body"),
                ("Home.md", "index", "frontmatter"),
                ("Home.md", "project/skardi", "body"),
                ("Home.md", "project/skardi", "frontmatter"),
                ("Home.md", "y2026", "body"),
                ("Meeting.md", "meeting", "body"),
                ("No Frontmatter.md", "plain", "body"),
                ("Projects/Design.md", "design", "frontmatter"),
                ("Projects/Design.md", "draft", "frontmatter"),
            ]
        );
    }

    #[tokio::test]
    async fn empty_vault_yields_no_notes_and_no_error() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".obsidian")).unwrap();
        std::fs::write(dir.path().join(".obsidian/app.json"), b"{}").unwrap();
        std::fs::create_dir_all(dir.path().join("attachments")).unwrap();
        std::fs::write(dir.path().join("attachments/x.png"), b"\x89PNG").unwrap();
        let notes = scan(dir.path(), defaults()).await.unwrap();
        assert!(notes.is_empty());
    }

    #[tokio::test]
    async fn missing_root_is_an_error_naming_it() {
        let err = scan(Path::new("/no/such/vault/root"), defaults())
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("/no/such/vault/root"), "{msg}");
    }

    #[tokio::test]
    async fn size_cap_skips_large_notes_before_reading() {
        let large = std::fs::metadata(fixture_root().join("Large.md"))
            .unwrap()
            .len();
        assert!(large > 2048, "fixture Large.md must exceed the test cap");
        let opts = ScanOptions::new(vec![".obsidian/**".into(), ".trash/**".into()], 2048).unwrap();
        let notes = scan(&fixture_root(), opts).await.unwrap();
        assert_eq!(notes.len(), 11);
        assert!(notes.iter().all(|n| n.path != "Large.md"));
    }

    #[tokio::test]
    async fn only_note_over_the_cap_is_empty_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("Big.md"), vec![b'x'; 4096]).unwrap();
        let opts = ScanOptions::new(vec![], 2048).unwrap();
        let notes = scan(dir.path(), opts).await.unwrap();
        assert!(notes.is_empty());
    }

    /// Spec acceptance: an over-cap note is never *read*, proven with a `get`
    /// that would fail if reached. Were the read attempted it would be the
    /// only attempt, fail, and trip the wholesale-failure guard; the skip
    /// happens on the listing's size, so the scan is empty and `Ok`.
    #[cfg(unix)]
    #[tokio::test]
    async fn oversized_note_is_never_opened() {
        let dir = tempfile::tempdir().unwrap();
        let big = dir.path().join("Big.md");
        std::fs::write(&big, vec![b'x'; 4096]).unwrap();
        if !make_unreadable(&big) {
            eprintln!("skipping: running as root, chmod 000 does not deny reads");
            restore_readable(dir.path());
            return;
        }
        let opts = ScanOptions::new(vec![], 2048).unwrap();
        let notes = scan(dir.path(), opts).await.unwrap();
        assert!(notes.is_empty());
        restore_readable(dir.path());
    }

    #[tokio::test]
    async fn custom_exclude_globs_replace_defaults_and_shrink_the_index() {
        let opts = ScanOptions::new(vec!["People/**".into()], u64::MAX).unwrap();
        let notes = scan(&fixture_root(), opts).await.unwrap();
        let paths: Vec<&str> = notes.iter().map(|n| n.path.as_str()).collect();
        // `.trash/Deleted.md` is now listed (default gone); `People/*` is not.
        assert!(paths.contains(&".trash/Deleted.md"));
        assert!(!paths.iter().any(|p| p.starts_with("People/")));
        assert_eq!(notes.len(), 11);
        // Excluded files are not in the resolution index either.
        let meeting = notes.iter().find(|n| n.path == "Meeting.md").unwrap();
        assert_eq!(meeting.links[0].resolution, Resolution::Missing);
        assert_eq!(meeting.links[2].resolution, Resolution::Exact); // Rooms/B12 still there
    }

    #[cfg(unix)]
    fn make_unreadable(path: &Path) -> bool {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o000)).unwrap();
        // Running as root ignores mode bits; report whether the file really
        // became unreadable so callers can skip.
        std::fs::read(path).is_err()
    }

    #[cfg(unix)]
    fn restore_readable(root: &Path) {
        use std::os::unix::fs::PermissionsExt;
        for entry in walkdir(root) {
            let _ = std::fs::set_permissions(&entry, std::fs::Permissions::from_mode(0o644));
        }
    }

    #[cfg(unix)]
    fn walkdir(root: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).unwrap().flatten() {
                let p = entry.path();
                if p.is_dir() {
                    stack.push(p);
                } else {
                    out.push(p);
                }
            }
        }
        out
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn every_read_failing_is_an_error_naming_root_and_first_path() {
        let dir = tempfile::tempdir().unwrap();
        copy_dir(&fixture_root(), dir.path());
        let mut really_unreadable = true;
        for p in walkdir(dir.path()) {
            if p.extension().is_some_and(|e| e == "md") {
                really_unreadable &= make_unreadable(&p);
            }
        }
        if !really_unreadable {
            eprintln!("skipping: running as root, chmod 000 does not deny reads");
            restore_readable(dir.path());
            return;
        }
        let err = scan(dir.path(), defaults()).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("every note read under"), "{msg}");
        assert!(
            msg.contains(&dir.path().to_string_lossy().into_owned()),
            "{msg}"
        );
        assert!(
            msg.contains("Archive/Notes.md"),
            "first failure named: {msg}"
        );
        assert!(msg.contains("12 attempted"), "{msg}");
        restore_readable(dir.path());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn one_unreadable_note_is_skipped_with_the_rest_returned() {
        let dir = tempfile::tempdir().unwrap();
        copy_dir(&fixture_root(), dir.path());
        if !make_unreadable(&dir.path().join("CJK.md")) {
            eprintln!("skipping: running as root, chmod 000 does not deny reads");
            restore_readable(dir.path());
            return;
        }
        let notes = scan(dir.path(), defaults()).await.unwrap();
        assert_eq!(notes.len(), 11);
        assert!(notes.iter().all(|n| n.path != "CJK.md"));
        restore_readable(dir.path());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinked_note_is_never_read() {
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret.md"), b"# Secret [[Home]]").unwrap();
        let dir = tempfile::tempdir().unwrap();
        copy_dir(&fixture_root(), dir.path());
        std::fs::remove_file(dir.path().join("CJK.md")).unwrap();
        std::os::unix::fs::symlink(outside.path().join("secret.md"), dir.path().join("CJK.md"))
            .unwrap();
        let notes = scan(dir.path(), defaults()).await.unwrap();
        assert_eq!(notes.len(), 11);
        assert!(notes.iter().all(|n| n.path != "CJK.md"));
        assert!(notes.iter().all(|n| !n.body.contains("Secret")));
    }

    #[test]
    fn parse_note_is_pure() {
        let index = Index::build(&["Projects/Design.md", "Home.md"]);
        let text = "---\naliases: X\ntags: t\n---\nSee [[Home]] #a #a\n";
        let note = parse_note(
            "Projects/Design.md",
            42,
            DateTime::<Utc>::from(std::time::UNIX_EPOCH),
            text,
            &index,
        );
        assert_eq!(note.name, "Design");
        assert_eq!(note.folder, "Projects");
        assert_eq!(note.size_bytes, 42);
        assert_eq!(note.modified_ms, 0);
        assert_eq!(note.body, "See [[Home]] #a #a\n");
        assert_eq!(note.aliases, Some(vec!["X".to_string()]));
        assert_eq!(note.links.len(), 1);
        assert_eq!(note.links[0].line, Some(5));
        assert_eq!(note.links[0].resolution, Resolution::Name);
        // Body duplicates collapse; (tag, source) order.
        let tags: Vec<(&str, &str)> = note
            .tags
            .iter()
            .map(|t| (t.tag.as_str(), t.source.as_str()))
            .collect();
        assert_eq!(tags, vec![("a", "body"), ("t", "frontmatter")]);
    }
}
