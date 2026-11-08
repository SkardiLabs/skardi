//! Link resolution: turn a `RawLink` plus the linking note's path into
//! `(to_path, kind, resolution)` using an index over every listed file.
//! Pure; the two rule tables in the spec (§Link Resolution) are implemented
//! one arm each below, with two recorded deviations, both of them Obsidian's
//! own behaviour: a bare dotted wikilink name (`[[Note v2.1]]`) that is not a
//! root-level file falls through to the name lookup instead of `missing`, and
//! a path-ish wikilink that matches nothing from the vault root falls back to
//! a unique path suffix (`[[Projects/Notes]]` -> `Work/Projects/Notes.md`).
//! Markdown links keep the stricter path rules; only wikilinks do this.

use std::collections::HashMap;

use super::markdown::{LinkSyntax, RawLink, has_url_scheme};

/// `links.resolution`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolution {
    Exact,
    Name,
    Ambiguous,
    Missing,
    External,
}

impl Resolution {
    pub fn as_str(self) -> &'static str {
        match self {
            Resolution::Exact => "exact",
            Resolution::Name => "name",
            Resolution::Ambiguous => "ambiguous",
            Resolution::Missing => "missing",
            Resolution::External => "external",
        }
    }
}

/// `links.kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkKind {
    Wikilink,
    Embed,
    Markdown,
    External,
}

impl LinkKind {
    pub fn as_str(self) -> &'static str {
        match self {
            LinkKind::Wikilink => "wikilink",
            LinkKind::Embed => "embed",
            LinkKind::Markdown => "markdown",
            LinkKind::External => "external",
        }
    }
}

/// The outcome of resolving one link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    /// `None` for `ambiguous`, `missing`, `external`.
    pub to_path: Option<String>,
    pub kind: LinkKind,
    pub resolution: Resolution,
}

/// Case-insensitive index over every listed vault file (notes and
/// attachments): by full relative path, and by name — the stem for `.md`
/// files, the full file name for anything else.
#[derive(Debug, Default)]
pub struct Index {
    by_path: HashMap<String, String>,
    by_name: HashMap<String, Vec<String>>,
    /// Multi-segment tails of every path, verbatim, for the suffix rule in
    /// [`Index::resolve_wikilink`].
    by_suffix: HashMap<String, Vec<String>>,
    /// The same tails for `.md` files with the extension dropped, kept apart
    /// from [`Index::by_suffix`] so a note and an extensionless file of the
    /// same name are two candidates under two keys rather than one
    /// `ambiguous` under one. Searched second, exactly as [`Index::lookup_exact`]
    /// tries the path before the path plus `.md`.
    by_suffix_stem: HashMap<String, Vec<String>>,
}

impl Index {
    pub fn build<S: AsRef<str>>(paths: &[S]) -> Self {
        let mut index = Index::default();
        for path in paths {
            let path = path.as_ref();
            if let Some(previous) = index.by_path.insert(path.to_lowercase(), path.to_string()) {
                // Case-sensitive filesystems can hold both; the listing is
                // sorted, so "last wins" is deterministic but still arbitrary.
                tracing::debug!(
                    kept = %path,
                    dropped = %previous,
                    "obsidian: two files differ only by case; the later one resolves"
                );
            }
            let file_name = path.rsplit('/').next().unwrap_or(path);
            let name = strip_md(file_name).unwrap_or(file_name);
            index
                .by_name
                .entry(name.to_lowercase())
                .or_default()
                .push(path.to_string());
            for (suffix, stem) in path_suffixes(path) {
                index
                    .by_suffix
                    .entry(suffix)
                    .or_default()
                    .push(path.to_string());
                if let Some(stem) = stem {
                    index
                        .by_suffix_stem
                        .entry(stem)
                        .or_default()
                        .push(path.to_string());
                }
            }
        }
        for candidates in index.by_name.values_mut() {
            candidates.sort();
        }
        for candidates in index.by_suffix.values_mut() {
            candidates.sort();
        }
        for candidates in index.by_suffix_stem.values_mut() {
            candidates.sort();
        }
        index
    }

    pub fn resolve(&self, from_path: &str, link: &RawLink) -> Resolved {
        if has_url_scheme(&link.target) {
            return Resolved {
                to_path: None,
                kind: LinkKind::External,
                resolution: Resolution::External,
            };
        }
        let kind = match (link.syntax, link.embed) {
            (_, true) => LinkKind::Embed,
            (LinkSyntax::Wikilink, false) => LinkKind::Wikilink,
            (LinkSyntax::Markdown | LinkSyntax::Autolink, false) => LinkKind::Markdown,
        };
        let target = link.target.trim();
        if target.is_empty() {
            return Resolved {
                to_path: Some(from_path.to_string()),
                kind,
                resolution: Resolution::Exact,
            };
        }
        let (to_path, resolution) = match link.syntax {
            LinkSyntax::Wikilink => self.resolve_wikilink(from_path, target),
            LinkSyntax::Markdown | LinkSyntax::Autolink => self.resolve_markdown(from_path, target),
        };
        Resolved {
            to_path,
            kind,
            resolution,
        }
    }

    /// Vault-root relative, except an explicit `./` / `../` prefix.
    fn resolve_wikilink(&self, from_path: &str, target: &str) -> (Option<String>, Resolution) {
        if target.starts_with("./") || target.starts_with("../") {
            return self.exact_or_missing(normalize(folder_of(from_path), target));
        }
        // A leading `/` names the vault root explicitly, so it is a path and
        // only a path: no suffix fallback, same as a Markdown link's `/`.
        if let Some(rooted) = target.strip_prefix('/') {
            return self.exact_or_missing(normalize("", rooted));
        }
        if target.contains('/') {
            let Some(candidate) = normalize("", target) else {
                return (None, Resolution::Missing);
            };
            if let Some(path) = self.lookup_exact(&candidate) {
                return (Some(path), Resolution::Exact);
            }
            // Obsidian also resolves a path-ish wikilink by its tail when
            // nothing sits at that path from the root: in a vault holding
            // `Work/Projects/Notes.md`, `[[Projects/Notes]]` is that note, not
            // a broken link. Same outcome vocabulary as a bare name — one hit
            // is `name`, several are `ambiguous` — because it is the same kind
            // of lookup: by what the link says, not by where it points.
            return self.lookup_suffix(&candidate);
        }
        // Bare name. A dot may be an extension (`Home.md`, `a.png`) or part
        // of a title (`Note v2.1`): try the root-level exact match, then fall
        // through to the name index either way (plan deviation (a)).
        if target.contains('.') {
            if let Some(path) = self.lookup_exact(target) {
                return (Some(path), Resolution::Exact);
            }
        }
        self.lookup_name(target)
    }

    /// A leading `/` is the vault root and nothing else. Otherwise
    /// note-relative first (CommonMark / Obsidian "Relative path to file"),
    /// then the vault root for paths with a `/`, then the name index for
    /// bare names (Obsidian "Shortest path").
    fn resolve_markdown(&self, from_path: &str, target: &str) -> (Option<String>, Resolution) {
        if let Some(rooted) = target.strip_prefix('/') {
            return self.exact_or_missing(normalize("", rooted));
        }
        if let Some(path) =
            normalize(folder_of(from_path), target).and_then(|p| self.lookup_exact(&p))
        {
            return (Some(path), Resolution::Exact);
        }
        if target.contains('/') {
            return self.exact_or_missing(normalize("", target));
        }
        self.lookup_name(target)
    }

    fn exact_or_missing(&self, candidate: Option<String>) -> (Option<String>, Resolution) {
        match candidate.and_then(|p| self.lookup_exact(&p)) {
            Some(path) => (Some(path), Resolution::Exact),
            None => (None, Resolution::Missing),
        }
    }

    /// Full relative path, `.md` optional.
    fn lookup_exact(&self, candidate: &str) -> Option<String> {
        let lower = candidate.to_lowercase();
        if let Some(path) = self.by_path.get(&lower) {
            return Some(path.clone());
        }
        if !lower.ends_with(".md") {
            if let Some(path) = self.by_path.get(&format!("{lower}.md")) {
                return Some(path.clone());
            }
        }
        None
    }

    /// A path-ish target matched against the multi-segment tails of every
    /// listed path (`Projects/Notes` -> `Work/Projects/Notes.md`). Only
    /// reached once the root-relative path missed.
    fn lookup_suffix(&self, candidate: &str) -> (Option<String>, Resolution) {
        let lower = candidate.to_lowercase();
        // The tail as written wins outright: a vault holding both `Sub/Notes`
        // and `Sub/Notes.md` has one answer for `[[Sub/Notes]]`, not an
        // `ambiguous` born of the index conflating the two.
        match self.by_suffix.get(&lower).map(Vec::as_slice) {
            Some([only]) => return (Some(only.clone()), Resolution::Name),
            Some([_, _, ..]) => return (None, Resolution::Ambiguous),
            Some([]) | None => {}
        }
        let stem = strip_md(&lower).unwrap_or(&lower);
        match self.by_suffix_stem.get(stem).map(Vec::as_slice) {
            Some([only]) => (Some(only.clone()), Resolution::Name),
            Some([_, _, ..]) => (None, Resolution::Ambiguous),
            Some([]) | None => (None, Resolution::Missing),
        }
    }

    /// Bare name: stem for notes (`Note` or `Note.md`), full name otherwise.
    fn lookup_name(&self, target: &str) -> (Option<String>, Resolution) {
        let key = strip_md(target).unwrap_or(target).to_lowercase();
        match self.by_name.get(&key).map(Vec::as_slice) {
            Some([only]) => (Some(only.clone()), Resolution::Name),
            Some([_, _, ..]) => (None, Resolution::Ambiguous),
            Some([]) | None => (None, Resolution::Missing),
        }
    }
}

/// Every multi-segment tail of `path`, lowercased, as `(verbatim, stem)` — the
/// keys a path-ish wikilink can match by suffix. `stem` is the tail with a
/// `.md` extension dropped, and `None` for anything else, so the two keys never
/// share a map.
///
/// The one-segment tail is deliberately absent: a bare name is `by_name`'s job,
/// under different rules (stem only, and it is what Obsidian's "Shortest path"
/// setting writes). Size is linear in the number of path segments in the vault,
/// which is why this is built once per scan rather than scanned per link.
fn path_suffixes(path: &str) -> Vec<(String, Option<String>)> {
    let segments: Vec<&str> = path.split('/').collect();
    let mut out = Vec::new();
    for start in (0..segments.len().saturating_sub(1)).rev() {
        let lower = segments[start..].join("/").to_lowercase();
        let stem = strip_md(&lower).map(str::to_string);
        out.push((lower, stem));
    }
    out
}

/// Parent folder of a relative path (`""` at the root).
fn folder_of(path: &str) -> &str {
    path.rsplit_once('/')
        .map(|(folder, _)| folder)
        .unwrap_or("")
}

/// Join `folder` and `target`, collapsing `.`/`..` and empty segments.
/// `None` when `..` climbs above the vault root.
fn normalize(folder: &str, target: &str) -> Option<String> {
    let mut parts: Vec<&str> = if folder.is_empty() {
        Vec::new()
    } else {
        folder.split('/').collect()
    };
    for segment in target.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                parts.pop()?;
            }
            other => parts.push(other),
        }
    }
    Some(parts.join("/"))
}

/// `Home.md` → `Home` (case-insensitive extension). Byte-based so a trailing
/// multi-byte character never lands the slice mid-character.
fn strip_md(name: &str) -> Option<&str> {
    let bytes = name.as_bytes();
    if bytes.len() > 3 && bytes[bytes.len() - 3..].eq_ignore_ascii_case(b".md") {
        Some(&name[..name.len() - 3])
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PATHS: &[&str] = &[
        "Home.md",
        "Meeting.md",
        "Notes.md",
        "Projects/Design.md",
        "Projects/Notes.md",
        "Archive/Notes.md",
        "attachments/diagram.png",
        "People/Alice.md",
        "foo#bar.md",
        "Note v2.1.md",
    ];

    fn idx() -> Index {
        Index::build(PATHS)
    }
    fn raw(syntax: LinkSyntax, embed: bool, target: &str) -> RawLink {
        RawLink {
            syntax,
            embed,
            target: target.to_string(),
            heading: None,
            block_id: None,
            display_text: None,
            line: None,
        }
    }
    fn wiki(target: &str) -> RawLink {
        raw(LinkSyntax::Wikilink, false, target)
    }
    fn md(target: &str) -> RawLink {
        raw(LinkSyntax::Markdown, false, target)
    }
    fn ok(path: &str, resolution: Resolution) -> (Option<String>, Resolution) {
        (Some(path.to_string()), resolution)
    }
    fn r(from: &str, link: &RawLink) -> (Option<String>, Resolution) {
        let res = idx().resolve(from, link);
        (res.to_path, res.resolution)
    }

    #[test]
    fn wikilink_rows() {
        assert_eq!(r("Home.md", &wiki("")), ok("Home.md", Resolution::Exact)); // [[#Heading]]
        assert_eq!(
            r("Projects/Design.md", &wiki("./Notes")),
            ok("Projects/Notes.md", Resolution::Exact)
        );
        assert_eq!(
            r("Projects/Design.md", &wiki("../Home")),
            ok("Home.md", Resolution::Exact)
        );
        assert_eq!(
            r("Projects/Design.md", &wiki("../../Home")),
            (None, Resolution::Missing)
        );
        assert_eq!(
            r("Projects/Design.md", &wiki("./Nope")),
            (None, Resolution::Missing)
        );
        assert_eq!(
            r("Home.md", &wiki("Projects/Design")),
            ok("Projects/Design.md", Resolution::Exact)
        );
        assert_eq!(
            r("Home.md", &wiki("projects/DESIGN.MD")),
            ok("Projects/Design.md", Resolution::Exact)
        );
        assert_eq!(
            r("Home.md", &wiki("Projects/Nope")),
            (None, Resolution::Missing)
        );
        assert_eq!(
            r("Home.md", &wiki("Design")),
            ok("Projects/Design.md", Resolution::Name)
        );
        assert_eq!(
            r("Home.md", &wiki("design.MD")),
            ok("Projects/Design.md", Resolution::Name)
        );
        // A bare root-level note is `name` (spec table); with its extension
        // written it is a vault path, hence `exact`.
        assert_eq!(
            r("Meeting.md", &wiki("Home")),
            ok("Home.md", Resolution::Name)
        );
        assert_eq!(
            r("Meeting.md", &wiki("Home.md")),
            ok("Home.md", Resolution::Exact)
        );
        assert_eq!(r("Home.md", &wiki("Notes")), (None, Resolution::Ambiguous));
        assert_eq!(r("Home.md", &wiki("Nowhere")), (None, Resolution::Missing));
        // Aliases never resolve.
        assert_eq!(r("Home.md", &wiki("Start")), (None, Resolution::Missing));
        // Attachments match by full file name.
        assert_eq!(
            r("Home.md", &wiki("diagram.png")),
            ok("attachments/diagram.png", Resolution::Name)
        );
        // A dotted title that is a root-level file is exact; elsewhere it
        // falls through to the name index (plan deviation (a)).
        assert_eq!(
            r("Home.md", &wiki("Note v2.1")),
            ok("Note v2.1.md", Resolution::Exact)
        );
        let nested = Index::build(&["Sub/Note v2.1.md"]);
        let res = nested.resolve("Home.md", &wiki("Note v2.1"));
        assert_eq!(
            (res.to_path, res.resolution),
            ok("Sub/Note v2.1.md", Resolution::Name)
        );
        // A colon with a space after it is not a URL scheme.
        assert_eq!(
            r("Home.md", &wiki("Note: subtitle")),
            (None, Resolution::Missing)
        );
    }

    /// Obsidian resolves a path-ish wikilink by its tail when nothing sits at
    /// that path from the vault root, so `[[Projects/Notes]]` finds the note
    /// filed under `Work/`. Root-relative still wins where it exists, and two
    /// candidate tails are `ambiguous`, exactly as two candidate names are.
    #[test]
    fn a_path_ish_wikilink_falls_back_to_a_unique_suffix() {
        let nested = Index::build(&["Work/Projects/Notes.md", "Home.md"]);
        let at = |i: &Index, target: &str| {
            let res = i.resolve("Home.md", &wiki(target));
            (res.to_path, res.resolution)
        };
        assert_eq!(
            at(&nested, "Projects/Notes"),
            ok("Work/Projects/Notes.md", Resolution::Name)
        );
        // Case-insensitive, and the written extension changes nothing.
        assert_eq!(
            at(&nested, "projects/NOTES.md"),
            ok("Work/Projects/Notes.md", Resolution::Name)
        );
        // The full path is still exact, not a suffix hit.
        assert_eq!(
            at(&nested, "Work/Projects/Notes"),
            ok("Work/Projects/Notes.md", Resolution::Exact)
        );
        // A tail nothing ends with stays missing.
        assert_eq!(at(&nested, "Nope/Notes"), (None, Resolution::Missing));

        // A note at that path from the root wins over a tail elsewhere.
        let both = Index::build(&["Projects/Notes.md", "Work/Projects/Notes.md"]);
        assert_eq!(
            at(&both, "Projects/Notes"),
            ok("Projects/Notes.md", Resolution::Exact)
        );

        // Two tails, no winner.
        let twice = Index::build(&["A/Projects/Notes.md", "B/Projects/Notes.md"]);
        assert_eq!(at(&twice, "Projects/Notes"), (None, Resolution::Ambiguous));

        // A leading `/` names the vault root and nothing else: a path that is
        // not there is `missing`, never a suffix hit elsewhere.
        assert_eq!(at(&nested, "/Projects/Notes"), (None, Resolution::Missing));
        assert_eq!(
            at(&nested, "/Work/Projects/Notes"),
            ok("Work/Projects/Notes.md", Resolution::Exact)
        );

        // A note and an extensionless file sharing a tail are two keys, not one
        // `ambiguous` key: the tail as written decides, and `.md` is optional
        // only when nothing matched verbatim.
        let both_kinds = Index::build(&["A/Sub/Notes.md", "A/Sub/Notes"]);
        assert_eq!(
            at(&both_kinds, "Sub/Notes"),
            ok("A/Sub/Notes", Resolution::Name)
        );
        assert_eq!(
            at(&both_kinds, "Sub/Notes.md"),
            ok("A/Sub/Notes.md", Resolution::Name)
        );
    }

    #[test]
    fn markdown_rows() {
        assert_eq!(r("Home.md", &md("")), ok("Home.md", Resolution::Exact)); // [t](#Heading)
        // Sibling first, even though `Notes.md` also exists at the root and in
        // `Archive/` (spec: "even when a root-level other.md also exists").
        assert_eq!(
            r("Projects/Design.md", &md("Notes.md")),
            ok("Projects/Notes.md", Resolution::Exact)
        );
        assert_eq!(
            r("Projects/Design.md", &md("Notes")),
            ok("Projects/Notes.md", Resolution::Exact)
        );
        assert_eq!(
            r("Projects/Design.md", &md("../Meeting.md")),
            ok("Meeting.md", Resolution::Exact)
        );
        assert_eq!(
            r("Projects/Design.md", &md("../../Meeting.md")),
            (None, Resolution::Missing)
        );
        // Vault path second.
        assert_eq!(
            r("Home.md", &md("Projects/Notes.md")),
            ok("Projects/Notes.md", Resolution::Exact)
        );
        assert_eq!(
            r("Projects/Design.md", &md("People/Alice.md")),
            ok("People/Alice.md", Resolution::Exact)
        );
        assert_eq!(
            r("Home.md", &md("missing/thing.md")),
            (None, Resolution::Missing)
        );
        // A leading `/` is root-only: from `Projects/`, `/Notes.md` is the
        // root note, never the sibling, and a missing root path stays missing
        // even when a sibling of that name exists.
        assert_eq!(
            r("Projects/Design.md", &md("/Notes.md")),
            ok("Notes.md", Resolution::Exact)
        );
        assert_eq!(
            r("Projects/Design.md", &md("/Home.md")),
            ok("Home.md", Resolution::Exact)
        );
        assert_eq!(
            r("Projects/Design.md", &md("/Design.md")),
            (None, Resolution::Missing)
        );
        assert_eq!(
            r("Home.md", &md("/Projects/Notes")),
            ok("Projects/Notes.md", Resolution::Exact)
        );
        // Bare name not a sibling: unique → name, repeated → ambiguous.
        assert_eq!(
            r("Projects/Design.md", &md("Home.md")),
            ok("Home.md", Resolution::Name)
        );
        // From the root, `Notes.md` is the root note itself (exact path).
        assert_eq!(
            r("Home.md", &md("Notes.md")),
            ok("Notes.md", Resolution::Exact)
        );
        let dup = Index::build(&["A/Dup.md", "B/Dup.md"]);
        let res = dup.resolve("Home.md", &md("Dup.md"));
        assert_eq!((res.to_path, res.resolution), (None, Resolution::Ambiguous));
        assert_eq!(r("Home.md", &md("nothing.md")), (None, Resolution::Missing));
        // The decoded `%23` file name is looked up literally.
        assert_eq!(
            r("Home.md", &md("foo#bar.md")),
            ok("foo#bar.md", Resolution::Exact)
        );
    }

    #[test]
    fn kinds_and_externals() {
        let i = idx();
        assert_eq!(
            i.resolve("Home.md", &wiki("Design")).kind,
            LinkKind::Wikilink
        );
        assert_eq!(
            i.resolve("Home.md", &raw(LinkSyntax::Wikilink, true, "diagram.png"))
                .kind,
            LinkKind::Embed
        );
        assert_eq!(
            i.resolve("Home.md", &md("Meeting.md")).kind,
            LinkKind::Markdown
        );
        assert_eq!(
            i.resolve(
                "Home.md",
                &raw(LinkSyntax::Markdown, true, "attachments/diagram.png")
            )
            .kind,
            LinkKind::Embed
        );
        for target in [
            "https://example.com",
            "mailto:a@b.c",
            "obsidian://open?vault=x",
        ] {
            for syntax in [
                LinkSyntax::Wikilink,
                LinkSyntax::Markdown,
                LinkSyntax::Autolink,
            ] {
                let res = i.resolve("Home.md", &raw(syntax, false, target));
                assert_eq!(res.to_path, None, "{target}");
                assert_eq!(res.kind, LinkKind::External);
                assert_eq!(res.resolution, Resolution::External);
            }
        }
        // An external image is still `external`, not `embed`.
        assert_eq!(
            i.resolve(
                "Home.md",
                &raw(LinkSyntax::Markdown, true, "https://x/i.png")
            )
            .kind,
            LinkKind::External
        );
    }

    #[test]
    fn as_str_values_match_the_schema_contract() {
        assert_eq!(Resolution::Exact.as_str(), "exact");
        assert_eq!(Resolution::Name.as_str(), "name");
        assert_eq!(Resolution::Ambiguous.as_str(), "ambiguous");
        assert_eq!(Resolution::Missing.as_str(), "missing");
        assert_eq!(Resolution::External.as_str(), "external");
        assert_eq!(LinkKind::Wikilink.as_str(), "wikilink");
        assert_eq!(LinkKind::Embed.as_str(), "embed");
        assert_eq!(LinkKind::Markdown.as_str(), "markdown");
        assert_eq!(LinkKind::External.as_str(), "external");
    }

    #[test]
    fn normalize_collapses_dots_and_refuses_to_climb_out() {
        assert_eq!(
            normalize("Projects", "Notes.md"),
            Some("Projects/Notes.md".into())
        );
        assert_eq!(
            normalize("Projects", "./Notes.md"),
            Some("Projects/Notes.md".into())
        );
        assert_eq!(normalize("Projects", "../Home.md"), Some("Home.md".into()));
        assert_eq!(normalize("A/B", "../../x"), Some("x".into()));
        assert_eq!(normalize("A", "../../x"), None);
        assert_eq!(normalize("", "a//b/./c"), Some("a/b/c".into()));
    }

    #[test]
    fn strip_md_is_case_insensitive_and_utf8_safe() {
        assert_eq!(strip_md("Home.md"), Some("Home"));
        assert_eq!(strip_md("Home.MD"), Some("Home"));
        assert_eq!(strip_md("笔记.md"), Some("笔记"));
        assert_eq!(strip_md("笔记"), None);
        assert_eq!(strip_md("😀😀"), None);
        assert_eq!(strip_md(".md"), None);
        assert_eq!(strip_md("a.png"), None);
    }
}
