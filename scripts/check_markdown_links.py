#!/usr/bin/env python3
"""Check relative links (and their #fragments) in tracked Markdown files.

Catches the failure class CI previously had no eyes on: a section or file
is removed and inbound `[text](path.md#anchor)` links elsewhere in the repo
silently start landing at the top of a page — or nowhere.

Scope is deliberately narrow so the check stays deterministic:
  - only git-tracked *.md files, minus docs/superpowers/ (historical
    plans/specs that describe past states of the tree) and the obsidian
    fixture vault (test data whose broken links are the fixture);
  - only relative targets — http(s)/mailto links are never fetched;
  - fragments are resolved against GitHub's heading-slug rules, plus
    explicit <a name=...>/<a id=...>/id="..." HTML anchors.

Exit code 1 with one line per broken link; 0 when everything resolves.
"""

import posixpath
import re
import subprocess
import sys
from pathlib import Path

EXCLUDE_PREFIXES = (
    "docs/superpowers/",
    # The obsidian source's fixture vault: its notes deliberately link to
    # missing files so `links.resolution = 'missing'` has rows to assert on.
    "crates/skardi/src/sources/providers/obsidian/fixtures/",
)

FENCE_RE = re.compile(r"^(```|~~~).*?^\1\s*$", re.M | re.S)
INLINE_CODE_RE = re.compile(r"`[^`\n]*`")
# [text](target) and ![alt](target); target may carry an optional #fragment.
MD_LINK_RE = re.compile(r"!?\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
# Relative targets in raw HTML: <img src>, <picture><source srcset>, <a href>.
HTML_REF_RE = re.compile(r"(?:href|src|srcset)=\"([^\"]+)\"")
HEADING_RE = re.compile(r"^#{1,6}\s+(.*?)\s*#*\s*$", re.M)
HTML_ANCHOR_RE = re.compile(r"(?:name|id)=\"([^\"]+)\"")


def tracked_files():
    """All git-tracked paths. Links are validated against this set, not the
    working tree: a target that exists on disk but is untracked (gitignored,
    generated) is still broken for everyone cloning the repo — exactly the
    local-pass/CI-fail divergence this check must not have."""
    out = subprocess.run(
        ["git", "ls-files"], capture_output=True, text=True, check=True
    ).stdout.splitlines()
    return set(out)


def markdown_files(tracked):
    return [
        Path(p)
        for p in tracked
        if p.endswith(".md") and not any(p.startswith(e) for e in EXCLUDE_PREFIXES)
    ]


def exists_in_repo(relpath, tracked):
    """True if `relpath` is a tracked file, or a directory containing one."""
    if relpath in tracked:
        return True
    prefix = relpath.rstrip("/") + "/"
    return any(t.startswith(prefix) for t in tracked)


def strip_code(text):
    return INLINE_CODE_RE.sub("", FENCE_RE.sub("", text))


def github_slug(heading):
    """GitHub's anchor slug: markdown/HTML stripped, lowercased, punctuation
    dropped, spaces hyphenated. Literal underscores are kept (only `*`/`` ` ``
    formatting markers are stripped). Duplicate-heading -N suffixes are
    handled by the caller collecting all slugs."""
    text = re.sub(r"[*~`]|\[|\]\([^)]*\)|<[^>]+>", "", heading)
    text = text.strip().lower()
    text = re.sub(r"[^\w\- ]", "", text, flags=re.UNICODE)
    return text.replace(" ", "-")


def clean_fragment(fragment):
    """Normalize a slug/fragment for comparison: emoji and variation
    selectors slug inconsistently across GitHub versions, so both sides are
    reduced to word characters and hyphens with edge hyphens trimmed."""
    return re.sub(r"[^\w-]", "", fragment.lower(), flags=re.UNICODE).strip("-")


def anchors_of(path, cache={}):
    if path not in cache:
        text = path.read_text(encoding="utf-8", errors="replace")
        slugs = set()
        counts = {}
        for h in HEADING_RE.findall(FENCE_RE.sub("", text)):
            slug = github_slug(h)
            n = counts.get(slug, 0)
            counts[slug] = n + 1
            slugs.add(slug if n == 0 else f"{slug}-{n}")
        slugs.update(HTML_ANCHOR_RE.findall(text))
        cache[path] = slugs
    return cache[path]


def targets_in(text):
    stripped = strip_code(text)
    for m in MD_LINK_RE.finditer(stripped):
        yield m.group(1)
    for m in HTML_REF_RE.finditer(stripped):
        yield m.group(1)


def main():
    tracked = tracked_files()
    errors = []
    for md in markdown_files(tracked):
        text = md.read_text(encoding="utf-8", errors="replace")
        for raw in targets_in(text):
            if re.match(r"[a-z][a-z0-9+.-]*:", raw):  # http:, https:, mailto:, …
                continue
            target, _, fragment = raw.partition("#")
            if target:
                rel = posixpath.normpath(posixpath.join(md.parent.as_posix(), target))
                if rel.startswith("..") or not exists_in_repo(rel, tracked):
                    errors.append(f"{md}: broken link -> {raw}")
                    continue
                resolved = Path(rel)
            else:
                resolved = md
            if fragment and resolved.suffix == ".md" and resolved.is_file():
                known = {clean_fragment(a) for a in anchors_of(resolved)}
                if clean_fragment(fragment) not in known:
                    errors.append(f"{md}: missing anchor -> {raw}")

    for e in errors:
        print(e)
    if errors:
        print(f"\n{len(errors)} broken markdown link(s).", file=sys.stderr)
        return 1
    print("All relative markdown links resolve.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
