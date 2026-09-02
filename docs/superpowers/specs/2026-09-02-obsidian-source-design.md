# Obsidian Vault Source Design

**Status:** Draft for review
**Date:** 2026-09-02
**Branch:** `feature/obsidian-source-design`

## Summary

Skardi will support Obsidian vaults as a first-class, read-only native data source, `type: obsidian`. One configured source binds one vault — a directory of Markdown files on the local filesystem or under an `s3://` prefix — and exposes three fixed tables as a catalog: `notes`, one row per Markdown file with its body and parsed frontmatter; `links`, one row per link found in a note, resolved to its target file with Obsidian's own resolution rules; and `tags`, one row per note × tag from both frontmatter and body.

Every scan re-reads the vault from scratch: list, read, parse, emit one `RecordBatch`. There is no cache, no network, no external process, and no write path. The provider is a format adapter: it understands exactly the three Obsidian-specific structures — YAML frontmatter, `[[wikilinks]]`, and `#tags` — and leaves everything else (search, embedding, graph analytics) to SQL over the three tables.

## Motivation

Obsidian is one of the most widely used personal knowledge bases, and its data is unusually well suited to Skardi: a vault is plain Markdown on disk, so an agent's own working notes, a user's research library, or a team's shared vault are already sitting in a queryable format — missing only the relational view. The value is not "read a Markdown file" (a `SELECT` over `documents` could do that) but the structures Obsidian layers on top: frontmatter properties that behave like columns, tags that behave like labels, and links that form a graph. Once those are tables, "which notes link to this one", "everything tagged `project/skardi` modified this week", and "notes with `status: draft` in frontmatter" are one SQL statement each, joinable with every other Skardi source.

## Research Findings

**Obsidian has no cloud API.** It is a local-first application; the official Sync and Publish services are proprietary with no public read API. The Open Connector gateway — Skardi's path to SaaS providers as source packs — carries no `obsidian` provider as of `oomol-lab/open-connector` `origin/main` on 2026-09-02 (checked against the full provider list; sibling note tools Roam, Trilium, and Memos are present, Obsidian is not). A source pack is therefore not an option without upstream work, and the shape of the data argues against one anyway: the only integration surface is the vault directory itself. This is a native source, sibling to `documents` and `rss`.

**A vault is a directory.** Notes are `.md` files; the note title is the file stem; subfolders are the user's folder tree; attachments live alongside. Obsidian keeps its own configuration under `.obsidian/` and deleted notes under `.trash/` — metadata and garbage, not content.

**The three structures have precise rules.** Frontmatter is a leading YAML block fenced by `---` lines. Tags are `#` followed by letters, digits, `_`, `-`, `/` (Unicode letters allowed), must contain at least one non-digit character, and are not recognized inside code. Links come in three syntaxes — `[[wikilinks]]` with optional `#heading`, `#^block`, and `|display` parts and an `!` prefix for embeds; standard Markdown `[text](target)`; and `<autolinks>` — and a bare wikilink target resolves against the whole vault by file name, then by frontmatter alias, with ambiguity possible when names repeat.

**The existing `documents` source already solves file access.** Its `blob::BlobStore` abstracts "list a prefix, read a blob" over a local directory or an `s3://` prefix under an env-only credential contract. It is compiled behind the `documents` Cargo feature today, alongside the PDF tooling it was written for.

## Goals

- Make a vault queryable as ordinary Arrow-backed DataFusion tables — `notes`, `links`, `tags` — with zero external processes and zero configuration beyond the path.
- Expose the three Obsidian-specific structures relationally: frontmatter as JSON (plus first-class `aliases`), tags as rows, links as resolved graph edges.
- Resolve links the way Obsidian does, and report honestly when it cannot: ambiguity and dangling links are distinguishable in the result, never guessed.
- Serve the current state of the vault on every query, with deterministic row order and no cache to invalidate.
- Isolate per-file faults: a malformed frontmatter block or an unreadable file degrades that row, never the scan.
- Reuse the `documents` file-access layer so local and `s3://` vaults share one code path and one credential contract.

## Non-goals

- A write path. The source registers strictly read-only; `access_mode: read_write` is rejected at registration. Skardi never modifies a vault.
- Honoring `.obsidian/app.json`. Link resolution uses Obsidian's defaults; per-vault settings (attachment folder, link format preference) are not read. Recorded as a future extension.
- A cache or incremental index. Every scan is a full rescan; large-vault or S3 performance work is deferred until a real vault shows the need (see Alternatives Considered).
- S3-specific verification. `s3://` paths work through the shared `BlobStore`, but the first release verifies them only at the URI-parsing layer, not against a live or mocked object store. Local directories are the primary target.
- Bare-URL detection. `https://…` written in running text without link syntax is not extracted; only bracketed links and `<autolinks>` are.
- Rendering Markdown, following embeds transitively, or parsing Dataview/Templater/other plugin syntaxes. The body is served as written.
- Watching the filesystem or pushing changes. Freshness is "whatever is on disk when the query runs".

## Decisions

**Relational model**

- Model one source as one vault. Two vaults are two sources; they cannot link to each other, so nothing is lost.
- Register three fixed tables as a catalog under the conventional `main` schema — `<name>.main.notes`, `<name>.main.links`, `<name>.main.tags` — mirroring the `rss` and `sqlite` catalog convention. `hierarchy_level: catalog` is required.
- Key everything by `path`: the note's path relative to the vault root with forward slashes. `links.from_path`, `links.to_path`, and `tags.path` all join to `notes.path`.
- Serve frontmatter as one `frontmatter_json` column rather than exploding keys into columns. Frontmatter is schemaless per note; a fixed column set would be wrong for every vault. `aliases` alone is lifted to a typed column because link resolution depends on it.

**Freshness and execution**

- Rescan on every query: list the vault, read every `.md`, parse, emit. No state survives a scan.
- Execute as a single partition producing one `RecordBatch` through a `MemoryStream`, as `DocumentsTable` does. Vaults are thousands of files, not millions; parsing them is tens to hundreds of milliseconds locally.
- Scan each table independently. A query joining `notes` and `links` parses the vault twice. This is accepted for the first release in exchange for zero shared state; the cost is documented.
- Support column projection and `LIMIT`; do not push filters down. DataFusion filters the in-memory batch.
- Order rows deterministically: `notes` by `path`; `links` by `(from_path, line, occurrence)`; `tags` by `(path, tag, source)`.

**Parsing**

- Parse Markdown with `pulldown-cmark`. Its event stream identifies code blocks and code spans (where tags and wikilinks are not recognized) and yields standard links and images with their destinations. Wikilinks and tags — Obsidian extensions no CommonMark parser knows — are extracted by pattern from the non-code text ranges.
- Parse frontmatter with `serde_yaml` (already a workspace dependency) and re-serialize to JSON. A frontmatter block that is present but malformed yields `frontmatter_json = NULL` and a populated `frontmatter_error`; the row is kept.
- Decode file bytes as UTF-8 with lossy replacement. No row is dropped for encoding.
- Classify links by syntax and by target: `wikilink`, `embed`, `markdown`, or `external` (any target with a URL scheme, regardless of syntax).
- Resolve internal links in a fixed order — exact path, unique file name, unique alias — and report `ambiguous` or `missing` with `to_path = NULL` rather than choosing.

**Configuration and registration**

- Configure through `path` plus the flat `options` map (`exclude_globs`, `max_file_bytes`), like `documents`. There is nothing here that warrants a typed block.
- Always exclude `.obsidian/**` and `.trash/**` by default.
- Perform no parsing at registration; only check that the root exists and is readable (a directory check locally, one `list` request for `s3://`), and reject non-catalog hierarchy or read-write access.

**Packaging**

- Gate the provider behind an `obsidian` Cargo feature, as `documents` and `rss` are gated. The reason is concrete: `s3://` support needs the optional `object_store` dependency with its AWS backend, which is currently pulled in only by the `documents` feature. `obsidian = ["dep:glob", "dep:object_store", "dep:pulldown-cmark"]`.
- Lift `documents::blob` to a shared module, `sources/providers/blob.rs`, compiled when either feature is enabled (`#[cfg(any(feature = "documents", feature = "obsidian"))]`). This is the one refactor in scope: the file moves, its API does not change, and `documents` keeps working unchanged.

**Security and trust boundary**

- Treat the vault as trusted user data, not hostile input. There is no network egress, no HTML rendering, and no execution of anything found in a note. Parser robustness (malformed YAML, odd bytes, huge files) is a correctness concern, handled per file.
- Do not follow symbolic links when listing. A symlink inside the vault pointing outside it would otherwise let `path: ~/vault` read arbitrary files. Symlinked files and directories are skipped.
- Cap per-file size (`max_file_bytes`, default 16 MiB). A file over the cap is skipped with a warning.
- Inherit the `documents` S3 credential contract: credentials come only from the environment; any credential-shaped key in `options` is rejected at registration. Errors and logs carry paths, never file contents.

## Alternatives Considered

### An Open Connector source pack

The natural first assumption, and the one the repository's tooling steers toward. Rejected on two grounds: the gateway has no `obsidian` provider (verified 2026-09-02), so a pack would first require contributing one upstream; and the only upstream integration surface would be the community Local REST API plugin, which runs on the user's own machine, uses a self-signed certificate, and exposes little more than "list files, get file" — a gateway hop that buys nothing over reading the directory.

### Teaching the `documents` source to read `.md`

Minimal code, but the wrong shape. `documents` serves a fixed `(file, page)` schema designed for PDFs and Office files; a note would arrive as one page of Markdown with no frontmatter, no tags, and no links. That is a Markdown reader, not an Obsidian source, and every Obsidian-specific query would have to re-parse `markdown` in SQL.

### Notes only, with links and tags as JSON array columns

One table instead of three. Rejected because the graph queries — the reason to want an Obsidian source at all — become `UNNEST` gymnastics, and the link resolution (`to_path`, `resolution`) needs a row per link to be expressed at all.

### A `properties` table exploding frontmatter keys

One row per `(path, key, value_json)`. Deferred, not rejected: `frontmatter_json` plus SQL JSON functions covers the need for the first release, and the table can be added without touching the three shipped ones. Recorded as a future extension.

### mtime-keyed incremental cache

Re-parse only files whose `(mtime, size)` changed. Meaningfully faster for large vaults and for S3, but it introduces the class of bug this design most wants to avoid — a stale row served after a file changed — and a second code path to test. Deferred until a real vault demonstrates that a full rescan is too slow.

### Parse once at registration

Fastest queries, but a note edited in Obsidian would not appear until the server reloads. Rejected: the point of a live source is that it is live.

## High-level Architecture

```
context YAML (type: obsidian, path, options)
        │  registration: validate path, hierarchy, access mode; no parsing
        ▼
MemoryCatalogProvider <name>
  └── MemorySchemaProvider "main"
        ├── NotesTable ─┐
        ├── LinksTable ─┼── each scan: VaultScan::run(root, opts) ──► one RecordBatch
        └── TagsTable ──┘              │
                                       ├── blob::BlobStore  (list + get; local dir or s3://)
                                       ├── frontmatter      (split + YAML→JSON)
                                       ├── markdown         (pulldown-cmark events → tags, links)
                                       └── resolve          (name/alias index → to_path, resolution)
```

`VaultScan` is the one unit that knows how to turn a root into parsed notes; the three `TableProvider`s each ask it for a full parse and project their own columns out of the result. The parsing modules are pure functions over strings and are unit-tested without any filesystem.

## Components

### `sources/providers/blob.rs` (moved from `documents/blob.rs`)

Unchanged API: `Loc::parse`, `BlobStore::resolve`, `list`, `get`. Gated on `any(feature = "documents", feature = "obsidian")`. `documents` imports it from the new location.

### `sources/providers/obsidian/mod.rs`

`register_obsidian_tables(session_ctx, name, path, options, access_mode, hierarchy_level)`: validates and registers the catalog. Module doc records every decision above with its rationale, in the style of `rss/mod.rs`.

### `obsidian/config.rs`

`ScanOptions::from_map(options)`: `exclude_globs` (comma-separated, default `.obsidian/**,.trash/**`), `max_file_bytes` (default `16777216`). Unknown keys are rejected at registration so a typo does not silently disable an exclusion.

### `obsidian/frontmatter.rs`

`split(text) -> (Option<&str> yaml, &str body)`: a frontmatter block is a first line of exactly `---`, YAML until the next line of exactly `---` (or `...`), and the body is everything after. `parse(yaml) -> Result<(serde_json::Value), String>`. Helpers lift `aliases` and `tags`/`tag` (list, or comma-/space-separated string; leading `#` stripped) out of the JSON value.

### `obsidian/markdown.rs`

`extract(body) -> Extracted { tags: Vec<(String, line)>, links: Vec<RawLink> }`. Walks `pulldown-cmark` events with byte offsets. Skips `CodeBlock` and `Code` ranges. Emits `RawLink` for `Link`/`Image` events (with `LinkType::Autolink` and scheme-bearing destinations classified external) and scans `Text` ranges for wikilinks and tags. Line numbers are derived from byte offsets.

### `obsidian/resolve.rs`

`Index::build(all_paths, aliases_by_path)` and `Index::resolve(from_path, target) -> (Option<path>, Resolution)`. Pure, tested against the rule table below.

### `obsidian/scan.rs`

`VaultScan::run(store, root, opts) -> Vec<ParsedNote>`: list, filter, read, parse, then resolve links using an index over all listed files. Async because `BlobStore::get` is.

### `obsidian/table.rs`

Three `TableProvider`s sharing one `ExecutionPlan` implementation parameterized by table kind; single partition; projection and limit applied when building the batch.

### `crates/server/src/config.rs`

One `DataSourceType::Obsidian` arm, structured like the `Rss` arm: feature-gated, re-checking nothing the provider checks itself.

## Configuration

```yaml
- name: vault
  type: obsidian
  path: /Users/me/Notes            # vault root: local directory or s3://bucket/prefix
  hierarchy_level: catalog         # required; anything else is rejected
  access_mode: read_only           # read_write is rejected
  description: "Personal knowledge base"
  options:
    exclude_globs: ".obsidian/**,.trash/**"   # default shown; globs relative to root
    max_file_bytes: "16777216"                # default 16 MiB; larger files are skipped
```

Only `path` is required. `exclude_globs` replaces the default when given; a user who wants to exclude `templates/**` as well writes all three. Globs are matched case-insensitively against the forward-slash relative path, as in `documents`.

## Catalog Namespace

```text
<name>.main.notes
<name>.main.links
<name>.main.tags
```

A source named `vault` is queried as `vault.main.notes`. This document uses `vault` throughout.

## Table Schemas

### `notes` — one row per `.md` file

| Column | Arrow type | Nullable | Meaning |
|---|---|---|---|
| `path` | Utf8 | no | Path relative to the vault root, forward slashes: `daily/2026-09-02.md`. Primary identity. |
| `name` | Utf8 | no | File stem (`2026-09-02`) — the title Obsidian displays. |
| `folder` | Utf8 | no | Parent path (`daily`); empty string at the root. |
| `body` | Utf8 | no | Markdown content with the frontmatter block removed. |
| `frontmatter_json` | Utf8 | yes | Frontmatter re-serialized as a JSON object. NULL when absent or malformed. |
| `frontmatter_error` | Utf8 | yes | YAML parse error message when a block is present but malformed; otherwise NULL. |
| `aliases` | List\<Utf8\> | yes | `aliases:` from frontmatter (a scalar becomes a one-element list). NULL when absent. |
| `size_bytes` | Int64 | no | File size in bytes. |
| `modified_at` | Timestamp(ms, UTC) | no | Filesystem mtime, or the object's `LastModified` on S3. |

No `created_at`: filesystem birth time is not portable and S3 has none; users who care record it in frontmatter.

### `links` — one row per link occurrence

| Column | Arrow type | Nullable | Meaning |
|---|---|---|---|
| `from_path` | Utf8 | no | Note containing the link. |
| `to_path` | Utf8 | yes | Resolved target path (a note or an attachment). NULL when `resolution` is `ambiguous`, `missing`, or `external`. |
| `target` | Utf8 | no | The target as written: `Note` in `[[Note#H\|text]]`, `Projects/Design.md`, or the full URL for external links. Empty string for same-note links (`[[#Heading]]`). |
| `kind` | Utf8 | no | `wikilink` (`[[…]]`), `embed` (`![[…]]`, or `![…](…)` with an internal target), `markdown` (`[…](…)` with an internal target), `external` (any URL-scheme target). |
| `display_text` | Utf8 | yes | Text after `\|` in a wikilink, or the bracketed text of a Markdown link. NULL when absent. |
| `heading` | Utf8 | yes | Text after `#` (not `#^`) in the target, percent-decoded for Markdown links. |
| `block_id` | Utf8 | yes | Text after `#^` in the target. |
| `resolution` | Utf8 | no | `exact`, `name`, `alias`, `ambiguous`, `missing`, `external` — see Link Resolution. |
| `line` | Int32 | no | 1-based line of the link's start in the source file. |

### `tags` — one row per distinct (note, tag, source)

| Column | Arrow type | Nullable | Meaning |
|---|---|---|---|
| `path` | Utf8 | no | Note path. |
| `tag` | Utf8 | no | Tag without `#`, case and hierarchy preserved: `project/skardi`. |
| `source` | Utf8 | no | `frontmatter` (from `tags:` or `tag:`) or `body` (inline `#tag`). |

All three schemas carry Arrow metadata `skardi.obsidian.surface_version = "1"`, following the `rss` precedent, so additive evolution has a version to hang on.

## Parsing Rules

### Frontmatter

A block is recognized only when line 1 is exactly `---` and a later line is exactly `---` or `...`. Anything else is body. The YAML is parsed to a `serde_yaml::Value` and converted to JSON; YAML features without a JSON equivalent (non-string keys, anchors) are stringified rather than rejected. Parse failure sets `frontmatter_error` to the parser's message with line/column and leaves `frontmatter_json` NULL; the body still excludes the block.

`aliases`: a string or a list of strings; other shapes are ignored (NULL). `tags` / `tag`: a list of strings, or a string split on commas and whitespace; each entry has a leading `#` removed. Both keys contribute; duplicates collapse.

### Tags in the body

A body tag is `#` immediately followed by one or more of: Unicode letters, digits, `_`, `-`, `/`, where at least one character is not a digit. The `#` must be at the start of a line or preceded by whitespace, so `C#`, `https://x/#anchor`, and `#` inside a word are not tags, and `# Heading` (space after `#`) is not a tag. Text inside fenced code blocks, indented code blocks, and inline code spans is not scanned. Trailing punctuation such as `.` or `,` is not part of the tag.

### Links in the body

Wikilinks are matched in non-code text: optional `!`, `[[`, target, optional `#heading` or `#^block`, optional `|display`, `]]`. Target and parts are trimmed. Markdown links and images come from the parser's `Link` and `Image` events; their destination is percent-decoded and split at the first `#` into path and heading/block. Autolinks (`<https://…>`) are `Link` events with `LinkType::Autolink`.

A destination with a URL scheme (`[a-zA-Z][a-zA-Z0-9+.-]*:` prefix, e.g. `https:`, `mailto:`, `obsidian:`) is `external`: `to_path` NULL, `resolution = 'external'`, `target` = the full destination. Everything else is internal and goes through resolution.

### Link Resolution

Inputs: the target string, the linking note's folder, and an index over every listed file (including attachments) keyed by lowercased file name — the stem for `.md` files (`Note` matches `Note.md`), the full name for others (`a.png` matches `a.png`) — plus an index of lowercased aliases to note paths. All matching is case-insensitive, matching Obsidian's behavior on case-insensitive filesystems.

Applied in order; the first step that applies decides:

| Step | Applies when | Result |
|---|---|---|
| `exact` (self) | target is empty (`[[#Heading]]`) | `to_path = from_path` |
| `exact` (relative) | target starts with `./` or `../` | resolve against the note's folder and normalize; `.md` appended if the result has no extension; must exist, else `missing` |
| `exact` (vault path) | target contains `/` or has a file extension | match the relative path from the vault root, `.md` optional for notes; must exist, else `missing` |
| `name` | exactly one file has that name | that file |
| `alias` | no file has that name and exactly one note declares that alias | that note |
| `ambiguous` | more than one file has that name, or no file does and more than one note declares the alias | `to_path = NULL` |
| `missing` | nothing matched | `to_path = NULL` |

Obsidian resolves an ambiguous bare name to one of the candidates by its own heuristics; this provider does not imitate them. Reporting `ambiguous` is the honest answer and lets the user fix the vault.

## Scan Execution

1. `BlobStore::list(root, recursive = true)`, skipping symlinks. Paths are normalized to forward-slash relative form and filtered by `exclude_globs`. `.md` files are notes; every other path is an attachment candidate for the resolution index.
2. Each note is read with `BlobStore::get`; a blob larger than `max_file_bytes` is skipped with a `tracing::warn!` naming the path. Bytes are decoded lossily as UTF-8.
3. Frontmatter is split and parsed; the body is walked for tags and raw links.
4. The resolution index is built from all listed paths and all notes' aliases; every raw link is resolved.
5. The requesting table projects its columns, applies `LIMIT`, and returns one `RecordBatch`.

Reads are sequential in the first release. Concurrency is an internal detail that can change without touching the surface.

## Failure Modes

| Situation | Behavior |
|---|---|
| `path` does not exist / is not a directory / S3 list fails | Registration fails with the path named. |
| `hierarchy_level` is not `catalog`; `access_mode: read_write`; unknown option key | Registration fails naming the offending field. |
| Malformed frontmatter | Row kept; `frontmatter_json` NULL; `frontmatter_error` set. |
| Invalid UTF-8 | Row kept; lossy decode. |
| File larger than `max_file_bytes` | File skipped; warning with path. Documented as the one case that drops a row. |
| Single file unreadable mid-scan (deleted, permission) | File skipped; warning with path. |
| Root becomes unreadable between registration and a scan | Scan fails with a `DataFusionError::External` naming the root. |
| Link to a file that exists only case-differently | Resolves (`exact`/`name`), since matching is case-insensitive. |

Errors name paths, never contents.

## Observability

Registration logs the source name, root, and `surface_version = 1` at `info`. Each scan logs at `debug` the file count, note count, skipped count, and elapsed time. Skipped files log at `warn` with the path and reason.

## Testing Strategy

All tests live in the crate, behind `feature = "obsidian"`, and run in CI with the rest of the library suite.

- **Unit, pure functions.** `frontmatter::split`/`parse`: no block, valid block, malformed block, `...` terminator, `---` in body text, aliases as scalar/list/other, tags as list/string/`tag:` key. `markdown::extract`: every wikilink variant (`|display`, `#heading`, `#^block`, embed, spaces and CJK in target, adjacent links, empty target), Markdown links and images, autolinks, external classification for `https:`/`mailto:`/`obsidian:`, tags at line start / after whitespace / rejected in `C#` and URLs and `# Heading` / rejected all-digit / nested `a/b` / trailing punctuation, everything inside fenced, indented, and inline code ignored, correct `line` numbers. `resolve::Index`: one case per row of the resolution table, including case-insensitivity and the `.md`-optional rule.
- **Fixture vault.** `crates/skardi/src/sources/providers/obsidian/fixtures/vault/` — a hand-written vault of roughly fifteen files covering every rule above plus `.obsidian/` and `.trash/` content that must not appear, an attachment, two same-named notes in different folders (ambiguity), and an alias target. Tests register it and assert full table contents for all three tables, projection, `LIMIT`, deterministic order, and the two canonical graph queries (in-degree by `to_path`; orphan notes via anti-join).
- **Registration.** Non-catalog rejected; read-write rejected; missing root rejected; unknown option rejected; default excludes applied; custom `exclude_globs` replaces the default; `max_file_bytes` skips and warns.
- **Blob move.** The existing `documents` tests continue to pass unchanged after `blob.rs` moves, and `Loc::parse` tests for `s3://` URIs run under the `obsidian` feature alone.

No live or mocked S3 test in the first release (see Non-goals).

## Acceptance Criteria

- A context declaring `type: obsidian` over the fixture vault registers, and `SELECT * FROM vault.main.notes / links / tags` return the expected rows in the documented order.
- Every column in the three schemas is non-NULL for at least one fixture row, and every documented NULL case appears in at least one.
- Every `resolution` value and every `kind` value appears in the fixture results.
- Editing a fixture note between two scans changes the second result with no restart.
- `.obsidian/` and `.trash/` never appear; a symlink out of the vault is never read.
- `cargo test -p skardi --lib --features obsidian` and the full library suite pass; `cargo fmt` and `cargo clippy` are clean; a build with `--features documents` alone and one with `--features obsidian` alone both compile.

## Expected Repository Shape

```
crates/skardi/Cargo.toml                          # obsidian feature; pulldown-cmark dep
crates/skardi/src/sources/data_source_type.rs     # Obsidian variant
crates/skardi/src/sources/providers/mod.rs        # pub mod blob (shared); pub mod obsidian
crates/skardi/src/sources/providers/blob.rs       # moved from documents/blob.rs
crates/skardi/src/sources/providers/documents/    # imports super::blob
crates/skardi/src/sources/providers/obsidian/
  mod.rs  config.rs  frontmatter.rs  markdown.rs  resolve.rs  scan.rs  table.rs
  fixtures/vault/…
crates/server/src/config.rs                       # DataSourceType::Obsidian arm
docs/obsidian.md                                  # user documentation
README.md                                         # source list entry
```

## Documentation Commitments

`docs/obsidian.md` documents the build flag, configuration and options, the catalog namespace, all three schemas, the frontmatter/tag/link rules including the resolution table, the failure-mode table, the double-parse cost of joins, and example queries: notes by tag, notes by frontmatter property, most-linked notes, orphan notes, dangling links, external sites referenced.

## Future Extensions

- `properties` table exploding frontmatter keys.
- Reading `.obsidian/app.json` for attachment folder and link-format settings.
- mtime-keyed incremental cache, and parallel reads, once a vault shows the need.
- `kind = 'external'` already ships; bare-URL detection could join it.
- Block-level rows (headings, callouts) if chunking pipelines want structure finer than a note.
- Live S3 verification as an opt-in integration test, mirroring `documents_s3_live`.
