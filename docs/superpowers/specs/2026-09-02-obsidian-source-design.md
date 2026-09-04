# Obsidian Vault Source Design

**Status:** Implemented (plan: docs/superpowers/plans/2026-09-03-obsidian-source.md, branch feature/obsidian-source)
**Date:** 2026-09-02
**Branch:** `feature/obsidian-source`

## Summary

Skardi will support Obsidian vaults as a first-class, read-only native data source, `type: obsidian`. One configured source binds one vault — a directory of Markdown files on the local filesystem or under an `s3://` prefix — and exposes three fixed tables as a catalog: `notes`, one row per Markdown file with its body and parsed frontmatter; `links`, one row per link found in a note — in its body or in its frontmatter properties — resolved to its target file with Obsidian's own resolution rules; and `tags`, one row per note × tag from both frontmatter and body.

Every scan re-reads the vault from scratch: list, read, parse, emit one `RecordBatch`. There is no cache, no external process, no write path, and — for a local path — no network; an `s3://` vault talks only to its object-store endpoint. The provider is a format adapter: it understands exactly the three Obsidian-specific structures — YAML frontmatter, `[[wikilinks]]`, and `#tags` — and leaves everything else (search, embedding, graph analytics) to SQL over the three tables.

## Motivation

Obsidian is one of the most widely used personal knowledge bases, and its data is unusually well suited to Skardi: a vault is plain Markdown on disk, so an agent's own working notes, a user's research library, or a team's shared vault are already sitting in a queryable format — missing only the relational view. The value is not "read a Markdown file" (a `SELECT` over `documents` could do that) but the structures Obsidian layers on top: frontmatter properties that behave like columns, tags that behave like labels, and links that form a graph. Once those are tables, "which notes link to this one", "everything tagged `project/skardi` modified this week", and "notes with `status: draft` in frontmatter" are one SQL statement each, joinable with every other Skardi source.

## Research Findings

**Obsidian has no cloud API.** It is a local-first application; the official Sync and Publish services are proprietary with no public read API. The Open Connector gateway — Skardi's path to SaaS providers as source packs — carries no `obsidian` provider as of `oomol-lab/open-connector` `origin/main` on 2026-09-02 (checked against the full provider list; sibling note tools Roam, Trilium, and Memos are present, Obsidian is not). A source pack is therefore not an option without upstream work, and the shape of the data argues against one anyway: the only integration surface is the vault directory itself. This is a native source, sibling to `documents` and `rss`.

**A vault is a directory.** Notes are `.md` files; the note title is the file stem; subfolders are the user's folder tree; attachments live alongside. Obsidian keeps its own configuration under `.obsidian/` and deleted notes under `.trash/` — metadata and garbage, not content.

**The three structures have precise rules.** Frontmatter is a leading YAML block fenced by `---` lines. Tags are `#` followed by letters, digits, `_`, `-`, `/` (Unicode letters allowed), must contain at least one non-digit character, and are not recognized inside code. Links come in three syntaxes — `[[wikilinks]]` with optional `#heading`, `#^block`, and `|display` parts and an `!` prefix for embeds; standard Markdown `[text](target)`; and `<autolinks>` — and a bare wikilink target resolves against the whole vault by file name, with ambiguity possible when names repeat. Aliases are display text, not destinations: Obsidian's Aliases documentation says it "creates the link with the alias as its custom display text, for example `[[Artificial Intelligence|AI]]`" and, "rather than just using the alias as the link destination (`[[AI]]`)", does so deliberately for wikilink interoperability — a hand-written `[[AI]]` is an unresolved link in Obsidian. Links also live in frontmatter: Obsidian's Properties documentation states that Text and List properties "can contain … [[Internal links]] using the `[[Link]]` syntax" and that such links "must be surrounded with quotes" (a bare `[[x]]` is a nested YAML sequence). Only the wikilink syntax is recognized there; Markdown-style links in properties are plain text. The two link syntaxes also differ in what a path means: a wikilink path (`[[folder/Note]]`) is relative to the vault root, while a Markdown destination is relative to the containing note, as in CommonMark — Obsidian's "Relative path to file" link format writes a sibling in the same folder as `[other](other.md)`, with no `./` prefix — and only its "Absolute path in vault" format writes root-relative Markdown paths.

**The existing `documents` source already solves file access.** Its `blob::BlobStore` abstracts "list a prefix, read a blob" over a local directory or an `s3://` prefix under an env-only credential contract. It is compiled behind the `documents` Cargo feature today, alongside the PDF tooling it was written for. Three details of its current shape matter here: `list` returns only `(Loc, rel_key)` — the S3 listing's `ObjectMeta.size` and `last_modified` are discarded and the local walk never calls `metadata()`; `get` buffers the whole object (`std::fs::read`, `bytes().to_vec()`); and the local walk decides directories with `Path::is_dir`, which follows symlinks. A size cap enforced before reading, non-null `size_bytes`/`modified_at`, and a no-symlink guarantee therefore all need a small, explicit extension of that API, not just a move.

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
- Serve frontmatter as one `frontmatter_json` column rather than exploding keys into columns. Frontmatter is schemaless per note; a fixed column set would be wrong for every vault. `aliases` alone is lifted to a typed column because it is the one property Obsidian itself gives semantics to (search, link autocomplete) and because it powers the alias-repair query over dangling links (see Documentation Commitments).

**Freshness and execution**

- Rescan on every query: list the vault, read every `.md`, parse, emit. No state survives a scan.
- Execute as a single partition producing one `RecordBatch` through a `MemoryStream`, as `DocumentsTable` does. Vaults are thousands of files, not millions; parsing them is tens to hundreds of milliseconds locally.
- Run the whole scan off the Tokio worker. `BlobStore`'s local arms are synchronous `std::fs` calls, and a vault is thousands of them; awaiting them inline from `TableProvider::scan` would block the worker executing the query, which AGENTS.md's async rule forbids. `VaultScan::run` is therefore a synchronous function executed inside `tokio::task::spawn_blocking`; the S3 arms' futures are driven from that blocking thread with `tokio::runtime::Handle::current().block_on`, and the `BlobStore` for the scan is resolved inside the same task so the S3 client is created and used on one runtime (the hazard `blob.rs` documents and `documents` avoids with a dedicated parse thread).
- Scan each table independently. A query joining `notes` and `links` parses the vault twice. This is accepted for the first release in exchange for zero shared state; the cost is documented.
- Support column projection and `LIMIT`; do not push filters down. DataFusion filters the in-memory batch.
- Fail the scan, not the row, when every attempted read fails. A non-empty listing where no `get` succeeds — S3 credentials with `List` but not `Get`, or credentials expired after registration — must surface as an error naming the root and the first failure, not as three empty tables that look like an empty vault. This is the guard `documents` already applies (`parse.rs`, "all matched objects failed to fetch/parse"). The denominator is *attempted reads*: files skipped by policy (over `max_file_bytes`, symlinks) are not failures, so a vault whose only note is oversized yields an empty result with a warning, not a credentials error. A malformed frontmatter block is a kept row, not a failure. An empty listing is a valid, empty vault.
- Order rows deterministically: `notes` by `path`; `links` by `from_path`, then frontmatter links in traversal order, then body links by `(line, occurrence)`; `tags` by `(path, tag, source)`.

**Parsing**

- Parse Markdown with `pulldown-cmark`. Its event stream identifies code blocks and code spans (where tags and wikilinks are not recognized) and yields standard links and images with their destinations. Wikilinks and tags — Obsidian extensions no CommonMark parser knows — are extracted by pattern from the non-code text ranges.
- Parse frontmatter with `serde_yaml` (already a workspace dependency) and re-serialize to JSON. A frontmatter block that is present but malformed yields `frontmatter_json = NULL` and a populated `frontmatter_error`; the row is kept.
- Decode file bytes as UTF-8 with lossy replacement. No row is dropped for encoding.
- Extract links from frontmatter too: every string value in the parsed frontmatter is scanned with the same wikilink pattern as the body, because Obsidian recognizes `[[…]]` in Text and List properties and counts them as links. Only the wikilink syntax is recognized there. A missed frontmatter link would understate a note's in-degree and misreport orphans — the queries the `links` table exists for.
- Record where each link came from in `links.source` (`body` / `frontmatter`), mirroring `tags.source`, and leave `line` NULL for frontmatter links: parsed YAML values carry no positions, and scanning the raw YAML text for line numbers would also catch `[[…]]` in comments and keys that Obsidian does not treat as links.
- Classify links by syntax and by target: `wikilink`, `embed`, `markdown`, or `external` (any target with a URL scheme, regardless of syntax).
- Resolve internal links in a fixed order — exact path, then unique file name — and report `ambiguous` or `missing` with `to_path = NULL` rather than choosing.
- Never resolve through aliases. Obsidian writes alias links as `[[Note|Alias]]` and treats a bare `[[Alias]]` as unresolved; resolving it here would report a dangling link as an edge and hide it from the dangling-link query. The alias-repair query (a join of `missing` links against `notes.aliases`) recovers the intent without misstating the graph.

**Configuration and registration**

- Configure through `path` plus the flat `options` map (`exclude_globs`, `max_file_bytes`), like `documents`. There is nothing here that warrants a typed block.
- Always exclude `.obsidian/**` and `.trash/**` by default.
- Perform no parsing at registration; only check that the root exists and is readable (a directory check locally, one `list` request for `s3://`), and reject non-catalog hierarchy or read-write access.

**Packaging**

- Gate the provider behind an `obsidian` Cargo feature, as `documents` and `rss` are gated. The reason is concrete: `s3://` support needs the optional `object_store` dependency with its AWS backend, which is currently pulled in only by the `documents` feature. `obsidian = ["dep:glob", "dep:object_store", "dep:pulldown-cmark"]` in `crates/skardi` (plus `libc` as an unconditional `cfg(unix)` dependency for `O_NOFOLLOW`, used by the shared `blob.rs`), and the same two-level mapping the siblings use in `crates/server/Cargo.toml`: `obsidian = ["skardi/obsidian"]`, since the server's `config.rs` arm is gated on the *server's* feature name. Without it, `cargo build -p skardi-server --features obsidian` is an unknown-feature error.
- Lift `documents::blob` to a shared module, `sources/providers/blob.rs`, compiled when either feature is enabled (`#[cfg(any(feature = "documents", feature = "obsidian"))]`), and extend its `list` in one bounded way: it takes `ListOptions { recursive, follow_symlinks }` and returns `Vec<BlobEntry { loc, rel_key, size, modified }>`, carrying the metadata both backends already have at listing time (`DirEntry::metadata()` locally, `ObjectMeta` on S3). `get` gains the same knob — `get(loc, ReadOptions { follow_symlinks })` — because the listing-time symlink check alone is a time-of-check/time-of-use gap: in a writable or shared vault a regular file can be replaced by a symlink between `DirEntry::file_type()` and `std::fs::read`, which follows it. With `follow_symlinks: false` the local arm opens with `O_NOFOLLOW` (unix, via the `libc` crate — a new direct dependency, already in the tree transitively through tokio) and then checks the opened handle's `metadata().file_type().is_file()`; on non-unix targets it falls back to a `symlink_metadata` check before opening, with the residual race documented. `put` is unchanged. `documents` adapts to the new entry type and passes `follow_symlinks: true` to both calls, so its behavior does not change in this work; it simply ignores the two new fields. This is the one refactor in scope.
- Enforce `max_file_bytes` from the listing's `size`, before any `get`. Without listing metadata the cap could only fire after a huge object was already in memory (S3 has no cheaper alternative than a HEAD per object), which would make the cap decorative.

**Security and trust boundary**

- Treat the vault as trusted user data, not hostile input. There is no HTML rendering and no execution of anything found in a note; link targets are matched against the listing, never fetched. Parser robustness (malformed YAML, odd bytes, huge files) is a correctness concern, handled per file.
- Network egress depends on the path. A local `path` performs no network I/O at all. An `s3://` path is network egress: registration issues one list request and every scan issues list and get requests to the configured S3 endpoint (AWS or an S3-compatible endpoint selected through the environment), authenticated with credentials from the environment under the `documents` contract. Operators in network-isolated deployments must allow that endpoint or use a local path; the source must not be described as egress-free in `docs/obsidian.md`.
- Do not follow symbolic links, at listing time or at read time. A symlink inside the vault pointing outside it would otherwise let `path: ~/vault` read arbitrary files. Listing (`ListOptions::follow_symlinks = false`) skips symlinked files and directories with a warning, detected with `DirEntry::file_type()`, which does not follow links; this must be a listing-time rule because the walk descends inside `list` — a caller cannot filter out a directory it has already been walked through. Reading (`ReadOptions::follow_symlinks = false`) opens with `O_NOFOLLOW` and verifies the opened handle is a regular file, so a file swapped for a symlink between listing and reading is refused rather than followed; the refusal counts as a read failure (warning, and it feeds the wholesale-failure guard). `documents` keeps following symlinks as it does today.
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

`Loc::parse`, `BlobStore::resolve`, and `put` are unchanged. `list(prefix, ListOptions) -> Vec<BlobEntry>` replaces `list(prefix, recursive) -> Vec<(Loc, String)>`, and `get(loc, ReadOptions) -> Vec<u8>` replaces `get(loc)`:

```rust
pub struct ListOptions { pub recursive: bool, pub follow_symlinks: bool }
pub struct ReadOptions { pub follow_symlinks: bool }
pub struct BlobEntry {
    pub loc: Loc,
    pub rel_key: String,             // relative to prefix, `/` separators (as today)
    pub size: u64,                   // fs metadata len / ObjectMeta.size
    pub modified: DateTime<Utc>,     // fs mtime / ObjectMeta.last_modified
}
```

Locally, `ListOptions::follow_symlinks: false` skips any entry whose `DirEntry::file_type()` is a symlink; `true` reproduces today's `Path::is_dir` behavior. `ReadOptions::follow_symlinks: false` opens with `OpenOptions::custom_flags(libc::O_NOFOLLOW)` on unix and errors if the open fails with `ELOOP` or the opened handle's `metadata().file_type()` is not a regular file; on non-unix it checks `symlink_metadata` before opening (documented residual race); `true` is today's `std::fs::read`. S3 has no symlinks; both flags are ignored there. Gated on `any(feature = "documents", feature = "obsidian")`. `documents` imports it from the new location, passes `follow_symlinks: true` to both, and destructures `BlobEntry` where it used the tuple.

### `sources/providers/obsidian/mod.rs`

`register_obsidian_tables(session_ctx, name, path, options, access_mode, hierarchy_level)`: validates and registers the catalog. Module doc records every decision above with its rationale, in the style of `rss/mod.rs`.

### `obsidian/config.rs`

`ScanOptions::from_map(options)`: `exclude_globs` (comma-separated, default `.obsidian/**,.trash/**`), `max_file_bytes` (default `16777216`). Unknown keys are rejected at registration so a typo does not silently disable an exclusion.

### `obsidian/frontmatter.rs`

`split(text) -> Split { yaml: Option<&str>, body: &str, body_first_line: u32 }`: a frontmatter block is a first line of exactly `---`, YAML until the next line of exactly `---` (or `...`), and the body is everything after. `body_first_line` is the 1-based source line on which `body` begins (1 when there is no block; the closing fence's line + 1 otherwise), so line numbers computed over the stripped body can be mapped back to the file. `parse(yaml) -> Result<(serde_json::Value), String>`. Helpers lift `aliases` and `tags`/`tag` (list, or comma-/space-separated string; leading `#` stripped) out of the JSON value. `links(value) -> Vec<RawLink>` walks every string value in the parsed frontmatter (top-level, inside lists, and inside nested maps, in document order) and applies the wikilink pattern shared with `markdown.rs`.

### `obsidian/markdown.rs`

`extract(body, body_first_line) -> Extracted { tags: Vec<(String, line)>, links: Vec<RawLink> }`. Walks `pulldown-cmark` events with byte offsets. Skips `CodeBlock` and `Code` ranges. Emits `RawLink` for `Link`/`Image` events (with `LinkType::Autolink` and scheme-bearing destinations classified external) and scans `Text` ranges for wikilinks and tags. A link's `line` is the number of newlines before its byte offset in `body`, plus `body_first_line` — so `links.line` names the line in the source file, as the schema promises, rather than restarting at 1 after a stripped frontmatter block.

### `obsidian/resolve.rs`

`Index::build(all_paths)` and `Index::resolve(from_path, &RawLink) -> (Option<path>, Resolution)`. `RawLink` carries the syntax it was written in, because wikilink paths and Markdown paths resolve against different bases (vault root vs. the containing note). Pure, tested against the rule tables below.

### `obsidian/scan.rs`

`VaultScan::run(root_uri, opts) -> Result<Vec<ParsedNote>>`: a **synchronous** function — resolve the `BlobStore`, list, filter, read, parse (frontmatter links first, then body links), then resolve every link using an index over all listed files. The table's `ExecutionPlan` calls it through `tokio::task::spawn_blocking`; inside, local I/O is plain `std::fs` via the blob arms and remote I/O is `Handle::current().block_on(store.get(..))`. Nothing in this module is `async`, which is also what makes it testable without a runtime.

### `obsidian/table.rs`

Three `TableProvider`s sharing one `ExecutionPlan` implementation parameterized by table kind; single partition; projection and limit applied when building the batch.

### `crates/server/src/config.rs`

One `DataSourceType::Obsidian` arm, structured like the `Rss` arm: gated on the server's `obsidian` feature, re-checking nothing the provider checks itself, with a `#[cfg(not(feature = "obsidian"))]` arm that fails registration with "obsidian data source type requires the `obsidian` feature to be enabled at build time" — the same shape as the `documents` and `rss` arms. `DataSourceType::Obsidian` is also added to `CATALOG_SUPPORTED_SOURCES`: `validate_data_sources` runs its reserved-catalog-name check (`datafusion`, `information_schema`) and its catalog-mode guards (no flat per-table `options`) only for variants in that list, and `register_catalog` replaces a built-in catalog unconditionally, so a source named `datafusion` would otherwise silently hide every built-in table. `Rss` joined the list for the same reason.

### `crates/server/Cargo.toml`

`obsidian = ["skardi/obsidian"]`, with a comment in the style of the `rss` entry.

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
| `resolution` | Utf8 | no | `exact`, `name`, `ambiguous`, `missing`, `external` — see Link Resolution. |
| `source` | Utf8 | no | `body` (found in the Markdown body) or `frontmatter` (found in a property value). |
| `line` | Int32 | yes | 1-based line of the link's start in the source file for body links; NULL for frontmatter links. |

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

Wikilinks are matched in non-code text: optional `!`, `[[`, target, optional `#heading` or `#^block`, optional `|display`, `]]`. Target and parts are trimmed. Markdown links and images come from the parser's `Link` and `Image` events. Their raw destination is split at the first **literal** `#` into path and fragment first, and only then are the two parts percent-decoded independently — so `foo%23bar.md` stays the file name `foo#bar.md` rather than becoming path `foo` with heading `bar.md`. A decoded fragment beginning with `^` is a `block_id`, otherwise a `heading`. Autolinks (`<https://…>`) are `Link` events with `LinkType::Autolink`. Every `RawLink` records which syntax produced it; resolution depends on it.

A destination with a URL scheme (`[a-zA-Z][a-zA-Z0-9+.-]*:` prefix, e.g. `https:`, `mailto:`, `obsidian:`) is `external`: `to_path` NULL, `resolution = 'external'`, `target` = the full destination. Everything else is internal and goes through resolution.

### Links in frontmatter

After the frontmatter parses, every string value it contains — top-level scalars, list elements, and strings inside nested maps, visited in document order — is scanned with the same wikilink pattern as the body. Each match becomes a `links` row with `source = 'frontmatter'`, `line = NULL`, and `kind`/`heading`/`block_id`/`display_text` filled exactly as for a body wikilink; it then goes through the same resolution. Two consequences follow from Obsidian's own rules and are documented rather than worked around: an unquoted `[[x]]` parses as a nested YAML list whose string is `x`, so it yields no link (Obsidian requires the quotes too); and Markdown-style `[text](target)` inside a property is plain text, not a link. `aliases` and `tags` values are scanned like any other string; a `[[…]]` there is a link as well as an alias or tag, which is what Obsidian does. Obsidian defines property links only for Text and List properties; this provider also scans strings in nested maps because Obsidian assigns nested maps no behavior at all, and one uniform rule is easier to state and test than an exclusion.

### Link Resolution

Inputs: the `RawLink` (target string and syntax), the linking note's folder, and an index over every listed file (including attachments) keyed both by full relative path and by lowercased file name — the stem for `.md` files (`Note` matches `Note.md`), the full name for others (`a.png` matches `a.png`). All matching is case-insensitive, matching Obsidian's behavior on case-insensitive filesystems. Aliases take no part in resolution. Path normalization collapses `.` and `..`; a path that climbs above the vault root is `missing`.

The base a path is resolved against depends on the syntax. **Wikilinks** (`[[…]]`, `![[…]]`) are vault-root relative, applied in order, first step that applies decides:

| Step | Applies when | Result |
|---|---|---|
| `exact` (self) | target is empty (`[[#Heading]]`) | `to_path = from_path` |
| `exact` (relative) | target starts with `./` or `../` | resolve against the note's folder and normalize; `.md` appended if the result has no extension; must exist, else `missing` |
| `exact` (vault path) | target contains `/` or has a file extension | match the relative path from the vault root, `.md` optional for notes; must exist, else `missing` |
| `name` | exactly one file has that name | that file |
| `ambiguous` | more than one file has that name | `to_path = NULL` |
| `missing` | nothing matched | `to_path = NULL` |

**Markdown links** (`[…](…)`, `![…](…)`) are note-relative, as in CommonMark and as Obsidian's "Relative path to file" format writes them (`[other](other.md)` for a sibling in the same folder, `../x.md` to climb); Obsidian's "Absolute path in vault" format writes root-relative paths in the same syntax, so the root is tried second:

| Step | Applies when | Result |
|---|---|---|
| `exact` (self) | path is empty (`[text](#Heading)`) | `to_path = from_path` |
| `exact` (note-relative) | always tried first | join the note's folder and the decoded path, normalize; `.md` appended if no extension; exists → that file |
| `exact` (vault path) | path contains `/` | match the relative path from the vault root, `.md` optional; exists → that file |
| `name` | path contains no `/` and exactly one file has that name | that file (covers Obsidian's "Shortest path" format writing `[x](other.md)` for a unique file elsewhere in the vault) |
| `ambiguous` | path contains no `/` and more than one file has that name | `to_path = NULL` |
| `missing` | nothing matched | `to_path = NULL` |

For `[child](other.md)` written in `folder/note.md`, this resolves `folder/other.md` when it exists; resolving from the vault root first would have reported an ordinary sibling link as `missing` or bound it to a different root-level `other.md`.

Obsidian resolves an ambiguous bare name to one of the candidates by its own heuristics; this provider does not imitate them. Reporting `ambiguous` is the honest answer and lets the user fix the vault.

A bare `[[Alias]]` whose text matches a note's `aliases` entry but no file name is `missing`, exactly as in Obsidian, where such a link opens a new empty note. Obsidian's autocomplete never produces this form — it writes `[[Note|Alias]]`, which resolves through `Note` — so a `missing` link matching an alias is almost always a hand-written mistake, and the alias-repair query in `docs/obsidian.md` surfaces it as one.

## Scan Execution

1. `BlobStore::list(root, ListOptions { recursive: true, follow_symlinks: false })`. Paths are normalized to forward-slash relative form and filtered by `exclude_globs`. `.md` files are notes; every other path is an attachment candidate for the resolution index. Each entry's `size` and `modified` feed `notes.size_bytes` and `notes.modified_at` directly.
2. Each note whose listed `size` exceeds `max_file_bytes` is skipped with a `tracing::warn!` naming the path and size — before any read. The rest are read with `BlobStore::get(loc, ReadOptions { follow_symlinks: false })` and decoded lossily as UTF-8. A read that fails — including a path that became a symlink since listing — is skipped with a warning; if at least one read was attempted and every one failed, the scan stops here with an error naming the root, the attempted count, and the first failure (path and cause) — see Failure Modes.
3. Frontmatter is split and parsed, and its string values are scanned for wikilinks; the body is walked for tags and raw links.
4. The resolution index is built from all listed paths; every raw link is resolved.
5. The requesting table projects its columns, applies `LIMIT`, and returns one `RecordBatch`.

Steps 1–4 run inside one `spawn_blocking` task (see Decisions); step 5 runs on the async side once the task returns. Reads are sequential in the first release. Concurrency is an internal detail that can change without touching the surface.

## Failure Modes

| Situation | Behavior |
|---|---|
| `path` does not exist / is not a directory / S3 list fails | Registration fails with the path named. |
| `hierarchy_level` is not `catalog`; `access_mode: read_write`; unknown option key | Registration fails naming the offending field. |
| Malformed frontmatter | Row kept; `frontmatter_json` NULL; `frontmatter_error` set. |
| Invalid UTF-8 | Row kept; lossy decode. |
| File larger than `max_file_bytes` | File skipped before it is read (size comes from the listing); warning with path and size. Documented as the one case that drops a row. |
| Symlinked file or directory inside the vault | Skipped at listing time; warning with path. |
| Listed regular file replaced by a symlink before it is read | Open refused (`O_NOFOLLOW`); treated as a read failure: warning with path, counts toward the wholesale-failure guard. |
| Source named `datafusion` or `information_schema`; flat per-table `options` on a catalog source | Rejected by `validate_data_sources`, as for every `CATALOG_SUPPORTED_SOURCES` type. |
| Some files unreadable mid-scan (deleted, permission) while others read | Failed files skipped; warning with path and cause; remaining rows returned. |
| Every attempted read fails (S3 `List` without `Get`, credentials expired after registration, vault directory permissions) | Scan fails with a `DataFusionError::External` naming the root, the attempted count, and the first failure — never an empty result. Policy skips (size cap, symlinks) do not count as attempts. |
| Vault lists no `.md` files (empty vault, or everything excluded/oversized) | Three empty tables; no error. Skips are still warned individually. |
| Root becomes unreadable between registration and a scan | Scan fails with a `DataFusionError::External` naming the root. |
| Link to a file that exists only case-differently | Resolves (`exact`/`name`), since matching is case-insensitive. |

Errors name paths, never contents.

## Observability

Registration logs the source name, root, and `surface_version = 1` at `info`. Each scan logs at `debug` the file count, note count, skipped count, and elapsed time. Skipped files log at `warn` with the path and reason.

## Testing Strategy

All tests live in the crate, behind `feature = "obsidian"`, and run in CI with the rest of the library suite.

- **Unit, pure functions.** `frontmatter::split`/`parse`: no block, valid block, malformed block, `...` terminator, `---` in body text, aliases as scalar/list/other, tags as list/string/`tag:` key. `frontmatter::links`: a quoted wikilink in a text property, several in a list property, one with `#heading|display`, one inside a nested map, an unquoted `[[x]]` (no link), a Markdown-style link (no link), a link inside `aliases`. `markdown::extract`: every wikilink variant (`|display`, `#heading`, `#^block`, embed, spaces and CJK in target, adjacent links, empty target), Markdown links and images, autolinks, external classification for `https:`/`mailto:`/`obsidian:`, tags at line start / after whitespace / rejected in `C#` and URLs and `# Heading` / rejected all-digit / nested `a/b` / trailing punctuation, everything inside fenced, indented, and inline code ignored, correct `line` numbers — including in a note with a frontmatter block, where a link on the first body line reports the block's line count + 1, not 1. `resolve::Index`: one case per row of both resolution tables, including case-insensitivity, the `.md`-optional rule, a bare alias resolving to `missing`, a Markdown sibling link `[x](other.md)` from `folder/note.md` resolving to `folder/other.md` even when a root-level `other.md` also exists, `../` climbing, a Markdown path that climbs above the root (`missing`), and `foo%23bar.md` resolving to the file `foo#bar.md` with no heading.
- **Fixture vault.** `crates/skardi/src/sources/providers/obsidian/fixtures/vault/` — a hand-written vault of roughly fifteen files covering every rule above plus `.obsidian/` and `.trash/` content that must not appear, an attachment, two same-named notes in different folders (ambiguity), a note declaring an alias plus one `[[Note|Alias]]` link to it (resolves through `Note`) and one bare `[[Alias]]` link (`missing`, found by the alias-repair query), and a note whose only inbound link is a frontmatter property on another note (so the in-degree and orphan queries are wrong unless frontmatter links are extracted). Tests register it and assert full table contents for all three tables, projection, `LIMIT`, deterministic order, and the two canonical graph queries (in-degree by `to_path`; orphan notes via anti-join).
- **Registration.** Non-catalog rejected; read-write rejected; missing root rejected; unknown option rejected; default excludes applied; custom `exclude_globs` replaces the default; `max_file_bytes` skips and warns. In `crates/server`, `validate_data_sources` rejects an obsidian source named `datafusion` or `information_schema` and one carrying a flat `table` option, the way the existing `rss` catalog-guard tests do.
- **Wholesale-failure guard.** Three cases on temp-directory copies of the fixture: an empty vault (no `.md`) returns three empty tables without error; every note made unreadable (`#[cfg(unix)]`, mode `000`; the test first checks that the file really is unreadable and skips itself when running as root) fails the scan with the root and the first path in the message; one note unreadable and the rest intact returns the intact rows and only warns. A fourth case: a vault whose only note exceeds a test-lowered `max_file_bytes` returns empty tables with a warning, not the guard's error.
- **Server feature mapping.** `cargo build -p skardi-server --features obsidian` and `cargo build -p skardi-server` (no feature) both compile; the no-feature build's registration of a `type: obsidian` source fails with the "requires the `obsidian` feature" error, tested the way the `documents`/`rss` no-feature arms are.
- **Blob move and `list` extension.** The existing `documents` tests continue to pass after `blob.rs` moves and `list` returns `BlobEntry`. New `blob` unit tests on a temp directory: `size` and `modified` match `fs::metadata`; a symlinked file and a symlinked directory (pointing outside the root) are skipped under `ListOptions::follow_symlinks: false` and included under `true`; `get` on a symlink errors under `ReadOptions::follow_symlinks: false` (unix) and reads the target under `true`; `rel_key` is unchanged from today. `Loc::parse` tests for `s3://` URIs run under the `obsidian` feature alone.

No live or mocked S3 test in the first release (see Non-goals).

## Acceptance Criteria

- A context declaring `type: obsidian` over the fixture vault registers, and `SELECT * FROM vault.main.notes / links / tags` return the expected rows in the documented order.
- Every column in the three schemas is non-NULL for at least one fixture row, and every documented NULL case appears in at least one.
- Every `resolution` value, every `kind` value, and both `links.source` values appear in the fixture results.
- Editing a fixture note between two scans changes the second result with no restart.
- A source named after a built-in catalog is rejected at validation, never registered.
- `.obsidian/` and `.trash/` never appear; a symlink out of the vault is never read, whether present at listing time or swapped in afterwards; a file over `max_file_bytes` is never read (verified by a fixture file whose size exceeds a test-lowered cap and a `get` that would fail if reached).
- An empty vault yields three empty tables; a non-empty vault whose every read fails yields an error naming the root, not empty tables; a partially unreadable vault yields the readable rows.
- `cargo test -p skardi --lib --features obsidian` and the full library suite pass; `cargo fmt` and `cargo clippy` are clean; a build with `--features documents` alone and one with `--features obsidian` alone both compile for `skardi` **and** for `skardi-server`; a `skardi-server` build without the feature compiles and rejects a `type: obsidian` source with the build-capability error.

## Expected Repository Shape

```
crates/skardi/Cargo.toml                          # obsidian feature; pulldown-cmark dep; libc (unix, O_NOFOLLOW)
crates/skardi/src/sources/data_source_type.rs     # Obsidian variant
crates/skardi/src/sources/providers/mod.rs        # pub mod blob (shared); pub mod obsidian
crates/skardi/src/sources/providers/blob.rs       # moved from documents/blob.rs; list → ListOptions / BlobEntry
crates/skardi/src/sources/providers/documents/    # imports super::blob; adapts to BlobEntry, follow_symlinks: true
crates/skardi/src/sources/providers/obsidian/
  mod.rs  config.rs  frontmatter.rs  markdown.rs  resolve.rs  scan.rs  table.rs
  fixtures/vault/…
crates/server/Cargo.toml                          # obsidian = ["skardi/obsidian"]
crates/server/src/config.rs                       # DataSourceType::Obsidian arm (+ no-feature arm); CATALOG_SUPPORTED_SOURCES
docs/obsidian.md                                  # user documentation
README.md                                         # source list entry
```

## Documentation Commitments

`docs/obsidian.md` documents the build flag, configuration and options, the catalog namespace, all three schemas, the frontmatter/tag/link rules including the resolution table and the frontmatter-link rules (quoting, wikilink syntax only), the failure-mode table, the double-parse cost of joins, and example queries: notes by tag, notes by frontmatter property, most-linked notes, orphan notes, dangling links, the alias-repair query (`missing` links whose `target` appears in another note's `aliases`, with the note they probably meant), external sites referenced.

## Future Extensions

- `properties` table exploding frontmatter keys.
- Reading `.obsidian/app.json` for attachment folder and link-format settings.
- mtime-keyed incremental cache, and parallel reads, once a vault shows the need.
- `kind = 'external'` already ships; bare-URL detection could join it.
- Block-level rows (headings, callouts) if chunking pipelines want structure finer than a note.
- Live S3 verification as an opt-in integration test, mirroring `documents_s3_live`.

## Implementation notes (2026-09)

Deviations from this spec that were settled during implementation, each
covered by a test:

- **Bare wikilink names containing `.`** (`[[Note.md]]`, `[[Note v2.1]]`)
  try a root-level exact path first and fall back to the name lookup. The
  spec's pure name rule made `[[Note.md]]` `missing` while Obsidian opens it.
- **URL-scheme detection** uses `^[A-Za-z][A-Za-z0-9+.-]*:\S` (RFC 3986
  scheme grammar) instead of a fixed scheme list, so `obsidian://`,
  `zotero://`, `mailto:` all classify as `external`.
- **Scan entry point** is an async `run_scan(root, opts)` wrapper around the
  synchronous `VaultScan::run` on `spawn_blocking`; `ObsidianScanExec` awaits
  it inside a one-item stream. No blocking on a Tokio worker.
- **Autolinks** (`<https://…>`) carry `display_text = NULL` rather than
  repeating the URL, so `display_text IS NOT NULL` means "the author wrote
  text".
- **Email autolinks** (`<me@example.com>`) get `mailto:` restored on their
  target. pulldown-cmark reports the bare address and prepends the scheme only
  when rendering HTML; without it the target carries no scheme and would
  resolve as a note name instead of `external`.
- **Tags inside comments and math** are not masked: `%%…%%`, `<!-- … -->`
  and `$…$` can yield `tags` rows. Only code is excluded, as Parsing Rules
  say; recorded so the gap is a decision rather than an oversight.
- **`[[Note:subtitle]]`** (no space after the colon) is `external` under the
  scheme grammar above; `[[Note: subtitle]]` is a note name. Tested.
- **A leading `/` in a Markdown link** is the vault root only; the linking
  note's folder is never tried (matches the docs' "root-relative" row).
- **`aliases` with nothing usable** (`""`, `[]`, `[7]`) is NULL, not an empty
  list — one shape for "no aliases".
- **Frontmatter tags** follow the body grammar's digit rule (`2026` dropped,
  `y2026` kept) and lose exactly one leading `#`.
- **Declared output ordering.** `ObsidianScanExec` declares `notes` by
  `path`, `links` by `from_path`, `tags` by `(path, tag, source)` whenever the
  projection keeps those leading columns, so an `ORDER BY` on them plans no
  sort. Emission type is `Final`: one batch after the whole scan.
- **Fixture:** `People/Bob.md` links only `[[Alice]]`, making `Rooms/B12.md`
  reachable solely through `Meeting.md`'s frontmatter — the frontmatter-only
  inbound note the Testing Strategy asks for. The link total is 27.
- **`max_file_bytes` is enforced at read time as well.** The listing's `size`
  is a snapshot, so a note that grows or is replaced before its `get` would
  otherwise be buffered in full. `ReadOptions` therefore became a struct —
  `{ symlinks, max_bytes }` — and the reader stops at `max_bytes + 1` observed
  bytes: locally an `fstat` on the open handle plus a `take`-bounded read, on
  S3 a size check against the body's own `ObjectMeta` plus a per-chunk running
  total over `into_stream()`, which aborts the transfer instead of collecting
  the whole response. The failure is a typed `SizeCapExceeded`, so the scan
  classifies it as the same policy skip as the listing-time cap (a skip, not
  an attempt) rather than as an unreadable note.
- **Read-time symlink guard covers every component.** The policy is
  `ReadOptions::symlinks` — `Symlinks::Follow` (what `documents` passes) or
  `Symlinks::NoneBeneath(&Loc)`, which carries the listed root — because
  `O_NOFOLLOW` guards only the last component of a path. The unix arm opens
  the root normally (operator configuration, may be a symlink), then `openat`s
  each component beneath it with `O_NOFOLLOW`: `O_DIRECTORY` for directories,
  `O_NONBLOCK` for the file, so a directory swapped for a symlink after
  listing fails with `ELOOP` too. The non-blocking final open plus an `fstat`
  regular-file check means a FIFO named `note.md` is refused instead of
  stalling the scan; the strict listing also skips anything that is not a
  regular file or directory. Non-unix keeps the `symlink_metadata`
  approximation, now per component.
