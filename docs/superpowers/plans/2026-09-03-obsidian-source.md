# Obsidian Vault Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `type: obsidian` — a read-only native source that exposes one Obsidian vault (local directory or `s3://` prefix) as the catalog tables `<name>.main.notes`, `<name>.main.links`, `<name>.main.tags`, rescanned on every query.

**Architecture:** The `documents` connector's `blob.rs` is lifted to a shared `providers/blob.rs` and extended with listing metadata and symlink control. A new `providers/obsidian/` module holds four pure parsing units (`config`, `frontmatter`, `markdown`, `resolve`), one synchronous scanner (`scan`) run inside `spawn_blocking`, and one `TableProvider`/`ExecutionPlan` pair parameterized by table kind (`table`). Registration (`mod.rs`) mirrors `rss`: catalog-only, read-only, one `MemoryCatalogProvider` published last. The server gets a `DataSourceType::Obsidian` arm behind a server-level `obsidian` feature.

**Tech Stack:** Rust edition 2024 (toolchain 1.96.1), DataFusion 52 / Arrow, `pulldown-cmark` 0.13 (`default-features = false`), `serde_yaml` 0.9 → `serde_json` (preserve_order), `glob` 0.3, `object_store` 0.12, `chrono` 0.4, `regex`, `percent-encoding` 2.3, `libc` (unix only, `O_NOFOLLOW`), `thiserror` 2, `tokio`.

**Spec:** `docs/superpowers/specs/2026-09-02-obsidian-source-design.md` (merged to `main` in `5713a6e`). Read it first; the plan argues from it and repeats only what an implementer needs at the keyboard.

## Global Constraints

- **No local test runs.** Nobody runs `cargo test`/`cargo nextest` locally (see memory rule); the only local command before a push is `cargo fmt --all`. Verification is GitHub CI: `cargo fmt --all -- --check`, `cargo check --all`, `cargo llvm-cov --no-report nextest --all-features`, `cargo test --doc -p skardi --all-features`, `cargo doc --no-deps -p skardi --all-features` with `RUSTDOCFLAGS=-D warnings`. Every "Run/Expected" line below describes what CI must show, not something to execute locally.
- **No commits by Claude.** Each task ends in a **Checkpoint**: run `cargo fmt --all`, then Owen reviews and commits/pushes. Suggested commit messages are given; Owen may change them.
- **Branch:** `feature/obsidian-source`, created from `origin/main` at `5713a6e` in worktree `.claude/worktrees/obsidian-sourcepack-dev-745bc6`. Verify with `git branch --show-current` before every destructive git operation.
- **Toolchain rules:** edition 2024; `unused_qualifications = "deny"` (write `use` imports, never inline `std::...` paths where an import exists in scope — but a one-off fully qualified path with no competing import is fine); clippy `large_futures = "warn"`; rustdoc must be warning-free (every intra-doc link must resolve; use backticks without brackets for names that are private or feature-gated).
- **Feature gating (verbatim from spec):** `crates/skardi`: `obsidian = ["dep:glob", "dep:object_store", "dep:pulldown-cmark"]`; `libc` as an unconditional `[target.'cfg(unix)'.dependencies]` entry; `blob.rs` compiled under `#[cfg(any(feature = "documents", feature = "obsidian"))]`. `crates/server`: `obsidian = ["skardi/obsidian"]`. Builds with `--features documents` alone, `--features obsidian` alone, and no feature must all compile for both crates.
- **Async rule (AGENTS.md):** no blocking I/O on a Tokio worker. `VaultScan::run` is synchronous and only ever runs inside `tokio::task::spawn_blocking`; S3 futures inside it use `tokio::runtime::Handle::current().block_on`. The `BlobStore` is resolved inside the same blocking task.
- **Defaults (verbatim):** `exclude_globs` default `.obsidian/**,.trash/**`; `max_file_bytes` default `16777216`; schema metadata key `skardi.obsidian.surface_version` = `"1"`; catalog schema name `main`; table names `notes`, `links`, `tags`.
- **Column contracts (verbatim from spec §Table Schemas):** `notes(path Utf8, name Utf8, folder Utf8, body Utf8, frontmatter_json Utf8?, frontmatter_error Utf8?, aliases List<Utf8>?, size_bytes Int64, modified_at Timestamp(ms, "UTC"))`; `links(from_path Utf8, to_path Utf8?, target Utf8, kind Utf8, display_text Utf8?, heading Utf8?, block_id Utf8?, resolution Utf8, source Utf8, line Int32?)`; `tags(path Utf8, tag Utf8, source Utf8)`. `kind ∈ {wikilink, embed, markdown, external}`, `resolution ∈ {exact, name, ambiguous, missing, external}`, `source ∈ {body, frontmatter}`.
- **Row order (verbatim):** `notes` by `path`; `links` by `from_path`, then frontmatter links in traversal order, then body links by `(line, occurrence)`; `tags` by `(path, tag, source)`.
- **Errors and logs name paths, never file contents.** Single-line `tracing` events with structured fields, as in `rss/mod.rs`.
- **No `.unwrap()`/`.expect()` in library code** (tests may). One exception: literal regexes compiled in a `LazyLock` may use `.expect("static regex")`, since a bad literal is a programming error caught by the first test run. No `unsafe`.
- **Plan deviations from the spec, all decided during planning and recorded in Task 10's spec edit:** (a) a dotted bare wikilink name (`[[Note v2.1]]`) that matches no exact vault path falls through to the `name` lookup instead of `missing`, matching Obsidian; (b) URL-scheme detection requires a non-space character after the colon (`^[A-Za-z][A-Za-z0-9+.-]*:\S`) so `[[Note: subtitle]]` is not `external`; (c) `scan.rs` additionally exposes an async `run_scan(root, opts)` wrapper doing the `spawn_blocking`, used by `table.rs` and tests; (d) the fixture avoids a CJK *file name* (macOS Unicode normalization risk) and instead uses `CJK.md` with CJK content; (e) autolinks (`<https://…>`) get `display_text = NULL`.

## File Structure

| Path | Responsibility |
|---|---|
| `crates/skardi/Cargo.toml` | `obsidian` feature, `pulldown-cmark` optional dep, `libc` unix dep. |
| `crates/skardi/src/sources/providers/blob.rs` | **moved** from `documents/blob.rs`; `ListOptions`, `ReadOptions`, `BlobEntry`; symlink control; metadata-carrying listings. |
| `crates/skardi/src/sources/providers/mod.rs` | `pub(crate) mod blob` (gated on either feature); `pub mod obsidian` (gated on `obsidian`). |
| `crates/skardi/src/sources/providers/documents/{mod,parse}.rs`, `crates/skardi/src/model/llm_extract/mod.rs` | adapt to the moved module and new API (`follow_symlinks: true`). |
| `crates/skardi/src/sources/data_source_type.rs` | `Obsidian` variant. |
| `crates/skardi/src/jobs/executor.rs`, `crates/server/src/pipeline_handlers.rs` | exhaustive `DataSourceType` matches gain an `Obsidian` arm. |
| `crates/skardi/src/sources/providers/obsidian/mod.rs` | module docs, `OBSIDIAN_SURFACE_VERSION`, `ObsidianError`, `register_obsidian_tables`, root check. |
| `crates/skardi/src/sources/providers/obsidian/config.rs` | `ScanOptions` from the flat `options` map; glob exclusion. |
| `crates/skardi/src/sources/providers/obsidian/markdown.rs` | `RawLink`, `LinkSyntax`, `Extracted`, `extract`, `find_wikilinks`, `has_url_scheme`, `LineIndex`. |
| `crates/skardi/src/sources/providers/obsidian/frontmatter.rs` | `split`, `parse`, `aliases`, `tags`, `links`. |
| `crates/skardi/src/sources/providers/obsidian/resolve.rs` | `Index`, `Resolution`, `LinkKind`, `Resolved`, the two resolution tables. |
| `crates/skardi/src/sources/providers/obsidian/scan.rs` | `ParsedNote`, `LinkRow`, `TagRow`, `Source`, `VaultScan::run`, `run_scan`, `parse_note`. |
| `crates/skardi/src/sources/providers/obsidian/table.rs` | `TableKind`, three schemas, `ObsidianTable`, `ObsidianScanExec`, `build_batch`. |
| `crates/skardi/src/sources/providers/obsidian/fixtures/vault/**` | hand-written fixture vault (15 files). |
| `crates/server/Cargo.toml`, `crates/server/src/config.rs` | server feature, `CATALOG_SUPPORTED_SOURCES`, registration arm + tests. |
| `docs/obsidian.md`, `README.md`, spec | user docs, source table row, spec status/deviations. |

---

### Task 1: Lift `blob.rs` to `providers/blob.rs` and extend its API

**Files:**
- Move: `crates/skardi/src/sources/providers/documents/blob.rs` → `crates/skardi/src/sources/providers/blob.rs` (`git mv`)
- Modify: `crates/skardi/Cargo.toml` (features block ~line 31; deps ~line 149; new `[target.'cfg(unix)'.dependencies]` section)
- Modify: `crates/skardi/src/sources/providers/mod.rs:1-3`
- Modify: `crates/skardi/src/sources/providers/documents/mod.rs:7-10`
- Modify: `crates/skardi/src/sources/providers/documents/parse.rs:16, 236-262, 620-621, 1069, 1081, 1109, 1117`
- Modify: `crates/skardi/src/model/llm_extract/mod.rs:656-663`
- Test: unit tests inside `crates/skardi/src/sources/providers/blob.rs`

**Interfaces:**
- Consumes: nothing new.
- Produces (used by Tasks 6 and 8):
  ```rust
  pub struct ListOptions { pub recursive: bool, pub follow_symlinks: bool }
  pub struct ReadOptions { pub follow_symlinks: bool }
  pub struct BlobEntry { pub loc: Loc, pub rel_key: String, pub size: u64, pub modified: chrono::DateTime<chrono::Utc> }
  impl BlobStore {
      pub async fn list(&self, prefix: &Loc, opts: ListOptions) -> anyhow::Result<Vec<BlobEntry>>;
      pub async fn get(&self, loc: &Loc, opts: ReadOptions) -> anyhow::Result<Vec<u8>>;
      pub async fn put(&self, loc: &Loc, bytes: &[u8]) -> anyhow::Result<()>;   // unchanged
      pub fn resolve(uri: &str) -> anyhow::Result<(BlobStore, Loc)>;              // unchanged
  }
  ```

- [ ] **Step 1: Cargo manifest — feature, deps**

In `crates/skardi/Cargo.toml`, after the `documents = [...]` feature line (line 31) add:

```toml
# obsidian source connector: an Obsidian vault (local dir or s3:// prefix) as
# `notes` / `links` / `tags` catalog tables. Shares `providers/blob.rs` with
# `documents` (hence glob + object_store); pulldown-cmark walks note bodies.
obsidian = ["dep:glob", "dep:object_store", "dep:pulldown-cmark"]
```

In `[dependencies]`, next to the `glob` line, add:

```toml
# CommonMark event stream for the obsidian connector (code ranges, links,
# images). `default-features = false` drops the getopts/html bits; the 0.13.4
# resolution is already in Cargo.lock via text-splitter.
pulldown-cmark = { version = "0.13", default-features = false, optional = true }
```

Add a new section after `[dependencies]` and before `[dev-dependencies]`:

```toml
[target.'cfg(unix)'.dependencies]
# `O_NOFOLLOW` for blob.rs's no-follow read (already in the tree via tokio).
libc = "0.2"
```

Update the `glob` and `object_store` comments to say they serve both connectors (`# include_globs (documents) / exclude_globs (obsidian) matching.`).

- [ ] **Step 2: Move the file and re-home the module**

```bash
git mv crates/skardi/src/sources/providers/documents/blob.rs crates/skardi/src/sources/providers/blob.rs
```

In `crates/skardi/src/sources/providers/mod.rs` replace the first three lines with:

```rust
// Shared local-vs-object-store I/O for the `documents` and `obsidian`
// connectors. `pub(crate)`: `llm_extract`'s image fetch also reads `s3://`
// refs through it so S3 client construction and the env-only credential
// contract live in exactly one place.
#[cfg(any(feature = "documents", feature = "obsidian"))]
pub(crate) mod blob;
pub mod clickhouse;
#[cfg(feature = "documents")]
pub mod documents;
```

(The `pub mod obsidian;` line is added in Task 2, once that directory exists.)

In `crates/skardi/src/sources/providers/documents/mod.rs` delete lines 7-10 (the comment and `pub(crate) mod blob;`).

- [ ] **Step 3: Rewrite the header and the two structs' API in `blob.rs`**

Replace the module doc (lines 1-6) with:

```rust
//! Local-vs-object-store I/O shared by the `documents` and `obsidian`
//! connectors.
//!
//! All filesystem / S3 access those connectors perform goes through
//! [`BlobStore`], so a source `path` (and `documents`' `image_store`) can each
//! independently be a local directory or an `s3://` prefix. Design docs:
//! `docs/superpowers/specs/2026-07-23-documents-s3-object-store-support-design.md`
//! and `docs/superpowers/specs/2026-09-02-obsidian-source-design.md`.
//!
//! Symlinks: `documents` follows them (its historical behavior); `obsidian`
//! refuses them at listing time ([`ListOptions::follow_symlinks`]) *and* at
//! read time ([`ReadOptions::follow_symlinks`], `O_NOFOLLOW` on unix) because a
//! symlink inside a vault pointing outside it would otherwise let `path:
//! ~/vault` read arbitrary files, and a file can be swapped for a symlink
//! between the two calls.
```

Add imports (keep the existing ones):

```rust
use chrono::{DateTime, Utc};
use object_store::ObjectMeta;
```

Add the three types after `impl Loc { … }`:

```rust
/// How [`BlobStore::list`] walks a prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListOptions {
    /// Descend into subdirectories / nested keys.
    pub recursive: bool,
    /// Local only: `true` reproduces `Path::is_dir` semantics (symlinks are
    /// followed); `false` skips any entry whose `DirEntry::file_type()` is a
    /// symlink, with a warning naming the path. Ignored for S3.
    pub follow_symlinks: bool,
}

/// How [`BlobStore::get`] opens a local file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadOptions {
    /// Local only: `true` is `std::fs::read`; `false` opens with `O_NOFOLLOW`
    /// (unix) and refuses anything that is not a regular file. Ignored for S3.
    pub follow_symlinks: bool,
}

/// One listed object/file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobEntry {
    pub loc: Loc,
    /// Relative to the listed prefix, `/`-separated on every backend so
    /// `doc_id`/`path` columns are stable across local and S3.
    pub rel_key: String,
    /// `fs::metadata().len()` locally; `ObjectMeta::size` on S3.
    pub size: u64,
    /// Filesystem mtime locally (UNIX epoch if the platform reports none);
    /// `ObjectMeta::last_modified` on S3.
    pub modified: DateTime<Utc>,
}
```

Replace `list` and `get`:

```rust
    /// List every object/file under `prefix`. No glob filtering happens here
    /// (callers filter on `rel_key`).
    pub async fn list(&self, prefix: &Loc, opts: ListOptions) -> Result<Vec<BlobEntry>> {
        match (self, prefix) {
            (BlobStore::Local, Loc::Local(root)) => list_local(root, opts),
            (BlobStore::Remote(store), Loc::S3 { bucket, key }) => {
                list_remote(store, bucket, key, opts.recursive).await
            }
            _ => anyhow::bail!("blob: BlobStore/Loc backend mismatch in list()"),
        }
    }

    /// Fetch the full bytes of one object/file.
    pub async fn get(&self, loc: &Loc, opts: ReadOptions) -> Result<Vec<u8>> {
        match (self, loc) {
            (BlobStore::Local, Loc::Local(path)) if opts.follow_symlinks => {
                std::fs::read(path).with_context(|| format!("reading {}", path.display()))
            }
            (BlobStore::Local, Loc::Local(path)) => read_local_no_follow(path),
            (BlobStore::Remote(store), Loc::S3 { key, .. }) => {
                let res = store
                    .get(&OsPath::from(key.as_str()))
                    .await
                    .with_context(|| format!("s3 get {key}"))?;
                let bytes = res
                    .bytes()
                    .await
                    .with_context(|| format!("s3 read body {key}"))?;
                Ok(bytes.to_vec())
            }
            _ => anyhow::bail!("blob: BlobStore/Loc backend mismatch in get()"),
        }
    }
```

Change the `put` mismatch message to `"blob: BlobStore/Loc backend mismatch in put()"`.

- [ ] **Step 4: Rewrite `list_local`, add `read_local_no_follow`, adapt the remote helpers**

```rust
/// Walk a local directory. A missing/unreadable root is a hard error;
/// subdirectory read errors are logged and skipped. Entries are sorted by
/// `rel_key` so listings are deterministic.
fn list_local(root: &Path, opts: ListOptions) -> Result<Vec<BlobEntry>> {
    std::fs::read_dir(root)
        .with_context(|| format!("blob: cannot read root directory {}", root.display()))?;

    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(dir = %dir.display(), error = %e, "blob: cannot read dir");
                continue;
            }
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !opts.follow_symlinks {
                // `DirEntry::file_type` does not follow links, so this sees
                // the symlink itself, whether it points at a file or a dir.
                let is_symlink = entry.file_type().map(|t| t.is_symlink()).unwrap_or(false);
                if is_symlink {
                    tracing::warn!(path = %path.display(), "blob: skipping symlink");
                    continue;
                }
            }
            if path.is_dir() {
                if opts.recursive {
                    stack.push(path);
                }
                continue;
            }
            let meta = match std::fs::metadata(&path) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "blob: cannot stat");
                    continue;
                }
            };
            let modified = meta
                .modified()
                .map(DateTime::<Utc>::from)
                .unwrap_or_else(|_| DateTime::<Utc>::from(std::time::UNIX_EPOCH));
            let rel_key = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            out.push(BlobEntry {
                size: meta.len(),
                modified,
                loc: Loc::Local(path),
                rel_key,
            });
        }
    }
    out.sort_by(|a, b| a.rel_key.cmp(&b.rel_key));
    Ok(out)
}

/// Read a local file refusing to follow a symlink at the final path component.
/// Unix: `O_NOFOLLOW` makes the open itself fail with `ELOOP` on a symlink,
/// then the opened handle is checked to be a regular file (a FIFO or device
/// would otherwise block or misbehave). This closes the listing→read race.
#[cfg(unix)]
fn read_local_no_follow(path: &Path) -> Result<Vec<u8>> {
    use std::io::Read;
    use std::os::unix::fs::OpenOptionsExt;

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .with_context(|| format!("blob: opening {} without following symlinks", path.display()))?;
    let meta = file
        .metadata()
        .with_context(|| format!("blob: stat {}", path.display()))?;
    if !meta.file_type().is_file() {
        anyhow::bail!("blob: {} is not a regular file", path.display());
    }
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)
        .with_context(|| format!("reading {}", path.display()))?;
    Ok(buf)
}

/// Non-unix fallback: check `symlink_metadata` before reading. There is a
/// residual time-of-check/time-of-use window between the check and the read;
/// documented in `docs/obsidian.md`.
#[cfg(not(unix))]
fn read_local_no_follow(path: &Path) -> Result<Vec<u8>> {
    let meta = std::fs::symlink_metadata(path)
        .with_context(|| format!("blob: stat {} without following symlinks", path.display()))?;
    if meta.file_type().is_symlink() {
        anyhow::bail!(
            "blob: opening {} without following symlinks: is a symlink",
            path.display()
        );
    }
    if !meta.file_type().is_file() {
        anyhow::bail!("blob: {} is not a regular file", path.display());
    }
    std::fs::read(path).with_context(|| format!("reading {}", path.display()))
}
```

`list_remote` returns `Result<Vec<BlobEntry>>`; its two `push_remote_entry` calls pass `&meta` instead of `meta.location.as_ref()`; the sort becomes `out.sort_by(|a, b| a.rel_key.cmp(&b.rel_key));`. Replace `push_remote_entry`:

```rust
/// Append one S3 object to the accumulator, stripping `norm` to the rel key.
/// Skips the zero-length "folder marker" (rel == "") and, when `single_level`,
/// any nested key that slipped through.
fn push_remote_entry(
    out: &mut Vec<BlobEntry>,
    bucket: &str,
    norm: &str,
    meta: &ObjectMeta,
    single_level: bool,
) {
    let full_key: &str = meta.location.as_ref();
    let rel_key = full_key.strip_prefix(norm).unwrap_or(full_key).to_string();
    if rel_key.is_empty() || (single_level && rel_key.contains('/')) {
        return;
    }
    out.push(BlobEntry {
        loc: Loc::S3 {
            bucket: bucket.to_string(),
            key: full_key.to_string(),
        },
        rel_key,
        size: meta.size,
        modified: meta.last_modified,
    });
}
```

- [ ] **Step 5: Adapt the existing tests and add the new ones**

In `local_backend_list_get_put_roundtrip`: every `store.list(&prefix, true)` becomes `store.list(&prefix, ListOptions { recursive: true, follow_symlinks: true })` (and `false` for the flat one); `.map(|(_, r)| r.as_str())` becomes `.map(|e| e.rel_key.as_str())`; the `get` lines become:

```rust
        let top = listed.iter().find(|e| e.rel_key == "top.pdf").unwrap();
        assert_eq!(
            store.get(&top.loc, ReadOptions { follow_symlinks: true }).await.unwrap(),
            b"TOP"
        );
```

`local_backend_missing_root_errors`: `store.list(&prefix, ListOptions { recursive: true, follow_symlinks: false })`. `remote_backend_prefix_is_directory_scoped`: same `ListOptions` change, `.map(|e| e.rel_key.as_str())`, and `&listed[0].0` → `&listed[0].loc`. `remote_backend_get_put_roundtrip`: `blob.get(&src, ReadOptions { follow_symlinks: false })` (both calls). Then append:

```rust
    #[tokio::test]
    async fn local_list_carries_size_and_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("a.md");
        std::fs::write(&file, b"hello").unwrap();
        let meta = std::fs::metadata(&file).unwrap();

        let listed = BlobStore::Local
            .list(
                &Loc::Local(dir.path().to_path_buf()),
                ListOptions { recursive: true, follow_symlinks: false },
            )
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].rel_key, "a.md");
        assert_eq!(listed[0].size, 5);
        assert_eq!(listed[0].modified, DateTime::<Utc>::from(meta.modified().unwrap()));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_list_skips_symlinks_unless_followed() {
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret.md"), b"S").unwrap();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("real.md"), b"R").unwrap();
        std::os::unix::fs::symlink(outside.path().join("secret.md"), dir.path().join("link.md"))
            .unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("linkdir")).unwrap();
        let prefix = Loc::Local(dir.path().to_path_buf());

        let strict = BlobStore::Local
            .list(&prefix, ListOptions { recursive: true, follow_symlinks: false })
            .await
            .unwrap();
        let rels: Vec<&str> = strict.iter().map(|e| e.rel_key.as_str()).collect();
        assert_eq!(rels, vec!["real.md"]);

        let followed = BlobStore::Local
            .list(&prefix, ListOptions { recursive: true, follow_symlinks: true })
            .await
            .unwrap();
        let rels: Vec<&str> = followed.iter().map(|e| e.rel_key.as_str()).collect();
        assert_eq!(rels, vec!["link.md", "linkdir/secret.md", "real.md"]);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_get_refuses_symlink_unless_followed() {
        let outside = tempfile::tempdir().unwrap();
        let target = outside.path().join("secret.md");
        std::fs::write(&target, b"S").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let link = dir.path().join("link.md");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let loc = Loc::Local(link);

        let err = BlobStore::Local
            .get(&loc, ReadOptions { follow_symlinks: false })
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("without following symlinks"),
            "unexpected: {err:#}"
        );
        assert_eq!(
            BlobStore::Local
                .get(&loc, ReadOptions { follow_symlinks: true })
                .await
                .unwrap(),
            b"S"
        );
    }

    #[tokio::test]
    async fn remote_list_carries_object_metadata() {
        let store = seed_inmemory(&[("corpus/a.pdf", b"HELLO")]).await;
        let blob = BlobStore::Remote(store);
        let listed = blob
            .list(
                &Loc::S3 { bucket: "bk".into(), key: "corpus".into() },
                ListOptions { recursive: true, follow_symlinks: false },
            )
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].size, 5);
        assert!(listed[0].modified > DateTime::<Utc>::from(std::time::UNIX_EPOCH));
    }
```

- [ ] **Step 6: Adapt `documents/parse.rs` and `llm_extract`**

`parse.rs:16` → `use crate::sources::providers::blob::{BlobEntry, BlobStore, ListOptions, Loc, ReadOptions};`

`list_docs` (~line 236) returns `Result<Vec<BlobEntry>>`; body:

```rust
    let entries = store
        .list(prefix, ListOptions { recursive: opts.recursive, follow_symlinks: true })
        .await?;
    Ok(entries
        .into_iter()
        .filter(|entry| {
            let name = entry.rel_key.rsplit('/').next().unwrap_or(&entry.rel_key);
            if !matches_globs(name, &opts.include_globs) {
                return false;
            }
            if let Some(base) = image_store {
                if loc_is_under(&entry.loc, base) {
                    return false;
                }
            }
            true
        })
        .collect())
```

The loop at ~line 620:

```rust
    for BlobEntry { loc, rel_key: rel_path, .. } in entries {
        let bytes = match runtime.block_on(read_store.get(&loc, ReadOptions { follow_symlinks: true })) {
```

Tests at ~1069/1081/1109/1117: `.map(|(_, r)| r)` → `.map(|e| e.rel_key)`.

`llm_extract/mod.rs:656` → `use crate::sources::providers::blob::{BlobStore, ReadOptions};` and line 663 → `store.get(&loc, ReadOptions { follow_symlinks: true }).await`. Because `blob` is now compiled under either feature, change line 655 `#[cfg(feature = "documents")]` to `#[cfg(any(feature = "documents", feature = "obsidian"))]` and line 671 `#[cfg(not(feature = "documents"))]` to `#[cfg(not(any(feature = "documents", feature = "obsidian")))]`. Reword the error string in that `not` arm to: `"cannot fetch image_ref '{image_ref}': s3:// refs require the `documents` or `obsidian` Cargo feature (either provides the S3 client). Rebuild with --features documents, or use a local image_store."` (keep the two-segment `\` string continuation style).

- [ ] **Step 7: Format and checkpoint**

```bash
cargo fmt --all
```

CI expectation: `cargo check --all` green with `--features documents`, `--features obsidian` (once Task 2 lands the module; at this checkpoint `obsidian` merely enables `blob`), and no features; `blob` tests (renamed paths) and `documents` tests pass unchanged in behavior.

Checkpoint — suggested commit: `refactor(blob): lift documents/blob.rs to providers/blob.rs; ListOptions/ReadOptions/BlobEntry`.

---

### Task 2: `obsidian` module skeleton, errors, and `ScanOptions`

**Files:**
- Create: `crates/skardi/src/sources/providers/obsidian/mod.rs`
- Create: `crates/skardi/src/sources/providers/obsidian/config.rs`
- Modify: `crates/skardi/src/sources/providers/mod.rs` (add `pub mod obsidian;` under `#[cfg(feature = "obsidian")]`)
- Test: unit tests inside `config.rs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  ```rust
  pub const OBSIDIAN_SURFACE_VERSION: u32 = 1;
  pub const OBSIDIAN_SCHEMA: &str = "main";
  pub const NOTES_TABLE: &str = "notes"; pub const LINKS_TABLE: &str = "links"; pub const TAGS_TABLE: &str = "tags";
  pub enum ObsidianError { CatalogHierarchyRequired{name}, ReadWriteNotSupported{name}, InvalidOptions{name, reason}, RootUnavailable{name, path, cause}, RootNotDirectory{name, path} }
  // config.rs
  pub const DEFAULT_EXCLUDE_GLOBS: &str = ".obsidian/**,.trash/**";
  pub const DEFAULT_MAX_FILE_BYTES: u64 = 16 * 1024 * 1024;
  pub enum OptionsError { UnknownKey{key}, InvalidGlob{glob, message}, InvalidMaxFileBytes{value} }
  pub struct ScanOptions { /* private */ pub max_file_bytes: u64 }
  impl ScanOptions {
      pub fn from_map(options: Option<&HashMap<String, String>>) -> Result<Self, OptionsError>;
      pub fn new(exclude_globs: Vec<String>, max_file_bytes: u64) -> Result<Self, OptionsError>;
      pub fn exclude_globs(&self) -> &[String];
      pub fn is_excluded(&self, rel_path: &str) -> bool;
  }
  ```

- [ ] **Step 1: `mod.rs` skeleton**

Create `crates/skardi/src/sources/providers/obsidian/mod.rs`. Submodule lines for later tasks are added by those tasks; start with only `config`:

```rust
//! `obsidian` data source connector: one Obsidian vault as three read-only
//! catalog tables — `<name>.main.notes`, `<name>.main.links`,
//! `<name>.main.tags`. Everything here is behind the `obsidian` Cargo feature.
//!
//! Design: `docs/superpowers/specs/2026-09-02-obsidian-source-design.md`.
//! The decisions that shape this module, with their reasons:
//!
//! - **Rescan on every query, no cache.** A vault is thousands of small files;
//!   a full list + read + parse is tens to hundreds of milliseconds locally,
//!   and a cache would introduce the stale-row bug class the design most
//!   wants to avoid. A query joining two of the tables parses the vault twice.
//! - **Whole scan off the Tokio worker.** `scan::VaultScan::run` is
//!   synchronous and runs inside `tokio::task::spawn_blocking`; the `BlobStore`
//!   is resolved inside that task so any S3 client lives on one runtime.
//! - **Frontmatter as JSON, `aliases` lifted.** Frontmatter is schemaless per
//!   note; only `aliases` has semantics Obsidian itself defines.
//! - **Links resolved like Obsidian, never through aliases.** `[[Alias]]` is
//!   `missing`, exactly as Obsidian treats it; the alias-repair query in
//!   `docs/obsidian.md` recovers the intent without misstating the graph.
//! - **Frontmatter links count.** Every string value in the parsed frontmatter
//!   is scanned for `[[…]]` (`links.source = 'frontmatter'`, `line` NULL).
//! - **No symlinks, ever.** Listing skips them and reads refuse them
//!   (`O_NOFOLLOW`), because `path: ~/vault` must not read outside the vault.
//! - **Size cap from listing metadata.** `max_file_bytes` is enforced before
//!   any read so a huge object is never buffered.
//! - **Wholesale-failure guard.** A non-empty listing where every attempted
//!   read fails is an error naming the root, never three empty tables.

pub mod config;

/// Surface generation of the three schemas (Arrow metadata
/// `skardi.obsidian.surface_version`). Bump on any incompatible change.
pub const OBSIDIAN_SURFACE_VERSION: u32 = 1;

/// The one schema every obsidian catalog exposes.
pub const OBSIDIAN_SCHEMA: &str = "main";
pub const NOTES_TABLE: &str = "notes";
pub const LINKS_TABLE: &str = "links";
pub const TAGS_TABLE: &str = "tags";

/// Registration-time failures. Each names the source and the offending field
/// or path; none carries file contents.
#[derive(Debug, thiserror::Error)]
pub enum ObsidianError {
    #[error("obsidian source '{name}': hierarchy_level must be `catalog`")]
    CatalogHierarchyRequired { name: String },
    #[error(
        "obsidian source '{name}': access_mode `read_write` is not supported (the source is read-only)"
    )]
    ReadWriteNotSupported { name: String },
    #[error("obsidian source '{name}': invalid options: {reason}")]
    InvalidOptions { name: String, reason: String },
    #[error("obsidian source '{name}': vault root {path} is unavailable: {cause}")]
    RootUnavailable {
        name: String,
        path: String,
        cause: String,
    },
    #[error("obsidian source '{name}': vault root {path} is not a directory")]
    RootNotDirectory { name: String, path: String },
}
```

In `crates/skardi/src/sources/providers/mod.rs`, after `pub mod mysql_wire;` add:

```rust
#[cfg(feature = "obsidian")]
pub mod obsidian;
```

- [ ] **Step 2: Write the failing tests for `ScanOptions`**

Create `crates/skardi/src/sources/providers/obsidian/config.rs` with only the test module first:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn defaults_when_no_options() {
        let opts = ScanOptions::from_map(None).unwrap();
        assert_eq!(opts.exclude_globs(), &[".obsidian/**", ".trash/**"]);
        assert_eq!(opts.max_file_bytes, 16_777_216);
        assert!(opts.is_excluded(".obsidian/app.json"));
        assert!(opts.is_excluded(".trash/Deleted.md"));
        assert!(!opts.is_excluded("Projects/Design.md"));
    }

    #[test]
    fn exclusion_is_case_insensitive_on_the_relative_path() {
        let opts = ScanOptions::from_map(None).unwrap();
        assert!(opts.is_excluded(".Obsidian/App.json"));
        assert!(opts.is_excluded(".TRASH/x.md"));
    }

    #[test]
    fn custom_exclude_globs_replace_the_default() {
        let m = map(&[("exclude_globs", " templates/** , drafts/*.md ")]);
        let opts = ScanOptions::from_map(Some(&m)).unwrap();
        assert_eq!(opts.exclude_globs(), &["templates/**", "drafts/*.md"]);
        assert!(opts.is_excluded("templates/daily.md"));
        assert!(opts.is_excluded("drafts/a.md"));
        // The default is gone, as the spec says ("replaces the default").
        assert!(!opts.is_excluded(".obsidian/app.json"));
    }

    #[test]
    fn empty_exclude_globs_excludes_nothing() {
        let m = map(&[("exclude_globs", "")]);
        let opts = ScanOptions::from_map(Some(&m)).unwrap();
        assert!(opts.exclude_globs().is_empty());
        assert!(!opts.is_excluded(".obsidian/app.json"));
    }

    #[test]
    fn max_file_bytes_parses_and_rejects_garbage() {
        let m = map(&[("max_file_bytes", " 2048 ")]);
        assert_eq!(ScanOptions::from_map(Some(&m)).unwrap().max_file_bytes, 2048);
        for bad in ["0", "-1", "abc", "1.5", ""] {
            let m = map(&[("max_file_bytes", bad)]);
            assert_eq!(
                ScanOptions::from_map(Some(&m)).unwrap_err(),
                OptionsError::InvalidMaxFileBytes { value: bad.trim().to_string() },
                "value {bad:?}"
            );
        }
    }

    #[test]
    fn unknown_key_is_rejected_and_the_first_sorted_key_is_named() {
        let m = map(&[("zeta", "1"), ("exclude_glob", "x"), ("max_file_bytes", "1")]);
        assert_eq!(
            ScanOptions::from_map(Some(&m)).unwrap_err(),
            OptionsError::UnknownKey { key: "exclude_glob".to_string() }
        );
    }

    #[test]
    fn invalid_glob_is_rejected() {
        let m = map(&[("exclude_globs", "[unclosed")]);
        assert!(matches!(
            ScanOptions::from_map(Some(&m)).unwrap_err(),
            OptionsError::InvalidGlob { glob, .. } if glob == "[unclosed"
        ));
    }

    #[test]
    fn errors_display_the_offending_value() {
        let e = OptionsError::UnknownKey { key: "foo".into() };
        assert_eq!(
            e.to_string(),
            "unknown option `foo` (supported: exclude_globs, max_file_bytes)"
        );
    }
}
```

- [ ] **Step 3: Implement `ScanOptions`**

Above the test module:

```rust
//! Scan options parsed from the flat `options` map of a `type: obsidian`
//! source. Two keys only; anything else is a registration error so a typo
//! (`exclude_glob`) can never silently disable an exclusion.

use std::collections::HashMap;

use glob::{MatchOptions, Pattern};

/// Default `exclude_globs`: Obsidian's own config and its trash.
pub const DEFAULT_EXCLUDE_GLOBS: &str = ".obsidian/**,.trash/**";
/// Default `max_file_bytes`: 16 MiB.
pub const DEFAULT_MAX_FILE_BYTES: u64 = 16 * 1024 * 1024;

/// Why an `options` map was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OptionsError {
    #[error("unknown option `{key}` (supported: exclude_globs, max_file_bytes)")]
    UnknownKey { key: String },
    #[error("exclude_globs entry `{glob}` is not a valid glob: {message}")]
    InvalidGlob { glob: String, message: String },
    #[error("max_file_bytes must be a positive integer, got `{value}`")]
    InvalidMaxFileBytes { value: String },
}

/// Parsed, validated scan options.
#[derive(Debug, Clone)]
pub struct ScanOptions {
    exclude_globs: Vec<String>,
    patterns: Vec<Pattern>,
    /// Files whose listed size exceeds this are skipped before any read.
    pub max_file_bytes: u64,
}

/// Globs match the forward-slash relative path case-insensitively, as in
/// `documents`; `**` may span separators and a leading `.` needs no literal.
const MATCH_OPTIONS: MatchOptions = MatchOptions {
    case_sensitive: false,
    require_literal_separator: false,
    require_literal_leading_dot: false,
};

impl ScanOptions {
    /// Parse the flat `options` map. Keys are visited in sorted order so the
    /// error for several unknown keys is deterministic. `exclude_globs`
    /// *replaces* the default when present (an empty string excludes nothing).
    pub fn from_map(options: Option<&HashMap<String, String>>) -> Result<Self, OptionsError> {
        let mut exclude = DEFAULT_EXCLUDE_GLOBS.to_string();
        let mut max_file_bytes = DEFAULT_MAX_FILE_BYTES;
        if let Some(map) = options {
            let mut pairs: Vec<(&String, &String)> = map.iter().collect();
            pairs.sort();
            for (key, value) in pairs {
                let value = value.trim();
                match key.as_str() {
                    "exclude_globs" => exclude = value.to_string(),
                    "max_file_bytes" => {
                        max_file_bytes = value
                            .parse::<u64>()
                            .ok()
                            .filter(|n| *n > 0)
                            .ok_or_else(|| OptionsError::InvalidMaxFileBytes {
                                value: value.to_string(),
                            })?;
                    }
                    other => {
                        return Err(OptionsError::UnknownKey {
                            key: other.to_string(),
                        });
                    }
                }
            }
        }
        let globs = exclude
            .split(',')
            .map(str::trim)
            .filter(|g| !g.is_empty())
            .map(str::to_string)
            .collect();
        Self::new(globs, max_file_bytes)
    }

    /// Build from already-split globs (tests and embedders).
    pub fn new(exclude_globs: Vec<String>, max_file_bytes: u64) -> Result<Self, OptionsError> {
        let mut patterns = Vec::with_capacity(exclude_globs.len());
        for glob in &exclude_globs {
            let pattern = Pattern::new(glob).map_err(|e| OptionsError::InvalidGlob {
                glob: glob.clone(),
                message: e.msg.to_string(),
            })?;
            patterns.push(pattern);
        }
        Ok(Self {
            exclude_globs,
            patterns,
            max_file_bytes,
        })
    }

    /// The globs in effect, as written (trimmed).
    pub fn exclude_globs(&self) -> &[String] {
        &self.exclude_globs
    }

    /// Whether a `/`-separated vault-relative path matches any exclude glob.
    pub fn is_excluded(&self, rel_path: &str) -> bool {
        self.patterns
            .iter()
            .any(|p| p.matches_with(rel_path, MATCH_OPTIONS))
    }
}
```

- [ ] **Step 4: Format and checkpoint**

```bash
cargo fmt --all
```

CI expectation: `--features obsidian` compiles; the eight `config` tests pass.

Checkpoint — suggested commit: `feat(obsidian): module skeleton, ObsidianError, ScanOptions`.

---

### Task 3: `markdown.rs` — tags and raw links from a note body

**Files:**
- Create: `crates/skardi/src/sources/providers/obsidian/markdown.rs`
- Modify: `crates/skardi/src/sources/providers/obsidian/mod.rs` (add `pub mod markdown;`)
- Test: unit tests inside `markdown.rs`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces (used by Tasks 4, 5, 6):
  ```rust
  pub enum LinkSyntax { Wikilink, Markdown, Autolink }
  pub struct RawLink { pub syntax: LinkSyntax, pub embed: bool, pub target: String, pub heading: Option<String>, pub block_id: Option<String>, pub display_text: Option<String>, pub line: Option<u32> }
  pub struct Extracted { pub tags: Vec<(String, u32)>, pub links: Vec<RawLink> }
  pub fn extract(body: &str, body_first_line: u32) -> Extracted;
  pub fn find_wikilinks(text: &str) -> Vec<(usize, RawLink)>;   // (byte offset, link with line = None)
  pub fn has_url_scheme(target: &str) -> bool;
  pub struct LineIndex; impl LineIndex { pub fn new(text: &str) -> Self; pub fn line_of(&self, offset: usize) -> u32 /* 1-based */ }
  ```

**Approach (read before coding):** `pulldown-cmark` is used for two things only — the byte ranges of fenced/indented code blocks and inline code spans, and the `Link`/`Image` events (Markdown links, images, autolinks). Wikilinks and tags are Obsidian extensions no CommonMark parser knows, so they are found by regex over a *masked* copy of the body in which every byte inside a code range is replaced by a space (offsets preserved, so line numbers are exact). Do **not** enable `Options::ENABLE_WIKILINKS`; Obsidian's `|display` and `#^block` parts differ from pulldown's dialect.

- [ ] **Step 1: Write the failing tests**

Create the file with this test module (the implementation goes above it in Step 2):

```rust
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
            vec![RawLink { display_text: Some("Shown".into()), ..wl("Note") }]
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
            vec![RawLink { block_id: Some("abc123".into()), ..wl("Meeting") }]
        );
        assert_eq!(
            links("![[attachments/diagram.png]]"),
            vec![RawLink { embed: true, ..wl("attachments/diagram.png") }]
        );
        assert_eq!(links("[[ Folder/Some Note ]]"), vec![wl("Folder/Some Note")]);
        assert_eq!(links("[[笔记]]"), vec![wl("笔记")]);
        assert_eq!(links("[[A]][[B]]"), vec![wl("A"), wl("B")]);
        assert_eq!(
            links("[[#Goals]]"),
            vec![RawLink { heading: Some("Goals".into()), ..wl("") }]
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
        assert_eq!(links("[t](Projects/Design.md)"), vec![md("Projects/Design.md")]);
        assert_eq!(
            links("![t](attachments/diagram.png)"),
            vec![RawLink { embed: true, ..md("attachments/diagram.png") }]
        );
        assert_eq!(
            links("[t](Note.md#Some%20Heading)"),
            vec![RawLink { heading: Some("Some Heading".into()), ..md("Note.md") }]
        );
        assert_eq!(
            links("[t](Note.md#^blk)"),
            vec![RawLink { block_id: Some("blk".into()), ..md("Note.md") }]
        );
        // Split at the literal `#` first, decode second: `%23` stays in the name.
        assert_eq!(links("[t](foo%23bar.md)"), vec![md("foo#bar.md")]);
        assert_eq!(links("[t](Some%20Note.md)"), vec![md("Some Note.md")]);
        assert_eq!(links("[t](<Some Note.md>)"), vec![md("Some Note.md")]);
        // Same-note link: empty path, heading kept.
        assert_eq!(
            links("[t](#Goals)"),
            vec![RawLink { heading: Some("Goals".into()), ..md("") }]
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
        assert_eq!(tags("see #alpha, then #beta."), vec![("alpha".into(), 1), ("beta".into(), 1)]);
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
```

- [ ] **Step 2: Implement `markdown.rs`**

```rust
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
    if has_url_scheme(&open.dest) {
        return RawLink {
            syntax,
            embed: open.image,
            target: open.dest,
            heading: None,
            block_id: None,
            display_text,
            line: None,
        };
    }
    // Split at the first LITERAL `#` first, then decode the two halves
    // independently, so `foo%23bar.md` stays the file name `foo#bar.md`.
    let (path, fragment) = match open.dest.split_once('#') {
        Some((p, f)) => (p, Some(f)),
        None => (open.dest.as_str(), None),
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
/// and the result is valid UTF-8.
fn mask_ranges(body: &str, ranges: &[Range<usize>]) -> String {
    let mut out = String::with_capacity(body.len());
    for (idx, ch) in body.char_indices() {
        if ranges.iter().any(|r| r.contains(&idx)) {
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
    options.insert(Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS);

    let mut code_ranges: Vec<Range<usize>> = Vec::new();
    let mut found: Vec<(usize, RawLink)> = Vec::new();
    let mut open: Vec<OpenLink> = Vec::new();
    for (event, range) in Parser::new_ext(body, options).into_offset_iter() {
        match event {
            // Start/End ranges of a container cover the whole element.
            Event::Start(Tag::CodeBlock(_)) | Event::End(TagEnd::CodeBlock) => {
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
```

Add `pub mod markdown;` to `obsidian/mod.rs` (alphabetical: after `config`).

- [ ] **Step 3: Format and checkpoint**

```bash
cargo fmt --all
```

CI expectation: all `markdown` tests pass. If `code_is_not_scanned` fails on the fenced block because the `Start(Tag::CodeBlock)` range does not cover the whole block on this `pulldown-cmark` version, the `End(TagEnd::CodeBlock)` arm already pushes the same range; if *both* are narrow, track `code_start = Some(range.start)` on `Start` and push `code_start..range.end` on `End` instead.

Checkpoint — suggested commit: `feat(obsidian): markdown.rs — tags, wikilinks, Markdown links with source lines`.

---

### Task 4: `frontmatter.rs` — split, YAML→JSON, aliases, tags, links

**Files:**
- Create: `crates/skardi/src/sources/providers/obsidian/frontmatter.rs`
- Modify: `crates/skardi/src/sources/providers/obsidian/mod.rs` (add `pub mod frontmatter;`)
- Test: unit tests inside `frontmatter.rs`

**Interfaces:**
- Consumes: `markdown::{RawLink, find_wikilinks}` (Task 3).
- Produces (used by Task 6):
  ```rust
  pub struct Split<'a> { pub yaml: Option<&'a str>, pub body: &'a str, pub body_first_line: u32 }
  pub fn split(text: &str) -> Split<'_>;
  pub fn parse(yaml: &str) -> Result<serde_json::Value, String>;   // always a JSON object on Ok
  pub fn aliases(frontmatter: &serde_json::Value) -> Option<Vec<String>>;
  pub fn tags(frontmatter: &serde_json::Value) -> Vec<String>;      // '#' stripped, deduped, document order
  pub fn links(frontmatter: &serde_json::Value) -> Vec<RawLink>;    // line = None, document order
  ```

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn split_recognizes_a_block_only_at_line_one() {
        let s = split("---\ntitle: x\ntags: [a]\n---\nbody line\n");
        assert_eq!(s.yaml, Some("title: x\ntags: [a]\n"));
        assert_eq!(s.body, "body line\n");
        assert_eq!(s.body_first_line, 5);

        let s = split("intro\n---\nnot: frontmatter\n---\n");
        assert_eq!(s.yaml, None);
        assert_eq!(s.body_first_line, 1);
        assert!(s.body.starts_with("intro"));

        let s = split("no block at all");
        assert_eq!(s, Split { yaml: None, body: "no block at all", body_first_line: 1 });
    }

    #[test]
    fn split_accepts_dots_terminator_crlf_bom_and_empty_block() {
        let s = split("---\na: 1\n...\nbody");
        assert_eq!(s.yaml, Some("a: 1\n"));
        assert_eq!(s.body, "body");
        assert_eq!(s.body_first_line, 4);

        let s = split("---\r\na: 1\r\n---\r\nbody\r\n");
        assert_eq!(s.yaml, Some("a: 1\r\n"));
        assert_eq!(s.body, "body\r\n");
        assert_eq!(s.body_first_line, 4);

        let s = split("\u{feff}---\na: 1\n---\nbody");
        assert_eq!(s.yaml, Some("a: 1\n"));
        assert_eq!(s.body, "body");

        let s = split("---\n---\nbody");
        assert_eq!(s.yaml, Some(""));
        assert_eq!(s.body, "body");
        assert_eq!(s.body_first_line, 3);
    }

    #[test]
    fn split_rejects_unterminated_and_inexact_fences() {
        let s = split("---\na: 1\nbody without closing fence");
        assert_eq!(s.yaml, None);
        assert_eq!(s.body_first_line, 1);
        // "--- " (trailing space) is not exactly "---".
        let s = split("--- \na: 1\n---\nbody");
        assert_eq!(s.yaml, None);
        // A `---` at line 1 followed by a `----` is not closed either.
        let s = split("---\na: 1\n----\nbody");
        assert_eq!(s.yaml, None);
    }

    #[test]
    fn parse_preserves_order_and_stringifies_odd_keys() {
        let v = parse("zeta: 1\nalpha: [x, y]\nnested:\n  room: B12\n1: one\ntrue: yes\n").unwrap();
        assert_eq!(
            serde_json::to_string(&v).unwrap(),
            r#"{"zeta":1,"alpha":["x","y"],"nested":{"room":"B12"},"1":"one","true":"yes"}"#
        );
    }

    #[test]
    fn parse_handles_empty_null_tagged_and_floats() {
        assert_eq!(parse("").unwrap(), json!({}));
        assert_eq!(parse("   \n").unwrap(), json!({}));
        assert_eq!(parse("k: !custom value").unwrap(), json!({"k": "value"}));
        assert_eq!(parse("f: 1.5\nn: -3\nb: false\nz: ~").unwrap(), json!({"f": 1.5, "n": -3, "b": false, "z": null}));
    }

    #[test]
    fn parse_reports_malformed_and_non_mapping() {
        let err = parse("title: [unclosed").unwrap_err();
        assert!(err.contains("line"), "should carry a position: {err}");
        let err = parse("- a\n- b").unwrap_err();
        assert!(err.contains("not a mapping"), "{err}");
        let err = parse("just a scalar").unwrap_err();
        assert!(err.contains("not a mapping"), "{err}");
    }

    #[test]
    fn aliases_scalar_list_or_null() {
        assert_eq!(aliases(&json!({"aliases": "Standup"})), Some(vec!["Standup".to_string()]));
        assert_eq!(
            aliases(&json!({"aliases": ["Start", " Landing ", 7]})),
            Some(vec!["Start".to_string(), "Landing".to_string()])
        );
        assert_eq!(aliases(&json!({"aliases": 42})), None);
        assert_eq!(aliases(&json!({"aliases": {"a": 1}})), None);
        assert_eq!(aliases(&json!({"title": "x"})), None);
    }

    #[test]
    fn tags_from_list_string_and_tag_key() {
        assert_eq!(tags(&json!({"tags": ["index", "project/skardi"]})), vec!["index", "project/skardi"]);
        assert_eq!(tags(&json!({"tags": "draft, design"})), vec!["draft", "design"]);
        assert_eq!(tags(&json!({"tags": "#a  b\tc"})), vec!["a", "b", "c"]);
        assert_eq!(tags(&json!({"tag": "solo"})), vec!["solo"]);
        // Both keys contribute; duplicates collapse; non-strings are ignored.
        assert_eq!(tags(&json!({"tags": ["a", "#b", 3], "tag": "b, c"})), vec!["a", "b", "c"]);
        assert!(tags(&json!({"title": "x"})).is_empty());
        assert!(tags(&json!({"tags": ["", "#"]})).is_empty());
    }

    #[test]
    fn links_walk_every_string_in_document_order() {
        let fm = parse(
            "related: \"[[Projects/Design]]\"\n\
             attendees:\n  - \"[[People/Alice]]\"\n  - \"[[People/Bob|Bob]]\"\n\
             location:\n  room: \"[[Rooms/B12#Layout|Room]]\"\n\
             raw: [[Home]]\n\
             md: \"[Home](Home.md)\"\n\
             aliases: [\"[[Alias Link]]\"]\n",
        )
        .unwrap();
        let got = links(&fm);
        let targets: Vec<&str> = got.iter().map(|l| l.target.as_str()).collect();
        // `raw: [[Home]]` is a nested YAML list containing "Home" — no link,
        // as in Obsidian; `md:` is plain text in a property.
        assert_eq!(
            targets,
            vec!["Projects/Design", "People/Alice", "People/Bob", "Rooms/B12", "Alias Link"]
        );
        assert_eq!(got[2].display_text.as_deref(), Some("Bob"));
        assert_eq!(got[3].heading.as_deref(), Some("Layout"));
        assert_eq!(got[3].display_text.as_deref(), Some("Room"));
        assert!(got.iter().all(|l| l.line.is_none()));
    }
}
```

- [ ] **Step 2: Implement `frontmatter.rs`**

```rust
//! Frontmatter: locate the `---` block, parse it to JSON, and lift the three
//! things the tables need from it — `aliases`, `tags`/`tag`, and `[[…]]`
//! links inside string values.

use serde_json::{Map, Value as Json};
use serde_yaml::Value as Yaml;

use super::markdown::{RawLink, find_wikilinks};

/// A note split into its frontmatter YAML (without the fences) and body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Split<'a> {
    /// `None` when line 1 is not exactly `---` or the block is never closed.
    pub yaml: Option<&'a str>,
    /// Everything after the closing fence line; the whole text otherwise.
    pub body: &'a str,
    /// 1-based source line on which `body` begins (closing fence line + 1;
    /// 1 when there is no block).
    pub body_first_line: u32,
}

/// Recognize a frontmatter block: line 1 exactly `---`, closed by a later
/// line exactly `---` or `...`. CRLF line endings and a leading BOM are
/// tolerated; anything else (trailing spaces on a fence, `----`) is body.
pub fn split(text: &str) -> Split<'_> {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let no_block = Split {
        yaml: None,
        body: text,
        body_first_line: 1,
    };
    let mut lines = text.split_inclusive('\n');
    let Some(first) = lines.next() else {
        return no_block;
    };
    if first.trim_end_matches(['\r', '\n']) != "---" {
        return no_block;
    }
    let yaml_start = first.len();
    let mut offset = yaml_start;
    let mut line_no: u32 = 1;
    for line in lines {
        line_no += 1;
        let fence = line.trim_end_matches(['\r', '\n']);
        if fence == "---" || fence == "..." {
            return Split {
                yaml: Some(&text[yaml_start..offset]),
                body: &text[offset + line.len()..],
                body_first_line: line_no + 1,
            };
        }
        offset += line.len();
    }
    no_block
}

/// Parse a frontmatter block to a JSON object. Empty/null YAML is `{}`; a
/// non-mapping document is an error; YAML-only features are stringified
/// (non-string keys) or unwrapped (tags). The error string is the parser's
/// message, which carries line/column.
pub fn parse(yaml: &str) -> Result<Json, String> {
    if yaml.trim().is_empty() {
        return Ok(Json::Object(Map::new()));
    }
    let value: Yaml = serde_yaml::from_str(yaml).map_err(|e| e.to_string())?;
    match yaml_to_json(value) {
        Json::Null => Ok(Json::Object(Map::new())),
        object @ Json::Object(_) => Ok(object),
        other => Err(format!(
            "frontmatter is not a mapping (found {})",
            json_kind(&other)
        )),
    }
}

fn json_kind(value: &Json) -> &'static str {
    match value {
        Json::Null => "null",
        Json::Bool(_) => "boolean",
        Json::Number(_) => "number",
        Json::String(_) => "string",
        Json::Array(_) => "sequence",
        Json::Object(_) => "mapping",
    }
}

fn yaml_to_json(value: Yaml) -> Json {
    match value {
        Yaml::Null => Json::Null,
        Yaml::Bool(b) => Json::Bool(b),
        Yaml::Number(n) => {
            if let Some(i) = n.as_i64() {
                Json::from(i)
            } else if let Some(u) = n.as_u64() {
                Json::from(u)
            } else {
                n.as_f64()
                    .and_then(serde_json::Number::from_f64)
                    .map(Json::Number)
                    .unwrap_or_else(|| Json::String(n.to_string()))
            }
        }
        Yaml::String(s) => Json::String(s),
        Yaml::Sequence(items) => Json::Array(items.into_iter().map(yaml_to_json).collect()),
        Yaml::Mapping(map) => {
            let mut out = Map::new();
            for (key, val) in map {
                out.insert(key_to_string(key), yaml_to_json(val));
            }
            Json::Object(out)
        }
        Yaml::Tagged(tagged) => yaml_to_json(tagged.value),
    }
}

/// JSON keys must be strings; YAML allows anything.
fn key_to_string(key: Yaml) -> String {
    match key {
        Yaml::String(s) => s,
        Yaml::Null => "null".to_string(),
        Yaml::Bool(b) => b.to_string(),
        Yaml::Number(n) => n.to_string(),
        other => serde_yaml::to_string(&other)
            .map(|s| s.trim_end().to_string())
            .unwrap_or_default(),
    }
}

/// `aliases:` — a string becomes a one-element list; a list keeps its string
/// items (trimmed, empties dropped); any other shape is `None`.
pub fn aliases(frontmatter: &Json) -> Option<Vec<String>> {
    match frontmatter.get("aliases")? {
        Json::String(s) => Some(vec![s.trim().to_string()]),
        Json::Array(items) => Some(
            items
                .iter()
                .filter_map(Json::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect(),
        ),
        _ => None,
    }
}

/// `tags:` and `tag:` — a list of strings, or one string split on commas and
/// whitespace. Leading `#` stripped, duplicates collapsed, document order.
pub fn tags(frontmatter: &Json) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for key in ["tags", "tag"] {
        match frontmatter.get(key) {
            Some(Json::String(s)) => {
                for part in s.split(|c: char| c == ',' || c.is_whitespace()) {
                    push_tag(&mut out, part);
                }
            }
            Some(Json::Array(items)) => {
                for item in items.iter().filter_map(Json::as_str) {
                    push_tag(&mut out, item);
                }
            }
            _ => {}
        }
    }
    out
}

fn push_tag(out: &mut Vec<String>, raw: &str) {
    let tag = raw.trim().trim_start_matches('#');
    if tag.is_empty() || out.iter().any(|t| t == tag) {
        return;
    }
    out.push(tag.to_string());
}

/// Every `[[…]]` in every string value — top-level scalars, list elements,
/// and strings inside nested maps, in document order. Only the wikilink
/// syntax counts (Obsidian's rule for properties); `[text](target)` is text.
pub fn links(frontmatter: &Json) -> Vec<RawLink> {
    let mut out = Vec::new();
    walk_strings(frontmatter, &mut |s| {
        out.extend(find_wikilinks(s).into_iter().map(|(_, link)| link));
    });
    out
}

fn walk_strings<'a>(value: &'a Json, visit: &mut dyn FnMut(&'a str)) {
    match value {
        Json::String(s) => visit(s),
        Json::Array(items) => items.iter().for_each(|v| walk_strings(v, visit)),
        Json::Object(map) => map.values().for_each(|v| walk_strings(v, visit)),
        Json::Null | Json::Bool(_) | Json::Number(_) => {}
    }
}
```

Add `pub mod frontmatter;` to `obsidian/mod.rs`.

- [ ] **Step 3: Format and checkpoint**

```bash
cargo fmt --all
```

CI expectation: all `frontmatter` tests pass. If `parse_reports_malformed_and_non_mapping` fails because `serde_yaml`'s message for `[unclosed` lacks the word "line", loosen only that assertion to `!err.is_empty()` and note it in the commit.

Checkpoint — suggested commit: `feat(obsidian): frontmatter.rs — split, YAML→JSON, aliases/tags/links`.

---

### Task 5: `resolve.rs` — link resolution index

**Files:**
- Create: `crates/skardi/src/sources/providers/obsidian/resolve.rs`
- Modify: `crates/skardi/src/sources/providers/obsidian/mod.rs` (add `pub mod resolve;`)
- Test: unit tests inside `resolve.rs`

**Interfaces:**
- Consumes: `markdown::{LinkSyntax, RawLink, has_url_scheme}` (Task 3).
- Produces (used by Tasks 6, 7):
  ```rust
  pub enum Resolution { Exact, Name, Ambiguous, Missing, External }  impl Resolution { pub fn as_str(self) -> &'static str }
  pub enum LinkKind { Wikilink, Embed, Markdown, External }           impl LinkKind { pub fn as_str(self) -> &'static str }
  pub struct Resolved { pub to_path: Option<String>, pub kind: LinkKind, pub resolution: Resolution }
  pub struct Index;
  impl Index { pub fn build<S: AsRef<str>>(paths: &[S]) -> Self; pub fn resolve(&self, from_path: &str, link: &RawLink) -> Resolved; }
  ```

**Rules implemented (spec §Link Resolution plus deviation (a)):** matching is case-insensitive; `.md` is optional for notes; `.` and `..` are normalized and climbing above the root is `missing`; aliases never resolve. Wikilinks: empty target → self; `./`/`../` → relative to the note's folder, exact or missing; contains `/` → vault path, exact or missing; otherwise a bare name goes to the name index (`name` / `ambiguous` / `missing`) — except that a bare name containing a `.` first tries an exact root-level match (`[[Home.md]]` → `exact`; `[[Note v2.1]]` → `exact` when that file sits at the root, `name` when it sits in a folder; `[[diagram.png]]` → `name`). Markdown links: empty path → self; note-relative exact first; then, if the path contains `/`, vault path exact or missing; otherwise the name index.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    const PATHS: &[&str] = &[
        "Home.md",
        "Meeting.md",
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
        assert_eq!(r("Projects/Design.md", &wiki("./Notes")), ok("Projects/Notes.md", Resolution::Exact));
        assert_eq!(r("Projects/Design.md", &wiki("../Home")), ok("Home.md", Resolution::Exact));
        assert_eq!(r("Projects/Design.md", &wiki("../../Home")), (None, Resolution::Missing));
        assert_eq!(r("Projects/Design.md", &wiki("./Nope")), (None, Resolution::Missing));
        assert_eq!(r("Home.md", &wiki("Projects/Design")), ok("Projects/Design.md", Resolution::Exact));
        assert_eq!(r("Home.md", &wiki("projects/DESIGN.MD")), ok("Projects/Design.md", Resolution::Exact));
        assert_eq!(r("Home.md", &wiki("Projects/Nope")), (None, Resolution::Missing));
        assert_eq!(r("Home.md", &wiki("Design")), ok("Projects/Design.md", Resolution::Name));
        assert_eq!(r("Home.md", &wiki("design.MD")), ok("Projects/Design.md", Resolution::Name));
        // A bare root-level note is `name` (spec table); with its extension
        // written it is a vault path, hence `exact`.
        assert_eq!(r("Meeting.md", &wiki("Home")), ok("Home.md", Resolution::Name));
        assert_eq!(r("Meeting.md", &wiki("Home.md")), ok("Home.md", Resolution::Exact));
        assert_eq!(r("Home.md", &wiki("Notes")), (None, Resolution::Ambiguous));
        assert_eq!(r("Home.md", &wiki("Nowhere")), (None, Resolution::Missing));
        // Aliases never resolve.
        assert_eq!(r("Home.md", &wiki("Start")), (None, Resolution::Missing));
        // Attachments match by full file name.
        assert_eq!(r("Home.md", &wiki("diagram.png")), ok("attachments/diagram.png", Resolution::Name));
        // A dotted title that is a root-level file is exact; elsewhere it
        // falls through to the name index (plan deviation (a)).
        assert_eq!(r("Home.md", &wiki("Note v2.1")), ok("Note v2.1.md", Resolution::Exact));
        let nested = Index::build(&["Sub/Note v2.1.md"]);
        let res = nested.resolve("Home.md", &wiki("Note v2.1"));
        assert_eq!((res.to_path, res.resolution), ok("Sub/Note v2.1.md", Resolution::Name));
        // A colon with a space after it is not a URL scheme.
        assert_eq!(r("Home.md", &wiki("Note: subtitle")), (None, Resolution::Missing));
    }

    #[test]
    fn markdown_rows() {
        assert_eq!(r("Home.md", &md("")), ok("Home.md", Resolution::Exact)); // [t](#Heading)
        // Sibling first, even though a same-named note exists elsewhere.
        assert_eq!(r("Projects/Design.md", &md("Notes.md")), ok("Projects/Notes.md", Resolution::Exact));
        assert_eq!(r("Projects/Design.md", &md("Notes")), ok("Projects/Notes.md", Resolution::Exact));
        assert_eq!(r("Projects/Design.md", &md("../Meeting.md")), ok("Meeting.md", Resolution::Exact));
        assert_eq!(r("Projects/Design.md", &md("../../Meeting.md")), (None, Resolution::Missing));
        // Vault path second.
        assert_eq!(r("Home.md", &md("Projects/Notes.md")), ok("Projects/Notes.md", Resolution::Exact));
        assert_eq!(r("Projects/Design.md", &md("People/Alice.md")), ok("People/Alice.md", Resolution::Exact));
        assert_eq!(r("Home.md", &md("missing/thing.md")), (None, Resolution::Missing));
        // Bare name not a sibling: unique → name, repeated → ambiguous.
        assert_eq!(r("Projects/Design.md", &md("Home.md")), ok("Home.md", Resolution::Name));
        assert_eq!(r("Home.md", &md("Notes.md")), (None, Resolution::Ambiguous));
        assert_eq!(r("Home.md", &md("nothing.md")), (None, Resolution::Missing));
        // The decoded `%23` file name is looked up literally.
        assert_eq!(r("Home.md", &md("foo#bar.md")), ok("foo#bar.md", Resolution::Exact));
    }

    #[test]
    fn kinds_and_externals() {
        let i = idx();
        assert_eq!(i.resolve("Home.md", &wiki("Design")).kind, LinkKind::Wikilink);
        assert_eq!(
            i.resolve("Home.md", &raw(LinkSyntax::Wikilink, true, "diagram.png")).kind,
            LinkKind::Embed
        );
        assert_eq!(i.resolve("Home.md", &md("Meeting.md")).kind, LinkKind::Markdown);
        assert_eq!(
            i.resolve("Home.md", &raw(LinkSyntax::Markdown, true, "attachments/diagram.png")).kind,
            LinkKind::Embed
        );
        for target in ["https://example.com", "mailto:a@b.c", "obsidian://open?vault=x"] {
            for syntax in [LinkSyntax::Wikilink, LinkSyntax::Markdown, LinkSyntax::Autolink] {
                let res = i.resolve("Home.md", &raw(syntax, false, target));
                assert_eq!(res.to_path, None, "{target}");
                assert_eq!(res.kind, LinkKind::External);
                assert_eq!(res.resolution, Resolution::External);
            }
        }
        // An external image is still `external`, not `embed`.
        assert_eq!(
            i.resolve("Home.md", &raw(LinkSyntax::Markdown, true, "https://x/i.png")).kind,
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
        assert_eq!(normalize("Projects", "Notes.md"), Some("Projects/Notes.md".into()));
        assert_eq!(normalize("Projects", "./Notes.md"), Some("Projects/Notes.md".into()));
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
```

- [ ] **Step 2: Implement `resolve.rs`**

```rust
//! Link resolution: turn a `RawLink` plus the linking note's path into
//! `(to_path, kind, resolution)` using an index over every listed file.
//! Pure; the two rule tables in the spec (§Link Resolution) are implemented
//! one arm each below, with one recorded deviation: a bare dotted wikilink
//! name (`[[Note v2.1]]`) that is not a root-level file falls through to the
//! name lookup instead of `missing`, as Obsidian does.

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
}

impl Index {
    pub fn build<S: AsRef<str>>(paths: &[S]) -> Self {
        let mut index = Index::default();
        for path in paths {
            let path = path.as_ref();
            index.by_path.insert(path.to_lowercase(), path.to_string());
            let file_name = path.rsplit('/').next().unwrap_or(path);
            let name = strip_md(file_name).unwrap_or(file_name);
            index
                .by_name
                .entry(name.to_lowercase())
                .or_default()
                .push(path.to_string());
        }
        for candidates in index.by_name.values_mut() {
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
        if target.contains('/') {
            return self.exact_or_missing(normalize("", target));
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

    /// Note-relative first (CommonMark / Obsidian "Relative path to file"),
    /// then the vault root for paths with a `/`, then the name index for
    /// bare names (Obsidian "Shortest path").
    fn resolve_markdown(&self, from_path: &str, target: &str) -> (Option<String>, Resolution) {
        if let Some(path) = normalize(folder_of(from_path), target).and_then(|p| self.lookup_exact(&p)) {
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

/// Parent folder of a relative path (`""` at the root).
fn folder_of(path: &str) -> &str {
    path.rsplit_once('/').map(|(folder, _)| folder).unwrap_or("")
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
```

Add `pub mod resolve;` to `obsidian/mod.rs`.

- [ ] **Step 3: Format and checkpoint**

```bash
cargo fmt --all
```

CI expectation: all `resolve` tests pass.

Checkpoint — suggested commit: `feat(obsidian): resolve.rs — Obsidian link resolution index`.

---

### Task 6: `scan.rs` — the vault scanner, plus the fixture vault

**Files:**
- Create: `crates/skardi/src/sources/providers/obsidian/scan.rs`
- Create: `crates/skardi/src/sources/providers/obsidian/fixtures/vault/**` (15 files, listed in Step 1)
- Modify: `crates/skardi/src/sources/providers/obsidian/mod.rs` (add `pub mod scan;`)
- Test: unit tests inside `scan.rs`

**Interfaces:**
- Consumes: `blob::{BlobEntry, BlobStore, ListOptions, ReadOptions}` (Task 1); `config::ScanOptions` (Task 2); `markdown::{extract, RawLink}` (Task 3); `frontmatter::{split, parse, aliases, tags, links}` (Task 4); `resolve::{Index, LinkKind, Resolution}` (Task 5).
- Produces (used by Task 7):
  ```rust
  pub enum Source { Body, Frontmatter }                       impl Source { pub fn as_str(self) -> &'static str }
  pub struct LinkRow { pub to_path: Option<String>, pub target: String, pub kind: LinkKind, pub display_text: Option<String>, pub heading: Option<String>, pub block_id: Option<String>, pub resolution: Resolution, pub source: Source, pub line: Option<u32> }
  pub struct TagRow { pub tag: String, pub source: Source }   // Ord: (tag, source)
  pub struct ParsedNote { pub path: String, pub name: String, pub folder: String, pub body: String, pub frontmatter_json: Option<String>, pub frontmatter_error: Option<String>, pub aliases: Option<Vec<String>>, pub size_bytes: i64, pub modified_ms: i64, pub tags: Vec<TagRow>, pub links: Vec<LinkRow> }
  pub struct VaultScan; impl VaultScan { pub fn run(root: &str, opts: &ScanOptions) -> anyhow::Result<Vec<ParsedNote>>; }  // SYNC; only inside spawn_blocking
  pub async fn run_scan(root: String, opts: ScanOptions) -> anyhow::Result<Vec<ParsedNote>>;                              // the spawn_blocking wrapper
  pub fn parse_note(path: &str, size: u64, modified: chrono::DateTime<chrono::Utc>, text: &str, index: &Index) -> ParsedNote;  // pure
  ```

- [ ] **Step 1: Create the fixture vault**

Run from `crates/skardi/src/sources/providers/obsidian/`. Every text file ends with exactly one trailing newline (the heredocs below do that). Line numbers in the tests depend on these contents byte for byte — do not reflow.

```bash
mkdir -p fixtures/vault/.obsidian fixtures/vault/.trash fixtures/vault/Projects fixtures/vault/Archive fixtures/vault/People fixtures/vault/Rooms fixtures/vault/attachments
cd fixtures/vault
```

```bash
cat > .obsidian/app.json <<'EOF'
{"attachmentFolderPath": "attachments"}
EOF
```

```bash
cat > .trash/Deleted.md <<'EOF'
# Deleted
[[Home]] #trash
EOF
```

```bash
cat > Home.md <<'EOF'
---
title: Home
tags: [index, project/skardi]
aliases: [Start, Landing]
related: "[[Projects/Design]]"
---
# Home

Welcome. See [[Projects/Design]] and [[Design#Goals|the design]].
Tags here: #project/skardi #y2026 but not #2026 or C# or `#code`.

```
#fenced [[Fenced]]
```

External: [Skardi](https://skardi.ai) and <https://example.com> and <hello@example.com>.
Embeds: ![[diagram.png]] and ![alt](attachments/diagram.png).
Block ref [[Meeting#^abc123]] and same-note [[#Goals]].
Ambiguous [[Notes]], missing [[Nowhere]], and [x](missing/thing.md).
EOF
```

(`Home.md` line map: frontmatter lines 1–6; body line 7 `# Home`; 9 the two Design links; 10 tags; 12–14 fenced code; 16 externals; 17 embeds; 18 block ref + same-note; 19 ambiguous/missing.)

```bash
cat > Projects/Design.md <<'EOF'
---
tags: "draft, design"
status: draft
---
# Design

## Goals

Back to [[Home]]. Sibling [Notes](Notes.md), parent [up](../Meeting.md).
Relative [[./Notes]] and [[../Home]]; shortest [home](Home.md).
EOF
```

```bash
cat > Projects/Notes.md <<'EOF'
Alias link [[Home|Start]] resolves through Home; bare [[Start]] does not.
EOF
```

```bash
cat > Archive/Notes.md <<'EOF'
Archived notes. Nothing links here.
EOF
```

```bash
cat > Meeting.md <<'EOF'
---
aliases: Standup
attendees:
  - "[[People/Alice]]"
  - "[[People/Bob|Bob]]"
location:
  room: "[[Rooms/B12]]"
raw: [[Home]]
md: "[Home](Home.md)"
---
Notes from the standup. ^abc123

#meeting at line start.
EOF
```

```bash
cat > People/Alice.md <<'EOF'
Alice works with [[Bob]].
EOF
```

```bash
cat > People/Bob.md <<'EOF'
Bob works with [[Alice]] in [[B12]].
EOF
```

```bash
: > Rooms/B12.md
```

```bash
cat > 'Bad Frontmatter.md' <<'EOF'
---
title: [unclosed
---
Body survives a bad frontmatter block. #bad
EOF
```

```bash
cat > 'No Frontmatter.md' <<'EOF'
Plain note with a rule below. #plain

---

Still body.
EOF
```

```bash
cat > CJK.md <<'EOF'
中文笔记：见 [[Home]] 和 #标签。
EOF
```

```bash
printf '\x89PNG\r\n\x1a\n' > attachments/diagram.png
```

```bash
{ printf '# Large\n\n'; for i in $(seq 1 40); do printf 'Padding line to exceed the two kilobyte test cap without any links or tags.\n'; done; } > Large.md
```

Verify: `find . -type f | sort` lists 15 files; `wc -c Large.md` is above 2048; `wc -c Rooms/B12.md` is 0.

**Expected results over this fixture (the oracle for every later test):**

`notes` (12 rows, in order): `Archive/Notes.md`, `Bad Frontmatter.md`, `CJK.md`, `Home.md`, `Large.md`, `Meeting.md`, `No Frontmatter.md`, `People/Alice.md`, `People/Bob.md`, `Projects/Design.md`, `Projects/Notes.md`, `Rooms/B12.md`.

`links` (28 rows). Per note, in output order — `(to_path, target, kind, resolution, source, line)`:

| from_path | rows |
|---|---|
| `CJK.md` | (`Home.md`, `Home`, wikilink, name, body, 1) |
| `Home.md` | (`Projects/Design.md`, `Projects/Design`, wikilink, exact, frontmatter, NULL); (`Projects/Design.md`, `Projects/Design`, wikilink, exact, body, 9); (`Projects/Design.md`, `Design`, wikilink, name, body, 9) [heading `Goals`, display `the design`]; (NULL, `https://skardi.ai`, external, external, body, 16) [display `Skardi`]; (NULL, `https://example.com`, external, external, body, 16) [display NULL]; (NULL, `mailto:hello@example.com`, external, external, body, 16); (`attachments/diagram.png`, `diagram.png`, embed, name, body, 17); (`attachments/diagram.png`, `attachments/diagram.png`, embed, exact, body, 17) [display `alt`]; (`Meeting.md`, `Meeting`, wikilink, name, body, 18) [block_id `abc123`]; (`Home.md`, ``, wikilink, exact, body, 18) [heading `Goals`]; (NULL, `Notes`, wikilink, ambiguous, body, 19); (NULL, `Nowhere`, wikilink, missing, body, 19); (NULL, `missing/thing.md`, markdown, missing, body, 19) [display `x`] |
| `Meeting.md` | (`People/Alice.md`, `People/Alice`, wikilink, exact, frontmatter, NULL); (`People/Bob.md`, `People/Bob`, wikilink, exact, frontmatter, NULL) [display `Bob`]; (`Rooms/B12.md`, `Rooms/B12`, wikilink, exact, frontmatter, NULL) |
| `People/Alice.md` | (`People/Bob.md`, `Bob`, wikilink, name, body, 1) |
| `People/Bob.md` | (`People/Alice.md`, `Alice`, wikilink, name, body, 1); (`Rooms/B12.md`, `B12`, wikilink, name, body, 1) |
| `Projects/Design.md` | (`Home.md`, `Home`, wikilink, name, body, 9); (`Projects/Notes.md`, `Notes.md`, markdown, exact, body, 9) [display `Notes`]; (`Meeting.md`, `../Meeting.md`, markdown, exact, body, 9) [display `up`]; (`Projects/Notes.md`, `./Notes`, wikilink, exact, body, 10); (`Home.md`, `../Home`, wikilink, exact, body, 10); (`Home.md`, `Home.md`, markdown, name, body, 10) [display `home`] |
| `Projects/Notes.md` | (`Home.md`, `Home`, wikilink, name, body, 1) [display `Start`]; (NULL, `Start`, wikilink, missing, body, 1) |

`tags` (10 rows, ordered by `(path, tag, source)`): (`Bad Frontmatter.md`, `bad`, body); (`CJK.md`, `标签`, body); (`Home.md`, `index`, frontmatter); (`Home.md`, `project/skardi`, body); (`Home.md`, `project/skardi`, frontmatter); (`Home.md`, `y2026`, body); (`Meeting.md`, `meeting`, body); (`No Frontmatter.md`, `plain`, body); (`Projects/Design.md`, `design`, frontmatter); (`Projects/Design.md`, `draft`, frontmatter).

Graph facts: in-degree of `Home.md` = 6 (self `[[#Goals]]`, three from Design, one from Notes, one from CJK). Orphans (no inbound `to_path`) = `Archive/Notes.md`, `Bad Frontmatter.md`, `CJK.md`, `Large.md`, `No Frontmatter.md`. Alias-repair: the one `missing` link whose `target` is another note's alias is `Projects/Notes.md` → `Start` → `Home.md`.

- [ ] **Step 2: Write the failing tests**

Create `scan.rs` with this test module at the bottom (implementation goes above it in Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    fn fixture_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/sources/providers/obsidian/fixtures/vault")
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

    type LinkSummary<'a> = (Option<&'a str>, &'a str, &'a str, &'a str, &'a str, Option<u32>);
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

        let design = notes.iter().find(|n| n.path == "Projects/Design.md").unwrap();
        assert_eq!(design.name, "Design");
        assert_eq!(design.folder, "Projects");
        assert_eq!(design.aliases, None);

        let bad = notes.iter().find(|n| n.path == "Bad Frontmatter.md").unwrap();
        assert_eq!(bad.frontmatter_json, None);
        assert!(bad.frontmatter_error.is_some());
        assert!(bad.body.starts_with("Body survives"));
        assert_eq!(bad.aliases, None);

        let plain = notes.iter().find(|n| n.path == "No Frontmatter.md").unwrap();
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
        assert_eq!(notes.iter().map(|n| n.links.len()).sum::<usize>(), 28);

        let home = by_path("Home.md");
        assert_eq!(
            summarize(&home.links),
            vec![
                (Some("Projects/Design.md"), "Projects/Design", "wikilink", "exact", "frontmatter", None),
                (Some("Projects/Design.md"), "Projects/Design", "wikilink", "exact", "body", Some(9)),
                (Some("Projects/Design.md"), "Design", "wikilink", "name", "body", Some(9)),
                (None, "https://skardi.ai", "external", "external", "body", Some(16)),
                (None, "https://example.com", "external", "external", "body", Some(16)),
                (None, "mailto:hello@example.com", "external", "external", "body", Some(16)),
                (Some("attachments/diagram.png"), "diagram.png", "embed", "name", "body", Some(17)),
                (Some("attachments/diagram.png"), "attachments/diagram.png", "embed", "exact", "body", Some(17)),
                (Some("Meeting.md"), "Meeting", "wikilink", "name", "body", Some(18)),
                (Some("Home.md"), "", "wikilink", "exact", "body", Some(18)),
                (None, "Notes", "wikilink", "ambiguous", "body", Some(19)),
                (None, "Nowhere", "wikilink", "missing", "body", Some(19)),
                (None, "missing/thing.md", "markdown", "missing", "body", Some(19)),
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
                (Some("People/Alice.md"), "People/Alice", "wikilink", "exact", "frontmatter", None),
                (Some("People/Bob.md"), "People/Bob", "wikilink", "exact", "frontmatter", None),
                (Some("Rooms/B12.md"), "Rooms/B12", "wikilink", "exact", "frontmatter", None),
            ]
        );
        assert_eq!(by_path("Meeting.md").links[1].display_text.as_deref(), Some("Bob"));

        assert_eq!(
            summarize(&by_path("Projects/Design.md").links),
            vec![
                (Some("Home.md"), "Home", "wikilink", "name", "body", Some(9)),
                (Some("Projects/Notes.md"), "Notes.md", "markdown", "exact", "body", Some(9)),
                (Some("Meeting.md"), "../Meeting.md", "markdown", "exact", "body", Some(9)),
                (Some("Projects/Notes.md"), "./Notes", "wikilink", "exact", "body", Some(10)),
                (Some("Home.md"), "../Home", "wikilink", "exact", "body", Some(10)),
                (Some("Home.md"), "Home.md", "markdown", "name", "body", Some(10)),
            ]
        );

        assert_eq!(
            summarize(&by_path("Projects/Notes.md").links),
            vec![
                (Some("Home.md"), "Home", "wikilink", "name", "body", Some(1)),
                (None, "Start", "wikilink", "missing", "body", Some(1)),
            ]
        );
        assert_eq!(
            summarize(&by_path("People/Bob.md").links),
            vec![
                (Some("People/Alice.md"), "Alice", "wikilink", "name", "body", Some(1)),
                (Some("Rooms/B12.md"), "B12", "wikilink", "name", "body", Some(1)),
            ]
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
            vec!["Archive/Notes.md", "Bad Frontmatter.md", "CJK.md", "Large.md", "No Frontmatter.md"]
        );
    }

    #[tokio::test]
    async fn fixture_tags_match_the_oracle() {
        let notes = scan(&fixture_root(), defaults()).await.unwrap();
        let mut rows: Vec<(&str, &str, &str)> = notes
            .iter()
            .flat_map(|n| n.tags.iter().map(move |t| (n.path.as_str(), t.tag.as_str(), t.source.as_str())))
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
        let err = scan(Path::new("/no/such/vault/root"), defaults()).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("/no/such/vault/root"), "{msg}");
    }

    #[tokio::test]
    async fn size_cap_skips_large_notes_before_reading() {
        let large = std::fs::metadata(fixture_root().join("Large.md")).unwrap().len();
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
        assert!(msg.contains(&dir.path().to_string_lossy().into_owned()), "{msg}");
        assert!(msg.contains("Archive/Notes.md"), "first failure named: {msg}");
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
        std::os::unix::fs::symlink(outside.path().join("secret.md"), dir.path().join("CJK.md")).unwrap();
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
            chrono::DateTime::<chrono::Utc>::from(std::time::UNIX_EPOCH),
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
        let tags: Vec<(&str, &str)> = note.tags.iter().map(|t| (t.tag.as_str(), t.source.as_str())).collect();
        assert_eq!(tags, vec![("a", "body"), ("t", "frontmatter")]);
    }
}
```

- [ ] **Step 3: Implement `scan.rs`**

```rust
//! The vault scanner: list → filter → read → parse → resolve, producing one
//! `ParsedNote` per `.md` file. `VaultScan::run` is synchronous and must run
//! inside `tokio::task::spawn_blocking` (use [`run_scan`]); it drives the
//! `BlobStore` futures with `Handle::current().block_on`, and resolves the
//! store inside the same task so an S3 client never crosses runtimes.

use std::time::Instant;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tokio::runtime::Handle;

use super::config::ScanOptions;
use super::frontmatter;
use super::markdown::{self, RawLink};
use super::resolve::{Index, LinkKind, Resolution};
use crate::sources::providers::blob::{BlobEntry, BlobStore, ListOptions, ReadOptions};

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
    let name = std::path::Path::new(file_name)
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
                ReadOptions {
                    follow_symlinks: false,
                },
            )) {
                Ok(bytes) => bytes,
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
            notes.push(parse_note(&entry.rel_key, entry.size, entry.modified, &text, &index));
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
```

Add `pub mod scan;` to `obsidian/mod.rs`.

- [ ] **Step 4: Format and checkpoint**

```bash
cargo fmt --all
```

CI expectation: all `scan` tests pass on Linux (the three `#[cfg(unix)]` permission tests self-skip when CI runs as root — they print a line and return). `Home.md`'s link lines in `fixture_links_match_the_oracle` are the most fragile assertion: if one is off by a constant, the fixture file has an extra/missing line — fix the fixture, not the test.

Checkpoint — suggested commit: `feat(obsidian): scan.rs — VaultScan with size cap, symlink refusal, wholesale-failure guard; fixture vault`.

---

### Task 7: `table.rs` — schemas, `ObsidianTable`, `ObsidianScanExec`, batch building

**Files:**
- Create: `crates/skardi/src/sources/providers/obsidian/table.rs`
- Modify: `crates/skardi/src/sources/providers/obsidian/mod.rs` (add `pub mod table;`)
- Test: unit tests inside `table.rs`

**Interfaces:**
- Consumes: `OBSIDIAN_SURFACE_VERSION`, `NOTES_TABLE`/`LINKS_TABLE`/`TAGS_TABLE` (Task 2); `config::ScanOptions` (Task 2); `scan::{ParsedNote, LinkRow, TagRow, run_scan, parse_note}` (Task 6); `resolve::Index` (tests only).
- Produces (used by Task 8):
  ```rust
  pub enum TableKind { Notes, Links, Tags }
  impl TableKind { pub fn table_name(self) -> &'static str; pub fn schema(self) -> SchemaRef; }
  pub struct ObsidianTable;  impl ObsidianTable { pub fn new(kind: TableKind, root: String, opts: ScanOptions) -> Self }  // TableProvider
  pub fn build_batch(kind: TableKind, notes: &[ParsedNote], projected_schema: &SchemaRef, projection: &[usize], limit: Option<usize>) -> datafusion::common::Result<RecordBatch>;
  ```

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::providers::obsidian::resolve::Index;
    use crate::sources::providers::obsidian::scan::parse_note;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::Int64Type;
    use datafusion::prelude::SessionContext;

    fn epoch() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::<chrono::Utc>::from(std::time::UNIX_EPOCH)
    }

    fn two_notes() -> Vec<ParsedNote> {
        let index = Index::build(&["A.md", "Sub/B.md"]);
        vec![
            parse_note(
                "A.md",
                10,
                epoch(),
                "---\naliases: [x, y]\nrel: \"[[B]]\"\ntags: t\n---\nbody [[B|bee]] #a\n",
                &index,
            ),
            parse_note("Sub/B.md", 0, epoch(), "", &index),
        ]
    }

    fn field(schema: &SchemaRef, name: &str) -> (DataType, bool) {
        let f = schema.field_with_name(name).unwrap();
        (f.data_type().clone(), f.is_nullable())
    }

    #[test]
    fn schemas_pin_the_spec_tables() {
        let ts = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
        let list = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));

        let notes = TableKind::Notes.schema();
        let names: Vec<&str> = notes.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec!["path", "name", "folder", "body", "frontmatter_json", "frontmatter_error", "aliases", "size_bytes", "modified_at"]
        );
        assert_eq!(field(&notes, "path"), (DataType::Utf8, false));
        assert_eq!(field(&notes, "body"), (DataType::Utf8, false));
        assert_eq!(field(&notes, "frontmatter_json"), (DataType::Utf8, true));
        assert_eq!(field(&notes, "frontmatter_error"), (DataType::Utf8, true));
        assert_eq!(field(&notes, "aliases"), (list, true));
        assert_eq!(field(&notes, "size_bytes"), (DataType::Int64, false));
        assert_eq!(field(&notes, "modified_at"), (ts, false));

        let links = TableKind::Links.schema();
        let names: Vec<&str> = links.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec!["from_path", "to_path", "target", "kind", "display_text", "heading", "block_id", "resolution", "source", "line"]
        );
        assert_eq!(field(&links, "to_path"), (DataType::Utf8, true));
        assert_eq!(field(&links, "target"), (DataType::Utf8, false));
        assert_eq!(field(&links, "kind"), (DataType::Utf8, false));
        assert_eq!(field(&links, "line"), (DataType::Int32, true));

        let tags = TableKind::Tags.schema();
        let names: Vec<&str> = tags.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["path", "tag", "source"]);
        assert!(tags.fields().iter().all(|f| !f.is_nullable()));

        for kind in [TableKind::Notes, TableKind::Links, TableKind::Tags] {
            assert_eq!(
                kind.schema().metadata().get("skardi.obsidian.surface_version").map(String::as_str),
                Some("1")
            );
        }
        assert_eq!(TableKind::Notes.table_name(), "notes");
        assert_eq!(TableKind::Links.table_name(), "links");
        assert_eq!(TableKind::Tags.table_name(), "tags");
    }

    #[test]
    fn notes_batch_has_aliases_list_and_nulls() {
        let notes = two_notes();
        let schema = TableKind::Notes.schema();
        let all: Vec<usize> = (0..schema.fields().len()).collect();
        let batch = build_batch(TableKind::Notes, &notes, &schema, &all, None).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let aliases = batch.column(6).as_list::<i32>();
        assert!(!aliases.is_null(0));
        assert!(aliases.is_null(1));
        let first = aliases.value(0);
        let first = first.as_string::<i32>();
        assert_eq!(first.value(0), "x");
        assert_eq!(first.value(1), "y");
        assert!(batch.column(4).is_null(1)); // no frontmatter → NULL json
        assert!(batch.column(5).is_null(0)); // valid frontmatter → NULL error
        let sizes = batch.column(7).as_primitive::<Int64Type>();
        assert_eq!(sizes.value(0), 10);
        assert_eq!(sizes.value(1), 0);
        assert_eq!(
            batch.column(8).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );
    }

    #[test]
    fn limit_and_projection_apply_to_every_kind() {
        let notes = two_notes();
        // notes: LIMIT 1, only `name`.
        let schema = Arc::new(TableKind::Notes.schema().project(&[1]).unwrap());
        let batch = build_batch(TableKind::Notes, &notes, &schema, &[1], Some(1)).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.column(0).as_string::<i32>().value(0), "A");

        // links: A.md has 2 (one frontmatter, one body); LIMIT 5 keeps both.
        let schema = TableKind::Links.schema();
        let all: Vec<usize> = (0..schema.fields().len()).collect();
        let batch = build_batch(TableKind::Links, &notes, &schema, &all, Some(5)).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.column(8).as_string::<i32>().value(0), "frontmatter");
        assert!(batch.column(9).is_null(0)); // frontmatter line NULL
        assert_eq!(batch.column(8).as_string::<i32>().value(1), "body");
        assert_eq!(batch.column(9).as_primitive::<arrow::datatypes::Int32Type>().value(1), 6);
        assert_eq!(batch.column(4).as_string::<i32>().value(1), "bee");

        // tags: (a, body), (t, frontmatter); LIMIT 1.
        let schema = TableKind::Tags.schema();
        let batch = build_batch(TableKind::Tags, &notes, &schema, &[0, 1, 2], Some(1)).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.column(1).as_string::<i32>().value(0), "a");
    }

    #[test]
    fn empty_projection_keeps_the_row_count() {
        let notes = two_notes();
        let schema = Arc::new(TableKind::Links.schema().project(&[]).unwrap());
        let batch = build_batch(TableKind::Links, &notes, &schema, &[], None).unwrap();
        assert_eq!(batch.num_columns(), 0);
        assert_eq!(batch.num_rows(), 2);
    }

    #[tokio::test]
    async fn table_is_queryable_through_datafusion() -> datafusion::error::Result<()> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/sources/providers/obsidian/fixtures/vault")
            .to_string_lossy()
            .into_owned();
        let opts = ScanOptions::from_map(None).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let ctx = SessionContext::new();
        ctx.register_table("notes", Arc::new(ObsidianTable::new(TableKind::Notes, root, opts)))?;
        let batches = ctx.sql("SELECT count(*) FROM notes").await?.collect().await?;
        assert_eq!(batches[0].column(0).as_primitive::<Int64Type>().value(0), 12);

        let plan = ctx.sql("EXPLAIN SELECT path FROM notes LIMIT 2").await?.collect().await?;
        let text = arrow::util::pretty::pretty_format_batches(&plan)?.to_string();
        assert!(text.contains("ObsidianScanExec"), "{text}");
        Ok(())
    }
}
```

- [ ] **Step 2: Implement `table.rs`**

```rust
//! The Arrow surface: three schemas, one `TableProvider` parameterized by
//! table kind, and one single-partition `ExecutionPlan` that runs the scan on
//! the blocking pool and emits one `RecordBatch`. Column order *is* the batch
//! shape (the exec projects by index), so the schemas here are the single
//! source of truth and the tests pin every name, type and nullability.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{self, Formatter};
use std::sync::{Arc, LazyLock};

use arrow::array::{
    ArrayRef, Int32Array, Int64Array, ListBuilder, RecordBatch, RecordBatchOptions, StringArray,
    StringBuilder, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::Expr;
use futures::stream;

use super::config::ScanOptions;
use super::scan::{LinkRow, ParsedNote, TagRow, run_scan};
use super::{LINKS_TABLE, NOTES_TABLE, OBSIDIAN_SURFACE_VERSION, TAGS_TABLE};

/// Schema-metadata key carrying [`OBSIDIAN_SURFACE_VERSION`].
const SURFACE_VERSION_KEY: &str = "skardi.obsidian.surface_version";

fn surface_metadata() -> HashMap<String, String> {
    HashMap::from([(
        SURFACE_VERSION_KEY.to_string(),
        OBSIDIAN_SURFACE_VERSION.to_string(),
    )])
}

fn utc_millis() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
}

static NOTES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(
        Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("folder", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, false),
            Field::new("frontmatter_json", DataType::Utf8, true),
            Field::new("frontmatter_error", DataType::Utf8, true),
            Field::new(
                "aliases",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("size_bytes", DataType::Int64, false),
            Field::new("modified_at", utc_millis(), false),
        ])
        .with_metadata(surface_metadata()),
    )
});

static LINKS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(
        Schema::new(vec![
            Field::new("from_path", DataType::Utf8, false),
            Field::new("to_path", DataType::Utf8, true),
            Field::new("target", DataType::Utf8, false),
            Field::new("kind", DataType::Utf8, false),
            Field::new("display_text", DataType::Utf8, true),
            Field::new("heading", DataType::Utf8, true),
            Field::new("block_id", DataType::Utf8, true),
            Field::new("resolution", DataType::Utf8, false),
            Field::new("source", DataType::Utf8, false),
            Field::new("line", DataType::Int32, true),
        ])
        .with_metadata(surface_metadata()),
    )
});

static TAGS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(
        Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("tag", DataType::Utf8, false),
            Field::new("source", DataType::Utf8, false),
        ])
        .with_metadata(surface_metadata()),
    )
});

/// Which of the three tables a provider serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableKind {
    Notes,
    Links,
    Tags,
}

impl TableKind {
    pub fn table_name(self) -> &'static str {
        match self {
            TableKind::Notes => NOTES_TABLE,
            TableKind::Links => LINKS_TABLE,
            TableKind::Tags => TAGS_TABLE,
        }
    }

    pub fn schema(self) -> SchemaRef {
        match self {
            TableKind::Notes => NOTES_SCHEMA.clone(),
            TableKind::Links => LINKS_SCHEMA.clone(),
            TableKind::Tags => TAGS_SCHEMA.clone(),
        }
    }
}

/// Read-only `TableProvider` over one vault root for one table kind.
#[derive(Debug)]
pub struct ObsidianTable {
    kind: TableKind,
    root: String,
    opts: ScanOptions,
}

impl ObsidianTable {
    pub fn new(kind: TableKind, root: String, opts: ScanOptions) -> Self {
        Self { kind, root, opts }
    }
}

#[async_trait]
impl TableProvider for ObsidianTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.kind.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let full = self.kind.schema();
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..full.fields().len()).collect());
        let projected_schema = Arc::new(full.project(&projection)?);
        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Both,
            Boundedness::Bounded,
        );
        Ok(Arc::new(ObsidianScanExec {
            kind: self.kind,
            root: self.root.clone(),
            opts: self.opts.clone(),
            projected_schema,
            projection,
            limit,
            properties,
        }))
    }
}

/// Single-partition scan: one `spawn_blocking` vault scan, one batch.
#[derive(Debug)]
struct ObsidianScanExec {
    kind: TableKind,
    root: String,
    opts: ScanOptions,
    projected_schema: SchemaRef,
    projection: Vec<usize>,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl DisplayAs for ObsidianScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "ObsidianScanExec: table={}, root={}, projected_cols={:?}, limit={:?}",
                self.kind.table_name(),
                self.root,
                self.projection,
                self.limit
            ),
            DisplayFormatType::TreeRender => {
                write!(f, "ObsidianScanExec({}: {})", self.kind.table_name(), self.root)
            }
        }
    }
}

impl ExecutionPlan for ObsidianScanExec {
    fn name(&self) -> &str {
        "ObsidianScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let kind = self.kind;
        let root = self.root.clone();
        let opts = self.opts.clone();
        let projection = self.projection.clone();
        let limit = self.limit;
        let batch_schema = schema.clone();
        // The scan runs on the blocking pool (see scan.rs); nothing here blocks
        // the worker that polls this stream.
        let batch = async move {
            let notes = run_scan(root, opts)
                .await
                .map_err(|e| DataFusionError::External(e.into()))?;
            build_batch(kind, &notes, &batch_schema, &projection, limit)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(batch),
        )))
    }
}

/// Project + limit the parsed notes into one batch of `kind`. An empty
/// projection (`count(*)`) still carries the row count.
pub fn build_batch(
    kind: TableKind,
    notes: &[ParsedNote],
    projected_schema: &SchemaRef,
    projection: &[usize],
    limit: Option<usize>,
) -> datafusion::common::Result<RecordBatch> {
    let (row_count, arrays) = match kind {
        TableKind::Notes => {
            let rows = truncate(notes, limit);
            (rows.len(), projection.iter().map(|&i| notes_column(i, rows)).collect())
        }
        TableKind::Links => {
            let all: Vec<(&str, &LinkRow)> = notes
                .iter()
                .flat_map(|n| n.links.iter().map(move |l| (n.path.as_str(), l)))
                .collect();
            let rows = truncate(&all, limit);
            (rows.len(), projection.iter().map(|&i| links_column(i, rows)).collect())
        }
        TableKind::Tags => {
            let all: Vec<(&str, &TagRow)> = notes
                .iter()
                .flat_map(|n| n.tags.iter().map(move |t| (n.path.as_str(), t)))
                .collect();
            let rows = truncate(&all, limit);
            (rows.len(), projection.iter().map(|&i| tags_column(i, rows)).collect())
        }
    };
    let options = RecordBatchOptions::new().with_row_count(Some(row_count));
    RecordBatch::try_new_with_options(projected_schema.clone(), arrays, &options)
        .map_err(|e| DataFusionError::Execution(format!("obsidian: building RecordBatch: {e}")))
}

fn truncate<T>(rows: &[T], limit: Option<usize>) -> &[T] {
    match limit {
        Some(max) if max < rows.len() => &rows[..max],
        _ => rows,
    }
}

fn utf8<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from_iter(values))
}

fn notes_column(idx: usize, rows: &[ParsedNote]) -> ArrayRef {
    match idx {
        0 => utf8(rows.iter().map(|n| Some(n.path.as_str()))),
        1 => utf8(rows.iter().map(|n| Some(n.name.as_str()))),
        2 => utf8(rows.iter().map(|n| Some(n.folder.as_str()))),
        3 => utf8(rows.iter().map(|n| Some(n.body.as_str()))),
        4 => utf8(rows.iter().map(|n| n.frontmatter_json.as_deref())),
        5 => utf8(rows.iter().map(|n| n.frontmatter_error.as_deref())),
        6 => {
            let mut builder = ListBuilder::new(StringBuilder::new());
            for note in rows {
                match &note.aliases {
                    Some(aliases) => {
                        for alias in aliases {
                            builder.values().append_value(alias);
                        }
                        builder.append(true);
                    }
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        7 => Arc::new(Int64Array::from_iter_values(rows.iter().map(|n| n.size_bytes))),
        8 => Arc::new(
            TimestampMillisecondArray::from_iter_values(rows.iter().map(|n| n.modified_ms))
                .with_timezone("UTC"),
        ),
        other => unreachable!("notes schema has 9 columns, got index {other}"),
    }
}

fn links_column(idx: usize, rows: &[(&str, &LinkRow)]) -> ArrayRef {
    match idx {
        0 => utf8(rows.iter().map(|(from, _)| Some(*from))),
        1 => utf8(rows.iter().map(|(_, l)| l.to_path.as_deref())),
        2 => utf8(rows.iter().map(|(_, l)| Some(l.target.as_str()))),
        3 => utf8(rows.iter().map(|(_, l)| Some(l.kind.as_str()))),
        4 => utf8(rows.iter().map(|(_, l)| l.display_text.as_deref())),
        5 => utf8(rows.iter().map(|(_, l)| l.heading.as_deref())),
        6 => utf8(rows.iter().map(|(_, l)| l.block_id.as_deref())),
        7 => utf8(rows.iter().map(|(_, l)| Some(l.resolution.as_str()))),
        8 => utf8(rows.iter().map(|(_, l)| Some(l.source.as_str()))),
        9 => Arc::new(Int32Array::from_iter(
            rows.iter()
                .map(|(_, l)| l.line.and_then(|line| i32::try_from(line).ok())),
        )),
        other => unreachable!("links schema has 10 columns, got index {other}"),
    }
}

fn tags_column(idx: usize, rows: &[(&str, &TagRow)]) -> ArrayRef {
    match idx {
        0 => utf8(rows.iter().map(|(path, _)| Some(*path))),
        1 => utf8(rows.iter().map(|(_, t)| Some(t.tag.as_str()))),
        2 => utf8(rows.iter().map(|(_, t)| Some(t.source.as_str()))),
        other => unreachable!("tags schema has 3 columns, got index {other}"),
    }
}
```

(`unreachable!` on a projection index is the one panic in this module; DataFusion validates projections against the schema before `scan`, exactly as `documents/table.rs` relies on.)

Add `pub mod table;` to `obsidian/mod.rs`.

- [ ] **Step 3: Format and checkpoint**

```bash
cargo fmt --all
```

CI expectation: all `table` tests pass; `cargo doc` clean (the intra-doc link to `OBSIDIAN_SURFACE_VERSION` resolves because it is `pub` in the parent).

Checkpoint — suggested commit: `feat(obsidian): table.rs — schemas, ObsidianTable, ObsidianScanExec over spawn_blocking scan`.

---

### Task 8: Registration — `register_obsidian_tables`, root check, SQL integration tests

**Files:**
- Modify: `crates/skardi/src/sources/providers/obsidian/mod.rs` (registration function, root check, test module)
- Test: `#[cfg(test)] mod tests` in `mod.rs`

**Interfaces:**
- Consumes: everything above; `crate::sources::hierarchy::HierarchyLevel`; `blob::{BlobStore, ListOptions, Loc}`.
- Produces (used by Task 9):
  ```rust
  pub async fn register_obsidian_tables(
      session_ctx: &mut SessionContext, name: &str, path: &str,
      options: Option<&HashMap<String, String>>, read_write: bool, hierarchy_level: HierarchyLevel,
  ) -> anyhow::Result<()>;
  ```

- [ ] **Step 1: Write the failing tests**

Append to `obsidian/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray, RecordBatch};
    use arrow::compute::cast;
    use arrow::datatypes::{DataType, Int64Type};
    use std::path::{Path, PathBuf};

    fn fixture_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/sources/providers/obsidian/fixtures/vault")
    }

    async fn register(root: &Path, name: &str) -> SessionContext {
        let mut ctx = SessionContext::new();
        register_obsidian_tables(
            &mut ctx,
            name,
            &root.to_string_lossy(),
            None,
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("fixture vault registers");
        ctx
    }

    async fn query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        ctx.sql(sql).await.unwrap().collect().await.unwrap()
    }

    /// Column `col` of every batch as strings (cast to Utf8 so a view type
    /// chosen by the planner does not matter).
    fn strings(batches: &[RecordBatch], col: usize) -> Vec<Option<String>> {
        let mut out = Vec::new();
        for b in batches {
            let arr = cast(b.column(col), &DataType::Utf8).unwrap();
            let arr = arr.as_string::<i32>();
            out.extend((0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i).to_string())));
        }
        out
    }

    fn int64(batches: &[RecordBatch], col: usize) -> Vec<i64> {
        let mut out = Vec::new();
        for b in batches {
            let arr = cast(b.column(col), &DataType::Int64).unwrap();
            out.extend(arr.as_primitive::<Int64Type>().values().iter().copied());
        }
        out
    }

    fn bools(batches: &[RecordBatch], col: usize) -> Vec<bool> {
        batches
            .iter()
            .flat_map(|b| {
                let arr = b.column(col).as_boolean();
                (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>()
            })
            .collect()
    }

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

    #[tokio::test]
    async fn rejects_non_catalog_hierarchy() {
        let mut ctx = SessionContext::new();
        let err = register_obsidian_tables(
            &mut ctx,
            "vault",
            &fixture_root().to_string_lossy(),
            None,
            false,
            HierarchyLevel::Table,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<ObsidianError>(),
            Some(ObsidianError::CatalogHierarchyRequired { name }) if name == "vault"
        ));
        assert!(ctx.catalog("vault").is_none(), "nothing registered on failure");
    }

    #[tokio::test]
    async fn rejects_read_write() {
        let mut ctx = SessionContext::new();
        let err = register_obsidian_tables(
            &mut ctx,
            "vault",
            &fixture_root().to_string_lossy(),
            None,
            true,
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<ObsidianError>(),
            Some(ObsidianError::ReadWriteNotSupported { .. })
        ));
    }

    #[tokio::test]
    async fn rejects_unknown_option_naming_it() {
        let mut ctx = SessionContext::new();
        let opts = HashMap::from([("exclude_glob".to_string(), "x".to_string())]);
        let err = register_obsidian_tables(
            &mut ctx,
            "vault",
            &fixture_root().to_string_lossy(),
            Some(&opts),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown option `exclude_glob`"), "{msg}");
        assert!(msg.contains("'vault'"), "{msg}");
    }

    #[tokio::test]
    async fn rejects_missing_root_and_file_root() {
        let mut ctx = SessionContext::new();
        let err = register_obsidian_tables(
            &mut ctx,
            "vault",
            "/no/such/vault",
            None,
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<ObsidianError>(),
            Some(ObsidianError::RootUnavailable { path, .. }) if path == "/no/such/vault"
        ));

        let file = tempfile::NamedTempFile::new().unwrap();
        let err = register_obsidian_tables(
            &mut ctx,
            "vault",
            &file.path().to_string_lossy(),
            None,
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<ObsidianError>(),
            Some(ObsidianError::RootNotDirectory { .. })
        ));
    }

    #[tokio::test]
    async fn registers_three_tables_under_main() {
        let ctx = register(&fixture_root(), "vault").await;
        let catalog = ctx.catalog("vault").expect("catalog registered");
        let schema = catalog.schema("main").expect("main schema");
        let mut names = schema.table_names();
        names.sort();
        assert_eq!(names, vec!["links", "notes", "tags"]);

        assert_eq!(int64(&query(&ctx, "SELECT count(*) FROM vault.main.notes").await, 0), vec![12]);
        assert_eq!(int64(&query(&ctx, "SELECT count(*) FROM vault.main.links").await, 0), vec![28]);
        assert_eq!(int64(&query(&ctx, "SELECT count(*) FROM vault.main.tags").await, 0), vec![10]);
    }

    #[tokio::test]
    async fn notes_projection_order_and_limit() {
        let ctx = register(&fixture_root(), "vault").await;
        let b = query(&ctx, "SELECT path, name, folder FROM vault.main.notes").await;
        let paths: Vec<String> = strings(&b, 0).into_iter().flatten().collect();
        assert_eq!(
            paths,
            vec![
                "Archive/Notes.md", "Bad Frontmatter.md", "CJK.md", "Home.md", "Large.md", "Meeting.md",
                "No Frontmatter.md", "People/Alice.md", "People/Bob.md", "Projects/Design.md",
                "Projects/Notes.md", "Rooms/B12.md",
            ]
        );
        let names = strings(&b, 1);
        let folders = strings(&b, 2);
        assert_eq!(names[9].as_deref(), Some("Design"));
        assert_eq!(folders[9].as_deref(), Some("Projects"));
        assert_eq!(folders[3].as_deref(), Some(""));

        let b = query(&ctx, "SELECT path FROM vault.main.notes LIMIT 3").await;
        assert_eq!(b.iter().map(|b| b.num_rows()).sum::<usize>(), 3);

        let b = query(&ctx, "SELECT arrow_typeof(modified_at) FROM vault.main.notes LIMIT 1").await;
        assert_eq!(
            strings(&b, 0)[0].as_deref(),
            Some("Timestamp(Millisecond, Some(\"UTC\"))")
        );
    }

    #[tokio::test]
    async fn frontmatter_null_cases() {
        let ctx = register(&fixture_root(), "vault").await;
        let b = query(
            &ctx,
            "SELECT path, frontmatter_json IS NULL, frontmatter_error IS NOT NULL, aliases IS NULL \
             FROM vault.main.notes \
             WHERE path IN ('Bad Frontmatter.md', 'Home.md', 'No Frontmatter.md') ORDER BY path",
        )
        .await;
        assert_eq!(bools(&b, 1), vec![true, false, true]);
        assert_eq!(bools(&b, 2), vec![true, false, false]);
        assert_eq!(bools(&b, 3), vec![true, false, true]);
    }

    #[tokio::test]
    async fn every_kind_resolution_and_source_value_appears() {
        let ctx = register(&fixture_root(), "vault").await;
        let kinds = strings(&query(&ctx, "SELECT DISTINCT kind FROM vault.main.links ORDER BY kind").await, 0);
        assert_eq!(kinds.into_iter().flatten().collect::<Vec<_>>(), vec!["embed", "external", "markdown", "wikilink"]);
        let res = strings(&query(&ctx, "SELECT DISTINCT resolution FROM vault.main.links ORDER BY resolution").await, 0);
        assert_eq!(res.into_iter().flatten().collect::<Vec<_>>(), vec!["ambiguous", "exact", "external", "missing", "name"]);
        let src = strings(&query(&ctx, "SELECT DISTINCT source FROM vault.main.links ORDER BY source").await, 0);
        assert_eq!(src.into_iter().flatten().collect::<Vec<_>>(), vec!["body", "frontmatter"]);
        let src = strings(&query(&ctx, "SELECT DISTINCT source FROM vault.main.tags ORDER BY source").await, 0);
        assert_eq!(src.into_iter().flatten().collect::<Vec<_>>(), vec!["body", "frontmatter"]);
    }

    #[tokio::test]
    async fn graph_queries_from_the_docs() {
        let ctx = register(&fixture_root(), "vault").await;

        // Most-linked note.
        let b = query(
            &ctx,
            "SELECT to_path, count(*) AS n FROM vault.main.links \
             WHERE to_path IS NOT NULL GROUP BY to_path ORDER BY n DESC, to_path LIMIT 1",
        )
        .await;
        assert_eq!(strings(&b, 0)[0].as_deref(), Some("Home.md"));
        assert_eq!(int64(&b, 1), vec![6]);

        // Orphans: notes nothing links to.
        let b = query(
            &ctx,
            "SELECT n.path FROM vault.main.notes n \
             LEFT JOIN vault.main.links l ON l.to_path = n.path \
             WHERE l.to_path IS NULL ORDER BY n.path",
        )
        .await;
        assert_eq!(
            strings(&b, 0).into_iter().flatten().collect::<Vec<_>>(),
            vec!["Archive/Notes.md", "Bad Frontmatter.md", "CJK.md", "Large.md", "No Frontmatter.md"]
        );

        // Alias repair: a missing link whose target is another note's alias.
        let b = query(
            &ctx,
            "SELECT l.from_path, l.target, a.path AS probably_meant \
             FROM vault.main.links l \
             JOIN (SELECT path, unnest(aliases) AS alias FROM vault.main.notes WHERE aliases IS NOT NULL) a \
               ON a.alias = l.target \
             WHERE l.resolution = 'missing' ORDER BY l.from_path",
        )
        .await;
        assert_eq!(strings(&b, 0), vec![Some("Projects/Notes.md".to_string())]);
        assert_eq!(strings(&b, 1), vec![Some("Start".to_string())]);
        assert_eq!(strings(&b, 2), vec![Some("Home.md".to_string())]);

        // Frontmatter-only inbound link: B12's in-degree is wrong without it.
        let b = query(
            &ctx,
            "SELECT count(*) FROM vault.main.links WHERE to_path = 'Rooms/B12.md' AND source = 'frontmatter'",
        )
        .await;
        assert_eq!(int64(&b, 0), vec![1]);
    }

    #[tokio::test]
    async fn explain_shows_the_scan_exec() {
        let ctx = register(&fixture_root(), "vault").await;
        let b = query(&ctx, "EXPLAIN SELECT tag FROM vault.main.tags").await;
        let text = arrow::util::pretty::pretty_format_batches(&b).unwrap().to_string();
        assert!(text.contains("ObsidianScanExec"), "{text}");
    }

    #[tokio::test]
    async fn edits_between_scans_are_visible_without_reregistration() {
        let dir = tempfile::tempdir().unwrap();
        copy_dir(&fixture_root(), dir.path());
        let ctx = register(dir.path(), "vault").await;
        assert_eq!(int64(&query(&ctx, "SELECT count(*) FROM vault.main.notes").await, 0), vec![12]);
        assert_eq!(int64(&query(&ctx, "SELECT count(*) FROM vault.main.links").await, 0), vec![28]);

        std::fs::write(dir.path().join("New.md"), "Fresh note linking [[Home]].\n").unwrap();
        assert_eq!(int64(&query(&ctx, "SELECT count(*) FROM vault.main.notes").await, 0), vec![13]);
        assert_eq!(int64(&query(&ctx, "SELECT count(*) FROM vault.main.links").await, 0), vec![29]);

        std::fs::remove_file(dir.path().join("New.md")).unwrap();
        assert_eq!(int64(&query(&ctx, "SELECT count(*) FROM vault.main.notes").await, 0), vec![12]);
    }

    #[tokio::test]
    async fn options_reach_the_scan() {
        let mut ctx = SessionContext::new();
        let opts = HashMap::from([
            ("exclude_globs".to_string(), "People/**".to_string()),
            ("max_file_bytes".to_string(), "2048".to_string()),
        ]);
        register_obsidian_tables(
            &mut ctx,
            "vault",
            &fixture_root().to_string_lossy(),
            Some(&opts),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap();
        // 12 − People (2) − Large (cap) + .trash/Deleted.md (default gone) = 10.
        assert_eq!(int64(&query(&ctx, "SELECT count(*) FROM vault.main.notes").await, 0), vec![10]);
    }
}
```

- [ ] **Step 2: Implement registration in `mod.rs`**

Add imports at the top of `obsidian/mod.rs` (after the module docs and `pub mod` lines):

```rust
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
};
use datafusion::prelude::SessionContext;
use tokio::runtime::Handle;

use crate::sources::hierarchy::HierarchyLevel;
use crate::sources::providers::blob::{BlobStore, ListOptions, Loc};
use config::ScanOptions;
use table::{ObsidianTable, TableKind};
```

(Use the exact `datafusion::catalog` import line `rss/mod.rs:107-110` uses; the trait imports are what make `register_table` / `register_schema` callable.)

Then, after `ObsidianError`:

```rust
/// Register one vault as the catalog `name` with schema `main` and tables
/// `notes`, `links`, `tags`.
///
/// Every invariant is enforced here, so the server's `config.rs` arm re-checks
/// nothing: catalog hierarchy, read-only access, valid options, and a
/// reachable root (a directory locally; one non-recursive list for `s3://`).
/// No parsing happens at registration. `register_catalog` is the **last**
/// step: it replaces whatever was registered under `name` unconditionally, so
/// a failed registration must never have touched the context.
pub async fn register_obsidian_tables(
    session_ctx: &mut SessionContext,
    name: &str,
    path: &str,
    options: Option<&HashMap<String, String>>,
    read_write: bool,
    hierarchy_level: HierarchyLevel,
) -> Result<()> {
    if hierarchy_level != HierarchyLevel::Catalog {
        return Err(ObsidianError::CatalogHierarchyRequired {
            name: name.to_string(),
        }
        .into());
    }
    if read_write {
        return Err(ObsidianError::ReadWriteNotSupported {
            name: name.to_string(),
        }
        .into());
    }
    let opts = ScanOptions::from_map(options).map_err(|e| ObsidianError::InvalidOptions {
        name: name.to_string(),
        reason: e.to_string(),
    })?;
    check_root(name, path).await?;

    let schema_provider = Arc::new(MemorySchemaProvider::new());
    for kind in [TableKind::Notes, TableKind::Links, TableKind::Tags] {
        schema_provider
            .register_table(
                kind.table_name().to_string(),
                Arc::new(ObsidianTable::new(kind, path.to_string(), opts.clone())),
            )
            .map_err(|e| {
                anyhow::anyhow!(
                    "obsidian source '{name}': failed to register {OBSIDIAN_SCHEMA}.{}: {e}",
                    kind.table_name()
                )
            })?;
    }
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog
        .register_schema(OBSIDIAN_SCHEMA, schema_provider)
        .map_err(|e| {
            anyhow::anyhow!(
                "obsidian source '{name}': failed to register schema '{OBSIDIAN_SCHEMA}': {e}"
            )
        })?;
    session_ctx.register_catalog(name, catalog);

    tracing::info!(
        source = %name,
        root = %path,
        exclude_globs = ?opts.exclude_globs(),
        max_file_bytes = opts.max_file_bytes,
        surface_version = OBSIDIAN_SURFACE_VERSION,
        "Obsidian source registered"
    );
    Ok(())
}

/// Registration-time root check. Local: must exist and be a directory. S3:
/// one non-recursive list, run on the blocking pool so the S3 client is built
/// and driven on one runtime (the same shape the scan uses).
async fn check_root(name: &str, path: &str) -> Result<()> {
    let unavailable = |cause: String| ObsidianError::RootUnavailable {
        name: name.to_string(),
        path: path.to_string(),
        cause,
    };
    let loc = Loc::parse(path).map_err(|e| unavailable(e.to_string()))?;
    match loc {
        Loc::Local(dir) => {
            let meta = tokio::fs::metadata(&dir)
                .await
                .map_err(|e| unavailable(e.to_string()))?;
            if !meta.is_dir() {
                return Err(ObsidianError::RootNotDirectory {
                    name: name.to_string(),
                    path: path.to_string(),
                }
                .into());
            }
            Ok(())
        }
        Loc::S3 { .. } => {
            let uri = path.to_string();
            let listed = tokio::task::spawn_blocking(move || -> Result<()> {
                let (store, prefix) = BlobStore::resolve(&uri)?;
                Handle::current().block_on(store.list(
                    &prefix,
                    ListOptions {
                        recursive: false,
                        follow_symlinks: false,
                    },
                ))?;
                Ok(())
            })
            .await
            .context("obsidian: root check task panicked or was cancelled")?;
            listed.map_err(|e| unavailable(format!("{e:#}")))?;
            Ok(())
        }
    }
}
```

- [ ] **Step 3: Format and checkpoint**

```bash
cargo fmt --all
```

CI expectation: all `obsidian::tests` pass. If `arrow_typeof` prints the timestamp type differently on this DataFusion version, adjust that one assertion to the printed form (it is a display-format pin, not a behavior pin). If `unnest(aliases)` in a subquery is rejected by the planner, rewrite the alias-repair query as `CROSS JOIN UNNEST(n.aliases) AS u(alias)` and mirror the change in `docs/obsidian.md` (Task 10).

Checkpoint — suggested commit: `feat(obsidian): register_obsidian_tables — catalog registration, root check, SQL integration tests`.

---

### Task 9: Server wiring — `DataSourceType::Obsidian`, feature flag, `config.rs` arm, exhaustive matches

**Files:**
- Modify: `crates/skardi/src/sources/data_source_type.rs` (enum variant + `as_str` + roundtrip test)
- Modify: `crates/skardi/src/jobs/executor.rs:~379` (read-only destination arm)
- Modify: `crates/server/Cargo.toml` (`obsidian` feature)
- Modify: `crates/server/src/config.rs:854` (`CATALOG_SUPPORTED_SOURCES`), `~1938-2045` (registration arm), tests `~3107`, `~3231`, `~3403`
- Modify: `crates/server/src/pipeline_handlers.rs:527-548` (path arm)
- Test: unit tests in `data_source_type.rs` and `config.rs`

**Interfaces:**
- Consumes: `skardi::sources::providers::obsidian::register_obsidian_tables(session_ctx, name, path, options, read_write, hierarchy_level)` (Task 8).
- Produces: `DataSourceType::Obsidian` (serde string `"obsidian"`), `skardi-server` feature `obsidian`.

- [ ] **Step 1: Write the failing tests**

In `data_source_type.rs` tests, next to the existing `Rss` roundtrip test:

```rust
#[test]
fn obsidian_roundtrips_as_lowercase_string() {
    assert_eq!(DataSourceType::Obsidian.as_str(), "obsidian");
    let json = serde_json::to_string(&DataSourceType::Obsidian).unwrap();
    assert_eq!(json, "\"obsidian\"");
    let back: DataSourceType = serde_json::from_str("\"obsidian\"").unwrap();
    assert_eq!(back, DataSourceType::Obsidian);
}
```

In `server/src/config.rs` tests, next to the `rss_source` helper (`~3107`):

```rust
fn obsidian_source(
    name: &str,
    path: &str,
    options: Option<HashMap<String, String>>,
    access_mode: AccessMode,
) -> DataSourceConfig {
    DataSourceConfig {
        name: name.to_string(),
        source_type: DataSourceType::Obsidian,
        path: PathBuf::from(path),
        connection_string: None,
        schema: None,
        database: None,
        options,
        hierarchy_level: HierarchyLevel::Catalog,
        access_mode,
        enable_cache: false,
        description: None,
        open_connector: None,
        rss: None,
        graph: None,
    }
}
```

(Copy the field list from `rss_source` verbatim and change only `source_type`, `path`, `options`, `access_mode`; if `rss_source` lacks a `database` field, drop it here too — the helper must compile against the struct as it is.)

```rust
#[test]
fn obsidian_rejects_reserved_catalog_names() {
    for reserved in ["datafusion", "information_schema"] {
        let cfg = obsidian_source(reserved, "/tmp/vault", None, AccessMode::ReadOnly);
        let err = validate_data_source(&cfg).unwrap_err();
        assert!(err.to_string().contains(reserved), "{err}");
    }
}

#[test]
fn obsidian_rejects_table_option_like_other_catalog_sources() {
    let opts = HashMap::from([("table".to_string(), "notes".to_string())]);
    let cfg = obsidian_source("vault", "/tmp/vault", Some(opts), AccessMode::ReadOnly);
    let err = validate_data_source(&cfg).unwrap_err();
    assert!(err.to_string().contains("table"), "{err}");
}

#[cfg(not(feature = "obsidian"))]
#[tokio::test]
async fn obsidian_without_feature_names_the_feature() {
    let mut ctx = SessionContext::new();
    let cfg = obsidian_source("vault", "/tmp/vault", None, AccessMode::ReadOnly);
    let err = register_data_source(&mut ctx, &cfg).await.unwrap_err();
    assert!(err.to_string().contains("`obsidian` feature"), "{err}");
}

#[cfg(feature = "obsidian")]
#[tokio::test]
async fn obsidian_registers_via_context_and_queries() {
    let root = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../skardi/src/sources/providers/obsidian/fixtures/vault"
    );
    let mut ctx = SessionContext::new();
    let cfg = obsidian_source("vault", root, None, AccessMode::ReadOnly);
    register_data_source(&mut ctx, &cfg).await.expect("fixture registers");
    let batches = ctx
        .sql("SELECT count(*) FROM vault.main.notes")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 12);
}

#[cfg(feature = "obsidian")]
#[tokio::test]
async fn obsidian_read_write_is_a_registration_failure() {
    let root = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../skardi/src/sources/providers/obsidian/fixtures/vault"
    );
    let mut ctx = SessionContext::new();
    let cfg = obsidian_source("vault", root, None, AccessMode::ReadWrite);
    let err = register_data_source(&mut ctx, &cfg).await.unwrap_err();
    assert!(matches!(err, ConfigError::DataSourceRegistrationFailed { .. }), "{err}");
    assert!(err.to_string().contains("read-only"), "{err}");
}
```

Use the same validation / registration entry points the neighbouring `rss` tests call (`~3231` for the `table`-option rejection and `~3403` for the featureless test); the names above (`validate_data_source`, `register_data_source`) must be replaced by whatever those tests actually invoke — read the two rss tests first and mirror them exactly.

- [ ] **Step 2: Add the enum variant**

In `crates/skardi/src/sources/data_source_type.rs`, add after `Rss`:

```rust
    /// Obsidian vault exposed as `notes` / `links` / `tags` (read-only, catalog-level).
    Obsidian,
```

and in `as_str`:

```rust
            DataSourceType::Obsidian => "obsidian",
```

Follow whatever serde attribute the enum uses (`#[serde(rename_all = "lowercase")]` or per-variant renames) so the JSON form is `"obsidian"`.

- [ ] **Step 3: Satisfy the exhaustive matches**

`crates/skardi/src/jobs/executor.rs` (~379), alongside the Documents/Rss/Graph read-only arms:

```rust
            Some(DataSourceType::Obsidian) => Err(JobSubmitError::Internal(anyhow!(
                "obsidian sources are read-only and cannot be used as a job destination"
            ))),
```

`crates/server/src/pipeline_handlers.rs:527-548`: add `| DataSourceType::Obsidian` to the arm that treats `path` as the source's location (the one that already lists `Documents` and `Rss`).

`cargo check --all` in CI is the safety net: every remaining non-exhaustive match on `DataSourceType` fails compilation, and each gets the same read-only / path-based treatment as `Rss`.

- [ ] **Step 4: Feature flag and `CATALOG_SUPPORTED_SOURCES`**

`crates/server/Cargo.toml`, under `[features]` next to `rss`:

```toml
# Obsidian vault as a read-only catalog (notes/links/tags). Pulls in pulldown-cmark, glob, object_store.
obsidian = ["skardi/obsidian"]
```

If the server has an umbrella feature (`full`, `all-sources`, or similar) that lists `rss`, add `"obsidian"` to it as well.

`crates/server/src/config.rs:854`:

```rust
/// Source types that register as a whole catalog (`<name>.<schema>.<table>`).
/// These reject the reserved catalog names and the `table` / `schema` /
/// `database` options that only make sense for table-level sources.
const CATALOG_SUPPORTED_SOURCES: &[DataSourceType] = &[
    // … existing entries …
    // Obsidian vault: catalog `<name>`, schema `main`, tables notes/links/tags.
    DataSourceType::Obsidian,
];
```

- [ ] **Step 5: The registration arm**

In `config.rs` next to the `DataSourceType::Rss` arm (~1938-2045), following its exact shape (feature-gated arm + `cfg(not)` twin):

```rust
        #[cfg(feature = "obsidian")]
        DataSourceType::Obsidian => {
            let path_str = source.path.to_str().ok_or_else(|| ConfigError::NonUtf8Path {
                name: source.name.clone(),
                path: source.path.clone(),
            })?;
            skardi::sources::providers::obsidian::register_obsidian_tables(
                session_ctx,
                &source.name,
                path_str,
                source.options.as_ref(),
                source.access_mode.is_read_write(),
                source.hierarchy_level,
            )
            .await
            .map_err(|error| ConfigError::DataSourceRegistrationFailed {
                name: source.name.clone(),
                error: format!("{error:#}"),
            })?;
        }
        #[cfg(not(feature = "obsidian"))]
        DataSourceType::Obsidian => {
            return Err(ConfigError::DataSourceRegistrationFailed {
                name: source.name.clone(),
                error: "obsidian data source type requires the `obsidian` feature to be enabled at build time"
                    .to_string(),
            });
        }
```

Match the `error` field's type to `ConfigError::DataSourceRegistrationFailed` as declared (if it is `anyhow::Error` or `Box<dyn Error>`, pass `error` / `error.into()` instead of the formatted string). Match the access-mode accessor to what `AccessMode` actually exposes (`is_read_write()`, `== AccessMode::ReadWrite`, …).

- [ ] **Step 6: Format and checkpoint**

```bash
cargo fmt --all
```

CI expectation: `cargo check --all` compiles every feature combination CI uses; the `#[cfg(not(feature = "obsidian"))]` test runs only in the no-feature job; the via-context tests pass with `--all-features`.

Checkpoint — suggested commit: `feat(server): wire DataSourceType::Obsidian — feature flag, catalog guard, registration arm`.

---

### Task 10: Documentation — `docs/obsidian.md`, README row, spec status

**Files:**
- Create: `docs/obsidian.md`
- Modify: `README.md:266-276` (source table row)
- Modify: `docs/superpowers/specs/2026-09-02-obsidian-source-design.md` (Status line, branch, recorded deviations)

**Interfaces:** none (docs only). Every SQL example below is one the Task 8 tests execute verbatim, so the docs cannot drift from behavior without a test failing.

- [ ] **Step 1: Write `docs/obsidian.md`**

````markdown
# Obsidian vault source

Query an [Obsidian](https://obsidian.md) vault with SQL. Skardi reads the
Markdown files directly — no plugin, no sync service, no cache — and exposes
the vault as three tables: every note, every link (resolved the way Obsidian
resolves it), and every tag.

## Enabling

The provider is behind a Cargo feature so the default build does not carry
its dependencies (`pulldown-cmark`, `glob`, `object_store`):

```bash
cargo build -p skardi-server --features obsidian
```

## Configuration

```yaml
data_sources:
  - name: vault                       # becomes the catalog name
    type: obsidian
    path: /Users/me/Notes             # vault root; s3://bucket/prefix also works
    hierarchy_level: catalog          # required — tables live under vault.main.*
    options:                          # all optional
      exclude_globs: ".obsidian/**,.trash/**"   # comma-separated; default shown
      max_file_bytes: "16777216"                # default 16 MiB; larger notes are skipped with a warning
```

Rules enforced at registration (the server refuses to start otherwise):

- `hierarchy_level` must be `catalog`; the source always owns the whole
  `<name>.main` namespace.
- `access_mode` must be read-only. Obsidian sources cannot be job destinations.
- `path` must exist and be a directory (for `s3://`, one non-recursive list must succeed).
- Unknown option keys are rejected by name. `max_file_bytes` must be a positive integer.
- `name` cannot be `datafusion` or `information_schema`, and the `table` /
  `schema` / `database` options are not accepted.

Nothing is parsed at registration: a vault that is unreadable at query time
fails the query, not the server.

## Tables

All three live under `<name>.main`. Rows are ordered by note path
(byte order), then by position within the note.

### `notes` — one row per `.md` file

| column | type | notes |
|---|---|---|
| `path` | Utf8 | vault-relative, `/`-separated, e.g. `Projects/Design.md` |
| `name` | Utf8 | file stem, what `[[Name]]` refers to |
| `folder` | Utf8 | parent folder, `""` at the root |
| `body` | Utf8 | Markdown *after* the frontmatter block |
| `frontmatter_json` | Utf8, nullable | YAML frontmatter as a JSON object; `NULL` when absent or invalid |
| `frontmatter_error` | Utf8, nullable | parse error text when the block is present but invalid |
| `aliases` | List<Utf8>, nullable | the `aliases` key, one string or a list; `NULL` when absent |
| `size_bytes` | Int64 | file size |
| `modified_at` | Timestamp(ms, UTC) | file mtime |

### `links` — one row per link, resolved

| column | type | notes |
|---|---|---|
| `from_path` | Utf8 | note containing the link |
| `to_path` | Utf8, nullable | resolved target path; `NULL` for `missing`, `ambiguous`, `external` |
| `target` | Utf8 | the link text as written, before resolution (full URL for externals) |
| `kind` | Utf8 | `wikilink`, `embed`, `markdown`, `external` |
| `display_text` | Utf8, nullable | `[[X\|text]]`, `[text](x)`, image alt; `NULL` for autolinks and bare wikilinks |
| `heading` | Utf8, nullable | `[[Note#Heading]]` → `Heading` |
| `block_id` | Utf8, nullable | `[[Note#^abc]]` → `abc` |
| `resolution` | Utf8 | `exact`, `name`, `ambiguous`, `missing`, `external` |
| `source` | Utf8 | `body` or `frontmatter` |
| `line` | Int32, nullable | 1-based source line; `NULL` for frontmatter links |

### `tags` — one row per (note, tag), deduplicated per source

| column | type | notes |
|---|---|---|
| `path` | Utf8 | |
| `tag` | Utf8 | without `#`, nested tags keep their `/` (`project/skardi`) |
| `source` | Utf8 | `body` or `frontmatter` |

Every schema carries the metadata key `skardi.obsidian.surface_version`
(currently `1`); it changes only when a column is renamed, removed or retyped.

## What gets parsed

- **Files:** every `*.md` under the root, minus `exclude_globs`
  (case-insensitive, `**` crosses folders), minus files over `max_file_bytes`,
  minus symlinks (never followed, never read — see Security).
- **Frontmatter:** a `---` block starting on line 1, closed by `---` or `...`.
  Invalid YAML or a non-mapping document keeps the note and fills
  `frontmatter_error`. `tags` (or `tag`) may be a list or a comma/space
  separated string; a leading `#` is stripped. `[[wikilinks]]` inside string
  values are extracted as `source = 'frontmatter'` links with `line = NULL`.
- **Body tags:** `#tag` at the start of a line or after whitespace, letters,
  digits, `_`, `/`, `-` (Unicode letters included). Not inside code spans or
  fenced blocks.
- **Body links:** `[[wikilinks]]` and `![[embeds]]` by regex over the text
  with code masked out; `[text](target)`, `![alt](target)`, `<autolinks>` via
  pulldown-cmark. Targets are percent-decoded and split at `#` into path,
  heading, and `^block`.

## How links resolve

Skardi mirrors Obsidian's rules, not a general file resolver:

| link as written | rule | `resolution` |
|---|---|---|
| `[[Name]]`, `[[Name#H]]`, `[[Name\|t]]` | exactly one note named `Name` anywhere → that note | `name` |
| same, several notes share `Name` | none picked | `ambiguous` |
| `[[Folder/Name]]` | root-relative exact path (`.md` optional) | `exact` / `missing` |
| `[[./X]]`, `[[../X]]` | relative to the linking note's folder | `exact` / `missing` |
| `[[Note.md]]`, `[[Note v2.1]]` | tries a root-level exact path first, then falls back to the name rule | `exact` or `name` |
| `[text](Note.md)`, `[text](../Note.md)` | relative to the linking note (Markdown semantics), then name fallback | `exact` / `name` / `missing` |
| `[text](/Folder/Note.md)` | root-relative | `exact` / `missing` |
| `[[]]` / `[[#Heading]]` | the note itself | `exact` |
| `https://…`, `mailto:…`, any `scheme:` | never resolved; `target` keeps the full URL | `external` |

Aliases are **not** resolved. Obsidian only offers aliases in autocomplete;
`[[Alias]]` in a file is a plain `Name` lookup and shows up as `missing`.
That is exactly what makes the alias-repair query below possible.

## Cost model

Every query scans the whole vault: list, read, parse. There is no cache, so
edits are visible on the next query and nothing needs reloading. A query that
touches two tables scans twice. For a few thousand notes this is tens of
milliseconds; for an `s3://` root every query is one `LIST` plus one `GET`
per note — budget the egress accordingly.

Notes larger than `max_file_bytes` are skipped with a `warn` log naming the
path. A vault where *every* read fails (permissions, a mounted drive that went
away) fails the query with the first cause instead of silently returning zero
rows.

## Security

- Symlinks under the root are never followed. Listing skips them, and each
  read opens the file with `O_NOFOLLOW`, so a symlink swapped in between
  listing and reading is refused rather than followed.
- On non-Unix targets the no-follow open is approximated by a `symlink_metadata`
  check before the read, which leaves a small race window. Run on Unix if that
  matters.
- Nothing is written, ever. `access_mode: read_write` is a startup error.

## Example queries

Most-linked notes:

```sql
SELECT to_path, count(*) AS n
FROM vault.main.links
WHERE to_path IS NOT NULL
GROUP BY to_path
ORDER BY n DESC, to_path
LIMIT 10;
```

Orphans — notes nothing links to:

```sql
SELECT n.path
FROM vault.main.notes n
LEFT JOIN vault.main.links l ON l.to_path = n.path
WHERE l.to_path IS NULL
ORDER BY n.path;
```

Alias repair — broken links that match another note's alias:

```sql
SELECT l.from_path, l.target, a.path AS probably_meant
FROM vault.main.links l
JOIN (
  SELECT path, unnest(aliases) AS alias
  FROM vault.main.notes
  WHERE aliases IS NOT NULL
) a ON a.alias = l.target
WHERE l.resolution = 'missing'
ORDER BY l.from_path;
```

Frontmatter fields as columns:

```sql
SELECT path,
       json_get_str(frontmatter_json, 'status') AS status
FROM vault.main.notes
WHERE frontmatter_json IS NOT NULL;
```

(Requires the JSON functions your build ships; `frontmatter_json` is plain
text otherwise.)

Tags per folder:

```sql
SELECT n.folder, t.tag, count(*) AS notes
FROM vault.main.tags t
JOIN vault.main.notes n ON n.path = t.path
GROUP BY n.folder, t.tag
ORDER BY notes DESC;
```
````

If `json_get_str` is not a function available in the server build, replace that example with one using only built-in string functions (e.g. `frontmatter_json LIKE '%"status":"draft"%'`) rather than documenting something that does not run.

- [ ] **Step 2: README source table row**

In the source table at `README.md:266-276`, add after the `RSS` row (same column layout as its neighbours):

```markdown
| Obsidian | Read | Yes | Vault as `notes`/`links`/`tags`; frontmatter JSON, resolved link graph, tags | [docs](docs/obsidian.md) |
```

If the table has a "feature flag" column, put `obsidian` there; otherwise mention the flag in the description cell (`(feature \`obsidian\`)`).

- [ ] **Step 3: Update the spec's status and record deviations**

In `docs/superpowers/specs/2026-09-02-obsidian-source-design.md`:

- Change the `Status:` line to `Implemented (plan: docs/superpowers/plans/2026-09-03-obsidian-source.md, branch feature/obsidian-source)`.
- Append a section:

```markdown
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
```

- [ ] **Step 4: Format and checkpoint**

```bash
cargo fmt --all
```

(No Rust changed in this task; the command is a no-op guard so the push habit stays uniform.)

CI expectation: unchanged from Task 9; `cargo doc` still clean because docs here are Markdown files, not rustdoc.

Checkpoint — suggested commit: `docs(obsidian): docs/obsidian.md, README row, spec status + recorded deviations`.

---

## Done criteria

- CI green on `feature/obsidian-source` for every job (`fmt`, `check --all`, `nextest --all-features`, `test --doc`, `doc -D warnings`).
- `SELECT count(*) FROM vault.main.notes` over the fixture returns 12 through the server config path (Task 9's via-context test).
- `docs/obsidian.md` exists and every SQL example in it appears in a Task 8 test.
- Spec `Status:` reads `Implemented`.
