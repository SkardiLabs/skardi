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

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use object_store::path::Path as OsPath;
use object_store::{Attribute, Attributes, ObjectMeta, ObjectStore, PutOptions, PutPayload};

/// A parsed source/target location: either a local path or an S3 object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Loc {
    Local(PathBuf),
    /// `key` never has a leading `/`.
    S3 {
        bucket: String,
        key: String,
    },
}

impl Loc {
    /// Parse a URI into a [`Loc`]. A bare path or a `file:` URL is [`Loc::Local`];
    /// an `s3://bucket/key` URL is [`Loc::S3`].
    pub fn parse(uri: &str) -> Result<Loc> {
        if let Some(rest) = uri.strip_prefix("s3://") {
            let (bucket, key) = match rest.split_once('/') {
                Some((b, k)) => (b, k),
                None => (rest, ""),
            };
            if bucket.is_empty() {
                anyhow::bail!("invalid s3 uri (no bucket): {uri}");
            }
            return Ok(Loc::S3 {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }
        if let Some(path) = uri.strip_prefix("file://") {
            return Ok(Loc::Local(PathBuf::from(path)));
        }
        Ok(Loc::Local(PathBuf::from(uri)))
    }
}

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

/// Normalize an S3 prefix key so it is empty or ends with exactly one `/`.
/// This makes object listing directory-scoped: `corpus` becomes `corpus/`, so
/// it never spuriously matches sibling keys like `corpus-2/…`.
fn normalize_prefix(key: &str) -> String {
    let trimmed = key.trim_end_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        format!("{trimmed}/")
    }
}

/// The `Content-Type` to stamp on a written object, inferred from its key's
/// extension. Returns `None` when unknown (the store then applies its default).
fn content_type_for_key(key: &str) -> Option<&'static str> {
    let ext = Path::new(key)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());
    match ext.as_deref() {
        Some("png") => Some("image/png"),
        Some("jpg") | Some("jpeg") => Some("image/jpeg"),
        Some("gif") => Some("image/gif"),
        Some("tif") | Some("tiff") => Some("image/tiff"),
        Some("bmp") => Some("image/bmp"),
        _ => None,
    }
}

/// Local-vs-object-store I/O backend for the documents connector.
#[derive(Clone)]
pub enum BlobStore {
    Local,
    Remote(Arc<dyn ObjectStore>),
}

impl std::fmt::Debug for BlobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlobStore::Local => write!(f, "BlobStore::Local"),
            BlobStore::Remote(_) => write!(f, "BlobStore::Remote(..)"),
        }
    }
}

impl BlobStore {
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

    /// Write bytes to one object/file (image crops / page renders).
    pub async fn put(&self, loc: &Loc, bytes: &[u8]) -> Result<()> {
        match (self, loc) {
            (BlobStore::Local, Loc::Local(path)) => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)
                        .with_context(|| format!("creating dir {}", parent.display()))?;
                }
                std::fs::write(path, bytes).with_context(|| format!("writing {}", path.display()))
            }
            (BlobStore::Remote(store), Loc::S3 { key, .. }) => {
                let mut attributes = Attributes::new();
                if let Some(ct) = content_type_for_key(key) {
                    attributes.insert(Attribute::ContentType, ct.into());
                }
                let opts = PutOptions {
                    attributes,
                    ..Default::default()
                };
                store
                    .put_opts(
                        &OsPath::from(key.as_str()),
                        PutPayload::from(bytes.to_vec()),
                        opts,
                    )
                    .await
                    .with_context(|| format!("s3 put {key}"))?;
                Ok(())
            }
            _ => anyhow::bail!("blob: BlobStore/Loc backend mismatch in put()"),
        }
    }

    /// Build the backend + parsed location for a URI. For S3 this constructs the
    /// `object_store` client **on the calling thread's runtime** (call from the
    /// documents parse thread) to avoid reqwest connection-pool cross-runtime
    /// hazards; credentials/region come from the environment.
    pub fn resolve(uri: &str) -> Result<(BlobStore, Loc)> {
        let loc = Loc::parse(uri)?;
        let store = match &loc {
            Loc::Local(_) => BlobStore::Local,
            Loc::S3 { bucket, .. } => BlobStore::Remote(build_s3_store(bucket)?),
        };
        Ok((store, loc))
    }
}

/// Build an S3 object store for `bucket`, reading credentials/region from the
/// environment (never from config — see `remote_storage.rs`).
pub fn build_s3_store(bucket: &str) -> Result<Arc<dyn ObjectStore>> {
    use object_store::aws::AmazonS3Builder;
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .build()
        .with_context(|| format!("building S3 object store for bucket '{bucket}'"))?;
    Ok(Arc::new(store))
}

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
        .with_context(|| {
            format!(
                "blob: opening {} without following symlinks",
                path.display()
            )
        })?;
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

/// List objects under an S3 prefix. Recursive uses a flat `list`; non-recursive
/// uses a delimiter-scoped listing (one level).
async fn list_remote(
    store: &Arc<dyn ObjectStore>,
    bucket: &str,
    prefix_key: &str,
    recursive: bool,
) -> Result<Vec<BlobEntry>> {
    let norm = normalize_prefix(prefix_key);
    let os_prefix = (!norm.is_empty()).then(|| OsPath::from(norm.as_str()));

    let mut out: Vec<BlobEntry> = Vec::new();
    if recursive {
        let mut stream = store.list(os_prefix.as_ref());
        while let Some(meta) = stream.next().await {
            let meta = meta.context("s3 list")?;
            push_remote_entry(&mut out, bucket, &norm, &meta, false);
        }
    } else {
        let res = store
            .list_with_delimiter(os_prefix.as_ref())
            .await
            .context("s3 list_with_delimiter")?;
        for meta in res.objects {
            push_remote_entry(&mut out, bucket, &norm, &meta, true);
        }
    }
    out.sort_by(|a, b| a.rel_key.cmp(&b.rel_key));
    Ok(out)
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loc_parse_detects_local_and_s3() {
        assert_eq!(
            Loc::parse("/tmp/corpus").unwrap(),
            Loc::Local(PathBuf::from("/tmp/corpus"))
        );
        assert_eq!(
            Loc::parse("file:///tmp/corpus").unwrap(),
            Loc::Local(PathBuf::from("/tmp/corpus"))
        );
        assert_eq!(
            Loc::parse("s3://my-bucket/corpus/a.pdf").unwrap(),
            Loc::S3 {
                bucket: "my-bucket".to_string(),
                key: "corpus/a.pdf".to_string(),
            }
        );
    }

    #[test]
    fn loc_parse_s3_prefix_without_key() {
        assert_eq!(
            Loc::parse("s3://my-bucket").unwrap(),
            Loc::S3 {
                bucket: "my-bucket".to_string(),
                key: String::new(),
            }
        );
        assert_eq!(
            Loc::parse("s3://my-bucket/").unwrap(),
            Loc::S3 {
                bucket: "my-bucket".to_string(),
                key: String::new(),
            }
        );
    }

    #[test]
    fn loc_parse_rejects_bucketless_s3() {
        assert!(Loc::parse("s3://").is_err());
        assert!(Loc::parse("s3:///key-only").is_err());
    }

    #[test]
    fn normalize_prefix_forces_single_trailing_slash() {
        assert_eq!(normalize_prefix("corpus"), "corpus/");
        assert_eq!(normalize_prefix("corpus/"), "corpus/");
        assert_eq!(normalize_prefix("a/b/c"), "a/b/c/");
        assert_eq!(normalize_prefix(""), "");
        assert_eq!(normalize_prefix("/"), "");
    }

    #[test]
    fn content_type_inferred_from_extension() {
        assert_eq!(content_type_for_key("x/y_0.png"), Some("image/png"));
        assert_eq!(content_type_for_key("a.JPG"), Some("image/jpeg"));
        assert_eq!(content_type_for_key("a.tiff"), Some("image/tiff"));
        assert_eq!(content_type_for_key("a.pdf"), None);
        assert_eq!(content_type_for_key("noext"), None);
    }

    #[tokio::test]
    async fn local_backend_list_get_put_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("top.pdf"), b"TOP").unwrap();
        let sub = dir.path().join("sub");
        std::fs::create_dir(&sub).unwrap();
        std::fs::write(sub.join("nested.pdf"), b"NESTED").unwrap();

        let store = BlobStore::Local;
        let prefix = Loc::Local(dir.path().to_path_buf());

        // Recursive lists both, with '/'-separated rel keys, sorted.
        let listed = store
            .list(
                &prefix,
                ListOptions {
                    recursive: true,
                    follow_symlinks: true,
                },
            )
            .await
            .unwrap();
        let rels: Vec<&str> = listed.iter().map(|e| e.rel_key.as_str()).collect();
        assert_eq!(rels, vec!["sub/nested.pdf", "top.pdf"]);

        // Non-recursive lists only the top level.
        let flat = store
            .list(
                &prefix,
                ListOptions {
                    recursive: false,
                    follow_symlinks: true,
                },
            )
            .await
            .unwrap();
        let flat_rels: Vec<&str> = flat.iter().map(|e| e.rel_key.as_str()).collect();
        assert_eq!(flat_rels, vec!["top.pdf"]);

        // get returns the file bytes.
        let top = listed.iter().find(|e| e.rel_key == "top.pdf").unwrap();
        assert_eq!(
            store
                .get(
                    &top.loc,
                    ReadOptions {
                        follow_symlinks: true
                    }
                )
                .await
                .unwrap(),
            b"TOP"
        );

        // put creates parent dirs and writes bytes.
        let out = Loc::Local(dir.path().join("crops/a_0.png"));
        store.put(&out, b"\x89PNG").await.unwrap();
        assert_eq!(
            std::fs::read(dir.path().join("crops/a_0.png")).unwrap(),
            b"\x89PNG"
        );
    }

    #[tokio::test]
    async fn local_backend_missing_root_errors() {
        let store = BlobStore::Local;
        let prefix = Loc::Local(PathBuf::from("/no/such/documents/root"));
        let err = store
            .list(
                &prefix,
                ListOptions {
                    recursive: true,
                    follow_symlinks: false,
                },
            )
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("cannot read root directory"),
            "unexpected: {err:#}"
        );
    }

    async fn seed_inmemory(pairs: &[(&str, &[u8])]) -> Arc<dyn ObjectStore> {
        let store = Arc::new(object_store::memory::InMemory::new());
        for (key, bytes) in pairs {
            store
                .put(&OsPath::from(*key), PutPayload::from(bytes.to_vec()))
                .await
                .unwrap();
        }
        store
    }

    #[tokio::test]
    async fn remote_backend_prefix_is_directory_scoped() {
        // `corpus` must not match the sibling `corpus-2/` prefix.
        let store = seed_inmemory(&[
            ("corpus/a.pdf", b"A"),
            ("corpus/sub/b.pdf", b"B"),
            ("corpus-2/c.pdf", b"C"),
        ])
        .await;
        let blob = BlobStore::Remote(store);
        let prefix = Loc::S3 {
            bucket: "bk".into(),
            key: "corpus".into(), // no trailing slash on purpose
        };

        let listed = blob
            .list(
                &prefix,
                ListOptions {
                    recursive: true,
                    follow_symlinks: false,
                },
            )
            .await
            .unwrap();
        let rels: Vec<&str> = listed.iter().map(|e| e.rel_key.as_str()).collect();
        assert_eq!(rels, vec!["a.pdf", "sub/b.pdf"]);

        // Loc carries the full key.
        assert!(matches!(
            &listed[0].loc,
            Loc::S3 { bucket, key } if bucket == "bk" && key == "corpus/a.pdf"
        ));

        // Non-recursive drops the nested entry.
        let flat = blob
            .list(
                &prefix,
                ListOptions {
                    recursive: false,
                    follow_symlinks: false,
                },
            )
            .await
            .unwrap();
        let flat_rels: Vec<&str> = flat.iter().map(|e| e.rel_key.as_str()).collect();
        assert_eq!(flat_rels, vec!["a.pdf"]);
    }

    #[tokio::test]
    async fn remote_backend_get_put_roundtrip() {
        let store = seed_inmemory(&[("corpus/a.pdf", b"HELLO")]).await;
        let blob = BlobStore::Remote(store);

        let src = Loc::S3 {
            bucket: "bk".into(),
            key: "corpus/a.pdf".into(),
        };
        assert_eq!(
            blob.get(
                &src,
                ReadOptions {
                    follow_symlinks: false
                }
            )
            .await
            .unwrap(),
            b"HELLO"
        );

        let dst = Loc::S3 {
            bucket: "bk".into(),
            key: "crops/a.pdf_img0.png".into(),
        };
        blob.put(&dst, b"\x89PNGcrop").await.unwrap();
        assert_eq!(
            blob.get(
                &dst,
                ReadOptions {
                    follow_symlinks: false
                }
            )
            .await
            .unwrap(),
            b"\x89PNGcrop"
        );
    }

    #[test]
    fn resolve_local_uri_yields_local_backend() {
        let (store, loc) = BlobStore::resolve("/tmp/corpus").unwrap();
        assert!(matches!(store, BlobStore::Local));
        assert_eq!(loc, Loc::Local(PathBuf::from("/tmp/corpus")));
    }

    #[tokio::test]
    async fn local_list_carries_size_and_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("a.md");
        std::fs::write(&file, b"hello").unwrap();
        let meta = std::fs::metadata(&file).unwrap();

        let listed = BlobStore::Local
            .list(
                &Loc::Local(dir.path().to_path_buf()),
                ListOptions {
                    recursive: true,
                    follow_symlinks: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].rel_key, "a.md");
        assert_eq!(listed[0].size, 5);
        assert_eq!(
            listed[0].modified,
            DateTime::<Utc>::from(meta.modified().unwrap())
        );
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
            .list(
                &prefix,
                ListOptions {
                    recursive: true,
                    follow_symlinks: false,
                },
            )
            .await
            .unwrap();
        let rels: Vec<&str> = strict.iter().map(|e| e.rel_key.as_str()).collect();
        assert_eq!(rels, vec!["real.md"]);

        let followed = BlobStore::Local
            .list(
                &prefix,
                ListOptions {
                    recursive: true,
                    follow_symlinks: true,
                },
            )
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
            .get(
                &loc,
                ReadOptions {
                    follow_symlinks: false,
                },
            )
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("without following symlinks"),
            "unexpected: {err:#}"
        );
        assert_eq!(
            BlobStore::Local
                .get(
                    &loc,
                    ReadOptions {
                        follow_symlinks: true
                    }
                )
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
                &Loc::S3 {
                    bucket: "bk".into(),
                    key: "corpus".into(),
                },
                ListOptions {
                    recursive: true,
                    follow_symlinks: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].size, 5);
        assert!(listed[0].modified > DateTime::<Utc>::from(std::time::UNIX_EPOCH));
    }
}
