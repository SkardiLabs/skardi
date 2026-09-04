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
//! read time ([`Symlinks::NoneBeneath`]: every path component below
//! the root is opened with `O_NOFOLLOW` on unix) because a symlink inside a
//! vault pointing outside it would otherwise let `path: ~/vault` read
//! arbitrary files, and a file or a directory can be swapped for a symlink
//! between the two calls.

use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use object_store::path::Path as OsPath;
#[cfg(feature = "documents")]
use object_store::{Attribute, Attributes, PutOptions, PutPayload};
use object_store::{ObjectMeta, ObjectStore};

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

/// Which symlinks [`BlobStore::get`] tolerates on a local path. Ignored for
/// S3, which has no symlinks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Symlinks<'a> {
    /// `std::fs::read`: symlinks anywhere on the path are followed.
    Follow,
    /// Refuse every symlink between the listed prefix and the file. The
    /// prefix (the vault root) is operator configuration and is opened
    /// normally, symlink or not; each component beneath it is then opened
    /// relative to its parent with `O_NOFOLLOW` (unix), so a directory *or*
    /// a file swapped for a symlink after listing is refused. The final open
    /// is non-blocking and the handle must be a regular file: a FIFO named
    /// `note.md` would otherwise stall the scan waiting for a writer.
    NoneBeneath(&'a Loc),
}

/// How [`BlobStore::get`] reads one object/file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadOptions<'a> {
    pub symlinks: Symlinks<'a>,
    /// Refuse to buffer more than this many bytes. The listing's `size` is a
    /// snapshot: a file or object that grows or is replaced between `list`
    /// and `get` would otherwise be read in full, whatever a caller's
    /// listing-time cap said. With a cap set the read stops at
    /// `max_bytes + 1` observed bytes and fails with [`SizeCapExceeded`],
    /// which callers can tell apart from an I/O error.
    pub max_bytes: Option<u64>,
}

impl<'a> ReadOptions<'a> {
    /// Follow symlinks, no cap: what `documents` has always done.
    pub fn follow() -> Self {
        ReadOptions {
            symlinks: Symlinks::Follow,
            max_bytes: None,
        }
    }

    /// Refuse symlinks anywhere under `root` (see [`Symlinks::NoneBeneath`]).
    pub fn no_symlinks_beneath(root: &'a Loc) -> Self {
        ReadOptions {
            symlinks: Symlinks::NoneBeneath(root),
            max_bytes: None,
        }
    }

    /// Stop reading past `max_bytes` bytes.
    pub fn with_max_bytes(mut self, max_bytes: u64) -> Self {
        self.max_bytes = Some(max_bytes);
        self
    }
}

/// A read hit [`ReadOptions::max_bytes`]. Its own type because a caller that
/// enforces a size policy at listing time needs to classify this as the same
/// policy skip, not as an unreadable file: `err.downcast_ref::<SizeCapExceeded>()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SizeCapExceeded {
    /// The file path or object key that was too large.
    pub target: String,
    /// The cap that was exceeded.
    pub max_bytes: u64,
}

impl std::fmt::Display for SizeCapExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "blob: {} exceeds max_bytes ({}); it grew or was replaced after listing",
            self.target, self.max_bytes
        )
    }
}

impl std::error::Error for SizeCapExceeded {}

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
#[cfg(feature = "documents")]
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

/// Local-vs-object-store I/O backend shared by the `documents` and `obsidian`
/// sources.
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

    /// Fetch the full bytes of one object/file, subject to
    /// [`ReadOptions::max_bytes`].
    pub async fn get(&self, loc: &Loc, opts: ReadOptions<'_>) -> Result<Vec<u8>> {
        match (self, loc) {
            (BlobStore::Local, Loc::Local(path)) => match opts.symlinks {
                Symlinks::Follow => {
                    let file = std::fs::File::open(path)
                        .with_context(|| format!("reading {}", path.display()))?;
                    read_capped(file, opts.max_bytes, &path.display().to_string())
                }
                Symlinks::NoneBeneath(Loc::Local(root)) => {
                    read_local_no_follow(root, path, opts.max_bytes)
                }
                Symlinks::NoneBeneath(Loc::S3 { .. }) => anyhow::bail!(
                    "blob: NoneBeneath needs a local root for {}",
                    path.display()
                ),
            },
            (BlobStore::Remote(store), Loc::S3 { key, .. }) => {
                let res = store
                    .get(&OsPath::from(key.as_str()))
                    .await
                    .with_context(|| format!("s3 get {key}"))?;
                // The listing's size is a snapshot; this one comes with the
                // body, so an object that grew since is refused before the
                // first chunk is buffered.
                if let Some(max) = opts.max_bytes {
                    if res.meta.size > max {
                        return Err(SizeCapExceeded {
                            target: key.clone(),
                            max_bytes: max,
                        }
                        .into());
                    }
                }
                let mut buf: Vec<u8> = Vec::new();
                let mut stream = res.into_stream();
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk.with_context(|| format!("s3 read body {key}"))?;
                    // A store that streams more than it advertised (or an
                    // object replaced mid-transfer) is cut off here: dropping
                    // the stream aborts the transfer.
                    if let Some(max) = opts.max_bytes {
                        if buf.len() as u64 + chunk.len() as u64 > max {
                            return Err(SizeCapExceeded {
                                target: key.clone(),
                                max_bytes: max,
                            }
                            .into());
                        }
                    }
                    buf.extend_from_slice(&chunk);
                }
                Ok(buf)
            }
            _ => anyhow::bail!("blob: BlobStore/Loc backend mismatch in get()"),
        }
    }

    /// Write bytes to one object/file (image crops / page renders). Only
    /// `documents` writes; `obsidian` is read-only by contract, so the writer
    /// (and its object_store attribute imports) stay behind that feature.
    #[cfg(feature = "documents")]
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
    /// source's blocking scan/parse thread) to avoid reqwest connection-pool cross-runtime
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
                // Only directories and regular files go on: a FIFO, socket or
                // device named `x.md` is not a note, and a blocking open on it
                // would stall the whole scan. Fail closed: an entry that cannot
                // be typed (unlinked between readdir and stat) is skipped
                // rather than handed to `is_dir` below, which does follow.
                match entry.file_type() {
                    Ok(kind) if kind.is_dir() || kind.is_file() => {}
                    Ok(kind) if kind.is_symlink() => {
                        tracing::warn!(path = %path.display(), "blob: skipping symlink");
                        continue;
                    }
                    Ok(_) => {
                        tracing::warn!(path = %path.display(), "blob: skipping special file (not a regular file or directory)");
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!(path = %path.display(), error = %e, "blob: cannot type entry, skipping");
                        continue;
                    }
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

/// Read an already-opened file, refusing to buffer more than `max_bytes`.
/// Reads one byte past the cap so growth after the `fstat` is caught too, and
/// the allocation is bounded by the cap rather than by the claimed length.
fn read_capped(file: std::fs::File, max_bytes: Option<u64>, target: &str) -> Result<Vec<u8>> {
    use std::io::Read;

    let Some(max) = max_bytes else {
        let mut buf = Vec::new();
        let mut file = file;
        file.read_to_end(&mut buf)
            .with_context(|| format!("reading {target}"))?;
        return Ok(buf);
    };
    let ceiling = max.saturating_add(1);
    let hint = file.metadata().map(|m| m.len().min(ceiling)).unwrap_or(0);
    let mut buf = Vec::with_capacity(usize::try_from(hint).unwrap_or(0));
    file.take(ceiling)
        .read_to_end(&mut buf)
        .with_context(|| format!("reading {target}"))?;
    if buf.len() as u64 > max {
        return Err(SizeCapExceeded {
            target: target.to_string(),
            max_bytes: max,
        }
        .into());
    }
    Ok(buf)
}

/// Read `path`, which the listing found beneath `root`, without following a
/// symlink at any component below the root.
///
/// Unix: the root is opened normally (it is operator configuration and may
/// itself be a symlink), then every component of the relative remainder is
/// opened with `openat(parent_fd, name, O_NOFOLLOW | …)`: directories with
/// `O_DIRECTORY`, the file with `O_NONBLOCK`. A symlink at any level fails its
/// open with `ELOOP` instead of being traversed, so the listing→read race is
/// closed for the whole path, not just its last component. The non-blocking
/// final open cannot stall on a FIFO waiting for a writer; the handle is then
/// `fstat`ed and anything but a regular file is refused (`O_NONBLOCK` is inert
/// on a regular file).
#[cfg(unix)]
fn read_local_no_follow(root: &Path, path: &Path, max_bytes: Option<u64>) -> Result<Vec<u8>> {
    use std::ffi::CString;
    use std::fs::File;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::OpenOptionsExt;

    let rel = path
        .strip_prefix(root)
        .with_context(|| format!("blob: {} is not beneath {}", path.display(), root.display()))?;
    let root_dir = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
        .open(root)
        .with_context(|| format!("blob: opening root directory {}", root.display()))?;
    let mut dir = OwnedFd::from(root_dir);
    let mut file: Option<File> = None;
    let mut components = rel.components().peekable();
    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            anyhow::bail!(
                "blob: refusing path component {component:?} in {}",
                path.display()
            );
        };
        let c_name = CString::new(name.as_bytes())
            .with_context(|| format!("blob: NUL byte in {}", path.display()))?;
        let last = components.peek().is_none();
        let flags = libc::O_RDONLY
            | libc::O_NOFOLLOW
            | libc::O_CLOEXEC
            | if last {
                libc::O_NONBLOCK
            } else {
                libc::O_DIRECTORY
            };
        // SAFETY: `dir` is an open directory descriptor owned by this frame
        // and `c_name` is a NUL-terminated string that outlives the call;
        // `openat` reads both and writes nothing into our memory.
        let fd = unsafe { libc::openat(dir.as_raw_fd(), c_name.as_ptr(), flags) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error()).with_context(|| {
                format!(
                    "blob: opening {} without following symlinks (component {})",
                    path.display(),
                    name.to_string_lossy()
                )
            });
        }
        // SAFETY: `fd` was just returned by a successful `openat` and nothing
        // else owns it; `OwnedFd` closes it exactly once.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        if last {
            file = Some(File::from(fd));
        } else {
            dir = fd;
        }
    }
    let file = file.ok_or_else(|| {
        anyhow::anyhow!("blob: {} is the root itself, not a file", path.display())
    })?;
    let meta = file
        .metadata()
        .with_context(|| format!("blob: stat {}", path.display()))?;
    if !meta.file_type().is_file() {
        anyhow::bail!("blob: {} is not a regular file", path.display());
    }
    // `fstat` on the very handle being read: a file that grew since the
    // listing is refused without reading it, and `read_capped` still stops one
    // byte past the cap in case it grows now.
    if let Some(max) = max_bytes {
        if meta.len() > max {
            return Err(SizeCapExceeded {
                target: path.display().to_string(),
                max_bytes: max,
            }
            .into());
        }
    }
    read_capped(file, max_bytes, &path.display().to_string())
}

/// Non-unix fallback: `symlink_metadata` on every component beneath `root`,
/// then a regular-file check, before reading. There is a residual
/// time-of-check/time-of-use window between the checks and the read;
/// documented in `docs/obsidian.md`.
#[cfg(not(unix))]
fn read_local_no_follow(root: &Path, path: &Path, max_bytes: Option<u64>) -> Result<Vec<u8>> {
    let rel = path
        .strip_prefix(root)
        .with_context(|| format!("blob: {} is not beneath {}", path.display(), root.display()))?;
    if rel.as_os_str().is_empty() {
        anyhow::bail!("blob: {} is the root itself, not a file", path.display());
    }
    let mut current = root.to_path_buf();
    for component in rel.components() {
        let Component::Normal(name) = component else {
            anyhow::bail!(
                "blob: refusing path component {component:?} in {}",
                path.display()
            );
        };
        current.push(name);
        let meta = std::fs::symlink_metadata(&current).with_context(|| {
            format!(
                "blob: stat {} without following symlinks",
                current.display()
            )
        })?;
        if meta.file_type().is_symlink() {
            anyhow::bail!(
                "blob: opening {} without following symlinks: {} is a symlink",
                path.display(),
                current.display()
            );
        }
    }
    let meta = std::fs::symlink_metadata(&current)
        .with_context(|| format!("blob: stat {}", path.display()))?;
    if !meta.file_type().is_file() {
        anyhow::bail!("blob: {} is not a regular file", path.display());
    }
    let file =
        std::fs::File::open(&current).with_context(|| format!("reading {}", path.display()))?;
    read_capped(file, max_bytes, &path.display().to_string())
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
    use object_store::PutPayload;

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

    #[cfg(feature = "documents")]
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
            store.get(&top.loc, ReadOptions::follow()).await.unwrap(),
            b"TOP"
        );

        // put creates parent dirs and writes bytes (documents-only writer).
        #[cfg(feature = "documents")]
        {
            let out = Loc::Local(dir.path().join("crops/a_0.png"));
            store.put(&out, b"\x89PNG").await.unwrap();
            assert_eq!(
                std::fs::read(dir.path().join("crops/a_0.png")).unwrap(),
                b"\x89PNG"
            );
        }
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
            blob.get(&src, ReadOptions::no_symlinks_beneath(&src))
                .await
                .unwrap(),
            b"HELLO"
        );

        // The writer is documents-only.
        #[cfg(feature = "documents")]
        {
            let dst = Loc::S3 {
                bucket: "bk".into(),
                key: "crops/a.pdf_img0.png".into(),
            };
            blob.put(&dst, b"\x89PNGcrop").await.unwrap();
            assert_eq!(
                blob.get(&dst, ReadOptions::follow()).await.unwrap(),
                b"\x89PNGcrop"
            );
        }
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
                ReadOptions::no_symlinks_beneath(&Loc::Local(dir.path().to_path_buf())),
            )
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("without following symlinks"),
            "unexpected: {err:#}"
        );
        assert_eq!(
            BlobStore::Local
                .get(&loc, ReadOptions::follow())
                .await
                .unwrap(),
            b"S"
        );
    }

    /// A directory on the path swapped for a symlink after listing is refused
    /// too: `O_NOFOLLOW` applies to every component beneath the root, not just
    /// the file. The root itself may be a symlink.
    #[cfg(unix)]
    #[tokio::test]
    async fn local_get_refuses_symlinked_directory_beneath_root() {
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("note.md"), b"S").unwrap();
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("real")).unwrap();
        std::fs::write(dir.path().join("real/note.md"), b"R").unwrap();
        // As if `real/` had been listed and then replaced by a link outside.
        std::os::unix::fs::symlink(outside.path(), dir.path().join("linkdir")).unwrap();
        let root = Loc::Local(dir.path().to_path_buf());

        let err = BlobStore::Local
            .get(
                &Loc::Local(dir.path().join("linkdir/note.md")),
                ReadOptions::no_symlinks_beneath(&root),
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
                    &Loc::Local(dir.path().join("real/note.md")),
                    ReadOptions::no_symlinks_beneath(&root)
                )
                .await
                .unwrap(),
            b"R"
        );

        // The root is operator configuration: a symlinked root is followed,
        // and the walk below it is just as strict.
        let holder = tempfile::tempdir().unwrap();
        let root_link = holder.path().join("vault");
        std::os::unix::fs::symlink(dir.path(), &root_link).unwrap();
        let linked_root = Loc::Local(root_link.clone());
        assert_eq!(
            BlobStore::Local
                .get(
                    &Loc::Local(root_link.join("real/note.md")),
                    ReadOptions::no_symlinks_beneath(&linked_root)
                )
                .await
                .unwrap(),
            b"R"
        );
        let err = BlobStore::Local
            .get(
                &Loc::Local(root_link.join("linkdir/note.md")),
                ReadOptions::no_symlinks_beneath(&linked_root),
            )
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("without following symlinks"),
            "unexpected: {err:#}"
        );

        // A path outside the root is refused before anything is opened.
        let err = BlobStore::Local
            .get(
                &Loc::Local(outside.path().join("note.md")),
                ReadOptions::no_symlinks_beneath(&root),
            )
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("is not beneath"),
            "unexpected: {err:#}"
        );
    }

    /// A FIFO named like a note is skipped by the strict listing and, if it
    /// shows up after listing, refused by a non-blocking open instead of
    /// stalling the scan until a writer appears. The read runs on its own
    /// thread with a deadline so a regression fails instead of hanging CI.
    #[cfg(unix)]
    #[tokio::test]
    async fn local_fifo_is_skipped_and_never_blocks() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("real.md"), b"R").unwrap();
        let fifo = dir.path().join("pipe.md");
        let status = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap();
        assert!(status.success(), "mkfifo failed: {status}");
        let root = Loc::Local(dir.path().to_path_buf());

        let strict = BlobStore::Local
            .list(
                &root,
                ListOptions {
                    recursive: true,
                    follow_symlinks: false,
                },
            )
            .await
            .unwrap();
        let rels: Vec<&str> = strict.iter().map(|e| e.rel_key.as_str()).collect();
        assert_eq!(rels, vec!["real.md"]);

        let (tx, rx) = std::sync::mpsc::channel();
        let root_path = dir.path().to_path_buf();
        std::thread::spawn(move || {
            let outcome =
                read_local_no_follow(&root_path, &fifo, None).map_err(|e| format!("{e:#}"));
            let _ = tx.send(outcome);
        });
        let outcome = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("opening a FIFO must not block");
        let msg = outcome.unwrap_err();
        assert!(msg.contains("is not a regular file"), "{msg}");
    }

    /// The listing's size is a snapshot: a note that grows before the read is
    /// refused by the cap instead of being buffered whole, and the error is a
    /// `SizeCapExceeded` so the caller can treat it as its own policy skip.
    #[tokio::test]
    async fn local_get_enforces_max_bytes_after_listing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("note.md");
        std::fs::write(&path, vec![b'x'; 8]).unwrap();
        let root = Loc::Local(dir.path().to_path_buf());
        let loc = Loc::Local(path.clone());

        // At the cap, and one byte over it.
        assert_eq!(
            BlobStore::Local
                .get(
                    &loc,
                    ReadOptions::no_symlinks_beneath(&root).with_max_bytes(8)
                )
                .await
                .unwrap()
                .len(),
            8
        );
        let err = BlobStore::Local
            .get(
                &loc,
                ReadOptions::no_symlinks_beneath(&root).with_max_bytes(7),
            )
            .await
            .unwrap_err();
        let cap = err
            .downcast_ref::<SizeCapExceeded>()
            .unwrap_or_else(|| panic!("not a cap error: {err:#}"));
        assert_eq!(cap.max_bytes, 7);
        assert!(cap.target.contains("note.md"), "{}", cap.target);

        // The follow-symlinks reader honors the cap too, and no cap reads all.
        assert!(
            BlobStore::Local
                .get(&loc, ReadOptions::follow().with_max_bytes(7))
                .await
                .unwrap_err()
                .downcast_ref::<SizeCapExceeded>()
                .is_some()
        );
        assert_eq!(
            BlobStore::Local
                .get(&loc, ReadOptions::follow())
                .await
                .unwrap()
                .len(),
            8
        );
    }

    #[tokio::test]
    async fn remote_get_enforces_max_bytes() {
        let store = seed_inmemory(&[("corpus/a.md", b"0123456789")]).await;
        let blob = BlobStore::Remote(store);
        let loc = Loc::S3 {
            bucket: "bk".into(),
            key: "corpus/a.md".into(),
        };
        assert_eq!(
            blob.get(&loc, ReadOptions::follow().with_max_bytes(10))
                .await
                .unwrap()
                .len(),
            10
        );
        let err = blob
            .get(&loc, ReadOptions::follow().with_max_bytes(9))
            .await
            .unwrap_err();
        let cap = err
            .downcast_ref::<SizeCapExceeded>()
            .unwrap_or_else(|| panic!("not a cap error: {err:#}"));
        assert_eq!((cap.target.as_str(), cap.max_bytes), ("corpus/a.md", 9u64));
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
