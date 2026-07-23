# `documents` connector: S3 / object-store support for source and image paths

- **Issue:** [SkardiLabs/skardi#167](https://github.com/SkardiLabs/skardi/issues/167) (question 1)
- **Status:** Draft for review
- **Date:** 2026-07-23
- **Scope:** S3 read path for the source `path`, and S3 write path for `image_store`.
  All four combinations of `{local, s3://}` source × `{local, s3://}` image store
  are supported. Predicate/prefix pushdown (issue question 2) is **out of scope**.

## 1. Motivation

The `documents` connector today is local-filesystem only:

- `parse.rs::collect_files()` walks the tree with `std::fs::read_dir`.
- `parse.rs::parse_file()` hands a **local path string** to `LiteParse::parse(path)`.
- `parse.rs::write_image_crop()` writes crops to a local `image_store` and is an
  explicit **no-op** for `s3://` stores (the ref is still recorded).

Cloud customers want to point a source at their **own** bucket
(`path: "s3://bucket/prefix/"`) rather than landing files on a mounted volume
(PVC/hostPath) inside our deployment, and to have extracted image crops written
back to a bucket they own. This design adds both directions.

### Non-goals

- Predicate/prefix pushdown, `limit` pushdown, or per-file streaming (issue
  question 2 — tracked separately). The scan stays eager: it parses the whole
  prefix into one `RecordBatch`, exactly as the local path does today.
- Non-S3 object stores (`gs://`, `az://`). The abstraction does not preclude
  them, but this design wires and tests **S3 only**, matching the convention the
  repo already enforces (`crates/server/src/remote_storage.rs`).
- Caching parsed output across scans.

## 2. Key finding that shapes the design

`liteparse` **2.4.0 already accepts in-memory bytes**:

```rust
pub async fn parse_input(&self, input: PdfInput) -> Result<ParseResult, ...>;
pub async fn screenshot_input(&self, input: PdfInput, pages) -> Result<...>;
// PdfInput::Bytes(Vec<u8>) is a first-class variant; liteparse writes its own
// temp files internally when it must convert non-PDF inputs via LibreOffice.
```

So skardi does **not** need to modify liteparse (despite the branch name). The
documents provider fetches object bytes itself and feeds `PdfInput::Bytes`.
liteparse stays a pure parser with no knowledge of cloud I/O or credentials.

## 3. Reuse of the existing S3 convention

The repo already standardizes S3 access in `crates/server/src/remote_storage.rs`
(`S3Storage`) and documents it in `docs/basic/ctx_s3_examples.yaml`. This design
**reuses that machinery** rather than introducing a second S3 stack:

- Uses the **`object_store` crate** (`object_store::aws::AmazonS3Builder`).
- **Credentials are environment-only, enforced.** `S3Storage::validate_configuration`
  *rejects* `aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`,
  and `aws_region` if present in a source's `options`. Credentials come from
  `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (+ optional `AWS_SESSION_TOKEN`),
  `AWS_PROFILE`, or an IAM role / IRSA; region from `AWS_REGION` /
  `AWS_DEFAULT_REGION`.
- Registers the built store on the DataFusion `SessionContext.runtime_env()` for
  the `s3://<bucket>/` scheme, so it can be retrieved later by URL.

Today `S3Storage::setup_object_store` is only invoked for `Csv | Parquet | Lance`
(`config.rs:1056`), because those use DataFusion's own `ListingTable`, which
reads *through* the registered store. `documents` does its **own** listing and
parsing, so it must retrieve the store handle and call `list` / `get` / `put`
directly.

## 4. Configuration & credentials

### ctx.yaml (no new option keys required)

```yaml
spec:
  data_sources:
    - name: "documents"
      type: "documents"
      access_mode: "read_only"
      path: "s3://my-bucket/corpus/"      # local dir OR s3:// prefix
      description: "Parsed corpus, one row per (file, page)"
      options:
        recursive: "true"
        include_globs: "*.pdf, *.docx"
        image_mode: "embedded"             # off | placeholder | embedded
        image_store: "s3://my-bucket/crops/"  # local dir OR s3:// prefix
        # NO aws_* keys — rejected by S3Storage::validate_configuration.
        # Credentials/region come from env / IAM role.
```

The `path` and `image_store` values are **independent** — either may be local or
`s3://`, and they may live in **different buckets**. All 2×2 combinations are
valid.

### Registration flow (`config.rs`, `DataSourceType::Documents` arm)

Before registering the `DocumentsTable`:

1. Collect the S3 buckets referenced by `path` and (if set) `options.image_store`.
2. For each source whose `path` is `s3://`, call
   `S3Storage::validate_configuration(source)` so the credential-rejection rule
   applies to `documents` exactly as it does to CSV/Parquet.
3. For each **distinct** bucket, build and register an object store via a new
   `S3Storage::setup_object_store_for_bucket` (a refactor of the existing
   `setup_object_store` that separates "build + register a bucket store" from
   "connectivity-test a single object" — see below). Registration is idempotent
   per bucket for the session.
4. Run a **prefix-aware connectivity check** (see §7) instead of the object
   `head` test.

`register_documents_tables` gains no new required parameter; the table pulls its
store handles from `runtime_env` at scan time via `TaskContext`.

## 5. Component design (Approach A — blob-backend abstraction)

New internal module `crates/skardi/src/sources/providers/documents/blob.rs`.
`object_store` is already available to the `skardi` crate transitively through
DataFusion; it will be made an explicit dependency for clarity.

```rust
/// A parsed source/target location.
enum Loc {
    Local(PathBuf),
    S3 { bucket: String, key: String },   // key has no leading '/'
}

/// All local-vs-S3 I/O for the documents connector lives here.
enum BlobStore {
    Local,
    Remote(Arc<dyn object_store::ObjectStore>),
}

impl BlobStore {
    /// List every object/file under `prefix` (honoring `recursive`), returning
    /// (loc, rel_key) pairs where rel_key is relative to the prefix.
    async fn list(&self, prefix: &Loc, recursive: bool) -> Result<Vec<(Loc, String)>>;

    /// Fetch the full bytes of one object/file.
    async fn get(&self, loc: &Loc) -> Result<Vec<u8>>;

    /// Write bytes to one object/file (used for image crops / page renders).
    async fn put(&self, loc: &Loc, bytes: &[u8]) -> Result<()>;
}

/// Parse a URI and pick a backend: bare/`file:` path -> Local; `s3://` ->
/// Remote, looking the bucket's store up from `runtime_env`.
fn resolve(uri: &str, rt: &RuntimeEnv) -> Result<(BlobStore, Loc)>;
```

### Changed call sites (all inside the documents module + its dispatch)

- `parse.rs`
  - `collect_files()` → `list_docs(store, prefix, opts)`: returns `(Loc, rel_path)`
    for entries whose **basename** matches `include_globs`. For `Local` this is the
    current `read_dir` walk; for `Remote` it is `ObjectStore::list(Some(prefix))`
    (recursive) or a delimiter-scoped list (non-recursive).
  - `parse_file()` signature changes from `(abs_path, rel_path, opts)` to
    `(bytes, rel_path, file_type, opts, write_store, image_loc_base)`. It calls
    `parser.parse_input(PdfInput::Bytes(bytes))` and, for page renders,
    `parser.screenshot_input(PdfInput::Bytes(bytes), …)`.
  - `write_image_crop()` is replaced by `write_store.put(loc, bytes)`. The
    `s3://` no-op branch is **deleted**; both local and S3 now perform a real
    write. `file_type` is derived from the `rel_path` extension (unchanged
    `file_type_for`) rather than a filesystem `Path`.
- `table.rs`
  - `DocumentsScanExec::execute()` obtains `context.runtime_env()`, calls
    `resolve()` for the source `path` (read store) and for `options.image_store`
    (write store, if any), and threads both into the parse loop.
- `config.rs`
  - The `Documents` arm performs the S3 validation + per-bucket registration
    described in §4 when `path`/`image_store` are `s3://`.

### Sync ↔ async bridge (unchanged strategy)

`parse_source` already runs its blocking loop on a **dedicated OS thread that
owns a current-thread tokio runtime** (because DataFusion calls the scan from a
tokio worker and liteparse is async). The same runtime `block_on`s the
`BlobStore` async calls (`list` / `get` / `put`). The `Arc<dyn ObjectStore>`
handles are `Send + Sync`, so they are moved into that thread. `parse_source`
gains the resolved `BlobStore`s as parameters; it stays a synchronous call from
DataFusion's perspective.

## 6. Data flow

### Read (source `path`)

1. `execute()` resolves the source `path` to `(read_store, prefix_loc)`.
2. `list_docs` enumerates matching entries → `Vec<(Loc, rel_path)>`, sorted for
   deterministic output.
3. For each entry: `read_store.get(loc)` → bytes → `parse_file(bytes, rel_path,
   …)` → `PdfInput::Bytes` → liteparse → `Vec<ParsedPage>`.
4. `doc_id = blake3(rel_path)` and `path = rel_path` — identical semantics to the
   local walk (rel path uses `/` separators; for S3 the key already does).

### Write (`image_store`, only when `image_mode=embedded` or `render_page_images`)

1. `execute()` resolves `options.image_store` to `(write_store, base_loc)`.
2. `image_ref_uri` / `page_image_uri` build the per-image URI exactly as today
   (`{image_store}/{relpath_underscored}_{id}.png`). This URI is recorded in the
   `image_refs` / `page_image_ref` output columns.
3. `write_store.put(loc, bytes)` writes the crop. For `Local` this is the current
   `create_dir_all` + `std::fs::write`; for S3 it is `ObjectStore::put`.
4. On a per-image write failure the ref is dropped and a warning logged — the
   existing local behavior, now applied uniformly to S3.

## 7. Reliability & error handling

- **Prefix-aware connectivity check.** The existing `test_connectivity` does
  `head(object_path)`, which is correct for a single object but returns
  `NotFound` for a **prefix** (`s3://bucket/corpus/`). The documents registration
  uses a `list(prefix)` with a small bound (e.g. take the first item) to verify
  credentials + bucket reachability without requiring the prefix itself to be a
  key. An empty-but-reachable prefix is **not** a startup error (it is a valid,
  currently-empty corpus); auth/network/permission failures **are**.
- **Missing/unreadable source.** Local semantics are unchanged (a missing root
  fails the scan loudly rather than looking like an empty corpus). For S3, a
  `list` that fails on auth/permission/network errors fails the scan with a clear
  message; a successful list returning zero objects yields zero rows.
- **Per-file fault isolation (unchanged).** A `get` or parse failure for one
  object is logged (`tracing::warn`) and skipped; remaining objects still produce
  rows. This already exists for local parse errors and now also covers per-object
  fetch failures.
- **Pagination.** `ObjectStore::list` returns a paginated stream; the backend
  drains it fully, so large prefixes are enumerated completely.
- **Memory.** v1 fetches each object fully into memory (`get` → `Vec<u8>`), then
  hands bytes to liteparse. This mirrors the current eager, whole-batch model and
  is bounded by the largest single file, not the whole corpus (files are parsed
  one at a time). Streaming/`spawn_blocking` remains the `TODO(scale)` tracked by
  issue question 2. This tradeoff is called out explicitly, not silent.
- **Multiple buckets / cross-bucket writes.** Distinct buckets across `path` and
  `image_store` each get their own registered store. Writing to an `image_store`
  bucket additionally requires `s3:PutObject`.

## 8. Security

- Credentials never live in ctx.yaml — enforced by reusing
  `S3Storage::validate_configuration` (rejects `aws_*` keys) for `documents`.
- The source `path` and `image_store` are operator-authored config, not
  data-derived values, so this is not the SSRF surface that data-derived image
  refs are in `llm_extract`; no per-request URL policy is needed here. (If a
  future feature lets agents author documents sources, revisit with the
  default-deny pattern from `llm_extract::fetch_image_with_policy`.)
- Required IAM: `s3:ListBucket` + `s3:GetObject` on the source bucket/prefix; add
  `s3:PutObject` on the `image_store` bucket/prefix when writing crops.

## 9. Testing

- **Unit — `Loc`/`resolve`:** scheme detection (`s3://` vs bare vs `file:`),
  bucket/key parsing, `image_ref_uri` for both local and `s3://` bases (extend
  the existing `image_ref_uri_and_page_image_uri_without_store` test).
- **Unit — `list_docs` glob filtering:** matches on basename for both backends
  (the S3 case uses a mocked/in-memory `ObjectStore`).
- **Integration — S3 read (mocked store):** register an
  `object_store::memory::InMemory` (or the S3-compatible in-memory) store under a
  bucket URL, seed a small PDF fixture, and assert `SELECT path, page FROM
  documents` yields the expected rows — proving the `list`→`get`→`parse_input`
  path without a live AWS dependency.
- **Integration — S3 write:** with `image_mode=embedded` over an in-memory store,
  assert crops are `put` under the derived keys and the refs are recorded.
- **Regression — local unchanged:** all existing `parse.rs` / `table.rs` tests
  must pass unmodified (the `Local` backend preserves current behavior byte for
  byte).
- **Connectivity check:** unit-test that the prefix-aware check treats an empty
  prefix as OK and an auth/network error as a hard failure.
- **Credential rejection:** assert an `aws_secret_access_key` under a documents
  source's `options` is rejected at registration.

## 10. Rollout / follow-ups

- No schema change; the output columns are identical. Existing local sources are
  unaffected (they resolve to the `Local` backend).
- Follow-up (issue question 2): prefix/predicate + `limit` pushdown and per-file
  streaming, which also reduces the whole-prefix memory/latency cost.
- Follow-up (optional): generalize the backend to `gs://` / `az://` via
  `object_store`'s other builders — the `resolve()` seam is the only place that
  would change.
