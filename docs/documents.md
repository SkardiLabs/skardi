# Documents Source (`type: documents`)

> **Build flag:**
> ```bash
> cargo build --release -p skardi-server --features documents
> ```
>
> Everything in this connector is gated behind the `documents` Cargo feature.
> Without it, a context declaring a `documents` source fails registration with a
> clear "feature not enabled" error.
>
> **⚠️ Build-time native download (no checksum):** the `documents` feature pulls
> in liteparse's `pdfium-sys`, whose `build.rs` **downloads a prebuilt PDFium
> native library at build time** from `github.com/run-llama/pdfium-binaries`
> (release tag `chromium/7897`) and caches it under `$XDG_CACHE_HOME`/`$HOME`.
> The download is **not checksum-verified**, and the build needs network access.
> For hermetic / offline / air-gapped builds, pre-provision PDFium and point the
> build at it:
> ```bash
> export PDFIUM_LIB_PATH=/opt/pdfium/lib      # dir containing libpdfium.{dylib,so,dll}
> export PDFIUM_INCLUDE_PATH=/opt/pdfium/include
> cargo build --release -p skardi-server --features documents
> ```
> When both are set, `pdfium-sys` links the provided library instead of
> downloading one.

`documents` is a read-only skardi data source that turns a **local directory
or `s3://` prefix** of files — PDF, Office (`.docx/.xlsx/.pptx`), ODF, and
images — into queryable rows. Each row is one parsed **(file, page)** carrying
reconstructed markdown, tables, and references to extracted images. It is
backed by the pure-Rust [`liteparse`](https://github.com/run-llama/liteparse)
crate.

> **S3 support.** `path` and `image_store` may **each independently** be a
> local directory or an `s3://bucket/prefix` URI; see [S3 / object
> store](#s3--object-store) for the credential contract and constraints.
> Other object-store schemes (`gs://`, `az://`) are not wired yet.

Once registered, `SELECT * FROM documents` behaves like any other table: it is
joinable in SQL and can be fed to the `llm_extract` UDF over its `markdown`
column (via `UNNEST(llm_extract(markdown, page_image_ref, …))`) — there is no
shared Rust between the two, only SQL.

## Configuration (context YAML)

```yaml
- name: documents
  type: documents
  path: /data/pp/inbound         # local directory or s3://bucket/prefix (the root); recurses by default
  access_mode: read_only
  description: "Supplier source documents"
  options:
    recursive: "true"            # descend subdirectories (default: true)
    include_globs: "*.pdf,*.docx,*.xlsx,*.png,*.jpg"  # default: all supported types
    image_mode: "embedded"       # embedded | placeholder | off (default: off)
    image_store: "/data/pp/extracted"  # where cropped images are written; local path or s3://bucket/prefix
    ocr: "auto"                  # auto | on | off (default: auto)
    render_page_images: "true"   # render full-page images for page_image_ref
    ocr_server_url: "http://ocr:8080/ocr"  # HTTP OCR engine (see OCR below)
```

All keys are optional except `path`.

| Option | Default | Meaning |
|--------|---------|---------|
| `recursive` | `true` | Descend into subdirectories. |
| `include_globs` | all supported | Comma-separated `*.ext` globs; only matching files are parsed. |
| `image_mode` | `off` | `embedded` extracts image bytes; `placeholder` keeps refs only; `off` strips images. |
| `image_store` | — | Destination for extracted image crops — a local path or an `s3://bucket/prefix`. Both backends perform a real write (S3 objects get a `Content-Type` inferred from the extension). Local refs are usable as `llm_extract`'s `image_ref` as-is; `s3://` refs are readable too but **only with `LLM_EXTRACT_IMAGE_FETCH=1`** — see [Image refs and `llm_extract`](#image-refs-and-llm_extract). When both `path` and `image_store` are `s3://`, they must be in the **same bucket** (see [S3 / object store](#s3--object-store)). |
| `ocr` | `auto` | `auto` OCRs only complex pages (needs `ocr_server_url`); `on` always (requires `ocr_server_url`, else hard error); `off` never. See OCR. |
| `render_page_images` | `false` | Render each page to a PNG into `image_store` and set `page_image_ref` (needed for multimodal `llm_extract`). |
| `ocr_server_url` | — | HTTP OCR engine URL. The only way to do OCR in this build (no bundled Tesseract). Mandatory for `ocr: on`. |

Filtering by "batch" is just a path predicate — `WHERE path LIKE 'batch-a/%'`.
There is no batch concept baked into the source, keeping it generic.

## Row schema

One row per **(file, page)**. The schema is fixed by design (it is what
liteparse uniformly produces for any file):

| Column | Type | Notes |
|--------|------|-------|
| `doc_id` | `Utf8` | Stable id for the file (BLAKE3 hash of the relative path). |
| `path` | `Utf8` | Path relative to the source root (e.g. `batch-a/catalog.pdf`). |
| `page` | `Int32` | 1-based page index within the file. |
| `markdown` | `Utf8` | liteparse reconstructed markdown for the page. |
| `tables_json` | `Utf8` | JSON array of reconstructed tables: `[{"header":[…],"rows":[[…]]}]` (may be `[]`). |
| `page_image_ref` | `Utf8` (nullable) | URI of the rendered full-page image, when produced. |
| `image_refs` | `Utf8` | JSON array of URIs of cropped images on the page (`[]` ok). |
| `file_type` | `Utf8` | `pdf` / `docx` / `xlsx` / `image` / … |

Custom/structured columns are not configured here — they come from the
`llm_extract` UDF, whose output schema is caller-defined. Path-derived columns
(a `batch_id`, factory tag, etc.) are expressed in SQL:
`split_part(path, '/', 1) AS batch_id`.

## Example query

```sql
-- One row per page; derive a batch tag from the path and keep table-bearing pages.
SELECT
  split_part(path, '/', 1) AS batch_id,
  path,
  page,
  markdown
FROM documents
WHERE path LIKE 'batch-a/%'
  AND tables_json <> '[]'
ORDER BY path, page;
```

## OCR and external tools

liteparse converts non-PDF inputs (Office/ODF/images) to PDF using
**LibreOffice** (and **ImageMagick** for some image formats), and can OCR
text-sparse / scanned pages.

This build links liteparse **without its bundled Tesseract engine** (the
`tesseract-rs → zip → xz2 → lzma-sys` chain collides with DataFusion's
`liblzma-sys` — two crates cannot both link the native `lzma` library). As a
result OCR is performed via an **HTTP OCR engine**: set `ocr_server_url`.

- `ocr: off` — never OCR (native text extraction only). Works with no extra tools.
- `ocr: on` — OCR is mandatory and **requires `ocr_server_url`**. Because this
  build has no bundled Tesseract, `ocr: on` without `ocr_server_url` is a hard
  error at registration (preflight) — it does not silently produce empty pages.
- `ocr: auto` — best-effort: OCR only the pages liteparse flags as complex, and
  only when `ocr_server_url` is configured. With no engine, parsing proceeds on
  native text (logged), no error.

**Preflight at registration:** problems surface when the source is registered,
not mid-scan. `ocr: on` with no `ocr_server_url` fails with a clear, actionable
error. Non-PDF globs without LibreOffice produce a warning (PDF-only corpora
still work). There is **no `tesseract` binary check** — this build never uses
the local Tesseract binary.

## Full-page images and multimodal (`render_page_images`)

With `render_page_images: "true"`, every page is rendered to a PNG (via PDFium,
no external OCR needed), written to `image_store` — local directory or
`s3://` prefix, both are real writes readable right away — and its URI is set
on the row's `page_image_ref`. This is what the multimodal
`llm_extract` escalation consumes (`llm_extract(markdown, page_image_ref, …)`),
so **`render_page_images` must be enabled (with an `image_store`) for multimodal
extraction to have an image to escalate to** — otherwise `page_image_ref` is
`NULL` and only the text path is available.

### Image refs and `llm_extract`

`llm_extract` treats `image_ref` as **untrusted data** (it is a column value, so
a crafted row could point it anywhere). Fetching is therefore default-deny:
only inline `data:` URIs resolve unless you set `LLM_EXTRACT_IMAGE_FETCH=1`,
which enables `http(s)://`, `s3://`, and local-file refs alike.

That means a **local** `image_store` needs `LLM_EXTRACT_IMAGE_FETCH=1` for
multimodal escalation, and so does an **`s3://`** one:

```bash
export LLM_EXTRACT_IMAGE_FETCH=1
```

`s3://` refs are read through the same object-store client and env-only
credential contract as the connector itself, so the process needs
`s3:GetObject` on the `image_store` prefix — which is *not* required for
writing crops, so a least-privilege policy scoped to `PutObject` alone will
fail here. `s3://` support also requires the `documents` Cargo feature (it owns
the S3 client); an `s3://` ref in a build without it is refused with a build
hint rather than being misread as a filesystem path.

## Error handling

- A single unparseable file is logged (`tracing::warn`) and skipped; the rest of
  the directory still produces rows.
- An empty or fully-unmatched source returns zero rows, not an error.
- **Wholesale failure is a hard error:** if the listing is non-empty but *every*
  matched file fails to fetch/parse (e.g. credentials expired after listing),
  the scan errors instead of silently returning zero rows that would masquerade
  as an empty corpus.

## S3 / object store

`path` and `image_store` may each independently be local or `s3://bucket/prefix`.

**Credentials and region come from the environment, never from config.**
Putting `aws_access_key_id` / `aws_secret_access_key` / `aws_session_token` /
`aws_region` in `options` fails registration. Set `AWS_REGION` (or
`AWS_DEFAULT_REGION`) plus `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
(and `AWS_SESSION_TOKEN` for temporary credentials). Note the S3 client reads
**environment variables only** — `~/.aws/` profiles are not consulted at
request time, so if you normally use a profile, export it first:

```bash
eval "$(aws configure export-credentials --format env)"
export AWS_REGION=us-east-1
```

Constraints and behavior:

- **Same bucket when both sides are S3.** An `s3://` `path` with an `s3://`
  `image_store` in a *different* bucket fails registration — one registered
  store / region / credential set cannot serve two buckets. Cross-bucket
  support is a tracked follow-up. Local + S3 combinations are unrestricted.
- **Registration-time preflight.** Before the source registers, the server
  checks read connectivity (a prefix-scoped list of `path`; an
  empty-but-reachable prefix is fine) and, when `image_store` is `s3://`,
  write access (put + delete of a `.skardi-write-probe` object under the
  prefix). Missing permissions (`s3:ListBucket`/`s3:GetObject`/`s3:PutObject`),
  a bad bucket, or missing env config all fail startup with an actionable
  error instead of surfacing mid-scan.
- **Prefix scoping.** `s3://bucket/corpus` lists exactly the `corpus/` prefix —
  a sibling `corpus-2/` never matches.
- **Self-ingestion guard.** Anything under the `image_store` location is
  excluded from the scan, so crops written by a previous scan are never
  re-ingested even when `image_store` nests inside `path` and matches
  `include_globs`.
- `doc_id` and `path` are computed from the prefix-relative key with `/`
  separators, identical to the local backend — moving a corpus between a local
  directory and S3 keeps ids stable.
