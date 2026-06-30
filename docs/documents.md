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

`documents` is a read-only skardi data source that turns a directory (or
object-store prefix) of files — PDF, Office (`.docx/.xlsx/.pptx`), ODF, and
images — into queryable rows. Each row is one parsed **(file, page)** carrying
reconstructed markdown, tables, and references to extracted images. It is backed
by the pure-Rust [`liteparse`](https://github.com/run-llama/liteparse) crate.

Once registered, `SELECT * FROM documents` behaves like any other table: it is
joinable in SQL and can be fed to the `llm_extract` UDF over its `markdown`
column (via `UNNEST(llm_extract(markdown, page_image_ref, …))`) — there is no
shared Rust between the two, only SQL.

## Configuration (context YAML)

```yaml
- name: documents
  type: documents
  path: s3://pp/inbound          # directory or object-store prefix (the root)
  access_mode: read_only
  description: "Supplier source documents"
  options:
    recursive: "true"            # descend subdirectories (default: true)
    include_globs: "*.pdf,*.docx,*.xlsx,*.png,*.jpg"  # default: all supported types
    image_mode: "embedded"       # embedded | placeholder | off (default: off)
    image_store: "s3://pp/extracted"   # where cropped images are written
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
| `image_store` | — | Destination for extracted image crops. Local paths are written immediately; remote (`s3://…`) refs are recorded and uploaded by a later pass. |
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
no external OCR needed), written to `image_store` (local paths are written
immediately; remote `s3://` URIs are recorded for a later upload pass), and its
URI is set on the row's `page_image_ref`. This is what the multimodal
`llm_extract` escalation consumes (`llm_extract(markdown, page_image_ref, …)`),
so **`render_page_images` must be enabled (with an `image_store`) for multimodal
extraction to have an image to escalate to** — otherwise `page_image_ref` is
`NULL` and only the text path is available.

## Error handling

- A single unparseable file is logged (`tracing::warn`) and skipped; the rest of
  the directory still produces rows.
- An empty or fully-unmatched source returns zero rows, not an error.
