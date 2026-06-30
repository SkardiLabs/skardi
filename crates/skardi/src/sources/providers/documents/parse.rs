//! liteparse wrapper — internal to the `documents` connector.
//!
//! Walks a source root, runs every matching file through `liteparse`, and
//! produces one [`ParsedPage`] per `(file, page)`. This module is consumed
//! only by the `DocumentsTable` provider; `llm_extract` (a separate primitive)
//! consumes the source's output through SQL, not this module.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use liteparse::config::ImageMode as LpImageMode;
use liteparse::{LiteParse, LiteParseConfig, OutputFormat};

/// How raster images on a page are surfaced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageMode {
    /// Extract image bytes and write crops to `image_store`.
    Embedded,
    /// Emit references in the markdown but do not extract pixels.
    Placeholder,
    /// Strip image references entirely.
    Off,
}

/// OCR strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OcrMode {
    /// Use liteparse's per-page complexity check; OCR only pages that need it.
    Auto,
    /// Always OCR.
    On,
    /// Never OCR.
    Off,
}

/// One parsed `(file, page)` row.
#[derive(Debug, Clone)]
pub struct ParsedPage {
    /// Stable id for the file (hash of the relative path).
    pub doc_id: String,
    /// Path relative to the source root (e.g. `batch-a/catalog.pdf`).
    pub path: String,
    /// 1-based page index within the file.
    pub page: i32,
    /// liteparse reconstructed markdown for the page.
    pub markdown: String,
    /// JSON array of reconstructed tables (may be `"[]"`).
    pub tables_json: String,
    /// URI of the rendered full-page image, when produced.
    pub page_image_ref: Option<String>,
    /// JSON array of URIs of cropped images on the page (`"[]"` ok).
    pub image_refs: Vec<String>,
    /// `pdf` / `docx` / `xlsx` / `image` / …
    pub file_type: String,
}

/// Parse-time options, derived from the context-YAML `options` map.
#[derive(Debug, Clone)]
pub struct ParseOptions {
    pub recursive: bool,
    pub include_globs: Vec<String>,
    pub image_mode: ImageMode,
    pub image_store: Option<String>,
    pub ocr: OcrMode,
    pub render_page_images: bool,
    /// Optional HTTP OCR server URL. liteparse is built here without the
    /// bundled Tesseract engine (see Cargo.toml), so OCR is only actually
    /// performed when this is set; otherwise `on`/`auto` degrade to no-OCR.
    pub ocr_server_url: Option<String>,
}

impl Default for ParseOptions {
    fn default() -> Self {
        // Defaults per spec §3: recurse, all supported types, no image
        // extraction, OCR auto, no full-page renders.
        Self {
            recursive: true,
            include_globs: default_globs(),
            image_mode: ImageMode::Off,
            image_store: None,
            ocr: OcrMode::Auto,
            render_page_images: false,
            ocr_server_url: None,
        }
    }
}

/// The default set of supported file globs.
fn default_globs() -> Vec<String> {
    [
        "*.pdf", "*.docx", "*.xlsx", "*.pptx", "*.doc", "*.xls", "*.ppt", "*.odt", "*.ods",
        "*.odp", "*.png", "*.jpg", "*.jpeg", "*.tif", "*.tiff", "*.bmp", "*.gif",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

impl ParseOptions {
    /// Build options from the context-YAML `options` map, applying spec defaults
    /// for any key that is absent.
    pub fn from_map(o: Option<&HashMap<String, String>>) -> Self {
        let mut opts = ParseOptions::default();
        let Some(map) = o else {
            return opts;
        };

        if let Some(v) = map.get("recursive") {
            opts.recursive = parse_bool(v, opts.recursive);
        }
        if let Some(v) = map.get("include_globs") {
            let globs: Vec<String> = v
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if !globs.is_empty() {
                opts.include_globs = globs;
            }
        }
        if let Some(v) = map.get("image_mode") {
            opts.image_mode = match v.trim().to_ascii_lowercase().as_str() {
                "embedded" | "embed" => ImageMode::Embedded,
                "off" | "none" => ImageMode::Off,
                _ => ImageMode::Placeholder,
            };
        }
        if let Some(v) = map.get("image_store") {
            let v = v.trim();
            if !v.is_empty() {
                opts.image_store = Some(v.to_string());
            }
        }
        if let Some(v) = map.get("ocr") {
            opts.ocr = match v.trim().to_ascii_lowercase().as_str() {
                "on" | "true" => OcrMode::On,
                "off" | "false" => OcrMode::Off,
                _ => OcrMode::Auto,
            };
        }
        if let Some(v) = map.get("render_page_images") {
            opts.render_page_images = parse_bool(v, opts.render_page_images);
        }
        if let Some(v) = map.get("ocr_server_url") {
            let v = v.trim();
            if !v.is_empty() {
                opts.ocr_server_url = Some(v.to_string());
            }
        }
        opts
    }
}

/// Whether OCR can actually run in this build for the given options. liteparse
/// is compiled without the bundled Tesseract engine, so OCR requires an HTTP
/// OCR server URL. Used to keep `on`/`auto` from hard-failing when no engine is
/// reachable (liteparse errors if `ocr_enabled` is set with no engine).
fn ocr_engine_available(opts: &ParseOptions) -> bool {
    opts.ocr_server_url.is_some()
}

fn parse_bool(v: &str, default: bool) -> bool {
    match v.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => true,
        "false" | "0" | "no" | "off" => false,
        _ => default,
    }
}

/// Map our public `ImageMode` onto liteparse's config enum.
fn lp_image_mode(mode: ImageMode) -> LpImageMode {
    match mode {
        ImageMode::Embedded => LpImageMode::Embed,
        ImageMode::Placeholder => LpImageMode::Placeholder,
        ImageMode::Off => LpImageMode::Off,
    }
}

/// Stable id for a file derived from its relative path.
fn doc_id_for(rel_path: &str) -> String {
    blake3::hash(rel_path.as_bytes()).to_hex().to_string()
}

/// File extension (lowercased) → coarse `file_type` label.
fn file_type_for(path: &Path) -> String {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match ext.as_str() {
        "png" | "jpg" | "jpeg" | "tif" | "tiff" | "bmp" | "gif" => "image".to_string(),
        other => other.to_string(),
    }
}

/// Does `file_name` match any of the simple `*.ext` / `*substr*` globs?
fn matches_globs(file_name: &str, globs: &[String]) -> bool {
    if globs.is_empty() {
        return true;
    }
    globs.iter().any(|g| glob_match(g, file_name))
}

/// Minimal case-insensitive glob: supports `*` wildcards (the only metachar the
/// spec's globs use, e.g. `*.pdf`). Falls back to equality when no `*`.
fn glob_match(pattern: &str, name: &str) -> bool {
    let pattern = pattern.to_ascii_lowercase();
    let name = name.to_ascii_lowercase();
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == name;
    }
    let mut pos = 0usize;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if i == 0 {
            // Must match at the start.
            if !name[pos..].starts_with(part) {
                return false;
            }
            pos += part.len();
        } else if i == parts.len() - 1 {
            // Last part must match at the end.
            if !name[pos..].ends_with(part) {
                return false;
            }
        } else if let Some(idx) = name[pos..].find(part) {
            pos += idx + part.len();
        } else {
            return false;
        }
    }
    true
}

/// Collect candidate files under `root`, honoring `recursive` + `include_globs`.
fn collect_files(root: &Path, opts: &ParseOptions) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("documents: cannot read dir {}: {}", dir.display(), e);
                continue;
            }
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if opts.recursive {
                    stack.push(path);
                }
                continue;
            }
            let file_name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n,
                None => continue,
            };
            if matches_globs(file_name, &opts.include_globs) {
                out.push(path);
            }
        }
    }
    out.sort();
    Ok(out)
}

/// Build the liteparse config for a parse run.
fn build_config(opts: &ParseOptions) -> LiteParseConfig {
    let mut cfg = LiteParseConfig {
        output_format: OutputFormat::Markdown,
        image_mode: lp_image_mode(opts.image_mode),
        quiet: true,
        ..Default::default()
    };
    // liteparse `LiteParseConfig::default()` turns OCR on only when the bundled
    // tesseract feature is compiled in (it is not, here). Force it off as the
    // baseline; we re-enable below only when an engine is actually reachable.
    cfg.ocr_enabled = false;
    cfg.ocr_server_url = opts.ocr_server_url.clone();
    // `On` enables OCR up front, but only if an engine is available — otherwise
    // liteparse would hard-error ("OCR enabled but no engine"). With no engine,
    // degrade to no-OCR rather than failing the whole scan.
    if matches!(opts.ocr, OcrMode::On) && ocr_engine_available(opts) {
        cfg.ocr_enabled = true;
    }
    cfg
}

/// Parse a single file into its pages. Errors propagate to the caller, which
/// isolates them per-file.
async fn parse_file(
    abs_path: &Path,
    rel_path: &str,
    opts: &ParseOptions,
) -> Result<Vec<ParsedPage>> {
    let path_str = abs_path
        .to_str()
        .with_context(|| format!("non-UTF-8 path: {}", abs_path.display()))?;

    let mut cfg = build_config(opts);

    // OCR Auto: ask liteparse which pages need OCR and only enable it when at
    // least one does. liteparse applies OCR per-page internally on text-sparse
    // pages once enabled, so a global flag is sufficient here. Skip entirely
    // when no OCR engine is reachable — enabling OCR would hard-fail the parse.
    if matches!(opts.ocr, OcrMode::Auto) && ocr_engine_available(opts) {
        let probe = LiteParse::new(cfg.clone());
        match probe
            .is_complex(liteparse::types::PdfInput::Path(path_str.to_string()))
            .await
        {
            Ok(stats) => {
                if stats.iter().any(|s| s.needs_ocr) {
                    cfg.ocr_enabled = true;
                }
            }
            Err(e) => {
                tracing::warn!("documents: complexity probe failed for {}: {}", rel_path, e);
            }
        }
    }

    let parser = LiteParse::new(cfg.clone());
    let result = parser
        .parse(path_str)
        .await
        .with_context(|| format!("liteparse failed for {}", rel_path))?;

    let file_type = file_type_for(abs_path);
    let doc_id = doc_id_for(rel_path);

    // Optionally render full-page images for `page_image_ref` (multimodal use,
    // e.g. llm_extract's image escalation). Rendering uses pdfium directly
    // (bundled), so it needs no external OCR/Tesseract — only LibreOffice for
    // non-PDF inputs, same as parsing. Map page_num -> written/ref URI.
    let mut page_image_refs: HashMap<u32, String> = HashMap::new();
    if opts.render_page_images {
        match parser.screenshot(path_str, None).await {
            Ok(shots) => {
                for shot in shots {
                    let uri = page_image_uri(opts.image_store.as_deref(), rel_path, shot.page_num);
                    if let Some(store) = opts.image_store.as_deref() {
                        if let Err(e) = write_image_crop(store, &uri, &shot.image_bytes) {
                            tracing::warn!(
                                "documents: failed to write page image {}: {:#}",
                                uri,
                                e
                            );
                            continue;
                        }
                    }
                    page_image_refs.insert(shot.page_num, uri);
                }
            }
            Err(e) => {
                tracing::warn!(
                    "documents: page-image rendering failed for {}: {}",
                    rel_path,
                    e
                );
            }
        }
    }

    let mut pages = Vec::with_capacity(result.pages.len());
    for page in &result.pages {
        // Per-page markdown: format just this page so the row carries markdown,
        // not the cross-page concatenation in `result.text`.
        let markdown = liteparse::output::markdown::format_markdown(
            std::slice::from_ref(page),
            &result.outline,
            cfg.image_mode,
        );

        // Reconstruct tables from the page markdown (liteparse renders tables as
        // GFM pipe tables; we lift them back out into structured JSON).
        let tables_json = tables_from_markdown(&markdown);

        // Image refs for this page, if image extraction is on. When an
        // `image_store` is configured we also write the crop bytes there and
        // record only the refs for crops we could materialize.
        let image_refs: Vec<String> = if opts.image_mode == ImageMode::Embedded {
            result
                .images
                .iter()
                .filter(|img| img.page as usize == page.page_number)
                .filter_map(|img| {
                    let uri = image_ref_uri(opts.image_store.as_deref(), rel_path, &img.id);
                    if let Some(store) = opts.image_store.as_deref() {
                        if let Err(e) = write_image_crop(store, &uri, &img.bytes) {
                            tracing::warn!(
                                "documents: failed to write image crop {}: {:#}",
                                uri,
                                e
                            );
                            return None;
                        }
                    }
                    Some(uri)
                })
                .collect()
        } else {
            Vec::new()
        };

        pages.push(ParsedPage {
            doc_id: doc_id.clone(),
            path: rel_path.to_string(),
            page: page.page_number as i32,
            markdown,
            tables_json,
            page_image_ref: page_image_refs.get(&(page.page_number as u32)).cloned(),
            image_refs,
            file_type: file_type.clone(),
        });
    }

    Ok(pages)
}

/// Build a URI for an extracted image crop.
fn image_ref_uri(store: Option<&str>, rel_path: &str, image_id: &str) -> String {
    let stem = rel_path.replace('/', "_");
    match store {
        Some(s) => format!("{}/{}_{}", s.trim_end_matches('/'), stem, image_id),
        None => format!("{}_{}", stem, image_id),
    }
}

/// Build a URI for a rendered full-page image.
fn page_image_uri(store: Option<&str>, rel_path: &str, page: u32) -> String {
    let stem = rel_path.replace('/', "_");
    match store {
        Some(s) => format!("{}/{}_page_{}.png", s.trim_end_matches('/'), stem, page),
        None => format!("{}_page_{}.png", stem, page),
    }
}

/// Write an extracted image crop to a local `image_store`. Remote stores
/// (`s3://…`, etc.) are not written here in v1 — the ref is still recorded so a
/// later object-store pass can upload (spec §7 open item). Returns `Ok` for the
/// remote-store no-op so the ref is kept.
fn write_image_crop(store: &str, uri: &str, bytes: &[u8]) -> Result<()> {
    if store.contains("://") {
        // Remote object store: defer the actual upload.
        return Ok(());
    }
    let path = Path::new(uri);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating image_store dir {}", parent.display()))?;
    }
    std::fs::write(path, bytes).with_context(|| format!("writing image crop {}", uri))?;
    Ok(())
}

/// Lift GFM pipe tables out of page markdown into a JSON array. Each table is
/// `{"header": [...], "rows": [[...], ...]}`. Returns `"[]"` when none found.
///
/// liteparse renders reconstructed tables as standard GFM pipe tables (a header
/// row, a `---|---` separator, then body rows), so a structural scan recovers
/// them without a full markdown parser.
fn tables_from_markdown(markdown: &str) -> String {
    let lines: Vec<&str> = markdown.lines().collect();
    let mut tables: Vec<serde_json::Value> = Vec::new();

    let mut i = 0;
    while i + 1 < lines.len() {
        let header = lines[i].trim();
        let sep = lines[i + 1].trim();
        if is_table_row(header) && is_separator_row(sep) {
            let headers = split_table_row(header);
            let mut rows: Vec<Vec<String>> = Vec::new();
            let mut j = i + 2;
            while j < lines.len() && is_table_row(lines[j].trim()) {
                rows.push(split_table_row(lines[j].trim()));
                j += 1;
            }
            tables.push(serde_json::json!({ "header": headers, "rows": rows }));
            i = j;
        } else {
            i += 1;
        }
    }

    serde_json::to_string(&tables).unwrap_or_else(|_| "[]".to_string())
}

/// A markdown line that looks like a pipe-table row (contains a `|`).
fn is_table_row(line: &str) -> bool {
    line.contains('|') && !line.is_empty()
}

/// The `|---|:--:|` separator row beneath a table header.
fn is_separator_row(line: &str) -> bool {
    if !line.contains('|') || !line.contains('-') {
        return false;
    }
    split_table_row(line)
        .iter()
        .all(|cell| !cell.is_empty() && cell.chars().all(|c| matches!(c, '-' | ':' | ' ')))
}

/// Split a pipe-table row into trimmed cell strings, dropping the empty
/// leading/trailing cells produced by border pipes.
fn split_table_row(line: &str) -> Vec<String> {
    let trimmed = line.trim().trim_matches('|');
    trimmed.split('|').map(|c| c.trim().to_string()).collect()
}

/// Parse every matching file under `root` into `(file, page)` rows.
///
/// Per-file errors are logged (`tracing::warn`) and skipped; remaining files
/// still produce rows. An empty / unmatched source returns zero rows.
pub fn parse_source(root: &str, opts: &ParseOptions) -> Result<Vec<ParsedPage>> {
    let root = root.to_string();
    let opts = opts.clone();

    // liteparse's parse APIs are async (PDFium FFI + optional OCR). The
    // DataFusion scan path calls this from inside a tokio worker thread, so we
    // can't build/`block_on` a runtime here directly (nested-runtime panic).
    // Run the whole parse on a dedicated OS thread that owns its own
    // current-thread runtime, keeping `parse_source` a plain synchronous call.
    std::thread::scope(|scope| {
        scope
            .spawn(move || parse_source_blocking(&root, &opts))
            .join()
            .map_err(|_| anyhow::anyhow!("documents parse thread panicked"))?
    })
}

/// The actual blocking parse loop, run on a thread that owns a tokio runtime.
fn parse_source_blocking(root: &str, opts: &ParseOptions) -> Result<Vec<ParsedPage>> {
    let root_path = Path::new(root);
    let files = collect_files(root_path, opts)?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime for documents parse")?;

    let mut rows = Vec::new();
    for file in files {
        let rel_path = file
            .strip_prefix(root_path)
            .unwrap_or(&file)
            .to_string_lossy()
            .replace('\\', "/");

        match runtime.block_on(parse_file(&file, &rel_path, opts)) {
            Ok(mut page_rows) => rows.append(&mut page_rows),
            Err(e) => {
                tracing::warn!("documents: skipping {}: {:#}", rel_path, e);
            }
        }
    }

    Ok(rows)
}

/// Validate the configured modes are satisfiable, surfacing problems at
/// registration rather than mid-scan.
///
/// OCR semantics for this build: liteparse is linked with
/// `default-features = false`, so its bundled Tesseract engine is **compiled
/// out** — OCR is only ever performed through liteparse's HTTP OCR engine
/// (`ocr_server_url`). There is therefore no `tesseract` binary involved, and
/// we do not look for one.
///
/// * `ocr = on`  — OCR is mandatory, so an `ocr_server_url` MUST be configured.
///   Missing it is a hard error here (rather than silently producing empty
///   pages mid-scan).
/// * `ocr = auto` — OCR is best-effort. With no `ocr_server_url`, no engine is
///   available, so parsing proceeds without OCR (logged); complex/scanned pages
///   simply yield whatever native text exists.
/// * `ocr = off` — never OCR.
///
/// Non-PDF inputs are converted to PDF by liteparse via LibreOffice/ImageMagick
/// at parse time; a missing converter is warned about (not fatal) so PDF-only
/// corpora still work.
pub fn preflight(opts: &ParseOptions) -> Result<()> {
    // OCR engine contract.
    match opts.ocr {
        OcrMode::On if !ocr_engine_available(opts) => {
            anyhow::bail!(
                "documents: ocr=on requires an `ocr_server_url` option (this build links \
                 liteparse without the bundled Tesseract engine, so OCR is only available via \
                 an HTTP OCR server). Set `ocr_server_url`, or use ocr=auto/off."
            );
        }
        OcrMode::Auto if !ocr_engine_available(opts) => {
            // Best-effort: no engine configured, so OCR is unavailable. Parsing
            // will proceed on native text only.
            tracing::info!(
                "documents: ocr=auto but no `ocr_server_url` configured; \
                 proceeding without OCR (native text only)"
            );
        }
        _ => {}
    }

    // Office/ODF/image conversion needs LibreOffice (and ImageMagick for some
    // image inputs). Only relevant when the globs admit non-PDF inputs. Not
    // fatal: a PDF-only directory still works when LibreOffice is absent.
    let needs_conversion = opts.include_globs.iter().any(|g| {
        let g = g.to_ascii_lowercase();
        !(g.ends_with(".pdf") || g == "*" || g == "*.*")
    });
    if needs_conversion && !tool_available("soffice") && !tool_available("libreoffice") {
        tracing::warn!(
            "documents: LibreOffice (soffice) not found; non-PDF inputs cannot be converted"
        );
    }

    Ok(())
}

/// Is `name` an executable on PATH? Used to decide whether optional converters
/// (LibreOffice/ImageMagick) are present so tests/preflight can degrade or skip.
fn tool_available(name: &str) -> bool {
    let path = match std::env::var_os("PATH") {
        Some(p) => p,
        None => return false,
    };
    std::env::split_paths(&path).any(|dir| {
        let candidate = dir.join(name);
        candidate.is_file()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_two_page_pdf_into_rows() {
        let opts = ParseOptions {
            recursive: true,
            include_globs: vec!["*.pdf".into()],
            image_mode: ImageMode::Off,
            image_store: None,
            ocr: OcrMode::Off,
            render_page_images: false,
            ocr_server_url: None,
        };
        let rows = parse_source("tests/fixtures/documents", &opts).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.file_type == "pdf"));
        assert!(rows.iter().all(|r| !r.markdown.is_empty()));
        assert_eq!(rows[0].page, 1);
    }

    #[test]
    fn glob_match_basics() {
        assert!(glob_match("*.pdf", "a.pdf"));
        assert!(glob_match("*.pdf", "DIR.PDF"));
        assert!(!glob_match("*.pdf", "a.docx"));
        assert!(glob_match("*", "anything.xyz"));
        assert!(glob_match("foo*bar", "fooXYZbar"));
        assert!(!glob_match("foo*bar", "fooXYZ"));
    }

    #[test]
    fn from_map_defaults_and_overrides() {
        let d = ParseOptions::from_map(None);
        assert!(d.recursive);
        assert_eq!(d.ocr, OcrMode::Auto);

        let mut m = HashMap::new();
        m.insert("recursive".to_string(), "false".to_string());
        m.insert("include_globs".to_string(), "*.pdf, *.png".to_string());
        m.insert("image_mode".to_string(), "embedded".to_string());
        m.insert("ocr".to_string(), "off".to_string());
        let o = ParseOptions::from_map(Some(&m));
        assert!(!o.recursive);
        assert_eq!(
            o.include_globs,
            vec!["*.pdf".to_string(), "*.png".to_string()]
        );
        assert_eq!(o.image_mode, ImageMode::Embedded);
        assert_eq!(o.ocr, OcrMode::Off);
    }

    #[test]
    fn doc_id_is_stable_and_path_relative() {
        let a = doc_id_for("batch-a/catalog.pdf");
        let b = doc_id_for("batch-a/catalog.pdf");
        let c = doc_id_for("batch-b/catalog.pdf");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn tables_from_markdown_extracts_gfm_tables() {
        let md = "Intro text.\n\n\
                  | Name | Qty |\n\
                  | --- | --- |\n\
                  | Widget | 10 |\n\
                  | Gadget | 20 |\n\n\
                  Trailing prose.";
        let json = tables_from_markdown(md);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v.as_array().unwrap().len(), 1);
        assert_eq!(v[0]["header"], serde_json::json!(["Name", "Qty"]));
        assert_eq!(v[0]["rows"][0], serde_json::json!(["Widget", "10"]));
        assert_eq!(v[0]["rows"][1], serde_json::json!(["Gadget", "20"]));
    }

    #[test]
    fn tables_from_markdown_empty_when_none() {
        assert_eq!(
            tables_from_markdown("just some prose\nno tables here"),
            "[]"
        );
    }

    #[test]
    fn write_image_crop_writes_local_file() {
        let dir = tempfile::tempdir().unwrap();
        let uri = format!("{}/sub/crop_p1_0.png", dir.path().display());
        write_image_crop(dir.path().to_str().unwrap(), &uri, b"\x89PNGfake").unwrap();
        let written = std::fs::read(&uri).unwrap();
        assert_eq!(written, b"\x89PNGfake");
    }

    #[test]
    fn write_image_crop_skips_remote_store() {
        // Remote stores are a no-op (deferred upload) but must not error.
        write_image_crop("s3://bucket/prefix", "s3://bucket/prefix/x.png", b"data").unwrap();
    }

    #[test]
    fn preflight_errors_when_ocr_on_without_engine() {
        // ocr=on but no ocr_server_url: this build has no bundled Tesseract, so
        // OCR is impossible. preflight must hard-fail with a clear message
        // rather than letting the scan silently produce empty pages.
        let opts = ParseOptions {
            ocr: OcrMode::On,
            ocr_server_url: None,
            ..ParseOptions::default()
        };
        let err = preflight(&opts).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("ocr=on"), "unexpected error: {msg}");
        assert!(msg.contains("ocr_server_url"), "unexpected error: {msg}");
    }

    #[test]
    fn preflight_ok_when_ocr_on_with_engine() {
        let opts = ParseOptions {
            ocr: OcrMode::On,
            ocr_server_url: Some("http://ocr.internal:8080/ocr".into()),
            include_globs: vec!["*.pdf".into()],
            ..ParseOptions::default()
        };
        assert!(preflight(&opts).is_ok());
    }

    #[test]
    fn preflight_ok_when_ocr_auto_without_engine() {
        // auto degrades to no-OCR (best-effort), not an error.
        let opts = ParseOptions {
            ocr: OcrMode::Auto,
            ocr_server_url: None,
            include_globs: vec!["*.pdf".into()],
            ..ParseOptions::default()
        };
        assert!(preflight(&opts).is_ok());
    }

    #[test]
    fn preflight_ok_when_ocr_off() {
        let opts = ParseOptions {
            ocr: OcrMode::Off,
            include_globs: vec!["*.pdf".into()],
            ..ParseOptions::default()
        };
        assert!(preflight(&opts).is_ok());
    }

    #[test]
    fn render_page_images_sets_page_image_ref_and_writes_files() {
        // PDF page rendering uses pdfium directly (bundled with the documents
        // build), so this needs no external tools.
        let store = tempfile::tempdir().unwrap();
        let opts = ParseOptions {
            include_globs: vec!["*.pdf".into()],
            ocr: OcrMode::Off,
            render_page_images: true,
            image_store: Some(store.path().to_str().unwrap().to_string()),
            ..ParseOptions::default()
        };
        let rows = parse_source("tests/fixtures/documents", &opts).unwrap();
        assert_eq!(rows.len(), 2);
        for r in &rows {
            let uri = r
                .page_image_ref
                .as_ref()
                .expect("page_image_ref should be set when render_page_images=true");
            assert!(
                std::path::Path::new(uri).exists(),
                "rendered page image not written: {uri}"
            );
            // Written file should be a non-empty PNG.
            let bytes = std::fs::read(uri).unwrap();
            assert!(bytes.starts_with(b"\x89PNG"), "not a PNG: {uri}");
        }
    }

    #[test]
    fn render_page_images_without_store_still_sets_ref() {
        // No image_store: ref is still populated (a relative URI) but nothing is
        // written to disk.
        let opts = ParseOptions {
            include_globs: vec!["*.pdf".into()],
            ocr: OcrMode::Off,
            render_page_images: true,
            image_store: None,
            ..ParseOptions::default()
        };
        let rows = parse_source("tests/fixtures/documents", &opts).unwrap();
        assert!(rows.iter().all(|r| r.page_image_ref.is_some()));
    }

    /// `image` file_type label and that an image source is parseable when OCR is
    /// off (liteparse converts the image to a 1-page PDF via ImageMagick). Skips
    /// when the conversion tool is absent — common in minimal CI/dev envs.
    #[test]
    fn scanned_png_parses_or_skips() {
        if !tool_available("convert") && !tool_available("magick") {
            eprintln!("skipping scanned_png_parses_or_skips: ImageMagick not found");
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        std::fs::copy(
            "tests/fixtures/documents/scanned.png",
            dir.path().join("scanned.png"),
        )
        .unwrap();
        let opts = ParseOptions {
            include_globs: vec!["*.png".into()],
            ocr: OcrMode::Off,
            ..ParseOptions::default()
        };
        let rows = parse_source(dir.path().to_str().unwrap(), &opts).unwrap();
        assert!(rows.iter().all(|r| r.file_type == "image"));
    }
}
