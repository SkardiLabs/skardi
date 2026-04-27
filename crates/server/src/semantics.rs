//! Natural-language semantics overlays for the catalog.
//!
//! Semantics files are a separate Kubernetes-style YAML kind (`kind: semantics`)
//! that attach human-readable descriptions to tables and columns already
//! registered through a `kind: context` file. They are loaded at server
//! startup alongside pipelines, jobs, and the context, and the catalog
//! endpoint (`GET /data_source`) emits the descriptions on its response so
//! agents can read them when picking a tool.
//!
//! ```yaml
//! kind: semantics
//! metadata:
//!   name: basic-semantics
//!   version: 1.0.0
//! spec:
//!   sources:
//!     - name: products              # must match data_sources[].name in the ctx
//!       description: "Product catalog with pricing/inventory"
//!       columns:
//!         - name: price_usd
//!           description: "Retail price in USD; nullable for unlisted SKUs"
//! ```
//!
//! Composition rules:
//! - Multiple files may be loaded by pointing `--semantics` at a directory.
//!   Files in that directory whose root `kind:` is not `semantics` are
//!   silently skipped, mirroring how `--jobs` tolerates plain pipelines.
//! - Two entries (across all files) for the same source — or two column
//!   entries on the same source/column — are a hard error. Auto-generated
//!   overlays must produce non-overlapping files.
//! - References to sources or columns that do not exist on the loaded ctx
//!   are warned about (not failed) so a stale overlay does not brick a
//!   server boot.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use thiserror::Error;

use crate::config::DataSource;

/// Required value of the root `kind:` discriminator.
pub const SEMANTICS_KIND: &str = "semantics";

/// Top-level envelope for a semantics YAML file. Mirrors the
/// `kind / metadata / spec` shape used by context, pipelines, jobs, and
/// aliases.
///
/// `kind` is `Option<String>` so the loader can distinguish "no kind"
/// (legitimate, treat as not-a-semantics file when scanning a mixed dir)
/// from "wrong kind" (only relevant when the file was named explicitly).
/// `metadata` is opaque — nothing at runtime reads inside it, but the
/// field is kept so a typo (`metdata:`) surfaces at parse time.
#[derive(Debug, Clone, Deserialize)]
pub struct SemanticsFile {
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    pub metadata: serde_yaml::Value,
    #[serde(default)]
    pub spec: SemanticsSpec,
}

/// `spec:` block — a flat list of per-source overlays.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SemanticsSpec {
    #[serde(default)]
    pub sources: Vec<SourceSemantics>,
}

/// Per-source overlay. `name` matches a `data_sources[].name` from the
/// loaded context. `description` overrides the ctx-inline description on
/// the catalog response. `columns` is optional — supply only the columns
/// that need a description.
#[derive(Debug, Clone, Deserialize)]
pub struct SourceSemantics {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub columns: Vec<ColumnSemantics>,
}

/// Per-column overlay. `name` must match the column name as it appears in
/// the registered Arrow schema (case-sensitive).
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnSemantics {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Error, Debug)]
pub enum SemanticsError {
    #[error("Semantics path not found: {path:?}")]
    PathNotFound { path: PathBuf },

    #[error("Semantics file {path:?} has unexpected kind {found:?}; expected `kind: semantics`")]
    WrongKind { path: PathBuf, found: String },

    #[error("Semantics file {path:?} is missing the root `kind:` field")]
    MissingKind { path: PathBuf },

    #[error("Failed to parse semantics file {path:?}: {error}")]
    ParseError { path: PathBuf, error: String },

    #[error(
        "Duplicate semantics entry for source `{source_name}` (already defined in {first:?}, redefined in {second:?})"
    )]
    DuplicateSource {
        source_name: String,
        first: PathBuf,
        second: PathBuf,
    },

    #[error(
        "Duplicate semantics entry for column `{source_name}.{column}` (already defined in {first:?}, redefined in {second:?})"
    )]
    DuplicateColumn {
        source_name: String,
        column: String,
        first: PathBuf,
        second: PathBuf,
    },
}

/// In-memory lookup attached to `ServerConfig` and consumed by the catalog
/// HTTP handler. Indexed by source name; per-column descriptions live in a
/// nested map.
///
/// The registry merges *all* semantics files passed to `--semantics`, plus
/// the ctx-inline `description` field, into a single view. Lookups in the
/// HTTP handler should read from this struct only, not from the raw
/// `DataSource` list, so that auto-generated overlays and hand-written
/// ones flow through the same path.
#[derive(Debug, Clone, Default)]
pub struct SemanticsRegistry {
    sources: HashMap<String, SourceEntry>,
}

#[derive(Debug, Clone, Default)]
struct SourceEntry {
    description: Option<String>,
    /// `column name → description`. Only columns with a non-empty description live here.
    columns: HashMap<String, String>,
    /// Origin file for the source-level description, used to render a
    /// helpful "redefined here" error if a second file collides.
    description_origin: Option<PathBuf>,
    /// Origin file per column, same purpose.
    column_origins: HashMap<String, PathBuf>,
}

impl SemanticsRegistry {
    /// Build the registry for the running server.
    ///
    /// `semantics_path` may be a single file, a directory, or `None`.
    /// `data_sources` is the ctx-loaded data source list — used both for
    /// the inline-description fallback and for the dangling-reference
    /// validation pass at the end.
    pub fn build(semantics_path: Option<&Path>, data_sources: &[DataSource]) -> Result<Self> {
        let mut registry = Self::default();

        // Seed with ctx-inline descriptions. Semantics-file entries can
        // overwrite these (with their own collision-tracking origin), so
        // load order is: ctx first, files second.
        for ds in data_sources {
            if let Some(desc) = &ds.description
                && !desc.is_empty()
            {
                registry
                    .sources
                    .entry(ds.name.clone())
                    .or_default()
                    .description = Some(desc.clone());
            }
        }

        // Now walk semantics files (if any) and merge.
        if let Some(path) = semantics_path {
            let files = resolve_semantics_files(path)?;
            for file_path in &files {
                let Some(loaded) = load_semantics_file(file_path)? else {
                    tracing::debug!("Skipping {:?}: no `kind: semantics` at root", file_path);
                    continue;
                };
                registry.merge(loaded.spec, file_path)?;
            }
        }

        // Warn (not fail) on dangling references — auto-generated overlays
        // shouldn't brick a partially-rebooted ctx.
        registry.warn_on_dangling_refs(data_sources);

        Ok(registry)
    }

    /// Merge a parsed semantics spec into the registry, hard-erroring on
    /// duplicate source or column entries.
    fn merge(&mut self, spec: SemanticsSpec, origin: &Path) -> Result<()> {
        for source in spec.sources {
            let entry = self.sources.entry(source.name.clone()).or_default();

            if let Some(desc) = source.description {
                if let Some(prior) = &entry.description_origin {
                    return Err(SemanticsError::DuplicateSource {
                        source_name: source.name.clone(),
                        first: prior.clone(),
                        second: origin.to_path_buf(),
                    }
                    .into());
                }
                entry.description = Some(desc);
                entry.description_origin = Some(origin.to_path_buf());
            }

            for col in source.columns {
                if let Some(desc) = col.description {
                    if let Some(prior) = entry.column_origins.get(&col.name) {
                        return Err(SemanticsError::DuplicateColumn {
                            source_name: source.name.clone(),
                            column: col.name.clone(),
                            first: prior.clone(),
                            second: origin.to_path_buf(),
                        }
                        .into());
                    }
                    entry.columns.insert(col.name.clone(), desc);
                    entry.column_origins.insert(col.name, origin.to_path_buf());
                }
            }
        }
        Ok(())
    }

    fn warn_on_dangling_refs(&self, data_sources: &[DataSource]) {
        let known: HashSet<&str> = data_sources.iter().map(|ds| ds.name.as_str()).collect();
        for name in self.sources.keys() {
            if !known.contains(name.as_str()) {
                tracing::warn!(
                    "Semantics references unknown data source `{name}`; entry will be ignored \
                     until a matching source is added to the context"
                );
            }
        }
    }

    /// Table-level description for a data source (semantics override > ctx-inline).
    pub fn table_description(&self, source: &str) -> Option<&str> {
        self.sources
            .get(source)
            .and_then(|e| e.description.as_deref())
    }

    /// Column-level description for `(source, column)`. Returns `None` when
    /// no overlay exists.
    pub fn column_description(&self, source: &str, column: &str) -> Option<&str> {
        self.sources
            .get(source)
            .and_then(|e| e.columns.get(column).map(String::as_str))
    }
}

/// Walk a path that may be either a single yaml file or a directory of yaml
/// files. Mirrors `resolve_pipeline_files` semantics: a directory yields
/// every `*.yaml` / `*.yml` at one level, sorted alphabetically; a missing
/// path is a hard error.
fn resolve_semantics_files(path: &Path) -> Result<Vec<PathBuf>> {
    if !path.exists() {
        return Err(SemanticsError::PathNotFound {
            path: path.to_path_buf(),
        }
        .into());
    }

    if path.is_file() {
        return Ok(vec![path.to_path_buf()]);
    }

    if path.is_dir() {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(path)
            .with_context(|| format!("Failed to read semantics directory: {:?}", path))?
        {
            let entry = entry.with_context(|| "Failed to read directory entry")?;
            let p = entry.path();
            if p.is_file()
                && let Some(ext) = p.extension()
            {
                let ext = ext.to_string_lossy().to_lowercase();
                if ext == "yaml" || ext == "yml" {
                    out.push(p);
                }
            }
        }
        out.sort();
        return Ok(out);
    }

    Err(SemanticsError::PathNotFound {
        path: path.to_path_buf(),
    }
    .into())
}

/// Load a single yaml file. Returns `Ok(None)` when the file's root `kind`
/// is missing or set to something other than `semantics` — that is a soft
/// skip during directory scans, mirroring the jobs loader. A malformed
/// yaml or a `kind: semantics` file with broken structure is a hard error.
fn load_semantics_file(path: &Path) -> Result<Option<SemanticsFile>> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read semantics file: {:?}", path))?;

    // Peek at the kind first so non-semantics files in a mixed dir parse
    // cheaply and don't trip the strict struct deserialization below.
    #[derive(Deserialize)]
    struct KindOnly {
        #[serde(default)]
        kind: Option<String>,
    }
    let peek: KindOnly = serde_yaml::from_str(&raw).map_err(|e| SemanticsError::ParseError {
        path: path.to_path_buf(),
        error: e.to_string(),
    })?;

    match peek.kind.as_deref() {
        Some(SEMANTICS_KIND) => {}
        Some(_) | None => return Ok(None),
    }

    let parsed: SemanticsFile =
        serde_yaml::from_str(&raw).map_err(|e| SemanticsError::ParseError {
            path: path.to_path_buf(),
            error: e.to_string(),
        })?;

    Ok(Some(parsed))
}

/// Load a single yaml file that the caller already knows is supposed to be
/// a semantics file (for example, because `--semantics path/to/file.yaml`
/// was passed explicitly). Unlike [`load_semantics_file`], a missing or
/// wrong `kind:` is a hard error here.
#[allow(dead_code)]
pub fn load_required_semantics_file(path: &Path) -> Result<SemanticsFile> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read semantics file: {:?}", path))?;

    let parsed: SemanticsFile =
        serde_yaml::from_str(&raw).map_err(|e| SemanticsError::ParseError {
            path: path.to_path_buf(),
            error: e.to_string(),
        })?;

    match parsed.kind.as_deref() {
        Some(SEMANTICS_KIND) => Ok(parsed),
        Some(other) => Err(SemanticsError::WrongKind {
            path: path.to_path_buf(),
            found: other.to_string(),
        }
        .into()),
        None => Err(SemanticsError::MissingKind {
            path: path.to_path_buf(),
        }
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DataSource, DataSourceType};
    use std::io::Write;
    use tempfile::TempDir;

    fn ds(name: &str, description: Option<&str>) -> DataSource {
        DataSource {
            name: name.to_string(),
            source_type: DataSourceType::Csv,
            path: PathBuf::new(),
            connection_string: None,
            schema: None,
            options: None,
            hierarchy_level: Default::default(),
            access_mode: Default::default(),
            enable_cache: false,
            description: description.map(str::to_string),
        }
    }

    fn write_yaml(dir: &Path, name: &str, content: &str) -> PathBuf {
        let p = dir.join(name);
        let mut f = std::fs::File::create(&p).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        p
    }

    #[test]
    fn loads_single_semantics_file() {
        let tmp = TempDir::new().unwrap();
        let path = write_yaml(
            tmp.path(),
            "sem.yaml",
            r#"
kind: semantics
metadata:
  name: t
  version: 1.0.0
spec:
  sources:
    - name: products
      description: "Product catalog"
      columns:
        - name: price_usd
          description: "Retail price in USD"
"#,
        );

        let sources = vec![ds("products", None)];
        let reg = SemanticsRegistry::build(Some(&path), &sources).unwrap();
        assert_eq!(reg.table_description("products"), Some("Product catalog"));
        assert_eq!(
            reg.column_description("products", "price_usd"),
            Some("Retail price in USD")
        );
        assert_eq!(reg.column_description("products", "missing"), None);
    }

    #[test]
    fn ctx_inline_description_used_as_fallback() {
        let sources = vec![ds("products", Some("From ctx"))];
        let reg = SemanticsRegistry::build(None, &sources).unwrap();
        assert_eq!(reg.table_description("products"), Some("From ctx"));
    }

    #[test]
    fn semantics_file_overrides_ctx_inline() {
        let tmp = TempDir::new().unwrap();
        let path = write_yaml(
            tmp.path(),
            "sem.yaml",
            r#"
kind: semantics
metadata:
  name: t
spec:
  sources:
    - name: products
      description: "Override"
"#,
        );

        let sources = vec![ds("products", Some("From ctx"))];
        let reg = SemanticsRegistry::build(Some(&path), &sources).unwrap();
        assert_eq!(reg.table_description("products"), Some("Override"));
    }

    #[test]
    fn directory_skips_non_semantics_yamls() {
        let tmp = TempDir::new().unwrap();
        write_yaml(
            tmp.path(),
            "sem.yaml",
            r#"
kind: semantics
metadata: { name: t }
spec:
  sources:
    - name: products
      description: "Product catalog"
"#,
        );
        write_yaml(
            tmp.path(),
            "pipeline.yaml",
            r#"
kind: pipeline
metadata: { name: p, version: 1.0.0 }
spec:
  query: SELECT 1
"#,
        );

        let sources = vec![ds("products", None)];
        let reg = SemanticsRegistry::build(Some(tmp.path()), &sources).unwrap();
        assert_eq!(reg.table_description("products"), Some("Product catalog"));
    }

    #[test]
    fn duplicate_source_description_is_hard_error() {
        let tmp = TempDir::new().unwrap();
        write_yaml(
            tmp.path(),
            "a.yaml",
            r#"
kind: semantics
metadata: { name: a }
spec:
  sources:
    - name: products
      description: "first"
"#,
        );
        write_yaml(
            tmp.path(),
            "b.yaml",
            r#"
kind: semantics
metadata: { name: b }
spec:
  sources:
    - name: products
      description: "second"
"#,
        );

        let sources = vec![ds("products", None)];
        let err = SemanticsRegistry::build(Some(tmp.path()), &sources).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("Duplicate semantics entry for source `products`"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn duplicate_column_description_is_hard_error() {
        let tmp = TempDir::new().unwrap();
        write_yaml(
            tmp.path(),
            "a.yaml",
            r#"
kind: semantics
metadata: { name: a }
spec:
  sources:
    - name: products
      columns:
        - name: price_usd
          description: "first"
"#,
        );
        write_yaml(
            tmp.path(),
            "b.yaml",
            r#"
kind: semantics
metadata: { name: b }
spec:
  sources:
    - name: products
      columns:
        - name: price_usd
          description: "second"
"#,
        );

        let sources = vec![ds("products", None)];
        let err = SemanticsRegistry::build(Some(tmp.path()), &sources).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("Duplicate semantics entry for column `products.price_usd`"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn dangling_reference_warns_but_does_not_fail() {
        let tmp = TempDir::new().unwrap();
        let path = write_yaml(
            tmp.path(),
            "sem.yaml",
            r#"
kind: semantics
metadata: { name: t }
spec:
  sources:
    - name: orphan
      description: "no matching source"
"#,
        );

        let sources = vec![ds("products", None)];
        let reg = SemanticsRegistry::build(Some(&path), &sources).unwrap();
        // The orphan entry is still in the registry — it just gets a warning at load time.
        assert_eq!(reg.table_description("orphan"), Some("no matching source"));
        assert_eq!(reg.table_description("products"), None);
    }

    #[test]
    fn missing_kind_in_explicit_file_is_treated_as_skip_for_directory_scan() {
        // Single-file mode through `build()` goes through `load_semantics_file`
        // (the soft-skip variant) — a yaml without `kind: semantics` is
        // silently ignored even when passed explicitly. This matches the
        // behavior of `--jobs path/to/single.yaml` for non-job files.
        let tmp = TempDir::new().unwrap();
        let path = write_yaml(
            tmp.path(),
            "stray.yaml",
            r#"
metadata: { name: not-a-semantics }
spec:
  sources:
    - name: products
      description: "ignored"
"#,
        );

        let sources = vec![ds("products", None)];
        let reg = SemanticsRegistry::build(Some(&path), &sources).unwrap();
        assert_eq!(reg.table_description("products"), None);
    }

    #[test]
    fn required_loader_rejects_missing_kind() {
        let tmp = TempDir::new().unwrap();
        let path = write_yaml(
            tmp.path(),
            "stray.yaml",
            r#"
metadata: { name: x }
spec:
  sources: []
"#,
        );

        let err = load_required_semantics_file(&path).unwrap_err();
        assert!(format!("{err}").contains("missing the root `kind:` field"));
    }

    #[test]
    fn required_loader_rejects_wrong_kind() {
        let tmp = TempDir::new().unwrap();
        let path = write_yaml(
            tmp.path(),
            "stray.yaml",
            r#"
kind: pipeline
metadata: { name: x, version: 1.0.0 }
spec:
  query: SELECT 1
"#,
        );

        let err = load_required_semantics_file(&path).unwrap_err();
        assert!(format!("{err}").contains("unexpected kind"));
    }

    #[test]
    fn empty_path_input_returns_empty_registry() {
        let sources: Vec<DataSource> = Vec::new();
        let reg = SemanticsRegistry::build(None, &sources).unwrap();
        assert_eq!(reg.table_description("anything"), None);
    }
}
