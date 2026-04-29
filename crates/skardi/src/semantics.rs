//! Natural-language semantics overlays for the catalog.
//!
//! Semantics files are a Kubernetes-style YAML kind (`kind: semantics`) that
//! attach human-readable descriptions to tables and columns already
//! registered through a `kind: context` file. They are loaded at startup
//! alongside pipelines, jobs, and the context, and consumed by:
//!
//! * `skardi-server`'s `GET /data_source` response, so agents can read the
//!   descriptions when picking a tool.
//! * `skardi query --schema`, which renders the descriptions inline next to
//!   each table and column.
//!
//! ```yaml
//! kind: semantics
//! metadata:
//!   name: basic-semantics
//!   version: 1.0.0
//! spec:
//!   sources:
//!     # Bare name: matches a `data_sources[].name` from the ctx. For
//!     # table-mode sources, this *is* the table description. For
//!     # catalog-mode sources, this is the broad fallback applied to
//!     # every inner table that isn't covered by a qualified entry.
//!     - name: products
//!       description: "Product catalog with pricing/inventory"
//!       columns:
//!         - name: price_usd
//!           description: "Retail price in USD; nullable for unlisted SKUs"
//!
//!     # Qualified `catalog.schema.table`: targets one specific physical
//!     # table inside a catalog-mode source.
//!     - name: mydb.public.users
//!       description: "Auth + profile data, one row per registered account"
//!       columns:
//!         - name: id
//!           description: "Auth UUID"
//! ```
//!
//! Composition rules:
//! - Multiple files may be loaded by pointing at a directory. Files in that
//!   directory whose root `kind:` is not `semantics` are silently skipped,
//!   mirroring how `--jobs` tolerates plain pipelines.
//! - The `name:` field is parsed into a [`SemanticsKey`]: a bare segment
//!   becomes [`SemanticsKey::Source`], three dot-separated segments become
//!   [`SemanticsKey::Qualified`]. Anything else (0, 2, 4+ segments, empty
//!   segments) is a hard error.
//! - Bare and qualified entries live in separate addressing spaces — one
//!   bare and one qualified entry can coexist for the same physical
//!   table, and the qualified entry wins through
//!   [`SemanticsRegistry::resolve_table_description`].
//! - Two entries with the same key (same bare name, or same qualified
//!   triple) at table or column level are a hard error. Auto-generated
//!   overlays must produce non-overlapping files.
//! - References to sources or columns that do not exist on the loaded ctx
//!   are warned about (not failed) so a stale overlay does not brick a
//!   server boot.
//!
//! Auto-discovery (see [`resolve_semantics_source`]):
//! - Both `skardi-server` and `skardi query --schema` look for an overlay
//!   next to the ctx file when no explicit path is supplied:
//!   `<ctx_dir>/semantics/` (directory) or `<ctx_dir>/semantics.yaml`
//!   (single file). Defining both is a hard error to prevent silent
//!   shadowing.

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

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

/// Per-source overlay. `name` is either a bare source name (matching a
/// `data_sources[].name` from the loaded context) or a fully-qualified
/// DataFusion path `catalog.schema.table` that targets one specific
/// physical table — useful for catalog-mode sources that expose many
/// inner tables under a single registration.
///
/// `description` overrides the ctx-inline description on the catalog
/// response. `columns` is optional — supply only the columns that need
/// a description.
#[derive(Debug, Clone, Deserialize)]
pub struct SourceSemantics {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub columns: Vec<ColumnSemantics>,
}

/// Parsed form of `SourceSemantics::name`. Either a bare source name
/// (matches table-mode sources directly and serves as the broad fallback
/// for catalog-mode sources) or a fully-qualified `catalog.schema.table`
/// triple that targets one specific inner table.
///
/// Used as the `HashMap` key inside [`SemanticsRegistry`] so lookups can
/// pick the most specific entry without needing to walk the whole
/// registry.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
enum SemanticsKey {
    /// Bare source name. Matches `data_sources[].name` from the ctx.
    Source(String),
    /// Fully-qualified DataFusion path. Matches a specific physical
    /// table.
    Qualified {
        catalog: String,
        schema: String,
        table: String,
    },
}

impl SemanticsKey {
    /// Parse a user-supplied `name:` string. Accepts either a single
    /// segment (bare source name) or three dot-separated segments
    /// (qualified path). Anything else is a hard error.
    fn parse(name: &str) -> Result<Self> {
        let parts: Vec<&str> = name.split('.').collect();
        match parts.as_slice() {
            [single] if !single.is_empty() => Ok(Self::Source((*single).to_string())),
            [catalog, schema, table]
                if !catalog.is_empty() && !schema.is_empty() && !table.is_empty() =>
            {
                Ok(Self::Qualified {
                    catalog: (*catalog).to_string(),
                    schema: (*schema).to_string(),
                    table: (*table).to_string(),
                })
            }
            _ => bail!(
                "semantics source name `{name}` must be either a bare \
                 source name (e.g. `products`) or a fully-qualified \
                 `catalog.schema.table` path (e.g. `mydb.public.users`)"
            ),
        }
    }

    /// Source name this key references — for bare keys, the name itself;
    /// for qualified keys, the catalog segment (which equals the
    /// catalog-mode source name). Used for dangling-reference checks
    /// against the loaded ctx.
    fn referenced_source(&self) -> &str {
        match self {
            Self::Source(name) => name,
            Self::Qualified { catalog, .. } => catalog,
        }
    }

    /// Human-readable form for error messages, matching the user's input.
    fn display(&self) -> String {
        match self {
            Self::Source(name) => name.clone(),
            Self::Qualified {
                catalog,
                schema,
                table,
            } => format!("{catalog}.{schema}.{table}"),
        }
    }
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

    #[error(
        "Ambiguous semantics auto-discovery: both {dir:?} and {file:?} exist next to the ctx file. \
         Remove one (or pass --semantics explicitly) so the loader knows which to use."
    )]
    AmbiguousAutoDiscovery { dir: PathBuf, file: PathBuf },
}

/// In-memory lookup attached to `ServerConfig` (server side) or built
/// on-demand by `skardi query --schema` (CLI side). Indexed by
/// [`SemanticsKey`] so a single registry can hold both bare source-name
/// entries and fully-qualified `catalog.schema.table` entries; per-column
/// descriptions live in a nested map.
///
/// The registry merges *all* semantics files passed in, plus the
/// ctx-inline `description` field, into a single view. Lookups should
/// read from this struct only, not from the raw data source list, so
/// that auto-generated overlays and hand-written ones flow through the
/// same path.
///
/// The inner map is `Arc`-wrapped so cloning the registry (e.g. once per
/// `GET /data_source` request to release the config lock before async
/// work) is O(1).
#[derive(Debug, Clone, Default)]
pub struct SemanticsRegistry {
    entries: Arc<HashMap<SemanticsKey, SourceEntry>>,
}

#[derive(Debug, Clone, Default)]
struct SourceEntry {
    description: Option<String>,
    /// `column name → description`. Only columns with a non-empty description live here.
    columns: HashMap<String, String>,
    /// Origin file for the source-level description, used to render a
    /// helpful "redefined here" error if a second file collides. `None`
    /// when the description came from the ctx-inline seed (which is
    /// always allowed to be overwritten by a semantics file).
    description_origin: Option<PathBuf>,
    /// Origin file per column, same purpose.
    column_origins: HashMap<String, PathBuf>,
}

impl SemanticsRegistry {
    /// Build the registry.
    ///
    /// `semantics_path` may be a single file, a directory, or `None`.
    /// `ctx_descriptions` is the ctx-loaded list of `(source_name,
    /// inline_description)` pairs — used both for the inline-description
    /// fallback and for the dangling-reference validation pass at the end.
    pub fn build(
        semantics_path: Option<&Path>,
        ctx_descriptions: &[(String, Option<String>)],
    ) -> Result<Self> {
        let mut entries: HashMap<SemanticsKey, SourceEntry> = HashMap::new();

        // Seed with ctx-inline descriptions. Semantics-file entries can
        // overwrite these (with their own collision-tracking origin), so
        // load order is: ctx first, files second.
        // Ctx-inline descriptions are always source-level (bare name),
        // since `data_sources[]` only addresses sources, not inner tables.
        for (name, desc) in ctx_descriptions {
            if let Some(d) = desc.as_deref()
                && !d.is_empty()
            {
                entries
                    .entry(SemanticsKey::Source(name.clone()))
                    .or_default()
                    .description = Some(d.to_string());
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
                merge_into(&mut entries, loaded.spec, file_path)?;
            }
        }

        // Warn (not fail) on dangling references — auto-generated overlays
        // shouldn't brick a partially-rebooted ctx.
        warn_on_dangling_refs(&entries, ctx_descriptions);

        Ok(Self {
            entries: Arc::new(entries),
        })
    }

    /// Bare source-name lookup. Matches the `name: foo` form in a
    /// semantics file (or the ctx-inline `data_sources[].description`
    /// fallback). For table-mode sources where the source name *is* the
    /// physical table name, this is the only form that resolves.
    pub fn table_description(&self, source: &str) -> Option<&str> {
        self.entries
            .get(&SemanticsKey::Source(source.to_string()))
            .and_then(|e| e.description.as_deref())
    }

    /// Bare-source column lookup. See [`Self::table_description`] for the
    /// addressing model.
    pub fn column_description(&self, source: &str, column: &str) -> Option<&str> {
        self.entries
            .get(&SemanticsKey::Source(source.to_string()))
            .and_then(|e| e.columns.get(column).map(String::as_str))
    }

    /// Fully-qualified table lookup — matches the `name:
    /// catalog.schema.table` form. Used to address a specific inner
    /// table of a catalog-mode source.
    pub fn qualified_table_description(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Option<&str> {
        self.entries
            .get(&SemanticsKey::Qualified {
                catalog: catalog.to_string(),
                schema: schema.to_string(),
                table: table.to_string(),
            })
            .and_then(|e| e.description.as_deref())
    }

    /// Fully-qualified column lookup. See
    /// [`Self::qualified_table_description`] for the addressing model.
    pub fn qualified_column_description(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        column: &str,
    ) -> Option<&str> {
        self.entries
            .get(&SemanticsKey::Qualified {
                catalog: catalog.to_string(),
                schema: schema.to_string(),
                table: table.to_string(),
            })
            .and_then(|e| e.columns.get(column).map(String::as_str))
    }

    /// Resolve a description for a physical `(catalog, schema, table)`
    /// triple, with fallback through the bare source name. Most-specific
    /// match wins:
    ///
    /// 1. Qualified `name: catalog.schema.table` entry, or
    /// 2. Bare `name: <source>` entry (for table-mode sources, or the
    ///    catalog-mode broad fallback), or
    /// 3. `None`.
    ///
    /// Pass `source = None` if the caller can't resolve a bare source
    /// name (e.g. ad-hoc URL-registered tables); that path will skip
    /// step 2.
    pub fn resolve_table_description(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        source: Option<&str>,
    ) -> Option<&str> {
        self.qualified_table_description(catalog, schema, table)
            .or_else(|| source.and_then(|s| self.table_description(s)))
    }

    /// Same fall-through as [`Self::resolve_table_description`], but
    /// for one column inside the physical table.
    pub fn resolve_column_description(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        source: Option<&str>,
        column: &str,
    ) -> Option<&str> {
        self.qualified_column_description(catalog, schema, table, column)
            .or_else(|| source.and_then(|s| self.column_description(s, column)))
    }

    /// True when no overlay (file or ctx-inline) registered any
    /// description. Used by `skardi query --schema` to skip the rendering
    /// path entirely when there is nothing to show.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Merge a parsed semantics spec into the in-progress entry map,
/// hard-erroring on duplicate entries (same key, same column).
///
/// Each `name:` is parsed into a [`SemanticsKey`]: a bare segment becomes
/// `Source(name)`, three dot-separated segments become a `Qualified`
/// triple. A 1-part entry and a 3-part entry are different keys — both
/// can coexist; lookup precedence is handled at the registry level.
///
/// Empty-string descriptions (`description: ""`) are treated as absent —
/// same policy as the ctx-inline seed pass — so they never overwrite a
/// non-empty fallback nor count toward the duplicate-detection.
fn merge_into(
    entries: &mut HashMap<SemanticsKey, SourceEntry>,
    spec: SemanticsSpec,
    origin: &Path,
) -> Result<()> {
    for source in spec.sources {
        let key = SemanticsKey::parse(&source.name)
            .with_context(|| format!("In semantics file {origin:?}"))?;
        let key_display = key.display();
        let entry = entries.entry(key.clone()).or_default();

        if let Some(desc) = source.description.filter(|d| !d.is_empty()) {
            if let Some(prior) = &entry.description_origin {
                return Err(SemanticsError::DuplicateSource {
                    source_name: key_display.clone(),
                    first: prior.clone(),
                    second: origin.to_path_buf(),
                }
                .into());
            }
            entry.description = Some(desc);
            entry.description_origin = Some(origin.to_path_buf());
        }

        for col in source.columns {
            if let Some(desc) = col.description.filter(|d| !d.is_empty()) {
                if let Some(prior) = entry.column_origins.get(&col.name) {
                    return Err(SemanticsError::DuplicateColumn {
                        source_name: key_display.clone(),
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

/// Warn for entries that don't reference a known ctx source.
///
/// For a bare `Source(name)` key, "known" means `name` appears in
/// `data_sources[].name`. For a `Qualified { catalog, .. }` key, we check
/// the catalog segment, since for catalog-mode sources the catalog name
/// equals the ctx source name. (Unknown inner schemas/tables aren't
/// validated here — they're resolved against the live Arrow schema at
/// render time.)
fn warn_on_dangling_refs(
    entries: &HashMap<SemanticsKey, SourceEntry>,
    ctx_descriptions: &[(String, Option<String>)],
) {
    let known: HashSet<&str> = ctx_descriptions.iter().map(|(n, _)| n.as_str()).collect();
    for key in entries.keys() {
        let referenced = key.referenced_source();
        if !known.contains(referenced) {
            let key_str = key.display();
            tracing::warn!(
                "Semantics references unknown data source `{key_str}`; entry will be ignored \
                 until a matching source is added to the context"
            );
        }
    }
}

/// Resolve which semantics path the loader should use, given an explicit
/// override and/or a ctx directory to auto-discover from.
///
/// Resolution order:
/// 1. `override_path` (e.g. `--semantics <path>`) — used directly if
///    `Some`. No existence check here; the downstream loader will report
///    a missing path with a clearer message.
/// 2. `<ctx_dir>/semantics/` if it exists as a directory.
/// 3. `<ctx_dir>/semantics.yaml` (or `.yml`) if it exists as a file.
/// 4. `None` — no overlay configured.
///
/// Defining both `<ctx_dir>/semantics/` and `<ctx_dir>/semantics.yaml`
/// is a hard error: silent shadowing of overlays that drive an agent's
/// catalog view is exactly the sort of bug we want loud.
pub fn resolve_semantics_source(
    ctx_dir: Option<&Path>,
    override_path: Option<&Path>,
) -> Result<Option<PathBuf>> {
    if let Some(p) = override_path {
        return Ok(Some(p.to_path_buf()));
    }
    let Some(dir) = ctx_dir else {
        return Ok(None);
    };

    let dir_path = dir.join("semantics");
    let yaml_path = dir.join("semantics.yaml");
    let yml_path = dir.join("semantics.yml");

    let dir_exists = dir_path.is_dir();
    let single_file = if yaml_path.is_file() {
        Some(yaml_path)
    } else if yml_path.is_file() {
        Some(yml_path)
    } else {
        None
    };

    match (dir_exists, single_file) {
        (true, Some(file)) => Err(SemanticsError::AmbiguousAutoDiscovery {
            dir: dir_path,
            file,
        }
        .into()),
        (true, None) => Ok(Some(dir_path)),
        (false, Some(file)) => Ok(Some(file)),
        (false, None) => Ok(None),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn ctx(name: &str, description: Option<&str>) -> (String, Option<String>) {
        (name.to_string(), description.map(str::to_string))
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

        let sources = vec![ctx("products", None)];
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
        let sources = vec![ctx("products", Some("From ctx"))];
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

        let sources = vec![ctx("products", Some("From ctx"))];
        let reg = SemanticsRegistry::build(Some(&path), &sources).unwrap();
        assert_eq!(reg.table_description("products"), Some("Override"));
    }

    #[test]
    fn empty_string_description_in_file_does_not_override_ctx_fallback() {
        // `description: ""` in a semantics file is treated as "no
        // description" — same policy as the ctx-inline seed pass — so it
        // must not overwrite a non-empty ctx fallback. Empty column
        // descriptions are likewise absent.
        let tmp = TempDir::new().unwrap();
        let path = write_yaml(
            tmp.path(),
            "sem.yaml",
            r#"
kind: semantics
metadata: { name: t }
spec:
  sources:
    - name: products
      description: ""
      columns:
        - name: id
          description: ""
"#,
        );

        let sources = vec![ctx("products", Some("From ctx"))];
        let reg = SemanticsRegistry::build(Some(&path), &sources).unwrap();
        assert_eq!(
            reg.table_description("products"),
            Some("From ctx"),
            "empty file description must not stomp the ctx fallback"
        );
        assert_eq!(reg.column_description("products", "id"), None);
    }

    #[test]
    fn empty_string_description_does_not_trigger_duplicate_error() {
        // First file sets a real description; second file has `description: ""`
        // for the same source. Since empty is treated as absent, this is
        // *not* a collision and the original description survives.
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
      columns:
        - name: price
          description: "real price"
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
      description: ""
      columns:
        - name: price
          description: ""
"#,
        );

        let sources = vec![ctx("products", None)];
        let reg = SemanticsRegistry::build(Some(tmp.path()), &sources).unwrap();
        assert_eq!(reg.table_description("products"), Some("first"));
        assert_eq!(
            reg.column_description("products", "price"),
            Some("real price")
        );
    }

    // ---------- qualified-path keys (catalog.schema.table) ----------

    #[test]
    fn semantics_key_parse_accepts_bare_and_qualified() {
        assert!(matches!(
            SemanticsKey::parse("products").unwrap(),
            SemanticsKey::Source(s) if s == "products"
        ));
        let qualified = SemanticsKey::parse("mydb.public.users").unwrap();
        match qualified {
            SemanticsKey::Qualified {
                catalog,
                schema,
                table,
            } => {
                assert_eq!(catalog, "mydb");
                assert_eq!(schema, "public");
                assert_eq!(table, "users");
            }
            _ => panic!("expected Qualified, got {qualified:?}"),
        }
    }

    #[test]
    fn semantics_key_parse_rejects_two_part_path() {
        // Two parts is ambiguous (schema.table? source.table?). We
        // require either 1 segment or all 3, never something in
        // between.
        let err = SemanticsKey::parse("schema.table").unwrap_err();
        assert!(
            format!("{err}").contains("must be either a bare"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn semantics_key_parse_rejects_too_many_parts() {
        let err = SemanticsKey::parse("a.b.c.d").unwrap_err();
        assert!(format!("{err}").contains("must be either a bare"));
    }

    #[test]
    fn semantics_key_parse_rejects_empty_segments() {
        // Leading / trailing / interior empty segment ("a..b") is invalid.
        assert!(SemanticsKey::parse("").is_err());
        assert!(SemanticsKey::parse("..").is_err());
        assert!(SemanticsKey::parse("mydb..users").is_err());
        assert!(SemanticsKey::parse(".mydb.public.users").is_err());
    }

    #[test]
    fn malformed_name_in_semantics_file_is_hard_error() {
        let tmp = TempDir::new().unwrap();
        let path = write_yaml(
            tmp.path(),
            "sem.yaml",
            r#"
kind: semantics
metadata: { name: t }
spec:
  sources:
    - name: schema.table        # 2-part is invalid
      description: "x"
"#,
        );
        let sources = vec![ctx("anything", None)];
        let err = SemanticsRegistry::build(Some(&path), &sources).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("must be either a bare"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn qualified_path_resolves_specific_inner_table() {
        // A `name: catalog.schema.table` entry is reachable via the
        // qualified-lookup helpers and is *separate* from any bare
        // `name: catalog` entry.
        let tmp = TempDir::new().unwrap();
        let path = write_yaml(
            tmp.path(),
            "sem.yaml",
            r#"
kind: semantics
metadata: { name: t }
spec:
  sources:
    - name: wiki.main.pages
      description: "Wiki page contents"
      columns:
        - name: title
          description: "Page title"
"#,
        );

        let sources = vec![ctx("wiki", None)];
        let reg = SemanticsRegistry::build(Some(&path), &sources).unwrap();
        assert_eq!(
            reg.qualified_table_description("wiki", "main", "pages"),
            Some("Wiki page contents")
        );
        assert_eq!(
            reg.qualified_column_description("wiki", "main", "pages", "title"),
            Some("Page title")
        );
        // The bare lookup must NOT pick up the qualified entry — it's a
        // different addressing space.
        assert_eq!(reg.table_description("wiki"), None);
        assert_eq!(reg.column_description("wiki", "title"), None);
    }

    #[test]
    fn resolve_table_description_prefers_qualified_over_bare() {
        // Both `wiki` and `wiki.main.pages` describe the same physical
        // table. The qualified form is more specific, so it wins through
        // resolve_table_description.
        let tmp = TempDir::new().unwrap();
        let path = write_yaml(
            tmp.path(),
            "sem.yaml",
            r#"
kind: semantics
metadata: { name: t }
spec:
  sources:
    - name: wiki
      description: "broad fallback"
      columns:
        - name: title
          description: "broad title"
    - name: wiki.main.pages
      description: "specific to pages"
      columns:
        - name: title
          description: "specific title"
"#,
        );

        let sources = vec![ctx("wiki", None)];
        let reg = SemanticsRegistry::build(Some(&path), &sources).unwrap();
        assert_eq!(
            reg.resolve_table_description("wiki", "main", "pages", Some("wiki")),
            Some("specific to pages")
        );
        assert_eq!(
            reg.resolve_column_description("wiki", "main", "pages", Some("wiki"), "title"),
            Some("specific title")
        );
        // For an inner table that has no qualified overlay, the bare
        // `wiki` fallback still applies.
        assert_eq!(
            reg.resolve_table_description("wiki", "main", "revisions", Some("wiki")),
            Some("broad fallback")
        );
        assert_eq!(
            reg.resolve_column_description("wiki", "main", "revisions", Some("wiki"), "title"),
            Some("broad title")
        );
    }

    #[test]
    fn resolve_falls_back_to_none_when_neither_form_present() {
        let sources = vec![ctx("wiki", None)];
        let reg = SemanticsRegistry::build(None, &sources).unwrap();
        assert_eq!(
            reg.resolve_table_description("wiki", "main", "pages", Some("wiki")),
            None
        );
        // No source name passed at all — still fine, just returns None.
        assert_eq!(
            reg.resolve_table_description("wiki", "main", "pages", None),
            None
        );
    }

    #[test]
    fn duplicate_qualified_entry_is_hard_error() {
        // Same `(catalog, schema, table)` defined in two files: still
        // a collision because the keys are equal.
        let tmp = TempDir::new().unwrap();
        write_yaml(
            tmp.path(),
            "a.yaml",
            r#"
kind: semantics
metadata: { name: a }
spec:
  sources:
    - name: wiki.main.pages
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
    - name: wiki.main.pages
      description: "second"
"#,
        );
        let sources = vec![ctx("wiki", None)];
        let err = SemanticsRegistry::build(Some(tmp.path()), &sources).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("Duplicate semantics entry for source `wiki.main.pages`"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn bare_and_qualified_for_same_source_coexist() {
        // A source-level entry and a qualified entry are *not*
        // duplicates of each other — they're addressed at different
        // levels and resolve_table_description handles precedence.
        let tmp = TempDir::new().unwrap();
        let path = write_yaml(
            tmp.path(),
            "sem.yaml",
            r#"
kind: semantics
metadata: { name: t }
spec:
  sources:
    - name: wiki
      description: "broad"
    - name: wiki.main.pages
      description: "specific"
"#,
        );
        let sources = vec![ctx("wiki", None)];
        // Should NOT error.
        let reg = SemanticsRegistry::build(Some(&path), &sources).unwrap();
        assert_eq!(reg.table_description("wiki"), Some("broad"));
        assert_eq!(
            reg.qualified_table_description("wiki", "main", "pages"),
            Some("specific")
        );
    }

    #[test]
    fn qualified_dangling_reference_warns_via_catalog_segment() {
        // When the catalog segment of a qualified entry doesn't match
        // any ctx source, we warn (just like with bare-name dangling
        // refs). The entry is still loaded and reachable — we just
        // surface the mismatch in logs.
        let tmp = TempDir::new().unwrap();
        let path = write_yaml(
            tmp.path(),
            "sem.yaml",
            r#"
kind: semantics
metadata: { name: t }
spec:
  sources:
    - name: nope.public.users
      description: "stale"
"#,
        );
        let sources = vec![ctx("wiki", None)];
        let reg = SemanticsRegistry::build(Some(&path), &sources).unwrap();
        // Entry is still queryable — only logged as a warning.
        assert_eq!(
            reg.qualified_table_description("nope", "public", "users"),
            Some("stale")
        );
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

        let sources = vec![ctx("products", None)];
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

        let sources = vec![ctx("products", None)];
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

        let sources = vec![ctx("products", None)];
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

        let sources = vec![ctx("products", None)];
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

        let sources = vec![ctx("products", None)];
        let reg = SemanticsRegistry::build(Some(&path), &sources).unwrap();
        assert_eq!(reg.table_description("products"), None);
    }

    #[test]
    fn empty_path_input_returns_empty_registry() {
        let sources: Vec<(String, Option<String>)> = Vec::new();
        let reg = SemanticsRegistry::build(None, &sources).unwrap();
        assert_eq!(reg.table_description("anything"), None);
        assert!(reg.is_empty());
    }

    // ---------- resolve_semantics_source ----------

    #[test]
    fn resolver_returns_override_when_provided() {
        let tmp = TempDir::new().unwrap();
        let explicit = tmp.path().join("custom.yaml");
        let resolved = resolve_semantics_source(Some(tmp.path()), Some(&explicit)).unwrap();
        assert_eq!(resolved.as_deref(), Some(explicit.as_path()));
    }

    #[test]
    fn resolver_returns_none_when_no_ctx_dir_and_no_override() {
        let resolved = resolve_semantics_source(None, None).unwrap();
        assert!(resolved.is_none());
    }

    #[test]
    fn resolver_picks_directory_when_present() {
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir(tmp.path().join("semantics")).unwrap();
        let resolved = resolve_semantics_source(Some(tmp.path()), None)
            .unwrap()
            .unwrap();
        assert_eq!(resolved, tmp.path().join("semantics"));
    }

    #[test]
    fn resolver_picks_yaml_file_when_present() {
        let tmp = TempDir::new().unwrap();
        let file = tmp.path().join("semantics.yaml");
        std::fs::File::create(&file).unwrap();
        let resolved = resolve_semantics_source(Some(tmp.path()), None)
            .unwrap()
            .unwrap();
        assert_eq!(resolved, file);
    }

    #[test]
    fn resolver_picks_yml_file_when_yaml_missing() {
        let tmp = TempDir::new().unwrap();
        let file = tmp.path().join("semantics.yml");
        std::fs::File::create(&file).unwrap();
        let resolved = resolve_semantics_source(Some(tmp.path()), None)
            .unwrap()
            .unwrap();
        assert_eq!(resolved, file);
    }

    #[test]
    fn resolver_returns_none_when_neither_present() {
        let tmp = TempDir::new().unwrap();
        let resolved = resolve_semantics_source(Some(tmp.path()), None).unwrap();
        assert!(resolved.is_none());
    }

    #[test]
    fn resolver_hard_errors_when_dir_and_file_both_present() {
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir(tmp.path().join("semantics")).unwrap();
        std::fs::File::create(tmp.path().join("semantics.yaml")).unwrap();
        let err = resolve_semantics_source(Some(tmp.path()), None).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("Ambiguous semantics auto-discovery"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn resolver_override_skips_collision_check() {
        // If the user passes an explicit path, we don't even look at the
        // ctx dir — collisions there don't matter.
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir(tmp.path().join("semantics")).unwrap();
        std::fs::File::create(tmp.path().join("semantics.yaml")).unwrap();
        let explicit = tmp.path().join("custom.yaml");
        let resolved = resolve_semantics_source(Some(tmp.path()), Some(&explicit))
            .unwrap()
            .unwrap();
        assert_eq!(resolved, explicit);
    }
}
