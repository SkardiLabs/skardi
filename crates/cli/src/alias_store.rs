//! Load/save the CLI aliases file.
//!
//! Aliases are stored as a Kubernetes-style manifest: a `kind: aliases`
//! discriminator at the root, a `metadata:` block, and the actual alias
//! entries under `spec:`:
//!
//! ```yaml
//! kind: aliases
//! metadata:
//!   name: wiki-cli-aliases
//!   version: 1.0.0
//!   description: Shortcuts for the llm_wiki CLI
//! spec:
//!   grep:
//!     pipeline: wiki-search-hybrid
//!     positional: [query]
//!     defaults:
//!       text_query: "{query}"
//!       limit: "10"
//!   ls:
//!     pipeline: wiki-list
//!     ...
//! ```
//!
//! The file path is resolved in this order:
//! 1. Explicit `--aliases <path>` flag.
//! 2. `SKARDI_ALIASES` env var.
//! 3. Inside the active "skardi home" directory: `<home>/aliases.yaml`.
//!    Home is derived from the `--ctx` argument or the `SKARDICONFIG` env
//!    var: a directory is used directly, a file uses its parent dir. So
//!    `export SKARDICONFIG=./demo/llm_wiki/cli` (or an equivalent `--ctx`)
//!    is enough to locate the sibling `aliases.yaml`.
//! 4. `~/.skardi/config/aliases.yaml`.

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::alias::AliasDef;

pub type AliasMap = BTreeMap<String, AliasDef>;

/// Root-level `kind:` discriminator. Only `aliases` is accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AliasFileKind {
    Aliases,
}

impl Default for AliasFileKind {
    fn default() -> Self {
        Self::Aliases
    }
}

fn default_version() -> String {
    "1.0.0".to_string()
}

fn default_metadata_name() -> String {
    "aliases".to_string()
}

/// `metadata:` block. `name` and `version` are required on load; `save`
/// fills in sensible defaults for freshly-created files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AliasMetadata {
    #[serde(default = "default_metadata_name")]
    pub name: String,
    #[serde(default = "default_version")]
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl Default for AliasMetadata {
    fn default() -> Self {
        Self {
            name: default_metadata_name(),
            version: default_version(),
            description: None,
        }
    }
}

/// Full on-disk shape of an aliases YAML file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AliasFile {
    #[serde(default)]
    pub kind: AliasFileKind,
    #[serde(default)]
    pub metadata: AliasMetadata,
    #[serde(default)]
    pub spec: AliasMap,
}

impl Default for AliasFile {
    fn default() -> Self {
        Self {
            kind: AliasFileKind::default(),
            metadata: AliasMetadata::default(),
            spec: AliasMap::new(),
        }
    }
}

/// Resolve the aliases file path. Returns `None` when neither an explicit
/// override nor a default home is available.
///
/// Resolution order:
/// 1. `override_path` (the `--aliases` CLI flag).
/// 2. `SKARDI_ALIASES` env var.
/// 3. `<home>/aliases.yaml`, where home is derived from `ctx_path` if given
///    or `SKARDICONFIG` otherwise — a directory is used directly; a file
///    uses its parent. The file must already exist to match here.
/// 4. `~/.skardi/config/aliases.yaml` (default).
pub fn resolve_aliases_path(
    override_path: Option<&Path>,
    ctx_path: Option<&Path>,
) -> Option<PathBuf> {
    if let Some(p) = override_path {
        return Some(p.to_path_buf());
    }
    if let Ok(env_path) = std::env::var("SKARDI_ALIASES") {
        return Some(PathBuf::from(env_path));
    }

    // Prefer the active ctx's home. `ctx_path` (the CLI arg) wins over
    // `SKARDICONFIG` env so an explicit `--ctx` is never silently overridden.
    let source: Option<PathBuf> = ctx_path
        .map(|p| p.to_path_buf())
        .or_else(|| std::env::var("SKARDICONFIG").ok().map(PathBuf::from));
    if let Some(p) = source {
        let home = if p.is_dir() {
            Some(p)
        } else {
            p.parent().map(|parent| parent.to_path_buf())
        };
        if let Some(home) = home {
            let candidate = home.join("aliases.yaml");
            if candidate.exists() {
                return Some(candidate);
            }
        }
    }

    let home_default = dirs::home_dir()?
        .join(".skardi")
        .join("config")
        .join("aliases.yaml");
    Some(home_default)
}

/// Load the aliases file. A missing or empty file is not an error — it
/// yields a default `AliasFile` with an empty `spec`, which is the correct
/// starting state for a brand-new project. The file's `kind:` must be
/// `aliases` when present.
pub fn load(path: &Path) -> Result<AliasFile> {
    if !path.exists() {
        return Ok(AliasFile::default());
    }
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read aliases file: {}", path.display()))?;
    if content.trim().is_empty() {
        return Ok(AliasFile::default());
    }

    // Peek at `kind:` first so we can give a clear error when someone points
    // us at the wrong type of YAML (e.g. a pipeline) rather than a confusing
    // serde mismatch.
    let root: serde_yaml::Value = serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse aliases YAML: {}", path.display()))?;
    if let Some(kind) = root.get("kind") {
        let kind_str = kind.as_str().unwrap_or("");
        if kind_str != "aliases" {
            return Err(anyhow!(
                "Expected `kind: aliases` in {}, got `kind: {kind_str}`",
                path.display()
            ));
        }
    } else {
        return Err(anyhow!(
            "Aliases file {} is missing `kind: aliases` at the root. \
             The file format now requires a `kind`, `metadata`, and `spec` envelope.",
            path.display()
        ));
    }

    let file: AliasFile = serde_yaml::from_value(root)
        .with_context(|| format!("Failed to parse aliases YAML: {}", path.display()))?;
    Ok(file)
}

/// Save the aliases file to disk, creating parent directories as needed.
pub fn save(path: &Path, file: &AliasFile) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create aliases directory: {}", parent.display())
            })?;
        }
    }
    let yaml = serde_yaml::to_string(file).context("Failed to serialize aliases file to YAML")?;
    std::fs::write(path, yaml)
        .with_context(|| format!("Failed to write aliases file: {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample_alias() -> AliasDef {
        let mut defaults = BTreeMap::new();
        defaults.insert("text_query".to_string(), "{query}".to_string());
        defaults.insert("limit".to_string(), "10".to_string());
        AliasDef {
            pipeline: "wiki-search-hybrid".to_string(),
            positional: vec!["query".to_string()],
            defaults,
            description: Some("Hybrid search".to_string()),
        }
    }

    fn sample_file() -> AliasFile {
        let mut file = AliasFile::default();
        file.metadata.name = "test-aliases".to_string();
        file.metadata.description = Some("Unit test fixture".to_string());
        file.spec.insert("grep".to_string(), sample_alias());
        file
    }

    #[test]
    fn save_then_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("aliases.yaml");
        save(&path, &sample_file()).unwrap();

        let loaded = load(&path).unwrap();
        assert_eq!(loaded.kind, AliasFileKind::Aliases);
        assert_eq!(loaded.metadata.name, "test-aliases");
        assert_eq!(loaded.metadata.version, "1.0.0");
        assert_eq!(loaded.spec.len(), 1);
        let grep = &loaded.spec["grep"];
        assert_eq!(grep.pipeline, "wiki-search-hybrid");
        assert_eq!(grep.positional, vec!["query".to_string()]);
        assert_eq!(grep.defaults["limit"], "10");
    }

    #[test]
    fn load_missing_file_returns_empty_default() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("does-not-exist.yaml");
        let loaded = load(&path).unwrap();
        assert!(loaded.spec.is_empty());
        assert_eq!(loaded.kind, AliasFileKind::Aliases);
    }

    #[test]
    fn load_legacy_flat_map_errors_with_clear_message() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("aliases.yaml");
        std::fs::write(
            &path,
            "grep:\n  pipeline: wiki-search-hybrid\n  positional: [query]\n",
        )
        .unwrap();
        let err = load(&path).unwrap_err().to_string();
        assert!(err.contains("kind: aliases"), "unexpected error: {err}");
    }

    #[test]
    fn load_wrong_kind_errors() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("aliases.yaml");
        std::fs::write(&path, "kind: pipeline\nmetadata:\n  name: p\n").unwrap();
        let err = load(&path).unwrap_err().to_string();
        assert!(err.contains("kind: aliases"), "unexpected error: {err}");
    }

    /// Regression guard: the bundled demo alias files should continue to load
    /// under the new envelope shape.
    #[test]
    fn demo_alias_files_still_load() {
        // Walk up from `crates/cli/src/` to the repo root so the test works
        // regardless of how cargo invokes it.
        let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .expect("cli crate lives under <repo>/crates/cli")
            .to_path_buf();

        for rel in [
            "demo/llm_wiki/cli/aliases.yaml",
            "demo/rag/cli/aliases.yaml",
        ] {
            let path = repo_root.join(rel);
            let loaded =
                load(&path).unwrap_or_else(|e| panic!("failed to load {}: {e:?}", path.display()));
            assert_eq!(loaded.kind, AliasFileKind::Aliases);
            assert!(
                !loaded.spec.is_empty(),
                "{} parsed but produced no aliases",
                path.display()
            );
            // Every entry must have a non-empty pipeline target.
            for (name, def) in &loaded.spec {
                assert!(
                    !def.pipeline.is_empty(),
                    "alias {name} in {} has empty pipeline",
                    path.display()
                );
            }
        }
    }
}
