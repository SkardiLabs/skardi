//! Load/save the CLI aliases file.
//!
//! Aliases are stored as a top-level YAML map keyed by alias name:
//!
//! ```yaml
//! grep:
//!   pipeline: wiki-search-hybrid
//!   positional: [query]
//!   defaults:
//!     text_query: "{query}"
//!     limit: "10"
//! ls:
//!   pipeline: wiki-list
//!   ...
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

use anyhow::{Context, Result};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::alias::AliasDef;

pub type AliasMap = BTreeMap<String, AliasDef>;

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

/// Load aliases from disk. A missing file is not an error — it yields an
/// empty map, which is the correct starting state for a brand-new project.
pub fn load(path: &Path) -> Result<AliasMap> {
    if !path.exists() {
        return Ok(AliasMap::new());
    }
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read aliases file: {}", path.display()))?;
    if content.trim().is_empty() {
        return Ok(AliasMap::new());
    }
    let map: AliasMap = serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse aliases YAML: {}", path.display()))?;
    Ok(map)
}

/// Save aliases to disk, creating parent directories as needed.
pub fn save(path: &Path, map: &AliasMap) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create aliases directory: {}", parent.display())
            })?;
        }
    }
    let yaml = serde_yaml::to_string(map).context("Failed to serialize aliases map to YAML")?;
    std::fs::write(path, yaml)
        .with_context(|| format!("Failed to write aliases file: {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn sample_alias() -> AliasDef {
        let mut defaults = HashMap::new();
        defaults.insert("text_query".to_string(), "{query}".to_string());
        defaults.insert("limit".to_string(), "10".to_string());
        AliasDef {
            pipeline: "wiki-search-hybrid".to_string(),
            positional: vec!["query".to_string()],
            defaults,
            description: Some("Hybrid search".to_string()),
        }
    }

    #[test]
    fn save_then_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("aliases.yaml");
        let mut map = AliasMap::new();
        map.insert("grep".to_string(), sample_alias());
        save(&path, &map).unwrap();

        let loaded = load(&path).unwrap();
        assert_eq!(loaded.len(), 1);
        let grep = &loaded["grep"];
        assert_eq!(grep.pipeline, "wiki-search-hybrid");
        assert_eq!(grep.positional, vec!["query".to_string()]);
        assert_eq!(grep.defaults["limit"], "10");
    }

    #[test]
    fn load_missing_file_returns_empty_map() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("does-not-exist.yaml");
        let loaded = load(&path).unwrap();
        assert!(loaded.is_empty());
    }
}
