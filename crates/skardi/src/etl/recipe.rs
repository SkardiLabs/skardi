//! Recipes — the semantic layer nobody's schema records.
//!
//! One embedded asset per *(pack, format)*: `etl/recipes/<pack>.<format>.yaml`.
//! A recipe says which pack columns carry the `content`, `title`, `author`,
//! and `timestamp` roles and which fold into `metadata` — knowledge that is
//! curated and contract-tested here, never inferred. Parsing is strict;
//! validation runs against the REAL `SourcePack` so a recipe referencing a
//! column the pack no longer declares fails the build (the contract suite
//! below), the same discipline as pack fingerprint tests.
//!
//! `--recipe` files go through the identical loader and validation with the
//! identical error text (FR-3): a replacement recipe is validated for shape
//! and pack-compatibility, though it forfeits this crate's curation.

use crate::sources::providers::open_connector::json_to_arrow::{FieldMapping, FieldType};
use crate::sources::providers::open_connector::source_pack::{SourcePack, SourcePackTable};

use super::config::TargetFormatKind;

use datafusion::logical_expr::Operator;
use serde::Deserialize;
use std::collections::BTreeMap;

/// A parsed, shape-valid recipe. Pack-compatibility is a separate step
/// ([`Recipe::resolve`]) because `--recipe` files parse before the pack
/// registry is consulted, and the two failure classes deserve distinct
/// diagnostics.
#[derive(Debug, Clone)]
pub struct Recipe {
    pub pack: String,
    pub format: TargetFormatKind,
    pub version: u32,
    /// Table-name → declaration, deterministic order.
    pub tables: BTreeMap<String, RecipeTable>,
}

#[derive(Debug, Clone)]
pub struct RecipeTable {
    /// `id` role: doc identity, must map a non-null pack column.
    pub id: String,
    /// `content` role: exactly one chunkable text column.
    pub content: String,
    pub title: Option<String>,
    pub author: Option<String>,
    /// Drives incremental detection when the pack pushes `GtEq` on it.
    pub timestamp: Option<String>,
    /// Extra columns folded into the metadata JSON object, in order.
    pub metadata: Vec<String>,
    pub incremental: IncrementalMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IncrementalMode {
    /// Derive from the pack: incremental iff the timestamp-role column has
    /// a `GtEq` filter mapping.
    #[default]
    Auto,
    /// Recipe-author override: full-load even when a pushdown exists (for
    /// pushdowns whose semantics cannot support accumulation).
    Full,
}

/// A recipe table resolved against the real pack: every role bound to its
/// `FieldMapping`, incremental mode decided. This is what the format layer
/// consumes — construction is proof of pack-compatibility.
#[derive(Debug, Clone)]
pub struct ResolvedTable {
    pub table: &'static SourcePackTable,
    /// The table's short name (the recipe key; `issues` for `github.issues`).
    pub short_name: String,
    pub id: &'static FieldMapping,
    pub content: &'static FieldMapping,
    pub title: Option<&'static FieldMapping>,
    pub author: Option<&'static FieldMapping>,
    pub timestamp: Option<&'static FieldMapping>,
    pub metadata: Vec<&'static FieldMapping>,
    /// `Some(provider_input_field)` when this table ingests incrementally:
    /// the pack declares `timestamp_column >= …` → that input. The format
    /// layer renders `WHERE <timestamp> >= {since}` and keeps `{limit}`.
    pub since_input: Option<&'static str>,
}

impl Recipe {
    /// Parse one recipe document (embedded asset or `--recipe` file).
    pub fn from_yaml(yaml: &str) -> Result<Self, String> {
        let raw: RawRecipe = serde_yaml::from_str(yaml).map_err(|e| e.to_string())?;
        raw.validate()
    }

    /// Bind every table declaration to the real pack, or fail naming the
    /// first incompatibility. `pack` must be the pack this recipe names —
    /// the caller resolves it from the registry (unknown-pack diagnostics
    /// live there).
    pub fn resolve(&self, pack: &'static SourcePack) -> Result<Vec<ResolvedTable>, String> {
        if pack.name != self.pack {
            return Err(format!(
                "recipe is for pack '{}' but was resolved against '{}'",
                self.pack, pack.name
            ));
        }
        let mut resolved = Vec::with_capacity(self.tables.len());
        for (short_name, decl) in &self.tables {
            resolved.push(resolve_table(pack, short_name, decl)?);
        }
        Ok(resolved)
    }
}

fn resolve_table(
    pack: &'static SourcePack,
    short_name: &str,
    decl: &RecipeTable,
) -> Result<ResolvedTable, String> {
    let table = pack
        .tables
        .iter()
        .find(|t| t.id.rsplit('.').next() == Some(short_name))
        .ok_or_else(|| {
            format!(
                "recipe table '{short_name}' does not exist in pack '{}' (tables: {})",
                pack.name,
                pack.tables
                    .iter()
                    .filter_map(|t| t.id.rsplit('.').next())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;

    let field = |role: &str, column: &str| -> Result<&'static FieldMapping, String> {
        table
            .fields
            .iter()
            .find(|f| f.name == column)
            .ok_or_else(|| {
                format!(
                    "{}: role '{role}' maps column '{column}', which the pack does not \
                     declare",
                    table.id
                )
            })
    };

    // id: doc identity — must be non-null in the pack's own contract.
    let id = field("id", &decl.id)?;
    if id.nullable {
        return Err(format!(
            "{}: role 'id' maps nullable column '{}'; doc identity requires a non-null \
             column",
            table.id, id.name
        ));
    }

    // content: chunkable text.
    let content = field("content", &decl.content)?;
    if !matches!(content.field_type, FieldType::Utf8) {
        return Err(format!(
            "{}: role 'content' maps column '{}' of type {:?}; content must be a plain \
             Utf8 text column",
            table.id, content.name, content.field_type
        ));
    }

    let utf8_role =
        |role: &str, column: &Option<String>| -> Result<Option<&'static FieldMapping>, String> {
            match column {
                None => Ok(None),
                Some(column) => {
                    let mapping = field(role, column)?;
                    if !matches!(mapping.field_type, FieldType::Utf8) {
                        return Err(format!(
                            "{}: role '{role}' maps column '{}' of type {:?}; expected Utf8",
                            table.id, mapping.name, mapping.field_type
                        ));
                    }
                    Ok(Some(mapping))
                }
            }
        };
    let title = utf8_role("title", &decl.title)?;
    let author = utf8_role("author", &decl.author)?;

    let timestamp = match &decl.timestamp {
        None => None,
        Some(column) => {
            let mapping = field("timestamp", column)?;
            if !matches!(
                mapping.field_type.arrow_type(),
                arrow::datatypes::DataType::Timestamp(_, _)
            ) {
                return Err(format!(
                    "{}: role 'timestamp' maps column '{}' of type {:?}; expected a \
                     timestamp column",
                    table.id, mapping.name, mapping.field_type
                ));
            }
            Some(mapping)
        }
    };

    let mut metadata = Vec::with_capacity(decl.metadata.len());
    for column in &decl.metadata {
        let mapping = field("metadata", column)?;
        // Everything json_pack can encode; nested Json text would
        // double-encode as a string, which is legal but surprising —
        // allowed, since the recipe author sees the pack contract.
        metadata.push(mapping);
    }

    // Incremental: auto = the timestamp-role column has a GtEq pushdown.
    let since_input = match (decl.incremental, timestamp) {
        (IncrementalMode::Full, _) | (IncrementalMode::Auto, None) => None,
        (IncrementalMode::Auto, Some(ts)) => table
            .filters
            .iter()
            .find(|f| f.column == ts.name && f.operator == Operator::GtEq)
            .map(|f| f.input_field),
    };

    Ok(ResolvedTable {
        table,
        short_name: short_name.to_string(),
        id,
        content,
        title,
        author,
        timestamp,
        metadata,
        since_input,
    })
}

// ─── Embedded assets ────────────────────────────────────────────────────

/// Every recipe shipped in this build, parse-validated. Deterministic
/// order (pack, then format).
pub fn embedded_recipes() -> Result<Vec<Recipe>, String> {
    EMBEDDED
        .iter()
        .map(|(asset, yaml)| {
            Recipe::from_yaml(yaml).map_err(|e| format!("embedded recipe {asset}: {e}"))
        })
        .collect()
}

/// The embedded recipe for `(pack, format)`, if this build ships one.
pub fn find_embedded(pack: &str, format: TargetFormatKind) -> Result<Option<Recipe>, String> {
    Ok(embedded_recipes()?
        .into_iter()
        .find(|r| r.pack == pack && r.format == format))
}

const EMBEDDED: &[(&str, &str)] = &[
    (
        "github.hybrid_search.yaml",
        include_str!("recipes/github.hybrid_search.yaml"),
    ),
    (
        "mock.hybrid_search.yaml",
        include_str!("recipes/mock.hybrid_search.yaml"),
    ),
];

// ─── Raw (serde-shaped) form ────────────────────────────────────────────

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRecipe {
    kind: String,
    metadata: RawMeta,
    spec: RawSpec,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMeta {
    pack: String,
    format: String,
    version: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSpec {
    tables: BTreeMap<String, RawTable>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTable {
    roles: RawRoles,
    #[serde(default)]
    metadata: Vec<String>,
    #[serde(default)]
    incremental: RawIncremental,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRoles {
    id: String,
    content: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    author: Option<String>,
    #[serde(default)]
    timestamp: Option<String>,
}

#[derive(Deserialize, Default, Clone, Copy)]
enum RawIncremental {
    #[default]
    #[serde(rename = "auto")]
    Auto,
    #[serde(rename = "full")]
    Full,
}

impl RawRecipe {
    fn validate(self) -> Result<Recipe, String> {
        if self.kind != "etl_recipe" {
            return Err(format!("kind must be 'etl_recipe', got '{}'", self.kind));
        }
        if self.metadata.pack.trim().is_empty() {
            return Err("metadata.pack must be non-empty".into());
        }
        let format = match self.metadata.format.as_str() {
            "hybrid_search" => TargetFormatKind::HybridSearch,
            "okf" => TargetFormatKind::Okf,
            other => {
                return Err(format!(
                    "metadata.format must be 'hybrid_search' or 'okf', got '{other}'"
                ));
            }
        };
        if self.spec.tables.is_empty() {
            return Err("spec.tables must declare at least one table".into());
        }

        let mut tables = BTreeMap::new();
        for (name, raw) in self.spec.tables {
            if name.trim().is_empty() {
                return Err("spec.tables keys must be non-empty table names".into());
            }
            // Two DIFFERENT roles sharing a column is a copy-paste slip
            // (the same value would drive two unrelated semantics), and a
            // metadata column listed twice would repeat a JSON key.
            // Metadata MAY repeat a role column on purpose — the flagship
            // does exactly that (`number` is the id role AND a metadata
            // key: one becomes doc identity, the other surfaces in the
            // metadata object).
            let mut roles_seen: Vec<String> = Vec::new();
            let mut claim_role = |role: &str, column: &str| -> Result<(), String> {
                if roles_seen.iter().any(|s| s == column) {
                    return Err(format!(
                        "table '{name}': column '{column}' is mapped by more than one \
                         role (role '{role}' repeats an earlier one)"
                    ));
                }
                roles_seen.push(column.to_string());
                Ok(())
            };
            claim_role("id", &raw.roles.id)?;
            claim_role("content", &raw.roles.content)?;
            if let Some(c) = &raw.roles.title {
                claim_role("title", c)?;
            }
            if let Some(c) = &raw.roles.author {
                claim_role("author", c)?;
            }
            if let Some(c) = &raw.roles.timestamp {
                claim_role("timestamp", c)?;
            }
            let mut metadata_seen: Vec<&str> = Vec::new();
            for c in &raw.metadata {
                if metadata_seen.contains(&c.as_str()) {
                    return Err(format!(
                        "table '{name}': metadata lists column '{c}' more than once"
                    ));
                }
                metadata_seen.push(c);
            }

            tables.insert(
                name,
                RecipeTable {
                    id: raw.roles.id,
                    content: raw.roles.content,
                    title: raw.roles.title,
                    author: raw.roles.author,
                    timestamp: raw.roles.timestamp,
                    metadata: raw.metadata,
                    incremental: match raw.incremental {
                        RawIncremental::Auto => IncrementalMode::Auto,
                        RawIncremental::Full => IncrementalMode::Full,
                    },
                },
            );
        }

        Ok(Recipe {
            pack: self.metadata.pack,
            format,
            version: self.metadata.version,
            tables,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::providers::open_connector::source_pack::SourcePackRegistry;

    /// THE contract suite (G2's tripwire): every embedded recipe resolves
    /// against the real pack registry, column-by-column — pack schema
    /// drift that invalidates a recipe fails the build.
    #[test]
    fn every_embedded_recipe_resolves_against_the_real_packs() {
        let registry = SourcePackRegistry::builtins().expect("packs parse");
        let recipes = embedded_recipes().expect("embedded recipes parse");
        assert!(!recipes.is_empty());
        for recipe in &recipes {
            let pack = registry
                .get(&recipe.pack)
                .unwrap_or_else(|| panic!("recipe pack '{}' is not built in", recipe.pack));
            let resolved = recipe
                .resolve(pack)
                .unwrap_or_else(|e| panic!("{}.{}: {e}", recipe.pack, recipe.format.as_str()));
            assert!(!resolved.is_empty());
        }
    }

    #[test]
    fn github_flagship_resolves_issues_with_the_real_pushdown() {
        let registry = SourcePackRegistry::builtins().unwrap();
        let recipe = find_embedded("github", TargetFormatKind::HybridSearch)
            .unwrap()
            .expect("flagship ships");
        let resolved = recipe.resolve(registry.get("github").unwrap()).unwrap();
        assert_eq!(resolved.len(), 1);
        let issues = &resolved[0];
        assert_eq!(issues.table.id, "github.issues");
        assert_eq!(issues.id.name, "number");
        assert_eq!(issues.content.name, "body");
        assert_eq!(issues.timestamp.unwrap().name, "updated_at");
        // incremental auto resolves to the pack's REAL GtEq pushdown.
        assert_eq!(issues.since_input, Some("since"));
        assert_eq!(
            issues.metadata.iter().map(|m| m.name).collect::<Vec<_>>(),
            vec!["number", "state"]
        );
    }

    #[test]
    fn mock_recipe_resolves_to_full_load_without_a_pushdown() {
        let registry = SourcePackRegistry::builtins().unwrap();
        let recipe = find_embedded("mock", TargetFormatKind::HybridSearch)
            .unwrap()
            .expect("mock recipe ships");
        let resolved = recipe.resolve(registry.get("mock").unwrap()).unwrap();
        let items = &resolved[0];
        // created_at has no GtEq mapping in the mock pack → full-load.
        assert!(items.timestamp.is_some());
        assert_eq!(items.since_input, None);
        // tags is List<Utf8> — json_pack's array path.
        assert!(items.metadata.iter().any(|m| m.name == "tags"));
    }

    #[test]
    fn incremental_full_overrides_a_real_pushdown() {
        let registry = SourcePackRegistry::builtins().unwrap();
        let yaml = include_str!("recipes/github.hybrid_search.yaml")
            .replace("incremental: auto", "incremental: full");
        let recipe = Recipe::from_yaml(&yaml).unwrap();
        let resolved = recipe.resolve(registry.get("github").unwrap()).unwrap();
        assert_eq!(
            resolved[0].since_input, None,
            "the recipe-author override wins over the pack's pushdown"
        );
    }

    #[test]
    fn resolution_rejects_pack_drift_with_targeted_errors() {
        let registry = SourcePackRegistry::builtins().unwrap();
        let github = registry.get("github").unwrap();
        let base = include_str!("recipes/github.hybrid_search.yaml");

        for (needle, replacement, expected) in [
            (
                "id: number",
                "id: html_url",
                "role 'id' maps nullable column 'html_url'",
            ),
            (
                "content: body",
                "content: comments",
                "content must be a plain Utf8 text column",
            ),
            (
                "timestamp: updated_at",
                "timestamp: state",
                "expected a timestamp column",
            ),
            (
                "metadata: [number, state]",
                "metadata: [number, milestone]",
                "'milestone', which the pack does not declare",
            ),
            (
                "    issues:",
                "    tickets:",
                "table 'tickets' does not exist in pack 'github'",
            ),
        ] {
            let yaml = base.replace(needle, replacement);
            assert_ne!(yaml, base, "mutation applies: {needle}");
            let recipe = Recipe::from_yaml(&yaml).expect("shape still parses");
            let err = recipe.resolve(github).expect_err(needle);
            assert!(err.contains(expected), "{needle}: {err}");
        }
    }

    #[test]
    fn shape_violations_fail_at_parse_with_targeted_errors() {
        let base = include_str!("recipes/github.hybrid_search.yaml");
        for (needle, replacement, expected) in [
            (
                "kind: etl_recipe",
                "kind: recipe",
                "kind must be 'etl_recipe'",
            ),
            (
                "format: hybrid_search",
                "format: page_index",
                "metadata.format must be",
            ),
            (
                "incremental: auto",
                "incremental: sometimes",
                "unknown variant",
            ),
            (
                "        author: author_login",
                "        author: author_login\n        reviewer: author_login",
                "unknown field",
            ),
            (
                "metadata: [number, state]",
                "metadata: [number, state, state]",
                "metadata lists column 'state' more than once",
            ),
            (
                "        title: title",
                "        title: body",
                "mapped by more than one role",
            ),
        ] {
            let yaml = base.replace(needle, replacement);
            assert_ne!(yaml, base, "mutation applies: {needle}");
            let err = Recipe::from_yaml(&yaml).expect_err(needle);
            assert!(err.contains(expected), "{needle}: {err}");
        }
    }
}
