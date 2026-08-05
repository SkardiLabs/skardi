//! The `kind: etl` generation config — parsing and cross-field validation.
//!
//! Strict everywhere (`deny_unknown_fields`): a misspelled key fails
//! parsing instead of silently disabling what it was meant to set — the
//! Open Connector config/pack-loader discipline, applied to this crate's
//! own surface (it is deliberately NOT a repo-wide convention; see the
//! design's Research Findings).
//!
//! Parsing yields a [`RawEtlConfig`] shaped exactly like the YAML;
//! [`EtlConfig::from_yaml`] then runs the cross-field rules the design's
//! §Generation Config declares and produces the validated form the rest
//! of the generator consumes. Every rejection names the field and the
//! rule, never a generic "invalid config".

use serde::Deserialize;

/// A validated generation config. Constructing one is only possible
/// through [`EtlConfig::from_yaml`], so downstream code can rely on the
/// cross-field invariants (e.g. hybrid configs always carry embedding +
/// chunking) without re-checking.
#[derive(Debug, Clone)]
pub struct EtlConfig {
    /// `metadata.name` — the slug source for every artifact name.
    pub name: String,
    pub source: SourceSpec,
    pub format: TargetFormatKind,
    pub destination: DestinationSpec,
    /// Present iff `format == HybridSearch` (validated).
    pub embedding: Option<EmbeddingSpec>,
    /// Present iff `format == HybridSearch` (validated).
    pub chunking: Option<ChunkingSpec>,
}

#[derive(Debug, Clone)]
pub struct SourceSpec {
    /// Source pack name (`github`, `mock`, …) — existence in the registry
    /// is checked at generation time, not parse time (the registry is a
    /// runtime dependency, and the recipe loader owns that diagnostic).
    pub pack: String,
    /// The Open Connector binding's catalog (SQL catalog name).
    pub binding_catalog: String,
    /// The binding's schema (the binding name inside the gateway catalog).
    pub binding_schema: String,
    /// Optional table subset; empty = the recipe's full table set.
    pub tables: Vec<String>,
}

impl SourceSpec {
    /// The `catalog.schema` prefix generated SQL reads from.
    pub fn binding(&self) -> String {
        format!("{}.{}", self.binding_catalog, self.binding_schema)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetFormatKind {
    HybridSearch,
    Okf,
}

impl TargetFormatKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::HybridSearch => "hybrid_search",
            Self::Okf => "okf",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineKind {
    Sqlite,
    Postgres,
    Lance,
    Mysql,
}

impl EngineKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
            Self::Lance => "lance",
            Self::Mysql => "mysql",
        }
    }
}

#[derive(Debug, Clone)]
pub struct DestinationSpec {
    pub engine: EngineKind,
    /// File-path locator (sqlite / lance). Validated present for those
    /// engines and absent for postgres (whose locator is host/port/db).
    pub path: Option<String>,
    /// The ctx data-source name the destination registers under.
    pub catalog: String,
    /// Postgres connection fields — env var NAMES for credentials, never
    /// values (the no-credentials-in-config rule).
    pub postgres: Option<PostgresFields>,
    /// SQLite extension loading (env var naming the sqlite-vec path).
    pub sqlite: Option<SqliteFields>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresFields {
    pub host: String,
    pub port: u16,
    pub database: String,
    /// Env var NAME holding the user.
    pub user_env: String,
    /// Env var NAME holding the password.
    pub pass_env: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqliteFields {
    /// Env var NAME pointing at the sqlite-vec extension.
    pub extensions_env: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddingUdf {
    Candle,
    RemoteEmbed,
}

#[derive(Debug, Clone)]
pub struct EmbeddingSpec {
    pub udf: EmbeddingUdf,
    pub model: String,
    /// Required for `remote_embed`, rejected for `candle` (validated).
    pub provider: Option<String>,
    /// Sized into the vector DDL. Declared, not verified against the
    /// model — the README's first-contact checklist and the deferred
    /// `skardi-etl verify` own that gap (design §Validation).
    pub dimensions: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkSplitter {
    Character,
    Markdown,
}

impl ChunkSplitter {
    /// The `chunk_parts` mode literal this splitter renders as.
    pub fn mode(&self) -> &'static str {
        match self {
            Self::Character => "character",
            Self::Markdown => "markdown",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChunkingSpec {
    pub splitter: ChunkSplitter,
    pub size: u32,
    pub overlap: u32,
}

impl EtlConfig {
    /// Parse and validate one `kind: etl` document.
    ///
    /// # Errors
    /// A message naming the offending field and the violated rule — parse
    /// errors carry serde's location, cross-field errors the field path.
    pub fn from_yaml(yaml: &str) -> Result<Self, String> {
        let raw: RawEtlConfig = serde_yaml::from_str(yaml).map_err(|e| e.to_string())?;
        raw.validate()
    }
}

// ─── Raw (serde-shaped) form ────────────────────────────────────────────

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawEtlConfig {
    kind: String,
    metadata: RawMetadata,
    spec: RawSpec,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMetadata {
    name: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSpec {
    source: RawSource,
    format: String,
    destination: RawDestination,
    #[serde(default)]
    embedding: Option<RawEmbedding>,
    #[serde(default)]
    chunking: Option<RawChunking>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSource {
    pack: String,
    binding: String,
    #[serde(default)]
    tables: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDestination {
    #[serde(rename = "type")]
    engine: String,
    #[serde(default)]
    path: Option<String>,
    catalog: String,
    #[serde(default)]
    postgres: Option<PostgresFields>,
    #[serde(default)]
    sqlite: Option<SqliteFields>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawEmbedding {
    udf: String,
    model: String,
    #[serde(default)]
    provider: Option<String>,
    dimensions: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawChunking {
    splitter: String,
    size: u32,
    #[serde(default)]
    overlap: u32,
}

impl RawEtlConfig {
    fn validate(self) -> Result<EtlConfig, String> {
        if self.kind != "etl" {
            return Err(format!("kind must be 'etl', got '{}'", self.kind));
        }
        let name = self.metadata.name.trim();
        if name.is_empty() {
            return Err("metadata.name must be non-empty (it seeds every artifact name)".into());
        }

        // source.binding is `catalog.schema` — exactly one dot, both non-empty.
        let source = {
            let raw = self.spec.source;
            if raw.pack.trim().is_empty() {
                return Err("spec.source.pack must be non-empty".into());
            }
            let mut parts = raw.binding.split('.');
            let (catalog, schema) = match (parts.next(), parts.next(), parts.next()) {
                (Some(c), Some(s), None) if !c.is_empty() && !s.is_empty() => (c, s),
                _ => {
                    return Err(format!(
                        "spec.source.binding must be 'catalog.schema' (an existing Open \
                         Connector binding), got '{}'",
                        raw.binding
                    ));
                }
            };
            for table in &raw.tables {
                if table.trim().is_empty() {
                    return Err("spec.source.tables entries must be non-empty".into());
                }
            }
            SourceSpec {
                pack: raw.pack,
                binding_catalog: catalog.to_string(),
                binding_schema: schema.to_string(),
                tables: raw.tables,
            }
        };

        let format = match self.spec.format.as_str() {
            "hybrid_search" => TargetFormatKind::HybridSearch,
            "okf" => TargetFormatKind::Okf,
            other => {
                return Err(format!(
                    "spec.format must be 'hybrid_search' or 'okf', got '{other}'"
                ));
            }
        };

        let destination = {
            let raw = self.spec.destination;
            let engine = match raw.engine.as_str() {
                "sqlite" => EngineKind::Sqlite,
                "postgres" => EngineKind::Postgres,
                "lance" => EngineKind::Lance,
                "mysql" => EngineKind::Mysql,
                other => {
                    return Err(format!(
                        "spec.destination.type must be one of sqlite | postgres | lance | \
                         mysql, got '{other}'"
                    ));
                }
            };
            if raw.catalog.trim().is_empty() {
                return Err("spec.destination.catalog must be non-empty".into());
            }
            match engine {
                EngineKind::Sqlite | EngineKind::Lance => {
                    if raw.path.as_deref().is_none_or(|p| p.trim().is_empty()) {
                        return Err(format!(
                            "spec.destination.path is required for {}",
                            engine.as_str()
                        ));
                    }
                    if raw.postgres.is_some() {
                        return Err(format!(
                            "spec.destination.postgres does not apply to {}",
                            engine.as_str()
                        ));
                    }
                }
                EngineKind::Postgres => {
                    if raw.postgres.is_none() {
                        return Err(
                            "spec.destination.postgres ({host, port, database, user_env, \
                             pass_env}) is required for postgres"
                                .into(),
                        );
                    }
                    if raw.path.is_some() {
                        return Err(
                            "spec.destination.path does not apply to postgres (its locator \
                             is host/port/database; credentials via user_env/pass_env)"
                                .into(),
                        );
                    }
                    if raw.sqlite.is_some() {
                        return Err("spec.destination.sqlite does not apply to postgres".into());
                    }
                }
                EngineKind::Mysql => {
                    // MySQL is OKF-only and its dialect lands in M2; the
                    // config type accepts it so the capability refusal
                    // (FR-5, at generation) owns the diagnostic — a parse
                    // error here would hide the more useful matrix answer.
                }
            }
            if let Some(pg) = &raw.postgres {
                for (field, value) in [("user_env", &pg.user_env), ("pass_env", &pg.pass_env)] {
                    if value.trim().is_empty() {
                        return Err(format!(
                            "spec.destination.postgres.{field} must name an environment \
                             variable (credential values never appear in this config)"
                        ));
                    }
                }
            }
            DestinationSpec {
                engine,
                path: raw.path,
                catalog: raw.catalog,
                postgres: raw.postgres,
                sqlite: raw.sqlite,
            }
        };

        // embedding + chunking: required for hybrid_search, rejected for okf.
        let (embedding, chunking) = match format {
            TargetFormatKind::HybridSearch => {
                let raw_e = self
                    .spec
                    .embedding
                    .ok_or("spec.embedding is required for format: hybrid_search")?;
                let udf = match raw_e.udf.as_str() {
                    "candle" => EmbeddingUdf::Candle,
                    "remote_embed" => EmbeddingUdf::RemoteEmbed,
                    other => {
                        return Err(format!(
                            "spec.embedding.udf must be 'candle' or 'remote_embed', got \
                             '{other}'"
                        ));
                    }
                };
                match (udf, &raw_e.provider) {
                    (EmbeddingUdf::RemoteEmbed, None) => {
                        return Err(
                            "spec.embedding.provider is required for udf: remote_embed".into()
                        );
                    }
                    (EmbeddingUdf::Candle, Some(_)) => {
                        return Err("spec.embedding.provider does not apply to udf: candle".into());
                    }
                    _ => {}
                }
                if raw_e.model.trim().is_empty() {
                    return Err("spec.embedding.model must be non-empty".into());
                }
                if raw_e.dimensions == 0 {
                    return Err(
                        "spec.embedding.dimensions must be > 0 (it sizes the vector \
                                DDL; its truth against the model is the deferred verify \
                                command's job)"
                            .into(),
                    );
                }
                let raw_c = self
                    .spec
                    .chunking
                    .ok_or("spec.chunking is required for format: hybrid_search")?;
                let splitter = match raw_c.splitter.as_str() {
                    "character" => ChunkSplitter::Character,
                    "markdown" => ChunkSplitter::Markdown,
                    other => {
                        return Err(format!(
                            "spec.chunking.splitter must be 'character' or 'markdown', got \
                             '{other}'"
                        ));
                    }
                };
                if raw_c.size == 0 {
                    return Err("spec.chunking.size must be > 0".into());
                }
                if raw_c.overlap >= raw_c.size {
                    return Err(format!(
                        "spec.chunking.overlap ({}) must be strictly less than size ({})",
                        raw_c.overlap, raw_c.size
                    ));
                }
                (
                    Some(EmbeddingSpec {
                        udf,
                        model: raw_e.model,
                        provider: raw_e.provider,
                        dimensions: raw_e.dimensions,
                    }),
                    Some(ChunkingSpec {
                        splitter,
                        size: raw_c.size,
                        overlap: raw_c.overlap,
                    }),
                )
            }
            TargetFormatKind::Okf => {
                if self.spec.embedding.is_some() {
                    return Err("spec.embedding does not apply to format: okf".into());
                }
                if self.spec.chunking.is_some() {
                    return Err("spec.chunking does not apply to format: okf".into());
                }
                (None, None)
            }
        };

        Ok(EtlConfig {
            name: name.to_string(),
            source,
            format,
            destination,
            embedding,
            chunking,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The design's normative §6.1 example, verbatim shape.
    const FLAGSHIP: &str = r#"
kind: etl
metadata:
  name: github-issues-search
spec:
  source:
    pack: github
    binding: saas.github_demo
    tables: [issues]
  format: hybrid_search
  destination:
    type: sqlite
    path: data/gh_search.db
    catalog: gh_search
    sqlite:
      extensions_env: SQLITE_VEC_PATH
  embedding:
    udf: candle
    model: models/generated/bge-small-en-v1.5
    dimensions: 384
  chunking:
    splitter: markdown
    size: 1200
    overlap: 200
"#;

    #[test]
    fn the_normative_example_parses_and_validates() {
        let config = EtlConfig::from_yaml(FLAGSHIP).expect("normative example is valid");
        assert_eq!(config.name, "github-issues-search");
        assert_eq!(config.source.pack, "github");
        assert_eq!(config.source.binding(), "saas.github_demo");
        assert_eq!(config.source.tables, vec!["issues"]);
        assert_eq!(config.format, TargetFormatKind::HybridSearch);
        assert_eq!(config.destination.engine, EngineKind::Sqlite);
        assert_eq!(config.destination.catalog, "gh_search");
        let embedding = config.embedding.as_ref().expect("hybrid carries embedding");
        assert_eq!(embedding.udf, EmbeddingUdf::Candle);
        assert_eq!(embedding.dimensions, 384);
        let chunking = config.chunking.as_ref().expect("hybrid carries chunking");
        assert_eq!(chunking.splitter.mode(), "markdown");
        assert_eq!((chunking.size, chunking.overlap), (1200, 200));
    }

    /// Every rejection rule, table-driven: (mutation of the flagship, the
    /// error fragment the message must carry).
    #[test]
    fn cross_field_rules_reject_with_targeted_errors() {
        let cases: &[(&str, &str, &str)] = &[
            // (needle to replace, replacement, expected error fragment)
            ("kind: etl", "kind: pipeline", "kind must be 'etl'"),
            (
                "name: github-issues-search",
                "name: ''",
                "metadata.name must be non-empty",
            ),
            (
                "binding: saas.github_demo",
                "binding: no_dot_here",
                "must be 'catalog.schema'",
            ),
            (
                "binding: saas.github_demo",
                "binding: a.b.c",
                "must be 'catalog.schema'",
            ),
            (
                "format: hybrid_search",
                "format: page_index",
                "spec.format must be",
            ),
            (
                "type: sqlite",
                "type: seekdb",
                "spec.destination.type must be",
            ),
            (
                "path: data/gh_search.db",
                "path: ''",
                "path is required for sqlite",
            ),
            (
                "catalog: gh_search",
                "catalog: ''",
                "catalog must be non-empty",
            ),
            ("udf: candle", "udf: openai", "spec.embedding.udf must be"),
            ("dimensions: 384", "dimensions: 0", "dimensions must be > 0"),
            (
                "splitter: markdown",
                "splitter: sentence",
                "spec.chunking.splitter",
            ),
            ("size: 1200", "size: 0", "spec.chunking.size must be > 0"),
            (
                "overlap: 200",
                "overlap: 1200",
                "must be strictly less than size",
            ),
        ];
        for (needle, replacement, expected) in cases {
            let mutated = FLAGSHIP.replace(needle, replacement);
            assert_ne!(mutated, FLAGSHIP, "mutation must apply: {needle}");
            let err = EtlConfig::from_yaml(&mutated).expect_err(needle);
            assert!(
                err.contains(expected),
                "{needle} -> {replacement}: expected '{expected}' in: {err}"
            );
        }
    }

    #[test]
    fn unknown_keys_fail_loudly_at_every_level() {
        for (needle, replacement) in [
            ("kind: etl", "kind: etl\nextra_top: 1"),
            (
                "  name: github-issues-search",
                "  name: github-issues-search\n  namespace: x",
            ),
            (
                "    pack: github",
                "    pack: github\n    packs: also-github",
            ),
            (
                "    catalog: gh_search",
                "    catalog: gh_search\n    catalogue: typo",
            ),
            (
                "    dimensions: 384",
                "    dimensions: 384\n    dimension: 384",
            ),
        ] {
            let mutated = FLAGSHIP.replace(needle, replacement);
            assert_ne!(mutated, FLAGSHIP);
            let err = EtlConfig::from_yaml(&mutated).expect_err(replacement);
            assert!(
                err.contains("unknown field"),
                "{replacement}: expected unknown-field error, got: {err}"
            );
        }
    }

    #[test]
    fn remote_embed_requires_provider_and_candle_rejects_it() {
        let remote = FLAGSHIP.replace(
            "udf: candle\n    model: models/generated/bge-small-en-v1.5",
            "udf: remote_embed\n    model: text-embedding-3-small",
        );
        let err = EtlConfig::from_yaml(&remote).expect_err("remote_embed without provider");
        assert!(
            err.contains("provider is required for udf: remote_embed"),
            "{err}"
        );

        let with_provider = remote.replace(
            "udf: remote_embed",
            "udf: remote_embed\n    provider: openai",
        );
        let config = EtlConfig::from_yaml(&with_provider).expect("provider satisfies the rule");
        assert_eq!(
            config.embedding.unwrap().provider.as_deref(),
            Some("openai")
        );

        let candle_with_provider =
            FLAGSHIP.replace("udf: candle", "udf: candle\n    provider: openai");
        let err = EtlConfig::from_yaml(&candle_with_provider).expect_err("candle + provider");
        assert!(err.contains("does not apply to udf: candle"), "{err}");
    }

    #[test]
    fn okf_rejects_embedding_and_chunking_and_accepts_their_absence() {
        let okf_with_embedding = FLAGSHIP.replace("format: hybrid_search", "format: okf");
        let err = EtlConfig::from_yaml(&okf_with_embedding).expect_err("okf + embedding");
        assert!(err.contains("does not apply to format: okf"), "{err}");

        let okf = r#"
kind: etl
metadata:
  name: github-issues-okf
spec:
  source:
    pack: github
    binding: saas.github_demo
  format: okf
  destination:
    type: sqlite
    path: data/okf.db
    catalog: gh_okf
"#;
        let config = EtlConfig::from_yaml(okf).expect("okf without embedding/chunking");
        assert_eq!(config.format, TargetFormatKind::Okf);
        assert!(config.embedding.is_none() && config.chunking.is_none());
        assert!(
            config.source.tables.is_empty(),
            "tables default to recipe's"
        );
    }

    #[test]
    fn hybrid_requires_embedding_and_chunking() {
        let no_embedding = FLAGSHIP.replace(
            "  embedding:\n    udf: candle\n    model: models/generated/bge-small-en-v1.5\n    dimensions: 384\n",
            "",
        );
        let err = EtlConfig::from_yaml(&no_embedding).expect_err("hybrid without embedding");
        assert!(err.contains("embedding is required"), "{err}");
    }

    #[test]
    fn postgres_requires_env_named_credentials_and_no_path() {
        let pg = r#"
kind: etl
metadata:
  name: gh-pg
spec:
  source:
    pack: github
    binding: saas.github_demo
  format: okf
  destination:
    type: postgres
    catalog: gh_okf
    postgres:
      host: db.internal
      port: 5432
      database: search
      user_env: PG_USER
      pass_env: PG_PASS
"#;
        let config = EtlConfig::from_yaml(pg).expect("valid postgres destination");
        assert_eq!(config.destination.engine, EngineKind::Postgres);

        let missing_block = pg.replace(
            "    postgres:\n      host: db.internal\n      port: 5432\n      database: search\n      user_env: PG_USER\n      pass_env: PG_PASS\n",
            "",
        );
        let err = EtlConfig::from_yaml(&missing_block).expect_err("postgres without fields");
        assert!(err.contains("postgres ({host, port, database"), "{err}");

        let with_path = pg.replace(
            "    catalog: gh_okf",
            "    catalog: gh_okf\n    path: nope.db",
        );
        let err = EtlConfig::from_yaml(&with_path).expect_err("postgres + path");
        assert!(err.contains("path does not apply to postgres"), "{err}");

        let empty_env = pg.replace("pass_env: PG_PASS", "pass_env: ''");
        let err = EtlConfig::from_yaml(&empty_env).expect_err("empty env name");
        assert!(
            err.contains("pass_env must name an environment variable"),
            "{err}"
        );
    }

    #[test]
    fn overlap_defaults_to_zero() {
        let no_overlap = FLAGSHIP.replace("    overlap: 200\n", "");
        let config = EtlConfig::from_yaml(&no_overlap).expect("overlap optional");
        assert_eq!(config.chunking.unwrap().overlap, 0);
    }
}
