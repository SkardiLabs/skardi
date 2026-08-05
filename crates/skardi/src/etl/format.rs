//! Engine-neutral target-format plans.
//!
//! A [`TargetFormatKind`] plus a resolved recipe becomes a plan: the
//! destination table's column set (exact order — the positional-INSERT
//! invariant starts here), one ingest plan per table, and the pipeline
//! pair. Everything engine-specific (DDL spellings, UDTF names, mirror
//! tables, embedding expressions) is the dialect layer's job; plans carry
//! only what every engine shares.
//!
//! v1 implements `hybrid_search`; `okf` is milestone 2.

use super::config::{ChunkingSpec, EtlConfig, TargetFormatKind};
use super::recipe::ResolvedTable;

/// The neutral type vocabulary destination columns use — deliberately the
/// design's set, not Arrow's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocColumnKind {
    Utf8,
    Int32,
    /// Timestamp(ms, UTC) — matches the pack timestamp field types.
    TimestampMs,
    /// JSON text (Utf8 on the wire; engines may refine).
    Json,
    /// The embedding vector; each dialect owns its physical spelling
    /// (BLOB on SQLite, `vector(N)` on Postgres, …).
    Vector,
}

/// One destination column: name, neutral type, nullability.
#[derive(Debug, Clone, Copy)]
pub struct DocColumn {
    pub name: &'static str,
    pub kind: DocColumnKind,
    pub nullable: bool,
}

/// The `documents` table, in DDL order (design §Destination schema).
/// `doc_id` is deliberately NOT a primary key: jobs are append-only (no
/// ON CONFLICT path exists in the executor), so a unique constraint would
/// hard-fail `{since}` replay mid-stream — punishing overlap, the SAFE
/// watermark direction — while no-PK replay merely accumulates rows the
/// search pipeline deduplicates at read time. The PRIMARY KEY arrives
/// WITH upsert in v2.
pub const DOCUMENT_COLUMNS: &[DocColumn] = &[
    DocColumn {
        name: "doc_id",
        kind: DocColumnKind::Utf8,
        nullable: false,
    },
    DocColumn {
        name: "source_table",
        kind: DocColumnKind::Utf8,
        nullable: false,
    },
    DocColumn {
        name: "source_id",
        kind: DocColumnKind::Utf8,
        nullable: false,
    },
    DocColumn {
        name: "title",
        kind: DocColumnKind::Utf8,
        nullable: true,
    },
    DocColumn {
        name: "content",
        kind: DocColumnKind::Utf8,
        nullable: false,
    },
    DocColumn {
        name: "chunk_index",
        kind: DocColumnKind::Int32,
        nullable: false,
    },
    DocColumn {
        name: "author",
        kind: DocColumnKind::Utf8,
        nullable: true,
    },
    DocColumn {
        name: "created_at",
        kind: DocColumnKind::TimestampMs,
        nullable: true,
    },
    DocColumn {
        name: "metadata",
        kind: DocColumnKind::Json,
        nullable: true,
    },
    DocColumn {
        name: "embedding",
        kind: DocColumnKind::Vector,
        nullable: false,
    },
];

/// The hybrid-search plan for one config: what the bundle will contain,
/// before any dialect rendering.
#[derive(Debug, Clone)]
pub struct HybridPlan {
    /// The ctx data-source name the destination registers under.
    pub dest_catalog: String,
    /// One per selected recipe table.
    pub ingests: Vec<IngestPlan>,
    pub search: SearchPlan,
    pub get_document: GetDocumentPlan,
}

/// One table's ingest: everything a dialect needs to render the
/// document-shaped SELECT, owned strings so the plan outlives the recipe.
#[derive(Debug, Clone)]
pub struct IngestPlan {
    /// The recipe table's short name — the `source_table` column value and
    /// the `doc_id` prefix.
    pub source_table: String,
    /// Fully-qualified source (`saas.github_demo.issues`).
    pub source_from: String,
    /// Column names, from the resolved roles.
    pub id_column: String,
    pub content_column: String,
    pub title_column: Option<String>,
    pub author_column: Option<String>,
    pub timestamp_column: Option<String>,
    /// `(json key == pack column name)` pairs, in recipe order.
    pub metadata_columns: Vec<String>,
    pub chunking: ChunkingSpec,
    /// `Some` ⇒ incremental: the inner SELECT gains
    /// `WHERE <timestamp_column> >= {since}` (reaching the SaaS API as the
    /// pack's pushdown) and KEEPS `{limit}` as the first-backfill bound.
    pub incremental: bool,
}

impl IngestPlan {
    /// The pack columns the inner SELECT needs (order-preserving dedup).
    /// `metadata` may deliberately repeat a role column; the inner SELECT
    /// lists it once.
    pub fn inner_columns(&self) -> Vec<&str> {
        let mut columns: Vec<&str> = Vec::new();
        let candidates = [
            Some(self.id_column.as_str()),
            Some(self.content_column.as_str()),
            self.title_column.as_deref(),
            self.author_column.as_deref(),
            self.timestamp_column.as_deref(),
        ]
        .into_iter()
        .flatten()
        .chain(self.metadata_columns.iter().map(String::as_str));
        for column in candidates {
            if !columns.contains(&column) {
                columns.push(column);
            }
        }
        columns
    }
}

/// The RRF search pipeline's fixed contract (FR-9).
#[derive(Debug, Clone)]
pub struct SearchPlan {
    /// Candidate depth for the vector arm.
    pub knn_candidates: u32,
    /// Candidate depth for the text arm.
    pub fts_candidates: u32,
    /// Embedding model (the query vector must use the same model the
    /// ingest embedded with).
    pub embedding_model: String,
}

/// `get-document`: one document's ordered chunks by
/// `(source_table, source_id)`.
#[derive(Debug, Clone)]
pub struct GetDocumentPlan;

/// Build the hybrid plan. `tables` is the resolved recipe subset the
/// config selected (config.tables ∩ recipe, already resolved).
pub fn hybrid_plan(config: &EtlConfig, tables: &[ResolvedTable]) -> Result<HybridPlan, String> {
    debug_assert_eq!(config.format, TargetFormatKind::HybridSearch);
    let chunking = config
        .chunking
        .clone()
        .ok_or("hybrid_search requires chunking (validated at config load)")?;
    let embedding = config
        .embedding
        .clone()
        .ok_or("hybrid_search requires embedding (validated at config load)")?;
    if tables.is_empty() {
        return Err(
            "no tables selected: the config's table subset matched nothing in the \
                    recipe"
                .to_string(),
        );
    }

    let ingests = tables
        .iter()
        .map(|t| IngestPlan {
            source_table: t.short_name.clone(),
            source_from: format!("{}.{}", config.source.binding(), t.short_name),
            id_column: t.id.name.to_string(),
            content_column: t.content.name.to_string(),
            title_column: t.title.map(|f| f.name.to_string()),
            author_column: t.author.map(|f| f.name.to_string()),
            timestamp_column: t.timestamp.map(|f| f.name.to_string()),
            metadata_columns: t.metadata.iter().map(|f| f.name.to_string()).collect(),
            chunking: chunking.clone(),
            incremental: t.since_input.is_some(),
        })
        .collect();

    Ok(HybridPlan {
        dest_catalog: config.destination.catalog.clone(),
        ingests,
        search: SearchPlan {
            knn_candidates: 80,
            fts_candidates: 60,
            embedding_model: embedding.model.clone(),
        },
        get_document: GetDocumentPlan,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::etl::recipe::find_embedded;
    use crate::sources::providers::open_connector::source_pack::SourcePackRegistry;

    fn flagship_config() -> EtlConfig {
        EtlConfig::from_yaml(
            r#"
kind: etl
metadata:
  name: github-issues-search
spec:
  source: { pack: github, binding: saas.github_demo, tables: [issues] }
  format: hybrid_search
  destination: { type: sqlite, path: data/gh.db, catalog: gh_search }
  embedding: { udf: candle, model: models/generated/bge-small-en-v1.5, dimensions: 384 }
  chunking: { splitter: markdown, size: 1200, overlap: 200 }
"#,
        )
        .unwrap()
    }

    #[test]
    fn flagship_plan_carries_roles_pushdown_and_source_qualification() {
        let registry = SourcePackRegistry::builtins().unwrap();
        let recipe = find_embedded("github", TargetFormatKind::HybridSearch)
            .unwrap()
            .unwrap();
        let resolved = recipe.resolve(registry.get("github").unwrap()).unwrap();
        let plan = hybrid_plan(&flagship_config(), &resolved).unwrap();

        assert_eq!(plan.dest_catalog, "gh_search");
        assert_eq!(plan.ingests.len(), 1);
        let ingest = &plan.ingests[0];
        assert_eq!(ingest.source_from, "saas.github_demo.issues");
        assert_eq!(ingest.id_column, "number");
        assert!(ingest.incremental, "issues rides the real GtEq pushdown");
        // number appears once in the inner SELECT even though it is both
        // the id role and a metadata key.
        assert_eq!(
            ingest.inner_columns(),
            vec![
                "number",
                "body",
                "title",
                "author_login",
                "updated_at",
                "state"
            ]
        );
    }

    #[test]
    fn document_columns_are_the_designs_ten_in_order() {
        let names: Vec<&str> = DOCUMENT_COLUMNS.iter().map(|c| c.name).collect();
        assert_eq!(
            names,
            vec![
                "doc_id",
                "source_table",
                "source_id",
                "title",
                "content",
                "chunk_index",
                "author",
                "created_at",
                "metadata",
                "embedding"
            ]
        );
    }
}
