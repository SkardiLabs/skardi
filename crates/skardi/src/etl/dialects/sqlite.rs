//! The SQLite dialect: fts5 + sqlite-vec (vec0) mirrors with sync
//! triggers, rowid-keyed.
//!
//! Two SQLite realities shape everything here (design §Generated search
//! pipeline):
//!
//! - **vec0 requires an INTEGER key**, and the neutral `documents` plan
//!   deliberately has no integer column (no-PK `doc_id` is Utf8). The
//!   dialect therefore PREPENDS `rid INTEGER PRIMARY KEY` — SQLite's
//!   rowid alias — to the physical table: the ingest SELECT emits a
//!   leading `CAST(NULL AS BIGINT) AS rid` (SQLite auto-assigns on NULL),
//!   sync triggers copy `NEW.rid` into both mirrors, ranked results join
//!   back on `d.rid`, and duplicate `doc_id`s under `{since}` replay are
//!   naturally fine — each copy gets a distinct rid while the read-time
//!   dedup still collapses by `doc_id`. `rid` is exposed because skardi's
//!   SQLite provider derives schemas from `PRAGMA table_info` — a bare
//!   (undeclared) rowid would be invisible to DataFusion.
//! - **`sqlite_knn` returns all non-vector columns + `_score`** — for the
//!   vec0 mirror that is `doc_rowid` + `_score`, exactly what the
//!   join-back needs. The query vector is a bare `candle()` scalar
//!   subquery: NO `vec_to_binary` on the read path (the UDTF packs the
//!   `List<Float32>` itself); packing is write-side only.
//!
//! Jobs are append-only and the refresh model is rebuild-first, so the
//! mirrors carry INSERT triggers only — there is nothing to update or
//! delete outside `--reset`, which drops every bundle-owned artifact.

use super::super::config::EtlConfig;
use super::super::dialect::{Capabilities, EngineDialect};
use super::super::format::{DOCUMENT_COLUMNS, DocColumnKind, HybridPlan, IngestPlan};
use tokio_rusqlite::rusqlite;

pub struct SqliteDialect;

impl SqliteDialect {
    fn embedding_dimensions(config: &EtlConfig) -> u32 {
        config
            .embedding
            .as_ref()
            .map(|e| e.dimensions)
            .unwrap_or_default()
    }

    fn embedding_model(config: &EtlConfig) -> String {
        config
            .embedding
            .as_ref()
            .map(|e| e.model.clone())
            .unwrap_or_default()
    }

    /// `documents` qualified for generated pipeline SQL. The SQLite
    /// provider registers the file under `<catalog>.main`.
    fn qualified(config: &EtlConfig, table: &str) -> String {
        format!("{}.main.{}", config.destination.catalog, table)
    }
}

impl EngineDialect for SqliteDialect {
    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            fts: true,
            knn: true,
            okf_table: true,
            needs_setup: true,
        }
    }

    fn setup_sql(&self, _plan: &HybridPlan, config: &EtlConfig) -> String {
        setup_statements(Self::embedding_dimensions(config))
            .iter()
            .map(|s| s.sql.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn reset_sql(&self, _plan: &HybridPlan, _config: &EtlConfig) -> String {
        // Every bundle-owned artifact, nothing else. Triggers drop with
        // their table but are listed explicitly so the reset is readable
        // as a complete inventory.
        "DROP TRIGGER IF EXISTS documents_ai_fts;\n\
         DROP TRIGGER IF EXISTS documents_ai_vec;\n\
         DROP TABLE IF EXISTS documents_fts;\n\
         DROP TABLE IF EXISTS documents_vec;\n\
         DROP TABLE IF EXISTS documents;\n"
            .to_string()
    }

    fn validate_ddl(&self, plan: &HybridPlan, config: &EtlConfig) -> Result<Vec<String>, String> {
        let mut warnings = Vec::new();
        let conn = rusqlite::Connection::open_in_memory()
            .map_err(|e| format!("open a throwaway in-memory sqlite connection: {e}"))?;

        // vec0 needs the sqlite-vec extension. The config names the env
        // var holding its path (never the path itself); when the var is
        // unset at generate time, vec0 statements degrade to shape-only —
        // reported, never silent.
        let ext_env = config
            .destination
            .sqlite
            .as_ref()
            .and_then(|s| s.extensions_env.as_deref());
        let ext_path = ext_env
            .and_then(|env| std::env::var(env).ok())
            .filter(|p| !p.trim().is_empty());
        let vec_available = match (&ext_env, &ext_path) {
            (Some(env), Some(path)) => {
                // SAFETY: mirrors the sqlite provider's init_connection —
                // loading is enabled only around the user-designated path,
                // then disabled again.
                let loaded = (|| -> Result<(), tokio_rusqlite::rusqlite::Error> {
                    unsafe { conn.load_extension_enable()? };
                    let result = unsafe { conn.load_extension(path, None::<&str>) };
                    conn.load_extension_disable()?;
                    result
                })();
                // A path the user DID configure that fails to load is an
                // error, not a warning — the deployed setup would fail the
                // same way.
                loaded.map_err(|e| {
                    format!("sqlite-vec extension '{path}' (from ${env}) failed to load: {e}")
                })?;
                true
            }
            (Some(env), None) => {
                warnings.push(format!(
                    "vec0 statements were shape-checked but not executed: ${env} \
                     (spec.destination.sqlite.extensions_env) is not set in this \
                     environment, so the sqlite-vec extension could not be loaded"
                ));
                false
            }
            (None, _) => {
                warnings.push(
                    "vec0 statements were shape-checked but not executed: no \
                     spec.destination.sqlite.extensions_env is configured, so the \
                     sqlite-vec extension could not be loaded"
                        .to_string(),
                );
                false
            }
        };

        let statements = setup_statements(Self::embedding_dimensions(config));
        let apply = |label: &str| -> Result<(), String> {
            for statement in &statements {
                if statement.requires_vec && !vec_available {
                    continue;
                }
                conn.execute_batch(&statement.sql).map_err(|e| {
                    format!(
                        "{label}: generated DDL failed on a throwaway in-memory \
                         connection: {e}\n{}",
                        statement.sql
                    )
                })?;
            }
            Ok(())
        };

        // The full lifecycle the bundle promises: apply, idempotent
        // re-apply, reset, apply again.
        apply("apply")?;
        apply("re-apply (idempotency)")?;
        conn.execute_batch(&self.reset_sql(plan, config))
            .map_err(|e| format!("reset: generated DROP list failed: {e}"))?;
        apply("re-apply after reset")?;
        Ok(warnings)
    }

    fn ingest_select_sql(&self, plan: &HybridPlan, index: usize, config: &EtlConfig) -> String {
        let ingest = &plan.ingests[index];
        let model = Self::embedding_model(config);
        render_ingest_select(ingest, &model)
    }

    fn search_sql(&self, plan: &HybridPlan, config: &EtlConfig) -> String {
        let documents = Self::qualified(config, "documents");
        let fts = Self::qualified(config, "documents_fts");
        let vec = Self::qualified(config, "documents_vec");
        let model = &plan.search.embedding_model;
        let knn_n = plan.search.knn_candidates;
        let fts_n = plan.search.fts_candidates;
        format!(
            "WITH vec AS (\n\
             \x20 SELECT doc_rowid AS rid, ROW_NUMBER() OVER (ORDER BY _score ASC) AS rk\n\
             \x20 FROM sqlite_knn('{vec}', 'embedding',\n\
             \x20     (SELECT candle('{model}', {{query}})),\n\
             \x20     {knn_n})\n\
             ),\n\
             fts AS (\n\
             \x20 SELECT doc_rowid AS rid, ROW_NUMBER() OVER (ORDER BY _score DESC) AS rk\n\
             \x20 FROM sqlite_fts('{fts}', 'content', {{text_query}}, {fts_n})\n\
             ),\n\
             ranked AS (\n\
             \x20 SELECT COALESCE(v.rid, f.rid) AS rid,\n\
             \x20        COALESCE({{vector_weight}} / (60.0 + v.rk), 0)\n\
             \x20          + COALESCE({{text_weight}} / (60.0 + f.rk), 0) AS rrf_score\n\
             \x20 FROM vec v\n\
             \x20 FULL OUTER JOIN fts f ON v.rid = f.rid\n\
             ),\n\
             hits AS (\n\
             \x20 -- doc_id is not unique under {{since}} replay; keep each\n\
             \x20 -- chunk's best-scoring copy (read-time dedup — the no-PK\n\
             \x20 -- decision's other half).\n\
             \x20 SELECT d.doc_id, d.source_table, d.source_id, d.chunk_index, d.title,\n\
             \x20        d.content, d.author, d.created_at, r.rrf_score,\n\
             \x20        ROW_NUMBER() OVER (PARTITION BY d.doc_id ORDER BY r.rrf_score DESC)\n\
             \x20          AS dup_rank\n\
             \x20 FROM ranked r\n\
             \x20 JOIN {documents} d ON d.rid = r.rid\n\
             )\n\
             SELECT doc_id, source_table, source_id, chunk_index, title, content,\n\
             \x20      author, created_at, rrf_score\n\
             FROM hits\n\
             WHERE dup_rank = 1\n\
             ORDER BY rrf_score DESC, doc_id\n\
             LIMIT {{limit}}"
        )
    }

    fn get_document_sql(&self, _plan: &HybridPlan, config: &EtlConfig) -> String {
        let documents = Self::qualified(config, "documents");
        format!(
            "-- One document's ordered chunks: reassembly and neighbor-chunk\n\
             -- context in one call, drivable from any search hit (FR-9 returns\n\
             -- source_table/source_id with every row). Same read-time doc_id\n\
             -- dedup as search: under {{since}} replay each chunk keeps its\n\
             -- newest copy (max rid = latest insert).\n\
             WITH chunks AS (\n\
             \x20 SELECT doc_id, source_table, source_id, chunk_index, title, content,\n\
             \x20        author, created_at,\n\
             \x20        ROW_NUMBER() OVER (PARTITION BY doc_id ORDER BY rid DESC) AS dup_rank\n\
             \x20 FROM {documents}\n\
             \x20 WHERE source_table = {{source_table}} AND source_id = {{source_id}}\n\
             )\n\
             SELECT doc_id, source_table, source_id, chunk_index, title, content,\n\
             \x20      author, created_at\n\
             FROM chunks\n\
             WHERE dup_rank = 1\n\
             ORDER BY chunk_index"
        )
    }

    fn ctx_fragment(&self, config: &EtlConfig) -> String {
        let catalog = &config.destination.catalog;
        let path = config.destination.path.as_deref().unwrap_or_default();
        let extensions = config
            .destination
            .sqlite
            .as_ref()
            .and_then(|s| s.extensions_env.as_deref());
        let options = match extensions {
            Some(env) => format!("      options:\n        extensions_env: {env}\n"),
            None => String::new(),
        };
        format!(
            "# Merge this data source into your ctx.yaml's spec.data_sources.\n\
             # hierarchy_level: catalog registers the file as the '{catalog}'\n\
             # catalog — the generated SQL's '{catalog}.main.…' qualification\n\
             # depends on it. access_mode: read_write is what the ingest job's\n\
             # WRITE path requires.\n\
             spec:\n\
             \x20 data_sources:\n\
             \x20   - name: {catalog}\n\
             \x20     type: sqlite\n\
             \x20     path: {path}\n\
             \x20     access_mode: read_write\n\
             \x20     hierarchy_level: catalog\n\
             {options}"
        )
    }

    fn destination_table(&self, config: &EtlConfig) -> String {
        Self::qualified(config, "documents")
    }
}

/// One setup statement plus what executing it requires, so
/// [`SqliteDialect::validate_ddl`] can run everything the bundled SQLite
/// supports and be explicit about what it could not.
struct SetupStatement {
    sql: String,
    /// vec0 statements execute only when the sqlite-vec extension is
    /// loadable; otherwise validate_ddl skips them and says so.
    requires_vec: bool,
}

/// The setup DDL as discrete statements, in apply order. This is the
/// single source [`SqliteDialect::setup_sql`] joins and
/// [`SqliteDialect::validate_ddl`] executes — no drift between what ships
/// and what was checked.
fn setup_statements(dims: u32) -> Vec<SetupStatement> {
    let mut columns = String::new();
    // The rowid alias, first — see the module doc.
    columns.push_str("  rid INTEGER PRIMARY KEY,\n");
    for (i, col) in DOCUMENT_COLUMNS.iter().enumerate() {
        let sql_type = match col.kind {
            DocColumnKind::Utf8 | DocColumnKind::Json => "TEXT",
            DocColumnKind::Int32 => "INTEGER",
            // SQLite has no timestamp type; the provider maps declared
            // TIMESTAMP columns to Arrow timestamps.
            DocColumnKind::TimestampMs => "TIMESTAMP",
            DocColumnKind::Vector => "BLOB",
        };
        let not_null = if col.nullable { "" } else { " NOT NULL" };
        let comma = if i + 1 == DOCUMENT_COLUMNS.len() {
            ""
        } else {
            ","
        };
        columns.push_str(&format!("  {} {sql_type}{not_null}{comma}\n", col.name));
    }

    vec![
        SetupStatement {
            sql: format!(
                "-- Generated by skardi-etl. Idempotent: plain re-apply is a no-op;\n\
                 -- `skardi-etl setup --reset` drops these artifacts first (the v1\n\
                 -- rebuild path).\n\
                 --\n\
                 -- doc_id is deliberately NOT a primary key: jobs are append-only\n\
                 -- (no ON CONFLICT path in the executor), so a unique constraint\n\
                 -- would hard-fail {{since}} replay mid-stream. Replay accumulates\n\
                 -- rows; the search pipeline deduplicates by doc_id at read time,\n\
                 -- and the PRIMARY KEY arrives with v2 upsert. `rid` is the\n\
                 -- INTEGER rowid alias the vec0 mirror requires (and the join key\n\
                 -- the pipelines use); SQLite assigns it on the NULL the ingest\n\
                 -- SELECT emits.\n\
                 CREATE TABLE IF NOT EXISTS documents (\n{columns});\n"
            ),
            requires_vec: false,
        },
        SetupStatement {
            sql: "-- Text arm: standalone fts5 mirror (rebuild-first keeps triggers\n\
                  -- insert-only).\n\
                  CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(\n\
                  \x20 content,\n\
                  \x20 doc_rowid UNINDEXED\n\
                  );\n"
                .to_string(),
            requires_vec: false,
        },
        SetupStatement {
            sql: format!(
                "-- Vector arm: vec0 requires the INTEGER key.\n\
                 CREATE VIRTUAL TABLE IF NOT EXISTS documents_vec USING vec0(\n\
                 \x20 doc_rowid INTEGER PRIMARY KEY,\n\
                 \x20 embedding float[{dims}]\n\
                 );\n"
            ),
            requires_vec: true,
        },
        SetupStatement {
            sql: "-- Sync triggers: INSERT-only by design (append-only jobs;\n\
                  -- rebuild-first refresh). NEW.rid carries the join key.\n\
                  CREATE TRIGGER IF NOT EXISTS documents_ai_fts\n\
                  AFTER INSERT ON documents BEGIN\n\
                  \x20 INSERT INTO documents_fts(doc_rowid, content)\n\
                  \x20 VALUES (NEW.rid, NEW.content);\n\
                  END;\n"
                .to_string(),
            requires_vec: false,
        },
        SetupStatement {
            sql: "CREATE TRIGGER IF NOT EXISTS documents_ai_vec\n\
                  AFTER INSERT ON documents BEGIN\n\
                  \x20 INSERT INTO documents_vec(doc_rowid, embedding)\n\
                  \x20 VALUES (NEW.rid, NEW.embedding);\n\
                  END;\n"
                .to_string(),
            requires_vec: true,
        },
    ]
}

/// The document-shaped SELECT: pinned unnest spelling (projection-position
/// `UNNEST(chunk_parts(...)) AS part` in the mid layer, field access
/// outside — the `chunk_parts` plannability test's exact shape), column
/// order ≡ the sqlite DDL order (rid first).
fn render_ingest_select(ingest: &IngestPlan, embedding_model: &str) -> String {
    let short = &ingest.source_table;
    let id = &ingest.id_column;
    let title = match &ingest.title_column {
        Some(col) => format!("u.{col}"),
        None => "CAST(NULL AS VARCHAR)".to_string(),
    };
    let author = match &ingest.author_column {
        Some(col) => format!("u.{col}"),
        None => "CAST(NULL AS VARCHAR)".to_string(),
    };
    let created_at = match &ingest.timestamp_column {
        Some(col) => format!("u.{col}"),
        // Exact-type NULL so the planned schema matches the destination's
        // Timestamp(ms, UTC) — a bare CAST(NULL AS TIMESTAMP) is
        // nanosecond-typed and would fail the order/type assertion.
        None => "arrow_cast(NULL, 'Timestamp(Millisecond, Some(\"UTC\"))')".to_string(),
    };
    let metadata = if ingest.metadata_columns.is_empty() {
        "CAST(NULL AS VARCHAR)".to_string()
    } else {
        let args = ingest
            .metadata_columns
            .iter()
            .map(|col| format!("'{col}', u.{col}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("json_pack({args})")
    };

    let inner_columns = ingest.inner_columns().join(", ");
    let since = if ingest.incremental {
        let ts = ingest
            .timestamp_column
            .as_deref()
            .expect("incremental requires a timestamp role (resolved)");
        format!("\n  WHERE {ts} >= {{since}}")
    } else {
        String::new()
    };
    let mode = ingest.chunking.splitter.mode();
    let (size, overlap) = (ingest.chunking.size, ingest.chunking.overlap);
    let content = &ingest.content_column;
    let from = &ingest.source_from;

    format!(
        "SELECT\n\
         \x20 CAST(NULL AS BIGINT)                                    AS rid,\n\
         \x20 '{short}:' || CAST(u.{id} AS VARCHAR) || ':'\n\
         \x20   || CAST(u.part['chunk_idx'] AS VARCHAR)               AS doc_id,\n\
         \x20 '{short}'                                               AS source_table,\n\
         \x20 CAST(u.{id} AS VARCHAR)                                 AS source_id,\n\
         \x20 {title}                                                 AS title,\n\
         \x20 u.part['chunk_text']                                    AS content,\n\
         \x20 u.part['chunk_idx']                                     AS chunk_index,\n\
         \x20 {author}                                                AS author,\n\
         \x20 {created_at}                                            AS created_at,\n\
         \x20 {metadata}                                              AS metadata,\n\
         \x20 vec_to_binary(candle('{embedding_model}', u.part['chunk_text'])) AS embedding\n\
         FROM (\n\
         \x20 SELECT s.*, UNNEST(chunk_parts('{mode}', s.{content}, {size}, {overlap})) AS part\n\
         \x20 FROM (\n\
         \x20   SELECT {inner_columns}\n\
         \x20   FROM {from}{since}\n\
         \x20   LIMIT {{limit}}\n\
         \x20 ) s\n\
         ) u"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::etl::config::EtlConfig;
    use crate::etl::config::TargetFormatKind;
    use crate::etl::format::hybrid_plan;
    use crate::etl::recipe::find_embedded;
    use crate::sources::providers::open_connector::source_pack::SourcePackRegistry;

    fn flagship() -> (EtlConfig, HybridPlan) {
        let config = EtlConfig::from_yaml(
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
        .unwrap();
        let registry = SourcePackRegistry::builtins().unwrap();
        let recipe = find_embedded("github", TargetFormatKind::HybridSearch)
            .unwrap()
            .unwrap();
        let resolved = recipe.resolve(registry.get("github").unwrap()).unwrap();
        let plan = hybrid_plan(&config, &resolved).unwrap();
        (config, plan)
    }

    #[test]
    fn setup_sql_declares_rid_first_then_the_neutral_columns_in_order() {
        let (config, plan) = flagship();
        let ddl = SqliteDialect.setup_sql(&plan, &config);
        // Column order in the DDL is the positional-INSERT contract; rid
        // leads, then the design's ten in order.
        let mut last = 0usize;
        for name in [
            "rid INTEGER PRIMARY KEY",
            "doc_id TEXT NOT NULL",
            "source_table TEXT NOT NULL",
            "source_id TEXT NOT NULL",
            "title TEXT",
            "content TEXT NOT NULL",
            "chunk_index INTEGER NOT NULL",
            "author TEXT",
            "created_at TIMESTAMP",
            "metadata TEXT",
            "embedding BLOB NOT NULL",
        ] {
            let at = ddl
                .find(name)
                .unwrap_or_else(|| panic!("{name} in DDL:\n{ddl}"));
            assert!(at > last, "{name} out of order");
            last = at;
        }
        assert!(ddl.contains("float[384]"), "dimensions sized into vec0");
        assert!(
            ddl.contains("deliberately NOT a primary key"),
            "the no-PK rationale ships in the DDL"
        );
        assert!(
            !ddl.contains("PRIMARY KEY (doc_id)"),
            "doc_id stays keyless"
        );
        // Idempotent everywhere.
        assert_eq!(ddl.matches("IF NOT EXISTS").count(), 5, "{ddl}");
    }

    #[test]
    fn reset_drops_every_bundle_owned_artifact_and_nothing_else() {
        let (config, plan) = flagship();
        let reset = SqliteDialect.reset_sql(&plan, &config);
        for artifact in [
            "documents_ai_fts",
            "documents_ai_vec",
            "documents_fts",
            "documents_vec",
            "documents",
        ] {
            assert!(reset.contains(artifact), "{artifact}");
        }
        assert_eq!(reset.matches("DROP").count(), 5);
    }

    #[test]
    fn ingest_select_matches_ddl_order_and_the_pinned_unnest_spelling() {
        let (config, plan) = flagship();
        let sql = SqliteDialect.ingest_select_sql(&plan, 0, &config);

        // Projection order ≡ DDL order (rid first) — the positional-INSERT
        // invariant, asserted lexically here and by plan-check in 1b.6.
        let mut last = 0usize;
        for alias in [
            "AS rid",
            "AS doc_id",
            "AS source_table",
            "AS source_id",
            "AS title",
            "AS content",
            "AS chunk_index",
            "AS author",
            "AS created_at",
            "AS metadata",
            "AS embedding",
        ] {
            let at = sql
                .find(alias)
                .unwrap_or_else(|| panic!("{alias} in:\n{sql}"));
            assert!(at > last, "{alias} out of order in:\n{sql}");
            last = at;
        }

        // The chunk_parts plannability pin's exact spelling.
        assert!(
            sql.contains("UNNEST(chunk_parts('markdown', s.body, 1200, 200)) AS part"),
            "{sql}"
        );
        assert!(sql.contains("u.part['chunk_text']"), "{sql}");

        // Incremental: the pack pushdown rides the timestamp column, and
        // {limit} is KEPT (first-backfill bound).
        assert!(sql.contains("WHERE updated_at >= {since}"), "{sql}");
        assert!(sql.contains("LIMIT {limit}"), "{sql}");

        // Metadata through json_pack, never string concatenation.
        assert!(
            sql.contains("json_pack('number', u.number, 'state', u.state)"),
            "{sql}"
        );

        // Write side packs; the source is fully qualified.
        assert!(sql.contains("vec_to_binary(candle("), "{sql}");
        assert!(sql.contains("FROM saas.github_demo.issues"), "{sql}");
    }

    #[test]
    fn search_sql_joins_on_rid_dedups_by_doc_id_and_never_packs_the_query_vector() {
        let (config, plan) = flagship();
        let sql = SqliteDialect.search_sql(&plan, &config);

        assert!(
            sql.contains("sqlite_knn('gh_search.main.documents_vec'"),
            "{sql}"
        );
        assert!(
            sql.contains("sqlite_fts('gh_search.main.documents_fts'"),
            "{sql}"
        );
        assert!(
            sql.contains("JOIN gh_search.main.documents d ON d.rid = r.rid"),
            "{sql}"
        );
        assert!(
            sql.contains("PARTITION BY d.doc_id ORDER BY r.rrf_score DESC"),
            "read-time doc_id dedup: {sql}"
        );
        // FR-9 params, all five.
        for param in [
            "{query}",
            "{text_query}",
            "{vector_weight}",
            "{text_weight}",
            "{limit}",
        ] {
            assert!(sql.contains(param), "{param} in: {sql}");
        }
        // The read path never packs — sqlite_knn takes the bare candle()
        // List<Float32>.
        assert!(!sql.contains("vec_to_binary"), "{sql}");
        assert!(
            sql.contains("(SELECT candle('models/generated/bge-small-en-v1.5', {query}))"),
            "{sql}"
        );
    }

    #[test]
    fn get_document_returns_ordered_chunks_for_one_document() {
        let (config, plan) = flagship();
        let sql = SqliteDialect.get_document_sql(&plan, &config);
        assert!(
            sql.contains("WHERE source_table = {source_table} AND source_id = {source_id}"),
            "{sql}"
        );
        assert!(sql.contains("ORDER BY chunk_index"), "{sql}");
        assert!(
            sql.contains("PARTITION BY doc_id ORDER BY rid DESC"),
            "newest copy per chunk: {sql}"
        );
    }

    #[test]
    fn validate_ddl_executes_the_lifecycle_and_reports_what_it_could_not_run() {
        let (config, plan) = flagship();
        // No extensions_env configured → fts5/documents/triggers really
        // execute (apply, re-apply, reset, re-apply); vec0 is skipped WITH
        // a warning, never silently.
        let warnings = SqliteDialect
            .validate_ddl(&plan, &config)
            .expect("DDL executes");
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].contains("vec0 statements were shape-checked but not executed"),
            "{warnings:?}"
        );
        assert!(
            warnings[0].contains("no spec.destination.sqlite.extensions_env"),
            "{warnings:?}"
        );
    }

    #[test]
    fn validate_ddl_names_the_unset_env_var_when_one_is_configured() {
        let (mut config, plan) = flagship();
        config.destination.sqlite = Some(crate::etl::config::SqliteFields {
            extensions_env: Some("SKARDI_ETL_TEST_UNSET_VEC_PATH".to_string()),
        });
        let warnings = SqliteDialect
            .validate_ddl(&plan, &config)
            .expect("DDL executes");
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].contains("$SKARDI_ETL_TEST_UNSET_VEC_PATH"),
            "{warnings:?}"
        );
    }

    #[test]
    fn ctx_fragment_grants_read_write_and_carries_the_extension_env() {
        let (mut config, plan) = flagship();
        config.destination.sqlite = Some(crate::etl::config::SqliteFields {
            extensions_env: Some("SQLITE_VEC_PATH".to_string()),
        });
        let fragment = SqliteDialect.ctx_fragment(&config);
        assert!(fragment.contains("name: gh_search"), "{fragment}");
        assert!(fragment.contains("type: sqlite"), "{fragment}");
        // The real DataSource model's keys: `path` (not connection_string)
        // and catalog-level registration, which the generated SQL's
        // `gh_search.main.…` qualification depends on.
        assert!(fragment.contains("path: data/gh.db"), "{fragment}");
        assert!(fragment.contains("access_mode: read_write"), "{fragment}");
        assert!(fragment.contains("hierarchy_level: catalog"), "{fragment}");
        assert!(
            fragment.contains("extensions_env: SQLITE_VEC_PATH"),
            "{fragment}"
        );
        assert_eq!(
            SqliteDialect.destination_table(&config),
            "gh_search.main.documents"
        );
        let _ = plan;
    }
}
