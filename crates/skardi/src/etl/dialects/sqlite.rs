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
//!   sync triggers copy `NEW.rid` into both mirrors, and ranked results
//!   join back on `d.rid`. `rid` is exposed because skardi's SQLite
//!   provider derives schemas from `PRAGMA table_info` — a bare
//!   (undeclared) rowid would be invisible to DataFusion.
//! - **`sqlite_knn` returns all non-vector columns + `_score`** — for the
//!   vec0 mirror that is `doc_rowid` + `_score`, exactly what the
//!   join-back needs. The query vector is a bare `candle()` scalar
//!   subquery: NO `vec_to_binary` on the read path (the UDTF packs the
//!   `List<Float32>` itself); packing is write-side only.
//!
//! Refresh is **replace-on-insert**: a BEFORE INSERT trigger deletes the
//! previous copy of the incoming `doc_id` (and its mirror rows), so
//! `{since}` replay REPLACES rather than accumulates — the fixed KNN/FTS
//! candidate pools (80/60) can never fill up with stale copies of one
//! hot document and crowd everything else out. The pipelines keep a
//! read-time `doc_id` dedup as defense in depth. One honest residue:
//! `doc_id` embeds `chunk_index`, so a document that SHRINKS on re-ingest
//! leaves stale tail chunks until a rebuild — the README says so.
//! `--reset` drops every bundle-owned artifact.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use tokio_rusqlite::rusqlite;

use super::super::config::{EmbeddingSpec, EtlConfig};
use super::super::dialect::{Capabilities, EngineDialect};
use super::super::format::{DOCUMENT_COLUMNS, DocColumnKind, HybridPlan, IngestPlan};

pub struct SqliteDialect;

impl SqliteDialect {
    fn embedding_dimensions(config: &EtlConfig) -> u32 {
        config
            .embedding
            .as_ref()
            .map(|e| e.dimensions)
            .unwrap_or_default()
    }

    fn embedding(config: &EtlConfig) -> &EmbeddingSpec {
        config
            .embedding
            .as_ref()
            .expect("hybrid configs always carry embedding (validated at config load)")
    }

    /// The env var naming the sqlite-vec loadable: the config's override,
    /// else `SQLITE_VEC_PATH` (the repo-wide convention). Hybrid search on
    /// sqlite ALWAYS needs vec0, so the ctx fragment always names an
    /// extension env — the provider only loads env-named extensions when
    /// the option is present, so an omitted option would leave vec0
    /// unloadable no matter what the operator exports.
    fn extensions_env(config: &EtlConfig) -> &str {
        config
            .destination
            .sqlite
            .as_ref()
            .and_then(|s| s.extensions_env.as_deref())
            .unwrap_or("SQLITE_VEC_PATH")
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
        // Every bundle-owned artifact, nothing else. Triggers and the
        // index drop with their table but are listed explicitly so the
        // reset is readable as a complete inventory.
        "DROP TRIGGER IF EXISTS documents_bi_replace;\n\
         DROP TRIGGER IF EXISTS documents_ai_fts;\n\
         DROP TRIGGER IF EXISTS documents_ai_vec;\n\
         DROP INDEX IF EXISTS documents_doc_id_idx;\n\
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
        // var holding its path (never the path itself; defaults to
        // SQLITE_VEC_PATH — the same env the ctx fragment names); when the
        // var is unset at generate time, vec0 statements degrade to
        // shape-only — reported, never silent.
        let env = Self::extensions_env(config);
        let ext_path = std::env::var(env).ok().filter(|p| !p.trim().is_empty());
        let vec_available = match &ext_path {
            Some(path) => {
                // SAFETY: mirrors the sqlite provider's init_connection —
                // loading is enabled only around the user-designated path,
                // then disabled again.
                let loaded = (|| -> Result<(), tokio_rusqlite::rusqlite::Error> {
                    unsafe { conn.load_extension_enable()? };
                    let result = unsafe { conn.load_extension(path, None::<&str>) };
                    conn.load_extension_disable()?;
                    result
                })();
                // A path the env DOES designate but that fails to load is
                // an error, not a warning — the deployed setup would fail
                // the same way.
                loaded.map_err(|e| {
                    format!("sqlite-vec extension '{path}' (from ${env}) failed to load: {e}")
                })?;
                true
            }
            None => {
                warnings.push(format!(
                    "vec0 statements were shape-checked but not executed: ${env} \
                     (spec.destination.sqlite.extensions_env, default SQLITE_VEC_PATH) \
                     is not set in this environment, so the sqlite-vec extension could \
                     not be loaded"
                ));
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
        // Replay smoke (full DDL only): inserting the same doc_id twice
        // must leave exactly ONE live copy in the table and each mirror —
        // the replace-on-insert trigger is what keeps the search candidate
        // pools from filling with stale copies, so it gets executed here,
        // not just created.
        if vec_available {
            let blob_bytes = Self::embedding_dimensions(config) * 4;
            for copy in ["first copy", "second copy"] {
                conn.execute(
                    &format!(
                        "INSERT INTO documents VALUES (NULL, 'smoke:1:0', 'smoke', '1', \
                         NULL, '{copy}', 0, NULL, NULL, NULL, zeroblob({blob_bytes}))"
                    ),
                    [],
                )
                .map_err(|e| format!("replay smoke: insert '{copy}' failed: {e}"))?;
            }
            for (table, what) in [
                ("documents", "the table"),
                ("documents_fts", "the fts mirror"),
                ("documents_vec", "the vec mirror"),
            ] {
                let live: i64 = conn
                    .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
                    .map_err(|e| format!("replay smoke: count {table}: {e}"))?;
                if live != 1 {
                    return Err(format!(
                        "replay smoke: re-inserting one doc_id left {live} rows in {what} \
                         ({table}); the replace-on-insert trigger must keep exactly one"
                    ));
                }
            }
        }
        apply("re-apply (idempotency)")?;
        conn.execute_batch(&self.reset_sql(plan, config))
            .map_err(|e| format!("reset: generated DROP list failed: {e}"))?;
        apply("re-apply after reset")?;
        Ok(warnings)
    }

    fn ingest_select_sql(&self, plan: &HybridPlan, index: usize, config: &EtlConfig) -> String {
        let ingest = &plan.ingests[index];
        render_ingest_select(ingest, Self::embedding(config))
    }

    fn search_sql(&self, plan: &HybridPlan, config: &EtlConfig) -> String {
        let documents = Self::qualified(config, "documents");
        let fts = Self::qualified(config, "documents_fts");
        let vec = Self::qualified(config, "documents_vec");
        // The SAME UDF+model the ingest embedded with — one renderer for
        // both sides, so a remote_embed config can never silently search
        // with candle.
        let query_vector = plan.search.embedding.call_expr("{query}");
        let knn_n = plan.search.knn_candidates;
        let fts_n = plan.search.fts_candidates;
        format!(
            "WITH vec AS (\n\
             \x20 SELECT doc_rowid AS rid, ROW_NUMBER() OVER (ORDER BY _score ASC) AS rk\n\
             \x20 FROM sqlite_knn('{vec}', 'embedding',\n\
             \x20     (SELECT {query_vector}),\n\
             \x20     {knn_n})\n\
             ),\n\
             fts AS (\n\
             \x20 -- fts5 columns read back as TEXT through the provider (PRAGMA\n\
             \x20 -- reports empty types), so the rowid rides home as a string —\n\
             \x20 -- cast it back before it meets the vec arm's Int64.\n\
             \x20 SELECT CAST(doc_rowid AS BIGINT) AS rid,\n\
             \x20        ROW_NUMBER() OVER (ORDER BY _score DESC) AS rk\n\
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
             \x20 -- Write-time replacement (documents_bi_replace) keeps one live\n\
             \x20 -- copy per doc_id; this read-time dedup stays as defense in\n\
             \x20 -- depth (e.g. rows written outside the generated jobs).\n\
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
             -- source_table/source_id with every row). Same defense-in-depth\n\
             -- doc_id dedup as search (write-time replacement already keeps one\n\
             -- copy): newest wins (max rid = latest insert).\n\
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
        // ALWAYS emitted: hybrid search on sqlite needs vec0, and the
        // provider only loads env-named extensions when this option is
        // present — a fragment without it cannot serve the vector arm.
        let env = Self::extensions_env(config);
        format!(
            "# Merge this data source into your ctx.yaml's spec.data_sources.\n\
             # hierarchy_level: catalog registers the file as the '{catalog}'\n\
             # catalog — the generated SQL's '{catalog}.main.…' qualification\n\
             # depends on it. access_mode: read_write is what the ingest job's\n\
             # WRITE path requires. options.extensions_env names the env var\n\
             # holding the sqlite-vec (vec0) loadable — required for the vector\n\
             # arm; export it before starting the server.\n\
             spec:\n\
             \x20 data_sources:\n\
             \x20   - name: {catalog}\n\
             \x20     type: sqlite\n\
             \x20     path: {path}\n\
             \x20     access_mode: read_write\n\
             \x20     hierarchy_level: catalog\n\
             \x20     options:\n\
             \x20       extensions_env: {env}\n"
        )
    }

    fn destination_table(&self, config: &EtlConfig) -> String {
        Self::qualified(config, "documents")
    }

    fn planned_destination_schema(&self, _config: &EtlConfig) -> SchemaRef {
        // What read_schema_from_pragma will derive from the setup DDL:
        // INTEGER → Int64 (rid, chunk_index), TEXT → Utf8 (including
        // created_at — sqlite has no timestamp type), BLOB → Binary.
        let mut fields = vec![Field::new("rid", DataType::Int64, true)];
        for col in DOCUMENT_COLUMNS {
            let dt = match col.kind {
                DocColumnKind::Utf8 | DocColumnKind::Json | DocColumnKind::TimestampMs => {
                    DataType::Utf8
                }
                DocColumnKind::Int32 => DataType::Int64,
                DocColumnKind::Vector => DataType::Binary,
            };
            fields.push(Field::new(col.name, dt, col.nullable));
        }
        Arc::new(Schema::new(fields))
    }

    fn udtf_stubs(&self, _config: &EtlConfig) -> Vec<(&'static str, SchemaRef)> {
        vec![
            // sqlite_knn over documents_vec: non-vector columns + _score.
            // The vec0 rowid alias reads back Int64 (pk with empty type).
            (
                "sqlite_knn",
                Arc::new(Schema::new(vec![
                    Field::new("doc_rowid", DataType::Int64, true),
                    Field::new("_score", DataType::Float64, true),
                ])),
            ),
            // sqlite_fts over documents_fts: fts5 columns all read back as
            // TEXT (hence the CAST in search_sql's fts arm) + _score.
            (
                "sqlite_fts",
                Arc::new(Schema::new(vec![
                    Field::new("content", DataType::Utf8, true),
                    Field::new("doc_rowid", DataType::Utf8, true),
                    Field::new("_score", DataType::Float64, true),
                ])),
            ),
        ]
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
            // TEXT, honestly: SQLite has no timestamp type, and the provider
            // derives Arrow schemas from PRAGMA table_info (a declared
            // TIMESTAMP would read back as Utf8 anyway). The ingest SELECT
            // writes RFC 3339 UTC text, which sorts chronologically.
            DocColumnKind::TimestampMs => "TEXT",
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
                 -- would hard-fail {{since}} replay mid-stream. Uniqueness is\n\
                 -- enforced by the documents_bi_replace trigger below instead:\n\
                 -- every INSERT first deletes the previous copy of its doc_id (and\n\
                 -- that copy's mirror rows), so replay REPLACES rather than\n\
                 -- accumulates and the search candidate pools never fill with\n\
                 -- stale copies. The declared PRIMARY KEY arrives with v2 upsert.\n\
                 -- `rid` is the INTEGER rowid alias the vec0 mirror requires (and\n\
                 -- the join key the pipelines use); SQLite assigns it on the NULL\n\
                 -- the ingest SELECT emits.\n\
                 CREATE TABLE IF NOT EXISTS documents (\n{columns});\n"
            ),
            requires_vec: false,
        },
        SetupStatement {
            sql: "-- The replace trigger's lookup path (doc_id is not a key).\n\
                  CREATE INDEX IF NOT EXISTS documents_doc_id_idx ON documents(doc_id);\n"
                .to_string(),
            requires_vec: false,
        },
        SetupStatement {
            sql: "-- Text arm: standalone fts5 mirror. Rows are inserted with an\n\
                  -- explicit rowid = documents.rid so the replace trigger can\n\
                  -- delete them by integer key.\n\
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
            sql: "-- Replace-on-insert: one live copy per doc_id. Deleting the old\n\
                  -- copy's mirror rows is inlined here (not chained through DELETE\n\
                  -- triggers) so the behavior never depends on recursive-trigger\n\
                  -- settings.\n\
                  CREATE TRIGGER IF NOT EXISTS documents_bi_replace\n\
                  BEFORE INSERT ON documents BEGIN\n\
                  \x20 DELETE FROM documents_fts\n\
                  \x20   WHERE rowid IN (SELECT rid FROM documents WHERE doc_id = NEW.doc_id);\n\
                  \x20 DELETE FROM documents_vec\n\
                  \x20   WHERE doc_rowid IN (SELECT rid FROM documents WHERE doc_id = NEW.doc_id);\n\
                  \x20 DELETE FROM documents WHERE doc_id = NEW.doc_id;\n\
                  END;\n"
                .to_string(),
            requires_vec: true,
        },
        SetupStatement {
            sql: "-- Sync triggers: NEW.rid carries the join key into both mirrors\n\
                  -- (and pins the fts row's OWN rowid so replacement can find it).\n\
                  CREATE TRIGGER IF NOT EXISTS documents_ai_fts\n\
                  AFTER INSERT ON documents BEGIN\n\
                  \x20 INSERT INTO documents_fts(rowid, doc_rowid, content)\n\
                  \x20 VALUES (NEW.rid, NEW.rid, NEW.content);\n\
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
fn render_ingest_select(ingest: &IngestPlan, embedding: &EmbeddingSpec) -> String {
    let short = &ingest.source_table;
    let id = &ingest.id_column;
    // The configured UDF (candle or remote_embed), from the one shared
    // renderer the search side also uses.
    let embed_call = embedding.call_expr("u.part['chunk_text']");
    // arrow_cast(…, 'Utf8'), never CAST(… AS VARCHAR): DataFusion's SQL
    // layer maps VARCHAR (and string concatenation) to Utf8View, while the
    // provider-derived destination schema is plain Utf8 — and the
    // executor's type preflight compares them verbatim.
    let title = match &ingest.title_column {
        Some(col) => format!("u.{col}"),
        None => "arrow_cast(NULL, 'Utf8')".to_string(),
    };
    let author = match &ingest.author_column {
        Some(col) => format!("u.{col}"),
        None => "arrow_cast(NULL, 'Utf8')".to_string(),
    };
    // ISO-8601 text: SQLite has no timestamp type and the provider derives
    // the destination schema from PRAGMA table_info, so created_at reads
    // back as Utf8 — the SELECT must produce Utf8 too. RFC 3339 in one
    // timezone sorts lexicographically = chronologically.
    let created_at = match &ingest.timestamp_column {
        Some(col) => format!("arrow_cast(u.{col}, 'Utf8')"),
        None => "arrow_cast(NULL, 'Utf8')".to_string(),
    };
    let metadata = if ingest.metadata_columns.is_empty() {
        "arrow_cast(NULL, 'Utf8')".to_string()
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
         \x20 arrow_cast('{short}:' || arrow_cast(u.{id}, 'Utf8') || ':'\n\
         \x20   || arrow_cast(u.part['chunk_idx'], 'Utf8'), 'Utf8')   AS doc_id,\n\
         \x20 '{short}'                                               AS source_table,\n\
         \x20 arrow_cast(u.{id}, 'Utf8')                              AS source_id,\n\
         \x20 {title}                                                 AS title,\n\
         \x20 u.part['chunk_text']                                    AS content,\n\
         \x20 -- BIGINT: the INTEGER destination column reads back as Int64 and\n\
         \x20 -- the executor's type preflight is exact (chunk_idx is Int32).\n\
         \x20 CAST(u.part['chunk_idx'] AS BIGINT)                     AS chunk_index,\n\
         \x20 {author}                                                AS author,\n\
         \x20 {created_at}                                            AS created_at,\n\
         \x20 {metadata}                                              AS metadata,\n\
         \x20 vec_to_binary({embed_call})                             AS embedding\n\
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
            "created_at TEXT",
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
        // Replace-on-insert: the trigger clears BOTH mirrors and the old
        // table row, and the fts sync trigger pins rowid = rid so the
        // replacement can find the fts row by integer key.
        assert!(ddl.contains("documents_bi_replace"), "{ddl}");
        assert!(
            ddl.contains("DELETE FROM documents WHERE doc_id = NEW.doc_id"),
            "{ddl}"
        );
        assert!(
            ddl.contains("INSERT INTO documents_fts(rowid, doc_rowid, content)"),
            "{ddl}"
        );
        assert!(
            ddl.contains("documents_doc_id_idx"),
            "the replace lookup path: {ddl}"
        );
        // Idempotent everywhere: table, index, 2 mirrors, 3 triggers.
        assert_eq!(ddl.matches("IF NOT EXISTS").count(), 7, "{ddl}");
    }

    #[test]
    fn reset_drops_every_bundle_owned_artifact_and_nothing_else() {
        let (config, plan) = flagship();
        let reset = SqliteDialect.reset_sql(&plan, &config);
        for artifact in [
            "documents_bi_replace",
            "documents_ai_fts",
            "documents_ai_vec",
            "documents_doc_id_idx",
            "documents_fts",
            "documents_vec",
            "documents",
        ] {
            assert!(reset.contains(artifact), "{artifact}");
        }
        assert_eq!(reset.matches("DROP").count(), 7);
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

        // Exact-type discipline against the PRAGMA-derived destination
        // schema (the executor's preflight compares types verbatim):
        // Int32 chunk ordinal → Int64 column; timestamp → RFC 3339 text —
        // via arrow_cast, because SQL VARCHAR would plan as Utf8View.
        assert!(sql.contains("CAST(u.part['chunk_idx'] AS BIGINT)"), "{sql}");
        assert!(sql.contains("arrow_cast(u.updated_at, 'Utf8')"), "{sql}");
        assert!(!sql.contains("AS VARCHAR)"), "no Utf8View leaks: {sql}");

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
        // The fts arm's doc_rowid reads back as TEXT (fts5 via PRAGMA);
        // without this cast the FULL OUTER JOIN mixes Utf8 and Int64.
        assert!(sql.contains("CAST(doc_rowid AS BIGINT) AS rid"), "{sql}");
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
        // No extensions_env configured → the default SQLITE_VEC_PATH
        // applies. When that env is set (CI sets it), the FULL DDL
        // executes including the replay smoke and there are no warnings;
        // when unset, vec0 is skipped WITH a warning naming the default —
        // never silently.
        let warnings = SqliteDialect
            .validate_ddl(&plan, &config)
            .expect("DDL executes");
        if std::env::var("SQLITE_VEC_PATH").is_ok() {
            assert!(warnings.is_empty(), "{warnings:?}");
        } else {
            assert_eq!(warnings.len(), 1, "{warnings:?}");
            assert!(
                warnings[0].contains("vec0 statements were shape-checked but not executed"),
                "{warnings:?}"
            );
            assert!(warnings[0].contains("$SQLITE_VEC_PATH"), "{warnings:?}");
            assert!(
                warnings[0].contains("default SQLITE_VEC_PATH"),
                "{warnings:?}"
            );
        }
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

    #[test]
    fn ctx_fragment_defaults_the_extension_env_when_none_is_configured() {
        // Hybrid search on sqlite always needs vec0; a fragment without an
        // extensions option could never load it (the provider only loads
        // env-named extensions when the option is present), so the default
        // SQLITE_VEC_PATH is always emitted.
        let (config, _plan) = flagship();
        assert!(
            config.destination.sqlite.is_none(),
            "fixture has no override"
        );
        let fragment = SqliteDialect.ctx_fragment(&config);
        assert!(
            fragment.contains("extensions_env: SQLITE_VEC_PATH"),
            "{fragment}"
        );
        // And a config override wins.
        let mut config = config;
        config.destination.sqlite = Some(crate::etl::config::SqliteFields {
            extensions_env: Some("MY_VEC0".to_string()),
        });
        assert!(
            SqliteDialect
                .ctx_fragment(&config)
                .contains("extensions_env: MY_VEC0")
        );
    }

    #[test]
    fn remote_embed_configs_render_remote_embed_in_both_ingest_and_search() {
        let config = EtlConfig::from_yaml(
            r#"
kind: etl
metadata:
  name: github-issues-search
spec:
  source: { pack: github, binding: saas.github_demo, tables: [issues] }
  format: hybrid_search
  destination: { type: sqlite, path: data/gh.db, catalog: gh_search }
  embedding: { udf: remote_embed, provider: openai, model: text-embedding-3-small, dimensions: 1536 }
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

        // The configured UDF reaches BOTH sides — never silently candle.
        let ingest = SqliteDialect.ingest_select_sql(&plan, 0, &config);
        assert!(
            ingest.contains(
                "vec_to_binary(remote_embed('openai', 'text-embedding-3-small', \
                 u.part['chunk_text']))"
            ),
            "{ingest}"
        );
        assert!(!ingest.contains("candle("), "{ingest}");

        let search = SqliteDialect.search_sql(&plan, &config);
        assert!(
            search.contains("(SELECT remote_embed('openai', 'text-embedding-3-small', {query}))"),
            "{search}"
        );
        assert!(!search.contains("candle("), "{search}");

        // Dimensions flow into the vec0 DDL as declared.
        assert!(
            SqliteDialect
                .setup_sql(&plan, &config)
                .contains("float[1536]")
        );
    }
}
