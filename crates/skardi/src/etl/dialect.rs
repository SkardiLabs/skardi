//! `EngineDialect` — everything engine-specific, behind an explicit
//! capability check.
//!
//! A dialect renders neutral plans ([`super::format`]) into concrete SQL:
//! the destination DDL (with its search artifacts and sync triggers), the
//! ingest SELECT, the search / get-document pipeline queries, and the ctx
//! fragment. Call-shape builders take the FULL per-engine argument set —
//! `pg_knn` needs a metric argument, `sqlite_knn` a candidate count — not
//! a fixed `(table, col)` pair (design Research Findings).
//!
//! v1 ships `sqlite`; postgres/lance/mysql land in M2/M3 — until then
//! [`resolve_dialect`] answers them with the FR-5 capability refusal so a
//! config naming them fails early with the engines that WOULD work.

use super::config::{EngineKind, EtlConfig, TargetFormatKind};
use super::format::HybridPlan;

/// What one engine can do (design §Engine Dialects; PRD §6.6). The
/// generator compares `TargetFormatKind`'s needs against this and refuses
/// early rather than emitting a broken bundle.
#[derive(Debug, Clone, Copy)]
pub struct Capabilities {
    pub fts: bool,
    pub knn: bool,
    pub okf_table: bool,
    /// Whether `setup` has DDL to apply (Lance has none).
    pub needs_setup: bool,
}

/// One destination engine's rendering surface.
pub trait EngineDialect {
    fn name(&self) -> &'static str;
    fn capabilities(&self) -> Capabilities;

    /// The full `setup.sql`: destination table, search artifacts, sync
    /// triggers — idempotent (`IF NOT EXISTS` throughout), so plain
    /// re-apply is a no-op and `--reset` (DROP + re-apply) is the rebuild
    /// path.
    fn setup_sql(&self, plan: &HybridPlan, config: &EtlConfig) -> String;

    /// The `DROP … IF EXISTS` prelude `--reset` runs before re-applying
    /// [`EngineDialect::setup_sql`] — every bundle-owned artifact, nothing
    /// else.
    fn reset_sql(&self, plan: &HybridPlan, config: &EtlConfig) -> String;

    /// Gate 3 of valid-by-construction: EXECUTE the DDL against a
    /// throwaway engine instance — apply, re-apply (idempotency), reset,
    /// re-apply again. The strongest check in the validation matrix where
    /// the engine supports it. `Ok` carries warnings for statements that
    /// could only be shape-checked (e.g. vec0 when the sqlite-vec
    /// extension isn't loadable at generate time).
    fn validate_ddl(&self, plan: &HybridPlan, config: &EtlConfig) -> Result<Vec<String>, String>;

    /// The document-shaped ingest SELECT for `plan.ingests[index]`.
    /// Column order MUST equal the destination DDL order — the executor
    /// preflights by name, order-insensitively, while the write is
    /// positional (`INSERT INTO dest SELECT *`), so this ordering is the
    /// generator's own invariant, asserted by the plan-check.
    fn ingest_select_sql(&self, plan: &HybridPlan, index: usize, config: &EtlConfig) -> String;

    /// The RRF search pipeline query (FR-9 parameters; read-time `doc_id`
    /// dedup).
    fn search_sql(&self, plan: &HybridPlan, config: &EtlConfig) -> String;

    /// The get-document pipeline query: one document's ordered chunks by
    /// `(source_table, source_id)`.
    fn get_document_sql(&self, plan: &HybridPlan, config: &EtlConfig) -> String;

    /// The ctx data-source fragment registering the destination.
    fn ctx_fragment(&self, config: &EtlConfig) -> String;

    /// The fully-qualified `documents` identifier for the generated job's
    /// `spec.destination.table` — engine-specific (the sqlite provider
    /// registers its file under `<catalog>.main`).
    fn destination_table(&self, config: &EtlConfig) -> String;
}

/// Resolve the config's engine to a dialect, enforcing the capability
/// matrix (FR-5): the refusal names the engine, the missing capability,
/// and the engines that would work.
pub fn resolve_dialect(config: &EtlConfig) -> Result<Box<dyn EngineDialect>, String> {
    let engine = config.destination.engine;
    let dialect: Box<dyn EngineDialect> = match engine {
        EngineKind::Sqlite => Box::new(super::dialects::sqlite::SqliteDialect),
        // M2 (mysql, okf-only) and M3 (postgres, lance) — until their
        // dialects land, the honest answer is the capability refusal, not
        // a partial render.
        EngineKind::Postgres | EngineKind::Lance | EngineKind::Mysql => {
            return Err(format!(
                "destination engine '{}' is not supported yet ({}); engines that work \
                 today: sqlite",
                engine.as_str(),
                match engine {
                    EngineKind::Postgres => "milestone 3",
                    EngineKind::Lance => "milestone 3, okf only",
                    EngineKind::Mysql => "milestone 2, okf only",
                    EngineKind::Sqlite => unreachable!(),
                }
            ));
        }
    };

    let caps = dialect.capabilities();
    match config.format {
        TargetFormatKind::HybridSearch if !(caps.fts && caps.knn) => Err(format!(
            "engine '{}' cannot serve format 'hybrid_search' (missing {}); engines that \
             can: sqlite",
            engine.as_str(),
            if caps.fts { "knn" } else { "fts" },
        )),
        TargetFormatKind::Okf if !caps.okf_table => Err(format!(
            "engine '{}' cannot serve format 'okf'; engines that can: sqlite",
            engine.as_str(),
        )),
        _ => Ok(dialect),
    }
}
