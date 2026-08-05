//! The four-gate valid-by-construction pipeline (design §Validation
//! Pipeline; spec 1b.6). Runs on a rendered [`Bundle`] BEFORE anything is
//! written; the first failure aborts with a targeted error.
//!
//! 1. **Loader round-trips.** Every `jobs/*.yaml` is parsed by skardi's
//!    REAL job loader and every `pipelines/*.yaml` by the real pipeline
//!    builder — what the server will read is what the generator already
//!    read. (The ctx fragment's full model lives in the server crate,
//!    which depends on THIS crate — the dependency direction forbids
//!    linking it here, so the fragment gets a structural check and the
//!    server loader exercises it end-to-end in the demo/e2e path.)
//! 2. **SQL plan-check.** A synthetic `SessionContext` — pack
//!    FieldMappings as empty MemTables under the binding's
//!    catalog.schema, the destination as the PROVIDER-derived schema,
//!    real `chunk`/`chunk_parts`/`json_pack`/`vec_to_binary`
//!    registrations, an embedding stub, and the dialect's UDTF stubs —
//!    must plan every statement. The loader round-trips above already
//!    plan through this context; ON TOP of that, the ingest SELECT's
//!    planned `(name, type)` sequence must EQUAL the destination schema's:
//!    the executor preflights by name order-insensitively while the
//!    write is positional (`INSERT INTO dest SELECT *`), so column order
//!    is this generator's own invariant — nobody else checks it.
//! 3. **Dialect DDL execution.** [`EngineDialect::validate_ddl`] — on
//!    SQLite the DDL really runs (apply → re-apply → reset → re-apply)
//!    against a throwaway in-memory connection.
//! 4. **Determinism.** The bundle is re-rendered and compared
//!    byte-for-byte — regeneration stability pinned on every generate,
//!    not just in golden tests.
//!
//! The module (like the generator) requires the `chunking` feature: the
//! ingest SQL calls `chunk_parts`, so a plan-check without the real UDF
//! would be theater.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider, TableFunctionImpl,
};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

use super::bundle::{Bundle, render_hybrid_bundle};
use super::config::{EtlConfig, TargetFormatKind};
use super::dialect::{EngineDialect, resolve_dialect};
use super::format::{HybridPlan, hybrid_plan};
use super::recipe::{ResolvedTable, find_embedded};
use crate::jobs::definition::JobDefinition;
use crate::model::chunking::ChunkingRegistry;
use crate::pipeline::pipeline::{Pipeline, StandardPipeline};
use crate::sources::providers::open_connector::source_pack::SourcePackRegistry;
use crate::sources::providers::sqlite::vec_to_binary::register_vec_to_binary_udf;
use crate::util::json_pack::register_json_pack_udf;

/// Everything `generate` produces: the validated bundle plus warnings
/// (currently only from DDL validation, e.g. vec0 shape-only).
#[derive(Debug)]
pub struct GeneratedBundle {
    pub bundle: Bundle,
    pub warnings: Vec<String>,
}

/// The full generate path: recipe lookup → resolve → plan → render →
/// validate. The bundle it returns has passed all four gates; writing it
/// is the caller's (CLI's) one remaining step.
pub async fn generate_hybrid(config: &EtlConfig) -> Result<GeneratedBundle, String> {
    generate_hybrid_with(config, None).await
}

/// [`generate_hybrid`] with an optional `--recipe` override (FR-3): a
/// user recipe loaded through the SAME parser replaces the built-in for
/// this run, and everything downstream — resolution against the real
/// pack, the four gates — treats it identically.
pub async fn generate_hybrid_with(
    config: &EtlConfig,
    recipe_override: Option<super::recipe::Recipe>,
) -> Result<GeneratedBundle, String> {
    if config.format != TargetFormatKind::HybridSearch {
        return Err(
            "format 'okf' is milestone 2; 'hybrid_search' is what generates today".to_string(),
        );
    }
    let registry = SourcePackRegistry::builtins().map_err(|e| e.to_string())?;
    let pack = registry.get(&config.source.pack).ok_or_else(|| {
        let available: Vec<&str> = registry.packs().map(|p| p.name).collect();
        format!(
            "spec.source.pack '{}' is not a built-in source pack (available: {})",
            config.source.pack,
            available.join(", ")
        )
    })?;
    let recipe = match recipe_override {
        Some(recipe) => {
            if recipe.pack != config.source.pack || recipe.format != config.format {
                return Err(format!(
                    "--recipe covers pack '{}' / format '{}', but the config asks for \
                     '{}' / '{}'",
                    recipe.pack,
                    recipe.format.as_str(),
                    config.source.pack,
                    config.format.as_str()
                ));
            }
            recipe
        }
        None => find_embedded(&config.source.pack, config.format)?.ok_or_else(|| {
            format!(
                "no embedded recipe covers pack '{}' with format '{}' — pass --recipe <file> \
                 or pick a covered pack (`skardi-etl recipes` lists coverage)",
                config.source.pack,
                config.format.as_str()
            )
        })?,
    };
    let resolved = recipe.resolve(pack)?;

    // config.tables ∩ recipe, in config order; empty = the recipe's full set.
    let selected: Vec<ResolvedTable> = if config.source.tables.is_empty() {
        resolved
    } else {
        config
            .source
            .tables
            .iter()
            .map(|want| {
                resolved
                    .iter()
                    .find(|t| &t.short_name == want)
                    .cloned()
                    .ok_or_else(|| {
                        let known: Vec<&str> =
                            resolved.iter().map(|t| t.short_name.as_str()).collect();
                        format!(
                            "spec.source.tables entry '{want}' is not in the '{}' recipe \
                             (recipe tables: {})",
                            config.source.pack,
                            known.join(", ")
                        )
                    })
            })
            .collect::<Result<_, _>>()?
    };

    let dialect = resolve_dialect(config)?;
    let plan = hybrid_plan(config, &selected)?;
    let bundle = render_hybrid_bundle(config, &plan, dialect.as_ref())?;
    let warnings = validate_bundle(&bundle, config, &plan, &selected, dialect.as_ref()).await?;
    Ok(GeneratedBundle { bundle, warnings })
}

/// Run all four gates on a rendered bundle. Returns validation warnings;
/// any hard failure is a targeted `Err`.
pub async fn validate_bundle(
    bundle: &Bundle,
    config: &EtlConfig,
    plan: &HybridPlan,
    tables: &[ResolvedTable],
    dialect: &dyn EngineDialect,
) -> Result<Vec<String>, String> {
    let ctx = synthetic_context(config, tables, dialect)?;

    // ── Gate 1: loader round-trips (which plan through the synthetic ctx,
    // so gate 2's "every statement plans" rides along).
    let staged = stage_for_loaders(bundle)?;
    let round_trip = round_trip_loaders(&staged.dir, bundle, Arc::clone(&ctx)).await;
    staged.cleanup();
    round_trip?;

    validate_ctx_fragment_shape(bundle, config)?;

    // ── Gate 2: the ingest SELECT's planned (name, type) order must equal
    // the provider-derived destination schema — the positional-INSERT
    // invariant, on planned schemas.
    let expected = dialect.planned_destination_schema(config);
    for (index, ingest) in plan.ingests.iter().enumerate() {
        let sql = dialect.ingest_select_sql(plan, index, config);
        let planned = planned_fields(&ctx, &sql).await.map_err(|e| {
            format!(
                "plan-check: the '{}' ingest SELECT failed to plan: {e}",
                ingest.source_table
            )
        })?;
        assert_field_order(&ingest.source_table, &planned, &expected)?;
    }

    // ── Gate 3: the DDL really executes where the engine supports it.
    let warnings = dialect.validate_ddl(plan, config)?;

    // ── Gate 4: regeneration determinism, checked on every generate.
    let again = render_hybrid_bundle(config, plan, dialect)?;
    if again.files() != bundle.files() {
        return Err(
            "determinism violation: re-rendering the same config produced different bytes \
             — this is a generator bug (map-order or timestamp nondeterminism)"
                .to_string(),
        );
    }

    Ok(warnings)
}

// ─── The synthetic SessionContext ────────────────────────────────────────

/// Build the plan-check context: real UDF registrations, stub embedding +
/// UDTFs, empty MemTables for the pack tables and the destination.
fn synthetic_context(
    config: &EtlConfig,
    tables: &[ResolvedTable],
    dialect: &dyn EngineDialect,
) -> Result<Arc<SessionContext>, String> {
    let mut ctx = SessionContext::new();

    // Real registrations — the same code paths the server registers.
    Arc::new(ChunkingRegistry::new()).register_chunk_udf(&mut ctx);
    register_json_pack_udf(&mut ctx);
    register_vec_to_binary_udf(&mut ctx);
    // The embedding stub: same name + return type as the real UDF, marked
    // volatile so constant folding never tries to invoke it (the search
    // query's `candle('model', NULL)` is all-literal after placeholder
    // substitution).
    ctx.register_udf(ScalarUDF::new_from_impl(EmbeddingStubUDF::new()));

    for (name, schema) in dialect.udtf_stubs(config) {
        ctx.register_udtf(name, Arc::new(StubTableFunction { schema }));
    }

    // Source tables: the pack's full FieldMapping schema, empty, under the
    // binding's catalog.schema.
    let source_schema = Arc::new(MemorySchemaProvider::new());
    for table in tables {
        let fields: Vec<Field> = table
            .table
            .fields
            .iter()
            .map(|f| Field::new(f.name, f.field_type.arrow_type(), f.nullable))
            .collect();
        let mem = MemTable::try_new(
            Arc::new(arrow::datatypes::Schema::new(fields)),
            vec![vec![]],
        )
        .map_err(|e| format!("plan-check: source table '{}': {e}", table.short_name))?;
        source_schema
            .register_table(table.short_name.clone(), Arc::new(mem))
            .map_err(|e| e.to_string())?;
    }
    let source_catalog = MemoryCatalogProvider::new();
    source_catalog
        .register_schema(&config.source.binding_schema, source_schema)
        .map_err(|e| e.to_string())?;
    ctx.register_catalog(&config.source.binding_catalog, Arc::new(source_catalog));

    // The destination, exactly as the engine's provider will expose it —
    // registered under the dialect's own qualification.
    let qualified = dialect.destination_table(config);
    let mut parts = qualified.split('.');
    let (Some(cat), Some(sch), Some(tbl), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(format!(
            "dialect destination_table must be catalog.schema.table, got '{qualified}'"
        ));
    };
    let dest_mem = MemTable::try_new(dialect.planned_destination_schema(config), vec![vec![]])
        .map_err(|e| format!("plan-check: destination table: {e}"))?;
    let dest_schema = Arc::new(MemorySchemaProvider::new());
    dest_schema
        .register_table(tbl.to_string(), Arc::new(dest_mem))
        .map_err(|e| e.to_string())?;
    let dest_catalog = MemoryCatalogProvider::new();
    dest_catalog
        .register_schema(sch, dest_schema)
        .map_err(|e| e.to_string())?;
    ctx.register_catalog(cat, Arc::new(dest_catalog));

    Ok(Arc::new(ctx))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct EmbeddingStubUDF {
    signature: Signature,
}

impl EmbeddingStubUDF {
    fn new() -> Self {
        Self {
            // Volatile: keeps the constant folder from invoking the stub.
            signature: Signature::variadic_any(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for EmbeddingStubUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "candle"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        // The real candle UDF's planned type: List<Float32>.
        Ok(DataType::List(Arc::new(Field::new_list_field(
            DataType::Float32,
            true,
        ))))
    }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Err(DataFusionError::Internal(
            "the plan-check embedding stub is never executed".to_string(),
        ))
    }
}

/// A UDTF stub: any argument list plans to an empty table with the
/// dialect-declared schema.
#[derive(Debug)]
struct StubTableFunction {
    schema: SchemaRef,
}

impl TableFunctionImpl for StubTableFunction {
    fn call(&self, _args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        Ok(Arc::new(MemTable::try_new(
            Arc::clone(&self.schema),
            vec![vec![]],
        )?))
    }
}

// ─── Gate 1 plumbing ─────────────────────────────────────────────────────

struct StagedBundle {
    dir: std::path::PathBuf,
}

impl StagedBundle {
    fn cleanup(&self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// The real loaders take file paths; stage the YAML artifacts in a unique
/// temp directory for the duration of gate 1.
fn stage_for_loaders(bundle: &Bundle) -> Result<StagedBundle, String> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "skardi-etl-validate-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    for (rel, contents) in bundle.files() {
        if !rel.ends_with(".yaml") {
            continue;
        }
        let dest = dir.join(rel);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("stage gate 1: {e}"))?;
        }
        std::fs::write(&dest, contents).map_err(|e| format!("stage gate 1: {e}"))?;
    }
    Ok(StagedBundle { dir })
}

async fn round_trip_loaders(
    dir: &std::path::Path,
    bundle: &Bundle,
    ctx: Arc<SessionContext>,
) -> Result<(), String> {
    for rel in bundle.files().keys() {
        let path = dir.join(rel);
        if rel.starts_with("jobs/") {
            let job = JobDefinition::load_from_file(&path, Arc::clone(&ctx))
                .await
                .map_err(|e| format!("gate 1: the real job loader rejected '{rel}': {e:#}"))?;
            if job.is_none() {
                return Err(format!(
                    "gate 1: '{rel}' is not `kind: job` — the job loader skipped it"
                ));
            }
        } else if rel.starts_with("pipelines/") {
            StandardPipeline::load_from_file(&path, Arc::clone(&ctx))
                .await
                .map_err(|e| format!("gate 1: the real pipeline loader rejected '{rel}': {e:#}"))?;
        }
    }
    Ok(())
}

/// Structural check of the ctx fragment (see the module doc for why the
/// full server-side model can't be linked from here).
fn validate_ctx_fragment_shape(bundle: &Bundle, config: &EtlConfig) -> Result<(), String> {
    let fragment = bundle
        .files()
        .get("ctx.fragment.yaml")
        .ok_or("gate 1: bundle is missing ctx.fragment.yaml")?;
    let value: serde_yaml::Value = serde_yaml::from_str(fragment)
        .map_err(|e| format!("gate 1: ctx.fragment.yaml is not valid YAML: {e}"))?;
    let sources = value
        .get("spec")
        .and_then(|s| s.get("data_sources"))
        .and_then(|d| d.as_sequence())
        .ok_or("gate 1: ctx.fragment.yaml must carry spec.data_sources")?;
    let entry = sources
        .first()
        .ok_or("gate 1: ctx.fragment.yaml has an empty spec.data_sources")?;
    let name = entry.get("name").and_then(|n| n.as_str());
    if name != Some(config.destination.catalog.as_str()) {
        return Err(format!(
            "gate 1: ctx fragment data source name {name:?} != destination catalog '{}'",
            config.destination.catalog
        ));
    }
    for key in ["type", "access_mode"] {
        if entry.get(key).is_none() {
            return Err(format!("gate 1: ctx fragment data source lacks '{key}'"));
        }
    }
    Ok(())
}

// ─── Gate 2 plumbing ─────────────────────────────────────────────────────

/// Plan a generated statement (placeholders → NULL, the same substitution
/// the pipeline inferencer applies) and return its planned fields in
/// order.
async fn planned_fields(
    ctx: &SessionContext,
    sql: &str,
) -> Result<Vec<(String, DataType)>, String> {
    let pattern = regex::Regex::new(r"\{[a-zA-Z_][a-zA-Z0-9_]*\}")
        .map_err(|e| format!("placeholder regex: {e}"))?;
    let nulled = pattern.replace_all(sql, "NULL");
    let df = ctx.sql(&nulled).await.map_err(|e| e.to_string())?;
    Ok(df
        .schema()
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect())
}

fn assert_field_order(
    table: &str,
    planned: &[(String, DataType)],
    expected: &SchemaRef,
) -> Result<(), String> {
    let want: Vec<(String, DataType)> = expected
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect();
    if planned != want {
        let planned_desc: Vec<String> = planned.iter().map(|(n, t)| format!("{n}: {t}")).collect();
        let want_desc: Vec<String> = want.iter().map(|(n, t)| format!("{n}: {t}")).collect();
        return Err(format!(
            "plan-check: the '{table}' ingest SELECT's planned columns do not equal the \
             destination DDL's, in order — the write is positional (INSERT INTO dest \
             SELECT *), so this WOULD corrupt data silently.\n  planned:  [{}]\n  \
             expected: [{}]",
            planned_desc.join(", "),
            want_desc.join(", ")
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::etl::bundle::slug;

    const FLAGSHIP: &str = r#"
kind: etl
metadata:
  name: github-issues-search
spec:
  source: { pack: github, binding: saas.github_demo, tables: [issues] }
  format: hybrid_search
  destination: { type: sqlite, path: data/gh.db, catalog: gh_search }
  embedding: { udf: candle, model: models/generated/bge-small-en-v1.5, dimensions: 384 }
  chunking: { splitter: markdown, size: 1200, overlap: 200 }
"#;

    #[tokio::test]
    async fn the_flagship_config_generates_a_fully_validated_bundle() {
        let config = EtlConfig::from_yaml(FLAGSHIP).unwrap();
        let generated = generate_hybrid(&config).await.unwrap_or_else(|e| {
            panic!("the flagship must pass all four gates:\n{e}");
        });
        // The PRD tree came out the other side…
        assert_eq!(
            generated.bundle.files().len(),
            6,
            "{}",
            generated.bundle.tree()
        );
        // …and the only warning is the expected vec0 shape-only note (no
        // sqlite-vec extension is configured here).
        assert_eq!(generated.warnings.len(), 1, "{:?}", generated.warnings);
        assert!(
            generated.warnings[0].contains("vec0"),
            "{:?}",
            generated.warnings
        );
    }

    #[tokio::test]
    async fn the_mock_recipe_generates_too() {
        let config = EtlConfig::from_yaml(
            &FLAGSHIP
                .replace("pack: github", "pack: mock")
                .replace("binding: saas.github_demo", "binding: saas.mock_demo")
                .replace("tables: [issues]", "tables: [items]")
                .replace("name: github-issues-search", "name: mock-items-search"),
        )
        .unwrap();
        let generated = generate_hybrid(&config).await.unwrap();
        assert!(
            generated
                .bundle
                .files()
                .contains_key("jobs/mock-items-search-ingest-items.yaml"),
            "{}",
            generated.bundle.tree()
        );
    }

    #[tokio::test]
    async fn a_wrong_table_and_a_wrong_pack_fail_with_targeted_errors() {
        let config =
            EtlConfig::from_yaml(&FLAGSHIP.replace("[issues]", "[pull_requests]")).unwrap();
        let err = generate_hybrid(&config).await.unwrap_err();
        assert!(
            err.contains("'pull_requests' is not in the 'github' recipe"),
            "{err}"
        );
        assert!(err.contains("issues"), "must list what IS available: {err}");

        let config = EtlConfig::from_yaml(&FLAGSHIP.replace("pack: github", "pack: jira")).unwrap();
        let err = generate_hybrid(&config).await.unwrap_err();
        assert!(
            err.contains("'jira' is not a built-in source pack"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn the_order_assertion_catches_a_swapped_projection() {
        // Sabotage: swap two SELECT columns. Everything still PLANS (both
        // are Utf8-typed), the executor's name-keyed preflight would still
        // pass — only this gate stands between the swap and silent data
        // corruption.
        let config = EtlConfig::from_yaml(FLAGSHIP).unwrap();
        let dialect = resolve_dialect(&config).unwrap();
        let registry = SourcePackRegistry::builtins().unwrap();
        let recipe = find_embedded("github", TargetFormatKind::HybridSearch)
            .unwrap()
            .unwrap();
        let resolved = recipe.resolve(registry.get("github").unwrap()).unwrap();
        let plan = hybrid_plan(&config, &resolved).unwrap();
        let sabotaged_sql = dialect
            .ingest_select_sql(&plan, 0, &config)
            .replace("AS source_table", "AS __tmp")
            .replace("AS source_id", "AS source_table")
            .replace("AS __tmp", "AS source_id");
        let ctx = synthetic_context(&config, &resolved, dialect.as_ref()).unwrap();
        let planned = planned_fields(&ctx, &sabotaged_sql).await.unwrap();
        let err = assert_field_order(
            "issues",
            &planned,
            &dialect.planned_destination_schema(&config),
        )
        .unwrap_err();
        assert!(err.contains("positional"), "{err}");
    }

    #[test]
    fn slugged_names_thread_through_generation() {
        // A lossy metadata.name must still produce one consistent slug
        // everywhere (file names AND metadata.name inside them) — checked
        // here at the slug level; the golden test (1d) pins the rest.
        let s = slug("My ETL_Config");
        assert!(s.starts_with("my-etl-config-"), "{s}");
    }

    // ── 1d.2: the plan-check must be able to FAIL, not just pass ────────

    async fn flagship_plan_ctx() -> (EtlConfig, HybridPlan, Arc<SessionContext>, String) {
        let config = EtlConfig::from_yaml(FLAGSHIP).unwrap();
        let dialect = resolve_dialect(&config).unwrap();
        let registry = SourcePackRegistry::builtins().unwrap();
        let recipe = find_embedded("github", TargetFormatKind::HybridSearch)
            .unwrap()
            .unwrap();
        let resolved = recipe.resolve(registry.get("github").unwrap()).unwrap();
        let plan = hybrid_plan(&config, &resolved).unwrap();
        let ctx = synthetic_context(&config, &resolved, dialect.as_ref()).unwrap();
        let sql = dialect.ingest_select_sql(&plan, 0, &config);
        (config, plan, ctx, sql)
    }

    #[tokio::test]
    async fn a_typoed_column_fails_the_plan_check() {
        let (_config, _plan, ctx, sql) = flagship_plan_ctx().await;
        let sabotaged = sql.replace("author_login", "author_logn");
        let err = planned_fields(&ctx, &sabotaged).await.unwrap_err();
        assert!(
            err.contains("author_logn"),
            "names the missing column: {err}"
        );
    }

    #[tokio::test]
    async fn the_unplannable_unnest_spelling_fails_the_plan_check() {
        // The exact regression the plannability pin guards: DataFusion
        // cannot plan `UNNEST … WITH ORDINALITY` (apache/datafusion#11419).
        // If someone "simplifies" the template back to it, this gate is
        // what fails.
        let (_config, _plan, ctx, sql) = flagship_plan_ctx().await;
        let sabotaged = sql.replace(") AS part", ") WITH ORDINALITY AS part");
        assert_ne!(sabotaged, sql, "mutation must apply");
        planned_fields(&ctx, &sabotaged)
            .await
            .expect_err("WITH ORDINALITY must not plan");
    }

    // ── 1d.1: golden bundles ─────────────────────────────────────────────

    /// Compare a freshly generated bundle to its checked-in golden copy,
    /// both directions (missing + unexpected files). Regenerate with:
    /// `UPDATE_ETL_GOLDEN=1 cargo test -p skardi --lib --features chunking golden`
    async fn assert_matches_golden(config_yaml: &str, golden_name: &str) {
        let config = EtlConfig::from_yaml(config_yaml).unwrap();
        let generated = generate_hybrid(&config).await.unwrap();
        let golden_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/etl/testdata/golden")
            .join(golden_name);

        if std::env::var("UPDATE_ETL_GOLDEN").is_ok() {
            let _ = std::fs::remove_dir_all(&golden_dir);
            for (rel, contents) in generated.bundle.files() {
                let dest = golden_dir.join(rel);
                std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
                std::fs::write(dest, contents).unwrap();
            }
            return;
        }

        for (rel, contents) in generated.bundle.files() {
            let golden_path = golden_dir.join(rel);
            let golden = std::fs::read_to_string(&golden_path).unwrap_or_else(|e| {
                panic!(
                    "golden file missing for generated '{rel}' ({e}) — if the change is \
                     intentional, regenerate with UPDATE_ETL_GOLDEN=1"
                )
            });
            assert_eq!(
                contents, &golden,
                "'{rel}' drifted from its golden — intentional changes regenerate with \
                 UPDATE_ETL_GOLDEN=1"
            );
        }
        // No stale goldens either.
        for entry in walk_files(&golden_dir) {
            let rel = entry
                .strip_prefix(&golden_dir)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            assert!(
                generated.bundle.files().contains_key(rel.as_str()),
                "golden '{rel}' has no generated counterpart — remove it or regenerate \
                 with UPDATE_ETL_GOLDEN=1"
            );
        }
    }

    fn walk_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut out = Vec::new();
        let Ok(entries) = std::fs::read_dir(dir) else {
            return out;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(walk_files(&path));
            } else {
                out.push(path);
            }
        }
        out
    }

    #[tokio::test]
    async fn golden_github_issues_hybrid_sqlite() {
        assert_matches_golden(FLAGSHIP, "github-issues-search").await;
    }

    #[tokio::test]
    async fn golden_mock_items_hybrid_sqlite() {
        let config = FLAGSHIP
            .replace("pack: github", "pack: mock")
            .replace("binding: saas.github_demo", "binding: saas.mock_demo")
            .replace("tables: [issues]", "tables: [items]")
            .replace("name: github-issues-search", "name: mock-items-search");
        assert_matches_golden(&config, "mock-items-search").await;
    }
}
