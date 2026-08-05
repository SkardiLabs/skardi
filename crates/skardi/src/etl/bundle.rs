//! Bundle assembly and the atomic write (design §bundle.rs; PRD §6.2).
//!
//! A [`Bundle`] is a `BTreeMap<relative path, file contents>` — BTreeMap
//! so every iteration (writing, printing the tree, golden comparison) is
//! deterministically ordered. Assembly renders the PRD §6.2 tree:
//!
//! ```text
//! setup.sql                          # dialect DDL
//! jobs/<slug>-ingest-<table>.yaml    # one kind: job per recipe table
//! pipelines/<slug>-search-hybrid.yaml
//! pipelines/<slug>-get-document.yaml
//! ctx.fragment.yaml                  # data-source entry to merge
//! README.md                          # the five-step path + refresh guidance
//! ```
//!
//! Naming: ONE slug function ([`slug`]) feeds every artifact name — file
//! names, job/pipeline `metadata.name`s — so bundles sharing a `--jobs`
//! directory can't collide (FR-10). Normalization to `[a-z0-9-]` is lossy
//! (`foo_bar` and `foo-bar` both normalize to `foo-bar`), so whenever it
//! ALTERS the input, a 6-hex BLAKE3 suffix of the ORIGINAL name is
//! appended — distinct configured names always yield distinct artifacts.
//!
//! Writing is atomic at the directory level: stage into a sibling
//! `<out>.etl-tmp-<pid>` (same filesystem ⇒ the swap is a rename), rename
//! any existing `out` to `<out>.etl-bak-<pid>` (only ever reached under
//! `--force` or for an empty dir), rename the staging dir into place, and
//! remove the backup LAST. The crash state space, honestly: old bundle
//! intact (swap not reached); new bundle plus a leftover `.etl-bak-*`;
//! or — between the two renames — no `out` at all with the complete old
//! bundle in `.etl-bak-<pid>`, recoverable by one rename (the README's
//! troubleshooting note says so). Never a half-written `out`.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Component, Path, PathBuf};

use super::config::EtlConfig;
use super::dialect::EngineDialect;
use super::format::HybridPlan;

/// The in-memory bundle: relative path → file contents, deterministically
/// ordered.
#[derive(Debug, Clone, Default)]
pub struct Bundle {
    files: BTreeMap<String, String>,
}

impl Bundle {
    pub fn files(&self) -> &BTreeMap<String, String> {
        &self.files
    }

    /// Add one file. The path must be relative and stay inside the bundle
    /// (no `..`, no absolute components) — enforced here so `write` never
    /// has to trust its inputs.
    pub fn insert(&mut self, rel_path: &str, contents: String) -> Result<(), String> {
        let p = Path::new(rel_path);
        let sane =
            !rel_path.is_empty() && p.components().all(|c| matches!(c, Component::Normal(_)));
        if !sane {
            return Err(format!(
                "bundle paths must be plain relative paths inside the bundle, got '{rel_path}'"
            ));
        }
        if self.files.insert(rel_path.to_string(), contents).is_some() {
            return Err(format!(
                "bundle path '{rel_path}' rendered twice — artifact names must be unique \
                 (slug collision?)"
            ));
        }
        Ok(())
    }

    /// The bundle tree as `generate` prints it — one path per line, in
    /// deterministic order.
    pub fn tree(&self) -> String {
        self.files
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Atomically write the bundle to `out_dir` (see the module doc for
    /// the staging/swap protocol and its crash window). Refuses a
    /// non-empty `out_dir` unless `force` — hand-edited bundles are never
    /// silently clobbered (PRD §6.2).
    pub fn write(&self, out_dir: &Path, force: bool) -> Result<(), String> {
        let exists = out_dir.exists();
        if exists {
            let non_empty = fs::read_dir(out_dir)
                .map_err(|e| format!("read output directory '{}': {e}", out_dir.display()))?
                .next()
                .is_some();
            if non_empty && !force {
                return Err(format!(
                    "output directory '{}' is not empty; pass --force to replace it (the \
                     old bundle is kept as a sibling backup until the swap completes)",
                    out_dir.display()
                ));
            }
        }

        let name = out_dir
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| format!("output directory '{}' has no name", out_dir.display()))?;
        let parent = match out_dir.parent() {
            Some(p) if p.as_os_str().is_empty() => PathBuf::from("."),
            Some(p) => p.to_path_buf(),
            None => PathBuf::from("."),
        };
        let pid = std::process::id();
        let tmp = parent.join(format!("{name}.etl-tmp-{pid}"));
        let bak = parent.join(format!("{name}.etl-bak-{pid}"));

        // A directory already at OUR tmp/bak name is a leftover from a
        // crashed earlier run of this same pid namespace — stale by
        // construction, safe to clear.
        for stale in [&tmp, &bak] {
            if stale.exists() {
                fs::remove_dir_all(stale)
                    .map_err(|e| format!("clear stale '{}': {e}", stale.display()))?;
            }
        }

        // Stage everything before touching out_dir.
        for (rel, contents) in &self.files {
            let dest = tmp.join(rel);
            if let Some(dir) = dest.parent() {
                fs::create_dir_all(dir).map_err(|e| format!("stage '{}': {e}", dir.display()))?;
            }
            fs::write(&dest, contents).map_err(|e| format!("stage '{}': {e}", dest.display()))?;
        }

        // The swap.
        if exists {
            fs::rename(out_dir, &bak).map_err(|e| {
                format!(
                    "back up existing '{}' to '{}': {e} (nothing was changed)",
                    out_dir.display(),
                    bak.display()
                )
            })?;
        }
        fs::rename(&tmp, out_dir).map_err(|e| {
            format!(
                "swap staged bundle into '{}': {e}. Your previous bundle is intact at \
                 '{}' — recover it with: mv '{}' '{}'",
                out_dir.display(),
                bak.display(),
                bak.display(),
                out_dir.display()
            )
        })?;
        // Backup removed last: from here every earlier state is recovered.
        if bak.exists() {
            fs::remove_dir_all(&bak).map_err(|e| {
                format!(
                    "bundle written, but removing the backup '{}' failed: {e} — remove it \
                     by hand",
                    bak.display()
                )
            })?;
        }
        Ok(())
    }
}

/// THE slug function (FR-10): `metadata.name` → `[a-z0-9-]`, feeding
/// every artifact name. Lossy normalization appends a 6-hex BLAKE3
/// suffix of the ORIGINAL input so distinct names stay distinct.
pub fn slug(name: &str) -> String {
    let mut normalized = String::new();
    for c in name.to_lowercase().chars() {
        if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' {
            normalized.push(c);
        } else if !normalized.ends_with('-') {
            normalized.push('-');
        }
    }
    let trimmed = normalized.trim_matches('-');
    if trimmed == name {
        return trimmed.to_string();
    }
    let hash = blake3::hash(name.as_bytes()).to_hex();
    let stem = if trimmed.is_empty() { "etl" } else { trimmed };
    format!("{stem}-{}", &hash.as_str()[..6])
}

/// Render the full hybrid-search bundle (PRD §6.2 tree). Pure assembly:
/// validation (loader round-trips, plan-check, DDL execution) is
/// `validate.rs`'s job and runs on the returned bundle before any write.
pub fn render_hybrid_bundle(
    config: &EtlConfig,
    plan: &HybridPlan,
    dialect: &dyn EngineDialect,
) -> Result<Bundle, String> {
    let slug = slug(&config.name);
    let mut bundle = Bundle::default();

    bundle.insert("setup.sql", dialect.setup_sql(plan, config))?;
    bundle.insert("ctx.fragment.yaml", dialect.ctx_fragment(config))?;

    for (index, ingest) in plan.ingests.iter().enumerate() {
        let job_name = format!("{slug}-ingest-{}", ingest.source_table);
        let sql = dialect.ingest_select_sql(plan, index, config);
        bundle.insert(
            &format!("jobs/{job_name}.yaml"),
            render_job_yaml(
                &job_name,
                ingest.incremental,
                &sql,
                &dialect.destination_table(config),
            ),
        )?;
    }

    let search_name = format!("{slug}-search-hybrid");
    bundle.insert(
        &format!("pipelines/{search_name}.yaml"),
        render_pipeline_yaml(
            &search_name,
            "RRF hybrid search over the generated documents index. Parameters: \
             {query} (embedded for the vector arm), {text_query} (FTS), \
             {vector_weight}, {text_weight} (RRF weights, e.g. 0.5), \
             {limit}.",
            &dialect.search_sql(plan, config),
        ),
    )?;

    let get_doc_name = format!("{slug}-get-document");
    bundle.insert(
        &format!("pipelines/{get_doc_name}.yaml"),
        render_pipeline_yaml(
            &get_doc_name,
            "Fetch one document's chunks, ordered by chunk_index, by the \
             (source_table, source_id) pair every search hit returns — \
             full-document reassembly and neighbor-chunk context in one call.",
            &dialect.get_document_sql(plan, config),
        ),
    )?;

    bundle.insert("README.md", render_readme(config, plan, &slug))?;
    Ok(bundle)
}

/// Indent every non-empty line — YAML block-scalar embedding.
fn indent(text: &str, pad: &str) -> String {
    let mut out = String::new();
    for line in text.lines() {
        if !line.is_empty() {
            out.push_str(pad);
            out.push_str(line);
        }
        out.push('\n');
    }
    out
}

fn render_job_yaml(name: &str, incremental: bool, sql: &str, dest_table: &str) -> String {
    let refresh = if incremental {
        "re-run with -p since=<last watermark> for incremental refresh (overlap is \
         safe; the search pipeline deduplicates)"
    } else {
        "the source exposes no timestamp pushdown, so refresh = setup --reset + full re-run"
    };
    format!(
        "kind: job\n\
         metadata:\n\
         \x20 name: {name}\n\
         \x20 version: \"1.0.0\"\n\
         \x20 description: >-\n\
         \x20   Generated by skardi-etl: chunk + embed one source table into the\n\
         \x20   documents index. Append-only; {refresh}.\n\
         spec:\n\
         \x20 query: |\n\
         {sql}\
         \x20 destination:\n\
         \x20   table: {dest_table}\n\
         \x20   mode: append\n\
         \x20 execution: {{}}\n",
        sql = indent(sql, "    "),
    )
}

fn render_pipeline_yaml(name: &str, description: &str, sql: &str) -> String {
    format!(
        "kind: pipeline\n\
         metadata:\n\
         \x20 name: {name}\n\
         \x20 version: \"1.0.0\"\n\
         \x20 description: >-\n\
         {description}\
         spec:\n\
         \x20 query: |\n\
         {sql}",
        description = indent(description, "    "),
        sql = indent(sql, "    "),
    )
}

fn render_readme(config: &EtlConfig, plan: &HybridPlan, slug: &str) -> String {
    let catalog = &config.destination.catalog;
    let dest_path = config.destination.path.as_deref().unwrap_or_default();
    let model = &plan.search.embedding_model;

    let mut job_runs = String::new();
    let mut refresh_notes = String::new();
    for ingest in &plan.ingests {
        let job = format!("{slug}-ingest-{}", ingest.source_table);
        job_runs.push_str(&format!("skardi job run {job} -p limit=500\n"));
        if ingest.incremental {
            refresh_notes.push_str(&format!(
                "- `{job}` is incremental: pass `-p since=<ISO-8601 watermark>` to load only \
                 rows updated since. Choose the watermark to OVERLAP the last run — duplicates \
                 accumulate harmlessly (the search pipeline deduplicates by `doc_id` at read \
                 time); a gap loses rows. `{{limit}}` stays as the first-backfill bound.\n"
            ));
        } else {
            refresh_notes.push_str(&format!(
                "- `{job}` is full-load (its pack exposes no timestamp pushdown): refresh = \
                 `skardi-etl setup --reset` + re-run. `{{limit}}` bounds each run.\n"
            ));
        }
    }

    format!(
        "# {name} — generated skardi bundle\n\
         \n\
         Generated by `skardi-etl` from a `kind: etl` config. Everything here is a\n\
         plain skardi artifact — read it, edit it, version-control it. Regenerating\n\
         with the same config reproduces this bundle byte-for-byte; `generate`\n\
         refuses to overwrite a non-empty directory without `--force`.\n\
         \n\
         ## Contents\n\
         \n\
         - `setup.sql` — destination DDL: the `documents` table plus the engine's\n\
         \x20 search artifacts and sync triggers. Idempotent (`IF NOT EXISTS`).\n\
         - `jobs/` — one `kind: job` per source table (chunk + embed inline in SQL).\n\
         - `pipelines/` — `{slug}-search-hybrid` (RRF) and `{slug}-get-document`.\n\
         - `ctx.fragment.yaml` — the data-source entry to merge into your `ctx.yaml`.\n\
         \n\
         ## Run it (five steps)\n\
         \n\
         ```bash\n\
         # 1. Apply the destination DDL (idempotent; --reset rebuilds).\n\
         #    sqlite-vec must be loadable for the vec0 table:\n\
         #    export SQLITE_VEC_PATH=/absolute/path/to/vec0.dylib\n\
         skardi-etl setup -f setup.sql --dest {dest_path}\n\
         \n\
         # 2. Merge ctx.fragment.yaml's data_sources entry into your ctx.yaml.\n\
         \n\
         # 3. Serve the bundle.\n\
         skardi-server --ctx ctx.yaml --jobs jobs/ --pipeline pipelines/\n\
         \n\
         # 4. Ingest (append-only; {{limit}} bounds each run).\n\
         {job_runs}\
         \n\
         # 5. Search.\n\
         skardi run {slug}-search-hybrid \\\n\
         \x20 -p query=\"vector databases\" -p text_query=\"vector databases\" \\\n\
         \x20 -p vector_weight=0.5 -p text_weight=0.5 -p limit=10\n\
         # Every hit carries (source_table, source_id) — fetch the whole document:\n\
         skardi run {slug}-get-document -p source_table=<t> -p source_id=<id>\n\
         ```\n\
         \n\
         The query vector embeds with `{model}` — the same model the ingest jobs\n\
         used; changing the embedding config is a rebuild, not an edit.\n\
         \n\
         ## Refresh\n\
         \n\
         The v1 refresh model is rebuild-first: `skardi-etl setup -f setup.sql \\\n\
         --dest {dest_path} --reset` drops every bundle-owned artifact and\n\
         re-applies the DDL; then re-run the jobs.\n\
         \n\
         {refresh_notes}\
         \n\
         `doc_id` (`<source_table>:<source_id>:<chunk_index>`) is deterministic but\n\
         deliberately NOT unique in v1 — replay accumulates rows instead of\n\
         hard-failing mid-job, and reads deduplicate. A chunking-config change moves\n\
         chunk boundaries: that is a rebuild.\n\
         \n\
         ## Troubleshooting\n\
         \n\
         - **`generate --force` crashed mid-swap?** Your previous bundle is intact\n\
         \x20 in a sibling `<out>.etl-bak-<pid>` directory; recover it with a single\n\
         \x20 rename (`mv <out>.etl-bak-<pid> <out>`). There is never a half-written\n\
         \x20 output directory.\n\
         - **`no such module: vec0`** — the sqlite-vec extension isn't loadable;\n\
         \x20 set the extension env var (see `ctx.fragment.yaml`) and re-apply setup.\n\
         - **Search returns stale/duplicate-looking rows** — expected under\n\
         \x20 incremental overlap; reads keep each chunk's best copy. `--reset` +\n\
         \x20 re-run clears history completely.\n\
         \n\
         Destination: `{catalog}` → `{dest_path}`.\n",
        name = config.name,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::etl::config::TargetFormatKind;
    use crate::etl::dialect::resolve_dialect;
    use crate::etl::format::hybrid_plan;
    use crate::etl::recipe::find_embedded;
    use crate::sources::providers::open_connector::source_pack::SourcePackRegistry;

    fn flagship_bundle() -> Bundle {
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
        let dialect = resolve_dialect(&config).unwrap();
        render_hybrid_bundle(&config, &plan, dialect.as_ref()).unwrap()
    }

    #[test]
    fn the_slug_function_is_identity_on_conforming_names_and_suffixes_lossy_ones() {
        // Conforming: passes through untouched — the flagship keeps its name.
        assert_eq!(slug("github-issues-search"), "github-issues-search");
        assert_eq!(slug("a1-b2"), "a1-b2");

        // Lossy: normalized + 6-hex BLAKE3 of the ORIGINAL, so the two
        // designs-cited colliders stay distinct.
        let a = slug("foo_bar");
        let b = slug("foo-bar");
        assert!(
            a.starts_with("foo-bar-") && a.len() == "foo-bar-".len() + 6,
            "{a}"
        );
        assert_eq!(b, "foo-bar");
        assert_ne!(a, b);

        // Case is normalization too (names differing only by case must not
        // collide silently).
        assert_ne!(slug("Foo"), slug("foo"));
        assert_eq!(slug("foo"), "foo");

        // Degenerate input still yields a usable stem.
        assert!(slug("___").starts_with("etl-"), "{}", slug("___"));
    }

    #[test]
    fn the_bundle_is_exactly_the_prd_tree() {
        let bundle = flagship_bundle();
        let paths: Vec<&str> = bundle.files().keys().map(String::as_str).collect();
        assert_eq!(
            paths,
            vec![
                "README.md",
                "ctx.fragment.yaml",
                "jobs/github-issues-search-ingest-issues.yaml",
                "pipelines/github-issues-search-get-document.yaml",
                "pipelines/github-issues-search-search-hybrid.yaml",
                "setup.sql",
            ]
        );
    }

    #[test]
    fn job_yaml_carries_the_loader_required_envelope_and_qualified_destination() {
        let bundle = flagship_bundle();
        let job = &bundle.files()["jobs/github-issues-search-ingest-issues.yaml"];
        assert!(job.starts_with("kind: job\n"), "{job}");
        assert!(
            job.contains("name: github-issues-search-ingest-issues"),
            "{job}"
        );
        // ComponentMetadata requires version; fixed value keeps bytes stable.
        assert!(job.contains("version: \"1.0.0\""), "{job}");
        assert!(job.contains("table: gh_search.main.documents"), "{job}");
        assert!(job.contains("mode: append"), "{job}");
        // The SQL rode in as a block scalar with the ingest invariants intact.
        assert!(job.contains("query: |"), "{job}");
        assert!(job.contains("UNNEST(chunk_parts("), "{job}");
        assert!(
            job.contains("since=<last watermark>"),
            "incremental refresh note: {job}"
        );
    }

    #[test]
    fn pipeline_yamls_carry_their_slugged_names_and_queries() {
        let bundle = flagship_bundle();
        let search = &bundle.files()["pipelines/github-issues-search-search-hybrid.yaml"];
        assert!(search.starts_with("kind: pipeline\n"), "{search}");
        assert!(
            search.contains("name: github-issues-search-search-hybrid"),
            "{search}"
        );
        assert!(search.contains("sqlite_knn("), "{search}");

        let get_doc = &bundle.files()["pipelines/github-issues-search-get-document.yaml"];
        assert!(
            get_doc.contains("name: github-issues-search-get-document"),
            "{get_doc}"
        );
        assert!(get_doc.contains("ORDER BY chunk_index"), "{get_doc}");
    }

    #[test]
    fn readme_walks_the_five_steps_with_real_artifact_names() {
        let bundle = flagship_bundle();
        let readme = &bundle.files()["README.md"];
        for needle in [
            "skardi-etl setup -f setup.sql --dest data/gh.db",
            "skardi-server --ctx ctx.yaml --jobs jobs/ --pipeline pipelines/",
            "skardi job run github-issues-search-ingest-issues",
            "skardi run github-issues-search-search-hybrid",
            "skardi run github-issues-search-get-document",
            "--reset",
            ".etl-bak-<pid>",
            "models/generated/bge-small-en-v1.5",
        ] {
            assert!(
                readme.contains(needle),
                "missing '{needle}' in README:\n{readme}"
            );
        }
    }

    #[test]
    fn rendering_twice_is_byte_identical() {
        let a = flagship_bundle();
        let b = flagship_bundle();
        assert_eq!(a.files(), b.files());
    }

    #[test]
    fn insert_rejects_escaping_paths_and_duplicates() {
        let mut bundle = Bundle::default();
        for bad in ["../evil", "/abs", "", "a/../b"] {
            assert!(bundle.insert(bad, String::new()).is_err(), "{bad}");
        }
        bundle.insert("ok.txt", "x".into()).unwrap();
        let err = bundle.insert("ok.txt", "y".into()).unwrap_err();
        assert!(err.contains("rendered twice"), "{err}");
    }

    #[test]
    fn write_stages_swaps_and_cleans_up() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out");
        let bundle = flagship_bundle();

        // Fresh write.
        bundle.write(&out, false).unwrap();
        assert!(
            out.join("jobs/github-issues-search-ingest-issues.yaml")
                .exists()
        );
        assert!(out.join("README.md").exists());

        // Refuses a non-empty dir without --force…
        let mut edited = bundle.clone();
        edited
            .insert(
                "EDITED.txt",
                "hand edit — the thing --force protects".into(),
            )
            .unwrap();
        let err = edited.write(&out, false).unwrap_err();
        assert!(err.contains("--force"), "{err}");
        assert!(!out.join("EDITED.txt").exists(), "refusal must not write");

        // …and replaces it cleanly with it.
        edited.write(&out, true).unwrap();
        assert!(out.join("EDITED.txt").exists());

        // No staging or backup siblings survive a successful swap.
        let siblings: Vec<String> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(siblings, vec!["out".to_string()], "{siblings:?}");
    }
}
