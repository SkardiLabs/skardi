//! `obsidian` data source connector: one Obsidian vault as three read-only
//! catalog tables — `<name>.main.notes`, `<name>.main.links`,
//! `<name>.main.tags`. Everything here is behind the `obsidian` Cargo feature.
//!
//! Design: `docs/superpowers/specs/2026-09-02-obsidian-source-design.md`.
//! The decisions that shape this module, with their reasons:
//!
//! - **Rescan on every query, no cache.** A vault is thousands of small files;
//!   a full list + read + parse is tens to hundreds of milliseconds locally,
//!   and a cache would introduce the stale-row bug class the design most
//!   wants to avoid. A query joining two of the tables parses the vault twice.
//! - **Whole scan off the Tokio worker.** `scan::VaultScan::run` is
//!   synchronous and runs inside `tokio::task::spawn_blocking`; the `BlobStore`
//!   is resolved inside that task so any S3 client lives on one runtime.
//! - **Frontmatter as JSON, `aliases` lifted.** Frontmatter is schemaless per
//!   note; only `aliases` has semantics Obsidian itself defines.
//! - **Links resolved like Obsidian, never through aliases.** `[[Alias]]` is
//!   `missing`, exactly as Obsidian treats it; the alias-repair query in
//!   `docs/obsidian.md` recovers the intent without misstating the graph.
//! - **Frontmatter links count.** Every string value in the parsed frontmatter
//!   is scanned for `[[…]]` (`links.source = 'frontmatter'`, `line` NULL).
//! - **No symlinks, ever.** Listing skips them and reads refuse them
//!   (`O_NOFOLLOW`), because `path: ~/vault` must not read outside the vault.
//! - **Size cap from listing metadata.** `max_file_bytes` is enforced before
//!   any read so a huge object is never buffered.
//! - **Wholesale-failure guard.** A non-empty listing where every attempted
//!   read fails is an error naming the root, never three empty tables.

// Private, like `documents`' `parse`/`table`: the whole connector is reached
// through `register_obsidian_tables`, so the scanner, parsers and resolver are
// implementation detail. Keeping them out of the crate's public API leaves
// their signatures free to change without a breaking release.
mod config;
mod frontmatter;
mod markdown;
mod resolve;
mod scan;
mod table;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
};
use datafusion::prelude::SessionContext;
use tokio::runtime::Handle;

use crate::sources::hierarchy::HierarchyLevel;
use crate::sources::providers::blob::{BlobStore, ListOptions, Loc};
use config::ScanOptions;
use table::{ObsidianTable, TableKind};

/// Surface generation of the three schemas (Arrow metadata
/// `skardi.obsidian.surface_version`). Bump on any incompatible change.
pub const OBSIDIAN_SURFACE_VERSION: u32 = 1;

/// The one schema every obsidian catalog exposes.
pub const OBSIDIAN_SCHEMA: &str = "main";
pub const NOTES_TABLE: &str = "notes";
pub const LINKS_TABLE: &str = "links";
pub const TAGS_TABLE: &str = "tags";

/// Registration-time failures. Each names the source and the offending field
/// or path; none carries file contents.
#[derive(Debug, thiserror::Error)]
pub enum ObsidianError {
    #[error("obsidian source '{name}': hierarchy_level must be `catalog`")]
    CatalogHierarchyRequired { name: String },
    #[error(
        "obsidian source '{name}': access_mode `read_write` is not supported (the source is read-only)"
    )]
    ReadWriteNotSupported { name: String },
    #[error("obsidian source '{name}': invalid options: {reason}")]
    InvalidOptions { name: String, reason: String },
    #[error("obsidian source '{name}': vault root {path} is unavailable: {cause}")]
    RootUnavailable {
        name: String,
        path: String,
        cause: String,
    },
    #[error("obsidian source '{name}': vault root {path} is not a directory")]
    RootNotDirectory { name: String, path: String },
}

/// Register one vault as the catalog `name` with schema `main` and tables
/// `notes`, `links`, `tags`.
///
/// Every invariant is enforced here, so the server's `config.rs` arm re-checks
/// nothing: catalog hierarchy, read-only access, valid options, and a
/// reachable root (a directory locally; one non-recursive list for `s3://`).
/// No parsing happens at registration. `register_catalog` is the **last**
/// step: it replaces whatever was registered under `name` unconditionally, so
/// a failed registration must never have touched the context.
///
/// # Example
///
/// ```no_run
/// use std::collections::HashMap;
///
/// use datafusion::prelude::SessionContext;
/// use skardi::sources::hierarchy::HierarchyLevel;
/// use skardi::sources::providers::obsidian::register_obsidian_tables;
///
/// # async fn register() -> anyhow::Result<()> {
/// let mut ctx = SessionContext::new();
/// let options = HashMap::from([("max_file_bytes".to_string(), "1048576".to_string())]);
///
/// register_obsidian_tables(
///     &mut ctx,
///     "vault",
///     "/home/me/notes",
///     Some(&options),
///     false, // read_write: the source is read-only
///     HierarchyLevel::Catalog,
/// )
/// .await?;
///
/// let df = ctx
///     .sql("SELECT path, name FROM vault.main.notes ORDER BY path")
///     .await?;
/// df.show().await?;
/// # Ok(())
/// # }
/// ```
pub async fn register_obsidian_tables(
    session_ctx: &mut SessionContext,
    name: &str,
    path: &str,
    options: Option<&HashMap<String, String>>,
    read_write: bool,
    hierarchy_level: HierarchyLevel,
) -> Result<()> {
    if hierarchy_level != HierarchyLevel::Catalog {
        return Err(ObsidianError::CatalogHierarchyRequired {
            name: name.to_string(),
        }
        .into());
    }
    if read_write {
        return Err(ObsidianError::ReadWriteNotSupported {
            name: name.to_string(),
        }
        .into());
    }
    let opts = ScanOptions::from_map(options).map_err(|e| ObsidianError::InvalidOptions {
        name: name.to_string(),
        reason: e.to_string(),
    })?;
    check_root(name, path).await?;

    let schema_provider = Arc::new(MemorySchemaProvider::new());
    for kind in [TableKind::Notes, TableKind::Links, TableKind::Tags] {
        schema_provider
            .register_table(
                kind.table_name().to_string(),
                Arc::new(ObsidianTable::new(kind, path.to_string(), opts.clone())),
            )
            .map_err(|e| {
                anyhow::anyhow!(
                    "obsidian source '{name}': failed to register {OBSIDIAN_SCHEMA}.{}: {e}",
                    kind.table_name()
                )
            })?;
    }
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog
        .register_schema(OBSIDIAN_SCHEMA, schema_provider)
        .map_err(|e| {
            anyhow::anyhow!(
                "obsidian source '{name}': failed to register schema '{OBSIDIAN_SCHEMA}': {e}"
            )
        })?;
    session_ctx.register_catalog(name, catalog);

    tracing::info!(
        source = %name,
        root = %path,
        exclude_globs = ?opts.exclude_globs(),
        max_file_bytes = opts.max_file_bytes,
        surface_version = OBSIDIAN_SURFACE_VERSION,
        "Obsidian source registered"
    );
    Ok(())
}

/// Registration-time root check. Local: must exist and be a directory. S3:
/// one non-recursive list, run on the blocking pool so the S3 client is built
/// and driven on one runtime (the same shape the scan uses).
async fn check_root(name: &str, path: &str) -> Result<()> {
    let unavailable = |cause: String| ObsidianError::RootUnavailable {
        name: name.to_string(),
        path: path.to_string(),
        cause,
    };
    let loc = Loc::parse(path).map_err(|e| unavailable(e.to_string()))?;
    match loc {
        Loc::Local(dir) => {
            let meta = tokio::fs::metadata(&dir)
                .await
                .map_err(|e| unavailable(e.to_string()))?;
            if !meta.is_dir() {
                return Err(ObsidianError::RootNotDirectory {
                    name: name.to_string(),
                    path: path.to_string(),
                }
                .into());
            }
            Ok(())
        }
        Loc::S3 { .. } => {
            let uri = path.to_string();
            let listed = tokio::task::spawn_blocking(move || -> Result<()> {
                let (store, prefix) = BlobStore::resolve(&uri)?;
                Handle::current().block_on(store.list(
                    &prefix,
                    ListOptions {
                        recursive: false,
                        follow_symlinks: false,
                    },
                ))?;
                Ok(())
            })
            .await
            .context("obsidian: root check task panicked or was cancelled")?;
            listed.map_err(|e| unavailable(format!("{e:#}")))?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray, RecordBatch};
    use arrow::compute::cast;
    use arrow::datatypes::{DataType, Int64Type};
    use std::path::{Path, PathBuf};

    fn fixture_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/sources/providers/obsidian/fixtures/vault")
    }

    async fn register(root: &Path, name: &str) -> SessionContext {
        let mut ctx = SessionContext::new();
        register_obsidian_tables(
            &mut ctx,
            name,
            &root.to_string_lossy(),
            None,
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .expect("fixture vault registers");
        ctx
    }

    async fn query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        ctx.sql(sql).await.unwrap().collect().await.unwrap()
    }

    /// Column `col` of every batch as strings (cast to Utf8 so a view type
    /// chosen by the planner does not matter).
    fn strings(batches: &[RecordBatch], col: usize) -> Vec<Option<String>> {
        let mut out = Vec::new();
        for b in batches {
            let arr = cast(b.column(col), &DataType::Utf8).unwrap();
            let arr = arr.as_string::<i32>();
            out.extend((0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i).to_string())));
        }
        out
    }

    fn int64(batches: &[RecordBatch], col: usize) -> Vec<i64> {
        let mut out = Vec::new();
        for b in batches {
            let arr = cast(b.column(col), &DataType::Int64).unwrap();
            out.extend(arr.as_primitive::<Int64Type>().values().iter().copied());
        }
        out
    }

    fn bools(batches: &[RecordBatch], col: usize) -> Vec<bool> {
        batches
            .iter()
            .flat_map(|b| {
                let arr = b.column(col).as_boolean();
                (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>()
            })
            .collect()
    }

    fn copy_dir(src: &Path, dst: &Path) {
        std::fs::create_dir_all(dst).unwrap();
        for entry in std::fs::read_dir(src).unwrap() {
            let entry = entry.unwrap();
            let target = dst.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_dir(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }

    #[tokio::test]
    async fn rejects_non_catalog_hierarchy() {
        let mut ctx = SessionContext::new();
        let err = register_obsidian_tables(
            &mut ctx,
            "vault",
            &fixture_root().to_string_lossy(),
            None,
            false,
            HierarchyLevel::Table,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<ObsidianError>(),
            Some(ObsidianError::CatalogHierarchyRequired { name }) if name == "vault"
        ));
        assert!(
            ctx.catalog("vault").is_none(),
            "nothing registered on failure"
        );
    }

    #[tokio::test]
    async fn rejects_read_write() {
        let mut ctx = SessionContext::new();
        let err = register_obsidian_tables(
            &mut ctx,
            "vault",
            &fixture_root().to_string_lossy(),
            None,
            true,
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<ObsidianError>(),
            Some(ObsidianError::ReadWriteNotSupported { .. })
        ));
    }

    #[tokio::test]
    async fn rejects_unknown_option_naming_it() {
        let mut ctx = SessionContext::new();
        let opts = HashMap::from([("exclude_glob".to_string(), "x".to_string())]);
        let err = register_obsidian_tables(
            &mut ctx,
            "vault",
            &fixture_root().to_string_lossy(),
            Some(&opts),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown option `exclude_glob`"), "{msg}");
        assert!(msg.contains("'vault'"), "{msg}");
    }

    #[tokio::test]
    async fn rejects_missing_root_and_file_root() {
        let mut ctx = SessionContext::new();
        let err = register_obsidian_tables(
            &mut ctx,
            "vault",
            "/no/such/vault",
            None,
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<ObsidianError>(),
            Some(ObsidianError::RootUnavailable { path, .. }) if path == "/no/such/vault"
        ));

        let file = tempfile::NamedTempFile::new().unwrap();
        let err = register_obsidian_tables(
            &mut ctx,
            "vault",
            &file.path().to_string_lossy(),
            None,
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<ObsidianError>(),
            Some(ObsidianError::RootNotDirectory { .. })
        ));
    }

    #[tokio::test]
    async fn registers_three_tables_under_main() {
        let ctx = register(&fixture_root(), "vault").await;
        let catalog = ctx.catalog("vault").expect("catalog registered");
        let schema = catalog.schema("main").expect("main schema");
        let mut names = schema.table_names();
        names.sort();
        assert_eq!(names, vec!["links", "notes", "tags"]);

        assert_eq!(
            int64(
                &query(&ctx, "SELECT count(*) FROM vault.main.notes").await,
                0
            ),
            vec![12]
        );
        assert_eq!(
            int64(
                &query(&ctx, "SELECT count(*) FROM vault.main.links").await,
                0
            ),
            vec![27]
        );
        assert_eq!(
            int64(
                &query(&ctx, "SELECT count(*) FROM vault.main.tags").await,
                0
            ),
            vec![10]
        );
    }

    #[tokio::test]
    async fn notes_projection_order_and_limit() {
        let ctx = register(&fixture_root(), "vault").await;
        let b = query(&ctx, "SELECT path, name, folder FROM vault.main.notes").await;
        let paths: Vec<String> = strings(&b, 0).into_iter().flatten().collect();
        assert_eq!(
            paths,
            vec![
                "Archive/Notes.md",
                "Bad Frontmatter.md",
                "CJK.md",
                "Home.md",
                "Large.md",
                "Meeting.md",
                "No Frontmatter.md",
                "People/Alice.md",
                "People/Bob.md",
                "Projects/Design.md",
                "Projects/Notes.md",
                "Rooms/B12.md",
            ]
        );
        let names = strings(&b, 1);
        let folders = strings(&b, 2);
        assert_eq!(names[9].as_deref(), Some("Design"));
        assert_eq!(folders[9].as_deref(), Some("Projects"));
        assert_eq!(folders[3].as_deref(), Some(""));

        let b = query(&ctx, "SELECT path FROM vault.main.notes LIMIT 3").await;
        assert_eq!(b.iter().map(|b| b.num_rows()).sum::<usize>(), 3);

        let b = query(
            &ctx,
            "SELECT arrow_typeof(modified_at) FROM vault.main.notes LIMIT 1",
        )
        .await;
        // DataFusion's own rendering of the type, not Arrow's Debug form.
        assert_eq!(strings(&b, 0)[0].as_deref(), Some("Timestamp(ms, \"UTC\")"));
    }

    #[tokio::test]
    async fn frontmatter_null_cases() {
        let ctx = register(&fixture_root(), "vault").await;
        let b = query(
            &ctx,
            "SELECT path, frontmatter_json IS NULL, frontmatter_error IS NOT NULL, aliases IS NULL \
             FROM vault.main.notes \
             WHERE path IN ('Bad Frontmatter.md', 'Home.md', 'No Frontmatter.md') ORDER BY path",
        )
        .await;
        assert_eq!(bools(&b, 1), vec![true, false, true]);
        assert_eq!(bools(&b, 2), vec![true, false, false]);
        assert_eq!(bools(&b, 3), vec![true, false, true]);
    }

    #[tokio::test]
    async fn every_kind_resolution_and_source_value_appears() {
        let ctx = register(&fixture_root(), "vault").await;
        let kinds = strings(
            &query(
                &ctx,
                "SELECT DISTINCT kind FROM vault.main.links ORDER BY kind",
            )
            .await,
            0,
        );
        assert_eq!(
            kinds.into_iter().flatten().collect::<Vec<_>>(),
            vec!["embed", "external", "markdown", "wikilink"]
        );
        let res = strings(
            &query(
                &ctx,
                "SELECT DISTINCT resolution FROM vault.main.links ORDER BY resolution",
            )
            .await,
            0,
        );
        assert_eq!(
            res.into_iter().flatten().collect::<Vec<_>>(),
            vec!["ambiguous", "exact", "external", "missing", "name"]
        );
        let src = strings(
            &query(
                &ctx,
                "SELECT DISTINCT source FROM vault.main.links ORDER BY source",
            )
            .await,
            0,
        );
        assert_eq!(
            src.into_iter().flatten().collect::<Vec<_>>(),
            vec!["body", "frontmatter"]
        );
        let src = strings(
            &query(
                &ctx,
                "SELECT DISTINCT source FROM vault.main.tags ORDER BY source",
            )
            .await,
            0,
        );
        assert_eq!(
            src.into_iter().flatten().collect::<Vec<_>>(),
            vec!["body", "frontmatter"]
        );
    }

    #[tokio::test]
    async fn graph_queries_from_the_docs() {
        let ctx = register(&fixture_root(), "vault").await;

        // Most-linked note.
        let b = query(
            &ctx,
            "SELECT to_path, count(*) AS n FROM vault.main.links \
             WHERE to_path IS NOT NULL GROUP BY to_path ORDER BY n DESC, to_path LIMIT 1",
        )
        .await;
        assert_eq!(strings(&b, 0)[0].as_deref(), Some("Home.md"));
        assert_eq!(int64(&b, 1), vec![6]);

        // Orphans: notes nothing links to.
        let b = query(
            &ctx,
            "SELECT n.path FROM vault.main.notes n \
             LEFT JOIN vault.main.links l ON l.to_path = n.path \
             WHERE l.to_path IS NULL ORDER BY n.path",
        )
        .await;
        assert_eq!(
            strings(&b, 0).into_iter().flatten().collect::<Vec<_>>(),
            vec![
                "Archive/Notes.md",
                "Bad Frontmatter.md",
                "CJK.md",
                "Large.md",
                "No Frontmatter.md"
            ]
        );

        // Alias repair: a missing link whose target is another note's alias.
        let b = query(
            &ctx,
            "SELECT l.from_path, l.target, a.path AS probably_meant \
             FROM vault.main.links l \
             JOIN (SELECT path, unnest(aliases) AS alias FROM vault.main.notes WHERE aliases IS NOT NULL) a \
               ON a.alias = l.target \
             WHERE l.resolution = 'missing' ORDER BY l.from_path",
        )
        .await;
        assert_eq!(strings(&b, 0), vec![Some("Projects/Notes.md".to_string())]);
        assert_eq!(strings(&b, 1), vec![Some("Start".to_string())]);
        assert_eq!(strings(&b, 2), vec![Some("Home.md".to_string())]);

        // Rooms/B12.md is linked from nowhere but Meeting.md's frontmatter
        // (`location.room`): its whole in-degree is that one row, so without
        // frontmatter link extraction it would be a sixth orphan above.
        let b = query(
            &ctx,
            "SELECT count(*) FROM vault.main.links WHERE to_path = 'Rooms/B12.md'",
        )
        .await;
        assert_eq!(int64(&b, 0), vec![1]);
        let b = query(
            &ctx,
            "SELECT source FROM vault.main.links WHERE to_path = 'Rooms/B12.md'",
        )
        .await;
        assert_eq!(strings(&b, 0), vec![Some("frontmatter".to_string())]);
    }

    #[tokio::test]
    async fn explain_shows_the_scan_exec() {
        let ctx = register(&fixture_root(), "vault").await;
        let b = query(&ctx, "EXPLAIN SELECT tag FROM vault.main.tags").await;
        let text = arrow::util::pretty::pretty_format_batches(&b)
            .unwrap()
            .to_string();
        assert!(text.contains("ObsidianScanExec"), "{text}");
    }

    async fn explain(ctx: &SessionContext, sql: &str) -> String {
        arrow::util::pretty::pretty_format_batches(&query(ctx, sql).await)
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn declared_ordering_lets_the_planner_skip_a_sort() {
        let ctx = register(&fixture_root(), "vault").await;
        for sql in [
            "EXPLAIN SELECT path FROM vault.main.notes ORDER BY path",
            "EXPLAIN SELECT from_path, target FROM vault.main.links ORDER BY from_path",
            "EXPLAIN SELECT path, tag, source FROM vault.main.tags ORDER BY path, tag, source",
        ] {
            let text = explain(&ctx, sql).await;
            assert!(!text.contains("SortExec"), "{sql}\n{text}");
        }
        // Projecting the leading column away withdraws the guarantee.
        let text = explain(&ctx, "EXPLAIN SELECT tag FROM vault.main.tags ORDER BY tag").await;
        assert!(text.contains("SortExec"), "{text}");
    }

    #[cfg(unix)]
    fn note_files(root: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).unwrap().flatten() {
                let p = entry.path();
                if p.is_dir() {
                    stack.push(p);
                } else if p.extension().is_some_and(|e| e == "md") {
                    out.push(p);
                }
            }
        }
        out
    }

    /// Spec Failure Modes: a vault where every read fails is a query error
    /// naming the root — asserted through DataFusion, not just the scan API.
    #[cfg(unix)]
    #[tokio::test]
    async fn every_read_failing_fails_the_query_naming_the_root() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        copy_dir(&fixture_root(), dir.path());
        let notes = note_files(dir.path());
        for p in &notes {
            std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o000)).unwrap();
        }
        let restore = || {
            for p in &notes {
                let _ = std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o644));
            }
        };
        if notes.iter().any(|p| std::fs::read(p).is_ok()) {
            eprintln!("skipping: running as root, chmod 000 does not deny reads");
            restore();
            return;
        }
        // Registration only checks that the root is a directory.
        let ctx = register(dir.path(), "vault").await;
        let err = ctx
            .sql("SELECT count(*) FROM vault.main.notes")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("every note read under"), "{msg}");
        assert!(
            msg.contains(&dir.path().to_string_lossy().into_owned()),
            "{msg}"
        );
        restore();
    }

    #[tokio::test]
    async fn edits_between_scans_are_visible_without_reregistration() {
        let dir = tempfile::tempdir().unwrap();
        copy_dir(&fixture_root(), dir.path());
        let ctx = register(dir.path(), "vault").await;
        assert_eq!(
            int64(
                &query(&ctx, "SELECT count(*) FROM vault.main.notes").await,
                0
            ),
            vec![12]
        );
        assert_eq!(
            int64(
                &query(&ctx, "SELECT count(*) FROM vault.main.links").await,
                0
            ),
            vec![27]
        );

        std::fs::write(dir.path().join("New.md"), "Fresh note linking [[Home]].\n").unwrap();
        assert_eq!(
            int64(
                &query(&ctx, "SELECT count(*) FROM vault.main.notes").await,
                0
            ),
            vec![13]
        );
        assert_eq!(
            int64(
                &query(&ctx, "SELECT count(*) FROM vault.main.links").await,
                0
            ),
            vec![28]
        );

        std::fs::remove_file(dir.path().join("New.md")).unwrap();
        assert_eq!(
            int64(
                &query(&ctx, "SELECT count(*) FROM vault.main.notes").await,
                0
            ),
            vec![12]
        );
    }

    #[tokio::test]
    async fn options_reach_the_scan() {
        let mut ctx = SessionContext::new();
        let opts = HashMap::from([
            ("exclude_globs".to_string(), "People/**".to_string()),
            ("max_file_bytes".to_string(), "2048".to_string()),
        ]);
        register_obsidian_tables(
            &mut ctx,
            "vault",
            &fixture_root().to_string_lossy(),
            Some(&opts),
            false,
            HierarchyLevel::Catalog,
        )
        .await
        .unwrap();
        // 12 − People (2) − Large (cap) + .trash/Deleted.md (default gone) = 10.
        assert_eq!(
            int64(
                &query(&ctx, "SELECT count(*) FROM vault.main.notes").await,
                0
            ),
            vec![10]
        );
    }
}
