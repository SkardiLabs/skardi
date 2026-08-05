//! `skardi-etl` — the offline generator CLI (design §CLI; PRD §6.3).
//!
//! Thin clap wiring over `skardi::etl`: `generate` runs the full
//! parse → recipe → plan → render → four-gate validate → atomic write
//! path; `setup` applies (or `--reset`s) a bundle's DDL through a native
//! connection; `recipes` lists built-in coverage and dumps starting
//! points for `--recipe` customization. `export-okf` arrives with the
//! OKF format in milestone 2.
//!
//! Exit codes: 0 success; 1 expected failure (invalid config, validation
//! gate, refused overwrite, missing artifact); 2 environment failure
//! (I/O the user didn't cause: unreadable/unwritable paths, a database
//! that can't be opened).
//!
//! No credential-bearing argv: SQLite/Lance locators are plain paths and
//! may be passed directly; Postgres (M3) resolves through `--dest-env`
//! or a ctx lookup so URLs stay out of shell history and process
//! listings.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use skardi::etl::EtlConfig;
use skardi::etl::config::TargetFormatKind;
use skardi::etl::recipe::{Recipe, embedded_recipe_assets, embedded_recipes};
use skardi::etl::validate::generate_hybrid_with;
use tokio_rusqlite::rusqlite;

/// Expected failure (exit 1) vs environment failure (exit 2).
#[derive(Debug)]
enum CliError {
    Expected(String),
    Environment(String),
}

impl CliError {
    fn exit_code(&self) -> ExitCode {
        match self {
            CliError::Expected(_) => ExitCode::FAILURE,
            CliError::Environment(_) => ExitCode::from(2),
        }
    }
    fn message(&self) -> &str {
        match self {
            CliError::Expected(m) | CliError::Environment(m) => m,
        }
    }
}

#[derive(Parser)]
#[command(
    name = "skardi-etl",
    version,
    about = "Compile `kind: etl` configs into validated skardi bundles"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate and validate a bundle from a `kind: etl` config.
    Generate {
        /// The `kind: etl` config file.
        #[arg(short = 'f', long, value_name = "FILE")]
        file: PathBuf,
        /// Output directory for the bundle.
        #[arg(short = 'o', long, value_name = "DIR")]
        out: PathBuf,
        /// Replace the built-in recipe with this file for the run (same
        /// parser, same validation).
        #[arg(long, value_name = "FILE")]
        recipe: Option<PathBuf>,
        /// Replace a non-empty output directory (the old bundle is kept
        /// as a sibling backup until the swap completes).
        #[arg(long)]
        force: bool,
    },
    /// Apply a bundle's destination DDL (idempotent; `--reset` rebuilds).
    Setup {
        /// The bundle's setup.sql.
        #[arg(short = 'f', long, value_name = "FILE")]
        file: PathBuf,
        /// Destination locator as a plain file path (SQLite).
        #[arg(long, value_name = "PATH", conflicts_with_all = ["dest_env", "ctx"])]
        dest: Option<PathBuf>,
        /// Environment variable NAME holding the destination locator —
        /// keeps credential-bearing URLs out of argv (Postgres, M3).
        #[arg(long, value_name = "VAR", conflicts_with = "ctx")]
        dest_env: Option<String>,
        /// Resolve the destination from a ctx.yaml data source (requires
        /// --catalog).
        #[arg(long, value_name = "FILE", requires = "catalog")]
        ctx: Option<PathBuf>,
        /// The ctx data-source name to resolve (with --ctx).
        #[arg(long, value_name = "NAME")]
        catalog: Option<String>,
        /// Drop every bundle-owned artifact first, then re-apply — the v1
        /// rebuild path. The DROP list is derived from setup.sql's own
        /// CREATE statements, in reverse order.
        #[arg(long)]
        reset: bool,
        /// SQLite extension to load before applying (e.g. sqlite-vec for
        /// vec0). Falls back to $SQLITE_VEC_PATH, then to the plain apply
        /// — which fails with a pointed error if the DDL needs vec0.
        #[arg(long, value_name = "PATH")]
        extension: Option<PathBuf>,
    },
    /// List built-in recipes and their coverage.
    Recipes {
        /// Filter by pack name.
        #[arg(long, value_name = "PACK")]
        pack: Option<String>,
        /// Filter by target format (hybrid_search | okf).
        #[arg(long, value_name = "FORMAT")]
        format: Option<String>,
        /// Dump one embedded recipe's YAML to stdout: --show <pack> <format>.
        #[arg(long, num_args = 2, value_names = ["PACK", "FORMAT"])]
        show: Option<Vec<String>>,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Commands::Generate {
            file,
            out,
            recipe,
            force,
        } => generate(&file, &out, recipe.as_deref(), force).await,
        Commands::Setup {
            file,
            dest,
            dest_env,
            ctx,
            catalog,
            reset,
            extension,
        } => setup(
            &file,
            dest.as_deref(),
            dest_env.as_deref(),
            ctx.as_deref(),
            catalog.as_deref(),
            reset,
            extension.as_deref(),
        ),
        Commands::Recipes { pack, format, show } => {
            recipes(pack.as_deref(), format.as_deref(), show.as_deref())
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {}", e.message());
            e.exit_code()
        }
    }
}

// ─── generate ────────────────────────────────────────────────────────────

async fn generate(
    file: &Path,
    out: &Path,
    recipe: Option<&Path>,
    force: bool,
) -> Result<(), CliError> {
    let yaml = std::fs::read_to_string(file)
        .map_err(|e| CliError::Environment(format!("read '{}': {e}", file.display())))?;
    let config = EtlConfig::from_yaml(&yaml)
        .map_err(|e| CliError::Expected(format!("{}: {e}", file.display())))?;

    let recipe_override = match recipe {
        Some(path) => {
            let recipe_yaml = std::fs::read_to_string(path)
                .map_err(|e| CliError::Environment(format!("read '{}': {e}", path.display())))?;
            Some(
                Recipe::from_yaml(&recipe_yaml)
                    .map_err(|e| CliError::Expected(format!("{}: {e}", path.display())))?,
            )
        }
        None => None,
    };

    let generated = generate_hybrid_with(&config, recipe_override)
        .await
        .map_err(CliError::Expected)?;
    for warning in &generated.warnings {
        eprintln!("warning: {warning}");
    }

    generated.bundle.write(out, force).map_err(|e| {
        // The --force refusal is the user's decision point; everything
        // else in the write path is environmental.
        if e.contains("--force") {
            CliError::Expected(e)
        } else {
            CliError::Environment(e)
        }
    })?;

    println!(
        "wrote {} files to {}:",
        generated.bundle.files().len(),
        out.display()
    );
    for path in generated.bundle.files().keys() {
        println!("  {path}");
    }
    println!(
        "\nnext: {}/README.md walks the five steps (setup → merge ctx → serve → ingest → search).",
        out.display()
    );
    Ok(())
}

// ─── setup ───────────────────────────────────────────────────────────────

fn setup(
    file: &Path,
    dest: Option<&Path>,
    dest_env: Option<&str>,
    ctx: Option<&Path>,
    catalog: Option<&str>,
    reset: bool,
    extension: Option<&Path>,
) -> Result<(), CliError> {
    let setup_sql = std::fs::read_to_string(file)
        .map_err(|e| CliError::Environment(format!("read '{}': {e}", file.display())))?;
    let dest_path = resolve_dest(dest, dest_env, ctx, catalog)?;

    let conn = rusqlite::Connection::open(&dest_path)
        .map_err(|e| CliError::Environment(format!("open sqlite database '{dest_path}': {e}")))?;
    load_extension(&conn, extension)?;

    if reset {
        let drops = reset_statements(&setup_sql);
        if drops.is_empty() {
            return Err(CliError::Expected(format!(
                "--reset found no CREATE statements to derive a DROP list from in '{}'",
                file.display()
            )));
        }
        println!("reset: dropping {} bundle-owned artifacts", drops.len());
        for drop in &drops {
            println!("  {drop}");
            conn.execute_batch(drop)
                .map_err(|e| CliError::Expected(format!("reset failed at `{drop}`: {e}")))?;
        }
    }

    conn.execute_batch(&setup_sql).map_err(|e| {
        let hint = if e.to_string().contains("vec0") {
            "\nhint: the vec0 virtual table needs the sqlite-vec extension — pass \
             --extension <path-to-vec0-library> or set SQLITE_VEC_PATH"
        } else {
            ""
        };
        CliError::Expected(format!("applying '{}' failed: {e}{hint}", file.display()))
    })?;
    println!(
        "applied '{}' to '{dest_path}' (idempotent; re-run with --reset to rebuild)",
        file.display()
    );
    Ok(())
}

/// Exactly one destination selector; plain paths only for file engines.
fn resolve_dest(
    dest: Option<&Path>,
    dest_env: Option<&str>,
    ctx: Option<&Path>,
    catalog: Option<&str>,
) -> Result<String, CliError> {
    match (dest, dest_env, ctx) {
        (Some(path), None, None) => Ok(path.display().to_string()),
        (None, Some(var), None) => std::env::var(var).map_err(|_| {
            CliError::Expected(format!("--dest-env: ${var} is not set in this environment"))
        }),
        (None, None, Some(ctx_path)) => {
            let name = catalog.expect("clap: --ctx requires --catalog");
            ctx_lookup(ctx_path, name)
        }
        _ => Err(CliError::Expected(
            "pass exactly one of --dest <path>, --dest-env <VAR>, or --ctx <ctx.yaml> \
             --catalog <name>"
                .to_string(),
        )),
    }
}

/// Find `spec.data_sources[name == catalog].path` in a ctx.yaml.
fn ctx_lookup(ctx_path: &Path, name: &str) -> Result<String, CliError> {
    let text = std::fs::read_to_string(ctx_path)
        .map_err(|e| CliError::Environment(format!("read '{}': {e}", ctx_path.display())))?;
    let value: serde_yaml::Value = serde_yaml::from_str(&text).map_err(|e| {
        CliError::Expected(format!("'{}' is not valid YAML: {e}", ctx_path.display()))
    })?;
    let sources = value
        .get("spec")
        .and_then(|s| s.get("data_sources"))
        .and_then(|d| d.as_sequence())
        .ok_or_else(|| {
            CliError::Expected(format!(
                "'{}' has no spec.data_sources to look '{name}' up in",
                ctx_path.display()
            ))
        })?;
    let entry = sources
        .iter()
        .find(|e| e.get("name").and_then(|n| n.as_str()) == Some(name))
        .ok_or_else(|| {
            let known: Vec<&str> = sources
                .iter()
                .filter_map(|e| e.get("name").and_then(|n| n.as_str()))
                .collect();
            CliError::Expected(format!(
                "no data source named '{name}' in '{}' (found: {})",
                ctx_path.display(),
                known.join(", ")
            ))
        })?;
    entry
        .get("path")
        .and_then(|p| p.as_str())
        .map(str::to_string)
        .ok_or_else(|| {
            CliError::Expected(format!(
                "data source '{name}' has no plain `path` — only file-path destinations \
                 (sqlite) resolve through --ctx today"
            ))
        })
}

fn load_extension(conn: &rusqlite::Connection, extension: Option<&Path>) -> Result<(), CliError> {
    let path = extension
        .map(|p| p.display().to_string())
        .or_else(|| std::env::var("SQLITE_VEC_PATH").ok())
        .filter(|p| !p.trim().is_empty());
    let Some(path) = path else { return Ok(()) };
    // SAFETY: same discipline as the skardi sqlite provider — loading is
    // enabled only around the user-designated library, then disabled.
    let loaded = (|| -> Result<(), rusqlite::Error> {
        unsafe { conn.load_extension_enable()? };
        let result = unsafe { conn.load_extension(&path, None::<&str>) };
        conn.load_extension_disable()?;
        result
    })();
    loaded.map_err(|e| CliError::Expected(format!("loading sqlite extension '{path}' failed: {e}")))
}

/// Derive the `--reset` DROP list from setup.sql's own CREATE statements,
/// reverse creation order (triggers were created last, drop first). This
/// keeps `setup` bundle-agnostic: whatever the DDL created is exactly
/// what reset removes.
fn reset_statements(setup_sql: &str) -> Vec<String> {
    let mut drops = Vec::new();
    for line in setup_sql.lines() {
        let line = line.trim();
        let rest = line
            .strip_prefix("CREATE TABLE IF NOT EXISTS ")
            .or_else(|| line.strip_prefix("CREATE VIRTUAL TABLE IF NOT EXISTS "))
            .map(|r| ("TABLE", r))
            .or_else(|| {
                line.strip_prefix("CREATE TRIGGER IF NOT EXISTS ")
                    .map(|r| ("TRIGGER", r))
            });
        if let Some((kind, rest)) = rest {
            let name: String = rest
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() {
                drops.push(format!("DROP {kind} IF EXISTS {name};"));
            }
        }
    }
    drops.reverse();
    drops
}

// ─── recipes ─────────────────────────────────────────────────────────────

fn recipes(
    pack: Option<&str>,
    format: Option<&str>,
    show: Option<&[String]>,
) -> Result<(), CliError> {
    if let Some(args) = show {
        let (want_pack, want_format) = (&args[0], &args[1]);
        let asset_name = format!("{want_pack}.{want_format}.yaml");
        let Some((_, yaml)) = embedded_recipe_assets()
            .iter()
            .find(|(asset, _)| *asset == asset_name)
        else {
            let known: Vec<&str> = embedded_recipe_assets().iter().map(|(a, _)| *a).collect();
            return Err(CliError::Expected(format!(
                "no embedded recipe '{asset_name}' (available: {})",
                known.join(", ")
            )));
        };
        print!("{yaml}");
        return Ok(());
    }

    let format_filter = match format {
        Some("hybrid_search") => Some(TargetFormatKind::HybridSearch),
        Some("okf") => Some(TargetFormatKind::Okf),
        Some(other) => {
            return Err(CliError::Expected(format!(
                "--format must be 'hybrid_search' or 'okf', got '{other}'"
            )));
        }
        None => None,
    };

    let mut shown = 0;
    for recipe in embedded_recipes().map_err(CliError::Expected)? {
        if pack.is_some_and(|p| p != recipe.pack) {
            continue;
        }
        if format_filter.is_some_and(|f| f != recipe.format) {
            continue;
        }
        let tables: Vec<&str> = recipe.tables.keys().map(String::as_str).collect();
        println!(
            "{} {} (v{}): tables [{}]",
            recipe.pack,
            recipe.format.as_str(),
            recipe.version,
            tables.join(", ")
        );
        shown += 1;
    }
    if shown == 0 {
        println!("no built-in recipe matches the filter — `skardi-etl recipes` lists all");
    } else {
        println!(
            "\n`skardi-etl recipes --show <pack> <format>` dumps one as a --recipe starting point"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_list_is_reverse_creation_order_and_complete() {
        let setup = "\
CREATE TABLE IF NOT EXISTS documents (\n  x TEXT\n);\n\
CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(content);\n\
CREATE VIRTUAL TABLE IF NOT EXISTS documents_vec USING vec0(embedding float[3]);\n\
CREATE TRIGGER IF NOT EXISTS documents_ai_fts\nAFTER INSERT ON documents BEGIN\nEND;\n\
CREATE TRIGGER IF NOT EXISTS documents_ai_vec\nAFTER INSERT ON documents BEGIN\nEND;\n";
        assert_eq!(
            reset_statements(setup),
            vec![
                "DROP TRIGGER IF EXISTS documents_ai_vec;",
                "DROP TRIGGER IF EXISTS documents_ai_fts;",
                "DROP TABLE IF EXISTS documents_vec;",
                "DROP TABLE IF EXISTS documents_fts;",
                "DROP TABLE IF EXISTS documents;",
            ]
        );
    }

    #[test]
    fn ctx_lookup_finds_paths_and_names_misses() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = dir.path().join("ctx.yaml");
        std::fs::write(
            &ctx,
            "kind: context\nmetadata: {name: t, version: 1.0.0}\nspec:\n  data_sources:\n\
             \x20   - name: gh_search\n      type: sqlite\n      path: data/gh.db\n",
        )
        .unwrap();
        assert_eq!(ctx_lookup(&ctx, "gh_search").unwrap(), "data/gh.db");
        let err = ctx_lookup(&ctx, "nope").unwrap_err();
        assert!(
            err.message().contains("found: gh_search"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn setup_applies_resets_and_reapplies_a_real_bundle_ddl() {
        // The fts5/documents/trigger part of a real generated setup.sql
        // (vec0 excluded: no extension in the test environment) must apply,
        // re-apply, reset, and re-apply through the CLI paths.
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("t.db");
        let sql = "\
CREATE TABLE IF NOT EXISTS documents (\n  rid INTEGER PRIMARY KEY,\n  content TEXT NOT NULL\n);\n\
CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(\n  content,\n  doc_rowid UNINDEXED\n);\n\
CREATE TRIGGER IF NOT EXISTS documents_ai_fts\nAFTER INSERT ON documents BEGIN\n  INSERT INTO documents_fts(doc_rowid, content)\n  VALUES (NEW.rid, NEW.content);\nEND;\n";
        let setup_file = dir.path().join("setup.sql");
        std::fs::write(&setup_file, sql).unwrap();

        setup(&setup_file, Some(&db), None, None, None, false, None)
            .map_err(|e| e.message().to_string())
            .unwrap();
        // Idempotent re-apply, then reset + re-apply.
        setup(&setup_file, Some(&db), None, None, None, false, None)
            .map_err(|e| e.message().to_string())
            .unwrap();
        setup(&setup_file, Some(&db), None, None, None, true, None)
            .map_err(|e| e.message().to_string())
            .unwrap();

        let conn = rusqlite::Connection::open(&db).unwrap();
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'documents%' \
                 AND type IN ('table', 'trigger') AND name NOT LIKE '%_fts_%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            n, 3,
            "documents + fts mirror + trigger survive the lifecycle"
        );
    }
}
