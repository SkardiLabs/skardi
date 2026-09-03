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

pub mod config;
pub mod frontmatter;
pub mod markdown;
pub mod resolve;
pub mod scan;
pub mod table;

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
