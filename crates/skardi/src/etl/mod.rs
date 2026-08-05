//! skardi-etl-generator: a compiler from semantic knowledge to skardi
//! configuration.
//!
//! Input: one `kind: etl` config naming a source pack, a target format,
//! and a destination engine. Output: a self-contained, validated,
//! byte-deterministic bundle — destination DDL, `kind: job` ingest
//! definitions, `kind: pipeline` search/read definitions, a ctx fragment,
//! and a README.
//!
//! Design and PRD (normative): skardi-cloud
//! `design_docs/skardi_etl_generator{,_prd}.md`. Milestone map:
//! `docs/superpowers/specs/2026-07-29-skardi-etl-generator-tasks.md`.
//!
//! The three load-bearing invariants every module here serves:
//!
//! - **Valid by construction**: nothing is written that hasn't
//!   round-tripped skardi's real loaders and plan-checked against schemas
//!   derived from the pack's field mappings.
//! - **SELECT column order ≡ destination DDL order**: the job executor
//!   preflights schemas by name, order-insensitively, while the DB write
//!   is positional (`INSERT INTO dest SELECT *`) — so column order is
//!   THIS crate's invariant, asserted in the plan-check.
//! - **Determinism**: regenerating with an unchanged config yields
//!   byte-identical output (BTreeMap iteration, insertion-ordered JSON,
//!   no timestamps at generate time).

pub mod bundle;
pub mod config;
pub mod dialect;
pub mod dialects;
pub mod format;
pub mod recipe;
// The generator's validation gates plan real `chunk_parts` SQL, so they
// (and the generate entry point) require the chunking feature — a
// plan-check against a stub splitter would be theater.
#[cfg(feature = "chunking")]
pub mod validate;

// The real-Open-Connector-source ingest e2e (mock gateway over HTTP →
// real provider pagination → real executor → real SQLite). In-crate
// because the gateway test double is pub(crate).
#[cfg(all(test, feature = "chunking"))]
mod oc_e2e;

pub use bundle::{Bundle, render_hybrid_bundle, slug};
pub use config::EtlConfig;
pub use dialect::{EngineDialect, resolve_dialect};
pub use format::hybrid_plan;
pub use recipe::Recipe;
