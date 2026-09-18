//! Small shared utilities with no better home.
//!
//! `http`, `json` and `text` now live in `skardi-source-pack` and are
//! re-exported here. They moved because the pack needs them and cannot depend
//! on the engine — the dependency runs engine → pack — while `rss`,
//! `model::llm_extract`, `model::remote_embed` and the server all reach them
//! through these paths. Re-exporting keeps one implementation and leaves every
//! existing call site spelled the way it was.
//!
//! They are a reasonable fit for the pack rather than an accident of who
//! needed them: retry-after parsing and request jitter are HTTP client
//! behaviour, canonical JSON and its hash are how an action's identity is
//! computed, and the text bound is what keeps a provider's error body from
//! becoming a log flood. All three are things an Open Connector facade does.

pub use skardi_source_pack::http;
pub use skardi_source_pack::json;
pub use skardi_source_pack::text;

pub mod json_getters;
pub mod json_pack;

/// The hand-rolled mock HTTP server, re-exported on the path it has always
/// had. It now lives in `skardi-source-pack`: the pack's own client suite
/// needs one and cannot depend on the engine, and a second copy here is the
/// duplication the extraction exists to remove. `rss` reaches it through this
/// path and has nothing to do with source packs, so the path stays.
///
/// `#[cfg(test)]` because the pack gates the module behind its `testing`
/// feature, which the engine turns on only as a dev-dependency — exactly the
/// gating the module had when it lived here.
#[cfg(test)]
pub(crate) use skardi_source_pack::mock_http;
