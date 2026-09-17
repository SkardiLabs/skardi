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
