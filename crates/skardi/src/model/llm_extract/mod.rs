//! `llm_extract` scalar UDF — source-agnostic structured extraction over a
//! text column via an LLM completion provider (Anthropic).
//!
//! Mirrors `remote_embed`'s mechanics: a registry holding a shared provider,
//! a `ScalarUDFImpl` returning a `List<Utf8>` per row (caller `UNNEST`s), and
//! an async→sync bridge for outbound calls. No dependency on the `documents`
//! connector.
