//! Opt-in live test for `llm_extract` against the configured real provider.
//!
//! Disabled by default. Enable with (defaults to the `deepseek` provider):
//!   LLM_EXTRACT_LIVE=1 DEEPSEEK_API_KEY=sk-... \
//!     cargo test -p skardi --test llm_extract_live --features llm-extract -- --ignored
//!
//! Pick another provider via `LLM_EXTRACT_PROVIDER` + its `<PROVIDER>_API_KEY`
//! (and optionally `LLM_EXTRACT_MODEL`). Hits a real cheap chat model on a tiny
//! sample to catch prompt/schema drift. It is `#[ignore]`d so the default suite
//! stays deterministic and offline.

#![cfg(feature = "llm-extract")]

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use skardi::model::llm_extract::LlmExtractRegistry;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: requires LLM_EXTRACT_LIVE=1 and the selected provider's API key"]
async fn live_extract_from_text() {
    if std::env::var("LLM_EXTRACT_LIVE").ok().as_deref() != Some("1") {
        eprintln!("skipping live test: set LLM_EXTRACT_LIVE=1 to run");
        return;
    }

    // Uses LLM_EXTRACT_PROVIDER / LLM_EXTRACT_MODEL (default: deepseek). The
    // selected provider's API key must be set in the environment, or the query
    // will fail with a clear missing-key error.
    let mut ctx = SessionContext::new();
    Arc::new(LlmExtractRegistry::from_env()).register(&mut ctx);

    let sql = "SELECT UNNEST(llm_extract(t.body, NULL, \
               '{\"type\":\"object\",\"properties\":{\
                 \"name\":{\"type\":\"string\"},\"price\":{\"type\":\"number\"}}}')) AS entity \
               FROM (SELECT 'Widget Pro costs $19.99. Gadget Plus costs $5.' AS body) t";

    let df = ctx.sql(sql).await.expect("plan");
    let batches = df.collect().await.expect("collect");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // At least one entity extracted from the two-product sentence.
    assert!(
        rows >= 1,
        "expected at least one extracted entity, got {rows}"
    );
    println!("live llm_extract produced {rows} entity row(s)");
}
