//! Plan-level composition guard for the `llm_extract` scalar UDF.
//!
//! Registers `llm_extract` (with a mock provider — no network) into a
//! `SessionContext` and asserts that `UNNEST(llm_extract(...))` over a plain
//! text column plans cleanly. The test stops at `ctx.sql(...)` (no `.collect()`)
//! so the UDF is never invoked and no API call fires — this is a structural
//! check that the UDF name-resolves and composes with `UNNEST`, proving it
//! works over any text source with no dependency on the `documents` connector.

#![cfg(feature = "llm-extract")]

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::FunctionRegistry;
use datafusion::prelude::SessionContext;
use serde_json::json;
use skardi::model::llm_extract::LlmExtractRegistry;
use skardi::model::llm_extract::provider::{
    CompletionProvider, CompletionRequest, CompletionResponse,
};

struct MockProvider;

#[async_trait]
impl CompletionProvider for MockProvider {
    async fn complete(&self, _req: CompletionRequest<'_>) -> anyhow::Result<CompletionResponse> {
        Ok(CompletionResponse {
            entities: vec![json!({"model": "x", "_confidence": 0.9})],
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn llm_extract_unnest_composes_over_text_column() {
    let mut ctx = SessionContext::new();
    Arc::new(LlmExtractRegistry::new(Arc::new(MockProvider), 0.75)).register(&mut ctx);

    assert!(ctx.udf("llm_extract").is_ok());

    // UNNEST over the List<Struct<...>> returned by llm_extract, on a plain
    // text column — no documents source involved. We never `.collect()`, so
    // no provider call fires; this is a planner-level composition check (the
    // struct's field set is derived from the json_schema literal at plan
    // time, so a successful plan already proves that resolution works).
    let sql = "SELECT UNNEST(llm_extract(t.body, NULL, \
               '{\"type\":\"object\",\"properties\":{\"model\":{\"type\":\"string\"}}}')) \
               FROM (SELECT 'page body' AS body) t";
    ctx.sql(sql)
        .await
        .expect("llm_extract + UNNEST composition should plan cleanly");
}
