//! Cross-UDF composition guards.
//!
//! Each test pairs `chunk()` with one embedding UDF and asserts both
//! resolve and plan together on a single `SessionContext`. Tests stop at
//! `ctx.sql(...)` (no `.collect()`) so the embedding UDF is never invoked
//! and no real model needs to exist on disk.
//!
//! These guard the regression class fixed in #128 — a UDF registered on
//! the planning context but missing from the runtime context would let
//! pipeline SQL validate at startup but fail at request time. Catching
//! that in unit tests would require simulating the server's two-context
//! setup; this is the next-best thing: structural assertions that every
//! shipping embedding UDF can coexist with `chunk()` on the same context.

#![cfg(feature = "chunking")]

use std::sync::Arc;

use datafusion::execution::FunctionRegistry;
use datafusion::prelude::SessionContext;
use skardi::model::ChunkingRegistry;

/// SQL fragment that all four tests reuse — chunks a literal markdown
/// body and pipes the chunks through the paired embedding UDF.
fn build_ctx() -> SessionContext {
    let mut ctx = SessionContext::new();
    Arc::new(ChunkingRegistry::new()).register_chunk_udf(&mut ctx);
    ctx
}

#[cfg(feature = "candle")]
#[tokio::test]
async fn chunk_composes_with_candle() {
    use skardi::model::CandleModelRegistry;

    let mut ctx = build_ctx();
    Arc::new(CandleModelRegistry::new()).register_candle_udf(&mut ctx);

    assert!(ctx.udf("chunk").is_ok());
    assert!(ctx.udf("candle").is_ok());

    let sql = "WITH src AS (SELECT 'doc body for chunking' AS body) \
               SELECT chunk_text, \
                      candle('models/does/not/exist', chunk_text) AS embedding \
               FROM ( \
                 SELECT UNNEST(chunk('markdown', body, 50)) AS chunk_text \
                 FROM src \
               )";
    ctx.sql(sql)
        .await
        .expect("chunk + candle composition should plan cleanly");
}

#[cfg(feature = "gguf")]
#[tokio::test]
async fn chunk_composes_with_gguf() {
    use skardi::model::GgufModelRegistry;

    let mut ctx = build_ctx();
    Arc::new(GgufModelRegistry::new()).register_gguf_udf(&mut ctx);

    assert!(ctx.udf("chunk").is_ok());
    assert!(ctx.udf("gguf").is_ok());

    let sql = "WITH src AS (SELECT 'doc body for chunking' AS body) \
               SELECT chunk_text, \
                      gguf('models/does/not/exist', chunk_text) AS embedding \
               FROM ( \
                 SELECT UNNEST(chunk('markdown', body, 50)) AS chunk_text \
                 FROM src \
               )";
    ctx.sql(sql)
        .await
        .expect("chunk + gguf composition should plan cleanly");
}

#[cfg(feature = "onnx")]
#[tokio::test]
async fn chunk_composes_with_onnx_predict() {
    use skardi::model::OnnxModelRegistry;

    let mut ctx = build_ctx();
    Arc::new(OnnxModelRegistry::new()).register_onnx_predict_udf(&mut ctx);

    assert!(ctx.udf("chunk").is_ok());
    assert!(ctx.udf("onnx_predict").is_ok());

    // onnx_predict takes a model path + numeric inputs. Pair it with the
    // chunk-text length so both UDFs land in the same query shape.
    let sql = "WITH src AS (SELECT 'doc body for chunking' AS body) \
               SELECT chunk_text, \
                      onnx_predict('models/does/not/exist.onnx', \
                                   CAST(LENGTH(chunk_text) AS BIGINT)) AS score \
               FROM ( \
                 SELECT UNNEST(chunk('markdown', body, 50)) AS chunk_text \
                 FROM src \
               )";
    ctx.sql(sql)
        .await
        .expect("chunk + onnx_predict composition should plan cleanly");
}

#[cfg(feature = "remote-embed")]
#[tokio::test]
async fn chunk_composes_with_remote_embed() {
    use skardi::model::RemoteEmbedRegistry;

    let mut ctx = build_ctx();
    Arc::new(RemoteEmbedRegistry::new()).register_remote_embed_udf(&mut ctx);

    assert!(ctx.udf("chunk").is_ok());
    assert!(ctx.udf("remote_embed").is_ok());

    // We never `.collect()` — no API call fires, just a planner-level check
    // that both UDFs name-resolve and compose.
    let sql = "WITH src AS (SELECT 'doc body for chunking' AS body) \
               SELECT chunk_text, \
                      remote_embed('openai', 'text-embedding-3-small', chunk_text) \
                        AS embedding \
               FROM ( \
                 SELECT UNNEST(chunk('markdown', body, 50)) AS chunk_text \
                 FROM src \
               )";
    ctx.sql(sql)
        .await
        .expect("chunk + remote_embed composition should plan cleanly");
}
