//! `contextualize` scalar UDF — write a short, search-oriented context for a
//! chunk, given the whole document it came from.
//!
//! This is the first half of Anthropic's *Contextual Retrieval*: a chunk
//! such as "The limit was raised to 50 in March" is unfindable on its own,
//! because nothing in it says *what* limit or *which* product. Asking a
//! model to situate the chunk in its document ("From the Acme rate-limit
//! guide, describing the per-key request limit change.") and indexing
//! `context || chunk` instead of the bare chunk lets both the embedding and
//! the full-text index match on what the chunk is *about*.
//!
//! ```sql
//! SELECT contextualize('ollama', 'qwen2.5:3b', {content}, chunk_text) || '\n\n' || chunk_text
//! FROM (SELECT UNNEST(chunk('markdown', {content}, 1200, 150)) AS chunk_text)
//! ```
//!
//! Arguments: provider (`'anthropic'`, `'openai'`, `'ollama'`), model id,
//! the full document, the chunk. Returns the context text, trimmed; NULL when
//! the chunk is NULL; an empty string for an empty chunk (no call is made).
//!
//! **Cost shape.** The document travels with every chunk. Rows are processed
//! one document at a time: the first chunk of a document is sent alone so it
//! can populate the provider's prompt cache, then the rest go out
//! concurrently (`CONTEXTUALIZE_CONCURRENCY`, default 4) and read from it.
//! Documents longer than `CONTEXTUALIZE_MAX_DOCUMENT_CHARS` (default 32000)
//! are cut to a window of that size centred on the chunk, so cost per chunk
//! is bounded no matter how long the document is.
//!
//! **Data leaves the machine** for `anthropic` and `openai` (unless the
//! OpenAI base URL points at a local server): every chunk plus its whole
//! document is sent to the provider. `ollama` stays local.
//!
//! **Failure is loud.** A provider error fails the statement rather than
//! writing a chunk without its context, so an ingest either lands a
//! document with every chunk contextualised or not at all.

pub mod provider;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, StringArray, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use futures::StreamExt;
use futures::stream;

use self::provider::{
    AnthropicProvider, ContextProvider, ContextRequest, OllamaProvider, OpenAiCompatProvider,
};

/// Default cap on the document text sent with each chunk, in characters.
pub const DEFAULT_MAX_DOCUMENT_CHARS: usize = 32_000;
/// Default number of in-flight requests per document after the first.
pub const DEFAULT_CONCURRENCY: usize = 4;
const ELLIPSIS: &str = "\n[…]\n";

// =============================================================================
// Registry
// =============================================================================

/// Holds the providers and the per-call limits for `contextualize`.
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use datafusion::prelude::SessionContext;
/// use skardi::model::contextualize::ContextualizeRegistry;
///
/// let mut ctx = SessionContext::new();
/// Arc::new(ContextualizeRegistry::from_env()).register(&mut ctx);
/// // SELECT contextualize('ollama', 'qwen2.5:3b', document, chunk) FROM chunks
/// ```
pub struct ContextualizeRegistry {
    providers: HashMap<String, Arc<dyn ContextProvider>>,
    max_document_chars: usize,
    concurrency: usize,
}

impl std::fmt::Debug for ContextualizeRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContextualizeRegistry")
            .field("providers", &self.providers.keys().collect::<Vec<_>>())
            .field("max_document_chars", &self.max_document_chars)
            .field("concurrency", &self.concurrency)
            .finish()
    }
}

impl ContextualizeRegistry {
    /// Build from explicit providers and limits (tests, embedders).
    pub fn new(
        providers: Vec<Arc<dyn ContextProvider>>,
        max_document_chars: usize,
        concurrency: usize,
    ) -> Self {
        Self {
            providers: providers
                .into_iter()
                .map(|p| (p.name().to_string(), p))
                .collect(),
            max_document_chars: max_document_chars.max(1),
            concurrency: concurrency.max(1),
        }
    }

    /// Build all three providers from the environment. A provider whose HTTP
    /// client cannot be built is logged and left out, never a panic; keys are
    /// read per request, so nothing here needs a key to be set.
    pub fn from_env() -> Self {
        let mut providers: Vec<Arc<dyn ContextProvider>> = Vec::new();
        match AnthropicProvider::from_env() {
            Ok(p) => providers.push(Arc::new(p)),
            Err(e) => tracing::warn!("contextualize: anthropic provider unavailable: {e}"),
        }
        match OpenAiCompatProvider::from_env() {
            Ok(p) => providers.push(Arc::new(p)),
            Err(e) => tracing::warn!("contextualize: openai provider unavailable: {e}"),
        }
        match OllamaProvider::from_env() {
            Ok(p) => providers.push(Arc::new(p)),
            Err(e) => tracing::warn!("contextualize: ollama provider unavailable: {e}"),
        }
        let max_document_chars = std::env::var("CONTEXTUALIZE_MAX_DOCUMENT_CHARS")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(DEFAULT_MAX_DOCUMENT_CHARS);
        let concurrency = std::env::var("CONTEXTUALIZE_CONCURRENCY")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(DEFAULT_CONCURRENCY);
        Self::new(providers, max_document_chars, concurrency)
    }

    /// Register the `contextualize` UDF.
    ///
    /// Usage: `contextualize('provider', 'model', document, chunk) -> Utf8`
    pub fn register(self: &Arc<Self>, ctx: &mut SessionContext) {
        ctx.register_udf(ScalarUDF::new_from_impl(ContextualizeUDF {
            registry: Arc::clone(self),
            // Model output → Volatile: never constant-folded or deduplicated.
            signature: Signature::any(4, Volatility::Volatile),
        }));
        tracing::info!("Registered 'contextualize' UDF");
    }
}

// =============================================================================
// Document window and output cleanup
// =============================================================================

/// The part of `document` sent with `chunk`: the whole document when it fits
/// in `max_chars`, otherwise a `max_chars` window centred on where the chunk
/// sits (or the document's opening, if the chunk cannot be located), marked
/// with `[…]` where text was cut. Always cut on character boundaries.
pub fn document_window(document: &str, chunk: &str, max_chars: usize) -> String {
    let total = document.chars().count();
    if total <= max_chars {
        return document.to_string();
    }
    let probe: String = chunk.trim().chars().take(200).collect();
    let chunk_start_char = if probe.is_empty() {
        None
    } else {
        document
            .find(&probe)
            .map(|byte_idx| document[..byte_idx].chars().count())
    };
    let chunk_len = chunk.chars().count();
    let start = match chunk_start_char {
        Some(c) => {
            let centre = c + chunk_len / 2;
            centre.saturating_sub(max_chars / 2).min(total - max_chars)
        }
        None => 0,
    };
    let end = start + max_chars;
    let body: String = document.chars().skip(start).take(max_chars).collect();
    let mut out = String::with_capacity(body.len() + 2 * ELLIPSIS.len());
    if start > 0 {
        out.push_str(ELLIPSIS.trim_start());
    }
    out.push_str(&body);
    if end < total {
        out.push_str(ELLIPSIS.trim_end());
    }
    out
}

/// Trim the model's answer and drop a leading `<think>…</think>` block that
/// reasoning models emit, so only the context itself lands in the index.
pub fn clean_context(raw: &str) -> String {
    let mut text = raw.trim();
    if let Some(rest) = text.strip_prefix("<think>") {
        text = match rest.find("</think>") {
            Some(end) => rest[end + "</think>".len()..].trim(),
            None => "",
        };
    }
    text.to_string()
}

// =============================================================================
// UDF
// =============================================================================

#[derive(Debug)]
struct ContextualizeUDF {
    registry: Arc<ContextualizeRegistry>,
    signature: Signature,
}

impl PartialEq for ContextualizeUDF {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.registry, &other.registry)
    }
}

impl Eq for ContextualizeUDF {}

impl std::hash::Hash for ContextualizeUDF {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.registry).hash(state);
    }
}

fn string_literal(value: &ColumnarValue, label: &str) -> datafusion::common::Result<String> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s))) => {
            Ok(s.trim().to_string())
        }
        _ => Err(DataFusionError::Execution(format!(
            "contextualize: {label} must be a non-null string literal"
        ))),
    }
}

fn utf8_rows(
    value: &ColumnarValue,
    num_rows: usize,
    label: &str,
) -> datafusion::common::Result<Vec<Option<String>>> {
    match value {
        ColumnarValue::Array(arr) => {
            let strings = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                DataFusionError::Execution(format!("contextualize: {label} must be Utf8"))
            })?;
            Ok((0..strings.len())
                .map(|i| (!strings.is_null(i)).then(|| strings.value(i).to_string()))
                .collect())
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v)) => {
            Ok(vec![v.clone(); num_rows])
        }
        ColumnarValue::Scalar(ScalarValue::Null) => Ok(vec![None; num_rows]),
        _ => Err(DataFusionError::Execution(format!(
            "contextualize: {label} must be Utf8"
        ))),
    }
}

/// One row that needs a model call.
struct Job {
    row: usize,
    document: String,
    chunk: String,
}

impl ContextualizeUDF {
    /// Run every job, one document at a time: the first chunk alone (to warm
    /// the provider's prompt cache), the rest concurrently. Results come back
    /// keyed by row.
    async fn run_jobs(
        &self,
        provider: Arc<dyn ContextProvider>,
        model: &str,
        jobs: Vec<Job>,
    ) -> anyhow::Result<Vec<(usize, String)>> {
        // Group by document text, keeping first-seen order.
        let mut groups: Vec<Vec<Job>> = Vec::new();
        let mut index: HashMap<String, usize> = HashMap::new();
        for job in jobs {
            match index.get(&job.document) {
                Some(&g) => groups[g].push(job),
                None => {
                    index.insert(job.document.clone(), groups.len());
                    groups.push(vec![job]);
                }
            }
        }

        let concurrency = self.registry.concurrency;
        let mut out = Vec::new();
        for group in groups {
            let mut iter = group.into_iter();
            let Some(first) = iter.next() else { continue };
            let call = |job: Job| {
                let provider = Arc::clone(&provider);
                async move {
                    let text = provider
                        .contextualize(ContextRequest {
                            model,
                            document: &job.document,
                            chunk: &job.chunk,
                        })
                        .await?;
                    anyhow::Ok((job.row, clean_context(&text)))
                }
            };
            out.push(call(first).await?);
            let rest: Vec<anyhow::Result<(usize, String)>> = stream::iter(iter.map(call))
                .buffer_unordered(concurrency)
                .collect()
                .await;
            for r in rest {
                out.push(r?);
            }
        }
        Ok(out)
    }
}

impl ScalarUDFImpl for ContextualizeUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "contextualize"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let num_rows = args.number_rows;
        let args = args.args;
        if args.len() != 4 {
            return Err(DataFusionError::Execution(
                "contextualize requires 4 arguments: provider, model, document, chunk".to_string(),
            ));
        }
        let provider_name = string_literal(&args[0], "provider (first argument)")?;
        let model = string_literal(&args[1], "model (second argument)")?;
        if model.is_empty() {
            return Err(DataFusionError::Execution(
                "contextualize: model (second argument) must not be empty".to_string(),
            ));
        }
        let documents = utf8_rows(&args[2], num_rows, "document (third argument)")?;
        let chunks = utf8_rows(&args[3], num_rows, "chunk (fourth argument)")?;
        let rows = documents.len().max(chunks.len());

        let provider = self
            .registry
            .providers
            .get(&provider_name.to_ascii_lowercase())
            .cloned()
            .ok_or_else(|| {
                let mut known: Vec<&String> = self.registry.providers.keys().collect();
                known.sort();
                DataFusionError::Execution(format!(
                    "Unknown contextualize provider '{provider_name}'. Available: {known:?}"
                ))
            })?;

        let mut results: Vec<Option<String>> = vec![None; rows];
        let mut jobs = Vec::new();
        for (row, slot) in results.iter_mut().enumerate() {
            let chunk = chunks.get(row).cloned().flatten();
            let Some(chunk) = chunk else { continue };
            if chunk.trim().is_empty() {
                *slot = Some(String::new());
                continue;
            }
            // A NULL document still gets a context from the chunk alone
            // rather than silently skipping the call.
            let document = documents
                .get(row)
                .cloned()
                .flatten()
                .unwrap_or_else(|| chunk.clone());
            jobs.push(Job {
                row,
                document: document_window(&document, &chunk, self.registry.max_document_chars),
                chunk,
            });
        }

        if !jobs.is_empty() {
            let handle = tokio::runtime::Handle::current();
            let done = tokio::task::block_in_place(|| {
                handle.block_on(self.run_jobs(provider, &model, jobs))
            })
            .map_err(|e| {
                tracing::error!("contextualize via '{}' failed: {}", provider_name, e);
                DataFusionError::Execution(format!("contextualize failed: {e}"))
            })?;
            for (row, text) in done {
                results[row] = Some(text);
            }
        }

        let mut builder = StringBuilder::new();
        for r in results {
            builder.append_option(r);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Records every request and answers with a context derived from the
    /// chunk, so tests can check row alignment.
    #[derive(Default)]
    struct Recorder {
        seen: Mutex<Vec<(String, String, String)>>,
        in_flight: AtomicUsize,
        peak: AtomicUsize,
        fail_on: Option<&'static str>,
    }

    #[async_trait]
    impl ContextProvider for Recorder {
        fn name(&self) -> &str {
            "mock"
        }
        async fn contextualize(&self, req: ContextRequest<'_>) -> anyhow::Result<String> {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            self.seen.lock().unwrap().push((
                req.model.to_string(),
                req.document.to_string(),
                req.chunk.to_string(),
            ));
            if self.fail_on == Some(req.chunk) {
                anyhow::bail!("provider exploded");
            }
            Ok(format!("  <think>hmm</think> ctx[{}]  ", req.chunk))
        }
    }

    fn ctx_with(recorder: Arc<Recorder>, concurrency: usize) -> SessionContext {
        let mut ctx = SessionContext::new();
        Arc::new(ContextualizeRegistry::new(
            vec![recorder as Arc<dyn ContextProvider>],
            50,
            concurrency,
        ))
        .register(&mut ctx);
        ctx
    }

    async fn column(ctx: &SessionContext, sql: &str) -> Vec<Option<String>> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        batches
            .iter()
            .flat_map(|b| {
                let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                (0..col.len())
                    .map(|i| (!col.is_null(i)).then(|| col.value(i).to_string()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn short_document_is_sent_whole() {
        assert_eq!(document_window("abc def", "def", 100), "abc def");
    }

    #[test]
    fn long_document_is_windowed_around_the_chunk() {
        let doc = format!("{}TARGET{}", "a".repeat(1000), "b".repeat(1000));
        let w = document_window(&doc, "TARGET", 100);
        assert!(w.contains("TARGET"), "{w}");
        assert!(w.starts_with("[…]") && w.ends_with("[…]"), "{w}");
        assert!(w.chars().filter(|c| *c == 'a' || *c == 'b').count() <= 100);
    }

    #[test]
    fn window_at_the_end_does_not_overrun_and_respects_multibyte_text() {
        let doc = format!("{}结尾的段落", "中".repeat(500));
        let w = document_window(&doc, "结尾的段落", 20);
        assert!(w.ends_with("结尾的段落"), "{w}");
        assert!(w.starts_with("[…]"));
    }

    #[test]
    fn unlocatable_chunk_falls_back_to_the_opening() {
        let doc = "x".repeat(300);
        let w = document_window(&doc, "not in there", 50);
        assert!(w.starts_with('x') && w.ends_with("[…]"), "{w}");
    }

    #[rstest::rstest]
    #[case::plain("  From the retry guide. ", "From the retry guide.")]
    #[case::thinking("<think>reasoning</think>\nFrom the guide.", "From the guide.")]
    #[case::unterminated_thinking("<think>never closed", "")]
    fn clean_context_cases(#[case] raw: &str, #[case] want: &str) {
        assert_eq!(clean_context(raw), want);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn contexts_line_up_with_rows_and_nulls_pass_through() {
        let rec = Arc::new(Recorder::default());
        let ctx = ctx_with(Arc::clone(&rec), 4);
        let out = column(
            &ctx,
            "SELECT contextualize('mock', 'm1', 'the whole doc', c) FROM \
             (VALUES ('one'), (NULL), (''), ('two'), ('three')) AS t(c)",
        )
        .await;
        assert_eq!(
            out,
            vec![
                Some("ctx[one]".to_string()),
                None,
                Some(String::new()),
                Some("ctx[two]".to_string()),
                Some("ctx[three]".to_string()),
            ]
        );
        let seen = rec.seen.lock().unwrap();
        assert_eq!(seen.len(), 3, "NULL and empty chunks make no call");
        assert!(
            seen.iter()
                .all(|(m, d, _)| m == "m1" && d == "the whole doc")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn first_chunk_goes_alone_then_the_rest_in_parallel() {
        let rec = Arc::new(Recorder::default());
        let ctx = ctx_with(Arc::clone(&rec), 3);
        let rows: Vec<String> = (0..8).map(|i| format!("('c{i}')")).collect();
        let sql = format!(
            "SELECT contextualize('mock', 'm', 'doc', c) FROM (VALUES {}) AS t(c)",
            rows.join(",")
        );
        let out = column(&ctx, &sql).await;
        assert_eq!(out.len(), 8);
        let seen = rec.seen.lock().unwrap();
        assert_eq!(seen[0].2, "c0", "the cache-warming call is the first chunk");
        let peak = rec.peak.load(Ordering::SeqCst);
        assert!((2..=3).contains(&peak), "peak in-flight {peak}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn long_documents_reach_the_provider_windowed() {
        let rec = Arc::new(Recorder::default());
        let ctx = ctx_with(Arc::clone(&rec), 1);
        let doc = format!("{}NEEDLE{}", "a".repeat(500), "b".repeat(500));
        let sql = format!("SELECT contextualize('mock', 'm', '{doc}', 'NEEDLE')");
        column(&ctx, &sql).await;
        let seen = rec.seen.lock().unwrap();
        assert!(seen[0].1.contains("NEEDLE"));
        assert!(seen[0].1.chars().count() < 100, "registry cap is 50 chars");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_provider_failure_fails_the_statement() {
        let rec = Arc::new(Recorder {
            fail_on: Some("bad"),
            ..Default::default()
        });
        let ctx = ctx_with(rec, 2);
        let err = ctx
            .sql(
                "SELECT contextualize('mock', 'm', 'doc', c) FROM \
                 (VALUES ('ok'), ('bad'), ('ok2')) AS t(c)",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("provider exploded"), "{err}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_provider_lists_the_known_ones() {
        let ctx = ctx_with(Arc::new(Recorder::default()), 1);
        let err = ctx
            .sql("SELECT contextualize('cohere', 'm', 'doc', 'chunk')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("cohere") && msg.contains("mock"), "{msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn provider_and_model_must_be_literals() {
        let ctx = ctx_with(Arc::new(Recorder::default()), 1);
        let err = ctx
            .sql("SELECT contextualize(c, 'm', 'doc', c) FROM (VALUES ('x')) AS t(c)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("provider"), "{err}");
    }
}
