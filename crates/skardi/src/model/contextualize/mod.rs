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
//! concurrently (`CONTEXTUALIZE_CONCURRENCY`; default 4 for hosted
//! providers, 1 for a local Ollama, whose queue makes concurrent calls
//! evict each other's cached prefix) and read from it.
//! Documents longer than `CONTEXTUALIZE_MAX_DOCUMENT_BYTES` (default 32000)
//! are cut into fixed, overlapping segments of that size and each chunk is
//! sent with the segment it sits in, so cost per chunk is bounded no matter
//! how long the document is and chunks of one segment still share a cache.
//!
//! **Data leaves the machine** for `anthropic` and `openai` (unless the
//! OpenAI base URL points at a local server): every chunk plus its whole
//! document is sent to the provider. `ollama` stays local.
//!
//! **Failure is loud.** A provider error fails the statement rather than
//! writing a chunk without its context, so an ingest either lands a
//! document with every chunk contextualised or not at all. The first error
//! stops the call: requests still in flight are dropped and no further
//! chunk is sent, so a failing document is not paid for in full.

pub mod provider;

use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use arrow::array::{Array, StringArray, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use futures::stream;
use futures::{StreamExt, TryStreamExt};

use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::task::block_in_place;

use self::provider::{
    AnthropicProvider, ContextProvider, ContextRequest, OllamaProvider, OpenAiCompatProvider,
};

/// Default cap on the document text sent with each chunk, in UTF-8 bytes
/// (see [`document_segment`]).
pub const DEFAULT_MAX_DOCUMENT_BYTES: usize = 32_000;
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
    max_document_bytes: usize,
    /// `CONTEXTUALIZE_CONCURRENCY`, when set; otherwise each provider's own
    /// default applies.
    concurrency: Option<usize>,
}

impl std::fmt::Debug for ContextualizeRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContextualizeRegistry")
            .field("providers", &self.providers.keys().collect::<Vec<_>>())
            .field("max_document_bytes", &self.max_document_bytes)
            .field("concurrency", &self.concurrency)
            .finish()
    }
}

impl ContextualizeRegistry {
    /// Build from explicit providers and limits (tests, embedders).
    ///
    /// # Example
    ///
    /// ```
    /// use skardi::model::ContextualizeRegistry;
    ///
    /// // No providers: every call reports an unknown provider.
    /// let registry = ContextualizeRegistry::new(Vec::new(), 32_000, Some(1));
    /// ```
    pub fn new(
        providers: Vec<Arc<dyn ContextProvider>>,
        max_document_bytes: usize,
        concurrency: Option<usize>,
    ) -> Self {
        Self {
            providers: providers
                .into_iter()
                .map(|p| (p.name().to_string(), p))
                .collect(),
            max_document_bytes: max_document_bytes.max(1),
            concurrency: concurrency.map(|c| c.max(1)),
        }
    }

    /// Build all three providers from the environment. A provider whose HTTP
    /// client cannot be built is logged and left out, never a panic; keys are
    /// read per request, so nothing here needs a key to be set.
    ///
    /// # Example
    ///
    /// ```
    /// use skardi::model::ContextualizeRegistry;
    ///
    /// let registry = ContextualizeRegistry::from_env();
    /// ```
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
        let max_document_bytes = env::var("CONTEXTUALIZE_MAX_DOCUMENT_BYTES")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(DEFAULT_MAX_DOCUMENT_BYTES);
        let concurrency = env::var("CONTEXTUALIZE_CONCURRENCY")
            .ok()
            .and_then(|v| v.trim().parse().ok());
        Self::new(providers, max_document_bytes, concurrency)
    }

    /// Register the `contextualize` UDF.
    ///
    /// Usage: `contextualize('provider', 'model', document, chunk) -> Utf8`
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    /// use datafusion::prelude::SessionContext;
    /// use skardi::model::ContextualizeRegistry;
    ///
    /// let mut ctx = SessionContext::new();
    /// Arc::new(ContextualizeRegistry::from_env()).register(&mut ctx);
    /// ```
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

/// Round `i` down to the nearest char boundary of `s`.
fn floor_char_boundary(s: &str, i: usize) -> usize {
    let mut i = i.min(s.len());
    while !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

/// Byte range `[start, end)` of `document` sent with `chunk`.
///
/// A document of at most `max_bytes` (UTF-8 bytes) is sent whole. A longer
/// one is cut into **fixed** segments of `max_bytes`, each overlapping the
/// previous by an eighth, and the chunk gets the segment its start falls in
/// (the opening segment if the chunk cannot be located). Because the grid
/// does not move with the chunk, every chunk in the same stretch of the
/// document gets byte-identical text, so the provider's prompt cache is
/// shared by them and they are grouped and sent together. A chunk shorter
/// than the overlap always lies wholly inside its segment.
///
/// The budget is bytes rather than characters so it tracks tokens across
/// scripts: 32 KB is roughly 8k tokens of English and also of Chinese
/// (about 10k characters), where 32k Chinese *characters* would overflow a
/// 16k-token local context. Cuts land on character boundaries.
///
/// # Example
///
/// ```
/// use skardi::model::contextualize::segment_bounds;
///
/// assert_eq!(segment_bounds("short doc", "doc", 100), (0, 9));
/// let doc = "x".repeat(1000);
/// let (start, end) = segment_bounds(&doc, "x", 100);
/// assert!(end - start <= 100);
/// ```
pub fn segment_bounds(document: &str, chunk: &str, max_bytes: usize) -> (usize, usize) {
    let total = document.len();
    if total <= max_bytes {
        return (0, total);
    }
    let max_bytes = max_bytes.max(8);
    let stride = max_bytes - max_bytes / 8;
    let probe: String = chunk.trim().chars().take(200).collect();
    let chunk_at = if probe.is_empty() {
        0
    } else {
        document.find(&probe).unwrap_or(0)
    };
    let last_start = total - max_bytes;
    let start = floor_char_boundary(document, ((chunk_at / stride) * stride).min(last_start));
    let end = floor_char_boundary(document, start + max_bytes);
    (start, end)
}

/// `document[start..end]`, with `[…]` marking each side that was cut.
fn render_segment(document: &str, start: usize, end: usize) -> String {
    let mut out = String::with_capacity(end - start + 2 * ELLIPSIS.len());
    if start > 0 {
        out.push_str(ELLIPSIS.trim_start());
    }
    out.push_str(&document[start..end]);
    if end < document.len() {
        out.push_str(ELLIPSIS.trim_end());
    }
    out
}

/// The text of `document` sent with `chunk`: the [`segment_bounds`] range,
/// marked with `[…]` where it was cut.
///
/// # Example
///
/// ```
/// use skardi::model::contextualize::document_segment;
///
/// assert_eq!(document_segment("abc def", "def", 100), "abc def");
/// let long = format!("{}NEEDLE{}", "a".repeat(500), "b".repeat(500));
/// let seg = document_segment(&long, "NEEDLE", 100);
/// assert!(seg.contains("NEEDLE") && seg.starts_with("[…]"));
/// ```
pub fn document_segment(document: &str, chunk: &str, max_bytes: usize) -> String {
    let (start, end) = segment_bounds(document, chunk, max_bytes);
    render_segment(document, start, end)
}

/// Trim the model's answer and drop a leading `<think>…</think>` block that
/// reasoning models emit, so only the context itself lands in the index.
/// An unterminated `<think>` (the model ran out of output budget while
/// reasoning) leaves nothing; the UDF treats an empty result as an error.
///
/// # Example
///
/// ```
/// use skardi::model::contextualize::clean_context;
///
/// assert_eq!(clean_context("<think>hmm</think> From the guide. "), "From the guide.");
/// assert_eq!(clean_context("<think>never closed"), "");
/// ```
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

fn string_literal(value: &ColumnarValue, label: &str) -> Result<String> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s))) => {
            Ok(s.trim().to_string())
        }
        _ => Err(DataFusionError::Execution(format!(
            "contextualize: {label} must be a non-null string literal"
        ))),
    }
}

/// A text argument, borrowed from Arrow rather than copied per row. A
/// literal (the ingest pipeline's `{content}`) is one value shared by every
/// row: copying it per row is what made a 2 MB document cut into 3,400
/// chunks cost gigabytes.
enum TextArg<'a> {
    Scalar(Option<&'a str>),
    Column(&'a StringArray),
}

impl<'a> TextArg<'a> {
    fn new(value: &'a ColumnarValue, label: &str) -> Result<Self> {
        match value {
            ColumnarValue::Array(arr) => arr
                .as_any()
                .downcast_ref::<StringArray>()
                .map(TextArg::Column)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("contextualize: {label} must be Utf8"))
                }),
            ColumnarValue::Scalar(ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v)) => {
                Ok(TextArg::Scalar(v.as_deref()))
            }
            ColumnarValue::Scalar(ScalarValue::Null) => Ok(TextArg::Scalar(None)),
            _ => Err(DataFusionError::Execution(format!(
                "contextualize: {label} must be Utf8"
            ))),
        }
    }

    fn get(&self, row: usize) -> Option<&'a str> {
        match self {
            TextArg::Scalar(v) => *v,
            TextArg::Column(a) => (row < a.len() && !a.is_null(row)).then(|| a.value(row)),
        }
    }

    fn rows(&self) -> Option<usize> {
        match self {
            TextArg::Scalar(_) => None,
            TextArg::Column(a) => Some(a.len()),
        }
    }
}

/// One row that needs a model call. `document` is the segment sent with
/// it, shared by every chunk of the same stretch of the same document.
struct Job<'a> {
    row: usize,
    document: Arc<str>,
    chunk: &'a str,
}

impl ContextualizeUDF {
    /// Run every job, one document at a time: the first chunk alone (to warm
    /// the provider's prompt cache), the rest concurrently. Results come back
    /// keyed by row.
    async fn run_jobs<'a>(
        &self,
        provider: Arc<dyn ContextProvider>,
        model: &str,
        jobs: Vec<Job<'a>>,
    ) -> anyhow::Result<Vec<(usize, String)>> {
        // Group by segment, keeping first-seen order. Segments are interned
        // (one allocation per distinct text), so pointer equality is text
        // equality.
        let mut groups: Vec<Vec<Job<'a>>> = Vec::new();
        let mut index: HashMap<*const u8, usize> = HashMap::new();
        for job in jobs {
            let key = job.document.as_ptr();
            match index.get(&key) {
                Some(&g) => groups[g].push(job),
                None => {
                    index.insert(key, groups.len());
                    groups.push(vec![job]);
                }
            }
        }

        let concurrency = self
            .registry
            .concurrency
            .unwrap_or_else(|| provider.default_concurrency());
        let mut out = Vec::new();
        for group in groups {
            let mut iter = group.into_iter();
            let Some(first) = iter.next() else { continue };
            let call = |job: Job<'a>| {
                let provider = Arc::clone(&provider);
                async move {
                    let raw = provider
                        .contextualize(ContextRequest {
                            model,
                            document: &job.document,
                            chunk: job.chunk,
                        })
                        .await?;
                    let context = clean_context(&raw);
                    if context.is_empty() {
                        let shown: String = raw.trim().chars().take(200).collect();
                        anyhow::bail!(
                            "{} model '{model}' returned no context for a chunk (answer: {shown:?}); \
                             a reasoning model may have spent its whole output budget thinking",
                            provider.name()
                        );
                    }
                    anyhow::Ok((job.row, context))
                }
            };
            out.push(call(first).await?);
            // try_collect returns on the first error and drops the stream,
            // which cancels the requests still in flight and sends no more.
            let rest: Vec<(usize, String)> = stream::iter(iter.map(call))
                .buffer_unordered(concurrency)
                .try_collect()
                .await?;
            out.extend(rest);
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
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
        let documents = TextArg::new(&args[2], "document (third argument)")?;
        let chunks = TextArg::new(&args[3], "chunk (fourth argument)")?;
        let rows = documents.rows().or(chunks.rows()).unwrap_or(num_rows);

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

        let max_bytes = self.registry.max_document_bytes;
        let mut results: Vec<Option<String>> = vec![None; rows];
        // One allocation per distinct segment, keyed by the borrowed slice
        // and whether each side was cut.
        let mut segments: HashMap<(&str, bool, bool), Arc<str>> = HashMap::new();
        let mut jobs = Vec::new();
        for (row, slot) in results.iter_mut().enumerate() {
            let Some(chunk) = chunks.get(row) else {
                continue;
            };
            if chunk.trim().is_empty() {
                *slot = Some(String::new());
                continue;
            }
            // A NULL document still gets a context from the chunk alone
            // rather than silently skipping the call.
            let document = documents.get(row).unwrap_or(chunk);
            let (start, end) = segment_bounds(document, chunk, max_bytes);
            let key = (&document[start..end], start > 0, end < document.len());
            let segment = segments
                .entry(key)
                .or_insert_with(|| Arc::from(render_segment(document, start, end)));
            jobs.push(Job {
                row,
                document: Arc::clone(segment),
                chunk,
            });
        }

        if !jobs.is_empty() {
            // Provider calls are async; blocking on them needs a worker that
            // can hand its other tasks off, which only the multi-threaded
            // runtime (skardi-server's) has.
            let handle = Handle::try_current()
                .ok()
                .filter(|h| h.runtime_flavor() == RuntimeFlavor::MultiThread)
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "contextualize needs a multi-threaded Tokio runtime".to_string(),
                    )
                })?;
            let done = block_in_place(|| handle.block_on(self.run_jobs(provider, &model, jobs)))
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
    use std::collections::HashSet;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::time::sleep;

    /// Records every request and answers with a context derived from the
    /// chunk, so tests can check row alignment.
    #[derive(Default)]
    struct Recorder {
        seen: Mutex<Vec<(String, String, String)>>,
        in_flight: AtomicUsize,
        peak: AtomicUsize,
        fail_on: Option<&'static str>,
        /// Raw answer to return instead of the default one.
        answer: Option<&'static str>,
        /// Address of every document buffer the provider was handed.
        doc_ptrs: Mutex<HashSet<usize>>,
    }

    #[async_trait]
    impl ContextProvider for Recorder {
        fn name(&self) -> &str {
            "mock"
        }
        async fn contextualize(&self, req: ContextRequest<'_>) -> anyhow::Result<String> {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            sleep(Duration::from_millis(5)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            self.doc_ptrs
                .lock()
                .unwrap()
                .insert(req.document.as_ptr() as usize);
            self.seen.lock().unwrap().push((
                req.model.to_string(),
                req.document.to_string(),
                req.chunk.to_string(),
            ));
            if self.fail_on == Some(req.chunk) {
                anyhow::bail!("provider exploded");
            }
            if let Some(answer) = self.answer {
                return Ok(answer.to_string());
            }
            Ok(format!("  <think>hmm</think> ctx[{}]  ", req.chunk))
        }
    }

    fn ctx_with(recorder: Arc<Recorder>, concurrency: usize) -> SessionContext {
        ctx_with_cap(recorder, concurrency, 50)
    }

    fn ctx_with_cap(recorder: Arc<Recorder>, concurrency: usize, cap: usize) -> SessionContext {
        let mut ctx = SessionContext::new();
        Arc::new(ContextualizeRegistry::new(
            vec![recorder as Arc<dyn ContextProvider>],
            cap,
            Some(concurrency),
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
        assert_eq!(document_segment("abc def", "def", 100), "abc def");
    }

    /// Strip the `[…]` markers to get the document bytes a segment carries.
    fn body(segment: &str) -> String {
        segment.replace("[…]\n", "").replace("\n[…]", "")
    }

    #[test]
    fn chunks_in_the_same_stretch_get_byte_identical_segments() {
        // 8 KB of distinct lines; 1 KB budget → stride 896.
        let doc: String = (0..400).map(|i| format!("line {i:04} of text\n")).collect();
        let a = document_segment(&doc, "line 0101 of text", 1024);
        let b = document_segment(&doc, "line 0110 of text", 1024);
        assert_eq!(a, b, "neighbouring chunks must share one cacheable prefix");
        assert!(a.contains("line 0101") && a.contains("line 0110"));
        assert!(body(&a).len() <= 1024);
        let far = document_segment(&doc, "line 0350 of text", 1024);
        assert_ne!(a, far);
        assert!(far.contains("line 0350"));
    }

    #[test]
    fn a_chunk_shorter_than_the_overlap_is_always_inside_its_segment() {
        let doc: String = (0..400).map(|i| format!("line {i:04} of text\n")).collect();
        for i in (0..395).step_by(7) {
            // Five lines ≈ 90 bytes, well under the 128-byte overlap.
            let chunk: String = (i..i + 5)
                .map(|j| format!("line {j:04} of text\n"))
                .collect();
            let seg = document_segment(&doc, &chunk, 1024);
            assert!(
                seg.contains(chunk.trim()),
                "chunk at line {i} cut by its segment"
            );
        }
    }

    #[test]
    fn the_last_segment_ends_at_the_document_end_on_a_char_boundary() {
        let doc = format!("{}结尾的段落", "中".repeat(500));
        let seg = document_segment(&doc, "结尾的段落", 60);
        assert!(seg.ends_with("结尾的段落"), "{seg}");
        assert!(seg.starts_with("[…]"));
        assert!(body(&seg).len() <= 60);
    }

    #[test]
    fn the_budget_is_bytes_so_cjk_gets_fewer_characters() {
        let doc = "中".repeat(1000); // 3000 bytes
        let seg = document_segment(&doc, "中中", 300);
        assert!(
            body(&seg).chars().count() <= 100,
            "{}",
            body(&seg).chars().count()
        );
    }

    #[test]
    fn unlocatable_chunk_falls_back_to_the_opening() {
        let doc = "x".repeat(300);
        let w = document_segment(&doc, "not in there", 50);
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
    async fn long_documents_reach_the_provider_as_one_segment() {
        let rec = Arc::new(Recorder::default());
        let ctx = ctx_with(Arc::clone(&rec), 1);
        let doc = format!("{}NEEDLE{}", "a".repeat(500), "b".repeat(500));
        let sql = format!("SELECT contextualize('mock', 'm', '{doc}', 'NEEDLE')");
        column(&ctx, &sql).await;
        let seen = rec.seen.lock().unwrap();
        assert!(seen[0].1.contains("NEEDLE"));
        assert!(body(&seen[0].1).len() <= 50, "registry cap is 50 bytes");
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
    async fn the_first_failure_stops_the_remaining_calls() {
        let rec = Arc::new(Recorder {
            fail_on: Some("c1"),
            ..Default::default()
        });
        let ctx = ctx_with(Arc::clone(&rec), 1);
        let rows: Vec<String> = (0..20).map(|i| format!("('c{i}')")).collect();
        let sql = format!(
            "SELECT contextualize('mock', 'm', 'doc', c) FROM (VALUES {}) AS t(c)",
            rows.join(",")
        );
        let err = ctx.sql(&sql).await.unwrap().collect().await.unwrap_err();
        assert!(err.to_string().contains("provider exploded"), "{err}");
        let calls = rec.seen.lock().unwrap().len();
        assert_eq!(calls, 2, "c0 then the failing c1; nothing after it is sent");
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

    /// Chunks of one long document share one buffer per segment: the
    /// document is not copied per chunk (the reviewer's 2 MB / 3,400-chunk
    /// ingest held one copy per chunk and peaked at 7 GB).
    #[tokio::test(flavor = "multi_thread")]
    async fn chunks_share_one_buffer_per_segment_not_one_per_chunk() {
        let rec = Arc::new(Recorder::default());
        let ctx = ctx_with_cap(Arc::clone(&rec), 4, 1024);
        let doc: String = (0..400).map(|i| format!("line {i:04} of text\n")).collect();
        let rows: Vec<String> = (0..400)
            .map(|i| format!("('line {i:04} of text')"))
            .collect();
        let sql = format!(
            "SELECT contextualize('mock', 'm', '{doc}', c) FROM (VALUES {}) AS t(c)",
            rows.join(",")
        );
        let out = column(&ctx, &sql).await;
        assert_eq!(out.len(), 400);
        assert_eq!(rec.seen.lock().unwrap().len(), 400);
        let buffers = rec.doc_ptrs.lock().unwrap().len();
        // 8 KB document, 1 KB segments with a 896-byte stride: 9 segments.
        assert!(buffers <= 10, "{buffers} document buffers for 400 chunks");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_empty_context_fails_instead_of_storing_a_bare_chunk() {
        let rec = Arc::new(Recorder {
            answer: Some("<think>the model ran out of budget while thinking"),
            ..Default::default()
        });
        let ctx = ctx_with(rec, 1);
        let err = ctx
            .sql("SELECT contextualize('mock', 'm', 'doc', 'chunk')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("returned no context"), "{err}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_single_threaded_runtime_is_an_error_not_a_panic() {
        let ctx = ctx_with(Arc::new(Recorder::default()), 1);
        let err = ctx
            .sql("SELECT contextualize('mock', 'm', 'doc', 'chunk')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("multi-threaded"), "{err}");
    }
}
