//! `rerank` scalar UDF — score (query, passage) pairs with a local
//! cross-encoder reranker, loaded through candle.
//!
//! A bi-encoder (`candle(...)`) embeds the query and each passage separately
//! and compares vectors; a cross-encoder reads the query and the passage
//! together and outputs one relevance logit, which is slower per pair but
//! ranks noticeably better. The intended use is the second stage of a
//! retrieval pipeline: a cheap first stage (vector + full-text) fetches a few
//! dozen candidates, `rerank` re-scores just those, and the pipeline keeps the
//! top K:
//!
//! ```sql
//! WITH candidates AS ( ... hybrid search, LIMIT 50 ... )
//! SELECT id, source, content,
//!        rerank('/models/bge-reranker-base', {query}, content) AS rerank_score
//! FROM candidates
//! ORDER BY rerank_score DESC NULLS LAST
//! LIMIT {limit}
//! ```
//!
//! Supported checkpoints, chosen by `config.json`'s `architectures`:
//!
//! | `architectures` | Examples |
//! |---|---|
//! | `XLMRobertaForSequenceClassification` | `BAAI/bge-reranker-base`, `bge-reranker-large`, `bge-reranker-v2-m3` |
//! | `BertForSequenceClassification` | `cross-encoder/ms-marco-MiniLM-L-6-v2`, `ms-marco-MiniLM-L-12-v2` |
//!
//! Only single-logit heads (`num_labels == 1`, which is what every reranker
//! above ships) are accepted; the score is that raw logit, so higher is more
//! relevant and the values are only comparable within one model.
//!
//! Everything runs in the server process — the query and passages never leave
//! the machine.

use std::collections::HashMap;
use std::fs;
use std::iter::repeat_n;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use anyhow::{Result, anyhow};
use arrow::array::{Array, Float32Array, StringArray};
use arrow::datatypes::DataType;
use candle_core::{
    DType, Device, Module, Result as CandleResult, Tensor, safetensors as candle_safetensors,
};
use candle_nn::{Linear, VarBuilder, linear};
use candle_transformers::models::{bert, xlm_roberta};
use datafusion::common::Result as DfResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use tokenizers::{
    PaddingDirection, PaddingParams, PaddingStrategy, Tokenizer, TruncationDirection,
    TruncationParams, TruncationStrategy,
};
use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::task::block_in_place;

/// Pairs scored per forward pass. Padding cost grows with the longest pair in
/// a batch, so this stays small; 16 measured no slower than 32 on CPU for
/// ~300-token pairs while keeping peak memory modest.
const BATCH_SIZE: usize = 16;

/// Hard ceiling on tokens per (query, passage) pair. Every supported family
/// is trained at 512; going past the position table is the
/// `index-select invalid index 512` failure the embedding path documents.
const MAX_PAIR_TOKENS: usize = 512;

// =============================================================================
// Cross-encoder backends
// =============================================================================

trait CrossEncoderBackend: Send + Sync {
    /// Forward a padded batch: returns logits `[batch, num_labels]`.
    fn logits(
        &self,
        input_ids: &Tensor,
        attention_mask: &Tensor,
        token_type_ids: &Tensor,
    ) -> CandleResult<Tensor>;
}

struct XlmRobertaBackend(xlm_roberta::XLMRobertaForSequenceClassification);

impl CrossEncoderBackend for XlmRobertaBackend {
    fn logits(
        &self,
        input_ids: &Tensor,
        attention_mask: &Tensor,
        token_type_ids: &Tensor,
    ) -> CandleResult<Tensor> {
        self.0.forward(input_ids, attention_mask, token_type_ids)
    }
}

/// `BertForSequenceClassification`: encoder, then the pooler (dense + tanh
/// over the CLS token), then a linear classifier. candle ships the encoder
/// only, so the two heads are built here from the checkpoint's own weights.
struct BertCrossEncoder {
    bert: bert::BertModel,
    pooler: Linear,
    classifier: Linear,
}

impl CrossEncoderBackend for BertCrossEncoder {
    fn logits(
        &self,
        input_ids: &Tensor,
        attention_mask: &Tensor,
        token_type_ids: &Tensor,
    ) -> CandleResult<Tensor> {
        let hidden = self
            .bert
            .forward(input_ids, token_type_ids, Some(attention_mask))?;
        let cls = hidden.get_on_dim(1, 0)?.contiguous()?;
        let pooled = self.pooler.forward(&cls)?.tanh()?;
        self.classifier.forward(&pooled)
    }
}

#[derive(serde::Deserialize)]
struct RawConfig {
    #[serde(default)]
    architectures: Vec<String>,
    #[serde(default)]
    id2label: Option<HashMap<String, String>>,
    #[serde(default)]
    num_labels: Option<usize>,
    #[serde(default)]
    max_position_embeddings: Option<usize>,
    #[serde(default)]
    pad_token_id: Option<u32>,
}

impl RawConfig {
    fn num_labels(&self) -> usize {
        self.num_labels
            .or_else(|| self.id2label.as_ref().map(|m| m.len()))
            .unwrap_or(1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RerankArchitecture {
    XlmRoberta,
    Bert,
}

impl RerankArchitecture {
    fn detect(architectures: &[String]) -> Result<Self> {
        match architectures.first().map(String::as_str) {
            Some("XLMRobertaForSequenceClassification") => Ok(Self::XlmRoberta),
            Some("BertForSequenceClassification") => Ok(Self::Bert),
            Some(other) => Err(anyhow!(
                "unsupported reranker architecture '{other}': rerank loads \
                 XLMRobertaForSequenceClassification (bge-reranker-*) or \
                 BertForSequenceClassification (ms-marco cross-encoders)"
            )),
            None => Err(anyhow!(
                "config.json has no `architectures` entry, so the reranker head cannot be chosen"
            )),
        }
    }
}

// =============================================================================
// RerankModel — public API
// =============================================================================

/// A loaded cross-encoder reranker and its tokenizer.
///
/// # Example
///
/// ```no_run
/// use skardi::model::candle::rerank::RerankModel;
///
/// let model = RerankModel::from_dir("/models/bge-reranker-base").unwrap();
/// let scores = model
///     .score("how do I rotate the API key?", &["Keys rotate via ...", "Pricing ..."])
///     .unwrap();
/// assert_eq!(scores.len(), 2);
/// ```
pub struct RerankModel {
    backend: Box<dyn CrossEncoderBackend>,
    tokenizer: Tokenizer,
    device: Device,
    /// Whether the backend reads `token_type_ids` from the pair encoding.
    /// XLM-R checkpoints have a single type embedding, so they get zeros.
    uses_segment_ids: bool,
}

impl RerankModel {
    /// Load a reranker from a directory holding one `.safetensors` file,
    /// `config.json` and `tokenizer.json`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use skardi::model::candle::rerank::RerankModel;
    ///
    /// let model = RerankModel::from_dir("/models/bge-reranker-base").unwrap();
    /// ```
    pub fn from_dir(model_dir: &str) -> Result<Self> {
        let dir = Path::new(model_dir);
        let weights_path = find_weights(dir, model_dir)?;
        let config_str = fs::read_to_string(dir.join("config.json"))
            .map_err(|e| anyhow!("Failed to read config.json in '{model_dir}': {e}"))?;
        let raw: RawConfig = serde_json::from_str(&config_str)
            .map_err(|e| anyhow!("Failed to parse config.json: {e}"))?;
        let arch = RerankArchitecture::detect(&raw.architectures)?;
        let num_labels = raw.num_labels();
        if num_labels != 1 {
            return Err(anyhow!(
                "reranker head has {num_labels} labels; rerank needs a single relevance logit \
                 (num_labels = 1), which is what bge-reranker and ms-marco cross-encoders ship"
            ));
        }

        let mut tokenizer = Tokenizer::from_file(dir.join("tokenizer.json"))
            .map_err(|e| anyhow!("Failed to load tokenizer.json in '{model_dir}': {e}"))?;
        // RoBERTa-style models reserve two positions (padding_idx + 1 offset),
        // so their usable length is max_position_embeddings - 2.
        let position_cap = match (arch, raw.max_position_embeddings) {
            (RerankArchitecture::XlmRoberta, Some(n)) => n.saturating_sub(2),
            (RerankArchitecture::Bert, Some(n)) => n,
            (_, None) => MAX_PAIR_TOKENS,
        };
        let max_length = position_cap.min(MAX_PAIR_TOKENS);
        let pad_id = raw.pad_token_id.unwrap_or(0);
        let pad_token = tokenizer
            .id_to_token(pad_id)
            .unwrap_or_else(|| "[PAD]".to_string());
        // Cut the longer side first. For the usual short query and long
        // passage that trims only the passage; a query that alone fills the
        // budget is trimmed too instead of failing the pair, which is what
        // `OnlySecond` does once the query leaves no room for the passage.
        tokenizer
            .with_truncation(Some(TruncationParams {
                max_length,
                strategy: TruncationStrategy::LongestFirst,
                stride: 0,
                direction: TruncationDirection::Right,
            }))
            .map_err(|e| anyhow!("Failed to configure tokenizer truncation: {e}"))?;
        tokenizer.with_padding(Some(PaddingParams {
            strategy: PaddingStrategy::BatchLongest,
            direction: PaddingDirection::Right,
            pad_to_multiple_of: None,
            pad_id,
            pad_type_id: 0,
            pad_token,
        }));

        let device = Device::Cpu;
        let tensors = candle_safetensors::load(&weights_path, &device)
            .map_err(|e| anyhow!("Failed to load safetensors weights: {e}"))?;
        let vb = VarBuilder::from_tensors(tensors, DType::F32, &device);

        let (backend, uses_segment_ids): (Box<dyn CrossEncoderBackend>, bool) = match arch {
            RerankArchitecture::XlmRoberta => {
                let config: xlm_roberta::Config = serde_json::from_str(&config_str)
                    .map_err(|e| anyhow!("Failed to parse XLM-RoBERTa config: {e}"))?;
                let model =
                    xlm_roberta::XLMRobertaForSequenceClassification::new(num_labels, &config, vb)
                        .map_err(|e| anyhow!("Failed to build XLM-RoBERTa reranker: {e}"))?;
                (Box::new(XlmRobertaBackend(model)), false)
            }
            RerankArchitecture::Bert => {
                let config: bert::Config = serde_json::from_str(&config_str)
                    .map_err(|e| anyhow!("Failed to parse BERT config: {e}"))?;
                let hidden = config.hidden_size;
                let model = bert::BertModel::load(vb.pp("bert"), &config)
                    .map_err(|e| anyhow!("Failed to build BERT encoder: {e}"))?;
                let pooler = linear(hidden, hidden, vb.pp("bert.pooler.dense"))
                    .map_err(|e| anyhow!("Failed to load BERT pooler: {e}"))?;
                let classifier = linear(hidden, num_labels, vb.pp("classifier"))
                    .map_err(|e| anyhow!("Failed to load classifier head: {e}"))?;
                (
                    Box::new(BertCrossEncoder {
                        bert: model,
                        pooler,
                        classifier,
                    }),
                    true,
                )
            }
        };

        tracing::info!(
            "Loaded {:?} reranker from '{}' (max {} tokens per pair)",
            arch,
            model_dir,
            max_length
        );
        Ok(Self {
            backend,
            tokenizer,
            device,
            uses_segment_ids,
        })
    }

    /// Score each passage against `query`. Higher is more relevant.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use skardi::model::candle::rerank::RerankModel;
    ///
    /// let model = RerankModel::from_dir("/models/bge-reranker-base").unwrap();
    /// let scores = model.score("retry limit", &["Retries stop after 5.", "Unrelated."]).unwrap();
    /// assert_eq!(scores.len(), 2);
    /// ```
    pub fn score(&self, query: &str, passages: &[&str]) -> Result<Vec<f32>> {
        let pairs: Vec<(&str, &str)> = passages.iter().map(|p| (query, *p)).collect();
        self.score_pairs(&pairs)
    }

    /// Score arbitrary (query, passage) pairs, in input order.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use skardi::model::candle::rerank::RerankModel;
    ///
    /// let model = RerankModel::from_dir("/models/bge-reranker-base").unwrap();
    /// let scores = model.score_pairs(&[("retry limit", "Retries stop after 5.")]).unwrap();
    /// assert_eq!(scores.len(), 1);
    /// ```
    pub fn score_pairs(&self, pairs: &[(&str, &str)]) -> Result<Vec<f32>> {
        let mut scores = Vec::with_capacity(pairs.len());
        for batch in pairs.chunks(BATCH_SIZE) {
            scores.extend(self.score_batch(batch)?);
        }
        Ok(scores)
    }

    fn score_batch(&self, pairs: &[(&str, &str)]) -> Result<Vec<f32>> {
        let inputs: Vec<(String, String)> = pairs
            .iter()
            .map(|(q, p)| ((*q).to_string(), (*p).to_string()))
            .collect();
        let encodings = self
            .tokenizer
            .encode_batch(inputs, true)
            .map_err(|e| anyhow!("Tokenization failed: {e}"))?;
        let batch = encodings.len();
        let seq_len = encodings.first().map(|e| e.get_ids().len()).unwrap_or(0);

        let mut ids = Vec::with_capacity(batch * seq_len);
        let mut mask = Vec::with_capacity(batch * seq_len);
        let mut types = Vec::with_capacity(batch * seq_len);
        for enc in &encodings {
            ids.extend_from_slice(enc.get_ids());
            mask.extend_from_slice(enc.get_attention_mask());
            if self.uses_segment_ids {
                types.extend_from_slice(enc.get_type_ids());
            } else {
                types.extend(repeat_n(0u32, enc.get_ids().len()));
            }
        }
        let shape = (batch, seq_len);
        let input_ids = Tensor::from_vec(ids, shape, &self.device)?;
        let attention_mask = Tensor::from_vec(mask, shape, &self.device)?;
        let token_type_ids = Tensor::from_vec(types, shape, &self.device)?;

        let logits = self
            .backend
            .logits(&input_ids, &attention_mask, &token_type_ids)
            .map_err(|e| anyhow!("Reranker forward pass failed: {e}"))?;
        let scores = logits
            .squeeze(1)
            .and_then(|t| t.to_dtype(DType::F32))
            .and_then(|t| t.to_vec1::<f32>())
            .map_err(|e| anyhow!("Unexpected reranker output shape: {e}"))?;
        Ok(scores)
    }
}

/// The checkpoint's weights: `model.safetensors` if present, otherwise the
/// directory's only `.safetensors` file. Several files without a
/// `model.safetensors` is a sharded checkpoint, which this loader does not
/// stitch together, so it is refused rather than half-loaded.
fn find_weights(dir: &Path, model_dir: &str) -> Result<PathBuf> {
    let canonical = dir.join("model.safetensors");
    if canonical.exists() {
        return Ok(canonical);
    }
    let mut files: Vec<_> = fs::read_dir(dir)
        .map_err(|e| anyhow!("Cannot read model directory '{model_dir}': {e}"))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("safetensors"))
        .collect();
    files.sort();
    match files.len() {
        0 => Err(anyhow!("No .safetensors file found in '{model_dir}'")),
        1 => Ok(files.remove(0)),
        n => Err(anyhow!(
            "'{model_dir}' holds {n} .safetensors files and no model.safetensors; \
             sharded checkpoints are not supported"
        )),
    }
}

// =============================================================================
// RerankModelRegistry — lazy model cache keyed by directory
// =============================================================================

/// Lazily loads and caches [`RerankModel`]s keyed by model directory.
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use datafusion::prelude::SessionContext;
/// use skardi::model::candle::rerank::RerankModelRegistry;
///
/// let mut ctx = SessionContext::new();
/// Arc::new(RerankModelRegistry::new()).register_rerank_udf(&mut ctx);
/// // SELECT rerank('/models/bge-reranker-base', 'query', content) FROM docs
/// ```
pub struct RerankModelRegistry {
    models: RwLock<HashMap<String, Arc<RerankModel>>>,
    /// Held while a checkpoint loads, so concurrent first uses of a
    /// reranker load it once instead of each holding a full copy.
    load_lock: Mutex<()>,
}

impl std::fmt::Debug for RerankModelRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let models = self.models.read().unwrap_or_else(|p| p.into_inner());
        f.debug_struct("RerankModelRegistry")
            .field("loaded_models", &models.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl Default for RerankModelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl RerankModelRegistry {
    /// An empty registry; models load on first use.
    ///
    /// # Example
    ///
    /// ```
    /// use skardi::model::RerankModelRegistry;
    ///
    /// let registry = RerankModelRegistry::new();
    /// ```
    pub fn new() -> Self {
        Self {
            models: RwLock::new(HashMap::new()),
            load_lock: Mutex::new(()),
        }
    }

    /// Get or lazily load a reranker by directory path.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use skardi::model::RerankModelRegistry;
    ///
    /// let registry = RerankModelRegistry::new();
    /// let model = registry.get_or_load("/models/bge-reranker-base").unwrap();
    /// ```
    pub fn get_or_load(&self, model_dir: &str) -> Result<Arc<RerankModel>> {
        {
            let models = self.models.read().unwrap_or_else(|p| p.into_inner());
            if let Some(model) = models.get(model_dir) {
                return Ok(Arc::clone(model));
            }
        }
        let _loading = self.load_lock.lock().unwrap_or_else(|p| p.into_inner());
        // Another caller may have finished loading while we waited.
        if let Some(model) = self
            .models
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(model_dir)
        {
            return Ok(Arc::clone(model));
        }
        let model = Arc::new(RerankModel::from_dir(model_dir)?);
        let mut models = self.models.write().unwrap_or_else(|p| p.into_inner());
        Ok(Arc::clone(
            models.entry(model_dir.to_string()).or_insert(model),
        ))
    }

    /// Register the `rerank` UDF.
    ///
    /// Usage: `rerank('path/to/reranker_dir', query, passage_col) -> Float32`
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    /// use datafusion::prelude::SessionContext;
    /// use skardi::model::RerankModelRegistry;
    ///
    /// let mut ctx = SessionContext::new();
    /// Arc::new(RerankModelRegistry::new()).register_rerank_udf(&mut ctx);
    /// ```
    pub fn register_rerank_udf(self: &Arc<Self>, ctx: &mut SessionContext) {
        ctx.register_udf(ScalarUDF::new_from_impl(RerankUDF::new(Arc::clone(self))));
        tracing::info!("Registered 'rerank' UDF");
    }
}

// =============================================================================
// RerankUDF
// =============================================================================

#[derive(Debug)]
struct RerankUDF {
    registry: Arc<RerankModelRegistry>,
    signature: Signature,
}

impl PartialEq for RerankUDF {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.registry, &other.registry)
    }
}

impl Eq for RerankUDF {}

impl std::hash::Hash for RerankUDF {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.registry).hash(state);
    }
}

impl RerankUDF {
    fn new(registry: Arc<RerankModelRegistry>) -> Self {
        Self {
            registry,
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

/// A Utf8 argument as one value per row: an array is read row by row, a
/// scalar is broadcast. `None` marks SQL NULL.
fn utf8_rows(value: &ColumnarValue, num_rows: usize, label: &str) -> DfResult<Vec<Option<String>>> {
    match value {
        ColumnarValue::Array(arr) => {
            let strings = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                DataFusionError::Execution(format!("rerank: {label} must be a Utf8 value"))
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
            "rerank: {label} must be a Utf8 value"
        ))),
    }
}

impl ScalarUDFImpl for RerankUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "rerank"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let num_rows = args.number_rows;
        let args = args.args;
        if args.len() != 3 {
            return Err(DataFusionError::Execution(
                "rerank requires 3 arguments: model_dir, query, passage".to_string(),
            ));
        }
        let model_dir = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(path))) => path.clone(),
            _ => {
                return Err(DataFusionError::Execution(
                    "rerank: first argument (model_dir) must be a non-null string literal"
                        .to_string(),
                ));
            }
        };
        let queries = utf8_rows(&args[1], num_rows, "query (second argument)")?;
        let passages = utf8_rows(&args[2], num_rows, "passage (third argument)")?;
        let rows = queries.len().max(passages.len());

        // NULL query or passage → NULL score, without spending a forward pass.
        let mut pairs: Vec<(&str, &str)> = Vec::with_capacity(rows);
        let mut slots: Vec<Option<usize>> = Vec::with_capacity(rows);
        for i in 0..rows {
            match (
                queries.get(i).and_then(|q| q.as_deref()),
                passages.get(i).and_then(|p| p.as_deref()),
            ) {
                (Some(q), Some(p)) => {
                    slots.push(Some(pairs.len()));
                    pairs.push((q, p));
                }
                _ => slots.push(None),
            }
        }

        let scores = if pairs.is_empty() {
            Vec::new()
        } else {
            let model = self.registry.get_or_load(&model_dir).map_err(|e| {
                DataFusionError::Execution(format!("Failed to load reranker '{model_dir}': {e}"))
            })?;
            // Forward passes are seconds of CPU work; on a multi-threaded
            // runtime tell Tokio so other tasks move off this worker.
            let run = || model.score_pairs(&pairs);
            let result = match Handle::try_current() {
                Ok(h) if h.runtime_flavor() == RuntimeFlavor::MultiThread => block_in_place(run),
                _ => run(),
            };
            result.map_err(|e| {
                tracing::error!("rerank failed for model '{}': {}", model_dir, e);
                DataFusionError::Execution(format!("Rerank failed: {e}"))
            })?
        };

        let out: Float32Array = slots
            .into_iter()
            .map(|slot| slot.map(|i| scores[i]))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::RecordBatch;
    use candle_nn::VarMap;
    use datafusion::datasource::MemTable;
    use rstest::rstest;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokenizers::models::wordlevel::WordLevel;
    use tokenizers::pre_tokenizers::whitespace::Whitespace;
    use tokenizers::processors::template::TemplateProcessing;

    const VOCAB: &[&str] = &[
        "[PAD]",
        "[UNK]",
        "[CLS]",
        "[SEP]",
        "retry",
        "backoff",
        "pricing",
        "how",
        "does",
        "work",
        "the",
        "plan",
        "costs",
        "money",
        "exponential",
        "waits",
        "between",
        "attempts",
    ];

    /// Write a tiny randomly-initialised cross-encoder plus a word-level
    /// tokenizer into a temp dir, in the exact on-disk layout a real
    /// checkpoint uses. Scores are meaningless; the tests check plumbing —
    /// shapes, batching, truncation, NULLs — not relevance.
    fn tiny_model(arch: RerankArchitecture) -> TempDir {
        let dir = tempfile::tempdir().unwrap();
        let hidden = 16;
        let max_pos = 34; // XLM-R: 32 usable after the 2-slot offset
        let pad_id = match arch {
            RerankArchitecture::XlmRoberta => 1u32,
            RerankArchitecture::Bert => 0u32,
        };

        // Tokenizer: word-level vocab, BERT-style pair template.
        let mut vocab: HashMap<String, u32> = HashMap::new();
        for (i, tok) in VOCAB.iter().enumerate() {
            vocab.insert((*tok).to_string(), i as u32);
        }
        // XLM-R pads with id 1; give that id the pad token so padding is
        // recognised by position-id computation.
        if arch == RerankArchitecture::XlmRoberta {
            vocab.insert("[PAD]".to_string(), 1);
            vocab.insert("[UNK]".to_string(), 0);
        }
        let model = WordLevel::builder()
            .vocab(vocab.into_iter().collect())
            .unk_token("[UNK]".to_string())
            .build()
            .unwrap();
        let mut tokenizer = Tokenizer::new(model);
        tokenizer.with_pre_tokenizer(Some(Whitespace {}));
        tokenizer.with_post_processor(Some(
            TemplateProcessing::builder()
                .try_single("[CLS] $A [SEP]")
                .unwrap()
                .try_pair("[CLS] $A:0 [SEP]:0 $B:1 [SEP]:1")
                .unwrap()
                .special_tokens(vec![("[CLS]", 2), ("[SEP]", 3)])
                .build()
                .unwrap(),
        ));
        tokenizer
            .save(dir.path().join("tokenizer.json"), false)
            .unwrap();

        let device = Device::Cpu;
        let varmap = VarMap::new();
        let vb = VarBuilder::from_varmap(&varmap, DType::F32, &device);
        let config = match arch {
            RerankArchitecture::XlmRoberta => {
                let cfg = serde_json::json!({
                    "architectures": ["XLMRobertaForSequenceClassification"],
                    "id2label": {"0": "LABEL_0"},
                    "hidden_size": hidden, "layer_norm_eps": 1e-5,
                    "attention_probs_dropout_prob": 0.0, "hidden_dropout_prob": 0.0,
                    "num_attention_heads": 2, "position_embedding_type": "absolute",
                    "intermediate_size": 32, "hidden_act": "gelu",
                    "num_hidden_layers": 2, "vocab_size": VOCAB.len(),
                    "max_position_embeddings": max_pos, "type_vocab_size": 1,
                    "pad_token_id": pad_id
                });
                let parsed: xlm_roberta::Config = serde_json::from_value(cfg.clone()).unwrap();
                xlm_roberta::XLMRobertaForSequenceClassification::new(1, &parsed, vb).unwrap();
                cfg
            }
            RerankArchitecture::Bert => {
                let cfg = serde_json::json!({
                    "architectures": ["BertForSequenceClassification"],
                    "id2label": {"0": "LABEL_0"},
                    "hidden_size": hidden, "layer_norm_eps": 1e-12,
                    "hidden_dropout_prob": 0.0, "num_attention_heads": 2,
                    "intermediate_size": 32, "hidden_act": "gelu",
                    "num_hidden_layers": 2, "vocab_size": VOCAB.len(),
                    "max_position_embeddings": max_pos, "type_vocab_size": 2,
                    "initializer_range": 0.02, "pad_token_id": pad_id,
                    "position_embedding_type": "absolute", "use_cache": false,
                    "model_type": "bert"
                });
                let parsed: bert::Config = serde_json::from_value(cfg.clone()).unwrap();
                bert::BertModel::load(vb.pp("bert"), &parsed).unwrap();
                linear(hidden, hidden, vb.pp("bert.pooler.dense")).unwrap();
                linear(hidden, 1, vb.pp("classifier")).unwrap();
                cfg
            }
        };
        std::fs::write(
            dir.path().join("config.json"),
            serde_json::to_string(&config).unwrap(),
        )
        .unwrap();
        varmap.save(dir.path().join("model.safetensors")).unwrap();
        dir
    }

    #[rstest]
    #[case::xlm_roberta(RerankArchitecture::XlmRoberta)]
    #[case::bert(RerankArchitecture::Bert)]
    fn batched_scores_match_one_at_a_time(#[case] arch: RerankArchitecture) {
        let dir = tiny_model(arch);
        let model = RerankModel::from_dir(dir.path().to_str().unwrap()).unwrap();
        let passages = [
            "exponential backoff waits between attempts",
            "pricing",
            "the plan costs money and the plan costs money",
        ];
        let batched = model
            .score("how does retry backoff work", &passages)
            .unwrap();
        assert_eq!(batched.len(), 3);
        for (i, p) in passages.iter().enumerate() {
            let single = model.score("how does retry backoff work", &[p]).unwrap();
            assert!(
                (single[0] - batched[i]).abs() < 1e-4,
                "padding changed the score of passage {i}: single {} vs batched {}",
                single[0],
                batched[i]
            );
        }
    }

    #[rstest]
    #[case::xlm_roberta(RerankArchitecture::XlmRoberta)]
    #[case::bert(RerankArchitecture::Bert)]
    fn passage_longer_than_position_table_is_truncated_not_an_error(
        #[case] arch: RerankArchitecture,
    ) {
        let dir = tiny_model(arch);
        let model = RerankModel::from_dir(dir.path().to_str().unwrap()).unwrap();
        // 200 tokens against a 32-34 position table: without truncation this
        // is the `index-select invalid index` failure.
        let long = "retry backoff ".repeat(100);
        let scores = model
            .score("how does retry work", &[long.as_str()])
            .unwrap();
        assert_eq!(scores.len(), 1);
        assert!(scores[0].is_finite());
    }

    #[rstest]
    #[case::xlm_roberta(RerankArchitecture::XlmRoberta)]
    #[case::bert(RerankArchitecture::Bert)]
    fn query_that_fills_the_budget_is_trimmed_not_an_error(#[case] arch: RerankArchitecture) {
        let dir = tiny_model(arch);
        let model = RerankModel::from_dir(dir.path().to_str().unwrap()).unwrap();
        // The query alone is longer than the 32-token pair budget.
        let query = "how does retry backoff work ".repeat(20);
        let scores = model
            .score(&query, &["exponential backoff waits between attempts"])
            .unwrap();
        assert!(scores[0].is_finite());
    }

    #[test]
    fn sharded_checkpoint_is_refused() {
        let dir = tiny_model(RerankArchitecture::Bert);
        let weights = dir.path().join("model.safetensors");
        std::fs::copy(
            &weights,
            dir.path().join("model-00001-of-00002.safetensors"),
        )
        .unwrap();
        std::fs::rename(
            &weights,
            dir.path().join("model-00002-of-00002.safetensors"),
        )
        .unwrap();
        let err = RerankModel::from_dir(dir.path().to_str().unwrap())
            .err()
            .expect("two shards without model.safetensors must be refused");
        assert!(err.to_string().contains("sharded"), "{err}");
    }

    #[test]
    fn more_pairs_than_one_batch_keep_their_order() {
        let dir = tiny_model(RerankArchitecture::Bert);
        let model = RerankModel::from_dir(dir.path().to_str().unwrap()).unwrap();
        let passages: Vec<String> = (0..(BATCH_SIZE * 2 + 3))
            .map(|i| "retry ".repeat(i % 5 + 1))
            .collect();
        let refs: Vec<&str> = passages.iter().map(String::as_str).collect();
        let all = model.score("retry", &refs).unwrap();
        assert_eq!(all.len(), refs.len());
        let last = model.score("retry", &[refs[refs.len() - 1]]).unwrap();
        assert!((all[refs.len() - 1] - last[0]).abs() < 1e-4);
    }

    #[test]
    fn unsupported_architecture_is_named_in_the_error() {
        let dir = tiny_model(RerankArchitecture::Bert);
        std::fs::write(
            dir.path().join("config.json"),
            r#"{"architectures": ["BertModel"]}"#,
        )
        .unwrap();
        let err = RerankModel::from_dir(dir.path().to_str().unwrap())
            .err()
            .expect("an embedding checkpoint must be refused");
        assert!(err.to_string().contains("BertModel"), "{err}");
    }

    #[test]
    fn multi_label_head_is_refused() {
        let dir = tiny_model(RerankArchitecture::Bert);
        let path = dir.path().join("config.json");
        let mut cfg: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        cfg["id2label"] = serde_json::json!({"0": "neg", "1": "pos"});
        std::fs::write(&path, cfg.to_string()).unwrap();
        let err = RerankModel::from_dir(dir.path().to_str().unwrap())
            .err()
            .expect("a two-label head must be refused");
        assert!(err.to_string().contains("2 labels"), "{err}");
    }

    async fn ctx_with_passages(passages: Vec<Option<&str>>) -> SessionContext {
        let mut ctx = SessionContext::new();
        Arc::new(RerankModelRegistry::new()).register_rerank_udf(&mut ctx);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, false),
            arrow::datatypes::Field::new("content", DataType::Utf8, true),
        ]));
        let ids: Vec<i64> = (0..passages.len() as i64).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::Int64Array::from(ids)),
                Arc::new(StringArray::from(passages)),
            ],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("docs", Arc::new(table)).unwrap();
        ctx
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sql_orders_by_score_and_maps_null_passages_to_null() {
        let dir = tiny_model(RerankArchitecture::XlmRoberta);
        let ctx = ctx_with_passages(vec![
            Some("retry backoff"),
            None,
            Some("pricing plan"),
            Some("exponential backoff waits"),
        ])
        .await;
        let sql = format!(
            "SELECT id, rerank('{}', 'how does retry work', content) AS s FROM docs ORDER BY id",
            dir.path().display()
        );
        let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let scores = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(scores.len(), 4);
        assert!(scores.is_null(1), "NULL passage must score NULL");
        assert!(!scores.is_null(0) && !scores.is_null(2) && !scores.is_null(3));

        // ORDER BY the score really orders by it.
        let sql = format!(
            "SELECT rerank('{}', 'how does retry work', content) AS s FROM docs \
             WHERE content IS NOT NULL ORDER BY s DESC",
            dir.path().display()
        );
        let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let ordered: Vec<f32> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ordered.len(), 3);
        assert!(ordered.windows(2).all(|w| w[0] >= w[1]), "{ordered:?}");
    }

    #[tokio::test]
    async fn missing_model_dir_fails_with_its_path() {
        let ctx = ctx_with_passages(vec![Some("retry")]).await;
        let err = ctx
            .sql("SELECT rerank('/nonexistent/reranker', 'q', content) FROM docs")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("/nonexistent/reranker"), "{err}");
    }

    #[tokio::test]
    async fn non_literal_model_dir_is_rejected() {
        let ctx = ctx_with_passages(vec![Some("retry")]).await;
        let err = ctx
            .sql("SELECT rerank(content, 'q', content) FROM docs")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("model_dir"), "{err}");
    }
}
