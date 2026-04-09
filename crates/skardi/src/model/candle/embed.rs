use anyhow::{Result, anyhow};
use candle_core::{DType, Device, Module, Tensor, safetensors as candle_safetensors};
use candle_nn::VarBuilder;
use candle_transformers::models::{bert, distilbert, jina_bert};
use std::path::Path;
use tokenizers::Tokenizer;

// =============================================================================
// EmbeddingBackend — normalises the different forward signatures
// =============================================================================

/// Internal trait that abstracts over candle model types for embedding.
///
/// Each concrete backend handles its own `token_type_ids` / attention-mask
/// needs internally, so the pooling logic above can stay model-agnostic.
trait EmbeddingBackend: Send + Sync {
    /// Forward pass: returns the last hidden state `[1, seq_len, hidden_size]`.
    fn embed(&self, token_ids: &Tensor, attention_mask: &Tensor) -> candle_core::Result<Tensor>;
}

// --- BERT / RoBERTa -----------------------------------------------------------

struct BertBackend(bert::BertModel);

impl EmbeddingBackend for BertBackend {
    fn embed(&self, token_ids: &Tensor, attention_mask: &Tensor) -> candle_core::Result<Tensor> {
        // BERT needs token_type_ids; all-zeros is correct for single-segment input.
        let token_type_ids = token_ids.zeros_like()?;
        self.0
            .forward(token_ids, &token_type_ids, Some(attention_mask))
    }
}

// --- DistilBERT ---------------------------------------------------------------

struct DistilBertBackend(distilbert::DistilBertModel);

impl EmbeddingBackend for DistilBertBackend {
    fn embed(&self, token_ids: &Tensor, attention_mask: &Tensor) -> candle_core::Result<Tensor> {
        // DistilBERT has no token_type_ids at all.
        self.0.forward(token_ids, attention_mask)
    }
}

// --- Jina BERT ----------------------------------------------------------------

struct JinaBertBackend(jina_bert::BertModel);

impl EmbeddingBackend for JinaBertBackend {
    fn embed(&self, token_ids: &Tensor, _attention_mask: &Tensor) -> candle_core::Result<Tensor> {
        // jina_bert implements the candle `Module` trait: forward takes only token_ids.
        self.0.forward(token_ids)
    }
}

// =============================================================================
// Architecture detection
// =============================================================================

/// The subset of `config.json` we need before picking a backend.
#[derive(serde::Deserialize)]
struct RawConfig {
    #[serde(default)]
    architectures: Vec<String>,
}

/// Which candle model to load, derived from `config.json`'s `architectures`.
#[derive(Debug, Clone, Copy)]
enum ModelArchitecture {
    /// Standard BERT encoder — covers BertModel, RobertaModel and most
    /// HuggingFace embedding fine-tunes (bge-*, all-MiniLM-*, e5-*, etc.).
    Bert,
    /// DistilBERT — lighter, no token_type_ids.
    DistilBert,
    /// Jina BERT — custom RoPE attention, only takes token_ids.
    JinaBert,
}

impl ModelArchitecture {
    fn detect(architectures: &[String]) -> Self {
        let arch = architectures.first().map(|s| s.as_str()).unwrap_or("");
        match arch {
            "DistilBertModel" | "DistilBertForMaskedLM" => Self::DistilBert,
            "JinaBertModel" => Self::JinaBert,
            // BertModel, RobertaModel, XLMRobertaModel, and unknown — all
            // share the BERT encoder layout and are handled by bert::BertModel.
            _ => {
                if !arch.is_empty()
                    && !matches!(arch, "BertModel" | "RobertaModel" | "XLMRobertaModel")
                {
                    tracing::warn!("Unknown architecture '{}'; falling back to BertModel", arch);
                }
                Self::Bert
            }
        }
    }
}

// =============================================================================
// Pooling strategy
// =============================================================================

/// How to reduce `[1, seq_len, hidden_size]` hidden states into a single vector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PoolingStrategy {
    /// Use the CLS token (first token) output. Common for BGE, GTE models.
    Cls,
    /// Average all token outputs (weighted by attention mask). Most common default.
    Mean,
    /// Use the last token output. Used by some causal-LM-based embeddings.
    LastToken,
}

/// Mirrors the `1_Pooling/config.json` file from sentence-transformers.
#[derive(serde::Deserialize, Default)]
struct PoolingConfig {
    #[serde(default)]
    pooling_mode_cls_token: bool,
    #[serde(default)]
    pooling_mode_mean_tokens: bool,
    #[serde(default)]
    pooling_mode_lasttoken: bool,
}

impl PoolingStrategy {
    /// Detect pooling strategy from the model directory.
    ///
    /// Checks (in order):
    /// 1. `1_Pooling/config.json` (sentence-transformers standard)
    /// 2. Falls back to `Mean` if the file is missing or unparseable.
    fn detect(model_dir: &Path) -> Self {
        let pooling_config_path = model_dir.join("1_Pooling/config.json");
        if let Ok(content) = std::fs::read_to_string(&pooling_config_path) {
            if let Ok(config) = serde_json::from_str::<PoolingConfig>(&content) {
                if config.pooling_mode_cls_token {
                    return Self::Cls;
                }
                if config.pooling_mode_lasttoken {
                    return Self::LastToken;
                }
                if config.pooling_mode_mean_tokens {
                    return Self::Mean;
                }
            }
        }
        // Default to mean pooling — the most common strategy.
        Self::Mean
    }
}

// =============================================================================
// EmbeddingModel — public API
// =============================================================================

/// A loaded embedding model with its tokenizer.
///
/// Loads whichever candle encoder architecture is declared in `config.json`'s
/// `architectures` field. Currently supports:
///
/// | `architectures` value | Candle backend |
/// |---|---|
/// | `BertModel`, `RobertaModel`, `XLMRobertaModel`, *(unknown)* | `bert::BertModel` |
/// | `DistilBertModel` | `distilbert::DistilBertModel` |
/// | `JinaBertModel` | `jina_bert::BertModel` |
///
/// The model directory must contain exactly one `.safetensors` weights file,
/// plus `config.json` and `tokenizer.json`.
pub struct EmbeddingModel {
    backend: Box<dyn EmbeddingBackend>,
    tokenizer: Tokenizer,
    device: Device,
    pooling: PoolingStrategy,
}

impl EmbeddingModel {
    /// Load an embedding model from a directory.
    ///
    /// The directory must contain a single `.safetensors` weights file,
    /// `config.json`, and `tokenizer.json`.
    pub fn from_dir(model_dir: &str) -> Result<Self> {
        let dir = Path::new(model_dir);

        // Prefer `model.safetensors` (single-file models). For sharded models
        // (e.g. `model-00001-of-00003.safetensors`), fall back to the first
        // shard alphabetically. This avoids non-deterministic `read_dir` order.
        let canonical = dir.join("model.safetensors");
        let weights_path = if canonical.exists() {
            canonical
        } else {
            let mut safetensor_files: Vec<_> = std::fs::read_dir(dir)
                .map_err(|e| anyhow!("Cannot read model directory '{}': {}", model_dir, e))?
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("safetensors"))
                .collect();
            safetensor_files.sort();
            safetensor_files
                .into_iter()
                .next()
                .ok_or_else(|| anyhow!("No .safetensors file found in '{}'", model_dir))?
        };

        let config_path = dir.join("config.json");
        let tokenizer_path = dir.join("tokenizer.json");

        let config_str = std::fs::read_to_string(&config_path).map_err(|e| {
            anyhow!(
                "Failed to read config.json at '{}': {}",
                config_path.display(),
                e
            )
        })?;

        let raw: RawConfig = serde_json::from_str(&config_str)
            .map_err(|e| anyhow!("Failed to parse config.json: {}", e))?;
        let arch = ModelArchitecture::detect(&raw.architectures);

        let tokenizer = Tokenizer::from_file(&tokenizer_path).map_err(|e| {
            anyhow!(
                "Failed to load tokenizer.json at '{}': {}",
                tokenizer_path.display(),
                e
            )
        })?;

        let device = Device::Cpu;

        let tensors = candle_safetensors::load(&weights_path, &device)
            .map_err(|e| anyhow!("Failed to load safetensors weights: {}", e))?;
        let vb = VarBuilder::from_tensors(tensors, DType::F32, &device);

        let backend: Box<dyn EmbeddingBackend> = match arch {
            ModelArchitecture::Bert => {
                let config: bert::Config = serde_json::from_str(&config_str)
                    .map_err(|e| anyhow!("Failed to parse BERT config: {}", e))?;
                Box::new(BertBackend(
                    bert::BertModel::load(vb, &config)
                        .map_err(|e| anyhow!("Failed to build BertModel: {}", e))?,
                ))
            }
            ModelArchitecture::DistilBert => {
                let config: distilbert::Config = serde_json::from_str(&config_str)
                    .map_err(|e| anyhow!("Failed to parse DistilBERT config: {}", e))?;
                Box::new(DistilBertBackend(
                    distilbert::DistilBertModel::load(vb, &config)
                        .map_err(|e| anyhow!("Failed to build DistilBertModel: {}", e))?,
                ))
            }
            ModelArchitecture::JinaBert => {
                let config: jina_bert::Config = serde_json::from_str(&config_str)
                    .map_err(|e| anyhow!("Failed to parse Jina BERT config: {}", e))?;
                Box::new(JinaBertBackend(
                    jina_bert::BertModel::new(vb, &config)
                        .map_err(|e| anyhow!("Failed to build Jina BertModel: {}", e))?,
                ))
            }
        };

        let pooling = PoolingStrategy::detect(dir);
        tracing::info!(
            "Loaded {:?} embedding model from '{}' (pooling: {:?})",
            arch,
            model_dir,
            pooling
        );

        Ok(Self {
            backend,
            tokenizer,
            device,
            pooling,
        })
    }

    /// Embed a batch of texts.
    ///
    /// `normalize` controls whether each vector is L2-normalised before being
    /// returned:
    ///
    /// - `true`  — unit-norm vectors; use this when the downstream metric is
    ///   cosine similarity (e.g. `lance_knn` with cosine distance). Models
    ///   trained with cosine loss (bge-\*, e5-\*, nomic-embed-text, …) expect
    ///   this. Models that already output unit-norm vectors are unaffected.
    /// - `false` — raw mean-pooled vectors; use this for dot-product search or
    ///   when the vector magnitude carries information (e.g. reranker scores).
    pub fn embed_texts(&self, texts: &[&str], normalize: bool) -> Result<Vec<Vec<f32>>> {
        texts
            .iter()
            .map(|t| self.embed_single(t, normalize))
            .collect()
    }

    fn embed_single(&self, text: &str, normalize: bool) -> Result<Vec<f32>> {
        let encoding = self
            .tokenizer
            .encode(text, true)
            .map_err(|e| anyhow!("Tokenization failed: {}", e))?;

        let ids: Vec<u32> = encoding.get_ids().to_vec();
        let mask: Vec<u32> = encoding.get_attention_mask().to_vec();

        let token_ids = Tensor::new(ids.as_slice(), &self.device)
            .map_err(|e| anyhow!("Failed to create token_ids tensor: {}", e))?
            .unsqueeze(0)
            .map_err(|e| anyhow!("{}", e))?;
        let attention_mask = Tensor::new(mask.as_slice(), &self.device)
            .map_err(|e| anyhow!("Failed to create attention_mask tensor: {}", e))?
            .unsqueeze(0)
            .map_err(|e| anyhow!("{}", e))?;

        // Forward: [1, seq_len, hidden_size]
        let hidden = self
            .backend
            .embed(&token_ids, &attention_mask)
            .map_err(|e| anyhow!("Model forward pass failed: {}", e))?;

        // Pool hidden states → [1, hidden_size]
        let pooled = match self.pooling {
            PoolingStrategy::Cls => cls_pool(&hidden)?,
            PoolingStrategy::Mean => mean_pool(&hidden, &attention_mask)?,
            PoolingStrategy::LastToken => last_token_pool(&hidden)?,
        };

        let out = if normalize {
            l2_normalize(&pooled)?
        } else {
            pooled
        };

        out.squeeze(0)
            .map_err(|e| anyhow!("{}", e))?
            .to_vec1::<f32>()
            .map_err(|e| anyhow!("Failed to extract embedding vector: {}", e))
    }
}

/// Take the CLS token (first token) output as the embedding.
///
/// Input shape:  `[batch=1, seq_len, hidden_size]`
/// Output shape: `[1, hidden_size]`
fn cls_pool(hidden: &Tensor) -> Result<Tensor> {
    hidden
        .narrow(1, 0, 1)
        .map_err(|e| anyhow!("CLS pooling failed: {}", e))?
        .squeeze(1)
        .map_err(|e| anyhow!("CLS pooling squeeze failed: {}", e))
}

/// Mean-pool the last hidden state, weighted by the attention mask.
///
/// Formula: `sum(hidden * mask) / sum(mask)` — padded tokens (mask=0) are excluded.
///
/// Input shapes:
///   `hidden`:         `[batch=1, seq_len, hidden_size]`
///   `attention_mask`:  `[batch=1, seq_len]`
/// Output shape: `[1, hidden_size]`
fn mean_pool(hidden: &Tensor, attention_mask: &Tensor) -> Result<Tensor> {
    // Expand mask from [1, seq_len] → [1, seq_len, 1] for broadcasting.
    let mask = attention_mask
        .unsqueeze(2)
        .map_err(|e| anyhow!("{}", e))?
        .to_dtype(hidden.dtype())
        .map_err(|e| anyhow!("{}", e))?;

    // sum(hidden * mask, dim=1) → [1, hidden_size]
    let masked = hidden.broadcast_mul(&mask).map_err(|e| anyhow!("{}", e))?;
    let sum = masked.sum(1).map_err(|e| anyhow!("{}", e))?;

    // sum(mask, dim=1) → [1, hidden_size] (broadcast), clamped to avoid div-by-zero.
    let count = mask
        .sum(1)
        .map_err(|e| anyhow!("{}", e))?
        .clamp(1e-9, f64::MAX)
        .map_err(|e| anyhow!("{}", e))?;

    sum.broadcast_div(&count)
        .map_err(|e| anyhow!("Mean pooling failed: {}", e))
}

/// Take the last token output as the embedding.
///
/// Input shape:  `[batch=1, seq_len, hidden_size]`
/// Output shape: `[1, hidden_size]`
fn last_token_pool(hidden: &Tensor) -> Result<Tensor> {
    let (_, seq_len, _) = hidden
        .dims3()
        .map_err(|e| anyhow!("Unexpected hidden-state shape: {}", e))?;
    if seq_len == 0 {
        return Err(anyhow!("Last-token pooling requires at least one token"));
    }
    hidden
        .narrow(1, seq_len - 1, 1)
        .map_err(|e| anyhow!("Last-token pooling failed: {}", e))?
        .squeeze(1)
        .map_err(|e| anyhow!("Last-token pooling squeeze failed: {}", e))
}

/// L2-normalise along the last dimension (in-place semantics, returns new tensor).
///
/// Input/output shape: `[1, hidden_size]`
fn l2_normalize(t: &Tensor) -> Result<Tensor> {
    let norm = t
        .sqr()
        .map_err(|e| anyhow!("{}", e))?
        .sum_keepdim(1)
        .map_err(|e| anyhow!("{}", e))?
        .sqrt()
        .map_err(|e| anyhow!("{}", e))?
        .clamp(1e-12, f64::MAX)
        .map_err(|e| anyhow!("{}", e))?;
    t.broadcast_div(&norm)
        .map_err(|e| anyhow!("L2 normalisation failed: {}", e))
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::Path;

    // -------------------------------------------------------------------------
    // Architecture detection (pure unit tests — no IO, no candle)
    // -------------------------------------------------------------------------

    fn arch(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn detect_bert_variants() {
        assert!(matches!(
            ModelArchitecture::detect(&arch(&["BertModel"])),
            ModelArchitecture::Bert
        ));
        assert!(matches!(
            ModelArchitecture::detect(&arch(&["RobertaModel"])),
            ModelArchitecture::Bert
        ));
        assert!(matches!(
            ModelArchitecture::detect(&arch(&["XLMRobertaModel"])),
            ModelArchitecture::Bert
        ));
    }

    #[test]
    fn detect_distilbert() {
        assert!(matches!(
            ModelArchitecture::detect(&arch(&["DistilBertModel"])),
            ModelArchitecture::DistilBert
        ));
        assert!(matches!(
            ModelArchitecture::detect(&arch(&["DistilBertForMaskedLM"])),
            ModelArchitecture::DistilBert
        ));
    }

    #[test]
    fn detect_jina() {
        assert!(matches!(
            ModelArchitecture::detect(&arch(&["JinaBertModel"])),
            ModelArchitecture::JinaBert
        ));
    }

    #[test]
    fn detect_empty_falls_back_to_bert() {
        assert!(matches!(
            ModelArchitecture::detect(&[]),
            ModelArchitecture::Bert
        ));
    }

    #[test]
    fn detect_unknown_falls_back_to_bert() {
        assert!(matches!(
            ModelArchitecture::detect(&arch(&["SomeFutureModel"])),
            ModelArchitecture::Bert
        ));
    }

    // -------------------------------------------------------------------------
    // Pooling strategy detection
    // -------------------------------------------------------------------------

    #[test]
    fn pooling_detect_cls_from_config() {
        let dir = tempfile::tempdir().unwrap();
        let pooling_dir = dir.path().join("1_Pooling");
        std::fs::create_dir_all(&pooling_dir).unwrap();
        std::fs::write(
            pooling_dir.join("config.json"),
            r#"{"pooling_mode_cls_token": true, "pooling_mode_mean_tokens": false}"#,
        )
        .unwrap();
        assert_eq!(PoolingStrategy::detect(dir.path()), PoolingStrategy::Cls);
    }

    #[test]
    fn pooling_detect_mean_from_config() {
        let dir = tempfile::tempdir().unwrap();
        let pooling_dir = dir.path().join("1_Pooling");
        std::fs::create_dir_all(&pooling_dir).unwrap();
        std::fs::write(
            pooling_dir.join("config.json"),
            r#"{"pooling_mode_cls_token": false, "pooling_mode_mean_tokens": true}"#,
        )
        .unwrap();
        assert_eq!(PoolingStrategy::detect(dir.path()), PoolingStrategy::Mean);
    }

    #[test]
    fn pooling_detect_lasttoken_from_config() {
        let dir = tempfile::tempdir().unwrap();
        let pooling_dir = dir.path().join("1_Pooling");
        std::fs::create_dir_all(&pooling_dir).unwrap();
        std::fs::write(
            pooling_dir.join("config.json"),
            r#"{"pooling_mode_lasttoken": true, "pooling_mode_mean_tokens": false}"#,
        )
        .unwrap();
        assert_eq!(
            PoolingStrategy::detect(dir.path()),
            PoolingStrategy::LastToken
        );
    }

    #[test]
    fn pooling_defaults_to_mean_when_missing() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(PoolingStrategy::detect(dir.path()), PoolingStrategy::Mean);
    }

    // -------------------------------------------------------------------------
    // Synthetic model fixture
    //
    // Creates a minimal but fully valid BERT model under `dir`:
    //   config.json   — tiny BERT config (hidden=32, layers=2, heads=2)
    //   tokenizer.json — WordPiece tokenizer with a-z + common test words
    //   model.safetensors — all required weight tensors initialised to small
    //                       random values / ones (shapes match the config)
    // -------------------------------------------------------------------------

    // Tiny model dimensions.
    const H: usize = 32; // hidden_size
    const L: usize = 2; // num_hidden_layers
    const A: usize = 2; // num_attention_heads
    const I: usize = 64; // intermediate_size
    const V: usize = 128; // vocab_size (>= highest token id in tokenizer)
    const P: usize = 64; // max_position_embeddings
    const T: usize = 2; // type_vocab_size

    fn make_bert_fixture(dir: &Path) {
        write_config(dir);
        write_tokenizer(dir);
        write_weights(dir);
    }

    fn write_config(dir: &Path) {
        // Minimal bert config.json; all fields required by bert::Config.
        let config = serde_json::json!({
            "architectures": ["BertModel"],
            "hidden_size": H,
            "num_hidden_layers": L,
            "num_attention_heads": A,
            "intermediate_size": I,
            "vocab_size": V,
            "max_position_embeddings": P,
            "type_vocab_size": T,
            "hidden_act": "gelu",
            "hidden_dropout_prob": 0.0,
            "attention_probs_dropout_prob": 0.0,
            "initializer_range": 0.02,
            "layer_norm_eps": 1e-12,
            "pad_token_id": 0,
            "position_embedding_type": "absolute"
        });
        std::fs::write(dir.join("config.json"), config.to_string()).unwrap();
    }

    fn write_tokenizer(dir: &Path) {
        // Build vocabulary: special tokens + a-z + ##a-##z + common test words.
        // All token IDs must be < V (128).
        let mut vocab = serde_json::Map::new();
        vocab.insert("[PAD]".into(), 0.into());
        vocab.insert("[UNK]".into(), 1.into());
        vocab.insert("[CLS]".into(), 2.into());
        vocab.insert("[SEP]".into(), 3.into());
        vocab.insert("[MASK]".into(), 4.into());
        for (i, c) in ('a'..='z').enumerate() {
            vocab.insert(c.to_string(), (5 + i as u32).into());
        }
        // Continuing subword prefix tokens (##a-##z) for WordPiece subword splits.
        for (i, c) in ('a'..='z').enumerate() {
            vocab.insert(format!("##{c}"), (31 + i as u32).into());
        }
        // Common words used in tests so they tokenise as single tokens.
        let words = ["hello", "world", "the", "quick", "brown", "fox", "cat"];
        for (i, w) in words.iter().enumerate() {
            vocab.insert(w.to_string(), (57 + i as u32).into());
        }

        let tokenizer_json = serde_json::json!({
            "version": "1.0",
            "truncation": null,
            "padding": null,
            "added_tokens": [
                {"id": 0, "content": "[PAD]", "single_word": false, "lstrip": false, "rstrip": false, "normalized": false, "special": true},
                {"id": 1, "content": "[UNK]", "single_word": false, "lstrip": false, "rstrip": false, "normalized": false, "special": true},
                {"id": 2, "content": "[CLS]", "single_word": false, "lstrip": false, "rstrip": false, "normalized": false, "special": true},
                {"id": 3, "content": "[SEP]", "single_word": false, "lstrip": false, "rstrip": false, "normalized": false, "special": true},
                {"id": 4, "content": "[MASK]", "single_word": false, "lstrip": false, "rstrip": false, "normalized": false, "special": true}
            ],
            "normalizer": {
                "type": "BertNormalizer",
                "clean_text": true,
                "handle_chinese_chars": true,
                "strip_accents": null,
                "lowercase": true
            },
            "pre_tokenizer": {"type": "BertPreTokenizer"},
            "post_processor": {
                "type": "BertProcessing",
                "sep": ["[SEP]", 3],
                "cls": ["[CLS]", 2]
            },
            "decoder": {"type": "WordPiece", "prefix": "##", "cleanup": true},
            "model": {
                "type": "WordPiece",
                "unk_token": "[UNK]",
                "continuing_subword_prefix": "##",
                "max_input_chars_per_word": 100,
                "vocab": vocab
            }
        });
        std::fs::write(dir.join("tokenizer.json"), tokenizer_json.to_string()).unwrap();
    }

    fn write_weights(dir: &Path) {
        use candle_core::safetensors as st;

        let dev = Device::Cpu;
        let mut tensors: HashMap<String, Tensor> = HashMap::new();

        // Helper closures for common initialisation patterns.
        let zeros = |shape: &[usize]| Tensor::zeros(shape, DType::F32, &dev).unwrap();
        let ones = |shape: &[usize]| Tensor::ones(shape, DType::F32, &dev).unwrap();
        // Small random values prevent NaN from division by near-zero norm.
        let rand = |shape: &[usize]| Tensor::randn(0f32, 0.02f32, shape, &dev).unwrap();

        // Embeddings
        tensors.insert("embeddings.word_embeddings.weight".into(), rand(&[V, H]));
        tensors.insert(
            "embeddings.position_embeddings.weight".into(),
            rand(&[P, H]),
        );
        tensors.insert(
            "embeddings.token_type_embeddings.weight".into(),
            rand(&[T, H]),
        );
        tensors.insert("embeddings.LayerNorm.weight".into(), ones(&[H]));
        tensors.insert("embeddings.LayerNorm.bias".into(), zeros(&[H]));

        // Encoder layers
        for i in 0..L {
            let pfx = format!("encoder.layer.{i}");

            // Self-attention: query / key / value — weight [H, H], bias [H]
            for name in ["query", "key", "value"] {
                tensors.insert(format!("{pfx}.attention.self.{name}.weight"), rand(&[H, H]));
                tensors.insert(format!("{pfx}.attention.self.{name}.bias"), zeros(&[H]));
            }

            // Attention output projection + LayerNorm
            tensors.insert(
                format!("{pfx}.attention.output.dense.weight"),
                rand(&[H, H]),
            );
            tensors.insert(format!("{pfx}.attention.output.dense.bias"), zeros(&[H]));
            tensors.insert(
                format!("{pfx}.attention.output.LayerNorm.weight"),
                ones(&[H]),
            );
            tensors.insert(
                format!("{pfx}.attention.output.LayerNorm.bias"),
                zeros(&[H]),
            );

            // Intermediate (FFN up-projection): weight [I, H], bias [I]
            tensors.insert(format!("{pfx}.intermediate.dense.weight"), rand(&[I, H]));
            tensors.insert(format!("{pfx}.intermediate.dense.bias"), zeros(&[I]));

            // Output (FFN down-projection) + LayerNorm: weight [H, I], bias [H]
            tensors.insert(format!("{pfx}.output.dense.weight"), rand(&[H, I]));
            tensors.insert(format!("{pfx}.output.dense.bias"), zeros(&[H]));
            tensors.insert(format!("{pfx}.output.LayerNorm.weight"), ones(&[H]));
            tensors.insert(format!("{pfx}.output.LayerNorm.bias"), zeros(&[H]));
        }

        st::save(&tensors, dir.join("model.safetensors")).unwrap();
    }

    // -------------------------------------------------------------------------
    // Integration tests using the synthetic BERT fixture
    // -------------------------------------------------------------------------

    #[test]
    #[ignore = "integration: builds a synthetic model on disk"]
    fn embedding_has_unit_norm() {
        let dir = tempfile::tempdir().unwrap();
        make_bert_fixture(dir.path());
        let model = EmbeddingModel::from_dir(dir.path().to_str().unwrap())
            .expect("Failed to load synthetic model");

        let embeddings = model
            .embed_texts(&["hello world", "the quick brown fox"], true)
            .expect("Embedding failed");

        assert_eq!(embeddings.len(), 2);
        for (i, emb) in embeddings.iter().enumerate() {
            assert!(!emb.is_empty(), "embedding {i} is empty");
            let norm: f32 = emb.iter().map(|v| v * v).sum::<f32>().sqrt();
            assert!(
                (norm - 1.0).abs() < 1e-4,
                "embedding {i} norm = {norm:.6}, expected ~1.0"
            );
        }
    }

    #[test]
    #[ignore = "integration: builds a synthetic model on disk"]
    fn normalize_false_returns_unnormalised_vector() {
        let dir = tempfile::tempdir().unwrap();
        make_bert_fixture(dir.path());
        let model = EmbeddingModel::from_dir(dir.path().to_str().unwrap()).unwrap();

        let raw = model
            .embed_texts(&["hello world"], false)
            .unwrap()
            .remove(0);
        let normed = model.embed_texts(&["hello world"], true).unwrap().remove(0);

        // Raw vector should NOT be unit-norm (with random weights it virtually
        // never is), whereas the normalised one should be.
        let raw_norm: f32 = raw.iter().map(|v| v * v).sum::<f32>().sqrt();
        let normed_norm: f32 = normed.iter().map(|v| v * v).sum::<f32>().sqrt();
        assert!(
            (raw_norm - 1.0).abs() > 1e-3 || raw_norm == 0.0,
            "raw vector unexpectedly has unit norm ({raw_norm:.6})"
        );
        assert!(
            (normed_norm - 1.0).abs() < 1e-4,
            "normalised vector has wrong norm ({normed_norm:.6})"
        );
    }

    #[test]
    #[ignore = "integration: builds a synthetic model on disk"]
    fn same_text_produces_same_embedding() {
        let dir = tempfile::tempdir().unwrap();
        make_bert_fixture(dir.path());
        let model = EmbeddingModel::from_dir(dir.path().to_str().unwrap()).unwrap();

        let a = model
            .embed_texts(&["determinism check"], true)
            .unwrap()
            .remove(0);
        let b = model
            .embed_texts(&["determinism check"], true)
            .unwrap()
            .remove(0);

        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(b.iter()) {
            assert_eq!(x, y, "embedding is not deterministic");
        }
    }

    #[test]
    #[ignore = "integration: builds a synthetic model on disk"]
    fn different_texts_produce_different_embeddings() {
        let dir = tempfile::tempdir().unwrap();
        make_bert_fixture(dir.path());
        let model = EmbeddingModel::from_dir(dir.path().to_str().unwrap()).unwrap();

        let mut out = model
            .embed_texts(&["cat", "the quick brown fox"], true)
            .unwrap();
        let b = out.remove(1);
        let a = out.remove(0);

        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        assert!(
            dot < 0.9999,
            "cosine similarity = {dot:.6}; 'cat' and 'the quick brown fox' appear identical"
        );
    }
}
