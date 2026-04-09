use anyhow::{Result, anyhow};
use llama_cpp_4::context::params::LlamaContextParams;
use llama_cpp_4::llama_backend::LlamaBackend;
use llama_cpp_4::llama_batch::LlamaBatch;
use llama_cpp_4::model::params::LlamaModelParams;
use llama_cpp_4::model::{AddBos, LlamaModel};
use std::path::Path;
use std::sync::OnceLock;

// =============================================================================
// Global llama.cpp backend — must be initialised exactly once per process
// =============================================================================

/// llama.cpp requires a single `LlamaBackend::init()` call per process.
/// Subsequent calls return `BackendAlreadyInitialized`. We use `OnceLock` to
/// guarantee one-time init and share the reference across all model loads.
///
/// The `LlamaBackend` is a proof-of-init token — `load_from_file` and
/// `new_context` take `&LlamaBackend` as a witness that init has occurred.
static LLAMA_BACKEND: OnceLock<LlamaBackend> = OnceLock::new();

fn get_backend() -> &'static LlamaBackend {
    LLAMA_BACKEND.get_or_init(|| {
        let mut backend = LlamaBackend::init()
            .expect("Failed to initialize llama.cpp backend (check CUDA/hardware support)");
        // Suppress llama.cpp's verbose C-level stderr output (tensor listings,
        // context params, etc.) unless debug-level tracing is enabled.
        if !tracing::enabled!(tracing::Level::DEBUG) {
            backend.void_logs();
        }
        backend
    })
}

// =============================================================================
// GgufEmbeddingModel — loads a GGUF model via llama-cpp-4
// =============================================================================

/// A loaded GGUF embedding model backed by llama.cpp.
///
/// Tokenization, architecture detection, and tensor mapping are all handled
/// internally by llama.cpp — no manual config or remapping needed.
pub struct GgufEmbeddingModel {
    model: LlamaModel,
    /// Embedding dimensionality reported by the model.
    pub n_embd: usize,
}

// SAFETY: `LlamaModel` holds a raw pointer to a llama.cpp model.
// - `Send`: the model is only accessed via `&self` after construction, and
//   llama.cpp model reads (tokenization via `str_to_token`, `n_embd`) are
//   thread-safe.
// - `Sync`: concurrent `embed_texts` calls are safe because each creates its
//   own `LlamaContext` — no mutable state is shared through `&self.model`.
unsafe impl Send for GgufEmbeddingModel {}
unsafe impl Sync for GgufEmbeddingModel {}

impl GgufEmbeddingModel {
    /// Load a GGUF model from a directory.
    ///
    /// The directory must contain exactly one `.gguf` file. The model is loaded
    /// with all layers offloaded to GPU (when available).
    pub fn from_dir(model_dir: &str) -> Result<Self> {
        let dir = Path::new(model_dir);
        if !dir.is_dir() {
            return Err(anyhow!(
                "GGUF model path '{}' is not a directory",
                model_dir
            ));
        }

        // Find the single .gguf file in the directory
        let gguf_path = find_gguf_file(dir)?;

        let backend = get_backend();

        let model_params = LlamaModelParams::default().with_n_gpu_layers(1000);
        let model = LlamaModel::load_from_file(backend, &gguf_path, &model_params)
            .map_err(|e| anyhow!("Failed to load GGUF model '{}': {}", gguf_path.display(), e))?;

        let n_embd = model.n_embd() as usize;
        if n_embd == 0 {
            return Err(anyhow!("Model reports 0 embedding dimensions"));
        }

        tracing::info!(
            "Loaded GGUF model from '{}' (embedding dim={})",
            gguf_path.display(),
            n_embd
        );

        Ok(Self { model, n_embd })
    }

    /// Embed a batch of texts, returning one `Vec<f32>` per input.
    ///
    /// Each text is decoded independently (its own batch + decode call) because
    /// BERT models in llama.cpp do not support multi-sequence batches. A shared
    /// `LlamaContext` is reused across texts with KV-cache cleared between calls.
    ///
    /// A new `LlamaContext` is allocated per call, sized to the longest sequence
    /// in the batch. This is cheap for embedding models (small KV cache) and
    /// avoids the complexity of pooling lifetime-bound contexts.
    pub fn embed_texts(&self, texts: &[&str], normalize: bool) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(vec![]);
        }

        // Find the longest tokenised sequence to size the context.
        let tokenised: Vec<Vec<_>> = texts
            .iter()
            .map(|t| {
                self.model
                    .str_to_token(t, AddBos::Always)
                    .map_err(|e| anyhow!("Tokenisation failed: {}", e))
            })
            .collect::<Result<Vec<_>>>()?;

        let max_seq_len = tokenised.iter().map(|t| t.len()).max().unwrap_or(0) as u32;
        if max_seq_len == 0 {
            return Err(anyhow!("All input texts tokenised to empty sequences"));
        }
        let ctx_params = LlamaContextParams::default()
            .with_n_ctx(std::num::NonZeroU32::new(max_seq_len))
            .with_n_batch(max_seq_len)
            .with_n_ubatch(max_seq_len)
            .with_embeddings(true);

        let backend = get_backend();
        let mut ctx = self
            .model
            .new_context(backend, ctx_params)
            .map_err(|e| anyhow!("Failed to create llama context: {}", e))?;

        let mut results = Vec::with_capacity(texts.len());

        for (i, tokens) in tokenised.iter().enumerate() {
            let mut batch = LlamaBatch::new(tokens.len(), 1);
            batch
                .add_sequence(tokens, 0, false)
                .map_err(|e| anyhow!("Failed to add sequence to batch: {}", e))?;

            ctx.clear_kv_cache();
            ctx.decode(&mut batch)
                .map_err(|e| anyhow!("llama decode failed for text {}: {}", i, e))?;

            let raw = ctx
                .embeddings_seq_ith(0)
                .map_err(|e| anyhow!("Failed to extract embedding for text {}: {}", i, e))?;

            let emb = if normalize {
                l2_normalize(raw)
            } else {
                raw.to_vec()
            };
            results.push(emb);
        }

        Ok(results)
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// Find exactly one `.gguf` file in the given directory.
fn find_gguf_file(dir: &Path) -> Result<std::path::PathBuf> {
    let mut gguf_files: Vec<_> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "gguf"))
        .collect();

    match gguf_files.len() {
        0 => Err(anyhow!(
            "No .gguf file found in directory '{}'",
            dir.display()
        )),
        1 => Ok(gguf_files.remove(0)),
        n => Err(anyhow!(
            "Expected exactly one .gguf file in '{}', found {}",
            dir.display(),
            n
        )),
    }
}

/// L2-normalise a slice of floats.
fn l2_normalize(v: &[f32]) -> Vec<f32> {
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm < f32::EPSILON {
        return v.to_vec();
    }
    v.iter().map(|x| x / norm).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Unit tests — helpers (no model files needed)
    // =========================================================================

    #[test]
    fn l2_normalize_unit_vector() {
        let v = vec![1.0, 0.0, 0.0];
        let n = l2_normalize(&v);
        assert!((n[0] - 1.0).abs() < 1e-6);
        assert!((n[1]).abs() < 1e-6);
        assert!((n[2]).abs() < 1e-6);
    }

    #[test]
    fn l2_normalize_non_unit() {
        let v = vec![3.0, 4.0];
        let n = l2_normalize(&v);
        let mag: f32 = n.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((mag - 1.0).abs() < 1e-5);
    }

    #[test]
    fn l2_normalize_zero_vector() {
        let v = vec![0.0, 0.0, 0.0];
        let n = l2_normalize(&v);
        assert_eq!(n, v);
    }

    #[test]
    fn find_gguf_file_errors_on_missing_dir() {
        let result = find_gguf_file(Path::new("/nonexistent/path"));
        assert!(result.is_err());
    }

    #[test]
    fn find_gguf_file_errors_on_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let result = find_gguf_file(dir.path());
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("No .gguf file"),
            "should report missing .gguf file"
        );
    }

    #[test]
    fn find_gguf_file_errors_on_multiple_gguf_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.gguf"), b"fake").unwrap();
        std::fs::write(dir.path().join("b.gguf"), b"fake").unwrap();
        let result = find_gguf_file(dir.path());
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("found 2"),
            "should report multiple .gguf files"
        );
    }

    #[test]
    fn find_gguf_file_finds_single_file() {
        let dir = tempfile::tempdir().unwrap();
        let gguf = dir.path().join("model.gguf");
        std::fs::write(&gguf, b"fake").unwrap();
        // Also put a non-gguf file to ensure filtering works.
        std::fs::write(dir.path().join("config.json"), b"{}").unwrap();

        let found = find_gguf_file(dir.path()).unwrap();
        assert_eq!(found, gguf);
    }

    #[test]
    fn from_dir_errors_on_non_directory() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("not_a_dir.txt");
        std::fs::write(&file, b"hello").unwrap();

        let result = GgufEmbeddingModel::from_dir(file.to_str().unwrap());
        let err = result.err().expect("should error on non-directory path");
        assert!(
            err.to_string().contains("not a directory"),
            "should report path is not a directory, got: {}",
            err
        );
    }

    // =========================================================================
    // Integration tests — build a synthetic BERT GGUF model on disk
    //
    // This mirrors the candle integration tests from PR #58: a tiny BERT model
    // with random weights is constructed using gguf-rs-lib, written to a temp
    // directory, then loaded via llama-cpp-4 for embedding inference.
    //
    // Run with: cargo test -p skardi --features gguf -- --ignored gguf
    // =========================================================================

    // Tiny model dimensions — just large enough for llama.cpp to accept.
    const H: usize = 64; // embedding_length (hidden_size)
    const L: usize = 1; // block_count (num_hidden_layers)
    const A: usize = 2; // attention head count
    const I: usize = 128; // feed_forward_length (intermediate_size)
    const V: usize = 128; // vocab_size
    const P: usize = 64; // max_position_embeddings (context_length)

    fn make_gguf_bert_fixture(dir: &Path) {
        use gguf_rs_lib::builder::GGUFBuilder;
        use gguf_rs_lib::format::metadata::{MetadataArray, MetadataValue};
        use gguf_rs_lib::format::types::GGUFValueType;

        // Build vocabulary: [PAD], [UNK], [CLS], [SEP], [MASK], then fill
        // remaining slots with letter tokens (a, b, c, ...) and common words.
        let mut tokens: Vec<MetadataValue> = Vec::with_capacity(V);
        let special = ["[PAD]", "[UNK]", "[CLS]", "[SEP]", "[MASK]"];
        for s in &special {
            tokens.push(MetadataValue::String(s.to_string()));
        }
        let words = [
            "hello",
            "world",
            "the",
            "quick",
            "brown",
            "fox",
            "cat",
            "dog",
            "determinism",
            "check",
            "a",
            "b",
            "c",
            "d",
            "e",
            "f",
            "g",
            "h",
            "i",
            "j",
            "k",
            "l",
            "m",
            "n",
            "o",
            "p",
            "q",
            "r",
            "s",
            "t",
            "u",
            "v",
            "w",
            "x",
            "y",
            "z",
        ];
        for w in &words {
            tokens.push(MetadataValue::String(w.to_string()));
        }
        // Fill remaining vocab slots with placeholder tokens.
        while tokens.len() < V {
            tokens.push(MetadataValue::String(format!("[unused{}]", tokens.len())));
        }

        // Token scores (all zero — not used for BERT-style models).
        let scores: Vec<MetadataValue> = vec![MetadataValue::F32(0.0); V];

        // Token types: 1 = normal, 3 = control (special tokens).
        let mut token_types: Vec<MetadataValue> = Vec::with_capacity(V);
        for i in 0..V {
            token_types.push(MetadataValue::I32(if i < special.len() { 3 } else { 1 }));
        }

        // Helper: generate random f32 data (small values to avoid NaN).
        let rand_f32 = |n: usize| -> Vec<f32> {
            // Simple deterministic pseudo-random for reproducibility.
            let mut data = Vec::with_capacity(n);
            let mut seed: u64 = 42 + n as u64;
            for _ in 0..n {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let val = ((seed >> 33) as f32) / (u32::MAX as f32) * 0.04 - 0.02;
                data.push(val);
            }
            data
        };
        let zeros_f32 = |n: usize| -> Vec<f32> { vec![0.0; n] };
        let ones_f32 = |n: usize| -> Vec<f32> { vec![1.0; n] };

        let builder = GGUFBuilder::new()
            // -- Architecture metadata --
            .add_metadata("general.architecture", MetadataValue::String("bert".into()))
            .add_metadata(
                "general.name",
                MetadataValue::String("test-bert-tiny".into()),
            )
            .add_metadata("bert.context_length", MetadataValue::U32(P as u32))
            .add_metadata("bert.embedding_length", MetadataValue::U32(H as u32))
            .add_metadata("bert.block_count", MetadataValue::U32(L as u32))
            .add_metadata("bert.attention.head_count", MetadataValue::U32(A as u32))
            .add_metadata("bert.feed_forward_length", MetadataValue::U32(I as u32))
            .add_metadata(
                "bert.attention.layer_norm_epsilon",
                MetadataValue::F32(1e-12),
            )
            .add_metadata("bert.attention.causal", MetadataValue::Bool(false))
            .add_metadata("bert.pooling_type", MetadataValue::U32(2)) // 2 = mean pooling
            // -- Tokenizer metadata --
            .add_metadata("tokenizer.ggml.model", MetadataValue::String("bert".into()))
            .add_metadata("tokenizer.ggml.token_type_count", MetadataValue::U32(2))
            .add_metadata(
                "tokenizer.ggml.tokens",
                MetadataValue::Array(Box::new(
                    MetadataArray::new(GGUFValueType::String, tokens).unwrap(),
                )),
            )
            .add_metadata(
                "tokenizer.ggml.scores",
                MetadataValue::Array(Box::new(
                    MetadataArray::new(GGUFValueType::F32, scores).unwrap(),
                )),
            )
            .add_metadata(
                "tokenizer.ggml.token_type",
                MetadataValue::Array(Box::new(
                    MetadataArray::new(GGUFValueType::I32, token_types).unwrap(),
                )),
            )
            // Special token IDs (indices into the vocab above)
            .add_metadata("tokenizer.ggml.bos_token_id", MetadataValue::U32(2)) // [CLS]
            .add_metadata("tokenizer.ggml.eos_token_id", MetadataValue::U32(3)) // [SEP]
            .add_metadata("tokenizer.ggml.unknown_token_id", MetadataValue::U32(1)) // [UNK]
            .add_metadata("tokenizer.ggml.separator_token_id", MetadataValue::U32(3)) // [SEP]
            .add_metadata("tokenizer.ggml.padding_token_id", MetadataValue::U32(0)) // [PAD]
            .add_metadata("tokenizer.ggml.mask_token_id", MetadataValue::U32(4)) // [MASK]
            // -- Embedding tensors --
            // GGUF shape convention: dimensions are listed innermost-first
            // (row-major), so [H, V] means V rows of H elements.
            .add_f32_tensor(
                "token_embd.weight",
                vec![H as u64, V as u64],
                rand_f32(V * H),
            )
            .add_f32_tensor("token_types.weight", vec![H as u64, 2], rand_f32(2 * H))
            .add_f32_tensor(
                "position_embd.weight",
                vec![H as u64, P as u64],
                rand_f32(P * H),
            )
            .add_f32_tensor("token_embd_norm.weight", vec![H as u64], ones_f32(H))
            .add_f32_tensor("token_embd_norm.bias", vec![H as u64], zeros_f32(H));

        // Add transformer block tensors for each layer.
        let mut builder = builder;
        for i in 0..L {
            let pfx = format!("blk.{i}");

            // Fused QKV: GGUF shape [H, 3*H] means 3*H rows of H elements
            builder = builder
                .add_f32_tensor(
                    format!("{pfx}.attn_qkv.weight"),
                    vec![H as u64, 3 * H as u64],
                    rand_f32(3 * H * H),
                )
                .add_f32_tensor(
                    format!("{pfx}.attn_qkv.bias"),
                    vec![3 * H as u64],
                    zeros_f32(3 * H),
                )
                // Attention output projection: [H, H]
                .add_f32_tensor(
                    format!("{pfx}.attn_output.weight"),
                    vec![H as u64, H as u64],
                    rand_f32(H * H),
                )
                .add_f32_tensor(
                    format!("{pfx}.attn_output.bias"),
                    vec![H as u64],
                    zeros_f32(H),
                )
                // Attention output layer norm
                .add_f32_tensor(
                    format!("{pfx}.attn_output_norm.weight"),
                    vec![H as u64],
                    ones_f32(H),
                )
                .add_f32_tensor(
                    format!("{pfx}.attn_output_norm.bias"),
                    vec![H as u64],
                    zeros_f32(H),
                )
                // FFN up-projection: [H, I]
                .add_f32_tensor(
                    format!("{pfx}.ffn_up.weight"),
                    vec![H as u64, I as u64],
                    rand_f32(I * H),
                )
                .add_f32_tensor(format!("{pfx}.ffn_up.bias"), vec![I as u64], zeros_f32(I))
                // FFN down-projection: [I, H]
                .add_f32_tensor(
                    format!("{pfx}.ffn_down.weight"),
                    vec![I as u64, H as u64],
                    rand_f32(H * I),
                )
                .add_f32_tensor(format!("{pfx}.ffn_down.bias"), vec![H as u64], zeros_f32(H))
                // Layer output norm
                .add_f32_tensor(
                    format!("{pfx}.layer_output_norm.weight"),
                    vec![H as u64],
                    ones_f32(H),
                )
                .add_f32_tensor(
                    format!("{pfx}.layer_output_norm.bias"),
                    vec![H as u64],
                    zeros_f32(H),
                );
        }

        builder
            .build_to_file(dir.join("model.gguf"))
            .expect("Failed to write synthetic GGUF model");
    }

    // -------------------------------------------------------------------------
    // Integration tests using the synthetic BERT GGUF fixture
    // -------------------------------------------------------------------------

    #[test]
    #[ignore = "integration: builds a synthetic GGUF model on disk"]
    fn embedding_has_unit_norm() {
        let dir = tempfile::tempdir().unwrap();
        make_gguf_bert_fixture(dir.path());
        let model = GgufEmbeddingModel::from_dir(dir.path().to_str().unwrap())
            .expect("Failed to load synthetic GGUF model");

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
    #[ignore = "integration: builds a synthetic GGUF model on disk"]
    fn normalize_false_returns_unnormalised_vector() {
        let dir = tempfile::tempdir().unwrap();
        make_gguf_bert_fixture(dir.path());
        let model = GgufEmbeddingModel::from_dir(dir.path().to_str().unwrap()).unwrap();

        let mut raw = model.embed_texts(&["hello world"], false).unwrap();
        let raw = raw.remove(0);
        let mut normed = model.embed_texts(&["hello world"], true).unwrap();
        let normed = normed.remove(0);

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
    #[ignore = "integration: builds a synthetic GGUF model on disk"]
    fn same_text_produces_same_embedding() {
        let dir = tempfile::tempdir().unwrap();
        make_gguf_bert_fixture(dir.path());
        let model = GgufEmbeddingModel::from_dir(dir.path().to_str().unwrap()).unwrap();

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
    #[ignore = "integration: builds a synthetic GGUF model on disk"]
    fn multiple_texts_each_produce_valid_embeddings() {
        let dir = tempfile::tempdir().unwrap();
        make_gguf_bert_fixture(dir.path());
        let model = GgufEmbeddingModel::from_dir(dir.path().to_str().unwrap()).unwrap();

        let out = model
            .embed_texts(&["cat", "the quick brown fox"], true)
            .unwrap();

        assert_eq!(out.len(), 2, "should return one embedding per input text");
        for (i, emb) in out.iter().enumerate() {
            assert_eq!(emb.len(), H, "embedding {i} has wrong dimension");
            assert!(
                emb.iter().all(|v| v.is_finite()),
                "embedding {i} contains NaN or Inf"
            );
            let norm: f32 = emb.iter().map(|v| v * v).sum::<f32>().sqrt();
            assert!(
                (norm - 1.0).abs() < 1e-4,
                "embedding {i} norm = {norm:.6}, expected ~1.0"
            );
        }
    }

    #[test]
    #[ignore = "integration: builds a synthetic GGUF model on disk"]
    fn empty_input_returns_empty_output() {
        let dir = tempfile::tempdir().unwrap();
        make_gguf_bert_fixture(dir.path());
        let model = GgufEmbeddingModel::from_dir(dir.path().to_str().unwrap()).unwrap();

        let embeddings = model.embed_texts(&[], true).unwrap();
        assert!(embeddings.is_empty());
    }

    #[test]
    #[ignore = "integration: builds a synthetic GGUF model on disk"]
    fn embedding_dimension_matches_model() {
        let dir = tempfile::tempdir().unwrap();
        make_gguf_bert_fixture(dir.path());
        let model = GgufEmbeddingModel::from_dir(dir.path().to_str().unwrap()).unwrap();

        let embeddings = model.embed_texts(&["test"], true).unwrap();
        assert_eq!(
            embeddings[0].len(),
            model.n_embd,
            "embedding length should match model.n_embd"
        );
        assert_eq!(model.n_embd, H);
    }
}
