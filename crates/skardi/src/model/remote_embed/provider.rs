use async_trait::async_trait;

/// Request to embed a batch of texts.
pub struct EmbeddingRequest<'a> {
    pub model: &'a str,
    pub texts: Vec<&'a str>,
}

/// Response containing embedding vectors, one per input text.
pub struct EmbeddingResponse {
    pub embeddings: Vec<Vec<f32>>,
}

/// A remote embedding API provider (e.g. OpenAI, Gemini, Voyage, Mistral).
#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    /// Human-readable provider name (e.g. "openai").
    fn name(&self) -> &str;

    /// Maximum number of texts per API call.
    fn batch_limit(&self) -> usize {
        512
    }

    /// Make the HTTP embedding call.
    async fn embed(&self, req: EmbeddingRequest<'_>) -> anyhow::Result<EmbeddingResponse>;
}
