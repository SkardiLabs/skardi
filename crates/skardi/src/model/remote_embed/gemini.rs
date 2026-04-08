use anyhow::{Context, anyhow};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use super::provider::{EmbeddingProvider, EmbeddingRequest, EmbeddingResponse};

const GEMINI_BATCH_EMBED_BASE: &str = "https://generativelanguage.googleapis.com/v1beta";

/// Google Gemini embedding provider.
///
/// Gemini uses a different request/response shape and passes the API key as a
/// query parameter rather than a Bearer token.
pub struct GeminiProvider {
    client: Client,
}

impl GeminiProvider {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    fn api_key() -> anyhow::Result<String> {
        std::env::var("GEMINI_API_KEY").map_err(|_| {
            anyhow!("Missing API key: set the GEMINI_API_KEY environment variable for the 'gemini' provider")
        })
    }
}

// -- Gemini JSON schema -----------------------------------------------------

#[derive(Serialize)]
struct GeminiRequest<'a> {
    requests: Vec<GeminiEmbedRequest<'a>>,
}

#[derive(Serialize)]
struct GeminiEmbedRequest<'a> {
    model: &'a str,
    content: GeminiContent<'a>,
}

#[derive(Serialize)]
struct GeminiContent<'a> {
    parts: Vec<GeminiPart<'a>>,
}

#[derive(Serialize)]
struct GeminiPart<'a> {
    text: &'a str,
}

#[derive(Deserialize)]
struct GeminiResponse {
    embeddings: Vec<GeminiEmbedding>,
}

#[derive(Deserialize)]
struct GeminiEmbedding {
    values: Vec<f32>,
}

#[async_trait]
impl EmbeddingProvider for GeminiProvider {
    fn name(&self) -> &str {
        "gemini"
    }

    fn batch_limit(&self) -> usize {
        100 // Gemini batchEmbedContents limit
    }

    async fn embed(&self, req: EmbeddingRequest<'_>) -> anyhow::Result<EmbeddingResponse> {
        let api_key = Self::api_key()?;
        let model_id = format!("models/{}", req.model);

        let url = format!(
            "{}/{}:batchEmbedContents",
            GEMINI_BATCH_EMBED_BASE, model_id
        );

        let body = GeminiRequest {
            requests: req
                .texts
                .iter()
                .map(|text| GeminiEmbedRequest {
                    model: &model_id,
                    content: GeminiContent {
                        parts: vec![GeminiPart { text }],
                    },
                })
                .collect(),
        };

        let build_request = || {
            self.client
                .post(&url)
                .header("x-goog-api-key", &api_key)
                .json(&body)
        };

        let resp = super::send_with_rate_limit_retry(build_request, "gemini").await?;

        let api_resp: GeminiResponse = resp
            .json()
            .await
            .context("Failed to parse Gemini response")?;
        Ok(EmbeddingResponse {
            embeddings: api_resp.embeddings.into_iter().map(|e| e.values).collect(),
        })
    }
}
