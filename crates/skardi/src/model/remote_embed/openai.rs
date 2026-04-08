use anyhow::{Context, anyhow};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use super::provider::{EmbeddingProvider, EmbeddingRequest, EmbeddingResponse};

/// Provider for APIs that follow the OpenAI embeddings shape.
///
/// Covers OpenAI, Voyage AI, and Mistral — they all accept the same
/// `{ model, input }` JSON and return `{ data: [{ embedding }] }`.
pub struct OpenAiCompatibleProvider {
    provider_name: String,
    base_url: String,
    api_key_env: String,
    client: Client,
    batch_limit: usize,
}

impl OpenAiCompatibleProvider {
    pub fn new(provider_name: &str, base_url: &str, api_key_env: &str, client: Client) -> Self {
        let batch_limit = match provider_name {
            "openai" => 2048,
            _ => 512,
        };
        Self {
            provider_name: provider_name.to_string(),
            base_url: base_url.to_string(),
            api_key_env: api_key_env.to_string(),
            client,
            batch_limit,
        }
    }

    fn api_key(&self) -> anyhow::Result<String> {
        std::env::var(&self.api_key_env).map_err(|_| {
            anyhow!(
                "Missing API key: set the {} environment variable for the '{}' provider",
                self.api_key_env,
                self.provider_name
            )
        })
    }
}

// -- OpenAI-compatible JSON schema ------------------------------------------

#[derive(Serialize)]
struct ApiRequest<'a> {
    model: &'a str,
    input: Vec<&'a str>,
}

#[derive(Deserialize)]
struct ApiResponse {
    data: Vec<EmbeddingData>,
}

#[derive(Deserialize)]
struct EmbeddingData {
    embedding: Vec<f32>,
}

#[async_trait]
impl EmbeddingProvider for OpenAiCompatibleProvider {
    fn name(&self) -> &str {
        &self.provider_name
    }

    fn batch_limit(&self) -> usize {
        self.batch_limit
    }

    async fn embed(&self, req: EmbeddingRequest<'_>) -> anyhow::Result<EmbeddingResponse> {
        let api_key = self.api_key()?;

        let body = ApiRequest {
            model: req.model,
            input: req.texts,
        };

        let resp = self
            .client
            .post(&self.base_url)
            .bearer_auth(&api_key)
            .json(&body)
            .send()
            .await
            .context("HTTP request to embedding API failed")?;

        let status = resp.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            // Single retry with 1s backoff for rate limits
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            let body = ApiRequest {
                model: req.model,
                input: body.input,
            };
            let resp = self
                .client
                .post(&self.base_url)
                .bearer_auth(&api_key)
                .json(&body)
                .send()
                .await
                .context("Retry HTTP request failed")?;

            let status = resp.status();
            if !status.is_success() {
                let text = resp.text().await.unwrap_or_default();
                return Err(anyhow!(
                    "{} API error (status {}): {}",
                    self.provider_name,
                    status,
                    text
                ));
            }

            let api_resp: ApiResponse = resp.json().await.context("Failed to parse response")?;
            return Ok(EmbeddingResponse {
                embeddings: api_resp.data.into_iter().map(|d| d.embedding).collect(),
            });
        }

        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "{} API error (status {}): {}",
                self.provider_name,
                status,
                text
            ));
        }

        let api_resp: ApiResponse = resp.json().await.context("Failed to parse response")?;
        Ok(EmbeddingResponse {
            embeddings: api_resp.data.into_iter().map(|d| d.embedding).collect(),
        })
    }
}
