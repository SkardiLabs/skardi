//! Chat providers behind `contextualize`.
//!
//! Three wire formats cover the deployments worth supporting:
//!
//! - **anthropic** — the Messages API, with the document block marked
//!   `cache_control: ephemeral`. Every chunk of a document repeats the same
//!   document prefix, so after the first chunk the provider serves that
//!   prefix from its prompt cache; this is what makes "send the whole
//!   document once per chunk" affordable.
//! - **openai** — any `/chat/completions` endpoint (OpenAI itself, DeepSeek,
//!   vLLM, LM Studio, a llama.cpp server). Hosted OpenAI caches long shared
//!   prefixes automatically; the prompt is laid out document-first so it can.
//! - **ollama** — Ollama's native `/api/chat`, local and keyless. The native
//!   endpoint is used instead of Ollama's OpenAI shim because only the native
//!   one accepts `num_ctx`: Ollama's default context window is a few thousand
//!   tokens and it truncates longer prompts silently, which would quietly cut
//!   most of the document out of every request.
//!
//! All outbound calls go through [`ContextProvider`] so tests can mock them.

use std::time::Duration;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::json;

use crate::util::http::parse_retry_after;
use crate::util::text::truncate_chars;

/// Output budget for one context. The prompt asks for one or two sentences
/// (the paper's contexts run 50–100 tokens); the cap only stops a model that
/// ignores the instruction from writing an essay into every chunk.
pub const MAX_OUTPUT_TOKENS: u32 = 200;
const HTTP_TIMEOUT: Duration = Duration::from_secs(180);
const MAX_ATTEMPTS: usize = 4;
const DEFAULT_RETRY_WAIT: Duration = Duration::from_secs(2);
const ERROR_BODY_CHARS: usize = 400;

const ANTHROPIC_URL: &str = "https://api.anthropic.com/v1/messages";
const ANTHROPIC_VERSION: &str = "2023-06-01";
const OPENAI_BASE_URL: &str = "https://api.openai.com/v1";
const OLLAMA_BASE_URL: &str = "http://localhost:11434";
/// Ollama's context window for contextualize calls. Fixed rather than sized
/// per document because changing `num_ctx` between requests makes Ollama
/// reload the model. 16k tokens holds the default 32k-character document cap
/// plus the chunk for Latin-script text.
const DEFAULT_OLLAMA_NUM_CTX: u32 = 16384;

/// One request: situate `chunk` inside `document`.
#[derive(Debug, Clone, Copy)]
pub struct ContextRequest<'a> {
    pub model: &'a str,
    pub document: &'a str,
    pub chunk: &'a str,
}

/// A chat backend that turns (document, chunk) into a short context.
#[async_trait]
pub trait ContextProvider: Send + Sync {
    /// Provider name as written in SQL (`'anthropic'`, `'openai'`, `'ollama'`).
    fn name(&self) -> &str;
    /// In-flight requests per document after the cache-warming first one,
    /// when `CONTEXTUALIZE_CONCURRENCY` does not say otherwise.
    fn default_concurrency(&self) -> usize {
        4
    }
    /// Generate the context for one chunk. Returns the model's raw text.
    async fn contextualize(&self, req: ContextRequest<'_>) -> anyhow::Result<String>;
}

/// The document half of the prompt. Kept byte-identical across a document's
/// chunks so providers can serve it from their prompt cache.
pub fn document_block(document: &str) -> String {
    format!("<document>\n{document}\n</document>")
}

/// The per-chunk half of the prompt — Anthropic's Contextual Retrieval
/// prompt, which situates the chunk for search rather than summarising it.
pub fn chunk_block(chunk: &str) -> String {
    format!(
        "Here is the chunk we want to situate within the whole document\n\
         <chunk>\n{chunk}\n</chunk>\n\
         Please give a short succinct context to situate this chunk within the overall \
         document for the purposes of improving search retrieval of the chunk. Answer only \
         with the succinct context and nothing else."
    )
}

fn http_client() -> anyhow::Result<Client> {
    Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .context("failed to build HTTP client for contextualize")
}

fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

/// POST `body` with bounded retries on 429 and 5xx, honouring `Retry-After`.
async fn post_json(
    client: &Client,
    provider: &str,
    url: &str,
    headers: &[(&str, String)],
    body: &serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    let mut last_error = None;
    for attempt in 0..MAX_ATTEMPTS {
        let mut request = client.post(url).json(body);
        for (name, value) in headers {
            request = request.header(*name, value);
        }
        let response = match request.send().await {
            Ok(r) => r,
            Err(e) if e.is_timeout() || e.is_connect() => {
                last_error = Some(anyhow!("{provider}: request to {url} failed: {e}"));
                tokio::time::sleep(DEFAULT_RETRY_WAIT * (attempt as u32 + 1)).await;
                continue;
            }
            Err(e) => return Err(anyhow!("{provider}: request to {url} failed: {e}")),
        };
        let status = response.status();
        if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
            let wait =
                parse_retry_after(&response).unwrap_or(DEFAULT_RETRY_WAIT * (attempt as u32 + 1));
            let text = response.text().await.unwrap_or_default();
            last_error = Some(anyhow!(
                "{provider}: HTTP {status}: {}",
                truncate_chars(&text, ERROR_BODY_CHARS)
            ));
            tokio::time::sleep(wait).await;
            continue;
        }
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "{provider}: HTTP {status}: {}",
                truncate_chars(&text, ERROR_BODY_CHARS)
            ));
        }
        return response
            .json::<serde_json::Value>()
            .await
            .with_context(|| format!("{provider}: response was not JSON"));
    }
    Err(last_error.unwrap_or_else(|| anyhow!("{provider}: no attempt was made")))
}

// =============================================================================
// Anthropic
// =============================================================================

/// Anthropic Messages API with prompt caching on the document block.
pub struct AnthropicProvider {
    client: Client,
    url: String,
    /// Fixed key (tests); `None` reads `ANTHROPIC_API_KEY` per request.
    api_key: Option<String>,
}

impl AnthropicProvider {
    /// `ANTHROPIC_API_KEY` is read per request so a key exported after the
    /// server started is picked up; `CONTEXTUALIZE_ANTHROPIC_URL` overrides
    /// the endpoint (a gateway or a test server).
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            client: http_client()?,
            url: env_nonempty("CONTEXTUALIZE_ANTHROPIC_URL")
                .unwrap_or_else(|| ANTHROPIC_URL.to_string()),
            api_key: None,
        })
    }

    /// Point at an explicit endpoint with an explicit key (tests, gateways).
    pub fn with_url(url: &str, api_key: &str) -> anyhow::Result<Self> {
        Ok(Self {
            client: http_client()?,
            url: url.to_string(),
            api_key: Some(api_key.to_string()),
        })
    }

    /// Request body, separated out so it can be asserted on without sending.
    pub fn build_body(req: &ContextRequest<'_>) -> serde_json::Value {
        json!({
            "model": req.model,
            "max_tokens": MAX_OUTPUT_TOKENS,
            "temperature": 0,
            "messages": [{
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": document_block(req.document),
                        "cache_control": {"type": "ephemeral"}
                    },
                    {"type": "text", "text": chunk_block(req.chunk)}
                ]
            }]
        })
    }
}

#[derive(Deserialize)]
struct AnthropicResponse {
    content: Vec<AnthropicBlock>,
}

#[derive(Deserialize)]
struct AnthropicBlock {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    text: String,
}

#[async_trait]
impl ContextProvider for AnthropicProvider {
    fn name(&self) -> &str {
        "anthropic"
    }

    async fn contextualize(&self, req: ContextRequest<'_>) -> anyhow::Result<String> {
        let key = self
            .api_key
            .clone()
            .or_else(|| env_nonempty("ANTHROPIC_API_KEY"))
            .ok_or_else(|| {
                anyhow!(
                    "Missing API key: set ANTHROPIC_API_KEY for contextualize('anthropic', ...)"
                )
            })?;
        let headers = [
            ("x-api-key", key),
            ("anthropic-version", ANTHROPIC_VERSION.to_string()),
        ];
        let value = post_json(
            &self.client,
            "anthropic",
            &self.url,
            &headers,
            &Self::build_body(&req),
        )
        .await?;
        let parsed: AnthropicResponse =
            serde_json::from_value(value).context("anthropic: unexpected response shape")?;
        Ok(parsed
            .content
            .into_iter()
            .filter(|b| b.kind == "text")
            .map(|b| b.text)
            .collect::<Vec<_>>()
            .join(""))
    }
}

// =============================================================================
// OpenAI-compatible
// =============================================================================

/// Any `/chat/completions` endpoint.
pub struct OpenAiCompatProvider {
    client: Client,
    base_url: String,
}

impl OpenAiCompatProvider {
    /// `CONTEXTUALIZE_OPENAI_BASE_URL` points it at any compatible server;
    /// `OPENAI_API_KEY` is sent when set and omitted otherwise (local
    /// servers usually take none).
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            client: http_client()?,
            base_url: env_nonempty("CONTEXTUALIZE_OPENAI_BASE_URL")
                .unwrap_or_else(|| OPENAI_BASE_URL.to_string()),
        })
    }

    /// Point at an explicit base URL (tests).
    pub fn with_base_url(base_url: &str) -> anyhow::Result<Self> {
        Ok(Self {
            client: http_client()?,
            base_url: base_url.to_string(),
        })
    }

    /// Request body. Document first, chunk second, in one user message, so
    /// the shared prefix is as long as possible for automatic prefix caching.
    pub fn build_body(req: &ContextRequest<'_>) -> serde_json::Value {
        json!({
            "model": req.model,
            "max_tokens": MAX_OUTPUT_TOKENS,
            "temperature": 0,
            "messages": [{
                "role": "user",
                "content": format!("{}\n{}", document_block(req.document), chunk_block(req.chunk))
            }]
        })
    }
}

#[derive(Deserialize)]
struct ChatCompletion {
    choices: Vec<ChatChoice>,
}

#[derive(Deserialize)]
struct ChatChoice {
    message: ChatMessage,
}

#[derive(Deserialize)]
struct ChatMessage {
    #[serde(default)]
    content: Option<String>,
}

#[async_trait]
impl ContextProvider for OpenAiCompatProvider {
    fn name(&self) -> &str {
        "openai"
    }

    async fn contextualize(&self, req: ContextRequest<'_>) -> anyhow::Result<String> {
        let url = format!("{}/chat/completions", self.base_url.trim_end_matches('/'));
        let headers: Vec<(&str, String)> = env_nonempty("OPENAI_API_KEY")
            .map(|k| vec![("authorization", format!("Bearer {k}"))])
            .unwrap_or_default();
        let value = post_json(
            &self.client,
            "openai",
            &url,
            &headers,
            &Self::build_body(&req),
        )
        .await?;
        let parsed: ChatCompletion =
            serde_json::from_value(value).context("openai: unexpected response shape")?;
        parsed
            .choices
            .into_iter()
            .next()
            .and_then(|c| c.message.content)
            .ok_or_else(|| anyhow!("openai: response carried no message content"))
    }
}

// =============================================================================
// Ollama
// =============================================================================

/// Ollama's native chat endpoint — local, keyless, with an explicit context
/// window.
pub struct OllamaProvider {
    client: Client,
    base_url: String,
    num_ctx: u32,
}

impl OllamaProvider {
    /// `CONTEXTUALIZE_OLLAMA_BASE_URL` (default `http://localhost:11434`) and
    /// `CONTEXTUALIZE_OLLAMA_NUM_CTX` (default 16384 tokens).
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            client: http_client()?,
            base_url: env_nonempty("CONTEXTUALIZE_OLLAMA_BASE_URL")
                .unwrap_or_else(|| OLLAMA_BASE_URL.to_string()),
            num_ctx: env_nonempty("CONTEXTUALIZE_OLLAMA_NUM_CTX")
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(DEFAULT_OLLAMA_NUM_CTX),
        })
    }

    /// Point at an explicit base URL (tests).
    pub fn with_base_url(base_url: &str, num_ctx: u32) -> anyhow::Result<Self> {
        Ok(Self {
            client: http_client()?,
            base_url: base_url.to_string(),
            num_ctx,
        })
    }

    /// Request body. `temperature: 0` keeps re-ingests as repeatable as a
    /// local model allows.
    pub fn build_body(&self, req: &ContextRequest<'_>) -> serde_json::Value {
        json!({
            "model": req.model,
            "stream": false,
            "options": {
                "temperature": 0,
                "num_ctx": self.num_ctx,
                "num_predict": MAX_OUTPUT_TOKENS
            },
            "messages": [{
                "role": "user",
                "content": format!("{}\n{}", document_block(req.document), chunk_block(req.chunk))
            }]
        })
    }
}

#[derive(Deserialize)]
struct OllamaChat {
    message: ChatMessage,
}

#[async_trait]
impl ContextProvider for OllamaProvider {
    fn name(&self) -> &str {
        "ollama"
    }

    /// One at a time. A local Ollama serves requests from a queue, and
    /// requests in flight together evict each other's cached document
    /// prefix: measured 2026-11-27 on an M5 with qwen2.5:3b and ~8.4k-token
    /// documents, 4 concurrent calls averaged ~7.5 s per chunk, sequential
    /// calls ~4.5 s (document prefill served from cache in under a second).
    fn default_concurrency(&self) -> usize {
        1
    }

    async fn contextualize(&self, req: ContextRequest<'_>) -> anyhow::Result<String> {
        let url = format!("{}/api/chat", self.base_url.trim_end_matches('/'));
        let value = post_json(&self.client, "ollama", &url, &[], &self.build_body(&req)).await?;
        let parsed: OllamaChat =
            serde_json::from_value(value).context("ollama: unexpected response shape")?;
        parsed
            .message
            .content
            .ok_or_else(|| anyhow!("ollama: response carried no message content"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::mock_http::{MockHttpServer, MockResponse};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn req() -> ContextRequest<'static> {
        ContextRequest {
            model: "m",
            document: "# Retry guide\nRetries back off exponentially.",
            chunk: "Retries back off exponentially.",
        }
    }

    #[test]
    fn anthropic_body_caches_the_document_and_not_the_chunk() {
        let body = AnthropicProvider::build_body(&req());
        let blocks = body["messages"][0]["content"].as_array().unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0]["cache_control"]["type"], "ephemeral");
        assert!(
            blocks[0]["text"]
                .as_str()
                .unwrap()
                .starts_with("<document>")
        );
        assert!(blocks[1].get("cache_control").is_none());
        assert!(blocks[1]["text"].as_str().unwrap().contains("<chunk>"));
        assert_eq!(body["max_tokens"], MAX_OUTPUT_TOKENS);
    }

    #[test]
    fn openai_and_ollama_bodies_put_the_document_first() {
        let openai = OpenAiCompatProvider::build_body(&req());
        let content = openai["messages"][0]["content"].as_str().unwrap();
        assert!(content.starts_with("<document>"));
        assert!(content.find("<document>") < content.find("<chunk>"));

        let ollama = OllamaProvider::with_base_url("http://x", 8192).unwrap();
        let body = ollama.build_body(&req());
        assert_eq!(body["options"]["num_ctx"], 8192);
        assert_eq!(body["stream"], false);
        assert!(
            body["messages"][0]["content"]
                .as_str()
                .unwrap()
                .starts_with("<document>")
        );
    }

    #[test]
    fn local_ollama_defaults_to_one_request_at_a_time() {
        let ollama = OllamaProvider::with_base_url("http://x", 4096).unwrap();
        let openai = OpenAiCompatProvider::with_base_url("http://x").unwrap();
        assert_eq!(ollama.default_concurrency(), 1);
        assert_eq!(openai.default_concurrency(), 4);
    }

    #[tokio::test]
    async fn ollama_reads_message_content() {
        let server = MockHttpServer::start(|_| {
            MockResponse::new(
                200,
                r#"{"message":{"role":"assistant","content":"From the retry guide."}}"#,
            )
        })
        .await;
        let provider = OllamaProvider::with_base_url(&server.url(), 4096).unwrap();
        let out = provider.contextualize(req()).await.unwrap();
        assert_eq!(out, "From the retry guide.");
        let seen = server.requests();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/api/chat");
    }

    #[tokio::test]
    async fn openai_retries_a_429_then_succeeds() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&calls);
        let server = MockHttpServer::start(move |_| {
            if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                MockResponse::new(429, "slow down").with_header("retry-after", "0")
            } else {
                MockResponse::new(200, r#"{"choices":[{"message":{"content":"ctx"}}]}"#)
            }
        })
        .await;
        let provider = OpenAiCompatProvider::with_base_url(&server.url()).unwrap();
        assert_eq!(provider.contextualize(req()).await.unwrap(), "ctx");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(server.requests()[0].path, "/chat/completions");
    }

    #[tokio::test]
    async fn client_error_is_not_retried_and_carries_the_body() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&calls);
        let server = MockHttpServer::start(move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            MockResponse::new(404, r#"{"error":"model 'nope' not found"}"#)
        })
        .await;
        let provider = OllamaProvider::with_base_url(&server.url(), 4096).unwrap();
        let err = provider.contextualize(req()).await.unwrap_err();
        assert!(err.to_string().contains("not found"), "{err}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn anthropic_joins_text_blocks() {
        let server = MockHttpServer::start(|_| {
            MockResponse::new(
                200,
                r#"{"content":[{"type":"text","text":"Part of "},{"type":"text","text":"the guide."}]}"#,
            )
        })
        .await;
        let provider =
            AnthropicProvider::with_url(&format!("{}/v1/messages", server.url()), "test-key")
                .unwrap();
        assert_eq!(
            provider.contextualize(req()).await.unwrap(),
            "Part of the guide."
        );
        let seen = server.requests();
        assert_eq!(seen[0].header("x-api-key").as_deref(), Some("test-key"));
        assert_eq!(
            seen[0].header("anthropic-version").as_deref(),
            Some(ANTHROPIC_VERSION)
        );
    }
}
