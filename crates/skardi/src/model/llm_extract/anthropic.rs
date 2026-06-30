//! Anthropic (Claude) completion provider for `llm_extract`.
//!
//! There is no existing Claude client in the repo — this adds one. It POSTs the
//! Anthropic Messages API (`/v1/messages`) and forces structured output via
//! tool-use: a single tool whose `input_schema` wraps the caller's `json_schema`
//! in an `entities` array, with `tool_choice` pinned to that tool. The model's
//! `tool_use.input.entities` is parsed into `Vec<serde_json::Value>`.
//!
//! Each entity is asked to carry a `_confidence` number (0–1) so the UDF's
//! confidence gate has a signal to act on.
//!
//! Mirrors `remote_embed`'s mechanics: a shared `reqwest::Client` with a
//! timeout, eager warning when the API key is unset, and a 429 retry helper.

use std::time::Duration;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;

use super::provider::{CompletionProvider, CompletionRequest, CompletionResponse};

const MESSAGES_URL: &str = "https://api.anthropic.com/v1/messages";
const ANTHROPIC_VERSION: &str = "2023-06-01";
const HTTP_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_RETRY_WAIT: Duration = Duration::from_secs(2);
const MAX_TOKENS: u32 = 4096;
const TOOL_NAME: &str = "record_entities";

/// Default Claude model. Current model id per the Anthropic API; overridable via
/// `LLM_EXTRACT_MODEL`.
const DEFAULT_MODEL: &str = "claude-opus-4-8";

/// Anthropic completion provider.
pub struct AnthropicProvider {
    client: Client,
    model: String,
}

impl AnthropicProvider {
    /// Build a provider, reading `ANTHROPIC_API_KEY` and `LLM_EXTRACT_MODEL`
    /// from the environment. Warns eagerly (does not panic) when the API key is
    /// unset, mirroring `remote_embed`'s startup behaviour.
    pub fn from_env() -> Self {
        if std::env::var("ANTHROPIC_API_KEY").is_err() {
            tracing::warn!(
                "llm_extract: ANTHROPIC_API_KEY not set — llm_extract queries will fail"
            );
        }
        let model =
            std::env::var("LLM_EXTRACT_MODEL").unwrap_or_else(|_| DEFAULT_MODEL.to_string());

        let client = Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .expect("failed to build reqwest client");

        Self { client, model }
    }

    fn api_key() -> anyhow::Result<String> {
        std::env::var("ANTHROPIC_API_KEY").map_err(|_| {
            anyhow!(
                "Missing API key: set the ANTHROPIC_API_KEY environment variable for llm_extract"
            )
        })
    }

    /// Build the JSON request body for the Messages API. Pulled out so it can be
    /// unit-tested without sending anything.
    fn build_body(&self, req: &CompletionRequest<'_>) -> serde_json::Value {
        // Parse the caller's JSON Schema; fall back to a permissive object so a
        // malformed schema still produces a valid request rather than erroring.
        let entity_schema: serde_json::Value =
            serde_json::from_str(req.json_schema).unwrap_or_else(|_| json!({"type": "object"}));

        // Tool whose input is an array of entities. Each entity follows the
        // caller's schema; we additionally request a `_confidence` field.
        let tool_schema = json!({
            "type": "object",
            "properties": {
                "entities": {
                    "type": "array",
                    "description": "All structured entities extracted from the input.",
                    "items": entity_schema
                }
            },
            "required": ["entities"]
        });

        // User content: optional image block first, then the text block.
        let mut content: Vec<serde_json::Value> = Vec::new();
        if let Some(image) = &req.image {
            content.push(json!({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": image.mime,
                    "data": image.base64,
                }
            }));
        }
        content.push(json!({
            "type": "text",
            "text": format!(
                "Extract every structured entity from the following content using the \
                 `{TOOL_NAME}` tool. For each entity include a `_confidence` number \
                 between 0 and 1 reflecting your certainty. If no entities are present, \
                 return an empty list.\n\nContent:\n{}",
                req.text
            )
        }));

        json!({
            "model": self.model,
            "max_tokens": MAX_TOKENS,
            "tools": [{
                "name": TOOL_NAME,
                "description": "Record the structured entities extracted from the content.",
                "input_schema": tool_schema,
            }],
            "tool_choice": {"type": "tool", "name": TOOL_NAME},
            "messages": [{
                "role": "user",
                "content": content,
            }],
        })
    }
}

// -- Anthropic response shape -----------------------------------------------

#[derive(Deserialize)]
struct ApiResponse {
    content: Vec<ContentBlock>,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum ContentBlock {
    #[serde(rename = "tool_use")]
    ToolUse { input: serde_json::Value },
    #[serde(other)]
    Other,
}

/// Parse the Messages API response into entities. Pulled out for unit testing.
fn parse_entities(body: &str) -> anyhow::Result<Vec<serde_json::Value>> {
    let resp: ApiResponse =
        serde_json::from_str(body).context("Failed to parse Anthropic response")?;

    for block in resp.content {
        if let ContentBlock::ToolUse { input } = block {
            let entities = input
                .get("entities")
                .and_then(|e| e.as_array())
                .ok_or_else(|| anyhow!("tool_use input missing 'entities' array"))?;
            return Ok(entities.clone());
        }
    }
    Err(anyhow!("Anthropic response contained no tool_use block"))
}

/// Parse `Retry-After` as seconds, falling back to `DEFAULT_RETRY_WAIT`.
fn parse_retry_after(resp: &reqwest::Response) -> Duration {
    resp.headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_RETRY_WAIT)
}

/// Send an HTTP request with a single retry on 429 (rate limit).
/// Mirrors `remote_embed::send_with_rate_limit_retry`.
async fn send_with_rate_limit_retry(
    build_request: impl Fn() -> reqwest::RequestBuilder,
) -> anyhow::Result<reqwest::Response> {
    let resp = build_request()
        .send()
        .await
        .context("HTTP request to Anthropic API failed")?;

    if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        let wait = parse_retry_after(&resp);
        tracing::warn!("anthropic: rate-limited (429), retrying after {wait:?}");
        tokio::time::sleep(wait).await;

        let resp = build_request()
            .send()
            .await
            .context("Retry HTTP request to Anthropic API failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Anthropic API error (status {status}): {text}"));
        }
        return Ok(resp);
    }

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow!("Anthropic API error (status {status}): {text}"));
    }

    Ok(resp)
}

#[async_trait]
impl CompletionProvider for AnthropicProvider {
    async fn complete(&self, req: CompletionRequest<'_>) -> anyhow::Result<CompletionResponse> {
        let api_key = Self::api_key()?;
        let body = self.build_body(&req);

        let build_request = || {
            self.client
                .post(MESSAGES_URL)
                .header("x-api-key", &api_key)
                .header("anthropic-version", ANTHROPIC_VERSION)
                .json(&body)
        };

        let resp = send_with_rate_limit_retry(build_request).await?;
        let text = resp.text().await.context("Failed to read response body")?;
        let entities = parse_entities(&text)?;
        Ok(CompletionResponse { entities })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::llm_extract::provider::ImageInput;

    fn provider() -> AnthropicProvider {
        AnthropicProvider {
            client: Client::new(),
            model: "test-model".to_string(),
        }
    }

    #[test]
    fn from_env_does_not_panic_without_key() {
        // Ensure the key is absent for this check.
        unsafe {
            std::env::remove_var("ANTHROPIC_API_KEY");
        }
        // Should warn, not panic.
        let _ = AnthropicProvider::from_env();
    }

    #[test]
    fn build_body_text_only_shape() {
        let p = provider();
        let req = CompletionRequest {
            model: "ignored",
            json_schema: r#"{"type":"object","properties":{"name":{"type":"string"}}}"#,
            text: "page body",
            image: None,
        };
        let body = p.build_body(&req);

        // model comes from the provider, not the request
        assert_eq!(body["model"], "test-model");
        assert_eq!(body["max_tokens"], MAX_TOKENS);

        // tool_choice forces our tool
        assert_eq!(body["tool_choice"]["type"], "tool");
        assert_eq!(body["tool_choice"]["name"], TOOL_NAME);

        // tool schema wraps the caller schema in an entities array
        let tool = &body["tools"][0];
        assert_eq!(tool["name"], TOOL_NAME);
        let items = &tool["input_schema"]["properties"]["entities"]["items"];
        assert_eq!(items["properties"]["name"]["type"], "string");

        // single user message, text-only content (no image block)
        let content = body["messages"][0]["content"].as_array().unwrap();
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], "text");
        assert!(content[0]["text"].as_str().unwrap().contains("page body"));
    }

    #[test]
    fn build_body_includes_image_block_first() {
        let p = provider();
        let req = CompletionRequest {
            model: "ignored",
            json_schema: r#"{"type":"object"}"#,
            text: "body",
            image: Some(ImageInput {
                base64: "aGVsbG8=".to_string(),
                mime: "image/png".to_string(),
            }),
        };
        let body = p.build_body(&req);
        let content = body["messages"][0]["content"].as_array().unwrap();
        assert_eq!(content.len(), 2);
        // image block first
        assert_eq!(content[0]["type"], "image");
        assert_eq!(content[0]["source"]["type"], "base64");
        assert_eq!(content[0]["source"]["media_type"], "image/png");
        assert_eq!(content[0]["source"]["data"], "aGVsbG8=");
        // text block second
        assert_eq!(content[1]["type"], "text");
    }

    #[test]
    fn build_body_tolerates_malformed_schema() {
        let p = provider();
        let req = CompletionRequest {
            model: "ignored",
            json_schema: "not json at all",
            text: "body",
            image: None,
        };
        let body = p.build_body(&req);
        // Falls back to a permissive object schema rather than erroring.
        let items = &body["tools"][0]["input_schema"]["properties"]["entities"]["items"];
        assert_eq!(items["type"], "object");
    }

    #[test]
    fn parse_entities_extracts_tool_use_array() {
        let body = r#"{
            "content": [
                {"type": "text", "text": "Here you go"},
                {"type": "tool_use", "id": "x", "name": "record_entities",
                 "input": {"entities": [
                     {"name": "a", "_confidence": 0.9},
                     {"name": "b", "_confidence": 0.5}
                 ]}}
            ]
        }"#;
        let entities = parse_entities(body).unwrap();
        assert_eq!(entities.len(), 2);
        assert_eq!(entities[0]["name"], "a");
        assert_eq!(entities[1]["_confidence"], 0.5);
    }

    #[test]
    fn parse_entities_errors_without_tool_use() {
        let body = r#"{"content": [{"type": "text", "text": "no tool"}]}"#;
        assert!(parse_entities(body).is_err());
    }
}
