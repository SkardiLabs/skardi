//! OpenAI-compatible chat-completions provider for `llm_extract`.
//!
//! Covers the cheap chat models the spec targets — DeepSeek, GLM (Zhipu),
//! Gemini (OpenAI-compat endpoint), and OpenAI — since they all speak the same
//! `/chat/completions` shape. One struct parameterized by
//! `(name, base_url, api_key_env)` handles all of them, mirroring
//! `remote_embed::openai::OpenAiCompatibleProvider`.
//!
//! Structured output is forced two ways, in order:
//!   1. `response_format: {type:"json_schema", json_schema:{…}}` wrapping the
//!      caller schema in an `entities` array (each entity prompted to carry a
//!      `_confidence` number).
//!   2. If a provider/model rejects `response_format`, fall back to a
//!      `record_entities` tool + `tool_choice` (same tool shape as the Anthropic
//!      provider).
//!
//! Multimodal escalation attaches the image as an OpenAI `image_url` base64
//! data-URI content block — only present when escalating.

use std::time::Duration;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;

use super::provider::{CompletionProvider, CompletionRequest};

const DEFAULT_RETRY_WAIT: Duration = Duration::from_secs(2);
const MAX_TOKENS: u32 = 4096;
const TOOL_NAME: &str = "record_entities";
const SCHEMA_NAME: &str = "entities_extraction";

/// An OpenAI-compatible completion provider (DeepSeek / GLM / Gemini / OpenAI).
pub struct OpenAiCompatibleCompletionProvider {
    provider_name: String,
    base_url: String,
    api_key_env: String,
    client: Client,
    model: String,
}

impl OpenAiCompatibleCompletionProvider {
    /// Construct a provider. `base_url` is the API root (e.g.
    /// `https://api.deepseek.com/v1`); requests POST to `{base_url}/chat/completions`.
    /// `model` is the chat model id (from `LLM_EXTRACT_MODEL`).
    pub fn new(name: &str, base_url: &str, api_key_env: &str, client: Client, model: &str) -> Self {
        Self {
            provider_name: name.to_string(),
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key_env: api_key_env.to_string(),
            client,
            model: model.to_string(),
        }
    }

    /// Human-readable provider name.
    pub fn name(&self) -> &str {
        &self.provider_name
    }

    fn api_key(&self) -> anyhow::Result<String> {
        std::env::var(&self.api_key_env).map_err(|_| {
            anyhow!(
                "Missing API key: set the {} environment variable for the '{}' llm_extract provider",
                self.api_key_env,
                self.provider_name
            )
        })
    }

    fn endpoint(&self) -> String {
        format!("{}/chat/completions", self.base_url)
    }

    /// Wrap the caller's JSON Schema in an `entities` array object, falling back
    /// to a permissive object if the caller schema is unparseable.
    fn entities_object_schema(json_schema: &str) -> serde_json::Value {
        let entity_schema: serde_json::Value =
            serde_json::from_str(json_schema).unwrap_or_else(|_| json!({"type": "object"}));
        json!({
            "type": "object",
            "properties": {
                "entities": {
                    "type": "array",
                    "description": "All structured entities extracted from the input.",
                    "items": entity_schema
                }
            },
            "required": ["entities"]
        })
    }

    /// Build the user `messages` content: optional image block first, then text.
    fn user_content(req: &CompletionRequest<'_>) -> serde_json::Value {
        let mut content: Vec<serde_json::Value> = Vec::new();
        if let Some(image) = &req.image {
            // OpenAI image_url block with a base64 data URI.
            content.push(json!({
                "type": "image_url",
                "image_url": {
                    "url": format!("data:{};base64,{}", image.mime, image.base64)
                }
            }));
        }
        content.push(json!({
            "type": "text",
            "text": format!(
                "Extract every structured entity from the following content. For each \
                 entity include a `_confidence` number between 0 and 1 reflecting your \
                 certainty. If no entities are present, return an empty list.\n\n\
                 Content:\n{}",
                req.text
            )
        }));
        json!(content)
    }

    /// Build the `/chat/completions` body using `response_format` json_schema.
    /// Pulled out so it can be unit-tested without sending.
    fn build_body_response_format(&self, req: &CompletionRequest<'_>) -> serde_json::Value {
        let schema = Self::entities_object_schema(req.json_schema);
        json!({
            "model": self.model,
            "max_tokens": MAX_TOKENS,
            "messages": [{
                "role": "user",
                "content": Self::user_content(req),
            }],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": SCHEMA_NAME,
                    "strict": true,
                    "schema": schema,
                }
            },
        })
    }

    /// Build the `/chat/completions` body using a forced tool call. Used as the
    /// fallback when a provider/model rejects `response_format`.
    fn build_body_tool(&self, req: &CompletionRequest<'_>) -> serde_json::Value {
        let schema = Self::entities_object_schema(req.json_schema);
        json!({
            "model": self.model,
            "max_tokens": MAX_TOKENS,
            "messages": [{
                "role": "user",
                "content": Self::user_content(req),
            }],
            "tools": [{
                "type": "function",
                "function": {
                    "name": TOOL_NAME,
                    "description": "Record the structured entities extracted from the content.",
                    "parameters": schema,
                }
            }],
            "tool_choice": {
                "type": "function",
                "function": {"name": TOOL_NAME}
            },
        })
    }
}

// -- OpenAI-compatible response shape ---------------------------------------

#[derive(Deserialize)]
struct ApiResponse {
    choices: Vec<Choice>,
}

#[derive(Deserialize)]
struct Choice {
    message: Message,
}

#[derive(Deserialize)]
struct Message {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<ToolCall>,
}

#[derive(Deserialize)]
struct ToolCall {
    function: FunctionCall,
}

#[derive(Deserialize)]
struct FunctionCall {
    arguments: String,
}

/// Extract the `entities` array from the `{ "entities": [...] }` object.
fn entities_from_object(obj: &serde_json::Value) -> anyhow::Result<Vec<serde_json::Value>> {
    obj.get("entities")
        .and_then(|e| e.as_array())
        .map(|a| a.clone())
        .ok_or_else(|| anyhow!("structured output missing 'entities' array"))
}

/// Parse a `/chat/completions` response body into entities, handling both the
/// `response_format` (JSON string in `message.content`) and tool-call
/// (`message.tool_calls[0].function.arguments`) shapes.
fn parse_entities(body: &str) -> anyhow::Result<Vec<serde_json::Value>> {
    let resp: ApiResponse =
        serde_json::from_str(body).context("Failed to parse chat completions response")?;
    let choice = resp
        .choices
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("chat completions response had no choices"))?;

    // Prefer a tool call if present (fallback path).
    if let Some(tc) = choice.message.tool_calls.into_iter().next() {
        let obj: serde_json::Value = serde_json::from_str(&tc.function.arguments)
            .context("Failed to parse tool_call arguments as JSON")?;
        return entities_from_object(&obj);
    }

    // Otherwise parse the JSON content (response_format path).
    let content = choice
        .message
        .content
        .ok_or_else(|| anyhow!("chat completions message had neither content nor tool_calls"))?;
    let obj: serde_json::Value =
        serde_json::from_str(&content).context("Failed to parse message content as JSON")?;
    entities_from_object(&obj)
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

/// One outcome of an HTTP attempt.
enum Attempt {
    Ok(String),
    /// Provider rejected the request body (4xx) — caller may try a fallback.
    BadRequest(String),
    Err(anyhow::Error),
}

/// Send a request with a single retry on 429. Returns the body text on success,
/// a `BadRequest` on 4xx (so the caller can fall back), or an error otherwise.
async fn send_once_with_retry(
    build_request: impl Fn() -> reqwest::RequestBuilder,
    provider_label: &str,
) -> Attempt {
    let resp = match build_request().send().await {
        Ok(r) => r,
        Err(e) => return Attempt::Err(anyhow!("HTTP request to {provider_label} failed: {e}")),
    };

    if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        let wait = parse_retry_after(&resp);
        tracing::warn!("{provider_label}: rate-limited (429), retrying after {wait:?}");
        tokio::time::sleep(wait).await;
        let resp = match build_request().send().await {
            Ok(r) => r,
            Err(e) => {
                return Attempt::Err(anyhow!(
                    "Retry HTTP request to {provider_label} failed: {e}"
                ));
            }
        };
        return classify(resp, provider_label).await;
    }

    classify(resp, provider_label).await
}

async fn classify(resp: reqwest::Response, provider_label: &str) -> Attempt {
    let status = resp.status();
    if status.is_success() {
        match resp.text().await {
            Ok(t) => Attempt::Ok(t),
            Err(e) => Attempt::Err(anyhow!(
                "Failed to read {provider_label} response body: {e}"
            )),
        }
    } else if status.is_client_error() {
        let text = resp.text().await.unwrap_or_default();
        Attempt::BadRequest(format!(
            "{provider_label} API error (status {status}): {text}"
        ))
    } else {
        let text = resp.text().await.unwrap_or_default();
        Attempt::Err(anyhow!(
            "{provider_label} API error (status {status}): {text}"
        ))
    }
}

#[async_trait]
impl CompletionProvider for OpenAiCompatibleCompletionProvider {
    async fn complete(
        &self,
        req: CompletionRequest<'_>,
    ) -> anyhow::Result<super::provider::CompletionResponse> {
        let api_key = self.api_key()?;
        let url = self.endpoint();

        // Attempt 1: response_format json_schema.
        let rf_body = self.build_body_response_format(&req);
        let rf_request = || self.client.post(&url).bearer_auth(&api_key).json(&rf_body);
        match send_once_with_retry(rf_request, &self.provider_name).await {
            Attempt::Ok(text) => {
                let entities = parse_entities(&text)?;
                return Ok(super::provider::CompletionResponse { entities });
            }
            Attempt::BadRequest(msg) => {
                // The provider/model likely doesn't support response_format —
                // fall back to a forced tool call.
                tracing::warn!(
                    "{}: response_format rejected ({msg}); falling back to tool call",
                    self.provider_name
                );
            }
            Attempt::Err(e) => return Err(e),
        }

        // Attempt 2: forced tool call.
        let tool_body = self.build_body_tool(&req);
        let tool_request = || {
            self.client
                .post(&url)
                .bearer_auth(&api_key)
                .json(&tool_body)
        };
        match send_once_with_retry(tool_request, &self.provider_name).await {
            Attempt::Ok(text) => {
                let entities = parse_entities(&text)?;
                Ok(super::provider::CompletionResponse { entities })
            }
            Attempt::BadRequest(msg) => Err(anyhow!(msg)),
            Attempt::Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::llm_extract::provider::ImageInput;

    fn provider() -> OpenAiCompatibleCompletionProvider {
        OpenAiCompatibleCompletionProvider::new(
            "deepseek",
            "https://api.deepseek.com/v1",
            "DEEPSEEK_API_KEY",
            Client::new(),
            "deepseek-chat",
        )
    }

    #[test]
    fn endpoint_appends_chat_completions() {
        let p = provider();
        assert_eq!(p.endpoint(), "https://api.deepseek.com/v1/chat/completions");
        // trailing slash on base_url is trimmed
        let p2 = OpenAiCompatibleCompletionProvider::new(
            "openai",
            "https://api.openai.com/v1/",
            "OPENAI_API_KEY",
            Client::new(),
            "gpt-4o-mini",
        );
        assert_eq!(p2.endpoint(), "https://api.openai.com/v1/chat/completions");
    }

    #[test]
    fn response_format_body_shape() {
        let p = provider();
        let req = CompletionRequest {
            json_schema: r#"{"type":"object","properties":{"name":{"type":"string"}}}"#,
            text: "page body",
            image: None,
        };
        let body = p.build_body_response_format(&req);

        assert_eq!(body["model"], "deepseek-chat");
        assert_eq!(body["max_tokens"], MAX_TOKENS);

        // response_format json_schema present
        assert_eq!(body["response_format"]["type"], "json_schema");
        assert_eq!(body["response_format"]["json_schema"]["name"], SCHEMA_NAME);
        let items =
            &body["response_format"]["json_schema"]["schema"]["properties"]["entities"]["items"];
        assert_eq!(items["properties"]["name"]["type"], "string");

        // single user message, text-only content (no image block)
        let content = body["messages"][0]["content"].as_array().unwrap();
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], "text");
        assert!(content[0]["text"].as_str().unwrap().contains("page body"));
    }

    #[test]
    fn response_format_body_includes_image_url_first() {
        let p = provider();
        let req = CompletionRequest {
            json_schema: r#"{"type":"object"}"#,
            text: "body",
            image: Some(ImageInput {
                base64: "aGVsbG8=".to_string(),
                mime: "image/png".to_string(),
            }),
        };
        let body = p.build_body_response_format(&req);
        let content = body["messages"][0]["content"].as_array().unwrap();
        assert_eq!(content.len(), 2);
        assert_eq!(content[0]["type"], "image_url");
        assert_eq!(
            content[0]["image_url"]["url"],
            "data:image/png;base64,aGVsbG8="
        );
        assert_eq!(content[1]["type"], "text");
    }

    #[test]
    fn tool_fallback_body_shape() {
        let p = provider();
        let req = CompletionRequest {
            json_schema: r#"{"type":"object","properties":{"name":{"type":"string"}}}"#,
            text: "body",
            image: None,
        };
        let body = p.build_body_tool(&req);
        assert_eq!(body["tool_choice"]["type"], "function");
        assert_eq!(body["tool_choice"]["function"]["name"], TOOL_NAME);
        let tool = &body["tools"][0];
        assert_eq!(tool["type"], "function");
        assert_eq!(tool["function"]["name"], TOOL_NAME);
        let items = &tool["function"]["parameters"]["properties"]["entities"]["items"];
        assert_eq!(items["properties"]["name"]["type"], "string");
    }

    #[test]
    fn malformed_schema_falls_back_to_object() {
        let p = provider();
        let req = CompletionRequest {
            json_schema: "not json",
            text: "body",
            image: None,
        };
        let body = p.build_body_response_format(&req);
        let items =
            &body["response_format"]["json_schema"]["schema"]["properties"]["entities"]["items"];
        assert_eq!(items["type"], "object");
    }

    #[test]
    fn parse_entities_from_response_format_content() {
        let body = r#"{
            "choices": [
                {"message": {"content": "{\"entities\": [{\"name\": \"a\", \"_confidence\": 0.9}]}"}}
            ]
        }"#;
        let entities = parse_entities(body).unwrap();
        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0]["name"], "a");
    }

    #[test]
    fn parse_entities_from_tool_call() {
        let body = r#"{
            "choices": [
                {"message": {"content": null, "tool_calls": [
                    {"function": {"name": "record_entities",
                     "arguments": "{\"entities\": [{\"x\": 1}, {\"x\": 2}]}"}}
                ]}}
            ]
        }"#;
        let entities = parse_entities(body).unwrap();
        assert_eq!(entities.len(), 2);
        assert_eq!(entities[1]["x"], 2);
    }

    #[test]
    fn parse_entities_errors_without_structure() {
        let body = r#"{"choices": [{"message": {"content": "plain text not json"}}]}"#;
        assert!(parse_entities(body).is_err());
    }
}
