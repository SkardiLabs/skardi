//! Completion provider abstraction for `llm_extract`.
//!
//! All outbound LLM calls go through [`CompletionProvider`] so unit tests can
//! mock it — there are no live API calls in the test suite. Mirrors
//! `remote_embed::provider` but for *completion* (structured extraction)
//! instead of *embedding*.

use async_trait::async_trait;

/// An image attached to a completion request for multimodal escalation.
#[derive(Debug)]
pub struct ImageInput {
    /// Base64-encoded image bytes.
    pub base64: String,
    /// MIME type, e.g. `image/png`.
    pub mime: String,
}

/// A single structured-extraction request for one input row.
///
/// The model id is *not* carried here: each provider is constructed with its
/// own model (from `LLM_EXTRACT_MODEL`) and uses that. A per-request model field
/// would be dead state — the UDF has no other model to supply.
pub struct CompletionRequest<'a> {
    /// JSON Schema (the literal third UDF arg) describing fields to extract.
    pub json_schema: &'a str,
    /// The unstructured text for this row.
    pub text: &'a str,
    /// Optional image, present only when escalating to multimodal.
    pub image: Option<ImageInput>,
}

/// Result of a completion: zero or more extracted entities.
///
/// Each entity is a `serde_json::Value` object carrying the schema fields and
/// optionally a `_confidence` number elicited from the model.
pub struct CompletionResponse {
    pub entities: Vec<serde_json::Value>,
}

/// A remote LLM completion provider (e.g. Anthropic).
#[async_trait]
pub trait CompletionProvider: Send + Sync {
    /// Run one structured-extraction completion.
    async fn complete(&self, req: CompletionRequest<'_>) -> anyhow::Result<CompletionResponse>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Minimal provider proving the trait shape compiles and is usable.
    struct MockProvider;

    #[async_trait]
    impl CompletionProvider for MockProvider {
        async fn complete(
            &self,
            _req: CompletionRequest<'_>,
        ) -> anyhow::Result<CompletionResponse> {
            Ok(CompletionResponse {
                entities: vec![
                    json!({"name": "a", "_confidence": 0.9}),
                    json!({"name": "b", "_confidence": 0.8}),
                ],
            })
        }
    }

    #[tokio::test]
    async fn mock_provider_returns_two_entities() {
        let provider = MockProvider;
        let req = CompletionRequest {
            json_schema: r#"{"type":"object"}"#,
            text: "hello",
            image: None,
        };
        let resp = provider.complete(req).await.unwrap();
        assert_eq!(resp.entities.len(), 2);
    }
}
