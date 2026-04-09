mod gemini;
mod openai;
pub mod provider;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use arrow::array::{Array, StringArray};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use reqwest::Client;

use self::gemini::GeminiProvider;
use self::openai::OpenAiCompatibleProvider;
use self::provider::{EmbeddingProvider, EmbeddingRequest};
use super::{embedding_return_type, vecs_to_list_array};

const HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_RETRY_WAIT: Duration = Duration::from_secs(2);

const OPENAI_EMBEDDINGS_URL: &str = "https://api.openai.com/v1/embeddings";
const VOYAGE_EMBEDDINGS_URL: &str = "https://api.voyageai.com/v1/embeddings";
const MISTRAL_EMBEDDINGS_URL: &str = "https://api.mistral.ai/v1/embeddings";

/// Parse `Retry-After` header value as seconds, falling back to `DEFAULT_RETRY_WAIT`.
fn parse_retry_after(resp: &reqwest::Response) -> Duration {
    resp.headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_RETRY_WAIT)
}

/// Send an HTTP request with a single retry on 429 (rate limit).
///
/// `build_request` is called to construct the request (possibly twice).
/// `provider_label` is used in error messages.
async fn send_with_rate_limit_retry(
    build_request: impl Fn() -> reqwest::RequestBuilder,
    provider_label: &str,
) -> anyhow::Result<reqwest::Response> {
    let resp = build_request()
        .send()
        .await
        .context("HTTP request to embedding API failed")?;

    if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        let wait = parse_retry_after(&resp);
        tracing::warn!("{provider_label}: rate-limited (429), retrying after {wait:?}");
        tokio::time::sleep(wait).await;

        let resp = build_request()
            .send()
            .await
            .context("Retry HTTP request to embedding API failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "{provider_label} API error (status {status}): {text}"
            ));
        }
        return Ok(resp);
    }

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow!(
            "{provider_label} API error (status {status}): {text}"
        ));
    }

    Ok(resp)
}

// =============================================================================
// RemoteEmbedRegistry — provider dispatch
// =============================================================================

/// Holds a shared `reqwest::Client` and a map of registered embedding
/// providers. Passed into the UDF so it can dispatch by provider name.
pub struct RemoteEmbedRegistry {
    providers: HashMap<String, Box<dyn EmbeddingProvider>>,
}

impl std::fmt::Debug for RemoteEmbedRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteEmbedRegistry")
            .field("providers", &self.providers.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl RemoteEmbedRegistry {
    /// Create a registry with the built-in providers (OpenAI, Voyage, Mistral, Gemini).
    pub fn new() -> Self {
        let client = Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .expect("failed to build reqwest client");

        let mut providers: HashMap<String, Box<dyn EmbeddingProvider>> = HashMap::new();

        providers.insert(
            "openai".to_string(),
            Box::new(OpenAiCompatibleProvider::new(
                "openai",
                OPENAI_EMBEDDINGS_URL,
                "OPENAI_API_KEY",
                client.clone(),
            )),
        );
        providers.insert(
            "voyage".to_string(),
            Box::new(OpenAiCompatibleProvider::new(
                "voyage",
                VOYAGE_EMBEDDINGS_URL,
                "VOYAGE_API_KEY",
                client.clone(),
            )),
        );
        providers.insert(
            "mistral".to_string(),
            Box::new(OpenAiCompatibleProvider::new(
                "mistral",
                MISTRAL_EMBEDDINGS_URL,
                "MISTRAL_API_KEY",
                client.clone(),
            )),
        );
        providers.insert("gemini".to_string(), Box::new(GeminiProvider::new(client)));

        // Warn eagerly about missing API keys so misconfiguration is visible at
        // startup rather than at first query time.
        for (name, env_var) in [
            ("openai", "OPENAI_API_KEY"),
            ("voyage", "VOYAGE_API_KEY"),
            ("mistral", "MISTRAL_API_KEY"),
            ("gemini", "GEMINI_API_KEY"),
        ] {
            if std::env::var(env_var).is_err() {
                tracing::warn!(
                    "remote_embed provider '{}': {} not set — queries using this provider will fail",
                    name,
                    env_var
                );
            }
        }

        Self { providers }
    }

    /// Register the `remote_embed` UDF with a DataFusion `SessionContext`.
    ///
    /// Usage: `remote_embed('openai', 'text-embedding-3-small', text_col) -> List<Float32>`
    pub fn register_remote_embed_udf(self: &Arc<Self>, ctx: &mut SessionContext) {
        let udf = ScalarUDF::new_from_impl(RemoteEmbedUDF::new(Arc::clone(self)));
        ctx.register_udf(udf);
        tracing::info!("Registered 'remote_embed' UDF");
    }
}

impl Default for RemoteEmbedRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// RemoteEmbedUDF — ScalarUDFImpl
// =============================================================================

#[derive(Debug)]
struct RemoteEmbedUDF {
    registry: Arc<RemoteEmbedRegistry>,
    signature: Signature,
}

impl PartialEq for RemoteEmbedUDF {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.registry, &other.registry)
    }
}

impl Eq for RemoteEmbedUDF {}

impl std::hash::Hash for RemoteEmbedUDF {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.registry).hash(state);
    }
}

impl RemoteEmbedUDF {
    fn new(registry: Arc<RemoteEmbedRegistry>) -> Self {
        Self {
            registry,
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for RemoteEmbedUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "remote_embed"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[arrow::datatypes::DataType],
    ) -> datafusion::common::Result<arrow::datatypes::DataType> {
        Ok(embedding_return_type())
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let args = args.args;

        if args.len() < 3 {
            return Err(DataFusionError::Execution(
                "remote_embed requires 3 arguments: provider, model, text_column".to_string(),
            ));
        }

        // --- arg 0: provider name (string literal) ---
        let provider_name = extract_string_literal(&args[0], "provider (first argument)")?;

        // --- arg 1: model name (string literal) ---
        let model_name = extract_string_literal(&args[1], "model (second argument)")?;

        // --- arg 2: text column (Utf8 array) ---
        let text_array = match &args[2] {
            ColumnarValue::Array(arr) => arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "Third argument to remote_embed must be a Utf8 text column".to_string(),
                    )
                })?
                .clone(),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                let arr: StringArray = vec![Some(s.as_str())].into_iter().collect();
                arr
            }
            _ => {
                return Err(DataFusionError::Execution(
                    "Third argument to remote_embed must be a Utf8 text column".to_string(),
                ));
            }
        };

        // Look up provider
        let provider = self.registry.providers.get(&provider_name).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Unknown remote_embed provider '{}'. Available: {:?}",
                provider_name,
                self.registry.providers.keys().collect::<Vec<_>>()
            ))
        })?;

        // Collect texts, tracking nulls
        let num_rows = text_array.len();
        let mut non_null_texts: Vec<&str> = Vec::with_capacity(num_rows);
        let mut null_mask: Vec<bool> = Vec::with_capacity(num_rows); // true = null

        for i in 0..num_rows {
            if text_array.is_null(i) {
                null_mask.push(true);
            } else {
                null_mask.push(false);
                non_null_texts.push(text_array.value(i));
            }
        }

        // Handle all-null input
        if non_null_texts.is_empty() {
            let embeddings: Vec<Option<Vec<f32>>> = vec![None; num_rows];
            return Ok(ColumnarValue::Array(vecs_to_list_array(embeddings)));
        }

        // Batch and call provider (async→sync bridge)
        let batch_limit = provider.batch_limit();
        let mut all_embeddings: Vec<Vec<f32>> = Vec::with_capacity(non_null_texts.len());

        // Use block_in_place so tokio moves async tasks off this thread while
        // we block. This is necessary because constant-folding may evaluate the
        // UDF directly on a tokio worker thread.
        let handle = tokio::runtime::Handle::current();
        tokio::task::block_in_place(|| {
            for chunk in non_null_texts.chunks(batch_limit) {
                let req = EmbeddingRequest {
                    model: &model_name,
                    texts: chunk.to_vec(),
                };
                let resp = handle.block_on(provider.embed(req)).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "remote_embed({}, {}) failed: {}",
                        provider_name, model_name, e
                    ))
                })?;
                if resp.embeddings.len() != chunk.len() {
                    return Err(DataFusionError::Execution(format!(
                        "remote_embed({}, {}): provider returned {} embeddings for {} texts",
                        provider_name,
                        model_name,
                        resp.embeddings.len(),
                        chunk.len()
                    )));
                }
                all_embeddings.extend(resp.embeddings);
            }
            Ok::<(), DataFusionError>(())
        })?;

        // Reassemble with null propagation
        let mut result: Vec<Option<Vec<f32>>> = Vec::with_capacity(num_rows);
        let mut embed_idx = 0;
        for is_null in &null_mask {
            if *is_null {
                result.push(None);
            } else {
                result.push(Some(all_embeddings[embed_idx].clone()));
                embed_idx += 1;
            }
        }

        Ok(ColumnarValue::Array(vecs_to_list_array(result)))
    }
}

/// Extract a string literal from a `ColumnarValue`.
fn extract_string_literal(val: &ColumnarValue, label: &str) -> Result<String, DataFusionError> {
    match val {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(s.clone()),
        ColumnarValue::Array(arr) => {
            let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                DataFusionError::Execution(format!("remote_embed {label} must be a string literal"))
            })?;
            if str_arr.is_empty() {
                return Err(DataFusionError::Execution(format!(
                    "remote_embed {label}: empty array"
                )));
            }
            Ok(str_arr.value(0).to_string())
        }
        _ => Err(DataFusionError::Execution(format!(
            "remote_embed {label} must be a string literal"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float32Array, ListArray};
    use arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;

    /// Build a `ScalarFunctionArgs` for testing.
    fn make_args(args: Vec<ColumnarValue>, num_rows: usize) -> ScalarFunctionArgs {
        let arg_fields = args
            .iter()
            .map(|a| {
                let dt = a.data_type();
                Arc::new(Field::new("_", dt, true))
            })
            .collect();
        ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: num_rows,
            return_field: Arc::new(Field::new("f", embedding_return_type(), true)),
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    #[test]
    fn registry_has_all_providers() {
        let reg = RemoteEmbedRegistry::new();
        assert!(reg.providers.contains_key("openai"));
        assert!(reg.providers.contains_key("voyage"));
        assert!(reg.providers.contains_key("mistral"));
        assert!(reg.providers.contains_key("gemini"));
    }

    #[test]
    fn udf_rejects_too_few_args() {
        let reg = Arc::new(RemoteEmbedRegistry::new());
        let udf = RemoteEmbedUDF::new(reg);
        let args = make_args(
            vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "openai".to_string(),
            )))],
            1,
        );
        let result = udf.invoke_with_args(args);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("3 arguments"));
    }

    #[test]
    fn udf_rejects_unknown_provider() {
        let reg = Arc::new(RemoteEmbedRegistry::new());
        let udf = RemoteEmbedUDF::new(reg);
        let text_arr: StringArray = vec![Some("hello")].into_iter().collect();
        let args = make_args(
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("unknown_provider".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("model".to_string()))),
                ColumnarValue::Array(Arc::new(text_arr)),
            ],
            1,
        );
        let result = udf.invoke_with_args(args);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("Unknown remote_embed provider"));
    }

    #[test]
    fn udf_null_propagation() {
        // All-null input should return all-null output without calling any provider
        let reg = Arc::new(RemoteEmbedRegistry::new());
        let udf = RemoteEmbedUDF::new(reg);
        let text_arr: StringArray = vec![None::<&str>, None].into_iter().collect();
        let args = make_args(
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("openai".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    "text-embedding-3-small".to_string(),
                ))),
                ColumnarValue::Array(Arc::new(text_arr)),
            ],
            2,
        );
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let list_arr = arr.as_any().downcast_ref::<ListArray>().unwrap();
                assert_eq!(list_arr.len(), 2);
                assert!(list_arr.is_null(0));
                assert!(list_arr.is_null(1));
            }
            _ => panic!("Expected array output"),
        }
    }

    #[test]
    fn vecs_to_list_array_roundtrip() {
        let input = vec![
            Some(vec![1.0f32, 2.0, 3.0]),
            None,
            Some(vec![4.0, 5.0, 6.0]),
        ];
        let arr = vecs_to_list_array(input);
        let list_arr = arr.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list_arr.len(), 3);
        assert!(!list_arr.is_null(0));
        assert!(list_arr.is_null(1));
        assert!(!list_arr.is_null(2));

        let first = list_arr.value(0);
        let first_vals = first.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(first_vals.values(), &[1.0, 2.0, 3.0]);
    }
}
