//! `llm_extract` scalar UDF — source-agnostic structured extraction over a
//! text column via cheap LLM chat models.
//!
//! Mirrors `remote_embed`'s mechanics: a registry holding a shared provider,
//! a `ScalarUDFImpl` returning a `List<Utf8>` per row (caller `UNNEST`s), an
//! async→sync bridge for outbound calls, and **multiple providers dispatched by
//! name**. No dependency on the `documents` connector.
//!
//! Extraction is an easy task, so the default providers are cheap
//! OpenAI-compatible chat models (DeepSeek / GLM / Gemini / OpenAI). A native
//! Anthropic provider is available behind the same trait but is optional and not
//! the default.

pub mod anthropic;
pub mod openai_compat;
pub mod provider;

use std::time::Duration;

use reqwest::Client;

use std::sync::Arc;

use arrow::array::{Array, ListBuilder, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Field};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use serde_json::json;

use self::provider::{CompletionProvider, CompletionRequest};

/// Default confidence threshold below which an entity is considered "weak".
pub const DEFAULT_THRESHOLD: f64 = 0.75;

// =============================================================================
// LlmExtractRegistry — holds the provider + threshold
// =============================================================================

/// Holds the shared completion provider and per-query knobs. Passed into the
/// UDF so it can run extractions per row.
pub struct LlmExtractRegistry {
    provider: Arc<dyn CompletionProvider>,
    threshold: f64,
    /// Per-query cap on multimodal escalation calls. `None` = unlimited. Read
    /// once at construction (`from_env`) — never re-read from the environment
    /// during query execution, so it can't race with a test mutating env vars.
    max_calls: Option<u32>,
    /// Whether the active model is vision-capable. When `false`, multimodal
    /// escalation is skipped (a text-only model would 400 on an image block) and
    /// weak entities stay `low_confidence` (spec §3 step 3). Determined once at
    /// `from_env` from the model id (heuristic) or the `LLM_EXTRACT_VISION`
    /// override.
    vision: bool,
}

impl std::fmt::Debug for LlmExtractRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LlmExtractRegistry")
            .field("threshold", &self.threshold)
            .field("max_calls", &self.max_calls)
            .field("vision", &self.vision)
            .finish()
    }
}

impl LlmExtractRegistry {
    /// Create a registry from an explicit provider + threshold, with no
    /// escalation-call cap and vision assumed capable.
    pub fn new(provider: Arc<dyn CompletionProvider>, threshold: f64) -> Self {
        Self::with_options(provider, threshold, None, true)
    }

    /// Create a registry with an explicit per-query escalation-call cap (vision
    /// assumed capable).
    pub fn with_max_calls(
        provider: Arc<dyn CompletionProvider>,
        threshold: f64,
        max_calls: Option<u32>,
    ) -> Self {
        Self::with_options(provider, threshold, max_calls, true)
    }

    /// Full constructor: provider, confidence threshold, escalation-call cap,
    /// and whether the active model is vision-capable.
    pub fn with_options(
        provider: Arc<dyn CompletionProvider>,
        threshold: f64,
        max_calls: Option<u32>,
        vision: bool,
    ) -> Self {
        Self {
            provider,
            threshold,
            max_calls,
            vision,
        }
    }

    /// Build a registry from the environment.
    ///
    /// Selects a completion provider via `LLM_EXTRACT_PROVIDER` (default
    /// `deepseek`), the model via `LLM_EXTRACT_MODEL`, and the confidence
    /// threshold via `LLM_EXTRACT_THRESHOLD` (default [`DEFAULT_THRESHOLD`]).
    ///
    /// The four built-in OpenAI-compatible providers — deepseek, glm, gemini,
    /// openai — are all constructed (each warning, not panicking, if its API-key
    /// env var is unset), and the named one is selected. `anthropic` is an
    /// optional native provider, not the default.
    pub fn from_env() -> Self {
        let threshold = std::env::var("LLM_EXTRACT_THRESHOLD")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(DEFAULT_THRESHOLD);
        // Per-query cost guard, read once here (not per-query in invoke).
        let max_calls = std::env::var("LLM_EXTRACT_MAX_CALLS")
            .ok()
            .and_then(|s| s.parse::<u32>().ok());
        let (provider, model) = build_provider_from_env();
        let vision = resolve_vision(&model);
        if !vision {
            tracing::info!(
                "llm_extract: model '{model}' is not vision-capable — multimodal \
                 escalation disabled (set LLM_EXTRACT_VISION=true to override)"
            );
        }
        Self::with_options(provider, threshold, max_calls, vision)
    }

    /// Register the `llm_extract` UDF with a DataFusion `SessionContext`.
    ///
    /// Usage: `llm_extract(text_col, image_ref_col, '{json schema}') -> List<Utf8>`
    pub fn register(self: &Arc<Self>, ctx: &mut SessionContext) {
        let udf = ScalarUDF::new_from_impl(LlmExtractUDF::new(Arc::clone(self)));
        ctx.register_udf(udf);
        tracing::info!("Registered 'llm_extract' UDF");
    }
}

const HTTP_TIMEOUT: Duration = Duration::from_secs(60);

/// Default chat-completion provider when `LLM_EXTRACT_PROVIDER` is unset.
const DEFAULT_PROVIDER: &str = "deepseek";

/// OpenAI-compatible provider table: `(name, base_url, api_key_env, default_model)`.
/// Default models are cheap chat models suited to schema-guided extraction.
const OPENAI_COMPAT_PROVIDERS: &[(&str, &str, &str, &str)] = &[
    (
        "deepseek",
        "https://api.deepseek.com/v1",
        "DEEPSEEK_API_KEY",
        "deepseek-chat",
    ),
    (
        "glm",
        "https://open.bigmodel.cn/api/paas/v4",
        "GLM_API_KEY",
        "glm-4-flash",
    ),
    (
        "gemini",
        "https://generativelanguage.googleapis.com/v1beta/openai",
        "GEMINI_API_KEY",
        "gemini-2.0-flash",
    ),
    (
        "openai",
        "https://api.openai.com/v1",
        "OPENAI_API_KEY",
        "gpt-4o-mini",
    ),
];

/// Resolve the active provider name from `LLM_EXTRACT_PROVIDER`, defaulting to
/// [`DEFAULT_PROVIDER`]. Lower-cased for matching.
fn active_provider_name() -> String {
    std::env::var("LLM_EXTRACT_PROVIDER")
        .ok()
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_PROVIDER.to_string())
}

/// Default model id for the optional Anthropic provider, used by the vision
/// heuristic when `LLM_EXTRACT_MODEL` is unset. Kept in sync with `anthropic.rs`.
const ANTHROPIC_DEFAULT_MODEL: &str = "claude-opus-4-8";

/// Build the selected completion provider from the environment, returning it
/// alongside the resolved model id (used to decide vision capability).
///
/// Constructs all four OpenAI-compatible providers — warning (not panicking) for
/// any whose API-key env var is unset, mirroring `remote_embed` — and returns
/// the one named by `LLM_EXTRACT_PROVIDER`. `anthropic` selects the optional
/// native provider. An unknown name falls back to the default with a warning.
fn build_provider_from_env() -> (Arc<dyn CompletionProvider>, String) {
    let selected = active_provider_name();

    // Eager warn for every OpenAI-compatible provider whose key is missing, so
    // misconfiguration is visible at startup (mirrors remote_embed).
    for (name, _url, env_var, _model) in OPENAI_COMPAT_PROVIDERS {
        if std::env::var(env_var).is_err() {
            tracing::warn!(
                "llm_extract provider '{}': {} not set — queries using this provider will fail",
                name,
                env_var
            );
        }
    }

    let model_override = std::env::var("LLM_EXTRACT_MODEL").ok();

    if selected == "anthropic" {
        let model = model_override
            .clone()
            .unwrap_or_else(|| ANTHROPIC_DEFAULT_MODEL.to_string());
        return (Arc::new(anthropic::AnthropicProvider::from_env()), model);
    }

    // Build the selected OpenAI-compatible provider, falling back to the default
    // on an unknown name. The resolved model id is the override, else the
    // table's default for the chosen provider.
    let resolved_name = if OPENAI_COMPAT_PROVIDERS.iter().any(|(n, ..)| *n == selected) {
        selected.as_str()
    } else {
        tracing::warn!(
            "llm_extract: unknown LLM_EXTRACT_PROVIDER '{}', falling back to '{}'",
            selected,
            DEFAULT_PROVIDER
        );
        DEFAULT_PROVIDER
    };

    let default_model = OPENAI_COMPAT_PROVIDERS
        .iter()
        .find(|(n, ..)| *n == resolved_name)
        .map(|(.., m)| *m)
        .expect("resolved provider must exist in the table");
    let model = model_override.unwrap_or_else(|| default_model.to_string());

    let provider = build_openai_compat(resolved_name, Some(&model))
        .expect("resolved provider must exist in the table");
    (Arc::new(provider), model)
}

/// Heuristic for whether a model id names a vision-capable model. Used at
/// `from_env` to decide whether multimodal escalation is safe; overridden by
/// `LLM_EXTRACT_VISION` (`true`/`false`/`1`/`0`).
fn model_is_vision_capable(model_id: &str) -> bool {
    let m = model_id.to_ascii_lowercase();
    // OpenAI 4o / o-series vision; Qwen/GLM "-vl"/"v" variants; Gemini flash/pro
    // (multimodal); Claude (vision-capable). Conservative: text-only ids like
    // `deepseek-chat`, `glm-4-flash`, `gpt-4o-mini`-less variants stay false.
    m.contains("4o")
        || m.contains("-vl")
        || m.contains("vl-")
        || m.contains("glm-4v")
        || m.contains("vision")
        || m.contains("gemini-")
        || m.contains("claude")
}

/// Resolve vision capability: explicit `LLM_EXTRACT_VISION` override wins,
/// otherwise the model-id heuristic.
fn resolve_vision(model_id: &str) -> bool {
    match std::env::var("LLM_EXTRACT_VISION")
        .ok()
        .map(|s| s.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("true") | Some("1") | Some("yes") => true,
        Some("false") | Some("0") | Some("no") => false,
        _ => model_is_vision_capable(model_id),
    }
}

/// Build one OpenAI-compatible provider by name, or `None` if the name isn't in
/// the provider table. `model_override` (from `LLM_EXTRACT_MODEL`) wins over the
/// table's default model. Deterministic and network-free — used in tests.
fn build_openai_compat(
    name: &str,
    model_override: Option<&str>,
) -> Option<openai_compat::OpenAiCompatibleCompletionProvider> {
    let (name, base_url, api_key_env, default_model) =
        OPENAI_COMPAT_PROVIDERS.iter().find(|(n, ..)| *n == name)?;

    let client = Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .expect("failed to build reqwest client");
    let model = model_override.unwrap_or(default_model);
    Some(openai_compat::OpenAiCompatibleCompletionProvider::new(
        name,
        base_url,
        api_key_env,
        client,
        model,
    ))
}

// =============================================================================
// LlmExtractUDF — ScalarUDFImpl
// =============================================================================

#[derive(Debug)]
pub struct LlmExtractUDF {
    registry: Arc<LlmExtractRegistry>,
    signature: Signature,
}

impl PartialEq for LlmExtractUDF {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.registry, &other.registry)
    }
}

impl Eq for LlmExtractUDF {}

impl std::hash::Hash for LlmExtractUDF {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.registry).hash(state);
    }
}

impl LlmExtractUDF {
    pub fn new(registry: Arc<LlmExtractRegistry>) -> Self {
        Self {
            registry,
            // `llm_extract` is non-deterministic (LLM output) → Volatile, so the
            // optimizer won't constant-fold or dedupe calls.
            signature: Signature::variadic_any(Volatility::Volatile),
        }
    }

    /// Run one provider completion synchronously (async→sync bridge mirroring
    /// `remote_embed`). `block_in_place` lets tokio move other tasks off this
    /// worker thread while we block.
    fn complete_blocking(
        &self,
        req: CompletionRequest<'_>,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let handle = tokio::runtime::Handle::current();
        let resp =
            tokio::task::block_in_place(|| handle.block_on(self.registry.provider.complete(req)))?;
        Ok(resp.entities)
    }

    /// Extract the entities for a single row: text-first pass, confidence gate,
    /// optional multimodal escalation, `_status` stamping, and never-drop error
    /// isolation.
    ///
    /// `escalation_budget` is the per-query cost guard: `Some(n)` allows at most
    /// `n` more multimodal escalation calls across the batch (decremented here),
    /// `None` means unlimited. When the budget is exhausted, weak rows keep their
    /// text-only entities and are stamped `low_confidence` rather than escalating.
    fn extract_row(
        &self,
        json_schema: &str,
        required: &[String],
        text: &str,
        image_ref: Option<&str>,
        escalation_budget: &mut Option<u32>,
    ) -> Vec<serde_json::Value> {
        // --- text-first pass ---
        let text_req = CompletionRequest {
            json_schema,
            text,
            image: None,
        };
        let mut entities = match self.complete_blocking(text_req) {
            Ok(e) => e,
            // Never drop: a provider/parse failure for the row yields a single
            // error entity; other rows are unaffected.
            Err(e) => {
                return vec![json!({
                    "_status": "error",
                    "_error": e.to_string(),
                })];
            }
        };

        // --- confidence gate ---
        let any_weak = entities
            .iter()
            .any(|e| is_weak(e, self.registry.threshold, required));

        // --- multimodal escalation ---
        // Escalate only if there's a weak entity, the active model is
        // vision-capable (a text-only model would 400 on an image block — keep
        // the entity low_confidence instead), we have an image to attach, and the
        // per-query escalation budget isn't exhausted.
        let budget_ok = !matches!(escalation_budget, Some(0));
        if any_weak && self.registry.vision && budget_ok {
            if let Some(reference) = image_ref {
                match fetch_image(reference) {
                    Ok(image) => {
                        // An escalation call is about to be made — spend one unit
                        // of the budget now. It stays spent even if the call
                        // below fails (intended: a failed escalation still
                        // consumed a model call / cost).
                        if let Some(n) = escalation_budget {
                            *n = n.saturating_sub(1);
                        }
                        let img_req = CompletionRequest {
                            json_schema,
                            text,
                            image: Some(image),
                        };
                        match self.complete_blocking(img_req) {
                            Ok(escalated) => {
                                // Escalated result replaces this row's entities.
                                entities = escalated;
                            }
                            Err(e) => {
                                return vec![json!({
                                    "_status": "error",
                                    "_error": format!("multimodal escalation failed: {e}"),
                                })];
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("llm_extract: could not fetch image '{reference}': {e}");
                        // Fall through: keep text-only entities, stamped below.
                    }
                }
            }
        }

        // --- stamp _status per entity ---
        for entity in &mut entities {
            stamp_status(entity, self.registry.threshold, required);
        }

        entities
    }
}

/// Whether an entity is "weak": confidence below threshold, or a required
/// schema field is missing/null.
fn is_weak(entity: &serde_json::Value, threshold: f64, required: &[String]) -> bool {
    // An explicit error entity is always weak.
    if entity.get("_status").and_then(|s| s.as_str()) == Some("error") {
        return true;
    }
    let conf = entity.get("_confidence").and_then(|c| c.as_f64());
    if let Some(c) = conf {
        if c < threshold {
            return true;
        }
    }
    // Missing required field (or present-but-null) → weak.
    required
        .iter()
        .any(|field| entity.get(field).map(|v| v.is_null()).unwrap_or(true))
}

/// Stamp `_status` on an entity in place, unless it's already an error entity.
fn stamp_status(entity: &mut serde_json::Value, threshold: f64, required: &[String]) {
    if let Some(obj) = entity.as_object_mut() {
        if obj.get("_status").and_then(|s| s.as_str()) == Some("error") {
            return;
        }
        let status = if is_weak(&serde_json::Value::Object(obj.clone()), threshold, required) {
            "low_confidence"
        } else {
            "ok"
        };
        obj.insert("_status".to_string(), json!(status));
    }
}

/// Parse the `required` field-name set from a JSON Schema string. Returns an
/// empty vec if absent or unparseable.
fn parse_required(json_schema: &str) -> Vec<String> {
    serde_json::from_str::<serde_json::Value>(json_schema)
        .ok()
        .and_then(|v| {
            v.get("required").and_then(|r| r.as_array()).map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            })
        })
        .unwrap_or_default()
}

/// Fetch an image referenced by `image_ref` and return it as base64 + mime.
///
/// Supports:
/// - `data:<mime>;base64,<payload>` — payload used directly (no network).
/// - `http://` / `https://` — fetched via a short-lived `reqwest` client.
/// - everything else — treated as a local filesystem path.
fn fetch_image(image_ref: &str) -> anyhow::Result<provider::ImageInput> {
    use anyhow::Context;

    if let Some(rest) = image_ref.strip_prefix("data:") {
        // data:<mime>;base64,<payload>
        let (meta, payload) = rest
            .split_once(',')
            .ok_or_else(|| anyhow::anyhow!("malformed data URI"))?;
        let mime = meta.strip_suffix(";base64").unwrap_or(meta).to_string();
        let mime = if mime.is_empty() {
            "image/png".to_string()
        } else {
            mime
        };
        return Ok(provider::ImageInput {
            base64: payload.to_string(),
            mime,
        });
    }

    use base64::Engine;
    let engine = base64::engine::general_purpose::STANDARD;

    if image_ref.starts_with("http://") || image_ref.starts_with("https://") {
        let handle = tokio::runtime::Handle::current();
        let (bytes, mime) = tokio::task::block_in_place(|| {
            handle.block_on(async {
                let client = reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(30))
                    .build()?;
                let resp = client.get(image_ref).send().await?.error_for_status()?;
                let mime = resp
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("image/png")
                    .to_string();
                let bytes = resp.bytes().await?;
                Ok::<_, anyhow::Error>((bytes.to_vec(), mime))
            })
        })?;
        return Ok(provider::ImageInput {
            base64: engine.encode(&bytes),
            mime,
        });
    }

    // Local file path.
    let path = image_ref.strip_prefix("file://").unwrap_or(image_ref);
    let bytes = std::fs::read(path).with_context(|| format!("reading image file '{path}'"))?;
    let mime = mime_from_path(path);
    Ok(provider::ImageInput {
        base64: engine.encode(&bytes),
        mime,
    })
}

/// Guess an image MIME type from a file extension.
fn mime_from_path(path: &str) -> String {
    let ext = path.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
    match ext.as_str() {
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        _ => "image/png",
    }
    .to_string()
}

impl ScalarUDFImpl for LlmExtractUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "llm_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(list_utf8_type())
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let num_rows = args.number_rows;
        let args = args.args;

        if args.len() != 3 {
            return Err(DataFusionError::Execution(
                "llm_extract requires 3 arguments: text, image_ref, json_schema".to_string(),
            ));
        }

        // --- arg 0: text column (Utf8 array, nullable) ---
        let text_array = to_string_array(&args[0], num_rows, "first argument (text)")?;

        // --- arg 1: image_ref column (Utf8 array, nullable) ---
        let image_array = to_string_array(&args[1], num_rows, "second argument (image_ref)")?;

        // --- arg 2: json_schema (Utf8 literal) ---
        let json_schema = extract_string_literal(&args[2], "third argument (json_schema)")?;

        // Parse the schema's `required` field set once per call.
        let required = parse_required(&json_schema);

        // Per-query cost guard: cap the number of multimodal escalation calls.
        // `None` = unlimited. Taken from the registry (read once at from_env),
        // not the live environment — avoids env races under parallel tests.
        let mut escalation_budget: Option<u32> = self.registry.max_calls;

        let n = text_array.len();
        let mut builder = ListBuilder::new(StringBuilder::new());

        for i in 0..n {
            // Empty/NULL text → empty list, no LLM call.
            if text_array.is_null(i) {
                builder.append(true);
                continue;
            }
            let text = text_array.value(i);
            if text.is_empty() {
                builder.append(true);
                continue;
            }

            let image_ref = if image_array.is_null(i) {
                None
            } else {
                Some(image_array.value(i))
            };

            let entities = self.extract_row(
                &json_schema,
                &required,
                text,
                image_ref,
                &mut escalation_budget,
            );
            for entity in entities {
                let s = serde_json::to_string(&entity).unwrap_or_else(|e| {
                    json!({"_status": "error", "_error": format!("serialize failed: {e}")})
                        .to_string()
                });
                builder.values().append_value(s);
            }
            builder.append(true);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Return type for `llm_extract`: `List<Utf8>`.
fn list_utf8_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
}

/// Coerce a `ColumnarValue` (array or scalar Utf8) into a `StringArray` of
/// `num_rows` rows. Used for the `text` and `image_ref` *columns*.
fn to_string_array(
    val: &ColumnarValue,
    num_rows: usize,
    label: &str,
) -> Result<StringArray, DataFusionError> {
    match val {
        ColumnarValue::Array(arr) => arr
            .as_any()
            .downcast_ref::<StringArray>()
            .cloned()
            .ok_or_else(|| {
                DataFusionError::Execution(format!("llm_extract {label} must be a Utf8 column"))
            }),
        ColumnarValue::Scalar(ScalarValue::Utf8(opt)) => {
            let v = opt.as_deref();
            let arr: StringArray = (0..num_rows.max(1)).map(|_| v).collect();
            Ok(arr)
        }
        ColumnarValue::Scalar(ScalarValue::Null) => {
            let arr: StringArray = (0..num_rows.max(1)).map(|_| None::<&str>).collect();
            Ok(arr)
        }
        _ => Err(DataFusionError::Execution(format!(
            "llm_extract {label} must be a Utf8 column"
        ))),
    }
}

/// Extract a string literal from a `ColumnarValue`. Rejects non-literal /
/// non-Utf8 values — the `json_schema` arg must be a string literal.
fn extract_string_literal(val: &ColumnarValue, label: &str) -> Result<String, DataFusionError> {
    match val {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(s.clone()),
        _ => Err(DataFusionError::Execution(format!(
            "llm_extract {label} must be a non-null string literal"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::llm_extract::provider::{CompletionRequest, CompletionResponse, ImageInput};
    use arrow::array::ListArray;
    use async_trait::async_trait;
    use datafusion::config::ConfigOptions;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Mock returning a fixed number of entities, counting calls.
    struct MockNEntities {
        n: usize,
        calls: AtomicUsize,
    }

    impl MockNEntities {
        fn new(n: usize) -> Self {
            Self {
                n,
                calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl CompletionProvider for MockNEntities {
        async fn complete(
            &self,
            _req: CompletionRequest<'_>,
        ) -> anyhow::Result<CompletionResponse> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let entities = (0..self.n)
                .map(|i| json!({"model": format!("m{i}"), "_confidence": 0.9}))
                .collect();
            Ok(CompletionResponse { entities })
        }
    }

    fn make_args(args: Vec<ColumnarValue>, num_rows: usize) -> ScalarFunctionArgs {
        let arg_fields = args
            .iter()
            .map(|a| Arc::new(Field::new("_", a.data_type(), true)))
            .collect();
        ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: num_rows,
            return_field: Arc::new(Field::new("f", list_utf8_type(), true)),
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    const SCHEMA: &str = r#"{"type":"object","properties":{"model":{"type":"string"}}}"#;

    fn schema_scalar() -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(SCHEMA.into())))
    }

    #[test]
    fn return_type_is_list_utf8() {
        let reg = Arc::new(LlmExtractRegistry::new(
            Arc::new(MockNEntities::new(1)),
            0.75,
        ));
        let udf = LlmExtractUDF::new(reg);
        let rt = udf.return_type(&[]).unwrap();
        assert_eq!(rt, list_utf8_type());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fan_out() {
        let reg = Arc::new(LlmExtractRegistry::new(
            Arc::new(MockNEntities::new(3)),
            0.75,
        ));
        let udf = LlmExtractUDF::new(reg);
        let text: StringArray = vec![Some("page body")].into_iter().collect();
        let img: StringArray = vec![None::<&str>].into_iter().collect();
        let args = make_args(
            vec![
                ColumnarValue::Array(Arc::new(text)),
                ColumnarValue::Array(Arc::new(img)),
                schema_scalar(),
            ],
            1,
        );
        let out = udf.invoke_with_args(args).unwrap();
        let ColumnarValue::Array(arr) = out else {
            panic!("expected array");
        };
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.value(0).len(), 3);

        // Each element is valid JSON containing the schema field "model".
        let elems = list.value(0);
        let strs = elems.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..strs.len() {
            let v: serde_json::Value = serde_json::from_str(strs.value(i)).unwrap();
            assert!(v.get("model").is_some());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn null_and_empty() {
        let mock = Arc::new(MockNEntities::new(1));
        let reg = Arc::new(LlmExtractRegistry::new(mock.clone(), 0.75));
        let udf = LlmExtractUDF::new(reg);
        let text: StringArray = vec![Some("x"), None, Some("")].into_iter().collect();
        let img: StringArray = vec![None::<&str>, None, None].into_iter().collect();
        let args = make_args(
            vec![
                ColumnarValue::Array(Arc::new(text)),
                ColumnarValue::Array(Arc::new(img)),
                schema_scalar(),
            ],
            3,
        );
        let out = udf.invoke_with_args(args).unwrap();
        let ColumnarValue::Array(arr) = out else {
            panic!("expected array");
        };
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list.value(0).len(), 1); // normal row
        assert_eq!(list.value(1).len(), 0); // null → empty list
        assert_eq!(list.value(2).len(), 0); // empty → empty list

        // Provider called exactly once (only for the normal row).
        assert_eq!(mock.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn arg_validation_arity() {
        let reg = Arc::new(LlmExtractRegistry::new(
            Arc::new(MockNEntities::new(1)),
            0.75,
        ));
        let udf = LlmExtractUDF::new(reg);
        let text: StringArray = vec![Some("x")].into_iter().collect();
        let args = make_args(
            vec![ColumnarValue::Array(Arc::new(text)), schema_scalar()],
            1,
        );
        let err = udf.invoke_with_args(args).unwrap_err().to_string();
        assert!(err.contains("3 arguments"), "got: {err}");
    }

    #[test]
    fn arg_validation_non_literal_schema() {
        let reg = Arc::new(LlmExtractRegistry::new(
            Arc::new(MockNEntities::new(1)),
            0.75,
        ));
        let udf = LlmExtractUDF::new(reg);
        let text: StringArray = vec![Some("x")].into_iter().collect();
        let img: StringArray = vec![None::<&str>].into_iter().collect();
        // json_schema passed as an array (column), not a literal.
        let schema_arr: StringArray = vec![Some(SCHEMA)].into_iter().collect();
        let args = make_args(
            vec![
                ColumnarValue::Array(Arc::new(text)),
                ColumnarValue::Array(Arc::new(img)),
                ColumnarValue::Array(Arc::new(schema_arr)),
            ],
            1,
        );
        let err = udf.invoke_with_args(args).unwrap_err().to_string();
        assert!(err.contains("string literal"), "got: {err}");
    }

    // ----- Task 3: confidence gate, escalation, never-drop -----

    /// Records each call's `image.is_some()`. First call returns a single weak
    /// (low-confidence) entity; subsequent (escalated) calls return a strong one.
    struct EscalatingMock {
        calls: Mutex<Vec<bool>>, // image.is_some() per call
    }

    impl EscalatingMock {
        fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl CompletionProvider for EscalatingMock {
        async fn complete(&self, req: CompletionRequest<'_>) -> anyhow::Result<CompletionResponse> {
            let had_image = req.image.is_some();
            self.calls.lock().unwrap().push(had_image);
            let entity = if had_image {
                json!({"model": "strong", "_confidence": 0.95})
            } else {
                json!({"model": "weak", "_confidence": 0.10})
            };
            Ok(CompletionResponse {
                entities: vec![entity],
            })
        }
    }

    /// Always returns an error.
    struct ErroringMock;

    #[async_trait]
    impl CompletionProvider for ErroringMock {
        async fn complete(
            &self,
            _req: CompletionRequest<'_>,
        ) -> anyhow::Result<CompletionResponse> {
            anyhow::bail!("boom")
        }
    }

    use std::sync::Mutex;

    fn first_entity(list: &ListArray, row: usize) -> serde_json::Value {
        let elems = list.value(row);
        let strs = elems.as_any().downcast_ref::<StringArray>().unwrap();
        serde_json::from_str(strs.value(0)).unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn escalates_with_image() {
        let mock = Arc::new(EscalatingMock::new());
        let reg = Arc::new(LlmExtractRegistry::new(mock.clone(), 0.75));
        let udf = LlmExtractUDF::new(reg);

        let text: StringArray = vec![Some("body")].into_iter().collect();
        // Use a data: URI so escalation needs no network.
        let img: StringArray = vec![Some("data:image/png;base64,aGVsbG8=")]
            .into_iter()
            .collect();
        let args = make_args(
            vec![
                ColumnarValue::Array(Arc::new(text)),
                ColumnarValue::Array(Arc::new(img)),
                schema_scalar(),
            ],
            1,
        );
        let out = udf.invoke_with_args(args).unwrap();
        let ColumnarValue::Array(arr) = out else {
            panic!()
        };
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();

        // Provider called twice; 2nd call had an image.
        let calls = mock.calls.lock().unwrap();
        assert_eq!(calls.len(), 2, "expected text + escalation call");
        assert!(!calls[0], "first call should be text-only");
        assert!(calls[1], "second call should carry the image");
        drop(calls);

        // Final entity is the escalated (strong) result, status ok.
        let e = first_entity(list, 0);
        assert_eq!(e["model"], "strong");
        assert_eq!(e["_status"], "ok");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_escalation_without_image() {
        let mock = Arc::new(EscalatingMock::new());
        let reg = Arc::new(LlmExtractRegistry::new(mock.clone(), 0.75));
        let udf = LlmExtractUDF::new(reg);

        let text: StringArray = vec![Some("body")].into_iter().collect();
        let img: StringArray = vec![None::<&str>].into_iter().collect();
        let args = make_args(
            vec![
                ColumnarValue::Array(Arc::new(text)),
                ColumnarValue::Array(Arc::new(img)),
                schema_scalar(),
            ],
            1,
        );
        let out = udf.invoke_with_args(args).unwrap();
        let ColumnarValue::Array(arr) = out else {
            panic!()
        };
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();

        // Provider called once (no escalation without an image).
        assert_eq!(mock.calls.lock().unwrap().len(), 1);

        let e = first_entity(list, 0);
        assert_eq!(e["model"], "weak");
        assert_eq!(e["_status"], "low_confidence");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_escalation_when_model_not_vision_capable() {
        // Weak entity + a real image_ref, but the active model is text-only
        // (vision = false). Escalation must be skipped: provider called exactly
        // once with NO image, and the entity stays low_confidence (spec §3 step3).
        let mock = Arc::new(EscalatingMock::new());
        let reg = Arc::new(LlmExtractRegistry::with_options(
            mock.clone(),
            0.75,
            None,
            false, // not vision-capable (e.g. deepseek-chat)
        ));
        let udf = LlmExtractUDF::new(reg);

        let text: StringArray = vec![Some("body")].into_iter().collect();
        let img: StringArray = vec![Some("data:image/png;base64,aGVsbG8=")]
            .into_iter()
            .collect();
        let args = make_args(
            vec![
                ColumnarValue::Array(Arc::new(text)),
                ColumnarValue::Array(Arc::new(img)),
                schema_scalar(),
            ],
            1,
        );
        let out = udf.invoke_with_args(args).unwrap();
        let ColumnarValue::Array(arr) = out else {
            panic!()
        };
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();

        // Exactly one call, and it carried no image (no escalation attempted).
        let calls = mock.calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "no escalation call for a text-only model");
        assert!(!calls[0], "the single call must not carry an image");
        drop(calls);

        let e = first_entity(list, 0);
        assert_eq!(e["model"], "weak");
        assert_eq!(e["_status"], "low_confidence");
    }

    #[test]
    fn vision_heuristic_classifies_models() {
        // Vision-capable ids.
        assert!(model_is_vision_capable("gpt-4o-mini"));
        assert!(model_is_vision_capable("glm-4v"));
        assert!(model_is_vision_capable("qwen2-vl-7b"));
        assert!(model_is_vision_capable("gemini-2.0-flash"));
        assert!(model_is_vision_capable("claude-opus-4-8"));
        // Text-only ids (the cheap defaults).
        assert!(!model_is_vision_capable("deepseek-chat"));
        assert!(!model_is_vision_capable("glm-4-flash"));
        assert!(!model_is_vision_capable("gpt-3.5-turbo"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn never_drop_on_error() {
        let reg = Arc::new(LlmExtractRegistry::new(Arc::new(ErroringMock), 0.75));
        let udf = LlmExtractUDF::new(reg);

        let text: StringArray = vec![Some("row0")].into_iter().collect();
        let img: StringArray = vec![None::<&str>].into_iter().collect();
        let args = make_args(
            vec![
                ColumnarValue::Array(Arc::new(text)),
                ColumnarValue::Array(Arc::new(img)),
                schema_scalar(),
            ],
            1,
        );
        let out = udf.invoke_with_args(args).unwrap();
        let ColumnarValue::Array(arr) = out else {
            panic!()
        };
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();

        // One-element error list for the failing row.
        assert_eq!(list.value(0).len(), 1);
        let e = first_entity(list, 0);
        assert_eq!(e["_status"], "error");
        assert!(e["_error"].as_str().unwrap().contains("boom"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn error_row_does_not_fail_other_rows() {
        // A normal row alongside an error row: a mock that errors only on a
        // specific text. We model this with a per-text switch.
        struct SelectiveMock;
        #[async_trait]
        impl CompletionProvider for SelectiveMock {
            async fn complete(
                &self,
                req: CompletionRequest<'_>,
            ) -> anyhow::Result<CompletionResponse> {
                if req.text == "bad" {
                    anyhow::bail!("boom");
                }
                Ok(CompletionResponse {
                    entities: vec![json!({"model": "ok", "_confidence": 0.99})],
                })
            }
        }
        let reg = Arc::new(LlmExtractRegistry::new(Arc::new(SelectiveMock), 0.75));
        let udf = LlmExtractUDF::new(reg);

        let text: StringArray = vec![Some("bad"), Some("good")].into_iter().collect();
        let img: StringArray = vec![None::<&str>, None].into_iter().collect();
        let args = make_args(
            vec![
                ColumnarValue::Array(Arc::new(text)),
                ColumnarValue::Array(Arc::new(img)),
                schema_scalar(),
            ],
            2,
        );
        let out = udf.invoke_with_args(args).unwrap();
        let ColumnarValue::Array(arr) = out else {
            panic!()
        };
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();

        let bad = first_entity(list, 0);
        assert_eq!(bad["_status"], "error");
        let good = first_entity(list, 1);
        assert_eq!(good["_status"], "ok");
    }

    #[test]
    fn parse_required_extracts_fields() {
        let schema = r#"{"type":"object","required":["a","b"],"properties":{}}"#;
        assert_eq!(
            parse_required(schema),
            vec!["a".to_string(), "b".to_string()]
        );
        assert!(parse_required("not json").is_empty());
        assert!(parse_required(r#"{"type":"object"}"#).is_empty());
    }

    #[test]
    fn weak_when_required_field_missing() {
        let required = vec!["price".to_string()];
        // High confidence but missing required field → still weak.
        let e = json!({"model": "x", "_confidence": 0.99});
        assert!(is_weak(&e, 0.75, &required));
        let e2 = json!({"model": "x", "price": 5, "_confidence": 0.99});
        assert!(!is_weak(&e2, 0.75, &required));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cost_guard_caps_escalations() {
        // max_calls = 1: a 2-weak-row batch (both with images) makes at most 1
        // escalation call; the un-escalated row stays low_confidence. The budget
        // is set on the registry directly — no process-env mutation, so this is
        // safe under parallel test runs.

        // Spy: counts escalation (image-bearing) calls.
        struct EscalationSpy {
            escalations: Mutex<usize>,
        }
        #[async_trait]
        impl CompletionProvider for EscalationSpy {
            async fn complete(
                &self,
                req: CompletionRequest<'_>,
            ) -> anyhow::Result<CompletionResponse> {
                if req.image.is_some() {
                    *self.escalations.lock().unwrap() += 1;
                    Ok(CompletionResponse {
                        entities: vec![json!({"model": "strong", "_confidence": 0.95})],
                    })
                } else {
                    // Text-first pass: always weak.
                    Ok(CompletionResponse {
                        entities: vec![json!({"model": "weak", "_confidence": 0.10})],
                    })
                }
            }
        }

        let spy = Arc::new(EscalationSpy {
            escalations: Mutex::new(0),
        });
        let reg = Arc::new(LlmExtractRegistry::with_max_calls(
            spy.clone(),
            0.75,
            Some(1),
        ));
        let udf = LlmExtractUDF::new(reg);

        let text: StringArray = vec![Some("row0"), Some("row1")].into_iter().collect();
        let img: StringArray = vec![
            Some("data:image/png;base64,aGVsbG8="),
            Some("data:image/png;base64,aGVsbG8="),
        ]
        .into_iter()
        .collect();
        let args = make_args(
            vec![
                ColumnarValue::Array(Arc::new(text)),
                ColumnarValue::Array(Arc::new(img)),
                schema_scalar(),
            ],
            2,
        );
        let out = udf.invoke_with_args(args).unwrap();
        let ColumnarValue::Array(arr) = out else {
            panic!()
        };
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();

        // At most one escalation despite two weak rows.
        assert_eq!(*spy.escalations.lock().unwrap(), 1);

        // One row escalated (ok/strong), the other stayed low_confidence/weak.
        let e0 = first_entity(list, 0);
        let e1 = first_entity(list, 1);
        let statuses = [
            e0["_status"].as_str().unwrap(),
            e1["_status"].as_str().unwrap(),
        ];
        assert!(
            statuses.contains(&"ok"),
            "one row should escalate: {statuses:?}"
        );
        assert!(
            statuses.contains(&"low_confidence"),
            "one row should stay low_confidence: {statuses:?}"
        );
    }

    // ----- Provider registry / selection (Task 4 multi-provider) -----

    #[test]
    fn provider_table_has_all_four() {
        let names: Vec<&str> = OPENAI_COMPAT_PROVIDERS.iter().map(|(n, ..)| *n).collect();
        assert!(names.contains(&"deepseek"));
        assert!(names.contains(&"glm"));
        assert!(names.contains(&"gemini"));
        assert!(names.contains(&"openai"));
        assert_eq!(names.len(), 4);
    }

    #[test]
    fn default_provider_is_deepseek() {
        assert_eq!(DEFAULT_PROVIDER, "deepseek");
        // build_openai_compat for the default resolves with the table's model.
        let p = build_openai_compat(DEFAULT_PROVIDER, None).unwrap();
        assert_eq!(p.name(), "deepseek");
    }

    #[test]
    fn build_openai_compat_selects_named_provider() {
        let glm = build_openai_compat("glm", None).unwrap();
        assert_eq!(glm.name(), "glm");
        let openai = build_openai_compat("openai", None).unwrap();
        assert_eq!(openai.name(), "openai");
        let gemini = build_openai_compat("gemini", None).unwrap();
        assert_eq!(gemini.name(), "gemini");
    }

    #[test]
    fn build_openai_compat_unknown_is_none() {
        assert!(build_openai_compat("nope", None).is_none());
        // anthropic is NOT in the OpenAI-compat table (it's the optional native one)
        assert!(build_openai_compat("anthropic", None).is_none());
    }

    #[test]
    fn build_openai_compat_with_model_override_resolves() {
        // An override is accepted; the provider still resolves by name.
        let p = build_openai_compat("deepseek", Some("custom-model")).unwrap();
        assert_eq!(p.name(), "deepseek");
    }

    // Keep ImageInput referenced.
    #[allow(dead_code)]
    fn _image_input_smoke() -> ImageInput {
        ImageInput {
            base64: String::new(),
            mime: String::new(),
        }
    }
}
