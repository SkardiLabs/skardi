//! `llm_extract` scalar UDF — source-agnostic structured extraction over a
//! text column via an LLM completion provider (Anthropic).
//!
//! Mirrors `remote_embed`'s mechanics: a registry holding a shared provider,
//! a `ScalarUDFImpl` returning a `List<Utf8>` per row (caller `UNNEST`s), and
//! an async→sync bridge for outbound calls. No dependency on the `documents`
//! connector.

pub mod anthropic;
pub mod provider;

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

/// Holds the shared completion provider and the confidence threshold. Passed
/// into the UDF so it can run extractions per row.
pub struct LlmExtractRegistry {
    provider: Arc<dyn CompletionProvider>,
    threshold: f64,
}

impl std::fmt::Debug for LlmExtractRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LlmExtractRegistry")
            .field("threshold", &self.threshold)
            .finish()
    }
}

impl LlmExtractRegistry {
    /// Create a registry from an explicit provider + threshold.
    pub fn new(provider: Arc<dyn CompletionProvider>, threshold: f64) -> Self {
        Self {
            provider,
            threshold,
        }
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
    fn extract_row(
        &self,
        model: &str,
        json_schema: &str,
        required: &[String],
        text: &str,
        image_ref: Option<&str>,
    ) -> Vec<serde_json::Value> {
        // --- text-first pass ---
        let text_req = CompletionRequest {
            model,
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
        // Escalate only if there's a weak entity AND we have an image to attach.
        if any_weak {
            if let Some(reference) = image_ref {
                match fetch_image(reference) {
                    Ok(image) => {
                        let img_req = CompletionRequest {
                            model,
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

        let model = std::env::var("LLM_EXTRACT_MODEL").unwrap_or_else(|_| default_model());

        // Parse the schema's `required` field set once per call.
        let required = parse_required(&json_schema);

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

            let entities = self.extract_row(&model, &json_schema, &required, text, image_ref);
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

/// Default Claude model id, overridable via `LLM_EXTRACT_MODEL`.
fn default_model() -> String {
    // Kept in sync with `anthropic.rs`.
    "claude-sonnet-4-5".to_string()
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

    // Keep ImageInput referenced.
    #[allow(dead_code)]
    fn _image_input_smoke() -> ImageInput {
        ImageInput {
            base64: String::new(),
            mime: String::new(),
        }
    }
}
