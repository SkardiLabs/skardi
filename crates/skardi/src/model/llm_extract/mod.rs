//! `llm_extract` scalar UDF — source-agnostic structured extraction over a
//! text column via an LLM completion provider (Anthropic).
//!
//! Mirrors `remote_embed`'s mechanics: a registry holding a shared provider,
//! a `ScalarUDFImpl` returning a `List<Utf8>` per row (caller `UNNEST`s), and
//! an async→sync bridge for outbound calls. No dependency on the `documents`
//! connector.

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

    /// Extract the entities for a single row. Returns the JSON `Value` entities
    /// for that row (may be empty). In Task 2 this is the plain text-first pass;
    /// the confidence gate / escalation / never-drop logic is layered in Task 3.
    fn extract_row(
        &self,
        model: &str,
        json_schema: &str,
        text: &str,
        image_ref: Option<&str>,
    ) -> Vec<serde_json::Value> {
        let _ = image_ref; // escalation wired in Task 3
        let req = CompletionRequest {
            model,
            json_schema,
            text,
            image: None,
        };

        let handle = tokio::runtime::Handle::current();
        let result =
            tokio::task::block_in_place(|| handle.block_on(self.registry.provider.complete(req)));

        match result {
            Ok(resp) => resp.entities,
            Err(e) => vec![json!({
                "_status": "error",
                "_error": e.to_string(),
            })],
        }
    }
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

            let entities = self.extract_row(&model, &json_schema, text, image_ref);
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

    // Keep ImageInput referenced so the import doesn't warn before Task 3.
    #[allow(dead_code)]
    fn _image_input_smoke() -> ImageInput {
        ImageInput {
            base64: String::new(),
            mime: String::new(),
        }
    }
}
