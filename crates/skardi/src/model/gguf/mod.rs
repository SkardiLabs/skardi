pub mod embed;

use anyhow::Result;
use arrow::array::{Array, StringArray};
use arrow::datatypes::DataType;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::{
    error::DataFusionError,
    logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility},
    prelude::SessionContext,
    scalar::ScalarValue,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use self::embed::GgufEmbeddingModel;
use super::{embedding_return_type, vecs_to_list_array};

// =============================================================================
// GgufModelRegistry — lazy model cache keyed by directory path
// =============================================================================

/// Lazily loads and caches `GgufEmbeddingModel` instances keyed by model
/// directory path.
pub struct GgufModelRegistry {
    models: RwLock<HashMap<String, Arc<GgufEmbeddingModel>>>,
}

impl std::fmt::Debug for GgufModelRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let models = self.models.read().unwrap();
        f.debug_struct("GgufModelRegistry")
            .field("loaded_models", &models.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl GgufModelRegistry {
    pub fn new() -> Self {
        Self {
            models: RwLock::new(HashMap::new()),
        }
    }

    /// Get or lazily load a `GgufEmbeddingModel` by model directory path.
    pub fn get_or_load(&self, model_path: &str) -> Result<Arc<GgufEmbeddingModel>> {
        // Fast path: already loaded (read lock only)
        {
            let models = self.models.read().unwrap();
            if let Some(model) = models.get(model_path) {
                return Ok(Arc::clone(model));
            }
        }

        // Slow path: acquire write lock, then double-check to avoid loading
        // the same model twice if another thread raced us.
        let mut models = self.models.write().unwrap();
        if let Some(model) = models.get(model_path) {
            return Ok(Arc::clone(model));
        }

        tracing::info!("Loading GGUF model from '{}'", model_path);
        let model = Arc::new(GgufEmbeddingModel::from_dir(model_path)?);
        tracing::info!("GGUF model '{}' loaded and cached", model_path);

        models.insert(model_path.to_string(), Arc::clone(&model));

        Ok(model)
    }

    /// Register the `gguf` UDF with a DataFusion `SessionContext`.
    ///
    /// Usage: `gguf('path/to/model_dir', text_col) -> List<Float32>`
    ///
    /// The first argument is the path to a model directory containing a single
    /// `.gguf` weights file. The model is loaded and cached on the first call.
    pub fn register_gguf_udf(self: &Arc<Self>, ctx: &mut SessionContext) {
        let udf = ScalarUDF::new_from_impl(GgufUDF::new(Arc::clone(self)));
        ctx.register_udf(udf);
        tracing::info!("Registered 'gguf' UDF");
    }
}

impl Default for GgufModelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// GgufUDF — ScalarUDFImpl
// =============================================================================

#[derive(Debug)]
struct GgufUDF {
    registry: Arc<GgufModelRegistry>,
    signature: Signature,
}

impl PartialEq for GgufUDF {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.registry, &other.registry)
    }
}

impl Eq for GgufUDF {}

impl std::hash::Hash for GgufUDF {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.registry).hash(state);
    }
}

impl GgufUDF {
    fn new(registry: Arc<GgufModelRegistry>) -> Self {
        Self {
            registry,
            // variadic_any for consistency with the ONNX UDF; arity and types
            // are validated in invoke_with_args (2 required + 1 optional bool).
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for GgufUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "gguf"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(embedding_return_type())
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let args = args.args;

        if args.len() < 2 {
            return Err(DataFusionError::Execution(
                "gguf requires at least 2 arguments: model_path and text column".to_string(),
            ));
        }

        // --- Extract model path (first arg, must be a scalar Utf8) ---
        let model_path = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(path))) => path.clone(),
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::Execution(
                        "First argument to gguf must be a string (model path)".to_string(),
                    )
                })?;
                if str_arr.is_empty() {
                    return Err(DataFusionError::Execution(
                        "Empty model path array".to_string(),
                    ));
                }
                if str_arr.is_null(0) {
                    return Err(DataFusionError::Execution(
                        "First argument to gguf (model path) must not be null".to_string(),
                    ));
                }
                str_arr.value(0).to_string()
            }
            _ => {
                return Err(DataFusionError::Execution(
                    "First argument to gguf must be a string literal (model path)".to_string(),
                ));
            }
        };

        // --- Extract text input (second arg) ---
        // Track which rows are null so we can propagate nulls in the output.
        let (texts, null_mask): (Vec<String>, Vec<bool>) = match &args[1] {
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::Execution(
                        "Second argument to gguf must be a text column (Utf8)".to_string(),
                    )
                })?;
                let texts = (0..str_arr.len())
                    .map(|i| {
                        if str_arr.is_null(i) {
                            String::new()
                        } else {
                            str_arr.value(i).to_string()
                        }
                    })
                    .collect();
                let nulls = (0..str_arr.len()).map(|i| str_arr.is_null(i)).collect();
                (texts, nulls)
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => (vec![s.clone()], vec![false]),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => (vec![String::new()], vec![true]),
            _ => {
                return Err(DataFusionError::Execution(
                    "Second argument to gguf must be a text column (Utf8)".to_string(),
                ));
            }
        };

        // --- Optional third argument: normalize (boolean, default true) ---
        let normalize = if args.len() >= 3 {
            match &args[2] {
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) => *b,
                ColumnarValue::Scalar(ScalarValue::Boolean(None)) => true,
                _ => {
                    return Err(DataFusionError::Execution(
                        "Third argument to gguf (normalize) must be a boolean literal".to_string(),
                    ));
                }
            }
        } else {
            true
        };

        // --- Load model (lazy, cached) ---
        let model = self.registry.get_or_load(&model_path).map_err(|e| {
            DataFusionError::Execution(format!("Failed to load GGUF model '{}': {}", model_path, e))
        })?;

        // --- Run embedding (only for non-null rows) ---
        let non_null_texts: Vec<&str> = texts
            .iter()
            .zip(&null_mask)
            .filter_map(|(s, is_null)| if !is_null { Some(s.as_str()) } else { None })
            .collect();

        let non_null_embeddings = model.embed_texts(&non_null_texts, normalize).map_err(|e| {
            tracing::error!("GGUF embedding failed for model '{}': {}", model_path, e);
            DataFusionError::Execution(format!("Embedding failed: {}", e))
        })?;

        // Reassemble results with nulls in the correct positions.
        let mut emb_iter = non_null_embeddings.into_iter();
        let embeddings: Vec<Option<Vec<f32>>> = null_mask
            .iter()
            .map(|&is_null| if is_null { None } else { emb_iter.next() })
            .collect();

        Ok(ColumnarValue::Array(vecs_to_list_array(embeddings)))
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float32Array, ListArray};
    use arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;
    use datafusion::execution::FunctionRegistry;

    // =========================================================================
    // vecs_to_list_array tests
    // =========================================================================

    #[test]
    fn vecs_to_list_array_shape_and_values() {
        let vecs = vec![Some(vec![1.0f32, 2.0, 3.0]), Some(vec![4.0, 5.0, 6.0])];
        let arr = vecs_to_list_array(vecs);

        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.len(), 2, "one list entry per input vector");

        let first = list
            .value(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(first, vec![1.0, 2.0, 3.0]);

        let second = list
            .value(1)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(second, vec![4.0, 5.0, 6.0]);
    }

    #[test]
    fn vecs_to_list_array_empty_input() {
        let arr = vecs_to_list_array(vec![]);
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.len(), 0);
    }

    #[test]
    fn vecs_to_list_array_variable_length_vectors() {
        let vecs = vec![Some(vec![1.0f32, 2.0]), Some(vec![3.0, 4.0, 5.0, 6.0])];
        let arr = vecs_to_list_array(vecs);
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list.value(0).len(), 2);
        assert_eq!(list.value(1).len(), 4);
    }

    #[test]
    fn vecs_to_list_array_null_entries() {
        let vecs = vec![Some(vec![1.0f32, 2.0]), None, Some(vec![3.0, 4.0])];
        let arr = vecs_to_list_array(vecs);
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.len(), 3);
        assert!(!list.is_null(0));
        assert!(list.is_null(1));
        assert!(!list.is_null(2));
    }

    // =========================================================================
    // GgufModelRegistry tests
    // =========================================================================

    #[test]
    fn registry_starts_empty() {
        let registry = GgufModelRegistry::new();
        let models = registry.models.read().unwrap();
        assert!(models.is_empty());
    }

    #[test]
    fn registry_get_or_load_errors_on_bad_path() {
        let registry = GgufModelRegistry::new();
        let result = registry.get_or_load("/nonexistent/model");
        assert!(result.is_err());
    }

    // =========================================================================
    // GgufUDF tests (ScalarUDFImpl contract)
    // =========================================================================

    #[test]
    fn udf_name_is_gguf() {
        let registry = Arc::new(GgufModelRegistry::new());
        let udf = GgufUDF::new(registry);
        assert_eq!(udf.name(), "gguf");
    }

    #[test]
    fn udf_return_type_is_list_float32() {
        let registry = Arc::new(GgufModelRegistry::new());
        let udf = GgufUDF::new(registry);
        let ret = udf.return_type(&[]).unwrap();
        assert_eq!(
            ret,
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true)))
        );
    }

    #[test]
    fn udf_errors_with_too_few_args() {
        let registry = Arc::new(GgufModelRegistry::new());
        let udf = GgufUDF::new(registry);
        let return_field = Arc::new(Field::new("item", DataType::Float32, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "path".to_string(),
            )))],
            arg_fields: vec![],
            number_rows: 1,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("at least 2 arguments")
        );
    }

    #[test]
    fn udf_registers_on_session_context() {
        let registry = Arc::new(GgufModelRegistry::new());
        let mut ctx = SessionContext::new();
        registry.register_gguf_udf(&mut ctx);

        // Verify the UDF is registered by checking it can be found.
        let udf = ctx.udf("gguf");
        assert!(udf.is_ok(), "gguf UDF should be registered");
    }
}
