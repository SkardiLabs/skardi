pub mod embed;

use anyhow::Result;
use arrow::array::{Array, StringArray};
use arrow::datatypes::{DataType, Field};
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::{
    error::DataFusionError,
    logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility},
    prelude::SessionContext,
    scalar::ScalarValue,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use self::embed::EmbeddingModel;
use super::vecs_to_list_array;

// =============================================================================
// CandleModelRegistry — lazy model cache keyed by file path
// =============================================================================

/// Lazily loads and caches `EmbeddingModel` instances keyed by weights file path.
pub struct CandleModelRegistry {
    models: RwLock<HashMap<String, Arc<EmbeddingModel>>>,
}

impl std::fmt::Debug for CandleModelRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let models = self.models.read().unwrap();
        f.debug_struct("CandleModelRegistry")
            .field("loaded_models", &models.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl CandleModelRegistry {
    pub fn new() -> Self {
        Self {
            models: RwLock::new(HashMap::new()),
        }
    }

    /// Get or lazily load an `EmbeddingModel` by model directory path.
    pub fn get_or_load(&self, model_path: &str) -> Result<Arc<EmbeddingModel>> {
        // Fast path: already loaded
        {
            let models = self.models.read().unwrap();
            if let Some(model) = models.get(model_path) {
                return Ok(Arc::clone(model));
            }
        }

        tracing::info!("Loading candle model from '{}'", model_path);
        let model = Arc::new(EmbeddingModel::from_dir(model_path)?);
        tracing::info!("Candle model '{}' loaded and cached", model_path);

        let mut models = self.models.write().unwrap();
        models.insert(model_path.to_string(), Arc::clone(&model));

        Ok(model)
    }

    /// Register the `candle` UDF with a DataFusion `SessionContext`.
    ///
    /// Usage: `candle('path/to/model_dir', text_col) -> List<Float32>`
    ///
    /// The first argument is the path to a model directory containing a single
    /// `.safetensors` weights file, `config.json`, and `tokenizer.json`. The
    /// model is loaded and cached on the first call.
    pub fn register_candle_udf(self: &Arc<Self>, ctx: &mut SessionContext) {
        let udf = ScalarUDF::new_from_impl(CandleUDF::new(Arc::clone(self)));
        ctx.register_udf(udf);
        tracing::info!("Registered 'candle' UDF");
    }
}

impl Default for CandleModelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// CandleUDF — ScalarUDFImpl
// =============================================================================

#[derive(Debug)]
struct CandleUDF {
    registry: Arc<CandleModelRegistry>,
    signature: Signature,
}

impl PartialEq for CandleUDF {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.registry, &other.registry)
    }
}

impl Eq for CandleUDF {}

impl std::hash::Hash for CandleUDF {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.registry).hash(state);
    }
}

impl CandleUDF {
    fn new(registry: Arc<CandleModelRegistry>) -> Self {
        Self {
            registry,
            // Variadic: model_path (Utf8) + one or more text columns
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CandleUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "candle"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Float32,
            true,
        ))))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let args = args.args;

        if args.len() < 2 {
            return Err(DataFusionError::Execution(
                "candle requires at least 2 arguments: model_path and text column".to_string(),
            ));
        }

        // --- Extract model path (first arg, must be a scalar Utf8) ---
        let model_path = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(path))) => path.clone(),
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::Execution(
                        "First argument to candle must be a string (model path)".to_string(),
                    )
                })?;
                if str_arr.is_empty() {
                    return Err(DataFusionError::Execution(
                        "Empty model path array".to_string(),
                    ));
                }
                if str_arr.is_null(0) {
                    return Err(DataFusionError::Execution(
                        "First argument to candle (model path) must not be null".to_string(),
                    ));
                }
                str_arr.value(0).to_string()
            }
            _ => {
                return Err(DataFusionError::Execution(
                    "First argument to candle must be a string literal (model path)".to_string(),
                ));
            }
        };

        // --- Extract text input (second arg) ---
        // For embedding mode: candle(model_path, text_col)
        // The text column is the second argument.
        let texts: Vec<String> = match &args[1] {
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::Execution(
                        "Second argument to candle must be a text column (Utf8)".to_string(),
                    )
                })?;
                (0..str_arr.len())
                    .map(|i| {
                        if str_arr.is_null(i) {
                            String::new()
                        } else {
                            str_arr.value(i).to_string()
                        }
                    })
                    .collect()
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => vec![s.clone()],
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => vec![String::new()],
            _ => {
                return Err(DataFusionError::Execution(
                    "Second argument to candle must be a text column (Utf8)".to_string(),
                ));
            }
        };

        // --- Optional third argument: normalize (boolean, default true) ---
        // normalize=true  → L2-unit-norm vectors; correct for cosine-similarity
        //                   search (lance_knn) and models trained with cosine loss.
        // normalize=false → raw mean-pooled vectors; use for dot-product search
        //                   or when vector magnitude carries meaning.
        let normalize = if args.len() >= 3 {
            match &args[2] {
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) => *b,
                ColumnarValue::Scalar(ScalarValue::Boolean(None)) => true,
                _ => {
                    return Err(DataFusionError::Execution(
                        "Third argument to candle (normalize) must be a boolean literal"
                            .to_string(),
                    ));
                }
            }
        } else {
            true
        };

        // --- Load model (lazy, cached) ---
        let model = self.registry.get_or_load(&model_path).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to load candle model '{}': {}",
                model_path, e
            ))
        })?;

        // --- Run embedding ---
        let text_refs: Vec<&str> = texts.iter().map(|s: &String| s.as_str()).collect();
        let embeddings = model.embed_texts(&text_refs, normalize).map_err(|e| {
            tracing::error!("Candle embedding failed for model '{}': {}", model_path, e);
            DataFusionError::Execution(format!("Embedding failed: {}", e))
        })?;

        let embeddings = embeddings.into_iter().map(Some).collect();
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
}
