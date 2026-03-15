use anyhow::{Result, anyhow};
use arrow::{
    array::{Array, ArrayRef, Float32Array, Float32Builder, ListArray, ListBuilder},
    datatypes::{DataType, Field},
};
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility},
    prelude::SessionContext,
};
use ndarray::ArrayD;
use ort::{
    logging::LogLevel,
    session::{Session, builder::GraphOptimizationLevel},
    value::{Tensor, TensorElementType, ValueType},
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use crate::converter::{arrayref_slice_to_ndarray, columnar_values_to_arrayrefs};
use crate::model::Model;

/// Metadata about a single model input, introspected from the ONNX file.
#[derive(Debug, Clone)]
pub struct InputMeta {
    pub name: String,
    pub elem_type: TensorElementType,
    /// Shape dimensions. -1 means dynamic (e.g., batch dimension).
    pub shape: Vec<i64>,
}

/// Metadata about a single model output, introspected from the ONNX file.
#[derive(Debug, Clone)]
pub struct OutputMeta {
    pub name: String,
    pub elem_type: TensorElementType,
    pub shape: Vec<i64>,
}

#[derive(Debug)]
pub struct OnnxModel {
    session: RwLock<Session>,
    pub inputs: Vec<InputMeta>,
    pub outputs: Vec<OutputMeta>,
}

impl OnnxModel {
    /// Load an ONNX model and introspect its input/output metadata.
    /// No need to manually specify types or shapes — they are read from the model.
    pub fn new(model_path: &str) -> Result<Self> {
        let session = Session::builder()
            .map_err(|e| anyhow!("{}", e))?
            .with_optimization_level(GraphOptimizationLevel::Level3)
            .map_err(|e| anyhow!("{}", e))?
            .with_intra_threads(num_cpus::get())
            .map_err(|e| anyhow!("{}", e))?
            .commit_from_file(model_path)
            .map_err(|e| anyhow!("{}", e))?;

        // Introspect inputs from the ONNX model
        let inputs: Vec<InputMeta> = session
            .inputs()
            .iter()
            .map(|outlet| {
                let (elem_type, shape) = extract_type_and_shape(outlet.dtype());
                tracing::debug!(
                    "Model input '{}': type={:?}, shape={:?}",
                    outlet.name(),
                    elem_type,
                    shape
                );
                InputMeta {
                    name: outlet.name().to_string(),
                    elem_type,
                    shape,
                }
            })
            .collect();

        // Introspect outputs from the ONNX model
        let outputs: Vec<OutputMeta> = session
            .outputs()
            .iter()
            .map(|outlet| {
                let (elem_type, shape) = extract_type_and_shape(outlet.dtype());
                tracing::debug!(
                    "Model output '{}': type={:?}, shape={:?}",
                    outlet.name(),
                    elem_type,
                    shape
                );
                OutputMeta {
                    name: outlet.name().to_string(),
                    elem_type,
                    shape,
                }
            })
            .collect();

        Ok(OnnxModel {
            session: RwLock::new(session),
            inputs,
            outputs,
        })
    }

    /// Whether all model inputs expect integer types (e.g., NCF-style ID models).
    pub fn is_integer_input_model(&self) -> bool {
        self.inputs.iter().all(|i| {
            matches!(
                i.elem_type,
                TensorElementType::Int32 | TensorElementType::Int64
            )
        })
    }

    /// Convert Arrow ArrayRefs into separate i64 2D tensors for integer-input models.
    fn arrayref_slice_to_multiple_ndarray_i64(
        &self,
        args: &[ArrayRef],
    ) -> Result<Vec<ndarray::Array2<i64>>> {
        use arrow::array::{Int32Array, Int64Array};

        if args.is_empty() {
            return Err(anyhow!("No input array provided for conversion"));
        }

        let batch_size = args[0].len();
        let mut tensors = Vec::new();

        for (idx, array) in args.iter().enumerate() {
            let mut tensor = ndarray::Array2::<i64>::zeros((batch_size, 1));

            if let Some(i64_arr) = array.as_any().downcast_ref::<Int64Array>() {
                for i in 0..batch_size {
                    tensor[[i, 0]] = i64_arr.value(i);
                }
            } else if let Some(i32_arr) = array.as_any().downcast_ref::<Int32Array>() {
                for i in 0..batch_size {
                    tensor[[i, 0]] = i32_arr.value(i) as i64;
                }
            } else {
                return Err(anyhow!(
                    "Unsupported data type for integer conversion at input {}: {:?}",
                    idx,
                    array.data_type()
                ));
            }

            tensors.push(tensor);
        }

        Ok(tensors)
    }

    /// Run inference on integer inputs, processing row-by-row for models that only support batch_size=1.
    fn predict_integer_inputs(&self, args: &[ArrayRef]) -> Result<Float32Array> {
        let input_tensors = self.arrayref_slice_to_multiple_ndarray_i64(args)?;
        let batch_size = input_tensors[0].shape()[0];

        if batch_size == 0 {
            return Ok(Float32Array::from(Vec::<f32>::new()));
        }

        let mut binding = self
            .session
            .write()
            .map_err(|e| anyhow!("Failed to acquire write lock: {}", e))?;

        // Process rows individually (many ONNX models only support batch_size=1)
        let mut all_predictions = Vec::with_capacity(batch_size);

        for row_idx in 0..batch_size {
            let row_tensors: Result<Vec<Tensor<i64>>> = input_tensors
                .iter()
                .map(|tensor| {
                    let row_data = vec![tensor[[row_idx, 0]]];
                    let row_array = ndarray::Array2::from_shape_vec((1, 1), row_data)
                        .map_err(|e| anyhow!("Failed to create row array: {}", e))?;
                    Tensor::from_array(row_array)
                        .map_err(|e| anyhow!("Failed to create tensor: {}", e))
                })
                .collect();
            let row_tensors = row_tensors?;

            // Build dynamic input list using model's introspected input names
            let ort_inputs: Vec<(&str, _)> = self
                .inputs
                .iter()
                .zip(row_tensors.iter())
                .map(|(meta, tensor)| (meta.name.as_str(), tensor.view().into_dyn()))
                .collect();

            let row_output = binding
                .run(ort_inputs)
                .map_err(|e| anyhow!("ORT run failed at row {}: {}", row_idx, e))?;

            let (_, pred_data) = row_output[0]
                .try_extract_tensor::<f32>()
                .map_err(|e| anyhow!("Cannot extract prediction: {}", e))?;
            all_predictions.push(pred_data[0]);
        }

        let mut builder = Float32Builder::new();
        for v in all_predictions {
            builder.append_value(v);
        }
        Ok(builder.finish())
    }

    /// Run inference on float inputs (time-series, embedding models).
    fn predict_float_inputs(&self, args: &[ArrayRef]) -> Result<Float32Array> {
        let input_nd: ArrayD<f32> = arrayref_slice_to_ndarray(args)?;
        let shape: Vec<i64> = input_nd.shape().iter().map(|&d| d as i64).collect();
        let data: Vec<f32> = input_nd.into_iter().collect();
        let input_tensor = Tensor::from_array((shape, data))
            .map_err(|e| anyhow!("failed to create tensor: {e}"))?;

        let mut binding = self
            .session
            .write()
            .map_err(|e| anyhow!("Failed to acquire write lock: {}", e))?;

        let outputs = binding
            .run(ort::inputs![input_tensor])
            .map_err(|e| anyhow!("ORT run failed: {}", e))?;

        self.extract_float_output(&outputs)
    }

    /// Extract Float32Array from ONNX output tensors.
    fn extract_float_output(&self, outputs: &ort::session::SessionOutputs) -> Result<Float32Array> {
        if outputs.len() == 0 {
            return Err(anyhow!("Model returned no outputs"));
        }

        let (shape, data) = outputs[0]
            .try_extract_tensor::<f32>()
            .map_err(|e| anyhow!("cannot extract tensor: {e}"))?;

        let vals: Vec<f32> = if shape.len() <= 3 {
            data.to_vec()
        } else {
            return Err(anyhow!("Unexpected output shape {:?}", shape));
        };

        let mut builder = Float32Builder::new();
        for v in vals {
            builder.append_value(v);
        }
        Ok(builder.finish())
    }
}

impl Model for OnnxModel {
    fn predict(&self, args: &[ArrayRef]) -> Result<Float32Array> {
        if self.is_integer_input_model() {
            self.predict_integer_inputs(args)
        } else {
            self.predict_float_inputs(args)
        }
    }
}

/// Extract element type and shape from an ort ValueType.
fn extract_type_and_shape(value_type: &ValueType) -> (TensorElementType, Vec<i64>) {
    match value_type {
        ValueType::Tensor { ty, shape, .. } => {
            let dims: Vec<i64> = shape.iter().copied().collect();
            (*ty, dims)
        }
        _ => (TensorElementType::Float32, vec![]),
    }
}

// =============================================================================
// OnnxModelRegistry — lazy model cache keyed by file path
// =============================================================================

/// A cache that lazily loads OnnxModel instances on first use, keyed by file path.
/// ORT runtime is only initialized when the first model is actually loaded.
/// No pre-registration needed — just pass a model path in SQL and it loads on demand.
pub struct OnnxModelRegistry {
    /// Models that have been loaded into memory, keyed by file path.
    models: RwLock<HashMap<String, Arc<OnnxModel>>>,
    /// Whether ORT runtime has been initialized.
    ort_initialized: AtomicBool,
}

impl std::fmt::Debug for OnnxModelRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let models = self.models.read().unwrap();
        f.debug_struct("OnnxModelRegistry")
            .field("loaded_models", &models.keys().collect::<Vec<_>>())
            .field(
                "ort_initialized",
                &self.ort_initialized.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl OnnxModelRegistry {
    pub fn new() -> Self {
        Self {
            models: RwLock::new(HashMap::new()),
            ort_initialized: AtomicBool::new(false),
        }
    }

    /// Get or lazily load a model by file path. Initializes ORT on first call.
    pub fn get_or_load(&self, model_path: &str) -> Result<Arc<OnnxModel>> {
        // Fast path: already loaded
        {
            let models = self.models.read().unwrap();
            if let Some(model) = models.get(model_path) {
                return Ok(Arc::clone(model));
            }
        }

        // Initialize ORT runtime once
        if !self.ort_initialized.swap(true, Ordering::SeqCst) {
            tracing::debug!("Initializing ORT runtime (lazy, first model load)");
            let _ = ort::init()
                .with_logger(Arc::new(|level, category, id, location, message| {
                    if level >= LogLevel::Warning {
                        tracing::warn!("[ort] [{}] [{}] ({}) {}", category, id, location, message);
                    }
                }))
                .commit();
        }

        let model = Arc::new(OnnxModel::new(model_path)?);

        tracing::debug!(
            "Model '{}' loaded: {} input(s), {} output(s)",
            model_path,
            model.inputs.len(),
            model.outputs.len()
        );

        let mut models = self.models.write().unwrap();
        models.insert(model_path.to_string(), Arc::clone(&model));

        Ok(model)
    }

    /// Register the generic `onnx_predict` UDF with a DataFusion SessionContext.
    ///
    /// Usage: `onnx_predict('path/to/model.onnx', input1, input2, ...) -> FLOAT`
    ///
    /// The first argument is a file path to an ONNX model (loaded and cached on first call).
    /// Remaining arguments are the model inputs.
    pub fn register_onnx_predict_udf(self: &Arc<Self>, ctx: &mut SessionContext) {
        let udf = ScalarUDF::new_from_impl(OnnxPredictUDF::new(Arc::clone(self)));
        ctx.register_udf(udf);
        tracing::info!("Registered 'onnx_predict' UDF");
    }
}

impl Default for OnnxModelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// OnnxPredictUDF — ScalarUDFImpl with variadic signature
// =============================================================================

#[derive(Debug)]
struct OnnxPredictUDF {
    registry: Arc<OnnxModelRegistry>,
    signature: Signature,
}

impl PartialEq for OnnxPredictUDF {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.registry, &other.registry)
    }
}

impl Eq for OnnxPredictUDF {}

impl std::hash::Hash for OnnxPredictUDF {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.registry).hash(state);
    }
}

impl OnnxPredictUDF {
    fn new(registry: Arc<OnnxModelRegistry>) -> Self {
        Self {
            registry,
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for OnnxPredictUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "onnx_predict"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        // arg_types[0] is the model path (Utf8), arg_types[1..] are model inputs.
        // If any model input is a List (e.g. from array_agg), the model produces a
        // sequence output — wrap it in List(Float32) so DataFusion gets 1 output row.
        let model_input_types = &arg_types[1..];
        if model_input_types
            .iter()
            .any(|t| matches!(t, DataType::List(_)))
        {
            Ok(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Float32,
                true,
            ))))
        } else {
            Ok(DataType::Float32)
        }
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        use arrow::array::StringArray;
        use datafusion::error::DataFusionError;

        let args = args.args;

        if args.len() < 2 {
            return Err(DataFusionError::Execution(
                "onnx_predict requires at least 2 arguments: model_path and inputs".to_string(),
            ));
        }

        // Extract model path from first argument
        let model_path = match &args[0] {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(path))) => {
                path.clone()
            }
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::Execution(
                        "First argument to onnx_predict must be a string (model path)".to_string(),
                    )
                })?;
                if str_arr.is_empty() {
                    return Err(DataFusionError::Execution(
                        "Empty model path array".to_string(),
                    ));
                }
                str_arr.value(0).to_string()
            }
            _ => {
                return Err(DataFusionError::Execution(
                    "First argument to onnx_predict must be a string (model path)".to_string(),
                ));
            }
        };

        // Load model (lazy, cached by path)
        let model = self.registry.get_or_load(&model_path).map_err(|e| {
            DataFusionError::Execution(format!("Failed to load model '{}': {}", model_path, e))
        })?;

        // Detect aggregated-list mode: any model arg is a ListArray (came from array_agg).
        // In this mode the model produces a sequence output that must be returned as
        // List(Float32) so DataFusion sees 1 output row instead of N prediction values.
        let model_args = &args[1..];
        let is_list_mode = model_args.iter().any(|arg| {
            matches!(arg, ColumnarValue::Array(a) if a.as_any().downcast_ref::<ListArray>().is_some())
        });

        let arrs = columnar_values_to_arrayrefs(model_args)
            .map_err(|e| DataFusionError::Execution(format!("Input conversion failed: {}", e)))?;

        // Run prediction
        let result_arr = model.predict(&arrs).map_err(|e| {
            tracing::error!("ONNX prediction failed for model '{}': {}", model_path, e);
            DataFusionError::Execution(format!("Prediction failed: {}", e))
        })?;

        if is_list_mode {
            // Wrap the flat Float32Array in a ListArray with a single element so the
            // output cardinality (1 row) matches the aggregated input cardinality.
            let mut builder = ListBuilder::new(Float32Builder::new());
            for v in result_arr.values() {
                builder.values().append_value(*v);
            }
            builder.append(true);
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        } else {
            Ok(ColumnarValue::Array(Arc::new(result_arr)))
        }
    }
}
