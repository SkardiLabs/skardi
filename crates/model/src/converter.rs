use anyhow::{Result, anyhow};
use arrow::array::{
    Array, ArrayRef, Float32Array, Float32Builder, Float64Array, Int32Array, Int32Builder,
    Int64Array, Int64Builder, ListArray,
};
use datafusion::{logical_expr::ColumnarValue, scalar::ScalarValue};
use ndarray::ArrayD;
use std::sync::Arc;

/// Convert a slice of Arrow ArrayRefs into a dynamic ndarray of f32.
///
/// This function handles two cases:
/// 1. Multiple scalar arrays (Float32/Float64): Creates 3D array [batch_size, sequence_length, num_features]
///    - Used for time series models with multiple feature columns
/// 2. Single ListArray: Creates 2D array [num_rows, embedding_dim]
///    - Used for embedding-based models where each row contains a vector
pub fn arrayref_slice_to_ndarray(args: &[ArrayRef]) -> Result<ArrayD<f32>> {
    if args.is_empty() {
        return Err(anyhow!("No input array provided for conversion"));
    }

    // Check if the first (and potentially only) array is a ListArray
    if args.len() == 1 {
        if let Some(_list_arr) = args[0].as_any().downcast_ref::<ListArray>() {
            // Use specialized converter for ListArray (embeddings)
            return list_arrayref_to_ndarray(args);
        }
    }

    // Otherwise, handle as scalar arrays (time series case)
    let num_features = args.len();
    let sequence_length = args[0].len();
    let batch_size = 1; // Process one sequence at a time

    // Create 3D array with shape [batch_size, sequence_length, num_features]
    let mut tensor = ndarray::Array3::<f32>::zeros((batch_size, sequence_length, num_features));

    for (feature_idx, array) in args.iter().enumerate() {
        if let Some(f32_arr) = array.as_any().downcast_ref::<Float32Array>() {
            for i in 0..sequence_length {
                tensor[[0, i, feature_idx]] = f32_arr.value(i);
            }
        } else if let Some(f64_arr) = array.as_any().downcast_ref::<Float64Array>() {
            for i in 0..sequence_length {
                tensor[[0, i, feature_idx]] = f64_arr.value(i) as f32;
            }
        } else if let Some(i64_arr) = array.as_any().downcast_ref::<Int64Array>() {
            for i in 0..sequence_length {
                tensor[[0, i, feature_idx]] = i64_arr.value(i) as f32;
            }
        } else if let Some(i32_arr) = array.as_any().downcast_ref::<Int32Array>() {
            for i in 0..sequence_length {
                tensor[[0, i, feature_idx]] = i32_arr.value(i) as f32;
            }
        } else {
            return Err(anyhow!(
                "Unsupported data type for column {} : {:?}",
                feature_idx,
                array.data_type()
            ));
        }
    }
    Ok(tensor.into_dyn())
}

/// Convert a slice of DataFusion ColumnarValue into a Vec<ArrayRef>,
/// expanding scalars to full-length arrays of floats.
///
/// This function supports two modes:
/// 1. Aggregated mode: Single ListArray element containing multiple values (for time series)
/// 2. Batched mode: Multiple ListArray elements, each containing a vector (for embeddings)
pub fn columnar_values_to_arrayrefs(args: &[ColumnarValue]) -> Result<Vec<ArrayRef>> {
    if args.is_empty() {
        return Err(anyhow!("No inputs provided for ColumnarValue conversion"));
    }

    // Determine row count from the first non-scalar argument
    // If all are scalars, row_count is 1
    let row_count = args
        .iter()
        .find_map(|arg| match arg {
            ColumnarValue::Array(a) => {
                if let Some(list_arr) = a.as_any().downcast_ref::<ListArray>() {
                    // For ListArray with single element (aggregated), use inner array length
                    // For ListArray with multiple elements (batched), use the list array length
                    if list_arr.len() == 1 {
                        Some(list_arr.value(0).len())
                    } else {
                        // Batched mode: multiple rows, each with a list/vector
                        Some(list_arr.len())
                    }
                } else {
                    Some(a.len())
                }
            }
            ColumnarValue::Scalar(_) => None,
        })
        .unwrap_or(1);

    let mut arrs: Vec<ArrayRef> = Vec::with_capacity(args.len());
    for cv in args {
        let array_ref = match cv {
            ColumnarValue::Array(a) => {
                if let Some(list_arr) = a.as_any().downcast_ref::<ListArray>() {
                    // Handle ListArray case
                    if list_arr.len() == 1 {
                        // Aggregated mode: extract the single inner array
                        list_arr.value(0)
                    } else {
                        // Batched mode: keep the ListArray as-is for later processing
                        a.clone()
                    }
                } else {
                    // Handle regular array case
                    a.clone()
                }
            }
            ColumnarValue::Scalar(s) => {
                // Expand scalar to ArrayRef of length row_count, preserving type
                match s {
                    ScalarValue::Float32(Some(v)) => {
                        let mut builder = Float32Builder::new();
                        builder.append_value_n(*v, row_count);
                        Arc::new(builder.finish()) as ArrayRef
                    }
                    ScalarValue::Float64(Some(v)) => {
                        let mut builder = Float32Builder::new();
                        builder.append_value_n(*v as f32, row_count);
                        Arc::new(builder.finish()) as ArrayRef
                    }
                    ScalarValue::Int64(Some(v)) => {
                        let mut builder = Int64Builder::new();
                        builder.append_value_n(*v, row_count);
                        Arc::new(builder.finish()) as ArrayRef
                    }
                    ScalarValue::Int32(Some(v)) => {
                        let mut builder = Int32Builder::new();
                        builder.append_value_n(*v, row_count);
                        Arc::new(builder.finish()) as ArrayRef
                    }
                    ScalarValue::UInt64(Some(v)) => {
                        let mut builder = Int64Builder::new();
                        builder.append_value_n(*v as i64, row_count);
                        Arc::new(builder.finish()) as ArrayRef
                    }
                    ScalarValue::UInt32(Some(v)) => {
                        let mut builder = Int32Builder::new();
                        builder.append_value_n(*v as i32, row_count);
                        Arc::new(builder.finish()) as ArrayRef
                    }
                    _ => {
                        return Err(anyhow!(
                            "Unsupported scalar type for ColumnarValue: {:?}",
                            s
                        ));
                    }
                }
            }
        };
        arrs.push(array_ref);
    }
    Ok(arrs)
}

/// Convert a slice of ArrayRefs containing ListArrays into a 2D ndarray
/// where each row is one embedding vector.
/// Expected input: A single ArrayRef that is a ListArray with shape [num_rows][embedding_dim]
/// Output: 2D ndarray with shape [num_rows, embedding_dim]
pub fn list_arrayref_to_ndarray(args: &[ArrayRef]) -> Result<ArrayD<f32>> {
    if args.is_empty() {
        return Err(anyhow!("No input array provided for conversion"));
    }

    if args.len() != 1 {
        return Err(anyhow!(
            "Expected single ListArray input for embedding conversion, got {} arrays",
            args.len()
        ));
    }

    let array = &args[0];

    // Handle ListArray: each element is an embedding vector
    if let Some(list_arr) = array.as_any().downcast_ref::<ListArray>() {
        let num_rows = list_arr.len();

        if num_rows == 0 {
            return Err(anyhow!("Empty ListArray provided"));
        }

        // Get the first list to determine embedding dimension
        let first_list = list_arr.value(0);
        let embedding_dim = first_list.len();

        // Create 2D array with shape [num_rows, embedding_dim]
        let mut tensor = ndarray::Array2::<f32>::zeros((num_rows, embedding_dim));

        // Fill the tensor with embedding vectors
        for row_idx in 0..num_rows {
            let embedding_array = list_arr.value(row_idx);

            // Try to downcast to Float32Array first, then Float64Array
            if let Some(f32_arr) = embedding_array.as_any().downcast_ref::<Float32Array>() {
                if f32_arr.len() != embedding_dim {
                    return Err(anyhow!(
                        "Inconsistent embedding dimension at row {}: expected {}, got {}",
                        row_idx,
                        embedding_dim,
                        f32_arr.len()
                    ));
                }
                for col_idx in 0..embedding_dim {
                    tensor[[row_idx, col_idx]] = f32_arr.value(col_idx);
                }
            } else if let Some(f64_arr) = embedding_array.as_any().downcast_ref::<Float64Array>() {
                if f64_arr.len() != embedding_dim {
                    return Err(anyhow!(
                        "Inconsistent embedding dimension at row {}: expected {}, got {}",
                        row_idx,
                        embedding_dim,
                        f64_arr.len()
                    ));
                }
                for col_idx in 0..embedding_dim {
                    tensor[[row_idx, col_idx]] = f64_arr.value(col_idx) as f32;
                }
            } else {
                return Err(anyhow!(
                    "Unsupported inner array type in ListArray: {:?}",
                    embedding_array.data_type()
                ));
            }
        }

        Ok(tensor.into_dyn())
    } else {
        Err(anyhow!(
            "Expected ListArray for embedding conversion, got {:?}",
            array.data_type()
        ))
    }
}
