//! `vec_to_binary` ScalarUDF — converts `List<Float32>` / `FixedSizeList<Float32>`
//! to a packed little-endian f32 `Binary` blob, suitable for sqlite-vec vec0 tables.
//!
//! Usage in SQL:
//!   vec_to_binary(candle('models/bge-small-en-v1.5', text_col))

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, BinaryBuilder, FixedSizeListArray, Float32Array, ListArray};
use arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;

/// Register the `vec_to_binary` scalar UDF on the given session context.
pub fn register_vec_to_binary_udf(ctx: &mut SessionContext) {
    let udf = ScalarUDF::new_from_impl(VecToBinaryUDF::new());
    ctx.register_udf(udf);
}

// ─── Implementation ─────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct VecToBinaryUDF {
    signature: Signature,
}

impl VecToBinaryUDF {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for VecToBinaryUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "vec_to_binary"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 1 {
            return Err(DataFusionError::Plan(
                "vec_to_binary expects exactly 1 argument".to_string(),
            ));
        }

        let array = match &args[0] {
            ColumnarValue::Array(arr) => Arc::clone(arr),
            ColumnarValue::Scalar(s) => s.to_array()?,
        };

        let result = match array.data_type() {
            DataType::List(field) if *field.data_type() == DataType::Float32 => {
                let list = array
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| DataFusionError::Internal("Expected ListArray".to_string()))?;
                list_f32_to_binary(list)?
            }
            DataType::FixedSizeList(field, _) if *field.data_type() == DataType::Float32 => {
                let list = array
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal("Expected FixedSizeListArray".to_string())
                    })?;
                fixed_list_f32_to_binary(list)?
            }
            DataType::Binary => {
                // Already binary — pass through.
                array
            }
            other => {
                return Err(DataFusionError::Plan(format!(
                    "vec_to_binary: unsupported input type {other}. \
                     Expected List<Float32>, FixedSizeList<Float32>, or Binary"
                )));
            }
        };

        Ok(ColumnarValue::Array(result))
    }
}

/// Convert a `ListArray` of `Float32` values to a `BinaryArray` of packed LE f32 bytes.
fn list_f32_to_binary(list: &ListArray) -> DFResult<Arc<dyn Array>> {
    let mut builder = BinaryBuilder::new();
    for i in 0..list.len() {
        if list.is_null(i) {
            builder.append_null();
        } else {
            let values = list.value(i);
            let f32_arr = values
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("List elements are not Float32".to_string())
                })?;
            let blob: Vec<u8> = f32_arr
                .values()
                .iter()
                .flat_map(|f| f.to_le_bytes())
                .collect();
            builder.append_value(&blob);
        }
    }
    Ok(Arc::new(builder.finish()) as Arc<dyn Array>)
}

/// Convert a `FixedSizeListArray` of `Float32` values to a `BinaryArray` of packed LE f32 bytes.
fn fixed_list_f32_to_binary(list: &FixedSizeListArray) -> DFResult<Arc<dyn Array>> {
    let mut builder = BinaryBuilder::new();
    for i in 0..list.len() {
        if list.is_null(i) {
            builder.append_null();
        } else {
            let values = list.value(i);
            let f32_arr = values
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("FixedSizeList elements are not Float32".to_string())
                })?;
            let blob: Vec<u8> = f32_arr
                .values()
                .iter()
                .flat_map(|f| f.to_le_bytes())
                .collect();
            builder.append_value(&blob);
        }
    }
    Ok(Arc::new(builder.finish()) as Arc<dyn Array>)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BinaryArray, Float32Array, ListArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::ScalarFunctionArgs;

    #[test]
    fn test_vec_to_binary_list_f32() {
        let udf = VecToBinaryUDF::new();
        // Build a ListArray with one row containing [1.0, 2.0, 3.0]
        let values = Float32Array::from(vec![1.0f32, 2.0, 3.0]);
        let offsets = OffsetBuffer::new(vec![0i32, 3].into());
        let field = Arc::new(Field::new("item", DataType::Float32, true));
        let list = ListArray::new(field, offsets, Arc::new(values), None);

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(list))],
            arg_fields: vec![Arc::new(Field::new(
                "v",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            ))],
            number_rows: 1,
            return_field: Arc::new(Field::new("out", DataType::Binary, true)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args).unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let binary = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(binary.len(), 1);
        // 3 floats × 4 bytes = 12 bytes
        assert_eq!(binary.value(0).len(), 12);

        // Verify the packed values
        let bytes = binary.value(0);
        let f1 = f32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let f2 = f32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let f3 = f32::from_le_bytes(bytes[8..12].try_into().unwrap());
        assert_eq!(f1, 1.0);
        assert_eq!(f2, 2.0);
        assert_eq!(f3, 3.0);
    }
}
