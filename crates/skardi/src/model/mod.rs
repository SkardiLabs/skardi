#[cfg(feature = "candle")]
pub mod candle;
#[cfg(feature = "chunking")]
pub mod chunking;
#[cfg(feature = "onnx")]
pub mod converter;
#[cfg(feature = "gguf")]
pub mod gguf;
#[cfg(feature = "llm-extract")]
pub mod llm_extract;
pub mod model;
#[cfg(feature = "onnx")]
pub mod onnx;
#[cfg(feature = "remote-embed")]
pub mod remote_embed;

// Re-export for convenience
#[cfg(feature = "candle")]
pub use candle::CandleModelRegistry;
#[cfg(feature = "chunking")]
pub use chunking::ChunkingRegistry;
#[cfg(feature = "gguf")]
pub use gguf::GgufModelRegistry;
#[cfg(feature = "onnx")]
pub use onnx::OnnxModelRegistry;
#[cfg(feature = "remote-embed")]
pub use remote_embed::RemoteEmbedRegistry;

use arrow::{
    array::{ArrayRef, Float32Builder, ListBuilder},
    datatypes::{DataType, Field},
};
use std::sync::Arc;

/// Convert a batch of embedding vectors into an Arrow `ListArray<Float32>`.
///
/// The resulting array has one list element per input row, compatible with
/// `lance_knn` directly. `None` entries produce null list elements.
pub fn vecs_to_list_array(vecs: Vec<Option<Vec<f32>>>) -> ArrayRef {
    let mut builder = ListBuilder::new(Float32Builder::new());
    for vec in vecs {
        match vec {
            Some(v) => {
                for val in &v {
                    builder.values().append_value(*val);
                }
                builder.append(true);
            }
            None => {
                builder.append(false);
            }
        }
    }
    Arc::new(builder.finish())
}

/// Return type for embedding UDFs: `List<Float32>`.
pub fn embedding_return_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Float32, true)))
}
