#[cfg(feature = "onnx")]
pub mod converter;
#[cfg(feature = "gguf")]
pub mod gguf;
pub mod model;
#[cfg(feature = "onnx")]
pub mod onnx;

// Re-export for convenience
#[cfg(feature = "gguf")]
pub use gguf::GgufModelRegistry;
#[cfg(feature = "onnx")]
pub use onnx::OnnxModelRegistry;
