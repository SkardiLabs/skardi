#[cfg(feature = "onnx")]
pub mod converter;
pub mod model;
#[cfg(feature = "onnx")]
pub mod onnx;

// Re-export for convenience
#[cfg(feature = "onnx")]
pub use onnx::OnnxModelRegistry;
