use anyhow::Result;
use arrow::array::{ArrayRef, Float32Array};

/// Generic model interface for Skardi ML modules.
pub trait Model {
    /// Run prediction on a slice of Arrow arrays. Returns a Float32Array of length equal to row count.
    fn predict(&self, args: &[ArrayRef]) -> Result<Float32Array>;
}
