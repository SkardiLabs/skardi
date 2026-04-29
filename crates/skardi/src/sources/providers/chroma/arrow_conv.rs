//! Convert Chroma response payloads into Arrow `RecordBatch`es matching
//! the schema produced by `table_provider::chroma_schema`.
//!
//! Chroma metadata values are heterogeneous (`String | i64 | f64 | bool | …`)
//! but the user-facing schema exposes metadata as `Map<Utf8, Utf8>` for
//! simplicity. We stringify each value here; users cast in SQL.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{
    Array, ArrayRef, FixedSizeListBuilder, Float32Array, Float32Builder, MapBuilder,
    StringBuilder, UInt32Array,
};
use arrow::array::{RecordBatch, StringArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, SchemaRef};
use chroma::types::{Metadata, MetadataValue};

/// Build a `RecordBatch` matching the full Chroma schema (id/document/embedding/metadata)
/// from the response of `ChromaCollection::get(...)`.
///
/// `embedding_dim` must match the schema's FixedSizeList width. If `embeddings` is
/// None (because the caller didn't request them in the include list), every row's
/// embedding is null.
pub fn get_response_to_batch(
    schema: SchemaRef,
    embedding_dim: i32,
    ids: Vec<String>,
    documents: Option<Vec<Option<String>>>,
    embeddings: Option<Vec<Vec<f32>>>,
    metadatas: Option<Vec<Option<Metadata>>>,
) -> Result<RecordBatch> {
    let n = ids.len();
    let id_arr: ArrayRef = Arc::new(StringArray::from(ids));
    let doc_arr: ArrayRef = match documents {
        Some(v) => Arc::new(StringArray::from(v)),
        None => Arc::new(StringArray::from(vec![None::<String>; n])),
    };
    let embedding_arr = build_embedding_array(embedding_dim, n, embeddings)?;
    let metadata_arr = build_metadata_array(n, metadatas)?;

    Ok(RecordBatch::try_new(
        schema,
        vec![id_arr, doc_arr, embedding_arr, metadata_arr],
    )?)
}

/// KNN output schema is `id, document, metadata, _distance` — embedding omitted.
/// Build a batch from the first query slot of `QueryResponse`.
pub fn knn_response_to_batch(
    schema: SchemaRef,
    ids: Vec<String>,
    documents: Vec<Option<String>>,
    metadatas: Vec<Option<Metadata>>,
    distances: Vec<Option<f32>>,
) -> Result<RecordBatch> {
    let n = ids.len();
    let id_arr: ArrayRef = Arc::new(StringArray::from(ids));
    let doc_arr: ArrayRef = Arc::new(StringArray::from(documents));
    let metadata_arr = build_metadata_array(n, Some(metadatas))?;
    let dist_arr: ArrayRef = Arc::new(Float32Array::from(distances));
    Ok(RecordBatch::try_new(
        schema,
        vec![id_arr, doc_arr, metadata_arr, dist_arr],
    )?)
}

fn build_embedding_array(
    dim: i32,
    n: usize,
    embeddings: Option<Vec<Vec<f32>>>,
) -> Result<ArrayRef> {
    let value_field = Arc::new(Field::new("item", DataType::Float32, true));
    let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), dim).with_field(value_field);
    match embeddings {
        Some(rows) => {
            for row in rows {
                if row.len() != dim as usize {
                    anyhow::bail!(
                        "chroma: embedding length {} does not match collection dim {}",
                        row.len(),
                        dim
                    );
                }
                for v in row {
                    builder.values().append_value(v);
                }
                builder.append(true);
            }
        }
        None => {
            for _ in 0..n {
                for _ in 0..dim {
                    builder.values().append_null();
                }
                builder.append(false);
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_metadata_array(
    n: usize,
    metadatas: Option<Vec<Option<Metadata>>>,
) -> Result<ArrayRef> {
    // Arrow Map = list of struct<key,value> pairs per row. MapBuilder handles
    // the offsets and the validity buffer for us.
    let mut builder: MapBuilder<StringBuilder, StringBuilder> =
        MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());

    let provided = metadatas.unwrap_or_else(|| vec![None; n]);
    for entry in provided {
        match entry {
            Some(map) => {
                for (k, v) in map_to_sorted_pairs(&map) {
                    builder.keys().append_value(&k);
                    builder.values().append_value(&v);
                }
                builder.append(true)?;
            }
            None => builder.append(false)?,
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn map_to_sorted_pairs(map: &HashMap<String, MetadataValue>) -> Vec<(String, String)> {
    // Sort by key for deterministic ordering — makes snapshot tests stable.
    let mut pairs: Vec<(String, String)> = map
        .iter()
        .map(|(k, v)| (k.clone(), metadata_value_to_string(v)))
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    pairs
}

fn metadata_value_to_string(v: &MetadataValue) -> String {
    match v {
        MetadataValue::Str(s) => s.clone(),
        MetadataValue::Int(i) => i.to_string(),
        MetadataValue::Float(f) => f.to_string(),
        MetadataValue::Bool(b) => b.to_string(),
        // Sparse vectors / arrays are uncommon in metadata; fall back to a
        // debug repr rather than failing the whole query.
        other => format!("{other:?}"),
    }
}

// Suppress dead-code on imports we keep for future projection logic.
#[allow(dead_code)]
fn _project_unused(arr: &ArrayRef) -> ArrayRef {
    Arc::new(UInt32Array::from(vec![0u32; arr.len()]))
}

#[allow(dead_code)]
fn _offset_buffer_unused() -> OffsetBuffer<i32> {
    OffsetBuffer::new_empty()
}
