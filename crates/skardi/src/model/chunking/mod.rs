//! `chunk` UDF — split text into chunks for inline ingestion.
//!
//! Returns `List<Utf8>` so callers can keep chunks as a list column or expand
//! them into rows with `UNNEST(chunk(...))`.
//!
//! Usage:
//! ```text
//! chunk('character', text_col, 1000)         -- size only
//! chunk('character', text_col, 1000, 200)    -- size + overlap
//! chunk('markdown',  text_col, 1000, 200)
//! ```
//!
//! Backed by the [`text-splitter`](https://crates.io/crates/text-splitter) crate.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ListBuilder, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Field};
use datafusion::common::Result as DfResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use text_splitter::{ChunkConfig, MarkdownSplitter, TextSplitter};

// =============================================================================
// ChunkingRegistry — handle for the `chunk` UDF
// =============================================================================

/// Registry for the `chunk` UDF.
///
/// The character / markdown splitters are stateless and cheap to construct, so
/// no caching is needed today. The registry exists for parity with other UDF
/// registries (`CandleModelRegistry`, etc.) and as a hook for future modes that
/// need cached state (e.g. tokenizer-based or code-language splitters).
#[derive(Debug, Default)]
pub struct ChunkingRegistry;

impl ChunkingRegistry {
    pub fn new() -> Self {
        Self
    }

    /// Register the `chunk` UDF with a DataFusion `SessionContext`.
    ///
    /// SQL signature:
    /// ```text
    /// chunk(mode, text, size [, overlap]) -> List<Utf8>
    /// ```
    /// - `mode`: `'character'` or `'markdown'`
    /// - `text`: `Utf8` literal, scalar subquery, or column
    /// - `size`: target max chunk length (characters), positive integer literal
    /// - `overlap`: optional characters of overlap between adjacent chunks; must be `< size`
    pub fn register_chunk_udf(self: &Arc<Self>, ctx: &mut SessionContext) {
        let udf = ScalarUDF::new_from_impl(ChunkingUDF::new(Arc::clone(self)));
        ctx.register_udf(udf);
        tracing::info!("Registered 'chunk' UDF");
    }
}

// =============================================================================
// ChunkingUDF — ScalarUDFImpl
// =============================================================================

#[derive(Debug)]
struct ChunkingUDF {
    registry: Arc<ChunkingRegistry>,
    signature: Signature,
}

impl PartialEq for ChunkingUDF {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.registry, &other.registry)
    }
}

impl Eq for ChunkingUDF {}

impl std::hash::Hash for ChunkingUDF {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.registry).hash(state);
    }
}

impl ChunkingUDF {
    fn new(registry: Arc<ChunkingRegistry>) -> Self {
        Self {
            registry,
            // mode + text + size [+ overlap]
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ChunkingUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "chunk"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let args = args.args;

        if args.len() < 3 || args.len() > 4 {
            return Err(DataFusionError::Execution(format!(
                "chunk expects 3 or 4 arguments (mode, text, size [, overlap]); got {}",
                args.len()
            )));
        }

        let mode = read_scalar_string(&args[0], "mode")?;
        let size = read_scalar_usize(&args[2], "size")?;
        if size == 0 {
            return Err(DataFusionError::Execution(
                "chunk: 'size' must be > 0".to_string(),
            ));
        }
        let overlap = if args.len() == 4 {
            read_scalar_usize(&args[3], "overlap")?
        } else {
            0
        };
        // text-splitter's ChunkConfig::with_overlap also rejects this; the explicit
        // check exists so the error names both values instead of a generic message.
        if overlap >= size {
            return Err(DataFusionError::Execution(format!(
                "chunk: 'overlap' ({overlap}) must be strictly less than 'size' ({size})"
            )));
        }

        let texts = read_text_column(&args[1], "text")?;

        let array: ArrayRef = match mode.as_str() {
            "character" => {
                let cfg = build_config(size, overlap)?;
                let splitter = TextSplitter::new(cfg);
                build_list_array(&texts, |t| splitter.chunks(t))
            }
            "markdown" => {
                let cfg = build_config(size, overlap)?;
                let splitter = MarkdownSplitter::new(cfg);
                build_list_array(&texts, |t| splitter.chunks(t))
            }
            other => {
                return Err(DataFusionError::Execution(format!(
                    "chunk: unsupported mode '{other}'; supported modes: 'character', 'markdown'"
                )));
            }
        };

        Ok(ColumnarValue::Array(array))
    }
}

// =============================================================================
// Argument decoding helpers
// =============================================================================

fn read_scalar_string(arg: &ColumnarValue, name: &str) -> DfResult<String> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => Ok(s.clone()),
        ColumnarValue::Scalar(ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None)) => Err(
            DataFusionError::Execution(format!("chunk: '{name}' argument must not be null")),
        ),
        ColumnarValue::Array(_) => Err(DataFusionError::Execution(format!(
            "chunk: '{name}' must be a literal, not a column"
        ))),
        _ => Err(DataFusionError::Execution(format!(
            "chunk: '{name}' argument must be a Utf8 literal"
        ))),
    }
}

fn read_scalar_usize(arg: &ColumnarValue, name: &str) -> DfResult<usize> {
    let n: i64 = match arg {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(n))) => *n,
        ColumnarValue::Scalar(ScalarValue::Int32(Some(n))) => i64::from(*n),
        ColumnarValue::Scalar(ScalarValue::Int16(Some(n))) => i64::from(*n),
        ColumnarValue::Scalar(ScalarValue::Int8(Some(n))) => i64::from(*n),
        ColumnarValue::Scalar(ScalarValue::UInt64(Some(n))) => i64::try_from(*n).map_err(|_| {
            DataFusionError::Execution(format!("chunk: '{name}' value {n} overflows i64"))
        })?,
        ColumnarValue::Scalar(ScalarValue::UInt32(Some(n))) => i64::from(*n),
        ColumnarValue::Scalar(ScalarValue::UInt16(Some(n))) => i64::from(*n),
        ColumnarValue::Scalar(ScalarValue::UInt8(Some(n))) => i64::from(*n),
        _ => {
            return Err(DataFusionError::Execution(format!(
                "chunk: '{name}' argument must be an integer literal"
            )));
        }
    };
    if n < 0 {
        return Err(DataFusionError::Execution(format!(
            "chunk: '{name}' must be non-negative (got {n})"
        )));
    }
    Ok(n as usize)
}

fn read_text_column<'a>(arg: &'a ColumnarValue, name: &str) -> DfResult<Vec<Option<&'a str>>> {
    match arg {
        ColumnarValue::Array(arr) => {
            let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                DataFusionError::Execution(format!("chunk: '{name}' must be a Utf8 column"))
            })?;
            Ok((0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) {
                        None
                    } else {
                        Some(str_arr.value(i))
                    }
                })
                .collect())
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => Ok(vec![Some(s.as_str())]),
        ColumnarValue::Scalar(ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None)) => {
            Ok(vec![None])
        }
        _ => Err(DataFusionError::Execution(format!(
            "chunk: '{name}' must be Utf8"
        ))),
    }
}

// =============================================================================
// Splitter helpers
// =============================================================================

fn build_config(size: usize, overlap: usize) -> DfResult<ChunkConfig<text_splitter::Characters>> {
    ChunkConfig::new(size)
        .with_overlap(overlap)
        .map_err(|e| DataFusionError::Execution(format!("chunk: invalid chunk config: {e}")))
}

/// Build a `ListArray<Utf8>` by applying `split` to each non-null row.
fn build_list_array<'t, F, I>(texts: &[Option<&'t str>], mut split: F) -> ArrayRef
where
    F: FnMut(&'t str) -> I,
    I: Iterator<Item = &'t str>,
{
    let mut builder = ListBuilder::new(StringBuilder::new());
    for maybe_text in texts {
        match maybe_text {
            Some(text) => {
                for chunk in split(text) {
                    builder.values().append_value(chunk);
                }
                builder.append(true);
            }
            None => builder.append(false),
        }
    }
    Arc::new(builder.finish())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ListArray, StringArray};
    use arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::ScalarFunctionArgs;

    fn make_args(args: Vec<ColumnarValue>) -> ScalarFunctionArgs {
        let number_rows = args
            .iter()
            .map(|a| match a {
                ColumnarValue::Array(arr) => arr.len(),
                ColumnarValue::Scalar(_) => 1,
            })
            .max()
            .unwrap_or(1);
        let arg_fields = args
            .iter()
            .map(|a| Arc::new(Field::new("_", a.data_type(), true)))
            .collect();
        let return_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field: Arc::new(Field::new("chunks", return_type, true)),
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    fn udf() -> ChunkingUDF {
        ChunkingUDF::new(Arc::new(ChunkingRegistry::new()))
    }

    fn list_at(arr: &ListArray, row: usize) -> Vec<String> {
        let inner = arr.value(row);
        let s = inner.as_any().downcast_ref::<StringArray>().unwrap();
        (0..s.len()).map(|i| s.value(i).to_string()).collect()
    }

    #[test]
    fn character_mode_splits_long_literal() {
        let text = "a".repeat(2500);
        let result = udf()
            .invoke_with_args(make_args(vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("character".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(text.clone()))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1000))),
            ]))
            .unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array result"),
        };
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.len(), 1);
        let chunks = list_at(list, 0);
        assert!(
            chunks.len() >= 3,
            "expected ≥3 chunks, got {}",
            chunks.len()
        );
        assert!(chunks.iter().all(|c| c.len() <= 1000));
        assert_eq!(chunks.concat(), text);
    }

    #[test]
    fn character_mode_with_overlap() {
        let text = "a".repeat(500);
        let result = udf()
            .invoke_with_args(make_args(vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("character".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(text))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(100))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(20))),
            ]))
            .unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
        let chunks = list_at(list, 0);
        // Without overlap: 500/100 = 5 chunks. With 20 overlap, expect ≥5.
        assert!(chunks.len() >= 5);
        assert!(chunks.iter().all(|c| c.len() <= 100));
    }

    #[test]
    fn markdown_mode_respects_headings() {
        let text = "# Heading One\n\nBody one.\n\n# Heading Two\n\nBody two.";
        let result = udf()
            .invoke_with_args(make_args(vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("markdown".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(text.to_string()))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(30))),
            ]))
            .unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
        let chunks = list_at(list, 0);
        assert!(chunks.len() >= 2);
        assert!(
            chunks.iter().any(|c| c.contains("# Heading")),
            "expected at least one chunk to keep a heading: {chunks:?}"
        );
    }

    #[test]
    fn character_mode_counts_chars_not_bytes() {
        // Each "日" is 3 bytes but 1 char. Size=20 chars must hold for char count,
        // not byte count — a byte-based splitter would emit chunks well under 20 chars.
        let text = "日本語段落。".repeat(50);
        let result = udf()
            .invoke_with_args(make_args(vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("character".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(text.clone()))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(20))),
            ]))
            .unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
        let chunks = list_at(list, 0);
        assert!(!chunks.is_empty());
        for c in &chunks {
            assert!(
                c.chars().count() <= 20,
                "chunk exceeds 20 chars: {} chars in {c:?}",
                c.chars().count()
            );
        }
        assert_eq!(chunks.concat(), text, "chunks should reconstruct input");
    }

    #[test]
    fn array_input_chunks_per_row() {
        let texts = StringArray::from(vec![Some("a".repeat(250)), Some("b".repeat(50)), None]);
        let result = udf()
            .invoke_with_args(make_args(vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("character".to_string()))),
                ColumnarValue::Array(Arc::new(texts)),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(100))),
            ]))
            .unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.len(), 3);
        assert!(list_at(list, 0).len() >= 3); // 250 chars / 100 → ≥3
        assert_eq!(list_at(list, 1).len(), 1); // 50 chars fits in one
        assert!(list.is_null(2)); // null in → null out
    }

    #[test]
    fn unknown_mode_errors() {
        let err = udf()
            .invoke_with_args(make_args(vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("token".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(100))),
            ]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("unsupported mode"), "got: {err}");
    }

    #[test]
    fn overlap_must_be_less_than_size() {
        let err = udf()
            .invoke_with_args(make_args(vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("character".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(100))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(100))),
            ]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("strictly less than"), "got: {err}");
    }

    #[test]
    fn array_mode_argument_rejected() {
        // `mode` must be a literal — passing a column (Array) should error,
        // not silently use row 0.
        let modes = StringArray::from(vec!["character", "markdown"]);
        let err = udf()
            .invoke_with_args(make_args(vec![
                ColumnarValue::Array(Arc::new(modes)),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(100))),
            ]))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("must be a literal, not a column"),
            "got: {err}"
        );
    }

    #[test]
    fn wrong_arity_errors() {
        let err = udf()
            .invoke_with_args(make_args(vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("character".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("hi".to_string()))),
            ]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("3 or 4 arguments"), "got: {err}");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SQL-level integration tests — exercise the full DataFusion path
    // ─────────────────────────────────────────────────────────────────────────

    use arrow::array::Int64Array;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::FunctionRegistry;

    fn build_ctx() -> SessionContext {
        let mut ctx = SessionContext::new();
        Arc::new(ChunkingRegistry::new()).register_chunk_udf(&mut ctx);
        ctx
    }

    #[tokio::test]
    async fn sql_registers_and_returns_list_column() {
        let ctx = build_ctx();
        assert!(ctx.udf("chunk").is_ok(), "chunk UDF should be registered");

        let body = "a".repeat(250);
        let sql = format!("SELECT chunk('character', '{body}', 100) AS chunks");
        let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);

        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("chunk should return a ListArray");
        let inner = list.value(0);
        let strings = inner.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(
            strings.len() >= 3,
            "expected ≥3 chunks, got {}",
            strings.len()
        );
        let joined: String = (0..strings.len())
            .map(|i| strings.value(i).to_string())
            .collect();
        assert_eq!(joined, body);
    }

    #[tokio::test]
    async fn sql_unnest_expands_chunks_into_rows() {
        let ctx = build_ctx();
        let body = "x".repeat(220);
        let sql = format!("SELECT UNNEST(chunk('character', '{body}', 100)) AS chunk_text");
        let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total >= 3, "expected ≥3 rows from UNNEST, got {total}");

        for batch in &batches {
            let strings = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("UNNEST'd column should be Utf8");
            for i in 0..strings.len() {
                assert!(strings.value(i).len() <= 100);
            }
        }
    }

    #[tokio::test]
    async fn sql_chunks_per_row_over_registered_table() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("body", DataType::Utf8, false),
        ]));
        let body0 = String::from("# Header\n\n") + &"para one. ".repeat(20);
        let body1 = String::from("short doc");
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2])),
                Arc::new(StringArray::from(vec![body0, body1])),
            ],
        )
        .unwrap();

        let ctx = build_ctx();
        ctx.register_batch("docs", batch).unwrap();

        let batches = ctx
            .sql(
                "SELECT id, UNNEST(chunk('markdown', body, 50)) AS chunk_text \
                 FROM docs ORDER BY id",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(
            total_rows >= 2,
            "expected ≥2 expanded rows, got {total_rows}"
        );

        let mut ids_seen = std::collections::BTreeSet::new();
        for b in &batches {
            let id_arr = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let text_arr = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..b.num_rows() {
                ids_seen.insert(id_arr.value(i));
                assert!(
                    !text_arr.value(i).is_empty(),
                    "chunk text should be non-empty"
                );
            }
        }
        assert_eq!(
            ids_seen,
            [1i64, 2].iter().copied().collect(),
            "both source rows should appear in the expanded output"
        );
    }

    /// Smoke-tests the exact SQL pattern the demo pipelines emit:
    /// `ROW_NUMBER() OVER (ORDER BY 1)` over `UNNEST(chunk(...))`.
    /// If this stops working, the demos break too — fail loudly here first.
    #[tokio::test]
    async fn sql_row_number_over_unnest_chunk() {
        let ctx = build_ctx();
        let body = "a".repeat(250);
        let sql = format!(
            "SELECT ROW_NUMBER() OVER (ORDER BY 1) AS rn, chunk_text \
             FROM (SELECT UNNEST(chunk('character', '{body}', 100)) AS chunk_text)"
        );
        let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total >= 3, "expected ≥3 rows, got {total}");
    }

    /// Smoke-tests slug synthesis used by the LLM Wiki bulk-create demo:
    /// `prefix || '/p' || lpad(CAST(rn AS VARCHAR), 3, '0')`.
    #[tokio::test]
    async fn sql_slug_synthesis_over_chunked_text() {
        let ctx = build_ctx();
        let body = "a".repeat(220);
        let sql = format!(
            "SELECT \
               'alice/chap1' || '/p' || lpad(CAST(rn AS VARCHAR), 3, '0') AS slug, \
               chunk_text \
             FROM ( \
               SELECT chunk_text, ROW_NUMBER() OVER (ORDER BY 1) AS rn \
               FROM (SELECT UNNEST(chunk('character', '{body}', 100)) AS chunk_text) \
             )"
        );
        let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total >= 3, "expected ≥3 rows, got {total}");

        let mut slugs: Vec<String> = vec![];
        for b in &batches {
            let s = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..s.len() {
                slugs.push(s.value(i).to_string());
            }
        }
        slugs.sort();
        assert!(slugs[0].starts_with("alice/chap1/p0"), "got {slugs:?}");
        assert!(slugs.iter().all(|s| s.starts_with("alice/chap1/p")));
    }

    // Cross-UDF composition guards (chunk × candle/gguf/onnx/remote_embed)
    // live in `crates/skardi/tests/cross_udf_composition.rs` so chunking
    // module tests stay focused on chunk()'s own behaviour.
}
