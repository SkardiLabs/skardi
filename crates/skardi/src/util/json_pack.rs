//! `json_pack` ScalarUDF — the SQL-callable JSON **encoder**.
//!
//! ```text
//! json_pack('number', number_col, 'state', state_col) -> Utf8 (a JSON object)
//! ```
//!
//! Exists because nothing else can serialize JSON from SQL on the locked
//! DataFusion: core has never shipped a JSON encoder (checked through 54.x),
//! `datafusion-functions-json` is read-side only (`json_get`, …), and
//! `named_struct` output has no serializer. The etl generator's `metadata`
//! and OKF `frontmatter` packing route through this UDF, which makes it the
//! **injection boundary** the generator's Security Model leans on: values
//! are encoded by `serde_json`, so untrusted SaaS strings (quotes,
//! backslashes, control characters) can never break out of the
//! serialization (skardi-cloud `design_docs/skardi_etl_generator.md`).
//!
//! Contract:
//! - arguments are `(key, value)` pairs; an odd count is an error;
//! - keys are non-null Utf8 **literals** (the object shape is
//!   pack/recipe-authored, never data-driven); a duplicate key is an error
//!   rather than last-wins — a generated statement carrying one is a bug;
//! - values may be Utf8/Boolean/Int*/UInt*/Float*/Timestamp columns or
//!   literals; SQL NULL encodes as JSON `null`;
//! - `List<Utf8>` values encode as JSON string arrays (recipe `tags`-style
//!   columns); other nested types are rejected with a targeted error;
//! - the result is deterministic: keys appear in argument order.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ListArray, StringArray, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use serde_json::Value;

/// Register the `json_pack` scalar UDF on the given session context.
pub fn register_json_pack_udf(ctx: &mut SessionContext) {
    let udf = ScalarUDF::new_from_impl(JsonPackUDF::new());
    ctx.register_udf(udf);
    tracing::info!("Registered 'json_pack' UDF");
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonPackUDF {
    signature: Signature,
}

impl JsonPackUDF {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonPackUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "json_pack"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let number_rows = args.number_rows;
        let args = args.args;

        if args.is_empty() || !args.len().is_multiple_of(2) {
            return Err(DataFusionError::Execution(format!(
                "json_pack expects a non-empty, even number of arguments \
                 (key, value, ...); got {}",
                args.len()
            )));
        }

        // Keys: non-null Utf8 literals, unique, in argument order.
        let mut keys: Vec<String> = Vec::with_capacity(args.len() / 2);
        for pair in args.chunks(2) {
            let key = match &pair[0] {
                // Utf8View included: DataFusion 52 carries computed string
                // literals as view scalars (the filters.rs precedent) — a
                // key produced by a coercion must not fail the pack.
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(k)))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(k)))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(k))) => k.clone(),
                ColumnarValue::Array(_) => {
                    return Err(DataFusionError::Execution(
                        "json_pack: keys must be Utf8 literals, not columns".to_string(),
                    ));
                }
                _ => {
                    return Err(DataFusionError::Execution(
                        "json_pack: keys must be non-null Utf8 literals".to_string(),
                    ));
                }
            };
            if keys.contains(&key) {
                return Err(DataFusionError::Execution(format!(
                    "json_pack: duplicate key '{key}'"
                )));
            }
            keys.push(key);
        }

        let mut builder = StringBuilder::new();
        for row in 0..number_rows {
            // Argument order IS object order — serde_json::Map preserves
            // insertion order because THIS crate enables `preserve_order`
            // explicitly (see Cargo.toml; without the feature Map is a
            // BTreeMap and silently re-sorts keys lexicographically),
            // which is what makes the generator's output deterministic
            // byte-for-byte.
            let mut object = serde_json::Map::with_capacity(keys.len());
            for (key, pair) in keys.iter().zip(args.chunks(2)) {
                object.insert(key.clone(), value_at(&pair[1], row)?);
            }
            builder.append_value(Value::Object(object).to_string());
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// The JSON value of `arg` at `row`. SQL NULL → JSON null; unsupported
/// types fail with the Arrow type named (never the value).
fn value_at(arg: &ColumnarValue, row: usize) -> DFResult<Value> {
    match arg {
        ColumnarValue::Scalar(scalar) => scalar_to_value(scalar),
        ColumnarValue::Array(array) => array_value_at(array, row),
    }
}

fn scalar_to_value(scalar: &ScalarValue) -> DFResult<Value> {
    Ok(match scalar {
        ScalarValue::Null => Value::Null,
        ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) | ScalarValue::Utf8View(v) => {
            v.as_ref().map_or(Value::Null, |s| Value::from(s.as_str()))
        }
        ScalarValue::Boolean(v) => v.map_or(Value::Null, Value::from),
        ScalarValue::Int8(v) => v.map_or(Value::Null, Value::from),
        ScalarValue::Int16(v) => v.map_or(Value::Null, Value::from),
        ScalarValue::Int32(v) => v.map_or(Value::Null, Value::from),
        ScalarValue::Int64(v) => v.map_or(Value::Null, Value::from),
        ScalarValue::UInt8(v) => v.map_or(Value::Null, Value::from),
        ScalarValue::UInt16(v) => v.map_or(Value::Null, Value::from),
        ScalarValue::UInt32(v) => v.map_or(Value::Null, Value::from),
        ScalarValue::UInt64(v) => v.map_or(Value::Null, Value::from),
        ScalarValue::Float32(v) => float_value(v.map(f64::from))?,
        ScalarValue::Float64(v) => float_value(*v)?,
        // Timestamps render as epoch milliseconds — a number, not a
        // formatted string, so consumers never parse a locale.
        ScalarValue::TimestampMillisecond(v, _) => v.map_or(Value::Null, Value::from),
        ScalarValue::TimestampSecond(v, _) => v.map_or(Ok(Value::Null), |s| {
            s.checked_mul(1000).map(Value::from).ok_or_else(|| {
                DataFusionError::Execution(
                    "json_pack: timestamp out of range for milliseconds".to_string(),
                )
            })
        })?,
        // Sub-millisecond precision is dropped by FLOOR division
        // (`div_euclid`, rounds toward -inf — a pre-epoch instant rounds
        // EARLIER, never toward zero: -1500us becomes -2ms), by design:
        // the contract is epoch milliseconds, and micro/nano inputs give
        // up their remainder. Asymmetric with the Second arm above on
        // purpose — narrowing can overflow and must be checked, widening
        // cannot.
        ScalarValue::TimestampMicrosecond(v, _) => {
            v.map_or(Value::Null, |us| Value::from(us.div_euclid(1000)))
        }
        ScalarValue::TimestampNanosecond(v, _) => {
            v.map_or(Value::Null, |ns| Value::from(ns.div_euclid(1_000_000)))
        }
        // A List<Utf8> LITERAL: the docs promise "column or literal" for
        // every supported type, and a `make_array('a','b')` argument
        // const-folds to exactly this scalar — routing it through the same
        // list conversion the array path uses keeps one contract.
        ScalarValue::List(list) => {
            debug_assert_eq!(list.len(), 1, "a list scalar wraps one row");
            if list.is_null(0) {
                Value::Null
            } else {
                utf8_list_value(&list.value(0))?
            }
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "json_pack: unsupported value type {}",
                other.data_type()
            )));
        }
    })
}

fn float_value(v: Option<f64>) -> DFResult<Value> {
    match v {
        None => Ok(Value::Null),
        Some(f) => serde_json::Number::from_f64(f)
            .map(Value::Number)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "json_pack: {f} has no JSON spelling; only finite numbers encode"
                ))
            }),
    }
}

fn array_value_at(array: &ArrayRef, row: usize) -> DFResult<Value> {
    if row >= array.len() {
        return Err(DataFusionError::Execution(
            "json_pack: value column shorter than the row count".to_string(),
        ));
    }
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    // List<Utf8> → JSON string array (recipe `tags`-style columns).
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        return utf8_list_value(&list.value(row));
    }
    // Everything scalar-shaped funnels through ScalarValue for one
    // conversion path (and one error vocabulary).
    let scalar = ScalarValue::try_from_array(array, row)?;
    scalar_to_value(&scalar)
}

/// One list row's elements as a JSON string array — the single conversion
/// both the `List<Utf8>` column path and the const-folded list-literal
/// path use, so the two spellings of the same value cannot diverge.
fn utf8_list_value(inner: &ArrayRef) -> DFResult<Value> {
    let strings = inner
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "json_pack: unsupported list element type {} (only List<Utf8>)",
                inner.data_type()
            ))
        })?;
    let items: Vec<Value> = (0..strings.len())
        .map(|i| {
            if strings.is_null(i) {
                Value::Null
            } else {
                Value::from(strings.value(i))
            }
        })
        .collect();
    Ok(Value::Array(items))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, TimestampMillisecondArray};
    use arrow::datatypes::Field;
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;

    async fn ctx_with_docs() -> SessionContext {
        let mut ctx = SessionContext::new();
        register_json_pack_udf(&mut ctx);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("title", DataType::Utf8, true),
            Field::new(
                "updated",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new((*schema).clone()),
            vec![
                Arc::new(Int64Array::from(vec![7, 8])),
                Arc::new(StringArray::from(vec![
                    // The adversarial row: every JSON metacharacter class.
                    Some("he said \"hi\" \\ \n\t\u{0000}控制"),
                    None,
                ])),
                Arc::new(TimestampMillisecondArray::from(vec![
                    Some(1_735_689_600_000),
                    None,
                ])),
            ],
        )
        .unwrap();
        ctx.register_table(
            "docs",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        ctx
    }

    /// Every documented timestamp granularity converts to epoch millis —
    /// seconds multiply (checked), micros and nanos floor. These are the
    /// conversions with sign/rounding/overflow subtleties, so each is
    /// pinned by value, including a pre-epoch (negative) instant.
    #[tokio::test]
    async fn every_timestamp_granularity_encodes_as_epoch_millis() {
        let ctx = ctx_with_docs().await;
        let batches = ctx
            .sql(
                "SELECT json_pack(\
                   's',  arrow_cast(1735689600, 'Timestamp(Second, None)'), \
                   'us', arrow_cast(1735689600000001, 'Timestamp(Microsecond, None)'), \
                   'ns', arrow_cast(1735689600000000001, 'Timestamp(Nanosecond, None)'), \
                   'neg', arrow_cast(-1500, 'Timestamp(Millisecond, None)'), \
                   'neg_us', arrow_cast(-1500, 'Timestamp(Microsecond, None)'), \
                   'neg_ns', arrow_cast(-1500, 'Timestamp(Nanosecond, None)')) AS m",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let row: Value = serde_json::from_str(col.value(0)).expect("valid JSON");
        assert_eq!(row["s"], 1_735_689_600_000_i64, "seconds × 1000");
        assert_eq!(row["us"], 1_735_689_600_000_i64, "micros floor toward −∞");
        assert_eq!(row["ns"], 1_735_689_600_000_i64, "nanos floor toward −∞");
        assert_eq!(row["neg"], -1500_i64, "pre-epoch instants keep their sign");
        // The negative micro/nano contract, pinned by value: floor rounds
        // pre-epoch instants EARLIER (-1500µs → -2ms), never toward zero
        // (truncation would give -1ms). The comment at the conversion says
        // "floor"; this is what keeps it honest.
        assert_eq!(row["neg_us"], -2_i64, "pre-epoch micros floor toward −∞");
        assert_eq!(row["neg_ns"], -1_i64, "pre-epoch nanos floor toward −∞");
    }

    /// A seconds timestamp too large for a millis i64 refuses with a
    /// targeted error rather than wrapping.
    #[tokio::test]
    async fn a_seconds_timestamp_overflowing_millis_refuses() {
        let ctx = ctx_with_docs().await;
        let err = ctx
            .sql(&format!(
                "SELECT json_pack('t', arrow_cast({}, 'Timestamp(Second, None)'))",
                i64::MAX / 500
            ))
            .await
            .unwrap()
            .collect()
            .await
            .expect_err("must not wrap");
        assert!(
            err.to_string().contains("out of range"),
            "targeted overflow error: {err}"
        );
    }

    /// The documented `List<Utf8>` path, both spellings: a real list COLUMN
    /// (null element and null list included) and a const-folded
    /// `make_array` LITERAL — the docs promise "column or literal", and the
    /// literal arrives as `ScalarValue::List`, not an array.
    #[tokio::test]
    async fn utf8_lists_encode_as_json_string_arrays_in_both_spellings() {
        let mut ctx = SessionContext::new();
        register_json_pack_udf(&mut ctx);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]));
        let mut tags = arrow::array::ListBuilder::new(arrow::array::StringBuilder::new());
        tags.values().append_value("physics");
        tags.values().append_null();
        tags.values().append_value("qc");
        tags.append(true); // ["physics", null, "qc"]
        tags.append(false); // NULL list
        let batch = RecordBatch::try_new(
            Arc::new((*schema).clone()),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(tags.finish()),
            ],
        )
        .unwrap();
        ctx.register_table(
            "items",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();

        let batches = ctx
            .sql("SELECT json_pack('tags', tags) AS m FROM items ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), r#"{"tags":["physics",null,"qc"]}"#);
        assert_eq!(col.value(1), r#"{"tags":null}"#, "a NULL list is JSON null");

        // The literal spelling: make_array const-folds to ScalarValue::List.
        let batches = ctx
            .sql("SELECT json_pack('tags', make_array('a', 'b')) AS m")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), r#"{"tags":["a","b"]}"#);
    }

    /// A list whose elements are not Utf8 refuses, naming the element type
    /// and never the value.
    #[tokio::test]
    async fn non_utf8_list_elements_refuse_with_the_type_named() {
        let ctx = ctx_with_docs().await;
        let err = ctx
            .sql("SELECT json_pack('xs', make_array(1, 2))")
            .await
            .unwrap()
            .collect()
            .await
            .expect_err("only List<Utf8> encodes");
        let message = err.to_string();
        assert!(
            message.contains("unsupported list element type") && message.contains("Int64"),
            "{message}"
        );
    }

    /// View-typed strings — how DataFusion 52 carries computed string
    /// expressions — encode like their classic layouts, for keys and
    /// values alike (the filters.rs precedent).
    #[tokio::test]
    async fn utf8view_keys_and_values_encode_like_classic_strings() {
        let ctx = ctx_with_docs().await;
        let batches = ctx
            .sql(
                "SELECT json_pack(\
                   arrow_cast('k', 'Utf8View'), arrow_cast('v', 'Utf8View'), \
                   'cast', arrow_cast(title, 'Utf8View')) AS m \
                 FROM docs ORDER BY id",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let row0: Value = serde_json::from_str(col.value(0)).expect("valid JSON");
        assert_eq!(row0["k"], "v");
        assert_eq!(
            row0["cast"].as_str().unwrap(),
            "he said \"hi\" \\ \n\t\u{0000}控制",
            "a view-typed column value survives byte-exact"
        );
        let row1: Value = serde_json::from_str(col.value(1)).expect("valid JSON");
        assert_eq!(row1["cast"], Value::Null);
    }

    /// The injection boundary: adversarial strings round-trip through a real
    /// JSON parser with byte-exact content — nothing escapes the encoding.
    #[tokio::test]
    async fn adversarial_strings_are_encoded_not_interpolated() {
        let ctx = ctx_with_docs().await;
        let batches = ctx
            .sql(
                "SELECT json_pack('id', id, 'title', title, 'updated', updated) AS m \
                  FROM docs ORDER BY id",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let row0: Value = serde_json::from_str(col.value(0)).expect("valid JSON");
        assert_eq!(row0["id"], 7);
        assert_eq!(
            row0["title"].as_str().unwrap(),
            "he said \"hi\" \\ \n\t\u{0000}控制",
            "quotes, backslashes, control chars, and non-ASCII survive byte-exact"
        );
        assert_eq!(row0["updated"], 1_735_689_600_000i64);

        // SQL NULLs → JSON null, and the keys still appear (fixed shape).
        let row1: Value = serde_json::from_str(col.value(1)).expect("valid JSON");
        assert_eq!(row1["title"], Value::Null);
        assert_eq!(row1["updated"], Value::Null);
        // Argument order is object order — determinism the golden bundles pin.
        assert_eq!(
            col.value(1),
            r#"{"id":8,"title":null,"updated":null}"#,
            "byte-deterministic encoding"
        );
    }

    /// Argument order must survive as object order under keys whose
    /// insertion order DISAGREES with lexicographic order — `id, title,
    /// updated` above happens to be alphabetical, so it passes under a
    /// BTreeMap fallback too and cannot detect the loss of serde_json's
    /// `preserve_order` feature. `z, a, m` can.
    #[tokio::test]
    async fn key_order_is_argument_order_not_lexicographic() {
        let ctx = ctx_with_docs().await;
        let batches = ctx
            .sql("SELECT json_pack('z', id, 'a', title, 'm', updated) AS m FROM docs ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            col.value(1),
            r#"{"z":8,"a":null,"m":null}"#,
            "insertion order beats lexicographic order"
        );
    }

    #[tokio::test]
    async fn contract_violations_fail_with_targeted_errors() {
        let ctx = ctx_with_docs().await;
        for (sql, expect) in [
            ("SELECT json_pack('only-key') FROM docs", "even number"),
            ("SELECT json_pack(title, id) FROM docs", "keys must be"),
            (
                "SELECT json_pack('a', id, 'a', title) FROM docs",
                "duplicate key 'a'",
            ),
        ] {
            let err = match ctx.sql(sql).await {
                Err(e) => e.to_string(),
                Ok(df) => df.collect().await.expect_err("must fail").to_string(),
            };
            assert!(err.contains(expect), "{sql}: {err}");
        }
    }

    #[tokio::test]
    async fn non_finite_floats_refuse_to_encode() {
        let mut ctx = SessionContext::new();
        register_json_pack_udf(&mut ctx);
        let err = ctx
            .sql("SELECT json_pack('x', CAST('NaN' AS DOUBLE))")
            .await
            .unwrap()
            .collect()
            .await
            .expect_err("NaN has no JSON spelling");
        assert!(err.to_string().contains("no JSON spelling"), "{err}");
    }
}
