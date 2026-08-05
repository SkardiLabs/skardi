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
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(k)))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(k))) => k.clone(),
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
            // insertion order (default feature set), which is what makes
            // the generator's output deterministic byte-for-byte.
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
        ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => {
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
        ScalarValue::TimestampMicrosecond(v, _) => {
            v.map_or(Value::Null, |us| Value::from(us.div_euclid(1000)))
        }
        ScalarValue::TimestampNanosecond(v, _) => {
            v.map_or(Value::Null, |ns| Value::from(ns.div_euclid(1_000_000)))
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
        let inner = list.value(row);
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
        return Ok(Value::Array(items));
    }
    // Everything scalar-shaped funnels through ScalarValue for one
    // conversion path (and one error vocabulary).
    let scalar = ScalarValue::try_from_array(array, row)?;
    scalar_to_value(&scalar)
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
