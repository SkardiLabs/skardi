//! Per-parameter JSON Schema fragments for the enriched pipeline inventory.
//!
//! Computed server-side (the enriched `GET /pipelines` response carries no
//! SQL, so downstream bindings cannot run the `VALUES` detection themselves).
//! The DataType → JSON Schema mapping and the unconditional `"null"` union
//! are specified in docs/superpowers/specs/2026-08-13-mcp-stdio-binding-design.md.

use datafusion::arrow::datatypes::DataType;
use serde_json::{Value, json};
use skardi::pipeline::inferencer::VALUES_PLACEHOLDER_RE;

/// The complete JSON Schema fragment for one pipeline parameter.
pub(crate) fn param_json_schema(
    field_type: &DataType,
    sql_template: &str,
    param_name: &str,
) -> Value {
    // `VALUES {name}` is a multi-row tuple list at request time; the inferred
    // Utf8 would be an actively wrong constraint, so it overrides wholesale.
    if VALUES_PLACEHOLDER_RE
        .captures_iter(sql_template)
        .any(|cap| &cap[1] == param_name)
    {
        return json!({"type": "array", "items": {"type": "array"}});
    }
    with_null_union(base_fragment(field_type))
}

/// The type-table mapping without the null union (list items use this raw).
fn base_fragment(field_type: &DataType) -> Value {
    match field_type {
        DataType::Utf8 | DataType::LargeUtf8 => json!({"type": "string"}),
        dt if dt.is_integer() => json!({"type": "integer"}),
        dt if dt.is_floating() || dt.is_decimal() => json!({"type": "number"}),
        DataType::Boolean => json!({"type": "boolean"}),
        DataType::Date32 | DataType::Date64 => json!({"type": "string", "format": "date"}),
        DataType::Timestamp(_, _) => json!({"type": "string", "format": "date-time"}),
        DataType::List(inner) => {
            json!({"type": "array", "items": base_fragment(inner.data_type())})
        }
        _ => json!({}),
    }
}

/// Fold `"null"` into the fragment's `type` — unconditionally, because the
/// server accepts an explicit JSON `null` for every parameter (rendered as
/// SQL `NULL`) and the inferrer hardcodes `nullable: true`. The `{}` (any)
/// fragment already admits null and is left alone.
fn with_null_union(mut fragment: Value) -> Value {
    if let Some(ty) = fragment.get("type").cloned() {
        fragment["type"] = json!([ty, "null"]);
    }
    fragment
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, TimeUnit};

    fn schema(dt: DataType) -> Value {
        param_json_schema(&dt, "SELECT 1 WHERE x = {p}", "p")
    }

    #[test]
    fn maps_scalar_types_with_null_union() {
        assert_eq!(schema(DataType::Utf8), json!({"type": ["string", "null"]}));
        assert_eq!(
            schema(DataType::LargeUtf8),
            json!({"type": ["string", "null"]})
        );
        assert_eq!(
            schema(DataType::Int64),
            json!({"type": ["integer", "null"]})
        );
        assert_eq!(
            schema(DataType::UInt32),
            json!({"type": ["integer", "null"]})
        );
        assert_eq!(
            schema(DataType::Float64),
            json!({"type": ["number", "null"]})
        );
        assert_eq!(
            schema(DataType::Decimal128(10, 2)),
            json!({"type": ["number", "null"]})
        );
        assert_eq!(
            schema(DataType::Boolean),
            json!({"type": ["boolean", "null"]})
        );
    }

    #[test]
    fn maps_temporal_types_with_format() {
        assert_eq!(
            schema(DataType::Date32),
            json!({"type": ["string", "null"], "format": "date"})
        );
        assert_eq!(
            schema(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            json!({"type": ["string", "null"], "format": "date-time"})
        );
    }

    #[test]
    fn maps_lists_with_typed_items() {
        let dt = DataType::List(Field::new("item", DataType::Utf8, true).into());
        assert_eq!(
            schema(dt),
            json!({"type": ["array", "null"], "items": {"type": "string"}})
        );
    }

    #[test]
    fn unknown_types_map_to_any() {
        assert_eq!(schema(DataType::Binary), json!({}));
    }

    #[test]
    fn values_placeholder_overrides_the_type_table() {
        let sql = "INSERT INTO docs (id, name, vec) values {rows}";
        assert_eq!(
            param_json_schema(&DataType::Utf8, sql, "rows"),
            json!({"type": "array", "items": {"type": "array"}})
        );
        // only the parameter named in the VALUES clause gets the override
        assert_eq!(
            param_json_schema(&DataType::Utf8, sql, "other"),
            json!({"type": ["string", "null"]})
        );
        // a parenthesized tuple list is not the multi-row shape
        assert_eq!(
            param_json_schema(&DataType::Utf8, "INSERT INTO t VALUES ({rows})", "rows"),
            json!({"type": ["string", "null"]})
        );
    }
}
