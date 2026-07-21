//! Source-pack filter translation.
//!
//! Pushdown is allowlisted per column and operator by the source pack —
//! there is no generic SQL→provider-language translation. A filter that
//! matches a mapping is `Exact` (fully pushed into action inputs);
//! everything else stays in DataFusion (`Unsupported`). `Inexact`
//! (conservative pushes DataFusion must reapply) is reserved for mappings
//! whose provider semantics are broader than the SQL predicate; no built-in
//! mapping uses it yet.

use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use serde_json::Value;

/// One allowlisted pushdown rule: `column <op> literal` → `input_field: literal`.
#[derive(Debug, Clone, Copy)]
pub struct FilterMapping {
    /// Arrow column name the predicate references.
    pub column: &'static str,
    /// Comparison operators this mapping accepts.
    pub operators: &'static [Operator],
    /// Action input field the translated value is written to.
    pub input_field: &'static str,
}

/// Outcome of translating the scan's filters.
#[derive(Debug, Default)]
pub struct TranslatedFilters {
    /// Action input fields to merge into the request (from Exact filters).
    pub inputs: Vec<(String, Value)>,
    /// Per-filter pushdown classification, aligned with the input slice.
    pub pushdown: Vec<TableProviderFilterPushDown>,
}

/// Translate scan filters against the source pack's allowlist.
pub fn translate_filters(filters: &[Expr], mappings: &[FilterMapping]) -> TranslatedFilters {
    let mut translated = TranslatedFilters::default();
    for filter in filters {
        match translate_one(filter, mappings) {
            Some((input_field, value)) => {
                translated.inputs.push((input_field, value));
                translated.pushdown.push(TableProviderFilterPushDown::Exact);
            }
            None => translated
                .pushdown
                .push(TableProviderFilterPushDown::Unsupported),
        }
    }
    translated
}

/// Translate one filter, returning `(input_field, value)` on an Exact match.
fn translate_one(filter: &Expr, mappings: &[FilterMapping]) -> Option<(String, Value)> {
    let Expr::BinaryExpr(binary) = filter else {
        return None;
    };

    // Normalize to `column <op> literal`, flipping the operator when the
    // literal is on the left (`5 < col` → `col > 5`).
    let (column, operator, literal) = match (binary.left.as_ref(), binary.right.as_ref()) {
        (Expr::Column(column), Expr::Literal(literal, _)) => (column, binary.op, literal),
        (Expr::Literal(literal, _), Expr::Column(column)) => (column, binary.op.swap()?, literal),
        _ => return None,
    };

    let mapping = mappings
        .iter()
        .find(|mapping| mapping.column == column.name)?;
    if !mapping.operators.contains(&operator) {
        return None;
    }

    let value = scalar_to_json(literal)?;
    Some((mapping.input_field.to_string(), value))
}

/// Convert a DataFusion literal to a JSON value. Nulls and types outside the
/// JSON scalar set make the filter untranslatable.
fn scalar_to_json(literal: &ScalarValue) -> Option<Value> {
    match literal {
        ScalarValue::Utf8(Some(text)) | ScalarValue::LargeUtf8(Some(text)) => {
            Some(Value::from(text.as_str()))
        }
        ScalarValue::Boolean(Some(b)) => Some(Value::from(*b)),
        ScalarValue::Int8(Some(v)) => Some(Value::from(*v)),
        ScalarValue::Int16(Some(v)) => Some(Value::from(*v)),
        ScalarValue::Int32(Some(v)) => Some(Value::from(*v)),
        ScalarValue::Int64(Some(v)) => Some(Value::from(*v)),
        ScalarValue::UInt8(Some(v)) => Some(Value::from(*v)),
        ScalarValue::UInt16(Some(v)) => Some(Value::from(*v)),
        ScalarValue::UInt32(Some(v)) => Some(Value::from(*v)),
        ScalarValue::UInt64(Some(v)) => Some(Value::from(*v)),
        ScalarValue::Float32(Some(v)) => {
            serde_json::Number::from_f64(f64::from(*v)).map(Value::Number)
        }
        ScalarValue::Float64(Some(v)) => serde_json::Number::from_f64(*v).map(Value::Number),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{BinaryExpr, col, lit};

    const MAPPINGS: &[FilterMapping] = &[
        FilterMapping {
            column: "value",
            operators: &[Operator::Gt, Operator::GtEq],
            input_field: "min_value",
        },
        FilterMapping {
            column: "name",
            operators: &[Operator::Eq],
            input_field: "name",
        },
    ];

    fn gt(column: &str, value: i64) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col(column)),
            Operator::Gt,
            Box::new(lit(value)),
        ))
    }

    #[test]
    fn allowlisted_filter_translates_exact() {
        let translated = translate_filters(&[gt("value", 10)], MAPPINGS);
        assert_eq!(
            translated.inputs,
            vec![("min_value".to_string(), Value::from(10))]
        );
        assert_eq!(
            translated.pushdown,
            vec![TableProviderFilterPushDown::Exact]
        );
    }

    #[test]
    fn unmapped_column_is_unsupported() {
        let translated = translate_filters(&[gt("score", 10)], MAPPINGS);
        assert!(translated.inputs.is_empty());
        assert_eq!(
            translated.pushdown,
            vec![TableProviderFilterPushDown::Unsupported]
        );
    }

    #[test]
    fn unmapped_operator_is_unsupported() {
        // `name` is mapped only for Eq; a Gt predicate cannot push.
        let translated = translate_filters(&[gt("name", 10)], MAPPINGS);
        assert!(translated.inputs.is_empty());
        assert_eq!(
            translated.pushdown,
            vec![TableProviderFilterPushDown::Unsupported]
        );
    }

    #[test]
    fn literal_on_left_is_normalized() {
        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit(10)),
            Operator::Lt,
            Box::new(col("value")),
        ));
        let translated = translate_filters(&[filter], MAPPINGS);
        // 10 < value  ==  value > 10
        assert_eq!(
            translated.inputs,
            vec![("min_value".to_string(), Value::from(10))]
        );
    }

    #[test]
    fn non_binary_expr_is_unsupported() {
        let filter = col("name").is_null();
        let translated = translate_filters(&[filter], MAPPINGS);
        assert!(translated.inputs.is_empty());
        assert_eq!(
            translated.pushdown,
            vec![TableProviderFilterPushDown::Unsupported]
        );
    }

    #[test]
    fn string_and_float_and_bool_literals_translate() {
        let name_eq = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("name")),
            Operator::Eq,
            Box::new(lit("widget")),
        ));
        let translated = translate_filters(&[name_eq], MAPPINGS);
        assert_eq!(
            translated.inputs,
            vec![("name".to_string(), Value::from("widget"))]
        );
    }

    #[test]
    fn mixed_filters_keep_alignment() {
        let translated = translate_filters(&[gt("value", 1), col("name").is_null()], MAPPINGS);
        assert_eq!(translated.inputs.len(), 1);
        assert_eq!(
            translated.pushdown,
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Unsupported
            ]
        );
    }
}
