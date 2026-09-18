//! Source-pack filter translation.
//!
//! Pushdown is allowlisted per column and operator by the source pack —
//! there is no generic SQL→provider-language translation. A filter that
//! matches a mapping is pushed into action inputs and classified by the
//! mapping's declared [`Fidelity`]: `Exact` mappings are fully handled by
//! the provider, `Inexact` mappings narrow the fetch but DataFusion
//! reapplies the predicate locally. Everything else stays entirely in
//! DataFusion (`Unsupported`).

use std::collections::HashSet;

use chrono::{DateTime, SecondsFormat};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr, Operator as SqlOperator, TableProviderFilterPushDown};

// The DECLARATIONS now live in the pack; what stays here is the
// translation — the half that needs an `Expr`. `SqlOperator` is
// DataFusion's; `Operator` is the pack's neutral one a mapping declares,
// and `sql_operator` below is the single place the two meet.
use serde_json::Value;
pub use skardi_source_pack::filters::{Fidelity, FilterMapping, Operator, ValueFormat};

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
    let mut claimed_inputs = HashSet::new();
    for filter in filters {
        match translate_one(filter, mappings) {
            // An action input holds one value. Marking two predicates that
            // target it as pushed would let the later insert overwrite the
            // former while DataFusion skips reapplying an Exact one.
            Some((input_field, value, fidelity)) if claimed_inputs.insert(input_field.clone()) => {
                translated.inputs.push((input_field, value));
                translated.pushdown.push(match fidelity {
                    Fidelity::Exact => TableProviderFilterPushDown::Exact,
                    Fidelity::Inexact => TableProviderFilterPushDown::Inexact,
                });
            }
            Some(_) | None => translated
                .pushdown
                .push(TableProviderFilterPushDown::Unsupported),
        }
    }
    translated
}

/// Translate one filter, returning `(input_field, value, fidelity)` on a match.
fn translate_one(filter: &Expr, mappings: &[FilterMapping]) -> Option<(String, Value, Fidelity)> {
    // DataFusion's simplifier rewrites boolean equality to the bare
    // column (`completed = true` → `completed`) or its negation
    // (`completed = false` → `NOT completed`) before pushdown, so an Eq
    // mapping on a boolean column would otherwise never fire. Normalize
    // both spellings back to the equality they mean.
    //
    // Invariant: these two arms fabricate a Boolean literal without
    // checking the mapped column's declared type, because only a
    // boolean-typed column can appear as a bare predicate in a
    // type-checked DataFusion plan — an Eq mapping on a non-boolean
    // column is reachable solely through the BinaryExpr arm below, where
    // the literal comes from the query.
    match filter {
        Expr::Column(column) => {
            let mapping = mappings.iter().find(|mapping| {
                mapping.column == column.name && mapping.operator == Operator::Eq
            })?;
            let value = scalar_to_json(&ScalarValue::Boolean(Some(true)), mapping.value_format)?;
            return Some((mapping.input_field.to_string(), value, mapping.fidelity));
        }
        Expr::Not(inner) => {
            let Expr::Column(column) = inner.as_ref() else {
                return None;
            };
            let mapping = mappings.iter().find(|mapping| {
                mapping.column == column.name && mapping.operator == Operator::Eq
            })?;
            let value = scalar_to_json(&ScalarValue::Boolean(Some(false)), mapping.value_format)?;
            return Some((mapping.input_field.to_string(), value, mapping.fidelity));
        }
        _ => {}
    }

    let Expr::BinaryExpr(binary) = filter else {
        return None;
    };

    // Normalize to `column <op> literal`, flipping the operator when the
    // literal is on the left (`5 < col` → `col > 5`). A cast around the
    // *column* is never matched — `CAST(updated_at AS DATE) >= …` changes
    // the predicate's semantics and must stay in DataFusion.
    let (column, operator, literal) = match (binary.left.as_ref(), binary.right.as_ref()) {
        (Expr::Column(column), right) => (column, binary.op, resolve_literal(right)?),
        (left, Expr::Column(column)) => (column, binary.op.swap()?, resolve_literal(left)?),
        _ => return None,
    };

    let mapping = mappings.iter().find(|mapping| {
        // The one place the two operator vocabularies meet: the
        // predicate's is DataFusion's, the mapping's is the pack's
        // neutral declaration. An operator no pack declares yields
        // `None` and simply does not push down, which is always
        // safe — DataFusion re-evaluates what we do not translate.
        mapping.column == column.name && declared_operator(operator) == Some(mapping.operator)
    })?;

    let value = scalar_to_json(&literal, mapping.value_format)?;
    Some((mapping.input_field.to_string(), value, mapping.fidelity))
}

/// Resolve the literal side of a predicate, folding literal-only `CAST` /
/// `TRY_CAST` wrappers with the same Arrow cast kernel the engine would use.
/// Type coercion wraps literals compared against typed columns (e.g.
/// `updated_at >= '2026-01-01'` becomes a cast to timestamp), and such casts
/// can survive into the pushdown filters. Evaluating the cast — rather than
/// stripping it — keeps Exact semantics exact: `CAST('10' AS DOUBLE)` pushes
/// the number 10, never the string `"10"`. A failing cast or a non-literal
/// operand returns None (→ Unsupported, evaluated locally).
fn resolve_literal(expr: &Expr) -> Option<ScalarValue> {
    match expr {
        Expr::Literal(scalar, _) => Some(scalar.clone()),
        Expr::Cast(cast) => resolve_literal(&cast.expr)?.cast_to(&cast.data_type).ok(),
        Expr::TryCast(cast) => resolve_literal(&cast.expr)?.cast_to(&cast.data_type).ok(),
        _ => None,
    }
}

/// Convert a DataFusion literal to a JSON value. Nulls and types outside the
/// JSON scalar set make the filter untranslatable.
///
/// Timestamps render per the mapping's [`ValueFormat`]. The scalar's epoch
/// value is absolute, so any timezone annotation only affects display; a
/// naive (timezone-less) literal is treated as UTC, matching the engine's
/// `TimestampMillisUtc` column semantics.
fn scalar_to_json(literal: &ScalarValue, format: ValueFormat) -> Option<Value> {
    match literal {
        // Utf8View included: DataFusion 52 can carry string literals as view
        // scalars after coercion, and a missed match here silently demotes an
        // Exact pushdown to a local re-filter over the full fetch. (There is
        // no LargeUtf8View — view types have no Large variants.)
        ScalarValue::Utf8(Some(text))
        | ScalarValue::LargeUtf8(Some(text))
        | ScalarValue::Utf8View(Some(text)) => Some(Value::from(text.as_str())),
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
        // Verbatim arms return None on purpose: the mapping declared no
        // timestamp spelling, so the predicate stays local rather than
        // being pushed in a guessed format.
        // Epoch renderings clamp pre-epoch instants to 0 (see the
        // ValueFormat docs): a negative would render with a `-` sign that
        // strict digit-string schemas 400 on, failing the whole scan where
        // no pushdown at all would have succeeded — and 0 is still a
        // superset for the lower bounds these formats are restricted to.
        ScalarValue::TimestampSecond(Some(v), _) => match format {
            ValueFormat::Verbatim => None,
            ValueFormat::Rfc3339 => DateTime::from_timestamp(*v, 0).map(rfc3339),
            ValueFormat::EpochSeconds => Some(Value::from((*v).max(0))),
            ValueFormat::EpochSecondsString => Some(Value::from((*v).max(0).to_string())),
        },
        ScalarValue::TimestampMillisecond(Some(v), _) => match format {
            ValueFormat::Verbatim => None,
            ValueFormat::Rfc3339 => DateTime::from_timestamp_millis(*v).map(rfc3339),
            ValueFormat::EpochSeconds => Some(Value::from(v.div_euclid(1000).max(0))),
            ValueFormat::EpochSecondsString => {
                Some(Value::from(v.div_euclid(1000).max(0).to_string()))
            }
        },
        ScalarValue::TimestampMicrosecond(Some(v), _) => match format {
            ValueFormat::Verbatim => None,
            ValueFormat::Rfc3339 => DateTime::from_timestamp_micros(*v).map(rfc3339),
            ValueFormat::EpochSeconds => Some(Value::from(v.div_euclid(1_000_000).max(0))),
            ValueFormat::EpochSecondsString => {
                Some(Value::from(v.div_euclid(1_000_000).max(0).to_string()))
            }
        },
        ScalarValue::TimestampNanosecond(Some(v), _) => match format {
            ValueFormat::Verbatim => None,
            ValueFormat::Rfc3339 => Some(rfc3339(DateTime::from_timestamp_nanos(*v))),
            ValueFormat::EpochSeconds => Some(Value::from(v.div_euclid(1_000_000_000).max(0))),
            ValueFormat::EpochSecondsString => {
                Some(Value::from(v.div_euclid(1_000_000_000).max(0).to_string()))
            }
        },
        _ => None,
    }
}

/// Render an epoch instant as an RFC 3339 UTC string, with subsecond digits
/// only when the value has them.
fn rfc3339(instant: DateTime<chrono::Utc>) -> Value {
    Value::from(instant.to_rfc3339_opts(SecondsFormat::AutoSi, true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{BinaryExpr, col, lit};

    const MAPPINGS: &[FilterMapping] = &[
        FilterMapping {
            column: "value",
            operator: Operator::Gt,
            input_field: "min_value",
            fidelity: Fidelity::Exact,
            value_format: ValueFormat::Verbatim,
        },
        FilterMapping {
            column: "value",
            operator: Operator::GtEq,
            input_field: "min_value_inclusive",
            fidelity: Fidelity::Exact,
            value_format: ValueFormat::Verbatim,
        },
        FilterMapping {
            column: "name",
            operator: Operator::Eq,
            input_field: "name",
            fidelity: Fidelity::Exact,
            value_format: ValueFormat::Verbatim,
        },
        FilterMapping {
            column: "updated_at",
            operator: Operator::GtEq,
            input_field: "since",
            fidelity: Fidelity::Inexact,
            value_format: ValueFormat::Rfc3339,
        },
        FilterMapping {
            column: "created",
            operator: Operator::GtEq,
            input_field: "ts_from",
            fidelity: Fidelity::Inexact,
            value_format: ValueFormat::EpochSeconds,
        },
        FilterMapping {
            column: "create_time",
            operator: Operator::GtEq,
            input_field: "startTime",
            fidelity: Fidelity::Inexact,
            value_format: ValueFormat::EpochSecondsString,
        },
        FilterMapping {
            column: "completed",
            operator: Operator::Eq,
            input_field: "completed",
            fidelity: Fidelity::Inexact,
            value_format: ValueFormat::Verbatim,
        },
    ];

    fn gt(column: &str, value: i64) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col(column)),
            SqlOperator::Gt,
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
    fn per_operator_mapping_routes_to_the_right_input() {
        // Same column, different operator → the per-operator mapping picks
        // the input that faithfully represents THIS operator.
        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("value")),
            SqlOperator::GtEq,
            Box::new(lit(10)),
        ));
        let translated = translate_filters(&[filter], MAPPINGS);
        assert_eq!(
            translated.inputs,
            vec![("min_value_inclusive".to_string(), Value::from(10))]
        );
        assert_eq!(
            translated.pushdown,
            vec![TableProviderFilterPushDown::Exact]
        );
    }

    #[test]
    fn range_composes_two_mappings_on_one_column() {
        // The scenario behind one-operator-per-mapping: Gt → min_value and
        // Lt → max_value on the SAME column must both resolve — resolution
        // is by (column, operator), so the second mapping is reachable.
        //
        // It used to use `Gt` and `Lt`. `Lt` is not in the YAML asset grammar
        // — `OpDoc` is eq/gt/gt_eq — so no pack could ever have declared that
        // mapping: the engine's type was wider than the format it loads, and
        // the neutral `Operator` closes the gap. Two operators a pack CAN
        // declare prove the same thing.
        const RANGE: &[FilterMapping] = &[
            FilterMapping {
                column: "value",
                operator: Operator::Gt,
                input_field: "after_value",
                fidelity: Fidelity::Exact,
                value_format: ValueFormat::Rfc3339,
            },
            FilterMapping {
                column: "value",
                operator: Operator::GtEq,
                input_field: "from_value",
                fidelity: Fidelity::Exact,
                value_format: ValueFormat::Rfc3339,
            },
        ];
        let gt = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("value")),
            SqlOperator::Gt,
            Box::new(lit(1)),
        ));
        let gt_eq = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("value")),
            SqlOperator::GtEq,
            Box::new(lit(5)),
        ));
        let translated = translate_filters(&[gt, gt_eq], RANGE);
        assert_eq!(
            translated.inputs,
            vec![
                ("after_value".to_string(), Value::from(1)),
                ("from_value".to_string(), Value::from(5)),
            ]
        );
        assert_eq!(
            translated.pushdown,
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Exact
            ]
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
            SqlOperator::Lt,
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
            SqlOperator::Eq,
            Box::new(lit("widget")),
        ));
        let translated = translate_filters(&[name_eq], MAPPINGS);
        assert_eq!(
            translated.inputs,
            vec![("name".to_string(), Value::from("widget"))]
        );

        // DataFusion 52 can coerce string literals into view scalars; a
        // missed match would silently demote the Exact pushdown to a local
        // re-filter over the full fetch.
        for scalar in [
            ScalarValue::LargeUtf8(Some("widget".to_string())),
            ScalarValue::Utf8View(Some("widget".to_string())),
        ] {
            let name_eq = Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("name")),
                SqlOperator::Eq,
                Box::new(Expr::Literal(scalar, None)),
            ));
            let translated = translate_filters(&[name_eq], MAPPINGS);
            assert_eq!(
                translated.inputs,
                vec![("name".to_string(), Value::from("widget"))],
            );
            assert_eq!(
                translated.pushdown,
                vec![TableProviderFilterPushDown::Exact]
            );
        }
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

    #[test]
    fn inexact_mapping_pushes_input_but_keeps_the_filter_local() {
        // The Inexact contract: the provider input narrows the fetch, and
        // the Inexact classification makes DataFusion reapply the predicate
        // — so a provider returning a superset can never leak wrong rows.
        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("updated_at")),
            SqlOperator::GtEq,
            Box::new(lit("2026-01-01T00:00:00Z")),
        ));
        let translated = translate_filters(&[filter], MAPPINGS);
        assert_eq!(
            translated.inputs,
            vec![("since".to_string(), Value::from("2026-01-01T00:00:00Z"))]
        );
        assert_eq!(
            translated.pushdown,
            vec![TableProviderFilterPushDown::Inexact]
        );
    }

    #[test]
    fn timestamp_literals_render_as_rfc3339_utc() {
        // GitHub's `since` takes an ISO 8601 instant; every timestamp
        // granularity DataFusion may coerce to must render the same way.
        let epoch_ms = 1_767_225_600_000i64; // 2026-01-01T00:00:00Z
        for scalar in [
            ScalarValue::TimestampSecond(Some(epoch_ms / 1000), None),
            ScalarValue::TimestampMillisecond(Some(epoch_ms), Some("UTC".into())),
            ScalarValue::TimestampMicrosecond(Some(epoch_ms * 1000), None),
            ScalarValue::TimestampNanosecond(Some(epoch_ms * 1_000_000), None),
        ] {
            let filter = Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("updated_at")),
                SqlOperator::GtEq,
                Box::new(Expr::Literal(scalar, None)),
            ));
            let translated = translate_filters(&[filter], MAPPINGS);
            assert_eq!(
                translated.inputs,
                vec![("since".to_string(), Value::from("2026-01-01T00:00:00Z"))],
            );
        }

        // Sub-second precision is preserved, not truncated away.
        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("updated_at")),
            SqlOperator::GtEq,
            Box::new(Expr::Literal(
                ScalarValue::TimestampMillisecond(Some(epoch_ms + 250), None),
                None,
            )),
        ));
        let translated = translate_filters(&[filter], MAPPINGS);
        assert_eq!(
            translated.inputs,
            vec![("since".to_string(), Value::from("2026-01-01T00:00:00.250Z"))],
        );
    }

    #[test]
    fn cast_wrapped_literals_are_folded_before_translation() {
        use arrow::datatypes::{DataType, TimeUnit};
        use datafusion::logical_expr::{Cast, TryCast};

        // Type coercion's shape for `updated_at >= '2026-01-01T00:00:00Z'`:
        // the string literal arrives wrapped in a cast to the column's
        // timestamp type.
        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("updated_at")),
            SqlOperator::GtEq,
            Box::new(Expr::Cast(Cast::new(
                Box::new(lit("2026-01-01T00:00:00Z")),
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            ))),
        ));
        let translated = translate_filters(&[filter], MAPPINGS);
        assert_eq!(
            translated.inputs,
            vec![("since".to_string(), Value::from("2026-01-01T00:00:00Z"))]
        );
        assert_eq!(
            translated.pushdown,
            vec![TableProviderFilterPushDown::Inexact]
        );

        // The cast is EVALUATED, not stripped: a numeric cast pushes the
        // JSON number, never the inner string — stripping would corrupt an
        // Exact pushdown with a wrongly-typed provider input.
        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("value")),
            SqlOperator::Gt,
            Box::new(Expr::TryCast(TryCast::new(
                Box::new(lit("10")),
                DataType::Float64,
            ))),
        ));
        let translated = translate_filters(&[filter], MAPPINGS);
        assert_eq!(
            translated.inputs,
            vec![("min_value".to_string(), Value::from(10.0))]
        );
        assert_eq!(
            translated.pushdown,
            vec![TableProviderFilterPushDown::Exact]
        );
    }

    #[test]
    fn unfoldable_and_column_side_casts_stay_local() {
        use arrow::datatypes::{DataType, TimeUnit};
        use datafusion::logical_expr::Cast;

        let timestamp = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));

        // A cast that cannot evaluate must classify Unsupported (DataFusion
        // evaluates locally) — never push a garbled value.
        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("updated_at")),
            SqlOperator::GtEq,
            Box::new(Expr::Cast(Cast::new(
                Box::new(lit("not a timestamp")),
                timestamp.clone(),
            ))),
        ));
        let translated = translate_filters(&[filter], MAPPINGS);
        assert!(translated.inputs.is_empty());
        assert_eq!(
            translated.pushdown,
            vec![TableProviderFilterPushDown::Unsupported]
        );

        // A cast around the COLUMN changes the predicate's semantics
        // (`CAST(updated_at AS DATE) >= …` truncates); it must never match.
        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Cast(Cast::new(
                Box::new(col("updated_at")),
                timestamp,
            ))),
            SqlOperator::GtEq,
            Box::new(lit("2026-01-01T00:00:00Z")),
        ));
        let translated = translate_filters(&[filter], MAPPINGS);
        assert!(translated.inputs.is_empty());
        assert_eq!(
            translated.pushdown,
            vec![TableProviderFilterPushDown::Unsupported]
        );
    }

    #[test]
    fn epoch_seconds_mappings_render_whole_seconds_and_floor_lower_bounds() {
        // Slack-style ts_from: every timestamp granularity renders as whole
        // epoch seconds, and sub-second literals floor — widening the lower
        // bound (superset), which Inexact re-filtering trims back.
        let epoch_ms = 1_767_225_600_000i64; // 2026-01-01T00:00:00Z
        for scalar in [
            ScalarValue::TimestampSecond(Some(epoch_ms / 1000), None),
            ScalarValue::TimestampMillisecond(Some(epoch_ms), Some("UTC".into())),
            ScalarValue::TimestampMicrosecond(Some(epoch_ms * 1000), None),
            ScalarValue::TimestampNanosecond(Some(epoch_ms * 1_000_000), None),
            // Sub-second precision floors.
            ScalarValue::TimestampMillisecond(Some(epoch_ms + 999), None),
        ] {
            let filter = Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("created")),
                SqlOperator::GtEq,
                Box::new(Expr::Literal(scalar, None)),
            ));
            let translated = translate_filters(&[filter], MAPPINGS);
            assert_eq!(
                translated.inputs,
                vec![("ts_from".to_string(), Value::from(1_767_225_600i64))],
            );
            assert_eq!(
                translated.pushdown,
                vec![TableProviderFilterPushDown::Inexact]
            );
        }
    }

    #[test]
    fn simplified_boolean_predicates_normalize_back_to_equality() {
        // DataFusion's simplifier hands pushdown `completed` instead of
        // `completed = true` and `NOT completed` instead of
        // `completed = false`; both must still reach the mapped input —
        // otherwise a boolean Eq mapping is dead code.
        let translated = translate_filters(&[col("completed")], MAPPINGS);
        assert_eq!(
            translated.inputs,
            vec![("completed".to_string(), Value::from(true))]
        );
        assert_eq!(
            translated.pushdown,
            vec![TableProviderFilterPushDown::Inexact]
        );

        let translated = translate_filters(&[Expr::Not(Box::new(col("completed")))], MAPPINGS);
        assert_eq!(
            translated.inputs,
            vec![("completed".to_string(), Value::from(false))]
        );

        // A bare column without an Eq mapping stays local — and NOT over
        // anything but a bare column is never translated.
        let translated = translate_filters(&[col("value")], MAPPINGS);
        assert!(translated.inputs.is_empty());
        assert_eq!(
            translated.pushdown,
            vec![TableProviderFilterPushDown::Unsupported]
        );
        let translated = translate_filters(&[Expr::Not(Box::new(gt("value", 5)))], MAPPINGS);
        assert!(translated.inputs.is_empty());
    }

    #[test]
    fn pre_epoch_instants_clamp_to_zero_never_a_signed_string() {
        // `create_time >= '1965-01-01'` would otherwise render "-157766400"
        // — not a digit string, so a strict provider schema (Feishu's
        // startTime) 400s and the whole scan fails where no pushdown at
        // all would have succeeded. Zero is still a superset for the
        // lower bounds these formats are restricted to.
        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("create_time")),
            SqlOperator::GtEq,
            Box::new(Expr::Literal(
                ScalarValue::TimestampMillisecond(Some(-157_766_400_000), Some("UTC".into())),
                None,
            )),
        ));
        let translated = translate_filters(&[filter], MAPPINGS);
        assert_eq!(
            translated.inputs,
            vec![("startTime".to_string(), Value::from("0"))],
        );
        assert_eq!(
            translated.pushdown,
            vec![TableProviderFilterPushDown::Inexact]
        );
    }

    #[test]
    fn epoch_seconds_string_mappings_render_digit_strings() {
        // Feishu-style startTime: same flooring semantics as epoch_seconds,
        // but the provider's strict schema types the input as a STRING —
        // the rendered literal must be a JSON string of digits, never a
        // number.
        let epoch_ms = 1_767_225_600_000i64; // 2026-01-01T00:00:00Z
        for scalar in [
            ScalarValue::TimestampSecond(Some(epoch_ms / 1000), None),
            ScalarValue::TimestampMillisecond(Some(epoch_ms), Some("UTC".into())),
            ScalarValue::TimestampMicrosecond(Some(epoch_ms * 1000), None),
            ScalarValue::TimestampNanosecond(Some(epoch_ms * 1_000_000), None),
            // Sub-second precision floors — widening the lower bound,
            // which Inexact re-filtering trims back.
            ScalarValue::TimestampMillisecond(Some(epoch_ms + 999), None),
        ] {
            let filter = Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("create_time")),
                SqlOperator::GtEq,
                Box::new(Expr::Literal(scalar, None)),
            ));
            let translated = translate_filters(&[filter], MAPPINGS);
            assert_eq!(
                translated.inputs,
                vec![("startTime".to_string(), Value::from("1767225600"))],
            );
            assert_eq!(
                translated.pushdown,
                vec![TableProviderFilterPushDown::Inexact]
            );
        }
    }

    #[test]
    fn verbatim_mappings_push_plain_scalars_and_keep_timestamps_local() {
        // Non-timestamp literals through Verbatim mappings push their
        // natural JSON rendering — the fixture's numeric and string
        // mappings above all declare Verbatim and every other test rides
        // them. The distinctive arm: a timestamp literal reaching a
        // Verbatim mapping does NOT translate (no declared spelling means
        // no guessing), so the predicate stays local.
        let translated = translate_filters(&[gt("value", 20)], MAPPINGS);
        assert_eq!(
            translated.inputs,
            vec![("min_value".to_string(), Value::from(20))]
        );
        assert_eq!(
            translated.pushdown,
            vec![TableProviderFilterPushDown::Exact]
        );

        let verbatim_timestamp: &[FilterMapping] = &[FilterMapping {
            column: "created",
            operator: Operator::GtEq,
            input_field: "ts_from",
            fidelity: Fidelity::Inexact,
            value_format: ValueFormat::Verbatim,
        }];
        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("created")),
            SqlOperator::GtEq,
            Box::new(Expr::Literal(
                ScalarValue::TimestampMillisecond(Some(1_767_225_600_000), Some("UTC".into())),
                None,
            )),
        ));
        let translated = translate_filters(&[filter], verbatim_timestamp);
        assert!(
            translated.inputs.is_empty(),
            "a timestamp under Verbatim must not reach the wire: {:?}",
            translated.inputs
        );
        assert_eq!(
            translated.pushdown,
            vec![TableProviderFilterPushDown::Unsupported]
        );
    }

    #[test]
    fn duplicate_action_input_keeps_later_predicate_local() {
        let translated = translate_filters(&[gt("value", 20), gt("value", 10)], MAPPINGS);
        assert_eq!(
            translated.inputs,
            vec![("min_value".to_string(), Value::from(20))]
        );
        assert_eq!(
            translated.pushdown,
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Unsupported
            ]
        );
    }
}

/// The declared operator a DataFusion one corresponds to, or `None` when the
/// predicate is something no provider declares.
///
/// The only place the two vocabularies meet. A pack declares
/// [`skardi_source_pack::filters::Operator`] because a syncer must be able to
/// read a pack without a query planner; this function is how the engine asks
/// "is this `Expr`'s operator the one that mapping accepts".
///
/// Returning `None` rather than a fallback is deliberate: an unmapped operator
/// means no pushdown, which is always safe — DataFusion re-evaluates the
/// predicate. Guessing a near-match would be a wrong `Exact` claim, and a
/// wrong `Exact` claim silently drops rows.
///
/// # Examples
///
/// ```
/// use skardi::sources::providers::open_connector::filters::declared_operator;
/// use skardi_source_pack::filters::Operator;
/// use datafusion::logical_expr::Operator as SqlOperator;
///
/// assert_eq!(declared_operator(SqlOperator::Eq), Some(Operator::Eq));
///
/// // `None` is the important answer: a pack declares only the comparisons it
/// // can push down, and anything else must stay a DataFusion filter rather
/// // than be approximated by a near-match.
/// assert_eq!(declared_operator(SqlOperator::Lt), None);
/// ```
pub fn declared_operator(operator: SqlOperator) -> Option<Operator> {
    match operator {
        SqlOperator::Eq => Some(Operator::Eq),
        SqlOperator::Gt => Some(Operator::Gt),
        SqlOperator::GtEq => Some(Operator::GtEq),
        _ => None,
    }
}
