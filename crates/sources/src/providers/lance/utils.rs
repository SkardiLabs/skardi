//! Shared utilities for converting DataFusion expressions to Lance SQL filter strings.

use datafusion::common::ScalarValue;
use datafusion::logical_expr::Expr;

/// Convert a DataFusion Expr to a plain SQL string for Lance's filter parser.
///
/// DataFusion's `Expr::to_string()` emits type-annotated literals (e.g. `Utf8("foo")`,
/// `Float64(3.14)`) that Lance cannot parse. This function walks the Expr tree directly
/// and produces standard SQL without type annotations.
pub fn expr_to_lance_sql(expr: &Expr) -> String {
    match expr {
        Expr::Column(col) => col.name.clone(),

        Expr::Literal(scalar, _) => scalar_to_sql(scalar),

        Expr::BinaryExpr(binary) => {
            let left = expr_to_lance_sql(&binary.left);
            let right = expr_to_lance_sql(&binary.right);
            format!("{left} {op} {right}", op = binary.op)
        }

        Expr::Not(inner) => format!("NOT ({})", expr_to_lance_sql(inner)),

        Expr::IsNull(inner) => format!("{} IS NULL", expr_to_lance_sql(inner)),

        Expr::IsNotNull(inner) => format!("{} IS NOT NULL", expr_to_lance_sql(inner)),

        Expr::Between(between) => {
            let expr_sql = expr_to_lance_sql(&between.expr);
            let low = expr_to_lance_sql(&between.low);
            let high = expr_to_lance_sql(&between.high);
            if between.negated {
                format!("{expr_sql} NOT BETWEEN {low} AND {high}")
            } else {
                format!("{expr_sql} BETWEEN {low} AND {high}")
            }
        }

        Expr::InList(in_list) => {
            let expr_sql = expr_to_lance_sql(&in_list.expr);
            let values: Vec<String> = in_list.list.iter().map(|e| expr_to_lance_sql(e)).collect();
            let list = values.join(", ");
            if in_list.negated {
                format!("{expr_sql} NOT IN ({list})")
            } else {
                format!("{expr_sql} IN ({list})")
            }
        }

        // Fallback: use Display (may contain type annotations, but covers edge cases)
        other => other.to_string(),
    }
}

/// Convert a DataFusion ScalarValue to a plain SQL literal string.
fn scalar_to_sql(scalar: &ScalarValue) -> String {
    match scalar {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            format!("'{}'", s.replace('\'', "''"))
        }
        ScalarValue::Boolean(Some(b)) => b.to_string(),
        ScalarValue::Int8(Some(n)) => n.to_string(),
        ScalarValue::Int16(Some(n)) => n.to_string(),
        ScalarValue::Int32(Some(n)) => n.to_string(),
        ScalarValue::Int64(Some(n)) => n.to_string(),
        ScalarValue::UInt8(Some(n)) => n.to_string(),
        ScalarValue::UInt16(Some(n)) => n.to_string(),
        ScalarValue::UInt32(Some(n)) => n.to_string(),
        ScalarValue::UInt64(Some(n)) => n.to_string(),
        ScalarValue::Float32(Some(n)) => n.to_string(),
        ScalarValue::Float64(Some(n)) => n.to_string(),
        ScalarValue::Null => "NULL".to_string(),
        // Fallback for other types
        other => other.to_string(),
    }
}
