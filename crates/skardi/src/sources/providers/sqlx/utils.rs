//! Shared utilities for converting DataFusion expressions to PostgreSQL SQL strings.

use datafusion::common::ScalarValue;
use datafusion::logical_expr::Expr;

/// Convert a DataFusion `Expr` to a plain PostgreSQL SQL string suitable for
/// use in a WHERE clause pushed down to Postgres.
///
/// Column names are double-quoted to preserve case and avoid keyword conflicts.
pub fn expr_to_pg_sql(expr: &Expr) -> String {
    match expr {
        Expr::Column(col) => format!("\"{}\"", col.name.replace('"', "\"\"")),

        Expr::Literal(scalar, _) => scalar_to_pg_sql(scalar),

        Expr::BinaryExpr(binary) => {
            let left = expr_to_pg_sql(&binary.left);
            let right = expr_to_pg_sql(&binary.right);
            format!("({left} {op} {right})", op = binary.op)
        }

        Expr::Not(inner) => format!("NOT ({})", expr_to_pg_sql(inner)),

        Expr::IsNull(inner) => format!("{} IS NULL", expr_to_pg_sql(inner)),

        Expr::IsNotNull(inner) => format!("{} IS NOT NULL", expr_to_pg_sql(inner)),

        Expr::Between(between) => {
            let expr_sql = expr_to_pg_sql(&between.expr);
            let low = expr_to_pg_sql(&between.low);
            let high = expr_to_pg_sql(&between.high);
            if between.negated {
                format!("{expr_sql} NOT BETWEEN {low} AND {high}")
            } else {
                format!("{expr_sql} BETWEEN {low} AND {high}")
            }
        }

        Expr::InList(in_list) => {
            let expr_sql = expr_to_pg_sql(&in_list.expr);
            let values: Vec<String> = in_list.list.iter().map(|e| expr_to_pg_sql(e)).collect();
            let list = values.join(", ");
            if in_list.negated {
                format!("{expr_sql} NOT IN ({list})")
            } else {
                format!("{expr_sql} IN ({list})")
            }
        }

        // Fallback: DataFusion's Display (may have type annotations, but covers edge cases)
        other => other.to_string(),
    }
}

fn scalar_to_pg_sql(scalar: &ScalarValue) -> String {
    match scalar {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            format!("'{}'", s.replace('\'', "''"))
        }
        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => "NULL".to_string(),
        ScalarValue::Int8(Some(v)) => v.to_string(),
        ScalarValue::Int16(Some(v)) => v.to_string(),
        ScalarValue::Int32(Some(v)) => v.to_string(),
        ScalarValue::Int64(Some(v)) => v.to_string(),
        ScalarValue::UInt8(Some(v)) => v.to_string(),
        ScalarValue::UInt16(Some(v)) => v.to_string(),
        ScalarValue::UInt32(Some(v)) => v.to_string(),
        ScalarValue::UInt64(Some(v)) => v.to_string(),
        ScalarValue::Float32(Some(v)) => v.to_string(),
        ScalarValue::Float64(Some(v)) => v.to_string(),
        ScalarValue::Boolean(Some(v)) => if *v { "TRUE" } else { "FALSE" }.to_string(),
        ScalarValue::Boolean(None)
        | ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None)
        | ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None)
        | ScalarValue::Float32(None)
        | ScalarValue::Float64(None) => "NULL".to_string(),
        other => other.to_string(),
    }
}
