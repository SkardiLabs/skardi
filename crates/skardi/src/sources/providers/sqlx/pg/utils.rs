//! Shared utilities for PostgreSQL providers (pg_knn, pg_fts).

use arrow::array::{
    ArrayRef, BooleanBuilder, Decimal128Builder, Float32Builder, Float64Builder, Int32Builder,
    Int64Builder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::Expr;
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::PostgreSqlDialect;
use sqlx::Row;
use std::sync::Arc;

/// Try to convert a DataFusion `Expr` to a PostgreSQL SQL string suitable for
/// use in a WHERE clause pushed down to Postgres.
///
/// Returns `None` for expressions that cannot be reliably converted, so the
/// caller can skip pushdown for those filters rather than generating invalid SQL.
pub fn expr_to_pg_sql(expr: &Expr) -> Option<String> {
    let unparser = Unparser::new(&PostgreSqlDialect {});
    unparser.expr_to_sql(expr).ok().map(|ast| ast.to_string())
}

// ─── Row → RecordBatch conversion ───────────────────────────────────────────

/// Convert a slice of sqlx `PgRow`s into an Arrow `RecordBatch` matching the
/// given schema. Used by both `PgKnnExec` and `PgFtsExec`.
pub fn rows_to_batch(rows: &[sqlx::postgres::PgRow], schema: &SchemaRef) -> DFResult<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        columns.push(build_column(rows, field.name(), field.data_type())?);
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Build a single Arrow array column from the named column across all rows.
///
/// Supports Int32, Int64, Float32, Float64, Boolean, Decimal128 (cast via
/// float8), and a Utf8 catch-all for everything else.
pub fn build_column(
    rows: &[sqlx::postgres::PgRow],
    col: &str,
    dtype: &DataType,
) -> DFResult<ArrayRef> {
    /// Convert a sqlx error into a DataFusion execution error for a named column.
    #[inline]
    fn decode_err(col: &str, expected: &str, e: sqlx::Error) -> DataFusionError {
        DataFusionError::Execution(format!(
            "pg: type mismatch decoding column '{}' as {}: {}",
            col, expected, e
        ))
    }

    Ok(match dtype {
        DataType::Int32 => {
            let mut b = Int32Builder::new();
            for row in rows {
                b.append_option(
                    row.try_get::<Option<i32>, _>(col)
                        .map_err(|e| decode_err(col, "Int32", e))?,
                );
            }
            Arc::new(b.finish())
        }
        DataType::Int64 => {
            let mut b = Int64Builder::new();
            for row in rows {
                b.append_option(
                    row.try_get::<Option<i64>, _>(col)
                        .map_err(|e| decode_err(col, "Int64", e))?,
                );
            }
            Arc::new(b.finish())
        }
        DataType::Float32 => {
            let mut b = Float32Builder::new();
            for row in rows {
                b.append_option(
                    row.try_get::<Option<f32>, _>(col)
                        .map_err(|e| decode_err(col, "Float32", e))?,
                );
            }
            Arc::new(b.finish())
        }
        DataType::Float64 => {
            let mut b = Float64Builder::new();
            for row in rows {
                b.append_option(
                    row.try_get::<Option<f64>, _>(col)
                        .map_err(|e| decode_err(col, "Float64", e))?,
                );
            }
            Arc::new(b.finish())
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::new();
            for row in rows {
                b.append_option(
                    row.try_get::<Option<bool>, _>(col)
                        .map_err(|e| decode_err(col, "Boolean", e))?,
                );
            }
            Arc::new(b.finish())
        }
        // NUMERIC/DECIMAL: read as f64 (the SELECT casts to float8) then scale to i128.
        DataType::Decimal128(_, scale) => {
            let scale_factor = 10i128.pow(*scale as u32);
            let mut b = Decimal128Builder::new().with_data_type(dtype.clone());
            for row in rows {
                match row
                    .try_get::<Option<f64>, _>(col)
                    .map_err(|e| decode_err(col, "Decimal128 (via float8)", e))?
                {
                    Some(v) => b.append_value((v * scale_factor as f64).round() as i128),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        // Utf8 catch-all: text, varchar, uuid, json, timestamp, date, etc.
        // Decode failures are non-fatal here because schema inference maps many
        // Postgres types to Utf8 as a best effort; warn and emit null instead.
        _ => {
            let mut b = StringBuilder::new();
            for row in rows {
                match row.try_get::<Option<String>, _>(col) {
                    Ok(Some(s)) => b.append_value(&s),
                    Ok(None) => b.append_null(),
                    Err(e) => {
                        tracing::warn!(
                            column = col,
                            error = %e,
                            "pg: failed to decode column as Utf8, emitting null"
                        );
                        b.append_null();
                    }
                }
            }
            Arc::new(b.finish())
        }
    })
}
