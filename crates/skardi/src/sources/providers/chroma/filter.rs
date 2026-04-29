//! Translate DataFusion `Expr` filters into Chroma's typed `Where` enum.
//!
//! Chroma uses one unified `Where` for both metadata and document filters.
//! `Where::Metadata(...)` predicates target metadata fields; `Where::Document(...)`
//! targets the `document` text column. Multiple top-level filters combine with
//! `Where::conjunction(...)`.
//!
//! This is the Chroma analog of `mongo::binary_expr_to_mongo` — same shape, typed
//! output instead of BSON. Unsupported expressions return an error so the
//! TableProvider can fall back (we mark filters Inexact, so DataFusion re-filters
//! anyway).

use anyhow::{Result, anyhow, bail};
use chroma::types::{
    DocumentExpression, DocumentOperator, MetadataComparison, MetadataExpression, MetadataValue,
    PrimitiveOperator, Where,
};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};

const DOCUMENT_COL: &str = "document";

pub fn exprs_to_chroma_filter(filters: &[Expr]) -> Result<Option<Where>> {
    if filters.is_empty() {
        return Ok(None);
    }
    let mut parts = Vec::with_capacity(filters.len());
    for f in filters {
        parts.push(expr_to_where(f)?);
    }
    Ok(Some(if parts.len() == 1 {
        parts.into_iter().next().unwrap()
    } else {
        Where::conjunction(parts)
    }))
}

fn expr_to_where(expr: &Expr) -> Result<Where> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And => Ok(Where::conjunction(vec![
                expr_to_where(left)?,
                expr_to_where(right)?,
            ])),
            Operator::Or => Ok(Where::disjunction(vec![
                expr_to_where(left)?,
                expr_to_where(right)?,
            ])),
            _ => binary_compare_to_where(left, *op, right),
        },
        Expr::IsNotNull(_) | Expr::IsNull(_) => {
            bail!("chroma: NULL predicates are not supported by Chroma's Where DSL")
        }
        Expr::Like(like) => {
            let col = match like.expr.as_ref() {
                Expr::Column(c) => c.name.as_str(),
                _ => bail!("chroma: LIKE only supported on column references"),
            };
            if col != DOCUMENT_COL {
                bail!("chroma: LIKE only supported on the 'document' column")
            }
            let pattern = match like.pattern.as_ref() {
                Expr::Literal(ScalarValue::Utf8(Some(s)), _) => s.clone(),
                _ => bail!("chroma: LIKE pattern must be a string literal"),
            };
            // Strip leading/trailing % to extract the substring; reject mid-pattern wildcards
            // since Chroma's $contains is plain substring, not regex.
            let trimmed = pattern.trim_matches('%').to_string();
            if trimmed.contains('%') || trimmed.contains('_') {
                bail!("chroma: only %substring% LIKE patterns are pushable; got '{pattern}'");
            }
            let op = if like.negated {
                DocumentOperator::NotContains
            } else {
                DocumentOperator::Contains
            };
            Ok(Where::Document(DocumentExpression {
                operator: op,
                pattern: trimmed,
            }))
        }
        _ => bail!("chroma: unsupported filter expression: {expr}"),
    }
}

fn binary_compare_to_where(left: &Expr, op: Operator, right: &Expr) -> Result<Where> {
    let (col, value) = match (left, right) {
        (Expr::Column(c), v) => (c.name.clone(), expr_to_metadata_value(v)?),
        (v, Expr::Column(c)) => (c.name.clone(), expr_to_metadata_value(v)?),
        _ => bail!("chroma: filter must compare a column to a literal"),
    };

    if col == DOCUMENT_COL {
        bail!(
            "chroma: comparison on 'document' is not supported — use LIKE for substring matching"
        );
    }

    let comparison = match op {
        Operator::Eq => MetadataComparison::Primitive(PrimitiveOperator::Equal, value),
        Operator::NotEq => MetadataComparison::Primitive(PrimitiveOperator::NotEqual, value),
        Operator::Lt => MetadataComparison::Primitive(PrimitiveOperator::LessThan, value),
        Operator::LtEq => MetadataComparison::Primitive(PrimitiveOperator::LessThanOrEqual, value),
        Operator::Gt => MetadataComparison::Primitive(PrimitiveOperator::GreaterThan, value),
        Operator::GtEq => {
            MetadataComparison::Primitive(PrimitiveOperator::GreaterThanOrEqual, value)
        }
        _ => bail!("chroma: unsupported comparison operator: {op}"),
    };

    Ok(Where::Metadata(MetadataExpression {
        key: col,
        comparison,
    }))
}

fn expr_to_metadata_value(expr: &Expr) -> Result<MetadataValue> {
    match expr {
        Expr::Literal(scalar, _) => scalar_to_metadata_value(scalar),
        _ => Err(anyhow!(
            "chroma: filter values must be literal scalars (got {expr})"
        )),
    }
}

fn scalar_to_metadata_value(s: &ScalarValue) -> Result<MetadataValue> {
    match s {
        ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => {
            Ok(MetadataValue::Str(v.clone()))
        }
        ScalarValue::Int8(Some(v)) => Ok(MetadataValue::Int(*v as i64)),
        ScalarValue::Int16(Some(v)) => Ok(MetadataValue::Int(*v as i64)),
        ScalarValue::Int32(Some(v)) => Ok(MetadataValue::Int(*v as i64)),
        ScalarValue::Int64(Some(v)) => Ok(MetadataValue::Int(*v)),
        ScalarValue::UInt8(Some(v)) => Ok(MetadataValue::Int(*v as i64)),
        ScalarValue::UInt16(Some(v)) => Ok(MetadataValue::Int(*v as i64)),
        ScalarValue::UInt32(Some(v)) => Ok(MetadataValue::Int(*v as i64)),
        ScalarValue::UInt64(Some(v)) => Ok(MetadataValue::Int(*v as i64)),
        ScalarValue::Float32(Some(v)) => Ok(MetadataValue::Float(*v as f64)),
        ScalarValue::Float64(Some(v)) => Ok(MetadataValue::Float(*v)),
        ScalarValue::Boolean(Some(v)) => Ok(MetadataValue::Bool(*v)),
        _ => Err(anyhow!("chroma: unsupported literal type: {s}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma::types::{BooleanOperator, CompositeExpression};
    use datafusion::common::Column;
    use datafusion::logical_expr::{BinaryExpr, Expr, Like, Operator};

    fn col(name: &str) -> Expr {
        Expr::Column(Column::from_name(name))
    }
    fn lit_str(s: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(s.to_string())), None)
    }
    fn lit_i64(n: i64) -> Expr {
        Expr::Literal(ScalarValue::Int64(Some(n)), None)
    }
    fn binary(left: Expr, op: Operator, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }

    fn metadata_of(w: &Where) -> &MetadataExpression {
        match w {
            Where::Metadata(m) => m,
            other => panic!("expected Where::Metadata, got {other:?}"),
        }
    }
    fn document_of(w: &Where) -> &DocumentExpression {
        match w {
            Where::Document(d) => d,
            other => panic!("expected Where::Document, got {other:?}"),
        }
    }
    fn composite_of(w: &Where) -> &CompositeExpression {
        match w {
            Where::Composite(c) => c,
            other => panic!("expected Where::Composite, got {other:?}"),
        }
    }

    #[test]
    fn empty_filters_return_none() {
        assert!(exprs_to_chroma_filter(&[]).unwrap().is_none());
    }

    #[test]
    fn eq_int_metadata() {
        let f = binary(col("category"), Operator::Eq, lit_i64(7));
        let w = exprs_to_chroma_filter(&[f]).unwrap().unwrap();
        let m = metadata_of(&w);
        assert_eq!(m.key, "category");
        assert!(matches!(
            m.comparison,
            MetadataComparison::Primitive(PrimitiveOperator::Equal, MetadataValue::Int(7))
        ));
    }

    #[test]
    fn comparison_operators_round_trip() {
        for (op, expected) in [
            (Operator::NotEq, PrimitiveOperator::NotEqual),
            (Operator::Lt, PrimitiveOperator::LessThan),
            (Operator::LtEq, PrimitiveOperator::LessThanOrEqual),
            (Operator::Gt, PrimitiveOperator::GreaterThan),
            (Operator::GtEq, PrimitiveOperator::GreaterThanOrEqual),
        ] {
            let f = binary(col("k"), op, lit_i64(1));
            let w = exprs_to_chroma_filter(&[f]).unwrap().unwrap();
            let MetadataComparison::Primitive(actual_op, _) = &metadata_of(&w).comparison else {
                panic!("expected primitive comparison");
            };
            assert_eq!(*actual_op, expected, "op {op:?} -> wrong primitive");
        }
    }

    #[test]
    fn and_or_compose() {
        let a = binary(col("a"), Operator::Eq, lit_i64(1));
        let b = binary(col("b"), Operator::Eq, lit_i64(2));
        let and = binary(a.clone(), Operator::And, b.clone());
        let or = binary(a, Operator::Or, b);

        let w_and = exprs_to_chroma_filter(&[and]).unwrap().unwrap();
        let c_and = composite_of(&w_and);
        assert_eq!(c_and.operator, BooleanOperator::And);
        assert_eq!(c_and.children.len(), 2);

        let w_or = exprs_to_chroma_filter(&[or]).unwrap().unwrap();
        let c_or = composite_of(&w_or);
        assert_eq!(c_or.operator, BooleanOperator::Or);
        assert_eq!(c_or.children.len(), 2);
    }

    #[test]
    fn multiple_filters_implicit_and() {
        let a = binary(col("x"), Operator::Eq, lit_i64(1));
        let b = binary(col("y"), Operator::Eq, lit_i64(2));
        let w = exprs_to_chroma_filter(&[a, b]).unwrap().unwrap();
        let c = composite_of(&w);
        assert_eq!(c.operator, BooleanOperator::And);
    }

    #[test]
    fn like_on_document_becomes_contains() {
        let f = Expr::Like(Like {
            negated: false,
            expr: Box::new(col("document")),
            pattern: Box::new(lit_str("%hello%")),
            escape_char: None,
            case_insensitive: false,
        });
        let w = exprs_to_chroma_filter(&[f]).unwrap().unwrap();
        let d = document_of(&w);
        assert_eq!(d.operator, DocumentOperator::Contains);
        assert_eq!(d.pattern, "hello");
    }

    #[test]
    fn not_like_on_document_becomes_not_contains() {
        let f = Expr::Like(Like {
            negated: true,
            expr: Box::new(col("document")),
            pattern: Box::new(lit_str("%spam%")),
            escape_char: None,
            case_insensitive: false,
        });
        let w = exprs_to_chroma_filter(&[f]).unwrap().unwrap();
        let d = document_of(&w);
        assert_eq!(d.operator, DocumentOperator::NotContains);
        assert_eq!(d.pattern, "spam");
    }

    #[test]
    fn like_on_non_document_column_errors() {
        let f = Expr::Like(Like {
            negated: false,
            expr: Box::new(col("title")),
            pattern: Box::new(lit_str("%hi%")),
            escape_char: None,
            case_insensitive: false,
        });
        let err = exprs_to_chroma_filter(&[f]).unwrap_err().to_string();
        assert!(err.contains("LIKE only supported on the 'document' column"));
    }

    #[test]
    fn mid_pattern_wildcard_rejected() {
        let f = Expr::Like(Like {
            negated: false,
            expr: Box::new(col("document")),
            pattern: Box::new(lit_str("%foo%bar%")),
            escape_char: None,
            case_insensitive: false,
        });
        let err = exprs_to_chroma_filter(&[f]).unwrap_err().to_string();
        assert!(err.contains("only %substring% LIKE patterns"));
    }

    #[test]
    fn comparison_on_document_column_errors() {
        let f = binary(col("document"), Operator::Eq, lit_str("hi"));
        let err = exprs_to_chroma_filter(&[f]).unwrap_err().to_string();
        assert!(err.contains("comparison on 'document'"));
    }

    #[test]
    fn null_predicates_unsupported() {
        let f = Expr::IsNull(Box::new(col("k")));
        let err = exprs_to_chroma_filter(&[f]).unwrap_err().to_string();
        assert!(err.contains("NULL predicates"));
    }

    #[test]
    fn float_and_bool_literals() {
        let f = binary(
            col("score"),
            Operator::Gt,
            Expr::Literal(ScalarValue::Float64(Some(0.5)), None),
        );
        let w = exprs_to_chroma_filter(&[f]).unwrap().unwrap();
        let MetadataComparison::Primitive(_, MetadataValue::Float(v)) = &metadata_of(&w).comparison
        else {
            panic!("expected float metadata value");
        };
        assert!((v - 0.5).abs() < 1e-9);

        let f = binary(
            col("active"),
            Operator::Eq,
            Expr::Literal(ScalarValue::Boolean(Some(true)), None),
        );
        let w = exprs_to_chroma_filter(&[f]).unwrap().unwrap();
        let MetadataComparison::Primitive(_, MetadataValue::Bool(v)) = &metadata_of(&w).comparison
        else {
            panic!("expected bool metadata value");
        };
        assert!(*v);
    }
}
