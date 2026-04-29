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
                bail!(
                    "chroma: only %substring% LIKE patterns are pushable; got '{pattern}'"
                );
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

