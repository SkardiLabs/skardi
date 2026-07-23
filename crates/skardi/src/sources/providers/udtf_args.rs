//! Planning-time argument extraction shared by every table function.
//!
//! Each UDTF used to re-implement its own `Expr` → string-literal extractor
//! with drifting error wording and NULL handling. These helpers keep the
//! wording uniform and make the three deliberate NULL semantics explicit at
//! each call site:
//!
//! - [`string_arg`] — NULL is accepted as a placeholder during pipeline
//!   schema inference (the inferencer replaces `{param}` with NULL before
//!   plan creation) and yields the empty string; the real value is
//!   substituted textually at request time before re-planning, so the
//!   placeholder never executes.
//! - [`optional_string_arg`] — NULL means "argument not provided".
//! - [`strict_string_arg`] — NULL is rejected: for arguments that determine
//!   the planned schema (a table, gateway, or column name), where a
//!   placeholder cannot produce a plan and accepting one would only trade
//!   this targeted error for a confusing lookup failure.

use datafusion::common::{ScalarValue, plan_err};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;

/// Extract a string-literal argument; NULL is a pipeline schema-inference
/// placeholder and yields the empty string. `fn_name` prefixes the error
/// (e.g. `"sqlite_fts"`), `arg` names the argument.
pub(crate) fn string_arg(expr: &Expr, fn_name: &str, arg: &str) -> DFResult<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => Ok(s.clone()),
        Expr::Literal(ScalarValue::Null, _) => Ok(String::new()),
        _ => plan_err!("{fn_name}: '{arg}' must be a string literal"),
    }
}

/// Extract an optional string-literal argument; NULL means "not provided".
pub(crate) fn optional_string_arg(
    expr: &Expr,
    fn_name: &str,
    arg: &str,
) -> DFResult<Option<String>> {
    match expr {
        Expr::Literal(ScalarValue::Null, _) => Ok(None),
        other => string_arg(other, fn_name, arg).map(Some),
    }
}

/// Extract a string-literal argument, rejecting NULL — for arguments that
/// determine the planned schema, where a placeholder cannot work.
pub(crate) fn strict_string_arg(expr: &Expr, fn_name: &str, arg: &str) -> DFResult<String> {
    match expr {
        Expr::Literal(ScalarValue::Null, _) => {
            plan_err!("{fn_name}: '{arg}' must be a string literal, not NULL")
        }
        other => string_arg(other, fn_name, arg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};

    fn null_expr() -> Expr {
        Expr::Literal(ScalarValue::Null, None)
    }

    #[test]
    fn string_arg_accepts_literals_and_placeholder_null() {
        assert_eq!(string_arg(&lit("wiki"), "f", "table").unwrap(), "wiki");
        assert_eq!(
            string_arg(
                &Expr::Literal(ScalarValue::LargeUtf8(Some("wiki".into())), None),
                "f",
                "table"
            )
            .unwrap(),
            "wiki"
        );
        // Pipeline schema inference passes NULL for {param} placeholders.
        assert_eq!(string_arg(&null_expr(), "f", "table").unwrap(), "");
    }

    #[test]
    fn non_literals_fail_with_uniform_wording() {
        for helper in [string_arg, strict_string_arg] {
            let err = helper(&col("x"), "my_fn", "query").unwrap_err();
            assert!(
                err.to_string()
                    .contains("my_fn: 'query' must be a string literal"),
                "got {err}"
            );
        }
        let err = optional_string_arg(&lit(42), "my_fn", "alias").unwrap_err();
        assert!(err.to_string().contains("'alias' must be a string literal"));
    }

    #[test]
    fn null_semantics_differ_by_variant() {
        assert_eq!(
            optional_string_arg(&null_expr(), "f", "alias").unwrap(),
            None
        );
        assert_eq!(
            optional_string_arg(&lit("work"), "f", "alias").unwrap(),
            Some("work".to_string())
        );
        let err = strict_string_arg(&null_expr(), "f", "gateway").unwrap_err();
        assert!(err.to_string().contains("not NULL"), "got {err}");
    }

    #[test]
    fn typed_null_strings_are_rejected_not_read_as_empty() {
        // Utf8(None) / LargeUtf8(None) are typed NULLs (e.g. from
        // CAST(NULL AS VARCHAR)), not string literals: no variant may read
        // them as a valid "" — and only the untyped ScalarValue::Null gets
        // the placeholder treatment, matching every provider's
        // pre-refactor behavior.
        for scalar in [ScalarValue::Utf8(None), ScalarValue::LargeUtf8(None)] {
            let expr = Expr::Literal(scalar, None);
            for helper in [string_arg, strict_string_arg] {
                let err = helper(&expr, "f", "arg").unwrap_err();
                assert!(
                    err.to_string().contains("'arg' must be a string literal"),
                    "got {err}"
                );
            }
            let err = optional_string_arg(&expr, "f", "arg").unwrap_err();
            assert!(
                err.to_string().contains("'arg' must be a string literal"),
                "got {err}"
            );
        }
    }
}
