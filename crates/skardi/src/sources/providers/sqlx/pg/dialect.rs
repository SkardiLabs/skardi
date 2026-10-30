//! PostgreSQL rendering for integer-array membership filters.

use arrow::array::{Array, Int64Array};
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::sql::sqlparser::{
    ast, dialect::PostgreSqlDialect as ParserDialect, parser::Parser,
};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::{Dialect, IntervalStyle, PostgreSqlDialect};

/// Renders integer-array predicates using PostgreSQL's native operators.
///
/// `governed_array_key_budget` is a measured tuning value, not a limit on readable keys.
/// Until a caller supplies it, integer-array overlap uses PostgreSQL's `&&`.

#[derive(Debug, Clone, Default)]
pub(super) struct GovernedPostgresDialect {
    governed_array_key_budget: Option<usize>,
}

impl GovernedPostgresDialect {
    /// Select an overlap/unnest crossover measured by the caller.
    pub(super) fn new(governed_array_key_budget: Option<usize>) -> Self {
        Self {
            governed_array_key_budget,
        }
    }
}

impl Dialect for GovernedPostgresDialect {
    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        PostgreSqlDialect {}.identifier_quote_style(identifier)
    }

    fn supports_qualify(&self) -> bool {
        false
    }
    fn requires_derived_table_alias(&self) -> bool {
        true
    }
    fn supports_empty_select_list(&self) -> bool {
        true
    }
    fn interval_style(&self) -> IntervalStyle {
        IntervalStyle::PostgresVerbose
    }
    fn float64_ast_dtype(&self) -> ast::DataType {
        ast::DataType::DoublePrecision
    }

    fn scalar_function_to_sql_overrides(
        &self,
        unparser: &Unparser,
        name: &str,
        args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        let predicate = match (name, args) {
            ("array_has_any", [column, keys]) => {
                let keys = integer_array(keys).ok_or_else(|| unsupported(name))?;
                let use_unnest = matches!(column, Expr::Column(_))
                    && self
                        .governed_array_key_budget
                        .is_some_and(|budget| keys.len() > budget);
                let column = unparser.expr_to_sql(column)?;
                let literal = array_literal(&keys);
                if keys.is_empty() {
                    // DataFusion's scalar-array fast path returns false even
                    // for a NULL column when the literal needle list is empty.
                    "FALSE".to_string()
                } else if use_unnest {
                    format!(
                        "CASE WHEN ({column}) IS NULL THEN NULL ELSE EXISTS \
                         (SELECT 1 FROM unnest({column}) AS _skardi_key(value) \
                         WHERE _skardi_key.value = ANY({literal})) END"
                    )
                } else {
                    format!("({column}) && {literal}")
                }
            }
            _ => {
                return PostgreSqlDialect {}.scalar_function_to_sql_overrides(unparser, name, args);
            }
        };
        let expression = Parser::new(&ParserDialect {})
            .try_with_sql(&predicate)
            .and_then(|mut parser| parser.parse_expr())
            .map_err(|error| {
                DataFusionError::Plan(format!("PostgreSQL membership expression: {error}"))
            })?;
        Ok(Some(expression))
    }
}

fn unsupported(name: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "PostgreSQL pushdown of {name} requires a non-null bigint literal array"
    ))
}

fn integer_array(expr: &Expr) -> Option<Vec<i64>> {
    match expr {
        Expr::Literal(ScalarValue::List(array), _) if !array.is_null(0) => {
            let values = array.value(0);
            let values = values.as_any().downcast_ref::<Int64Array>()?;
            (values.null_count() == 0).then(|| values.values().to_vec())
        }
        Expr::ScalarFunction(function) if function.name() == "make_array" => {
            function.args.iter().map(integer_literal).collect()
        }
        _ => None,
    }
}

fn integer_literal(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => Some(*value),
        _ => None,
    }
}

fn array_literal(keys: &[i64]) -> String {
    let keys = keys
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    format!("'{{{keys}}}'::bigint[]")
}
