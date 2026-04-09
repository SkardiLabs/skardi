//! Shared utilities for converting DataFusion expressions to PostgreSQL SQL strings.

use datafusion::logical_expr::Expr;
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::PostgreSqlDialect;

/// Try to convert a DataFusion `Expr` to a PostgreSQL SQL string suitable for
/// use in a WHERE clause pushed down to Postgres.
///
/// Returns `None` for expressions that cannot be reliably converted, so the
/// caller can skip pushdown for those filters rather than generating invalid SQL.
pub fn expr_to_pg_sql(expr: &Expr) -> Option<String> {
    let unparser = Unparser::new(&PostgreSqlDialect {});
    unparser.expr_to_sql(expr).ok().map(|ast| ast.to_string())
}
