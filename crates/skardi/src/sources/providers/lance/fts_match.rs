//! `fts_match(column, query)` — a full-text predicate that a Lance provider
//! answers with its inverted indexes, registered as a scalar UDF whose only job
//! is to let a statement naming it **plan** and **push down**.
//!
//! # Why a scalar and not `lance_fts`
//!
//! `lance_fts` is a table function, and a table function builds its provider at
//! plan time out of arguments the caller controls. An engine that authorizes a
//! query by walking its plan (skardi-cloud's planning tier) removes every table
//! function for that reason, so `lance_fts` cannot serve there. A scalar
//! constructs nothing, opens nothing and holds no credential: it is an
//! expression in a `WHERE` clause, which is exactly what such a walk already
//! covers. The provider that owns the scan recognises it ([`as_fts_match`]) and
//! turns it into a Lance full-text query.
//!
//! ```sql
//! SELECT path, title
//! FROM corpus
//! WHERE fts_match(body, 'onboarding checklist')
//!   AND "type" = 'document'
//! ```
//!
//! # Why `invoke` errors instead of returning NULL
//!
//! The same reason as the PostgreSQL stubs in `sqlx::pg::fts_udfs`: reaching
//! `invoke` means no provider took the predicate, and there is no honest local
//! answer. A NULL would filter every row out and the query would report "no
//! matches", indistinguishable from a genuine empty result. A loud error naming
//! the cause is the failure mode this exists to guarantee.
//!
//! Constant folding cannot reach it: the first argument is a column, so the
//! call is never all-literal and the optimizer never tries to evaluate it
//! early.
//!
//! # Not registered by the server
//!
//! No provider in this crate pushes `fts_match` down yet, so the server does not
//! install it in its session: there it could only ever fail. An engine whose
//! provider does answer it calls [`register_fts_match_udf`] itself.

use std::any::Any;

use arrow::datatypes::DataType;
use datafusion::common::{Column, not_impl_err};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// The function's SQL name.
pub const FTS_MATCH: &str = "fts_match";

/// `fts_match` as DataFusion sees it. See the module doc for why
/// `invoke_with_args` errors.
#[derive(Debug, PartialEq, Eq, Hash)]
struct FtsMatchUdf {
    signature: Signature,
}

impl FtsMatchUdf {
    fn new() -> Self {
        Self {
            // Two strings, the column and the query text. `Immutable` is what
            // lets the optimizer move the predicate freely, so it reaches the
            // scan intact and the provider can claim it.
            signature: Signature::string(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for FtsMatchUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        FTS_MATCH
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    /// Always an error, deliberately: no provider took the predicate.
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        not_impl_err!(
            "{FTS_MATCH} is answered by a Lance provider's full-text indexes; this query \
             was not pushed down to one"
        )
    }
}

/// Register `fts_match` on `ctx`.
///
/// Session-wide and safe in a planning-only context: it carries no data access
/// of its own.
///
/// # Example
/// ```
/// use datafusion::prelude::SessionContext;
/// use skardi::sources::providers::lance::register_fts_match_udf;
///
/// # async fn demo() -> datafusion::error::Result<()> {
/// let ctx = SessionContext::new();
/// register_fts_match_udf(&ctx);
/// // Plans. (Executing it without a provider that pushes it down is an
/// // error, by design.)
/// let _plan = ctx
///     .state()
///     .create_logical_plan("SELECT 1 WHERE fts_match('body', 'query')")
///     .await?;
/// # Ok(())
/// # }
/// ```
pub fn register_fts_match_udf(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::new_from_impl(FtsMatchUdf::new()));
}

/// An `fts_match` call a provider can serve: the column it searches and the
/// query expression.
///
/// # Example
/// ```
/// use datafusion::execution::FunctionRegistry;
/// use datafusion::prelude::{SessionContext, col, lit};
/// use skardi::sources::providers::lance::{FtsMatch, as_fts_match, register_fts_match_udf};
///
/// let ctx = SessionContext::new();
/// register_fts_match_udf(&ctx);
/// let expr = ctx.udf("fts_match").unwrap().call(vec![col("body"), lit("onboarding")]);
/// let FtsMatch { column, query } = as_fts_match(&expr).expect("an fts_match on a column");
/// assert_eq!(column.name, "body");
/// assert_eq!(query, &lit("onboarding"));
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FtsMatch<'a> {
    pub column: &'a Column,
    pub query: &'a Expr,
}

/// Recognise `fts_match(<column>, <query>)` in a filter expression.
///
/// `None` for anything else, including an `fts_match` whose first argument is
/// not a bare column: a provider searches an indexed column, and an expression
/// there has no index to search. The query is returned as an expression rather
/// than a string so the provider decides what it accepts; after parameter
/// substitution it is normally a string literal.
///
/// # Example
/// ```
/// use datafusion::execution::FunctionRegistry;
/// use datafusion::prelude::{SessionContext, col, lit};
/// use skardi::sources::providers::lance::{as_fts_match, register_fts_match_udf};
///
/// // `fts_match` built the way the SQL planner builds it.
/// let ctx = SessionContext::new();
/// register_fts_match_udf(&ctx);
/// let udf = ctx.udf("fts_match").unwrap();
/// let expr = udf.call(vec![col("body"), lit("onboarding")]);
/// let m = as_fts_match(&expr).expect("recognised");
/// assert_eq!(m.column.name, "body");
/// ```
pub fn as_fts_match(expr: &Expr) -> Option<FtsMatch<'_>> {
    let Expr::ScalarFunction(call) = expr else {
        return None;
    };
    if call.func.name() != FTS_MATCH {
        return None;
    }
    match call.args.as_slice() {
        [Expr::Column(column), query] => Some(FtsMatch { column, query }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{Field, Schema};
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::datasource::MemTable;
    use datafusion::execution::FunctionRegistry;
    use datafusion::logical_expr::LogicalPlan;
    use datafusion::logical_expr::utils::split_conjunction;
    use datafusion::prelude::{col, lit};
    use std::sync::Arc;

    async fn ctx_with_corpus() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("body", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("onboarding notes"), None])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table(
            "corpus",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        register_fts_match_udf(&ctx);
        ctx
    }

    /// Every filter predicate in an optimized plan.
    fn filter_predicates(plan: &LogicalPlan) -> Vec<Expr> {
        let mut out = Vec::new();
        plan.apply(|node| {
            if let LogicalPlan::Filter(filter) = node {
                out.push(filter.predicate.clone());
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        out
    }

    #[tokio::test]
    async fn the_predicate_plans_and_survives_optimization_intact() {
        let ctx = ctx_with_corpus().await;
        let plan = ctx
            .sql("SELECT id FROM corpus WHERE fts_match(body, 'onboarding') AND id > 0")
            .await
            .expect("fts_match plans")
            .into_optimized_plan()
            .unwrap();
        let predicates = filter_predicates(&plan);
        let found = predicates.iter().any(|p| {
            split_conjunction(p)
                .into_iter()
                .any(|conjunct| as_fts_match(conjunct).is_some())
        });
        assert!(
            found,
            "fts_match must reach the scan's filter: {predicates:?}"
        );
    }

    #[tokio::test]
    async fn evaluating_it_locally_is_an_error_not_an_empty_result() {
        let ctx = ctx_with_corpus().await;
        let err = ctx
            .sql("SELECT id FROM corpus WHERE fts_match(body, 'onboarding')")
            .await
            .unwrap()
            .collect()
            .await
            .expect_err("a MemTable cannot answer fts_match");
        let msg = err.to_string();
        assert!(
            msg.contains("fts_match") && msg.contains("not pushed down"),
            "{msg}"
        );
    }

    #[tokio::test]
    async fn it_returns_boolean_and_refuses_the_wrong_arity() {
        let ctx = ctx_with_corpus().await;
        let df = ctx
            .sql("SELECT fts_match(body, 'q') AS m FROM corpus")
            .await
            .unwrap();
        assert_eq!(
            df.schema().field_with_name(None, "m").unwrap().data_type(),
            &DataType::Boolean
        );
        // DataFusion checks a call's argument count during analysis, so the
        // refusal arrives with the optimized plan rather than from `sql`. It
        // must be that refusal, not the deliberate "not pushed down" error.
        let one_argument = match ctx.sql("SELECT id FROM corpus WHERE fts_match(body)").await {
            Err(e) => e.to_string(),
            Ok(df) => df
                .into_optimized_plan()
                .expect_err("one argument must not plan")
                .to_string(),
        };
        assert!(
            one_argument.contains("fts_match") && !one_argument.contains("not pushed down"),
            "{one_argument}"
        );
    }

    #[tokio::test]
    async fn as_fts_match_takes_a_column_and_nothing_else() {
        let ctx = ctx_with_corpus().await;
        let udf = ctx.udf(FTS_MATCH).unwrap();

        let on_column = udf.call(vec![col("body"), lit("onboarding")]);
        let m = as_fts_match(&on_column).expect("a column is recognised");
        assert_eq!(m.column.name, "body");
        assert_eq!(m.query, &lit("onboarding"));

        let on_expression = udf.call(vec![lit("body"), lit("onboarding")]);
        assert!(
            as_fts_match(&on_expression).is_none(),
            "a literal is not an index"
        );
        assert!(as_fts_match(&col("body").eq(lit("x"))).is_none());
    }
}
