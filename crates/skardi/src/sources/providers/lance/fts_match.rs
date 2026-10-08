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
//! Constant folding does not get around that. An all-literal call
//! (`fts_match('body', 'q')`) IS offered to the simplifier, whose evaluation
//! fails in `invoke`; DataFusion keeps an expression whose folding errors, so
//! the call survives to execution and fails there, as any other unpushed call
//! does (pinned by `an_all_literal_call_survives_folding_and_fails_at_execution`).
//!
//! # What a query means
//!
//! **The query's syntax is the provider's.** OSS's `lance_fts` reads `foo bar`
//! as either word; skardi-cloud's provider reads it as both, as
//! `websearch_to_tsquery` does. This module fixes only what every provider must
//! agree on, because a query string reaches them all the same way:
//!
//! - a **NULL** query makes the predicate NULL, so no row matches (SQL's rule);
//! - an **empty** query (`''`, or only whitespace) matches no row;
//! - a query that is **not a literal** (a column, a subquery) is not
//!   translatable, and is refused ([`FtsQueryText::NotALiteral`]).
//!
//! [`FtsMatch::query_text`] classifies a call's query into those cases.
//!
//! # The provider contract
//!
//! A provider that answers `fts_match` must:
//!
//! 1. **Answer `Exact`** in `supports_filters_pushdown`, never `Inexact`.
//!    `Inexact` keeps a `Filter` above the scan, that `Filter` calls `invoke`,
//!    and every query errors. A wrapper that downgrades `Exact` to `Inexact`
//!    (as a read-write registration might) breaks it the same way.
//! 2. **Return every match, with no implicit top-k.** `fts_match` is a boolean
//!    predicate. A provider may stop early only at a `limit` DataFusion hands
//!    the scan, which it does only when every conjunct was claimed `Exact`.
//!    Setting a Lance `FullTextSearchQuery` limit, or a `wand_factor` above 1,
//!    while claiming `Exact` silently drops matching rows.
//! 3. **Refuse what it cannot translate** by answering `Unsupported`: a query
//!    that is not a literal, a column with no full-text index. The call then
//!    fails loudly in `invoke` instead of being dropped or claimed and
//!    ignored.
//!
//! **Ranking is not part of the predicate.** `fts_match` has no score, so
//! `ORDER BY relevance` cannot be written. A provider may return matches best
//! first, and with every conjunct claimed and the `LIMIT` pushed into its scan
//! that order is what a query sees; DataFusion does not promise to preserve
//! it otherwise, since the scan declares no output ordering.
//!
//! # Not registered by the server
//!
//! No provider in this crate pushes `fts_match` down yet, so the server does not
//! install it in its session: there it could only ever fail. An engine whose
//! provider does answer it calls [`register_fts_match_udf`] itself.

use std::any::Any;

use arrow::datatypes::DataType;
use datafusion::common::{Column, ScalarValue, not_impl_err, plan_err};
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
            // User-defined so the COLUMN keeps its own type (`coerce_types`):
            // a coercion that cast it would wrap it, and `as_fts_match` only
            // recognises a bare column. `Immutable` is what lets the
            // optimizer move the predicate freely and offer it to the scan:
            // DataFusion never offers a volatile conjunct to
            // `supports_filters_pushdown`.
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

/// The column types Lance 8 builds an inverted index on, plus `Utf8View`.
fn indexable(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::LargeBinary => true,
        DataType::List(field) | DataType::LargeList(field) => {
            matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8)
        }
        _ => false,
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

    /// The column keeps its type; the query becomes `Utf8`. A numeric query
    /// (a `{query}` parameter that parsed as a number) is text too, and NULL
    /// stays NULL.
    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        let [column, query] = arg_types else {
            return plan_err!(
                "{FTS_MATCH} takes two arguments, a column and a query, not {}",
                arg_types.len()
            );
        };
        if !indexable(column) && *column != DataType::Null {
            return plan_err!(
                "{FTS_MATCH} searches a text, binary or list-of-text column, not {column}"
            );
        }
        let query = match query {
            DataType::Null => DataType::Null,
            q if q.is_numeric()
                || matches!(q, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) =>
            {
                DataType::Utf8
            }
            other => return plan_err!("{FTS_MATCH}'s query is text, not {other}"),
        };
        Ok(vec![column.clone(), query])
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
/// use std::sync::Arc;
/// use arrow::array::{RecordBatch, StringArray};
/// use arrow::datatypes::{DataType, Field, Schema};
/// use datafusion::datasource::MemTable;
/// use datafusion::prelude::SessionContext;
/// use skardi::sources::providers::lance::register_fts_match_udf;
///
/// # async fn demo() -> datafusion::error::Result<()> {
/// let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
/// let batch = RecordBatch::try_new(
///     Arc::clone(&schema),
///     vec![Arc::new(StringArray::from(vec!["onboarding notes"]))],
/// )?;
/// let ctx = SessionContext::new();
/// ctx.register_table("corpus", Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))?;
/// register_fts_match_udf(&ctx);
/// // Plans against a real column. (Executing it without a provider that
/// // pushes it down is an error, by design.)
/// let _plan = ctx
///     .sql("SELECT body FROM corpus WHERE fts_match(body, 'onboarding')")
///     .await?
///     .into_optimized_plan()?;
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
/// use skardi::sources::providers::lance::{
///     FtsMatch, FtsQueryText, as_fts_match, register_fts_match_udf,
/// };
///
/// let ctx = SessionContext::new();
/// register_fts_match_udf(&ctx);
/// let expr = ctx.udf("fts_match").unwrap().call(vec![col("body"), lit("onboarding")]);
/// let m: FtsMatch = as_fts_match(&expr).expect("an fts_match on a column");
/// assert_eq!(m.column.name, "body");
/// assert_eq!(m.query_text(), FtsQueryText::Text("onboarding"));
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FtsMatch<'a> {
    pub column: &'a Column,
    pub query: &'a Expr,
}

/// An `fts_match` query, classified the way every provider must treat it
/// (see "What a query means" in the module doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FtsQueryText<'a> {
    /// A non-empty string literal: the provider's own syntax applies.
    Text(&'a str),
    /// A NULL literal, or an empty or whitespace-only string: no row matches.
    MatchesNothing,
    /// Anything else (a column, a subquery): not translatable, so the
    /// provider answers `Unsupported`.
    NotALiteral,
}

impl<'a> FtsMatch<'a> {
    /// This call's query, classified. Saves each provider re-implementing
    /// literal extraction across the three string types.
    pub fn query_text(&self) -> FtsQueryText<'a> {
        match self.query {
            Expr::Literal(
                ScalarValue::Utf8(Some(text))
                | ScalarValue::LargeUtf8(Some(text))
                | ScalarValue::Utf8View(Some(text)),
                _,
            ) if !text.trim().is_empty() => FtsQueryText::Text(text),
            Expr::Literal(
                ScalarValue::Utf8(_)
                | ScalarValue::LargeUtf8(_)
                | ScalarValue::Utf8View(_)
                | ScalarValue::Null,
                _,
            ) => FtsQueryText::MatchesNothing,
            _ => FtsQueryText::NotALiteral,
        }
    }
}

/// Recognise `fts_match(<column>, <query>)` in a filter expression.
///
/// `None` for anything else, including an `fts_match` whose first argument is
/// not a bare column (a provider searches an indexed column, and an expression
/// there has no index to search) and a function that is only NAMED
/// `fts_match`: the call must be THIS crate's UDF, registered by
/// [`register_fts_match_udf`], whose meaning the contract above describes.
/// The query is returned as an expression; [`FtsMatch::query_text`]
/// classifies it.
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
    if call.func.name() != FTS_MATCH || !call.func.inner().as_any().is::<FtsMatchUdf>() {
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

    // ---- pushdown ---------------------------------------------------------

    /// A provider that claims `fts_match` the way the contract asks (or, with
    /// `exact: false`, the way it forbids), wrapping a `MemTable`.
    #[derive(Debug)]
    struct Claiming {
        inner: MemTable,
        exact: bool,
    }

    #[async_trait::async_trait]
    impl datafusion::catalog::TableProvider for Claiming {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            self.inner.schema()
        }
        fn table_type(&self) -> datafusion::datasource::TableType {
            datafusion::datasource::TableType::Base
        }
        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> DFResult<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
            use datafusion::logical_expr::TableProviderFilterPushDown as P;
            Ok(filters
                .iter()
                .map(|f| match (as_fts_match(f), self.exact) {
                    (Some(_), true) => P::Exact,
                    (Some(_), false) => P::Inexact,
                    (None, _) => P::Unsupported,
                })
                .collect())
        }
        async fn scan(
            &self,
            state: &dyn datafusion::catalog::Session,
            projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            limit: Option<usize>,
        ) -> DFResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
            // Answers the search by returning every row; the test is about
            // where the predicate goes, not what it matches.
            self.inner.scan(state, projection, &[], limit).await
        }
    }

    async fn ctx_with_claiming(exact: bool) -> SessionContext {
        let ctx = ctx_with_corpus().await;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("body", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some("onboarding notes")])),
            ],
        )
        .unwrap();
        let table = Claiming {
            inner: MemTable::try_new(schema, vec![vec![batch]]).unwrap(),
            exact,
        };
        ctx.register_table("claiming", Arc::new(table)).unwrap();
        ctx
    }

    fn scan_filters(plan: &LogicalPlan) -> Vec<Expr> {
        let mut out = Vec::new();
        plan.apply(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                out.extend(scan.filters.iter().cloned());
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        out
    }

    /// The pushed path: a provider that claims the predicate `Exact` gets it
    /// in its scan, no `Filter` is left above, and `invoke` is never reached.
    /// This is what an `Immutable` signature buys: a volatile conjunct is
    /// never offered to `supports_filters_pushdown`.
    #[tokio::test]
    async fn a_provider_that_claims_it_exact_receives_it_and_the_query_runs() {
        let ctx = ctx_with_claiming(true).await;
        let df = ctx
            .sql("SELECT id FROM claiming WHERE fts_match(body, 'onboarding')")
            .await
            .unwrap();
        let plan = df.clone().into_optimized_plan().unwrap();
        assert!(
            filter_predicates(&plan).is_empty(),
            "no Filter left: {plan}"
        );
        assert!(
            scan_filters(&plan)
                .iter()
                .any(|f| as_fts_match(f).is_some()),
            "the scan received the predicate: {plan}"
        );
        df.collect().await.expect("invoke was never reached");
    }

    /// Compound predicates are not claimed (`as_fts_match` sees an OR or a
    /// NOT), so they fail loudly rather than changing results; and a provider
    /// that answers `Inexact` leaves a Filter that calls `invoke`.
    #[tokio::test]
    async fn compound_predicates_and_an_inexact_provider_fail_loudly() {
        let exact = ctx_with_claiming(true).await;
        for sql in [
            "SELECT id FROM claiming WHERE fts_match(body, 'q') OR id > 0",
            "SELECT id FROM claiming WHERE NOT fts_match(body, 'q')",
        ] {
            let err = exact
                .sql(sql)
                .await
                .unwrap()
                .collect()
                .await
                .expect_err(sql);
            assert!(err.to_string().contains("not pushed down"), "{sql}: {err}");
        }
        let inexact = ctx_with_claiming(false).await;
        let err = inexact
            .sql("SELECT id FROM claiming WHERE fts_match(body, 'q')")
            .await
            .unwrap()
            .collect()
            .await
            .expect_err("Inexact keeps the Filter");
        assert!(err.to_string().contains("not pushed down"), "{err}");
    }

    // ---- types, folding, identity ---------------------------------------

    /// The column keeps its own type, so the call stays recognisable; list
    /// columns (which Lance indexes) are accepted; a numeric query is text.
    #[tokio::test]
    async fn the_column_keeps_its_type_and_the_query_becomes_text() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("body", DataType::Utf8, true),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]));
        let ctx = SessionContext::new();
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![]]).unwrap()),
        )
        .unwrap();
        register_fts_match_udf(&ctx);
        for (sql, text) in [
            (
                "SELECT 1 FROM t WHERE fts_match(body, arrow_cast('q', 'Utf8View'))",
                "q",
            ),
            ("SELECT 1 FROM t WHERE fts_match(tags, 'q')", "q"),
            ("SELECT 1 FROM t WHERE fts_match(body, 2024)", "2024"),
        ] {
            let plan = ctx.sql(sql).await.unwrap().into_optimized_plan().unwrap();
            let found = filter_predicates(&plan)
                .iter()
                .flat_map(|p| {
                    split_conjunction(p)
                        .into_iter()
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .find_map(|c| {
                    as_fts_match(&c).map(|m| {
                        (
                            m.column.name.clone(),
                            m.query_text() == FtsQueryText::Text(text),
                        )
                    })
                });
            let (column, query_ok) =
                found.unwrap_or_else(|| panic!("{sql}: not recognised: {plan}"));
            assert!(query_ok, "{sql}: query is not {text:?}: {plan}");
            assert!(column == "body" || column == "tags", "{sql}");
        }
        // Refused while planning: by `sql` or, at the latest, the analyzer.
        let err = match ctx.sql("SELECT 1 FROM t WHERE fts_match(1, 'q')").await {
            Err(e) => e.to_string(),
            Ok(df) => df
                .into_optimized_plan()
                .expect_err("a number is not a searchable column")
                .to_string(),
        };
        assert!(err.contains("searches a text"), "{err}");
    }

    /// NULL and empty queries match nothing, a non-literal is untranslatable.
    #[tokio::test]
    async fn query_text_classifies_the_three_cases() {
        let ctx = ctx_with_corpus().await;
        let udf = ctx.udf(FTS_MATCH).unwrap();
        let text = |query: Expr| {
            let call = udf.call(vec![col("body"), query]);
            as_fts_match(&call).unwrap().query_text().clone_static()
        };
        assert_eq!(text(lit("onboarding")), Static::Text("onboarding".into()));
        assert_eq!(text(lit("   ")), Static::MatchesNothing);
        assert_eq!(text(lit(ScalarValue::Utf8(None))), Static::MatchesNothing);
        assert_eq!(text(lit(ScalarValue::Null)), Static::MatchesNothing);
        assert_eq!(text(col("id")), Static::NotALiteral);
    }

    #[derive(Debug, PartialEq)]
    enum Static {
        Text(String),
        MatchesNothing,
        NotALiteral,
    }

    trait CloneStatic {
        fn clone_static(&self) -> Static;
    }

    impl CloneStatic for FtsQueryText<'_> {
        fn clone_static(&self) -> Static {
            match self {
                FtsQueryText::Text(t) => Static::Text(t.to_string()),
                FtsQueryText::MatchesNothing => Static::MatchesNothing,
                FtsQueryText::NotALiteral => Static::NotALiteral,
            }
        }
    }

    /// An all-literal call is offered to constant folding, whose evaluation
    /// fails; DataFusion keeps the expression, and it fails at execution.
    #[tokio::test]
    async fn an_all_literal_call_survives_folding_and_fails_at_execution() {
        let ctx = ctx_with_corpus().await;
        let df = ctx
            .sql("SELECT id FROM corpus WHERE fts_match('body', 'q')")
            .await
            .unwrap();
        df.clone()
            .into_optimized_plan()
            .expect("folding failed quietly and kept the call");
        let err = df.collect().await.expect_err("and it errors when run");
        assert!(err.to_string().contains("not pushed down"), "{err}");
    }

    /// Only this crate's UDF is recognised, not any function of the name.
    #[test]
    fn a_different_function_of_the_same_name_is_not_recognised() {
        #[derive(Debug, PartialEq, Eq, Hash)]
        struct Impostor(Signature);
        impl ScalarUDFImpl for Impostor {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn name(&self) -> &str {
                FTS_MATCH
            }
            fn signature(&self) -> &Signature {
                &self.0
            }
            fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
                Ok(DataType::Boolean)
            }
            fn invoke_with_args(&self, _: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
            }
        }
        let impostor = ScalarUDF::new_from_impl(Impostor(Signature::any(2, Volatility::Immutable)));
        let expr = impostor.call(vec![col("body"), lit("q")]);
        assert!(as_fts_match(&expr).is_none());
    }
}
