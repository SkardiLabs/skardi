//! The three PostgreSQL full-text-search functions — `to_tsvector`,
//! `websearch_to_tsquery` and `ts_rank` — registered as scalar UDFs whose
//! only job is to let a statement naming them **plan** and **push down**.
//!
//! # Why they have to exist at all
//!
//! `pg_fts` (the UDTF next door) hand-builds its Postgres SQL as a string, so
//! these three names live there only inside a `format!`. Nothing ever
//! registered them with DataFusion. A raw-SQL full-text query — the shape the
//! cloud pipeline generator emits —
//!
//! ```sql
//! SELECT path,
//!        ts_rank(to_tsvector('english', body),
//!                websearch_to_tsquery('english', $1)) AS rank
//! FROM docs
//! WHERE to_tsvector('english', body) @@ websearch_to_tsquery('english', $1)
//! ```
//!
//! therefore failed at SQL→LogicalPlan resolution, *before* federation or any
//! provider was consulted. Registering the scalars is the only fix that serves
//! both servers: the UDTF route is structurally unavailable on the cloud
//! engine, which deregisters every table function by design (a UDTF's provider
//! is constructed at plan time, ahead of any authorization check).
//!
//! # Why `invoke` errors instead of returning NULL
//!
//! **Do not "fix" these into NULL-returning stubs.** PostgreSQL evaluates
//! them; DataFusion only ever carries them across. If a query fails to push
//! down and DataFusion evaluates a NULL-returning stub instead, the predicate
//! `NULL @@ NULL` is NULL, the filter keeps nothing, and the query answers
//! "no matches" — indistinguishable from a genuine empty result. A loud
//! `not_impl_err!` naming the cause is strictly better than a silent wrong
//! answer, and is the failure mode this module exists to guarantee.
//!
//! The constant-folding interaction is deliberate and safe: the args of
//! `websearch_to_tsquery('english', 'cats')` are all literals and the
//! functions are `Immutable`, so DataFusion's const-evaluator *will* try to
//! fold them at optimize time and *will* get this error back. DataFusion 52
//! treats a const-fold failure on a non-CAST expression as "leave the
//! expression alone" (`ConstSimplifyResult::SimplifyRuntimeError` →
//! `Transformed::yes(expr)`), so the call survives into the pushed-down SQL
//! rather than failing the plan. `constant_folding_does_not_kill_the_plan` pins that.
//!
//! # How far the pushdown actually reaches
//!
//! Two different mechanisms can carry these to Postgres, and they carry
//! different amounts:
//!
//! - **Scan-level** (what skardi's own Postgres source uses today): a plain
//!   `SqlTable` from datafusion-table-providers. Its
//!   `supports_filters_pushdown` is `default_filter_pushdown`, i.e. "does
//!   `Unparser<PostgreSqlDialect>::expr_to_sql` succeed?" — so the WHERE
//!   predicate goes down whole and `Exact`, but the SELECT list does not.
//!   A `ts_rank(...) AS rank` projection is therefore evaluated in DataFusion
//!   and hits the error below. Tested by
//!   `the_predicate_unparses_back_to_postgres_sql`.
//! - **Whole-plan federation** (`FederatedTableProviderAdaptor` +
//!   `datafusion-federation`, which skardi's cloud engine uses): the entire
//!   subtree is unparsed with `plan_to_sql`, projection, ORDER BY and LIMIT
//!   included. Tested by
//!   `the_whole_search_okf_statement_unparses_back_to_postgres_sql`.
//!
//! Both renderings are valid Postgres. The distinction matters to any caller
//! that puts one of these in a SELECT list: it needs the federated path, or a
//! `pg_fts`-style provider, not a bare `SqlTable`.
//!
//! # Types
//!
//! Postgres' `tsvector` and `tsquery` are opaque to DataFusion — the values
//! only ever travel to Postgres, never back — so both are modelled as `Utf8`.
//! That is not cosmetic: `@@` coerces through `like_coercion`, which needs
//! string operands on both sides and yields `Boolean`, so `Utf8` is what makes
//! the generated predicate type-check as a filter. `ts_rank` returns
//! Postgres' `float4`, i.e. `Float32`.

use std::any::Any;

use arrow::datatypes::DataType;
use datafusion::common::not_impl_err;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// A Postgres FTS function that DataFusion can plan and unparse but must
/// never evaluate. See the module doc for why `invoke_with_args` errors.
#[derive(Debug, PartialEq, Eq, Hash)]
struct PgFtsUdf {
    name: &'static str,
    signature: Signature,
    return_type: DataType,
}

impl PgFtsUdf {
    fn new(name: &'static str, arg_count: usize, return_type: DataType) -> Self {
        Self {
            name,
            // `Volatility::Immutable` matches Postgres (all three are declared
            // IMMUTABLE there) and is what lets the optimizer move these
            // freely so they reach the pushdown boundary intact.
            signature: Signature::string(arg_count, Volatility::Immutable),
            return_type,
        }
    }
}

impl ScalarUDFImpl for PgFtsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(self.return_type.clone())
    }

    /// Always an error, deliberately.
    ///
    /// Reaching here means the query was NOT pushed down to PostgreSQL, and
    /// there is no honest local answer to give. Returning NULL instead would
    /// turn that into a query that quietly reports "no matches"; see the
    /// module doc. If you are here because a query failed, the fix is to make
    /// it push down (or to use `pg_fts`), not to soften this.
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        not_impl_err!(
            "{} is evaluated by PostgreSQL; this query was not pushed down",
            self.name
        )
    }
}

/// Register `to_tsvector`, `websearch_to_tsquery` and `ts_rank` on `ctx`.
///
/// Registered beside `register_pg_fts_udtf` / `register_pg_knn_udtf`, but
/// unlike those two these are session-wide scalars with no registry to bind:
/// they carry no data access of their own, so they are safe to install in any
/// context, including a planning-only one.
///
/// # Example
/// ```
/// use datafusion::prelude::SessionContext;
/// use skardi::sources::providers::sqlx::pg::register_pg_fts_udfs;
///
/// # async fn demo() -> datafusion::error::Result<()> {
/// let ctx = SessionContext::new();
/// register_pg_fts_udfs(&ctx);
/// // Plans. (Executing it in DataFusion is an error, by design.)
/// let _plan = ctx.state().create_logical_plan("SELECT to_tsvector('english', 'a')").await?;
/// # Ok(())
/// # }
/// ```
pub fn register_pg_fts_udfs(ctx: &SessionContext) {
    for udf in [
        PgFtsUdf::new("to_tsvector", 2, DataType::Utf8),
        PgFtsUdf::new("websearch_to_tsquery", 2, DataType::Utf8),
        // Postgres' ts_rank returns float4.
        PgFtsUdf::new("ts_rank", 2, DataType::Float32),
    ] {
        ctx.register_udf(ScalarUDF::new_from_impl(udf));
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{RecordBatch, StringArray};
    use arrow::datatypes::{Field, Schema, SchemaRef};
    use datafusion::common::tree_node::TreeNode;
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown};
    use datafusion::sql::unparser::Unparser;
    use datafusion::sql::unparser::dialect::PostgreSqlDialect;
    use datafusion::sql::unparser::plan_to_sql;
    use datafusion_table_providers::sql::sql_provider_datafusion::default_filter_pushdown;

    fn docs_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("path", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, true),
        ]))
    }

    /// One real row — the refusal test needs a batch to evaluate over, since
    /// an empty table never calls `invoke_with_args` at all.
    fn ctx_with_docs() -> SessionContext {
        let ctx = SessionContext::new();
        register_pg_fts_udfs(&ctx);
        let schema = docs_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["/a.md"])),
                Arc::new(StringArray::from(vec!["cats and dogs"])),
            ],
        )
        .expect("batch");
        let table = MemTable::try_new(schema, vec![vec![batch]]).expect("memtable");
        ctx.register_table("docs", Arc::new(table))
            .expect("register");
        ctx
    }

    /// The exact predicate shape the cloud `search-okf` generator emits.
    const SEARCH_OKF_SQL: &str = "SELECT path, ts_rank(to_tsvector('english', body), \
         websearch_to_tsquery('english', 'cats')) AS rank \
         FROM docs WHERE to_tsvector('english', body) @@ \
         websearch_to_tsquery('english', 'cats') \
         ORDER BY rank DESC LIMIT 5";

    // ─── Step 1: they plan ─────────────────────────────────────────────────

    #[tokio::test]
    async fn the_three_fts_functions_plan() {
        let ctx = SessionContext::new();
        register_pg_fts_udfs(&ctx);
        // Planning is the whole point: these never execute in DataFusion,
        // they exist so a statement naming them resolves and can be pushed
        // down to Postgres whole.
        for sql in [
            "SELECT to_tsvector('english', 'a')",
            "SELECT websearch_to_tsquery('english', 'a')",
            "SELECT ts_rank(to_tsvector('english','a'), websearch_to_tsquery('english','a'))",
        ] {
            ctx.state()
                .create_logical_plan(sql)
                .await
                .unwrap_or_else(|e| panic!("{sql} must plan: {e}"));
        }
    }

    #[tokio::test]
    async fn the_search_okf_predicate_shape_plans_end_to_end() {
        let ctx = ctx_with_docs();
        ctx.state()
            .create_logical_plan(SEARCH_OKF_SQL)
            .await
            .unwrap_or_else(|e| panic!("the search-okf predicate must plan: {e}"));
    }

    /// `@@` must type-check as a filter, which is only true because
    /// `to_tsvector`/`websearch_to_tsquery` return `Utf8`: DataFusion coerces
    /// `AtAt` through `like_coercion`, which demands strings and yields
    /// `Boolean`. A non-string return type would fail here, not at run time.
    #[tokio::test]
    async fn the_at_at_operator_type_checks_over_the_two_string_returns() {
        let ctx = ctx_with_docs();
        let plan = ctx
            .state()
            .create_logical_plan(
                "SELECT path FROM docs WHERE to_tsvector('english', body) @@ \
                 websearch_to_tsquery('english', 'cats')",
            )
            .await
            .expect("the @@ predicate must plan");
        assert!(
            format!("{}", plan.display_indent()).contains("@@"),
            "the operator survives planning: {}",
            plan.display_indent()
        );
    }

    /// The whole statement must also survive the OPTIMIZER. This is the
    /// non-obvious half: `websearch_to_tsquery('english', 'cats')` takes only
    /// literals and is `Immutable`, so the const-evaluator folds it — calling
    /// `invoke_with_args`, which errors by design. DataFusion 52 keeps the
    /// original expression on a const-fold failure rather than propagating it,
    /// and this test is what stops a DataFusion upgrade changing that silently.
    #[tokio::test]
    async fn constant_folding_does_not_kill_the_plan() {
        let ctx = ctx_with_docs();
        let optimized = ctx
            .sql(SEARCH_OKF_SQL)
            .await
            .expect("plans")
            .into_optimized_plan()
            .unwrap_or_else(|e| panic!("the search-okf predicate must optimize: {e}"));
        let shown = format!("{}", optimized.display_indent());
        for needle in ["to_tsvector", "websearch_to_tsquery", "ts_rank"] {
            assert!(
                shown.contains(needle),
                "{needle} survives optimization rather than being folded away: {shown}"
            );
        }
    }

    // ─── Step 3: it refuses to answer rather than answering wrongly ────────

    #[tokio::test]
    async fn executing_one_in_datafusion_refuses_rather_than_returning_a_wrong_answer() {
        // A stub that silently returned NULL would make a non-pushed-down
        // query answer "no matches" instead of failing — the exact silent
        // failure shape the design exists to package against.
        let ctx = ctx_with_docs();
        for (sql, needle) in [
            (
                "SELECT to_tsvector('english', body) FROM docs",
                "to_tsvector",
            ),
            (
                "SELECT websearch_to_tsquery('english', body) FROM docs",
                "websearch_to_tsquery",
            ),
            // Not nested inside the other two: the innermost call errors
            // first, and this case is about ts_rank naming itself.
            ("SELECT ts_rank(body, body) FROM docs", "ts_rank"),
        ] {
            let err = ctx
                .sql(sql)
                .await
                .expect("plans")
                .collect()
                .await
                .expect_err("local evaluation must refuse, never answer");
            let msg = err.to_string();
            assert!(msg.contains(needle), "the error names the function: {msg}");
            assert!(msg.contains("not pushed down"), "and the cause: {msg}");
        }
    }

    // ─── Step 4: prove the pushdown ────────────────────────────────────────

    /// Pull the WHERE predicate out of the optimized plan, exactly as a
    /// `TableProvider` receives it in `scan`/`supports_filters_pushdown`.
    async fn optimized_filter(ctx: &SessionContext, sql: &str) -> Expr {
        let plan = ctx
            .sql(sql)
            .await
            .expect("plans")
            .into_optimized_plan()
            .expect("optimizes");
        let mut found = None;
        plan.apply(|node| {
            if let LogicalPlan::Filter(f) = node {
                found = Some(f.predicate.clone());
            }
            if let LogicalPlan::TableScan(t) = node
                && let Some(e) = t.filters.first()
            {
                found = Some(e.clone());
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })
        .expect("walk");
        found.expect("the plan carries the FTS predicate")
    }

    /// The pushdown contract skardi's Postgres provider actually uses:
    /// `SqlTable::supports_filters_pushdown` calls
    /// `default_filter_pushdown`, which is nothing but
    /// `Unparser::new(PostgreSqlDialect).expr_to_sql(filter)` — a filter that
    /// unparses is `Exact` (pushed whole), one that does not is `Unsupported`
    /// (evaluated locally, which for these three means the loud error above).
    /// So this assertion is the difference between the design working and the
    /// design failing every query.
    #[tokio::test]
    async fn the_predicate_unparses_back_to_postgres_sql() {
        let ctx = ctx_with_docs();
        let predicate = optimized_filter(
            &ctx,
            "SELECT path FROM docs WHERE to_tsvector('english', body) @@ \
             websearch_to_tsquery('english', 'cats')",
        )
        .await;

        let rendered = Unparser::new(&PostgreSqlDialect {})
            .expr_to_sql(&predicate)
            .unwrap_or_else(|e| panic!("the FTS predicate must unparse: {e}"))
            .to_string();
        eprintln!("PUSHED PREDICATE: {rendered}");

        for needle in [
            "to_tsvector('english'",
            "websearch_to_tsquery('english'",
            " @@ ",
        ] {
            assert!(
                rendered.contains(needle),
                "unparsed predicate must contain {needle}: {rendered}"
            );
        }

        // ...and the provider must therefore classify it as fully pushed.
        assert_eq!(
            default_filter_pushdown(&[&predicate], &PostgreSqlDialect {}),
            vec![TableProviderFilterPushDown::Exact],
            "the predicate is pushed whole, not evaluated locally"
        );
    }

    /// The whole statement — projection, `ts_rank`, ORDER BY and LIMIT
    /// included — must render back to Postgres. This is the shape a federated
    /// engine pushes down (the cloud server, and any provider wrapped in a
    /// `FederatedTableProviderAdaptor`); the scan-level pushdown above only
    /// carries the WHERE clause.
    #[tokio::test]
    async fn the_whole_search_okf_statement_unparses_back_to_postgres_sql() {
        let ctx = ctx_with_docs();
        let plan = ctx
            .state()
            .create_logical_plan(SEARCH_OKF_SQL)
            .await
            .expect("plans");
        let rendered = plan_to_sql(&plan)
            .unwrap_or_else(|e| panic!("the search-okf statement must unparse: {e}"))
            .to_string();
        eprintln!("PUSHED STATEMENT: {rendered}");
        for needle in [
            "to_tsvector('english'",
            "websearch_to_tsquery('english'",
            "ts_rank(",
            "@@",
            "ORDER BY",
            "LIMIT",
        ] {
            assert!(
                rendered.contains(needle),
                "unparsed statement must contain {needle}: {rendered}"
            );
        }
    }
}
