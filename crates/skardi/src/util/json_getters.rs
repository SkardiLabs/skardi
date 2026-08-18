//! The `datafusion-functions-json` getter UDF family, registered WITHOUT
//! the operator rewrite.
//!
//! The getters (`json_get_str(properties, 'name')`, …) are the extraction
//! tool for every JSON-typed column — graph node/relationship `properties`
//! first among them — so the server session registers them unconditionally
//! (planning AND runtime contexts). What this module deliberately does NOT
//! do is call `datafusion_functions_json::register_all`:
//!
//! - `register_all` additionally installs a `JsonExprPlanner` +
//!   `JsonFunctionRewriter`, which rewrite the SQL operators `->`, `->>`
//!   and `?` into `json_get(...)` calls at planning time, session-wide.
//! - DataFusion 52 natively parses `->` into `Expr::BinaryExpr(Arrow)`,
//!   and datafusion-table-providers' unparser (PostgreSqlDialect) can
//!   unparse that expression back into `->`, pushing the filter down to
//!   the remote Postgres. Once rewritten to `json_get(...)`, the filter
//!   no longer unparses, is marked Unsupported, and degrades into a
//!   local full scan of the federated table.
//! - So the session gets the UDFs only: explicit `json_get_str(...)`
//!   works everywhere, while a remote `data -> 'k'` keeps its native
//!   pushdown behaviour unchanged.
//!
//! (Design: `docs/superpowers/specs/2026-08-08-graph-engine-bypass-design.md`,
//! milestone 4 — the carried-in obligation this module discharges.)

use datafusion::error::Result as DFResult;
use datafusion::prelude::SessionContext;
use datafusion_functions_json::udfs;

/// Register the twelve JSON getter UDFs on the session — and nothing
/// else. See the module doc for why the operator rewrite
/// (`register_all`'s side effect) must never be installed here.
///
/// # Errors
/// Infallible today (`SessionContext::register_udf` cannot fail); the
/// `Result` keeps the signature honest against a future fallible
/// registration step, matching `register_graph_udtfs`.
///
/// # Example
/// ```
/// use datafusion::prelude::SessionContext;
/// use skardi::util::json_getters::register_json_getter_udfs;
///
/// # async fn demo() -> datafusion::error::Result<()> {
/// let ctx = SessionContext::new();
/// register_json_getter_udfs(&ctx)?;
/// let df = ctx.sql("SELECT json_get_str('{\"a\": \"x\"}', 'a')").await?;
/// # Ok(())
/// # }
/// ```
pub fn register_json_getter_udfs(ctx: &SessionContext) -> DFResult<()> {
    // The exact set `register_all` registers, listed individually so the
    // rewriter/planner pair it installs afterwards can never sneak in.
    for udf in [
        udfs::json_get_udf(),
        udfs::json_get_bool_udf(),
        udfs::json_get_float_udf(),
        udfs::json_get_int_udf(),
        udfs::json_get_json_udf(),
        udfs::json_get_array_udf(),
        udfs::json_as_text_udf(),
        udfs::json_get_str_udf(),
        udfs::json_contains_udf(),
        udfs::json_length_udf(),
        udfs::json_object_keys_udf(),
        udfs::json_from_scalar_udf(),
    ] {
        ctx.register_udf((*udf).clone());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_getters_are_registered_and_extract() {
        let ctx = SessionContext::new();
        register_json_getter_udfs(&ctx).expect("registers");
        let batches = ctx
            .sql("SELECT json_get_str('{\"a\": \"x\"}', 'a') AS v")
            .await
            .expect("plans")
            .collect()
            .await
            .expect("executes");
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("string column");
        assert_eq!(col.value(0), "x");
    }

    /// The federation-pushdown contract (module doc): with ONLY the getter
    /// UDFs registered, `->>` must NOT be silently rewritten to `json_get`.
    /// DataFusion 52 has no native Arrow-operator planner either, so the
    /// observable contract is a loud planning error naming the operator —
    /// never a plan containing `json_get`.
    #[tokio::test]
    async fn arrow_operators_keep_native_planning() {
        let ctx = SessionContext::new();
        register_json_getter_udfs(&ctx).expect("registers");
        let err = ctx
            .sql("SELECT '{\"a\":1}'::text ->> 'a'")
            .await
            .expect_err("no rewrite means no plan");
        let msg = err.to_string();
        assert!(msg.contains("->>"), "the operator is named: {msg}");
        assert!(
            msg.contains("not yet supported"),
            "native (unsupported), not rewritten: {msg}"
        );
    }
}
