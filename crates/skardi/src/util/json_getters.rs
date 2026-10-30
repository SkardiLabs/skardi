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

use std::any::Any;
use std::sync::Arc;

use arrow::array::new_empty_array;
use arrow::datatypes::{DataType, FieldRef};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ExpressionPlacement, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature,
};
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
/// Empty dynamic-path batches keep their declared type; nonempty calls use
/// the upstream implementation unchanged.
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
    ] {
        ctx.register_udf(ScalarUDF::new_from_impl(EmptyPathBatch(udf)));
    }
    ctx.register_udf((*udfs::json_from_scalar_udf()).clone());
    Ok(())
}

/// The upstream scalar-JSON/array-path helper constructs a run-end array with
/// a single run of length `path.len()`. A zero-row batch would make that run
/// end zero, which Arrow rejects (datafusion-functions-json 0.53.1,
/// `common.rs::invoke_scalar_array`). Keep all nonempty calls upstream.
#[derive(Debug, PartialEq, Eq, Hash)]
struct EmptyPathBatch(Arc<ScalarUDF>);

impl ScalarUDFImpl for EmptyPathBatch {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.0.name()
    }

    fn aliases(&self) -> &[String] {
        self.0.aliases()
    }

    fn signature(&self) -> &Signature {
        self.0.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        self.0.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        self.0.return_field_from_args(args)
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        self.0.placement(args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if matches!(
            args.args.as_slice(),
            [ColumnarValue::Scalar(_), ColumnarValue::Array(path)] if path.is_empty()
        ) {
            return Ok(ColumnarValue::Array(new_empty_array(args.return_type())));
        }
        self.0.invoke_with_args(args)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::StringArray;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionConfig;

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

    #[tokio::test]
    async fn scalar_json_dynamic_paths_accept_an_empty_batch_after_a_row() {
        let config = SessionConfig::new().set_bool("datafusion.execution.coalesce_batches", false);
        let ctx = SessionContext::new_with_config(config);
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
        let row = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["a"]))],
        )
        .unwrap();
        let empty = RecordBatch::new_empty(Arc::clone(&schema));
        let input = MemTable::try_new(schema, vec![vec![row, empty]]).unwrap();
        ctx.register_table("paths", Arc::new(input)).unwrap();
        let sql = "SELECT json_get_str('{\"a\":\"value\"}', key) AS value FROM paths";

        // Prove that this input reaches the affected upstream execution path.
        ctx.register_udf((*udfs::json_get_str_udf()).clone());
        let error = ctx.sql(sql).await.unwrap().collect().await.unwrap_err();
        assert!(error.to_string().contains("run_ends"), "{error}");

        register_json_getter_udfs(&ctx).unwrap();
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let row = batches.iter().find(|batch| batch.num_rows() == 1).unwrap();
        assert_eq!(
            row.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "value"
        );
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
