//! `chroma_knn` UDTF.
//!
//! Usage:
//! ```sql
//! -- Literal vector
//! SELECT * FROM chroma_knn('docs', 'embedding', [0.1, 0.2, ...], 5);
//!
//! -- Subquery vector
//! SELECT * FROM chroma_knn('docs', 'embedding',
//!     (SELECT embedding FROM users WHERE id = 1), 5);
//!
//! -- With Skardi WHERE filter pushed down to Chroma's typed Where DSL
//! SELECT * FROM chroma_knn('docs', 'embedding', [0.1, 0.2, ...], 5)
//! WHERE category = 'finance';
//! ```

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{Result as DFResult, plan_err};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;

use crate::sources::providers::chroma::filter::exprs_to_chroma_filter;
use crate::sources::providers::chroma::knn_exec::{ChromaKnnExec, knn_output_schema};
use crate::sources::providers::chroma::table_provider::ChromaEntry;
use crate::sources::providers::knn_utils::{extract_k, extract_literal_vector};
use crate::sources::providers::{DatasetEntry, DatasetRegistry};

#[derive(Debug)]
pub struct ChromaKnnTableFunction {
    registry: DatasetRegistry,
}

impl ChromaKnnTableFunction {
    pub fn new(registry: DatasetRegistry) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for ChromaKnnTableFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if exprs.len() != 4 {
            return plan_err!(
                "chroma_knn(table_name, embedding_column, query_vector, k) expects 4 arguments, got {}",
                exprs.len()
            );
        }
        let table_name = extract_string(&exprs[0], "table_name")?;
        let _embedding_column = extract_string(&exprs[1], "embedding_column")?;
        let k = extract_k(&exprs[3], "chroma_knn")?;

        // Either a literal `[…]` vector or a `(SELECT …)` subquery — anything
        // else is a planning error.
        let query_vector_expr = exprs[2].clone();
        let literal_vector = match &query_vector_expr {
            Expr::Literal(_, _) => Some(extract_literal_vector(&query_vector_expr, "chroma_knn")?),
            Expr::ScalarSubquery(_) => None,
            _ => {
                return plan_err!(
                    "chroma_knn: query_vector must be a literal array or scalar subquery"
                );
            }
        };

        let entry = {
            let registry = self.registry.read().map_err(|e| {
                datafusion::error::DataFusionError::Internal(format!(
                    "chroma_knn: registry lock error: {e}"
                ))
            })?;
            let raw = registry.get(&table_name).cloned().ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!(
                    "chroma_knn: table '{table_name}' not found in registry"
                ))
            })?;
            match raw {
                DatasetEntry::Chroma(e) => e,
                _ => {
                    return plan_err!(
                        "chroma_knn: table '{}' is not a Chroma collection",
                        table_name
                    );
                }
            }
        };

        Ok(Arc::new(ChromaKnnProvider {
            entry,
            literal_vector,
            query_vector_expr,
            k,
        }))
    }
}

#[derive(Debug)]
struct ChromaKnnProvider {
    entry: ChromaEntry,
    literal_vector: Option<Vec<f32>>,
    query_vector_expr: Expr,
    k: usize,
}

#[async_trait]
impl TableProvider for ChromaKnnProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        knn_output_schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let where_filter = exprs_to_chroma_filter(filters)
            .map_err(|e| datafusion::error::DataFusionError::Plan(format!("chroma_knn: {e}")))?;

        let exec = if let Some(ref v) = self.literal_vector {
            ChromaKnnExec::try_new_literal(
                self.entry.collection.clone(),
                v.clone(),
                self.k,
                where_filter,
            )?
        } else {
            let Expr::ScalarSubquery(subquery) = &self.query_vector_expr else {
                return plan_err!(
                    "chroma_knn: query_vector must be a literal array or scalar subquery"
                );
            };
            let physical_plan = state
                .create_physical_plan(subquery.subquery.as_ref())
                .await?;
            ChromaKnnExec::try_new_with_deferred_extraction(
                self.entry.collection.clone(),
                physical_plan,
                self.k,
                where_filter,
            )?
        };
        Ok(Arc::new(exec))
    }
}

fn extract_string(expr: &Expr, name: &str) -> DFResult<String> {
    use datafusion::common::ScalarValue;
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => Ok(s.clone()),
        _ => plan_err!("chroma_knn: expected literal string for {name}"),
    }
}

/// Register the UDTF on a SessionContext.
pub fn register_chroma_knn_udtf(
    ctx: &datafusion::prelude::SessionContext,
    registry: DatasetRegistry,
) {
    ctx.register_udtf(
        "chroma_knn",
        Arc::new(ChromaKnnTableFunction::new(registry)),
    );
}
