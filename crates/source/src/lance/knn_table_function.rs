//! Table function for Lance KNN search
//!
//! Usage:
//! ```sql
//! -- With literal vector
//! SELECT * FROM lance_knn('table_name', 'embedding', [0.1, 0.2, ...], 10)
//!
//! -- With subquery vector
//! SELECT * FROM lance_knn('table_name', 'embedding',
//!     (SELECT embedding FROM users WHERE id = $1), 10)
//!
//! -- With optional filter
//! SELECT * FROM lance_knn('table_name', 'embedding',
//!     (SELECT embedding FROM users WHERE id = $1), 10, 'category = ''electronics''')
//! ```

use arrow::array::{Array, ArrayRef, Float32Array};
use arrow::datatypes::{DataType, Field, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{Result as DFResult, ScalarValue, plan_err};
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use lance::dataset::Dataset;
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::knn_exec::LanceKnnExec;

/// Table function that creates KNN search on Lance tables
#[derive(Debug)]
pub struct LanceKnnTableFunction {
    dataset_registry: Arc<RwLock<HashMap<String, Arc<Dataset>>>>,
}

impl LanceKnnTableFunction {
    pub fn new(dataset_registry: Arc<RwLock<HashMap<String, Arc<Dataset>>>>) -> Self {
        Self { dataset_registry }
    }
}

impl TableFunctionImpl for LanceKnnTableFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if exprs.len() < 4 || exprs.len() > 5 {
            return plan_err!(
                "lance_knn(table_name, vector_column, query_vector, k, [filter]) expects 4-5 arguments, got {}",
                exprs.len()
            );
        }

        // Extract string arguments
        let table_name = extract_string(&exprs[0], "table_name")?;
        let vector_column = extract_string(&exprs[1], "vector_column")?;
        let k = extract_int(&exprs[3], "k")?;
        let filter = if exprs.len() == 5 {
            Some(extract_string(&exprs[4], "filter")?)
        } else {
            None
        };

        // Get dataset from registry
        let dataset = {
            let registry = self.dataset_registry.read().map_err(|e| {
                datafusion::error::DataFusionError::Internal(format!("Registry lock error: {}", e))
            })?;
            registry.get(&table_name).cloned().ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!(
                    "lance_knn: table '{}' not found in registry",
                    table_name
                ))
            })?
        };

        // Try to extract literal vector, otherwise store expr for subquery
        let query_vector_expr = exprs[2].clone();
        let literal_vector = try_extract_vector(&query_vector_expr)?;

        Ok(Arc::new(LanceKnnProvider {
            dataset,
            vector_column,
            literal_vector,
            query_vector_expr,
            k,
            filter,
        }))
    }
}

/// Thin wrapper that returns LanceKnnExec from scan()
struct LanceKnnProvider {
    dataset: Arc<Dataset>,
    vector_column: String,
    literal_vector: Option<ArrayRef>,
    query_vector_expr: Expr,
    k: usize,
    filter: Option<String>,
}

impl std::fmt::Debug for LanceKnnProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LanceKnnProvider")
            .field("vector_column", &self.vector_column)
            .field("k", &self.k)
            .finish()
    }
}

#[async_trait]
impl TableProvider for LanceKnnProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // Build schema: dataset fields (excluding vector) + _distance
        let lance_schema = self.dataset.schema();
        let mut fields: Vec<Field> = lance_schema
            .fields
            .iter()
            .filter(|f| f.name.as_str() != self.vector_column)
            .map(|f| f.into())
            .collect();
        fields.push(Field::new("_distance", DataType::Float32, true));
        Arc::new(arrow::datatypes::Schema::new(fields))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let exec = if let Some(ref vector) = self.literal_vector {
            // Literal vector path
            LanceKnnExec::try_new(
                self.dataset.clone(),
                self.vector_column.clone(),
                vector.clone(),
                self.k,
            )?
        } else {
            // Subquery path - create physical plan for deferred evaluation
            let Expr::ScalarSubquery(subquery) = &self.query_vector_expr else {
                return plan_err!(
                    "lance_knn: query_vector must be literal array or scalar subquery"
                );
            };

            let physical_plan = state
                .create_physical_plan(subquery.subquery.as_ref())
                .await?;

            let column_name = subquery
                .subquery
                .schema()
                .fields()
                .first()
                .map(|f| f.name().clone())
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(
                        "lance_knn: subquery must return at least one column".to_string(),
                    )
                })?;

            LanceKnnExec::try_new_with_deferred_extraction(
                self.dataset.clone(),
                self.vector_column.clone(),
                physical_plan,
                column_name,
                self.k,
            )?
        };

        let exec = match &self.filter {
            Some(f) => exec.with_filter(f.clone()),
            None => exec,
        };

        Ok(Arc::new(exec))
    }
}

// Helper functions for argument extraction

fn extract_string(expr: &Expr, name: &str) -> DFResult<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Ok(s.clone()),
        Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => Ok(s.clone()),
        _ => plan_err!("lance_knn: {} must be a string literal", name),
    }
}

fn extract_int(expr: &Expr, name: &str) -> DFResult<usize> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(n)), _) => Ok(*n as usize),
        Expr::Literal(ScalarValue::Int32(Some(n)), _) => Ok(*n as usize),
        Expr::Literal(ScalarValue::UInt64(Some(n)), _) => Ok(*n as usize),
        _ => plan_err!("lance_knn: {} must be an integer literal", name),
    }
}

fn try_extract_vector(expr: &Expr) -> DFResult<Option<ArrayRef>> {
    match expr {
        Expr::Literal(ScalarValue::List(arr), _) => {
            let list_arr = arr.as_ref();
            if list_arr.is_empty() {
                return plan_err!("lance_knn: query_vector cannot be empty");
            }
            let values = list_arr.value(0);
            match values.as_any().downcast_ref::<Float32Array>() {
                Some(float_arr) => Ok(Some(Arc::new(float_arr.clone()) as ArrayRef)),
                None => plan_err!("lance_knn: query_vector must contain Float32 values"),
            }
        }
        Expr::Literal(ScalarValue::FixedSizeList(arr), _) => {
            let list_arr = arr.as_ref();
            if list_arr.is_empty() {
                return plan_err!("lance_knn: query_vector cannot be empty");
            }
            let values = list_arr.value(0);
            match values.as_any().downcast_ref::<Float32Array>() {
                Some(float_arr) => Ok(Some(Arc::new(float_arr.clone()) as ArrayRef)),
                None => plan_err!("lance_knn: query_vector must contain Float32 values"),
            }
        }
        _ => Ok(None), // Not a literal, could be subquery
    }
}

/// Register lance_knn table function with SessionContext
pub fn register_lance_knn_udtf(
    ctx: &datafusion::prelude::SessionContext,
    dataset_registry: Arc<RwLock<HashMap<String, Arc<Dataset>>>>,
) {
    ctx.register_udtf(
        "lance_knn",
        Arc::new(LanceKnnTableFunction::new(dataset_registry)),
    );
}
