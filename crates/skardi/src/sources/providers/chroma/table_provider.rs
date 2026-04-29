//! `ChromaTableProvider` — TableProvider for one Chroma collection.
//!
//! Schema is fixed:
//! - `id`        Utf8 (non-null)
//! - `document`  Utf8 (nullable)
//! - `embedding` FixedSizeList<Float32, dim>
//! - `metadata`  Map<Utf8, Utf8>     (JSON-stringified scalar values)
//!
//! KNN queries route through the `chroma_knn` UDTF, not through `scan` directly.

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use chroma::ChromaCollection;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;

use crate::sources::providers::chroma::filter::exprs_to_chroma_filter;
use crate::sources::providers::chroma::scan_exec::ChromaScanExec;
use crate::sources::providers::chroma::writes::{ChromaDmlExec, ChromaInsertExec, ChromaWriteOp};

/// Registry entry for a Chroma collection — handed to `chroma_knn` so the UDTF
/// can find the live collection without re-resolving from the SessionContext.
#[derive(Clone, Debug)]
pub struct ChromaEntry {
    pub collection: Arc<ChromaCollection>,
    pub schema: SchemaRef,
    pub embedding_dim: i32,
}

#[derive(Debug)]
pub struct ChromaTableProvider {
    pub(crate) collection: Arc<ChromaCollection>,
    pub(crate) schema: SchemaRef,
    pub(crate) embedding_dim: i32,
}

impl ChromaTableProvider {
    pub fn new(collection: Arc<ChromaCollection>, schema: SchemaRef, embedding_dim: i32) -> Self {
        Self {
            collection,
            schema,
            embedding_dim,
        }
    }
}

/// Build the Arrow schema for a Chroma collection given the embedding dimension.
pub fn chroma_schema(embedding_dim: i32) -> SchemaRef {
    let entry_struct = DataType::Struct(
        vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]
        .into(),
    );
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("document", DataType::Utf8, true),
        Field::new(
            "embedding",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                embedding_dim,
            ),
            true,
        ),
        Field::new(
            "metadata",
            DataType::Map(Arc::new(Field::new("entries", entry_struct, false)), false),
            true,
        ),
    ]))
}

#[async_trait]
impl TableProvider for ChromaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Inexact: Chroma's $contains is case-sensitive substring, which doesn't match
        // SQL LIKE semantics losslessly. DataFusion re-applies the filter to be safe.
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let where_filter = exprs_to_chroma_filter(filters)
            .map_err(|e| DataFusionError::Plan(format!("chroma: {e}")))?;
        Ok(Arc::new(ChromaScanExec::try_new(
            self.collection.clone(),
            self.schema.clone(),
            self.embedding_dim,
            projection.cloned(),
            where_filter,
            limit,
        )?))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ChromaInsertExec::new(
            self.collection.clone(),
            self.schema.clone(),
            input,
            op,
        )))
    }

    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let where_filter = exprs_to_chroma_filter(&filters)
            .map_err(|e| DataFusionError::Plan(format!("chroma: {e}")))?;
        Ok(Arc::new(ChromaDmlExec::new(
            self.collection.clone(),
            ChromaWriteOp::Delete { where_filter },
        )))
    }
}
