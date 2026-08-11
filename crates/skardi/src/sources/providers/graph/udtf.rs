//! The graph SQL surface: `cypher_query` and `graph_schema` (design
//! §SQL surface, §cypher_query UDTF signature).
//!
//! Both are registered ONCE as global UDTFs and resolve the connection
//! by name at call time — the `open_connector_query` shape, not one
//! function per source. Planning performs no network I/O: the schema
//! comes from the caller-declared `columns` (parsed and validated at
//! plan time), the keyword guard runs at plan time, and the backend is
//! first touched when the plan executes.
//!
//! Call-shape constraints (stated because agents generate these calls):
//! arguments are positional, so declaring `columns` requires passing
//! `params` — `'{}'` is the no-parameters spelling, and NULL is rejected
//! (schema-shaping arguments are strict string literals). Milestone 1 is
//! AGE-only, and AGE's `cypher()` call must declare its result arity —
//! so `columns` is REQUIRED here: omitting it is a targeted error, and
//! the JSON-`record` fallback ships with the Neo4j milestone, where Bolt
//! needs no declared arity.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::plan_err;
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use futures::stream::{self, TryStreamExt};
use serde_json::Value;

use super::client::{GraphClient, QueryBounds, validate_params};
use super::error::{GraphError, json_kind};
use super::guard::reject_mutations;
use super::value::{ACCEPTED_TYPES, DeclaredColumn, GraphType, build_batch, declared_schema};
use crate::sources::providers::udtf_args::strict_string_arg;

/// Conversion batch size (design §Schema handling): the atomic unit —
/// each batch converts as a whole before it is emitted, so a mid-scan
/// type mismatch never emits a partially converted batch, and peak
/// conversion memory is one batch, not one result.
const CONVERSION_BATCH_ROWS: usize = 1024;

/// One registered `type: graph` source, resolvable by connection name.
#[derive(Debug)]
pub struct GraphSourceHandle {
    pub client: Arc<dyn GraphClient>,
    pub bounds: QueryBounds,
}

/// Shared map of connection name → handle, owned by the front-end the
/// way `OpenConnectorGateways` is.
pub type GraphSources = Arc<RwLock<HashMap<String, Arc<GraphSourceHandle>>>>;

/// Register `cypher_query`, `graph_schema`, and the JSON getter family
/// (`datafusion-functions-json`: `json_get`, `json_get_str`, …) on a
/// session. The getters are what make node/relationship `properties`
/// columns queryable without leaving SQL.
pub fn register_graph_udtfs(ctx: &SessionContext, sources: GraphSources) -> DFResult<()> {
    datafusion_functions_json::register_all(&mut ctx.clone())?;
    ctx.register_udtf(
        "cypher_query",
        Arc::new(CypherQueryFunction {
            sources: Arc::clone(&sources),
        }),
    );
    ctx.register_udtf("graph_schema", Arc::new(GraphSchemaFunction { sources }));
    Ok(())
}

fn lookup(sources: &GraphSources, name: &str) -> DFResult<Arc<GraphSourceHandle>> {
    let map = sources
        .read()
        .map_err(|_| DataFusionError::Internal("graph sources lock poisoned".into()))?;
    map.get(name).cloned().ok_or_else(|| {
        let mut known: Vec<&str> = map.keys().map(String::as_str).collect();
        known.sort_unstable();
        plan_error(GraphError::ConnectionNotFound {
            name: name.to_string(),
            known: if known.is_empty() {
                "none".to_string()
            } else {
                known.join(", ")
            },
        })
    })
}

fn plan_error(e: GraphError) -> DataFusionError {
    DataFusionError::Plan(e.to_string())
}

/// `cypher_query('connection', 'cypher'[, 'params_json'[, 'columns_json']])`.
#[derive(Debug)]
pub struct CypherQueryFunction {
    sources: GraphSources,
}

impl TableFunctionImpl for CypherQueryFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if exprs.len() < 2 || exprs.len() > 4 {
            return plan_err!(
                "cypher_query(connection, cypher, [params_json], [columns_json]) \
                 expects 2-4 arguments, got {}",
                exprs.len()
            );
        }
        let connection = strict_string_arg(&exprs[0], "cypher_query", "connection")?;
        let cypher = strict_string_arg(&exprs[1], "cypher_query", "cypher")?;
        let params_json = exprs
            .get(2)
            .map(|e| strict_string_arg(e, "cypher_query", "params_json"))
            .transpose()?;
        let columns_json = exprs
            .get(3)
            .map(|e| strict_string_arg(e, "cypher_query", "columns_json"))
            .transpose()?;

        // The guard runs at PLAN time — an agent's mutating Cypher fails
        // before any network round-trip, keyword named.
        reject_mutations(&cypher).map_err(plan_error)?;

        let params: Value = match params_json.as_deref() {
            None | Some("") => Value::Object(serde_json::Map::new()),
            Some(text) => {
                let parsed: Value = serde_json::from_str(text).map_err(|e| {
                    plan_error(GraphError::InvalidParams {
                        found: format!("unparseable JSON ({e})"),
                    })
                })?;
                validate_params(&parsed).map_err(plan_error)?;
                parsed
            }
        };

        // Milestone 1 is AGE-only: `columns` is required (AGE's cypher()
        // must declare its arity; the JSON-record fallback ships with the
        // Neo4j milestone).
        let Some(columns_json) = columns_json else {
            return plan_err!(
                "cypher_query: 'columns' is required on the age backend — declare the \
                 output columns as the 4th argument, e.g. \
                 '{{\"name\": \"string\", \"n\": \"node\"}}' (accepted types: {})",
                ACCEPTED_TYPES
            );
        };
        let columns = parse_columns(&columns_json).map_err(plan_error)?;

        let handle = lookup(&self.sources, &connection)?;
        Ok(Arc::new(CypherQueryProvider {
            handle,
            cypher,
            params,
            columns: Arc::new(columns),
        }))
    }
}

/// Parse the declared-columns JSON object. Declaration order is object
/// order (this crate's serde_json enables `preserve_order`); every
/// column is nullable (Cypher can produce null in any position — there
/// is no way to declare otherwise, by design).
fn parse_columns(text: &str) -> Result<Vec<DeclaredColumn>, GraphError> {
    let parsed: Value = serde_json::from_str(text).map_err(|e| GraphError::InvalidColumns {
        reason: format!("unparseable JSON ({e})"),
        accepted: ACCEPTED_TYPES,
    })?;
    let Value::Object(map) = parsed else {
        return Err(GraphError::InvalidColumns {
            reason: format!("expected a JSON object, got {}", json_kind(&parsed)),
            accepted: ACCEPTED_TYPES,
        });
    };
    if map.is_empty() {
        return Err(GraphError::InvalidColumns {
            reason: "at least one column must be declared".to_string(),
            accepted: ACCEPTED_TYPES,
        });
    }
    map.into_iter()
        .map(|(name, ty)| {
            let Value::String(ty_name) = &ty else {
                return Err(GraphError::InvalidColumns {
                    reason: format!(
                        "column '{name}': type must be a string, got {}",
                        json_kind(&ty)
                    ),
                    accepted: ACCEPTED_TYPES,
                });
            };
            let ty = GraphType::parse(ty_name).ok_or_else(|| GraphError::InvalidColumns {
                reason: format!("column '{name}': unknown type '{ty_name}'"),
                accepted: ACCEPTED_TYPES,
            })?;
            Ok(DeclaredColumn { name, ty })
        })
        .collect()
}

/// The provider behind one `cypher_query` call: planning-time-stable
/// declared schema, backend touched only at execute.
#[derive(Debug)]
struct CypherQueryProvider {
    handle: Arc<GraphSourceHandle>,
    cypher: String,
    params: Value,
    columns: Arc<Vec<DeclaredColumn>>,
}

#[async_trait]
impl TableProvider for CypherQueryProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        declared_schema(&self.columns)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(GraphScanExec::new(
            GraphScanKind::Cypher {
                handle: Arc::clone(&self.handle),
                cypher: self.cypher.clone(),
                params: self.params.clone(),
                columns: Arc::clone(&self.columns),
            },
            self.schema(),
            projection.cloned(),
        )?))
    }
}

/// `graph_schema('connection')` — the agent-discovery surface: one row
/// per label, `(label, kind)`, straight off the backend catalog. Names
/// only, never property values.
#[derive(Debug)]
pub struct GraphSchemaFunction {
    sources: GraphSources,
}

impl TableFunctionImpl for GraphSchemaFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if exprs.len() != 1 {
            return plan_err!(
                "graph_schema(connection) expects exactly 1 argument, got {}",
                exprs.len()
            );
        }
        let connection = strict_string_arg(&exprs[0], "graph_schema", "connection")?;
        let handle = lookup(&self.sources, &connection)?;
        Ok(Arc::new(GraphSchemaProvider { handle }))
    }
}

fn graph_schema_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("label", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
    ]))
}

#[derive(Debug)]
struct GraphSchemaProvider {
    handle: Arc<GraphSourceHandle>,
}

#[async_trait]
impl TableProvider for GraphSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        graph_schema_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(GraphScanExec::new(
            GraphScanKind::Labels {
                handle: Arc::clone(&self.handle),
            },
            self.schema(),
            projection.cloned(),
        )?))
    }
}

/// What one graph scan executes.
enum GraphScanKind {
    Cypher {
        handle: Arc<GraphSourceHandle>,
        cypher: String,
        params: Value,
        columns: Arc<Vec<DeclaredColumn>>,
    },
    Labels {
        handle: Arc<GraphSourceHandle>,
    },
}

impl fmt::Debug for GraphScanKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The Cypher text and params are caller data — identity only.
        match self {
            Self::Cypher { columns, .. } => f
                .debug_struct("Cypher")
                .field("columns", &columns.len())
                .finish_non_exhaustive(),
            Self::Labels { .. } => f.debug_struct("Labels").finish_non_exhaustive(),
        }
    }
}

/// Leaf plan: one partition, executes the graph call on first poll.
#[derive(Debug)]
struct GraphScanExec {
    kind: GraphScanKind,
    projection: Option<Vec<usize>>,
    properties: PlanProperties,
}

impl GraphScanExec {
    fn new(
        kind: GraphScanKind,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> DFResult<Self> {
        let projected = match &projection {
            Some(indices) => Arc::new(schema.project(indices)?),
            None => Arc::clone(&schema),
        };
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        // `schema` itself is not stored: the projected schema lives in
        // the plan properties, and the full schema is the provider's.
        Ok(Self {
            kind,
            projection,
            properties,
        })
    }

    fn projected_schema(&self) -> SchemaRef {
        Arc::clone(self.properties.eq_properties.schema())
    }
}

impl DisplayAs for GraphScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.kind {
            GraphScanKind::Cypher { columns, .. } => {
                write!(f, "GraphScanExec: cypher_query columns={}", columns.len())
            }
            GraphScanKind::Labels { .. } => write!(f, "GraphScanExec: graph_schema"),
        }
    }
}

impl ExecutionPlan for GraphScanExec {
    fn name(&self) -> &str {
        "GraphScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "GraphScanExec is a leaf plan and takes no children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "GraphScanExec has 1 partition, got partition {partition}"
            )));
        }
        let projected = self.projected_schema();
        let projection = self.projection.clone();
        let batches = match &self.kind {
            GraphScanKind::Cypher {
                handle,
                cypher,
                params,
                columns,
            } => cypher_batches(
                Arc::clone(handle),
                cypher.clone(),
                params.clone(),
                Arc::clone(columns),
            ),
            GraphScanKind::Labels { handle } => labels_batch(Arc::clone(handle)),
        };
        let stream = batches
            .map(move |batch| {
                let batch = batch?;
                match &projection {
                    Some(indices) => batch
                        .project(indices)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None)),
                    None => Ok(batch),
                }
            })
            .boxed();
        Ok(Box::pin(RecordBatchStreamAdapter::new(projected, stream)))
    }
}

/// The cypher scan: run on first poll, then convert in batch-atomic
/// chunks (design §Schema handling — the conversion batch is the defined
/// atomic unit; a type mismatch fails the CURRENT batch before emission).
fn cypher_batches(
    handle: Arc<GraphSourceHandle>,
    cypher: String,
    params: Value,
    columns: Arc<Vec<DeclaredColumn>>,
) -> futures::stream::BoxStream<'static, DFResult<RecordBatch>> {
    stream::once(async move {
        let rows = handle
            .client
            .execute(&cypher, &params, columns.len(), handle.bounds)
            .await
            .map_err(execution_error)?
            .try_collect::<Vec<_>>()
            .await
            .map_err(execution_error)?;
        let mut batches = Vec::with_capacity(rows.len() / CONVERSION_BATCH_ROWS + 1);
        for (chunk_idx, chunk) in rows.chunks(CONVERSION_BATCH_ROWS).enumerate() {
            batches.push(
                build_batch(&columns, chunk, chunk_idx * CONVERSION_BATCH_ROWS)
                    .map_err(execution_error)?,
            );
        }
        if batches.is_empty() {
            // An empty result is a complete result with the SAME schema.
            batches.push(RecordBatch::new_empty(declared_schema(&columns)));
        }
        Ok::<_, DataFusionError>(batches)
    })
    .map_ok(|batches| stream::iter(batches.into_iter().map(Ok)))
    .try_flatten()
    .boxed()
}

fn labels_batch(
    handle: Arc<GraphSourceHandle>,
) -> futures::stream::BoxStream<'static, DFResult<RecordBatch>> {
    stream::once(async move {
        let labels = handle
            .client
            .labels(handle.bounds)
            .await
            .map_err(execution_error)?;
        let mut names = arrow::array::StringBuilder::new();
        let mut kinds = arrow::array::StringBuilder::new();
        for (name, kind) in &labels {
            names.append_value(name);
            kinds.append_value(kind);
        }
        RecordBatch::try_new(
            graph_schema_schema(),
            vec![Arc::new(names.finish()), Arc::new(kinds.finish())],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    })
    .boxed()
}

fn execution_error(e: GraphError) -> DataFusionError {
    DataFusionError::Execution(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::BoxStream;

    /// Canned-rows client: the UDTF/exec seam without a backend.
    #[derive(Debug)]
    struct MockClient {
        rows: Vec<Vec<Value>>,
        labels: Vec<(String, String)>,
    }

    #[async_trait]
    impl GraphClient for MockClient {
        async fn execute(
            &self,
            _cypher: &str,
            _params: &Value,
            _arity: usize,
            _bounds: QueryBounds,
        ) -> Result<BoxStream<'static, Result<Vec<Value>, GraphError>>, GraphError> {
            Ok(stream::iter(self.rows.clone().into_iter().map(Ok)).boxed())
        }

        async fn labels(&self, _bounds: QueryBounds) -> Result<Vec<(String, String)>, GraphError> {
            Ok(self.labels.clone())
        }
    }

    fn sources_with(rows: Vec<Vec<Value>>) -> GraphSources {
        let handle = Arc::new(GraphSourceHandle {
            client: Arc::new(MockClient {
                rows,
                labels: vec![
                    ("Person".to_string(), "vertex".to_string()),
                    ("KNOWS".to_string(), "edge".to_string()),
                ],
            }),
            bounds: QueryBounds {
                timeout: std::time::Duration::from_secs(5),
                max_rows: 100,
            },
        });
        Arc::new(RwLock::new(HashMap::from([("kg".to_string(), handle)])))
    }

    async fn ctx_with(rows: Vec<Vec<Value>>) -> SessionContext {
        let ctx = SessionContext::new();
        register_graph_udtfs(&ctx, sources_with(rows)).expect("registration");
        ctx
    }

    async fn collect(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        ctx.sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect")
    }

    #[tokio::test]
    async fn declared_columns_scan_end_to_end() {
        let ctx = ctx_with(vec![
            vec![serde_json::json!("ada"), serde_json::json!(1)],
            vec![serde_json::json!("bob"), Value::Null],
        ])
        .await;
        let batches = collect(
            &ctx,
            "SELECT name, n FROM cypher_query('kg', \
             'MATCH (p:Person) RETURN p.name, p.n', '{}', \
             '{\"name\": \"string\", \"n\": \"int\"}') ORDER BY name",
        )
        .await;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn mutating_cypher_fails_at_plan_time_with_the_keyword() {
        let ctx = ctx_with(vec![]).await;
        let err = ctx
            .sql(
                "SELECT * FROM cypher_query('kg', 'CREATE (n) RETURN n', '{}', \
                 '{\"n\": \"node\"}')",
            )
            .await
            .expect_err("plans must fail");
        let msg = err.to_string();
        assert!(msg.contains("'CREATE'"), "{msg}");
        assert!(msg.contains("read-only"), "{msg}");
    }

    #[tokio::test]
    async fn omitted_columns_is_a_targeted_age_error() {
        let ctx = ctx_with(vec![]).await;
        let err = ctx
            .sql("SELECT * FROM cypher_query('kg', 'MATCH (n) RETURN n', '{}')")
            .await
            .expect_err("columns required on age");
        let msg = err.to_string();
        assert!(msg.contains("'columns' is required"), "{msg}");
        assert!(msg.contains("age backend"), "{msg}");
    }

    #[tokio::test]
    async fn unknown_connection_lists_the_known_roster() {
        let ctx = ctx_with(vec![]).await;
        let err = ctx
            .sql(
                "SELECT * FROM cypher_query('nope', 'MATCH (n) RETURN n', '{}', \
                 '{\"n\": \"node\"}')",
            )
            .await
            .expect_err("unknown connection");
        let msg = err.to_string();
        assert!(msg.contains("'nope'"), "{msg}");
        assert!(msg.contains("kg"), "the roster names what exists: {msg}");
    }

    #[tokio::test]
    async fn unknown_type_names_the_accepted_set() {
        let ctx = ctx_with(vec![]).await;
        let err = ctx
            .sql(
                "SELECT * FROM cypher_query('kg', 'MATCH (n) RETURN n', '{}', \
                 '{\"n\": \"Utf8\"}')",
            )
            .await
            .expect_err("PascalCase is not the vocabulary");
        let msg = err.to_string();
        assert!(msg.contains("unknown type 'Utf8'"), "{msg}");
        assert!(msg.contains("node, relationship, path"), "{msg}");
    }

    #[tokio::test]
    async fn empty_results_keep_the_declared_schema() {
        let ctx = ctx_with(vec![]).await;
        let batches = collect(
            &ctx,
            "SELECT name FROM cypher_query('kg', 'MATCH (p) RETURN p.name', '{}', \
             '{\"name\": \"string\"}')",
        )
        .await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
        assert_eq!(batches[0].schema().field(0).name(), "name");
    }

    #[tokio::test]
    async fn mid_scan_type_mismatch_is_typed_and_value_free() {
        let ctx = ctx_with(vec![
            vec![serde_json::json!(1)],
            vec![serde_json::json!("secret")],
        ])
        .await;
        let err = ctx
            .sql(
                "SELECT n FROM cypher_query('kg', 'MATCH (x) RETURN x.n', '{}', \
                 '{\"n\": \"int\"}')",
            )
            .await
            .expect("plans")
            .collect()
            .await
            .expect_err("second row is a string");
        let msg = err.to_string();
        assert!(msg.contains("declared 'int'"), "{msg}");
        assert!(!msg.contains("secret"), "values never leak: {msg}");
    }

    #[tokio::test]
    async fn graph_schema_lists_labels_and_kinds() {
        let ctx = ctx_with(vec![]).await;
        let batches = collect(
            &ctx,
            "SELECT label, kind FROM graph_schema('kg') ORDER BY label",
        )
        .await;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn json_getters_are_registered_for_properties_extraction() {
        // A node's `properties` column is JSON text; the registered
        // json_get family is what makes it queryable without leaving SQL.
        let node = serde_json::json!({
            "id": 1, "label": "Person", "properties": {"name": "ada"}
        });
        let ctx = ctx_with(vec![vec![node]]).await;
        let batches = collect(
            &ctx,
            "SELECT json_get_str(v.properties, 'name') AS name FROM \
             cypher_query('kg', 'MATCH (v) RETURN v', '{}', '{\"v\": \"node\"}')",
        )
        .await;
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "ada");
    }
}
