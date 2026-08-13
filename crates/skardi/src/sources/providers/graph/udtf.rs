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
//! (schema-shaping arguments are strict string literals). Whether
//! `columns` may be OMITTED is per backend: AGE's `cypher()` call must
//! declare its result arity, so omitting it there is a targeted error;
//! on Neo4j (Bolt needs no declared arity) the omission is the
//! JSON-`record` fallback — one `record: Utf8` column carrying each
//! whole record as canonical JSON text, keys in sorted order.
//!
//! **How declared columns BIND is per backend, and both contracts are
//! stated here** (the schema itself is identical either way):
//!
//! - **AGE: positional.** The binding to the Cypher `RETURN` clause is
//!   positional (all AGE's `cypher()` gives us), so `columns` must list
//!   them in RETURN order. Two same-typed columns declared out of order
//!   swap SILENTLY — same JSON kind, no `TypeMismatch`, nothing
//!   downstream can tell — which is also the mis-declaration an LLM is
//!   most likely to produce. The error message, this doc, and the
//!   design's §Schema handling all state it because no structural check
//!   is possible.
//! - **Neo4j: by NAME.** Bolt records carry field names, so each
//!   declared column binds to the RETURN entry of the same name —
//!   declaration order only sets output column order, and a declared
//!   name the query never returns is a typed error naming it (alias the
//!   entry with `AS`). The positional footgun above does not exist on
//!   this backend.

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

/// Projection prunes AFTER conversion, deliberately: the declared
/// schema is the CALLER'S CONTRACT, so a violated declaration fails
/// loudly whether or not this query selected the column — otherwise the
/// error surfaces only when someone finally selects it, far from the
/// query that established the bad declaration. A reader making
/// projection pushdown actually prune work would change observable
/// behaviour;
/// `an_unprojected_declared_column_still_fails_its_type_contract` pins
/// it.
///
/// Conversion batch size (design §Schema handling): the atomic unit —
/// each batch converts as a whole before it is emitted, so a mid-scan
/// type mismatch never emits a partially converted batch.
///
/// Milestone-1 reality, stated so this comment cannot outlive the code
/// it describes: BOTH layers fully buffer (the client collects every
/// row, and `cypher_batches` materializes every RecordBatch before the
/// first is emitted — which `EmissionType::Final` declares honestly).
/// So today NOTHING is emitted before a mid-scan failure — the design's
/// explicitly-permitted whole-result variant, strictly stronger than
/// the batch-atomic contract. The chunking below is where the contract
/// will start to bind if a later client genuinely streams; it is not
/// buying partial emission now.
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
///
/// **Scope, stated plainly**: `register_all` registers 12 UDFs *and* a
/// function rewriter *and* an expr planner that remap the SQL operators
/// `->`, `->>` and `?` to `json_get`/`json_as_text`/`json_contains` for
/// EVERY query in the session, not just graph ones — and it overwrites
/// same-named UDFs (debug-level log only). Additive today (DF 52 parses
/// `->` but ships no implementation; `json_pack` does not collide).
/// Before M4 wires this into the server session: re-home the JSON family
/// next to skardi's other UDF registrations, and check the
/// datafusion-federation interaction — the rewrite runs at analysis,
/// ahead of federation planning, so a remote `data->'k'` that used to
/// push down could become a local `json_get` and a full scan (recorded
/// in the design's M4 milestone).
///
/// # Example
/// ```
/// use std::collections::HashMap;
/// use std::sync::{Arc, RwLock};
/// use datafusion::prelude::SessionContext;
/// use skardi::sources::providers::graph::udtf::{GraphSources, register_graph_udtfs};
///
/// let sources: GraphSources = Arc::new(RwLock::new(HashMap::new()));
/// let ctx = SessionContext::new();
/// register_graph_udtfs(&ctx, sources).unwrap();
/// // cypher_query('kg', …) now plans; with no source registered it
/// // fails at planning naming the (empty) roster.
/// ```
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
    // Poisoning degrades gracefully (AGENTS.md: the optimizer-registry /
    // model-cache pattern) — the map holds only Arc'd handles, so a
    // panicked writer cannot leave it half-updated in a harmful way.
    let map = sources.read().unwrap_or_else(|p| p.into_inner());
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

        // Whether `columns` may be omitted is the BACKEND's contract
        // (resolved through the handle, still at plan time): the
        // requirement REASON comes from the backend itself, so this
        // shared code never hardcodes one backend's binding rules.
        let handle = lookup(&self.sources, &connection)?;
        let columns = match columns_json {
            Some(text) => Some(Arc::new(parse_columns(&text).map_err(plan_error)?)),
            None => match handle.client.declared_columns_requirement() {
                Some(reason) => {
                    return plan_err!("cypher_query: {reason} (accepted types: {ACCEPTED_TYPES})");
                }
                None => None,
            },
        };
        Ok(Arc::new(CypherQueryProvider {
            handle,
            cypher,
            params,
            columns,
        }))
    }
}

/// The JSON-`record` fallback's one-column schema: each row is the whole
/// Cypher record as canonical JSON text (keys sorted — the Bolt driver
/// hands records over as hash maps, so RETURN order is not recoverable).
/// Static: it is consulted on every planning pass and every fallback
/// scan and never changes.
fn record_fallback_columns() -> Arc<Vec<DeclaredColumn>> {
    static COLUMNS: std::sync::LazyLock<Arc<Vec<DeclaredColumn>>> =
        std::sync::LazyLock::new(|| {
            Arc::new(vec![DeclaredColumn {
                name: "record".to_string(),
                ty: GraphType::Json,
            }])
        });
    Arc::clone(&COLUMNS)
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
/// schema (declared columns, or the record fallback), backend touched
/// only at execute.
#[derive(Debug)]
struct CypherQueryProvider {
    handle: Arc<GraphSourceHandle>,
    cypher: String,
    params: Value,
    /// `None` is the JSON-`record` fallback (never on AGE — the call
    /// refuses it at plan time there).
    columns: Option<Arc<Vec<DeclaredColumn>>>,
}

#[async_trait]
impl TableProvider for CypherQueryProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        match &self.columns {
            Some(columns) => declared_schema(columns),
            None => declared_schema(&record_fallback_columns()),
        }
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(GraphScanExec::new(
            GraphScanKind::Cypher {
                handle: Arc::clone(&self.handle),
                cypher: self.cypher.clone(),
                params: self.params.clone(),
                columns: self.columns.clone(),
                // LIMIT pushes to the CONSUMPTION side: the client stops
                // reading the backend stream after this many rows instead
                // of pulling max_rows and letting DataFusion truncate.
                limit,
            },
            self.schema(),
            projection.cloned(),
        )?))
    }
}

/// `graph_schema('connection')` — the agent-discovery surface: one row
/// per label `(label, kind, property, property_type)`, straight off the
/// backend catalog. Names and types only, never property values.
///
/// The property columns are filled per backend, by what each catalog
/// actually knows: Neo4j serves property names and types via
/// `db.schema.nodeTypeProperties()` / `relTypeProperties()` (one row per
/// label × property; a property-less label keeps one row with nulls);
/// on AGE they are ALWAYS null, structurally — `ag_catalog` records
/// label names and kinds only (AGE is schema-optional — properties are
/// untyped agtype maps with no catalog declaration), and property
/// discovery would mean scanning data, unbounded on the agent's FIRST
/// call. Kuzu's typed catalog arrives with its milestone.
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
        // Nullable BY MEANING: null is "this backend's catalog declares
        // no properties" (all of AGE) or "this label has none" (Neo4j).
        Field::new("property", DataType::Utf8, true),
        Field::new("property_type", DataType::Utf8, true),
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
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(GraphScanExec::new(
            GraphScanKind::Labels {
                handle: Arc::clone(&self.handle),
                // The SQL LIMIT rides into the catalog fetch itself.
                limit,
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
        /// `None` is the record fallback.
        columns: Option<Arc<Vec<DeclaredColumn>>>,
        limit: Option<usize>,
    },
    Labels {
        handle: Arc<GraphSourceHandle>,
        limit: Option<usize>,
    },
}

impl fmt::Debug for GraphScanKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The Cypher text and params are caller data — identity only.
        match self {
            Self::Cypher { columns, .. } => f
                .debug_struct("Cypher")
                .field("columns", &columns.as_ref().map_or(1, |c| c.len()))
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
            // Final, honestly: the milestone-1 client buffers the whole
            // result before the first batch is emitted (the repo's
            // buffering scans — postgres/mysql/mongo/sqlite — all declare
            // Final; Incremental is for genuinely streaming plans, and
            // the optimizer takes the declaration at its word).
            EmissionType::Final,
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
            GraphScanKind::Cypher {
                columns: Some(columns),
                ..
            } => {
                write!(f, "GraphScanExec: cypher_query columns={}", columns.len())
            }
            GraphScanKind::Cypher { columns: None, .. } => {
                write!(f, "GraphScanExec: cypher_query record-fallback")
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
                limit,
            } => cypher_batches(
                Arc::clone(handle),
                cypher.clone(),
                params.clone(),
                columns.clone(),
                *limit,
            ),
            GraphScanKind::Labels { handle, limit } => labels_batch(Arc::clone(handle), *limit),
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
    columns: Option<Arc<Vec<DeclaredColumn>>>,
    limit: Option<usize>,
) -> futures::stream::BoxStream<'static, DFResult<RecordBatch>> {
    stream::once(async move {
        let rows = handle
            .client
            .execute(
                &cypher,
                &params,
                columns.as_ref().map(|c| c.as_slice()),
                handle.bounds,
                limit,
            )
            .await
            .map_err(execution_error)?
            .try_collect::<Vec<_>>()
            .await
            .map_err(execution_error)?;
        // Conversion always runs against concrete columns: the declared
        // ones, or the fallback's single `record: json`.
        let columns = columns.unwrap_or_else(record_fallback_columns);
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
    limit: Option<usize>,
) -> futures::stream::BoxStream<'static, DFResult<RecordBatch>> {
    stream::once(async move {
        let rows = handle
            .client
            .schema(handle.bounds, limit)
            .await
            .map_err(execution_error)?;
        let mut names = arrow::array::StringBuilder::new();
        let mut kinds = arrow::array::StringBuilder::new();
        let mut properties = arrow::array::StringBuilder::new();
        let mut property_types = arrow::array::StringBuilder::new();
        for row in &rows {
            names.append_value(&row.label);
            kinds.append_value(&row.kind);
            properties.append_option(row.property.as_deref());
            property_types.append_option(row.property_type.as_deref());
        }
        RecordBatch::try_new(
            graph_schema_schema(),
            vec![
                Arc::new(names.finish()),
                Arc::new(kinds.finish()),
                Arc::new(properties.finish()),
                Arc::new(property_types.finish()),
            ],
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
    use arrow::array::Array;
    use futures::stream::BoxStream;

    /// Canned-rows client: the UDTF/exec seam without a backend.
    /// `declared_required` flips it between the two backend contracts
    /// (true = AGE-shaped, false = Neo4j-shaped with record fallback).
    #[derive(Debug)]
    struct MockClient {
        rows: Vec<Vec<Value>>,
        schema_rows: Vec<super::super::client::SchemaRow>,
        declared_required: bool,
    }

    #[async_trait]
    impl GraphClient for MockClient {
        async fn execute(
            &self,
            _cypher: &str,
            _params: &Value,
            columns: Option<&[DeclaredColumn]>,
            _bounds: QueryBounds,
            limit: Option<usize>,
        ) -> Result<BoxStream<'static, Result<Vec<Value>, GraphError>>, GraphError> {
            assert!(
                columns.is_some() || !self.declared_required,
                "the UDTF must not reach a declared-required client without columns"
            );
            let mut rows = self.rows.clone();
            if let Some(l) = limit {
                rows.truncate(l);
            }
            Ok(stream::iter(rows.into_iter().map(Ok)).boxed())
        }

        fn declared_columns_requirement(&self) -> Option<&'static str> {
            self.declared_required.then_some(
                "'columns' is required on the age backend — declare the output \
                 columns IN THE SAME ORDER AS YOUR RETURN CLAUSE",
            )
        }

        async fn schema(
            &self,
            _bounds: QueryBounds,
            limit: Option<usize>,
        ) -> Result<Vec<super::super::client::SchemaRow>, GraphError> {
            let mut rows = self.schema_rows.clone();
            if let Some(l) = limit {
                rows.truncate(l);
            }
            Ok(rows)
        }
    }

    fn schema_row(
        label: &str,
        kind: &str,
        property: Option<&str>,
        property_type: Option<&str>,
    ) -> super::super::client::SchemaRow {
        super::super::client::SchemaRow {
            label: label.to_string(),
            kind: kind.to_string(),
            property: property.map(str::to_string),
            property_type: property_type.map(str::to_string),
        }
    }

    fn sources_shaped(rows: Vec<Vec<Value>>, declared_required: bool) -> GraphSources {
        let handle = Arc::new(GraphSourceHandle {
            client: Arc::new(MockClient {
                rows,
                schema_rows: vec![
                    schema_row("Person", "vertex", Some("name"), Some("String")),
                    schema_row("KNOWS", "edge", None, None),
                ],
                declared_required,
            }),
            bounds: QueryBounds {
                timeout: std::time::Duration::from_secs(5),
                max_rows: 100,
            },
        });
        Arc::new(RwLock::new(HashMap::from([("kg".to_string(), handle)])))
    }

    fn sources_with(rows: Vec<Vec<Value>>) -> GraphSources {
        sources_shaped(rows, true)
    }

    async fn ctx_with(rows: Vec<Vec<Value>>) -> SessionContext {
        let ctx = SessionContext::new();
        register_graph_udtfs(&ctx, sources_with(rows)).expect("registration");
        ctx
    }

    /// A Neo4j-shaped session: declared columns optional.
    async fn ctx_record_capable(rows: Vec<Vec<Value>>) -> SessionContext {
        let ctx = SessionContext::new();
        register_graph_udtfs(&ctx, sources_shaped(rows, false)).expect("registration");
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
    async fn omitted_columns_is_the_record_fallback_where_the_backend_allows() {
        // A record-capable (Neo4j-shaped) client: each row is one whole
        // record object, and the planned schema is the single `record`
        // Utf8 column carrying it as JSON text.
        let ctx = ctx_record_capable(vec![
            vec![serde_json::json!({"name": "ada", "age": 36})],
            vec![serde_json::json!({"name": "bob", "age": 41})],
        ])
        .await;
        let batches = collect(
            &ctx,
            "SELECT record FROM cypher_query('kg', 'MATCH (p) RETURN p.name, p.age')",
        )
        .await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        assert_eq!(batches[0].schema().field(0).name(), "record");
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        // Verbatim JSON text — and queryable in place via the registered
        // json getters.
        assert!(col.value(0).contains("\"ada\""), "{}", col.value(0));
        let extracted = collect(
            &ctx,
            "SELECT json_get_str(record, 'name') AS name FROM \
             cypher_query('kg', 'MATCH (p) RETURN p.name, p.age') ORDER BY name",
        )
        .await;
        let names = extracted[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "ada");
    }

    #[tokio::test]
    async fn record_fallback_keeps_its_schema_on_empty_results() {
        let ctx = ctx_record_capable(vec![]).await;
        let batches = collect(
            &ctx,
            "SELECT * FROM cypher_query('kg', 'MATCH (p) RETURN p')",
        )
        .await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
        assert_eq!(batches[0].schema().field(0).name(), "record");
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
    async fn sql_limit_pushes_to_the_consumption_side() {
        // LIMIT reaches the client as a consumption bound (the mock
        // truncates, standing in for the AGE client's early stop) — the
        // scan does not pull max_rows and let DataFusion discard.
        let ctx = ctx_with(vec![
            vec![serde_json::json!("a")],
            vec![serde_json::json!("b")],
            vec![serde_json::json!("c")],
        ])
        .await;
        let batches = collect(
            &ctx,
            "SELECT name FROM cypher_query('kg', 'MATCH (p) RETURN p.name', '{}', \
             '{\"name\": \"string\"}') LIMIT 1",
        )
        .await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
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
    async fn wrong_arg_counts_fail_with_the_signature_named() {
        let ctx = ctx_with(vec![]).await;
        for sql in [
            "SELECT * FROM cypher_query('kg')",
            "SELECT * FROM cypher_query('kg', 'RETURN 1', '{}', '{\"a\":\"int\"}', 'extra')",
            "SELECT * FROM graph_schema()",
            "SELECT * FROM graph_schema('kg', 'extra')",
        ] {
            let err = ctx.sql(sql).await.expect_err(sql);
            let msg = err.to_string();
            assert!(
                msg.contains("expects") || msg.contains("exactly"),
                "{sql}: {msg}"
            );
        }
    }

    #[tokio::test]
    async fn malformed_params_and_columns_fail_at_planning() {
        let ctx = ctx_with(vec![]).await;
        // params: unparseable JSON, then a non-object.
        for (params, needle) in [("{not json", "unparseable JSON"), ("[1]", "an array")] {
            let err = ctx
                .sql(&format!(
                    "SELECT * FROM cypher_query('kg', 'MATCH (n) RETURN n', '{params}', \
                     '{{\"n\": \"node\"}}')"
                ))
                .await
                .expect_err(params);
            assert!(err.to_string().contains(needle), "{params}: {err}");
        }
        // columns: unparseable, non-object, empty, non-string type.
        for (columns, needle) in [
            ("{not json", "unparseable JSON"),
            ("[1]", "expected a JSON object"),
            ("{}", "at least one column"),
            ("{\"n\": 7}", "type must be a string"),
        ] {
            let err = ctx
                .sql(&format!(
                    "SELECT * FROM cypher_query('kg', 'MATCH (n) RETURN n', '{{}}', '{columns}')"
                ))
                .await
                .expect_err(columns);
            assert!(err.to_string().contains(needle), "{columns}: {err}");
        }
    }

    #[tokio::test]
    async fn projection_prunes_columns_and_explain_renders_the_plan() {
        let ctx = ctx_with(vec![vec![serde_json::json!("ada"), serde_json::json!(1)]]).await;
        // Projection: only the second declared column is scanned out.
        let batches = collect(
            &ctx,
            "SELECT n FROM cypher_query('kg', 'MATCH (p) RETURN p.name, p.n', '{}', \
             '{\"name\": \"string\", \"n\": \"int\"}')",
        )
        .await;
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "n");
        // EXPLAIN drives DisplayAs for both scan kinds.
        let plan = collect(
            &ctx,
            "EXPLAIN SELECT n FROM cypher_query('kg', 'MATCH (p) RETURN p.n', '{}', \
             '{\"n\": \"int\"}')",
        )
        .await;
        assert!(!plan.is_empty());
        let plan = collect(&ctx, "EXPLAIN SELECT * FROM graph_schema('kg')").await;
        assert!(!plan.is_empty());
    }

    #[tokio::test]
    async fn an_unprojected_declared_column_still_fails_its_type_contract() {
        // The projection contract (see CONVERSION_BATCH_ROWS' doc): the
        // declared schema is the contract, projection prunes AFTER
        // conversion — `b` mis-declared fails even though only `a` was
        // selected.
        let ctx = ctx_with(vec![vec![
            serde_json::json!("ada"),
            serde_json::json!("not-an-int"),
        ]])
        .await;
        let err = ctx
            .sql(
                "SELECT a FROM cypher_query('kg', 'MATCH (x) RETURN x.a, x.b', '{}', \
                 '{\"a\": \"string\", \"b\": \"int\"}')",
            )
            .await
            .expect("plans")
            .collect()
            .await
            .expect_err("the unprojected column's violated declaration is still loud");
        let msg = err.to_string();
        assert!(msg.contains("'b'"), "{msg}");
        assert!(msg.contains("declared 'int'"), "{msg}");
    }

    #[tokio::test]
    async fn graph_schema_respects_a_sql_limit() {
        let ctx = ctx_with(vec![]).await;
        let batches = collect(&ctx, "SELECT label FROM graph_schema('kg') LIMIT 1").await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }

    #[tokio::test]
    async fn graph_schema_lists_labels_kinds_and_per_backend_properties() {
        let ctx = ctx_with(vec![]).await;
        let batches = collect(
            &ctx,
            "SELECT label, kind, property, property_type FROM graph_schema('kg') \
             ORDER BY label",
        )
        .await;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
        let batch = &batches[0];
        let property = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        // KNOWS sorts first: a property-less label keeps its row, nulls
        // in the property columns; Person carries (name, String).
        assert!(property.is_null(0));
        assert_eq!(property.value(1), "name");
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
