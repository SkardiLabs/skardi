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
//! `params` — `'{}'` is the no-parameters spelling. `connection`,
//! `cypher`, and `columns` are strict string literals (connection decides
//! the plan-time source lookup, columns decide the plan-time schema, and
//! cypher is what the plan-time guard screens); `params` alone accepts a
//! bare NULL as the pipeline schema-inference placeholder, read as the
//! empty object (design Risks #0).
//!
//! **Pipeline usage**: the params placeholder occupies the WHOLE argument
//! position — `cypher_query('kg', 'MATCH (u:User) WHERE u.id = $uid
//! RETURN u.name', {params}, '{"name": "string"}')`. At inference the
//! placeholder becomes NULL (no params); at request time the substituted
//! value is a string literal carrying JSON text, parsed as the params
//! object.
//!
//! Milestone 1 is AGE-only, and AGE's `cypher()` call must declare its
//! result arity — so `columns` is REQUIRED here: omitting it is a
//! targeted error, and the JSON-`record` fallback ships with the Neo4j
//! milestone, where Bolt needs no declared arity.
//!
//! **Declared-column ORDER is load-bearing**: the binding to the Cypher
//! `RETURN` clause is positional (all AGE's `cypher()` gives us), so
//! `columns` must list them in RETURN order. Two same-typed columns
//! declared out of order swap SILENTLY — same JSON kind, no
//! `TypeMismatch`, nothing downstream can tell — which is also the
//! mis-declaration an LLM is most likely to produce. The error message,
//! this doc, and the design's §Schema handling all state it because no
//! structural check is possible.

use std::any::Any;
use std::collections::{HashMap, HashSet};
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
use crate::sources::providers::udtf_args::{strict_string_arg, string_arg};

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
    /// Registration-time health (design §Schema handling): a source whose
    /// backend was unreachable at registration is Degraded with the
    /// registration error's summary; the first scan retries validation
    /// and flips this back to Healthy on success.
    ///
    /// The flip is ONE-WAY: nothing moves a Healthy source back to
    /// Degraded, deliberately — a post-recovery failure can be the
    /// CALLER's (bad Cypher, a mis-typed declaration) as easily as the
    /// backend's, and flipping on any error would mark healthy sources
    /// degraded over queries they never could have served. Read this as
    /// "registered degraded and not yet recovered", never as a liveness
    /// probe — /data_source's status field carries the same caveat.
    pub health: Arc<RwLock<GraphSourceHealth>>,
    /// The validation contract of every YAML view on this source. The
    /// degraded recovery re-proves them to answer ONE question — "did the
    /// backend come back?" — not to re-litigate every contract: any
    /// response from the server (validation success OR a contract
    /// violation) flips the source Healthy, and a still-broken view then
    /// fails its OWN scans with the typed error execution already
    /// produces (arity from the backend, types from conversion,
    /// nullability from build_batch). Only availability artifacts (the
    /// backend did not answer) keep the source Degraded. Empty for
    /// view-less sources (the engine API included), where a successful
    /// query is itself the recovery evidence.
    pub view_contracts: Arc<Vec<super::view::ViewContract>>,
    /// SINGLE-FLIGHT gate for the degraded recovery: concurrent first
    /// scans of a degraded source would each re-validate every view —
    /// requests × views backend probes, and (with the pool saturated by
    /// the winners) queued losers converting wait into spurious acquire
    /// timeouts. Recovery acquires this, RE-CHECKS health (the winner
    /// already flipped it), and only the first arrival pays the
    /// validation; the rest see Healthy on wake-up. Never held across a
    /// healthy fast path — that stays a lock-free read.
    pub recovery_gate: Arc<tokio::sync::Mutex<()>>,
    /// Backoff for FAILED recovery attempts: while the backend stays
    /// down, every query would otherwise re-pay the full re-validation
    /// (N views × timeout, serialized behind the gate — a dashboard
    /// refresh against an afternoon-long outage becomes a pile-up).
    /// Holds the instant of the last availability-failed attempt; the
    /// next attempt before `recovery_backoff_interval` elapses returns
    /// the cached degraded error instead. `tokio::time::Instant`, so the
    /// paused-clock tests can drive it.
    pub last_failed_recovery: Arc<std::sync::Mutex<Option<tokio::time::Instant>>>,
    /// The bounded-concurrency limit for view (re)validation — the
    /// pool's own `max_connections`, so no probe ever queues behind its
    /// siblings in the acquire queue (see
    /// `view::validate_views_concurrently`).
    pub validation_limit: usize,
}

/// Registration-time health of a graph source.
#[derive(Debug, Clone)]
pub enum GraphSourceHealth {
    /// The backend answered at registration (and every view validated).
    Healthy,
    /// The backend was unreachable at registration; the payload is the
    /// registration error's summary, for diagnostics.
    Degraded(String),
}

impl GraphSourceHealth {
    /// Whether the backend validated at registration.
    pub fn is_healthy(&self) -> bool {
        matches!(self, Self::Healthy)
    }
}

/// Shared map of connection name → handle, owned by the front-end the
/// way `OpenConnectorGateways` is.
pub type GraphSources = Arc<RwLock<HashMap<String, Arc<GraphSourceHandle>>>>;

/// Register `cypher_query` and `graph_schema` on a session.
///
/// The JSON getter family (`json_get`, `json_get_str`, …) is
/// deliberately NOT registered here: `datafusion-functions-json`'s
/// `register_all` also installs an expr planner rewriting the SQL
/// operators `->`, `->>` and `?` session-wide — a side effect that must
/// not hide behind a graph-only registration (a remote `data->'k'` that
/// pushes down today could become a local `json_get` over a full scan —
/// the datafusion-federation interaction recorded in the design's M4
/// milestone). The server session registers the getters unconditionally
/// (`util::json_getters::register_json_getter_udfs`, wired with the
/// server task); engine-API users who need them register the individual
/// UDFs themselves (`datafusion_functions_json::udfs::json_get_str_udf()`
/// and siblings).
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
        // params accepts the pipeline-inference NULL placeholder (design
        // Risks #0): NULL reads as the empty object below, exactly the
        // `Some("")` case — inference plans with no params, execution
        // gets the substituted JSON text.
        let params_json = exprs
            .get(2)
            .map(|e| string_arg(e, "cypher_query", "params_json"))
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
                 output columns IN THE SAME ORDER AS YOUR RETURN CLAUSE (the binding \
                 is positional; two same-typed columns declared out of order swap \
                 silently), e.g. '{{\"name\": \"string\", \"n\": \"node\"}}' \
                 (accepted types: {})",
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
    // serde_json's object parse REPLACES duplicate keys silently, so a
    // declaration like '{"n": "int", "n": "string"}' would otherwise
    // slip through as one column — the last type wins and the author's
    // first declaration is dropped without a word. Re-walk the raw text
    // as key/value PAIRS to catch what the map has already collapsed.
    let pairs = object_pairs(text).map_err(|e| GraphError::InvalidColumns {
        reason: format!("unparseable JSON ({e})"),
        accepted: ACCEPTED_TYPES,
    })?;
    let mut seen = HashSet::with_capacity(pairs.len());
    for (name, _) in &pairs {
        if !seen.insert(name.as_str()) {
            return Err(GraphError::InvalidColumns {
                reason: format!("column '{name}' is declared twice"),
                accepted: ACCEPTED_TYPES,
            });
        }
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
            Ok(DeclaredColumn {
                name,
                ty,
                // The ad-hoc surface cannot declare nullability (design
                // §Schema handling) — every ad-hoc column is nullable.
                nullable: true,
            })
        })
        .collect()
}

/// The declared-columns object as raw key/value PAIRS, duplicates
/// preserved — the duplicate-detection walk [`parse_columns`] runs after
/// serde_json's own object parse has already collapsed repeats.
fn object_pairs(text: &str) -> Result<Vec<(String, Value)>, serde_json::Error> {
    struct Pairs;
    impl<'de> serde::de::Visitor<'de> for Pairs {
        type Value = Vec<(String, Value)>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a JSON object")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut access: A,
        ) -> Result<Self::Value, A::Error> {
            let mut pairs = Vec::new();
            while let Some(entry) = access.next_entry::<String, Value>()? {
                pairs.push(entry);
            }
            Ok(pairs)
        }
    }
    let mut de = serde_json::Deserializer::from_str(text);
    let pairs = serde::de::Deserializer::deserialize_map(&mut de, Pairs)?;
    de.end()?;
    Ok(pairs)
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
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(GraphScanExec::new(
            GraphScanKind::Cypher {
                handle: Arc::clone(&self.handle),
                cypher: self.cypher.clone(),
                params: self.params.clone(),
                columns: Arc::clone(&self.columns),
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
/// per label, `(label, kind)`, straight off the backend catalog. Names
/// only, never property values.
///
/// No property columns on AGE, structurally: `ag_catalog` records label
/// names and kinds ONLY (AGE is schema-optional — properties are
/// untyped agtype maps with no catalog declaration), and property
/// discovery would mean scanning data, unbounded on the agent's FIRST
/// call. Property names/types arrive with the Neo4j
/// (`db.schema.nodeTypeProperties()`) and Kuzu (typed catalog)
/// milestones, whose catalogs actually carry them.
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

/// What one graph scan executes. `pub(crate)` for the YAML view
/// provider (`view.rs`), which scans through the same leaf plan.
pub(crate) enum GraphScanKind {
    Cypher {
        handle: Arc<GraphSourceHandle>,
        cypher: String,
        params: Value,
        columns: Arc<Vec<DeclaredColumn>>,
        limit: Option<usize>,
    },
    /// A YAML view scan: fixed Cypher, no params, and the degraded
    /// recovery (re-validating ALL the source's view contracts) runs
    /// inside the lazy stream on first poll — never during plan
    /// construction (`TableProvider::scan` is physical planning; the
    /// design forbids network I/O there).
    View {
        handle: Arc<GraphSourceHandle>,
        view_name: String,
        cypher: String,
        columns: Arc<Vec<DeclaredColumn>>,
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
                .field("columns", &columns.len())
                .finish_non_exhaustive(),
            Self::View { view_name, .. } => f
                .debug_struct("View")
                .field("view", view_name)
                .finish_non_exhaustive(),
            Self::Labels { .. } => f.debug_struct("Labels").finish_non_exhaustive(),
        }
    }
}

/// Leaf plan: one partition, executes the graph call on first poll.
/// `pub(crate)` for the YAML view provider (`view.rs`).
#[derive(Debug)]
pub(crate) struct GraphScanExec {
    kind: GraphScanKind,
    projection: Option<Vec<usize>>,
    properties: PlanProperties,
}

impl GraphScanExec {
    pub(crate) fn new(
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
            GraphScanKind::Cypher { columns, .. } => {
                write!(f, "GraphScanExec: cypher_query columns={}", columns.len())
            }
            GraphScanKind::View { view_name, .. } => {
                write!(f, "GraphScanExec: view {view_name}")
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
                Arc::clone(columns),
                *limit,
            ),
            GraphScanKind::View {
                handle,
                view_name,
                cypher,
                columns,
                limit,
            } => super::view::view_batches(
                Arc::clone(handle),
                view_name.clone(),
                cypher.clone(),
                Arc::clone(columns),
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

/// The UDTF twin of the view path's degraded retry (view.rs's
/// `ensure_healthy`): for a source registered DEGRADED, the caller's own
/// query IS the re-validation — an ad-hoc call has no separate probe. A
/// failure is wrapped with the registration error, so the real cause
/// (the dial refused at startup) survives next to the fresh failure
/// instead of surfacing as a bare acquire timeout; a success flips the
/// source Healthy — the ONLY recovery route for a view-less or
/// UDTF-only source, since view scans are what otherwise flip it.
pub(crate) fn degraded_reason(handle: &GraphSourceHandle) -> Option<String> {
    let health = handle.health.read().unwrap_or_else(|p| p.into_inner());
    match &*health {
        GraphSourceHealth::Healthy => None,
        GraphSourceHealth::Degraded(reason) => Some(reason.clone()),
    }
}

/// Flip a recovered source Healthy. Idempotent — racing scanners may
/// both write; the transition is benign either way.
fn mark_healthy(handle: &GraphSourceHandle) {
    *handle.health.write().unwrap_or_else(|p| p.into_inner()) = GraphSourceHealth::Healthy;
}

/// Wrap an execution failure with the degraded registration context
/// when the source is degraded; pass healthy sources' errors through
/// untouched (they never carried a registration failure). The
/// registration reason names the source (the clients build it from
/// `backend_error`, whose text carries `on '{source}'`).
fn degraded_execution_error(degraded: Option<String>, e: GraphError) -> DataFusionError {
    match degraded {
        Some(reason) => DataFusionError::Execution(format!(
            "graph source is registered DEGRADED (registration error: {reason}); \
             the query was retried against the backend and failed: {e}"
        )),
        None => execution_error(e),
    }
}

/// Recovery after a successful backend answer on a degraded source:
/// re-prove the source's view contracts, then flip Healthy. This holds
/// the recovery path to the same contract strength as reachable
/// registration (which refuses a source unless EVERY view validates) —
/// otherwise a contract-violating view would silently downgrade from
/// "registration refused" to "scan-time conversion error" the moment an
/// ad-hoc query flipped the source. For a view-less source there is
/// nothing to re-prove and the successful query is itself the evidence.
/// NEVER fails the caller: on this path the caller's own query has
/// ALREADY SUCCEEDED, and whether an UNRELATED view honors its contract
/// is a different question from whether these rows are good — failing
/// here would discard a correct answer, permanently (a source with one
/// mis-declared view would fail every ad-hoc query and graph_schema —
/// the agent's discovery surface — forever, with a restart then refusing
/// to start). Instead: flip Healthy only on a clean re-validation, stay
/// Degraded otherwise with a warning naming the cause, and let the
/// broken view's OWN scan be the loud failure (view.rs::ensure_healthy
/// keeps that behaviour).
async fn recover_if_degraded(handle: &Arc<GraphSourceHandle>, degraded: bool) {
    if !degraded {
        return;
    }
    // SINGLE-FLIGHT with the view path's recovery (the same gate):
    // concurrent recoveries would each re-prove every view. Re-check
    // under the gate — a racing winner (view scan or sibling query)
    // already flipped the source and there is nothing left to prove.
    let _gate = handle.recovery_gate.lock().await;
    if degraded_reason(handle).is_none() {
        return;
    }
    // Inside the failed-recovery backoff window, don't re-pay the
    // re-validation: the caller's own query already succeeded (or the
    // caller's own failure is being reported), and the view path owns
    // the loud degraded diagnostics.
    if super::view::recovery_backoff_remaining(handle).is_some() {
        return;
    }
    match super::view::revalidate_all_views(handle).await {
        // The backend answered — success or a contract violation both
        // flip Healthy; a broken view's own scans carry its typed error.
        Ok(()) => mark_healthy(handle),
        Err(e) if !super::view::recovery_keeps_degraded(&e) => {
            tracing::warn!(
                error = %e,
                "graph source recovered (the backend answered), but a view failed \
                 re-validation — its own scans will report this"
            );
            mark_healthy(handle);
        }
        Err(e) => {
            super::view::arm_recovery_backoff(handle);
            tracing::warn!(
                error = %e,
                "the ad-hoc query succeeded but view re-validation found the backend \
                 unavailable — the source stays degraded"
            );
        }
    }
}

/// The cypher scan: run on first poll, then convert in batch-atomic
/// chunks (design §Schema handling — the conversion batch is the defined
/// atomic unit; a type mismatch fails the CURRENT batch before emission).
pub(crate) fn cypher_batches(
    handle: Arc<GraphSourceHandle>,
    cypher: String,
    params: Value,
    columns: Arc<Vec<DeclaredColumn>>,
    limit: Option<usize>,
) -> futures::stream::BoxStream<'static, DFResult<RecordBatch>> {
    stream::once(async move {
        let degraded = degraded_reason(&handle);
        let rows = match handle
            .client
            .execute(&cypher, &params, columns.len(), handle.bounds, limit)
            .await
        {
            Ok(stream) => stream.try_collect::<Vec<_>>().await,
            Err(e) => Err(e),
        };
        let rows = match (rows, degraded) {
            (Ok(rows), degraded) => {
                // The backend answered — re-prove the view contracts and
                // flip Healthy (a no-op for healthy sources).
                recover_if_degraded(&handle, degraded.is_some()).await;
                rows
            }
            (Err(e), degraded) => return Err(degraded_execution_error(degraded, e)),
        };
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
        let degraded = degraded_reason(&handle);
        let labels = match handle.client.labels(handle.bounds, limit).await {
            Ok(labels) => {
                recover_if_degraded(&handle, degraded.is_some()).await;
                labels
            }
            Err(e) => return Err(degraded_execution_error(degraded, e)),
        };
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
            limit: Option<usize>,
        ) -> Result<BoxStream<'static, Result<Vec<Value>, GraphError>>, GraphError> {
            let mut rows = self.rows.clone();
            if let Some(l) = limit {
                rows.truncate(l);
            }
            Ok(stream::iter(rows.into_iter().map(Ok)).boxed())
        }

        async fn labels(
            &self,
            _bounds: QueryBounds,
            limit: Option<usize>,
        ) -> Result<Vec<(String, String)>, GraphError> {
            let mut labels = self.labels.clone();
            if let Some(l) = limit {
                labels.truncate(l);
            }
            Ok(labels)
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
            health: Arc::new(RwLock::new(GraphSourceHealth::Healthy)),
            view_contracts: Arc::new(vec![]),
            recovery_gate: Arc::new(tokio::sync::Mutex::new(())),
            last_failed_recovery: Arc::new(std::sync::Mutex::new(None)),
            validation_limit: 4,
        });
        Arc::new(RwLock::new(HashMap::from([("kg".to_string(), handle)])))
    }

    async fn ctx_with(rows: Vec<Vec<Value>>) -> SessionContext {
        let ctx = SessionContext::new();
        register_graph_udtfs(&ctx, sources_with(rows)).expect("registration");
        // The getter family is the SESSION's registration, not the graph
        // UDTFs' (see register_graph_udtfs' doc) — the test session
        // registers only what it uses, never register_all (its `->`
        // expr-planner rewrite is the session-level side effect the
        // re-home removed).
        ctx.register_udf((*datafusion_functions_json::udfs::json_get_str_udf()).clone());
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

    /// A client whose every call fails with the given backend error —
    /// the degraded-retry tests' still-down backend.
    #[derive(Debug)]
    struct FailingClient {
        message: String,
    }

    #[async_trait]
    impl GraphClient for FailingClient {
        async fn execute(
            &self,
            _cypher: &str,
            _params: &Value,
            _arity: usize,
            _bounds: QueryBounds,
            _limit: Option<usize>,
        ) -> Result<BoxStream<'static, Result<Vec<Value>, GraphError>>, GraphError> {
            Err(GraphError::backend("kg", "io", &self.message))
        }

        async fn labels(
            &self,
            _bounds: QueryBounds,
            _limit: Option<usize>,
        ) -> Result<Vec<(String, String)>, GraphError> {
            Err(GraphError::backend("kg", "io", &self.message))
        }
    }

    fn sources_with_health(
        health: GraphSourceHealth,
        client: Arc<dyn GraphClient>,
    ) -> GraphSources {
        let handle = Arc::new(GraphSourceHandle {
            client,
            bounds: QueryBounds {
                timeout: std::time::Duration::from_secs(5),
                max_rows: 100,
            },
            health: Arc::new(RwLock::new(health)),
            view_contracts: Arc::new(vec![]),
            recovery_gate: Arc::new(tokio::sync::Mutex::new(())),
            last_failed_recovery: Arc::new(std::sync::Mutex::new(None)),
            validation_limit: 4,
        });
        Arc::new(RwLock::new(HashMap::from([("kg".to_string(), handle)])))
    }

    fn health_of(sources: &GraphSources) -> GraphSourceHealth {
        sources
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get("kg")
            .expect("kg registered")
            .health
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    #[tokio::test]
    async fn a_degraded_source_reports_the_registration_reason_not_a_bare_timeout() {
        // The P1 regression shape: a degraded source's UDTF failure used
        // to surface as a bare acquire/statement timeout — the real
        // cause (the dial refused at registration) was swallowed, and
        // the timeout's "narrow the traversal" advice was actively
        // misleading for a backend that was never reached.
        let sources = sources_with_health(
            GraphSourceHealth::Degraded(
                "graph backend error on 'kg' [io]: Connection refused".to_string(),
            ),
            Arc::new(FailingClient {
                message: "could not acquire a connection".to_string(),
            }),
        );
        let ctx = SessionContext::new();
        register_graph_udtfs(&ctx, Arc::clone(&sources)).expect("registration");
        let err = ctx
            .sql(
                "SELECT name FROM cypher_query('kg', 'MATCH (p) RETURN p.name', '{}', \
                 '{\"name\": \"string\"}')",
            )
            .await
            .expect("plans")
            .collect()
            .await
            .expect_err("the retried query fails");
        let msg = err.to_string();
        assert!(msg.contains("DEGRADED"), "{msg}");
        assert!(
            msg.contains("Connection refused"),
            "the registration error survives: {msg}"
        );
        assert!(
            msg.contains("could not acquire a connection"),
            "the fresh failure rides along: {msg}"
        );
        assert!(
            !msg.contains("narrow the traversal"),
            "no misleading advice: {msg}"
        );
        // A failed retry leaves the source degraded.
        assert!(!health_of(&sources).is_healthy());

        // graph_schema takes the same path.
        let err = ctx
            .sql("SELECT * FROM graph_schema('kg')")
            .await
            .expect("plans")
            .collect()
            .await
            .expect_err("labels fail too");
        assert!(err.to_string().contains("DEGRADED"), "{err}");
    }

    #[tokio::test]
    async fn a_successful_query_on_a_degraded_source_flips_it_healthy() {
        // The UDTF path is the ONLY recovery route for a view-less (or
        // UDTF-only) source: without the flip, /data_source would report
        // degraded forever even after the backend recovered.
        let sources = sources_with_health(
            GraphSourceHealth::Degraded("connection refused at startup".to_string()),
            Arc::new(MockClient {
                rows: vec![vec![serde_json::json!("ada")]],
                labels: vec![("Person".to_string(), "vertex".to_string())],
            }),
        );
        let ctx = SessionContext::new();
        register_graph_udtfs(&ctx, Arc::clone(&sources)).expect("registration");
        let batches = collect(
            &ctx,
            "SELECT name FROM cypher_query('kg', 'MATCH (p) RETURN p.name', '{}', \
             '{\"name\": \"string\"}')",
        )
        .await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        assert!(
            health_of(&sources).is_healthy(),
            "a successful retry flips the source healthy"
        );
    }

    #[tokio::test]
    async fn a_recovered_query_keeps_its_rows_when_a_sibling_view_fails_revalidation() {
        // The recovery pin: the caller's query ALREADY succeeded, so a
        // sibling view breaking its contract must not discard those rows.
        // And because a contract violation is the backend ANSWERING, the
        // source flips healthy — the broken view's own scans carry its
        // typed failure (keeping it degraded would re-pay the whole
        // re-validation on every query and misreport an outage that
        // isn't one).
        let contract = super::super::view::ViewContract {
            name: "people".to_string(),
            cypher: "MATCH (p:Person) RETURN p.name, p.age".to_string(),
            columns: vec![
                DeclaredColumn {
                    name: "name".to_string(),
                    ty: GraphType::String,
                    nullable: true,
                },
                DeclaredColumn {
                    name: "age".to_string(),
                    ty: GraphType::Int,
                    nullable: true,
                },
            ],
        };
        // One value per row: the ad-hoc single-column query succeeds,
        // but the two-column view contract hits an arity mismatch.
        let handle = Arc::new(GraphSourceHandle {
            client: Arc::new(MockClient {
                rows: vec![vec![serde_json::json!("ada")]],
                labels: vec![("Person".to_string(), "vertex".to_string())],
            }),
            bounds: QueryBounds {
                timeout: std::time::Duration::from_secs(5),
                max_rows: 100,
            },
            health: Arc::new(RwLock::new(GraphSourceHealth::Degraded(
                "connection refused at startup".to_string(),
            ))),
            view_contracts: Arc::new(vec![contract]),
            recovery_gate: Arc::new(tokio::sync::Mutex::new(())),
            last_failed_recovery: Arc::new(std::sync::Mutex::new(None)),
            validation_limit: 4,
        });
        let sources: GraphSources =
            Arc::new(RwLock::new(HashMap::from([("kg".to_string(), handle)])));
        let ctx = SessionContext::new();
        register_graph_udtfs(&ctx, Arc::clone(&sources)).expect("registration");
        let batches = collect(
            &ctx,
            "SELECT name FROM cypher_query('kg', 'MATCH (p) RETURN p.name', '{}', \
             '{\"name\": \"string\"}')",
        )
        .await;
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            1,
            "the successful query's rows are emitted, not discarded"
        );
        assert!(
            health_of(&sources).is_healthy(),
            "the backend answered (a contract violation is an answer) — the source \
             recovers; the broken view's own scans report its failure"
        );
    }

    #[tokio::test]
    async fn a_healthy_source_failure_is_not_wrapped_in_degraded_context() {
        let sources = sources_with_health(
            GraphSourceHealth::Healthy,
            Arc::new(FailingClient {
                message: "syntax error at or near".to_string(),
            }),
        );
        let ctx = SessionContext::new();
        register_graph_udtfs(&ctx, sources).expect("registration");
        let err = ctx
            .sql(
                "SELECT name FROM cypher_query('kg', 'MATCH (p) RETURN p.name', '{}', \
                 '{\"name\": \"string\"}')",
            )
            .await
            .expect("plans")
            .collect()
            .await
            .expect_err("the backend error passes through");
        let msg = err.to_string();
        assert!(msg.contains("syntax error at or near"), "{msg}");
        assert!(
            !msg.contains("DEGRADED"),
            "a healthy source never had a registration error to cite: {msg}"
        );
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
    async fn a_duplicate_column_declaration_is_rejected_not_silently_collapsed() {
        // serde_json keeps the LAST duplicate key, so without the pair
        // walk '{"n": "int", "n": "string"}' would plan as a single
        // string column — the author's first declaration silently gone.
        let ctx = ctx_with(vec![]).await;
        let err = ctx
            .sql(
                "SELECT * FROM cypher_query('kg', 'MATCH (p) RETURN p.n', '{}', \
                 '{\"n\": \"int\", \"n\": \"string\"}')",
            )
            .await
            .expect_err("duplicate columns fail at plan time");
        let msg = err.to_string();
        assert!(msg.contains("column 'n' is declared twice"), "{msg}");
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
    async fn json_get_str_extracts_properties_when_the_session_registers_it() {
        // A node's `properties` column is JSON text; the getter family
        // (registered by the session, not by register_graph_udtfs) is
        // what makes it queryable without leaving SQL.
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

    #[tokio::test]
    async fn a_null_params_placeholder_plans_as_no_params() {
        // Pipeline schema inference substitutes `{params}` with a bare
        // NULL (design Risks #0): params — alone among the arguments —
        // accepts it and plans as the empty object; the other arguments
        // stay strict.
        let ctx = ctx_with(vec![vec![serde_json::json!("ada")]]).await;
        let batches = collect(
            &ctx,
            "SELECT name FROM cypher_query('kg', 'MATCH (p) RETURN p.name', NULL, \
             '{\"name\": \"string\"}')",
        )
        .await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        // …while connection/cypher/columns still reject NULL outright.
        for sql in [
            "SELECT * FROM cypher_query(NULL, 'MATCH (n) RETURN n', '{}', '{\"n\": \"node\"}')",
            "SELECT * FROM cypher_query('kg', NULL, '{}', '{\"n\": \"node\"}')",
            "SELECT * FROM cypher_query('kg', 'MATCH (n) RETURN n', '{}', NULL)",
        ] {
            let err = ctx.sql(sql).await.expect_err(sql);
            assert!(err.to_string().contains("not NULL"), "{sql}: {err}");
        }
    }

    #[tokio::test]
    async fn an_unknown_source_on_an_empty_registry_says_none() {
        // The empty-registry arm of the known-sources hint.
        let sources: GraphSources = Arc::new(RwLock::new(HashMap::new()));
        let ctx = SessionContext::new();
        register_graph_udtfs(&ctx, sources).expect("registration");
        let err = ctx
            .sql(
                "SELECT * FROM cypher_query('nope', 'RETURN 1', '{}', \
                 '{\"n\": \"int\"}')",
            )
            .await
            .expect_err("an unknown source fails at plan time");
        let msg = err.to_string();
        assert!(msg.contains("nope"), "{msg}");
        assert!(msg.contains("none"), "the empty registry says so: {msg}");
    }

    #[tokio::test]
    async fn a_stale_degraded_flag_rechecks_under_the_gate_and_returns() {
        // The ad-hoc twin of the view path's gate-loser re-check: the
        // caller captured `degraded = true` at stream construction, but
        // a racing winner already recovered the source — the re-check
        // under the gate must return without touching health.
        let sources = sources_with(vec![]);
        let handle = Arc::clone(
            sources
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .get("kg")
                .expect("kg registered"),
        );
        recover_if_degraded(&handle, true).await;
        assert!(
            health_of(&sources).is_healthy(),
            "a healthy source stays healthy through the stale-flag path"
        );
    }

    #[tokio::test]
    async fn the_leaf_plan_pins_its_contract_and_redacts_cypher() {
        // GraphScanExec's ExecutionPlan plumbing, pinned directly: Debug
        // and Display carry identity only (the Cypher text is caller
        // data — the module-wide redaction rule), the plan is a strict
        // leaf, and only partition 0 exists.
        let sources = sources_with(vec![vec![serde_json::json!("ada")]]);
        let handle = Arc::clone(
            sources
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .get("kg")
                .expect("kg registered"),
        );
        let columns = Arc::new(vec![DeclaredColumn {
            name: "name".to_string(),
            ty: GraphType::String,
            nullable: true,
        }]);
        let secret = "MATCH (creds {token: 'hunter2'}) RETURN creds.name";
        let kinds = [
            GraphScanKind::Cypher {
                handle: Arc::clone(&handle),
                cypher: secret.to_string(),
                params: serde_json::json!({}),
                columns: Arc::clone(&columns),
                limit: None,
            },
            GraphScanKind::View {
                handle: Arc::clone(&handle),
                view_name: "people".to_string(),
                cypher: secret.to_string(),
                columns: Arc::clone(&columns),
                limit: None,
            },
            GraphScanKind::Labels {
                handle: Arc::clone(&handle),
                limit: None,
            },
        ];
        for kind in &kinds {
            let dbg = format!("{kind:?}");
            assert!(
                !dbg.contains("hunter2") && !dbg.contains("MATCH"),
                "the Cypher text never appears in Debug: {dbg}"
            );
        }
        let [_, view_kind, _] = kinds;
        assert!(format!("{view_kind:?}").contains("people"));

        // No projection: the full declared schema is the plan's schema.
        let schema = declared_schema(&columns);
        let exec = Arc::new(
            GraphScanExec::new(view_kind, Arc::clone(&schema), None).expect("plan builds"),
        );
        assert_eq!(exec.schema(), schema);
        let display = format!(
            "{}",
            datafusion::physical_plan::displayable(exec.as_ref()).one_line()
        );
        assert!(display.contains("view people"), "{display}");
        assert!(!display.contains("hunter2"), "{display}");

        // A leaf: no children accepted, only partition 0 served.
        let err = Arc::clone(&exec)
            .with_new_children(vec![Arc::clone(&exec) as Arc<dyn ExecutionPlan>])
            .expect_err("a leaf takes no children");
        assert!(err.to_string().contains("leaf"), "{err}");
        let err = match exec.execute(1, Arc::new(TaskContext::default())) {
            Ok(_) => panic!("only partition 0 exists"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("partition 1"), "{err}");

        // Partition 0, no projection, no limit: the row comes through.
        let stream = exec
            .execute(0, Arc::new(TaskContext::default()))
            .expect("partition 0 executes");
        let batches: Vec<RecordBatch> = stream.try_collect().await.expect("collects");
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }
}
