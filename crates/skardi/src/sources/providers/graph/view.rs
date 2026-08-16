//! YAML-declared catalog views (design §Schema handling, milestone 4):
//! each `views:` entry becomes the catalog table `<source>.main.<name>` —
//! a fixed Cypher query plus a declared schema, with the backend first
//! touched at scan time, never at planning.
//!
//! The degraded state machine lives here: a source registered while its
//! backend was unreachable carries [`GraphSourceHealth::Degraded`], and
//! the FIRST scan of any of its views retries the live validation of ALL
//! the source's views — success flips the source Healthy and the scan
//! proceeds; failure is loud, naming the failing view and the underlying
//! cause. Re-proving every view keeps the recovery path at the same
//! contract strength as the reachable-registration path (which refuses
//! the source unless every view validates): without it, a
//! contract-violating view would silently downgrade from
//! "registration refused" to "scan-time conversion error" the moment any
//! OTHER view's scan flipped the source healthy.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use futures::stream;
use futures::{StreamExt, TryStreamExt};
use serde_json::Value;

use super::error::GraphError;
use super::udtf::{
    GraphScanExec, GraphScanKind, GraphSourceHandle, GraphSourceHealth, degraded_reason,
};
use super::value::{DeclaredColumn, build_batch, declared_schema};

/// One view's validation contract — everything `validate_view` needs to
/// re-prove the view against a recovered backend. Stored on the source
/// handle at registration so the degraded retry can re-validate ALL of
/// the source's views, not just the one being scanned (see the module
/// doc for why recovery must match registration's contract strength).
#[derive(Debug, Clone)]
pub struct ViewContract {
    /// The view (catalog table) name.
    pub name: String,
    /// The view's fixed Cypher text.
    pub cypher: String,
    /// The declared output columns, nullable bits included.
    pub columns: Vec<DeclaredColumn>,
}

/// One YAML-declared view: fixed Cypher + declared schema, registered as
/// a catalog table. The schema is precomputed at registration (the
/// planning-time contract, nullable bits included); the backend is first
/// touched when a scan executes.
pub(crate) struct GraphViewProvider {
    handle: Arc<GraphSourceHandle>,
    view_name: String,
    cypher: String,
    columns: Arc<Vec<DeclaredColumn>>,
    schema: SchemaRef,
}

impl GraphViewProvider {
    pub(crate) fn new(
        handle: Arc<GraphSourceHandle>,
        view_name: String,
        cypher: String,
        columns: Vec<DeclaredColumn>,
    ) -> Self {
        let schema = declared_schema(&columns);
        Self {
            handle,
            view_name,
            cypher,
            columns: Arc::new(columns),
            schema,
        }
    }
}

impl fmt::Debug for GraphViewProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Identity only — the Cypher text is config data and never
        // appears in diagnostics (the module-wide rule).
        f.debug_struct("GraphViewProvider")
            .field("view", &self.view_name)
            .field("columns", &self.columns.len())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for GraphViewProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
        // NO backend contact in this function: TableProvider::scan runs
        // during PHYSICAL PLAN construction (DataFrame::create_physical_plan),
        // and the design's hard rule is that planning performs no network
        // I/O. The degraded recovery lives in the lazy stream built by
        // GraphScanKind::View — it runs on the first poll, not here.
        Ok(Arc::new(GraphScanExec::new(
            GraphScanKind::View {
                handle: Arc::clone(&self.handle),
                view_name: self.view_name.clone(),
                cypher: self.cypher.clone(),
                columns: Arc::clone(&self.columns),
                limit,
            },
            self.schema(),
            projection.cloned(),
        )?))
    }
}

/// The view scan's lazy stream: the degraded recovery runs HERE, on
/// first poll — never during plan construction. A degraded source
/// re-validates ALL of its view contracts and flips Healthy before the
/// view's own Cypher runs; a healthy source pays nothing (the health
/// check is a lock read). Everything past the recovery is the shared
/// cypher_batches machinery.
pub(crate) fn view_batches(
    handle: Arc<GraphSourceHandle>,
    view_name: String,
    cypher: String,
    columns: Arc<Vec<DeclaredColumn>>,
    limit: Option<usize>,
) -> futures::stream::BoxStream<'static, DFResult<RecordBatch>> {
    stream::once(async move {
        ensure_healthy(&handle, &view_name).await?;
        Ok::<_, DataFusionError>(super::udtf::cypher_batches(
            handle,
            cypher,
            // Views are fixed Cypher with no parameter surface.
            Value::Object(serde_json::Map::new()),
            columns,
            limit,
        ))
    })
    .map_ok(|inner| inner)
    .try_flatten()
    .boxed()
}

/// The degraded retry: a Degraded source re-validates ALL of its views
/// against the backend and flips Healthy on success; a failed retry is
/// the loud error the design asks for ("the first scan retries the
/// validation and fails loudly"). Called from the lazy scan stream —
/// the lock is dropped BEFORE any await, never held across it.
pub(crate) async fn ensure_healthy(
    handle: &Arc<GraphSourceHandle>,
    view_name: &str,
) -> DFResult<()> {
    // Fast path: a healthy source pays one lock-free-ish read, never the
    // gate.
    if degraded_reason(handle).is_none() {
        return Ok(());
    }
    // SINGLE-FLIGHT: without the gate, N concurrent first scans each
    // re-validate every view — requests × views backend probes, with the
    // losers queueing on the saturated pool until acquire_timeout turns
    // wait into spurious failures. One arrival pays; the rest wake up to
    // a Healthy re-check and proceed.
    let _gate = handle.recovery_gate.lock().await;
    let Some(registration_error) = degraded_reason(handle) else {
        return Ok(()); // the winner already recovered the source
    };
    revalidate_all_views(handle).await.map_err(|e| {
        DataFusionError::Execution(format!(
            "graph source of view '{view_name}' is registered DEGRADED (registration \
             error: {registration_error}) and its first-scan re-validation \
             failed: {e}"
        ))
    })?;
    *handle.health.write().unwrap_or_else(|p| p.into_inner()) = GraphSourceHealth::Healthy;
    Ok(())
}

/// Prove one view against the live backend: run its Cypher fetching at
/// most ONE row, then convert what came back. AGE arity mismatches are
/// raised by the backend inside `execute`; type mismatches and
/// `nullable: false` violations are caught by the conversion. Errors are
/// wrapped to name the view — the identity an operator needs.
pub(crate) async fn validate_view(
    handle: &GraphSourceHandle,
    view_name: &str,
    cypher: &str,
    columns: &[DeclaredColumn],
) -> Result<(), GraphError> {
    let fail = |e: GraphError| GraphError::ViewValidationFailed {
        view: view_name.to_string(),
        source: Box::new(e),
    };
    let rows = handle
        .client
        .execute(
            cypher,
            &Value::Object(serde_json::Map::new()),
            columns.len(),
            handle.bounds,
            Some(1),
        )
        .await
        .map_err(&fail)?
        .try_collect::<Vec<_>>()
        .await
        .map_err(&fail)?;
    build_batch(columns, &rows, 0).map_err(&fail)?;
    Ok(())
}

/// Re-prove every view contract of a source against the (recovered)
/// backend. This is what keeps the degraded-recovery path at the same
/// contract strength as reachable registration: there, ANY view failing
/// validation refuses the source; here, any view failing re-validation
/// keeps the source degraded and names the failing view. A source with
/// no views has nothing to re-prove — the caller's own successful query
/// is the recovery evidence (the UDTF path in udtf.rs relies on that).
pub(crate) async fn revalidate_all_views(
    handle: &Arc<GraphSourceHandle>,
) -> Result<(), GraphError> {
    let n = handle.view_contracts.len();
    if n == 0 {
        return Ok(());
    }
    // Same bounded launch as reachable registration (see
    // validate_views_concurrently): at most the pool's worth in flight,
    // so no probe queues behind its siblings. The whole run gets a
    // computed BACKSTOP deadline — waves × (per-probe budget + the
    // client wrap's slack) — which per-probe bounds make all but
    // unreachable; it exists so a pathological stall can never wedge
    // the recovery gate forever.
    let limit = handle.validation_limit.max(1);
    let waves = n.div_ceil(limit) as u32;
    let deadline = handle
        .bounds
        .timeout
        .saturating_add(std::time::Duration::from_secs(5))
        .saturating_mul(waves);
    let contracts = handle.view_contracts.iter().cloned().collect();
    tokio::time::timeout(
        deadline,
        validate_views_concurrently(handle, contracts, limit),
    )
    .await
    .map_err(|_| GraphError::RecoveryDeadlineExceeded {
        seconds: deadline.as_secs(),
    })?
}

/// Validate every contract with AT MOST `limit` validations in flight —
/// `limit` is the pool's own `max_connections`, so no validation ever
/// waits in the pool's acquire queue behind its siblings. An unbounded
/// `try_join_all` here parks the excess in that queue, where
/// `acquire_timeout` (wired to `query_timeout_seconds`) keeps ticking:
/// with more views than connections and validations that take a
/// meaningful fraction of the timeout, queued waves overrun the deadline
/// and a HEALTHY backend with a CORRECT contract is refused registration
/// as `ConnectionAcquireTimeout` — an availability artifact of the
/// launch shape, not a contract violation. The first failure aborts the
/// remaining validations (they are independent probes; one refusal is
/// enough to refuse registration).
pub(crate) async fn validate_views_concurrently(
    handle: &Arc<GraphSourceHandle>,
    contracts: Vec<ViewContract>,
    limit: usize,
) -> Result<(), GraphError> {
    stream::iter(contracts.into_iter().map(Ok))
        .try_for_each_concurrent(limit.max(1), |contract| {
            let handle = Arc::clone(handle);
            async move {
                validate_view(&handle, &contract.name, &contract.cypher, &contract.columns).await
            }
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::RwLock;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use futures::stream::{self, BoxStream};
    use futures::{StreamExt, TryStreamExt};

    use super::super::client::{GraphClient, QueryBounds};
    use super::super::value::GraphType;

    /// Canned-rows client with a call counter — the validation/scan
    /// distinction is the count (a validating scan executes twice).
    #[derive(Debug)]
    struct CountingMock {
        rows: Vec<Vec<Value>>,
        error: Option<String>,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl GraphClient for CountingMock {
        async fn execute(
            &self,
            _cypher: &str,
            _params: &Value,
            _arity: usize,
            _bounds: QueryBounds,
            limit: Option<usize>,
        ) -> Result<BoxStream<'static, Result<Vec<Value>, GraphError>>, GraphError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if let Some(message) = &self.error {
                return Err(GraphError::backend("kg", "io", message));
            }
            let mut rows = self.rows.clone();
            if let Some(l) = limit {
                rows.truncate(l);
            }
            Ok(stream::iter(rows.into_iter().map(Ok)).boxed())
        }

        async fn labels(
            &self,
            _bounds: QueryBounds,
            _limit: Option<usize>,
        ) -> Result<Vec<(String, String)>, GraphError> {
            Ok(vec![])
        }
    }

    fn handle_with(
        health: GraphSourceHealth,
        rows: Vec<Vec<Value>>,
        error: Option<String>,
    ) -> (Arc<GraphSourceHandle>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let handle = Arc::new(GraphSourceHandle {
            client: Arc::new(CountingMock {
                rows,
                error,
                calls: Arc::clone(&calls),
            }),
            bounds: QueryBounds {
                timeout: Duration::from_secs(5),
                max_rows: 100,
            },
            health: Arc::new(RwLock::new(health)),
            // The provider under test is this source's only view, so its
            // contract is the whole re-validation set.
            view_contracts: Arc::new(vec![ViewContract {
                name: "user_posts".to_string(),
                cypher: "MATCH (u:User) RETURN u.name".to_string(),
                columns: columns(),
            }]),
            recovery_gate: Arc::new(tokio::sync::Mutex::new(())),
            validation_limit: 4,
        });
        (handle, calls)
    }

    fn columns() -> Vec<DeclaredColumn> {
        vec![DeclaredColumn {
            name: "name".to_string(),
            ty: GraphType::String,
            nullable: true,
        }]
    }

    fn provider(handle: Arc<GraphSourceHandle>) -> GraphViewProvider {
        GraphViewProvider::new(
            handle,
            "user_posts".to_string(),
            "MATCH (u:User) RETURN u.name".to_string(),
            columns(),
        )
    }

    fn is_healthy(handle: &GraphSourceHandle) -> bool {
        handle
            .health
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .is_healthy()
    }

    async fn scan_rows(provider: GraphViewProvider) -> DFResult<Vec<RecordBatch>> {
        let ctx = SessionContext::new();
        ctx.register_table("user_posts", Arc::new(provider))?;
        ctx.sql("SELECT name FROM user_posts")
            .await?
            .collect()
            .await
    }

    /// Fails only when the Cypher carries the marker — the two-view
    /// re-validation test's way to break one contract and not the other.
    #[derive(Debug)]
    struct PickyMock {
        fail_marker: &'static str,
    }

    #[async_trait]
    impl GraphClient for PickyMock {
        async fn execute(
            &self,
            cypher: &str,
            _params: &Value,
            _arity: usize,
            _bounds: QueryBounds,
            _limit: Option<usize>,
        ) -> Result<BoxStream<'static, Result<Vec<Value>, GraphError>>, GraphError> {
            if cypher.contains(self.fail_marker) {
                return Err(GraphError::backend(
                    "kg",
                    "42804",
                    "return row and column definition list do not match",
                ));
            }
            Ok(stream::iter(vec![vec![serde_json::json!("ada")]].into_iter().map(Ok)).boxed())
        }

        async fn labels(
            &self,
            _bounds: QueryBounds,
            _limit: Option<usize>,
        ) -> Result<Vec<(String, String)>, GraphError> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn physical_planning_never_touches_the_backend() {
        // The regression pin: TableProvider::scan runs during PHYSICAL
        // PLAN construction, so the degraded recovery must live in the
        // lazy stream. Building the physical plan for a DEGRADED
        // source's view makes ZERO backend calls; executing the plan
        // pays the re-validation plus the scan.
        let (handle, calls) = handle_with(
            GraphSourceHealth::Degraded("connection refused".to_string()),
            vec![vec![serde_json::json!("ada")]],
            None,
        );
        let ctx = SessionContext::new();
        ctx.register_table("user_posts", Arc::new(provider(Arc::clone(&handle))))
            .expect("register");
        let df = ctx.sql("SELECT name FROM user_posts").await.expect("plans");
        let plan = df
            .create_physical_plan()
            .await
            .expect("physical planning performs no network I/O");
        assert_eq!(
            calls.load(Ordering::Relaxed),
            0,
            "no backend call before execution"
        );
        // First poll: re-validation (1) + the view scan (1).
        let stream = plan.execute(0, ctx.task_ctx()).expect("execute");
        let batches: Vec<RecordBatch> = stream.try_collect().await.expect("backend answers");
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        assert_eq!(calls.load(Ordering::Relaxed), 2, "validate + scan");
        assert!(is_healthy(&handle), "recovered on execution");
    }

    #[tokio::test]
    async fn a_degraded_backend_failure_surfaces_at_execution_not_planning() {
        // Same rule from the failure side: a still-down backend must not
        // make plan CONSTRUCTION fail — the loud degraded error belongs
        // to the stream.
        let (handle, _calls) = handle_with(
            GraphSourceHealth::Degraded("connection refused".to_string()),
            vec![],
            Some("still refused".to_string()),
        );
        let ctx = SessionContext::new();
        ctx.register_table("user_posts", Arc::new(provider(Arc::clone(&handle))))
            .expect("register");
        let df = ctx.sql("SELECT name FROM user_posts").await.expect("plans");
        let plan = df
            .create_physical_plan()
            .await
            .expect("physical planning succeeds against a down backend");
        let err = plan
            .execute(0, ctx.task_ctx())
            .expect("execute")
            .try_collect::<Vec<RecordBatch>>()
            .await
            .expect_err("the failure arrives at execution");
        assert!(err.to_string().contains("DEGRADED"), "{err}");
    }

    #[tokio::test]
    async fn recovery_revalidates_every_view_not_just_the_scanned_one() {
        // The contract-strength parity pin: the SCANNED view is fine, but
        // a sibling view on the same source violates its contract — the
        // source must stay degraded and the error must name the broken
        // view. Reachable registration refuses the source unless every
        // view validates; recovery holds the same line, or the broken
        // view would silently downgrade to a scan-time conversion error.
        let handle = Arc::new(GraphSourceHandle {
            client: Arc::new(PickyMock {
                fail_marker: "Broken",
            }),
            bounds: QueryBounds {
                timeout: Duration::from_secs(5),
                max_rows: 100,
            },
            health: Arc::new(RwLock::new(GraphSourceHealth::Degraded(
                "connection refused".to_string(),
            ))),
            view_contracts: Arc::new(vec![
                ViewContract {
                    name: "good_view".to_string(),
                    cypher: "MATCH (g:Good) RETURN g.name".to_string(),
                    columns: columns(),
                },
                ViewContract {
                    name: "broken_view".to_string(),
                    cypher: "MATCH (b:Broken) RETURN b.name".to_string(),
                    columns: columns(),
                },
            ]),
            recovery_gate: Arc::new(tokio::sync::Mutex::new(())),
            validation_limit: 4,
        });
        let provider = GraphViewProvider::new(
            handle.clone(),
            "good_view".to_string(),
            "MATCH (g:Good) RETURN g.name".to_string(),
            columns(),
        );
        let err = scan_rows(provider)
            .await
            .expect_err("a broken sibling view blocks the recovery flip");
        let msg = err.to_string();
        assert!(
            msg.contains("broken_view"),
            "the failing view is named: {msg}"
        );
        assert!(msg.contains("DEGRADED"), "{msg}");
        assert!(!is_healthy(&handle), "the source stays degraded");
    }

    #[tokio::test]
    async fn a_degraded_view_validates_on_first_scan_and_flips_healthy() {
        let (handle, calls) = handle_with(
            GraphSourceHealth::Degraded("connection refused".to_string()),
            vec![vec![serde_json::json!("ada")]],
            None,
        );
        let batches = scan_rows(provider(Arc::clone(&handle)))
            .await
            .expect("the backend answers now");
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        assert!(is_healthy(&handle), "a successful retry flips Healthy");
        assert_eq!(
            calls.load(Ordering::Relaxed),
            2,
            "validation execute + scan execute"
        );
        // …and a later scan skips the re-validation entirely.
        let batches = scan_rows(provider(Arc::clone(&handle)))
            .await
            .expect("healthy scans");
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        assert_eq!(calls.load(Ordering::Relaxed), 3, "no second validation");
    }

    #[tokio::test]
    async fn a_degraded_view_fails_loudly_when_the_backend_is_still_gone() {
        let (handle, calls) = handle_with(
            GraphSourceHealth::Degraded("connection refused".to_string()),
            vec![],
            Some("still refused".to_string()),
        );
        let err = scan_rows(provider(Arc::clone(&handle)))
            .await
            .expect_err("the retry fails loudly");
        let msg = err.to_string();
        assert!(msg.contains("user_posts"), "the view is named: {msg}");
        assert!(msg.contains("DEGRADED"), "{msg}");
        assert!(msg.contains("connection refused"), "{msg}");
        assert!(!is_healthy(&handle), "a failed retry stays Degraded");
        assert_eq!(calls.load(Ordering::Relaxed), 1, "validation only");
    }

    #[tokio::test]
    async fn a_healthy_view_scans_without_revalidating() {
        let (handle, calls) = handle_with(
            GraphSourceHealth::Healthy,
            vec![vec![serde_json::json!("ada")]],
            None,
        );
        let batches = scan_rows(provider(Arc::clone(&handle)))
            .await
            .expect("healthy scans");
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "no validation round-trip for a healthy source"
        );
    }

    #[tokio::test]
    async fn validation_catches_type_and_not_null_violations() {
        // A declared type the row violates → the wrapped mismatch names
        // the view.
        let (handle, _) = handle_with(
            GraphSourceHealth::Healthy,
            vec![vec![serde_json::json!(7)]],
            None,
        );
        let err = validate_view(&handle, "user_posts", "MATCH (n) RETURN n", &columns())
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("user_posts"), "{msg}");
        assert!(msg.contains("declared 'string'"), "{msg}");

        // A null under a nullable: false declaration → NotNullViolation.
        let (handle, _) = handle_with(GraphSourceHealth::Healthy, vec![vec![Value::Null]], None);
        let strict = vec![DeclaredColumn {
            name: "name".to_string(),
            ty: GraphType::String,
            nullable: false,
        }];
        let err = validate_view(&handle, "user_posts", "MATCH (n) RETURN n", &strict)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("nullable: false"), "{err}");

        // An empty result is a valid view (nothing to convert).
        let (handle, _) = handle_with(GraphSourceHealth::Healthy, vec![], None);
        validate_view(&handle, "user_posts", "MATCH (n) RETURN n", &columns())
            .await
            .expect("empty is valid");
    }

    #[tokio::test]
    async fn concurrent_first_scans_share_one_recovery_flight() {
        // Without the gate, N concurrent first scans each re-validate
        // every view: requests × views probes. With it, exactly ONE
        // validation flight runs; the losers wake to a Healthy re-check.
        // Expected calls: 1 validation (the single contract) + 4 scans.
        let (handle, calls) = handle_with(
            GraphSourceHealth::Degraded("connection refused".to_string()),
            vec![vec![serde_json::json!("ada")]],
            None,
        );
        let scans = (0..4).map(|_| {
            let handle = Arc::clone(&handle);
            async move { scan_rows(provider(Arc::clone(&handle))).await }
        });
        let results = futures::future::join_all(scans).await;
        for result in results {
            result.expect("every concurrent scan succeeds");
        }
        assert!(is_healthy(&handle), "the winner flipped the source");
        assert_eq!(
            calls.load(Ordering::Relaxed),
            5,
            "one shared validation flight + four scans — never 4 + 4"
        );
    }

    /// A slow, in-flight-counting client: pins that view validation
    /// launches AT MOST `limit` probes concurrently. Unbounded launch
    /// parks the excess in the pool's acquire queue, where a healthy
    /// backend's queue wait converts into a spurious
    /// ConnectionAcquireTimeout refusal (the P2 this test guards).
    #[derive(Debug)]
    struct GaugeMock {
        current: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl GraphClient for GaugeMock {
        async fn execute(
            &self,
            _cypher: &str,
            _params: &Value,
            _arity: usize,
            _bounds: QueryBounds,
            _limit: Option<usize>,
        ) -> Result<BoxStream<'static, Result<Vec<Value>, GraphError>>, GraphError> {
            let now = self.current.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            self.calls.fetch_add(1, Ordering::SeqCst);
            // Long enough that an unbounded launch would overlap all
            // probes and drive the peak to the view count.
            tokio::time::sleep(Duration::from_millis(25)).await;
            self.current.fetch_sub(1, Ordering::SeqCst);
            Ok(stream::iter(vec![Ok(vec![serde_json::json!("x")])]).boxed())
        }

        async fn labels(
            &self,
            _bounds: QueryBounds,
            _limit: Option<usize>,
        ) -> Result<Vec<(String, String)>, GraphError> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn registration_validation_concurrency_is_bounded_by_the_limit() {
        let current = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let handle = Arc::new(GraphSourceHandle {
            client: Arc::new(GaugeMock {
                current: Arc::clone(&current),
                peak: Arc::clone(&peak),
                calls: Arc::clone(&calls),
            }),
            bounds: QueryBounds {
                timeout: Duration::from_secs(5),
                max_rows: 100,
            },
            health: Arc::new(RwLock::new(GraphSourceHealth::Healthy)),
            view_contracts: Arc::new(vec![]),
            recovery_gate: Arc::new(tokio::sync::Mutex::new(())),
            validation_limit: 4,
        });
        let contracts: Vec<ViewContract> = (0..8)
            .map(|i| ViewContract {
                name: format!("v{i}"),
                cypher: "MATCH (n) RETURN n.x".to_string(),
                columns: columns(),
            })
            .collect();
        validate_views_concurrently(&handle, contracts, 2)
            .await
            .expect("all views validate");
        assert_eq!(calls.load(Ordering::SeqCst), 8, "every view was proven");
        assert!(
            peak.load(Ordering::SeqCst) <= 2,
            "at most `limit` probes in flight, got {}",
            peak.load(Ordering::SeqCst)
        );
    }

    /// A client that never answers — the backstop-deadline test's wedge.
    #[derive(Debug)]
    struct WedgedClient;

    #[async_trait]
    impl GraphClient for WedgedClient {
        async fn execute(
            &self,
            _cypher: &str,
            _params: &Value,
            _arity: usize,
            _bounds: QueryBounds,
            _limit: Option<usize>,
        ) -> Result<BoxStream<'static, Result<Vec<Value>, GraphError>>, GraphError> {
            std::future::pending().await
        }

        async fn labels(
            &self,
            _bounds: QueryBounds,
            _limit: Option<usize>,
        ) -> Result<Vec<(String, String)>, GraphError> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn diagnostics_carry_the_view_identity_never_the_cypher() {
        // The module-wide redaction rule: Cypher text is caller/config
        // data and never reaches Debug output (it can embed literals the
        // operator considers sensitive).
        let (handle, _) = handle_with(GraphSourceHealth::Healthy, vec![], None);
        let provider = provider(handle);
        assert_eq!(provider.table_type(), TableType::Base);
        let dbg = format!("{provider:?}");
        assert!(dbg.contains("user_posts"), "{dbg}");
        assert!(
            !dbg.contains("MATCH"),
            "the Cypher text never appears in diagnostics: {dbg}"
        );
    }

    #[tokio::test]
    async fn a_gate_loser_wakes_to_a_healthy_source_and_skips_revalidation() {
        // The single-flight loser path, deterministically: the "winner"
        // (this test) holds the gate, the loser passes the fast path
        // while the source is still degraded and parks on the gate; the
        // winner flips Healthy and releases — the loser's under-gate
        // re-check must return without a single backend probe.
        let (handle, calls) = handle_with(
            GraphSourceHealth::Degraded("down at startup".to_string()),
            vec![],
            None,
        );
        let gate = handle.recovery_gate.lock().await;
        let loser = tokio::spawn({
            let handle = Arc::clone(&handle);
            async move { ensure_healthy(&handle, "user_posts").await }
        });
        // Let the loser reach the gate before the flip (if it hasn't, it
        // takes the fast path instead — same observable outcome).
        tokio::time::sleep(Duration::from_millis(20)).await;
        *handle.health.write().unwrap_or_else(|p| p.into_inner()) = GraphSourceHealth::Healthy;
        drop(gate);
        loser
            .await
            .expect("no panic")
            .expect("the loser proceeds without error");
        assert_eq!(
            calls.load(Ordering::Relaxed),
            0,
            "the loser re-proved nothing"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_wedged_revalidation_hits_the_backstop_deadline() {
        // The backstop exists for the pathological case the per-probe
        // bounds cannot reach: a client future that never resolves. One
        // contract, timeout 5s → deadline 1 wave × (5s + 5s) = 10s; the
        // paused clock makes the wait instant.
        let handle = Arc::new(GraphSourceHandle {
            client: Arc::new(WedgedClient),
            bounds: QueryBounds {
                timeout: Duration::from_secs(5),
                max_rows: 100,
            },
            health: Arc::new(RwLock::new(GraphSourceHealth::Degraded(
                "down at startup".to_string(),
            ))),
            view_contracts: Arc::new(vec![ViewContract {
                name: "user_posts".to_string(),
                cypher: "MATCH (u:User) RETURN u.name".to_string(),
                columns: columns(),
            }]),
            recovery_gate: Arc::new(tokio::sync::Mutex::new(())),
            validation_limit: 4,
        });
        let err = revalidate_all_views(&handle)
            .await
            .expect_err("the wedge cannot pass validation");
        assert!(
            matches!(err, GraphError::RecoveryDeadlineExceeded { seconds: 10 }),
            "{err}"
        );
    }
}
