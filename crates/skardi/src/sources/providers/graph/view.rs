//! YAML-declared catalog views (design §Schema handling, milestone 4):
//! each `views:` entry becomes the catalog table `<source>.main.<name>` —
//! a fixed Cypher query plus a declared schema, with the backend first
//! touched at scan time, never at planning.
//!
//! The degraded state machine lives here: a source registered while its
//! backend was unreachable carries [`GraphSourceHealth::Degraded`], and
//! the FIRST scan of any of its views retries the live validation —
//! success flips the source Healthy and the scan proceeds; failure is
//! loud, naming the view and the underlying cause.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use futures::TryStreamExt;
use serde_json::Value;

use super::error::GraphError;
use super::udtf::{GraphScanExec, GraphScanKind, GraphSourceHandle, GraphSourceHealth};
use super::value::{DeclaredColumn, build_batch, declared_schema};

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

    /// The degraded retry: a Degraded source re-validates this view
    /// against the backend on first scan and flips Healthy on success; a
    /// failed retry is the loud error the design asks for ("the first
    /// scan retries the validation and fails loudly"). The lock is
    /// dropped BEFORE the await — never held across it.
    async fn ensure_healthy(&self) -> DFResult<()> {
        let registration_error = {
            let health = self.handle.health.read().unwrap_or_else(|p| p.into_inner());
            match &*health {
                GraphSourceHealth::Healthy => None,
                GraphSourceHealth::Degraded(reason) => Some(reason.clone()),
            }
        };
        let Some(registration_error) = registration_error else {
            return Ok(());
        };
        validate_view(&self.handle, &self.view_name, &self.cypher, &self.columns)
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "graph view '{}' is registered DEGRADED (registration error: \
                     {registration_error}) and its first-scan re-validation failed: {e}",
                    self.view_name
                ))
            })?;
        // Two racing scans may both validate and both flip — benign: the
        // transition is idempotent and the validation is read-only.
        *self
            .handle
            .health
            .write()
            .unwrap_or_else(|p| p.into_inner()) = GraphSourceHealth::Healthy;
        Ok(())
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
        self.ensure_healthy().await?;
        Ok(Arc::new(GraphScanExec::new(
            GraphScanKind::Cypher {
                handle: Arc::clone(&self.handle),
                cypher: self.cypher.clone(),
                // Views are fixed Cypher with no parameter surface.
                params: Value::Object(serde_json::Map::new()),
                columns: Arc::clone(&self.columns),
                limit,
            },
            self.schema(),
            projection.cloned(),
        )?))
    }
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
    let fail = |e: GraphError| GraphError::InvalidConfig {
        name: view_name.to_string(),
        reason: format!("view validation failed: {e}"),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::RwLock;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;
    use futures::stream::{self, BoxStream};

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
}
