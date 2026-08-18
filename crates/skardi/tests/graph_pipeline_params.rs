//! Pins the milestone-4 settlement of the design's Risks #0: a pipeline
//! `{params}` placeholder occupying the WHOLE third argument of
//! `cypher_query` is the supported way for request parameters to reach
//! Cypher parameters.
//!
//! The two substitution passes disagree inside nested literals
//! (inference writes a bare `NULL`; execution writes a quoted `'value'`),
//! so the placeholder must not sit inside the params JSON string. This
//! test proves the endorsed spelling end to end, with no backend: a
//! recording [`GraphClient`] stands in for AGE and captures the params
//! JSON the driver would have received.
//!
//! 1. LOAD: pipeline inference replaces `{params}` with `NULL`, which
//!    the UDTF accepts as "no parameters" — the pipeline loads and
//!    `params` lands in the request schema.
//! 2. EXECUTE: the server's execution pass substitutes a quoted string
//!    literal in the whole argument position; the params JSON parses and
//!    reaches the client unchanged.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use serde_json::Value;

use skardi::pipeline::pipeline::{Pipeline, StandardPipeline};
use skardi::sources::providers::graph::client::{GraphClient, GraphRow, QueryBounds};
use skardi::sources::providers::graph::error::GraphError;
use skardi::sources::providers::graph::register_graph_udtfs;
use skardi::sources::providers::graph::udtf::{GraphSourceHandle, GraphSourceHealth, GraphSources};

/// A client that records the params of every `execute` call and answers
/// with one canned row matching the pipeline's declared columns.
#[derive(Debug, Default)]
struct RecordingClient {
    seen_params: Mutex<Vec<Value>>,
}

#[async_trait]
impl GraphClient for RecordingClient {
    async fn execute(
        &self,
        _cypher: &str,
        params: &Value,
        _arity: usize,
        _bounds: QueryBounds,
        _limit: Option<usize>,
    ) -> Result<BoxStream<'static, Result<GraphRow, GraphError>>, GraphError> {
        self.seen_params
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(params.clone());
        Ok(stream::iter(vec![Ok(vec![
            serde_json::json!("ada"),
            serde_json::json!(36),
        ])])
        .boxed())
    }

    async fn labels(
        &self,
        _bounds: QueryBounds,
        _limit: Option<usize>,
    ) -> Result<Vec<(String, String)>, GraphError> {
        Ok(vec![])
    }
}

const PIPELINE_YAML: &str = r#"
kind: pipeline
metadata:
  name: people-over-age
  version: 1.0.0
spec:
  query: |
    SELECT name, age
    FROM cypher_query(
      'kg',
      'MATCH (p:Person) WHERE p.age > $min RETURN p.name AS name, p.age AS age',
      {params},
      '{"name": "string", "age": "int"}'
    )
    ORDER BY age DESC
"#;

#[tokio::test]
async fn pipeline_params_placeholder_reaches_cypher_params() {
    let client = Arc::new(RecordingClient::default());
    let sources: GraphSources = Arc::new(RwLock::new(HashMap::from([(
        "kg".to_string(),
        Arc::new(GraphSourceHandle::new(
            client.clone() as Arc<dyn GraphClient>,
            QueryBounds {
                timeout: std::time::Duration::from_secs(5),
                max_rows: 100,
            },
            GraphSourceHealth::Healthy,
            Arc::new(vec![]),
            4,
        )),
    )])));
    let ctx = SessionContext::new();
    register_graph_udtfs(&ctx, sources).expect("udtfs register");

    // LOAD: inference plans the SQL with `{params}` replaced by NULL —
    // the whole-argument placeholder must not fail pipeline loading, and
    // the parameter must surface in the request schema.
    let dir = tempfile::TempDir::new().expect("temp dir");
    let path = dir.path().join("pipeline.yaml");
    std::fs::write(&path, PIPELINE_YAML).expect("write pipeline");
    let pipeline = StandardPipeline::load_from_file(&path, Arc::new(ctx.clone()))
        .await
        .expect("pipeline loads: the NULL placeholder plans as no-params");
    assert!(
        pipeline.request_schema().fields.contains_key("params"),
        "params surfaces in the request schema: {:?}",
        pipeline.request_schema().fields.keys()
    );

    // EXECUTE: the server's execution pass renders a String request
    // parameter as a quoted SQL literal (substitute_sql_params); the
    // whole-argument position makes that a valid params JSON string.
    let sql = pipeline
        .query_definition()
        .sql
        .replace("{params}", "'{\"min\": 40}'");
    let batches = ctx
        .sql(&sql)
        .await
        .expect("the substituted SQL re-plans")
        .collect()
        .await
        .expect("the substituted SQL executes");
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

    // …and the params JSON arrived at the client exactly as sent.
    let seen = client.seen_params.lock().unwrap_or_else(|p| p.into_inner());
    assert_eq!(seen.as_slice(), [serde_json::json!({"min": 40})]);
}
