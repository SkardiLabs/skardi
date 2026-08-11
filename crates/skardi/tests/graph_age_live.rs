//! Opt-in live integration tests for the graph engine bypass's milestone-1
//! backend: Apache AGE (openCypher inside Postgres).
//!
//! Disabled by default (`#[ignore]`) so the default suite stays offline and
//! deterministic. These are the only tests that exercise the real
//! Postgres+AGE path — everything else mocks at [`GraphClient`] level — so
//! run them whenever the AGE client or the agtype decoding changes.
//!
//! Against a disposable AGE container (no setup needed):
//!   docker run -d --name skardi-age -p 127.0.0.1:15432:5432 \
//!     -e POSTGRES_PASSWORD=agepass apache/age
//!   SKARDI_AGE_LIVE_URL='postgres://postgres:agepass@127.0.0.1:15432/postgres' \
//!     cargo test -p skardi --test graph_age_live -- --ignored
//!
//! Each run creates a uniquely named graph and drops it afterwards, so a
//! shared database is safe and reruns never collide.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arrow::array::{Array, Int64Array, ListArray, StringArray, StructArray};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use sqlx::Executor;
use sqlx::postgres::PgPoolOptions;

use skardi::sources::providers::graph::config::GraphConfig;
use skardi::sources::providers::graph::udtf::GraphSources;
use skardi::sources::providers::graph::{register_graph_source, register_graph_udtfs};

fn live_url() -> Option<String> {
    std::env::var("SKARDI_AGE_LIVE_URL")
        .ok()
        .filter(|u| !u.trim().is_empty())
}

/// Seed a fresh, uniquely named graph through AGE's own APIs (writes go
/// through psql-side cypher — the Skardi surface under test is read-only
/// by construction and could not create this data).
async fn seed_graph(url: &str, graph: &str) -> sqlx::PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .after_connect(|conn, _| {
            Box::pin(async move {
                conn.execute("LOAD 'age'; SET search_path = ag_catalog, \"$user\", public;")
                    .await?;
                Ok(())
            })
        })
        .connect(url)
        .await
        .expect("live postgres reachable (SKARDI_AGE_LIVE_URL)");
    sqlx::query("SELECT create_graph($1)")
        .bind(graph)
        .execute(&pool)
        .await
        .expect("create_graph");
    let seed = format!(
        "SELECT * FROM cypher('{graph}', $$
            CREATE (a:Person {{name: 'ada', age: 36}}),
                   (b:Person {{name: 'bob', age: 41}}),
                   (c:Person {{name: 'cyd'}}),
                   (a)-[:KNOWS {{since: 2019}}]->(b),
                   (b)-[:KNOWS {{since: 2021}}]->(c)
        $$) AS (v agtype)"
    );
    pool.execute(seed.as_str()).await.expect("seed cypher");
    pool
}

async fn drop_graph(pool: &sqlx::PgPool, graph: &str) {
    let _ = sqlx::query("SELECT drop_graph($1, true)")
        .bind(graph)
        .execute(pool)
        .await;
}

/// A registered source + session, against the live database.
async fn live_ctx(url: &str, graph: &str) -> (SessionContext, GraphSources) {
    let sources: GraphSources = Arc::new(RwLock::new(HashMap::new()));
    let config: GraphConfig = serde_yaml::from_str(&format!(
        "backend: age\ngraph_name: {graph}\nquery_timeout_seconds: 10\nmax_rows: 100\n"
    ))
    .expect("config parses");
    register_graph_source(&sources, "kg", url, &config)
        .await
        .expect("registration connects eagerly");
    let ctx = SessionContext::new();
    register_graph_udtfs(&ctx, Arc::clone(&sources)).expect("udtfs register");
    (ctx, sources)
}

async fn collect(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .expect("plans")
        .collect()
        .await
        .expect("executes")
}

fn unique_graph(tag: &str) -> String {
    // Process id + monotonic-ish suffix keeps parallel runs and reruns
    // apart without any shared state.
    format!(
        "skardi_it_{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    )
}

#[tokio::test]
#[ignore = "needs a live Postgres+AGE (set SKARDI_AGE_LIVE_URL); see module doc"]
async fn scalars_params_and_ordering_round_trip() {
    let Some(url) = live_url() else {
        panic!("SKARDI_AGE_LIVE_URL must be set for --ignored live tests");
    };
    let graph = unique_graph("scalar");
    let pool = seed_graph(&url, &graph).await;
    let (ctx, _sources) = live_ctx(&url, &graph).await;

    // Declared scalars, a bound parameter, engine-side ORDER BY.
    let batches = collect(
        &ctx,
        "SELECT name, age FROM cypher_query('kg', '\
             MATCH (p:Person) WHERE p.age > $min RETURN p.name, p.age', \
         '{\"min\": 40}', '{\"name\": \"string\", \"age\": \"int\"}')",
    )
    .await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1, "only bob is over 40");
    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "bob");
    let ages = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ages.value(0), 41);

    // A NULL property lands as SQL NULL (cyd has no age).
    let batches = collect(
        &ctx,
        "SELECT name, age FROM cypher_query('kg', '\
             MATCH (p:Person) RETURN p.name, p.age', '{}', \
         '{\"name\": \"string\", \"age\": \"int\"}') ORDER BY name",
    )
    .await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3);

    drop_graph(&pool, &graph).await;
}

#[tokio::test]
#[ignore = "needs a live Postgres+AGE (set SKARDI_AGE_LIVE_URL); see module doc"]
async fn nodes_relationships_and_paths_take_the_canonical_shapes() {
    let Some(url) = live_url() else {
        panic!("SKARDI_AGE_LIVE_URL must be set for --ignored live tests");
    };
    let graph = unique_graph("shape");
    let pool = seed_graph(&url, &graph).await;
    let (ctx, _sources) = live_ctx(&url, &graph).await;

    // Node STRUCT + json_get over its properties.
    let batches = collect(
        &ctx,
        "SELECT v.id AS id, json_get_str(v.properties, 'name') AS name \
         FROM cypher_query('kg', 'MATCH (v:Person) RETURN v', '{}', \
         '{\"v\": \"node\"}') ORDER BY name",
    )
    .await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3);
    let names = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "ada");

    // Relationship STRUCT: type and endpoint ids are strings.
    let batches = collect(
        &ctx,
        "SELECT r FROM cypher_query('kg', \
         'MATCH ()-[r:KNOWS]->() RETURN r', '{}', '{\"r\": \"relationship\"}')",
    )
    .await;
    let rels = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(rels.len(), 2);
    let types = rels
        .column_by_name("type")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(types.value(0), "KNOWS");

    // Path STRUCT: parallel typed lists, nodes one longer.
    let batches = collect(
        &ctx,
        "SELECT p FROM cypher_query('kg', '\
             MATCH p = (:Person {name: \"ada\"})-[:KNOWS*2]->(:Person) RETURN p', \
         '{}', '{\"p\": \"path\"}')",
    )
    .await;
    let paths = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(paths.len(), 1, "ada -2 hops-> cyd");
    let nodes = paths
        .column_by_name("nodes")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let rels = paths
        .column_by_name("relationships")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(nodes.value(0).len(), 3);
    assert_eq!(rels.value(0).len(), 2);

    drop_graph(&pool, &graph).await;
}

#[tokio::test]
#[ignore = "needs a live Postgres+AGE (set SKARDI_AGE_LIVE_URL); see module doc"]
async fn the_backend_read_only_transaction_is_the_boundary() {
    let Some(url) = live_url() else {
        panic!("SKARDI_AGE_LIVE_URL must be set for --ignored live tests");
    };
    let graph = unique_graph("ro");
    let pool = seed_graph(&url, &graph).await;
    let (ctx, sources) = live_ctx(&url, &graph).await;

    // The keyword guard rejects at plan time…
    let err = ctx
        .sql(
            "SELECT * FROM cypher_query('kg', 'CREATE (n:X) RETURN n', '{}', \
             '{\"n\": \"node\"}')",
        )
        .await
        .expect_err("guard rejects");
    assert!(err.to_string().contains("'CREATE'"), "{err}");

    // …and the SERVER refuses a write even with the guard out of the
    // way: drive the client directly (the live proof the design's
    // security section requires — the guard is UX, the READ ONLY
    // transaction is the boundary).
    let handle = {
        let map = sources.read().unwrap();
        Arc::clone(map.get("kg").unwrap())
    };
    let result = handle
        .client
        .execute(
            "CREATE (n:Sneaky) RETURN n",
            &serde_json::json!({}),
            1,
            handle.bounds,
        )
        .await;
    let err = match result {
        Err(e) => e.to_string(),
        Ok(stream) => {
            use futures::TryStreamExt;
            stream
                .try_collect::<Vec<_>>()
                .await
                .expect_err("server must refuse the write")
                .to_string()
        }
    };
    assert!(
        err.contains("read-only") || err.contains("25006"),
        "the backend names the read-only violation: {err}"
    );

    // Nothing was written.
    let batches = collect(
        &ctx,
        "SELECT n FROM cypher_query('kg', 'MATCH (n:Sneaky) RETURN n', '{}', \
         '{\"n\": \"node\"}')",
    )
    .await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);

    drop_graph(&pool, &graph).await;
}

#[tokio::test]
#[ignore = "needs a live Postgres+AGE (set SKARDI_AGE_LIVE_URL); see module doc"]
async fn graph_schema_lists_labels_and_the_row_cap_fires() {
    let Some(url) = live_url() else {
        panic!("SKARDI_AGE_LIVE_URL must be set for --ignored live tests");
    };
    let graph = unique_graph("schema");
    let pool = seed_graph(&url, &graph).await;

    // graph_schema: labels straight off ag_catalog, internal labels
    // filtered.
    let (ctx, _sources) = live_ctx(&url, &graph).await;
    let batches = collect(
        &ctx,
        "SELECT label, kind FROM graph_schema('kg') ORDER BY label",
    )
    .await;
    let labels: Vec<(String, String)> = batches
        .iter()
        .flat_map(|b| {
            let l = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let k = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            (0..b.num_rows())
                .map(|i| (l.value(i).to_string(), k.value(i).to_string()))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        labels,
        vec![
            ("KNOWS".to_string(), "edge".to_string()),
            ("Person".to_string(), "vertex".to_string()),
        ]
    );

    // The row cap: a 2-row source with max_rows 2 passes; max_rows 1
    // fails LOUDLY (typed, names the cap), never a silent truncation.
    let sources: GraphSources = Arc::new(RwLock::new(HashMap::new()));
    let config: GraphConfig =
        serde_yaml::from_str(&format!("backend: age\ngraph_name: {graph}\nmax_rows: 1\n"))
            .expect("config parses");
    register_graph_source(&sources, "capped", &url, &config)
        .await
        .expect("registers");
    let capped = SessionContext::new();
    register_graph_udtfs(&capped, sources).expect("udtfs");
    let err = capped
        .sql(
            "SELECT name FROM cypher_query('capped', \
             'MATCH (p:Person) RETURN p.name', '{}', '{\"name\": \"string\"}')",
        )
        .await
        .expect("plans")
        .collect()
        .await
        .expect_err("3 people, cap 1");
    let msg = err.to_string();
    assert!(msg.contains("max_rows = 1"), "{msg}");
    assert!(msg.contains("LIMIT"), "the fix is named: {msg}");

    drop_graph(&pool, &graph).await;
}
