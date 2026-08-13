//! Opt-in live integration tests for the graph engine bypass's
//! milestone-2 backend: Neo4j over Bolt.
//!
//! Disabled by default (`#[ignore]`) so the default suite stays offline
//! and deterministic. These are the only tests that exercise the real
//! Bolt path — everything else mocks at [`GraphClient`] level — so run
//! them whenever the Neo4j client or the Bolt decoding changes.
//!
//! Against a disposable Neo4j container (no setup needed):
//!   docker run -d --name skardi-neo4j -p 127.0.0.1:17687:7687 \
//!     -e NEO4J_AUTH=neo4j/skardipass neo4j:5
//!   SKARDI_NEO4J_LIVE_URL='bolt://neo4j:skardipass@127.0.0.1:17687' \
//!     cargo test -p skardi --test graph_neo4j_live -- --ignored
//!
//! Neo4j Community has ONE user database (multi-db is enterprise), so
//! isolation rides uniquely suffixed LABELS per run, cleaned up
//! afterwards — a shared server is safe and reruns never collide.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arrow::array::{Array, ListArray, StringArray, StructArray};
use datafusion::prelude::SessionContext;

use skardi::sources::providers::graph::client::{GraphClient, QueryBounds};
use skardi::sources::providers::graph::config::GraphConfig;
use skardi::sources::providers::graph::neo4j::Neo4jClient;
use skardi::sources::providers::graph::udtf::GraphSources;
use skardi::sources::providers::graph::value::{DeclaredColumn, GraphType};
use skardi::sources::providers::graph::{register_graph_source, register_graph_udtfs};

mod graph_live_support;
use graph_live_support::{collect, split_creds};

fn live_url() -> Option<String> {
    graph_live_support::live_url("SKARDI_NEO4J_LIVE_URL", "SKARDI_NEO4J_LIVE_REQUIRED")
}

/// One declared column for direct `GraphClient::execute` calls. On this
/// backend the NAME binds (unlike AGE's positional count).
fn cols(names: &[(&str, GraphType)]) -> Vec<DeclaredColumn> {
    names
        .iter()
        .map(|(name, ty)| DeclaredColumn {
            name: name.to_string(),
            ty: *ty,
        })
        .collect()
}

/// A run-unique label suffix: Community Neo4j has one database, so the
/// data namespace is the label alphabet.
fn unique_suffix(tag: &str) -> String {
    format!(
        "{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    )
}

/// Seed through the DRIVER's write path (the Skardi surface under test
/// is read-only by construction and could not create this data).
/// Labels carry the run suffix; returns the seeding Graph for cleanup.
async fn seed(url: &str, suffix: &str) -> neo4rs::Graph {
    let (clean_url, user, pass) = split_creds(url);
    let graph = neo4rs::Graph::new(
        clean_url,
        user.unwrap_or_default(),
        pass.unwrap_or_default(),
    )
    .expect("live neo4j reachable (SKARDI_NEO4J_LIVE_URL)");
    let create = format!(
        "CREATE (a:Person_{suffix}:Admin_{suffix} {{name: 'ada', age: 36}}), \
                (b:Person_{suffix} {{name: 'bob', age: 41}}), \
                (c:Person_{suffix} {{name: 'cyd'}}), \
                (d:Person_{suffix} {{name: '颱風', note: 'café ☔', \
                 joined: date('2024-05-17')}}), \
                (a)-[:KNOWS_{suffix} {{since: 2019}}]->(b), \
                (b)-[:KNOWS_{suffix} {{since: 2021}}]->(c), \
                (a)-[:KNOWS_{suffix} {{since: 2024}}]->(d)"
    );
    graph.run(neo4rs::Query::new(create)).await.expect("seed");
    graph
}

async fn cleanup(graph: &neo4rs::Graph, suffix: &str) {
    let _ = graph
        .run(neo4rs::Query::new(format!(
            "MATCH (n) WHERE any(l IN labels(n) WHERE l ENDS WITH '{suffix}') \
             DETACH DELETE n"
        )))
        .await;
}

/// A registered `backend: neo4j` source + session against the live
/// server. Credentials ride env vars (unique per suffix, so parallel
/// tests never race). Registration itself runs the read-mode proof —
/// every `live_ctx` success is one more pass of that gate.
async fn live_ctx(url: &str, suffix: &str) -> (SessionContext, GraphSources) {
    let sources: GraphSources = Arc::new(RwLock::new(HashMap::new()));
    let (clean_url, user, pass) = split_creds(url);
    let mut cred_lines = String::new();
    if let Some(u) = &user {
        let env = format!("SKARDI_NEO4J_LIVE_USER_{}", suffix.to_uppercase());
        unsafe { std::env::set_var(&env, u) };
        cred_lines.push_str(&format!("username_env: {env}\n"));
    }
    if let Some(p) = &pass {
        let env = format!("SKARDI_NEO4J_LIVE_PASS_{}", suffix.to_uppercase());
        unsafe { std::env::set_var(&env, p) };
        cred_lines.push_str(&format!("password_env: {env}\n"));
    }
    let config: GraphConfig = serde_yaml::from_str(&format!(
        "backend: neo4j\nquery_timeout_seconds: 10\nmax_rows: 100\n{cred_lines}"
    ))
    .expect("config parses");
    register_graph_source(&sources, "kg", &clean_url, &config)
        .await
        .expect("registration connects eagerly and passes the read-mode proof");
    let ctx = SessionContext::new();
    register_graph_udtfs(&ctx, Arc::clone(&sources)).expect("udtfs register");
    (ctx, sources)
}

/// A bare client with tailored bounds, for direct-trait tests.
async fn live_client(url: &str, timeout_secs: u64) -> Neo4jClient {
    let (clean_url, user, pass) = split_creds(url);
    unsafe {
        std::env::set_var("SKARDI_NEO4J_DIRECT_USER", user.unwrap_or_default());
        std::env::set_var("SKARDI_NEO4J_DIRECT_PASS", pass.unwrap_or_default());
    }
    Neo4jClient::connect(
        "kg",
        &clean_url,
        None,
        Some("SKARDI_NEO4J_DIRECT_USER"),
        Some("SKARDI_NEO4J_DIRECT_PASS"),
        4,
        std::time::Duration::from_secs(timeout_secs),
    )
    .await
    .expect("connects")
}

fn skip() -> bool {
    if live_url().is_none() {
        // CI's coverage job runs `-- --ignored` across the board (the
        // documents_s3_live convention): absent gating env means SKIP,
        // loudly on stderr — never a panic.
        eprintln!("skipping live Neo4j test: set SKARDI_NEO4J_LIVE_URL to run");
        return true;
    }
    false
}

#[tokio::test]
#[ignore = "needs a live Neo4j (set SKARDI_NEO4J_LIVE_URL); see module doc"]
async fn scalars_params_and_by_name_binding_round_trip() {
    if skip() {
        return;
    }
    let url = live_url().unwrap();
    let suffix = unique_suffix("scalar");
    let graph = seed(&url, &suffix).await;
    let (ctx, _sources) = live_ctx(&url, &suffix).await;

    // Declared columns in the OPPOSITE order of the RETURN clause: on
    // this backend the binding is BY NAME, so — unlike AGE, where this
    // exact shape silently swaps — the values land under their names.
    let batches = collect(
        &ctx,
        &format!(
            "SELECT name, age FROM cypher_query('kg', '\
                 MATCH (p:Person_{suffix}) WHERE p.age > $min \
                 RETURN p.age AS age, p.name AS name', \
             '{{\"min\": 40}}', '{{\"name\": \"string\", \"age\": \"int\"}}')"
        ),
    )
    .await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1, "only bob is over 40");
    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "bob", "name carries the NAME, not the age");

    // CJK and emoji survive Bolt round-trips byte-faithfully.
    let batches = collect(
        &ctx,
        &format!(
            "SELECT note FROM cypher_query('kg', '\
                 MATCH (p:Person_{suffix}) WHERE p.name = $who RETURN p.note AS note', \
             '{{\"who\": \"颱風\"}}', '{{\"note\": \"string\"}}')"
        ),
    )
    .await;
    let notes = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(notes.value(0), "café ☔");

    // A declared name the query never returns is a typed error naming it.
    let err = ctx
        .sql(&format!(
            "SELECT nope FROM cypher_query('kg', '\
                 MATCH (p:Person_{suffix}) RETURN p.name AS name', \
             '{{}}', '{{\"nope\": \"string\"}}')"
        ))
        .await
        .expect("plans")
        .collect()
        .await
        .expect_err("no field named nope");
    let msg = err.to_string();
    assert!(msg.contains("'nope'"), "{msg}");
    assert!(msg.contains("BY NAME"), "{msg}");

    cleanup(&graph, &suffix).await;
}

#[tokio::test]
#[ignore = "needs a live Neo4j (set SKARDI_NEO4J_LIVE_URL); see module doc"]
async fn nodes_relationships_and_paths_take_the_canonical_shapes() {
    if skip() {
        return;
    }
    let url = live_url().unwrap();
    let suffix = unique_suffix("shape");
    let graph = seed(&url, &suffix).await;
    let (ctx, _sources) = live_ctx(&url, &suffix).await;

    // A MULTI-label node (ada is Person+Admin): both labels land in the
    // canonical list — the shape AGE structurally cannot produce.
    let batches = collect(
        &ctx,
        &format!(
            "SELECT v, json_get_str(v.properties, 'name') AS name \
             FROM cypher_query('kg', '\
                 MATCH (v:Admin_{suffix}) RETURN v', \
             '{{}}', '{{\"v\": \"node\"}}')"
        ),
    )
    .await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let node = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let labels = node
        .column_by_name("labels")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(labels.value(0).len(), 2, "Person + Admin, both present");
    let names = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "ada");

    // Relationship: endpoints and type; properties queryable in place.
    let batches = collect(
        &ctx,
        &format!(
            "SELECT r, json_get_int(r.properties, 'since') AS since \
             FROM cypher_query('kg', '\
                 MATCH (:Person_{suffix} {{name: \"ada\"}})-[r:KNOWS_{suffix}]->\
                       (:Person_{suffix} {{name: \"bob\"}}) RETURN r', \
             '{{}}', '{{\"r\": \"relationship\"}}')"
        ),
    )
    .await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let rel = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let rel_type = rel
        .column_by_name("type")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(rel_type.value(0), format!("KNOWS_{suffix}"));
    let start_id = rel
        .column_by_name("start_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(!start_id.value(0).is_empty(), "endpoints resolve");

    // Path: ada → bob → cyd in traversal order, parallel lists, hop i
    // joins node i to node i+1.
    let batches = collect(
        &ctx,
        &format!(
            "SELECT p FROM cypher_query('kg', '\
                 MATCH p = (:Person_{suffix} {{name: \"ada\"}})\
                           -[:KNOWS_{suffix}*2]->\
                           (:Person_{suffix} {{name: \"cyd\"}}) RETURN p', \
             '{{}}', '{{\"p\": \"path\"}}')"
        ),
    )
    .await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let path = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let nodes = path
        .column_by_name("nodes")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let rels = path
        .column_by_name("relationships")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(nodes.value(0).len(), 3, "ada, bob, cyd");
    assert_eq!(rels.value(0).len(), 2, "two hops");

    // Temporal property: date() renders as ISO text inside properties
    // JSON — a datetime property must never fail a scan.
    let batches = collect(
        &ctx,
        &format!(
            "SELECT json_get_str(v.properties, 'joined') AS joined \
             FROM cypher_query('kg', '\
                 MATCH (v:Person_{suffix}) WHERE v.name = \"颱風\" RETURN v', \
             '{{}}', '{{\"v\": \"node\"}}')"
        ),
    )
    .await;
    let joined = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(joined.value(0), "2024-05-17");

    cleanup(&graph, &suffix).await;
}

#[tokio::test]
#[ignore = "needs a live Neo4j (set SKARDI_NEO4J_LIVE_URL); see module doc"]
async fn record_fallback_returns_json_records_with_sorted_keys() {
    if skip() {
        return;
    }
    let url = live_url().unwrap();
    let suffix = unique_suffix("record");
    let graph = seed(&url, &suffix).await;
    let (ctx, _sources) = live_ctx(&url, &suffix).await;

    // No declared columns: one `record` column, whole record as JSON.
    let batches = collect(
        &ctx,
        &format!(
            "SELECT record FROM cypher_query('kg', '\
                 MATCH (p:Person_{suffix}) WHERE p.age IS NOT NULL \
                 RETURN p.name AS name, p.age AS age')"
        ),
    )
    .await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    let records = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let parsed: serde_json::Value = serde_json::from_str(records.value(0)).expect("valid JSON");
    assert!(parsed.get("name").is_some() && parsed.get("age").is_some());
    // Sorted keys — the documented determinism contract.
    assert!(
        records.value(0).find("\"age\"") < records.value(0).find("\"name\""),
        "{}",
        records.value(0)
    );

    // The record is queryable in place through the json getters.
    let batches = collect(
        &ctx,
        &format!(
            "SELECT json_get_str(record, 'name') AS name FROM cypher_query('kg', '\
                 MATCH (p:Person_{suffix}) WHERE p.age IS NOT NULL \
                 RETURN p.name AS name, p.age AS age') \
             ORDER BY name"
        ),
    )
    .await;
    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "ada");

    // Empty results keep the record schema.
    let batches = collect(
        &ctx,
        "SELECT * FROM cypher_query('kg', 'MATCH (p:NoSuchLabelAnywhere) RETURN p')",
    )
    .await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
    assert_eq!(batches[0].schema().field(0).name(), "record");

    cleanup(&graph, &suffix).await;
}

#[tokio::test]
#[ignore = "needs a live Neo4j (set SKARDI_NEO4J_LIVE_URL); see module doc"]
async fn read_only_is_server_enforced_past_the_guard() {
    if skip() {
        return;
    }
    let url = live_url().unwrap();
    let suffix = unique_suffix("readonly");
    let graph = seed(&url, &suffix).await;
    let (ctx, sources) = live_ctx(&url, &suffix).await;

    // Layer 1: the keyword guard rejects at PLAN time, keyword named.
    let err = ctx
        .sql("SELECT * FROM cypher_query('kg', 'CREATE (n:Sneaky) RETURN n', '{}', '{\"n\": \"node\"}')")
        .await
        .expect_err("guard rejects");
    assert!(err.to_string().contains("'CREATE'"), "{err}");

    // Layer 2: bypass the guard entirely (direct client call — the
    // deliberately-disabled-guard proof the design's testing strategy
    // names) and require the SERVER to refuse the write inside the
    // read-mode transaction.
    let handle = {
        let map = sources.read().unwrap();
        Arc::clone(map.get("kg").unwrap())
    };
    let columns = cols(&[("n", GraphType::Node)]);
    let result = handle
        .client
        .execute(
            "CREATE (n:Sneaky) RETURN n",
            &serde_json::json!({}),
            Some(&columns),
            handle.bounds,
            None,
        )
        .await;
    let err = match result {
        Err(e) => e.to_string(),
        Ok(stream) => {
            use futures::TryStreamExt;
            stream
                .try_collect::<Vec<_>>()
                .await
                .expect_err("the backend must refuse")
                .to_string()
        }
    };
    assert!(
        err.contains("graph backend error"),
        "server-side refusal surfaces as a typed backend error: {err}"
    );
    // The refusal is the ACCESS-MODE one
    // (Neo.ClientError.Statement.AccessMode, pinned live), not an
    // incidental failure — the boundary the registration proof relies on.
    assert!(err.contains("read access mode"), "{err}");
    // And the write demonstrably did not happen.
    let batches = collect(
        &ctx,
        "SELECT record FROM cypher_query('kg', 'MATCH (n:Sneaky) RETURN n')",
    )
    .await;
    assert_eq!(
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        0,
        "nothing was created"
    );

    cleanup(&graph, &suffix).await;
}

#[tokio::test]
#[ignore = "needs a live Neo4j (set SKARDI_NEO4J_LIVE_URL); see module doc"]
async fn graph_schema_serves_property_names_and_types() {
    if skip() {
        return;
    }
    let url = live_url().unwrap();
    let suffix = unique_suffix("schema");
    let graph = seed(&url, &suffix).await;
    let (ctx, _sources) = live_ctx(&url, &suffix).await;

    // The milestone-2 discovery upgrade: property names AND types, per
    // label, straight off db.schema.* — the columns AGE serves as null.
    let batches = collect(
        &ctx,
        &format!(
            "SELECT label, kind, property, property_type FROM graph_schema('kg') \
             WHERE label LIKE '%{suffix}' ORDER BY kind, label, property"
        ),
    )
    .await;
    let mut rows: Vec<(String, String, Option<String>, Option<String>)> = Vec::new();
    for batch in &batches {
        let label = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let kind = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let property = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let property_type = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((
                label.value(i).to_string(),
                kind.value(i).to_string(),
                (!property.is_null(i)).then(|| property.value(i).to_string()),
                (!property_type.is_null(i)).then(|| property_type.value(i).to_string()),
            ));
        }
    }
    // The KNOWS edge carries (since, Long).
    assert!(
        rows.iter().any(|(label, kind, property, ty)| {
            label == &format!("KNOWS_{suffix}")
                && kind == "edge"
                && property.as_deref() == Some("since")
                && ty.as_deref().is_some_and(|t| t.contains("Long"))
        }),
        "KNOWS since:Long expected in {rows:?}"
    );
    // Person carries (name, String).
    assert!(
        rows.iter().any(|(label, kind, property, ty)| {
            label == &format!("Person_{suffix}")
                && kind == "vertex"
                && property.as_deref() == Some("name")
                && ty.as_deref().is_some_and(|t| t.contains("String"))
        }),
        "Person name:String expected in {rows:?}"
    );

    cleanup(&graph, &suffix).await;
}

#[tokio::test]
#[ignore = "needs a live Neo4j (set SKARDI_NEO4J_LIVE_URL); see module doc"]
async fn bounds_hold_row_cap_limit_and_typed_timeout() {
    if skip() {
        return;
    }
    let url = live_url().unwrap();
    let suffix = unique_suffix("bounds");
    let graph = seed(&url, &suffix).await;

    let client = live_client(&url, 10).await;
    let tight = QueryBounds {
        timeout: std::time::Duration::from_secs(10),
        max_rows: 1,
    };
    let columns = cols(&[("name", GraphType::String)]);

    // Row cap: 4 people, cap 1 — loud typed overflow.
    let err = client
        .execute(
            &format!("MATCH (p:Person_{suffix}) RETURN p.name AS name"),
            &serde_json::json!({}),
            Some(&columns),
            tight,
            None,
        )
        .await
        .err()
        .expect("cap must fire");
    assert!(err.to_string().contains("max_rows = 1"), "{err}");

    // LIMIT under the cap: clean early stop, exactly 1 row.
    use futures::TryStreamExt;
    let rows: Vec<_> = client
        .execute(
            &format!("MATCH (p:Person_{suffix}) RETURN p.name AS name"),
            &serde_json::json!({}),
            Some(&columns),
            tight,
            Some(1),
        )
        .await
        .expect("limit is a clean stop")
        .try_collect()
        .await
        .expect("collects");
    assert_eq!(rows.len(), 1);

    // graph_schema shares the bounds discipline, AGE-parity: a LIMIT at
    // or under the cap is a CLEAN stop even when the flattened catalog
    // exceeds the cap, and only an uncapped read overflows loudly.
    let schema_bounds = QueryBounds {
        timeout: std::time::Duration::from_secs(10),
        max_rows: 1,
    };
    let rows = client
        .schema(schema_bounds, Some(1))
        .await
        .expect("limit at the cap is a clean stop, never a cap error");
    assert_eq!(rows.len(), 1);
    let err = client
        .schema(schema_bounds, None)
        .await
        .expect_err("the seeded catalog flattens past a cap of 1");
    assert!(err.to_string().contains("max_rows = 1"), "{err}");

    // Server-side tx_timeout: a summation the server cannot finish in 1s
    // dies THERE and surfaces as the typed Timeout naming the bound —
    // this is the live proof the RUN-extra tx_timeout actually lands.
    let err = client
        .execute(
            "UNWIND range(1, 100000000) AS i \
             UNWIND range(1, 100) AS j \
             RETURN sum(i * j) AS s",
            &serde_json::json!({}),
            Some(&cols(&[("s", GraphType::Int)])),
            QueryBounds {
                timeout: std::time::Duration::from_secs(1),
                max_rows: 10,
            },
            None,
        )
        .await
        .err()
        .expect("cannot finish in 1s");
    let msg = err.to_string();
    assert!(msg.contains("timed out after 1s"), "typed, named: {msg}");

    cleanup(&graph, &suffix).await;
}

#[tokio::test]
#[ignore = "needs a live Neo4j (set SKARDI_NEO4J_LIVE_URL); see module doc"]
async fn credentials_never_reach_error_text() {
    if skip() {
        return;
    }
    let url = live_url().unwrap();
    let (clean_url, user, pass) = split_creds(&url);
    let Some(real_pass) = pass else {
        eprintln!("skipping credential-leak probe: live URL carries no password");
        return;
    };
    // Force an auth failure with a WRONG password and assert neither the
    // wrong nor the real password appears in the error.
    unsafe {
        std::env::set_var("SKARDI_NEO4J_WRONGPASS_USER", user.unwrap_or_default());
        std::env::set_var("SKARDI_NEO4J_WRONGPASS_PASS", "definitely_wrong_pw_9");
    }
    let err = Neo4jClient::connect(
        "kg",
        &clean_url,
        None,
        Some("SKARDI_NEO4J_WRONGPASS_USER"),
        Some("SKARDI_NEO4J_WRONGPASS_PASS"),
        2,
        std::time::Duration::from_secs(10),
    )
    .await
    .expect_err("wrong password fails registration");
    let msg = err.to_string();
    assert!(!msg.contains("definitely_wrong_pw_9"), "{msg}");
    assert!(!msg.contains(&real_pass), "real password never leaks");
}
